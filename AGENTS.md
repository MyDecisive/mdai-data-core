# AGENTS.md

## Repository overview

`mdai-data-core` is a Go library shared by MDAI services. It provides the common data and integration layer for:

- Valkey client configuration and variable storage
- audited variable mutations and variable-update event publication
- variable default canonicalization and resolution
- NATS/JetStream event publication, subscription, subjects, triggers, and rules
- Kubernetes ConfigMap and Secret informers
- OpAMP agent connection management
- application and OpenTelemetry logging

The module path is `github.com/mydecisive/mdai-data-core`. This is a library, not a standalone service, so exported APIs and wire/storage formats may have downstream consumers outside this repository.

## Repository map

- `audit/`: audit event types and Valkey stream operations.
- `eventing/`: the shared event envelope plus NATS/JetStream configuration.
  - `config/`: environment-backed NATS configuration and stream/consumer setup.
  - `publisher/`: event publisher and its generated interface mock.
  - `subscriber/`: event subscription and dispatch.
  - `rule/` and `triggers/`: strict JSON models for rules and trigger matching.
- `handlers/`: higher-level variable mutations. A mutation combines the Valkey update, an audit entry, and publication of a variable-update event.
- `variables/`: Valkey variable reads/commands, canonical data types, and default resolution.
- `valkey/`: low-level client configuration and connection setup.
- `kube/`: ConfigMap/Secret informer controllers and Kubernetes client construction. Test fixtures live in `kube/testdata/`; reusable test stores live in `kube/kubetest/`.
- `opamp/`: connected-agent bookkeeping and OpAMP logger adaptation.
- `interpolation/`: event/template value interpolation.
- `service/`: Zap and OpenTelemetry logger setup.
- `helpers/`: small environment helpers.
- `mock/`: Mockery-generated mocks for `kube` and `opamp` interfaces (`mock/kube/`, `mock/opamp/`).
- `internal/mocks/`: MockGen-generated mocks (currently the eventing `Publisher`).
- Both mock trees are generated; do not hand-edit them.
- `docs/`: focused behavior documentation, currently interpolation syntax.

## Toolchain and common commands

The module declares `go 1.25.0`. The Makefile defaults to `GOTOOLCHAIN=go1.25.0` and disables CGO. CI installs Go from `go-version-file: go.mod`, not from the Makefile's `GOTOOLCHAIN`.

`make generate` (and targets that depend on it, such as `make test`, `make cover`, and `make coverhtml`) needs network access to `go install` the pinned mock generators. `make tidy`, `make vendor`, and `make test-race` may also download modules. Plain `go test` against an already-populated module cache does not need the network.

Prefer the smallest command that validates the change while iterating:

```sh
# One package
CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 go test -count=1 ./variables

# One test
CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 go test -count=1 ./eventing/config -run '^TestLoadConfig$'

# All packages without regenerating files
CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 go test -count=1 ./...

# Full test workflow (tidies, vendors, and regenerates mocks; needs network)
make test

# CI-equivalent test workflow (same as make test, plus writes coverage.out)
make cover

# Race detector (runs tidy + vendor, but not generate)
make test-race

# Generate mocks
make generate

# Apply formatters used by CI (gofmt + goimports, per .golangci.yml)
golangci-lint fmt

# Lint exactly as CI does (requires golangci-lint v2; CI pins v2.4.0)
golangci-lint run
```

`make test` and `make cover` are intentionally heavier than a direct `go test`: they may download pinned mock generators, run `go mod tidy`, refresh `vendor/`, and regenerate mocks; `make cover` also creates `coverage.out`. `vendor/` and `*.out` are gitignored, so they normally won't appear as untracked changes. Do not force-add or commit them. Afterwards, inspect `git diff` for unrelated churn in `go.mod`, `go.sum`, and the generated mock files, and do not include it.

`.golangci.yml` uses the golangci-lint v2 config format. A v1.x binary will reject it or behave differently, so match CI's `v2.4.0` when possible.

## Coding conventions

- Use standard Go formatting and import grouping. CI enforces both `gofmt` and `goimports` through golangci-lint formatters; `gofmt` alone does not fix import grouping, so prefer `golangci-lint fmt` (or `goimports -w`).
- Follow the existing package layout and keep changes narrowly scoped. Do not introduce a new cross-package abstraction when a small package-local interface is sufficient.
- Accept `context.Context` as the first argument for I/O or lifecycle operations and propagate it to Valkey, NATS, Kubernetes, and OpenTelemetry calls.
- Wrap errors with useful operation context and `%w` when callers may need `errors.Is`/`errors.As`. Preserve established sentinel errors such as `variables.ErrInvalidDefault` and `variables.ErrUnsupportedDataType`.
- Keep dependency interfaces small and consumer-owned. The `variables.Reader`, event `Publisher`, Kubernetes store interfaces, and injected dialers are examples of the preferred testability pattern.
- Use functional options for optional construction behavior when extending constructors that already use them.
- Use structured Zap fields for logs. Never log NATS passwords, credentials, tokens, Secret contents, or other sensitive values.
- Add doc comments for exported identifiers and explain behavior that is not obvious from the signature.
- Add a narrowly scoped `//nolint:<linter>` only when the API constraint makes the warning unavoidable; include a short explanation where useful.
- Do not silently change environment variable names or defaults. Configuration is part of the public operational contract.

## Domain invariants

Changes in these areas need extra care:

- Variable storage keys are hub-scoped. The stored key is `variable/<hubName>/<variableKey>` (`composeStorageKey` in `variables/adapter.go`). Note the difference: public APIs take arguments in the order `variableKey, hubName`, but the stored key puts the hub first. Preserve both unless performing an intentional migration.
- `variables.DataType` string values are shared wire/CRD/storage values. Changing them requires coordination with the operator, gateway, and event hub.
- Scalar values are stored canonically as strings. Integers, floats, and booleans must continue to use the canonicalization rules in `variables/canonicalize.go`.
- For `Resolve`, a stored empty scalar is present, while an empty set or map is treated as absent. `defaultRaw == nil` means no default was declared. Meta types do not materialize defaults.
- `ResolveResult.Value` is a canonical string for scalars, `[]string` for sets and meta priority lists, `map[string]string` for maps, and a `string` for meta hash sets. `Typed` performs the conversion for consumers; do not change the meta-hash-set shape without coordinating with consumers.
- Event JSON field names, event type strings, subjects, consumer-group names, source names, operation names, and schema version are compatibility surfaces. Update serialization tests when intentionally changing them.
- New events should receive defaults through `MdaiEvent.ApplyDefaults` and pass `Validate` before publication. Event IDs are time-ordered UUIDv7 values and timestamps are UTC.
- Handler mutations are a coordinated workflow: update Valkey and write the audit entry, then publish the matching variable event with hub, type, action, correlation ID, source, and recursion depth intact. Specifically:
  - The variable update and the audit write are sent together with `DoMulti`. That is a pipeline, not a `MULTI`/`EXEC` transaction, so it is not atomic. Do not document or rely on transactional guarantees.
  - Publication runs only after the Valkey step succeeds.
  - Publication is wrapped in `retryWithBackoff`, bounded by the adapter's `retryMaxTime`. A non-positive value means a single attempt.
  - Keep the retry when adding new mutations.
- Rule and trigger decoding intentionally rejects unknown fields. Preserve strict decoding so configuration mistakes fail early.
- Informer-backed objects may originate from shared caches. Return copies before exposing mutable Kubernetes objects or maps; do not let callers mutate cached state through returned pointers.

## Tests

- Add or update tests in the same package as the changed code. The repository generally uses `testify/assert` for comparisons and `testify/require` for prerequisites and fatal checks.
- Prefer table-driven tests for input matrices and explicit tests for important behavioral branches.
- Cover success, not-found/empty behavior, invalid input, and dependency errors. When changing a public or serialized type, add round-trip or exact JSON assertions.
- Keep tests deterministic. Use `t.Setenv` for environment configuration, `t.Context()` or bounded contexts for blocking work, Kubernetes fake clients/stores for informer behavior, and injected fakes/mocks for external dependencies.
- Eventing tests may start embedded NATS servers and can take longer than pure unit tests. Always clean up connections and servers and use bounded readiness waits rather than arbitrary long sleeps.
- Do not require a developer's Kubernetes cluster, Valkey instance, NATS deployment, credentials, or network access in tests.
- Run the changed package first, then `go test -count=1 ./...`. Run `make test-race` when changing goroutines, informer callbacks, connection management, shared maps, or shutdown behavior. `make test-race` does not regenerate mocks, so run `make generate` first if an interface changed.
- CI (`chores.yml`) runs on pushes to `main` and on PRs targeting `main`. A push to a feature branch with no open PR triggers nothing. Within a run, lint always runs, but tests run only when Go files, `go.mod`, or `go.sum` change. A docs-only PR passing CI does not mean the tests ran.

## Generated files and dependencies

- Files beginning with `Code generated ... DO NOT EDIT` must be regenerated rather than patched manually.
- `mock/mock.go` drives Mockery (pinned `v3.5.4`) using `mock/mockery.yml`, producing `mock/kube/mocks.go` and `mock/opamp/mocks.go`.
- `eventing/publisher/publisher.go` drives MockGen (pinned `v0.6.0`) for `internal/mocks/eventing/publisher/publisher.go`.
- `make generate` installs the pinned generator versions from the Makefile before running `go generate ./...`.
- After changing an interface represented by a generated mock, regenerate it and include both the source-interface change and generated output.
- Keep `go.mod` and `go.sum` consistent. To check for drift without modifying files, run `CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 go mod tidy -diff` (or `make tidy-check`). `vendor/` is gitignored: it is built locally and in CI by `make vendor` and is never committed. Use `go mod tidy` only when dependency changes require it, and review `go.mod`/`go.sum` diffs carefully.

## Before handing off a change

```sh
golangci-lint fmt                                                     # gofmt + goimports
CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 go test -count=1 ./<pkg>/...       # focused
make generate                                                         # only if an interface changed
CGO_ENABLED=0 GOTOOLCHAIN=go1.25.0 go test -count=1 ./...             # full suite
golangci-lint run                                                     # v2; CI pins v2.4.0
git status && git diff                                                # review
```

When reviewing the diff, check for:

- unintended `go.mod`/`go.sum` changes;
- generated mocks out of sync with their source interfaces;
- any `vendor/` or `coverage.*` file that was force-added despite `.gitignore`.

If a step could not run (for example, no network or no golangci-lint), say so explicitly.

Preserve backward compatibility unless the task explicitly calls for a breaking change, and call out any intentional API, storage, configuration, or wire-format change.

PR titles must follow the semantic PR title format, enforced by `.github/workflows/pr-title.yaml`. That workflow delegates to `MyDecisive/changelogs/.github/workflows/reusable-semantic-pr-title.yaml@main`, which lives outside this repository and can change without any change here. As of 2026-10-05 the allowed types are `build`, `chore`, `doc`, `feat`, `fix`, `perf`, `refactor`, `revert`, `security`, `style`, and `test`. A scope is optional, and the type is `doc:`, not `docs:`. If a title is rejected, check the reusable workflow for the current list.
