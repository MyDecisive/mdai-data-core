.PHONY: test test-race lint tidy tidy-check vendor generate generate-check install-mocks cover coverv coverhtml clean-coverage
GOTOOLCHAIN ?= go1.25.14
GO := CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
# -mod=readonly: build from the module cache even if a local (gitignored) vendor/ exists and is stale.
GO_TEST := $(GO) test -mod=readonly -count=1
# Where `go install` puts the mock generators; put first on PATH so `go generate` finds them.
GO_BIN := $(or $(shell $(GO) env GOBIN),$(shell $(GO) env GOPATH)/bin)
# Directories holding generated mocks (see mock/mock.go and eventing/publisher/publisher.go).
MOCK_DIRS := mock internal/mocks
# Keep in sync with the golangci-lint-action version in .github/workflows/chores.yml.
GOLANGCI_LINT_VERSION ?= v2.4.0

test:
	$(GO_TEST) -v ./...

test-race:
	$(GO_TEST) -race -v ./...

# Runs the same golangci-lint version as CI under the repo's toolchain, whatever is installed locally.
lint:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

tidy:
	@$(GO) mod tidy

tidy-check:
	@$(GO) mod tidy -diff

vendor:
	@$(GO) mod vendor

generate: install-mocks
	@PATH="$(GO_BIN):$$PATH" $(GO) generate ./...

# Fails if the mocks in the working tree differ from what the pinned generators produce.
# Generates into a temporary copy of the working tree, so it never modifies your files.
generate-check: install-mocks
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	rsync -a --exclude=/.git/ --exclude=/vendor/ ./ "$$tmp/" && \
	(cd "$$tmp" && PATH="$(GO_BIN):$$PATH" $(GO) generate ./... >/dev/null) || exit 1; \
	stale=0; \
	for dir in $(MOCK_DIRS); do \
		diff -ru "$$dir" "$$tmp/$$dir" || stale=1; \
	done; \
	if [ "$$stale" -ne 0 ]; then \
		echo "generated mocks are out of date (diff above: working tree -> generated); run 'make generate' and commit the result"; \
		exit 1; \
	fi

install-mocks:
	@$(GO) install go.uber.org/mock/mockgen@v0.6.0
	@$(GO) install github.com/vektra/mockery/v3@v3.5.4

cover:
	$(GO_TEST) -v -coverprofile=coverage.out ./...

# Kept for existing scripts; same as cover.
coverv: cover

coverhtml:
	@trap 'rm -f coverage.out' EXIT; \
	$(GO_TEST) -coverprofile=coverage.out ./... && \
	$(GO) tool cover -html=coverage.out -o coverage.html && \
	( open coverage.html || xdg-open coverage.html )

clean-coverage:
	@rm -f coverage.out coverage.html
