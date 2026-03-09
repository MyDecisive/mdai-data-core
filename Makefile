.PHONY: test test-race tidy vendor generate install-mocks
GOTOOLCHAIN ?= go1.25.0
GO := CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
GO_TEST := $(GO) test -count=1

test: tidy vendor
	$(GO_TEST) -v -count=1 ./...

test-race: tidy vendor
	$(GO_TEST) -race -mod=vendor -v -count=1 ./...

tidy:
	@$(GO) mod tidy

vendor:
	@$(GO) mod vendor

generate:
	@$(GO) generate ./...

install-mocks:
	@$(GO) install go.uber.org/mock/mockgen@v0.6.0
	@$(GO) install github.com/vektra/mockery/v3@v3.5.4

.PHONY: cover
cover: tidy vendor
	$(GO_TEST) -cover ./...

.PHONY: coverv
coverv: tidy vendor
	$(GO_TEST) -v -cover ./...

.PHONY: coverhtml
coverhtml:
	@trap 'rm -f coverage.out' EXIT; \
	$(GO_TEST) -coverprofile=coverage.out ./... && \
	$(GO) tool cover -html=coverage.out -o coverage.html && \
	( open coverage.html || xdg-open coverage.html )

.PHONY: clean-coverage
clean-coverage:
	@rm -f coverage.out coverage.html
