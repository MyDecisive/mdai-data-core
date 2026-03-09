.PHONY: test test-race tidy tidy-check vendor generate install-mocks cover coverv coverhtml clean-coverage
GOTOOLCHAIN ?= go1.25.0
GO := CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go
GO_TEST := $(GO) test -count=1

test: tidy vendor generate
	$(GO_TEST) -v ./...

test-race: tidy vendor
	$(GO_TEST) -race -v ./...

tidy:
	@$(GO) mod tidy

tidy-check: tidy
	@$(GO) mod tidy -diff

vendor:
	@$(GO) mod vendor

generate: install-mocks
	@$(GO) generate ./...

install-mocks:
	@$(GO) install go.uber.org/mock/mockgen@v0.6.0
	@$(GO) install github.com/vektra/mockery/v3@v3.5.4

cover: tidy vendor generate
	$(GO_TEST) -cover ./...

coverv: tidy vendor generate
	$(GO_TEST) -v -cover ./...

coverhtml: tidy vendor generate
	@trap 'rm -f coverage.out' EXIT; \
	$(GO_TEST) -coverprofile=coverage.out ./... && \
	$(GO) tool cover -html=coverage.out -o coverage.html && \
	( open coverage.html || xdg-open coverage.html )

clean-coverage:
	@rm -f coverage.out coverage.html
