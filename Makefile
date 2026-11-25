# Local checks for SQLiteRelay. CI runs the same targets.

GO ?= go
PKGS ?= ./...
E2E_PKGS ?= ./e2e/...
GOLANGCI_LINT_VERSION ?= v2.15.0
GOLANGCI_LINT_PKG := github.com/golangci/golangci-lint/v2/cmd/golangci-lint

.DEFAULT_GOAL := help

.PHONY: help build tests e2e bench vet lint fmt fmt-check tools

help: ## Show the available targets
	@awk -F':.*## ' '/^[a-z0-9-]+:.*## / && $$1 != "help" { printf "%-12s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Compile every package
	$(GO) build $(PKGS)

tests: ## Run the tests with the race detector (PKGS=./driver/... to scope them)
	$(GO) test -race $(PKGS)

e2e: ## Run the end-to-end tests of the relay (E2E_PKGS to scope them)
	$(GO) test -race -tags e2e $(E2E_PKGS)

bench: ## Run the benchmarks
	$(GO) test -bench . -benchmem -run '^$$' -tags e2e $(PKGS)

vet: ## Run go vet
	$(GO) vet $(PKGS)

lint: fmt-check ## Check formatting and run golangci-lint
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not found, run 'make tools'"; exit 1; }
	@pinned=$$($(GO) env GOPATH)/bin/golangci-lint; \
	if [ -x "$$pinned" ]; then "$$pinned" run --build-tags=e2e $(PKGS); else golangci-lint run --build-tags=e2e $(PKGS); fi

fmt: ## Format every file
	gofmt -w .

fmt-check: ## Fail when a file is not gofmt clean
	@files=$$(gofmt -l .) || { echo "gofmt could not read the tree"; exit 1; }; \
	if [ -n "$$files" ]; then echo "gofmt needed:"; echo "$$files"; exit 1; fi

tools: ## Install the pinned golangci-lint
	$(GO) install $(GOLANGCI_LINT_PKG)@$(GOLANGCI_LINT_VERSION)
