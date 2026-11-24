# Local checks for SQLiteRelay. CI runs the same targets.

GO ?= go
PKGS ?= ./...
GOLANGCI_LINT_VERSION ?= v2.15.0
GOLANGCI_LINT_PKG := github.com/golangci/golangci-lint/v2/cmd/golangci-lint

.DEFAULT_GOAL := help

.PHONY: help build test test-race bench vet lint fmt fmt-check tools

help: ## Show the available targets
	@awk -F':.*## ' '/^[a-z-]+:.*## / && $$1 != "help" { printf "%-12s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Compile every package
	$(GO) build $(PKGS)

test: ## Run the tests (PKGS=./driver/... to scope them)
	$(GO) test $(PKGS)

test-race: ## Run the tests with the race detector
	$(GO) test -race $(PKGS)

bench: ## Run the benchmarks
	$(GO) test -bench . -benchmem -run '^$$' $(PKGS)

vet: ## Run go vet
	$(GO) vet $(PKGS)

lint: fmt-check ## Check formatting and run golangci-lint
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not found, run 'make tools'"; exit 1; }
	golangci-lint run $(PKGS)

fmt: ## Format every file
	gofmt -w .

fmt-check: ## Fail when a file is not gofmt clean
	@files=$$(gofmt -l .); if [ -n "$$files" ]; then echo "gofmt needed:"; echo "$$files"; exit 1; fi

tools: ## Install the pinned golangci-lint
	$(GO) install $(GOLANGCI_LINT_PKG)@$(GOLANGCI_LINT_VERSION)
