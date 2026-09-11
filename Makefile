# KubeScaleSense — developer entrypoints
#
# Phase 0 targets are real. Phase 2 demo targets exist so that the interface is
# stable, and fail with an explicit message rather than pretending to work; a
# demo target that silently does nothing is worse than one that refuses.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

BINARY      := kubescalesense
CMD_PKG     := ./cmd/kubescalesense
BIN_DIR     := bin
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE       ?= kubescalesense:$(VERSION)
LDFLAGS     := -s -w -X main.version=$(VERSION)
COVER_FILE  := coverage.out

GO          ?= go
GOLANGCI    ?= golangci-lint
PYTHON      ?= python3

.PHONY: help
help: ## List the available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## --- Phase 0: build, test, lint -------------------------------------------

.PHONY: build
build: ## Compile the controller into bin/
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) $(CMD_PKG)
	@echo "built $(BIN_DIR)/$(BINARY) ($(VERSION))"

.PHONY: test
test: ## Run the unit tests with the race detector and coverage
	$(GO) test -race -covermode=atomic -coverprofile=$(COVER_FILE) ./...

.PHONY: cover
cover: test ## Show per-package coverage
	$(GO) tool cover -func=$(COVER_FILE)

.PHONY: lint
lint: fmt-check vet ## Run gofmt, go vet and golangci-lint
	@if command -v $(GOLANGCI) >/dev/null 2>&1; then \
		$(GOLANGCI) run; \
	else \
		echo "golangci-lint not installed; ran gofmt and go vet only."; \
		echo "install: $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format the tree
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-clean
	@unformatted=$$(gofmt -l . 2>/dev/null); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt-clean:"; echo "$$unformatted"; exit 1; \
	fi

.PHONY: tidy
tidy: ## Tidy and verify go.mod/go.sum
	$(GO) mod tidy
	$(GO) mod verify

.PHONY: validate-config
validate-config: build ## Validate the shipped config file
	KSS_WORKLOAD_SIGNAL_SOURCE=none $(BIN_DIR)/$(BINARY) -config config/kubescalesense.yaml -validate

.PHONY: docs-check
docs-check: ## Verify every internal docs link and anchor resolves
	$(PYTHON) hack/linkcheck.py

.PHONY: image
image: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf $(BIN_DIR) $(COVER_FILE)

.PHONY: verify
verify: fmt-check vet lint test docs-check ## Everything CI runs

## --- Phase 2: demonstration pipeline --------------------------------------

.PHONY: demo-up
demo-up: ## (P2) Bring up the kind demo pipeline
	@echo "make demo-up is not implemented until P2 — Demonstration workload."; \
	echo "P2 adds the Normalizer, normalizer-service, SFTP, NiFi, the file generator,"; \
	echo "and the ballast Deployment. See docs/implementation-plan.md#p2--demonstration-workload."; \
	exit 1

.PHONY: demo-down
demo-down: ## (P2) Tear down the kind demo pipeline
	@echo "make demo-down is not implemented until P2 — Demonstration workload."; \
	echo "See docs/implementation-plan.md#p2--demonstration-workload."; \
	exit 1
