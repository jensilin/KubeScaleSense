# KubeScaleSense — developer entrypoints
#
# `make verify` is what CI runs and what a change must pass. The demo-* targets
# drive the Phase 2 demonstration pipeline on an isolated kind cluster; they
# refuse to run rather than improvise if a prerequisite is missing, because a
# demo target that silently does something else is worse than one that stops.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

BINARY      := kubescalesense
CMD_PKG     := ./cmd/kubescalesense
NORMALIZER  := normalizer
NORM_PKG    := ./cmd/normalizer
LOADGEN     := loadgen
LOADGEN_PKG := ./tests/demo/loadgen
BIN_DIR     := bin
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE       ?= kubescalesense:$(VERSION)
NORM_IMAGE  ?= normalizer:$(VERSION)
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
build: ## Compile the controller, the Normalizer and the load generator into bin/
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) $(CMD_PKG)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(NORMALIZER) $(NORM_PKG)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '-s -w' -o $(BIN_DIR)/$(LOADGEN) $(LOADGEN_PKG)
	@echo "built $(BIN_DIR)/{$(BINARY),$(NORMALIZER),$(LOADGEN)} ($(VERSION))"

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
validate-config: build ## Validate the shipped config files
	KSS_WORKLOAD_SIGNAL_SOURCE=none $(BIN_DIR)/$(BINARY) -config config/kubescalesense.yaml -validate
	$(BIN_DIR)/$(NORMALIZER) -validate

.PHONY: docs-check
docs-check: ## Verify every internal docs link and anchor resolves
	$(PYTHON) hack/linkcheck.py

.PHONY: image
image: ## Build the controller container image
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

.PHONY: image-normalizer
image-normalizer: ## Build the Normalizer container image
	docker build --build-arg VERSION=$(VERSION) -f Dockerfile.normalizer -t $(NORM_IMAGE) .

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf $(BIN_DIR) $(COVER_FILE)

.PHONY: verify
verify: fmt-check vet lint test docs-check ## Everything CI runs

## --- Phase 2: demonstration pipeline --------------------------------------
#
# These three targets operate on the kind cluster named kubescalesense-demo and
# nothing else. Every kubectl call inside them is pinned to that cluster's
# context, which is why they are scripts rather than recipes: an unpinned
# kubectl in a Makefile inherits whatever context happens to be current, and
# "whatever happens to be current" is somebody's real cluster.

.PHONY: demo-up
demo-up: ## Create the kind demo cluster and bring up the pipeline
	tests/e2e/demo-up.sh

.PHONY: demo-spike
demo-spike: ## Drive a LOW -> HIGH -> LOW workload spike through the pipeline
	tests/e2e/demo-spike.sh

.PHONY: demo-down
demo-down: ## Delete the kind demo cluster
	tests/e2e/demo-down.sh

.PHONY: demo-observe
demo-observe: ## Follow the controller's decisions while a spike runs
	@kubectl --context kind-kubescalesense-demo -n kubescalesense \
		logs -l app.kubernetes.io/name=kubescalesense -f --tail=20

## --- Phase 3: actuation ---------------------------------------------------
#
# Enabling actuation is its own target rather than a flag on demo-up, because
# the documented order matters: bring the environment up in dry-run, confirm the
# decisions are the ones you expected, and only then let the controller act on
# them.

.PHONY: demo-actuate
demo-actuate: ## Let the demo controller actually scale (dryRun: false)
	tests/e2e/demo-actuate.sh on

.PHONY: demo-dry-run
demo-dry-run: ## Return the demo controller to observing only (dryRun: true)
	tests/e2e/demo-actuate.sh off

.PHONY: demo-replicas
demo-replicas: ## Show the target's replica count and the controller's last decision
	@kubectl --context kind-kubescalesense-demo -n data-pipeline \
		get deployment normalizer -o custom-columns=NAME:.metadata.name,DESIRED:.spec.replicas,READY:.status.readyReplicas
	@kubectl --context kind-kubescalesense-demo -n kubescalesense \
		logs -l app.kubernetes.io/name=kubescalesense --tail=1
