VERSION ?= $(shell cat VERSION)
IMG ?= quay.io/opendatahub/odh-batch-gateway-operator:latest
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.3
ENVTEST ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21
ENVTEST_K8S_VERSION ?= 1.33.0
LOCALBIN ?= $(shell pwd)/bin/k8s

KUSTOMIZE_VERSION ?= v5.8.1
KUSTOMIZE ?= $(shell pwd)/bin/kustomize

KIND_CLUSTER_NAME ?= batch-gateway-dev

## The full batch-gateway repo is checked out in $(BATCH_GATEWAY_DIR); the operator uses its chart + e2e tests.
## This replaces the old git submodule solution.
BATCH_GATEWAY_REPO ?= https://github.com/opendatahub-io/batch-gateway.git
BATCH_GATEWAY_REF  ?= 3372e140a342275ce6cb09bee51456884f275077
BATCH_GATEWAY_DIR  ?= batch-gateway

## Only the async-processor chart is sparse-checked-out from llm-d-async.
LLM_D_ASYNC_REPO ?= https://github.com/opendatahub-io/llm-d-async.git
LLM_D_ASYNC_REF  ?= dc30b7bb03d5f7d27cf097fd2d296675ed9f5376
LLM_D_ASYNC_DIR  ?= llm-d-async

## Deps

.PHONY: deps
deps:
	go mod tidy -v

## Build

.PHONY: build
build:
	go build -ldflags "-X main.version=$(VERSION)" -o bin/manager ./cmd/

## Lint & Format

GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint:
	$(GOLANGCI_LINT) run

## Code Generation

.PHONY: manifests
manifests:
	$(CONTROLLER_GEN) rbac:roleName=operator crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate:
	$(CONTROLLER_GEN) object paths="./..."

## Test

.PHONY: test
test: generate manifests setup-envtest fetch-batch-gateway fetch-llm-d-async
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
	go test -v ./... -count=1

.PHONY: setup-envtest
setup-envtest:
	$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN)

## Container

CONTAINER_TOOL ?= $(shell command -v docker 2>/dev/null || command -v podman 2>/dev/null)

.PHONY: docker-build
docker-build: fetch-batch-gateway fetch-llm-d-async
	$(CONTAINER_TOOL) build -t $(IMG) -f Dockerfile .

.PHONY: docker-build-konflux
docker-build-konflux: ## Build with Dockerfile.konflux
	$(CONTAINER_TOOL) build -t $(IMG) -f Dockerfile.konflux .

.PHONY: docker-push
docker-push:
	$(CONTAINER_TOOL) push $(IMG)

## Install

.PHONY: install
install: manifests
	kubectl apply -f config/crd/bases/

.PHONY: uninstall
uninstall:
	kubectl delete -f config/crd/bases/

## Kustomize

.PHONY: kustomize
kustomize: ## Install kustomize locally
	@test -s $(KUSTOMIZE) || \
		GOBIN=$(shell pwd)/bin go install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

.PHONY: verify-manifests
verify-manifests: manifests kustomize ## Verify that every overlay builds successfully
	@return=0; for dir in config/overlays/*/; do \
		$(KUSTOMIZE) build "$$dir" >/dev/null && echo "✓ $$dir" || { echo "✗ $$dir" >&2; return=1; }; \
	done; exit $$return

## Deploy (kustomize)

.PHONY: deploy
deploy: manifests kustomize
	cd config/default && $(KUSTOMIZE) edit set image controller=$(IMG)
	kubectl apply -k config/default

.PHONY: undeploy
undeploy:
	kubectl delete -k config/default --ignore-not-found

## Dev (Kind)

.PHONY: dev-deploy
dev-deploy: kustomize fetch-batch-gateway fetch-llm-d-async
	PATH="$(shell pwd)/bin:$$PATH" hack/dev-deploy.sh

.PHONY: dev-clean
dev-clean:
	hack/dev-clean.sh

.PHONY: dev-rm-cluster
dev-rm-cluster:
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# TODO: enable more e2e tests (currently only running Batches/Lifecycle as a smoke test)
TEST_RUN ?= TestE2E/Batches/Lifecycle

.PHONY: test-e2e-batch-gateway
test-e2e-batch-gateway: fetch-batch-gateway
	bash hack/test-e2e-batch-gateway.sh

.PHONY: test-e2e-operator
test-e2e-operator: dev-deploy
	cd test/e2e && TEST_CR_NAME=batch-gateway-dev go test -v -count=1 -timeout 5m ./...

.PHONY: fetch-batch-gateway
fetch-batch-gateway: ## Fetch the full batch-gateway repo at BATCH_GATEWAY_REF (the operator uses its chart + e2e tests).
	@if ! git -C $(BATCH_GATEWAY_DIR) rev-parse --git-dir >/dev/null 2>&1; then \
		echo "Cloning batch-gateway $(BATCH_GATEWAY_REF) from $(BATCH_GATEWAY_REPO)"; \
		git init -q $(BATCH_GATEWAY_DIR) && \
		git -C $(BATCH_GATEWAY_DIR) fetch -q --depth 1 $(BATCH_GATEWAY_REPO) $(BATCH_GATEWAY_REF) && \
		git -C $(BATCH_GATEWAY_DIR) checkout -q FETCH_HEAD; \
	elif [ "$$(git -C $(BATCH_GATEWAY_DIR) rev-parse HEAD)" = "$(BATCH_GATEWAY_REF)" ]; then \
		echo "batch-gateway already at $(BATCH_GATEWAY_REF)"; \
	elif [ -n "$$(git -C $(BATCH_GATEWAY_DIR) status --porcelain -uno)" ]; then \
		echo "WARNING: $(BATCH_GATEWAY_DIR) has uncommitted changes and is not at $(BATCH_GATEWAY_REF); using it as-is."; \
		echo "Commit/stash them (or 'rm -rf $(BATCH_GATEWAY_DIR)') and re-run to use the pinned ref."; \
	else \
		echo "Updating batch-gateway to $(BATCH_GATEWAY_REF) from $(BATCH_GATEWAY_REPO)"; \
		git -C $(BATCH_GATEWAY_DIR) fetch -q --depth 1 $(BATCH_GATEWAY_REPO) $(BATCH_GATEWAY_REF) && \
		git -C $(BATCH_GATEWAY_DIR) checkout -q FETCH_HEAD; \
	fi

.PHONY: fetch-llm-d-async
fetch-llm-d-async: ## Sparse-checkout only charts/async-processor from llm-d-async at LLM_D_ASYNC_REF.
	@if ! git -C "$(LLM_D_ASYNC_DIR)" rev-parse --git-dir >/dev/null 2>&1; then \
		echo "Cloning llm-d-async $(LLM_D_ASYNC_REF) from $(LLM_D_ASYNC_REPO) (sparse: charts/async-processor)"; \
		git init -q "$(LLM_D_ASYNC_DIR)" && \
		git -C "$(LLM_D_ASYNC_DIR)" sparse-checkout set --no-cone 'charts/async-processor/*' && \
		git -C "$(LLM_D_ASYNC_DIR)" fetch -q --depth 1 "$(LLM_D_ASYNC_REPO)" "$(LLM_D_ASYNC_REF)" && \
		git -C "$(LLM_D_ASYNC_DIR)" checkout -q FETCH_HEAD; \
	elif [ "$$(git -C "$(LLM_D_ASYNC_DIR)" rev-parse HEAD)" = "$(LLM_D_ASYNC_REF)" ]; then \
		echo "llm-d-async already at $(LLM_D_ASYNC_REF)"; \
	else \
		echo "Updating llm-d-async to $(LLM_D_ASYNC_REF) from $(LLM_D_ASYNC_REPO)"; \
		git -C "$(LLM_D_ASYNC_DIR)" fetch -q --depth 1 "$(LLM_D_ASYNC_REPO)" "$(LLM_D_ASYNC_REF)" && \
		git -C "$(LLM_D_ASYNC_DIR)" checkout -q FETCH_HEAD; \
	fi

.PHONY: sync-prefetched-charts
sync-prefetched-charts: fetch-batch-gateway fetch-llm-d-async ## For downstream with konflux only
	@rm -rf prefetched-charts/batch-gateway && mkdir -p prefetched-charts
	@cp -r "$(BATCH_GATEWAY_DIR)/charts/batch-gateway" prefetched-charts/batch-gateway
	@echo "synced prefetched-charts/batch-gateway at $(BATCH_GATEWAY_REF)"
	@rm -rf prefetched-charts/async-processor && mkdir -p prefetched-charts
	@cp -r "$(LLM_D_ASYNC_DIR)/charts/async-processor" prefetched-charts/async-processor
	@echo "synced prefetched-charts/async-processor at $(LLM_D_ASYNC_REF)"
