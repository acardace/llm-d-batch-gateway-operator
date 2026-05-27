VERSION ?= $(shell cat VERSION)
IMG ?= ghcr.io/opendatahub-io/batch-gateway-operator:latest
ASYNC_PROCESSOR_IMG ?= ghcr.io/llm-d-incubation/llm-d-async:v0.7.0-RC3
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.3
ENVTEST ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21
ENVTEST_K8S_VERSION ?= 1.33.0
LOCALBIN ?= $(shell pwd)/bin/k8s

## Utility

print-%:
	@echo $($*)

## Deps

.PHONY: deps
deps:
	go mod tidy -v

## Build

.PHONY: build
build:
	go build -ldflags "-X main.version=$(VERSION) -X main.defaultAsyncProcessorImage=$(ASYNC_PROCESSOR_IMG)" -o bin/manager ./cmd/

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
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate:
	$(CONTROLLER_GEN) object paths="./..."

## Test

.PHONY: test
test: generate manifests setup-envtest
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
	CGO_ENABLED=1 \
	go test -v ./... -race -count=1

.PHONY: setup-envtest
setup-envtest:
	$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN)

## Container

CONTAINER_TOOL ?= $(shell command -v docker 2>/dev/null || command -v podman 2>/dev/null)

.PHONY: docker-build
docker-build:
	$(CONTAINER_TOOL) build -t $(IMG) \
		--build-arg VERSION=$(VERSION) \
		--build-arg ASYNC_PROCESSOR_IMG=$(ASYNC_PROCESSOR_IMG) \
		-f Dockerfile .

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

## Deploy (kustomize)

.PHONY: deploy
deploy: manifests
	cd config/default && kustomize edit set image controller=$(IMG)
	kubectl apply -k config/default

.PHONY: undeploy
undeploy:
	kubectl delete -k config/default --ignore-not-found

## Dev (Kind)

KIND_CLUSTER_NAME ?= batch-gateway-dev
DEPLOY_ASYNC_PROCESSOR ?= false

.PHONY: dev-deploy
dev-deploy: ## Deploy dev environment. Set DEPLOY_ASYNC_PROCESSOR=true to include the async processor.
	DEPLOY_ASYNC_PROCESSOR=$(DEPLOY_ASYNC_PROCESSOR) ASYNC_PROCESSOR_IMG=$(ASYNC_PROCESSOR_IMG) hack/dev-deploy.sh

.PHONY: dev-clean
dev-clean:
	hack/dev-clean.sh

.PHONY: dev-rm-cluster
dev-rm-cluster:
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# TODO: enable more e2e tests (currently only running Batches/Lifecycle as a smoke test)
TEST_RUN ?= TestE2E/Batches/Lifecycle

.PHONY: test-e2e-batch-gateway
test-e2e-batch-gateway:
	cd batch-gateway/test/e2e && go test -v -count=1 -run "$(TEST_RUN)" ./...

.PHONY: test-e2e-operator
test-e2e-operator:
	cd test/e2e && go test -v -count=1 -timeout 5m ./...

## Submodule

.PHONY: update-submodule
update-submodule:
	git submodule update --remote batch-gateway

