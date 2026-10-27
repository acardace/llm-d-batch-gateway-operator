# LLM Batch Gateway Operator

A Kubernetes operator that manages the lifecycle of [batch-gateway](https://github.com/opendatahub-io/batch-gateway) deployments. It reconciles a single `LLMBatchGateway` custom resource into the full set of Kubernetes resources by rendering the upstream Helm chart at runtime.

[Design Document](https://docs.google.com/document/d/1cYrR-GRnFEfaG-D2WwJqK5c8m6iq3QHyPHs6KMjou0o/edit?usp=sharing)

```
LLMBatchGateway CR
       |
       v
  Controller (Reconcile)
       |
       v
  specToBatchHelmValues() / specToAsyncHelmValues()  ->  Helm values map
       |
       v
  HelmRenderer.RenderBatchChart() / RenderAsyncChart()  ->  []unstructured.Unstructured
       |
       v
  Server-Side Apply  ->  K8s resources (Deployments, Services, etc.)
```

## 1. Prerequisites

- Go 1.26+
- kubectl
- [kustomize](https://kubectl.docs.kubernetes.io/installation/kustomize/)
- Docker or Podman

For local end-to-end testing:

- [Kind](https://kind.sigs.k8s.io/)
- [Helm](https://helm.sh/)

## 2. Quick Start

```bash
git clone https://github.com/opendatahub-io/llm-d-batch-gateway-operator.git
cd llm-d-batch-gateway-operator

# Build
make build

# Run tests
make test
```

The entire upstream batch-gateway repo is fetched on demand into `batch-gateway/` at a pinned commit by
`make fetch-batch-gateway`, which the relevant targets (`test`, `docker-build`, `dev-deploy`, `test-e2e-batch-gateway`) depend on; the operator uses its Helm chart
and e2e tests. To fetch it explicitly:

```bash
make fetch-batch-gateway
```

## 3. Project Structure

```
.
├── api/v1alpha1/                # CRD type definitions
│   ├── llmbatchgateway_types.go # LLMBatchGateway spec/status structs
│   └── zz_generated.deepcopy.go # Generated DeepCopy methods
├── cmd/main.go                  # Operator entrypoint
├── internal/controller/
│   ├── llmbatchgateway_controller.go  # Reconcile loop
│   ├── helm.go                        # HelmRenderer: renders a chart to objects
│   ├── helm_batch.go                  # CRD spec → batch-gateway Helm values
│   ├── helm_async.go                  # CRD spec → llm-d-async Helm values
│   └── *_test.go                      # Unit and integration tests
├── config/
│   ├── crd/bases/               # Generated CRD YAML
│   ├── manager/                 # Operator Deployment manifest
│   ├── rbac/                    # RBAC roles and bindings
│   └── samples/                 # Example LLMBatchGateway CRs
├── hack/                        # Dev scripts (Kind cluster setup)
├── batch-gateway/
├── Makefile
└── Dockerfile
```

## 4. Development

### 4.1 Modifying the CRD

To add or change fields in the `LLMBatchGateway` custom resource:

1. Edit the Go structs in `api/v1alpha1/llmbatchgateway_types.go`
2. Regenerate DeepCopy methods and CRD manifests:

```bash
make generate   # updates zz_generated.deepcopy.go
make manifests  # updates config/crd/bases/batch.llm-d.ai_llmbatchgateways.yaml
```

3. If the new field needs to be passed to a Helm chart, update `specToBatchHelmValues()` in `internal/controller/helm_batch.go` (batch-gateway components) or `specToAsyncHelmValues()` in `internal/controller/helm_async.go` (async processor)
4. Update sample CRs in `config/samples/` to include the new field

### 4.2 Modifying the Controller

The reconcile loop lives in `internal/controller/llmbatchgateway_controller.go`. The flow is:

1. Fetch the `LLMBatchGateway` CR
2. Call `HelmRenderer.RenderChart()` to produce unstructured K8s objects
3. Set owner references and apply each object via Server-Side Apply
4. Update status conditions (`Ready`, `APIServerAvailable`, `ProcessorAvailable`, `GCAvailable`, `AsyncProcessorAvailable`)

### 4.3 Modifying the Helm Values Mapping

The mapping from CRD spec to Helm values lives in `internal/controller/helm_batch.go` (`specToBatchHelmValues`, for the batch-gateway chart) and `internal/controller/helm_async.go` (`specToAsyncHelmValues`, for the llm-d-async chart). When the upstream chart adds new values or the CRD adds new fields, update the matching function accordingly.

### 4.4 Updating the Helm Chart

The entire batch-gateway git repo is fetched on demand into `batch-gateway/` folder
at a pinned commit (the operator uses its Helm chart and e2e tests). The pin lives in the `Makefile` as
`BATCH_GATEWAY_REF` — it is the single source of truth. The `batch-gateway/` directory itself is gitignored and never committed.

Bump the pin to the current tip of upstream `main`. Resolve it to a SHA and pin
that — don't set `BATCH_GATEWAY_REF` to the branch name `main`, or builds would
fetch a moving target and stop being reproducible:

```bash
# resolve the current main tip to an immutable commit SHA
git ls-remote https://github.com/opendatahub-io/batch-gateway.git refs/heads/main
# set BATCH_GATEWAY_REF in the Makefile to that SHA (or a release tag)
git add Makefile
git commit -m "chore: bump opendatahub-io/batch-gateway to <sha>"
```

To try a specific commit, tag, or branch for local test, override `BATCH_GATEWAY_REF`
on the command line — the committed pin in the `Makefile` is left unchanged:

```bash
make fetch-batch-gateway BATCH_GATEWAY_REF=<commit-tag-or-branch>
```

To build or test against it, pass the same override to the target doing the work
(or `export BATCH_GATEWAY_REF=<ref>` for a whole session) so the fetch and the
build agree on the ref:

```bash
make test         BATCH_GATEWAY_REF=my-branch
make docker-build BATCH_GATEWAY_REF=my-branch
```

A bare `make test` afterwards would re-fetch the pinned ref and reset
`batch-gateway/`, so always pass the override on the command that does the work.
(A branch re-fetches its latest tip each run; an uncommitted local checkout is
left untouched.)

## 5. Testing

### 5.1 Unit and Integration Tests

```bash
make test
```

This runs all tests using [envtest](https://book.kubebuilder.io/reference/envtest) (a local control plane without a real cluster). Tests cover:

- `specToBatchHelmValues()` / `specToAsyncHelmValues()` mapping correctness
- Helm chart rendering (requires the batch-gateway chart; `make test` fetches it automatically)
- Controller reconciliation: resource creation, owner references, status conditions, spec updates

### 5.2 E2E Tests with Kind

One command sets up a full local environment:

```bash
make dev-deploy
```

This creates a Kind cluster and deploys PostgreSQL, SeaweedFS, and Redis, plus a vLLM simulator, the operator, and a dev `LLMBatchGateway` CR. Configurable via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KIND_CLUSTER_NAME` | `batch-gateway-dev` | Kind cluster name |
| `NAMESPACE` | `default` | Target namespace |
| `OPERATOR_IMG` | `localhost/llm-d-batch-gateway-operator:dev` | Operator image |
| `POSTGRESQL_PASSWORD` | `postgres` | PostgreSQL password |
| `SEAWEEDFS_ACCESS_KEY` | generated by `dev-deploy` | Unique SeaweedFS S3 access key; required when running `setup-prereqs.sh` directly |
| `SEAWEEDFS_SECRET_KEY` | generated by `dev-deploy` | Unique SeaweedFS S3 secret key; required when running `setup-prereqs.sh` directly |
| `APISERVER_IMG` | `ghcr.io/llm-d/batch-gateway-apiserver:latest` | API server image |
| `PROCESSOR_IMG` | `ghcr.io/llm-d/batch-gateway-processor:latest` | Processor image |
| `GC_IMG` | `ghcr.io/llm-d/batch-gateway-gc:latest` | GC image |
| `APISERVER_NODE_PORT` | `30080` | NodePort for API server |

Once the environment is up, run the upstream e2e tests:

```bash
make test-e2e
```

See `batch-gateway/test/e2e/README.md` for the full list of `TEST_*` environment variables.

Cleanup:

```bash
make dev-clean       # remove operator and dependencies, keep cluster
make dev-rm-cluster  # delete the Kind cluster
```

## 6. Building and Pushing the Image

```bash
make docker-build                           # build with default tag
make docker-build IMG=my-registry/operator:v0.1.0  # custom image
make docker-push IMG=my-registry/operator:v0.1.0
```
