# ODH Installation Guide

Deploy LLM Batch Gateway through the ODH platform on OpenShift.

For upgrades, read the [Async Dispatch and Core Storage Migration](async-default-migration.md) guide first. It covers the separate PostgreSQL migration for Batch Gateway metadata and the transport choice for async queues.

## Architecture

```
opendatahub-operator (platform orchestrator)
  → ai-gateway-operator (module operator)
    → llm-d-batch-gateway-operator (sub-module operator)
      → operands: apiserver, processor, gc
```

1. **User** creates a DataScienceCluster CR with `aigateway.managementState: Managed` and `aigateway.batchGateway.managementState: Managed`
2. **ODH operator** renders ai-gateway-operator's kustomize manifests and deploys it (Deployment, RBAC, CRD) into the applications namespace
3. **ODH operator** creates an AIGateway CR with the batch gateway config projected from DSC
4. **ai-gateway-operator** reconciles the AIGateway CR and deploys llm-d-batch-gateway-operator (Deployment, RBAC, CRD)
5. **User** creates an LLMBatchGateway CR in a workload namespace with database, storage, and inference gateway config
6. **llm-d-batch-gateway-operator** reconciles the LLMBatchGateway CR and deploys the operands: API server, request processor, and garbage collector

## Prerequisites

- OpenShift cluster with ODH operator installed
- `oc` / `kubectl` CLI connected to the cluster
- `helm` (for installing PostgreSQL and optional Redis test dependencies)
- `kubectl` (for installing the SeaweedFS test dependency)
- External dependencies for LLMBatchGateway: PostgreSQL and S3-compatible storage. Async dispatch needs either Redis/Valkey for the default sorted-set transport or PostgreSQL for the experimental SQL transport.

## Installation

### Step 1: Create DSCI and DSC with Batch Gateway enabled

```bash
# DSCInitialization
oc apply -f - <<'EOF'
apiVersion: dscinitialization.opendatahub.io/v2
kind: DSCInitialization
metadata:
  name: default-dsci
spec:
  applicationsNamespace: opendatahub
  monitoring:
    managementState: Removed
    namespace: opendatahub
  trustedCABundle:
    managementState: Removed
EOF

# DataScienceCluster with aigateway + batchGateway enabled
oc apply -f - <<'EOF'
apiVersion: datasciencecluster.opendatahub.io/v2
kind: DataScienceCluster
metadata:
  name: default-dsc
spec:
  components:
    aigateway:
      managementState: Managed
      batchGateway:
        managementState: Managed
EOF
```

Check AI Gateway status:

```bash
$ oc get deployments -n opendatahub
NAME                           READY   UP-TO-DATE   AVAILABLE   AGE
ai-gateway-operator            1/1     1            1           2m
llm-d-batch-gateway-operator   1/1     1            1           90s

$ oc get aigateway
NAME                READY   REASON   VERSION
default-aigateway   True             0.0.0-dev

$ oc get aigateway default-aigateway -o yaml
apiVersion: components.platform.opendatahub.io/v1alpha1
kind: AIGateway
metadata:
  name: default-aigateway
  labels:
    platform.opendatahub.io/part-of: datasciencecluster
  ownerReferences:
  - apiVersion: datasciencecluster.opendatahub.io/v2
    kind: DataScienceCluster
    name: default-dsc
spec:
  batchGateway:
    managementState: Managed
status:
  conditions:
  - status: "True"
    type: Ready
  - status: "True"
    type: ProvisioningSucceeded
  - status: "True"
    type: DeploymentsAvailable
  phase: Ready
```

### Step 2: Set up external dependencies

LLMBatchGateway requires PostgreSQL and S3-compatible storage. Async dispatch uses either Redis/Valkey sorted-set or PostgreSQL SQL queues. For synchronous testing, use the provided script:

```bash
export NAMESPACE=batch-api
export SEAWEEDFS_ACCESS_KEY="batch-gateway-$(openssl rand -hex 16)"
export SEAWEEDFS_SECRET_KEY="$(openssl rand -hex 32)"
bash hack/setup-prereqs.sh
```

Use unique values for `SEAWEEDFS_ACCESS_KEY` and `SEAWEEDFS_SECRET_KEY`. The
setup script requires both values and stores the secret key in
`batch-gateway-secrets`. For production, use credentials from a managed Secret
and do not use the local SeaweedFS setup script.

For Redis-backed async testing, install Redis as well:

```bash
INSTALL_REDIS=true bash hack/setup-prereqs.sh
```

This creates the `$NAMESPACE` namespace and installs:
- PostgreSQL (`postgresql.$NAMESPACE.svc.cluster.local:5432`)
- SeaweedFS S3 (`seaweedfs.$NAMESPACE.svc.cluster.local:8333`, bucket: `batch-gateway`)
- `batch-gateway-secrets` Secret

The async setup also installs Redis at `redis-master.$NAMESPACE.svc.cluster.local:6379` and adds its URL to `batch-gateway-secrets`.

For production, replace PostgreSQL and S3 with your own endpoints. Configure Redis only when using the Redis async transport. For SQL async transport, ensure the referenced Secret contains `postgresql-url` and configure `spec.processor.asyncConfig.sql`.

### Step 3: Create LLMBatchGateway CR

```bash
oc apply -f - <<EOF
apiVersion: batch.llm-d.ai/v1alpha1
kind: LLMBatchGateway
metadata:
  name: batch-gateway
  namespace: $NAMESPACE
spec:
  secretRef:
    name: batch-gateway-secrets
  dbBackend: postgresql
  extraEndpoints:
  - /v1/classify
  fileStorage:
    s3:
      region: us-east-1
      endpoint: http://seaweedfs.$NAMESPACE.svc.cluster.local:8333
      accessKeyId: ${SEAWEEDFS_ACCESS_KEY}
      usePathStyle: true
      autoCreateBucket: true
  apiServer:
    replicas: 1
  processor:
    replicas: 1
    config:
      inputFileDiskBudgetPercent: 10
      progressUpdateInterval: 15s
    # Preserve synchronous dispatch for this existing-style configuration.
    dispatchMode: sync
    globalInferenceGateway:
      url: http://inference-gateway.default.svc.cluster.local:8000
      requestTimeout: 5m
      maxRetries: 3
      initialBackoff: 1s
      maxBackoff: 60s
  gc:
    interval: 30m
EOF
```

### Step 4: Check Batch Gateway status

```bash
$ oc get llmbatchgateway -n batch-api
NAMESPACE   NAME            READY   API-READY   PROC-READY   GC-READY   AGE
batch-api   batch-gateway   True    1           1            1          20s

$ oc get pods -n batch-api
NAME                                       READY   STATUS    RESTARTS   AGE
batch-gateway-apiserver-79b49fc6ff-d9fbh   1/1     Running   0          25s
batch-gateway-gc-64987bc9c4-xqrtb          1/1     Running   0          25s
batch-gateway-processor-7cc44bf4dd-m9jbk   1/1     Running   0          25s

$ oc get llmbatchgateway batch-gateway -n batch-api -o yaml
apiVersion: batch.llm-d.ai/v1alpha1
kind: LLMBatchGateway
metadata:
  name: batch-gateway
  namespace: batch-api
spec:
  apiServer:
    replicas: 1
  dbBackend: postgresql
  fileStorage:
    s3:
      accessKeyId: <generated-access-key>
      autoCreateBucket: true
      endpoint: http://seaweedfs.batch-api.svc.cluster.local:8333
      region: us-east-1
      usePathStyle: true
  gc:
    interval: 30m
  processor:
    dispatchMode: sync
    globalInferenceGateway:
      url: http://inference-gateway.default.svc.cluster.local:8000
      requestTimeout: 5m
      maxRetries: 3
      initialBackoff: 1s
      maxBackoff: 60s
    replicas: 1
  secretRef:
    name: batch-gateway-secrets
status:
  componentStatus:
    apiServer:
      readyReplicas: 1
      replicas: 1
    gc:
      readyReplicas: 1
      replicas: 1
    processor:
      readyReplicas: 1
      replicas: 1
  conditions:
  - message: API server has at least one ready replica
    reason: Available
    status: "True"
    type: APIServerAvailable
  - message: Processor has at least one ready replica
    reason: Available
    status: "True"
    type: ProcessorAvailable
  - message: GC has at least one ready replica
    reason: Available
    status: "True"
    type: GCAvailable
  - message: All components have at least one ready replica
    reason: AllComponentsReady
    status: "True"
    type: Ready
```

## Disable / Remove

```bash
# Disable AI Gateway (removes operators in reverse order)
oc patch datasciencecluster default-dsc --type merge \
    -p '{"spec":{"components":{"aigateway":{"managementState":"Removed"}}}}'

# Delete LLMBatchGateway CR and namespace
oc delete llmbatchgateway batch-gateway -n batch-api
oc delete namespace batch-api
```

## References

- [ai-gateway-operator architecture](https://github.com/opendatahub-io/ai-gateway-operator/blob/main/docs/architecture.md)
- [Development workflow](dev-workflow.md)
