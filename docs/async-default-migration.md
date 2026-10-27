# Async Dispatch and Core Storage Migration

## Scope

This guide applies to the OpenDataHub downstream `LLMBatchGateway` operator when upgrading to a release where new resources default to asynchronous dispatch and core Batch Gateway storage uses PostgreSQL.

The upstream `llm-d` default is unchanged.

## Core Storage Migration

This release changes only the Batch Gateway core storage backend:

```text
Batch metadata and job state: Redis/Valkey -> PostgreSQL
Async request/result queues:  Redis/Valkey sorted-set (default) or PostgreSQL SQL
```

These are separate concerns. PostgreSQL stores Batch Gateway metadata and job state. Async request/result queues are selected under `processor.asyncConfig`: use `redis` for the Redis/Valkey sorted-set transport or `sql` for the PostgreSQL transport. The selected transport must be configured consistently in both Batch Gateway and llm-d-async.

Existing resources with `dbBackend: redis`, `dbBackend: valkey`, or the old top-level `spec.redis` field fail reconciliation after the operator upgrade. The CRD now accepts only `dbBackend: postgresql`.

The operator does not migrate Redis/Valkey data to PostgreSQL automatically. For each existing resource that uses Redis or Valkey for core Batch Gateway storage, complete the following steps before upgrading the operator or its CRD:

1. Deploy PostgreSQL and create the Batch Gateway database.
2. Migrate any required Batch Gateway metadata and job state using your approved database migration process.
3. Update the referenced Secret with a `postgresql-url` entry.
4. Set `spec.dbBackend: postgresql` and remove the old top-level `spec.redis` field.
5. Keep `redis-url` and deploy Redis only when the resource uses the Redis async transport. The SQL async transport uses the existing `postgresql-url` Secret entry.

Do not treat this as only a CR field change. Starting PostgreSQL without migrating required data creates an empty Batch Gateway database and can lose access to existing Batches.

If a resource already uses `dbBackend: postgresql`, no core storage migration is needed; remove any unused top-level `spec.redis` field before upgrading. Do not move that field into `processor.asyncConfig`; async Redis connection and queue settings are configured separately.

If Redis is used only by `processor.asyncConfig.redis`, keep it when retaining the Redis async transport. It does not need to move to PostgreSQL. To migrate async queues to SQL, drain the Redis-backed requests/results first, then configure `processor.asyncConfig.sql` and the llm-d-async SQL transport together.

## Resumable Pipeline Upgrade

This release runs async batches through the resumable pipeline. There is no `resumable.enabled` field; `spec.processor.dispatchMode` still selects the request transport.

Rows written before the unified resumable pipeline cannot be resumed. Drain in-flight batches before upgrading. Completed and terminal batches are not affected.

The current llm-d-async chart always deploys one async processor replica. `spec.processor.asyncConfig.replicas` is accepted but ignored, including the value `0`; it does not change or suspend the async processor Deployment. Tune lease renewal, finalization concurrency, and finalization timeout with `spec.processor.config.resumable`.

## Resulting Behavior

```text
New LLMBatchGateway without dispatchMode -> async
Existing resource with dispatchMode: sync -> sync
Existing resource relying on implicit sync -> add dispatchMode: sync before upgrade
Explicit dispatchMode: async -> async
```

## Why Migration Is Required

Synchronous configurations commonly use `globalInferenceGateway` or synchronous model gateway URLs. Async configurations require model-to-InferencePool mappings, a transport-specific queue configuration, and an `asyncConfig` section.

Do not allow an existing sync resource to acquire the async default accidentally during an upgrade.

## Before Upgrading

List the existing resources:

```bash
kubectl get llmbatchgateways.batch.llm-d.ai --all-namespaces
```

Inspect each resource:

```bash
kubectl get llmbatchgateway <name> \
  --namespace <namespace> \
  -o yaml
```

For every resource currently using synchronous dispatch without an explicit mode, add:

```yaml
spec:
  processor:
    dispatchMode: sync
```

Preserve the existing `globalInferenceGateway` or synchronous `modelGateways` configuration.

An individual resource can be patched with:

```bash
kubectl patch llmbatchgateway <name> \
  --namespace <namespace> \
  --type=merge \
  --patch '{"spec":{"processor":{"dispatchMode":"sync"}}}'
```

The merge patch changes only `dispatchMode` and preserves the other processor fields.

## Verify Before Upgrade

Confirm that the resource contains:

```yaml
spec:
  processor:
    dispatchMode: sync
```

Confirm that the generated processor configuration contains:

```yaml
dispatch_mode: "sync"
```

Do not proceed if a production resource still relies on an omitted mode and is intended to remain synchronous.

## Upgrade

Upgrade the CRD and operator after existing synchronous resources have been made explicit.

New resources may omit `dispatchMode`, but they must provide the async configuration expected by the operator:

```yaml
spec:
  processor:
    modelGateways:
      llama-3:
        inferencePoolName: llama-pool
    asyncConfig:
      resultPollTimeout: 30s
      inferenceGateway:
        url: http://inference-gateway.default.svc:8000
      redis:
        queuesConfig:
          - name: llm-d-async:requests:llama-pool
```

An empty `redis: {}` is not enough: with no `queuesConfig` the dispatcher consumes a single queue named `request-sortedset`, not the `llm-d-async:requests:<inferencePoolName>` queue the processor submits to, and it needs `inferenceGateway` (or a per-queue `igwBaseURL`) to start. List one queue per `inferencePoolName`.

The operator validates that async resources contain `asyncConfig`, model mappings, and an `inferencePoolName` for each model.

## Migrating an Existing Resource to Async

Async migration is opt-in. Add:

```yaml
spec:
  processor:
    dispatchMode: async
    modelGateways:
      llama-3:
        inferencePoolName: llama-pool
    asyncConfig:
      resultPollTimeout: 30s
```

Choose exactly one async queue backend and configure it with the inference gateway, queue gates, and any required Prometheus metrics:

```yaml
asyncConfig:
  inferenceGateway:
    url: http://inference-gateway.default.svc:8000
  redis:               # Redis/Valkey sorted-set, existing default
    queuesConfig:
      - name: llm-d-async:requests:llama-pool
```

or:

```yaml
asyncConfig:
  inferenceGateway:
    url: http://inference-gateway.default.svc:8000
  sql:                 # PostgreSQL SQL transport
    maxConnections: 8  # per processor replica; omit for the chart default
    queuesConfig:
      - name: llm-d-async:requests:llama-pool
```

`maxConnections` caps the Batch Gateway processor's SQL pool per replica. It is
separate from the llm-d-async dispatcher's own pool
(`ap.transportConfig.max_connections`, default 32). Budget both against
PostgreSQL's `max_connections`. Explicit values must be positive; omit the field
to use the default of 8.

The Batch Gateway and llm-d-async dispatcher must use the same transport and
matching queue names. Do not configure both `redis` and `sql`.

With the SQL transport, each model gateway must resolve to its own result queue
(default `llm-d-async:results:<inferencePoolName>`), because the processor polls
one result queue per model and SQL result reads are destructive. Each model must
also use a distinct `inferencePoolName`; the processor requires one consumer per
pool. `requestQueueName` may be shared, but every model needs a distinct
`resultQueueName`. Set both queue names or neither (the operator derives defaults);
setting only one is rejected. For example:

```yaml
asyncConfig:
  sql:
    queuesConfig:
      - name: requests-shared
modelGateways:
  model-a:
    inferencePoolName: pool-a
    requestQueueName: requests-shared
    resultQueueName: results-model-a
  model-b:
    inferencePoolName: pool-b
    requestQueueName: requests-shared
    resultQueueName: results-model-b
```

Do not keep `globalInferenceGateway` when using async dispatch.

## Rollback

To return an async resource to synchronous dispatch:

```yaml
spec:
  processor:
    dispatchMode: sync
    asyncConfig: null
    modelGateways: null
    globalInferenceGateway:
      url: http://existing-gateway:8000
```

The async fields must be removed when switching to the synchronous gateway form; otherwise validation rejects a resource containing both gateway configurations.

Restore the complete synchronous gateway configuration before removing async-specific settings.

## Acceptance Checks

- Existing sync resources remain Ready after the operator upgrade.
- Existing processor ConfigMaps contain `dispatch_mode: "sync"`.
- New resources default to async.
- New async resources create an async processor.
- Async requests and results flow through the selected Redis/Valkey or PostgreSQL SQL transport.
- Explicit sync remains supported.
- Async cancellation and deadline handling continue to work.
