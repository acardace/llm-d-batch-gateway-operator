# Async Dispatch and Core Storage Migration

## Scope

This guide applies to the OpenDataHub downstream `LLMBatchGateway` operator when upgrading to a release where new resources default to asynchronous dispatch and core Batch Gateway storage uses PostgreSQL.

The upstream `llm-d` default is unchanged.

## Core Storage Migration

This release changes only the Batch Gateway core storage backend:

```text
Batch metadata and job state: Redis/Valkey -> PostgreSQL
Async request/result queues:  Redis remains required for async dispatch
```

These are separate uses of Redis. PostgreSQL stores Batch Gateway metadata and job state. Redis under `processor.asyncConfig` is still used by the async processor for request/result queues and dispatch gates.

Existing resources with `dbBackend: redis`, `dbBackend: valkey`, or the old top-level `spec.redis` field fail reconciliation after the operator upgrade. The CRD now accepts only `dbBackend: postgresql`.

The operator does not migrate Redis/Valkey data to PostgreSQL automatically. For each existing resource that uses Redis or Valkey for core Batch Gateway storage, complete the following steps before upgrading the operator or its CRD:

1. Deploy PostgreSQL and create the Batch Gateway database.
2. Migrate any required Batch Gateway metadata and job state using your approved database migration process.
3. Update the referenced Secret with a `postgresql-url` entry.
4. Set `spec.dbBackend: postgresql` and remove the old top-level `spec.redis` field.
5. Keep `redis-url` and deploy Redis only when the resource uses async dispatch.

Do not treat this as only a CR field change. Starting PostgreSQL without migrating required data creates an empty Batch Gateway database and can lose access to existing Batches.

If a resource already uses `dbBackend: postgresql`, no core storage migration is needed; remove any unused top-level `spec.redis` field before upgrading. Do not move that field into `processor.asyncConfig`; async Redis connection and queue settings are configured separately.

If Redis is used only by `processor.asyncConfig`, keep it. That async Redis setup does not need to move to PostgreSQL.

## Resulting Behavior

```text
New LLMBatchGateway without dispatchMode -> async
Existing resource with dispatchMode: sync -> sync
Existing resource relying on implicit sync -> add dispatchMode: sync before upgrade
Explicit dispatchMode: async -> async
```

## Why Migration Is Required

Synchronous configurations commonly use `globalInferenceGateway` or synchronous model gateway URLs. Async configurations require model-to-InferencePool mappings, Redis-backed queues, and an `asyncConfig` section.

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
```

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

Also configure the async processor's Redis connection, inference gateway, queue gates, and any required Prometheus metrics.

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
- Async requests and results flow through Redis.
- Explicit sync remains supported.
- Async cancellation and deadline handling continue to work.
