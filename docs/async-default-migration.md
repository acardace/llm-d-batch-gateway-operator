# Async Dispatch and Core Storage Migration

## Scope

This guide applies to the OpenDataHub downstream `LLMBatchGateway` operator when upgrading to a release where asynchronous dispatch is the only dispatch mode and core Batch Gateway storage uses PostgreSQL.

The upstream `llm-d` default is unchanged.

This release contains **breaking changes to the `LLMBatchGateway` spec**. Read the sections below before upgrading.

Existing synchronous deployments are **not migrated in place**. Drain them and
re-deploy as async: see [Migration path](#migration-path-drain-then-re-deploy).

## Sync Dispatch Removal

Synchronous dispatch is removed. The `dispatchMode` field remains only as a
deprecated, async-only compatibility field; `dispatchMode: sync` is rejected.
The following spec fields no longer exist:

**Dispatch mode and storage**

| Removed field | Replacement |
|---|---|
| `spec.processor.globalInferenceGateway` | `spec.processor.modelGateways[]` (see below) |
| `spec.redis` | `spec.processor.asyncConfig.redis` |
| `spec.processor.config.inferenceObjective` | `spec.processor.modelGateways[].inferenceObjective` |

`spec.processor.dispatchMode` is retained, so manifests that still set
`dispatchMode: async` keep applying: `kubectl` rejects unknown fields, so removing
it outright would break them. It is deprecated, `async` is the only accepted
value, and the operator ignores it. Setting it to `sync` is rejected, so drop the
field from new manifests. A resource that still carries `dispatchMode: sync` from
an earlier release can no longer be updated, including its status, and must be
deleted as part of the drain-and-re-deploy below.

`spec.processor.globalInferenceGateway` was a whole gateway object, not a single
URL. Model selection maps to `spec.processor.modelGateways[].inferencePoolName`,
and its `inferenceObjective` maps to `modelGateways[].inferenceObjective`. There
is no replacement for its transport tuning knobs, which were sync-only.

`spec.processor.config.inferenceObjective` was already deprecated as a
process-wide default; it is now removed rather than defaulted onto each
`modelGateways[]` entry, so set `inferenceObjective` per model explicitly.

**Per-gateway transport tuning** — these lived on the shared gateway type, so
they were accepted under both `globalInferenceGateway` and every
`modelGateways[]` entry:

| Removed field | Replacement |
|---|---|
| `requestTimeout`, `maxRetries`, `initialBackoff`, `maxBackoff` | none. Async timing is governed by `spec.processor.asyncConfig.resultPollTimeout` and `spec.processor.asyncConfig.drainTimeout`; the dispatcher keeps its own defaults. |
| `tlsCACertFile`, `tlsClientCertFile`, `tlsClientKeyFile` | none per gateway. Configure gateway TLS with `spec.processor.asyncConfig.tls` (Secret-backed keys). |
| `tlsInsecureSkipVerify` | none. It was already rejected by validation; use `spec.processor.asyncConfig.tls.insecureSkipVerify` only where unavoidable. |

Inside `spec.processor.asyncConfig.tls`, every key (`secretName`, `caCertKey`,
`certKey`, `keyKey`) must be non-empty when present: an explicit `""` is rejected
rather than silently dropped from the rendered configuration.

`spec.processor.asyncConfig` is now required, as is at least one
`spec.processor.modelGateways` entry with an `inferencePoolName`.

### What the API server does with a removed field

Removed fields are **pruned, not rejected**. The CRD does not preserve unknown
fields, so a manifest that still sets `globalInferenceGateway`, `spec.redis`, or
any of the per-gateway tuning knobs **applies successfully** and those keys are
silently dropped (`dispatchMode` is exempt: it still exists, so `kubectl` accepts
`async` and refuses `sync`). Read the object back to see what was
stored:

```bash
kubectl get llmbatchgateway <name> -n <namespace> -o yaml \
  | grep -E '^  redis:|globalInferenceGateway|requestTimeout|maxRetries|initialBackoff|maxBackoff|tlsCACertFile|tlsClientCertFile|tlsClientKeyFile' \
  || echo "all removed fields pruned"
```

The only *new* admission failure comes from the now-required `asyncConfig`: a
synchronous manifest prunes to a resource without it, so re-applying it fails
with

```text
spec.processor.asyncConfig: Required value
```

A resource that already exists in that state keeps reporting a
`ValidationFailed` Ready condition and is not reconciled, because sync
resources are not migrated in place — see
[Migration path](#migration-path-drain-then-re-deploy).

The async processor is always deployed, so an async queue transport is always
required: `processor.asyncConfig.redis` or `processor.asyncConfig.sql`. Redis is
only required when the Redis transport is selected.

### Migration path: drain, then re-deploy

An existing synchronous deployment is **not migrated in place**. Work in flight
under synchronous dispatch cannot be handed to the async processor, and rows
written by the synchronous pipeline are not resumable, so the supported path is
to drain the deployment and re-deploy it with an async spec:

1. **Drain.** Stop submitting new batches and wait for active batches to reach a
   terminal state. Cancel any batch that must not run to completion, and confirm
   nothing is still running:

   ```bash
   kubectl get llmbatchgateway <name> -n <namespace> -o jsonpath='{.status.conditions}'
   # and/or list non-terminal batches through the Batch API
   ```

2. **Capture the current spec** so the migration is reproducible:

   ```bash
   kubectl get llmbatchgateway <name> -n <namespace> -o yaml > <name>-sync-backup.yaml
   ```

3. **Delete the resource.** All managed components are removed with it. Batch
   metadata and files are kept in PostgreSQL and object storage, so completed
   work is not lost.

   ```bash
   kubectl delete llmbatchgateway <name> -n <namespace>
   ```

4. **Upgrade** the CRD and operator (see [Upgrade](#upgrade)).

5. **Re-deploy** the resource using an async spec:

   ```yaml
   spec:
     processor:
       modelGateways:
         llama-3:
           inferencePoolName: llama-pool
       asyncConfig:
         resultPollTimeout: 30s
   ```

   Also configure the async queue transport (`redis` or `sql`), the inference
   gateway, queue gates, and any required Prometheus metrics. See
   `config/samples/batch_v1alpha1_llmbatchgateway_async.yaml` for a complete
   example.

Resources that never used synchronous dispatch only need to keep their existing
`modelGateways` and `asyncConfig` sections.

## Core Storage Migration

This release changes only the Batch Gateway core storage backend:

```text
Batch metadata and job state: Redis/Valkey -> PostgreSQL
Async request/result queues:  Redis/Valkey sorted-set (default) or PostgreSQL SQL
```

These are separate concerns. PostgreSQL stores Batch Gateway metadata and job state. Async request/result queues are selected under `processor.asyncConfig`: use `redis` for the Redis/Valkey sorted-set transport or `sql` for the PostgreSQL transport. The selected transport must be configured consistently in both Batch Gateway and llm-d-async.

Existing resources with `dbBackend: redis` or `dbBackend: valkey` fail reconciliation after the operator upgrade. The CRD now accepts only `dbBackend: postgresql`.

The operator does not migrate Redis/Valkey data to PostgreSQL automatically. For each existing resource that uses Redis or Valkey for core Batch Gateway storage, complete the following steps before upgrading the operator or its CRD:

1. Deploy PostgreSQL and create the Batch Gateway database.
2. Migrate any required Batch Gateway metadata and job state using your approved database migration process.
3. Update the referenced Secret with a `postgresql-url` entry.
4. Set `spec.dbBackend: postgresql`.
5. Keep `redis-url` and deploy Redis only when the resource uses the Redis async
   transport. The SQL async transport uses the existing `postgresql-url` Secret
   entry.

Do not treat this as only a CR field change. Starting PostgreSQL without
migrating required data creates an empty Batch Gateway database and can lose
access to existing Batches.

If a resource already uses `dbBackend: postgresql`, no core storage migration is
needed. If Redis is used only by `processor.asyncConfig.redis`, keep it when
retaining the Redis async transport. To migrate async queues to SQL, drain the
Redis-backed requests/results first, then configure `processor.asyncConfig.sql`
and the llm-d-async SQL transport together.
## Resumable Pipeline Upgrade

This release runs async batches through the resumable pipeline. There is no `resumable.enabled` field.

Rows written before the unified resumable pipeline cannot be resumed. Drain in-flight batches before upgrading. Completed and terminal batches are not affected.

The current llm-d-async chart always deploys one async processor replica. `spec.processor.asyncConfig.replicas` is accepted but ignored, including the value `0`; it does not change or suspend the async processor Deployment. Tune lease renewal, finalization concurrency, and finalization timeout with `spec.processor.config.resumable`.

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

For every resource that still uses synchronous dispatch, follow the
[drain and re-deploy](#migration-path-drain-then-re-deploy) procedure: drain its
batches, delete the resource, then recreate it with an async spec after the
upgrade. Resources that already use async dispatch need no spec change.

## Verify Before Upgrade

Confirm that every resource that will still exist after the upgrade:

- has `spec.processor.asyncConfig` with a `resultPollTimeout`,
- has at least one `spec.processor.modelGateways` entry with an `inferencePoolName`,
- sets `spec.dbBackend: postgresql`,
- does not set `spec.processor.dispatchMode: sync` (`async` is accepted and is
  the CRD default),
- sets none of the removed fields listed under
  [Sync Dispatch Removal](#sync-dispatch-removal): neither
  `globalInferenceGateway` or `spec.redis`, nor the per-gateway
  `requestTimeout`/`maxRetries`/`initialBackoff`/`maxBackoff`/`tlsCACertFile`/
  `tlsClientCertFile`/`tlsClientKeyFile`/`tlsInsecureSkipVerify`.

Any resource failing these checks is a synchronous one that must be drained and
deleted instead of upgraded in place. Do not rely on admission to catch them:
see [What the API server does with a removed field](#what-the-api-server-does-with-a-removed-field).

## Upgrade

1. Drain and delete every synchronous `LLMBatchGateway` as described in
   [Migration path](#migration-path-drain-then-re-deploy). No synchronous
   workload may be running when the new operator starts.
2. Install the new CRD, then the operator.
3. Re-create the drained resources with an async spec and verify they become
   Ready with an async-processor Deployment.

Re-created resources must provide the async configuration expected by the operator:

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

If a synchronous resource is left in place, it keeps reporting a
`ValidationFailed` Ready condition; it is not converted automatically.

## Rollback

There is no in-place rollback: the removed spec fields and the synchronous
dispatch code path are gone. To return to an earlier operator release, downgrade
the CRD and operator together, then restore the synchronous manifests from your
backup. Batches completed under async dispatch remain in PostgreSQL and object
storage; batches that were in flight when you drained must be resubmitted.

## Acceptance Checks

- Synchronous resources are drained and deleted before the upgrade; none is left
  running.
- Re-deployed resources carry `asyncConfig` and model-to-pool mappings, and
  become Ready with an async-processor Deployment.
- Manifests without `asyncConfig` are rejected at admission
  (`spec.processor.asyncConfig: Required value`).
- Removed fields are pruned rather than rejected: after applying, reading the
  object back shows no `globalInferenceGateway`, `spec.redis`, or per-gateway
  tuning knobs, while `dispatchMode: sync` is refused at admission.
- Async requests and results flow through the selected Redis/Valkey or
  PostgreSQL SQL transport.
- Async cancellation and deadline handling continue to work.
