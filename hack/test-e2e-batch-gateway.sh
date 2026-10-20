#!/usr/bin/env bash
# Runs batch-gateway e2e tests and verifies async dispatch via llm-d-async metrics.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

BATCH_GATEWAY_DIR="${BATCH_GATEWAY_DIR:-batch-gateway}"
TEST_RUN="${TEST_RUN:-}"
NAMESPACE="${NAMESPACE:-default}"
CR_NAME="${CR_NAME:-batch-gateway-dev}"

log()  { echo "  [INFO]  $*"; }
step() { echo ""; echo "==> $*"; }
die()  { echo "  [FATAL] $*" >&2; exit 1; }

# ── Helpers ──────────────────────────────────────────────────────────────────

PF_PIDS=()

cleanup_port_forwards() {
    for pid in "${PF_PIDS[@]}"; do
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup_port_forwards EXIT

start_port_forward() {
    local resource="$1" local_port="$2" remote_port="$3"
    kubectl port-forward -n "${NAMESPACE}" "$resource" "${local_port}:${remote_port}" &>/dev/null &
    PF_PIDS+=($!)
    local attempt
    for attempt in $(seq 1 30); do
        if curl -sf "http://localhost:${local_port}/" &>/dev/null 2>&1 || \
           nc -z localhost "${local_port}" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    die "Port-forward ${resource} ${local_port}:${remote_port} not ready after 30s"
}

ASYNC_METRICS_PF_PID=""

get_async_metrics() {
    # Start port-forward if not already running
    if [ -z "$ASYNC_METRICS_PF_PID" ] || ! kill -0 "$ASYNC_METRICS_PF_PID" 2>/dev/null; then
        kubectl port-forward -n "${NAMESPACE}" \
            "deployment/${CR_NAME}-async-processor" 19090:9090 &>/dev/null &
        ASYNC_METRICS_PF_PID=$!
        PF_PIDS+=("$ASYNC_METRICS_PF_PID")
        local attempt
        for attempt in $(seq 1 30); do
            if curl -sf http://localhost:19090/metrics &>/dev/null; then
                break
            fi
            sleep 1
        done
    fi
    curl -sf http://localhost:19090/metrics
}

get_async_metric() {
    local metric_name="$1"
    get_async_metrics | { grep "^${metric_name}" || true; } | awk '{sum+=$2} END {printf "%d", sum}'
}

get_async_transport_config() {
    local deploy_json
    deploy_json=$(kubectl get deployment "${CR_NAME}-async-processor" -n "${NAMESPACE}" -o json)
    echo "$deploy_json" | jq -r '
        .spec.template.spec.containers[0].args as $args |
        ($args | index("--transport-config")) as $index |
        if $index == null then "" else $args[$index + 1] end
    '
}

# ── Detect dispatch mode from processor configmap ────────────────────────────

dispatch_mode=$(kubectl get configmap -n "${NAMESPACE}" \
    -l "app.kubernetes.io/instance=${CR_NAME},app.kubernetes.io/component=processor" \
    -o jsonpath='{.items[0].data.config\.yaml}' | yq '.dispatch_mode // "sync"')
log "Detected dispatch mode: $dispatch_mode"

# ── Run tests ────────────────────────────────────────────────────────────────

cd "${OPERATOR_DIR}/${BATCH_GATEWAY_DIR}/test/e2e"

if [[ "$dispatch_mode" == "async" ]]; then
    # The upstream TestE2E auto-skips in async mode. Run TestDispatcher instead
    # to verify the async dispatch round-trip end-to-end.
    TEST_RUN="${TEST_RUN:-TestDispatcher/BatchThroughDispatcher}"

    step "Setting up port-forwards for async dispatcher tests..."
    start_port_forward "svc/redis-master" 6399 6379
    log "Port-forwards ready (redis:6399)"

    step "Recording async metrics before tests..."
    before_success=$(get_async_metric "llm_d_async_async_successful_requests_total")
    log "async_successful_requests_total before: $before_success"

    step "Running async dispatcher e2e tests (${TEST_RUN})..."
    # The operator deploys the CR under CR_NAME; pass that release name to the
    # batch-gateway E2E module so it finds the processor ConfigMap and pods.
    TEST_HELM_RELEASE="${CR_NAME}" go test -v -count=1 -run "${TEST_RUN}" ./...

    step "Verifying requests went through llm-d-async..."
    after_success=$(get_async_metric "llm_d_async_async_successful_requests_total")
    diff=$((after_success - before_success))

    if [ "$diff" -gt 0 ]; then
        log "Confirmed: $diff requests completed via llm-d-async ($before_success → $after_success)"
    else
        die "No requests went through llm-d-async (metric unchanged at $before_success)"
    fi

    pool_metric=$(get_async_metric "llm_d_async_async_pool_worker_limit")
    if [ "$pool_metric" -gt 0 ]; then
        log "Worker pool active: pool_worker_limit=$pool_metric"
    else
        die "Worker pool not active (async_pool_worker_limit=0)"
    fi

    # Verify gate budget if a gate type is configured
    transport_config=$(get_async_transport_config)
    gate_type=$(echo "$transport_config" | jq -r '.queues[0].gate_type // empty')
    if [ -n "$gate_type" ]; then
        budget_line=$(get_async_metrics | grep "^llm_d_async_async_dispatch_budget" | head -1)
        if [ -n "$budget_line" ]; then
            log "Gate budget: $budget_line"
        else
            die "Gate not active: async_dispatch_budget metric not found"
        fi
    fi
else
    TEST_RUN="${TEST_RUN:-TestE2E/Batches/Lifecycle}"

    step "Running batch-gateway e2e tests (${TEST_RUN})..."
    go test -v -count=1 -run "${TEST_RUN}" ./...
fi

log "All batch-gateway e2e checks passed."
