#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration (all overridable via env) ──────────────────────────────────

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-batch-gateway-dev}"
NAMESPACE="${NAMESPACE:-default}"
# Operator namespace, must match config/default/kustomization.yaml's `namespace:` field.
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-llm-d-batch-gateway-operator-system}"
OPERATOR_IMG="${OPERATOR_IMG:-localhost/llm-d-batch-gateway-operator:dev}"

GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-}"
PROMETHEUS_OPERATOR_VERSION="${PROMETHEUS_OPERATOR_VERSION:-}"

# Dev profile selects which config/samples/dev-<profile>.yaml CR to apply.
# Available: sync (default), async, async-gate-redis, async-gate-prometheus-query,
#            async-gate-prometheus-budget, async-gate-endpoint-scrape.
DEV_PROFILE="${DEV_PROFILE:-sync}"

# read from params.env if you wanna test a different image, go update params.env
PARAMS_ENV="${OPERATOR_DIR}/config/base/params.env"
param_image() { grep -E "^$1=" "${PARAMS_ENV}" 2>/dev/null | head -n1 | cut -d= -f2- || true; }
APISERVER_IMG="${APISERVER_IMG:-$(param_image LLM_D_BATCH_GATEWAY_APISERVER_IMAGE)}"
PROCESSOR_IMG="${PROCESSOR_IMG:-$(param_image LLM_D_BATCH_GATEWAY_PROCESSOR_IMAGE)}"
GC_IMG="${GC_IMG:-$(param_image LLM_D_BATCH_GATEWAY_GC_IMAGE)}"
ASYNC_IMG="${ASYNC_IMG:-$(param_image LLM_D_ASYNC_IMAGE)}"
VLLM_SIM_IMG="${VLLM_SIM_IMG:-ghcr.io/llm-d/llm-d-inference-sim:latest}"

random_hex() {
    od -An -N16 -tx1 /dev/urandom | tr -d ' \n'
}

# Generate credentials for this local deployment unless the caller supplies them.
# setup-prereqs.sh requires both values and applies them to SeaweedFS on every run.
SEAWEEDFS_ACCESS_KEY="${SEAWEEDFS_ACCESS_KEY:-batch-gateway-$(random_hex)}"
SEAWEEDFS_SECRET_KEY="${SEAWEEDFS_SECRET_KEY:-$(random_hex)}"

# Port configuration (matches batch-gateway defaults)
APISERVER_NODE_PORT="${APISERVER_NODE_PORT:-30080}"
APISERVER_OBS_NODE_PORT="${APISERVER_OBS_NODE_PORT:-30081}"
PROCESSOR_NODE_PORT="${PROCESSOR_NODE_PORT:-30090}"
LOCAL_APISERVER_PORT="${LOCAL_APISERVER_PORT:-8000}"
LOCAL_OBS_PORT="${LOCAL_OBS_PORT:-8081}"
LOCAL_PROCESSOR_PORT="${LOCAL_PROCESSOR_PORT:-9090}"

# ── Utils ──────────────────────────────────────────────────────────────────
TIMEOUT="${TIMEOUT:-180s}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"

log()  { echo "  [INFO]  $*"; }
step() { echo ""; echo "==> $*"; }
warn() { echo "  [WARN]  $*" >&2; }
die()  { echo "  [FATAL] $*" >&2; exit 1; }

wait_for() {
    local desc="$1" check_cmd="$2"
    local max=${TIMEOUT%s} elapsed=0
    log "Waiting for $desc (timeout=${TIMEOUT})..."
    while [ "$elapsed" -lt "$max" ]; do
        if eval "$check_cmd" &>/dev/null; then
            return 0
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done
    return 1
}

wait_for_deployment() {
    local name="$1" ns="$2"
    if ! wait_for "deployment ${name} to exist" \
        "kubectl get deployment/${name} -n ${ns}"; then
        return 1
    fi
    kubectl wait "deployment/${name}" -n "$ns" \
        --for="condition=Available" --timeout="${TIMEOUT}"
}

wait_for_statefulset() {
    local name="$1" ns="$2"
    if ! wait_for "statefulset ${name} to exist" \
        "kubectl get statefulset/${name} -n ${ns}"; then
        return 1
    fi
    kubectl rollout status "statefulset/${name}" -n "$ns" --timeout="${TIMEOUT}"
}

# ── Prerequisites ────────────────────────────────────────────────────────────

CONTAINER_TOOL=""

detect_container_tool() {
    if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
        echo "docker"
    elif command -v podman &>/dev/null; then
        echo "podman"
    else
        die "Neither docker (running) nor podman found."
    fi
}

check_prerequisites() {
    step "Checking prerequisites..."
    if ! command -v kustomize &>/dev/null && [ -x "${OPERATOR_DIR}/bin/kustomize" ]; then
        export PATH="${OPERATOR_DIR}/bin:${PATH}"
    fi
    local missing=()
    for cmd in kubectl helm kind kustomize curl yq jq; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        die "Missing required tools: ${missing[*]}"
    fi
    CONTAINER_TOOL="$(detect_container_tool)"
    if [ "${CONTAINER_TOOL}" = "podman" ]; then
        export KIND_EXPERIMENTAL_PROVIDER=podman
    fi
    log "Container tool: ${CONTAINER_TOOL}"
}

# ── Kind Cluster ─────────────────────────────────────────────────────────────

ensure_cluster() {
    step "Ensuring Kind cluster '${KIND_CLUSTER_NAME}'..."

    if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
        log "Cluster already exists. Switching context..."
        kubectl config use-context "kind-${KIND_CLUSTER_NAME}"
    else
        kind create cluster --name "${KIND_CLUSTER_NAME}" --config=- <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: ${APISERVER_NODE_PORT}
    hostPort: ${LOCAL_APISERVER_PORT}
    protocol: TCP
  - containerPort: ${APISERVER_OBS_NODE_PORT}
    hostPort: ${LOCAL_OBS_PORT}
    protocol: TCP
  - containerPort: ${PROCESSOR_NODE_PORT}
    hostPort: ${LOCAL_PROCESSOR_PORT}
    protocol: TCP
EOF
    fi

    log "Kind cluster ready."
}

# ── Dependencies ─────────────────────────────────────────────────────────────

detect_file_storage_type() {
    local cr_file="config/samples/dev-${DEV_PROFILE}.yaml"
    if [ -f "${cr_file}" ] && yq -e '.spec.fileStorage.fs' "${cr_file}" &>/dev/null; then
        echo "fs"
    else
        echo "s3"
    fi
}

install_prereqs() {
    local install_redis=false
    if [[ "${DEV_PROFILE}" == async* ]]; then
        install_redis=true
    fi
    FILE_CLIENT_TYPE="$(detect_file_storage_type)" \
        INSTALL_REDIS="${install_redis}" \
        NAMESPACE="${NAMESPACE}" \
        SEAWEEDFS_ACCESS_KEY="${SEAWEEDFS_ACCESS_KEY}" \
        SEAWEEDFS_SECRET_KEY="${SEAWEEDFS_SECRET_KEY}" \
        bash "${SCRIPT_DIR}/setup-prereqs.sh"
}

install_gateway_api_crds() {
    step "Installing Gateway API CRDs..."

    local version="${GATEWAY_API_VERSION:-}"
    if [ -z "${version}" ]; then
        version=$(cd "${OPERATOR_DIR}" && go list -m -f '{{.Version}}' sigs.k8s.io/gateway-api)
    fi

    if [ -z "${version}" ]; then
        die "Could not determine Gateway API version from go.mod (set GATEWAY_API_VERSION to override)."
    fi

    log "Gateway API version: ${version}"

    kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${version}/standard-install.yaml"

    log "Gateway API CRDs installed."
}

install_prometheus_operator_crds() {
    step "Installing Prometheus Operator CRDs..."

    local version="${PROMETHEUS_OPERATOR_VERSION:-}"
    if [ -z "${version}" ]; then
        version=$(cd "${OPERATOR_DIR}" && go list -m -f '{{.Version}}' github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring)
    fi

    if [ -z "${version}" ]; then
        die "Could not determine Prometheus Operator version from go.mod (set PROMETHEUS_OPERATOR_VERSION to override)."
    fi

    log "Prometheus Operator version: ${version}"

    kubectl apply --server-side -f "https://github.com/prometheus-operator/prometheus-operator/releases/download/${version}/stripped-down-crds.yaml"

    log "Prometheus Operator CRDs installed."
}

install_vllm_sim() {
    step "Installing vLLM simulator..."

    if kubectl get deployment vllm-sim -n "${NAMESPACE}" &>/dev/null; then
        log "vLLM simulator already exists. Skipping."
        return
    fi

    kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vllm-sim
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vllm-sim
  template:
    metadata:
      labels:
        app: vllm-sim
    spec:
      containers:
      - name: vllm-sim
        image: ${VLLM_SIM_IMG}
        imagePullPolicy: IfNotPresent
        args:
        - --model
        - sim-model
        - --port
        - "8000"
        - --time-to-first-token=50ms
        - --inter-token-latency=100ms
        - --fake-metrics
        - '{"kv-cache-usage": 0, "waiting-requests": 0, "running-requests": 0}'
        ports:
        - containerPort: 8000
          name: http
        resources:
          requests:
            cpu: 10m
---
apiVersion: v1
kind: Service
metadata:
  name: vllm-sim
spec:
  selector:
    app: vllm-sim
  ports:
  - name: http
    port: 8000
    targetPort: 8000
EOF

    kubectl rollout status deployment vllm-sim -n "${NAMESPACE}" --timeout=120s
    log "vLLM simulator installed."
}

# ── Gate Dependencies ───────────────────────────────────────────────────────

# Gate types control when async-processor dispatches requests from the queue:
#   redis             — reads a budget key from Redis; external systems set the value to open/close the gate
#   prometheus-query   — evaluates a PromQL expression via Prometheus as the dispatch budget
#   prometheus-budget  — cascades two Prometheus metric sources (primary: queue_size, fallback: vllm_running)
#   endpoint-scrape   — directly scrapes a model server's /metrics endpoint to compute budget
# All gates use a budget value in [0,1]: >0 dispatches, <=0 refuses.
# See: https://github.com/llm-d-incubation/llm-d-async/blob/main/pkg/async/inference/flowcontrol/gate_factory.go
prepare_gate_deps() {
    case "${DEV_PROFILE}" in
        sync|sync-fs|async)
            ;;
        async-gate-redis)
            prepare_gate_redis
            ;;
        async-gate-prometheus-query)
            prepare_gate_prometheus
            ;;
        async-gate-prometheus-budget)
            prepare_gate_prometheus
            ;;
        async-gate-endpoint-scrape)
            prepare_gate_scrape
            ;;
        *)
            die "Unknown DEV_PROFILE: ${DEV_PROFILE}"
            ;;
    esac
}

prepare_gate_redis() {
    step "Setting Redis dispatch gate budget..."
    wait_for "redis-master-0 to be ready" \
        "kubectl get pod redis-master-0 -n ${NAMESPACE} -o jsonpath='{.status.phase}' | grep -q Running"
    # The redis gate reads this key and applies a binary decision:
    #   budget > 0  → dispatch (requests are processed)
    #   budget <= 0 → refuse  (requests stay in queue)
    # The value is exposed via the async_dispatch_budget metric.
    # See: https://github.com/llm-d-incubation/llm-d-async/blob/main/pkg/redis/dispatch_gate.go
    kubectl exec redis-master-0 -n "${NAMESPACE}" -- redis-cli SET dispatch-gate-budget 0.1
    log "Redis gate budget set to 0.1."
}

prepare_gate_scrape() {
    # The endpoint-scrape gate directly scrapes a model server's /metrics endpoint
    # and computes dispatch budget from a specific metric.
    # In dev-async-gate-scrape.yaml:
    #   metric: vllm:num_requests_waiting, max_count_per_pod: 5
    #   budget = 1 - (waiting / max_count_per_pod)
    # vllm-sim --fake-metrics exposes waiting-requests=0, so budget = 1 - 0/5 = 1.0 (fully open).
    # No extra setup needed — vllm-sim already has --fake-metrics enabled in install_vllm_sim.
    # See: https://github.com/llm-d-incubation/llm-d-async/blob/main/pkg/async/inference/flowcontrol/gate_factory.go
    step "Verifying vllm-sim /metrics endpoint..."
    wait_for "vllm-sim metrics" \
        "kubectl port-forward -n ${NAMESPACE} deployment/vllm-sim 18000:8000 &>/dev/null & sleep 2; curl -sf http://localhost:18000/metrics | grep -q vllm:num_requests_waiting; kill %1 2>/dev/null"
    log "vllm-sim /metrics endpoint is ready."
}

prepare_gate_prometheus() {
    # The prometheus-query gate evaluates a user-supplied PromQL expression as the dispatch budget.
    # The expression must return a single value in [0, 1]:
    #   budget > 0  → dispatch
    #   budget <= 0 → refuse
    # In dev-async-gate-prometheus-query.yaml the query is:
    #   "1 - clamp_max(vllm:num_requests_waiting / 5, 1)"
    # This computes budget based on vllm-sim's waiting requests metric.
    # vllm-sim --fake-metrics exposes waiting-requests=0, so budget = 1 - 0/5 = 1.0 (fully open).
    # See: https://github.com/llm-d-incubation/llm-d-async/blob/main/pkg/async/inference/flowcontrol/gate_factory.go
    step "Installing Prometheus for gate testing..."

    if kubectl get deployment prometheus -n "${NAMESPACE}" &>/dev/null; then
        log "Prometheus already exists. Skipping install."
    else
        kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  prometheus.yml: |
    global:
      scrape_interval: 5s
    scrape_configs:
    - job_name: 'vllm-sim'
      metrics_path: /metrics
      static_configs:
      - targets: ['vllm-sim.${NAMESPACE}.svc.cluster.local:8000']
        labels:
          component: vllm-sim
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
      - name: prometheus
        image: prom/prometheus:v3.5.0
        args:
        - --config.file=/etc/prometheus/prometheus.yml
        - --storage.tsdb.retention.time=1h
        ports:
        - containerPort: 9090
        volumeMounts:
        - name: config
          mountPath: /etc/prometheus
      volumes:
      - name: config
        configMap:
          name: prometheus-config
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
spec:
  selector:
    app: prometheus
  ports:
  - port: 9090
    targetPort: 9090
EOF
        kubectl rollout status deployment prometheus -n "${NAMESPACE}" --timeout=120s
    fi

    step "Verifying Prometheus can scrape vllm-sim metrics..."
    wait_for "vllm:num_requests_waiting in Prometheus" \
        "kubectl port-forward -n ${NAMESPACE} deployment/prometheus 19090:9090 &>/dev/null & sleep 2; curl -sf 'http://localhost:19090/api/v1/query?query=vllm:num_requests_waiting' | grep -q success; kill %1 2>/dev/null"
    log "Prometheus is scraping vllm-sim metrics."
}

# ── Operator ─────────────────────────────────────────────────────────────────

build_operator() {
    step "Building operator image '${OPERATOR_IMG}'..."
    cd "${OPERATOR_DIR}"
    local build_args=(--no-cache -t "${OPERATOR_IMG}" -f Dockerfile.konflux)
    if [ "${CONTAINER_TOOL}" = "podman" ]; then
        build_args+=(--ignorefile Dockerfile.dockerignore)
    fi
    ${CONTAINER_TOOL} build "${build_args[@]}" .
    log "Operator image built."
}

load_operator() {
    step "Loading operator image into Kind..."
    if [ "${CONTAINER_TOOL}" = "podman" ]; then
        podman save "${OPERATOR_IMG}" | kind load image-archive /dev/stdin --name "${KIND_CLUSTER_NAME}"
    else
        kind load docker-image "${OPERATOR_IMG}" --name "${KIND_CLUSTER_NAME}"
    fi
    log "Operator image loaded."
}

deploy_operator() {
    step "Installing CRD and deploying operator..."
    cd "${OPERATOR_DIR}"

    kubectl create namespace "${OPERATOR_NAMESPACE}" 2>/dev/null || true
    make install
    IMG="${OPERATOR_IMG}" make deploy

    # override the env vars on the deployment to pin the images dev wants (defaults read from params.env.
    step "Setting component images on the operator deployment as env variable..."
    kubectl set env deployment/llm-d-batch-gateway-operator -n "${OPERATOR_NAMESPACE}" \
        LLM_D_BATCH_GATEWAY_APISERVER_IMAGE="${APISERVER_IMG}" \
        LLM_D_BATCH_GATEWAY_PROCESSOR_IMAGE="${PROCESSOR_IMG}" \
        LLM_D_BATCH_GATEWAY_GC_IMAGE="${GC_IMG}" \
        LLM_D_ASYNC_IMAGE="${ASYNC_IMG}"

    kubectl rollout status deployment/llm-d-batch-gateway-operator \
        -n "${OPERATOR_NAMESPACE}" --timeout=120s

    log "Operator deployed."
}

# ── Wait & Verify ────────────────────────────────────────────────────────────

apply_and_verify_cr() {
    cd "${OPERATOR_DIR}"

    local cr_file="config/samples/dev-${DEV_PROFILE}.yaml"

    # Apply CR
    step "Applying dev LLMBatchGateway CR (${cr_file})..."
    if [ ! -f "${cr_file}" ]; then
        die "Dev profile CR not found: ${cr_file} (DEV_PROFILE=${DEV_PROFILE})"
    fi
    if [[ "$(detect_file_storage_type)" == "s3" ]]; then
        SEAWEEDFS_ACCESS_KEY="${SEAWEEDFS_ACCESS_KEY}" \
            yq eval '.spec.fileStorage.s3.accessKeyId = strenv(SEAWEEDFS_ACCESS_KEY)' "${cr_file}" | \
            kubectl apply -f - -n "${NAMESPACE}"
    else
        kubectl apply -f "${cr_file}" -n "${NAMESPACE}"
    fi

    local cr_name
    cr_name=$(yq '.metadata.name' "$cr_file")

    # 1. Determine expected dispatch mode from profile name
    local expected_dispatch="sync"
    if [[ "${DEV_PROFILE}" == async* ]]; then
        expected_dispatch="async"
    fi

    # 2. Verify CR's dispatchMode matches
    local cr_dispatch_mode
    cr_dispatch_mode=$(kubectl get llmbatchgateway "${cr_name}" -n "${NAMESPACE}" \
        -o jsonpath='{.spec.processor.dispatchMode}')
    if [[ "$cr_dispatch_mode" != "$expected_dispatch" ]]; then
        die "CR dispatchMode is '$cr_dispatch_mode', expected '$expected_dispatch' (profile=${DEV_PROFILE})"
    fi
    log "CR dispatch mode verified: $cr_dispatch_mode"

    # 3. Wait for workloads
    for dep in "${cr_name}-apiserver" "${cr_name}-gc"; do
        if ! wait_for_deployment "${dep}" "${NAMESPACE}"; then
            kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${cr_name}"
            return 1
        fi
    done
    if ! wait_for_statefulset "${cr_name}-processor" "${NAMESPACE}"; then
        kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${cr_name}"
        return 1
    fi
    if [[ "$expected_dispatch" == "async" ]]; then
        if ! wait_for_deployment "${cr_name}-async-processor" "${NAMESPACE}"; then
            kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${cr_name}"
            return 1
        fi
    fi

    # 4. Wait for CR Ready
    if ! wait_for "CR ${cr_name} Ready" \
        "kubectl get llmbatchgateway ${cr_name} -n ${NAMESPACE} -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}' | grep -q True"; then
        die "CR ${cr_name} not Ready"
    fi

    # 5. Verify processor configmap dispatch_mode matches
    local processor_cm_data
    processor_cm_data=$(kubectl get configmap -n "${NAMESPACE}" \
        -l "app.kubernetes.io/instance=${cr_name},app.kubernetes.io/component=processor" \
        -o jsonpath='{.items[0].data.config\.yaml}')
    local cm_dispatch
    cm_dispatch=$(echo "$processor_cm_data" | yq '.dispatch_mode // "sync"')
    if [[ "$cm_dispatch" != "$expected_dispatch" ]]; then
        die "Processor configmap dispatch_mode is '$cm_dispatch', expected '$expected_dispatch'"
    fi
    log "Processor configmap dispatch mode verified: $cm_dispatch"

    # 6. Verify async-processor configuration from deployment spec
    if [[ "$expected_dispatch" == "async" ]]; then
        local deploy_json
        deploy_json=$(kubectl get deployment "${cr_name}-async-processor" -n "${NAMESPACE}" -o json)
        local queues_json
        queues_json=$(echo "$deploy_json" | jq -r '.spec.template.spec.containers[0].args[]' | grep '^\[')

        # Verify gate type if a gate profile is deployed
        if [[ "${DEV_PROFILE}" == async-gate-* ]]; then
            local expected_gate="${DEV_PROFILE##*gate-}"
            local actual_gate
            actual_gate=$(echo "$queues_json" | jq -r '.[0].gate_type')

            if [[ "$actual_gate" == "$expected_gate" ]]; then
                log "Queue gate type verified: $actual_gate"
            else
                die "Expected queue gate type '$expected_gate', got '$actual_gate'"
            fi
        fi

        # Verify worker pool from queuesConfig args matches CR
        local expected_pool_id
        expected_pool_id=$(yq '.spec.processor.asyncConfig.redis.queuesConfig[0].workerPoolID' "$cr_file")
        local pool_id
        pool_id=$(echo "$queues_json" | jq -r '.[0].worker_pool_id')
        if [[ -z "$expected_pool_id" || "$expected_pool_id" == "null" ]]; then
            log "Worker pool ID not set in CR, using API default: $pool_id"
        elif [[ "$pool_id" == "$expected_pool_id" ]]; then
            log "Worker pool verified: $pool_id"
        else
            die "Expected worker_pool_id '$expected_pool_id', got '$pool_id'"
        fi

        # Verify pool gate from ConfigMap matches CR
        local expected_pool_gate
        expected_pool_gate=$(yq '.spec.processor.asyncConfig.workerPools[0].gateType' "$cr_file")
        if [[ -n "$expected_pool_gate" && "$expected_pool_gate" != "null" ]]; then
            local actual_pool_gate
            actual_pool_gate=$(kubectl get configmap "${cr_name}-async-processor-config" -n "${NAMESPACE}" \
                -o jsonpath='{.data.worker-pools\.json}' \
                | jq -r '.[0].gate_type')
            if [[ "$actual_pool_gate" == "$expected_pool_gate" ]]; then
                log "Pool gate verified: $actual_pool_gate"
            else
                die "Expected pool gate '$expected_pool_gate', got '$actual_pool_gate'"
            fi
        fi
    fi

    log "All batch-gateway components are ready."
}


# ── NodePort Services ────────────────────────────────────────────────────────

create_nodeport_services() {
    step "Creating NodePort services for local access..."

    kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: batch-gateway-apiserver-nodeport
spec:
  type: NodePort
  selector:
    app.kubernetes.io/component: apiserver
  ports:
  - name: http
    protocol: TCP
    port: 8000
    targetPort: http
    nodePort: ${APISERVER_NODE_PORT}
  - name: observability
    protocol: TCP
    port: 8081
    targetPort: observability
    nodePort: ${APISERVER_OBS_NODE_PORT}
---
apiVersion: v1
kind: Service
metadata:
  name: batch-gateway-processor-nodeport
spec:
  type: NodePort
  selector:
    app.kubernetes.io/component: processor
  ports:
  - name: metrics
    protocol: TCP
    port: 9090
    targetPort: metrics
    nodePort: ${PROCESSOR_NODE_PORT}
EOF

    log "NodePort services created."
}

print_status() {
    step "Deployment complete!"

    echo "----------------------------------------"
    echo "  Operator (${OPERATOR_NAMESPACE}):"
    kubectl get all -n "${OPERATOR_NAMESPACE}"

    echo "----------------------------------------"
    echo "  CR Status:"
    kubectl get llmbatchgateway -n "${NAMESPACE}"

    echo "----------------------------------------"
    echo "  Workloads (${NAMESPACE}):"
    kubectl get all -n "${NAMESPACE}"

    echo "----------------------------------------"
    echo "  Access:"
    echo "    API Server:  http://localhost:${LOCAL_APISERVER_PORT}"
    echo "    Observability: http://localhost:${LOCAL_OBS_PORT}"
    echo "    Processor:   http://localhost:${LOCAL_PROCESSOR_PORT}"

    echo "----------------------------------------"
    echo "  Cleanup:"
    echo "    make dev-clean        # remove operator + deps"
    echo "    make dev-rm-cluster   # delete Kind cluster"
}

# ── Main ─────────────────────────────────────────────────────────────────────

main() {
    echo ""
    echo "  ╔══════════════════════════════════════════════╗"
    echo "  ║   Batch Gateway Operator - Dev Deployment    ║"
    echo "  ╚══════════════════════════════════════════════╝"
    echo ""

    check_prerequisites
    build_operator
    ensure_cluster
    install_gateway_api_crds
    install_prometheus_operator_crds
    install_prereqs
    install_vllm_sim
    prepare_gate_deps
    load_operator
    deploy_operator
    apply_and_verify_cr
    create_nodeport_services
    print_status
}

main "$@"
