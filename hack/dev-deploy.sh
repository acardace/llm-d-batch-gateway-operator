#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Configuration (all overridable via env) ──────────────────────────────────

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-batch-gateway-dev}"
NAMESPACE="${NAMESPACE:-default}"
OPERATOR_IMG="${OPERATOR_IMG:-localhost/batch-gw-operator:dev}"

POSTGRESQL_PASSWORD="${POSTGRESQL_PASSWORD:-postgres}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"

APISERVER_IMG="${APISERVER_IMG:-ghcr.io/llm-d-incubation/batch-gateway-apiserver:latest}"
PROCESSOR_IMG="${PROCESSOR_IMG:-ghcr.io/llm-d-incubation/batch-gateway-processor:latest}"
GC_IMG="${GC_IMG:-ghcr.io/llm-d-incubation/batch-gateway-gc:latest}"
VLLM_SIM_IMG="${VLLM_SIM_IMG:-ghcr.io/llm-d/llm-d-inference-sim:latest}"
ASYNC_PROCESSOR_IMG="${ASYNC_PROCESSOR_IMG:-ghcr.io/llm-d-incubation/llm-d-async:v0.7.0-RC3}"
# Set to "true" to deploy the async processor component alongside the batch gateway.
DEPLOY_ASYNC_PROCESSOR="${DEPLOY_ASYNC_PROCESSOR:-false}"

# Port configuration (matches batch-gateway defaults)
APISERVER_NODE_PORT="${APISERVER_NODE_PORT:-30080}"
APISERVER_OBS_NODE_PORT="${APISERVER_OBS_NODE_PORT:-30081}"
PROCESSOR_NODE_PORT="${PROCESSOR_NODE_PORT:-30090}"
LOCAL_PORT="${LOCAL_PORT:-8000}"
LOCAL_OBS_PORT="${LOCAL_OBS_PORT:-8081}"
LOCAL_PROCESSOR_PORT="${LOCAL_PROCESSOR_PORT:-9090}"

# ── Logging ──────────────────────────────────────────────────────────────────

log()  { echo "  [INFO]  $*"; }
step() { echo ""; echo "==> $*"; }
warn() { echo "  [WARN]  $*" >&2; }
die()  { echo "  [FATAL] $*" >&2; exit 1; }

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
    local missing=()
    for cmd in kubectl helm kind kustomize curl; do
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
    hostPort: ${LOCAL_PORT}
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

install_postgresql() {
    step "Installing PostgreSQL..."

    if ! helm repo list 2>/dev/null | grep -q bitnami; then
        helm repo add bitnami https://charts.bitnami.com/bitnami
    fi
    helm repo update bitnami || warn "Helm repo update failed; continuing."

    if helm status postgresql -n "${NAMESPACE}" &>/dev/null; then
        log "PostgreSQL already installed. Skipping."
        return
    fi

    helm install postgresql bitnami/postgresql \
        --namespace "${NAMESPACE}" \
        --set auth.postgresPassword="${POSTGRESQL_PASSWORD}" \
        --set primary.persistence.enabled=false \
        --wait --timeout 120s

    log "PostgreSQL installed."
}

install_redis() {
    step "Installing Redis..."

    if helm status redis -n "${NAMESPACE}" &>/dev/null; then
        log "Redis already installed. Skipping."
        return
    fi

    helm install redis bitnami/redis \
        --namespace "${NAMESPACE}" \
        --set auth.enabled=false \
        --set replica.replicaCount=0 \
        --set master.persistence.enabled=false \
        --wait --timeout 120s

    log "Redis installed."
}

install_minio() {
    step "Installing MinIO..."

    if kubectl get deployment minio -n "${NAMESPACE}" &>/dev/null; then
        log "MinIO already exists. Skipping."
        return
    fi

    kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args: ["server", "/data", "--console-address", ":9001"]
        env:
        - name: MINIO_ROOT_USER
          value: "${MINIO_ACCESS_KEY}"
        - name: MINIO_ROOT_PASSWORD
          value: "${MINIO_SECRET_KEY}"
        ports:
        - containerPort: 9000
          name: api
        readinessProbe:
          httpGet:
            path: /minio/health/ready
            port: 9000
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  selector:
    app: minio
  ports:
  - name: api
    port: 9000
    targetPort: 9000
EOF

    kubectl rollout status deployment minio -n "${NAMESPACE}" --timeout=120s
    log "MinIO installed."
}

install_gateway_api_crds() {
    step "Installing Gateway API CRDs..."

    local version="${GATEWAY_API_VERSION:-}"
    if [ -z "${version}" ]; then
        local curl_args=(-fsSL)
        if [ -n "${GITHUB_TOKEN:-}" ]; then
            curl_args+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
        fi
        version=$(curl "${curl_args[@]}" https://api.github.com/repos/kubernetes-sigs/gateway-api/releases/latest \
            | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n1)
    fi

    if [ -z "${version}" ]; then
        die "Could not determine Gateway API release version (set GATEWAY_API_VERSION to override)."
    fi

    log "Gateway API version: ${version}"

    kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${version}/standard-install.yaml"

    log "Gateway API CRDs installed."
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

create_secret() {
    step "Creating batch-gateway-secrets..."

    local redis_url="redis://redis-master.${NAMESPACE}.svc.cluster.local:6379/0"
    local postgresql_url="postgresql://postgres:${POSTGRESQL_PASSWORD}@postgresql.${NAMESPACE}.svc.cluster.local:5432/postgres"

    kubectl create secret generic batch-gateway-secrets \
        --namespace "${NAMESPACE}" \
        --from-literal=redis-url="${redis_url}" \
        --from-literal=postgresql-url="${postgresql_url}" \
        --from-literal=inference-api-key="dummy-api-key" \
        --from-literal=s3-secret-access-key="${MINIO_SECRET_KEY}" \
        --dry-run=client -o yaml | kubectl apply -f -

    log "Secret created."
}

# ── Operator ─────────────────────────────────────────────────────────────────

build_operator() {
    step "Building operator image '${OPERATOR_IMG}'..."
    cd "${OPERATOR_DIR}"
    local build_args=(-t "${OPERATOR_IMG}" -f Dockerfile)
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

    kubectl create namespace batch-gw-operator-system 2>/dev/null || true
    make install
    IMG="${OPERATOR_IMG}" make deploy

    kubectl rollout status deployment -l control-plane=controller-manager \
        -n batch-gw-operator-system --timeout=120s

    log "Operator deployed."
}

apply_cr() {
    step "Applying dev LLMBatchGateway CR..."
    cd "${OPERATOR_DIR}"

    kubectl apply -f config/samples/dev.yaml -n "${NAMESPACE}"

    if [ "${DEPLOY_ASYNC_PROCESSOR}" = "true" ]; then
        log "Patching CR to enable async processor (image: ${ASYNC_PROCESSOR_IMG})..."
        kubectl patch llmbatchgateway batch-gateway -n "${NAMESPACE}" --type merge -p \
            "{\"spec\":{\"asyncProcessor\":{\"image\":\"${ASYNC_PROCESSOR_IMG}\",\"concurrency\":8,\"requestTimeout\":\"5m\",\"pollIntervalMs\":1000,\"batchSize\":10}}}"
    fi

    log "CR applied. Operator will reconcile and create batch-gateway components."
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
    app.kubernetes.io/name: batch-gateway-apiserver
    app.kubernetes.io/instance: batch-gateway
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
    app.kubernetes.io/name: batch-gateway-processor
    app.kubernetes.io/instance: batch-gateway
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

# ── Wait & Verify ────────────────────────────────────────────────────────────

wait_for_batch_gateway() {
    step "Waiting for batch-gateway components..."

    local timeout=180
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local ready
        ready=$(kubectl get deployments -n "${NAMESPACE}" \
            -l "app.kubernetes.io/instance=batch-gateway" \
            -o jsonpath='{range .items[*]}{.status.readyReplicas}{"\n"}{end}' 2>/dev/null | grep -c "^[1-9]" || true)

        local expected=3
        [ "${DEPLOY_ASYNC_PROCESSOR}" = "true" ] && expected=4

        if [ "$ready" -ge "$expected" ]; then
            log "All batch-gateway components are ready."
            return
        fi

        sleep 5
        elapsed=$((elapsed + 5))
        log "Waiting... ($elapsed/${timeout}s, $ready/$expected deployments ready)"
    done

    warn "Timed out waiting for batch-gateway components. Showing current state:"
    kubectl get pods -n "${NAMESPACE}"
}

print_status() {
    step "Deployment complete!"
    echo ""
    echo "  Operator:"
    kubectl get pods -n batch-gw-operator-system
    echo ""
    echo "  Batch Gateway:"
    kubectl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=batch-gateway"
    echo ""
    echo "  CR Status:"
    kubectl get llmbatchgateway -n "${NAMESPACE}" 2>/dev/null || true
    echo ""
    echo "  Access:"
    echo "    API Server:  http://localhost:${LOCAL_PORT}"
    echo "    Observability: http://localhost:${LOCAL_OBS_PORT}"
    echo "    Processor:   http://localhost:${LOCAL_PROCESSOR_PORT}"
    if [ "${DEPLOY_ASYNC_PROCESSOR}" = "true" ]; then
        echo "    Async Processor: enabled (image: ${ASYNC_PROCESSOR_IMG})"
    fi
    echo ""
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
    install_postgresql
    install_redis
    install_minio
    install_vllm_sim
    create_secret
    load_operator
    deploy_operator
    apply_cr
    wait_for_batch_gateway
    create_nodeport_services
    print_status
}

main "$@"
