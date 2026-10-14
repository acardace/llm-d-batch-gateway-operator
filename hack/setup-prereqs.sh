#!/bin/bash
set -euo pipefail

# ── Setup prerequisites for LLMBatchGateway CR ──────────────────────────────
#
# Installs PostgreSQL, Redis, SeaweedFS (with bucket), and creates the
# batch-gateway-secrets Secret in the target namespace.
# Run this before creating a LLMBatchGateway CR.
#
# Usage:
#   ./hack/setup-prereqs.sh                    # install in batch-api namespace
#   NAMESPACE=my-ns ./hack/setup-prereqs.sh    # install in custom namespace
#   INSTALL_REDIS=true ./hack/setup-prereqs.sh  # also install Redis for async dispatch

# ── Configuration ────────────────────────────────────────────────────────────

NAMESPACE="${NAMESPACE:-batch-api}"

POSTGRESQL_RELEASE="${POSTGRESQL_RELEASE:-postgresql}"
POSTGRESQL_PASSWORD="${POSTGRESQL_PASSWORD:-postgres}"

REDIS_RELEASE="${REDIS_RELEASE:-redis}"
INSTALL_REDIS="${INSTALL_REDIS:-false}"

SEAWEEDFS_RELEASE="${SEAWEEDFS_RELEASE:-seaweedfs}"
SEAWEEDFS_ACCESS_KEY="${SEAWEEDFS_ACCESS_KEY:-seaweedfsadmin}"
SEAWEEDFS_SECRET_KEY="${SEAWEEDFS_SECRET_KEY:-seaweedfsadmin}"
SEAWEEDFS_BUCKET="${SEAWEEDFS_BUCKET:-batch-gateway}"

FILE_CLIENT_TYPE="${FILE_CLIENT_TYPE:-s3}"
FILES_PVC_NAME="${FILES_PVC_NAME:-batch-gateway-files}"

SECRET_NAME="${SECRET_NAME:-batch-gateway-secrets}"

# ── Helpers ──────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
step() { echo -e "${BLUE}[STEP]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*" >&2; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

wait_for_deployment() {
    local name="$1" ns="$2" timeout="${3:-180s}"
    step "Waiting for deployment '${name}' to be ready..."
    local retries=0
    while ! kubectl get deploy "${name}" -n "${ns}" &>/dev/null; do
        retries=$((retries + 1))
        if [ "$retries" -ge 30 ]; then
            die "Deployment '${name}' not visible after 60s"
        fi
        sleep 2
    done
    kubectl rollout status deploy/"${name}" -n "${ns}" --timeout="${timeout}"
    log "Deployment '${name}' is ready."
}

# ── Install steps ────────────────────────────────────────────────────────────

install_postgresql() {
    step "Installing PostgreSQL..."
    if helm status "${POSTGRESQL_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        log "PostgreSQL already installed. Skipping."
        return
    fi
    helm install "${POSTGRESQL_RELEASE}" oci://registry-1.docker.io/bitnamicharts/postgresql \
        --namespace "${NAMESPACE}" --create-namespace \
        --set auth.postgresPassword="${POSTGRESQL_PASSWORD}" \
        --set auth.database=batch \
        --set primary.persistence.enabled=false

    kubectl rollout status statefulset/"${POSTGRESQL_RELEASE}" -n "${NAMESPACE}" --timeout=180s
    log "PostgreSQL installed (database: batch)."
}

install_redis() {
    step "Installing Redis..."
    if helm status "${REDIS_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        log "Redis already installed. Skipping."
        return
    fi
    helm install "${REDIS_RELEASE}" oci://registry-1.docker.io/bitnamicharts/redis \
        --namespace "${NAMESPACE}" --create-namespace \
        --set architecture=standalone \
        --set auth.enabled=false

    kubectl rollout status statefulset/"${REDIS_RELEASE}-master" -n "${NAMESPACE}" --timeout=180s
    log "Redis installed (standalone, no auth)."
}

install_seaweedfs() {
    step "Installing SeaweedFS..."
    if kubectl get deployment "${SEAWEEDFS_RELEASE}" -n "${NAMESPACE}" &>/dev/null; then
        log "SeaweedFS already exists. Skipping deployment creation."
    else
        kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${SEAWEEDFS_RELEASE}
  namespace: ${NAMESPACE}
  labels:
    app: ${SEAWEEDFS_RELEASE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${SEAWEEDFS_RELEASE}
  template:
    metadata:
      labels:
        app: ${SEAWEEDFS_RELEASE}
    spec:
      containers:
      - name: seaweedfs
        image: chrislusf/seaweedfs:4.48
        args:
        - server
        - -dir=/data
        - -ip.bind=0.0.0.0
        - -s3
        - -s3.port=8333
        env:
        - name: AWS_ACCESS_KEY_ID
          value: "${SEAWEEDFS_ACCESS_KEY}"
        - name: AWS_SECRET_ACCESS_KEY
          value: "${SEAWEEDFS_SECRET_KEY}"
        ports:
        - containerPort: 8333
          name: s3
        readinessProbe:
          tcpSocket:
            port: s3
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: ${SEAWEEDFS_RELEASE}
  namespace: ${NAMESPACE}
  labels:
    app: ${SEAWEEDFS_RELEASE}
spec:
  selector:
    app: ${SEAWEEDFS_RELEASE}
  ports:
  - name: s3
    port: 8333
    targetPort: 8333
EOF
    fi

    wait_for_deployment "${SEAWEEDFS_RELEASE}" "${NAMESPACE}"

    step "Ensuring SeaweedFS bucket '${SEAWEEDFS_BUCKET}'..."
    local seaweedfs_pod bucket_list bucket_query_ready=false
    seaweedfs_pod=$(kubectl get pod -n "${NAMESPACE}" -l "app=${SEAWEEDFS_RELEASE}" \
        -o jsonpath='{.items[0].metadata.name}')
    [ -z "${seaweedfs_pod}" ] && die "No SeaweedFS pod found"

    for _ in $(seq 1 30); do
        if bucket_list=$(printf 's3.bucket.list\n' | kubectl exec -i -n "${NAMESPACE}" "${seaweedfs_pod}" -- \
            weed shell -master=localhost:9333 -filer=localhost:8888 2>/dev/null); then
            bucket_query_ready=true
            break
        fi
        sleep 2
    done
    [ "${bucket_query_ready}" = true ] || die "Failed to query SeaweedFS buckets"

    if printf '%s\n' "${bucket_list}" | awk -v bucket="${SEAWEEDFS_BUCKET}" '$1 == bucket { found=1 } END { exit !found }'; then
        log "SeaweedFS bucket '${SEAWEEDFS_BUCKET}' already exists."
    else
        printf 's3.bucket.create -name %s\n' "${SEAWEEDFS_BUCKET}" | \
            kubectl exec -i -n "${NAMESPACE}" "${seaweedfs_pod}" -- \
            weed shell -master=localhost:9333 -filer=localhost:8888 \
            || die "Failed to create SeaweedFS bucket '${SEAWEEDFS_BUCKET}'"
        log "SeaweedFS installed (bucket: ${SEAWEEDFS_BUCKET})."
    fi
}

create_pvc() {
    step "Ensuring PVC '${FILES_PVC_NAME}' for file storage..."

    if kubectl get pvc "${FILES_PVC_NAME}" -n "${NAMESPACE}" &>/dev/null; then
        log "PVC '${FILES_PVC_NAME}' already exists. Skipping."
        return
    fi

    # kind's local-path-provisioner only supports ReadWriteOnce.
    local access_mode="ReadWriteOnce"

    kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${FILES_PVC_NAME}
  namespace: ${NAMESPACE}
spec:
  accessModes:
    - ${access_mode}
  resources:
    requests:
      storage: 1Gi
EOF
    log "PVC '${FILES_PVC_NAME}' created."
}

create_secret() {
    step "Creating ${SECRET_NAME}..."

    local postgresql_url="postgresql://postgres:${POSTGRESQL_PASSWORD}@${POSTGRESQL_RELEASE}.${NAMESPACE}.svc.cluster.local:5432/batch?sslmode=disable"
    local redis_secret_entry=""
    if [ "${INSTALL_REDIS}" = true ]; then
        local redis_url="redis://${REDIS_RELEASE}-master.${NAMESPACE}.svc.cluster.local:6379/0"
        redis_secret_entry="  redis-url: \"${redis_url}\""
    fi

    kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  namespace: ${NAMESPACE}
stringData:
${redis_secret_entry}
  postgresql-url: "${postgresql_url}"
  inference-api-key: "dummy-api-key"
  s3-secret-access-key: "${SEAWEEDFS_SECRET_KEY}"
EOF
    log "Secret '${SECRET_NAME}' created."
}

# ── Main ─────────────────────────────────────────────────────────────────────

main() {
    step "Setting up prerequisites for LLMBatchGateway in ${NAMESPACE} (file storage: ${FILE_CLIENT_TYPE})"

    kubectl create namespace "${NAMESPACE}" 2>/dev/null || true

    install_postgresql
    if [ "${INSTALL_REDIS}" = true ]; then
        install_redis
    fi

    if [ "${FILE_CLIENT_TYPE}" = "fs" ]; then
        create_pvc
    else
        install_seaweedfs
    fi

    create_secret

    echo ""
    log "Prerequisites ready:"
    echo "  Namespace:    ${NAMESPACE}"
    echo "  PostgreSQL:   ${POSTGRESQL_RELEASE}.${NAMESPACE}.svc.cluster.local:5432/batch"
    if [ "${INSTALL_REDIS}" = true ]; then
        echo "  Redis:        ${REDIS_RELEASE}-master.${NAMESPACE}.svc.cluster.local:6379"
    fi
    if [ "${FILE_CLIENT_TYPE}" = "fs" ]; then
        echo "  File (PVC):   ${FILES_PVC_NAME}"
    else
        echo "  SeaweedFS (S3): ${SEAWEEDFS_RELEASE}.${NAMESPACE}.svc.cluster.local:8333 (bucket: ${SEAWEEDFS_BUCKET})"
    fi
    echo "  Secret:       ${SECRET_NAME}"
}

main "$@"
