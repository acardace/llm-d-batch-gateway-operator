#!/usr/bin/env bash
# Verifies dev-deploy.sh renders S3-backed dev CRs against the SeaweedFS Service
# that setup-prereqs.sh installs in NAMESPACE (not the default-namespace sample value).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

fail() { echo "FAIL: $*" >&2; exit 1; }

# render_case <profile> <namespace> <seaweedfs-release>
# Sources dev-deploy.sh in a subshell and prints the rendered CR.
render_case() {
    (
        DEV_PROFILE="$1"
        NAMESPACE="$2"
        SEAWEEDFS_RELEASE="$3"
        SEAWEEDFS_ACCESS_KEY="test-access-key"
        # shellcheck source=dev-deploy.sh
        source "${SCRIPT_DIR}/dev-deploy.sh"
        cd "${OPERATOR_DIR}"
        render_dev_cr "config/samples/dev-${DEV_PROFILE}.yaml"
    )
}

check_case() {
    local profile="$1" namespace="$2" release="$3" want_endpoint="$4"
    local rendered endpoint access_key
    rendered="$(render_case "${profile}" "${namespace}" "${release}")"
    endpoint="$(yq '.spec.fileStorage.s3.endpoint' <<<"${rendered}")"
    access_key="$(yq '.spec.fileStorage.s3.accessKeyId' <<<"${rendered}")"

    [[ "${endpoint}" == "${want_endpoint}" ]] \
        || fail "profile=${profile} namespace=${namespace} release=${release}: endpoint=${endpoint}, want ${want_endpoint}"
    [[ "${access_key}" == "test-access-key" ]] \
        || fail "profile=${profile} namespace=${namespace}: accessKeyId=${access_key}, want test-access-key"
    echo "ok: profile=${profile} namespace=${namespace} release=${release} -> ${endpoint}"
}

check_case async default seaweedfs "http://seaweedfs.default.svc.cluster.local:8333"
check_case async foo seaweedfs "http://seaweedfs.foo.svc.cluster.local:8333"
check_case sync foo seaweedfs "http://seaweedfs.foo.svc.cluster.local:8333"
check_case async-gate-redis foo seaweedfs "http://seaweedfs.foo.svc.cluster.local:8333"
check_case async foo s3store "http://s3store.foo.svc.cluster.local:8333"

echo "All dev-deploy render checks passed."
