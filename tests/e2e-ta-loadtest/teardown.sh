#!/usr/bin/env bash
# Teardown script for the TA load test PoC.
# Removes all deployed resources. Optionally deletes the kind cluster.
#
# Usage:
#   ./teardown.sh                      # Remove namespace only (keep cluster)
#   DELETE_CLUSTER=true ./teardown.sh   # Also delete kind cluster
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DELETE_CLUSTER="${DELETE_CLUSTER:-false}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-otel-operator}"

echo ">>> Deleting load test resources..."

# Delete namespace (cascades to all namespaced resources)
kubectl delete namespace ta-loadtest --ignore-not-found --wait=false

# Delete cluster-scoped resources
kubectl delete clusterrole ta-loadtest-target-allocator --ignore-not-found
kubectl delete clusterrolebinding ta-loadtest-target-allocator --ignore-not-found

echo ">>> Namespace and RBAC removed"

if [[ "$DELETE_CLUSTER" == "true" ]]; then
  echo ">>> Deleting kind cluster '${KIND_CLUSTER_NAME}'..."
  KIND_BIN="${REPO_ROOT}/bin/kind"
  "$KIND_BIN" delete cluster --name "${KIND_CLUSTER_NAME}" 2>/dev/null || true
  echo ">>> Kind cluster deleted"
fi

echo ">>> Teardown complete"
