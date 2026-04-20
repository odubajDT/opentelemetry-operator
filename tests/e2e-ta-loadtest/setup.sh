#!/usr/bin/env bash
# Setup script for the TA load test PoC.
# Builds TA image, creates kind cluster, and deploys all components.
# Uses Avalanche (prometheus-community/avalanche) as the metrics source.
#
# Usage:
#   ./setup.sh                         # Full setup with defaults
#   AVALANCHE_REPLICAS=50 ./setup.sh   # Override avalanche replica count
#   SKIP_KIND=true ./setup.sh          # Skip kind cluster creation (use existing)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Configuration (override via env vars)
AVALANCHE_REPLICAS="${AVALANCHE_REPLICAS:-40}"
COLLECTOR_MIN_REPLICAS="${COLLECTOR_MIN_REPLICAS:-2}"
COLLECTOR_MAX_REPLICAS="${COLLECTOR_MAX_REPLICAS:-10}"
CPU_TARGET="${CPU_TARGET:-50}"
SKIP_KIND="${SKIP_KIND:-false}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-otel-operator}"
KUBE_VERSION="${KUBE_VERSION:-1.35}"

# Image names
AVALANCHE_IMG="${AVALANCHE_IMG:-quay.io/prometheuscommunity/avalanche:latest}"
TARGETALLOCATOR_IMG="${TARGETALLOCATOR_IMG:-}"
COLLECTOR_IMG="${COLLECTOR_IMG:-}"

# Resolve TA image: use the locally-built image tag
if [[ -z "$TARGETALLOCATOR_IMG" ]]; then
  # Get the version from git describe (same logic as Makefile)
  cd "$REPO_ROOT"
  VERSION=$(git describe --tags --match='v*' 2>/dev/null | sed 's/^v//' || echo "latest")
  TARGETALLOCATOR_IMG="ghcr.io/open-telemetry/opentelemetry-operator/target-allocator:v${VERSION}"
  cd "$SCRIPT_DIR"
fi

# Resolve Collector image from versions.txt if not set
if [[ -z "$COLLECTOR_IMG" ]]; then
  COLLECTOR_VERSION=$(awk -F= '/^opentelemetry-collector=/ {print $2}' "${REPO_ROOT}/versions.txt")
  COLLECTOR_IMG="ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib:${COLLECTOR_VERSION}"
fi

echo "=== TA Load Test PoC Setup ==="
echo "Avalanche replicas:   ${AVALANCHE_REPLICAS}"
echo "Collector replicas:   ${COLLECTOR_MIN_REPLICAS}-${COLLECTOR_MAX_REPLICAS}"
echo "CPU target:           ${CPU_TARGET}%"
echo "Avalanche image:      ${AVALANCHE_IMG}"
echo "TA image:             ${TARGETALLOCATOR_IMG}"
echo "Collector image:      ${COLLECTOR_IMG}"
echo "=============================="

# Step 1: Build TA image
echo ""
echo ">>> Building Target Allocator image..."
cd "$REPO_ROOT"
make container-target-allocator
cd "$SCRIPT_DIR"

# Step 2: Create kind cluster (optional)
if [[ "$SKIP_KIND" != "true" ]]; then
  echo ""
  echo ">>> Creating kind cluster..."
  KIND_BIN="${REPO_ROOT}/bin/kind"
  if [[ ! -f "$KIND_BIN" ]]; then
    cd "$REPO_ROOT" && make kind && cd "$SCRIPT_DIR"
  fi
  "$KIND_BIN" create cluster --name "${KIND_CLUSTER_NAME}" --config "${REPO_ROOT}/kind-${KUBE_VERSION}.yaml" 2>/dev/null || \
    echo "Kind cluster '${KIND_CLUSTER_NAME}' already exists, reusing"
fi

# Step 3: Pull and load images into kind
echo ""
echo ">>> Loading images into kind cluster..."
KIND_BIN="${REPO_ROOT}/bin/kind"

echo "  Loading TA image..."
"$KIND_BIN" load --name "${KIND_CLUSTER_NAME}" docker-image "${TARGETALLOCATOR_IMG}" || true

echo "  Pulling and loading Avalanche image..."
docker pull "${AVALANCHE_IMG}" 2>/dev/null || true
"$KIND_BIN" load --name "${KIND_CLUSTER_NAME}" docker-image "${AVALANCHE_IMG}" || true

echo "  Pulling and loading Collector image..."
docker pull "${COLLECTOR_IMG}" 2>/dev/null || true
"$KIND_BIN" load --name "${KIND_CLUSTER_NAME}" docker-image "${COLLECTOR_IMG}" || true

# Step 4: Install metrics-server (required for HPA)
echo ""
echo ">>> Installing metrics-server..."
cd "$REPO_ROOT"
make install-metrics-server 2>/dev/null || \
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml 2>/dev/null || \
  echo "metrics-server may already be installed"
cd "$SCRIPT_DIR"

# Step 5: Apply manifests with image substitutions
echo ""
echo ">>> Deploying components..."

# Create namespace
kubectl apply -f "${SCRIPT_DIR}/manifests/00-namespace.yaml"

# Deploy Avalanche with configured replicas
cat "${SCRIPT_DIR}/manifests/01-avalanche.yaml" | \
  sed "s|replicas: 20|replicas: ${AVALANCHE_REPLICAS}|" | \
  sed "s|image: quay.io/prometheuscommunity/avalanche:latest|image: ${AVALANCHE_IMG}|" | \
  kubectl apply -f -

# Deploy TA with configured image
cat "${SCRIPT_DIR}/manifests/02-target-allocator.yaml" | \
  sed "s|image: target-allocator:latest|image: ${TARGETALLOCATOR_IMG}|" | \
  kubectl apply -f -

# Deploy collector with configured image and HPA settings
cat "${SCRIPT_DIR}/manifests/03-collector.yaml" | \
  sed "s|image: otel-collector:latest|image: ${COLLECTOR_IMG}|" | \
  sed "s|replicas: 2|replicas: ${COLLECTOR_MIN_REPLICAS}|" | \
  sed "s|minReplicas: 2|minReplicas: ${COLLECTOR_MIN_REPLICAS}|" | \
  sed "s|maxReplicas: 10|maxReplicas: ${COLLECTOR_MAX_REPLICAS}|" | \
  sed "s|averageUtilization: 70|averageUtilization: ${CPU_TARGET}|" | \
  kubectl apply -f -

echo ""
echo ">>> Setup complete!"
echo ""
echo "Monitor with:"
echo "  kubectl -n ta-loadtest get pods -w"
echo "  kubectl -n ta-loadtest get hpa -w"
echo ""
echo "Run the test:"
echo "  cd ${REPO_ROOT}"
echo "  go test -tags e2e -count=1 -timeout 20m -v ./tests/e2e-ta-loadtest/..."
