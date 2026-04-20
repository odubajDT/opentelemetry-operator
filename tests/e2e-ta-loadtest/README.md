# TA Load Test PoC

Validates that OTel Collector with HPA auto-scales under Prometheus scraping load,
with Target Allocator redistributing targets across collector replicas as they scale.

Uses [Avalanche](https://github.com/prometheus-community/avalanche) — the standard
Prometheus community load testing tool — as the metrics source.

## Architecture

```
┌──────────────────────────────────────┐
│  Avalanche (prometheus-community)    │
│  40 replicas, realistic metric types │
│  Gauges, counters, histograms, etc.  │
│  Headless Service (DNS-based SD)     │
│  Serves /metrics on port 9001       │
└──────────────┬───────────────────────┘
               │ discovered via dns_sd_configs
               ↓
┌──────────────────────────────────────┐
│  Target Allocator (standalone)       │
│  consistent-hashing strategy         │
│  Distributes targets to collectors   │
└──────────────┬───────────────────────┘
               │ collectors poll GET /jobs/avalanche/targets?collector_id=
               ↓
┌──────────────────────────────────────┐
│  OTel Collector (StatefulSet)        │
│  Prometheus receiver + debug exp.    │
│  HPA: CPU 50%, min=2, max=10        │
│  Each scrapes only assigned targets  │
└──────────────────────────────────────┘
```

## Prerequisites

- Docker or Podman (≥23.0.0)
- `kind` (installed via `make kind` or present in `bin/`)
- `kubectl` configured
- Go (for running the test)
- metrics-server installed in cluster (required for HPA)

## Quick Start

### Option A: Automated Setup

```bash
# From repo root — builds images, creates kind cluster, deploys everything:
./tests/e2e-ta-loadtest/setup.sh
```

### Option B: Manual Setup (with existing kind cluster)

If you already have a kind cluster with images loaded:

```bash
# Set image references
export TA_IMG="ghcr.io/open-telemetry/opentelemetry-operator/target-allocator:<version>"
export COLLECTOR_IMG="ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib:<version>"

# Deploy all manifests
kubectl apply -f tests/e2e-ta-loadtest/manifests/00-namespace.yaml
kubectl apply -f tests/e2e-ta-loadtest/manifests/01-avalanche.yaml
sed "s|image: target-allocator:latest|image: ${TA_IMG}|" tests/e2e-ta-loadtest/manifests/02-target-allocator.yaml | kubectl apply -f -
sed "s|image: otel-collector:latest|image: ${COLLECTOR_IMG}|" tests/e2e-ta-loadtest/manifests/03-collector.yaml | kubectl apply -f -
```

### Monitor

```bash
# Watch pods come up (expect 43 total: 40 avalanche + 2 collectors + 1 TA)
kubectl -n ta-loadtest get pods -w

# Watch HPA scaling
kubectl -n ta-loadtest get hpa -w

# Check TA target allocation (port-forward)
kubectl -n ta-loadtest port-forward svc/target-allocator 8080:80 &
curl -s localhost:8080/jobs | jq
curl -s localhost:8080/jobs/avalanche/targets | jq
```

### Run the Test

```bash
# From repo root:
go test -tags e2e -count=1 -timeout 15m -v ./tests/e2e-ta-loadtest/...
```

The test will:

1. Wait for all components (40 Avalanche + TA + 2 collectors) to be ready
2. Verify TA discovers and allocates all 40 Avalanche targets
3. Check targets are distributed across collectors
4. Monitor HPA for 5 minutes, reporting CPU and scaling events
5. Verify target redistribution if HPA scaled up
6. Print a summary with throughput estimates

### Teardown

```bash
./tests/e2e-ta-loadtest/teardown.sh

# Or manually:
kubectl delete namespace ta-loadtest --wait=false
kubectl delete clusterrole ta-loadtest-target-allocator --ignore-not-found
kubectl delete clusterrolebinding ta-loadtest-target-allocator --ignore-not-found
```

## Test Results & Findings

### Successful Run: 2 → 5 Replicas (PASS, 140s)

**Configuration:**
- 40 Avalanche pods, 500 metric names × 30 series each (~34,500 series/pod)
- Collector CPU request: 100m, limit: 1 CPU, memory: 4Gi
- HPA threshold: 50% CPU, min=2, max=10
- Scrape interval: 15s (TA polls every 10s)

**Scaling timeline:**

| Time   | Replicas | CPU   | Desired | Event                    |
|--------|----------|-------|---------|--------------------------|
| 0:45   | 2        | 49%   | 2       | Below threshold          |
| 1:00   | 2        | 82%   | 2       | Above threshold          |
| 1:15   | 2        | 158%  | 4       | HPA wants 4              |
| 1:30   | **4**    | 112%  | 4       | **Scale-up: 2 → 4**      |
| 1:45   | 4        | 135%  | 5       | HPA wants 5              |
| 2:00   | **5**    | 72%   | 5       | **Scale-up: 4 → 5, stable** |

**Target redistribution after scaling (40 targets across 5 collectors):**

| Collector          | Targets |
|--------------------|---------|
| otel-collector-0   | 11      |
| otel-collector-1   | 9       |
| otel-collector-2   | 9       |
| otel-collector-3   | 8       |
| otel-collector-4   | 3       |

**Throughput:** ~1.6M data points/min

### Previous Iterations

| Attempt | Config | Result |
|---------|--------|--------|
| 20 replicas, 200m req, 70% threshold, 15s interval | CPU 17-27% | No scaling — insufficient load |
| 40 replicas, 200m req, 50% threshold, 15s interval | CPU ~65%, 2→3 replicas | Scaled to 3, stable |
| 60 replicas, 200m req, 50% threshold, 15s interval | Collectors OOMKilled, API server crashed | Too many pods for single-node kind |
| 40 replicas, 100m req, 30% threshold, 15s interval | 2→4→9 replicas, cluster crash | Threshold too low — HPA overshoots |
| **40 replicas, 100m req, 50% threshold, 15s interval** | **2→5 replicas, stable at 72% CPU** | **Best result** |

## Configuration

Override via environment variables in `setup.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `AVALANCHE_REPLICAS` | `40` | Number of Avalanche endpoints |
| `COLLECTOR_MIN_REPLICAS` | `2` | HPA minimum replicas |
| `COLLECTOR_MAX_REPLICAS` | `10` | HPA maximum replicas |
| `CPU_TARGET` | `50` | HPA CPU utilization target (%) |
| `SKIP_KIND` | `false` | Skip kind cluster creation |
| `AVALANCHE_IMG` | `quay.io/prometheuscommunity/avalanche:latest` | Avalanche image |
| `TARGETALLOCATOR_IMG` | (from git describe) | TA container image |
| `COLLECTOR_IMG` | (from versions.txt) | Collector container image |

### Avalanche Metrics Configuration

The `--series-count=30` flag controls how many unique label combinations (series) each metric name gets.
Each metric name is fanned out into 30 time series with distinct label values.

Default per-replica breakdown (in [01-avalanche.yaml](manifests/01-avalanche.yaml)):

| Metric type | Count | × series | Prometheus expansion | Series per pod |
|-------------|-------|----------|----------------------|----------------|
| Gauges      | 200   | × 30     | 1 series each        | 6,000          |
| Counters    | 200   | × 30     | 1 series each        | 6,000          |
| Histograms  | 50    | × 30     | 8 buckets + `le="+Inf"` + `_sum` + `_count` = 11 series each | 16,500 |
| Summaries   | 50    | × 30     | 2 objectives + `_sum` + `_count` = 4 series each | 6,000 |
| **Total**   | **500** | | | **~34,500** |

**Cluster-wide load** (40 replicas, 15s scrape interval):
- 40 pods × 34,500 series = **1,380,000 total time series**
- Scraped 4×/min (every 15s) → **~5.5M data points/min**

> **Note:** The `target_allocator.interval: 10s` in the collector config is how often
> collectors poll the TA for target assignments — it is **not** the scrape interval.
> The actual scrape interval (`scrape_interval: 15s`) is defined in the TA's
> [scrape config](manifests/02-target-allocator.yaml).
