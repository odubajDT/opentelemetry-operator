//go:build e2e
// +build e2e

package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	namespace          = "ta-loadtest"
	taServiceName      = "target-allocator"
	collectorName      = "otel-collector"
	avalancheName      = "avalanche"
	manifestsDir       = "manifests"
	defaultPollTimeout = 5 * time.Minute
	pollInterval       = 5 * time.Second
)

// testContext holds shared state for the test.
type testContext struct {
	t         *testing.T
	clientset *kubernetes.Clientset
}

func newTestContext(t *testing.T) *testContext {
	t.Helper()

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("failed to get home dir: %v", err)
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("failed to build kube config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create kubernetes client: %v", err)
	}

	return &testContext{
		t:         t,
		clientset: clientset,
	}
}

// taTargetsResponse represents the TA /jobs/<job>/targets response.
// The actual response is map[collectorName]{ _link: string, targets: []taTarget }
type taCollectorAllocation struct {
	Link    string     `json:"_link"`
	Targets []taTarget `json:"targets"`
}

type taTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// TestLoadTestScenario is the main test orchestrating the full PoC flow.
func TestLoadTestScenario(t *testing.T) {
	tc := newTestContext(t)
	ctx := context.Background()

	// Step 1: Verify all components are ready
	t.Run("01_components_ready", func(t *testing.T) {
		tc.waitForDeploymentReady(ctx, avalancheName, 5*time.Minute)
		tc.waitForDeploymentReady(ctx, taServiceName, 3*time.Minute)
		tc.waitForStatefulSetReady(ctx, collectorName, 3*time.Minute)
		t.Log("All components are ready")
	})

	// Step 2: Verify TA discovers targets and allocates them
	t.Run("02_initial_target_allocation", func(t *testing.T) {
		tc.waitForTargetAllocation(ctx, "avalanche", defaultPollTimeout)
	})

	// Step 3: Verify targets are distributed across collectors
	t.Run("03_target_distribution", func(t *testing.T) {
		tc.verifyTargetDistribution(ctx, 2) // expect 2 collectors initially
	})

	// Step 4: Monitor HPA and wait for scale-up
	t.Run("04_hpa_monitoring", func(t *testing.T) {
		tc.monitorHPA(ctx, 5*time.Minute)
	})

	// Step 5: If HPA scaled, verify TA redistributed targets
	t.Run("05_redistribution_after_scale", func(t *testing.T) {
		replicas := tc.getCurrentCollectorReplicas(ctx)
		if replicas <= 2 {
			t.Log("HPA did not scale up — skipping redistribution check")
			t.Log("This may mean the load is insufficient to trigger CPU-based scaling")
			t.Log("Consider increasing mock server replicas or metrics per endpoint")
			return
		}
		t.Logf("HPA scaled to %d replicas — verifying redistribution", replicas)
		tc.verifyTargetDistribution(ctx, int(replicas))
	})

	// Step 6: Print summary
	t.Run("06_summary", func(t *testing.T) {
		tc.printSummary(ctx)
	})
}

// waitForDeploymentReady waits until a Deployment has all replicas available.
func (tc *testContext) waitForDeploymentReady(ctx context.Context, name string, timeout time.Duration) {
	tc.t.Helper()
	tc.t.Logf("Waiting for deployment %s/%s to be ready...", namespace, name)

	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		dep, err := tc.clientset.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		ready := dep.Status.ReadyReplicas >= *dep.Spec.Replicas
		if !ready {
			tc.t.Logf("  %s: %d/%d ready", name, dep.Status.ReadyReplicas, *dep.Spec.Replicas)
		}
		return ready, nil
	})
	if err != nil {
		tc.t.Fatalf("Deployment %s did not become ready within %v: %v", name, timeout, err)
	}
	tc.t.Logf("Deployment %s is ready", name)
}

// waitForStatefulSetReady waits until a StatefulSet has all replicas ready.
func (tc *testContext) waitForStatefulSetReady(ctx context.Context, name string, timeout time.Duration) {
	tc.t.Helper()
	tc.t.Logf("Waiting for statefulset %s/%s to be ready...", namespace, name)

	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		sts, err := tc.clientset.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		ready := sts.Status.ReadyReplicas >= *sts.Spec.Replicas
		if !ready {
			tc.t.Logf("  %s: %d/%d ready", name, sts.Status.ReadyReplicas, *sts.Spec.Replicas)
		}
		return ready, nil
	})
	if err != nil {
		tc.t.Fatalf("StatefulSet %s did not become ready within %v: %v", name, timeout, err)
	}
	tc.t.Logf("StatefulSet %s is ready", name)
}

// waitForTargetAllocation polls the TA until the given job has allocated targets.
func (tc *testContext) waitForTargetAllocation(ctx context.Context, jobName string, timeout time.Duration) {
	tc.t.Helper()
	tc.t.Logf("Waiting for TA to allocate targets for job %q...", jobName)

	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		allocations, err := tc.queryTATargets(ctx, jobName)
		if err != nil {
			tc.t.Logf("  TA query failed (will retry): %v", err)
			return false, nil
		}

		totalTargets := 0
		for collector, targets := range allocations {
			tc.t.Logf("  %s: %d targets", collector, len(targets))
			totalTargets += len(targets)
		}

		if totalTargets == 0 {
			tc.t.Log("  No targets allocated yet")
			return false, nil
		}

		tc.t.Logf("Total: %d targets across %d collectors", totalTargets, len(allocations))
		return true, nil
	})
	if err != nil {
		tc.t.Fatalf("TA did not allocate targets within %v: %v", timeout, err)
	}
}

// verifyTargetDistribution checks that targets are spread across the expected number of collectors.
func (tc *testContext) verifyTargetDistribution(ctx context.Context, expectedCollectors int) {
	tc.t.Helper()

	allocations, err := tc.queryTATargets(ctx, "avalanche")
	if err != nil {
		tc.t.Fatalf("Failed to query TA: %v", err)
	}

	if len(allocations) < expectedCollectors {
		tc.t.Logf("WARNING: targets distributed across %d collectors, expected %d", len(allocations), expectedCollectors)
	}

	totalTargets := 0
	collectors := make([]string, 0, len(allocations))
	for collector, targets := range allocations {
		collectors = append(collectors, collector)
		targetCount := len(targets)
		totalTargets += targetCount
		tc.t.Logf("  Collector %s: %d targets", collector, targetCount)
	}
	sort.Strings(collectors)

	tc.t.Logf("Distribution: %d targets across %d collectors", totalTargets, len(collectors))

	if totalTargets == 0 {
		tc.t.Fatal("No targets were allocated")
	}

	// Check for reasonable distribution (no collector should have 0 targets if there are enough)
	if totalTargets >= len(collectors) {
		for _, collector := range collectors {
			if len(allocations[collector]) == 0 {
				tc.t.Errorf("Collector %s has 0 targets — unbalanced distribution", collector)
			}
		}
	}
}

// monitorHPA watches the HPA and reports scaling events.
func (tc *testContext) monitorHPA(ctx context.Context, duration time.Duration) {
	tc.t.Helper()
	tc.t.Logf("Monitoring HPA for %v...", duration)

	startTime := time.Now()
	initialReplicas := tc.getCurrentCollectorReplicas(ctx)
	tc.t.Logf("Initial collector replicas: %d", initialReplicas)
	maxReplicas := initialReplicas
	scaledUp := false

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			tc.t.Logf("HPA monitoring complete after %v", time.Since(startTime).Round(time.Second))
			tc.t.Logf("Final replicas: %d (started at %d, max %d)", tc.getCurrentCollectorReplicas(ctx), initialReplicas, maxReplicas)
			if !scaledUp {
				tc.t.Log("NOTE: HPA did not trigger scale-up during monitoring period")
			}
			return
		case <-ticker.C:
			hpa, err := tc.clientset.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, collectorName, metav1.GetOptions{})
			if err != nil {
				tc.t.Logf("  Failed to get HPA: %v", err)
				continue
			}

			currentReplicas := hpa.Status.CurrentReplicas
			elapsed := time.Since(startTime).Round(time.Second)

			// Report CPU metrics if available
			cpuStr := "unknown"
			for _, metric := range hpa.Status.CurrentMetrics {
				if metric.Type == autoscalingv2.ResourceMetricSourceType && metric.Resource != nil && metric.Resource.Name == "cpu" {
					if metric.Resource.Current.AverageUtilization != nil {
						cpuStr = fmt.Sprintf("%d%%", *metric.Resource.Current.AverageUtilization)
					}
				}
			}

			tc.t.Logf("  [%v] replicas=%d, cpu=%s, desired=%d",
				elapsed, currentReplicas, cpuStr, hpa.Status.DesiredReplicas)

			if currentReplicas > maxReplicas {
				maxReplicas = currentReplicas
				scaledUp = true
				tc.t.Logf("  >>> SCALE UP detected: %d -> %d replicas", initialReplicas, currentReplicas)
			}

			// Early exit if we've seen scale-up and it stabilized
			if scaledUp && currentReplicas == hpa.Status.DesiredReplicas && time.Since(startTime) > 2*time.Minute {
				tc.t.Log("HPA scaled and stabilized — ending monitoring early")
				return
			}
		}
	}
}

// queryTATargets queries the TA for target allocations via port-forward.
// Returns map[collectorName][]target
func (tc *testContext) queryTATargets(ctx context.Context, jobName string) (map[string][]taTarget, error) {
	// Find the TA pod
	pods, err := tc.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=target-allocator",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list TA pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no TA pods found")
	}

	pod := pods.Items[0]
	if pod.Status.Phase != corev1.PodRunning {
		return nil, fmt.Errorf("TA pod is not running (phase: %s)", pod.Status.Phase)
	}

	// Port-forward to the TA pod
	localPort, stopCh, err := tc.portForward(ctx, pod.Name, 8080)
	if err != nil {
		return nil, fmt.Errorf("port-forward failed: %w", err)
	}
	defer close(stopCh)

	// Query the TA
	url := fmt.Sprintf("http://localhost:%d/jobs/%s/targets", localPort, jobName)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TA returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var result map[string]taCollectorAllocation
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse TA response: %w (body: %s)", err, truncate(string(body), 500))
	}

	// Flatten the result
	flat := make(map[string][]taTarget)
	for collector, alloc := range result {
		flat[collector] = append(flat[collector], alloc.Targets...)
	}
	return flat, nil
}

// portForward sets up port forwarding to a pod and returns the local port.
func (tc *testContext) portForward(ctx context.Context, podName string, remotePort int) (int, chan struct{}, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return 0, nil, err
	}

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return 0, nil, err
	}

	url := tc.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward").
		URL()

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})

	// Use port 0 to get a random local port
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", remotePort)}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return 0, nil, err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- fw.ForwardPorts()
	}()

	select {
	case <-readyCh:
		ports, err := fw.GetPorts()
		if err != nil {
			close(stopCh)
			return 0, nil, err
		}
		return int(ports[0].Local), stopCh, nil
	case err := <-errCh:
		return 0, nil, fmt.Errorf("port-forward error: %w", err)
	case <-time.After(10 * time.Second):
		close(stopCh)
		return 0, nil, fmt.Errorf("port-forward timed out")
	}
}

// getCurrentCollectorReplicas returns the current ready replica count.
func (tc *testContext) getCurrentCollectorReplicas(ctx context.Context) int32 {
	tc.t.Helper()
	sts, err := tc.clientset.AppsV1().StatefulSets(namespace).Get(ctx, collectorName, metav1.GetOptions{})
	if err != nil {
		tc.t.Logf("Failed to get StatefulSet: %v", err)
		return 0
	}
	return sts.Status.ReadyReplicas
}

// printSummary outputs a final report.
func (tc *testContext) printSummary(ctx context.Context) {
	tc.t.Helper()

	tc.t.Log("=== LOAD TEST SUMMARY ===")

	// Avalanche status
	avalancheDep, err := tc.clientset.AppsV1().Deployments(namespace).Get(ctx, avalancheName, metav1.GetOptions{})
	if err == nil {
		tc.t.Logf("Avalanche: %d/%d replicas ready", avalancheDep.Status.ReadyReplicas, *avalancheDep.Spec.Replicas)
	}

	// TA status
	taDep, err := tc.clientset.AppsV1().Deployments(namespace).Get(ctx, taServiceName, metav1.GetOptions{})
	if err == nil {
		tc.t.Logf("Target Allocator: %d/%d replicas ready", taDep.Status.ReadyReplicas, *taDep.Spec.Replicas)
	}

	// Collector status
	sts, err := tc.clientset.AppsV1().StatefulSets(namespace).Get(ctx, collectorName, metav1.GetOptions{})
	if err == nil {
		tc.t.Logf("Collector: %d/%d replicas ready", sts.Status.ReadyReplicas, *sts.Spec.Replicas)
	}

	// HPA status
	hpa, err := tc.clientset.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, collectorName, metav1.GetOptions{})
	if err == nil {
		tc.t.Logf("HPA: min=%d, max=%d, current=%d, desired=%d",
			*hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas,
			hpa.Status.CurrentReplicas, hpa.Status.DesiredReplicas)
		for _, cond := range hpa.Status.Conditions {
			tc.t.Logf("  HPA condition: %s=%s (%s)", cond.Type, cond.Status, cond.Message)
		}
	}

	// Target allocation
	allocations, err := tc.queryTATargets(ctx, "avalanche")
	if err == nil {
		totalTargets := 0
		for collector, targets := range allocations {
			targetCount := len(targets)
			totalTargets += targetCount
			tc.t.Logf("  %s: %d targets", collector, targetCount)
		}
		tc.t.Logf("Total targets: %d across %d collectors", totalTargets, len(allocations))

		// Estimate data volume
		metricsPerTarget := 10000 // matches mock server default
		scrapeInterval := 15      // seconds
		dataPointsPerMin := totalTargets * metricsPerTarget * (60 / scrapeInterval)
		tc.t.Logf("Estimated throughput: %d data points/min (%.1fM/min)",
			dataPointsPerMin, float64(dataPointsPerMin)/1_000_000)
	}

	tc.t.Log("=========================")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
