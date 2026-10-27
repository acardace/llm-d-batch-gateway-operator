package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var (
	testNamespace = getEnvOrDefault("TEST_NAMESPACE", "default")
	testCRName    = getEnvOrDefault("TEST_CR_NAME", "batch-gateway")
)

func TestE2E(t *testing.T) {
	t.Run("Operator", func(t *testing.T) {
		t.Run("StatusConditions", testStatusConditions)
		t.Run("OperandPodsReady", testOperandPodsReady)
		t.Run("ServiceReachability", testServiceReachability)
		t.Run("ReadyFalseConformance", testReadyFalseConformance)
		t.Run("CRDeletionCleanup", testCRDeletionCleanup)
		t.Run("OrphanCleanup", testOrphanCleanup)
		t.Run("SpecUpdate", testSpecUpdate)
		t.Run("ProcessorReplicasUpdate", testProcessorReplicasUpdate)
		t.Run("ConfigChangeRollout", testConfigChangeRollout)
		t.Run("ResourcesUpdate", testResourcesUpdate)
		t.Run("ProcessorConcurrencyUpdate", testProcessorConcurrencyUpdate)
	})
}

func testStatusConditions(t *testing.T) {
	expected := []string{"Ready", "APIServerAvailable", "ProcessorAvailable"}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		conditions := getCRConditions(t, testCRName, testNamespace)
		found := map[string]bool{}
		for _, c := range conditions {
			if typ, ok := c["type"].(string); ok {
				found[typ] = true
			}
		}
		allPresent := true
		for _, e := range expected {
			if !found[e] {
				allPresent = false
				break
			}
		}
		if allPresent {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for status conditions: %v", expected)
}

func testOperandPodsReady(t *testing.T) {
	for _, component := range []string{"apiserver", "processor", "gc"} {
		t.Run(component, func(t *testing.T) {
			waitForComponentPodsReady(t, testNamespace, testCRName, component, 120*time.Second)
		})
	}
}

func testServiceReachability(t *testing.T) {
	serviceName := findServiceByComponent(t, testNamespace, testCRName, "apiserver")
	waitForServiceEndpointsReady(t, serviceName, testNamespace, 60*time.Second)

	apiPort := getServicePortByAnyName(t, serviceName, testNamespace, "http", "https")
	withServicePortForward(t, testNamespace, serviceName, apiPort, func(baseURL string) {
		requireHTTPSuccess(t, baseURL+"/v1/files")
	})

	observabilityPort := getServicePortByName(t, serviceName, testNamespace, "observability")
	withServicePortForward(t, testNamespace, serviceName, observabilityPort, func(baseURL string) {
		requireHTTPStatus(t, baseURL+"/ready", 200)
	})
}

func testReadyFalseConformance(t *testing.T) {
	t.Cleanup(snapshotCRSpec(t, testCRName, testNamespace))
	deploymentName := findDeploymentByComponent(t, testNamespace, testCRName, "apiserver")
	originalReplicas := getDeploymentReplicas(t, deploymentName, testNamespace)

	kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace, `{"spec":{"apiServer":{"replicas":0}}}`)

	waitForConditionStatus(t, testCRName, testNamespace, "APIServerAvailable", "False", 90*time.Second)
	waitForConditionStatus(t, testCRName, testNamespace, "Ready", "False", 90*time.Second)

	kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
		fmt.Sprintf(`{"spec":{"apiServer":{"replicas":%d}}}`, originalReplicas))

	waitForConditionStatus(t, testCRName, testNamespace, "APIServerAvailable", "True", 120*time.Second)
	waitForConditionStatus(t, testCRName, testNamespace, "Ready", "True", 120*time.Second)
	waitForComponentPodsReady(t, testNamespace, testCRName, "apiserver", 120*time.Second)
}

func testCRDeletionCleanup(t *testing.T) {
	tempCRName := fmt.Sprintf("%s-cleanup-%d", testCRName, time.Now().UnixNano())
	selector := fmt.Sprintf("app.kubernetes.io/instance=%s", tempCRName)

	cloneExistingCR(t, testCRName, testNamespace, tempCRName)
	t.Cleanup(func() {
		kubectlDelete(t, llmBatchGatewayKind, tempCRName, testNamespace)
	})

	cr := kubectlGetJSON(t, llmBatchGatewayKind, tempCRName, testNamespace)
	ownerUID := getObjectUID(t, cr)

	// 2 batch Deployments (apiserver, gc) + 1 async-processor Deployment;
	// the processor is a StatefulSet.
	minDeployments := 3
	minConfigmaps := 4
	for _, tc := range []struct {
		resource string
		minCount int
	}{
		{resource: "deployment", minCount: minDeployments},
		{resource: "statefulset", minCount: 1},
		{resource: "service", minCount: 2},
		{resource: "configmap", minCount: minConfigmaps},
	} {
		items := waitForResourceCountAtLeast(t, tc.resource, testNamespace, selector, tc.minCount, 120*time.Second)
		for _, item := range items {
			if !hasOwnerUID(item, ownerUID) {
				t.Fatalf("%s for %s is not owned by %s", tc.resource, tempCRName, ownerUID)
			}
		}
	}

	kubectlDelete(t, llmBatchGatewayKind, tempCRName, testNamespace)
	for _, resource := range []string{"deployment", "statefulset", "service", "configmap"} {
		waitForResourcesGoneBySelector(t, resource, testNamespace, selector, 120*time.Second)
	}
}

func testOrphanCleanup(t *testing.T) {
	dashboardCM := testCRName + "-batch-gateway-dashboards"

	kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
		`{"spec":{"grafana":{"enabled":true}}}`)
	t.Cleanup(func() {
		kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
			`{"spec":{"grafana":{"enabled":false}}}`)
		waitForResourceGone(t, "configmap", dashboardCM, testNamespace, 60*time.Second)
	})

	waitForResourceExists(t, "configmap", dashboardCM, testNamespace, 60*time.Second)

	kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
		`{"spec":{"grafana":{"enabled":false}}}`)

	waitForResourceGone(t, "configmap", dashboardCM, testNamespace, 60*time.Second)
}

func testSpecUpdate(t *testing.T) {
	deploymentName := findDeploymentByComponent(t, testNamespace, testCRName, "apiserver")

	original := getDeploymentReplicas(t, deploymentName, testNamespace)
	target := original + 1

	kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
		fmt.Sprintf(`{"spec":{"apiServer":{"replicas":%d}}}`, target))
	t.Cleanup(func() {
		kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
			fmt.Sprintf(`{"spec":{"apiServer":{"replicas":%d}}}`, original))
	})

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if getDeploymentReplicas(t, deploymentName, testNamespace) == target {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("deployment %s replicas did not update to %d", deploymentName, target)
}

func testProcessorReplicasUpdate(t *testing.T) {
	statefulSetName := findStatefulSetByComponent(t, testNamespace, testCRName, "processor")

	original := getStatefulSetReplicas(t, statefulSetName, testNamespace)
	target := original + 1

	kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
		fmt.Sprintf(`{"spec":{"processor":{"replicas":%d}}}`, target))
	t.Cleanup(func() {
		kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
			fmt.Sprintf(`{"spec":{"processor":{"replicas":%d}}}`, original))
	})

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if getStatefulSetReplicas(t, statefulSetName, testNamespace) == target {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("statefulset %s replicas did not update to %d", statefulSetName, target)
}

func testConfigChangeRollout(t *testing.T) {
	components := []struct {
		name     string
		resource string
		patch    string
		cmSubstr string // expected substring in the ConfigMap config.yaml
	}{
		{
			name:     "apiserver",
			resource: "deployment",
			patch:    `{"spec":{"apiServer":{"config":{"readTimeoutSeconds":999}}}}`,
			cmSubstr: "read_timeout_seconds: 999",
		},
		{
			name:     "processor",
			resource: "statefulset",
			patch:    `{"spec":{"processor":{"config":{"numWorkers":99}}}}`,
			cmSubstr: "num_workers: 99",
		},
		{
			name:     "gc",
			resource: "deployment",
			patch:    `{"spec":{"gc":{"interval":"59m"}}}`,
			cmSubstr: `interval: "59m"`,
		},
	}

	for _, tc := range components {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(snapshotCRSpec(t, testCRName, testNamespace))
			workloadName := findWorkloadByComponent(t, tc.resource, testNamespace, testCRName, tc.name)
			configMapName := findConfigMapByComponent(t, testNamespace, testCRName, tc.name)
			checksumBefore := getWorkloadPodAnnotation(t, tc.resource, workloadName, testNamespace, "checksum/config")

			kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace, tc.patch)

			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				// Verify the ConfigMap data contains the expected value.
				cmData := getConfigMapData(t, configMapName, testNamespace)
				if !strings.Contains(cmData, tc.cmSubstr) {
					time.Sleep(pollInterval)
					continue
				}

				// Verify the workload pod template was updated.
				checksumAfter := getWorkloadPodAnnotation(t, tc.resource, workloadName, testNamespace, "checksum/config")
				if checksumAfter != checksumBefore {
					return
				}
				time.Sleep(pollInterval)
			}
			t.Fatalf("%s %s or configmap %s did not update after config change", tc.resource, workloadName, configMapName)
		})
	}
}

func testResourcesUpdate(t *testing.T) {
	components := []struct {
		name      string
		specField string
		resource  string
	}{
		{name: "apiserver", specField: "apiServer", resource: "deployment"},
		{name: "processor", specField: "processor", resource: "statefulset"},
	}

	for _, tc := range components {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(snapshotCRSpec(t, testCRName, testNamespace))
			workloadName := findWorkloadByComponent(t, tc.resource, testNamespace, testCRName, tc.name)

			kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace,
				`{"spec":{"`+tc.specField+`":{"resources":{"requests":{"cpu":"111m","memory":"99Mi"}}}}}`)

			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				resources := getWorkloadContainerResources(t, tc.resource, workloadName, testNamespace)
				requests, _ := resources["requests"].(map[string]any)
				if requests != nil && requests["cpu"] == "111m" && requests["memory"] == "99Mi" {
					return
				}
				time.Sleep(pollInterval)
			}
			t.Fatalf("%s %s container resources did not update", tc.resource, workloadName)
		})
	}
}

// testProcessorConcurrencyUpdate verifies that changes to the processor
// concurrency config (including AIMD settings) are reconciled into the
// processor ConfigMap and trigger a pod-template rollout.
func testProcessorConcurrencyUpdate(t *testing.T) {
	cases := []struct {
		name     string
		patch    string
		cmSubstr string
	}{
		{
			name:     "global concurrency",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"global":42}}}}}`,
			cmSubstr: "global: 42",
		},
		{
			name:     "per-endpoint concurrency",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"perEndpoint":7}}}}}`,
			cmSubstr: "per_endpoint: 7",
		},
		{
			name:     "recovery concurrency",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"recovery":3}}}}}`,
			cmSubstr: "recovery: 3",
		},
		{
			name:     "aimd backoff factor",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"aimd":{"backoffFactor":"0.3"}}}}}}`,
			cmSubstr: "backoff_factor: 0.3",
		},
		{
			name:     "aimd min",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"aimd":{"min":2}}}}}}`,
			cmSubstr: "min: 2",
		},
		{
			name:     "aimd additive increase",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"aimd":{"additiveIncrease":3}}}}}}`,
			cmSubstr: "additive_increase: 3",
		},
		{
			name:     "aimd disabled",
			patch:    `{"spec":{"processor":{"config":{"concurrency":{"aimd":{"enabled":false}}}}}}`,
			cmSubstr: "enabled: false",
		},
	}

	statefulSetName := findStatefulSetByComponent(t, testNamespace, testCRName, "processor")
	configMapName := findConfigMapByComponent(t, testNamespace, testCRName, "processor")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(snapshotCRSpec(t, testCRName, testNamespace))
			checksumBefore := getStatefulSetPodAnnotation(t, statefulSetName, testNamespace, "checksum/config")

			kubectlPatch(t, llmBatchGatewayKind, testCRName, testNamespace, tc.patch)

			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				cmData := getConfigMapData(t, configMapName, testNamespace)
				if !strings.Contains(cmData, tc.cmSubstr) {
					time.Sleep(pollInterval)
					continue
				}
				checksumAfter := getStatefulSetPodAnnotation(t, statefulSetName, testNamespace, "checksum/config")
				if checksumAfter != checksumBefore {
					return
				}
				time.Sleep(pollInterval)
			}
			t.Fatalf("processor configmap did not contain %q or statefulset did not roll out", tc.cmSubstr)
		})
	}
}
