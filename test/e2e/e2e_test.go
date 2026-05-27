package e2e

import (
	"fmt"
	"testing"
	"time"
)

var (
	testNamespace = getEnvOrDefault("TEST_NAMESPACE", "default")
	testCRName    = getEnvOrDefault("TEST_CR_NAME", "batch-gateway")
)

const asyncProcessorImage = "ghcr.io/llm-d-incubation/llm-d-async:v0.7.0-RC3"

func TestE2E(t *testing.T) {
	t.Run("Operator", func(t *testing.T) {
		t.Run("StatusConditions", testStatusConditions)
		t.Run("OrphanCleanup", testOrphanCleanup)
		t.Run("SpecUpdate", testSpecUpdate)
		t.Run("AsyncProcessor", func(t *testing.T) {
			t.Run("DeploymentCreated", testAsyncProcessorDeploymentCreated)
			t.Run("StatusCondition", testAsyncProcessorStatusCondition)
			t.Run("SpecUpdatePropagates", testAsyncProcessorSpecUpdate)
			t.Run("DisableRemovesResources", testAsyncProcessorDisable)
		})
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

func testOrphanCleanup(t *testing.T) {
	dashboardCM := testCRName + "-batch-gateway-dashboards"

	kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
		`{"spec":{"grafana":{"enabled":true}}}`)
	t.Cleanup(func() {
		kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
			`{"spec":{"grafana":{"enabled":false}}}`)
		waitForResourceGone(t, "configmap", dashboardCM, testNamespace, 60*time.Second)
	})

	waitForResourceExists(t, "configmap", dashboardCM, testNamespace, 60*time.Second)

	kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
		`{"spec":{"grafana":{"enabled":false}}}`)

	waitForResourceGone(t, "configmap", dashboardCM, testNamespace, 60*time.Second)
}

func testSpecUpdate(t *testing.T) {
	deploymentName := findDeploymentByComponent(t, testNamespace, testCRName, "apiserver")

	original := getDeploymentReplicas(t, deploymentName, testNamespace)
	target := original + 1

	kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
		`{"spec":{"apiServer":{"replicas":`+itoa(target)+`}}}`)
	t.Cleanup(func() {
		kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
			`{"spec":{"apiServer":{"replicas":`+itoa(original)+`}}}`)
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

func itoa(n int64) string {
	return fmt.Sprintf("%d", n)
}

func asyncProcessorDeploymentName() string {
	return testCRName + "-batch-gateway-asyncprocessor"
}

func enableAsyncProcessor(t *testing.T) {
	t.Helper()
	kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace, fmt.Sprintf(
		`{"spec":{"asyncProcessor":{"image":%q,"concurrency":8,"requestTimeout":"5m","pollIntervalMs":1000,"batchSize":10}}}`,
		asyncProcessorImage,
	))
}

func disableAsyncProcessor(t *testing.T) {
	t.Helper()
	kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
		`{"spec":{"asyncProcessor":null}}`)
}

func testAsyncProcessorDeploymentCreated(t *testing.T) {
	enableAsyncProcessor(t)
	t.Cleanup(func() { disableAsyncProcessor(t) })

	waitForResourceExists(t, "deployment", asyncProcessorDeploymentName(), testNamespace, 60*time.Second)
	waitForResourceExists(t, "serviceaccount", asyncProcessorDeploymentName(), testNamespace, 60*time.Second)
}

func testAsyncProcessorStatusCondition(t *testing.T) {
	enableAsyncProcessor(t)
	t.Cleanup(func() { disableAsyncProcessor(t) })

	kubectlWait(t, "llmbatchgateway", testCRName, testNamespace, "AsyncProcessorAvailable", 120*time.Second)
}

func testAsyncProcessorSpecUpdate(t *testing.T) {
	enableAsyncProcessor(t)
	t.Cleanup(func() { disableAsyncProcessor(t) })

	waitForResourceExists(t, "deployment", asyncProcessorDeploymentName(), testNamespace, 60*time.Second)

	original := getDeploymentReplicas(t, asyncProcessorDeploymentName(), testNamespace)
	target := original + 1

	kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
		`{"spec":{"asyncProcessor":{"replicas":`+itoa(target)+`}}}`)
	t.Cleanup(func() {
		kubectlPatch(t, "llmbatchgateway", testCRName, testNamespace,
			`{"spec":{"asyncProcessor":{"replicas":`+itoa(original)+`}}}`)
	})

	kubectlWait(t, "deployment", asyncProcessorDeploymentName(), testNamespace,
		fmt.Sprintf("jsonpath={.spec.replicas}=%d", target), 60*time.Second)
}

func testAsyncProcessorDisable(t *testing.T) {
	enableAsyncProcessor(t)
	waitForResourceExists(t, "deployment", asyncProcessorDeploymentName(), testNamespace, 60*time.Second)

	disableAsyncProcessor(t)

	waitForResourceGone(t, "deployment", asyncProcessorDeploymentName(), testNamespace, 60*time.Second)
	waitForResourceGone(t, "serviceaccount", asyncProcessorDeploymentName(), testNamespace, 60*time.Second)
}
