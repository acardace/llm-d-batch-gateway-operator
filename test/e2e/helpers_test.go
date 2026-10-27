package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	pollInterval        = 2 * time.Second
	llmBatchGatewayKind = "llmbatchgateway"
)

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func kubectl(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	return cmd.CombinedOutput()
}

func kubectlWithInput(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = bytes.NewReader(input)
	return cmd.CombinedOutput()
}

// snapshotCRSpec captures the full spec of the named LLMBatchGateway CR and
// returns a restore function that applies it back as a merge patch. Pass the
// returned function directly to t.Cleanup.
func snapshotCRSpec(t *testing.T, name, namespace string) func() {
	t.Helper()
	obj := kubectlGetJSON(t, "llmbatchgateway", name, namespace)
	spec, ok := obj["spec"]
	if !ok {
		t.Fatalf("CR %s has no spec", name)
	}
	b, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshalling spec snapshot for %s: %v", name, err)
	}
	snapshot := string(b)
	return func() {
		kubectlPatch(t, "llmbatchgateway", name, namespace, snapshot)
	}
}

func kubectlPatch(t *testing.T, resource, name, namespace, patchJSON string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := kubectl(ctx, "patch", resource, name, "-n", namespace, "--type", "merge", "-p", patchJSON)
	if err != nil {
		t.Fatalf("kubectl patch %s %s: %v\n%s", resource, name, err, out)
	}
}

func kubectlCreateObject(t *testing.T, obj map[string]any) {
	t.Helper()
	body, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal object: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := kubectlWithInput(ctx, body, "create", "-f", "-")
	if err != nil {
		t.Fatalf("kubectl create object: %v\n%s", err, out)
	}
}

func kubectlDelete(t *testing.T, resource, name, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := kubectl(ctx, "delete", resource, name, "-n", namespace, "--ignore-not-found=true", "--wait=true")
	if err != nil {
		t.Fatalf("kubectl delete %s %s: %v\n%s", resource, name, err, out)
	}
}

func kubectlGetExists(t *testing.T, resource, name, namespace string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := kubectl(ctx, "get", resource, name, "-n", namespace, "--no-headers")
	if err == nil {
		return true
	}
	if strings.Contains(string(out), "NotFound") {
		return false
	}
	t.Fatalf("kubectl get %s %s: %v\n%s", resource, name, err, out)
	return false
}

func kubectlGetJSON(t *testing.T, resource, name, namespace string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := kubectl(ctx, "get", resource, name, "-n", namespace, "-o", "json")
	if err != nil {
		t.Fatalf("kubectl get %s %s: %v\n%s", resource, name, err, out)
	}

	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parsing JSON for %s %s: %v", resource, name, err)
	}
	return result
}

func kubectlListJSON(t *testing.T, resource, namespace, selector string) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := []string{"get", resource, "-n", namespace, "-o", "json"}
	if selector != "" {
		args = append(args, "-l", selector)
	}

	out, err := kubectl(ctx, args...)
	if err != nil {
		t.Fatalf("kubectl list %s: %v\n%s", resource, err, out)
	}

	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("parsing JSON list for %s: %v", resource, err)
	}

	rawItems, ok := result["items"].([]any)
	if !ok {
		return nil
	}

	items := make([]map[string]any, 0, len(rawItems))
	for _, item := range rawItems {
		if obj, ok := item.(map[string]any); ok {
			items = append(items, obj)
		}
	}
	return items
}

func waitForResourceExists(t *testing.T, resource, name, namespace string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if kubectlGetExists(t, resource, name, namespace) {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for %s/%s to exist", resource, name)
}

func waitForResourceGone(t *testing.T, resource, name, namespace string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !kubectlGetExists(t, resource, name, namespace) {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for %s/%s to be deleted", resource, name)
}

func getCRConditions(t *testing.T, crName, namespace string) []map[string]any {
	t.Helper()
	obj := kubectlGetJSON(t, llmBatchGatewayKind, crName, namespace)

	status, ok := obj["status"].(map[string]any)
	if !ok {
		return nil
	}
	rawConds, ok := status["conditions"].([]any)
	if !ok {
		return nil
	}

	var conditions []map[string]any
	for _, c := range rawConds {
		if m, ok := c.(map[string]any); ok {
			conditions = append(conditions, m)
		}
	}
	return conditions
}

func waitForConditionStatus(t *testing.T, crName, namespace, conditionType, wantStatus string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, condition := range getCRConditions(t, crName, namespace) {
			typ, _ := condition["type"].(string)
			status, _ := condition["status"].(string)
			if typ == conditionType && status == wantStatus {
				return
			}
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for condition %s=%s on %s", conditionType, wantStatus, crName)
}

func findDeploymentByComponent(t *testing.T, namespace, instance, component string) string {
	return findWorkloadByComponent(t, "deployment", namespace, instance, component)
}

func findStatefulSetByComponent(t *testing.T, namespace, instance, component string) string {
	return findWorkloadByComponent(t, "statefulset", namespace, instance, component)
}

func findWorkloadByComponent(t *testing.T, resource, namespace, instance, component string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=%s", instance, component)
	out, err := kubectl(ctx, "get", resource, "-n", namespace, "-l", selector, "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		t.Fatalf("finding %s with component=%s: %v\n%s", resource, component, err, out)
	}
	name := string(out)
	if name == "" {
		t.Fatalf("no %s found with labels instance=%s,component=%s", resource, instance, component)
	}
	return name
}

func findServiceByComponent(t *testing.T, namespace, instance, component string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=%s", instance, component)
	out, err := kubectl(ctx, "get", "service", "-n", namespace, "-l", selector, "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		t.Fatalf("finding service with component=%s: %v\n%s", component, err, out)
	}
	name := string(out)
	if name == "" {
		t.Fatalf("no service found with labels instance=%s,component=%s", instance, component)
	}
	return name
}

func getDeploymentReplicas(t *testing.T, name, namespace string) int64 {
	return getWorkloadReplicas(t, "deployment", name, namespace)
}

func getStatefulSetReplicas(t *testing.T, name, namespace string) int64 {
	return getWorkloadReplicas(t, "statefulset", name, namespace)
}

func getWorkloadReplicas(t *testing.T, resource, name, namespace string) int64 {
	t.Helper()
	obj := kubectlGetJSON(t, resource, name, namespace)

	spec, ok := obj["spec"].(map[string]any)
	if !ok {
		t.Fatalf("%s %s has no spec", resource, name)
	}
	replicas, ok := spec["replicas"].(float64)
	if !ok {
		t.Fatalf("%s %s has no spec.replicas", resource, name)
	}
	return int64(replicas)
}

func findConfigMapByComponent(t *testing.T, namespace, instance, component string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=%s", instance, component)
	out, err := kubectl(ctx, "get", "configmap", "-n", namespace, "-l", selector, "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		t.Fatalf("finding configmap with component=%s: %v\n%s", component, err, out)
	}
	name := string(out)
	if name == "" {
		t.Fatalf("no configmap found with labels instance=%s,component=%s", instance, component)
	}
	return name
}

func getConfigMapData(t *testing.T, name, namespace string) string {
	t.Helper()
	obj := kubectlGetJSON(t, "configmap", name, namespace)
	data, _ := obj["data"].(map[string]any)
	configYAML, _ := data["config.yaml"].(string)
	return configYAML
}

func getStatefulSetPodAnnotation(t *testing.T, name, namespace, annotation string) string {
	return getWorkloadPodAnnotation(t, "statefulset", name, namespace, annotation)
}

func getWorkloadPodAnnotation(t *testing.T, resource, name, namespace, annotation string) string {
	t.Helper()
	obj := kubectlGetJSON(t, resource, name, namespace)

	spec, _ := obj["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	metadata, _ := template["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	val, _ := annotations[annotation].(string)
	return val
}

func getWorkloadContainerResources(t *testing.T, resource, name, namespace string) map[string]any {
	t.Helper()
	obj := kubectlGetJSON(t, resource, name, namespace)

	spec, _ := obj["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	if len(containers) == 0 {
		t.Fatalf("%s %s has no containers", resource, name)
	}
	container, _ := containers[0].(map[string]any)
	resources, _ := container["resources"].(map[string]any)
	return resources
}

func getObjectUID(t *testing.T, obj map[string]any) string {
	t.Helper()
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		t.Fatal("object has no metadata")
	}
	uid, _ := metadata["uid"].(string)
	if uid == "" {
		t.Fatal("object has no metadata.uid")
	}
	return uid
}

func hasOwnerUID(obj map[string]any, uid string) bool {
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return false
	}
	rawRefs, ok := metadata["ownerReferences"].([]any)
	if !ok {
		return false
	}
	for _, rawRef := range rawRefs {
		ref, ok := rawRef.(map[string]any)
		if !ok {
			continue
		}
		refUID, _ := ref["uid"].(string)
		if refUID == uid {
			return true
		}
	}
	return false
}

func waitForResourceCountAtLeast(t *testing.T, resource, namespace, selector string, minCount int, timeout time.Duration) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		items := kubectlListJSON(t, resource, namespace, selector)
		if len(items) >= minCount {
			return items
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for at least %d %s resources with selector %q", minCount, resource, selector)
	return nil
}

func waitForResourcesGoneBySelector(t *testing.T, resource, namespace, selector string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(kubectlListJSON(t, resource, namespace, selector)) == 0 {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for %s resources with selector %q to be deleted", resource, selector)
}

func podReady(obj map[string]any) bool {
	status, ok := obj["status"].(map[string]any)
	if !ok {
		return false
	}
	rawConditions, ok := status["conditions"].([]any)
	if !ok {
		return false
	}
	for _, rawCondition := range rawConditions {
		condition, ok := rawCondition.(map[string]any)
		if !ok {
			continue
		}
		typ, _ := condition["type"].(string)
		value, _ := condition["status"].(string)
		if typ == "Ready" && value == "True" {
			return true
		}
	}
	return false
}

func waitForComponentPodsReady(t *testing.T, namespace, instance, component string, timeout time.Duration) {
	t.Helper()
	selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=%s", instance, component)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods := kubectlListJSON(t, "pods", namespace, selector)
		if len(pods) == 0 {
			time.Sleep(pollInterval)
			continue
		}

		allReady := true
		for _, pod := range pods {
			if !podReady(pod) {
				allReady = false
				break
			}
		}
		if allReady {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for %s pods to become Ready", component)
}

func waitForServiceEndpointsReady(t *testing.T, serviceName, namespace string, timeout time.Duration) {
	t.Helper()
	selector := fmt.Sprintf("kubernetes.io/service-name=%s", serviceName)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		endpointSlices := kubectlListJSON(t, "endpointslice", namespace, selector)
		for _, endpointSlice := range endpointSlices {
			endpoints, ok := endpointSlice["endpoints"].([]any)
			if !ok {
				continue
			}
			for _, rawEndpoint := range endpoints {
				endpoint, ok := rawEndpoint.(map[string]any)
				if !ok {
					continue
				}
				if rawAddresses, ok := endpoint["addresses"].([]any); ok && len(rawAddresses) > 0 {
					conditions, _ := endpoint["conditions"].(map[string]any)
					ready, _ := conditions["ready"].(bool)
					if ready {
						return
					}
				}
			}
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out waiting for endpoints on service %s", serviceName)
}

func getServicePortByName(t *testing.T, serviceName, namespace, portName string) int {
	t.Helper()
	service := kubectlGetJSON(t, "service", serviceName, namespace)
	spec, ok := service["spec"].(map[string]any)
	if !ok {
		t.Fatalf("service %s has no spec", serviceName)
	}
	rawPorts, ok := spec["ports"].([]any)
	if !ok {
		t.Fatalf("service %s has no spec.ports", serviceName)
	}
	for _, rawPort := range rawPorts {
		port, ok := rawPort.(map[string]any)
		if !ok {
			continue
		}
		name, _ := port["name"].(string)
		value, _ := port["port"].(float64)
		if name == portName {
			return int(value)
		}
	}
	t.Fatalf("service %s has no port named %s", serviceName, portName)
	return 0
}

func getServicePortByAnyName(t *testing.T, serviceName, namespace string, portNames ...string) int {
	t.Helper()
	service := kubectlGetJSON(t, "service", serviceName, namespace)
	spec, ok := service["spec"].(map[string]any)
	if !ok {
		t.Fatalf("service %s has no spec", serviceName)
	}
	rawPorts, ok := spec["ports"].([]any)
	if !ok {
		t.Fatalf("service %s has no spec.ports", serviceName)
	}
	for _, candidate := range portNames {
		for _, rawPort := range rawPorts {
			port, ok := rawPort.(map[string]any)
			if !ok {
				continue
			}
			name, _ := port["name"].(string)
			value, _ := port["port"].(float64)
			if name == candidate {
				return int(value)
			}
		}
	}
	t.Fatalf("service %s has no port named any of %v", serviceName, portNames)
	return 0
}

func withServicePortForward(t *testing.T, namespace, serviceName string, remotePort int, fn func(baseURL string)) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate local port: %v", err)
	}
	localPort := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("close local port listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, "kubectl", "-n", namespace, "port-forward", "service/"+serviceName, fmt.Sprintf("%d:%d", localPort, remotePort))
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start port-forward for service %s: %v", serviceName, err)
	}

	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	address := fmt.Sprintf("127.0.0.1:%d", localPort)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			fn("http://" + address)
			return
		}

		select {
		case <-done:
			t.Fatalf("port-forward for service %s exited early: %v\n%s", serviceName, waitErr, output.String())
		default:
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for service %s port-forward\n%s", serviceName, output.String())
}

func requireHTTPStatus(t *testing.T, url string, want int) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != want {
		t.Fatalf("GET %s returned status %d, want %d", url, resp.StatusCode, want)
	}
}

func requireHTTPSuccess(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("GET %s returned status %d, want 2xx", url, resp.StatusCode)
	}
}

func cloneExistingCR(t *testing.T, sourceName, namespace, targetName string) {
	t.Helper()
	obj := kubectlGetJSON(t, llmBatchGatewayKind, sourceName, namespace)

	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("source CR %s has no metadata", sourceName)
	}
	metadata["name"] = targetName
	metadata["namespace"] = namespace
	delete(metadata, "uid")
	delete(metadata, "resourceVersion")
	delete(metadata, "generation")
	delete(metadata, "creationTimestamp")
	delete(metadata, "managedFields")
	delete(metadata, "annotations")

	delete(obj, "status")
	kubectlCreateObject(t, obj)
}
