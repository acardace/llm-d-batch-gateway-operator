package asyncprocessor_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	"github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/asyncprocessor"
)

const (
	testGWName     = "my-gateway"
	testNamespace  = "default"
	testSecretName = "my-gateway-credentials"
	testImage      = "ghcr.io/llm-d-incubation/llm-d-async:v0.7.0-RC3"
	testIGWURL     = "http://vllm-sim.default.svc.cluster.local:8000"
)

func newGateway(ap *batchv1alpha1.AsyncProcessorSpec) *batchv1alpha1.LLMBatchGateway {
	gw := &batchv1alpha1.LLMBatchGateway{}
	gw.Name = testGWName
	gw.Namespace = testNamespace
	gw.Spec.Processor.GlobalInferenceGateway = &batchv1alpha1.InferenceGatewaySpec{
		URL: testIGWURL,
	}
	gw.Spec.AsyncProcessor = ap
	return gw
}

func defaultSpec() *batchv1alpha1.AsyncProcessorSpec {
	return &batchv1alpha1.AsyncProcessorSpec{
		Image:          testImage,
		Concurrency:    8,
		RequestTimeout: "5m",
		PollIntervalMs: 1000,
		BatchSize:      10,
		Replicas:       ptr.To(int32(1)),
	}
}

func kindsOf(objects []*unstructured.Unstructured) []string {
	kinds := make([]string, len(objects))
	for i, o := range objects {
		kinds[i] = o.GetKind()
	}
	return kinds
}

func findKind(t *testing.T, objects []*unstructured.Unstructured, kind string) map[string]interface{} {
	t.Helper()
	for _, obj := range objects {
		if obj.GetKind() == kind {
			return obj.Object
		}
	}
	t.Fatalf("no %s found in objects", kind)
	return nil
}

func deploymentArgs(t *testing.T, objects []*unstructured.Unstructured) []string {
	t.Helper()
	deploy := findKind(t, objects, "Deployment")
	containers := deploy["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})["containers"].([]interface{})
	rawArgs := containers[0].(map[string]interface{})["args"].([]interface{})
	args := make([]string, len(rawArgs))
	for i, a := range rawArgs {
		args[i] = a.(string)
	}
	return args
}

func assertArg(t *testing.T, args []string, want string) {
	t.Helper()
	if !slices.Contains(args, want) {
		t.Errorf("missing arg %q in %v", want, args)
	}
}

func assertArgPrefix(t *testing.T, args []string, prefix string) {
	t.Helper()
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return
		}
	}
	t.Errorf("no arg with prefix %q in %v", prefix, args)
}

func assertNoArgPrefix(t *testing.T, args []string, prefix string) {
	t.Helper()
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			t.Errorf("unexpected arg with prefix %q: %q", prefix, a)
			return
		}
	}
}

func TestBuild_NilSpec(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(nil), testSecretName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if objects != nil {
		t.Errorf("expected nil when async processor disabled, got %d objects", len(objects))
	}
}

func TestBuild_SingleQueueMode(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(defaultSpec()), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	kinds := kindsOf(objects)
	if !slices.Contains(kinds, "ServiceAccount") {
		t.Error("expected ServiceAccount")
	}
	if !slices.Contains(kinds, "Deployment") {
		t.Error("expected Deployment")
	}
	if slices.Contains(kinds, "ConfigMap") {
		t.Error("unexpected ConfigMap in single-queue mode")
	}
	if len(objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(objects))
	}
}

func TestBuild_MultiQueueMode(t *testing.T) {
	spec := defaultSpec()
	spec.Queues = map[string]batchv1alpha1.AsyncProcessorQueueSpec{
		"model-a": {InferenceGatewayURL: "http://gw-a:8000", QueueName: "queue-a"},
		"model-b": {InferenceGatewayURL: "http://gw-b:8000", QueueName: "queue-b"},
	}

	objects, err := asyncprocessor.Build(newGateway(spec), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if !slices.Contains(kindsOf(objects), "ConfigMap") {
		t.Error("expected ConfigMap in multi-queue mode")
	}
	if len(objects) != 3 {
		t.Errorf("expected 3 objects (SA, ConfigMap, Deployment), got %d", len(objects))
	}
}

func TestBuild_MultiQueueConfigMapJSON(t *testing.T) {
	spec := defaultSpec()
	spec.Queues = map[string]batchv1alpha1.AsyncProcessorQueueSpec{
		"model-a": {
			InferenceGatewayURL: "http://gw-a:8000",
			QueueName:           "queue-a",
			ResultQueueName:     "result-a",
			GateType:            "prometheus-saturation",
			GateParams:          map[string]string{"threshold": "0.8"},
		},
	}

	objects, err := asyncprocessor.Build(newGateway(spec), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	cm := findKind(t, objects, "ConfigMap")
	jsonStr := cm["data"].(map[string]interface{})["queues.json"].(string)

	var configs []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &configs); err != nil {
		t.Fatalf("invalid queues.json: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("expected 1 queue config, got %d", len(configs))
	}
	cfg := configs[0]
	if cfg["id"] != "model-a" {
		t.Errorf("id = %v, want model-a", cfg["id"])
	}
	if cfg["igw_base_url"] != "http://gw-a:8000" {
		t.Errorf("igw_base_url = %v, want http://gw-a:8000", cfg["igw_base_url"])
	}
	if cfg["gate_type"] != "prometheus-saturation" {
		t.Errorf("gate_type = %v, want prometheus-saturation", cfg["gate_type"])
	}
}

func TestBuild_SingleQueueArgs(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(defaultSpec()), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	args := deploymentArgs(t, objects)
	assertArg(t, args, "--message-queue-impl=redis-sortedset")
	assertArg(t, args, "--concurrency=8")
	assertArg(t, args, "--request-timeout=5m")
	assertArg(t, args, "--redis.ss.poll-interval-ms=1000")
	assertArg(t, args, "--redis.ss.batch-size=10")
	assertArg(t, args, "--redis.ss.igw-base-url="+testIGWURL)
	assertNoArgPrefix(t, args, "--redis.ss.queues-config-file")
}

func TestBuild_MultiQueueArgs(t *testing.T) {
	spec := defaultSpec()
	spec.Queues = map[string]batchv1alpha1.AsyncProcessorQueueSpec{
		"q": {InferenceGatewayURL: "http://gw:8000"},
	}

	objects, err := asyncprocessor.Build(newGateway(spec), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	args := deploymentArgs(t, objects)
	assertArgPrefix(t, args, "--redis.ss.queues-config-file=")
	assertNoArgPrefix(t, args, "--redis.ss.igw-base-url")
}

func TestBuild_PrometheusArgs(t *testing.T) {
	spec := defaultSpec()
	spec.Prometheus = &batchv1alpha1.AsyncProcessorPrometheusSpec{
		URL:      "http://prometheus:9090",
		CacheTTL: "10s",
	}

	objects, err := asyncprocessor.Build(newGateway(spec), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	args := deploymentArgs(t, objects)
	assertArg(t, args, "--prometheus-url=http://prometheus:9090")
	assertArg(t, args, "--prometheus-cache-ttl=10s")
}

func TestBuild_NoPrometheusArgs(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(defaultSpec()), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	args := deploymentArgs(t, objects)
	assertNoArgPrefix(t, args, "--prometheus-url")
	assertNoArgPrefix(t, args, "--prometheus-cache-ttl")
}

func TestBuild_RedisURLFromSecret(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(defaultSpec()), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	deploy := findKind(t, objects, "Deployment")
	containers := deploy["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})["containers"].([]interface{})
	env := containers[0].(map[string]interface{})["env"].([]interface{})

	for _, e := range env {
		entry := e.(map[string]interface{})
		if entry["name"] == "REDIS_URL" {
			ref := entry["valueFrom"].(map[string]interface{})["secretKeyRef"].(map[string]interface{})
			if ref["name"] != testSecretName {
				t.Errorf("secret name = %v, want %v", ref["name"], testSecretName)
			}
			if ref["key"] != "redis-url" {
				t.Errorf("secret key = %v, want redis-url", ref["key"])
			}
			return
		}
	}
	t.Error("REDIS_URL env var not found")
}

func TestBuild_Labels(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(defaultSpec()), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	for _, obj := range objects {
		labels := obj.GetLabels()
		if labels["app.kubernetes.io/instance"] != testGWName {
			t.Errorf("%s: instance = %q, want %q", obj.GetKind(), labels["app.kubernetes.io/instance"], testGWName)
		}
		if labels["app.kubernetes.io/component"] != "asyncprocessor" {
			t.Errorf("%s: component = %q, want asyncprocessor", obj.GetKind(), labels["app.kubernetes.io/component"])
		}
	}
}

func TestBuild_SecurityContext(t *testing.T) {
	objects, err := asyncprocessor.Build(newGateway(defaultSpec()), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	deploy := findKind(t, objects, "Deployment")
	podSpec := deploy["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
	sc := podSpec["securityContext"].(map[string]interface{})
	if sc["runAsNonRoot"] != true {
		t.Error("expected runAsNonRoot: true")
	}

	containers := podSpec["containers"].([]interface{})
	csc := containers[0].(map[string]interface{})["securityContext"].(map[string]interface{})
	if csc["allowPrivilegeEscalation"] != false {
		t.Error("expected allowPrivilegeEscalation: false")
	}
	if csc["readOnlyRootFilesystem"] != true {
		t.Error("expected readOnlyRootFilesystem: true")
	}
}

func TestBuild_CustomReplicas(t *testing.T) {
	spec := defaultSpec()
	spec.Replicas = ptr.To(int32(3))

	objects, err := asyncprocessor.Build(newGateway(spec), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	deploy := findKind(t, objects, "Deployment")
	replicas, _, _ := unstructured.NestedInt64(deploy, "spec", "replicas")
	if replicas != 3 {
		t.Errorf("replicas = %v, want 3", replicas)
	}
}

func TestBuild_Resources(t *testing.T) {
	spec := defaultSpec()
	spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}

	objects, err := asyncprocessor.Build(newGateway(spec), testSecretName)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	deploy := findKind(t, objects, "Deployment")
	containers := deploy["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})["containers"].([]interface{})
	resources := containers[0].(map[string]interface{})["resources"].(map[string]interface{})
	requests := resources["requests"].(map[string]interface{})
	if requests["cpu"] == nil {
		t.Error("expected cpu request to be set")
	}
	if requests["memory"] == nil {
		t.Error("expected memory request to be set")
	}
}
