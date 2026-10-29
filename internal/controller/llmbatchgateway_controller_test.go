package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	tlspkg "github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/tls"
)

const (
	llmBatchGatewayKind     = "LLMBatchGateway"
	secretKind              = "Secret"
	testDuration30s         = "30s"
	testInferenceGatewayURL = "http://epp:8081"
)

func newTestGateway(name string) *batchv1alpha1.LLMBatchGateway {
	concurrency := int32(8)
	return &batchv1alpha1.LLMBatchGateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: batchv1alpha1.LLMBatchGatewaySpec{
			SecretRef: corev1.SecretReference{Name: "test-secrets"},
			DBBackend: dbBackendPostgreSQL,
			FileStorage: &batchv1alpha1.FileStorageSpec{
				S3: &batchv1alpha1.S3StorageSpec{
					Region:   "us-east-1",
					Bucket:   "llm-d-batch-gateway",
					Endpoint: "https://s3.example.com",
				},
			},
			APIServer: batchv1alpha1.APIServerSpec{
				Replicas: ptr.To(int32(1)),
			},
			Processor: batchv1alpha1.ProcessorSpec{
				Replicas: ptr.To(int32(1)),
				ModelGateways: map[string]batchv1alpha1.ModelGatewaySpec{
					"sim-model": {InferencePoolName: "sim-pool"},
				},
				AsyncConfig: &batchv1alpha1.AsyncProcessorSpec{
					Concurrency:       &concurrency,
					DrainTimeout:      "2m",
					ResultPollTimeout: "30s",
					InferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
						URL: testInferenceGatewayURL,
					},
					Redis: &batchv1alpha1.AsyncRedisSpec{},
				},
			},
			GC: batchv1alpha1.GCSpec{
				Interval: "30m",
			},
		},
	}
}

func TestValidateSpec(t *testing.T) {
	t.Run("valid async gateway", func(t *testing.T) {
		gw := newTestGateway("test-valid-async")
		if err := validateSpec(gw); err != nil {
			t.Fatalf("validateSpec() error = %v, want nil", err)
		}
	})

	t.Run("rejects missing asyncConfig", func(t *testing.T) {
		gw := newTestGateway("test-no-asyncconfig")
		gw.Spec.Processor.AsyncConfig = nil

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "processor.asyncConfig is required") {
			t.Fatalf("validateSpec() error = %v, want asyncConfig required error", err)
		}
	})

	t.Run("rejects missing resultPollTimeout", func(t *testing.T) {
		gw := newTestGateway("test-no-poll-timeout")
		gw.Spec.Processor.AsyncConfig.ResultPollTimeout = ""

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "resultPollTimeout is required") {
			t.Fatalf("validateSpec() error = %v, want resultPollTimeout required error", err)
		}
	})

	t.Run("rejects missing modelGateways", func(t *testing.T) {
		gw := newTestGateway("test-no-modelgateways")
		gw.Spec.Processor.ModelGateways = nil

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "processor.modelGateways is required") {
			t.Fatalf("validateSpec() error = %v, want modelGateways required error", err)
		}
	})

	t.Run("rejects modelGateway without inferencePoolName", func(t *testing.T) {
		gw := newTestGateway("test-no-pool-name")
		gw.Spec.Processor.ModelGateways["sim-model"] = batchv1alpha1.ModelGatewaySpec{}

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "processor.modelGateways[sim-model].inferencePoolName is required") {
			t.Fatalf("validateSpec() error = %v, want inferencePoolName required error", err)
		}
	})

	t.Run("rejects mismatched queue names", func(t *testing.T) {
		gw := newTestGateway("test-queue-pairing")
		gw.Spec.Processor.ModelGateways["sim-model"] = batchv1alpha1.ModelGatewaySpec{
			InferencePoolName: "sim-pool",
			RequestQueueName:  "requests",
		}

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "requestQueueName and resultQueueName must both be set or both be empty") {
			t.Fatalf("validateSpec() error = %v, want queue name pairing error", err)
		}
	})
}

func TestValidateSpecRejectsBothAsyncTransports(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}

	if err := validateSpec(g); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("validateSpec() error = %v, want mutually exclusive transport error", err)
	}
}

func TestValidateSpecRequiresAsyncTransport(t *testing.T) {
	g := newTestAsyncGateway("test-async-no-transport")
	g.Spec.Processor.AsyncConfig.Redis = nil
	if err := validateSpec(g); err == nil || !strings.Contains(err.Error(), "redis or processor.asyncConfig.sql is required") {
		t.Fatalf("validateSpec() error = %v, want missing transport error", err)
	}
}

// TestValidateSpecRedis covers the Redis transport rules, which mirror the SQL
// ones: queue coverage, an inference gateway URL and Prometheus gates.
func TestValidateSpecRedis(t *testing.T) {
	base := func() *batchv1alpha1.LLMBatchGateway {
		g := minimalAsyncGateway()
		g.Spec.Processor.Config = nil
		g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
		g.Spec.Processor.AsyncConfig.Redis = &batchv1alpha1.AsyncRedisSpec{}
		g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
			"model-a": {InferencePoolName: "pool-a"},
			"model-b": {InferencePoolName: "pool-b"},
		}
		return g
	}

	t.Run("derived queues need the shared gateway URL", func(t *testing.T) {
		g := base()
		g.Spec.Processor.AsyncConfig.InferenceGateway = nil

		// Derived queues carry no igwBaseURL, so the first one to be checked
		// reports the missing URL.
		err := validateSpec(g)
		if err == nil || !strings.Contains(err.Error(), "processor.asyncConfig.inferenceGateway.url is required when Redis queue") {
			t.Fatalf("validateSpec() error = %v, want missing gateway URL error", err)
		}
	})

	t.Run("derived queues accepted with shared gateway URL", func(t *testing.T) {
		g := base()
		g.Spec.Processor.AsyncConfig.InferenceGateway = &batchv1alpha1.InferenceGatewaySpec{URL: testInferenceGatewayURL}

		if err := validateSpec(g); err != nil {
			t.Fatalf("validateSpec() error = %v, want nil", err)
		}
	})

	t.Run("per-queue gateway URLs are enough", func(t *testing.T) {
		g := base()
		g.Spec.Processor.AsyncConfig.InferenceGateway = nil
		g.Spec.Processor.AsyncConfig.Redis.QueuesConfig = []batchv1alpha1.AsyncQueueConfig{
			{Name: asyncRequestQueueName("pool-a"), IGWBaseURL: "http://a:8000"},
			{Name: asyncRequestQueueName("pool-b"), IGWBaseURL: "http://b:8000"},
		}

		if err := validateSpec(g); err != nil {
			t.Fatalf("validateSpec() error = %v, want nil", err)
		}
	})

	t.Run("queuesConfig must cover every model queue", func(t *testing.T) {
		g := base()
		g.Spec.Processor.AsyncConfig.InferenceGateway = &batchv1alpha1.InferenceGatewaySpec{URL: testInferenceGatewayURL}
		g.Spec.Processor.AsyncConfig.Redis.QueuesConfig = []batchv1alpha1.AsyncQueueConfig{
			{Name: asyncRequestQueueName("pool-a")},
		}

		err := validateSpec(g)
		want := `processor.asyncConfig.redis.queuesConfig is missing request queue "llm-d-async:requests:pool-b" required by processor.modelGateways[model-b]`
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("validateSpec() error = %v, want %q", err, want)
		}
	})

	t.Run("explicit requestQueueName is used for coverage", func(t *testing.T) {
		g := base()
		g.Spec.Processor.AsyncConfig.InferenceGateway = &batchv1alpha1.InferenceGatewaySpec{URL: testInferenceGatewayURL}
		g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
			"model-a": {InferencePoolName: "pool-a", RequestQueueName: "custom-requests", ResultQueueName: "custom-results"},
		}
		g.Spec.Processor.AsyncConfig.Redis.QueuesConfig = []batchv1alpha1.AsyncQueueConfig{
			{Name: asyncRequestQueueName("pool-a")},
		}

		err := validateSpec(g)
		if err == nil || !strings.Contains(err.Error(), `missing request queue "custom-requests"`) {
			t.Fatalf("validateSpec() error = %v, want coverage error for the explicit queue name", err)
		}
	})

	t.Run("duplicate queuesConfig entries rejected", func(t *testing.T) {
		g := base()
		g.Spec.Processor.AsyncConfig.InferenceGateway = &batchv1alpha1.InferenceGatewaySpec{URL: testInferenceGatewayURL}
		name := asyncRequestQueueName("pool-a")
		g.Spec.Processor.AsyncConfig.Redis.QueuesConfig = []batchv1alpha1.AsyncQueueConfig{
			{Name: name},
			{Name: name},
		}

		err := validateSpec(g)
		if err == nil || !strings.Contains(err.Error(), `processor.asyncConfig.redis.queuesConfig contains duplicate queue`) {
			t.Fatalf("validateSpec() error = %v, want duplicate queue error", err)
		}
	})
}

// TestRedisQueuesDerivedFromModels pins the effective dispatcher queues used for
// both rendering and validation.
func TestRedisQueuesDerivedFromModels(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = &batchv1alpha1.AsyncRedisSpec{
		RequestPathURL: "/v1/chat/completions",
		GateType:       "prometheus-saturation",
		GateParams:     map[string]string{"pool": "p"},
	}
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
		"model-b": {InferencePoolName: "pool-b", RequestQueueName: "shared-requests", ResultQueueName: "shared-results"},
	}
	g.Spec.Processor.ModelGateways["model-c"] = batchv1alpha1.ModelGatewaySpec{
		InferencePoolName: "pool-c",
		RequestQueueName:  "shared-requests",
		ResultQueueName:   "other-results",
	}

	queues := redisQueues(g)
	got := make([]string, 0, len(queues))
	for _, q := range queues {
		got = append(got, q.Name)
		if q.RequestPathURL != "/v1/chat/completions" {
			t.Errorf("queue %s requestPathURL = %q, want the top-level value", q.Name, q.RequestPathURL)
		}
		if q.GateType != "prometheus-saturation" {
			t.Errorf("queue %s gateType = %q, want the top-level value", q.Name, q.GateType)
		}
	}

	// One queue per distinct request queue name, in model order.
	want := []string{asyncRequestQueueName("pool-a"), "shared-requests"}
	if len(got) != len(want) {
		t.Fatalf("redisQueues() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("redisQueues()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestAsyncTLSAdmission pins the asyncConfig.tls invariants. Fixtures are
// unstructured so an explicit "" survives omitempty and reaches the API server.
func TestAsyncTLSAdmission(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		tls         map[string]any
		wantInvalid bool
		wantMessage string
	}{
		{name: "secret with CA", tls: map[string]any{"secretName": "igw-tls", "caCertKey": "ca.crt"}},
		{name: "secret with mTLS pair", tls: map[string]any{"secretName": "igw-tls", "certKey": "tls.crt", "keyKey": "tls.key"}},
		{name: "insecure skip verify alone", tls: map[string]any{"insecureSkipVerify": true}},
		{
			name:        "CA key without secret",
			tls:         map[string]any{"caCertKey": "ca.crt"},
			wantInvalid: true,
			wantMessage: "tls.secretName is required",
		},
		{
			name:        "client cert without secret",
			tls:         map[string]any{"certKey": "tls.crt", "keyKey": "tls.key"},
			wantInvalid: true,
			wantMessage: "tls.secretName is required",
		},
		{
			name:        "client cert without key",
			tls:         map[string]any{"secretName": "igw-tls", "certKey": "tls.crt"},
			wantInvalid: true,
			wantMessage: "tls.certKey and tls.keyKey must both be set or both be empty",
		},
		{
			name:        "client key without cert",
			tls:         map[string]any{"secretName": "igw-tls", "keyKey": "tls.key"},
			wantInvalid: true,
			wantMessage: "tls.certKey and tls.keyKey must both be set or both be empty",
		},
		{
			// has() is true for "", so MinLength is what rejects this.
			name:        "explicit empty cert and key",
			tls:         map[string]any{"secretName": "igw-tls", "certKey": "", "keyKey": ""},
			wantInvalid: true,
		},
		{
			name:        "explicit empty cert with real key",
			tls:         map[string]any{"secretName": "igw-tls", "certKey": "", "keyKey": "tls.key"},
			wantInvalid: true,
		},
		{
			name:        "explicit empty CA key",
			tls:         map[string]any{"secretName": "igw-tls", "caCertKey": ""},
			wantInvalid: true,
		},
		{
			name:        "explicit empty secret name with cert keys",
			tls:         map[string]any{"secretName": "", "certKey": "tls.crt", "keyKey": "tls.key"},
			wantInvalid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{}
			obj.SetUnstructuredContent(map[string]any{
				"apiVersion": batchv1alpha1.GroupVersion.String(),
				"kind":       llmBatchGatewayKind,
				"metadata": map[string]any{
					"name":      "test-tls-" + strings.ReplaceAll(strings.ToLower(tt.name), " ", "-"),
					"namespace": "default",
				},
				"spec": map[string]any{
					"secretRef":   map[string]any{"name": "test-secrets"},
					"fileStorage": map[string]any{"s3": map[string]any{"bucket": "llm-d-batch-gateway"}},
					"apiServer":   map[string]any{},
					"gc":          map[string]any{},
					"processor": map[string]any{
						"asyncConfig": map[string]any{
							"redis": map[string]any{},
							"tls":   tt.tls,
						},
					},
				},
			})

			err := k8sClient.Create(ctx, obj)
			if !tt.wantInvalid {
				if err != nil {
					t.Fatalf("Create() error = %v, want nil", err)
				}
				t.Cleanup(func() { _ = k8sClient.Delete(ctx, obj) })
				return
			}
			if !apierrors.IsInvalid(err) {
				t.Fatalf("Create() error = %v, want Invalid", err)
			}
			if tt.wantMessage != "" && !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("Create() error = %v, want message %q", err, tt.wantMessage)
			}
		})
	}
}

// TestDispatchModeDeprecatedField pins the compatibility shim: manifests that
// still set dispatchMode: async keep applying (kubectl rejects unknown fields,
// so dropping the field would break them), while sync is refused.
func TestDispatchModeDeprecatedField(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		dispatchMode string
		wantInvalid  bool
	}{
		{name: "absent"},
		{name: "async", dispatchMode: "async"},
		{name: "sync", dispatchMode: "sync", wantInvalid: true},
		{name: "unknown", dispatchMode: "parallel", wantInvalid: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := map[string]any{
				"asyncConfig": map[string]any{"redis": map[string]any{}},
			}
			if tt.dispatchMode != "" {
				processor["dispatchMode"] = tt.dispatchMode
			}

			obj := &unstructured.Unstructured{}
			obj.SetUnstructuredContent(map[string]any{
				"apiVersion": batchv1alpha1.GroupVersion.String(),
				"kind":       llmBatchGatewayKind,
				"metadata": map[string]any{
					"name":      "test-dispatchmode-" + strings.ToLower(tt.name),
					"namespace": "default",
				},
				"spec": map[string]any{
					"secretRef":   map[string]any{"name": "test-secrets"},
					"fileStorage": map[string]any{"s3": map[string]any{"bucket": "llm-d-batch-gateway"}},
					"apiServer":   map[string]any{},
					"gc":          map[string]any{},
					"processor":   processor,
				},
			})

			err := k8sClient.Create(ctx, obj)
			if tt.wantInvalid {
				if !apierrors.IsInvalid(err) {
					t.Fatalf("Create() with dispatchMode %q error = %v, want Invalid", tt.dispatchMode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Create() with dispatchMode %q error = %v, want nil", tt.dispatchMode, err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, obj) })
		})
	}
}

func TestAsyncTransportAdmission(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		redis       bool
		sql         bool
		wantInvalid bool
	}{
		{name: "neither transport", wantInvalid: true},
		{name: "redis", redis: true},
		{name: "sql", sql: true},
		{name: "both", redis: true, sql: true, wantInvalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := newTestAsyncGateway("test-transport-" + strings.ReplaceAll(strings.ToLower(tt.name), " ", "-"))
			if tt.redis {
				gw.Spec.Processor.AsyncConfig.Redis = &batchv1alpha1.AsyncRedisSpec{}
			}
			if tt.sql {
				gw.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}
			}
			err := k8sClient.Create(ctx, gw)
			if tt.wantInvalid {
				if !apierrors.IsInvalid(err) {
					t.Fatalf("Create() error = %v, want Invalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Create() error = %v, want nil", err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })
		})
	}
}

func TestValidateSpecAllowsSQLDistinctResultQueues(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
		"model-b": {InferencePoolName: "pool-b"},
	}

	if err := validateSpec(g); err != nil {
		t.Fatalf("validateSpec() error = %v, want nil", err)
	}
}

func TestValidateSpecRejectsSQLSharedResultQueue(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a", ResultQueueName: "result-shared", RequestQueueName: "request-a"},
		"model-b": {InferencePoolName: "pool-b", ResultQueueName: "result-shared", RequestQueueName: "request-b"},
	}

	if err := validateSpec(g); err == nil || !strings.Contains(err.Error(), "same SQL result queue") {
		t.Fatalf("validateSpec() error = %v, want shared SQL result queue error", err)
	}
}

func TestValidateSpecRejectsSQLSharedInferencePool(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "shared-pool", RequestQueueName: "request-a", ResultQueueName: "result-a"},
		"model-b": {InferencePoolName: "shared-pool", RequestQueueName: "request-b", ResultQueueName: "result-b"},
	}

	if err := validateSpec(g); err == nil || !strings.Contains(err.Error(), "same SQL inference pool") {
		t.Fatalf("validateSpec() error = %v, want shared SQL inference pool error", err)
	}
}

func TestValidateSpecAllowsSQLRequestQueueCoverage(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{
			{Name: "request-a"},
			{Name: "request-b"},
		},
	}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {
			InferencePoolName: "pool-a",
			RequestQueueName:  "request-a",
			ResultQueueName:   "result-a",
		},
		"model-b": {
			InferencePoolName: "pool-b",
			RequestQueueName:  "request-b",
			ResultQueueName:   "result-b",
		},
	}

	if err := validateSpec(g); err != nil {
		t.Fatalf("validateSpec() error = %v, want nil", err)
	}
}

func TestValidateSpecRejectsSQLDuplicateQueueNames(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{
			{Name: "request-a", IGWBaseURL: testInferenceGatewayURL},
			{Name: "request-a", IGWBaseURL: testInferenceGatewayURL},
		},
	}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {
			InferencePoolName: "pool-a",
			RequestQueueName:  "request-a",
			ResultQueueName:   "result-a",
		},
	}

	err := validateSpec(g)
	if err == nil || !strings.Contains(err.Error(), `duplicate queue "request-a"`) {
		t.Fatalf("validateSpec() error = %v, want duplicate SQL queue error", err)
	}
}

func TestValidateSpecSQLPrometheusGateRequiresURL(t *testing.T) {
	tests := []struct {
		name       string
		queueGate  string
		workerGate string
		url        string
		wantError  bool
	}{
		{name: "queue budget without URL", queueGate: "prometheus-budget", wantError: true},
		{name: "queue saturation without URL", queueGate: "prometheus-saturation", wantError: true},
		{name: "queue query without URL", queueGate: "prometheus-query", wantError: true},
		{name: "worker budget without URL", workerGate: "prometheus-budget", wantError: true},
		{name: "worker saturation without URL", workerGate: "prometheus-saturation", wantError: true},
		{name: "worker query without URL", workerGate: "prometheus-query", wantError: true},
		{name: "queue gate with URL", queueGate: "prometheus-budget", url: "http://prometheus:9090"},
		{name: "worker gate with URL", workerGate: "prometheus-saturation", url: "http://prometheus:9090"},
		{name: "other gate without URL", queueGate: "none"},
		{name: "no gate without URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := minimalAsyncGateway()
			g.Spec.Processor.AsyncConfig.Redis = nil
			g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
				QueuesConfig: []batchv1alpha1.AsyncQueueConfig{{Name: asyncRequestQueueName("pool-a"), GateType: tt.queueGate}},
			}
			g.Spec.Processor.AsyncConfig.WorkerPools = []batchv1alpha1.AsyncWorkerPool{{Name: "default", Workers: 1, GateType: tt.workerGate}}
			g.Spec.Processor.AsyncConfig.PrometheusURL = tt.url
			g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
			g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
				"model-a": {InferencePoolName: "pool-a"},
			}

			err := validateSpec(g)
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), "prometheusURL") {
					t.Fatalf("validateSpec() error = %v, want missing prometheusURL error", err)
				}
			} else if err != nil {
				t.Fatalf("validateSpec() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateSpecRequiresPrometheusURLForRedisGates(t *testing.T) {
	tests := []struct {
		name          string
		pools         []batchv1alpha1.AsyncWorkerPool
		queueGate     string
		prometheusURL string
		wantMessage   string
	}{
		{
			name:        "queue gate without prometheusURL",
			queueGate:   "prometheus-saturation",
			wantMessage: `prometheusURL is required for Redis queue`,
		},
		{
			name:        "worker pool gate without prometheusURL",
			pools:       []batchv1alpha1.AsyncWorkerPool{{Name: "default", Workers: 1, GateType: "prometheus-budget"}},
			wantMessage: `prometheusURL is required for worker pool`,
		},
		{
			name:          "prometheusURL set",
			queueGate:     "prometheus-saturation",
			prometheusURL: "http://prometheus:9090",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := minimalAsyncGateway()
			g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
			g.Spec.Processor.AsyncConfig.WorkerPools = tt.pools
			g.Spec.Processor.AsyncConfig.PrometheusURL = tt.prometheusURL
			g.Spec.Processor.AsyncConfig.Redis.GateType = tt.queueGate
			g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
				"model-a": {InferencePoolName: "pool-a"},
			}

			err := validateSpec(g)
			if tt.wantMessage == "" {
				if err != nil {
					t.Fatalf("validateSpec() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf("validateSpec() error = %v, want %q", err, tt.wantMessage)
			}
		})
	}
}

func TestValidateSpecAllowsSQLDefaultRequestQueueCoverage(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{
			{Name: asyncRequestQueueName("pool-a")},
		},
	}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
	}

	if err := validateSpec(g); err != nil {
		t.Fatalf("validateSpec() error = %v, want nil", err)
	}
}

func TestValidateSpecRejectsSQLMissingRequestQueue(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{{Name: "request-a"}},
	}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {
			InferencePoolName: "pool-a",
			RequestQueueName:  "request-a",
			ResultQueueName:   "result-a",
		},
		"model-b": {
			InferencePoolName: "pool-b",
			RequestQueueName:  "request-b",
			ResultQueueName:   "result-b",
		},
	}

	err := validateSpec(g)
	if err == nil || !strings.Contains(err.Error(), `missing request queue "request-b"`) {
		t.Fatalf("validateSpec() error = %v, want missing SQL request queue error", err)
	}
}

func TestValidateSpecRejectsSQLMissingDefaultRequestQueue(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{{Name: "request-other"}},
	}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
	}

	err := validateSpec(g)
	if err == nil || !strings.Contains(err.Error(), `missing request queue "`+asyncRequestQueueName("pool-a")+`"`) {
		t.Fatalf("validateSpec() error = %v, want missing default SQL request queue error", err)
	}
}

func TestLLMBatchGatewayResumableFinalizeTimeoutValidation(t *testing.T) {
	ctx := context.Background()
	gw := newTestGateway("test-resumable-finalize-timeout")
	gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		Resumable: &batchv1alpha1.ResumableConfigSpec{
			FinalizeTimeout: "0s",
		},
	}

	err := k8sClient.Create(ctx, gw)
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Create() with finalizeTimeout 0s error = %v, want Invalid", err)
	}

	gw.Spec.Processor.Config.Resumable.FinalizeTimeout = "1s"
	if err := k8sClient.Create(ctx, gw); err != nil {
		t.Fatalf("Create() with finalizeTimeout 1s: %v", err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, gw)
	})
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}
	asyncHelmRenderer, err := NewHelmRenderer("../../llm-d-async/charts/llm-d-async", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer(async) error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	resyncTimeout := 5 * time.Minute
	reconcileTimeout := 30 * time.Second

	reconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, asyncHelmRenderer, fakeRecorder, resyncTimeout, reconcileTimeout)

	t.Run("renders async dispatch mode in the processor config", func(t *testing.T) {
		gw := newTestGateway("test-renders-async")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		var stored batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &stored); err != nil {
			t.Fatalf("getting stored CR: %v", err)
		}

		values := specToBatchHelmValues(&stored, testSecretName(&stored), testImages(), tlspkg.ProfileValues{})
		processor, ok := values["processor"].(map[string]any)
		if !ok {
			t.Fatal("rendered processor config missing")
		}
		config, ok := processor["config"].(map[string]any)
		if !ok {
			t.Fatal("rendered processor config section missing")
		}
		if got := config["dispatchMode"]; got != dispatchModeAsync {
			t.Errorf("rendered dispatchMode = %v, want %q", got, dispatchModeAsync)
		}
		ad, ok := config["asyncDispatch"].(map[string]any)
		if !ok {
			t.Fatal("rendered asyncDispatch config missing")
		}
		if got := ad["resultPollTimeout"]; got != "30s" {
			t.Errorf("rendered asyncDispatch.resultPollTimeout = %v, want 30s", got)
		}
	})

	t.Run("returns RequeueAfter on successful reconcile", func(t *testing.T) {
		gw := newTestGateway("test-requeue")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}
		if result.RequeueAfter != resyncTimeout {
			t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, resyncTimeout)
		}
	})

	t.Run("creates all child resources", func(t *testing.T) {
		gw := newTestGateway("test-create")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		t.Run("deployments", func(t *testing.T) {
			var deployList appsv1.DeploymentList
			if err := k8sClient.List(ctx, &deployList); err != nil {
				t.Fatalf("listing deployments: %v", err)
			}
			deployCount := 0
			hasAsyncProcessor := false
			for _, d := range deployList.Items {
				if isOwnedByUID(d.OwnerReferences, gw.UID) {
					deployCount++
					if d.Labels[labelKeyComponent] == componentAsyncProcessor {
						hasAsyncProcessor = true
					}
				}
			}
			// 2 batch Deployments (apiserver, gc) + 1 async-processor Deployment.
			if deployCount != 3 {
				t.Errorf("deployment count = %d, want 3", deployCount)
			}
			if !hasAsyncProcessor {
				t.Error("no async-processor deployment found")
			}
		})

		t.Run("processor statefulset", func(t *testing.T) {
			var statefulSetList appsv1.StatefulSetList
			if err := k8sClient.List(ctx, &statefulSetList); err != nil {
				t.Fatalf("listing statefulsets: %v", err)
			}
			count := 0
			for _, ss := range statefulSetList.Items {
				if isOwnedByUID(ss.OwnerReferences, gw.UID) {
					count++
				}
			}
			if count != 1 {
				t.Errorf("statefulset count = %d, want 1", count)
			}
		})

		t.Run("configmaps", func(t *testing.T) {
			var cmList corev1.ConfigMapList
			if err := k8sClient.List(ctx, &cmList); err != nil {
				t.Fatalf("listing configmaps: %v", err)
			}
			cmCount := 0
			for _, cm := range cmList.Items {
				if isOwnedByUID(cm.OwnerReferences, gw.UID) {
					cmCount++
				}
			}
			if cmCount < 3 {
				t.Errorf("configmap count = %d, want >= 3", cmCount)
			}
		})

		t.Run("service accounts", func(t *testing.T) {
			var saList corev1.ServiceAccountList
			if err := k8sClient.List(ctx, &saList); err != nil {
				t.Fatalf("listing serviceaccounts: %v", err)
			}
			saCount := 0
			for _, sa := range saList.Items {
				if isOwnedByUID(sa.OwnerReferences, gw.UID) {
					saCount++
				}
			}
			// 3 batch ServiceAccounts (apiserver, processor, gc) + 1 async-processor.
			if saCount != 4 {
				t.Errorf("serviceaccount count = %d, want 4", saCount)
			}
		})

		t.Run("service", func(t *testing.T) {
			var svcList corev1.ServiceList
			if err := k8sClient.List(ctx, &svcList); err != nil {
				t.Fatalf("listing services: %v", err)
			}
			svcCount := 0
			for _, svc := range svcList.Items {
				if isOwnedByUID(svc.OwnerReferences, gw.UID) {
					svcCount++
				}
			}
			if svcCount != 2 {
				t.Errorf("service count = %d, want 2", svcCount)
			}
		})
	})

	t.Run("sets owner references", func(t *testing.T) {
		gw := newTestGateway("test-owner")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		var deployList appsv1.DeploymentList
		if err := k8sClient.List(ctx, &deployList); err != nil {
			t.Fatalf("listing deployments: %v", err)
		}

		for _, d := range deployList.Items {
			if !isOwnedByUID(d.OwnerReferences, gw.UID) {
				continue
			}
			found := false
			for _, ref := range d.OwnerReferences {
				if ref.UID == gw.UID && ref.Kind == llmBatchGatewayKind && ref.Controller != nil && *ref.Controller {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("deployment %s missing controller owner reference", d.Name)
			}
		}
	})

	t.Run("sets status conditions", func(t *testing.T) {
		gw := newTestGateway("test-status")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		conditionTypes := map[string]bool{}
		for _, c := range updated.Status.Conditions {
			conditionTypes[c.Type] = true
		}

		for _, ct := range []string{conditionReady, conditionAPIServerAvailable, conditionProcessorAvailable, conditionGCAvailable, conditionAsyncProcessorAvailable} {
			if !conditionTypes[ct] {
				t.Errorf("missing condition %q", ct)
			}
		}

		if updated.Status.ObservedGeneration != updated.Generation {
			t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
		}
	})

	t.Run("updates on spec change", func(t *testing.T) {
		gw := newTestGateway("test-update")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.APIServer.Replicas = ptr.To(int32(3))
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		var deployList appsv1.DeploymentList
		if err := k8sClient.List(ctx, &deployList); err != nil {
			t.Fatalf("listing deployments: %v", err)
		}

		for _, d := range deployList.Items {
			if !isOwnedByUID(d.OwnerReferences, gw.UID) {
				continue
			}
			component := d.Labels[labelKeyComponent]
			if component == componentAPIServer {
				if d.Spec.Replicas == nil || *d.Spec.Replicas != 3 {
					replicas := int32(0)
					if d.Spec.Replicas != nil {
						replicas = *d.Spec.Replicas
					}
					t.Errorf("apiserver replicas = %d, want 3", replicas)
				}
			}
		}
	})

	t.Run("deletes orphaned resources on spec change", func(t *testing.T) {
		gw := newTestGateway("test-orphan")
		gw.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		var cmList corev1.ConfigMapList
		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps: %v", err)
		}
		cmCountBefore := 0
		hasDashboard := false
		for _, cm := range cmList.Items {
			if !isOwnedByUID(cm.OwnerReferences, gw.UID) {
				continue
			}
			cmCountBefore++
			if cm.Labels["grafana_dashboard"] == "1" {
				hasDashboard = true
			}
		}
		if !hasDashboard {
			t.Fatal("expected grafana dashboard ConfigMap to exist after first reconcile")
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: false}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps after: %v", err)
		}
		cmCountAfter := 0
		for _, cm := range cmList.Items {
			if !isOwnedByUID(cm.OwnerReferences, gw.UID) {
				continue
			}
			cmCountAfter++
			if cm.Labels["grafana_dashboard"] == "1" {
				t.Error("grafana dashboard ConfigMap should have been deleted")
			}
		}
		if cmCountAfter != cmCountBefore-2 {
			t.Errorf("configmap count = %d, want %d (batch and async dashboards removed)", cmCountAfter, cmCountBefore-2)
		}
	})

	t.Run("sets ValidationFailed when no model gateways configured", func(t *testing.T) {
		gw := newTestGateway("test-validation-none")
		gw.Spec.Processor.ModelGateways = nil
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for validation failure, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		for _, condType := range []string{conditionReady, conditionAPIServerAvailable, conditionProcessorAvailable, conditionGCAvailable, conditionAsyncProcessorAvailable} {
			found := false
			for _, c := range updated.Status.Conditions {
				if c.Type == condType {
					found = true
					if c.Status != metav1.ConditionFalse {
						t.Errorf("%s status = %v, want False", condType, c.Status)
					}
					if c.Reason != reasonValidationFailed {
						t.Errorf("%s reason = %v, want ValidationFailed", condType, c.Reason)
					}
					break
				}
			}
			if !found {
				t.Errorf("missing condition %s", condType)
			}
		}
		if updated.Status.ObservedGeneration != updated.Generation {
			t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
		}

		assertWarningEvent(t, fakeRecorder, reasonValidationFailed)
	})

	t.Run("sets ReferenceNotPermitted when no ReferenceGrant exists", func(t *testing.T) {
		gw := newTestGateway("test-refnotpermitted")
		// Cross-namespace secretRef — no ReferenceGrant is present.
		gw.Spec.SecretRef = corev1.SecretReference{Name: "src-secret", Namespace: "other-ns"}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for permanent condition, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}
		for _, c := range updated.Status.Conditions {
			if c.Type == conditionReady {
				if c.Status != metav1.ConditionFalse {
					t.Errorf("Ready status = %v, want False", c.Status)
				}
				if c.Reason != reasonReferenceNotPermitted {
					t.Errorf("Ready reason = %q, want %q", c.Reason, reasonReferenceNotPermitted)
				}
				if updated.Status.ObservedGeneration != updated.Generation {
					t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
				}
				assertWarningEvent(t, fakeRecorder, reasonReferenceNotPermitted)
				return
			}
		}
		t.Error("missing Ready condition")
	})

	t.Run("sets SecretRefImmutable when managed copy annotation mismatches", func(t *testing.T) {
		gwName := "test-secretrefimmutable"

		// Create the managed copy with an annotation pointing at a different source.
		existingCopy := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gwName + managedSecretSuffix,
				Namespace: "default",
				Annotations: map[string]string{
					"batch.llm-d.ai/copied-from": "other-ns/old-secret",
				},
			},
		}
		if err := k8sClient.Create(ctx, existingCopy); err != nil {
			t.Fatalf("creating pre-existing managed copy: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, existingCopy) })

		gw := newTestGateway(gwName)
		// Cross-namespace secretRef pointing at a different secret than the copy.
		gw.Spec.SecretRef = corev1.SecretReference{Name: "new-secret", Namespace: "other-ns"}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for permanent condition, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}
		for _, c := range updated.Status.Conditions {
			if c.Type == conditionReady {
				if c.Status != metav1.ConditionFalse {
					t.Errorf("Ready status = %v, want False", c.Status)
				}
				if c.Reason != reasonSecretRefImmutable {
					t.Errorf("Ready reason = %q, want %q", c.Reason, reasonSecretRefImmutable)
				}
				if updated.Status.ObservedGeneration != updated.Generation {
					t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
				}
				assertWarningEvent(t, fakeRecorder, reasonSecretRefImmutable)
				return
			}
		}
		t.Error("missing Ready condition")
	})

	t.Run("updates processor replicas on spec change", func(t *testing.T) {
		gw := newTestGateway("test-proc-replicas")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.Processor.Replicas = ptr.To(int32(5))
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		assertProcessorStatefulSetReplicas(ctx, t, gw, 5)
	})

	t.Run("updates apiserver resources on spec change", func(t *testing.T) {
		gw := newTestGateway("test-api-resources")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.APIServer.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		d := findOwnedDeployment(ctx, t, gw, "apiserver")
		containers := d.Spec.Template.Spec.Containers
		if len(containers) == 0 {
			t.Fatal("apiserver deployment has no containers")
		}
		res := containers[0].Resources
		assertResourceValue(t, "apiserver requests.cpu", res.Requests, corev1.ResourceCPU, "500m")
		assertResourceValue(t, "apiserver requests.memory", res.Requests, corev1.ResourceMemory, "256Mi")
		assertResourceValue(t, "apiserver limits.cpu", res.Limits, corev1.ResourceCPU, "1")
		assertResourceValue(t, "apiserver limits.memory", res.Limits, corev1.ResourceMemory, "512Mi")
	})

	t.Run("unsupported dbBackend is rejected", func(t *testing.T) {
		gw := newTestGateway("test-dbbackend")
		gw.Spec.DBBackend = "redis"
		if err := validateSpec(gw); err == nil || !strings.Contains(err.Error(), "only \"postgresql\" is supported") {
			t.Fatalf("validateSpec() error = %v, want PostgreSQL-only error", err)
		}
	})

	t.Run("missing asyncConfig is rejected by the CRD", func(t *testing.T) {
		gw := newTestGateway("test-no-asyncconfig-crd")
		gw.Spec.Processor.AsyncConfig = nil

		err := k8sClient.Create(ctx, gw)
		if !apierrors.IsInvalid(err) {
			t.Fatalf("Create() without asyncConfig error = %v, want Invalid", err)
		}
		if !strings.Contains(err.Error(), "asyncConfig") {
			t.Errorf("Create() error = %v, want an asyncConfig validation error", err)
		}
	})

	t.Run("apiserver config change updates ConfigMap and Deployment", func(t *testing.T) {
		gw := newTestGateway("test-api-config")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		apiBefore := findOwnedDeployment(ctx, t, gw, "apiserver")
		checksumBefore := apiBefore.Spec.Template.Annotations["checksum/config"]

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.APIServer.Config = &batchv1alpha1.APIServerConfigSpec{
			Port:                9090,
			ReadTimeoutSeconds:  120,
			WriteTimeoutSeconds: 180,
			EnablePprof:         true,
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "apiserver")
		assertConfigMapContains(t, cm, `port: "9090"`)
		assertConfigMapContains(t, cm, "read_timeout_seconds: 120")
		assertConfigMapContains(t, cm, "write_timeout_seconds: 180")
		assertConfigMapContains(t, cm, "enable_pprof: true")

		apiAfter := findOwnedDeployment(ctx, t, gw, "apiserver")
		checksumAfter := apiAfter.Spec.Template.Annotations["checksum/config"]
		if checksumAfter == checksumBefore {
			t.Error("apiserver deployment pod template not updated after config change")
		}

		// The Deployment must expose the new port on the container.
		found := false
		for _, p := range apiAfter.Spec.Template.Spec.Containers[0].Ports {
			if p.Name == "http" && p.ContainerPort == 9090 {
				found = true
			}
		}
		if !found {
			t.Error("apiserver deployment container port 'http' not updated to 9090")
		}
	})

	t.Run("processor config change updates ConfigMap and StatefulSet", func(t *testing.T) {
		gw := newTestGateway("test-proc-config")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		procBefore := findOwnedProcessorStatefulSet(ctx, t, gw)
		checksumBefore := procBefore.Spec.Template.Annotations["checksum/config"]

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
			NumWorkers: 50,
			Concurrency: &batchv1alpha1.ConcurrencyConfig{
				Global:      200,
				PerEndpoint: 25,
			},
			DefaultOutputExpirationSeconds: 7200,
			EnablePprof:                    true,
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "processor")
		assertConfigMapContains(t, cm, "num_workers: 50")
		assertConfigMapContains(t, cm, "global: 200")
		assertConfigMapContains(t, cm, "per_endpoint: 25")
		assertConfigMapContains(t, cm, "default_output_expiration_seconds: 7200")
		assertConfigMapContains(t, cm, "enable_pprof: true")

		procAfter := findOwnedProcessorStatefulSet(ctx, t, gw)
		checksumAfter := procAfter.Spec.Template.Annotations["checksum/config"]
		if checksumAfter == checksumBefore {
			t.Error("processor statefulset pod template not updated after config change")
		}
	})

	t.Run("gc config change updates ConfigMap and Deployment", func(t *testing.T) {
		gw := newTestGateway("test-gc-config")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		gcBefore := findOwnedDeployment(ctx, t, gw, "gc")
		checksumBefore := gcBefore.Spec.Template.Annotations["checksum/config"]

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.GC.Interval = "1h"
		gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{
			DryRun:         true,
			MaxConcurrency: 10,
			Reconciler: &batchv1alpha1.ReconcilerSpec{
				Enabled:  true,
				Interval: "90m",
			},
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "gc")
		assertConfigMapContains(t, cm, `interval: "1h"`)
		assertConfigMapContains(t, cm, "dry_run: true")
		assertConfigMapContains(t, cm, "max_concurrency: 10")
		assertConfigMapContains(t, cm, "enabled: true")
		assertConfigMapContains(t, cm, `interval: "90m"`)

		gcAfter := findOwnedDeployment(ctx, t, gw, "gc")
		checksumAfter := gcAfter.Spec.Template.Annotations["checksum/config"]
		if checksumAfter == checksumBefore {
			t.Error("gc deployment pod template not updated after config change")
		}
	})

	t.Run("does not delete resources owned by a different CR", func(t *testing.T) {
		gwA := newTestGateway("test-orphan-a")
		gwA.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}
		gwB := newTestGateway("test-orphan-b")
		gwB.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}

		if err := k8sClient.Create(ctx, gwA); err != nil {
			t.Fatalf("creating CR A: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gwA) })
		if err := k8sClient.Create(ctx, gwB); err != nil {
			t.Fatalf("creating CR B: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gwB) })

		nnA := types.NamespacedName{Name: gwA.Name, Namespace: gwA.Namespace}
		nnB := types.NamespacedName{Name: gwB.Name, Namespace: gwB.Namespace}

		if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nnA}); err != nil {
			t.Fatalf("Reconcile A: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nnB}); err != nil {
			t.Fatalf("Reconcile B: %v", err)
		}

		var cmList corev1.ConfigMapList
		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps: %v", err)
		}
		cmCountB := 0
		for _, cm := range cmList.Items {
			if isOwnedByUID(cm.OwnerReferences, gwB.UID) {
				cmCountB++
			}
		}

		if err := k8sClient.Get(ctx, nnA, gwA); err != nil {
			t.Fatalf("getting CR A: %v", err)
		}
		gwA.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: false}
		if err := k8sClient.Update(ctx, gwA); err != nil {
			t.Fatalf("updating CR A: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nnA}); err != nil {
			t.Fatalf("Reconcile A after update: %v", err)
		}

		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps after: %v", err)
		}
		cmCountBAfter := 0
		for _, cm := range cmList.Items {
			if isOwnedByUID(cm.OwnerReferences, gwB.UID) {
				cmCountBAfter++
			}
		}
		if cmCountBAfter != cmCountB {
			t.Errorf("CR B configmap count changed from %d to %d", cmCountB, cmCountBAfter)
		}
	})
}

// newTestAsyncGateway returns a gateway whose asyncConfig carries only an
// inference gateway, so individual tests add the transport (Redis or SQL)
// they exercise.
func newTestAsyncGateway(name string) *batchv1alpha1.LLMBatchGateway {
	gw := newTestGateway(name)
	concurrency := int32(8)
	gw.Spec.Processor.AsyncConfig = &batchv1alpha1.AsyncProcessorSpec{
		Concurrency:       &concurrency,
		DrainTimeout:      "2m",
		ResultPollTimeout: testDuration30s,
		InferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
			URL: testInferenceGatewayURL,
		},
	}
	return gw
}

// TestRemovedModelGatewayFieldsArePruned pins the narrowed modelGateways API:
// fields that the operator no longer renders (sync-era gateway tuning knobs)
// are dropped by the API server instead of being accepted and silently ignored.
func TestRemovedModelGatewayFieldsArePruned(t *testing.T) {
	ctx := context.Background()

	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(map[string]any{
		"apiVersion": batchv1alpha1.GroupVersion.String(),
		"kind":       llmBatchGatewayKind,
		"metadata": map[string]any{
			"name":      "test-pruned-fields",
			"namespace": "default",
		},
		"spec": map[string]any{
			"secretRef": map[string]any{"name": "test-secrets"},
			// Removed top-level fields: pruned silently, not rejected.
			"redis":       map[string]any{"enableTLS": true, "insecure": true},
			"fileStorage": map[string]any{"s3": map[string]any{"bucket": "llm-d-batch-gateway"}},
			"apiServer":   map[string]any{},
			"gc":          map[string]any{},
			"processor": map[string]any{
				"globalInferenceGateway": map[string]any{
					"url": "http://legacy-gateway:8000",
				},
				"modelGateways": map[string]any{
					"model-a": map[string]any{
						"inferencePoolName": "pool-a",
						// Removed sync-era knobs: must not survive admission.
						"requestTimeout":        "5m",
						"maxRetries":            3,
						"initialBackoff":        "1s",
						"maxBackoff":            "60s",
						"tlsCACertFile":         "/ca.crt",
						"tlsClientCertFile":     "/tls.crt",
						"tlsClientKeyFile":      "/tls.key",
						"tlsInsecureSkipVerify": false,
						"url":                   "http://gateway:8000",
					},
				},
				"asyncConfig": map[string]any{
					"resultPollTimeout": "30s",
					// A transport is required; Redis is enough for this fixture.
					"redis": map[string]any{},
					// asyncConfig.inferenceGateway only keeps url.
					"inferenceGateway": map[string]any{
						"url":               "http://epp:8081",
						"inferencePoolName": "not-a-pool",
					},
				},
			},
		},
	})

	if err := k8sClient.Create(ctx, obj); err != nil {
		t.Fatalf("creating CR with removed fields: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, obj) })

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(batchv1alpha1.GroupVersion.WithKind(llmBatchGatewayKind))
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, got); err != nil {
		t.Fatalf("getting stored CR: %v", err)
	}

	for _, removed := range []string{"redis"} {
		if _, ok := got.Object["spec"].(map[string]any)[removed]; ok {
			t.Errorf("spec.%s still present after admission, want pruned", removed)
		}
	}
	processorSpec := got.Object["spec"].(map[string]any)["processor"].(map[string]any)
	// dispatchMode is not asserted here: it is retained as a deprecated
	// async-only field, covered by TestDispatchModeDeprecatedField.
	if _, ok := processorSpec["globalInferenceGateway"]; ok {
		t.Error("spec.processor.globalInferenceGateway still present after admission, want pruned")
	}

	model, found, err := unstructured.NestedMap(got.Object, "spec", "processor", "modelGateways", "model-a")
	if err != nil || !found {
		t.Fatalf("reading modelGateways.model-a: found=%v err=%v", found, err)
	}
	if got := model["inferencePoolName"]; got != "pool-a" {
		t.Errorf("modelGateways.model-a.inferencePoolName = %v, want pool-a", got)
	}
	for _, removed := range []string{
		"url", "requestTimeout", "maxRetries", "initialBackoff", "maxBackoff",
		"tlsCACertFile", "tlsClientCertFile", "tlsClientKeyFile", "tlsInsecureSkipVerify",
	} {
		if _, ok := model[removed]; ok {
			t.Errorf("modelGateways.model-a still carries removed field %q", removed)
		}
	}

	igw, found, err := unstructured.NestedMap(got.Object, "spec", "processor", "asyncConfig", "inferenceGateway")
	if err != nil || !found {
		t.Fatalf("reading asyncConfig.inferenceGateway: found=%v err=%v", found, err)
	}
	if len(igw) != 1 || igw["url"] != "http://epp:8081" {
		t.Errorf("asyncConfig.inferenceGateway = %v, want only url=http://epp:8081", igw)
	}
}

// TestSamplesValidateAgainstCRD dry-run creates every sample manifest against
// the installed CRD, mirroring the integration workflow's sample validation
// step. Without this, a sample that drifts from the schema (for example one
// still setting a removed field) only fails in CI, minutes into a cluster
// deploy.
func TestSamplesValidateAgainstCRD(t *testing.T) {
	ctx := context.Background()

	paths, err := filepath.Glob("../../config/samples/batch_v1alpha1_*.yaml")
	if err != nil {
		t.Fatalf("globbing samples: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no sample manifests found")
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}

			decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
			found := 0
			for {
				var obj unstructured.Unstructured
				if err := decoder.Decode(&obj); err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					t.Fatalf("decoding %s: %v", path, err)
				}
				if len(obj.Object) == 0 || obj.GetKind() != llmBatchGatewayKind {
					continue
				}
				found++

				if obj.GetNamespace() == "" {
					obj.SetNamespace("default")
				}
				// Strict validation matches `kubectl apply`, which is what the
				// integration workflow runs; a plain create would prune unknown
				// fields instead of rejecting them.
				if err := k8sClient.Create(ctx, &obj, client.DryRunAll,
					client.FieldValidation(metav1.FieldValidationStrict)); err != nil {
					if apierrors.IsAlreadyExists(err) {
						continue
					}
					t.Errorf("%s (%s/%s) rejected by the CRD schema: %v", path, obj.GetKind(), obj.GetName(), err)
				}
			}
			if found == 0 {
				t.Errorf("%s contains no %s manifest", path, llmBatchGatewayKind)
			}
		})
	}
}

func TestAllConditionTypes(t *testing.T) {
	got := allConditionTypes()

	want := []string{
		conditionReady,
		conditionAPIServerAvailable,
		conditionProcessorAvailable,
		conditionGCAvailable,
		conditionAsyncProcessorAvailable,
	}
	if len(got) != len(want) {
		t.Fatalf("allConditionTypes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("allConditionTypes()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRecordFailure(t *testing.T) {
	ctx := context.Background()

	gw := newTestGateway("test-record-failure")
	if err := k8sClient.Create(ctx, gw); err != nil {
		t.Fatalf("creating CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

	recorder := record.NewFakeRecorder(10)
	r := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), nil, nil, recorder, time.Minute, time.Minute)

	failure := statusFailure{
		reason:     reasonRenderFailed,
		message:    "batch-gateway chart render failed: boom",
		conditions: allConditionTypes(),
	}
	if err := r.recordFailure(ctx, gw, failure); err != nil {
		t.Fatalf("recordFailure() error: %v", err)
	}

	t.Run("marks every condition false", func(t *testing.T) {
		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		for _, condType := range allConditionTypes() {
			cond := meta.FindStatusCondition(updated.Status.Conditions, condType)
			if cond == nil {
				t.Errorf("condition %s not set", condType)
				continue
			}
			if cond.Status != metav1.ConditionFalse {
				t.Errorf("%s status = %v, want False", condType, cond.Status)
			}
			if cond.Reason != failure.reason {
				t.Errorf("%s reason = %q, want %q", condType, cond.Reason, failure.reason)
			}
			if cond.Message != failure.message {
				t.Errorf("%s message = %q, want %q", condType, cond.Message, failure.message)
			}
			if cond.ObservedGeneration != updated.Generation {
				t.Errorf("%s observedGeneration = %d, want %d", condType, cond.ObservedGeneration, updated.Generation)
			}
		}
	})

	t.Run("emits a warning event", func(t *testing.T) {
		select {
		case ev := <-recorder.Events:
			if !strings.Contains(ev, corev1.EventTypeWarning) ||
				!strings.Contains(ev, failure.reason) ||
				!strings.Contains(ev, failure.message) {
				t.Errorf("event = %q, want warning with reason %q and message %q", ev, failure.reason, failure.message)
			}
		default:
			t.Error("expected a warning event, got none")
		}
	})
}

// TestReconcileRedisValidationFailure covers the invalid Redis shapes end to
// end: the CR is accepted by the CRD, reconcile refuses to deploy workloads and
// reports ValidationFailed with an actionable message.
func TestReconcileRedisValidationFailure(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer(batch) error: %v", err)
	}
	asyncHelmRenderer, err := NewHelmRenderer("../../llm-d-async/charts/llm-d-async", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer(async) error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(200)
	reconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(),
		batchGWHelmRenderer, asyncHelmRenderer, fakeRecorder, 5*time.Minute, 30*time.Second)

	tests := []struct {
		name        string
		mutate      func(*batchv1alpha1.LLMBatchGateway)
		wantMessage string
	}{
		{
			name: "derived queues without a gateway URL",
			mutate: func(g *batchv1alpha1.LLMBatchGateway) {
				g.Spec.Processor.AsyncConfig.InferenceGateway = nil
			},
			wantMessage: "processor.asyncConfig.inferenceGateway.url is required",
		},
		{
			name: "queuesConfig does not cover the model queue",
			mutate: func(g *batchv1alpha1.LLMBatchGateway) {
				g.Spec.Processor.ModelGateways["model-b"] = batchv1alpha1.ModelGatewaySpec{InferencePoolName: "pool-b"}
				g.Spec.Processor.AsyncConfig.Redis.QueuesConfig = []batchv1alpha1.AsyncQueueConfig{
					{Name: asyncRequestQueueName("pool-a"), IGWBaseURL: testInferenceGatewayURL},
				}
			},
			wantMessage: "processor.asyncConfig.redis.queuesConfig is missing request queue",
		},
		{
			name: "prometheus gate without prometheusURL",
			mutate: func(g *batchv1alpha1.LLMBatchGateway) {
				g.Spec.Processor.AsyncConfig.Redis.GateType = "prometheus-saturation"
			},
			wantMessage: "processor.asyncConfig.prometheusURL is required for Redis queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := newTestGateway("test-redis-validation-" + strings.ReplaceAll(strings.ToLower(tt.name), " ", "-"))
			tt.mutate(gw)

			if err := k8sClient.Create(ctx, gw); err != nil {
				t.Fatalf("creating CR: %v", err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

			// A validation failure is reported through status, not as a
			// reconcile error, and must happen before any workload is applied.
			if _, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
			}); err != nil {
				t.Fatalf("Reconcile() error = %v, want nil", err)
			}

			var updated batchv1alpha1.LLMBatchGateway
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
				t.Fatalf("getting updated CR: %v", err)
			}

			readyCond := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
			if readyCond == nil {
				t.Fatal("Ready condition missing")
			}
			if readyCond.Reason != reasonValidationFailed {
				t.Errorf("Ready reason = %q, want %q (message: %s)", readyCond.Reason, reasonValidationFailed, readyCond.Message)
			}
			if !strings.Contains(readyCond.Message, tt.wantMessage) {
				t.Errorf("Ready message = %q, want it to contain %q", readyCond.Message, tt.wantMessage)
			}

			var deployments appsv1.DeploymentList
			if err := k8sClient.List(ctx, &deployments); err != nil {
				t.Fatalf("listing deployments: %v", err)
			}
			for i := range deployments.Items {
				d := &deployments.Items[i]
				if isOwnedBy(d, gw) && d.Labels[labelKeyComponent] == componentAsyncProcessor {
					t.Error("async-processor Deployment must not be created for an invalid spec")
				}
			}
			assertWarningEvent(t, fakeRecorder, reasonValidationFailed)
		})
	}
}

func TestReconcileNilAsyncRenderer(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	resyncTimeout := 5 * time.Minute
	reconcileTimeout := 30 * time.Second

	nilAsyncReconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, nil, fakeRecorder, resyncTimeout, reconcileTimeout)

	gw := newTestGateway("test-async-nil-renderer")
	if err := k8sClient.Create(ctx, gw); err != nil {
		t.Fatalf("creating CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

	_, err = nilAsyncReconciler.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
	})
	if err == nil {
		t.Fatal("Reconcile() expected error for nil async renderer, got nil")
	}
	if !strings.Contains(err.Error(), "async helm renderer") {
		t.Errorf("error = %v, want mention of async helm renderer", err)
	}
}

func TestConditionHelpers(t *testing.T) {
	if got := conditionStatus(true); got != metav1.ConditionTrue {
		t.Errorf("conditionStatus(true) = %v, want True", got)
	}
	if got := conditionStatus(false); got != metav1.ConditionFalse {
		t.Errorf("conditionStatus(false) = %v, want False", got)
	}
	if got := conditionReason(true, "Yes", "No"); got != "Yes" {
		t.Errorf("conditionReason(true) = %v, want Yes", got)
	}
	if got := conditionReason(false, "Yes", "No"); got != "No" {
		t.Errorf("conditionReason(false) = %v, want No", got)
	}
	if got := conditionMessage(true, "ok", "bad"); got != "ok" {
		t.Errorf("conditionMessage(true) = %v, want ok", got)
	}
	if got := conditionMessage(false, "ok", "bad"); got != "bad" {
		t.Errorf("conditionMessage(false) = %v, want bad", got)
	}
}

func TestSetComponentAvailableCondition(t *testing.T) {
	tests := []struct {
		name        string
		component   componentCondition
		status      *batchv1alpha1.ComponentReplicaStatus
		wantReady   bool
		wantReason  string
		wantMessage string
	}{
		{
			name:        "single replica ready",
			component:   processorCondition,
			status:      &batchv1alpha1.ComponentReplicaStatus{Replicas: 1, ReadyReplicas: 1},
			wantReady:   true,
			wantReason:  reasonAvailable,
			wantMessage: "Processor: 1/1 replicas ready",
		},
		{
			name:        "all replicas ready",
			component:   apiServerCondition,
			status:      &batchv1alpha1.ComponentReplicaStatus{Replicas: 3, ReadyReplicas: 3},
			wantReady:   true,
			wantReason:  reasonAvailable,
			wantMessage: "API server: 3/3 replicas ready",
		},
		{
			name:        "partial rollout is not available",
			component:   processorCondition,
			status:      &batchv1alpha1.ComponentReplicaStatus{Replicas: 2, ReadyReplicas: 1},
			wantReady:   false,
			wantReason:  reasonUnavailable,
			wantMessage: "Processor: 1/2 replicas ready",
		},
		{
			name:        "no ready replicas",
			component:   asyncProcessorCondition,
			status:      &batchv1alpha1.ComponentReplicaStatus{Replicas: 1},
			wantReady:   false,
			wantReason:  reasonUnavailable,
			wantMessage: "Async processor: 0/1 replicas ready",
		},
		{
			name:        "scaled to zero is not available",
			component:   apiServerCondition,
			status:      &batchv1alpha1.ComponentReplicaStatus{Replicas: 0, ReadyReplicas: 0},
			wantReady:   false,
			wantReason:  reasonUnavailable,
			wantMessage: "API server: 0/0 replicas ready",
		},
		{
			name:        "component not deployed",
			component:   gcCondition,
			status:      nil,
			wantReady:   false,
			wantReason:  reasonUnavailable,
			wantMessage: "GC: 0/0 replicas ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := newTestGateway("test-availability")
			gw.Generation = 7

			gotReady := setComponentAvailableCondition(gw, tt.component, tt.status)
			if gotReady != tt.wantReady {
				t.Errorf("available = %v, want %v", gotReady, tt.wantReady)
			}

			cond := meta.FindStatusCondition(gw.Status.Conditions, tt.component.conditionType)
			if cond == nil {
				t.Fatal("condition not set")
			}
			wantStatus := metav1.ConditionFalse
			if tt.wantReady {
				wantStatus = metav1.ConditionTrue
			}
			if cond.Status != wantStatus {
				t.Errorf("status = %v, want %v", cond.Status, wantStatus)
			}
			if cond.Reason != tt.wantReason {
				t.Errorf("reason = %v, want %v", cond.Reason, tt.wantReason)
			}
			if cond.Message != tt.wantMessage {
				t.Errorf("message = %q, want %q", cond.Message, tt.wantMessage)
			}
			if cond.ObservedGeneration != gw.Generation {
				t.Errorf("observedGeneration = %d, want %d", cond.ObservedGeneration, gw.Generation)
			}
		})
	}
}

func TestIsOwnedBy(t *testing.T) {
	gw := newTestGateway("gw")
	gw.UID = "test-uid"

	owned := &corev1.ConfigMap{}
	owned.OwnerReferences = []metav1.OwnerReference{{UID: gw.UID}}

	notOwned := &corev1.ConfigMap{}

	if !isOwnedBy(owned, gw) {
		t.Error("isOwnedBy: expected true for owned object")
	}
	if isOwnedBy(notOwned, gw) {
		t.Error("isOwnedBy: expected false for unowned object")
	}
}

func isOwnedByUID(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

func TestReconcileTimeout(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	resyncTimeout := 5 * time.Minute
	reconcileTimeout := -1 * time.Second
	// Use a 1ns timeout so the context is expired before the first API call.
	reconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, nil, fakeRecorder, resyncTimeout, reconcileTimeout)

	gw := newTestGateway("test-timeout")
	if err := k8sClient.Create(ctx, gw); err != nil {
		t.Fatalf("creating CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

	_, err = reconciler.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
	})
	if err == nil {
		t.Fatal("Reconcile() expected error due to timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Reconcile() error = %v, want context.DeadlineExceeded", err)
	}
}

// getOwnedConfigMap returns the ConfigMap for the given component that is
// owned by gw. ConfigMap names follow the pattern <name>-batch-gateway-<component>-config.
func getOwnedConfigMap(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, component string) *corev1.ConfigMap {
	t.Helper()
	cmName := gw.Name + "-batch-gateway-" + component + "-config"
	var cm corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: gw.Namespace}, &cm); err != nil {
		t.Fatalf("getting %s configmap %s: %v", component, cmName, err)
	}
	if !isOwnedByUID(cm.OwnerReferences, gw.UID) {
		t.Fatalf("configmap %s is not owned by %s", cmName, gw.Name)
	}
	return &cm
}

// assertConfigMapContains verifies that the config.yaml data in the ConfigMap
// contains the expected substring.
func assertConfigMapContains(t *testing.T, cm *corev1.ConfigMap, want string) {
	t.Helper()
	data := cm.Data["config.yaml"]
	if !strings.Contains(data, want) {
		t.Errorf("configmap %s config.yaml missing %q\ngot:\n%s", cm.Name, want, data)
	}
}

// findOwnedDeployment returns the Deployment with the given component label
// that is owned by gw. It fails the test if no such Deployment is found.
func findOwnedDeployment(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, component string) *appsv1.Deployment {
	t.Helper()
	var deployList appsv1.DeploymentList
	if err := k8sClient.List(ctx, &deployList); err != nil {
		t.Fatalf("listing deployments: %v", err)
	}
	for i := range deployList.Items {
		d := &deployList.Items[i]
		if !isOwnedByUID(d.OwnerReferences, gw.UID) {
			continue
		}
		if d.Labels[labelKeyComponent] == component {
			return d
		}
	}
	t.Fatalf("no deployment found with component=%s owned by %s", component, gw.Name)
	return nil
}

func findOwnedProcessorStatefulSet(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway) *appsv1.StatefulSet {
	t.Helper()
	var statefulSetList appsv1.StatefulSetList
	if err := k8sClient.List(ctx, &statefulSetList); err != nil {
		t.Fatalf("listing statefulsets: %v", err)
	}
	for i := range statefulSetList.Items {
		ss := &statefulSetList.Items[i]
		if !isOwnedByUID(ss.OwnerReferences, gw.UID) {
			continue
		}
		if ss.Labels[labelKeyComponent] == componentProcessor {
			return ss
		}
	}
	t.Fatalf("no processor statefulset found owned by %s", gw.Name)
	return nil
}

// assertProcessorStatefulSetReplicas verifies that the processor StatefulSet
// has the expected replica count.
func assertProcessorStatefulSetReplicas(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, want int32) {
	t.Helper()
	ss := findOwnedProcessorStatefulSet(ctx, t, gw)
	if ss.Spec.Replicas == nil || *ss.Spec.Replicas != want {
		got := int32(0)
		if ss.Spec.Replicas != nil {
			got = *ss.Spec.Replicas
		}
		t.Errorf("processor replicas = %d, want %d", got, want)
	}
}

// assertResourceValue verifies that the given resource list contains the expected
// value for the named resource.
func assertResourceValue(t *testing.T, label string, list corev1.ResourceList, name corev1.ResourceName, want string) {
	t.Helper()
	got, ok := list[name]
	if !ok {
		t.Errorf("%s: resource %s not set", label, name)
		return
	}
	expected := resource.MustParse(want)
	if got.Cmp(expected) != 0 {
		t.Errorf("%s = %s, want %s", label, got.String(), expected.String())
	}
}

// assertWarningEvent drains the fake recorder channel and asserts that at least one
// Warning event contains the expected reason substring.
func assertWarningEvent(t *testing.T, recorder *record.FakeRecorder, reason string) {
	t.Helper()
	for {
		select {
		case got := <-recorder.Events:
			if strings.Contains(got, corev1.EventTypeWarning) && strings.Contains(got, reason) {
				return
			}
			// Drain other events that don't match (e.g. from previous subtests).
		default:
			t.Errorf("expected Warning event with reason %q but none found", reason)
			return
		}
	}
}

func TestValidateSpecRejectsSQLDefaultQueuesWithoutGatewayURL(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.AsyncConfig.InferenceGateway = nil
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
	}

	if err := validateSpec(g); err == nil || !strings.Contains(err.Error(), "inferenceGateway.url is required") {
		t.Fatalf("validateSpec() error = %v, want missing inference gateway URL error", err)
	}
}

func TestValidateSpecAllowsSQLQueueGatewayURLWithoutGlobalURL(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{
			{Name: asyncRequestQueueName("pool-a"), IGWBaseURL: testInferenceGatewayURL},
		},
	}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s
	g.Spec.Processor.AsyncConfig.InferenceGateway = nil
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
	}

	if err := validateSpec(g); err != nil {
		t.Fatalf("validateSpec() error = %v, want nil", err)
	}
}
