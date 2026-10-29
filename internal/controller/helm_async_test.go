package controller

import (
	"slices"
	"testing"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	tlspkg "github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/tls"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func minimalAsyncGateway() *batchv1alpha1.LLMBatchGateway {
	gw := minimalGateway()
	concurrency := int32(8)
	gw.Spec.Processor.AsyncConfig = &batchv1alpha1.AsyncProcessorSpec{
		Concurrency:  &concurrency,
		DrainTimeout: "2m",
		InferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
			URL: testInferenceGatewayURL,
		},
		Redis: &batchv1alpha1.AsyncRedisSpec{},
	}
	return gw
}

func TestSpecToAsyncHelmValues(t *testing.T) {
	gw := minimalAsyncGateway()
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())

	ap, ok := vals["ap"].(map[string]any)
	if !ok {
		t.Fatal("vals[\"ap\"] missing or wrong type")
	}

	t.Run("image", func(t *testing.T) {
		wantRepo, wantTag := splitImage(testImages().Async)
		img := ap["image"].(map[string]any)
		if got := img["repository"]; got != wantRepo {
			t.Errorf("image.repository = %v, want %s", got, wantRepo)
		}
		if got := img["tag"]; got != wantTag {
			t.Errorf("image.tag = %v, want %s", got, wantTag)
		}
	})

	t.Run("concurrency", func(t *testing.T) {
		if got := ap["concurrency"]; got != int64(8) {
			t.Errorf("concurrency = %v, want 8", got)
		}
	})

	t.Run("igwBaseURL", func(t *testing.T) {
		if got := ap["igwBaseURL"]; got != testInferenceGatewayURL {
			t.Errorf("igwBaseURL = %v, want %s", got, testInferenceGatewayURL)
		}
	})

	t.Run("drainTimeout", func(t *testing.T) {
		if got := ap["drainTimeout"]; got != "2m" {
			t.Errorf("drainTimeout = %v, want 2m", got)
		}
	})

	t.Run("redis", func(t *testing.T) {
		if got := ap["transport"]; got != asyncTransportRedis {
			t.Errorf("transport = %v, want redis-sortedset", got)
		}
		transportConfig, ok := ap["transportConfig"].(map[string]any)
		if !ok {
			t.Fatal("ap.transportConfig missing or wrong type")
		}
		urlSecret, ok := transportConfig["urlSecret"].(map[string]any)
		if !ok {
			t.Fatal("ap.transportConfig.urlSecret missing or wrong type")
		}
		if got := urlSecret["name"]; got != testSecretName(gw) {
			t.Errorf("transportConfig.urlSecret.name = %v, want %s", got, testSecretName(gw))
		}
		if got := urlSecret["key"]; got != "redis-url" {
			t.Errorf("transportConfig.urlSecret.key = %v, want redis-url", got)
		}
		queues, ok := transportConfig["queues"].([]any)
		if !ok || len(queues) != 1 {
			t.Fatalf("transportConfig.queues = %v, want one queue", transportConfig["queues"])
		}
		queue := queues[0].(map[string]any)
		// With no queuesConfig the operator derives the queue the processor
		// publishes to, instead of the dispatcher default request-sortedset.
		wantQueue := asyncRequestQueueName("pool-a")
		if got := queue["queue_name"]; got != wantQueue {
			t.Errorf("transportConfig.queues[0].queue_name = %v, want %s", got, wantQueue)
		}
		if got := queue["igw_base_url"]; got != testInferenceGatewayURL {
			t.Errorf("transportConfig.queues[0].igw_base_url = %v, want %s", got, testInferenceGatewayURL)
		}
		if _, ok := ap["redis"]; ok {
			t.Error("ap.redis must not be set when using the transport surface")
		}
		if _, ok := ap["messageQueueImpl"]; ok {
			t.Error("ap.messageQueueImpl must not be set when using the transport surface")
		}
	})

	t.Run("name override", func(t *testing.T) {
		if got := vals["nameOverride"]; got != "async-processor" {
			t.Errorf("nameOverride = %v, want async-processor", got)
		}
	})
}

func TestSpecToAsyncHelmValues_NilAsyncConfig(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.AsyncConfig = nil
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())

	if len(vals) != 0 {
		t.Errorf("expected empty map for nil AsyncConfig, got %v", vals)
	}
}

func TestSpecToAsyncHelmValues_TransportQueues(t *testing.T) {
	gw := minimalAsyncGateway()
	pollIntervalMs := int32(500)
	batchSize := int32(10)
	gw.Spec.Processor.AsyncConfig.Redis.PollIntervalMs = &pollIntervalMs
	gw.Spec.Processor.AsyncConfig.Redis.BatchSize = &batchSize
	gw.Spec.Processor.AsyncConfig.Redis.QueuesConfig = []batchv1alpha1.AsyncQueueConfig{
		{
			Name:           asyncRequestQueueName("pool-a"),
			WorkerPoolID:   "pool-a-workers",
			IGWBaseURL:     "http://epp-pool-a:8081",
			RequestPathURL: "/v1/completions",
			GateType:       "prometheus-budget",
			GateParams:     map[string]string{"pool": "pool-a"},
		},
		{
			Name:         asyncRequestQueueName("pool-b"),
			WorkerPoolID: "pool-b-workers",
		},
	}
	gw.Spec.OTEL = &batchv1alpha1.OTELSpec{
		Endpoint:     "http://jaeger:4317",
		RedisTracing: true,
	}

	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	transportConfig, ok := ap["transportConfig"].(map[string]any)
	if !ok {
		t.Fatal("ap.transportConfig missing or wrong type")
	}
	if got := transportConfig["poll_interval_ms"]; got != int64(500) {
		t.Errorf("transportConfig.poll_interval_ms = %v, want 500", got)
	}
	if got := transportConfig["batch_size"]; got != int64(10) {
		t.Errorf("transportConfig.batch_size = %v, want 10", got)
	}
	if got := transportConfig["enable_tracing"]; got != true {
		t.Errorf("transportConfig.enable_tracing = %v, want true", got)
	}
	queues, ok := transportConfig["queues"].([]any)
	if !ok || len(queues) != 2 {
		t.Fatalf("transportConfig.queues = %v, want two queues", transportConfig["queues"])
	}
	queue, ok := queues[0].(map[string]any)
	if !ok {
		t.Fatal("transportConfig.queues[0] is not a map")
	}
	if got := queue["queue_name"]; got != asyncRequestQueueName("pool-a") {
		t.Errorf("queue_name = %v, want %s", got, asyncRequestQueueName("pool-a"))
	}
	if got := queue["worker_pool_id"]; got != "pool-a-workers" {
		t.Errorf("worker_pool_id = %v, want pool-a-workers", got)
	}
	if got := queue["igw_base_url"]; got != "http://epp-pool-a:8081" {
		t.Errorf("igw_base_url = %v, want http://epp-pool-a:8081", got)
	}
	if got := queue["gate_type"]; got != "prometheus-budget" {
		t.Errorf("gate_type = %v, want prometheus-budget", got)
	}
	gateParams, ok := queue["gate_params"].(map[string]any)
	if !ok {
		t.Fatal("gate_params missing or wrong type")
	}
	if got := gateParams["pool"]; got != "pool-a" {
		t.Errorf("gate_params.pool = %v, want pool-a", got)
	}
	inheritedQueue, ok := queues[1].(map[string]any)
	if !ok {
		t.Fatal("transportConfig.queues[1] is not a map")
	}
	if got := inheritedQueue["igw_base_url"]; got != testInferenceGatewayURL {
		t.Errorf("transportConfig.queues[1].igw_base_url = %v, want %s", got, testInferenceGatewayURL)
	}
}

func TestSpecToAsyncHelmValues_SQLTransport(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a"},
	}
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{{Name: "request-sql"}},
	}

	vals := specToAsyncHelmValues(g, testSecretName(g), testImages())
	ap := vals["ap"].(map[string]any)
	if got := ap["transport"]; got != asyncTransportSQL {
		t.Fatalf("transport = %v, want sql", got)
	}
	transportConfig := ap["transportConfig"].(map[string]any)
	urlSecret := transportConfig["urlSecret"].(map[string]any)
	if got := urlSecret["key"]; got != "postgresql-url" {
		t.Errorf("urlSecret.key = %v, want postgresql-url", got)
	}
	if got := transportConfig["result_queue_name"]; got != asyncResultQueueName("pool-a") {
		t.Errorf("result_queue_name = %v, want %s", got, asyncResultQueueName("pool-a"))
	}
	if got := transportConfig["queues"].([]any)[0].(map[string]any)["queue_name"]; got != "request-sql" {
		t.Errorf("queue_name = %v, want request-sql", got)
	}
	if _, ok := ap["env"]; ok {
		t.Error("ap.env should not be set; the chart injects SQL_URL from transportConfig.urlSecret")
	}
}

func TestSpecToAsyncHelmValues_SQLTransportExplicitResultQueue(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {
			InferencePoolName: "pool-a",
			RequestQueueName:  "request-sql",
			ResultQueueName:   "result-sql",
		},
	}
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}

	vals := specToAsyncHelmValues(g, testSecretName(g), testImages())
	transportConfig := vals["ap"].(map[string]any)["transportConfig"].(map[string]any)
	if got := transportConfig["result_queue_name"]; got != "result-sql" {
		t.Errorf("result_queue_name = %v, want result-sql", got)
	}
	if got := transportConfig["queues"].([]any)[0].(map[string]any)["queue_name"]; got != "request-sql" {
		t.Errorf("queue_name = %v, want request-sql", got)
	}
}

func TestSpecToAsyncHelmValues_Monitoring(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.Monitoring = &batchv1alpha1.MonitoringSpec{Enabled: true}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())

	ap, ok := vals["ap"].(map[string]any)
	if !ok {
		t.Fatal("vals[\"ap\"] missing or wrong type")
	}
	pm, ok := ap["podMonitor"].(map[string]any)
	if !ok {
		t.Fatal("ap.podMonitor not set when monitoring is enabled")
	}
	if got := pm["enabled"]; got != true {
		t.Errorf("podMonitor.enabled = %v, want true", got)
	}
	labels, ok := pm["labels"].(map[string]any)
	if !ok {
		t.Fatal("podMonitor.labels missing")
	}
	if got := labels[odhMonitoringScrapeLabel]; got != odhMonitoringScrapeValue {
		t.Errorf("podMonitor.labels[%s] = %v, want %s", odhMonitoringScrapeLabel, got, odhMonitoringScrapeValue)
	}
}

func TestSpecToAsyncHelmValues_TLS(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.Processor.AsyncConfig.TLS = &batchv1alpha1.AsyncTLSSpec{
		SecretName:         "igw-tls",
		InsecureSkipVerify: true,
		CACertKey:          "ca.crt",
		CertKey:            "tls.crt",
		KeyKey:             "tls.key",
	}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	tls, ok := ap["tls"].(map[string]any)
	if !ok {
		t.Fatal("ap.tls not set")
	}
	if got := tls["secretName"]; got != "igw-tls" {
		t.Errorf("tls.secretName = %v, want igw-tls", got)
	}
	if got := tls["insecureSkipVerify"]; got != true {
		t.Errorf("tls.insecureSkipVerify = %v, want true", got)
	}
	if got := tls["keyKey"]; got != "tls.key" {
		t.Errorf("tls.keyKey = %v, want tls.key", got)
	}
}

func TestSpecToAsyncHelmValues_TransformConfig(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.Processor.AsyncConfig.TransformConfig = &batchv1alpha1.AsyncTransformConfig{
		RequestTransforms: []batchv1alpha1.AsyncRequestTransform{
			{Name: "gcs-whisper", Type: "gcs_uri_multipart", Parameters: map[string]string{"providers": "whisper"}},
		},
	}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	tc, ok := ap["transformConfig"].(map[string]any)
	if !ok {
		t.Fatal("ap.transformConfig not set")
	}
	transforms, ok := tc["requestTransforms"].([]any)
	if !ok || len(transforms) != 1 {
		t.Fatalf("requestTransforms length = %d, want 1", len(transforms))
	}
	tm := transforms[0].(map[string]any)
	if got := tm["name"]; got != "gcs-whisper" {
		t.Errorf("transform.name = %v, want gcs-whisper", got)
	}
	if got := tm["type"]; got != "gcs_uri_multipart" {
		t.Errorf("transform.type = %v, want gcs_uri_multipart", got)
	}
}

func TestSpecToAsyncHelmValues_ModelServerMonitor(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.Processor.AsyncConfig.ModelServerMonitor = &batchv1alpha1.AsyncModelServerMonitorSpec{
		Enabled:  true,
		Selector: map[string]string{"llm-d.ai/role": "decode"},
		Port:     "modelserver",
		Path:     "/metrics",
		Interval: "15s",
	}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	msm, ok := ap["modelServerMonitor"].(map[string]any)
	if !ok {
		t.Fatal("ap.modelServerMonitor not set")
	}
	if got := msm["enabled"]; got != true {
		t.Errorf("modelServerMonitor.enabled = %v, want true", got)
	}
	if got := msm["port"]; got != "modelserver" {
		t.Errorf("modelServerMonitor.port = %v, want modelserver", got)
	}
	sel, ok := msm["selector"].(map[string]any)
	if !ok {
		t.Fatal("modelServerMonitor.selector missing")
	}
	if got := sel["llm-d.ai/role"]; got != "decode" {
		t.Errorf("selector[llm-d.ai/role] = %v, want decode", got)
	}
}

func TestSpecToAsyncHelmValues_OTEL(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.OTEL = &batchv1alpha1.OTELSpec{
		Endpoint:     "http://jaeger:4317",
		Insecure:     true,
		Sampler:      "parentbased_traceidratio",
		SamplerArg:   "0.1",
		RedisTracing: false,
	}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	otel, ok := ap["otel"].(map[string]any)
	if !ok {
		t.Fatal("ap.otel not set")
	}
	if got := otel["endpoint"]; got != "http://jaeger:4317" {
		t.Errorf("otel.endpoint = %v, want http://jaeger:4317", got)
	}
	if got := otel["insecure"]; got != true {
		t.Errorf("otel.insecure = %v, want true", got)
	}
	if got := otel["redisTracing"]; got != false {
		t.Errorf("otel.redisTracing = %v, want false", got)
	}
}

func TestSpecToAsyncHelmValues_Grafana(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	grafana, ok := ap["grafana"].(map[string]any)
	if !ok {
		t.Fatal("ap.grafana not set")
	}
	dashboards, ok := grafana["dashboards"].(map[string]any)
	if !ok {
		t.Fatal("ap.grafana.dashboards missing")
	}
	if got := dashboards["enabled"]; got != true {
		t.Errorf("grafana.dashboards.enabled = %v, want true", got)
	}
}

func TestSpecToAsyncHelmValues_PrometheusRule(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.PrometheusRule = &batchv1alpha1.PrometheusRuleSpec{
		Enabled: true,
		Labels:  map[string]string{"team": "ml"},
	}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	pr, ok := ap["prometheusRule"].(map[string]any)
	if !ok {
		t.Fatal("ap.prometheusRule not set")
	}
	if got := pr["enabled"]; got != true {
		t.Errorf("prometheusRule.enabled = %v, want true", got)
	}
	labels, ok := pr["labels"].(map[string]any)
	if !ok {
		t.Fatal("prometheusRule.labels missing")
	}
	if got := labels["team"]; got != "ml" {
		t.Errorf("prometheusRule.labels[team] = %v, want ml", got)
	}
}

func TestSpecToAsyncHelmValues_ImagePullSecrets(t *testing.T) {
	gw := minimalAsyncGateway()
	gw.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
		{Name: "registry-creds"},
		{Name: "ghcr-token"},
	}
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	secrets, ok := ap["imagePullSecrets"].([]any)
	if !ok {
		t.Fatal("ap.imagePullSecrets not set")
	}
	if len(secrets) != 2 {
		t.Fatalf("ap.imagePullSecrets length = %d, want 2", len(secrets))
	}
	if got := secrets[0].(map[string]any)["name"]; got != "registry-creds" {
		t.Errorf("ap.imagePullSecrets[0].name = %v, want registry-creds", got)
	}
	if got := secrets[1].(map[string]any)["name"]; got != "ghcr-token" {
		t.Errorf("ap.imagePullSecrets[1].name = %v, want ghcr-token", got)
	}
}

func TestSpecToAsyncHelmValues_NoImagePullSecrets(t *testing.T) {
	gw := minimalAsyncGateway()
	vals := specToAsyncHelmValues(gw, testSecretName(gw), testImages())
	ap := vals["ap"].(map[string]any)
	if _, ok := ap["imagePullSecrets"]; ok {
		t.Error("ap.imagePullSecrets should not be set when CRD has none")
	}
}

func TestRenderAsyncChart(t *testing.T) {
	renderer, err := NewHelmRenderer("../../llm-d-async/charts/llm-d-async", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	gw := minimalAsyncGateway()
	objects, err := renderer.RenderAsyncChart(gw, testSecretName(gw))
	if err != nil {
		t.Fatalf("RenderAsyncChart() error: %v", err)
	}

	if len(objects) == 0 {
		t.Fatal("RenderAsyncChart() returned no objects")
	}

	t.Run("component label injected", func(t *testing.T) {
		for _, obj := range objects {
			labels := obj.GetLabels()
			if got := labels[labelKeyComponent]; got != componentAsyncProcessor {
				t.Errorf("%s %s: %s = %q, want %q",
					obj.GetKind(), obj.GetName(), labelKeyComponent, got, componentAsyncProcessor)
			}
		}
	})

	t.Run("has deployment", func(t *testing.T) {
		found := false
		deploymentName := gw.Name + "-async-processor"
		for _, obj := range objects {
			if obj.GetKind() == "Deployment" {
				found = true
				if obj.GetName() != deploymentName {
					t.Errorf("Deployment name = %q, want %q", obj.GetName(), deploymentName)
				}
				selector, found, err := unstructured.NestedString(obj.Object, "spec", "selector", "matchLabels", "app.kubernetes.io/name")
				if err != nil || !found {
					t.Fatalf("Deployment selector app.kubernetes.io/name not found: %v", err)
				}
				if selector != "async-processor" {
					t.Errorf("Deployment selector app.kubernetes.io/name = %q, want async-processor", selector)
				}
				break
			}
		}
		if !found {
			t.Error("no Deployment found in rendered objects")
		}
	})
}

func TestRenderAsyncChart_SQLTransport(t *testing.T) {
	renderer, err := NewHelmRenderer("../../llm-d-async/charts/llm-d-async", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	g := minimalAsyncGateway()
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{
		QueuesConfig: []batchv1alpha1.AsyncQueueConfig{{Name: "request-sql"}},
	}
	objects, err := renderer.RenderAsyncChart(g, testSecretName(g))
	if err != nil {
		t.Fatalf("RenderAsyncChart() SQL error: %v", err)
	}

	for _, obj := range objects {
		if obj.GetKind() != "Deployment" {
			continue
		}
		containers, found, err := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
		if err != nil || !found || len(containers) == 0 {
			t.Fatalf("SQL deployment containers not found: %v", err)
		}
		container := containers[0].(map[string]any)
		args, _, _ := unstructured.NestedStringSlice(container, "args")
		if !slices.Contains(args, "--transport=sql") {
			t.Fatalf("SQL deployment args = %v, want --transport=sql", args)
		}
		env, _, _ := unstructured.NestedSlice(container, "env")
		if !hasEnvSecret(env, "SQL_URL", testSecretName(g), "postgresql-url") {
			t.Fatalf("SQL_URL env is not wired to the PostgreSQL Secret: %v", env)
		}
		return
	}
	t.Fatal("SQL deployment not found")
}

func hasEnvSecret(env []any, name, secretName, secretKey string) bool {
	for _, raw := range env {
		entry := raw.(map[string]any)
		if entry["name"] != name {
			continue
		}
		valueFrom, ok := entry["valueFrom"].(map[string]any)
		if !ok {
			return false
		}
		secretRef, ok := valueFrom["secretKeyRef"].(map[string]any)
		return ok && secretRef["name"] == secretName && secretRef["key"] == secretKey
	}
	return false
}

func TestSpecToAsyncHelmValues_SQLDefaultQueuesDeduplicated(t *testing.T) {
	g := minimalAsyncGateway()
	g.Spec.Processor.ModelGateways = map[string]batchv1alpha1.ModelGatewaySpec{
		"model-a": {InferencePoolName: "pool-a", RequestQueueName: "request-shared", ResultQueueName: "result-a"},
		"model-b": {InferencePoolName: "pool-b", RequestQueueName: "request-shared", ResultQueueName: "result-b"},
	}
	g.Spec.Processor.AsyncConfig.Redis = nil
	g.Spec.Processor.AsyncConfig.SQL = &batchv1alpha1.AsyncSQLSpec{}
	g.Spec.Processor.AsyncConfig.ResultPollTimeout = testDuration30s

	if err := validateSpec(g); err != nil {
		t.Fatalf("validateSpec() error = %v, want nil", err)
	}
	vals := specToAsyncHelmValues(g, testSecretName(g), testImages())
	ap := vals["ap"].(map[string]any)
	queues := ap["transportConfig"].(map[string]any)["queues"].([]any)
	if len(queues) != 1 {
		t.Fatalf("len(queues) = %d, want 1 (deduplicated)", len(queues))
	}
}
