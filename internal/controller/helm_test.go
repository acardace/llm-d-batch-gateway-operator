package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	tlspkg "github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/tls"
)

const classifyEndpoint = "/v1/classify"

// testSecretName returns the secret name for use in tests, simulating what
// resolveSecret returns in the same-namespace case.
func testSecretName(gw *batchv1alpha1.LLMBatchGateway) string {
	return gw.Spec.SecretRef.Name
}

// testImages returns the component images used in tests, simulating what the
// operator reads from its environment (params.env) in production.
func testImages() ComponentImages {
	return ComponentImages{
		APIServer: "ghcr.io/llm-d/batch-gateway-apiserver:latest",
		Processor: "ghcr.io/llm-d/batch-gateway-processor:latest",
		GC:        "ghcr.io/llm-d/batch-gateway-gc:latest",
		Async:     "ghcr.io/llm-d-incubation/llm-d-async:v0.7.2", // TODO: sync with Makefile tag
	}
}

func TestSplitImage(t *testing.T) {
	tests := []struct {
		name     string
		image    string
		wantRepo string
		wantTag  string
	}{
		{
			name:     "standard image with tag",
			image:    "ghcr.io/llm-d/batch-gateway-apiserver:v0.1.0",
			wantRepo: "ghcr.io/llm-d/batch-gateway-apiserver",
			wantTag:  "v0.1.0",
		},
		{
			name:     "image with latest tag",
			image:    "ghcr.io/llm-d/batch-gateway-apiserver:latest",
			wantRepo: "ghcr.io/llm-d/batch-gateway-apiserver",
			wantTag:  "latest",
		},
		{
			name:     "image without tag",
			image:    "ghcr.io/llm-d/batch-gateway-apiserver",
			wantRepo: "ghcr.io/llm-d/batch-gateway-apiserver",
			wantTag:  "latest",
		},
		{
			name:     "image with registry port and tag",
			image:    "registry.example.com:5000/batch-gateway-apiserver:v1.0",
			wantRepo: "registry.example.com:5000/batch-gateway-apiserver",
			wantTag:  "v1.0",
		},
		{
			name:     "image with registry port no tag",
			image:    "registry.example.com:5000/batch-gateway-apiserver",
			wantRepo: "registry.example.com:5000/batch-gateway-apiserver",
			wantTag:  "latest",
		},
		{
			name:     "image with sha tag prefix",
			image:    "ghcr.io/llm-d/batch-gateway-apiserver:sha-abc123",
			wantRepo: "ghcr.io/llm-d/batch-gateway-apiserver",
			wantTag:  "sha-abc123",
		},
		{
			name:     "digest splits so chart reconstructs repo@sha256:hex correctly",
			image:    "ghcr.io/llm-d/batch-gateway-apiserver@sha256:abc123def456",
			wantRepo: "ghcr.io/llm-d/batch-gateway-apiserver@sha256",
			wantTag:  "abc123def456",
		},
		{
			name:     "digest with registry port",
			image:    "registry.example.com:5000/batch-gateway-apiserver@sha256:abc123def456",
			wantRepo: "registry.example.com:5000/batch-gateway-apiserver@sha256",
			wantTag:  "abc123def456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, tag := splitImage(tt.image)
			if repo != tt.wantRepo {
				t.Errorf("repo = %q, want %q", repo, tt.wantRepo)
			}
			if tag != tt.wantTag {
				t.Errorf("tag = %q, want %q", tag, tt.wantTag)
			}
		})
	}
}

func TestSpecToHelmValues(t *testing.T) {
	replicas := int32(2)
	gw := &batchv1alpha1.LLMBatchGateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gw",
			Namespace: "test-ns",
		},
		Spec: batchv1alpha1.LLMBatchGatewaySpec{
			SecretRef: corev1.SecretReference{
				Name: "my-secret",
			},
			DBBackend:      dbBackendPostgreSQL,
			ExtraEndpoints: []string{classifyEndpoint},
			FileStorage: &batchv1alpha1.FileStorageSpec{
				S3: &batchv1alpha1.S3StorageSpec{
					Region:   "us-east-1",
					Bucket:   "llm-d-batch-gateway",
					Endpoint: "https://s3.example.com",
				},
			},
			APIServer: batchv1alpha1.APIServerSpec{
				Replicas: &replicas,
				Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1000m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
			},
			Processor: batchv1alpha1.ProcessorSpec{
				Replicas: &replicas,
				GlobalInferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
					URL:            "http://inference-gateway:8000",
					RequestTimeout: "5m",
				},
			},
			GC: batchv1alpha1.GCSpec{
				Interval: "30m",
			},
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), ComponentImages{
		APIServer: "ghcr.io/llm-d/batch-gateway-apiserver:v0.1.0",
		Processor: "ghcr.io/llm-d/batch-gateway-processor:v0.1.0",
		GC:        "ghcr.io/llm-d/batch-gateway-gc:v0.1.0",
	}, tlspkg.ProfileValues{})

	t.Run("global secret name", func(t *testing.T) {
		global, ok := vals["global"].(map[string]interface{})
		if !ok {
			t.Fatal("global not found")
		}
		if got := global["secretName"]; got != "my-secret" {
			t.Errorf("secretName = %v, want %q", got, "my-secret")
		}
	})

	t.Run("global db client type", func(t *testing.T) {
		global := vals["global"].(map[string]interface{})
		dbClient := global["dbClient"].(map[string]interface{})
		if got := dbClient["type"]; got != dbBackendPostgreSQL {
			t.Errorf("dbClient.type = %v, want %q", got, dbBackendPostgreSQL)
		}
	})

	t.Run("global db client redis TLS not set", func(t *testing.T) {
		global := vals["global"].(map[string]interface{})
		dbClient := global["dbClient"].(map[string]interface{})
		if _, ok := dbClient["redis"]; ok {
			t.Error("dbClient.redis should not be present when RedisClient is nil")
		}
	})

	t.Run("global extra endpoints", func(t *testing.T) {
		global := vals["global"].(map[string]interface{})
		endpoints := global["extraEndpoints"].([]interface{})
		if len(endpoints) != 1 || endpoints[0] != classifyEndpoint {
			t.Errorf("global.extraEndpoints = %v, want [/v1/classify]", endpoints)
		}
	})

	t.Run("file storage s3", func(t *testing.T) {
		global := vals["global"].(map[string]interface{})
		fc := global["fileClient"].(map[string]interface{})
		if got := fc["type"]; got != "s3" {
			t.Errorf("fileClient.type = %v, want %q", got, "s3")
		}
		s3 := fc["s3"].(map[string]interface{})
		if got := s3["region"]; got != "us-east-1" {
			t.Errorf("s3.region = %v, want %q", got, "us-east-1")
		}
	})

	t.Run("apiserver image split", func(t *testing.T) {
		apiserver := vals["apiserver"].(map[string]interface{})
		img := apiserver["image"].(map[string]interface{})
		if got := img["repository"]; got != "ghcr.io/llm-d/batch-gateway-apiserver" {
			t.Errorf("image.repository = %v", got)
		}
		if got := img["tag"]; got != "v0.1.0" {
			t.Errorf("image.tag = %v", got)
		}
	})

	t.Run("apiserver replicas", func(t *testing.T) {
		apiserver := vals["apiserver"].(map[string]interface{})
		if got := apiserver["replicaCount"]; got != int64(2) {
			t.Errorf("replicaCount = %v, want 2", got)
		}
	})

	t.Run("apiserver enabled", func(t *testing.T) {
		apiserver := vals["apiserver"].(map[string]interface{})
		if got := apiserver["enabled"]; got != true {
			t.Errorf("enabled = %v, want true", got)
		}
	})

	t.Run("processor inference gateway", func(t *testing.T) {
		processor := vals["processor"].(map[string]interface{})
		config := processor["config"].(map[string]interface{})
		gig := config["globalInferenceGateway"].(map[string]interface{})
		if got := gig["url"]; got != "http://inference-gateway:8000" {
			t.Errorf("url = %v", got)
		}
		if got := gig["requestTimeout"]; got != "5m" {
			t.Errorf("requestTimeout = %v", got)
		}
	})

	t.Run("gc config", func(t *testing.T) {
		gc := vals["gc"].(map[string]interface{})
		config := gc["config"].(map[string]interface{})
		collector := config["collector"].(map[string]interface{})
		if got := collector["interval"]; got != "30m" {
			t.Errorf("interval = %v, want %q", got, "30m")
		}
	})
}

func TestSpecToHelmValues_Monitoring(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Monitoring = &batchv1alpha1.MonitoringSpec{Enabled: true}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	t.Run("service monitor enabled", func(t *testing.T) {
		apiserver := vals["apiserver"].(map[string]interface{})
		sm := apiserver["serviceMonitor"].(map[string]interface{})
		if got := sm["enabled"]; got != true {
			t.Errorf("serviceMonitor.enabled = %v, want true", got)
		}
		labels := sm["labels"].(map[string]interface{})
		if got := labels[odhMonitoringScrapeLabel]; got != odhMonitoringScrapeValue {
			t.Errorf("serviceMonitor scrape label = %v, want \"true\"", got)
		}
	})

	t.Run("processor pod monitor enabled with scrape label", func(t *testing.T) {
		processor := vals["processor"].(map[string]interface{})
		pm := processor["podMonitor"].(map[string]interface{})
		if got := pm["enabled"]; got != true {
			t.Errorf("podMonitor.enabled = %v, want true", got)
		}
		labels := pm["labels"].(map[string]interface{})
		if got := labels[odhMonitoringScrapeLabel]; got != odhMonitoringScrapeValue {
			t.Errorf("podMonitor scrape label = %v, want \"true\"", got)
		}
	})

	t.Run("gc pod monitor enabled with scrape label", func(t *testing.T) {
		gc := vals["gc"].(map[string]interface{})
		pm := gc["podMonitor"].(map[string]interface{})
		if got := pm["enabled"]; got != true {
			t.Errorf("podMonitor.enabled = %v, want true", got)
		}
		labels := pm["labels"].(map[string]interface{})
		if got := labels[odhMonitoringScrapeLabel]; got != odhMonitoringScrapeValue {
			t.Errorf("podMonitor scrape label = %v, want \"true\"", got)
		}
	})
}

func TestSpecToHelmValues_TLS(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.TLS = &batchv1alpha1.TLSSpec{
		Enabled: true,
		CertManager: &batchv1alpha1.CertManagerSpec{
			IssuerName: "letsencrypt",
			IssuerKind: "ClusterIssuer",
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	tls := apiserver["tls"].(map[string]interface{})

	t.Run("tls enabled", func(t *testing.T) {
		if got := tls["enabled"]; got != true {
			t.Errorf("tls.enabled = %v, want true", got)
		}
	})

	t.Run("cert manager", func(t *testing.T) {
		cm := tls["certManager"].(map[string]interface{})
		if got := cm["enabled"]; got != true {
			t.Errorf("certManager.enabled = %v, want true", got)
		}
		if got := cm["issuerName"]; got != "letsencrypt" {
			t.Errorf("issuerName = %v, want %q", got, "letsencrypt")
		}
	})
}

func TestSpecToHelmValues_HTTPRoute(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.HTTPRoute = &batchv1alpha1.HTTPRouteSpec{
		Enabled: true,
		ParentRefs: []batchv1alpha1.ParentReference{
			{Name: "my-gateway", Namespace: "gateway-ns"},
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	hr := apiserver["httpRoute"].(map[string]interface{})

	t.Run("http route enabled", func(t *testing.T) {
		if got := hr["enabled"]; got != true {
			t.Errorf("httpRoute.enabled = %v, want true", got)
		}
	})

	t.Run("parent refs", func(t *testing.T) {
		refs := hr["parentRefs"].([]interface{})
		if len(refs) != 1 {
			t.Fatalf("parentRefs length = %d, want 1", len(refs))
		}
		ref := refs[0].(map[string]interface{})
		if got := ref["name"]; got != "my-gateway" {
			t.Errorf("parentRef.name = %v, want %q", got, "my-gateway")
		}
	})
}

func TestNewHelmRenderer(t *testing.T) {
	t.Run("valid chart path", func(t *testing.T) {
		renderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
		if err != nil {
			t.Fatalf("NewHelmRenderer() error: %v", err)
		}
		if renderer == nil || renderer.chart == nil {
			t.Fatal("renderer or chart is nil")
		}
	})

	t.Run("invalid chart path", func(t *testing.T) {
		_, err := NewHelmRenderer("/nonexistent/path", testImages(), tlspkg.ProfileValues{})
		if err == nil {
			t.Fatal("expected error for invalid chart path")
		}
	})
}

func TestRenderBatchChart(t *testing.T) {
	renderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	gw := minimalGateway()
	objects, err := renderer.RenderBatchChart(gw, testSecretName(gw))
	if err != nil {
		t.Fatalf("RenderBatchChart() error: %v", err)
	}

	kinds := map[string]int{}
	for _, obj := range objects {
		kinds[obj.GetKind()]++
	}

	t.Run("renders deployments", func(t *testing.T) {
		if got := kinds["Deployment"]; got != 2 {
			t.Errorf("Deployment count = %d, want 2", got)
		}
	})

	t.Run("renders processor statefulset", func(t *testing.T) {
		if got := kinds["StatefulSet"]; got != 1 {
			t.Errorf("StatefulSet count = %d, want 1", got)
		}
	})

	t.Run("renders configmaps", func(t *testing.T) {
		if got := kinds["ConfigMap"]; got < 3 {
			t.Errorf("ConfigMap count = %d, want >= 3", got)
		}
	})

	t.Run("renders service accounts", func(t *testing.T) {
		if got := kinds["ServiceAccount"]; got != 3 {
			t.Errorf("ServiceAccount count = %d, want 3", got)
		}
	})

	t.Run("renders service", func(t *testing.T) {
		if got := kinds["Service"]; got != 2 {
			t.Errorf("Service count = %d, want 2", got)
		}
	})

	t.Run("no optional resources by default", func(t *testing.T) {
		for _, kind := range []string{"Certificate", "HTTPRoute", "ServiceMonitor", "PodMonitor", "PrometheusRule"} {
			if got := kinds[kind]; got != 0 {
				t.Errorf("%s count = %d, want 0", kind, got)
			}
		}
	})
}

func TestRenderBatchChart_WithMonitoring(t *testing.T) {
	renderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	gw := minimalGateway()
	gw.Spec.Monitoring = &batchv1alpha1.MonitoringSpec{Enabled: true}

	objects, err := renderer.RenderBatchChart(gw, testSecretName(gw))
	if err != nil {
		t.Fatalf("RenderBatchChart() error: %v", err)
	}

	kinds := map[string]int{}
	for _, obj := range objects {
		kinds[obj.GetKind()]++
	}

	t.Run("renders service monitor", func(t *testing.T) {
		if got := kinds["ServiceMonitor"]; got != 1 {
			t.Errorf("ServiceMonitor count = %d, want 1", got)
		}
	})

	t.Run("renders pod monitors", func(t *testing.T) {
		if got := kinds["PodMonitor"]; got != 2 {
			t.Errorf("PodMonitor count = %d, want 2 (processor + gc)", got)
		}
	})
}

func TestSpecToHelmValues_Logging(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.APIServer.Config = &batchv1alpha1.APIServerConfigSpec{
		Logging: &batchv1alpha1.LoggingConfig{Verbosity: 5},
	}
	gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		Logging: &batchv1alpha1.LoggingConfig{Verbosity: 3},
	}
	gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{
		Logging: &batchv1alpha1.LoggingConfig{Verbosity: 4},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	t.Run("apiserver logging verbosity", func(t *testing.T) {
		apiserver := vals["apiserver"].(map[string]interface{})
		logging := apiserver["logging"].(map[string]interface{})
		if got := logging["verbosity"]; got != int64(5) {
			t.Errorf("apiserver logging.verbosity = %v, want 5", got)
		}
	})

	t.Run("processor logging verbosity", func(t *testing.T) {
		processor := vals["processor"].(map[string]interface{})
		logging := processor["logging"].(map[string]interface{})
		if got := logging["verbosity"]; got != int64(3) {
			t.Errorf("processor logging.verbosity = %v, want 3", got)
		}
	})

	t.Run("gc logging verbosity", func(t *testing.T) {
		gc := vals["gc"].(map[string]interface{})
		logging := gc["logging"].(map[string]interface{})
		if got := logging["verbosity"]; got != int64(4) {
			t.Errorf("gc logging.verbosity = %v, want 4", got)
		}
	})
}

func TestSpecToHelmValues_FSStorage(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.FileStorage = &batchv1alpha1.FileStorageSpec{
		FS: &batchv1alpha1.FSStorageSpec{
			BasePath:  "/data",
			ClaimName: "my-pvc",
		},
		Retry: &batchv1alpha1.FileRetrySpec{
			MaxRetries:     ptr.To(int32(5)),
			InitialBackoff: "500ms",
			MaxBackoff:     "30s",
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	global := vals["global"].(map[string]interface{})
	fc := global["fileClient"].(map[string]interface{})

	if got := fc["type"]; got != "fs" {
		t.Errorf("fileClient.type = %v, want fs", got)
	}
	fs := fc["fs"].(map[string]interface{})
	if got := fs["basePath"]; got != "/data" {
		t.Errorf("fs.basePath = %v, want /data", got)
	}
	if got := fs["pvcName"]; got != "my-pvc" {
		t.Errorf("fs.pvcName = %v, want my-pvc", got)
	}

	retry := fc["retry"].(map[string]interface{})
	if got := retry["maxRetries"]; got != int64(5) {
		t.Errorf("retry.maxRetries = %v, want 5", got)
	}
	if got := retry["initialBackoff"]; got != "500ms" {
		t.Errorf("retry.initialBackoff = %v, want 500ms", got)
	}
}

func TestSpecToHelmValues_OTEL(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.OTEL = &batchv1alpha1.OTELSpec{
		Endpoint:          "http://otel-collector:4317",
		Insecure:          true,
		Sampler:           "parentbased_traceidratio",
		SamplerArg:        "0.1",
		RedisTracing:      true,
		PostgresqlTracing: true,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	global := vals["global"].(map[string]interface{})
	otel := global["otel"].(map[string]interface{})

	if got := otel["endpoint"]; got != "http://otel-collector:4317" {
		t.Errorf("otel.endpoint = %v", got)
	}
	if got := otel["insecure"]; got != true {
		t.Errorf("otel.insecure = %v, want true", got)
	}
	if got := otel["sampler"]; got != "parentbased_traceidratio" {
		t.Errorf("otel.sampler = %v", got)
	}
	if _, ok := otel["redisTracing"]; ok {
		t.Error("otel.redisTracing must not be rendered by the current chart")
	}
	if got := otel["postgresqlTracing"]; got != true {
		t.Errorf("otel.postgresqlTracing = %v, want true", got)
	}
}

func TestSpecToHelmValues_TLSSecretName(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.TLS = &batchv1alpha1.TLSSpec{
		Enabled:    true,
		SecretName: "my-tls-secret",
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	tls := apiserver["tls"].(map[string]interface{})

	if got := tls["secretName"]; got != "my-tls-secret" {
		t.Errorf("tls.secretName = %v, want my-tls-secret", got)
	}
}

func TestSpecToHelmValues_TLSDNSNames(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.TLS = &batchv1alpha1.TLSSpec{
		Enabled: true,
		CertManager: &batchv1alpha1.CertManagerSpec{
			IssuerName: "letsencrypt",
			DNSNames:   []string{"api.example.com", "api2.example.com"},
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	tls := apiserver["tls"].(map[string]interface{})
	cm := tls["certManager"].(map[string]interface{})

	dnsNames, ok := cm["dnsNames"].([]interface{})
	if !ok {
		t.Fatalf("dnsNames not a []interface{}: %T", cm["dnsNames"])
	}
	if len(dnsNames) != 2 {
		t.Fatalf("dnsNames length = %d, want 2", len(dnsNames))
	}
	if got := dnsNames[0]; got != "api.example.com" {
		t.Errorf("dnsNames[0] = %v, want api.example.com", got)
	}
}

func TestSpecToHelmValues_HTTPRouteAnnotations(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.HTTPRoute = &batchv1alpha1.HTTPRouteSpec{
		Enabled: true,
		Annotations: map[string]string{
			"custom.io/ann": "value",
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	hr := apiserver["httpRoute"].(map[string]interface{})
	anns, ok := hr["annotations"].(map[string]interface{})
	if !ok {
		t.Fatalf("annotations not a map: %T", hr["annotations"])
	}
	if got := anns["custom.io/ann"]; got != "value" {
		t.Errorf("annotations[custom.io/ann] = %v, want value", got)
	}
}

func TestSpecToHelmValues_ModelGateways(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.GlobalInferenceGateway = nil
	gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
		"model-a": {URL: "http://model-a:8000", RequestTimeout: "2m", InferenceObjective: "latency"},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	mg := config["modelGateways"].(map[string]interface{})
	ma := mg["model-a"].(map[string]interface{})
	if got := ma["url"]; got != "http://model-a:8000" {
		t.Errorf("modelGateways.model-a.url = %v", got)
	}
	if got := ma["requestTimeout"]; got != "2m" {
		t.Errorf("modelGateways.model-a.requestTimeout = %v", got)
	}
	if got := ma["inferenceObjective"]; got != "latency" {
		t.Errorf("modelGateways.model-a.inferenceObjective = %v, want latency", got)
	}
}

func TestSpecToHelmValues_SyncGatewaysOmitQueueNames(t *testing.T) {
	// validateSpec rejects these fields outside async mode; this covers the pure mapper.
	t.Run("globalInferenceGateway", func(t *testing.T) {
		gw := minimalGateway()
		gw.Spec.Processor.GlobalInferenceGateway.RequestQueueName = "requests"
		gw.Spec.Processor.GlobalInferenceGateway.ResultQueueName = "results"

		vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

		processor := vals["processor"].(map[string]interface{})
		config := processor["config"].(map[string]interface{})
		global := config["globalInferenceGateway"].(map[string]interface{})
		if _, ok := global["requestQueueName"]; ok {
			t.Error("globalInferenceGateway.requestQueueName should not be rendered")
		}
		if _, ok := global["resultQueueName"]; ok {
			t.Error("globalInferenceGateway.resultQueueName should not be rendered")
		}
	})

	t.Run("modelGateways", func(t *testing.T) {
		gw := minimalGateway()
		gw.Spec.Processor.GlobalInferenceGateway = nil
		gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
			"model-a": {
				URL:              "http://model-a:8000",
				RequestQueueName: "requests",
				ResultQueueName:  "results",
			},
		}

		vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

		processor := vals["processor"].(map[string]interface{})
		config := processor["config"].(map[string]interface{})
		models := config["modelGateways"].(map[string]interface{})
		model := models["model-a"].(map[string]interface{})
		if _, ok := model["requestQueueName"]; ok {
			t.Error("modelGateways.model-a.requestQueueName should not be rendered")
		}
		if _, ok := model["resultQueueName"]; ok {
			t.Error("modelGateways.model-a.resultQueueName should not be rendered")
		}
	})
}

func TestSpecToHelmValues_PerGatewayInferenceObjective(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.GlobalInferenceGateway = &batchv1alpha1.InferenceGatewaySpec{
		URL:                "http://global:8000",
		InferenceObjective: "throughput",
	}
	gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
		"model-a": {URL: "http://model-a:8000", InferenceObjective: "latency"},
		"model-b": {URL: "http://model-b:8000"},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})

	gwConfig := config["globalInferenceGateway"].(map[string]interface{})
	if got := gwConfig["inferenceObjective"]; got != "throughput" {
		t.Errorf("globalInferenceGateway.inferenceObjective = %v, want throughput", got)
	}

	mg := config["modelGateways"].(map[string]interface{})
	ma := mg["model-a"].(map[string]interface{})
	if got := ma["inferenceObjective"]; got != "latency" {
		t.Errorf("model-a.inferenceObjective = %v, want latency", got)
	}

	mb := mg["model-b"].(map[string]interface{})
	if _, exists := mb["inferenceObjective"]; exists {
		t.Errorf("model-b should not have inferenceObjective, got %v", mb["inferenceObjective"])
	}
}

func TestSpecToHelmValues_LegacyInferenceObjective(t *testing.T) {
	tests := []struct {
		name       string
		global     *batchv1alpha1.InferenceGatewaySpec
		models     map[string]batchv1alpha1.InferenceGatewaySpec
		wantGlobal string
		wantModels map[string]string
	}{
		{
			name:       "fills global gateway",
			global:     &batchv1alpha1.InferenceGatewaySpec{URL: "http://global:8000"},
			wantGlobal: "batch-sheddable",
		},
		{
			name:       "global gateway objective wins",
			global:     &batchv1alpha1.InferenceGatewaySpec{URL: "http://global:8000", InferenceObjective: "own"},
			wantGlobal: "own",
		},
		{
			name: "fills model gateways that have none",
			models: map[string]batchv1alpha1.InferenceGatewaySpec{
				"model-a": {URL: "http://model-a:8000"},
				"model-b": {URL: "http://model-b:8000", InferenceObjective: "own"},
			},
			wantModels: map[string]string{"model-a": "batch-sheddable", "model-b": "own"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := minimalGateway()
			gw.Spec.Processor.GlobalInferenceGateway = tt.global
			gw.Spec.Processor.ModelGateways = tt.models
			gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{InferenceObjective: "batch-sheddable"}

			vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})
			config := vals["processor"].(map[string]interface{})["config"].(map[string]interface{})

			if _, exists := config["inferenceObjective"]; exists {
				t.Errorf("processor.config.inferenceObjective should not be rendered, got %v", config["inferenceObjective"])
			}
			if tt.global != nil {
				gwConfig := config["globalInferenceGateway"].(map[string]interface{})
				if got := gwConfig["inferenceObjective"]; got != tt.wantGlobal {
					t.Errorf("globalInferenceGateway.inferenceObjective = %v, want %s", got, tt.wantGlobal)
				}
			}
			for model, want := range tt.wantModels {
				mg := config["modelGateways"].(map[string]interface{})[model].(map[string]interface{})
				if got := mg["inferenceObjective"]; got != want {
					t.Errorf("modelGateways.%s.inferenceObjective = %v, want %s", model, got, want)
				}
			}
		})
	}
}

func TestSpecToHelmValues_PrometheusRule(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.PrometheusRule = &batchv1alpha1.PrometheusRuleSpec{
		Enabled: true,
		Labels:  map[string]string{"prometheus": "kube-prometheus"},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	pr := vals["prometheusRule"].(map[string]interface{})
	if got := pr["enabled"]; got != true {
		t.Errorf("prometheusRule.enabled = %v, want true", got)
	}
	labels := pr["labels"].(map[string]interface{})
	if got := labels["prometheus"]; got != "kube-prometheus" {
		t.Errorf("labels.prometheus = %v, want kube-prometheus", got)
	}
}

func TestSpecToHelmValues_APIServerConfig(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.APIServer.Config = &batchv1alpha1.APIServerConfigSpec{
		Port:                8080,
		ObservabilityPort:   9090,
		ReadTimeoutSeconds:  30,
		WriteTimeoutSeconds: 60,
		IdleTimeoutSeconds:  120,
		EnablePprof:         true,
		BatchAPI: &batchv1alpha1.BatchAPIConfig{
			EventTTLSeconds:    3600,
			PassThroughHeaders: []string{"X-Request-ID", "Authorization"},
			ExtraEndpoints:     ptr.To([]string{classifyEndpoint}),
		},
		FileAPI: &batchv1alpha1.FileAPIConfig{
			DefaultExpirationSeconds: 86400,
			MaxSizeBytes:             104857600,
			MaxLineCount:             100000,
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	config := apiserver["config"].(map[string]interface{})

	if got := config["port"]; got != int64(8080) {
		t.Errorf("port = %v, want 8080", got)
	}
	if got := config["observabilityPort"]; got != int64(9090) {
		t.Errorf("observabilityPort = %v, want 9090", got)
	}
	if got := config["readTimeoutSeconds"]; got != int64(30) {
		t.Errorf("readTimeoutSeconds = %v, want 30", got)
	}
	if got := config["writeTimeoutSeconds"]; got != int64(60) {
		t.Errorf("writeTimeoutSeconds = %v, want 60", got)
	}
	if got := config["idleTimeoutSeconds"]; got != int64(120) {
		t.Errorf("idleTimeoutSeconds = %v, want 120", got)
	}
	if got := config["enablePprof"]; got != true {
		t.Errorf("enablePprof = %v, want true", got)
	}

	batchAPI := config["batchAPI"].(map[string]interface{})
	if got := batchAPI["eventTTLSeconds"]; got != int64(3600) {
		t.Errorf("batchAPI.eventTTLSeconds = %v, want 3600", got)
	}
	headers := batchAPI["passThroughHeaders"].([]interface{})
	if len(headers) != 2 || headers[0] != "X-Request-ID" {
		t.Errorf("passThroughHeaders = %v", headers)
	}
	if endpoints := batchAPI["extraEndpoints"].([]interface{}); len(endpoints) != 1 || endpoints[0] != classifyEndpoint {
		t.Errorf("batchAPI.extraEndpoints = %v, want [/v1/classify]", endpoints)
	}

	fileAPI := config["fileAPI"].(map[string]interface{})
	if got := fileAPI["defaultExpirationSeconds"]; got != int64(86400) {
		t.Errorf("fileAPI.defaultExpirationSeconds = %v, want 86400", got)
	}
	if _, found := fileAPI["maxSizeBytes"]; found {
		t.Error("fileAPI.maxSizeBytes should not be rendered by the current chart")
	}
	if got := fileAPI["maxLineCount"]; got != int64(100000) {
		t.Errorf("fileAPI.maxLineCount = %v, want 100000", got)
	}
	global := vals["global"].(map[string]interface{})
	if got := global["maxInputFileSizeBytes"]; got != int64(104857600) {
		t.Errorf("global.maxInputFileSizeBytes = %v, want 104857600", got)
	}
}

func TestSpecToHelmValues_ProcessorConfig(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.GlobalInferenceGateway.InferenceObjective = "max-throughput"
	gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		NumWorkers: 8,
		Concurrency: &batchv1alpha1.ConcurrencyConfig{
			Global:      32,
			PerEndpoint: 16,
			Recovery:    4,
		},
		InputFileDiskBudgetPercent:     20,
		ExtraEndpoints:                 ptr.To([]string{classifyEndpoint}),
		DefaultOutputExpirationSeconds: 7200,
		ProgressUpdateInterval:         "30s",
		EnablePprof:                    true,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})

	if got := config["numWorkers"]; got != int64(8) {
		t.Errorf("numWorkers = %v, want 8", got)
	}
	concurrency, ok := config["concurrency"].(map[string]interface{})
	if !ok {
		t.Fatalf("concurrency not found or wrong type: %T", config["concurrency"])
	}
	if got := concurrency["global"]; got != int64(32) {
		t.Errorf("concurrency.global = %v, want 32", got)
	}
	if got := concurrency["perEndpoint"]; got != int64(16) {
		t.Errorf("concurrency.perEndpoint = %v, want 16", got)
	}
	if got := concurrency["recovery"]; got != int64(4) {
		t.Errorf("concurrency.recovery = %v, want 4", got)
	}
	gwConfig, ok := config["globalInferenceGateway"].(map[string]interface{})
	if !ok {
		t.Fatalf("globalInferenceGateway not found or wrong type: %T", config["globalInferenceGateway"])
	}
	if got := gwConfig["inferenceObjective"]; got != "max-throughput" {
		t.Errorf("globalInferenceGateway.inferenceObjective = %v, want max-throughput", got)
	}
	if got := config["defaultOutputExpirationSeconds"]; got != int64(7200) {
		t.Errorf("defaultOutputExpirationSeconds = %v, want 7200", got)
	}
	if got := config["progressUpdateInterval"]; got != "30s" {
		t.Errorf("progressUpdateInterval = %v, want 30s", got)
	}
	if got := config["inputFileDiskBudgetPercent"]; got != int64(20) {
		t.Errorf("inputFileDiskBudgetPercent = %v, want 20", got)
	}
	if endpoints := config["extraEndpoints"].([]interface{}); len(endpoints) != 1 || endpoints[0] != classifyEndpoint {
		t.Errorf("processor.extraEndpoints = %v, want [/v1/classify]", endpoints)
	}
	if got := config["enablePprof"]; got != true {
		t.Errorf("enablePprof = %v, want true", got)
	}
}

func TestSpecToHelmValues_EmptyExtraEndpointsOverride(t *testing.T) {
	gateway := minimalGateway()
	gateway.Spec.ExtraEndpoints = []string{classifyEndpoint}
	gateway.Spec.APIServer.Config = &batchv1alpha1.APIServerConfigSpec{
		BatchAPI: &batchv1alpha1.BatchAPIConfig{
			ExtraEndpoints: ptr.To([]string{}),
		},
	}
	gateway.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		ExtraEndpoints: ptr.To([]string{}),
	}

	vals := specToBatchHelmValues(gateway, testSecretName(gateway), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	apiConfig := apiserver["config"].(map[string]interface{})
	batchAPI := apiConfig["batchAPI"].(map[string]interface{})
	apiEndpoints, ok := batchAPI["extraEndpoints"].([]interface{})
	if !ok || len(apiEndpoints) != 0 {
		t.Errorf("apiserver extraEndpoints = %v, want an explicit empty list", batchAPI["extraEndpoints"])
	}

	processor := vals["processor"].(map[string]interface{})
	processorConfig := processor["config"].(map[string]interface{})
	processorEndpoints, ok := processorConfig["extraEndpoints"].([]interface{})
	if !ok || len(processorEndpoints) != 0 {
		t.Errorf("processor extraEndpoints = %v, want an explicit empty list", processorConfig["extraEndpoints"])
	}
}

func TestSpecToHelmValues_ProcessorAIMD(t *testing.T) {
	enabled := true
	gw := minimalGateway()
	gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		Concurrency: &batchv1alpha1.ConcurrencyConfig{
			Global:      100,
			PerEndpoint: 10,
			Recovery:    5,
			AIMD: &batchv1alpha1.AIMDConfig{
				Enabled:          &enabled,
				Min:              3,
				BackoffFactor:    "0.5",
				AdditiveIncrease: 2,
			},
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	concurrency, ok := config["concurrency"].(map[string]interface{})
	if !ok {
		t.Fatalf("concurrency not found or wrong type: %T", config["concurrency"])
	}
	aimd, ok := concurrency["aimd"].(map[string]interface{})
	if !ok {
		t.Fatalf("concurrency.aimd not found or wrong type: %T", concurrency["aimd"])
	}
	if got := aimd["enabled"]; got != true {
		t.Errorf("aimd.enabled = %v, want true", got)
	}
	if got := aimd["min"]; got != int64(3) {
		t.Errorf("aimd.min = %v, want 3", got)
	}
	if got := aimd["backoffFactor"]; got != "0.5" {
		t.Errorf("aimd.backoffFactor = %v, want 0.5", got)
	}
	if got := aimd["additiveIncrease"]; got != int64(2) {
		t.Errorf("aimd.additiveIncrease = %v, want 2", got)
	}
}

func TestSpecToHelmValues_GCConfig(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{
		DryRun:         true,
		MaxConcurrency: 10,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	gc := vals["gc"].(map[string]interface{})
	config := gc["config"].(map[string]interface{})

	if got := config["dryRun"]; got != true {
		t.Errorf("dryRun = %v, want true", got)
	}
	collector := config["collector"].(map[string]interface{})
	if got := collector["maxConcurrency"]; got != int64(10) {
		t.Errorf("maxConcurrency = %v, want 10", got)
	}
}

func TestSpecToHelmValues_GCReconciler(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{
		Reconciler: &batchv1alpha1.ReconcilerSpec{
			Enabled:  true,
			Interval: "60m",
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	gc := vals["gc"].(map[string]interface{})
	config := gc["config"].(map[string]interface{})
	reconciler, ok := config["reconciler"].(map[string]interface{})
	if !ok {
		t.Fatalf("reconciler not found or wrong type: %T", config["reconciler"])
	}
	if got := reconciler["enabled"]; got != true {
		t.Errorf("reconciler.enabled = %v, want true", got)
	}
	if got := reconciler["interval"]; got != "60m" {
		t.Errorf("reconciler.interval = %v, want 60m", got)
	}
}

func TestSpecToHelmValues_GCReconcilerDisabled(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{
		Reconciler: &batchv1alpha1.ReconcilerSpec{},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	gc := vals["gc"].(map[string]interface{})
	config := gc["config"].(map[string]interface{})
	reconciler, ok := config["reconciler"].(map[string]interface{})
	if !ok {
		t.Fatalf("reconciler not found or wrong type: %T", config["reconciler"])
	}
	if got := reconciler["enabled"]; got != false {
		t.Errorf("reconciler.enabled = %v, want false", got)
	}
	if _, hasInterval := reconciler["interval"]; hasInterval {
		t.Errorf("reconciler.interval should be absent when not set, got %v", reconciler["interval"])
	}
}

func TestSpecToHelmValues_GCReconcilerNil(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	gc := vals["gc"].(map[string]interface{})
	if _, hasConfig := gc["config"]; hasConfig {
		config := gc["config"].(map[string]interface{})
		if _, hasReconciler := config["reconciler"]; hasReconciler {
			t.Error("reconciler should not be present when Reconciler is nil")
		}
	}
}

func TestSpecToHelmValues_InferenceGatewayMaxRetries(t *testing.T) {
	maxRetries := int32(3)
	gw := minimalGateway()
	gw.Spec.Processor.GlobalInferenceGateway = &batchv1alpha1.InferenceGatewaySpec{
		URL:        "http://gw:8000",
		MaxRetries: &maxRetries,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	gig := config["globalInferenceGateway"].(map[string]interface{})
	if got := gig["maxRetries"]; got != int64(3) {
		t.Errorf("maxRetries = %v, want 3", got)
	}
}

func TestSpecToHelmValues_CoreRedisSettingsIgnored(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Redis = &batchv1alpha1.RedisClientSpec{
		EnableTLS: true,
		Insecure:  true,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	global := vals["global"].(map[string]interface{})
	dbClient := global["dbClient"].(map[string]interface{})
	if _, ok := dbClient["redis"]; ok {
		t.Error("dbClient.redis must not be rendered by the current chart")
	}
}

func TestSpecToHelmValues_InferenceGatewayDefaults(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.GlobalInferenceGateway = &batchv1alpha1.InferenceGatewaySpec{
		URL: "http://gw:8000",
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	gig := config["globalInferenceGateway"].(map[string]interface{})

	checks := map[string]interface{}{
		"maxRetries":     int64(3),
		"requestTimeout": "5m",
		"initialBackoff": "1s",
		"maxBackoff":     "60s",
	}
	for key, want := range checks {
		got, exists := gig[key]
		if !exists {
			t.Errorf("%s must always be present to avoid <nil>/empty rendering in Helm templates", key)
			continue
		}
		if got != want {
			t.Errorf("%s = %v, want %v when field is unset", key, got, want)
		}
	}
}

func TestSpecToHelmValues_OmitsTLSInsecureSkipVerify(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.GlobalInferenceGateway = &batchv1alpha1.InferenceGatewaySpec{
		URL:                   "https://gw:8443",
		TLSInsecureSkipVerify: true,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	gig := config["globalInferenceGateway"].(map[string]interface{})
	if _, found := gig["tlsInsecureSkipVerify"]; found {
		t.Fatal("tlsInsecureSkipVerify should not be rendered into Helm values")
	}
}

func TestSpecToHelmValues_ResourceRequirements(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.APIServer.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	res := apiserver["resources"].(map[string]interface{})

	limits := res["limits"].(map[string]interface{})
	if got := limits["cpu"]; got != "2" {
		t.Errorf("limits.cpu = %q, want %q", got, "2")
	}
	if got := limits["memory"]; got != "1Gi" {
		t.Errorf("limits.memory = %q, want %q", got, "1Gi")
	}

	requests := res["requests"].(map[string]interface{})
	if got := requests["cpu"]; got != "500m" {
		t.Errorf("requests.cpu = %q, want %q", got, "500m")
	}
	if got := requests["memory"]; got != "256Mi" {
		t.Errorf("requests.memory = %q, want %q", got, "256Mi")
	}
}

func TestSpecToHelmValues_GCReplicasAndResources(t *testing.T) {
	gcReplicas := int32(3)
	gw := minimalGateway()
	gw.Spec.GC.Replicas = &gcReplicas
	gw.Spec.GC.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	gc := vals["gc"].(map[string]interface{})
	if got := gc["replicaCount"]; got != int64(3) {
		t.Errorf("gc.replicaCount = %v, want 3", got)
	}
	res := gc["resources"].(map[string]interface{})
	requests := res["requests"].(map[string]interface{})
	if got := requests["cpu"]; got != "10m" {
		t.Errorf("gc.resources.requests.cpu = %v, want 10m", got)
	}
	limits := res["limits"].(map[string]interface{})
	if got := limits["memory"]; got != "128Mi" {
		t.Errorf("gc.resources.limits.memory = %v, want 128Mi", got)
	}
}

func TestSpecToHelmValues_ImagePullSecrets(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
		{Name: "regcred"},
		{Name: "another-secret"},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	global := vals["global"].(map[string]interface{})
	secrets, ok := global["imagePullSecrets"].([]interface{})
	if !ok {
		t.Fatalf("imagePullSecrets not a []interface{}: %T", global["imagePullSecrets"])
	}
	if len(secrets) != 2 {
		t.Fatalf("imagePullSecrets length = %d, want 2", len(secrets))
	}
	first := secrets[0].(map[string]interface{})
	if got := first["name"]; got != "regcred" {
		t.Errorf("imagePullSecrets[0].name = %v, want regcred", got)
	}
	second := secrets[1].(map[string]interface{})
	if got := second["name"]; got != "another-secret" {
		t.Errorf("imagePullSecrets[1].name = %v, want another-secret", got)
	}
}

func TestSpecToHelmValues_InputHeaders(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.APIServer.Config = &batchv1alpha1.APIServerConfigSpec{
		InputHeaders: map[string]string{
			"tenant": "X-MaaS-Username",
		},
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	apiserver := vals["apiserver"].(map[string]interface{})
	config := apiserver["config"].(map[string]interface{})
	ih := config["inputHeaders"].(map[string]interface{})
	if got := ih["tenant"]; got != "X-MaaS-Username" {
		t.Errorf("inputHeaders.tenant = %v, want X-MaaS-Username", got)
	}
}

func TestSpecToHelmValues_SendFairnessHeader(t *testing.T) {
	enabled := true
	gw := minimalGateway()
	gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		SendFairnessHeader: &enabled,
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	if got := config["sendFairnessHeader"]; got != true {
		t.Errorf("sendFairnessHeader = %v, want true", got)
	}
}

func TestSpecToHelmValues_RouteKeyMethod(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
		RouteKeyMethod: "tenant",
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	if got := config["routeKeyMethod"]; got != "tenant" {
		t.Errorf("routeKeyMethod = %v, want tenant", got)
	}
}

func TestSpecToHelmValues_AsyncDispatchModels(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.DispatchMode = dispatchModeAsync
	gw.Spec.Processor.GlobalInferenceGateway = nil
	gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
		"model-a": {
			InferencePoolName:  "pool-a",
			InferenceObjective: "batch-a",
			RequestQueueName:   "llm-d-async:requests:pool-a",
			ResultQueueName:    "llm-d-async:results:pool-a",
		},
	}
	gw.Spec.Processor.AsyncConfig = &batchv1alpha1.AsyncProcessorSpec{
		ResultPollTimeout: "45s",
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	ad, ok := config["asyncDispatch"].(map[string]any)
	if !ok {
		t.Fatal("processor.config.asyncDispatch not set")
	}
	if got := ad["resultPollTimeout"]; got != "45s" {
		t.Errorf("asyncDispatch.resultPollTimeout = %v, want 45s", got)
	}
	models := ad["models"].(map[string]any)
	model := models["model-a"].(map[string]any)
	if got := model["inferencePoolName"]; got != "pool-a" {
		t.Errorf("asyncDispatch.models.model-a.inferencePoolName = %v, want pool-a", got)
	}
	if got := model["inferenceObjective"]; got != "batch-a" {
		t.Errorf("asyncDispatch.models.model-a.inferenceObjective = %v, want batch-a", got)
	}
	if got := model["requestQueueName"]; got != "llm-d-async:requests:pool-a" {
		t.Errorf("asyncDispatch.models.model-a.requestQueueName = %v", got)
	}
	if got := model["resultQueueName"]; got != "llm-d-async:results:pool-a" {
		t.Errorf("asyncDispatch.models.model-a.resultQueueName = %v", got)
	}
	if _, ok := config["modelGateways"]; ok {
		t.Error("processor.config.modelGateways should not be rendered in async mode")
	}
}

func TestSpecToHelmValues_AsyncDispatchResultPollTimeout_Unset(t *testing.T) {
	gw := minimalGateway()

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	if _, ok := config["asyncDispatch"]; ok {
		t.Error("processor.config.asyncDispatch should not be set when resultPollTimeout is empty")
	}
}

func TestSpecToHelmValues_AsyncConfigIgnoredInSyncMode(t *testing.T) {
	gw := minimalGateway()
	gw.Spec.Processor.DispatchMode = "sync"
	gw.Spec.Processor.AsyncConfig = &batchv1alpha1.AsyncProcessorSpec{
		ResultPollTimeout: "45s",
	}

	vals := specToBatchHelmValues(gw, testSecretName(gw), testImages(), tlspkg.ProfileValues{})

	processor := vals["processor"].(map[string]interface{})
	config := processor["config"].(map[string]interface{})
	if _, ok := config["asyncDispatch"]; ok {
		t.Error("processor.config.asyncDispatch should not be set in sync mode")
	}
}

func minimalGateway() *batchv1alpha1.LLMBatchGateway {
	replicas := int32(1)
	return &batchv1alpha1.LLMBatchGateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-gateway",
			Namespace: "default",
		},
		Spec: batchv1alpha1.LLMBatchGatewaySpec{
			SecretRef: corev1.SecretReference{
				Name: "batch-gateway-secrets",
			},
			DBBackend: dbBackendPostgreSQL,
			FileStorage: &batchv1alpha1.FileStorageSpec{
				S3: &batchv1alpha1.S3StorageSpec{
					Region:   "us-east-1",
					Bucket:   "llm-d-batch-gateway",
					Endpoint: "https://s3.example.com",
				},
			},
			APIServer: batchv1alpha1.APIServerSpec{
				Replicas: &replicas,
			},
			Processor: batchv1alpha1.ProcessorSpec{
				Replicas: &replicas,
				GlobalInferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
					URL: "http://inference-gateway:8000",
				},
			},
			GC: batchv1alpha1.GCSpec{
				Interval: "30m",
			},
		},
	}
}
