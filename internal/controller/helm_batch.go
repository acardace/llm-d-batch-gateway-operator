package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	tlspkg "github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/tls"
)

func (h *HelmRenderer) RenderBatchChart(gw *batchv1alpha1.LLMBatchGateway, secretName string) ([]*unstructured.Unstructured, error) {
	return h.renderChart(gw, specToBatchHelmValues(gw, secretName, h.images, h.tlsProfile), nil)
}

func specToBatchHelmValues(gw *batchv1alpha1.LLMBatchGateway, secretName string, images ComponentImages, tlsProfile tlspkg.ProfileValues) map[string]any {
	vals := map[string]any{}

	// --- Global ---
	dbType := gw.Spec.DBBackend
	if dbType == "" {
		dbType = "postgresql"
	}
	dbClient := map[string]any{
		"type": dbType,
	}
	global := map[string]any{
		"secretName": secretName,
		"dbClient":   dbClient,
	}
	if gw.Spec.ExtraEndpoints != nil {
		global["extraEndpoints"] = toInterfaceSlice(gw.Spec.ExtraEndpoints)
	}
	if gw.Spec.APIServer.Config != nil && gw.Spec.APIServer.Config.FileAPI != nil && gw.Spec.APIServer.Config.FileAPI.MaxSizeBytes != 0 {
		global["maxInputFileSizeBytes"] = gw.Spec.APIServer.Config.FileAPI.MaxSizeBytes
	}

	if gw.Spec.FileStorage != nil {
		fc := map[string]any{}
		if gw.Spec.FileStorage.S3 != nil {
			fc["type"] = "s3"
			s3 := gw.Spec.FileStorage.S3
			s3Vals := map[string]any{}
			setIfNotEmpty(s3Vals, "region", s3.Region)
			setIfNotEmpty(s3Vals, "bucket", s3.Bucket)
			setIfNotEmpty(s3Vals, "endpoint", s3.Endpoint)
			setIfNotEmpty(s3Vals, "accessKeyId", s3.AccessKeyID)
			if s3.Prefix != "" {
				s3Vals["prefix"] = s3.Prefix
			}
			s3Vals["usePathStyle"] = s3.UsePathStyle
			if s3.AutoCreateBucket != nil {
				s3Vals["autoCreateBucket"] = *s3.AutoCreateBucket
			}
			fc["s3"] = s3Vals
		}
		if gw.Spec.FileStorage.FS != nil {
			fc["type"] = "fs"
			fs := gw.Spec.FileStorage.FS
			fsVals := map[string]any{}
			setIfNotEmpty(fsVals, "basePath", fs.BasePath)
			setIfNotEmpty(fsVals, "pvcName", fs.ClaimName)
			fc["fs"] = fsVals
		}
		if gw.Spec.FileStorage.Retry != nil {
			r := gw.Spec.FileStorage.Retry
			retryVals := map[string]any{}
			if r.MaxRetries != nil {
				retryVals["maxRetries"] = int64(*r.MaxRetries)
			}
			setIfNotEmpty(retryVals, "initialBackoff", r.InitialBackoff)
			setIfNotEmpty(retryVals, "maxBackoff", r.MaxBackoff)
			if len(retryVals) > 0 {
				fc["retry"] = retryVals
			}
		}
		global["fileClient"] = fc
	}

	if gw.Spec.OTEL != nil {
		otel := gw.Spec.OTEL
		otelVals := map[string]any{}
		setIfNotEmpty(otelVals, "endpoint", otel.Endpoint)
		otelVals["insecure"] = otel.Insecure
		setIfNotEmpty(otelVals, "sampler", otel.Sampler)
		setIfNotEmpty(otelVals, "samplerArg", otel.SamplerArg)
		otelVals["postgresqlTracing"] = otel.PostgresqlTracing
		global["otel"] = otelVals
	}

	if len(gw.Spec.ImagePullSecrets) > 0 {
		var secrets []any
		for _, s := range gw.Spec.ImagePullSecrets {
			secrets = append(secrets, map[string]any{"name": s.Name})
		}
		global["imagePullSecrets"] = secrets
	}

	vals["global"] = global

	// --- API Server ---
	apiRepo, apiTag := splitImage(images.APIServer)
	apiserverImage := map[string]any{
		"repository": apiRepo,
		"tag":        apiTag,
	}
	if gw.Spec.APIServer.ImagePullPolicy != "" {
		apiserverImage["pullPolicy"] = string(gw.Spec.APIServer.ImagePullPolicy)
	}
	apiserver := map[string]any{
		"enabled": true,
		"image":   apiserverImage,
		"serviceAccount": map[string]any{
			"create": true,
		},
	}
	if gw.Spec.APIServer.Replicas != nil {
		apiserver["replicaCount"] = int64(*gw.Spec.APIServer.Replicas)
	}
	if gw.Spec.APIServer.Resources != nil {
		apiserver["resources"] = resourceRequirementsToMap(gw.Spec.APIServer.Resources)
	}
	if gw.Spec.APIServer.Config != nil {
		apiserver["config"] = apiServerConfigToMap(gw.Spec.APIServer.Config)
		if gw.Spec.APIServer.Config.Logging != nil && gw.Spec.APIServer.Config.Logging.Verbosity != 0 {
			apiserver["logging"] = map[string]any{
				"verbosity": int64(gw.Spec.APIServer.Config.Logging.Verbosity),
			}
		}
	}

	// TLS
	tls := map[string]any{}
	if gw.Spec.TLS != nil && gw.Spec.TLS.Enabled {
		tls["enabled"] = true
		if gw.Spec.TLS.SecretName != "" {
			tls["secretName"] = gw.Spec.TLS.SecretName
		}
		if gw.Spec.TLS.CertManager != nil {
			cm := map[string]any{
				"enabled": true,
			}
			setIfNotEmpty(cm, "issuerName", gw.Spec.TLS.CertManager.IssuerName)
			setIfNotEmpty(cm, "issuerKind", gw.Spec.TLS.CertManager.IssuerKind)
			if len(gw.Spec.TLS.CertManager.DNSNames) > 0 {
				cm["dnsNames"] = toInterfaceSlice(gw.Spec.TLS.CertManager.DNSNames)
			}
			tls["certManager"] = cm
		}
	}
	setIfNotEmpty(tls, "minVersion", tlsProfile.MinVersion)
	setIfNotEmpty(tls, "cipherSuites", tlsProfile.CipherSuites)
	setIfNotEmpty(tls, "nextProtos", tlsProfile.NextProtos)
	if len(tls) > 0 {
		apiserver["tls"] = tls
	}

	// HTTPRoute
	if gw.Spec.HTTPRoute != nil && gw.Spec.HTTPRoute.Enabled {
		hr := map[string]any{
			"enabled": true,
		}
		if len(gw.Spec.HTTPRoute.Annotations) > 0 {
			hr["annotations"] = toStringInterfaceMap(gw.Spec.HTTPRoute.Annotations)
		}
		if len(gw.Spec.HTTPRoute.ParentRefs) > 0 {
			var refs []any
			for _, ref := range gw.Spec.HTTPRoute.ParentRefs {
				r := map[string]any{
					"name": ref.Name,
				}
				setIfNotEmpty(r, "namespace", ref.Namespace)
				setIfNotEmpty(r, "sectionName", ref.SectionName)
				refs = append(refs, r)
			}
			hr["parentRefs"] = refs
		}
		apiserver["httpRoute"] = hr
	}

	// ServiceMonitor
	if gw.Spec.Monitoring != nil && gw.Spec.Monitoring.Enabled {
		apiserver["serviceMonitor"] = map[string]any{
			"enabled": true,
			"labels": map[string]any{
				odhMonitoringScrapeLabel: odhMonitoringScrapeValue,
			},
		}
	}

	vals["apiserver"] = apiserver

	// --- Processor ---
	procRepo, procTag := splitImage(images.Processor)
	processorImage := map[string]any{
		"repository": procRepo,
		"tag":        procTag,
	}
	if gw.Spec.Processor.ImagePullPolicy != "" {
		processorImage["pullPolicy"] = string(gw.Spec.Processor.ImagePullPolicy)
	}
	processor := map[string]any{
		"enabled": true,
		"image":   processorImage,
		"serviceAccount": map[string]any{
			"create": true,
		},
	}
	if gw.Spec.Processor.Replicas != nil {
		processor["replicaCount"] = int64(*gw.Spec.Processor.Replicas)
	}
	if gw.Spec.Processor.Resources != nil {
		processor["resources"] = resourceRequirementsToMap(gw.Spec.Processor.Resources)
	}

	procConfig := map[string]any{}
	setIfNotEmpty(procConfig, "dispatchMode", gw.Spec.Processor.DispatchMode)
	if gw.Spec.Processor.GlobalInferenceGateway != nil {
		procConfig["globalInferenceGateway"] = inferenceGatewayToMap(gw.Spec.Processor.GlobalInferenceGateway)
	}
	if gw.Spec.Processor.DispatchMode != dispatchModeAsync && len(gw.Spec.Processor.ModelGateways) > 0 {
		mg := map[string]any{}
		for model, spec := range gw.Spec.Processor.ModelGateways {
			mg[model] = inferenceGatewayToMap(&spec)
		}
		procConfig["modelGateways"] = mg
	} else if gw.Spec.Processor.DispatchMode == dispatchModeAsync && gw.Spec.Processor.AsyncConfig != nil {
		asyncDispatch := map[string]any{
			"resultPollTimeout": gw.Spec.Processor.AsyncConfig.ResultPollTimeout,
		}
		if len(gw.Spec.Processor.ModelGateways) > 0 {
			models := map[string]any{}
			for model, spec := range gw.Spec.Processor.ModelGateways {
				models[model] = asyncModelGatewayToMap(&spec)
			}
			asyncDispatch["models"] = models
		}
		procConfig["asyncDispatch"] = asyncDispatch
	}
	if gw.Spec.Processor.Config != nil {
		mergeProcessorConfig(procConfig, gw.Spec.Processor.Config)
		if gw.Spec.Processor.Config.Logging != nil && gw.Spec.Processor.Config.Logging.Verbosity != 0 {
			processor["logging"] = map[string]any{
				"verbosity": int64(gw.Spec.Processor.Config.Logging.Verbosity),
			}
		}
	}
	if len(procConfig) > 0 {
		processor["config"] = procConfig
	}

	// PodMonitor
	if gw.Spec.Monitoring != nil && gw.Spec.Monitoring.Enabled {
		processor["podMonitor"] = map[string]any{
			"enabled": true,
			"labels": map[string]any{
				odhMonitoringScrapeLabel: odhMonitoringScrapeValue,
			},
		}
	}

	vals["processor"] = processor

	// --- GC ---
	gcRepo, gcTag := splitImage(images.GC)
	gcImage := map[string]any{
		"repository": gcRepo,
		"tag":        gcTag,
	}
	if gw.Spec.GC.ImagePullPolicy != "" {
		gcImage["pullPolicy"] = string(gw.Spec.GC.ImagePullPolicy)
	}
	gc := map[string]any{
		"enabled": true,
		"image":   gcImage,
		"serviceAccount": map[string]any{
			"create": true,
		},
	}
	if gw.Spec.GC.Replicas != nil {
		gc["replicaCount"] = int64(*gw.Spec.GC.Replicas)
	}
	if gw.Spec.GC.Resources != nil {
		gc["resources"] = resourceRequirementsToMap(gw.Spec.GC.Resources)
	}
	gcConfig := map[string]any{}
	gcCollector := map[string]any{}
	setIfNotEmpty(gcCollector, "interval", gw.Spec.GC.Interval)
	if gw.Spec.GC.Config != nil {
		if gw.Spec.GC.Config.DryRun {
			gcConfig["dryRun"] = true
		}
		if gw.Spec.GC.Config.MaxConcurrency != 0 {
			gcCollector["maxConcurrency"] = int64(gw.Spec.GC.Config.MaxConcurrency)
		}
		if gw.Spec.GC.Config.Logging != nil && gw.Spec.GC.Config.Logging.Verbosity != 0 {
			gc["logging"] = map[string]any{
				"verbosity": int64(gw.Spec.GC.Config.Logging.Verbosity),
			}
		}
		if gw.Spec.GC.Config.Reconciler != nil {
			reconciler := map[string]any{
				"enabled": gw.Spec.GC.Config.Reconciler.Enabled,
			}
			setIfNotEmpty(reconciler, "interval", gw.Spec.GC.Config.Reconciler.Interval)
			gcConfig["reconciler"] = reconciler
		}
	}
	if len(gcCollector) > 0 {
		gcConfig["collector"] = gcCollector
	}
	if len(gcConfig) > 0 {
		gc["config"] = gcConfig
	}

	// PodMonitor
	if gw.Spec.Monitoring != nil && gw.Spec.Monitoring.Enabled {
		gc["podMonitor"] = map[string]any{
			"enabled": true,
			"labels": map[string]any{
				odhMonitoringScrapeLabel: odhMonitoringScrapeValue,
			},
		}
	}

	vals["gc"] = gc

	// --- Grafana ---
	if gw.Spec.Grafana != nil {
		vals["grafana"] = map[string]any{
			"dashboards": map[string]any{
				"enabled": gw.Spec.Grafana.Enabled,
			},
		}
	}

	// --- PrometheusRule ---
	if gw.Spec.PrometheusRule != nil {
		pr := map[string]any{
			"enabled": gw.Spec.PrometheusRule.Enabled,
		}
		if len(gw.Spec.PrometheusRule.Labels) > 0 {
			pr["labels"] = toStringInterfaceMap(gw.Spec.PrometheusRule.Labels)
		}
		vals["prometheusRule"] = pr
	}

	return vals
}

func inferenceGatewayToMap(gw *batchv1alpha1.InferenceGatewaySpec) map[string]interface{} {
	m := map[string]interface{}{}
	setIfNotEmpty(m, "url", gw.URL)
	setIfNotEmpty(m, "inferencePoolName", gw.InferencePoolName)

	// The batch-gateway processor validates that requestTimeout, maxRetries,
	// initialBackoff, and maxBackoff are all present (non-nil) in the rendered
	// config. Always emit these keys with sensible defaults so the chart
	// template never renders <nil> or empty strings for them.
	if gw.RequestTimeout != "" {
		m["requestTimeout"] = gw.RequestTimeout
	} else {
		m["requestTimeout"] = "5m"
	}
	if gw.MaxRetries != nil {
		m["maxRetries"] = int64(*gw.MaxRetries)
	} else {
		m["maxRetries"] = int64(3)
	}
	if gw.InitialBackoff != "" {
		m["initialBackoff"] = gw.InitialBackoff
	} else {
		m["initialBackoff"] = "1s"
	}
	if gw.MaxBackoff != "" {
		m["maxBackoff"] = gw.MaxBackoff
	} else {
		m["maxBackoff"] = "60s"
	}

	setIfNotEmpty(m, "tlsCaCertFile", gw.TLSCACertFile)
	setIfNotEmpty(m, "inferenceObjective", gw.InferenceObjective)
	setIfNotEmpty(m, "tlsClientCertFile", gw.TLSClientCertFile)
	setIfNotEmpty(m, "tlsClientKeyFile", gw.TLSClientKeyFile)
	return m
}

func asyncModelGatewayToMap(gw *batchv1alpha1.InferenceGatewaySpec) map[string]interface{} {
	m := map[string]interface{}{}
	setIfNotEmpty(m, "inferencePoolName", gw.InferencePoolName)
	setIfNotEmpty(m, "inferenceObjective", gw.InferenceObjective)
	setIfNotEmpty(m, "requestQueueName", gw.RequestQueueName)
	setIfNotEmpty(m, "resultQueueName", gw.ResultQueueName)
	return m
}

func apiServerConfigToMap(cfg *batchv1alpha1.APIServerConfigSpec) map[string]interface{} {
	m := map[string]interface{}{}
	if cfg.Port != 0 {
		m["port"] = int64(cfg.Port)
	}
	if cfg.ObservabilityPort != 0 {
		m["observabilityPort"] = int64(cfg.ObservabilityPort)
	}
	if cfg.ReadTimeoutSeconds != 0 {
		m["readTimeoutSeconds"] = int64(cfg.ReadTimeoutSeconds)
	}
	if cfg.WriteTimeoutSeconds != 0 {
		m["writeTimeoutSeconds"] = int64(cfg.WriteTimeoutSeconds)
	}
	if cfg.IdleTimeoutSeconds != 0 {
		m["idleTimeoutSeconds"] = int64(cfg.IdleTimeoutSeconds)
	}
	if cfg.BatchAPI != nil {
		ba := map[string]interface{}{}
		if cfg.BatchAPI.EventTTLSeconds != 0 {
			ba["eventTTLSeconds"] = int64(cfg.BatchAPI.EventTTLSeconds)
		}
		if len(cfg.BatchAPI.PassThroughHeaders) > 0 {
			ba["passThroughHeaders"] = toInterfaceSlice(cfg.BatchAPI.PassThroughHeaders)
		}
		if cfg.BatchAPI.ExtraEndpoints != nil {
			ba["extraEndpoints"] = toInterfaceSlice(cfg.BatchAPI.ExtraEndpoints)
		}
		if len(ba) > 0 {
			m["batchAPI"] = ba
		}
	}
	if cfg.FileAPI != nil {
		fa := map[string]interface{}{}
		if cfg.FileAPI.DefaultExpirationSeconds != 0 {
			fa["defaultExpirationSeconds"] = cfg.FileAPI.DefaultExpirationSeconds
		}
		if cfg.FileAPI.MaxLineCount != 0 {
			fa["maxLineCount"] = cfg.FileAPI.MaxLineCount
		}
		if len(fa) > 0 {
			m["fileAPI"] = fa
		}
	}
	if len(cfg.InputHeaders) > 0 {
		m["inputHeaders"] = toStringInterfaceMap(cfg.InputHeaders)
	}
	if cfg.EnablePprof {
		m["enablePprof"] = true
	}
	return m
}

func mergeProcessorConfig(m map[string]interface{}, cfg *batchv1alpha1.ProcessorConfigSpec) {
	if cfg.NumWorkers != 0 {
		m["numWorkers"] = int64(cfg.NumWorkers)
	}
	if cfg.InputFileDiskBudgetPercent != 0 {
		m["inputFileDiskBudgetPercent"] = int64(cfg.InputFileDiskBudgetPercent)
	}
	if cfg.ExtraEndpoints != nil {
		m["extraEndpoints"] = toInterfaceSlice(cfg.ExtraEndpoints)
	}
	if cfg.Concurrency != nil {
		concurrency := map[string]interface{}{}
		if cfg.Concurrency.Global != 0 {
			concurrency["global"] = int64(cfg.Concurrency.Global)
		}
		if cfg.Concurrency.PerEndpoint != 0 {
			concurrency["perEndpoint"] = int64(cfg.Concurrency.PerEndpoint)
		}
		if cfg.Concurrency.Recovery != 0 {
			concurrency["recovery"] = int64(cfg.Concurrency.Recovery)
		}
		if cfg.Concurrency.AIMD != nil {
			aimd := map[string]interface{}{}
			if cfg.Concurrency.AIMD.Enabled != nil {
				aimd["enabled"] = *cfg.Concurrency.AIMD.Enabled
			}
			if cfg.Concurrency.AIMD.Min != 0 {
				aimd["min"] = int64(cfg.Concurrency.AIMD.Min)
			}
			setIfNotEmpty(aimd, "backoffFactor", cfg.Concurrency.AIMD.BackoffFactor)
			if cfg.Concurrency.AIMD.AdditiveIncrease != 0 {
				aimd["additiveIncrease"] = int64(cfg.Concurrency.AIMD.AdditiveIncrease)
			}
			if len(aimd) > 0 {
				concurrency["aimd"] = aimd
			}
		}
		if len(concurrency) > 0 {
			m["concurrency"] = concurrency
		}
	}
	legacyObjective := cfg.InferenceObjective //nolint:staticcheck // deprecated field is honored on purpose
	if legacyObjective != "" {
		if gw, ok := m["globalInferenceGateway"].(map[string]any); ok {
			setDefaultObjective(gw, legacyObjective)
		}
		if mgs, ok := m["modelGateways"].(map[string]any); ok {
			for _, v := range mgs {
				if mg, ok := v.(map[string]any); ok {
					setDefaultObjective(mg, legacyObjective)
				}
			}
		}
		if ad, ok := m["asyncDispatch"].(map[string]any); ok {
			if models, ok := ad["models"].(map[string]any); ok {
				for _, v := range models {
					if model, ok := v.(map[string]any); ok {
						setDefaultObjective(model, legacyObjective)
					}
				}
			}
		}
	}
	if cfg.DefaultOutputExpirationSeconds != 0 {
		m["defaultOutputExpirationSeconds"] = cfg.DefaultOutputExpirationSeconds
	}
	if cfg.ProgressUpdateInterval != "" {
		m["progressUpdateInterval"] = cfg.ProgressUpdateInterval
	}
	if cfg.SendFairnessHeader != nil {
		m["sendFairnessHeader"] = *cfg.SendFairnessHeader
	}
	setIfNotEmpty(m, "routeKeyMethod", cfg.RouteKeyMethod)
	if cfg.EnablePprof {
		m["enablePprof"] = true
	}
}

func setDefaultObjective(gw map[string]any, objective string) {
	if _, exists := gw["inferenceObjective"]; !exists {
		gw["inferenceObjective"] = objective
	}
}

func resourceRequirementsToMap(r *corev1.ResourceRequirements) map[string]interface{} {
	m := map[string]interface{}{}
	if r.Limits != nil {
		limits := map[string]interface{}{}
		if cpu, ok := r.Limits[corev1.ResourceCPU]; ok {
			limits["cpu"] = cpu.String()
		}
		if mem, ok := r.Limits[corev1.ResourceMemory]; ok {
			limits["memory"] = mem.String()
		}
		m["limits"] = limits
	}
	if r.Requests != nil {
		requests := map[string]interface{}{}
		if cpu, ok := r.Requests[corev1.ResourceCPU]; ok {
			requests["cpu"] = cpu.String()
		}
		if mem, ok := r.Requests[corev1.ResourceMemory]; ok {
			requests["memory"] = mem.String()
		}
		m["requests"] = requests
	}
	return m
}
