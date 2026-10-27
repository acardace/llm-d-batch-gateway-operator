package controller

import (
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
)

const (
	asyncQueuePrefix    = "llm-d-async:"
	asyncTransportRedis = "redis-sortedset"
	asyncTransportSQL   = "sql"
)

func asyncRequestQueueName(pool string) string {
	return asyncQueuePrefix + "requests:" + pool
}

func asyncResultQueueName(pool string) string {
	return asyncQueuePrefix + "results:" + pool
}

func (h *HelmRenderer) RenderAsyncChart(gw *batchv1alpha1.LLMBatchGateway, secretName string) ([]*unstructured.Unstructured, error) {
	return h.renderChart(gw, specToAsyncHelmValues(gw, secretName, h.images), func(obj *unstructured.Unstructured) {
		// async does not use this label to indicate component name, workaround to append it for update status
		labels := obj.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[labelKeyComponent] = componentAsyncProcessor
		obj.SetLabels(labels)
	})
}

// specToAsyncHelmValues maps the CRD spec to upstream llm-d-async Helm values.
func specToAsyncHelmValues(gw *batchv1alpha1.LLMBatchGateway, secretName string, images ComponentImages) map[string]any {
	ac := gw.Spec.Processor.AsyncConfig
	if ac == nil {
		return map[string]any{}
	}

	asyncRepo, asyncTag := splitImage(images.Async)
	ap := map[string]any{
		"image": map[string]any{
			"repository": asyncRepo,
			"tag":        asyncTag,
		},
	}
	if ac.Replicas != nil {
		// upstream chart currently hardcodes replicas: 1 in the template;
		// pass the value so it takes effect once upstream templates it from values.
		ap["replicaCount"] = int64(*ac.Replicas)
	}
	if ac.ImagePullPolicy != "" {
		ap["imagePullPolicy"] = string(ac.ImagePullPolicy)
	}

	if ac.Concurrency != nil {
		ap["concurrency"] = int64(*ac.Concurrency)
	}

	if ac.InferenceGateway != nil {
		ap["igwBaseURL"] = ac.InferenceGateway.URL
	}

	if ac.Resources != nil {
		ap["resources"] = resourceRequirementsToMap(ac.Resources)
	}

	setIfNotEmpty(ap, "drainTimeout", ac.DrainTimeout)

	setIfNotEmpty(ap, "prometheusURL", ac.PrometheusURL)
	setIfNotEmpty(ap, "prometheusCacheTTL", ac.PrometheusCacheTTL)

	gatewayURL := ""
	if ac.InferenceGateway != nil {
		gatewayURL = ac.InferenceGateway.URL
	}

	if ac.Redis != nil {
		transportConfig := map[string]any{
			"urlSecret": map[string]any{
				"name": secretName,
				"key":  "redis-url",
			},
		}
		if ac.Redis.PollIntervalMs != nil {
			transportConfig["poll_interval_ms"] = int64(*ac.Redis.PollIntervalMs)
		}
		if ac.Redis.BatchSize != nil {
			transportConfig["batch_size"] = int64(*ac.Redis.BatchSize)
		}
		if gw.Spec.OTEL != nil && gw.Spec.OTEL.RedisTracing {
			transportConfig["enable_tracing"] = true
		}

		queueConfigs := ac.Redis.QueuesConfig
		if len(queueConfigs) == 0 {
			queueConfigs = []batchv1alpha1.AsyncQueueConfig{{
				Name:           "request-sortedset",
				RequestPathURL: ac.Redis.RequestPathURL,
				GateType:       ac.Redis.GateType,
				GateParams:     ac.Redis.GateParams,
			}}
		}

		transportConfig["queues"] = asyncQueueValues(queueConfigs, gatewayURL)
		ap["transport"] = asyncTransportRedis
		ap["transportConfig"] = transportConfig
	} else if ac.SQL != nil {
		transportConfig := map[string]any{
			"urlSecret": map[string]any{
				"name": secretName,
				"key":  "postgresql-url",
			},
		}
		if resultQueue := sqlResultQueueName(gw); resultQueue != "" {
			transportConfig["result_queue_name"] = resultQueue
		}
		if ac.SQL.PollIntervalMs != nil {
			transportConfig["poll_interval_ms"] = int64(*ac.SQL.PollIntervalMs)
		}
		if ac.SQL.BatchSize != nil {
			transportConfig["batch_size"] = int64(*ac.SQL.BatchSize)
		}
		if ac.SQL.ResultBatchSize != nil {
			transportConfig["result_batch_size"] = int64(*ac.SQL.ResultBatchSize)
		}
		if ac.SQL.LeaseTTLSeconds != nil {
			transportConfig["lease_ttl_seconds"] = int64(*ac.SQL.LeaseTTLSeconds)
		}
		if ac.SQL.HandoffTimeoutSeconds != nil {
			transportConfig["handoff_timeout_seconds"] = int64(*ac.SQL.HandoffTimeoutSeconds)
		}
		queueConfigs := ac.SQL.QueuesConfig
		if len(queueConfigs) == 0 {
			queueConfigs = sqlQueueDefaults(gw)
		}
		transportConfig["queues"] = asyncQueueValues(queueConfigs, gatewayURL)
		ap["transport"] = asyncTransportSQL
		ap["transportConfig"] = transportConfig
	}

	if len(ac.WorkerPools) > 0 {
		var pools []any
		for _, wp := range ac.WorkerPools {
			pm := map[string]any{
				"id":      wp.Name,
				"workers": int64(wp.Workers),
			}
			setIfNotEmpty(pm, "gate_type", wp.GateType)
			if len(wp.GateParams) > 0 {
				pm["gate_params"] = toStringInterfaceMap(wp.GateParams)
			}
			pools = append(pools, pm)
		}
		ap["workerPools"] = pools
	}

	if ac.TLS != nil {
		tls := map[string]any{}
		setIfNotEmpty(tls, "secretName", ac.TLS.SecretName)
		tls["insecureSkipVerify"] = ac.TLS.InsecureSkipVerify
		setIfNotEmpty(tls, "caCertKey", ac.TLS.CACertKey)
		setIfNotEmpty(tls, "certKey", ac.TLS.CertKey)
		setIfNotEmpty(tls, "keyKey", ac.TLS.KeyKey)
		ap["tls"] = tls
	}

	if ac.TransformConfig != nil && len(ac.TransformConfig.RequestTransforms) > 0 {
		var transforms []any
		for _, rt := range ac.TransformConfig.RequestTransforms {
			tm := map[string]any{
				"name": rt.Name,
				"type": rt.Type,
			}
			if len(rt.Parameters) > 0 {
				tm["parameters"] = toJSONAwareMap(rt.Parameters)
			}
			transforms = append(transforms, tm)
		}
		ap["transformConfig"] = map[string]any{
			"requestTransforms": transforms,
		}
	}

	if gw.Spec.OTEL != nil {
		otelVals := map[string]any{}
		setIfNotEmpty(otelVals, "endpoint", gw.Spec.OTEL.Endpoint)
		otelVals["insecure"] = gw.Spec.OTEL.Insecure
		setIfNotEmpty(otelVals, "sampler", gw.Spec.OTEL.Sampler)
		setIfNotEmpty(otelVals, "samplerArg", gw.Spec.OTEL.SamplerArg)
		otelVals["redisTracing"] = gw.Spec.OTEL.RedisTracing
		ap["otel"] = otelVals
	}

	if ac.ModelServerMonitor != nil && ac.ModelServerMonitor.Enabled {
		msm := map[string]any{
			"enabled": true,
		}
		if len(ac.ModelServerMonitor.Selector) > 0 {
			msm["selector"] = toStringInterfaceMap(ac.ModelServerMonitor.Selector)
		}
		setIfNotEmpty(msm, "port", ac.ModelServerMonitor.Port)
		setIfNotEmpty(msm, "path", ac.ModelServerMonitor.Path)
		setIfNotEmpty(msm, "interval", ac.ModelServerMonitor.Interval)
		ap["modelServerMonitor"] = msm
	}

	if len(gw.Spec.ImagePullSecrets) > 0 {
		var secrets []any
		for _, s := range gw.Spec.ImagePullSecrets {
			secrets = append(secrets, map[string]any{"name": s.Name})
		}
		ap["imagePullSecrets"] = secrets
	}

	if gw.Spec.Monitoring != nil && gw.Spec.Monitoring.Enabled {
		ap["podMonitor"] = map[string]any{
			"enabled": true,
			"labels": map[string]any{
				odhMonitoringScrapeLabel: odhMonitoringScrapeValue,
			},
		}
	}

	if gw.Spec.Grafana != nil && gw.Spec.Grafana.Enabled {
		ap["grafana"] = map[string]any{
			"dashboards": map[string]any{
				"enabled": true,
			},
		}
	}

	if gw.Spec.PrometheusRule != nil && gw.Spec.PrometheusRule.Enabled {
		pr := map[string]any{
			"enabled": true,
		}
		if len(gw.Spec.PrometheusRule.Labels) > 0 {
			pr["labels"] = toStringInterfaceMap(gw.Spec.PrometheusRule.Labels)
		}
		ap["prometheusRule"] = pr
	}

	return map[string]any{
		// Preserve the resource names and Deployment selector used by the
		// previous async-processor chart during in-place operator upgrades.
		"nameOverride": "async-processor",
		"ap":           ap,
	}
}

func asyncQueueValues(queueConfigs []batchv1alpha1.AsyncQueueConfig, gatewayURL string) []any {
	queues := make([]any, 0, len(queueConfigs))
	for _, q := range queueConfigs {
		qm := map[string]any{"queue_name": q.Name}
		setIfNotEmpty(qm, "worker_pool_id", q.WorkerPoolID)
		queueGatewayURL := q.IGWBaseURL
		if queueGatewayURL == "" {
			queueGatewayURL = gatewayURL
		}
		setIfNotEmpty(qm, "igw_base_url", queueGatewayURL)
		setIfNotEmpty(qm, "request_path_url", q.RequestPathURL)
		setIfNotEmpty(qm, "gate_type", q.GateType)
		if len(q.GateParams) > 0 {
			qm["gate_params"] = toStringInterfaceMap(q.GateParams)
		}
		queues = append(queues, qm)
	}
	return queues
}

func sortedModelGateways(gw *batchv1alpha1.LLMBatchGateway) []batchv1alpha1.ModelGatewaySpec {
	models := slices.Sorted(maps.Keys(gw.Spec.Processor.ModelGateways))

	gateways := make([]batchv1alpha1.ModelGatewaySpec, 0, len(models))
	for _, model := range models {
		gateways = append(gateways, gw.Spec.Processor.ModelGateways[model])
	}
	return gateways
}

// sqlResultQueueName returns the default result queue used by the SQL dispatcher.
// The dispatcher applies it only when a request does not carry its own result
// routing. The Batch Gateway processor stamps every request with
// <base>:<consumerID>, which the dispatcher honors per request, so each model
// keeps its own result queue. The first model's queue (by sorted name) is used
// only as the fallback.
func sqlResultQueueNameFor(gateway batchv1alpha1.ModelGatewaySpec) string {
	if gateway.ResultQueueName != "" {
		return gateway.ResultQueueName
	}
	return asyncResultQueueName(gateway.InferencePoolName)
}

func sqlResultQueueName(gw *batchv1alpha1.LLMBatchGateway) string {
	gateways := sortedModelGateways(gw)
	if len(gateways) == 0 {
		return ""
	}
	return sqlResultQueueNameFor(gateways[0])
}

func sqlRequestQueueName(gateway batchv1alpha1.ModelGatewaySpec) string {
	if gateway.RequestQueueName != "" {
		return gateway.RequestQueueName
	}
	return asyncRequestQueueName(gateway.InferencePoolName)
}

func sqlQueueDefaults(gw *batchv1alpha1.LLMBatchGateway) []batchv1alpha1.AsyncQueueConfig {
	gateways := sortedModelGateways(gw)
	queues := make([]batchv1alpha1.AsyncQueueConfig, 0, len(gateways))
	seen := make(map[string]struct{}, len(gateways))
	for _, gateway := range gateways {
		name := sqlRequestQueueName(gateway)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		queues = append(queues, batchv1alpha1.AsyncQueueConfig{Name: name})
	}
	return queues
}
