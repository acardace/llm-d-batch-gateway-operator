package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
)

// --- Async Processor ---

// AsyncProcessorSpec configures the async-processor component that dispatches
// batch inference requests via a transport-backed queue (Redis or SQL) to
// inference gateways. It is a required part of LLMBatchGatewaySpec; the
// controller always deploys the async-processor Deployment alongside the
// core components.
// +kubebuilder:validation:XValidation:rule="!(has(self.redis) && has(self.sql))",message="asyncConfig.redis and asyncConfig.sql are mutually exclusive"
type AsyncProcessorSpec struct {
	// Replicas is reserved for the future desired number of async-processor pods.
	// It is currently ignored: the upstream llm-d-async chart hardcodes replicas to 1.
	// Setting this field, including 0, does not change or suspend the async processor.
	// It will take effect only after the upstream chart templates it from values.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources defines CPU and memory requests/limits for the async-processor container.
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// ImagePullPolicy overrides the image pull policy for the async-processor container.
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// InferenceGateway configures the connection to the inference gateway
	// EPP (EndpointPicker) service where inference requests are sent.
	InferenceGateway *InferenceGatewaySpec `json:"inferenceGateway,omitempty"`

	// Concurrency is the number of concurrent inference requests the processor dispatches.
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	Concurrency *int32 `json:"concurrency,omitempty"`

	// DrainTimeout is the maximum time to wait for in-flight requests during shutdown (e.g. "2m").
	// +kubebuilder:default="2m"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	DrainTimeout string `json:"drainTimeout,omitempty"`

	// ResultPollTimeout is the maximum time the batch-gateway processor waits
	// for an async dispatch result before timing out (e.g. "30s").
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:MaxLength=32
	ResultPollTimeout string `json:"resultPollTimeout,omitempty"`

	// Redis configures the Redis message queue backend.
	Redis *AsyncRedisSpec `json:"redis,omitempty"`

	// SQL configures the PostgreSQL SQL message queue backend.
	// Redis and SQL are mutually exclusive; async dispatch requires one of them.
	SQL *AsyncSQLSpec `json:"sql,omitempty"`

	// WorkerPools defines named worker pool configurations.
	// +optional
	WorkerPools []AsyncWorkerPool `json:"workerPools,omitempty"`

	// PrometheusURL is the URL of a Prometheus instance for gate metrics queries.
	// +kubebuilder:validation:MaxLength=2048
	PrometheusURL string `json:"prometheusURL,omitempty"`

	// PrometheusCacheTTL is the cache duration for Prometheus query results (e.g. "30s").
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	PrometheusCacheTTL string `json:"prometheusCacheTTL,omitempty"`

	// TLS configures TLS for connections to inference gateways.
	TLS *AsyncTLSSpec `json:"tls,omitempty"`

	// TransformConfig modifies request bodies before forwarding to the inference gateway.
	// +optional
	TransformConfig *AsyncTransformConfig `json:"transformConfig,omitempty"`

	// ModelServerMonitor configs a PodMonitor that scrapes model server metrics.
	ModelServerMonitor *AsyncModelServerMonitorSpec `json:"modelServerMonitor,omitempty"`
}

// AsyncRedisSpec configures the Redis message queue backend for the async-processor.
// Presence of this field (non-nil pointer) enables Redis as the message queue backend.
type AsyncRedisSpec struct {
	// RequestPathURL is the path appended to the inference gateway URL for requests.
	// +kubebuilder:default="/v1/completions"
	// +kubebuilder:validation:MaxLength=2048
	RequestPathURL string `json:"requestPathURL,omitempty"`

	// PollIntervalMs is the polling interval in milliseconds for the Redis queue.
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=100
	PollIntervalMs *int32 `json:"pollIntervalMs,omitempty"`

	// BatchSize is the number of requests to dequeue per poll cycle.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	BatchSize *int32 `json:"batchSize,omitempty"`

	// GateType specifies the dispatch gating mechanism (e.g. "prometheus-saturation", "redis-quota").
	// +kubebuilder:validation:MaxLength=253
	GateType string `json:"gateType,omitempty"`

	// GateParams provides parameters for the gating mechanism.
	GateParams map[string]string `json:"gateParams,omitempty"`

	// QueuesConfig is a list of per-queue configurations.
	// When set, overrides the single-queue settings (RequestPathURL, GateType, etc.).
	// +optional
	QueuesConfig []AsyncQueueConfig `json:"queuesConfig,omitempty"`
}

// AsyncSQLSpec configures the PostgreSQL SQL transport for the async-processor.
// The PostgreSQL URL is read from the operator-managed Secret's
// postgresql-url key; credentials are never stored in the custom resource.
type AsyncSQLSpec struct {
	// PollIntervalMs is the interval in milliseconds for polling SQL queues.
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	PollIntervalMs *int32 `json:"pollIntervalMs,omitempty"`

	// BatchSize is the number of requests to dispatch per poll cycle.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	BatchSize *int32 `json:"batchSize,omitempty"`

	// MaxConnections caps the SQL connection pool for each Batch Gateway
	// processor replica (async_dispatch.max_connections). It is separate from
	// the async processor dispatcher's own SQL pool.
	// When omitted, the chart default (8) applies. Explicit values must be positive.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxConnections *int32 `json:"maxConnections,omitempty"`

	// ResultBatchSize is the maximum number of results written per transaction.
	// +kubebuilder:validation:Minimum=1
	ResultBatchSize *int32 `json:"resultBatchSize,omitempty"`

	// LeaseTTLSeconds controls SQL partition lease expiry after a processor stops heartbeating.
	// +kubebuilder:validation:Minimum=1
	LeaseTTLSeconds *int32 `json:"leaseTTLSeconds,omitempty"`

	// HandoffTimeoutSeconds bounds graceful partition handoff during processor shutdown.
	// +kubebuilder:validation:Minimum=1
	HandoffTimeoutSeconds *int32 `json:"handoffTimeoutSeconds,omitempty"`

	// QueuesConfig configures SQL request queues and their dispatch gates.
	// +optional
	QueuesConfig []AsyncQueueConfig `json:"queuesConfig,omitempty"`
}

// AsyncQueueConfig configures a single async-processor request queue.
type AsyncQueueConfig struct {
	// Name is the transport-specific request queue name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// WorkerPoolID assigns this queue to a named worker pool.
	// Must match a workerPools[].name entry. Defaults to "default" if omitted.
	// +kubebuilder:validation:MaxLength=253
	WorkerPoolID string `json:"workerPoolID,omitempty"`

	// IGWBaseURL overrides the top-level asyncConfig.inferenceGateway URL for this queue.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https?://.+$`
	IGWBaseURL string `json:"igwBaseURL,omitempty"`

	// RequestPathURL overrides the request path for this queue.
	// +kubebuilder:validation:MaxLength=2048
	RequestPathURL string `json:"requestPathURL,omitempty"`

	// GateType overrides the gating mechanism for this queue.
	// +kubebuilder:validation:MaxLength=253
	GateType string `json:"gateType,omitempty"`

	// GateParams overrides the gate parameters for this queue.
	GateParams map[string]string `json:"gateParams,omitempty"`
}

// AsyncTLSSpec configures TLS for the async processor's connections to the
// inference gateway. It applies to every queue of the dispatcher, which is a
// single process-wide client configuration.
// +kubebuilder:validation:XValidation:rule="!(has(self.caCertKey) || has(self.certKey) || has(self.keyKey)) || has(self.secretName)",message="tls.secretName is required when tls.caCertKey, tls.certKey or tls.keyKey are set"
// +kubebuilder:validation:XValidation:rule="has(self.certKey) == has(self.keyKey)",message="tls.certKey and tls.keyKey must both be set or both be empty"
type AsyncTLSSpec struct {
	// SecretName is the name of the Secret, in the LLMBatchGateway's namespace,
	// that holds the CA certificate and optional client certificate/key. Its
	// keys are selected with caCertKey, certKey and keyKey below.
	// +kubebuilder:validation:MaxLength=253
	SecretName string `json:"secretName,omitempty"`

	// InsecureSkipVerify disables verification of the inference gateway's TLS
	// certificate. Not recommended for production; prefer caCertKey.
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CACertKey is the key in SecretName holding a custom CA certificate used to
	// verify the inference gateway.
	// +kubebuilder:validation:MaxLength=253
	CACertKey string `json:"caCertKey,omitempty"`

	// CertKey is the key in SecretName holding the client certificate for mutual
	// TLS. Requires keyKey.
	// +kubebuilder:validation:MaxLength=253
	CertKey string `json:"certKey,omitempty"`

	// KeyKey is the key in SecretName holding the client private key for mutual
	// TLS. Requires certKey.
	// +kubebuilder:validation:MaxLength=253
	KeyKey string `json:"keyKey,omitempty"`
}

type AsyncTransformConfig struct {
	// +optional
	RequestTransforms []AsyncRequestTransform `json:"requestTransforms,omitempty"`
}

type AsyncRequestTransform struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Type string `json:"type"`

	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

type AsyncModelServerMonitorSpec struct {
	Enabled bool `json:"enabled,omitempty"`

	// +optional
	Selector map[string]string `json:"selector,omitempty"`

	// +kubebuilder:default="modelserver"
	// +kubebuilder:validation:MaxLength=253
	Port string `json:"port,omitempty"`

	// +kubebuilder:default="/metrics"
	// +kubebuilder:validation:MaxLength=2048
	Path string `json:"path,omitempty"`

	// +kubebuilder:default="15s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	Interval string `json:"interval,omitempty"`
}

// AsyncWorkerPool defines a named worker pool configuration for the async-processor.
type AsyncWorkerPool struct {
	// Name identifies this worker pool.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Workers is the number of concurrent workers in this pool.
	// +kubebuilder:validation:Minimum=1
	Workers int32 `json:"workers,omitempty"`

	// GateType specifies the dispatch gating mechanism for this pool
	// (e.g. "prometheus-saturation", "prometheus-budget", "redis-quota").
	// +kubebuilder:validation:MaxLength=253
	GateType string `json:"gateType,omitempty"`

	// GateParams provides parameters for the gating mechanism.
	GateParams map[string]string `json:"gateParams,omitempty"`
}
