package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LLMBatchGateway is the Schema for the llmbatchgateways API.
// It represents a fully-managed deployment of the LLM-D batch gateway,
// consisting of an API server, a request processor and a garbage-collector.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:shortName=lbg
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="API-Ready",type="integer",JSONPath=`.status.componentStatus.apiServer.readyReplicas`
// +kubebuilder:printcolumn:name="Proc-Ready",type="integer",JSONPath=`.status.componentStatus.processor.readyReplicas`
// +kubebuilder:printcolumn:name="GC-Ready",type="integer",JSONPath=`.status.componentStatus.gc.readyReplicas`
// +kubebuilder:printcolumn:name="AsyncProc-Ready",type="integer",JSONPath=`.status.componentStatus.asyncProcessor.readyReplicas`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type LLMBatchGateway struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of LLMBatchGateway.
	Spec LLMBatchGatewaySpec `json:"spec"`

	// Status defines the observed state of LLMBatchGateway.
	Status LLMBatchGatewayStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LLMBatchGatewayList contains a list of LLMBatchGateway.
type LLMBatchGatewayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LLMBatchGateway `json:"items"`
}

// LLMBatchGatewaySpec defines the desired state of the batch gateway deployment.
// +kubebuilder:validation:XValidation:rule="size(self.secretRef.name) > 0",message="spec.secretRef.name is required"
// +kubebuilder:validation:XValidation:rule="!has(self.asyncProcessor) || size(self.asyncProcessor.queues) > 0 || has(self.processor.globalInferenceGateway)",message="spec.processor.globalInferenceGateway is required when asyncProcessor is enabled without explicit queues"
type LLMBatchGatewaySpec struct {
	// SecretRef references the Kubernetes Secret that holds runtime credentials
	// (database URL, S3 keys, inference API key, etc.).
	//
	// When Namespace is omitted the Secret must reside in the same namespace as
	// the LLMBatchGateway CR.
	//
	// When Namespace is set to a different namespace, a Gateway API ReferenceGrant
	// must exist in that namespace permitting this LLMBatchGateway to reference
	// the Secret. The controller will copy the Secret into the CR's namespace
	// under a managed name (<gateway-name>-credentials) so that the workload
	// Pods can mount it.
	//
	// SecretRef is immutable after creation. To use a different Secret, delete
	// and recreate the LLMBatchGateway CR.
	// +kubebuilder:validation:Required
	SecretRef corev1.SecretReference `json:"secretRef"`

	// DBBackend selects the database backend used for job state storage.
	// +kubebuilder:validation:Enum=redis;postgresql;valkey
	// +kubebuilder:default=postgresql
	DBBackend string `json:"dbBackend,omitempty"`

	// FileStorage configures the file storage backend used to persist batch
	// input/output files. If omitted, S3 defaults are used.
	FileStorage *FileStorageSpec `json:"fileStorage,omitempty"`

	// APIServer configures the HTTP API server component.
	// +kubebuilder:validation:Required
	APIServer APIServerSpec `json:"apiServer"`

	// Processor configures the request-processing worker component.
	// +kubebuilder:validation:Required
	Processor ProcessorSpec `json:"processor"`

	// GC configures the garbage-collector component that expires old jobs and files.
	// +kubebuilder:validation:Required
	GC GCSpec `json:"gc"`

	// Monitoring enables Prometheus ServiceMonitor and PodMonitor resources.
	Monitoring *MonitoringSpec `json:"monitoring,omitempty"`

	// Grafana enables the bundled Grafana dashboard ConfigMap.
	Grafana *GrafanaSpec `json:"grafana,omitempty"`

	// TLS configures TLS termination for the API server.
	TLS *TLSSpec `json:"tls,omitempty"`

	// HTTPRoute configures a Gateway API HTTPRoute resource to expose the API server.
	HTTPRoute *HTTPRouteSpec `json:"httpRoute,omitempty"`

	// OTEL configures OpenTelemetry tracing for all components.
	OTEL *OTELSpec `json:"otel,omitempty"`

	// PrometheusRule configures a PrometheusRule resource with pre-built alerting rules.
	PrometheusRule *PrometheusRuleSpec `json:"prometheusRule,omitempty"`

	// AsyncProcessor optionally enables the async processor (batch dispatcher) component.
	// When nil, the async processor is not deployed.
	AsyncProcessor *AsyncProcessorSpec `json:"asyncProcessor,omitempty"`
}

// --- File Storage ---

// FileStorageSpec configures the file storage backend.
// Exactly one of s3 or fs must be set.
// +kubebuilder:validation:XValidation:rule="has(self.s3) != has(self.fs)",message="exactly one of fileStorage.s3 or fileStorage.fs must be set"
type FileStorageSpec struct {
	// S3 configures S3-compatible object storage. Mutually exclusive with fs.
	S3 *S3StorageSpec `json:"s3,omitempty"`

	// FS configures PVC-backed filesystem storage. Mutually exclusive with s3.
	FS *FSStorageSpec `json:"fs,omitempty"`

	// Retry configures retry behaviour for transient file storage errors.
	Retry *FileRetrySpec `json:"retry,omitempty"`
}

// S3StorageSpec configures S3-compatible object storage access.
// The secret access key must be provided via spec.secretRef (key: s3-secret-access-key).
type S3StorageSpec struct {
	// Region is the AWS region (e.g. "us-east-1").
	// +kubebuilder:validation:MaxLength=253
	Region string `json:"region,omitempty"`

	// Endpoint overrides the S3 endpoint URL for non-AWS providers (e.g. MinIO).
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint,omitempty"`

	// AccessKeyID is the S3 access key ID (non-sensitive). The corresponding
	// secret access key must be provided via spec.secretRef.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	AccessKeyID string `json:"accessKeyId,omitempty"`

	// Prefix is an optional key prefix applied to all objects written to the bucket.
	// +kubebuilder:validation:MaxLength=1024
	Prefix string `json:"prefix,omitempty"`

	// UsePathStyle forces path-style addressing (required by some S3-compatible stores).
	UsePathStyle bool `json:"usePathStyle,omitempty"`

	// AutoCreateBucket causes the operator to create the bucket if it does not exist.
	AutoCreateBucket bool `json:"autoCreateBucket,omitempty"`
}

// FSStorageSpec configures PVC-backed filesystem storage.
type FSStorageSpec struct {
	// BasePath is the root directory inside the PVC where files are stored.
	// +kubebuilder:validation:MaxLength=4096
	BasePath string `json:"basePath,omitempty"`

	// ClaimName is the name of the PersistentVolumeClaim to mount.
	// +kubebuilder:validation:MaxLength=253
	ClaimName string `json:"claimName,omitempty"`
}

// FileRetrySpec configures retry behaviour for transient file storage errors.
type FileRetrySpec struct {
	// MaxRetries is the maximum number of retry attempts.
	// +kubebuilder:default=3
	MaxRetries int32 `json:"maxRetries,omitempty"`

	// InitialBackoff is the wait duration before the first retry (e.g. "1s").
	// +kubebuilder:default="1s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	InitialBackoff string `json:"initialBackoff,omitempty"`

	// MaxBackoff is the maximum wait duration between retries (e.g. "10s").
	// +kubebuilder:default="10s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	MaxBackoff string `json:"maxBackoff,omitempty"`
}

// --- API Server ---

// APIServerSpec configures the HTTP API server component.
type APIServerSpec struct {
	// Replicas is the desired number of API server pods.
	// Setting this to 0 suspends the API server; the Ready condition will be False.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Image is the container image for the API server (e.g. "ghcr.io/org/apiserver:v1.2.3").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=1024
	Image string `json:"image"`

	// Resources defines CPU and memory requests/limits for the API server container.
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Config holds fine-grained API server configuration.
	Config *APIServerConfigSpec `json:"config,omitempty"`
}

// APIServerConfigSpec holds fine-grained configuration for the API server process.
type APIServerConfigSpec struct {
	// Port is the TCP port the API server listens on.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`

	// ObservabilityPort is the TCP port that exposes metrics and health endpoints.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	ObservabilityPort int32 `json:"observabilityPort,omitempty"`

	// ReadTimeoutSeconds is the maximum duration for reading an entire HTTP request.
	ReadTimeoutSeconds int32 `json:"readTimeoutSeconds,omitempty"`

	// WriteTimeoutSeconds is the maximum duration before timing out writes of the response.
	WriteTimeoutSeconds int32 `json:"writeTimeoutSeconds,omitempty"`

	// IdleTimeoutSeconds is the maximum amount of time to wait for the next request.
	IdleTimeoutSeconds int32 `json:"idleTimeoutSeconds,omitempty"`

	// BatchAPI configures the batch job submission endpoint behaviour.
	BatchAPI *BatchAPIConfig `json:"batchAPI,omitempty"`

	// FileAPI configures the file upload/download endpoint behaviour.
	FileAPI *FileAPIConfig `json:"fileAPI,omitempty"`

	// EnablePprof enables the Go pprof profiling HTTP endpoints.
	EnablePprof bool `json:"enablePprof,omitempty"`

	// Logging configures log verbosity for the API server.
	Logging *LoggingConfig `json:"logging,omitempty"`
}

// BatchAPIConfig configures the batch job submission API behaviour.
type BatchAPIConfig struct {
	// EventTTLSeconds is how long job events are retained before expiry.
	EventTTLSeconds int32 `json:"eventTTLSeconds,omitempty"`

	// PassThroughHeaders is a list of HTTP request headers forwarded to the inference backend.
	// +kubebuilder:validation:items:MaxLength=256
	PassThroughHeaders []string `json:"passThroughHeaders,omitempty"`
}

// FileAPIConfig configures the file upload/download API behaviour.
type FileAPIConfig struct {
	// DefaultExpirationSeconds is the default TTL for uploaded files.
	DefaultExpirationSeconds int64 `json:"defaultExpirationSeconds,omitempty"`

	// MaxSizeBytes is the maximum allowed size of a single uploaded file.
	MaxSizeBytes int64 `json:"maxSizeBytes,omitempty"`

	// MaxLineCount is the maximum number of lines allowed in a JSONL batch input file.
	MaxLineCount int64 `json:"maxLineCount,omitempty"`
}

// LoggingConfig configures log verbosity.
type LoggingConfig struct {
	// Verbosity sets the log verbosity level (higher means more verbose).
	Verbosity int32 `json:"verbosity,omitempty"`
}

// --- Processor ---

// ProcessorSpec configures the request-processing worker component.
type ProcessorSpec struct {
	// Replicas is the desired number of processor pods.
	// Setting this to 0 suspends the processor; the Ready condition will be False.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Image is the container image for the processor (e.g. "ghcr.io/org/processor:v1.2.3").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=1024
	Image string `json:"image"`

	// Resources defines CPU and memory requests/limits for the processor container.
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// GlobalInferenceGateway is the default inference gateway used for all models
	// unless overridden by a ModelGateways entry.
	GlobalInferenceGateway *InferenceGatewaySpec `json:"globalInferenceGateway,omitempty"`

	// ModelGateways maps model names to per-model inference gateway configurations,
	// overriding GlobalInferenceGateway for those models.
	ModelGateways map[string]InferenceGatewaySpec `json:"modelGateways,omitempty"`

	// Config holds fine-grained processor configuration.
	Config *ProcessorConfigSpec `json:"config,omitempty"`
}

// InferenceGatewaySpec configures a connection to an inference gateway.
type InferenceGatewaySpec struct {
	// URL is the base URL of the inference gateway (e.g. "http://gateway.svc:8000").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`

	// RequestTimeout is the maximum time to wait for a single inference response (e.g. "5m").
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:MaxLength=32
	RequestTimeout string `json:"requestTimeout,omitempty"`

	// MaxRetries is the maximum number of retry attempts on transient errors.
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// InitialBackoff is the wait duration before the first retry (e.g. "500ms").
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:MaxLength=32
	InitialBackoff string `json:"initialBackoff,omitempty"`

	// MaxBackoff is the maximum wait duration between retries (e.g. "30s").
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:MaxLength=32
	MaxBackoff string `json:"maxBackoff,omitempty"`

	// TLSInsecureSkipVerify disables TLS certificate verification. Not recommended for production.
	TLSInsecureSkipVerify bool `json:"tlsInsecureSkipVerify,omitempty"`

	// TLSCACertFile is the path to a custom CA certificate file for verifying the gateway's TLS certificate.
	// +kubebuilder:validation:MaxLength=4096
	TLSCACertFile string `json:"tlsCACertFile,omitempty"`

	// TLSClientCertFile is the path to the client TLS certificate file for mutual TLS.
	// +kubebuilder:validation:MaxLength=4096
	TLSClientCertFile string `json:"tlsClientCertFile,omitempty"`

	// TLSClientKeyFile is the path to the client TLS private key file for mutual TLS.
	// +kubebuilder:validation:MaxLength=4096
	TLSClientKeyFile string `json:"tlsClientKeyFile,omitempty"`
}

// ProcessorConfigSpec holds fine-grained configuration for the processor process.
type ProcessorConfigSpec struct {
	// NumWorkers is the number of concurrent worker goroutines processing jobs.
	NumWorkers int32 `json:"numWorkers,omitempty"`

	// GlobalConcurrency is the maximum number of in-flight inference requests across all models.
	GlobalConcurrency int32 `json:"globalConcurrency,omitempty"`

	// PerModelMaxConcurrency is the maximum number of in-flight inference requests per model.
	PerModelMaxConcurrency int32 `json:"perModelMaxConcurrency,omitempty"`

	// RecoveryMaxConcurrency is the maximum concurrency for recovering in-progress jobs after restart.
	RecoveryMaxConcurrency int32 `json:"recoveryMaxConcurrency,omitempty"`

	// InferenceObjective specifies the scheduling objective (e.g. "throughput", "latency").
	// +kubebuilder:validation:MaxLength=253
	InferenceObjective string `json:"inferenceObjective,omitempty"`

	// DefaultOutputExpirationSeconds is the TTL for job output files.
	DefaultOutputExpirationSeconds int64 `json:"defaultOutputExpirationSeconds,omitempty"`

	// ProgressTTLSeconds is how long in-progress job state is retained before being considered stale.
	ProgressTTLSeconds int64 `json:"progressTTLSeconds,omitempty"`

	// EnablePprof enables the Go pprof profiling HTTP endpoints.
	EnablePprof bool `json:"enablePprof,omitempty"`

	// Logging configures log verbosity for the processor.
	Logging *LoggingConfig `json:"logging,omitempty"`
}

// --- GC ---

// GCSpec configures the garbage-collector component.
type GCSpec struct {
	// Image is the container image for the GC (e.g. "ghcr.io/org/batch-gc:v1.2.3").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=1024
	Image string `json:"image"`

	// Interval is how often the GC runs (e.g. "30m").
	// +kubebuilder:default="30m"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	Interval string `json:"interval,omitempty"`

	// Config holds fine-grained GC configuration.
	Config *GCConfigSpec `json:"config,omitempty"`
}

// GCConfigSpec holds fine-grained configuration for the garbage-collector process.
type GCConfigSpec struct {
	// DryRun causes the GC to log what it would delete without actually deleting anything.
	DryRun bool `json:"dryRun,omitempty"`

	// MaxConcurrency is the maximum number of deletion operations performed in parallel.
	MaxConcurrency int32 `json:"maxConcurrency,omitempty"`

	// Logging configures log verbosity for the GC.
	Logging *LoggingConfig `json:"logging,omitempty"`
}

// --- Observability ---

// MonitoringSpec enables Prometheus monitoring resources.
type MonitoringSpec struct {
	// Enabled controls whether a ServiceMonitor and PodMonitor are created for each component.
	Enabled bool `json:"enabled,omitempty"`
}

// GrafanaSpec enables the bundled Grafana dashboard.
type GrafanaSpec struct {
	// Enabled controls whether a Grafana dashboard ConfigMap is created.
	Enabled bool `json:"enabled,omitempty"`
}

// PrometheusRuleSpec configures a PrometheusRule resource with pre-built alerting rules.
type PrometheusRuleSpec struct {
	// Enabled controls whether a PrometheusRule resource is created.
	Enabled bool `json:"enabled,omitempty"`

	// Labels are additional labels applied to the PrometheusRule resource,
	// used to match Prometheus operator rule selectors.
	Labels map[string]string `json:"labels,omitempty"`
}

// OTELSpec configures OpenTelemetry tracing for all components.
type OTELSpec struct {
	// Endpoint is the OTLP gRPC or HTTP endpoint (e.g. "http://collector:4317").
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint,omitempty"`

	// Insecure disables TLS for the OTLP connection.
	Insecure bool `json:"insecure,omitempty"`

	// Sampler is the OTEL sampler type (e.g. "parentbased_traceidratio").
	// +kubebuilder:validation:MaxLength=253
	Sampler string `json:"sampler,omitempty"`

	// SamplerArg is the argument passed to the sampler (e.g. "0.1" for 10% sampling).
	// +kubebuilder:validation:MaxLength=253
	SamplerArg string `json:"samplerArg,omitempty"`

	// RedisTracing enables tracing of Redis operations.
	RedisTracing bool `json:"redisTracing,omitempty"`

	// PostgresqlTracing enables tracing of PostgreSQL operations.
	PostgresqlTracing bool `json:"postgresqlTracing,omitempty"`
}

// --- TLS ---

// TLSSpec configures TLS termination for the API server.
// +kubebuilder:validation:XValidation:rule="!self.enabled || size(self.secretName) > 0 || has(self.certManager)",message="tls.secretName or tls.certManager must be set when tls.enabled is true"
// +kubebuilder:validation:XValidation:rule="!(size(self.secretName) > 0 && has(self.certManager))",message="tls.secretName and tls.certManager are mutually exclusive"
type TLSSpec struct {
	// Enabled controls whether TLS termination is configured for the API server.
	Enabled bool `json:"enabled,omitempty"`

	// SecretName is the name of an existing TLS Secret (type kubernetes.io/tls)
	// containing the certificate and key. Mutually exclusive with certManager.
	// +kubebuilder:validation:MaxLength=253
	SecretName string `json:"secretName,omitempty"`

	// CertManager configures automatic certificate provisioning via cert-manager.
	// Mutually exclusive with secretName.
	CertManager *CertManagerSpec `json:"certManager,omitempty"`
}

// CertManagerSpec configures automatic certificate provisioning via cert-manager.
// +kubebuilder:validation:XValidation:rule="size(self.issuerName) > 0",message="certManager.issuerName must be set"
type CertManagerSpec struct {
	// IssuerName is the name of the cert-manager Issuer or ClusterIssuer.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	IssuerName string `json:"issuerName"`

	// IssuerKind is the kind of the cert-manager issuer resource.
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	// +kubebuilder:default=ClusterIssuer
	IssuerKind string `json:"issuerKind,omitempty"`

	// DNSNames is the list of DNS SANs to include in the issued certificate.
	// +kubebuilder:validation:items:MaxLength=253
	DNSNames []string `json:"dnsNames,omitempty"`
}

// --- HTTPRoute ---

// HTTPRouteSpec configures a Gateway API HTTPRoute to expose the API server.
type HTTPRouteSpec struct {
	// Enabled controls whether an HTTPRoute resource is created.
	Enabled bool `json:"enabled,omitempty"`

	// Annotations are extra annotations applied to the HTTPRoute resource.
	Annotations map[string]string `json:"annotations,omitempty"`

	// ParentRefs is the list of Gateways this HTTPRoute should attach to.
	// +listType=atomic
	ParentRefs []ParentReference `json:"parentRefs,omitempty"`
}

// ParentReference identifies a Gateway to which an HTTPRoute should attach.
type ParentReference struct {
	// Name is the name of the Gateway resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace is the namespace of the Gateway resource. Defaults to the HTTPRoute's namespace.
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace,omitempty"`

	// SectionName is the name of a specific listener on the Gateway to attach to.
	// +kubebuilder:validation:MaxLength=253
	SectionName string `json:"sectionName,omitempty"`
}

// --- Async Processor ---

// AsyncProcessorSpec configures the async processor (batch dispatcher) component.
type AsyncProcessorSpec struct {
	// Image is the container image for the async processor.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=1024
	Image string `json:"image"`

	// Replicas is the desired number of async processor pods.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources defines CPU and memory requests/limits for the async processor container.
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Concurrency is the number of concurrent dispatch workers.
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	Concurrency int32 `json:"concurrency,omitempty"`

	// RequestTimeout is the maximum time to wait for a single inference response (e.g. "5m").
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:MaxLength=32
	RequestTimeout string `json:"requestTimeout,omitempty"`

	// PollIntervalMs is the Redis sorted set poll interval in milliseconds.
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=100
	PollIntervalMs int32 `json:"pollIntervalMs,omitempty"`

	// BatchSize is the number of messages to dequeue per poll cycle.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	BatchSize int32 `json:"batchSize,omitempty"`

	// Prometheus configures the Prometheus server for metric-based dispatch gates.
	Prometheus *AsyncProcessorPrometheusSpec `json:"prometheus,omitempty"`

	// Queues defines per-queue configuration for multi-queue mode.
	// Each key is a unique queue ID.
	// When omitted, a single default queue is configured using
	// spec.processor.globalInferenceGateway.
	Queues map[string]AsyncProcessorQueueSpec `json:"queues,omitempty"`
}

// AsyncProcessorPrometheusSpec configures a Prometheus server connection.
type AsyncProcessorPrometheusSpec struct {
	// URL is the Prometheus server base URL (e.g. "http://prometheus.monitoring.svc:9090").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`

	// CacheTTL is the TTL for cached Prometheus query results (e.g. "5s").
	// +kubebuilder:default="5s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`
	// +kubebuilder:validation:MaxLength=32
	CacheTTL string `json:"cacheTTL,omitempty"`
}

// AsyncProcessorQueueSpec configures a single dispatch queue.
type AsyncProcessorQueueSpec struct {
	// InferenceGatewayURL is the base URL of the inference gateway for this queue.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=2048
	InferenceGatewayURL string `json:"inferenceGatewayURL"`

	// RequestPathURL is the HTTP path for inference requests.
	// +kubebuilder:default="/v1/completions"
	// +kubebuilder:validation:MaxLength=2048
	RequestPathURL string `json:"requestPathURL,omitempty"`

	// InferenceObjective is the value of the inference-objective header.
	// +kubebuilder:validation:MaxLength=253
	InferenceObjective string `json:"inferenceObjective,omitempty"`

	// QueueName is the Redis sorted set name for incoming requests.
	// +kubebuilder:default="request-sortedset"
	// +kubebuilder:validation:MaxLength=253
	QueueName string `json:"queueName,omitempty"`

	// ResultQueueName is the Redis list name for completed results.
	// +kubebuilder:default="result-list"
	// +kubebuilder:validation:MaxLength=253
	ResultQueueName string `json:"resultQueueName,omitempty"`

	// GateType is the dispatch gate type (e.g. "constant", "redis",
	// "prometheus-saturation", "prometheus-budget").
	// +kubebuilder:validation:MaxLength=253
	GateType string `json:"gateType,omitempty"`

	// GateParams are key-value parameters passed to the dispatch gate.
	GateParams map[string]string `json:"gateParams,omitempty"`
}

// --- Status ---

// LLMBatchGatewayStatus defines the observed state of LLMBatchGateway.
type LLMBatchGatewayStatus struct {
	// ObservedGeneration is the .metadata.generation that was last processed by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the LLMBatchGateway's state.
	// Known condition types: Ready, APIServerAvailable, ProcessorAvailable, GCAvailable, AsyncProcessorAvailable.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ComponentStatus reports the replica counts for each managed component.
	ComponentStatus *ComponentStatus `json:"componentStatus,omitempty"`
}

// ComponentStatus reports the replica status of each managed component.
type ComponentStatus struct {
	// APIServer reports the replica status of the API server Deployment.
	APIServer *ComponentReplicaStatus `json:"apiServer,omitempty"`

	// Processor reports the replica status of the processor Deployment.
	Processor *ComponentReplicaStatus `json:"processor,omitempty"`

	// GC reports the replica status of the garbage-collector Deployment.
	GC *ComponentReplicaStatus `json:"gc,omitempty"`

	// AsyncProcessor reports the replica status of the async processor Deployment.
	AsyncProcessor *ComponentReplicaStatus `json:"asyncProcessor,omitempty"`
}

// ComponentReplicaStatus reports the desired and ready replica counts for a Deployment.
type ComponentReplicaStatus struct {
	// Replicas is the total number of non-terminated pods.
	Replicas int32 `json:"replicas"`

	// ReadyReplicas is the number of pods that have passed their readiness checks.
	ReadyReplicas int32 `json:"readyReplicas"`
}

func init() {
	SchemeBuilder.Register(&LLMBatchGateway{}, &LLMBatchGatewayList{})
}
