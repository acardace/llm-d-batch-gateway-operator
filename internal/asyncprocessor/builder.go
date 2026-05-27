package asyncprocessor

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
)

const (
	component       = "asyncprocessor"
	nameFormat      = "%s-batch-gateway-asyncprocessor"
	redisSecretKey  = "redis-url"
	metricsPort     = int32(9090)
	configMountPath = "/etc/async-processor"
	configFileName  = "queues.json"
)

// Build constructs the Kubernetes objects needed to run the async processor
// for the given LLMBatchGateway CR. Returns nil if spec.asyncProcessor is nil.
// secretName is the name of the resolved credentials secret in gw.Namespace.
func Build(gw *batchv1alpha1.LLMBatchGateway, secretName string) ([]*unstructured.Unstructured, error) {
	if gw.Spec.AsyncProcessor == nil {
		return nil, nil
	}

	spec := gw.Spec.AsyncProcessor
	name := fmt.Sprintf(nameFormat, gw.Name)
	labels := map[string]string{
		"app.kubernetes.io/name":       "batch-gateway-" + component,
		"app.kubernetes.io/instance":   gw.Name,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/managed-by": "llmbatchgateway-controller",
	}

	var objects []*unstructured.Unstructured

	sa, err := toUnstructured(buildServiceAccount(name, gw.Namespace, labels))
	if err != nil {
		return nil, fmt.Errorf("building ServiceAccount: %w", err)
	}
	objects = append(objects, sa)

	multiQueue := len(spec.Queues) > 0
	if multiQueue {
		cm, err := buildQueuesConfigMap(name, gw.Namespace, labels, spec.Queues)
		if err != nil {
			return nil, fmt.Errorf("building queues ConfigMap: %w", err)
		}
		u, err := toUnstructured(cm)
		if err != nil {
			return nil, fmt.Errorf("converting queues ConfigMap to unstructured: %w", err)
		}
		objects = append(objects, u)
	}

	igwURL := ""
	if !multiQueue && gw.Spec.Processor.GlobalInferenceGateway != nil {
		igwURL = gw.Spec.Processor.GlobalInferenceGateway.URL
	}

	deploy, err := toUnstructured(buildDeployment(name, gw.Namespace, labels, spec, secretName, igwURL, multiQueue))
	if err != nil {
		return nil, fmt.Errorf("building Deployment: %w", err)
	}
	objects = append(objects, deploy)

	return objects, nil
}

func buildServiceAccount(name, namespace string, labels map[string]string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ServiceAccount",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}
}

// queueConfig mirrors the JSON structure expected by --redis.ss.queues-config-file.
type queueConfig struct {
	ID                 string            `json:"id"`
	QueueName          string            `json:"queue_name,omitempty"`
	ResultQueueName    string            `json:"result_queue_name,omitempty"`
	IGWBaseURL         string            `json:"igw_base_url"`
	RequestPathURL     string            `json:"request_path_url,omitempty"`
	InferenceObjective string            `json:"inference_objective,omitempty"`
	GateType           string            `json:"gate_type,omitempty"`
	GateParams         map[string]string `json:"gate_params,omitempty"`
}

func buildQueuesConfigMap(name, namespace string, labels map[string]string, queues map[string]batchv1alpha1.AsyncProcessorQueueSpec) (*corev1.ConfigMap, error) {
	configs := make([]queueConfig, 0, len(queues))
	for id, q := range queues {
		configs = append(configs, queueConfig{
			ID:                 id,
			QueueName:          q.QueueName,
			ResultQueueName:    q.ResultQueueName,
			IGWBaseURL:         q.InferenceGatewayURL,
			RequestPathURL:     q.RequestPathURL,
			InferenceObjective: q.InferenceObjective,
			GateType:           q.GateType,
			GateParams:         q.GateParams,
		})
	}

	data, err := json.Marshal(configs)
	if err != nil {
		return nil, fmt.Errorf("marshalling queues config: %w", err)
	}

	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-queues",
			Namespace: namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			configFileName: string(data),
		},
	}, nil
}

func buildDeployment(name, namespace string, labels map[string]string, spec *batchv1alpha1.AsyncProcessorSpec, secretName, igwURL string, multiQueue bool) *appsv1.Deployment {
	selectorLabels := map[string]string{
		"app.kubernetes.io/name":      labels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance":  labels["app.kubernetes.io/instance"],
		"app.kubernetes.io/component": labels["app.kubernetes.io/component"],
	}

	replicas := int32(1)
	if spec.Replicas != nil {
		replicas = *spec.Replicas
	}

	gracePeriod := int64(130)

	env := []corev1.EnvVar{
		{
			Name: "REDIS_URL",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  redisSecretKey,
				},
			},
		},
	}

	var volumeMounts []corev1.VolumeMount
	var volumes []corev1.Volume
	if multiQueue {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "queues-config",
			MountPath: configMountPath,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "queues-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name + "-queues"},
				},
			},
		})
	}

	container := corev1.Container{
		Name:            component,
		Image:           spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            buildArgs(spec, igwURL, multiQueue),
		Env:             env,
		VolumeMounts:    volumeMounts,
		Ports: []corev1.ContainerPort{
			{Name: "metrics", ContainerPort: metricsPort, Protocol: corev1.ProtocolTCP},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	if spec.Resources != nil {
		container.Resources = *spec.Resources
	}

	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:            name,
					TerminationGracePeriodSeconds: &gracePeriod,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
		},
	}
}

func buildArgs(spec *batchv1alpha1.AsyncProcessorSpec, igwURL string, multiQueue bool) []string {
	args := []string{
		"--message-queue-impl=redis-sortedset",
		fmt.Sprintf("--concurrency=%d", spec.Concurrency),
		fmt.Sprintf("--request-timeout=%s", spec.RequestTimeout),
		fmt.Sprintf("--redis.ss.poll-interval-ms=%d", spec.PollIntervalMs),
		fmt.Sprintf("--redis.ss.batch-size=%d", spec.BatchSize),
		fmt.Sprintf("--metrics-port=%d", metricsPort),
		"--metrics-endpoint-auth=false",
	}

	if multiQueue {
		args = append(args, fmt.Sprintf("--redis.ss.queues-config-file=%s/%s", configMountPath, configFileName))
	} else {
		args = append(args, fmt.Sprintf("--redis.ss.igw-base-url=%s", igwURL))
	}

	if spec.Prometheus != nil {
		args = append(args,
			fmt.Sprintf("--prometheus-url=%s", spec.Prometheus.URL),
			fmt.Sprintf("--prometheus-cache-ttl=%s", spec.Prometheus.CacheTTL),
		)
	}

	return args
}

func toUnstructured(obj any) (*unstructured.Unstructured, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshalling object: %w", err)
	}
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(data); err != nil {
		return nil, fmt.Errorf("unmarshalling to unstructured: %w", err)
	}
	return u, nil
}
