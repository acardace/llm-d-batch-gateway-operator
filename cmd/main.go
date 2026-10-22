package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	"github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/controller"
	"github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/monitoring"
	tlsinit "github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/tls"
	"github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/utils"
)

// version is stamped at build time via -ldflags "-X main.version=<version>".
// It defaults to "dev" when built without the flag (e.g. go run).
var version = "dev"

// the way for batch-gateway-operator to know which exactly are the 4 component images(disgest) are via env variable set in the deployment
// since it cannot read params.env which is updated by opendatahub-operator
const (
	envImageAPIServer = "LLM_D_BATCH_GATEWAY_APISERVER_IMAGE"
	envImageProcessor = "LLM_D_BATCH_GATEWAY_PROCESSOR_IMAGE"
	envImageGC        = "LLM_D_BATCH_GATEWAY_GC_IMAGE"
	envImageAsync     = "LLM_D_ASYNC_IMAGE"
)

// componentImagesFromEnv reads the pinned component images from the environment
// and fails if any are missing, since the operator cannot render workloads
// without them.
func componentImagesFromEnv() (controller.ComponentImages, error) {
	required := []string{
		envImageAPIServer,
		envImageProcessor,
		envImageGC,
		envImageAsync,
	}
	var missing []string
	for _, e := range required {
		if os.Getenv(e) == "" {
			missing = append(missing, e)
		}
	}
	if len(missing) > 0 {
		return controller.ComponentImages{}, fmt.Errorf("required image environment variables are not set: %s", strings.Join(missing, ", "))
	}

	return controller.ComponentImages{
		APIServer: os.Getenv(envImageAPIServer),
		Processor: os.Getenv(envImageProcessor),
		GC:        os.Getenv(envImageGC),
		Async:     os.Getenv(envImageAsync),
	}, nil
}

// +kubebuilder:rbac:groups="",resources=services,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors;prometheusrules,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups=config.openshift.io,resources=apiservers,verbs=get;list;watch

var (
	scheme                  = runtime.NewScheme()
	syncPeriodDefault       = 5 * time.Minute
	reconcileTimeoutDefault = 30 * time.Second
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(batchv1alpha1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1.Install(scheme))
	utilruntime.Must(gatewayv1beta1.Install(scheme))
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	utilruntime.Must(configv1.Install(scheme))
}

func main() {
	var batchGatewayChartPath string
	var asyncChartPath string
	var metricsAddr string
	var metricsSecure bool
	var metricsCertPath string
	var probeAddr string
	var enableLeaderElection bool
	var syncPeriod time.Duration
	var reconcileTimeout time.Duration

	flag.StringVar(&batchGatewayChartPath, "batch-gateway-chart-path", "/charts/batch-gateway", "Path to the batch-gateway Helm chart directory")
	flag.StringVar(&asyncChartPath, "async-chart-path", "/charts/llm-d-async", "Path to the llm-d-async Helm chart directory")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "Address the metrics endpoint binds to")
	flag.BoolVar(&metricsSecure, "metrics-secure", true, "Serve metrics via HTTPS with authn/authz. Use --metrics-secure=false for HTTP on xKS.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "", "Directory containing the metrics server TLS cert and key. Empty uses in-memory certificates when --metrics-secure=true.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the health probe endpoint binds to")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager")
	flag.DurationVar(&syncPeriod, "sync-period", syncPeriodDefault, "How often to re-sync all LLMBatchGateway resources to catch out-of-band drift")
	flag.DurationVar(&reconcileTimeout, "reconcile-timeout", reconcileTimeoutDefault, "Maximum duration for a single reconcile")

	klog.InitFlags(nil)
	flag.Parse()

	logger := klog.NewKlogr()
	ctrl.SetLogger(logger)

	restConfig := ctrl.GetConfigOrDie()
	bootstrapClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		logger.Error(err, "unable to create bootstrap client")
		os.Exit(1)
	}

	tlsResult, err := tlsinit.Resolve(context.Background(), bootstrapClient, logger)
	if err != nil {
		logger.Error(err, "unable to resolve cluster TLS profile")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions(metricsAddr, metricsSecure, metricsCertPath, tlsResult.TLSOpts),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       utils.LeaderElectionID,
	})
	if err != nil {
		logger.Error(err, "unable to create manager")
		os.Exit(1)
	}

	images, err := componentImagesFromEnv()
	if err != nil {
		logger.Error(err, "unable to resolve 3 component images from env variables")
		os.Exit(1)
	}

	helmTLSProfile, unsupportedCiphers, err := tlsinit.ProfileValuesFromSpec(tlsResult.Profile)
	if err != nil {
		logger.Error(err, "unable to convert cluster TLS profile for Batch Gateway")
		os.Exit(1)
	}
	if len(unsupportedCiphers) > 0 {
		logger.Info("omitting TLS cipher suites unsupported by Batch Gateway", "unsupported", unsupportedCiphers)
	}

	batchGWHelmRenderer, err := controller.NewHelmRenderer(batchGatewayChartPath, images, helmTLSProfile)
	if err != nil {
		logger.Error(err, "unable to create batch-gateway helm renderer", "batchGatewayChartPath", batchGatewayChartPath)
		os.Exit(1)
	}

	asyncHelmRenderer, err := controller.NewHelmRenderer(asyncChartPath, images, helmTLSProfile)
	if err != nil {
		logger.Error(err, "unable to create async helm renderer", "asyncChartPath", asyncChartPath)
		os.Exit(1)
	}

	recorder := mgr.GetEventRecorderFor("llmbatchgateway-controller") //nolint:staticcheck

	if err := controller.NewLLMBatchGatewayReconciler(
		mgr.GetClient(), mgr.GetScheme(),
		batchGWHelmRenderer, asyncHelmRenderer,
		recorder, syncPeriod, reconcileTimeout,
	).SetupWithManager(mgr); err != nil {
		logger.Error(err, "unable to create controller", "controller", "LLMBatchGateway")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	operatorNamespace := os.Getenv("POD_NAMESPACE")
	if operatorNamespace != "" {
		metricsController := &monitoring.MetricsController{
			Client:    mgr.GetClient(),
			Namespace: operatorNamespace,
			Recorder:  recorder,
		}
		if err := metricsController.SetupWithManager(mgr); err != nil {
			logger.Error(err, "unable to create controller", "controller", "MetricsController")
			os.Exit(1)
		}
	} else {
		logger.Info("POD_NAMESPACE not set, skipping metrics controller reconciliation")
	}

	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())

	if err := tlsinit.SetupWatcher(mgr, tlsResult, cancel, logger); err != nil {
		cancel()
		logger.Error(err, "unable to set up TLS profile watcher")
		os.Exit(1)
	}

	logger.Info("starting manager", "version", version)
	if err := mgr.Start(ctx); err != nil {
		cancel()
		logger.Error(err, "problem running manager")
		os.Exit(1)
	}
	cancel()
}
