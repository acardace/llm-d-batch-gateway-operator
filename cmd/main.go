package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	"github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/controller"
)

// version is stamped at build time via -ldflags "-X main.version=<version>".
// It defaults to "dev" when built without the flag (e.g. go run).
var version = "dev"

// defaultAsyncProcessorImage is stamped at build time via -ldflags.
var defaultAsyncProcessorImage = "ghcr.io/llm-d-incubation/llm-d-async:latest"

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
}

func main() {
	var chartPath string
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var syncPeriod time.Duration
	var reconcileTimeout time.Duration

	flag.StringVar(&chartPath, "chart-path", "/charts/batch-gateway", "Path to the batch-gateway Helm chart directory")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the metrics endpoint binds to")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the health probe endpoint binds to")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager")
	flag.DurationVar(&syncPeriod, "sync-period", syncPeriodDefault, "How often to re-sync all LLMBatchGateway resources to catch out-of-band drift")
	flag.DurationVar(&reconcileTimeout, "reconcile-timeout", reconcileTimeoutDefault, "Maximum duration for a single reconcile")

	klog.InitFlags(nil)
	flag.Parse()

	logger := klog.NewKlogr()
	ctrl.SetLogger(logger)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "llmbatchgateway.batch.llm-d.ai",
	})
	if err != nil {
		logger.Error(err, "unable to create manager")
		os.Exit(1)
	}

	helmRenderer, err := controller.NewHelmRenderer(chartPath)
	if err != nil {
		logger.Error(err, "unable to create helm renderer", "chartPath", chartPath)
		os.Exit(1)
	}

	if err := controller.NewLLMBatchGatewayReconciler(mgr.GetClient(), mgr.GetScheme(), helmRenderer, mgr.GetEventRecorderFor("llmbatchgateway-controller"), syncPeriod, reconcileTimeout, defaultAsyncProcessorImage).SetupWithManager(mgr); err != nil { //nolint:staticcheck
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

	logger.Info("starting manager", "version", version)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "problem running manager")
		os.Exit(1)
	}
}
