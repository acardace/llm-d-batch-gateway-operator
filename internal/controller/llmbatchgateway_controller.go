package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/json"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	"github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/asyncprocessor"
)

const (
	conditionReady                   = "Ready"
	conditionAPIServerAvailable      = "APIServerAvailable"
	conditionProcessorAvailable      = "ProcessorAvailable"
	conditionGCAvailable             = "GCAvailable"
	conditionAsyncProcessorAvailable = "AsyncProcessorAvailable"

	fieldOwner = "llmbatchgateway-controller"

	conditionsStatusField         = "conditions"
	componentStatusField          = "componentStatus"
	observedGenerationStatusField = "observedGeneration"
)

// managedGVKs must be a superset of all GVK types the Helm chart can produce.
// Update this list when adding new resource types to the chart.
var managedGVKs = []schema.GroupVersionKind{
	{Group: "apps", Version: "v1", Kind: "Deployment"},
	{Group: "", Version: "v1", Kind: "Service"},
	{Group: "", Version: "v1", Kind: "ConfigMap"},
	{Group: "", Version: "v1", Kind: "ServiceAccount"},
	{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"},
	{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"},
	{Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor"},
	{Group: "monitoring.coreos.com", Version: "v1", Kind: "PodMonitor"},
	{Group: "monitoring.coreos.com", Version: "v1", Kind: "PrometheusRule"},
}

type resourceKey struct {
	Group string
	Kind  string
	Name  string
}

// +kubebuilder:rbac:groups=batch.llm-d.ai,resources=llmbatchgateways,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch.llm-d.ai,resources=llmbatchgateways/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.llm-d.ai,resources=llmbatchgateways/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=referencegrants,verbs=get;list;watch
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors;podmonitors;prometheusrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

type LLMBatchGatewayReconciler struct {
	client.Client
	Scheme                     *runtime.Scheme
	HelmRenderer               *HelmRenderer
	Recorder                   record.EventRecorder
	ReconcileTimeout           time.Duration
	SyncPeriod                 time.Duration
	DefaultAsyncProcessorImage string
	secretFilter               *secretWatchFilter
}

func NewLLMBatchGatewayReconciler(c client.Client, scheme *runtime.Scheme, helm *HelmRenderer, recorder record.EventRecorder, syncPeriod time.Duration, reconcileTimeout time.Duration, defaultAsyncProcessorImage string) *LLMBatchGatewayReconciler {
	return &LLMBatchGatewayReconciler{
		Client:                     c,
		Scheme:                     scheme,
		HelmRenderer:               helm,
		Recorder:                   recorder,
		ReconcileTimeout:           reconcileTimeout,
		SyncPeriod:                 syncPeriod,
		DefaultAsyncProcessorImage: defaultAsyncProcessorImage,
		secretFilter:               &secretWatchFilter{watched: make(map[string]struct{})},
	}
}

func (r *LLMBatchGatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, r.ReconcileTimeout)
	defer cancel()

	logger := log.FromContext(ctx)

	var gw batchv1alpha1.LLMBatchGateway
	if err := r.Get(ctx, req.NamespacedName, &gw); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetching LLMBatchGateway: %w", err)
	}

	if err := validateSpec(&gw); err != nil {
		for _, condType := range []string{conditionReady, conditionAPIServerAvailable, conditionProcessorAvailable} {
			meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
				Type:               condType,
				Status:             metav1.ConditionFalse,
				Reason:             "ValidationFailed",
				Message:            err.Error(),
				ObservedGeneration: gw.Generation,
			})
		}
		r.Recorder.Eventf(&gw, corev1.EventTypeWarning, "ValidationFailed", "Spec validation failed: %s", err)
		if statusErr := NewStatusPatch(gw.ResourceVersion).
			Add(conditionsStatusField, gw.Status.Conditions).
			Apply(ctx, r.Client, &gw); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("patching status after validation failure: %w", statusErr)
		}
		return ctrl.Result{}, nil
	}

	// Resolve the credentials Secret, copying it into gw.Namespace when it
	// lives in a different namespace (requires a permitting ReferenceGrant).
	localSecretName, err := r.resolveSecret(ctx, &gw)
	if err != nil {
		var refErr *ReferenceNotPermittedError
		var immutableErr *SecretRefImmutableError
		reason, permanent := "", false
		if errors.As(err, &refErr) {
			reason, permanent = "ReferenceNotPermitted", true
		} else if errors.As(err, &immutableErr) {
			reason, permanent = "SecretRefImmutable", true
		}
		if permanent {
			meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
				Type:               conditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             reason,
				Message:            err.Error(),
				ObservedGeneration: gw.Generation,
			})
			r.Recorder.Eventf(&gw, corev1.EventTypeWarning, reason, "%s", err)
			if statusErr := NewStatusPatch(gw.ResourceVersion).
				Add(conditionsStatusField, gw.Status.Conditions).
				Apply(ctx, r.Client, &gw); statusErr != nil {
				return ctrl.Result{}, fmt.Errorf("patching status after %s: %w", reason, statusErr)
			}
			// Permanent error — no requeue. The user must act (create a
			// ReferenceGrant, or delete+recreate the CR for SecretRefImmutable).
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("resolving secret: %w", err)
	}

	// Track the source secret in the filter so the cluster-wide Secret watch
	// only triggers reconciles for secrets this controller actually cares about.
	// Only cross-namespace refs are tracked; same-namespace refs are not watched.
	ref := gw.Spec.SecretRef
	if ref.Namespace != "" && ref.Namespace != gw.Namespace {
		r.secretFilter.add(ref.Namespace, ref.Name)
	}

	objects, err := r.HelmRenderer.RenderChart(&gw, localSecretName)
	if err != nil {
		meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "RenderFailed",
			Message:            err.Error(),
			ObservedGeneration: gw.Generation,
		})
		r.Recorder.Eventf(&gw, corev1.EventTypeWarning, "RenderFailed", "Helm chart render failed: %s", err)
		if statusErr := NewStatusPatch(gw.ResourceVersion).
			Add(conditionsStatusField, gw.Status.Conditions).
			Apply(ctx, r.Client, &gw); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("rendering chart: %w; also failed to patch status: %w", err, statusErr)
		}
		return ctrl.Result{}, fmt.Errorf("rendering chart: %w", err)
	}

	if gw.Spec.AsyncProcessor != nil && gw.Spec.AsyncProcessor.Image == "" {
		gw.Spec.AsyncProcessor.Image = r.DefaultAsyncProcessorImage
	}

	asyncObjects, err := asyncprocessor.Build(&gw, localSecretName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("building async processor resources: %w", err)
	}
	objects = append(objects, asyncObjects...)

	for _, obj := range objects {
		obj.SetNamespace(gw.Namespace)

		if err := controllerutil.SetControllerReference(&gw, obj, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting owner reference on %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}

		if err := serverSideApply(ctx, r.Client, obj, fieldOwner); err != nil {
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				logger.V(1).Info("skipping resource (CRD not installed)", "kind", obj.GetKind(), "name", obj.GetName())
				continue
			}
			r.Recorder.Eventf(&gw, corev1.EventTypeWarning, "ApplyFailed", "Failed to apply %s/%s: %s", obj.GetKind(), obj.GetName(), err)
			return ctrl.Result{}, fmt.Errorf("applying %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
		logger.V(2).Info("applied resource", "kind", obj.GetKind(), "name", obj.GetName())
	}

	if err := r.deleteOrphanedResources(ctx, &gw, objects); err != nil {
		return ctrl.Result{}, fmt.Errorf("deleting orphaned resources: %w", err)
	}

	if err := r.updateStatus(ctx, &gw); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{RequeueAfter: r.SyncPeriod}, nil
}

func (r *LLMBatchGatewayReconciler) deleteOrphanedResources(
	ctx context.Context,
	gw *batchv1alpha1.LLMBatchGateway,
	renderedObjects []*unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx)

	desired := make(map[resourceKey]struct{}, len(renderedObjects))
	for _, obj := range renderedObjects {
		gvk := obj.GroupVersionKind()
		desired[resourceKey{
			Group: gvk.Group,
			Kind:  gvk.Kind,
			Name:  obj.GetName(),
		}] = struct{}{}
	}

	var errs []error
	for _, gvk := range managedGVKs {
		var list unstructured.UnstructuredList
		list.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind + "List",
		})

		// TODO: use client.MatchingLabels for GVKs that have consistent labels to reduce API server load
		if err := r.List(ctx, &list, client.InNamespace(gw.Namespace)); err != nil {
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				logger.V(1).Info("skipping orphan check (CRD not installed)", "kind", gvk.Kind)
				continue
			}
			errs = append(errs, fmt.Errorf("listing %s: %w", gvk.Kind, err))
			continue
		}

		for i := range list.Items {
			item := &list.Items[i]
			if !isControllerOwnedBy(item, gw) {
				continue
			}

			key := resourceKey{
				Group: gvk.Group,
				Kind:  gvk.Kind,
				Name:  item.GetName(),
			}
			if _, ok := desired[key]; ok {
				continue
			}

			logger.Info("deleting orphaned resource", "kind", gvk.Kind, "name", item.GetName())
			if err := r.Delete(ctx, item); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				errs = append(errs, fmt.Errorf("deleting %s/%s: %w", gvk.Kind, item.GetName(), err))
			}
		}
	}

	return errors.Join(errs...)
}

func (r *LLMBatchGatewayReconciler) updateStatus(ctx context.Context, gw *batchv1alpha1.LLMBatchGateway) error {
	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(gw.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": gw.Name,
	}); err != nil {
		return fmt.Errorf("listing deployments: %w", err)
	}

	componentStatus := &batchv1alpha1.ComponentStatus{}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		if !isOwnedBy(d, gw) {
			continue
		}

		component, ok := d.Labels["app.kubernetes.io/component"]
		if !ok {
			continue
		}

		status := &batchv1alpha1.ComponentReplicaStatus{
			Replicas:      d.Status.Replicas,
			ReadyReplicas: d.Status.ReadyReplicas,
		}

		switch component {
		case "apiserver":
			componentStatus.APIServer = status
		case "processor":
			componentStatus.Processor = status
		case "gc":
			componentStatus.GC = status
		case "asyncprocessor":
			componentStatus.AsyncProcessor = status
		}
	}

	gw.Status.ComponentStatus = componentStatus
	gw.Status.ObservedGeneration = gw.Generation

	apiAvailable := componentStatus.APIServer != nil && componentStatus.APIServer.ReadyReplicas >= 1
	meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
		Type:               conditionAPIServerAvailable,
		Status:             conditionStatus(apiAvailable),
		Reason:             conditionReason(apiAvailable, "Available", "Unavailable"),
		Message:            conditionMessage(apiAvailable, "API server has at least one ready replica", "API server has no ready replicas"),
		ObservedGeneration: gw.Generation,
	})

	procAvailable := componentStatus.Processor != nil && componentStatus.Processor.ReadyReplicas >= 1
	meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
		Type:               conditionProcessorAvailable,
		Status:             conditionStatus(procAvailable),
		Reason:             conditionReason(procAvailable, "Available", "Unavailable"),
		Message:            conditionMessage(procAvailable, "Processor has at least one ready replica", "Processor has no ready replicas"),
		ObservedGeneration: gw.Generation,
	})

	gcAvailable := componentStatus.GC != nil && componentStatus.GC.ReadyReplicas >= 1
	meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
		Type:               conditionGCAvailable,
		Status:             conditionStatus(gcAvailable),
		Reason:             conditionReason(gcAvailable, "Available", "Unavailable"),
		Message:            conditionMessage(gcAvailable, "GC has at least one ready replica", "GC has no ready replicas"),
		ObservedGeneration: gw.Generation,
	})

	asyncEnabled := gw.Spec.AsyncProcessor != nil
	asyncAvailable := componentStatus.AsyncProcessor != nil && componentStatus.AsyncProcessor.ReadyReplicas >= 1
	if asyncEnabled {
		meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
			Type:               conditionAsyncProcessorAvailable,
			Status:             conditionStatus(asyncAvailable),
			Reason:             conditionReason(asyncAvailable, "Available", "Unavailable"),
			Message:            conditionMessage(asyncAvailable, "Async processor has at least one ready replica", "Async processor has no ready replicas"),
			ObservedGeneration: gw.Generation,
		})
	}

	ready := apiAvailable && procAvailable && gcAvailable && (!asyncEnabled || asyncAvailable)

	// Snapshot the previous Ready condition before overwriting it so we can
	// detect transitions and emit an event only when the state changes.
	var prevReadyStatus metav1.ConditionStatus
	prevReady := meta.FindStatusCondition(gw.Status.Conditions, conditionReady)
	if prevReady != nil {
		prevReadyStatus = prevReady.Status
	}

	meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             conditionStatus(ready),
		Reason:             conditionReason(ready, "AllComponentsReady", "ComponentsNotReady"),
		Message:            conditionMessage(ready, "All components have at least one ready replica", "One or more components have no ready replicas"),
		ObservedGeneration: gw.Generation,
	})

	newReadyStatus := conditionStatus(ready)
	if prevReadyStatus != newReadyStatus {
		if ready {
			r.Recorder.Event(gw, corev1.EventTypeNormal, "Ready", "All components have at least one ready replica")
		} else {
			r.Recorder.Event(gw, corev1.EventTypeWarning, "NotReady", "One or more components have no ready replicas")
		}
	}

	return NewStatusPatch(gw.ResourceVersion).
		Add(conditionsStatusField, gw.Status.Conditions).
		Add(componentStatusField, gw.Status.ComponentStatus).
		Add(observedGenerationStatusField, gw.Status.ObservedGeneration).
		Apply(ctx, r.Client, gw)
}

func (r *LLMBatchGatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// enqueueForSecret maps a Secret event to the LLMBatchGateway CRs whose
	// cross-namespace secretRef points at it. The secretWatchFilter predicate
	// ensures this map func is only called for secrets we actually track, so the
	// list is already narrow by the time we get here.
	enqueueForSecret := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			var gwList batchv1alpha1.LLMBatchGatewayList
			if err := r.List(ctx, &gwList); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for _, gw := range gwList.Items {
				ref := gw.Spec.SecretRef

				if ref.Namespace == obj.GetNamespace() && ref.Name == obj.GetName() {
					reqs = append(reqs, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      gw.Name,
							Namespace: gw.Namespace,
						},
					})
				}
			}
			return reqs
		},
	)

	// enqueueForReferenceGrant maps a ReferenceGrant change to all
	// LLMBatchGateway CRs whose secretRef.namespace matches the grant's namespace.
	enqueueForReferenceGrant := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			var gwList batchv1alpha1.LLMBatchGatewayList
			if err := r.List(ctx, &gwList); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for _, gw := range gwList.Items {
				refNS := gw.Spec.SecretRef.Namespace

				if refNS == obj.GetNamespace() {
					reqs = append(reqs, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      gw.Name,
							Namespace: gw.Namespace,
						},
					})
				}
			}
			return reqs
		},
	)

	c := ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.LLMBatchGateway{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Watches(&corev1.Secret{}, enqueueForSecret, builder.WithPredicates(r.secretFilter))

	mapper := mgr.GetRESTMapper()

	optionalOwns := []client.Object{
		&gatewayv1beta1.HTTPRoute{},
		&certmanagerv1.Certificate{},
		&monitoringv1.ServiceMonitor{},
		&monitoringv1.PodMonitor{},
		&monitoringv1.PrometheusRule{},
	}
	for _, obj := range optionalOwns {
		if r.isCRDInstalled(mapper, obj) {
			c = c.Owns(obj)
		}
	}

	if r.isCRDInstalled(mapper, &gatewayv1beta1.ReferenceGrant{}) {
		c = c.Watches(&gatewayv1beta1.ReferenceGrant{}, enqueueForReferenceGrant)
	}

	return c.Complete(r)
}

// isCRDInstalled reports whether the CRD for obj is registered in the cluster.
// Any mapper error is treated as "not installed" so that optional watches are
// skipped rather than preventing the operator from starting.
func (r *LLMBatchGatewayReconciler) isCRDInstalled(mapper meta.RESTMapper, obj client.Object) bool {
	gvks, _, err := r.Scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return false
	}
	_, err = mapper.RESTMapping(gvks[0].GroupKind(), gvks[0].Version)
	return err == nil
}

func isControllerOwnedBy(obj metav1.Object, owner *batchv1alpha1.LLMBatchGateway) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == owner.UID &&
			ref.Kind == "LLMBatchGateway" &&
			ref.APIVersion == batchv1alpha1.GroupVersion.String() &&
			ref.Controller != nil &&
			*ref.Controller {
			return true
		}
	}
	return false
}

func isOwnedBy(obj metav1.Object, owner *batchv1alpha1.LLMBatchGateway) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == owner.UID {
			return true
		}
	}
	return false
}

func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func conditionReason(ok bool, trueReason, falseReason string) string {
	if ok {
		return trueReason
	}
	return falseReason
}

func validateSpec(gw *batchv1alpha1.LLMBatchGateway) error {
	hasGlobal := gw.Spec.Processor.GlobalInferenceGateway != nil
	hasModel := len(gw.Spec.Processor.ModelGateways) > 0
	if !hasGlobal && !hasModel {
		return errors.New("processor must have either globalInferenceGateway or modelGateways configured")
	}
	if hasGlobal && hasModel {
		return errors.New("processor cannot have both globalInferenceGateway and modelGateways configured")
	}
	return nil
}

func conditionMessage(ok bool, trueMsg, falseMsg string) string {
	if ok {
		return trueMsg
	}
	return falseMsg
}

// serverSideApply marshals obj to JSON and applies it via server-side apply.
func serverSideApply(ctx context.Context, c client.Client, obj client.Object, fieldOwner string) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshalling: %w", err)
	}
	return c.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, data), client.FieldOwner(fieldOwner), client.ForceOwnership)
}

var _ reconcile.Reconciler = (*LLMBatchGatewayReconciler)(nil)
