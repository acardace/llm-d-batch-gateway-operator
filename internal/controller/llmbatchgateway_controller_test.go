package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	batchv1alpha1 "github.com/opendatahub-io/llm-d-batch-gateway-operator/api/v1alpha1"
	tlspkg "github.com/opendatahub-io/llm-d-batch-gateway-operator/internal/tls"
)

const (
	llmBatchGatewayKind = "LLMBatchGateway"
	secretKind          = "Secret"
)

func newTestGateway(name string) *batchv1alpha1.LLMBatchGateway {
	return &batchv1alpha1.LLMBatchGateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: batchv1alpha1.LLMBatchGatewaySpec{
			SecretRef: corev1.SecretReference{Name: "test-secrets"},
			DBBackend: "postgresql",
			FileStorage: &batchv1alpha1.FileStorageSpec{
				S3: &batchv1alpha1.S3StorageSpec{
					Region:   "us-east-1",
					Bucket:   "llm-d-batch-gateway",
					Endpoint: "https://s3.example.com",
				},
			},
			APIServer: batchv1alpha1.APIServerSpec{
				Replicas: ptr.To(int32(1)),
			},
			Processor: batchv1alpha1.ProcessorSpec{
				Replicas:     ptr.To(int32(1)),
				DispatchMode: "sync",
				GlobalInferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
					URL:            "http://inference-gw:8000",
					RequestTimeout: "5m",
				},
			},
			GC: batchv1alpha1.GCSpec{
				Interval: "30m",
			},
		},
	}
}

func TestValidateSpecRejectsQueueNamesOutsideAsyncMode(t *testing.T) {
	t.Run("globalInferenceGateway", func(t *testing.T) {
		gw := newTestGateway("test-sync-global-queues")
		gw.Spec.Processor.GlobalInferenceGateway.RequestQueueName = "requests"
		gw.Spec.Processor.GlobalInferenceGateway.ResultQueueName = "results"

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "processor.globalInferenceGateway.requestQueueName") {
			t.Fatalf("validateSpec() error = %v, want globalInferenceGateway queue name error", err)
		}
	})

	t.Run("modelGateways", func(t *testing.T) {
		gw := newTestGateway("test-sync-model-queues")
		gw.Spec.Processor.GlobalInferenceGateway = nil
		gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
			"model-a": {
				URL:              "http://model-a:8000",
				RequestQueueName: "requests",
				ResultQueueName:  "results",
			},
		}

		err := validateSpec(gw)
		if err == nil || !strings.Contains(err.Error(), "processor.modelGateways[model-a].requestQueueName") {
			t.Fatalf("validateSpec() error = %v, want modelGateways queue name error", err)
		}
	})
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	resyncTimeout := 5 * time.Minute
	reconcileTimeout := 30 * time.Second

	reconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, nil, fakeRecorder, resyncTimeout, reconcileTimeout)

	t.Run("defaults omitted dispatch mode to async", func(t *testing.T) {
		gw := newTestAsyncGateway("test-default-async")
		gw.Spec.Processor.DispatchMode = ""
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		var stored batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &stored); err != nil {
			t.Fatalf("getting defaulted CR: %v", err)
		}
		if stored.Spec.Processor.DispatchMode != dispatchModeAsync {
			t.Fatalf("dispatchMode = %q, want %q", stored.Spec.Processor.DispatchMode, dispatchModeAsync)
		}

		values := specToBatchHelmValues(&stored, testSecretName(&stored), testImages(), tlspkg.ProfileValues{})
		processor, ok := values["processor"].(map[string]any)
		if !ok {
			t.Fatal("rendered processor config missing")
		}
		config, ok := processor["config"].(map[string]any)
		if !ok {
			t.Fatal("rendered processor config section missing")
		}
		if got := config["dispatchMode"]; got != dispatchModeAsync {
			t.Errorf("rendered dispatchMode = %v, want %q", got, dispatchModeAsync)
		}
	})

	t.Run("returns RequeueAfter on successful reconcile", func(t *testing.T) {
		gw := newTestGateway("test-requeue")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}
		if result.RequeueAfter != resyncTimeout {
			t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, resyncTimeout)
		}
	})

	t.Run("creates all child resources", func(t *testing.T) {
		gw := newTestGateway("test-create")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		t.Run("deployments", func(t *testing.T) {
			var deployList appsv1.DeploymentList
			if err := k8sClient.List(ctx, &deployList); err != nil {
				t.Fatalf("listing deployments: %v", err)
			}
			deployCount := 0
			for _, d := range deployList.Items {
				if isOwnedByUID(d.OwnerReferences, gw.UID) {
					deployCount++
				}
			}
			if deployCount != 2 {
				t.Errorf("deployment count = %d, want 2", deployCount)
			}
		})

		t.Run("processor statefulset", func(t *testing.T) {
			var statefulSetList appsv1.StatefulSetList
			if err := k8sClient.List(ctx, &statefulSetList); err != nil {
				t.Fatalf("listing statefulsets: %v", err)
			}
			count := 0
			for _, ss := range statefulSetList.Items {
				if isOwnedByUID(ss.OwnerReferences, gw.UID) {
					count++
				}
			}
			if count != 1 {
				t.Errorf("statefulset count = %d, want 1", count)
			}
		})

		t.Run("configmaps", func(t *testing.T) {
			var cmList corev1.ConfigMapList
			if err := k8sClient.List(ctx, &cmList); err != nil {
				t.Fatalf("listing configmaps: %v", err)
			}
			cmCount := 0
			for _, cm := range cmList.Items {
				if isOwnedByUID(cm.OwnerReferences, gw.UID) {
					cmCount++
				}
			}
			if cmCount < 3 {
				t.Errorf("configmap count = %d, want >= 3", cmCount)
			}
		})

		t.Run("service accounts", func(t *testing.T) {
			var saList corev1.ServiceAccountList
			if err := k8sClient.List(ctx, &saList); err != nil {
				t.Fatalf("listing serviceaccounts: %v", err)
			}
			saCount := 0
			for _, sa := range saList.Items {
				if isOwnedByUID(sa.OwnerReferences, gw.UID) {
					saCount++
				}
			}
			if saCount != 3 {
				t.Errorf("serviceaccount count = %d, want 3", saCount)
			}
		})

		t.Run("service", func(t *testing.T) {
			var svcList corev1.ServiceList
			if err := k8sClient.List(ctx, &svcList); err != nil {
				t.Fatalf("listing services: %v", err)
			}
			svcCount := 0
			for _, svc := range svcList.Items {
				if isOwnedByUID(svc.OwnerReferences, gw.UID) {
					svcCount++
				}
			}
			if svcCount != 2 {
				t.Errorf("service count = %d, want 2", svcCount)
			}
		})
	})

	t.Run("sets owner references", func(t *testing.T) {
		gw := newTestGateway("test-owner")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		var deployList appsv1.DeploymentList
		if err := k8sClient.List(ctx, &deployList); err != nil {
			t.Fatalf("listing deployments: %v", err)
		}

		for _, d := range deployList.Items {
			if !isOwnedByUID(d.OwnerReferences, gw.UID) {
				continue
			}
			found := false
			for _, ref := range d.OwnerReferences {
				if ref.UID == gw.UID && ref.Kind == llmBatchGatewayKind && ref.Controller != nil && *ref.Controller {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("deployment %s missing controller owner reference", d.Name)
			}
		}
	})

	t.Run("sets status conditions", func(t *testing.T) {
		gw := newTestGateway("test-status")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		conditionTypes := map[string]bool{}
		for _, c := range updated.Status.Conditions {
			conditionTypes[c.Type] = true
		}

		for _, ct := range []string{conditionReady, conditionAPIServerAvailable, conditionProcessorAvailable, conditionGCAvailable} {
			if !conditionTypes[ct] {
				t.Errorf("missing condition %q", ct)
			}
		}

		if updated.Status.ObservedGeneration != updated.Generation {
			t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
		}
	})

	t.Run("updates on spec change", func(t *testing.T) {
		gw := newTestGateway("test-update")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.APIServer.Replicas = ptr.To(int32(3))
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		var deployList appsv1.DeploymentList
		if err := k8sClient.List(ctx, &deployList); err != nil {
			t.Fatalf("listing deployments: %v", err)
		}

		for _, d := range deployList.Items {
			if !isOwnedByUID(d.OwnerReferences, gw.UID) {
				continue
			}
			component := d.Labels[labelKeyComponent]
			if component == componentAPIServer {
				if d.Spec.Replicas == nil || *d.Spec.Replicas != 3 {
					replicas := int32(0)
					if d.Spec.Replicas != nil {
						replicas = *d.Spec.Replicas
					}
					t.Errorf("apiserver replicas = %d, want 3", replicas)
				}
			}
		}
	})

	t.Run("deletes orphaned resources on spec change", func(t *testing.T) {
		gw := newTestGateway("test-orphan")
		gw.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		var cmList corev1.ConfigMapList
		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps: %v", err)
		}
		cmCountBefore := 0
		hasDashboard := false
		for _, cm := range cmList.Items {
			if !isOwnedByUID(cm.OwnerReferences, gw.UID) {
				continue
			}
			cmCountBefore++
			if cm.Labels["grafana_dashboard"] == "1" {
				hasDashboard = true
			}
		}
		if !hasDashboard {
			t.Fatal("expected grafana dashboard ConfigMap to exist after first reconcile")
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: false}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps after: %v", err)
		}
		cmCountAfter := 0
		for _, cm := range cmList.Items {
			if !isOwnedByUID(cm.OwnerReferences, gw.UID) {
				continue
			}
			cmCountAfter++
			if cm.Labels["grafana_dashboard"] == "1" {
				t.Error("grafana dashboard ConfigMap should have been deleted")
			}
		}
		if cmCountAfter != cmCountBefore-1 {
			t.Errorf("configmap count = %d, want %d (one dashboard removed)", cmCountAfter, cmCountBefore-1)
		}
	})

	t.Run("sets ValidationFailed when no inference gateway configured", func(t *testing.T) {
		gw := newTestGateway("test-validation-none")
		gw.Spec.Processor.GlobalInferenceGateway = nil
		gw.Spec.Processor.ModelGateways = nil
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for validation failure, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		for _, condType := range []string{conditionReady, conditionAPIServerAvailable, conditionProcessorAvailable, conditionGCAvailable} {
			found := false
			for _, c := range updated.Status.Conditions {
				if c.Type == condType {
					found = true
					if c.Status != metav1.ConditionFalse {
						t.Errorf("%s status = %v, want False", condType, c.Status)
					}
					if c.Reason != "ValidationFailed" {
						t.Errorf("%s reason = %v, want ValidationFailed", condType, c.Reason)
					}
					break
				}
			}
			if !found {
				t.Errorf("missing condition %s", condType)
			}
		}
		if updated.Status.ObservedGeneration != updated.Generation {
			t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
		}

		assertWarningEvent(t, fakeRecorder, "ValidationFailed")
	})

	t.Run("sets ValidationFailed when both gateways configured", func(t *testing.T) {
		gw := newTestGateway("test-validation-both")
		gw.Spec.Processor.GlobalInferenceGateway = &batchv1alpha1.InferenceGatewaySpec{
			URL: "http://global:8000",
		}
		gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
			"model-a": {URL: "http://model-a:8000"},
		}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() {
			_ = k8sClient.Delete(ctx, gw)
		})

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for validation failure, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		for _, c := range updated.Status.Conditions {
			if c.Type == conditionReady {
				if c.Status != metav1.ConditionFalse {
					t.Errorf("Ready status = %v, want False", c.Status)
				}
				if c.Reason != "ValidationFailed" {
					t.Errorf("Ready reason = %v, want ValidationFailed", c.Reason)
				}
				if updated.Status.ObservedGeneration != updated.Generation {
					t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
				}
				return
			}
		}
		t.Error("missing Ready condition")
	})

	t.Run("sets ReferenceNotPermitted when no ReferenceGrant exists", func(t *testing.T) {
		gw := newTestGateway("test-refnotpermitted")
		// Cross-namespace secretRef — no ReferenceGrant is present.
		gw.Spec.SecretRef = corev1.SecretReference{Name: "src-secret", Namespace: "other-ns"}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for permanent condition, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}
		for _, c := range updated.Status.Conditions {
			if c.Type == conditionReady {
				if c.Status != metav1.ConditionFalse {
					t.Errorf("Ready status = %v, want False", c.Status)
				}
				if c.Reason != reasonReferenceNotPermitted {
					t.Errorf("Ready reason = %q, want %q", c.Reason, reasonReferenceNotPermitted)
				}
				if updated.Status.ObservedGeneration != updated.Generation {
					t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
				}
				assertWarningEvent(t, fakeRecorder, reasonReferenceNotPermitted)
				return
			}
		}
		t.Error("missing Ready condition")
	})

	t.Run("sets SecretRefImmutable when managed copy annotation mismatches", func(t *testing.T) {
		gwName := "test-secretrefimmutable"

		// Create the managed copy with an annotation pointing at a different source.
		existingCopy := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gwName + managedSecretSuffix,
				Namespace: "default",
				Annotations: map[string]string{
					"batch.llm-d.ai/copied-from": "other-ns/old-secret",
				},
			},
		}
		if err := k8sClient.Create(ctx, existingCopy); err != nil {
			t.Fatalf("creating pre-existing managed copy: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, existingCopy) })

		gw := newTestGateway(gwName)
		// Cross-namespace secretRef pointing at a different secret than the copy.
		gw.Spec.SecretRef = corev1.SecretReference{Name: "new-secret", Namespace: "other-ns"}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() should not return error for permanent condition, got: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}
		for _, c := range updated.Status.Conditions {
			if c.Type == conditionReady {
				if c.Status != metav1.ConditionFalse {
					t.Errorf("Ready status = %v, want False", c.Status)
				}
				if c.Reason != reasonSecretRefImmutable {
					t.Errorf("Ready reason = %q, want %q", c.Reason, reasonSecretRefImmutable)
				}
				if updated.Status.ObservedGeneration != updated.Generation {
					t.Errorf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
				}
				assertWarningEvent(t, fakeRecorder, reasonSecretRefImmutable)
				return
			}
		}
		t.Error("missing Ready condition")
	})

	t.Run("updates processor replicas on spec change", func(t *testing.T) {
		gw := newTestGateway("test-proc-replicas")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.Processor.Replicas = ptr.To(int32(5))
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		assertProcessorStatefulSetReplicas(ctx, t, gw, 5)
	})

	t.Run("updates apiserver resources on spec change", func(t *testing.T) {
		gw := newTestGateway("test-api-resources")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.APIServer.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		d := findOwnedDeployment(ctx, t, gw, "apiserver")
		containers := d.Spec.Template.Spec.Containers
		if len(containers) == 0 {
			t.Fatal("apiserver deployment has no containers")
		}
		res := containers[0].Resources
		assertResourceValue(t, "apiserver requests.cpu", res.Requests, corev1.ResourceCPU, "500m")
		assertResourceValue(t, "apiserver requests.memory", res.Requests, corev1.ResourceMemory, "256Mi")
		assertResourceValue(t, "apiserver limits.cpu", res.Limits, corev1.ResourceCPU, "1")
		assertResourceValue(t, "apiserver limits.memory", res.Limits, corev1.ResourceMemory, "512Mi")
	})

	t.Run("unsupported dbBackend is rejected", func(t *testing.T) {
		gw := newTestGateway("test-dbbackend")
		gw.Spec.DBBackend = "redis"
		if err := validateSpec(gw); err == nil || !strings.Contains(err.Error(), "only \"postgresql\" is supported") {
			t.Fatalf("validateSpec() error = %v, want PostgreSQL-only error", err)
		}
	})

	t.Run("apiserver config change updates ConfigMap and Deployment", func(t *testing.T) {
		gw := newTestGateway("test-api-config")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		apiBefore := findOwnedDeployment(ctx, t, gw, "apiserver")
		checksumBefore := apiBefore.Spec.Template.Annotations["checksum/config"]

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.APIServer.Config = &batchv1alpha1.APIServerConfigSpec{
			Port:                9090,
			ReadTimeoutSeconds:  120,
			WriteTimeoutSeconds: 180,
			EnablePprof:         true,
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "apiserver")
		assertConfigMapContains(t, cm, `port: "9090"`)
		assertConfigMapContains(t, cm, "read_timeout_seconds: 120")
		assertConfigMapContains(t, cm, "write_timeout_seconds: 180")
		assertConfigMapContains(t, cm, "enable_pprof: true")

		apiAfter := findOwnedDeployment(ctx, t, gw, "apiserver")
		checksumAfter := apiAfter.Spec.Template.Annotations["checksum/config"]
		if checksumAfter == checksumBefore {
			t.Error("apiserver deployment pod template not updated after config change")
		}

		// The Deployment must expose the new port on the container.
		found := false
		for _, p := range apiAfter.Spec.Template.Spec.Containers[0].Ports {
			if p.Name == "http" && p.ContainerPort == 9090 {
				found = true
			}
		}
		if !found {
			t.Error("apiserver deployment container port 'http' not updated to 9090")
		}
	})

	t.Run("processor config change updates ConfigMap and StatefulSet", func(t *testing.T) {
		gw := newTestGateway("test-proc-config")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		procBefore := findOwnedProcessorStatefulSet(ctx, t, gw)
		checksumBefore := procBefore.Spec.Template.Annotations["checksum/config"]

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{
			NumWorkers: 50,
			Concurrency: &batchv1alpha1.ConcurrencyConfig{
				Global:      200,
				PerEndpoint: 25,
			},
			DefaultOutputExpirationSeconds: 7200,
			EnablePprof:                    true,
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "processor")
		assertConfigMapContains(t, cm, "num_workers: 50")
		assertConfigMapContains(t, cm, "global: 200")
		assertConfigMapContains(t, cm, "per_endpoint: 25")
		assertConfigMapContains(t, cm, "default_output_expiration_seconds: 7200")
		assertConfigMapContains(t, cm, "enable_pprof: true")

		procAfter := findOwnedProcessorStatefulSet(ctx, t, gw)
		checksumAfter := procAfter.Spec.Template.Annotations["checksum/config"]
		if checksumAfter == checksumBefore {
			t.Error("processor statefulset pod template not updated after config change")
		}
	})

	t.Run("legacy processor.config.inferenceObjective lands on the gateway", func(t *testing.T) {
		gw := newTestGateway("test-legacy-objective")
		gw.Spec.Processor.Config = &batchv1alpha1.ProcessorConfigSpec{InferenceObjective: "batch-sheddable"}
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "processor")
		assertConfigMapContains(t, cm, `inference_objective: "batch-sheddable"`)
		assertWarningEvent(t, fakeRecorder, reasonDeprecatedField)
	})

	t.Run("gc config change updates ConfigMap and Deployment", func(t *testing.T) {
		gw := newTestGateway("test-gc-config")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		nn := types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("first Reconcile() error: %v", err)
		}

		gcBefore := findOwnedDeployment(ctx, t, gw, "gc")
		checksumBefore := gcBefore.Spec.Template.Annotations["checksum/config"]

		if err := k8sClient.Get(ctx, nn, gw); err != nil {
			t.Fatalf("getting CR for update: %v", err)
		}
		gw.Spec.GC.Interval = "1h"
		gw.Spec.GC.Config = &batchv1alpha1.GCConfigSpec{
			DryRun:         true,
			MaxConcurrency: 10,
			Reconciler: &batchv1alpha1.ReconcilerSpec{
				Enabled:  true,
				Interval: "90m",
			},
		}
		if err := k8sClient.Update(ctx, gw); err != nil {
			t.Fatalf("updating CR: %v", err)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		if err != nil {
			t.Fatalf("second Reconcile() error: %v", err)
		}

		cm := getOwnedConfigMap(ctx, t, gw, "gc")
		assertConfigMapContains(t, cm, `interval: "1h"`)
		assertConfigMapContains(t, cm, "dry_run: true")
		assertConfigMapContains(t, cm, "max_concurrency: 10")
		assertConfigMapContains(t, cm, "enabled: true")
		assertConfigMapContains(t, cm, `interval: "90m"`)

		gcAfter := findOwnedDeployment(ctx, t, gw, "gc")
		checksumAfter := gcAfter.Spec.Template.Annotations["checksum/config"]
		if checksumAfter == checksumBefore {
			t.Error("gc deployment pod template not updated after config change")
		}
	})

	t.Run("does not delete resources owned by a different CR", func(t *testing.T) {
		gwA := newTestGateway("test-orphan-a")
		gwA.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}
		gwB := newTestGateway("test-orphan-b")
		gwB.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: true}

		if err := k8sClient.Create(ctx, gwA); err != nil {
			t.Fatalf("creating CR A: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gwA) })
		if err := k8sClient.Create(ctx, gwB); err != nil {
			t.Fatalf("creating CR B: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gwB) })

		nnA := types.NamespacedName{Name: gwA.Name, Namespace: gwA.Namespace}
		nnB := types.NamespacedName{Name: gwB.Name, Namespace: gwB.Namespace}

		if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nnA}); err != nil {
			t.Fatalf("Reconcile A: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nnB}); err != nil {
			t.Fatalf("Reconcile B: %v", err)
		}

		var cmList corev1.ConfigMapList
		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps: %v", err)
		}
		cmCountB := 0
		for _, cm := range cmList.Items {
			if isOwnedByUID(cm.OwnerReferences, gwB.UID) {
				cmCountB++
			}
		}

		if err := k8sClient.Get(ctx, nnA, gwA); err != nil {
			t.Fatalf("getting CR A: %v", err)
		}
		gwA.Spec.Grafana = &batchv1alpha1.GrafanaSpec{Enabled: false}
		if err := k8sClient.Update(ctx, gwA); err != nil {
			t.Fatalf("updating CR A: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nnA}); err != nil {
			t.Fatalf("Reconcile A after update: %v", err)
		}

		if err := k8sClient.List(ctx, &cmList); err != nil {
			t.Fatalf("listing configmaps after: %v", err)
		}
		cmCountBAfter := 0
		for _, cm := range cmList.Items {
			if isOwnedByUID(cm.OwnerReferences, gwB.UID) {
				cmCountBAfter++
			}
		}
		if cmCountBAfter != cmCountB {
			t.Errorf("CR B configmap count changed from %d to %d", cmCountB, cmCountBAfter)
		}
	})
}

func newTestAsyncGateway(name string) *batchv1alpha1.LLMBatchGateway {
	gw := newTestGateway(name)
	concurrency := int32(8)
	gw.Spec.Processor.DispatchMode = dispatchModeAsync
	gw.Spec.Processor.GlobalInferenceGateway = nil
	gw.Spec.Processor.ModelGateways = map[string]batchv1alpha1.InferenceGatewaySpec{
		"sim-model": {InferencePoolName: "sim-pool"},
	}
	gw.Spec.Processor.AsyncConfig = &batchv1alpha1.AsyncProcessorSpec{
		Concurrency:       &concurrency,
		DrainTimeout:      "2m",
		ResultPollTimeout: "30s",
		InferenceGateway: &batchv1alpha1.InferenceGatewaySpec{
			URL: "http://epp:8081",
		},
		Redis: &batchv1alpha1.AsyncRedisSpec{},
	}
	return gw
}

func TestReconcileAsync(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer(batch) error: %v", err)
	}
	asyncHelmRenderer, err := NewHelmRenderer("../../llm-d-async/charts/async-processor", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer(async) error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	resyncTimeout := 5 * time.Minute
	reconcileTimeout := 30 * time.Second

	reconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, asyncHelmRenderer, fakeRecorder, resyncTimeout, reconcileTimeout)

	t.Run("creates async-processor deployment alongside batch resources", func(t *testing.T) {
		gw := newTestAsyncGateway("test-async-create")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}
		if result.RequeueAfter != resyncTimeout {
			t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, resyncTimeout)
		}

		var deployList appsv1.DeploymentList
		if err := k8sClient.List(ctx, &deployList); err != nil {
			t.Fatalf("listing deployments: %v", err)
		}
		deployCount := 0
		hasAsyncProcessor := false
		for _, d := range deployList.Items {
			if !isOwnedByUID(d.OwnerReferences, gw.UID) {
				continue
			}
			deployCount++
			if d.Labels[labelKeyComponent] == componentAsyncProcessor {
				hasAsyncProcessor = true
			}
		}
		// 2 batch Deployments (apiserver, gc) + 1 async-processor Deployment.
		if deployCount != 3 {
			t.Errorf("deployment count = %d, want 3", deployCount)
		}
		if !hasAsyncProcessor {
			t.Error("no async-processor deployment found")
		}
	})

	t.Run("sets AsyncProcessorAvailable condition", func(t *testing.T) {
		gw := newTestAsyncGateway("test-async-condition")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}

		var updated batchv1alpha1.LLMBatchGateway
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace}, &updated); err != nil {
			t.Fatalf("getting updated CR: %v", err)
		}

		found := false
		for _, c := range updated.Status.Conditions {
			if c.Type == conditionAsyncProcessorAvailable {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing condition %q", conditionAsyncProcessorAvailable)
		}
	})

	t.Run("nil async renderer returns error for async mode", func(t *testing.T) {
		nilAsyncReconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, nil, fakeRecorder, resyncTimeout, reconcileTimeout)

		gw := newTestAsyncGateway("test-async-nil-renderer")
		if err := k8sClient.Create(ctx, gw); err != nil {
			t.Fatalf("creating CR: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

		_, err := nilAsyncReconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
		})
		if err == nil {
			t.Fatal("Reconcile() expected error for nil async renderer, got nil")
		}
		if !strings.Contains(err.Error(), "async helm renderer") {
			t.Errorf("error = %v, want mention of async helm renderer", err)
		}
	})
}

func TestConditionHelpers(t *testing.T) {
	if got := conditionStatus(true); got != metav1.ConditionTrue {
		t.Errorf("conditionStatus(true) = %v, want True", got)
	}
	if got := conditionStatus(false); got != metav1.ConditionFalse {
		t.Errorf("conditionStatus(false) = %v, want False", got)
	}
	if got := conditionReason(true, "Yes", "No"); got != "Yes" {
		t.Errorf("conditionReason(true) = %v, want Yes", got)
	}
	if got := conditionReason(false, "Yes", "No"); got != "No" {
		t.Errorf("conditionReason(false) = %v, want No", got)
	}
	if got := conditionMessage(true, "ok", "bad"); got != "ok" {
		t.Errorf("conditionMessage(true) = %v, want ok", got)
	}
	if got := conditionMessage(false, "ok", "bad"); got != "bad" {
		t.Errorf("conditionMessage(false) = %v, want bad", got)
	}
}

func TestIsOwnedBy(t *testing.T) {
	gw := newTestGateway("gw")
	gw.UID = "test-uid"

	owned := &corev1.ConfigMap{}
	owned.OwnerReferences = []metav1.OwnerReference{{UID: gw.UID}}

	notOwned := &corev1.ConfigMap{}

	if !isOwnedBy(owned, gw) {
		t.Error("isOwnedBy: expected true for owned object")
	}
	if isOwnedBy(notOwned, gw) {
		t.Error("isOwnedBy: expected false for unowned object")
	}
}

func isOwnedByUID(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

func TestReconcileTimeout(t *testing.T) {
	ctx := context.Background()

	batchGWHelmRenderer, err := NewHelmRenderer("../../batch-gateway/charts/batch-gateway", testImages(), tlspkg.ProfileValues{})
	if err != nil {
		t.Fatalf("NewHelmRenderer() error: %v", err)
	}

	fakeRecorder := record.NewFakeRecorder(100)
	resyncTimeout := 5 * time.Minute
	reconcileTimeout := -1 * time.Second
	// Use a 1ns timeout so the context is expired before the first API call.
	reconciler := NewLLMBatchGatewayReconciler(k8sClient, k8sClient.Scheme(), batchGWHelmRenderer, nil, fakeRecorder, resyncTimeout, reconcileTimeout)

	gw := newTestGateway("test-timeout")
	if err := k8sClient.Create(ctx, gw); err != nil {
		t.Fatalf("creating CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, gw) })

	_, err = reconciler.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: gw.Name, Namespace: gw.Namespace},
	})
	if err == nil {
		t.Fatal("Reconcile() expected error due to timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Reconcile() error = %v, want context.DeadlineExceeded", err)
	}
}

// getOwnedConfigMap returns the ConfigMap for the given component that is
// owned by gw. ConfigMap names follow the pattern <name>-batch-gateway-<component>-config.
func getOwnedConfigMap(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, component string) *corev1.ConfigMap {
	t.Helper()
	cmName := gw.Name + "-batch-gateway-" + component + "-config"
	var cm corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: gw.Namespace}, &cm); err != nil {
		t.Fatalf("getting %s configmap %s: %v", component, cmName, err)
	}
	if !isOwnedByUID(cm.OwnerReferences, gw.UID) {
		t.Fatalf("configmap %s is not owned by %s", cmName, gw.Name)
	}
	return &cm
}

// assertConfigMapContains verifies that the config.yaml data in the ConfigMap
// contains the expected substring.
func assertConfigMapContains(t *testing.T, cm *corev1.ConfigMap, want string) {
	t.Helper()
	data := cm.Data["config.yaml"]
	if !strings.Contains(data, want) {
		t.Errorf("configmap %s config.yaml missing %q\ngot:\n%s", cm.Name, want, data)
	}
}

// findOwnedDeployment returns the Deployment with the given component label
// that is owned by gw. It fails the test if no such Deployment is found.
func findOwnedDeployment(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, component string) *appsv1.Deployment {
	t.Helper()
	var deployList appsv1.DeploymentList
	if err := k8sClient.List(ctx, &deployList); err != nil {
		t.Fatalf("listing deployments: %v", err)
	}
	for i := range deployList.Items {
		d := &deployList.Items[i]
		if !isOwnedByUID(d.OwnerReferences, gw.UID) {
			continue
		}
		if d.Labels[labelKeyComponent] == component {
			return d
		}
	}
	t.Fatalf("no deployment found with component=%s owned by %s", component, gw.Name)
	return nil
}

func findOwnedProcessorStatefulSet(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway) *appsv1.StatefulSet {
	t.Helper()
	var statefulSetList appsv1.StatefulSetList
	if err := k8sClient.List(ctx, &statefulSetList); err != nil {
		t.Fatalf("listing statefulsets: %v", err)
	}
	for i := range statefulSetList.Items {
		ss := &statefulSetList.Items[i]
		if !isOwnedByUID(ss.OwnerReferences, gw.UID) {
			continue
		}
		if ss.Labels[labelKeyComponent] == componentProcessor {
			return ss
		}
	}
	t.Fatalf("no processor statefulset found owned by %s", gw.Name)
	return nil
}

func workloadConfigChecksum(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, component string) string {
	t.Helper()
	if component == componentProcessor {
		ss := findOwnedProcessorStatefulSet(ctx, t, gw)
		return ss.Spec.Template.Annotations["checksum/config"]
	}
	d := findOwnedDeployment(ctx, t, gw, component)
	return d.Spec.Template.Annotations["checksum/config"]
}

// assertProcessorStatefulSetReplicas verifies that the processor StatefulSet
// has the expected replica count.
func assertProcessorStatefulSetReplicas(ctx context.Context, t *testing.T, gw *batchv1alpha1.LLMBatchGateway, want int32) {
	t.Helper()
	ss := findOwnedProcessorStatefulSet(ctx, t, gw)
	if ss.Spec.Replicas == nil || *ss.Spec.Replicas != want {
		got := int32(0)
		if ss.Spec.Replicas != nil {
			got = *ss.Spec.Replicas
		}
		t.Errorf("processor replicas = %d, want %d", got, want)
	}
}

// assertResourceValue verifies that the given resource list contains the expected
// value for the named resource.
func assertResourceValue(t *testing.T, label string, list corev1.ResourceList, name corev1.ResourceName, want string) {
	t.Helper()
	got, ok := list[name]
	if !ok {
		t.Errorf("%s: resource %s not set", label, name)
		return
	}
	expected := resource.MustParse(want)
	if got.Cmp(expected) != 0 {
		t.Errorf("%s = %s, want %s", label, got.String(), expected.String())
	}
}

// assertWarningEvent drains the fake recorder channel and asserts that at least one
// Warning event contains the expected reason substring.
func assertWarningEvent(t *testing.T, recorder *record.FakeRecorder, reason string) {
	t.Helper()
	for {
		select {
		case got := <-recorder.Events:
			if strings.Contains(got, corev1.EventTypeWarning) && strings.Contains(got, reason) {
				return
			}
			// Drain other events that don't match (e.g. from previous subtests).
		default:
			t.Errorf("expected Warning event with reason %q but none found", reason)
			return
		}
	}
}
