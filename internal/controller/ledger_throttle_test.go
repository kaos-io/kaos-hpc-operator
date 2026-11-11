package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Regression (found by the scale test): every reconcile of every job published the HPCSite status, so a
// busy project wrote it 31 times in one second. The writes are now bounded by LedgerPublishInterval and
// a request inside the interval is written once the interval has passed.
func TestShouldBoundHPCSiteStatusWritesAndStillPublishTheNewestState(t *testing.T) {
	ctx := context.Background()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "p-train", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "p"}}}
	site := &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "site", Namespace: "p-ml", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "p"}},
		Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
			Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://s.example", RESTVersion: "v0.0.44",
				Classes: []hpcv1alpha1.SiteClass{{Name: "gpu", Partition: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 600}}},
			Identity: hpcv1alpha1.ProjectIdentity{User: "u", Account: "a", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "i"}},
			Budget:   hpcv1alpha1.Budget{GPUNodeHours: 10}},
	}
	var writes atomic.Int32
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns, site).WithStatusSubresource(site, &hpcv1alpha1.HPCArgoWorkflowJob{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string,
			obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*hpcv1alpha1.HPCSite); ok {
				writes.Add(1)
			}
			return c.SubResource(sub).Patch(ctx, obj, p, opts...)
		}}).Build()
	r := &HPCArgoWorkflowJobReconciler{Client: c, Scheme: s, LedgerPublishInterval: 300 * time.Millisecond}

	if err := r.publishLedger(ctx, site.DeepCopy()); err != nil { // the first request writes at once
		t.Fatal(err)
	}
	if writes.Load() != 1 {
		t.Fatalf("first publish wrote %d times, want 1", writes.Load())
	}
	// A job is booked, then 50 requests arrive inside the interval: none writes, one is scheduled.
	b := metav1.NewTime(time.Now())
	job := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "p-train", UID: "u1"},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "gpu", Image: "reg.example/img:1", WallTimeMinutes: 60},
		Status: hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted, Attempt: 1,
			Site:    &hpcv1alpha1.SiteRef{Name: "site", Namespace: "p-ml"},
			Booking: &hpcv1alpha1.Booking{Site: "meluxina", SiteName: "site", SiteNamespace: "p-ml", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 60, At: b}}}
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := r.publishLedger(ctx, site.DeepCopy()); err != nil {
			t.Fatal(err)
		}
	}
	if writes.Load() != 1 {
		t.Fatalf("%d writes inside the interval, want still 1", writes.Load())
	}
	deadline := time.Now().Add(3 * time.Second)
	var got hpcv1alpha1.HPCSite
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(site), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.ActiveJobs == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got.Status.ActiveJobs != 1 || got.Status.GPU == nil || got.Status.GPU.BookedNodeMinutes != 60 {
		t.Fatalf("the deferred write never published the booking: %+v", got.Status)
	}
	if writes.Load() != 2 {
		t.Fatalf("%d writes in total, want 2 (the first and one deferred)", writes.Load())
	}
}
