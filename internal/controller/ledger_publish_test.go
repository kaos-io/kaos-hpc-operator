package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// laggingCache serves List from a snapshot, like an informer cache that has not seen the newest write.
// catchUp makes the next List fresh again. Get and writes go straight to the API (the fake client).
type laggingCache struct {
	client.Client
	frozen *hpcv1alpha1.HPCArgoWorkflowJobList
}

func (l *laggingCache) freeze(t *testing.T) {
	t.Helper()
	l.frozen = &hpcv1alpha1.HPCArgoWorkflowJobList{}
	if err := l.Client.List(context.Background(), l.frozen); err != nil {
		t.Fatal(err)
	}
}

func (l *laggingCache) catchUp() { l.frozen = nil }

func (l *laggingCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if jl, ok := list.(*hpcv1alpha1.HPCArgoWorkflowJobList); ok && l.frozen != nil {
		l.frozen.DeepCopyInto(jl)
		return nil
	}
	return l.Client.List(ctx, list, opts...)
}

// Regression (found by the scale test): the ledger published in the reconcile that ends a job is
// computed from the informer cache, which does not show that very write, so the last job to end left
// its booking (and one active job) in HPCSite.status until the 10-minute resync. The next reconcile of
// the ended job, woken by its own end event, must publish the ledger again.
func TestShouldPublishTheLedgerAgainOnceTheCacheShowsAJobsEnd(t *testing.T) {
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
	start, end := metav1.NewTime(time.Now().Add(-10*time.Minute)), metav1.NewTime(time.Now().Add(-1*time.Minute))
	zero := int32(0)
	job := &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "p-train", UID: "uid-1"},
		Spec:       hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "gpu", Image: "i", WallTimeMinutes: 60},
		Status: hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Attempt: 1,
			Site:    &hpcv1alpha1.SiteRef{Name: "site", Namespace: "p-ml"},
			Booking: &hpcv1alpha1.Booking{Site: "meluxina", SiteName: "site", SiteNamespace: "p-ml", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 60, At: start},
			Run: &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "7", State: "COMPLETED", ExitCode: &zero, StartTime: &start, EndTime: &end,
				Terminal: true}},
	}
	base := fake.NewClientBuilder().WithScheme(s).WithObjects(ns, site, job).WithStatusSubresource(site, job).Build()
	cache := &laggingCache{Client: base}
	r := &HPCArgoWorkflowJobReconciler{Client: cache, Scheme: s}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}

	cache.freeze(t) // the cache still shows the job running with its booking
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var got hpcv1alpha1.HPCSite
	if err := base.Get(ctx, client.ObjectKeyFromObject(site), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GPU == nil || got.Status.GPU.BookedNodeMinutes != 60 {
		t.Fatalf("setup: the lagging cache should make the first publish stale, got %+v", got.Status.GPU)
	}

	cache.catchUp() // the job's end event arrives: the reconcile it wakes sees the end
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(site), &got); err != nil {
		t.Fatal(err)
	}
	if g := got.Status.GPU; g.BookedNodeMinutes != 0 || g.UsedNodeMinutes != 9 || got.Status.ActiveJobs != 0 {
		t.Fatalf("published ledger booked %d used %d active %d, want 0, 9, 0", g.BookedNodeMinutes, g.UsedNodeMinutes, got.Status.ActiveJobs)
	}
}
