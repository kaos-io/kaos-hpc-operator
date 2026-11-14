package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// A job whose spec.env is outside the allowlist cannot get past the CRD, but the controller
// refuses it too (second lock): tested on a fake client, which applies no CEL.
func TestShouldRefuseAnEnvOutsideTheAllowlistWithInvalidSpec(t *testing.T) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	job := &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "evil", Namespace: "ns", UID: "u1"},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "c", Image: "reg.example/img:1", WallTimeMinutes: 5, StepName: "s",
			Env: []hpcv1alpha1.NameValue{{Name: "MLFLOW_EXTERNAL_URL", Value: "https://evil.example"}}},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(job).WithStatusSubresource(job).Build()
	r := &HPCArgoWorkflowJobReconciler{Client: c, Scheme: s, RunnerImage: "img"}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}); err != nil {
		t.Fatal(err)
	}
	var got hpcv1alpha1.HPCArgoWorkflowJob
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != hpcv1alpha1.PhaseRefused || got.Status.Reason != hpcv1alpha1.ReasonInvalidSpec {
		t.Fatalf("phase %q reason %q, want Refused/InvalidSpec (%s)", got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
	if len(got.Finalizers) != 0 {
		t.Fatalf("a refused job needs no cancel finalizer: %v", got.Finalizers)
	}
}

// The manager wiring: a serve-mode job gets its runner Job, and a failed runner Job wakes the
// job through the Job watch.
var _ = Describe("HPCArgoWorkflowJob controller wiring", func() {
	It("creates the runner Job and reacts to its failure through the Job watch", func() {
		project := fmt.Sprintf("mgr%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS := project+"-train", project+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://s.example", RESTVersion: "v0.0.44",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 600}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u", Account: "a", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "i"}},
				Budget:   hpcv1alpha1.Budget{GPUNodeHours: 10}},
		})).To(Succeed())

		jobSel, _ := labels.Parse(runnerjob.LabelJobUID)
		podSel, _ := labels.Parse(runnerjob.LabelRunnerJob)
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  k8sClient.Scheme(),
			Metrics: metricsserver.Options{BindAddress: "0"},
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{envNS: {}, hpcNS: {}},
				ByObject: map[client.Object]cache.ByObject{
					&batchv1.Job{}: {Label: jobSel}, &corev1.Pod{}: {Label: podSel}},
			},
		})
		Expect(err).NotTo(HaveOccurred())
		r := &HPCArgoWorkflowJobReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), RunnerImage: "img", APIReader: mgr.GetAPIReader()}
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		mctx, stop := context.WithCancel(ctx)
		defer stop()
		go func() { defer GinkgoRecover(); Expect(mgr.Start(mctx)).To(Succeed()) }()

		job := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "wired", Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/img:1", WallTimeMinutes: 30, StepName: "s"}}
		withWorkflow(job)
		Expect(k8sClient.Create(ctx, job)).To(Succeed())

		var rj batchv1.Job
		Eventually(func() error {
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
				return err
			}
			return k8sClient.Get(ctx, client.ObjectKey{Namespace: hpcNS, Name: runnerjob.Name(job.UID, 1)}, &rj)
		}, 20*time.Second, 200*time.Millisecond).Should(Succeed())

		now := metav1.Now()
		rj.Status.StartTime = &now
		rj.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
		Expect(k8sClient.Status().Update(ctx, &rj)).To(Succeed())
		// The runner reported no site job id: the sweep's cancel-only runner finds none and the site says so.
		Eventually(func() bool {
			_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job)
			return job.Status.Sweep != nil
		}, 20*time.Second, 200*time.Millisecond).Should(BeTrue())
		end := metav1.NewTime(time.Now().Truncate(time.Second))
		orig := job.DeepCopy()
		job.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "-", State: "NOT_SUBMITTED", EndTime: &end, Terminal: true}
		Expect(k8sClient.Status().Patch(ctx, job, client.MergeFrom(orig))).To(Succeed())
		Eventually(func() hpcv1alpha1.Phase {
			_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job)
			return job.Status.Phase
		}, 20*time.Second, 200*time.Millisecond).Should(Equal(hpcv1alpha1.PhaseFailed))
		Expect(job.Status.Reason).To(Equal(hpcv1alpha1.ReasonRunnerFailed))
	})
})
