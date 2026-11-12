package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// PRD-HPC-1461 AC-39 (amended 2026-11-09): at the site's submit cap a new job FAILS with SubmitLimit and
// a message naming the cap and the count; the concurrency limit (D-49) still waits. A fake site with a
// cap of 5, against a real API server.
var _ = Describe("HPC submit limit", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		rec          *record.FakeRecorder
		project      string
		envNS, hpcNS string
		seq          int
	)

	BeforeEach(func() {
		project = fmt.Sprintf("sub%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-hpc"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		rec = record.NewFakeRecorder(50)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
	})

	newSite := func(maxSubmit, maxConcurrent int32) {
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://slurm-api.example",
					RESTVersion: "v0.0.44", MaxSubmitJobs: maxSubmit, Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu",
						Partition: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget:   hpcv1alpha1.Budget{GPUNodeHours: 100},
				Limits:   hpcv1alpha1.Limits{MaxConcurrentJobs: maxConcurrent}}})).To(Succeed())
	}
	submit := func() *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("step-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/img:1", WallTimeMinutes: 10}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}

	It("admits five jobs and fails the sixth with SubmitLimit, naming the cap and the count", func() {
		newSite(5, 0)
		for i := 0; i < 5; i++ {
			Expect(submit().Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		}
		got := submit()
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSubmitLimit))
		Expect(got.Status.Message).To(And(ContainSubstring("allows 5"), ContainSubstring("already has 5")))
		Expect(got.Status.Booking).To(BeNil(), "a refused job books nothing")
	})

	It("keeps the concurrency limit a wait when it is the lower one (D-49)", func() {
		newSite(5, 2)
		submit()
		submit()
		got := submit()
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonConcurrencyLimit))
	})

	It("ends an attempt the site refused for its cap as Failed SubmitLimit with the runner's sentence", func() {
		newSite(5, 0)
		j := submit()
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		code := hpcv1alpha1.ExitCodeSubmitLimit
		got.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "FAILED", ExitCode: &code, Terminal: true,
			Message: "the site accepts at most 5 queued or running jobs per user and 5 are already queued or running"}
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSubmitLimit))
		Expect(got.Status.Message).To(ContainSubstring("at most 5"))
		Expect(got.Status.SettledNodeMinutes).To(HaveValue(Equal(int64(0))))
	})
})
