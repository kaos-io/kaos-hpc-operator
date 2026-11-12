package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// An image reference that can never be pulled ends the job at once: no booking, no runner Job, no site job.
// Live 2026-11-10: ":probe-learn-latest" was submitted and retried with fresh credentials three times.
var _ = Describe("HPC invalid image", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		envNS, hpcNS string
	)

	BeforeEach(func() {
		project := fmt.Sprintf("img%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://slurm-api.example",
					RESTVersion: "v0.0.44", Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu",
						Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget:   hpcv1alpha1.Budget{GPUNodeHours: 100}}})).To(Succeed())
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(20),
			RunnerImage: "kaos/hpc-operator:test"}
	})

	newJob := func(name, image string) *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: image, WallTimeMinutes: 10,
				StepName: "train", Command: []string{"x"}}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}
	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}

	It("should_refuse_an_unpullable_reference_with_no_booking_and_no_runner", func() {
		got := reconcileOnce(newJob("bad-ref", ":probe-learn-latest"))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonInvalidImage))
		Expect(got.Status.Message).To(ContainSubstring(":probe-learn-latest"))
		Expect(got.Status.Booking).To(BeNil())
		var runners batchv1.JobList
		Expect(k8sClient.List(ctx, &runners, client.InNamespace(hpcNS))).To(Succeed())
		Expect(runners.Items).To(BeEmpty(), "no runner, so no site job")
	})

	It("should_admit_a_good_reference", func() {
		Expect(reconcileOnce(newJob("good-ref", "zot.example/proj/app:v1")).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
	})

	// PRD kaos#1911 F-04: the runner and the site's staging tool ship separately; when their staging contracts
	// differ the runner stops before it stages or submits, and the job says why.
	It("should_end_a_job_whose_runner_met_another_staging_contract_as_failed_staging_mismatch_and_settle_nothing", func() {
		j := reconcileOnce(newJob("contract", "zot.example/proj/app:v1"))
		orig := j.DeepCopy()
		code := hpcv1alpha1.ExitCodeStagingMismatch
		j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "FAILED", ExitCode: &code, Terminal: true,
			Message: "the site's staging tool and this runner speak different staging contracts (staging tool: 1, runner: 2)"}
		Expect(k8sClient.Status().Patch(ctx, j, client.MergeFrom(orig))).To(Succeed())
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonStagingMismatch))
		Expect(got.Status.Message).To(ContainSubstring("different staging contracts"))
		Expect(got.Status.SettledNodeMinutes).To(HaveValue(Equal(int64(0))))
	})

	It("should_end_a_job_whose_dataset_holds_a_reserved_name_as_failed_reserved_name_and_settle_nothing", func() {
		j := reconcileOnce(newJob("reserved", "zot.example/proj/app:v1"))
		orig := j.DeepCopy()
		code := hpcv1alpha1.ExitCodeReservedName
		j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "FAILED", ExitCode: &code, Terminal: true,
			Message: "dataset d@c0ffee has an object named '.kaos-ready': names starting with .kaos are reserved for the platform"}
		Expect(k8sClient.Status().Patch(ctx, j, client.MergeFrom(orig))).To(Succeed())
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonReservedName))
		Expect(got.Status.Message).To(ContainSubstring(".kaos-ready"))
		Expect(got.Status.SettledNodeMinutes).To(HaveValue(Equal(int64(0))))
	})

	It("should_end_a_job_the_runner_found_invalid_as_failed_invalid_image_and_settle_nothing", func() {
		j := reconcileOnce(newJob("runner-found", "zot.example/proj/app:v1"))
		orig := j.DeepCopy()
		code := hpcv1alpha1.ExitCodeInvalidImage
		j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "FAILED", ExitCode: &code, Terminal: true,
			Message: "the step image \"x\" cannot be pulled: it has no tag or digest"}
		Expect(k8sClient.Status().Patch(ctx, j, client.MergeFrom(orig))).To(Succeed())
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonInvalidImage))
		Expect(got.Status.Message).To(ContainSubstring("cannot be pulled"))
		Expect(got.Status.SettledNodeMinutes).To(HaveValue(Equal(int64(0))))
	})
})
