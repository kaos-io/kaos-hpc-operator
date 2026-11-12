package controller

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// A step whose declared limit is above the longest lane (PRD-HPC-1461 D-67, D-74, AC-77): refused at
// admission with a reason the follower prints when it is not resumable, run as continuations of at most
// the lane maximum when it is, and refused again beyond the continuation cap.
var _ = Describe("a step longer than the longest lane", func() {
	const laneMax = int32(2880)
	var (
		r      *HPCArgoWorkflowJobReconciler
		envNS  string
		hpcNS  string
		seq    int
		sitePr string
	)

	submit := func(wall int32, mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("long-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: fmt.Sprintf("reg.example/lab/long%d:v1", seq),
				WallTimeMinutes: wall, Command: []string{"python", "train.py"}, StepName: "train"},
		}
		if mod != nil {
			mod(j)
		}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}

	runnerEnv := func(j *hpcv1alpha1.HPCArgoWorkflowJob) map[string]string {
		rj := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.Name(j.UID, 1)}, rj)).To(Succeed())
		m := map[string]string{}
		for _, e := range rj.Spec.Template.Spec.Containers[0].Env {
			m[e.Name] = e.Value
		}
		return m
	}

	BeforeEach(func() {
		seq = 0
		sitePr = fmt.Sprintf("tl%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = sitePr+"-train", sitePr+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: sitePr}}})).To(Succeed())
		}
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: sitePr}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44", ScratchRoot: "/project/scratch/p1",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: laneMax}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 1000}},
		})).To(Succeed())
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test"}
	})

	It("refuses a step that is not resumable with the numbers and the fix, books nothing and submits nothing", func() {
		got := submit(4*1440, nil)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTooLongNotResumable))
		for _, w := range []string{"4 days (5760 min)", "2 days (2880 min)", "resumable=True", "walltime"} {
			Expect(got.Status.Message).To(ContainSubstring(w))
		}
		Expect(got.Status.Booking).To(BeNil())
		rj := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.Name(got.UID, 1)}, rj)
		Expect(client.IgnoreNotFound(err)).To(Succeed())
		Expect(err).To(HaveOccurred(), "nothing is submitted for a refused step")
	})

	It("refuses a step that opted out of resuming even when it is longer only slightly", func() {
		got := submit(laneMax+1, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
			j.Spec.ResumableOptOut = true
		})
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTooLongNotResumable))
	})

	It("runs a declared resumable step as continuations of at most the lane maximum", func() {
		got := submit(5*1440, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(laneMax))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(laneMax)), "one run is booked, not the whole declared limit")
		Expect(runnerEnv(got)).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "2880"))
	})

	It("refuses a resumable step that would need more runs than the continuation cap", func() {
		got := submit(6*laneMax+1, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTooLongBeyondContinuations))
		Expect(strings.Contains(got.Status.Message, "5 continuations")).To(BeTrue(), got.Status.Message)
	})

	It("admits a new job of a step that earlier jobs verified resumable", func() {
		old := submit(60, nil)
		image := old.Spec.Image
		orig := old.DeepCopy()
		old.Status.ResumeEvidence = &hpcv1alpha1.ResumeEvidence{Verified: true, At: metav1.Now()}
		Expect(k8sClient.Status().Patch(ctx, old, client.MergeFrom(orig))).To(Succeed())
		got := submit(4*1440, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = image })
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), got.Status.Message)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(laneMax))
	})
})
