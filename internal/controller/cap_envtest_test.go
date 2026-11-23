package controller

import (
	"fmt"
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

// A step whose cap on its total time is above what one job can get (PRD kaos#1911 D-75): nothing is refused for
// its time; its first job gets the most a job can get and the cap bounds the jobs together.
var _ = Describe("a step with a cap above what one job can get", func() {
	const laneMax = int32(600) // shorter than the cap, so the lane is what binds
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
		withWorkflow(j)
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

	It("admits a cap of days and gives the first job the longest lane, booking one job, not the cap", func() {
		got := submit(4*1440, nil)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), got.Status.Message)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(laneMax))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(laneMax)))
		Expect(runnerEnv(got)).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "600"))
		Expect(runnerEnv(got)).To(HaveKeyWithValue("HPC_RESUMABLE_LEAD", "10"), "every job is warned before its limit")
	})

	It("admits a cap below the longest lane as the one job", func() {
		got := submit(90, nil)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), got.Status.Message)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(90)))
	})

	// No credential bounds a job (D-76): on a site whose lane is long, the first job gets the whole cap.
	Context("on a site whose lane is longer than the cap", func() {
		BeforeEach(func() {
			site := &hpcv1alpha1.HPCSite{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: "meluxina"}, site)).To(Succeed())
			site.Spec.Profile.Classes[0].MaxWallTimeMinutes = 2880
			Expect(k8sClient.Update(ctx, site)).To(Succeed())
		})

		It("admits a step of 12 hours or 20 hours with a first job of the whole cap", func() {
			for _, wall := range []int32{12 * 60, 20 * 60} {
				got := submit(wall, nil)
				Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), got.Status.Message)
				Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(wall))
				Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(wall)))
				Expect(runnerEnv(got)).To(HaveKeyWithValue("SLURM_TIME_LIMIT", fmt.Sprint(wall)))
			}
		})
	})
})
