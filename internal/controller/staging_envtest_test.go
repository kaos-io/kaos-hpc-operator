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

// PRD-HPC-1461: the runner reports how each input was staged in status.run.staged; the controller keeps
// it across its own reconciles and a restart, and the CRD refuses what is not a bounded record.
var _ = Describe("HPC dataset staging record", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		envNS, hpcNS string
	)

	BeforeEach(func() {
		project := fmt.Sprintf("st%d", time.Now().UnixNano()%1_000_000_000)
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

	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}
	writeStaging := func(j *hpcv1alpha1.HPCArgoWorkflowJob, s ...hpcv1alpha1.StagingStatus) error {
		cur := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), cur)).To(Succeed())
		orig := cur.DeepCopy()
		cur.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "105", State: "PENDING", Staged: s}
		return k8sClient.Status().Patch(ctx, cur, client.MergeFrom(orig))
	}
	newJob := func() *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "st-job", Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "zot.example/proj/app:v1", WallTimeMinutes: 10,
				StepName: "train", Command: []string{"x"}}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		Expect(reconcileOnce(j).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		return j
	}
	good := hpcv1alpha1.StagingStatus{Repo: "dataset-poulaki", Commit: "c2c2c2c2c2c2", Kind: hpcv1alpha1.StagingDelta,
		Files: 12, Bytes: 3_400_000_000, Seconds: 140}

	It("should_persist_the_staging_record_and_keep_it_across_reconciles_and_a_controller_restart", func() {
		j := newJob()
		second := hpcv1alpha1.StagingStatus{Repo: "dataset-labels", Commit: "d3d3d3d3d3d3", Kind: hpcv1alpha1.StagingCached}
		Expect(writeStaging(j, good, second)).To(Succeed()) // one record per input
		reconcileOnce(j)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(20),
			RunnerImage: "kaos/hpc-operator:test"} // a fresh process: only the object holds the record
		got := reconcileOnce(j)
		Expect(got.Status.Run).NotTo(BeNil())
		Expect(got.Status.Run.Staged).To(Equal([]hpcv1alpha1.StagingStatus{good, second}))
	})

	It("should_refuse_an_unknown_kind_a_long_commit_and_a_negative_size", func() {
		j := newJob()
		for _, bad := range []hpcv1alpha1.StagingStatus{
			{Repo: "ds", Commit: "abc", Kind: "partial"},
			{Repo: "ds", Commit: "0123456789abcdef", Kind: hpcv1alpha1.StagingFirst},
			{Repo: "ds", Commit: "abc", Kind: hpcv1alpha1.StagingFirst, Bytes: -1},
		} {
			Expect(writeStaging(j, bad)).NotTo(Succeed())
		}
	})
})
