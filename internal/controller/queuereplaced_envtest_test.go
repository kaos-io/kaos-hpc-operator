package controller

import (
	"fmt"
	"strings"
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

// PRD-HPC-1461 AC-35: a queued site job replaced before its token expires loses its queue place; the runner
// reports each replacement in status.run, the controller keeps it (bounded) and copies it to the attempt record.
var _ = Describe("HPC queue replacements", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		envNS, hpcNS string
	)

	newReconciler := func() *HPCArgoWorkflowJobReconciler {
		return &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(20),
			RunnerImage: "kaos/hpc-operator:test"}
	}

	BeforeEach(func() {
		project := fmt.Sprintf("qr%d", time.Now().UnixNano()%1_000_000_000)
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
		r = newReconciler()
	})

	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}
	replacements := func(n int) []hpcv1alpha1.QueueReplacement {
		var out []hpcv1alpha1.QueueReplacement
		for i := range n {
			out = append(out, hpcv1alpha1.QueueReplacement{At: metav1.NewTime(time.Now().Add(time.Duration(i) * time.Minute)),
				OldSiteJobID: fmt.Sprint(100 + i), NewSiteJobID: fmt.Sprint(101 + i), QueuedSeconds: int64(41400 + i),
				Reason: "TokenRefresh"})
		}
		return out
	}
	writeRun := func(j *hpcv1alpha1.HPCArgoWorkflowJob, run hpcv1alpha1.RunStatus) error {
		cur := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), cur)).To(Succeed())
		orig := cur.DeepCopy()
		cur.Status.Run = &run
		return k8sClient.Status().Patch(ctx, cur, client.MergeFrom(orig))
	}
	newJob := func() *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "qr-job", Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "zot.example/proj/app:v1", WallTimeMinutes: 10,
				StepName: "train", Command: []string{"x"}}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		Expect(reconcileOnce(j).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		return j
	}

	It("should_persist_the_reported_replacements_and_survive_a_controller_restart", func() {
		j := newJob()
		kept := replacements(hpcv1alpha1.QueueReplacedKept)
		Expect(writeRun(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "105", State: "PENDING",
			QueueReplaced: kept, QueueReplacedCount: 8})).To(Succeed())
		reconcileOnce(j)
		r = newReconciler() // a fresh process: nothing is in memory, the record is in the job's status
		got := reconcileOnce(j)
		Expect(got.Status.Run.QueueReplacedCount).To(Equal(int32(8)))
		Expect(got.Status.Run.QueueReplaced).To(HaveLen(hpcv1alpha1.QueueReplacedKept))
		Expect(got.Status.Run.QueueReplaced[0].OldSiteJobID).To(Equal("100"))
		Expect(got.Status.Run.QueueReplaced[0].QueuedSeconds).To(Equal(int64(41400)))
		Expect(recentJobOf(got, time.Now()).QueueReplacedCount).To(Equal(int32(8)))
	})

	It("should_refuse_a_list_longer_than_the_bound_and_a_reason_it_does_not_know", func() {
		j := newJob()
		err := writeRun(j, hpcv1alpha1.RunStatus{Attempt: 1, QueueReplaced: replacements(hpcv1alpha1.QueueReplacedKept + 1)})
		Expect(err).To(HaveOccurred())
		Expect(strings.ToLower(err.Error())).To(ContainSubstring("queuereplaced"))
		bad := replacements(1)
		bad[0].Reason = "Whim"
		Expect(writeRun(j, hpcv1alpha1.RunStatus{Attempt: 1, QueueReplaced: bad})).NotTo(Succeed())
	})

	It("should_copy_the_replacements_to_the_attempt_record_when_the_attempt_is_settled", func() {
		j := newJob()
		start := metav1.NewTime(time.Now().Add(-5 * time.Minute))
		end := metav1.NewTime(time.Now().Add(-1 * time.Minute))
		zero := int32(0)
		Expect(writeRun(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "102", State: "COMPLETED", ExitCode: &zero,
			StartTime: &start, EndTime: &end, Terminal: true,
			QueueReplaced: replacements(2), QueueReplacedCount: 2})).To(Succeed())
		got := reconcileOnce(j)
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Attempts[0].QueueReplacedCount).To(Equal(int32(2)))
		Expect(got.Status.Attempts[0].QueueReplaced).To(HaveLen(2))
		Expect(recentJobOf(got, time.Now()).QueueReplacedCount).To(Equal(int32(2)), "counted once, not run plus attempt")
	})
})
