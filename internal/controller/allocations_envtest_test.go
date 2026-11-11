package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// PRD-HPC-1461 AC-84: every allocation (GPU, CPU, FPGA, large memory) has its own budget, admission check
// and ledger. A fake site with one spent allocation (CPU, as in the 2026-11-05 incident), source site, against
// a real API server.
var _ = Describe("HPC per-allocation budgets", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		project      string
		envNS, hpcNS string
		seq          int
	)

	classes := []hpcv1alpha1.SiteClass{
		{Name: "meluxina-gpu", Partition: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880},
		{Name: "meluxina-cpu", Partition: "cpu", Tier: hpcv1alpha1.TierCPU, MaxWallTimeMinutes: 2880},
		{Name: "meluxina-fpga", Partition: "fpga", Tier: hpcv1alpha1.TierFPGA, MaxWallTimeMinutes: 2880},
		{Name: "meluxina-largemem", Partition: "largemem", Tier: hpcv1alpha1.TierLargeMem, MaxWallTimeMinutes: 2880},
	}

	BeforeEach(func() {
		project = fmt.Sprintf("alc%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-hpc"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(50)}
		obs := metav1.NewTime(time.Now().Add(-time.Hour))
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://slurm-api.example",
					RESTVersion: "v0.0.44", Classes: classes},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				// What the site reports left per allocation: CPU is spent (0), the rest have hours.
				Budget: hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, ObservedAt: &obs,
					GPUNodeHours: 10, CPUNodeHours: 0, FPGANodeHours: i64p(5), LargeMemNodeHours: i64p(5)}}})).To(Succeed())
	})

	submit := func(class string, wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("step-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: class, Image: "reg.example/img:1", WallTimeMinutes: wall}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return reconcileOnce(r, j)
	}

	siteNow := func() *hpcv1alpha1.HPCSite {
		s := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, s)).To(Succeed())
		return s
	}

	It("blocks only the CPU steps when the CPU allocation is spent, naming it", func() {
		cpu := submit("meluxina-cpu", 60)
		Expect(cpu.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(cpu.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteLimit))
		Expect(cpu.Status.Message).To(And(ContainSubstring("CPU"), ContainSubstring("0 left on the CPU allocation"),
			ContainSubstring("the site reports the account has")))
		for _, c := range []string{"meluxina-gpu", "meluxina-fpga", "meluxina-largemem"} {
			Expect(submit(c, 60).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), c)
		}
	})

	It("shows all four allocations in the HPCSite status, each with its own booking", func() {
		submit("meluxina-fpga", 120)
		submit("meluxina-largemem", 60)
		Expect(r.publishLedger(ctx, siteNow())).To(Succeed())
		st := siteNow().Status
		Expect(st.GPU).NotTo(BeNil())
		Expect(st.CPU).NotTo(BeNil())
		Expect(st.FPGA).NotTo(BeNil())
		Expect(st.LargeMem).NotTo(BeNil())
		Expect(st.GPU.LeftNodeMinutes).To(Equal(int64(600)))
		Expect(st.CPU.LeftNodeMinutes).To(BeZero(), "the spent allocation shows nothing left")
		Expect(st.FPGA.BookedNodeMinutes).To(Equal(int64(120)))
		Expect(st.FPGA.LeftNodeMinutes).To(Equal(int64(180)))
		Expect(st.LargeMem.BookedNodeMinutes).To(Equal(int64(60)))
	})

	It("holds a step only until its own allocation settles, and settles each allocation apart", func() {
		first := submit("meluxina-fpga", 240) // 240 of 300 FPGA minutes booked
		second := submit("meluxina-fpga", 120)
		Expect(second.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(second.Status.Reason).To(Equal(hpcv1alpha1.ReasonWaitingForBudget))
		Expect(second.Status.Message).To(ContainSubstring("FPGA allocation"))
		Expect(submit("meluxina-largemem", 240).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "large memory is another allocation")

		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(first), got)).To(Succeed())
		orig := got.DeepCopy()
		start, end, zero := metav1.NewTime(time.Now().Add(-30*time.Minute)), metav1.NewTime(time.Now()), int32(0)
		got.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "100", State: "COMPLETED", ExitCode: &zero,
			StartTime: &start, EndTime: &end, Terminal: true}
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
		Expect(reconcileOnce(r, first).Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(reconcileOnce(r, second).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))

		Expect(r.publishLedger(ctx, siteNow())).To(Succeed())
		st := siteNow().Status
		Expect(st.FPGA.UsedNodeMinutes).To(Equal(int64(30)))
		Expect(st.GPU.UsedNodeMinutes).To(BeZero())
		Expect(st.CPU.UsedNodeMinutes).To(BeZero())
		Expect(st.LargeMem.UsedNodeMinutes).To(BeZero(), "the FPGA settlement never touches another allocation")
		Expect(st.LargeMem.BookedNodeMinutes).To(Equal(int64(240)))
	})
})

// reconcileOnce reconciles the job once and returns it as stored.
func reconcileOnce(r *HPCArgoWorkflowJobReconciler, j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
	Expect(err).NotTo(HaveOccurred())
	got := &hpcv1alpha1.HPCArgoWorkflowJob{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
	return got
}

func i64p(v int64) *int64 { return &v }
