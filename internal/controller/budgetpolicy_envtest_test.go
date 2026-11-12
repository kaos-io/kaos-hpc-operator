package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

// PRD-HPC-1461 AC-22 (at-limit policy, warning threshold), AC-23 (SiteLimit), AC-33 (InsufficientQuota),
// against a real API server so the CRD schema accepts what the controller writes.
var _ = Describe("HPC budget policy", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		rec          *record.FakeRecorder
		project      string
		envNS, hpcNS string
		seq          int
	)

	BeforeEach(func() {
		project = fmt.Sprintf("pol%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-hpc"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		rec = record.NewFakeRecorder(50)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
	})

	newSite := func(b hpcv1alpha1.Budget) {
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://slurm-api.example",
					RESTVersion: "v0.0.44", Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu",
						Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget:   b}})).To(Succeed())
	}
	submit := func(wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("step-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/img:1", WallTimeMinutes: wall}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}

	siteNow := func() *hpcv1alpha1.HPCSite {
		s := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, s)).To(Succeed())
		return s
	}

	events := func() []string {
		var out []string
		for {
			select {
			case e := <-rec.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	It("raises BudgetWarning (condition, event, metric) once used plus booked reach the threshold", func() {
		newSite(hpcv1alpha1.Budget{GPUNodeHours: 5, WarnAtPercent: 60}) // 300 node-minutes
		Expect(submit(120).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		c := meta.FindStatusCondition(siteNow().Status.Conditions, hpcv1alpha1.ConditionBudgetWarning)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse), "120 of 300 is 40%, below 60")
		Expect(c.Reason).To(Equal(hpcv1alpha1.ReasonBelowThreshold))
		Expect(events()).NotTo(ContainElement(ContainSubstring("BudgetWarning")))
		Expect(testutil.ToFloat64(metrics.BudgetWarning.WithLabelValues("meluxina", project, "gpu"))).To(Equal(0.0))

		Expect(submit(90).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted)) // 210 of 300 = 70%
		c = meta.FindStatusCondition(siteNow().Status.Conditions, hpcv1alpha1.ConditionBudgetWarning)
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(hpcv1alpha1.ReasonThresholdCrossed))
		Expect(c.Message).To(ContainSubstring("gpu"))
		Expect(events()).To(ContainElement(And(ContainSubstring("Warning BudgetWarning"), ContainSubstring("60%"))))
		Expect(testutil.ToFloat64(metrics.BudgetWarning.WithLabelValues("meluxina", project, "gpu"))).To(Equal(1.0))
		Expect(testutil.ToFloat64(metrics.BudgetPercent.WithLabelValues("meluxina", project, "gpu"))).To(Equal(70.0))
		// The same figure is in status (additive), and a tier with no allowance publishes -1.
		Expect(siteNow().Status.GPU.Percent).To(Equal(int32(70)))
		Expect(siteNow().Status.CPU.Percent).To(Equal(int32(-1)))

		// Another job and a resync while still above: no second event, the condition stays True.
		submit(30)
		Expect(r.publishLedger(ctx, siteNow())).To(Succeed())
		Expect(events()).NotTo(ContainElement(ContainSubstring("BudgetWarning")))
	})

	// AC-22 with source site: the figures are what is LEFT, so the warning reads against the month's grant
	// (spent before the observation + ended since + booked, over the grant). Admission still uses what is left.
	It("warns against the month's grant under source site, and clears when the period resets", func() {
		obs := metav1.NewTime(time.Now())
		gpuGrant := int64(10) // 600 node-minutes; 3 hours (180 minutes) are left, so 420 were spent before obs
		newSite(hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, GPUNodeHours: 3, ObservedAt: &obs,
			GrantNodeHours: &hpcv1alpha1.GrantNodeHours{GPU: &gpuGrant}, WarnAtPercent: 80})
		warning := func() *metav1.Condition {
			return meta.FindStatusCondition(siteNow().Status.Conditions, hpcv1alpha1.ConditionBudgetWarning)
		}

		Expect(submit(30).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted)) // (420+30)/600 = 75%
		Expect(warning().Status).To(Equal(metav1.ConditionFalse))
		Expect(testutil.ToFloat64(metrics.BudgetPercent.WithLabelValues("meluxina", project, "gpu"))).To(Equal(75.0))
		Expect(siteNow().Status.GPU.Percent).To(Equal(int32(75)), "status.gpu.percent is against the grant too")
		Expect(events()).NotTo(ContainElement(ContainSubstring("BudgetWarning")))

		Expect(submit(30).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted)) // (420+60)/600 = 80%
		Expect(warning().Status).To(Equal(metav1.ConditionTrue))
		Expect(warning().Reason).To(Equal(hpcv1alpha1.ReasonThresholdCrossed))
		Expect(events()).To(ContainElement(And(ContainSubstring("Warning BudgetWarning"), ContainSubstring("80%"))))
		Expect(testutil.ToFloat64(metrics.BudgetWarning.WithLabelValues("meluxina", project, "gpu"))).To(Equal(1.0))
		Expect(siteNow().Status.GPU.Percent).To(Equal(int32(80)))
		Expect(siteNow().Status.GPU.LeftNodeMinutes).To(Equal(int64(120)), "admission keeps counting the remainder: 180 - 60 booked")

		// The period resets: the site reports the new month's allocation (left = grant) and the jobs are
		// long over; what is still booked is all that counts.
		site := siteNow()
		reset := metav1.NewTime(time.Now())
		site.Spec.Budget.GPUNodeHours, site.Spec.Budget.ObservedAt = gpuGrant, &reset
		Expect(k8sClient.Update(ctx, site)).To(Succeed())
		Expect(r.publishLedger(ctx, siteNow())).To(Succeed())
		Expect(warning().Status).To(Equal(metav1.ConditionFalse), "60 booked of a 600 grant is 10%")
		Expect(warning().Reason).To(Equal(hpcv1alpha1.ReasonBelowThreshold))
		Expect(testutil.ToFloat64(metrics.BudgetWarning.WithLabelValues("meluxina", project, "gpu"))).To(Equal(0.0))
	})

	It("keeps the earlier warning base when the site gave no grant", func() {
		obs := metav1.NewTime(time.Now())
		newSite(hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, GPUNodeHours: 3, ObservedAt: &obs, WarnAtPercent: 80})
		Expect(submit(150).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted)) // 150 of 180 left = 83%
		c := meta.FindStatusCondition(siteNow().Status.Conditions, hpcv1alpha1.ConditionBudgetWarning)
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
	})

	It("blocks under the default policy and names the project share (BudgetBlocked)", func() {
		newSite(hpcv1alpha1.Budget{GPUNodeHours: 1})
		got := submit(120)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonBudgetBlocked))
	})

	It("admits beyond the share under warn and says so (OverShare event)", func() {
		newSite(hpcv1alpha1.Budget{GPUNodeHours: 1, AtLimit: hpcv1alpha1.AtLimitWarn})
		got := submit(120)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(events()).To(ContainElement(ContainSubstring("Warning OverShare")))
		c := meta.FindStatusCondition(siteNow().Status.Conditions, hpcv1alpha1.ConditionBudgetWarning)
		Expect(c.Status).To(Equal(metav1.ConditionTrue), "120 booked of a 60 minute share is over the threshold")
	})

	It("refuses with SiteLimit when the site's remainder, not the share, is what runs out", func() {
		obs := metav1.NewTime(time.Now())
		newSite(hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceShare, GPUNodeHours: 10,
			SiteLeft: &hpcv1alpha1.SiteLeft{GPUNodeHours: 1, ObservedAt: &obs}})
		got := submit(120)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteLimit))
		Expect(got.Status.Message).To(ContainSubstring("the site reports the account has"))
	})

	It("ends an attempt that reported InsufficientQuota as Failed with the runner's sentence", func() {
		newSite(hpcv1alpha1.Budget{GPUNodeHours: 5})
		j := submit(60)
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		code := hpcv1alpha1.ExitCodeInsufficientQuota
		got.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "FAILED", ExitCode: &code, Terminal: true,
			Message: "the dataset needs 90 GB but 10 GB are free"}
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonInsufficientQuota))
		Expect(got.Status.Message).To(Equal("the dataset needs 90 GB but 10 GB are free"))
		Expect(got.Status.SettledNodeMinutes).To(HaveValue(Equal(int64(0))))
		Expect(events()).To(ContainElement(ContainSubstring("Warning InsufficientQuota")))
	})
})
