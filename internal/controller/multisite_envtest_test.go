package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
)

// PRD-HPC-1461 AC-15: a project binds two sites. Each job finds the site that offers its class; the ledger,
// the budget, the limits and the credentials stay per HPCSite; a class both sites offer is refused, not guessed.
var _ = Describe("a project with two HPC sites", func() {
	var (
		r       *HPCArgoWorkflowJobReconciler
		project string
		envNS   string
		hpcNS   string
		seq     int
	)

	site := func(name, class, prefix string, gpuHours int64) *hpcv1alpha1.HPCSite {
		return &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: name,
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api." + name + ".example", RESTVersion: "v0.0.44", ScratchRoot: "/scratch/" + name,
					Classes: []hpcv1alpha1.SiteClass{{Name: class, Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u-" + name, Account: "acct-" + name, CredentialKeyPrefix: prefix,
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: gpuHours}},
		}
	}

	newJob := func(class string, hours int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("hpc-ms-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: class, Image: "reg/app:v1", WallTimeMinutes: hours * 60,
				Command: []string{"python", "train.py"}, StepName: "train", Outputs: []string{"model"},
				Env: []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}},
		}
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

	BeforeEach(func() {
		seq = 0
		project = fmt.Sprintf("ms%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test",
			Health: sitestate.NewHealth(), SiteOutageMax: 30 * time.Minute, Now: time.Now}
	})

	It("should_admit_each_job_on_the_site_that_offers_its_class_and_book_it_there_only", func() {
		Expect(k8sClient.Create(ctx, site("alpha", "alpha-gpu", "", 100))).To(Succeed())
		Expect(k8sClient.Create(ctx, site("beta", "beta-gpu", "beta.", 100))).To(Succeed())
		a, b := reconcileOnce(newJob("alpha-gpu", 2)), reconcileOnce(newJob("beta-gpu", 3))

		Expect(a.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(a.Status.Booking.SiteName).To(Equal("alpha"))
		Expect(a.Status.Booking.NodeMinutes).To(BeEquivalentTo(120))
		Expect(b.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(b.Status.Booking.SiteName).To(Equal("beta"))
		Expect(b.Status.Booking.NodeMinutes).To(BeEquivalentTo(180))

		// The ledgers are per HPCSite: each holds only its own booking.
		for name, want := range map[string]int64{"alpha": 120, "beta": 180} {
			s := &hpcv1alpha1.HPCSite{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: hpcNS, Name: name}, s)).To(Succeed())
			l, err := r.ledgerOf(ctx, s)
			Expect(err).NotTo(HaveOccurred())
			Expect(l.Tiers[hpcv1alpha1.TierGPU].Booked).To(Equal(want), "site %s", name)
			Expect(l.Active).To(BeEquivalentTo(1), "site %s", name)
		}
	})

	It("should_keep_one_sites_spent_budget_from_blocking_the_other_site", func() {
		Expect(k8sClient.Create(ctx, site("alpha", "alpha-gpu", "", 3))).To(Succeed()) // 3 node-hours
		Expect(k8sClient.Create(ctx, site("beta", "beta-gpu", "beta.", 100))).To(Succeed())
		Expect(reconcileOnce(newJob("alpha-gpu", 2)).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		over := reconcileOnce(newJob("alpha-gpu", 2)) // 2 booked + 2 asked > 3
		Expect(over.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(over.Status.Reason).To(Equal(hpcv1alpha1.ReasonWaitingForBudget))
		Expect(reconcileOnce(newJob("beta-gpu", 2)).Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
	})

	It("should_refuse_a_class_both_sites_offer_with_AmbiguousSite_and_book_nothing", func() {
		Expect(k8sClient.Create(ctx, site("alpha", "shared-gpu", "", 100))).To(Succeed())
		Expect(k8sClient.Create(ctx, site("beta", "shared-gpu", "beta.", 100))).To(Succeed())
		got := reconcileOnce(newJob("shared-gpu", 1))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonAmbiguousSite))
		Expect(got.Status.Message).To(ContainSubstring("alpha, beta"))
		Expect(got.Status.Booking).To(BeNil())
	})

	It("should_keep_a_booked_job_on_its_site_when_a_second_site_later_offers_the_same_class", func() {
		Expect(k8sClient.Create(ctx, site("alpha", "shared-gpu", "", 100))).To(Succeed())
		got := reconcileOnce(newJob("shared-gpu", 1))
		Expect(got.Status.Booking.SiteName).To(Equal("alpha"))

		Expect(k8sClient.Create(ctx, site("beta", "shared-gpu", "beta.", 100))).To(Succeed())
		got = reconcileOnce(got)
		Expect(got.Status.Reason).NotTo(Equal(hpcv1alpha1.ReasonAmbiguousSite))
		Expect(got.Status.Booking).NotTo(BeNil())
		Expect(got.Status.Booking.SiteName).To(Equal("alpha"), "adding a site never moves a booked job")
	})

	It("should_spawn_the_runner_with_the_second_sites_own_credential_keys", func() {
		Expect(k8sClient.Create(ctx, site("alpha", "alpha-gpu", "", 100))).To(Succeed())
		Expect(k8sClient.Create(ctx, site("beta", "beta-gpu", "beta.", 100))).To(Succeed())
		got := reconcileOnce(newJob("beta-gpu", 1))
		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(hpcNS), client.MatchingLabels{runnerjob.LabelJobUID: string(got.UID)})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
		pod := jobs.Items[0].Spec.Template.Spec
		var tokenKey string
		for _, e := range pod.Containers[0].Env {
			if e.Name == "SLURM_TOKEN" {
				tokenKey = e.ValueFrom.SecretKeyRef.Key
			}
			if e.Name == "HPC_USER" {
				Expect(e.Value).To(Equal("u-beta"))
			}
		}
		Expect(tokenKey).To(Equal("beta.token"))
	})
})
