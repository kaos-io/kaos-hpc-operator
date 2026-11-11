package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
)

// PRD-HPC-1461 AC-28, AC-30: a site that does not answer is a WAIT. Nothing is admitted or spawned for it,
// the job shows Waiting/SiteUnavailable, and it resumes when the site answers; a long outage ends it with a clear reason.
var _ = Describe("HPCArgoWorkflowJob waits for an unreachable site", func() {
	var (
		r       *HPCArgoWorkflowJobReconciler
		health  *sitestate.Health
		clock   time.Time
		project string
		envNS   string
		hpcNS   string
		siteKey string
		seq     int
	)

	siteDown := func() {
		for i := range sitestate.FailuresToDown {
			health.Observe(siteKey, sitestate.Down, "HTTP 503", clock.Add(time.Duration(i)*sitestate.MinDownAfter))
		}
	}
	siteUp := func() { health.Observe(siteKey, sitestate.Up, "", clock) }

	newJob := func() *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("hpc-su-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "reg.example/app:v1", WallTimeMinutes: 60,
				Command: []string{"python", "train.py"}, StepName: "train", Outputs: []string{"model"},
				Env: []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}},
		}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) (reconcile.Result, *hpcv1alpha1.HPCArgoWorkflowJob) {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return res, got
	}

	runnerJobs := func(j *hpcv1alpha1.HPCArgoWorkflowJob) []batchv1.Job {
		var all batchv1.JobList
		Expect(k8sClient.List(ctx, &all, client.InNamespace(hpcNS), client.MatchingLabels{runnerjob.LabelJobUID: string(j.UID)})).To(Succeed())
		return all.Items
	}

	BeforeEach(func() {
		seq = 0
		project = fmt.Sprintf("su%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44", ScratchRoot: "/project/scratch/p1",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 100}},
		})).To(Succeed())
		siteKey = types.NamespacedName{Namespace: hpcNS, Name: "meluxina"}.String()
		health, clock = sitestate.NewHealth(), time.Now()
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test",
			Health: health, SiteOutageMax: 30 * time.Minute, Now: func() time.Time { return clock }}
	})

	It("should_wait_with_reason_SiteUnavailable_and_book_nothing_when_the_site_is_down_before_admission", func() {
		siteDown()
		j := newJob()
		res, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		Expect(got.Status.Message).To(ContainSubstring("not answering"))
		Expect(got.Status.Booking).To(BeNil(), "no node-minutes are held for a job that cannot start")
		Expect(meta.IsStatusConditionTrue(got.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable)).To(BeTrue())
		Expect(res.RequeueAfter).To(Equal(WaitingResync))
		Expect(runnerJobs(got)).To(BeEmpty(), "no runner Job is spawned while the site is down")
	})

	It("should_admit_and_spawn_the_runner_when_the_site_answers_again", func() {
		siteDown()
		j := newJob()
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))

		siteUp()
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonAdmitted))
		Expect(meta.FindStatusCondition(got.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable)).To(BeNil())
		Expect(runnerJobs(got)).To(HaveLen(1))
	})

	It("should_spawn_no_runner_after_one_failed_probe_and_spawn_it_after_the_next_good_one", func() {
		health.Observe(siteKey, sitestate.Down, "HTTP 503", clock)
		j := newJob()
		res, got := reconcileOnce(j)
		Expect(runnerJobs(got)).To(BeEmpty(), "nothing new starts against a site whose latest probe failed")
		Expect(res.RequeueAfter).To(Equal(FailingSiteResync))
		Expect(got.Status.Reason).NotTo(Equal(hpcv1alpha1.ReasonSiteUnavailable), "one failed probe is a blip: the job is not told the site is down")
		Expect(meta.FindStatusCondition(got.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable)).To(BeNil())

		siteUp()
		_, got = reconcileOnce(j)
		Expect(runnerJobs(got)).To(HaveLen(1))
	})

	It("should_keep_an_admitted_job_booked_but_spawn_no_runner_while_the_site_is_down", func() {
		j := newJob()
		_, got := reconcileOnce(j)
		rjs := runnerJobs(got)
		Expect(rjs).To(HaveLen(1))
		Expect(k8sClient.Delete(ctx, &rjs[0], client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Eventually(func() []batchv1.Job { return runnerJobs(got) }).Should(BeEmpty())

		siteDown()
		res, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		Expect(got.Status.Booking).NotTo(BeNil())
		Expect(runnerJobs(got)).To(BeEmpty())
		Expect(res.RequeueAfter).To(Equal(WaitingResync))

		siteUp()
		_, got = reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonAdmitted))
		Expect(runnerJobs(got)).To(HaveLen(1))
	})

	It("should_fail_a_waiting_job_with_SiteUnavailable_after_the_outage_bound", func() {
		siteDown()
		j := newJob()
		reconcileOnce(j)
		clock = clock.Add(29 * time.Minute)
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting), "still inside the bound")

		clock = clock.Add(2 * time.Minute)
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		Expect(got.Status.Message).To(ContainSubstring("did not answer for more than 30m0s"))
		Expect(meta.FindStatusCondition(got.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable)).To(BeNil())
	})

	It("should_fail_an_admitted_job_without_a_runner_after_the_bound_and_release_its_booking", func() {
		j := newJob()
		_, got := reconcileOnce(j)
		rjs := runnerJobs(got)
		Expect(k8sClient.Delete(ctx, &rjs[0], client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Eventually(func() []batchv1.Job { return runnerJobs(got) }).Should(BeEmpty())
		siteDown()
		reconcileOnce(j)
		clock = clock.Add(31 * time.Minute)
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		Expect(got.Status.Booking).To(BeNil())
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Attempts[0].SettledNodeMinutes).To(BeZero(), "nothing ran, nothing is charged")
	})

	It("should_never_hold_a_job_that_already_has_a_site_job", func() {
		j := newJob()
		_, got := reconcileOnce(j)
		orig := got.DeepCopy()
		got.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "4711", State: "RUNNING"}
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
		siteDown()
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning), "a running job is the poller's and the runner's business")
		Expect(got.Status.Reason).NotTo(Equal(hpcv1alpha1.ReasonSiteUnavailable))
	})

	It("should_show_the_wait_of_a_runner_that_reports_SiteUnavailable_but_not_fail_it", func() {
		j := newJob()
		_, got := reconcileOnce(j)
		orig := got.DeepCopy()
		got.Status.PendingReason = hpcv1alpha1.ReasonSiteUnavailable
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
		clock = clock.Add(2 * time.Hour)
		_, got = reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "the runner's own bound ends it, not the controller's")
		Expect(runnerJobs(got)).To(HaveLen(1))
	})

	It("should_end_the_attempt_with_SiteUnavailable_when_the_runner_gives_up_waiting", func() {
		j := newJob()
		_, got := reconcileOnce(j)
		rj := runnerJobs(got)[0]
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "su-runner-pod", Namespace: hpcNS,
			Labels: map[string]string{runnerjob.LabelRunnerJob: rj.Name}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "reg.example/img:1"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "runner", State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "SiteUnavailable: the site did not answer for 30 min (last error: HTTP 503)"}}}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		now := metav1.Now()
		rj.Status.StartTime = &now
		rj.Status.Failed = 1
		rj.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
		Expect(k8sClient.Status().Update(ctx, &rj)).To(Succeed())

		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable), "not RunnerFailed")
		Expect(got.Status.Message).To(ContainSubstring("did not answer for 30 min"))
	})

	It("should_pass_the_outage_bound_to_the_runner_and_extend_its_deadline", func() {
		r.SiteOutageMax = 45 * time.Minute
		j := newJob()
		_, got := reconcileOnce(j)
		rj := runnerJobs(got)[0]
		env := map[string]string{}
		for _, e := range rj.Spec.Template.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		Expect(env).To(HaveKeyWithValue("HPC_SITE_WAIT_MAX_S", "2700"))
		Expect(*rj.Spec.ActiveDeadlineSeconds).To(Equal(int64((60+480+30)*60 + 2700)))
	})

	It("should_publish_the_SiteReachable_condition_on_the_HPCSite", func() {
		siteDown()
		j := newJob()
		reconcileOnce(j)
		var site hpcv1alpha1.HPCSite
		Eventually(func() *metav1.Condition {
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: "meluxina"}, &site)).To(Succeed())
			return meta.FindStatusCondition(site.Status.Conditions, hpcv1alpha1.ConditionSiteReachable)
		}).ShouldNot(BeNil())
		c := meta.FindStatusCondition(site.Status.Conditions, hpcv1alpha1.ConditionSiteReachable)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(hpcv1alpha1.ReasonSiteNotAnswering))
		Expect(site.Status.RecentJobs).NotTo(BeEmpty())
		Expect(site.Status.RecentJobs[0].Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable), "the console lists the job as SiteUnavailable")
	})
})
