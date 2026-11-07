package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// Extreme cases of the controller (docs/extreme-cases.md): the budget under pressure, a restart in the middle
// of a booking, the project or HPCSite disappearing under a running job, a lost shared-node host, a burst of
// jobs for the one-job lanes. Real API server (envtest); the site and the runner are played by status writes.
var _ = Describe("HPCArgoWorkflowJob extreme cases", func() {
	var (
		r       *HPCArgoWorkflowJobReconciler
		project string
		envNS   string
		hpcNS   string
		seq     int
	)

	laneCandidates := []lanes.Candidate{
		{QOS: "test"},
		{QOS: "dev", ReservationByPartition: map[string]string{"gpu": "gpudev"}},
	}

	newNS := func(name string) {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name,
			Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
	}

	newSite := func(gpuHours int64) {
		end := metav1.NewTime(time.Now().Add(24 * time.Hour))
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Sharing: true,
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44", ScratchRoot: "/project/scratch/p1",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}},
					QOS: []hpcv1alpha1.SiteQOS{{Name: "default", MaxWallTimeMinutes: 2880},
						{Name: "test", MaxWallTimeMinutes: 30, MaxJobsPerUser: 1},
						{Name: "dev", MaxWallTimeMinutes: 360, MaxJobsPerUser: 1}},
					Reservations: []hpcv1alpha1.SiteReservation{{Name: "gpudev", Partition: "gpu", NodeCount: 5, EndTime: &end}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: project, Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: gpuHours}},
		})).To(Succeed())
	}

	// newJob creates a phase-0 job (no runner Job) or, with serve, a serve-mode job.
	newJob := func(serve bool, wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		spec := hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: fmt.Sprintf("reg/x/app%d:v1", seq), WallTimeMinutes: wall}
		if serve {
			spec.StepName, spec.Command = "train", []string{"python", "train.py"}
			spec.Env = []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}
		}
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("x-%d", seq), Namespace: envNS}, Spec: spec}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	get := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)
		if err != nil {
			Expect(client.IgnoreNotFound(err)).To(Succeed())
			return nil
		}
		return got
	}

	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) (reconcile.Result, *hpcv1alpha1.HPCArgoWorkflowJob) {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		return res, get(j)
	}

	report := func(j *hpcv1alpha1.HPCArgoWorkflowJob, run hpcv1alpha1.RunStatus) {
		got := get(j)
		orig := got.DeepCopy()
		got.Status.Run = &run
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
	}

	siteStatus := func() hpcv1alpha1.HPCSiteStatus {
		s := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, s)).To(Succeed())
		return s.Status
	}

	// ended reports a finished attempt of exactly the given minutes, ending now.
	ended := func(j *hpcv1alpha1.HPCArgoWorkflowJob, state string, minutes int) {
		end := metav1.NewTime(time.Now().Truncate(time.Second))
		start := metav1.NewTime(end.Add(-time.Duration(minutes) * time.Minute))
		zero := int32(0)
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "700", State: state, ExitCode: &zero,
			StartTime: &start, EndTime: &end, Terminal: true})
	}

	// gc plays the garbage collector envtest lacks: it finishes a Foreground deletion.
	gc := func(rj *batchv1.Job) {
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rj), rj)).To(Succeed())
		Expect(rj.DeletionTimestamp).NotTo(BeNil(), "the runner Job must be deleted")
		controllerutil.RemoveFinalizer(rj, metav1.FinalizerDeleteDependents)
		Expect(k8sClient.Update(ctx, rj)).To(Succeed())
	}

	runnerJob := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *batchv1.Job {
		rj := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.Name(j.UID, 1)}, rj)).To(Succeed())
		return rj
	}

	newReconciler := func() *HPCArgoWorkflowJobReconciler {
		return &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test"}
	}

	BeforeEach(func() {
		seq = 0
		project = fmt.Sprintf("xt%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		newNS(envNS)
		newNS(hpcNS)
		r = newReconciler()
	})

	// ---------------------------------------------------------------- 1. budget

	Context("budget", func() {
		It("should_admit_up_to_the_last_node_minute_and_wait_for_one_more", func() {
			newSite(4) // 240 node-minutes
			a, b := newJob(false, 120), newJob(false, 120)
			_, ga := reconcileOnce(a)
			_, gb := reconcileOnce(b)
			Expect(ga.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			Expect(gb.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "120 + 120 is exactly the share")
			c := newJob(false, 1)
			_, gc1 := reconcileOnce(c)
			Expect(gc1.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting), "one minute more than the share")
			Expect(gc1.Status.Reason).To(Equal(hpcv1alpha1.ReasonWaitingForBudget))
			Expect(siteStatus().GPU.LeftNodeMinutes).To(BeZero())

			ended(a, "COMPLETED", 100) // a used 100 of its 120: 20 are free
			reconcileOnce(a)
			_, gc1 = reconcileOnce(c)
			Expect(gc1.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "the freed minutes go to the waiting job")
			c2, c3 := newJob(false, 20), newJob(false, 19)
			_, g2 := reconcileOnce(c2)
			_, g3 := reconcileOnce(c3)
			Expect(g2.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting), "19 minutes are left, not 20")
			Expect(g3.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "and exactly 19 fit")
			st := siteStatus()
			Expect(st.GPU.UsedNodeMinutes).To(Equal(int64(100)))
			Expect(st.GPU.LeftNodeMinutes).To(BeZero())
		})

		It("should_refuse_new_work_once_the_share_is_spent_but_never_touch_a_job_already_admitted", func() {
			newSite(1) // 60 node-minutes
			a, d := newJob(false, 30), newJob(false, 30)
			reconcileOnce(a)
			reconcileOnce(d)
			f := newJob(false, 30)
			_, gf := reconcileOnce(f)
			Expect(gf.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))

			ended(a, "COMPLETED", 60) // a ran twice its booking: the whole share is used
			reconcileOnce(a)

			_, gd := reconcileOnce(d)
			Expect(gd.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "running jobs are never cancelled for budget (CON-05)")
			Expect(gd.Status.Booking).NotTo(BeNil())
			_, gf = reconcileOnce(f)
			Expect(gf.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused), "the waiting job can never fit this period")
			Expect(gf.Status.Reason).To(Equal(hpcv1alpha1.ReasonBudgetBlocked))
			_, ge := reconcileOnce(newJob(false, 1))
			Expect(ge.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
			Expect(ge.Status.Reason).To(Equal(hpcv1alpha1.ReasonBudgetBlocked))
		})

		It("should_never_overbook_a_burst_of_forty_jobs_through_a_lagging_cache_and_a_restart", func() {
			newSite(10) // 600 node-minutes
			lag := &hidingClient{Client: k8sClient, hidden: map[types.UID]bool{}}
			r.Client = lag
			walls := []int32{30, 45, 60, 90, 120, 240}
			var jobs []*hpcv1alpha1.HPCArgoWorkflowJob
			left, want := int32(600), map[string]bool{}
			for i := range 40 {
				wall := walls[i%len(walls)]
				j := newJob(false, wall)
				jobs = append(jobs, j)
				if wall <= left {
					left -= wall
					want[j.Name] = true
				}
				if i == 20 {
					// The controller restarts: it forgets its own bookings, and its cache starts out synced.
					r = newReconciler()
					r.Client = lag
					lag.hidden = map[types.UID]bool{}
				}
				lag.hidden[j.UID] = true // the cache never shows this process's own write for j
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
				Expect(err).NotTo(HaveOccurred())
			}
			var booked int64
			for _, j := range jobs {
				got := get(j)
				if got.Status.Phase == hpcv1alpha1.PhaseAdmitted {
					booked += got.Status.Booking.NodeMinutes
					Expect(want).To(HaveKey(j.Name), "admitted although the share was already spoken for")
				} else {
					Expect(got.Status.Booking).To(BeNil())
					Expect(want).NotTo(HaveKey(j.Name), "left waiting although it fitted")
				}
			}
			Expect(booked).To(Equal(int64(600 - left)))
			Expect(booked).To(BeNumerically("<=", 600))
		})

		It("should_book_once_when_the_status_write_fails_and_the_reconcile_is_retried", func() {
			newSite(10)
			fail := true
			wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			r.Client = interceptor.NewClient(wc, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
					patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if _, ok := obj.(*hpcv1alpha1.HPCArgoWorkflowJob); ok && fail {
						fail = false
						return errors.New("the API server went away")
					}
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				}})
			j := newJob(false, 240)
			before := testutil.ToFloat64(metrics.NodeMinutesBooked.WithLabelValues("gpu", project))
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
			Expect(err).To(HaveOccurred())
			Expect(get(j).Status.Booking).To(BeNil(), "nothing was booked")
			Expect(r.isPending(string(j.UID))).To(BeFalse(), "and the failed booking is not remembered")
			Expect(testutil.ToFloat64(metrics.NodeMinutesBooked.WithLabelValues("gpu", project))).To(Equal(before))

			_, got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			reconcileOnce(j)
			reconcileOnce(j)
			Expect(siteStatus().GPU.BookedNodeMinutes).To(Equal(int64(240)), "booked once, not once per attempt")
			Expect(testutil.ToFloat64(metrics.NodeMinutesBooked.WithLabelValues("gpu", project)) - before).To(Equal(240.0))
		})

		It("should_keep_the_bookings_of_a_controller_that_restarted_between_two_admissions", func() {
			newSite(5) // 300 node-minutes
			_, a := reconcileOnce(newJob(false, 200))
			Expect(a.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			r = newReconciler() // a fresh process: the ledger is derived from the jobs, not from memory
			_, b := reconcileOnce(newJob(false, 101))
			Expect(b.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting), "200 are booked, only 100 are free")
			_, b = reconcileOnce(newJob(false, 100))
			Expect(b.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		})
	})

	// ---------------------------------------------------------------- 9. lifecycle

	Context("a running serve-mode job whose project binding disappears", func() {
		var clock time.Time
		runningJob := func() (*hpcv1alpha1.HPCArgoWorkflowJob, *batchv1.Job) {
			clock = time.Now()
			r.Now = func() time.Time { return clock }
			j := newJob(true, 60)
			_, got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			rj := runnerJob(got)
			start := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "800", State: siteStateRunning, StartTime: &start})
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning))
			return j, rj
		}
		deleteSite := func() {
			site := &hpcv1alpha1.HPCSite{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, site)).To(Succeed())
			Expect(k8sClient.Delete(ctx, site)).To(Succeed())
		}
		unlabel := func(on bool) {
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: envNS}, ns)).To(Succeed())
			if on {
				ns.Labels[hpcv1alpha1.ProjectLabel] = project
			} else {
				delete(ns.Labels, hpcv1alpha1.ProjectLabel)
			}
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		}
		expectWatchedAndUntouched := func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			_, got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning))
			Expect(got.Status.Booking).NotTo(BeNil(), "the booking is kept")
			Expect(got.Annotations).NotTo(HaveKey(hpcv1alpha1.CancelRequestedAnnotation))
			Expect(runnerJob(got).DeletionTimestamp).To(BeNil(), "the runner keeps watching the site job")
		}
		// stopThroughTheRunner: the grace is over; the runner Job is deleted first, its SIGTERM cancels the
		// site job and reports it; the attempt is then settled and the booking released.
		stopThroughTheRunner := func(j *hpcv1alpha1.HPCArgoWorkflowJob, rj *batchv1.Job, why string) {
			res, got := reconcileOnce(j)
			Expect(res.RequeueAfter).To(Equal(CancelPoll), "the runner Job is deleted first")
			Expect(got.Annotations).To(HaveKey(hpcv1alpha1.CancelRequestedAnnotation))
			Expect(got.Status.Phase).NotTo(Equal(hpcv1alpha1.PhaseRefused), "a live site job is never abandoned as Refused")
			gc(rj)
			start := got.Status.Run.StartTime
			end := metav1.NewTime(clock)
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "800", State: "CANCELLED", StartTime: start, EndTime: &end, Terminal: true})
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
			Expect(got.Status.Message).To(ContainSubstring(why))
			Expect(got.Status.Booking).To(BeNil(), "the booking is released")
			Expect(got.Status.Attempts).To(HaveLen(1), "and the attempt is settled")
			Expect(got.Status.Attempts[0].SettledNodeMinutes).To(BeNumerically(">=", 10))
			_, got = reconcileOnce(j)
			Expect(got.Finalizers).NotTo(ContainElement(hpcv1alpha1.CancelFinalizer))
		}

		It("should_keep_a_training_untouched_when_the_hpcsite_is_recreated_within_the_grace_period", func() {
			newSite(100)
			j, _ := runningJob()
			deleteSite()
			res, got := reconcileOnce(j)
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteMissing))
			Expect(got.Status.Message).To(ContainSubstring("HPCSiteMissing since "))
			Expect(got.Status.SiteMissingSince).NotTo(BeNil())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			clock = clock.Add(DefaultSiteMissingGrace - time.Minute)
			expectWatchedAndUntouched(j)
			newSite(100) // GitOps put it back
			clock = clock.Add(30 * time.Second)
			expectWatchedAndUntouched(j)
			got = get(j)
			Expect(got.Status.SiteMissingSince).To(BeNil())
			Expect(got.Status.Reason).To(Equal("Submitted"))
			clock = clock.Add(time.Hour) // the old marker must not linger: a later loss starts a new grace period
			deleteSite()
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning))
			Expect(got.Status.SiteMissingSince.Time).To(BeTemporally("~", clock, time.Second))
		})

		It("should_cancel_settle_and_release_when_the_hpcsite_is_still_gone_after_the_grace_period_even_across_a_restart", func() {
			newSite(100)
			j, rj := runningJob()
			deleteSite()
			reconcileOnce(j) // first observation, persisted in status
			r = newReconciler()
			clock = clock.Add(DefaultSiteMissingGrace - time.Minute)
			r.Now = func() time.Time { return clock }
			expectWatchedAndUntouched(j) // the restarted controller did not reset the clock...
			clock = clock.Add(2 * time.Minute)
			stopThroughTheRunner(j, rj, "HPCSite") // ...so one minute later the grace is over
		})

		It("should_cancel_when_the_namespace_stays_without_its_project_label_after_the_grace_period", func() {
			newSite(100)
			j, rj := runningJob()
			unlabel(false)
			expectWatchedAndUntouched(j)
			clock = clock.Add(DefaultSiteMissingGrace + time.Minute)
			stopThroughTheRunner(j, rj, "project")
		})

		It("should_keep_the_job_when_the_project_label_comes_back_within_the_grace_period", func() {
			newSite(100)
			j, _ := runningJob()
			unlabel(false)
			expectWatchedAndUntouched(j)
			unlabel(true)
			clock = clock.Add(time.Minute)
			expectWatchedAndUntouched(j)
			Expect(get(j).Status.SiteMissingSince).To(BeNil())
		})

		It("should_refuse_a_job_that_was_never_booked_when_its_hpcsite_is_gone", func() {
			j := newJob(true, 60) // no site at all
			_, got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoSite))
		})
	})

	Context("the runner Job", func() {
		It("should_not_fail_the_attempt_while_the_runner_Job_replaces_failed_pods", func() {
			newSite(100)
			j := newJob(true, 60)
			_, got := reconcileOnce(j)
			rj := runnerJob(got)
			now := metav1.Now()
			rj.Status.StartTime = &now
			rj.Status.Failed = 2
			Expect(k8sClient.Status().Update(ctx, rj)).To(Succeed())
			before := testutil.ToFloat64(metrics.RunnerRestarts)
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			Expect(got.Status.Attempts).To(BeEmpty())
			Expect(got.Status.Booking).NotTo(BeNil())
			Expect(runnerJob(got).DeletionTimestamp).To(BeNil())
			Expect(testutil.ToFloat64(metrics.RunnerRestarts) - before).To(Equal(2.0))
		})
	})

	// ---------------------------------------------------------------- the orphan sweep (AC-08)

	Context("the orphan sweep", func() {
		var clock time.Time
		var rec *record.FakeRecorder
		// liveJob: admitted, submitted and RUNNING at the site since 15 minutes.
		liveJob := func() (*hpcv1alpha1.HPCArgoWorkflowJob, *batchv1.Job) {
			clock = time.Now()
			rec = record.NewFakeRecorder(20)
			r.Now, r.Recorder = func() time.Time { return clock }, rec
			j := newJob(true, 60)
			_, got := reconcileOnce(j)
			rj := runnerJob(got)
			start := metav1.NewTime(time.Now().Truncate(time.Second).Add(-15 * time.Minute))
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "801", State: siteStateRunning, StartTime: &start})
			reconcileOnce(j)
			return j, rj
		}
		failRunner := func(rj *batchv1.Job, reason string) {
			now := metav1.Now()
			rj.Status.StartTime = &now
			rj.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: reason},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: reason}}
			Expect(k8sClient.Status().Update(ctx, rj)).To(Succeed())
		}
		cancelRunner := func(j *hpcv1alpha1.HPCArgoWorkflowJob, n int32) *batchv1.Job {
			rj := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.CancelName(j.UID, 1, n)}, rj)).To(Succeed())
			return rj
		}
		// siteConfirms plays the cancel-only runner: it reports the site's final state.
		siteConfirms := func(j *hpcv1alpha1.HPCArgoWorkflowJob, state string, minutes int) {
			end := metav1.NewTime(time.Now().Truncate(time.Second))
			start := metav1.NewTime(end.Add(-time.Duration(minutes) * time.Minute))
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "801", State: state, StartTime: &start, EndTime: &end, Terminal: true})
		}

		It("should_stop_the_site_job_before_it_releases_the_booking_when_the_runner_Job_ran_out_of_time", func() {
			newSite(100)
			j, rj := liveJob()
			failRunner(rj, "DeadlineExceeded")
			_, got := reconcileOnce(j)
			Expect(got.Status.Sweep).NotTo(BeNil())
			Expect(got.Status.Booking).NotTo(BeNil(), "the booking is kept: the site job may still run")
			Expect(got.Status.Phase.Terminal()).To(BeFalse())
			Expect(got.Status.Message).To(ContainSubstring("DeadlineExceeded"))
			reconcileOnce(j)
			c := cancelRunner(j, 0)
			Expect(c.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{"--cancel-only"}))
			Expect(c.Labels).To(HaveKeyWithValue(runnerjob.LabelCancelOnly, "true"))
			Expect(c.Annotations).To(HaveKeyWithValue(runnerjob.AnnotationJobName, j.Name))
			Expect(get(j).Status.Booking).NotTo(BeNil())

			siteConfirms(j, "CANCELLED", 15)
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRunnerFailed))
			Expect(got.Status.Message).To(And(ContainSubstring("DeadlineExceeded"), ContainSubstring("the site confirmed")))
			Expect(got.Status.Booking).To(BeNil())
			Expect(got.Status.Sweep).To(BeNil())
			Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(15)), "the time the site job ran is charged")
		})

		It("should_prove_a_cancel_requested_after_the_runner_is_gone", func() {
			newSite(100)
			j, rj := liveJob()
			// the runner Job vanished (node loss, manual delete) without a terminal report
			Expect(k8sClient.Delete(ctx, rj, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			Expect(k8sClient.Delete(ctx, get(j))).To(Succeed())
			_, got := reconcileOnce(j)
			Expect(got.Status.Sweep).NotTo(BeNil(), "no 'Cancelled' on the runner's absence alone")
			Expect(got.Status.Phase.Terminal()).To(BeFalse())
			reconcileOnce(j)
			cancelRunner(j, 0)
			siteConfirms(j, "CANCELLED", 15)
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
			Expect(got.Status.Message).To(ContainSubstring("the site confirmed"))
			Expect(got.Status.Booking).To(BeNil())
			Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(15)))
			reconcileOnce(j)
			Expect(get(j)).To(BeNil(), "the finalizer is dropped once the cancel is proven")
		})

		It("should_keep_the_booking_while_the_site_stays_silent_and_decide_without_proof_only_after_the_give_up_time", func() {
			newSite(100)
			j, rj := liveJob()
			failRunner(rj, "BackoffLimitExceeded")
			reconcileOnce(j)
			reconcileOnce(j)
			// the first cancel-only runner could not reach the site either
			failRunner(cancelRunner(j, 0), "BackoffLimitExceeded")
			clock = clock.Add(time.Hour)
			reconcileOnce(j)
			_, got := reconcileOnce(j)
			Expect(got.Status.Sweep.Launches).To(Equal(int32(1)))
			cancelRunner(j, 1) // the next one is running
			Expect(got.Status.Booking).NotTo(BeNil(), "an hour in, still no proof: the booking stays")
			Expect(got.Status.Phase.Terminal()).To(BeFalse())

			clock = clock.Add(DefaultSweepGiveUp)
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRunnerFailed))
			Expect(got.Status.Message).To(And(ContainSubstring("did not confirm"), ContainSubstring("801 may still be running")))
			Expect(got.Status.Booking).To(BeNil())
			Expect(*got.Status.SettledNodeMinutes).To(BeNumerically(">=", int64(DefaultSweepGiveUp.Minutes())), "settled up to now: conservative")
			var events []string
			for len(rec.Events) > 0 {
				events = append(events, <-rec.Events)
			}
			Expect(events).To(ContainElement(ContainSubstring("SiteJobUnconfirmed")))
		})

		It("should_not_start_a_sweep_when_no_site_job_is_known", func() {
			newSite(100)
			clock = time.Now()
			j := newJob(true, 60)
			_, got := reconcileOnce(j)
			failRunner(runnerJob(got), "BackoffLimitExceeded")
			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Sweep).To(BeNil())
		})
	})

	// ---------------------------------------------------------------- 4. node sharing

	Context("node sharing", func() {
		It("should_settle_a_host_lost_to_node_fail_from_its_reported_run", func() {
			newSite(100)
			host := newJob(false, 120)
			reconcileOnce(host)
			ended(host, "NODE_FAIL", 50)
			_, got := reconcileOnce(host)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Reason).To(Equal("SiteNODE_FAIL"))
			Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(50)), "the run, not the 120 booked")
			Expect(got.Status.Booking).To(BeNil())
			Expect(siteStatus().GPU.UsedNodeMinutes).To(Equal(int64(50)))
			Expect(siteStatus().GPU.BookedNodeMinutes).To(BeZero())
		})

		It("should_book_a_guest_again_when_its_host_is_lost_and_charge_it_by_its_own_run", func() {
			newSite(100)
			g := newJob(false, 60)
			reconcileOnce(g)
			report(g, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "G1", State: "PENDING", HostSiteJobID: "H", Slot: ptrInt32(1)})
			reconcileOnce(g)
			st := siteStatus()
			Expect(st.GPU.BookedNodeMinutes).To(BeZero(), "taken by a host: the host's booking covers the node")
			Expect(st.ActiveJobs).To(Equal(int32(1)))
			Expect(st.RunningJobs).To(Equal(int32(1)))

			// The host died with the guest on it; the guest's runner started the job over alone.
			start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
			report(g, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "G2", State: siteStateRunning, StartTime: &start})
			reconcileOnce(g)
			st = siteStatus()
			Expect(st.GPU.BookedNodeMinutes).To(Equal(int64(60)), "alone again: its own booking counts again")
			Expect(st.ActiveJobs).To(Equal(int32(1)))

			end := metav1.NewTime(time.Now().Truncate(time.Second))
			start = metav1.NewTime(end.Add(-30 * time.Minute))
			zero := int32(0)
			report(g, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "G2", State: "COMPLETED", ExitCode: &zero,
				StartTime: &start, EndTime: &end, Terminal: true})
			_, got := reconcileOnce(g)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
			Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(30)))
			st = siteStatus()
			Expect(st.GPU.UsedNodeMinutes).To(Equal(int64(30)))
			Expect(st.GPU.BookedNodeMinutes).To(BeZero())
		})

		It("should_charge_nothing_for_a_guest_that_ended_on_a_host_whose_node_then_failed", func() {
			newSite(100)
			g := newJob(false, 60)
			reconcileOnce(g)
			start := metav1.NewTime(time.Now().Add(-20 * time.Minute))
			end := metav1.NewTime(time.Now())
			zero := int32(0)
			report(g, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "G1", State: "COMPLETED", ExitCode: &zero, HostSiteJobID: "H",
				StartTime: &start, EndTime: &end, Terminal: true})
			_, got := reconcileOnce(g)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
			Expect(*got.Status.SettledNodeMinutes).To(BeZero(), "the host's booking and settlement cover the node")
		})
	})

	// ---------------------------------------------------------------- 5/7. lanes under load

	Context("lanes", func() {
		It("should_hand_each_one_job_lane_out_once_in_a_burst_the_cache_cannot_see", func() {
			newSite(1000)
			lag := &laggingClient{Client: k8sClient}
			r = newReconciler()
			r.Client, r.LaneCandidates = lag, laneCandidates
			var jobs []*hpcv1alpha1.HPCArgoWorkflowJob
			for range 12 {
				j := newJob(true, 20)
				jobs = append(jobs, j)
				lag.freeze = true
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
				Expect(err).NotTo(HaveOccurred())
			}
			count := map[string]int{}
			for _, j := range jobs {
				got := get(j)
				Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
				count[got.Status.Placement.QOS]++
			}
			Expect(count).To(Equal(map[string]int{"test": 1, "dev": 1, "": 10}),
				"each lane allows one job per site user, whatever the cache shows")
		})

		It("should_never_place_a_job_on_a_lane_the_profile_no_longer_offers", func() {
			newSite(1000)
			site := &hpcv1alpha1.HPCSite{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, site)).To(Succeed())
			site.Spec.Profile.QOS = site.Spec.Profile.QOS[:1] // test and dev were removed from the account
			site.Spec.Profile.Reservations = nil
			Expect(k8sClient.Update(ctx, site)).To(Succeed())
			r.LaneCandidates = laneCandidates
			_, got := reconcileOnce(newJob(true, 20))
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			Expect(got.Status.Placement.QOS).To(BeEmpty(), "the class default QoS")
			Expect(got.Status.Placement.Reservation).To(BeEmpty())
		})
	})
})

func ptrInt32(v int32) *int32 { return &v }

// hidingClient lists HPCArgoWorkflowJobs without the status of the hidden ones: an informer cache that has not
// yet seen the writes of this process, while everything older (a previous process's bookings) is visible.
type hidingClient struct {
	client.Client
	hidden map[types.UID]bool
}

func (c *hidingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if jobs, ok := list.(*hpcv1alpha1.HPCArgoWorkflowJobList); ok {
		for i := range jobs.Items {
			if c.hidden[jobs.Items[i].UID] {
				jobs.Items[i].Status = hpcv1alpha1.HPCArgoWorkflowJobStatus{}
			}
		}
	}
	return nil
}
