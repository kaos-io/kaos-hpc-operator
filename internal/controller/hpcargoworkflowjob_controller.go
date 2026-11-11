/*
Copyright 2026 The KAOS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// WaitingResync is how often a Waiting job is judged again (bookings settle, limits free up).
const WaitingResync = 30 * time.Second

// ContinueResync is how soon the next attempt of a continued job is judged.
const ContinueResync = 100 * time.Millisecond

// HPCArgoWorkflowJobReconciler admits, books and settles HPCArgoWorkflowJobs against their project's HPCSite (phase 0,
// PRD-HPC-1461 F-26, D-12, D-24, D-49). Admission runs one job at a time (MaxConcurrentReconciles
// is 1), and the ledger is derived from the jobs themselves plus the bookings this process made
// that the cache may not show yet, so concurrent jobs never overbook.
type HPCArgoWorkflowJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Now is the clock; tests replace it.
	Now func() time.Time
	// RunnerImage is the image of the runner Jobs of serve-mode jobs (--runner-image).
	RunnerImage string
	// RunnerScheduling is where runner pods go (chart values pass it through).
	RunnerScheduling runnerjob.Scheduling
	// APIReader reads without the informer cache where a stale answer would be wrong (are the
	// runner Jobs gone?). Defaults to the client.
	APIReader client.Reader
	// LaneCandidates are the lanes (QoS, reservation) tried in order for serve-mode attempts
	// (PRD-HPC-1461 F-42, F-44). Empty: every attempt uses its class's own QoS, as before.
	LaneCandidates []lanes.Candidate
	// SiteMissingGrace is how long a booked job survives its HPCSite being gone (DefaultSiteMissingGrace).
	SiteMissingGrace time.Duration
	// SweepGiveUp is how long an orphan sweep waits for the site's confirmation (DefaultSweepGiveUp).
	SweepGiveUp time.Duration
	// Health is what the site poller knows about each site's reachability. Nil: a job never waits for a site.
	Health *sitestate.Health
	// SiteOutageMax is how long a job waits for an unreachable site before it fails (DefaultSiteOutageMax).
	SiteOutageMax time.Duration
	// LedgerPublishInterval is the shortest time between two writes of one HPCSite's status
	// (DefaultLedgerPublishInterval in the manager). A request inside the interval is not lost: the
	// newest state is written once the interval has passed. Zero (tests): every request writes.
	LedgerPublishInterval time.Duration
	// HostStartWait is how long a guest waits for a queued host before it is planned again (DefaultHostStartWait).
	HostStartWait time.Duration
	// GuestTakeWait is how long a guest waits on a running host that has not taken it (DefaultGuestTakeWait).
	GuestTakeWait time.Duration

	mu           sync.Mutex
	pending      map[string]hpcv1alpha1.Booking             // by job UID
	pendingShare map[string]*hpcv1alpha1.HPCArgoWorkflowJob // jobs this process placed in a shared node that the cache may not show yet, by job UID
	pendingLane  map[string]string                          // QoS of the pending bookings that were placed in a lane, by job UID
	laneNoted    map[string]int32                           // attempt whose refused lane was counted, by job UID
	failed       map[string]int32                           // failed runner pods seen, by runner Job UID
	ended        map[string]struct{}                        // jobs whose end this process wrote and whose ledger is not published yet, by job UID
	published    map[client.ObjectKey]time.Time             // when each HPCSite's status was last written (ledger_throttle.go)
	trailing     map[client.ObjectKey]bool                  // sites with a deferred write scheduled
}

// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcargoworkflowjobs,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=argoproj.io,resources=workflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcargoworkflowjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcsites,verbs=get;list;watch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcsites/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get,resourceNames=meluxina-jwt

// Reconcile judges a new or waiting job, follows an admitted one, and settles it when it ends.
// A job with spec.stepName set is in serve mode (phase 1): the controller also runs it through a
// runner Job per attempt and cancels it with its Workflow (serve.go). Without it, the job behaves
// as in phase 0.
func (r *HPCArgoWorkflowJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var job hpcv1alpha1.HPCArgoWorkflowJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		if apierrors.IsNotFound(err) {
			metrics.ForgetJob(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	serve := job.Spec.StepName != ""
	var site *hpcv1alpha1.HPCSite
	defer func() { r.observePhase(&job, site) }()

	if serve {
		if res, done, err := r.serveGate(ctx, &job); done {
			return res, err
		}
	}
	if job.Status.Phase.Terminal() {
		r.forget(string(job.UID))
		r.publishAfterEnd(ctx, &job)
		return r.resettleHostEnd(ctx, &job)
	}

	if serve && job.Status.Sweep != nil && job.Status.Booking != nil {
		// The attempt is ending but the site has not confirmed it: only the sweep acts (no normal settlement).
		site, _, _, _ = r.siteOf(ctx, &job)
		return r.continueSweep(ctx, &job, site)
	}
	var reason, msg string
	var err error
	site, reason, msg, err = r.siteOf(ctx, &job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if site == nil {
		if serve && job.Status.Booking != nil {
			// Already admitted: a site job may be queued or running under this booking. Refusing it here would
			// drop the booking while the site job lives on, unsettled and unwatched. A transient loss (a GitOps
			// prune and re-create) must not kill a training either: wait out the grace period, then cancel.
			return r.siteMissing(ctx, &job, msg)
		}
		return ctrl.Result{}, r.setPhase(ctx, &job, hpcv1alpha1.PhaseRefused, reason, msg, nil)
	}

	if err := r.siteBack(ctx, &job); err != nil {
		return ctrl.Result{}, err
	}
	switch {
	case job.Status.Booking != nil || r.isPending(string(job.UID)):
		// Admitted already (the cache may lag behind our own write): follow it.
		if job.Status.Booking == nil {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		next, err := r.followAndReplan(ctx, &job, site, serve)
		if err != nil {
			return ctrl.Result{}, err
		}
		if next {
			// A continuation: judge the next attempt right away.
			return ctrl.Result{RequeueAfter: ContinueResync}, nil
		}
	default:
		if res, done, err := r.judgeAndPublish(ctx, &job, site); done {
			return res, err
		}
	}
	if serve && job.Status.Booking != nil && !job.Status.Phase.Terminal() {
		if err := r.driveRunner(ctx, &job, site); err != nil {
			var wait *requeueError
			if errors.As(err, &wait) {
				return ctrl.Result{RequeueAfter: wait.after}, nil
			}
			return ctrl.Result{}, err
		}
	}
	if err := r.publishLedger(ctx, site); err != nil {
		log.Error(err, "publishing the ledger", "site", client.ObjectKeyFromObject(site))
	}
	if serve && !job.Status.Phase.Terminal() {
		// The Workflow is polled, not watched: it can be huge and is cached nowhere else.
		return ctrl.Result{RequeueAfter: ServeResync}, nil
	}
	return ctrl.Result{}, nil
}

// judgeAndPublish judges a new or waiting job. A job that is not admitted yet (or failed) ends this
// reconcile; a waiting one is listed (with its reason) in the site's recentJobs now, not at the next
// 10-minute resync (F-43).
func (r *HPCArgoWorkflowJobReconciler) judgeAndPublish(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (ctrl.Result, bool, error) {
	res, err := r.judge(ctx, job, site)
	if err != nil {
		return res, true, err
	}
	if res.RequeueAfter == 0 {
		return res, false, nil
	}
	if perr := r.publishLedger(ctx, site); perr != nil {
		logf.FromContext(ctx).Error(perr, "publishing the ledger", "site", client.ObjectKeyFromObject(site))
	}
	return res, true, nil
}

// siteOf resolves the job's project from its namespace label and the project's HPCSite that offers
// the job's class (3.11). Nothing in the job names a project or a site (T-02).
func (r *HPCArgoWorkflowJobReconciler) siteOf(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (*hpcv1alpha1.HPCSite, string, string, error) {
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: job.Namespace}, &ns); err != nil {
		return nil, "", "", err
	}
	project := ns.Labels[hpcv1alpha1.ProjectLabel]
	if project == "" {
		return nil, hpcv1alpha1.ReasonNoProject,
			"namespace " + job.Namespace + " belongs to no KAOS project (no " + hpcv1alpha1.ProjectLabel + " label)", nil
	}
	var sites hpcv1alpha1.HPCSiteList
	if err := r.List(ctx, &sites, client.MatchingLabels{hpcv1alpha1.ProjectLabel: project}); err != nil {
		return nil, "", "", err
	}
	for i := range sites.Items {
		if _, ok := admission.ClassOf(&sites.Items[i], job.Spec.Class); ok {
			return &sites.Items[i], "", "", nil
		}
	}
	return nil, hpcv1alpha1.ReasonNoSite, "project " + project + " has no HPC site offering class " + job.Spec.Class, nil
}

// judge decides a new or waiting job and books it when admitted.
func (r *HPCArgoWorkflowJobReconciler) judge(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) (ctrl.Result, error) {
	// An unreachable site is a wait, not a booking: nothing is admitted, and no runner is spawned, until it answers (AC-30).
	if held, err := r.siteWait(ctx, job, site, false); err != nil || held {
		return ctrl.Result{RequeueAfter: WaitingResync}, err
	}
	l, err := r.ledgerOf(ctx, site)
	if err != nil {
		return ctrl.Result{}, err
	}
	pl, err := r.placementFor(ctx, job, site)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Admission sees the placement just chosen, so a step verified resumable by earlier jobs of the
	// same step (pl.Resumable) is judged resumable even though this job has no placement status yet.
	judged := job.DeepCopy()
	judged.Status.Placement = pl
	d := admission.DecideFor(judged, site, l, pl.TimeLimitMinutes)
	switch d.Phase {
	case hpcv1alpha1.PhaseAdmitted:
		// The lane is chosen with the booking, in the same serial step, so the one-job semaphore
		// (D-69) sees every lane this process has handed out. A shareable step is also given its place in
		// a shared node (host or guest, share.go) in the same step, so the packing never races.
		if pl, err = r.withShare(ctx, job, site, pl); err != nil {
			return ctrl.Result{}, err
		}
		b := &hpcv1alpha1.Booking{Site: site.Spec.Site, SiteName: site.Name, SiteNamespace: site.Namespace,
			Tier: d.Tier, NodeMinutes: d.Cost, At: metav1.NewTime(r.now())}
		r.remember(string(job.UID), *b)
		r.rememberLane(string(job.UID), pl.QOS)
		r.rememberShare(job, pl, b)
		if err := r.setPhasePlaced(ctx, job, d.Phase, d.Reason, d.Message, b, pl); err != nil {
			r.forget(string(job.UID))
			return ctrl.Result{}, err
		}
		metrics.NodeMinutesBooked.WithLabelValues(string(d.Tier), projectOf(site)).Add(float64(d.Cost))
		r.countLanePlacement(job, site, pl)
		if pl.Share != nil {
			metrics.SharePlacements.WithLabelValues(string(pl.Share.Role)).Inc()
		}
		if d.OverShare && r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, "OverShare", d.Message)
		}
		return ctrl.Result{}, nil
	case hpcv1alpha1.PhaseWaiting:
		if job.Status.Phase != d.Phase || job.Status.Reason != d.Reason || job.Status.Message != d.Message {
			if err := r.setPhase(ctx, job, d.Phase, d.Reason, d.Message, nil); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: WaitingResync}, nil
	default:
		return ctrl.Result{}, r.setPhase(ctx, job, d.Phase, d.Reason, d.Message, nil)
	}
}

// followAndReplan follows an admitted job and, while it continues, plans a guest no host took again (no starvation, D-70).
func (r *HPCArgoWorkflowJobReconciler) followAndReplan(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, serve bool) (bool, error) {
	next, err := r.follow(ctx, job, site)
	if err != nil || next || !serve {
		return next, err
	}
	return false, r.replanGuest(ctx, job, site)
}

// follow moves an admitted job to Running when the site job exists, and ends the attempt when the
// submitter reports it will not run again (phase 0: the in-pod runner writes status.run). It
// returns true when the job continues in a next attempt.
func (r *HPCArgoWorkflowJobReconciler) follow(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (bool, error) {
	run := job.Status.Run
	if run == nil || !current(run, job.Status.Attempt) {
		return false, nil // nothing reported yet, or a report of an earlier attempt
	}
	if err := r.reflectLane(ctx, job, run); err != nil {
		return false, err
	}
	if run.Terminal {
		return r.endAttempt(ctx, job, site)
	}
	if run.SiteJobID != "" && job.Status.Phase == hpcv1alpha1.PhaseAdmitted {
		return false, r.setPhase(ctx, job, hpcv1alpha1.PhaseRunning, "Submitted", "site job "+run.SiteJobID, job.Status.Booking)
	}
	return false, nil
}

// current reports whether a submitter's report is about the job's current attempt. A report without
// an attempt number counts only for attempt 1: after a continuation it may be the old attempt's.
func current(run *hpcv1alpha1.RunStatus, attempt int32) bool {
	if run.Attempt == 0 {
		return attempt <= 1
	}
	return run.Attempt == attempt
}

// endAttempt settles the current attempt and either ends the job or continues it under this same
// HPCArgoWorkflowJob (D-44 as amended 2026-11-05, F-29). The attempt's actual spend replaces its booking
// (D-12); phase 0 settles from the run times the site reported, phase 1 from the site's accounting
// record (D-32). A guest on a shared node is covered by its host's booking and settles to zero
// (D-26). A continuation needs a resumable step, a checkpoint the job staged out before it exited
// (T-19), and a continuation left under the step's cap; it is then admitted and booked like a new
// step (A4).
func (r *HPCArgoWorkflowJobReconciler) endAttempt(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (bool, error) {
	run := job.Status.Run
	st := settleAttempt(job, r.now())
	used, end := st.Minutes, st.End
	if len(st.Discarded) > 0 && r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeWarning, "ReportedTimeDiscarded",
			"discarded implausible site times, settled "+strconv.FormatInt(used, 10)+" node-minutes: "+strings.Join(st.Discarded, ", "))
	}
	phase, reason := outcome(run)
	orig := job.DeepCopy()
	attempt := max(job.Status.Attempt, 1)
	tier := hpcv1alpha1.TierGPU
	var admittedAt *metav1.Time
	booking := job.Status.Booking
	if booking != nil {
		tier = booking.Tier
		admittedAt = &booking.At
	}
	job.Status.Attempts = append(job.Status.Attempts, hpcv1alpha1.AttemptRecord{Attempt: attempt,
		SiteJobID: run.SiteJobID, ExitCode: run.ExitCode, Outcome: reason, Tier: tier,
		SettledNodeMinutes: used, EndTime: end, QueueReplaced: run.QueueReplaced, QueueReplacedCount: run.QueueReplacedCount})
	job.Status.Booking = nil
	msg := settledWords(attempt, used)
	if reason == hpcv1alpha1.ReasonIdle {
		// D-72: the on-node watchdog ended a hung step; name it, count it, record what it spared.
		saved := nodeMinutesSaved(job, run, end)
		job.Status.Attempts[len(job.Status.Attempts)-1].NodeMinutesSaved = &saved
		msg = idleMessage(job, run, saved, msg)
		r.noteIdleKill(job, site, tier, msg)
	}

	if reason == hpcv1alpha1.ReasonInsufficientQuota {
		msg = quotaMessage(run)
		if r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonInsufficientQuota, msg)
		}
	}

	if reason == hpcv1alpha1.ReasonInvalidImage {
		msg = strings.TrimSpace(run.Message)
		if msg == "" {
			msg = "the step image reference cannot be pulled; nothing was submitted to the site"
		}
		if r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonInvalidImage, msg)
		}
	}

	if reason == hpcv1alpha1.ReasonSubmitLimit {
		msg = submitLimitMessage(run, site)
		if r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonSubmitLimit, msg)
		}
	}

	v := r.recoverAttempt(job, run, attempt, attemptEnd{phase: phase, reason: reason, msg: msg})
	phase, reason, msg, next := v.phase, v.reason, v.msg, v.next
	if retry := timeoutRetry(job, site, run); !next && reason == hpcv1alpha1.ReasonWallTimeReached && retry != nil {
		// D-67: a learned limit that was too tight is not a failure; one more attempt with twice
		// the limit, up to the spec's cap. The job is re-admitted and re-booked like a continuation.
		msg += retryMessage(job.Status.Placement.TimeLimitMinutes, retry.TimeLimitMinutes, attempt+1)
		next = true
		job.Status.Attempt = attempt + 1
		job.Status.Placement = retry
		phase, reason = hpcv1alpha1.PhasePending, hpcv1alpha1.ReasonTimeLimitRetry
	}
	stopped := !next && reason == hpcv1alpha1.ReasonWallTimeReached
	if stopped {
		// The step ran its whole declared limit: say so in plain words (settled minutes are in attempts).
		msg = timelimit.StopMessage(job.EffectiveWallMinutes(), capsOf(job, site).Max(), job.Spec.Resumable != nil)
	}
	if run.HostJobOpen && run.HostSiteJobID == "" && phase.Terminal() && booking != nil {
		// D-26: the step ended but the host's Slurm job still holds the node for its guests, and the site
		// charges it until that job ends. The booking stays held (the guests hold none of their own) until the
		// runner reports the job's end (resettleHostEnd); the step's result does not wait for it.
		job.Status.Booking = booking
		job.Status.Attempts[len(job.Status.Attempts)-1].HostJobOpen = true
		msg += hostOpenNote
	}
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, reason, msg
	if phase.Terminal() {
		var total int64
		for _, a := range job.Status.Attempts {
			total += a.SettledNodeMinutes
		}
		job.Status.SettledNodeMinutes = &total
	}
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	r.forget(string(job.UID))
	r.noteEnded(string(job.UID))
	observeSettled(site, tier, used, run, admittedAt)
	if stopped && r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonWallTimeReached, msg)
	}
	return next, nil
}

// outcome maps the site's final state and exit code of one attempt to the job's phase.
func outcome(run *hpcv1alpha1.RunStatus) (hpcv1alpha1.Phase, string) {
	state := strings.ToUpper(run.State)
	failed := state == "FAILED"
	switch {
	case strings.HasPrefix(state, "CANCELLED"):
		return hpcv1alpha1.PhaseCancelled, "Cancelled"
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeContinue:
		return hpcv1alpha1.PhaseFailed, "Continued"
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeIdle && failed:
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonIdle
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeInsufficientQuota && failed:
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonInsufficientQuota
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeInvalidImage && failed:
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonInvalidImage
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeSubmitLimit && failed:
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonSubmitLimit
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeGPUViolation && failed:
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonGPUViolation
	case state == "COMPLETED" && (run.ExitCode == nil || *run.ExitCode == 0):
		return hpcv1alpha1.PhaseSucceeded, "Completed"
	case state == "TIMEOUT":
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonWallTimeReached
	case state == "":
		return hpcv1alpha1.PhaseFailed, "Ended"
	default:
		return hpcv1alpha1.PhaseFailed, "Site" + state
	}
}

// ledgerOf computes the site's ledger from every job booked against it.
func (r *HPCArgoWorkflowJobReconciler) ledgerOf(ctx context.Context, site *hpcv1alpha1.HPCSite) (ledger.Ledger, error) {
	l, _, err := r.ledgerAndJobs(ctx, site)
	return l, err
}

// ledgerAndJobs is ledgerOf that also returns the jobs it read, so the publisher can list them.
func (r *HPCArgoWorkflowJobReconciler) ledgerAndJobs(ctx context.Context, site *hpcv1alpha1.HPCSite) (ledger.Ledger, []hpcv1alpha1.HPCArgoWorkflowJob, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs); err != nil {
		return ledger.Ledger{}, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range jobs.Items {
		if jobs.Items[i].Status.Booking != nil {
			delete(r.pending, string(jobs.Items[i].UID)) // the cache shows it now
			delete(r.pendingLane, string(jobs.Items[i].UID))
		}
	}
	return ledger.Compute(site, jobs.Items, r.pending, r.now()), jobs.Items, nil
}

// publishLedgerNow writes the site's ledger to its status, for people and the hub to read (F-30).
func (r *HPCArgoWorkflowJobReconciler) publishLedgerNow(ctx context.Context, site *hpcv1alpha1.HPCSite) error {
	l, jobs, err := r.ledgerAndJobs(ctx, site)
	if err != nil {
		return err
	}
	recent, err := r.recentJobsOf(ctx, site, jobs)
	if err != nil {
		return err
	}
	steps, err := r.resumableStepsFor(ctx, site, jobs)
	if err != nil {
		return err
	}
	orig := site.DeepCopy()
	start := metav1.NewTime(l.PeriodStart)
	site.Status.PeriodStart = &start
	counted := metav1.NewTime(l.CountedFrom)
	site.Status.CountedFrom = &counted
	site.Status.ActiveJobs = l.Active
	site.Status.QueuedJobs = l.Queued
	site.Status.RunningJobs = l.Running
	site.Status.RecentJobs = recent
	site.Status.ResumableSteps = steps
	for _, tier := range hpcv1alpha1.Tiers {
		t := l.Tiers[tier]
		left := t.Left()
		if t.Unbounded {
			left = 0 // meaningless without a figure; the ledger says so with unbounded=true
		}
		site.Status.SetLedger(tier, &hpcv1alpha1.TierLedger{UsedNodeMinutes: t.Used, BookedNodeMinutes: t.Booked,
			LeftNodeMinutes: left, Unbounded: t.Unbounded,
			Percent: int32(t.Percent())})
		metrics.SetSiteLedger(site.Spec.Site, projectOf(site), string(tier), t.Used, t.Booked, left)
	}
	metrics.ActiveJobs.WithLabelValues(projectOf(site), site.Spec.Site).Set(float64(l.Active))
	r.noteBudgetWarning(site, l)
	r.noteSiteReachable(site)
	if u, uerr := r.checkpointUsage(ctx, site); uerr == nil {
		site.Status.Checkpoints = &u
	}
	err = r.Status().Patch(ctx, site, client.MergeFrom(orig))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *HPCArgoWorkflowJobReconciler) setPhase(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, phase hpcv1alpha1.Phase,
	reason, msg string, booking *hpcv1alpha1.Booking) error {
	return r.setPhasePlaced(ctx, job, phase, reason, msg, booking, nil)
}

// setPhasePlaced is setPhase that also records the attempt's time limit in the same write, so a
// booking never exists without the limit it was made for.
func (r *HPCArgoWorkflowJobReconciler) setPhasePlaced(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, phase hpcv1alpha1.Phase,
	reason, msg string, booking *hpcv1alpha1.Booking, placement *hpcv1alpha1.Placement) error {
	orig := job.DeepCopy()
	if placement != nil {
		job.Status.Placement = placement
	}
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, reason, msg
	if booking != nil {
		job.Status.Booking = booking
		if job.Status.Site == nil {
			job.Status.Site = &hpcv1alpha1.SiteRef{Name: booking.SiteName, Namespace: booking.SiteNamespace}
		}
		if job.Status.Attempt == 0 {
			job.Status.Attempt = 1
		}
	}
	// The optimistic lock makes a decision made on a stale read fail and be retried on fresh data.
	return r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}

func (r *HPCArgoWorkflowJobReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *HPCArgoWorkflowJobReconciler) remember(uid string, b hpcv1alpha1.Booking) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[string]hpcv1alpha1.Booking{}
	}
	r.pending[uid] = b
}

func (r *HPCArgoWorkflowJobReconciler) forget(uid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, uid)
	delete(r.pendingLane, uid)
	delete(r.pendingShare, uid)
	delete(r.laneNoted, uid)
}

// noteEnded remembers that this process just wrote the job's end. The ledger published in the same
// reconcile is computed from the informer cache, which does not show that write yet, so it still
// counts the job's booking; publishAfterEnd publishes it again once the cache has caught up.
func (r *HPCArgoWorkflowJobReconciler) noteEnded(uid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended == nil {
		r.ended = map[string]struct{}{}
	}
	r.ended[uid] = struct{}{}
}

// publishAfterEnd republishes the ledger of the site of a job this process ended, once. It runs in
// the reconcile that the job's own end event wakes, when the cache shows the end (a job reconcile
// that reads a terminal job from the cache has seen that write). Without it the last job to end
// leaves its booking in HPCSite.status until the 10-minute resync.
func (r *HPCArgoWorkflowJobReconciler) publishAfterEnd(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) {
	r.mu.Lock()
	_, owed := r.ended[string(job.UID)]
	delete(r.ended, string(job.UID))
	r.mu.Unlock()
	if !owed || job.Status.Site == nil {
		return
	}
	var site hpcv1alpha1.HPCSite
	if err := r.Get(ctx, client.ObjectKey{Namespace: job.Status.Site.Namespace, Name: job.Status.Site.Name}, &site); err != nil {
		return
	}
	if err := r.publishLedger(ctx, &site); err != nil {
		logf.FromContext(ctx).Error(err, "publishing the ledger after a job ended", "site", client.ObjectKeyFromObject(&site))
	}
}

// rememberLane keeps the QoS of a booking this process made until the cache shows it, so the lane
// semaphore never misses a job it admitted a moment ago. An empty qos (class default) is not kept.
func (r *HPCArgoWorkflowJobReconciler) rememberLane(uid, qos string) {
	if qos == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingLane == nil {
		r.pendingLane = map[string]string{}
	}
	r.pendingLane[uid] = qos
}

func (r *HPCArgoWorkflowJobReconciler) isPending(uid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pending[uid]
	return ok
}

// SetupWithManager sets up the controller with the Manager. One worker: admission is serial.
func (r *HPCArgoWorkflowJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hpcv1alpha1.HPCArgoWorkflowJob{}).
		// A runner Job's change (failed, finished, gone) wakes the job it runs for.
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(jobOfRunner),
			builder.WithPredicates(predicate.NewPredicateFuncs(isRunnerJob))).
		Named("hpcargoworkflowjob").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// quotaMessage is the plain sentence of an attempt that stopped before staging for want of scratch
// quota (AC-33): what the runner said did not fit, else a generic line. Nothing was copied and
// nothing in use was deleted.
func quotaMessage(run *hpcv1alpha1.RunStatus) string {
	if m := strings.TrimSpace(run.Message); m != "" {
		return m
	}
	return "not enough scratch space or files left on the site for the dataset or image this step needs; " +
		"nothing was copied and nothing in use was deleted. Free space on the project's scratch or raise its quota"
}

// submitLimitMessage is the plain sentence of an attempt the site refused for its submit cap (AC-39):
// what the runner said (it names the cap and the count it read from the site), else the profile's cap.
func submitLimitMessage(run *hpcv1alpha1.RunStatus, site *hpcv1alpha1.HPCSite) string {
	if m := strings.TrimSpace(run.Message); m != "" {
		return m
	}
	msg := "the site refused the job: too many queued or running jobs for the project's user"
	if c := site.Spec.Profile.MaxSubmitJobs; c > 0 {
		msg += " (cap " + strconv.Itoa(int(c)) + ")"
	}
	return msg
}
