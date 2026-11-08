package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// The orphan sweep (PRD-HPC-1461 AC-08, D-20). An attempt whose site job may still exist is never ended on the
// controller's word alone. When its runner Job failed or is gone, or a cancel was requested and the runner
// could not prove the site job over, the controller keeps the booking and starts a cancel-only runner Job
// (the runner image with --cancel-only): it adopts the site job by key, cancels it with retries, and reports
// its final state. The attempt is then settled from that report and the booking released.
//
// Decision rule when the site never answers: the booking is kept while cancel-only runners are retried (each
// is bounded by its own budget and deadline). After SweepGiveUp (default 6 h) from the start of the sweep the
// controller decides without proof: the attempt is settled up to now (conservative: never zero for a job that
// ran), the booking is released, and a Warning event SiteJobUnconfirmed names the site job so that a person
// can look. A site that answers "I do not know this job" for 10 minutes is a proof of its own (the runner
// reports state SITE_UNKNOWN).

const (
	// DefaultSweepGiveUp is how long a sweep waits for the site's confirmation.
	DefaultSweepGiveUp = 6 * time.Hour
	// SweepPoll is how often a sweep looks at its cancel-only runner Job.
	SweepPoll = 5 * time.Second
	// DefaultSiteMissingGrace is how long a booked job survives its HPCSite (or project label) being gone.
	DefaultSiteMissingGrace = 15 * time.Minute
	// SiteMissingPoll is how often a job with a missing HPCSite is looked at again.
	SiteMissingPoll = 30 * time.Second
)

func (r *HPCArgoWorkflowJobReconciler) sweepGiveUp() time.Duration {
	if r.SweepGiveUp > 0 {
		return r.SweepGiveUp
	}
	return DefaultSweepGiveUp
}

func (r *HPCArgoWorkflowJobReconciler) siteMissingGrace() time.Duration {
	if r.SiteMissingGrace > 0 {
		return r.SiteMissingGrace
	}
	return DefaultSiteMissingGrace
}

// siteJobMayLive reports whether the current attempt has a site job that no report has shown over.
func siteJobMayLive(job *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	run := job.Status.Run
	return run != nil && current(run, job.Status.Attempt) && run.SiteJobID != "" && !run.Terminal
}

// siteJobIDOf is the site job id the current attempt reported, or "" when none was reported yet.
func siteJobIDOf(job *hpcv1alpha1.HPCArgoWorkflowJob) string {
	if run := job.Status.Run; run != nil && current(run, job.Status.Attempt) {
		return run.SiteJobID
	}
	return ""
}

// runnerMayHaveSubmitted reports whether a runner Job exists for the attempt whose site job no report shows
// over: the runner may have submitted it without reporting its id (a SIGTERM in that window leaves it
// running). It is false for a job that ended, that already sweeps, that never got a runner Job, or that
// still waits for an unreachable site (the runner submits only after it has reached the site).
func (r *HPCArgoWorkflowJobReconciler) runnerMayHaveSubmitted(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (bool, error) {
	if job.Status.Phase.Terminal() || job.Status.Sweep != nil || job.Status.Site == nil {
		return false, nil
	}
	if run := job.Status.Run; run != nil && current(run, job.Status.Attempt) && run.Terminal {
		return false, nil
	}
	if siteJobIDOf(job) == "" && job.Status.PendingReason == hpcv1alpha1.ReasonSiteUnavailable {
		return false, nil
	}
	var jobs batchv1.JobList
	if err := r.reader().List(ctx, &jobs, client.InNamespace(job.Status.Site.Namespace),
		client.MatchingLabels{runnerjob.LabelJobUID: string(job.UID)}); err != nil {
		return false, fmt.Errorf("listing the runner Jobs: %w", err)
	}
	for i := range jobs.Items {
		if jobs.Items[i].Labels[runnerjob.LabelCancelOnly] == "" {
			return true, nil
		}
	}
	return false, nil
}

// beginSweep records what the job becomes once the site confirms the end of its attempt, and keeps the
// booking. Idempotent: a sweep already begun is left as it is.
func (r *HPCArgoWorkflowJobReconciler) beginSweep(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	phase hpcv1alpha1.Phase, reason, msg string) error {
	if job.Status.Sweep != nil {
		return nil
	}
	orig := job.DeepCopy()
	job.Status.Sweep = &hpcv1alpha1.SweepStatus{Phase: phase, Reason: reason, Message: msg, Since: metav1.NewTime(r.now())}
	id := siteJobIDOf(job)
	what := "site job " + id
	if id == "" {
		what = "the site job of this attempt (its id was not reported)"
	}
	job.Status.Message = truncate(msg + "; waiting for the site to confirm that " + what + " has ended")
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("orphan sweep started", "siteJobID", id, "becomes", phase, "reason", reason)
	if r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeNormal, "SweepStarted",
			"the runner is gone but "+what+" may still run: a cancel-only runner will stop it")
	}
	return nil
}

// continueSweep advances a sweep: finish it when the site's end is reported or the give-up time passed,
// otherwise make sure a cancel-only runner Job is working on it. site may be nil (its HPCSite is gone): no
// runner can then be built and only the give-up time can end the sweep.
func (r *HPCArgoWorkflowJobReconciler) continueSweep(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (ctrl.Result, error) {
	sw := job.Status.Sweep
	if run := job.Status.Run; run != nil && current(run, job.Status.Attempt) && run.Terminal {
		return ctrl.Result{}, r.finishSweep(ctx, job, site, sw.Message+"; the site confirmed that its job ended ("+run.State+")")
	}
	if age := r.now().Sub(sw.Since.Time); age >= r.sweepGiveUp() {
		id := ""
		if job.Status.Run != nil {
			id = job.Status.Run.SiteJobID
		}
		if r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, "SiteJobUnconfirmed", "site job "+id+
				" could not be confirmed ended within "+age.Round(time.Minute).String()+"; it may still run")
		}
		return ctrl.Result{}, r.finishSweep(ctx, job, site, sw.Message+"; the site did not confirm the end of its job within "+
			age.Round(time.Minute).String()+" (settled up to now; site job "+id+" may still be running)")
	}
	if site == nil || job.Status.Site == nil {
		return ctrl.Result{RequeueAfter: SweepPoll}, nil
	}
	attempt := max(job.Status.Attempt, 1)
	key := types.NamespacedName{Namespace: job.Status.Site.Namespace, Name: runnerjob.CancelName(job.UID, attempt, sw.Launches)}
	var rj batchv1.Job
	err := r.Get(ctx, key, &rj)
	switch {
	case apierrors.IsNotFound(err):
		return ctrl.Result{RequeueAfter: SweepPoll}, r.launchCancelRunner(ctx, job, site, attempt, sw.Launches)
	case err != nil:
		return ctrl.Result{}, err
	case jobCondition(&rj, batchv1.JobFailed), jobCondition(&rj, batchv1.JobComplete) && sweepReportOverdue(&rj, r.now()):
		// This cancel-only runner ended without proof: start the next one.
		return ctrl.Result{RequeueAfter: SweepPoll}, r.nextLaunch(ctx, job, &rj)
	}
	return ctrl.Result{RequeueAfter: SweepPoll}, nil
}

// sweepReportOverdue is true when a finished cancel-only Job had RunnerReportGrace to show its report.
func sweepReportOverdue(rj *batchv1.Job, now time.Time) bool {
	t := rj.Status.CompletionTime
	return t == nil || now.Sub(t.Time) >= RunnerReportGrace
}

func (r *HPCArgoWorkflowJobReconciler) launchCancelRunner(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, attempt, n int32) error {
	if r.RunnerImage == "" {
		return fmt.Errorf("a cancel-only runner for %s/%s needs a runner image: start the controller with --runner-image", job.Namespace, job.Name)
	}
	rj, err := runnerjob.BuildCancel(runnerjob.Config{Image: r.RunnerImage, Scheduling: r.RunnerScheduling}, job, site, attempt, n)
	if err != nil {
		return fmt.Errorf("building the cancel-only runner Job: %w", err)
	}
	if err := r.Create(ctx, rj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating the cancel-only runner Job: %w", err)
	}
	logf.FromContext(ctx).Info("cancel-only runner Job created", "runnerJob", rj.Name, "attempt", attempt, "launch", n)
	return nil
}

// nextLaunch counts the ended cancel-only runner and removes its Job so the next one can start.
func (r *HPCArgoWorkflowJobReconciler) nextLaunch(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, rj *batchv1.Job) error {
	orig := job.DeepCopy()
	job.Status.Sweep.Launches++
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, rj, client.PropagationPolicy(metav1.DeletePropagationBackground)))
}

// finishSweep ends the attempt the sweep stood for: settled from the run as reported, booking released.
func (r *HPCArgoWorkflowJobReconciler) finishSweep(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, msg string) error {
	sw := job.Status.Sweep
	logf.FromContext(ctx).Info("orphan sweep finished", "siteJobID", siteJobIDOf(job), "becomes", sw.Phase, "message", truncate(msg))
	return r.failAttempt(ctx, job, site, sw.Phase, sw.Reason, sw.Reason, truncate(msg))
}

// siteMissing handles a booked serve-mode job whose HPCSite (or project label) is gone. It keeps the job and
// its booking for the grace period, counted from the first observation and kept in status so a restart does
// not reset it; a job still unbound after it is cancelled. The runner keeps watching the site job meanwhile.
func (r *HPCArgoWorkflowJobReconciler) siteMissing(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, why string) (ctrl.Result, error) {
	if job.Status.SiteMissingSince == nil {
		orig := job.DeepCopy()
		since := metav1.NewTime(r.now())
		job.Status.SiteMissingSince = &since
		job.Status.Reason = hpcv1alpha1.ReasonSiteMissing
		job.Status.Message = truncate("HPCSiteMissing since " + since.UTC().Format(time.RFC3339) + ": " + why +
			"; the job is cancelled if it stays missing for " + r.siteMissingGrace().String())
		if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		if r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonSiteMissing, job.Status.Message)
		}
		return ctrl.Result{RequeueAfter: SiteMissingPoll}, nil
	}
	if age := r.now().Sub(job.Status.SiteMissingSince.Time); age < r.siteMissingGrace() {
		return ctrl.Result{RequeueAfter: min(SiteMissingPoll, r.siteMissingGrace()-age)}, nil
	}
	return r.cancel(ctx, job, "its HPCSite binding has been gone for "+r.siteMissingGrace().String()+" ("+why+")")
}

// siteBack clears the missing-site marker once the HPCSite resolves again.
func (r *HPCArgoWorkflowJobReconciler) siteBack(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) error {
	if job.Status.SiteMissingSince == nil {
		return nil
	}
	orig := job.DeepCopy()
	job.Status.SiteMissingSince = nil
	if job.Status.Reason == hpcv1alpha1.ReasonSiteMissing {
		job.Status.Reason, job.Status.Message = hpcv1alpha1.ReasonAdmitted, "the HPCSite is back"
		if job.Status.Phase == hpcv1alpha1.PhaseRunning {
			job.Status.Reason = "Submitted"
		}
	}
	return r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}
