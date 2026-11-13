package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

// DefaultJobRetention is how long a finished HPCArgoWorkflowJob is kept (--job-retention): 35 days, longer than any calendar
// month. Zero keeps them for ever.
const DefaultJobRetention = 35 * 24 * time.Hour

// legacyBudgetFinalizer was put on finished jobs by an earlier build of this branch. Nothing holds a finished job: it is
// stripped wherever it is found.
const legacyBudgetFinalizer = "hpc.kaos.io/budget"

// The budget ledger is derived from the job objects (internal/ledger): what a project has spent this period is the sum of
// the settled attempts the objects still hold, and a budget period is a calendar month (ledger.PeriodStart), or starts at
// the site's ObservedAt for a source-site budget (ledger.CountedFrom). So no object may be deleted during the period its
// last attempt ended in; once that period has closed its attempts no longer count and the object is free. Nothing is stored
// and nothing holds a finished job (no finalizer): this controller simply does not expire a job inside its period, and the
// engine keeps a run (whose Workflow owns its jobs) longer than any month. A person who deletes a run by hand inside the
// month removes its spend from the ledger; that is not guarded.

// jobEndedAt is when the job's last piece of work ended: the newest settled attempt or the run's own end, else the
// job's creation (a job refused before it ran has neither).
func jobEndedAt(job *hpcv1alpha1.HPCArgoWorkflowJob) time.Time {
	var at time.Time
	for _, a := range job.Status.Attempts {
		if a.EndTime.After(at) {
			at = a.EndTime.Time
		}
	}
	if run := job.Status.Run; run != nil && run.EndTime != nil && run.EndTime.After(at) {
		at = run.EndTime.Time
	}
	if at.IsZero() {
		at = job.CreationTimestamp.Time
	}
	return at
}

// spendOutsideLedger: the job's settled attempts all ended before the time the ledger counts from, or there are none, so
// deleting the object changes no ledger.
func spendOutsideLedger(job *hpcv1alpha1.HPCArgoWorkflowJob, countedFrom time.Time) bool {
	for _, a := range job.Status.Attempts {
		if !a.EndTime.Time.Before(countedFrom) {
			return false
		}
	}
	return true
}

// expiry says whether a finished job may be deleted now and, when it may not, how long until it can be looked at again
// (zero: the normal resync, or never). It must be terminal with no sweep and no open host attempt, have ended `retention`
// ago, and have ended before `countedFrom`.
func expiry(job *hpcv1alpha1.HPCArgoWorkflowJob, now time.Time, retention time.Duration, countedFrom time.Time) (bool, time.Duration) {
	if retention <= 0 || !job.Status.Phase.Terminal() || job.Status.Sweep != nil || ledger.OpenHost(job) {
		return false, 0
	}
	if wait := retention - now.Sub(jobEndedAt(job)); wait > 0 {
		return false, wait
	}
	return spendOutsideLedger(job, countedFrom), 0
}

// countedFrom is the time the ledger of the job's site counts from; without the site (gone) the period start, which
// is the safe side.
func (r *HPCArgoWorkflowJobReconciler) countedFrom(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) time.Time {
	now := r.now()
	if site, _, _, err := r.siteOf(ctx, job); err == nil && site != nil {
		return ledger.CountedFrom(site, now)
	}
	return ledger.PeriodStart(now)
}

// stripLegacyFinalizer removes the budget finalizer an earlier build put on a job. stop is true when the job was being
// deleted: it is gone with the finalizer.
func (r *HPCArgoWorkflowJobReconciler) stripLegacyFinalizer(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (stop bool, err error) {
	if !controllerutil.ContainsFinalizer(job, legacyBudgetFinalizer) {
		return false, nil
	}
	deleting := !job.DeletionTimestamp.IsZero()
	orig := job.DeepCopy()
	controllerutil.RemoveFinalizer(job, legacyBudgetFinalizer)
	err = client.IgnoreNotFound(r.Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})))
	return deleting || err != nil, err
}

// expireFinished deletes a job past its retention that ended in a closed period. done is true when the delete was issued.
// A job that is not yet old enough is looked at again when it will be; one inside its period is skipped until the next
// resync (no requeue to the end of the month).
func (r *HPCArgoWorkflowJobReconciler) expireFinished(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (ctrl.Result, bool, error) {
	if !job.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, false, nil
	}
	ok, wait := expiry(job, r.now(), r.JobRetention, r.countedFrom(ctx, job))
	if !ok {
		if wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, false, nil
		}
		return ctrl.Result{}, false, nil
	}
	if err := r.Delete(ctx, job, client.Preconditions{UID: &job.UID}); err != nil {
		return ctrl.Result{}, false, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, true, nil
}

// finishedPass is the terminal branch of Reconcile after the job's own end-of-life work: an open host attempt is settled
// (resettleHostEnd), and a job past its retention and its period is deleted.
func (r *HPCArgoWorkflowJobReconciler) finishedPass(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (ctrl.Result, error) {
	res, err := r.resettleHostEnd(ctx, job)
	if err != nil {
		return res, err
	}
	expire, gone, err := r.expireFinished(ctx, job)
	if gone || err != nil {
		return expire, err
	}
	if res.RequeueAfter == 0 || (expire.RequeueAfter > 0 && expire.RequeueAfter < res.RequeueAfter) {
		res.RequeueAfter = expire.RequeueAfter
	}
	return res, nil
}
