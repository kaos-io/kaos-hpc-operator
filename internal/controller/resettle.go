package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// hostOpenNote ends the message of a host attempt whose Slurm job still holds the node (D-26).
const hostOpenNote = "; its Slurm job still holds the node for guests: the booking is held until that job ends"

// settledWords is the start of an ended attempt's status.message.
func settledWords(attempt int32, minutes int64) string {
	return "attempt " + strconv.Itoa(int(attempt)) + " settled " + strconv.FormatInt(minutes, 10) + " node-minutes"
}

// lateEnd decides whether a host's runner has reported the end of the host Slurm JOB after the
// attempt was settled from its step's end (a shared-node host keeps its node, and the site keeps
// charging it, while guests run and for a short while after the last one). It returns the index of the
// attempt record to correct and the new settlement, or ok=false when there is nothing to correct.
//
// An open host attempt (D-26: its booking is still held) is closed by the first report without
// hostJobOpen: settled from the job's start to its end (hostJobEndTime), never below what was already
// settled; an end the site did not give settles up to now (conservative). An attempt settled before (a runner without
// hostJobOpen) only ever grows, and only from an end time that is valid and later than the one already
// recorded, so a repeated report changes nothing and an implausible one (NO_VAL, which settle() would turn
// into "now") can never extend the spend.
func lateEnd(job *hpcv1alpha1.HPCArgoWorkflowJob, now time.Time) (int, settlement, bool) {
	run := job.Status.Run
	n := len(job.Status.Attempts)
	if !job.Status.Phase.Terminal() || run == nil || !run.Terminal || run.HostSiteJobID != "" || n == 0 {
		return 0, settlement{}, false
	}
	rec := job.Status.Attempts[n-1]
	if run.Attempt != rec.Attempt || run.SiteJobID == "" || run.SiteJobID != rec.SiteJobID {
		return 0, settlement{}, false
	}
	if rec.HostJobOpen {
		if run.HostJobOpen {
			return 0, settlement{}, false // the host job still runs
		}
		// Settled to the host job's end; run.EndTime stays the step's own end (the learned limit's sample).
		toJobEnd := *run
		toJobEnd.EndTime = run.HostJobEndTime
		st := settle(job.Spec.Nodes, &toJobEnd, now)
		st.Minutes = max(st.Minutes, rec.SettledNodeMinutes)
		if st.End.Before(&rec.EndTime) {
			st.End = rec.EndTime
		}
		return n - 1, st, true
	}
	if run.EndTime == nil {
		return 0, settlement{}, false
	}
	st := settle(job.Spec.Nodes, run, now)
	if len(st.Discarded) > 0 || !st.End.After(rec.EndTime.Time) || st.Minutes <= rec.SettledNodeMinutes {
		return 0, settlement{}, false
	}
	return n - 1, st, true
}

// resettleHostEnd settles an ended host attempt to its Slurm job's own start to end, once the runner
// reports that end, and releases the booking an open host still held (D-26). Called for terminal jobs only;
// a no-op in every other case. An open host whose runner can no longer report is closed by abandonedHost.
func (r *HPCArgoWorkflowJobReconciler) resettleHostEnd(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (ctrl.Result, error) {
	if i, st, ok := lateEnd(job, r.now()); ok {
		return ctrl.Result{}, r.settleHostEnd(ctx, job, i, st, false)
	}
	return r.abandonedHost(ctx, job)
}

// abandonedHost closes an open host attempt whose runner can no longer report the job's end: its runner Job
// finished or is gone, and RunnerReportGrace has passed for a report on its way. The attempt is settled up
// to now (an unknown end is conservative, never zero). While the runner still runs, the job is looked at
// again after OpenHostPoll.
func (r *HPCArgoWorkflowJobReconciler) abandonedHost(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (ctrl.Result, error) {
	if !ledger.OpenHost(job) || job.Spec.StepName == "" || job.Status.Site == nil {
		return ctrl.Result{}, nil
	}
	attempt := job.Status.Attempts[len(job.Status.Attempts)-1].Attempt
	var rj batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: job.Status.Site.Namespace, Name: runnerjob.Name(job.UID, attempt)}, &rj)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return ctrl.Result{}, err
	case !jobCondition(&rj, batchv1.JobComplete) && !jobCondition(&rj, batchv1.JobFailed):
		return ctrl.Result{RequeueAfter: OpenHostPoll}, nil
	case rj.Status.CompletionTime != nil && r.now().Sub(rj.Status.CompletionTime.Time) < RunnerReportGrace:
		return ctrl.Result{RequeueAfter: RunnerReportGrace}, nil
	}
	closed := job.DeepCopy()
	closed.Status.Run.HostJobOpen, closed.Status.Run.HostJobEndTime = false, &metav1.Time{Time: r.now()}
	i, st, ok := lateEnd(closed, r.now())
	if !ok {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.settleHostEnd(ctx, job, i, st, true)
}

// settleHostEnd writes the host attempt's settlement to its job's end and releases a still-held booking. The
// job total is recomputed from the attempt records (never incremented), so repeating the same report cannot
// double-count, and the settled-minutes metric gets only the difference. The site's ledger is published
// again at the next reconcile, not at the 10-minute resync (live 2026-11-10: HPCSite still showed host
// 5439966's step-only 2 node-minutes after the job was settled to 21). unknown: the end was not reported.
func (r *HPCArgoWorkflowJobReconciler) settleHostEnd(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	i int, st settlement, unknown bool) error {
	orig := job.DeepCopy()
	rec := &job.Status.Attempts[i]
	delta, was := st.Minutes-rec.SettledNodeMinutes, settledWords(rec.Attempt, rec.SettledNodeMinutes)
	if rec.HostJobOpen {
		job.Status.Booking = nil
	}
	rec.SettledNodeMinutes, rec.EndTime, rec.HostJobOpen = st.Minutes, st.End, false
	until, kind, reason := " (its Slurm job held the node until ", corev1.EventTypeNormal, "HostSettledToJobEnd"
	what := "the host Slurm job ended after its step; settled %d node-minutes (+%d)"
	if unknown {
		until, kind, reason = " (its runner ended before the host Slurm job's end was known: settled up to ", corev1.EventTypeWarning, "HostEndUnknown"
		what = "the runner ended before the host Slurm job's end was reported; settled %d node-minutes up to now (+%d)"
	}
	job.Status.Message = strings.Replace(strings.Replace(job.Status.Message, hostOpenNote, "", 1), was,
		settledWords(rec.Attempt, st.Minutes)+until+st.End.UTC().Format(time.RFC3339)+")", 1)
	var total int64
	for _, a := range job.Status.Attempts {
		total += a.SettledNodeMinutes
	}
	job.Status.SettledNodeMinutes = &total
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("settling the host job's end: %w", err)
	}
	r.noteEnded(string(job.UID))
	if r.Recorder != nil {
		r.Recorder.Event(job, kind, reason, fmt.Sprintf("attempt %d: "+what, rec.Attempt, st.Minutes, delta))
	}
	if site, _, _, err := r.siteOf(ctx, job); err == nil && site != nil {
		observeDelta(site, rec.Tier, delta)
	}
	return nil
}

// observeDelta adds extra settled node-minutes of an already counted attempt to the metric.
func observeDelta(site *hpcv1alpha1.HPCSite, tier hpcv1alpha1.Tier, delta int64) {
	observeSettled(site, tier, delta, nil, (*metav1.Time)(nil))
}
