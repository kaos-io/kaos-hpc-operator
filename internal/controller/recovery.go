package controller

import (
	"strconv"
	"strings"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/resume"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// Recovery from interrupted attempts (PRD-HPC-1461 D-74, D-75).
//
//	tier 1, every step: an end that is not the step's doing (node failure, maintenance, pre-emption) re-runs
//	a step that has no checkpoint from the start, at most MaxReruns times. Outputs are published only on
//	success, so a re-run is safe. It needs no scratch space and no checkpoint folder.
//	tier 2, a step with a checkpoint: every recoverable end (exit 85, wall time, node failure, pre-emption,
//	idle kill) continues from the newest persisted copy; a continuation that does not get past its checkpoint
//	ends NoProgress, and the cap on the step's total time ends it too (D-75). No count of continuations. The
//	step's own error is never recovered. A job that ran to its limit and saved nothing ends NoSavedProgress.
//	A continuation that an infrastructure end stops before a new checkpoint starts again from the same one; these
//	count with the re-runs of tier 1, MaxReruns for the step in all.

// attemptEnd is the verdict on one ended attempt: the job's next phase, and whether another attempt follows.
type attemptEnd struct {
	phase  hpcv1alpha1.Phase
	reason string
	msg    string
	next   bool
	// spent: the step's jobs together used the whole cap on its total time (see capSpent).
	spent bool
}

// recoverAttempt decides what follows an ended attempt from its phase/reason as outcome() set them. It
// updates the job status in memory (counters, resume info, evidence); the caller patches it.
func (r *HPCArgoWorkflowJobReconciler) recoverAttempt(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus,
	attempt int32, v attemptEnd) attemptEnd {
	end := resume.Classify(run.State, run.ExitCode)
	hasNew := run.CheckpointRef != ""
	cur := resume.MarkOf(run.Checkpoint)
	if ev := resume.Evidence(job.Status.Resume, end, hasNew, cur, r.now()); ev != nil {
		job.Status.ResumeEvidence = ev
	}
	if job.Spec.StepName == "" {
		return v // phase 0: the in-pod runner submits; nothing is continued
	}
	switch {
	case !end.Recoverable():
		return v
	case capSpent(job):
		v.phase, v.reason, v.spent = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonWallTimeReached, true
		v.msg += "; it reached the platform's cap of " + timelimit.FormatMinutes(job.Spec.WallTimeMinutes) +
			" (" + jobsRun(job) + ")"
		return v
	case hasNew:
		return continueFrom(job, run, attempt, end, cur, v)
	case end == resume.EndContinue:
		return exitWithoutCheckpoint(v)
	case end.Infrastructure():
		return afterInfrastructureEnd(job, attempt, end, v)
	case (end == resume.EndWallTime || end == resume.EndIdle) && job.Status.Resume != nil:
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNoProgress
		v.msg += "; the continuation saved no checkpoint before it ended (" + end.Words() + "), so it did not get past " +
			progressWords(job.Status.Resume.ProgressValues, "its checkpoint")
	}
	return v
}

// continueFrom starts the next attempt from the checkpoint the ended attempt persisted, unless the step
// did not get past the checkpoint it started from.
func continueFrom(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus, attempt int32, end resume.End,
	cur resume.Mark, v attemptEnd) attemptEnd {
	if prev := job.Status.Resume; prev != nil && !resume.Advanced(resume.MarkOfResume(prev), cur) {
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNoProgress
		v.msg += "; the continuation's newest checkpoint (" + progressWords(cur.Values, "unchanged folder") +
			") is not past the one it started from (" + progressWords(prev.ProgressValues, "its folder") + ")"
		return v
	}
	job.Status.Continuations++
	job.Status.Attempt = attempt + 1
	job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: run.CheckpointRef, FromAttempt: attempt,
		ProgressValues: cur.Values, Digest: cur.Digest}
	v.phase, v.reason, v.next = hpcv1alpha1.PhasePending, hpcv1alpha1.ReasonContinuing, true
	v.msg += "; " + end.Words() + ", continuing from checkpoint " + run.CheckpointRef + " as attempt " + strconv.Itoa(int(attempt+1))
	return v
}

// exitWithoutCheckpoint: the step said "saved, continue me" but no copy reached the checkpoint store.
func exitWithoutCheckpoint(v attemptEnd) attemptEnd {
	v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonCheckpointMissing
	v.msg += "; checkpoint not saved: no verified copy of the checkpoint folder to continue from"
	return v
}

// afterInfrastructureEnd handles a node failure or pre-emption with no new checkpoint: a continuation
// that had a checkpoint tries the same one again; a step without one is re-run from the start (tier 1). Both
// count as re-runs, MaxReruns for the step in all.
func afterInfrastructureEnd(job *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32, end resume.End, v attemptEnd) attemptEnd {
	if job.Status.Reruns >= hpcv1alpha1.MaxReruns {
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonRerunLimit
		v.msg += "; " + end.Words() + " and the step was already re-run " + strconv.Itoa(int(hpcv1alpha1.MaxReruns)) + " times"
		return v
	}
	job.Status.Reruns++
	count := " (" + strconv.Itoa(int(job.Status.Reruns)) + "/" + strconv.Itoa(int(hpcv1alpha1.MaxReruns)) + ")"
	if prev := job.Status.Resume; prev != nil {
		job.Status.Continuations++
		job.Status.Attempt = attempt + 1
		v.phase, v.reason, v.next = hpcv1alpha1.PhasePending, hpcv1alpha1.ReasonContinuing, true
		v.msg += "; " + end.Words() + " before a new checkpoint, continuing again from " + prev.CheckpointRef + count +
			" as attempt " + strconv.Itoa(int(attempt+1))
		return v
	}
	job.Status.Attempt = attempt + 1
	job.Status.Resume = nil
	v.phase, v.reason, v.next = hpcv1alpha1.PhasePending, end.RerunReason(), true
	v.msg += "; " + end.Words() + "; re-running the step from the start" + count + " as attempt " + strconv.Itoa(int(attempt+1)) +
		"; outputs are published only when a run succeeds"
	return v
}

// jobsRun counts the step's jobs that started at the site, the ended attempt included: an attempt that never
// started (a node failure while it queued) used none of the step's time.
func jobsRun(job *hpcv1alpha1.HPCArgoWorkflowJob) string {
	n := 0
	for _, a := range job.Status.Attempts {
		if a.WallMinutes > 0 {
			n++
		}
	}
	if n == 1 {
		return "1 job"
	}
	return strconv.Itoa(n) + " jobs"
}

// noSavedProgressMessage is the sentence of ending 2: the job ran to its limit on its lane and saved nothing.
func noSavedProgressMessage(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus) string {
	lane := run.QOS
	if p := job.Status.Placement; lane == "" && p != nil {
		lane = p.QOS
	}
	if lane == "" {
		lane = defaultQOS
	}
	return "Ended: it ran " + timelimit.FormatMinutes(job.EffectiveWallMinutes()) + " on " + lane +
		" without saving progress. A step that saves progress through the library is continued instead."
}

// progressWords renders progress values for a message.
func progressWords(values []int64, none string) string {
	if len(values) == 0 {
		return none
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.FormatInt(v, 10)
	}
	return "progress " + strings.Join(parts, "/")
}
