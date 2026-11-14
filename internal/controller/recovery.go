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
//	idle kill) continues from the newest persisted copy, unless the step opted out; a continuation that does
//	not get past its checkpoint ends NoProgress. The step's own error is never recovered.

// attemptEnd is the verdict on one ended attempt: the job's next phase, and whether another attempt follows.
type attemptEnd struct {
	phase  hpcv1alpha1.Phase
	reason string
	msg    string
	next   bool
	// spent: the step's runs together used the whole declared time limit (see segmentsSpent).
	spent bool
}

// recoverAttempt decides what follows an ended attempt from its phase/reason as outcome() set them. It
// updates the job status in memory (counters, resume info, evidence); the caller patches it.
func (r *HPCArgoWorkflowJobReconciler) recoverAttempt(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus,
	caps timelimit.Caps, attempt int32, v attemptEnd) attemptEnd {
	end := resume.Classify(run.State, run.ExitCode)
	hasNew := run.CheckpointRef != ""
	cur := resume.MarkOf(run.Checkpoint)
	if ev := resume.Evidence(job.Status.Resume, end, hasNew, cur, r.now()); ev != nil {
		job.Status.ResumeEvidence = ev
	}
	if job.Spec.StepName == "" {
		return legacyContinue(job, run, attempt, v) // phase 0: the in-pod runner submits; only exit 85
	}
	optOut := job.Spec.ResumableOptOut
	switch {
	case !end.Recoverable():
		return v
	case segmentsSpent(job, caps):
		v.phase, v.reason, v.spent = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonWallTimeReached, true
		v.msg += "; the step's runs used its whole time limit of " + timelimit.FormatMinutes(job.Spec.WallTimeMinutes) +
			" (" + strconv.Itoa(int(attempt)) + " runs)"
		return v
	case hasNew && !optOut:
		return continueFrom(job, run, attempt, end, cur, v)
	case end == resume.EndContinue:
		return exit85WithoutCheckpoint(optOut, v)
	case end.Infrastructure():
		return afterInfrastructureEnd(job, attempt, end, v)
	case (end == resume.EndWallTime || end == resume.EndIdle) && job.Status.Resume != nil:
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNoProgress
		v.msg += "; the continuation saved no checkpoint before it ended (" + end.Words() + "), so it did not get past " +
			progressWords(job.Status.Resume.ProgressValues, "its checkpoint")
	case end == resume.EndWallTime && job.EffectiveResumable() != nil:
		v.msg += "; no checkpoint was saved before the wall time"
	}
	return v
}

// continueFrom starts the next attempt from the checkpoint the ended attempt persisted, unless the step
// reached its continuation cap or did not get past the checkpoint it started from.
func continueFrom(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus, attempt int32, end resume.End,
	cur resume.Mark, v attemptEnd) attemptEnd {
	if limit := continuationCap(job); job.Status.Continuations >= limit {
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonContinuationLimit
		v.msg += "; the step reached its " + strconv.Itoa(int(limit)) + " continuations"
		return v
	}
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

// exit85WithoutCheckpoint: the step said "saved, continue me" but no copy reached the checkpoint store, or
// the step opted out of being continued.
func exit85WithoutCheckpoint(optOut bool, v attemptEnd) attemptEnd {
	if optOut {
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNotResumable
		v.msg += "; the step exited 85 but opted out of resuming (resumable=False)"
		return v
	}
	v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonCheckpointMissing
	v.msg += "; checkpoint not saved: no verified copy of the checkpoint folder to continue from"
	return v
}

// afterInfrastructureEnd handles a node failure or pre-emption with no new checkpoint: a continuation
// that had a checkpoint tries the same one again; a step without one is re-run from the start (tier 1).
func afterInfrastructureEnd(job *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32, end resume.End, v attemptEnd) attemptEnd {
	if prev := job.Status.Resume; prev != nil && !job.Spec.ResumableOptOut {
		if limit := continuationCap(job); job.Status.Continuations >= limit {
			v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonContinuationLimit
			v.msg += "; the step reached its " + strconv.Itoa(int(limit)) + " continuations"
			return v
		}
		job.Status.Continuations++
		job.Status.Attempt = attempt + 1
		v.phase, v.reason, v.next = hpcv1alpha1.PhasePending, hpcv1alpha1.ReasonContinuing, true
		v.msg += "; " + end.Words() + " before a new checkpoint, continuing again from " + prev.CheckpointRef +
			" as attempt " + strconv.Itoa(int(attempt+1))
		return v
	}
	if job.Status.Reruns >= hpcv1alpha1.MaxReruns {
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonRerunLimit
		v.msg += "; " + end.Words() + " and the step was already re-run " + strconv.Itoa(int(hpcv1alpha1.MaxReruns)) + " times"
		return v
	}
	job.Status.Reruns++
	job.Status.Attempt = attempt + 1
	job.Status.Resume = nil
	v.phase, v.reason, v.next = hpcv1alpha1.PhasePending, end.RerunReason(), true
	v.msg += "; " + end.Words() + "; re-running the step from the start (" + strconv.Itoa(int(job.Status.Reruns)) + "/" +
		strconv.Itoa(int(hpcv1alpha1.MaxReruns)) + ") as attempt " + strconv.Itoa(int(attempt+1)) +
		"; outputs are published only when a run succeeds"
	return v
}

// continuationCap is how many continuations the step may have: its declaration's, else the default.
func continuationCap(job *hpcv1alpha1.HPCArgoWorkflowJob) int32 {
	if job.Spec.Resumable != nil {
		return job.Spec.Resumable.MaxContinuations
	}
	return hpcv1alpha1.DefaultMaxContinuations
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

// legacyContinue is the phase-0 rule, unchanged: only exit 85 with a staged checkpoint of a declared
// resumable step continues.
func legacyContinue(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus, attempt int32, v attemptEnd) attemptEnd {
	if run.ExitCode == nil || *run.ExitCode != hpcv1alpha1.ExitCodeContinue {
		return v
	}
	res := job.Spec.Resumable
	switch {
	case res == nil:
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNotResumable
		v.msg += "; the step exited 85 but is not declared resumable"
	case run.CheckpointRef == "":
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonCheckpointMissing
		v.msg += "; checkpoint upload incomplete: no staged-out checkpoint to continue from"
	case job.Status.Continuations >= res.MaxContinuations:
		v.phase, v.reason = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonContinuationLimit
		v.msg += "; the step reached its " + strconv.Itoa(int(res.MaxContinuations)) + " continuations"
	default:
		job.Status.Continuations++
		job.Status.Attempt = attempt + 1
		job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: run.CheckpointRef, FromAttempt: attempt}
		v.phase, v.reason, v.next = hpcv1alpha1.PhasePending, hpcv1alpha1.ReasonContinuing, true
		v.msg += "; continuing from checkpoint " + run.CheckpointRef + " as attempt " + strconv.Itoa(int(attempt+1))
	}
	return v
}
