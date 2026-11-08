// Package resume holds the pure decisions behind discovered resumability and recovery from
// interrupted attempts (PRD-HPC-1461 D-74, D-75): what kind of end an attempt had, whether a
// continuation got past its checkpoint, what the step's history says about its checkpoints, and how
// much scratch the checkpoint copies hold. The controller gathers the facts; this package decides.
package resume

import (
	"strings"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// ExpiryDays is how long the job script keeps an abandoned step's checkpoint copies on scratch.
const ExpiryDays = 7

// End is the kind of end an attempt had, as far as recovery is concerned.
type End int

const (
	// EndStepError: the step's own failure (non-zero exit, out of memory). Never recovered.
	EndStepError End = iota
	// EndSuccess: the step completed.
	EndSuccess
	// EndCancelled: cancelled by the user or the platform. Never recovered.
	EndCancelled
	// EndContinue: the step saved and exited 85 (the stop signal before the wall time).
	EndContinue
	// EndWallTime: the site killed the job at its time limit.
	EndWallTime
	// EndIdle: the idle watchdog ended a hung step.
	EndIdle
	// EndNodeFailure: the node failed, or maintenance took it (NODE_FAIL, BOOT_FAIL, REVOKED).
	EndNodeFailure
	// EndPreempted: the site pre-empted the job.
	EndPreempted
)

// Classify maps the site's final state and the step's exit code to an End.
func Classify(state string, exit *int32) End {
	st := strings.ToUpper(state)
	switch {
	case strings.HasPrefix(st, "CANCELLED"):
		return EndCancelled
	case exit != nil && *exit == hpcv1alpha1.ExitCodeContinue:
		return EndContinue
	case exit != nil && *exit == hpcv1alpha1.ExitCodeIdle && st == "FAILED":
		return EndIdle
	case st == "COMPLETED" && (exit == nil || *exit == 0):
		return EndSuccess
	case st == "TIMEOUT":
		return EndWallTime
	case st == "NODE_FAIL" || st == "BOOT_FAIL" || st == "REVOKED":
		return EndNodeFailure
	case st == "PREEMPTED":
		return EndPreempted
	}
	return EndStepError
}

// Infrastructure reports whether the end is not the step's doing: a node or the site took the job away.
func (e End) Infrastructure() bool { return e == EndNodeFailure || e == EndPreempted }

// Recoverable reports whether a step with a checkpoint continues after this end (D-75 5).
func (e End) Recoverable() bool {
	return e == EndContinue || e == EndWallTime || e == EndIdle || e.Infrastructure()
}

// RerunReason is the status reason of a tier-1 re-run after an infrastructure end.
func (e End) RerunReason() string {
	if e == EndPreempted {
		return hpcv1alpha1.ReasonRerunPreempted
	}
	return hpcv1alpha1.ReasonRerunNodeFail
}

// Words names the end for a message.
func (e End) Words() string {
	switch e {
	case EndNodeFailure:
		return "the node failed or was taken for maintenance"
	case EndPreempted:
		return "the site pre-empted the job"
	case EndWallTime:
		return "the wall time was reached"
	case EndIdle:
		return "the idle watchdog ended the step"
	case EndContinue:
		return "the step saved before its wall time"
	}
	return "the attempt ended"
}

// Mark is a checkpoint's position: the progress values its step reported, else the content digest.
type Mark struct {
	Values []int64
	Digest string
}

// MarkOf is the Mark of a reported checkpoint (the zero Mark for nil).
func MarkOf(c *hpcv1alpha1.CheckpointInfo) Mark {
	if c == nil {
		return Mark{}
	}
	return Mark{Values: c.ProgressValues, Digest: c.Digest}
}

// MarkOfResume is the Mark of the checkpoint a continuation started from.
func MarkOfResume(r *hpcv1alpha1.ResumeInfo) Mark {
	if r == nil {
		return Mark{}
	}
	return Mark{Values: r.ProgressValues, Digest: r.Digest}
}

// Advanced reports whether cur is past prev. With progress values on both, they compare in order
// (most significant first); a longer list wins a tie. Otherwise a different, non-empty digest counts:
// the folder's content changed.
func Advanced(prev, cur Mark) bool {
	if len(prev.Values) > 0 && len(cur.Values) > 0 {
		for i := 0; i < len(prev.Values) && i < len(cur.Values); i++ {
			if cur.Values[i] != prev.Values[i] {
				return cur.Values[i] > prev.Values[i]
			}
		}
		return len(cur.Values) > len(prev.Values)
	}
	if cur.Digest == "" {
		return false
	}
	return cur.Digest != prev.Digest
}

// Evidence is what one attempt that continued from a checkpoint shows, or nil for an attempt that
// shows nothing (a first attempt, an infrastructure end without a new checkpoint).
// hasNew: the attempt reported a new checkpoint copy. A continuation that completed is verified
// without one: the job script deletes a succeeded step's copies, so none is reported, and finishing
// the run from a restored checkpoint is the strongest proof there is (live job hpc-289baf48: restored
// copy 000001, resumed at epoch 1112, completed, yet no evidence was recorded).
func Evidence(resumedFrom *hpcv1alpha1.ResumeInfo, end End, hasNew bool, cur Mark, now time.Time) *hpcv1alpha1.ResumeEvidence {
	if resumedFrom == nil {
		return nil
	}
	switch {
	case hasNew && Advanced(MarkOfResume(resumedFrom), cur):
		return &hpcv1alpha1.ResumeEvidence{Verified: true, At: metaTime(now),
			Reason: "a continuation loaded its checkpoint and got past it"}
	case hasNew:
		return &hpcv1alpha1.ResumeEvidence{Verified: false, At: metaTime(now), Reason: hpcv1alpha1.ReasonNoProgress +
			": the continuation's newest checkpoint is not past the one it started from"}
	case end == EndSuccess:
		return &hpcv1alpha1.ResumeEvidence{Verified: true, At: metaTime(now),
			Reason: "a continuation loaded its checkpoint and ran to completion"}
	case !hasNew && (end == EndWallTime || end == EndIdle || end == EndContinue):
		return &hpcv1alpha1.ResumeEvidence{Verified: false, At: metaTime(now), Reason: hpcv1alpha1.ReasonNoProgress +
			": the continuation saved no checkpoint before it ended"}
	}
	return nil
}

// Status derives the resumable status from the evidence of a step's jobs: the latest evidence decides, so
// a later failure clears an earlier verification. nil when there is none.
func Status(evidence []*hpcv1alpha1.ResumeEvidence) *hpcv1alpha1.ResumableStatus {
	var latest *hpcv1alpha1.ResumeEvidence
	for _, e := range evidence {
		if e != nil && (latest == nil || e.At.After(latest.At.Time)) {
			latest = e
		}
	}
	if latest == nil {
		return nil
	}
	at := latest.At
	return &hpcv1alpha1.ResumableStatus{Verified: latest.Verified, Since: &at, Reason: latest.Reason}
}

// Usage is the scratch held by the checkpoint copies of jobs on one site: every job that has not
// succeeded (a succeeded step's copies are deleted by its job script) and ended less than ExpiryDays ago.
func Usage(jobs []hpcv1alpha1.HPCArgoWorkflowJob, now time.Time, owns func(*hpcv1alpha1.HPCArgoWorkflowJob) bool) hpcv1alpha1.CheckpointUsage {
	var u hpcv1alpha1.CheckpointUsage
	for i := range jobs {
		j := &jobs[i]
		run := j.Status.Run
		if !owns(j) || run == nil || run.Checkpoint == nil || run.Checkpoint.StepBytes <= 0 ||
			j.Status.Phase == hpcv1alpha1.PhaseSucceeded {
			continue
		}
		if run.EndTime != nil && now.Sub(run.EndTime.Time) > ExpiryDays*24*time.Hour {
			continue
		}
		u.Bytes += run.Checkpoint.StepBytes
		u.Steps++
	}
	return u
}
