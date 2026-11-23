package controller

import (
	"strings"
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// D-75: the cap ending counts the jobs that ran. An attempt that never started at the site (a node failure
// before the start, re-run) is an attempt, not a job that used the step's time.
func TestTheCapEndingCountsTheJobsThatRan(t *testing.T) {
	job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{StepName: "train", WallTimeMinutes: 60}}
	job.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1}, {Attempt: 2, WallMinutes: 30}, {Attempt: 3, WallMinutes: 30}}
	run := &hpcv1alpha1.RunStatus{Attempt: 3, State: "TIMEOUT", Terminal: true, CheckpointRef: "scratch://ckpt/b"}
	r := &HPCArgoWorkflowJobReconciler{}
	v := r.recoverAttempt(job, run, 3, attemptEnd{phase: hpcv1alpha1.PhaseFailed, reason: hpcv1alpha1.ReasonWallTimeReached,
		msg: "attempt 3 settled 30 node-minutes"})
	if v.reason != hpcv1alpha1.ReasonWallTimeReached || !v.spent || v.next {
		t.Fatalf("got %+v, want the cap ending", v)
	}
	if want := "; it reached the platform's cap of 1 h (2 jobs)"; !strings.HasSuffix(v.msg, want) {
		t.Fatalf("message %q, want it to end with %q", v.msg, want)
	}
}

func TestTheCapEndingOfOneJobSaysOneJob(t *testing.T) {
	job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{StepName: "train", WallTimeMinutes: 60}}
	job.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, WallMinutes: 60}}
	run := &hpcv1alpha1.RunStatus{Attempt: 1, State: "TIMEOUT", Terminal: true, CheckpointRef: "scratch://ckpt/a"}
	v := (&HPCArgoWorkflowJobReconciler{}).recoverAttempt(job, run, 1, attemptEnd{phase: hpcv1alpha1.PhaseFailed,
		reason: hpcv1alpha1.ReasonWallTimeReached})
	if want := "; it reached the platform's cap of 1 h (1 job)"; v.msg != want {
		t.Fatalf("message %q, want %q", v.msg, want)
	}
}
