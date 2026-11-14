package controller

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// PRD kaos#1911 AC-13 (S13): whatever the site reports, the chain of attempts of one step ends. Random ends
// are fed through recoverAttempt the way the reconciler feeds them; the chain may never be longer than the first
// attempt, MaxReruns re-runs and the continuation cap, plus the one that is refused.
func TestRecoveryChainsTerminate(t *testing.T) {
	states := []struct {
		state string
		exit  *int32
	}{
		{"TIMEOUT", nil}, {"NODE_FAIL", nil}, {"BOOT_FAIL", nil}, {"PREEMPTED", nil}, {"REVOKED", nil},
		{"FAILED", i32p(1)}, {"FAILED", i32p(hpcv1alpha1.ExitCodeContinue)}, {"FAILED", i32p(hpcv1alpha1.ExitCodeIdle)},
		{"OUT_OF_MEMORY", nil}, {"CANCELLED by 1000", nil}, {"COMPLETED", i32p(0)},
	}
	rng := rand.New(rand.NewSource(20261114))
	r := &HPCArgoWorkflowJobReconciler{Now: func() time.Time { return time.Unix(1_800_000_000, 0) }}
	for trial := range 2000 {
		cap := int32(1 + rng.Intn(6))
		job := &hpcv1alpha1.HPCArgoWorkflowJob{}
		job.Spec.StepName = "train"
		job.Spec.WallTimeMinutes = 120
		job.Spec.Resumable = &hpcv1alpha1.Resumable{MaxContinuations: cap}
		job.Status.Attempt = 1
		limit := 1 + hpcv1alpha1.MaxReruns + cap + 1
		attempts := int32(0)
		for {
			attempts++
			if attempts > limit {
				t.Fatalf("trial %d: chain still going after %d attempts (bound %d)", trial, attempts, limit)
			}
			s := states[rng.Intn(len(states))]
			run := &hpcv1alpha1.RunStatus{State: s.state, ExitCode: s.exit}
			if rng.Intn(2) == 0 { // a new checkpoint, sometimes past the last one, sometimes not
				run.CheckpointRef = fmt.Sprintf("ckpt-%d", attempts)
				run.Checkpoint = &hpcv1alpha1.CheckpointInfo{Digest: fmt.Sprintf("d%d", rng.Intn(3)), ProgressValues: []int64{int64(rng.Intn(4))}}
			}
			v := r.recoverAttempt(job, run, timelimit.Caps{Spec: 120}, job.Status.Attempt, attemptEnd{phase: hpcv1alpha1.PhaseFailed})
			if !v.next {
				break
			}
			if job.Status.Reruns > hpcv1alpha1.MaxReruns || job.Status.Continuations > cap {
				t.Fatalf("trial %d: reruns %d / continuations %d beyond their bounds (%d / %d)",
					trial, job.Status.Reruns, job.Status.Continuations, hpcv1alpha1.MaxReruns, cap)
			}
		}
	}
}

func i32p(v int32) *int32 { return &v }
