package resume

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func i32(v int32) *int32 { return &v }

func TestClassify(t *testing.T) {
	cases := []struct {
		name  string
		state string
		exit  *int32
		want  End
	}{
		{"should_be_success_when_completed_with_exit_0", "COMPLETED", i32(0), EndSuccess},
		{"should_be_step_error_when_failed_with_exit_1", "FAILED", i32(1), EndStepError},
		{"should_be_step_error_when_out_of_memory", "OUT_OF_MEMORY", i32(0), EndStepError},
		{"should_be_continue_when_exit_85", "FAILED", i32(85), EndContinue},
		{"should_be_wall_time_when_timeout", "TIMEOUT", nil, EndWallTime},
		{"should_be_node_failure_when_node_fail", "NODE_FAIL", nil, EndNodeFailure},
		{"should_be_node_failure_when_boot_fail", "BOOT_FAIL", nil, EndNodeFailure},
		{"should_be_preempted_when_preempted", "PREEMPTED", nil, EndPreempted},
		{"should_be_idle_when_failed_with_236", "FAILED", i32(236), EndIdle},
		{"should_be_step_error_when_236_but_not_failed", "COMPLETED", i32(236), EndStepError},
		{"should_be_cancelled_when_cancelled_by", "CANCELLED by 1000", nil, EndCancelled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.state, c.exit); got != c.want {
				t.Fatalf("Classify(%q, %v) = %v, want %v", c.state, c.exit, got, c.want)
			}
		})
	}
}

func TestRecoverableNeverCoversTheStepsOwnError(t *testing.T) {
	for _, e := range []End{EndStepError, EndSuccess, EndCancelled} {
		if e.Recoverable() {
			t.Errorf("end %v must not be recoverable", e)
		}
	}
	for _, e := range []End{EndContinue, EndWallTime, EndIdle, EndNodeFailure, EndPreempted} {
		if !e.Recoverable() {
			t.Errorf("end %v must be recoverable", e)
		}
	}
	if EndIdle.Infrastructure() || EndWallTime.Infrastructure() {
		t.Error("idle and wall time are the step's, not the infrastructure's: no tier-1 re-run")
	}
}

func TestAdvanced(t *testing.T) {
	cases := []struct {
		name      string
		prev, cur Mark
		want      bool
	}{
		{"should_advance_when_first_checkpoint_has_a_digest", Mark{}, Mark{Digest: "a"}, true},
		{"should_not_advance_when_nothing_is_reported", Mark{Digest: "a"}, Mark{}, false},
		{"should_advance_when_epoch_grows", Mark{Values: []int64{3, 100}}, Mark{Values: []int64{4, 0}}, true},
		{"should_advance_when_step_grows_in_the_same_epoch", Mark{Values: []int64{3, 100}}, Mark{Values: []int64{3, 101}}, true},
		{"should_not_advance_when_equal", Mark{Values: []int64{3, 100}}, Mark{Values: []int64{3, 100}}, false},
		{"should_not_advance_when_step_goes_back", Mark{Values: []int64{3, 100}}, Mark{Values: []int64{3, 50}}, false},
		{"should_not_advance_when_epoch_goes_back_though_step_is_larger", Mark{Values: []int64{3, 1}}, Mark{Values: []int64{2, 999}}, false},
		{"should_advance_when_digest_differs_and_no_values", Mark{Digest: "a"}, Mark{Digest: "b"}, true},
		{"should_not_advance_when_digest_is_the_same", Mark{Digest: "a"}, Mark{Digest: "a"}, false},
		{"should_prefer_values_over_digest", Mark{Values: []int64{5}, Digest: "a"}, Mark{Values: []int64{5}, Digest: "b"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Advanced(c.prev, c.cur); got != c.want {
				t.Fatalf("Advanced = %v, want %v", got, c.want)
			}
		})
	}
}

func TestEvidence(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	from := &hpcv1alpha1.ResumeInfo{ProgressValues: []int64{3}}
	cases := []struct {
		name      string
		from      *hpcv1alpha1.ResumeInfo
		end       End
		hasNew    bool
		cur       Mark
		wantNil   bool
		wantVerif bool
	}{
		{"should_verify_when_the_continuation_got_past_its_checkpoint", from, EndWallTime, true, Mark{Values: []int64{4}}, false, true},
		{"should_fail_with_no_progress_when_the_new_checkpoint_is_not_past", from, EndWallTime, true, Mark{Values: []int64{3}}, false, false},
		{"should_fail_when_a_wall_time_end_saved_nothing_new", from, EndWallTime, false, Mark{}, false, false},
		{"should_fail_when_an_idle_end_saved_nothing_new", from, EndIdle, false, Mark{}, false, false},
		{"should_show_nothing_when_the_node_failed_before_any_new_checkpoint", from, EndNodeFailure, false, Mark{}, true, false},
		{"should_show_nothing_when_the_step_errored", from, EndStepError, false, Mark{}, true, false},
		{"should_verify_when_the_continuation_completed_though_a_success_leaves_no_new_copy", from, EndSuccess, false, Mark{}, false, true},
		{"should_show_nothing_for_a_first_attempt", nil, EndWallTime, true, Mark{Digest: "a"}, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Evidence(c.from, c.end, c.hasNew, c.cur, now)
			if (got == nil) != c.wantNil {
				t.Fatalf("evidence = %v, want nil=%v", got, c.wantNil)
			}
			if got != nil && got.Verified != c.wantVerif {
				t.Fatalf("verified = %v, want %v (%s)", got.Verified, c.wantVerif, got.Reason)
			}
		})
	}
}

func TestStatusLatestEvidenceWinsSoALaterFailureClearsAVerification(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Hour))
	ok := &hpcv1alpha1.ResumeEvidence{Verified: true, At: t0}
	bad := &hpcv1alpha1.ResumeEvidence{Verified: false, At: t1, Reason: "NoProgress: x"}
	if s := Status([]*hpcv1alpha1.ResumeEvidence{ok, nil, bad}); s == nil || s.Verified || s.Reason != "NoProgress: x" {
		t.Fatalf("a later failure must clear: %+v", s)
	}
	if s := Status([]*hpcv1alpha1.ResumeEvidence{bad, ok}); s == nil || s.Verified {
		t.Fatalf("order must not matter: %+v", s)
	}
	later := &hpcv1alpha1.ResumeEvidence{Verified: true, At: metav1.NewTime(t1.Add(time.Hour))}
	if s := Status([]*hpcv1alpha1.ResumeEvidence{ok, bad, later}); s == nil || !s.Verified {
		t.Fatalf("a later pass must restore: %+v", s)
	}
	if Status(nil) != nil || Status([]*hpcv1alpha1.ResumeEvidence{nil}) != nil {
		t.Fatal("no evidence, no status")
	}
}

func TestUsageCountsHeldCopiesOnly(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	end := func(d time.Duration) *metav1.Time { m := metav1.NewTime(now.Add(-d)); return &m }
	job := func(phase hpcv1alpha1.Phase, bytes int64, endAge time.Duration) hpcv1alpha1.HPCArgoWorkflowJob {
		return hpcv1alpha1.HPCArgoWorkflowJob{Status: hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: phase,
			Run: &hpcv1alpha1.RunStatus{EndTime: end(endAge), Checkpoint: &hpcv1alpha1.CheckpointInfo{StepBytes: bytes}}}}
	}
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		job(hpcv1alpha1.PhaseRunning, 100, time.Hour),
		job(hpcv1alpha1.PhaseFailed, 50, 24*time.Hour),
		job(hpcv1alpha1.PhaseSucceeded, 999, time.Hour),   // deleted by its job script
		job(hpcv1alpha1.PhaseFailed, 777, 8*24*time.Hour), // expired
		job(hpcv1alpha1.PhaseRunning, 0, time.Hour),       // holds nothing
	}
	u := Usage(jobs, now, func(*hpcv1alpha1.HPCArgoWorkflowJob) bool { return true })
	if u.Bytes != 150 || u.Steps != 2 {
		t.Fatalf("usage = %+v, want 150 bytes in 2 steps", u)
	}
	if none := Usage(jobs, now, func(*hpcv1alpha1.HPCArgoWorkflowJob) bool { return false }); none.Bytes != 0 {
		t.Fatalf("other sites' jobs must not count: %+v", none)
	}
}
