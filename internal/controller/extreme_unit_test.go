package controller

import (
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Extreme cases decided by pure functions (docs/extreme-cases.md, sections 4 and 6).

func TestOutcomeOfEverySiteEndStateOfAnAttempt(t *testing.T) {
	zero, three, idle, cont := int32(0), int32(3), hpcv1alpha1.ExitCodeIdle, hpcv1alpha1.ExitCodeContinue
	cases := []struct {
		name   string
		state  string
		code   *int32
		phase  hpcv1alpha1.Phase
		reason string
	}{
		{"should_succeed_on_completed_with_exit_0", "COMPLETED", &zero, hpcv1alpha1.PhaseSucceeded, "Completed"},
		{"should_succeed_on_completed_without_an_exit_code", "COMPLETED", nil, hpcv1alpha1.PhaseSucceeded, "Completed"},
		{"should_fail_on_completed_with_a_nonzero_exit_code", "COMPLETED", &three, hpcv1alpha1.PhaseFailed, "SiteCOMPLETED"},
		{"should_fail_a_lost_node_as_site_node_fail", "NODE_FAIL", nil, hpcv1alpha1.PhaseFailed, "SiteNODE_FAIL"},
		{"should_fail_a_preempted_job_as_site_preempted", "PREEMPTED", nil, hpcv1alpha1.PhaseFailed, "SitePREEMPTED"},
		{"should_fail_an_oom_kill_as_site_out_of_memory", "OUT_OF_MEMORY", nil, hpcv1alpha1.PhaseFailed, "SiteOUT_OF_MEMORY"},
		{"should_fail_a_plain_failure", "FAILED", &three, hpcv1alpha1.PhaseFailed, "SiteFAILED"},
		{"should_name_the_wall_time_on_timeout", "TIMEOUT", nil, hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonWallTimeReached},
		{"should_cancel_on_cancelled", "CANCELLED", nil, hpcv1alpha1.PhaseCancelled, "Cancelled"},
		{"should_cancel_on_cancelled_by_uid", "CANCELLED by 1234", nil, hpcv1alpha1.PhaseCancelled, "Cancelled"},
		{"should_read_a_lowercase_state", "cancelled", nil, hpcv1alpha1.PhaseCancelled, "Cancelled"},
		{"should_fail_an_empty_state_as_ended", "", nil, hpcv1alpha1.PhaseFailed, "Ended"},
		{"should_name_the_idle_kill", "FAILED", &idle, hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonIdle},
		{"should_not_call_a_completed_idle_exit_an_idle_kill", "COMPLETED", &idle, hpcv1alpha1.PhaseFailed, "SiteCOMPLETED"},
		{"should_report_a_continuation_exit_as_continued", "FAILED", &cont, hpcv1alpha1.PhaseFailed, "Continued"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			phase, reason := outcome(&hpcv1alpha1.RunStatus{State: c.state, ExitCode: c.code})
			if phase != c.phase || reason != c.reason {
				t.Fatalf("got %s/%s, want %s/%s", phase, reason, c.phase, c.reason)
			}
		})
	}
}

func TestSettleOfAHostLostToNodeFailChargesItsRunNotTheBooking(t *testing.T) {
	now := ts("2026-11-07T12:00:00Z").Time
	run := &hpcv1alpha1.RunStatus{State: "NODE_FAIL", StartTime: ts("2026-11-07T11:10:00Z"), EndTime: ts("2026-11-07T11:50:00Z")}
	if st := settle(1, run, now); st.Minutes != 40 || len(st.Discarded) != 0 {
		t.Fatalf("got %d min discarded %v, want 40 and none", st.Minutes, st.Discarded)
	}
	// NODE_FAIL often reports no end time: the controller settles to the moment it saw the end, never to zero.
	run.EndTime = nil
	if st := settle(1, run, now); st.Minutes != 50 {
		t.Fatalf("got %d, want 50 (start to now)", st.Minutes)
	}
}

func TestRunTimeOfZeroLengthIsNotASampleOfTheStep(t *testing.T) {
	now := ts("2026-11-07T12:00:00Z").Time
	run := &hpcv1alpha1.RunStatus{Attempt: 1, StartTime: ts("2026-11-07T10:00:00Z"), EndTime: ts("2026-11-07T10:00:00Z")}
	if d, ok := runTime(run, 1, now); ok {
		t.Fatalf("a run of %v (start == end) was taken as a sample: three of them would learn the 15-minute floor", d)
	}
}
