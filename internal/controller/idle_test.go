package controller

import (
	"testing"

	"k8s.io/utils/ptr"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func TestOutcomeMapsTheIdleExitToFailedIdle(t *testing.T) {
	cases := []struct {
		name       string
		state      string
		code       *int32
		wantPhase  hpcv1alpha1.Phase
		wantReason string
	}{
		{"should_map_failed_with_the_idle_exit_to_failed_idle", "FAILED", ptr.To(hpcv1alpha1.ExitCodeIdle), hpcv1alpha1.PhaseFailed, "Idle"},
		{"should_not_call_a_completed_job_idle", "COMPLETED", ptr.To(hpcv1alpha1.ExitCodeIdle), hpcv1alpha1.PhaseFailed, "SiteCOMPLETED"},
		{"should_keep_other_failures_as_site_state", "FAILED", ptr.To(int32(1)), hpcv1alpha1.PhaseFailed, "SiteFAILED"},
		{"should_map_failed_with_the_gpu_violation_exit_to_gpu_violation", "FAILED", ptr.To(hpcv1alpha1.ExitCodeGPUViolation), hpcv1alpha1.PhaseFailed, "GPUViolation"},
		{"should_keep_a_timeout_a_wall_time_end", "TIMEOUT", ptr.To(int32(0)), hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonWallTimeReached},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			phase, reason := outcome(&hpcv1alpha1.RunStatus{State: c.state, ExitCode: c.code})
			if phase != c.wantPhase || reason != c.wantReason {
				t.Fatalf("got %s/%s, want %s/%s", phase, reason, c.wantPhase, c.wantReason)
			}
		})
	}
}

func TestNodeMinutesSaved(t *testing.T) {
	job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{WallTimeMinutes: 240, Nodes: 2}}
	end := ts("2026-11-06T05:30:00Z")
	cases := []struct {
		name string
		run  hpcv1alpha1.RunStatus
		want int64
	}{
		{"should_save_limit_minus_elapsed_times_nodes", hpcv1alpha1.RunStatus{StartTime: ts("2026-11-06T05:00:00Z")}, 2 * 210},
		{"should_save_nothing_for_a_guest", hpcv1alpha1.RunStatus{HostSiteJobID: "9", StartTime: ts("2026-11-06T05:00:00Z")}, 0},
		{"should_save_nothing_without_a_start", hpcv1alpha1.RunStatus{}, 0},
		{"should_save_nothing_past_the_limit", hpcv1alpha1.RunStatus{StartTime: ts("2026-11-06T00:00:00Z")}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := nodeMinutesSaved(job, &c.run, *end); got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}
}
