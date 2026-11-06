package follower

import (
	"bytes"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func jobWith(st hpcv1alpha1.HPCArgoWorkflowJobStatus) *hpcv1alpha1.HPCArgoWorkflowJob {
	j := &hpcv1alpha1.HPCArgoWorkflowJob{}
	j.Spec.WallTimeMinutes = 60
	j.Status = st
	return j
}

func TestPlacementLines(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 11, 7, 21, 40, 0, 0, time.UTC))
	learned := &hpcv1alpha1.Placement{TimeLimitMinutes: 15, Source: hpcv1alpha1.PlacementLearned, Samples: 5, QOS: "test"}
	cases := []struct {
		name string
		st   hpcv1alpha1.HPCArgoWorkflowJobStatus
		want []string
	}{
		{"nothing_known_prints_nothing", hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhasePending}, nil},
		{"waiting_learned_test_lane_with_queue_reason",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Placement: learned, PendingReason: "Priority",
				Run: &hpcv1alpha1.RunStatus{State: "PENDING"}},
			[]string{"HPC: waiting · lane test · time limit 15 min (learned from 5 runs) · queue reason: Priority"}},
		{"waiting_declared_standard_no_reason",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted,
				Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementDeclared, QOS: "default"}},
			[]string{"HPC: waiting · lane standard · time limit 60 min (declared)"}},
		{"waiting_without_placement_uses_declared_wall_time",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, PendingReason: "Resources"},
			[]string{"HPC: waiting · lane standard · time limit 60 min (declared) · queue reason: Resources"}},
		{"waiting_reserved_nodes_lane",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning,
				Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 10, Source: hpcv1alpha1.PlacementLearned, Samples: 1, Reservation: "gpudev"}},
			[]string{"HPC: waiting · lane dev (reserved nodes gpudev) · time limit 10 min (learned from 1 run)"}},
		{"waiting_timeout_retry_source",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning,
				Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementTimeoutRetry}},
			[]string{"HPC: waiting · lane standard · time limit 60 min (retry at the full limit after a timeout)"}},
		{"running_since_start_time",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Placement: learned,
				Run: &hpcv1alpha1.RunStatus{State: "RUNNING", StartTime: &start, QOS: "test"}},
			[]string{"HPC: running since 21:40 UTC · lane test · time limit 15 min (learned from 5 runs)"}},
		{"running_without_start_time",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Placement: learned,
				Run: &hpcv1alpha1.RunStatus{State: "RUNNING", QOS: "test"}},
			[]string{"HPC: running · lane test · time limit 15 min (learned from 5 runs)"}},
		{"fallback_when_site_refused_fast_lane",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Placement: learned,
				Run: &hpcv1alpha1.RunStatus{State: "RUNNING", StartTime: &start, QOS: ""}},
			[]string{"HPC: fast lane refused by the site; running on standard",
				"HPC: running since 21:40 UTC · lane standard · time limit 15 min (learned from 5 runs)"}},
		{"idle_with_last_log",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseFailed, Reason: hpcv1alpha1.ReasonIdle,
				Run: &hpcv1alpha1.RunStatus{LastLog: "epoch 3\nloading batch\n"}},
			[]string{"HPC: stopped: no GPU use and no output for 20 min (idle watchdog); last lines below", "epoch 3\nloading batch"}},
		{"idle_without_last_log",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseFailed, Reason: hpcv1alpha1.ReasonIdle},
			[]string{"HPC: stopped: no GPU use and no output for 20 min (idle watchdog); last lines below"}},
		{"time_limit_retry",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted, Reason: hpcv1alpha1.ReasonTimeLimitRetry},
			[]string{"HPC: hit its learned time limit; retrying once with the full limit"}},
		{"wall_time_reached",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseFailed, Reason: hpcv1alpha1.ReasonWallTimeReached},
			[]string{"HPC: stopped at its time limit"}},
		{"succeeded_prints_nothing_extra", hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded, Placement: learned}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := placementLines(jobWith(tc.st))
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShowPlacementPrintsOnlyOnChange(t *testing.T) {
	var out bytes.Buffer
	f := &follower{o: Options{Out: &out}}
	pl := &hpcv1alpha1.Placement{TimeLimitMinutes: 15, Source: hpcv1alpha1.PlacementLearned, Samples: 5, QOS: "test"}
	job := jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Placement: pl, PendingReason: "Priority"})
	f.showPlacement(job)
	f.showPlacement(job) // same state: no second line
	job.Status.Message = "unrelated change"
	f.showPlacement(job)
	job.Status.PendingReason = "Resources"
	f.showPlacement(job)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.HasSuffix(lines[0], "queue reason: Priority") || !strings.HasSuffix(lines[1], "queue reason: Resources") {
		t.Fatalf("unexpected output: %q", lines)
	}
}
