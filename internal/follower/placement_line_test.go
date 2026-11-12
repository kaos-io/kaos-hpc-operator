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
		{"waiting_site_monthly_limit_is_named_in_plain_words",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, PendingReason: "AssocGrpGRESMinutes",
				Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementDeclared, QOS: "default"},
				Run:       &hpcv1alpha1.RunStatus{State: "PENDING"}},
			[]string{"HPC: waiting · lane standard · time limit 60 min (declared) · queue reason: SiteLimit: " + hpcv1alpha1.SiteLimitText}},
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
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning,
				Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 15, Source: hpcv1alpha1.PlacementLearned, Samples: 5,
					QOS: "default", RefusedQOS: "test"},
				Run: &hpcv1alpha1.RunStatus{State: "RUNNING", StartTime: &start, QOS: "default"}},
			[]string{"HPC: fast lane test refused by the site; running on standard",
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
		{"wall_time_reached_prints_the_plain_guidance_from_the_message",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseFailed, Reason: hpcv1alpha1.ReasonWallTimeReached,
				Message: "This step ran its full 4 h time limit and was stopped. Set a longer limit."},
			[]string{"HPC: This step ran its full 4 h time limit and was stopped. Set a longer limit."}},
		{"waiting_long_resumable_step_shows_its_continuation_plan",
			hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted,
				Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 30, Source: hpcv1alpha1.PlacementDeclared, QOS: "default"}},
			[]string{"HPC: waiting · lane standard · time limit 30 min (declared), declared 60 min runs as continuations of up to 30 min each"}},
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

func TestShowPlacementPrintsTheWarningOnce(t *testing.T) {
	var out bytes.Buffer
	f := &follower{o: Options{Out: &out}}
	w := "Warning: this step was stopped at its 4 h limit in an earlier run and will be again unless you raise its limit on the run form (`<step>-walltime`) or make it resumable."
	job := jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted,
		Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 240, Source: hpcv1alpha1.PlacementDeclared, Warning: w}})
	f.showPlacement(job)
	job.Status.Phase = hpcv1alpha1.PhaseRunning
	job.Status.PendingReason = "Priority" // placement line changes; the warning must not repeat
	f.showPlacement(job)
	if n := strings.Count(out.String(), w); n != 1 {
		t.Fatalf("warning printed %d times:\n%s", n, out.String())
	}
	quiet := &bytes.Buffer{}
	g := &follower{o: Options{Out: quiet}}
	g.showPlacement(jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted,
		Placement: &hpcv1alpha1.Placement{TimeLimitMinutes: 240, Source: hpcv1alpha1.PlacementDeclared}}))
	if strings.Contains(quiet.String(), "Warning") {
		t.Fatalf("unexpected warning: %s", quiet.String())
	}
}

func TestResumableAndRecoveryLines(t *testing.T) {
	since := metav1.NewTime(time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC))
	place := func(rs *hpcv1alpha1.ResumableStatus) *hpcv1alpha1.Placement {
		return &hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementDeclared, QOS: "default", Resumable: rs}
	}
	cases := []struct {
		name string
		job  *hpcv1alpha1.HPCArgoWorkflowJob
		want string
	}{
		{"should_show_verified_date_while_waiting", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted,
				Placement: place(&hpcv1alpha1.ResumableStatus{Verified: true, Since: &since})})
			return j
		}(), "HPC: waiting · lane standard · time limit 60 min (declared) · resumable (verified 2026-11-07)"},
		{"should_show_why_not_verified", jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted,
			Placement: place(&hpcv1alpha1.ResumableStatus{Reason: "NoProgress: nothing new"})}),
			"HPC: waiting · lane standard · time limit 60 min (declared) · resumable not verified: NoProgress: nothing new"},
		{"should_show_opt_out", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted, Placement: place(nil)})
			j.Spec.ResumableOptOut = true
			return j
		}(), "HPC: waiting · lane standard · time limit 60 min (declared) · not resumable (resumable=False)"},
		{"should_explain_a_rerun", jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhasePending,
			Reason: hpcv1alpha1.ReasonRerunNodeFail, Reruns: 1}),
			"HPC: re-running from the start (1/2): the node failed or was taken for maintenance"},
		{"should_explain_a_preemption_rerun", jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhasePending,
			Reason: hpcv1alpha1.ReasonRerunPreempted, Reruns: 2}), "HPC: re-running from the start (2/2): the site pre-empted the job"},
		{"should_explain_no_progress", jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseFailed,
			Reason: hpcv1alpha1.ReasonNoProgress, Message: "x"}),
			"HPC: stopped: the continuation did not get past its checkpoint (NoProgress): x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := placementLines(c.job)
			if len(got) == 0 || got[len(got)-1] != c.want {
				t.Fatalf("lines = %q, want last %q", got, c.want)
			}
		})
	}
}

func TestSkippedCheckpointIsSaidWhileRunning(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 11, 7, 21, 40, 0, 0, time.UTC))
	j := jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning,
		Run: &hpcv1alpha1.RunStatus{State: "RUNNING", StartTime: &start,
			Checkpoint: &hpcv1alpha1.CheckpointInfo{SkippedReason: "over the checkpoint budget"}}})
	got := strings.Join(placementLines(j), "\n")
	if !strings.Contains(got, "HPC: checkpoint copy skipped: over the checkpoint budget") {
		t.Fatalf("lines = %q", got)
	}
}
