package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func ts(s string) *metav1.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	m := metav1.NewTime(t)
	return &m
}

// The live values of the 2026-11-06 node-sharing proof run (three jobs on one node).
func TestSettle(t *testing.T) {
	now := ts("2026-11-06T05:32:31Z").Time
	cases := []struct {
		name      string
		run       hpcv1alpha1.RunStatus
		nodes     int32
		wantMin   int64
		wantEnd   time.Time
		discarded int
	}{
		{"should_settle_zero_for_guest_with_no_val_start_and_valid_end",
			hpcv1alpha1.RunStatus{State: "CANCELLED", Terminal: true, StartTime: ts("2106-02-07T06:28:14Z"), EndTime: ts("2026-11-06T05:30:23Z")},
			1, 0, ts("2026-11-06T05:30:23Z").Time, 1},
		{"should_settle_to_now_for_terminal_host_without_end",
			hpcv1alpha1.RunStatus{State: "COMPLETED", Terminal: true, StartTime: ts("2026-11-06T05:28:53Z")},
			1, 4, now, 0},
		{"should_settle_zero_for_guest_with_host_site_job_id",
			hpcv1alpha1.RunStatus{State: "COMPLETED", Terminal: true, HostSiteJobID: "5427313", StartTime: ts("2026-11-06T05:28:53Z"), EndTime: ts("2026-11-06T05:32:31Z")},
			1, 0, ts("2026-11-06T05:32:31Z").Time, 0},
		{"should_settle_nodes_times_ceil_minutes_for_valid_times",
			hpcv1alpha1.RunStatus{State: "COMPLETED", Terminal: true, StartTime: ts("2026-11-06T05:00:00Z"), EndTime: ts("2026-11-06T05:10:01Z")},
			2, 22, ts("2026-11-06T05:10:01Z").Time, 0},
		{"should_treat_end_before_start_as_unknown_and_settle_to_now",
			hpcv1alpha1.RunStatus{State: "FAILED", Terminal: true, StartTime: ts("2026-11-06T05:30:00Z"), EndTime: ts("2026-11-06T05:00:00Z")},
			1, 3, now, 1},
		{"should_treat_pre_2000_and_epoch_times_as_unknown",
			hpcv1alpha1.RunStatus{State: "FAILED", Terminal: true, StartTime: ts("1970-01-01T00:00:00Z"), EndTime: ts("1999-12-31T00:00:00Z")},
			1, 0, now, 2},
		{"should_treat_end_after_now_plus_one_hour_as_unknown",
			hpcv1alpha1.RunStatus{State: "FAILED", Terminal: true, StartTime: ts("2026-11-06T05:30:31Z"), EndTime: ts("2106-02-07T06:28:14Z")},
			1, 2, now, 1},
		{"should_settle_zero_when_nothing_was_reported",
			hpcv1alpha1.RunStatus{State: "FAILED", Terminal: true}, 1, 0, now, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := settle(c.nodes, &c.run, now)
			if got.Minutes != c.wantMin || !got.End.Time.Equal(c.wantEnd) || len(got.Discarded) != c.discarded {
				t.Fatalf("got minutes=%d end=%s discarded=%v; want %d %s %d", got.Minutes, got.End, got.Discarded, c.wantMin, c.wantEnd, c.discarded)
			}
			if got.Minutes < 0 {
				t.Fatal("negative settlement")
			}
		})
	}
}

// A host whose Slurm job ends after its step: the late end raises the settlement, once.
func TestLateEnd(t *testing.T) {
	now := ts("2026-11-07T10:00:00Z").Time
	start := ts("2026-11-07T09:50:00Z")
	mk := func(run hpcv1alpha1.RunStatus, settled int64, end string) *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{}
		j.Spec.Nodes = 1
		j.Status.Phase = hpcv1alpha1.PhaseSucceeded
		j.Status.Run = &run
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, SiteJobID: "5430776", SettledNodeMinutes: settled, EndTime: *ts(end)}}
		return j
	}
	host := func(end *metav1.Time) hpcv1alpha1.RunStatus {
		return hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5430776", State: "COMPLETED", Terminal: true, StartTime: start, EndTime: end}
	}
	guest := host(ts("2026-11-07T09:53:00Z"))
	guest.HostSiteJobID = "1"
	open := func(run hpcv1alpha1.RunStatus, stillOpen bool) *hpcv1alpha1.HPCArgoWorkflowJob {
		// The step ended at 09:51; the report of the job's end carries it in hostJobEndTime (D-26, D-67).
		run.HostJobOpen = stillOpen
		if !stillOpen {
			run.HostJobEndTime, run.EndTime = run.EndTime, ts("2026-11-07T09:51:00Z")
		}
		j := mk(run, 1, "2026-11-07T09:51:00Z")
		j.Status.Attempts[0].HostJobOpen = true
		return j
	}
	cases := []struct {
		name    string
		job     *hpcv1alpha1.HPCArgoWorkflowJob
		wantMin int64
		wantOK  bool
	}{
		{"should_settle_the_job_length_when_the_host_job_ends_after_its_step", mk(host(ts("2026-11-07T09:53:00Z")), 1, "2026-11-07T09:51:00Z"), 3, true},
		{"should_change_nothing_when_the_same_end_is_reported_again", mk(host(ts("2026-11-07T09:53:00Z")), 3, "2026-11-07T09:53:00Z"), 0, false},
		{"should_never_lower_a_settlement", mk(host(ts("2026-11-07T09:50:30Z")), 1, "2026-11-07T09:51:00Z"), 0, false},
		{"should_ignore_a_no_val_late_end", mk(host(ts("2106-02-07T06:28:14Z")), 1, "2026-11-07T09:51:00Z"), 0, false},
		{"should_keep_a_guest_at_zero", mk(guest, 0, "2026-11-07T09:51:00Z"), 0, false},
		{"should_ignore_a_report_of_another_site_job", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := mk(host(ts("2026-11-07T09:53:00Z")), 1, "2026-11-07T09:51:00Z")
			j.Status.Run.SiteJobID = "other"
			return j
		}(), 0, false},
		{"should_ignore_a_job_that_is_not_terminal", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := mk(host(ts("2026-11-07T09:53:00Z")), 1, "2026-11-07T09:51:00Z")
			j.Status.Phase = hpcv1alpha1.PhaseRunning
			return j
		}(), 0, false},
		// D-26 (live 2026-11-10, host 5439966): an open host is closed by the report of its job's end.
		{"should_keep_an_open_host_open_while_its_job_still_runs", open(host(ts("2026-11-07T09:51:00Z")), true), 0, false},
		{"should_close_an_open_host_at_its_job_end", open(host(ts("2026-11-07T09:58:00Z")), false), 8, true},
		{"should_close_an_open_host_at_its_provisional_settlement_when_the_job_ended_with_its_step",
			open(host(ts("2026-11-07T09:51:00Z")), false), 1, true},
		{"should_close_an_open_host_up_to_now_when_the_site_gave_no_usable_end",
			open(host(ts("2106-02-07T06:28:14Z")), false), 10, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, st, ok := lateEnd(c.job, now)
			if ok != c.wantOK || (ok && st.Minutes != c.wantMin) {
				t.Fatalf("got ok=%v minutes=%d; want ok=%v minutes=%d", ok, st.Minutes, c.wantOK, c.wantMin)
			}
		})
	}
}

// Live 2026-11-08 (site job 5435560): a failed host's terminal report carried no start and settled 0
// node-minutes for ~11 minutes of a GPU node. A job that provably ran settles from the booking.
func TestSettleAttempt(t *testing.T) {
	now := ts("2026-11-08T21:56:00Z").Time
	bookedAt := *ts("2026-11-08T21:44:00Z")
	one := int32(1)
	mk := func(run hpcv1alpha1.RunStatus, booked bool, nodeMinutes int64) *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{}
		j.Spec.Nodes = 1
		run.SiteJobID = "5435560"
		j.Status.Run = &run
		if booked {
			j.Status.Booking = &hpcv1alpha1.Booking{At: bookedAt, NodeMinutes: nodeMinutes}
		}
		return j
	}
	end := ts("2026-11-08T21:55:43Z")
	cases := []struct {
		name    string
		job     *hpcv1alpha1.HPCArgoWorkflowJob
		wantMin int64
	}{
		{"should_settle_a_failed_host_without_start_from_the_booking",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true, EndTime: end}, true, 240), 12},
		{"should_settle_a_failed_non_host_with_a_start_from_the_site_times",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true, StartTime: ts("2026-11-08T21:44:52Z"), EndTime: end}, true, 240), 11},
		{"should_settle_a_failed_job_without_start_and_end_up_to_now",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true}, true, 240), 12},
		{"should_settle_a_running_job_without_start_up_to_now_for_a_sweep",
			mk(hpcv1alpha1.RunStatus{State: "RUNNING"}, true, 240), 12},
		{"should_never_settle_more_than_was_booked",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true, EndTime: end}, true, 5), 5},
		{"should_settle_zero_for_a_cancelled_job_that_never_started",
			mk(hpcv1alpha1.RunStatus{State: "CANCELLED", ExitCode: &one, Terminal: true, EndTime: end}, true, 240), 0},
		{"should_settle_zero_for_a_failed_record_without_exit_code",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", Terminal: true, EndTime: end}, true, 240), 0},
		{"should_settle_zero_for_a_failure_before_any_site_job_exists",
			func() *hpcv1alpha1.HPCArgoWorkflowJob {
				j := mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true, EndTime: end}, true, 240)
				j.Status.Run.SiteJobID = ""
				return j
			}(), 0},
		{"should_settle_zero_for_a_guest",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true, HostSiteJobID: "1", EndTime: end}, true, 240), 0},
		{"should_settle_zero_without_a_booking_to_start_from",
			mk(hpcv1alpha1.RunStatus{State: "FAILED", ExitCode: &one, Terminal: true, EndTime: end}, false, 0), 0},
		{"should_prefer_a_valid_site_start_over_the_booking",
			mk(hpcv1alpha1.RunStatus{State: "COMPLETED", ExitCode: &one, Terminal: true, StartTime: ts("2026-11-08T21:50:00Z"), EndTime: end}, true, 240), 6},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := settleAttempt(c.job, now); got.Minutes != c.wantMin {
				t.Fatalf("got %d node-minutes, want %d", got.Minutes, c.wantMin)
			}
		})
	}
}
