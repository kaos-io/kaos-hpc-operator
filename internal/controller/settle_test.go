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
