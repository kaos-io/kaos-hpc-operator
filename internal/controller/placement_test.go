package controller

import (
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func TestRunTime(t *testing.T) {
	now := ts("2026-11-06T12:00:00Z").Time
	run := func(start, end string) *hpcv1alpha1.RunStatus {
		r := &hpcv1alpha1.RunStatus{Attempt: 1}
		if start != "" {
			r.StartTime = ts(start)
		}
		if end != "" {
			r.EndTime = ts(end)
		}
		return r
	}
	cases := []struct {
		name string
		run  *hpcv1alpha1.RunStatus
		want time.Duration
		ok   bool
	}{
		{"valid_times_give_the_run_time", run("2026-11-06T10:00:00Z", "2026-11-06T10:20:00Z"), 20 * time.Minute, true},
		{"no_report_is_ignored", nil, 0, false},
		{"missing_end_is_ignored", run("2026-11-06T10:00:00Z", ""), 0, false},
		{"missing_start_is_ignored", run("", "2026-11-06T10:20:00Z"), 0, false},
		{"epoch_start_is_ignored", run("1970-01-01T00:00:00Z", "2026-11-06T10:20:00Z"), 0, false},
		{"slurm_no_val_start_in_2106_is_ignored", run("2106-02-07T06:28:14Z", "2026-11-06T10:20:00Z"), 0, false},
		{"end_in_the_future_is_ignored", run("2026-11-06T10:00:00Z", "2026-11-06T15:00:00Z"), 0, false},
		{"end_before_start_is_ignored", run("2026-11-06T10:20:00Z", "2026-11-06T10:00:00Z"), 0, false},
		{"a_report_of_an_earlier_attempt_is_ignored", &hpcv1alpha1.RunStatus{Attempt: 1,
			StartTime: ts("2026-11-06T10:00:00Z"), EndTime: ts("2026-11-06T10:20:00Z")}, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			attempt := int32(1)
			if c.name == "a_report_of_an_earlier_attempt_is_ignored" {
				attempt = 2
			}
			got, ok := runTime(c.run, attempt, now)
			if got != c.want || ok != c.ok {
				t.Fatalf("got %v %v, want %v %v", got, ok, c.want, c.ok)
			}
		})
	}
}

// D-67: a learned limit predicts how long the STEP runs. A host that kept its node for a guest (D-26) is
// sampled from its own step (run.endTime), never from its Slurm job's end (run.hostJobEndTime); a guest,
// whose own job never starts, from the step start its host wrote.
func TestSamplesAreTheStepsOwnRunTimeForHostsAndGuests(t *testing.T) {
	now := ts("2026-11-10T06:00:00Z").Time
	job := func(run hpcv1alpha1.RunStatus) *hpcv1alpha1.HPCArgoWorkflowJob {
		run.Attempt = 1
		j := &hpcv1alpha1.HPCArgoWorkflowJob{}
		j.Status.Phase, j.Status.Attempt, j.Status.Run = hpcv1alpha1.PhaseSucceeded, 1, &run
		return j
	}
	// Live 2026-11-10: host 5439966's step 04:24:34-04:26:09, its job held the node for guest 5439976 until 04:45:23.
	host := job(hpcv1alpha1.RunStatus{SiteJobID: "5439966", State: "COMPLETED", Terminal: true,
		StartTime: ts("2026-11-10T04:24:34Z"), EndTime: ts("2026-11-10T04:26:09Z"), HostJobEndTime: ts("2026-11-10T04:45:23Z")})
	guest := job(hpcv1alpha1.RunStatus{SiteJobID: "5439977", State: "COMPLETED", Terminal: true, HostSiteJobID: "5439966",
		StartTime: ts("2026-11-10T04:27:58Z"), EndTime: ts("2026-11-10T04:43:16Z")})
	got := samplesOf([]*hpcv1alpha1.HPCArgoWorkflowJob{host, guest}, now)
	want := []time.Duration{95 * time.Second, 15*time.Minute + 18*time.Second}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("samples %v, want the steps' own run times %v", got, want)
	}
}

func TestTimeoutRetry(t *testing.T) {
	site := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Profile: hpcv1alpha1.SiteProfile{
		Classes: []hpcv1alpha1.SiteClass{{Name: "c", MaxWallTimeMinutes: 2880}}}}}
	job := func(p *hpcv1alpha1.Placement, wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "c", WallTimeMinutes: wall}}
		j.Status.Placement = p
		return j
	}
	learned := &hpcv1alpha1.Placement{TimeLimitMinutes: 30, Source: hpcv1alpha1.PlacementLearned, Samples: 4}
	cases := []struct {
		name  string
		job   *hpcv1alpha1.HPCArgoWorkflowJob
		state string
		want  int32
	}{
		{"should_retry_once_at_the_declared_limit", job(learned, 240), "TIMEOUT", 240},
		{"should_cap_the_retry_at_the_spec", job(learned, 45), "TIMEOUT", 45},
		{"should_not_retry_a_declared_limit", job(&hpcv1alpha1.Placement{TimeLimitMinutes: 240, Source: hpcv1alpha1.PlacementDeclared}, 240), "TIMEOUT", 0},
		{"should_not_retry_twice", job(&hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementTimeoutRetry}, 240), "TIMEOUT", 0},
		{"should_not_retry_another_failure", job(learned, 240), "FAILED", 0},
		{"should_not_retry_a_job_without_placement", job(nil, 240), "TIMEOUT", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := timeoutRetry(c.job, site, &hpcv1alpha1.RunStatus{State: c.state})
			switch {
			case c.want == 0 && got != nil:
				t.Fatalf("got a retry %+v", got)
			case c.want > 0 && (got == nil || got.TimeLimitMinutes != c.want || got.Source != hpcv1alpha1.PlacementTimeoutRetry || got.Samples != 4):
				t.Fatalf("got %+v, want %d min timeout-retry with the samples kept", got, c.want)
			}
		})
	}
}

func TestHeldQOS(t *testing.T) {
	placed := &hpcv1alpha1.Placement{QOS: "test"}
	cases := []struct {
		name string
		p    *hpcv1alpha1.Placement
		run  *hpcv1alpha1.RunStatus
		want string
	}{
		{"should_hold_the_placed_lane", placed, nil, "test"},
		{"should_hold_the_lane_the_runner_really_used", placed, &hpcv1alpha1.RunStatus{Attempt: 1, QOS: "default"}, "default"},
		{"should_ignore_a_report_of_another_attempt", placed, &hpcv1alpha1.RunStatus{Attempt: 1, QOS: "default"}, "test"},
		{"should_release_the_lane_once_a_host_took_the_guest", placed, &hpcv1alpha1.RunStatus{Attempt: 1, QOS: "test", HostSiteJobID: "77"}, ""},
		{"should_hold_no_lane_without_a_placement", nil, nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := &hpcv1alpha1.HPCArgoWorkflowJob{}
			j.Status.Placement, j.Status.Run, j.Status.Attempt = c.p, c.run, 1
			if c.name == "should_ignore_a_report_of_another_attempt" {
				j.Status.Attempt = 2
			}
			if got := heldQOS(j); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}
