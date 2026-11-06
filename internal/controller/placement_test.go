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

func TestTimeoutRetry(t *testing.T) {
	site := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Profile: hpcv1alpha1.SiteProfile{
		Classes: []hpcv1alpha1.SiteClass{{Name: "c", MaxWallTimeMinutes: 2880}}}}}
	job := func(p *hpcv1alpha1.Placement, wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "c", WallTimeMinutes: wall}}
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
		{"should_retry_once_with_the_doubled_limit", job(learned, 240), "TIMEOUT", 60},
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
