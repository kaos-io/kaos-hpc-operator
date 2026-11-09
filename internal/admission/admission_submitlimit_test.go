package admission

import (
	"strings"
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// PRD-HPC-1461 AC-39: at the site's submit cap a new job FAILS with SubmitLimit (it does not wait).
func TestDecideSubmitLimit(t *testing.T) {
	capped := func(cap, conc int32) *hpcv1alpha1.HPCSite {
		return site(func(s *hpcv1alpha1.HPCSite) {
			s.Spec.Profile.MaxSubmitJobs = cap
			s.Spec.Limits.MaxConcurrentJobs = conc
			s.Spec.Budget.GPUNodeHours = 1000
		})
	}
	cases := []struct {
		name     string
		site     *hpcv1alpha1.HPCSite
		siteJobs int32
		phase    hpcv1alpha1.Phase
		reason   string
	}{
		{"should_admit_when_below_the_submit_cap", capped(5, 0), 4, hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted},
		{"should_fail_with_submit_limit_at_the_cap", capped(5, 0), 5, hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonSubmitLimit},
		{"should_fail_with_submit_limit_above_the_cap", capped(5, 0), 9, hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonSubmitLimit},
		{"should_not_apply_a_cap_of_zero", capped(0, 0), 50, hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted},
		{"should_keep_waiting_on_the_concurrency_limit_when_it_is_the_lower_one", capped(5, 3), 3,
			hpcv1alpha1.PhaseWaiting, hpcv1alpha1.ReasonConcurrencyLimit},
		{"should_fail_on_the_cap_when_the_concurrency_limit_is_higher", capped(5, 10), 5,
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonSubmitLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := led(c.site, 0, 0, c.siteJobs)
			l.SiteJobs = c.siteJobs
			d := Decide(job("meluxina-gpu", 60, 1), c.site, l)
			if d.Phase != c.phase || d.Reason != c.reason {
				t.Fatalf("got %s/%s (%s), want %s/%s", d.Phase, d.Reason, d.Message, c.phase, c.reason)
			}
			if c.reason == hpcv1alpha1.ReasonSubmitLimit && !strings.Contains(d.Message, "5") {
				t.Fatalf("the message names the cap: %q", d.Message)
			}
		})
	}
}

// Guests ride on a host's Slurm job: they are active but are not counted against the cap.
func TestDecideSubmitLimitCountsSiteJobsNotGuests(t *testing.T) {
	s := site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Profile.MaxSubmitJobs = 2; s.Spec.Budget.GPUNodeHours = 1000 })
	l := led(s, 0, 0, 6) // six active, two of them site jobs
	l.SiteJobs = 1
	if d := Decide(job("meluxina-gpu", 60, 1), s, l); d.Phase != hpcv1alpha1.PhaseAdmitted {
		t.Fatalf("guests must not count: %s/%s", d.Phase, d.Reason)
	}
}
