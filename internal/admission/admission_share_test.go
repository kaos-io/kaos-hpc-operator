package admission

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

var shareNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// shareProject is a project with a 10 GPU-hour share per month bounded by siteHours reported left
// 30 minutes ago, one admitted 4-hour job booked, and used node-minutes of settled spend this month.
func shareProject(siteHours int64, policy hpcv1alpha1.AtLimitPolicy, booked, used int64) (*hpcv1alpha1.HPCSite, ledger.Ledger) {
	obs := metav1.NewTime(shareNow.Add(-30 * time.Minute))
	s := site(func(s *hpcv1alpha1.HPCSite) {
		s.Name, s.Namespace = "meluxina", "ns"
		s.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceShare, GPUNodeHours: 10, AtLimit: policy,
			SiteLeft: &hpcv1alpha1.SiteLeft{GPUNodeHours: siteHours, ObservedAt: &obs}}
	})
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	if used > 0 {
		j := hpcv1alpha1.HPCArgoWorkflowJob{}
		j.Status.Site = &hpcv1alpha1.SiteRef{Name: "meluxina", Namespace: "ns"}
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: hpcv1alpha1.TierGPU, SettledNodeMinutes: used,
			EndTime: metav1.NewTime(shareNow.Add(-time.Hour)), Outcome: "Completed"}}
		jobs = append(jobs, j)
	}
	pending := map[string]hpcv1alpha1.Booking{}
	if booked > 0 {
		pending["b"] = hpcv1alpha1.Booking{SiteName: "meluxina", SiteNamespace: "ns", Tier: hpcv1alpha1.TierGPU, NodeMinutes: booked}
	}
	return s, ledger.Compute(s, jobs, pending, shareNow)
}

// PRD-HPC-1461 D-12, D-13, AC-21, AC-22 with a project share bounded by the site.
func TestDecide_share_budget(t *testing.T) {
	cases := []struct {
		name         string
		siteHours    int64
		policy       hpcv1alpha1.AtLimitPolicy
		booked, used int64
		wall         int32
		phase        hpcv1alpha1.Phase
		reason       string
		over         bool
	}{
		{name: "should_admit_when_the_job_fits_the_share_and_the_site",
			siteHours: 50, booked: 0, used: 0, wall: 240, phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted},
		{name: "should_wait_for_budget_when_bookings_hold_the_share_it_would_fit_once_they_settle",
			siteHours: 50, booked: 480, used: 0, wall: 240, phase: hpcv1alpha1.PhaseWaiting, reason: hpcv1alpha1.ReasonWaitingForBudget},
		{name: "should_refuse_budget_blocked_when_the_share_can_never_cover_it_this_month",
			siteHours: 50, booked: 0, used: 500, wall: 240, phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonBudgetBlocked},
		{name: "should_refuse_site_limit_when_the_site_has_less_left_than_the_share",
			siteHours: 3, booked: 0, used: 0, wall: 240, phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonSiteLimit},
		{name: "should_admit_exactly_the_smaller_of_share_and_site",
			siteHours: 4, booked: 0, used: 0, wall: 240, phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted},
		{name: "should_admit_beyond_the_share_and_flag_it_under_warn",
			siteHours: 50, policy: hpcv1alpha1.AtLimitWarn, used: 500, wall: 240,
			phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted, over: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, l := shareProject(c.siteHours, c.policy, c.booked, c.used)
			d := Decide(job("meluxina-gpu", c.wall, 1), s, l)
			if d.Phase != c.phase || d.Reason != c.reason || d.OverShare != c.over {
				t.Fatalf("got %s/%s over=%v (%s), want %s/%s over=%v", d.Phase, d.Reason, d.OverShare, d.Message, c.phase, c.reason, c.over)
			}
		})
	}
}

// AC-21: five 4-hour jobs against a 10-hour share: two fit, three wait, none is refused.
func TestDecide_share_budget_burst_never_overbooks(t *testing.T) {
	s, _ := shareProject(50, "", 0, 0)
	var booked int64
	admitted, waiting := 0, 0
	for i := 0; i < 5; i++ {
		_, l := shareProject(50, "", booked, 0)
		d := Decide(job("meluxina-gpu", 240, 1), s, l)
		switch d.Phase {
		case hpcv1alpha1.PhaseAdmitted:
			admitted++
			booked += d.Cost
		case hpcv1alpha1.PhaseWaiting:
			waiting++
		default:
			t.Fatalf("job %d: %s/%s", i, d.Phase, d.Reason)
		}
	}
	if admitted != 2 || waiting != 3 || booked > 600 {
		t.Fatalf("admitted %d waiting %d booked %d, want 2/3/<=600", admitted, waiting, booked)
	}
}
