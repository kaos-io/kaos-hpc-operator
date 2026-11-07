package admission

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

// siteSourceProject is a project with no share: the budget is what the site reported left (source
// site), observed an hour ago, with usedSince node-minutes ended after that.
func siteSourceProject(leftHours, usedSince int64) (*hpcv1alpha1.HPCSite, ledger.Ledger) {
	obs := metav1.NewTime(shareNow.Add(-time.Hour))
	s := site(func(s *hpcv1alpha1.HPCSite) {
		s.Name, s.Namespace = "meluxina", "ns"
		s.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, GPUNodeHours: leftHours, ObservedAt: &obs}
	})
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	if usedSince > 0 {
		j := hpcv1alpha1.HPCArgoWorkflowJob{}
		j.Status.Site = &hpcv1alpha1.SiteRef{Name: "meluxina", Namespace: "ns"}
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: hpcv1alpha1.TierGPU, SettledNodeMinutes: usedSince,
			EndTime: metav1.NewTime(shareNow.Add(-10 * time.Minute)), Outcome: "Completed"}}
		jobs = append(jobs, j)
	}
	return s, ledger.Compute(s, jobs, nil, shareNow)
}

// AC-23: the site's monthly limit is its own reason, the project's share another.
func TestDecide_site_limit_is_its_own_reason(t *testing.T) {
	cases := []struct {
		name   string
		build  func() (*hpcv1alpha1.HPCSite, ledger.Ledger)
		wall   int32
		reason string
		inMsg  string
	}{
		{name: "should_refuse_site_limit_when_the_site_remainder_binds_below_the_share",
			build: func() (*hpcv1alpha1.HPCSite, ledger.Ledger) { return shareProject(3, "", 0, 0) },
			wall:  240, reason: hpcv1alpha1.ReasonSiteLimit, inMsg: "the site reports the account has"},
		{name: "should_refuse_budget_blocked_when_the_share_binds_below_the_site_remainder",
			build: func() (*hpcv1alpha1.HPCSite, ledger.Ledger) { return shareProject(50, "", 0, 500) },
			wall:  240, reason: hpcv1alpha1.ReasonBudgetBlocked, inMsg: "the project's share has"},
		{name: "should_refuse_site_limit_for_a_project_without_a_share_when_the_site_is_spent",
			build: func() (*hpcv1alpha1.HPCSite, ledger.Ledger) { return siteSourceProject(2, 0) },
			wall:  240, reason: hpcv1alpha1.ReasonSiteLimit, inMsg: "the site reports the account has"},
		{name: "should_count_what_ended_since_the_site_was_read_against_a_site_budget",
			build: func() (*hpcv1alpha1.HPCSite, ledger.Ledger) { return siteSourceProject(5, 100) },
			wall:  240, reason: hpcv1alpha1.ReasonSiteLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, l := c.build()
			d := Decide(job("meluxina-gpu", c.wall, 1), s, l)
			if d.Phase != hpcv1alpha1.PhaseRefused || d.Reason != c.reason || !strings.Contains(d.Message, c.inMsg) {
				t.Fatalf("got %s/%s %q, want Refused/%s containing %q", d.Phase, d.Reason, d.Message, c.reason, c.inMsg)
			}
		})
	}
}

// A job that fits the site's remainder but not what is free while others hold bookings waits; it is
// not a SiteLimit (the limit is not reached, bookings in flight hold it, D-24).
func TestDecide_site_limit_wait_for_bookings_is_not_site_limit(t *testing.T) {
	s, l := siteSourceProject(10, 0)
	l.Tiers[hpcv1alpha1.TierGPU] = withBooked(l.Tiers[hpcv1alpha1.TierGPU], 480)
	d := Decide(job("meluxina-gpu", 240, 1), s, l)
	if d.Phase != hpcv1alpha1.PhaseWaiting || d.Reason != hpcv1alpha1.ReasonWaitingForBudget {
		t.Fatalf("got %s/%s, want Waiting/WaitingForBudget", d.Phase, d.Reason)
	}
}

func withBooked(t ledger.Tier, booked int64) ledger.Tier { t.Booked = booked; return t }

// Under warn a site-limited project is still admitted beyond the limit and flagged.
func TestDecide_site_limit_under_warn_admits_over_the_limit(t *testing.T) {
	s, l := shareProject(3, hpcv1alpha1.AtLimitWarn, 0, 0)
	d := Decide(job("meluxina-gpu", 240, 1), s, l)
	if d.Phase != hpcv1alpha1.PhaseAdmitted || !d.OverShare {
		t.Fatalf("got %s/%s over=%v, want Admitted over=true", d.Phase, d.Reason, d.OverShare)
	}
}
