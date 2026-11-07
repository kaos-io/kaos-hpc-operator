package ledger

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// shareSite is a project with a 10 GPU-hour share per month, bounded by what the site reported
// left (siteHours) at observed.
func shareSite(siteHours int64, observed *time.Time) *hpcv1alpha1.HPCSite {
	s := theSite()
	s.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceShare, GPUNodeHours: 10, CPUNodeHours: 0}
	if observed != nil {
		at := metav1.NewTime(*observed)
		s.Spec.Budget.SiteLeft = &hpcv1alpha1.SiteLeft{GPUNodeHours: siteHours, ObservedAt: &at}
	}
	return s
}

// PRD-HPC-1461 D-10, AC-21: the share counts from the first of the month, from the project's own
// settled attempts, and a share with no site figure is the share alone.
func TestShareBudget_should_count_the_month_from_its_own_attempts(t *testing.T) {
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("c", hpcv1alpha1.PhaseSucceeded, 240, settled(90, now.Add(-2*time.Hour))),
		booked("last-month", hpcv1alpha1.PhaseSucceeded, 240, settled(500, time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC))),
		booked("r", hpcv1alpha1.PhaseRunning, 60, nil),
	}
	l := Compute(shareSite(0, nil), jobs, nil, now)
	g := l.Tiers[hpcv1alpha1.TierGPU]
	if g.SiteBound || g.Share != 600 || g.Used != 90 || g.Booked != 60 || g.Left() != 450 {
		t.Fatalf("got %+v left %d", g, g.Left())
	}
	if want := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC); !l.PeriodStart.Equal(want) {
		t.Fatalf("period starts %v, want %v", l.PeriodStart, want)
	}
}

func TestShareBudget_should_be_bounded_by_the_site_remainder_less_what_ended_since(t *testing.T) {
	obs := now.Add(-30 * time.Minute)
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("seen", hpcv1alpha1.PhaseSucceeded, 0, settled(100, now.Add(-time.Hour))),       // in the site's figure
		booked("unseen", hpcv1alpha1.PhaseSucceeded, 0, settled(50, now.Add(-10*time.Minute))), // ended after
	}
	// Site reports 3 h = 180 min left; share left is 600-150 = 450; the site binds at 180-50 = 130.
	g := Compute(shareSite(3, &obs), jobs, nil, now).Tiers[hpcv1alpha1.TierGPU]
	if !g.SiteBound || g.Used != 150 || g.SiteUsed != 50 || g.Ceiling() != 130 || !g.SiteBinds() || g.Left() != 130 {
		t.Fatalf("got %+v ceiling %d", g, g.Ceiling())
	}
	// Site reports 100 h: the share (450 left) binds.
	g = Compute(shareSite(100, &obs), jobs, nil, now).Tiers[hpcv1alpha1.TierGPU]
	if g.SiteBinds() || g.Ceiling() != 450 || g.Left() != 450 {
		t.Fatalf("share must bind: %+v ceiling %d", g, g.Ceiling())
	}
}

// A site figure observed last month says nothing about this one: the share alone governs, and the
// month's used restarts at zero (period rollover).
func TestShareBudget_should_ignore_a_site_figure_from_the_previous_period_and_roll_over(t *testing.T) {
	obs := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("oct", hpcv1alpha1.PhaseSucceeded, 0, settled(580, time.Date(2026, 10, 31, 20, 0, 0, 0, time.UTC))),
	}
	novFirst := time.Date(2026, 11, 1, 0, 0, 1, 0, time.UTC)
	g := Compute(shareSite(1, &obs), jobs, nil, novFirst).Tiers[hpcv1alpha1.TierGPU]
	if g.SiteBound || g.Used != 0 || g.Left() != 600 {
		t.Fatalf("a new month starts from the full share: %+v left %d", g, g.Left())
	}
	// The same job still counts on the last day of October.
	oct := time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC)
	if g := Compute(shareSite(0, nil), jobs, nil, oct).Tiers[hpcv1alpha1.TierGPU]; g.Used != 580 || g.Left() != 20 {
		t.Fatalf("in October the job counts: %+v", g)
	}
}

func TestShareBudget_should_leave_nothing_for_a_tier_with_no_share(t *testing.T) {
	if g := Compute(shareSite(0, nil), nil, nil, now).Tiers[hpcv1alpha1.TierCPU]; g.Left() != 0 {
		t.Fatalf("a zero share spends nothing, got %d", g.Left())
	}
}

func TestShareBudget_source_site_is_unchanged_by_a_stray_siteLeft(t *testing.T) {
	obs := now.Add(-time.Minute)
	s := shareSite(1, &obs)
	s.Spec.Budget.Source = hpcv1alpha1.BudgetSourceSite
	at := metav1.NewTime(obs)
	s.Spec.Budget.ObservedAt = &at
	if g := Compute(s, nil, nil, now).Tiers[hpcv1alpha1.TierGPU]; g.SiteBound || g.Left() != 600 {
		t.Fatalf("source site ignores siteLeft: %+v", g)
	}
}
