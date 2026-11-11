package ledger

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func hours(v int64) *int64 { return &v }

// siteWithGrant is a source-site budget observed 30 minutes ago: left hours remain, grant hours were granted.
func siteWithGrant(left int64, grant *hpcv1alpha1.GrantNodeHours, observed time.Time) *hpcv1alpha1.HPCSite {
	site := theSite()
	at := metav1.NewTime(observed)
	site.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, ObservedAt: &at,
		GPUNodeHours: left, GrantNodeHours: grant}
	return site
}

// PRD-HPC-1461 AC-22: with a grant, the warning percentage is the share of the grant spent or committed.
func TestPercentWithAGrant(t *testing.T) {
	obs := now.Add(-30 * time.Minute)
	cases := []struct {
		name       string
		left       int64 // node-hours left at the observation
		grant      *hpcv1alpha1.GrantNodeHours
		usedSince  int64 // node-minutes that ended after the observation
		booked     int64 // node-minutes booked now
		wantPct    int64
		wantGrant  int64 // node-minutes
		wantLeft   int64 // admission is unchanged: the remainder less used and booked
		observedAt time.Time
	}{
		{name: "should_read_about_2_percent_for_the_live_lab_numbers",
			left: 847, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(868)}, wantPct: 2, wantGrant: 52080, wantLeft: 50820},
		{name: "should_add_what_ended_since_the_observation_and_what_is_booked",
			left: 847, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(868)}, usedSince: 100, booked: 200,
			wantPct: (1260 + 300) * 100 / 52080, wantGrant: 52080, wantLeft: 50820 - 300},
		{name: "should_cross_80_percent_when_the_month_is_mostly_spent",
			left: 150, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(868)}, booked: 600, wantPct: ((868-150)*60 + 600) * 100 / 52080,
			wantGrant: 52080, wantLeft: 150*60 - 600},
		{name: "should_be_100_when_the_allocation_is_spent",
			left: 0, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(868)}, wantPct: 100, wantGrant: 52080, wantLeft: 0},
		{name: "should_keep_the_old_base_without_a_grant",
			left: 10, grant: nil, usedSince: 60, wantPct: 10, wantLeft: 540},
		{name: "should_keep_the_old_base_when_the_grant_is_zero",
			left: 10, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(0)}, usedSince: 60, wantPct: 10, wantLeft: 540},
		{name: "should_keep_the_old_base_when_the_grant_is_below_what_is_left",
			left: 10, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(5)}, usedSince: 60, wantPct: 10, wantLeft: 540},
		{name: "should_keep_the_old_base_when_the_observation_is_from_last_month",
			left: 10, grant: &hpcv1alpha1.GrantNodeHours{GPU: hours(100)}, usedSince: 60, wantPct: 10, wantLeft: 540,
			observedAt: PeriodStart(now).Add(-time.Hour)},
		{name: "should_keep_the_old_base_for_a_tier_the_grant_does_not_name",
			left: 10, grant: &hpcv1alpha1.GrantNodeHours{CPU: hours(100)}, usedSince: 60, wantPct: 10, wantLeft: 540},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			at := obs
			if !tc.observedAt.IsZero() {
				at = tc.observedAt
			}
			site := siteWithGrant(tc.left, tc.grant, at)
			var jobs []hpcv1alpha1.HPCArgoWorkflowJob
			if tc.usedSince > 0 {
				jobs = append(jobs, booked("since", hpcv1alpha1.PhaseSucceeded, 0, settled(tc.usedSince, now.Add(-10*time.Minute))))
			}
			if tc.booked > 0 {
				jobs = append(jobs, booked("held", hpcv1alpha1.PhaseRunning, tc.booked, nil))
			}
			g := Compute(site, jobs, nil, now).Tiers[hpcv1alpha1.TierGPU]
			if g.Percent() != tc.wantPct || g.Grant != tc.wantGrant || g.Left() != tc.wantLeft {
				t.Fatalf("percent %d grant %d left %d; want %d, %d, %d", g.Percent(), g.Grant, g.Left(), tc.wantPct, tc.wantGrant, tc.wantLeft)
			}
		})
	}
}

// The grant changes the warning only: the share path and the ceiling are not touched.
func TestAGrantIsIgnoredForASharedBudget(t *testing.T) {
	site := theSite()
	site.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceShare, GPUNodeHours: 10,
		GrantNodeHours: &hpcv1alpha1.GrantNodeHours{GPU: hours(500)}}
	l := Compute(site, []hpcv1alpha1.HPCArgoWorkflowJob{booked("b", hpcv1alpha1.PhaseRunning, 300, nil)}, nil, now)
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Grant != 0 || g.Percent() != 50 {
		t.Fatalf("grant %d percent %d, want 0 and 50", g.Grant, g.Percent())
	}
}

func TestAnUnboundedAllocationHasNoPercentEvenWithAGrant(t *testing.T) {
	obs := now.Add(-time.Hour)
	site := siteWithGrant(10, &hpcv1alpha1.GrantNodeHours{FPGA: hours(100)}, obs)
	if f := Compute(site, nil, nil, now).Tiers[hpcv1alpha1.TierFPGA]; f.Percent() != -1 || f.Grant != 0 {
		t.Fatalf("fpga percent %d grant %d, want -1 and 0", f.Percent(), f.Grant)
	}
}
