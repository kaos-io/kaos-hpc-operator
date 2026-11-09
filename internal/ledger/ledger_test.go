package ledger

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

var now = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func theSite() *hpcv1alpha1.HPCSite {
	s := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Budget: hpcv1alpha1.Budget{GPUNodeHours: 10}}}
	s.Name, s.Namespace = "meluxina", "proj-a-hpc"
	return s
}

func booked(uid string, phase hpcv1alpha1.Phase, minutes int64, mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) hpcv1alpha1.HPCArgoWorkflowJob {
	j := hpcv1alpha1.HPCArgoWorkflowJob{}
	j.UID = types.UID(uid)
	j.Status.Phase = phase
	j.Status.Site = &hpcv1alpha1.SiteRef{Name: "meluxina", Namespace: "proj-a-hpc"}
	j.Status.Booking = &hpcv1alpha1.Booking{SiteName: "meluxina", SiteNamespace: "proj-a-hpc",
		Tier: hpcv1alpha1.TierGPU, NodeMinutes: minutes, At: metav1.NewTime(now.Add(-time.Hour))}
	if mod != nil {
		mod(&j)
	}
	return j
}

// settled ends the job: one attempt of m node-minutes that ended at end; its booking is gone.
func settled(m int64, end time.Time) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
	return func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.SettledNodeMinutes = &m
		j.Status.Booking = nil
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: hpcv1alpha1.TierGPU,
			SettledNodeMinutes: m, EndTime: metav1.NewTime(end), Outcome: "Completed"}}
	}
}

func TestPeriodStartIsTheCalendarMonthInUTC(t *testing.T) {
	athens := time.FixedZone("Athens", 2*3600)
	got := PeriodStart(time.Date(2026, 12, 1, 1, 0, 0, 0, athens)) // still 30 Nov in UTC
	if want := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestComputeBookedUsedLeftAndActive(t *testing.T) {
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("a", hpcv1alpha1.PhaseAdmitted, 120, nil),
		booked("b", hpcv1alpha1.PhaseRunning, 60, nil),
		booked("c", hpcv1alpha1.PhaseSucceeded, 240, settled(90, now.Add(-2*time.Hour))),
		booked("d", hpcv1alpha1.PhaseSucceeded, 240, settled(500, time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC))), // last month
	}
	other := booked("e", hpcv1alpha1.PhaseRunning, 999, nil)
	other.Status.Booking.SiteNamespace = "proj-b-hpc" // another project's site
	other.Status.Site.Namespace = "proj-b-hpc"
	jobs = append(jobs, other)

	l := Compute(theSite(), jobs, nil, now)
	g := l.Tiers[hpcv1alpha1.TierGPU]
	if g.Booked != 180 || g.Used != 90 || g.Share != 600 || g.Left() != 330 || l.Active != 2 {
		t.Fatalf("got booked %d used %d share %d left %d active %d", g.Booked, g.Used, g.Share, g.Left(), l.Active)
	}
}

func TestPendingBookingsCountUntilTheCacheShowsThem(t *testing.T) {
	pending := map[string]hpcv1alpha1.Booking{
		"x": {SiteName: "meluxina", SiteNamespace: "proj-a-hpc", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 100},
		"a": {SiteName: "meluxina", SiteNamespace: "proj-a-hpc", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 120},
	}
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{booked("a", hpcv1alpha1.PhaseAdmitted, 120, nil)}
	l := Compute(theSite(), jobs, pending, now)
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Booked != 220 || l.Active != 2 {
		t.Fatalf("a counted once and x pending: got booked %d active %d", g.Booked, l.Active)
	}
}

func TestAGuestOnASharedNodeHoldsNothing(t *testing.T) {
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{booked("g", hpcv1alpha1.PhaseRunning, 240, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.Run = &hpcv1alpha1.RunStatus{HostSiteJobID: "5425070"}
	})}
	l := Compute(theSite(), jobs, nil, now)
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Booked != 0 || l.Active != 1 || l.Running != 1 || l.Queued != 0 {
		t.Fatalf("the host's booking covers the node, the guest is a running job: got booked %d active %d running %d queued %d",
			g.Booked, l.Active, l.Running, l.Queued)
	}
}

func TestLeftNeverBelowZero(t *testing.T) {
	if left := (Tier{Share: 60, Used: 50, Booked: 30}).Left(); left != 0 {
		t.Fatalf("got %d", left)
	}
}

func TestAContinuedJobCountsEveryAttemptInItsOwnPeriodAndItsNextBooking(t *testing.T) {
	j := booked("c", hpcv1alpha1.PhaseAdmitted, 60, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.Attempt = 3
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{
			{Attempt: 1, Tier: hpcv1alpha1.TierGPU, SettledNodeMinutes: 50, EndTime: metav1.NewTime(time.Date(2026, 10, 31, 22, 0, 0, 0, time.UTC))},
			{Attempt: 2, Tier: hpcv1alpha1.TierGPU, SettledNodeMinutes: 55, EndTime: metav1.NewTime(now.Add(-time.Hour))},
		}
	})
	l := Compute(theSite(), []hpcv1alpha1.HPCArgoWorkflowJob{j}, nil, now)
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Used != 55 || g.Booked != 60 || l.Active != 1 {
		t.Fatalf("attempt 1 is last month's, attempt 2 this month's, attempt 3 is booked: got used %d booked %d active %d",
			g.Used, g.Booked, l.Active)
	}
}

func TestBetweenAttemptsTheJobStillBelongsToItsSite(t *testing.T) {
	j := booked("c", hpcv1alpha1.PhasePending, 0, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.Booking = nil // continuing: the next attempt is not booked yet
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: hpcv1alpha1.TierGPU,
			SettledNodeMinutes: 40, EndTime: metav1.NewTime(now.Add(-time.Minute))}}
	})
	l := Compute(theSite(), []hpcv1alpha1.HPCArgoWorkflowJob{j}, nil, now)
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Used != 40 || g.Booked != 0 {
		t.Fatalf("got used %d booked %d", g.Used, g.Booked)
	}
}

func TestASiteBudgetCountsOnlyWhatTheSiteHasNotSeenYet(t *testing.T) {
	site := theSite()
	observed := metav1.NewTime(now.Add(-30 * time.Minute))
	site.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, ObservedAt: &observed,
		GPUNodeHours: 10} // left on the account when the site was asked
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("seen", hpcv1alpha1.PhaseSucceeded, 0, settled(100, now.Add(-time.Hour))),       // in the site's figure
		booked("unseen", hpcv1alpha1.PhaseSucceeded, 0, settled(50, now.Add(-10*time.Minute))), // ended after
		booked("running", hpcv1alpha1.PhaseRunning, 60, nil),
	}
	l := Compute(site, jobs, nil, now)
	g := l.Tiers[hpcv1alpha1.TierGPU]
	if g.Share != 600 || g.Used != 50 || g.Booked != 60 || g.Left() != 490 {
		t.Fatalf("got share %d used %d booked %d left %d", g.Share, g.Used, g.Booked, g.Left())
	}
	if !l.CountedFrom.Equal(observed.Time) {
		t.Fatalf("counted from %v, want the observation %v", l.CountedFrom, observed.Time)
	}
	if want := PeriodStart(now); !l.PeriodStart.Equal(want) {
		t.Fatalf("period starts %v, want the calendar month %v even for a site budget", l.PeriodStart, want)
	}
}

func TestASpentSiteAllocationLeavesNothing(t *testing.T) {
	site := theSite()
	observed := metav1.NewTime(now.Add(-time.Minute))
	site.Spec.Budget = hpcv1alpha1.Budget{Source: hpcv1alpha1.BudgetSourceSite, ObservedAt: &observed}
	if g := Compute(site, nil, nil, now).Tiers[hpcv1alpha1.TierCPU]; g.Left() != 0 {
		t.Fatalf("a spent CPU allocation must leave nothing, got %d", g.Left())
	}
}

func TestComputeClampsGarbageAttempts(t *testing.T) {
	end := now.Add(-time.Hour)
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("g1", hpcv1alpha1.PhaseSucceeded, 0, settled(-41683737, end)),
		booked("g2", hpcv1alpha1.PhaseSucceeded, 0, settled(-41683737, end)),
		booked("ok", hpcv1alpha1.PhaseSucceeded, 0, settled(30, end)),
	}
	l := Compute(theSite(), jobs, nil, now)
	if got := l.Tiers[hpcv1alpha1.TierGPU]; got.Used != 30 || got.Left() != 570 {
		t.Fatalf("used=%d left=%d; want 30 and 570 (garbage attempts count 0)", got.Used, got.Left())
	}
	only := Compute(theSite(), jobs[:2], nil, now)
	if only.Tiers[hpcv1alpha1.TierGPU].Used != 0 {
		t.Fatalf("used went negative or nonzero: %d", only.Tiers[hpcv1alpha1.TierGPU].Used)
	}
}

func withState(state string) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
	return func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.Attempt = 1
		j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "1", State: state}
	}
}

func TestSubmittedJobsAreQueuedUntilTheSiteSaysRunning(t *testing.T) {
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("admitted", hpcv1alpha1.PhaseAdmitted, 60, nil),
		booked("submitted-pending", hpcv1alpha1.PhaseRunning, 60, withState("PENDING")),
		booked("submitted-no-state", hpcv1alpha1.PhaseRunning, 60, withState("")),
		booked("running", hpcv1alpha1.PhaseRunning, 60, withState("RUNNING")),
		booked("stale-report", hpcv1alpha1.PhaseRunning, 60, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Status.Attempt = 2
			j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "RUNNING"} // earlier attempt's report
		}),
		booked("guest", hpcv1alpha1.PhaseRunning, 60, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Status.Run = &hpcv1alpha1.RunStatus{HostSiteJobID: "9"}
		}),
		booked("done", hpcv1alpha1.PhaseSucceeded, 60, withState("COMPLETED")),
	}
	pending := map[string]hpcv1alpha1.Booking{
		"x": {SiteName: "meluxina", SiteNamespace: "proj-a-hpc", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 10},
	}
	l := Compute(theSite(), jobs, pending, now)
	if l.Queued != 5 || l.Running != 2 || l.Active != 7 {
		t.Fatalf("got queued %d running %d active %d, want 5, 2, 7", l.Queued, l.Running, l.Active)
	}
}

func TestNoJobsMeansNoCounts(t *testing.T) {
	l := Compute(theSite(), nil, nil, now)
	if l.Queued != 0 || l.Running != 0 || l.Active != 0 {
		t.Fatalf("got %+v", l)
	}
}

// A controller restart keeps no ledger state: the period and the spend are recomputed from the
// jobs, so a job that ended before the restart, earlier in the month, still counts.
func TestPoolBudgetCountsEveryAttemptSettledInTheMonthWhateverTheRestart(t *testing.T) {
	site := theSite()
	monthStart := PeriodStart(now)
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("first-of-month", hpcv1alpha1.PhaseSucceeded, 0, settled(30, monthStart.Add(time.Minute))),
		booked("before-restart", hpcv1alpha1.PhaseFailed, 0, settled(12, now.Add(-2*time.Hour))),
		booked("last-month", hpcv1alpha1.PhaseSucceeded, 0, settled(99, monthStart.Add(-time.Minute))),
	}
	// Computed at two different instants (a restart in between): same period, same spend.
	for _, at := range []time.Time{now.Add(-time.Hour), now} {
		l := Compute(site, jobs, nil, at)
		if !l.PeriodStart.Equal(monthStart) || !l.CountedFrom.Equal(monthStart) {
			t.Fatalf("at %v: period %v counted from %v, want %v", at, l.PeriodStart, l.CountedFrom, monthStart)
		}
		if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Used != 42 {
			t.Fatalf("at %v: used %d, want 42 (last month's 99 excluded)", at, g.Used)
		}
	}
}

func TestComputeCountsSiteJobsWithoutGuests(t *testing.T) {
	site := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Site: "m"}}
	site.Name, site.Namespace = "s", "ns"
	mk := func(uid string, host string) hpcv1alpha1.HPCArgoWorkflowJob {
		j := hpcv1alpha1.HPCArgoWorkflowJob{}
		j.UID = types.UID(uid)
		j.Status.Phase = hpcv1alpha1.PhaseRunning
		j.Status.Booking = &hpcv1alpha1.Booking{SiteName: "s", SiteNamespace: "ns", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 60}
		j.Status.Run = &hpcv1alpha1.RunStatus{HostSiteJobID: host}
		return j
	}
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{mk("a", ""), mk("b", ""), mk("g", "99")}
	l := Compute(site, jobs, map[string]hpcv1alpha1.Booking{
		"p": {SiteName: "s", SiteNamespace: "ns", Tier: hpcv1alpha1.TierGPU, NodeMinutes: 10}}, time.Now())
	if l.Active != 4 || l.SiteJobs != 3 {
		t.Fatalf("active %d site jobs %d, want 4 and 3 (the guest holds no Slurm job)", l.Active, l.SiteJobs)
	}
}

// AC-84: settlement is per allocation: an attempt's minutes count only in the tier it booked, and a
// site-reported remainder bounds only its own allocation.
func TestComputeSettlesAndBoundsEachAllocationApart(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	obs := metav1.NewTime(now.Add(-time.Hour))
	site := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Site: "m", Budget: hpcv1alpha1.Budget{
		GPUNodeHours: 10, CPUNodeHours: 10, FPGANodeHours: ptr(10), LargeMemNodeHours: ptr(10),
		SiteLeft: &hpcv1alpha1.SiteLeft{GPUNodeHours: 10, CPUNodeHours: 0, FPGANodeHours: ptr(2), LargeMemNodeHours: nil, ObservedAt: &obs}}}}
	site.Name, site.Namespace = "s", "ns"
	jobs := make([]hpcv1alpha1.HPCArgoWorkflowJob, 0, len(hpcv1alpha1.Tiers))
	for i, tier := range hpcv1alpha1.Tiers {
		j := hpcv1alpha1.HPCArgoWorkflowJob{}
		j.UID = types.UID(string(rune('a' + i)))
		j.Status.Booking = nil
		j.Status.Site = &hpcv1alpha1.SiteRef{Name: "s", Namespace: "ns"}
		j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: tier, SettledNodeMinutes: int64(10 * (i + 1)),
			EndTime: metav1.NewTime(now.Add(-10 * time.Minute))}}
		jobs = append(jobs, j)
	}
	l := Compute(site, jobs, nil, now)
	want := map[hpcv1alpha1.Tier]int64{hpcv1alpha1.TierGPU: 10, hpcv1alpha1.TierCPU: 20, hpcv1alpha1.TierFPGA: 30, hpcv1alpha1.TierLargeMem: 40}
	for tier, used := range want {
		if l.Tiers[tier].Used != used {
			t.Fatalf("%s used %d, want %d", tier, l.Tiers[tier].Used, used)
		}
	}
	// CPU: the site says 0 left: nothing may be spent. FPGA: 2 h = 120 less 30 ended since = 90.
	if c := l.Tiers[hpcv1alpha1.TierCPU].Ceiling(); c != -20 {
		t.Fatalf("cpu ceiling %d, want -20 (the site's 0 less 20 used)", c)
	}
	if c := l.Tiers[hpcv1alpha1.TierFPGA].Ceiling(); c != 90 {
		t.Fatalf("fpga ceiling %d, want 90", c)
	}
	if c := l.Tiers[hpcv1alpha1.TierGPU].Ceiling(); c != 590 {
		t.Fatalf("gpu ceiling %d, want min(600-10, 600-10)", c)
	}
}

func ptr(v int64) *int64 { return &v }

// An allocation siteLeft omits has no site bound: the share alone applies (a writer that predates it).
func TestComputeOmittedSiteLeftAllocationIsNotBoundToZero(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	obs := metav1.NewTime(now.Add(-time.Hour))
	site := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Budget: hpcv1alpha1.Budget{
		GPUNodeHours: 10, FPGANodeHours: ptr(10), LargeMemNodeHours: ptr(10),
		SiteLeft: &hpcv1alpha1.SiteLeft{GPUNodeHours: 5, ObservedAt: &obs}}}}
	l := Compute(site, nil, nil, now)
	if tr := l.Tiers[hpcv1alpha1.TierFPGA]; tr.SiteBound || tr.Ceiling() != 600 {
		t.Fatalf("fpga bound %v ceiling %d, want unbound 600", tr.SiteBound, tr.Ceiling())
	}
	if tr := l.Tiers[hpcv1alpha1.TierGPU]; !tr.SiteBound || tr.Ceiling() != 300 {
		t.Fatalf("gpu still bound by the site: %v %d", tr.SiteBound, tr.Ceiling())
	}
	site.Spec.Budget.SiteLeft.LargeMemNodeHours = ptr(0)
	l = Compute(site, nil, nil, now)
	if tr := l.Tiers[hpcv1alpha1.TierLargeMem]; !tr.SiteBound || tr.Ceiling() != 0 {
		t.Fatalf("explicit 0 is spent: %v %d", tr.SiteBound, tr.Ceiling())
	}
}

// Source site: an omitted fpga/largemem figure is "no figure": unbounded, still settled; an explicit value bounds it.
func TestComputeSourceSiteOmittedAllocationIsUnboundedButStillSettled(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	obs := metav1.NewTime(now.Add(-time.Hour))
	site := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Budget: hpcv1alpha1.Budget{
		Source: hpcv1alpha1.BudgetSourceSite, ObservedAt: &obs, GPUNodeHours: 10, CPUNodeHours: 0, LargeMemNodeHours: ptr(0)}}}
	site.Name, site.Namespace = "s", "ns"
	j := hpcv1alpha1.HPCArgoWorkflowJob{}
	j.Status.Site = &hpcv1alpha1.SiteRef{Name: "s", Namespace: "ns"}
	j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: hpcv1alpha1.TierFPGA, SettledNodeMinutes: 25,
		EndTime: metav1.NewTime(now.Add(-10 * time.Minute))}}
	l := Compute(site, []hpcv1alpha1.HPCArgoWorkflowJob{j}, nil, now)
	f := l.Tiers[hpcv1alpha1.TierFPGA]
	if !f.Unbounded || f.Used != 25 || f.Left() <= 0 || f.Percent() != -1 {
		t.Fatalf("fpga omitted: %+v", f)
	}
	if m := l.Tiers[hpcv1alpha1.TierLargeMem]; m.Unbounded || m.Left() != 0 {
		t.Fatalf("largemem explicit 0 is bounded and spent: %+v", m)
	}
	if c := l.Tiers[hpcv1alpha1.TierCPU]; c.Unbounded || c.Left() != 0 {
		t.Fatalf("cpu omitted stays 0 as before: %+v", c)
	}
}
