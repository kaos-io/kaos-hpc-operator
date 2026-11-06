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
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Booked != 0 || l.Active != 0 {
		t.Fatalf("the host's booking covers the node: got booked %d active %d", g.Booked, l.Active)
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
	if !l.PeriodStart.Equal(observed.Time) {
		t.Fatalf("counted from %v, want the observation %v", l.PeriodStart, observed.Time)
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
