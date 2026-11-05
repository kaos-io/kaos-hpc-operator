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

func booked(uid string, phase hpcv1alpha1.Phase, minutes int64, mod func(*hpcv1alpha1.HPCJob)) hpcv1alpha1.HPCJob {
	j := hpcv1alpha1.HPCJob{}
	j.UID = types.UID(uid)
	j.Status.Phase = phase
	j.Status.Booking = &hpcv1alpha1.Booking{SiteName: "meluxina", SiteNamespace: "proj-a-hpc",
		Tier: hpcv1alpha1.TierGPU, NodeMinutes: minutes, At: metav1.NewTime(now.Add(-time.Hour))}
	if mod != nil {
		mod(&j)
	}
	return j
}

func settled(m int64, end time.Time) func(*hpcv1alpha1.HPCJob) {
	return func(j *hpcv1alpha1.HPCJob) {
		j.Status.SettledNodeMinutes = &m
		e := metav1.NewTime(end)
		j.Status.Run = &hpcv1alpha1.RunStatus{EndTime: &e}
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
	jobs := []hpcv1alpha1.HPCJob{
		booked("a", hpcv1alpha1.PhaseAdmitted, 120, nil),
		booked("b", hpcv1alpha1.PhaseRunning, 60, nil),
		booked("c", hpcv1alpha1.PhaseSucceeded, 240, settled(90, now.Add(-2*time.Hour))),
		booked("d", hpcv1alpha1.PhaseSucceeded, 240, settled(500, time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC))), // last month
	}
	other := booked("e", hpcv1alpha1.PhaseRunning, 999, nil)
	other.Status.Booking.SiteNamespace = "proj-b-hpc" // another project's site
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
	jobs := []hpcv1alpha1.HPCJob{booked("a", hpcv1alpha1.PhaseAdmitted, 120, nil)}
	l := Compute(theSite(), jobs, pending, now)
	if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Booked != 220 || l.Active != 2 {
		t.Fatalf("a counted once and x pending: got booked %d active %d", g.Booked, l.Active)
	}
}

func TestAGuestOnASharedNodeHoldsNothing(t *testing.T) {
	jobs := []hpcv1alpha1.HPCJob{booked("g", hpcv1alpha1.PhaseRunning, 240, func(j *hpcv1alpha1.HPCJob) {
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
