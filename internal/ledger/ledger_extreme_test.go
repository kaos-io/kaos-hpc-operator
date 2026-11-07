package ledger

import (
	"fmt"
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Extreme cases of the ledger (docs/extreme-cases.md, sections 1, 4 and 7).

func TestShouldDeriveALedgerOfTwentyThousandJobsWithinASecond(t *testing.T) {
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	for i := range 20000 {
		switch i % 3 {
		case 0:
			jobs = append(jobs, booked(fmt.Sprint("a", i), hpcv1alpha1.PhaseRunning, 60, nil))
		case 1:
			jobs = append(jobs, booked(fmt.Sprint("s", i), hpcv1alpha1.PhaseSucceeded, 0, settled(30, now.Add(-time.Hour))))
		default:
			jobs = append(jobs, booked(fmt.Sprint("w", i), hpcv1alpha1.PhaseWaiting, 0, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
				j.Status.Booking = nil
			}))
		}
	}
	pending := map[string]hpcv1alpha1.Booking{}
	for i := range 500 {
		pending[fmt.Sprint("p", i)] = hpcv1alpha1.Booking{SiteName: "meluxina", SiteNamespace: "proj-a-hpc",
			Tier: hpcv1alpha1.TierGPU, NodeMinutes: 10}
	}
	t0 := time.Now()
	l := Compute(theSite(), jobs, pending, now)
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("Compute over 20000 jobs took %v: it must stay linear", d)
	}
	g := l.Tiers[hpcv1alpha1.TierGPU]
	wantBooked := int64(6667*60 + 500*10) // 6667 running jobs (i%3==0 for i<20000) + the pending ones
	if g.Booked != wantBooked || g.Used != 6667*30 || l.Active != 6667+500 {
		t.Fatalf("booked %d (want %d), used %d, active %d", g.Booked, wantBooked, g.Used, l.Active)
	}
}

func TestShouldBookAGuestAgainWhenItsHostIsLostAndKeepItActiveThroughout(t *testing.T) {
	site := theSite()
	hosted := func(host string) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
		return func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Status.Attempt = 1
			j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "9", State: "PENDING", HostSiteJobID: host}
		}
	}
	for name, tc := range map[string]struct {
		mod        func(*hpcv1alpha1.HPCArgoWorkflowJob)
		wantBooked int64
	}{
		"taken_by_a_host_the_host_covers_the_node":                           {hosted("H"), 0},
		"host_lost_and_job_resubmitted_alone_it_holds_its_own_booking_again": {hosted(""), 60},
	} {
		t.Run(name, func(t *testing.T) {
			l := Compute(site, []hpcv1alpha1.HPCArgoWorkflowJob{booked("g", hpcv1alpha1.PhaseRunning, 60, tc.mod)}, nil, now)
			if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Booked != tc.wantBooked || l.Active != 1 {
				t.Fatalf("booked %d (want %d) active %d (want 1)", g.Booked, tc.wantBooked, l.Active)
			}
		})
	}
}

func TestShouldNeverCountAJobTwiceWhenItIsBothPendingAndInTheCache(t *testing.T) {
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{booked("a", hpcv1alpha1.PhaseAdmitted, 100, nil)}
	pending := map[string]hpcv1alpha1.Booking{"a": *jobs[0].Status.Booking}
	for range 3 { // a restart replays the same state any number of times
		l := Compute(theSite(), jobs, pending, now)
		if g := l.Tiers[hpcv1alpha1.TierGPU]; g.Booked != 100 || l.Active != 1 {
			t.Fatalf("double count: booked %d active %d", g.Booked, l.Active)
		}
	}
}

func TestShouldSpendTheLastNodeMinuteExactlyAndNotOne(t *testing.T) {
	site := theSite() // 10 h = 600 node-minutes
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		booked("a", hpcv1alpha1.PhaseRunning, 599, nil),
	}
	if g := Compute(site, jobs, nil, now).Tiers[hpcv1alpha1.TierGPU]; g.Left() != 1 {
		t.Fatalf("left %d, want 1", g.Left())
	}
	jobs = append(jobs, booked("b", hpcv1alpha1.PhaseAdmitted, 1, nil))
	if g := Compute(site, jobs, nil, now).Tiers[hpcv1alpha1.TierGPU]; g.Left() != 0 || g.Booked != 600 {
		t.Fatalf("left %d booked %d, want 0 and 600", g.Left(), g.Booked)
	}
	// An overspend (a warn-policy project, or a late host settlement) never shows as negative room.
	jobs = append(jobs, booked("c", hpcv1alpha1.PhaseAdmitted, 50, nil))
	if g := Compute(site, jobs, nil, now).Tiers[hpcv1alpha1.TierGPU]; g.Left() != 0 {
		t.Fatalf("left %d, want 0", g.Left())
	}
}
