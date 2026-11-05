// Package ledger derives a project's spend on one site from its jobs (PRD-HPC-1461 D-12).
//
// The ledger is never a stored counter: it is recomputed from the jobs' own status every time,
// so it cannot drift and needs no repair after a restart.
package ledger

import (
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Tier is one tier's spend in node-minutes for the current period.
type Tier struct {
	Used   int64
	Booked int64
	Share  int64
}

// Left is the share minus used and booked, never below zero.
func (t Tier) Left() int64 {
	if l := t.Share - t.Used - t.Booked; l > 0 {
		return l
	}
	return 0
}

// Ledger is one project's spend on one site.
type Ledger struct {
	PeriodStart time.Time
	Tiers       map[hpcv1alpha1.Tier]Tier
	// Active is the number of jobs holding a booking (admitted or running, not ended).
	Active int32
}

// PeriodStart is the start of the budget period containing now: the calendar month in UTC (D-11).
func PeriodStart(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// Holds reports whether a job currently holds its booking.
func Holds(j *hpcv1alpha1.HPCJob) bool {
	if j.Status.Booking == nil || j.Status.SettledNodeMinutes != nil {
		return false
	}
	switch j.Status.Phase {
	case hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.PhaseRunning:
		// A guest on a shared node is covered by its host's booking (D-26).
		return j.Status.Run == nil || j.Status.Run.HostSiteJobID == ""
	}
	return false
}

// Compute derives the ledger of one HPCSite from the jobs booked against it. pending holds
// bookings made in this process that the informer cache may not show yet, keyed by job UID; a job
// present in jobs with a booking takes precedence over its pending entry.
func Compute(site *hpcv1alpha1.HPCSite, jobs []hpcv1alpha1.HPCJob,
	pending map[string]hpcv1alpha1.Booking, now time.Time) Ledger {
	start := PeriodStart(now)
	l := Ledger{PeriodStart: start, Tiers: map[hpcv1alpha1.Tier]Tier{
		hpcv1alpha1.TierGPU: {Share: site.Spec.Budget.GPUNodeHours * 60},
		hpcv1alpha1.TierCPU: {Share: site.Spec.Budget.CPUNodeHours * 60},
	}}
	seen := map[string]bool{}
	for i := range jobs {
		j := &jobs[i]
		b := j.Status.Booking
		if b == nil || b.SiteName != site.Name || b.SiteNamespace != site.Namespace {
			continue
		}
		seen[string(j.UID)] = true
		t := l.Tiers[b.Tier]
		switch {
		case j.Status.SettledNodeMinutes != nil:
			if settledAt(j).Before(start) {
				continue
			}
			t.Used += *j.Status.SettledNodeMinutes
		case Holds(j):
			t.Booked += b.NodeMinutes
			l.Active++
		}
		l.Tiers[b.Tier] = t
	}
	for uid, b := range pending {
		if seen[uid] || b.SiteName != site.Name || b.SiteNamespace != site.Namespace {
			continue
		}
		t := l.Tiers[b.Tier]
		t.Booked += b.NodeMinutes
		l.Tiers[b.Tier] = t
		l.Active++
	}
	return l
}

// settledAt is when a settled job's spend counts: its end time as the site reported it, else the
// time it was booked.
func settledAt(j *hpcv1alpha1.HPCJob) time.Time {
	if j.Status.Run != nil && j.Status.Run.EndTime != nil {
		return j.Status.Run.EndTime.Time
	}
	return j.Status.Booking.At.Time
}
