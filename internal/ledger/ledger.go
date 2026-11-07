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
	// Active is the number of jobs holding a booking or running as a guest: Queued + Running.
	Active int32
	// Queued is the number of active jobs the site has not started: admitted, submitted or pending.
	Queued int32
	// Running is the number of active jobs the site reports RUNNING, plus guests on a running host.
	Running int32
}

// SiteStateRunning is the scheduler state of a site job that has started.
const SiteStateRunning = "RUNNING"

// count adds one active job to the ledger, as running or queued.
func (l *Ledger) count(running bool) {
	l.Active++
	if running {
		l.Running++
	} else {
		l.Queued++
	}
}

// isRunning reports whether the job's site job has started. Phase Running only means the job was
// submitted, so the site's own state decides; a guest runs on its host's node by definition.
func isRunning(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	r := j.Status.Run
	if r == nil {
		return false
	}
	if r.HostSiteJobID != "" {
		return true
	}
	// A report without an attempt number counts only for attempt 1 (as the controller's current()).
	current := r.Attempt == j.Status.Attempt || (r.Attempt == 0 && j.Status.Attempt <= 1)
	return current && r.State == SiteStateRunning
}

// holdsOrGuest reports whether the job is active: it holds a booking, or is a guest on a host.
func holdsOrGuest(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	if j.Status.Booking == nil || j.Status.Phase.Terminal() {
		return false
	}
	return j.Status.Phase == hpcv1alpha1.PhaseAdmitted || j.Status.Phase == hpcv1alpha1.PhaseRunning
}

// PeriodStart is the start of the budget period containing now: the calendar month in UTC (D-11).
func PeriodStart(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// Holds reports whether a job currently holds its booking.
func Holds(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	if j.Status.Booking == nil || j.Status.Phase.Terminal() {
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
func Compute(site *hpcv1alpha1.HPCSite, jobs []hpcv1alpha1.HPCArgoWorkflowJob,
	pending map[string]hpcv1alpha1.Booking, now time.Time) Ledger {
	start := PeriodStart(now)
	// Source site (D-56): the share is what the site reported left at ObservedAt, which already
	// counts every job that ended before it, so only later ends are used against it. An attempt
	// that was running at ObservedAt is counted whole: conservative, never an overspend.
	if b := site.Spec.Budget; b.Source == hpcv1alpha1.BudgetSourceSite && b.ObservedAt != nil {
		start = b.ObservedAt.UTC()
	}
	l := Ledger{PeriodStart: start, Tiers: map[hpcv1alpha1.Tier]Tier{
		hpcv1alpha1.TierGPU: {Share: site.Spec.Budget.GPUNodeHours * 60},
		hpcv1alpha1.TierCPU: {Share: site.Spec.Budget.CPUNodeHours * 60},
	}}
	seen := map[string]bool{}
	for i := range jobs {
		j := &jobs[i]
		if !onSite(j, site) {
			continue
		}
		// Every finished attempt counts in the period it ended (a continued job spends per attempt).
		for _, a := range j.Status.Attempts {
			if a.EndTime.Time.Before(start) {
				continue
			}
			t := l.Tiers[a.Tier]
			// Records stored before settlement was validated can carry garbage (negative) minutes.
			t.Used += max(a.SettledNodeMinutes, 0)
			l.Tiers[a.Tier] = t
		}
		if b := j.Status.Booking; b != nil && b.SiteName == site.Name && b.SiteNamespace == site.Namespace {
			seen[string(j.UID)] = true
			if Holds(j) {
				t := l.Tiers[b.Tier]
				t.Booked += b.NodeMinutes
				l.Tiers[b.Tier] = t
			}
			// A guest holds no booking of its own (its host's covers the node) but is still active.
			if holdsOrGuest(j) {
				l.count(isRunning(j))
			}
		}
	}
	for uid, b := range pending {
		if seen[uid] || b.SiteName != site.Name || b.SiteNamespace != site.Namespace {
			continue
		}
		t := l.Tiers[b.Tier]
		t.Booked += b.NodeMinutes
		l.Tiers[b.Tier] = t
		l.count(false) // booked in this process, not yet submitted
	}
	return l
}

// onSite reports whether the job's spend belongs to this HPCSite: its booking names it, or (between
// a continuation and its next booking) its last booking did.
func onSite(j *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) bool {
	if b := j.Status.Booking; b != nil {
		return b.SiteName == site.Name && b.SiteNamespace == site.Namespace
	}
	return j.Status.Site != nil && j.Status.Site.Name == site.Name && j.Status.Site.Namespace == site.Namespace
}
