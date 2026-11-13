// Package ledger derives a project's spend on one site from its jobs (PRD-HPC-1461 D-12).
//
// The ledger is never a stored counter: it is recomputed from the jobs' own status every time,
// so it cannot drift and needs no repair after a restart.
package ledger

import (
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Tier is one tier's spend in node-minutes for the current period.
type Tier struct {
	Used   int64
	Booked int64
	Share  int64
	// SiteBound is true when the site's own remainder bounds the share (source share with a usable
	// siteLeft). SiteLeft is what the site reported at observation; SiteUsed is what ended since.
	SiteBound bool
	SiteLeft  int64
	SiteUsed  int64
	// Unbounded is true when the site gave no figure for this allocation (source site, AC-84): nothing
	// bounds it, but its use is still settled and recorded.
	Unbounded bool
	// SiteSource is true when the budget is the site's own remainder (source site): there is no
	// project share, so the site's limit is the only one.
	SiteSource bool
	// Grant is what the site granted for the month in node-minutes (source site, AC-22), 0 when unknown.
	// Set only when the budget's observation is from this period and the grant is not below what was
	// left at the observation (Share). It decides the warning percentage and nothing else: Ceiling,
	// Left and admission keep using the remainder (D-56).
	Grant int64
}

// Ceiling is the most the project can still spend this period before counting bookings: the share
// less what was used, bounded by the site's remainder less what ended since it was observed
// (min(share left, site left), PRD-HPC-1461 D-10, D-56). May be negative.
func (t Tier) Ceiling() int64 {
	if t.Unbounded {
		return math.MaxInt64 / 4
	}
	c := t.Share - t.Used
	if s := t.SiteLeft - t.SiteUsed; t.SiteBound && s < c {
		return s
	}
	return c
}

// SiteBinds reports whether the site's remainder, not a project share, is the limit: the budget
// is the site's own (source site), or the site's figure is the lower bound beside a share. This
// decides the refusal reason, SiteLimit or BudgetBlocked (AC-23).
func (t Tier) SiteBinds() bool {
	return t.SiteSource || (t.SiteBound && t.SiteLeft-t.SiteUsed < t.Share-t.Used)
}

// Total is the most the project can spend in this period as things stand: what was used plus what
// the ceiling still allows (the lower of the share and the site's remainder). It is the base of the
// warning percentage (AC-22). May be zero or negative when nothing may be spent.
func (t Tier) Total() int64 { return t.Used + t.Ceiling() }

// Percent is the share of the month's allowance that is spent or committed, rounded down, capped at
// 100 000 so a tiny total cannot overflow a gauge. -1 when there is nothing to be a percentage of
// (AC-22). With a known Grant (source site) it is (Grant - Share + Used + Booked) / Grant: what the
// site had already charged when it was asked (the grant less what it reported left), plus what ended
// since, plus what is booked. Otherwise it is used plus booked over Total (the lower of the share and the
// site's remainder, as before the grant existed).
func (t Tier) Percent() int64 {
	if t.Unbounded {
		return -1 // no figure from the site: nothing to be a percentage of
	}
	if t.Grant > 0 {
		return min((t.Grant-t.Share+t.Used+t.Booked)*100/t.Grant, 100000)
	}
	total := t.Total()
	if total <= 0 {
		return -1
	}
	return min((t.Used+t.Booked)*100/total, 100000)
}

// Left is the ceiling minus what is booked, never below zero.
func (t Tier) Left() int64 {
	if l := t.Ceiling() - t.Booked; l > 0 {
		return l
	}
	return 0
}

// Ledger is one project's spend on one site.
type Ledger struct {
	// PeriodStart is always the start of the calendar month (UTC) containing now.
	PeriodStart time.Time
	// CountedFrom is where the spend in Tiers starts: PeriodStart, or, for a source-site budget,
	// the observation the share is the remainder at (what ended before it is already in the figure).
	CountedFrom time.Time
	Tiers       map[hpcv1alpha1.Tier]Tier
	// Active is the number of jobs holding a booking or running as a guest: Queued + Running.
	Active int32
	// SiteJobs is the number of Slurm jobs the project's active jobs hold, the count a site's submit cap applies
	// to (AC-39): every active job, a guest included, since a guest's own job stays queued at the site (held by
	// its host) until its step is over.
	SiteJobs int32
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

// OpenHost reports whether the job ended as a host whose Slurm job still holds the node for guests
// (D-26): its last attempt is provisional and its booking is still held until the job's end is reported.
func OpenHost(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	n := len(j.Status.Attempts)
	return j.Status.Booking != nil && j.Status.Phase.Terminal() && n > 0 && j.Status.Attempts[n-1].HostJobOpen
}

// heldMinutes is what a held booking counts for. An open host's node is charged by the site until its
// Slurm job ends, which guests can make longer than the booking: it holds the larger of the booking, its
// provisional settlement and its node time so far (live 2026-11-10, host 5439966: a 1-minute step, a
// node held 21 minutes for a guest).
func heldMinutes(j *hpcv1alpha1.HPCArgoWorkflowJob, now time.Time) int64 {
	b := j.Status.Booking.NodeMinutes
	if !OpenHost(j) {
		return b
	}
	b = max(b, j.Status.Attempts[len(j.Status.Attempts)-1].SettledNodeMinutes)
	if r := j.Status.Run; r != nil && r.StartTime != nil && r.StartTime.Before(&metav1.Time{Time: now}) {
		b = max(b, int64(max(j.Spec.Nodes, 1))*int64(math.Ceil(now.Sub(r.StartTime.Time).Minutes())))
	}
	return b
}

// Holds reports whether a job currently holds its booking: an active job (a guest excepted), or a host
// whose Slurm job still holds the node after its step ended (D-26).
func Holds(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	if OpenHost(j) {
		return true
	}
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

// CountedFrom is the time from which a settled attempt counts against the site's budget now: the start of the period,
// except for a source-site budget (D-56), whose share is what the site reported left at ObservedAt. That figure already
// counts every job that ended before it, so only later ends are used against it; an attempt that was running at ObservedAt is
// counted whole: conservative, never an overspend. A job object whose last attempt ended before this time can be deleted
// without changing the ledger; any other cannot.
func CountedFrom(site *hpcv1alpha1.HPCSite, now time.Time) time.Time {
	if b := site.Spec.Budget; b.Source == hpcv1alpha1.BudgetSourceSite && b.ObservedAt != nil {
		return b.ObservedAt.UTC()
	}
	return PeriodStart(now)
}

// Compute derives the ledger of one HPCSite from the jobs booked against it. pending holds
// bookings made in this process that the informer cache may not show yet, keyed by job UID; a job
// present in jobs with a booking takes precedence over its pending entry.
func Compute(site *hpcv1alpha1.HPCSite, jobs []hpcv1alpha1.HPCArgoWorkflowJob,
	pending map[string]hpcv1alpha1.Booking, now time.Time) Ledger {
	periodStart := PeriodStart(now)
	start := CountedFrom(site, now)
	siteSource := site.Spec.Budget.Source == hpcv1alpha1.BudgetSourceSite
	// Every allocation has its own ledger (AC-84): a spent one never touches another.
	l := Ledger{PeriodStart: periodStart, CountedFrom: start, Tiers: map[hpcv1alpha1.Tier]Tier{}}
	for _, tier := range hpcv1alpha1.Tiers {
		hours, known := site.Spec.Budget.NodeHours(tier)
		l.Tiers[tier] = Tier{Share: hours * 60, SiteSource: siteSource, Unbounded: !known}
	}
	applyGrant(&l, site, periodStart)
	// Source share with siteLeft: the site's remainder is an upper bound beside the share. It counts
	// only when observed within this period, since what ended before the observation is already in
	// the site's figure and a figure from last month says nothing about this one. Attempts that ended
	// at or after the observation are taken off it (an attempt running at the time counts whole:
	// conservative, never an overspend).
	var siteObserved time.Time
	if b := site.Spec.Budget; b.Source != hpcv1alpha1.BudgetSourceSite && b.SiteLeft != nil &&
		b.SiteLeft.ObservedAt != nil && !b.SiteLeft.ObservedAt.UTC().Before(start) {
		siteObserved = b.SiteLeft.ObservedAt.UTC()
		for _, tier := range hpcv1alpha1.Tiers {
			// No figure for an allocation: no site bound, the share alone applies (AC-84).
			if left, ok := b.SiteLeft.NodeHours(tier); ok {
				l.Tiers[tier] = withSiteBound(l.Tiers[tier], left*60)
			}
		}
	}
	seen := map[string]bool{}
	for i := range jobs {
		j := &jobs[i]
		if !onSite(j, site) {
			continue
		}
		// Every finished attempt counts in the period it ended (a continued job spends per attempt).
		for _, a := range j.Status.Attempts {
			// An open host's settlement is provisional: its held booking counts instead (heldMinutes).
			if a.HostJobOpen || a.EndTime.Time.Before(start) {
				continue
			}
			t := l.Tiers[a.Tier]
			// Records stored before settlement was validated can carry garbage (negative) minutes.
			t.Used += max(a.SettledNodeMinutes, 0)
			if t.SiteBound && !a.EndTime.Time.Before(siteObserved) {
				t.SiteUsed += max(a.SettledNodeMinutes, 0)
			}
			l.Tiers[a.Tier] = t
		}
		if b := j.Status.Booking; b != nil && b.SiteName == site.Name && b.SiteNamespace == site.Namespace {
			seen[string(j.UID)] = true
			if Holds(j) {
				l.SiteJobs++
				t := l.Tiers[b.Tier]
				t.Booked += heldMinutes(j, now)
				l.Tiers[b.Tier] = t
			}
			// A guest holds no booking of its own (its host's covers the node) but is still active, and its own
			// Slurm job stays queued (held) at the site until its host cancels it with the step's result: it
			// counts toward the site's submit cap like any queued job (AC-39).
			if holdsOrGuest(j) {
				l.count(isRunning(j))
				if !Holds(j) {
					l.SiteJobs++
				}
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
		l.SiteJobs++
	}
	return l
}

// applyGrant sets each tier's Grant (AC-22), for a source-site budget with a grant figure. A grant counts only
// when its observation is from this period (an older figure describes another month) and is not below what was
// left at it (inconsistent: ignored, the earlier warning base applies).
func applyGrant(l *Ledger, site *hpcv1alpha1.HPCSite, periodStart time.Time) {
	b := site.Spec.Budget
	if b.Source != hpcv1alpha1.BudgetSourceSite || b.ObservedAt == nil || b.ObservedAt.UTC().Before(periodStart) {
		return
	}
	for _, tier := range hpcv1alpha1.Tiers {
		t := l.Tiers[tier]
		if g, ok := b.GrantNodeHours.Of(tier); ok && !t.Unbounded && g*60 >= t.Share {
			t.Grant = g * 60
			l.Tiers[tier] = t
		}
	}
}

func withSiteBound(t Tier, siteLeftMinutes int64) Tier {
	t.SiteBound, t.SiteLeft = true, siteLeftMinutes
	return t
}

// onSite reports whether the job's spend belongs to this HPCSite: its booking names it, or (between
// a continuation and its next booking) its last booking did.
func onSite(j *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) bool {
	if b := j.Status.Booking; b != nil {
		return b.SiteName == site.Name && b.SiteNamespace == site.Namespace
	}
	return j.Status.Site != nil && j.Status.Site.Name == site.Name && j.Status.Site.Namespace == site.Namespace
}
