package controller

import (
	"context"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

// Deterministic node sharing (PRD-HPC-1461 D-26, D-70). A running host can only take a guest that is still
// PENDING at the site, so steps that all start at once on free nodes never pack (live 2026-11-10: four 1-GPU
// steps, four nodes, 30 node-minutes for 8). The controller therefore decides the packing at admission, one job
// at a time (admission is serial), from the jobs it already placed:
//
//   - a shareable step joins an OPEN host (running, or queued and not yet over HostStartWait) of its own
//     (site, scope, class) with a free slot and enough time left, as its GUEST; only when none fits does it open a
//     host, in a lane that leaves room for a guest. N steps of one class need ceil(N / gpusPerNode) hosts.
//   - a guest is submitted deferred (the runner sets its BeginTime far ahead, see runner.submit): it can never
//     start on a node of its own, and the host's slot manager takes it as it does a job in its offer window. A
//     native Slurm hold would need the slot manager to learn a new claim protocol; a deferred job needs none.
//   - a guest that is not taken is planned again (replanGuest): host ended, never started, no longer fits, or did
//     not take it. It then joins another open host or becomes a host; the runner submits it as one.

const (
	// ShareMarginMinutes is the time a guest must end before its host's job does: the slot manager's own margin
	// (5 min, payload/slots.py MARGIN_S) plus a minute for its poll.
	ShareMarginMinutes = 6
	// DefaultHostStartWait is how long a guest waits for a host whose Slurm job is still queued before it is
	// planned again; a queued host older than this takes no new guests either.
	DefaultHostStartWait = 20 * time.Minute
	// DefaultGuestTakeWait is how long a guest waits on a RUNNING host that has not taken it before it becomes a
	// host itself (the host skipped it: its step does not fit the host's time or its token).
	DefaultGuestTakeWait = 10 * time.Minute
	// MaxShareReplans bounds how often one guest is planned again; past it the guest becomes a host.
	MaxShareReplans = 3
)

// statePending is the scheduler state of a queued site job.
const statePending = "PENDING"

// Reasons a guest that was not taken is planned again (metric label, event).
const (
	replanHostEnded      = "host_ended"
	replanHostNotStarted = "host_not_started"
	replanNoLongerFits   = "no_longer_fits"
	replanNotTaken       = "not_taken"
)

// shareableClass returns the job's class when an attempt can share a node: the job and the site opted in, it runs in
// serve mode and its class has GPUs to split. A resumable step shares too (AC-73): its guest fits by its lead (see
// fits), and its host's job is not padded. The checks mirror runnerjob.Build and the runner's Settings.sharing.
func shareableClass(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) (hpcv1alpha1.SiteClass, bool) {
	class, ok := admission.ClassOf(site, job.Spec.Class)
	if !ok || job.Spec.Sharing == nil || !site.Spec.Sharing || job.Spec.StepName == "" || class.GPUs <= 0 {
		return hpcv1alpha1.SiteClass{}, false
	}
	return class, true
}

// resumableOf reports whether the attempt is treated as resumable and its signal lead in minutes: the declaration,
// else the default lead of a step the controller found resumable (D-74), unless the step opted out. pl is the
// placement being decided (the job's own may be an earlier attempt's).
func resumableOf(job *hpcv1alpha1.HPCArgoWorkflowJob, pl *hpcv1alpha1.Placement) (int32, bool) {
	switch {
	case job.Spec.ResumableOptOut:
		return 0, false
	case job.Spec.Resumable != nil:
		return job.Spec.Resumable.LeadMinutes, true
	case pl != nil && pl.Resumable != nil && pl.Resumable.Verified:
		return hpcv1alpha1.DefaultLeadMinutes, true
	}
	return 0, false
}

// fits reports whether a guest of step limit and resumable lead (0: plain) can be taken by a host with remaining
// minutes of its job left. It is the slot manager's rule (payload/slots.py fits) plus a minute for its poll: the step
// and the margin must end before the host does; a resumable guest is also taken when only its lead plus the margin
// do, since it is told to save before the host's end.
func fits(remaining, limit, lead int32) bool {
	return remaining >= limit+ShareMarginMinutes || (lead > 0 && remaining >= lead+ShareMarginMinutes)
}

// withShare returns pl with the attempt's lane and share plan. A step that cannot share gets the lane of withLane
// alone. A shareable step is a guest of an open host (no lane of its own: it never holds a one-job lane) or a host
// in a lane that leaves room for a guest.
func (r *HPCArgoWorkflowJobReconciler) withShare(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, pl *hpcv1alpha1.Placement) (*hpcv1alpha1.Placement, error) {
	class, ok := shareableClass(job, site)
	if !ok {
		out := *pl
		out.Share = nil
		return r.withLane(ctx, job, site, &out)
	}
	return r.planShare(ctx, job, site, class, pl, "", 0, false)
}

// planShare decides the role of a shareable attempt. exclude names a host (namespace/name) it must not join;
// forceHost makes it a host without looking for one; replans is how often it was planned again.
func (r *HPCArgoWorkflowJobReconciler) planShare(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, class hpcv1alpha1.SiteClass, pl *hpcv1alpha1.Placement, exclude string, replans int32,
	forceHost bool) (*hpcv1alpha1.Placement, error) {
	out := *pl
	out.Share = nil
	now := metav1.NewTime(r.now())
	if !forceHost {
		peers, err := r.sharePeers(ctx, job, site)
		if err != nil {
			return nil, err
		}
		if host := r.pickHost(peers, site, class, pl.TimeLimitMinutes, leadOf(job, pl), exclude); host != nil {
			// The guest rides on the host's node: no lane of its own, so it never takes a one-job lane a host needs.
			out.QOS, out.Reservation = "", ""
			out.Share = &hpcv1alpha1.SharePlan{Role: hpcv1alpha1.ShareGuest, Host: client.ObjectKeyFromObject(host).String(),
				Since: now, Replans: replans}
			return &out, nil
		}
	}
	hosted, err := r.hostLane(ctx, job, site, &out)
	if err != nil {
		return nil, err
	}
	_, resumable := resumableOf(job, pl)
	hosted.Share = &hpcv1alpha1.SharePlan{Role: hpcv1alpha1.ShareHost, Since: now, Replans: replans,
		JobLimitMinutes: hostJobLimit(site, class, hosted.TimeLimitMinutes, hosted.QOS, resumable)}
	return hosted, nil
}

// hostLane places a new host. It prefers a lane that leaves room for one guest of the step's own size, so the
// node can be shared at all (a 30-minute step in a 30-minute lane could never take a guest); the class's own QoS comes next when it leaves room; a lane that does not leave room is used only when nothing else can
// give a guest room.
func (r *HPCArgoWorkflowJobReconciler) hostLane(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, pl *hpcv1alpha1.Placement) (*hpcv1alpha1.Placement, error) {
	room, err := r.laneFor(ctx, job, site, pl, pl.TimeLimitMinutes+ShareMarginMinutes)
	if err != nil {
		return nil, err
	}
	if room.QOS != "" {
		return room, nil
	}
	// No lane leaves room. The class's own QoS does when its maximum allows (it is no one-job lane): a host there
	// can take guests, a host in a lane that cannot would force every later step onto a node of its own.
	if class, ok := admission.ClassOf(site, job.Spec.Class); ok && class.MaxWallTimeMinutes >= pl.TimeLimitMinutes+ShareMarginMinutes {
		out := *pl
		out.QOS, out.Reservation = "", ""
		return &out, nil
	}
	return r.laneFor(ctx, job, site, pl, pl.TimeLimitMinutes)
}

// leadOf is the resumable lead of the attempt in minutes, 0 for a plain step.
func leadOf(job *hpcv1alpha1.HPCArgoWorkflowJob, pl *hpcv1alpha1.Placement) int32 {
	lead, _ := resumableOf(job, pl)
	return lead
}

// hostJobLimit is the time limit of a host's Slurm job as the runner asks for it (Runner.submit): the step's own
// limit padded to twice, capped by the class's maximum and, in a lane, the lane's; no known cap, no padding. A
// resumable host's job is not padded (its end is reported when the job ends, AC-73).
func hostJobLimit(site *hpcv1alpha1.HPCSite, class hpcv1alpha1.SiteClass, step int32, qos string, resumable bool) int32 {
	if resumable {
		return step
	}
	limit := class.MaxWallTimeMinutes
	if m := qosMax(site, qos); m > 0 && (limit == 0 || m < limit) {
		limit = m
	}
	if limit <= 0 {
		return step
	}
	return max(step, min(2*step, limit))
}

// qosMax is the wall-time maximum of a QoS of the site profile (0: none, or unknown, or no QoS).
func qosMax(site *hpcv1alpha1.HPCSite, name string) int32 {
	for _, q := range site.Spec.Profile.QOS {
		if q.Name == name && name != "" {
			return q.MaxWallTimeMinutes
		}
	}
	return 0
}

// shareRoleOf is the role of a job in the plan; a job placed before the plan existed is its own host.
func shareRoleOf(j *hpcv1alpha1.HPCArgoWorkflowJob) hpcv1alpha1.ShareRole {
	if p := j.Status.Placement; p != nil && p.Share != nil {
		return p.Share.Role
	}
	return hpcv1alpha1.ShareHost
}

// activePeer reports whether the job holds a node or a place on one: booked and not over, or an open host.
func activePeer(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	return (j.Status.Booking != nil && !j.Status.Phase.Terminal()) || ledger.OpenHost(j)
}

// inShareGroup reports whether o shares a node with jobs of site, scope and class: same project site, same scope
// and class, and active.
func inShareGroup(o *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, scope, class string) bool {
	b := o.Status.Booking
	return o.Spec.Sharing != nil && o.Spec.Sharing.Scope == scope && o.Spec.Class == class && activePeer(o) &&
		b != nil && b.SiteName == site.Name && b.SiteNamespace == site.Namespace
}

// sharePeers lists the active jobs of job's share group (site, scope, class), the job itself excluded. A job this
// process placed that the informer cache does not show yet is taken from the process's own memory, so a burst of
// admissions never plans against a stale picture.
func (r *HPCArgoWorkflowJobReconciler) sharePeers(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) ([]*hpcv1alpha1.HPCArgoWorkflowJob, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs); err != nil {
		return nil, err
	}
	byUID := make(map[string]*hpcv1alpha1.HPCArgoWorkflowJob, len(jobs.Items))
	for i := range jobs.Items {
		byUID[string(jobs.Items[i].UID)] = &jobs.Items[i]
	}
	r.mu.Lock()
	for uid, mine := range r.pendingShare {
		cached := byUID[uid]
		switch {
		case cached != nil && cached.Status.Placement != nil && cached.Status.Placement.Share != nil &&
			!cached.Status.Placement.Share.Since.Before(&metav1.Time{Time: mine.Status.Placement.Share.Since.Truncate(time.Second)}):
			// The cache shows the decision (or a later one). The API keeps whole seconds, so ours is compared as such.
			delete(r.pendingShare, uid)
		case cached == nil && r.now().Sub(mine.Status.Booking.At.Time) > time.Minute:
			delete(r.pendingShare, uid) // the job is gone
		default:
			byUID[uid] = mine
		}
	}
	r.mu.Unlock()
	scope := job.Spec.Sharing.Scope
	var out []*hpcv1alpha1.HPCArgoWorkflowJob
	for uid, o := range byUID {
		if uid != string(job.UID) && inShareGroup(o, site, scope, job.Spec.Class) {
			out = append(out, o)
		}
	}
	return out, nil
}

// rememberShare keeps a job this process just placed until the cache shows its plan.
func (r *HPCArgoWorkflowJobReconciler) rememberShare(job *hpcv1alpha1.HPCArgoWorkflowJob, pl *hpcv1alpha1.Placement,
	b *hpcv1alpha1.Booking) {
	if pl.Share == nil || b == nil {
		return
	}
	mine := job.DeepCopy()
	mine.Status.Placement, mine.Status.Booking = pl, b
	mine.Status.Phase = hpcv1alpha1.PhaseAdmitted
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingShare == nil {
		r.pendingShare = map[string]*hpcv1alpha1.HPCArgoWorkflowJob{}
	}
	r.pendingShare[string(job.UID)] = mine
}

// hostSlot is one open host with what it can still take.
type hostSlot struct {
	job       *hpcv1alpha1.HPCArgoWorkflowJob
	free      int32 // slots not held by its own step or a guest
	remaining int32 // minutes of its Slurm job left
}

// pickHost returns the open host of the peers a guest with the given step limit joins, or nil. A host qualifies
// when it has a free slot, enough time left for the step and the margin, was not excluded and is not a queued host
// that waited too long. The fullest host wins, so nodes fill one after the other and N steps use ceil(N / slots)
// of them; ties go to the older host, then the name.
func (r *HPCArgoWorkflowJobReconciler) pickHost(peers []*hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite,
	class hpcv1alpha1.SiteClass, limit, lead int32, exclude string) *hpcv1alpha1.HPCArgoWorkflowJob {
	fit := make([]hostSlot, 0, len(peers))
	for _, h := range r.hostSlots(peers, site, class) {
		if client.ObjectKeyFromObject(h.job).String() == exclude || h.free <= 0 || !fits(h.remaining, limit, lead) {
			continue
		}
		if !hostStarted(h.job) && r.now().Sub(hostSince(h.job)) > r.hostStartWait() {
			continue
		}
		fit = append(fit, h)
	}
	if len(fit) == 0 {
		return nil
	}
	sort.SliceStable(fit, func(i, j int) bool {
		a, b := fit[i], fit[j]
		switch {
		case a.free != b.free:
			return a.free < b.free
		case !a.job.CreationTimestamp.Equal(&b.job.CreationTimestamp):
			return a.job.CreationTimestamp.Before(&b.job.CreationTimestamp)
		}
		return a.job.Name < b.job.Name
	})
	return fit[0].job
}

// hostSlots computes the free slots and remaining time of every open host among the peers. A host's own step holds
// one slot until it ends; a guest holds one on the host that took it, else on the host it is planned onto.
func (r *HPCArgoWorkflowJobReconciler) hostSlots(peers []*hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite,
	class hpcv1alpha1.SiteClass) []hostSlot {
	bySiteJob := map[string]string{} // site job id -> host key, for guests a host took
	used := map[string]int32{}
	hosts := make([]*hpcv1alpha1.HPCArgoWorkflowJob, 0, len(peers))
	for _, p := range peers {
		if shareRoleOf(p) != hpcv1alpha1.ShareHost {
			continue
		}
		key := client.ObjectKeyFromObject(p).String()
		hosts = append(hosts, p)
		if !p.Status.Phase.Terminal() {
			used[key]++ // its own step
		}
		if run := p.Status.Run; run != nil && current(run, p.Status.Attempt) && run.SiteJobID != "" {
			bySiteJob[run.SiteJobID] = key
		}
	}
	for _, p := range peers {
		if shareRoleOf(p) != hpcv1alpha1.ShareGuest || p.Status.Phase.Terminal() {
			continue
		}
		key := ""
		if p.Status.Placement.Share != nil {
			key = p.Status.Placement.Share.Host
		}
		if run := p.Status.Run; run != nil && current(run, p.Status.Attempt) && run.HostSiteJobID != "" && bySiteJob[run.HostSiteJobID] != "" {
			key = bySiteJob[run.HostSiteJobID]
		}
		used[key]++
	}
	out := make([]hostSlot, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, hostSlot{job: h, free: class.GPUs - used[client.ObjectKeyFromObject(h).String()],
			remaining: r.hostRemaining(h, site, class)})
	}
	return out
}

// hostRemaining is how many minutes of its Slurm job a host has left: its whole limit while it is queued, the
// limit less the time since it started once it runs.
func (r *HPCArgoWorkflowJobReconciler) hostRemaining(h *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite,
	class hpcv1alpha1.SiteClass) int32 {
	limit := hostLimitOf(h, site, class)
	if !hostStarted(h) {
		return limit
	}
	used := int32(r.now().Sub(h.Status.Run.StartTime.Time) / time.Minute)
	return max(limit-used, 0)
}

// hostLimitOf is the time limit of a host's Slurm job: the one the plan recorded, else computed as the runner does.
func hostLimitOf(h *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, class hpcv1alpha1.SiteClass) int32 {
	if p := h.Status.Placement; p != nil && p.Share != nil && p.Share.JobLimitMinutes > 0 {
		return p.Share.JobLimitMinutes
	}
	qos := ""
	if p := h.Status.Placement; p != nil {
		qos = p.QOS
	}
	_, resumable := resumableOf(h, h.Status.Placement)
	return hostJobLimit(site, class, h.EffectiveWallMinutes(), qos, resumable)
}

// hostStarted reports whether the host's Slurm job runs, by the site's own report.
func hostStarted(h *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	run := h.Status.Run
	if run == nil || !current(run, h.Status.Attempt) || run.StartTime == nil {
		return false
	}
	return run.State == ledger.SiteStateRunning || (run.State != statePending && run.State != "")
}

// hostSince is when the host's role was decided, else when it was booked.
func hostSince(h *hpcv1alpha1.HPCArgoWorkflowJob) time.Time {
	if p := h.Status.Placement; p != nil && p.Share != nil && !p.Share.Since.IsZero() {
		return p.Share.Since.Time
	}
	if b := h.Status.Booking; b != nil {
		return b.At.Time
	}
	return h.CreationTimestamp.Time
}

func (r *HPCArgoWorkflowJobReconciler) hostStartWait() time.Duration {
	if r.HostStartWait > 0 {
		return r.HostStartWait
	}
	return DefaultHostStartWait
}

func (r *HPCArgoWorkflowJobReconciler) guestTakeWait() time.Duration {
	if r.GuestTakeWait > 0 {
		return r.GuestTakeWait
	}
	return DefaultGuestTakeWait
}

// replanGuest plans a guest again when its host cannot or will not take it (no starvation): the host ended, never
// started, no longer has the time, or runs and did not take it. A guest that a host took is left alone: the runner
// handles a host lost under it. The guest then joins another open host or becomes a host; the runner, which reads
// the plan, submits it as one.
func (r *HPCArgoWorkflowJobReconciler) replanGuest(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) error {
	p := job.Status.Placement
	if p == nil || p.Share == nil || p.Share.Role != hpcv1alpha1.ShareGuest || job.Status.Booking == nil || job.Status.Phase.Terminal() {
		return nil
	}
	if run := job.Status.Run; run != nil && current(run, job.Status.Attempt) && (run.HostSiteJobID != "" || run.Terminal) {
		return nil
	}
	class, ok := shareableClass(job, site)
	if !ok {
		return nil
	}
	why, err := r.guestStranded(ctx, job, site, class)
	if err != nil || why == "" {
		return err
	}
	replans := p.Share.Replans + 1
	np, err := r.planShare(ctx, job, site, class, p, p.Share.Host, replans, why == replanNotTaken || replans > MaxShareReplans)
	if err != nil {
		return err
	}
	orig := job.DeepCopy()
	job.Status.Placement = np
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	r.rememberShare(job, np, job.Status.Booking)
	r.rememberLane(string(job.UID), np.QOS)
	metrics.ShareReplans.WithLabelValues(why).Inc()
	metrics.SharePlacements.WithLabelValues(string(np.Share.Role)).Inc()
	if r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeNormal, "ShareReplanned", replanMessage(p.Share.Host, why, np.Share))
	}
	return nil
}

// replanMessage says in plain words why a guest was planned again and where it went.
func replanMessage(host, why string, to *hpcv1alpha1.SharePlan) string {
	reason := map[string]string{
		replanHostEnded:      "its host ended",
		replanHostNotStarted: "its host did not start in time",
		replanNoLongerFits:   "its step no longer fits the host's time",
		replanNotTaken:       "its host did not take it",
	}[why]
	if to.Role == hpcv1alpha1.ShareGuest {
		return "the guest of " + host + " was planned again because " + reason + ": it joins " + to.Host
	}
	return "the guest of " + host + " was planned again because " + reason + ": it becomes a host"
}

// guestStranded returns why a guest that no host took must be planned again, or "" while waiting is right.
func (r *HPCArgoWorkflowJobReconciler) guestStranded(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, class hpcv1alpha1.SiteClass) (string, error) {
	share := job.Status.Placement.Share
	ns, name, _ := splitKey(share.Host)
	var host hpcv1alpha1.HPCArgoWorkflowJob
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &host); err != nil {
		if apierrors.IsNotFound(err) {
			return replanHostEnded, nil
		}
		return "", err
	}
	if mine := r.rememberedShare(string(host.UID)); mine != nil && host.Status.Placement == nil {
		host = *mine
	}
	if !activePeer(&host) || shareRoleOf(&host) != hpcv1alpha1.ShareHost {
		return replanHostEnded, nil
	}
	now := r.now()
	if !hostStarted(&host) {
		if now.Sub(hostSince(&host)) > r.hostStartWait() {
			return replanHostNotStarted, nil
		}
		return "", nil
	}
	if !fits(r.hostRemaining(&host, site, class), job.Status.Placement.TimeLimitMinutes, leadOf(job, job.Status.Placement)) {
		return replanNoLongerFits, nil
	}
	// A running host takes a waiting guest at its next look (seconds); one that has not by now will not.
	waiting := share.Since.Time
	if s := host.Status.Run.StartTime.Time; s.After(waiting) {
		waiting = s
	}
	if now.Sub(waiting) > r.guestTakeWait() {
		return replanNotTaken, nil
	}
	return "", nil
}

// rememberedShare is the in-memory copy of a job this process placed, or nil.
func (r *HPCArgoWorkflowJobReconciler) rememberedShare(uid string) *hpcv1alpha1.HPCArgoWorkflowJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pendingShare[uid]
}

// splitKey splits a namespace/name key.
func splitKey(key string) (ns, name string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:], true
		}
	}
	return "", key, false
}
