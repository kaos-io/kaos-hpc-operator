package controller

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// Learned time limit (PRD-HPC-1461 F-41, D-67). The spec's wallTimeMinutes is the cap; the limit
// of an attempt is learned from the run times of the same step's earlier successful jobs that are
// still in the cluster (no store of its own), and written to status.placement.

// capsOf are the bounds of a job's time limit on a site.
func capsOf(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) timelimit.Caps {
	c := timelimit.Caps{Spec: job.Spec.WallTimeMinutes, Site: site.Spec.Limits.MaxWallTimeMinutes}
	if class, ok := admission.ClassOf(site, job.Spec.Class); ok {
		c.Class = class.MaxWallTimeMinutes
	}
	return c
}

// placementFor chooses the time limit of the job's next attempt. A job that already got its one
// timeout retry keeps that limit. Only serve-mode jobs learn (the runner Job carries the limit
// there), and a resumable step keeps its declared limit: its checkpoint signal is timed from it.
func (r *HPCArgoWorkflowJobReconciler) placementFor(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (*hpcv1alpha1.Placement, error) {
	if p := job.Status.Placement; p != nil && p.Source == hpcv1alpha1.PlacementTimeoutRetry {
		return p, nil
	}
	caps := capsOf(job, site)
	if job.Spec.StepName == "" {
		return &hpcv1alpha1.Placement{TimeLimitMinutes: caps.Spec, Source: hpcv1alpha1.PlacementDeclared}, nil
	}
	history, err := r.historyOf(ctx, job)
	if err != nil {
		return nil, err
	}
	if job.Spec.Resumable != nil {
		return declaredWithWarning(job, caps, history, 0), nil
	}
	samples := samplesOf(history, r.now())
	limit, source := timelimit.Learn(samples, caps)
	if source == hpcv1alpha1.PlacementDeclared {
		return declaredWithWarning(job, caps, history, int32(len(samples))), nil
	}
	return &hpcv1alpha1.Placement{TimeLimitMinutes: limit, Source: source, Samples: int32(len(samples))}, nil
}

// declaredWithWarning is the placement at the declared limit, with a warning when a recent run of
// the step was stopped at that same limit. It never changes the limit.
func declaredWithWarning(job *hpcv1alpha1.HPCArgoWorkflowJob, caps timelimit.Caps,
	history []*hpcv1alpha1.HPCArgoWorkflowJob, samples int32) *hpcv1alpha1.Placement {
	p := &hpcv1alpha1.Placement{TimeLimitMinutes: caps.Spec, Source: hpcv1alpha1.PlacementDeclared, Samples: samples}
	if stoppedAtLimit(history, caps.Spec) {
		p.Warning = timelimit.WarnMessage(caps.Spec, job.Spec.Resumable != nil)
	}
	return p
}

// RecentRuns is how many of a step's latest finished jobs are looked at for a time-limit warning.
const RecentRuns = 3

// stoppedAtLimit reports whether, among the last RecentRuns finished jobs of the step (history is
// newest first), one ended WallTimeReached at the given declared limit.
func stoppedAtLimit(history []*hpcv1alpha1.HPCArgoWorkflowJob, limit int32) bool {
	for _, o := range history[:min(len(history), RecentRuns)] {
		if o.Status.Reason == hpcv1alpha1.ReasonWallTimeReached && o.Spec.WallTimeMinutes == limit &&
			o.EffectiveWallMinutes() == limit {
			return true
		}
	}
	return false
}

// finishedAt orders jobs: the end of the last attempt, else the job's creation.
func finishedAt(o *hpcv1alpha1.HPCArgoWorkflowJob) time.Time {
	if n := len(o.Status.Attempts); n > 0 && !o.Status.Attempts[n-1].EndTime.IsZero() {
		return o.Status.Attempts[n-1].EndTime.Time
	}
	return o.CreationTimestamp.Time
}

// historyOf returns the finished jobs of the same step (same Key), newest first, the job itself excluded.
func (r *HPCArgoWorkflowJobReconciler) historyOf(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) ([]*hpcv1alpha1.HPCArgoWorkflowJob, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs, client.InNamespace(job.Namespace)); err != nil {
		return nil, err
	}
	key := timelimit.KeyOf(job)
	var out []*hpcv1alpha1.HPCArgoWorkflowJob
	for i := range jobs.Items {
		o := &jobs.Items[i]
		if o.UID != job.UID && o.Spec.StepName != "" && timelimit.KeyOf(o) == key && o.Status.Phase.Terminal() {
			out = append(out, o)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return finishedAt(out[i]).After(finishedAt(out[j])) })
	return out, nil
}

// samplesOf returns the run times of the step's earlier jobs: Succeeded, never continued (a
// continued job's last segment is not the step's run time), with valid site times.
func samplesOf(history []*hpcv1alpha1.HPCArgoWorkflowJob, now time.Time) []time.Duration {
	var out []time.Duration
	for _, o := range history {
		if o.Status.Phase != hpcv1alpha1.PhaseSucceeded || o.Status.Continuations > 0 {
			continue
		}
		if d, ok := runTime(o.Status.Run, o.Status.Attempt, now); ok {
			out = append(out, d)
		}
	}
	return out
}

// runTime is start to end of the job's final attempt, when the report is current and both times
// are valid (the rules of settle: set, after 2000, not in the future, end not before start).
func runTime(run *hpcv1alpha1.RunStatus, attempt int32, now time.Time) (time.Duration, bool) {
	if run == nil || !current(run, attempt) || !validSiteTime(run.StartTime, now) || !validSiteTime(run.EndTime, now) {
		return 0, false
	}
	if d := run.EndTime.Sub(run.StartTime.Time); d >= 0 {
		return d, true
	}
	return 0, false
}

// timeoutRetry decides the one retry after a TIMEOUT: the attempt ran with a learned limit below
// the spec's cap and no retry happened yet. It returns the retry's placement, or nil.
func timeoutRetry(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, run *hpcv1alpha1.RunStatus) *hpcv1alpha1.Placement {
	p := job.Status.Placement
	if p == nil || p.Source != hpcv1alpha1.PlacementLearned || !strings.EqualFold(run.State, "TIMEOUT") {
		return nil
	}
	limit := timelimit.Retry(capsOf(job, site))
	if limit <= p.TimeLimitMinutes {
		return nil
	}
	return &hpcv1alpha1.Placement{TimeLimitMinutes: limit, Source: hpcv1alpha1.PlacementTimeoutRetry, Samples: p.Samples}
}

// retryMessage explains a timeout retry in status.message.
func retryMessage(from, to, attempt int32) string {
	return "; the learned limit of " + strconv.Itoa(int(from)) + " min was reached, retrying once with " +
		strconv.Itoa(int(to)) + " min as attempt " + strconv.Itoa(int(attempt))
}

// Lane placement (PRD-HPC-1461 F-42, F-44, D-68, D-69). The controller knows no site: the ordered
// candidates come from its configuration, the facts from the HPCSite profile. The lane is chosen
// once per attempt, at admission, after the time limit is known, and written to status.placement.

// withLane returns pl with the first eligible lane of r.LaneCandidates, or pl unchanged (the
// class's own QoS) when there are none configured, the job is not in serve mode, or no candidate
// is eligible. It never changes pl in place: a timeout retry hands in the job's own placement.
func (r *HPCArgoWorkflowJobReconciler) withLane(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, pl *hpcv1alpha1.Placement) (*hpcv1alpha1.Placement, error) {
	out := *pl
	out.QOS, out.Reservation = "", ""
	class, ok := admission.ClassOf(site, job.Spec.Class)
	if len(r.LaneCandidates) == 0 || job.Spec.StepName == "" || !ok {
		return &out, nil
	}
	inUse, err := r.laneUse(ctx, job, site)
	if err != nil {
		return nil, err
	}
	lane, found := lanes.Choose(r.LaneCandidates, lanes.Request{Class: class, Profile: site.Spec.Profile,
		LimitMinutes: pl.TimeLimitMinutes, Now: r.now(), InUse: inUse})
	if found {
		out.QOS, out.Reservation = lane.QOS, lane.Reservation
	}
	return &out, nil
}

// identityOf names the site user a job runs as: lanes' per-user limits are per site user (D-69).
func identityOf(site *hpcv1alpha1.HPCSite) string {
	return site.Spec.Site + "/" + site.Spec.Identity.User
}

// laneUse returns how many jobs of the same site identity as site hold each QoS: every job with a
// booking in any HPCSite the controller sees (admitted, submitted or running; a settled attempt
// has none), plus the bookings this process made that the cache may not show yet. The job itself
// is not counted.
func (r *HPCArgoWorkflowJobReconciler) laneUse(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (func(string) int32, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs); err != nil {
		return nil, err
	}
	var sites hpcv1alpha1.HPCSiteList
	if err := r.List(ctx, &sites); err != nil {
		return nil, err
	}
	identity := map[client.ObjectKey]string{}
	for i := range sites.Items {
		identity[client.ObjectKeyFromObject(&sites.Items[i])] = identityOf(&sites.Items[i])
	}
	mine, count, seen := identityOf(site), map[string]int32{}, map[string]bool{}
	hold := func(uid string, b *hpcv1alpha1.Booking, qos string) {
		seen[uid] = true
		if qos != "" && identity[client.ObjectKey{Namespace: b.SiteNamespace, Name: b.SiteName}] == mine {
			count[qos]++
		}
	}
	for i := range jobs.Items {
		o := &jobs.Items[i]
		if o.UID != job.UID && o.Status.Booking != nil && !o.Status.Phase.Terminal() {
			hold(string(o.UID), o.Status.Booking, heldQOS(o))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for uid, qos := range r.pendingLane {
		if b, ok := r.pending[uid]; ok && !seen[uid] && uid != string(job.UID) {
			hold(uid, &b, qos)
		}
	}
	return func(qos string) int32 { return count[qos] }, nil
}

// heldQOS is the lane a booked job occupies at the site: what its runner reports it was submitted
// with (a refused lane falls back to the class default), else the lane it was placed in; none once
// a host took it.
func heldQOS(o *hpcv1alpha1.HPCArgoWorkflowJob) string {
	if run := o.Status.Run; run != nil && current(run, o.Status.Attempt) {
		// A guest a host took runs in the host's allocation and its own site job is only held until
		// the host cancels it: it no longer uses a slot of its lane (PRD-1609).
		if run.HostSiteJobID != "" {
			return ""
		}
		if run.QOS != "" {
			return run.QOS
		}
	}
	if p := o.Status.Placement; p != nil {
		return p.QOS
	}
	return ""
}

// countLanePlacement records the lane chosen at admission. Nothing is counted while no candidates
// are configured, so the metric appears only where lanes are in use.
func (r *HPCArgoWorkflowJobReconciler) countLanePlacement(job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, pl *hpcv1alpha1.Placement) {
	if len(r.LaneCandidates) == 0 || job.Spec.StepName == "" {
		return
	}
	qos := pl.QOS
	if class, ok := admission.ClassOf(site, job.Spec.Class); qos == "" && ok {
		qos = class.QOS
	}
	if qos == "" {
		qos = "default"
	}
	metrics.LanePlacements.WithLabelValues(qos, pl.Reservation, "false").Inc()
}

// reflectLane makes status.placement show the lane the runner really submitted with. The runner
// retries once on the class default when the site refuses a lane; without this the placement, the
// lane semaphore and the metric would keep claiming the lane. Jobs not placed in a lane are left
// alone, so nothing changes while no candidates are configured.
func (r *HPCArgoWorkflowJobReconciler) reflectLane(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	run *hpcv1alpha1.RunStatus) error {
	p := job.Status.Placement
	if p == nil || p.QOS == "" || run.QOS == "" || (run.QOS == p.QOS && run.Reservation == p.Reservation) {
		return nil
	}
	orig := job.DeepCopy()
	job.Status.Placement = p.DeepCopy()
	job.Status.Placement.RefusedQOS = p.QOS
	job.Status.Placement.QOS, job.Status.Placement.Reservation = run.QOS, run.Reservation
	if err := r.Status().Patch(ctx, job, client.MergeFrom(orig)); err != nil {
		return err
	}
	r.noteRefusedLane(job, orig.Status.Placement, run)
	return nil
}

// noteRefusedLane raises the event and the metric once per attempt, however often the cache lags.
func (r *HPCArgoWorkflowJobReconciler) noteRefusedLane(job *hpcv1alpha1.HPCArgoWorkflowJob,
	was *hpcv1alpha1.Placement, run *hpcv1alpha1.RunStatus) {
	r.mu.Lock()
	if r.laneNoted == nil {
		r.laneNoted = map[string]int32{}
	}
	done := r.laneNoted[string(job.UID)] == job.Status.Attempt
	r.laneNoted[string(job.UID)] = job.Status.Attempt
	r.mu.Unlock()
	if done {
		return
	}
	metrics.LanePlacements.WithLabelValues(run.QOS, run.Reservation, "true").Inc()
	if r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeNormal, "LaneRefused",
			"the site refused lane "+was.QOS+" "+was.Reservation+"; the attempt runs on QoS "+run.QOS)
	}
}
