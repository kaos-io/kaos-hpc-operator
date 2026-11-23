package controller

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// Time of one job (PRD kaos#1911 D-75). The spec's wallTimeMinutes is the platform's cap on the step's total
// time. A job asks for the most a job can get (the lowest of the class's, the site's and the data access's
// maximum), unless the step has a learned limit (F-41, D-67): that comes from the run times of the same step's
// earlier successful jobs still in the cluster (no store of its own). The jobs of a step together never exceed
// the cap. The choice is written to status.placement.

// capsOf are the bounds of a job's time limit on a site.
func capsOf(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) timelimit.Caps {
	c := timelimit.Caps{Spec: job.Spec.WallTimeMinutes, Site: site.Spec.Limits.MaxWallTimeMinutes, Token: timelimit.TokenCoverMinutes}
	if class, ok := admission.ClassOf(site, job.Spec.Class); ok {
		c.Class = class.MaxWallTimeMinutes
	}
	return c
}

// placementFor chooses the time limit of the job's next attempt. A job that already got its one
// timeout retry keeps that limit. Only serve-mode jobs learn (the runner Job carries the limit there).
func (r *HPCArgoWorkflowJobReconciler) placementFor(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (*hpcv1alpha1.Placement, error) {
	p, err := r.placementUncapped(ctx, job, site)
	if err != nil {
		return nil, err
	}
	return capAtRemaining(p, capsOf(job, site), usedMinutes(job)), nil
}

// capAtRemaining gives a job no more than what the earlier jobs (used minutes) left of the cap on the step's total
// time (timelimit.Segment): a step that needs more jobs than one runs as continuations, each a new site job
// with its own fresh access, and the jobs together stay within the cap.
func capAtRemaining(p *hpcv1alpha1.Placement, caps timelimit.Caps, used int64) *hpcv1alpha1.Placement {
	p.TimeLimitMinutes, _ = timelimit.Segment(caps.Spec, p.TimeLimitMinutes, int32(min(used, math.MaxInt32)))
	return p
}

// defaultQOS names the QoS of a class that names none.
const defaultQOS = "default"

// usedMinutes is the time the job's earlier attempts ran (their settled node-minutes; a job runs on one node).
// A guest on a shared node settles to zero, so its runs are not counted.
func usedMinutes(job *hpcv1alpha1.HPCArgoWorkflowJob) int64 {
	var total int64
	for _, a := range job.Status.Attempts {
		total += a.SettledNodeMinutes
	}
	return total
}

// capSpent: the earlier jobs of the step used the whole cap on its total time, so no further job is started.
func capSpent(job *hpcv1alpha1.HPCArgoWorkflowJob) bool {
	return usedMinutes(job) >= int64(job.Spec.WallTimeMinutes)
}

// placementUncapped is placementFor before the cap on the step's total time is applied.
func (r *HPCArgoWorkflowJobReconciler) placementUncapped(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) (*hpcv1alpha1.Placement, error) {
	if p := job.Status.Placement; p != nil && p.Source == hpcv1alpha1.PlacementTimeoutRetry {
		return p, nil
	}
	caps := capsOf(job, site)
	rs, err := r.resumableFor(ctx, job)
	if err != nil {
		return nil, err
	}
	if job.Spec.StepName == "" {
		return &hpcv1alpha1.Placement{TimeLimitMinutes: caps.Spec, Source: hpcv1alpha1.PlacementDeclared}, nil
	}
	history, err := r.historyOf(ctx, job)
	if err != nil {
		return nil, err
	}
	samples := samplesOf(history, r.now())
	limit, source := timelimit.Learn(samples, caps)
	return &hpcv1alpha1.Placement{TimeLimitMinutes: limit, Source: source, Samples: int32(len(samples)), Resumable: rs}, nil
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
// are valid (the rules of settle: set, after 2000, not in the future) and the run took time (end after start).
func runTime(run *hpcv1alpha1.RunStatus, attempt int32, now time.Time) (time.Duration, bool) {
	if run == nil || !current(run, attempt) || !validSiteTime(run.StartTime, now) || !validSiteTime(run.EndTime, now) {
		return 0, false
	}
	// A run of no length (start == end, both bogus or sub-second) is not a sample: three of them would
	// teach the 15-minute floor to a step that runs for hours.
	if d := run.EndTime.Sub(run.StartTime.Time); d > 0 {
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
	caps := capsOf(job, site)
	limit, _ := timelimit.Segment(caps.Spec, timelimit.Retry(caps), int32(min(usedMinutes(job), math.MaxInt32)))
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
	return r.laneFor(ctx, job, site, pl, pl.TimeLimitMinutes)
}

// laneFor is withLane for a lane that must admit limit minutes: the step's own limit, or more where the job
// asks for room beside it (a shared-node host, share.go).
func (r *HPCArgoWorkflowJobReconciler) laneFor(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, pl *hpcv1alpha1.Placement, limit int32) (*hpcv1alpha1.Placement, error) {
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
		LimitMinutes: limit, Now: r.now(), InUse: inUse})
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
		// A host whose Slurm job still holds the node after its step (D-26) still occupies its lane.
		if o.UID != job.UID && o.Status.Booking != nil && (!o.Status.Phase.Terminal() || ledger.OpenHost(o)) {
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
		qos = defaultQOS
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
