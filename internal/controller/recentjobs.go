package controller

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
)

// siteStateRunning is Slurm's state of a job that holds its nodes.
const siteStateRunning = "RUNNING"

// Recent jobs (PRD-HPC-1461 F-43, AC-76): the console reads a bounded list of the project's jobs
// from HPCSite.status, because the hub holds no pool kubeconfig. It is built from the jobs the
// ledger already read, so it costs no site call.

// recentJobsOf returns the site's project jobs that are active, or ended within RecentJobsWindow,
// active first, newest first within each group, ties broken by name so the list is deterministic,
// cut at RecentJobsMax entries.
func (r *HPCArgoWorkflowJobReconciler) recentJobsOf(ctx context.Context, site *hpcv1alpha1.HPCSite,
	jobs []hpcv1alpha1.HPCArgoWorkflowJob) ([]hpcv1alpha1.RecentJob, error) {
	projectNS, err := r.projectNamespaces(ctx, site)
	if err != nil {
		return nil, err
	}
	now := r.now()
	var mine []*hpcv1alpha1.HPCArgoWorkflowJob
	for i := range jobs {
		if jobOfSite(&jobs[i], site, projectNS) && inRecentWindow(&jobs[i], now) {
			mine = append(mine, &jobs[i])
		}
	}
	sort.Slice(mine, func(a, b int) bool {
		ja, jb := mine[a], mine[b]
		if aa, ab := !ja.Status.Phase.Terminal(), !jb.Status.Phase.Terminal(); aa != ab {
			return aa
		}
		if !ja.CreationTimestamp.Equal(&jb.CreationTimestamp) {
			return jb.CreationTimestamp.Before(&ja.CreationTimestamp)
		}
		return ja.Namespace+"/"+ja.Name < jb.Namespace+"/"+jb.Name
	})
	if len(mine) > hpcv1alpha1.RecentJobsMax {
		mine = mine[:hpcv1alpha1.RecentJobsMax]
	}
	out := make([]hpcv1alpha1.RecentJob, 0, len(mine))
	for _, j := range mine {
		out = append(out, recentJobOf(j, now))
	}
	if len(out) == 0 {
		return nil, nil
	}
	return capRecentBytes(out), nil
}

// capRecentBytes drops the tail (the oldest ended jobs) until the JSON of the list fits
// RecentJobsMaxBytes, so a list of worst-case entries can never grow the status past it.
func capRecentBytes(list []hpcv1alpha1.RecentJob) []hpcv1alpha1.RecentJob {
	total := 2 // the brackets
	for i := range list {
		raw, err := json.Marshal(&list[i])
		if err != nil { // cannot happen for these plain fields; keep the list rather than lose it
			return list
		}
		if total += len(raw) + 1; total > hpcv1alpha1.RecentJobsMaxBytes {
			return list[:i]
		}
	}
	return list
}

// inRecentWindow keeps every active job, and an ended one for RecentJobsWindow after it ended.
func inRecentWindow(j *hpcv1alpha1.HPCArgoWorkflowJob, now time.Time) bool {
	if !j.Status.Phase.Terminal() {
		return true
	}
	return now.Sub(endedAt(j)) <= hpcv1alpha1.RecentJobsWindow
}

// endedAt is when an ended job ended: the site's end time of its current run, else the end of its
// last settled attempt, else (refused, or cancelled before any site job) its creation.
func endedAt(j *hpcv1alpha1.HPCArgoWorkflowJob) time.Time {
	if run := j.Status.Run; run != nil && run.EndTime != nil && !run.EndTime.IsZero() {
		return run.EndTime.Time
	}
	if n := len(j.Status.Attempts); n > 0 {
		return j.Status.Attempts[n-1].EndTime.Time
	}
	return j.CreationTimestamp.Time
}

// truncateMessage cuts s to RecentJobMessageMax characters (never inside a UTF-8 sequence).
func truncateMessage(s string) string {
	if r := []rune(s); len(r) > hpcv1alpha1.RecentJobMessageMax {
		return string(r[:hpcv1alpha1.RecentJobMessageMax])
	}
	return s
}

// projectNamespaces are the namespaces labelled with the site's project; a job that was never
// admitted (Waiting, Pending) names no site yet, so its namespace decides.
func (r *HPCArgoWorkflowJobReconciler) projectNamespaces(ctx context.Context, site *hpcv1alpha1.HPCSite) (map[string]bool, error) {
	project := projectOf(site)
	if project == "" {
		return nil, nil
	}
	var nss corev1.NamespaceList
	if err := r.List(ctx, &nss, client.MatchingLabels{hpcv1alpha1.ProjectLabel: project}); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(nss.Items))
	for i := range nss.Items {
		out[nss.Items[i].Name] = true
	}
	return out, nil
}

// jobOfSite reports whether the job belongs on this site's list: bound to it by booking or site
// reference, or not bound yet, in a namespace of the site's project, and of a class the site offers.
func jobOfSite(j *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, projectNS map[string]bool) bool {
	if b := j.Status.Booking; b != nil {
		return b.SiteName == site.Name && b.SiteNamespace == site.Namespace
	}
	if s := j.Status.Site; s != nil {
		return s.Name == site.Name && s.Namespace == site.Namespace
	}
	if !projectNS[j.Namespace] {
		return false
	}
	_, ok := admission.ClassOf(site, j.Spec.Class)
	return ok
}

// recentJobOf copies the console-relevant fields of one job.
func recentJobOf(j *hpcv1alpha1.HPCArgoWorkflowJob, now time.Time) hpcv1alpha1.RecentJob {
	st := &j.Status
	rj := hpcv1alpha1.RecentJob{
		Name: j.Name, Namespace: j.Namespace, Step: j.Spec.StepName,
		Phase: st.Phase, Reason: st.Reason, Message: truncateMessage(st.Message), Attempt: st.Attempt,
		CreatedAt:          j.CreationTimestamp,
		IdleTimeoutMinutes: j.EffectiveIdleTimeoutMinutes(),
		TimeLimitMinutes:   j.EffectiveWallMinutes(),
	}
	if w := j.Spec.Workflow; w != nil {
		rj.Workflow = w.Name
	}
	if p := st.Placement; p != nil {
		rj.QOS, rj.Reservation, rj.RefusedQOS = p.QOS, p.Reservation, p.RefusedQOS
		rj.LimitSource, rj.LimitSamples = p.Source, p.Samples
	}
	if run := st.Run; run != nil {
		rj.SiteState, rj.SiteJobID, rj.HostSiteJobID = run.State, run.SiteJobID, run.HostSiteJobID
		rj.StartTime, rj.EndTime = siteTime(run.StartTime, now), siteTime(run.EndTime, now)
		if run.QOS != "" { // what the site job was actually submitted with
			rj.QOS, rj.Reservation = run.QOS, run.Reservation
		}
	}
	if !st.Phase.Terminal() && rj.SiteState != siteStateRunning && rj.HostSiteJobID == "" {
		raw := pendingReasonOf(st)
		rj.PendingReason = hpcv1alpha1.DisplayPendingReason(raw)
		if hpcv1alpha1.IsSiteLimitPendingReason(raw) {
			// The site's monthly account limit holds this job in the site's queue (AC-23): its own reason,
			// not the generic "Submitted".
			rj.Reason = hpcv1alpha1.ReasonSiteLimit
		}
	}
	rj.BookedNodeMinutes = bookedOf(j)
	for _, a := range st.Attempts {
		rj.SettledNodeMinutes += max(a.SettledNodeMinutes, 0)
	}
	return rj
}

// pendingReasonOf is the scheduler's pending reason, preferring the controller's batched reading.
func pendingReasonOf(st *hpcv1alpha1.HPCArgoWorkflowJobStatus) string {
	if o := st.SiteObservation; o != nil && o.PendingReason != "" {
		return o.PendingReason
	}
	return st.PendingReason
}

// bookedOf is the booking the job holds; for a job without one, its limit times nodes.
func bookedOf(j *hpcv1alpha1.HPCArgoWorkflowJob) int64 {
	if b := j.Status.Booking; b != nil {
		return b.NodeMinutes
	}
	return int64(j.EffectiveWallMinutes()) * int64(max(j.Spec.Nodes, 1))
}

// siteTime copies a site-reported time for the console, or nil when the site reported none it can
// use (validSiteTime: Slurm's NO_VAL for a never-started job is year 2106 and must never show as a start).
func siteTime(t *metav1.Time, now time.Time) *metav1.Time {
	if !validSiteTime(t, now) {
		return nil
	}
	c := *t
	return &c
}
