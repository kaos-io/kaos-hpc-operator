package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

// DefaultSiteOutageMax is how long a job waits for an unreachable site before it fails with reason
// SiteUnavailable (PRD-HPC-1461 AC-28: a short outage is retried with backoff for up to 30 minutes).
const DefaultSiteOutageMax = 30 * time.Minute

// siteOutageMax is the bound in force.
func (r *HPCArgoWorkflowJobReconciler) siteOutageMax() time.Duration {
	if r.SiteOutageMax > 0 {
		return r.SiteOutageMax
	}
	return DefaultSiteOutageMax
}

// siteDown reports whether the job's site is unreachable now, and since when. Two sources: the poller's
// health store (REST probes and jobs/state reads), and the job's own runner, which reports SiteUnavailable as
// the pending reason while it waits for the site before it has submitted anything.
func (r *HPCArgoWorkflowJobReconciler) siteDown(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) (since time.Time, why string, down bool) {
	if out, ok := r.Health.Down(client.ObjectKeyFromObject(site).String()); ok {
		return out.Since, out.Reason, true
	}
	if job.Status.PendingReason == hpcv1alpha1.ReasonSiteUnavailable && (job.Status.Run == nil || job.Status.Run.SiteJobID == "") {
		return time.Time{}, "the runner cannot reach the site", true
	}
	return time.Time{}, "", false
}

// siteWait holds a serve-mode job that has not yet got a site job while its site is unreachable
// (PRD-HPC-1461 AC-30). held is true when the caller must stop: the job waits (phase Waiting before admission,
// Admitted after it, with reason SiteUnavailable), or it has just failed because the wait passed the
// outage bound. A job that already has a site job is never held: its runner and the poller handle that.
// runner says whether a runner Job already exists; then only the display is updated and the runner's own
// bound ends the attempt, so the controller never fails a job whose runner could still submit.
func (r *HPCArgoWorkflowJobReconciler) siteWait(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, runner bool) (held bool, err error) {
	if job.Spec.StepName == "" || (job.Status.Run != nil && current(job.Status.Run, job.Status.Attempt) && job.Status.Run.SiteJobID != "") {
		return false, r.endSiteWait(ctx, job)
	}
	since, why, down := r.siteDown(job, site)
	if !down {
		return false, r.endSiteWait(ctx, job)
	}
	began := r.now()
	if c := meta.FindStatusCondition(job.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable); c != nil && c.Status == metav1.ConditionTrue {
		began = c.LastTransitionTime.Time
	}
	max := r.siteOutageMax()
	if !runner && r.now().Sub(began) > max {
		return true, r.failSiteWait(ctx, job, site, max)
	}
	msg := siteWaitMessage(site, since, why, max)
	return !runner, r.markSiteWait(ctx, job, msg)
}

// siteWaitMessage is what the user reads: the site, since when, why, and how long the job waits.
func siteWaitMessage(site *hpcv1alpha1.HPCSite, since time.Time, why string, max time.Duration) string {
	m := "the HPC site " + site.Spec.Site + " is not answering"
	if !since.IsZero() {
		m += " (since " + since.UTC().Format("15:04:05 MST") + ")"
	}
	if why != "" {
		m += ": " + why
	}
	return truncate(m + "; the job waits and starts when the site answers (gives up after " + max.String() + ")")
}

// markSiteWait writes reason SiteUnavailable and the condition; the phase is Waiting for a job that is not
// admitted and stays Admitted for one that is. It writes nothing when nothing changed.
func (r *HPCArgoWorkflowJobReconciler) markSiteWait(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, msg string) error {
	phase := job.Status.Phase
	if job.Status.Booking == nil {
		phase = hpcv1alpha1.PhaseWaiting
	}
	c := meta.FindStatusCondition(job.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable)
	if job.Status.Phase == phase && job.Status.Reason == hpcv1alpha1.ReasonSiteUnavailable && job.Status.Message == msg &&
		c != nil && c.Status == metav1.ConditionTrue {
		return nil
	}
	orig := job.DeepCopy()
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, hpcv1alpha1.ReasonSiteUnavailable, msg
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: hpcv1alpha1.ConditionSiteUnavailable,
		Status: metav1.ConditionTrue, Reason: hpcv1alpha1.ReasonSiteUnavailable, Message: msg, ObservedGeneration: job.Generation})
	return r.Status().Patch(ctx, job, client.MergeFrom(orig))
}

// endSiteWait clears a wait that is over (the site answers, or the job got its site job). A job held after
// admission goes back to Admitted; a Waiting job is judged again by the caller and gets its own reason.
func (r *HPCArgoWorkflowJobReconciler) endSiteWait(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) error {
	if meta.FindStatusCondition(job.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable) == nil &&
		job.Status.Reason != hpcv1alpha1.ReasonSiteUnavailable {
		return nil
	}
	orig := job.DeepCopy()
	meta.RemoveStatusCondition(&job.Status.Conditions, hpcv1alpha1.ConditionSiteUnavailable)
	if job.Status.Reason == hpcv1alpha1.ReasonSiteUnavailable && job.Status.Phase == hpcv1alpha1.PhaseAdmitted {
		job.Status.Reason, job.Status.Message = hpcv1alpha1.ReasonAdmitted, ""
	}
	return r.Status().Patch(ctx, job, client.MergeFrom(orig))
}

// failSiteWait ends a job that waited longer than the bound: Failed with reason SiteUnavailable. A booked
// job is settled at zero and its booking released.
func (r *HPCArgoWorkflowJobReconciler) failSiteWait(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, max time.Duration) error {
	msg := fmt.Sprintf("the HPC site %s did not answer for more than %s; the job was not run", site.Spec.Site, max)
	if r.Recorder != nil {
		r.Recorder.Event(job, "Warning", hpcv1alpha1.ReasonSiteUnavailable, msg)
	}
	if err := r.endSiteWait(ctx, job); err != nil { // drops the condition in its own write
		return err
	}
	if job.Status.Booking != nil {
		return r.failAttempt(ctx, job, site, hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonSiteUnavailable,
			hpcv1alpha1.ReasonSiteUnavailable, msg)
	}
	return r.setPhase(ctx, job, hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonSiteUnavailable, msg, nil)
}

// noteSiteReachable sets the HPCSite's SiteReachable condition from the health store (AC-30, the console).
// It changes only the in-memory status; the caller's patch writes it. No health store: no condition.
func (r *HPCArgoWorkflowJobReconciler) noteSiteReachable(site *hpcv1alpha1.HPCSite) {
	if r.Health == nil {
		return
	}
	cond := metav1.Condition{Type: hpcv1alpha1.ConditionSiteReachable, Status: metav1.ConditionTrue,
		Reason: hpcv1alpha1.ReasonSiteAnswering, Message: "the site's REST API answers", ObservedGeneration: site.Generation}
	up := 1.0
	if out, down := r.Health.Down(client.ObjectKeyFromObject(site).String()); down {
		cond.Status, cond.Reason = metav1.ConditionFalse, hpcv1alpha1.ReasonSiteNotAnswering
		cond.Message = strings.TrimSpace("the site's REST API has not answered since " + out.Since.UTC().Format(time.RFC3339) + ": " + out.Reason)
		up = 0
	}
	metrics.SiteUp.WithLabelValues(site.Spec.Site, projectOf(site)).Set(up)
	meta.SetStatusCondition(&site.Status.Conditions, cond)
}

// PublishSite writes the site's status now (ledger, recent jobs, SiteReachable), within the publish throttle.
func (r *HPCArgoWorkflowJobReconciler) PublishSite(ctx context.Context, site *hpcv1alpha1.HPCSite) {
	if err := r.publishLedger(ctx, site); err != nil {
		logf.FromContext(ctx).Error(err, "publishing the site status", "site", client.ObjectKeyFromObject(site))
	}
}
