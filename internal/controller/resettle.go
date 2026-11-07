package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// lateEnd decides whether a host's runner has reported the end of the host Slurm JOB after the
// attempt was settled from its step's end (a shared-node host keeps its node, and the site keeps
// charging it, for a short while after its last step). It returns the index of the attempt record
// to correct and the new settlement, or ok=false when there is nothing to correct.
//
// It only ever grows a settlement and only from an end time that is valid and later than the one
// already recorded, so a repeated report changes nothing and an implausible one (NO_VAL, which
// settle() would turn into "now") can never extend the spend.
func lateEnd(job *hpcv1alpha1.HPCArgoWorkflowJob, now time.Time) (int, settlement, bool) {
	run := job.Status.Run
	n := len(job.Status.Attempts)
	if !job.Status.Phase.Terminal() || run == nil || !run.Terminal || run.HostSiteJobID != "" || n == 0 {
		return 0, settlement{}, false
	}
	rec := job.Status.Attempts[n-1]
	if run.Attempt != rec.Attempt || run.SiteJobID == "" || run.SiteJobID != rec.SiteJobID || run.EndTime == nil {
		return 0, settlement{}, false
	}
	st := settle(job.Spec.Nodes, run, now)
	if len(st.Discarded) > 0 || !st.End.After(rec.EndTime.Time) || st.Minutes <= rec.SettledNodeMinutes {
		return 0, settlement{}, false
	}
	return n - 1, st, true
}

// resettleHostEnd raises the settlement of an ended host attempt to its Slurm job's own start to
// end, once the runner reports that end. Called for terminal jobs only; a no-op in every other
// case. The job total is recomputed from the attempt records (never incremented), so repeating the
// same report cannot double-count, and the settled-minutes metric gets only the difference.
func (r *HPCArgoWorkflowJobReconciler) resettleHostEnd(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) error {
	i, st, ok := lateEnd(job, r.now())
	if !ok {
		return nil
	}
	orig := job.DeepCopy()
	rec := &job.Status.Attempts[i]
	delta := st.Minutes - rec.SettledNodeMinutes
	rec.SettledNodeMinutes, rec.EndTime = st.Minutes, st.End
	var total int64
	for _, a := range job.Status.Attempts {
		total += a.SettledNodeMinutes
	}
	job.Status.SettledNodeMinutes = &total
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("resettleHostEnd: %w", err)
	}
	if r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeNormal, "HostSettledToJobEnd", fmt.Sprintf(
			"attempt %d: the host Slurm job ended after its step; settled %d node-minutes (+%d)", rec.Attempt, st.Minutes, delta))
	}
	site, _, _, err := r.siteOf(ctx, job)
	if err == nil && site != nil {
		observeDelta(site, rec.Tier, delta)
	}
	return nil
}

// observeDelta adds extra settled node-minutes of an already counted attempt to the metric.
func observeDelta(site *hpcv1alpha1.HPCSite, tier hpcv1alpha1.Tier, delta int64) {
	observeSettled(site, tier, delta, nil, (*metav1.Time)(nil))
}
