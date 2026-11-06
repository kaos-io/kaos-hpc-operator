package controller

import (
	"context"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
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
	if job.Spec.StepName == "" || job.Spec.Resumable != nil {
		return &hpcv1alpha1.Placement{TimeLimitMinutes: caps.Spec, Source: hpcv1alpha1.PlacementDeclared}, nil
	}
	samples, err := r.samplesOf(ctx, job)
	if err != nil {
		return nil, err
	}
	limit, source := timelimit.Learn(samples, caps)
	return &hpcv1alpha1.Placement{TimeLimitMinutes: limit, Source: source, Samples: int32(len(samples))}, nil
}

// samplesOf returns the run times of the job's step in earlier jobs: same Key, Succeeded, never
// continued (a continued job's last segment is not the step's run time), with valid site times.
func (r *HPCArgoWorkflowJobReconciler) samplesOf(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) ([]time.Duration, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs, client.InNamespace(job.Namespace)); err != nil {
		return nil, err
	}
	key, now := timelimit.KeyOf(job), r.now()
	var out []time.Duration
	for i := range jobs.Items {
		o := &jobs.Items[i]
		if o.UID == job.UID || o.Spec.StepName == "" || timelimit.KeyOf(o) != key ||
			o.Status.Phase != hpcv1alpha1.PhaseSucceeded || o.Status.Continuations > 0 {
			continue
		}
		if d, ok := runTime(o.Status.Run, o.Status.Attempt, now); ok {
			out = append(out, d)
		}
	}
	return out, nil
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
