package controller

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/resume"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// resumableFor derives whether the job's step is verified resumable (D-74) from the evidence the
// continuations of the same step left on their jobs (same Key: namespace, step, class, image
// repository). The latest evidence decides, so a continuation that makes no progress clears an earlier
// verification. nil: no continuation has shown anything yet.
func (r *HPCArgoWorkflowJobReconciler) resumableFor(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (*hpcv1alpha1.ResumableStatus, error) {
	if job.Spec.StepName == "" {
		return nil, nil
	}
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs, client.InNamespace(job.Namespace)); err != nil {
		return nil, err
	}
	key := timelimit.KeyOf(job)
	evidence := []*hpcv1alpha1.ResumeEvidence{job.Status.ResumeEvidence}
	for i := range jobs.Items {
		o := &jobs.Items[i]
		if o.UID != job.UID && o.Spec.StepName != "" && timelimit.KeyOf(o) == key {
			evidence = append(evidence, o.Status.ResumeEvidence)
		}
	}
	return resume.Status(evidence), nil
}

// checkpointUsage computes the scratch the site's step checkpoints hold to the HPCSite status.
func (r *HPCArgoWorkflowJobReconciler) checkpointUsage(ctx context.Context, site *hpcv1alpha1.HPCSite) (hpcv1alpha1.CheckpointUsage, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := r.List(ctx, &jobs); err != nil {
		return hpcv1alpha1.CheckpointUsage{}, err
	}
	owns := func(j *hpcv1alpha1.HPCArgoWorkflowJob) bool {
		s := j.Status.Site
		return s != nil && s.Name == site.Name && s.Namespace == site.Namespace
	}
	return resume.Usage(jobs.Items, r.now(), owns), nil
}
