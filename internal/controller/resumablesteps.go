package controller

import (
	"sort"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/resume"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// resumableStepsOf lists what the site's jobs recorded about their steps' resumability (D-74): per
// app and step the latest continuation evidence. The hub copies it to the app's platform context so
// the pipeline linter judges "is this step resumable" from what happened, not from the step's imports.
// Only names, a verdict, a time and a reason leave the pool. nil when no step has evidence.
func resumableStepsOf(site *hpcv1alpha1.HPCSite, jobs []hpcv1alpha1.HPCArgoWorkflowJob,
	projectNS map[string]bool) []hpcv1alpha1.ResumableStep {
	evidence := map[[2]string][]*hpcv1alpha1.ResumeEvidence{}
	for i := range jobs {
		j := &jobs[i]
		app := timelimit.AppOf(j.Spec.Image)
		if j.Spec.StepName == "" || app == "" || j.Status.ResumeEvidence == nil || !jobOfSite(j, site, projectNS) {
			continue
		}
		key := [2]string{app, j.Spec.StepName}
		evidence[key] = append(evidence[key], j.Status.ResumeEvidence)
	}
	out := make([]hpcv1alpha1.ResumableStep, 0, len(evidence))
	for key, ev := range evidence {
		// resume.Status picks the latest evidence, so a later failure clears an earlier verification.
		if st := resume.Status(ev); st != nil {
			out = append(out, hpcv1alpha1.ResumableStep{App: key[0], Step: key[1], Verified: st.Verified,
				Since: st.Since, Reason: st.Reason})
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].App != out[b].App {
			return out[a].App < out[b].App
		}
		return out[a].Step < out[b].Step
	})
	if len(out) > hpcv1alpha1.ResumableStepsMax {
		out = out[:hpcv1alpha1.ResumableStepsMax]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
