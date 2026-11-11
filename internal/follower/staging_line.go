package follower

import (
	"fmt"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// stagingLine is the one line that says how the step's dataset was staged, from status.run.staging, or ""
// while the runner has not reported it (or the job stages the dataset itself). It names the repository, the
// short commit and the size, never a path or a site identity: the record holds none.
func stagingLine(job *hpcv1alpha1.HPCArgoWorkflowJob) string {
	if job.Status.Run == nil || job.Status.Run.Staging == nil {
		return ""
	}
	s := job.Status.Run.Staging
	head := fmt.Sprintf("HPC: dataset %s@%s staged", s.Repo, s.Commit)
	switch s.Kind {
	case hpcv1alpha1.StagingCached:
		return head + " (already cached)"
	case hpcv1alpha1.StagingDelta:
		return fmt.Sprintf("%s (delta), %s, %ds", head, humanBytes(s.Bytes), s.Seconds)
	case hpcv1alpha1.StagingFirst:
		return fmt.Sprintf("%s (first copy), %s, %ds", head, humanBytes(s.Bytes), s.Seconds)
	}
	return ""
}

// humanBytes is a size in decimal units ("1.4 GB"), the way the runner's log writes it.
func humanBytes(n int64) string {
	f := float64(n)
	for _, unit := range []string{"B", "kB", "MB", "GB", "TB"} {
		if f < 1000 || unit == "TB" {
			if unit == "B" {
				return fmt.Sprintf("%d B", n)
			}
			return fmt.Sprintf("%.1f %s", f, unit)
		}
		f /= 1000
	}
	return fmt.Sprintf("%d B", n)
}

// showStaging prints the staging line once per distinct text (a resubmit that finds the copy resident
// reports the same record, so the line is not repeated).
func (f *follower) showStaging(job *hpcv1alpha1.HPCArgoWorkflowJob) {
	line := stagingLine(job)
	if line == "" || line == f.lastStaging {
		return
	}
	f.lastStaging = line
	_, _ = fmt.Fprintln(f.o.Out, line)
}
