package follower

import (
	"fmt"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// stagingLines says how the step's inputs were staged, one line per record of status.run.staged; none
// while the runner has not reported it (or the job stages its inputs itself). A line names the repository,
// the short commit and the size, never a path or a site identity: the record holds none.
func stagingLines(job *hpcv1alpha1.HPCArgoWorkflowJob) []string {
	if job.Status.Run == nil {
		return nil
	}
	var lines []string
	for _, s := range job.Status.Run.Staged {
		head := fmt.Sprintf("HPC: dataset %s@%s staged", s.Repo, s.Commit)
		switch s.Kind {
		case hpcv1alpha1.StagingCached:
			lines = append(lines, head+" (already cached)")
		case hpcv1alpha1.StagingDelta:
			lines = append(lines, fmt.Sprintf("%s (delta), %s, %ds", head, humanBytes(s.Bytes), s.Seconds))
		case hpcv1alpha1.StagingFirst:
			lines = append(lines, fmt.Sprintf("%s (first copy), %s, %ds", head, humanBytes(s.Bytes), s.Seconds))
		}
	}
	return lines
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

// showStaging prints each staging line once (a resubmit that finds a copy resident reports the same
// record, so its line is not repeated).
func (f *follower) showStaging(job *hpcv1alpha1.HPCArgoWorkflowJob) {
	for _, line := range stagingLines(job) {
		if f.shownStaging[line] {
			continue
		}
		if f.shownStaging == nil {
			f.shownStaging = map[string]bool{}
		}
		f.shownStaging[line] = true
		_, _ = fmt.Fprintln(f.o.Out, line)
	}
}
