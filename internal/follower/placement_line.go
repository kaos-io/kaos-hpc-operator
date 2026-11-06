package follower

import (
	"fmt"
	"strings"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// laneStandard is the plain name of the class default lane.
const laneStandard = "standard"

// placementLines builds the human lines for the job's placement or waiting state, in plain words
// for a pipeline author. It returns nil when there is nothing to say yet. The lines are a pure
// function of the job, so a caller prints them again only when they differ from the last print.
//
// The status carries no node name and no start estimate today, so neither is shown.
func placementLines(job *hpcv1alpha1.HPCArgoWorkflowJob) []string {
	if end := endLines(job); end != nil {
		return end
	}
	if job.Status.Phase.Terminal() {
		return nil
	}
	run := job.Status.Run
	if run != nil && isStarted(run) {
		return runningLines(job)
	}
	if job.Status.Placement == nil && job.Status.PendingReason == "" && run == nil {
		return nil
	}
	parts := []string{"HPC: waiting", "lane " + laneOf(job.Status.Placement), timeLimitText(job)}
	if r := job.Status.PendingReason; r != "" {
		parts = append(parts, "queue reason: "+r)
	}
	return []string{strings.Join(parts, " · ")}
}

// isStarted reports whether the site job is running (or past running).
func isStarted(run *hpcv1alpha1.RunStatus) bool {
	return run.State == "RUNNING" || (run.StartTime != nil && run.State != "PENDING" && run.State != "")
}

// runningLines is the "it started" line, preceded by a notice when the site refused the lane.
func runningLines(job *hpcv1alpha1.HPCArgoWorkflowJob) []string {
	run := job.Status.Run
	p := job.Status.Placement
	lane := laneOf(p)
	var lines []string
	if p != nil && run.QOS != p.QOS {
		lane = laneOf(&hpcv1alpha1.Placement{QOS: run.QOS, Reservation: run.Reservation})
		lines = append(lines, "HPC: fast lane refused by the site; running on "+lane)
	}
	head := "HPC: running"
	if run.StartTime != nil {
		head += " since " + run.StartTime.UTC().Format("15:04") + " UTC"
	}
	return append(lines, strings.Join([]string{head, "lane " + lane, timeLimitText(job)}, " · "))
}

// endLines explains why an attempt ended or will be retried; nil when the reason is none of them.
func endLines(job *hpcv1alpha1.HPCArgoWorkflowJob) []string {
	switch job.Status.Reason {
	case hpcv1alpha1.ReasonIdle:
		lines := []string{fmt.Sprintf("HPC: stopped: no GPU use and no output for %d min (idle watchdog); last lines below",
			job.EffectiveIdleTimeoutMinutes())}
		if job.Status.Run != nil && job.Status.Run.LastLog != "" {
			lines = append(lines, strings.TrimRight(job.Status.Run.LastLog, "\n"))
		}
		return lines
	case hpcv1alpha1.ReasonTimeLimitRetry:
		return []string{"HPC: hit its learned time limit; retrying once with the full limit"}
	case hpcv1alpha1.ReasonWallTimeReached:
		return []string{"HPC: stopped at its time limit"}
	}
	return nil
}

// laneOf names the lane: reserved nodes, the standard lane (class default QoS), or the QoS name.
func laneOf(p *hpcv1alpha1.Placement) string {
	switch {
	case p == nil:
		return laneStandard
	case p.Reservation != "":
		return "dev (reserved nodes " + p.Reservation + ")"
	case p.QOS == "" || p.QOS == "default":
		return laneStandard
	}
	return p.QOS
}

// timeLimitText says the limit and where it came from.
func timeLimitText(job *hpcv1alpha1.HPCArgoWorkflowJob) string {
	limit := job.EffectiveWallMinutes()
	src := "declared"
	if p := job.Status.Placement; p != nil {
		switch p.Source {
		case hpcv1alpha1.PlacementLearned:
			runs := "runs"
			if p.Samples == 1 {
				runs = "run"
			}
			src = fmt.Sprintf("learned from %d %s", p.Samples, runs)
		case hpcv1alpha1.PlacementTimeoutRetry:
			src = "retry at the full limit after a timeout"
		}
	}
	return fmt.Sprintf("time limit %d min (%s)", limit, src)
}

// showPlacement prints the placement lines when they differ from the last print.
func (f *follower) showPlacement(job *hpcv1alpha1.HPCArgoWorkflowJob) {
	lines := placementLines(job)
	key := strings.Join(lines, "\n")
	if lines == nil || key == f.lastPlacement {
		return
	}
	f.lastPlacement = key
	for _, l := range lines {
		_, _ = fmt.Fprintln(f.o.Out, l)
	}
}
