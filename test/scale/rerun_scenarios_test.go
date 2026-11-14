//go:build scale

package scale

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

// PRD kaos#1911 AC-13 (S12): many steps at once, each with its own random ending. Every step reaches a final
// state, none goes past its budget of re-runs and continuations, the fake site keeps no live job, and the whole
// run takes a bounded time. The seed is fixed: the same endings every run.
func TestScaleRerunEndings(t *testing.T) {
	r := newResult(t, "AC-13 re-run endings", "50 steps with random endings (success after 2-4 segments, node failure, "+
		"pre-emption, boot failure, out of memory, own error, 8 segments against the cap of 5), fixed seed.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 4, restBurst: 10})
	site := fakeslurm.New(siteOptions("meluxina", 7))
	p := w.addProject("pr", "meluxina", "u1", "acct1", site,
		budget{gpuShare: 4000, cpuShare: 4000, gpuSiteLeft: 3500, cpuSiteLeft: 3500}, 60)
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()

	type ending struct {
		name  string
		end   string
		segs  int
		check func(j *hpcv1alpha1.HPCArgoWorkflowJob) string
	}
	endings := []ending{
		{"segments_then_success", "", 3, func(j *hpcv1alpha1.HPCArgoWorkflowJob) string {
			if j.Status.Phase != hpcv1alpha1.PhaseSucceeded || j.Status.Continuations != 2 {
				return fmt.Sprintf("want Succeeded after 2 continuations, got %s/%s with %d", j.Status.Phase, j.Status.Reason, j.Status.Continuations)
			}
			return ""
		}},
		{"node_fail", fakeslurm.NodeFail, 0, rerunLimit},
		{"preempted", fakeslurm.Preempted, 0, rerunLimit},
		{"boot_fail", "BOOT_FAIL", 0, rerunLimit},
		{"out_of_memory", "OUT_OF_MEMORY", 0, neverRerun},
		{"own_error", fakeslurm.Failed, 0, neverRerun},
		{"too_many_segments", "", 8, func(j *hpcv1alpha1.HPCArgoWorkflowJob) string {
			if j.Status.Phase != hpcv1alpha1.PhaseFailed || j.Status.Reason != hpcv1alpha1.ReasonContinuationLimit || j.Status.Continuations != 5 {
				return fmt.Sprintf("want Failed/ContinuationLimit after 5 continuations, got %s/%s with %d", j.Status.Phase, j.Status.Reason, j.Status.Continuations)
			}
			return ""
		}},
	}
	const n = 50
	rng := rand.New(rand.NewSource(20261114))
	kind := map[string]ending{}
	specs := make([]jobSpec, n)
	picks := make([]ending, n)
	for i := range specs {
		e := endings[rng.Intn(len(endings))]
		picks[i] = e
		specs[i] = jobSpec{class: "hpc-gpu", wallMinutes: 45, run: "5", forceEnd: e.end}
		if e.segs > 0 {
			specs[i].resumable, specs[i].segments = true, e.segs
		}
	}
	start := time.Now()
	w.createJobs(p, specs)
	for i, name := range p.jobs {
		kind[p.envNS+"/"+name] = picks[i]
	}
	ok := w.waitTerminal(n, 20*time.Minute, func(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
		t.Logf("%s %v", time.Since(start).Round(time.Second), histogram(jobs))
	})
	dur := time.Since(start)
	jobs := w.allJobs()
	r.row("duration (create to last terminal)", dur.Round(time.Second))
	r.row("final phases", histogram(jobs))
	if !ok {
		r.fail("not every step reached a final state: %v", histogram(jobs))
	}
	const bound = 1 + int(hpcv1alpha1.MaxReruns) + 5 + 1
	for i := range jobs {
		j := &jobs[i]
		e := kind[j.Namespace+"/"+j.Name]
		if j.Status.Reruns > hpcv1alpha1.MaxReruns || j.Status.Continuations > 5 || len(j.Status.Attempts) > bound {
			r.fail("%s (%s): %d re-runs, %d continuations, %d attempts: past the budget", j.Name, e.name, j.Status.Reruns, j.Status.Continuations, len(j.Status.Attempts))
		}
		if e.check != nil {
			if msg := e.check(j); msg != "" {
				r.fail("%s (%s): %s", j.Name, e.name, msg)
			}
		}
	}
	// No leaked job: after the last step ended, the site holds nothing that is still queued or running.
	deadline := time.Now().Add(30 * time.Second)
	var live int
	for time.Now().Before(deadline) {
		st := site.States()
		live = st[fakeslurm.Pending] + st[fakeslurm.Running]
		if live == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	r.row("site jobs still pending or running after the last step ended", live)
	if live != 0 {
		r.fail("%d site jobs leaked (still pending or running)", live)
	}
	w.verifySubmits(r, jobs)
}

func rerunLimit(j *hpcv1alpha1.HPCArgoWorkflowJob) string {
	if j.Status.Phase != hpcv1alpha1.PhaseFailed || j.Status.Reason != hpcv1alpha1.ReasonRerunLimit || j.Status.Reruns != hpcv1alpha1.MaxReruns {
		return fmt.Sprintf("want Failed/RerunLimit after %d re-runs, got %s/%s with %d", hpcv1alpha1.MaxReruns, j.Status.Phase, j.Status.Reason, j.Status.Reruns)
	}
	return ""
}

func neverRerun(j *hpcv1alpha1.HPCArgoWorkflowJob) string {
	if j.Status.Phase != hpcv1alpha1.PhaseFailed || j.Status.Reruns != 0 || j.Status.Continuations != 0 || len(j.Status.Attempts) != 1 {
		return fmt.Sprintf("want Failed after one attempt, got %s/%s with %d re-runs and %d attempts", j.Status.Phase, j.Status.Reason, j.Status.Reruns, len(j.Status.Attempts))
	}
	return ""
}
