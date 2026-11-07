//go:build scale

package scale

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

// result is one scenario's numbers, written to REPORT.md.
type result struct {
	name, desc string
	t          *testing.T
	mu         sync.Mutex // the chaos goroutine and the test both write
	rows       [][2]string
	failures   []string
}

func (r *result) row(k string, v any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, [2]string{k, fmt.Sprint(v)})
}

// fail records a regression: the test fails and the report says so.
func (r *result) fail(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	r.mu.Lock()
	r.failures = append(r.failures, msg)
	r.mu.Unlock()
	r.t.Error(msg)
}

// verifySubmits asserts that no attempt was submitted twice and that every job was submitted exactly
// as many times as it has attempts that reached the site. Returns (submits, jobs).
func (w *world) verifySubmits(r *result, jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
	perName := map[string]int{}
	total, dup := 0, 0
	seen := map[*fakeslurm.Site]bool{}
	for _, p := range w.projects {
		if seen[p.site] {
			continue
		}
		seen[p.site] = true
		for k, n := range p.site.Snapshot().Submits {
			total += n
			if n > 1 {
				dup++
				r.fail("duplicate site submission: %s submitted %d times", k, n)
			}
			name, _, _ := strings.Cut(k, "#")
			perName[name] += n
		}
	}
	mismatch := 0
	for i := range jobs {
		j := &jobs[i]
		attempts := 0
		for _, a := range j.Status.Attempts {
			if a.SiteJobID != "" {
				attempts++
			}
		}
		if got := perName[siteName(j)]; got != attempts && mismatch < 5 {
			mismatch++
			r.fail("job %s/%s: %d site submissions but %d attempts reached the site", j.Namespace, j.Name, got, attempts)
		}
	}
	r.row("site submissions (all attempts)", total)
	r.row("jobs", len(jobs))
	r.row("duplicate submissions (same job, same attempt)", dup)
}

// verifyRateLimit asserts that the jobs/state calls to each fake site never exceeded the token bucket
// that the controller shares across the HPCSites of one site profile: in any window w, at most
// burst + rate*w calls (plus one for the call in flight at the edge).
func (w *world) verifyRateLimit(r *result) {
	seen := map[*fakeslurm.Site]bool{}
	for _, p := range w.projects {
		if seen[p.site] {
			continue
		}
		seen[p.site] = true
		ts := p.site.Snapshot().StateCallTimes
		worst := 0.0
		for _, win := range []time.Duration{time.Second, 5 * time.Second, 30 * time.Second} {
			limit := float64(w.opts.restBurst) + w.opts.restRate*win.Seconds() + 1
			j := 0
			for i := range ts {
				for ts[i].Sub(ts[j]) > win {
					j++
				}
				n := float64(i - j + 1)
				worst = max(worst, n/limit)
				if n > limit {
					r.fail("site %s: %0.f jobs/state calls within %s, the bucket allows %.0f", p.siteName, n, win, limit)
					break
				}
			}
		}
		r.row("rate limit "+p.siteName+": busiest window as share of the bucket", fmt.Sprintf("%.0f%%", worst*100))
	}
}

// siteCallRows reports the site traffic and asserts the batched poll is O(sites), not O(jobs):
// calls per poll cycle per HPCSite stay at ceil(active/chunk), not at the number of active jobs.
// storm: the run injected 422s, whose one-by-one fallback legitimately costs more calls.
func (w *world) siteCallRows(r *result, dur time.Duration, storm bool) {
	seen := map[*fakeslurm.Site]bool{}
	var calls, ids int
	hpcSites := 0
	for _, p := range w.projects {
		hpcSites++
		if seen[p.site] {
			continue
		}
		seen[p.site] = true
		st := p.site.Snapshot()
		calls += st.Calls["state"]
		ids += st.StateIDs
		var parts []string
		for _, k := range sortedKeys(st.Calls) {
			parts = append(parts, fmt.Sprintf("%s=%d", k, st.Calls[k]))
		}
		r.row("site calls "+p.siteName, strings.Join(parts, " ")+fmt.Sprintf(" (peak running %d, peak active %d, 422s %d, injected faults %d)",
			st.PeakRunning, st.PeakActive, st.State422, st.Injected))
	}
	polls := math.Ceil(dur.Seconds() / w.opts.pollInterval.Seconds())
	perPoll := float64(calls) / polls / float64(hpcSites)
	r.row("jobs/state calls", calls)
	r.row("job IDs asked for in those calls (= calls a per-job poll would make)", ids)
	r.row("batching factor (IDs per call)", fmt.Sprintf("%.1f", float64(ids)/math.Max(float64(calls), 1)))
	r.row("jobs/state calls per poll cycle per HPCSite (avg)", fmt.Sprintf("%.2f", perPoll))
	w.kube.mu.Lock()
	r.row("runner submit calls / adoptions of a site job found by key", fmt.Sprintf("%d / %d", w.kube.submits, w.kube.adopts))
	w.kube.mu.Unlock()
	r.row("runner site reads on a stale observation (fallback)", w.kube.restFall)
	w.mon.mu.Lock()
	for _, ns := range sortedKeys(w.mon.obsSeen) {
		r.row("observation age of live jobs in "+ns+": oldest / share older than 3 poll intervals", fmt.Sprintf("%s / %.1f%%",
			w.mon.obsPeak[ns].Round(time.Second), 100*float64(w.mon.obsStale[ns])/math.Max(float64(w.mon.obsSeen[ns]), 1)))
	}
	w.mon.mu.Unlock()
	if !storm && float64(ids) > 0 {
		// Each call carries up to the chunk size; a per-job poll would need one call per ID.
		if float64(calls) > float64(ids)/5 && ids > 200 {
			r.fail("jobs/state is not batched: %d calls for %d IDs", calls, ids)
		}
		if limit := float64(w.maxActivePerSite()/sitestate.DefaultChunk + 2); perPoll > limit {
			r.fail("jobs/state calls per poll per HPCSite %.2f exceed ceil(active/%d)+1 = %.0f", perPoll, sitestate.DefaultChunk, limit)
		}
	}
}

func (w *world) maxActivePerSite() int {
	w.mon.mu.Lock()
	defer w.mon.mu.Unlock()
	return w.mon.maxActive
}

// memRows reports and asserts the controller's memory: flat after warmup. The run is cut in thirds
// after the first quarter; the last third must not exceed the middle one by more than 30 % + 30 MB.
func (w *world) memRows(r *result, from time.Time, memCeilingMB float64) {
	ss := w.sampler.all()
	var run []sample
	for _, s := range ss {
		if !s.at.Before(from) {
			run = append(run, s)
		}
	}
	if len(run) < 9 {
		r.row("memory", "too few samples")
		return
	}
	warm := len(run) / 4
	rest := run[warm:]
	mid, last := rest[:len(rest)/2], rest[len(rest)/2:]
	all, mm, ml := memOf(run), memOf(mid), memOf(last)
	r.row("controller RSS MB (avg / max over run)", fmt.Sprintf("%.0f / %.0f", all.rssAvg, all.rssMax))
	r.row("controller heap in use MB (avg / max over run)", fmt.Sprintf("%.0f / %.0f", all.heapAvg, all.heapMax))
	r.row("RSS MB: first half / second half after warmup", fmt.Sprintf("%.0f / %.0f", mm.rssAvg, ml.rssAvg))
	r.row("heap MB: first half / second half after warmup", fmt.Sprintf("%.0f / %.0f", mm.heapAvg, ml.heapAvg))
	gs := 0.0
	for _, s := range run {
		gs = max(gs, s.m["go_goroutines"])
	}
	r.row("controller goroutines (max)", int(gs))
	if ml.rssAvg > 1.3*mm.rssAvg+30 {
		r.fail("controller memory grows after warmup: RSS %.0f MB -> %.0f MB", mm.rssAvg, ml.rssAvg)
	}
	if all.rssMax > memCeilingMB {
		r.fail("controller RSS peaked at %.0f MB, above the %.0f MB guard", all.rssMax, memCeilingMB)
	}
}

// reconcileRows reports reconcile throughput, latency, queue depth and API traffic of the controller.
func (w *world) reconcileRows(r *result, from time.Time, dur time.Duration) {
	var run []sample
	for _, s := range w.sampler.all() {
		if !s.at.Before(from) {
			run = append(run, s)
		}
	}
	recs := counterDelta(run, "controller_runtime_reconcile_total")
	sum, cnt := counterDelta(run, "controller_runtime_reconcile_time_seconds_sum"), counterDelta(run, "controller_runtime_reconcile_time_seconds_count")
	depth, depthLate := 0.0, 0.0
	for _, s := range run {
		depth = max(depth, s.m["workqueue_depth"])
		if s.at.Sub(from) > time.Minute { // after the burst of creating the jobs
			depthLate = max(depthLate, s.m["workqueue_depth"])
		}
	}
	r.row("reconciles (total / per second)", fmt.Sprintf("%.0f / %.1f", recs, recs/dur.Seconds()))
	r.row("reconcile errors", fmt.Sprintf("%.0f", counterDelta(run, "controller_runtime_reconcile_errors_total")))
	if cnt > 0 {
		r.row("reconcile duration avg (ms)", fmt.Sprintf("%.1f", sum/cnt*1000))
	}
	r.row("workqueue depth (max / max after the first minute)", fmt.Sprintf("%d / %d", int(depth), int(depthLate)))
	if cnt > 0 {
		// One worker reconciles jobs (MaxConcurrentReconciles 1): this is how busy it was.
		util := recs / dur.Seconds() * sum / cnt
		r.row("reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated)", fmt.Sprintf("%.2f", util))
		if util > 0.9 && dur > time.Minute {
			r.fail("the single reconcile worker was %.0f%% busy: the controller is at its capacity", util*100)
		}
	}
	r.row("controller API requests (total / per second)", fmt.Sprintf("%.0f / %.1f",
		counterDelta(run, "rest_client_requests_total"), counterDelta(run, "rest_client_requests_total")/dur.Seconds()))
	r.row("controller API PATCHes", fmt.Sprintf("%.0f", counterDelta(run, "rest_client_requests_total|PATCH")))
}

// siteStatusRows reports how often each HPCSite status was really written and the size of the object.
func (w *world) siteStatusRows(r *result, dur time.Duration, maxPerSec int) {
	total, peak := w.mon.writesPerSite()
	for _, ns := range sortedKeys(total) {
		r.row("HPCSite "+ns+" writes (total / avg per s / busiest second)",
			fmt.Sprintf("%d / %.2f / %d", total[ns], float64(total[ns])/dur.Seconds(), peak[ns]))
		if peak[ns] > maxPerSec {
			r.fail("HPCSite %s was written %d times in one second (bound %d)", ns, peak[ns], maxPerSec)
		}
	}
	r.row("recentJobs entries (max) / HPCSite object size (max bytes)", fmt.Sprintf("%d / %d", w.mon.maxRecent, w.mon.maxSiteB))
	if w.mon.maxRecent > hpcv1alpha1.RecentJobsMax {
		r.fail("recentJobs reached %d entries (bound %d)", w.mon.maxRecent, hpcv1alpha1.RecentJobsMax)
	}
	if w.mon.maxSiteB > 128<<10 {
		r.fail("HPCSite object grew to %d bytes", w.mon.maxSiteB)
	}
}

// budgetRows asserts the budget invariants, collected on every snapshot, and the site-side truth: the
// fake site's own accounting never consumed more than min(share, siteLeft).
func (w *world) budgetRows(r *result, jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
	w.mon.mu.Lock()
	viol := append([]string(nil), w.mon.violations...)
	snaps := w.mon.snapshots
	peaks := make([]string, 0, len(w.mon.peakRatio))
	for _, k := range sortedKeys(w.mon.peakRatio) {
		peaks = append(peaks, fmt.Sprintf("%s %.0f%%", k, w.mon.peakRatio[k]*100))
	}
	w.mon.mu.Unlock()
	r.row("budget invariant checks (snapshots)", snaps)
	r.row("peak (used+booked) / min(share, siteLeft)", strings.Join(peaks, ", "))
	for _, v := range viol {
		r.fail("budget invariant violated: %s", v)
	}
	sort.Slice(w.projects, func(a, b int) bool { return w.projects[a].name < w.projects[b].name })
	for _, p := range w.projects {
		var hs hpcv1alpha1.HPCSite
		if err := w.c.Get(w.ctx, objKey(p.hpcNS, "site"), &hs); err != nil {
			continue
		}
		t, _, _ := tallyOf(&hs, jobs)
		charged := p.site.Charged(p.account)
		limit := float64(p.budget.gpuShare+p.budget.cpuShare) * 60
		if sl := float64(p.budget.gpuSiteLeft+p.budget.cpuSiteLeft) * 60; sl > 0 && sl < limit {
			limit = sl
		}
		r.row("project "+p.name+": controller used GPU+CPU node-min / site charged / bound",
			fmt.Sprintf("%d / %.0f / %.0f", t.used[0]+t.used[1], charged, limit))
		if charged > limit {
			r.fail("project %s: the site charged %.0f node-minutes, above min(share, siteLeft) = %.0f", p.name, charged, limit)
		}
	}
}
