//go:build scale

package scale

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/watch"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// ---- controller metrics ---------------------------------------------------------------------------

// scrape reads the controller's /metrics and sums the series the report needs.
func scrape(port int) (map[string]float64, error) {
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, ln := range strings.Split(string(b), "\n") {
		if ln == "" || ln[0] == '#' {
			continue
		}
		name, labels, val := parseLine(ln)
		v, perr := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if perr != nil {
			continue
		}
		key := name
		switch name {
		case "controller_runtime_reconcile_total", "controller_runtime_reconcile_errors_total",
			"controller_runtime_reconcile_time_seconds_sum", "controller_runtime_reconcile_time_seconds_count":
			if strings.Contains(labels, `controller="hpcargoworkflowjob"`) {
				out[key] += v
			}
		case "workqueue_depth":
			if strings.Contains(labels, `name="hpcargoworkflowjob"`) {
				out[key] += v
			}
		case "rest_client_requests_total":
			out[key] += v
			if m := between(labels, `method="`, `"`); m != "" {
				out[key+"|"+m] += v
			}
		case "process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes", "go_goroutines":
			out[key] = v
		}
	}
	return out, nil
}

// parseLine splits a metrics line "name{labels} value" (labels optional).
func parseLine(ln string) (name, labels, val string) {
	if head, rest, ok := strings.Cut(ln, "{"); ok {
		labels, val, _ = strings.Cut(rest, "} ")
		return head, labels, val
	}
	name, val, _ = strings.Cut(ln, " ")
	return name, "", val
}

func between(s, a, b string) string {
	_, rest, ok := strings.Cut(s, a)
	if !ok {
		return ""
	}
	v, _, _ := strings.Cut(rest, b)
	return v
}

// sample is one scrape of the controller process.
type sample struct {
	at    time.Time
	epoch int // controller process number: counters restart with it
	m     map[string]float64
}

// sampleEvery is how often the controller is scraped.
const sampleEvery = 2 * time.Second

// sampler scrapes the controller every sampleEvery until stopped.
type sampler struct {
	w       *world
	mu      sync.Mutex
	samples []sample
}

func (w *world) startSampler() {
	s := &sampler{w: w}
	w.sampler = s
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		t := time.NewTicker(sampleEvery)
		defer t.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-t.C:
				cp := w.ctrl
				if cp == nil || cp.cmd == nil {
					continue
				}
				if m, err := scrape(cp.port); err == nil {
					s.mu.Lock()
					s.samples = append(s.samples, sample{at: time.Now(), epoch: cp.epoch, m: m})
					s.mu.Unlock()
				}
			}
		}
	}()
}

func (s *sampler) all() []sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sample(nil), s.samples...)
}

// memStats are the controller's memory over a window of samples, in MB.
type memStats struct{ rssAvg, rssMax, heapAvg, heapMax float64 }

func memOf(ss []sample) memStats {
	var m memStats
	if len(ss) == 0 {
		return m
	}
	for _, s := range ss {
		rss, heap := s.m["process_resident_memory_bytes"]/1e6, s.m["go_memstats_heap_inuse_bytes"]/1e6
		m.rssAvg += rss / float64(len(ss))
		m.heapAvg += heap / float64(len(ss))
		m.rssMax, m.heapMax = max(m.rssMax, rss), max(m.heapMax, heap)
	}
	return m
}

// counterDelta is how much a counter grew over the samples, summed over controller restarts.
func counterDelta(ss []sample, key string) float64 {
	var total float64
	for i := 1; i < len(ss); i++ {
		if ss[i].epoch != ss[i-1].epoch {
			total += ss[i].m[key] // a new process starts at zero
			continue
		}
		total += max(ss[i].m[key]-ss[i-1].m[key], 0)
	}
	return total
}

// ---- the invariant monitor --------------------------------------------------------------------------

// monitor reads the API every second and checks, on every consistent snapshot, the properties that
// must hold at every moment: bookings plus use never above min(share, siteLeft); the recentJobs list
// bounded; and it records how the HPCSite status is written.
type monitor struct {
	w *world

	mu         sync.Mutex
	violations []string
	peakRatio  map[string]float64 // site/tier -> max (used+booked)/ceiling
	maxRecent  int
	maxSiteB   int
	siteWrites map[string][]time.Time // HPCSite name -> MODIFIED event times
	maxActive  int
	snapshots  int
	// obsPeak is the oldest status.siteObservation seen on a live job; obsStale counts the job-snapshots
	// older than three poll intervals (the point where a runner starts polling the site itself).
	obsPeak  map[string]time.Duration // by project namespace
	obsStale map[string]int
	obsSeen  map[string]int
}

func (w *world) startMonitor() {
	m := &monitor{w: w, peakRatio: map[string]float64{}, siteWrites: map[string][]time.Time{},
		obsPeak: map[string]time.Duration{}, obsStale: map[string]int{}, obsSeen: map[string]int{}}
	w.mon = m
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-t.C:
				m.check(w.ctx)
			}
		}
	}()
	go func() { defer w.wg.Done(); m.watchSites(w.ctx) }()
}

func (m *monitor) watchSites(ctx context.Context) {
	for ctx.Err() == nil {
		wi, err := m.w.c.Watch(ctx, &hpcv1alpha1.HPCSiteList{})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ev := range wi.ResultChan() {
			if ev.Type != watch.Modified {
				continue
			}
			if s, ok := ev.Object.(*hpcv1alpha1.HPCSite); ok {
				m.mu.Lock()
				m.siteWrites[s.Namespace] = append(m.siteWrites[s.Namespace], time.Now())
				m.mu.Unlock()
			}
		}
		wi.Stop()
	}
}

func (m *monitor) violate(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.violations) < 50 {
		m.violations = append(m.violations, fmt.Sprintf(format, a...))
	}
}

// onSite reports whether the job's spend or booking belongs to the HPCSite.
func onSite(j *hpcv1alpha1.HPCArgoWorkflowJob, s *hpcv1alpha1.HPCSite) bool {
	if b := j.Status.Booking; b != nil {
		return b.SiteName == s.Name && b.SiteNamespace == s.Namespace
	}
	return j.Status.Site != nil && j.Status.Site.Name == s.Name && j.Status.Site.Namespace == s.Namespace
}

// coverageOf is the project of an HPCSite namespace whose scenario checks coverage, or nil.
func (w *world) coverageOf(hpcNS string) *project {
	for _, p := range w.projects {
		if p.hpcNS == hpcNS && p.coverage {
			return p
		}
	}
	return nil
}

// tally is one site's spend by tier, computed independently of the controller's ledger package.
type tally struct{ used, booked [2]int64 } // index 0 GPU, 1 CPU

func tierIdx(t hpcv1alpha1.Tier) int {
	if t == hpcv1alpha1.TierCPU {
		return 1
	}
	return 0
}

func tallyOf(s *hpcv1alpha1.HPCSite, jobs []hpcv1alpha1.HPCArgoWorkflowJob) (t tally, siteUsed [2]int64, active int) {
	var observed time.Time
	if sl := s.Spec.Budget.SiteLeft; sl != nil && sl.ObservedAt != nil {
		observed = sl.ObservedAt.Time
	}
	for i := range jobs {
		j := &jobs[i]
		if !onSite(j, s) {
			continue
		}
		n := len(j.Status.Attempts)
		// D-26: an ended host whose Slurm job still holds the node keeps its booking; its settlement is provisional.
		openHost := n > 0 && j.Status.Attempts[n-1].HostJobOpen && j.Status.Phase.Terminal()
		for _, a := range j.Status.Attempts {
			if a.HostJobOpen {
				continue
			}
			t.used[tierIdx(a.Tier)] += max(a.SettledNodeMinutes, 0)
			if !a.EndTime.Time.Before(observed) {
				siteUsed[tierIdx(a.Tier)] += max(a.SettledNodeMinutes, 0)
			}
		}
		if b := j.Status.Booking; b != nil && !j.Status.Phase.Terminal() &&
			(j.Status.Phase == hpcv1alpha1.PhaseAdmitted || j.Status.Phase == hpcv1alpha1.PhaseRunning) {
			t.booked[tierIdx(b.Tier)] += b.NodeMinutes
			active++
		}
		if b := j.Status.Booking; b != nil && openHost {
			t.booked[tierIdx(b.Tier)] += b.NodeMinutes
		}
	}
	return t, siteUsed, active
}

func (m *monitor) check(ctx context.Context) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	var sites hpcv1alpha1.HPCSiteList
	if m.w.c.List(ctx, &jobs, rv0()) != nil || m.w.c.List(ctx, &sites, rv0()) != nil {
		return
	}
	m.mu.Lock()
	m.snapshots++
	m.mu.Unlock()
	m.observationAges(jobs.Items)
	for i := range sites.Items {
		s := &sites.Items[i]
		t, siteUsed, active := tallyOf(s, jobs.Items)
		share := [2]int64{s.Spec.Budget.GPUNodeHours * 60, s.Spec.Budget.CPUNodeHours * 60}
		var left [2]int64
		if sl := s.Spec.Budget.SiteLeft; sl != nil {
			left = [2]int64{sl.GPUNodeHours * 60, sl.CPUNodeHours * 60}
		}
		for tier := 0; tier < 2; tier++ {
			if t.used[tier]+t.booked[tier] > share[tier] {
				m.violate("%s tier %d: used %d + booked %d exceeds the share %d", s.Namespace, tier, t.used[tier], t.booked[tier], share[tier])
			}
			if left[tier] > 0 && siteUsed[tier]+t.booked[tier] > left[tier] {
				m.violate("%s tier %d: ended-since-observed %d + booked %d exceeds siteLeft %d", s.Namespace, tier, siteUsed[tier], t.booked[tier], left[tier])
			}
			ceiling := share[tier]
			if left[tier] > 0 && left[tier] < ceiling {
				ceiling = left[tier]
			}
			if ceiling > 0 {
				k := fmt.Sprintf("%s/tier%d", s.Namespace, tier)
				m.mu.Lock()
				m.peakRatio[k] = max(m.peakRatio[k], float64(t.used[tier]+t.booked[tier])/float64(ceiling))
				m.mu.Unlock()
			}
		}
		raw, _ := json.Marshal(s)
		m.mu.Lock()
		m.maxRecent = max(m.maxRecent, len(s.Status.RecentJobs))
		m.maxSiteB = max(m.maxSiteB, len(raw))
		m.maxActive = max(m.maxActive, active)
		m.mu.Unlock()
		// D-26: the ledger covers what the site charges at every moment (a node a host keeps for guests included).
		// Checked where a scenario asks for it; read after the jobs, so the site figure is never older.
		if p := m.w.coverageOf(s.Namespace); p != nil {
			if charged := p.site.Charged(p.account); float64(t.used[0]+t.used[1]+t.booked[0]+t.booked[1]) < charged-0.5 {
				m.violate("%s: used %d + booked %d node-minutes cover less than the site charged (%.1f)", s.Namespace,
					t.used[0]+t.used[1], t.booked[0]+t.booked[1], charged)
			}
		}
		if len(s.Status.RecentJobs) > hpcv1alpha1.RecentJobsMax {
			m.violate("%s recentJobs has %d entries (> %d)", s.Namespace, len(s.Status.RecentJobs), hpcv1alpha1.RecentJobsMax)
		}
	}
}

// observationAges tracks how old the controller's observations of live site jobs are. A job whose
// observation is older than three poll intervals makes its runner poll the site itself.
func (m *monitor) observationAges(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
	stale := 3*m.w.opts.pollInterval + time.Second
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range jobs {
		run, obs := jobs[i].Status.Run, jobs[i].Status.SiteObservation
		if run == nil || run.Terminal || run.SiteJobID == "" || obs == nil || obs.JobID != run.SiteJobID {
			continue
		}
		age, ns := time.Since(obs.ObservedAt.Time), jobs[i].Namespace
		m.obsSeen[ns]++
		m.obsPeak[ns] = max(m.obsPeak[ns], age)
		if age > stale {
			m.obsStale[ns]++
		}
	}
}

// writesPerSite returns the HPCSite MODIFIED events seen: total per site, and the busiest second.
func (m *monitor) writesPerSite() (total map[string]int, peakPerSec map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	total, peakPerSec = map[string]int{}, map[string]int{}
	for ns, ts := range m.siteWrites {
		total[ns] = len(ts)
		per := map[int64]int{}
		for _, t := range ts {
			per[t.Unix()]++
			peakPerSec[ns] = max(peakPerSec[ns], per[t.Unix()])
		}
	}
	return total, peakPerSec
}

// ---- waiting and counting ---------------------------------------------------------------------------

// allJobs lists every HPCArgoWorkflowJob (consistent read).
func (w *world) allJobs() []hpcv1alpha1.HPCArgoWorkflowJob {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := w.c.List(w.ctx, &jobs); err != nil {
		return nil
	}
	return jobs.Items
}

// histogram counts jobs by phase/reason.
func histogram(jobs []hpcv1alpha1.HPCArgoWorkflowJob) map[string]int {
	h := map[string]int{}
	for i := range jobs {
		k := string(jobs[i].Status.Phase)
		if k == "" {
			k = "(none)"
		}
		if jobs[i].Status.Phase.Terminal() || jobs[i].Status.Phase == hpcv1alpha1.PhaseWaiting {
			k += "/" + jobs[i].Status.Reason
		}
		h[k]++
	}
	return h
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// allTerminal reports whether every job is in a final phase and has released its cancel finalizer.
func allTerminal(jobs []hpcv1alpha1.HPCArgoWorkflowJob, want int) bool {
	if len(jobs) < want {
		return false
	}
	for i := range jobs {
		if !jobs[i].Status.Phase.Terminal() || len(jobs[i].Finalizers) > 0 {
			return false
		}
	}
	return true
}

// waitTerminal polls until all want jobs are terminal or the deadline passes; progress is called each poll.
func (w *world) waitTerminal(want int, timeout time.Duration, progress func(jobs []hpcv1alpha1.HPCArgoWorkflowJob)) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		jobs := w.allJobs()
		if progress != nil {
			progress(jobs)
		}
		if allTerminal(jobs, want) {
			return true
		}
		time.Sleep(2 * time.Second)
	}
	return false
}
