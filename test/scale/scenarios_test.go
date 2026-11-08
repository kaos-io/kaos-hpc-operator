//go:build scale

package scale

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

// Scenarios S, M, L and L-chaos of the scale test (see README.md in this directory). They run the real
// controller binary against envtest, the fake kubelet and fake Slurm sites. Run with: make scale-test.

var (
	reportMu sync.Mutex
	results  []*result
)

func objKey(ns, name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func TestMain(m *testing.M) {
	code := m.Run()
	writeReport()
	os.Exit(code)
}

// laneCandidates are the lanes the controller tries in order, the shape of the production setting.
const laneCandidates = `[{"qos":"test"},{"qos":"dev","reservationByPartition":{"gpu":"gpudev"}}]`

func newResult(t *testing.T, name, desc string) *result {
	r := &result{name: name, desc: desc, t: t}
	reportMu.Lock()
	results = append(results, r)
	reportMu.Unlock()
	return r
}

// mixedSpecs makes n job specs over both classes with wall limits 15..45 minutes.
func mixedSpecs(n int, rng *rand.Rand) []jobSpec {
	walls := []int32{15, 20, 30, 45}
	out := make([]jobSpec, n)
	for i := range out {
		class := "hpc-gpu"
		if i%10 >= 7 {
			class = "hpc-cpu"
		}
		wall := walls[rng.Intn(len(walls))]
		// The run is 40-75 % of the limit (dilated minutes = real seconds), long enough to live through
		// several poll intervals and to learn nothing from a TIMEOUT.
		run := float64(wall) * (0.4 + 0.35*rng.Float64())
		out[i] = jobSpec{class: class, wallMinutes: wall, run: fmt.Sprintf("%.1f", run)}
	}
	return out
}

// TestScaleS: one job, end to end.
func TestScaleS(t *testing.T) {
	r := newResult(t, "S (small)", "1 job end to end: admit, book, runner Job, submit, observe, settle.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 2, restBurst: 5})
	site := fakeslurm.New(siteOptions("meluxina", 1))
	p := w.addProject("ps", "meluxina", "u1", "acct1", site, budget{gpuShare: 10, cpuShare: 10, gpuSiteLeft: 8, cpuSiteLeft: 8}, 0)
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	start := time.Now()
	w.createJobs(p, []jobSpec{{class: "hpc-gpu", wallMinutes: 30, run: "3"}})
	if !w.waitTerminal(1, 2*time.Minute, nil) {
		t.Fatalf("job did not finish: %v", histogram(w.allJobs()))
	}
	dur := time.Since(start)
	jobs := w.allJobs()
	j := jobs[0]
	r.row("duration", dur.Round(time.Millisecond))
	r.row("final phases", histogram(jobs))
	if j.Status.Phase != hpcv1alpha1.PhaseSucceeded {
		r.fail("job ended %s/%s: %s", j.Status.Phase, j.Status.Reason, j.Status.Message)
	}
	if j.Status.Booking != nil || len(j.Status.Attempts) != 1 {
		r.fail("booking %v attempts %d, want released and 1", j.Status.Booking, len(j.Status.Attempts))
	}
	if j.Status.SettledNodeMinutes == nil || *j.Status.SettledNodeMinutes < 2 || *j.Status.SettledNodeMinutes > 6 {
		r.fail("settled node-minutes %v, want about 3", j.Status.SettledNodeMinutes)
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.reconcileRows(r, start, dur)
}

// TestScaleM: 50 jobs of mixed classes against a share that admits only part of them.
func TestScaleM(t *testing.T) {
	r := newResult(t, "M (medium)", "50 jobs, 2 classes, a share and siteLeft that admit only part at once: the rest wait "+
		"(WaitingForBudget) and are admitted as bookings settle.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 2, restBurst: 5})
	site := fakeslurm.New(siteOptions("meluxina", 2))
	p := w.addProject("pm", "meluxina", "u1", "acct1", site, budget{gpuShare: 14, cpuShare: 6, gpuSiteLeft: 12, cpuSiteLeft: 5}, 0)
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	n := envInt("SCALE_M_JOBS", 50)
	start := time.Now()
	w.createJobs(p, mixedSpecs(n, rand.New(rand.NewSource(11))))
	peakWaiting := 0
	ok := w.waitTerminal(n, 8*time.Minute, func(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
		waiting := 0
		for i := range jobs {
			if jobs[i].Status.Phase == hpcv1alpha1.PhaseWaiting && jobs[i].Status.Reason == hpcv1alpha1.ReasonWaitingForBudget {
				waiting++
			}
		}
		peakWaiting = max(peakWaiting, waiting)
	})
	dur := time.Since(start)
	jobs := w.allJobs()
	r.row("duration", dur.Round(time.Second))
	r.row("final phases", histogram(jobs))
	r.row("peak jobs waiting for budget", peakWaiting)
	if !ok {
		r.fail("not every job reached a terminal phase: %v", histogram(jobs))
	}
	if peakWaiting == 0 {
		r.fail("the share never made a job wait: the scenario does not exercise WaitingForBudget")
	}
	if h := histogram(jobs); h["Succeeded/Completed"] != n {
		r.fail("want all %d jobs Succeeded/Completed, got %v", n, h)
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyRateLimit(r)
	w.siteCallRows(r, dur, false)
	w.reconcileRows(r, start, dur)
	w.memRows(r, start, 1024)
	w.siteStatusRows(r, dur, 3)
	w.verifyPublishedLedger(r, jobs)
}

// verifyPublishedLedger waits for the published ledger of each HPCSite to settle and compares it with
// the jobs: with every job over, nothing may be shown as booked or active.
func (w *world) verifyPublishedLedger(r *result, jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
	time.Sleep(10 * time.Second)
	for _, p := range w.projects {
		var hs hpcv1alpha1.HPCSite
		if err := w.c.Get(w.ctx, objKey(p.hpcNS, "site"), &hs); err != nil {
			r.fail("reading HPCSite %s: %v", p.hpcNS, err)
			continue
		}
		t, _, _ := tallyOf(&hs, jobs)
		gpu, cpu := hs.Status.GPU, hs.Status.CPU
		if gpu == nil || cpu == nil {
			r.fail("HPCSite %s publishes no ledger", p.hpcNS)
			continue
		}
		r.row("published ledger "+p.name+" (gpu used/booked, cpu used/booked, active)", fmt.Sprintf("%d/%d, %d/%d, %d",
			gpu.UsedNodeMinutes, gpu.BookedNodeMinutes, cpu.UsedNodeMinutes, cpu.BookedNodeMinutes, hs.Status.ActiveJobs))
		if gpu.BookedNodeMinutes != 0 || cpu.BookedNodeMinutes != 0 || hs.Status.ActiveJobs != 0 {
			r.fail("HPCSite %s still publishes booked %d/%d and %d active jobs after every job ended (10 s later)",
				p.hpcNS, gpu.BookedNodeMinutes, cpu.BookedNodeMinutes, hs.Status.ActiveJobs)
		}
		if gpu.UsedNodeMinutes != t.used[0] || cpu.UsedNodeMinutes != t.used[1] {
			r.fail("HPCSite %s publishes used %d/%d, the jobs say %d/%d", p.hpcNS, gpu.UsedNodeMinutes, cpu.UsedNodeMinutes, t.used[0], t.used[1])
		}
	}
}

// lWorld builds the L topology: 3 projects on 2 fake sites. Projects pa and pb share the profile
// "meluxina" (one endpoint, one token bucket); pc is alone on "lumi".
func lWorld(t *testing.T, nodeFail float64, flaky bool) (*world, []*project) {
	w := newWorld(t, ctrlOpts{pollInterval: 3 * time.Second, restRate: 2, restBurst: 5, lanes: laneCandidates})
	o1, o2 := siteOptions("meluxina", 3), siteOptions("lumi", 4)
	o1.NodeFailRate, o2.NodeFailRate = nodeFail, nodeFail
	o1.Capacity, o2.Capacity = 80, 60
	for _, o := range []*fakeslurm.Options{&o1, &o2} {
		o.Latency, o.Jitter = 5*time.Millisecond, 20*time.Millisecond
	}
	if flaky {
		// Both sites are slow and sometimes wrong all the time, on top of the outage that hits meluxina: an
		// unreachable site is a wait for the runner (AC-28), so a second fault in the middle of it fails nothing.
		// A share of the lost-reply jobs finishes before the runner asks again (LostReplyQuick): only slurmdbd
		// still lists them, and they must be found by name, never submitted twice.
		o2.Latency, o2.Jitter = 20*time.Millisecond, 60*time.Millisecond
		o2.Err5xx, o2.TimeoutRate, o2.HangFor, o2.LostReplyRate, o2.LostReplyHold = 0.01, 0.004, 4*time.Second, 0.05, 60*time.Second
		o2.LostReplyQuick = 0.6
		o1.Err5xx, o1.TimeoutRate, o1.HangFor, o1.LostReplyRate, o1.LostReplyHold = 0.005, 0.002, 4*time.Second, 0.05, 60*time.Second
		o1.LostReplyQuick = 0.6
	}
	s1, s2 := fakeslurm.New(o1), fakeslurm.New(o2)
	big := budget{gpuShare: 4000, cpuShare: 4000, gpuSiteLeft: 3500, cpuSiteLeft: 3500}
	ps := []*project{
		w.addProject("pa", "meluxina", "ua", "acct-a", s1, big, 120),
		w.addProject("pb", "meluxina", "ub", "acct-b", s1, big, 120),
		w.addProject("pc", "lumi", "uc", "acct-c", s2, big, 120),
	}
	return w, ps
}

// TestScaleL: 1,000 jobs over 3 projects and 2 sites, with the test and dev lanes on.
func TestScaleL(t *testing.T) {
	r := newResult(t, "L (large)", "1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), "+
		"lanes test+dev on, per-user one-job lane limits at the site.")
	w, ps := lWorld(t, 0, false)
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	n := envInt("SCALE_L_JOBS", 1000)
	start := time.Now()
	rng := rand.New(rand.NewSource(21))
	for i, p := range ps {
		w.createJobs(p, mixedSpecs(n/3+boolInt(i < n%3), rng))
	}
	created := time.Since(start)
	ok := w.waitTerminal(n, 25*time.Minute, func(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
		t.Logf("%s %v", time.Since(start).Round(time.Second), histogram(jobs))
	})
	dur := time.Since(start)
	jobs := w.allJobs()
	r.row("jobs created in", created.Round(time.Second))
	r.row("duration (create to last terminal)", dur.Round(time.Second))
	r.row("throughput (jobs/s)", fmt.Sprintf("%.2f", float64(n)/dur.Seconds()))
	r.row("final phases", histogram(jobs))
	if !ok {
		r.fail("not every job reached a terminal phase: %v", histogram(jobs))
	}
	if h := histogram(jobs); h["Succeeded/Completed"] != n {
		r.fail("want all %d jobs Succeeded/Completed, got %v", n, h)
	}
	w.commonL(r, jobs, start, dur, false)
	w.verifyPublishedLedger(r, jobs)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// commonL are the checks every L-sized run makes.
func (w *world) commonL(r *result, jobs []hpcv1alpha1.HPCArgoWorkflowJob, start time.Time, dur time.Duration, storm bool) {
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyRateLimit(r)
	w.siteCallRows(r, dur, storm)
	w.reconcileRows(r, start, dur)
	w.memRows(r, start, 1536)
	w.siteStatusRows(r, dur, 3)
	w.laneRows(r, jobs)
	errs, throttled := w.ctrl.logCounts()
	r.row("controller log: error lines / client-side throttling waits", fmt.Sprintf("%d / %d", errs, throttled))
	for _, e := range w.ctrl.errorSamples(5) {
		r.row("controller log error sample", e)
	}
}

// laneRows reports the lane placement and asserts the one-job lane semaphore held: the site never
// refused a lane the controller placed (a refusal would mean it submitted a second job into a
// one-job lane, PRD D-69).
func (w *world) laneRows(r *result, jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
	byQOS := map[string]int{}
	refused := 0
	for i := range jobs {
		if p := jobs[i].Status.Placement; p != nil {
			q := p.QOS
			if q == "" {
				q = "default"
			}
			byQOS[q]++
			if p.RefusedQOS != "" {
				refused++
			}
		}
	}
	refusals := 0
	seen := map[*fakeslurm.Site]bool{}
	for _, p := range w.projects {
		if !seen[p.site] {
			seen[p.site] = true
			refusals += p.site.Snapshot().Refusals
		}
	}
	r.row("lane placement (last attempt of each job)", byQOS)
	lost := 0
	seenSite := map[*fakeslurm.Site]bool{}
	for _, p := range w.projects {
		if !seenSite[p.site] {
			seenSite[p.site] = true
			lost += p.site.Snapshot().LostReplies
		}
	}
	r.row("submissions accepted whose reply was lost (found again by key, never repeated)", lost)
	r.row("lane refusals at the site / jobs showing RefusedQOS", fmt.Sprintf("%d / %d", refusals, refused))
	if refusals > 0 || refused > 0 {
		r.fail("the site refused %d lane submissions (%d jobs show a refused lane): the one-job lane semaphore let a second job in", refusals, refused)
	}
}

// TestScaleLChaos: the L run with a controller restart, a 2-minute site outage, 422 storms, 10 % node
// failures, forced re-run limits and checkpointing steps.
func TestScaleLChaos(t *testing.T) {
	r := newResult(t, "L-chaos", "L plus: controller restart mid-run, a site outage (all 5xx, 10 minutes) on the shared site, "+
		"422 storm (slurmctld forgets finished jobs + spurious 422) on the other, 10% NODE_FAIL, jobs that always fail "+
		"(RerunLimit), checkpointing steps (continuations <= 5, one cohort that needs more).")
	w, ps := lWorld(t, 0.10, true)
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	n := envInt("SCALE_L_JOBS", 1000)
	rng := rand.New(rand.NewSource(31))
	var alwaysFail, overLimit, resumable []string
	for i, p := range ps {
		specs := mixedSpecs(n/3+boolInt(i < n%3), rng)
		for k := range specs {
			switch {
			case k < 4: // always lose the node: tier-1 re-runs twice, then RerunLimit
				specs[k].forceEnd = fakeslurm.NodeFail
			case k < 8: // needs 8 segments, 5 continuations allowed: ContinuationLimit
				specs[k].resumable, specs[k].segments, specs[k].wallMinutes, specs[k].run = true, 8, 45, "10"
			case k < 40: // checkpoints, 2 or 3 segments
				specs[k].resumable, specs[k].segments, specs[k].wallMinutes, specs[k].run = true, 2+k%2, 45, "10"
			}
		}
		w.createJobs(p, specs)
		for k, name := range p.jobs {
			switch {
			case k < 4:
				alwaysFail = append(alwaysFail, p.envNS+"/"+name)
			case k < 8:
				overLimit = append(overLimit, p.envNS+"/"+name)
			case k < 40:
				resumable = append(resumable, p.envNS+"/"+name)
			}
		}
	}
	start := time.Now()
	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.chaos(r, ps) }()

	peakWaiting := 0
	ok := w.waitTerminal(n, 60*time.Minute, func(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
		t.Logf("%s %v", time.Since(start).Round(time.Second), histogram(jobs))
		waiting := 0
		for i := range jobs {
			if jobs[i].Status.Phase == hpcv1alpha1.PhaseWaiting && jobs[i].Status.Reason == hpcv1alpha1.ReasonSiteUnavailable {
				waiting++
			}
		}
		peakWaiting = max(peakWaiting, waiting)
	})
	dur := time.Since(start)
	jobs := w.allJobs()
	r.row("duration (create to last terminal)", dur.Round(time.Second))
	r.row("final phases", histogram(jobs))
	if !ok {
		r.fail("not every job reached a terminal phase: %v", histogram(jobs))
	}
	w.verifyOutage(r, ps[:2], peakWaiting, jobs)
	w.verifyQuickLost(r)
	w.verifyChaosOutcomes(r, jobs, alwaysFail, overLimit, resumable)
	w.commonL(r, jobs, start, dur, true)
}

// chaos injects the faults on a schedule, recording what it did into the result. The 422 storm hits the
// small site while it is busy, the outage hits the shared one, and the controller restarts before both.
func (w *world) chaos(r *result, ps []*project) {
	sleep := func(d time.Duration) bool {
		select {
		case <-w.ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	if !sleep(12 * time.Second) {
		return
	}
	// 1. controller restart: a booking of an attempt must be the same on the other side, and nothing may
	// be submitted twice (checked at the end for the whole run).
	before := w.bookings()
	w.ctrl.stop()
	sleep(3 * time.Second)
	w.startController()
	r.row("chaos: controller killed (SIGTERM) and restarted at", "t+12s, down about 3s")
	sleep(10 * time.Second)
	for key, b := range before {
		if a, still := w.bookings()[key]; still && a != b {
			r.fail("restart changed the booking %s: %v -> %v (re-admitted or double-booked)", key, b, a)
		}
	}
	// 2. 422 storm on the small site while it is busy: slurmctld forgets finished jobs at once, and a
	// third of the batched reads are answered 422 for no reason.
	siteC := ps[2].site
	siteC.SetMinJobAge(time.Nanosecond)
	siteC.SetSpurious422(0.3)
	r.row("chaos: 422 storm on site lumi (jobs forgotten at once, 30% spurious 422)", "t+25s, 60s")
	sleep(10 * time.Second)
	// 3. the shared site goes down while the storm is still on, for siteOutage (10 minutes): every call is a 503. Jobs
	// must wait (SiteUnavailable), no runner may be spawned for it, nothing may fail, and all of them finish afterwards.
	siteA := ps[0].site
	siteA.SetDown(true)
	w.outageStart = time.Now()
	r.row("chaos: site meluxina down (all 5xx)", fmt.Sprintf("t+35s, %s", siteOutage))
	sleep(50 * time.Second)
	siteC.SetSpurious422(0)
	siteC.SetMinJobAge(5 * time.Minute)
	// A site counts as down only after probes failed for sitestate.MinDownAfter (90 s) plus a poll: check after 130 s.
	sleep(80 * time.Second)
	w.checkReachableCondition(r, ps[0], false)
	sleep(siteOutage - 130*time.Second)
	siteA.SetDown(false)
	w.outageEnd = time.Now()
	sleep(20 * time.Second)
	w.checkReachableCondition(r, ps[0], true)
}

// siteOutage is how long the shared site is down in L-chaos (SCALE_OUTAGE, a Go duration).
var siteOutage = envDuration("SCALE_OUTAGE", 10*time.Minute)

func envDuration(name string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(name)); err == nil && d > 0 {
		return d
	}
	return def
}

// verifyQuickLost makes sure the scenario really produced the case behind the duplicate-submit fix: submissions
// the site accepted whose reply was lost and whose job ended before the runner asked again. Duplicates
// themselves are asserted by verifySubmits (no attempt submitted twice).
func (w *world) verifyQuickLost(r *result) {
	quick, seen := 0, map[*fakeslurm.Site]bool{}
	for _, p := range w.projects {
		if !seen[p.site] {
			seen[p.site] = true
			quick += p.site.Snapshot().QuickLost
		}
	}
	r.row("lost-reply submissions that finished before the retry (found in accounting by name, never repeated)", quick)
	if quick == 0 {
		r.fail("no lost-reply job finished before its retry: the scenario no longer exercises the adopt-by-name path")
	}
}

// checkReachableCondition asserts the HPCSite's SiteReachable condition (AC-30: health shown in the console).
func (w *world) checkReachableCondition(r *result, p *project, want bool) {
	var hs hpcv1alpha1.HPCSite
	if err := w.c.Get(w.ctx, objKey(p.hpcNS, "site"), &hs); err != nil {
		return
	}
	c := meta.FindStatusCondition(hs.Status.Conditions, hpcv1alpha1.ConditionSiteReachable)
	got := c != nil && c.Status == metav1.ConditionTrue
	r.row(fmt.Sprintf("HPCSite %s SiteReachable (want %v)", p.hpcNS, want), got)
	if c == nil || got != want {
		r.fail("HPCSite %s: SiteReachable is %v, want %v", p.hpcNS, c, want)
	}
}

// verifyOutage asserts what a long outage must look like (AC-28, AC-30): jobs waited with SiteUnavailable, the
// controller spawned no runner Job for the down site while it lasted (after the two probes that notice it), and the
// per-job counters show no runner failure.
func (w *world) verifyOutage(r *result, down []*project, peakWaiting int, jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
	r.row("outage: peak jobs Waiting/SiteUnavailable", peakWaiting)
	if peakWaiting == 0 {
		r.fail("no job ever showed Waiting/SiteUnavailable during a %s outage", siteOutage)
	}
	ns := map[string]bool{}
	for _, p := range down {
		ns[p.hpcNS] = true
	}
	// 20 s after the start the poller has probed the site twice (3 s interval); the end is left alone for the same reason.
	spawned := w.kube.spawnedBetween(ns, w.outageStart.Add(20*time.Second), w.outageEnd)
	r.row("outage: runner Jobs created for the down site while it was down", spawned)
	if spawned > 0 {
		r.fail("%d runner Jobs were created for the down site during the outage: the controller must stop spawning attempts", spawned)
	}
	unavailable := 0
	for i := range jobs {
		if jobs[i].Status.Reason == hpcv1alpha1.ReasonSiteUnavailable {
			unavailable++
		}
	}
	r.row("jobs that ended SiteUnavailable (the bound is 30 min, the outage 10)", unavailable)
	if unavailable > 0 {
		r.fail("%d jobs ended SiteUnavailable: a %s outage is inside the %s bound", unavailable, siteOutage, "30m")
	}
}

// bookings maps job#attempt -> booking for every job that holds one.
func (w *world) bookings() map[string]string {
	out := map[string]string{}
	for _, j := range w.allJobs() {
		if b := j.Status.Booking; b != nil && !j.Status.Phase.Terminal() {
			// Keyed by attempt: a re-run or a continuation is a new attempt with a new booking, by design.
			out[fmt.Sprintf("%s/%s#a%d", j.Namespace, j.Name, j.Status.Attempt)] = fmt.Sprintf("%s %d %s", b.Tier, b.NodeMinutes, b.At.Time.UTC().Format(time.RFC3339))
		}
	}
	return out
}

// verifyChaosOutcomes asserts the recovery bounds on the final state of every job.
func (w *world) verifyChaosOutcomes(r *result, jobs []hpcv1alpha1.HPCArgoWorkflowJob, alwaysFail, overLimit, resumable []string) {
	by := map[string]*hpcv1alpha1.HPCArgoWorkflowJob{}
	reruns, maxReruns, contin, runnerFailed, rerunLimit := 0, int32(0), 0, 0, 0
	for i := range jobs {
		j := &jobs[i]
		by[j.Namespace+"/"+j.Name] = j
		maxReruns = max(maxReruns, j.Status.Reruns)
		reruns += int(j.Status.Reruns)
		contin += int(j.Status.Continuations)
		if j.Status.Reruns > hpcv1alpha1.MaxReruns {
			r.fail("job %s/%s was re-run %d times (max %d)", j.Namespace, j.Name, j.Status.Reruns, hpcv1alpha1.MaxReruns)
		}
		if j.Status.Continuations > hpcv1alpha1.DefaultMaxContinuations {
			r.fail("job %s/%s continued %d times (max %d)", j.Namespace, j.Name, j.Status.Continuations, hpcv1alpha1.DefaultMaxContinuations)
		}
		if j.Status.Reason == hpcv1alpha1.ReasonRunnerFailed {
			runnerFailed++
		}
		if j.Status.Reason == hpcv1alpha1.ReasonRerunLimit {
			rerunLimit++
		}
	}
	r.row("tier-1 re-runs (total / max per job)", fmt.Sprintf("%d / %d", reruns, maxReruns))
	r.row("continuations (total)", contin)
	r.row("jobs ended RerunLimit", rerunLimit)
	r.row("jobs ended RunnerFailed (a runner pod out of retries)", runnerFailed)
	if runnerFailed > 0 {
		r.fail("%d jobs failed RunnerFailed: the site outage must make jobs wait, not fail", runnerFailed)
	}
	for _, k := range alwaysFail {
		if j := by[k]; j == nil || j.Status.Reason != hpcv1alpha1.ReasonRerunLimit || j.Status.Reruns != hpcv1alpha1.MaxReruns {
			r.fail("always-NODE_FAIL job %s: %s, want Failed/RerunLimit after %d re-runs", k, describe(j), hpcv1alpha1.MaxReruns)
		}
	}
	notLimited := 0
	for _, k := range overLimit {
		if j := by[k]; j == nil || j.Status.Phase == hpcv1alpha1.PhaseSucceeded {
			notLimited++
		}
	}
	if notLimited > 0 {
		r.fail("%d of %d jobs that need 8 segments succeeded: the continuation cap of 5 did not hold", notLimited, len(overLimit))
	}
	okRes, limited := 0, 0
	for _, k := range resumable {
		j := by[k]
		switch {
		case j != nil && j.Status.Phase == hpcv1alpha1.PhaseSucceeded:
			okRes++
		case j != nil && j.Status.Reason == hpcv1alpha1.ReasonContinuationLimit:
			limited++
		}
	}
	r.row("checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total",
		fmt.Sprintf("%d / %d / %d", okRes, limited, len(resumable)))
	// A 10 % node failure rate must not push the checkpointing steps off their rails: most succeed.
	if okRes < len(resumable)*8/10 {
		r.fail("only %d of %d checkpointing steps succeeded", okRes, len(resumable))
	}
	// Everything else (not forced, not segmented) succeeds unless it lost its node three times in a row.
	other, failed := 0, 0
	skip := map[string]bool{}
	for _, l := range [][]string{alwaysFail, overLimit, resumable} {
		for _, k := range l {
			skip[k] = true
		}
	}
	for k, j := range by {
		if skip[k] {
			continue
		}
		other++
		if j.Status.Phase != hpcv1alpha1.PhaseSucceeded && j.Status.Reason != hpcv1alpha1.ReasonRerunLimit {
			failed++
			if failed <= 5 {
				r.fail("job %s ended %s", k, describe(j))
			}
		}
	}
	r.row("ordinary jobs / not succeeded and not RerunLimit", fmt.Sprintf("%d / %d", other, failed))
}

func describe(j *hpcv1alpha1.HPCArgoWorkflowJob) string {
	if j == nil {
		return "(missing)"
	}
	return fmt.Sprintf("%s/%s reruns=%d continuations=%d attempts=%d: %s", j.Status.Phase, j.Status.Reason, j.Status.Reruns,
		j.Status.Continuations, len(j.Status.Attempts), j.Status.Message)
}
