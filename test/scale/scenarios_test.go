//go:build scale

package scale

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
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

// TestScaleShared: node sharing (PRD-HPC-1461 D-26). Hosts whose Slurm job keeps the node after their step while
// guests run on it, and guests that never start on their own. Live 2026-11-10 (host 5439966, guest 5439976): the
// host's step ended at 04:26 and its booking was released while the site charged the node until 04:45. The
// ledger must cover what the site charges at every moment and equal it once every job has ended.
func TestScaleShared(t *testing.T) {
	r := newResult(t, "S-shared (node sharing)", "8 hosts whose Slurm job keeps the node 6-14 minutes after the step for "+
		"16 guests that never start on their own: the ledger covers the site's charge at every snapshot and equals it at the end.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 4, restBurst: 10})
	site := fakeslurm.New(siteOptions("meluxina", 7))
	p := w.addProject("psh", "meluxina", "u1", "acct1", site, budget{gpuShare: 100, cpuShare: 10, gpuSiteLeft: 90, cpuSiteLeft: 8}, 0)
	p.coverage = true
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	start := time.Now()
	rng := rand.New(rand.NewSource(7))
	hosts := make([]jobSpec, 8)
	for i := range hosts {
		hosts[i] = jobSpec{class: "hpc-gpu", wallMinutes: 30, run: fmt.Sprintf("%.1f", 3+2*rng.Float64()),
			hostTail: fmt.Sprintf("%.1f", 6+8*rng.Float64())}
	}
	w.createJobs(p, hosts)
	time.Sleep(3 * time.Second) // the hosts start first, as a host must be running to take a guest
	guests := make([]jobSpec, 16)
	for i := range guests {
		guests[i] = jobSpec{class: "hpc-gpu", wallMinutes: 30, run: fmt.Sprintf("%.1f", 2+2*rng.Float64()), guest: true}
	}
	w.createJobs(p, guests)
	// Every job ended and every host closed: a host is Succeeded at its step's end, its booking goes with its job.
	deadline := time.Now().Add(4 * time.Minute)
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	for time.Now().Before(deadline) {
		jobs = w.allJobs()
		open := 0
		for i := range jobs {
			if !jobs[i].Status.Phase.Terminal() || jobs[i].Status.Booking != nil {
				open++
			}
		}
		if len(jobs) == 24 && open == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	dur := time.Since(start)
	r.row("duration", dur.Round(time.Second))
	r.row("final phases", histogram(jobs))
	var used int64
	hostsClosed, guestsZero, guestSamples := 0, 0, 0
	for i := range jobs {
		j := &jobs[i]
		if j.Status.Phase != hpcv1alpha1.PhaseSucceeded || j.Status.Booking != nil || len(j.Status.Attempts) != 1 {
			r.fail("%s ended %s/%s booking %v attempts %d: %s", j.Name, j.Status.Phase, j.Status.Reason,
				j.Status.Booking, len(j.Status.Attempts), j.Status.Message)
			continue
		}
		a := j.Status.Attempts[0]
		used += a.SettledNodeMinutes
		switch {
		case j.Annotations[annGuest] != "":
			if j.Status.Run.HostSiteJobID == "" || a.SettledNodeMinutes != 0 {
				r.fail("guest %s: host %q settled %d, want a host and 0", j.Name, j.Status.Run.HostSiteJobID, a.SettledNodeMinutes)
			} else {
				guestsZero++
			}
			if _, ok := runSample(j); ok {
				guestSamples++ // its step start from its host's comment: a learned-limit sample (D-67)
			}
		case a.HostJobOpen || !strings.Contains(j.Status.Message, "its Slurm job held the node until"):
			r.fail("host %s not settled to its job's end: open %v, %q", j.Name, a.HostJobOpen, j.Status.Message)
		default:
			hostsClosed++
			// D-67: the learned-limit sample is the step's run time; the node kept for guests is hostJobEndTime.
			run := j.Status.Run
			if d, ok := runSample(j); !ok || run.HostJobEndTime == nil || !run.HostJobEndTime.After(run.EndTime.Time) ||
				float64(a.SettledNodeMinutes) < d.Minutes() {
				r.fail("host %s: step %v, job end %v, settled %d: want the step's own time below the job's", j.Name, d, run.HostJobEndTime, a.SettledNodeMinutes)
			}
		}
	}
	charged := site.Charged(p.account)
	r.row("hosts settled to their job's end / guests settled 0", fmt.Sprintf("%d of 8 / %d of 16", hostsClosed, guestsZero))
	r.row("guests with a step-time sample (start from the host)", fmt.Sprintf("%d of 16", guestSamples))
	if guestSamples != 16 {
		r.fail("%d of 16 guests carry their step's start", guestSamples)
	}
	r.row("controller settled node-min / site charged", fmt.Sprintf("%d / %.1f", used, charged))
	// Each attempt is settled in whole minutes, rounded up: at most one minute above the site's figure per host.
	if float64(used) < charged-0.5 || float64(used) > charged+8 {
		r.fail("the ledger settled %d node-minutes for what the site charged %.1f", used, charged)
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyPublishedLedger(r, jobs)
	w.reconcileRows(r, start, dur)
}

// TestScaleSharedResumable: resumable steps share nodes (PRD-HPC-1461 AC-73, reversing D-44's gate). A mix of
// resumable and plain steps packs onto hosts: resumable guests are taken when only their lead fits, are signalled
// at their host job's end minus the lead and exit 85, and their continuations (a new job each) host, guest or run
// alone. Every job succeeds, every resumable one through the right number of continuations, guests settle 0, a
// plain step never continues, and the controller's ledger equals what the site charged.
func TestScaleSharedResumable(t *testing.T) {
	r := newResult(t, "S-shared-resumable (node sharing with resumable steps)", "10 hosts (6 resumable with 3 segments, 4 plain) "+
		"and 30 guests (20 resumable with 2-3 segments, 10 plain) that run 8-16 minutes on hosts that end sooner: resumable guests are "+
		"signalled and continued, plain ones fit or run alone; the ledger equals the site's charge.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 4, restBurst: 10})
	opts := siteOptions("meluxina", 11)
	opts.GuestAloneAfter = 6 * time.Second
	site := fakeslurm.New(opts)
	p := w.addProject("psr", "meluxina", "u1", "acct1", site, budget{gpuShare: 100000, cpuShare: 10, gpuSiteLeft: 90000, cpuSiteLeft: 8}, 0)
	p.coverage = true
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	start := time.Now()
	rng := rand.New(rand.NewSource(11))
	hosts := make([]jobSpec, 10)
	for i := range hosts {
		hosts[i] = jobSpec{class: "hpc-gpu", wallMinutes: 60, run: fmt.Sprintf("%.1f", 6+2*rng.Float64()),
			hostTail: fmt.Sprintf("%.1f", 12+8*rng.Float64())}
		if i < 6 {
			hosts[i].resumable, hosts[i].segments, hosts[i].maxCont = true, 3, 12
		}
	}
	w.createJobs(p, hosts)
	time.Sleep(3 * time.Second) // the hosts start first, as a host must be running to take a guest
	guests := make([]jobSpec, 30)
	for i := range guests {
		guests[i] = jobSpec{class: "hpc-gpu", wallMinutes: 60, run: fmt.Sprintf("%.1f", 8+8*rng.Float64()), guest: true}
		if i < 20 {
			guests[i].resumable, guests[i].segments, guests[i].maxCont = true, 2+i%2, 12
		}
	}
	w.createJobs(p, guests)
	all := append(hosts, guests...)
	deadline := time.Now().Add(5 * time.Minute)
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	for time.Now().Before(deadline) {
		jobs = w.allJobs()
		open := 0
		for i := range jobs {
			if !jobs[i].Status.Phase.Terminal() || jobs[i].Status.Booking != nil {
				open++
			}
		}
		if len(jobs) == len(all) && open == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	dur := time.Since(start)
	r.row("duration", dur.Round(time.Second))
	r.row("final phases", histogram(jobs))
	byName := map[string]jobSpec{}
	for i, name := range p.jobs {
		byName[name] = all[i]
	}
	var used int64
	continued, plainContinued := 0, 0
	for i := range jobs {
		j := &jobs[i]
		spec := byName[j.Name]
		for _, a := range j.Status.Attempts {
			used += a.SettledNodeMinutes
		}
		if j.Status.Phase != hpcv1alpha1.PhaseSucceeded || j.Status.Booking != nil {
			r.fail("%s ended %s/%s booking %v: %s", j.Name, j.Status.Phase, j.Status.Reason, j.Status.Booking, j.Status.Message)
			continue
		}
		attempts := len(j.Status.Attempts)
		switch {
		case !spec.resumable && attempts != 1:
			plainContinued++
			r.fail("%s is not resumable but ran %d attempts", j.Name, attempts)
		case spec.resumable:
			// A resumable step makes at least its segments in all; a continuation each time but the last.
			if attempts < spec.segments || int(j.Status.Continuations) != attempts-1 {
				r.fail("%s: %d attempts, %d continuations for %d segments", j.Name, attempts, j.Status.Continuations, spec.segments)
			}
			if attempts > 1 {
				continued++
				if j.Status.Resume == nil || j.Status.Resume.CheckpointRef == "" {
					r.fail("%s continued without a checkpoint to continue from", j.Name)
				}
			}
		}
	}
	snap := site.Snapshot()
	charged := site.Charged(p.account)
	r.row("guests taken by a host / resumable guests signalled before their run was over", fmt.Sprintf("%d / %d", snap.GuestsTaken, snap.GuestsSignalled))
	r.row("resumable jobs continued / plain jobs that continued", fmt.Sprintf("%d of 26 / %d", continued, plainContinued))
	r.row("controller settled node-min / site charged", fmt.Sprintf("%d / %.1f", used, charged))
	if snap.GuestsTaken == 0 || snap.GuestsSignalled == 0 {
		r.fail("the packing did not happen: %d guests taken, %d signalled", snap.GuestsTaken, snap.GuestsSignalled)
	}
	if continued != 26 {
		r.fail("%d of 26 resumable jobs continued, want all", continued)
	}
	// Each attempt is settled in whole minutes, rounded up: a little above the site's figure per attempt that ran.
	if float64(used) < charged-0.5 || float64(used) > charged+float64(len(jobs))*2 {
		r.fail("the ledger settled %d node-minutes for what the site charged %.1f", used, charged)
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyPublishedLedger(r, jobs)
	w.reconcileRows(r, start, dur)
}

// TestScalePacking: deterministic node sharing (PRD-HPC-1461 D-26, D-70). N shareable 1-GPU steps of one scope and
// class that arrive at once on a site with ample free nodes use ceil(N / 4) nodes, whatever the site's capacity.
// Live 2026-11-10: four such steps of one run started on four nodes in the same second (nobody was running to take
// a guest) and held them idle for 8, 9, 9 and 4 minutes, 30 node-minutes for about 8. Against the controller before
// the plan the site starts N jobs: the node count and the charge assertions fail.
func TestScalePacking(t *testing.T) {
	r := newResult(t, "S-pack (deterministic node sharing)", "4, 8 and 10 shareable 1-GPU steps arrive at once on sites with ample free "+
		"nodes (4 GPUs per node): each run uses ceil(N/4) nodes, the rest ride as guests; every step succeeds; the site's charge is "+
		"about one node's time per host, not one per step.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 4, restBurst: 10, lanes: laneCandidates})
	type run struct {
		n    int
		p    *project
		site *fakeslurm.Site
	}
	var runs []run
	for i, n := range []int{4, 8, 10} {
		site := fakeslurm.New(siteOptions("meluxina", int64(20+i)))
		p := w.addProject(fmt.Sprintf("ppk%d", n), "meluxina", fmt.Sprintf("u%d", i+1), fmt.Sprintf("acct%d", i+1), site,
			budget{gpuShare: 100, cpuShare: 10, gpuSiteLeft: 90, cpuSiteLeft: 8}, 0)
		w.enableSharing(p)
		p.coverage = true
		runs = append(runs, run{n, p, site})
	}
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	start := time.Now()
	rng := rand.New(rand.NewSource(11))
	for _, ru := range runs {
		steps := make([]jobSpec, ru.n)
		for i := range steps {
			// The live run's shape: one class, one scope, the same 30-minute limit, a few minutes of work.
			steps[i] = jobSpec{class: "hpc-gpu", wallMinutes: 30, run: fmt.Sprintf("%.1f", 4+2*rng.Float64()), shareable: true}
		}
		w.createJobs(ru.p, steps)
	}
	deadline := time.Now().Add(4 * time.Minute)
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	for time.Now().Before(deadline) {
		jobs = w.allJobs()
		open := 0
		for i := range jobs {
			if !jobs[i].Status.Phase.Terminal() || jobs[i].Status.Booking != nil {
				open++
			}
		}
		if len(jobs) == 22 && open == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	r.row("duration", time.Since(start).Round(time.Second))
	r.row("final phases", histogram(jobs))
	for _, ru := range runs {
		verifyPacking(r, ru.p, ru.site, ru.n, jobs)
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyPublishedLedger(r, jobs)
	w.reconcileRows(r, start, time.Since(start))
}

// TestScaleMatrix: the items of a matrix step (PRD kaos#1911). Each item is one job of one run with the step's name,
// created together by the follower; they are shareable steps of one scope and class like any others, so the plan of
// D-70 packs them: four items of a 1-GPU step use one node (a host and three guests), each with its own result. The
// same four items on a CPU class are four jobs on four nodes: the controller shares a node only between steps that
// take one GPU each (internal/controller/share.go shareableClass). The test states that as it is, so a change of
// the rule shows here.
func TestScaleMatrix(t *testing.T) {
	r := newResult(t, "S-matrix (the items of a matrix step)", "a 4-item matrix step on a 1-GPU class: one node, a host and "+
		"three guests, every item succeeds with its own result; the same step on a CPU class: four nodes, no guests.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 4, restBurst: 10, lanes: laneCandidates})
	gpuSite := fakeslurm.New(siteOptions("meluxina", 41))
	gpu := w.addProject("mxg", "meluxina", "u1", "acct1", gpuSite, budget{gpuShare: 100, cpuShare: 10, gpuSiteLeft: 90, cpuSiteLeft: 8}, 0)
	w.enableSharing(gpu)
	gpu.coverage = true
	cpuSite := fakeslurm.New(siteOptions("meluxina", 42))
	cpu := w.addProject("mxc", "meluxina", "u2", "acct2", cpuSite, budget{gpuShare: 10, cpuShare: 100, gpuSiteLeft: 8, cpuSiteLeft: 90}, 0)
	w.enableSharing(cpu)
	cpu.coverage = true
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	start := time.Now()
	items := func(class string) []jobSpec {
		out := make([]jobSpec, 4)
		for i := range out {
			out[i] = jobSpec{class: class, wallMinutes: 30, run: "5", shareable: true, stepName: "train"}
		}
		return out
	}
	w.createJobs(gpu, items("hpc-gpu"))
	w.createJobs(cpu, items("hpc-cpu"))
	deadline := time.Now().Add(4 * time.Minute)
	var jobs []hpcv1alpha1.HPCArgoWorkflowJob
	for time.Now().Before(deadline) {
		jobs = w.allJobs()
		open := 0
		for i := range jobs {
			if !jobs[i].Status.Phase.Terminal() || jobs[i].Status.Booking != nil {
				open++
			}
		}
		if len(jobs) == 8 && open == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	r.row("duration", time.Since(start).Round(time.Second))
	r.row("final phases", histogram(jobs))
	verifyPacking(r, gpu, gpuSite, 4, jobs)
	hosts, guests := 0, 0
	for i := range jobs {
		j := &jobs[i]
		if j.Namespace != cpu.envNS {
			continue
		}
		if j.Spec.StepName != "train" || j.Status.Phase != hpcv1alpha1.PhaseSucceeded {
			r.fail("%s: step %q ended %s/%s", j.Name, j.Spec.StepName, j.Status.Phase, j.Status.Reason)
		}
		if j.Status.Run != nil && j.Status.Run.HostSiteJobID != "" {
			guests++
		} else {
			hosts++
		}
	}
	started := cpuSite.Snapshot().Started
	r.row("CPU class, 4 items: nodes started / hosts / guests (a CPU class is not shared: want 4 / 4 / 0)", fmt.Sprintf("%d / %d / %d", started, hosts, guests))
	if started != 4 || hosts != 4 || guests != 0 {
		r.fail("4 items of a CPU matrix step used %d nodes (%d hosts, %d guests); the controller shares only 1-GPU classes: want 4 / 4 / 0", started, hosts, guests)
	}
	names := map[string]bool{}
	for i := range jobs {
		names[jobs[i].Namespace+"/"+jobs[i].Name] = true
		if jobs[i].Namespace == gpu.envNS && jobs[i].Spec.StepName != "train" {
			r.fail("%s has the step name %q, want the step's own: train", jobs[i].Name, jobs[i].Spec.StepName)
		}
	}
	if len(names) != 8 {
		r.fail("%d distinct jobs for 8 items", len(names))
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyPublishedLedger(r, jobs)
	w.reconcileRows(r, start, time.Since(start))
}

// TestScaleMatrixBudget: a 32-item matrix step against a budget of one node-hour (PRD kaos#1911). What the controller
// does with a fan-out larger than the budget (internal/admission/admission.go, Decide): every item books nodes x its
// limit when it is admitted, a guest included, and settles to what it used when it ends. An item that fits what is
// free is admitted; one that fits the share but not what is free (running items hold it) waits with WaitingForBudget;
// one that is more than what is left this period after what was USED is refused at once with BudgetBlocked, naming
// the minutes it needs and the minutes left. So no item is submitted beyond the project's share: the first items
// run, the next wait, and once the spent minutes leave less than one item needs, the rest are refused, not queued
// for ever. An item whose own limit is more than the whole share is refused before any of this.
func TestScaleMatrixBudget(t *testing.T) {
	r := newResult(t, "S-matrix-budget (a 32-item matrix against a one-node-hour budget)", "32 items of 30 minutes on a 1-GPU "+
		"class with 1 GPU node-hour of share: the first items are admitted, the next wait for budget, the rest are refused "+
		"with BudgetBlocked once the spent minutes leave less than an item needs; the site is never charged more than the share; "+
		"one more item of 90 minutes is refused as soon as it is seen.")
	w := newWorld(t, ctrlOpts{pollInterval: 2 * time.Second, restRate: 4, restBurst: 10, lanes: laneCandidates})
	site := fakeslurm.New(siteOptions("meluxina", 43))
	p := w.addProject("mxb", "meluxina", "u1", "acct1", site, budget{gpuShare: 1, cpuShare: 10, gpuSiteLeft: 1, cpuSiteLeft: 8}, 0)
	w.enableSharing(p)
	p.coverage = true
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	start := time.Now()
	items := make([]jobSpec, 33)
	for i := range items {
		items[i] = jobSpec{class: "hpc-gpu", wallMinutes: 30, run: "3", shareable: true, stepName: "train"}
	}
	items[32].wallMinutes = 90
	w.createJobs(p, items)
	peakWaiting, firstWaiting := 0, ""
	ok := w.waitTerminal(33, 10*time.Minute, func(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
		waiting := 0
		for i := range jobs {
			if jobs[i].Status.Phase == hpcv1alpha1.PhaseWaiting && jobs[i].Status.Reason == hpcv1alpha1.ReasonWaitingForBudget {
				waiting++
				if firstWaiting == "" {
					firstWaiting = jobs[i].Status.Message
				}
			}
		}
		peakWaiting = max(peakWaiting, waiting)
	})
	jobs := w.allJobs()
	r.row("duration", time.Since(start).Round(time.Second))
	r.row("final phases", histogram(jobs))
	r.row("peak items waiting for budget", peakWaiting)
	r.row("a waiting item's message", firstWaiting)
	if !ok {
		r.fail("not every job reached a terminal phase: %v", histogram(jobs))
	}
	refused, succeeded, oversize := 0, 0, 0
	for i := range jobs {
		j := &jobs[i]
		switch {
		case j.Status.Phase == hpcv1alpha1.PhaseRefused:
			refused++
			if j.Status.Reason != hpcv1alpha1.ReasonBudgetBlocked || !strings.Contains(j.Status.Message, "node-minutes; the project's share has") {
				r.fail("%s refused (%s: %s), want BudgetBlocked naming the minutes needed and left", j.Name, j.Status.Reason, j.Status.Message)
			}
			if j.Spec.WallTimeMinutes == 90 {
				oversize++
				r.row("the 90-minute item", fmt.Sprintf("%s/%s: %s", j.Status.Phase, j.Status.Reason, j.Status.Message))
			} else if refused-oversize == 1 {
				r.row("the first refused 30-minute item", fmt.Sprintf("%s/%s: %s", j.Status.Phase, j.Status.Reason, j.Status.Message))
			}
		case j.Status.Phase == hpcv1alpha1.PhaseSucceeded:
			succeeded++
		}
	}
	r.row("items succeeded / refused", fmt.Sprintf("%d / %d", succeeded, refused))
	if oversize != 1 || succeeded < 2 || succeeded+refused != 33 || refused-oversize < 1 {
		r.fail("want the 90-minute item refused, at least 2 items run and at least 1 refused; got %d succeeded, %d refused (oversize %d)", succeeded, refused, oversize)
	}
	if charged := site.Charged(p.account); charged > 60 {
		r.fail("the site charged %.1f node-minutes against a share of 60", charged)
	}
	if peakWaiting == 0 {
		r.fail("the budget never made an item wait")
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyPublishedLedger(r, jobs)
	w.reconcileRows(r, start, time.Since(start))
}

// verifyPacking asserts the node count of one run of n shareable steps: the site started ceil(n/4) jobs, those are
// the steps without a host, the others are guests that settled to zero, and the charge is about one node per host.
func verifyPacking(r *result, p *project, site *fakeslurm.Site, n int, all []hpcv1alpha1.HPCArgoWorkflowJob) {
	want := (n + 3) / 4
	hosts, guests, settled := 0, 0, int64(0)
	for i := range all {
		j := &all[i]
		if j.Namespace != p.envNS {
			continue
		}
		if j.Status.Phase != hpcv1alpha1.PhaseSucceeded || j.Status.Booking != nil || len(j.Status.Attempts) != 1 {
			r.fail("%s ended %s/%s booking %v attempts %d: %s", j.Name, j.Status.Phase, j.Status.Reason, j.Status.Booking,
				len(j.Status.Attempts), j.Status.Message)
			continue
		}
		settled += j.Status.Attempts[0].SettledNodeMinutes
		if j.Status.Run.HostSiteJobID != "" {
			guests++
			if j.Status.Attempts[0].SettledNodeMinutes != 0 {
				r.fail("guest %s settled %d node-minutes, want 0", j.Name, j.Status.Attempts[0].SettledNodeMinutes)
			}
		} else {
			hosts++
		}
	}
	started := site.Snapshot().Started
	charged := site.Charged(p.account)
	plan := ""
	for i := range all {
		if j := &all[i]; j.Namespace == p.envNS && j.Status.Placement != nil {
			role, host := "none", ""
			if sh := plannedShare(j.Status.Placement); sh != "" {
				role = sh
			}
			plan += fmt.Sprintf(" %s:%s@%s/qos=%s/host=%s", j.Name, role, j.Status.Run.SiteJobID, j.Status.Placement.QOS, host)
		}
	}
	r.row(fmt.Sprintf("%d steps: plan", n), plan)
	r.row(fmt.Sprintf("%d steps: nodes started at the site / hosts / guests (want %d nodes)", n, want), fmt.Sprintf("%d / %d / %d", started, hosts, guests))
	r.row(fmt.Sprintf("%d steps: site charge, node-minutes (settled %d)", n, settled), fmt.Sprintf("%.1f", charged))
	if started != want || hosts != want || guests != n-want {
		r.fail("%d shareable steps used %d nodes (%d hosts, %d guests); want ceil(%d/4) = %d nodes", n, started, hosts, guests, n, want)
	}
	// A host's job lasts its longest step (6 minutes) plus the slot manager's 2-minute idle release: far below one
	// node per step. A step per node would charge at least 6 node-minutes each.
	if limit := float64(want) * 10; charged > limit {
		r.fail("%d shareable steps were charged %.1f node-minutes; want at most %.0f for %d nodes", n, charged, limit, want)
	}
}

// runSample is the step's own run time, start to end of the final report: what a learned limit is made of (D-67).
func runSample(j *hpcv1alpha1.HPCArgoWorkflowJob) (time.Duration, bool) {
	run := j.Status.Run
	if run == nil || run.StartTime == nil || run.EndTime == nil || !run.EndTime.After(run.StartTime.Time) {
		return 0, false
	}
	return run.EndTime.Sub(run.StartTime.Time), true
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

// TestScaleLateRecord: PRD-HPC-1461 AC-58 / T-14. 300 jobs on one site whose accounting lags slurmctld by 5 minutes
// (SCALE_ACCT_LAG): after a job ends, jobs/state already says so, but slurmdbd still shows it RUNNING without an
// exit code or an end for 5 minutes. The terminal state of a job comes from the site's record, never from a guess:
// no job may end Failed, every job ends Succeeded/Completed once the record arrives, and the call rate to the site
// stays batched and inside the token bucket (one list-owned call per HPCSite per interval, not one per job).
func TestScaleLateRecord(t *testing.T) {
	lag := envDuration("SCALE_ACCT_LAG", 5*time.Minute)
	r := newResult(t, "L-late (late exit record)", "300 jobs on one site whose accounting record lags the job's end by "+
		lag.String()+": a late record never marks a good job Failed and the call rate stays constant (D-36, T-14).")
	w := newWorld(t, ctrlOpts{pollInterval: 3 * time.Second, restRate: 2, restBurst: 5})
	o := siteOptions("meluxina", 31)
	o.AcctLag, o.Capacity = lag, 100
	site := fakeslurm.New(o)
	p := w.addProject("pl", "meluxina", "u1", "acct1", site, budget{gpuShare: 4000, cpuShare: 4000, gpuSiteLeft: 3500, cpuSiteLeft: 3500}, 0)
	w.startController()
	w.startKubelet()
	w.startSampler()
	w.startMonitor()
	n := envInt("SCALE_LATE_JOBS", 300)
	start := time.Now()
	w.createJobs(p, mixedSpecs(n, rand.New(rand.NewSource(58))))
	ok := w.waitTerminal(n, lag+12*time.Minute, func(jobs []hpcv1alpha1.HPCArgoWorkflowJob) {
		t.Logf("%s %v", time.Since(start).Round(time.Second), histogram(jobs))
	})
	dur := time.Since(start)
	jobs := w.allJobs()
	st := site.Snapshot()
	r.row("duration", dur.Round(time.Second))
	r.row("final phases", histogram(jobs))
	r.row("accounting reads that lagged (record still RUNNING after the end)", st.LaggedReads)
	if !ok {
		r.fail("not every job reached a terminal phase: %v", histogram(jobs))
	}
	if h := histogram(jobs); h["Succeeded/Completed"] != n {
		r.fail("want all %d jobs Succeeded/Completed with the exit record %s late, got %v", n, lag, h)
	}
	for i := range jobs {
		if jobs[i].Status.Phase == hpcv1alpha1.PhaseFailed {
			r.fail("job %s ended Failed/%s while the record was late: %s", jobs[i].Name, jobs[i].Status.Reason, jobs[i].Status.Message)
		}
	}
	if st.LaggedReads < n {
		r.fail("only %d lagged accounting reads for %d jobs: the scenario does not exercise the late record", st.LaggedReads, n)
	}
	if dur < lag {
		r.fail("all jobs ended in %s, before the %s lag: the late record was not waited for", dur.Round(time.Second), lag)
	}
	w.verifySubmits(r, jobs)
	w.budgetRows(r, jobs)
	w.verifyRateLimit(r)
	w.siteCallRows(r, dur, false)
	w.reconcileRows(r, start, dur)
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
	if c != nil && !got && !want {
		// When the controller itself noticed the outage: what "no runner Job after noticing" is measured from.
		w.outageNoticed = c.LastTransitionTime.Time
	}
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
	// From the moment the controller marked the site unreachable (SiteReachable turned False), not from a fixed time
	// after the outage began: how soon it notices depends on the machine's load, "no spawn after noticing" does not.
	from := w.outageNoticed
	if from.IsZero() {
		r.fail("the HPCSite never showed SiteReachable False: the controller did not notice the outage")
		from = w.outageStart.Add(130 * time.Second)
	}
	spawned := w.kube.spawnedBetween(ns, from.Add(5*time.Second), w.outageEnd)
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

// plannedShare is the planned role of a placement read from its JSON (status.placement.share), "" when there is none: the
// harness does not depend on the field, so it also runs against a controller that has no share plan.
func plannedShare(p *hpcv1alpha1.Placement) string {
	b, _ := json.Marshal(p)
	var v struct {
		Share *struct {
			Role string `json:"role"`
			Host string `json:"host"`
		} `json:"share"`
	}
	if json.Unmarshal(b, &v) != nil || v.Share == nil {
		return ""
	}
	return v.Share.Role + ">" + v.Share.Host
}
