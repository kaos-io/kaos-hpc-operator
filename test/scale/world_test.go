//go:build scale

package scale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

// The world is everything one scenario runs against: an envtest API server, the fake Slurm sites,
// the fake kubelet (runner pods) and the REAL controller binary (cmd/main.go) as a child process, so
// its RSS, heap, restart and flags are those of production, not of a test double.

// budget of one project: share per tier in node-hours, and what the site reports left.
type budget struct{ gpuShare, cpuShare, gpuSiteLeft, cpuSiteLeft int64 }

// project is one KAOS project bound to one HPCSite on one fake Slurm site.
type project struct {
	name, envNS, hpcNS string
	user, account      string
	site               *fakeslurm.Site
	siteName           string // the site profile name: projects of one profile share a token bucket
	budget             budget
	maxConcurrent      int32
	coverage           bool // the monitor checks that used+booked never falls below what the site charged
	wiring             *siteWiring
	jobs               []string // names of the jobs created for it
}

type world struct {
	t      *testing.T
	env    *envtest.Environment
	cfg    *rest.Config
	c      client.WithWatch
	dir    string
	kcfg   string
	ctrl   *ctrlProc
	kube   *kubelet
	cancel context.CancelFunc
	ctx    context.Context
	wg     sync.WaitGroup

	// outageStart and outageEnd are when L-chaos took the shared site down and up.
	outageStart, outageEnd time.Time
	// outageNoticed is when the HPCSite's SiteReachable condition turned False.
	outageNoticed time.Time

	projects []*project
	sampler  *sampler
	mon      *monitor
	opts     ctrlOpts
}

var binDir = os.TempDir()

// binOnce builds the controller binary once per test run.
var (
	binOnce sync.Once
	binPath string
	binErr  error
)

func buildManager(dir string) (string, error) {
	binOnce.Do(func() {
		if bin := os.Getenv("SCALE_MANAGER_BIN"); bin != "" { // compare another build of the controller
			binPath = bin
			return
		}
		binPath = filepath.Join(dir, "manager")
		cmd := exec.Command("go", "build", "-o", binPath, "../../cmd")
		cmd.Env = os.Environ()
		if out, err := cmd.CombinedOutput(); err != nil {
			binErr = fmt.Errorf("go build ../../cmd: %v\n%s", err, out)
		}
	})
	return binPath, binErr
}

func scratchDir(t *testing.T) string {
	base := os.Getenv("SCALE_TMPDIR")
	if base == "" {
		base = os.TempDir()
	}
	d, err := os.MkdirTemp(base, "scale-"+strings.ReplaceAll(t.Name(), "/", "_")+"-")
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SCALE_KEEP") == "" {
		t.Cleanup(func() { _ = os.RemoveAll(d) })
	}
	return d
}

func newWorld(t *testing.T, opts ctrlOpts) *world {
	t.Helper()
	w := &world{t: t, opts: opts, dir: scratchDir(t)}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	if _, err := buildManager(binDir); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatal(err)
	}
	w.env = &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true}
	cfg, err := w.env.Start()
	if err != nil {
		t.Fatalf("envtest: %v (set KUBEBUILDER_ASSETS)", err)
	}
	w.cfg = cfg
	cfg2 := rest.CopyConfig(cfg)
	cfg2.QPS, cfg2.Burst = 2000, 4000 // the harness is not under test: it must never throttle
	if w.c, err = client.NewWithWatch(cfg2, client.Options{Scheme: scheme.Scheme}); err != nil {
		t.Fatal(err)
	}
	user, err := w.env.ControlPlane.AddUser(envtest.User{Name: "scale-controller", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	kc, err := user.KubeConfig()
	if err != nil {
		t.Fatal(err)
	}
	w.kcfg = filepath.Join(w.dir, "kubeconfig")
	if err := os.WriteFile(w.kcfg, kc, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	return w
}

func (w *world) close() {
	w.cancel()
	if w.ctrl != nil {
		w.ctrl.stop()
	}
	w.wg.Wait()
	for _, p := range w.projects {
		p.site.Close()
	}
	_ = w.env.Stop()
}

// ---- fixtures ---------------------------------------------------------------------------------

// siteOptions is the fake site every scenario starts from; scenarios override fields.
func siteOptions(name string, seed int64) fakeslurm.Options {
	return fakeslurm.Options{Name: name, Token: "tok-" + name, Seed: seed, Dilation: 60, Capacity: 60,
		QueueMin: 200 * time.Millisecond, QueueMax: 2 * time.Second, RunMin: 4, RunMax: 12,
		QOS: []fakeslurm.QOS{{Name: "default"}, {Name: "test", MaxWallMinutes: 30, MaxJobsPerUser: 1},
			{Name: "dev", MaxWallMinutes: 120, MaxJobsPerUser: 1}},
		Reservations: []string{"gpudev"}, AllocationNodeHours: map[string]float64{}}
}

// addProject creates a project's namespaces, token Secret and HPCSite against a fake site, and wires
// the fake kubelet to it. The site's budget is rendered like the hub renders it: share from the
// project, siteLeft from the site's own sshare.
func (w *world) addProject(name, siteName, user, account string, site *fakeslurm.Site, b budget, maxConcurrent int32) *project {
	w.t.Helper()
	p := &project{name: name, envNS: name + "-train", hpcNS: name + "-ml", user: user, account: account, site: site,
		siteName: siteName, budget: b, maxConcurrent: maxConcurrent}
	for _, ns := range []string{p.envNS, p.hpcNS} {
		w.must(w.c.Create(w.ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns,
			Labels: map[string]string{hpcv1alpha1.ProjectLabel: name}}}))
	}
	w.must(w.c.Create(w.ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: runnerjob.SecretJWT, Namespace: p.hpcNS},
		Data: map[string][]byte{"token": []byte(site.Opts.Token)}}))
	observed := metav1.Now()
	classes := []hpcv1alpha1.SiteClass{
		{Name: "hpc-gpu", Partition: "gpu", QOS: "default", Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 120,
			Lanes: []string{"default", "test", "dev"}},
		{Name: "hpc-cpu", Partition: "cpu", QOS: "default", Tier: hpcv1alpha1.TierCPU, MaxWallTimeMinutes: 120,
			Lanes: []string{"default", "test"}}}
	end := metav1.NewTime(time.Now().Add(48 * time.Hour))
	hs := &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "site", Namespace: p.hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: name}},
		Spec: hpcv1alpha1.HPCSiteSpec{Site: siteName,
			Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: site.URL(), RESTVersion: "v0.0.44",
				Classes: classes,
				QOS: []hpcv1alpha1.SiteQOS{{Name: "default", MaxWallTimeMinutes: 120},
					{Name: "test", MaxWallTimeMinutes: 30, MaxJobsPerUser: 1}, {Name: "dev", MaxWallTimeMinutes: 120, MaxJobsPerUser: 1}},
				Reservations: []hpcv1alpha1.SiteReservation{{Name: "gpudev", Partition: "gpu", NodeCount: 2, EndTime: &end}}},
			Identity: hpcv1alpha1.ProjectIdentity{User: user, Account: account,
				SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
			Budget: hpcv1alpha1.Budget{GPUNodeHours: b.gpuShare, CPUNodeHours: b.cpuShare,
				SiteLeft: &hpcv1alpha1.SiteLeft{GPUNodeHours: b.gpuSiteLeft, CPUNodeHours: b.cpuSiteLeft, ObservedAt: &observed}},
			Limits: hpcv1alpha1.Limits{MaxConcurrentJobs: maxConcurrent}},
	}
	w.must(w.c.Create(w.ctx, hs))
	cls := map[string]hpcv1alpha1.SiteClass{}
	for _, c := range classes {
		cls[c.Name] = c
	}
	p.wiring = &siteWiring{hpcNS: p.hpcNS, url: site.URL(), version: "v0.0.44", user: user, account: account,
		token: site.Opts.Token, classes: cls}
	w.projects = append(w.projects, p)
	return p
}

// enableSharing turns node sharing on for the project's HPCSite (spec.sharing): its steps that name a sharing scope
// are then planned as hosts or guests by the controller.
func (w *world) enableSharing(p *project) {
	w.t.Helper()
	var hs hpcv1alpha1.HPCSite
	if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: p.hpcNS, Name: "site"}, &hs); err != nil {
		w.t.Fatal(err)
	}
	hs.Spec.Sharing = true
	if err := w.c.Update(w.ctx, &hs); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) must(err error) {
	w.t.Helper()
	if err != nil && !apierrors.IsAlreadyExists(err) {
		w.t.Fatal(err)
	}
}

// jobSpec describes one job to create.
type jobSpec struct {
	class       string
	wallMinutes int32
	run         string // dilated run minutes (annotation), "" = the site's random
	forceEnd    string
	segments    int
	resumable   bool
	maxCont     int32  // continuations a resumable job may make (0: 5)
	hostTail    string // a node-sharing host: dilated minutes its Slurm job keeps the node after its step
	guest       bool   // a node-sharing guest: taken by a running host, never started on its own
	shareable   bool   // a step of the project's sharing scope; the controller plans it as a host or a guest (D-70)
}

// createJobs creates the jobs of a project with a bounded number of parallel writers.
func (w *world) createJobs(p *project, specs []jobSpec) {
	w.t.Helper()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 24)
	base := len(p.jobs)
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = fmt.Sprintf("j%05d", base+i)
		wg.Add(1)
		sem <- struct{}{}
		go func(name string, s jobSpec) {
			defer wg.Done()
			defer func() { <-sem }()
			j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: p.envNS, Annotations: map[string]string{}},
				Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: s.class, Image: "reg.example/app:v1", WallTimeMinutes: s.wallMinutes,
					Command: []string{"python", "train.py"}, StepName: "step-" + name}}
			if s.run != "" {
				j.Annotations[annRunMinutes] = s.run
			}
			if s.forceEnd != "" {
				j.Annotations[annForceEnd] = s.forceEnd
			}
			if s.segments > 0 {
				j.Annotations[annSegments] = fmt.Sprint(s.segments)
			}
			if s.resumable {
				j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 5, MaxContinuations: max(s.maxCont, 5)}
			}
			if s.hostTail != "" || s.guest || s.shareable {
				j.Spec.Sharing = &hpcv1alpha1.Sharing{Scope: "scalescope1"}
				j.Annotations[annHostTail] = s.hostTail
				if s.guest {
					j.Annotations[annGuest] = "1"
				}
			}
			w.must(w.c.Create(w.ctx, j))
		}(names[i], s)
	}
	wg.Wait()
	p.jobs = append(p.jobs, names...)
}

// ---- the controller process ---------------------------------------------------------------------

// ctrlOpts are the flags a scenario starts the controller with.
type ctrlOpts struct {
	pollInterval time.Duration
	restRate     float64
	restBurst    int
	lanes        string
}

type ctrlProc struct {
	w       *world
	cmd     *exec.Cmd
	port    int
	log     *os.File
	logPath string
	epoch   int // incremented per start: counters restart with the process
	exited  chan struct{}
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 18080
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// startController launches the real manager binary against the envtest API server.
func (w *world) startController() {
	w.t.Helper()
	if w.ctrl == nil {
		w.ctrl = &ctrlProc{w: w, logPath: filepath.Join(w.dir, "controller.log")}
	}
	cp := w.ctrl
	var err error
	if cp.log, err = os.OpenFile(cp.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err != nil {
		w.t.Fatal(err)
	}
	cp.port, cp.epoch = freePort(), cp.epoch+1
	args := []string{"--metrics-bind-address=127.0.0.1:" + fmt.Sprint(cp.port), "--metrics-secure=false",
		"--health-probe-bind-address=0", "--runner-image=fake-runner:test",
		"--site-poll-interval=" + w.opts.pollInterval.String(), fmt.Sprintf("--site-rest-rate=%g", w.opts.restRate),
		fmt.Sprintf("--site-rest-burst=%d", w.opts.restBurst), "--zap-log-level=info", "--zap-devel=false"}
	if w.opts.lanes != "" {
		args = append(args, "--lane-candidates="+w.opts.lanes)
	}
	cp.cmd = exec.Command(binPath, args...)
	cp.cmd.Env = append(os.Environ(), "KUBECONFIG="+w.kcfg, "SSL_CERT_FILE="+w.certFile())
	cp.cmd.Stdout, cp.cmd.Stderr = cp.log, cp.log
	if err := cp.cmd.Start(); err != nil {
		w.t.Fatal(err)
	}
	cp.exited = make(chan struct{})
	go func() { _ = cp.cmd.Wait(); close(cp.exited) }()
	w.waitMetrics(cp)
}

func (w *world) waitMetrics(cp *ctrlProc) {
	w.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := scrape(cp.port); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	w.t.Fatalf("controller did not serve metrics:\n%s", tailFile(cp.logPath, 3000))
}

func tailFile(path string, n int) string {
	b, _ := os.ReadFile(path)
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

// stop terminates the controller (SIGTERM, then SIGKILL after 30 s) and waits for it to exit.
func (cp *ctrlProc) stop() {
	if cp.cmd == nil || cp.cmd.Process == nil {
		return
	}
	select {
	case <-cp.exited:
	default:
		_ = cp.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-cp.exited:
		case <-time.After(30 * time.Second):
			_ = cp.cmd.Process.Kill()
			<-cp.exited
		}
	}
	_ = cp.log.Close()
}

// logCounts returns how many error lines and "client-side throttling" waits the log holds.
func (cp *ctrlProc) logCounts() (errs, throttled int) {
	b, _ := os.ReadFile(cp.logPath)
	for _, ln := range bytes.Split(b, []byte("\n")) {
		s := string(ln)
		switch {
		case strings.Contains(s, "client-side throttling"):
			throttled++
		case strings.Contains(s, `"level":"error"`) || strings.Contains(s, "ERROR"):
			errs++
		}
	}
	return
}

// errorSamples returns up to n distinct error lines of the controller log, shortened, for the report.
func (cp *ctrlProc) errorSamples(n int) []string {
	b, _ := os.ReadFile(cp.logPath)
	seen := map[string]bool{}
	var out []string
	for _, ln := range strings.Split(string(b), "\n") {
		if !strings.Contains(ln, `"level":"error"`) {
			continue
		}
		var m struct {
			Msg string `json:"msg"`
			Err string `json:"error"`
		}
		if json.Unmarshal([]byte(ln), &m) != nil {
			continue
		}
		k := m.Msg + ": " + m.Err
		if len(k) > 300 {
			k = k[:300]
		}
		if !seen[k] && len(out) < n {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// startKubelet starts the fake runner pods for every project of the world.
func (w *world) startKubelet() {
	wirings := make([]*siteWiring, 0, len(w.projects))
	for _, p := range w.projects {
		wirings = append(wirings, p.wiring)
	}
	w.kube = newKubelet(w.c, wirings, kubeletTick, w.opts.pollInterval)
	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.kube.run(w.ctx) }()
}

// kubeletTick is how often the fake runner pods look at the world (the runner's POLL_S, scaled down).
const kubeletTick = 500 * time.Millisecond

// certFile writes the fake sites' certificate (the httptest one, shared by all) for the controller process.
func (w *world) certFile() string {
	path := filepath.Join(w.dir, "sites.pem")
	if len(w.projects) > 0 {
		_ = os.WriteFile(path, w.projects[0].site.CertPEM(), 0o600)
	}
	return path
}
