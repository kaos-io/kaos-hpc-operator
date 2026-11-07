package follower

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func testConfig() Config {
	return Config{Namespace: "ns", WorkflowName: "wf-1", WorkflowUID: "uid-1", StepName: "train", NodeKey: "wf-1-train",
		Image: "img:1", Command: []string{"python", "train.py"}, WallTimeMinutes: 60, Class: "meluxina-gpu",
		Outputs: []string{"model", "metrics"}}
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

type harness struct {
	t      *testing.T
	c      client.WithWatch
	out    *bytes.Buffer
	opts   Options
	creats atomic.Int32
}

func newHarness(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *harness {
	t.Helper()
	h := &harness{t: t, out: &bytes.Buffer{}}
	create := funcs.Create
	funcs.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.CreateOption) error {
		h.creats.Add(1)
		if create != nil {
			return create(ctx, c, obj, o...)
		}
		return c.Create(ctx, obj, o...)
	}
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&hpcv1alpha1.HPCArgoWorkflowJob{}).Build()
	h.c = interceptor.NewClient(base, funcs)
	dir := t.TempDir()
	h.opts = Options{Poll: 5 * time.Millisecond, Recheck: 10 * time.Millisecond,
		OutputsDir: filepath.Join(dir, "outputs"), SlurmIDPath: filepath.Join(dir, "slurm-job-id"), Out: h.out}
	return h
}

// setStatus moves the step's job to the given status as the controller would.
func (h *harness) setStatus(cfg Config, st hpcv1alpha1.HPCArgoWorkflowJobStatus) {
	h.t.Helper()
	var job hpcv1alpha1.HPCArgoWorkflowJob
	key := client.ObjectKey{Namespace: cfg.Namespace, Name: JobName(cfg.WorkflowUID, cfg.NodeKey)}
	deadline := time.Now().Add(2 * time.Second)
	for h.c.Get(context.Background(), key, &job) != nil {
		if time.Now().After(deadline) {
			h.t.Error("the job was never created")
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	job.Status = st
	if err := h.c.Status().Update(context.Background(), &job); err != nil {
		h.t.Error(err)
	}
}

func (h *harness) run(ctx context.Context, cfg Config) int { return Run(ctx, h.c, cfg, h.opts) }

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestJobNameIsDeterministicAndPerNode(t *testing.T) {
	a, b := JobName("uid-1", "wf-train"), JobName("uid-1", "wf-train")
	if a != b || !strings.HasPrefix(a, "hpc-") || len(a) != 4+20 {
		t.Fatalf("name %q", a)
	}
	if a == JobName("uid-2", "wf-train") || a == JobName("uid-1", "wf-eval") {
		t.Fatal("different workflow or node must give a different name")
	}
}

func TestShouldCreateTheJobOwnedByTheWorkflowAndWriteOutputsOnSuccess(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, interceptor.Funcs{})
	go func() {
		h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Reason: "Submitted",
			Run: &hpcv1alpha1.RunStatus{SiteJobID: "4242"}, PendingReason: "Priority"})
		time.Sleep(30 * time.Millisecond)
		h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded, Reason: "Completed",
			Run: &hpcv1alpha1.RunStatus{SiteJobID: "4242"}, Outputs: map[string]string{"model": `{"uri":"lakefs://x"}`}})
	}()
	if code := h.run(context.Background(), cfg); code != ExitSucceeded {
		t.Fatalf("exit %d, output:\n%s", code, h.out)
	}
	var job hpcv1alpha1.HPCArgoWorkflowJob
	if err := h.c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: JobName("uid-1", "wf-1-train")}, &job); err != nil {
		t.Fatal(err)
	}
	ref := job.OwnerReferences
	if len(ref) != 1 || ref[0].Kind != "Workflow" || ref[0].APIVersion != "argoproj.io/v1alpha1" || ref[0].Name != "wf-1" ||
		string(ref[0].UID) != "uid-1" || *ref[0].Controller || *ref[0].BlockOwnerDeletion {
		t.Fatalf("owner reference %+v", ref)
	}
	if job.Spec.StepName != "train" || job.Spec.Workflow.UID != "uid-1" || job.Spec.Class != "meluxina-gpu" {
		t.Fatalf("spec %+v", job.Spec)
	}
	if got := read(t, filepath.Join(h.opts.OutputsDir, "model.json")); got != `{"uri":"lakefs://x"}` {
		t.Errorf("model.json = %q", got)
	}
	if got := read(t, filepath.Join(h.opts.OutputsDir, "metrics.json")); got != "{}" {
		t.Errorf("a declared output the runner did not fill must be {}, got %q", got)
	}
	if got := read(t, h.opts.SlurmIDPath); got != "4242" {
		t.Errorf("slurm-job-id = %q", got)
	}
	for _, want := range []string{"created", "site job id: 4242", "phase=Running", `pending="Priority"`, "the job succeeded"} {
		if !strings.Contains(h.out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, h.out)
		}
	}
}

func TestShouldReattachToTheExistingJobWithoutCreatingAnother(t *testing.T) {
	cfg := testConfig()
	existing := cfg.Job()
	existing.Status = hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded, Reason: "Completed"}
	h := newHarness(t, interceptor.Funcs{}, existing)
	if code := h.run(context.Background(), cfg); code != ExitSucceeded {
		t.Fatalf("exit %d:\n%s", code, h.out)
	}
	if !strings.Contains(h.out.String(), "re-attached") {
		t.Errorf("want re-attached:\n%s", h.out)
	}
	var list hpcv1alpha1.HPCArgoWorkflowJobList
	if err := h.c.List(context.Background(), &list); err != nil || len(list.Items) != 1 {
		t.Fatalf("want exactly one job, got %d (%v)", len(list.Items), err)
	}
}

func TestShouldFailWhenTheExistingJobHasAnotherSpec(t *testing.T) {
	cfg := testConfig()
	existing := cfg.Job()
	existing.Spec.Image = "other:2"
	h := newHarness(t, interceptor.Funcs{}, existing)
	if code := h.run(context.Background(), cfg); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(h.out.String(), "different spec") {
		t.Errorf("output:\n%s", h.out)
	}
}

func TestShouldExitOneAndStillWriteDefaultsWhenTheJobFails(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, interceptor.Funcs{})
	go h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseFailed, Reason: "RunnerFailed", Message: "pod oom"})
	if code := h.run(context.Background(), cfg); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(h.out.String(), "pod oom") {
		t.Errorf("the failure message must be printed:\n%s", h.out)
	}
	if got := read(t, filepath.Join(h.opts.OutputsDir, "model.json")); got != "{}" {
		t.Errorf("model.json = %q", got)
	}
	if read(t, h.opts.SlurmIDPath) != "" {
		t.Error("no site job id expected")
	}
}

func TestShouldExitOneOnRefused(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, interceptor.Funcs{})
	go h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRefused, Reason: "BudgetBlocked"})
	if code := h.run(context.Background(), cfg); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
}

func TestShouldSurviveAPIBlipsOnCreateAndGet(t *testing.T) {
	cfg := testConfig()
	var creates, gets atomic.Int32
	blip := apierrors.NewServiceUnavailable("etcd leader change")
	h := newHarness(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.CreateOption) error {
			if creates.Add(1) <= 2 {
				return blip
			}
			return c.Create(ctx, obj, o...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, o ...client.GetOption) error {
			if _, ok := obj.(*hpcv1alpha1.HPCArgoWorkflowJob); ok && gets.Add(1) <= 3 {
				return errors.New("connection refused")
			}
			return c.Get(ctx, key, obj, o...)
		},
	})
	go h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded})
	if code := h.run(context.Background(), cfg); code != ExitSucceeded {
		t.Fatalf("exit %d, blips must not fail the step:\n%s", code, h.out)
	}
	if !strings.Contains(h.out.String(), "API error, retrying") {
		t.Errorf("blips should be printed:\n%s", h.out)
	}
}

func TestShouldFailImmediatelyWhenTheCreateIsForbidden(t *testing.T) {
	h := newHarness(t, interceptor.Funcs{
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return apierrors.NewForbidden(schema.GroupResource{Group: "hpc.kaos.io", Resource: "hpcargoworkflowjobs"}, "x", errors.New("no"))
		},
	})
	if code := h.run(context.Background(), testConfig()); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	if h.creats.Load() != 1 {
		t.Errorf("a forbidden create must not be retried, got %d creates", h.creats.Load())
	}
}

func TestShouldExitOneAndKeepTheJobWhenStopped(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, interceptor.Funcs{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(40 * time.Millisecond); cancel() }()
	if code := h.run(ctx, cfg); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	var job hpcv1alpha1.HPCArgoWorkflowJob
	if err := h.c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: JobName("uid-1", "wf-1-train")}, &job); err != nil {
		t.Fatalf("the follower must never delete the job: %v", err)
	}
}

func TestShouldExitOneWhenTheJobDisappears(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, interceptor.Funcs{})
	go func() {
		h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning})
		var job hpcv1alpha1.HPCArgoWorkflowJob
		_ = h.c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: JobName("uid-1", "wf-1-train")}, &job)
		_ = h.c.Delete(context.Background(), &job)
	}()
	if code := h.run(context.Background(), cfg); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
}

func TestConfigFromEnv(t *testing.T) {
	base := map[string]string{"HPC_JOB_NAMESPACE": "ns", "WF_NAME": "wf", "WF_UID": "u", "STEP_NAME": "s", "NODE_KEY": "n",
		"IMAGE_REF": "img", "HPC_CLASS": "c", "SLURM_TIME_LIMIT": "90"}
	with := func(kv ...string) func(string) string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return func(k string) string { return m[k] }
	}
	cases := []struct {
		name    string
		get     func(string) string
		wantErr string
		check   func(t *testing.T, c Config)
	}{
		{"should_read_a_minimal_environment", with(), "", func(t *testing.T, c Config) {
			if c.WallTimeMinutes != 90 || c.ResumableLead != 0 || c.Job().Spec.Resumable != nil || c.Job().Spec.Sharing != nil {
				t.Errorf("%+v", c)
			}
		}},
		{"should_opt_out_of_resuming_when_resumable_is_false", with("HPC_RESUMABLE", "False"), "", func(t *testing.T, c Config) {
			if !c.ResumableOptOut || !c.Job().Spec.ResumableOptOut {
				t.Errorf("resumable=False must set spec.resumableOptOut: %+v", c)
			}
		}},
		{"should_read_resumable_sharing_outputs_command_and_allowlisted_env",
			with("HPC_RESUMABLE_LEAD", "20", "HPC_MAX_CONTINUATIONS", "3", "HPC_SCOPE", "abcdef123456", "STEP_OUTPUTS", "a, b,",
				"STEP_COMMAND", `["x","y"]`, "OUTPUTS_REPO", "outputs-app", "HOME", "/root", "KAOS_LIB_SUPPORT", "{}"), "",
			func(t *testing.T, c Config) {
				s := c.Job().Spec
				if s.Resumable.LeadMinutes != 20 || s.Resumable.MaxContinuations != 3 || s.Sharing.Scope != "abcdef123456" ||
					len(s.Outputs) != 2 || len(s.Command) != 2 || len(s.Env) != 2 {
					t.Errorf("%+v", s)
				}
			}},
		{"should_fail_when_a_required_variable_is_missing", with("NODE_KEY", ""), "NODE_KEY", nil},
		{"should_fail_on_a_bad_wall_time", with("SLURM_TIME_LIMIT", "abc"), "SLURM_TIME_LIMIT", nil},
		{"should_fail_on_a_command_that_is_not_json", with("STEP_COMMAND", "python x"), "STEP_COMMAND", nil},
		{"should_fail_on_sharing_without_a_scope", with("HPC_SHARING", "1"), "HPC_SCOPE", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := ConfigFromEnv(tc.get)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, c)
		})
	}
}
