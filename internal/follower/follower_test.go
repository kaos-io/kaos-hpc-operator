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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
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
		{"should_leave_the_idle_window_to_the_platform_when_unset", with(), "", func(t *testing.T, c Config) {
			if c.Job().Spec.IdleTimeoutMinutes != nil {
				t.Errorf("idleTimeoutMinutes = %v", *c.Job().Spec.IdleTimeoutMinutes)
			}
		}},
		{"should_carry_a_step_idle_timeout_to_the_job", with("HPC_IDLE_TIMEOUT_MINUTES", "45"), "", func(t *testing.T, c Config) {
			if got := c.Job().Spec.IdleTimeoutMinutes; got == nil || *got != 45 {
				t.Errorf("idleTimeoutMinutes = %v", got)
			}
		}},
		{"should_carry_idle_timeout_zero_as_off", with("HPC_IDLE_TIMEOUT_MINUTES", "0"), "", func(t *testing.T, c Config) {
			if got := c.Job().Spec.IdleTimeoutMinutes; got == nil || *got != 0 {
				t.Errorf("idleTimeoutMinutes = %v", got)
			}
		}},
		{"should_fail_on_a_bad_idle_timeout", with("HPC_IDLE_TIMEOUT_MINUTES", "soon"), "HPC_IDLE_TIMEOUT_MINUTES", nil},
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

// podEnv is what the hpc-follower template renders into every pod of the twin; a retried pod gets the
// same values (only Argo's node name and the pod name differ, and neither is in the environment).
func podEnv(extra map[string]string) func(string) string {
	m := map[string]string{
		"HPC_JOB_NAMESPACE": "ns", "WF_NAME": "wf-1", "WF_UID": "uid-1", "STEP_NAME": "train", "NODE_KEY": "train-meluxina",
		"IMAGE_REF": "img:1", "STEP_COMMAND": `["python","train.py"]`, "SLURM_TIME_LIMIT": "60", "HPC_CLASS": "meluxina-gpu",
		"HPC_RESUMABLE_LEAD": "0", "HPC_RESUMABLE": "auto", "HPC_IDLE_TIMEOUT_MINUTES": "", "HPC_MAX_CONTINUATIONS": "5",
		"STEP_OUTPUTS": "model,metrics", "DATASET_PIN": "", "OUTPUTS_REPO": "outputs-app", "OUTPUTS_REF": "main",
		"CHECKPOINT_BUCKET": "outputs-app", "CHECKPOINT_PREFIX": "main/checkpoints",
	}
	for k, v := range extra {
		m[k] = v
	}
	return func(k string) string { return m[k] }
}

func TestShouldBuildTheSameJobOnEveryPodAttemptOfTheStep(t *testing.T) {
	cases := map[string]map[string]string{
		"should_build_the_same_job_for_a_default_step":     nil,
		"should_build_the_same_job_for_a_resumable_step":   {"HPC_RESUMABLE_LEAD": "15"},
		"should_build_the_same_job_for_a_sharing_step":     {"HPC_SHARING": "on", "HPC_SCOPE": "abcdef12"},
		"should_build_the_same_job_when_idle_timeout_is_0": {"HPC_IDLE_TIMEOUT_MINUTES": "0"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			first, err := ConfigFromEnv(podEnv(extra))
			if err != nil {
				t.Fatal(err)
			}
			retry, err := ConfigFromEnv(podEnv(extra))
			if err != nil {
				t.Fatal(err)
			}
			if a, b := first.Job(), retry.Job(); a.Name != b.Name || !equality.Semantic.DeepEqual(a.Spec, b.Spec) {
				t.Fatalf("two pods of one step build different jobs:\n%+v\n%+v", a, b)
			}
		})
	}
}

// The API server stores the job with the CRD defaults (idleTimeoutMinutes=20, nodes=1, resumable
// lead/cap); the retried pod's spec is raw. Only a real difference may refuse.
func TestShouldReattachToTheJobAsTheAPIServerStoredItAndRefuseRealConflicts(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*hpcv1alpha1.HPCArgoWorkflowJob)
		refused bool
	}{
		{"should_reattach_to_the_server_defaulted_copy", func(*hpcv1alpha1.HPCArgoWorkflowJob) {}, false},
		{"should_refuse_another_step", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.StepName = "eval" }, true},
		{"should_refuse_another_image", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "other:2" }, true},
		{"should_refuse_another_class", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Class = "meluxina-cpu" }, true},
		{"should_refuse_another_idle_timeout", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.IdleTimeoutMinutes = ptr.To(int32(5)) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			existing := cfg.Job()
			existing.Spec = withCRDDefaults(existing.Spec)
			tc.mutate(existing)
			existing.Status = hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded, Reason: "Completed"}
			h := newHarness(t, interceptor.Funcs{}, existing)
			code := h.run(context.Background(), cfg)
			if tc.refused && (code != ExitFailed || !strings.Contains(h.out.String(), "different spec")) {
				t.Fatalf("want a refusal, exit %d:\n%s", code, h.out)
			}
			if !tc.refused && (code != ExitSucceeded || !strings.Contains(h.out.String(), "re-attached")) {
				t.Fatalf("want a re-attach, exit %d:\n%s", code, h.out)
			}
		})
	}
}

func TestTheJobsDatasetIsExactlyWhatTheComposeStepPinned(t *testing.T) {
	commit := strings.Repeat("c2", 32)
	pin := `{"repo":"dataset-a","version":"latest","commit":"` + commit + `","owner":""}`
	cfg, err := ConfigFromEnv(podEnv(map[string]string{"DATASET_PIN": pin}))
	if err != nil {
		t.Fatal(err)
	}
	if d := cfg.Job().Spec.Dataset; d == nil || d.Repo != "dataset-a" || d.Version != "latest" || d.Commit != commit || d.Owner != "" {
		t.Errorf("spec.dataset = %+v", d)
	}
	if cfg.Job().Spec.DatasetRef != "" {
		t.Error("the deprecated datasetRef is never written")
	}
	branch := `{"repo":"dataset-a","version":"feature/x","commit":"` + commit + `","owner":""}`
	if cfg, err = ConfigFromEnv(podEnv(map[string]string{"DATASET_PIN": branch})); err != nil || cfg.Dataset.Version != "feature/x" {
		t.Errorf("a branch as version is fine: %+v, %v", cfg.Dataset, err)
	}
	for _, none := range []string{"", "  \n"} {
		if cfg, err = ConfigFromEnv(podEnv(map[string]string{"DATASET_PIN": none})); err != nil || cfg.Job().Spec.Dataset != nil {
			t.Errorf("no dataset: no pin and no error, got %+v, %v", cfg.Dataset, err)
		}
	}
}

// Nothing but a complete pin of the project's own dataset becomes a job: every other value fails the step
// with a reason, and never yields a job without its data.
func TestAPinThatIsNotCompleteAndOwnIsRefused(t *testing.T) {
	commit := strings.Repeat("c2", 32)
	for name, pin := range map[string]string{
		"not json":         "{",
		"an array":         "[]",
		"two values":       `{"repo":"dataset-a","version":"v1","commit":"` + commit + `"} {}`,
		"unknown field":    `{"repo":"dataset-a","version":"v1","commit":"` + commit + `","endpoint":"https://evil.example"}`,
		"no commit":        `{"repo":"dataset-a","version":"v1"}`,
		"short commit":     `{"repo":"dataset-a","version":"v1","commit":"abc123"}`,
		"upper commit":     `{"repo":"dataset-a","version":"v1","commit":"` + strings.ToUpper(commit) + `"}`,
		"no repo":          `{"version":"v1","commit":"` + commit + `"}`,
		"one-letter repo":  `{"repo":"x","version":"v1","commit":"` + commit + `"}`,
		"not a dataset":    `{"repo":"models-v2","version":"v1","commit":"` + commit + `"}`,
		"outputs repo":     `{"repo":"outputs-app","version":"main","commit":"` + commit + `"}`,
		"bare prefix":      `{"repo":"dataset-","version":"v1","commit":"` + commit + `"}`,
		"repo with slash":  `{"repo":"a/b","version":"v1","commit":"` + commit + `"}`,
		"repo upper case":  `{"repo":"Dataset-A","version":"v1","commit":"` + commit + `"}`,
		"no version":       `{"repo":"dataset-a","commit":"` + commit + `"}`,
		"version option":   `{"repo":"dataset-a","version":"--base=/etc","commit":"` + commit + `"}`,
		"version shell":    `{"repo":"dataset-a","version":"x; rm -rf /","commit":"` + commit + `"}`,
		"version dot-dot":  `{"repo":"dataset-a","version":"a/../b","commit":"` + commit + `"}`,
		"version slashes":  `{"repo":"dataset-a","version":"a//b","commit":"` + commit + `"}`,
		"version trailing": `{"repo":"dataset-a","version":"a/","commit":"` + commit + `"}`,
		"version too long": `{"repo":"dataset-a","version":"` + strings.Repeat("v", 256) + `","commit":"` + commit + `"}`,
		"version subshell": `{"repo":"dataset-a","version":"$(id)","commit":"` + commit + `"}`,
		"shared dataset":   `{"repo":"dataset-a","version":"v1","commit":"` + commit + `","owner":"other"}`,
	} {
		cfg, err := ConfigFromEnv(podEnv(map[string]string{"DATASET_PIN": pin}))
		if err == nil {
			t.Errorf("%s: expected an error, got dataset %+v", name, cfg.Dataset)
		}
	}
	_, err := ConfigFromEnv(podEnv(map[string]string{"DATASET_PIN": `{"repo":"dataset-a","version":"v1","commit":"` + commit + `","owner":"other"}`}))
	if err == nil || !strings.Contains(err.Error(), "cannot be read on HPC yet") {
		t.Errorf("a shared dataset is refused with what to do instead, got %v", err)
	}
}

// A pipeline released before the run's data was pinned names a dataset and no commit: the step fails and
// says the pipeline is being re-released, instead of creating a job without data.
func TestAPipelineReleasedBeforeThePinIsRefused(t *testing.T) {
	_, err := ConfigFromEnv(podEnv(map[string]string{"DATASET": "dataset-a@latest"}))
	if err == nil || !strings.Contains(err.Error(), "released before the platform update") {
		t.Errorf("got %v", err)
	}
}

// A cluster whose CRD is older than this follower would drop spec.dataset and run the step without its data.
// The follower creates strictly (such a cluster refuses the create), and when a server stores the job without
// the dataset anyway, the follower removes the job and fails the step.
func TestAJobStoredWithoutItsDatasetIsRemovedAndTheStepFails(t *testing.T) {
	pin := `{"repo":"dataset-a","version":"latest","commit":"` + strings.Repeat("c2", 32) + `","owner":""}`
	cfg, err := ConfigFromEnv(podEnv(map[string]string{"DATASET_PIN": pin}))
	if err != nil {
		t.Fatal(err)
	}
	strict := false
	h := newHarness(t, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.CreateOption) error {
		opts := &client.CreateOptions{}
		opts.ApplyOptions(o)
		strict = opts.FieldValidation == "Strict"
		obj.(*hpcv1alpha1.HPCArgoWorkflowJob).Spec.Dataset = nil // what an older CRD does to the field
		return c.Create(ctx, obj, o...)
	}})
	if code := h.run(context.Background(), cfg); code != ExitFailed {
		t.Fatalf("exit %d, want the step to fail", code)
	}
	if !strict {
		t.Error("the job must be created with strict field validation")
	}
	if !strings.Contains(h.out.String(), "without its dataset dataset-a@latest") {
		t.Errorf("the reason must be printed:\n%s", h.out)
	}
	var left hpcv1alpha1.HPCArgoWorkflowJob
	key := client.ObjectKey{Namespace: cfg.Namespace, Name: JobName(cfg.WorkflowUID, cfg.NodeKey)}
	if err := h.c.Get(context.Background(), key, &left); !apierrors.IsNotFound(err) {
		t.Errorf("the job without its data must be gone, got err=%v", err)
	}
}
