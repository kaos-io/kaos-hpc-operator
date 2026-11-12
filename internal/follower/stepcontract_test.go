package follower

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// The step contract's fixtures (docs/step-contract/fixtures): loaded, never copied (PRD kaos#1911 CON-01).
const fixtureDir = "../../docs/step-contract/fixtures"

func fixture(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(filepath.FromSlash(fixtureDir), name))
	if err != nil {
		t.Fatalf("the step contract's fixtures are part of this repository: %v", err)
	}
	return string(raw)
}

func withContext(t *testing.T, wire string) Config {
	t.Helper()
	cfg, err := ConfigFromEnv(podEnv(map[string]string{"KAOS_STEP_CONTEXT": wire}))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The job the follower builds from the fixture context is the fixture job: spec.context is the text it was
// handed, byte for byte; spec.app and spec.inputs are read from it and from nothing else.
func TestTheJobIsBuiltFromTheContextAndFromNothingElse(t *testing.T) {
	var want struct {
		Spec struct {
			App     string                  `json:"app"`
			Context string                  `json:"context"`
			Inputs  []hpcv1alpha1.StepInput `json:"inputs"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(fixture(t, "hpc-job.json")), &want); err != nil {
		t.Fatal(err)
	}
	wire := fixture(t, "context-one-upstream.wire")
	job := withContext(t, wire).Job()
	if job.Spec.Context != wire || job.Spec.Context != want.Spec.Context {
		t.Errorf("spec.context is not the text the follower was handed")
	}
	if job.Spec.App != want.Spec.App {
		t.Errorf("spec.app = %q, want %q", job.Spec.App, want.Spec.App)
	}
	if !reflect.DeepEqual(job.Spec.Inputs, want.Spec.Inputs) {
		t.Errorf("spec.inputs = %+v\nwant          %+v", job.Spec.Inputs, want.Spec.Inputs)
	}
	if job.Labels[hpcv1alpha1.StepKindLabel] != testConfig().Kind { // the fixture context's kind
		t.Errorf("the job must be labelled with the step's kind, got %v", job.Labels)
	}
}

func TestEveryFixtureContextIsCarriedUnchanged(t *testing.T) {
	inputs := map[string][]string{
		"context-no-upstream":      {"raw"},
		"context-promote":          {"raw"},
		"context-one-upstream":     {"labels", "embeddings"},
		"context-skipped-upstream": nil, // evaluate declares no input; train wrote a model, export was skipped
		"context-escapes":          nil,
	}
	for name, wantInputs := range inputs {
		t.Run(name, func(t *testing.T) {
			wire := fixture(t, name+".wire")
			cfg := withContext(t, wire)
			if cfg.Context != wire {
				t.Fatalf("the context was changed on its way into the job")
			}
			var names []string
			for _, in := range cfg.Inputs {
				names = append(names, in.Name)
			}
			if !reflect.DeepEqual(names, wantInputs) {
				t.Errorf("inputs %v, want %v", names, wantInputs)
			}
		})
	}
}

// The cost of a value is its length as a JSON string (section 7): for a value in wire form, its bytes plus
// one for each quote and each backslash.
func TestCostIsTheLengthAsAJSONString(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(filepath.FromSlash(fixtureDir), "*.wire"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no .wire fixtures: %v", err)
	}
	for _, f := range files {
		wire := fixture(t, filepath.Base(f))
		want := len(wire) + strings.Count(wire, `"`) + strings.Count(wire, `\`)
		if got := Cost(wire); got != want {
			t.Errorf("%s: cost %d, want %d", filepath.Base(f), got, want)
		}
	}
}

// contextCosting is a step context whose cost is exactly cost.
func contextCosting(t *testing.T, cost int) string {
	t.Helper()
	head := `{"version":1,"run":"r","app":"hera-demo","project":"p","step":"s","kind":"train","experiment":"e","params":{"blob":"`
	tail := `"},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{}}`
	fill := cost - Cost(head+tail)
	if fill < 0 {
		t.Fatalf("cost %d is too small", cost)
	}
	text := head + strings.Repeat("x", fill) + tail
	if Cost(text) != cost {
		t.Fatalf("built a context costing %d, want %d", Cost(text), cost)
	}
	return text
}

func TestAContextOverTheCapIsRefusedByCost(t *testing.T) {
	atCap := contextCosting(t, hpcv1alpha1.ContextMaxLength)
	if len(atCap) >= hpcv1alpha1.ContextMaxLength {
		t.Fatalf("the test context must be shorter than its cost (%d bytes)", len(atCap))
	}
	if cfg := withContext(t, atCap); cfg.Context != atCap {
		t.Fatal("a context at the cap must be carried unchanged")
	}
	over := contextCosting(t, hpcv1alpha1.ContextMaxLength+1)
	_, err := ConfigFromEnv(podEnv(map[string]string{"KAOS_STEP_CONTEXT": over}))
	if err == nil || !strings.Contains(err.Error(), "costs 98305 bytes") || !strings.Contains(err.Error(), "98304") {
		t.Fatalf("a context one byte over the cap by cost (%d bytes long) must be refused naming both, got %v", len(over), err)
	}
}

func contextWith(reads, upstream string) string {
	return `{"version":1,"run":"r","app":"hera-demo","project":"p","step":"s","kind":"train","experiment":"e","params":{},` +
		`"ledger":{"version":2,"reads":[` + reads + `],"writes":[]},"produces":[],"upstream":{` + upstream + `}}`
}

func read1(name, repo, owner, commit string) string {
	return `{"name":"` + name + `","repo":"` + repo + `","owner":` + owner + `,"ref":"latest","commit":"` + commit + `"}`
}

// A shared dataset (a read with an owner) reaches the follower inside the context. It cannot be staged on an
// HPC site yet: the step fails saying what to do instead, and no job is created.
func TestASharedDatasetIsRefusedWithWhatToDoInstead(t *testing.T) {
	commit := strings.Repeat("c2", 32)
	_, err := ConfigFromEnv(podEnv(map[string]string{"KAOS_STEP_CONTEXT": contextWith(read1("raw", "dataset-raw", `"other"`, commit), "")}))
	if err == nil || !strings.Contains(err.Error(), "shared by project other") || !strings.Contains(err.Error(), "cannot be read on HPC yet") {
		t.Fatalf("got %v", err)
	}
	if cfg := withContext(t, contextWith(read1("raw", "dataset-raw", "null", commit), "")); len(cfg.Inputs) != 1 || cfg.Inputs[0].Owner != "" {
		t.Fatalf("an own dataset (owner null) is an input without an owner: %+v", cfg.Inputs)
	}
}

// Nothing but a pinned dataset becomes an input: every other entry fails the step with a reason, and never
// yields a job without its data.
func TestAContextThatCannotBeStagedIsRefused(t *testing.T) {
	commit := strings.Repeat("c2", 32)
	var many []string
	for i := 0; i < 17; i++ {
		many = append(many, read1("in"+string(rune('a'+i)), "dataset-raw", "null", commit))
	}
	for name, bad := range map[string]string{
		"not json":           "{",
		"an array":           "[]",
		"no app":             `{"version":1,"kind":"train","ledger":{"reads":[]},"upstream":{}}`,
		"no kind":            `{"version":1,"app":"a","ledger":{"reads":[]},"upstream":{}}`,
		"no ledger":          `{"version":1,"app":"a","kind":"train","upstream":{}}`,
		"no commit":          contextWith(`{"name":"raw","repo":"dataset-raw","owner":null,"ref":"latest"}`, ""),
		"short commit":       contextWith(read1("raw", "dataset-raw", "null", "abc123"), ""),
		"not a dataset":      contextWith(read1("raw", "outputs-app", "null", commit), ""),
		"name with a slash":  contextWith(read1("../raw", "dataset-raw", "null", commit), ""),
		"name with a space":  contextWith(read1("raw x", "dataset-raw", "null", commit), ""),
		"name too long":      contextWith(read1(strings.Repeat("n", 41), "dataset-raw", "null", commit), ""),
		"the same name":      contextWith(read1("raw", "dataset-raw", "null", commit)+","+read1("raw", "dataset-b", "null", commit), ""),
		"seventeen inputs":   contextWith(strings.Join(many, ","), ""),
		"upstream no commit": contextWith("", `"prepare":{"version":1,"status":"ok","values":{},"reads":[],"writes":[{"name":"clean","type":"dataset","repo":"dataset-clean"}]}`),
	} {
		if cfg, err := ConfigFromEnv(podEnv(map[string]string{"KAOS_STEP_CONTEXT": bad})); err == nil {
			t.Errorf("%s: expected an error, got inputs %+v", name, cfg.Inputs)
		}
	}
}

// A model or an artifact a needed step wrote is not staged: only its datasets are. A step that was skipped
// (the sentinel) adds nothing.
func TestOnlyDatasetsOfNeededStepsAreInputs(t *testing.T) {
	commit := strings.Repeat("ab", 32)
	up := `"b":{"version":1,"status":"absent"},` +
		`"a":{"version":1,"status":"ok","values":{},"reads":[],"writes":[` +
		`{"name":"model","type":"model","repo":"outputs-app","commit":"` + commit + `"},` +
		`{"name":"clean","type":"dataset","repo":"dataset-clean","commit":"` + commit + `"}]}`
	cfg := withContext(t, contextWith("", up))
	want := []hpcv1alpha1.StepInput{{Name: "clean", Repo: "dataset-clean", Commit: commit}}
	if !reflect.DeepEqual(cfg.Inputs, want) {
		t.Fatalf("inputs %+v, want %+v", cfg.Inputs, want)
	}
}

func succeeded(result string) hpcv1alpha1.HPCArgoWorkflowJobStatus {
	return hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded, Reason: "Completed",
		Run: &hpcv1alpha1.RunStatus{SiteJobID: "77"}, Result: result}
}

// PRD kaos#1911 CON-04, X-2. Before: every declared output the job did not fill was written as "{}", also for
// a failed or vanished job (run.go writeOutputs), so the next step got an empty input and the run stayed green.
func TestOnlyASucceededJobHandsOnAResult(t *testing.T) {
	t.Run("should_hand_on_the_result_of_a_succeeded_job_unchanged", func(t *testing.T) {
		cfg, h := testConfig(), newHarness(t, interceptor.Funcs{})
		go h.setStatus(cfg, succeeded(fixture(t, "result-ok.wire")))
		if code := h.run(context.Background(), cfg); code != ExitSucceeded {
			t.Fatalf("exit %d:\n%s", code, h.out)
		}
		if got := read(t, h.opts.ResultPath); got != fixture(t, "result-ok.wire") {
			t.Errorf("kaos-result.json = %q", got)
		}
	})
	t.Run("should_fail_the_step_when_a_succeeded_job_carries_no_result", func(t *testing.T) {
		// The runner fails a job whose step wrote no result; should one still succeed without, nothing stands in.
		cfg, h := testConfig(), newHarness(t, interceptor.Funcs{})
		go h.setStatus(cfg, succeeded(""))
		if code := h.run(context.Background(), cfg); code != ExitFailed {
			t.Fatalf("exit %d:\n%s", code, h.out)
		}
		if _, err := os.Stat(h.opts.ResultPath); !os.IsNotExist(err) {
			t.Errorf("no result file may exist (never the sentinel, never {}), got err=%v", err)
		}
		if !strings.Contains(h.out.String(), "carries no result") {
			t.Errorf("the cause must be printed:\n%s", h.out)
		}
	})
	for _, phase := range []hpcv1alpha1.Phase{hpcv1alpha1.PhaseFailed, hpcv1alpha1.PhaseCancelled, hpcv1alpha1.PhaseRefused} {
		t.Run("should_hand_on_nothing_when_the_job_ended_"+string(phase), func(t *testing.T) {
			cfg, h := testConfig(), newHarness(t, interceptor.Funcs{})
			// Even with a result in its status: only a job that Succeeded publishes one.
			go h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: phase, Reason: hpcv1alpha1.ReasonResultNotHandedBack,
				Message: "the result could not be fetched", Result: fixture(t, "result-ok.wire")})
			if code := h.run(context.Background(), cfg); code != ExitFailed {
				t.Fatalf("exit %d", code)
			}
			if _, err := os.Stat(h.opts.ResultPath); !os.IsNotExist(err) {
				t.Errorf("no result file may exist, got err=%v", err)
			}
			if !strings.Contains(h.out.String(), "ResultNotHandedBack") || !strings.Contains(h.out.String(), "the result could not be fetched") {
				t.Errorf("the reason and the cause must be printed:\n%s", h.out)
			}
		})
	}
	t.Run("should_hand_on_nothing_when_the_job_is_gone", func(t *testing.T) {
		cfg, h := testConfig(), newHarness(t, interceptor.Funcs{})
		go func() {
			h.setStatus(cfg, hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning})
			var job hpcv1alpha1.HPCArgoWorkflowJob
			_ = h.c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: JobName("uid-1", "wf-1-train")}, &job)
			_ = h.c.Delete(context.Background(), &job)
		}()
		if code := h.run(context.Background(), cfg); code != ExitFailed {
			t.Fatalf("exit %d", code)
		}
		if _, err := os.Stat(h.opts.ResultPath); !os.IsNotExist(err) {
			t.Errorf("no result file may exist, got err=%v", err)
		}
	})
	t.Run("should_fail_the_step_when_the_result_cannot_be_written_for_the_workflow", func(t *testing.T) {
		cfg, h := testConfig(), newHarness(t, interceptor.Funcs{})
		blocker := filepath.Join(t.TempDir(), "a-file")
		if err := os.WriteFile(blocker, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		h.opts.ResultPath = filepath.Join(blocker, "kaos-result.json") // its folder cannot be made
		go h.setStatus(cfg, succeeded(fixture(t, "result-ok.wire")))
		if code := h.run(context.Background(), cfg); code != ExitFailed {
			t.Fatalf("a result that cannot be handed on must fail the step, exit %d:\n%s", code, h.out)
		}
		if !strings.Contains(h.out.String(), "cannot be handed on to the workflow") {
			t.Errorf("the cause must be printed:\n%s", h.out)
		}
	})
}

// A cluster whose job definition is older than this follower would drop spec.context and run the step without
// it. The follower creates strictly (such a cluster refuses the create), and when a server stores the job
// without the context anyway, the follower removes the job and fails the step.
func TestAJobStoredWithoutItsContextIsRemovedAndTheStepFails(t *testing.T) {
	cfg := withContext(t, fixture(t, "context-one-upstream.wire"))
	for name, drop := range map[string]func(*hpcv1alpha1.HPCArgoWorkflowJob){
		"the context": func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Context = "" },
		"the inputs":  func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Inputs = nil },
	} {
		t.Run(name, func(t *testing.T) {
			strict := false
			h := newHarness(t, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, o ...client.CreateOption) error {
				opts := &client.CreateOptions{}
				opts.ApplyOptions(o)
				strict = opts.FieldValidation == "Strict"
				drop(obj.(*hpcv1alpha1.HPCArgoWorkflowJob)) // what an older definition does to the field
				return c.Create(ctx, obj, o...)
			}})
			if code := h.run(context.Background(), cfg); code != ExitFailed {
				t.Fatalf("exit %d, want the step to fail", code)
			}
			if !strict {
				t.Error("the job must be created with strict field validation")
			}
			if !strings.Contains(h.out.String(), "without its step context or its inputs") {
				t.Errorf("the reason must be printed:\n%s", h.out)
			}
			var left hpcv1alpha1.HPCArgoWorkflowJob
			key := client.ObjectKey{Namespace: cfg.Namespace, Name: JobName(cfg.WorkflowUID, cfg.NodeKey)}
			if err := h.c.Get(context.Background(), key, &left); !apierrors.IsNotFound(err) {
				t.Errorf("the job without its context must be gone, got err=%v", err)
			}
		})
	}
}
