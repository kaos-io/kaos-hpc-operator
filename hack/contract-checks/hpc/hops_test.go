//go:build contractcheck

package hpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/follower"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

// The caps of docs/step-contract.md, section 7.
const (
	contextCap = 96 * 1024
	resultCap  = 8 * 1024
)

const contextEnv = "KAOS_STEP_CONTEXT"

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

// largeContext is a step context in wire form of exactly size bytes. Its strings hold what a shell or
// a template engine could change: quotes, a dollar, backticks, a backslash, a semicolon, spaces.
func largeContext(t *testing.T, size int) string {
	t.Helper()
	head := `{"version":1,"run":"contract-4","app":"hera-demo","project":"vision","step":"train","kind":"train",` +
		`"experiment":"it's $HOME ` + "`x`" + ` ; done \\n \"q\" \u007b\u007bworkflow.name\u007d\u007d","params":{"blob":"`
	tail := `"},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{"prepare":{"version":1,"status":"absent"}}}`
	fill := size - len(head) - len(tail)
	var b strings.Builder
	h := "contract-4"
	for b.Len() < fill {
		sum := sha256.Sum256([]byte(h))
		h = hex.EncodeToString(sum[:])
		b.WriteString(h)
	}
	text := head + b.String()[:fill] + tail
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil || len(text) != size {
		t.Fatalf("the test context is not %d bytes of JSON: %d bytes, %v", size, len(text), err)
	}
	return text
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// crdsWithContractFields loads the repository's definitions and adds the contract's fields to the HPC
// job: spec.context, spec.app, status.result (section 8). Nothing else of the definition changes, so
// its other rules (the immutable spec among them) are checked with the large field in place.
func crdsWithContractFields(t *testing.T, root string) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "config", "crd", "bases", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no definitions under config/crd/bases: %v", err)
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	patched := false
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal(raw, crd); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if crd.Spec.Names.Kind == "HPCArgoWorkflowJob" {
			schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
			spec := schema.Properties["spec"]
			spec.Properties["context"] = apiextensionsv1.JSONSchemaProps{Type: "string", MaxLength: ptr.To(int64(contextCap))}
			spec.Properties["app"] = apiextensionsv1.JSONSchemaProps{Type: "string", MaxLength: ptr.To(int64(55))}
			schema.Properties["spec"] = spec
			status := schema.Properties["status"]
			status.Properties["result"] = apiextensionsv1.JSONSchemaProps{Type: "string", MaxLength: ptr.To(int64(resultCap))}
			schema.Properties["status"] = status
			patched = true
		}
		out = append(out, crd)
	}
	if !patched {
		t.Fatal("the HPCArgoWorkflowJob definition was not found")
	}
	return out
}

func TestA96KiBContextSurvivesEveryHop(t *testing.T) {
	root := repoRoot(t)
	value := largeContext(t, contextCap)
	t.Logf("the context: %d bytes, sha256 %s", len(value), sha(value))

	env := &envtest.Environment{CRDs: crdsWithContractFields(t, root), ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("the API server did not take the definitions with the contract's fields: %v", err)
	}
	defer func() { _ = env.Stop() }()
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = hpcv1alpha1.AddToScheme(s)
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, ns := range []string{"ml-vision-env", "ml-vision"} {
		if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}

	// ---- hop 1: the follower creates the job from its environment
	followerEnv := map[string]string{
		"HPC_JOB_NAMESPACE": "ml-vision-env", "WF_NAME": "contract-4", "WF_UID": "0b7f6c1e-1111-2222-3333-444455556666",
		"STEP_NAME": "train", "NODE_KEY": "train-hpc", "IMAGE_REF": "registry.example/vision/hera-demo/train:v1",
		"HPC_CLASS": "meluxina-gpu", "SLURM_TIME_LIMIT": "120", "STEP_COMMAND": `["python","-m","app.entry"]`,
		contextEnv: value,
	}
	fc, err := follower.ConfigFromEnv(func(k string) string { return followerEnv[k] })
	if err != nil {
		t.Fatal(err)
	}
	typed := fc.Job()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
	if err != nil {
		t.Fatal(err)
	}
	job := &unstructured.Unstructured{Object: obj}
	job.SetGroupVersionKind(hpcv1alpha1.GroupVersion.WithKind("HPCArgoWorkflowJob"))
	if err := unstructured.SetNestedField(job.Object, followerEnv[contextEnv], "spec", "context"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(job.Object, "hera-demo", "spec", "app"); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, job, client.FieldValidation("Strict")); err != nil {
		t.Fatalf("hop 1, follower to job object: the API server refused the job: %v", err)
	}
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(job.GroupVersionKind())
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatal(err)
	}
	got, _, _ := unstructured.NestedString(stored.Object, "spec", "context")
	if got != value {
		t.Fatalf("hop 1, follower to job object: spec.context came back changed (%d bytes, sha256 %s)", len(got), sha(got))
	}
	t.Logf("PASS  hop 1, follower to job object: spec.context read back from the API server, %d bytes, sha256 %s", len(got), sha(got))

	// The definition is where the cap is enforced: one byte more is refused.
	over := job.DeepCopy()
	over.SetName("hpc-over-the-cap")
	over.SetResourceVersion("")
	_ = unstructured.SetNestedField(over.Object, value+" ", "spec", "context")
	if err := c.Create(ctx, over, client.FieldValidation("Strict")); err == nil {
		t.Fatal("a context one byte over the cap was accepted")
	} else {
		t.Logf("PASS  a context of %d bytes is refused by the job definition: %s", len(value)+1, firstLine(err.Error()))
	}

	// The spec stays immutable with the large field in it (the rule compares the whole spec).
	changed := stored.DeepCopy()
	_ = unstructured.SetNestedField(changed.Object, strings.Replace(value, "contract-4", "contract-5", 1), "spec", "context")
	if err := c.Update(ctx, changed); err == nil {
		t.Fatal("spec.context could be changed after creation")
	} else {
		t.Logf("PASS  spec.context cannot be changed after creation: %s", firstLine(err.Error()))
	}

	// status.result takes a result at its cap and nothing longer. The cap is counted on the result's cost,
	// which is never less than its length, so a definition that allows resultCap characters allows every
	// result within the cap.
	const head, tail = `{"version":1,"status":"ok","values":{"blob":"`, `"},"reads":[],"writes":[]}`
	result := head + strings.Repeat("r", resultCap-len(head)-len(tail)) + tail
	withResult := stored.DeepCopy()
	_ = unstructured.SetNestedField(withResult.Object, result, "status", "result")
	if err := c.Status().Update(ctx, withResult); err != nil {
		t.Fatalf("status.result of %d bytes was refused: %v", len(result), err)
	}
	t.Logf("PASS  status.result takes a result of %d bytes", len(result))
	tooLong := withResult.DeepCopy()
	_ = unstructured.SetNestedField(tooLong.Object, head+strings.Repeat("r", resultCap-len(head)-len(tail)+1)+tail, "status", "result")
	if err := c.Status().Update(ctx, tooLong); err == nil {
		t.Fatalf("status.result of %d bytes was accepted", len(result)+1)
	} else {
		t.Logf("PASS  status.result of %d bytes is refused by the job definition: %s", len(result)+1, firstLine(err.Error()))
	}

	// ---- hop 2: the controller builds the runner Job from the stored job
	storedTyped := &hpcv1alpha1.HPCArgoWorkflowJob{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), storedTyped); err != nil {
		t.Fatal(err)
	}
	site := &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: "ml-vision"},
		Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Sharing: true,
			Profile: hpcv1alpha1.SiteProfile{Endpoint: "https://slurm-api.example", RESTVersion: "v0.0.44",
				ScratchRoot: "/project/scratch/p1", Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu",
					QOS: "default", Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
			Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1"}},
	}
	rj, err := runnerjob.Build(runnerjob.Config{Image: "registry.example/kaos-hpc-operator:check"}, storedTyped, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	// What the controller will add for the contract: the job's context, under the same name.
	stored2, _, _ := unstructured.NestedString(stored.Object, "spec", "context")
	rj.Spec.Template.Spec.Containers[0].Env = append(rj.Spec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: contextEnv, Value: stored2})
	if err := c.Create(ctx, rj); err != nil {
		t.Fatalf("hop 2, job object to runner Job: the API server refused the runner Job: %v", err)
	}
	storedRJ := &batchv1.Job{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(rj), storedRJ); err != nil {
		t.Fatal(err)
	}
	runnerEnv := map[string]string{}
	for _, e := range storedRJ.Spec.Template.Spec.Containers[0].Env {
		runnerEnv[e.Name] = e.Value
	}
	if runnerEnv[contextEnv] != value {
		t.Fatalf("hop 2, job object to runner Job: the env value came back changed (%d bytes)", len(runnerEnv[contextEnv]))
	}
	raw, _ := json.Marshal(storedRJ)
	t.Logf("PASS  hop 2, job object to runner Job: the runner container's %s read back from the API server, %d bytes, sha256 %s (the Job object: %d bytes)",
		contextEnv, len(runnerEnv[contextEnv]), sha(runnerEnv[contextEnv]), len(raw))

	// ---- hops 3 and 4: the runner process, the Slurm submission, the job script, the step
	slurm := fakeslurm.New(fakeslurm.Options{Name: "contract", Token: "check-token",
		QOS: []fakeslurm.QOS{{Name: "default", MaxWallMinutes: 2880}}})
	defer slurm.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, slurm.CertPEM(), 0o600); err != nil {
		t.Fatal(err)
	}
	runnerEnv["HPC_ENDPOINT"] = slurm.URL()
	runnerEnv["SLURM_TOKEN"] = "check-token"
	runnerEnv["SSL_CERT_FILE"] = ca
	runnerEnv["PATH"] = os.Getenv("PATH")
	report := runnerHop(t, root, runnerEnv)
	if report.RunnerSaw.SHA256 != sha(value) || report.RunnerSaw.Bytes != len(value) {
		t.Fatalf("hop 3, runner Job to runner process: the process saw %d bytes, sha256 %s", report.RunnerSaw.Bytes, report.RunnerSaw.SHA256)
	}
	t.Logf("PASS  hop 3, runner Job to runner process: the process read %s from its environment, %d bytes, sha256 %s",
		contextEnv, report.RunnerSaw.Bytes, report.RunnerSaw.SHA256)

	// Alone: the value rides in the Slurm environment; the fake site holds what it was sent.
	atSite := slurm.JobEnv(report.Alone.Job)[contextEnv]
	if atSite != value {
		t.Fatalf("hop 4, runner to site (alone): the site holds %d bytes, sha256 %s", len(atSite), sha(atSite))
	}
	t.Logf("PASS  hop 4, runner to site, job alone on its node: the fake site accepted the request (%d bytes; largest environment string %d) and holds %s unchanged",
		report.Alone.RequestBytes, report.Alone.LargestEnv, contextEnv)
	// Shared node: the value rides in the script, never in the Slurm environment.
	if _, there := slurm.JobEnv(report.Shared.Job)[contextEnv]; there || slurm.JobEnv(report.Shared.Job) == nil {
		t.Fatalf("hop 4, runner to site (shared): the site job is missing or holds the context in its environment")
	}
	t.Logf("PASS  hop 4, runner to site, job for a shared node: the fake site accepted the request (%d bytes; job script %d bytes); the value is in the script, not the Slurm environment",
		report.Shared.RequestBytes, report.Shared.ScriptBytes)

	for name, m := range map[string]mode{"alone (Slurm environment)": report.Alone, "shared node (export line in the job script)": report.Shared} {
		if m.StepSaw.SHA256 != sha(value) || m.StepSaw.Bytes != len(value) || m.ScriptExit != 0 {
			t.Fatalf("hop 5, job script to step, %s: the step saw %d bytes, sha256 %s; the script exited %d\n%s",
				name, m.StepSaw.Bytes, m.StepSaw.SHA256, m.ScriptExit, m.Tail)
		}
		t.Logf("PASS  hop 5, job script to step, %s: the step read %s, %d bytes, sha256 %s",
			name, contextEnv, m.StepSaw.Bytes, m.StepSaw.SHA256)
	}

	// ---- the control run: one character of what the job is handed is changed. Both steps must see it.
	runnerEnv["CONTRACT_CHECK_DAMAGE"] = "1"
	control := runnerHop(t, root, runnerEnv)
	for name, m := range map[string]mode{"alone": control.Alone, "shared node": control.Shared} {
		if m.ScriptExit != 0 || m.StepSaw.Bytes != len(value) {
			t.Fatalf("control run, %s: the step did not run to its end (exit %d, %d bytes)\n%s", name, m.ScriptExit, m.StepSaw.Bytes, m.Tail)
		}
		if m.StepSaw.SHA256 == sha(value) {
			t.Fatalf("control run, %s: a changed value was reported as unchanged: the comparison of hop 5 proves nothing", name)
		}
		t.Logf("PASS  control run, %s: a value with one character changed is seen as changed (sha256 %s)", name, m.StepSaw.SHA256)
	}
	if got := slurm.JobEnv(control.Alone.Job)[contextEnv]; got == value || len(got) != len(value) {
		t.Fatalf("control run: the site should hold the changed value for %s, it holds %d bytes, sha256 %s", control.Alone.Job, len(got), sha(got))
	}
	t.Logf("PASS  control run, the site: it holds the changed value for the control job, and the unchanged one for the first job")
	if slurm.JobEnv(report.Alone.Job)[contextEnv] != value {
		t.Fatalf("the first job's environment at the site changed after the control run")
	}
}

type saw struct {
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type mode struct {
	Job          string `json:"job"`
	SiteJobID    int    `json:"site_job_id"`
	RequestBytes int    `json:"request_bytes"`
	ScriptBytes  int    `json:"script_bytes"`
	LargestEnv   int    `json:"largest_env_string"`
	ScriptExit   int    `json:"script_exit"`
	StepSaw      saw    `json:"step_saw"`
	Tail         string `json:"script_output_tail"`
}

type hopReport struct {
	Alone     mode `json:"alone"`
	Shared    mode `json:"shared"`
	RunnerSaw saw  `json:"runner_saw"`
}

// runnerHop starts runner_hop.py with env as its whole environment and returns what it reports.
func runnerHop(t *testing.T, root string, env map[string]string) hopReport {
	t.Helper()
	cmd := exec.Command("python3", filepath.Join(root, "hack", "contract-checks", "hpc", "runner_hop.py"))
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("the runner hop: %v\n%s", err, stderr.String())
	}
	var report hopReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("the runner hop printed no report: %v\n%s\n%s", err, stdout.String(), stderr.String())
	}
	return report
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 220 {
		s = s[:220] + "..."
	}
	return s
}
