package follower

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"sigs.k8s.io/yaml"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// engineGolden is the follower template the pipeline engine (engine/kubecore/meluxina.py) renders; the
// engine's own tests keep it current (engine/tests/template/test_meluxina_follower.py). The follower and
// the engine ship at one version (D-58/D-63), so this is the contract test between the two halves.
const engineGolden = "../../engine/tests/template/golden/meluxina_follower.yaml"

// engineGoldenArgoValues stands in for the Argo placeholders of the golden. A placeholder that is not
// listed here fails the test: the engine started to pass the follower something new.
var engineGoldenArgoValues = map[string]string{
	"workflow.name":                    "wf-1",
	"workflow.uid":                     "uid-1",
	"workflow.parameters.dataset":      "dataset-default@latest",
	"inputs.parameters.step-name":      "train",
	"inputs.parameters.image":          "registry.example/app/train:v0",
	"inputs.parameters.step-command":   `["python","-m","app.entry"]`,
	"inputs.parameters.time-limit":     "90",
	"inputs.parameters.hpc-class":      "meluxina-gpu",
	"inputs.parameters.resumable-lead": "10",
	"inputs.parameters.resumable":      "auto",
	"inputs.parameters.idle-timeout":   "40",
	"inputs.parameters.step-outputs":   "model,metrics",
}

type goldenEnvVar struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	ValueFrom *struct {
		FieldRef struct {
			FieldPath string `json:"fieldPath"`
		} `json:"fieldRef"`
	} `json:"valueFrom"`
}

type goldenTemplate struct {
	Container struct {
		Command []string       `json:"command"`
		Env     []goldenEnvVar `json:"env"`
	} `json:"container"`
	ServiceAccountName string `json:"serviceAccountName"`
}

func loadEngineGolden(t *testing.T) goldenTemplate {
	t.Helper()
	raw, err := os.ReadFile(filepath.FromSlash(engineGolden))
	if err != nil {
		t.Fatalf("the engine's follower golden is part of this repository: %v", err)
	}
	var tpl goldenTemplate
	if err := yaml.Unmarshal(raw, &tpl); err != nil {
		t.Fatal(err)
	}
	return tpl
}

var argoPlaceholder = regexp.MustCompile(`\{\{\s*([^}\s]+)\s*\}\}`)

// envFromGolden resolves the golden's env the way Argo and the kubelet would.
func envFromGolden(t *testing.T, tpl goldenTemplate) map[string]string {
	t.Helper()
	env := map[string]string{}
	for _, e := range tpl.Container.Env {
		value := e.Value
		if e.ValueFrom != nil {
			value = "ns" // metadata.namespace
		}
		env[e.Name] = argoPlaceholder.ReplaceAllStringFunc(value, func(m string) string {
			key := argoPlaceholder.FindStringSubmatch(m)[1]
			v, ok := engineGoldenArgoValues[key]
			if !ok {
				t.Errorf("env %s uses the Argo placeholder %q the contract test does not know", e.Name, key)
			}
			return v
		})
	}
	return env
}

func TestEngineGoldenIsAValidFollowerEnvironment(t *testing.T) {
	tpl := loadEngineGolden(t)
	if len(tpl.Container.Command) != 1 || tpl.Container.Command[0] != "/follower" {
		t.Fatalf("the engine runs %v, the image ships /follower", tpl.Container.Command)
	}
	env := envFromGolden(t, tpl)
	cfg, err := ConfigFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("the engine's follower template is not a valid follower environment: %v", err)
	}
	if cfg.StepName != "train" || cfg.Class != "meluxina-gpu" || cfg.WallTimeMinutes != 90 || cfg.ResumableLead != 10 {
		t.Errorf("fields did not come through: %+v", cfg)
	}
	if cfg.IdleTimeout == nil || *cfg.IdleTimeout != 40 {
		t.Errorf("HPC_IDLE_TIMEOUT_MINUTES did not come through: %v", cfg.IdleTimeout)
	}
	if len(cfg.Outputs) != 2 || len(cfg.Command) != 3 || cfg.Dataset != "dataset-default@latest" {
		t.Errorf("outputs, command or dataset did not come through: %+v", cfg)
	}
}

func TestEngineGoldenSetsNoVariableTheFollowerIgnores(t *testing.T) {
	read := map[string]bool{
		"HPC_JOB_NAMESPACE": true, "WF_NAME": true, "WF_UID": true, "STEP_NAME": true, "NODE_KEY": true,
		"IMAGE_REF": true, "HPC_CLASS": true, "DATASET": true, "HPC_SCOPE": true, "STEP_COMMAND": true,
		"SLURM_TIME_LIMIT": true, "HPC_IDLE_TIMEOUT_MINUTES": true, "STEP_OUTPUTS": true, "HPC_SHARING": true,
		"HPC_RESUMABLE_LEAD": true, "HPC_RESUMABLE": true, "HPC_MAX_CONTINUATIONS": true,
	}
	for _, n := range hpcv1alpha1.AllowedEnvNames {
		read[n] = true
	}
	for _, e := range loadEngineGolden(t).Container.Env {
		if !read[e.Name] {
			t.Errorf("the engine sets %s, which the follower does not read (contract skew)", e.Name)
		}
	}
}

func TestEngineGoldenRunsAsTheSubmitterServiceAccount(t *testing.T) {
	if got := loadEngineGolden(t).ServiceAccountName; got != "hpc-submitter" {
		t.Errorf("serviceAccountName = %q, want hpc-submitter (AC-18)", got)
	}
}
