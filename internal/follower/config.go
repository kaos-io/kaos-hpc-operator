// Package follower is the credential-free pipeline side of an HPC step (PRD-HPC-1461 D-59): it
// creates (or re-attaches to) the step's HPCArgoWorkflowJob, prints what the controller reports,
// and on the end hands the step's result on to the workflow and exits with the job's result. It carries
// the step context (docs/step-contract.md) into the job unchanged. It holds no Slurm, SSH,
// registry or lakeFS credential and never deletes the job: cancel is the controller's (D-04).
package follower

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// nameHexLen is how many hex characters of the hash the job name keeps.
const nameHexLen = 20

// defaultMaxContinuations is the continuation cap when HPC_MAX_CONTINUATIONS is not set.
const defaultMaxContinuations = 5

// Config is what the follower reads from its environment (set by the enhancer).
type Config struct {
	Namespace        string // HPC_JOB_NAMESPACE (the pod's namespace)
	WorkflowName     string // WF_NAME
	WorkflowUID      string // WF_UID
	StepName         string // STEP_NAME
	NodeKey          string // NODE_KEY: the Argo node name without the retry suffix
	Image            string // IMAGE_REF
	Command          []string
	WallTimeMinutes  int32 // SLURM_TIME_LIMIT
	Class            string
	ResumableLead    int32 // HPC_RESUMABLE_LEAD, 0 = not resumable
	MaxContinuations int32
	ResumableOptOut  bool   // HPC_RESUMABLE=0|false (resumable=False in pipeline.py): never continued from a checkpoint
	Context          string // KAOS_STEP_CONTEXT: the step context in wire form, carried into the job unchanged
	App              string // the context's app
	Kind             string // the context's kind: the job's step-kind label
	// Inputs are the datasets the job stages: the context's ledger reads and every dataset a needed step wrote.
	Inputs      []hpcv1alpha1.StepInput
	Scope       string // HPC_SCOPE: set means node sharing
	IdleTimeout *int32 // HPC_IDLE_TIMEOUT_MINUTES: nil = the platform's 20, 0 = off (step idle_timeout=)
	Env         []hpcv1alpha1.NameValue
}

// ConfigFromEnv reads the Config from a lookup function (os.LookupEnv in the binary).
func ConfigFromEnv(get func(string) string) (Config, error) {
	c := Config{
		Namespace: get("HPC_JOB_NAMESPACE"), WorkflowName: get("WF_NAME"), WorkflowUID: get("WF_UID"),
		StepName: get("STEP_NAME"), NodeKey: get("NODE_KEY"), Image: get("IMAGE_REF"),
		Class: get("HPC_CLASS"), Scope: get("HPC_SCOPE"),
	}
	var missing []string
	for name, v := range map[string]string{"HPC_JOB_NAMESPACE": c.Namespace, "WF_NAME": c.WorkflowName, "WF_UID": c.WorkflowUID,
		"STEP_NAME": c.StepName, "NODE_KEY": c.NodeKey, "IMAGE_REF": c.Image, "HPC_CLASS": c.Class,
		hpcv1alpha1.ContextEnv: get(hpcv1alpha1.ContextEnv)} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return c, fmt.Errorf("missing environment: %s", strings.Join(missing, ", "))
	}
	if err := c.readContext(get(hpcv1alpha1.ContextEnv)); err != nil {
		return c, err
	}
	if raw := get("STEP_COMMAND"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.Command); err != nil {
			return c, fmt.Errorf("STEP_COMMAND is not a JSON array of strings: %w", err)
		}
	}
	wall, err := strconv.Atoi(get("SLURM_TIME_LIMIT"))
	if err != nil || wall < 1 {
		return c, fmt.Errorf("SLURM_TIME_LIMIT must be a positive number of minutes, got %q", get("SLURM_TIME_LIMIT"))
	}
	c.WallTimeMinutes = int32(wall)
	if err := c.readResumable(get); err != nil {
		return c, err
	}
	if raw := get("HPC_IDLE_TIMEOUT_MINUTES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return c, fmt.Errorf("HPC_IDLE_TIMEOUT_MINUTES must be a number of minutes (0 = off), got %q", raw)
		}
		c.IdleTimeout = ptr.To(int32(n))
	}
	if (get("HPC_SHARING") == "1" || get("HPC_SHARING") == "true") && c.Scope == "" {
		return c, fmt.Errorf("HPC_SHARING is set but HPC_SCOPE is empty")
	}
	for _, n := range hpcv1alpha1.AllowedEnvNames {
		if v := get(n); v != "" {
			c.Env = append(c.Env, hpcv1alpha1.NameValue{Name: n, Value: v})
		}
	}
	return c, nil
}

func (c *Config) readResumable(get func(string) string) error {
	if raw := get("HPC_RESUMABLE_LEAD"); raw != "" && raw != "0" {
		lead, err := strconv.Atoi(raw)
		if err != nil || lead < 1 {
			return fmt.Errorf("HPC_RESUMABLE_LEAD must be a positive number of minutes, got %q", raw)
		}
		c.ResumableLead = int32(lead)
		c.MaxContinuations = defaultMaxContinuations
	}
	switch strings.ToLower(get("HPC_RESUMABLE")) {
	case "0", "false", "no":
		c.ResumableOptOut = true
	}
	if raw := get("HPC_MAX_CONTINUATIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return fmt.Errorf("HPC_MAX_CONTINUATIONS must be a number, got %q", raw)
		}
		c.MaxContinuations = int32(n)
	}
	return nil
}

// Cost is what a value in wire form costs against its cap (docs/step-contract.md, section 7): its length as
// a JSON string, as encoding/json writes it. The workflow engine hands a step its definition with the context
// inside it as a JSON string, and that is what the limit is counted on.
func Cost(wire string) int {
	b, err := json.Marshal(wire)
	if err != nil { // a Go string always marshals
		return len(wire)
	}
	return len(b) - 2
}

var (
	inputName   = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9_-]*[A-Za-z0-9])?$`) // at most 40, below
	inputRepo   = regexp.MustCompile(`^dataset-[a-z0-9][a-z0-9-]{0,54}$`)         // a platform Dataset: dataset-<name>
	inputCommit = regexp.MustCompile(`^[0-9a-f]{64}$`)
	inputRef    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`) // at most 255, below
)

const (
	maxInputs    = 16
	maxInputName = 40
	maxInputRef  = 255
)

// contextView is the part of the step context the follower needs: the app, the kind, and where the step's
// datasets are pinned. It reads nothing else and never writes the context again: the job gets the text.
type contextView struct {
	App    *string `json:"app"`
	Kind   *string `json:"kind"`
	Ledger *struct {
		Reads []contextEntry `json:"reads"`
	} `json:"ledger"`
	Upstream map[string]struct {
		Writes []contextEntry `json:"writes"`
	} `json:"upstream"`
}

// contextEntry is a ledger read, or a write of a needed step's result.
type contextEntry struct {
	Name   string  `json:"name"`
	Type   string  `json:"type"` // writes only
	Repo   string  `json:"repo"`
	Owner  *string `json:"owner"`
	Ref    string  `json:"ref"`
	Commit string  `json:"commit"`
	Line   string  `json:"line"`
}

// readContext takes the step context as the workflow handed it over: it keeps the text unchanged for the job
// and reads from it the app, the kind and the datasets to stage. The follower resolves no version: every
// entry carries the commit the run pinned or the commit a needed step sealed. A context that is too large, is
// not a context, or names a dataset that cannot be staged is an error, never a job without its data.
func (c *Config) readContext(raw string) error {
	if cost := Cost(raw); cost > hpcv1alpha1.ContextMaxLength {
		return fmt.Errorf("the step context costs %d bytes, more than the %d (96 KiB) a step can be handed: "+
			"the run's configuration or the results of the steps this one needs are too large. Large values belong "+
			"in MLflow or in an artifact", cost, hpcv1alpha1.ContextMaxLength)
	}
	var view contextView
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		return fmt.Errorf("%s is not a step context (a JSON object): %w", hpcv1alpha1.ContextEnv, err)
	}
	if view.App == nil || *view.App == "" || view.Kind == nil || *view.Kind == "" || view.Ledger == nil {
		return fmt.Errorf("%s is not a step context: it must hold app, kind and ledger", hpcv1alpha1.ContextEnv)
	}
	c.Context, c.App, c.Kind = raw, *view.App, *view.Kind
	entries := append([]contextEntry{}, view.Ledger.Reads...)
	steps := make([]string, 0, len(view.Upstream))
	for step := range view.Upstream {
		steps = append(steps, step)
	}
	sort.Strings(steps) // the job's spec is compared when a pod re-attaches: the order must not depend on a map
	for _, step := range steps {
		for _, w := range view.Upstream[step].Writes {
			if w.Type == "dataset" {
				entries = append(entries, w)
			}
		}
	}
	seen := map[string]bool{}
	for _, e := range entries {
		in, err := stepInput(e)
		if err != nil {
			return err
		}
		if seen[in.Name] {
			return fmt.Errorf("the step context names the input %q twice", in.Name)
		}
		seen[in.Name] = true
		c.Inputs = append(c.Inputs, in)
	}
	if len(c.Inputs) > maxInputs {
		return fmt.Errorf("the step reads %d datasets; a step on HPC can be given at most %d", len(c.Inputs), maxInputs)
	}
	return nil
}

// stepInput is one entry of the context as the job stages it. Anything that is not a pinned dataset of the
// run's own project is an error.
func stepInput(e contextEntry) (hpcv1alpha1.StepInput, error) {
	in := hpcv1alpha1.StepInput{Name: e.Name, Repo: e.Repo, Commit: e.Commit, Ref: e.Ref, Line: e.Line}
	switch {
	case len(e.Name) > maxInputName || !inputName.MatchString(e.Name):
		return in, fmt.Errorf("the step context names an input %q: an input's name is at most %d letters, digits, "+
			"hyphens and underscores", e.Name, maxInputName)
	case !inputRepo.MatchString(e.Repo):
		return in, fmt.Errorf("input %s: %q is not a dataset (a dataset is a repository named dataset-<name>)", e.Name, e.Repo)
	case !inputCommit.MatchString(e.Commit):
		return in, fmt.Errorf("input %s: dataset %s has no commit (got %q): the run pins every input before any step",
			e.Name, e.Repo, e.Commit)
	case e.Ref != "" && (len(e.Ref) > maxInputRef || !inputRef.MatchString(e.Ref)):
		return in, fmt.Errorf("input %s: %q is not a dataset version", e.Name, e.Ref)
	case e.Owner != nil && *e.Owner != "":
		return in, fmt.Errorf("input %s is the dataset %s shared by project %s: a dataset of another project cannot be "+
			"read on HPC yet. Run this step on a cluster class, or copy the dataset into this project", e.Name, e.Repo, *e.Owner)
	}
	return in, nil
}

// JobName is the deterministic name of the step's job: hpc- plus the first 20 hex characters of
// sha256(workflow uid + "/" + node key). A re-created pod of the same step computes the same name.
func JobName(workflowUID, nodeKey string) string {
	sum := sha256.Sum256([]byte(workflowUID + "/" + nodeKey))
	return "hpc-" + hex.EncodeToString(sum[:])[:nameHexLen]
}

// Job is the HPCArgoWorkflowJob the config describes, owned by the Workflow.
func (c Config) Job() *hpcv1alpha1.HPCArgoWorkflowJob {
	spec := hpcv1alpha1.HPCArgoWorkflowJobSpec{
		Class: c.Class, Image: c.Image, Command: c.Command, WallTimeMinutes: c.WallTimeMinutes, Nodes: 1,
		App: c.App, Context: c.Context, Inputs: c.Inputs, StepName: c.StepName,
		Workflow: &hpcv1alpha1.WorkflowRef{Name: c.WorkflowName, UID: c.WorkflowUID}, Env: c.Env,
	}
	if c.ResumableLead > 0 {
		spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: c.ResumableLead, MaxContinuations: c.MaxContinuations}
	}
	spec.ResumableOptOut = c.ResumableOptOut
	spec.IdleTimeoutMinutes = c.IdleTimeout
	if c.Scope != "" {
		spec.Sharing = &hpcv1alpha1.Sharing{Scope: c.Scope}
	}
	no := false
	return &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: JobName(c.WorkflowUID, c.NodeKey), Namespace: c.Namespace,
			Labels: map[string]string{hpcv1alpha1.StepKindLabel: c.Kind},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "argoproj.io/v1alpha1", Kind: "Workflow", Name: c.WorkflowName,
				UID: apitypes.UID(c.WorkflowUID), Controller: &no, BlockOwnerDeletion: &no,
			}},
		},
		Spec: spec,
	}
}

// CRD defaults of HPCArgoWorkflowJobSpec (kubebuilder:default markers in api/v1alpha1) the API server
// fills into every job it stores. A nil or zero value in a request is indistinguishable from an
// absent one, so they are applied before two specs are compared.
const (
	crdDefaultNodes       = int32(1)
	crdDefaultLeadMinutes = int32(10)
)

// withCRDDefaults returns spec as the API server would store it. It never changes the argument.
func withCRDDefaults(spec hpcv1alpha1.HPCArgoWorkflowJobSpec) hpcv1alpha1.HPCArgoWorkflowJobSpec {
	if spec.Nodes == 0 {
		spec.Nodes = crdDefaultNodes
	}
	if spec.IdleTimeoutMinutes == nil {
		spec.IdleTimeoutMinutes = ptr.To(hpcv1alpha1.DefaultIdleTimeoutMinutes)
	}
	if spec.Resumable != nil {
		r := *spec.Resumable
		if r.LeadMinutes == 0 {
			r.LeadMinutes = crdDefaultLeadMinutes
		}
		if r.MaxContinuations == 0 {
			r.MaxContinuations = int32(defaultMaxContinuations)
		}
		spec.Resumable = &r
	}
	return spec
}
