// Package follower is the credential-free pipeline side of an HPC step (PRD-HPC-1461 D-59): it
// creates (or re-attaches to) the step's HPCArgoWorkflowJob, prints what the controller reports,
// and on the end writes the Argo outputs and exits with the job's result. It holds no Slurm, SSH,
// registry or lakeFS credential and never deletes the job: cancel is the controller's (D-04).
package follower

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	ResumableOptOut  bool     // HPC_RESUMABLE=0|false (resumable=False in pipeline.py): never continued from a checkpoint
	Outputs          []string // STEP_OUTPUTS, comma separated
	Dataset          string
	Scope            string // HPC_SCOPE: set means node sharing
	IdleTimeout      *int32 // HPC_IDLE_TIMEOUT_MINUTES: nil = the platform's 20, 0 = off (step idle_timeout=)
	Env              []hpcv1alpha1.NameValue
}

// ConfigFromEnv reads the Config from a lookup function (os.LookupEnv in the binary).
func ConfigFromEnv(get func(string) string) (Config, error) {
	c := Config{
		Namespace: get("HPC_JOB_NAMESPACE"), WorkflowName: get("WF_NAME"), WorkflowUID: get("WF_UID"),
		StepName: get("STEP_NAME"), NodeKey: get("NODE_KEY"), Image: get("IMAGE_REF"),
		Class: get("HPC_CLASS"), Dataset: get("DATASET"), Scope: get("HPC_SCOPE"),
	}
	var missing []string
	for name, v := range map[string]string{"HPC_JOB_NAMESPACE": c.Namespace, "WF_NAME": c.WorkflowName, "WF_UID": c.WorkflowUID,
		"STEP_NAME": c.StepName, "NODE_KEY": c.NodeKey, "IMAGE_REF": c.Image, "HPC_CLASS": c.Class} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return c, fmt.Errorf("missing environment: %s", strings.Join(missing, ", "))
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
	for _, o := range strings.Split(get("STEP_OUTPUTS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			c.Outputs = append(c.Outputs, o)
		}
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
		DatasetRef: c.Dataset, Outputs: c.Outputs, StepName: c.StepName,
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
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "argoproj.io/v1alpha1", Kind: "Workflow", Name: c.WorkflowName,
				UID: apitypes.UID(c.WorkflowUID), Controller: &no, BlockOwnerDeletion: &no,
			}},
		},
		Spec: spec,
	}
}
