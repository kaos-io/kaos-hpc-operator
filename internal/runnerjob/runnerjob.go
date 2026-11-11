// Package runnerjob builds the Kubernetes Job that runs one attempt of a serve-mode
// HPCArgoWorkflowJob (PRD-HPC-1461 D-64). It is a pure function of the job, its HPCSite and the
// controller's own settings: the tenant's spec reaches the runner only through the allowlisted
// fields, and every endpoint and identity comes from the HPCSite or the operator-rendered
// ConfigMap (T-02).
package runnerjob

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Names the runner contract fixes (phase 1 contract section 2).
const (
	// ServiceAccount the runner runs as, in the HPCSite's namespace.
	ServiceAccount = "hpc-runner"
	// ConfigMapName holds the trusted endpoints, rendered by kubecore-operator.
	ConfigMapName = "hpc-runner-config"
	// SecretJWT holds the project's Slurm token (key token).
	SecretJWT = "meluxina-jwt"
	// SecretStaging holds the stager key.
	SecretStaging = "meluxina-staging"
	// SecretMLflow holds the data-plane machine key (key ZITADEL_MACHINE_KEY).
	SecretMLflow = "mlflow-svc"

	// LabelJobUID and LabelAttempt label a runner Job; AnnotationJobName and AnnotationJobNamespace
	// name the HPCArgoWorkflowJob (names can exceed the 63 characters a label allows).
	LabelJobUID            = "hpc.kaos.io/job-uid"
	LabelAttempt           = "hpc.kaos.io/attempt"
	LabelRunnerJob         = "hpc.kaos.io/runner-job"
	AnnotationJobName      = "hpc.kaos.io/job-name"
	AnnotationJobNamespace = "hpc.kaos.io/job-namespace"

	// Mount paths of the Secrets.
	MountJWT     = "/etc/meluxina-jwt"
	MountStaging = "/etc/meluxina-staging"
	MountMLflow  = "/etc/mlflow-svc"

	// Entrypoint of the runner program inside the image (the runner/ directory).
	runnerCommand = "/runner/main.py"

	// Wall-time arithmetic of the Job deadline: queue allowance and teardown margin, in minutes.
	queueAllowanceMinutes = 480
	teardownMarginMinutes = 30
	backoffLimit          = 4
	ttlSeconds            = 3600
	// graceSeconds is the time the runner has to cancel the site job on SIGTERM.
	graceSeconds = 60
	runnerUID    = 65532

	maxEnvItems = 16
	maxEnvValue = 2048
)

var outputsRepoPattern = regexp.MustCompile(`^outputs-[a-z0-9-]{1,55}$`)

// Scheduling is where runner pods go (chart values pass it through, like the controller's own).
type Scheduling struct {
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
}

// Config is what the controller itself decides about runner Jobs.
type Config struct {
	Image      string
	Scheduling Scheduling
	// SiteWaitMax is how long the runner waits for an unreachable site before it fails (PRD-HPC-1461 AC-28,
	// AC-30). It is added to the Job's deadline, so a long outage never eats the time of the site job. Zero: the
	// runner's own default (30 minutes) applies and the deadline is not extended.
	SiteWaitMax time.Duration
}

// Name is the deterministic runner Job name of an attempt: hpcr-<hash8(job uid)>-a<attempt>.
func Name(uid types.UID, attempt int32) string {
	sum := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("hpcr-%s-a%d", hex.EncodeToString(sum[:])[:8], attempt)
}

// LabelCancelOnly marks the runner Jobs that only stop an attempt's site job (the orphan sweep, AC-08).
const LabelCancelOnly = "hpc.kaos.io/cancel-only"

// cancelOnlyDeadlineMinutes bounds one cancel-only runner Job; the runner's own budget is shorter.
const cancelOnlyDeadlineMinutes = 60

// CancelName is the name of the n-th cancel-only runner Job of an attempt: hpcc-<hash8>-a<attempt>-<n>.
func CancelName(uid types.UID, attempt, n int32) string {
	sum := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("hpcc-%s-a%d-%d", hex.EncodeToString(sum[:])[:8], attempt, n)
}

// BuildCancel returns a cancel-only runner Job of an attempt: the runner image started with
// --cancel-only. It adopts the attempt's site job by key, cancels it, and reports its final state.
func BuildCancel(cfg Config, job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, attempt, n int32) (*batchv1.Job, error) {
	rj, err := Build(cfg, job, site, attempt)
	if err != nil {
		return nil, err
	}
	name := CancelName(job.UID, attempt, n)
	rj.Name = name
	rj.Labels[LabelCancelOnly] = "true"
	rj.Labels[LabelRunnerJob] = name
	rj.Spec.Template.Labels = rj.Labels
	rj.Spec.BackoffLimit = ptr.To(int32(2))
	rj.Spec.ActiveDeadlineSeconds = ptr.To(int64(cancelOnlyDeadlineMinutes * 60))
	rj.Spec.Template.Spec.Containers[0].Args = []string{"--cancel-only"}
	return rj, nil
}

// ValidateDataset refuses a job that names a dataset without a pinned commit: one stored before the run's
// data was pinned by the pipeline, or created by an older follower (spec.datasetRef). Running it would run
// the step without its data.
func ValidateDataset(spec *hpcv1alpha1.HPCArgoWorkflowJobSpec) error {
	if spec.Dataset == nil && spec.DatasetRef != "" {
		return fmt.Errorf("the job names dataset %s without a pinned commit: it was created before the platform "+
			"update that pins a run's dataset. Run the pipeline again", spec.DatasetRef)
	}
	return nil
}

// ValidateEnv enforces the allowlist on spec.env (the CRD enforces it too; this is the second lock).
func ValidateEnv(env []hpcv1alpha1.NameValue) error {
	if len(env) > maxEnvItems {
		return fmt.Errorf("env has %d entries, at most %d are allowed", len(env), maxEnvItems)
	}
	seen := map[string]bool{}
	for _, e := range env {
		if !contains(hpcv1alpha1.AllowedEnvNames, e.Name) {
			return fmt.Errorf("env %q is not allowed (allowed: %s)", e.Name, strings.Join(hpcv1alpha1.AllowedEnvNames, ", "))
		}
		if seen[e.Name] {
			return fmt.Errorf("env %q is set twice", e.Name)
		}
		seen[e.Name] = true
		if len(e.Value) > maxEnvValue {
			return fmt.Errorf("env %q is longer than %d bytes", e.Name, maxEnvValue)
		}
		if (e.Name == "OUTPUTS_REPO" || e.Name == "CHECKPOINT_BUCKET") && !outputsRepoPattern.MatchString(e.Value) {
			return fmt.Errorf("env %q must be an outputs-<name> repository", e.Name)
		}
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, l := range list {
		if l == s {
			return true
		}
	}
	return false
}

// Build returns the runner Job of one attempt of job, to run in the HPCSite's namespace.
func Build(cfg Config, job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, attempt int32) (*batchv1.Job, error) {
	if err := ValidateEnv(job.Spec.Env); err != nil {
		return nil, fmt.Errorf("spec.env: %w", err)
	}
	env, err := Env(job, site, attempt)
	if err != nil {
		return nil, err
	}
	if cfg.SiteWaitMax > 0 {
		env = append(env, corev1.EnvVar{Name: "HPC_SITE_WAIT_MAX_S", Value: strconv.Itoa(int(cfg.SiteWaitMax / time.Second))})
	}
	name := Name(job.UID, attempt)
	labels := map[string]string{
		"app.kubernetes.io/name":       "kaos-hpc-runner",
		"app.kubernetes.io/managed-by": "kaos-hpc-operator",
		LabelJobUID:                    string(job.UID),
		LabelAttempt:                   strconv.Itoa(int(attempt)),
		LabelRunnerJob:                 name,
	}
	wall := int64(job.EffectiveWallMinutes())
	if job.Spec.Sharing != nil && site.Spec.Sharing && job.EffectiveResumable() == nil {
		// A host's Slurm job may run up to twice its step's limit for guests, and its runner follows it to its
		// end so the node is settled to what the site charges (D-26). A resumable step never hosts.
		wall *= 2
	}
	deadline := (wall+queueAllowanceMinutes+teardownMarginMinutes)*60 + int64(cfg.SiteWaitMax/time.Second)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: site.Namespace, Labels: labels,
			Annotations: map[string]string{AnnotationJobName: job.Name, AnnotationJobNamespace: job.Namespace},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To(int32(backoffLimit)),
			ActiveDeadlineSeconds:   ptr.To(deadline),
			TTLSecondsAfterFinished: ptr.To(int32(ttlSeconds)),
			// Node loss and disruption are not the runner's fault: they do not count against the limit.
			PodFailurePolicy: &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{{
				Action: batchv1.PodFailurePolicyActionIgnore,
				OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{{
					Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue}},
			}}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       podSpec(cfg, env),
			},
		},
	}, nil
}

func podSpec(cfg Config, env []corev1.EnvVar) corev1.PodSpec {
	return corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		ServiceAccountName:            ServiceAccount,
		TerminationGracePeriodSeconds: ptr.To(int64(graceSeconds)),
		NodeSelector:                  cfg.Scheduling.NodeSelector,
		Tolerations:                   cfg.Scheduling.Tolerations,
		Affinity:                      cfg.Scheduling.Affinity,
		// fsGroup makes the 0400 Secret files readable by the non-root runner user.
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(runnerUID)), RunAsGroup: ptr.To(int64(runnerUID)),
			FSGroup:        ptr.To(int64(runnerUID)),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Containers: []corev1.Container{{
			Name:    "runner",
			Image:   cfg.Image,
			Command: []string{"python3", "-u", runnerCommand},
			Env:     env,
			EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: ConfigMapName}}}},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"),
					corev1.ResourceMemory: resource.MustParse("128Mi")},
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
			},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false),
				Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			VolumeMounts: []corev1.VolumeMount{
				{Name: SecretJWT, MountPath: MountJWT, ReadOnly: true},
				{Name: SecretStaging, MountPath: MountStaging, ReadOnly: true},
				{Name: SecretMLflow, MountPath: MountMLflow, ReadOnly: true},
			},
		}},
		Volumes: []corev1.Volume{
			secretVolume(SecretJWT, false), // fail fast: the runner cannot start without a Slurm token
			secretVolume(SecretStaging, true),
			secretVolume(SecretMLflow, false),
		},
	}
}

func secretVolume(name string, optional bool) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
		SecretName: name, DefaultMode: ptr.To(int32(0o400)), Optional: ptr.To(optional)}}}
}

// Env is the runner's environment. Names set here win over the ConfigMap (envFrom), and the
// tenant's spec.env is limited to the allowlist, so no tenant value can shadow an endpoint.
func Env(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, attempt int32) ([]corev1.EnvVar, error) {
	spec := job.Spec
	cmd, err := json.Marshal(append([]string{}, spec.Command...))
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	classes, err := classesJSON(site)
	if err != nil {
		return nil, err
	}
	var wfName, wfUID string
	if spec.Workflow != nil {
		wfName, wfUID = spec.Workflow.Name, spec.Workflow.UID
	}
	sharing, scope := "0", ""
	if spec.Sharing != nil && site.Spec.Sharing {
		sharing, scope = "1", spec.Sharing.Scope
	}
	// A job already running when the platform was updated names its dataset the old way (no commit). Its
	// next attempt must not run without data: the runner is told, and refuses to start.
	ds, unpinned := hpcv1alpha1.DatasetPin{}, ""
	if spec.Dataset != nil {
		ds = *spec.Dataset
	} else {
		unpinned = spec.DatasetRef
	}
	kv := [][2]string{
		{"HPC_JOB_NAMESPACE", job.Namespace}, {"HPC_JOB_NAME", job.Name},
		{"KAOS_ATTEMPT", strconv.Itoa(int(attempt))},
		{"IMAGE_REF", spec.Image}, {"STEP_COMMAND", string(cmd)},
		{"SLURM_TIME_LIMIT", strconv.Itoa(int(job.EffectiveWallMinutes()))},
		{"STEP_OUTPUTS", strings.Join(spec.Outputs, ",")}, {"HPC_CLASS", spec.Class},
		{"STEP_NAME", spec.StepName}, {"WF_UID", wfUID},
		{"KUBECORE_WORKFLOW_NAME", wfName}, {"KUBECORE_RUN_ID", wfName},
		{"DATASET_REPO", ds.Repo}, {"DATASET_VERSION", ds.Version}, {"DATASET_COMMIT", ds.Commit},
		{"DATASET_OWNER", ds.Owner}, {"DATASET_UNPINNED", unpinned}, {"HPC_SHARING", sharing}, {"HPC_SCOPE", scope},
		{"HPC_SITE", site.Spec.Site}, {"HPC_ACCOUNT", site.Spec.Identity.Account}, {"HPC_USER", site.Spec.Identity.User},
		{"HPC_SCRATCH", site.Spec.Profile.ScratchRoot}, {"HPC_HOME", homeOf(site)},
		{"HPC_CLASSES_JSON", classes}, {"HPC_ENDPOINT", site.Spec.Profile.Endpoint},
		{"HPC_REST_VERSION", site.Spec.Profile.RESTVersion},
		{"ZITADEL_MACHINE_KEY_FILE", MountMLflow + "/ZITADEL_MACHINE_KEY"},
	}
	if p := job.Status.Placement; p != nil {
		// The lane the controller chose (PRD-HPC-1461 D-68). Unset: the runner uses the class's QoS.
		if p.QOS != "" {
			kv = append(kv, [2]string{"HPC_QOS", p.QOS})
		}
		if p.Reservation != "" {
			kv = append(kv, [2]string{"HPC_RESERVATION", p.Reservation})
		}
		// A shared-node host pads its limit up to the class cap: in a lane, up to the lane's.
		if m := qosMaxMinutes(site, p.QOS); m > 0 {
			kv = append(kv, [2]string{"HPC_QOS_MAX_MINUTES", strconv.Itoa(int(m))})
		}
	}
	// D-72: the idle watchdog's window; the runner hands it to the job script (0 = off).
	kv = append(kv, [2]string{"HPC_IDLE_TIMEOUT_MINUTES", strconv.Itoa(int(job.EffectiveIdleTimeoutMinutes()))})
	// A step declared resumable, or verified resumable from its history (D-74), gets the wall-time signal
	// and never shares a node; every other step still gets its checkpoint folder (D-75).
	if res := job.EffectiveResumable(); res != nil {
		kv = append(kv, [2]string{"HPC_RESUMABLE_LEAD", strconv.Itoa(int(res.LeadMinutes))})
	}
	// AC-39: the site's submit cap, so a refusal for it can name the cap beside the count the runner reads.
	if m := site.Spec.Profile.MaxSubmitJobs; m > 0 {
		kv = append(kv, [2]string{"HPC_MAX_SUBMIT_JOBS", strconv.Itoa(int(m))})
	}
	kv = append(kv, [2]string{"HPC_CKPT_BUDGET_PERCENT", strconv.Itoa(int(checkpointBudgetPercent(site)))})
	if r := job.Status.Resume; r != nil && attempt > 1 {
		kv = append(kv, [2]string{"KAOS_RESUME_REF", r.CheckpointRef},
			[2]string{"KAOS_RESUME_FROM_ATTEMPT", strconv.Itoa(int(r.FromAttempt))})
	}
	for _, e := range spec.Env {
		kv = append(kv, [2]string{e.Name, e.Value})
	}
	env := make([]corev1.EnvVar, 0, len(kv)+1)
	for _, p := range kv {
		env = append(env, corev1.EnvVar{Name: p[0], Value: p[1]})
	}
	env = append(env, corev1.EnvVar{Name: "SLURM_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: SecretJWT}, Key: "token", Optional: ptr.To(false)}}})
	return env, nil
}

// checkpointBudgetPercent is the share of the project's scratch space the step checkpoints may hold.
func checkpointBudgetPercent(site *hpcv1alpha1.HPCSite) int32 {
	if p := site.Spec.Limits.CheckpointBudgetPercent; p > 0 {
		return p
	}
	return DefaultCheckpointBudgetPercent
}

// DefaultCheckpointBudgetPercent is the checkpoint budget when the HPCSite sets none (D-75).
const DefaultCheckpointBudgetPercent = 10

func homeOf(site *hpcv1alpha1.HPCSite) string {
	root := site.Spec.Profile.HomeRoot
	if root == "" {
		root = "/home/users"
	}
	return strings.TrimRight(root, "/") + "/" + site.Spec.Identity.User
}

// classesJSON is the site's class table as the runner reads it (the phase-0 context shape).
func classesJSON(site *hpcv1alpha1.HPCSite) (string, error) {
	type class struct {
		Name      string `json:"name"`
		Tier      string `json:"tier"`
		Partition string `json:"partition"`
		QOS       string `json:"qos,omitempty"`
		GPUs      int32  `json:"gpus"`
		MaxWall   int32  `json:"maxWallTimeMinutes"`
	}
	out := make([]class, 0, len(site.Spec.Profile.Classes))
	for _, c := range site.Spec.Profile.Classes {
		out = append(out, class{c.Name, string(c.Tier), c.Partition, c.QOS, c.GPUs, c.MaxWallTimeMinutes})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("marshal classes: %w", err)
	}
	return string(b), nil
}

// qosMaxMinutes is the maximum wall time of a QoS of the site profile (0: none or unknown).
func qosMaxMinutes(site *hpcv1alpha1.HPCSite, name string) int32 {
	for _, q := range site.Spec.Profile.QOS {
		if q.Name == name && name != "" {
			return q.MaxWallTimeMinutes
		}
	}
	return 0
}
