package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProjectLabel names the KAOS project a namespace belongs to. kubecore-operator writes it on every
// environment namespace and on the project's HPC namespace; tenants cannot set it (PRD-HPC-1461
// 3.11, T-02). An HPCArgoWorkflowJob never names its project: the controller reads this label.
const ProjectLabel = "hpc.kaos.io/project"

// Phase of an HPCArgoWorkflowJob.
// +kubebuilder:validation:Enum=Pending;Waiting;Refused;Admitted;Running;Succeeded;Failed;Cancelled
type Phase string

const (
	// PhasePending: not judged yet.
	PhasePending Phase = "Pending"
	// PhaseWaiting: admissible later, held KAOS-side with a reason (D-24, D-49).
	PhaseWaiting Phase = "Waiting"
	// PhaseRefused: never admissible as asked; terminal (D-24, D-41).
	PhaseRefused Phase = "Refused"
	// PhaseAdmitted: booked; the submitter may submit it.
	PhaseAdmitted Phase = "Admitted"
	// PhaseRunning: the site job exists (queued or running).
	PhaseRunning Phase = "Running"
	// PhaseSucceeded, PhaseFailed, PhaseCancelled: terminal, settled.
	PhaseSucceeded Phase = "Succeeded"
	PhaseFailed    Phase = "Failed"
	PhaseCancelled Phase = "Cancelled"
)

// Reasons the controller writes in status.reason.
const (
	ReasonAdmitted             = "Admitted"
	ReasonWaitingForBudget     = "WaitingForBudget"
	ReasonConcurrencyLimit     = "ConcurrencyLimit"
	ReasonBudgetBlocked        = "BudgetBlocked"
	ReasonMultiNodeUnsupported = "MultiNodeUnsupported"
	ReasonWallTimeOverLimit    = "WallTimeOverLimit"
	ReasonClassNotAllowed      = "ClassNotAllowed"
	ReasonNoProject            = "NoProject"
	ReasonNoSite               = "NoSite"
	// Continuation (D-44 as amended 2026-11-05, F-29, T-19).
	ReasonContinuing        = "Continuing"
	ReasonContinuationLimit = "ContinuationLimit"
	ReasonNotResumable      = "NotResumable"
	ReasonCheckpointMissing = "Timeout"
	ReasonWallTimeReached   = "WallTimeReached"
	ExitCodeContinue        = int32(85)
	// ReasonRunnerFailed: the runner Job ended (backoff exhausted) without a terminal report (phase 1).
	ReasonRunnerFailed = "RunnerFailed"
	// ReasonInvalidSpec: the spec asks for something the API does not allow (phase 1: env outside
	// the allowlist). Terminal, with the phase Refused.
	ReasonInvalidSpec = "InvalidSpec"
)

// CancelFinalizer holds a serve-mode job until its runner Job is gone and its attempt is settled
// (PRD-HPC-1461 D-04): cancel is the controller's, so a lost follower never orphans a site job.
const CancelFinalizer = "hpc.kaos.io/cancel"

// AllowedEnvNames are the only environment variables a job's spec may set on the runner
// (PRD-1461 phase 1 contract section 1). Everything that names an identity, a path, a class table
// or an endpoint comes from the HPCSite and the operator-rendered ConfigMap, never from the
// tenant (T-02). The CRD enforces the list with CEL; the controller enforces it again.
var AllowedEnvNames = []string{"OUTPUTS_REPO", "OUTPUTS_REF", "CHECKPOINT_BUCKET", "CHECKPOINT_PREFIX", "KAOS_LIB_SUPPORT"}

// WorkflowRef names the Argo Workflow a serve-mode job belongs to.
type WorkflowRef struct {
	// Name of the Workflow, in the job's namespace.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// UID of the Workflow. A Workflow of that name with another UID counts as deleted.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID string `json:"uid"`
}

// Sharing is the job's node-sharing scope (PRD-1609).
type Sharing struct {
	// Scope jobs must have in common to share a node.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]{8,16}$`
	Scope string `json:"scope"`
}

// NameValue is one environment variable.
type NameValue struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	Name string `json:"name"`
	// +kubebuilder:validation:MaxLength=2048
	Value string `json:"value"`
}

// HPCArgoWorkflowJobSpec is one pipeline step to run on an HPC site (D-01).
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable; create a new HPCArgoWorkflowJob"
type HPCArgoWorkflowJobSpec struct {
	// Class is the site class the user picked, for example meluxina-gpu.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Class string `json:"class"`
	// Image of the step.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// Command of the step.
	// +optional
	Command []string `json:"command,omitempty"`
	// WallTimeMinutes is the job's run limit once started (Slurm --time).
	// +kubebuilder:validation:Minimum=1
	WallTimeMinutes int32 `json:"wallTimeMinutes"`
	// Nodes the job asks for. Only 1 is admitted (D-41, CON-06); more is refused with a reason.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Nodes int32 `json:"nodes,omitempty"`
	// DatasetRef the step reads, <dataset-repo>@<version>.
	// +optional
	DatasetRef string `json:"datasetRef,omitempty"`
	// Outputs the step declares.
	// +optional
	Outputs []string `json:"outputs,omitempty"`
	// Resumable makes the step continue across jobs: it gets the wall-time signal LeadMinutes before
	// its wall time, saves a checkpoint through the KAOS library and exits 85, and the job continues
	// from that checkpoint in a new site job under this same HPCArgoWorkflowJob (D-38, D-44 as amended
	// 2026-11-05, F-29). Absent: the step ends when its wall time is reached.
	// +optional
	Resumable *Resumable `json:"resumable,omitempty"`
	// StepName is the DAG step's name (the Argo step-name input). Setting it turns on
	// serve mode: the controller runs the job through a runner Job (PRD-HPC-1461 D-58, D-64).
	// Without it the job behaves as in phase 0 (observe and admit; the in-pod runner submits).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +optional
	StepName string `json:"stepName,omitempty"`
	// Workflow is the Argo Workflow that owns the step. The controller cancels the job when the
	// Workflow is deleted, shut down or ended (D-04).
	// +optional
	Workflow *WorkflowRef `json:"workflow,omitempty"`
	// Sharing opts the job into node sharing under a scope (PRD-1609). Absent: no sharing.
	// +optional
	Sharing *Sharing `json:"sharing,omitempty"`
	// Env is extra environment for the runner, from a fixed allowlist: OUTPUTS_REPO, OUTPUTS_REF,
	// CHECKPOINT_BUCKET, CHECKPOINT_PREFIX, KAOS_LIB_SUPPORT. Nothing else is accepted (T-02).
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:XValidation:rule="self.all(e, e.name in ['OUTPUTS_REPO','OUTPUTS_REF','CHECKPOINT_BUCKET','CHECKPOINT_PREFIX','KAOS_LIB_SUPPORT'])",message="env may only set OUTPUTS_REPO, OUTPUTS_REF, CHECKPOINT_BUCKET, CHECKPOINT_PREFIX, KAOS_LIB_SUPPORT"
	// +kubebuilder:validation:XValidation:rule="self.all(e, !(e.name in ['OUTPUTS_REPO','CHECKPOINT_BUCKET']) || e.value.matches('^outputs-[a-z0-9-]{1,55}$'))",message="OUTPUTS_REPO and CHECKPOINT_BUCKET must be an outputs-<name> repository"
	// +listType=map
	// +listMapKey=name
	// +optional
	Env []NameValue `json:"env,omitempty"`
}

// Resumable is a step's declaration that it saves and continues (step(..., resumable=...)).
type Resumable struct {
	// LeadMinutes before the wall time the signal arrives: the time to save and upload the
	// checkpoint (resumable="20m" gives 20).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	// +optional
	LeadMinutes int32 `json:"leadMinutes,omitempty"`
	// MaxContinuations caps how many times the step continues (counted apart from failure retries).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=5
	// +optional
	MaxContinuations int32 `json:"maxContinuations,omitempty"`
}

// SiteRef names an HPCSite.
type SiteRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// Booking is what admission reserved for the job (D-12).
type Booking struct {
	// Site the job was admitted to (the HPCSite's spec.site) and the HPCSite that holds the ledger.
	Site     string `json:"site"`
	SiteName string `json:"siteName"`
	// SiteNamespace is where the HPCSite lives (the project's HPC namespace).
	SiteNamespace string `json:"siteNamespace"`
	Tier          Tier   `json:"tier"`
	// NodeMinutes booked: nodes × wall time.
	NodeMinutes int64 `json:"nodeMinutes"`
	// At is when the booking was made.
	At metav1.Time `json:"at"`
}

// RunStatus is written by the submitter (in phase 0 the in-pod runner, D-34): what the site says
// about the current attempt's site job. The controller only reads it.
type RunStatus struct {
	// Attempt this report is about (status.attempt when it was written). A report of an earlier
	// attempt is ignored.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Attempt int32 `json:"attempt,omitempty"`
	// SiteJobID is the scheduler's job id.
	// +optional
	SiteJobID string `json:"siteJobID,omitempty"`
	// State is the scheduler's job state, for example PENDING, RUNNING, COMPLETED, FAILED, TIMEOUT.
	// +optional
	State string `json:"state,omitempty"`
	// ExitCode of the job's step; 85 means it saved and continues (D-44).
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// StartTime and EndTime as the site reports them.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`
	// Terminal is set by the submitter once the job will not run again (it gave up retrying).
	// +optional
	Terminal bool `json:"terminal,omitempty"`
	// HostSiteJobID is set when a host job of the same project took this job onto its node
	// (node sharing, PRD-1609 D-26). The host's booking then covers the node.
	// +optional
	HostSiteJobID string `json:"hostSiteJobID,omitempty"`
	// Slot is the GPU slot the host gave this job.
	// +optional
	Slot *int32 `json:"slot,omitempty"`
	// CheckpointRef is set with exit code 85: the lakeFS commit (or URI) of the checkpoint the job
	// staged out before it exited, checked by the submitter (T-19). Without it nothing continues.
	// +optional
	CheckpointRef string `json:"checkpointRef,omitempty"`
	// LogOffset is how far into the site job's output file the runner has printed (bytes), so a
	// restarted runner prints each line once (PRD-1461 D-65, AC-70).
	// +kubebuilder:validation:Minimum=0
	// +optional
	LogOffset int64 `json:"logOffset,omitempty"`
}

// RunnerRef names the runner Job of the current attempt (written by the controller).
type RunnerRef struct {
	// JobName of the Kubernetes Job, in the HPCSite's namespace.
	JobName string `json:"jobName"`
	// Attempt the Job runs.
	Attempt int32 `json:"attempt"`
}

// AttemptRecord is one finished site job of an HPCArgoWorkflowJob, settled (written by the controller).
type AttemptRecord struct {
	Attempt   int32  `json:"attempt"`
	SiteJobID string `json:"siteJobID,omitempty"`
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// Outcome: Continued, Succeeded, Failed, Cancelled, Timeout, ...
	Outcome string `json:"outcome"`
	// Tier and SettledNodeMinutes: what the attempt spent.
	Tier               Tier  `json:"tier"`
	SettledNodeMinutes int64 `json:"settledNodeMinutes"`
	// EndTime decides the budget period the spend counts in.
	EndTime metav1.Time `json:"endTime"`
}

// ResumeInfo is what the next attempt resumes from.
type ResumeInfo struct {
	// CheckpointRef of the attempt that saved it.
	CheckpointRef string `json:"checkpointRef"`
	FromAttempt   int32  `json:"fromAttempt"`
}

// HPCArgoWorkflowJobStatus: the controller owns phase, reason, message, booking and settlement; the submitter
// owns run. The two never write the same field.
type HPCArgoWorkflowJobStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// Site is the HPCSite the job was first admitted to; it stays set across continuations, so every
	// attempt's spend counts on the same project ledger.
	// +optional
	Site *SiteRef `json:"site,omitempty"`
	// Booking held by the current attempt; cleared when the attempt ends.
	// +optional
	Booking *Booking `json:"booking,omitempty"`
	// SettledNodeMinutes is the job's actual spend over all its attempts, set once it ended.
	// +optional
	SettledNodeMinutes *int64 `json:"settledNodeMinutes,omitempty"`
	// Attempt is the current attempt (1 for the first site job; +1 per continuation).
	// +optional
	Attempt int32 `json:"attempt,omitempty"`
	// Continuations so far (exit 85 with a checkpoint), counted apart from failure retries.
	// +optional
	Continuations int32 `json:"continuations,omitempty"`
	// Resume is set for a continuation: the submitter passes it to the new site job.
	// +optional
	Resume *ResumeInfo `json:"resume,omitempty"`
	// Attempts are the finished, settled site jobs of this HPCArgoWorkflowJob.
	// +optional
	Attempts []AttemptRecord `json:"attempts,omitempty"`
	// Run is the submitter's report of the site job.
	// +optional
	Run *RunStatus `json:"run,omitempty"`
	// Outputs are the step's declared outputs, JSON text each (at most 64 KiB in all), written by
	// the runner after stage-out and before it reports run.terminal. The follower turns them into
	// the Argo step's outputs.
	// +kubebuilder:validation:MaxProperties=64
	// +optional
	Outputs map[string]string `json:"outputs,omitempty"`
	// PendingReason is the scheduler's pending reason while the site job is queued (runner's poll).
	// +optional
	PendingReason string `json:"pendingReason,omitempty"`
	// Runner is the runner Job of the current attempt (serve mode).
	// +optional
	Runner *RunnerRef `json:"runner,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.class`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name="Attempt",type=integer,JSONPath=`.status.attempt`
// +kubebuilder:printcolumn:name="Site job",type=string,JSONPath=`.status.run.siteJobID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HPCArgoWorkflowJob is one Argo Workflow step run on an HPC site. The step creates it and supplies what the job
// needs from the platform and the run (image, command, dataset, outputs, credentials); it cannot run on its
// own. The controller admits and books it against the project's share before anything is submitted
// (PRD-HPC-1461 F-01, F-26). The name HPCJob is kept for a future kind that runs without a workflow.
type HPCArgoWorkflowJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HPCArgoWorkflowJobSpec   `json:"spec"`
	Status HPCArgoWorkflowJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HPCArgoWorkflowJobList contains a list of HPCArgoWorkflowJob.
type HPCArgoWorkflowJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HPCArgoWorkflowJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HPCArgoWorkflowJob{}, &HPCArgoWorkflowJobList{})
}

// Terminal reports whether the phase is final.
func (p Phase) Terminal() bool {
	switch p {
	case PhaseRefused, PhaseSucceeded, PhaseFailed, PhaseCancelled:
		return true
	}
	return false
}
