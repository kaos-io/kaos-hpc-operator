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

// Reasons the controller writes in status.reason. docs/reasons.md says what each one means and what the user
// sees.
const (
	ReasonAdmitted         = "Admitted"
	ReasonWaitingForBudget = "WaitingForBudget"
	ReasonConcurrencyLimit = "ConcurrencyLimit"
	ReasonBudgetBlocked    = "BudgetBlocked"
	// ReasonSiteLimit: the site's own monthly account limit, not the project's share, stops the job
	// (PRD-HPC-1461 AC-23). Refused at admission when the site's remainder is what the job cannot fit;
	// shown instead of Slurm's AssocGrp*Minutes while a submitted job queues behind that limit.
	ReasonSiteLimit = "SiteLimit"
	// ReasonSubmitLimit: the site's cap on queued plus running jobs per user (Slurm MaxSubmitJobs, or a
	// QOSMaxSubmitJobPerUserLimit refusal) stops a new job (PRD-HPC-1461 AC-39, amended 2026-11-09). The
	// job FAILS with the cap and the count in the message; it does not wait (unlike ConcurrencyLimit,
	// D-49). Refused at admission when the project's own jobs already fill the cap from the site profile
	// (HPCSite spec.profile.maxSubmitJobs); Failed when the site itself refused the submit (the runner
	// reports ExitCodeSubmitLimit). Platform site operations (site jobs, staging, cancel) are not judged.
	ReasonSubmitLimit = "SubmitLimit"
	// ReasonInsufficientQuota: staging the dataset or the image would exceed the project's scratch
	// quota (space or files) even after evicting everything unused; the step stops before any copy
	// (PRD-HPC-1461 AC-33, D-06). Terminal. The runner reports it with ExitCodeInsufficientQuota.
	ReasonInsufficientQuota = "InsufficientQuota"
	// ReasonInvalidImage: the step's image reference is not registry-host/repository with a tag or digest, so it
	// can never be pulled. Refused at admission (nothing booked), or Failed when the runner finds it (exit
	// ExitCodeInvalidImage, no site job). Terminal; no credential retry can fix it.
	ReasonInvalidImage = "InvalidImage"
	// ReasonStagingMismatch: the site's staging tool and the runner speak different staging contracts (the two
	// ship separately and one was updated first), so no copy of the dataset can be trusted to be whole
	// (PRD kaos#1911 F-04). Failed before anything is staged or submitted; the runner reports it with
	// ExitCodeStagingMismatch. Terminal: it ends when the other side is updated.
	ReasonStagingMismatch = "StagingMismatch"
	// ReasonReservedName: the dataset version holds an object whose name cannot be copied to the site. Either
	// it is reserved for the platform there (a root name starting with ".kaos-", or a file name ending in
	// ".kaos-part"), where it would be taken for a marker of the copy; or its path could leave the dataset's
	// folder (a ".." or "." or empty part, a leading "/", a NUL byte). Failed before anything is staged or
	// submitted; the runner reports it with ExitCodeReservedName and its message names the object and which of
	// the two it is. Terminal: it ends with a new version of the dataset.
	ReasonReservedName = "ReservedName"
	// ReasonNoContext: the job carries no step context (spec.context). It was created by a follower from before
	// the step contract (PRD kaos#1911), or stored before the definition changed and read back without the
	// fields that were removed. Refused before anything is booked; a job that was already booked ends Failed
	// with this reason instead of starting, or publishing, without its data. Terminal: run the pipeline again.
	ReasonNoContext = "NoContext"
	// ReasonSharedInput: an entry of spec.inputs names an owner, a dataset another project shares. Such a dataset
	// cannot be staged on an HPC site yet. Refused before anything is booked.
	ReasonSharedInput = "SharedInput"
	// ReasonResultNotHandedBack: the step ended well, but what it handed back could not be brought from the
	// site to status.result: the job could not read or upload the result file, or the runner could not fetch
	// it (PRD kaos#1911 CON-04). Also when the run has no way to hand a result back at all (no data-plane token,
	// no lakeFS endpoint, no outputs repository): then nothing is submitted. The job is Failed and publishes no
	// result; the message names the cause. The job script and the runner report ExitCodeResultNotHandedBack.
	ReasonResultNotHandedBack = "ResultNotHandedBack"
	// ReasonInvalidResult: the step ended well and its result file reached the runner, but it is not a result
	// of the step contract: not a JSON object, not version 1 with status "ok" and the three fields values,
	// reads and writes, or it costs more than 8 KiB. The job is Failed and publishes no result; the message
	// names what is wrong. The runner reports ExitCodeInvalidResult.
	ReasonInvalidResult        = "InvalidResult"
	ReasonMultiNodeUnsupported = "MultiNodeUnsupported"
	ReasonWallTimeOverLimit    = "WallTimeOverLimit"
	// ReasonTooLongNotResumable: the declared limit is above the longest lane and the step is not
	// resumable, so it would be cut at the lane maximum. Refused at admission, never cut silently.
	ReasonTooLongNotResumable = "TooLongNotResumable"
	// ReasonTooLongBeyondContinuations: the step is resumable but the declared limit needs more
	// runs than the continuation cap allows.
	ReasonTooLongBeyondContinuations = "TooLongBeyondContinuations"
	ReasonClassNotAllowed            = "ClassNotAllowed"
	ReasonNoProject                  = "NoProject"
	ReasonNoSite                     = "NoSite"
	// Continuation (D-44 as amended 2026-11-05, F-29, T-19).
	// ReasonContinuing: the attempt ended with a verified checkpoint copy and the next attempt (phase Pending)
	// starts from it.
	ReasonContinuing = "Continuing"
	// ReasonContinuationLimit: the step used all its continuations (spec.resumable.maxContinuations, default 5).
	ReasonContinuationLimit = "ContinuationLimit"
	// ReasonNotResumable: the step exited 85 ("saved, continue me") but opted out (resumable=False) or, in the
	// phase-0 path, was not declared resumable.
	ReasonNotResumable = "NotResumable"
	// ReasonCheckpointMissing: the step exited 85 but no verified checkpoint copy exists to continue from. The
	// reason string is "Timeout" for historical reasons; read it as "checkpoint missing".
	ReasonCheckpointMissing = "Timeout"
	// ReasonWallTimeReached: the step ran to its time limit (Slurm TIMEOUT) and was not continued.
	ReasonWallTimeReached = "WallTimeReached"
	ExitCodeContinue      = int32(85)
	// ReasonIdle: the on-node idle watchdog ended a hung step (PRD-HPC-1461 D-72, F-46). The job
	// script exits ExitCodeIdle for it; the attempt is Failed with this reason.
	ReasonIdle = "Idle"
	// ExitCodeIdle is the job script's exit code for a step the idle watchdog ended. It sits in the
	// script's own range (231-235), clear of any code a step is likely to choose.
	ExitCodeIdle = int32(236)
	// ExitCodeInsufficientQuota is the exit code the runner reports (state FAILED, no site job) when staging
	// stopped for want of scratch quota. Next to ExitCodeIdle, in the same reserved range.
	ExitCodeInsufficientQuota = int32(237)
	// ExitCodeSubmitLimit is the exit code the runner reports (state FAILED, no site job) when the site
	// refused the submit because of its submit cap (AC-39). Next to ExitCodeInsufficientQuota.
	ExitCodeSubmitLimit = int32(238)
	// ExitCodeInvalidImage is the exit code the runner reports (state FAILED, no site job) for an unpullable image reference.
	ExitCodeInvalidImage = int32(239)
	// ReasonGPUViolation: on a node shared by several steps, the slot manager stopped this step for computing on a
	// GPU outside its set. The runner reports ExitCodeGPUViolation for it; the attempt is Failed with this reason.
	ReasonGPUViolation = "GPUViolation"
	// ExitCodeGPUViolation is the exit code the runner reports for a step stopped for GPUViolation. Next to
	// ExitCodeInvalidImage, in the same reserved range.
	ExitCodeGPUViolation = int32(240)
	// ExitCodeStagingMismatch is the exit code the runner reports (state FAILED, no site job) when the site's
	// staging tool speaks another staging contract.
	ExitCodeStagingMismatch = int32(241)
	// ExitCodeReservedName is the exit code the runner reports (state FAILED, no site job) when the dataset
	// holds an object whose name is reserved for the platform or whose path could leave the dataset's folder.
	ExitCodeReservedName = int32(242)
	// ExitCodeResultNotHandedBack is the exit code of a job whose step ended well but whose result could not be
	// handed back: the job script exits with it when it cannot read or upload the result file, and the runner
	// reports it (state FAILED) when it cannot fetch what the job uploaded, or before it submits anything when
	// the run has no way to hand a result back.
	ExitCodeResultNotHandedBack = int32(243)
	// ExitCodeInvalidResult is the exit code the runner reports (state FAILED) when the result file the job
	// handed back is not a result of the step contract.
	ExitCodeInvalidResult = int32(244)
	// ContextMaxLength and ResultMaxLength are the caps of the step contract (docs/step-contract.md, section 7):
	// what a step context and a result may cost. A value's length is never more than its cost, so the definition
	// allows every value within its cap.
	ContextMaxLength = 96 * 1024
	ResultMaxLength  = 8 * 1024
	// ContextEnv is the one value a step receives (docs/step-contract.md, section 2): the follower reads the
	// step context from it, and the runner and the step in the job get it under the same name.
	ContextEnv = "KAOS_STEP_CONTEXT"
	// StepKindLabel is the label every pod and HPC job of a step carries: the step's kind (kind= in pipeline.py).
	StepKindLabel = "platform.kubecore.io/step-kind"
	// DefaultIdleTimeoutMinutes is the idle window of a step that does not set one (D-72).
	DefaultIdleTimeoutMinutes = int32(20)
	// ReasonSiteUnavailable: the site does not answer (connection errors, 5xx, timeouts, slurmctld down). The job
	// WAITS (phase Waiting before admission, Admitted after it, no runner Job is created) and resumes when the
	// site answers; after the controller's --site-outage-max it ends Failed with this reason (PRD-HPC-1461
	// AC-28, AC-30). Also the pending reason a runner reports while it waits for the site.
	ReasonSiteUnavailable = "SiteUnavailable"
	// ConditionSiteUnavailable is the job condition that is True while the job waits for an unreachable site;
	// its lastTransitionTime is when the wait began, which the outage bound counts from.
	ConditionSiteUnavailable = "SiteUnavailable"
	// ReasonRunnerFailed: the runner Job ended (backoff exhausted) without a terminal report (phase 1).
	ReasonRunnerFailed = "RunnerFailed"
	// ReasonSiteMissing is the reason while the job's HPCSite is gone and the grace period runs.
	ReasonSiteMissing = "HPCSiteMissing"
	// ReasonInvalidSpec: the spec asks for something the API does not allow (phase 1: env outside
	// the allowlist). Terminal, with the phase Refused.
	ReasonInvalidSpec = "InvalidSpec"
	// ReasonTimeLimitRetry: the attempt hit a learned time limit (Slurm TIMEOUT) below the spec's
	// cap; the job runs once more at the declared limit (PRD-HPC-1461 D-67).
	ReasonTimeLimitRetry = "TimeLimitRetry"
	// ReasonNoProgress: a continuation did not get past the checkpoint it started from (D-75 3).
	ReasonNoProgress = "NoProgress"
	// Tier 1 (D-75): a step without a checkpoint is re-run from the start after an end that is not its
	// own fault, at most MaxReruns times. One reason per kind of end, so the user reads why.
	ReasonRerunNodeFail  = "RerunNodeFailure"
	ReasonRerunPreempted = "RerunPreempted"
	ReasonRerunLimit     = "RerunLimit"
	// MaxReruns caps the re-runs of a step that has no checkpoint.
	MaxReruns = int32(2)
	// DefaultMaxContinuations caps the continuations of a step discovered resumable (no spec.resumable).
	DefaultMaxContinuations = int32(5)
	// DefaultLeadMinutes is the wall-time signal lead of a step discovered resumable (D-75 4: fixed).
	DefaultLeadMinutes = int32(10)
)

// CancelFinalizer holds a serve-mode job until its runner Job is gone and its attempt is settled
// (PRD-HPC-1461 D-04): cancel is the controller's, so a lost follower never orphans a site job.
const CancelFinalizer = "hpc.kaos.io/cancel"

// CancelRequestedAnnotation is set by the controller on the job, with an RFC3339 time, BEFORE it
// deletes the runner Job for a real cancel (job deleted, Workflow deleted, shut down or ended). A
// runner pod also gets SIGTERM when its node drains or evicts it; the runner cancels the site job
// only when it finds this annotation on its HPCArgoWorkflowJob, otherwise it exits and the Job
// starts a replacement that re-attaches. The controller waits until the annotation is in the API
// (not just its cache) before it deletes the runner Job.
const CancelRequestedAnnotation = "hpc.kaos.io/cancel-requested"

// AllowedEnvNames are the only environment variables a job's spec may set on the runner
// (PRD-1461 phase 1 contract section 1). Everything that names an identity, a path, a class table
// or an endpoint comes from the HPCSite and the operator-rendered ConfigMap, never from the
// tenant (T-02). The CRD enforces the list with CEL; the controller enforces it again.
var AllowedEnvNames = []string{"KAOS_LIB_SUPPORT"}

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
	// App is the app the step belongs to, as the step context names it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=55
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	App string `json:"app"`
	// Context is the step context (docs/step-contract.md, section 3) in wire form, exactly as the follower
	// received it in KAOS_STEP_CONTEXT. The step in the job gets it under the same name, unchanged. Nothing
	// rewrites it. A job without it is refused.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=98304
	Context string `json:"context"`
	// Inputs are the datasets staged on the site before the step starts, each pinned to one commit: every
	// read of the context's ledger and every dataset a needed step wrote. The follower fills it from the
	// context and from nothing else; the runner stages exactly these commits and never resolves a version.
	// Absent: the step reads no dataset.
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	// +optional
	Inputs []StepInput `json:"inputs,omitempty"`
	// Resumable declares that the step continues across jobs: it gets the wall-time signal LeadMinutes before
	// its wall time, saves a checkpoint (its checkpoint folder, or the KAOS library) and exits 85, and the job
	// continues from that checkpoint in a new site job under this same HPCArgoWorkflowJob (D-38, D-44 as amended
	// 2026-11-05, F-29). A declared step may also ask for more wall time than the longest run the class allows: it
	// then runs as continuations. Absent: the step is not declared resumable, but one that left a checkpoint copy
	// is still continued (capped at 5) and one that did so successfully is verified resumable (D-74); a step
	// that is neither is refused with TooLongNotResumable when it asks for more than the longest run.
	// +optional
	Resumable *Resumable `json:"resumable,omitempty"`
	// ResumableOptOut is resumable=False: the step is never continued from a checkpoint, even when it
	// uses the checkpoint folder and the controller has verified it (D-74). A node failure still re-runs it.
	// +optional
	ResumableOptOut bool `json:"resumableOptOut,omitempty"`
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
	// IdleTimeoutMinutes is the idle watchdog's window (D-72, F-46): when the step's GPU (CPU on a
	// CPU class) utilisation stays below 5 % and its log does not grow for this many consecutive
	// minutes, the job ends the step with reason Idle. 0 disables the watchdog. Absent: 20.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=20
	// +optional
	IdleTimeoutMinutes *int32 `json:"idleTimeoutMinutes,omitempty"`
	// Env is extra environment for the step, from a fixed allowlist: KAOS_LIB_SUPPORT. Nothing else is
	// accepted (T-02).
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:XValidation:rule="self.all(e, e.name in ['KAOS_LIB_SUPPORT'])",message="env may only set KAOS_LIB_SUPPORT"
	// +listType=map
	// +listMapKey=name
	// +optional
	Env []NameValue `json:"env,omitempty"`
}

// StepInput is one dataset the step reads, at one commit.
type StepInput struct {
	// Name is the input's name: what the step asks for (input("name")). On the site the staged copy is the
	// folder of this name under the step's staged root.
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]([A-Za-z0-9_-]*[A-Za-z0-9])?$`
	Name string `json:"name"`
	// Repo is the dataset's repository: a platform Dataset is a repository named dataset-<name>.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^dataset-[a-z0-9][a-z0-9-]{0,54}$`
	Repo string `json:"repo"`
	// Commit is the lakeFS commit that is staged and read.
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	Commit string `json:"commit"`
	// Ref is what the run asked for (latest, a name, a commit). It is a label: the commit decides what is read.
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`
	// +optional
	Ref string `json:"ref,omitempty"`
	// Owner is the project that shares the dataset with this one. Absent: the project's own dataset. A job
	// with an owner is refused: a dataset of another project cannot be staged on an HPC site yet.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`
	// +optional
	Owner string `json:"owner,omitempty"`
	// Line is reserved for the branch form of a dataset (PRD kaos#1911 D-01). Nothing reads it yet.
	// +kubebuilder:validation:MaxLength=255
	// +optional
	Line string `json:"line,omitempty"`
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

// GPUUtilization is one GPU's utilisation in percent, as sampled on the compute node (nvidia-smi).
type GPUUtilization struct {
	// GPU is the GPU's position in the node's nvidia-smi list ("0", "1", ...).
	// +kubebuilder:validation:MaxLength=8
	GPU string `json:"gpu"`
	// Percent is the latest reading.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Percent int32 `json:"percent"`
	// MaxPercent is the highest reading of this site job.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxPercent int32 `json:"maxPercent"`
}

// Staging kinds: how the dataset copy on the site's scratch came to be.
const (
	StagingFirst  = "first"  // no earlier version of the repository was on scratch: the whole commit was copied
	StagingDelta  = "delta"  // only what differs from an earlier version was copied
	StagingCached = "cached" // the commit was already on scratch: nothing was copied
)

// StagingStatus says how one dataset of the step was staged on the site's scratch before the site job was
// submitted. It is bounded (one short record, no paths) and set by the runner; the follower prints it
// as one line in the step's log.
type StagingStatus struct {
	// Repo is the dataset repository's name.
	// +kubebuilder:validation:MaxLength=128
	Repo string `json:"repo"`
	// Commit is the pinned commit, shortened to 12 characters.
	// +kubebuilder:validation:MaxLength=12
	Commit string `json:"commit"`
	// Kind is first, delta or cached.
	// +kubebuilder:validation:Enum=first;delta;cached
	Kind string `json:"kind"`
	// Files is the number of files copied in this staging (0 when cached).
	// +kubebuilder:validation:Minimum=0
	Files int32 `json:"files"`
	// Bytes is the number of bytes copied in this staging (0 when cached).
	// +kubebuilder:validation:Minimum=0
	Bytes int64 `json:"bytes"`
	// Seconds is how long the staging took (0 when cached).
	// +kubebuilder:validation:Minimum=0
	Seconds int32 `json:"seconds"`
}

// QueueReplacedKept is how many replacements status.run.queueReplaced (and an attempt record) lists: the
// newest. The count of all of them is kept apart.
const QueueReplacedKept = 5

// QueueReplacement is one queued site job the runner replaced by a fresh one (PRD-HPC-1461 AC-35): the
// replacement loses the job's place in the site's queue, so it is recorded.
type QueueReplacement struct {
	// At is when the runner replaced the job.
	At metav1.Time `json:"at"`
	// OldSiteJobID is the cancelled job; NewSiteJobID its replacement.
	// +kubebuilder:validation:MaxLength=32
	OldSiteJobID string `json:"oldSiteJobID"`
	// +kubebuilder:validation:MaxLength=32
	NewSiteJobID string `json:"newSiteJobID"`
	// QueuedSeconds is how long the old job had been queued.
	// +kubebuilder:validation:Minimum=0
	QueuedSeconds int64 `json:"queuedSeconds"`
	// Reason: TokenRefresh (the job's wallet or token would expire before it started: the replacement
	// carries fresh credentials), HostLost (the host a guest waited for is gone), Replanned (the
	// controller planned a waiting guest as a host).
	Reason QueueReplacementReason `json:"reason"`
}

// QueueReplacementReason says why a queued site job was replaced.
// +kubebuilder:validation:Enum=TokenRefresh;HostLost;Replanned
type QueueReplacementReason string

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
	// ExitCode of the job's step; 85 means it saved and continues (D-44). Codes the job script and the runner
	// reserve: 210 (no Apptainer), 231 (image pull), 232 (staging), 233 (batch script), 234 (no verified
	// checkpoint to restore), 235 (checkpoint copy or job comment failed), 236 (Idle), 237 (InsufficientQuota),
	// 238 (SubmitLimit), 239 (InvalidImage), 240 (GPUViolation), 241 (StagingMismatch), 242 (ReservedName),
	// 243 (ResultNotHandedBack), 244 (InvalidResult).
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
	// HostJobOpen is set on a host's terminal report when its step ended but its Slurm job still holds the
	// node for guests (node sharing, D-26): the site charges the node until that job ends. The runner
	// reports the job's own end with HostJobOpen unset; until then the host's booking stays held.
	// +optional
	HostJobOpen bool `json:"hostJobOpen,omitempty"`
	// HostJobEndTime is the end of a host's Slurm job when it ended after the step (D-26): the attempt is
	// settled to it. EndTime stays the step's own end, so a learned limit comes from the step's run time,
	// never from the time the node was kept for guests (D-67).
	// +optional
	HostJobEndTime *metav1.Time `json:"hostJobEndTime,omitempty"`
	// CheckpointRef names the newest verified checkpoint copy the job left on the site's scratch, as
	// scratch://<tar path>#<sha256> (T-19). It is reported with every recoverable end (exit 85, wall time, node
	// failure, pre-emption, idle kill). Without it a step is not continued from a checkpoint.
	// +optional
	CheckpointRef string `json:"checkpointRef,omitempty"`
	// LogOffset is how far into the site job's output file the runner has printed (bytes), so a
	// restarted runner prints each line once (PRD-1461 D-65, AC-70).
	// +kubebuilder:validation:Minimum=0
	// +optional
	LogOffset int64 `json:"logOffset,omitempty"`
	// GPUUtilization is the step's GPU use as the on-node sampler last read it, per GPU (last value and
	// highest value of this site job). The runner reports it with the log offset every 30 s while the
	// job runs; the controller exports it as kaos_hpc_job_gpu_utilization_percent (PRD-1461 AC-69).
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=gpu
	// +optional
	GPUUtilization []GPUUtilization `json:"gpuUtilization,omitempty"`
	// Staged says how each of the step's inputs (spec.inputs) was staged before submit, one record per dataset
	// commit (first copy, delta or already cached; what was copied and how long it took). Absent when the step
	// has no input; an input the job stages itself has no record. The follower prints one "HPC: dataset ...
	// staged" line per record.
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	// +optional
	Staged []StagingStatus `json:"staged,omitempty"`
	// QueueReplaced lists the newest replacements of a queued site job of this attempt (at most
	// QueueReplacedKept, oldest first); QueueReplacedCount counts all of them. A queued job is replaced
	// before its credentials expire, which loses its place in the site's queue (AC-35).
	// +kubebuilder:validation:MaxItems=5
	// +optional
	QueueReplaced []QueueReplacement `json:"queueReplaced,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	QueueReplacedCount int32 `json:"queueReplacedCount,omitempty"`
	// LastLog is the tail of the step's log (a few lines, at most 2 KiB), reported only when the
	// idle watchdog ended the step, so the controller can name what the step last said (D-72).
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	LastLog string `json:"lastLog,omitempty"`
	// Message is a plain sentence from the submitter about why the attempt ended, set when the controller
	// cannot know it from the site (InsufficientQuota: what did not fit; SubmitLimit: the site's cap and the
	// count it read). At most 512 characters.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Message string `json:"message,omitempty"`
	// Checkpoint is what the job reported about the checkpoint copies it keeps (D-74, D-75): the newest
	// copy's progress and size, and the step's and project's total. Reported with every attempt end.
	// +optional
	Checkpoint *CheckpointInfo `json:"checkpoint,omitempty"`
	// QOS and Reservation are what the site job was actually submitted with. The submitter reports
	// them because it may fall back from a lane the site refused (PRD-HPC-1461 D-68).
	// +optional
	QOS string `json:"qos,omitempty"`
	// +optional
	Reservation string `json:"reservation,omitempty"`
}

// CheckpointInfo describes the newest persisted copy of a step's checkpoint folder.
type CheckpointInfo struct {
	// ProgressValues are the numbers the copy's progress is compared by (epoch, step, ...; most
	// significant first), when the step saved through the KAOS library. Empty: compare Digest.
	// +optional
	ProgressValues []int64 `json:"progressValues,omitempty"`
	// ProgressText is the progress in words, for example "epoch=3 step=1200".
	// +kubebuilder:validation:MaxLength=200
	// +optional
	ProgressText string `json:"progressText,omitempty"`
	// Digest identifies the folder's content (sha256 over its files' checksums). A copy with another digest
	// than the one before it counts as progress when the step reports no progress values.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Digest string `json:"digest,omitempty"`
	// Bytes is the size of the newest copy.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Bytes int64 `json:"bytes,omitempty"`
	// StepBytes is all the copies the step holds (at most two) and ProjectBytes all checkpoint copies of the
	// project, as read when the report was written.
	// +kubebuilder:validation:Minimum=0
	// +optional
	StepBytes int64 `json:"stepBytes,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	ProjectBytes int64 `json:"projectBytes,omitempty"`
	// SkippedReason is why the latest periodic copy was skipped (space guard), when it was.
	// +kubebuilder:validation:MaxLength=300
	// +optional
	SkippedReason string `json:"skippedReason,omitempty"`
}

// SiteObservation is what the controller's one batched poll per HPCSite per interval saw of the
// attempt's site job (PRD-HPC-1461 D-36, D-45). The controller writes it; the runner reads its job's
// state here instead of calling the site's REST API itself while the job is pending or running.
type SiteObservation struct {
	// JobID is the site job this observation is about (a resubmit changes it; a reader ignores an
	// observation of another id).
	JobID string `json:"jobID"`
	// State is the scheduler's job state(s), comma separated, for example PENDING or RUNNING. Empty:
	// the site no longer lists the job (it left slurmctld's memory), so the reader asks accounting.
	// +optional
	State string `json:"state,omitempty"`
	// PendingReason is Slurm's reason while the job is pending, when the site reports one.
	// +optional
	PendingReason string `json:"pendingReason,omitempty"`
	// Comment is the site job's comment, when the site reports it (node sharing, PRD-1609: a host
	// writes kaos-state=taken|done into its guest's comment). Nil: not reported, so a sharing
	// reader reads it from the job itself.
	// +optional
	Comment *string `json:"comment,omitempty"`
	// ObservedAt is when the controller read the site.
	ObservedAt metav1.Time `json:"observedAt"`
	// IntervalSeconds is the controller's poll interval; a reader treats an observation older than
	// three intervals as the controller being down.
	// +kubebuilder:validation:Minimum=1
	// +optional
	IntervalSeconds int32 `json:"intervalSeconds,omitempty"`
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
	// NodeMinutesSaved is set for an attempt the idle watchdog ended (reason Idle): the step's
	// limit minus the time it ran, times the nodes. A guest on a shared node reports 0 (its host's
	// booking covers the node).
	// +optional
	NodeMinutesSaved *int64 `json:"nodeMinutesSaved,omitempty"`
	// EndTime decides the budget period the spend counts in.
	EndTime metav1.Time `json:"endTime"`
	// HostJobOpen: this attempt was a host whose Slurm job still held the node for guests when its step
	// ended (D-26). Its settlement is provisional and its booking stays held until the runner reports the
	// job's own end; the attempt is then settled from the job's start to that end.
	// +optional
	HostJobOpen bool `json:"hostJobOpen,omitempty"`
	// QueueReplaced and QueueReplacedCount are copied from the attempt's run when it is settled: the
	// queued site jobs replaced during the attempt (the newest QueueReplacedKept, and the total). The
	// attempt's queue place was lost that many times (AC-35).
	// +kubebuilder:validation:MaxItems=5
	// +optional
	QueueReplaced []QueueReplacement `json:"queueReplaced,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	QueueReplacedCount int32 `json:"queueReplacedCount,omitempty"`
}

// PlacementSource says where a Placement's time limit came from.
// +kubebuilder:validation:Enum=declared;learned;timeout-retry
type PlacementSource string

const (
	// PlacementDeclared: the spec's wallTimeMinutes (too few samples, or the step is resumable).
	PlacementDeclared PlacementSource = "declared"
	// PlacementLearned: derived from earlier runs of the same step (D-67).
	PlacementLearned PlacementSource = "learned"
	// PlacementTimeoutRetry: the learned limit timed out; this is the one retry at the declared limit.
	PlacementTimeoutRetry PlacementSource = "timeout-retry"
)

// Placement is the time limit the controller chose for the job's current attempt (PRD-HPC-1461
// D-67, F-41). The spec's wallTimeMinutes is the cap: the limit never exceeds it.
type Placement struct {
	// TimeLimitMinutes is the limit the booking and the site job use.
	// +kubebuilder:validation:Minimum=1
	TimeLimitMinutes int32 `json:"timeLimitMinutes"`
	// Source of the limit.
	Source PlacementSource `json:"source"`
	// Samples is how many earlier runs of the same step the decision looked at.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Samples int32 `json:"samples,omitempty"`
	// QOS is the lane (site QoS) the attempt was placed in (PRD-HPC-1461 D-68, F-42). Empty: the
	// class's own QoS. It follows the QoS the runner reports using, so a lane the site refused
	// shows the class default here.
	// +optional
	QOS string `json:"qos,omitempty"`
	// Reservation is the site reservation the attempt was placed in. Empty: none.
	// +optional
	Reservation string `json:"reservation,omitempty"`
	// RefusedQOS is the lane the site refused for this attempt (the runner then submitted on the
	// class default, which QOS shows). Kept so the follower and the console can say so after QOS
	// was rewritten. Empty: no refusal.
	// +optional
	RefusedQOS string `json:"refusedQOS,omitempty"`
	// Warning is a plain-words notice for the pipeline author, set at admission when a recent run
	// of the same step ended WallTimeReached at this same declared limit. It changes nothing: the
	// limit is not raised. Empty: no warning.
	// +optional
	Warning string `json:"warning,omitempty"`
	// Resumable is whether the step is treated as resumable, and whether that is verified (D-74). Absent for
	// a step with no earlier continuation evidence and no declaration.
	// +optional
	Resumable *ResumableStatus `json:"resumable,omitempty"`
	// Share is the attempt's place in a shared node (PRD-HPC-1461 D-26, D-70): a host that owns a node, or a
	// guest planned onto a host's node. Absent for a step that does not share (resumable, no scope, or the site
	// does not share) and on jobs admitted before the field existed (then each job is its own host).
	// +optional
	Share *SharePlan `json:"share,omitempty"`
}

// ShareRole is what a sharing step is in the controller's plan of a shared node.
// +kubebuilder:validation:Enum=host;guest
type ShareRole string

const (
	// ShareHost: the step's Slurm job starts on a node of its own and its slot manager takes guests.
	ShareHost ShareRole = "host"
	// ShareGuest: the step is submitted deferred (it never starts on a node of its own) and is taken onto a
	// host's node by that host's slot manager. The runner submits a host only once the plan says host.
	ShareGuest ShareRole = "guest"
)

// SharePlan is the controller's decision, made at admission and revised while a guest is not taken (D-70).
type SharePlan struct {
	// Role of the attempt.
	Role ShareRole `json:"role"`
	// Host is the HPCArgoWorkflowJob (namespace/name) a guest is planned onto. Empty for a host.
	// +optional
	Host string `json:"host,omitempty"`
	// JobLimitMinutes is the time limit of a host's Slurm job: its step's limit padded for guests (twice, capped by the
	// class's and the lane's maximum). A guest must end 6 minutes before the host's job does. Zero for a guest.
	// +kubebuilder:validation:Minimum=0
	// +optional
	JobLimitMinutes int32 `json:"jobLimitMinutes,omitempty"`
	// Since is when the role was decided; a guest's wait for its host is measured from the later of it and the
	// host's own.
	Since metav1.Time `json:"since"`
	// Replans is how often a guest that was not taken was planned again (host ended, did not start in time, no
	// longer fits, or did not take it). At the bound the guest becomes a host.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replans int32 `json:"replans,omitempty"`
}

// ResumeInfo is what the next attempt resumes from.
type ResumeInfo struct {
	// CheckpointRef of the attempt that saved it.
	CheckpointRef string `json:"checkpointRef"`
	FromAttempt   int32  `json:"fromAttempt"`
	// ProgressValues and Digest of the checkpoint resumed from: the next attempt must get past them (D-75 3).
	// +optional
	ProgressValues []int64 `json:"progressValues,omitempty"`
	// +optional
	Digest string `json:"digest,omitempty"`
}

// ResumableStatus says whether the controller treats a step as resumable (D-74): discovered when its
// checkpoint folder persisted and verified when a continuation from it made progress.
type ResumableStatus struct {
	// Verified: an earlier continuation of the same step loaded its checkpoint and made progress.
	Verified bool `json:"verified"`
	// Since is when the latest evidence was recorded.
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
	// Reason is the evidence in words: what verified the step, or why it is not (for example NoProgress).
	// +kubebuilder:validation:MaxLength=300
	// +optional
	Reason string `json:"reason,omitempty"`
}

// ResumeEvidence is the result of one continuation: the history the controller derives Verified from.
type ResumeEvidence struct {
	// Verified is true when the continuation got past its checkpoint, false when it did not (NoProgress).
	Verified bool `json:"verified"`
	// At is when the continuation ended.
	At metav1.Time `json:"at"`
	// Reason in words.
	// +kubebuilder:validation:MaxLength=300
	// +optional
	Reason string `json:"reason,omitempty"`
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
	// Placement is the time limit chosen for the current attempt, set when it is admitted. Absent
	// on jobs admitted before the field existed: the spec's wallTimeMinutes applies.
	// +optional
	Placement *Placement `json:"placement,omitempty"`
	// Reruns so far: how often a step without a checkpoint was re-run from the start after a node failure
	// or pre-emption (tier 1, D-75), at most MaxReruns.
	// +optional
	Reruns int32 `json:"reruns,omitempty"`
	// ResumeEvidence is what the latest continuation showed about the step's checkpoints (D-74).
	// +optional
	ResumeEvidence *ResumeEvidence `json:"resumeEvidence,omitempty"`
	// Resume is set for a continuation: the submitter passes it to the new site job.
	// +optional
	Resume *ResumeInfo `json:"resume,omitempty"`
	// Attempts are the finished, settled site jobs of this HPCArgoWorkflowJob.
	// +optional
	Attempts []AttemptRecord `json:"attempts,omitempty"`
	// Run is the submitter's report of the site job.
	// +optional
	Run *RunStatus `json:"run,omitempty"`
	// Result is the step's result (docs/step-contract.md, section 4) in wire form, as the job handed it back:
	// the runner writes it with its report of a step that ended well, after it checked it. Set only on a job
	// that Succeeded; absent when the step wrote none. The follower hands it on as the step's kaos-result.
	// +kubebuilder:validation:MaxLength=8192
	// +optional
	Result string `json:"result,omitempty"`
	// PendingReason is the scheduler's pending reason while the site job is queued (runner's poll).
	// +optional
	PendingReason string `json:"pendingReason,omitempty"`
	// SiteObservation is the controller's batched reading of the site job's state (D-36). Optional:
	// absent when the poller is off or has not read the job yet.
	// +optional
	SiteObservation *SiteObservation `json:"siteObservation,omitempty"`
	// Runner is the runner Job of the current attempt (serve mode).
	// +optional
	Runner *RunnerRef `json:"runner,omitempty"`
	// SiteMissingSince is when the controller first found the job's HPCSite (or its project label) gone while
	// the job holds a booking; cleared when it is back. After the grace period the job is cancelled.
	// +optional
	SiteMissingSince *metav1.Time `json:"siteMissingSince,omitempty"`
	// Sweep is set while the attempt is ending but the site has not yet confirmed that its job is over
	// (the runner Job failed or was deleted, or a cancel was requested): the booking is kept until it has.
	// +optional
	Sweep *SweepStatus `json:"sweep,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SweepStatus is the pending end of an attempt whose site job is not yet proven over (PRD-HPC-1461 AC-08).
type SweepStatus struct {
	// Phase, Reason and Message are what the job becomes once the site has confirmed the end.
	Phase   Phase  `json:"phase"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
	// Since is when the sweep began; the give-up time counts from it.
	Since metav1.Time `json:"since"`
	// Launches is how many cancel-only runner Jobs were started for it.
	// +optional
	Launches int32 `json:"launches,omitempty"`
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
// needs from the platform and the run (image, command, step context, inputs, credentials); it cannot run on its
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

// EffectiveIdleTimeoutMinutes is the idle watchdog's window in minutes: the spec's value, 20 when
// absent (a job created before the field existed), 0 meaning the watchdog is off.
func (j *HPCArgoWorkflowJob) EffectiveIdleTimeoutMinutes() int32 {
	if j.Spec.IdleTimeoutMinutes == nil {
		return DefaultIdleTimeoutMinutes
	}
	return *j.Spec.IdleTimeoutMinutes
}

// EffectiveWallMinutes is the time limit of the job's current attempt: the controller's
// placement when it chose one, else the spec's wallTimeMinutes (the cap).
func (j *HPCArgoWorkflowJob) EffectiveWallMinutes() int32 {
	if p := j.Status.Placement; p != nil && p.TimeLimitMinutes > 0 {
		return p.TimeLimitMinutes
	}
	return j.Spec.WallTimeMinutes
}

// EffectiveResumable is how the step is treated: its declaration, else (when it did not opt out) the default
// lead and cap of a step the controller found resumable from its checkpoint history. nil: not resumable.
func (j *HPCArgoWorkflowJob) EffectiveResumable() *Resumable {
	if j.Spec.ResumableOptOut {
		return nil
	}
	if j.Spec.Resumable != nil {
		return j.Spec.Resumable
	}
	if p := j.Status.Placement; p != nil && p.Resumable != nil && p.Resumable.Verified {
		return &Resumable{LeadMinutes: DefaultLeadMinutes, MaxContinuations: DefaultMaxContinuations}
	}
	return nil
}
