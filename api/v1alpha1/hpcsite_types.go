package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Tier is the kind of node-hours a class spends. Sites meter every allocation separately (MeluXina: Slurm
// GrpTRESMins per partition, AssocGrp<TRES>Minutes when one is spent): GPU, CPU, FPGA and large memory
// nodes each have their own budget (PRD-HPC-1461 AC-84).
// +kubebuilder:validation:Enum=gpu;cpu;fpga;largemem
type Tier string

const (
	TierGPU      Tier = "gpu"
	TierCPU      Tier = "cpu"
	TierFPGA     Tier = "fpga"
	TierLargeMem Tier = "largemem"
)

// Tiers lists every allocation a project can have a budget for, in the order they are shown. Add a new
// allocation here, in Budget.NodeHours, SiteLeft.NodeHours, HPCSiteStatus and its ledger accessors; nothing else enumerates them.
var Tiers = []Tier{TierGPU, TierCPU, TierFPGA, TierLargeMem}

// Label is the allocation's name in a sentence a person reads: "GPU", "CPU", "FPGA", "large memory".
func (t Tier) Label() string {
	switch t {
	case TierGPU:
		return "GPU"
	case TierCPU:
		return "CPU"
	case TierFPGA:
		return "FPGA"
	case TierLargeMem:
		return "large memory"
	}
	return string(t)
}

// Transport is how the controller reaches the site's scheduler (PRD-HPC-1461 D-35).
// Only slurmrestd is built in v1alpha1; the others are reserved names.
// +kubebuilder:validation:Enum=slurmrestd;ssh-slurm-cli;firecrest
type Transport string

const (
	TransportSlurmREST Transport = "slurmrestd"
)

// ConditionBudgetWarning is the HPCSite condition that is True while a tier's used plus booked
// node-minutes are at or above budget.warnAtPercent of what the project may spend this period
// (PRD-HPC-1461 AC-22). Reasons: ReasonThresholdCrossed, ReasonBelowThreshold.
const (
	ConditionBudgetWarning = "BudgetWarning"
	ReasonThresholdCrossed = "ThresholdCrossed"
	ReasonBelowThreshold   = "BelowThreshold"
)

// ConditionSiteReachable is the HPCSite condition that is False while the site's REST API does not answer
// (PRD-HPC-1461 AC-30: health shown in the console). Reasons: ReasonSiteAnswering, ReasonSiteNotAnswering.
const (
	ConditionSiteReachable = "SiteReachable"
	ReasonSiteAnswering    = "Answering"
	ReasonSiteNotAnswering = "NotAnswering"
)

// AtLimitPolicy is what happens when a project's share is spent (D-13).
// +kubebuilder:validation:Enum=block;warn
type AtLimitPolicy string

const (
	AtLimitBlock AtLimitPolicy = "block"
	AtLimitWarn  AtLimitPolicy = "warn"
)

// SiteClass is a site-specific choice in the run form, for example meluxina-gpu (D-08, D-28).
type SiteClass struct {
	// Name is unique within the site, for example meluxina-gpu.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// Partition and QOS the class submits to.
	Partition string `json:"partition"`
	// +optional
	QOS string `json:"qos,omitempty"`
	// Tier is whose node-hours the class spends.
	Tier Tier `json:"tier"`
	// GPUs per step of this class (0 for CPU classes).
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUs int32 `json:"gpus,omitempty"`
	// MaxWallTimeMinutes is the site's limit for the class's partition and QOS.
	// +kubebuilder:validation:Minimum=1
	MaxWallTimeMinutes int32 `json:"maxWallTimeMinutes"`
	// Lanes are the names of the profile's QoS this class may use (D-66). Empty means only QOS.
	// +listType=set
	// +optional
	Lanes []string `json:"lanes,omitempty"`
}

// SiteQOS is one QoS (lane) the project's account may use on the site, as the site reports it (D-66).
type SiteQOS struct {
	// Name of the QoS at the site, for example dev.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Priority is the QoS priority.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Priority int64 `json:"priority,omitempty"`
	// MaxWallTimeMinutes is the QoS's wall-time limit. Zero means the site reports none.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxWallTimeMinutes int32 `json:"maxWallTimeMinutes,omitempty"`
	// MaxJobsPerUser is the QoS's running-jobs-per-user limit. Zero means the site reports none.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxJobsPerUser int32 `json:"maxJobsPerUser,omitempty"`
	// Preempts names the QoS this one preempts.
	// +listType=set
	// +optional
	Preempts []string `json:"preempts,omitempty"`
	// UsageFactorMilli is the usage factor times 1000 (1.0 is 1000), an integer so the CRD carries no float.
	// +kubebuilder:validation:Minimum=0
	// +optional
	UsageFactorMilli int32 `json:"usageFactorMilli,omitempty"`
}

// SiteReservation is a reservation the project's account may use on the site (D-66).
type SiteReservation struct {
	// Name of the reservation at the site, for example gpudev.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Partition the reservation covers.
	// +optional
	Partition string `json:"partition,omitempty"`
	// NodeCount is the number of reserved nodes.
	// +kubebuilder:validation:Minimum=0
	// +optional
	NodeCount int32 `json:"nodeCount,omitempty"`
	// EndTime is when the reservation ends.
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`
}

// SiteProfile is the site part of an HPCSite: what the operator needs to reach and understand the site. It is
// rendered by the platform that integrates the operator (in KAOS, from its curated site profile), never set by a
// tenant (F-03, D-47).
type SiteProfile struct {
	// Transport to the scheduler.
	// +kubebuilder:default=slurmrestd
	Transport Transport `json:"transport"`
	// Endpoint of the scheduler API, for example https://slurm-api.example.org.
	// +kubebuilder:validation:Pattern=`^https://`
	Endpoint string `json:"endpoint"`
	// RESTVersion of the scheduler API, for example v0.0.44.
	// +kubebuilder:validation:Pattern=`^v[0-9]+\.[0-9]+\.[0-9]+$`
	RESTVersion string `json:"restVersion"`
	// LoginHosts the controller may SSH to for token minting (host:port).
	// +optional
	LoginHosts []string `json:"loginHosts,omitempty"`
	// ScratchRoot is the per-project working area on the site's shared filesystem.
	// +optional
	ScratchRoot string `json:"scratchRoot,omitempty"`
	// HomeRoot is the directory holding the users' home directories on the site, for example
	// /home/users. The runner's HOME is HomeRoot/<identity.user>. Empty means /home/users.
	// +optional
	HomeRoot string `json:"homeRoot,omitempty"`
	// MaxSubmitJobs is the site's cap on a user's queued plus running jobs (Slurm MaxSubmitJobs, per-user
	// submit limit; AC-39). A new job that would exceed it FAILS with reason SubmitLimit instead of an
	// opaque Slurm error; it does not wait. Zero or absent: no cap known to KAOS (the site's own refusal
	// is still reported as SubmitLimit).
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSubmitJobs int32 `json:"maxSubmitJobs,omitempty"`
	// Classes offered by the site.
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Classes []SiteClass `json:"classes"`
	// QOS are the lanes the project's account may use, with their limits (D-66). Discovered, optional.
	// +listType=map
	// +listMapKey=name
	// +optional
	QOS []SiteQOS `json:"qos,omitempty"`
	// Reservations the project's account may use (D-66). Discovered, optional.
	// +listType=map
	// +listMapKey=name
	// +optional
	Reservations []SiteReservation `json:"reservations,omitempty"`
}

// SecretKeyRef names one key of a Secret in the HPCSite's namespace.
type SecretKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// ProjectIdentity is the site identity KAOS uses for one KAOS project (CON-03). Never shared.
type ProjectIdentity struct {
	// User is the site user name, for example u100001.
	User string `json:"user"`
	// Account is the site account (subscription) the jobs are charged to.
	Account string `json:"account"`
	// SSHKeySecretRef holds the private key used to mint scheduler tokens (D-19).
	SSHKeySecretRef SecretKeyRef `json:"sshKeySecretRef"`
}

// BudgetSource is where a project's node-hours come from.
// +kubebuilder:validation:Enum=share;site
type BudgetSource string

const (
	// BudgetSourceShare: the project's own cap per calendar month UTC (D-10, D-11), bounded by
	// SiteLeft when the hub renders it: the controller admits against min(share left, site left).
	BudgetSourceShare BudgetSource = "share"
	// BudgetSourceSite: what the site reports is left on the project's own account, polled
	// (PRD-HPC-1461 D-56). The project owns its account, so no share has to be typed in.
	BudgetSourceSite BudgetSource = "site"
)

// Budget is the project's node-hours per tier: a fixed share per period (D-10, D-11), or what
// the site reports is left on the project's account (source site, D-56).
type Budget struct {
	// Source: share (default) or site. With site, the NodeHours figures (GPU, CPU, FPGA, large memory) are what
	// the site reported left at ObservedAt; the site's figure already counts every job that ran, so
	// only bookings and attempts that ended after ObservedAt are taken off it.
	// +kubebuilder:default=share
	// +optional
	Source BudgetSource `json:"source,omitempty"`
	// ObservedAt is when the site reported the remaining node-hours (source site).
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
	// AllocationRef is reserved and unused: the share is the project's own cap, with no org allocation.
	// +optional
	AllocationRef string `json:"allocationRef,omitempty"`
	// SiteLeft is what the site reported left on the project's account (source share only): an
	// upper bound beside the share. Admission counts against the smaller of the share left this
	// month and SiteLeft less what ended since it was observed. Absent: the share alone.
	// +optional
	SiteLeft *SiteLeft `json:"siteLeft,omitempty"`
	// GPUNodeHours per period (source share) or left on the account (source site). Zero means
	// the project may not spend GPU node-hours.
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUNodeHours int64 `json:"gpuNodeHours,omitempty"`
	// CPUNodeHours per period (source share) or left on the account (source site). Zero means the project
	// may not spend CPU node-hours.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CPUNodeHours int64 `json:"cpuNodeHours,omitempty"`
	// FPGANodeHours per period (source share) or left on the account (source site). Source share: absent or
	// zero means the project may not spend FPGA node-hours. Source site: absent means the site gave no figure
	// (not bounded by the ledger, use is still recorded); an explicit value, 0 included, bounds it (AC-84).
	// +kubebuilder:validation:Minimum=0
	// +optional
	FPGANodeHours *int64 `json:"fpgaNodeHours,omitempty"`
	// LargeMemNodeHours per period (source share) or left on the account (source site). Absent and zero mean
	// what they do for FPGANodeHours.
	// +kubebuilder:validation:Minimum=0
	// +optional
	LargeMemNodeHours *int64 `json:"largeMemNodeHours,omitempty"`
	// GrantNodeHours is what the site granted the project for the month, per allocation (source site only):
	// the base of the warning percentage (AC-22). With source site the figures above are what is LEFT at
	// ObservedAt, so used since then over what was left stays near 0 all month; the share of the grant
	// spent or committed is (grant - left at ObservedAt + used since + booked) / grant. It never changes
	// admission or ceilings, which keep using what is left (D-56). Absent, or for an allocation without
	// a figure, or when a figure is below what is left, or ObservedAt is from before this month: the
	// warning keeps the earlier base (used plus booked over used plus what is left).
	// +optional
	GrantNodeHours *GrantNodeHours `json:"grantNodeHours,omitempty"`
	// AtLimit: block (default) or warn (D-13).
	// +kubebuilder:default=block
	// +optional
	AtLimit AtLimitPolicy `json:"atLimit,omitempty"`
	// WarnAtPercent is the percentage of what the project may spend this month (used plus booked, over used
	// plus the lower of share left and site left; with source site and a grantNodeHours figure, spent and committed over the grant) at which the BudgetWarning condition, event and metric are
	// raised for an allocation. Raised whatever AtLimit says; AtLimit decides only what happens at the limit.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	// +optional
	WarnAtPercent int32 `json:"warnAtPercent,omitempty"`
}

// GrantNodeHours is the site's grant of node-hours for the month per allocation, as the site reported it
// at the budget's ObservedAt. A figure absent means the site gave none for that allocation; 0 is not
// a grant (a spent or empty allocation has no percentage) and is treated as absent.
type GrantNodeHours struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPU *int64 `json:"gpu,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	CPU *int64 `json:"cpu,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	FPGA *int64 `json:"fpga,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	LargeMem *int64 `json:"largeMem,omitempty"`
}

// Of is the grant of one allocation in node-hours, and whether the site gave a positive figure.
func (g *GrantNodeHours) Of(t Tier) (int64, bool) {
	if g == nil {
		return 0, false
	}
	var v *int64
	switch t {
	case TierGPU:
		v = g.GPU
	case TierCPU:
		v = g.CPU
	case TierFPGA:
		v = g.FPGA
	case TierLargeMem:
		v = g.LargeMem
	}
	if v == nil || *v <= 0 {
		return 0, false
	}
	return *v, true
}

// SiteLeft is the site's remaining node-hours per tier, as the site reported at ObservedAt.
type SiteLeft struct {
	// GPUNodeHours left on the account. Omitted means 0 left: a spent allocation.
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUNodeHours int64 `json:"gpuNodeHours,omitempty"`
	// CPUNodeHours left on the account. Omitted means 0 left: a spent allocation.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CPUNodeHours int64 `json:"cpuNodeHours,omitempty"`
	// FPGANodeHours left on the account (AC-84). Absent means the site gave no figure: no site bound, the
	// share alone applies. An explicit 0 (or a negative remainder) means the allocation is spent.
	// +optional
	FPGANodeHours *int64 `json:"fpgaNodeHours,omitempty"`
	// LargeMemNodeHours left on the account (AC-84). Absent and zero mean what they do for FPGANodeHours.
	// +optional
	LargeMemNodeHours *int64 `json:"largeMemNodeHours,omitempty"`
	// ObservedAt is when the site reported it. Without it, or from before the current period, the
	// figure is ignored: it cannot be placed against what ended since.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// Limits are the project's central job limits (F-11, D-49).
type Limits struct {
	// MaxConcurrentJobs: extra jobs WAIT in KAOS, they are never refused (D-49). Zero means no limit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConcurrentJobs int32 `json:"maxConcurrentJobs,omitempty"`
	// MaxWallTimeMinutes caps any job's wall time; above it the job is refused at admission.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxWallTimeMinutes int32 `json:"maxWallTimeMinutes,omitempty"`
	// CheckpointBudgetPercent is the share of the project's scratch space quota that step checkpoints may
	// hold (D-75). A periodic copy that would exceed it, or push the project over 80 % of its space or file
	// quota, is skipped. Absent or zero: 10.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +optional
	CheckpointBudgetPercent int32 `json:"checkpointBudgetPercent,omitempty"`
}

// HPCSiteSpec is one project's whole specification for one site (D-47).
// +kubebuilder:validation:XValidation:rule="self.site == oldSelf.site",message="site is immutable"
type HPCSiteSpec struct {
	// Site is the site profile name, for example meluxina.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Site string `json:"site"`
	// Profile is the site part, rendered by the integrating platform.
	Profile SiteProfile `json:"profile"`
	// Identity is the project identity on the site.
	Identity ProjectIdentity `json:"identity"`
	// Budget is what the project may spend on this site, one figure per allocation (gpu, cpu, fpga, largemem).
	Budget Budget `json:"budget"`
	// Limits are the project's central job limits.
	// +optional
	Limits Limits `json:"limits,omitempty"`
	// Sharing lets the project's one-GPU jobs share nodes (PRD-1609, D-26, D-50).
	// +optional
	Sharing bool `json:"sharing,omitempty"`
	// AllowedClasses restricts the profile's classes for this project; empty allows them all.
	// +optional
	AllowedClasses []string `json:"allowedClasses,omitempty"`
}

// TierLedger is the project's spend in one tier for the current period, in node-minutes (the
// site meters node-minutes; shares are set in node-hours).
type TierLedger struct {
	// UsedNodeMinutes settled from finished jobs this period.
	UsedNodeMinutes int64 `json:"usedNodeMinutes"`
	// BookedNodeMinutes held by admitted jobs that have not ended (worst case, D-12).
	BookedNodeMinutes int64 `json:"bookedNodeMinutes"`
	// LeftNodeMinutes = share - used - booked, never below zero.
	LeftNodeMinutes int64 `json:"leftNodeMinutes"`
	// Unbounded is true when the site gave no figure for this allocation (source site, AC-84): nothing
	// limits it here, so leftNodeMinutes is 0 and means nothing. Used and booked are still recorded.
	// +optional
	Unbounded bool `json:"unbounded,omitempty"`
	// Percent is the share of the month's allowance that is spent or committed, rounded down: the figure
	// the BudgetWarning condition and the kaos_hpc_budget_percent metric use (ledger.Tier.Percent; with a
	// grant under source site it is against the grant). -1 when there is nothing to be a percentage of
	// (unbounded, or no allowance). Always written, so 0 means 0 percent, not unknown.
	// +kubebuilder:validation:Minimum=-1
	// +optional
	Percent int32 `json:"percent"`
}

// RecentJobsMax bounds HPCSiteStatus.RecentJobs: every active job plus the jobs that ended in the
// last RecentJobsWindow, newest first, cut at this many entries or at RecentJobsMaxBytes, whichever
// comes first. A typical entry is about 0.6 KiB (300 of them about 180 KiB); a theoretical worst
// entry (every bounded field at its schema maximum) is about 2.9 KiB, which the byte cap catches.
const RecentJobsMax = 300

// RecentJobsMaxBytes is the most the JSON of HPCSiteStatus.RecentJobs may take, far below the etcd
// object limit (about 1.5 MiB) and leaving room for the rest of the status.
const RecentJobsMaxBytes = 256 * 1024

// RecentJobsWindow is how long an ended job stays in HPCSiteStatus.RecentJobs.
const RecentJobsWindow = 7 * 24 * time.Hour

// RecentJobMessageMax is the longest RecentJob.Message, in characters.
const RecentJobMessageMax = 512

// RecentJob is the console's view of one HPCArgoWorkflowJob of the project (PRD-HPC-1461 F-43,
// AC-76, AC-26, AC-46). Copied from the job's status when the ledger is published: no site call.
type RecentJob struct {
	// Name and Namespace of the HPCArgoWorkflowJob (the namespace is the environment's).
	Name string `json:"name"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Step is the DAG step's name and Workflow the Argo Workflow it belongs to.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Step string `json:"step,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Workflow string `json:"workflow,omitempty"`
	// Phase and Reason of the job, for example Waiting / WaitingForBudget or Failed / Idle.
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Reason string `json:"reason,omitempty"`
	// Message is the job's status.message (a plain sentence for the user), cut to RecentJobMessageMax characters.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Message string `json:"message,omitempty"`
	// SiteState is the scheduler's state of the current site job (PENDING, RUNNING, ...).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	SiteState string `json:"siteState,omitempty"`
	// PendingReason is the scheduler's reason while the site job is queued (for example Priority).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	PendingReason string `json:"pendingReason,omitempty"`
	// QOS and Reservation are the lane the current attempt was placed in; RefusedQOS is the lane
	// the site refused (the job then ran on the class default).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	QOS string `json:"qos,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Reservation string `json:"reservation,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	RefusedQOS string `json:"refusedQOS,omitempty"`
	// TimeLimitMinutes is the current attempt's limit; LimitSource says where it came from and
	// LimitSamples how many earlier runs of the step it learned from.
	// +optional
	TimeLimitMinutes int32 `json:"timeLimitMinutes,omitempty"`
	// +optional
	LimitSource PlacementSource `json:"limitSource,omitempty"`
	// +optional
	LimitSamples int32 `json:"limitSamples,omitempty"`
	// IdleTimeoutMinutes is the idle watchdog's window (0 means off).
	// +optional
	IdleTimeoutMinutes int32 `json:"idleTimeoutMinutes,omitempty"`
	// Attempt is the current attempt (1 for the first site job).
	// +optional
	Attempt int32 `json:"attempt,omitempty"`
	// SiteJobID is the scheduler's job id; HostSiteJobID is set when the job runs as a guest on a
	// host job's node (node sharing).
	// +kubebuilder:validation:MaxLength=63
	// +optional
	SiteJobID string `json:"siteJobID,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	HostSiteJobID string `json:"hostSiteJobID,omitempty"`
	// CreatedAt, StartTime and EndTime; start and end are as the site reports them.
	CreatedAt metav1.Time `json:"createdAt"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`
	// BookedNodeMinutes is what the current attempt holds, or for an ended job the limit times
	// nodes of its last attempt. SettledNodeMinutes is the spend of the finished attempts.
	// +optional
	BookedNodeMinutes int64 `json:"bookedNodeMinutes,omitempty"`
	// +optional
	SettledNodeMinutes int64 `json:"settledNodeMinutes,omitempty"`
	// QueueReplacedCount is how many times the job's queue place was lost to a replacement of its queued
	// site job (token refresh), over all attempts; the console says "queue place lost N times". Only a
	// count: the records are in the job's status (AC-35).
	// +optional
	QueueReplacedCount int32 `json:"queueReplacedCount,omitempty"`
}

// ResumableStepsMax bounds HPCSiteStatus.ResumableSteps (one entry per app and step).
const ResumableStepsMax = 100

// ResumableStep is what the controller recorded about one step of one app (D-74): whether a
// continuation of it loaded its checkpoint and got past it. Only names, a verdict, a time and a
// reason in words: nothing about the jobs. The hub copies the list into the project's status and
// the app's platform context, where the pipeline linter reads it.
type ResumableStep struct {
	// App is the app the step belongs to: the last path segment of the step's image repository
	// (the pipeline builds one image repository per app).
	// +kubebuilder:validation:MaxLength=253
	App string `json:"app"`
	// Step is the DAG step's name.
	// +kubebuilder:validation:MaxLength=253
	Step string `json:"step"`
	// Verified is true when the latest evidence is a continuation that got past its checkpoint and
	// false when it did not (NoProgress).
	Verified bool `json:"verified"`
	// Since is when the latest evidence was recorded.
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
	// Reason is the evidence in words.
	// +kubebuilder:validation:MaxLength=300
	// +optional
	Reason string `json:"reason,omitempty"`
}

// SiteStorage is the project's scratch storage on the site: the quota (the whole subscription's,
// shared with its other users) and which of KAOS's own categories use it. All sizes are bytes.
type SiteStorage struct {
	// SpaceUsed and SpaceLimit are the quota's used and limit bytes; a limit of 0 means none set.
	// +optional
	SpaceUsed int64 `json:"spaceUsed,omitempty"`
	// +optional
	SpaceLimit int64 `json:"spaceLimit,omitempty"`
	// FilesUsed and FilesLimit are the quota's used and limit file counts (0 limit: none set).
	// +optional
	FilesUsed int64 `json:"filesUsed,omitempty"`
	// +optional
	FilesLimit int64 `json:"filesLimit,omitempty"`
	// ByCategory splits the used space and files: datasets, images, checkpoints, work, and
	// other (the rest of the quota: other users and projects of the subscription).
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	// +optional
	ByCategory []StorageCategory `json:"byCategory,omitempty"`
	// Images breaks the images category down (PRD-HPC-1461 AC-65): SIFs, staged tars, the layer
	// cache, what a queued or running job leases, the largest (app, step) lineages, the images
	// budget and the last cleanup. Bounded: at most ImageLineagesMax lineages are named.
	// +optional
	Images *ImageStorage `json:"images,omitempty"`
	// ObservedAt is when the quota was read; MeasuredAt is when the category sizes were last
	// measured (re-measured at most hourly, the quota on every report).
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
	// +optional
	MeasuredAt *metav1.Time `json:"measuredAt,omitempty"`
}

// ImageLineagesMax bounds ImageStorage.Lineages (the HPCSite object is capped near 256 KiB).
const ImageLineagesMax = 20

// ImageGroup is a count, the allocated bytes and the age of the oldest member (by last use).
type ImageGroup struct {
	// +optional
	Count int64 `json:"count,omitempty"`
	// Space is allocated bytes, as the site's quota counts them.
	// +optional
	Space int64 `json:"space,omitempty"`
	// +optional
	OldestAgeSeconds int64 `json:"oldestAgeSeconds,omitempty"`
}

// ImageCache is the Apptainer layer cache.
type ImageCache struct {
	// +optional
	Space int64 `json:"space,omitempty"`
	// +optional
	Files int64 `json:"files,omitempty"`
	// Partial is true when the measurement hit its time bound (a lower bound).
	// +optional
	Partial bool `json:"partial,omitempty"`
}

// ImageLineage is the SIFs of one (app, step): the last path segment of the step's image repository
// and the DAG step's name. Normally one SIF (the newest) plus those a queued or running job still uses.
type ImageLineage struct {
	// +kubebuilder:validation:MaxLength=253
	App string `json:"app"`
	// +kubebuilder:validation:MaxLength=253
	Step string `json:"step"`
	// +optional
	SIFs int64 `json:"sifs,omitempty"`
	// +optional
	Space int64 `json:"space,omitempty"`
	// NewestAgeSeconds and OldestAgeSeconds are the age of the newest and oldest build of the lineage.
	// +optional
	NewestAgeSeconds int64 `json:"newestAgeSeconds,omitempty"`
	// +optional
	OldestAgeSeconds int64 `json:"oldestAgeSeconds,omitempty"`
}

// ImageCleanupReason is what one kind of deletion removed in the last cleanup.
type ImageCleanupReason struct {
	// Reason is superseded, orphan, unused, tar_built, tar_unleased, stale_part, orphan_meta or over_budget.
	// +kubebuilder:validation:MaxLength=64
	Reason string `json:"reason"`
	// +optional
	Count int64 `json:"count,omitempty"`
	// +optional
	Space int64 `json:"space,omitempty"`
}

// ImageCleanup is the summary of the stager's last image cleanup.
type ImageCleanup struct {
	// +optional
	At *metav1.Time `json:"at,omitempty"`
	// FreedBytes is the allocated bytes the cleanup released (files and layer cache).
	// +optional
	FreedBytes int64 `json:"freedBytes,omitempty"`
	// +optional
	CacheFilesRemoved int64 `json:"cacheFilesRemoved,omitempty"`
	// OverBudget is true when leased files alone kept the images over the budget.
	// +optional
	OverBudget bool `json:"overBudget,omitempty"`
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	// +optional
	ByReason []ImageCleanupReason `json:"byReason,omitempty"`
}

// ImageStorage is what the images category of SiteStorage holds, written with the rest of the
// storage report by the project's snapshot job from the site stager's `usage` (AC-65). All sizes
// are allocated bytes.
type ImageStorage struct {
	// SIFs are the built images in the SIF cache, Tars the staged docker-archives waiting for a
	// build, Parts half-written files of builds and streams in flight, UnlabeledSIFs the SIFs with
	// no (app, step) label (built before the lifecycle existed; they go after three days).
	// +optional
	SIFs ImageGroup `json:"sifs,omitempty"`
	// +optional
	Tars ImageGroup `json:"tars,omitempty"`
	// +optional
	Parts ImageGroup `json:"parts,omitempty"`
	// +optional
	UnlabeledSIFs ImageGroup `json:"unlabeledSifs,omitempty"`
	// Leased is what a queued or running job names (never deleted). Absent when the queue could not
	// be read.
	// +optional
	Leased *ImageGroup `json:"leased,omitempty"`
	// +optional
	Cache *ImageCache `json:"cache,omitempty"`
	// Lineages are the largest (app, step) groups, at most ImageLineagesMax; LineagesTotal counts all.
	// +kubebuilder:validation:MaxItems=20
	// +listType=atomic
	// +optional
	Lineages []ImageLineage `json:"lineages,omitempty"`
	// +optional
	LineagesTotal int64 `json:"lineagesTotal,omitempty"`
	// BudgetBytes is the cap on SIFs, tars and layer cache (default 60 GB). Over it, the cleanup
	// removes the least recently used SIFs no job leases; OverBudget is true when leased files alone
	// keep the images above it.
	// +optional
	BudgetBytes int64 `json:"budgetBytes,omitempty"`
	// +optional
	OverBudget bool `json:"overBudget,omitempty"`
	// +optional
	LastCleanup *ImageCleanup `json:"lastCleanup,omitempty"`
}

// StorageCategory is one use of the project's scratch storage.
type StorageCategory struct {
	// Name is datasets, images, checkpoints, work or other.
	Name string `json:"name"`
	// Space is bytes and Files the file (and folder) count, as the site's quota counts them.
	// +optional
	Space int64 `json:"space,omitempty"`
	// +optional
	Files int64 `json:"files,omitempty"`
	// Partial is true when the measurement hit its time bound: the figure is a lower bound (for
	// other, an upper bound).
	// +optional
	Partial bool `json:"partial,omitempty"`
}

// HPCSiteStatus is the controller's view of the project's site use.
type HPCSiteStatus struct {
	// PeriodStart is the start of the current budget period: always the first of the calendar
	// month, UTC.
	// +optional
	PeriodStart *metav1.Time `json:"periodStart,omitempty"`
	// CountedFrom is where the used figures start. It equals periodStart, except for a budget of
	// source site: the share is then what the site reported left at spec.budget.observedAt, which
	// already counts everything that ended before it, so only later attempts are added.
	// +optional
	CountedFrom *metav1.Time `json:"countedFrom,omitempty"`
	// GPU and CPU ledgers, one per allocation (see TierLedger).
	// +optional
	GPU *TierLedger `json:"gpu,omitempty"`
	// +optional
	CPU *TierLedger `json:"cpu,omitempty"`
	// FPGA and LargeMem are the other two allocations (AC-84). Additive: gpu and cpu are unchanged.
	// +optional
	FPGA *TierLedger `json:"fpga,omitempty"`
	// +optional
	LargeMem *TierLedger `json:"largeMem,omitempty"`
	// ActiveJobs is the number of admitted jobs that have not ended (= queuedJobs + runningJobs).
	// +optional
	ActiveJobs int32 `json:"activeJobs,omitempty"`
	// QueuedJobs is the number of active jobs the site has not started yet: admitted, submitted
	// or pending in the site's queue.
	// +optional
	QueuedJobs int32 `json:"queuedJobs,omitempty"`
	// RunningJobs is the number of active jobs the site reports RUNNING, plus guests running on a
	// running host's node.
	// +optional
	RunningJobs int32 `json:"runningJobs,omitempty"`
	// RecentJobs are the project's jobs on this site: every active job, then the jobs that ended in the
	// last RecentJobsWindow (7 days), newest first within each group, at most RecentJobsMax (300) entries
	// (active jobs are kept first when the cap bites). Written with the ledger, for the console.
	// +kubebuilder:validation:MaxItems=300
	// +listType=atomic
	// +optional
	RecentJobs []RecentJob `json:"recentJobs,omitempty"`
	// ResumableSteps is what the controller recorded about the resumability of the project's steps,
	// one entry per app and step with evidence (D-74), at most ResumableStepsMax. Written with the ledger.
	// +kubebuilder:validation:MaxItems=100
	// +listType=atomic
	// +optional
	ResumableSteps []ResumableStep `json:"resumableSteps,omitempty"`
	// Storage is the project's scratch quota on the site and what uses it. It is NOT written by
	// this controller: the project's snapshot job (it holds the staging key) patches this one
	// field every 10 minutes from the site stager's `usage` report. The controller's status patches
	// are merge patches of its own fields, so they leave it alone.
	// +optional
	Storage *SiteStorage `json:"storage,omitempty"`
	// Checkpoints is the scratch the project's step checkpoints hold (D-75): at most two copies per step.
	// +optional
	Checkpoints *CheckpointUsage `json:"checkpoints,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CheckpointUsage is the space step checkpoints hold on the site's scratch.
type CheckpointUsage struct {
	// Bytes held by the checkpoint copies of steps that have not succeeded (a succeeded step's are deleted).
	Bytes int64 `json:"bytes"`
	// Steps is how many steps hold copies.
	Steps int32 `json:"steps"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Site",type=string,JSONPath=`.spec.site`
// +kubebuilder:printcolumn:name="GPU left min",type=integer,JSONPath=`.status.gpu.leftNodeMinutes`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.activeJobs`

// HPCSite is one project's specification for one HPC (Slurm) site: the site part (endpoint, classes) and the
// tenant part (identity, budget, at-limit policy, limits). It is the only object admission reads for a project's
// budget, and its status holds the project's ledger. It is rendered by the platform that integrates the operator
// (in KAOS: by kubecore-operator, owned by the KubeProject; PRD-HPC-1461 D-47).
type HPCSite struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HPCSiteSpec   `json:"spec"`
	Status HPCSiteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HPCSiteList contains a list of HPCSite.
type HPCSiteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HPCSite `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HPCSite{}, &HPCSiteList{})
}

// NodeHours is the budget's figure for one allocation: the share per period, or (source site) what the
// site reported left, and whether there is one. GPU and CPU always have one (an omitted 0 means no spend,
// as before AC-84). FPGA and large memory have one when set; when omitted, source site means "no figure"
// (false: the caller must not bound the tier) and source share means 0, no spend.
func (b Budget) NodeHours(t Tier) (int64, bool) {
	switch t {
	case TierGPU:
		return b.GPUNodeHours, true
	case TierCPU:
		return b.CPUNodeHours, true
	case TierFPGA:
		return optionalNodeHours(b.FPGANodeHours, b.Source)
	case TierLargeMem:
		return optionalNodeHours(b.LargeMemNodeHours, b.Source)
	}
	return 0, false
}

func optionalNodeHours(v *int64, src BudgetSource) (int64, bool) {
	if v != nil {
		return *v, true
	}
	return 0, src != BudgetSourceSite
}

// NodeHours is what the site reported left on the account for one allocation, and whether it gave a figure.
// GPU and CPU always count as reported (an omitted 0 is a spent allocation, as before AC-84); FPGA and
// large memory are reported only when present, because a writer that predates them omits them.
func (l SiteLeft) NodeHours(t Tier) (int64, bool) {
	switch t {
	case TierGPU:
		return l.GPUNodeHours, true
	case TierCPU:
		return l.CPUNodeHours, true
	case TierFPGA:
		if l.FPGANodeHours != nil {
			return *l.FPGANodeHours, true
		}
	case TierLargeMem:
		if l.LargeMemNodeHours != nil {
			return *l.LargeMemNodeHours, true
		}
	}
	return 0, false
}

// LedgerOf is the status ledger of one allocation, nil when it has not been published.
func (s *HPCSiteStatus) LedgerOf(t Tier) *TierLedger {
	switch t {
	case TierGPU:
		return s.GPU
	case TierCPU:
		return s.CPU
	case TierFPGA:
		return s.FPGA
	case TierLargeMem:
		return s.LargeMem
	}
	return nil
}

// SetLedger stores the status ledger of one allocation.
func (s *HPCSiteStatus) SetLedger(t Tier, l *TierLedger) {
	switch t {
	case TierGPU:
		s.GPU = l
	case TierCPU:
		s.CPU = l
	case TierFPGA:
		s.FPGA = l
	case TierLargeMem:
		s.LargeMem = l
	}
}
