package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Tier is the kind of node-hours a class spends. Sites meter GPU and CPU nodes separately.
// +kubebuilder:validation:Enum=gpu;cpu
type Tier string

const (
	TierGPU Tier = "gpu"
	TierCPU Tier = "cpu"
)

// Transport is how the controller reaches the site's scheduler (PRD-HPC-1461 D-35).
// Only slurmrestd is built in v1alpha1; the others are reserved names.
// +kubebuilder:validation:Enum=slurmrestd;ssh-slurm-cli;firecrest
type Transport string

const (
	TransportSlurmREST Transport = "slurmrestd"
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

// SiteProfile is the site part of an HPCSite, rendered from the KAOS-curated profile (F-03, D-47).
// Tenants never set it.
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
	// Source: share (default) or site. With site, GPUNodeHours and CPUNodeHours are what the
	// site reported left at ObservedAt; the site's figure already counts every job that ran, so
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
	// CPUNodeHours per period.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CPUNodeHours int64 `json:"cpuNodeHours,omitempty"`
	// AtLimit: block (default) or warn (D-13).
	// +kubebuilder:default=block
	// +optional
	AtLimit AtLimitPolicy `json:"atLimit,omitempty"`
	// WarnAtPercent of the share raises an alert (D-13).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	// +optional
	WarnAtPercent int32 `json:"warnAtPercent,omitempty"`
}

// SiteLeft is the site's remaining node-hours per tier, as the site reported at ObservedAt.
type SiteLeft struct {
	// GPUNodeHours left on the account.
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUNodeHours int64 `json:"gpuNodeHours,omitempty"`
	// CPUNodeHours left on the account.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CPUNodeHours int64 `json:"cpuNodeHours,omitempty"`
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

// HPCSiteSpec is one KAOS project's whole specification for one site (D-47).
// +kubebuilder:validation:XValidation:rule="self.site == oldSelf.site",message="site is immutable"
type HPCSiteSpec struct {
	// Site is the site profile name, for example meluxina.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Site string `json:"site"`
	// Profile is the site part, rendered from the KAOS-curated profile.
	Profile SiteProfile `json:"profile"`
	// Identity is the project identity on the site.
	Identity ProjectIdentity `json:"identity"`
	// Budget is the project's share of the org allocation.
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
}

// RecentJobsMax bounds HPCSiteStatus.RecentJobs. Each entry is a few hundred bytes, so the list
// stays far below the object size limit (about 25 KiB at the bound).
const RecentJobsMax = 50

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
	// ObservedAt is when the quota was read; MeasuredAt is when the category sizes were last
	// measured (re-measured at most hourly, the quota on every report).
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
	// +optional
	MeasuredAt *metav1.Time `json:"measuredAt,omitempty"`
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
	// PeriodStart is the start of the current budget period (calendar month, UTC).
	// +optional
	PeriodStart *metav1.Time `json:"periodStart,omitempty"`
	// GPU and CPU ledgers.
	// +optional
	GPU *TierLedger `json:"gpu,omitempty"`
	// +optional
	CPU *TierLedger `json:"cpu,omitempty"`
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
	// RecentJobs are at most RecentJobsMax of the project's jobs on this site: active jobs first,
	// then ended ones, newest first within each group. Written with the ledger, for the console.
	// +kubebuilder:validation:MaxItems=50
	// +listType=atomic
	// +optional
	RecentJobs []RecentJob `json:"recentJobs,omitempty"`
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

// HPCSite is one KAOS project's specification for one HPC site: the site part from the KAOS
// profile and the tenant part (identity, budget share, policy, limits, classes). Owned by the
// KubeProject and rendered by kubecore-operator (PRD-HPC-1461 D-47).
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
