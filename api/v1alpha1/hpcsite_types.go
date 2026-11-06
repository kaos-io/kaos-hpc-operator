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
	// BudgetSourceShare: a fixed share per calendar month (D-10, D-11).
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
	// AllocationRef names the org's HPCAllocation the share is carved from (checked on the hub, D-48).
	// +optional
	AllocationRef string `json:"allocationRef,omitempty"`
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
	// ActiveJobs is the number of admitted jobs that have not ended.
	// +optional
	ActiveJobs int32 `json:"activeJobs,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
