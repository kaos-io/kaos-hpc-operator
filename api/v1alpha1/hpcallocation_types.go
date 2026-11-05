package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HPCAllocationSpec is an org's allocation on one site and account, per period (D-10, D-11).
// Project shares are carved from it; the sum check runs on the hub (D-48).
type HPCAllocationSpec struct {
	// Site profile name, for example meluxina.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Site string `json:"site"`
	// Account (subscription) on the site.
	Account string `json:"account"`
	// GPUNodeHours and CPUNodeHours per period (calendar month).
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUNodeHours int64 `json:"gpuNodeHours,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	CPUNodeHours int64 `json:"cpuNodeHours,omitempty"`
}

// HPCAllocationStatus is the hub's view of what the org's projects were given.
type HPCAllocationStatus struct {
	// SharedGPUNodeHours and SharedCPUNodeHours: the sum of the project shares.
	// +optional
	SharedGPUNodeHours int64 `json:"sharedGPUNodeHours,omitempty"`
	// +optional
	SharedCPUNodeHours int64 `json:"sharedCPUNodeHours,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Site",type=string,JSONPath=`.spec.site`
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.spec.account`
// +kubebuilder:printcolumn:name="GPU h",type=integer,JSONPath=`.spec.gpuNodeHours`

// HPCAllocation is an org's node-hours on one site and account (PRD-HPC-1461 F-08). It lives on
// the hub, in the org namespace.
type HPCAllocation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HPCAllocationSpec   `json:"spec"`
	Status HPCAllocationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HPCAllocationList contains a list of HPCAllocation.
type HPCAllocationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HPCAllocation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HPCAllocation{}, &HPCAllocationList{})
}
