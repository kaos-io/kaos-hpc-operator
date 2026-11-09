package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OperationKind is an operation on a site (D-17, D-31).
// +kubebuilder:validation:Enum=Stage;CacheCheck;Cleanup;QuotaRead
type OperationKind string

// HPCOperationSpec is one operation on a site. By design it is named by a hash of its content, so every
// trigger of the same work converges on one object (D-31).
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
type HPCOperationSpec struct {
	Kind OperationKind `json:"kind"`
	// SiteName is the HPCSite (same namespace) the operation runs for.
	SiteName string `json:"siteName"`
	// Params of the operation, for example the dataset repo and commit to stage.
	// +optional
	Params map[string]string `json:"params,omitempty"`
}

// HPCOperationStatus mirrors an HPCArgoWorkflowJob's: phase, reason and the site's report.
type HPCOperationStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Run *RunStatus `json:"run,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// HPCOperation is an operation on a site: staging, cache check, cleanup or quota reading (PRD-HPC-1461 D-17,
// D-31). The CRD is installed, but no controller in this repository creates or reconciles it yet: the runner
// stages and cleans up inside its own job, and the quota is read by the project's snapshot job.
type HPCOperation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HPCOperationSpec   `json:"spec"`
	Status HPCOperationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HPCOperationList contains a list of HPCOperation.
type HPCOperationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HPCOperation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HPCOperation{}, &HPCOperationList{})
}
