package idlewatch

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkflowReader tells whether Argo has recorded a step as finished.
type WorkflowReader interface {
	// NodeFinished reports whether the Workflow node is in a final phase. known is false when the
	// answer cannot be read (offloaded node status, no such node); a Workflow that no longer exists
	// counts as finished.
	NodeFinished(ctx context.Context, namespace, workflow, nodeID string) (finished, known bool, err error)
}

// ArgoWorkflowReader reads Workflows as unstructured objects, so the operator takes no Argo dependency.
type ArgoWorkflowReader struct {
	Reader client.Reader
}

// NodeFinished implements WorkflowReader.
func (a *ArgoWorkflowReader) NodeFinished(ctx context.Context, namespace, workflow, nodeID string) (bool, bool, error) {
	wf := &unstructured.Unstructured{}
	wf.SetGroupVersionKind(schema.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Workflow"})
	if err := a.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workflow}, wf); err != nil {
		if apierrors.IsNotFound(err) {
			return true, true, nil
		}
		return false, false, fmt.Errorf("idlewatch: get workflow %s/%s: %w", namespace, workflow, err)
	}
	phase, found, _ := unstructured.NestedString(wf.Object, "status", "nodes", nodeID, "phase")
	if !found {
		return false, false, nil
	}
	switch phase {
	case "Succeeded", "Failed", "Error", "Skipped", "Omitted":
		return true, true, nil
	}
	return false, true, nil
}
