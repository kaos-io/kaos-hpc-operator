package idlewatch

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func workflow(phase string) *unstructured.Unstructured {
	wf := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "wf-1", "namespace": "proj"},
		"status":   map[string]any{"nodes": map[string]any{"n1": map[string]any{"phase": phase}}},
	}}
	wf.SetGroupVersionKind(schema.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Workflow"})
	return wf
}

func TestArgoWorkflowReader(t *testing.T) {
	cases := []struct {
		name            string
		wf              *unstructured.Unstructured
		node            string
		finished, known bool
	}{
		{"should_report_a_failed_node_as_finished", workflow("Failed"), "n1", true, true},
		{"should_report_an_errored_node_as_finished", workflow("Error"), "n1", true, true},
		{"should_report_a_running_node_as_not_finished", workflow("Running"), "n1", false, true},
		{"should_report_an_unknown_node_as_unknown", workflow("Failed"), "other", false, false},
		{"should_report_a_deleted_workflow_as_finished", nil, "n1", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder()
			if tc.wf != nil {
				b = b.WithObjects(tc.wf)
			}
			r := &ArgoWorkflowReader{Reader: b.Build()}
			fin, known, err := r.NodeFinished(context.Background(), "proj", "wf-1", tc.node)
			if err != nil || fin != tc.finished || known != tc.known {
				t.Fatalf("got %v %v %v", fin, known, err)
			}
		})
	}
}
