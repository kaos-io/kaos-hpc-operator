package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// PRD-HPC-1461 AC-41: the controller runs in a cluster without Argo Workflows. A job that names a
// Workflow whose API is not installed must not fail every reconcile; the Workflow is simply no
// reason to cancel. A real read failure still is an error.
func TestWorkflowCancelCauseWithoutTheArgoAPI(t *testing.T) {
	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "argoproj.io", Kind: "Workflow"}, SearchedVersions: []string{"v1alpha1"}}
	cases := []struct {
		name    string
		getErr  error
		wantErr bool
	}{
		{"should_not_cancel_or_fail_when_the_argo_api_is_not_installed", noMatch, false},
		{"should_fail_the_reconcile_on_any_other_read_error", context.DeadlineExceeded, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl := interceptor.NewClient(fake.NewClientBuilder().Build(), interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return c.getErr
				}})
			r := &HPCArgoWorkflowJobReconciler{Client: cl, Scheme: runtime.NewScheme()}
			job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{
				Workflow: &hpcv1alpha1.WorkflowRef{Name: "wf", UID: "u"}}}
			cause, err := r.workflowCancelCause(context.Background(), job)
			if (err != nil) != c.wantErr || cause != "" {
				t.Fatalf("got cause %q err %v, want no cause and error=%v", cause, err, c.wantErr)
			}
		})
	}
}
