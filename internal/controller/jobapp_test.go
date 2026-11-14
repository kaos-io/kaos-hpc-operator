package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func workflowOf(labels map[string]string, template string) *unstructured.Unstructured {
	wf := &unstructured.Unstructured{Object: map[string]interface{}{}}
	wf.SetGroupVersionKind(workflowGVK)
	wf.SetName("wf")
	wf.SetNamespace("ml-proj")
	wf.SetUID("u")
	wf.SetLabels(labels)
	if template != "" {
		_ = unstructured.SetNestedField(wf.Object, template, "spec", "workflowTemplateRef", "name")
	}
	return wf
}

// AC-14 F1: the follower that creates a job is one service account for every app of the project, so the controller
// holds spec.app against the app of the Workflow that owns the job.
func TestAJobOfAnotherAppThanItsWorkflowsIsRefused(t *testing.T) {
	appLabel := map[string]string{"platform.kubecore.io/app": "yolo"}
	cases := []struct {
		name   string
		app    string
		wf     *unstructured.Unstructured
		noRef  bool
		refuse string // a part of the message; "" = admitted
	}{
		{"the app of the label", "yolo", workflowOf(appLabel, ""), false, ""},
		{"the app of the template name", "yolo", workflowOf(nil, "yolo-pipeline"), false, ""},
		{"another app than the label", "rival", workflowOf(appLabel, ""), false, "rival"},
		{"another app than the template", "rival", workflowOf(nil, "yolo-pipeline"), false, "rival"},
		{"label and template disagree", "yolo", workflowOf(appLabel, "rival-pipeline"), false, "rival"},
		{"a workflow that names no app", "yolo", workflowOf(nil, ""), false, "names no app"},
		{"no app at all", "", workflowOf(appLabel, ""), false, "names no app"},
		{"no workflow at all", "yolo", nil, true, "names no Workflow"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := fake.NewClientBuilder()
			if c.wf != nil {
				b = b.WithRuntimeObjects(c.wf)
			}
			r := &HPCArgoWorkflowJobReconciler{Client: b.Build(), Scheme: runtime.NewScheme()}
			job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: c.app}}
			job.Namespace = "ml-proj"
			if !c.noRef {
				job.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: "wf", UID: "u"}
			}
			err, transient := r.checkApp(context.Background(), job)
			if transient != nil {
				t.Fatalf("the Workflow was read, yet: %v", transient)
			}
			switch {
			case c.refuse == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case c.refuse != "" && (err == nil || !strings.Contains(err.Error(), c.refuse)):
				t.Fatalf("want a refusal naming %q, got %v", c.refuse, err)
			}
		})
	}
}

// A Workflow that cannot be read now says nothing about the app: no refusal, an error so the job is asked again.
func TestAWorkflowWithoutTheArgoAPIDoesNotRefuseTheApp(t *testing.T) {
	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "argoproj.io", Kind: "Workflow"}, SearchedVersions: []string{"v1alpha1"}}
	cl := interceptor.NewClient(fake.NewClientBuilder().Build(), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return noMatch
		}})
	r := &HPCArgoWorkflowJobReconciler{Client: cl, Scheme: runtime.NewScheme()}
	job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "yolo",
		Workflow: &hpcv1alpha1.WorkflowRef{Name: "wf", UID: "u"}}}
	if refusal, err := r.checkApp(context.Background(), job); refusal != nil || err != nil {
		t.Fatalf("refusal %v, error %v", refusal, err)
	}
}

func TestAWorkflowThatCannotBeReadNowIsNotARefusalOfTheApp(t *testing.T) {
	gr := schema.GroupResource{Group: "argoproj.io", Resource: "workflows"}
	cases := map[string]error{
		"timeout":   apierrors.NewTimeoutError("slow", 1),
		"server":    apierrors.NewInternalError(errors.New("boom")),
		"forbidden": apierrors.NewForbidden(gr, "wf", errors.New("no")),
		"not found": apierrors.NewNotFound(gr, "wf"), // workflowCancelCause cancels the job for it, before this check
	}
	for name, getErr := range cases {
		t.Run(name, func(t *testing.T) {
			cl := interceptor.NewClient(fake.NewClientBuilder().Build(), interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return getErr
				}})
			r := &HPCArgoWorkflowJobReconciler{Client: cl, Scheme: runtime.NewScheme()}
			job := &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "yolo",
				Workflow: &hpcv1alpha1.WorkflowRef{Name: "wf", UID: "u"}}}
			refusal, err := r.checkApp(context.Background(), job)
			if refusal != nil || err == nil {
				t.Fatalf("want no refusal and an error to ask again, got refusal %v, error %v", refusal, err)
			}
		})
	}
}

func TestAJobRefusedAfterItWasSubmittedDoesNotSayNothingWasSubmitted(t *testing.T) {
	if got := refusedMessage(&hpcv1alpha1.RunStatus{SiteJobID: "5001"}, ""); strings.Contains(got, "nothing was staged or submitted") ||
		!strings.Contains(got, "5001") {
		t.Fatalf("message %q", got)
	}
	if got := refusedMessage(&hpcv1alpha1.RunStatus{}, ""); !strings.Contains(got, "nothing was staged or submitted") {
		t.Fatalf("message %q", got)
	}
	if got := refusedMessage(&hpcv1alpha1.RunStatus{SiteJobID: "5001"}, "the app yolo has no identity"); got != "the app yolo has no identity" {
		t.Fatalf("message %q", got)
	}
}
