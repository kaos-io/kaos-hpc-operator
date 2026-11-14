package controller

import (
	"fmt"
	"sync/atomic"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

var fixtureWorkflows atomic.Int64

// withWorkflow gives a serve-mode job (spec.stepName) the Workflow a follower always names: a real one in the job's
// namespace, made from the job's app's pipeline. A job that is not serve-mode, or already names one, is left alone.
func withWorkflow(j *hpcv1alpha1.HPCArgoWorkflowJob) {
	if j.Spec.StepName == "" || j.Spec.Workflow != nil {
		return
	}
	wf := &unstructured.Unstructured{}
	wf.SetGroupVersionKind(workflowGVK)
	wf.SetName(fmt.Sprintf("fixture-wf-%d", fixtureWorkflows.Add(1)))
	wf.SetNamespace(j.Namespace)
	wf.SetLabels(map[string]string{workflowAppLabel: j.Spec.App})
	wf.Object["spec"] = map[string]any{"entrypoint": "main"}
	Expect(k8sClient.Create(ctx, wf)).To(Succeed())
	j.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: wf.GetName(), UID: string(wf.GetUID())}
}
