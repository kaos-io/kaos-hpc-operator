//go:build contractcheck

package hpc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

const jobCRD = "hpcargoworkflowjobs.hpc.kaos.io"

var jobGVK = hpcv1alpha1.GroupVersion.WithKind("HPCArgoWorkflowJob")

// oldJob is a job as today's follower creates it and today's controller serves it: a dataset pin,
// declared outputs, the cancel finalizer, and outputs in its status.
func oldJob(t *testing.T, ctx context.Context, c client.Client, name string) {
	t.Helper()
	job := &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ml-vision-env", Finalizers: []string{hpcv1alpha1.CancelFinalizer}},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "registry.example/app:v1",
			Command: []string{"python", "-m", "app.entry"}, WallTimeMinutes: 120, StepName: "train",
			Dataset:  &hpcv1alpha1.DatasetPin{Repo: "dataset-raw", Version: "v1", Commit: strings.Repeat("c2", 32)},
			Outputs:  []string{"training-result"},
			Workflow: &hpcv1alpha1.WorkflowRef{Name: "wf-1", UID: "wf-uid"}},
	}
	if err := c.Create(ctx, job, client.FieldValidation("Strict")); err != nil {
		t.Fatal(err)
	}
	job.Status.Phase = hpcv1alpha1.Phase("Running")
	job.Status.Outputs = map[string]string{"training-result": `{"accuracy":0.9}`}
	if err := c.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
}

// swapDefinition changes the stored definition in place, as a chart upgrade does: the old fields go, the
// contract's fields come. It returns when the API server serves the new schema.
func swapDefinition(t *testing.T, ctx context.Context, c client.Client, contextRequired bool) {
	t.Helper()
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := c.Get(ctx, types.NamespacedName{Name: jobCRD}, crd); err != nil {
		t.Fatal(err)
	}
	schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	spec := schema.Properties["spec"]
	for _, gone := range []string{"dataset", "datasetRef", "outputs"} {
		delete(spec.Properties, gone)
	}
	spec.Properties["context"] = apiextensionsv1.JSONSchemaProps{Type: "string", MaxLength: ptr.To(int64(contextCap))}
	spec.Properties["app"] = apiextensionsv1.JSONSchemaProps{Type: "string", MaxLength: ptr.To(int64(55))}
	spec.Properties["inputs"] = apiextensionsv1.JSONSchemaProps{Type: "array", MaxItems: ptr.To(int64(16)),
		Items: &apiextensionsv1.JSONSchemaPropsOrArray{Schema: &apiextensionsv1.JSONSchemaProps{Type: "object",
			Required: []string{"name", "repo", "commit"},
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"name": {Type: "string", MaxLength: ptr.To(int64(63))}, "repo": {Type: "string", MaxLength: ptr.To(int64(63))},
				"commit": {Type: "string", MaxLength: ptr.To(int64(64))}, "ref": {Type: "string", MaxLength: ptr.To(int64(255))},
				"owner": {Type: "string", MaxLength: ptr.To(int64(63))}, "line": {Type: "string", MaxLength: ptr.To(int64(255))}}}}}
	required := []string{"class", "image", "wallTimeMinutes"}
	if contextRequired {
		required = append(required, "context")
	}
	spec.Required = required
	schema.Properties["spec"] = spec
	status := schema.Properties["status"]
	delete(status.Properties, "outputs")
	status.Properties["result"] = apiextensionsv1.JSONSchemaProps{Type: "string", MaxLength: ptr.To(int64(resultCap))}
	schema.Properties["status"] = status
	if err := c.Update(ctx, crd); err != nil {
		t.Fatalf("the API server refused the new definition: %v", err)
	}
	// Served when a job with spec.context is accepted under strict validation (and, with context required,
	// one without it is refused).
	deadline := time.Now().Add(60 * time.Second)
	for i := 0; ; i++ {
		probe := newJob(fmt.Sprintf("probe-%v-%d", contextRequired, i), true)
		err := c.Create(ctx, probe, client.FieldValidation("Strict"))
		if err == nil {
			_ = c.Delete(ctx, probe)
			if !contextRequired {
				return
			}
			bare := newJob(fmt.Sprintf("bare-%d", i), false)
			if err := c.Create(ctx, bare, client.FieldValidation("Strict")); err != nil {
				return
			}
			_ = c.Delete(ctx, bare)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the new definition was not served within a minute: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func newJob(name string, withContext bool) *unstructured.Unstructured {
	spec := map[string]any{"class": "meluxina-gpu", "image": "registry.example/app:v1", "wallTimeMinutes": int64(120)}
	if withContext {
		spec["context"] = `{"version":1}`
	}
	u := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	u.SetGroupVersionKind(jobGVK)
	u.SetNamespace("ml-vision-env")
	u.SetName(name)
	return u
}

func keys(m map[string]any) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// storedJobAfterSwap reads an old job under the new definition and does to it what the controller has
// to be able to do: write its status, drop its finalizer, delete it.
func storedJobAfterSwap(t *testing.T, ctx context.Context, c client.Client, name, label string) {
	t.Helper()
	key := types.NamespacedName{Namespace: "ml-vision-env", Name: name}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(jobGVK)
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("%s: the stored job cannot be read: %v", label, err)
	}
	spec, _, _ := unstructured.NestedMap(got.Object, "spec")
	status, _, _ := unstructured.NestedMap(got.Object, "status")
	t.Logf("PASS  %s: the stored job is readable. spec holds: %s. status holds: %s", label, keys(spec), keys(status))
	for _, gone := range []string{"dataset", "datasetRef", "outputs"} {
		if _, there := spec[gone]; there {
			t.Errorf("%s: spec.%s is still returned", label, gone)
		}
	}
	if _, there := status["outputs"]; there {
		t.Errorf("%s: status.outputs is still returned", label)
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(jobGVK)
	if err := c.List(ctx, list, client.InNamespace("ml-vision-env")); err != nil {
		t.Fatalf("%s: jobs cannot be listed: %v", label, err)
	}
	t.Logf("PASS  %s: jobs can be listed (%d)", label, len(list.Items))

	// The typed client of today's controller reads it too (the removed fields are simply empty).
	typed := &hpcv1alpha1.HPCArgoWorkflowJob{}
	if err := c.Get(ctx, key, typed); err != nil {
		t.Fatalf("%s: the typed read failed: %v", label, err)
	}

	// Status write, as the controller ends a job.
	patch := []byte(`{"status":{"phase":"Failed","reason":"Replaced","message":"this job was created before the platform update"}}`)
	if err := c.Status().Patch(ctx, got, client.RawPatch(types.MergePatchType, patch)); err != nil {
		t.Errorf("FAIL  %s: the status of the stored job cannot be written: %s", label, firstLine(err.Error()))
	} else {
		t.Logf("PASS  %s: the status of the stored job can be written", label)
	}
	// Finalizer removal, as the controller lets a job go.
	if err := c.Patch(ctx, got, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`))); err != nil {
		t.Errorf("FAIL  %s: the finalizer of the stored job cannot be removed: %s", label, firstLine(err.Error()))
	} else {
		t.Logf("PASS  %s: the finalizer of the stored job can be removed", label)
	}
	if err := c.Delete(ctx, got); err != nil {
		t.Errorf("FAIL  %s: the stored job cannot be deleted: %s", label, firstLine(err.Error()))
		return
	}
	if err := c.Get(ctx, key, got); !apierrors.IsNotFound(err) {
		t.Errorf("FAIL  %s: the stored job is still there after delete: %v", label, err)
	} else {
		t.Logf("PASS  %s: the stored job can be deleted and is gone", label)
	}
}

// oldFollower creates a job the way a follower released before the contract does: with spec.dataset and
// spec.outputs, under strict field validation (internal/follower/run.go).
func oldFollower(t *testing.T, ctx context.Context, c client.Client, label string, strict bool) {
	t.Helper()
	u := newJob("old-follower-"+strings.ReplaceAll(strings.ToLower(label), " ", "-")+fmt.Sprintf("-%v", strict), false)
	_ = unstructured.SetNestedMap(u.Object, map[string]any{"repo": "dataset-raw", "version": "v1", "commit": strings.Repeat("c2", 32)}, "spec", "dataset")
	_ = unstructured.SetNestedStringSlice(u.Object, []string{"training-result"}, "spec", "outputs")
	var opts []client.CreateOption
	how := "without strict field validation"
	if strict {
		opts, how = append(opts, client.FieldValidation("Strict")), "with strict field validation, as the follower does"
	}
	err := c.Create(ctx, u, opts...)
	if err != nil {
		t.Logf("NOTE  %s: a job in the old shape, created %s, is refused: %s", label, how, firstLine(err.Error()))
		return
	}
	spec, _, _ := unstructured.NestedMap(u.Object, "spec")
	t.Logf("NOTE  %s: a job in the old shape, created %s, is ACCEPTED and stored without its dataset. spec holds: %s", label, how, keys(spec))
	_ = c.Delete(ctx, u)
}

func TestStoredJobsStayReadableWhenFieldsAreRemoved(t *testing.T) {
	root := repoRoot(t)
	env := &envtest.Environment{CRDDirectoryPaths: []string{root + "/config/crd/bases"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = env.Stop() }()
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = hpcv1alpha1.AddToScheme(s)
	_ = apiextensionsv1.AddToScheme(s)
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ml-vision-env"}}); err != nil {
		t.Fatal(err)
	}
	oldJob(t, ctx, c, "hpc-stored-1")
	oldJob(t, ctx, c, "hpc-stored-2")
	t.Logf("two jobs stored under today's definition, each with spec.dataset, spec.outputs, status.outputs and the cancel finalizer")

	swapDefinition(t, ctx, c, false)
	t.Logf("the definition was changed in place: dataset, datasetRef, outputs, status.outputs removed; context, app, inputs, status.result added; context optional")
	storedJobAfterSwap(t, ctx, c, "hpc-stored-1", "context optional")
	oldFollower(t, ctx, c, "context optional", true)
	oldFollower(t, ctx, c, "context optional", false)

	swapDefinition(t, ctx, c, true)
	t.Logf("the definition was changed again: spec.context is now required")
	storedJobAfterSwap(t, ctx, c, "hpc-stored-2", "context required")
	oldFollower(t, ctx, c, "context required", true)
	oldFollower(t, ctx, c, "context required", false)
}
