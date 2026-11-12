//go:build contractcheck

package hpc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/controller"
)

const jobCRD = "hpcargoworkflowjobs.hpc.kaos.io"

var jobGVK = hpcv1alpha1.GroupVersion.WithKind("HPCArgoWorkflowJob")

// oldJob is a job as the follower created it and the controller served it before the step contract: a
// dataset pin, declared outputs, the cancel finalizer, and (when it ran) outputs in its status.
func oldJob(ctx context.Context, t *testing.T, c client.Client, name string, running bool) {
	t.Helper()
	job := newJob(name, false)
	job.SetFinalizers([]string{hpcv1alpha1.CancelFinalizer})
	_ = unstructured.SetNestedMap(job.Object, map[string]any{"repo": "dataset-raw", "version": "v1",
		"commit": strings.Repeat("c2", 32)}, "spec", "dataset")
	_ = unstructured.SetNestedStringSlice(job.Object, []string{"training-result"}, "spec", "outputs")
	_ = unstructured.SetNestedStringSlice(job.Object, []string{"python", "-m", "app.entry"}, "spec", "command")
	_ = unstructured.SetNestedField(job.Object, "train", "spec", "stepName")
	if err := c.Create(ctx, job, client.FieldValidation("Strict")); err != nil {
		t.Fatal(err)
	}
	if !running {
		return
	}
	_ = unstructured.SetNestedField(job.Object, "Running", "status", "phase")
	_ = unstructured.SetNestedStringMap(job.Object, map[string]string{"training-result": `{"accuracy":0.9}`},
		"status", "outputs")
	if err := c.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
}

// loadDefinition reads one definition file.
func loadDefinition(t *testing.T, path string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, crd); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return crd
}

// swapDefinition replaces the stored definition in place by the repository's own (config/crd/bases), as a
// chart upgrade does: the old fields go, the contract's fields come, spec.context is required. It returns
// when the API server serves the new schema.
func swapDefinition(ctx context.Context, t *testing.T, c client.Client, root string) {
	t.Helper()
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := c.Get(ctx, types.NamespacedName{Name: jobCRD}, crd); err != nil {
		t.Fatal(err)
	}
	crd.Spec = loadDefinition(t, filepath.Join(root, "config", "crd", "bases", "hpc.kaos.io_hpcargoworkflowjobs.yaml")).Spec
	if err := c.Update(ctx, crd); err != nil {
		t.Fatalf("the API server refused the new definition: %v", err)
	}
	// Served when a job with spec.context is accepted under strict validation and one without it is refused.
	deadline := time.Now().Add(60 * time.Second)
	for i := 0; ; i++ {
		probe := newJob(fmt.Sprintf("probe-%d", i), true)
		err := c.Create(ctx, probe, client.FieldValidation("Strict"))
		if err == nil {
			_ = c.Delete(ctx, probe)
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

func newJob(name string, contract bool) *unstructured.Unstructured {
	spec := map[string]any{"class": "meluxina-gpu", "image": "registry.example/app:v1", "wallTimeMinutes": int64(120)}
	if contract {
		spec["context"], spec["app"] = `{"version":1}`, "hera-demo"
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
func storedJobAfterSwap(ctx context.Context, t *testing.T, c client.Client, name, label string) {
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
	patch := []byte(`{"status":{"phase":"Failed","reason":"Replaced","message":"this job was created ` +
		`before the platform update"}}`)
	if err := c.Status().Patch(ctx, got, client.RawPatch(types.MergePatchType, patch)); err != nil {
		t.Errorf("FAIL  %s: the status of the stored job cannot be written: %s", label, firstLine(err.Error()))
	} else {
		t.Logf("PASS  %s: the status of the stored job can be written", label)
	}
	// Finalizer removal, as the controller lets a job go.
	if err := c.Patch(ctx, got, client.RawPatch(types.MergePatchType,
		[]byte(`{"metadata":{"finalizers":null}}`))); err != nil {
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
// spec.outputs. It creates under strict field validation; the same job without it is tried too. Under the
// new definition neither may be stored: a stored one would be a job without its data (PRD kaos#1911 T-04).
func oldFollower(ctx context.Context, t *testing.T, c client.Client, strict bool) {
	t.Helper()
	u := newJob(fmt.Sprintf("old-follower-%v", strict), false)
	_ = unstructured.SetNestedMap(u.Object, map[string]any{"repo": "dataset-raw", "version": "v1",
		"commit": strings.Repeat("c2", 32)}, "spec", "dataset")
	_ = unstructured.SetNestedStringSlice(u.Object, []string{"training-result"}, "spec", "outputs")
	var opts []client.CreateOption
	how := "without strict field validation"
	if strict {
		opts, how = append(opts, client.FieldValidation("Strict")), "with strict field validation, as the follower does"
	}
	err := c.Create(ctx, u, opts...)
	if err == nil {
		spec, _, _ := unstructured.NestedMap(u.Object, "spec")
		t.Errorf("FAIL  a job in the old shape, created %s, is ACCEPTED and stored without its dataset. "+
			"spec holds: %s", how, keys(spec))
		_ = c.Delete(ctx, u)
		return
	}
	t.Logf("PASS  a job in the old shape, created %s, is refused: %s", how, firstLine(err.Error()))
}

// controllerRefuses runs the controller itself on a job stored before the change that it had not judged yet:
// read back without its dataset, it is refused with its own reason and never booked, and it can then go.
func controllerRefuses(ctx context.Context, t *testing.T, c client.Client, name string) {
	t.Helper()
	r := &controller.HPCArgoWorkflowJobReconciler{Client: c, Scheme: c.Scheme()}
	key := types.NamespacedName{Namespace: "ml-vision-env", Name: name}
	got := &hpcv1alpha1.HPCArgoWorkflowJob{}
	for i := 0; i < 2; i++ { // the second pass lets a refused job go (it removes the cancel finalizer)
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key}); err != nil {
			t.Fatalf("the controller could not handle the stored job: %v", err)
		}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if got.Status.Phase != hpcv1alpha1.PhaseRefused || got.Status.Reason != hpcv1alpha1.ReasonNoContext ||
				got.Status.Booking != nil {
				t.Fatalf("FAIL  the controller left the stored job %s/%s (booking %v): it must be Refused/NoContext",
					got.Status.Phase, got.Status.Reason, got.Status.Booking)
			}
			t.Logf("PASS  the controller refuses the stored job, booking nothing: phase %s, reason %s: %s",
				got.Status.Phase, got.Status.Reason, firstLine(got.Status.Message))
		}
	}
	if len(got.Finalizers) != 0 {
		t.Errorf("FAIL  the refused job still holds %v", got.Finalizers)
		return
	}
	if err := c.Delete(ctx, got); err != nil {
		t.Errorf("FAIL  the refused job cannot be deleted: %v", err)
		return
	}
	t.Logf("PASS  the controller let the refused job go (no finalizer) and it can be deleted")
}

func TestStoredJobsStayReadableWhenFieldsAreRemoved(t *testing.T) {
	root := repoRoot(t)
	// Before: the definition as it was (testdata), next to the repository's other definitions.
	before := t.TempDir()
	bases, err := filepath.Glob(filepath.Join(root, "config", "crd", "bases", "*.yaml"))
	if err != nil || len(bases) == 0 {
		t.Fatalf("no definitions under config/crd/bases: %v", err)
	}
	for _, f := range bases {
		src := f
		if filepath.Base(f) == "hpc.kaos.io_hpcargoworkflowjobs.yaml" {
			src = filepath.Join(root, "hack", "contract-checks", "hpc", "testdata", "hpcargoworkflowjobs-before.yaml")
		}
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(before, filepath.Base(f)), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{before}, ErrorIfCRDPathMissing: true}
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
	oldJob(ctx, t, c, "hpc-stored-1", true)
	oldJob(ctx, t, c, "hpc-stored-2", false)
	t.Logf("two jobs stored under the definition of before the step contract, each with spec.dataset, " +
		"spec.outputs and the cancel finalizer; the first is Running with status.outputs, the second not judged yet")

	swapDefinition(ctx, t, c, root)
	t.Logf("the definition was replaced in place by the repository's own (config/crd/bases): dataset, datasetRef, " +
		"outputs, status.outputs removed; context (required), app (required), inputs, status.result added")
	storedJobAfterSwap(ctx, t, c, "hpc-stored-1", "after the change")
	controllerRefuses(ctx, t, c, "hpc-stored-2")
	oldFollower(ctx, t, c, true)
	oldFollower(ctx, t, c, false)
}
