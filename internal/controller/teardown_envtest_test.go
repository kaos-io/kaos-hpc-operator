package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// The standalone e2e's teardown (helm uninstall, then `kubectl delete crd hpcargoworkflowjobs.hpc.kaos.io`) hung for 30 minutes
// when a finished job held a finalizer and the controller was gone. A finished job holds none: against a real API server and no
// controller at all, the CRD and a finished job are deleted and the deletion completes.
func TestShouldDeleteTheCRDWithAFinishedJobInItAndNoControllerRunning(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set: no API server to test against")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = env.Stop() }()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, hpcv1alpha1.AddToScheme, apiextv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ml-p"}}); err != nil {
		t.Fatal(err)
	}
	job := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "finished", Namespace: "ml-p"},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "gpu", Image: "reg.example/img:1", WallTimeMinutes: 5}}
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Phase = hpcv1alpha1.PhaseSucceeded
	if err := c.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	crd := &apiextv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "hpcargoworkflowjobs.hpc.kaos.io"}}
	if err := c.Delete(ctx, crd); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		err := c.Get(ctx, client.ObjectKeyFromObject(crd), crd)
		if apierrors.IsNotFound(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the CRD is still being deleted after 60 s (job finalizers held it?): %v, finalizers %v", err, crd.Finalizers)
		}
		time.Sleep(time.Second)
	}
}
