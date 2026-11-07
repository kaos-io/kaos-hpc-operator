package idlewatch

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// startEnv boots a real API server (no kubelet). It skips when the envtest binaries are not
// installed (KUBEBUILDER_ASSETS unset), like `make test` provides them.
func startEnv(t *testing.T) client.Client {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// runningArgoPod creates a pod the way Argo does (workflow label, main container with a GPU limit)
// and sets its status Running, which a kubelet would.
func runningArgoPod(t *testing.T, c client.Client, name string) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	pod := gpuPod(name, t0, nil)
	pod.Namespace = "default"
	pod.Spec.Containers[0].Image = "busybox"
	pod.Spec.Containers[1].Image = "busybox"
	status := pod.Status
	pod.Status = corev1.PodStatus{}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = status
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestEnderAgainstARealAPIServer(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	rec := record.NewFakeRecorder(10)
	ender := &KubeEnder{Client: c, Recorder: rec}
	lister := &APIPodLister{Reader: c}
	pod := runningArgoPod(t, c, "hung-step")
	other := runningArgoPod(t, c, "training-step")

	running, err := lister.Running(ctx)
	if err != nil || len(running) != 2 {
		t.Fatalf("Running = %d pods, %v", len(running), err)
	}

	// End: the pod is marked and Failed with the Idle reason and the KaosIdle message.
	if err := ender.End(ctx, pod, "GPU peak 0% and no log output", t0); err != nil {
		t.Fatalf("End: %v", err)
	}
	var got corev1.Pod
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != corev1.PodFailed || got.Status.Reason != ReasonIdle ||
		got.Status.Message != StatusMessage("GPU peak 0% and no log output") {
		t.Fatalf("status = %+v", got.Status)
	}
	if got.Labels[LabelIdleEnded] != valueTrue || got.Annotations[AnnotationIdleEnded] == "" {
		t.Fatalf("pod not marked: %v %v", got.Labels, got.Annotations)
	}
	if len(rec.Events) != 1 {
		t.Fatalf("want one Warning event, got %d", len(rec.Events))
	}

	// The other pod is untouched, and only the marked pod is listed as ended.
	ended, err := lister.Ended(ctx)
	if err != nil || len(ended) != 1 || ended[0].Name != "hung-step" {
		t.Fatalf("Ended = %v, %v", ended, err)
	}
	running, _ = lister.Running(ctx)
	if len(running) != 1 || running[0].Name != other.Name {
		t.Fatalf("Running after End = %v", running)
	}

	// Recorded fact: the API server (1.33) lets a Failed pod go back to Running through the status
	// subresource, which is what the kubelet does within ~10 s. This is why the watchdog repeats End
	// until Argo has recorded the step instead of trusting one patch.
	back := got.DeepCopy()
	back.Status.Phase = corev1.PodRunning
	if err := c.Status().Update(ctx, back); err != nil {
		t.Logf("API server now refuses Failed -> Running: %v (the repeat in reap is then only a safety net)", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Skipf("API server kept the pod Failed: nothing to repeat")
	}

	// End again (the reap path after the kubelet's write-back) sets Failed once more.
	if err := ender.End(ctx, &got, "again", t0.Add(time.Minute)); err != nil {
		t.Fatalf("second End: %v", err)
	}
	if got.Status.Phase != corev1.PodFailed {
		t.Fatalf("second End left phase %s", got.Status.Phase)
	}

	// Reap deletes the pod (zero grace is not needed: envtest has no kubelet, delete is immediate for
	// a pod that never got a node) and a second Reap on a gone pod is not an error.
	if err := ender.Reap(ctx, &got); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	err = c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
	var after corev1.Pod
	if err == nil {
		_ = c.Get(ctx, client.ObjectKeyFromObject(pod), &after)
		if after.DeletionTimestamp == nil {
			t.Fatal("pod still there without a deletion timestamp")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	if err := ender.Reap(ctx, &got); err != nil {
		t.Fatalf("Reap of a gone pod: %v", err)
	}
}
