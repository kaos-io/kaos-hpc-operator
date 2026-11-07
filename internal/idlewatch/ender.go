package idlewatch

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func mustExists(key string) labels.Requirement {
	r, err := labels.NewRequirement(key, selection.Exists, nil)
	if err != nil {
		panic(err) // a constant key cannot be invalid
	}
	return *r
}

// Ender ends a pod's step as Idle and later frees its GPU.
//
// Ending is two steps on purpose. Deleting a pod makes Argo mark the step Error "pod deleted"
// (or Failed with only an exit code), which the template's retry expression can re-run like a lost
// node. So End first sets the pod phase Failed with a KaosIdle message (Argo reads a Failed pod
// with a status message as a Failed step carrying that message, which the expression does not
// retry). The kubelet writes its own Running status back within about 10 s, so the watchdog
// repeats End until Argo has recorded the step, and only then Reap deletes the pod (a finished
// step ignores a missing pod).
type Ender interface {
	// End marks the pod and sets its status Failed/Idle. It is safe to call again on a pod it already marked.
	End(ctx context.Context, pod *corev1.Pod, why string, now time.Time) error
	// Reap deletes a pod End marked, so its containers stop and the GPU is freed.
	Reap(ctx context.Context, pod *corev1.Pod) error
}

// KubeEnder is the Ender that writes to the API server.
type KubeEnder struct {
	Client   client.Client
	Recorder record.EventRecorder
}

// End implements Ender.
func (e *KubeEnder) End(ctx context.Context, pod *corev1.Pod, why string, now time.Time) error {
	// Metadata first: once the label is set a failed status patch is retried by the next scan.
	if pod.Labels[LabelIdleEnded] != valueTrue {
		orig := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Labels[LabelIdleEnded] = valueTrue
		pod.Annotations[AnnotationIdleEnded] = now.UTC().Format(time.RFC3339)
		if err := e.Client.Patch(ctx, pod, client.MergeFrom(orig)); err != nil {
			return fmt.Errorf("idlewatch: mark %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return e.fail(ctx, pod, why)
}

// fail sets the pod status Failed with the Idle reason and message.
func (e *KubeEnder) fail(ctx context.Context, pod *corev1.Pod, why string) error {
	orig := pod.DeepCopy()
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = ReasonIdle
	pod.Status.Message = StatusMessage(why)
	if err := e.Client.Status().Patch(ctx, pod, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("idlewatch: set %s/%s Failed: %w", pod.Namespace, pod.Name, err)
	}
	if e.Recorder != nil {
		e.Recorder.Event(pod, corev1.EventTypeWarning, ReasonIdle, pod.Status.Message)
	}
	return nil
}

// Reap implements Ender. A pod that is already gone is not an error.
func (e *KubeEnder) Reap(ctx context.Context, pod *corev1.Pod) error {
	uid := pod.UID
	err := e.Client.Delete(ctx, pod, client.Preconditions{UID: &uid})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("idlewatch: delete %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}
