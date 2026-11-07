package idlewatch

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodLister lists the pods the watchdog works on.
type PodLister interface {
	// Running returns the pool's Argo pods in phase Running.
	Running(ctx context.Context) ([]corev1.Pod, error)
	// Ended returns the pods the watchdog already marked (LabelIdleEnded=true).
	Ended(ctx context.Context) ([]corev1.Pod, error)
}

// APIPodLister lists through an uncached reader: the manager's cache holds only runner pods, and a
// scan every minute is cheaper than a cluster-wide pod informer.
type APIPodLister struct {
	Reader client.Reader
}

// Running implements PodLister.
func (l *APIPodLister) Running(ctx context.Context) ([]corev1.Pod, error) {
	var list corev1.PodList
	err := l.Reader.List(ctx, &list,
		client.MatchingLabelsSelector{Selector: labels.NewSelector().Add(mustExists(LabelArgoWorkflow))},
		client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning))})
	if err != nil {
		return nil, fmt.Errorf("idlewatch: list running workflow pods: %w", err)
	}
	return list.Items, nil
}

// Ended implements PodLister.
func (l *APIPodLister) Ended(ctx context.Context) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := l.Reader.List(ctx, &list, client.MatchingLabels{LabelIdleEnded: valueTrue}); err != nil {
		return nil, fmt.Errorf("idlewatch: list ended pods: %w", err)
	}
	return list.Items, nil
}
