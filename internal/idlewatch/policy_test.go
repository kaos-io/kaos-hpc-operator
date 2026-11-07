package idlewatch

import (
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var t0 = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

func gpuPod(name string, started time.Time, annots map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "proj", Annotations: annots,
			Labels: map[string]string{LabelArgoWorkflow: "wf-1"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "wait"},
			{Name: MainContainer, Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				GPUResource: resource.MustParse("1")}}},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
			{Name: MainContainer, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
				StartedAt: metav1.NewTime(started)}}}}},
	}
}

func TestIsGPUPod(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(p *corev1.Pod)
		want   bool
	}{
		{"should_accept_a_running_argo_pod_with_a_gpu_limit", func(*corev1.Pod) {}, true},
		{"should_reject_a_pod_without_the_argo_label", func(p *corev1.Pod) { delete(p.Labels, LabelArgoWorkflow) }, false},
		{"should_reject_a_pod_without_a_gpu", func(p *corev1.Pod) { p.Spec.Containers[1].Resources = corev1.ResourceRequirements{} }, false},
		{"should_reject_a_pod_that_is_not_running", func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending }, false},
		{"should_reject_a_pod_already_ended", func(p *corev1.Pod) { p.Labels[LabelIdleEnded] = "true" }, false},
		{"should_reject_a_pod_being_deleted", func(p *corev1.Pod) { n := metav1.Now(); p.DeletionTimestamp = &n }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := gpuPod("p", t0, nil)
			tc.mutate(p)
			if got := IsGPUPod(p, GPUResource); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestWindowFor(t *testing.T) {
	cases := []struct {
		name    string
		annots  map[string]string
		window  time.Duration
		enabled bool
		bad     bool
	}{
		{"should_use_the_default_without_an_annotation", nil, 20 * time.Minute, true, false},
		{"should_raise_the_window_from_the_annotation", map[string]string{AnnotationIdleTimeout: "45"}, 45 * time.Minute, true, false},
		{"should_turn_the_watchdog_off_for_zero", map[string]string{AnnotationIdleTimeout: "0"}, 0, false, false},
		{"should_ignore_a_value_that_is_not_minutes", map[string]string{AnnotationIdleTimeout: "20m"}, 20 * time.Minute, true, true},
		{"should_ignore_a_negative_value", map[string]string{AnnotationIdleTimeout: "-5"}, 20 * time.Minute, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, en, bad := WindowFor(gpuPod("p", t0, tc.annots), DefaultWindow)
			if w != tc.window || en != tc.enabled || (bad != "") != tc.bad {
				t.Fatalf("got %v %v %q", w, en, bad)
			}
		})
	}
}

func TestDecide(t *testing.T) {
	cfg := Config{}.WithDefaults()
	w := cfg.Window
	old := t0.Add(-2 * time.Hour)
	quiet := UtilStat{Max: 0, Samples: 20}
	cases := []struct {
		name    string
		started time.Time
		sig     Signals
		want    Verdict
	}{
		{"should_end_when_gpu_quiet_and_log_silent_for_the_window", old,
			Signals{Util: quiet, LogRead: true, LastLog: t0.Add(-25 * time.Minute)}, VerdictIdle},
		{"should_end_when_gpu_quiet_and_the_step_never_logged", old,
			Signals{Util: quiet, LogRead: true}, VerdictIdle},
		{"should_not_end_when_the_log_grew_within_the_window", old,
			Signals{Util: quiet, LogRead: true, LastLog: t0.Add(-5 * time.Minute)}, VerdictBusy},
		{"should_not_end_when_the_gpu_was_busy", old,
			Signals{Util: UtilStat{Max: 37, Samples: 20}, LogRead: true}, VerdictBusy},
		{"should_not_end_at_exactly_the_busy_threshold", old,
			Signals{Util: UtilStat{Max: 5, Samples: 20}, LogRead: true}, VerdictBusy},
		{"should_not_end_when_the_log_could_not_be_read", old,
			Signals{Util: quiet}, VerdictUnknown},
		{"should_not_end_with_too_few_gpu_samples", old,
			Signals{Util: UtilStat{Max: 0, Samples: 3}, LogRead: true}, VerdictUnknown},
		{"should_not_end_a_step_younger_than_the_window", t0.Add(-10 * time.Minute),
			Signals{Util: quiet, LogRead: true}, VerdictUnknown},
		{"should_not_end_a_step_whose_start_is_unknown", time.Time{},
			Signals{Util: quiet, LogRead: true}, VerdictUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if v, why := Decide(cfg, w, tc.started, tc.sig, t0); v != tc.want {
				t.Fatalf("got %v (%s) want %v", v, why, tc.want)
			}
		})
	}
}

// The status message is read by Argo as the step's message and then by the Hera template's retry
// expression. It must carry the KaosIdle prefix and none of the pod-loss words, or a step ended as
// Idle would be re-run like a lost node. The words are copied from kubecore/meluxina.py
// POD_LOSS_MESSAGES (hera-pipeline-template); keep them in step.
func TestStatusMessageIsNotAPodLossMessage(t *testing.T) {
	podLoss := regexp.MustCompile(".*(imminent node shutdown|node is shutting down|node is draining" +
		"|The node was low on resource|The node had condition" +
		"|Pod was rejected|Evicted|evicted|pod deleted|Pod was deleted).*")
	for _, why := range []string{
		"GPU peak 0% (below 5%) and no log output for 20m0s",
		"no GPU use and no log output (status patch repeated)",
	} {
		msg := StatusMessage(why)
		if !strings.HasPrefix(msg, MessagePrefix) {
			t.Fatalf("message %q does not start with %s", msg, MessagePrefix)
		}
		if podLoss.MatchString(msg) {
			t.Fatalf("message %q matches the pod-loss retry expression", msg)
		}
	}
}

func TestParseLogTime(t *testing.T) {
	cases := []struct {
		name, line string
		want       time.Time
		err        bool
	}{
		{"should_read_the_timestamp_of_a_line", "2026-11-07T11:30:01.123456789Z epoch 3 loss 0.2\n",
			time.Date(2026, 11, 7, 11, 30, 1, 123456789, time.UTC), false},
		{"should_return_zero_for_no_output", "", time.Time{}, false},
		{"should_fail_on_a_line_without_a_timestamp", "epoch 3\n", time.Time{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLogTime(tc.line)
			if (err != nil) != tc.err || !got.Equal(tc.want) {
				t.Fatalf("got %v %v", got, err)
			}
		})
	}
}
