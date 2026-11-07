package idlewatch

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Verdict is the outcome of judging one pod.
type Verdict string

// The verdicts. Only Idle ends a step.
const (
	// VerdictIdle: GPU quiet and log silent for the whole window.
	VerdictIdle Verdict = "Idle"
	// VerdictBusy: the GPU was used or the log grew within the window.
	VerdictBusy Verdict = "Busy"
	// VerdictUnknown: a signal is missing or the pod is too young; never ends a step.
	VerdictUnknown Verdict = "Unknown"
)

// Signals are the readings the verdict is made from. A nil pointer or false flag is "not known".
type Signals struct {
	// Util is the GPU utilisation over the window; Known false when the metric had too few samples.
	Util UtilStat
	// LogRead is true when the log was read; LastLog is the newest line's time (zero: no output).
	LogRead bool
	LastLog time.Time
}

// UtilStat is the highest utilisation (percent) any of the pod's GPUs showed in the window and how
// many samples back it. Samples is the smallest count over the pod's GPU series.
type UtilStat struct {
	Max     float64
	Samples int
}

// PodKey names a pod in a metric result.
type PodKey = types.NamespacedName

// IsGPUPod reports whether the pod is a running Argo step that holds a GPU and is not already
// being ended. It is the only filter on which pods the watchdog may touch.
func IsGPUPod(pod *corev1.Pod, gpuResource string) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if _, ok := pod.Labels[LabelArgoWorkflow]; !ok {
		return false
	}
	if pod.Labels[LabelIdleEnded] == valueTrue {
		return false
	}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name != MainContainer {
			continue
		}
		q, ok := c.Resources.Limits[corev1.ResourceName(gpuResource)]
		return ok && !q.IsZero()
	}
	return false
}

// WindowFor is the pod's idle window: the annotation (whole minutes) over the default. enabled is
// false for "0". A value that is not a non-negative integer is ignored, so a typo cannot disable
// the watchdog or kill a step early; bad says so.
func WindowFor(pod *corev1.Pod, def time.Duration) (window time.Duration, enabled bool, bad string) {
	raw, ok := pod.Annotations[AnnotationIdleTimeout]
	if !ok {
		return def, true, ""
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		return def, true, fmt.Sprintf("%s=%q is not whole minutes", AnnotationIdleTimeout, raw)
	}
	if n == 0 {
		return 0, false, ""
	}
	return time.Duration(n) * time.Minute, true, ""
}

// MainStartedAt is when the main container started running, or zero when it is not running.
func MainStartedAt(pod *corev1.Pod) time.Time {
	for i := range pod.Status.ContainerStatuses {
		s := &pod.Status.ContainerStatuses[i]
		if s.Name == MainContainer && s.State.Running != nil {
			return s.State.Running.StartedAt.Time
		}
	}
	return time.Time{}
}

// MinSamples is how many samples a window must hold before its utilisation counts as known.
func MinSamples(window, scrape time.Duration) int {
	n := int(float64(window/scrape) * MinSampleCoverage)
	return max(n, 1)
}

// GPUQuiet reports whether the utilisation reading is known and below the busy threshold. It is the
// cheap half of the verdict: the log is read only for pods that pass it.
func GPUQuiet(cfg Config, window time.Duration, u UtilStat) bool {
	return u.Samples >= MinSamples(window, cfg.ScrapeInterval) && u.Max < cfg.BusyPercent
}

// Decide judges a pod from its signals. The step is Idle only when it ran for the whole window,
// the utilisation is known and below busy, and the log was read and is silent for the window.
// why is a short human reason for logs and the event.
func Decide(cfg Config, window time.Duration, startedAt time.Time, s Signals, now time.Time) (v Verdict, why string) {
	if startedAt.IsZero() || now.Sub(startedAt) < window {
		return VerdictUnknown, "the main container has not run for the whole window"
	}
	if s.Util.Samples < MinSamples(window, cfg.ScrapeInterval) {
		return VerdictUnknown, fmt.Sprintf("only %d GPU samples in the window", s.Util.Samples)
	}
	if s.Util.Max >= cfg.BusyPercent {
		return VerdictBusy, fmt.Sprintf("GPU reached %.0f%% in the window", s.Util.Max)
	}
	if !s.LogRead {
		return VerdictUnknown, "the log could not be read"
	}
	// No output at all counts as silent since the container started (checked above to be a full window).
	if !s.LastLog.IsZero() && now.Sub(s.LastLog) < window {
		return VerdictBusy, "the log grew in the window"
	}
	return VerdictIdle, fmt.Sprintf("GPU peak %.0f%% (below %.0f%%) and no log output for %s",
		s.Util.Max, cfg.BusyPercent, window)
}

// StatusMessage is the pod status message, which Argo copies to the step: it starts with
// MessagePrefix (the retry expression keys on it) and names the rule and the override.
func StatusMessage(why string) string {
	return MessagePrefix + ": ended by the idle watchdog - " + why +
		". Raise or turn off the window with step(idle_timeout=...)."
}
