package idlewatch

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

// Watchdog scans the pool's GPU steps on an interval and ends the idle ones. It is a manager
// Runnable (mgr.Add), so it runs on the elected leader only.
type Watchdog struct {
	Cfg     Config
	Pods    PodLister
	Util    UtilSource
	Logs    LogReader
	Ender   Ender
	Flows   WorkflowReader
	Now     func() time.Time
	Log     logr.Logger
	Timeout time.Duration // per scan; zero: the scan interval
}

// ScanResult counts what one scan did; the tests and the log line read it.
type ScanResult struct {
	Considered, Ended, Reaped, Errors int
}

// Start runs scans until ctx ends.
func (w *Watchdog) Start(ctx context.Context) error {
	t := time.NewTicker(w.Cfg.ScanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			res := w.Scan(ctx)
			if res.Ended+res.Reaped+res.Errors > 0 {
				w.Log.Info("idle watchdog scan", "considered", res.Considered, "ended", res.Ended,
					"reaped", res.Reaped, "errors", res.Errors)
			}
		}
	}
}

// Scan does one pass: reap what was ended earlier, then judge the running GPU steps.
func (w *Watchdog) Scan(ctx context.Context) ScanResult {
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = w.Cfg.ScanInterval
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var res ScanResult
	now := w.Now()
	w.reap(ctx, now, &res)
	w.judge(ctx, now, &res)
	return res
}

// reap finishes pods End marked. A pod the kubelet put back to Running gets its Failed status again;
// a Failed one is deleted once Argo has recorded the step, or after ConfirmTimeout.
func (w *Watchdog) reap(ctx context.Context, now time.Time, res *ScanResult) {
	pods, err := w.Pods.Ended(ctx)
	if err != nil {
		w.fail("list-ended", err, res)
		return
	}
	for i := range pods {
		pod := &pods[i]
		endedAt, perr := time.Parse(time.RFC3339, pod.Annotations[AnnotationIdleEnded])
		if perr != nil {
			endedAt = now // unreadable mark: count the timeout from now rather than never delete
		}
		if pod.Status.Phase == corev1.PodRunning {
			if err := w.Ender.End(ctx, pod, "no GPU use and no log output (status repeated)", now); err != nil {
				w.fail("repeat-end", err, res)
			}
			// Without the Workflow's word, stop repeating after the timeout and delete.
			if now.Sub(endedAt) < w.Cfg.ConfirmTimeout {
				continue
			}
		}
		if !w.recorded(ctx, pod) && now.Sub(endedAt) < w.Cfg.ConfirmTimeout {
			continue
		}
		if err := w.Ender.Reap(ctx, pod); err != nil {
			w.fail("reap", err, res)
			continue
		}
		res.Reaped++
	}
}

// recorded is true when Argo has the pod's step as finished. Unreadable counts as not yet.
func (w *Watchdog) recorded(ctx context.Context, pod *corev1.Pod) bool {
	node := pod.Annotations[AnnotationArgoNodeID]
	if node == "" {
		return false
	}
	finished, known, err := w.Flows.NodeFinished(ctx, pod.Namespace, pod.Labels[LabelArgoWorkflow], node)
	if err != nil {
		w.fail("workflow", err, &ScanResult{})
		return false
	}
	return known && finished
}

// candidate is a GPU step with the window that applies to it.
type candidate struct {
	pod    *corev1.Pod
	window time.Duration
}

// judge ends the running GPU steps that have been idle for their window.
func (w *Watchdog) judge(ctx context.Context, now time.Time, res *ScanResult) {
	pods, err := w.Pods.Running(ctx)
	if err != nil {
		w.fail("list-running", err, res)
		return
	}
	byWindow := map[time.Duration][]candidate{}
	for i := range pods {
		pod := &pods[i]
		if !IsGPUPod(pod, GPUResource) {
			continue
		}
		window, enabled, bad := WindowFor(pod, w.Cfg.Window)
		if bad != "" {
			w.Log.Info("ignoring the idle-timeout annotation", "pod", pod.Namespace+"/"+pod.Name, "reason", bad)
		}
		if !enabled {
			continue
		}
		res.Considered++
		byWindow[window] = append(byWindow[window], candidate{pod, window})
	}
	for window, cands := range byWindow {
		w.judgeWindow(ctx, now, window, cands, res)
	}
}

// judgeWindow reads the utilisation once for all pods of one window, then the log of the quiet ones.
func (w *Watchdog) judgeWindow(ctx context.Context, now time.Time, window time.Duration, cands []candidate,
	res *ScanResult) {
	// Skip the metric query when no pod has run for the window yet.
	var old []candidate
	for _, c := range cands {
		if s := MainStartedAt(c.pod); !s.IsZero() && now.Sub(s) >= window {
			old = append(old, c)
		}
	}
	if len(old) == 0 {
		return
	}
	util, err := w.Util.Window(ctx, window, now)
	if err != nil {
		w.fail("metrics", err, res)
		return
	}
	for _, c := range old {
		u, ok := util[PodKey{Namespace: c.pod.Namespace, Name: c.pod.Name}]
		if !ok || !GPUQuiet(w.Cfg, window, u) {
			continue
		}
		w.endIfSilent(ctx, now, c, u, res)
	}
}

// endIfSilent reads the log of a pod whose GPU was quiet and ends the step when the log is silent too.
func (w *Watchdog) endIfSilent(ctx context.Context, now time.Time, c candidate, u UtilStat, res *ScanResult) {
	sig := Signals{Util: u}
	last, err := w.Logs.LastLine(ctx, c.pod.Namespace, c.pod.Name, MainContainer)
	if err != nil {
		w.fail("log", err, res)
	} else {
		sig.LogRead, sig.LastLog = true, last
	}
	v, why := Decide(w.Cfg, c.window, MainStartedAt(c.pod), sig, now)
	if v != VerdictIdle {
		return
	}
	if err := w.Ender.End(ctx, c.pod, why, now); err != nil {
		w.fail("end", err, res)
		return
	}
	res.Ended++
	metrics.K8sIdleKills.WithLabelValues(c.pod.Namespace).Inc()
	w.Log.Info("ended an idle GPU step", "pod", c.pod.Namespace+"/"+c.pod.Name, "why", why)
}

func (w *Watchdog) fail(stage string, err error, res *ScanResult) {
	res.Errors++
	metrics.K8sIdleErrors.WithLabelValues(stage).Inc()
	w.Log.Error(fmt.Errorf("idle watchdog %s: %w", stage, err), "scan step failed")
}
