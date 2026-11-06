package controller

import (
	"math"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

// nodeMinutesSaved is what an idle kill spared the allocation (PRD-HPC-1461 D-72): the attempt's
// time limit minus the time it ran, times the nodes. A guest on a shared node reports 0, because
// its host's booking covers the node whether the guest runs or not. An unknown start or an
// attempt that ran past its limit saves nothing.
func nodeMinutesSaved(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus, end metav1.Time) int64 {
	if run.HostSiteJobID != "" || run.StartTime == nil || run.StartTime.IsZero() {
		return 0
	}
	elapsed := int64(math.Ceil(end.Sub(run.StartTime.Time).Minutes()))
	left := int64(job.EffectiveWallMinutes()) - elapsed
	if left <= 0 {
		return 0
	}
	return left * int64(max(job.Spec.Nodes, 1))
}

// idleMessage is the job's status message for an attempt the idle watchdog ended: what happened,
// the minutes it spared and, when the runner reported them, the step's last log lines.
func idleMessage(job *hpcv1alpha1.HPCArgoWorkflowJob, run *hpcv1alpha1.RunStatus, saved int64, settled string) string {
	msg := settled + "; the idle watchdog ended the step after " +
		strconv.Itoa(int(job.EffectiveIdleTimeoutMinutes())) + " minutes without GPU/CPU use or log output"
	if saved > 0 {
		msg += " (" + strconv.FormatInt(saved, 10) + " node-minutes saved)"
	}
	if run.LastLog != "" {
		msg += "; last log lines: " + run.LastLog
	}
	return msg
}

// noteIdleKill raises the Warning event and counts the kill (metric kaos_hpc_idle_kills_total).
func (r *HPCArgoWorkflowJobReconciler) noteIdleKill(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite,
	tier hpcv1alpha1.Tier, msg string) {
	metrics.IdleKills.WithLabelValues(string(tier), projectOf(site)).Inc()
	if r.Recorder != nil {
		r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonIdle, msg)
	}
}
