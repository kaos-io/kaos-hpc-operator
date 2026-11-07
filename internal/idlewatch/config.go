// Package idlewatch is the idle watchdog for in-cluster (Kubernetes) GPU steps (PRD-HPC-1461 D-72,
// D-76). A GPU step whose GPU utilisation stays below a threshold AND whose log does not grow for
// the idle window (20 minutes) is ended with the reason Idle, so an idle cloud GPU stops costing
// money. The same rule as the HPC job script; here it runs on the pool because a sidecar cannot see
// a GPU that the device plugin allocated exclusively to the main container.
//
// The watchdog never acts on a signal it cannot read: no metric, a short sample history or an
// unreadable log all mean "unknown", never "idle".
package idlewatch

import "time"

// Names the rest of the platform shares with this package. The retry expression of the Hera
// template (kubecore/resilience.py) excludes MessagePrefix, so change both together.
const (
	// AnnotationIdleTimeout is the per-step override, set by the template from step(idle_timeout=...):
	// whole minutes, "0" turns the watchdog off for the pod.
	AnnotationIdleTimeout = "platform.kubecore.io/idle-timeout"
	// AnnotationIdleEnded marks a pod the watchdog ended: the RFC 3339 time of the decision.
	AnnotationIdleEnded = "platform.kubecore.io/idle-ended-at"
	// LabelIdleEnded is "true" on a pod the watchdog ended; the reaper lists by it.
	LabelIdleEnded = "platform.kubecore.io/idle-ended"
	// ReasonIdle is the pod status reason and the event reason.
	ReasonIdle = "Idle"
	// MessagePrefix starts the pod status message Argo shows as the step's message. It must never
	// contain a word of the pod-loss retry expression (evicted, deleted, node is draining, ...).
	MessagePrefix = "KaosIdle"

	// LabelArgoWorkflow is on every pod an Argo workflow runs; the only pods the watchdog looks at.
	LabelArgoWorkflow = "workflows.argoproj.io/workflow"
	// AnnotationArgoNodeID names the pod's node in the Workflow's status.
	AnnotationArgoNodeID = "workflows.argoproj.io/node-id"
	// MainContainer is the Argo container that runs the step's code.
	MainContainer = "main"
	// GPUResource is the extended resource that makes a step a GPU step.
	GPUResource = "nvidia.com/gpu"
)

// valueTrue is the label value of a marked pod.
const valueTrue = "true"

// Defaults of Config; every one is a flag of the manager.
const (
	DefaultWindow         = 20 * time.Minute
	DefaultBusyPercent    = 5.0
	DefaultScanInterval   = 30 * time.Second
	DefaultScrapeInterval = time.Minute
	DefaultConfirmTimeout = 5 * time.Minute
	// DefaultMetric is the GKE accelerator duty cycle the pool's stackdriver exporter publishes
	// (compositions/apis/kubepool/gcp/observability-cost). A pool with DCGM exporter sets
	// DCGM_FI_DEV_GPU_UTIL and the label names "namespace" and "pod".
	DefaultMetric         = "stackdriver_k_8_s_container_kubernetes_io_container_accelerator_duty_cycle"
	DefaultNamespaceLabel = "namespace_name"
	DefaultPodLabel       = "pod_name"
	// MinSampleCoverage is the share of the expected samples of the window that must exist before the
	// utilisation counts as known (a missing scrape or a late metric must not look like idleness).
	MinSampleCoverage = 0.5
)

// Config is the watchdog's settings. The zero value of a field means its default.
type Config struct {
	// MetricsURL is the PromQL API base (VictoriaMetrics vmselect or Prometheus). Empty: no watchdog.
	MetricsURL string
	// Metric is the GPU utilisation gauge in percent (0-100) per pod.
	Metric string
	// NamespaceLabel and PodLabel name the metric's labels that identify the pod.
	NamespaceLabel, PodLabel string
	// Window is the idle window of a step without its own annotation.
	Window time.Duration
	// BusyPercent is the utilisation at or above which the GPU counts as in use.
	BusyPercent float64
	// ScanInterval is how often the watchdog looks at the pool's GPU pods.
	ScanInterval time.Duration
	// ScrapeInterval is the metric's scrape interval; it sets how many samples a window should hold.
	ScrapeInterval time.Duration
	// ConfirmTimeout is the longest a pod marked Failed waits for Argo to record the step before it
	// is deleted anyway (the Workflow is unreadable, or its node status is offloaded).
	ConfirmTimeout time.Duration
}

// WithDefaults returns c with every unset field at its default.
func (c Config) WithDefaults() Config {
	if c.Metric == "" {
		c.Metric = DefaultMetric
	}
	if c.NamespaceLabel == "" {
		c.NamespaceLabel = DefaultNamespaceLabel
	}
	if c.PodLabel == "" {
		c.PodLabel = DefaultPodLabel
	}
	if c.Window <= 0 {
		c.Window = DefaultWindow
	}
	if c.BusyPercent <= 0 {
		c.BusyPercent = DefaultBusyPercent
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = DefaultScanInterval
	}
	if c.ScrapeInterval <= 0 {
		c.ScrapeInterval = DefaultScrapeInterval
	}
	if c.ConfirmTimeout <= 0 {
		c.ConfirmTimeout = DefaultConfirmTimeout
	}
	return c
}
