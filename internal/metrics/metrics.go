// Package metrics holds the controller's Prometheus metrics (PRD-HPC-1461 D-65, F-39). They are
// registered on the controller-runtime registry, so the manager's metrics endpoint serves them and
// the pool's vmagent scrapes them. Nothing here talks to the HPC site.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Label names shared by several metrics.
const (
	LabelNamespace = "namespace"
	LabelJob       = "job"
	LabelClass     = "class"
	LabelTier      = "tier"
	LabelPhase     = "phase"
	LabelProject   = "project"
	LabelSite      = "site"
	LabelKind      = "kind"
)

// Kinds of the kaos_hpc_site_node_minutes gauge.
const (
	KindUsed   = "used"
	KindBooked = "booked"
	KindLeft   = "left"
)

var (
	// JobPhase is 1 for the phase a job is in now and absent for the others.
	JobPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaos_hpc_job_phase",
		Help: "1 for the phase an HPCArgoWorkflowJob is in now.",
	}, []string{LabelNamespace, LabelJob, LabelClass, LabelTier, LabelPhase})

	// JobQueueSeconds is the time from admission to the site job starting, observed when an attempt ends.
	JobQueueSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kaos_hpc_job_queue_seconds",
		Help:    "Seconds from admission to the site job starting, per attempt.",
		Buckets: prometheus.ExponentialBuckets(30, 2, 12),
	}, []string{LabelTier})

	// JobRunSeconds is the site job's run time (start to end), observed when an attempt ends.
	JobRunSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kaos_hpc_job_run_seconds",
		Help:    "Seconds the site job ran, per attempt.",
		Buckets: prometheus.ExponentialBuckets(60, 2, 12),
	}, []string{LabelTier})

	// NodeMinutesBooked counts node-minutes booked at admission.
	NodeMinutesBooked = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaos_hpc_node_minutes_booked",
		Help: "Node-minutes booked at admission.",
	}, []string{LabelTier, LabelProject})

	// NodeMinutesSettled counts node-minutes settled when an attempt ends.
	NodeMinutesSettled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaos_hpc_node_minutes_settled",
		Help: "Node-minutes actually spent, settled when an attempt ends.",
	}, []string{LabelTier, LabelProject})

	// SiteNodeMinutes is a project's ledger on a site: used, booked and left this period.
	SiteNodeMinutes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaos_hpc_site_node_minutes",
		Help: "Node-minutes of a project on a site this period, by kind (used, booked, left).",
	}, []string{LabelSite, LabelProject, LabelTier, LabelKind})

	// ActiveJobs is the number of jobs holding a booking.
	ActiveJobs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaos_hpc_active_jobs",
		Help: "Admitted jobs that have not ended.",
	}, []string{LabelProject, LabelSite})

	// RunnerRestarts counts runner pods that failed and were replaced by their Job.
	RunnerRestarts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kaos_hpc_runner_restarts_total",
		Help: "Runner pods that failed and were retried by their Job.",
	})
)

func init() {
	ctrlmetrics.Registry.MustRegister(JobPhase, JobQueueSeconds, JobRunSeconds, NodeMinutesBooked,
		NodeMinutesSettled, SiteNodeMinutes, ActiveJobs, RunnerRestarts)
}

// SetJobPhase makes phase the only phase series of the job.
func SetJobPhase(namespace, job, class, tier, phase string) {
	JobPhase.DeletePartialMatch(prometheus.Labels{LabelNamespace: namespace, LabelJob: job})
	JobPhase.WithLabelValues(namespace, job, class, tier, phase).Set(1)
}

// ForgetJob drops a deleted job's phase series.
func ForgetJob(namespace, job string) {
	JobPhase.DeletePartialMatch(prometheus.Labels{LabelNamespace: namespace, LabelJob: job})
}

// SetSiteLedger publishes one tier of a project's ledger on a site.
func SetSiteLedger(site, project, tier string, used, booked, left int64) {
	SiteNodeMinutes.WithLabelValues(site, project, tier, KindUsed).Set(float64(used))
	SiteNodeMinutes.WithLabelValues(site, project, tier, KindBooked).Set(float64(booked))
	SiteNodeMinutes.WithLabelValues(site, project, tier, KindLeft).Set(float64(left))
}
