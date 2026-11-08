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

	LabelQOS         = "qos"
	LabelReservation = "reservation"
	LabelFallback    = "fallback"
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

	// SiteUp is 1 while the site's REST API answers and 0 while it does not (AC-30).
	SiteUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaos_hpc_site_up",
		Help: "1 while the HPC site's REST API answers, 0 while it does not.",
	}, []string{LabelSite, LabelProject})

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

	// LanePlacements counts attempts by the lane (QoS and reservation) they were placed in
	// (PRD-HPC-1461 F-42). fallback is "false" at admission, when the controller chose the lane,
	// and "true" when the runner reports that the site refused it and the attempt ran on the
	// class default: a refused attempt is counted in both, so fallback="true" is the refusal count.
	LanePlacements = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaos_hpc_lane_placements_total",
		Help: "Attempts by the lane they were placed in; fallback=true counts lanes the site refused.",
	}, []string{LabelQOS, LabelReservation, LabelFallback})

	// IdleKills counts steps the on-node idle watchdog ended (PRD-HPC-1461 D-72, F-46).
	IdleKills = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaos_hpc_idle_kills_total",
		Help: "Steps ended with reason Idle by the on-node idle watchdog.",
	}, []string{LabelTier, LabelProject})

	// K8sIdleKills counts in-cluster GPU steps the idle watchdog ended (PRD-HPC-1461 D-72, D-76).
	K8sIdleKills = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaos_k8s_idle_kills_total",
		Help: "In-cluster GPU steps ended with reason Idle by the pool's idle watchdog.",
	}, []string{LabelNamespace})

	// K8sIdleErrors counts failed steps of the in-cluster idle watchdog, by stage; a watchdog that
	// cannot read its metric or logs ends nothing, so this is the alert signal.
	K8sIdleErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaos_k8s_idle_watchdog_errors_total",
		Help: "Failed steps of the in-cluster idle watchdog, by stage.",
	}, []string{"stage"})

	// BudgetWarning is 1 while a project's used plus booked node-minutes on a site are at or above the
	// warning threshold of what it may spend this period (PRD-HPC-1461 AC-22), else 0.
	BudgetWarning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaos_hpc_budget_warning",
		Help: "1 while used plus booked node-minutes are at or above budget.warnAtPercent of what the project may spend.",
	}, []string{LabelSite, LabelProject, LabelTier})

	// BudgetPercent is used plus booked as a percentage of what the project may spend this period
	// (the lower of its share and the site's remainder); 0 when nothing may be spent.
	BudgetPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaos_hpc_budget_percent",
		Help: "Used plus booked node-minutes as a percentage of what the project may spend this period.",
	}, []string{LabelSite, LabelProject, LabelTier})

	// RunnerRestarts counts runner pods that failed and were replaced by their Job.
	RunnerRestarts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kaos_hpc_runner_restarts_total",
		Help: "Runner pods that failed and were retried by their Job.",
	})
)

func init() {
	ctrlmetrics.Registry.MustRegister(JobPhase, JobQueueSeconds, JobRunSeconds, NodeMinutesBooked,
		NodeMinutesSettled, SiteNodeMinutes, ActiveJobs, RunnerRestarts, LanePlacements, IdleKills,
		K8sIdleKills, K8sIdleErrors, BudgetWarning, BudgetPercent, SiteUp)
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

// SetBudgetWarning publishes one tier's budget percentage and whether it is at or above the threshold.
func SetBudgetWarning(site, project, tier string, percent int64, warning bool) {
	v := 0.0
	if warning {
		v = 1
	}
	BudgetWarning.WithLabelValues(site, project, tier).Set(v)
	BudgetPercent.WithLabelValues(site, project, tier).Set(float64(max(percent, 0)))
}
