/*
Copyright 2026 The KAOS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

// WaitingResync is how often a Waiting job is judged again (bookings settle, limits free up).
const WaitingResync = 30 * time.Second

// HPCJobReconciler admits, books and settles HPCJobs against their project's HPCSite (phase 0,
// PRD-HPC-1461 F-26, D-12, D-24, D-49). Admission runs one job at a time (MaxConcurrentReconciles
// is 1), and the ledger is derived from the jobs themselves plus the bookings this process made
// that the cache may not show yet, so concurrent jobs never overbook.
type HPCJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Now is the clock; tests replace it.
	Now func() time.Time

	mu      sync.Mutex
	pending map[string]hpcv1alpha1.Booking // by job UID
}

// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcsites,verbs=get;list;watch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcsites/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile judges a new or waiting job, follows an admitted one, and settles it when it ends.
func (r *HPCJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var job hpcv1alpha1.HPCJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if job.Status.Phase.Terminal() {
		r.forget(string(job.UID))
		return ctrl.Result{}, nil
	}

	site, reason, msg, err := r.siteOf(ctx, &job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if site == nil {
		return ctrl.Result{}, r.setPhase(ctx, &job, hpcv1alpha1.PhaseRefused, reason, msg, nil)
	}

	switch {
	case job.Status.Booking != nil || r.isPending(string(job.UID)):
		// Admitted already (the cache may lag behind our own write): follow it.
		if job.Status.Booking == nil {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		if err := r.follow(ctx, &job); err != nil {
			return ctrl.Result{}, err
		}
	default:
		res, err := r.judge(ctx, &job, site)
		if err != nil || res.RequeueAfter > 0 {
			return res, err
		}
	}
	if err := r.publishLedger(ctx, site); err != nil {
		log.Error(err, "publishing the ledger", "site", client.ObjectKeyFromObject(site))
	}
	return ctrl.Result{}, nil
}

// siteOf resolves the job's project from its namespace label and the project's HPCSite that offers
// the job's class (3.11). Nothing in the job names a project or a site (T-02).
func (r *HPCJobReconciler) siteOf(ctx context.Context, job *hpcv1alpha1.HPCJob) (*hpcv1alpha1.HPCSite, string, string, error) {
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: job.Namespace}, &ns); err != nil {
		return nil, "", "", err
	}
	project := ns.Labels[hpcv1alpha1.ProjectLabel]
	if project == "" {
		return nil, hpcv1alpha1.ReasonNoProject,
			"namespace " + job.Namespace + " belongs to no KAOS project (no " + hpcv1alpha1.ProjectLabel + " label)", nil
	}
	var sites hpcv1alpha1.HPCSiteList
	if err := r.List(ctx, &sites, client.MatchingLabels{hpcv1alpha1.ProjectLabel: project}); err != nil {
		return nil, "", "", err
	}
	for i := range sites.Items {
		if _, ok := admission.ClassOf(&sites.Items[i], job.Spec.Class); ok {
			return &sites.Items[i], "", "", nil
		}
	}
	return nil, hpcv1alpha1.ReasonNoSite, "project " + project + " has no HPC site offering class " + job.Spec.Class, nil
}

// judge decides a new or waiting job and books it when admitted.
func (r *HPCJobReconciler) judge(ctx context.Context, job *hpcv1alpha1.HPCJob, site *hpcv1alpha1.HPCSite) (ctrl.Result, error) {
	l, err := r.ledgerOf(ctx, site)
	if err != nil {
		return ctrl.Result{}, err
	}
	d := admission.Decide(job, site, l)
	switch d.Phase {
	case hpcv1alpha1.PhaseAdmitted:
		b := &hpcv1alpha1.Booking{Site: site.Spec.Site, SiteName: site.Name, SiteNamespace: site.Namespace,
			Tier: d.Tier, NodeMinutes: d.Cost, At: metav1.NewTime(r.now())}
		r.remember(string(job.UID), *b)
		if err := r.setPhase(ctx, job, d.Phase, d.Reason, d.Message, b); err != nil {
			r.forget(string(job.UID))
			return ctrl.Result{}, err
		}
		if d.OverShare && r.Recorder != nil {
			r.Recorder.Event(job, corev1.EventTypeWarning, "OverShare", d.Message)
		}
		return ctrl.Result{}, nil
	case hpcv1alpha1.PhaseWaiting:
		if job.Status.Phase != d.Phase || job.Status.Reason != d.Reason || job.Status.Message != d.Message {
			if err := r.setPhase(ctx, job, d.Phase, d.Reason, d.Message, nil); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: WaitingResync}, nil
	default:
		return ctrl.Result{}, r.setPhase(ctx, job, d.Phase, d.Reason, d.Message, nil)
	}
}

// follow moves an admitted job to Running when the site job exists, and settles it when the
// submitter reports it will not run again (phase 0: the in-pod runner writes status.run).
func (r *HPCJobReconciler) follow(ctx context.Context, job *hpcv1alpha1.HPCJob) error {
	run := job.Status.Run
	if run == nil {
		return nil
	}
	if run.Terminal {
		return r.settle(ctx, job)
	}
	if run.SiteJobID != "" && job.Status.Phase == hpcv1alpha1.PhaseAdmitted {
		return r.setPhase(ctx, job, hpcv1alpha1.PhaseRunning, "Submitted", "site job "+run.SiteJobID, job.Status.Booking)
	}
	return nil
}

// settle ends a job: its actual spend replaces its booking (D-12). Phase 0 settles from the run
// times the site reported; phase 1 reads the site's accounting record (D-32). A guest on a shared
// node is covered by its host's booking and settles to zero (D-26).
func (r *HPCJobReconciler) settle(ctx context.Context, job *hpcv1alpha1.HPCJob) error {
	run := job.Status.Run
	var used int64
	if run.HostSiteJobID == "" && run.StartTime != nil && run.EndTime != nil {
		nodes := int64(max(job.Spec.Nodes, 1))
		used = nodes * int64(math.Ceil(run.EndTime.Sub(run.StartTime.Time).Minutes()))
	}
	phase, reason := outcome(run)
	orig := job.DeepCopy()
	job.Status.Phase, job.Status.Reason = phase, reason
	job.Status.Message = "settled " + strconv.FormatInt(used, 10) + " node-minutes from the run times the site reported"
	job.Status.SettledNodeMinutes = &used
	if err := r.Status().Patch(ctx, job, client.MergeFrom(orig)); err != nil {
		return err
	}
	r.forget(string(job.UID))
	return nil
}

// outcome maps the site's final state and exit code to the job's phase.
func outcome(run *hpcv1alpha1.RunStatus) (hpcv1alpha1.Phase, string) {
	state := strings.ToUpper(run.State)
	switch {
	case strings.HasPrefix(state, "CANCELLED"):
		return hpcv1alpha1.PhaseCancelled, "Cancelled"
	case run.ExitCode != nil && *run.ExitCode == 85:
		// The step saved and continues in a new job (D-44); that attempt is a new HPCJob.
		return hpcv1alpha1.PhaseFailed, "Continued"
	case state == "COMPLETED" && (run.ExitCode == nil || *run.ExitCode == 0):
		return hpcv1alpha1.PhaseSucceeded, "Completed"
	case state == "":
		return hpcv1alpha1.PhaseFailed, "Ended"
	default:
		return hpcv1alpha1.PhaseFailed, "Site" + state
	}
}

// ledgerOf computes the site's ledger from every job booked against it.
func (r *HPCJobReconciler) ledgerOf(ctx context.Context, site *hpcv1alpha1.HPCSite) (ledger.Ledger, error) {
	var jobs hpcv1alpha1.HPCJobList
	if err := r.List(ctx, &jobs); err != nil {
		return ledger.Ledger{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range jobs.Items {
		if jobs.Items[i].Status.Booking != nil {
			delete(r.pending, string(jobs.Items[i].UID)) // the cache shows it now
		}
	}
	return ledger.Compute(site, jobs.Items, r.pending, r.now()), nil
}

// publishLedger writes the site's ledger to its status, for people and the hub to read (F-30).
func (r *HPCJobReconciler) publishLedger(ctx context.Context, site *hpcv1alpha1.HPCSite) error {
	l, err := r.ledgerOf(ctx, site)
	if err != nil {
		return err
	}
	orig := site.DeepCopy()
	start := metav1.NewTime(l.PeriodStart)
	site.Status.PeriodStart = &start
	site.Status.ActiveJobs = l.Active
	for tier, dst := range map[hpcv1alpha1.Tier]**hpcv1alpha1.TierLedger{
		hpcv1alpha1.TierGPU: &site.Status.GPU, hpcv1alpha1.TierCPU: &site.Status.CPU} {
		t := l.Tiers[tier]
		*dst = &hpcv1alpha1.TierLedger{UsedNodeMinutes: t.Used, BookedNodeMinutes: t.Booked, LeftNodeMinutes: t.Left()}
	}
	err = r.Status().Patch(ctx, site, client.MergeFrom(orig))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *HPCJobReconciler) setPhase(ctx context.Context, job *hpcv1alpha1.HPCJob, phase hpcv1alpha1.Phase,
	reason, msg string, booking *hpcv1alpha1.Booking) error {
	orig := job.DeepCopy()
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, reason, msg
	if booking != nil {
		job.Status.Booking = booking
	}
	return r.Status().Patch(ctx, job, client.MergeFrom(orig))
}

func (r *HPCJobReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *HPCJobReconciler) remember(uid string, b hpcv1alpha1.Booking) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[string]hpcv1alpha1.Booking{}
	}
	r.pending[uid] = b
}

func (r *HPCJobReconciler) forget(uid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, uid)
}

func (r *HPCJobReconciler) isPending(uid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pending[uid]
	return ok
}

// SetupWithManager sets up the controller with the Manager. One worker: admission is serial.
func (r *HPCJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hpcv1alpha1.HPCJob{}).
		Named("hpcjob").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
