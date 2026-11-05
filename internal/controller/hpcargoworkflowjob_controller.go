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

// ContinueResync is how soon the next attempt of a continued job is judged.
const ContinueResync = 100 * time.Millisecond

// HPCArgoWorkflowJobReconciler admits, books and settles HPCArgoWorkflowJobs against their project's HPCSite (phase 0,
// PRD-HPC-1461 F-26, D-12, D-24, D-49). Admission runs one job at a time (MaxConcurrentReconciles
// is 1), and the ledger is derived from the jobs themselves plus the bookings this process made
// that the cache may not show yet, so concurrent jobs never overbook.
type HPCArgoWorkflowJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Now is the clock; tests replace it.
	Now func() time.Time

	mu      sync.Mutex
	pending map[string]hpcv1alpha1.Booking // by job UID
}

// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcargoworkflowjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcargoworkflowjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcsites,verbs=get;list;watch
// +kubebuilder:rbac:groups=hpc.kaos.io,resources=hpcsites/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile judges a new or waiting job, follows an admitted one, and settles it when it ends.
func (r *HPCArgoWorkflowJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var job hpcv1alpha1.HPCArgoWorkflowJob
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
		next, err := r.follow(ctx, &job)
		if err != nil {
			return ctrl.Result{}, err
		}
		if next {
			// A continuation: judge the next attempt right away.
			return ctrl.Result{RequeueAfter: ContinueResync}, nil
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
func (r *HPCArgoWorkflowJobReconciler) siteOf(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (*hpcv1alpha1.HPCSite, string, string, error) {
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
func (r *HPCArgoWorkflowJobReconciler) judge(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) (ctrl.Result, error) {
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

// follow moves an admitted job to Running when the site job exists, and ends the attempt when the
// submitter reports it will not run again (phase 0: the in-pod runner writes status.run). It
// returns true when the job continues in a next attempt.
func (r *HPCArgoWorkflowJobReconciler) follow(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (bool, error) {
	run := job.Status.Run
	if run == nil || !current(run, job.Status.Attempt) {
		return false, nil // nothing reported yet, or a report of an earlier attempt
	}
	if run.Terminal {
		return r.endAttempt(ctx, job)
	}
	if run.SiteJobID != "" && job.Status.Phase == hpcv1alpha1.PhaseAdmitted {
		return false, r.setPhase(ctx, job, hpcv1alpha1.PhaseRunning, "Submitted", "site job "+run.SiteJobID, job.Status.Booking)
	}
	return false, nil
}

// current reports whether a submitter's report is about the job's current attempt. A report without
// an attempt number counts only for attempt 1: after a continuation it may be the old attempt's.
func current(run *hpcv1alpha1.RunStatus, attempt int32) bool {
	if run.Attempt == 0 {
		return attempt <= 1
	}
	return run.Attempt == attempt
}

// endAttempt settles the current attempt and either ends the job or continues it under this same
// HPCArgoWorkflowJob (D-44 as amended 2026-11-05, F-29). The attempt's actual spend replaces its booking
// (D-12); phase 0 settles from the run times the site reported, phase 1 from the site's accounting
// record (D-32). A guest on a shared node is covered by its host's booking and settles to zero
// (D-26). A continuation needs a resumable step, a checkpoint the job staged out before it exited
// (T-19), and a continuation left under the step's cap; it is then admitted and booked like a new
// step (A4).
func (r *HPCArgoWorkflowJobReconciler) endAttempt(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (bool, error) {
	run := job.Status.Run
	var used int64
	if run.HostSiteJobID == "" && run.StartTime != nil && run.EndTime != nil {
		nodes := int64(max(job.Spec.Nodes, 1))
		used = nodes * int64(math.Ceil(run.EndTime.Sub(run.StartTime.Time).Minutes()))
	}
	end := metav1.NewTime(r.now())
	if run.EndTime != nil {
		end = *run.EndTime
	}
	phase, reason := outcome(run)
	orig := job.DeepCopy()
	attempt := max(job.Status.Attempt, 1)
	tier := hpcv1alpha1.TierGPU
	if job.Status.Booking != nil {
		tier = job.Status.Booking.Tier
	}
	job.Status.Attempts = append(job.Status.Attempts, hpcv1alpha1.AttemptRecord{Attempt: attempt,
		SiteJobID: run.SiteJobID, ExitCode: run.ExitCode, Outcome: reason, Tier: tier,
		SettledNodeMinutes: used, EndTime: end})
	job.Status.Booking = nil
	msg := "attempt " + strconv.Itoa(int(attempt)) + " settled " + strconv.FormatInt(used, 10) + " node-minutes"

	next := false
	if run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeContinue {
		res := job.Spec.Resumable
		switch {
		case res == nil:
			phase, reason, msg = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNotResumable,
				msg+"; the step exited 85 but is not declared resumable"
		case run.CheckpointRef == "":
			phase, reason, msg = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonCheckpointMissing,
				msg+"; checkpoint upload incomplete: no staged-out checkpoint to continue from"
		case job.Status.Continuations >= res.MaxContinuations:
			phase, reason, msg = hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonContinuationLimit,
				msg+"; the step reached its "+strconv.Itoa(int(res.MaxContinuations))+" continuations"
		default:
			next = true
			job.Status.Continuations++
			job.Status.Attempt = attempt + 1
			job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: run.CheckpointRef, FromAttempt: attempt}
			phase, reason = hpcv1alpha1.PhasePending, hpcv1alpha1.ReasonContinuing
			msg += "; continuing from checkpoint " + run.CheckpointRef + " as attempt " + strconv.Itoa(int(attempt+1))
		}
	}
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, reason, msg
	if phase.Terminal() {
		var total int64
		for _, a := range job.Status.Attempts {
			total += a.SettledNodeMinutes
		}
		job.Status.SettledNodeMinutes = &total
	}
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	r.forget(string(job.UID))
	return next, nil
}

// outcome maps the site's final state and exit code of one attempt to the job's phase.
func outcome(run *hpcv1alpha1.RunStatus) (hpcv1alpha1.Phase, string) {
	state := strings.ToUpper(run.State)
	switch {
	case strings.HasPrefix(state, "CANCELLED"):
		return hpcv1alpha1.PhaseCancelled, "Cancelled"
	case run.ExitCode != nil && *run.ExitCode == hpcv1alpha1.ExitCodeContinue:
		return hpcv1alpha1.PhaseFailed, "Continued"
	case state == "COMPLETED" && (run.ExitCode == nil || *run.ExitCode == 0):
		return hpcv1alpha1.PhaseSucceeded, "Completed"
	case state == "TIMEOUT":
		return hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonWallTimeReached
	case state == "":
		return hpcv1alpha1.PhaseFailed, "Ended"
	default:
		return hpcv1alpha1.PhaseFailed, "Site" + state
	}
}

// ledgerOf computes the site's ledger from every job booked against it.
func (r *HPCArgoWorkflowJobReconciler) ledgerOf(ctx context.Context, site *hpcv1alpha1.HPCSite) (ledger.Ledger, error) {
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
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
func (r *HPCArgoWorkflowJobReconciler) publishLedger(ctx context.Context, site *hpcv1alpha1.HPCSite) error {
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

func (r *HPCArgoWorkflowJobReconciler) setPhase(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, phase hpcv1alpha1.Phase,
	reason, msg string, booking *hpcv1alpha1.Booking) error {
	orig := job.DeepCopy()
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, reason, msg
	if booking != nil {
		job.Status.Booking = booking
		if job.Status.Site == nil {
			job.Status.Site = &hpcv1alpha1.SiteRef{Name: booking.SiteName, Namespace: booking.SiteNamespace}
		}
		if job.Status.Attempt == 0 {
			job.Status.Attempt = 1
		}
	}
	// The optimistic lock makes a decision made on a stale read fail and be retried on fresh data.
	return r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}

func (r *HPCArgoWorkflowJobReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *HPCArgoWorkflowJobReconciler) remember(uid string, b hpcv1alpha1.Booking) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[string]hpcv1alpha1.Booking{}
	}
	r.pending[uid] = b
}

func (r *HPCArgoWorkflowJobReconciler) forget(uid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, uid)
}

func (r *HPCArgoWorkflowJobReconciler) isPending(uid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pending[uid]
	return ok
}

// SetupWithManager sets up the controller with the Manager. One worker: admission is serial.
func (r *HPCArgoWorkflowJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hpcv1alpha1.HPCArgoWorkflowJob{}).
		Named("hpcargoworkflowjob").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
