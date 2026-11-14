package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/admission"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// Serve mode (PRD-HPC-1461 phase 1, D-58, D-64): a job with spec.stepName runs through a runner
// Job per attempt in its HPCSite's namespace. Admission, booking and the continuation decision stay
// in the reconciler; this file creates and watches the runner Jobs and cancels with the Workflow.

const (
	// ServeResync is how often a live serve-mode job is looked at again: the owning Workflow is
	// polled, not watched.
	ServeResync = 15 * time.Second
	// CancelPoll is how often a cancelling job checks that its runner Job is gone.
	CancelPoll = 2 * time.Second
	// RunnerReportGrace is how long a finished runner Job may wait for its terminal report to show
	// in the cache before the attempt counts as failed.
	RunnerReportGrace = 15 * time.Second
	// OpenHostPoll is how often an ended host whose Slurm job still holds the node (D-26) is looked at,
	// in case its runner ends without reporting the job's end.
	OpenHostPoll = time.Minute

	blockedWaitingReason = "CreateContainerConfigError"
	maxMessage           = 1000
)

// workflowGVK is the Argo Workflow, read as unstructured so the controller has no Argo dependency.
var workflowGVK = schema.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Workflow"}

// isRunnerJob selects the Jobs this controller created.
func isRunnerJob(o client.Object) bool {
	_, ok := o.GetLabels()[runnerjob.LabelJobUID]
	return ok
}

// jobOfRunner maps a runner Job to the HPCArgoWorkflowJob it runs for.
func jobOfRunner(_ context.Context, o client.Object) []reconcile.Request {
	a := o.GetAnnotations()
	if a[runnerjob.AnnotationJobName] == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Namespace: a[runnerjob.AnnotationJobNamespace], Name: a[runnerjob.AnnotationJobName]}}}
}

// serveGate runs first for a serve-mode job that carries its step context. It keeps the cancel finalizer,
// refuses a spec outside the env allowlist, and cancels the job when it is deleted or its Workflow
// ended. done is true when the reconcile is finished.
func (r *HPCArgoWorkflowJobReconciler) serveGate(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (ctrl.Result, bool, error) {
	has := controllerutil.ContainsFinalizer(job, hpcv1alpha1.CancelFinalizer)
	if job.Status.Phase.Terminal() {
		return ctrl.Result{}, false, r.setFinalizer(ctx, job, false)
	}
	cause := ""
	if !job.DeletionTimestamp.IsZero() {
		cause = "the job was deleted"
	} else {
		var err error
		if cause, err = r.workflowCancelCause(ctx, job); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("reading the owning Workflow: %w", err)
		}
	}
	if cause != "" {
		res, err := r.cancel(ctx, job, cause)
		return res, true, err
	}
	if job.Status.Booking == nil {
		if err := runnerjob.ValidateEnv(job.Spec.Env); err != nil {
			return ctrl.Result{}, true, r.setPhase(ctx, job, hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonInvalidSpec, err.Error(), nil)
		}
		if err := runnerjob.ValidateImage(job.Spec.Image); err != nil {
			// Nothing is booked and no runner starts: no site job, no node time, for a reference no credential fixes.
			return ctrl.Result{}, true, r.setPhase(ctx, job, hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonInvalidImage, err.Error(), nil)
		}
		if err := r.checkApp(ctx, job); err != nil {
			// The job names an app that is not the one its Workflow belongs to: nothing is booked and no runner starts.
			if r.Recorder != nil {
				r.Recorder.Event(job, corev1.EventTypeWarning, hpcv1alpha1.ReasonAppIdentityUnavailable, err.Error())
			}
			return ctrl.Result{}, true, r.setPhase(ctx, job, hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonAppIdentityUnavailable, err.Error(), nil)
		}
	}
	if !has {
		if err := r.setFinalizer(ctx, job, true); err != nil {
			return ctrl.Result{}, true, err
		}
	}
	return ctrl.Result{}, false, nil
}

// setFinalizer adds or removes the cancel finalizer. The optimistic lock keeps it from wiping
// finalizers another writer set.
func (r *HPCArgoWorkflowJobReconciler) setFinalizer(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, want bool) error {
	if controllerutil.ContainsFinalizer(job, hpcv1alpha1.CancelFinalizer) == want {
		return nil
	}
	orig := job.DeepCopy()
	if want {
		controllerutil.AddFinalizer(job, hpcv1alpha1.CancelFinalizer)
	} else {
		controllerutil.RemoveFinalizer(job, hpcv1alpha1.CancelFinalizer)
	}
	err := r.Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
	return client.IgnoreNotFound(err)
}

// workflowCancelCause says why the job must be cancelled for its Workflow, or "" when it need not.
// A Workflow that cannot be read for lack of the Argo API (not installed) is not a reason to cancel.
func (r *HPCArgoWorkflowJobReconciler) workflowCancelCause(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (string, error) {
	ref := job.Spec.Workflow
	if ref == nil {
		return "", nil
	}
	wf := &unstructured.Unstructured{}
	wf.SetGroupVersionKind(workflowGVK)
	err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: ref.Name}, wf)
	switch {
	case apierrors.IsNotFound(err):
		return "the Workflow " + ref.Name + " was deleted", nil
	case meta.IsNoMatchError(err):
		// No Argo API in this cluster (a standalone install, PRD-HPC-1461 AC-41): nothing can end
		// the Workflow, so it is no reason to cancel and not a reason to fail every reconcile.
		return "", nil
	case err != nil:
		return "", err
	case string(wf.GetUID()) != ref.UID:
		return "the Workflow " + ref.Name + " was deleted and re-created", nil
	}
	if shutdown, _, _ := unstructured.NestedString(wf.Object, "spec", "shutdown"); shutdown != "" {
		return "the Workflow was shut down (" + shutdown + ")", nil
	}
	switch phase, _, _ := unstructured.NestedString(wf.Object, "status", "phase"); phase {
	case "Succeeded", "Failed", "Error":
		return "the Workflow ended (" + phase + ") while the step had not", nil
	}
	return "", nil
}

// workflowAppLabel is the label of a pipeline's WorkflowTemplate (and so of the Workflows made from it) that names the
// app; the template itself is named <app>-pipeline.
const workflowAppLabel = "platform.kubecore.io/app"

// checkApp holds spec.app against the app of the Workflow that owns the job (PRD kaos#1911, AC-14). The follower that
// creates the job is one service account for every app of the project, so the API server cannot tell whose job it is;
// the Workflow can: it is made from the app's own WorkflowTemplate, whose label and name (<app>-pipeline) give the app.
// A job that names another app would read and write that app's data. Without a Workflow reference (a standalone
// install) or without the Argo API there is nothing to compare with.
func (r *HPCArgoWorkflowJobReconciler) checkApp(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) error {
	app := job.Spec.App
	if app == "" {
		return fmt.Errorf("the job names no app, so it has no identity for its data. Nothing was staged and nothing was submitted to the site")
	}
	ref := job.Spec.Workflow
	if ref == nil {
		return nil
	}
	wf := &unstructured.Unstructured{}
	wf.SetGroupVersionKind(workflowGVK)
	switch err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: ref.Name}, wf); {
	case apierrors.IsNotFound(err), meta.IsNoMatchError(err):
		return nil // workflowCancelCause has said or will say what that means
	case err != nil:
		return fmt.Errorf("reading the owning Workflow: %w", err)
	}
	var trusted []string
	if v := wf.GetLabels()[workflowAppLabel]; v != "" {
		trusted = append(trusted, v)
	}
	if name, _, _ := unstructured.NestedString(wf.Object, "spec", "workflowTemplateRef", "name"); strings.HasSuffix(name, "-pipeline") {
		trusted = append(trusted, strings.TrimSuffix(name, "-pipeline"))
	}
	if len(trusted) == 0 {
		return fmt.Errorf("the Workflow %s names no app (no %s label, no <app>-pipeline template), so the job's app %s cannot be confirmed. "+
			"Nothing was staged and nothing was submitted to the site", ref.Name, workflowAppLabel, app)
	}
	for _, t := range trusted {
		if t != app {
			return fmt.Errorf("the job names the app %s, but its Workflow %s belongs to the app %s. Nothing was staged and nothing was "+
				"submitted to the site", app, ref.Name, t)
		}
	}
	return nil
}

// cancel stops a serve-mode job: it deletes the runner Job in the foreground (its SIGTERM handler
// cancels the site job), waits until it is gone, settles the attempt, marks the job Cancelled and
// drops the finalizer (D-04).
func (r *HPCArgoWorkflowJobReconciler) cancel(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, cause string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	first := job.Annotations[hpcv1alpha1.CancelRequestedAnnotation] == ""
	// The marker comes first: the runner tells a cancel from a node drain by it.
	if err := r.markCancelRequested(ctx, job); err != nil {
		return ctrl.Result{}, err
	}
	// A runner that was killed before it reported its site job may have submitted it all the same (the
	// SIGTERM handler cancels only a job id it already holds). The sweep is decided now, while the runner
	// Job still shows that a runner ran, and is kept in status: the cancel-only runner finds the site job by key.
	if may, err := r.runnerMayHaveSubmitted(ctx, job); err != nil {
		return ctrl.Result{}, err
	} else if may {
		if err := r.beginSweep(ctx, job, hpcv1alpha1.PhaseCancelled, "Cancelled", "cancelled: "+cause); err != nil {
			return ctrl.Result{}, err
		}
	}
	if first {
		log.Info("cancelling the job", "cause", cause, "siteJobID", siteJobIDOf(job), "sweep", job.Status.Sweep != nil)
	}
	gone, err := r.deleteRunnerJobs(ctx, job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !gone {
		return ctrl.Result{RequeueAfter: CancelPoll}, nil
	}
	// The runner may have reported while it shut down: work on the latest copy.
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	site, _, _, _ := r.siteOf(ctx, job)
	if !job.Status.Phase.Terminal() && (job.Status.Sweep != nil || siteJobMayLive(job)) {
		// The runner is gone without proof that the site job ended: the site must confirm it first.
		if err := r.beginSweep(ctx, job, hpcv1alpha1.PhaseCancelled, "Cancelled", "cancelled: "+cause); err != nil {
			return ctrl.Result{}, err
		}
		return r.continueSweep(ctx, job, site)
	}
	if !job.Status.Phase.Terminal() {
		if err := r.failAttempt(ctx, job, site, hpcv1alpha1.PhaseCancelled, "Cancelled", "Cancelled", "cancelled: "+cause); err != nil {
			return ctrl.Result{}, err
		}
	}
	log.Info("the job is cancelled: the finalizer is dropped", "cause", cause, "phase", job.Status.Phase)
	return ctrl.Result{}, r.setFinalizer(ctx, job, false)
}

// markCancelRequested sets hpc.kaos.io/cancel-requested on the job and returns only once the API
// server (not the informer cache) shows it, so the runner Job's SIGTERM can never overtake it.
func (r *HPCArgoWorkflowJobReconciler) markCancelRequested(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) error {
	if job.Annotations[hpcv1alpha1.CancelRequestedAnnotation] == "" {
		orig := job.DeepCopy()
		if job.Annotations == nil {
			job.Annotations = map[string]string{}
		}
		job.Annotations[hpcv1alpha1.CancelRequestedAnnotation] = r.now().UTC().Format(time.RFC3339)
		if err := r.Patch(ctx, job, client.MergeFrom(orig)); err != nil {
			return fmt.Errorf("marking the cancel request: %w", err)
		}
	}
	var live hpcv1alpha1.HPCArgoWorkflowJob
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(job), &live); err != nil {
		return fmt.Errorf("confirming the cancel request: %w", err)
	}
	if live.Annotations[hpcv1alpha1.CancelRequestedAnnotation] == "" {
		return fmt.Errorf("the cancel request is not in the API yet")
	}
	return nil
}

// deleteRunnerJobs deletes the job's runner Jobs with Foreground propagation and reports whether
// none is left. Foreground makes the Job wait for its pods, so the runner's SIGTERM handler runs
// before the Job disappears.
func (r *HPCArgoWorkflowJobReconciler) deleteRunnerJobs(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob) (bool, error) {
	if job.Status.Site == nil {
		return true, nil // never admitted: no runner Job exists
	}
	var jobs batchv1.JobList
	if err := r.reader().List(ctx, &jobs, client.InNamespace(job.Status.Site.Namespace),
		client.MatchingLabels{runnerjob.LabelJobUID: string(job.UID)}); err != nil {
		return false, err
	}
	live := 0
	for i := range jobs.Items {
		rj := &jobs.Items[i]
		if rj.Labels[runnerjob.LabelCancelOnly] != "" {
			continue // the sweep's own runner finishes the cancel; it is not the one being stopped
		}
		live++
		if !rj.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, rj, client.PropagationPolicy(metav1.DeletePropagationForeground)); client.IgnoreNotFound(err) != nil {
			return false, err
		}
	}
	return live == 0, nil
}

func (r *HPCArgoWorkflowJobReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// driveRunner makes sure the current attempt has its runner Job and reacts to the Job ending
// without a terminal report. It runs only for a booked job (admission came first).
func (r *HPCArgoWorkflowJobReconciler) driveRunner(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite) error {
	attempt := max(job.Status.Attempt, 1)
	key := types.NamespacedName{Namespace: site.Namespace, Name: runnerjob.Name(job.UID, attempt)}
	var rj batchv1.Job
	err := r.Get(ctx, key, &rj)
	if apierrors.IsNotFound(err) {
		// No runner Job is spawned while the site is down: it would only burn its retries (AC-30).
		if held, herr := r.siteWait(ctx, job, site, false); herr != nil || held {
			if herr == nil {
				herr = &requeueError{after: WaitingResync}
			}
			return herr
		}
		// The latest probe failed but the site does not count as down yet (that takes FailuresToDown probes
		// over MinDownAfter): start nothing new in the doubt window and look again soon. The job keeps its
		// phase and reason; one good probe lets it through. Without this every admitted job got a runner Job
		// for a dead site until the verdict, which a starved poller delays (PRD-HPC-1461 AC-106).
		if r.Health.Failing(client.ObjectKeyFromObject(site).String()) {
			return &requeueError{after: FailingSiteResync}
		}
		if job.Spec.Context == "" {
			// Booked before the definition changed and read back without its context: its next runner would
			// start the step without its context and its data. It ends here instead (PRD kaos#1911 T-04).
			return r.failAttempt(ctx, job, site, hpcv1alpha1.PhaseFailed, hpcv1alpha1.ReasonNoContext, hpcv1alpha1.ReasonNoContext,
				"the job carries no step context (spec.context): it was created before the platform update that changed how "+
					"a step is given its inputs. Nothing was submitted for this attempt. Run the pipeline again")
		}
		return r.createRunnerJob(ctx, job, site, attempt)
	}
	if err != nil {
		return err
	}
	if _, err := r.siteWait(ctx, job, site, true); err != nil { // display only: the runner has its own wait
		return err
	}
	if err := r.noteRunner(ctx, job, key.Name, attempt); err != nil {
		return err
	}
	r.countRestarts(&rj)
	switch {
	case jobCondition(&rj, batchv1.JobFailed):
		return r.runnerFailed(ctx, job, site, &rj, "")
	case jobCondition(&rj, batchv1.JobComplete):
		return r.runnerEnded(ctx, job, site, &rj)
	}
	return r.noteBlocked(ctx, job, &rj)
}

// createRunnerJob creates the attempt's runner Job. AlreadyExists is fine: the name is deterministic.
func (r *HPCArgoWorkflowJobReconciler) createRunnerJob(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, attempt int32) error {
	if r.RunnerImage == "" {
		return fmt.Errorf("serve-mode job %s/%s needs a runner image: start the controller with --runner-image", job.Namespace, job.Name)
	}
	rj, err := runnerjob.Build(runnerjob.Config{Image: r.RunnerImage, Scheduling: r.RunnerScheduling, SiteWaitMax: r.siteOutageMax()}, job, site, attempt)
	if err != nil {
		return fmt.Errorf("building the runner Job: %w", err)
	}
	if err := r.Create(ctx, rj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating the runner Job: %w", err)
	}
	logf.FromContext(ctx).Info("runner Job created", "runnerJob", rj.Name, "attempt", attempt)
	return r.noteRunner(ctx, job, rj.Name, attempt)
}

// noteRunner records the current runner Job in status.runner.
func (r *HPCArgoWorkflowJobReconciler) noteRunner(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, name string, attempt int32) error {
	if cur := job.Status.Runner; cur != nil && cur.JobName == name && cur.Attempt == attempt {
		return nil
	}
	orig := job.DeepCopy()
	job.Status.Runner = &hpcv1alpha1.RunnerRef{JobName: name, Attempt: attempt}
	return r.Status().Patch(ctx, job, client.MergeFrom(orig))
}

// countRestarts adds the runner pods that failed since the last look to the restart counter.
func (r *HPCArgoWorkflowJobReconciler) countRestarts(rj *batchv1.Job) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed == nil {
		r.failed = map[string]int32{}
	}
	uid := string(rj.UID)
	if d := rj.Status.Failed - r.failed[uid]; d > 0 {
		metrics.RunnerRestarts.Add(float64(d))
	}
	r.failed[uid] = rj.Status.Failed
}

func jobCondition(rj *batchv1.Job, t batchv1.JobConditionType) bool {
	for _, c := range rj.Status.Conditions {
		if c.Type == t && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// runnerEnded handles a runner Job that finished cleanly without the terminal report the
// reconciler acts on: the report may still be on its way to the cache, so it waits a grace first.
func (r *HPCArgoWorkflowJobReconciler) runnerEnded(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, rj *batchv1.Job) error {
	if t := rj.Status.CompletionTime; t != nil {
		if wait := RunnerReportGrace - r.now().Sub(t.Time); wait > 0 {
			return &requeueError{after: wait}
		}
	}
	return r.runnerFailed(ctx, job, site, rj, "the runner exited without reporting the end of the site job")
}

// FailingSiteResync is how soon a job looks again when its runner Job was held back because the site's latest
// probe failed.
const FailingSiteResync = 10 * time.Second

// requeueError asks the caller to look again after a delay without it being a failure.
type requeueError struct{ after time.Duration }

func (e *requeueError) Error() string { return "requeue after " + e.after.String() }

// runnerFailed ends the attempt: the runner Job is over and the site job's result is unknown.
func (r *HPCArgoWorkflowJobReconciler) runnerFailed(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, rj *batchv1.Job, why string) error {
	if why == "" {
		why = "runner Job " + rj.Name + " failed"
		for _, c := range rj.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Reason != "" {
				why += " (" + c.Reason + ")"
			}
		}
	}
	if m := r.lastPodMessage(ctx, rj); m != "" {
		why += ": " + m
	}
	reason := hpcv1alpha1.ReasonRunnerFailed
	if strings.Contains(why, hpcv1alpha1.ReasonSiteUnavailable+":") {
		reason = hpcv1alpha1.ReasonSiteUnavailable // the runner waited for the site as long as it may and said so
	}
	// A runner that crashed after the site accepted its sbatch but before it reported the id left a job no
	// one knows: the cancel-only runner finds it by name (NOT_SUBMITTED when there is none). A runner that
	// gave up waiting for the site never submitted.
	unreported := false
	if reason != hpcv1alpha1.ReasonSiteUnavailable {
		var err error
		if unreported, err = r.runnerMayHaveSubmitted(ctx, job); err != nil {
			return err
		}
	}
	if siteJobMayLive(job) || unreported {
		// The runner is over but its site job may not be: stop it before the booking is released (AC-08).
		if err := r.beginSweep(ctx, job, hpcv1alpha1.PhaseFailed, reason, truncate(why)); err != nil {
			return err
		}
		return &requeueError{after: SweepPoll}
	}
	return r.failAttempt(ctx, job, site, hpcv1alpha1.PhaseFailed, reason, reason, truncate(why))
}

func truncate(s string) string {
	if len(s) > maxMessage {
		return s[:maxMessage]
	}
	return s
}

// runnerPods lists the pods of a runner Job, newest first.
func (r *HPCArgoWorkflowJobReconciler) runnerPods(ctx context.Context, rj *batchv1.Job) []corev1.Pod {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(rj.Namespace), client.MatchingLabels{runnerjob.LabelRunnerJob: rj.Name}); err != nil {
		return nil
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.After(pods.Items[j].CreationTimestamp.Time)
	})
	return pods.Items
}

// lastPodMessage is the newest runner pod's termination message (the tail of its log on error).
func (r *HPCArgoWorkflowJobReconciler) lastPodMessage(ctx context.Context, rj *batchv1.Job) string {
	for _, p := range r.runnerPods(ctx, rj) {
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil {
				if t.Message != "" {
					return t.Message
				}
				return "exit code " + strconv.Itoa(int(t.ExitCode)) + " " + t.Reason
			}
		}
	}
	return ""
}

// noteBlocked tells the user when the runner cannot start for a missing Secret or ConfigMap,
// instead of leaving the job silently Admitted. The phase and reason stay the controller's.
func (r *HPCArgoWorkflowJobReconciler) noteBlocked(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, rj *batchv1.Job) error {
	if job.Status.Phase != hpcv1alpha1.PhaseAdmitted {
		return nil
	}
	for _, p := range r.runnerPods(ctx, rj) {
		for _, cs := range p.Status.ContainerStatuses {
			w := cs.State.Waiting
			if w == nil || w.Reason != blockedWaitingReason {
				continue
			}
			msg := truncate("the runner cannot start: " + w.Message)
			if job.Status.Message != msg {
				if r.Recorder != nil {
					r.Recorder.Event(job, corev1.EventTypeWarning, "RunnerStartBlocked", msg)
				}
				orig := job.DeepCopy()
				job.Status.Message = msg
				return r.Status().Patch(ctx, job, client.MergeFrom(orig))
			}
			return nil
		}
	}
	return nil
}

// usedMinutes is what the current attempt spent so far: nodes x minutes from the site's start to
// its end, or to now when it has not ended. A guest on a shared node is covered by its host (D-26).
func (r *HPCArgoWorkflowJobReconciler) usedMinutes(job *hpcv1alpha1.HPCArgoWorkflowJob) int64 {
	run := job.Status.Run
	if run == nil || !current(run, job.Status.Attempt) {
		return 0
	}
	return settleAttempt(job, r.now()).Minutes
}

// failAttempt ends the job in a terminal phase without a continuation: it settles the booked
// attempt from what is known, releases the booking and writes the phase. Used for a failed runner
// and for cancel.
func (r *HPCArgoWorkflowJobReconciler) failAttempt(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob,
	site *hpcv1alpha1.HPCSite, phase hpcv1alpha1.Phase, reason, outcome, msg string) error {
	orig := job.DeepCopy()
	if b := job.Status.Booking; b != nil {
		used := r.usedMinutes(job)
		rec := hpcv1alpha1.AttemptRecord{Attempt: max(job.Status.Attempt, 1), Tier: b.Tier, Outcome: outcome,
			SettledNodeMinutes: used, EndTime: metav1.NewTime(r.now())}
		if run := job.Status.Run; run != nil && current(run, job.Status.Attempt) {
			rec.SiteJobID, rec.ExitCode = run.SiteJobID, run.ExitCode
			rec.QueueReplaced, rec.QueueReplacedCount = run.QueueReplaced, run.QueueReplacedCount
		}
		job.Status.Attempts = append(job.Status.Attempts, rec)
		observeSettled(site, b.Tier, used, job.Status.Run, &b.At)
	}
	job.Status.Booking = nil
	job.Status.Sweep, job.Status.SiteMissingSince = nil, nil
	job.Status.Phase, job.Status.Reason, job.Status.Message = phase, reason, msg
	keepResultOnlyOnSuccess(job)
	var total int64
	for _, a := range job.Status.Attempts {
		total += a.SettledNodeMinutes
	}
	job.Status.SettledNodeMinutes = &total
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	r.forget(string(job.UID))
	r.noteEnded(string(job.UID))
	return nil
}

// observeSettled feeds the settlement metrics of one ended attempt.
func observeSettled(site *hpcv1alpha1.HPCSite, tier hpcv1alpha1.Tier, used int64, run *hpcv1alpha1.RunStatus, admittedAt *metav1.Time) {
	metrics.NodeMinutesSettled.WithLabelValues(string(tier), projectOf(site)).Add(float64(used))
	if run == nil || !validSiteTime(run.StartTime, time.Now()) {
		return
	}
	if admittedAt != nil {
		metrics.JobQueueSeconds.WithLabelValues(string(tier)).Observe(max(run.StartTime.Sub(admittedAt.Time).Seconds(), 0))
	}
	if validSiteTime(run.EndTime, time.Now()) {
		metrics.JobRunSeconds.WithLabelValues(string(tier)).Observe(max(run.EndTime.Sub(run.StartTime.Time).Seconds(), 0))
	}
}

// projectOf is the KAOS project an HPCSite belongs to ("" when unknown).
func projectOf(site *hpcv1alpha1.HPCSite) string {
	if site == nil {
		return ""
	}
	return site.Labels[hpcv1alpha1.ProjectLabel]
}

// observePhase publishes the job's phase as kaos_hpc_job_phase.
func (r *HPCArgoWorkflowJobReconciler) observePhase(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) {
	phase := string(job.Status.Phase)
	if phase == "" {
		phase = string(hpcv1alpha1.PhasePending)
	}
	tier := ""
	if site != nil {
		if c, ok := admission.ClassOf(site, job.Spec.Class); ok {
			tier = string(c.Tier)
		}
	}
	metrics.SetJobPhase(job.Namespace, job.Name, job.Spec.Class, tier, phase)
	observeGPU(job)
}

// observeGPU publishes the GPU utilisation the runner reported (kaos_hpc_job_gpu_utilization_percent,
// AC-69) while this attempt's site job runs, and drops the series otherwise: a job that ended, a
// queued attempt, or a report of an earlier attempt must not leave a stale reading behind.
func observeGPU(job *hpcv1alpha1.HPCArgoWorkflowJob) {
	run := job.Status.Run
	if run == nil || job.Status.Phase.Terminal() || !current(run, job.Status.Attempt) ||
		run.State != "RUNNING" || len(run.GPUUtilization) == 0 {
		metrics.ForgetJobGPU(job.Namespace, job.Name)
		return
	}
	workflow := ""
	if job.Spec.Workflow != nil {
		workflow = job.Spec.Workflow.Name
	}
	readings := make([]metrics.GPUReading, 0, len(run.GPUUtilization))
	for _, g := range run.GPUUtilization {
		readings = append(readings, metrics.GPUReading{GPU: g.GPU, Percent: g.Percent, MaxPercent: g.MaxPercent})
	}
	metrics.SetJobGPU(job.Namespace, workflow, job.Spec.StepName, job.Name, run.SiteJobID, readings)
}
