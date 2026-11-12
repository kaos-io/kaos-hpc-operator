package follower

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Exit codes of the follower: the step succeeds only when the job did.
const (
	ExitSucceeded = 0
	ExitFailed    = 1
)

// Defaults for Options.
const (
	DefaultPoll    = 5 * time.Second
	DefaultRecheck = time.Minute
)

// Options are the follower's side effects, replaceable in tests.
type Options struct {
	// Poll is the wait between attempts after an API error or when watching is unavailable.
	Poll time.Duration
	// Recheck is the longest a watch is held before the job is read again.
	Recheck time.Duration
	// ResultPath receives the step's result when the job succeeded (the workflow reads it as the step's
	// kaos-result output); SlurmIDPath receives the site job id.
	ResultPath  string
	SlurmIDPath string
	// TerminationPath receives the follower's last line when the step fails: the workflow shows a pod's
	// termination message as the step's message, so the cause is named on the step, not only in its log.
	TerminationPath string
	Out             io.Writer
	Now             func() time.Time
}

// Run creates or re-attaches to the step's job, follows it to its end and reports the result as an
// exit code. API errors never end the step: it keeps trying until the job ends, the context is
// cancelled (Argo stopped the pod) or the request itself is rejected.
func Run(ctx context.Context, c client.WithWatch, cfg Config, o Options) int {
	f := &follower{c: c, cfg: cfg, o: o, key: types.NamespacedName{Namespace: cfg.Namespace, Name: JobName(cfg.WorkflowUID, cfg.NodeKey)}}
	if f.o.Now == nil {
		f.o.Now = time.Now
	}
	code := ExitFailed
	if err := f.attach(ctx); err != nil {
		f.say("cannot start the job: %v", err)
	} else {
		code = f.follow(ctx)
	}
	if code != ExitSucceeded && f.o.TerminationPath != "" {
		_ = os.WriteFile(f.o.TerminationPath, []byte(f.lastSaid), 0o644)
	}
	return code
}

type follower struct {
	c    client.WithWatch
	cfg  Config
	o    Options
	key  types.NamespacedName
	last view
	// lastPlacement is the last placement text printed (see showPlacement).
	lastPlacement string
	// shownStaging are the dataset staging lines already printed (see showStaging).
	shownStaging map[string]bool
	// lastWarning is the time-limit warning already printed.
	lastWarning string
	// lastSaid is the last line said: on a failure, why.
	lastSaid string
}

// view is what a change of is printed for.
type view struct {
	phase, reason, message, pending, siteJob string
}

func (f *follower) say(format string, args ...any) {
	f.lastSaid = fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintf(f.o.Out, "%s %s\n", f.o.Now().UTC().Format(time.RFC3339), f.lastSaid)
}

// attach creates the job, or finds the one an earlier pod of this step created. The same name for
// every retry of the step is what makes a re-created pod re-attach instead of submitting twice.
func (f *follower) attach(ctx context.Context) error {
	for {
		// Strict: a cluster whose job definition does not know a field this follower sets (one from before the
		// step contract, without spec.context) refuses the create instead of dropping the field, which would
		// run the step without its context and its data. The stored object is checked as well, for an API
		// server that does not validate.
		job := f.cfg.Job()
		err := f.c.Create(ctx, job, client.FieldValidation("Strict"))
		if err == nil && (job.Spec.Context != f.cfg.Context || !equality.Semantic.DeepEqual(job.Spec.Inputs, f.cfg.Inputs)) {
			// The job exists without its context: it must not be run. The Workflow owns it, so it would go
			// with the Workflow anyway; removing it now keeps the controller from starting it meanwhile.
			if derr := f.c.Delete(ctx, job); derr != nil && !apierrors.IsNotFound(derr) {
				f.say("could not remove %s: %v", f.key, derr)
			}
			return &droppedError{msg: fmt.Sprintf("the cluster stored %s without its step context or its inputs: the "+
				"platform's HPC controller is older than this pipeline. Run it again once the platform update is complete",
				f.key)}
		}
		if err == nil {
			f.say("created %s", f.key)
			return nil
		}
		if apierrors.IsAlreadyExists(err) {
			err = f.checkExisting(ctx)
			if err == nil {
				f.say("re-attached to %s", f.key)
				return nil
			}
		}
		if permanent(err) {
			return err
		}
		f.say("API error, retrying: %v", err)
		if !f.sleep(ctx, f.o.Poll) {
			return ctx.Err()
		}
	}
}

// checkExisting fails on a job of this name whose spec is not this step's: the name collided
// or the step changed under a running workflow, and following the other job would be wrong.
// Both specs are compared with the CRD defaults applied (see withCRDDefaults): comparing the
// server-defaulted stored spec with a raw one refused every re-attach of a step that sets no
// idle_timeout (2026-11-08, PRD-HPC-1461 AC-02).
func (f *follower) checkExisting(ctx context.Context) error {
	var got hpcv1alpha1.HPCArgoWorkflowJob
	if err := f.c.Get(ctx, f.key, &got); err != nil {
		return err
	}
	// The stored spec carries the API server's CRD defaults; this pod's freshly built one does not.
	if !equality.Semantic.DeepEqual(withCRDDefaults(got.Spec), withCRDDefaults(f.cfg.Job().Spec)) {
		return &mismatchError{name: f.key.String()}
	}
	return nil
}

type mismatchError struct{ name string }

func (e *mismatchError) Error() string {
	return "job " + e.name + " exists with a different spec; refusing to follow it"
}

// droppedError: the cluster stored the job without something this follower set. Trying again cannot help.
type droppedError struct{ msg string }

func (e *droppedError) Error() string { return e.msg }

// permanent reports whether an error will not go away by trying again.
func permanent(err error) bool {
	var m *mismatchError
	return errors.As(err, &m) || apierrors.IsInvalid(err) || apierrors.IsForbidden(err) ||
		apierrors.IsUnauthorized(err) || apierrors.IsBadRequest(err) || errors.Is(err, context.Canceled)
}

// follow prints the job's changes until it ends, then reports.
func (f *follower) follow(ctx context.Context) int {
	for {
		var job hpcv1alpha1.HPCArgoWorkflowJob
		err := f.c.Get(ctx, f.key, &job)
		switch {
		case ctx.Err() != nil:
			f.say("stopped; the job keeps its place under the controller, which cancels it with the workflow")
			return ExitFailed
		case apierrors.IsNotFound(err):
			f.say("the job %s is gone (deleted or cancelled)", f.key)
			return ExitFailed
		case err != nil:
			f.say("API error, retrying: %v", err)
			f.sleep(ctx, f.o.Poll)
			continue
		}
		f.show(&job)
		if job.Status.Phase.Terminal() {
			return f.finish(&job)
		}
		f.wait(ctx, job.ResourceVersion)
	}
}

// show prints the job's status when it changed since the last print.
func (f *follower) show(job *hpcv1alpha1.HPCArgoWorkflowJob) {
	v := view{phase: string(job.Status.Phase), reason: job.Status.Reason, message: job.Status.Message,
		pending: job.Status.PendingReason}
	if job.Status.Run != nil {
		v.siteJob = job.Status.Run.SiteJobID
	}
	defer f.showStaging(job)
	defer f.showPlacement(job)
	if v == f.last {
		return
	}
	if v.siteJob != f.last.siteJob {
		f.say("site job id: %s", v.siteJob)
	}
	if v.phase != f.last.phase || v.reason != f.last.reason || v.message != f.last.message || v.pending != f.last.pending {
		line := fmt.Sprintf("phase=%s reason=%s", orDash(v.phase), orDash(v.reason))
		if v.pending != "" {
			line += fmt.Sprintf(" pending=%q", v.pending)
		}
		if v.message != "" {
			line += fmt.Sprintf(" message=%q", v.message)
		}
		f.say("%s", line)
	}
	f.last = v
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// finish maps the phase to the exit code. Only a job that succeeded hands a result on: its status.result, which
// the runner checked. A job that ended any other way writes no result, so the workflow can never take one from
// it. A job that succeeded without a result, or a result that cannot be written for the workflow, fails the
// step, naming why: nothing stands in for a missing result (PRD kaos#1911 CON-04).
func (f *follower) finish(job *hpcv1alpha1.HPCArgoWorkflowJob) int {
	if job.Status.Phase != hpcv1alpha1.PhaseSucceeded {
		f.say("the job ended %s (%s): %s", job.Status.Phase, job.Status.Reason, job.Status.Message)
		return ExitFailed
	}
	if job.Status.Result == "" {
		f.say("the job succeeded, but it carries no result (status.result): the step's result was not handed back")
		return ExitFailed
	}
	siteJob := ""
	if job.Status.Run != nil {
		siteJob = job.Status.Run.SiteJobID
	}
	for path, body := range map[string]string{f.o.ResultPath: job.Status.Result, f.o.SlurmIDPath: siteJob} {
		err := os.MkdirAll(filepath.Dir(path), 0o755)
		if err == nil {
			err = os.WriteFile(path, []byte(body), 0o644)
		}
		if err != nil {
			f.say("the job succeeded, but its result cannot be handed on to the workflow: %v", err)
			return ExitFailed
		}
	}
	f.say("the job succeeded")
	return ExitSucceeded
}

// wait blocks until the job changes, Recheck passes or the context ends. It watches the one job;
// when watching fails it just waits Poll, and the caller reads the job again either way.
func (f *follower) wait(ctx context.Context, rv string) {
	recheck := f.o.Recheck
	if recheck <= 0 {
		recheck = DefaultRecheck
	}
	w, err := f.c.Watch(ctx, &hpcv1alpha1.HPCArgoWorkflowJobList{}, &client.ListOptions{
		Namespace:     f.key.Namespace,
		FieldSelector: fields.OneTermEqualSelector("metadata.name", f.key.Name),
		Raw:           &metav1.ListOptions{ResourceVersion: rv},
	})
	if err != nil {
		f.sleep(ctx, f.o.Poll)
		return
	}
	defer w.Stop()
	t := time.NewTimer(recheck)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-w.ResultChan():
	case <-t.C:
	}
}

// sleep waits d (DefaultPoll when zero) and reports false if the context ended first.
func (f *follower) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = DefaultPoll
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
