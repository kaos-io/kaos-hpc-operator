package follower

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	// OutputsDir receives <name>.json per output; SlurmIDPath receives the site job id.
	OutputsDir  string
	SlurmIDPath string
	Out         io.Writer
	Now         func() time.Time
}

// Run creates or re-attaches to the step's job, follows it to its end and reports the result as an
// exit code. API errors never end the step: it keeps trying until the job ends, the context is
// cancelled (Argo stopped the pod) or the request itself is rejected.
func Run(ctx context.Context, c client.WithWatch, cfg Config, o Options) int {
	f := &follower{c: c, cfg: cfg, o: o, key: types.NamespacedName{Namespace: cfg.Namespace, Name: JobName(cfg.WorkflowUID, cfg.NodeKey)}}
	if f.o.Now == nil {
		f.o.Now = time.Now
	}
	if err := f.attach(ctx); err != nil {
		f.say("cannot start the job: %v", err)
		return ExitFailed
	}
	return f.follow(ctx)
}

type follower struct {
	c    client.WithWatch
	cfg  Config
	o    Options
	key  types.NamespacedName
	last view
}

// view is what a change of is printed for.
type view struct {
	phase, reason, message, pending, siteJob string
}

func (f *follower) say(format string, args ...any) {
	_, _ = fmt.Fprintf(f.o.Out, "%s %s\n", f.o.Now().UTC().Format(time.RFC3339), fmt.Sprintf(format, args...))
}

// attach creates the job, or finds the one an earlier pod of this step created. The same name for
// every retry of the step is what makes a re-created pod re-attach instead of submitting twice.
func (f *follower) attach(ctx context.Context) error {
	for {
		err := f.c.Create(ctx, f.cfg.Job())
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
func (f *follower) checkExisting(ctx context.Context) error {
	var got hpcv1alpha1.HPCArgoWorkflowJob
	if err := f.c.Get(ctx, f.key, &got); err != nil {
		return err
	}
	if !equality.Semantic.DeepEqual(got.Spec, f.cfg.Job().Spec) {
		return &mismatchError{name: f.key.String()}
	}
	return nil
}

type mismatchError struct{ name string }

func (e *mismatchError) Error() string {
	return "job " + e.name + " exists with a different spec; refusing to follow it"
}

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
			f.writeOutputs(nil)
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

// finish writes the Argo outputs and the site job id, and maps the phase to the exit code.
func (f *follower) finish(job *hpcv1alpha1.HPCArgoWorkflowJob) int {
	f.writeOutputs(job)
	if job.Status.Phase == hpcv1alpha1.PhaseSucceeded {
		f.say("the job succeeded")
		return ExitSucceeded
	}
	f.say("the job ended %s (%s): %s", job.Status.Phase, job.Status.Reason, job.Status.Message)
	return ExitFailed
}

// writeOutputs writes <OutputsDir>/<name>.json for every declared output (and any the runner
// filled), {} when missing, and the site job id. A nil job writes the empty defaults.
func (f *follower) writeOutputs(job *hpcv1alpha1.HPCArgoWorkflowJob) {
	out := map[string]string{}
	siteJob := ""
	if job != nil {
		for k, v := range job.Status.Outputs {
			out[k] = v
		}
		if job.Status.Run != nil {
			siteJob = job.Status.Run.SiteJobID
		}
	}
	for _, n := range f.cfg.Outputs {
		if _, ok := out[n]; !ok {
			out[n] = "{}"
		}
	}
	if err := os.MkdirAll(f.o.OutputsDir, 0o755); err != nil {
		f.say("cannot create %s: %v", f.o.OutputsDir, err)
	}
	for name, body := range out {
		if !safeName(name) {
			f.say("skipping output %q: not a plain name", name)
			continue
		}
		if body == "" {
			body = "{}"
		}
		if err := os.WriteFile(filepath.Join(f.o.OutputsDir, name+".json"), []byte(body), 0o644); err != nil {
			f.say("cannot write output %s: %v", name, err)
		}
	}
	if err := os.WriteFile(f.o.SlurmIDPath, []byte(siteJob), 0o644); err != nil {
		f.say("cannot write %s: %v", f.o.SlurmIDPath, err)
	}
}

func safeName(n string) bool {
	return n != "" && n != "." && n != ".." && !strings.ContainsAny(n, `/\`)
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
