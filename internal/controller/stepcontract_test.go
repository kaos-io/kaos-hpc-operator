package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// contractFixture is a fixture of the step contract (docs/step-contract/fixtures): loaded, never copied.
func contractFixture(name string) string {
	raw, err := os.ReadFile(filepath.FromSlash("../../docs/step-contract/fixtures/" + name))
	if err != nil {
		panic("the step contract's fixtures are part of this repository: " + err.Error())
	}
	return string(raw)
}

// A job without spec.context cannot be created under the definition (it is required). One can still be read:
// a job stored before the definition changed comes back without the fields that were removed (contract check
// 5). The fake client stands in for that read; the definition itself is exercised in the envtest suite.
func contextlessWorld(t *testing.T, mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) (*HPCArgoWorkflowJobReconciler, client.Client, *hpcv1alpha1.HPCArgoWorkflowJob) {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	ns := func(name string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{hpcv1alpha1.ProjectLabel: "vision"}}}
	}
	site := &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: "ml-vision", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "vision"}},
		Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
			Profile: hpcv1alpha1.SiteProfile{Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44", ScratchRoot: "/scratch/p1",
				Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default", Tier: hpcv1alpha1.TierGPU,
					GPUs: 4, MaxWallTimeMinutes: 2880}}},
			Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1"}, Budget: hpcv1alpha1.Budget{GPUNodeHours: 100}},
	}
	// As a follower from before the step contract created it, read back under the new definition: no context,
	// no inputs, and nothing left of spec.dataset.
	job := &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-old", Namespace: "ml-vision-env", UID: "old-uid"},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "reg.example/app:v1", WallTimeMinutes: 60,
			Command: []string{"python", "-m", "app.entry"}, StepName: "train"},
	}
	if mod != nil {
		mod(job)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns("ml-vision-env"), ns("ml-vision"), site, job).
		WithStatusSubresource(&hpcv1alpha1.HPCArgoWorkflowJob{}, &hpcv1alpha1.HPCSite{}).Build()
	return &HPCArgoWorkflowJobReconciler{Client: c, Scheme: s, RunnerImage: "kaos/hpc-operator:test"}, c, job
}

func reconcileJob(t *testing.T, r *HPCArgoWorkflowJobReconciler, c client.Client, job *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}); err != nil {
		t.Fatal(err)
	}
	got := &hpcv1alpha1.HPCArgoWorkflowJob{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func runnerJobs(t *testing.T, c client.Client) int {
	t.Helper()
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs); err != nil {
		t.Fatal(err)
	}
	return len(jobs.Items)
}

// PRD kaos#1911 T-04: an old follower's job, or one stored before the definition changed, is refused with its
// own reason. It is never booked and never run without its data.
func TestAJobWithoutAStepContextIsRefusedBeforeAnythingIsBooked(t *testing.T) {
	for name, mod := range map[string]func(*hpcv1alpha1.HPCArgoWorkflowJob){
		"should_refuse_a_serve_mode_job":              nil,
		"should_refuse_a_job_without_a_step_name_too": func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.StepName = "" },
	} {
		t.Run(name, func(t *testing.T) {
			r, c, job := contextlessWorld(t, mod)
			got := reconcileJob(t, r, c, job)
			if got.Status.Phase != hpcv1alpha1.PhaseRefused || got.Status.Reason != hpcv1alpha1.ReasonNoContext {
				t.Fatalf("phase %s reason %s, want Refused/NoContext", got.Status.Phase, got.Status.Reason)
			}
			if !strings.Contains(got.Status.Message, "no step context") || !strings.Contains(got.Status.Message, "Run the pipeline again") {
				t.Errorf("the message must name the cause and what to do: %q", got.Status.Message)
			}
			if got.Status.Booking != nil || runnerJobs(t, c) != 0 {
				t.Errorf("nothing may be booked or started: booking %v, %d runner Jobs", got.Status.Booking, runnerJobs(t, c))
			}
		})
	}
}

func booked(j *hpcv1alpha1.HPCArgoWorkflowJob) {
	j.Finalizers = []string{hpcv1alpha1.CancelFinalizer}
	j.Status = hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseAdmitted, Reason: hpcv1alpha1.ReasonAdmitted, Attempt: 1,
		Site: &hpcv1alpha1.SiteRef{Name: "meluxina", Namespace: "ml-vision"},
		Booking: &hpcv1alpha1.Booking{Site: "meluxina", SiteName: "meluxina", SiteNamespace: "ml-vision", Tier: hpcv1alpha1.TierGPU,
			NodeMinutes: 60, At: metav1.Now()}}
}

// A job that was already booked when the definition changed (the cutover drains HPC jobs first; this is the
// belt) never gets a runner for an attempt without its context.
func TestABookedJobWithoutAStepContextGetsNoRunner(t *testing.T) {
	r, c, job := contextlessWorld(t, booked)
	got := reconcileJob(t, r, c, job)
	if got.Status.Phase != hpcv1alpha1.PhaseFailed || got.Status.Reason != hpcv1alpha1.ReasonNoContext {
		t.Fatalf("phase %s reason %s (%s), want Failed/NoContext", got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
	if got.Status.Booking != nil || runnerJobs(t, c) != 0 {
		t.Errorf("the booking must be released and no runner started: booking %v, %d runner Jobs", got.Status.Booking, runnerJobs(t, c))
	}
}

// And one whose runner of before the update still reports a good end is not a success: what it handed back
// (the removed status.outputs) is gone, so the next step would get nothing.
func TestAJobWithoutAStepContextThatRanToItsEndIsNotASuccess(t *testing.T) {
	now := metav1.Now()
	r, c, job := contextlessWorld(t, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		booked(j)
		j.Status.Phase = hpcv1alpha1.PhaseRunning
		j.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "7", State: "COMPLETED", ExitCode: ptr.To(int32(0)),
			StartTime: &now, EndTime: &now, Terminal: true}
	})
	got := reconcileJob(t, r, c, job)
	if got.Status.Phase != hpcv1alpha1.PhaseFailed || got.Status.Reason != hpcv1alpha1.ReasonNoContext {
		t.Fatalf("phase %s reason %s (%s), want Failed/NoContext", got.Status.Phase, got.Status.Reason, got.Status.Message)
	}
	if got.Status.Result != "" {
		t.Errorf("no result may be published: %q", got.Status.Result)
	}
}
