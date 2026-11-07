package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

var recentT0 = time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)

func recentSite() *hpcv1alpha1.HPCSite {
	return &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: "p-hpc", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "p"}},
		Spec: hpcv1alpha1.HPCSiteSpec{Profile: hpcv1alpha1.SiteProfile{
			Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu"}}}},
	}
}

func recentJob(name string, age time.Duration, phase hpcv1alpha1.Phase) hpcv1alpha1.HPCArgoWorkflowJob {
	return hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "p-train", CreationTimestamp: metav1.NewTime(recentT0.Add(-age))},
		Spec:       hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", WallTimeMinutes: 60, StepName: name},
		Status:     hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: phase},
	}
}

func recentReconciler(t *testing.T, nss ...*corev1.Namespace) *HPCArgoWorkflowJobReconciler {
	t.Helper()
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	b := fake.NewClientBuilder().WithScheme(sch)
	for _, ns := range nss {
		b = b.WithObjects(ns)
	}
	return &HPCArgoWorkflowJobReconciler{Client: b.Build()}
}

func projectNS(name, project string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}}
}

func names(l []hpcv1alpha1.RecentJob) []string {
	out := make([]string, 0, len(l))
	for _, j := range l {
		out = append(out, j.Name)
	}
	return out
}

func TestRecentJobsOrderAndScope(t *testing.T) {
	r := recentReconciler(t, projectNS("p-train", "p"), projectNS("q-train", "q"))
	site := recentSite()
	done := recentJob("done-new", time.Minute, hpcv1alpha1.PhaseSucceeded)
	oldActive := recentJob("active-old", time.Hour, hpcv1alpha1.PhaseRunning)
	oldActive.Status.Booking = &hpcv1alpha1.Booking{SiteName: "meluxina", SiteNamespace: "p-hpc", NodeMinutes: 60}
	waiting := recentJob("waiting-new", 2*time.Minute, hpcv1alpha1.PhaseWaiting) // never admitted: no site yet
	other := recentJob("other-project", time.Minute, hpcv1alpha1.PhaseWaiting)
	other.Namespace = "q-train"
	otherClass := recentJob("other-class", time.Minute, hpcv1alpha1.PhaseWaiting)
	otherClass.Spec.Class = "unknown"
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{done, oldActive, waiting, other, otherClass}

	got, err := r.recentJobsOf(context.Background(), site, jobs)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"waiting-new", "active-old", "done-new"} // active first, newest first within a group
	if fmt.Sprint(names(got)) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", names(got), want)
	}
}

func TestRecentJobsAreBoundedAndDeterministic(t *testing.T) {
	r := recentReconciler(t, projectNS("p-train", "p"))
	jobs := make([]hpcv1alpha1.HPCArgoWorkflowJob, 0, 80)
	for i := range 80 {
		jobs = append(jobs, recentJob(fmt.Sprintf("j%02d", i), time.Duration(i)*time.Minute, hpcv1alpha1.PhaseSucceeded))
	}
	a, err := r.recentJobsOf(context.Background(), recentSite(), jobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != hpcv1alpha1.RecentJobsMax || a[0].Name != "j00" {
		t.Fatalf("got %d jobs, first %q", len(a), a[0].Name)
	}
	rev := make([]hpcv1alpha1.HPCArgoWorkflowJob, len(jobs))
	for i := range jobs {
		rev[len(jobs)-1-i] = jobs[i]
	}
	b, _ := r.recentJobsOf(context.Background(), recentSite(), rev)
	if fmt.Sprint(names(a)) != fmt.Sprint(names(b)) {
		t.Fatal("the list depends on the order the cache lists the jobs in")
	}
}

func TestRecentJobsEmptyIsNil(t *testing.T) {
	got, err := recentReconciler(t).recentJobsOf(context.Background(), recentSite(), nil)
	if err != nil || got != nil {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestRecentJobOfCopiesLaneLimitAndSpend(t *testing.T) {
	start, end := metav1.NewTime(recentT0), metav1.NewTime(recentT0.Add(40*time.Minute))
	j := recentJob("train", time.Hour, hpcv1alpha1.PhaseFailed)
	j.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: "wf-1", UID: "u"}
	j.Status.Reason = hpcv1alpha1.ReasonIdle
	j.Status.Attempt = 2
	j.Status.Placement = &hpcv1alpha1.Placement{TimeLimitMinutes: 45, Source: hpcv1alpha1.PlacementLearned, Samples: 4, QOS: "dev", Reservation: "gpudev", RefusedQOS: "test"}
	j.Status.Run = &hpcv1alpha1.RunStatus{SiteJobID: "77", State: "FAILED", StartTime: &start, EndTime: &end, QOS: "default"}
	j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{SettledNodeMinutes: 30}, {SettledNodeMinutes: 12}}

	rj := recentJobOf(&j)
	if rj.Workflow != "wf-1" || rj.Step != "train" || rj.Reason != "Idle" || rj.Attempt != 2 {
		t.Fatalf("identity fields: %+v", rj)
	}
	if rj.TimeLimitMinutes != 45 || rj.LimitSource != hpcv1alpha1.PlacementLearned || rj.LimitSamples != 4 || rj.IdleTimeoutMinutes != 20 {
		t.Fatalf("limit fields: %+v", rj)
	}
	// The lane the site job was submitted with wins over the placement's (the runner may fall back).
	if rj.QOS != "default" || rj.RefusedQOS != "test" || rj.SiteJobID != "77" {
		t.Fatalf("lane fields: %+v", rj)
	}
	if rj.BookedNodeMinutes != 45 || rj.SettledNodeMinutes != 42 || rj.PendingReason != "" {
		t.Fatalf("spend fields: %+v", rj)
	}
	if rj.StartTime == nil || !rj.StartTime.Equal(&start) || rj.EndTime == nil {
		t.Fatalf("times: %+v", rj)
	}
}

func TestRecentJobOfPendingReasonOnlyWhileQueued(t *testing.T) {
	j := recentJob("q", time.Minute, hpcv1alpha1.PhaseRunning)
	j.Status.PendingReason = "Resources"
	j.Status.SiteObservation = &hpcv1alpha1.SiteObservation{JobID: "1", State: "PENDING", PendingReason: "Priority"}
	j.Status.Run = &hpcv1alpha1.RunStatus{SiteJobID: "1", State: "PENDING"}
	if got := recentJobOf(&j).PendingReason; got != "Priority" {
		t.Fatalf("queued job: got %q, want the controller's reading", got)
	}
	j.Status.Run.State = "RUNNING"
	if got := recentJobOf(&j).PendingReason; got != "" {
		t.Fatalf("running job: got %q, want none", got)
	}
}
