package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

func gpuJob(phase hpcv1alpha1.Phase, state string, attempt int32) *hpcv1alpha1.HPCArgoWorkflowJob {
	return &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "job-1"},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{StepName: "train",
			Workflow: &hpcv1alpha1.WorkflowRef{Name: "wf-x", UID: "u"}},
		Status: hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: phase, Attempt: 1, Run: &hpcv1alpha1.RunStatus{
			Attempt: attempt, SiteJobID: "5433585", State: state,
			GPUUtilization: []hpcv1alpha1.GPUUtilization{{GPU: "0", Percent: 63, MaxPercent: 97}}}},
	}
}

func TestObserveGPU(t *testing.T) {
	cases := []struct {
		name  string
		job   *hpcv1alpha1.HPCArgoWorkflowJob
		count int
	}{
		{"should_export_the_reading_while_the_site_job_runs", gpuJob(hpcv1alpha1.PhaseRunning, "RUNNING", 1), 1},
		{"should_drop_the_series_when_the_job_is_terminal", gpuJob(hpcv1alpha1.PhaseSucceeded, "RUNNING", 1), 0},
		{"should_drop_the_series_when_the_site_job_is_not_running", gpuJob(hpcv1alpha1.PhaseRunning, "COMPLETED", 1), 0},
		{"should_ignore_a_report_of_an_earlier_attempt", gpuJob(hpcv1alpha1.PhaseRunning, "RUNNING", 2), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer metrics.ForgetJob("ns", "job-1")
			observeGPU(gpuJob(hpcv1alpha1.PhaseRunning, "RUNNING", 1)) // a series from before
			observeGPU(c.job)
			if got := testutil.CollectAndCount(metrics.JobGPUUtilization); got != c.count {
				t.Fatalf("series = %d, want %d", got, c.count)
			}
			if c.count == 1 {
				v := testutil.ToFloat64(metrics.JobGPUUtilization.WithLabelValues("ns", "wf-x", "train", "job-1", "5433585", "0"))
				if v != 63 {
					t.Fatalf("value = %v, want 63", v)
				}
			}
		})
	}
}
