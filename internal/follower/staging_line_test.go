package follower

import (
	"bytes"
	"strings"
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func stagedJob(s *hpcv1alpha1.StagingStatus) *hpcv1alpha1.HPCArgoWorkflowJob {
	return jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning,
		Run: &hpcv1alpha1.RunStatus{State: "PENDING", Staging: s}})
}

func TestStagingLine(t *testing.T) {
	st := func(kind string, size int64, secs int32) *hpcv1alpha1.StagingStatus {
		return &hpcv1alpha1.StagingStatus{Repo: "dataset-poulaki", Commit: "c2c2c2c2c2c2", Kind: kind, Files: 3, Bytes: size, Seconds: secs}
	}
	cases := []struct {
		name string
		job  *hpcv1alpha1.HPCArgoWorkflowJob
		want string
	}{
		{"should_say_first_copy_with_size_and_seconds", stagedJob(st(hpcv1alpha1.StagingFirst, 1_400_000_000, 95)),
			"HPC: dataset dataset-poulaki@c2c2c2c2c2c2 staged (first copy), 1.4 GB, 95s"},
		{"should_say_delta", stagedJob(st(hpcv1alpha1.StagingDelta, 12_300_000, 4)),
			"HPC: dataset dataset-poulaki@c2c2c2c2c2c2 staged (delta), 12.3 MB, 4s"},
		{"should_say_already_cached_without_size", stagedJob(st(hpcv1alpha1.StagingCached, 0, 0)),
			"HPC: dataset dataset-poulaki@c2c2c2c2c2c2 staged (already cached)"},
		{"should_show_plain_bytes_below_a_kilobyte", stagedJob(st(hpcv1alpha1.StagingDelta, 512, 1)),
			"HPC: dataset dataset-poulaki@c2c2c2c2c2c2 staged (delta), 512 B, 1s"},
		{"should_print_nothing_before_the_runner_reports_it", stagedJob(nil), ""},
		{"should_print_nothing_without_a_run", jobWith(hpcv1alpha1.HPCArgoWorkflowJobStatus{}), ""},
		{"should_print_nothing_for_an_unknown_kind", stagedJob(st("other", 1, 1)), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stagingLine(tc.job); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShowStagingPrintsOneLineOnceAndNoPath(t *testing.T) {
	var out bytes.Buffer
	f := &follower{o: Options{Out: &out}}
	job := stagedJob(nil)
	f.showStaging(job) // unknown yet
	job.Status.Run.Staging = &hpcv1alpha1.StagingStatus{Repo: "ds", Commit: "abc", Kind: hpcv1alpha1.StagingFirst, Files: 2, Bytes: 2000, Seconds: 3}
	f.showStaging(job)
	f.showStaging(job) // same record: not again
	job.Status.Run.State = "RUNNING"
	f.showStaging(job)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 || lines[0] != "HPC: dataset ds@abc staged (first copy), 2.0 kB, 3s" {
		t.Fatalf("unexpected output: %q", lines)
	}
	if strings.Contains(out.String(), "/") {
		t.Fatalf("the line must hold no path: %q", out.String())
	}
}
