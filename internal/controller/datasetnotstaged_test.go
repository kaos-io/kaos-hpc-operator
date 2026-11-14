package controller

import (
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func TestShouldEndADatasetNotStagedAttemptWithItsReasonBeforeStaging(t *testing.T) {
	if got := failedExitReasons[hpcv1alpha1.ExitCodeDatasetNotStaged]; got != hpcv1alpha1.ReasonDatasetNotStaged {
		t.Errorf("exit %d means %q, want %q", hpcv1alpha1.ExitCodeDatasetNotStaged, got, hpcv1alpha1.ReasonDatasetNotStaged)
	}
	if !refusedBeforeStaging(hpcv1alpha1.ReasonDatasetNotStaged) {
		t.Error("DatasetNotStaged is ended before staging or submission: it carries the runner's message and settles nothing")
	}
}
