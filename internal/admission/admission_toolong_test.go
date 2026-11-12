package admission

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// A declared limit above the longest lane (PRD-HPC-1461 D-67, D-74): a step that is not resumable is
// refused with numbers and the fix; a resumable one is admitted up to (continuation cap + 1) runs.
func TestDecideStepLongerThanTheLongestLane(t *testing.T) {
	const laneMax = 2880
	resumable := func(caps int32) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
		return func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: caps}
		}
	}
	verified := func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.Placement = &hpcv1alpha1.Placement{TimeLimitMinutes: laneMax, Source: hpcv1alpha1.PlacementDeclared,
			Resumable: &hpcv1alpha1.ResumableStatus{Verified: true, Since: &metav1.Time{}}}
	}
	cases := []struct {
		name   string
		wall   int32
		mod    func(*hpcv1alpha1.HPCArgoWorkflowJob)
		phase  hpcv1alpha1.Phase
		reason string
		in     []string
	}{
		{"should_refuse_too_long_not_resumable_with_numbers_and_both_fixes", 4320, nil,
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonTooLongNotResumable,
			[]string{"3 days (4320 min)", "2 days (2880 min)", "not resumable", "resumable=True", "to 2880 minutes or less"}},
		{"should_refuse_one_minute_over_the_lane_when_not_resumable", laneMax + 1, nil,
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonTooLongNotResumable, nil},
		{"should_admit_exactly_the_lane_maximum_without_resumable", laneMax, nil,
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, nil},
		{"should_refuse_when_resumable_is_opted_out_even_if_verified", 4320,
			func(j *hpcv1alpha1.HPCArgoWorkflowJob) { verified(j); j.Spec.ResumableOptOut = true },
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonTooLongNotResumable, nil},
		{"should_admit_a_declared_resumable_step_longer_than_the_lane", 4320, resumable(5),
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, nil},
		{"should_admit_a_verified_resumable_step_longer_than_the_lane", 4320, verified,
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, nil},
		{"should_admit_resumable_up_to_the_cap_times_the_lane", 6 * laneMax, resumable(5),
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, nil},
		{"should_refuse_resumable_beyond_the_continuation_cap", 6*laneMax + 1, resumable(5),
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonTooLongBeyondContinuations,
			[]string{"5 continuations", "6 runs", "17280 min", "maxContinuations"}},
		{"should_use_the_declared_cap_of_the_step", 3*laneMax + 1, resumable(2),
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonTooLongBeyondContinuations, nil},
		{"should_use_the_default_cap_for_a_verified_step", 6*laneMax + 1, verified,
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonTooLongBeyondContinuations, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Budget.GPUNodeHours = 1000 })
			j := job("meluxina-gpu", c.wall, 1)
			if c.mod != nil {
				c.mod(j)
			}
			// The attempt books at most the lane maximum, as the controller caps it (placementFor).
			d := DecideFor(j, s, led(s, 0, 0, 0), min(c.wall, laneMax))
			if d.Phase != c.phase || d.Reason != c.reason {
				t.Fatalf("got %s/%s (%s), want %s/%s", d.Phase, d.Reason, d.Message, c.phase, c.reason)
			}
			for _, w := range c.in {
				if !strings.Contains(d.Message, w) {
					t.Errorf("message %q lacks %q", d.Message, w)
				}
			}
			// Idempotent: the same input judges the same.
			if again := DecideFor(j, s, led(s, 0, 0, 0), min(c.wall, laneMax)); again != d {
				t.Errorf("second decision differs: %+v vs %+v", again, d)
			}
		})
	}
}

func TestDecideLongStepUsesTheLowestOfClassAndProjectLimit(t *testing.T) {
	s := site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Limits.MaxWallTimeMinutes = 360 })
	d := DecideFor(job("meluxina-gpu", 720, 1), s, led(s, 0, 0, 0), 360)
	if d.Reason != hpcv1alpha1.ReasonTooLongNotResumable || !strings.Contains(d.Message, "6 h (360 min)") {
		t.Fatalf("got %s: %s", d.Reason, d.Message)
	}
}
