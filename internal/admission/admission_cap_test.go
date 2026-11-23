package admission

import (
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// The spec's wall time is the platform's cap on the step's total time (D-75). A cap above the time of one job
// (the class's, the site's or the data access's maximum) is the normal case: it is never refused, whether or not
// the step saves progress, and the attempt books what it will run with, not the cap.
func TestDecideAdmitsACapAboveTheTimeOfOneJob(t *testing.T) {
	const total = 10080
	cases := []struct {
		name string
		site *hpcv1alpha1.HPCSite
		mod  func(*hpcv1alpha1.HPCArgoWorkflowJob)
		wall int32
	}{
		{"should_admit_a_cap_above_the_longest_lane", laneMaxSite(600), nil, 600},
		{"should_admit_a_cap_above_the_site_maximum",
			site(func(s *hpcv1alpha1.HPCSite) {
				s.Spec.Limits.MaxWallTimeMinutes = 360
				s.Spec.Budget.GPUNodeHours = 1000
			}), nil, 360},
		{"should_admit_a_cap_above_the_data_access_of_one_job", laneMaxSite(2880), nil, timelimit.TokenCoverMinutes},
		{"should_admit_a_step_named_in_serve_mode", laneMaxSite(2880),
			func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.StepName = "train" }, timelimit.TokenCoverMinutes},
		{"should_admit_a_class_with_no_known_maximum", laneMaxSite(0), nil, 600},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := job("meluxina-gpu", total, 1)
			if c.mod != nil {
				c.mod(j)
			}
			d := DecideFor(j, c.site, led(c.site, 0, 0, 0), c.wall)
			if d.Phase != hpcv1alpha1.PhaseAdmitted || d.Reason != hpcv1alpha1.ReasonAdmitted || d.Cost != int64(c.wall) {
				t.Fatalf("got %s/%s cost %d (%s), want admitted at %d", d.Phase, d.Reason, d.Cost, d.Message, c.wall)
			}
		})
	}
}

// laneMaxSite is a site whose classes all have the given longest lane.
func laneMaxSite(laneMax int32) *hpcv1alpha1.HPCSite {
	return site(func(s *hpcv1alpha1.HPCSite) {
		s.Spec.Budget.GPUNodeHours = 1000
		for i := range s.Spec.Profile.Classes {
			s.Spec.Profile.Classes[i].MaxWallTimeMinutes = laneMax
		}
	})
}
