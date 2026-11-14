package timelimit

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func mins(m ...float64) []time.Duration {
	out := make([]time.Duration, len(m))
	for i, v := range m {
		out[i] = time.Duration(v * float64(time.Minute))
	}
	return out
}

func slow(n int, m float64) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(m * float64(time.Minute))
	}
	return out
}

func TestLearn(t *testing.T) {
	caps := Caps{Spec: 240, Class: 2880}
	cases := []struct {
		name    string
		samples []time.Duration
		caps    Caps
		want    int32
		source  hpcv1alpha1.PlacementSource
	}{
		{"fewer_than_3_samples_use_the_declared_limit", mins(3, 3), caps, 240, hpcv1alpha1.PlacementDeclared},
		{"no_samples_use_the_declared_limit", nil, caps, 240, hpcv1alpha1.PlacementDeclared},
		{"live_3_minute_step_gets_the_floor_of_15", mins(3, 3, 3), caps, 15, hpcv1alpha1.PlacementLearned},
		{"p95_x_1_5_rounds_up_to_a_multiple_of_15", mins(20, 20, 21), caps, 45, hpcv1alpha1.PlacementLearned}, // 21*1.5=31.5
		{"exact_multiple_does_not_round_up", mins(20, 20, 20), caps, 30, hpcv1alpha1.PlacementLearned},
		{"one_slow_run_of_20_is_beyond_the_p95", append(slow(18, 10), mins(100, 10)...), caps, 15, hpcv1alpha1.PlacementLearned},
		{"two_slow_runs_of_20_reach_the_p95", append(slow(18, 10), mins(70, 70)...), caps, 105, hpcv1alpha1.PlacementLearned},
		{"p95_of_3_is_the_slowest_run", mins(5, 5, 70), caps, 105, hpcv1alpha1.PlacementLearned},
		{"a_long_step_keeps_its_declared_limit", mins(100, 100, 100), caps, 240, hpcv1alpha1.PlacementDeclared},
		{"exactly_2h_is_still_learned", mins(80, 80, 80), caps, 120, hpcv1alpha1.PlacementLearned},
		{"multi_day_training_keeps_its_declared_limit", mins(600, 610, 620), Caps{Spec: 2880, Class: 2880}, 2880, hpcv1alpha1.PlacementDeclared},
		{"capped_by_the_spec_means_declared", mins(200, 200, 200), caps, 240, hpcv1alpha1.PlacementDeclared},
		{"capped_by_the_class_maximum", mins(70, 70, 70), Caps{Spec: 240, Class: 90}, 90, hpcv1alpha1.PlacementLearned},
		{"capped_by_the_site_maximum", mins(70, 70, 70), Caps{Spec: 240, Class: 2880, Site: 90}, 90, hpcv1alpha1.PlacementLearned},
		{"unknown_class_and_site_maxima_do_not_bound", mins(70, 70, 70), Caps{Spec: 240}, 105, hpcv1alpha1.PlacementLearned},
		{"spec_below_the_floor_stays_the_cap", mins(1, 1, 1), Caps{Spec: 10, Class: 2880}, 10, hpcv1alpha1.PlacementDeclared},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, src := Learn(c.samples, c.caps)
			if got != c.want || src != c.source {
				t.Fatalf("got %d %s, want %d %s", got, src, c.want, c.source)
			}
		})
	}
}

func TestRetry(t *testing.T) {
	cases := []struct {
		name string
		caps Caps
		want int32
	}{
		{"goes_straight_to_the_declared_limit", Caps{Spec: 240, Class: 2880}, 240},
		{"capped_by_the_class", Caps{Spec: 240, Class: 50}, 50},
		{"capped_by_the_site", Caps{Spec: 240, Class: 2880, Site: 40}, 40},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Retry(c.caps); got != c.want {
				t.Fatalf("got %d want %d", got, c.want)
			}
		})
	}
}

func TestImageRepo(t *testing.T) {
	cases := map[string]string{
		"reg.example/proj/app:v0.0.0":         "reg.example/proj/app",
		"reg.example:5000/proj/app:abc":       "reg.example:5000/proj/app",
		"reg.example:5000/proj/app":           "reg.example:5000/proj/app",
		"reg.example/proj/app@sha256:0123":    "reg.example/proj/app",
		"reg.example/proj/app:v1@sha256:0123": "reg.example/proj/app",
		"app":                                 "app",
	}
	for in, want := range cases {
		if got := ImageRepo(in); got != want {
			t.Errorf("ImageRepo(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKeyOfIsolatesAppsOfOneNamespace(t *testing.T) {
	mk := func(ns, step, class, image string) *hpcv1alpha1.HPCArgoWorkflowJob {
		return &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Namespace: ns},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{StepName: step, Class: class, Image: image}}
	}
	base := KeyOf(mk("hpc-lab-app", "train", "gpu", "reg.example/lab/app:v1"))
	if base != KeyOf(mk("hpc-lab-app", "train", "gpu", "reg.example/lab/app:v2")) {
		t.Error("a new build of the same app must share the key")
	}
	for name, other := range map[string]*hpcv1alpha1.HPCArgoWorkflowJob{
		"another app":       mk("hpc-lab-app", "train", "gpu", "reg.example/lab/yolo:v1"),
		"another namespace": mk("hpc-lab-yolo", "train", "gpu", "reg.example/lab/app:v1"),
		"another step":      mk("hpc-lab-app", "eval", "gpu", "reg.example/lab/app:v1"),
		"another class":     mk("hpc-lab-app", "train", "cpu", "reg.example/lab/app:v1"),
	} {
		if KeyOf(other) == base {
			t.Errorf("%s must not share the key", name)
		}
	}
}

// Extreme cases (docs/extreme-cases.md, section 6).

func TestLearnNeverLeavesItsBoundsWhateverTheHistoryHolds(t *testing.T) {
	// Garbage in the history: zero, negative and absurdly large run times, in every mix.
	pool := []time.Duration{0, -time.Hour, time.Second, 3 * time.Minute, 2 * time.Hour, 10000 * time.Hour}
	caps := Caps{Spec: 240, Class: 2880, Site: 600}
	for a := range pool {
		for b := range pool {
			for c := range pool {
				for d := range pool {
					got, src := Learn([]time.Duration{pool[a], pool[b], pool[c], pool[d]}, caps)
					if got < RoundMinutes || got > caps.Spec {
						t.Fatalf("limit %d outside [%d, %d] for %v", got, RoundMinutes, caps.Spec,
							[]time.Duration{pool[a], pool[b], pool[c], pool[d]})
					}
					if src == hpcv1alpha1.PlacementDeclared && got != caps.Spec {
						t.Fatalf("declared source with limit %d, want the cap %d", got, caps.Spec)
					}
				}
			}
		}
	}
}

func TestLearnIgnoresOneHugeOutlierWhenThereAreEnoughRuns(t *testing.T) {
	samples := append(slow(30, 10), 24*60*time.Minute) // 30 runs of 10 min and one of a day
	got, src := Learn(samples, Caps{Spec: 2880, Class: 2880})
	if got != 15 || src != hpcv1alpha1.PlacementLearned {
		t.Fatalf("got %d %s: one outlier in 31 runs is above the 95th percentile", got, src)
	}
}

func TestLearnWithAHugeOutlierAmongFewRunsFallsBackToTheCap(t *testing.T) {
	got, src := Learn(mins(10, 10, 24*60), Caps{Spec: 240, Class: 2880})
	if got != 240 || src != hpcv1alpha1.PlacementDeclared {
		t.Fatalf("got %d %s: with three runs the slowest is the p95, and it is over the cap", got, src)
	}
}

func TestRetryNeverExceedsAnyCapAndNeverShrinks(t *testing.T) {
	for _, c := range []Caps{{Spec: 240}, {Spec: 240, Class: 100}, {Spec: 240, Site: 30}, {Spec: 10, Class: 2880}} {
		if got := Retry(c); got > c.Spec || got <= 0 {
			t.Fatalf("Retry(%+v) = %d", c, got)
		}
	}
}

func TestAppOfIsTheLastSegmentOfTheImageRepository(t *testing.T) {
	cases := map[string]string{
		"reg.example/proj/app:train-1":     "app",
		"reg.example:5000/proj/app:abc":    "app",
		"reg.example/proj/app@sha256:0123": "app",
		"app:v1":                           "",
		"":                                 "",
	}
	for in, want := range cases {
		if got := AppOf(in); got != want {
			t.Errorf("AppOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTokenCoverIsTheLifeLessTheMargin(t *testing.T) {
	if TokenLifeMinutes != 720 || TokenCoverMinutes != 690 {
		t.Fatalf("life %d, cover %d", TokenLifeMinutes, TokenCoverMinutes)
	}
}

func TestCapsMaxIncludesWhatOneRunsAccessCovers(t *testing.T) {
	if got := (Caps{Spec: 60, Class: 2880, Site: 1440, Token: TokenCoverMinutes}).Max(); got != TokenCoverMinutes {
		t.Fatalf("got %d, want the access cover", got)
	}
	if got := (Caps{Spec: 60, Class: 300, Token: TokenCoverMinutes}).Max(); got != 300 {
		t.Fatalf("got %d, want the lane's 300", got)
	}
}

func TestSegment(t *testing.T) {
	const run = TokenCoverMinutes
	cases := []struct {
		name                   string
		declared, perRun, used int32
		limit                  int32
		spent                  bool
	}{
		{"should_run_a_limit_shorter_than_one_run_as_declared", 400, run, 0, 400, false},
		{"should_not_count_down_a_limit_that_fits_one_run", 400, run, 800, 400, false},
		{"should_run_a_limit_equal_to_one_run_as_declared", run, run, 0, run, false},
		{"should_cut_a_limit_of_several_runs_at_one_run_first", 3 * run, run, 0, run, false},
		{"should_cut_the_second_run_at_one_run", 3 * run, run, run, run, false},
		{"should_cut_the_third_run_at_one_run", 3 * run, run, 2 * run, run, false},
		{"should_give_the_last_run_what_is_left", 3*run - 120, run, 2 * run, run - 120, false},
		{"should_count_runs_that_ended_early", 3 * run, run, 100, run, false},
		{"should_give_a_short_remainder_after_early_ends", 2*run + 30, run, 2*run + 10, 20, false},
		{"should_be_spent_when_the_whole_limit_is_used", 3 * run, run, 3 * run, 0, true},
		{"should_be_spent_when_more_than_the_limit_is_used", 3 * run, run, 3*run + 5, 0, true},
		{"should_not_cut_when_no_run_maximum_is_known", 5000, 0, 100, 5000, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			limit, spent := Segment(c.declared, c.perRun, c.used)
			if limit != c.limit || spent != c.spent {
				t.Fatalf("Segment(%d, %d, %d) = %d, %v; want %d, %v", c.declared, c.perRun, c.used, limit, spent, c.limit, c.spent)
			}
		})
	}
}

func TestHours(t *testing.T) {
	for in, want := range map[int32]string{690: "11.5 h", 720: "12 h", 60: "1 h", 100: "1.7 h"} {
		if got := Hours(in); got != want {
			t.Errorf("Hours(%d) = %q, want %q", in, got, want)
		}
	}
}
