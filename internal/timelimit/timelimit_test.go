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
		{"capped_by_the_class_maximum", mins(70, 70, 70), Caps{Spec: 240, Class: 90}, 90, hpcv1alpha1.PlacementDeclared},
		{"capped_by_the_site_maximum", mins(70, 70, 70), Caps{Spec: 240, Class: 2880, Site: 90}, 90, hpcv1alpha1.PlacementDeclared},
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

func TestAJobAsksForTheMostAJobCanGetAndNeverMoreThanTheCap(t *testing.T) {
	cases := []struct {
		name string
		c    Caps
		want int32
	}{
		{"should_take_the_class_maximum_when_it_is_the_lowest", Caps{Spec: 10080, Class: 2880, Site: 4320}, 2880},
		{"should_take_the_lane_when_it_is_below_the_site_maximum", Caps{Spec: 10080, Class: 300, Site: 1440}, 300},
		{"should_take_the_site_maximum", Caps{Spec: 10080, Class: 2880, Site: 400}, 400},
		{"should_never_exceed_the_cap", Caps{Spec: 60, Class: 2880}, 60},
		{"should_take_the_cap_when_no_maximum_is_known", Caps{Spec: 10080}, 10080},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, src := Learn(nil, c.c); got != c.want || src != hpcv1alpha1.PlacementDeclared {
				t.Fatalf("Learn without history = %d (%s), want %d declared", got, src, c.want)
			}
			if got := Retry(c.c); got != c.want {
				t.Fatalf("Retry = %d, want %d", got, c.want)
			}
		})
	}
}

func TestALearnedLimitIsKeptBelowTheJobLimit(t *testing.T) {
	got, src := Learn(mins(20, 20, 20), Caps{Spec: 10080, Class: 2880})
	if got != 30 || src != hpcv1alpha1.PlacementLearned {
		t.Fatalf("got %d (%s), want a learned 30", got, src)
	}
}

func TestSegment(t *testing.T) {
	const job = 690
	cases := []struct {
		name                string
		total, perJob, used int32
		limit               int32
		spent               bool
	}{
		{"should_give_a_job_its_limit_when_the_cap_is_far", 10080, job, 0, job, false},
		{"should_count_down_a_cap_that_fits_one_job", 400, job, 100, 300, false},
		{"should_give_the_last_job_what_is_left", 10080, job, 10080 - 120, 120, false},
		{"should_count_jobs_that_ended_early", 10080, job, 100, job, false},
		{"should_keep_a_learned_limit_below_what_is_left", 10080, 30, 100, 30, false},
		{"should_be_spent_when_the_whole_cap_is_used", 3 * job, job, 3 * job, 0, true},
		{"should_be_spent_when_more_than_the_cap_is_used", 3 * job, job, 3*job + 5, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			limit, spent := Segment(c.total, c.perJob, c.used)
			if limit != c.limit || spent != c.spent {
				t.Fatalf("Segment(%d, %d, %d) = %d, %v; want %d, %v", c.total, c.perJob, c.used, limit, spent, c.limit, c.spent)
			}
		})
	}
}

func TestMinTimeIsAnHourOrTheLimitIfShorter(t *testing.T) {
	for limit, want := range map[int32]int32{690: 60, 60: 60, 45: 45} {
		if got := MinTime(limit); got != want {
			t.Errorf("MinTime(%d) = %d, want %d", limit, got, want)
		}
	}
}

func TestLeadIsASixthOfTheLimitBetweenOneAndTenMinutes(t *testing.T) {
	for limit, want := range map[int32]int32{0: 0, 1: 0, 2: 1, 3: 1, 12: 2, 15: 2, 30: 5, 60: 10, 120: 10, 690: 10, 2880: 10} {
		if got := Lead(limit); got != want {
			t.Errorf("Lead(%d) = %d, want %d", limit, got, want)
		}
	}
	// The warning never fires as the job starts: the lead is always shorter than the limit.
	for limit := int32(1); limit <= 3000; limit++ {
		if got := Lead(limit); got >= limit || got < 0 {
			t.Errorf("Lead(%d) = %d, want less than the limit", limit, got)
		}
	}
	// A step that is on time under a learned limit is never warned: the lead is shorter than the slack.
	for need := int32(1); need <= 80; need++ {
		d := time.Duration(need) * time.Minute
		limit, src := Learn([]time.Duration{d, d, d}, Caps{Spec: 10080, Class: 2880, Site: 2880})
		if src == hpcv1alpha1.PlacementLearned && limit-Lead(limit) < need {
			t.Errorf("need %d min, learned limit %d: warned at %d, before the step is done", need, limit, limit-Lead(limit))
		}
	}
}
