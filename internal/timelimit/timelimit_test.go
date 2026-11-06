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
		{"two_slow_runs_of_20_reach_the_p95", append(slow(18, 10), mins(100, 100)...), caps, 150, hpcv1alpha1.PlacementLearned},
		{"p95_of_3_is_the_slowest_run", mins(5, 5, 100), caps, 150, hpcv1alpha1.PlacementLearned},
		{"capped_by_the_spec_means_declared", mins(200, 200, 200), caps, 240, hpcv1alpha1.PlacementDeclared},
		{"capped_by_the_class_maximum", mins(100, 100, 100), Caps{Spec: 240, Class: 120}, 120, hpcv1alpha1.PlacementLearned},
		{"capped_by_the_site_maximum", mins(100, 100, 100), Caps{Spec: 240, Class: 2880, Site: 90}, 90, hpcv1alpha1.PlacementLearned},
		{"unknown_class_and_site_maxima_do_not_bound", mins(100, 100, 100), Caps{Spec: 240}, 150, hpcv1alpha1.PlacementLearned},
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
		prev int32
		caps Caps
		want int32
	}{
		{"doubles_the_previous_limit", 30, Caps{Spec: 240, Class: 2880}, 60},
		{"capped_by_the_spec", 30, Caps{Spec: 45, Class: 2880}, 45},
		{"capped_by_the_class", 30, Caps{Spec: 240, Class: 50}, 50},
		{"capped_by_the_site", 30, Caps{Spec: 240, Class: 2880, Site: 40}, 40},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Retry(c.prev, c.caps); got != c.want {
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
	base := KeyOf(mk("hpc-lab-app", "train", "gpu", "reg/lab/app:v1"))
	if base != KeyOf(mk("hpc-lab-app", "train", "gpu", "reg/lab/app:v2")) {
		t.Error("a new build of the same app must share the key")
	}
	for name, other := range map[string]*hpcv1alpha1.HPCArgoWorkflowJob{
		"another app":       mk("hpc-lab-app", "train", "gpu", "reg/lab/yolo:v1"),
		"another namespace": mk("hpc-lab-yolo", "train", "gpu", "reg/lab/app:v1"),
		"another step":      mk("hpc-lab-app", "eval", "gpu", "reg/lab/app:v1"),
		"another class":     mk("hpc-lab-app", "train", "cpu", "reg/lab/app:v1"),
	} {
		if KeyOf(other) == base {
			t.Errorf("%s must not share the key", name)
		}
	}
}
