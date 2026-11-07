package timelimit

import "testing"

func TestFormatMinutes(t *testing.T) {
	for in, want := range map[int32]string{240: "4 h", 90: "90 min", 60: "1 h", 45: "45 min", 2880: "48 h"} {
		if got := FormatMinutes(in); got != want {
			t.Errorf("FormatMinutes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestCapsMax(t *testing.T) {
	cases := []struct {
		name string
		c    Caps
		want int32
	}{
		{"should_take_the_lower_of_class_and_site", Caps{Spec: 60, Class: 2880, Site: 1440}, 1440},
		{"should_take_the_only_known_cap", Caps{Spec: 60, Class: 2880}, 2880},
		{"should_be_zero_when_unknown", Caps{Spec: 60}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.c.Max(); got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}
}

func TestStopMessage(t *testing.T) {
	cases := []struct {
		name       string
		limit, max int32
		resumable  bool
		want       string
	}{
		{"should_name_limit_and_site_maximum", 240, 2880, false,
			"This step ran its full 4 h time limit and was stopped. Set a longer limit for it (`walltime`, up to 48 h on this site) or make it resumable so it continues automatically from a checkpoint."},
		{"should_use_minutes_for_odd_limits", 90, 1440, false,
			"This step ran its full 90 min time limit and was stopped. Set a longer limit for it (`walltime`, up to 24 h on this site) or make it resumable so it continues automatically from a checkpoint."},
		{"should_not_promise_a_longer_limit_at_the_maximum", 2880, 2880, false,
			"This step ran its full 48 h time limit and was stopped. Its limit is already the longest this site allows (48 h); make it resumable so it continues automatically from a checkpoint."},
		{"should_omit_the_maximum_when_unknown", 60, 0, false,
			"This step ran its full 1 h time limit and was stopped. Set a longer limit for it (`walltime`) or make it resumable so it continues automatically from a checkpoint."},
		{"should_explain_a_missing_checkpoint_for_a_resumable_step", 240, 2880, true,
			"This step ran its full 4 h time limit and was stopped before it saved a checkpoint; save more often or ask to be signalled earlier."},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := StopMessage(c.limit, c.max, c.resumable); got != c.want {
				t.Fatalf("got %q\nwant %q", got, c.want)
			}
		})
	}
}

func TestWarnMessage(t *testing.T) {
	if got, want := WarnMessage(240, false), "Warning: this step was stopped at its 4 h limit in an earlier run and will be again unless you raise its limit (`walltime`) or make it resumable."; got != want {
		t.Fatalf("got %q", got)
	}
	if got, want := WarnMessage(90, true), "Warning: this step was stopped at its 90 min limit in an earlier run before it saved a checkpoint; save more often or ask to be signalled earlier."; got != want {
		t.Fatalf("got %q", got)
	}
}
