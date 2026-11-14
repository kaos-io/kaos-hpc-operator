package timelimit

import "testing"

// PRD kaos#1911 AC-13 (S9, S10): the plan of runs of a long resumable step, and the learned-limit retry.

// planRuns plays a step that always uses its whole run limit and returns the limits it was given.
func planRuns(declared, perRun int32) []int32 {
	var runs []int32
	var used int32
	for range 100000 {
		limit, spent := Segment(declared, perRun, used)
		if spent {
			return runs
		}
		runs = append(runs, limit)
		used += limit
		if declared <= perRun {
			return runs // a limit that fits one run applies to each run: exactly one attempt planned
		}
	}
	panic("plan does not end")
}

func TestSegmentPlanBoundaries(t *testing.T) {
	const run = TokenCoverMinutes
	cases := []struct {
		name     string
		declared int32
		want     []int32
	}{
		{"exactly_one_attempt_at_the_run_maximum", run, []int32{run}},
		{"one_minute_over_gives_a_second_one_minute_run", run + 1, []int32{run, 1}},
		{"cap_that_is_not_a_multiple_ends_with_the_remainder", 2*run + 100, []int32{run, run, 100}},
		{"cap_that_is_a_multiple_has_no_remainder_run", 3 * run, []int32{run, run, run}},
		{"one_minute_under_a_multiple", 2*run - 1, []int32{run, run - 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := planRuns(c.declared, run)
			if len(got) != len(c.want) {
				t.Fatalf("plan = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("plan = %v, want %v", got, c.want)
				}
			}
		})
	}
}

// Whatever the declared limit, no run is longer than the access life covers, the runs together are exactly the
// declared limit, and their number is the declared limit divided by one run, rounded up.
func TestSegmentPlanNeverPassesTheAccessCoverOrTheDeclaredLimit(t *testing.T) {
	for declared := int32(1); declared <= 5000; declared++ {
		runs := planRuns(declared, TokenCoverMinutes)
		var sum int32
		for _, l := range runs {
			if l < 1 || l > TokenCoverMinutes {
				t.Fatalf("declared %d: run limit %d outside 1..%d", declared, l, TokenCoverMinutes)
			}
			sum += l
		}
		if sum != declared {
			t.Fatalf("declared %d: runs add up to %d (%v)", declared, sum, runs)
		}
		if want := (declared + TokenCoverMinutes - 1) / TokenCoverMinutes; int32(len(runs)) != want {
			t.Fatalf("declared %d: %d runs, want %d", declared, len(runs), want)
		}
	}
}

// D-74 will change this: today a step that is verified resumable keeps its declared limit when it has history;
// it is learned only from run times, never from the resumable evidence. Learn does not look at resumability.
func TestLearnedLimitAfterATimeoutRetriesOnceAtTheDeclaredLimit(t *testing.T) {
	caps := Caps{Spec: 240, Class: 2880, Token: TokenCoverMinutes}
	learned, _ := Learn(mins(20, 20, 21), caps)
	if learned >= caps.Spec {
		t.Fatalf("history of 20 minute runs should learn a limit below the declared one, got %d", learned)
	}
	if got := Retry(caps); got != caps.Spec {
		t.Fatalf("the retry after a timeout under a learned limit must be the declared limit %d, got %d", caps.Spec, got)
	}
	// The retry is the end of the ladder: it is the declared limit, which is the most the step may have.
	if got := Retry(Caps{Spec: 240, Class: 90}); got != 90 {
		t.Fatalf("the retry never passes the class maximum, got %d", got)
	}
}
