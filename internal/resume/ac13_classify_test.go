package resume

import (
	"testing"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// PRD kaos#1911 AC-13 (S4, S5): every state the site can report ends in a defined End, and the ones that
// must never be re-run are not Recoverable.
func TestClassifyRecoverability(t *testing.T) {
	cases := []struct {
		state       string
		exit        *int32
		want        End
		recoverable bool
	}{
		{"OUT_OF_MEMORY", nil, EndStepError, false},
		{"FAILED", i32(1), EndStepError, false},
		{"FAILED", i32(hpcv1alpha1.ExitCodeContinue), EndContinue, true},
		{"BOOT_FAIL", nil, EndNodeFailure, true},
		{"NODE_FAIL", nil, EndNodeFailure, true},
		{"PREEMPTED", nil, EndPreempted, true},
		{"TIMEOUT", nil, EndWallTime, true},
		{"CANCELLED", nil, EndCancelled, false},
		{"CANCELLED by 1000", nil, EndCancelled, false},
		{"node_fail", nil, EndNodeFailure, true}, // the site's spelling is not trusted
	}
	for _, c := range cases {
		got := Classify(c.state, c.exit)
		if got != c.want || got.Recoverable() != c.recoverable {
			t.Errorf("Classify(%q, %v) = %v recoverable=%v, want %v recoverable=%v", c.state, c.exit, got, got.Recoverable(), c.want, c.recoverable)
		}
	}
}

func TestAdvancedNeedsRealProgress(t *testing.T) {
	cases := []struct {
		name      string
		prev, cur Mark
		want      bool
	}{
		{"same_values", Mark{Values: []int64{3}}, Mark{Values: []int64{3}}, false},
		{"lower_values", Mark{Values: []int64{3}}, Mark{Values: []int64{2}}, false},
		{"higher_values", Mark{Values: []int64{3}}, Mark{Values: []int64{4}}, true},
		{"same_digest", Mark{Digest: "a"}, Mark{Digest: "a"}, false},
		{"new_digest", Mark{Digest: "a"}, Mark{Digest: "b"}, true},
		{"nothing_reported", Mark{Digest: "a"}, Mark{}, false},
	}
	for _, c := range cases {
		if got := Advanced(c.prev, c.cur); got != c.want {
			t.Errorf("%s: Advanced = %v, want %v", c.name, got, c.want)
		}
	}
}
