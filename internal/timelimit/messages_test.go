package timelimit

import "testing"

func TestFormatMinutes(t *testing.T) {
	for in, want := range map[int32]string{240: "4 h", 90: "90 min", 60: "1 h", 45: "45 min", 2880: "2 days", 1440: "1 day", 10080: "7 days"} {
		if got := FormatMinutes(in); got != want {
			t.Errorf("FormatMinutes(%d) = %q, want %q", in, got, want)
		}
	}
}
