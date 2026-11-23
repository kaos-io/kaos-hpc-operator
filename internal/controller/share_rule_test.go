package controller

import "testing"

// D-75: when a guest joins a host and with what limit.
func TestGuestLimit(t *testing.T) {
	cases := []struct {
		name                   string
		remaining, limit, lead int32
		want                   int32
		ok                     bool
	}{
		{"should_join_a_host_with_the_limit_and_the_margin_left", 36, 30, 0, 30, true},
		{"should_join_by_the_lead_when_a_guest_with_verified_progress_fits_by_it", 20, 120, 10, 120, true},
		{"should_join_a_host_with_less_than_its_limit_left_by_what_the_host_has_left", 100, 120, 0, 100 - ShareMarginMinutes, true},
		{"should_join_a_host_with_its_limit_less_30_minutes_left", 90, 120, 0, 90 - ShareMarginMinutes, true},
		{"should_get_a_node_of_its_own_when_the_host_has_more_than_30_minutes_less_than_its_limit", 89, 120, 0, 0, false},
		{"should_get_a_node_of_its_own_when_what_is_left_after_the_margin_is_below_the_rounding", 7, 30, 0, 0, false},
		{"should_keep_the_rule_of_a_verified_guest_when_its_lead_does_not_fit_either", 10, 120, 10, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := guestLimit(c.remaining, c.limit, c.lead)
			if got != c.want || ok != c.ok {
				t.Fatalf("guestLimit(%d, %d, %d) = %d, %v; want %d, %v", c.remaining, c.limit, c.lead, got, ok, c.want, c.ok)
			}
		})
	}
}
