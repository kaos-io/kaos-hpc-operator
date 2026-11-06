package lanes

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

var now = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

const meluxinaLanes = `[{"qos":"test"},{"qos":"dev","reservationByPartition":{"gpu":"gpudev","cpu":"cpudev"}}]`

func profile() hpcv1alpha1.SiteProfile {
	end := metav1.NewTime(now.Add(24 * time.Hour))
	return hpcv1alpha1.SiteProfile{
		QOS: []hpcv1alpha1.SiteQOS{
			{Name: "default", MaxWallTimeMinutes: 2880},
			{Name: "test", MaxWallTimeMinutes: 30, MaxJobsPerUser: 1},
			{Name: "dev", MaxWallTimeMinutes: 360, MaxJobsPerUser: 1},
		},
		Reservations: []hpcv1alpha1.SiteReservation{
			{Name: "gpudev", Partition: "gpu", NodeCount: 5, EndTime: &end},
			{Name: "cpudev", Partition: "cpu", NodeCount: 5},
		},
	}
}

func req(limit int32, mod func(*Request)) Request {
	r := Request{Class: hpcv1alpha1.SiteClass{Name: "gpu", Partition: "gpu", QOS: "default"},
		Profile: profile(), LimitMinutes: limit, Now: now, InUse: func(string) int32 { return 0 }}
	if mod != nil {
		mod(&r)
	}
	return r
}

func TestChoose(t *testing.T) {
	cands, err := Parse(meluxinaLanes)
	if err != nil {
		t.Fatal(err)
	}
	expired := metav1.NewTime(now.Add(-time.Minute))
	taken := func(qs ...string) func(string) int32 {
		return func(q string) int32 {
			for _, x := range qs {
				if x == q {
					return 1
				}
			}
			return 0
		}
	}
	cases := []struct {
		name string
		req  Request
		want Lane
		ok   bool
	}{
		{"should_use_test_when_the_limit_fits_30_minutes", req(30, nil), Lane{QOS: "test"}, true},
		{"should_use_dev_with_the_reservation_when_the_limit_is_over_the_test_maximum", req(31, nil), Lane{"dev", "gpudev"}, true},
		{"should_never_use_a_qos_another_qos_preempts", req(30, func(r *Request) {
			r.Profile.QOS = append(r.Profile.QOS, hpcv1alpha1.SiteQOS{Name: "long", Preempts: []string{"test"}})
			r.Profile.Reservations = nil
		}), Lane{}, false},
		{"should_use_default_when_the_limit_is_over_every_lane", req(361, nil), Lane{}, false},
		{"should_skip_dev_when_the_partition_has_no_reservation", req(60, func(r *Request) { r.Class.Partition = "fpga" }), Lane{}, false},
		{"should_skip_dev_when_the_reservation_is_not_in_the_profile", req(60, func(r *Request) { r.Profile.Reservations = nil }), Lane{}, false},
		{"should_skip_dev_when_the_reservation_is_of_another_partition", req(60, func(r *Request) { r.Profile.Reservations[0].Partition = "cpu" }), Lane{}, false},
		{"should_skip_dev_when_the_reservation_expired", req(60, func(r *Request) { r.Profile.Reservations[0].EndTime = &expired }), Lane{}, false},
		{"should_skip_a_lane_the_class_does_not_list", req(30, func(r *Request) { r.Class.Lanes = []string{"default", "dev"} }), Lane{"dev", "gpudev"}, true},
		{"should_use_a_lane_the_class_lists", req(30, func(r *Request) { r.Class.Lanes = []string{"test"} }), Lane{QOS: "test"}, true},
		{"should_skip_a_qos_the_account_does_not_have", req(30, func(r *Request) { r.Profile.QOS = r.Profile.QOS[:1] }), Lane{}, false},
		{"should_move_to_dev_when_the_test_job_slot_is_taken", req(20, func(r *Request) { r.InUse = taken("test") }), Lane{"dev", "gpudev"}, true},
		{"should_fall_back_to_default_when_both_job_slots_are_taken", req(20, func(r *Request) { r.InUse = taken("test", "dev") }), Lane{}, false},
		{"should_ignore_the_count_of_a_lane_without_a_job_limit", req(20, func(r *Request) {
			r.Profile.QOS[1].MaxJobsPerUser = 0
			r.InUse = func(string) int32 { return 9 }
		}), Lane{QOS: "test"}, true},
		{"should_treat_a_zero_maximum_wall_time_as_no_limit", req(5000, func(r *Request) { r.Profile.QOS[1].MaxWallTimeMinutes = 0 }), Lane{QOS: "test"}, true},
		{"should_use_a_reservation_that_never_ends", req(60, func(r *Request) { r.Class.Partition = "cpu" }), Lane{"dev", "cpudev"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := Choose(cands, c.req)
			if got != c.want || ok != c.ok {
				t.Fatalf("got %+v %v, want %+v %v", got, ok, c.want, c.ok)
			}
		})
	}
}

func TestChooseWithoutCandidatesChoosesNothing(t *testing.T) {
	if l, ok := Choose(nil, req(10, nil)); ok || l != (Lane{}) {
		t.Fatalf("got %+v %v", l, ok)
	}
}

func TestParse(t *testing.T) {
	for _, raw := range []string{"", "  ", "null", "[]"} {
		if c, err := Parse(raw); err != nil || len(c) != 0 {
			t.Errorf("Parse(%q) = %v, %v; want none", raw, c, err)
		}
	}
	for _, raw := range []string{"{", `[{"reservationByPartition":{"gpu":"x"}}]`} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("Parse(%q) must fail", raw)
		}
	}
	c, err := Parse(meluxinaLanes)
	if err != nil || len(c) != 2 || c[1].ReservationByPartition["gpu"] != "gpudev" {
		t.Fatalf("got %+v, %v", c, err)
	}
}
