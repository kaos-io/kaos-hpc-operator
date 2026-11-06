package v1alpha1

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSiteProfileLanesRoundTrip proves the F-40 fields survive JSON and DeepCopy unchanged.
func TestSiteProfileLanesRoundTrip(t *testing.T) {
	end := metav1.NewTime(time.Date(2027, 9, 6, 0, 0, 0, 0, time.UTC))
	in := SiteProfile{
		Transport: TransportSlurmREST, Endpoint: "https://x", RESTVersion: "v0.0.44",
		Classes: []SiteClass{{Name: "gpu", Partition: "gpu", Tier: TierGPU, MaxWallTimeMinutes: 60, Lanes: []string{"default", "dev"}}},
		QOS: []SiteQOS{{Name: "long", Priority: 1000, MaxWallTimeMinutes: 8640, MaxJobsPerUser: 1,
			Preempts: []string{"short-preempt"}, UsageFactorMilli: 1000}},
		Reservations: []SiteReservation{{Name: "gpudev", Partition: "gpu", NodeCount: 5, EndTime: &end}},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out SiteProfile
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.QOS, out.QOS) || !reflect.DeepEqual(in.Classes, out.Classes) ||
		len(out.Reservations) != 1 || !out.Reservations[0].EndTime.Equal(&end) {
		t.Fatalf("round trip changed profile: %s", raw)
	}
	if cp := in.DeepCopy(); !reflect.DeepEqual(&in, cp) {
		t.Fatal("deepcopy differs")
	}
}

// TestSiteProfileLanesOptional proves a profile without the new fields serialises without them.
func TestSiteProfileLanesOptional(t *testing.T) {
	raw, _ := json.Marshal(SiteProfile{Classes: []SiteClass{{Name: "c"}}})
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if _, ok := m["qos"]; ok {
		t.Fatal("qos must be omitted when empty")
	}
	if _, ok := m["reservations"]; ok {
		t.Fatal("reservations must be omitted when empty")
	}
}
