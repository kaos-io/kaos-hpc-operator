// Package lanes chooses the site lane (QoS and reservation) of one attempt (PRD-HPC-1461 F-42,
// F-44, D-68, D-69). It is pure and site-agnostic: the ordered candidates come from the
// controller's configuration, the facts (QoS, reservations, class lanes) from the HPCSite profile,
// and the count of jobs already holding a one-job lane from the caller.
package lanes

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Candidate is one lane the controller may try, in the order it prefers.
type Candidate struct {
	// QOS names a QoS of the site profile.
	QOS string `json:"qos"`
	// ReservationByPartition names the reservation to use per partition of the class. Empty: the
	// lane needs no reservation. Non-empty: the class's partition must be listed, else the
	// candidate is skipped (a reservation lane is never used without its reservation).
	ReservationByPartition map[string]string `json:"reservationByPartition,omitempty"`
}

// Lane is the choice: the QoS and the reservation (empty for none).
type Lane struct {
	QOS, Reservation string
}

// Request is everything Choose needs to judge the candidates for one attempt.
type Request struct {
	Class   hpcv1alpha1.SiteClass
	Profile hpcv1alpha1.SiteProfile
	// LimitMinutes is the time limit the site job will ask for.
	LimitMinutes int32
	Now          time.Time
	// InUse is how many jobs of the same site identity already hold the QoS (D-69).
	InUse func(qos string) int32
}

// Parse reads the candidates from their JSON form. Empty text, null and [] mean none.
func Parse(raw string) ([]Candidate, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []Candidate
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("lane candidates: %w", err)
	}
	for i, c := range out {
		if c.QOS == "" {
			return nil, fmt.Errorf("lane candidates: entry %d has no qos", i)
		}
	}
	return out, nil
}

// Choose returns the first eligible candidate's lane, or false when none is (the class default
// QoS and no reservation then apply).
func Choose(candidates []Candidate, req Request) (Lane, bool) {
	for _, c := range candidates {
		if l, ok := eligible(c, req); ok {
			return l, true
		}
	}
	return Lane{}, false
}

// eligible applies the four rules of D-68/D-69 to one candidate.
func eligible(c Candidate, req Request) (Lane, bool) {
	// (a) the class may use the QoS, and the account has it.
	if len(req.Class.Lanes) > 0 && !slices.Contains(req.Class.Lanes, c.QOS) {
		return Lane{}, false
	}
	idx := slices.IndexFunc(req.Profile.QOS, func(q hpcv1alpha1.SiteQOS) bool { return q.Name == c.QOS })
	if idx < 0 {
		return Lane{}, false
	}
	q := req.Profile.QOS[idx]
	// A preemptible lane is never used: a preempted host takes its guests down with it.
	if slices.ContainsFunc(req.Profile.QOS, func(o hpcv1alpha1.SiteQOS) bool { return slices.Contains(o.Preempts, c.QOS) }) {
		return Lane{}, false
	}
	// (b) the limit fits the lane (0 = the site reports no limit).
	if q.MaxWallTimeMinutes > 0 && req.LimitMinutes > q.MaxWallTimeMinutes {
		return Lane{}, false
	}
	// (c) a reservation lane needs a live reservation of the class's partition.
	reservation, ok := reservationFor(c, req)
	if !ok {
		return Lane{}, false
	}
	// (d) the lane's per-user job limit is not used up (0 = unlimited).
	if q.MaxJobsPerUser > 0 && req.InUse != nil && req.InUse(c.QOS) >= q.MaxJobsPerUser {
		return Lane{}, false
	}
	return Lane{QOS: c.QOS, Reservation: reservation}, true
}

// reservationFor resolves the candidate's reservation for the class's partition: ("", true) when
// the lane needs none, (name, true) when the profile lists it live, ("", false) otherwise.
func reservationFor(c Candidate, req Request) (string, bool) {
	if len(c.ReservationByPartition) == 0 {
		return "", true
	}
	name := c.ReservationByPartition[req.Class.Partition]
	if name == "" {
		return "", false
	}
	for _, r := range req.Profile.Reservations {
		if r.Name != name || r.Partition != req.Class.Partition {
			continue
		}
		if r.EndTime != nil && !r.EndTime.After(req.Now) {
			return "", false // expired
		}
		return name, true
	}
	return "", false
}
