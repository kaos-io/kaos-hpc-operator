package controller

import (
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

var (
	// earliestSiteTime is the oldest time a site report can plausibly carry; anything before it
	// (the Unix epoch from an unset field, for one) is unknown, not a start.
	earliestSiteTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	// futureSlack tolerates clock skew between the site and this controller.
	futureSlack = time.Hour
)

// settlement is what one attempt spent, from the times the site reported.
type settlement struct {
	Minutes int64
	// End is the attempt's end: the site's when valid, else the controller's own observation time.
	End metav1.Time
	// Discarded names each reported time that was rejected, for a Warning event.
	Discarded []string
}

// validSiteTime reports whether a reported time is usable: set, not before 2000, not after now+1h.
// Slurm's NO_VAL (0xFFFFFFFE seconds, year 2106) for a never-started job fails the last test.
func validSiteTime(t *metav1.Time, now time.Time) bool {
	//nolint:staticcheck // t.Time is explicit: metav1.Time has its own Before(*Time)
	return t != nil && !t.IsZero() && !t.Time.Before(earliestSiteTime) && !t.Time.After(now.Add(futureSlack))
}

// settle computes the node-minutes an attempt spent. The result is never negative and never
// absurd: an unknown start means the job never ran on its own (0); an unknown end settles up to
// now, which is conservative and never zero for a job that ran. A guest is covered by its host (D-26).
func settle(nodes int32, run *hpcv1alpha1.RunStatus, now time.Time) settlement {
	s := settlement{End: metav1.NewTime(now)}
	startOK := validSiteTime(run.StartTime, now)
	if run.StartTime != nil && !startOK {
		s.Discarded = append(s.Discarded, "startTime "+run.StartTime.UTC().Format(time.RFC3339))
	}
	endOK := validSiteTime(run.EndTime, now)
	if run.EndTime != nil && !endOK {
		s.Discarded = append(s.Discarded, "endTime "+run.EndTime.UTC().Format(time.RFC3339))
	}
	if startOK && endOK && run.EndTime.Time.Before(run.StartTime.Time) {
		s.Discarded = append(s.Discarded, "endTime "+run.EndTime.UTC().Format(time.RFC3339)+" (before startTime)")
		endOK = false
	}
	if endOK {
		s.End = *run.EndTime
	}
	if run.HostSiteJobID != "" || !startOK {
		return s
	}
	end := now
	if endOK {
		end = run.EndTime.Time
	}
	s.Minutes = int64(max(nodes, 1)) * max(int64(math.Ceil(end.Sub(run.StartTime.Time).Minutes())), 0)
	return s
}
