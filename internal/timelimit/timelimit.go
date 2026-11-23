// Package timelimit learns a step's Slurm time limit from its earlier runs (PRD-HPC-1461 F-41,
// D-67). It is pure: the controller gathers the samples, this package decides. The job spec's
// wallTimeMinutes is the platform's cap on the step's total time (D-75); one job asks for the most a job can
// get, or the learned limit, and the jobs together never exceed the cap.
package timelimit

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

const (
	// MinSamples is how many earlier runs are needed before the declared limit is replaced.
	MinSamples = 3
	// Percentile of the sampled run times the limit is based on.
	Percentile = 0.95
	// Margin multiplies the percentile run time.
	Margin = 1.5
	// RoundMinutes is the granularity of a learned limit; it is also the smallest limit.
	RoundMinutes = 15
	// MaxLearnedMinutes: a limit is learned only for short steps. A shorter limit helps a job into
	// the site's backfill gaps (minutes to about an hour); a long training gains nothing from it and
	// would risk being killed hours in when one run is slower than its history (Petros 2026-11-07:
	// nobody loses a training). Above this, the step keeps its declared limit.
	MaxLearnedMinutes = 120
)

// Key identifies "the same step of the same app on the same class": jobs with an equal Key share
// their run times. The app is its image repository (registry and path without tag or digest): the
// pipeline builds one image per app and the tag changes with every build, while a namespace can
// hold several apps, so namespace and step name alone would mix them.
type Key struct {
	Namespace, Step, Class, ImageRepo string
}

// KeyOf is the Key of a serve-mode job.
func KeyOf(j *hpcv1alpha1.HPCArgoWorkflowJob) Key {
	return Key{Namespace: j.Namespace, Step: j.Spec.StepName, Class: j.Spec.Class, ImageRepo: ImageRepo(j.Spec.Image)}
}

// ImageRepo returns an image reference without its tag and digest.
func ImageRepo(image string) string {
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	// A colon after the last slash is a tag; one before it is a registry port.
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		image = image[:i]
	}
	return image
}

// AppOf is the app an image belongs to: the last path segment of its repository, as the pipeline
// pushes one repository per app ({registry}/{project}/{app}). "" for an image with no path.
func AppOf(image string) string {
	repo := ImageRepo(image)
	i := strings.LastIndex(repo, "/")
	if i < 0 {
		return ""
	}
	return repo[i+1:]
}

// Caps are the bounds of a job's time limit: the spec's cap on the step's total time, and the class's and the
// site's maximum wall time (0 means unknown, no bound).
type Caps struct {
	Spec, Class, Site int32
}

// lowest is the limit of one job that is not learned: the most a job can get (the lowest of the class's and
// the site's maximum), and never more than the cap.
func (c Caps) lowest() int32 {
	m := c.Spec
	for _, v := range []int32{c.Class, c.Site} {
		if v > 0 && v < m {
			m = v
		}
	}
	return m
}

// Learn returns the time limit for a step from the run times of its earlier successful attempts.
// With fewer than MinSamples it returns the most a job can get, source declared. Otherwise the limit is
// p95 x Margin rounded up to RoundMinutes, at least RoundMinutes and at most that; a learned limit that
// reaches it is the declared one.
func Learn(samples []time.Duration, caps Caps) (int32, hpcv1alpha1.PlacementSource) {
	job := caps.lowest()
	if len(samples) < MinSamples {
		return job, hpcv1alpha1.PlacementDeclared
	}
	p95 := percentile(samples, Percentile).Minutes()
	limit := int32(math.Ceil(p95*Margin/RoundMinutes)) * RoundMinutes
	limit = max(limit, RoundMinutes)
	if limit > MaxLearnedMinutes {
		return job, hpcv1alpha1.PlacementDeclared // a long step keeps the full job limit
	}
	if limit >= job {
		return job, hpcv1alpha1.PlacementDeclared
	}
	return limit, hpcv1alpha1.PlacementLearned
}

// Retry is the limit of the one attempt after a TIMEOUT under a learned limit: the most a job can get, not a
// multiple of the learned one. A step whose run time depends on its inputs (epochs=1 for weeks, then epochs=50)
// would otherwise be killed again at 2x and fail where the full limit passes; each kill wastes the node time it
// ran. So a step is killed at most once, and the worst case is one queue wait more.
func Retry(caps Caps) int32 {
	return caps.lowest()
}

// percentile is the nearest-rank percentile of a non-empty sample set.
func percentile(samples []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := int(math.Ceil(p*float64(len(s)))) - 1
	return s[min(max(rank, 0), len(s)-1)]
}

// FormatMinutes writes a limit the way a person says it: whole days as "7 days", whole hours as "4 h", else minutes.
func FormatMinutes(m int32) string {
	switch {
	case m >= 1440 && m%1440 == 0 && m/1440 == 1:
		return "1 day"
	case m >= 1440 && m%1440 == 0:
		return strconv.Itoa(int(m/1440)) + " days"
	case m >= 60 && m%60 == 0:
		return strconv.Itoa(int(m/60)) + " h"
	}
	return strconv.Itoa(int(m)) + " min"
}

// Segment is the time limit of the next job of a step whose total time is capped (D-75), after used minutes of
// its earlier jobs: a job gets perRun, the limit one job asks for, and never more than what is left of the cap,
// so the jobs together stay within it. spent is true when the whole cap is used up.
func Segment(total, perRun, used int32) (limit int32, spent bool) {
	left := total - used
	if left <= 0 {
		return 0, true
	}
	return min(perRun, left), false
}

// MinTimeMinutes is the minimum time a step with verified saved progress asks for besides its limit (D-75): one
// hour, so the site may fit it into any gap.
const MinTimeMinutes = 60

// MaxLeadMinutes is the longest warning before a job's limit (D-75).
const MaxLeadMinutes = 10

// Lead is how long before its limit a job is warned (the site sends the signal, the job script passes it to a
// process that catches it): a sixth of the limit, at least one minute and at most MaxLeadMinutes. A fixed ten
// minutes would stop a short job early: a learned limit is one and a half times what the step needs
// (Margin), so a warning in the last sixth never reaches a step that is on time, and a job of a few minutes
// is not told to stop as it starts. The lead is always shorter than the limit: a job of one minute gets 0, no
// warning.
func Lead(limit int32) int32 {
	return min(MaxLeadMinutes, max(1, limit/6), max(limit-1, 0))
}

// MinTime is the minimum time submitted with a job of the given limit: one hour, or the limit if that is shorter.
func MinTime(limit int32) int32 {
	return min(MinTimeMinutes, limit)
}
