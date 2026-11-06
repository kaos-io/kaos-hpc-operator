// Package timelimit learns a step's Slurm time limit from its earlier runs (PRD-HPC-1461 F-41,
// D-67). It is pure: the controller gathers the samples, this package decides. The job spec's
// wallTimeMinutes is the cap and is never exceeded.
package timelimit

import (
	"math"
	"sort"
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

// Caps are the upper bounds of a limit besides the spec's: the class's and the site's maximum
// wall time (0 means unknown, no bound).
type Caps struct {
	Spec, Class, Site int32
}

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
// With fewer than MinSamples it returns the spec's limit, source declared. Otherwise the limit is
// p95 x Margin rounded up to RoundMinutes, at least RoundMinutes and at most the lowest cap; a
// learned limit that reaches the cap is the declared one.
func Learn(samples []time.Duration, caps Caps) (int32, hpcv1alpha1.PlacementSource) {
	if len(samples) < MinSamples {
		return caps.Spec, hpcv1alpha1.PlacementDeclared
	}
	p95 := percentile(samples, Percentile).Minutes()
	limit := int32(math.Ceil(p95*Margin/RoundMinutes)) * RoundMinutes
	limit = max(limit, RoundMinutes)
	limit = min(limit, caps.lowest())
	if limit >= caps.Spec {
		return caps.Spec, hpcv1alpha1.PlacementDeclared
	}
	return limit, hpcv1alpha1.PlacementLearned
}

// Retry is the limit of the one attempt after a TIMEOUT under a learned limit: the declared limit
// (the lowest cap), not a multiple of the learned one. A step whose run time depends on its inputs
// (epochs=1 for weeks, then epochs=50) would otherwise be killed again at 2x and fail where the
// declared limit passes; each kill wastes the node time it ran. So a step is killed at most once,
// and the worst case is today's behaviour plus one queue wait.
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
