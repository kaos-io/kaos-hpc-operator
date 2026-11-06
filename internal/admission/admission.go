// Package admission decides whether an HPCArgoWorkflowJob may run, waits, or is refused (PRD-HPC-1461 D-12,
// D-13, D-24, D-41, D-49, F-11). It is a pure function of the job, its site and the ledger.
package admission

import (
	"fmt"
	"slices"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

// Decision is the outcome for one job.
type Decision struct {
	Phase   hpcv1alpha1.Phase
	Reason  string
	Message string
	// Tier and Cost are set when the job is admitted: what to book.
	Tier hpcv1alpha1.Tier
	Cost int64
	// OverShare is true when a warn-policy project was admitted beyond its share.
	OverShare bool
}

// ClassOf returns the site class named by the job, if the site offers it.
func ClassOf(site *hpcv1alpha1.HPCSite, class string) (hpcv1alpha1.SiteClass, bool) {
	for _, c := range site.Spec.Profile.Classes {
		if c.Name == class {
			return c, true
		}
	}
	return hpcv1alpha1.SiteClass{}, false
}

// Decide judges one job against its project's site and current ledger, booking its declared
// wall time.
func Decide(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, l ledger.Ledger) Decision {
	return DecideFor(job, site, l, job.Spec.WallTimeMinutes)
}

// DecideFor is Decide with the wall time the attempt will run with (PRD-HPC-1461 D-67): the cost
// booked is nodes x effectiveWall, not nodes x the spec's cap. The spec's wall time is still
// checked against the site's limits, since that is what the job asked for.
func DecideFor(job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite, l ledger.Ledger, effectiveWall int32) Decision {
	spec := job.Spec
	class, ok := ClassOf(site, spec.Class)
	if !ok {
		return refuse(hpcv1alpha1.ReasonNoSite, "site %s offers no class %q", site.Spec.Site, spec.Class)
	}
	if allowed := site.Spec.AllowedClasses; len(allowed) > 0 && !slices.Contains(allowed, spec.Class) {
		return refuse(hpcv1alpha1.ReasonClassNotAllowed, "class %q is not allowed for this project", spec.Class)
	}
	nodes := spec.Nodes
	if nodes == 0 {
		nodes = 1
	}
	if nodes > 1 {
		return refuse(hpcv1alpha1.ReasonMultiNodeUnsupported,
			"a job runs on one node (up to the class's GPUs); %d nodes were asked for", nodes)
	}
	limit := class.MaxWallTimeMinutes
	if m := site.Spec.Limits.MaxWallTimeMinutes; m > 0 && m < limit {
		limit = m
	}
	if spec.WallTimeMinutes > limit {
		return refuse(hpcv1alpha1.ReasonWallTimeOverLimit,
			"wall time %d min is above the limit of %d min", spec.WallTimeMinutes, limit)
	}

	cost := int64(nodes) * int64(effectiveWall)
	t := l.Tiers[class.Tier]
	block := site.Spec.Budget.AtLimit != hpcv1alpha1.AtLimitWarn
	if block && cost > t.Share-t.Used {
		return refuse(hpcv1alpha1.ReasonBudgetBlocked,
			"needs %d %s node-minutes; the project's share has %d left this period after what was used",
			cost, class.Tier, max(t.Share-t.Used, 0))
	}
	if m := site.Spec.Limits.MaxConcurrentJobs; m > 0 && l.Active >= m {
		return Decision{Phase: hpcv1alpha1.PhaseWaiting, Reason: hpcv1alpha1.ReasonConcurrencyLimit,
			Message: fmt.Sprintf("the project runs %d jobs, its limit; this one waits for a slot", l.Active)}
	}
	over := cost > t.Left()
	if over && block {
		return Decision{Phase: hpcv1alpha1.PhaseWaiting, Reason: hpcv1alpha1.ReasonWaitingForBudget,
			Message: fmt.Sprintf("needs %d %s node-minutes; %d are free until running jobs settle",
				cost, class.Tier, t.Left())}
	}
	d := Decision{Phase: hpcv1alpha1.PhaseAdmitted, Reason: hpcv1alpha1.ReasonAdmitted,
		Message: fmt.Sprintf("booked %d %s node-minutes", cost, class.Tier),
		Tier:    class.Tier, Cost: cost, OverShare: over}
	if over {
		d.Message += " beyond the project's share (policy warn)"
	}
	return d
}

func refuse(reason, format string, a ...any) Decision {
	return Decision{Phase: hpcv1alpha1.PhaseRefused, Reason: reason, Message: fmt.Sprintf(format, a...)}
}
