package controller

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
)

// DefaultWarnAtPercent is the threshold of a site that does not set one (the CRD default).
const DefaultWarnAtPercent = int32(80)

// budgetWarning is the evaluation of one tier against the site's warning threshold.
type budgetWarning struct {
	tier    hpcv1alpha1.Tier
	percent int64
	over    bool
}

// evaluateBudgetWarnings judges each tier: used plus booked as a percentage of what the project may
// spend this period (the lower of its share and the site's remainder, PRD-HPC-1461 AC-22). A tier
// that may spend nothing has no percentage and never warns: the limit is already the message.
func evaluateBudgetWarnings(site *hpcv1alpha1.HPCSite, l ledger.Ledger) []budgetWarning {
	threshold := int64(site.Spec.Budget.WarnAtPercent)
	if threshold <= 0 {
		threshold = int64(DefaultWarnAtPercent)
	}
	out := make([]budgetWarning, 0, 2)
	for _, tier := range []hpcv1alpha1.Tier{hpcv1alpha1.TierGPU, hpcv1alpha1.TierCPU} {
		pct := l.Tiers[tier].Percent()
		out = append(out, budgetWarning{tier: tier, percent: pct, over: pct >= threshold})
	}
	return out
}

// noteBudgetWarning sets the BudgetWarning condition and the metrics from the ledger, and raises a
// Warning event on the site when the condition turns True (or a further tier crosses). It changes
// only the in-memory status; the caller's patch writes it.
func (r *HPCArgoWorkflowJobReconciler) noteBudgetWarning(site *hpcv1alpha1.HPCSite, l ledger.Ledger) {
	ws := evaluateBudgetWarnings(site, l)
	threshold := site.Spec.Budget.WarnAtPercent
	if threshold <= 0 {
		threshold = DefaultWarnAtPercent
	}
	var crossed []string
	for _, w := range ws {
		metrics.SetBudgetWarning(site.Spec.Site, projectOf(site), string(w.tier), w.percent, w.over)
		if w.over {
			crossed = append(crossed, string(w.tier))
		}
	}
	cond := metav1.Condition{Type: hpcv1alpha1.ConditionBudgetWarning, ObservedGeneration: site.Generation,
		Status: metav1.ConditionFalse, Reason: hpcv1alpha1.ReasonBelowThreshold,
		Message: fmt.Sprintf("used plus booked node-hours are below %d%% of what the project may spend this month", threshold)}
	if len(crossed) > 0 {
		cond.Status, cond.Reason = metav1.ConditionTrue, hpcv1alpha1.ReasonThresholdCrossed
		cond.Message = fmt.Sprintf("used plus booked %s node-hours are at or above %d%% of what the project may spend this month "+
			"(at the limit the policy is %s)", strings.Join(crossed, ", "), threshold, policyOf(site))
	}
	// A copy: SetStatusCondition updates the stored element in place.
	var prev *metav1.Condition
	if c := meta.FindStatusCondition(site.Status.Conditions, hpcv1alpha1.ConditionBudgetWarning); c != nil {
		prev = c.DeepCopy()
	}
	meta.SetStatusCondition(&site.Status.Conditions, cond)
	// The message names tiers, not percentages, so an event comes only when the condition turns True or
	// another tier crosses: not on every resync.
	if cond.Status == metav1.ConditionTrue && r.Recorder != nil &&
		(prev == nil || prev.Status != metav1.ConditionTrue || prev.Message != cond.Message) {
		r.Recorder.Event(site, corev1.EventTypeWarning, "BudgetWarning", cond.Message)
	}
}

// policyOf is the site's at-limit policy, block when unset.
func policyOf(site *hpcv1alpha1.HPCSite) hpcv1alpha1.AtLimitPolicy {
	if site.Spec.Budget.AtLimit == hpcv1alpha1.AtLimitWarn {
		return hpcv1alpha1.AtLimitWarn
	}
	return hpcv1alpha1.AtLimitBlock
}
