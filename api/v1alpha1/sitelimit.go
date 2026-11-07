package v1alpha1

import "strings"

// IsSiteLimitPendingReason reports whether a Slurm pending reason means the site's own account
// limit on node-minutes (the monthly allocation) holds the job, not the queue or the project's
// share (PRD-HPC-1461 AC-23). Slurm names it AssocGrp<resource>Minutes (MeluXina: AssocGrpGRESMinutes,
// AssocGrpBillingMinutes, AssocGrpCPUMinutes); the AssocGrp...RunMinutes limits cap what runs at one
// time, not the period's total, and are not it.
func IsSiteLimitPendingReason(reason string) bool {
	return strings.HasPrefix(reason, "AssocGrp") && strings.HasSuffix(reason, "Minutes") &&
		!strings.HasSuffix(reason, "RunMinutes")
}

// SiteLimitText is the plain sentence for a job held by the site's monthly limit. Console and
// follower share it, so people read the same words everywhere.
const SiteLimitText = "the site's monthly limit on this account is reached; the job starts when the site frees hours"

// DisplayPendingReason is the pending reason people see: SiteLimit for the site's monthly account
// limit, Slurm's own word for anything else. The raw reason stays on status.pendingReason.
func DisplayPendingReason(raw string) string {
	if IsSiteLimitPendingReason(raw) {
		return ReasonSiteLimit
	}
	return raw
}
