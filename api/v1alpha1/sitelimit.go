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

// siteHoldTexts are the plain sentences for a job the SITE holds in its queue for a reason that is not the
// queue itself: the site's own trouble or decision. Keys are Slurm's pending reasons (state_reason). A site in
// maintenance shows as ReqNodeNotAvail (the nodes are reserved for it) or as a closed partition.
var siteHoldTexts = map[string]string{
	"ReqNodeNotAvail": "the nodes this job needs are not available at the site: down, drained, or reserved " +
		"(a site maintenance shows like this); the job starts when the site returns them",
	"PartitionDown":     "the site has closed this partition (a site maintenance shows like this); the job starts when the site reopens it",
	"PartitionInactive": "the site has closed this partition (a site maintenance shows like this); the job starts when the site reopens it",
	"NodeDown":          "a node this job needs is down at the site; the job starts when the site brings it back",
	"JobHeldAdmin":      "the site's administrators hold this job; it starts when they release it",
	ReasonSiteAccessRefused: "the site refuses the platform's sign-in (a site maintenance shows like this); " +
		"the job is submitted when the site accepts it again",
	"Reservation": "the reserved nodes this job asks for are not available yet; the job starts when the reservation opens",
}

// SiteHoldText is the plain sentence for a pending reason that says the site itself holds the job, and "" for
// every other reason (the ordinary queue: Priority, Resources, and the limits named elsewhere). Console and
// follower share it, as they share SiteLimitText.
func SiteHoldText(reason string) string {
	if i := strings.IndexAny(reason, ", "); i > 0 {
		reason = reason[:i] // "ReqNodeNotAvail, Reserved for maintenance" on sites that append the detail
	}
	return siteHoldTexts[reason]
}

// PendingReasonLine is what people read for a queued job: the reason, and after it the plain sentence when
// there is one. Empty for no reason.
func PendingReasonLine(raw string) string {
	switch {
	case raw == "":
		return ""
	case IsSiteLimitPendingReason(raw):
		return ReasonSiteLimit + ": " + SiteLimitText
	}
	if t := SiteHoldText(raw); t != "" {
		return raw + ": " + t
	}
	return raw
}
