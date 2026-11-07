package v1alpha1

import "testing"

func TestIsSiteLimitPendingReason(t *testing.T) {
	cases := map[string]bool{
		"AssocGrpGRESMinutes":    true,
		"AssocGrpBillingMinutes": true,
		"AssocGrpCPUMinutes":     true,
		"AssocGrpGRESRunMinutes": false,
		"AssocGrpCPURunMinutes":  false,
		"AssocMaxJobsLimit":      false,
		"Priority":               false,
		"Resources":              false,
		"":                       false,
		"QOSMaxGRESPerUser":      false,
		"AssocGrpNodeMinutes":    true,
		"NotAssocGrpGRESMinutes": false,
		"AssocGrpGRESMinutesFoo": false,
	}
	for in, want := range cases {
		if got := IsSiteLimitPendingReason(in); got != want {
			t.Errorf("IsSiteLimitPendingReason(%q)=%v, want %v", in, got, want)
		}
	}
	if DisplayPendingReason("AssocGrpGRESMinutes") != ReasonSiteLimit || DisplayPendingReason("Priority") != "Priority" {
		t.Error("DisplayPendingReason must map only the site limit")
	}
}
