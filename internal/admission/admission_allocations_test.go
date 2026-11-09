package admission

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

// allocSite is a project with a class per allocation and a share in each (hours).
func allocSite(gpu, cpu, fpga, largemem int64) *hpcv1alpha1.HPCSite {
	return &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
		Profile: hpcv1alpha1.SiteProfile{Classes: []hpcv1alpha1.SiteClass{
			{Name: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880},
			{Name: "cpu", Tier: hpcv1alpha1.TierCPU, MaxWallTimeMinutes: 2880},
			{Name: "fpga", Tier: hpcv1alpha1.TierFPGA, MaxWallTimeMinutes: 2880},
			{Name: "largemem", Tier: hpcv1alpha1.TierLargeMem, MaxWallTimeMinutes: 2880},
		}},
		Budget: hpcv1alpha1.Budget{GPUNodeHours: gpu, CPUNodeHours: cpu, FPGANodeHours: fpga, LargeMemNodeHours: largemem}}}
}

func allocLedger(s *hpcv1alpha1.HPCSite, booked map[hpcv1alpha1.Tier]int64) ledger.Ledger {
	l := ledger.Ledger{PeriodStart: ledger.PeriodStart(time.Now()), Tiers: map[hpcv1alpha1.Tier]ledger.Tier{}}
	for _, t := range hpcv1alpha1.Tiers {
		l.Tiers[t] = ledger.Tier{Share: s.Spec.Budget.NodeHours(t) * 60, Booked: booked[t]}
	}
	return l
}

// PRD-HPC-1461 AC-84: each allocation is judged on its own ledger.
func TestDecideJudgesEveryAllocationOnItsOwnLedger(t *testing.T) {
	cases := []struct {
		name   string
		site   *hpcv1alpha1.HPCSite
		class  string
		booked map[hpcv1alpha1.Tier]int64
		phase  hpcv1alpha1.Phase
		reason string
		inMsg  string
	}{
		{"should_admit_each_allocation_that_has_hours", allocSite(1, 1, 1, 1), "fpga", nil,
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, "FPGA"},
		{"should_refuse_a_cpu_step_when_the_cpu_allocation_is_spent", allocSite(10, 0, 10, 10), "cpu", nil,
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonBudgetBlocked, "CPU allocation"},
		{"should_still_admit_gpu_when_only_cpu_is_spent", allocSite(10, 0, 10, 10), "gpu", nil,
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, "GPU"},
		{"should_still_admit_fpga_and_largemem_when_only_cpu_is_spent", allocSite(10, 0, 10, 10), "largemem", nil,
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, "large memory"},
		{"should_refuse_fpga_when_no_fpga_hours_are_set", allocSite(10, 10, 0, 10), "fpga", nil,
			hpcv1alpha1.PhaseRefused, hpcv1alpha1.ReasonBudgetBlocked, "FPGA allocation"},
		{"should_wait_for_the_largemem_allocation_when_its_own_bookings_fill_it", allocSite(10, 10, 10, 2), "largemem",
			map[hpcv1alpha1.Tier]int64{hpcv1alpha1.TierLargeMem: 100},
			hpcv1alpha1.PhaseWaiting, hpcv1alpha1.ReasonWaitingForBudget, "large memory allocation"},
		{"should_not_wait_for_cpu_because_gpu_bookings_fill_the_gpu_allocation", allocSite(2, 10, 10, 10), "cpu",
			map[hpcv1alpha1.Tier]int64{hpcv1alpha1.TierGPU: 120},
			hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.ReasonAdmitted, "CPU"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := job(c.class, 60, 1)
			d := Decide(j, c.site, allocLedger(c.site, c.booked))
			if d.Phase != c.phase || d.Reason != c.reason {
				t.Fatalf("got %s/%s (%s), want %s/%s", d.Phase, d.Reason, d.Message, c.phase, c.reason)
			}
			if !strings.Contains(d.Message, c.inMsg) {
				t.Fatalf("message %q must name %q", d.Message, c.inMsg)
			}
			if c.phase == hpcv1alpha1.PhaseAdmitted && d.Tier != tierOfClass(c.site, c.class) {
				t.Fatalf("booked on %s, want the class's tier", d.Tier)
			}
		})
	}
}

func tierOfClass(s *hpcv1alpha1.HPCSite, class string) hpcv1alpha1.Tier {
	c, _ := ClassOf(s, class)
	return c.Tier
}

// A site-reported zero is the site's limit: SiteLimit, naming the allocation (the 2026-11-05 incident).
func TestDecideSpentSiteAllocationIsSiteLimit(t *testing.T) {
	s := allocSite(10, 0, 10, 10)
	s.Spec.Budget.Source = hpcv1alpha1.BudgetSourceSite
	l := allocLedger(s, nil)
	for _, tier := range hpcv1alpha1.Tiers {
		x := l.Tiers[tier]
		x.SiteSource = true
		l.Tiers[tier] = x
	}
	d := Decide(job("cpu", 60, 1), s, l)
	if d.Phase != hpcv1alpha1.PhaseRefused || d.Reason != hpcv1alpha1.ReasonSiteLimit ||
		!strings.Contains(d.Message, "0 left on the CPU allocation") {
		t.Fatalf("got %s/%s %q", d.Phase, d.Reason, d.Message)
	}
	if d := Decide(job("gpu", 60, 1), s, l); d.Phase != hpcv1alpha1.PhaseAdmitted {
		t.Fatalf("gpu must not be blocked: %s/%s", d.Phase, d.Reason)
	}
}

// siteLeft omitting an allocation means no site figure (share applies); an explicit 0 is a spent allocation.
func TestDecideSiteLeftOmittedVersusExplicitZero(t *testing.T) {
	zero := int64(0)
	cases := []struct {
		name  string
		left  hpcv1alpha1.SiteLeft
		class string
		phase hpcv1alpha1.Phase
		inMsg string
	}{
		{"should_admit_fpga_when_site_left_omits_it", hpcv1alpha1.SiteLeft{GPUNodeHours: 5}, "fpga", hpcv1alpha1.PhaseAdmitted, ""},
		{"should_admit_largemem_when_site_left_omits_it", hpcv1alpha1.SiteLeft{GPUNodeHours: 5}, "largemem", hpcv1alpha1.PhaseAdmitted, ""},
		{"should_block_fpga_naming_it_when_site_left_says_zero", hpcv1alpha1.SiteLeft{GPUNodeHours: 5, FPGANodeHours: &zero},
			"fpga", hpcv1alpha1.PhaseRefused, "FPGA allocation"},
		{"should_block_cpu_when_cpu_omitted_as_before", hpcv1alpha1.SiteLeft{GPUNodeHours: 5}, "cpu", hpcv1alpha1.PhaseRefused, "CPU allocation"},
		{"should_still_admit_gpu_present", hpcv1alpha1.SiteLeft{GPUNodeHours: 5}, "gpu", hpcv1alpha1.PhaseAdmitted, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := allocSite(10, 10, 10, 10)
			obs := metav1.NewTime(time.Now().Add(-time.Hour))
			left := c.left
			left.ObservedAt = &obs
			s.Spec.Budget.SiteLeft = &left
			l := ledger.Compute(s, nil, nil, time.Now())
			d := Decide(&hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: c.class, Image: "i", WallTimeMinutes: 60}}, s, l)
			if d.Phase != c.phase || !strings.Contains(d.Message, c.inMsg) {
				t.Fatalf("got %s/%s %q, want %s containing %q", d.Phase, d.Reason, d.Message, c.phase, c.inMsg)
			}
		})
	}
}
