package admission

import (
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

func site(mod func(*hpcv1alpha1.HPCSite)) *hpcv1alpha1.HPCSite {
	s := &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{
		Site: "meluxina",
		Profile: hpcv1alpha1.SiteProfile{Classes: []hpcv1alpha1.SiteClass{
			{Name: "meluxina-gpu", Partition: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880},
			{Name: "meluxina-cpu", Partition: "cpu", Tier: hpcv1alpha1.TierCPU, MaxWallTimeMinutes: 2880},
		}},
		Budget: hpcv1alpha1.Budget{GPUNodeHours: 10, CPUNodeHours: 0},
	}}
	if mod != nil {
		mod(s)
	}
	return s
}

func job(class string, wall int32, nodes int32) *hpcv1alpha1.HPCArgoWorkflowJob {
	return &hpcv1alpha1.HPCArgoWorkflowJob{Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: class, Image: "i", WallTimeMinutes: wall, Nodes: nodes}}
}

func led(s *hpcv1alpha1.HPCSite, gpuUsed, gpuBooked int64, active int32) ledger.Ledger {
	return ledger.Ledger{PeriodStart: ledger.PeriodStart(time.Now()), Active: active, Tiers: map[hpcv1alpha1.Tier]ledger.Tier{
		hpcv1alpha1.TierGPU: {Share: s.Spec.Budget.GPUNodeHours * 60, Used: gpuUsed, Booked: gpuBooked},
		hpcv1alpha1.TierCPU: {Share: s.Spec.Budget.CPUNodeHours * 60},
	}}
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name       string
		site       *hpcv1alpha1.HPCSite
		job        *hpcv1alpha1.HPCArgoWorkflowJob
		used, book int64
		active     int32
		phase      hpcv1alpha1.Phase
		reason     string
		cost       int64
		over       bool
	}{
		{name: "fits: admitted and booked nodes x wall time",
			site: site(nil), job: job("meluxina-gpu", 240, 1),
			phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted, cost: 240},
		{name: "zero nodes means one",
			site: site(nil), job: job("meluxina-gpu", 60, 0),
			phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted, cost: 60},
		{name: "exactly the share left is admitted",
			site: site(nil), job: job("meluxina-gpu", 600, 1),
			phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted, cost: 600},
		{name: "unknown class",
			site: site(nil), job: job("meluxina-fpga", 60, 1),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonNoSite},
		{name: "class not allowed for the project",
			site:  site(func(s *hpcv1alpha1.HPCSite) { s.Spec.AllowedClasses = []string{"meluxina-cpu"} }),
			job:   job("meluxina-gpu", 60, 1),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonClassNotAllowed},
		{name: "more than one node is refused (D-41)",
			site: site(nil), job: job("meluxina-gpu", 60, 2),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonMultiNodeUnsupported},
		{name: "wall time over the class limit",
			site: site(nil), job: job("meluxina-gpu", 2881, 1),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonTooLongNotResumable},
		{name: "a class with no known wall limit cannot run a longer step: WallTimeOverLimit",
			site:  site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Profile.Classes[0].MaxWallTimeMinutes = 0 }),
			job:   job("meluxina-gpu", 60, 1),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonWallTimeOverLimit},
		{name: "wall time over the project limit, below the class limit",
			site:  site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Limits.MaxWallTimeMinutes = 120 }),
			job:   job("meluxina-gpu", 121, 1),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonTooLongNotResumable},
		{name: "never fits this period: BudgetBlocked (D-24)",
			site: site(nil), job: job("meluxina-gpu", 300, 1), used: 400,
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonBudgetBlocked},
		{name: "a tier with no share blocks (CPU 0 node-hours)",
			site: site(nil), job: job("meluxina-cpu", 10, 1),
			phase: hpcv1alpha1.PhaseRefused, reason: hpcv1alpha1.ReasonBudgetBlocked},
		{name: "fits only once bookings settle: WaitingForBudget (D-24)",
			site: site(nil), job: job("meluxina-gpu", 300, 1), book: 400,
			phase: hpcv1alpha1.PhaseWaiting, reason: hpcv1alpha1.ReasonWaitingForBudget},
		{name: "at the concurrency limit: waits, never refused (D-49)",
			site: site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Limits.MaxConcurrentJobs = 2 }),
			job:  job("meluxina-gpu", 60, 1), active: 2,
			phase: hpcv1alpha1.PhaseWaiting, reason: hpcv1alpha1.ReasonConcurrencyLimit},
		{name: "below the concurrency limit: admitted",
			site: site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Limits.MaxConcurrentJobs = 2 }),
			job:  job("meluxina-gpu", 60, 1), active: 1,
			phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted, cost: 60},
		{name: "warn policy admits beyond the share and flags it (D-13)",
			site: site(func(s *hpcv1alpha1.HPCSite) { s.Spec.Budget.AtLimit = hpcv1alpha1.AtLimitWarn }),
			job:  job("meluxina-gpu", 300, 1), used: 400,
			phase: hpcv1alpha1.PhaseAdmitted, reason: hpcv1alpha1.ReasonAdmitted, cost: 300, over: true},
		{name: "warn policy still honours the concurrency limit",
			site: site(func(s *hpcv1alpha1.HPCSite) {
				s.Spec.Budget.AtLimit = hpcv1alpha1.AtLimitWarn
				s.Spec.Limits.MaxConcurrentJobs = 1
			}),
			job: job("meluxina-gpu", 60, 1), active: 1,
			phase: hpcv1alpha1.PhaseWaiting, reason: hpcv1alpha1.ReasonConcurrencyLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Decide(c.job, c.site, led(c.site, c.used, c.book, c.active))
			if d.Phase != c.phase || d.Reason != c.reason {
				t.Fatalf("got %s/%s (%s), want %s/%s", d.Phase, d.Reason, d.Message, c.phase, c.reason)
			}
			if d.Cost != c.cost || d.OverShare != c.over {
				t.Fatalf("cost %d over %v, want %d %v", d.Cost, d.OverShare, c.cost, c.over)
			}
			if d.Message == "" {
				t.Fatal("every decision carries a message the user can read (R8)")
			}
		})
	}
}

func TestDecideForBooksTheEffectiveWallTime(t *testing.T) {
	s := site(nil)
	// A 240 min cap, learned down to 30: cost is nodes x 30, not x 240 (PRD-HPC-1461 D-67).
	d := DecideFor(job("meluxina-gpu", 240, 1), s, led(s, 0, 0, 0), 30)
	if d.Phase != hpcv1alpha1.PhaseAdmitted || d.Cost != 30 {
		t.Fatalf("got %s cost %d, want Admitted 30", d.Phase, d.Cost)
	}
	// What the job asked for is still checked against the site limits.
	d = DecideFor(job("meluxina-gpu", 2881, 1), s, led(s, 0, 0, 0), 30)
	if d.Reason != hpcv1alpha1.ReasonTooLongNotResumable {
		t.Fatalf("got %s, want TooLongNotResumable", d.Reason)
	}
	// A cap that does not fit the share fits once learned down.
	d = DecideFor(job("meluxina-gpu", 700, 1), s, led(s, 0, 0, 0), 30)
	if d.Phase != hpcv1alpha1.PhaseAdmitted {
		t.Fatalf("got %s, want Admitted", d.Phase)
	}
}
