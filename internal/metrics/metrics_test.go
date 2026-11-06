package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func TestSetJobPhaseKeepsOnlyTheCurrentPhase(t *testing.T) {
	SetJobPhase("ns", "j1", "meluxina-gpu", "gpu", "Admitted")
	SetJobPhase("ns", "j1", "meluxina-gpu", "gpu", "Running")
	if got := testutil.CollectAndCount(JobPhase); got != 1 {
		t.Fatalf("want 1 series, got %d", got)
	}
	if v := testutil.ToFloat64(JobPhase.WithLabelValues("ns", "j1", "meluxina-gpu", "gpu", "Running")); v != 1 {
		t.Fatalf("Running = %v", v)
	}
	ForgetJob("ns", "j1")
	if got := testutil.CollectAndCount(JobPhase); got != 0 {
		t.Fatalf("want 0 series after ForgetJob, got %d", got)
	}
}

func TestSetSiteLedgerPublishesThreeKinds(t *testing.T) {
	SetSiteLedger("meluxina", "p1", "gpu", 10, 20, 30)
	for kind, want := range map[string]float64{KindUsed: 10, KindBooked: 20, KindLeft: 30} {
		if v := testutil.ToFloat64(SiteNodeMinutes.WithLabelValues("meluxina", "p1", "gpu", kind)); v != want {
			t.Errorf("%s = %v, want %v", kind, v, want)
		}
	}
}

func TestMetricsAreOnTheControllerRuntimeRegistry(t *testing.T) {
	SetJobPhase("ns", "reg", "c", "gpu", "Pending")
	defer ForgetJob("ns", "reg")
	RunnerRestarts.Inc()
	mfs, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, mf := range mfs {
		seen[mf.GetName()] = true
	}
	for _, n := range []string{"kaos_hpc_job_phase", "kaos_hpc_runner_restarts_total"} {
		if !seen[n] {
			t.Errorf("%s is not on the registry", n)
		}
	}
}
