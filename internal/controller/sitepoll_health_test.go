package controller

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

// pingReader is a StateReader that is also a Pinger.
type pingReader struct {
	*fakeReader
	verdict sitestate.Verdict
	pings   int
}

func (p *pingReader) Ping(context.Context, sitestate.Site, sitestate.Waiter) (sitestate.Verdict, string) {
	p.pings++
	return p.verdict, "HTTP 503"
}

func healthRig(t *testing.T, objs ...client.Object) (*pollRig, *pingReader, string) {
	t.Helper()
	r := newPollRig(t, objs...)
	pr := &pingReader{fakeReader: r.reader}
	r.poller.Reader = pr
	r.poller.Health = sitestate.NewHealth()
	return r, pr, client.ObjectKey{Namespace: "hpc", Name: "meluxina"}.String()
}

func waitingJob(ns, name string) *hpcv1alpha1.HPCArgoWorkflowJob {
	return pollJob(ns, name, "hpc", "", func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		j.Status.Phase, j.Status.Site, j.Status.Run = hpcv1alpha1.PhaseWaiting, nil, nil
	})
}

func TestShouldProbeASiteThatOnlyHasJobsWaitingForIt(t *testing.T) {
	r, pr, key := healthRig(t, append(pollSite("hpc", "https://s.example"), waitingJob("env", "w1"))...)
	pr.verdict = sitestate.Down
	r.poller.PollOnce(context.Background())
	r.now = r.now.Add(sitestate.MinDownAfter)
	r.poller.PollOnce(context.Background())
	if pr.pings != 2 || len(r.reader.calls) != 0 {
		t.Fatalf("pings %d, state calls %d: the idle site is pinged once per poll and jobs/state is not called", pr.pings, len(r.reader.calls))
	}
	if _, down := r.poller.Health.Down(key); !down {
		t.Fatal("two failed probes in a row must mark the site down")
	}
	pr.verdict = sitestate.Up
	r.poller.PollOnce(context.Background())
	if _, down := r.poller.Health.Down(key); down {
		t.Fatal("an answered probe must end the outage")
	}
}

func TestShouldNotProbeWhenNoJobIsUnfinished(t *testing.T) {
	done := pollJob("env", "done", "hpc", "1", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.Phase = hpcv1alpha1.PhaseSucceeded })
	r, pr, _ := healthRig(t, append(pollSite("hpc", "https://s.example"), done)...)
	r.poller.PollOnce(context.Background())
	if pr.pings != 0 {
		t.Fatalf("pings %d for a cluster with nothing to run", pr.pings)
	}
}

func TestShouldTakeAFailedStateReadAsAnOutageAndASuccessfulOneAsUp(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		down bool
	}{
		"should_mark_down_when_jobs_state_gets_a_connection_error": {&net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		"should_not_mark_down_when_jobs_state_is_refused_with_401": {errors.New("jobs/state: HTTP 401"), false},
	} {
		t.Run(name, func(t *testing.T) {
			r, _, key := healthRig(t, append(pollSite("hpc", "https://s.example"), pollJob("env", "a", "hpc", "1"))...)
			r.reader.err = tc.err
			r.poller.PollOnce(context.Background())
			r.now = r.now.Add(sitestate.MinDownAfter)
			r.poller.PollOnce(context.Background())
			if _, down := r.poller.Health.Down(key); down != tc.down {
				t.Fatalf("down = %v, want %v", down, tc.down)
			}
		})
	}
	r, _, key := healthRig(t, append(pollSite("hpc", "https://s.example"), pollJob("env", "a", "hpc", "1"))...)
	r.poller.Health.Observe(key, sitestate.Down, "", r.now)
	r.poller.Health.Observe(key, sitestate.Down, "", r.now.Add(sitestate.MinDownAfter))
	r.poller.PollOnce(context.Background())
	if _, down := r.poller.Health.Down(key); down {
		t.Fatal("a successful read of the site is an answer")
	}
}

func TestShouldLeaveHealthAloneWithoutAHealthStore(t *testing.T) {
	r := newPollRig(t, append(pollSite("hpc", "https://s.example"), waitingJob("env", "w1"))...)
	r.poller.Reader = &pingReader{fakeReader: r.reader, verdict: sitestate.Down}
	r.poller.PollOnce(context.Background()) // Health is nil: must not panic or probe
}

// A site that answers every read with 422 (it forgot the job IDs) is up: regression for the 10-minute outage
// scale run, where SiteReachable stayed False after recovery because only a successful read marked the site up.
func TestShouldMarkSiteUpWhenItAnswersWithA4xxRefusal(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"errors":[{"description":"Invalid job id specified"}]}`, http.StatusUnprocessableEntity)
	}))
	defer srv.Close()
	r, _, key := healthRig(t, append(pollSite("hpc", srv.URL), pollJob("env", "a", "hpc", "1"))...)
	r.poller.Reader = &sitestate.Client{HTTP: srv.Client()}
	r.poller.Health.Observe(key, sitestate.Down, "503", r.now)
	r.poller.Health.Observe(key, sitestate.Down, "503", r.now.Add(sitestate.MinDownAfter))
	if _, down := r.poller.Health.Down(key); !down {
		t.Fatal("precondition: the site is down")
	}
	r.poller.PollOnce(context.Background())
	if _, down := r.poller.Health.Down(key); down {
		t.Fatal("a 422 reply is the site answering: it must be marked up")
	}
}

// Live 2026-11-08: jobs/state answered HTTP 500 for minutes during a 12-job burst while submits and pings worked.
// A failed read of a site whose ping answers is not an outage, however long it lasts.
func TestShouldNotMarkSiteDownWhenJobsStateFailsButThePingAnswers(t *testing.T) {
	r, pr, key := healthRig(t, append(pollSite("hpc", "https://s.example"), pollJob("env", "a", "hpc", "1"))...)
	r.reader.err = &net.OpError{Op: "read", Err: errors.New("i/o timeout")}
	pr.verdict = sitestate.Up
	for range 20 {
		r.poller.PollOnce(context.Background())
		r.now = r.now.Add(30 * time.Second)
	}
	if _, down := r.poller.Health.Down(key); down {
		t.Fatal("a site that answers its ping must not be down because one call keeps failing")
	}
	if pr.pings != 20 {
		t.Fatalf("pings %d, want one confirmation per failed read", pr.pings)
	}
	pr.verdict = sitestate.Down
	for range 4 {
		r.poller.PollOnce(context.Background())
		r.now = r.now.Add(30 * time.Second)
	}
	if _, down := r.poller.Health.Down(key); !down {
		t.Fatal("a failed read and a failed ping for over MinDownAfter must mark the site down")
	}
}

// A site that 503s half of its calls under load but answers the rest is flapping, not down (scale fake).
func TestShouldNotMarkAFlappingSiteDown(t *testing.T) {
	site := fakeslurm.New(fakeslurm.Options{Seed: 7, Err5xx: 0.5})
	defer site.Close()
	r, _, key := healthRig(t, append(pollSite("hpc", site.URL()), pollJob("env", "a", "hpc", "4000001"))...)
	r.poller.Reader = &sitestate.Client{HTTP: site.Srv.Client()}
	for range 60 {
		r.poller.PollOnce(context.Background())
		if _, down := r.poller.Health.Down(key); down {
			t.Fatalf("flapping site marked down at %s", r.now)
		}
		r.now = r.now.Add(30 * time.Second)
	}
}
