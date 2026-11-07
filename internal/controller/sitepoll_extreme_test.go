package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
)

// Extreme cases of the site poller (docs/extreme-cases.md, sections 2 and 7).

// perEndpointReader fails for the endpoints in down and answers every job RUNNING for the others.
type perEndpointReader struct {
	down  map[string]bool
	calls map[string]int
}

func (p *perEndpointReader) States(_ context.Context, s sitestate.Site, _ sitestate.Waiter, ids []string) (map[string]sitestate.Job, error) {
	p.calls[s.Endpoint]++
	if p.down[s.Endpoint] {
		return nil, errors.New("jobs/state: HTTP 503")
	}
	out := map[string]sitestate.Job{}
	for _, id := range ids {
		out[id] = sitestate.Job{ID: id, State: []string{siteStateRunning}}
	}
	return out, nil
}

func TestShouldKeepPollingTheOtherSitesWhenOneSiteIsDownOrItsHPCSiteIsGone(t *testing.T) {
	objs := append(pollSite("hpc-a", "https://a.example"), pollSite("hpc-b", "https://b.example")...)
	objs = append(objs, pollJob("ea", "a1", "hpc-a", "1"), pollJob("eb", "b1", "hpc-b", "2"),
		pollJob("ec", "c1", "hpc-gone", "3")) // c1's HPCSite was deleted
	r := newPollRig(t, objs...)
	pr := &perEndpointReader{down: map[string]bool{"https://a.example": true}, calls: map[string]int{}}
	r.poller.Reader = pr
	r.poller.PollOnce(context.Background())
	var a, b hpcv1alpha1.HPCArgoWorkflowJob
	if err := r.c.Get(context.Background(), client.ObjectKey{Namespace: "ea", Name: "a1"}, &a); err != nil {
		t.Fatal(err)
	}
	if err := r.c.Get(context.Background(), client.ObjectKey{Namespace: "eb", Name: "b1"}, &b); err != nil {
		t.Fatal(err)
	}
	if a.Status.SiteObservation != nil {
		t.Fatalf("site A was down: its job must keep its status, got %+v", a.Status.SiteObservation)
	}
	if b.Status.SiteObservation == nil || b.Status.SiteObservation.State != siteStateRunning {
		t.Fatalf("site B must still be polled: %+v", b.Status.SiteObservation)
	}
}

func TestShouldUseTheRotatedJWTOnTheNextPollWithoutARestart(t *testing.T) {
	objs := append(pollSite("hpc", "https://s.example"), pollJob("env", "j", "hpc", "1"))
	r := newPollRig(t, objs...)
	r.poller.PollOnce(context.Background())
	var sec corev1.Secret
	key := client.ObjectKey{Namespace: "hpc", Name: "meluxina-jwt"}
	if err := r.c.Get(context.Background(), key, &sec); err != nil {
		t.Fatal(err)
	}
	sec.Data["token"] = []byte("rotated-jwt\n")
	if err := r.c.Update(context.Background(), &sec); err != nil {
		t.Fatal(err)
	}
	r.poller.PollOnce(context.Background())
	if len(r.reader.sites) != 2 || r.reader.sites[0].Token != "jwt-hpc" || r.reader.sites[1].Token != "rotated-jwt" {
		t.Fatalf("tokens used: %+v", r.reader.sites)
	}
}

// fakeClock is a bucket clock that moves only when the bucket sleeps, so a test never waits.
type fakeClock struct {
	mu     sync.Mutex
	t      time.Time
	sleeps []time.Duration
}

func (f *fakeClock) now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.t }

func (f *fakeClock) sleep(_ context.Context, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sleeps = append(f.sleeps, d)
	f.t = f.t.Add(d)
	return nil
}

func TestShouldPollThreeHundredJobsWithThreeCallsOfAHundredThroughTheTokenBucket(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ids := strings.Split(req.URL.Query().Get("job_id"), ",")
		mu.Lock()
		sizes = append(sizes, len(ids))
		mu.Unlock()
		var parts []string
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf(`{"job_id":"%s","state":["RUNNING"]}`, id))
		}
		_, _ = fmt.Fprintf(w, `{"jobs":[%s]}`, strings.Join(parts, ","))
	}))
	defer srv.Close()

	objs := pollSite("hpc", srv.URL)
	for i := 1; i <= 300; i++ {
		objs = append(objs, pollJob("env", fmt.Sprintf("j%d", i), "hpc", fmt.Sprint(1000+i)))
	}
	r := newPollRig(t, objs...)
	r.poller.Reader = &sitestate.Client{HTTP: srv.Client()}
	b := sitestate.NewBucket(1, 1) // one call per second, no burst; starts full
	clk := &fakeClock{t: time.Now()}
	b.Now, b.Sleep = clk.now, clk.sleep
	r.poller.buckets = map[string]*sitestate.Bucket{"meluxina|" + srv.URL: b}

	r.poller.PollOnce(context.Background())

	if fmt.Sprint(sizes) != "[100 100 100]" {
		t.Fatalf("call sizes %v, want three calls of 100 ids", sizes)
	}
	if len(clk.sleeps) != 2 {
		t.Fatalf("the bucket slept %v: the first call takes the full bucket's token, the other two wait for theirs", clk.sleeps)
	}
	for _, d := range clk.sleeps {
		if d < 900*time.Millisecond || d > 1100*time.Millisecond {
			t.Fatalf("a call waited %v for its token, want ~1s at 1 call/s", d)
		}
	}
	if o := r.job(t, "j300").Status.SiteObservation; o == nil || o.State != siteStateRunning {
		t.Fatalf("job 300 observation: %+v", o)
	}
}

func TestShouldReturnWithinTheIntervalWhenTheSiteNeverAnswersAndLeaveEveryStatusAlone(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	objs := append(pollSite("hpc", srv.URL), pollJob("env", "j", "hpc", "1"))
	r := newPollRig(t, objs...)
	r.poller.Interval = 300 * time.Millisecond
	r.poller.Reader = &sitestate.Client{HTTP: srv.Client()}
	t0 := time.Now()
	r.poller.PollOnce(context.Background())
	if d := time.Since(t0); d > 5*time.Second {
		t.Fatalf("PollOnce took %v against a hung site: it must be bounded by the interval", d)
	}
	if r.job(t, "j").Status.SiteObservation != nil {
		t.Fatal("a hung site must not produce an observation")
	}
}

func TestShouldTreatAnHTMLPageWithHTTP200AsAFailedPoll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html><body>Request blocked by the site allow-list</body></html>"))
	}))
	defer srv.Close()
	old := &hpcv1alpha1.SiteObservation{JobID: "1", State: siteStateRunning, ObservedAt: metav1.NewTime(time.Unix(5, 0))}
	objs := append(pollSite("hpc", srv.URL),
		pollJob("env", "j", "hpc", "1", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.SiteObservation = old }))
	r := newPollRig(t, objs...)
	r.poller.Reader = &sitestate.Client{HTTP: srv.Client()}
	r.poller.PollOnce(context.Background())
	if got := r.job(t, "j").Status.SiteObservation; got == nil || !got.ObservedAt.Time.Equal(time.Unix(5, 0)) {
		t.Fatalf("the HTML page overwrote the observation: %+v", got)
	}
}
