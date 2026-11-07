package sitestate

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type noWait struct{ n int }

func (w *noWait) Wait(context.Context) error { w.n++; return nil }

type recorder struct {
	mu    sync.Mutex
	calls []string
	hdr   http.Header
}

func (r *recorder) handler(reply func(ids []string) (int, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, q *http.Request) {
		r.mu.Lock()
		r.calls = append(r.calls, q.URL.RequestURI())
		r.hdr = q.Header.Clone()
		r.mu.Unlock()
		code, body := reply(strings.Split(q.URL.Query().Get("job_id"), ","))
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}
}

func site(u string) Site {
	return Site{Endpoint: u, RESTVersion: "v0.0.44", User: "u1", Token: "secret-jwt"}
}

func TestShouldAskOnceWithAnExplicitStringJobIDListAndTheSiteIdentity(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(func([]string) (int, string) {
		// The site answers job_id as a string (D-51) and the state as a list.
		return 200, `{"jobs":[{"job_id":"11","state":["PENDING"],"state_reason":"Priority"},{"job_id":12,"state":["RUNNING"],"comment":"kaos-state=taken host=9"}]}`
	}))
	defer srv.Close()
	w := &noWait{}
	got, err := (&Client{HTTP: srv.Client()}).States(context.Background(), site(srv.URL), w, []string{"11", "12"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 1 || w.n != 1 {
		t.Fatalf("calls %v, waits %d, want exactly one", rec.calls, w.n)
	}
	// A plain comma list: slurmrestd does not decode %2C (HTTP 422 live on MeluXina, 2026-11-07).
	if want := "/slurm/v0.0.44/jobs/state?job_id=11,12"; rec.calls[0] != want {
		t.Fatalf("request %q, want %q", rec.calls[0], want)
	}
	if rec.hdr.Get("X-SLURM-USER-NAME") != "u1" || rec.hdr.Get("X-SLURM-USER-TOKEN") != "secret-jwt" {
		t.Fatalf("identity headers: %v", rec.hdr)
	}
	if j := got["11"]; j.State[0] != "PENDING" || j.Reason != "Priority" || j.Comment != nil {
		t.Fatalf("job 11: %+v", j)
	}
	if j := got["12"]; j.State[0] != "RUNNING" || j.Comment == nil || !strings.Contains(*j.Comment, "taken") {
		t.Fatalf("job 12 (numeric id, comment): %+v", j)
	}
}

func TestShouldChunkALongListAndPassEveryChunkThroughTheBucket(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(func(ids []string) (int, string) {
		var parts []string
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf(`{"job_id":"%s","state":["RUNNING"]}`, id))
		}
		return 200, `{"jobs":[` + strings.Join(parts, ",") + `]}`
	}))
	defer srv.Close()
	var ids []string
	for i := 1; i <= 250; i++ {
		ids = append(ids, fmt.Sprint(i))
	}
	w := &noWait{}
	got, err := (&Client{HTTP: srv.Client(), Chunk: 100}).States(context.Background(), site(srv.URL), w, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 3 || w.n != 3 || len(got) != 250 {
		t.Fatalf("calls %d, waits %d, jobs %d; want 3, 3, 250", len(rec.calls), w.n, len(got))
	}
}

func TestShouldNeverKeepAJobThatWasNotAsked(t *testing.T) {
	srv := httptest.NewServer((&recorder{}).handler(func([]string) (int, string) {
		return 200, `{"jobs":[{"job_id":"1","state":["RUNNING"]},{"job_id":"999","state":["RUNNING"]}]}`
	}))
	defer srv.Close()
	got, err := (&Client{HTTP: srv.Client()}).States(context.Background(), site(srv.URL), &noWait{}, []string{"1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, leaked := got["999"]; leaked || len(got) != 1 {
		t.Fatalf("another user's job was kept: %v", got)
	}
}

func TestShouldFailTheWholePollOnAnyBadReply(t *testing.T) {
	for name, reply := range map[string]struct {
		code int
		body string
	}{
		"html_page_with_http_200": {200, `<html>blocked</html>`},
		"http_500":                {500, `{"jobs":[]}`},
		"http_401":                {401, `{}`},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer((&recorder{}).handler(func([]string) (int, string) { return reply.code, reply.body }))
			defer srv.Close()
			got, err := (&Client{HTTP: srv.Client()}).States(context.Background(), site(srv.URL), &noWait{}, []string{"1"})
			if err == nil || got != nil {
				t.Fatalf("got %v, %v; want an error and no map", got, err)
			}
		})
	}
}

func TestShouldNotPutTheTokenInAnError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens: a transport error
	_, err := (&Client{HTTP: &http.Client{Timeout: time.Second}}).States(context.Background(), site(url), &noWait{}, []string{"1"})
	if err == nil || strings.Contains(err.Error(), "secret-jwt") {
		t.Fatalf("error %v", err)
	}
}

func TestShouldAcceptTheDbStyleCommentObject(t *testing.T) {
	jobs, err := parse([]byte(`{"jobs":[{"job_id":"3","state":"PENDING","comment":{"job":"host=1"}}]}`))
	if err != nil || len(jobs) != 1 || *jobs[0].Comment != "host=1" || jobs[0].State[0] != "PENDING" {
		t.Fatalf("%+v %v", jobs, err)
	}
}

func TestBucketShouldAllowTheBurstThenSpaceCallsByTheRate(t *testing.T) {
	now := time.Unix(1000, 0)
	var slept time.Duration
	b := NewBucket(2, 3) // 2 per second, burst 3
	b.last = now
	b.Now = func() time.Time { return now }
	b.Sleep = func(_ context.Context, d time.Duration) error { slept += d; now = now.Add(d); return nil }
	for i := 0; i < 3; i++ {
		if err := b.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if slept != 0 {
		t.Fatalf("the burst slept %v", slept)
	}
	for i := 0; i < 4; i++ {
		_ = b.Wait(context.Background())
	}
	if want := 2 * time.Second; slept < want-10*time.Millisecond || slept > want+10*time.Millisecond {
		t.Fatalf("4 calls past the burst at 2/s slept %v, want ~%v", slept, want)
	}
}

func TestBucketShouldStopWaitingWhenTheContextEnds(t *testing.T) {
	b := NewBucket(0.001, 1)
	_ = b.Wait(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Wait(ctx); err == nil {
		t.Fatal("want the context's error")
	}
}

// Live on MeluXina 2026-11-07: one unknown ID (a guest's job already purged) makes the site reject
// the whole list with 422. The poll then splits the list until the unknown ID stands alone, skips it
// and keeps the rest.
func TestShouldSplitTheListWhenOneUnknownIDFailsIt(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(func(ids []string) (int, string) {
		for _, id := range ids {
			if id == "7" {
				return 422, `{"errors":[{"description":"Invalid job id specified","error":"Invalid job id specified"}]}`
			}
		}
		return 200, fmt.Sprintf(`{"jobs":[{"job_id":"%s","state":["PENDING"]}]}`, ids[0])
	}))
	defer srv.Close()
	w := &noWait{}
	got, err := (&Client{HTTP: srv.Client()}).States(context.Background(), site(srv.URL), w, []string{"5", "7", "9"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got["5"].State) == 0 || len(got["9"].State) == 0 {
		t.Fatalf("got %v, want 5 and 9", got)
	}
	if len(rec.calls) != 5 || w.n != 5 {
		t.Fatalf("calls %v, waits %d: want the list, then its halves, every call through the bucket", rec.calls, w.n)
	}
}

// Regression (found by the scale test): one unknown ID in a list of 100 made the poll walk the list one ID
// at a time, 101 calls. At the production rate (2 calls per second) that outlasts the 30 s poll interval,
// the poll is abandoned, and no job of the list gets an observation while the unknown ID stays in it.
func TestShouldFindOneUnknownIDInAFullListInAboutTwoLog2NCalls(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(func(ids []string) (int, string) {
		parts := make([]string, 0, len(ids))
		for _, id := range ids {
			if id == "57" {
				return 422, `{"errors":[{"description":"Invalid job id specified"}]}`
			}
			parts = append(parts, fmt.Sprintf(`{"job_id":"%s","state":["RUNNING"]}`, id))
		}
		return 200, `{"jobs":[` + strings.Join(parts, ",") + `]}`
	}))
	defer srv.Close()
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = fmt.Sprint(i + 1)
	}
	w := &noWait{}
	got, err := (&Client{HTTP: srv.Client()}).States(context.Background(), site(srv.URL), w, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 99 || got["56"].State == nil || got["58"].State == nil || got["57"].State != nil {
		t.Fatalf("got %d jobs, want the 99 known ones and not 57", len(got))
	}
	if len(rec.calls) > 16 {
		t.Fatalf("%d calls for one unknown ID in 100, want at most 16 (1 + 2*7), the one-by-one walk made 101", len(rec.calls))
	}
}

func TestShouldReportTheSitesErrorTextWithoutTheToken(t *testing.T) {
	srv := httptest.NewServer((&recorder{}).handler(func([]string) (int, string) {
		return 500, `{"errors":[{"description":"slurmctld down secret-jwt","error":"Unable to contact slurm controller"}]}`
	}))
	defer srv.Close()
	_, err := (&Client{HTTP: srv.Client()}).States(context.Background(), site(srv.URL), &noWait{}, []string{"1", "2"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") || !strings.Contains(err.Error(), "slurmctld down") {
		t.Fatalf("err %v", err)
	}
	if strings.Contains(err.Error(), "secret-jwt") {
		t.Fatalf("token leaked: %v", err)
	}
}

func TestShouldRefuseAnIDThatIsNotDigits(t *testing.T) {
	_, err := (&Client{}).States(context.Background(), site("http://unused"), &noWait{}, []string{"1&x=y"})
	if err == nil || !strings.Contains(err.Error(), "not a job ID") {
		t.Fatalf("err %v", err)
	}
}

// Extreme cases (docs/extreme-cases.md, section 7).

func TestBucketShouldHoldTheRateWhenManyPollersWaitAtOnce(t *testing.T) {
	const waiters, rate = 21, 200.0 // burst 1: the 21 calls need at least 20 refills of 1/200 s
	b := NewBucket(rate, 1)
	var wg sync.WaitGroup
	t0 := time.Now()
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Wait(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if min := time.Duration(float64(waiters-1) / rate * float64(time.Second)); time.Since(t0) < min-5*time.Millisecond {
		t.Fatalf("%d concurrent calls passed in %v, the rate allows no less than %v", waiters, time.Since(t0), min)
	}
}

func TestShouldFailTheWholePollWhenTheSiteNeverAnswersAndStopAtTheContext(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	got, err := (&Client{HTTP: srv.Client()}).States(ctx, site(srv.URL), &noWait{}, []string{"1"})
	if err == nil || got != nil || time.Since(t0) > 5*time.Second {
		t.Fatalf("got %v, %v after %v; want an error and no map, quickly", got, err, time.Since(t0))
	}
}

func TestShouldDiscardEverythingWhenALaterChunkFails(t *testing.T) {
	calls := 0
	srv := httptest.NewServer((&recorder{}).handler(func(ids []string) (int, string) {
		calls++
		if calls == 2 {
			return 503, ""
		}
		var parts []string
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf(`{"job_id":"%s","state":["RUNNING"]}`, id))
		}
		return 200, `{"jobs":[` + strings.Join(parts, ",") + `]}`
	}))
	defer srv.Close()
	var ids []string
	for i := 1; i <= 250; i++ {
		ids = append(ids, fmt.Sprint(i))
	}
	got, err := (&Client{HTTP: srv.Client(), Chunk: 100}).States(context.Background(), site(srv.URL), &noWait{}, ids)
	if err == nil || got != nil {
		t.Fatalf("a partial picture was returned: %d jobs, %v", len(got), err)
	}
}
