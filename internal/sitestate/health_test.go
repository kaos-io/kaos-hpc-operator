package sitestate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestShouldTellAnOutageFromARealError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"should_be_an_outage_on_http_503", &statusError{code: 503}, true},
		{"should_be_an_outage_on_http_500", &statusError{code: 500}, true},
		{"should_be_an_outage_on_http_429", &statusError{code: 429}, true},
		{"should_be_an_outage_on_a_timeout", fmt.Errorf("jobs/state: %w", context.DeadlineExceeded), true},
		{"should_be_an_outage_on_a_net_error", fmt.Errorf("jobs/state: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}), true},
		{"should_be_an_outage_on_a_refused_connection_text", errors.New(`Get "https://x": dial tcp: connection refused`), true},
		{"should_not_be_an_outage_on_http_401", &statusError{code: 401}, false},
		{"should_not_be_an_outage_on_http_422", &statusError{code: 422}, false},
		{"should_not_be_an_outage_on_an_html_reply", errors.New("jobs/state: reply is not JSON: invalid character '<'"), false},
		{"should_not_be_an_outage_when_only_the_poll_s_own_bucket_ran_dry", &rateLimitError{context.DeadlineExceeded}, false},
		{"should_not_be_an_outage_on_nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsOutage(c.err); got != c.want {
				t.Fatalf("IsOutage(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestShouldCallASiteDownOnlyAfterConsecutiveFailuresAndUpAtOnce(t *testing.T) {
	h, now := NewHealth(), time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	if h.Observe("s", Down, "503", now) {
		t.Fatal("one failed probe is a blip, not an outage")
	}
	if _, down := h.Down("s"); down {
		t.Fatal("down after one failure")
	}
	if !h.Observe("s", Down, "503 again", now.Add(time.Minute)) {
		t.Fatal("the second failure in a row must report the change")
	}
	out, down := h.Down("s")
	if !down || !out.Since.Equal(now.Add(time.Minute)) || out.Reason != "503 again" {
		t.Fatalf("outage = %+v down=%v", out, down)
	}
	h.Observe("s", Unknown, "", now.Add(2*time.Minute))
	if _, down := h.Down("s"); !down {
		t.Fatal("an inconclusive probe must not end an outage")
	}
	if !h.Observe("s", Up, "", now.Add(3*time.Minute)) {
		t.Fatal("up after an outage is a change")
	}
	if _, down := h.Down("s"); down {
		t.Fatal("still down after an answer")
	}
	h.Observe("s", Down, "", now)
	h.Observe("s", Up, "", now)
	h.Observe("s", Down, "", now)
	if _, down := h.Down("s"); down {
		t.Fatal("failures must be consecutive")
	}
}

func TestShouldReadANilHealthAsEverySiteUp(t *testing.T) {
	var h *Health
	if h.Observe("s", Down, "", time.Now()) {
		t.Fatal("nil Health changed")
	}
	if _, down := h.Down("s"); down {
		t.Fatal("nil Health reports an outage")
	}
}

func TestShouldPingTheSiteAndReadTheVerdict(t *testing.T) {
	cases := []struct {
		name string
		code int
		body string
		want Verdict
	}{
		{"should_be_up_when_a_controller_responds", 200, `{"pings":[{"hostname":"c1","pinged":"UP","responding":true}]}`, Up},
		{"should_be_down_when_no_controller_responds", 200, `{"pings":[{"hostname":"c1","pinged":"DOWN","responding":false}]}`, Down},
		{"should_be_down_on_503", 503, `{}`, Down},
		{"should_be_down_on_500", 500, `<html>`, Down},
		{"should_be_unknown_on_an_allow_list_page", 200, `<html>blocked</html>`, Unknown},
		{"should_be_unknown_when_the_site_has_no_ping", 404, `{}`, Unknown},
		{"should_be_unknown_on_403", 403, `{}`, Unknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/slurm/v0.0.44/ping" || r.Header.Get("X-SLURM-USER-TOKEN") != "secret-jwt" {
					t.Errorf("unexpected request %s", r.URL)
				}
				w.WriteHeader(c.code)
				_, _ = w.Write([]byte(c.body))
			}))
			defer srv.Close()
			w := &noWait{}
			v, _ := (&Client{HTTP: srv.Client()}).Ping(context.Background(), site(srv.URL), w)
			if v != c.want || w.n != 1 {
				t.Fatalf("verdict = %v (want %v), bucket takes = %d", v, c.want, w.n)
			}
		})
	}
}

func TestShouldReadAnUnreachableSiteAsDownAndNeverLeakTheToken(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens: connection refused
	v, why := (&Client{HTTP: &http.Client{Timeout: time.Second}}).Ping(context.Background(), site(url), &noWait{})
	if v != Down {
		t.Fatalf("verdict = %v (%s), want Down", v, why)
	}
	if contains(why, "secret-jwt") {
		t.Fatalf("token leaked: %s", why)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestIsAnswer(t *testing.T) {
	for name, c := range map[string]struct {
		err  error
		want bool
	}{
		"should_be_answer_for_422":       {&statusError{code: 422}, true},
		"should_be_answer_for_401":       {&statusError{code: 401}, true},
		"should_not_be_answer_for_503":   {&statusError{code: 503}, false},
		"should_not_be_answer_for_429":   {&statusError{code: 429}, false},
		"should_not_be_answer_for_nil":   {nil, false},
		"should_not_be_answer_for_other": {errors.New("boom"), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := IsAnswer(c.err); got != c.want {
				t.Fatalf("IsAnswer = %v, want %v", got, c.want)
			}
		})
	}
}
