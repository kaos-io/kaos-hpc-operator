package sitestate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// FailuresToDown is how many probes in a row must find the site unreachable before it counts as down. One
// blip (a single 503) must not put every waiting job on SiteUnavailable.
const FailuresToDown = 2

// Verdict is what one probe of the site concluded.
type Verdict int

const (
	// Unknown: the probe says nothing about reachability (the site answered, but not with what a ping looks like:
	// an allow-list page, 404, 403). It changes nothing.
	Unknown Verdict = iota
	// Up: the site's REST API answered.
	Up
	// Down: no connection, a timeout, a gateway or server error, or a ping that says no controller responds.
	Down
)

// IsOutage reports whether an error from a call to the site means the site is unreachable: a network error,
// a timeout (including the poll's own deadline) or an HTTP 5xx, 408 or 429. A 4xx (auth, bad request, unknown
// job) is the site answering and is not an outage.
func IsOutage(err error) bool {
	if err == nil {
		return false
	}
	var rl *rateLimitError
	if errors.As(err, &rl) {
		return false
	}
	var se *statusError
	if errors.As(err, &se) {
		return se.code >= 500 || se.code == http.StatusRequestTimeout || se.code == http.StatusTooManyRequests
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Connection refused/reset and DNS errors reach us wrapped as plain url.Error texts.
	t := strings.ToLower(err.Error())
	for _, m := range []string{"connection refused", "connection reset", "no such host", "i/o timeout"} {
		if strings.Contains(t, m) {
			return true
		}
	}
	return false
}

// IsAnswer reports whether an error from a call to the site is the site's own refusal: an HTTP 4xx other than
// 408 and 429 (unknown job ID, bad request, auth). The REST API is up, it just said no, so such an error proves
// reachability even though the call failed. A site that returns 422 for job IDs it forgot would otherwise
// never be seen as back after an outage.
func IsAnswer(err error) bool {
	var se *statusError
	return errors.As(err, &se) && se.code >= 400 && se.code < 500 &&
		se.code != http.StatusRequestTimeout && se.code != http.StatusTooManyRequests
}

// Ping asks the site's REST API whether it is there (GET /slurm/<version>/ping): the cheapest call that needs no
// job ID. It takes one token of the bucket. Unknown for any answer that is not a ping, so a site that does
// not offer the call (an allow-list that blocks it) is never read as down.
func (c *Client) Ping(ctx context.Context, s Site, bucket Waiter) (Verdict, string) {
	if err := bucket.Wait(ctx); err != nil {
		return Unknown, "rate limit: " + err.Error()
	}
	u := fmt.Sprintf("%s/slurm/%s/ping", strings.TrimSuffix(s.Endpoint, "/"), s.RESTVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Unknown, err.Error()
	}
	req.Header.Set("X-SLURM-USER-NAME", s.User)
	req.Header.Set("X-SLURM-USER-TOKEN", s.Token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if IsOutage(err) {
			return Down, redactText(err.Error(), s.Token)
		}
		return Unknown, redactText(err.Error(), s.Token)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	switch {
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests:
		return Down, fmt.Sprintf("ping: HTTP %d", resp.StatusCode)
	case resp.StatusCode/100 != 2:
		return Unknown, fmt.Sprintf("ping: HTTP %d", resp.StatusCode)
	}
	return pingVerdict(body)
}

// pingVerdict reads a ping reply: {"pings": [{"hostname", "pinged": "UP"|"DOWN", "responding": bool}]}. Down only
// when it lists controllers and none responds; a reply that is not JSON or lists none says nothing.
func pingVerdict(body []byte) (Verdict, string) {
	var r struct {
		Pings []struct {
			Hostname   string `json:"hostname"`
			Pinged     string `json:"pinged"`
			Responding *bool  `json:"responding"`
		} `json:"pings"`
	}
	if json.Unmarshal(body, &r) != nil || len(r.Pings) == 0 {
		return Unknown, "ping: not a ping reply"
	}
	for _, p := range r.Pings {
		if (p.Responding != nil && *p.Responding) || strings.EqualFold(p.Pinged, "UP") {
			return Up, ""
		}
	}
	return Down, "ping: no slurm controller responds"
}

// Outage describes a site that is down.
type Outage struct {
	Since  time.Time
	Reason string
}

type siteHealth struct {
	failures int
	down     bool
	since    time.Time
	reason   string
}

// Health is the controller's memory of which sites answer, written by the site poller and read by the
// reconciler (PRD-HPC-1461 AC-30). Safe for concurrent use. A nil *Health reports every site as up.
type Health struct {
	mu sync.Mutex
	m  map[string]*siteHealth
}

// NewHealth returns an empty Health: every site is up until probes say otherwise.
func NewHealth() *Health { return &Health{m: map[string]*siteHealth{}} }

// Observe records one probe of the site `key` and reports whether its up/down state changed. A Down verdict
// counts toward FailuresToDown; an Up verdict resets at once; Unknown changes nothing.
func (h *Health) Observe(key string, v Verdict, why string, now time.Time) (changed bool) {
	if h == nil || v == Unknown {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.m[key]
	if e == nil {
		e = &siteHealth{}
		h.m[key] = e
	}
	if v == Up {
		changed = e.down
		*e = siteHealth{}
		return changed
	}
	e.failures++
	e.reason = why
	if !e.down && e.failures >= FailuresToDown {
		e.down, e.since = true, now
		return true
	}
	return false
}

// Down returns the outage of the site `key`, ok false when the site is believed up.
func (h *Health) Down(key string) (Outage, bool) {
	if h == nil {
		return Outage{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.m[key]; e != nil && e.down {
		return Outage{Since: e.since, Reason: e.reason}, true
	}
	return Outage{}, false
}
