package sitestate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DefaultChunk is how many job IDs one jobs/state call carries.
const DefaultChunk = 100

// maxBody bounds what one reply may weigh (an explicit ID list keeps real replies small).
const maxBody = 4 << 20

// Job is the site's reading of one job.
type Job struct {
	ID    string
	State []string
	// Reason is Slurm's state reason, "" when the site does not report one.
	Reason string
	// Comment is the job comment, nil when the site does not report one.
	Comment *string
}

// Site names the endpoint and the identity of one HPCSite for a call.
type Site struct {
	Endpoint, RESTVersion, User, Token string
}

// Waiter takes one rate-limit token per REST call.
type Waiter interface {
	Wait(ctx context.Context) error
}

// Client calls jobs/state.
type Client struct {
	HTTP  *http.Client
	Chunk int
}

// States asks the site for the given job IDs, in chunks, one call per chunk, each after bucket.Wait.
// The answer holds only IDs that were asked for (D-45: a site that returns other jobs does not get
// them stored). IDs the site does not list are absent from the map. The first failing call ends the
// poll with an error and the map read so far is discarded: a partial picture is never written.
func (c *Client) States(ctx context.Context, s Site, bucket Waiter, ids []string) (map[string]Job, error) {
	out := make(map[string]Job, len(ids))
	chunk := c.Chunk
	if chunk <= 0 {
		chunk = DefaultChunk
	}
	for from := 0; from < len(ids); from += chunk {
		part := ids[from:min(from+chunk, len(ids))]
		if err := bucket.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limit: %w", err)
		}
		jobs, err := c.call(ctx, s, part)
		var se *statusError
		if errors.As(err, &se) && se.code == http.StatusUnprocessableEntity && len(part) > 1 {
			// The site rejects the whole list when one ID is unknown to it (a guest's job already
			// cancelled and purged): split the list until the unknown IDs stand alone, so they never
			// hide the others' state and cost about 2*log2(n) calls each, not n.
			jobs, err = c.split(ctx, s, bucket, part)
		}
		if err != nil {
			return nil, err
		}
		asked := make(map[string]bool, len(part))
		for _, id := range part {
			asked[id] = true
		}
		for _, j := range jobs {
			if asked[j.ID] {
				out[j.ID] = j
			}
		}
	}
	return out, nil
}

// split asks for a list the site rejected with 422 in two halves, and each half that is rejected in
// two again, down to single IDs. An ID the site does not know is skipped (the caller reads it as "not
// listed"); any other error stops. One unknown ID in a list of 100 costs about 14 calls, not 100: at
// the production rate of 2 calls per second the old one-by-one walk took longer than the poll interval,
// so the whole list's observations were never written while the unknown ID stayed in it.
func (c *Client) split(ctx context.Context, s Site, bucket Waiter, ids []string) ([]Job, error) {
	if len(ids) <= 1 {
		return nil, nil // a single ID that was rejected is the unknown one
	}
	var out []Job
	for _, half := range [][]string{ids[:len(ids)/2], ids[len(ids)/2:]} {
		if err := bucket.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limit: %w", err)
		}
		jobs, err := c.call(ctx, s, half)
		var se *statusError
		if errors.As(err, &se) && se.code == http.StatusUnprocessableEntity {
			jobs, err = c.split(ctx, s, bucket, half)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, jobs...)
	}
	return out, nil
}

// statusError is a non-2xx reply; text is the site's own error description (never a token).
type statusError struct {
	code int
	text string
}

func (e *statusError) Error() string {
	if e.text == "" {
		return fmt.Sprintf("jobs/state: HTTP %d", e.code)
	}
	return fmt.Sprintf("jobs/state: HTTP %d: %s", e.code, e.text)
}

// siteErrorText is the first error description of a Slurm REST error reply, bounded.
func siteErrorText(body []byte) string {
	var r struct {
		Errors []struct {
			Description string `json:"description"`
			Error       string `json:"error"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &r) != nil || len(r.Errors) == 0 {
		return ""
	}
	t := strings.TrimSpace(r.Errors[0].Description + " " + r.Errors[0].Error)
	if len(t) > 200 {
		t = t[:200]
	}
	return t
}

func (c *Client) call(ctx context.Context, s Site, ids []string) ([]Job, error) {
	// job_id is always explicit (D-45): unfiltered, the site lists jobs we must not read. A plain
	// comma list: slurmrestd reads the query itself and does not decode an escaped comma (%2C), so
	// "1%2C2" was one invalid ID and the whole call failed with 422 (live on MeluXina 2026-11-07).
	for _, id := range ids {
		if !isJobID(id) {
			return nil, fmt.Errorf("jobs/state: %q is not a job ID", id)
		}
	}
	u := fmt.Sprintf("%s/slurm/%s/jobs/state?job_id=%s", strings.TrimSuffix(s.Endpoint, "/"), s.RESTVersion,
		strings.Join(ids, ","))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("jobs/state request: %w", err)
	}
	req.Header.Set("X-SLURM-USER-NAME", s.User)
	req.Header.Set("X-SLURM-USER-TOKEN", s.Token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jobs/state: %w", redact(err, s.Token))
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("jobs/state body: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, &statusError{code: resp.StatusCode, text: redactText(siteErrorText(body), s.Token)}
	}
	return parse(body)
}

// redact keeps a token out of an error text.
func redact(err error, token string) error {
	if token == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), token, "***"))
}

// wire is the reply. The site returns job_id as a string where the bundled spec says integer
// (D-51), so it is read raw and either form is accepted; state, reason and comment tolerate the
// shapes the REST versions use.
type wire struct {
	Jobs []struct {
		JobID   json.RawMessage `json:"job_id"`
		State   json.RawMessage `json:"state"`
		Reason  string          `json:"state_reason"`
		Comment json.RawMessage `json:"comment"`
	} `json:"jobs"`
}

// parse reads a jobs/state reply. Anything that is not JSON is an error whatever the HTTP status
// (D-45: the allow-list block is an HTML page with HTTP 200).
func parse(body []byte) ([]Job, error) {
	var w wire
	if err := json.Unmarshal(bytes.TrimSpace(body), &w); err != nil {
		return nil, fmt.Errorf("jobs/state: reply is not JSON: %w", err)
	}
	jobs := make([]Job, 0, len(w.Jobs))
	for _, r := range w.Jobs {
		id := strings.Trim(strings.TrimSpace(string(r.JobID)), `"`)
		if id == "" || id == "null" {
			continue
		}
		jobs = append(jobs, Job{ID: id, State: states(r.State), Reason: r.Reason, Comment: comment(r.Comment)})
	}
	return jobs, nil
}

func states(raw json.RawMessage) []string {
	var list []string
	if json.Unmarshal(raw, &list) == nil {
		return list
	}
	var one string
	if json.Unmarshal(raw, &one) == nil && one != "" {
		return []string{one}
	}
	return nil
}

// comment reads a job comment: a string (slurmctld) or {"job": ...} (slurmdbd).
func comment(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return &s
	}
	var o struct {
		Job string `json:"job"`
	}
	if json.Unmarshal(raw, &o) == nil {
		return &o.Job
	}
	return nil
}

// isJobID: digits only, so a plain comma list cannot carry anything else into the URL.
func isJobID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func redactText(t, token string) string {
	if token == "" {
		return t
	}
	return strings.ReplaceAll(t, token, "***")
}
