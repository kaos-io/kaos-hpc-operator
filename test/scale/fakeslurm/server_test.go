package fakeslurm

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func client() *http.Client {
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test-only
}

func do(t *testing.T, s *Site, method, path, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, s.URL()+path, strings.NewReader(body))
	req.Header.Set("X-SLURM-USER-NAME", "u1")
	req.Header.Set("X-SLURM-USER-TOKEN", s.Opts.Token)
	resp, err := client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func submit(t *testing.T, s *Site, name, qos string, limit int, env ...string) int {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"job": map[string]any{"name": name, "qos": qos, "account": "a", "time_limit": limit, "environment": env}})
	code, out := do(t, s, http.MethodPost, "/slurm/v0.0.44/job/submit", string(b))
	if code != 200 {
		t.Fatalf("submit %s: %d %s", name, code, out)
	}
	var r struct {
		JobID int `json:"job_id"`
	}
	_ = json.Unmarshal([]byte(out), &r)
	return r.JobID
}

func TestShouldAnswerBatchedStateWithA422WhenOneIDIsUnknown(t *testing.T) {
	s := New(Options{Token: "t", Dilation: 600})
	defer s.Close()
	a, b := submit(t, s, "a", "default", 30), submit(t, s, "b", "default", 30)
	code, body := do(t, s, http.MethodGet, fmt.Sprintf("/slurm/v0.0.44/jobs/state?job_id=%d,%d", a, b), "")
	if code != 200 || !strings.Contains(body, `"job_id":"`) {
		t.Fatalf("known ids: %d %s", code, body)
	}
	if code, _ = do(t, s, http.MethodGet, fmt.Sprintf("/slurm/v0.0.44/jobs/state?job_id=%d,999", a), ""); code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown id: got %d, want 422", code)
	}
	if st := s.Snapshot(); st.State422 != 1 || st.Calls["state"] != 2 || st.StateIDs != 4 {
		t.Fatalf("stats %+v", st)
	}
}

func TestShouldRefuseASecondJobInAOneJobLaneAndAcceptItOnTheDefaultQoS(t *testing.T) {
	s := New(Options{Token: "t", QOS: []QOS{{Name: "default"}, {Name: "test", MaxWallMinutes: 30, MaxJobsPerUser: 1}}})
	defer s.Close()
	submit(t, s, "a", "test", 20)
	b, _ := json.Marshal(map[string]any{"job": map[string]any{"name": "b", "qos": "test", "time_limit": 20}})
	if code, body := do(t, s, http.MethodPost, "/slurm/v0.0.44/job/submit", string(b)); code != 400 || !strings.Contains(body, "QOS") {
		t.Fatalf("second job in the lane: %d %s", code, body)
	}
	submit(t, s, "b", "default", 20)
	if s.Snapshot().Refusals != 1 {
		t.Fatal("the refusal is not counted")
	}
}

func TestShouldRunAJobToItsPlannedEndAndKeepItInAccountingAfterSlurmctldForgetsIt(t *testing.T) {
	s := New(Options{Token: "t", Dilation: 600, MinJobAge: 50 * time.Millisecond}) // 1 real second = 10 minutes
	defer s.Close()
	id := submit(t, s, "a", "default", 60, "FAKE_RUN_MINUTES=1", "FAKE_FORCE_END=NODE_FAIL")
	time.Sleep(600 * time.Millisecond)
	_, body := do(t, s, http.MethodGet, fmt.Sprintf("/slurmdb/v0.0.44/job/%d", id), "")
	if !strings.Contains(body, "NODE_FAIL") {
		t.Fatalf("accounting: %s", body)
	}
	if code, _ := do(t, s, http.MethodGet, fmt.Sprintf("/slurm/v0.0.44/jobs/state?job_id=%d", id), ""); code != http.StatusUnprocessableEntity {
		t.Fatalf("a finished job older than MinJobAge must be unknown to slurmctld, got %d", code)
	}
}

func TestShouldAnswer503WhileTheSiteIsDown(t *testing.T) {
	s := New(Options{Token: "t"})
	defer s.Close()
	s.SetDown(true)
	if code, _ := do(t, s, http.MethodGet, "/slurm/v0.0.44/jobs/state?job_id=1", ""); code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", code)
	}
	s.SetDown(false)
	if code, _ := do(t, s, http.MethodGet, "/slurm/v0.0.44/jobs?name=x", ""); code != 200 {
		t.Fatalf("got %d", code)
	}
}

func TestShouldPlanTheSameRunForTheSameAttempt(t *testing.T) {
	a := New(Options{Token: "t", Seed: 5, NodeFailRate: 0.5})
	b := New(Options{Token: "t", Seed: 5, NodeFailRate: 0.5})
	defer a.Close()
	defer b.Close()
	for i := 0; i < 20; i++ {
		n := fmt.Sprintf("job-%d", i)
		ia, ib := submit(t, a, n, "default", 60, "KAOS_ATTEMPT=1"), submit(t, b, n, "default", 60, "KAOS_ATTEMPT=1")
		a.mu.Lock()
		b.mu.Lock()
		if a.jobs[ia].endState != b.jobs[ib].endState || a.jobs[ia].runReal != b.jobs[ib].runReal {
			t.Fatalf("%s planned differently", n)
		}
		b.mu.Unlock()
		a.mu.Unlock()
	}
}

func TestShouldListAJobFromAccountingByNameAfterSlurmctldForgotIt(t *testing.T) {
	s := New(Options{Token: "t", Dilation: 600, MinJobAge: time.Nanosecond, LostReplyRate: 1, LostReplyQuick: 1})
	defer s.Close()
	b, _ := json.Marshal(map[string]any{"job": map[string]any{"name": "kaos-x-a1", "account": "a", "time_limit": 30}})
	if code, _ := do(t, s, http.MethodPost, "/slurm/v0.0.44/job/submit", string(b)); code != http.StatusGatewayTimeout {
		t.Fatalf("a lost reply must be a 504, got %d", code)
	}
	var out string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, out = do(t, s, http.MethodGet, "/slurmdb/v0.0.44/jobs?job_name=kaos-x-a1&start_time=1", "")
		if strings.Contains(out, `"current":["COMPLETED"]`) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(out, `"job_id":4000000`) || !strings.Contains(out, `"COMPLETED"`) {
		t.Fatalf("the quick lost-reply job must end COMPLETED and be listed by name: %s", out)
	}
	if _, other := do(t, s, http.MethodGet, "/slurmdb/v0.0.44/jobs?job_name=other", ""); strings.Contains(other, "4000000") {
		t.Fatalf("a different name must not match: %s", other)
	}
	if _, live := do(t, s, http.MethodGet, "/slurm/v0.0.44/jobs?name=kaos-x-a1", ""); strings.Contains(live, `"job_state":["PENDING"]`) ||
		strings.Contains(live, `"job_state":["RUNNING"]`) {
		t.Fatalf("the job is over: slurmctld lists it as live: %s", live)
	}
	if st := s.Snapshot(); st.QuickLost != 1 || st.LostReplies != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestShouldAnswerPingAndFailItWhileTheSiteIsDown(t *testing.T) {
	s := New(Options{Token: "t"})
	defer s.Close()
	if code, body := do(t, s, http.MethodGet, "/slurm/v0.0.44/ping", ""); code != 200 || !strings.Contains(body, `"responding":true`) {
		t.Fatalf("ping: %d %s", code, body)
	}
	s.SetDown(true)
	if code, _ := do(t, s, http.MethodGet, "/slurm/v0.0.44/ping", ""); code != http.StatusServiceUnavailable {
		t.Fatalf("ping while down: %d", code)
	}
}

// D-26 (live 2026-11-10, host 5439966 and guest 5439976): a host's job keeps its node after its step while a
// guest runs on it, and the site charges the host for all of it; the guest's own job never starts and costs nothing.
func TestShouldKeepAHostRunningForItsGuestAndChargeOnlyTheHost(t *testing.T) {
	s := New(Options{Token: "t", Dilation: 600}) // 1 real second = 10 minutes
	defer s.Close()
	host := submit(t, s, "h", "default", 60, "FAKE_RUN_MINUTES=2", "FAKE_HOST_TAIL_MINUTES=18")
	guest := submit(t, s, "g", "default", 60, "FAKE_RUN_MINUTES=15", "FAKE_GUEST=1")
	job := func(id int) Job { s.mu.Lock(); defer s.mu.Unlock(); return *s.jobs[id] }
	time.Sleep(400 * time.Millisecond) // 4 minutes: the host's step is over, its job runs on
	h, g := job(host), job(guest)
	if h.State != Running || !strings.Contains(h.Cm, "kaos-state=done rc=0") {
		t.Fatalf("host after its step: %s %q, want RUNNING with its step's result in the comment", h.State, h.Cm)
	}
	if g.State != Pending || g.Reason != "JobHeldUser" || !strings.Contains(g.Cm, fmt.Sprintf("kaos-state=taken host=%d slot=1", host)) {
		t.Fatalf("guest: %s %s %q, want held and taken by the host", g.State, g.Reason, g.Cm)
	}
	time.Sleep(2 * time.Second) // 24 minutes: both are over
	h, g = job(host), job(guest)
	if h.State != Completed || g.State != Cancelled || !strings.Contains(g.Cm, fmt.Sprintf("kaos-state=done rc=0 host=%d", host)) {
		t.Fatalf("host %s, guest %s %q", h.State, g.State, g.Cm)
	}
	if ran := h.EndAt.Sub(h.StartAt).Minutes(); ran < 19.9 || ran > 20.1 {
		t.Fatalf("the host job ran %.1f minutes, want its step plus its tail (20)", ran)
	}
	if !g.StartAt.IsZero() {
		t.Fatal("a guest's own job never starts")
	}
	if c := s.Charged("a"); c < 19.9 || c > 20.1 {
		t.Fatalf("charged %.1f node-minutes, want the host's 20 and nothing for the guest", c)
	}
}
