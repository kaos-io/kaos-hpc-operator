// Package fakeslurm is an in-memory Slurm REST site (slurmrestd + slurmdbd) for the scale tests of
// the kaos-hpc-operator. It speaks the calls the controller and the runner make, and nothing else:
//
//	controller:  GET  /slurm/<v>/jobs/state?job_id=1,2,3   (comma list; 422 when one ID is unknown)
//	runner:      POST /slurm/<v>/job/submit                (find-by-key first: GET /slurm/<v>/jobs?name=)
//	             GET  /slurm/<v>/job/<id>                  (404 when slurmctld forgot the job)
//	             DELETE /slurm/<v>/job/<id>                (cancel)
//	             GET  /slurmdb/<v>/job/<id>                (accounting: kept forever)
//	hub:         GET  /slurmdb/<v>/sshare, /slurmdb/<v>/qos, /slurm/<v>/reservations
//
// The hub-side reads (the last line) are not made by this repo's controller: the harness plays the
// hub with them, to render the HPCSite budget (siteLeft), QoS and reservations from the site.
//
// Jobs are simulated, not executed: a submit fixes a plan (run length, how it ends) from the job's
// environment hints (FAKE_*), the site's seeded random failure rates and its limits; a background
// ticker moves jobs through PENDING -> RUNNING -> an end state against a node capacity.
//
// Time dilation. Real seconds are cheap, node-minutes are the budget unit. The site reports job
// start and end times with the run time multiplied by Dilation, so a job that really ran 8 s is
// accounted as 8 minutes at Dilation 60. The start time is real (the controller rejects reported
// times more than an hour in its future), so a dilated run must stay below about 55 minutes.
package fakeslurm

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// keyState is the JSON key of a job state in the REST replies, and the name of the jobs/state call kind.
const keyState = "state"

// Slurm job states the fake produces.
const (
	Pending   = "PENDING"
	Running   = "RUNNING"
	Completed = "COMPLETED"
	Failed    = "FAILED"
	Timeout   = "TIMEOUT"
	NodeFail  = "NODE_FAIL"
	Preempted = "PREEMPTED"
	Cancelled = "CANCELLED"
)

// Environment hints a submitter may put in the job's environment to steer the simulation.
const (
	// EnvRunMinutes is the dilated run time of the job in minutes (default: random in Options.RunMin..RunMax).
	EnvRunMinutes = "FAKE_RUN_MINUTES"
	// EnvForceEnd makes every run of the job end in that state (NODE_FAIL, PREEMPTED, FAILED) at 60 % of its length.
	EnvForceEnd = "FAKE_FORCE_END"
	// EnvSegments makes the job a checkpointing step of that many segments: every run but the last
	// ends with exit 85 and leaves a checkpoint copy, the run that restores the last one completes.
	EnvSegments = "FAKE_SEGMENTS"
	// EnvHostTailMinutes makes the job a node-sharing host (PRD-HPC-1461 D-26): when its step ends the job
	// comment says so as the slot manager writes it ("kaos-state=done rc=N"), and the Slurm job keeps the node
	// this many more (dilated) minutes for its guests, never past its time limit. The site charges all of it.
	EnvHostTailMinutes = "FAKE_HOST_TAIL_MINUTES"
	// EnvGuest makes the job a guest: it never starts on its own. A running host of its account whose job
	// outlasts the guest's run takes it ("kaos-state=taken host=H slot=1", held), runs it, and cancels it
	// ("kaos-state=done rc=0 host=H start=<step start>"): the guest's own job is charged nothing.
	EnvGuest = "FAKE_GUEST"
	// EnvLeadMinutes is a resumable step's lead (dilated minutes before its end that it is asked to save,
	// PRD-HPC-1461 AC-73). A resumable guest is taken when its lead fits the host's remaining time, and
	// is signalled at the host job's end minus its lead: it saves and exits 85 ("kaos-state=done rc=85
	// host=H start=<start> kaos-ckpt-seq=N") instead of running on past its host.
	EnvLeadMinutes = "FAKE_LEAD_MINUTES"
	// EnvShareScope and EnvShareSlots make the job a node-sharing job as the runner submits it (PRD-1609,
	// PRD-HPC-1461 D-70): it carries a scope and its node has that many GPU slots. Once running it plays the
	// slot manager: it takes pending jobs of the same account, partition and scope (also one still waiting for
	// its BeginTime) into its free slots while their step ends before its own time limit, and keeps its node
	// until its last guest is done plus ShareIdle. A job submitted without a BeginTime in the future that finds
	// a free node simply starts: it is a host.
	EnvShareScope = "FAKE_SHARE_SCOPE"
	EnvShareSlots = "FAKE_SHARE_SLOTS"
	// EnvResumeRef is the checkpoint reference of the attempt (set by the controller via the runner).
	EnvResumeRef = "KAOS_RESUME_REF"
	// EnvAttempt is the controller's attempt number of the submission.
	EnvAttempt = "KAOS_ATTEMPT"
)

// ReasonHeld is the pending reason of a job a host took: held by the user (the slot manager).
const ReasonHeld = "JobHeldUser"

// ShareIdle is the dilated time a slot-manager host keeps its node after its last step (the slot manager's idle
// release, KAOS_IDLE_S 120 s) and ShareMargin the time a guest must end before the host's own limit (MARGIN_S 300 s).
const (
	ShareIdle   = 2 * time.Minute
	ShareMargin = 5 * time.Minute
)

// ExitCodeContinue is the step's "saved, continue me" exit code (PRD-HPC-1461 D-44).
const ExitCodeContinue = 85

// QOS is a quality of service of the fake site.
type QOS struct {
	Name           string
	MaxWallMinutes int
	// MaxJobsPerUser limits the pending plus running jobs one user holds in the QoS (0 = unlimited).
	MaxJobsPerUser int
}

// Options configure a Site. Zero values mean "off" unless a default is named.
type Options struct {
	Name, Version, Token string
	// Dilation multiplies real run time into reported time (default 60).
	Dilation float64
	// Capacity is how many jobs run at once (default 50).
	Capacity int
	// Latency and Jitter delay every request.
	Latency, Jitter time.Duration
	// Err5xx is the chance that a request is answered 503; TimeoutRate that it hangs for HangFor.
	Err5xx, TimeoutRate float64
	HangFor             time.Duration
	// LostReplyRate is the chance that a submission is accepted but its reply is lost (504): the
	// submitter must find the job by key instead of submitting it again. Such a job queues for at
	// least LostReplyHold, as a real job outlives the 10 s the runner pod waits before it retries (the
	// runner's adopt step finds ACTIVE jobs only: a job that finished in between would be submitted again).
	LostReplyRate float64
	LostReplyHold time.Duration
	// LostReplyQuick is the share of those lost-reply jobs that start at once (a free node) and end within
	// about a second, before the submitter asks again: the job is finished when the runner looks for it, so
	// only slurmdbd still lists it (found by name; the case that used to submit the attempt twice).
	LostReplyQuick float64
	// QueueMin..QueueMax is the real time a job waits before it may start (queue delay).
	QueueMin, QueueMax time.Duration
	// RunMin..RunMax is the dilated run time (minutes) of a job without a FAKE_RUN_MINUTES hint.
	RunMin, RunMax float64
	// Per-run chances of a random end: NODE_FAIL, PREEMPTED, FAILED (the step's own error), and a run
	// that outlasts its limit (TIMEOUT) are decided by the time limit and the hint, not by chance.
	NodeFailRate, PreemptRate, FailRate float64
	// MinJobAge is how long (real) a finished job stays known to slurmctld; after it jobs/state
	// answers 422 for the ID and only accounting still has the job (default 5 min).
	MinJobAge time.Duration
	// AcctLag is how long (real) slurmdbd lags behind slurmctld: for that long after a job ended, slurmctld
	// already says so (jobs/state) but the accounting record still shows the job as RUNNING, with no exit code
	// and no end. It is the late exit record of PRD-HPC-1461 T-14 (a 5-minute lag is the acceptance case).
	AcctLag time.Duration
	QOS     []QOS
	// Reservations are the reservation names the account may use.
	Reservations []string
	// AllocationNodeHours per account, for sshare.
	AllocationNodeHours map[string]float64
	Seed                int64
	// GuestAloneAfter: a guest no host took within this (real) time after it became eligible starts as a job of
	// its own, as a job does that finds no host. Zero: a guest waits for a host forever.
	GuestAloneAfter time.Duration
}

// Job is one simulated site job. Fields are read under the site's lock only.
type Job struct {
	ID                                              int
	Name, User, Account, Partition, QOS, Reserv, Cm string
	Env                                             map[string]string
	LimitMin                                        int
	Attempt                                         string

	SubmitAt, EligibleAt, StartAt, EndAt time.Time // real times
	State                                string
	Reason                               string
	Exit                                 int
	endState                             string
	endExit                              int
	runReal                              time.Duration
	endedReal                            time.Time // when the job really ended (EndAt is dilated)
	quick                                bool      // a lost-reply job that starts at once and ends within a second
	forgotten                            bool      // slurmctld no longer knows it
	ckptSeq                              int
	tailReal                             time.Duration // a host keeps its node this long after its step (D-26)
	stepDone                             bool          // a host's step is over, its job still runs
	guest                                bool
	leadReal                             time.Duration // a resumable step's lead, real time (EnvLeadMinutes)
	slots                                int           // a sharing job's GPU slots (FAKE_SHARE_SLOTS)
	scope                                string        // its sharing scope
	slotNo                               int           // the slot a host gave this guest
	host                                 int           // the host that took this guest
	takenAt                              time.Time     // when the host took it
}

// Stats are the counters a test reads after a run.
type Stats struct {
	// Calls by endpoint kind: state, submit, list, jobget, acct, cancel, hub.
	Calls map[string]int
	// StateCallTimes are the arrival times of every jobs/state call, for the rate-limit check.
	StateCallTimes []time.Time
	// StateIDs is the total number of IDs asked for in jobs/state calls.
	StateIDs int
	// State422 is the number of jobs/state calls answered 422.
	State422 int
	// Submits counts accepted submissions per job key (name + attempt); a value above 1 is a duplicate.
	Submits map[string]int
	// Refusals counts submissions refused for a lane (QoS limit or time limit).
	Refusals int
	// Injected counts the requests answered with an injected 5xx or hang; LostReplies the accepted
	// submissions whose reply was dropped.
	Injected, LostReplies int
	// LaggedReads is the number of accounting reads answered with a record that lagged (AcctLag).
	LaggedReads int
	// QuickLost counts the lost-reply submissions that finished before any retry (LostReplyQuick).
	QuickLost int
	// Started counts the jobs that ever ran on a node: a guest never does, so for sharing jobs this is the number of
	// nodes used.
	Started int
	// PeakRunning and PeakActive are the highest number of running and of pending+running jobs.
	PeakRunning, PeakActive int
	// ChargedMinutes is the dilated node-minutes charged to each account so far (ended jobs).
	ChargedMinutes map[string]float64
	// GuestsTaken is the number of times a host took a guest; GuestsSignalled the number of resumable guests
	// that were sent the stop signal (AC-73) before their run was over, and so exited 85 on their host.
	GuestsTaken, GuestsSignalled int
}

// Site is a running fake site.
type Site struct {
	Opts Options
	Srv  *httptest.Server

	mu        sync.Mutex
	rng       *rand.Rand
	jobs      map[int]*Job
	order     []int // submission order, for FIFO start
	nextID    int
	stats     Stats
	down      bool
	spurious  float64 // chance that a jobs/state call is answered 422 for no reason
	minAge    time.Duration
	stopTick  context.CancelFunc
	tickerEnd chan struct{}
}

// New starts a site on an httptest TLS server (the HPCSite endpoint must be https); the caller closes it with Close.
func New(o Options) *Site {
	if o.Version == "" {
		o.Version = "v0.0.44"
	}
	if o.Dilation <= 0 {
		o.Dilation = 60
	}
	if o.Capacity <= 0 {
		o.Capacity = 50
	}
	if o.MinJobAge <= 0 {
		o.MinJobAge = 5 * time.Minute
	}
	if o.RunMax <= 0 {
		o.RunMin, o.RunMax = 4, 10
	}
	if o.HangFor <= 0 {
		o.HangFor = 5 * time.Second
	}
	s := &Site{Opts: o, rng: rand.New(rand.NewSource(o.Seed + 1)), jobs: map[int]*Job{}, nextID: 4_000_000, minAge: o.MinJobAge,
		stats: Stats{Calls: map[string]int{}, Submits: map[string]int{}, ChargedMinutes: map[string]float64{}}}
	s.Srv = httptest.NewTLSServer(http.HandlerFunc(s.handle))
	ctx, cancel := context.WithCancel(context.Background())
	s.stopTick, s.tickerEnd = cancel, make(chan struct{})
	go s.ticker(ctx)
	return s
}

// CertPEM is the server certificate, for SSL_CERT_FILE of the controller process.
func (s *Site) CertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Srv.Certificate().Raw})
}

// URL is the endpoint to put in HPCSite.spec.profile.endpoint.
func (s *Site) URL() string { return s.Srv.URL }

// Close stops the ticker and the server.
func (s *Site) Close() {
	s.stopTick()
	<-s.tickerEnd
	s.Srv.Close()
}

// SetDown makes every request answer 503 (a site outage) until SetDown(false).
func (s *Site) SetDown(down bool) { s.mu.Lock(); s.down = down; s.mu.Unlock() }

// SetSpurious422 makes that share of jobs/state calls answer 422 whatever the IDs (a proxy or a
// slurmctld glitch); the one-by-one fallback of the client then meets the same chance per ID.
func (s *Site) SetSpurious422(rate float64) { s.mu.Lock(); s.spurious = rate; s.mu.Unlock() }

// SetMinJobAge changes how long finished jobs stay known to slurmctld; 1ns forgets them at once.
func (s *Site) SetMinJobAge(d time.Duration) { s.mu.Lock(); s.minAge = d; s.mu.Unlock() }

// Snapshot returns a copy of the counters.
func (s *Site) Snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.Calls = copyMap(s.stats.Calls)
	st.Submits = copyMap(s.stats.Submits)
	st.StateCallTimes = append([]time.Time(nil), s.stats.StateCallTimes...)
	st.ChargedMinutes = map[string]float64{}
	for k, v := range s.stats.ChargedMinutes {
		st.ChargedMinutes[k] = v
	}
	return st
}

func copyMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// JobCount is the number of jobs the site has ever accepted.
func (s *Site) JobCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.jobs) }

// Charged is the dilated node-minutes the account has consumed so far, including running jobs.
func (s *Site) Charged(account string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chargedLocked(account, time.Now())
}

func (s *Site) chargedLocked(account string, now time.Time) float64 {
	total := s.stats.ChargedMinutes[account]
	for _, j := range s.jobs {
		if j.Account == account && j.State == Running {
			total += now.Sub(j.StartAt).Seconds() * s.Opts.Dilation / 60
		}
	}
	return total
}

// ---- request handling -------------------------------------------------------------------------

func (s *Site) handle(w http.ResponseWriter, r *http.Request) {
	kind := kindOf(r)
	s.mu.Lock()
	s.stats.Calls[kind]++
	if kind == keyState {
		s.stats.StateCallTimes = append(s.stats.StateCallTimes, time.Now())
	}
	down := s.down
	inject5xx := s.rng.Float64() < s.Opts.Err5xx
	hang := s.rng.Float64() < s.Opts.TimeoutRate
	delay := s.Opts.Latency
	if s.Opts.Jitter > 0 {
		delay += time.Duration(s.rng.Int63n(int64(s.Opts.Jitter)))
	}
	if down || inject5xx || hang {
		s.stats.Injected++
	}
	s.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	switch {
	case down || inject5xx:
		writeErr(w, http.StatusServiceUnavailable, "site unavailable")
		return
	case hang:
		select {
		case <-r.Context().Done():
		case <-time.After(s.Opts.HangFor):
		}
		writeErr(w, http.StatusGatewayTimeout, "timeout")
		return
	}
	if s.Opts.Token != "" && r.Header.Get("X-SLURM-USER-TOKEN") != s.Opts.Token {
		writeErr(w, http.StatusUnauthorized, "invalid token")
		return
	}
	user := r.Header.Get("X-SLURM-USER-NAME")
	switch kind {
	case keyState:
		s.jobsState(w, r)
	case "submit":
		s.submit(w, r, user)
	case "list":
		s.list(w, r)
	case "jobget":
		s.jobGet(w, r)
	case "cancel":
		s.cancel(w, r)
	case "acct":
		s.accounting(w, r)
	case "acctlist":
		s.accountingList(w, r)
	case "ping":
		writeJSON(w, http.StatusOK, map[string]any{"pings": []map[string]any{
			{"hostname": "slurmctld", "pinged": "UP", "responding": true}}})
	case "hub":
		s.hub(w, r)
	default:
		writeErr(w, http.StatusNotFound, "no such endpoint")
	}
}

func kindOf(r *http.Request) string {
	p := r.URL.Path
	switch {
	case strings.HasSuffix(p, "/jobs/state"):
		return keyState
	case strings.HasSuffix(p, "/job/submit"):
		return "submit"
	case strings.HasSuffix(p, "/ping") || strings.HasSuffix(p, "/ping/"):
		return "ping"
	case strings.Contains(p, "/slurmdb/") && strings.HasSuffix(p, "/jobs"):
		return "acctlist"
	case strings.Contains(p, "/slurmdb/") && strings.Contains(p, "/job/"):
		return "acct"
	case strings.HasSuffix(p, "/sshare"), strings.HasSuffix(p, "/qos"), strings.HasSuffix(p, "/reservations"):
		return "hub"
	case strings.HasSuffix(p, "/jobs"):
		return "list"
	case strings.Contains(p, "/job/") && r.Method == http.MethodDelete:
		return "cancel"
	case strings.Contains(p, "/job/"):
		return "jobget"
	}
	return "other"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"errors": []map[string]any{{"description": msg, "error": msg}}})
}

func idOf(r *http.Request) int {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	n, _ := strconv.Atoi(parts[len(parts)-1])
	return n
}

// known reports whether slurmctld still has the job.
func (s *Site) known(id int) (*Job, bool) {
	j, ok := s.jobs[id]
	if !ok || j.forgotten {
		return nil, false
	}
	return j, true
}

func (s *Site) jobsState(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("job_id")
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := strings.Split(raw, ",")
	s.stats.StateIDs += len(ids)
	reject := func() {
		s.stats.State422++
		writeErr(w, http.StatusUnprocessableEntity, "Invalid job id specified")
	}
	if raw == "" {
		reject()
		return
	}
	if s.spurious > 0 && s.rng.Float64() < s.spurious {
		reject()
		return
	}
	out := make([]map[string]any, 0, len(ids))
	for _, f := range ids {
		n, err := strconv.Atoi(f)
		j, ok := s.known(n)
		if err != nil || !ok {
			reject()
			return
		}
		out = append(out, map[string]any{"job_id": strconv.Itoa(j.ID), keyState: []string{j.State},
			"state_reason": j.Reason, "comment": j.Cm})
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Site) list(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, 0, len(s.order))
	for _, id := range s.order {
		j := s.jobs[id]
		if j.forgotten || (name != "" && j.Name != name) {
			continue
		}
		out = append(out, ctldView(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func ctldView(j *Job) map[string]any {
	return map[string]any{"job_id": j.ID, "name": j.Name, "job_state": []string{j.State},
		"state_reason": j.Reason, "comment": j.Cm}
}

func (s *Site) jobGet(w http.ResponseWriter, r *http.Request) {
	n := idOf(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.known(n)
	if !ok {
		writeErr(w, http.StatusNotFound, "Invalid job id specified")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": []map[string]any{ctldView(j)}})
}

func (s *Site) cancel(w http.ResponseWriter, r *http.Request) {
	n := idOf(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.known(n)
	if !ok {
		writeErr(w, http.StatusNotFound, "Invalid job id specified")
		return
	}
	if j.State == Pending || j.State == Running {
		s.endLocked(j, Cancelled, 0, time.Now())
	}
	writeJSON(w, http.StatusOK, map[string]any{"errors": []any{}})
}

// accountingRecord is slurmdbd's record of a job. Under AcctLag it lags: a job that ended less than AcctLag ago
// is still RUNNING in it, with no exit code and no end time. The caller holds the site's lock.
func (s *Site) accountingRecord(j *Job) map[string]any {
	if lag := s.Opts.AcctLag; lag > 0 && j.State != Pending && j.State != Running && !j.endedReal.IsZero() && time.Since(j.endedReal) < lag {
		s.stats.LaggedReads++
		return map[string]any{"job_id": j.ID, "name": j.Name, "account": j.Account, "user": j.User,
			keyState:    map[string]any{"current": []string{Running}},
			"exit_code": map[string]any{"return_code": map[string]any{"set": false, "number": 0}},
			"comment":   map[string]any{"job": j.Cm},
			"time":      map[string]any{"start": epoch(j.StartAt), "end": epoch(time.Time{})}}
	}
	return map[string]any{"job_id": j.ID, "name": j.Name, "account": j.Account, "user": j.User,
		keyState:    map[string]any{"current": []string{j.State}},
		"exit_code": map[string]any{"return_code": map[string]any{"set": true, "number": j.Exit}},
		"comment":   map[string]any{"job": j.Cm},
		"time":      map[string]any{"start": epoch(j.StartAt), "end": epoch(j.EndAt)}}
}

func (s *Site) accounting(w http.ResponseWriter, r *http.Request) {
	n := idOf(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[n]
	if !ok {
		writeErr(w, http.StatusNotFound, "Invalid job id specified")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": []map[string]any{s.accountingRecord(j)}})
}

// accountingList is GET /slurmdb/v/jobs?job_name=: slurmdbd lists every job of that name, finished or not,
// for as long as it keeps them (the fake: forever). start_time bounds the search like the real query.
func (s *Site) accountingList(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("job_name")
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, 0, 1)
	for _, id := range s.order {
		if j := s.jobs[id]; name == "" || j.Name == name {
			out = append(out, s.accountingRecord(j))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

// epoch is a Slurm time object; an unset time is "set": false.
func epoch(t time.Time) map[string]any {
	if t.IsZero() {
		return map[string]any{"set": false, "number": 0}
	}
	return map[string]any{"set": true, "number": t.Unix()}
}

func (s *Site) hub(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case strings.HasSuffix(r.URL.Path, "/sshare"):
		acct := r.URL.Query().Get("account")
		alloc := s.Opts.AllocationNodeHours[acct]
		used := s.chargedLocked(acct, time.Now()) / 60
		writeJSON(w, http.StatusOK, map[string]any{"account": acct, "allocated_node_hours": alloc,
			"used_node_hours": used, "left_node_hours": math.Max(alloc-used, 0)})
	case strings.HasSuffix(r.URL.Path, "/qos"):
		writeJSON(w, http.StatusOK, map[string]any{"qos": s.Opts.QOS})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"reservations": s.Opts.Reservations})
	}
}

// ---- submit ---------------------------------------------------------------------------------

type submitBody struct {
	Job struct {
		Name        string   `json:"name"`
		Partition   string   `json:"partition"`
		Account     string   `json:"account"`
		QOS         string   `json:"qos"`
		TimeLimit   int      `json:"time_limit"`
		Comment     string   `json:"comment"`
		Reservation string   `json:"reservation"`
		Environment []string `json:"environment"`
		BeginTime   struct {
			Set    bool  `json:"set"`
			Number int64 `json:"number"`
		} `json:"begin_time"`
	} `json:"job"`
}

func (s *Site) submit(w http.ResponseWriter, r *http.Request, user string) {
	var b submitBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "bad body")
		return
	}
	env := map[string]string{}
	for _, kv := range b.Job.Environment {
		if k, v, ok := strings.Cut(kv, "="); ok {
			env[k] = v
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if why := s.refuseLocked(&b, user); why != "" {
		s.stats.Refusals++
		writeErr(w, http.StatusBadRequest, why)
		return
	}
	now := time.Now()
	j := &Job{ID: s.nextID, Name: b.Job.Name, User: user, Account: b.Job.Account, Partition: b.Job.Partition,
		QOS: b.Job.QOS, Reserv: b.Job.Reservation, Cm: b.Job.Comment, Env: env, LimitMin: b.Job.TimeLimit,
		Attempt: env[EnvAttempt], SubmitAt: now, State: Pending, Reason: "Priority"}
	s.nextID++
	j.EligibleAt = now.Add(s.between(s.Opts.QueueMin, s.Opts.QueueMax))
	if b.Job.BeginTime.Set {
		// BeginTime (real seconds): the job is not eligible before it. A sharing guest sets it far ahead.
		if begin := time.Unix(b.Job.BeginTime.Number, 0); begin.After(j.EligibleAt) {
			j.EligibleAt, j.Reason = begin, "BeginTime"
		}
	}
	lost := s.rng.Float64() < s.Opts.LostReplyRate
	quick := lost && s.rng.Float64() < s.Opts.LostReplyQuick
	if lost && !quick {
		j.EligibleAt = j.EligibleAt.Add(s.Opts.LostReplyHold)
	}
	s.planLocked(j)
	if quick {
		j.quick, j.EligibleAt, j.runReal = true, now, 800*time.Millisecond
		s.stats.QuickLost++
	}
	s.jobs[j.ID] = j
	s.order = append(s.order, j.ID)
	s.stats.Submits[j.Name+"#"+j.Attempt]++
	s.peaksLocked()
	if lost {
		s.stats.LostReplies++
		writeErr(w, http.StatusGatewayTimeout, "reply lost")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": j.ID, "errors": []any{}})
}

// refuseLocked returns Slurm's refusal text for a submission outside a QoS limit, "" when accepted.
func (s *Site) refuseLocked(b *submitBody, user string) string {
	if b.Job.TimeLimit <= 0 {
		return "Requested time limit is invalid (missing or exceeds some limit)"
	}
	for _, q := range s.Opts.QOS {
		if q.Name != b.Job.QOS {
			continue
		}
		if q.MaxWallMinutes > 0 && b.Job.TimeLimit > q.MaxWallMinutes {
			return "Requested time limit is invalid (missing or exceeds some limit)"
		}
		if q.MaxJobsPerUser > 0 {
			held := 0
			for _, j := range s.jobs {
				if j.User == user && j.QOS == q.Name && (j.State == Pending || j.State == Running) {
					held++
				}
			}
			if held >= q.MaxJobsPerUser {
				return "Job violates accounting/QOS policy (QOSMaxSubmitJobPerUserLimit)"
			}
		}
	}
	if b.Job.Reservation != "" && !contains(s.Opts.Reservations, b.Job.Reservation) {
		return "Requested reservation is invalid"
	}
	return ""
}

func contains(l []string, v string) bool {
	for _, x := range l {
		if x == v {
			return true
		}
	}
	return false
}

func (s *Site) between(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(s.rng.Int63n(int64(hi-lo)))
}

// planLocked decides how the job will run and end. The plan is a function of the job's name, its
// attempt and the site's seed, so a repeated submission of the same attempt plans the same run.
func (s *Site) planLocked(j *Job) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(j.Name + "#" + j.Attempt))
	rng := rand.New(rand.NewSource(int64(h.Sum64()) ^ s.Opts.Seed))

	runMin := s.Opts.RunMin + rng.Float64()*(s.Opts.RunMax-s.Opts.RunMin)
	if v, err := strconv.ParseFloat(j.Env[EnvRunMinutes], 64); err == nil && v > 0 {
		runMin = v
	}
	segments, seq := atoi(j.Env[EnvSegments]), ckptSeqOf(j.Env[EnvResumeRef])
	end, exit := Completed, 0
	if segments > 0 && seq+1 < segments {
		end, exit, j.ckptSeq = Failed, ExitCodeContinue, seq+1 // saved, continue me
	}
	u := rng.Float64()
	switch force := j.Env[EnvForceEnd]; {
	case force != "":
		end, exit, runMin, j.ckptSeq = force, 0, runMin*0.6, 0
		if force == Failed {
			exit = 1
		}
	case u < s.Opts.NodeFailRate, u < s.Opts.NodeFailRate+s.Opts.PreemptRate:
		end, exit, runMin = NodeFail, 0, runMin*(0.2+0.7*rng.Float64())
		if u >= s.Opts.NodeFailRate {
			end = Preempted
		}
		j.ckptSeq = 0
		if segments > 0 && rng.Intn(2) == 0 { // a periodic copy was taken before the node went
			j.ckptSeq = seq + 1
		}
	case u < s.Opts.NodeFailRate+s.Opts.PreemptRate+s.Opts.FailRate:
		end, exit, j.ckptSeq = Failed, 1, 0
	}
	if j.LimitMin > 0 && runMin > float64(j.LimitMin) {
		end, exit, runMin, j.ckptSeq = Timeout, 0, float64(j.LimitMin), 0
	}
	j.endState, j.endExit = end, exit
	j.runReal = time.Duration(runMin * 60 / s.Opts.Dilation * float64(time.Second))
	j.guest = j.Env[EnvGuest] != ""
	if lead, err := strconv.ParseFloat(j.Env[EnvLeadMinutes], 64); err == nil && lead > 0 {
		j.leadReal = time.Duration(lead * 60 / s.Opts.Dilation * float64(time.Second))
	}
	j.scope, j.slots = j.Env[EnvShareScope], atoi(j.Env[EnvShareSlots])
	if tail, err := strconv.ParseFloat(j.Env[EnvHostTailMinutes], 64); err == nil && tail > 0 && !j.guest && (end == Completed || end == Failed) {
		if j.LimitMin > 0 {
			tail = math.Max(0, math.Min(tail, float64(j.LimitMin)-runMin)) // the job ends by its time limit
		}
		j.tailReal = time.Duration(tail * 60 / s.Opts.Dilation * float64(time.Second))
	}
}

// guestLocked plays the slot manager for a waiting guest: a running host of its account whose job outlasts
// the guest's run takes it, and once the run is over the guest's job is cancelled with the step's result. A
// resumable guest (AC-73) is also taken when only its lead fits the host's remaining time, and is signalled
// at the host job's end minus its lead: it saves and ends exit 85 with a checkpoint, however much of its run
// is left.
func (s *Site) guestLocked(j *Job, now time.Time) {
	if j.host == 0 {
		if now.Before(j.EligibleAt) {
			return
		}
		for _, id := range s.order {
			h := s.jobs[id]
			if h.State != Running || h.Account != j.Account || h.tailReal <= 0 {
				continue
			}
			end := h.StartAt.Add(h.runReal + h.tailReal)
			if !now.Add(j.runReal).After(end) || (j.leadReal > 0 && !now.Add(j.leadReal).After(end)) {
				j.host, j.takenAt, j.Reason, j.slotNo = h.ID, now, ReasonHeld, 1
				j.Cm += fmt.Sprintf(" kaos-state=taken host=%d slot=1", h.ID)
				s.stats.GuestsTaken++
				return
			}
		}
		return
	}
	end := s.jobs[j.host].StartAt.Add(s.jobs[j.host].runReal + s.jobs[j.host].tailReal)
	runEnd := j.takenAt.Add(j.runReal)
	if j.leadReal > 0 && end.Add(-j.leadReal).Before(runEnd) {
		if now.Before(end.Add(-j.leadReal)) {
			return
		}
		// Signalled before its run is over: it saves (one more checkpoint than it started from) and exits 85.
		seq := 0
		if atoi(j.Env[EnvSegments]) > 0 {
			seq = ckptSeqOf(j.Env[EnvResumeRef]) + 1
		}
		s.stats.GuestsSignalled++
		s.finishGuestLocked(j, now, ExitCodeContinue, seq)
		return
	}
	if now.Before(runEnd) {
		return
	}
	rc := 0
	if j.endExit == ExitCodeContinue {
		rc = ExitCodeContinue
	}
	s.finishGuestLocked(j, now, rc, j.ckptSeq)
}

// finishGuestLocked writes what the slot manager writes when a guest's step ends, and cancels the guest's job.
func (s *Site) finishGuestLocked(j *Job, now time.Time, rc, seq int) {
	done := fmt.Sprintf("kaos-state=done rc=%d host=%d start=%d", rc, j.host, j.takenAt.Unix())
	if rc == ExitCodeContinue && seq > 0 {
		done += fmt.Sprintf(" kaos-ckpt-seq=%d", seq)
	}
	j.Cm = strings.Replace(j.Cm, fmt.Sprintf("kaos-state=taken host=%d slot=%d", j.host, j.slotNo), done, 1)
	s.endLocked(j, Cancelled, 0, now)
}

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

// ckptSeqOf reads the sequence number of a checkpoint reference "scratch://.../<seq>.tar#sha".
func ckptSeqOf(ref string) int {
	ref, _, _ = strings.Cut(ref, "#")
	ref = strings.TrimSuffix(ref, ".tar")
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		ref = ref[i+1:]
	}
	return atoi(ref)
}

// ---- simulation ---------------------------------------------------------------------------------

func (s *Site) ticker(ctx context.Context) {
	defer close(s.tickerEnd)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.mu.Lock()
			s.advanceLocked(now)
			s.mu.Unlock()
		}
	}
}

// advanceLocked ends running jobs whose time is up, starts queued ones in submission order while
// nodes are free, and makes slurmctld forget jobs that finished MinJobAge ago.
func (s *Site) advanceLocked(now time.Time) {
	s.slotManagersLocked(now)
	running := 0
	for _, id := range s.order {
		j := s.jobs[id]
		if j.State == Running {
			stepEnd := j.StartAt.Add(j.runReal)
			switch {
			case j.slots > 0 && j.tailReal == 0 && (j.endState == Completed || j.endState == Failed):
				// A slot-manager host keeps its node until its last guest is done (plus the idle release).
				hold := s.holdUntilLocked(j)
				if now.Before(stepEnd) {
					running++
					break
				}
				if now.Before(hold) {
					if !j.stepDone {
						j.stepDone = true
						j.Cm += fmt.Sprintf(" kaos-state=done rc=%d", j.endExit)
					}
					running++
					break
				}
				s.endLocked(j, j.endState, j.endExit, hold)
			case now.Before(stepEnd):
				running++
			case now.Before(stepEnd.Add(j.tailReal)): // a host's job holds the node for guests after its step
				if !j.stepDone {
					j.stepDone = true
					j.Cm += fmt.Sprintf(" kaos-state=done rc=%d", j.endExit)
				}
				running++
			default:
				s.endLocked(j, j.endState, j.endExit, stepEnd.Add(j.tailReal))
			}
		}
	}
	for _, id := range s.order {
		j := s.jobs[id]
		if j.State == Pending && j.guest && j.host == 0 && s.Opts.GuestAloneAfter > 0 &&
			!now.Before(j.EligibleAt.Add(s.Opts.GuestAloneAfter)) {
			j.guest = false // no host took it: it runs as a job of its own
		}
		if j.State == Pending && j.guest {
			s.guestLocked(j, now)
			continue
		}
		if j.State == Pending && (running < s.Opts.Capacity || j.quick) && !now.Before(j.EligibleAt) {
			j.State, j.Reason, j.StartAt = Running, "None", now
			s.stats.Started++
			running++
		}
		if !j.forgotten && !j.endedReal.IsZero() && now.Sub(j.endedReal) >= s.minAge {
			j.forgotten = true
		}
	}
	s.peaksLocked()
}

// limitReal is the real time a job may run: its time limit, dilated.
func (s *Site) limitReal(j *Job) time.Duration {
	return time.Duration(float64(j.LimitMin) * 60 / s.Opts.Dilation * float64(time.Second))
}

// holdUntilLocked is when a slot-manager host's job ends if it takes no more guests: its own step's end or its
// last guest's end, plus the idle release, never past its time limit.
func (s *Site) holdUntilLocked(h *Job) time.Time {
	hold := h.StartAt.Add(h.runReal)
	for _, g := range s.jobs {
		if g.host == h.ID && g.State == Pending {
			if end := g.takenAt.Add(g.runReal); end.After(hold) {
				hold = end
			}
		}
	}
	hold = hold.Add(time.Duration(float64(ShareIdle) / s.Opts.Dilation))
	if limit := h.StartAt.Add(s.limitReal(h)); h.LimitMin > 0 && hold.After(limit) {
		hold = limit
	}
	return hold
}

// slotManagersLocked plays the slot manager (payload/slots.py) of every running sharing job: it takes waiting
// jobs of its scope, account and partition into its free slots, in the queue's order, while the guest's step
// plus the margin ends before the host's own limit. A job still waiting for its BeginTime is takeable too.
func (s *Site) slotManagersLocked(now time.Time) {
	margin := time.Duration(float64(ShareMargin) / s.Opts.Dilation)
	for _, hid := range s.order {
		h := s.jobs[hid]
		if h.State != Running || h.slots <= 0 || h.tailReal > 0 {
			continue
		}
		if stepEnd := h.StartAt.Add(h.runReal); !now.Before(stepEnd) && !now.Before(s.holdUntilLocked(h)) {
			continue // its job ends in this tick: no guest is taken onto a node that is being released
		}
		used := 0
		if now.Before(h.StartAt.Add(h.runReal)) {
			used = 1
		}
		for _, g := range s.jobs {
			if g.host == h.ID && g.State == Pending {
				used++
			}
		}
		hostEnd := h.StartAt.Add(s.limitReal(h))
		for _, id := range s.order {
			j := s.jobs[id]
			if used >= h.slots {
				break
			}
			if j.State != Pending || j.guest || j.host != 0 || j.slots <= 0 || j.scope != h.scope || j.Account != h.Account ||
				j.Partition != h.Partition || (h.LimitMin > 0 && now.Add(j.runReal).Add(margin).After(hostEnd)) {
				continue
			}
			used++
			j.host, j.takenAt, j.Reason, j.slotNo, j.guest = h.ID, now, ReasonHeld, used, true
			j.Cm += fmt.Sprintf(" kaos-state=taken host=%d slot=%d", h.ID, used)
		}
	}
}

func (s *Site) peaksLocked() {
	run, act := 0, 0
	for _, id := range s.order {
		switch s.jobs[id].State {
		case Running:
			run++
			act++
		case Pending:
			act++
		}
	}
	s.stats.PeakRunning, s.stats.PeakActive = max(s.stats.PeakRunning, run), max(s.stats.PeakActive, act)
}

// endLocked puts a job into an end state. The reported end is the start plus the dilated run.
func (s *Site) endLocked(j *Job, state string, exit int, realEnd time.Time) {
	if j.State == Pending {
		j.StartAt = time.Time{}
	}
	ran := time.Duration(0)
	if !j.StartAt.IsZero() {
		ran = realEnd.Sub(j.StartAt)
		if ran < 0 {
			ran = 0
		}
		j.EndAt = j.StartAt.Add(time.Duration(float64(ran) * s.Opts.Dilation))
		s.stats.ChargedMinutes[j.Account] += ran.Seconds() * s.Opts.Dilation / 60
	} else {
		j.EndAt = realEnd
	}
	j.State, j.Exit, j.Reason, j.endedReal = state, exit, "None", time.Now()
	if j.ckptSeq > 0 && (state == NodeFail || state == Preempted || exit == ExitCodeContinue) {
		j.Cm += fmt.Sprintf(" kaos-ckpt-seq=%d", j.ckptSeq)
	}
}

// States lists the IDs of jobs in a state, for tests.
func (s *Site) States() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, j := range s.jobs {
		out[j.State]++
	}
	return out
}

// JobNames returns the sorted names of the jobs the site holds.
func (s *Site) JobNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j.Name)
	}
	sort.Strings(out)
	return out
}
