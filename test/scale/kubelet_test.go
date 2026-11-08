//go:build scale

package scale

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// The fake kubelet plays the runner pods. In production the controller creates one batch Job per
// attempt (runnerjob.Build) and its pod runs runner/kaos_hpc_runner against the site. A scale test
// cannot run 1,000 Python pods, and envtest has no kubelet, so this loop watches the same runner
// Jobs and does what the runner does, in-process, through the same interfaces:
//
//   - find the attempt's site job by key (the job name, which carries the HPC job and the attempt): the live job,
//     else the finished one in accounting (a submit whose answer was lost), else the one it reported, else submit;
//   - wait while the site does not answer (connection errors, 5xx): ask again after 5, 10, 20 ... 60 s, show
//     pendingReason SiteUnavailable, fail only past siteWaitMax (AC-28, AC-30); every round starts with the adopt step;
//   - on a lane refusal submit once more on the class's own QoS (runner.submit_in_lane);
//   - report status.run (siteJobID, state, start, the one terminal report with exit code and times,
//     the checkpoint reference of a recoverable end);
//   - read the job's state from status.siteObservation while it is fresh and call the site only for
//     the accounting record of a state change or an end, or itself when the observation is stale.
//
// What is NOT modelled: dataset/image staging, the job script, logs, outputs, node sharing. Submit
// refusals (4xx) follow the real pod behaviour: the pod exits, Kubernetes restarts it after 10, 20, 40, 80 s
// (backoffLimit 4), and the batch Job fails after the fifth try.
//
// One goroutine ticks; every runner step runs on a bounded worker pool so a slow site call never
// stalls the others. The loop lists with resourceVersion=0 (served from the API server's watch
// cache), so the harness adds little load on top of the controller's own.

// annotations on an HPCArgoWorkflowJob steer the fake site (the real spec has no such inputs).
const (
	annRunMinutes = "fake.kaos.io/run-minutes"
	annForceEnd   = "fake.kaos.io/force-end"
	annSegments   = "fake.kaos.io/segments"
)

// siteWiring tells the kubelet which fake site serves an HPCSite namespace.
type siteWiring struct {
	hpcNS   string
	url     string
	version string
	user    string
	account string
	token   string
	// classes by name -> partition and default QoS, as the HPCSite profile has them.
	classes map[string]hpcv1alpha1.SiteClass
}

type kubelet struct {
	c      client.Client
	http   *http.Client
	sites  map[string]*siteWiring // by HPC namespace
	poll   time.Duration          // the runner's poll tick (POLL_S scaled down)
	obsTTL time.Duration          // the controller's poll interval; stale after 3 of them

	mu      sync.Mutex
	runners map[string]*frunner
	sem     chan struct{}
	rng     *rand.Rand

	submits  int64 // runner submit calls, for the report
	adopts   int64
	restFall int64 // site calls a runner made itself because the observation was stale
}

type frunner struct {
	rj        batchv1.Job
	job       types.NamespacedName
	attempt   int32
	cancel    bool
	site      *siteWiring
	startAt   time.Time
	nextTry   time.Time
	fails     int
	siteJobID string
	reported  string
	waitSince time.Time // the site stopped answering this runner at this time (zero: it answers)
	waitDelay time.Duration
	busy      bool
	done      bool
	watchSeen time.Time
	lastRest  time.Time
}

func newKubelet(c client.Client, sites []*siteWiring, poll, obsTTL time.Duration) *kubelet {
	m := map[string]*siteWiring{}
	for _, s := range sites {
		m[s.hpcNS] = s
	}
	return &kubelet{c: c, sites: m, poll: poll, obsTTL: obsTTL, runners: map[string]*frunner{},
		sem: make(chan struct{}, 48), rng: rand.New(rand.NewSource(7)),
		http: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64, MaxConnsPerHost: 64,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}} //nolint:gosec // test-only client of a self-signed fake site
}

// run ticks until ctx ends.
func (k *kubelet) run(ctx context.Context) {
	t := time.NewTicker(k.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			k.tick(ctx)
		}
	}
}

func rv0() *client.ListOptions {
	return &client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: "0"}}
}

func (k *kubelet) tick(ctx context.Context) {
	var rjs batchv1.JobList
	if err := k.c.List(ctx, &rjs, rv0(), client.HasLabels{runnerjob.LabelJobUID}); err != nil {
		return
	}
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := k.c.List(ctx, &jobs, rv0()); err != nil {
		return
	}
	snap := make(map[types.NamespacedName]*hpcv1alpha1.HPCArgoWorkflowJob, len(jobs.Items))
	for i := range jobs.Items {
		snap[client.ObjectKeyFromObject(&jobs.Items[i])] = &jobs.Items[i]
	}
	now := time.Now()
	k.mu.Lock()
	for i := range rjs.Items {
		rj := rjs.Items[i]
		if _, ok := k.runners[rj.Name]; ok || !rj.DeletionTimestamp.IsZero() {
			continue
		}
		site := k.sites[rj.Namespace]
		attempt, _ := strconv.Atoi(rj.Labels[runnerjob.LabelAttempt])
		if site == nil {
			continue
		}
		k.runners[rj.Name] = &frunner{rj: rj, site: site, attempt: int32(attempt),
			cancel: rj.Labels[runnerjob.LabelCancelOnly] != "",
			job: types.NamespacedName{Namespace: rj.Annotations[runnerjob.AnnotationJobNamespace],
				Name: rj.Annotations[runnerjob.AnnotationJobName]},
			startAt: now.Add(time.Duration(100+k.rng.Intn(500)) * time.Millisecond)}
	}
	var todo []*frunner
	for _, r := range k.runners {
		if !r.done && !r.busy && !now.Before(r.startAt) && !now.Before(r.nextTry) {
			r.busy = true
			todo = append(todo, r)
		}
	}
	k.mu.Unlock()
	for _, r := range todo {
		j := snap[r.job]
		go func(r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
			k.sem <- struct{}{}
			defer func() { <-k.sem; k.mu.Lock(); r.busy = false; k.mu.Unlock() }()
			k.step(ctx, r, j)
		}(r, j)
	}
}

// Constants of the runner's wait for an unreachable site (runner.py SITE_RETRY_*, settings DEFAULT_SITE_WAIT_MAX_S).
const (
	siteRetryFirst = 5 * time.Second
	siteRetryMax   = 60 * time.Second
	siteWaitMax    = 30 * time.Minute
	// finishedLookback bounds the accounting search by name (slurm.py FINISHED_LOOKBACK_S).
	finishedLookback = 7 * 24 * time.Hour
)

// siteName is the attempt's key at the site: the runner's job_site_name (the workflow uid is the job uid here),
// with the HPC job's identity and the attempt in it, so a finished job is found again by name.
func siteName(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32) string {
	id := strings.TrimPrefix(j.Name, "hpc-")
	if len(id) > 8 {
		id = id[:8]
	}
	return fmt.Sprintf("kaos-%s-%s-%s-a%d", j.UID, j.Spec.StepName, id, attempt)
}

var terminalStates = map[string]bool{"COMPLETED": true, "FAILED": true, "CANCELLED": true, "TIMEOUT": true,
	"NODE_FAIL": true, "PREEMPTED": true, "OUT_OF_MEMORY": true, "BOOT_FAIL": true, "REVOKED": true}

// step advances one runner by one tick. j is the snapshot of its HPCArgoWorkflowJob (nil: gone).
func (k *kubelet) step(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
	if j == nil || j.Status.Attempt > r.attempt && j.Status.Attempt != 0 && !r.cancel {
		k.finish(r) // the job is gone or moved to a later attempt
		return
	}
	if run := j.Status.Run; !r.cancel && run != nil && run.Attempt == r.attempt && run.Terminal {
		k.finish(r)
		return
	}
	if r.cancel {
		k.stepCancel(ctx, r, j)
		return
	}
	if r.siteJobID == "" {
		k.stepSubmit(ctx, r, j)
		return
	}
	k.stepFollow(ctx, r, j)
}

func (k *kubelet) finish(r *frunner) { k.mu.Lock(); r.done = true; k.mu.Unlock() }

// stepSubmit adopts or submits the attempt's site job. A site that does not answer is waited for; a refusal
// ends the pod, which Kubernetes restarts with a backoff.
func (k *kubelet) stepSubmit(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
	name := siteName(j, r.attempt)
	prev := ""
	if run := j.Status.Run; run != nil && run.Attempt == r.attempt {
		prev = run.SiteJobID
	}
	id, found, err := k.findActive(ctx, r.site, name)
	if err == nil && !found {
		id, found, err = k.findFinished(ctx, r.site, name, prev)
	}
	if err != nil {
		k.siteDown(ctx, r, j)
		return
	}
	if found {
		k.siteAnswered(ctx, r, j)
		k.mu.Lock()
		k.adopts++
		k.mu.Unlock()
		r.siteJobID = id
		return
	}
	if prev != "" {
		if rec, err := k.accounting(ctx, r.site, prev); err == nil && rec.Name == name &&
			len(rec.State.Current) > 0 && rec.State.Current[0] != "CANCELLED" {
			r.siteJobID = prev
			return
		}
	}
	id, refused, err := k.submit(ctx, r, j, name)
	switch {
	case err != nil && !refused: // no answer, or a server error: the job may exist; the next round starts with the adopt step
		k.siteDown(ctx, r, j)
		return
	case err != nil:
		k.podFailed(ctx, r)
		return
	}
	k.siteAnswered(ctx, r, j)
	r.siteJobID = id
	k.report(ctx, r, j, func(rs *hpcv1alpha1.RunStatus) { rs.SiteJobID, rs.State = id, "PENDING" })
}

// siteDown is the runner's wait: ask again after 5, 10, 20 ... 60 s, with pendingReason SiteUnavailable shown,
// and fail the Job only past siteWaitMax. A runner pod is not restarted for it.
func (k *kubelet) siteDown(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
	now := time.Now()
	if r.waitSince.IsZero() {
		r.waitSince, r.waitDelay = now, siteRetryFirst
		k.setPendingReason(ctx, j, hpcv1alpha1.ReasonSiteUnavailable)
	}
	if now.Sub(r.waitSince) > siteWaitMax {
		r.fails = 99
		k.podFailed(ctx, r) // fails the batch Job at once
		return
	}
	k.mu.Lock()
	r.nextTry = now.Add(r.waitDelay)
	k.mu.Unlock()
	r.waitDelay = min(r.waitDelay*2, siteRetryMax)
}

// siteAnswered ends a wait: the pending reason is cleared.
func (k *kubelet) siteAnswered(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
	if !r.waitSince.IsZero() {
		r.waitSince = time.Time{}
		k.setPendingReason(ctx, j, "")
	}
}

// setPendingReason writes status.pendingReason like Reporter.pending_reason (best effort).
func (k *kubelet) setPendingReason(ctx context.Context, j *hpcv1alpha1.HPCArgoWorkflowJob, reason string) {
	orig := j.DeepCopy()
	j.Status.PendingReason = reason
	_ = k.c.Status().Patch(ctx, j, client.MergeFrom(orig))
}

// podFailed models a runner pod exiting: Kubernetes restarts it after 10, 20, 40, 80 s and fails the
// Job after the fifth try (backoffLimit 4).
func (k *kubelet) podFailed(ctx context.Context, r *frunner) {
	r.fails++
	if r.fails > 4 {
		var rj batchv1.Job
		if err := k.c.Get(ctx, client.ObjectKeyFromObject(&r.rj), &rj); err == nil {
			now := metav1.Now()
			rj.Status.StartTime = &now
			rj.Status.Failed = int32(r.fails)
			rj.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
			_ = k.c.Status().Update(ctx, &rj)
		}
		k.finish(r)
		return
	}
	k.mu.Lock()
	r.nextTry = time.Now().Add(time.Duration(10<<(r.fails-1)) * time.Second)
	k.mu.Unlock()
}

type submitReq struct {
	Job    map[string]any `json:"job"`
	Script string         `json:"script"`
}

func (k *kubelet) submit(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob, name string) (id string, refused bool, err error) {
	cls := r.site.classes[j.Spec.Class]
	qos, resv, limit := cls.QOS, "", int(j.Spec.WallTimeMinutes)
	if p := j.Status.Placement; p != nil {
		if p.QOS != "" {
			qos = p.QOS
		}
		resv, limit = p.Reservation, int(p.TimeLimitMinutes)
	}
	env := []string{"KAOS_ATTEMPT=" + strconv.Itoa(int(r.attempt))}
	if rs := j.Status.Resume; rs != nil && rs.CheckpointRef != "" {
		env = append(env, "KAOS_RESUME_REF="+rs.CheckpointRef)
	}
	for ann, key := range map[string]string{annRunMinutes: "FAKE_RUN_MINUTES", annForceEnd: "FAKE_FORCE_END", annSegments: "FAKE_SEGMENTS"} {
		if v := j.Annotations[ann]; v != "" {
			env = append(env, key+"="+v)
		}
	}
	body := submitReq{Job: map[string]any{"name": name, "partition": cls.Partition, "account": r.site.account, "qos": qos,
		"time_limit": limit, "comment": "kaos-ckpt-lease=" + string(j.UID) + "/" + j.Spec.StepName, "environment": env}}
	if resv != "" {
		body.Job["reservation"] = resv
	}
	k.mu.Lock()
	k.submits++
	k.mu.Unlock()
	id, refusal, err := k.post(ctx, r.site, body)
	if refusal != "" && (qos != cls.QOS || resv != "") && laneRefusal(refusal) {
		// runner.submit_in_lane: a refused lane is tried once more on the class's own QoS, no reservation.
		body.Job["qos"] = cls.QOS
		delete(body.Job, "reservation")
		qos, resv = cls.QOS, ""
		id, refusal, err = k.post(ctx, r.site, body)
	}
	if err == nil && refusal != "" {
		return "", true, fmt.Errorf("refused: %s", refusal)
	}
	if err != nil {
		return "", false, err
	}
	r.rj.Annotations["fake.qos"], r.rj.Annotations["fake.reservation"] = qos, resv
	return id, false, nil
}

func laneRefusal(text string) bool {
	t := strings.ToLower(text)
	return strings.Contains(t, "qos") || strings.Contains(t, "reservation") || strings.Contains(t, "time limit is invalid")
}

// post returns the site job id, or the site's refusal text (HTTP 4xx), or an error (5xx, network).
func (k *kubelet) post(ctx context.Context, s *siteWiring, body submitReq) (id, refusal string, err error) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/slurm/"+s.version+"/job/submit", bytes.NewReader(b))
	s.headers(req)
	resp, err := k.http.Do(req)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode / 100 {
	case 2:
		var out struct {
			JobID int `json:"job_id"`
		}
		if json.Unmarshal(raw, &out) != nil || out.JobID == 0 {
			return "", "", fmt.Errorf("submit: no job id")
		}
		return strconv.Itoa(out.JobID), "", nil
	case 4:
		return "", string(raw), nil
	}
	return "", "", fmt.Errorf("submit: HTTP %d", resp.StatusCode)
}

func (s *siteWiring) headers(req *http.Request) {
	req.Header.Set("X-SLURM-USER-NAME", s.user)
	req.Header.Set("X-SLURM-USER-TOKEN", s.token)
}

func (k *kubelet) get(ctx context.Context, s *siteWiring, path string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.url+path, nil)
	s.headers(req)
	resp, err := k.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// findActive returns the id of the live site job with that name; found false when none; an error when the site failed.
func (k *kubelet) findActive(ctx context.Context, s *siteWiring, name string) (string, bool, error) {
	var out struct {
		Jobs []struct {
			ID    int      `json:"job_id"`
			State []string `json:"job_state"`
		} `json:"jobs"`
	}
	if err := k.get(ctx, s, "/slurm/"+s.version+"/jobs?name="+name, &out); err != nil {
		return "", false, err
	}
	for _, j := range out.Jobs {
		if len(j.State) > 0 && !terminalStates[j.State[0]] {
			return strconv.Itoa(j.ID), true, nil
		}
	}
	return "", false, nil
}

// findFinished is runner Slurm.find_finished: the newest job of that name that slurmdbd lists and that was
// not cancelled, with an id above `after` (the job the previous runner reported).
func (k *kubelet) findFinished(ctx context.Context, s *siteWiring, name, after string) (string, bool, error) {
	var out struct {
		Jobs []acctRecord `json:"jobs"`
	}
	path := fmt.Sprintf("/slurmdb/%s/jobs?job_name=%s&start_time=%d", s.version, name, time.Now().Add(-finishedLookback).Unix())
	if err := k.get(ctx, s, path, &out); err != nil {
		return "", false, err
	}
	floor, _ := strconv.Atoi(after)
	best := 0
	for _, rec := range out.Jobs {
		if rec.Name != name || len(rec.State.Current) == 0 || rec.State.Current[0] == "CANCELLED" {
			continue
		}
		if rec.ID > floor && rec.ID > best {
			best = rec.ID
		}
	}
	if best == 0 {
		return "", false, nil
	}
	return strconv.Itoa(best), true, nil
}

type acctRecord struct {
	ID    int    `json:"job_id"`
	Name  string `json:"name"`
	State struct {
		Current []string `json:"current"`
	} `json:"state"`
	ExitCode struct {
		ReturnCode struct {
			Number int `json:"number"`
		} `json:"return_code"`
	} `json:"exit_code"`
	Comment struct {
		Job string `json:"job"`
	} `json:"comment"`
	Time struct {
		Start struct {
			Set    bool  `json:"set"`
			Number int64 `json:"number"`
		} `json:"start"`
		End struct {
			Set    bool  `json:"set"`
			Number int64 `json:"number"`
		} `json:"end"`
	} `json:"time"`
}

func (k *kubelet) accounting(ctx context.Context, s *siteWiring, id string) (acctRecord, error) {
	var out struct {
		Jobs []acctRecord `json:"jobs"`
	}
	if err := k.get(ctx, s, "/slurmdb/"+s.version+"/job/"+id, &out); err != nil || len(out.Jobs) == 0 {
		return acctRecord{}, fmt.Errorf("accounting %s: %v", id, err)
	}
	return out.Jobs[0], nil
}

// stepFollow reads the site job's state like runner.view(): the controller's observation while it is
// fresh, the site itself only when it is not, and the accounting record when the state changed or ended.
func (k *kubelet) stepFollow(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
	obs := j.Status.SiteObservation
	fresh := obs != nil && obs.JobID == r.siteJobID &&
		time.Since(obs.ObservedAt.Time) <= 3*time.Duration(obs.IntervalSeconds)*time.Second+time.Second
	if r.watchSeen.IsZero() {
		r.watchSeen = time.Now()
	}
	var state string
	switch {
	case fresh && obs.State != "":
		state = obs.State
	case fresh: // the site no longer lists the job: its record is in accounting
		state = "?"
	default:
		// STALE_GRACE_S / STALE_POLL_S of the runner, scaled to the test's poll interval.
		if time.Since(r.watchSeen) < 2*k.obsTTL || time.Since(r.lastRest) < 2*k.obsTTL {
			return
		}
		r.lastRest = time.Now()
		k.mu.Lock()
		k.restFall++
		k.mu.Unlock()
		state = "?"
	}
	first := strings.Split(state, ",")[0]
	if state != "?" && !terminalStates[first] {
		if state != r.reported {
			if rec, err := k.accounting(ctx, r.site, r.siteJobID); err == nil {
				r.reported = state
				k.report(ctx, r, j, func(rs *hpcv1alpha1.RunStatus) {
					rs.SiteJobID, rs.State = r.siteJobID, state
					rs.StartTime = siteTime(rec.Time.Start.Set, rec.Time.Start.Number)
				})
			}
		}
		return
	}
	rec, err := k.accounting(ctx, r.site, r.siteJobID)
	if err != nil || len(rec.State.Current) == 0 || !terminalStates[rec.State.Current[0]] {
		return // not over (or unreadable): ask again next tick
	}
	k.reportEnd(ctx, r, j, rec)
}

func siteTime(set bool, n int64) *metav1.Time {
	if !set || n <= 0 {
		return nil
	}
	t := metav1.NewTime(time.Unix(n, 0).UTC())
	return &t
}

// reportEnd writes the one terminal report of the attempt.
func (k *kubelet) reportEnd(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob, rec acctRecord) {
	st := rec.State.Current[0]
	rc := int32(rec.ExitCode.ReturnCode.Number)
	seq := 0
	if _, after, ok := strings.Cut(rec.Comment.Job, "kaos-ckpt-seq="); ok {
		seq, _ = strconv.Atoi(strings.Fields(after)[0])
	}
	recoverable := (rc == 85 || st == "NODE_FAIL" || st == "PREEMPTED" || st == "TIMEOUT") && st != "COMPLETED"
	ok := k.report(ctx, r, j, func(rs *hpcv1alpha1.RunStatus) {
		rs.SiteJobID, rs.State, rs.ExitCode, rs.Terminal = r.siteJobID, st, &rc, true
		rs.StartTime = siteTime(rec.Time.Start.Set, rec.Time.Start.Number)
		end := siteTime(rec.Time.End.Set, rec.Time.End.Number)
		if end == nil {
			n := metav1.Now()
			end = &n
		}
		rs.EndTime = end
		rs.QOS, rs.Reservation = r.rj.Annotations["fake.qos"], r.rj.Annotations["fake.reservation"]
		if recoverable && seq > 0 {
			path := fmt.Sprintf("scratch://ckpt/%s/%d.tar", j.UID, seq)
			sum := sha256.Sum256([]byte(path))
			rs.CheckpointRef = path + "#" + hex.EncodeToString(sum[:])
			rs.Checkpoint = &hpcv1alpha1.CheckpointInfo{ProgressValues: []int64{int64(seq)},
				ProgressText: "epoch=" + strconv.Itoa(seq), Digest: hex.EncodeToString(sum[:8]), Bytes: 1 << 20}
		}
	})
	if ok {
		k.finish(r)
	}
}

// report merge-patches status.run with the whole RunStatus of this attempt (the real reporter writes
// every field of the attempt, so an earlier attempt's exit code or checkpoint never lingers).
func (k *kubelet) report(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob, mod func(*hpcv1alpha1.RunStatus)) bool {
	orig := j.DeepCopy()
	rs := &hpcv1alpha1.RunStatus{Attempt: r.attempt}
	mod(rs)
	j.Status.Run = rs
	for try := 0; try < 3; try++ {
		if err := k.c.Status().Patch(ctx, j, client.MergeFrom(orig)); err == nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// stepCancel is the cancel-only runner: stop the attempt's site job and report its final state.
func (k *kubelet) stepCancel(ctx context.Context, r *frunner, j *hpcv1alpha1.HPCArgoWorkflowJob) {
	name := siteName(j, r.attempt)
	id, ok, err := k.findActive(ctx, r.site, name)
	if err != nil {
		return
	}
	reported := ""
	if run := j.Status.Run; run != nil && run.Attempt == r.attempt {
		reported = run.SiteJobID
	}
	if !ok && reported == "" {
		// A submit whose answer was lost may have run and ended unreported: settle it from accounting.
		if id, _, err = k.findFinished(ctx, r.site, name, ""); err != nil {
			return
		}
		reported = id
	}
	if ok {
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, r.site.url+"/slurm/"+r.site.version+"/job/"+id, nil)
		r.site.headers(req)
		if resp, err := k.http.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	} else {
		id = reported
	}
	rec, err := k.accounting(ctx, r.site, id)
	if err != nil || len(rec.State.Current) == 0 || !terminalStates[rec.State.Current[0]] {
		return
	}
	r.siteJobID = id
	k.reportEnd(ctx, r, j, rec)
}

// spawnedBetween counts the runner Jobs created in the given HPC namespaces within [from, to).
func (k *kubelet) spawnedBetween(ns map[string]bool, from, to time.Time) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	n := 0
	for _, r := range k.runners {
		created := r.rj.CreationTimestamp.Time
		if ns[r.rj.Namespace] && !created.Before(from) && created.Before(to) {
			n++
		}
	}
	return n
}
