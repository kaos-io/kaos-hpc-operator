package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
)

// DefaultSitePollInterval is how often the site is read, once per HPCSite (PRD-HPC-1461 D-36).
const DefaultSitePollInterval = 30 * time.Second

// siteTokenKey is the key of the JWT in the runnerjob.SecretJWT Secret.
const siteTokenKey = "token"

// StateReader reads the state of explicit site job IDs (sitestate.Client implements it).
type StateReader interface {
	States(ctx context.Context, s sitestate.Site, bucket sitestate.Waiter, ids []string) (map[string]sitestate.Job, error)
}

// Pinger asks a site whether its REST API answers (sitestate.Client implements it). A StateReader that is also
// a Pinger lets the poller keep the sites' health (PRD-HPC-1461 AC-30) even while no site job is polled.
type Pinger interface {
	Ping(ctx context.Context, s sitestate.Site, bucket sitestate.Waiter) (sitestate.Verdict, string)
}

// SitePoller reads the state of every non-terminal job's site job with one batched call per
// HPCSite per interval (chunked when the list is long) and writes it to each job's
// status.siteObservation, so runners never poll the site for state themselves (D-36, T-25).
// A failed read leaves every status as it was: the runners fall back on their own when an
// observation grows older than three intervals. It implements manager.Runnable and runs on the leader only.
type SitePoller struct {
	Client client.Client
	// Secrets reads the site JWT without the informer cache (the JWT rotates; a Secret cache would
	// need a watch on every Secret of the cluster).
	Secrets client.Reader
	Reader  StateReader
	// Interval between polls; DefaultSitePollInterval when zero.
	Interval time.Duration
	// Health receives what every poll and probe learned about each site's reachability; the reconciler reads it
	// (AC-30). Nil: reachability is not tracked and no job ever waits for a site.
	Health *sitestate.Health
	// Republish is called when a site's reachability changed, so its HPCSite condition SiteReachable is
	// written at once and not at the next job reconcile. Optional.
	Republish func(ctx context.Context, site *hpcv1alpha1.HPCSite)
	// BucketRate and BucketBurst shape the token bucket of one site profile (shared by all of its HPCSites).
	BucketRate  float64
	BucketBurst int
	Now         func() time.Time

	mu      sync.Mutex
	buckets map[string]*sitestate.Bucket
}

// NeedLeaderElection keeps two replicas from polling the same site.
func (p *SitePoller) NeedLeaderElection() bool { return true }

// Start polls every interval until ctx ends.
func (p *SitePoller) Start(ctx context.Context) error {
	t := time.NewTicker(p.interval())
	defer t.Stop()
	for {
		p.PollOnce(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (p *SitePoller) interval() time.Duration {
	if p.Interval > 0 {
		return p.Interval
	}
	return DefaultSitePollInterval
}

func (p *SitePoller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// polledJob is a job whose site job the poller reads.
type polledJob struct {
	key   types.NamespacedName
	jobID string
}

// PollOnce reads every site once and writes the observations. Errors are logged, never returned:
// one site's failure must not stop the others.
func (p *SitePoller) PollOnce(ctx context.Context) {
	log := logf.FromContext(ctx).WithName("sitepoll")
	var jobs hpcv1alpha1.HPCArgoWorkflowJobList
	if err := p.Client.List(ctx, &jobs); err != nil {
		log.Error(err, "listing jobs")
		return
	}
	groups := groupBySite(jobs.Items)
	for site, group := range groups {
		if err := p.pollSite(ctx, site, group); err != nil {
			log.Error(err, "site poll failed; observations left as they were", "site", site.String())
		}
	}
	p.probeIdleSites(ctx, jobs.Items, groups)
}

// probeIdleSites pings every slurmrestd HPCSite that was not just read through jobs/state, while any job is
// unfinished: a job that waits to be admitted or submitted has no site job to poll, yet it must know whether
// the site answers (AC-30). With no unfinished job nothing is probed.
func (p *SitePoller) probeIdleSites(ctx context.Context, items []hpcv1alpha1.HPCArgoWorkflowJob, polled map[types.NamespacedName][]polledJob) {
	pinger, ok := p.Reader.(Pinger)
	if p.Health == nil || !ok || !anyUnfinished(items) {
		return
	}
	var sites hpcv1alpha1.HPCSiteList
	if err := p.Client.List(ctx, &sites); err != nil {
		logf.FromContext(ctx).Error(err, "listing sites to probe")
		return
	}
	for i := range sites.Items {
		site := &sites.Items[i]
		if _, done := polled[client.ObjectKeyFromObject(site)]; done || site.Spec.Profile.Transport != hpcv1alpha1.TransportSlurmREST {
			continue
		}
		creds, err := p.credentials(ctx, site)
		if err != nil {
			logf.FromContext(ctx).Error(err, "probe: no credentials", "site", client.ObjectKeyFromObject(site).String())
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, p.interval())
		v, why := pinger.Ping(cctx, creds, p.bucket(site))
		cancel()
		p.observe(ctx, site, v, why)
	}
}

func anyUnfinished(items []hpcv1alpha1.HPCArgoWorkflowJob) bool {
	for i := range items {
		if !items[i].Status.Phase.Terminal() {
			return true
		}
	}
	return false
}

// credentials reads the site endpoint and JWT of an HPCSite.
func (p *SitePoller) credentials(ctx context.Context, site *hpcv1alpha1.HPCSite) (sitestate.Site, error) {
	var secret corev1.Secret
	if err := p.Secrets.Get(ctx, client.ObjectKey{Namespace: site.Namespace, Name: runnerjob.SecretJWT}, &secret); err != nil {
		return sitestate.Site{}, fmt.Errorf("get Slurm token Secret: %w", err)
	}
	token := strings.TrimSpace(string(secret.Data[siteTokenKey]))
	if token == "" {
		return sitestate.Site{}, fmt.Errorf("the Slurm token Secret %s has no %q", runnerjob.SecretJWT, siteTokenKey)
	}
	return sitestate.Site{Endpoint: site.Spec.Profile.Endpoint, RESTVersion: site.Spec.Profile.RESTVersion,
		User: site.Spec.Identity.User, Token: token}, nil
}

// observe records a verdict in the health store and logs a change of state.
func (p *SitePoller) observe(ctx context.Context, site *hpcv1alpha1.HPCSite, v sitestate.Verdict, why string) {
	key := client.ObjectKeyFromObject(site).String()
	if p.Health.Observe(key, v, why, p.now()) {
		_, down := p.Health.Down(key)
		logf.FromContext(ctx).Info("site reachability changed", "site", key, "down", down, "why", why)
		if p.Republish != nil {
			p.Republish(ctx, site)
		}
	}
}

// groupBySite collects the jobs to poll by their HPCSite. A job is polled while its attempt's site
// job is known and has not been reported over, and the job itself is not finished.
func groupBySite(items []hpcv1alpha1.HPCArgoWorkflowJob) map[types.NamespacedName][]polledJob {
	out := map[types.NamespacedName][]polledJob{}
	for i := range items {
		j := &items[i]
		run := j.Status.Run
		if j.Status.Phase.Terminal() || j.Status.Site == nil || run == nil || run.SiteJobID == "" ||
			run.Terminal || run.Attempt != j.Status.Attempt {
			continue
		}
		site := types.NamespacedName{Namespace: j.Status.Site.Namespace, Name: j.Status.Site.Name}
		out[site] = append(out[site], polledJob{key: client.ObjectKeyFromObject(j), jobID: run.SiteJobID})
	}
	return out
}

func (p *SitePoller) pollSite(ctx context.Context, key types.NamespacedName, group []polledJob) error {
	var site hpcv1alpha1.HPCSite
	if err := p.Client.Get(ctx, key, &site); err != nil {
		return fmt.Errorf("get HPCSite: %w", err)
	}
	if site.Spec.Profile.Transport != hpcv1alpha1.TransportSlurmREST {
		return nil
	}
	creds, err := p.credentials(ctx, &site)
	if err != nil {
		return err
	}
	ids := uniqueIDs(group)
	// A poll that outlasts its interval would overlap the next one: bound it.
	cctx, cancel := context.WithTimeout(ctx, p.interval())
	defer cancel()
	seen, err := p.Reader.States(cctx, creds, p.bucket(&site), ids)
	if err != nil {
		// The site not answering is a fact about the site, not about the poll: remember it (AC-30).
		if sitestate.IsOutage(err) {
			v, why := p.confirmOutage(ctx, &site, creds, err)
			p.observe(ctx, &site, v, why)
		} else if sitestate.IsAnswer(err) {
			// A 4xx (e.g. 422 for job IDs the site has forgotten) is the site answering: after an outage it
			// must not keep the site marked down just because the read itself still fails.
			p.observe(ctx, &site, sitestate.Up, "")
		}
		return err
	}
	p.observe(ctx, &site, sitestate.Up, "")
	at := metav1.NewTime(p.now())
	for _, pj := range group {
		obs := &hpcv1alpha1.SiteObservation{JobID: pj.jobID, ObservedAt: at, IntervalSeconds: int32(p.interval() / time.Second)}
		if j, ok := seen[pj.jobID]; ok {
			obs.State, obs.PendingReason, obs.Comment = strings.Join(j.State, ","), j.Reason, j.Comment
		}
		if err := p.write(ctx, pj.key, obs); err != nil {
			logf.FromContext(ctx).Error(err, "writing observation", "job", pj.key.String())
		}
	}
	return nil
}

// confirmOutage decides what a failed jobs/state read says about the site. The read can fail while the site
// is fine (a 500 for one batch, a slow reply to a long ID list), so the cheaper ping gets the last word: a
// ping that answers means the REST API is up and the failure is not counted. A ping that does not answer, or
// that says nothing (not offered), leaves the read's failure standing as a Down probe.
func (p *SitePoller) confirmOutage(ctx context.Context, site *hpcv1alpha1.HPCSite, creds sitestate.Site, readErr error) (sitestate.Verdict, string) {
	pinger, ok := p.Reader.(Pinger)
	if !ok {
		return sitestate.Down, readErr.Error()
	}
	// The poll's own deadline may be spent; the ping gets a fresh one.
	cctx, cancel := context.WithTimeout(ctx, p.interval())
	defer cancel()
	if v, _ := pinger.Ping(cctx, creds, p.bucket(site)); v == sitestate.Up {
		logf.FromContext(ctx).Info("jobs/state failed but the site answers a ping: not an outage",
			"site", client.ObjectKeyFromObject(site).String(), "error", readErr.Error())
		return sitestate.Up, ""
	}
	return sitestate.Down, readErr.Error()
}

func uniqueIDs(group []polledJob) []string {
	ids := make([]string, 0, len(group))
	for _, pj := range group {
		ids = append(ids, pj.jobID)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// write merge-patches only status.siteObservation, so it never overwrites what the runner or the
// reconciler wrote meanwhile. An observation of a job the runner has since replaced (another
// site job id) is dropped.
func (p *SitePoller) write(ctx context.Context, key types.NamespacedName, obs *hpcv1alpha1.SiteObservation) error {
	var job hpcv1alpha1.HPCArgoWorkflowJob
	if err := p.Client.Get(ctx, key, &job); err != nil {
		return client.IgnoreNotFound(err)
	}
	if job.Status.Run == nil || job.Status.Run.SiteJobID != obs.JobID || job.Status.Run.Terminal {
		return nil
	}
	orig := job.DeepCopy()
	job.Status.SiteObservation = obs
	return client.IgnoreNotFound(p.Client.Status().Patch(ctx, &job, client.MergeFrom(orig)))
}

// bucket returns the token bucket of the site's profile: one per profile name and endpoint, shared
// by every HPCSite of that site (D-36, corrected 2026-11-05).
func (p *SitePoller) bucket(site *hpcv1alpha1.HPCSite) *sitestate.Bucket {
	k := site.Spec.Site + "|" + site.Spec.Profile.Endpoint
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.buckets == nil {
		p.buckets = map[string]*sitestate.Bucket{}
	}
	if b, ok := p.buckets[k]; ok {
		return b
	}
	b := sitestate.NewBucket(p.BucketRate, p.BucketBurst)
	p.buckets[k] = b
	return b
}
