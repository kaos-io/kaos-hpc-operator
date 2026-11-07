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
	for site, group := range groupBySite(jobs.Items) {
		if err := p.pollSite(ctx, site, group); err != nil {
			log.Error(err, "site poll failed; observations left as they were", "site", site.String())
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
	var secret corev1.Secret
	if err := p.Secrets.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: runnerjob.SecretJWT}, &secret); err != nil {
		return fmt.Errorf("get Slurm token Secret: %w", err)
	}
	token := strings.TrimSpace(string(secret.Data[siteTokenKey]))
	if token == "" {
		return fmt.Errorf("the Slurm token Secret %s has no %q", runnerjob.SecretJWT, siteTokenKey)
	}
	ids := uniqueIDs(group)
	// A poll that outlasts its interval would overlap the next one: bound it.
	cctx, cancel := context.WithTimeout(ctx, p.interval())
	defer cancel()
	seen, err := p.Reader.States(cctx, sitestate.Site{Endpoint: site.Spec.Profile.Endpoint,
		RESTVersion: site.Spec.Profile.RESTVersion, User: site.Spec.Identity.User, Token: token},
		p.bucket(&site), ids)
	if err != nil {
		return err
	}
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
