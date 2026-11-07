package controller

import (
	"context"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// DefaultLedgerPublishInterval is the shortest time between two writes of one HPCSite's status. Every
// reconcile of every job asks for the ledger to be published; without a bound a busy project wrote its
// HPCSite (about 23 KiB with 50 recent jobs) dozens of times per second, in step with the reconciles
// (scale test, 1,000 jobs: 31 writes in one second).
const DefaultLedgerPublishInterval = time.Second

// publishLedger asks for the site's ledger to be written to its status. At most one write per
// LedgerPublishInterval and site goes out; a request inside the interval schedules one deferred write
// that publishes the newest state when the interval has passed, so the status is never left behind.
func (r *HPCArgoWorkflowJobReconciler) publishLedger(ctx context.Context, site *hpcv1alpha1.HPCSite) error {
	key := client.ObjectKeyFromObject(site)
	if wait := r.ledgerWait(key); wait > 0 {
		r.publishLater(key, wait)
		return nil
	}
	return r.publishLedgerNow(ctx, site)
}

// ledgerWait returns 0 and reserves the write when one may go out now, else how long to wait.
func (r *HPCArgoWorkflowJobReconciler) ledgerWait(key client.ObjectKey) time.Duration {
	if r.LedgerPublishInterval <= 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published == nil {
		r.published = map[client.ObjectKey]time.Time{}
	}
	if since := time.Since(r.published[key]); since < r.LedgerPublishInterval {
		return r.LedgerPublishInterval - since
	}
	r.published[key] = time.Now()
	return 0
}

// publishLater schedules the one deferred write of a site (a second request while it is pending
// adds nothing: the write reads the state when it runs).
func (r *HPCArgoWorkflowJobReconciler) publishLater(key client.ObjectKey, wait time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.trailing == nil {
		r.trailing = map[client.ObjectKey]bool{}
	}
	if r.trailing[key] {
		return
	}
	r.trailing[key] = true
	time.AfterFunc(wait, func() {
		r.mu.Lock()
		delete(r.trailing, key)
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var site hpcv1alpha1.HPCSite
		if err := r.Get(ctx, key, &site); err != nil {
			return
		}
		if err := r.publishLedger(ctx, &site); err != nil {
			logf.FromContext(ctx).Error(err, "publishing the ledger", "site", key)
		}
	})
}
