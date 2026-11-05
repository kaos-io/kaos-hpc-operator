/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// LedgerResync is how often a site's ledger is published again without a change: the period
// rolls over (D-11) and jobs that ended elsewhere settle.
const LedgerResync = 10 * time.Minute

// HPCSiteReconciler publishes a site's ledger (used, booked, left, active jobs) when the site is
// created or its spec changes, and every LedgerResync. Without it a project that has run nothing
// shows no budget, and a new budget figure from the site appears only with the next job (F-30).
// It reuses the job reconciler's ledger so both count the same bookings.
type HPCSiteReconciler struct {
	client.Client
	Jobs *HPCArgoWorkflowJobReconciler
}

func (r *HPCSiteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var site hpcv1alpha1.HPCSite
	if err := r.Get(ctx, req.NamespacedName, &site); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := r.Jobs.publishLedger(ctx, &site); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: LedgerResync}, nil
}

// SetupWithManager watches sites by spec generation only, so the ledger this reconciler writes to
// the status does not wake it again.
func (r *HPCSiteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hpcv1alpha1.HPCSite{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("hpcsite").
		Complete(r)
}
