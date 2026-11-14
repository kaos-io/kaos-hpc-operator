package controller

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// The runner of a job reads and writes the data store as the job's APP: it asks the cluster for a short-lived token
// of the app's workflow service account (TokenRequest) and exchanges it at the data store for the app's own access key
// (runner/kaos_hpc_runner/identity.py). The runner's service account may create that token for the app's workflow
// service account and for no other: a Role in the app's namespace names exactly that service account (resourceNames),
// and a RoleBinding gives it to the runner's service account. The controller creates both before it creates the runner
// Job. Both are shared by every job of the app and are removed with the namespace.

// appServiceAccount is the app's workflow service account (the data store's token exchange accepts that subject only).
func appServiceAccount(app string) string { return app + "-workflow" }

// appTokenRoleName is the Role and RoleBinding of one app.
func appTokenRoleName(app string) string { return "hpc-runner-token-" + app }

// +kubebuilder:rbac:groups="",resources=serviceaccounts/token,verbs=create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=create;update

// ensureAppIdentityAccess makes the runner's service account (in the site's namespace) able to create a token for the
// job's app service account, in the job's namespace, and only for that. Idempotent. A job without an app gets nothing:
// the runner refuses it before staging (the data store has no identity for it).
//
// The controller holds `create serviceaccounts/token` itself because Kubernetes lets nobody grant a right they do not
// hold (privilege escalation prevention): the broad right is the controller's, the runner pods hold the narrow one.
func (r *HPCArgoWorkflowJobReconciler) ensureAppIdentityAccess(ctx context.Context, job *hpcv1alpha1.HPCArgoWorkflowJob, site *hpcv1alpha1.HPCSite) error {
	app := job.Spec.App
	if app == "" {
		return nil
	}
	name := appTokenRoleName(app)
	meta := metav1.ObjectMeta{Name: name, Namespace: job.Namespace, Labels: map[string]string{
		"app.kubernetes.io/managed-by": "kaos-hpc-operator", "hpc.kaos.io/app": app}}
	role := &rbacv1.Role{ObjectMeta: meta, Rules: []rbacv1.PolicyRule{{
		APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"},
		ResourceNames: []string{appServiceAccount(app)}, Verbs: []string{"create"}}}}
	binding := &rbacv1.RoleBinding{ObjectMeta: meta,
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
		Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: runnerjob.ServiceAccount, Namespace: site.Namespace}}}
	if err := r.createOrReplace(ctx, role); err != nil {
		return fmt.Errorf("the runner's access to the token of service account %s in %s (Role %s): %w", appServiceAccount(app), job.Namespace, name, err)
	}
	if err := r.createOrReplace(ctx, binding); err != nil {
		return fmt.Errorf("the runner's access to the token of service account %s in %s (RoleBinding %s): %w", appServiceAccount(app), job.Namespace, name, err)
	}
	return nil
}

// createOrReplace creates the object, or replaces it when it exists (without a read: the controller's cache does not
// watch Roles). Both objects are fully ours, so a replace never loses anything.
func (r *HPCArgoWorkflowJobReconciler) createOrReplace(ctx context.Context, obj client.Object) error {
	err := r.Create(ctx, obj)
	if apierrors.IsAlreadyExists(err) {
		return r.Update(ctx, obj)
	}
	return err
}
