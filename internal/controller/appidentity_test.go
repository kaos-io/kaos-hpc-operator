package controller

import (
	"context"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// PRD kaos#1911 AC-14: the runner's service account may create a token for the job's app service account and for no other.

func appIdentityFixture(t *testing.T) (client.Client, *HPCArgoWorkflowJobReconciler) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	return c, &HPCArgoWorkflowJobReconciler{Client: c, Scheme: s}
}

func appJob(app string) (*hpcv1alpha1.HPCArgoWorkflowJob, *hpcv1alpha1.HPCSite) {
	return &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "env-ns"}, Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: app}},
		&hpcv1alpha1.HPCSite{ObjectMeta: metav1.ObjectMeta{Name: "site", Namespace: "ml-ns"}}
}

func TestShouldLetTheRunnerCreateATokenForTheAppsWorkflowServiceAccountOnly(t *testing.T) {
	c, r := appIdentityFixture(t)
	job, site := appJob("yolo")
	for range 2 { // idempotent: the second round replaces what the first made
		if err := r.ensureAppIdentityAccess(context.Background(), job, site); err != nil {
			t.Fatal(err)
		}
	}
	var role rbacv1.Role
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "env-ns", Name: "hpc-runner-token-yolo"}, &role); err != nil {
		t.Fatal(err)
	}
	if len(role.Rules) != 1 {
		t.Fatalf("rules %+v: want exactly one", role.Rules)
	}
	rule := role.Rules[0]
	if len(rule.Resources) != 1 || rule.Resources[0] != "serviceaccounts/token" || len(rule.Verbs) != 1 || rule.Verbs[0] != "create" ||
		len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != "yolo-workflow" {
		t.Fatalf("rule %+v: want create serviceaccounts/token of yolo-workflow only", rule)
	}
	var rb rbacv1.RoleBinding
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "env-ns", Name: "hpc-runner-token-yolo"}, &rb); err != nil {
		t.Fatal(err)
	}
	if rb.RoleRef.Kind != "Role" || rb.RoleRef.Name != "hpc-runner-token-yolo" || len(rb.Subjects) != 1 ||
		rb.Subjects[0].Name != "hpc-runner" || rb.Subjects[0].Namespace != "ml-ns" {
		t.Fatalf("binding %+v", rb)
	}
}

func TestShouldGiveAJobWithoutAnAppNoAccessToAnyToken(t *testing.T) {
	c, r := appIdentityFixture(t)
	job, site := appJob("")
	if err := r.ensureAppIdentityAccess(context.Background(), job, site); err != nil {
		t.Fatal(err)
	}
	var roles rbacv1.RoleList
	if err := c.List(context.Background(), &roles); err != nil || len(roles.Items) != 0 {
		t.Fatalf("roles %+v, err %v: want none", roles.Items, err)
	}
}
