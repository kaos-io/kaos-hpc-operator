package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// PRD-HPC-1461 AC-50 (CON-03, D-53): a site job runs as the project identity of the KAOS project whose environment
// namespace holds the HPCArgoWorkflowJob, whichever environment namespace of the project it is in; a spec cannot
// name another identity.
var _ = Describe("HPCArgoWorkflowJob site identity", func() {
	var (
		r      *HPCArgoWorkflowJobReconciler
		suffix string
	)

	// project creates a KAOS project's namespaces (two environment namespaces and the one that holds its HPCSite)
	// and its HPCSite for one site user. It returns the two environment namespaces.
	project := func(name, siteUser string) (string, string) {
		envs := []string{name + "-train", name + "-prod"}
		for _, n := range append([]string{name + "-ml"}, envs...) {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: name}}})).To(Succeed())
		}
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: name + "-ml", Labels: map[string]string{hpcv1alpha1.ProjectLabel: name}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: siteUser, Account: "acct-" + siteUser,
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 1000}},
		})).To(Succeed())
		return envs[0], envs[1]
	}

	spec := func(seq int) hpcv1alpha1.HPCArgoWorkflowJobSpec {
		return hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: fmt.Sprintf("reg.example/id/app%d:v1", seq),
			WallTimeMinutes: 20, Command: []string{"python", "train.py"}, StepName: "train"}
	}

	// runnerEnv reconciles the job once and returns the environment of its runner Job, which lives in the
	// project's HPCSite namespace.
	runnerEnv := func(ns, siteNS, name string, seq int) map[string]string {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: spec(seq)}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		rj := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: siteNS, Name: runnerjob.Name(got.UID, 1)}, rj)).To(Succeed())
		m := map[string]string{}
		for _, e := range rj.Spec.Template.Spec.Containers[0].Env {
			m[e.Name] = e.Value
		}
		return m
	}

	BeforeEach(func() {
		suffix = fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test"}
	})

	It("should_run_the_jobs_of_both_environment_namespaces_of_one_project_as_its_identity_and_another_project_as_its_own", func() {
		a, b := "ida"+suffix, "idb"+suffix
		aTrain, aProd := project(a, "ua"+suffix)
		bTrain, _ := project(b, "ub"+suffix)

		first := runnerEnv(aTrain, a+"-ml", "j1", 1)
		second := runnerEnv(aProd, a+"-ml", "j2", 2)
		other := runnerEnv(bTrain, b+"-ml", "j3", 3)

		Expect(first).To(HaveKeyWithValue("HPC_USER", "ua"+suffix))
		Expect(first).To(HaveKeyWithValue("HPC_ACCOUNT", "acct-ua"+suffix))
		Expect(second).To(HaveKeyWithValue("HPC_USER", "ua"+suffix), "the second environment namespace of the project runs as the same identity")
		Expect(second).To(HaveKeyWithValue("HPC_ACCOUNT", "acct-ua"+suffix))
		Expect(other).To(HaveKeyWithValue("HPC_USER", "ub"+suffix), "another project runs as its own identity")
		Expect(other).To(HaveKeyWithValue("HPC_ACCOUNT", "acct-ub"+suffix))
	})

	It("should_refuse_a_spec_that_tries_to_name_the_identity_account_or_site", func() {
		a := "idc" + suffix
		ns, _ := project(a, "uc"+suffix)
		for _, name := range []string{"HPC_USER", "HPC_ACCOUNT", "HPC_SITE", "HPC_ENDPOINT"} {
			s := spec(9)
			s.Env = []hpcv1alpha1.NameValue{{Name: name, Value: "someone-else"}}
			err := k8sClient.Create(ctx, &hpcv1alpha1.HPCArgoWorkflowJob{
				ObjectMeta: metav1.ObjectMeta{Name: "evil-" + name2k8s(name), Namespace: ns}, Spec: s})
			Expect(err).To(HaveOccurred(), "the CRD must refuse env %s", name)
		}
	})
})

// name2k8s turns an environment variable name into a DNS-safe object name part.
func name2k8s(env string) string {
	out := []byte{}
	for _, c := range []byte(env) {
		switch {
		case c >= 'A' && c <= 'Z':
			out = append(out, c+('a'-'A'))
		case c == '_':
			out = append(out, '-')
		default:
			out = append(out, c)
		}
	}
	return string(out)
}
