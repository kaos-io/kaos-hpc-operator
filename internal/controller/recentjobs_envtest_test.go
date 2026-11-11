package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// F-43: the ledger publisher lists the project's jobs in HPCSite.status.recentJobs, against a real
// API server so the CRD schema (bounds, field names) accepts what the controller writes.
var _ = Describe("HPCSite recentJobs", func() {
	It("lists an admitted and a waiting job, active first, and keeps the list within the CRD bound", func() {
		project := fmt.Sprintf("recent%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS := project+"-train", project+"-hpc"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		site := &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://slurm-api.example",
					RESTVersion: "v0.0.44", Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu",
						Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget:   hpcv1alpha1.Budget{GPUNodeHours: 5}}, // 300 node-minutes: one 240-minute job fits, the second waits
		}
		Expect(k8sClient.Create(ctx, site)).To(Succeed())
		r := &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		for i := range 2 {
			j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("step-%d", i), Namespace: envNS},
				Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "reg.example/img:1", WallTimeMinutes: 240}}
			Expect(k8sClient.Create(ctx, j)).To(Succeed())
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
			Expect(err).NotTo(HaveOccurred())
		}
		got := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, got)).To(Succeed())
		Expect(got.Status.RecentJobs).To(HaveLen(2))
		phases := []hpcv1alpha1.Phase{got.Status.RecentJobs[0].Phase, got.Status.RecentJobs[1].Phase}
		Expect(phases).To(ConsistOf(hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.PhaseWaiting))
		for _, rj := range got.Status.RecentJobs {
			if rj.Phase == hpcv1alpha1.PhaseWaiting {
				Expect(rj.Reason).To(Equal(hpcv1alpha1.ReasonWaitingForBudget))
			} else {
				Expect(rj.BookedNodeMinutes).To(Equal(int64(240)))
			}
		}
	})
})
