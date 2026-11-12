package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
)

// The poller against a real API server: the merge patch of status.siteObservation is accepted by
// the CRD schema and leaves the runner's status.run alone.
var _ = Describe("SitePoller", func() {
	It("writes status.siteObservation through the status subresource and keeps status.run", func() {
		ns := fmt.Sprintf("poll%d", time.Now().UnixNano()%1_000_000_000)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
		for _, o := range pollSite(ns, "https://s.example") {
			o.SetNamespace(ns)
			Expect(k8sClient.Create(ctx, o)).To(Succeed())
		}
		job := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: ns},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "c", Image: "reg.example/img:1", WallTimeMinutes: 5}}
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		job.Status = pollJob(ns, "j", ns, "4711").Status
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		comment := "kaos-state=taken host=9 slot=1"
		p := &SitePoller{Client: k8sClient, Secrets: k8sClient, Interval: 30 * time.Second, BucketRate: 100, BucketBurst: 10,
			Reader: &fakeReader{reply: map[string]sitestate.Job{
				"4711": {ID: "4711", State: []string{"PENDING"}, Reason: "Resources", Comment: &comment}}}}
		p.PollOnce(ctx)

		var got hpcv1alpha1.HPCArgoWorkflowJob
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(job), &got)).To(Succeed())
		Expect(got.Status.SiteObservation).NotTo(BeNil())
		Expect(got.Status.SiteObservation.State).To(Equal("PENDING"))
		Expect(got.Status.SiteObservation.PendingReason).To(Equal("Resources"))
		Expect(*got.Status.SiteObservation.Comment).To(Equal(comment))
		Expect(got.Status.Run.SiteJobID).To(Equal("4711"))
	})
})
