/*
Copyright 2026 The KAOS Authors.

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

// Phase 0 (PRD-HPC-1461 F-26): the operator admits, books and settles HPCArgoWorkflowJobs. These specs drive
// the reconciler against a real API server (envtest).
var _ = Describe("HPCArgoWorkflowJob admission", func() {
	var (
		r       *HPCArgoWorkflowJobReconciler
		project string
		envNS   string
		hpcNS   string
		seq     int
	)

	newNS := func(name, proj string) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if proj != "" {
			ns.Labels = map[string]string{hpcv1alpha1.ProjectLabel: proj}
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	}

	newSite := func(gpuHours int64, mod func(*hpcv1alpha1.HPCSite)) *hpcv1alpha1.HPCSite {
		s := &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{
				Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.example", RESTVersion: "v0.0.44",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu",
						Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: gpuHours},
			},
		}
		if mod != nil {
			mod(s)
		}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		return s
	}

	newJob := func(ns string, wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("job-%d", seq), Namespace: ns},
			Spec:       hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "img", WallTimeMinutes: wall},
		}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	reconcileJob := func(j *hpcv1alpha1.HPCArgoWorkflowJob) (reconcile.Result, *hpcv1alpha1.HPCArgoWorkflowJob) {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return res, got
	}

	report := func(j *hpcv1alpha1.HPCArgoWorkflowJob, run hpcv1alpha1.RunStatus) {
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		got.Status.Run = &run
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
	}

	newResumable := func(ns string, wall int32, maxCont int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("job-%d", seq), Namespace: ns},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "img", WallTimeMinutes: wall,
				Resumable: &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: maxCont}},
		}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	// end85 reports that the given attempt saved and exited 85 after the given minutes.
	end85 := func(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32, minutes int, ckpt string) {
		code := hpcv1alpha1.ExitCodeContinue
		start := metav1.NewTime(time.Now().Add(-time.Duration(minutes) * time.Minute))
		endT := metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: attempt, SiteJobID: fmt.Sprintf("10%d", attempt), State: "FAILED",
			ExitCode: &code, StartTime: &start, EndTime: &endT, Terminal: true, CheckpointRef: ckpt})
	}

	siteStatus := func() hpcv1alpha1.HPCSiteStatus {
		s := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "meluxina", Namespace: hpcNS}, s)).To(Succeed())
		return s.Status
	}

	BeforeEach(func() {
		seq = 0
		project = fmt.Sprintf("proj%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-hpc"
		newNS(envNS, project)
		newNS(hpcNS, project)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	It("admits a job that fits and books nodes x wall time", func() {
		newSite(10, nil)
		_, got := reconcileJob(newJob(envNS, 240))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Booking).NotTo(BeNil())
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(240)))
		Expect(got.Status.Booking.SiteNamespace).To(Equal(hpcNS))
		st := siteStatus()
		Expect(st.GPU.BookedNodeMinutes).To(Equal(int64(240)))
		Expect(st.GPU.LeftNodeMinutes).To(Equal(int64(360)))
		Expect(st.ActiveJobs).To(Equal(int32(1)))
	})

	It("never overbooks: of five 4-hour jobs on a 10-hour share, two are admitted and three wait", func() {
		newSite(10, nil)
		var phases []hpcv1alpha1.Phase
		for range 5 {
			_, got := reconcileJob(newJob(envNS, 240))
			phases = append(phases, got.Status.Phase)
		}
		Expect(phases).To(Equal([]hpcv1alpha1.Phase{hpcv1alpha1.PhaseAdmitted, hpcv1alpha1.PhaseAdmitted,
			hpcv1alpha1.PhaseWaiting, hpcv1alpha1.PhaseWaiting, hpcv1alpha1.PhaseWaiting}))
		Expect(siteStatus().GPU.BookedNodeMinutes).To(Equal(int64(480)))
	})

	It("admits a waiting job once a running job settles below its booking", func() {
		newSite(5, nil) // 300 node-minutes
		first := newJob(envNS, 240)
		_, got := reconcileJob(first)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		second := newJob(envNS, 120)
		res, got := reconcileJob(second)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWaitingForBudget))
		Expect(res.RequeueAfter).To(Equal(WaitingResync))

		start := metav1.NewTime(time.Now().Add(-30 * time.Minute))
		end := metav1.NewTime(time.Now())
		zero := int32(0)
		report(first, hpcv1alpha1.RunStatus{SiteJobID: "100", State: "COMPLETED", ExitCode: &zero,
			StartTime: &start, EndTime: &end, Terminal: true})
		_, got = reconcileJob(first)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(30)))

		_, got = reconcileJob(second)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		st := siteStatus()
		Expect(st.GPU.UsedNodeMinutes).To(Equal(int64(30)))
		Expect(st.GPU.BookedNodeMinutes).To(Equal(int64(120)))
	})

	It("waits at the concurrency limit and is never refused for it (D-49)", func() {
		newSite(100, func(s *hpcv1alpha1.HPCSite) { s.Spec.Limits.MaxConcurrentJobs = 1 })
		_, got := reconcileJob(newJob(envNS, 60))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		_, got = reconcileJob(newJob(envNS, 60))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonConcurrencyLimit))
	})

	It("refuses a job whose namespace belongs to no project", func() {
		ns := project + "-stray"
		newNS(ns, "")
		_, got := reconcileJob(newJob(ns, 60))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoProject))
	})

	It("refuses a job of a project with no site for its class", func() {
		_, got := reconcileJob(newJob(envNS, 60))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoSite))
	})

	It("moves to Running when the submitter reports the site job", func() {
		newSite(10, nil)
		j := newJob(envNS, 60)
		reconcileJob(j)
		report(j, hpcv1alpha1.RunStatus{SiteJobID: "5425070", State: "PENDING"})
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning))
		Expect(got.Status.Run.SiteJobID).To(Equal("5425070"), "the operator never overwrites status.run")
	})

	It("charges a guest on a shared node nothing: its host's booking covers the node (D-26)", func() {
		newSite(10, nil)
		j := newJob(envNS, 60)
		reconcileJob(j)
		slot := int32(2)
		report(j, hpcv1alpha1.RunStatus{SiteJobID: "5425072", State: "PENDING", HostSiteJobID: "5425070", Slot: &slot})
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning))
		Expect(siteStatus().GPU.BookedNodeMinutes).To(BeZero())
	})

	It("fails a step that exits 85 but is not declared resumable", func() {
		newSite(10, nil)
		j := newJob(envNS, 60)
		reconcileJob(j)
		end85(j, 1, 50, "lakefs://outputs/c1")
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNotResumable))
		Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(50)))
	})

	It("continues a resumable step under the same HPCArgoWorkflowJob: settled, re-admitted, re-booked, resume info set", func() {
		newSite(10, nil)
		j := newResumable(envNS, 60, 5)
		_, got := reconcileJob(j)
		Expect(got.Status.Attempt).To(Equal(int32(1)))
		Expect(got.Status.Site).NotTo(BeNil())
		end85(j, 1, 50, "lakefs://outputs-app/ckpt-commit-1")

		res, got := reconcileJob(j)
		Expect(res.RequeueAfter).To(Equal(ContinueResync), "the next attempt is judged at once")
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhasePending))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
		Expect(got.Status.Attempt).To(Equal(int32(2)))
		Expect(got.Status.Continuations).To(Equal(int32(1)))
		Expect(got.Status.Resume).To(Equal(&hpcv1alpha1.ResumeInfo{CheckpointRef: "lakefs://outputs-app/ckpt-commit-1", FromAttempt: 1}))
		Expect(got.Status.Booking).To(BeNil())
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Attempts[0].Outcome).To(Equal("Continued"))

		_, got = reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "the continuation is admitted like a new step")
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(60)))
		st := siteStatus()
		Expect(st.GPU.UsedNodeMinutes).To(Equal(int64(50)))
		Expect(st.GPU.BookedNodeMinutes).To(Equal(int64(60)))

		// The runner's late report of attempt 1 is ignored once attempt 2 is the current one.
		_, got = reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))

		zero := int32(0)
		start := metav1.NewTime(time.Now().Add(-20 * time.Minute))
		endT := metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: 2, SiteJobID: "102", State: "COMPLETED", ExitCode: &zero,
			StartTime: &start, EndTime: &endT, Terminal: true})
		_, got = reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(70)), "50 + 20 over both attempts")
		Expect(got.Status.Attempts).To(HaveLen(2))
	})

	It("does not continue without a staged-out checkpoint: Timeout, checkpoint upload incomplete (T-19)", func() {
		newSite(10, nil)
		j := newResumable(envNS, 60, 5)
		reconcileJob(j)
		end85(j, 1, 55, "")
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonCheckpointMissing))
		Expect(got.Status.Message).To(ContainSubstring("checkpoint upload incomplete"))
	})

	It("stops at the step's continuation cap", func() {
		newSite(100, nil)
		j := newResumable(envNS, 60, 1)
		reconcileJob(j)
		end85(j, 1, 50, "c1")
		reconcileJob(j) // continuing
		reconcileJob(j) // admitted as attempt 2
		end85(j, 2, 50, "c2")
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuationLimit))
		Expect(got.Status.Continuations).To(Equal(int32(1)))
	})

	It("refuses a continuation the share can no longer cover, like a new step (block, A4)", func() {
		newSite(1, nil) // 60 node-minutes
		j := newResumable(envNS, 60, 5)
		reconcileJob(j)
		end85(j, 1, 50, "c1")
		reconcileJob(j)
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonBudgetBlocked))
	})

	It("ends a step that is not resumable at its wall time", func() {
		newSite(10, nil)
		j := newJob(envNS, 60)
		reconcileJob(j)
		start := metav1.NewTime(time.Now().Add(-60 * time.Minute))
		endT := metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "1", State: "TIMEOUT", StartTime: &start, EndTime: &endT, Terminal: true})
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
	})

	It("ignores a report without an attempt number once the job has continued", func() {
		newSite(10, nil)
		j := newResumable(envNS, 60, 5)
		reconcileJob(j)
		end85(j, 1, 50, "c1")
		reconcileJob(j) // continuing
		reconcileJob(j) // attempt 2 admitted
		code := hpcv1alpha1.ExitCodeContinue
		start := metav1.NewTime(time.Now().Add(-50 * time.Minute))
		endT := metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{SiteJobID: "101", State: "FAILED", ExitCode: &code,
			StartTime: &start, EndTime: &endT, Terminal: true, CheckpointRef: "c1"}) // no attempt number
		_, got := reconcileJob(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Attempts).To(HaveLen(1))
	})

	It("is idempotent: reconciling the same state again records nothing twice", func() {
		newSite(10, nil)
		j := newResumable(envNS, 60, 5)
		reconcileJob(j)
		end85(j, 1, 50, "c1")
		reconcileJob(j)
		for range 3 {
			reconcileJob(j)
		}
		_, got := reconcileJob(j)
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Continuations).To(Equal(int32(1)))
		Expect(got.Status.Attempt).To(Equal(int32(2)))
		Expect(siteStatus().GPU.BookedNodeMinutes).To(Equal(int64(60)), "one booking for attempt 2, not one per reconcile")
	})

	It("fails a decision made on a stale copy of the job instead of applying it (optimistic lock)", func() {
		newSite(10, nil)
		j := newResumable(envNS, 60, 5)
		reconcileJob(j)
		end85(j, 1, 50, "c1")
		stale := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), stale)).To(Succeed())
		reconcileJob(j) // the real decision: continuing, attempt 2
		_, err := r.endAttempt(ctx, stale)
		Expect(err).To(HaveOccurred(), "the stale write must conflict")
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Continuations).To(Equal(int32(1)))
	})

	It("never overbooks when its own bookings are not visible yet (a lagging cache)", func() {
		newSite(10, nil)
		lag := &laggingClient{Client: k8sClient}
		r.Client = lag
		var admitted int
		for range 5 {
			j := newJob(envNS, 240)
			lag.freeze = true // list calls keep returning the jobs as they were before our writes
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
			Expect(err).NotTo(HaveOccurred())
			got := &hpcv1alpha1.HPCArgoWorkflowJob{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
			if got.Status.Phase == hpcv1alpha1.PhaseAdmitted {
				admitted++
			}
		}
		Expect(admitted).To(Equal(2), "600 node-minutes hold two 240-minute jobs, whatever the cache shows")
	})

	It("rejects a change to a job's spec (CEL)", func() {
		j := newJob(envNS, 60)
		j.Spec.WallTimeMinutes = 90
		Expect(k8sClient.Update(ctx, j)).NotTo(Succeed())
	})
})

// laggingClient answers List for HPCArgoWorkflowJobs with the jobs' state from before this test's own admission
// writes, the way an informer cache can lag behind the controller's writes.
type laggingClient struct {
	client.Client
	freeze bool
}

func (c *laggingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if jobs, ok := list.(*hpcv1alpha1.HPCArgoWorkflowJobList); ok && c.freeze {
		for i := range jobs.Items {
			jobs.Items[i].Status = hpcv1alpha1.HPCArgoWorkflowJobStatus{} // no booking visible yet
		}
	}
	return nil
}
