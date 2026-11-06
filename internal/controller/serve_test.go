package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// Phase 1 (PRD-HPC-1461 D-58, D-64): a job with spec.stepName runs through a controller-owned
// runner Job per attempt and is cancelled with its Workflow.
var _ = Describe("HPCArgoWorkflowJob serve mode", func() {
	var (
		r       *HPCArgoWorkflowJobReconciler
		project string
		envNS   string
		hpcNS   string
		seq     int
	)

	newNS := func(name string) {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name,
			Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
	}

	newSite := func() {
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Sharing: true,
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44", ScratchRoot: "/project/scratch/p1",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 100}},
		})).To(Succeed())
	}

	newWorkflow := func() *unstructured.Unstructured {
		seq++
		wf := &unstructured.Unstructured{}
		wf.SetGroupVersionKind(workflowGVK)
		wf.SetName(fmt.Sprintf("wf-%d", seq))
		wf.SetNamespace(envNS)
		wf.Object["spec"] = map[string]any{"entrypoint": "main"}
		Expect(k8sClient.Create(ctx, wf)).To(Succeed())
		return wf
	}

	newServeJob := func(wf *unstructured.Unstructured, mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("hpc-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "reg/app:v1", WallTimeMinutes: 60,
				Command: []string{"python", "train.py"}, StepName: "train", Outputs: []string{"model"},
				Env: []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}},
		}
		if wf != nil {
			j.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: wf.GetName(), UID: string(wf.GetUID())}
		}
		if mod != nil {
			mod(j)
		}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) (reconcile.Result, *hpcv1alpha1.HPCArgoWorkflowJob) {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)
		if err != nil {
			Expect(client.IgnoreNotFound(err)).To(Succeed())
			return res, nil
		}
		return res, got
	}

	runnerJob := func(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32) *batchv1.Job {
		rj := &batchv1.Job{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.Name(j.UID, attempt)}, rj); err != nil {
			Expect(client.IgnoreNotFound(err)).To(Succeed())
			return nil
		}
		return rj
	}

	envMap := func(rj *batchv1.Job) map[string]string {
		m := map[string]string{}
		for _, e := range rj.Spec.Template.Spec.Containers[0].Env {
			m[e.Name] = e.Value
		}
		return m
	}

	report := func(j *hpcv1alpha1.HPCArgoWorkflowJob, run hpcv1alpha1.RunStatus) {
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		got.Status.Run = &run
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
	}

	// gc plays the garbage collector envtest lacks: it finishes a Foreground deletion.
	gc := func(rj *batchv1.Job) {
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(rj), rj)).To(Succeed())
		Expect(rj.DeletionTimestamp).NotTo(BeNil(), "the runner Job must be deleted")
		Expect(rj.Finalizers).To(ContainElement(metav1.FinalizerDeleteDependents), "with Foreground propagation")
		controllerutil.RemoveFinalizer(rj, metav1.FinalizerDeleteDependents)
		Expect(k8sClient.Update(ctx, rj)).To(Succeed())
	}

	BeforeEach(func() {
		seq = 0
		project = fmt.Sprintf("srv%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		newNS(envNS)
		newNS(hpcNS)
		newSite()
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test",
			RunnerScheduling: runnerjob.Scheduling{NodeSelector: map[string]string{"pool": "system"}}}
	})

	It("creates the runner Job in the HPCSite namespace with env built only by the controller", func() {
		wf := newWorkflow()
		j := newServeJob(wf, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Sharing = &hpcv1alpha1.Sharing{Scope: "abcdef12"}
			j.Spec.DatasetRef = "ds@v1"
		})
		res, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(res.RequeueAfter).To(Equal(ServeResync), "a live serve job polls its Workflow")
		Expect(got.Finalizers).To(ContainElement(hpcv1alpha1.CancelFinalizer))

		rj := runnerJob(got, 1)
		Expect(rj).NotTo(BeNil())
		Expect(rj.Labels).To(HaveKeyWithValue(runnerjob.LabelJobUID, string(got.UID)))
		Expect(rj.Labels).To(HaveKeyWithValue(runnerjob.LabelAttempt, "1"))
		Expect(*rj.Spec.BackoffLimit).To(Equal(int32(4)))
		Expect(*rj.Spec.ActiveDeadlineSeconds).To(Equal(int64((60 + 480 + 30) * 60)))
		pod := rj.Spec.Template.Spec
		Expect(pod.ServiceAccountName).To(Equal("hpc-runner"))
		Expect(pod.NodeSelector).To(HaveKeyWithValue("pool", "system"))
		c := pod.Containers[0]
		Expect(c.Image).To(Equal("kaos/hpc-operator:test"))
		Expect(c.EnvFrom).To(HaveLen(1))
		Expect(c.EnvFrom[0].ConfigMapRef.Name).To(Equal("hpc-runner-config"))
		var volumes []string
		for _, v := range pod.Volumes {
			volumes = append(volumes, v.Secret.SecretName)
		}
		Expect(volumes).To(ConsistOf("meluxina-jwt", "meluxina-staging", "mlflow-svc"))

		env := envMap(rj)
		Expect(env).To(HaveKeyWithValue("HPC_JOB_NAMESPACE", envNS))
		Expect(env).To(HaveKeyWithValue("HPC_JOB_NAME", got.Name))
		Expect(env).To(HaveKeyWithValue("KAOS_ATTEMPT", "1"))
		Expect(env).To(HaveKeyWithValue("STEP_NAME", "train"))
		Expect(env).To(HaveKeyWithValue("WF_UID", string(wf.GetUID())))
		Expect(env).To(HaveKeyWithValue("STEP_COMMAND", `["python","train.py"]`))
		Expect(env).To(HaveKeyWithValue("OUTPUTS_REPO", "outputs-app"))
		Expect(env).To(HaveKeyWithValue("HPC_SCOPE", "abcdef12"))
		// Site-derived, never from the tenant.
		Expect(env).To(HaveKeyWithValue("HPC_ENDPOINT", "https://slurm-api.site.example"))
		Expect(env).To(HaveKeyWithValue("HPC_ACCOUNT", "p1"))
		Expect(env).To(HaveKeyWithValue("HPC_USER", "u1"))
		Expect(env).To(HaveKeyWithValue("HPC_SCRATCH", "/project/scratch/p1"))
		for _, forbidden := range []string{"MLFLOW_EXTERNAL_URL", "LAKEFS_EXTERNAL_URL", "ZITADEL_DOMAIN", "MLFLOW_TRACKING_URI"} {
			Expect(env).NotTo(HaveKey(forbidden), "%s comes from hpc-runner-config only", forbidden)
		}

		Expect(got.Status.Runner).To(Equal(&hpcv1alpha1.RunnerRef{JobName: rj.Name, Attempt: 1}))
		Expect(testutil.ToFloat64(metrics.JobPhase.WithLabelValues(envNS, got.Name, "meluxina-gpu", "gpu", "Admitted"))).To(Equal(1.0))

		// Idempotent: another reconcile keeps the one Job.
		reconcileOnce(j)
		var all batchv1.JobList
		Expect(k8sClient.List(ctx, &all, client.InNamespace(hpcNS), client.MatchingLabels{runnerjob.LabelJobUID: string(got.UID)})).To(Succeed())
		Expect(all.Items).To(HaveLen(1))
	})

	It("refuses a spec.env outside the allowlist: the CRD rejects it", func() {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: "evil", Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "i", WallTimeMinutes: 10, StepName: "s",
				Env: []hpcv1alpha1.NameValue{{Name: "HPC_ENDPOINT", Value: "https://evil.example"}}},
		}
		err := k8sClient.Create(ctx, j)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("env may only set"))
		j.Spec.Env = []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "not-an-outputs-repo"}}
		Expect(k8sClient.Create(ctx, j)).NotTo(Succeed())
	})

	It("continues in a new runner Job for attempt 2 with the resume environment", func() {
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		_, got := reconcileOnce(j)
		Expect(runnerJob(got, 1)).NotTo(BeNil())

		code := hpcv1alpha1.ExitCodeContinue
		start, end := metav1.NewTime(time.Now().Add(-50*time.Minute)), metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "101", State: "FAILED", ExitCode: &code, StartTime: &start,
			EndTime: &end, Terminal: true, CheckpointRef: "ckpt-1"})
		for range 3 {
			reconcileOnce(j)
		}
		_, got = reconcileOnce(j)
		Expect(got.Status.Attempt).To(Equal(int32(2)))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		rj2 := runnerJob(got, 2)
		Expect(rj2).NotTo(BeNil(), "attempt 2 needs its own runner Job")
		env := envMap(rj2)
		Expect(env).To(HaveKeyWithValue("KAOS_ATTEMPT", "2"))
		Expect(env).To(HaveKeyWithValue("KAOS_RESUME_REF", "ckpt-1"))
		Expect(env).To(HaveKeyWithValue("KAOS_RESUME_FROM_ATTEMPT", "1"))
		Expect(got.Status.Runner.Attempt).To(Equal(int32(2)))
		Expect(got.Status.Runner.JobName).To(Equal(rj2.Name))
	})

	It("fails the attempt with RunnerFailed and the pod's message when the runner Job ends in failure", func() {
		j := newServeJob(nil, nil)
		_, got := reconcileOnce(j)
		rj := runnerJob(got, 1)

		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner-pod", Namespace: hpcNS,
			Labels: map[string]string{runnerjob.LabelRunnerJob: rj.Name}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "x"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "runner", State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, Message: "HPC identity missing"}}}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		now := metav1.Now()
		rj.Status.StartTime = &now
		rj.Status.Failed = 5
		rj.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
		Expect(k8sClient.Status().Update(ctx, rj)).To(Succeed())

		before := testutil.ToFloat64(metrics.RunnerRestarts)
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRunnerFailed))
		Expect(got.Status.Message).To(ContainSubstring("HPC identity missing"))
		Expect(got.Status.Booking).To(BeNil(), "the booking is released")
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Attempts[0].Outcome).To(Equal(hpcv1alpha1.ReasonRunnerFailed))
		Expect(testutil.ToFloat64(metrics.RunnerRestarts) - before).To(Equal(5.0))

		// Terminal: the next reconcile drops the finalizer.
		_, got = reconcileOnce(j)
		Expect(got.Finalizers).NotTo(ContainElement(hpcv1alpha1.CancelFinalizer))
	})

	DescribeTable("cancels through the runner Job when the owning Workflow ends",
		func(change func(wf *unstructured.Unstructured), wantInMessage string) {
			wf := newWorkflow()
			j := newServeJob(wf, nil)
			_, got := reconcileOnce(j)
			rj := runnerJob(got, 1)
			Expect(rj).NotTo(BeNil())

			change(wf)

			res, got := reconcileOnce(j)
			Expect(res.RequeueAfter).To(Equal(CancelPoll), "waits for the runner Job to go")
			Expect(got.Status.Phase).NotTo(Equal(hpcv1alpha1.PhaseCancelled), "not before the runner has been stopped")
			gc(rj) // the Foreground deletion finishes: the runner's SIGTERM handler has run

			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
			Expect(got.Status.Message).To(ContainSubstring(wantInMessage))
			Expect(got.Status.Booking).To(BeNil())
			Expect(got.Finalizers).NotTo(ContainElement(hpcv1alpha1.CancelFinalizer))
			Expect(got.Status.Attempts).To(HaveLen(1))
			Expect(got.Status.Attempts[0].Outcome).To(Equal("Cancelled"))
			Expect(runnerJob(got, 1)).To(BeNil())
		},
		Entry("should_cancel_when_the_workflow_is_shut_down", func(wf *unstructured.Unstructured) {
			Expect(unstructured.SetNestedField(wf.Object, "Stop", "spec", "shutdown")).To(Succeed())
			Expect(k8sClient.Update(ctx, wf)).To(Succeed())
		}, "shut down"),
		Entry("should_cancel_when_the_workflow_is_deleted", func(wf *unstructured.Unstructured) {
			Expect(k8sClient.Delete(ctx, wf)).To(Succeed())
		}, "was deleted"),
		Entry("should_cancel_when_the_workflow_ended_while_the_step_had_not", func(wf *unstructured.Unstructured) {
			Expect(unstructured.SetNestedField(wf.Object, "Failed", "status", "phase")).To(Succeed())
			Expect(k8sClient.Update(ctx, wf)).To(Succeed())
		}, "ended"),
	)

	It("marks the cancel request in the API before it deletes the runner Job", func() {
		wf := newWorkflow()
		j := newServeJob(wf, nil)
		_, got := reconcileOnce(j)
		rj := runnerJob(got, 1)
		oc := &orderClient{Client: k8sClient, key: client.ObjectKeyFromObject(j)}
		r.Client = oc
		Expect(unstructured.SetNestedField(wf.Object, "Stop", "spec", "shutdown")).To(Succeed())
		Expect(k8sClient.Update(ctx, wf)).To(Succeed())

		_, got = reconcileOnce(j)
		Expect(oc.deletes).To(Equal(1))
		Expect(oc.markedAtDelete).To(BeTrue(), "the annotation must be in the API when the runner Job is deleted")
		Expect(got.Annotations).To(HaveKey(hpcv1alpha1.CancelRequestedAnnotation))
		_, perr := time.Parse(time.RFC3339, got.Annotations[hpcv1alpha1.CancelRequestedAnnotation])
		Expect(perr).NotTo(HaveOccurred())
		gc(rj)
	})

	It("never marks a cancel when only the runner pod is evicted (Job alive)", func() {
		wf := newWorkflow()
		j := newServeJob(wf, nil)
		_, got := reconcileOnce(j)
		rj := runnerJob(got, 1)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "evicted", Namespace: hpcNS,
			Labels: map[string]string{runnerjob.LabelRunnerJob: rj.Name}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "x"}}}}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())

		_, got = reconcileOnce(j)
		Expect(got.Annotations).NotTo(HaveKey(hpcv1alpha1.CancelRequestedAnnotation))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(runnerJob(got, 1).DeletionTimestamp).To(BeNil())
	})

	It("cancels when the job itself is deleted, and then lets it go", func() {
		j := newServeJob(nil, nil)
		_, got := reconcileOnce(j)
		rj := runnerJob(got, 1)

		Expect(k8sClient.Delete(ctx, got)).To(Succeed())
		res, got := reconcileOnce(j)
		Expect(got).NotTo(BeNil(), "the cancel finalizer holds the job")
		Expect(res.RequeueAfter).To(Equal(CancelPoll))
		gc(rj)

		_, got = reconcileOnce(j)
		Expect(got).To(BeNil(), "the finalizer is dropped and the job is gone")
	})

	It("does not cancel a job whose Workflow is the same and still running", func() {
		wf := newWorkflow()
		Expect(unstructured.SetNestedField(wf.Object, "Running", "status", "phase")).To(Succeed())
		Expect(k8sClient.Update(ctx, wf)).To(Succeed())
		j := newServeJob(wf, nil)
		reconcileOnce(j)
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(runnerJob(got, 1).DeletionTimestamp).To(BeNil())
	})

	It("treats a Workflow with another UID as deleted", func() {
		wf := newWorkflow()
		j := newServeJob(wf, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Workflow.UID = "some-older-workflow" })
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).NotTo(Equal(hpcv1alpha1.PhaseAdmitted))
	})

	It("leaves a phase-0 job (no stepName) exactly as before: no runner Job, no finalizer, no polling", func() {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("p0-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "img", WallTimeMinutes: 60}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		res, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(res.RequeueAfter).To(BeZero())
		Expect(got.Finalizers).To(BeEmpty())
		Expect(got.Status.Runner).To(BeNil())
		var all batchv1.JobList
		Expect(k8sClient.List(ctx, &all, client.InNamespace(hpcNS))).To(Succeed())
		Expect(all.Items).To(BeEmpty())
	})

	It("reports a missing runner image loudly instead of silently waiting", func() {
		r.RunnerImage = ""
		j := newServeJob(nil, nil)
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).To(MatchError(ContainSubstring("--runner-image")))
	})

	// Learned time limit (PRD-HPC-1461 F-41, D-67).
	const appImage = "reg/lab/app:v1"
	// seedRun plays an earlier job of a step: terminal in the given phase with a run of the given minutes.
	seedRun := func(image string, phase hpcv1alpha1.Phase, runMinutes int, mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) {
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = image })
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		end := metav1.NewTime(time.Now().Add(-time.Hour))
		start := metav1.NewTime(end.Add(-time.Duration(runMinutes) * time.Minute))
		got.Status.Phase = phase
		got.Status.Attempt = 1
		got.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "COMPLETED", StartTime: &start, EndTime: &end, Terminal: true}
		if mod != nil {
			mod(got)
		}
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
	}

	timeout := func(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32, minutes int) {
		start := metav1.NewTime(time.Now().Add(-time.Duration(minutes) * time.Minute))
		end := metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: attempt, SiteJobID: fmt.Sprintf("20%d", attempt), State: "TIMEOUT",
			StartTime: &start, EndTime: &end, Terminal: true})
	}

	// settle reconciles until the job leaves the given phase (a retry is judged on a later pass).
	settleTo := func(j *hpcv1alpha1.HPCArgoWorkflowJob, want hpcv1alpha1.Phase) *hpcv1alpha1.HPCArgoWorkflowJob {
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for range 4 {
			_, got = reconcileOnce(j)
			if got.Status.Phase == want && got.Status.Booking != nil {
				break
			}
		}
		return got
	}

	It("books and submits the learned limit: p95 x 1.5 of the same app's runs, never another app's or an invalid one", func() {
		for range 3 {
			seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil) // earlier builds of the same app
		}
		for range 5 {
			seedRun("reg/lab/other:v1", hpcv1alpha1.PhaseSucceeded, 200, nil) // another app, same namespace
		}
		seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseFailed, 200, nil) // not successful
		seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 200, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
			g.Status.Run.StartTime, g.Status.Run.EndTime = g.Status.Run.EndTime, g.Status.Run.StartTime // end < start
		})
		seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 200, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
			g.Status.Continuations = 1 // a continued job's last segment is not the step's run time
		})
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg/lab/app:v9"; j.Spec.WallTimeMinutes = 240 })

		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 30, Source: hpcv1alpha1.PlacementLearned, Samples: 3}))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(30)), "admission books the learned limit")
		rj := runnerJob(got, 1)
		Expect(envMap(rj)).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "30"))
		Expect(*rj.Spec.ActiveDeadlineSeconds).To(Equal(int64((30 + 480 + 30) * 60)))
	})

	It("keeps the declared limit with fewer than three samples of its own app", func() {
		seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 5, nil)
		seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 5, nil)
		for range 4 {
			seedRun("reg/lab/yolo:v1", hpcv1alpha1.PhaseSucceeded, 5, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementDeclared, Samples: 2}))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(60)))
		Expect(envMap(runnerJob(got, 1))).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "60"))
	})

	It("never learns for a resumable step: its checkpoint signal is timed from the declared limit", func() {
		for range 4 {
			seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 5, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Image = appImage
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementDeclared))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(60)))
	})

	It("retries once with twice the limit when a learned limit times out, then fails as today on a second TIMEOUT", func() {
		for range 3 {
			seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage; j.Spec.WallTimeMinutes = 240 })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(30)))

		timeout(j, 1, 30)
		res, got := reconcileOnce(j)
		Expect(res.RequeueAfter).To(Equal(ContinueResync))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhasePending))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTimeLimitRetry))
		Expect(got.Status.Attempt).To(Equal(int32(2)))
		Expect(got.Status.Continuations).To(BeZero(), "a retry is not a continuation")
		Expect(got.Status.Resume).To(BeNil())
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(got.Status.Attempts[0].SettledNodeMinutes).To(Equal(int64(30)))

		got = settleTo(j, hpcv1alpha1.PhaseAdmitted)
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementTimeoutRetry, Samples: 3}))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(60)))
		Expect(envMap(runnerJob(got, 2))).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "60"))

		timeout(j, 2, 60)
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(got.Status.Attempts).To(HaveLen(2))
		Expect(got.Status.Attempt).To(Equal(int32(2)), "no third attempt")
	})

	It("caps the timeout retry at the spec's wall time and does not retry a declared limit", func() {
		for range 3 {
			seedRun("reg/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage; j.Spec.WallTimeMinutes = 40 })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(30)))
		timeout(j, 1, 30)
		reconcileOnce(j)
		got = settleTo(j, hpcv1alpha1.PhaseAdmitted)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(40)), "min(2 x 30, cap 40)")

		declared := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg/lab/fresh:v1" })
		reconcileOnce(declared)
		timeout(declared, 1, 60)
		_, got = reconcileOnce(declared)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed), "a declared limit that times out fails as before")
		Expect(got.Status.Attempts).To(HaveLen(1))
	})
})

// orderClient records whether the cancel marker was in the API when a runner Job was deleted.
type orderClient struct {
	client.Client
	key            types.NamespacedName
	deletes        int
	markedAtDelete bool
}

func (c *orderClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*batchv1.Job); ok {
		c.deletes++
		var j hpcv1alpha1.HPCArgoWorkflowJob
		if err := c.Get(ctx, c.key, &j); err == nil {
			c.markedAtDelete = j.Annotations[hpcv1alpha1.CancelRequestedAnnotation] != ""
		}
	}
	return c.Client.Delete(ctx, obj, opts...)
}
