package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// slowWorkflowReads fails every read of an Argo Workflow with a timeout and passes everything else on.
type slowWorkflowReads struct{ client.Client }

func (c slowWorkflowReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == workflowGVK {
		return apierrors.NewTimeoutError("the API server is slow", 1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

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
		wf.SetLabels(map[string]string{"platform.kubecore.io/app": "app"}) // the app of the pipeline's WorkflowTemplate
		wf.Object["spec"] = map[string]any{"entrypoint": "main"}
		Expect(k8sClient.Create(ctx, wf)).To(Succeed())
		return wf
	}

	newServeJob := func(wf *unstructured.Unstructured, mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("hpc-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/app:v1", WallTimeMinutes: 60,
				Command: []string{"python", "train.py"}, StepName: "train",
				Env: []hpcv1alpha1.NameValue{{Name: "KAOS_LIB_SUPPORT", Value: "{}"}}},
		}
		if wf != nil {
			j.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: wf.GetName(), UID: string(wf.GetUID())}
		}
		if mod != nil {
			mod(j)
		}
		withWorkflow(j) // a follower always names the Workflow: a job built without one gets a real one
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

	It("refuses, before any runner starts, a job whose app is not the app of its Workflow", func() {
		j := newServeJob(newWorkflow(), func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.App = "rival" })
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonAppIdentityUnavailable))
		Expect(got.Status.Message).To(ContainSubstring("rival"))
		Expect(runnerJob(got, 1)).To(BeNil())
	})

	It("refuses a serve-mode job that names no Workflow before any booking or runner", func() {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("hpc-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/app:v1",
				WallTimeMinutes: 60, Command: []string{"python", "train.py"}, StepName: "train"}}
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonAppIdentityUnavailable))
		Expect(got.Status.Message).To(ContainSubstring("names no Workflow"))
		Expect(got.Status.Booking).To(BeNil())
		Expect(runnerJob(got, 1)).To(BeNil())
	})

	It("does not refuse a job whose Workflow cannot be read now: it asks again", func() {
		wf := newWorkflow()
		j := newServeJob(wf, nil)
		flaky := &HPCArgoWorkflowJobReconciler{Client: slowWorkflowReads{k8sClient}, Scheme: r.Scheme, RunnerImage: r.RunnerImage}
		_, err := flaky.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).To(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		Expect(got.Status.Phase).NotTo(Equal(hpcv1alpha1.PhaseRefused))
		Expect(runnerJob(got, 1)).To(BeNil())
	})

	It("creates the runner Job in the HPCSite namespace with env built only by the controller", func() {
		wf := newWorkflow()
		j := newServeJob(wf, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Sharing = &hpcv1alpha1.Sharing{Scope: "abcdef12"}
			j.Spec.Context = contractFixture("context-one-upstream.wire")
			j.Spec.Inputs = []hpcv1alpha1.StepInput{
				{Name: "labels", Repo: "dataset-labels", Ref: "v3", Commit: strings.Repeat("2", 64)},
				{Name: "embeddings", Repo: "dataset-embeddings", Commit: strings.Repeat("3", 64)}}
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
		// A sharing host's runner follows its Slurm job up to twice the step's limit (D-26), + the 30-minute site wait.
		Expect(*rj.Spec.ActiveDeadlineSeconds).To(Equal(int64((2*60+480+30+72*60)*60 + 30*60)))
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
		Expect(volumes).To(ConsistOf("meluxina-jwt", "meluxina-staging", "mlflow-hpc"))

		env := envMap(rj)
		Expect(env).To(HaveKeyWithValue("HPC_JOB_NAMESPACE", envNS))
		Expect(env).To(HaveKeyWithValue("HPC_JOB_NAME", got.Name))
		Expect(env).To(HaveKeyWithValue("KAOS_ATTEMPT", "1"))
		Expect(env).To(HaveKeyWithValue("STEP_NAME", "train"))
		Expect(env).To(HaveKeyWithValue("WF_UID", string(wf.GetUID())))
		Expect(env).To(HaveKeyWithValue("STEP_COMMAND", `["python","train.py"]`))
		Expect(env).To(HaveKeyWithValue("KAOS_LIB_SUPPORT", "{}"))
		// For the runner itself: the Workflow's name (log labels) and the app (its outputs repository).
		Expect(env).To(HaveKeyWithValue("WF_NAME", wf.GetName()))
		Expect(env).To(HaveKeyWithValue("KAOS_APP", "app"))
		for _, gone := range []string{"KUBECORE_WORKFLOW_NAME", "KUBECORE_RUN_ID", "OUTPUTS_REPO", "STEP_OUTPUTS", "DATASET_REPO"} {
			Expect(env).NotTo(HaveKey(gone))
		}
		Expect(env).To(HaveKeyWithValue("HPC_SCOPE", "abcdef12"))
		// The step contract: the context as the job stores it, byte for byte, and every input to stage.
		Expect(env["KAOS_STEP_CONTEXT"]).To(Equal(contractFixture("context-one-upstream.wire")))
		Expect(env["HPC_INPUTS"]).To(Equal(`[{"name":"labels","repo":"dataset-labels","commit":"` + strings.Repeat("2", 64) + `","ref":"v3"},` +
			`{"name":"embeddings","repo":"dataset-embeddings","commit":"` + strings.Repeat("3", 64) + `"}]`))
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

	It("should_admit_a_job_with_a_dataset_another_project_shares_and_tell_the_runner_the_owner", func() {
		j := newServeJob(newWorkflow(), func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Inputs = []hpcv1alpha1.StepInput{{Name: "raw", Repo: "dataset-raw", Commit: strings.Repeat("c2", 32), Owner: "other"}}
		})
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).NotTo(Equal(hpcv1alpha1.PhaseRefused))
		rj := runnerJob(got, 1)
		Expect(rj).NotTo(BeNil())
		env := map[string]string{}
		for _, e := range rj.Spec.Template.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		Expect(env["HPC_INPUTS"]).To(Equal(`[{"name":"raw","repo":"dataset-raw","commit":"` + strings.Repeat("c2", 32) + `","owner":"other"}]`))
	})

	It("should_refuse_a_job_without_a_step_context_at_the_definition", func() {
		bare := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: "hpc-bare", Namespace: envNS},
			Spec:       hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Class: "meluxina-gpu", Image: "reg.example/app:v1", WallTimeMinutes: 60, StepName: "train"},
		}
		err := k8sClient.Create(ctx, bare)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.context"))
	})

	// PRD kaos#1911 CON-04: status.result is set only on a job that Succeeded.
	It("should_keep_the_result_of_a_step_that_ended_well_and_publish_none_for_any_other_end", func() {
		result := contractFixture("result-model.wire")
		end := func(run hpcv1alpha1.RunStatus) *hpcv1alpha1.HPCArgoWorkflowJob {
			j := newServeJob(newWorkflow(), nil)
			_, got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			// The runner's terminal report: the run and the result in one patch.
			orig := got.DeepCopy()
			got.Status.Run, got.Status.Result = &run, result
			Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
			_, got = reconcileOnce(j)
			return got
		}
		now := metav1.Now()
		ok := end(hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "1", State: "COMPLETED", ExitCode: ptr.To(int32(0)), StartTime: &now, EndTime: &now, Terminal: true})
		Expect(ok.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(ok.Status.Result).To(Equal(result))

		failed := end(hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "2", State: "FAILED", ExitCode: ptr.To(int32(1)), StartTime: &now, EndTime: &now, Terminal: true})
		Expect(failed.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(failed.Status.Result).To(BeEmpty(), "a failed job never publishes a result, whatever the step wrote")

		cancelled := end(hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "3", State: "CANCELLED", StartTime: &now, EndTime: &now, Terminal: true})
		Expect(cancelled.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
		Expect(cancelled.Status.Result).To(BeEmpty())
	})

	// PRD kaos#1911 X-2. Before: a step whose result could not be handed back ended Succeeded with empty outputs.
	It("should_fail_the_job_with_its_own_reason_when_the_result_was_not_handed_back_or_is_not_a_result", func() {
		now := metav1.Now()
		end := func(code int32, message string) *hpcv1alpha1.HPCArgoWorkflowJob {
			j := newServeJob(newWorkflow(), nil)
			reconcileOnce(j)
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "9", State: "FAILED", ExitCode: ptr.To(code),
				StartTime: &now, EndTime: &now, Terminal: true, Message: message})
			_, got := reconcileOnce(j)
			return got
		}
		lost := end(hpcv1alpha1.ExitCodeResultNotHandedBack, "the step ended well, but its result could not be fetched: lakeFS answered 503")
		Expect(lost.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(lost.Status.Reason).To(Equal(hpcv1alpha1.ReasonResultNotHandedBack))
		Expect(lost.Status.Message).To(ContainSubstring("lakeFS answered 503"))
		Expect(lost.Status.Result).To(BeEmpty())

		// The job script ended 243 on the node: the runner has no sentence, the controller says what the code means.
		onNode := end(hpcv1alpha1.ExitCodeResultNotHandedBack, "")
		Expect(onNode.Status.Reason).To(Equal(hpcv1alpha1.ReasonResultNotHandedBack))
		Expect(onNode.Status.Message).To(ContainSubstring("could not read or upload the step's result file"))

		invalid := end(hpcv1alpha1.ExitCodeInvalidResult, "the step's result is not valid: it costs 9000 bytes, more than the 8192 a result may cost")
		Expect(invalid.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(invalid.Status.Reason).To(Equal(hpcv1alpha1.ReasonInvalidResult))
		Expect(invalid.Status.Message).To(ContainSubstring("costs 9000 bytes"))
	})

	It("refuses a spec.env outside the allowlist: the CRD rejects it", func() {
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: "evil", Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/img:1", WallTimeMinutes: 10, StepName: "s",
				Env: []hpcv1alpha1.NameValue{{Name: "HPC_ENDPOINT", Value: "https://evil.example"}}},
		}
		err := k8sClient.Create(ctx, j)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("env may only set"))
		// What the engine set before the step contract is refused too: nothing reads it any more.
		j.Spec.Env = []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}
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
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "reg.example/img:1"}}}}
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
		// No site job id was reported, but sbatch may have been accepted before the crash: the sweep decides.
		Expect(got.Status.Sweep).NotTo(BeNil())
		reconcileOnce(j)
		end := metav1.NewTime(time.Now().Truncate(time.Second))
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "-", State: "NOT_SUBMITTED", EndTime: &end, Terminal: true})
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
			// the runner's SIGTERM handler cancelled the site job and reported it before the Job went
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "801", State: "CANCELLED", Terminal: true})
			gc(rj) // the Foreground deletion finishes

			_, got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
			Expect(got.Status.Message).To(ContainSubstring(wantInMessage))
			Expect(got.Status.Booking).To(BeNil())
			Expect(got.Status.Attempts).To(HaveLen(1))
			Expect(got.Status.Attempts[0].Outcome).To(Equal("Cancelled"))
			Expect(runnerJob(got, 1)).To(BeNil())
			_, got = reconcileOnce(j) // the terminal job lets go of its finalizer
			Expect(got.Finalizers).NotTo(ContainElement(hpcv1alpha1.CancelFinalizer))
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
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "reg.example/img:1"}}}}
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
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "801", State: "CANCELLED", Terminal: true})
		gc(rj)

		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled), "the cancel is settled while the finalizer still holds")
		_, got = reconcileOnce(j)
		Expect(got).To(BeNil(), "the finalizer is dropped and the job is gone")
	})

	// AC-08, live 2026-11-08 (hpc-05256877ed03ad42d80a): the Workflow was deleted 8 s after the runner started. The
	// runner died on SIGTERM before it reported its site job, the controller then ended the job as Cancelled and the
	// site job (5434810) was left running, unwatched. A runner that ran but did not report must be swept.
	It("should_sweep_with_a_cancel_only_runner_when_the_workflow_is_deleted_before_the_runner_reported_its_site_job", func() {
		rec := record.NewFakeRecorder(20)
		r.Recorder = rec
		wf := newWorkflow()
		j := newServeJob(wf, nil)
		_, got := reconcileOnce(j)
		Expect(got.Finalizers).To(ContainElement(hpcv1alpha1.CancelFinalizer), "the finalizer is set before the runner Job exists")
		Expect(got.Status.Booking).NotTo(BeNil())
		rj := runnerJob(got, 1)
		Expect(rj).NotTo(BeNil())

		// The owner is deleted: the finalizer holds the job, as it does against the garbage collector.
		Expect(k8sClient.Delete(ctx, wf)).To(Succeed())
		Expect(k8sClient.Delete(ctx, got, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		res, got := reconcileOnce(j)
		Expect(got).NotTo(BeNil())
		Expect(res.RequeueAfter).To(Equal(CancelPoll))
		Expect(got.Status.Sweep).NotTo(BeNil(), "decided while the runner Job still shows that a runner ran")
		gc(rj) // the runner died on SIGTERM without a report

		_, got = reconcileOnce(j)
		Expect(got).NotTo(BeNil(), "the finalizer holds until the site reports")
		Expect(got.Status.Phase.Terminal()).To(BeFalse(), "no Cancelled on the runner's absence alone")
		Expect(got.Status.Booking).NotTo(BeNil(), "the booking is kept: the site job may still run")
		Expect(got.Finalizers).To(ContainElement(hpcv1alpha1.CancelFinalizer))
		reconcileOnce(j)
		c := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.CancelName(got.UID, 1, 0)}, c)).To(Succeed())
		Expect(c.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{"--cancel-only"}))

		// the cancel-only runner finds the site job by its name, cancels it and reports what the site says
		end := metav1.NewTime(time.Now().Truncate(time.Second))
		start := metav1.NewTime(end.Add(-3 * time.Minute))
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5434810", State: "CANCELLED", StartTime: &start, EndTime: &end, Terminal: true})
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
		Expect(got.Status.Message).To(ContainSubstring("the site confirmed"))
		Expect(got.Status.Booking).To(BeNil())
		Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(3)), "the spend is settled from the site's own times")
		_, got = reconcileOnce(j)
		Expect(got).To(BeNil(), "the finalizer is dropped once the site confirmed")
		var events []string
		for len(rec.Events) > 0 {
			events = append(events, <-rec.Events)
		}
		Expect(events).To(ContainElement(ContainSubstring("SweepStarted")))
	})

	It("should_not_sweep_a_deleted_job_whose_runner_waits_for_an_unreachable_site", func() {
		j := newServeJob(nil, nil)
		_, got := reconcileOnce(j)
		rj := runnerJob(got, 1)
		orig := got.DeepCopy()
		got.Status.PendingReason = hpcv1alpha1.ReasonSiteUnavailable
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
		Expect(k8sClient.Delete(ctx, got)).To(Succeed())
		reconcileOnce(j)
		gc(rj)
		_, got = reconcileOnce(j)
		Expect(got).To(BeNil(), "no site job can exist: cancelled and let go at once, no cancel-only runner")
		c := &batchv1.Job{}
		Expect(client.IgnoreNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.CancelName(j.UID, 1, 0)}, c))).To(Succeed())
		Expect(c.Name).To(BeEmpty())
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
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: "reg.example/img:1", WallTimeMinutes: 60}}
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
	const appImage = "reg.example/lab/app:v1"
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

	// AC-72 / D-67 (live 2026-11-10, guest 5439976 on host 5439966): srun --time stopped the guest's step at its
	// learned 15 minutes; the slot manager says reason=TIMEOUT and the runner reports state TIMEOUT.
	It("retries a guest that its own learned limit stopped on a shared node once at the declared limit", func() {
		for range 3 {
			seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage; j.Spec.WallTimeMinutes = 240 })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementLearned))
		end, rc, slot := metav1.NewTime(time.Now()), int32(143), int32(0)
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5439976", State: "TIMEOUT", ExitCode: &rc, EndTime: &end,
			Terminal: true, HostSiteJobID: "5439966", Slot: &slot})
		_, got = reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTimeLimitRetry))
		Expect(got.Status.Attempt).To(Equal(int32(2)))
		Expect(got.Status.Attempts[0].Outcome).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(got.Status.Attempts[0].SettledNodeMinutes).To(BeZero(), "the host's booking covered the node (D-26)")
		got = settleTo(j, hpcv1alpha1.PhaseAdmitted)
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 240, Source: hpcv1alpha1.PlacementTimeoutRetry, Samples: 3}))
	})

	It("does not retry a guest killed on a shared node for any other reason", func() {
		for range 3 {
			seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage; j.Spec.WallTimeMinutes = 240 })
		reconcileOnce(j)
		end, rc := metav1.NewTime(time.Now()), int32(143)
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5439976", State: "FAILED", ExitCode: &rc, EndTime: &end,
			Terminal: true, HostSiteJobID: "5439966"})
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal("SiteFAILED"))
		Expect(got.Status.Attempt).To(Equal(int32(1)))
	})

	theSite := func() *hpcv1alpha1.HPCSite {
		site := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: "meluxina"}, site)).To(Succeed())
		return site
	}

	// D-26 (live 2026-11-10, host 5439966): the host's 1-minute step ended at 04:26, its Slurm job held the node
	// for guest 5439976 until 04:45 and the site charged it all; the ledger had released the booking at 04:26.
	hostOpen := func() (*hpcv1alpha1.HPCArgoWorkflowJob, metav1.Time) {
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Sharing = &hpcv1alpha1.Sharing{Scope: "abcdef12"} })
		_, got := reconcileOnce(j)
		Expect(got.Status.Booking).NotTo(BeNil())
		before, err := r.ledgerOf(ctx, theSite())
		Expect(err).NotTo(HaveOccurred())
		start := metav1.NewTime(time.Now().Add(-20 * time.Minute).Truncate(time.Second))
		stepEnd, rc := metav1.NewTime(start.Add(2*time.Minute)), int32(0)
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5439966", State: "COMPLETED", ExitCode: &rc,
			StartTime: &start, EndTime: &stepEnd, Terminal: true, HostJobOpen: true})
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded), "the step's result does not wait for the host job")
		Expect(got.Status.Booking).NotTo(BeNil(), "the booking covers the node until the host job ends")
		Expect(got.Status.Attempts[0].HostJobOpen).To(BeTrue())
		Expect(got.Status.Message).To(Equal("attempt 1 settled 2 node-minutes" + hostOpenNote))
		l, err := r.ledgerOf(ctx, theSite())
		Expect(err).NotTo(HaveOccurred())
		Expect(l.Tiers[hpcv1alpha1.TierGPU].Booked).To(Equal(before.Tiers[hpcv1alpha1.TierGPU].Booked), "the open host holds its booking (60)")
		Expect(l.Tiers[hpcv1alpha1.TierGPU].Used).To(Equal(before.Tiers[hpcv1alpha1.TierGPU].Used), "its provisional settlement is not counted twice")
		res, _ := reconcileOnce(j)
		Expect(res.RequeueAfter).To(Equal(OpenHostPoll), "looked at again while its runner runs")
		return j, start
	}

	It("holds an ended host's booking while its Slurm job keeps the node, then settles it to that job's end (D-26)", func() {
		j, start := hostOpen()
		jobEnd, rc := metav1.NewTime(start.Add(21*time.Minute)), int32(0)
		stepEnd := metav1.NewTime(start.Add(2 * time.Minute))
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5439966", State: "COMPLETED", ExitCode: &rc,
			StartTime: &start, EndTime: &stepEnd, HostJobEndTime: &jobEnd, Terminal: true})
		_, got := reconcileOnce(j)
		Expect(got.Status.Run.EndTime.Time).To(BeTemporally("==", stepEnd.Time), "the step's own end is kept for learning")
		Expect(got.Status.Booking).To(BeNil(), "released once the host job ended")
		Expect(got.Status.Attempts[0].HostJobOpen).To(BeFalse())
		Expect(got.Status.Attempts[0].SettledNodeMinutes).To(Equal(int64(21)), "what the site charged for the node")
		Expect(*got.Status.SettledNodeMinutes).To(Equal(int64(21)))
		Expect(got.Status.Message).To(Equal("attempt 1 settled 21 node-minutes (its Slurm job held the node until " +
			jobEnd.UTC().Format(time.RFC3339) + ")"))
		l, err := r.ledgerOf(ctx, theSite())
		Expect(err).NotTo(HaveOccurred())
		Expect(l.Tiers[hpcv1alpha1.TierGPU].Used).To(Equal(int64(21)))
		Expect(l.Tiers[hpcv1alpha1.TierGPU].Booked).To(BeZero())
	})

	It("learns a host's limit from its own step time, not from the time its job kept the node for a guest (D-67)", func() {
		for range 3 {
			j, start := hostOpen() // a 2-minute step
			jobEnd, stepEnd, rc := metav1.NewTime(start.Add(50*time.Minute)), metav1.NewTime(start.Add(2*time.Minute)), int32(0)
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5439966", State: "COMPLETED", ExitCode: &rc,
				StartTime: &start, EndTime: &stepEnd, HostJobEndTime: &jobEnd, Terminal: true})
			_, got := reconcileOnce(j)
			Expect(got.Status.Attempts[0].SettledNodeMinutes).To(Equal(int64(50)), "settled to the node the site charged")
		}
		next := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Sharing = &hpcv1alpha1.Sharing{Scope: "abcdef12"} })
		_, got := reconcileOnce(next)
		Expect(got.Status.Placement.Share).To(HaveField("Role", hpcv1alpha1.ShareHost), "no open host of the scope has a free slot: it is a host (D-70)")
		got.Status.Placement.Share = nil
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 15, Source: hpcv1alpha1.PlacementLearned, Samples: 3}),
			"3 samples of 2 minutes give the 15-minute floor; the 50-minute jobs would have given the declared 60")
	})

	It("closes an open host whose runner ended without its job's end, settled up to now", func() {
		j, _ := hostOpen()
		rj := runnerJob(j, 1)
		now := metav1.Now()
		rj.Status.StartTime = &now
		rj.Status.Failed = 5
		rj.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}
		Expect(k8sClient.Status().Update(ctx, rj)).To(Succeed())
		_, got := reconcileOnce(j)
		Expect(got.Status.Booking).To(BeNil())
		Expect(got.Status.Attempts[0].HostJobOpen).To(BeFalse())
		Expect(got.Status.Attempts[0].SettledNodeMinutes).To(BeNumerically(">=", 20), "up to now: conservative, never the step's 2")
		Expect(got.Status.Message).To(ContainSubstring("its runner ended before the host Slurm job's end was known"))
	})

	It("books and submits the learned limit: p95 x 1.5 of the same app's runs, never another app's or an invalid one", func() {
		for range 3 {
			seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil) // earlier builds of the same app
		}
		for range 5 {
			seedRun("reg.example/lab/other:v1", hpcv1alpha1.PhaseSucceeded, 200, nil) // another app, same namespace
		}
		seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseFailed, 200, nil) // not successful
		seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 200, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
			g.Status.Run.StartTime, g.Status.Run.EndTime = g.Status.Run.EndTime, g.Status.Run.StartTime // end < start
		})
		seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 200, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
			g.Status.Continuations = 1 // a continued job's last segment is not the step's run time
		})
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Image = "reg.example/lab/app:v9"
			j.Spec.WallTimeMinutes = 240
		})

		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 30, Source: hpcv1alpha1.PlacementLearned, Samples: 3}))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(30)), "admission books the learned limit")
		rj := runnerJob(got, 1)
		Expect(envMap(rj)).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "30"))
		Expect(*rj.Spec.ActiveDeadlineSeconds).To(Equal(int64((30+480+30)*60 + 30*60)))
	})

	It("keeps the declared limit with fewer than three samples of its own app", func() {
		seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 5, nil)
		seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 5, nil)
		for range 4 {
			seedRun("reg.example/lab/yolo:v1", hpcv1alpha1.PhaseSucceeded, 5, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 60, Source: hpcv1alpha1.PlacementDeclared, Samples: 2}))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(60)))
		Expect(envMap(runnerJob(got, 1))).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "60"))
	})

	It("never learns for a resumable step: its checkpoint signal is timed from the declared limit", func() {
		for range 4 {
			seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 5, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Image = appImage
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementDeclared))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(60)))
	})

	It("retries once at the declared limit when a learned limit times out, then fails as today on a second TIMEOUT", func() {
		for range 3 {
			seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil)
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
		Expect(got.Status.Placement).To(Equal(&hpcv1alpha1.Placement{TimeLimitMinutes: 240, Source: hpcv1alpha1.PlacementTimeoutRetry, Samples: 3}))
		Expect(got.Status.Booking.NodeMinutes).To(Equal(int64(240)))
		Expect(envMap(runnerJob(got, 2))).To(HaveKeyWithValue("SLURM_TIME_LIMIT", "240"))

		timeout(j, 2, 240)
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(got.Status.Attempts).To(HaveLen(2))
		Expect(got.Status.Attempt).To(Equal(int32(2)), "no third attempt")
	})

	It("caps the timeout retry at the spec's wall time and does not retry a declared limit", func() {
		for range 3 {
			seedRun("reg.example/lab/app:v0", hpcv1alpha1.PhaseSucceeded, 20, nil)
		}
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage; j.Spec.WallTimeMinutes = 40 })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(30)))
		timeout(j, 1, 30)
		reconcileOnce(j)
		got = settleTo(j, hpcv1alpha1.PhaseAdmitted)
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(40)), "the declared cap")

		declared := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg.example/lab/fresh:v1" })
		reconcileOnce(declared)
		timeout(declared, 1, 60)
		_, got = reconcileOnce(declared)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed), "a declared limit that times out fails as before")
		Expect(got.Status.Attempts).To(HaveLen(1))
	})

	// Time-limit guidance (a stop at the declared limit is explained; a repeat is warned about).
	stoppedRun := func(wall int32) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
		return func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
			g.Status.Reason = hpcv1alpha1.ReasonWallTimeReached
			g.Status.Run.State = "TIMEOUT"
			g.Status.Placement = &hpcv1alpha1.Placement{TimeLimitMinutes: wall, Source: hpcv1alpha1.PlacementDeclared}
		}
	}

	It("explains a stop at the declared limit with the real limit and the site maximum, and raises a Warning event", func() {
		rec := record.NewFakeRecorder(10)
		r.Recorder = rec
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.WallTimeMinutes = 240 })
		reconcileOnce(j)
		timeout(j, 1, 240)
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached), "the machine-read code is unchanged")
		want := "This step ran its full 4 h time limit and was stopped. Set a longer limit for it on the run form (`<step>-walltime`, up to 690 min on this site) " +
			"or make it resumable so it continues automatically from a checkpoint."
		Expect(got.Status.Message).To(Equal(want))
		Expect(<-rec.Events).To(Equal("Warning WallTimeReached " + want))
	})

	It("says a resumable step was stopped before it saved a checkpoint", func() {
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.WallTimeMinutes = 90
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		reconcileOnce(j)
		timeout(j, 1, 90)
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(got.Status.Message).To(Equal("This step ran its full 90 min time limit and was stopped before it saved a checkpoint; " +
			"save more often or ask to be signalled earlier."))
	})

	It("warns at admission when a recent run of the step was stopped at the same declared limit, and changes nothing else", func() {
		seedRun(appImage, hpcv1alpha1.PhaseFailed, 60, stoppedRun(60))
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(60)), "the limit is not raised")
		Expect(got.Status.Placement.Warning).To(Equal("Warning: this step was stopped at its 1 h limit in an earlier run " +
			"and will be again unless you raise its limit on the run form (`<step>-walltime`) or make it resumable."))
	})

	It("gives no warning without such a history: other limit, other app, success, or older than the last three", func() {
		n := 0
		ended := func(mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
			n++
			at := metav1.NewTime(time.Now().Add(-time.Duration(100-n) * time.Minute)) // later seeds ended later
			return func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
				if mod != nil {
					mod(g)
				}
				g.Status.Attempts = []hpcv1alpha1.AttemptRecord{{Attempt: 1, Tier: hpcv1alpha1.TierGPU, EndTime: at}}
			}
		}
		seedRun(appImage, hpcv1alpha1.PhaseFailed, 60, ended(stoppedRun(60))) // stopped at 60, but older than the last three
		for range 3 {
			seedRun(appImage, hpcv1alpha1.PhaseSucceeded, 10, ended(nil))
		}
		seedRun(appImage, hpcv1alpha1.PhaseFailed, 120, ended(stoppedRun(120))) // another declared limit
		seedRun("reg.example/lab/other:v1", hpcv1alpha1.PhaseFailed, 60, ended(stoppedRun(60)))
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		_, got := reconcileOnce(j)
		Expect(got.Status.Placement.Warning).To(BeEmpty())
	})

	// ---- D-74, D-75: recovery from interrupted attempts, discovered resumability ----

	// ended reports the end of an attempt: the site state, the step's exit code, and (when the job
	// persisted a checkpoint copy) its reference and progress.
	ended := func(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32, state string, exit *int32, ref string, ck *hpcv1alpha1.CheckpointInfo) {
		start, end := metav1.NewTime(time.Now().Add(-30*time.Minute)), metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: attempt, SiteJobID: fmt.Sprintf("30%d", attempt), State: state, ExitCode: exit,
			StartTime: &start, EndTime: &end, Terminal: true, CheckpointRef: ref, Checkpoint: ck})
	}
	progress := func(values ...int64) *hpcv1alpha1.CheckpointInfo {
		return &hpcv1alpha1.CheckpointInfo{ProgressValues: values, Bytes: 10, StepBytes: 20}
	}
	i32 := func(v int32) *int32 { return &v }

	It("tier 1: re-runs a step without a checkpoint from the start after a node failure or pre-emption, twice at most, with a reason each time", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil)
		res, got := reconcileOnce(j)
		Expect(res.RequeueAfter).To(Equal(ContinueResync))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRerunNodeFail))
		Expect(got.Status.Message).To(ContainSubstring("re-running the step from the start (1/2) as attempt 2"))
		Expect(got.Status.Reruns).To(Equal(int32(1)))
		Expect(got.Status.Continuations).To(BeZero(), "a re-run is not a continuation")
		Expect(got.Status.Resume).To(BeNil())
		got = settleTo(j, hpcv1alpha1.PhaseAdmitted)
		Expect(envMap(runnerJob(got, 2))).NotTo(HaveKey("KAOS_RESUME_REF"), "the re-run starts from scratch")

		ended(j, 2, "PREEMPTED", nil, "", nil)
		_, got = reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRerunPreempted))
		Expect(got.Status.Reruns).To(Equal(int32(2)))
		settleTo(j, hpcv1alpha1.PhaseAdmitted)

		ended(j, 3, "NODE_FAIL", nil, "", nil)
		_, got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRerunLimit))
		Expect(got.Status.Attempts).To(HaveLen(3), "every interrupted attempt is settled")
	})

	It("tier 1: never re-runs the step's own error, an out-of-memory kill, a wall-time kill or a cancel", func() {
		for _, c := range []struct {
			state string
			exit  *int32
		}{{"FAILED", i32(1)}, {"OUT_OF_MEMORY", i32(0)}, {"TIMEOUT", nil}, {"CANCELLED by 1000", nil}} {
			j := newServeJob(nil, nil)
			reconcileOnce(j)
			ended(j, 1, c.state, c.exit, "", nil)
			_, got := reconcileOnce(j)
			Expect(got.Status.Phase.Terminal()).To(BeTrue(), c.state)
			Expect(got.Status.Reruns).To(BeZero(), c.state)
		}
	})

	It("tier 1: a step that opted out of resuming is still re-run after a node failure", func() {
		j := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.ResumableOptOut = true })
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil)
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRerunNodeFail))
	})

	It("continues a step that was never marked from the checkpoint copy it persisted, on every recoverable end", func() {
		for i, c := range []struct {
			state string
			exit  *int32
		}{{"TIMEOUT", nil}, {"NODE_FAIL", nil}, {"PREEMPTED", nil}, {"FAILED", i32(85)}} {
			j := newServeJob(nil, nil)
			reconcileOnce(j)
			ended(j, 1, c.state, c.exit, fmt.Sprintf("scratch://ckpt/%d", i), progress(2, 40))
			_, got := reconcileOnce(j)
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing), c.state)
			Expect(got.Status.Continuations).To(Equal(int32(1)), c.state)
			Expect(got.Status.Reruns).To(BeZero(), c.state)
			Expect(got.Status.Resume.ProgressValues).To(Equal([]int64{2, 40}), c.state)
			got = settleTo(j, hpcv1alpha1.PhaseAdmitted)
			Expect(envMap(runnerJob(got, 2))).To(HaveKeyWithValue("KAOS_RESUME_REF", fmt.Sprintf("scratch://ckpt/%d", i)))
		}
	})

	It("does not continue the step's own error or the idle watchdog's end even when a checkpoint exists, nor a step that opted out", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		ended(j, 1, "FAILED", i32(1), "scratch://ckpt/x", progress(1))
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Continuations).To(BeZero())

		idle := newServeJob(nil, nil)
		reconcileOnce(idle)
		ended(idle, 1, "FAILED", i32(236), "scratch://ckpt/z", progress(1))
		_, got = reconcileOnce(idle)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonIdle))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Continuations).To(BeZero())

		out := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.ResumableOptOut = true })
		reconcileOnce(out)
		ended(out, 1, "TIMEOUT", nil, "scratch://ckpt/y", progress(1))
		_, got = reconcileOnce(out)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Continuations).To(BeZero())
	})

	It("a wall-time end with no checkpoint and an idle kill with no checkpoint end as before", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		ended(j, 1, "FAILED", i32(236), "", nil)
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonIdle))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
	})

	It("ends a continuation that did not get past its checkpoint with NoProgress and records the failed evidence", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		ended(j, 1, "TIMEOUT", nil, "scratch://ckpt/a", progress(3, 100))
		reconcileOnce(j)
		settleTo(j, hpcv1alpha1.PhaseAdmitted)
		ended(j, 2, "TIMEOUT", nil, "scratch://ckpt/b", progress(3, 100))
		_, got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoProgress))
		Expect(got.Status.Message).To(ContainSubstring("not past the one it started from"))
		Expect(got.Status.ResumeEvidence).NotTo(BeNil())
		Expect(got.Status.ResumeEvidence.Verified).To(BeFalse())
	})

	It("ends a continuation that saved no checkpoint at all at its wall time with NoProgress", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		ended(j, 1, "TIMEOUT", nil, "scratch://ckpt/a", progress(3))
		reconcileOnce(j)
		settleTo(j, hpcv1alpha1.PhaseAdmitted)
		ended(j, 2, "TIMEOUT", nil, "", nil)
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoProgress))
	})

	It("a node failure before a continuation's first new checkpoint continues again from the same checkpoint (counted)", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		ended(j, 1, "TIMEOUT", nil, "scratch://ckpt/a", progress(3))
		reconcileOnce(j)
		settleTo(j, hpcv1alpha1.PhaseAdmitted)
		ended(j, 2, "NODE_FAIL", nil, "", nil)
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
		Expect(got.Status.Continuations).To(Equal(int32(2)))
		Expect(got.Status.Resume.CheckpointRef).To(Equal("scratch://ckpt/a"))
		Expect(got.Status.Resume.FromAttempt).To(Equal(int32(1)))
	})

	It("stops at the continuation cap (5 for a step that was never marked)", func() {
		j := newServeJob(nil, nil)
		reconcileOnce(j)
		for a := int32(1); a <= 5; a++ {
			ended(j, a, "TIMEOUT", nil, fmt.Sprintf("scratch://ckpt/%d", a), progress(int64(a)))
			_, got := reconcileOnce(j)
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
			settleTo(j, hpcv1alpha1.PhaseAdmitted)
		}
		ended(j, 6, "TIMEOUT", nil, "scratch://ckpt/6", progress(6))
		_, got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuationLimit))
	})

	It("verifies a step passively when its continuation completes (live hpc-289baf48: success leaves no copy to compare)", func() {
		first := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		reconcileOnce(first)
		ended(first, 1, "FAILED", i32(85), "scratch://ckpt/a", progress(1111))
		reconcileOnce(first)
		settleTo(first, hpcv1alpha1.PhaseAdmitted)
		zero := int32(0)
		ended(first, 2, "COMPLETED", &zero, "", nil)
		_, got := reconcileOnce(first)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(got.Status.ResumeEvidence).NotTo(BeNil())
		Expect(got.Status.ResumeEvidence.Verified).To(BeTrue())

		next := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		_, got = reconcileOnce(next)
		Expect(got.Status.Placement.Resumable).NotTo(BeNil())
		Expect(got.Status.Placement.Resumable.Verified).To(BeTrue())
	})

	It("verifies a step passively: after a continuation gets past its checkpoint, the next job of that step is resumable without a mark", func() {
		first := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		reconcileOnce(first)
		_, got := reconcileOnce(first)
		Expect(got.Status.Placement.Resumable).To(BeNil(), "nothing is verified before a continuation has shown progress")
		Expect(envMap(runnerJob(got, 1))).NotTo(HaveKey("HPC_RESUMABLE_LEAD"))

		ended(first, 1, "TIMEOUT", nil, "scratch://ckpt/a", progress(3))
		reconcileOnce(first)
		settleTo(first, hpcv1alpha1.PhaseAdmitted)
		ended(first, 2, "TIMEOUT", nil, "scratch://ckpt/b", progress(4))
		_, got = reconcileOnce(first)
		Expect(got.Status.ResumeEvidence).NotTo(BeNil())
		Expect(got.Status.ResumeEvidence.Verified).To(BeTrue())
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))

		next := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg.example/lab/app:v2" }) // a new build of the same app
		_, got = reconcileOnce(next)
		Expect(got.Status.Placement.Resumable).NotTo(BeNil())
		Expect(got.Status.Placement.Resumable.Verified).To(BeTrue())
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementDeclared), "a verified step keeps its declared limit (D-67)")
		Expect(got.EffectiveResumable()).NotTo(BeNil())
		env := envMap(runnerJob(got, 1))
		Expect(env).To(HaveKeyWithValue("HPC_RESUMABLE_LEAD", "10"))
		Expect(env).To(HaveKeyWithValue("HPC_CKPT_BUDGET_PERCENT", "10"))

		optOut := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Image = "reg.example/lab/app:v3"
			j.Spec.ResumableOptOut = true
		})
		_, got = reconcileOnce(optOut)
		Expect(got.EffectiveResumable()).To(BeNil(), "resumable=False wins over a verified history")
		Expect(envMap(runnerJob(got, 1))).NotTo(HaveKey("HPC_RESUMABLE_LEAD"))
	})

	It("a later continuation with no progress clears the verification", func() {
		old := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = appImage })
		reconcileOnce(old)
		for _, v := range []int64{3, 4} {
			ended(old, int32(v-2), "TIMEOUT", nil, fmt.Sprintf("scratch://ckpt/%d", v), progress(v))
			reconcileOnce(old)
			settleTo(old, hpcv1alpha1.PhaseAdmitted)
		}
		time.Sleep(1100 * time.Millisecond) // evidence times have one-second resolution
		bad := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg.example/lab/app:v2" })
		reconcileOnce(bad)
		ended(bad, 1, "TIMEOUT", nil, "scratch://ckpt/p", progress(9))
		reconcileOnce(bad)
		settleTo(bad, hpcv1alpha1.PhaseAdmitted)
		ended(bad, 2, "TIMEOUT", nil, "scratch://ckpt/q", progress(9))
		reconcileOnce(bad)

		fresh := newServeJob(nil, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg.example/lab/app:v3" })
		_, got := reconcileOnce(fresh)
		Expect(got.Status.Placement.Resumable).NotTo(BeNil())
		Expect(got.Status.Placement.Resumable.Verified).To(BeFalse())
		Expect(got.Status.Placement.Resumable.Reason).To(ContainSubstring("NoProgress"))
		Expect(got.EffectiveResumable()).To(BeNil())
	})

	It("publishes the scratch the project's checkpoint copies hold on the HPCSite and drops a succeeded step's", func() {
		held := newServeJob(nil, nil)
		reconcileOnce(held)
		ended(held, 1, "TIMEOUT", nil, "scratch://ckpt/a", &hpcv1alpha1.CheckpointInfo{Bytes: 5, StepBytes: 12})
		reconcileOnce(held)
		done := newServeJob(nil, nil)
		reconcileOnce(done)
		ended(done, 1, "COMPLETED", i32(0), "", &hpcv1alpha1.CheckpointInfo{StepBytes: 999})
		reconcileOnce(done)
		reconcileOnce(held)
		site := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: "meluxina"}, site)).To(Succeed())
		Expect(site.Status.Checkpoints).NotTo(BeNil())
		Expect(site.Status.Checkpoints.Bytes).To(Equal(int64(12)))
		Expect(site.Status.Checkpoints.Steps).To(Equal(int32(1)))
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
