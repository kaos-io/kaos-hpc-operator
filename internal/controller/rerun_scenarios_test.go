package controller

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
	"github.com/kaos-io/kaos-hpc-operator/internal/timelimit"
)

// PRD kaos#1911 D-74 / AC-13: every step can fail and be run again. These scenarios drive the real reconciler
// against envtest with the runner's reports patched in (the controller never talks to a site itself), one scenario
// per kind of ending. They assert today's behaviour on main. Where the brief of AC-13 asks for more than main does,
// the scenario is an expected failure (xfailRerun): it SKIPs with the defect while the defect stands, and FAILS when
// the defect is fixed, so the marker must then be removed.

// xfailRerun is the expected-failure marker of this file: ok is whether the wanted behaviour holds today.
func xfailRerun(ok bool, defect string) {
	GinkgoHelper()
	if ok {
		Fail("expected failure now passes, remove the marker: " + defect)
	}
	Skip("EXPECTED FAILURE (reported defect): " + defect)
}

var _ = Describe("AC-13 failure and re-run scenarios", func() {
	var (
		r       *HPCArgoWorkflowJobReconciler
		project string
		envNS   string
		hpcNS   string
		seq     int
	)
	const image = "reg.example/lab/rr:v1"
	i32 := func(v int32) *int32 { return &v }
	sha := strings.Repeat("ab", 32)

	newReconciler := func() *HPCArgoWorkflowJobReconciler {
		return &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test"}
	}

	BeforeEach(func() {
		seq = 0
		project = fmt.Sprintf("rr%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Sharing: true,
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44", ScratchRoot: "/project/scratch/p1",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 1000}},
		})).To(Succeed())
		r = newReconciler()
	})

	newJob := func(mod func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("rr-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: image,
				WallTimeMinutes: 60, Command: []string{"python", "train.py"}, StepName: "train",
				Inputs: []hpcv1alpha1.StepInput{{Name: "labels", Repo: "dataset-labels", Commit: sha}}},
		}
		if mod != nil {
			mod(j)
		}
		withWorkflow(j)
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	reconcileWith := func(rc *HPCArgoWorkflowJobReconciler, j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		_, err := rc.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}
	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob { return reconcileWith(r, j) }

	// toAdmitted reconciles until the job holds a booking in phase Admitted (a continuation is judged on a later pass).
	toAdmitted := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for range 4 {
			got = reconcileOnce(j)
			if got.Status.Phase == hpcv1alpha1.PhaseAdmitted && got.Status.Booking != nil {
				break
			}
		}
		return got
	}

	runnerJobs := func(j *hpcv1alpha1.HPCArgoWorkflowJob) []batchv1.Job {
		var l batchv1.JobList
		Expect(k8sClient.List(ctx, &l, client.InNamespace(hpcNS), client.MatchingLabels{runnerjob.LabelJobUID: string(j.UID)})).To(Succeed())
		return l.Items
	}
	runnerJob := func(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32) *batchv1.Job {
		rj := &batchv1.Job{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.Name(j.UID, attempt)}, rj); err != nil {
			Expect(client.IgnoreNotFound(err)).To(Succeed())
			return nil
		}
		return rj
	}
	envOf := func(rj *batchv1.Job) map[string]string {
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
	// ended reports the terminal end of an attempt that ran for the given minutes.
	ended := func(j *hpcv1alpha1.HPCArgoWorkflowJob, attempt int32, state string, exit *int32, ref string, ck *hpcv1alpha1.CheckpointInfo, minutes int) {
		start, end := metav1.NewTime(time.Now().Add(-time.Duration(minutes)*time.Minute)), metav1.NewTime(time.Now())
		report(j, hpcv1alpha1.RunStatus{Attempt: attempt, SiteJobID: fmt.Sprintf("70%d", attempt), State: state, ExitCode: exit,
			StartTime: &start, EndTime: &end, Terminal: true, CheckpointRef: ref, Checkpoint: ck})
	}
	progress := func(values ...int64) *hpcv1alpha1.CheckpointInfo {
		return &hpcv1alpha1.CheckpointInfo{ProgressValues: values, Bytes: 10, StepBytes: 20}
	}
	ref := func(n int) string { return fmt.Sprintf("scratch://ckpt/%d", n) }
	resumable := func(max int32) func(*hpcv1alpha1.HPCArgoWorkflowJob) {
		return func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: max}
		}
	}
	shutdown := func(j *hpcv1alpha1.HPCArgoWorkflowJob, how string) {
		wf := &unstructured.Unstructured{}
		wf.SetGroupVersionKind(workflowGVK)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: envNS, Name: j.Spec.Workflow.Name}, wf)).To(Succeed())
		Expect(unstructured.SetNestedField(wf.Object, how, "spec", "shutdown")).To(Succeed())
		Expect(k8sClient.Update(ctx, wf)).To(Succeed())
	}
	// finishDeletes plays the garbage collector envtest lacks for every runner Job of the step being deleted.
	finishDeletes := func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
		for _, rj := range runnerJobs(j) {
			if rj.DeletionTimestamp != nil {
				controllerutil.RemoveFinalizer(&rj, metav1.FinalizerDeleteDependents)
				Expect(k8sClient.Update(ctx, &rj)).To(Succeed())
			}
		}
	}
	// stopAndSettle reconciles a job being stopped to its end, playing what envtest lacks: the garbage collector and the
	// cancel-only runner of the orphan sweep, which reports the site's final state (CANCELLED) for the current attempt.
	stopAndSettle := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for range 10 {
			got = reconcileOnce(j)
			finishDeletes(j)
			if got.Status.Sweep != nil && !got.Status.Phase.Terminal() {
				now := metav1.Now()
				report(j, hpcv1alpha1.RunStatus{Attempt: max(got.Status.Attempt, 1), State: "CANCELLED", StartTime: &now, EndTime: &now, Terminal: true})
			}
		}
		return got
	}
	// seedSuccess adds a finished, succeeded run of the same step (same namespace, step, class, image repository).
	seedSuccess := func(minutes int) {
		j := newJob(nil)
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		end := metav1.NewTime(time.Now().Add(-time.Hour))
		start := metav1.NewTime(end.Add(-time.Duration(minutes) * time.Minute))
		got.Status.Phase, got.Status.Attempt = hpcv1alpha1.PhaseSucceeded, 1
		got.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "COMPLETED", StartTime: &start, EndTime: &end, Terminal: true}
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
	}

	// ---- 1. limit reached with a new checkpoint ----

	It("S1: continues as a new job with its own runner (fresh access) and the same pinned inputs, up to the budget of 5; the 6th stops", func() {
		j := newJob(resumable(5))
		got := reconcileOnce(j)
		wantInputs := envOf(runnerJob(got, 1))["HPC_INPUTS"]
		Expect(wantInputs).To(ContainSubstring(sha))
		seen := map[string]bool{runnerJob(got, 1).Name: true}
		for a := int32(1); a <= 5; a++ {
			ended(j, a, "TIMEOUT", nil, ref(int(a)), progress(int64(a)), 30)
			got = reconcileOnce(j)
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing), "attempt %d", a)
			Expect(got.Status.Continuations).To(Equal(a))
			Expect(got.Status.Resume.CheckpointRef).To(Equal(ref(int(a))))
			got = toAdmitted(j)
			rj := runnerJob(got, a+1)
			Expect(rj).NotTo(BeNil(), "attempt %d has a runner of its own", a+1)
			Expect(seen).NotTo(HaveKey(rj.Name), "a new job, never the old runner")
			seen[rj.Name] = true
			env := envOf(rj)
			Expect(env).To(HaveKeyWithValue("KAOS_RESUME_REF", ref(int(a))))
			Expect(env).To(HaveKeyWithValue("KAOS_ATTEMPT", fmt.Sprint(a+1)))
			Expect(env).To(HaveKeyWithValue("HPC_INPUTS", wantInputs), "the same inputs, pinned by commit")
		}
		ended(j, 6, "TIMEOUT", nil, ref(6), progress(6), 30)
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuationLimit))
		Expect(got.Status.Message).To(ContainSubstring("5 continuations"))
		Expect(got.Status.Continuations).To(Equal(int32(5)))
		Expect(runnerJob(got, 7)).To(BeNil(), "no seventh run")
		Expect(got.Status.Attempts).To(HaveLen(6))
	})

	It("S1: the runs of a long step together never pass its walltime cap, the last run is cut to what is left", func() {
		// Declared 1500 min; one run covers at most the data access (690 min): 690 + 690 + 120.
		j := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.WallTimeMinutes = 1500
			j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
		})
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(timelimit.TokenCoverMinutes)))
		total := got.Status.Placement.TimeLimitMinutes
		for a, want := range []int32{690, 120} {
			ended(j, int32(a+1), "TIMEOUT", nil, ref(a+1), progress(int64(a+1)), int(got.Status.Placement.TimeLimitMinutes))
			got = reconcileOnce(j)
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
			got = toAdmitted(j)
			Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(want), "limit of run %d", a+2)
			total += got.Status.Placement.TimeLimitMinutes
		}
		Expect(total).To(Equal(int32(1500)), "the planned runs add up to the declared cap, no more")
		ended(j, 3, "TIMEOUT", nil, ref(3), progress(3), 120)
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached), "the whole declared limit is used up: no fourth run")
		Expect(runnerJob(got, 4)).To(BeNil())
	})

	// ---- 2. limit reached, nothing saved ----

	It("S2: at a learned limit, a timeout with nothing saved retries once at the declared limit; a second timeout fails", func() {
		for range 3 {
			seedSuccess(20)
		}
		j := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.WallTimeMinutes = 240 })
		got := reconcileOnce(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementLearned))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(30)))
		ended(j, 1, "TIMEOUT", nil, "", nil, 30)
		got = reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTimeLimitRetry))
		got = toAdmitted(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementTimeoutRetry))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(240)), "the declared limit")
		ended(j, 2, "TIMEOUT", nil, "", nil, 240)
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(runnerJob(got, 3)).To(BeNil(), "retried once, not twice")
	})

	It("S2: at the declared limit, a timeout with nothing saved fails at once and says no checkpoint was saved", func() {
		j := newJob(resumable(5))
		reconcileOnce(j)
		ended(j, 1, "TIMEOUT", nil, "", nil, 60)
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(got.Status.Message).To(ContainSubstring("before it saved a checkpoint"))
		Expect(got.Status.Attempts).To(HaveLen(1))
		Expect(runnerJob(got, 2)).To(BeNil())
		// A step that cannot checkpoint is told how to get there.
		plain := newJob(nil)
		reconcileOnce(plain)
		ended(plain, 1, "TIMEOUT", nil, "", nil, 60)
		got = reconcileOnce(plain)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonWallTimeReached))
		Expect(got.Status.Message).To(ContainSubstring("make it resumable"))
	})

	// ---- 3. continuation that got no further ----

	DescribeTable("S3: a continuation that got no further than its checkpoint fails NoProgress",
		func(first, second *hpcv1alpha1.CheckpointInfo) {
			j := newJob(nil)
			reconcileOnce(j)
			ended(j, 1, "TIMEOUT", nil, ref(1), first, 30)
			Expect(reconcileOnce(j).Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
			toAdmitted(j)
			ended(j, 2, "TIMEOUT", nil, ref(2), second, 30)
			got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoProgress))
			Expect(got.Status.Continuations).To(Equal(int32(1)), "no second continuation was counted")
			Expect(runnerJob(got, 3)).To(BeNil())
		},
		Entry("same progress values", progress(3, 100), progress(3, 100)),
		Entry("progress went backwards", progress(3, 100), progress(3, 90)),
		Entry("same digest, no progress values", &hpcv1alpha1.CheckpointInfo{Digest: "d1"}, &hpcv1alpha1.CheckpointInfo{Digest: "d1"}),
		Entry("no digest, no progress values", &hpcv1alpha1.CheckpointInfo{Bytes: 5}, &hpcv1alpha1.CheckpointInfo{Bytes: 5}),
	)

	It("S3: a continuation that moved on (a bigger value, or a changed digest) is not NoProgress", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "TIMEOUT", nil, ref(1), &hpcv1alpha1.CheckpointInfo{Digest: "d1"}, 30)
		reconcileOnce(j)
		toAdmitted(j)
		ended(j, 2, "TIMEOUT", nil, ref(2), &hpcv1alpha1.CheckpointInfo{Digest: "d2"}, 30)
		got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
		toAdmitted(j)
		ended(j, 3, "TIMEOUT", nil, ref(3), &hpcv1alpha1.CheckpointInfo{Digest: "d3", ProgressValues: []int64{1}}, 30)
		got = reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing), "a mark that gains progress values is compared by digest")
	})

	// ---- 4. ends that are not the step's doing ----

	DescribeTable("S4: an end that is not the step's doing re-runs a step without a checkpoint from the start, twice at most",
		func(state string, wantReason string) {
			j := newJob(nil)
			reconcileOnce(j)
			for n := int32(1); n <= 2; n++ {
				ended(j, n, state, nil, "", nil, 10)
				got := reconcileOnce(j)
				Expect(got.Status.Reason).To(Equal(wantReason))
				Expect(got.Status.Reruns).To(Equal(n))
				Expect(got.Status.Resume).To(BeNil())
				got = toAdmitted(j)
				Expect(envOf(runnerJob(got, n+1))).NotTo(HaveKey("KAOS_RESUME_REF"), "from the start")
			}
			ended(j, 3, state, nil, "", nil, 10)
			got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRerunLimit))
			Expect(got.Status.Reruns).To(Equal(int32(2)))
			Expect(runnerJob(got, 4)).To(BeNil())
		},
		Entry("NODE_FAIL", "NODE_FAIL", hpcv1alpha1.ReasonRerunNodeFail),
		Entry("BOOT_FAIL", "BOOT_FAIL", hpcv1alpha1.ReasonRerunNodeFail),
		Entry("REVOKED", "REVOKED", hpcv1alpha1.ReasonRerunNodeFail),
		Entry("PREEMPTED", "PREEMPTED", hpcv1alpha1.ReasonRerunPreempted),
	)

	DescribeTable("S4: the same ends with a checkpoint continue from it",
		func(state string) {
			j := newJob(nil)
			reconcileOnce(j)
			ended(j, 1, state, nil, ref(1), progress(7), 10)
			got := reconcileOnce(j)
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
			Expect(got.Status.Reruns).To(BeZero())
			got = toAdmitted(j)
			Expect(envOf(runnerJob(got, 2))).To(HaveKeyWithValue("KAOS_RESUME_REF", ref(1)))
		},
		Entry("NODE_FAIL", "NODE_FAIL"), Entry("BOOT_FAIL", "BOOT_FAIL"), Entry("REVOKED", "REVOKED"), Entry("PREEMPTED", "PREEMPTED"))

	It("S4: a cancel the user's stop caused is never re-run", func() {
		j := newJob(nil)
		reconcileOnce(j)
		shutdown(j, "Terminate")
		got := stopAndSettle(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
		Expect(got.Status.Reruns).To(BeZero())
		Expect(runnerJob(got, 2)).To(BeNil())
	})

	It("S4: a Slurm CANCELLED the platform did not ask for (an admin) is re-run", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "CANCELLED by 0", nil, "", nil, 10)
		got := reconcileOnce(j)
		xfailRerun(got.Status.Reason == hpcv1alpha1.ReasonRerunNodeFail || got.Status.Reason == hpcv1alpha1.ReasonRerunPreempted,
			"internal/resume/resume.go:47 Classify maps every CANCELLED* state to EndCancelled, never recovered: an admin's cancel ends the step Cancelled, it cannot be told from the user's stop by state")
	})

	It("S4: a site job the site no longer knows (the runner reports SITE_UNKNOWN) is re-run", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "SITE_UNKNOWN", nil, "", nil, 10)
		got := reconcileOnce(j)
		xfailRerun(got.Status.Reruns == 1 && got.Status.Reason != "SiteSITE_UNKNOWN",
			"internal/resume/resume.go:60 Classify sends any unknown state (SITE_UNKNOWN, runner.py:886) to EndStepError: a vanished job fails 'SiteSITE_UNKNOWN', never re-run")
	})

	It("S4: a REQUEUED report that ends the attempt is re-run (a live REQUEUED never reaches the controller as terminal: runner TERMINAL excludes it)", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "REQUEUED", nil, "", nil, 10)
		got := reconcileOnce(j)
		xfailRerun(got.Status.Reruns == 1,
			"internal/resume/resume.go:60 Classify has no REQUEUED: a terminal REQUEUED report fails 'SiteREQUEUED'")
	})

	// ---- 5. the step's own end ----

	DescribeTable("S5: the step's own error and out-of-memory are never re-run, with or without a checkpoint; the reason names it",
		func(state string, exit *int32, wantReason string, ckpt string) {
			j := newJob(nil)
			reconcileOnce(j)
			var ck *hpcv1alpha1.CheckpointInfo
			if ckpt != "" {
				ck = progress(5)
			}
			ended(j, 1, state, exit, ckpt, ck, 10)
			got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
			Expect(got.Status.Reason).To(Equal(wantReason))
			Expect(got.Status.Reruns).To(BeZero())
			Expect(got.Status.Continuations).To(BeZero())
			Expect(runnerJob(got, 2)).To(BeNil())
		},
		Entry("exit 1", "FAILED", i32(1), "SiteFAILED", ""),
		Entry("exit 1 with a checkpoint", "FAILED", i32(1), "SiteFAILED", ref(1)),
		Entry("out of memory", "OUT_OF_MEMORY", i32(0), "SiteOUT_OF_MEMORY", ""),
		Entry("out of memory with a checkpoint", "OUT_OF_MEMORY", i32(0), "SiteOUT_OF_MEMORY", ref(1)),
	)

	It("S5: exit 85 continues only with progress", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "FAILED", i32(85), ref(1), progress(2), 10)
		Expect(reconcileOnce(j).Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
		toAdmitted(j)
		ended(j, 2, "FAILED", i32(85), ref(2), progress(2), 10) // saved, but no further
		got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonNoProgress))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))

		k := newJob(nil)
		reconcileOnce(k)
		ended(k, 1, "FAILED", i32(85), ref(1), progress(2), 10)
		reconcileOnce(k)
		toAdmitted(k)
		ended(k, 2, "FAILED", i32(85), ref(2), progress(3), 10)
		Expect(reconcileOnce(k).Status.Reason).To(Equal(hpcv1alpha1.ReasonContinuing))
	})

	// ---- 6. checkpoint recorded but unusable at the next start ----

	It("S6: exit 85 with no verified copy ends CheckpointMissing (reason string Timeout), no loop", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "FAILED", i32(85), "", nil, 10)
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonCheckpointMissing))
		Expect(got.Status.Message).To(ContainSubstring("checkpoint not saved"))
		Expect(runnerJob(got, 2)).To(BeNil())
	})

	It("S6: a continuation whose restore failed (no new copy, exit 85 or wall time) ends in one more attempt at most, never loops", func() {
		for _, end := range []struct {
			state string
			exit  *int32
			want  string
		}{{"FAILED", i32(85), hpcv1alpha1.ReasonCheckpointMissing}, {"TIMEOUT", nil, hpcv1alpha1.ReasonNoProgress}} {
			j := newJob(nil)
			reconcileOnce(j)
			ended(j, 1, "TIMEOUT", nil, ref(1), progress(4), 10)
			reconcileOnce(j)
			toAdmitted(j)
			ended(j, 2, end.state, end.exit, "", nil, 1)
			got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed), end.state)
			Expect(got.Status.Reason).To(Equal(end.want), end.state)
			Expect(got.Status.Attempts).To(HaveLen(2))
			Expect(runnerJob(got, 3)).To(BeNil())
			for range 3 { // later reconciles of a finished job start nothing
				got = reconcileOnce(j)
			}
			Expect(runnerJobs(got)).To(HaveLen(2))
		}
	})

	It("S6: an opted-out step that exits 85 ends NotResumable", func() {
		j := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.ResumableOptOut = true })
		reconcileOnce(j)
		ended(j, 1, "FAILED", i32(85), "", nil, 10)
		Expect(reconcileOnce(j).Status.Reason).To(Equal(hpcv1alpha1.ReasonNotResumable))
	})

	// ---- 7. two writers ----

	It("S7: the site requeueing the job itself (a live, non-terminal report) never starts a second runner or attempt", func() {
		j := newJob(nil)
		got := reconcileOnce(j)
		Expect(runnerJobs(got)).To(HaveLen(1))
		for _, state := range []string{"RUNNING", "REQUEUED", "PENDING", "RUNNING"} {
			report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "701", State: state})
			for range 3 {
				got = reconcileOnce(j)
			}
			Expect(runnerJobs(got)).To(HaveLen(1), "state %s", state)
			Expect(got.Status.Attempt).To(Equal(int32(1)))
			Expect(got.Status.Phase.Terminal()).To(BeFalse())
			Expect(got.Status.Attempts).To(BeEmpty())
		}
	})

	It("S7: while the controller resubmits, there is one live runner per attempt number and none for the next", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 5)
		reconcileOnce(j)
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for range 5 { // many passes, as the resync would
			got = toAdmitted(j)
		}
		live := map[string]int{}
		for _, rj := range runnerJobs(got) {
			live[rj.Labels[runnerjob.LabelAttempt]]++
		}
		Expect(live).To(Equal(map[string]int{"1": 1, "2": 1}))
	})

	It("S7: an old job reported finished late does not overwrite the newer attempt's status", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 5)
		reconcileOnce(j)
		got := toAdmitted(j)
		Expect(got.Status.Attempt).To(Equal(int32(2)))
		// the first site job's late report: COMPLETED, for attempt 1, then one with no attempt number
		for _, a := range []int32{1, 0} {
			zero := int32(0)
			now := metav1.Now()
			report(j, hpcv1alpha1.RunStatus{Attempt: a, SiteJobID: "701", State: "COMPLETED", ExitCode: &zero, StartTime: &now, EndTime: &now, Terminal: true})
			got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "attempt-%d report ignored", a)
			Expect(got.Status.Attempt).To(Equal(int32(2)))
			Expect(got.Status.Attempts).To(HaveLen(1), "only attempt 1's own end was settled")
			Expect(got.Status.Result).To(BeEmpty())
		}
		// the newer attempt then ends and is judged as its own
		ended(j, 2, "COMPLETED", i32(0), "", nil, 5)
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(got.Status.Attempts).To(HaveLen(2))
	})

	// ---- 8. controller restart ----

	It("S8: a restart in the middle of continuations loses no counter and submits nothing twice", func() {
		j := newJob(resumable(5))
		reconcileOnce(j)
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for a := int32(1); a <= 2; a++ {
			ended(j, a, "TIMEOUT", nil, ref(int(a)), progress(int64(a)), 20)
			reconcileOnce(j)
			toAdmitted(j)
		}
		r = newReconciler() // restart: every in-memory map is gone
		for range 3 {
			got = reconcileOnce(j)
		}
		Expect(got.Status.Continuations).To(Equal(int32(2)))
		Expect(got.Status.Attempt).To(Equal(int32(3)))
		Expect(runnerJobs(got)).To(HaveLen(3), "one runner per attempt, no duplicate")
		// and mid-wait: the end is seen, then the controller restarts before the next attempt is booked
		ended(j, 3, "TIMEOUT", nil, ref(3), progress(3), 20)
		got = reconcileOnce(j)
		Expect(got.Status.Continuations).To(Equal(int32(3)))
		r = newReconciler()
		got = toAdmitted(j)
		Expect(got.Status.Continuations).To(Equal(int32(3)))
		Expect(got.Status.Attempt).To(Equal(int32(4)))
		Expect(runnerJobs(got)).To(HaveLen(4))
		Expect(envOf(runnerJob(got, 4))).To(HaveKeyWithValue("KAOS_RESUME_REF", ref(3)))
	})

	It("S8: a restart between re-runs keeps the re-run count, so the limit of 2 still holds", func() {
		j := newJob(nil)
		reconcileOnce(j)
		for n := int32(1); n <= 2; n++ {
			ended(j, n, "NODE_FAIL", nil, "", nil, 5)
			Expect(reconcileOnce(j).Status.Reruns).To(Equal(n))
			r = newReconciler()
			got := toAdmitted(j)
			Expect(runnerJobs(got)).To(HaveLen(int(n) + 1))
		}
		ended(j, 3, "NODE_FAIL", nil, "", nil, 5)
		r = newReconciler()
		got := reconcileOnce(j)
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonRerunLimit))
	})

	It("S8: a restart while an old attempt's late report is pending still ignores it and keeps one runner", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 5)
		reconcileOnce(j)
		toAdmitted(j)
		ended(j, 1, "COMPLETED", i32(0), "", nil, 5) // late, stale
		r = newReconciler()
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for range 3 {
			got = reconcileOnce(j)
		}
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		Expect(runnerJobs(got)).To(HaveLen(2))
		Expect(got.Status.Reruns).To(Equal(int32(1)))
	})

	// ---- 9. access expiry ----

	It("S9: a step is never submitted with a limit longer than the access life covers (a step that cannot be cut is refused, no booking)", func() {
		plain := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.WallTimeMinutes = timelimit.TokenCoverMinutes + 1 })
		got := reconcileOnce(plain)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonTooLongNotResumable))
		Expect(got.Status.Booking).To(BeNil())
		Expect(runnerJobs(got)).To(BeEmpty())

		for _, wall := range []int32{timelimit.TokenCoverMinutes, timelimit.TokenCoverMinutes + 1, 2 * timelimit.TokenCoverMinutes, 1500} {
			j := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
				j.Spec.WallTimeMinutes = wall
				j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 10, MaxContinuations: 5}
			})
			got = reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), "wall %d", wall)
			Expect(got.Status.Placement.TimeLimitMinutes).To(BeNumerically("<=", timelimit.TokenCoverMinutes), "wall %d", wall)
			Expect(got.Status.Placement.TimeLimitMinutes).To(BeNumerically("<=", wall))
		}
	})

	// ---- 10. learned limit ----

	It("S10: history shorter than the run times out once under the learned limit and retries at the declared limit; a verified resumable step keeps its declared limit", func() {
		for range 3 {
			seedSuccess(10)
		}
		j := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.WallTimeMinutes = 120 })
		got := reconcileOnce(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementLearned))
		learned := got.Status.Placement.TimeLimitMinutes
		Expect(learned).To(BeNumerically("<", 120))
		ended(j, 1, "TIMEOUT", nil, "", nil, int(learned))
		reconcileOnce(j)
		got = toAdmitted(j)
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementTimeoutRetry))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(120)))

		// Verified resumable: a continuation of an earlier job of this step got past its checkpoint.
		v := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg.example/lab/verified:v1" })
		reconcileOnce(v)
		ended(v, 1, "TIMEOUT", nil, ref(1), progress(3), 10)
		reconcileOnce(v)
		toAdmitted(v)
		ended(v, 2, "TIMEOUT", nil, ref(2), progress(4), 10)
		reconcileOnce(v)
		for range 3 {
			seedSuccessOf := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Image = "reg.example/lab/verified:v1" })
			g := &hpcv1alpha1.HPCArgoWorkflowJob{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(seedSuccessOf), g)).To(Succeed())
			o := g.DeepCopy()
			end := metav1.NewTime(time.Now().Add(-time.Hour))
			start := metav1.NewTime(end.Add(-10 * time.Minute))
			g.Status.Phase, g.Status.Attempt = hpcv1alpha1.PhaseSucceeded, 1
			g.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, State: "COMPLETED", StartTime: &start, EndTime: &end, Terminal: true}
			Expect(k8sClient.Status().Patch(ctx, g, client.MergeFrom(o))).To(Succeed())
		}
		next := newJob(func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
			j.Spec.Image = "reg.example/lab/verified:v2"
			j.Spec.WallTimeMinutes = 120
		})
		got = reconcileOnce(next)
		Expect(got.Status.Placement.Resumable).NotTo(BeNil())
		Expect(got.Status.Placement.Resumable.Verified).To(BeTrue())
		// Today's rule (D-67): three short successes would teach a 15 minute limit, but a verified resumable step keeps
		// its declared limit. NOTE D-74 changes this rule (the limit becomes the step's, not the declared one); update
		// this assertion with that change.
		Expect(got.Status.Placement.Source).To(Equal(hpcv1alpha1.PlacementDeclared))
		Expect(got.Status.Placement.TimeLimitMinutes).To(Equal(int32(120)))
	})

	// ---- 11. stop or terminate during a wait ----

	DescribeTable("S11: a stop or terminate while the next attempt waits ends the step and starts no further job",
		func(first func(*hpcv1alpha1.HPCArgoWorkflowJob, int), how string) {
			j := newJob(nil)
			reconcileOnce(j)
			first(j, 1)
			got := reconcileOnce(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhasePending), "waiting for the next attempt")
			shutdown(j, how)
			got = stopAndSettle(j)
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
			Expect(runnerJob(got, 2)).To(BeNil(), "no job after the stop")
			Expect(got.Status.Booking).To(BeNil())
			for range 3 {
				got = reconcileOnce(j)
			}
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
			Expect(runnerJob(got, 2)).To(BeNil())
		},
		Entry("continuation wait, Stop", func(j *hpcv1alpha1.HPCArgoWorkflowJob, a int) {
			ended(j, int32(a), "TIMEOUT", nil, ref(1), progress(1), 10)
		}, "Stop"),
		Entry("continuation wait, Terminate", func(j *hpcv1alpha1.HPCArgoWorkflowJob, a int) {
			ended(j, int32(a), "TIMEOUT", nil, ref(1), progress(1), 10)
		}, "Terminate"),
		Entry("re-run wait, Stop", func(j *hpcv1alpha1.HPCArgoWorkflowJob, a int) {
			ended(j, int32(a), "NODE_FAIL", nil, "", nil, 10)
		}, "Stop"),
		Entry("re-run wait, Terminate", func(j *hpcv1alpha1.HPCArgoWorkflowJob, a int) {
			ended(j, int32(a), "PREEMPTED", nil, "", nil, 10)
		}, "Terminate"),
	)

	It("S11: deleting the job during a re-run wait ends it and starts no further job", func() {
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 10)
		reconcileOnce(j)
		Expect(k8sClient.Delete(ctx, j)).To(Succeed())
		for range 8 {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
			Expect(err).NotTo(HaveOccurred())
			finishDeletes(j)
		}
		Expect(runnerJob(j, 2)).To(BeNil())
	})

	// ---- 13. every waiting state has an upper bound ----
	//
	// The waits of recovery, each with its bound:
	//   - the next attempt waits for the site to answer: DefaultSiteOutageMax (30 min), then SiteUnavailable;
	//   - a stopped job waits for the site to confirm its site job ended: DefaultSweepGiveUp (6 h), then it settles;
	//   - a booked job whose HPCSite is gone: DefaultSiteMissingGrace (15 min), then cancelled;
	//   - a running attempt: its runner Job has an activeDeadlineSeconds;
	//   - the chain of attempts itself: TestRecoveryChainsTerminate;
	//   - the next attempt waits for a slot (ConcurrencyLimit) or for budget: no deadline of its own (see below).

	It("S13: a re-run that waits for an unreachable site fails SiteUnavailable after the outage bound", func() {
		clock := time.Now()
		r.Now = func() time.Time { return clock }
		r.SiteOutageMax = 30 * time.Minute
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 5)
		got := reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhasePending))
		health := sitestate.NewHealth()
		r.Health = health
		for i := range sitestate.FailuresToDown { // the poller's probes: the site does not answer
			health.Observe(hpcNS+"/meluxina", sitestate.Down, "HTTP 503", clock.Add(time.Duration(i)*sitestate.MinDownAfter))
		}
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		clock = clock.Add(31 * time.Minute)
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseFailed))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSiteUnavailable))
		Expect(runnerJob(got, 2)).To(BeNil())
	})

	It("S13: a stop whose site job cannot be confirmed settles after the sweep give-up", func() {
		clock := time.Now()
		r.Now = func() time.Time { return clock }
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 5)
		reconcileOnce(j)
		shutdown(j, "Terminate")
		var got *hpcv1alpha1.HPCArgoWorkflowJob
		for range 4 { // the cancel-only runner never reports: the site does not answer
			got = reconcileOnce(j)
			finishDeletes(j)
		}
		Expect(got.Status.Phase.Terminal()).To(BeFalse())
		Expect(got.Status.Sweep).NotTo(BeNil())
		clock = clock.Add(DefaultSweepGiveUp + time.Minute)
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseCancelled))
	})

	It("S13: every attempt's runner Job has a deadline, so a running attempt cannot last for ever", func() {
		j := newJob(resumable(5))
		got := reconcileOnce(j)
		for a := int32(1); a <= 3; a++ {
			rj := runnerJob(got, a)
			Expect(rj).NotTo(BeNil())
			Expect(rj.Spec.ActiveDeadlineSeconds).NotTo(BeNil(), "attempt %d", a)
			Expect(*rj.Spec.ActiveDeadlineSeconds).To(BeNumerically(">", int64(60*60)))
			ended(j, a, "TIMEOUT", nil, ref(int(a)), progress(int64(a)), 20)
			reconcileOnce(j)
			got = toAdmitted(j)
		}
	})

	It("S13: a re-run that waits for a slot has no deadline of its own", func() {
		site := &hpcv1alpha1.HPCSite{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: "meluxina"}, site)).To(Succeed())
		orig := site.DeepCopy()
		site.Spec.Limits.MaxConcurrentJobs = 1
		Expect(k8sClient.Patch(ctx, site, client.MergeFrom(orig))).To(Succeed())
		clock := time.Now()
		r.Now = func() time.Time { return clock }
		j := newJob(nil)
		reconcileOnce(j)
		ended(j, 1, "NODE_FAIL", nil, "", nil, 5)
		reconcileOnce(j) // Pending, booking released
		blocker := newJob(nil)
		got := reconcileOnce(blocker)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
		got = reconcileOnce(j)
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseWaiting))
		Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonConcurrencyLimit))
		clock = clock.Add(400 * 24 * time.Hour)
		got = reconcileOnce(j)
		xfailRerun(got.Status.Phase.Terminal(),
			"internal/admission/admission.go:95 a step waiting for a slot (ConcurrencyLimit, or WaitingForBudget at :107) has no deadline of its own: it ends only when another job frees the slot")
	})
})
