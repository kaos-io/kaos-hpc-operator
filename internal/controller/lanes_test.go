package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/metrics"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// Lane placement through Reconcile (PRD-HPC-1461 F-42, F-44, D-68, D-69).
var _ = Describe("HPCArgoWorkflowJob lane placement", func() {
	var (
		r    *HPCArgoWorkflowJobReconciler
		user string
		seq  int
	)

	candidates := []lanes.Candidate{
		{QOS: "test"},
		{QOS: "dev", ReservationByPartition: map[string]string{"gpu": "gpudev"}},
	}

	// project creates a project's two namespaces and its HPCSite for the given site user.
	project := func(name, siteUser string) (envNS string) {
		envNS = name + "-train"
		for _, n := range []string{envNS, name + "-ml"} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: name}}})).To(Succeed())
		}
		end := metav1.NewTime(time.Now().Add(24 * time.Hour))
		Expect(k8sClient.Create(ctx, &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: name + "-ml", Labels: map[string]string{hpcv1alpha1.ProjectLabel: name}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}},
					QOS: []hpcv1alpha1.SiteQOS{{Name: "default", MaxWallTimeMinutes: 2880},
						{Name: "test", MaxWallTimeMinutes: 30, MaxJobsPerUser: 1},
						{Name: "dev", MaxWallTimeMinutes: 360, MaxJobsPerUser: 1}},
					Reservations: []hpcv1alpha1.SiteReservation{{Name: "gpudev", Partition: "gpu", NodeCount: 5, EndTime: &end}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: siteUser, Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 1000}},
		})).To(Succeed())
		return envNS
	}

	newJob := func(ns string, wall int32) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("lane-%d", seq), Namespace: ns},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{App: "app", Context: `{"version":1}`, Class: "meluxina-gpu", Image: fmt.Sprintf("reg.example/lane/app%d:v1", seq),
				WallTimeMinutes: wall, Command: []string{"python", "train.py"}, StepName: "train",
				Env: []hpcv1alpha1.NameValue{{Name: "KAOS_LIB_SUPPORT", Value: "{}"}}},
		}
		withWorkflow(j)
		Expect(k8sClient.Create(ctx, j)).To(Succeed())
		return j
	}

	reconcileOnce := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)})
		Expect(err).NotTo(HaveOccurred())
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		return got
	}

	env := func(j *hpcv1alpha1.HPCArgoWorkflowJob, ns string) map[string]string {
		rj := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: runnerjob.Name(j.UID, 1)}, rj)).To(Succeed())
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

	BeforeEach(func() {
		seq = 0
		user = fmt.Sprintf("u%d", time.Now().UnixNano()%1_000_000_000)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RunnerImage: "kaos/hpc-operator:test",
			LaneCandidates: candidates}
	})

	It("should_keep_the_class_qos_when_no_candidates_are_configured", func() {
		r.LaneCandidates = nil
		ns := project(user+"-a", user)
		got := reconcileOnce(newJob(ns, 20))
		Expect(got.Status.Placement.QOS).To(BeEmpty())
		Expect(got.Status.Placement.Reservation).To(BeEmpty())
		Expect(env(got, user+"-a-ml")).NotTo(HaveKey("HPC_QOS"))
		Expect(env(got, user+"-a-ml")).NotTo(HaveKey("HPC_RESERVATION"))
	})

	It("should_place_two_parallel_short_jobs_in_test_and_dev_and_the_rest_on_the_class_default", func() {
		ns := project(user+"-b", user)
		before := testutil.ToFloat64(metrics.LanePlacements.WithLabelValues("test", "", "false"))
		var qos []string
		var envs []map[string]string
		for range 4 {
			got := reconcileOnce(newJob(ns, 20))
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted))
			qos = append(qos, got.Status.Placement.QOS+"/"+got.Status.Placement.Reservation)
			envs = append(envs, env(got, user+"-b-ml"))
		}
		Expect(qos).To(Equal([]string{"test/", "dev/gpudev", "/", "/"}))
		Expect(envs[0]).To(HaveKeyWithValue("HPC_QOS", "test"))
		Expect(envs[0]).NotTo(HaveKey("HPC_RESERVATION"))
		Expect(envs[1]).To(HaveKeyWithValue("HPC_QOS", "dev"))
		Expect(envs[1]).To(HaveKeyWithValue("HPC_RESERVATION", "gpudev"))
		Expect(envs[2]).NotTo(HaveKey("HPC_QOS"))
		Expect(testutil.ToFloat64(metrics.LanePlacements.WithLabelValues("test", "", "false")) - before).To(Equal(1.0))
	})

	It("should_use_dev_for_a_limit_over_the_test_maximum", func() {
		ns := project(user+"-c", user)
		got := reconcileOnce(newJob(ns, 120))
		Expect(got.Status.Placement.QOS).To(Equal("dev"))
		Expect(got.Status.Placement.Reservation).To(Equal("gpudev"))
	})

	It("should_count_the_same_site_identity_across_projects_but_not_another_identity", func() {
		a := project(user+"-d1", user)
		same := project(user+"-d2", user)
		other := project(user+"-d3", user+"x")
		Expect(reconcileOnce(newJob(a, 20)).Status.Placement.QOS).To(Equal("test"))
		Expect(reconcileOnce(newJob(same, 20)).Status.Placement.QOS).To(Equal("dev"), "test is taken by the same site user")
		Expect(reconcileOnce(newJob(other, 20)).Status.Placement.QOS).To(Equal("test"), "another site user has its own test slot")
	})

	It("should_free_the_lane_when_the_attempt_settles", func() {
		ns := project(user+"-e", user)
		first := reconcileOnce(newJob(ns, 20))
		Expect(first.Status.Placement.QOS).To(Equal("test"))
		start, end := metav1.NewTime(time.Now().Add(-10*time.Minute)), metav1.NewTime(time.Now().Add(-time.Minute))
		zero := int32(0)
		report(first, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "1", State: "COMPLETED", ExitCode: &zero,
			StartTime: &start, EndTime: &end, Terminal: true, QOS: "test"})
		Expect(reconcileOnce(first).Status.Phase).To(Equal(hpcv1alpha1.PhaseSucceeded))
		Expect(reconcileOnce(newJob(ns, 20)).Status.Placement.QOS).To(Equal("test"))
	})

	It("should_show_the_class_default_and_free_the_lane_when_the_runner_reports_a_fallback", func() {
		ns := project(user+"-f", user)
		refused := reconcileOnce(newJob(ns, 20))
		Expect(refused.Status.Placement.QOS).To(Equal("test"))
		before := testutil.ToFloat64(metrics.LanePlacements.WithLabelValues("default", "", "true"))
		report(refused, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "7", State: "PENDING", QOS: "default"})
		got := reconcileOnce(refused)
		Expect(got.Status.Placement.QOS).To(Equal("default"))
		Expect(got.Status.Placement.Reservation).To(BeEmpty())
		Expect(got.Status.Placement.RefusedQOS).To(Equal("test"), "the refusal stays visible after QOS is rewritten")
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRunning))
		reconcileOnce(refused) // counted once per attempt
		Expect(testutil.ToFloat64(metrics.LanePlacements.WithLabelValues("default", "", "true")) - before).To(Equal(1.0))
		Expect(reconcileOnce(newJob(ns, 20)).Status.Placement.QOS).To(Equal("test"), "the refused job no longer holds test")
	})

	It("should_not_touch_a_placement_that_never_had_a_lane_when_the_runner_reports_its_qos", func() {
		r.LaneCandidates = nil
		ns := project(user+"-g", user)
		j := reconcileOnce(newJob(ns, 20))
		report(j, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "9", State: "PENDING", QOS: "default"})
		Expect(reconcileOnce(j).Status.Placement.QOS).To(BeEmpty())
	})

	It("should_free_the_test_slot_when_a_host_takes_the_guest_that_held_it", func() {
		ns := project(user+"-h", user)
		guest := reconcileOnce(newJob(ns, 20))
		Expect(guest.Status.Placement.QOS).To(Equal("test"))
		report(guest, hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "5", State: "PENDING", QOS: "test", HostSiteJobID: "6"})
		reconcileOnce(guest)
		Expect(reconcileOnce(newJob(ns, 20)).Status.Placement.QOS).To(Equal("test"))
	})
})
