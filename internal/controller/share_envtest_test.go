package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
)

// Deterministic node sharing (PRD-HPC-1461 D-26, D-70): the controller packs shareable steps onto
// ceil(N / gpusPerNode) hosts at admission, whatever the site's free capacity, and plans a guest again when its
// host cannot take it. Live 2026-11-10: four 1-GPU steps of one run started on four nodes at the same second.
var _ = Describe("HPCArgoWorkflowJob node sharing plan", func() {
	var (
		r            *HPCArgoWorkflowJobReconciler
		rec          *record.FakeRecorder
		project      string
		envNS, hpcNS string
		seq          int
		clock        time.Time
	)

	candidates := []lanes.Candidate{
		{QOS: "test"},
		{QOS: "dev", ReservationByPartition: map[string]string{"gpu": "gpudev"}},
	}

	// siteWith creates the project's namespaces and its sharing HPCSite (4 GPUs per node, lanes test and dev).
	siteWith := func(mut func(*hpcv1alpha1.HPCSite)) {
		project = fmt.Sprintf("shr%d", time.Now().UnixNano()%1_000_000_000)
		envNS, hpcNS = project+"-train", project+"-ml"
		for _, n := range []string{envNS, hpcNS} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n,
				Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}})).To(Succeed())
		}
		end := metav1.NewTime(time.Now().Add(24 * time.Hour))
		site := &hpcv1alpha1.HPCSite{
			ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: hpcNS, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Sharing: true,
				Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST,
					Endpoint: "https://slurm-api.site.example", RESTVersion: "v0.0.44",
					Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu", QOS: "default",
						Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}},
					QOS: []hpcv1alpha1.SiteQOS{{Name: "default", MaxWallTimeMinutes: 2880},
						{Name: "test", MaxWallTimeMinutes: 30, MaxJobsPerUser: 1},
						{Name: "dev", MaxWallTimeMinutes: 360, MaxJobsPerUser: 1}},
					Reservations: []hpcv1alpha1.SiteReservation{{Name: "gpudev", Partition: "gpu", NodeCount: 5, EndTime: &end}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: project, Account: "p1",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "id"}},
				Budget: hpcv1alpha1.Budget{GPUNodeHours: 100000}},
		}
		if mut != nil {
			mut(site)
		}
		Expect(k8sClient.Create(ctx, site)).To(Succeed())
	}

	newStep := func(wall int32, mut func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
		seq++
		j := &hpcv1alpha1.HPCArgoWorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("share-%d", seq), Namespace: envNS},
			Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "reg.example/share/app:v1", WallTimeMinutes: wall,
				Command: []string{"python", "train.py"}, StepName: fmt.Sprintf("train-%d", seq),
				Sharing: &hpcv1alpha1.Sharing{Scope: "abcdef12"},
				Env:     []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}},
		}
		if mut != nil {
			mut(j)
		}
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

	admit := func(wall int32, mut func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
		got := reconcileOnce(newStep(wall, mut))
		Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseAdmitted), got.Status.Message)
		return got
	}

	patchStatus := func(j *hpcv1alpha1.HPCArgoWorkflowJob, mut func(*hpcv1alpha1.HPCArgoWorkflowJob)) {
		got := &hpcv1alpha1.HPCArgoWorkflowJob{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(j), got)).To(Succeed())
		orig := got.DeepCopy()
		mut(got)
		Expect(k8sClient.Status().Patch(ctx, got, client.MergeFrom(orig))).To(Succeed())
	}

	// start makes the job's Slurm job RUNNING since the (fake) clock's now.
	start := func(j *hpcv1alpha1.HPCArgoWorkflowJob, id string) {
		t := metav1.NewTime(clock)
		patchStatus(j, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
			g.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: id, State: "RUNNING", StartTime: &t}
		})
	}

	share := func(j *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.SharePlan {
		Expect(j.Status.Placement).NotTo(BeNil())
		Expect(j.Status.Placement.Share).NotTo(BeNil(), "a shareable step has a share plan")
		return j.Status.Placement.Share
	}

	envOf := func(j *hpcv1alpha1.HPCArgoWorkflowJob) map[string]string {
		rj := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: hpcNS, Name: runnerjob.Name(j.UID, 1)}, rj)).To(Succeed())
		m := map[string]string{}
		for _, e := range rj.Spec.Template.Spec.Containers[0].Env {
			m[e.Name] = e.Value
		}
		return m
	}

	hostsOf := func(jobs []*hpcv1alpha1.HPCArgoWorkflowJob) (hosts int) {
		for _, j := range jobs {
			if share(j).Role == hpcv1alpha1.ShareHost {
				hosts++
			}
		}
		return hosts
	}

	BeforeEach(func() {
		seq = 0
		clock = time.Now()
		rec = record.NewFakeRecorder(100)
		r = &HPCArgoWorkflowJobReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec,
			RunnerImage: "kaos/hpc-operator:test", LaneCandidates: candidates, Now: func() time.Time { return clock }}
	})

	Describe("packing", func() {
		It("should_pack_eight_steps_that_arrive_at_once_onto_two_hosts_whatever_the_free_capacity", func() {
			siteWith(nil)
			var jobs []*hpcv1alpha1.HPCArgoWorkflowJob
			for range 8 {
				jobs = append(jobs, admit(20, nil))
			}
			Expect(hostsOf(jobs)).To(Equal(2), "ceil(8 / 4 gpus per node)")
			for i, j := range jobs {
				role := share(j).Role
				if i%4 == 0 {
					Expect(role).To(Equal(hpcv1alpha1.ShareHost), "step %d opens a node", i)
				} else {
					Expect(role).To(Equal(hpcv1alpha1.ShareGuest), "step %d joins the open host", i)
					Expect(share(j).Host).To(Equal(client.ObjectKeyFromObject(jobs[i/4*4]).String()),
						"the fullest open host takes it, so nodes fill one after the other")
				}
			}
		})

		It("should_use_ceil_n_over_slots_hosts_for_any_n_and_never_more", func() {
			siteWith(func(s *hpcv1alpha1.HPCSite) { s.Spec.Budget.GPUNodeHours = 1_000_000 })
			var jobs []*hpcv1alpha1.HPCArgoWorkflowJob
			for n := 1; n <= 37; n++ {
				jobs = append(jobs, admit(20, nil))
				Expect(hostsOf(jobs)).To(Equal((n+3)/4), "after %d steps", n)
			}
		})

		It("should_give_a_guest_no_lane_so_it_never_takes_a_one_job_lane_a_host_needs", func() {
			siteWith(nil)
			host, guest := admit(20, nil), admit(20, nil)
			Expect(host.Status.Placement.QOS).To(Equal("test"), "the host keeps the lane of a short step: it leaves room for a guest")
			Expect(guest.Status.Placement.QOS).To(BeEmpty())
			Expect(guest.Status.Placement.Reservation).To(BeEmpty())
			next := admit(20, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Sharing.Scope = "otherscope1" })
			Expect(share(next).Role).To(Equal(hpcv1alpha1.ShareHost))
			Expect(next.Status.Placement.QOS).To(Equal("dev"), "test is taken by the first host; the guest used none")
		})

		It("should_put_the_role_in_the_runner_environment_and_the_hosts_job_limit_in_the_plan", func() {
			siteWith(nil)
			host, guest := admit(20, nil), admit(20, nil)
			Expect(envOf(host)).To(HaveKeyWithValue("HPC_SHARE_ROLE", "host"))
			Expect(envOf(guest)).To(HaveKeyWithValue("HPC_SHARE_ROLE", "guest"))
			Expect(share(host).JobLimitMinutes).To(Equal(int32(30)), "20 min padded to 40, capped by the test lane's 30")
			Expect(share(guest).JobLimitMinutes).To(BeZero())
		})

		It("should_keep_siblings_in_one_lane_when_the_step_limit_equals_the_fast_lanes_maximum", func() {
			// Live 2026-11-10: four 30-minute steps were spread over test, dev and the default; a 30-minute
			// host in the 30-minute test lane has no time left for a guest. The host goes to a lane that has.
			siteWith(nil)
			var jobs []*hpcv1alpha1.HPCArgoWorkflowJob
			for range 4 {
				jobs = append(jobs, admit(30, nil))
			}
			Expect(hostsOf(jobs)).To(Equal(1), "four steps, one node")
			Expect(jobs[0].Status.Placement.QOS).To(Equal("dev"))
			Expect(jobs[0].Status.Placement.Reservation).To(Equal("gpudev"))
			Expect(share(jobs[0]).JobLimitMinutes).To(Equal(int32(60)))
			for _, g := range jobs[1:] {
				Expect(g.Status.Placement.QOS).To(BeEmpty())
			}
		})

		It("should_open_a_host_of_its_own_for_a_step_that_does_not_fit_the_open_hosts_time", func() {
			siteWith(nil)
			host := admit(20, nil)
			long := admit(120, nil)
			Expect(share(host).JobLimitMinutes).To(Equal(int32(30)))
			Expect(share(long).Role).To(Equal(hpcv1alpha1.ShareHost), "126 minutes do not fit a 30-minute host")
			short := admit(20, nil)
			Expect(share(short).Host).To(Equal(client.ObjectKeyFromObject(host).String()))
		})

		It("should_not_join_a_host_with_less_time_left_than_the_step_and_the_margin", func() {
			siteWith(nil)
			host := admit(20, nil)
			start(host, "100")
			clock = clock.Add(5 * time.Minute) // 30-minute job, 5 gone: 25 left < 20 + 6
			Expect(share(admit(20, nil)).Role).To(Equal(hpcv1alpha1.ShareHost))
			clock = clock.Add(-4 * time.Minute) // 29 left... of 30: 1 gone, 29 >= 26
			Expect(share(admit(20, nil)).Role).To(Equal(hpcv1alpha1.ShareGuest))
		})

		It("should_not_share_across_scopes_or_classes", func() {
			siteWith(func(s *hpcv1alpha1.HPCSite) {
				s.Spec.Profile.Classes = append(s.Spec.Profile.Classes, hpcv1alpha1.SiteClass{Name: "meluxina-gpu-b", Partition: "gpu2",
					QOS: "default", Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880})
			})
			a := admit(20, nil)
			otherScope := admit(20, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Sharing.Scope = "zzzzzzzz9" })
			otherClass := admit(20, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Class = "meluxina-gpu-b" })
			Expect(share(a).Role).To(Equal(hpcv1alpha1.ShareHost))
			Expect(share(otherScope).Role).To(Equal(hpcv1alpha1.ShareHost))
			Expect(share(otherClass).Role).To(Equal(hpcv1alpha1.ShareHost))
		})

		It("should_not_plan_a_step_that_names_no_scope", func() {
			siteWith(nil)
			admit(20, nil)
			got := admit(20, func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Spec.Sharing = nil })
			Expect(got.Status.Placement.Share).To(BeNil())
			Expect(envOf(got)).NotTo(HaveKey("HPC_SHARE_ROLE"))
		})

		It("should_pack_a_mix_of_resumable_and_plain_steps_onto_ceil_n_over_four_hosts", func() {
			// AC-73: a resumable step shares too. Seven steps, three of them resumable, arrive at once. (A resumable step that
			// opens a host gets a job of its own limit, not padded, so it takes only guests that fit that: here both hosts are plain.)
			siteWith(nil)
			resumable := func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
				j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 5, MaxContinuations: 3}
			}
			muts := []func(*hpcv1alpha1.HPCArgoWorkflowJob){nil, resumable, nil, resumable, nil, resumable, nil}
			var jobs []*hpcv1alpha1.HPCArgoWorkflowJob
			for i, m := range muts {
				jobs = append(jobs, admit(20, m))
				Expect(hostsOf(jobs)).To(Equal((i+4)/4), "after %d steps", i+1)
			}
			Expect(hostsOf(jobs)).To(Equal(2), "ceil(7 / 4)")
			Expect(envOf(jobs[1])).To(HaveKeyWithValue("HPC_SHARE_ROLE", "guest"))
		})

		It("should_not_pad_a_resumable_hosts_job_and_join_it_by_the_guests_lead_when_only_the_lead_fits", func() {
			siteWith(nil)
			lead := func(j *hpcv1alpha1.HPCArgoWorkflowJob) {
				j.Spec.Resumable = &hpcv1alpha1.Resumable{LeadMinutes: 5, MaxContinuations: 3}
			}
			host := admit(20, lead)
			Expect(share(host).JobLimitMinutes).To(Equal(int32(20)), "a resumable host's job is the step's own limit")
			// A 60-minute resumable guest does not fit 20 minutes by its limit, but its lead (5 + 6) does.
			Expect(share(admit(60, lead)).Host).To(Equal(client.ObjectKeyFromObject(host).String()))
			// A 60-minute plain step does not.
			Expect(share(admit(60, nil)).Role).To(Equal(hpcv1alpha1.ShareHost))
		})

		It("should_not_plan_anything_on_a_site_that_does_not_share", func() {
			siteWith(func(s *hpcv1alpha1.HPCSite) { s.Spec.Sharing = false })
			Expect(admit(20, nil).Status.Placement.Share).To(BeNil())
		})

		It("should_count_a_guests_held_job_toward_the_sites_submit_cap", func() {
			// Four steps, one node: every one of them is a Slurm job at the site (the guests' are held until a
			// host takes them), so a cap of 3 refuses the fourth at admission. Plan within it.
			siteWith(func(s *hpcv1alpha1.HPCSite) { s.Spec.Profile.MaxSubmitJobs = 3 })
			for range 3 {
				admit(20, nil)
			}
			got := reconcileOnce(newStep(20, nil))
			Expect(got.Status.Phase).To(Equal(hpcv1alpha1.PhaseRefused))
			Expect(got.Status.Reason).To(Equal(hpcv1alpha1.ReasonSubmitLimit))
		})

		It("should_plan_against_a_job_the_cache_does_not_show_yet", func() {
			siteWith(nil)
			host := admit(20, nil)
			// The informer cache of a real manager lags behind this process's own write: what the process placed
			// is remembered, so the next admission still sees the open host.
			r.Client = &lagClient{Client: k8sClient, hide: host.UID}
			Expect(share(admit(20, nil)).Host).To(Equal(client.ObjectKeyFromObject(host).String()))
		})
	})

	Describe("no starvation: a guest that no host took is planned again", func() {
		replanned := func(g *hpcv1alpha1.HPCArgoWorkflowJob) *hpcv1alpha1.HPCArgoWorkflowJob { return reconcileOnce(g) }

		finish := func(h *hpcv1alpha1.HPCArgoWorkflowJob, state string) {
			t, e, rc := metav1.NewTime(clock.Add(-10*time.Minute)), metav1.NewTime(clock), int32(1)
			patchStatus(h, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
				g.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "100", State: state, ExitCode: &rc,
					StartTime: &t, EndTime: &e, Terminal: true}
			})
			reconcileOnce(h)
		}

		It("should_make_a_guest_a_host_when_its_host_ended", func() {
			siteWith(nil)
			host, guest := admit(20, nil), admit(20, nil)
			start(host, "100")
			finish(host, "FAILED")
			got := replanned(guest)
			Expect(share(got).Role).To(Equal(hpcv1alpha1.ShareHost))
			Expect(share(got).Replans).To(Equal(int32(1)))
			Expect(got.Status.Placement.QOS).To(Equal("test"), "a lane again: the host's lane was freed when it ended")
			Expect(got.Status.Booking).NotTo(BeNil(), "its booking is unchanged")
			Expect(rec.Events).To(Receive(ContainSubstring("its host ended")))
		})

		It("should_let_the_next_stranded_guest_join_the_host_the_first_one_opened", func() {
			siteWith(nil)
			first, guest, second := admit(20, nil), admit(20, nil), admit(20, nil)
			Expect(share(second).Host).To(Equal(client.ObjectKeyFromObject(first).String()))
			start(first, "100")
			finish(first, "FAILED")
			got := replanned(guest)
			Expect(share(got).Role).To(Equal(hpcv1alpha1.ShareHost), "no other open host: the first stranded guest opens one")
			again := replanned(second)
			Expect(share(again).Role).To(Equal(hpcv1alpha1.ShareGuest), "the next stranded guest joins the host just opened")
			Expect(share(again).Host).To(Equal(client.ObjectKeyFromObject(got).String()))
		})

		It("should_plan_a_guest_again_when_its_host_was_queued_too_long", func() {
			siteWith(nil)
			host, guest := admit(20, nil), admit(20, nil)
			clock = clock.Add(DefaultHostStartWait - time.Minute)
			Expect(share(replanned(guest)).Role).To(Equal(hpcv1alpha1.ShareGuest), "still inside the wait")
			clock = clock.Add(2 * time.Minute)
			got := replanned(guest)
			Expect(share(got).Role).To(Equal(hpcv1alpha1.ShareHost))
			Expect(share(admit(20, nil)).Host).To(Equal(client.ObjectKeyFromObject(got).String()),
				"a host that waited too long takes no new guests: the next step joins the new one")
			Expect(share(host).Role).To(Equal(hpcv1alpha1.ShareHost))
		})

		It("should_make_a_guest_a_host_when_a_running_host_did_not_take_it", func() {
			siteWith(nil)
			host, guest := admit(60, nil), admit(60, nil) // a 120-minute host in the dev lane: time is not what stops it
			Expect(share(host).JobLimitMinutes).To(Equal(int32(120)))
			start(host, "100")
			clock = clock.Add(DefaultGuestTakeWait - time.Minute)
			Expect(share(replanned(guest)).Role).To(Equal(hpcv1alpha1.ShareGuest))
			clock = clock.Add(2 * time.Minute)
			got := replanned(guest)
			Expect(share(got).Role).To(Equal(hpcv1alpha1.ShareHost), "the host skipped it: it opens a node, it does not join the host again")
			Expect(rec.Events).To(Receive(ContainSubstring("did not take it")))
		})

		It("should_leave_a_guest_a_host_took_alone", func() {
			siteWith(nil)
			host, guest := admit(20, nil), admit(20, nil)
			start(host, "100")
			patchStatus(guest, func(g *hpcv1alpha1.HPCArgoWorkflowJob) {
				g.Status.Run = &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: "101", State: "PENDING", HostSiteJobID: "100"}
			})
			clock = clock.Add(3 * time.Hour)
			Expect(share(replanned(guest)).Role).To(Equal(hpcv1alpha1.ShareGuest), "the runner handles a host lost under a taken guest")
		})

		It("should_bound_how_often_one_guest_is_planned_again", func() {
			siteWith(nil)
			var first []*hpcv1alpha1.HPCArgoWorkflowJob
			for range 4 {
				first = append(first, admit(20, nil))
			}
			h2 := admit(20, nil) // the fifth step opens the second node
			Expect(share(h2).Role).To(Equal(hpcv1alpha1.ShareHost), "the first node is full")
			patchStatus(first[1], func(g *hpcv1alpha1.HPCArgoWorkflowJob) { g.Status.Placement.Share.Replans = MaxShareReplans })
			start(first[0], "100")
			finish(first[0], "FAILED")
			Expect(share(replanned(first[1])).Role).To(Equal(hpcv1alpha1.ShareHost), "at the bound it opens a node although another host has room")
			joined := share(replanned(first[2]))
			Expect(joined.Role).To(Equal(hpcv1alpha1.ShareGuest), "within the bound it joins an open host")
			Expect(joined.Host).NotTo(Equal(client.ObjectKeyFromObject(first[0]).String()), "never the host that ended")
		})
	})
})

// lagClient hides one job from List, as an informer cache that has not seen a write yet.
type lagClient struct {
	client.Client
	hide types.UID
}

func (c *lagClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if jl, ok := list.(*hpcv1alpha1.HPCArgoWorkflowJobList); ok {
		kept := jl.Items[:0]
		for _, j := range jl.Items {
			if j.UID != c.hide {
				kept = append(kept, j)
			}
		}
		jl.Items = kept
	}
	return nil
}
