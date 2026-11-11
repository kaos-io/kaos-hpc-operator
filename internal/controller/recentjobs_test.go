package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

var recentT0 = time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)

func recentSite() *hpcv1alpha1.HPCSite {
	return &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: "p-hpc", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "p"}},
		Spec: hpcv1alpha1.HPCSiteSpec{Profile: hpcv1alpha1.SiteProfile{
			Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu"}}}},
	}
}

func recentJob(name string, age time.Duration, phase hpcv1alpha1.Phase) hpcv1alpha1.HPCArgoWorkflowJob {
	return hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "p-train", CreationTimestamp: metav1.NewTime(recentT0.Add(-age))},
		Spec:       hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", WallTimeMinutes: 60, StepName: name},
		Status:     hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: phase},
	}
}

func recentReconciler(t *testing.T, nss ...*corev1.Namespace) *HPCArgoWorkflowJobReconciler {
	t.Helper()
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	b := fake.NewClientBuilder().WithScheme(sch)
	for _, ns := range nss {
		b = b.WithObjects(ns)
	}
	return &HPCArgoWorkflowJobReconciler{Client: b.Build(), Now: func() time.Time { return recentT0 }}
}

func projectNS(name, project string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{hpcv1alpha1.ProjectLabel: project}}}
}

func names(l []hpcv1alpha1.RecentJob) []string {
	out := make([]string, 0, len(l))
	for _, j := range l {
		out = append(out, j.Name)
	}
	return out
}

func TestRecentJobsOrderAndScope(t *testing.T) {
	r := recentReconciler(t, projectNS("p-train", "p"), projectNS("q-train", "q"))
	site := recentSite()
	done := recentJob("done-new", time.Minute, hpcv1alpha1.PhaseSucceeded)
	oldActive := recentJob("active-old", time.Hour, hpcv1alpha1.PhaseRunning)
	oldActive.Status.Booking = &hpcv1alpha1.Booking{SiteName: "meluxina", SiteNamespace: "p-hpc", NodeMinutes: 60}
	waiting := recentJob("waiting-new", 2*time.Minute, hpcv1alpha1.PhaseWaiting) // never admitted: no site yet
	other := recentJob("other-project", time.Minute, hpcv1alpha1.PhaseWaiting)
	other.Namespace = "q-train"
	otherClass := recentJob("other-class", time.Minute, hpcv1alpha1.PhaseWaiting)
	otherClass.Spec.Class = "unknown"
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{done, oldActive, waiting, other, otherClass}

	got, err := r.recentJobsOf(context.Background(), site, jobs)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"waiting-new", "active-old", "done-new"} // active first, newest first within a group
	if fmt.Sprint(names(got)) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", names(got), want)
	}
}

func TestRecentJobsAreBoundedAndDeterministic(t *testing.T) {
	r := recentReconciler(t, projectNS("p-train", "p"))
	jobs := make([]hpcv1alpha1.HPCArgoWorkflowJob, 0, hpcv1alpha1.RecentJobsMax+80)
	for i := range hpcv1alpha1.RecentJobsMax + 80 {
		jobs = append(jobs, recentJob(fmt.Sprintf("j%02d", i), time.Duration(i)*time.Minute, hpcv1alpha1.PhaseRunning))
	}
	a, err := r.recentJobsOf(context.Background(), recentSite(), jobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != hpcv1alpha1.RecentJobsMax || a[0].Name != "j00" {
		t.Fatalf("got %d jobs, first %q", len(a), a[0].Name)
	}
	rev := make([]hpcv1alpha1.HPCArgoWorkflowJob, len(jobs))
	for i := range jobs {
		rev[len(jobs)-1-i] = jobs[i]
	}
	b, _ := r.recentJobsOf(context.Background(), recentSite(), rev)
	if fmt.Sprint(names(a)) != fmt.Sprint(names(b)) {
		t.Fatal("the list depends on the order the cache lists the jobs in")
	}
}

func TestRecentJobsEmptyIsNil(t *testing.T) {
	got, err := recentReconciler(t).recentJobsOf(context.Background(), recentSite(), nil)
	if err != nil || got != nil {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestRecentJobOfCopiesLaneLimitAndSpend(t *testing.T) {
	start, end := metav1.NewTime(recentT0), metav1.NewTime(recentT0.Add(40*time.Minute))
	j := recentJob("train", time.Hour, hpcv1alpha1.PhaseFailed)
	j.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: "wf-1", UID: "u"}
	j.Status.Reason = hpcv1alpha1.ReasonIdle
	j.Status.Attempt = 2
	j.Status.Placement = &hpcv1alpha1.Placement{TimeLimitMinutes: 45, Source: hpcv1alpha1.PlacementLearned, Samples: 4, QOS: "dev", Reservation: "gpudev", RefusedQOS: "test"}
	j.Status.Run = &hpcv1alpha1.RunStatus{SiteJobID: "77", State: "FAILED", StartTime: &start, EndTime: &end, QOS: "default"}
	j.Status.Attempts = []hpcv1alpha1.AttemptRecord{{SettledNodeMinutes: 30}, {SettledNodeMinutes: 12}}

	rj := recentJobOf(&j, time.Now())
	if rj.Workflow != "wf-1" || rj.Step != "train" || rj.Reason != "Idle" || rj.Attempt != 2 {
		t.Fatalf("identity fields: %+v", rj)
	}
	if rj.TimeLimitMinutes != 45 || rj.LimitSource != hpcv1alpha1.PlacementLearned || rj.LimitSamples != 4 || rj.IdleTimeoutMinutes != 20 {
		t.Fatalf("limit fields: %+v", rj)
	}
	// The lane the site job was submitted with wins over the placement's (the runner may fall back).
	if rj.QOS != "default" || rj.RefusedQOS != "test" || rj.SiteJobID != "77" {
		t.Fatalf("lane fields: %+v", rj)
	}
	if rj.BookedNodeMinutes != 45 || rj.SettledNodeMinutes != 42 || rj.PendingReason != "" {
		t.Fatalf("spend fields: %+v", rj)
	}
	if rj.StartTime == nil || !rj.StartTime.Equal(&start) || rj.EndTime == nil {
		t.Fatalf("times: %+v", rj)
	}
}

func TestRecentJobOfPendingReasonOnlyWhileQueued(t *testing.T) {
	j := recentJob("q", time.Minute, hpcv1alpha1.PhaseRunning)
	j.Status.PendingReason = "Resources"
	j.Status.SiteObservation = &hpcv1alpha1.SiteObservation{JobID: "1", State: "PENDING", PendingReason: "Priority"}
	j.Status.Run = &hpcv1alpha1.RunStatus{SiteJobID: "1", State: "PENDING"}
	if got := recentJobOf(&j, time.Now()).PendingReason; got != "Priority" {
		t.Fatalf("queued job: got %q, want the controller's reading", got)
	}
	j.Status.Run.State = siteStateRunning
	if got := recentJobOf(&j, time.Now()).PendingReason; got != "" {
		t.Fatalf("running job: got %q, want none", got)
	}
}

func TestRecentJobShouldDropTheSitesNeverStartedSentinel(t *testing.T) {
	never := metav1.NewTime(time.Unix(4294967294, 0)) // Slurm NO_VAL: year 2106
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	j := hpcv1alpha1.HPCArgoWorkflowJob{}
	j.Status.Run = &hpcv1alpha1.RunStatus{StartTime: &never, EndTime: &never}
	if rj := recentJobOf(&j, time.Now()); rj.StartTime != nil || rj.EndTime != nil {
		t.Fatalf("sentinel times copied: %v %v", rj.StartTime, rj.EndTime)
	}
	j.Status.Run = &hpcv1alpha1.RunStatus{StartTime: &start}
	if rj := recentJobOf(&j, time.Now()); rj.StartTime == nil || !rj.StartTime.Equal(&start) {
		t.Fatalf("a real start was dropped: %v", rj.StartTime)
	}
}

// AC-23: a job queued behind the site's monthly account limit shows SiteLimit as its reason and pending reason.
func TestRecentJobOfSiteLimitPendingReason(t *testing.T) {
	cases := []struct {
		name, slurm, wantPending, wantReason string
	}{
		{"should_map_assocgrpgresminutes_to_site_limit", "AssocGrpGRESMinutes", hpcv1alpha1.ReasonSiteLimit, hpcv1alpha1.ReasonSiteLimit},
		{"should_map_assocgrpbillingminutes_to_site_limit", "AssocGrpBillingMinutes", hpcv1alpha1.ReasonSiteLimit, hpcv1alpha1.ReasonSiteLimit},
		{"should_keep_the_queue_reasons_that_are_not_the_monthly_limit", "Priority", "Priority", "Submitted"},
		{"should_not_map_the_running_minutes_limit", "AssocGrpGRESRunMinutes", "AssocGrpGRESRunMinutes", "Submitted"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := recentJob("q", time.Minute, hpcv1alpha1.PhaseRunning)
			j.Status.Reason = "Submitted"
			j.Status.SiteObservation = &hpcv1alpha1.SiteObservation{JobID: "1", State: "PENDING", PendingReason: c.slurm}
			j.Status.Run = &hpcv1alpha1.RunStatus{SiteJobID: "1", State: "PENDING"}
			rj := recentJobOf(&j, time.Now())
			if rj.PendingReason != c.wantPending || rj.Reason != c.wantReason {
				t.Fatalf("got pending %q reason %q, want %q / %q", rj.PendingReason, rj.Reason, c.wantPending, c.wantReason)
			}
		})
	}
}

func TestRecentJobsKeepActiveAndLastSevenDays(t *testing.T) {
	r := recentReconciler(t, projectNS("p-train", "p"))
	day := 24 * time.Hour
	endedAgo := func(name string, ago time.Duration) hpcv1alpha1.HPCArgoWorkflowJob {
		j := recentJob(name, ago+time.Hour, hpcv1alpha1.PhaseSucceeded)
		end := metav1.NewTime(recentT0.Add(-ago))
		j.Status.Run = &hpcv1alpha1.RunStatus{State: "COMPLETED", EndTime: &end}
		return j
	}
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		recentJob("active-ancient", 30*day, hpcv1alpha1.PhaseRunning), // active is always kept
		endedAgo("ended-6d", 6*day),
		endedAgo("ended-8d", 8*day),
		recentJob("refused-8d", 8*day, hpcv1alpha1.PhaseRefused), // no run: creation time decides
	}
	got, err := r.recentJobsOf(context.Background(), recentSite(), jobs)
	if err != nil {
		t.Fatal(err)
	}
	if want := "[active-ancient ended-6d]"; fmt.Sprint(names(got)) != want {
		t.Fatalf("got %v, want %s", names(got), want)
	}
}

func TestRecentJobsCapKeepsActiveBeforeEnded(t *testing.T) {
	r := recentReconciler(t, projectNS("p-train", "p"))
	jobs := make([]hpcv1alpha1.HPCArgoWorkflowJob, 0, hpcv1alpha1.RecentJobsMax+1)
	for i := range hpcv1alpha1.RecentJobsMax {
		jobs = append(jobs, recentJob(fmt.Sprintf("done%03d", i), time.Duration(i+1)*time.Minute, hpcv1alpha1.PhaseSucceeded))
	}
	jobs = append(jobs, recentJob("active-old", 6*time.Hour, hpcv1alpha1.PhaseRunning))
	got, _ := r.recentJobsOf(context.Background(), recentSite(), jobs)
	if len(got) != hpcv1alpha1.RecentJobsMax || got[0].Name != "active-old" {
		t.Fatalf("got %d entries, first %q", len(got), got[0].Name)
	}
}

func TestRecentJobOfCopiesAndTruncatesMessage(t *testing.T) {
	cases := []struct {
		name, in  string
		wantRunes int
	}{
		{"should_copy_short_message_unchanged", "Idle for 20 minutes", 19},
		{"should_truncate_long_message_to_512_characters", strings.Repeat("x", 2000), 512},
		{"should_truncate_on_character_boundary", strings.Repeat("\u00e9", 600), 512},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := recentJob("m", time.Hour, hpcv1alpha1.PhaseFailed)
			j.Status.Message = c.in
			got := recentJobOf(&j, time.Now()).Message
			if n := len([]rune(got)); n != c.wantRunes {
				t.Fatalf("got %d characters, want %d", n, c.wantRunes)
			}
		})
	}
}

// worstCaseRecentJob has every bounded field at its schema maximum, with 2-byte characters in the
// message (the longest JSON a 512-character string can take without escapes).
func worstCaseRecentJob(i int) hpcv1alpha1.RecentJob {
	s := func(n int) string { return strings.Repeat("x", n) }
	ts := metav1.NewTime(recentT0)
	return hpcv1alpha1.RecentJob{
		Name: fmt.Sprintf("%s%03d", s(250), i), Namespace: s(63), Step: s(253), Workflow: s(253),
		Phase: hpcv1alpha1.PhaseWaiting, Reason: s(63), Message: strings.Repeat("\u00e9", hpcv1alpha1.RecentJobMessageMax),
		SiteState: s(63), PendingReason: s(63), QOS: s(63), Reservation: s(63), RefusedQOS: s(63),
		TimeLimitMinutes: 2147483647, LimitSource: hpcv1alpha1.PlacementTimeoutRetry, LimitSamples: 2147483647,
		IdleTimeoutMinutes: 2147483647, Attempt: 2147483647, SiteJobID: s(63), HostSiteJobID: s(63),
		CreatedAt: ts, StartTime: &ts, EndTime: &ts,
		BookedNodeMinutes: 9223372036854775807, SettledNodeMinutes: 9223372036854775807, QueueReplacedCount: 2147483647,
	}
}

func TestRecentJobsStatusSizeStaysUnderBudget(t *testing.T) {
	// A typical entry: names of a 63-character label, a one-sentence message.
	typical := func(i int) hpcv1alpha1.HPCArgoWorkflowJob {
		j := recentJob(fmt.Sprintf("%s-%03d", strings.Repeat("x", 55), i), time.Duration(i+1)*time.Minute, hpcv1alpha1.PhaseFailed)
		j.Spec.StepName = strings.Repeat("s", 63)
		j.Spec.Workflow = &hpcv1alpha1.WorkflowRef{Name: strings.Repeat("w", 63), UID: "u"}
		j.Status.Message = strings.Repeat("m", 200)
		return j
	}
	cases := []struct {
		name      string
		entry     func(int) hpcv1alpha1.RecentJob
		wantCount int // 0: only the byte bound is asserted
	}{
		{"should_keep_300_typical_entries_under_256KiB", func(i int) hpcv1alpha1.RecentJob {
			j := typical(i)
			return recentJobOf(&j, recentT0)
		}, hpcv1alpha1.RecentJobsMax},
		{"should_cut_worst_case_entries_to_fit_256KiB", worstCaseRecentJob, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			list := make([]hpcv1alpha1.RecentJob, 0, hpcv1alpha1.RecentJobsMax)
			for i := range hpcv1alpha1.RecentJobsMax {
				list = append(list, c.entry(i))
			}
			raw, _ := json.Marshal(list)
			t.Logf("uncapped: %d bytes per entry, %d bytes for %d entries", len(raw)/len(list), len(raw), len(list))
			got := capRecentBytes(list)
			capped, _ := json.Marshal(got)
			t.Logf("capped: %d entries, %d bytes", len(got), len(capped))
			if len(capped) > hpcv1alpha1.RecentJobsMaxBytes {
				t.Fatalf("%d bytes over the %d budget", len(capped), hpcv1alpha1.RecentJobsMaxBytes)
			}
			if c.wantCount != 0 && len(got) != c.wantCount {
				t.Fatalf("got %d entries, want %d", len(got), c.wantCount)
			}
		})
	}
}

func resumeJob(name, step, image string, ev *hpcv1alpha1.ResumeEvidence) hpcv1alpha1.HPCArgoWorkflowJob {
	j := recentJob(name, time.Hour, hpcv1alpha1.PhaseSucceeded)
	j.Spec.StepName, j.Spec.Image = step, image
	j.Status.ResumeEvidence = ev
	return j
}

func TestResumableStepsPerAppAndStep(t *testing.T) {
	at := func(h int) metav1.Time { return metav1.NewTime(recentT0.Add(time.Duration(h) * time.Hour)) }
	ok := &hpcv1alpha1.ResumeEvidence{Verified: true, At: at(1), Reason: "a continuation loaded its checkpoint and got past it"}
	bad := &hpcv1alpha1.ResumeEvidence{Verified: false, At: at(2), Reason: "NoProgress: the continuation saved no checkpoint before it ended"}
	site, projectNS := recentSite(), map[string]bool{"p-train": true}
	jobs := []hpcv1alpha1.HPCArgoWorkflowJob{
		resumeJob("a1", "fit", "reg.io/p/app-a:fit-1", ok),
		resumeJob("a2", "fit", "reg.io/p/app-a:fit-2", bad), // the later evidence decides
		resumeJob("b1", "fit", "reg.io/p/app-b:fit-1", ok),
		resumeJob("none", "eval", "reg.io/p/app-a:eval-1", nil), // no evidence: no entry
		resumeJob("noimg", "fit", "", ok),                       // no app: no entry
		resumeJob("elsewhere", "fit", "reg.io/p/app-c:fit-1", ok),
	}
	jobs[5].Namespace = "other-project-ns"
	got := resumableStepsOf(site, jobs, projectNS)
	if len(got) != 2 {
		t.Fatalf("want 2 entries (app-a/fit, app-b/fit), got %+v", got)
	}
	if got[0].App != "app-a" || got[0].Step != "fit" || got[0].Verified || got[0].Reason[:10] != "NoProgress" {
		t.Errorf("app-a/fit must carry the later NoProgress evidence, got %+v", got[0])
	}
	if got[1].App != "app-b" || !got[1].Verified || got[1].Since == nil {
		t.Errorf("app-b/fit must be verified with a time, got %+v", got[1])
	}
	if resumableStepsOf(site, nil, projectNS) != nil {
		t.Error("no jobs must give nil")
	}
}
