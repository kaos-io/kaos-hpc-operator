package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/ledger"
)

// PRD kaos#1911, cleanup. The budget ledger is derived from the job objects and a budget period is a calendar month, so a job
// object is deleted only after its retention AND after the period its last attempt ended in has closed. Nothing is stored and
// nothing holds a finished job.

var expireNow = time.Date(2026, 12, 20, 12, 0, 0, 0, time.UTC)

func ended(at time.Time, phase hpcv1alpha1.Phase) *hpcv1alpha1.HPCArgoWorkflowJob {
	t := metav1.NewTime(at)
	return &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "ml-p", UID: "u1", CreationTimestamp: metav1.NewTime(at.Add(-time.Hour))},
		Status: hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: phase,
			Attempts: []hpcv1alpha1.AttemptRecord{{Attempt: 1, EndTime: t, Outcome: string(phase), SettledNodeMinutes: 10, Tier: hpcv1alpha1.TierGPU}}},
	}
}

func TestShouldExpireAFinishedJobOnlyAfterTheRetentionAndOutsideTheRunningBudgetPeriod(t *testing.T) {
	const month = 30 * 24 * time.Hour
	from := ledger.PeriodStart(expireNow)
	cases := []struct {
		name string
		job  func() *hpcv1alpha1.HPCArgoWorkflowJob
		ret  time.Duration
		want bool
	}{
		{"ended 40 days ago, in an earlier month", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			return ended(expireNow.AddDate(0, 0, -40), hpcv1alpha1.PhaseFailed)
		}, month, true},
		{"ended 15 days ago, in an earlier month, not old enough", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			return ended(expireNow.AddDate(0, 0, -29), hpcv1alpha1.PhaseFailed)
		}, month, false},
		{"old enough but it ended in the running month: its spend still counts", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			return ended(time.Date(2026, 12, 2, 0, 0, 0, 0, time.UTC), hpcv1alpha1.PhaseSucceeded)
		}, 7 * 24 * time.Hour, false},
		{"retention 0 keeps everything", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			return ended(expireNow.AddDate(0, 0, -400), hpcv1alpha1.PhaseFailed)
		}, 0, false},
		{"not finished", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			return ended(expireNow.AddDate(0, 0, -400), hpcv1alpha1.PhaseRunning)
		}, month, false},
		{"a sweep is still waiting for the site", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := ended(expireNow.AddDate(0, 0, -400), hpcv1alpha1.PhaseFailed)
			j.Status.Sweep = &hpcv1alpha1.SweepStatus{}
			return j
		}, month, false},
		{"an open host attempt", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := ended(expireNow.AddDate(0, 0, -400), hpcv1alpha1.PhaseSucceeded)
			j.Status.Booking = &hpcv1alpha1.Booking{}
			j.Status.Attempts[0].HostJobOpen = true
			return j
		}, month, false},
		{"refused long ago, never ran: no spend, its creation time counts for the retention", func() *hpcv1alpha1.HPCArgoWorkflowJob {
			j := ended(expireNow, hpcv1alpha1.PhaseRefused)
			j.Status.Attempts = nil
			j.CreationTimestamp = metav1.NewTime(expireNow.AddDate(0, 0, -90))
			return j
		}, month, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, _ := expiry(c.job(), expireNow, c.ret, from); got != c.want {
				t.Fatalf("expired = %v, want %v", got, c.want)
			}
		})
	}
}

// The arithmetic behind --job-retention 35 days: a job of the 1st of a 31-day month, or of its last second, is deleted only
// after the month has closed; with 31 days (the old default was 30) a job of the 1st would be collected inside its month, but
// the period guard still holds it.
func TestShouldNeverDeleteAJobInsideTheMonthItEndedInWhateverTheRetention(t *testing.T) {
	first := ended(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC), hpcv1alpha1.PhaseSucceeded)
	last := ended(time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC), hpcv1alpha1.PhaseSucceeded)
	for _, ret := range []time.Duration{7 * 24 * time.Hour, 30 * 24 * time.Hour, 31 * 24 * time.Hour, DefaultJobRetention} {
		for _, at := range []time.Time{time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)} {
			from := ledger.PeriodStart(at)
			for _, j := range []*hpcv1alpha1.HPCArgoWorkflowJob{first, last} {
				if ok, _ := expiry(j, at, ret, from); ok {
					t.Fatalf("retention %v: a job that ended %v was deletable at %v, inside December", ret, jobEndedAt(j), at)
				}
			}
		}
		jan := time.Date(2027, 1, 1, 0, 0, 1, 0, time.UTC)
		if ok, _ := expiry(last, jan, ret, ledger.PeriodStart(jan)); ok != (jan.Sub(jobEndedAt(last)) >= ret) {
			t.Fatalf("retention %v: the last second of December must be deletable in January exactly when the retention has passed", ret)
		}
	}
	if DefaultJobRetention != 35*24*time.Hour || DefaultJobRetention < 31*24*time.Hour {
		t.Fatal("the default retention must outlive the longest month")
	}
}

func TestShouldSayHowLongToWaitForAJobThatIsNotYetOldEnough(t *testing.T) {
	ok, wait := expiry(ended(expireNow.AddDate(0, 0, -20), hpcv1alpha1.PhaseFailed), expireNow, 30*24*time.Hour, ledger.PeriodStart(expireNow))
	if ok || wait < 9*24*time.Hour || wait > 10*24*time.Hour+time.Hour {
		t.Fatalf("expired %v, wait %v: want not yet, about 10 days", ok, wait)
	}
}

func newExpireFixture(t *testing.T, jobs ...*hpcv1alpha1.HPCArgoWorkflowJob) (client.Client, *HPCArgoWorkflowJobReconciler) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	site := &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "site", Namespace: "p-hpc", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "p"}},
		Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina",
			Profile: hpcv1alpha1.SiteProfile{Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: "https://s.example", RESTVersion: "v0.0.44",
				Classes: []hpcv1alpha1.SiteClass{{Name: "gpu", Partition: "gpu", Tier: hpcv1alpha1.TierGPU, GPUs: 1, MaxWallTimeMinutes: 600}}},
			Identity: hpcv1alpha1.ProjectIdentity{User: "u", Account: "a", SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "i"}},
			Budget:   hpcv1alpha1.Budget{GPUNodeHours: 100}},
	}
	objs := []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ml-p", Labels: map[string]string{hpcv1alpha1.ProjectLabel: "p"}}}, site}
	for _, j := range jobs {
		j.Spec.Class = "gpu"
		objs = append(objs, j)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&hpcv1alpha1.HPCArgoWorkflowJob{}).Build()
	return c, &HPCArgoWorkflowJobReconciler{Client: c, Scheme: s, Now: func() time.Time { return expireNow }, JobRetention: DefaultJobRetention}
}

func TestShouldDeleteAnExpiredJobInReconcileAndKeepYoungAndInPeriodOnes(t *testing.T) {
	old := ended(expireNow.AddDate(0, 0, -60), hpcv1alpha1.PhaseFailed)
	young := ended(expireNow.AddDate(0, 0, -3), hpcv1alpha1.PhaseFailed)
	inPeriod := ended(time.Date(2026, 12, 2, 0, 0, 0, 0, time.UTC), hpcv1alpha1.PhaseSucceeded)
	old.Name, young.Name, inPeriod.Name = "old", "young", "inperiod"
	old.UID, young.UID, inPeriod.UID = "uo", "uy", "ui"
	c, r := newExpireFixture(t, old, young, inPeriod)
	r.JobRetention = 7 * 24 * time.Hour // shorter than the month on purpose: the period guard must still hold
	for _, j := range []*hpcv1alpha1.HPCArgoWorkflowJob{old, young, inPeriod} {
		var cur hpcv1alpha1.HPCArgoWorkflowJob
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(j), &cur); err != nil {
			t.Fatal(err)
		}
		res, done, err := r.expireFinished(context.Background(), &cur)
		if err != nil {
			t.Fatal(err)
		}
		switch j.Name {
		case "old":
			if !done {
				t.Fatal("the expired job was not deleted")
			}
		case "young":
			if done || res.RequeueAfter <= 0 {
				t.Fatalf("a young job stays and is looked at again later: done=%v requeue=%v", done, res.RequeueAfter)
			}
		case "inperiod":
			if done || res.RequeueAfter != 0 {
				t.Fatalf("a job inside its period is skipped, with no requeue to the end of the month: done=%v requeue=%v", done, res.RequeueAfter)
			}
		}
	}
	for name, want := range map[string]bool{"old": false, "young": true, "inperiod": true} {
		var cur hpcv1alpha1.HPCArgoWorkflowJob
		err := c.Get(context.Background(), client.ObjectKey{Namespace: "ml-p", Name: name}, &cur)
		if (err == nil) != want {
			t.Fatalf("%s: exists=%v, want %v", name, err == nil, want)
		}
	}
}

func TestShouldHoldNoFinalizerOnAFinishedJobAndLetItGoWithNoController(t *testing.T) {
	j := ended(expireNow.AddDate(0, 0, -3), hpcv1alpha1.PhaseSucceeded)
	c, r := newExpireFixture(t, j)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)}); err != nil {
		t.Fatal(err)
	}
	var cur hpcv1alpha1.HPCArgoWorkflowJob
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(j), &cur); err != nil || len(cur.Finalizers) != 0 {
		t.Fatalf("a finished job holds no finalizer: %v %v", err, cur.Finalizers)
	}
	// helm uninstall, a namespace delete, Workflow garbage collection: nobody reconciles, the object goes at once
	if err := c.Delete(context.Background(), &cur); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(j), &cur); err == nil {
		t.Fatal("the finished job is still there after its deletion")
	}
}

func TestShouldStripTheBudgetFinalizerOfAnEarlierBuild(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		j := ended(expireNow.AddDate(0, 0, -3), hpcv1alpha1.PhaseSucceeded)
		j.Finalizers = []string{legacyBudgetFinalizer}
		c, r := newExpireFixture(t, j)
		if deleting {
			var cur hpcv1alpha1.HPCArgoWorkflowJob
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(j), &cur); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(context.Background(), &cur); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(j)}); err != nil {
			t.Fatal(err)
		}
		var cur hpcv1alpha1.HPCArgoWorkflowJob
		err := c.Get(context.Background(), client.ObjectKeyFromObject(j), &cur)
		if deleting && err == nil {
			t.Fatal("a deleted job held only by the old finalizer must go")
		}
		if !deleting && (err != nil || len(cur.Finalizers) != 0) {
			t.Fatalf("the old finalizer must be removed from a live job: %v %v", err, cur.Finalizers)
		}
	}
}
