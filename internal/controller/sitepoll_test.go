package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
)

type fakeReader struct {
	calls [][]string
	sites []sitestate.Site
	reply map[string]sitestate.Job
	err   error
}

func (f *fakeReader) States(_ context.Context, s sitestate.Site, _ sitestate.Waiter, ids []string) (map[string]sitestate.Job, error) {
	f.calls = append(f.calls, ids)
	f.sites = append(f.sites, s)
	if f.err != nil {
		return nil, f.err
	}
	return f.reply, nil
}

type pollRig struct {
	c      client.Client
	reader *fakeReader
	poller *SitePoller
	now    time.Time
}

func newPollRig(t *testing.T, objs ...client.Object) *pollRig {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hpcv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
		WithStatusSubresource(&hpcv1alpha1.HPCArgoWorkflowJob{}).Build()
	r := &pollRig{c: c, reader: &fakeReader{reply: map[string]sitestate.Job{}}, now: time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)}
	r.poller = &SitePoller{Client: c, Secrets: c, Reader: r.reader, Interval: 30 * time.Second,
		BucketRate: 100, BucketBurst: 100, Now: func() time.Time { return r.now }}
	return r
}

func pollSite(ns, endpoint string) []client.Object {
	return []client.Object{
		&hpcv1alpha1.HPCSite{ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: ns},
			Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Profile: hpcv1alpha1.SiteProfile{
				Transport: hpcv1alpha1.TransportSlurmREST, Endpoint: endpoint, RESTVersion: "v0.0.44",
				Classes: []hpcv1alpha1.SiteClass{{Name: "c", Partition: "gpu", Tier: hpcv1alpha1.TierGPU, MaxWallTimeMinutes: 60}}},
				Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "a",
					SSHKeySecretRef: hpcv1alpha1.SecretKeyRef{Name: "k", Key: "i"}}}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "meluxina-jwt", Namespace: ns},
			Data: map[string][]byte{"token": []byte("jwt-" + ns)}},
	}
}

func pollJob(ns, name, siteNS, id string, mut ...func(*hpcv1alpha1.HPCArgoWorkflowJob)) *hpcv1alpha1.HPCArgoWorkflowJob {
	j := &hpcv1alpha1.HPCArgoWorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Status: hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseRunning, Attempt: 1,
			Site: &hpcv1alpha1.SiteRef{Name: "meluxina", Namespace: siteNS},
			Run:  &hpcv1alpha1.RunStatus{Attempt: 1, SiteJobID: id, State: "PENDING"}}}
	for _, m := range mut {
		m(j)
	}
	return j
}

func (r *pollRig) job(t *testing.T, name string) hpcv1alpha1.HPCArgoWorkflowJob {
	t.Helper()
	var j hpcv1alpha1.HPCArgoWorkflowJob
	if err := r.c.Get(context.Background(), client.ObjectKey{Namespace: "env", Name: name}, &j); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestShouldPollOnceForManyJobsOfOneSiteAndWriteEachObservation(t *testing.T) {
	objs := pollSite("hpc", "https://s.example")
	reply := map[string]sitestate.Job{}
	for i := 1; i <= 40; i++ {
		objs = append(objs, pollJob("env", fmt.Sprintf("j%d", i), "hpc", fmt.Sprint(100+i)))
		reply[fmt.Sprint(100+i)] = sitestate.Job{ID: fmt.Sprint(100 + i), State: []string{"PENDING"}, Reason: "Priority"}
	}
	r := newPollRig(t, objs...)
	r.reader.reply = reply
	r.poller.PollOnce(context.Background())

	if len(r.reader.calls) != 1 || len(r.reader.calls[0]) != 40 {
		t.Fatalf("calls %d (first has %d ids), want one call with all 40", len(r.reader.calls), len(r.reader.calls[0]))
	}
	if s := r.reader.sites[0]; s.Token != "jwt-hpc" || s.User != "u1" || s.Endpoint != "https://s.example" {
		t.Fatalf("site call: %+v", s)
	}
	o := r.job(t, "j7").Status.SiteObservation
	if o == nil || o.JobID != "107" || o.State != "PENDING" || o.PendingReason != "Priority" || o.IntervalSeconds != 30 ||
		!o.ObservedAt.Time.Equal(r.now) {
		t.Fatalf("observation: %+v", o)
	}
}

func TestShouldPollEachSiteOnceWithItsOwnTokenAndOnlyItsOwnJobs(t *testing.T) {
	objs := append(pollSite("hpc-a", "https://a.example"), pollSite("hpc-b", "https://b.example")...)
	objs = append(objs, pollJob("ea", "a1", "hpc-a", "1"), pollJob("ea", "a2", "hpc-a", "2"),
		pollJob("eb", "b1", "hpc-b", "3"))
	r := newPollRig(t, objs...)
	r.poller.PollOnce(context.Background())
	if len(r.reader.calls) != 2 {
		t.Fatalf("calls: %v", r.reader.calls)
	}
	for i, s := range r.reader.sites {
		want := map[string]int{"jwt-hpc-a": 2, "jwt-hpc-b": 1}[s.Token]
		if len(r.reader.calls[i]) != want {
			t.Fatalf("site %s got ids %v", s.Token, r.reader.calls[i])
		}
	}
}

func TestShouldIgnoreTerminalJobsAndJobsWithoutAKnownLiveSiteJob(t *testing.T) {
	objs := pollSite("hpc", "https://s.example")
	objs = append(objs,
		pollJob("env", "done", "hpc", "1", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.Phase = hpcv1alpha1.PhaseSucceeded }),
		pollJob("env", "reported-end", "hpc", "2", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.Run.Terminal = true }),
		pollJob("env", "no-id", "hpc", "", nil2),
		pollJob("env", "old-attempt", "hpc", "4", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.Attempt = 2 }),
		pollJob("env", "no-site", "hpc", "5", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.Site = nil }),
		pollJob("env", "live", "hpc", "6"))
	r := newPollRig(t, objs...)
	r.poller.PollOnce(context.Background())
	if len(r.reader.calls) != 1 || len(r.reader.calls[0]) != 1 || r.reader.calls[0][0] != "6" {
		t.Fatalf("polled %v, want only job 6", r.reader.calls)
	}
	if r.job(t, "done").Status.SiteObservation != nil {
		t.Fatal("a terminal job got an observation")
	}
}

func nil2(*hpcv1alpha1.HPCArgoWorkflowJob) {}

func TestShouldLeaveEveryStatusAloneWhenTheSiteFails(t *testing.T) {
	old := &hpcv1alpha1.SiteObservation{JobID: "1", State: "PENDING", ObservedAt: metav1.NewTime(time.Unix(1, 0))}
	objs := append(pollSite("hpc", "https://s.example"),
		pollJob("env", "j", "hpc", "1", func(j *hpcv1alpha1.HPCArgoWorkflowJob) { j.Status.SiteObservation = old }))
	r := newPollRig(t, objs...)
	r.reader.err = errors.New("jobs/state: HTTP 503")
	r.poller.PollOnce(context.Background())
	got := r.job(t, "j").Status.SiteObservation
	if got == nil || !got.ObservedAt.Time.Equal(time.Unix(1, 0)) || got.State != "PENDING" {
		t.Fatalf("a failed poll changed the observation: %+v", got)
	}
}

func TestShouldRecordAJobTheSiteNoLongerListsAsEmptyStateSoTheRunnerAsksAccounting(t *testing.T) {
	objs := append(pollSite("hpc", "https://s.example"), pollJob("env", "j", "hpc", "1"))
	r := newPollRig(t, objs...) // the reply is empty
	r.poller.PollOnce(context.Background())
	o := r.job(t, "j").Status.SiteObservation
	if o == nil || o.State != "" || o.JobID != "1" {
		t.Fatalf("%+v", o)
	}
}

func TestShouldNotCallTheSiteWithoutAToken(t *testing.T) {
	objs := []client.Object{pollSite("hpc", "https://s.example")[0], pollJob("env", "j", "hpc", "1")}
	r := newPollRig(t, objs...)
	r.poller.PollOnce(context.Background())
	if len(r.reader.calls) != 0 {
		t.Fatalf("called the site without a token: %v", r.reader.calls)
	}
}

func TestShouldDropAnObservationOfASiteJobTheRunnerHasReplaced(t *testing.T) {
	objs := append(pollSite("hpc", "https://s.example"), pollJob("env", "j", "hpc", "1"))
	r := newPollRig(t, objs...)
	obs := &hpcv1alpha1.SiteObservation{JobID: "OLD", State: siteStateRunning, ObservedAt: metav1.NewTime(r.now)}
	if err := r.poller.write(context.Background(), client.ObjectKey{Namespace: "env", Name: "j"}, obs); err != nil {
		t.Fatal(err)
	}
	if r.job(t, "j").Status.SiteObservation != nil {
		t.Fatal("an observation of another site job was written")
	}
}

func TestShouldShareOneBucketPerSiteProfileAcrossHPCSites(t *testing.T) {
	p := &SitePoller{BucketRate: 1, BucketBurst: 1}
	mk := func(site, ep string) *hpcv1alpha1.HPCSite {
		return &hpcv1alpha1.HPCSite{Spec: hpcv1alpha1.HPCSiteSpec{Site: site, Profile: hpcv1alpha1.SiteProfile{Endpoint: ep}}}
	}
	a, b, c := p.bucket(mk("meluxina", "https://x")), p.bucket(mk("meluxina", "https://x")), p.bucket(mk("other", "https://y"))
	if a != b || a == c {
		t.Fatal("buckets are per site profile, shared by every HPCSite of it")
	}
}

// AC-15: a second site of a project reads its own key of the shared token Secret.
func TestShouldReadTheSecondSitesTokenFromItsPrefixedKey(t *testing.T) {
	objs := pollSite("hpc", "https://s.example")
	objs[0].(*hpcv1alpha1.HPCSite).Spec.Identity.CredentialKeyPrefix = "beta."
	secret := objs[1].(*corev1.Secret)
	secret.Data = map[string][]byte{"token": []byte("first-site-token"), "beta.token": []byte("beta-token")}
	r := newPollRig(t, objs...)
	var site hpcv1alpha1.HPCSite
	if err := r.c.Get(context.Background(), client.ObjectKey{Namespace: "hpc", Name: "meluxina"}, &site); err != nil {
		t.Fatal(err)
	}
	got, err := r.poller.credentials(context.Background(), &site)
	if err != nil || got.Token != "beta-token" {
		t.Fatalf("token %q err %v, want the beta.token value", got.Token, err)
	}
	site.Spec.Identity.CredentialKeyPrefix = "gone."
	if _, err := r.poller.credentials(context.Background(), &site); err == nil {
		t.Fatal("a site whose prefixed key is missing must not fall back to another site's token")
	}
}
