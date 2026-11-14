package runnerjob

import (
	"testing"
	"time"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// Live lessons of the old in-pod waiter (PRD-HPC-1461 AC-11); docs/live-lessons.md maps all of them.

// ssh refuses a private key that group or world can read. The old waiter wrote the stager key with 0400 itself;
// the key now reaches the runner as a Secret volume, so the mode is the volume's. Live: the six runner Jobs
// that staged on 2026-11-06 authenticated with this key. The key is optional (no key: staging falls back to
// the job), the mode is not. fsGroup makes the file readable by the runner user without opening it to others:
// ssh checks the mode only for a file its own user owns, and the Secret file is owned by root.
func TestShouldMountTheStagerKeyReadOnlyWithMode0400WhetherOrNotItExists(t *testing.T) {
	job, site := fixtures()
	rj, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	p := rj.Spec.Template.Spec
	var vol *int32
	for _, v := range p.Volumes {
		if v.Name == SecretStaging {
			if v.Secret == nil || v.Secret.DefaultMode == nil || v.Secret.Optional == nil || !*v.Secret.Optional {
				t.Fatalf("staging volume %+v: want an optional Secret with an explicit mode", v)
			}
			vol = v.Secret.DefaultMode
		}
	}
	if vol == nil || *vol != 0o400 {
		t.Fatalf("stager key mode %v, want 0400: ssh refuses a group- or world-readable key", vol)
	}
	var mount bool
	for _, m := range p.Containers[0].VolumeMounts {
		if m.Name == SecretStaging {
			mount = m.ReadOnly && m.MountPath == MountStaging
		}
	}
	if !mount {
		t.Errorf("the stager key must be mounted read-only at %s", MountStaging)
	}
}

// The old waiter's deadline covered every continuation (commit e56383a): a step that checkpoints and
// continues must not run out of time on its third job. Each continuation is a NEW runner Job with its own
// deadline, so the lesson is that every attempt gets the full allowance (never a remainder of the first) and
// that the Job names differ, so the later Job never inherits the earlier one's clock.
func TestShouldGiveEveryContinuationAttemptTheFullDeadlineOfItsOwn(t *testing.T) {
	const want = (120 + 480 + 30 + stagingAllowanceMinutes) * 60
	job, site := fixtures()
	seen := map[string]bool{}
	for attempt := int32(1); attempt <= 4; attempt++ {
		job.Status.Resume = nil
		if attempt > 1 {
			job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: "ckpt", FromAttempt: attempt - 1}
		}
		rj, err := Build(Config{Image: "kaos/op:1"}, job, site, attempt)
		if err != nil {
			t.Fatal(err)
		}
		if got := *rj.Spec.ActiveDeadlineSeconds; got != want {
			t.Errorf("attempt %d deadline %d, want the full %d", attempt, got, want)
		}
		if seen[rj.Name] {
			t.Errorf("attempt %d reuses Job %s: it would inherit the earlier deadline", attempt, rj.Name)
		}
		seen[rj.Name] = true
	}
}

// A site outage extends the deadline by the outage bound instead of eating the site job's time (AC-30); the
// extension applies to every attempt of the run alike.
func TestShouldExtendEachContinuationDeadlineByTheOutageBound(t *testing.T) {
	job, site := fixtures()
	job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: "ckpt", FromAttempt: 1}
	rj, err := Build(Config{Image: "kaos/op:1", SiteWaitMax: 2 * time.Hour}, job, site, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := *rj.Spec.ActiveDeadlineSeconds, int64((120+480+30+stagingAllowanceMinutes)*60+2*3600); got != want {
		t.Errorf("deadline %d, want %d", got, want)
	}
}

func TestShouldAllowAJobWithDatasetsToStageTheLongestStagingOnTopOfItsDeadline(t *testing.T) {
	job, site := fixtures()
	with, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Inputs = nil
	without, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := *with.Spec.ActiveDeadlineSeconds - *without.Spec.ActiveDeadlineSeconds; got != 72*3600 {
		t.Errorf("a job with datasets gets %d s more than one without, want the 72 hours the runner may stage for", got)
	}
}
