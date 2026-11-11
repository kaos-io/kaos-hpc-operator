"""The runner's attempt end to end against fakes: Slurm REST, the Kubernetes API, lakeFS, the stager.

Ported from the waiter's behaviour (submit-or-adopt by key, pull / wallet / lost-host resubmits, failure comment,
checkpoint check, on_term cancel) and the phase-1 contract: one attempt per runner, report to the job's status,
re-attach by key, never submit a continuation.
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

import helpers
import pytest

from kaos_hpc_runner import logs as logsmod
from kaos_hpc_runner import reporter as rep
from kaos_hpc_runner import runner as runmod
from kaos_hpc_runner import slurm as slurmmod
from kaos_hpc_runner import staging as staging_mod

WF = "0b7f6c1e-1111-2222-3333-444455556666"
NAME = "kaos-%s-model-training" % WF
CID = "ab" * 32
REF = "scratch://kaos-ckpt/%s/model-training/000002.tar#%s" % (WF, CID)


def acc(state, rc=0, comment="", start=1762300800, end=1762304400):
    """One slurmdbd record."""
    return {"name": NAME, "state": {"current": state.split(",")}, "comment": {"job": comment},
            "exit_code": {"return_code": {"number": rc}}, "time": {"start": start, "end": end}}


class FakeSlurm:
    """slurmrestd + slurmdbd. jobs: id -> list of accounting records, one per poll (the last repeats)."""

    def __init__(self, jobs=None, active=None, live=None):
        self.jobs = {k: list(v) for k, v in (jobs or {}).items()}
        self.active, self.live_view = active, live or {}
        self.submitted, self.cancelled, self.next_id = [], [], 5000
        self.accounting_calls = []

    def find_active(self):
        return self.active

    # slurmdbd's search by job NAME: the real query and the real filtering, over the jobs listed in `finished`
    # (ids whose accounting record this fake shows to a name search; none by default, as before a submit).
    finished = ()
    cfg = None
    find_finished = slurmmod.Slurm.find_finished

    def get(self, url):
        assert "/jobs?job_name=" in url, url
        return {"jobs": [dict(self.jobs[str(i)][-1], job_id=int(i)) for i in self.finished]}

    def accounting(self, jid):
        self.accounting_calls.append(jid)
        recs = self.jobs.get(str(jid))
        if not recs:
            raise urllib.error.HTTPError("x", 404, "no job", {}, None)
        return recs.pop(0) if len(recs) > 1 else recs[0]

    def live(self, jid):
        v = self.live_view.get(str(jid), {})
        if isinstance(v, Exception):
            raise v
        return v

    def cancel(self, jid):
        self.cancelled.append(str(jid))
        return True

    def submit(self, body):
        self.next_id += 1
        self.submitted.append(body)
        return {"job_id": self.next_id, "errors": []}


class Net:
    """urllib.request.urlopen: the metadata server is absent, lakeFS has the staged outputs and checkpoint."""

    def __init__(self):
        self.objects, self.commits, self.deleted = {}, set(), []

    def __call__(self, req, timeout=0, **kw):
        import io
        url = req.full_url if hasattr(req, "full_url") else req
        if "metadata.google.internal" in url or "/v2/" in url:
            raise OSError("no metadata server here")
        if "/branches/" in url and getattr(req, "method", "GET") == "DELETE":
            self.deleted.append(url)
            return io.BytesIO(b"")
        if "/commits/" in url:
            cid = url.rsplit("/", 1)[1]
            if cid not in self.commits:
                raise urllib.error.HTTPError(url, 404, "nope", {}, io.BytesIO(b"{}"))
            return io.BytesIO(json.dumps({"id": cid}).encode())
        if "/objects?" in url:
            name = urllib.parse.unquote(url.split("path=", 1)[1])
            if name not in self.objects:
                raise urllib.error.HTTPError(url, 404, "nope", {}, io.BytesIO(b"{}"))
            return io.BytesIO(self.objects[name])
        raise AssertionError(url)


class NoStager:
    """No staging key on the pod: dataset and image are staged in-job / pulled; logs are empty."""
    log_data = b""

    def have_key(self):
        return False

    def __call__(self, words, feed=None, quiet=False):
        if words[0] == "log":
            return 0, [{"event": "log", "offset": 0, "data": ""}]
        raise AssertionError(words)


@pytest.fixture
def rig(monkeypatch):
    net = Net()
    monkeypatch.setattr(urllib.request, "urlopen", net)
    monkeypatch.setattr(runmod.time, "sleep", lambda s: None)
    monkeypatch.setattr(runmod, "POLL_S", 0)
    # No controller observation in these tests: the runner polls the site itself, at once (the waits
    # for the controller's first poll and between fallback polls are tested in test_observation.py).
    monkeypatch.setattr(runmod, "STALE_GRACE_S", 0)
    monkeypatch.setattr(runmod, "STALE_POLL_S", 0)
    monkeypatch.setattr(runmod, "REASON_POLL_S", 0)

    class Rig:
        pass
    r = Rig()
    r.net, r.monkeypatch = net, monkeypatch

    def make(slurm, status=None, wallet="wallet.jwt", stager=None, **env):
        cfg = helpers.settings(**env)
        r.cfg, r.slurm = cfg, slurm
        slurm.cfg = cfg
        r.kube = helpers.FakeKube(cfg, status=status)
        r.reporter = rep.Reporter(cfg, r.kube, sleep=lambda s: None)
        r.stager = stager or NoStager()
        r.out = []
        r.logs = logsmod.LogStreamer(cfg, r.stager, r.reporter, out=r.out.append)
        r.logs.start = lambda: None           # the tests drive the stream by hand
        r.runner = runmod.Runner(cfg, slurm=slurm, stager=r.stager, reporter=r.reporter, logs=r.logs,
                                 sleep=lambda s: None, mint=lambda: wallet)
        r.sigterm = []
        monkeypatch.setattr(runmod.signal, "signal", lambda s, h: r.sigterm.append((s, h)))
        return r
    r.make = make
    return r


# ---------------------------------------------------------------- the normal attempt

def test_a_successful_attempt_submits_once_reports_each_state_and_the_outputs(rig):
    slurm = FakeSlurm({"5001": [acc("PENDING", start=0), acc("RUNNING"), acc("COMPLETED")]},
                      live={"5001": {"state_reason": "Priority", "job_state": ["PENDING"]}})
    rig.make(slurm)
    rig.net.objects["hpc-outputs/%s/model-training/training-result.json" % WF] = b'{"map50": 0.71}'
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1 and rig.runner.jid == 5001
    run, status = rig.kube.run, rig.kube.status
    assert run == {"attempt": 1, "siteJobID": "5001", "state": "COMPLETED", "exitCode": 0, "terminal": True,
                   "startTime": "2025-11-05T00:00:00Z", "endTime": "2025-11-05T01:00:00Z", "qos": "default"}
    assert status["outputs"] == {"training-result": '{"map50": 0.71}'}
    assert "pendingReason" not in status, "cleared once the job left the queue"
    states = [c[2]["status"]["run"]["state"] for c in rig.kube.patches() if "run" in c[2]["status"]]
    assert states == ["PENDING", "PENDING", "RUNNING", "COMPLETED"]
    assert rig.net.deleted and "hpc-%s-model-training" % WF in rig.net.deleted[0], "the hand-off branch is deleted"
    assert (signal_name(rig.sigterm[0][0]), rig.sigterm[0][1]) == ("SIGTERM", rig.runner.on_term)


def signal_name(n):
    import signal
    return signal.Signals(n).name


def test_the_pending_reason_is_reported_when_it_changes(rig):
    slurm = FakeSlurm({"5001": [acc("PENDING"), acc("PENDING"), acc("PENDING"), acc("RUNNING"), acc("COMPLETED")]})
    rig.make(slurm)
    slurm.live_view["5001"] = {"state_reason": "Priority"}
    seen = []
    orig = rig.reporter.pending_reason

    def pr(reason):
        seen.append(reason)
        orig(reason)
    rig.reporter.pending_reason = pr
    rig.runner.run()
    assert seen == ["Priority", None], "once while queued, cleared when it left the queue"


def test_a_failed_job_is_reported_and_the_runner_still_exits_zero(rig):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=2, comment="err2:boom|")]})
    rig.make(slurm)
    assert rig.runner.run() == 0, "the controller decides what a failed attempt means; the Job must not retry"
    assert rig.kube.run["state"] == "FAILED" and rig.kube.run["exitCode"] == 2 and rig.kube.run["terminal"]
    assert "outputs" not in rig.kube.status


def test_timeout_and_node_fail_are_terminal_states_too(rig):
    for state in ("TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY", "CANCELLED"):
        slurm = FakeSlurm({"5001": [acc(state, rc=None)]})
        rig.make(slurm)
        assert rig.runner.run() == 0
        assert rig.kube.run["state"] == state and rig.kube.run["terminal"] is True


def test_the_submitted_job_is_the_proven_one(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, KAOS_ATTEMPT="3", HPC_RESUMABLE_LEAD="20", CHECKPOINT_BUCKET="b", KUBECORE_RUN_ID="run-1",
             KAOS_RESUME_REF=REF)
    rig.runner.run()
    job = slurm.submitted[0]["job"]
    env = dict(e.split("=", 1) for e in job["environment"])
    assert (job["name"], job["partition"], job["account"], job["qos"], job["time_limit"]) == \
        (NAME + "-01234567-a3", "gpu", "p1", "default", 720)
    assert job["current_working_directory"] == "/home/u1" and job["requeue"] is True
    assert (env["KAOS_ATTEMPT"], env["KAOS_RESUMABLE"], env["KAOS_RESUME_REF"]) == ("3", "1", CID)
    assert env["KAOS_RESUME_PREFIX"] == "kaos-ckpt/%s/model-training/000002.tar" % WF and env["HPC_GPUS"] == "4"
    assert env["KAOS_PROJID"] == "1" and env["KAOS_CKPT_BUDGET_PCT"] == "10" and "CKPT_B64" in env
    assert env["MLFLOW_TRACKING_TOKEN"] == "wallet.jwt" and env["LAKEFS_BEARER_TOKEN"] == "wallet.jwt"
    assert env["LAKEFS_ENDPOINT"] == "https://lakefs.example" and env["OUTPUTS_REPO"] == "outputs-yolo"
    assert env["CHECKPOINT_BUCKET"] == "b" and env["KUBECORE_RUN_ID"] == "run-1" and "CKPT_B64" in env
    assert "ZITADEL_MACHINE_KEY" not in " ".join(job["environment"]), "the machine key never reaches the site (T-03)"
    assert job["kill_warning_signal"] == "USR1" and job["kill_warning_delay"]["number"] == 20 * 60
    assert slurm.submitted[0]["script"].startswith("#!/bin/bash -l\n# kaos-job v2 scope=-\n")
    assert "SCR=/scr" in slurm.submitted[0]["script"]


def test_without_a_wallet_the_job_runs_without_the_data_plane(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, wallet="")
    rig.runner.run()
    env = dict(e.split("=", 1) for e in slurm.submitted[0]["job"]["environment"])
    assert not [k for k in env if k.startswith(("MLFLOW", "LAKEFS", "STAGE"))]


def test_a_refused_submit_stops_the_runner_with_the_reason_and_reports_nothing(rig, capsys):
    class Refusing(FakeSlurm):
        def submit(self, body):
            raise slurmmod.Refused("HTTP 500 {'errors': [{'description': 'Invalid qos specification'}]}")
    rig.make(Refusing())
    with pytest.raises(SystemExit) as e:
        rig.runner.run()
    assert e.value.code == 1 and "Invalid qos specification" in capsys.readouterr().out
    assert rig.kube.run == {}


def test_staging_that_does_not_fit_the_quota_ends_the_attempt_insufficient_quota_without_submitting(rig, capsys):
    """AC-33: no site job, one terminal report (FAILED, exit 237, a plain message); the run is not a fallback."""
    slurm = FakeSlurm()
    rig.make(slurm)

    def no_room(cfg, stager, mint, **kw):
        raise staging_mod.InsufficientQuota("not enough room on the project's scratch for dataset d@c")
    rig.monkeypatch.setattr(runmod.staging, "stage_dataset", no_room)
    assert rig.runner.run() == 0
    assert slurm.submitted == []
    run = rig.kube.run
    assert run["state"] == "FAILED" and run["exitCode"] == 237 and run["terminal"] is True
    assert run["message"].startswith("not enough room") and not run.get("siteJobID")
    assert "InsufficientQuota" in capsys.readouterr().out


def test_an_image_that_does_not_fit_the_quota_also_ends_insufficient_quota(rig):
    slurm = FakeSlurm()
    rig.make(slurm)
    rig.monkeypatch.setattr(runmod.staging, "stage_dataset", lambda cfg, st, mint, **kw: "")

    def no_room(img, tok, st, strict=False):
        assert strict, "the first submit is strict"
        raise staging_mod.InsufficientQuota("not enough room for the step image")
    rig.monkeypatch.setattr(runmod.imagemod, "stage_image", no_room)
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and rig.kube.run["exitCode"] == 237


# ---------------------------------------------------------------- sharing, leases

def test_a_sharing_gpu_job_is_a_host_with_an_offer_window_and_inputs_in_its_script(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="240")
    rig.runner.run()
    sub = slurm.submitted[0]
    job = sub["job"]
    assert job["name"] == "kaos-a1b2c3d4e5-%s-model-training-01234567-a1" % WF
    assert sub["script"].split("\n")[1] == "# kaos-job v2 scope=a1b2c3d4e5"
    assert [e.split("=")[0] for e in job["environment"]] == ["PATH", "HOME", "USER"], "secrets leave the Slurm environment"
    assert "export LAKEFS_BEARER_TOKEN=wallet.jwt" in sub["script"] and "export KAOS_SLOTS_B64=" in sub["script"]
    assert job["time_limit"] == 480, "twice the step, below the class cap"
    assert job["begin_time"]["set"] and "requeue" not in job
    slurm2 = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm2, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="2000")
    rig.runner.run()
    assert slurm2.submitted[0]["job"]["time_limit"] == 2880, "capped by the class"


def test_the_leases_ride_in_the_job_comment(rig, monkeypatch):
    commit = "c0ffee12" + "0" * 56
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    monkeypatch.setattr(runmod.imagemod, "stage_image", lambda img, tok, st, **kw: "/scr/kaos/images/k.tar")
    rig.runner.run()
    key = runmod.imagemod.image_key(rig.cfg.image)
    assert slurm.submitted[0]["job"]["comment"] == \
        "kaos-commit=" + commit + " kaos-image=" + key + " kaos-ckpt-lease=%s/model-training" % WF
    env = dict(e.split("=", 1) for e in slurm.submitted[0]["job"]["environment"])
    assert env["IMAGE_STAGED"] == "/scr/kaos/images/k.tar" and env["KAOS_COMMIT"] == commit
    assert json.loads(env["KAOS_LIB_LOCK"])["reads"][0]["commit"] == commit, "the step's kaos-lib reads the pin"
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    # no dataset, nothing staged (the SIF may be resident): the image lease is still there (AC-65)
    rig.make(slurm, DATASET_REPO=None, DATASET_VERSION=None, DATASET_COMMIT=None)
    monkeypatch.setattr(runmod.imagemod, "stage_image", lambda img, tok, st, **kw: "")
    rig.runner.run()
    assert slurm.submitted[0]["job"]["comment"] == "kaos-image=%s kaos-ckpt-lease=%s/model-training" % (key, WF)


def test_the_image_cleanup_runs_when_the_attempt_is_over_and_never_fails_it(rig, monkeypatch):
    """AC-65: after the job ends the runner asks the stager for its cleanup (tars of ended jobs, superseded
    SIFs); image.cleanup_images itself never raises (test_image.py)."""
    asked = []
    monkeypatch.setattr(runmod.imagemod, "cleanup_images", lambda stager: asked.append(stager))
    rig.make(FakeSlurm({"5001": [acc("COMPLETED")]}))
    assert rig.runner.run() == 0 and asked == [rig.runner.stager]


# ---------------------------------------------------------------- re-attach: never a second submission

def test_a_restarted_runner_adopts_the_active_job_by_key(rig):
    slurm = FakeSlurm({"5001": [acc("RUNNING"), acc("COMPLETED")]}, active=5001)
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "PENDING", "logOffset": 77}})
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and rig.kube.run["terminal"] is True and rig.kube.run["siteJobID"] == "5001"
    assert rig.logs.job == "5001"


def test_the_log_offset_of_the_adopted_job_is_where_the_stream_resumes(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]}, active=5001)
    stager = type("S", (), {"have_key": lambda s: False, "calls": [],
                            "__call__": lambda s, w, feed=None, quiet=False: (s.calls.append(w) or
                                                                              (0, [{"event": "log", "offset": 177, "data": ""}]))})()
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "logOffset": 77}}, stager=stager)
    rig.runner.run()
    assert ["log", "--job", "5001", "--offset", 77, "--max", logsmod.MAX_BYTES] in stager.calls


def test_a_job_that_ended_while_no_runner_watched_is_adopted_not_resubmitted(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]}, active=None)
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "RUNNING"}})
    assert rig.runner.run() == 0
    assert slurm.submitted == [], "the previous runner reported 5001: that is this attempt's job, finished"
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"]


def test_a_job_this_runner_cancelled_for_a_resubmit_is_not_adopted(rig):
    slurm = FakeSlurm({"5001": [acc("CANCELLED")], "5002": [acc("COMPLETED")]}, active=None)
    slurm.next_id = 5001
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "PENDING"}})
    rig.runner.run()
    assert len(slurm.submitted) == 1 and rig.kube.run["siteJobID"] == "5002"


def test_another_attempts_job_is_never_adopted(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")], "5002": [acc("COMPLETED")]}, active=None)
    slurm.next_id = 5001
    rig.make(slurm, KAOS_ATTEMPT="2", status={"run": {"attempt": 1, "siteJobID": "5001", "terminal": True}})
    rig.runner.run()
    assert len(slurm.submitted) == 1 and rig.kube.run["attempt"] == 2 and rig.kube.run["siteJobID"] == "5002"


def test_an_attempt_that_already_reported_its_end_does_nothing(rig):
    slurm = FakeSlurm({})
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "COMPLETED", "terminal": True}})
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and slurm.accounting_calls == [] and rig.kube.patches() == []


def test_a_deleted_job_means_nothing_to_run(rig, capsys):
    slurm = FakeSlurm({})
    r = rig.make(slurm)
    r.kube.job = None
    assert r.runner.run() == 0 and slurm.submitted == []
    assert "is gone" in capsys.readouterr().out


# ---------------------------------------------------------------- no continuation submission (D-44: the controller decides)

def _ckpt_comment(seq=3, info=None):
    note = "kaos-commit=c0ffee12 kaos-ckpt-lease=%s/model-training kaos-ckpt=%s kaos-ckpt-path=kaos-ckpt/%s/model-training/%06d.tar" % (
        WF, CID, WF, seq)
    info = {"p": [3, 40], "pt": "epoch=3 step=40", "d": "dd", "b": 1000, "sb": 2000, "pb": 9000} if info is None else info
    return note + " kaos-ckpt-info=" + urllib.parse.quote(json.dumps(info, separators=(",", ":")), safe="")


def test_a_checkpoint_copy_is_reported_and_the_runner_exits_without_submitting_the_continuation(rig):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment=_ckpt_comment())]})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20")
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1, "one attempt per runner: the next job is the controller's decision (D-44)"
    assert rig.kube.run["exitCode"] == 85 and rig.kube.run["terminal"] is True
    assert rig.kube.run["checkpointRef"] == "scratch://kaos-ckpt/%s/model-training/000003.tar#%s" % (WF, CID)
    assert rig.kube.run["checkpoint"] == {"progressValues": [3, 40], "progressText": "epoch=3 step=40", "digest": "dd",
                                          "bytes": 1000, "stepBytes": 2000, "projectBytes": 9000}
    assert "outputs" not in rig.kube.status


@pytest.mark.parametrize("state,rc", [("TIMEOUT", None), ("NODE_FAIL", None), ("PREEMPTED", None), ("BOOT_FAIL", None),
                                      ("REVOKED", None), ("FAILED", 236), ("FAILED", 85)])
def test_every_recoverable_end_reports_the_copy_the_job_left(rig, state, rc):
    slurm = FakeSlurm({"5001": [acc(state, rc=rc, comment=_ckpt_comment())]})
    rig.make(slurm)
    assert rig.runner.run() == 0
    assert rig.kube.run["checkpointRef"].endswith("#" + CID) and rig.kube.run["checkpoint"]["progressValues"] == [3, 40]


@pytest.mark.parametrize("state,rc", [("FAILED", 1), ("OUT_OF_MEMORY", None), ("CANCELLED", 85), ("COMPLETED", 0)])
def test_the_steps_own_error_a_cancel_or_a_success_is_never_reported_with_a_copy(rig, state, rc):
    slurm = FakeSlurm({"5001": [acc(state, rc=rc, comment=_ckpt_comment())]})
    rig.make(slurm)
    rig.runner.run()
    assert "checkpointRef" not in rig.kube.run


@pytest.mark.parametrize("comment, why", [
    ("kaos-commit=c0ffee12", "the job comment names no checkpoint copy"),
    ("kaos-ckpt=%s kaos-ckpt-path=kaos-ckpt/other-wf/model-training/000001.tar" % CID, "the job comment names no checkpoint copy"),   # another step's
    ("kaos-ckpt=nothex kaos-ckpt-path=kaos-ckpt/%s/model-training/000001.tar" % WF, "the job comment names no checkpoint copy"),
    ("kaos-ckpt=%s kaos-ckpt-path=../../etc/passwd" % CID, "the job comment names no checkpoint copy"),
])
def test_exit_85_without_a_valid_copy_of_this_step_reports_no_ref(rig, capsys, comment, why):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment=comment)]})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20")
    assert rig.runner.run() == 0 and len(slurm.submitted) == 1
    assert rig.kube.run["terminal"] is True and "checkpointRef" not in rig.kube.run
    assert why in capsys.readouterr().out


def test_the_exit_85_message_does_not_blame_the_lead_when_the_comment_is_all_that_is_missing(rig, capsys):
    # Live job 5433318: the copy was made, only its comment words were lost. The message names both causes.
    rig.make(FakeSlurm({"5001": [acc("FAILED", rc=85, comment="kaos-commit=c0ffee12 kaos-image=e882f17eaa4ee41b")]}),
             HPC_RESUMABLE_LEAD="20")
    rig.runner.run()
    out = capsys.readouterr().out
    assert "only its entry in the job comment is missing" in out and "no copy was made in time" in out


def test_a_node_failure_with_no_copy_reports_no_ref_and_does_not_wait_for_one(rig):
    slurm = FakeSlurm({"5001": [acc("NODE_FAIL", rc=None, comment="kaos-commit=c0ffee12")]})
    rig.make(slurm)
    assert rig.runner.run() == 0 and "checkpointRef" not in rig.kube.run and rig.kube.run["state"] == "NODE_FAIL"


def test_lagging_accounting_is_covered_by_slurmctld(rig):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment="kaos-commit=c0ffee12")]},
                      live={"5001": {"comment": _ckpt_comment()}})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20")
    rig.runner.run()
    assert "checkpointRef" in rig.kube.run


def test_the_submitted_comment_leases_the_steps_checkpoint_copies(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner.run()
    assert "kaos-ckpt-lease=%s/model-training" % WF in slurm.submitted[0]["job"]["comment"]


def test_a_skipped_periodic_copy_is_reported_while_the_job_runs(rig):
    info = {"sb": 10, "pb": 20, "sk": "skipped the periodic copy: it would take the project over 80% of its scratch space"}
    slurm = FakeSlurm({"5001": [acc("RUNNING", comment=_ckpt_comment(info=info)), acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner.run()
    running = [c[2]["status"]["run"] for c in rig.kube.patches() if c[2]["status"]["run"].get("state") == "RUNNING"]
    assert running and running[0]["checkpoint"]["skippedReason"].startswith("skipped the periodic copy")


# ---------------------------------------------------------------- resubmits inside one attempt

def test_a_pull_failure_resubmits_with_fresh_credentials_within_the_same_attempt(rig, capsys):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=231, comment="err231:pull|")], "5002": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner.run()
    assert len(slurm.submitted) == 2 and rig.kube.run["siteJobID"] == "5002" and rig.kube.run["attempt"] == 1
    assert "pull failed - resubmitting" in capsys.readouterr().out
    first = next(c for c in rig.kube.patches() if c[2]["status"]["run"]["siteJobID"] == "5001")
    assert first[2]["status"]["run"]["terminal"] is None, "a resubmit does not end the attempt"
    assert rig.kube.run["terminal"] is True and rig.kube.run["state"] == "COMPLETED"


def test_pull_resubmits_are_bounded(rig):
    slurm = FakeSlurm({str(5001 + i): [acc("FAILED", rc=231)] for i in range(5)})
    rig.make(slurm)
    rig.runner.run()
    assert len(slurm.submitted) == 1 + runmod.PULL_RESUBMITS
    assert rig.kube.run["terminal"] is True and rig.kube.run["exitCode"] == 231


def test_the_dataset_staging_record_is_reported_from_the_first_report_on(rig):
    """The follower prints it as one line: it must be in status.run while the job still queues."""
    slurm = FakeSlurm({"5001": [acc("PENDING"), acc("COMPLETED")]})
    rig.make(slurm)
    rec = {"repo": "ds", "commit": "c2" * 6, "kind": "delta", "files": 3, "bytes": 1234, "seconds": 12}
    rig.monkeypatch.setattr(runmod.staging, "stage_dataset", lambda cfg, st, mint, info=None: info.update(rec) or "/p")
    rig.runner.run()
    runs = [c[2]["status"]["run"] for c in rig.kube.patches() if "run" in c[2]["status"]]
    assert runs[0]["state"] == "PENDING" and runs[0]["staging"] == rec and runs[-1]["staging"] == rec


def test_a_queued_job_is_replaced_before_its_wallet_expires(rig, monkeypatch):
    slurm = FakeSlurm({"5001": [acc("PENDING")], "5002": [acc("COMPLETED")]})
    rig.make(slurm)
    exp = [1000.0]
    monkeypatch.setattr(runmod.wallet, "wallet_exp", lambda w: exp[0])
    monkeypatch.setattr(runmod.time, "time", lambda: 1000.0 - 600)        # 10 min before: inside the margin
    rig.runner.run()
    assert slurm.cancelled == ["5001"] and len(slurm.submitted) == 2 and rig.kube.run["siteJobID"] == "5002"


def test_the_replacement_of_a_queued_job_is_reported_with_both_ids_and_the_queued_seconds(rig, monkeypatch):
    """AC-35: the queue place lost to a token refresh is recorded in status.run, not only in the log."""
    slurm = FakeSlurm({"5001": [acc("PENDING")], "5002": [acc("COMPLETED")]})
    rig.make(slurm)
    clock = [100.0]
    monkeypatch.setattr(runmod.wallet, "wallet_exp", lambda w: 100.0 + 4 * 3600)
    monkeypatch.setattr(runmod.time, "time", lambda: clock[0])
    rig.runner.sleep = lambda s: clock.__setitem__(0, clock[0] + 3600)   # every poll of the queued job takes an hour
    rig.runner.run()
    assert slurm.cancelled == ["5001"] and rig.kube.run["siteJobID"] == "5002"
    (rec,) = rig.kube.run["queueReplaced"]
    assert (rec["oldSiteJobID"], rec["newSiteJobID"], rec["reason"]) == ("5001", "5002", "TokenRefresh")
    assert 3 * 3600 <= rec["queuedSeconds"] <= 4 * 3600 and rec["at"].endswith("Z")
    assert rig.kube.run["queueReplacedCount"] == 1


def test_a_job_a_host_took_is_never_replaced_for_its_wallet(rig, monkeypatch):
    taken = acc("PENDING", comment="kaos-commit=c0ffee12 kaos-state=taken host=4000 slot=1 attempt=1")
    done = acc("RUNNING", comment="kaos-state=done rc=0 host=4000")
    slurm = FakeSlurm({"5001": [taken, done], "4000": [acc("RUNNING")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    monkeypatch.setattr(runmod.wallet, "wallet_exp", lambda w: 1000.0)
    monkeypatch.setattr(runmod.time, "time", lambda: 5000.0)               # long past the wallet
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["exitCode"] == 0 and rig.kube.run["terminal"]
    patch = next(c for c in rig.kube.patches() if c[2]["status"]["run"].get("hostSiteJobID"))
    assert patch[2]["status"]["run"]["hostSiteJobID"] == "4000" and patch[2]["status"]["run"]["slot"] == 1


def test_a_guest_that_never_started_reports_its_host_slot_and_an_end(rig, monkeypatch):
    """Live 2026-11-06: the guest's record had a NO_VAL start, no end, and a done comment naming the host."""
    taken = acc("PENDING", comment="kaos-state=taken host=5427313 slot=2 attempt=1")
    done = acc("CANCELLED", comment="kaos-state=done rc=0 host=5427313", start=4294967294, end=0)
    slurm = FakeSlurm({"5001": [taken, done], "5427313": [acc("RUNNING")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    monkeypatch.setattr(runmod.time, "time", lambda: 1762304400.0)
    rig.runner.run()
    run = rig.kube.run
    assert run["terminal"] and run["hostSiteJobID"] == "5427313" and run["slot"] == 2
    assert "startTime" not in run, "NO_VAL is not a start"
    assert run["endTime"] == "2025-11-05T01:00:00Z", "no end from Slurm: when the runner saw it over"


def test_a_guest_reports_the_step_start_its_host_wrote(rig, monkeypatch):
    """D-67: a guest's own job never starts; the start its host wrote is its step's, so its run time is a sample."""
    taken = acc("PENDING", comment="kaos-state=taken host=5439966 slot=0 attempt=1")
    done = acc("CANCELLED", comment="kaos-state=done rc=0 host=5439966 start=1762300800", start=4294967294, end=1762301700)
    rig.make(FakeSlurm({"5001": [taken, done], "5439966": [acc("RUNNING")]}), HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    run = rig.kube.run
    assert (run["startTime"], run["endTime"], run["hostSiteJobID"]) == ("2025-11-05T00:00:00Z", "2025-11-05T00:15:00Z", "5439966")


def test_a_host_that_died_with_the_job_starts_it_over_bounded(rig):
    taken = acc("PENDING", comment="kaos-state=taken host=4000 slot=0 attempt=1")
    slurm = FakeSlurm({"5001": [taken], "5002": [acc("COMPLETED")], "4000": [acc("NODE_FAIL")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert slurm.cancelled == ["5001"] and len(slurm.submitted) == 2 and rig.kube.run["siteJobID"] == "5002"


def test_a_resubmit_replaces_the_log_and_clears_its_offset(rig):
    files = {"5001": b"queued\n", "5002": b"new\n"}
    from test_logs import LogStager
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=231)], "5002": [acc("COMPLETED")]})
    rig.make(slurm, stager=LogStager(files))
    rig.runner.run()
    msgs = [(json.loads(l)["hpc_job"], json.loads(l)["msg"]) for l in rig.out]
    assert msgs == [("5001", "queued"), ("5002", "new")]
    assert rig.kube.run["logOffset"] == 4, "the new job's offset, not the old job's"


# ---------------------------------------------------------------- outputs

def test_outputs_above_the_cap_fail_the_attempt_loudly(rig, capsys):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.net.objects["hpc-outputs/%s/model-training/training-result.json" % WF] = b"x" * (64 * 1024 + 1)
    assert rig.runner.run() == 0
    assert rig.kube.run["state"] == "FAILED" and rig.kube.run["exitCode"] == 1 and rig.kube.run["terminal"]
    assert "outputs" not in rig.kube.status and "ERROR:" in capsys.readouterr().out


def test_an_output_the_step_did_not_write_is_left_out_for_the_follower_to_default(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, STEP_OUTPUTS="a,b")
    rig.net.objects["hpc-outputs/%s/model-training/a.json" % WF] = b"[1]"
    rig.runner.run()
    assert rig.kube.status["outputs"] == {"a": "[1]"}


def test_a_long_job_gets_a_fresh_wallet_for_its_outputs(rig, monkeypatch):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    minted = iter(["w1", "w2"])
    r = rig.make(slurm)
    r.runner._mint = lambda: next(minted)
    seen = []
    monkeypatch.setattr(runmod.wallet, "wallet_exp", lambda w: 100.0 if w == "w1" else 1e12)
    monkeypatch.setattr(r.runner, "fetch_outputs", lambda: seen.append(r.runner.wallet) or {})
    r.runner.run()
    assert seen == ["w2"], "the wallet minted at submit had expired by the end of the job"


# ---------------------------------------------------------------- SIGTERM (the controller deleted the runner Job)

def _term(rig, slurm, cancel, state="PENDING"):
    r = rig.make(slurm)
    r.runner.jid = "5001"
    if cancel:
        r.kube.job["metadata"]["annotations"] = {"hpc.kaos.io/cancel-requested": "2026-11-06T10:00:00Z"}
    return r


@pytest.mark.parametrize("state", ["PENDING", "RUNNING"])
def test_sigterm_with_a_cancel_request_stops_the_site_job_and_reports_it(rig, state):
    slurm = FakeSlurm({}, live={"5001": {"job_state": [state]}})
    r = _term(rig, slurm, True)
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 0 and slurm.cancelled == ["5001"]
    assert r.kube.run["state"] == "CANCELLED" and r.kube.run["terminal"] is True


def test_sigterm_without_a_cancel_request_leaves_the_site_job_for_the_next_runner(rig):
    slurm = FakeSlurm({}, live={"5001": {"job_state": ["PENDING"]}})
    r = _term(rig, slurm, False)
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 143 and slurm.cancelled == [] and r.kube.run == {}, "no cancel, no terminal report"


def test_sigterm_with_an_unreadable_job_does_not_cancel(rig):
    slurm = FakeSlurm({}, live={"5001": {"job_state": ["PENDING"]}})
    r = _term(rig, slurm, True)
    r.kube.fail = [500, OSError("reset"), 503]       # the first read and both retries fail
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 143 and slurm.cancelled == [] and r.kube.run == {}
    assert len([c for c in r.kube.calls if c[0] == "GET"]) == 3


def test_a_cancel_check_that_works_on_a_retry_still_cancels(rig):
    slurm = FakeSlurm({}, live={})
    r = _term(rig, slurm, True)
    r.kube.fail = [500]
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 0 and slurm.cancelled == ["5001"]


def test_sigterm_during_staging_just_exits(rig):
    r = rig.make(FakeSlurm({}))
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 143


def test_sigterm_saves_the_log_offset_for_the_next_runner(rig):
    from test_logs import LogStager
    slurm = FakeSlurm({}, live={"5001": {"job_state": ["RUNNING"]}})
    r = rig.make(slurm, stager=LogStager({"5001": b"one\ntwo\npar"}))
    r.runner.jid = "5001"
    r.logs.attach("5001", 0)
    r.logs.poll()
    with pytest.raises(SystemExit):
        r.runner.on_term(15, None)
    assert r.kube.run["logOffset"] == len(b"one\ntwo\n")


def test_a_failing_state_poll_is_transient(rig, capsys):
    class Flaky(FakeSlurm):
        n = 0

        def accounting(self, jid):
            Flaky.n += 1
            if Flaky.n < 3:
                raise OSError("502")
            return super().accounting(jid)
    slurm = Flaky({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    assert rig.runner.run() == 0
    assert capsys.readouterr().out.count("poll error (transient)") == 2


def test_main_builds_the_runner_from_the_environment(monkeypatch):
    seen = {}
    monkeypatch.setattr(runmod.Runner, "run", lambda self: seen.setdefault("cfg", self.cfg) and 0)
    assert runmod.main(dict(helpers.BASE_ENV)) == 0
    assert seen["cfg"].job_name == "hpc-0123456789abcdef0123"


# ---------------------------------------------------------------- lanes (PRD-HPC-1461 D-68)

class LaneSlurm(FakeSlurm):
    """Refuses the first submits with the given texts; records a copy of every body."""

    def __init__(self, refusals=(), **kw):
        super().__init__(**kw)
        self.refusals = list(refusals)

    def submit(self, body):
        import copy
        self.submitted.append(copy.deepcopy(body))
        if self.refusals:
            raise slurmmod.Refused(self.refusals.pop(0))
        self.next_id += 1
        return {"job_id": self.next_id, "errors": []}


def test_a_lane_is_submitted_with_its_qos_and_reservation_and_reported(rig):
    slurm = LaneSlurm(jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_QOS="dev", HPC_RESERVATION="gpudev", SLURM_TIME_LIMIT="120")
    rig.runner.run()
    assert len(slurm.submitted) == 1
    job = slurm.submitted[0]["job"]
    assert (job["qos"], job["reservation"], job["time_limit"]) == ("dev", "gpudev", 120)
    assert (rig.kube.run["qos"], rig.kube.run.get("reservation")) == ("dev", "gpudev")


def test_without_a_lane_the_class_qos_is_used_reported_and_no_reservation_is_sent(rig):
    slurm = LaneSlurm(jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner.run()
    assert "reservation" not in slurm.submitted[0]["job"] and slurm.submitted[0]["job"]["qos"] == "default"
    assert rig.kube.run["qos"] == "default" and rig.kube.run.get("reservation") is None


@pytest.mark.parametrize("text", [
    "HTTP 500 {'errors': [{'error': 'Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)'}]}",
    "HTTP 500 {'errors': [{'error': 'Invalid qos specification'}]}",
    "HTTP 500 {'errors': [{'error': 'Requested reservation is invalid'}]}",
    "HTTP 500 {'errors': [{'error': 'Access denied to requested reservation'}]}",
    "HTTP 500 {'errors': [{'error': 'Requested time limit is invalid (missing or exceeds some limit)'}]}",
    "HTTP 500 {'errors': [{'error': 'QOSMaxJobsPerUserLimit'}]}",
])
def test_a_refused_lane_is_retried_once_on_the_class_default_without_a_reservation(rig, text, capsys):
    slurm = LaneSlurm([text], jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_QOS="test", HPC_RESERVATION="gpudev")
    assert rig.runner.run() == 0
    first, second = (s["job"] for s in slurm.submitted)
    assert (first["qos"], first["reservation"]) == ("test", "gpudev")
    assert second["qos"] == "default" and "reservation" not in second
    assert "refused by the site" in capsys.readouterr().out
    assert rig.kube.run["qos"] == "default" and rig.kube.run.get("reservation") is None


def test_a_second_refusal_after_the_fallback_still_fails_loudly(rig):
    slurm = LaneSlurm(["HTTP 500 QOSMaxJobsPerUserLimit", "HTTP 500 Invalid account or account/partition"])
    rig.make(slurm, HPC_QOS="test")
    with pytest.raises(SystemExit) as e:
        rig.runner.run()
    assert e.value.code == 1 and len(slurm.submitted) == 2


def test_a_refusal_unrelated_to_the_lane_is_not_retried(rig):
    slurm = LaneSlurm(["HTTP 500 Invalid account or account/partition combination specified"])
    rig.make(slurm, HPC_QOS="test")
    with pytest.raises(SystemExit):
        rig.runner.run()
    assert len(slurm.submitted) == 1


def test_a_shared_node_host_pads_its_limit_only_up_to_the_lane_maximum(rig):
    slurm = LaneSlurm(jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="20",
             HPC_QOS="test", HPC_QOS_MAX_MINUTES="30")
    rig.runner.run()
    assert slurm.submitted[0]["job"]["time_limit"] == 30, "2 x 20 shrinks to the test lane's 30"
    slurm = LaneSlurm(jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="20")
    rig.runner.run()
    assert slurm.submitted[0]["job"]["time_limit"] == 40, "no lane: today's padding"


def test_a_guest_never_inherits_the_hosts_reservation(rig):
    slurm = LaneSlurm(jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", HPC_QOS="dev", HPC_RESERVATION="gpudev")
    rig.runner.run()
    sub = slurm.submitted[0]
    assert sub["job"]["reservation"] == "gpudev"
    assert "gpudev" not in sub["script"] and "reservation" not in sub["script"].lower()


def test_an_adopting_runner_leaves_the_reported_lane_alone(rig):
    slurm = LaneSlurm(jobs={"5001": [acc("COMPLETED")]}, active="5001")
    rig.make(slurm, status={"run": {"attempt": 1, "qos": "default"}}, HPC_QOS="test")
    rig.runner.run()
    assert slurm.submitted == [] and rig.kube.run["qos"] == "default"


# ---------------------------------------------------------------- D-72: the idle watchdog

def test_the_idle_window_defaults_to_20_and_zero_turns_it_off():
    assert helpers.settings().idle_timeout == 20
    assert helpers.settings(HPC_IDLE_TIMEOUT_MINUTES="45").idle_timeout == 45
    assert helpers.settings(HPC_IDLE_TIMEOUT_MINUTES="0").idle_timeout == 0
    assert helpers.settings(HPC_IDLE_TIMEOUT_MINUTES="junk").idle_timeout == 20


def test_the_job_carries_the_idle_window_and_the_watchdog_program(rig):
    rig.make(FakeSlurm({"5001": [acc("COMPLETED")]}), HPC_IDLE_TIMEOUT_MINUTES="35")
    rig.runner.run()
    env = dict(e.split("=", 1) for e in rig.slurm.submitted[0]["job"]["environment"])
    assert env["KAOS_IDLE_TIMEOUT_MIN"] == "35" and env["IDLEWATCH_B64"]
    assert "IDLEWATCH_B64" in rig.slurm.submitted[0]["script"]


def test_idle_window_0_sends_no_watchdog_program(rig):
    rig.make(FakeSlurm({"5001": [acc("COMPLETED")]}), HPC_IDLE_TIMEOUT_MINUTES="0")
    rig.runner.run()
    env = dict(e.split("=", 1) for e in rig.slurm.submitted[0]["job"]["environment"])
    assert env["KAOS_IDLE_TIMEOUT_MIN"] == "0" and "IDLEWATCH_B64" not in env


def test_an_idle_end_reports_exit_236_and_the_last_log_lines(rig):
    rig.make(FakeSlurm({"5001": [acc("FAILED", rc=236, comment="err236:idle|")]}))
    rig.logs.tail_text = lambda: "epoch 3 done\nwaiting for rank 1"
    assert rig.runner.run() == 0
    assert rig.kube.run["exitCode"] == 236 and rig.kube.run["state"] == "FAILED" and rig.kube.run["terminal"]
    assert rig.kube.run["lastLog"] == "epoch 3 done\nwaiting for rank 1"


def test_other_failures_report_no_last_log(rig):
    rig.make(FakeSlurm({"5001": [acc("FAILED", rc=2, comment="err2:boom|")]}))
    rig.logs.tail_text = lambda: "x"
    rig.runner.run()
    assert rig.kube.run.get("lastLog") is None


# ---------------------------------------------------------------- a host settles from its job's end

HOST_DONE = "kaos-state=done rc=0"


def terminal_reports(rig):
    return [c[2]["status"]["run"] for c in rig.kube.patches() if (c[2]["status"].get("run") or {}).get("terminal")]


def test_a_host_reports_its_step_at_once_and_its_job_end_when_the_job_ends(rig):
    """Live 2026-11-07 (job 5430776): the step ended, the job ran ~2 minutes more waiting for guests."""
    step_over = acc("RUNNING", comment=HOST_DONE, start=1762300800, end=0)
    job_over = acc("COMPLETED", comment=HOST_DONE, start=1762300800, end=1762300980)
    slurm = FakeSlurm({"5001": [step_over, step_over, job_over]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    first, last = terminal_reports(rig)[0], terminal_reports(rig)[-1]
    assert len(terminal_reports(rig)) == 2
    assert first["state"] == "COMPLETED" and first.get("hostSiteJobID") is None
    assert first["hostJobOpen"] is True, "D-26: the controller holds the host's booking until the job's end"
    assert last["hostJobEndTime"] == "2025-11-05T00:03:00Z", "the host job's own end, for the settlement"
    assert last["endTime"] == first["endTime"], "endTime stays the step's end: the learned limit's sample (D-67)"
    assert last["hostJobOpen"] is None and "hostJobOpen" not in rig.kube.run, "the job's end closes the host"
    same = ("hostJobEndTime", "hostJobOpen")
    assert {k: v for k, v in last.items() if k not in same} == {k: v for k, v in first.items() if k not in same}
    outputs_patches = [c for c in rig.kube.patches() if "outputs" in c[2]["status"]]
    assert len(outputs_patches) <= 1, "outputs ride with the step's report only"
    assert "outputs" not in rig.kube.patches()[-1][2]["status"]


def test_a_failed_host_keeps_the_start_an_earlier_poll_saw_when_its_end_record_has_none(rig):
    """Live 2026-11-08 (job 5435560): the terminal report carried no start, the controller settled 0 node-minutes."""
    running = acc("RUNNING", start=1762300800, end=0)
    step_over = acc("RUNNING", comment="kaos-state=done rc=1", start=0, end=0)
    job_over = acc("FAILED", comment="kaos-state=done rc=1", start=0, end=1762301400)
    slurm = FakeSlurm({"5001": [running, step_over, job_over]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    first, last = terminal_reports(rig)[0], terminal_reports(rig)[-1]
    assert first["startTime"] == "2025-11-05T00:00:00Z", "the start is the site's, from the earlier poll"
    assert last["startTime"] == first["startTime"], "the host job's end report keeps it too"


def test_a_resumable_host_that_saved_is_reported_once_when_its_slurm_job_ends(rig):
    """AC-73: the controller continues a checkpointed step at once and releases its booking, but the site charges the
    node until the host's job ends. The step's end is held back; the one terminal report carries the job's end, so
    the controller settles the node time the host held for its guests (and the continuation follows)."""
    running = acc("RUNNING", start=1762300800, end=0)
    step_saved = acc("RUNNING", comment=_ckpt_comment() + " kaos-state=done rc=85", start=1762300800, end=0)
    job_over = acc("FAILED", rc=85, comment=_ckpt_comment() + " kaos-state=done rc=85", start=1762300800, end=1762302000)
    slurm = FakeSlurm({"5001": [running, step_saved, step_saved, job_over]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", HPC_RESUMABLE_LEAD="10")
    assert rig.runner.run() == 0
    reports = terminal_reports(rig)
    assert len(reports) == 1, "one terminal report, at the job's end - no early report with hostJobOpen"
    run = rig.kube.run
    assert (run["state"], run["exitCode"], run["endTime"]) == ("FAILED", 85, "2025-11-05T00:20:00Z")
    assert "hostJobOpen" not in run and run["checkpointRef"].endswith("#" + CID)


def test_a_resumable_host_job_is_not_padded_and_a_plain_one_is(rig):
    """AC-73: a plain host's job asks for twice its step so guests fit; a resumable host's guests are resumable
    steps that fit by their lead, and its job must end soon after its step."""
    plain, resumable = FakeSlurm({"5001": [acc("COMPLETED")]}), FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(plain, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="240")
    rig.runner.run()
    rig.make(resumable, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="240", HPC_RESUMABLE_LEAD="10")
    rig.runner.run()
    assert plain.submitted[0]["job"]["time_limit"] == 480 and resumable.submitted[0]["job"]["time_limit"] == 240


def test_a_start_is_never_carried_over_to_another_site_job(rig):
    rig.make(FakeSlurm({"5001": [acc("COMPLETED")]}))
    rig.runner.jid = "5001"
    assert rig.runner.start_of(acc("RUNNING", start=1762300800)) == "2025-11-05T00:00:00Z"
    rig.runner.jid = "5002"
    assert rig.runner.start_of(acc("RUNNING", start=0)) is None


def test_a_job_that_ended_by_itself_reports_its_end_once(rig):
    slurm = FakeSlurm({"5001": [acc("RUNNING"), acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner.run()
    assert len(terminal_reports(rig)) == 1


def test_a_guest_never_follows_a_host_job(rig, monkeypatch):
    taken = acc("PENDING", comment="kaos-state=taken host=5427313 slot=2 attempt=1")
    done = acc("RUNNING", comment="kaos-state=done rc=0 host=5427313")
    slurm = FakeSlurm({"5001": [taken, done], "5427313": [acc("RUNNING")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert len(terminal_reports(rig)) == 1
    assert rig.runner.host_job_open is False


def test_a_host_job_that_never_ends_is_given_up_after_its_limit_plus_margin(rig, monkeypatch):
    step_over = acc("RUNNING", comment=HOST_DONE, end=0)
    slurm = FakeSlurm({"5001": [step_over]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    clock = [1762300800.0]
    monkeypatch.setattr(runmod.time, "time", lambda: clock[0])
    rig.runner.sleep = lambda s: clock.__setitem__(0, clock[0] + 600)
    assert rig.runner.run() == 0
    first, last = terminal_reports(rig)
    assert first["hostJobOpen"] is True and last["hostJobOpen"] is None, "the host is closed all the same"
    limit_s = rig.runner.job_limit_min * 60
    assert last["hostJobEndTime"] == time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(1762300800 + limit_s)), \
        "the latest end Slurm allows the job: its start plus its time limit"


def test_a_restarted_host_runner_follows_its_host_job_to_its_end_without_submitting(rig):
    """The runner pod was replaced after the step's report (hostJobOpen): only the host job's end is left."""
    opened = {"attempt": 1, "siteJobID": "5001", "state": "COMPLETED", "exitCode": 0, "terminal": True,
              "startTime": "2025-11-05T00:00:00Z", "endTime": "2025-11-05T00:01:00Z", "hostJobOpen": True}
    slurm = FakeSlurm({"5001": [acc("RUNNING", comment=HOST_DONE, end=0), acc("COMPLETED", comment=HOST_DONE, end=1762302060)]})
    rig.make(slurm, status={"run": dict(opened)}, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and slurm.cancelled == []
    assert rig.kube.run["hostJobEndTime"] == "2025-11-05T00:21:00Z" and "hostJobOpen" not in rig.kube.run
    assert rig.kube.run["endTime"] == "2025-11-05T00:01:00Z", "the step's end is kept"
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["exitCode"] == 0 and rig.kube.run["terminal"]


def test_a_guest_stopped_by_its_own_time_limit_reports_a_timeout_and_any_other_kill_a_failure(rig):
    """AC-72 / D-67 (live 2026-11-10, guest 5439976): the slot manager says reason=TIMEOUT when srun --time stopped
    the step; the attempt then ends like a TIMEOUT alone, so the controller retries a learned limit."""
    for comment, state in (("kaos-state=done rc=143 host=5439966 reason=TIMEOUT", "TIMEOUT"),
                           ("kaos-state=done rc=143 host=5439966", "FAILED")):
        taken = acc("PENDING", comment="kaos-state=taken host=5439966 slot=0 attempt=1")
        done = acc("CANCELLED", comment=comment, start=4294967294, end=0)
        rig.make(FakeSlurm({"5001": [taken, done], "5439966": [acc("RUNNING")]}), HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
        rig.runner.run()
        run = rig.kube.run
        assert (run["state"], run["exitCode"], run["hostSiteJobID"], run["slot"]) == (state, 143, "5439966", 0), comment
        assert "hostJobOpen" not in run, "a guest never holds a node of its own"

def test_a_resumable_sharing_job_hands_its_lead_to_the_slot_manager_and_asks_slurm_for_no_warning_signal(rig):
    """AC-73: a host's batch shell is the slot manager, which USR1 would kill; every step on a shared node is
    signalled by the slot manager, from the lead in its script (KAOS_RESUMABLE_LEAD_MIN). A resumable job alone
    keeps Slurm's own warning signal (test_the_submitted_job_is_the_proven_one)."""
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", HPC_RESUMABLE_LEAD="15")
    rig.runner.run()
    sub = slurm.submitted[0]
    assert "export KAOS_RESUMABLE_LEAD_MIN=15" in sub["script"].split("\n"), "a host reads it from the waiting script"
    assert "export KAOS_RESUMABLE=1" in sub["script"].split("\n")
    assert "kill_warning_signal" not in sub["job"] and "kill_warning_delay" not in sub["job"]
    assert sub["job"]["name"].startswith("kaos-a1b2c3d4e5-"), "a resumable step is a job a host takes"
    slurm2 = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm2, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert "KAOS_RESUMABLE_LEAD_MIN" not in slurm2.submitted[0]["script"], "a step that is not resumable carries none"


def test_a_resumable_guest_that_saved_is_reported_like_a_resumable_step_alone(rig):
    """AC-73: a guest that checkpointed and exited 85 ends as FAILED/85 with its copy, on its host, and holds no
    node of its own: the controller continues it from the checkpoint (a continuation may host or be a guest)."""
    taken = acc("PENDING", comment="kaos-state=taken host=5439966 slot=1 attempt=1")
    done = acc("CANCELLED", comment=_ckpt_comment() + " kaos-state=done rc=85 host=5439966 start=1762300800",
               start=4294967294, end=1762301700)
    rig.make(FakeSlurm({"5001": [taken, done], "5439966": [acc("RUNNING")]}), HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5",
             HPC_RESUMABLE_LEAD="10")
    assert rig.runner.run() == 0
    run = rig.kube.run
    assert (run["state"], run["exitCode"], run["hostSiteJobID"], run["slot"]) == ("FAILED", 85, "5439966", 1)
    assert run["checkpointRef"] == "scratch://kaos-ckpt/%s/model-training/000003.tar#%s" % (WF, CID)
    assert run["checkpoint"]["progressValues"] == [3, 40] and "hostJobOpen" not in run


def test_a_step_the_slot_manager_stopped_is_a_failure_even_with_exit_0__live_job_5442274(rig):
    """Live 2026-11-10: `reason=GPUViolation` with rc 0 ended the HPC job Succeeded. Whatever exit code an older
    slot manager wrote, a memkill or GPUViolation step is FAILED, as a guest and as a host's own step."""
    for reason in ("GPUViolation", "memkill"):
        taken = acc("PENDING", comment="kaos-state=taken host=5442272 slot=2 attempt=1")
        done = acc("CANCELLED", comment="kaos-state=done rc=0 host=5442272 reason=" + reason, start=4294967294, end=0)
        rig.make(FakeSlurm({"5001": [taken, done], "5442272": [acc("RUNNING")]}), HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
        rig.runner.run()
        assert (rig.kube.run["state"], rig.kube.run["exitCode"]) == ("FAILED", 1), reason
    own = acc("COMPLETED", comment="kaos-state=done rc=0 reason=GPUViolation", start=1762300800, end=1762301400)
    rig.make(FakeSlurm({"5001": [own]}), HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert (rig.kube.run["state"], rig.kube.run["exitCode"]) == ("FAILED", 1)


# ---------------------------------------------------------------- the controller's batched reading (D-36)

T0 = 1762500000.0     # the tests' clock: 2025-11-07T08:00:00Z


def stamp(t):
    import time as _t
    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(t))


def obs(state, t=T0, jid="5001", reason=None, comment=None, interval=30):
    o = {"jobID": jid, "state": state, "observedAt": stamp(t), "intervalSeconds": interval}
    if reason is not None:
        o["pendingReason"] = reason
    if comment is not None:
        o["comment"] = comment
    return o


class CountingSlurm(FakeSlurm):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.live_calls = 0

    def live(self, jid):
        self.live_calls += 1
        return super().live(jid)


def drive(r, script, step=30):
    """Each poll tick sleeps once: install the next observation (None: leave the last) and move the clock."""
    clock = {"t": T0}
    script = list(script)

    def sleep(s):
        if script:
            o = script.pop(0)
            if o is not None:
                r.kube.status["siteObservation"] = o(clock["t"]) if callable(o) else o
        clock["t"] += step
    r.runner.clock = lambda: clock["t"]
    r.runner.sleep = sleep
    r.clock = clock


def test_a_queued_job_makes_no_site_call_while_the_controller_reports_its_state(rig):
    slurm = CountingSlurm({"5001": [acc("PENDING"), acc("COMPLETED")]})
    rig.make(slurm)
    queued = lambda t: obs("PENDING", t, reason="Priority")      # noqa: E731
    drive(rig, [queued] * 10 + [lambda t: obs("COMPLETED", t)])
    rig.runner.run()
    # one accounting read for the first state report and one for the final record; none in between
    assert slurm.accounting_calls == [5001, 5001] and slurm.live_calls == 0
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"]
    states = [c[2]["status"]["run"]["state"] for c in rig.kube.patches() if "run" in c[2]["status"]]
    assert states == ["PENDING", "PENDING", "COMPLETED"]


def test_the_pending_reason_comes_from_the_observation_and_clears_when_it_runs(rig):
    slurm = CountingSlurm({"5001": [acc("PENDING"), acc("RUNNING"), acc("COMPLETED")]})
    rig.make(slurm)
    seen = []
    orig = rig.reporter.pending_reason
    rig.reporter.pending_reason = lambda reason: (seen.append(reason), orig(reason))
    drive(rig, [lambda t: obs("PENDING", t, reason="Priority"), lambda t: obs("PENDING", t, reason="Resources"),
                lambda t: obs("RUNNING", t), lambda t: obs("COMPLETED", t)])
    rig.runner.run()
    assert seen == ["Priority", "Resources", None] and slurm.live_calls == 0


def test_a_missing_reason_is_asked_of_the_job_only_every_so_often(rig):
    rig.monkeypatch.setattr(runmod, "REASON_POLL_S", 120)
    slurm = CountingSlurm({"5001": [acc("PENDING"), acc("COMPLETED")]}, live={"5001": {"state_reason": "Priority"}})
    rig.make(slurm)
    drive(rig, [lambda t: obs("PENDING", t)] * 9 + [lambda t: obs("COMPLETED", t)])
    rig.runner.run()
    assert 1 <= slurm.live_calls <= 3, "9 ticks of 30 s: about one per 120 s, not one per tick"


def test_a_sharing_guest_reads_its_comment_from_the_observation_without_asking_the_site(rig):
    slurm = CountingSlurm({"5001": [acc("PENDING"), acc("PENDING", comment="kaos-state=done host=9 slot=1 rc=0"),
                                    acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    drive(rig, [lambda t: obs("PENDING", t, reason="Priority", comment=""),
                lambda t: obs("PENDING", t, reason="Priority", comment="kaos-state=taken host=9 slot=1"),
                lambda t: obs("RUNNING", t, comment="kaos-state=done host=9 slot=1 rc=0")])
    rig.runner.run()
    assert slurm.live_calls == 0
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["hostSiteJobID"] == "9"


def test_a_sharing_job_whose_observation_has_no_comment_still_reads_it_from_the_job(rig):
    slurm = CountingSlurm({"5001": [acc("PENDING"), acc("COMPLETED")]}, live={"5001": {"state_reason": "Priority"}})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    drive(rig, [lambda t: obs("PENDING", t, reason="Priority")] * 3 + [lambda t: obs("COMPLETED", t)])
    rig.runner.run()
    assert slurm.live_calls == 3, "a host writes into the comment; without it in the observation the job is asked"


def test_without_any_observation_the_runner_waits_for_the_controller_then_polls_slowly(rig):
    rig.monkeypatch.setattr(runmod, "STALE_GRACE_S", 90)
    rig.monkeypatch.setattr(runmod, "STALE_POLL_S", 120)
    slurm = CountingSlurm({"5001": [acc("PENDING")] * 12 + [acc("COMPLETED")]})
    rig.make(slurm)
    drive(rig, [])           # the controller never reports
    seen = []
    orig = slurm.accounting

    def acc_at(jid):
        seen.append(rig.clock["t"] - T0)
        return orig(jid)
    slurm.accounting = acc_at
    rig.runner.run()
    # nothing in the first 90 s, then one read per 120 s (ticks are 30 s apart)
    assert seen[0] >= 90 and all(b - a >= 120 for a, b in zip(seen, seen[1:])), seen


def test_a_stale_observation_means_the_controller_is_down_and_the_runner_polls_slowly(rig):
    rig.monkeypatch.setattr(runmod, "STALE_POLL_S", 120)
    slurm = CountingSlurm({"5001": [acc("RUNNING")] * 6 + [acc("COMPLETED")]})
    rig.make(slurm)
    drive(rig, [lambda t: obs("RUNNING", t)] + [None] * 30)    # one fresh reading, then silence
    seen = []
    orig = slurm.accounting

    def acc_at(jid):
        seen.append(rig.clock["t"] - T0)
        return orig(jid)
    slurm.accounting = acc_at
    rig.runner.run()
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"]
    after_stale = [t for t in seen if t > 90]
    assert after_stale and all(b - a >= 120 for a, b in zip(after_stale, after_stale[1:])), seen


def test_an_observation_of_another_site_job_is_ignored(rig):
    rig.monkeypatch.setattr(runmod, "STALE_GRACE_S", 90)
    slurm = CountingSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    drive(rig, [lambda t: obs("RUNNING", t, jid="4999")] * 2)
    rig.runner.run()
    assert rig.clock["t"] - T0 >= 90, "waited for an observation of its own job before polling itself"


def test_a_job_the_site_no_longer_lists_is_read_from_accounting(rig):
    slurm = CountingSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    drive(rig, [lambda t: obs("", t)])
    rig.runner.run()
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"]


def test_the_end_of_an_observed_job_is_taken_from_its_accounting_record(rig):
    slurm = CountingSlurm({"5001": [acc("PENDING"), acc("FAILED", rc=2, comment="err2:boom|")]})
    rig.make(slurm)
    drive(rig, [lambda t: obs("PENDING", t), lambda t: obs("FAILED", t)])
    rig.runner.run()
    assert rig.kube.run["exitCode"] == 2 and rig.kube.run["endTime"] == "2025-11-05T01:00:00Z"


def test_a_cancel_and_a_submit_still_go_to_the_site(rig):
    slurm = CountingSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    drive(rig, [lambda t: obs("COMPLETED", t)])
    rig.runner.run()
    assert len(slurm.submitted) == 1, "submit is the runner's own call; cancel on SIGTERM is covered by the sigterm tests"


# ---------------------------------------------------------------- the site's submit cap (PRD-HPC-1461 AC-39)

class CappedSlurm(LaneSlurm):
    """Refuses with the given texts and reports `queued` of the user's jobs as queued or running."""
    queued = 5

    def active_count(self, user):
        return self.queued


SUBMIT_CAP = "HTTP 500 {'errors': [{'error': 'QOSMaxSubmitJobPerUserLimit'}]}"


def test_a_submit_refused_for_the_site_cap_ends_the_attempt_submit_limit_with_the_cap_and_the_count(rig, capsys):
    slurm = CappedSlurm([SUBMIT_CAP])
    rig.make(slurm, HPC_MAX_SUBMIT_JOBS="5")
    with pytest.raises(SystemExit) as e:
        rig.runner.run()
    assert e.value.code == 0 and len(slurm.submitted) == 1
    run = rig.kube.run
    assert run["state"] == "FAILED" and run["exitCode"] == 238 and run["terminal"] is True
    assert "at most 5" in run["message"] and "5 are already queued or running" in run["message"]
    assert not run.get("siteJobID") and "SubmitLimit" in capsys.readouterr().out


def test_a_cap_refusal_on_a_lane_is_first_retried_on_the_class_qos(rig):
    slurm = CappedSlurm([SUBMIT_CAP], jobs={"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_QOS="test", HPC_MAX_SUBMIT_JOBS="5")
    assert rig.runner.run() == 0
    assert [s["job"]["qos"] for s in slurm.submitted] == ["test", "default"]


def test_a_cap_refusal_without_a_known_cap_or_count_is_still_submit_limit(rig):
    class Blind(CappedSlurm):
        def active_count(self, user):
            raise OSError("site silent")
    slurm = Blind([SUBMIT_CAP])
    rig.make(slurm)
    with pytest.raises(SystemExit):
        rig.runner.run()
    assert rig.kube.run["exitCode"] == 238 and "a fixed number of" in rig.kube.run["message"]


def test_the_generic_accounting_policy_refusal_is_not_called_a_cap(rig):
    slurm = CappedSlurm(["HTTP 500 Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"])
    rig.make(slurm)
    with pytest.raises(SystemExit) as e:
        rig.runner.run()
    assert e.value.code == 1 and rig.kube.run.get("exitCode") != 238


def test_only_the_cap_texts_are_matched_as_submit_limit(rig):
    assert slurmmod.submit_limit_refusal("QOSMaxSubmitJobPerUserLimit")
    assert slurmmod.submit_limit_refusal("AssocMaxSubmitJobLimit")
    assert not slurmmod.submit_limit_refusal("Invalid account or account/partition combination specified")
