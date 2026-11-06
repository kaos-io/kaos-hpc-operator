"""The runner's attempt end to end against fakes: Slurm REST, the Kubernetes API, lakeFS, the stager.

Ported from the waiter's behaviour (submit-or-adopt by key, pull / wallet / lost-host resubmits, failure comment,
checkpoint check, on_term cancel) and the phase-1 contract: one attempt per runner, report to the job's status,
re-attach by key, never submit a continuation.
"""
import json
import urllib.error
import urllib.request

import helpers
import pytest

from kaos_hpc_runner import logs as logsmod
from kaos_hpc_runner import reporter as rep
from kaos_hpc_runner import runner as runmod
from kaos_hpc_runner import slurm as slurmmod

WF = "0b7f6c1e-1111-2222-3333-444455556666"
NAME = "kaos-%s-model-training" % WF
CID = "ab" * 32


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

    class Rig:
        pass
    r = Rig()
    r.net, r.monkeypatch = net, monkeypatch

    def make(slurm, status=None, wallet="wallet.jwt", stager=None, **env):
        cfg = helpers.settings(**env)
        r.cfg, r.slurm = cfg, slurm
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
                   "startTime": "2025-11-05T00:00:00Z", "endTime": "2025-11-05T01:00:00Z"}
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
             KAOS_RESUME_REF=CID, KAOS_RESUME_PREFIX="hpc-checkpoints/%s/model-training/attempt-2/" % WF)
    rig.runner.run()
    job = slurm.submitted[0]["job"]
    env = dict(e.split("=", 1) for e in job["environment"])
    assert (job["name"], job["partition"], job["account"], job["qos"], job["time_limit"]) == \
        (NAME, "gpu", "p1", "default", 720)
    assert job["current_working_directory"] == "/home/u1" and job["requeue"] is True
    assert (env["KAOS_ATTEMPT"], env["KAOS_RESUMABLE"], env["KAOS_RESUME_REF"]) == ("3", "1", CID)
    assert env["KAOS_RESUME_PREFIX"].endswith("attempt-2/") and env["HPC_GPUS"] == "4"
    assert env["MLFLOW_TRACKING_TOKEN"] == "wallet.jwt" and env["LAKEFS_BEARER_TOKEN"] == "wallet.jwt"
    assert env["LAKEFS_ENDPOINT"] == "https://lakefs.example" and env["OUTPUTS_REPO"] == "outputs-yolo"
    assert env["CHECKPOINT_BUCKET"] == "b" and env["KUBECORE_RUN_ID"] == "run-1" and "CKPTOUT_B64" in env
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


# ---------------------------------------------------------------- sharing, leases

def test_a_sharing_gpu_job_is_a_host_with_an_offer_window_and_inputs_in_its_script(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="240")
    rig.runner.run()
    sub = slurm.submitted[0]
    job = sub["job"]
    assert job["name"] == "kaos-a1b2c3d4e5-%s-model-training" % WF
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
    summary = json.dumps({"dataset_commit": "c0ffee12"})
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, STEP_COMMAND=json.dumps(["x", "--info", summary]))
    monkeypatch.setattr(runmod.imagemod, "stage_image", lambda img, tok, st: "/scr/kaos/images/k.tar")
    rig.runner.run()
    key = runmod.imagemod.image_key(rig.cfg.image)
    assert slurm.submitted[0]["job"]["comment"] == "kaos-commit=c0ffee12 kaos-image=" + key
    env = dict(e.split("=", 1) for e in slurm.submitted[0]["job"]["environment"])
    assert env["IMAGE_STAGED"] == "/scr/kaos/images/k.tar" and env["KAOS_COMMIT"] == "c0ffee12"
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)           # no pin, nothing staged: no comment at all
    monkeypatch.setattr(runmod.imagemod, "stage_image", lambda img, tok, st: "")
    rig.runner.run()
    assert "comment" not in slurm.submitted[0]["job"]


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

def _ckpt_comment(attempt=1):
    return "kaos-commit=c0ffee12 kaos-ckpt=%s kaos-ckpt-path=hpc-checkpoints/%s/model-training/attempt-%d/" % (
        CID, WF, attempt)


def test_a_checkpoint_is_reported_and_the_runner_exits_without_submitting_the_continuation(rig):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment=_ckpt_comment())]})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20")
    rig.net.commits.add(CID)
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1, "one attempt per runner: the next job is the controller's decision (D-44)"
    assert rig.kube.run["exitCode"] == 85 and rig.kube.run["terminal"] is True
    assert rig.kube.run["checkpointRef"] == "lakefs://outputs-yolo/%s/hpc-checkpoints/%s/model-training/attempt-1/" % (CID, WF)
    assert "outputs" not in rig.kube.status


@pytest.mark.parametrize("lead, comment, commit_exists, why", [
    (0, _ckpt_comment(), True, "not declared resumable"),
    (20, "kaos-commit=c0ffee12", True, "did not reach the outputs repository"),
    (20, _ckpt_comment(attempt=2), True, "did not reach the outputs repository"),    # the previous attempt's
    (20, _ckpt_comment(), False, "is not in the outputs repository"),
])
def test_without_a_committed_checkpoint_of_this_attempt_no_ref_is_reported(rig, capsys, lead, comment, commit_exists, why):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment=comment)]})
    rig.make(slurm, HPC_RESUMABLE_LEAD=str(lead))
    if commit_exists:
        rig.net.commits.add(CID)
    assert rig.runner.run() == 0 and len(slurm.submitted) == 1
    assert rig.kube.run["terminal"] is True and "checkpointRef" not in rig.kube.run
    assert why in capsys.readouterr().out


def test_lagging_accounting_is_covered_by_slurmctld(rig):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment="kaos-commit=c0ffee12")]},
                      live={"5001": {"comment": _ckpt_comment()}})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20")
    rig.net.commits.add(CID)
    rig.runner.run()
    assert "checkpointRef" in rig.kube.run


def test_a_stopped_run_is_never_reported_with_a_checkpoint(rig):
    slurm = FakeSlurm({"5001": [acc("CANCELLED", rc=85, comment=_ckpt_comment())]})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20")
    rig.net.commits.add(CID)
    rig.runner.run()
    assert "checkpointRef" not in rig.kube.run and rig.kube.run["state"] == "CANCELLED"


def test_an_unreachable_lakefs_never_lets_the_step_continue(rig):
    slurm = FakeSlurm({"5001": [acc("FAILED", rc=85, comment=_ckpt_comment())]})
    rig.make(slurm, HPC_RESUMABLE_LEAD="20", wallet="")
    rig.runner.run()
    assert "checkpointRef" not in rig.kube.run


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


def test_a_queued_job_is_replaced_before_its_wallet_expires(rig, monkeypatch):
    slurm = FakeSlurm({"5001": [acc("PENDING")], "5002": [acc("COMPLETED")]})
    rig.make(slurm)
    exp = [1000.0]
    monkeypatch.setattr(runmod.wallet, "wallet_exp", lambda w: exp[0])
    monkeypatch.setattr(runmod.time, "time", lambda: 1000.0 - 600)        # 10 min before: inside the margin
    rig.runner.run()
    assert slurm.cancelled == ["5001"] and len(slurm.submitted) == 2 and rig.kube.run["siteJobID"] == "5002"


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

def test_sigterm_cancels_a_queued_job_and_reports_it(rig):
    slurm = FakeSlurm({}, live={"5001": {"job_state": ["PENDING"]}})
    r = rig.make(slurm)
    r.runner.jid = "5001"
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 143 and slurm.cancelled == ["5001"]
    assert r.kube.run["state"] == "CANCELLED" and r.kube.run["terminal"] is True


def test_sigterm_leaves_a_running_job_to_finish(rig):
    slurm = FakeSlurm({}, live={"5001": {"job_state": ["RUNNING"]}})
    r = rig.make(slurm)
    r.runner.jid = "5001"
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 143 and slurm.cancelled == [] and r.kube.run == {}


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
