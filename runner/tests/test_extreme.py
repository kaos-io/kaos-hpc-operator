"""Extreme cases of the runner (docs/extreme-cases.md): long site outages, a lost submit answer, a purged
site job, a rotated or expired JWT, lost shared-node hosts, an unreachable stager, a hung GPU step.

Every test runs against the fakes of test_runner.py / test_staging.py (no cluster, no site, no sleeping).
"""
import io
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import helpers
import pytest
from test_runner import NAME, WF, FakeSlurm, acc, rig  # noqa: F401  (rig is a fixture)

from kaos_hpc_runner import runner as runmod
from kaos_hpc_runner import slurm as slurmmod
from kaos_hpc_runner import staging

ROOT = Path(__file__).resolve().parents[1]


class Stop(BaseException):
    """Ends a poll loop that would never end by itself (BaseException: the loop swallows Exception)."""


def stop_after(runner, ticks):
    """Make the runner's sleep raise Stop on the given tick."""
    n = [0]

    def sleep(_):
        n[0] += 1
        if n[0] >= ticks:
            raise Stop()
    runner.sleep = sleep
    return n


# ------------------------------------------------------------------------------------------------
# 2. Site outage: REST 5xx / 401 / timeouts / non-JSON for a long time

OUTAGES = [
    ("http_502", urllib.error.HTTPError("u", 502, "bad gateway", {}, io.BytesIO(b"<html>"))),
    ("http_503", urllib.error.HTTPError("u", 503, "unavailable", {}, io.BytesIO(b""))),
    ("http_429", urllib.error.HTTPError("u", 429, "slow down", {}, io.BytesIO(b"{}"))),
    ("http_401_expired_jwt", urllib.error.HTTPError("u", 401, "unauthorized", {}, io.BytesIO(b"{}"))),
    ("socket_timeout", socket.timeout("timed out")),
    ("connection_reset", ConnectionResetError("reset by peer")),
    ("url_error", urllib.error.URLError("name resolution failed")),
    ("non_json_with_http_200", json.JSONDecodeError("Expecting value", "<html>blocked</html>", 0)),
]


class Outage(FakeSlurm):
    """accounting() raises `err` for `down` consecutive calls, then answers from the records."""

    def __init__(self, jobs, err, down, **kw):
        super().__init__(jobs, **kw)
        self.err, self.down = err, down

    def accounting(self, jid):
        if self.down > 0:
            self.down -= 1
            raise self.err
        return super().accounting(jid)


@pytest.mark.parametrize("name,err", OUTAGES, ids=[o[0] for o in OUTAGES])
@pytest.mark.parametrize("when", ["while_queued", "while_running"])
def test_should_ride_out_a_long_site_outage_without_failing_resubmitting_or_cancelling(rig, capsys, name, err, when):
    """300 polls (2.5 h at 30 s) of a dead REST API: not one submit, not one cancel, not one terminal report;
    when the site is back the runner reports the real end."""
    first = acc("PENDING") if when == "while_queued" else acc("RUNNING")
    slurm = Outage({"5001": [first, acc("RUNNING"), acc("COMPLETED")]}, err, down=300)
    rig.make(slurm)
    rig.runner.jid = None
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1, "an outage never makes the runner submit a second job"
    assert slurm.cancelled == [], "an outage never makes the runner cancel the live site job"
    terminal = [p for p in rig.kube.patches() if (p[2]["status"].get("run") or {}).get("terminal")]
    assert len(terminal) == 1 and rig.kube.run["state"] == "COMPLETED"
    assert capsys.readouterr().out.count("poll error (transient)") == 300


def test_should_not_end_a_run_by_itself_when_the_site_never_comes_back(rig):
    """There is no give-up in the runner: the outage ends only with the runner Job's deadline (wall time +
    8 h queue allowance + 30 min), which the controller turns into Failed/RunnerFailed. Pinned so a future
    change to that contract is a decision, not an accident."""
    slurm = Outage({"5001": [acc("RUNNING")]}, OSError("down"), down=10**9)
    rig.make(slurm)
    stop_after(rig.runner, 2000)
    with pytest.raises(Stop):
        rig.runner.run()
    assert len(slurm.submitted) == 1 and slurm.cancelled == []
    assert not [p for p in rig.kube.patches() if (p[2]["status"].get("run") or {}).get("terminal")]


def test_should_never_resubmit_or_report_over_a_site_job_the_site_has_purged(rig):
    """The site answers 404 for the job in both slurmctld and slurmdbd (purged): the runner keeps asking, never
    submits another job for the attempt and never invents a terminal state."""
    slurm = FakeSlurm({})                                   # accounting raises 404 for every id
    slurm.next_id = 5000
    rig.make(slurm)
    stop_after(rig.runner, 500)
    with pytest.raises(Stop):
        rig.runner.run()
    assert len(slurm.submitted) == 1 and slurm.cancelled == []
    assert not [p for p in rig.kube.patches() if (p[2]["status"].get("run") or {}).get("terminal")]


def test_should_adopt_instead_of_resubmitting_when_the_submit_answer_was_lost(rig):
    """The site created the job but the answer timed out. The same runner asks again (the site may be down), and
    its adopt step finds the live job by name: exactly one site job exists."""
    class LostAnswer(FakeSlurm):
        def submit(self, body):
            self.submitted.append(body)
            self.active = "5001"
            self.jobs["5001"] = [acc("COMPLETED")]
            raise socket.timeout("timed out reading the answer")
    slurm = LostAnswer({})
    rig.make(slurm)
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1, "the runner adopted the job it found by key instead of submitting again"
    assert rig.kube.run["siteJobID"] == "5001" and rig.kube.run["state"] == "COMPLETED"


def test_should_adopt_the_finished_job_when_the_submit_answer_was_lost_and_the_job_ended_before_the_retry(rig):
    """The scale test's duplicate (2 in ~2,700): the site accepted the job, the answer was lost, the job ended before
    the runner came back, so no live job exists. The job is found in accounting by its NAME (which carries the HPC
    job and the attempt) and adopted: still one site job."""
    class LostAnswerThenDone(FakeSlurm):
        def submit(self, body):
            self.submitted.append(body)
            assert body["job"]["name"] == rig.cfg.job_site_name
            self.jobs["5001"] = [dict(acc("COMPLETED"), name=body["job"]["name"])]
            self.finished = ("5001",)               # slurmctld has forgotten it; slurmdbd lists it
            raise socket.timeout("timed out reading the answer")
    slurm = LostAnswerThenDone({})
    rig.make(slurm)
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1, "no second submission of the same attempt"
    assert rig.kube.run["siteJobID"] == "5001" and rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"]


@pytest.mark.parametrize("other", [
    {"name": "kaos-%s-model-training-01234567-a2" % WF},        # another attempt of the same HPC job
    {"name": "kaos-%s-model-training-89abcdef-a1" % WF},        # the same step re-run by Argo under another HPC job
    {"state": {"current": ["CANCELLED"]}},                      # cancelled by this runner for a resubmit
])
def test_should_not_adopt_a_finished_job_that_is_not_this_attempts(rig, other):
    slurm = FakeSlurm({"4001": [dict(acc("COMPLETED"), **other)], "5001": [acc("COMPLETED")]})
    slurm.finished = ("4001",)
    slurm.next_id = 5000
    rig.make(slurm)
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1 and rig.kube.run["siteJobID"] == "5001"


def test_should_adopt_only_a_finished_job_newer_than_the_one_already_reported(rig):
    """A job the runner replaced (resubmit) must not come back: only ids above the reported one count."""
    slurm = FakeSlurm({"4001": [acc("FAILED")], "5001": [acc("COMPLETED")]})
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "4002", "state": "PENDING"}})
    slurm.jobs["4001"] = [dict(acc("FAILED"), name=rig.cfg.job_site_name)]
    slurm.finished = ("4001",)
    slurm.next_id = 5000
    rig.runner.run()
    assert len(slurm.submitted) == 1 and rig.kube.run["siteJobID"] == "5001"


def test_should_read_the_accounting_search_by_name_as_unavailable_not_as_empty_when_the_site_is_down(monkeypatch):
    """A site that cannot answer the search must raise, never read as 'no such job' (that would submit twice)."""
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")

    def urlopen(req, timeout=0, **kw):
        raise urllib.error.HTTPError(req.full_url, 503, "x", {}, io.BytesIO(b"{}"))
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError):
        s.find_finished()


def test_should_ask_slurmdb_by_the_attempts_name_within_the_lookback_window(monkeypatch):
    seen = []
    cfg = helpers.settings()
    s = slurmmod.Slurm(cfg, token_file="/nonexistent")
    monkeypatch.setattr(s, "get", lambda url: seen.append(url) or {"jobs": []})
    assert s.find_finished(now=lambda: 2_000_000_000) is None
    assert seen == ["%s/jobs?job_name=%s&start_time=%d" % (cfg.sdb, cfg.job_site_name,
                                                          2_000_000_000 - slurmmod.FINISHED_LOOKBACK_S)]


@pytest.mark.parametrize("code", [500, 502, 503, 504, 429])
def test_should_read_a_gateway_or_nameless_5xx_at_submit_as_a_site_that_is_unavailable(monkeypatch, code):
    """A gateway error, or a 500 that names no reason, says the site is down, not that it refused the job (AC-30)."""
    def urlopen(req, timeout=0, **kw):
        raise urllib.error.HTTPError(req.full_url, code, "x", {}, io.BytesIO(b"<html>gateway</html>"))
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(slurmmod.SiteUnavailable) as e:
        s.submit({"job": {}})
    assert "HTTP %d" % code in str(e.value) and slurmmod.is_site_down(e.value)


@pytest.mark.parametrize("code,body", [
    (500, {"errors": [{"error": "Invalid account or account/partition combination specified"}]}),
    (500, {"errors": [{"error": "Requested time limit is invalid (missing or exceeds some limit)"}]}),
    (400, {"errors": [{"error": "bad request"}]}), (401, {}), (403, {})])
def test_should_still_fail_at_once_on_a_real_refusal_or_an_auth_error(monkeypatch, code, body):
    def urlopen(req, timeout=0, **kw):
        raise urllib.error.HTTPError(req.full_url, code, "x", {}, io.BytesIO(json.dumps(body).encode()))
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(slurmmod.Refused) as e:
        s.submit({"job": {}})
    assert not slurmmod.is_site_down(e.value)


def test_should_read_a_500_that_names_a_dead_slurmctld_as_unavailable(monkeypatch):
    def urlopen(req, timeout=0, **kw):
        raise urllib.error.HTTPError(req.full_url, 500, "x", {}, io.BytesIO(
            b'{"errors": [{"error": "Unable to contact slurm controller (connect failure)"}]}'))
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(slurmmod.SiteUnavailable):
        s.submit({"job": {}})


@pytest.mark.parametrize("err,down", [
    (socket.timeout("t"), True), (ConnectionRefusedError("r"), True), (urllib.error.URLError("dns"), True),
    (urllib.error.HTTPError("u", 503, "x", {}, None), True), (urllib.error.HTTPError("u", 502, "x", {}, None), True),
    (urllib.error.HTTPError("u", 401, "x", {}, None), False), (urllib.error.HTTPError("u", 404, "x", {}, None), False),
    (urllib.error.HTTPError("u", 422, "x", {}, None), False), (ValueError("not json"), False),
    (PermissionError("local"), False)])
def test_should_tell_an_unreachable_site_from_a_real_error(err, down):
    assert slurmmod.is_site_down(err) is down


class SiteDownAtStart(FakeSlurm):
    """The site answers nothing for the first `fails` calls of find_active, then behaves."""

    def __init__(self, fails, err, *a, **kw):
        super().__init__(*a, **kw)
        self.fails, self.err, self.asked = fails, err, 0

    def find_active(self):
        self.asked += 1
        if self.asked <= self.fails:
            raise self.err
        return super().find_active()


def test_should_wait_for_an_unreachable_site_and_then_submit_without_failing(rig):
    """AC-30 / AC-28: ten minutes of connection errors are a wait: no failure, one submission, the reason shown
    while it lasts and cleared when the site answers."""
    slurm = SiteDownAtStart(14, urllib.error.URLError("connection refused"), {"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    sleeps = []
    rig.runner.sleep = sleeps.append
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1 and rig.kube.run["state"] == "COMPLETED"
    assert sum(sleeps) >= 600, "the outage was waited out, not skipped"
    reasons = [p[2]["status"]["pendingReason"] for p in rig.kube.patches() if "pendingReason" in p[2]["status"]]
    assert reasons[0] == "SiteUnavailable" and reasons[1] is None


def test_should_back_off_between_asks_up_to_a_ceiling(rig):
    slurm = SiteDownAtStart(12, socket.timeout("t"), {"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    sleeps = []
    rig.runner.sleep = sleeps.append
    rig.runner.run()
    waits = sleeps[:12]
    assert waits[:4] == [5, 10, 20, 40] and max(waits) == runmod.SITE_RETRY_MAX_S


def test_should_fail_with_a_clear_reason_when_the_site_stays_down_past_the_bound(rig):
    slurm = SiteDownAtStart(10 ** 6, urllib.error.URLError("connection refused"), {})
    rig.make(slurm, HPC_SITE_WAIT_MAX_S="300")
    rig.runner.sleep = lambda s: None
    with pytest.raises(SystemExit) as e:
        rig.runner.run()
    assert e.value.code == 1 and slurm.submitted == []


def test_should_not_wait_on_an_auth_error_or_an_unreadable_reply(rig):
    for err in (urllib.error.HTTPError("u", 401, "x", {}, None), ValueError("not JSON")):
        slurm = SiteDownAtStart(1, err, {})
        rig.make(slurm)
        sleeps = []
        rig.runner.sleep = sleeps.append
        with pytest.raises(type(err)):
            rig.runner.run()
        assert sleeps == [], "a real error is not waited for"


def test_should_not_submit_a_second_job_when_the_outage_swallows_the_answer_and_then_lifts(rig):
    """The submit POST times out after the site created the job, then the site is down for two more asks, then back
    with the job live: one site job."""
    class LostThenDown(FakeSlurm):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.lookups = 0

        def find_active(self):
            self.lookups += 1
            if self.lookups in (2, 3):                 # the asks right after the lost answer meet the outage
                raise socket.timeout("down")
            return self.active

        def submit(self, body):
            self.submitted.append(body)
            self.active = "5001"
            raise socket.timeout("lost")
    slurm = LostThenDown({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner.sleep = lambda s: None
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1 and rig.kube.run["siteJobID"] == "5001"


def test_should_use_the_rotated_jwt_on_the_very_next_request_after_a_401(tmp_path, monkeypatch):
    """The platform rotates the Secret every 25 min (60 min lifetime). A request with the expired token gets
    401; the next request reads the file afresh and succeeds. The token never appears in a log line."""
    f = tmp_path / "token"
    f.write_text("expired-jwt\n")
    s = slurmmod.Slurm(helpers.settings(), token_file=str(f))
    seen = []

    def urlopen(req, timeout=0, **kw):
        tok = req.get_header("X-slurm-user-token")
        seen.append(tok)
        if tok != "fresh-jwt":
            raise urllib.error.HTTPError(req.full_url, 401, "unauthorized", {}, io.BytesIO(b"{}"))
        return io.BytesIO(json.dumps({"jobs": [{"job_id": 7, "state": {"current": ["RUNNING"]}}]}).encode())
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError):
        s.accounting("7")
    f.write_text("fresh-jwt\n")                              # the Secret rotates under the running pod
    assert s.accounting("7")["state"]["current"] == ["RUNNING"]
    assert seen == ["expired-jwt", "fresh-jwt"]


def test_should_keep_a_queued_job_through_a_jwt_that_is_401_until_it_rotates(rig, monkeypatch):
    """Runner-level: 401 for 40 ticks (the Secret not rotated yet), then fine: no submit, no cancel, no failure."""
    slurm = Outage({"5001": [acc("PENDING"), acc("COMPLETED")]},
                   urllib.error.HTTPError("u", 401, "expired", {}, io.BytesIO(b"{}")), down=40)
    rig.make(slurm)
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1 and slurm.cancelled == [] and rig.kube.run["state"] == "COMPLETED"


# ------------------------------------------------------------------------------------------------
# 4. Node sharing: the host is lost

@pytest.mark.parametrize("state", ["NODE_FAIL", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "PREEMPTED", "FAILED",
                                   "COMPLETED"])
def test_should_start_a_taken_guest_over_when_its_host_ended_in_any_terminal_state(rig, state):
    taken = acc("PENDING", comment="kaos-state=taken host=4000 slot=0 attempt=1")
    slurm = FakeSlurm({"5001": [taken], "5002": [acc("COMPLETED")], "4000": [acc(state)]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert slurm.cancelled == ["5001"], "the held job of the dead host is cancelled, once"
    assert len(slurm.submitted) == 2 and rig.kube.run["siteJobID"] == "5002" and rig.kube.run["terminal"]


@pytest.mark.parametrize("state", ["RUNNING", "COMPLETING", "SUSPENDED", "PENDING"])
def test_should_leave_a_taken_guest_alone_while_its_host_is_alive(rig, state):
    taken = acc("PENDING", comment="kaos-state=taken host=4000 slot=0 attempt=1")
    done = acc("RUNNING", comment="kaos-state=done rc=0 host=4000")
    slurm = FakeSlurm({"5001": [taken, taken, done], "4000": [acc(state)]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"]


def test_should_leave_a_taken_guest_alone_when_the_host_record_cannot_be_read(rig):
    taken = acc("PENDING", comment="kaos-state=taken host=4000 slot=0 attempt=1")
    done = acc("RUNNING", comment="kaos-state=done rc=0 host=4000")
    slurm = FakeSlurm({"5001": [taken, taken, done]})        # no record of host 4000: accounting raises 404
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1


def test_should_start_over_at_most_three_times_when_hosts_keep_dying(rig):
    """Each new job is taken by a host that dies too. After LOST_HOST_RESUBMITS the runner stops resubmitting
    (no cancel/submit storm) and keeps watching the last job; the Job deadline is the only way out then."""
    jobs, hosts = {}, {}
    for i in range(1, 6):
        jobs[str(5000 + i)] = [acc("PENDING", comment="kaos-state=taken host=%d slot=0 attempt=1" % (4000 + i))]
        hosts[str(4000 + i)] = [acc("NODE_FAIL")]
    slurm = FakeSlurm({**jobs, **hosts})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    stop_after(rig.runner, 200)
    with pytest.raises(Stop):
        rig.runner.run()
    assert len(slurm.submitted) == 1 + runmod.LOST_HOST_RESUBMITS
    assert slurm.cancelled == ["5001", "5002", "5003"]


# ------------------------------------------------------------------------------------------------
# 2. Stager SSH unreachable

class _Unreachable:
    """A stager whose login node does not answer: every call exits 255 (ssh's own code)."""

    def __init__(self):
        self.calls = []

    def have_key(self):
        return True

    def __call__(self, words, feed=None, quiet=False):
        self.calls.append(words)
        return 255, []


def test_should_retry_an_unreachable_stager_with_bounded_waits_then_stage_in_job(monkeypatch):
    from test_staging import _LakeFS, _twin
    clock = [1000.0]
    sleeps = []
    stager = _Unreachable()
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    monkeypatch.setattr(staging.time, "time", lambda: clock[0])

    def sleep(s):
        sleeps.append(s)
        clock[0] += s
    monkeypatch.setattr(staging.time, "sleep", sleep)
    path = staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key)
    assert path == "", "nothing staged: the job stages in-job and the step is not failed"
    assert sleeps and max(sleeps) <= 600, "waits are capped at 10 min"
    assert sum(sleeps) <= staging.stage_budget(0) + 600, "the whole wait is bounded by the staging budget (4 h for a small dataset)"
    assert len(stager.calls) == len(sleeps) + 1 and len(stager.calls) < 60


def test_should_pull_the_image_in_job_when_the_stager_is_unreachable(monkeypatch):
    from kaos_hpc_runner import image as imagemod
    monkeypatch.setattr(imagemod, "ssh_present", lambda: True)
    stager = _Unreachable()
    assert imagemod.stage_image("zot.example/p/a:v0.0.0", "", stager) == ""
    assert len(stager.calls) == 1, "one probe, no retry storm"


def test_should_not_hang_or_raise_when_ssh_itself_cannot_connect(monkeypatch, tmp_path):
    """The real Stager with an `ssh` that refuses the connection (exit 255, like a dead login node)."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    ssh = bindir / "ssh"
    ssh.write_text("#!/bin/sh\necho 'ssh: connect to host login port 22: Connection refused' >&2\nexit 255\n")
    ssh.chmod(0o755)
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ["PATH"])
    st = staging.Stager(helpers.settings(), staging_dir=str(tmp_path))
    t0 = time.time()
    rc, events = st(["status", "--repo", "r"])
    st.close()
    assert rc == 255 and events == [] and time.time() - t0 < 30


# ------------------------------------------------------------------------------------------------
# 8. Idle watchdog

def _load_idlewatch():
    import importlib.util
    spec = importlib.util.spec_from_file_location("idlewatch_x", ROOT / "kaos_hpc_runner" / "payload" / "idlewatch.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_should_end_a_gpu_step_whose_gpu_is_idle_in_a_silent_cpu_phase_T24(tmp_path, monkeypatch):
    """PRD T-24, documented and accepted: on a GPU class only the step's own GPUs and its log count. A step that
    is busy on the CPU (here: a spinning process) with an idle GPU and a silent log is ended when the window
    is full, with the marker file the job script turns into exit 236."""
    iw = _load_idlewatch()
    monkeypatch.setattr(iw, "SAMPLE_S", 0.05)
    monkeypatch.setattr(iw, "GRACE_S", 0.05)
    real_probe = iw.GpuProbe
    monkeypatch.setattr(iw, "GpuProbe", lambda tokens: real_probe(tokens, query=lambda: [("0", "GPU-aaaa", 0)]))
    spin = subprocess.Popen([sys.executable, "-c", "while True: pass"], start_new_session=True)
    log, mark = tmp_path / "slurm-1.out", tmp_path / "mark"
    log.write_text("started\n")
    try:
        rc = iw.main([str(spin.pid), str(log), str(mark), "1", "1"],
                     env={"CUDA_VISIBLE_DEVICES": "0", "KAOS_IDLE_WINDOW_S": "0.3"})
        assert rc == 0 and mark.exists(), "the busy CPU did not save the step"
        assert spin.wait(timeout=5) in (-signal.SIGTERM, -signal.SIGKILL)
    finally:
        if spin.poll() is None:
            spin.kill()


def test_should_spare_a_gpu_step_that_keeps_logging_while_the_gpu_is_idle(tmp_path, monkeypatch):
    iw = _load_idlewatch()
    clock = [0.0]
    log = tmp_path / "slurm-1.out"
    log.write_text("")
    wd = iw.Watchdog(os.getpid(), iw.GpuProbe(["0"], query=lambda: [("0", "GPU-aaaa", 0)]), iw.LogWatch(str(log)), str(tmp_path / "mark"), 20,
                     clock=lambda: clock[0])
    for i in range(200):                                       # 100 min of one log line per 30 s sample
        clock[0] += 30
        with open(log, "a") as f:
            f.write("epoch %d\n" % i)
        assert wd.tick() is False


def test_should_start_no_watchdog_for_a_zero_window():
    iw = _load_idlewatch()
    called = []
    iw.GpuProbe = lambda *a, **k: called.append("gpu")
    iw.CpuProbe = lambda *a, **k: called.append("cpu")
    assert iw.main(["1", "/dev/null", "/dev/null", "0", "1"], env={}) == 0
    assert called == [], "idle_timeout=0 turns the watchdog off before it builds anything"


# ------------------------------------------------------------------------------------------------
# 2/9. Cancel while the site errs (PRD D-20: retried until the site confirms)

def _delete_urlopen(monkeypatch, outcomes):
    calls = []

    def urlopen(req, timeout=0, **kw):
        calls.append(req.get_method())
        o = outcomes[min(len(calls), len(outcomes)) - 1]
        if isinstance(o, Exception):
            raise o
        return io.BytesIO(b"{}")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return calls


def _http(code):
    return urllib.error.HTTPError("u", code, "x", {}, io.BytesIO(b"{}"))


def test_should_retry_a_cancel_the_site_answers_with_5xx_until_it_goes_through(monkeypatch):
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")
    sleeps = []
    calls = _delete_urlopen(monkeypatch, [_http(503), socket.timeout("t"), None])
    assert s.cancel("5001", sleep=sleeps.append) is True
    assert calls == ["DELETE"] * 3 and sleeps == [2, 4]


def test_should_give_up_a_cancel_after_a_bounded_backoff_and_say_the_job_may_live(monkeypatch, capsys):
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")
    sleeps = []
    calls = _delete_urlopen(monkeypatch, [_http(502)])
    assert s.cancel("5001", sleep=sleeps.append) is False
    assert len(calls) == slurmmod.CANCEL_TRIES and sum(sleeps) <= 30, "inside the pod's 60 s grace"
    assert "may still be alive" in capsys.readouterr().out


def test_should_not_retry_a_cancel_of_a_job_the_site_does_not_know(monkeypatch):
    s = slurmmod.Slurm(helpers.settings(), token_file="/nonexistent")
    sleeps = []
    calls = _delete_urlopen(monkeypatch, [_http(404)])
    assert s.cancel("5001", sleep=sleeps.append) is True and len(calls) == 1 and sleeps == []


# ------------------------------------------------------------------------------------------------
# 2/3/9. The orphan sweep's runner: --cancel-only (PRD AC-08, D-20)

class Sweepable(FakeSlurm):
    """cancel() ends the active job (accounting then shows it CANCELLED), like the site."""

    def cancel(self, jid):
        self.cancelled.append(str(jid))
        self.jobs[str(jid)] = [acc("CANCELLED", start=1762300800, end=1762301000)]
        self.active = None
        return True


def _ticking(runner, step=10):
    """A clock that only moves when the runner sleeps; returns the list of sleeps."""
    t = [1_000_000.0]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        t[0] += s
    runner.clock, runner.sleep = lambda: t[0], sleep
    return sleeps


def test_cancel_only_should_cancel_the_active_site_job_and_report_what_the_site_confirms(rig):
    slurm = Sweepable({"5001": [acc("RUNNING")]}, active="5001")
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "RUNNING"}})
    _ticking(rig.runner)
    assert rig.runner.cancel_only() == 0
    assert slurm.cancelled == ["5001"] and slurm.submitted == [], "it cancels and never submits"
    run = rig.kube.run
    assert run["terminal"] is True and run["state"] == "CANCELLED" and run["siteJobID"] == "5001"
    assert run["startTime"] == "2025-11-05T00:00:00Z" and run["endTime"] == "2025-11-05T00:03:20Z"
    assert run["attempt"] == 1


def test_cancel_only_should_report_a_job_that_ended_by_itself_without_cancelling_it(rig):
    slurm = Sweepable({"5001": [acc("COMPLETED")]}, active=None)
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "RUNNING"}})
    _ticking(rig.runner)
    assert rig.runner.cancel_only() == 0
    assert slurm.cancelled == [] and rig.kube.run["state"] == "COMPLETED" and rig.kube.run["terminal"] is True


def test_cancel_only_should_keep_asking_a_site_that_does_not_answer_then_stop_with_no_report(rig):
    class Dead(Sweepable):
        def find_active(self):
            raise socket.timeout("down")
    slurm = Dead({}, active=None)
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "RUNNING"}})
    sleeps = _ticking(rig.runner)
    with pytest.raises(SystemExit) as e:
        rig.runner.cancel_only()
    assert e.value.code == 1
    assert runmod.CANCEL_ONLY_BUDGET_S // runmod.CANCEL_ONLY_POLL_S - 2 <= len(sleeps) <= runmod.CANCEL_ONLY_BUDGET_S // runmod.CANCEL_ONLY_POLL_S + 1
    assert slurm.submitted == [] and not rig.kube.run.get("terminal"), "no terminal report without the site's word"


def test_cancel_only_should_call_a_job_the_site_has_forgotten_unknown_only_after_ten_minutes(rig):
    slurm = Sweepable({}, active=None)            # accounting answers 404 for 5001
    start = "2025-11-05T00:00:00Z"
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "RUNNING", "startTime": start}})
    sleeps = _ticking(rig.runner)
    assert rig.runner.cancel_only() == 0
    assert sum(sleeps) >= runmod.SITE_UNKNOWN_S, "not before the site has said so for 10 minutes"
    run = rig.kube.run
    assert run["state"] == "SITE_UNKNOWN" and run["terminal"] is True and run["startTime"] == start


def test_cancel_only_should_not_call_a_job_unknown_while_the_site_still_lists_it(rig):
    class Stubborn(Sweepable):
        n = 0

        def cancel(self, jid):                    # the site is slow: the job stays active a long time
            self.cancelled.append(str(jid))
            Stubborn.n += 1
            if Stubborn.n >= 100:
                return super().cancel(jid)
            return True
    slurm = Stubborn({"5001": [acc("RUNNING")]}, active="5001")
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "RUNNING"}})
    _ticking(rig.runner)
    assert rig.runner.cancel_only() == 0
    assert rig.kube.run["state"] == "CANCELLED" and len(slurm.cancelled) >= 100, "it kept cancelling until the site agreed"


def test_cancel_only_should_report_not_submitted_when_there_is_nothing_to_cancel(rig):
    slurm = Sweepable({}, active=None)
    rig.make(slurm, status={})
    _ticking(rig.runner)
    assert rig.runner.cancel_only() == 0
    assert rig.kube.run["state"] == "NOT_SUBMITTED" and rig.kube.run["terminal"] is True


def test_cancel_only_should_do_nothing_for_an_attempt_that_already_reported_its_end(rig):
    slurm = Sweepable({}, active="5001")
    rig.make(slurm, status={"run": {"attempt": 1, "siteJobID": "5001", "state": "COMPLETED", "terminal": True}})
    assert rig.runner.cancel_only() == 0
    assert slurm.cancelled == [] and rig.kube.patches() == []


def test_main_should_run_the_cancel_only_mode_with_the_flag(monkeypatch):
    seen = []
    monkeypatch.setattr(runmod.Runner, "cancel_only", lambda self: seen.append("cancel") or 0)
    monkeypatch.setattr(runmod.Runner, "run", lambda self: seen.append("run") or 0)
    assert runmod.main(dict(helpers.BASE_ENV), argv=["--cancel-only"]) == 0
    assert runmod.main(dict(helpers.BASE_ENV), argv=[]) == 0
    assert seen == ["cancel", "run"]


def test_sigterm_should_not_report_cancelled_when_the_site_did_not_take_the_cancel(rig):
    from test_runner import _term

    class Refusing(FakeSlurm):
        def cancel(self, jid):
            self.cancelled.append(str(jid))
            return False
    slurm = Refusing({}, live={"5001": {"job_state": ["RUNNING"]}})
    r = _term(rig, slurm, True)
    with pytest.raises(SystemExit) as e:
        r.runner.on_term(15, None)
    assert e.value.code == 143 and r.kube.run == {}, "no CANCELLED on the site's silence: the controller sweeps"
