"""The registry token reaches a job by a file, never by the job's environment, script, name or comment (PRD kaos#1911, T-18).

The runner mints the token (1 h) and hands it to the site's staging tool (the `registry` verb, staging contract 5), which
writes it into a Docker-config auth file in the project's private folder, `$SCR/kaos-tmp/reg-<job>/auth.json` (folder
0700, file 0600). The job script points the pull at that file with `--authfile <path>` (a path, not a secret), and removes
the folder as soon as the image is there, in its exit trap, and the next job's sweep removes one a killed job left.

Runner side: what the submitted job is made of (scanned with secretscan), what the staging tool is handed. Node side: the
whole script runs in bash against the stand-in node of test_step_contract (fake Apptainer, fake Slurm). The tool's own
half (modes, links, ownership of the folder it writes) is tested where the tool is: `images/meluxina-tokenmint/stager`.
No token is printed by any test: they are compared by value only.
"""
import io
import json
import os
import signal
import subprocess
import time
import urllib.request

import helpers
import pytest
import secretscan
import test_step_contract as tsc
from test_hardening import GAR, REGISTRY, WALLET, HSite, lakefs  # noqa: F401
from test_runner import FakeSlurm, NoStager, acc, rig  # noqa: F401

from kaos_hpc_runner import jobscript, staging
from kaos_hpc_runner import runner as runmod

HOST = GAR.partition("/")[0]
OTHER = "zot.example/proj/yolo:v0.0.0"


# ---------------------------------------------------------------- the runner's side: what the job is made of

def _runner(image, sharing=False):
    over = dict(IMAGE_REF=image)
    if sharing:
        over.update(HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    cfg = helpers.settings(**over)
    cfg.check_job()
    r = runmod.Runner(cfg, slurm=object(), stager=object(), reporter=object(), logs=object(), mint=lambda: WALLET)
    r.wallet = WALLET
    return r


@pytest.fixture
def no_workdir(monkeypatch):
    monkeypatch.setattr(runmod.wallet, "fetch_workdir", lambda image, token: "")


@pytest.mark.parametrize("sharing", [False, True], ids=["alone", "shared node"])
def test_the_rendered_job_holds_no_registry_token_anywhere(no_workdir, sharing):
    r = _runner(GAR, sharing)
    batch, env, commits, _ = r.site_job(REGISTRY, "")
    found = secretscan.surfaces(batch, env, comment=" ".join(commits))
    # The tracker's bearer is the one credential that still travels in the environment (docs/job-environment.md).
    secretscan.assert_clean(found, secrets=[REGISTRY], allowed_names={"MLFLOW_TRACKING_TOKEN"})
    names = [e.split("=", 1)[0] for e in env]
    assert "REG_TOKEN" not in names and "APPTAINER_DOCKER_PASSWORD" not in names
    told = "export KAOS_REG_AUTH=1" in batch if sharing else "KAOS_REG_AUTH=1" in env     # a shared node's script carries the variables
    assert told, "the job is told a file will come, not what is in it"


def test_the_scan_catches_what_it_is_for(no_workdir):
    r = _runner(GAR)
    batch, env, commits, _ = r.site_job(REGISTRY, "")
    leaky = secretscan.surfaces(batch, env + ["REG_TOKEN=" + REGISTRY, "NEW_API_KEY=abc"], comment="ya29." + "x" * 30)
    found = secretscan.scan(leaky, secrets=[REGISTRY], allowed_names={"MLFLOW_TRACKING_TOKEN"})
    assert any("env REG_TOKEN: holds a held credential value" in f for f in found)
    assert any("env NEW_API_KEY: a credential-named variable" in f for f in found)
    assert any("comment: holds the shape of a google access token" in f for f in found)
    assert not any(REGISTRY in f for f in found), "a finding never carries the value"


def test_a_host_that_may_not_have_the_token_gets_no_file_and_no_token(no_workdir):
    r = _runner(OTHER)
    _, env, _, _ = r.site_job(REGISTRY, "")
    assert not any(e.startswith("KAOS_REG_AUTH") for e in env)
    assert r.reg == "", "nothing to place on the site"


# ---------------------------------------------------------------- the runner hands the token to the staging tool

class RegStager(NoStager):
    def __init__(self, fail=0):
        self.registry, self.fail = [], fail

    def __call__(self, words, feed=None, quiet=False):
        if words[0] == "registry":
            self.registry.append((list(words), list(feed)))
            if self.fail:
                self.fail -= 1
                return 1, [{"event": "error", "error": "no"}]
            return 0, [{"event": "registry"}]
        return super().__call__(words, feed, quiet)


def _with_metadata_token(rig, token):
    orig = urllib.request.urlopen

    def urlopen(req, timeout=0, **kw):
        url = req.full_url if hasattr(req, "full_url") else req
        if "metadata.google.internal" in url:
            return io.BytesIO(json.dumps({"access_token": token}).encode())
        return orig(req, timeout, **kw)
    rig.monkeypatch.setattr(urllib.request, "urlopen", urlopen)


def _slurm():
    return FakeSlurm({"5001": [acc("PENDING", start=0), acc("RUNNING"), acc("COMPLETED")],
                      "5002": [acc("PENDING", start=0), acc("RUNNING"), acc("COMPLETED")]},
                     live={"5001": {"state_reason": "Priority", "job_state": ["PENDING"]}})


def test_the_token_goes_to_the_staging_tool_for_the_new_job_and_the_submit_carries_none(rig):
    stager = RegStager()
    slurm = _slurm()
    rig.make(slurm, stager=stager, IMAGE_REF=GAR)
    _with_metadata_token(rig, REGISTRY)
    assert rig.runner.run() == 0
    body = slurm.submitted[0]
    secretscan.assert_clean(secretscan.surfaces(body["script"], body["job"]["environment"], body["job"].get("comment", ""),
                                                body["job"]["name"]),
                            secrets=[REGISTRY], allowed_names={"MLFLOW_TRACKING_TOKEN"})
    assert len(stager.registry) == 1, "once per site job"
    words, feed = stager.registry[0]
    assert [str(w) for w in words] == ["registry", "--contract", str(staging.CONTRACT), "--job", "5001"]
    assert REGISTRY not in " ".join(str(w) for w in words), "never on a command line"
    assert feed == [{"host": HOST, "token": REGISTRY}], "on the tool's standard input"


def test_a_placement_the_tool_refused_is_asked_again_at_the_next_poll_and_never_logged(rig, capsys):
    stager = RegStager(fail=2)
    rig.make(_slurm(), stager=stager, IMAGE_REF=GAR)
    _with_metadata_token(rig, REGISTRY)
    assert rig.runner.run() == 0
    assert len(stager.registry) == 3, "two refusals, then placed"
    assert REGISTRY not in capsys.readouterr().out


def test_a_replacement_job_gets_a_fresh_token_in_its_own_file(rig):
    stager = RegStager()
    rig.make(_slurm(), stager=stager, IMAGE_REF=GAR)
    _with_metadata_token(rig, REGISTRY)
    rig.runner.submit()
    rig.runner.jid = 5001
    rig.runner.keep_key()
    _with_metadata_token(rig, REGISTRY + "-fresh")
    rig.runner.jid = 5002
    rig.runner.submit()
    rig.runner.keep_key()
    assert [(str(w[4]), f[0]["token"]) for w, f in stager.registry] == [("5001", REGISTRY), ("5002", REGISTRY + "-fresh")]


def test_the_log_stream_and_the_tools_stderr_redact_the_token(rig):
    rig.make(_slurm(), stager=RegStager(), IMAGE_REF=GAR)
    _with_metadata_token(rig, REGISTRY)
    rig.runner.submit()
    assert REGISTRY in rig.runner.held_secrets()


# ---------------------------------------------------------------- the node's side: the script reads the file

def _reg_dir(site, jid="5001"):
    return site.scratch / "kaos-tmp" / ("reg-" + jid)


def _pull(site):
    """What the fake Apptainer's pull was given: {'authfile': path, 'content': ..., environment...}."""
    out = {}
    for ln in (site.tmp / "pull.log").read_text().splitlines():
        k, _, v = ln.partition("=")
        out.setdefault(k, v)
    return out


def _run(tmp_path, lakefs, monkeypatch, shared=False, place=True, **kw):
    site = HSite(tmp_path, lakefs, shared, monkeypatch, **kw)
    site.reg_placed = place
    return site


@tsc.MODES
def test_the_pull_is_pointed_at_the_file_and_holds_no_token_in_its_environment(tmp_path, lakefs, monkeypatch, shared):
    site = _run(tmp_path, lakefs, monkeypatch, shared)
    assert site.run(extra={"FAKE_PULL_LOG": str(tmp_path / "pull.log"), "KAOS_REG_WAIT_S": "2"}) == 0, site.out
    got = _pull(site)
    assert got["authfile"] == str(_reg_dir(site) / "auth.json"), "a path"
    import base64
    assert base64.b64encode(("oauth2accesstoken:" + REGISTRY).encode()).decode() in got["content"], "the file held the credential"
    env_dump = (tmp_path / "pull.log").read_text().split("content=", 1)[1].split("\n", 1)[1]
    assert REGISTRY not in env_dump and "REG_TOKEN" not in env_dump and "APPTAINER_DOCKER_PASSWORD" not in env_dump
    assert REGISTRY not in site.out


@tsc.MODES
def test_the_file_is_gone_when_the_job_ends(tmp_path, lakefs, monkeypatch, shared):
    site = _run(tmp_path, lakefs, monkeypatch, shared)
    assert site.run(extra={"KAOS_REG_WAIT_S": "2"}) == 0, site.out
    assert not _reg_dir(site).exists()
    assert site.leaked() == []


def test_the_file_is_gone_when_the_step_fails(tmp_path, lakefs, monkeypatch):
    site = _run(tmp_path, lakefs, monkeypatch, does="fail")
    assert site.run(extra={"KAOS_REG_WAIT_S": "2"}) == 3, site.out
    assert not _reg_dir(site).exists() and site.leaked() == []


def test_the_file_is_gone_when_the_pull_fails(tmp_path, lakefs, monkeypatch):
    site = _run(tmp_path, lakefs, monkeypatch)
    (tmp_path / "bin" / "apptainer").write_text("#!/bin/bash\n[ \"$1\" = pull ] && exit 1\nexit 0\n")
    assert site.run(extra={"KAOS_REG_WAIT_S": "2"}) == 231, site.out
    assert not _reg_dir(site).exists() and site.leaked() == []


def test_the_file_is_gone_when_the_job_is_cancelled_in_the_pull(tmp_path, lakefs, monkeypatch):
    site = _run(tmp_path, lakefs, monkeypatch)
    up = tmp_path / "pull.up"
    p = site.start(extra={"FAKE_PULL_SLEEP": str(up), "KAOS_REG_WAIT_S": "2"})
    deadline = time.time() + 60
    while not up.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert up.exists(), "the pull never started"
    assert (_reg_dir(site) / "auth.json").exists(), "the file is there while the pull needs it"
    os.killpg(p.pid, signal.SIGTERM)
    site.out = p.communicate(timeout=60)[0].decode(errors="replace")
    assert p.returncode == 143
    assert not _reg_dir(site).exists() and site.leaked() == []


def test_a_cached_image_needs_no_file_and_a_late_file_goes_with_the_job(tmp_path, lakefs, monkeypatch):
    site = _run(tmp_path, lakefs, monkeypatch, place=False)
    (site.scratch / "sif-cache").mkdir(parents=True)
    import hashlib
    (site.scratch / "sif-cache" / (hashlib.sha256(GAR.encode()).hexdigest()[:16] + ".sif")).write_text("sif")
    start = time.time()
    assert site.run(extra={"KAOS_REG_WAIT_S": "30"}) == 0, site.out
    assert time.time() - start < 25, "no pull, no wait for a file"


@pytest.mark.parametrize("how", ["loose folder", "linked folder", "linked file"])
def test_a_folder_or_file_that_is_not_the_job_users_own_private_one_is_never_read(tmp_path, lakefs, monkeypatch, how):
    site = _run(tmp_path, lakefs, monkeypatch, place=False)
    reg = _reg_dir(site)
    (tmp_path / "elsewhere").mkdir()
    real = helpers.write_registry_file(tmp_path / "elsewhere", HOST, REGISTRY)
    reg.parent.mkdir(parents=True, exist_ok=True)
    if how == "loose folder":
        helpers.write_registry_file(reg, HOST, REGISTRY)
        reg.chmod(0o770)
    elif how == "linked folder":
        reg.symlink_to(tmp_path / "elsewhere")
    else:
        reg.mkdir(mode=0o700)
        (reg / "auth.json").symlink_to(real)
    assert site.run(extra={"FAKE_PULL_LOG": str(tmp_path / "pull.log"), "KAOS_REG_WAIT_S": "2"}) == 231, site.out
    assert "not read" in site.out and REGISTRY not in site.out
    log = tmp_path / "pull.log"
    assert not log.exists() or "authfile=\n" in log.read_text(), "the pull was never pointed at it"
    assert os.path.exists(real), "what the link pointed at is untouched"


def test_a_file_that_never_comes_means_an_anonymous_pull_after_the_wait(tmp_path, lakefs, monkeypatch):
    site = _run(tmp_path, lakefs, monkeypatch, place=False)
    assert site.run(extra={"FAKE_PULL_LOG": str(tmp_path / "pull.log"), "KAOS_REG_WAIT_S": "2"}) == 0, site.out
    assert _pull(site)["authfile"] == ""
    assert "registry credentials were not placed" in site.out


def test_the_sweep_removes_the_folder_of_a_job_that_is_over_and_only_that(tmp_path):
    scr = tmp_path / "scr"
    for name in ("reg-100", "reg-200", "reg-x", "reg-", "reg-12a", "key-100"):
        (scr / "kaos-tmp" / name).mkdir(parents=True)
        (scr / "kaos-tmp" / name / "auth.json").write_text("{}")
    old = time.time() - 3600
    for d in (scr / "kaos-tmp").iterdir():
        os.utime(d, (old, old))
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "squeue").write_text('#!/bin/bash\nprintf "300 RUNNING\\n200 PENDING\\n"\n')
    (tmp_path / "bin" / "squeue").chmod(0o755)
    script = "\n".join(jobscript.SWEEP_HELPER + ['KAOS_SWEEP_SELF=300 kaos_sweep "$1"'])
    env = dict(os.environ, PATH=str(tmp_path / "bin") + os.pathsep + os.environ["PATH"], USER=os.environ.get("USER", "u"))
    subprocess.run(["bash", "-c", script, "x", str(scr)], env=env, check=True)
    assert sorted(p.name for p in (scr / "kaos-tmp").iterdir() if p.name.startswith("reg-")) == ["reg-", "reg-12a", "reg-200", "reg-x"]
    assert not (scr / "kaos-tmp" / "key-100").exists(), "the sweep still does what it did"


def test_the_whole_script_text_is_clean_of_secret_shapes():
    for sharing in ("-", "a1b2c3d4e5"):
        secretscan.assert_clean({"script": "\n".join(jobscript.batch_lines(sharing, "/scr/kaos/p1"))})
