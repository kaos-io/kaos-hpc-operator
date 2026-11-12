"""Hardening of the HPC job path before a client release (PRD kaos#1911): what a job script trusts, what the step
sees, what is left on the site, what a failed job shows.

1. The registry token is presented only to a host parsed out of the image reference, and a reference that does not
   parse never reaches a script.
2. The step container gets an allow-list of variables (`--cleanenv`); no token is written to a place that outlives
   the job; the registry token is unset once the image is there.
3. Everything the job creates is private to the site user.
4. The tail of a failed job's output is redacted before it is copied into the job comment.

The whole job script runs here in bash against the stand-in node of test_step_contract (a fake Apptainer, a fake
Slurm), so the script under test is the one the runner submits. No token is printed by any test: they are
compared by value only.
"""
import json
import os
import signal
import stat
import subprocess
import sys
import time
import urllib.request

import helpers
import pytest
import test_step_contract as tsc
from test_jobscript import FakeLakeFS
from test_runner import FakeSlurm, rig  # noqa: F401

from kaos_hpc_runner import jobscript, settings
from kaos_hpc_runner import runner as runmod

WALLET = "WALLET-SECRET-" + "w" * 24          # stands in for the data/MLflow bearer
REGISTRY = "REGISTRY-SECRET-" + "r" * 24       # stands in for the registry token
GAR = "europe-docker.pkg.dev/proj/yolo:v0.0.0"

# ---------------------------------------------------------------- 1. the reference

HOSTILE = [
    "evil.example/x -docker.pkg.dev/p/app:v1",          # a space
    "zot.example/proj/app x:v1",
    "zot.example/p/app:v1\nrm -rf /",                    # a newline
    "zot.example/p/app:v1\n",
    "zot.example/p/app:v1\x1b[0m",                       # a control character
    "zot.example/p/$(id):v1",                            # command substitution
    "zot.example/p/`id`:v1",
    "zot.example/p/app:v1;id",
    "zot.example/p/app:v1'",
    'zot.example/p/app:v1"',
    "zot.example/p/app:v1&&id",
    "zot.example/p/app:v1|id",
    "user@zot.example/p/app:v1",                         # userinfo
    "zot.example/P/app:v1",                              # not a name a registry has
    "zot.example//app:v1",
    "zot.example/../app:v1",
    "zot.example/p/app:v1 --docker-login",
    "zot.example/p/app:v1@sha256:" + "a" * 63,
]


@pytest.mark.parametrize("ref", HOSTILE, ids=range(len(HOSTILE)))
def test_a_hostile_reference_does_not_parse(ref):
    assert settings.image_problem(ref) != "" and settings.parse_image(ref) is None


@pytest.mark.parametrize("ref,parts", [
    ("zot.example/p/app:v1", ("zot.example", "p/app", "v1", None)),
    ("zot.example:5000/app:v1", ("zot.example:5000", "app", "v1", None)),
    ("europe-docker.pkg.dev/p/r/app@sha256:" + "a" * 64, ("europe-docker.pkg.dev", "p/r/app", None, "sha256:" + "a" * 64)),
    ("localhost/app:v1", ("localhost", "app", "v1", None)),
])
def test_a_normal_reference_parses_into_host_path_tag_and_digest(ref, parts):
    assert settings.image_problem(ref) == "" and settings.parse_image(ref) == parts


@pytest.mark.parametrize("host,ok", [
    ("europe-docker.pkg.dev", True), ("us-central1-docker.pkg.dev", True),
    ("evil.example", False), ("zot.example:5000", False), ("", False),
    ("x.europe-docker.pkg.dev", False),                  # a name below a region's, not the region's
    ("europe-docker.pkg.dev.evil.example", False), ("-docker.pkg.dev", False), ("europe-docker.pkg.dev:443", False),
])
def test_the_token_goes_only_to_an_artifact_registry_region_host(host, ok):
    assert settings.registry_token_host(host) is ok


def _pull_with_token(tmp_path, ref):
    """The image block of the script, a fake Apptainer that writes the credentials it was given. Returns the
    password the pull was made with, or None for an anonymous pull."""
    lines = jobscript.batch_lines("-", "/scr")
    start = lines.index('if [ ! -f "$SIF" ]; then')
    end = next(i for i, l in enumerate(lines) if l.startswith("unset REG_TOKEN"))
    scr = tmp_path / "scr"
    (scr / "sif-cache").mkdir(parents=True)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text('#!/bin/bash\nif [ "$1" = pull ]; then printf "%s" "${APPTAINER_DOCKER_PASSWORD-none}" > "$SCR/pw"; '
                                      'printf "%s" "$3" > "$SCR/ref"; printf sif > "$2"; fi\n')
    (bindir / "apptainer").chmod(0o755)
    script = "\n".join(['fail(){ exit "$1"; }', 'kaos_sif_label(){ :; }'] + lines[start:end + 1]
                       + ['printf "%s:%s" "${REG_TOKEN-unset}" "${APPTAINER_DOCKER_PASSWORD-unset}" > "$SCR/after"'])
    env = dict(os.environ, SCR=str(scr), SIF=str(scr / "sif-cache" / "k.sif"), IMAGE_REF=ref, SLURM_JOB_ID="1",
               REG_TOKEN=REGISTRY, STEP_NAME="s", PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    assert subprocess.run(["bash", "-c", script], env=env, stdout=subprocess.PIPE).returncode == 0
    return (scr / "pw").read_text(), (scr / "ref").read_text(), (scr / "after").read_text()


def test_the_script_presents_the_token_to_an_artifact_registry_host(tmp_path):
    pw, ref, after = _pull_with_token(tmp_path, GAR)
    assert pw == REGISTRY and ref == "docker://" + GAR
    assert after == "unset:unset", "the token is gone from the script's environment as soon as the image is there"


@pytest.mark.parametrize("ref", [
    "evil.example/x-docker.pkg.dev/p/app:v1",            # the old glob matched this: the token went to evil.example
    "evil.example/europe-docker.pkg.dev/p/app:v1",
    "europe-docker.pkg.dev.evil.example/p/app:v1",
    "x.europe-docker.pkg.dev/p/app:v1",
    "zot.example:5000/europe-docker.pkg.dev/app:v1",
    "-docker.pkg.dev/p/app:v1",
    "Europe-docker.pkg.dev/p/app:v1",
])
def test_the_script_never_presents_the_token_to_another_host(tmp_path, ref):
    pw, pulled, after = _pull_with_token(tmp_path, ref)
    assert pw == "none", "an anonymous pull"
    assert after == "unset:unset"


def test_the_reference_is_quoted_where_it_is_used(tmp_path):
    # Spaces never reach a script (the checks above), and the script does not rely on it: every use is quoted.
    pw, pulled, _ = _pull_with_token(tmp_path, "zot.example/p/a b:v1")
    assert pulled == "docker://zot.example/p/a b:v1", "one argument, as written"
    assert not any(" docker://$IMAGE_REF" in l for l in jobscript.batch_lines("-", "/scr"))


@pytest.mark.parametrize("ref", HOSTILE[:12], ids=range(12))
def test_a_hostile_reference_is_refused_before_any_network_call_or_submit(rig, ref):  # noqa: F811
    calls = []
    rig.monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: calls.append(a) or (_ for _ in ()).throw(OSError("net")))
    slurm = FakeSlurm({})
    rig.make(slurm, IMAGE_REF=ref)
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and calls == [], "no registry, metadata, lakeFS or Slurm call"
    assert rig.kube.run["state"] == "FAILED" and rig.kube.run["exitCode"] == 239


def test_a_script_is_never_written_for_a_reference_that_does_not_parse():
    cfg = helpers.settings(IMAGE_REF="zot.example/p/$(id):v1")
    cfg.check_job()
    r = runmod.Runner(cfg, slurm=object(), stager=object(), reporter=object(), logs=object(), mint=lambda: "w")
    with pytest.raises(ValueError, match="cannot be used"):
        r.site_job(REGISTRY, "")


def test_the_registry_token_is_not_put_in_the_job_for_a_host_that_may_not_have_it():
    for image, has in [(GAR, True), ("zot.example/p/yolo:v0.0.0", False), ("evil.example/europe-docker.pkg.dev/p/a:v1", False)]:
        cfg = helpers.settings(IMAGE_REF=image)
        cfg.check_job()
        r = runmod.Runner(cfg, slurm=object(), stager=object(), reporter=object(), logs=object(), mint=lambda: "w")
        r.wallet = "w"
        old, runmod.wallet.fetch_workdir = runmod.wallet.fetch_workdir, lambda i, t: ""
        try:
            _, env, _, _ = r.site_job(REGISTRY, "")
        finally:
            runmod.wallet.fetch_workdir = old
        assert ("REG_TOKEN=" + REGISTRY in env) is has


# ---------------------------------------------------------------- 2/3. the node: step environment, files, modes

# The fake Apptainer of test_step_contract, except that --cleanenv is honoured: the step gets only what the
# script passed as APPTAINERENV_<name> (and a PATH, as the image has), exactly the contract of Apptainer.
CLEAN_APPTAINER = tsc.APPTAINER.replace('exec "$@"\n', '''ENVS=()
for v in $(compgen -v APPTAINERENV_); do
  val=${!v}; case "$val" in /*) val=$ROOT$val ;; esac
  ENVS+=("${v#APPTAINERENV_}=$val")
done
case "$CLEAN" in 1) exec env -i PATH=/usr/bin:/bin "${ENVS[@]}" "$@" ;; esac
exec "$@"
''').replace('exec) shift ;;', 'exec) shift; CLEAN=0; for a in "$@"; do [ "$a" = --cleanenv ] && CLEAN=1; done ;;')

# The step writes the environment it was given, and what the platform needs to see it did its work.
STEP = r'''
import json, os, pathlib, sys, time
pathlib.Path(%(saw)r).write_text(json.dumps(dict(os.environ)))
mode = %(mode)r
if mode == "sleep":
    pathlib.Path(%(started)r).write_text(str(os.getpid()))
    time.sleep(60)
out = pathlib.Path(os.environ["KAOS_CHECKPOINT_DIR"]).parent / "output" / "kaos-result.json"
if mode in ("result", "fail"):
    out.write_text(%(result)r)
sys.exit(3 if mode == "fail" else 0)
'''

FILE_TOKENS = (WALLET, REGISTRY)


class HSite(tsc.Site):
    def __init__(self, tmp_path, lakefs, shared, monkeypatch, does="result", image=GAR, inputs=()):
        monkeypatch.setattr(tsc, "APPTAINER", CLEAN_APPTAINER)
        super().__init__(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), inputs=inputs,
                         does=does, monkeypatch=monkeypatch)
        lakefs.bearer = WALLET
        self.cfg_image = image
        self.cfg.image = image
        self.runner._mint = lambda: WALLET
        self.runner.cfg = self.cfg
        (tmp_path / "step.py").write_text(STEP % dict(saw=str(self.saw), mode=does, started=str(tmp_path / "started"),
                                                      result=helpers.fixture("result-model.wire")))
        self.started = tmp_path / "started"

    def start(self, jid="5001"):
        self.runner.prepare_handback()
        batch, env, _, _ = self.runner.site_job(REGISTRY, "")
        self.batch, self.env = batch, env
        node = dict(e.split("=", 1) for e in env)
        node.update(PATH=str(self.tmp / "bin") + os.pathsep + os.environ["PATH"], SLURM_JOB_ID=jid, HOME=str(self.home),
                    FAKE_ROOT=str(self.tmp / "rootfs"), KAOS_HANDBACK_RETRY_SLEEP="0", KAOS_COMMENT_RETRY_SLEEP="0")
        if self.shared:
            node["KAOS_IN_STEP"] = "1"
        (self.tmp / "job.sh").write_text(batch)
        return subprocess.Popen(["bash", str(self.tmp / "job.sh")], cwd=str(self.home), env=node, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, start_new_session=True)

    def run(self, jid="5001"):
        p = self.start(jid)
        self.out = p.communicate(timeout=120)[0].decode(errors="replace")
        return p.returncode

    def cancel(self, jid="5001"):
        """Slurm's cancel: TERM to every process of the job."""
        p = self.start(jid)
        deadline = time.time() + 60
        while not self.started.exists() and time.time() < deadline:
            time.sleep(0.05)
        assert self.started.exists(), "the step never started"
        os.killpg(p.pid, signal.SIGTERM)
        self.kill_step()
        self.out = p.communicate(timeout=60)[0].decode(errors="replace")
        return p.returncode

    def kill_step(self):
        """The step runs in a process group of its own (set -m); the site's cancel ends the whole job."""
        try:
            os.killpg(int(self.started.read_text()), signal.SIGTERM)
        except (OSError, ValueError):
            pass

    def leaked(self):
        """Every file under the node's folders (but the harness's own copy of the script) that holds a token."""
        found = []
        for root in (self.scratch, self.home, self.tmp / "rootfs"):
            for dirpath, _, names in os.walk(str(root), followlinks=False):
                for n in names:
                    path = os.path.join(dirpath, n)
                    if os.path.islink(path):
                        continue
                    try:
                        data = open(path, "rb").read()
                    except OSError:
                        continue
                    if any(t.encode() in data for t in FILE_TOKENS):
                        found.append(path)
        return found


@pytest.fixture
def lakefs():
    srv = FakeLakeFS()
    yield srv
    srv.httpd.shutdown()


SHELL_ADDED = {"PWD", "OLDPWD", "SHLVL", "_", "LC_CTYPE"}      # what the shell that runs the step command adds itself
STEP_ALLOWED = set(jobscript.STEP_ENV) | {"PATH", "KAOS_STAGED_ROOT", "KAOS_CHECKPOINT_DIR", "KAOS_RESUME_PATH"}


@tsc.MODES
def test_the_step_receives_the_allow_list_and_nothing_else(tmp_path, lakefs, monkeypatch, shared):
    site = HSite(tmp_path, lakefs, shared, monkeypatch)
    assert site.run() == 0, site.out
    saw = site.step_saw()
    assert set(saw) - SHELL_ADDED <= STEP_ALLOWED, "outside the allow-list: " + ", ".join(sorted(set(saw) - SHELL_ADDED - STEP_ALLOWED))
    assert "REG_TOKEN" not in saw and REGISTRY not in json.dumps(saw), "the registry token never reaches the step"
    for k in ("KAOS_STEP_CONTEXT", "LAKEFS_ENDPOINT", "LAKEFS_BEARER_TOKEN", "KAOS_CHECKPOINT_DIR"):
        assert saw.get(k), k + " is what the step needs"
    assert saw["LAKEFS_BEARER_TOKEN"] == WALLET
    assert saw["KAOS_STEP_CONTEXT"] == helpers.fixture("context-one-upstream.wire"), "the context, unchanged"
    assert not {"WF_UID", "STEP_NAME", "STEP_CMD", "IMAGE_REF", "CKPT_B64", "HANDBACK_B64", "STAGEIN_B64", "IDLEWATCH_B64",
                "KAOS_OUTPUTS_REPO", "HPC_GPUS", "KAOS_WORK", "SAW", "FAKE_ROOT"} & set(saw)


@tsc.MODES
@pytest.mark.parametrize("does", ["result", "fail"])
def test_no_file_on_the_site_holds_a_token_when_the_job_ends(tmp_path, lakefs, monkeypatch, shared, does):
    site = HSite(tmp_path, lakefs, shared, monkeypatch, does=does)
    rc = site.run()
    assert rc == (0 if does == "result" else 3), site.out
    if does == "fail":
        assert (site.scratch / "kaos-work" / "5001").is_dir(), "a failed job keeps its work folder for the developer"
    assert site.leaked() == []
    assert not [p for p in os.listdir("/tmp") if p.startswith(("kaos-job-5001", "kaos-slots-5001"))]


@tsc.MODES
def test_no_file_holds_a_token_after_a_cancel(tmp_path, lakefs, monkeypatch, shared):
    site = HSite(tmp_path, lakefs, shared, monkeypatch, does="sleep")
    site.cancel()
    assert site.leaked() == []


@pytest.mark.parametrize("cancel", [False, True], ids=["ends", "cancelled"])
def test_a_shared_node_host_removes_the_copy_of_its_script_on_every_way_out(tmp_path, lakefs, monkeypatch, cancel):
    """The host's script is copied (credentials in it) for the slot manager to run. The copy is in a folder only
    the user enters, never in /tmp, mode 0600, and gone when the host script ends or is cancelled."""
    site = HSite(tmp_path, lakefs, True, monkeypatch)
    seen = tmp_path / "seen.json"
    slots = (
        "import json, os, stat, sys, time\n"
        "p = sys.argv[1]\n"
        "d = os.path.dirname(p)\n"
        "json.dump({'copy': p, 'copy_mode': stat.S_IMODE(os.stat(p).st_mode), 'dir_mode': stat.S_IMODE(os.stat(d).st_mode),\n"
        "           'token_in_copy': %r in open(p).read(), 'umask': os.umask(0o077)}, open(%r, 'w'))\n"
        "if %r:\n    open(%r, 'w').write('1'); time.sleep(60)\n"
        "sys.exit(7)\n" % (WALLET, str(seen), cancel, str(site.started)))
    (tmp_path / "bin" / "scontrol").write_text('#!/bin/bash\n[ "$1 $2" = "write batch_script" ] && cp "$HOME/../job.sh" "$4"; exit 0\n')
    site.runner.prepare_handback()
    batch, env, _, _ = site.runner.site_job(REGISTRY, "")
    assert "export KAOS_SLOTS_B64=" in batch, "a sharing host: the slot manager rides in the script"
    import base64
    import shlex
    batch = "\n".join("export KAOS_SLOTS_B64=" + shlex.quote(base64.b64encode(slots.encode()).decode())
                      if l.startswith("export KAOS_SLOTS_B64=") else l for l in batch.split("\n"))
    (tmp_path / "job.sh").write_text(batch)
    node = dict(e.split("=", 1) for e in env)
    node.update(PATH=str(tmp_path / "bin") + os.pathsep + os.environ["PATH"], SLURM_JOB_ID="5001", HOME=str(site.home))
    proc = subprocess.Popen(["bash", str(tmp_path / "job.sh")], cwd=str(site.home), env=node, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, start_new_session=True)
    if cancel:
        deadline = time.time() + 60
        while not site.started.exists() and time.time() < deadline:
            time.sleep(0.05)
        os.killpg(proc.pid, signal.SIGTERM)
    out = proc.communicate(timeout=60)[0].decode(errors="replace")
    info = json.loads(seen.read_text())
    print(out)
    assert info["token_in_copy"] is True, "the copy is a real copy of the script: " + out
    assert info["copy_mode"] == 0o600 and info["dir_mode"] == 0o700 and info["umask"] == 0o077
    assert str(site.scratch / "kaos-priv") in info["copy"] and not info["copy"].startswith("/tmp/")
    assert not os.path.exists(info["copy"]) and not os.path.exists(os.path.dirname(info["copy"])), "gone"
    assert proc.returncode == (143 if cancel else 7)
    assert site.leaked() == []


def _modes(root):
    return {os.path.relpath(os.path.join(d, n), str(root)): stat.S_IMODE(os.lstat(os.path.join(d, n)).st_mode)
            for d, dirs, files in os.walk(str(root)) for n in dirs + files if not os.path.islink(os.path.join(d, n))}


@tsc.MODES
def test_everything_the_job_creates_is_private_to_the_site_user(tmp_path, lakefs, monkeypatch, shared):
    site = HSite(tmp_path, lakefs, shared, monkeypatch, does="fail")
    os.chmod(str(site.scratch), 0o700)
    assert site.run() == 3, site.out
    modes = _modes(site.scratch)
    assert modes, "the job made folders"
    loose = {p: oct(m) for p, m in modes.items() if m & 0o077}
    assert loose == {}, "readable by group or others"
    for folder in ("kaos-work", "kaos-work/5001", "kaos-work/5001/ckpt", "kaos-work/5001/output", "kaos-ckpt",
                   "sif-cache", "kaos-tmp", "data-cache"):
        assert modes[folder] == 0o700, folder


def test_a_folder_an_older_job_left_open_is_closed(tmp_path, lakefs, monkeypatch):
    site = HSite(tmp_path, lakefs, False, monkeypatch, does="fail")
    for d in ("kaos-work", "sif-cache", "data-cache", "kaos-ckpt"):
        (site.scratch / d).mkdir()
        os.chmod(str(site.scratch / d), 0o755)
    site.run()
    for d in ("kaos-work", "sif-cache", "data-cache", "kaos-ckpt"):
        assert stat.S_IMODE(os.stat(str(site.scratch / d)).st_mode) == 0o700, d


def test_the_script_sets_the_umask_before_it_makes_anything():
    lines = jobscript.batch_lines("-", "/scr")
    assert lines.index("umask 077") < min(i for i, l in enumerate(lines) if "mkdir" in l or "base64 -d >" in l)
    assert lines.index("umask 077") == 3, "right after the header, so the sharing exports still follow line 3"


def test_the_two_places_that_run_the_step_name_the_same_variables():
    # The allow-list is one tuple; the script's loop and the contract text read it.
    line = next(l for l in jobscript.batch_lines("-", "/scr") if l.startswith("KAOS_STEP_ENV="))
    assert line == 'KAOS_STEP_ENV="%s"' % " ".join(jobscript.STEP_ENV)
    assert "LAKEFS_BEARER_TOKEN" in jobscript.STEP_ENV and "REG_TOKEN" not in jobscript.STEP_ENV
    assert sys.version_info >= (3, 6)
