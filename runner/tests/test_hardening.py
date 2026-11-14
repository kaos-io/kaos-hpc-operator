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
import base64
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

from kaos_hpc_runner import identity, jobscript, payloads, settings
from kaos_hpc_runner import runner as runmod
from kaos_hpc_runner.redact import Redactor

WALLET = "WALLET-SECRET-" + "w" * 24          # stands in for the tracker's token
KEY_SECRET = "KEY-SECRET-" + "k" * 24            # stands in for the app's data store key
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
    "zot.example/p/app:@sha256:" + "a" * 64,             # an empty tag before the digest (Go and Python agree)
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
    end = next(i for i, ln in enumerate(lines) if ln.startswith("unset REG_TOKEN"))
    scr = tmp_path / "scr"
    (scr / "sif-cache").mkdir(parents=True)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text('#!/bin/bash\nif [ "$1" = pull ]; then '
                                      'printf "%s" "${APPTAINER_DOCKER_PASSWORD-none}" > "$SCR/pw"; '
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
    assert not any(" docker://$IMAGE_REF" in ln for ln in jobscript.batch_lines("-", "/scr"))


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

# The fake Apptainer of test_step_contract (which evaluates APPTAINERENV_ values like the real one), except that
# --cleanenv is honoured: the step gets only what the script passed as APPTAINERENV_<name> (and a PATH, as the image
# has), exactly the contract of Apptainer.
CLEAN_APPTAINER = tsc.APPTAINER.replace('exec "${ARGS[@]}"\n', '''ENVS=()
for v in $(compgen -v APPTAINERENV_); do n=${v#APPTAINERENV_}; ENVS+=("$n=${!n}"); done
case "$CLEAN" in 1) exec env -i PATH=/usr/bin:/bin "${ENVS[@]}" "${ARGS[@]}" ;; esac
exec "${ARGS[@]}"
''').replace('exec) shift ;;', 'exec) shift; CLEAN=0; for a in "$@"; do [ "$a" = --cleanenv ] && CLEAN=1; done ;;')

# The step writes the environment it was given, and what the platform needs to see it did its work.
STEP = r'''
import json, os, pathlib, sys, time
pathlib.Path(%(saw)r).write_text(json.dumps(dict(os.environ)))
pathlib.Path(%(saw)r + ".proc").write_bytes(open("/proc/self/environ", "rb").read())
mode = %(mode)r
if mode == "sleep":
    pathlib.Path(%(started)r).write_text(str(os.getpid()))
    time.sleep(60)
out = pathlib.Path(os.environ["KAOS_CHECKPOINT_DIR"]).parent / "output" / "kaos-result.json"
if mode in ("result", "fail"):
    out.write_text(%(result)r)
sys.exit(3 if mode == "fail" else 0)
'''

FILE_TOKENS = (WALLET, REGISTRY, KEY_SECRET)


class HSite(tsc.Site):
    def __init__(self, tmp_path, lakefs, shared, monkeypatch, does="result", image=GAR, inputs=(), context=None):
        monkeypatch.setattr(tsc, "APPTAINER", CLEAN_APPTAINER)
        super().__init__(tmp_path, lakefs, shared, context or helpers.fixture("context-one-upstream.wire"), inputs=inputs,
                         does=does, monkeypatch=monkeypatch)
        self.cfg_image = image
        self.cfg.image = image
        self.runner._mint = lambda: WALLET
        self.runner._keys = lambda: identity.Key("KAOSXHARD", KEY_SECRET, 1e12, "2099-01-01T00:00:00Z")
        self.runner.cfg = self.cfg
        (tmp_path / "step.py").write_text(STEP % dict(saw=str(self.saw), mode=does, started=str(tmp_path / "started"),
                                                      result=helpers.fixture("result-model.wire")))
        self.started = tmp_path / "started"

    def start(self, jid="5001", extra=None):
        self.runner.prepare_handback()
        self.place_key(jid)
        batch, env, _, _ = self.runner.site_job(REGISTRY, "")
        self.batch, self.env = batch, env
        node = dict(e.split("=", 1) for e in env)
        node.update(PATH=str(self.tmp / "bin") + os.pathsep + os.environ["PATH"], SLURM_JOB_ID=jid, HOME=str(self.home),
                    FAKE_ROOT=str(self.tmp / "rootfs"), KAOS_HANDBACK_RETRY_SLEEP="0", KAOS_COMMENT_RETRY_SLEEP="0")
        if self.shared:
            node["KAOS_IN_STEP"] = "1"
        node.update(extra or {})        # what Slurm and the site's login environment add to the job
        (self.tmp / "job.sh").write_text(batch)
        return subprocess.Popen(["bash", str(self.tmp / "job.sh")], cwd=str(self.home), env=node, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, start_new_session=True)

    def run(self, jid="5001", extra=None):
        p = self.start(jid, extra)
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
STEP_ALLOWED = set(jobscript.STEP_ENV) | {"LAKEFS_KEY_FILE", "PATH", "KAOS_STAGED_ROOT", "KAOS_CHECKPOINT_DIR", "KAOS_RESUME_PATH"}


@tsc.MODES
def test_the_step_receives_the_allow_list_and_nothing_else(tmp_path, lakefs, monkeypatch, shared):
    site = HSite(tmp_path, lakefs, shared, monkeypatch)
    assert site.run() == 0, site.out
    saw = site.step_saw()
    outside = set(saw) - SHELL_ADDED - STEP_ALLOWED
    assert not outside, "outside the allow-list: " + ", ".join(sorted(outside))
    assert "REG_TOKEN" not in saw and REGISTRY not in json.dumps(saw), "the registry token never reaches the step"
    for k in ("KAOS_STEP_CONTEXT", "LAKEFS_ENDPOINT", "LAKEFS_KEY_FILE", "KAOS_CHECKPOINT_DIR"):
        assert saw.get(k), k + " is what the step needs"
    assert "LAKEFS_BEARER_TOKEN" not in saw and KEY_SECRET not in json.dumps(saw), "the step gets a file, never the key itself"
    assert saw["LAKEFS_KEY_FILE"].endswith("/kaos/key/lakefs-key.json"), "the file the folder is bound to inside the container"
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
    (tmp_path / "bin" / "scontrol").write_text(
        '#!/bin/bash\n[ "$1 $2" = "write batch_script" ] && cp "$HOME/../job.sh" "$4"; exit 0\n')
    site.runner.prepare_handback()
    batch, env, _, _ = site.runner.site_job(REGISTRY, "")
    assert "export KAOS_SLOTS_B64=" in batch, "a sharing host: the slot manager rides in the script"
    import base64
    import shlex
    batch = "\n".join("export KAOS_SLOTS_B64=" + shlex.quote(base64.b64encode(slots.encode()).decode())
                      if ln.startswith("export KAOS_SLOTS_B64=") else ln for ln in batch.split("\n"))
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
    # (& 0o777: a folder in the project's root inherits the setgid bit of the shared folder above it; the group stays)
    assert info["copy_mode"] == 0o600 and info["dir_mode"] & 0o777 == 0o700 and info["umask"] == 0o077
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
    assert lines.index("umask 077") < min(i for i, ln in enumerate(lines) if "mkdir" in ln or "base64 -d >" in ln)
    assert lines.index("umask 077") == 3, "right after the header, so the sharing exports still follow line 3"


# ---------------------------------------------------------------- 4. the failure tail and the log stream

def _node_redact(text, env=None, with_program=True):
    """The text through the job script's own kaos_redact on a node (the program is shipped as REDACT_B64)."""
    full = dict(os.environ, REDACT_B64=payloads.b64(payloads.REDACT) if with_program else "", **(env or {}))
    helper = "\n".join(jobscript.REDACT_HELPER)
    return subprocess.run(["bash", "-c", helper + "\nkaos_redact"], input=text, capture_output=True, text=True, env=full).stdout


JWT = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjEifQ.eyJzdWIiOiJ1c2VyIiwiZXhwIjo5OTl9.c2lnbmF0dXJlX3ZhbHVlXzEyMw"
PEM = ("-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo\n4lgOEePzNm0tRgeLezV6ffAt0gun\n"
       "-----END RSA PRIVATE KEY-----")

SECRETS = [
    ("abc123def456", "Authorization: Bearer abc123def456"),
    ("abc123def456", "authorization: bearer abc123def456 and more"),
    ("abcdefghij0123456789", "sent Bearer abcdefghij0123456789 to the server"),
    ("ya29.a0AfH6SMBx-1_zz", "token ya29.a0AfH6SMBx-1_zz expired"),
    (JWT, "got " + JWT + " from the server"),
    ("eyJhbGciOiJub25lIn0", "bare eyJhbGciOiJub25lIn0 header"),
    ("hunter2", "password=hunter2"),
    ("hunter2 and more", 'password: "hunter2 and more" next'),
    ("s3cr3tvalue", "LAKEFS_BEARER_TOKEN=s3cr3tvalue"),
    ("s3cr3tvalue", "MLFLOW_TRACKING_TOKEN=s3cr3tvalue&x=1"),
    ("s3cr3tvalue", "MY_APP_SECRET_KEY=s3cr3tvalue"),
    ("zzz999", "api_key=zzz999;next"),
    ("zzz999", "client_secret=zzz999,other"),
    ("abc123", '{"access_token": "abc123", "n": 1}'),
    ("abc123", "{'access_token': 'abc123', 'n': 1}"),                       # a Python repr
    ("abc123", '{\\"access_token\\": \\"abc123\\", \\"n\\": 1}'),            # JSON inside a JSON string
    ("abc123", "GET /v1/files?token=abc123&x=1"),
    ("dXNlcjpwYXNzd29yZDEyMzQ=", "Authorization: Basic dXNlcjpwYXNzd29yZDEyMzQ="),
    ("TOKEN-V4LUE", "Token=TOKEN-V4LUE"),
    ("p4ssw0rd", "cloning https://alice:p4ssw0rd@git.example/org/repo.git"),
    ("k3y-value", "X-Api-Key: k3y-value"),
    ("k3y-value", "request headers {'X-Api-Key': 'k3y-value'}"),
    ("AKIAIOSFODNN7EXAMPLE", "aws key AKIAIOSFODNN7EXAMPLE in use"),
    ("ghp_" + "a1B2" * 9, "pushing with ghp_" + "a1B2" * 9),
    ("github_pat_" + "A1" * 30, "GH github_pat_" + "A1" * 30),
    ("a=1; sessionid=zz9", "Cookie: a=1; sessionid=zz9"),
    ("hunter2x9", "run.py --password hunter2x9 --lr 1"),
    ("abcdefghij0123456789", "run.py --token abcdefghij0123456789"),
    ("abcdefghij0123456789", "run.py --token=abcdefghij0123456789"),
    ("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
    ("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
    ("3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b",
     "GET https://s.example/o?X-Amz-Algorithm=A&X-Amz-Signature=3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b&x=1"),
    ("0f1e2d3c4b5a69788796a5b4c3d2e1f0", "https://storage.example/o?X-Goog-Signature=0f1e2d3c4b5a69788796a5b4c3d2e1f0"),
    ("mQENBF2sZ0wBCADexample0123456789abcdefghij", "-----BEGIN PGP PRIVATE KEY BLOCK-----\nVersion: GnuPG v2\n\n"
     "mQENBF2sZ0wBCADexample0123456789abcdefghij\n=abcd\n-----END PGP PRIVATE KEY BLOCK-----"),
    ("MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo", PEM),
    ("MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo", PEM.replace("\n", "\\n")),         # one JSON string: escaped newlines
]


@pytest.mark.parametrize("secret,text", SECRETS)
def test_what_looks_like_a_credential_is_redacted(secret, text):
    out = _node_redact("before\n" + text + "\nafter\n")
    assert secret not in out and "[redacted" in out, out
    assert out.startswith("before\n") and out.endswith("after\n"), "the lines around it are kept: " + out


ORDINARY = [
    "epoch 3 loss=0.25 lr=1e-05", "Traceback (most recent call last):", '  File "a.py", line 3', "ValueError: boom",
    "max_new_tokens=512", "tokenizer=bert-base-uncased", "num_keypoints=17", "monkey=3", "val_key_metric=0.91",
    '{"primary_key": "id", "keys": 3}', "The Bearer token expired", "Basic principles of design", "n_tokens: 2048",
    "token count 17", "keyboard=us", "using key frame 4", "secretary=Ann", "passwordless login enabled",
    "sha256 " + "ab" * 32, "-----BEGIN CERTIFICATE-----", "see https://example.org/a/b?page=2",
    "token: 3 of 128", "unexpected token: '}'", "secret: False", "authorization: required", "PRIVATE_KEY=unset",
    "GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json", "password: None", "token=None", "secret=true",
    "error: expected -----BEGIN RSA PRIVATE KEY----- header in file",
]


@pytest.mark.parametrize("text", ORDINARY)
def test_ordinary_output_is_not_changed(text):
    assert _node_redact(text + "\n") == text + "\n"


HELD = "Z9y8X7w6V5u4T3s2R1q0-held.value+with/odd=chars"


def test_the_exact_credentials_the_job_holds_are_redacted_in_every_spelling_with_no_guessing():
    import base64
    import urllib.parse
    forms = [HELD, base64.b64encode(HELD.encode()).decode(), base64.urlsafe_b64encode(HELD.encode()).decode().rstrip("="),
             urllib.parse.quote(HELD, safe=""), base64.b64encode(b"oauth2accesstoken:" + HELD.encode()).decode()]
    text = "".join("line %d: the server said [%s] and nothing else\n" % (i, f) for i, f in enumerate(forms))
    out = _node_redact(text, env={"MLFLOW_TRACKING_TOKEN": HELD})
    for f in forms:
        assert f not in out
    assert out.count("[redacted]") == len(forms) and "the server said [[redacted]]" in out
    # the same held value, in a log line, is not redacted when it is not a credential this job holds
    assert _node_redact(text, env={"MLFLOW_TRACKING_TOKEN": "something-else-entirely"}) == text


def test_a_value_too_short_to_be_a_credential_does_not_blot_out_the_output():
    assert _node_redact("a short abc here\n", env={"MLFLOW_TRACKING_TOKEN": "abc"}) == "a short abc here\n"


def test_redaction_costs_time_in_proportion_to_the_line_not_more():
    # linear-time: lines built to make a backtracking pattern work hard, a megabyte each
    big = ["x-" + "a" * (1 << 20), "token=" + "a" * (1 << 20), "A_" * (1 << 19), "-----BEGIN " * 100000, "eyJ" + "a." * (1 << 19),
           "https://" + "a:" * (1 << 19), "password" + " " * (1 << 20), "Bearer " + "a" * (1 << 20), "a" * (1 << 20)]
    started = time.time()
    out = _node_redact("\n".join(big) + "\n")
    assert time.time() - started < 60 and len(out) > 0


def test_a_node_without_the_redaction_program_shows_none_of_the_output():
    out = _node_redact("password=hunter2\n", with_program=False)
    assert "hunter2" not in out and "withheld" in out


def test_the_comment_of_a_failed_job_holds_no_credential(tmp_path):
    """fail() copies the tail of the job's output into the job comment: redacted first, and a token cut by the
    700-byte window is redacted whole because the window is taken after redaction."""
    fail = "\n".join(jobscript.COMMENT_HELPER + jobscript.REDACT_HELPER
                     + [next(ln for ln in jobscript.batch_lines("-", "/scr") if ln.startswith("fail(){"))])
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "scontrol").write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$OUT"\n')
    (bindir / "scontrol").chmod(0o755)
    long_jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0." + "A" * 400      # a signature of 400 characters
    (tmp_path / "slurm-4242.out").write_text(
        "B" * 500 + long_jwt + "z" * 150 + "\nERROR Authorization: Bearer BEARERVALUE123\nLAKEFS_BEARER_TOKEN=" + WALLET
        + "\nhttps://u:PASSWD123@h/x\nlogin failed for " + WALLET + "\nboom\n")
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], OUT=str(tmp_path / "args"),
               KAOS_JOB_ID="4242", KAOS_COMMENT_RETRY_SLEEP="0", REDACT_B64=payloads.b64(payloads.REDACT),
               MLFLOW_TRACKING_TOKEN=WALLET)
    assert subprocess.run(["bash", "-c", fail + "\nfail 7"], cwd=str(tmp_path), env=env).returncode == 7
    comment = (tmp_path / "args").read_text().split("\n")[2]
    assert comment.startswith("Comment=err7:") and "boom" in comment and "[redacted" in comment
    for secret in (WALLET, "BEARERVALUE123", "eyJhbGci", "A" * 40, "PASSWD123"):
        assert secret not in comment, "a credential or a piece of one is in the job comment"
    assert len(comment) < 800


def test_the_app_key_in_its_file_is_redacted_on_the_node_in_every_spelling(tmp_path):
    """AC-14 B: the key is in a file, not a variable: the node's redactor reads the file and hides the secret, the id
    with it, and the pair a Basic header holds."""
    path = helpers.write_key_file(tmp_path / "key", identity.Key("KAOSXHARD", KEY_SECRET, 1e12, "x"))
    pair = base64.b64encode(("KAOSXHARD:" + KEY_SECRET).encode()).decode()
    text = "bad: %s\nid %s\nAuthorization header %s\nfine line\n" % (KEY_SECRET, "KAOSXHARD", pair)
    out = _node_redact(text, env={"LAKEFS_KEY_FILE": path})
    assert KEY_SECRET not in out and pair not in out and "KAOSXHARD" not in out and "fine line" in out, out


def test_the_log_stream_redacts_every_line_before_it_prints_it():
    from kaos_hpc_runner.logs import LogStreamer
    printed = []
    cfg = helpers.settings()
    streamer = LogStreamer(cfg, stager=None, reporter=type("R", (), {"gpu_util": None})(), out=printed.append,
                           secrets=lambda: (WALLET, ""))
    streamer.job = "5001"
    metric = 'KAOS_METRIC {"t":1,"gpus":[],"load":[0,0,0],"mem_available_kb":1}'
    lines = (["step started", "LAKEFS_BEARER_TOKEN=" + WALLET, "login failed for " + WALLET,
              "Authorization: Bearer abc123def456ghi", "max_new_tokens=512"] + PEM.split("\n") + ["after the key", metric])
    for ln in lines:
        streamer._line(ln.encode())
    shown = "\n".join(printed)
    for secret in (WALLET, "abc123def456ghi", "MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo", "4lgOEePzNm0tRg", "END RSA"):
        assert secret not in shown
    msgs = [json.loads(p)["msg"] for p in printed]
    assert "step started" in msgs and "max_new_tokens=512" in msgs and "after the key" in msgs
    assert any(m.startswith("[redacted: private key]") for m in msgs), "the block is one marker, its lines are dropped"
    assert streamer.tail_text().count("MIIE") == 0 and "after the key" in streamer.tail_text()
    assert any(json.loads(p).get("metric") for p in printed), "the sampler's lines still parse"


def test_the_two_places_that_run_the_step_name_the_same_variables():
    # The allow-list is one tuple; the script's loop and the contract text read it.
    line = next(ln for ln in jobscript.batch_lines("-", "/scr") if ln.startswith("KAOS_STEP_ENV="))
    assert line == 'KAOS_STEP_ENV="%s"' % " ".join(jobscript.STEP_ENV)
    assert "LAKEFS_BEARER_TOKEN" not in jobscript.STEP_ENV and "REG_TOKEN" not in jobscript.STEP_ENV
    assert sys.version_info >= (3, 6)


# ---------------------------------------------------------------- 5. how a value reaches the step

def _fake_apptainer(tmp_path, args, env):
    """The stand-in Apptainer run directly: returns what a variable became in the step."""
    (tmp_path / "ap").write_text(tsc.APPTAINER)
    os.chmod(str(tmp_path / "ap"), 0o755)
    out = subprocess.run([str(tmp_path / "ap"), "exec", *args, "x.sif", "/bin/sh", "-c", 'printf %s "$P"'],
                         env=dict(os.environ, FAKE_ROOT=str(tmp_path / "root"), **env), capture_output=True, text=True)
    return out.stdout


def test_the_stand_in_apptainer_evaluates_values_like_the_real_one_unless_no_eval(tmp_path):
    """Without this the tests below prove nothing: the real Apptainer (1.1.0 and later) runs `$(...)` in an
    APPTAINERENV_ value in the container's startup shell, and --no-eval turns that off."""
    mark = tmp_path / "ran"
    value = "$(touch %s; echo ran)-$HOME-\\x" % mark
    got = _fake_apptainer(tmp_path, [], {"APPTAINERENV_P": value})
    assert mark.exists() and got.startswith("ran-") and os.environ["HOME"] in got, "evaluated: " + got
    mark.unlink()
    got = _fake_apptainer(tmp_path, ["--no-eval"], {"APPTAINERENV_P": value})
    assert not mark.exists() and got == value, "--no-eval keeps the literal value"


# A context built to be changed by every layer that might read it: the shell, Apptainer's evaluation, a JSON or
# URL handler. 100 KB of it.
def _hostile_context(tmp_path):
    marks = [tmp_path / "ran1", tmp_path / "ran2", tmp_path / "ran3"]
    param = '$HOME $(touch %s) `touch %s` ${X:-$(touch %s)} \\ " \' \\\\n \\u0041 %%s {{x}} ~ * ?' % tuple(marks)
    escaped = param.replace("\\", "\\\\").replace('"', '\\"')
    ctx = '{"version":1,"parameters":{"p":"' + escaped + '","pad":"' + "éx\\n" * 25000 + '"}}'
    assert len(ctx) >= 100000
    return ctx, marks


@tsc.MODES
def test_a_context_with_shell_characters_reaches_the_step_byte_for_byte_and_nothing_in_it_runs(
        tmp_path, lakefs, monkeypatch, shared):
    ctx, marks = _hostile_context(tmp_path)
    site = HSite(tmp_path, lakefs, shared, monkeypatch, context=ctx)
    assert site.run() == 0, site.out
    saw = site.step_saw()
    assert saw["KAOS_STEP_CONTEXT"] == ctx, "the context, byte for byte"
    assert [m for m in marks if m.exists()] == [], "a command in a run parameter was run"
    assert not any("APPTAINERENV_" in ln and "KAOS_STEP_CONTEXT" in ln for ln in site.batch.split("\n")), "never as APPTAINERENV_"


def test_the_script_hands_no_value_with_a_special_character_to_apptainers_evaluation(tmp_path, lakefs, monkeypatch):
    """Every variable the node can add, hostile: a proxy with `$(...)` in the password, a CA path with a space, a
    support value of JSON, a locale with a backtick. None is evaluated and each arrives exactly as it was."""
    mark = tmp_path / "ran"
    hostile = {"https_proxy": "http://u:p$(touch %s)@proxy:3128" % mark, "NO_PROXY": ".local,`touch %s`" % mark,
               "SSL_CERT_FILE": "/etc/ssl/my certs/ca.pem", "LANG": "en_US.UTF-8 $(touch %s)" % mark,
               "KAOS_LIB_SUPPORT": '{"a":"$(touch %s)","b":"\\\\"}' % mark, "TZ": "Europe/Athens",
               "SLURM_JOB_NODELIST": "mel[2008-2010]", "OMP_NUM_THREADS": "8"}
    site = HSite(tmp_path, lakefs, False, monkeypatch)
    assert site.run(extra=hostile) == 0, site.out
    saw = site.step_saw()
    assert not mark.exists(), "something was evaluated"
    for k, v in hostile.items():
        assert saw.get(k) == v, k


def test_the_watchdog_variables_reach_the_step_through_the_real_script(tmp_path, lakefs, monkeypatch):
    """The slot manager (payload/slots.py step_rss, step_gpu_claim) finds a step's processes by SLURM_JOB_ID and
    SLURM_STEP_ID in /proc/<pid>/environ and judges its GPUs by KAOS_STEP_GPUS. Run through the script the runner
    submits (not a plain shell guest): the three are in the environment of the process the step becomes."""
    site = HSite(tmp_path, lakefs, True, monkeypatch)
    extra = {"SLURM_STEP_ID": "7", "KAOS_STEP_GPUS": "0,1", "CUDA_VISIBLE_DEVICES": "0,1"}
    assert site.run(jid="5001", extra=extra) == 0, site.out
    saw = site.step_saw()
    assert (saw["SLURM_JOB_ID"], saw["SLURM_STEP_ID"], saw["KAOS_STEP_GPUS"]) == ("5001", "7", "0,1")
    proc = dict(kv.split(b"=", 1) for kv in open(str(site.saw) + ".proc", "rb").read().split(b"\0") if b"=" in kv)
    assert (proc[b"SLURM_JOB_ID"], proc[b"SLURM_STEP_ID"], proc[b"KAOS_STEP_GPUS"]) == (b"5001", b"7", b"0,1"), \
        "the kernel's view of the step's environment, which is all the watchdog has"
    # the slot manager's own parsing of it
    given = [x.strip() for x in proc.get(b"KAOS_STEP_GPUS", b"").decode().split(",") if x.strip()]
    assert proc[b"SLURM_JOB_ID"] == b"5001" and b"SLURM_STEP_ID" in proc and set(given) == {"0", "1"}


ALLOWED_FACTS = {
    "http_proxy": "http://proxy.site:3128", "https_proxy": "http://proxy.site:3128", "HTTP_PROXY": "http://proxy.site:3128",
    "HTTPS_PROXY": "http://proxy.site:3128", "no_proxy": "127.0.0.1,localhost,.site", "NO_PROXY": "127.0.0.1,localhost,.site",
    "SSL_CERT_FILE": "/etc/ssl/ca.pem", "REQUESTS_CA_BUNDLE": "/etc/ssl/ca.pem", "CURL_CA_BUNDLE": "/etc/ssl/ca.pem",
    "SLURM_CPUS_PER_TASK": "16", "SLURM_GPUS_ON_NODE": "4", "SLURM_JOB_GPUS": "0,1,2,3", "SLURM_NTASKS": "1",
    "SLURM_MEM_PER_NODE": "480000", "SLURM_JOB_NODELIST": "mel2008", "SLURM_TMPDIR": "/tmp/job.5001",
    "OMP_NUM_THREADS": "16", "MKL_NUM_THREADS": "16", "LANG": "en_US.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC",
    "PYTHONUNBUFFERED": "1", "XDG_CACHE_HOME": "/scratch/cache", "NVIDIA_DRIVER_CAPABILITIES": "compute,utility",
    "GPU_DEVICE_ORDINAL": "0,1,2,3", "SLURM_CPUS_ON_NODE": "128",
}
NEVER = {"LD_LIBRARY_PATH": "/opt/host/lib", "REG_TOKEN": REGISTRY,
         "AWS_SECRET_ACCESS_KEY": "aws-secret-value", "GITHUB_TOKEN": "gh-secret-value", "SSH_AUTH_SOCK": "/tmp/agent",
         "APPTAINER_DOCKER_PASSWORD": REGISTRY}


@pytest.mark.parametrize("shared", [False], ids=["alone"])
def test_a_job_passes_its_proxy_ca_threads_locale_and_slurm_facts_and_nothing_else(tmp_path, lakefs, monkeypatch, shared):
    site = HSite(tmp_path, lakefs, shared, monkeypatch)
    assert site.run(extra=dict(ALLOWED_FACTS, **NEVER)) == 0, site.out
    saw = site.step_saw()
    root = str(tmp_path / "rootfs")     # the stand-in maps an absolute path under its fake root; the real one does not
    for k, v in ALLOWED_FACTS.items():
        assert saw.get(k, "").replace(root, "") == v, k
    assert not (set(NEVER) | {"HOME", "USER"}) & set(saw), "passed from the host: " + ", ".join(sorted(set(NEVER) & set(saw)))
    assert REGISTRY not in json.dumps(saw)
    assert set(jobscript.PASS_ENV) <= set(jobscript.STEP_ENV)


def test_a_variable_the_job_does_not_have_is_not_invented(tmp_path, lakefs, monkeypatch):
    site = HSite(tmp_path, lakefs, False, monkeypatch)
    assert site.run() == 0, site.out
    saw = site.step_saw()
    assert "http_proxy" not in saw and "TZ" not in saw and "LANG" not in saw


HOST_WIDE = {"SLURM_CPUS_PER_TASK": "64", "SLURM_CPUS_ON_NODE": "128", "SLURM_MEM_PER_NODE": "512000", "SLURM_NTASKS": "8",
             "SLURM_GPUS_ON_NODE": "4", "SLURM_JOB_GPUS": "0,1,2,3", "SLURM_JOB_NODELIST": "mel2008"}


def test_a_step_on_a_shared_node_sees_its_own_share_not_the_host_jobs(tmp_path, lakefs, monkeypatch):
    """srun hands every step the host job's Slurm variables. The slot manager knows the step's share (cores, memory,
    its one GPU): those are what the step is told; a figure it does not have is left out, never the host's."""
    site = HSite(tmp_path, lakefs, True, monkeypatch)
    slot = {"KUBECORE_SLOT_SHARED": "1", "KUBECORE_SLOT_GPUS": "1", "KUBECORE_SLOT_CPUS": "32", "KUBECORE_SLOT_MEM_GIB": "100",
            "KAOS_STEP_GPUS": "2", "CUDA_VISIBLE_DEVICES": "2", "SLURM_STEP_ID": "3"}
    assert site.run(extra=dict(HOST_WIDE, **slot)) == 0, site.out
    saw = site.step_saw()
    assert (saw["SLURM_CPUS_PER_TASK"], saw["SLURM_CPUS_ON_NODE"], saw["SLURM_MEM_PER_NODE"], saw["SLURM_NTASKS"]) == \
        ("32", "32", str(100 * 1024), "1")
    assert (saw["SLURM_GPUS_ON_NODE"], saw["SLURM_JOB_GPUS"]) == ("1", "2")
    assert saw["SLURM_JOB_NODELIST"] == "mel2008", "the node is shared: the same one"
    assert "512000" not in json.dumps(saw) and "0,1,2,3" not in json.dumps(saw)


def test_a_shared_step_with_no_figure_for_a_fact_is_not_given_the_hosts(tmp_path, lakefs, monkeypatch):
    site = HSite(tmp_path, lakefs, True, monkeypatch)
    assert site.run(extra=dict(HOST_WIDE, KUBECORE_SLOT_SHARED="1")) == 0, site.out
    saw = site.step_saw()
    hosts = {"SLURM_CPUS_PER_TASK", "SLURM_CPUS_ON_NODE", "SLURM_MEM_PER_NODE", "SLURM_GPUS_ON_NODE", "SLURM_JOB_GPUS"}
    assert not hosts & set(saw), "the host's figures are never given to a step"
    assert saw["SLURM_NTASKS"] == "1"


def test_a_job_alone_on_its_node_gets_slurms_own_figures(tmp_path, lakefs, monkeypatch):
    site = HSite(tmp_path, lakefs, False, monkeypatch)
    assert site.run(extra=HOST_WIDE) == 0, site.out
    saw = site.step_saw()
    assert {k: saw[k] for k in HOST_WIDE} == HOST_WIDE


def test_the_variables_of_a_running_step_are_in_a_private_folder_and_gone_on_cancel(tmp_path, lakefs, monkeypatch):
    site = HSite(tmp_path, lakefs, False, monkeypatch, does="sleep")
    p = site.start()
    deadline = time.time() + 60
    while not site.started.exists() and time.time() < deadline:
        time.sleep(0.05)
    env_dir = site.scratch / "kaos-tmp" / "env-5001"
    try:
        assert stat.S_IMODE(os.stat(str(env_dir)).st_mode) & 0o777 == 0o700
        files = {f.name: stat.S_IMODE(f.stat().st_mode) for f in env_dir.iterdir()}
        assert set(files) >= {"KAOS_STEP_CONTEXT"} and "LAKEFS_BEARER_TOKEN" not in files, files
        assert set(files.values()) == {0o600}
        assert (env_dir / "MLFLOW_TRACKING_TOKEN").read_text() == WALLET
        key_dir = site.scratch / "kaos-tmp" / "key-5001"          # the key is in its own folder, never among the variables
        assert stat.S_IMODE(os.stat(str(key_dir)).st_mode) == 0o700
        assert stat.S_IMODE(os.stat(str(key_dir / "lakefs-key.json")).st_mode) == 0o600
    finally:
        os.killpg(p.pid, signal.SIGTERM)
        site.kill_step()
        p.communicate(timeout=60)
    assert not env_dir.exists() and site.leaked() == []


# ---------------------------------------------------------------- 6. what a killed job leaves behind

SWEPT = ("kaos-priv/host-100", "kaos-priv/host-200", "kaos-tmp/in-300", "kaos-tmp/in-400", "kaos-tmp/env-500")
ODD = ("kaos-priv/host-", "kaos-priv/host-12a", "kaos-priv/host-x", "kaos-priv/other-5", "kaos-tmp/ds-7", "kaos-tmp/idle-8",
       "kaos-tmp/in-1 2")


def _sweep(tmp_path, queue, root=None, self_id=None, age="0", folders=SWEPT, allow_empty=False):
    """kaos_sweep on a scratch root with these folders (each with a file), a fake squeue that answers `queue` (a list of
    ids, or None for a squeue that fails). Returns (root, what is left of the folders)."""
    scr = tmp_path / "scr"
    for f in folders:
        (scr / f).mkdir(parents=True)
        (scr / f / "job.sh").write_text("secret")
    bindir = tmp_path / "bin"
    bindir.mkdir(parents=True, exist_ok=True)
    rows = [q.split(":") if ":" in q else [q, "RUNNING"] for q in (queue or [])]
    answer = "".join("%s %s\n" % (i, st) for i, st in rows)
    (bindir / "squeue").write_text('#!/bin/bash\necho "$@" > "%s"\n%s\nprintf %%b %r\n' % (
        tmp_path / "squeue.args", "exit 1" if queue is None else ":", answer))
    (bindir / "squeue").chmod(0o755)
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], USER=os.environ.get("USER") or "me",
               KAOS_SWEEP_MIN_AGE_MIN=age)
    if self_id is not None:
        env["KAOS_SWEEP_SELF"] = self_id
    if allow_empty:
        env["KAOS_SWEEP_ALLOW_EMPTY"] = "1"
    script = "\n".join(jobscript.SWEEP_HELPER) + '\nkaos_sweep "%s"\n' % (str(scr) if root is None else root)
    assert subprocess.run(["bash", "-c", script], env=env).returncode == 0
    return scr, sorted(f for f in folders if (scr / f).exists())


def test_the_sweep_removes_the_folders_of_jobs_that_are_over_and_keeps_the_others(tmp_path):
    scr, left = _sweep(tmp_path, ["200", "400", "9"])
    assert left == ["kaos-priv/host-200", "kaos-tmp/in-400"], "only the folders whose job is still queued or running stay"
    assert (scr / "kaos-priv").is_dir() and (scr / "kaos-tmp").is_dir()


def test_the_sweep_removes_nothing_when_squeue_cannot_answer(tmp_path):
    _, left = _sweep(tmp_path, None)
    assert len(left) == 5, "unknown never means delete"


def test_the_sweep_distrusts_an_answer_that_lacks_the_job_that_is_asking(tmp_path):
    _, left = _sweep(tmp_path, [], self_id="777")           # a running job is always in squeue: an empty answer is wrong
    assert len(left) == 5
    _, left = _sweep(tmp_path / "b", ["777"], self_id="777")
    assert left == [], "an answer that names the asking job is believed"


def test_the_sweep_asks_for_every_job_of_the_user_that_is_not_finished(tmp_path):
    """squeue without -t lists only pending, running and completing jobs, and without -a leaves out hidden partitions
    (Slurm squeue documentation: `-t`: "If no state is specified then pending, running, and completing jobs are
    reported"; `-a`: "information about jobs ... in all partitions ... configured as hidden")."""
    queue = ["100:SUSPENDED", "200:COMPLETING", "300:REQUEUED", "400:CANCELLED", "500:COMPLETED", "9:FAILED"]
    scr, left = _sweep(tmp_path, queue)
    args = (tmp_path / "squeue.args").read_text().split()
    assert "-a" in args and args[args.index("-t") + 1] == "all", args
    assert left == ["kaos-priv/host-100", "kaos-priv/host-200", "kaos-tmp/in-300"], \
        "a job in any state that is not finished keeps its folders; finished ones do not"


def test_the_sweep_with_an_empty_answer_and_no_asking_job_removes_nothing_unless_told_it_may(tmp_path):
    _, left = _sweep(tmp_path, [])
    assert len(left) == 5, "an empty answer cannot be told from a squeue that lost the queue"
    _, left = _sweep(tmp_path / "b", [], allow_empty=True)
    assert left == [], "a caller that checked the queue itself says so with KAOS_SWEEP_ALLOW_EMPTY=1"


def test_the_sweep_does_nothing_when_the_asking_job_has_an_empty_id(tmp_path):
    _, left = _sweep(tmp_path, ["9"], self_id="")
    assert len(left) == 5
    # and the job script's own call is guarded the same way
    call = next(ln for ln in jobscript.batch_lines("-", "/scr") if "kaos_sweep" in ln and ln.startswith("[ -z"))
    assert 'SLURM_JOB_ID' in call and call.startswith('[ -z "$SLURM_JOB_ID" ] ||')
    scr = tmp_path / "c" / "scr"
    (scr / "kaos-tmp" / "in-300").mkdir(parents=True)
    (tmp_path / "c" / "bin").mkdir()
    (tmp_path / "c" / "bin" / "squeue").write_text('#!/bin/bash\nprintf "9 RUNNING\\n"\n')
    (tmp_path / "c" / "bin" / "squeue").chmod(0o755)
    env = dict(os.environ, PATH=str(tmp_path / "c" / "bin") + os.pathsep + os.environ["PATH"],
               USER=os.environ.get("USER") or "me",
               KAOS_SWEEP_MIN_AGE_MIN="0", SLURM_JOB_ID="", SCR=str(scr))
    env.pop("KAOS_SWEEP_SELF", None)
    assert subprocess.run(["bash", "-c", "\n".join(jobscript.SWEEP_HELPER) + "\n" + call], env=env).returncode == 0
    assert (scr / "kaos-tmp" / "in-300").is_dir()


def test_the_sweep_leaves_young_folders_alone(tmp_path):
    _, left = _sweep(tmp_path, ["1"], age="10")
    assert len(left) == 5


def test_the_sweep_never_follows_a_link_and_never_touches_a_name_it_does_not_know(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("x")
    scr = tmp_path / "scr"
    (scr / "kaos-priv").mkdir(parents=True)
    (scr / "kaos-tmp").mkdir()
    os.symlink(str(outside), str(scr / "kaos-priv" / "host-900"))                 # a link named like a job's folder
    os.symlink(str(outside), str(scr / "kaos-tmp" / "in-901"))
    for odd in ODD:
        (scr / odd).mkdir(parents=True)
    os.symlink(str(outside), str(scr / "sif-cache"))
    _, _ = _sweep(tmp_path / "unused", [], folders=())
    bindir = tmp_path / "unused" / "bin"
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], USER=os.environ.get("USER") or "me",
               KAOS_SWEEP_MIN_AGE_MIN="0")
    script = "\n".join(jobscript.SWEEP_HELPER) + '\nkaos_sweep "%s"\n' % scr
    assert subprocess.run(["bash", "-c", script], env=env).returncode == 0
    assert (outside / "keep").read_text() == "x", "a link was followed"
    for odd in ODD:
        assert (scr / odd).is_dir(), odd


@pytest.mark.parametrize("root", ["", "/", "relative/path", "/nonexistent/scratch"])
def test_the_sweep_does_nothing_for_an_empty_or_unsafe_root(tmp_path, root):
    _, left = _sweep(tmp_path, [], root=root)
    assert len(left) == 5


def test_a_job_script_sweeps_what_a_killed_job_left_when_it_starts(tmp_path, lakefs, monkeypatch):
    """Node loss or SIGKILL leaves the host's private script copy (it holds the bearer) and the folders of the step."""
    site = HSite(tmp_path, lakefs, False, monkeypatch)
    for f in ("kaos-priv/host-4999", "kaos-tmp/in-4999", "kaos-tmp/env-4999"):
        (site.scratch / f).mkdir(parents=True)
        (site.scratch / f / "job.sh").write_text("bearer " + WALLET)
    (tmp_path / "bin").mkdir(exist_ok=True)
    (tmp_path / "bin" / "squeue").write_text('#!/bin/bash\nprintf "5001\\n"\n')
    (tmp_path / "bin" / "squeue").chmod(0o755)
    assert site.run(extra={"KAOS_SWEEP_MIN_AGE_MIN": "0", "USER": os.environ.get("USER") or "me"}) == 0, site.out
    assert not [f for f in ("kaos-priv/host-4999", "kaos-tmp/in-4999", "kaos-tmp/env-4999") if (site.scratch / f).exists()]
    assert site.leaked() == []
    assert "kaos_sweep" in site.batch and site.batch.index("kaos_sweep \"$SCR\"") < site.batch.index("kaos-priv/host-")


# ---------------------------------------------------------------- 7. one grammar for an image reference

@pytest.mark.parametrize("ref", ["zot.example/p/app:@sha256:" + "a" * 64, "zot.example/p/app:", "zot.example/p/app", "app:v1"])
def test_every_use_of_a_reference_goes_through_the_one_grammar_and_refuses_what_it_refuses(ref, monkeypatch):
    from kaos_hpc_runner import image, wallet
    assert settings.image_problem(ref) != "" and settings.parse_image(ref) is None
    with pytest.raises(ValueError):
        image.parse_image(ref)
    calls = []
    def refuse(*a, **k):
        calls.append(a)
        raise OSError("net")
    monkeypatch.setattr(wallet.urllib.request, "urlopen", refuse)
    assert wallet.fetch_workdir(ref, "") == "" and calls == [], "no registry is asked about a reference that does not parse"


@pytest.mark.parametrize("ref,want", [
    ("zot.example/p/app:v1", ("zot.example", "p/app", "v1")),
    ("zot.example/p/app@sha256:" + "b" * 64, ("zot.example", "p/app", "sha256:" + "b" * 64)),
    ("zot.example:5000/p/app:v1@sha256:" + "b" * 64, ("zot.example:5000", "p/app", "sha256:" + "b" * 64)),
])
def test_the_image_stager_and_the_workdir_lookup_read_host_repository_and_reference_from_the_grammar(ref, want, monkeypatch):
    from kaos_hpc_runner import image, wallet
    assert image.parse_image(ref) == want
    urls = []

    def fake(req, timeout=0):
        urls.append(req.full_url)
        raise OSError("stop")
    monkeypatch.setattr(wallet.urllib.request, "urlopen", fake)
    wallet.fetch_workdir(ref, "")
    assert urls == ["https://%s/v2/%s/manifests/%s" % want]


def test_a_reference_with_an_empty_tag_is_not_rewritten_into_a_valid_one_before_it_is_checked():
    ref = "zot.example/p/app:@sha256:" + "a" * 64
    assert settings.normalize_image(ref) == ref and settings.image_problem(settings.normalize_image(ref)) != ""
    assert settings.normalize_image("zot.example/p/app:v1@sha256:" + "a" * 64) == "zot.example/p/app@sha256:" + "a" * 64


def test_a_slot_memory_figure_with_a_leading_zero_is_decimal_and_a_bad_one_never_aborts_the_step(tmp_path, lakefs, monkeypatch):
    slot = {"KUBECORE_SLOT_SHARED": "1", "KUBECORE_SLOT_GPUS": "1", "KUBECORE_SLOT_CPUS": "08", "KAOS_STEP_GPUS": "2",
            "CUDA_VISIBLE_DEVICES": "2", "SLURM_STEP_ID": "3"}
    for figure, want in (("08", str(8 * 1024)), ("09", str(9 * 1024)), ("100", str(100 * 1024)), ("0x10", None), ("-5", None),
                         ("1e3", None), ("99999999999999999999", None), ("", None)):
        where = tmp_path / ("m" + (figure.replace("-", "n") or "empty"))
        where.mkdir()
        site = HSite(where, lakefs, True, monkeypatch)
        assert site.run(extra=dict(HOST_WIDE, KUBECORE_SLOT_MEM_GIB=figure, **slot)) == 0, (figure, site.out)
        assert site.step_saw().get("SLURM_MEM_PER_NODE") == want, figure
        assert "syntax error" not in site.out and "value too great" not in site.out, site.out


# ---- a private key block is bounded; a marker in prose is not a block

EIGHT = ["step 1 done", "loss=0.5", 'KAOS_METRIC {"t":1,"gpus":[],"load":[0,0,0],"mem_available_kb":1}', "epoch 2", "epoch 3",
         "checkpoint saved", "step 4 done", "finished"]


def test_a_begin_marker_without_an_end_does_not_silence_what_follows():
    text = "error: expected -----BEGIN RSA PRIVATE KEY----- header in file\n" + "\n".join(EIGHT) + "\n"
    for out in (_node_redact(text), Redactor().text(text)):
        assert out == text, "nothing is a secret here, and every later line (the metric line too) is kept: " + out


def test_a_block_that_starts_a_line_is_dropped_up_to_its_end_and_no_further():
    body = ["MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo", "4lgOEePzNm0tRgeLezV6ffAt0gunAAAA", "AQIDAQAB"]
    text = "\n".join(["before", "  -----BEGIN RSA PRIVATE KEY-----"] + body + ["-----END RSA PRIVATE KEY-----"] + EIGHT) + "\n"
    out = _node_redact(text)
    assert out == "before\n[redacted: private key]\n" + "\n".join(EIGHT) + "\n", out


def test_a_block_with_no_end_ends_at_the_first_line_that_is_not_key_material_and_a_metric_line_is_never_dropped():
    lines = ["-----BEGIN PRIVATE KEY-----", "MIIEowIBAAKCAQEAu1SU1LfVLPHCozMxH2Mo", EIGHT[2], "after"]
    out = _node_redact("\n".join(lines) + "\n").split("\n")
    assert out[0] == "[redacted: private key]" and out[1:3] == [EIGHT[2], "after"], out


def test_a_block_swallows_at_most_a_hundred_lines():
    lines = ["-----BEGIN PRIVATE KEY-----"] + ["A" * 64] * 150 + ["done"]
    out = _node_redact("\n".join(lines) + "\n").split("\n")
    assert out[0] == "[redacted: private key]" and out[-2] == "done", "the stream resumes"
    assert len(out) - 2 < 100 and sum(1 for ln in out if ln == "A" * 64) >= 50, "lines past the bound are shown, not swallowed"


def test_a_marker_in_the_middle_of_prose_starts_no_block():
    r = Redactor()
    assert r.line("see the -----BEGIN RSA PRIVATE KEY----- line") == "see the -----BEGIN RSA PRIVATE KEY----- line"
    assert r.line("next line is shown") == "next line is shown"


def test_the_held_bearers_are_redacted_in_hex_and_with_json_escaped_slashes():
    import binascii
    held = "ab/cd+ef/gh-ijklmnop0123456789"
    import base64
    b64 = base64.b64encode(held.encode()).decode()
    spellings = [binascii.hexlify(held.encode()).decode(), binascii.hexlify(held.encode()).decode().upper(),
                 held.replace("/", "\\/"), b64.replace("/", "\\/"), b64]
    text = "".join("[%s]\n" % sp for sp in spellings)
    out = _node_redact(text, env={"MLFLOW_TRACKING_TOKEN": held})
    for sp in spellings:
        assert sp not in out, sp
    assert out.count("[redacted]") == len(spellings)
