"""The Slurm batch script (jobscript.batch_lines) and the on-node payloads, run in bash/python against
fakes: caches, the SIF pull and build, the stop signal, the checkpoint stage-out and stage-in, the failure
comment, the inputs in the script. Ported from the waiter tests (hera-pipeline-template).
"""
import ast
import base64
import hashlib
import io
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from kaos_hpc_runner.sharing import inputs_in_script

ROOT = Path(__file__).resolve().parents[1]
from kaos_hpc_runner import jobscript, payloads  # noqa: E402

# The dataset stager's lease pattern (kubecore-operator images/meluxina-tokenmint/stager/stager.py,
# LEASE_RE), applied with findall over `squeue -o %k`.
LEASE_RE = re.compile(r'kaos-commit=([0-9a-f]{6,64})')
OLD = time.time() - 20 * 86400


def _job_script_lines():
    """The job script's lines, sharing off."""
    return jobscript.batch_lines("-", "/scratch")


_job_lines = _job_script_lines

CKPTOUT, CKPTIN = payloads.CKPTOUT, payloads.CKPTIN


class FakeLakeFS:
    def __init__(self):
        self.objects = {}      # (branch, path) -> bytes
        self.commits = {}      # id -> {path: bytes}
        self.commit_meta = {}
        self.branches = set()
        self.fail_uploads = 0  # answer this many uploads with 500
        self.truncate = False  # serve object downloads one byte short
        srv = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body=b"", ctype="application/json"):
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _parts(self):
                u = urllib.parse.urlsplit(self.path)
                return u.path.split("/")[3:], dict(urllib.parse.parse_qsl(u.query))

            def do_POST(self):
                assert self.headers["Authorization"] == "Bearer wallet"
                parts, q = self._parts()
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                # repositories/<repo>/branches
                if parts[2:] == ["branches"]:
                    name = json.loads(body)["name"]
                    if name in srv.branches:
                        return self._send(409, {})
                    srv.branches.add(name)
                    return self._send(201, {})
                # repositories/<repo>/branches/<b>/objects
                if parts[2] == "branches" and parts[4] == "objects":
                    if srv.fail_uploads:
                        srv.fail_uploads -= 1
                        return self._send(500, {})
                    head, rest = body.split(b"\r\n\r\n", 1)
                    assert rest.endswith(b"\r\n--B--\r\n") and b'name="content"' in head
                    srv.objects[(parts[3], q["path"])] = rest[:-len(b"\r\n--B--\r\n")]
                    return self._send(201, {})
                # repositories/<repo>/branches/<b>/commits
                if parts[2] == "branches" and parts[4] == "commits":
                    snap = {p: d for (b, p), d in srv.objects.items() if b == parts[3]}
                    cid = hashlib.sha256(repr(sorted(snap.items())).encode()).hexdigest()
                    srv.commits[cid] = snap
                    srv.commit_meta[cid] = json.loads(body)["metadata"]
                    return self._send(201, {"id": cid})
                self._send(404, {})

            def do_GET(self):
                assert self.headers["Authorization"] == "Bearer wallet"
                parts, q = self._parts()
                if parts[2] == "commits":
                    return self._send(200, {"id": parts[3]}) if parts[3] in srv.commits else self._send(404, {})
                ref = parts[3]
                view = srv.commits.get(ref) or {p: d for (b, p), d in srv.objects.items() if b == ref}
                if parts[4:] == ["objects", "stat"]:
                    d = view.get(q["path"])
                    return self._send(200, {"size_bytes": len(d)}) if d is not None else self._send(404, {})
                if parts[4:] == ["objects", "ls"]:
                    keys = sorted(k for k in view if k.startswith(q["prefix"]) and k > q.get("after", ""))
                    page, more = keys[:2], len(keys) > 2          # tiny pages: pagination is exercised
                    return self._send(200, {"results": [{"path": k, "path_type": "object",
                                                         "size_bytes": len(view[k])} for k in page],
                                            "pagination": {"has_more": more,
                                                           "next_offset": page[-1] if page else ""}})
                if parts[4:] == ["objects"]:
                    d = view.get(q["path"])
                    if d is None:
                        return self._send(404, {})
                    return self._send(200, d[:-1] if srv.truncate else d, "application/octet-stream")
                self._send(404, {})

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()


@pytest.fixture
def lakefs():
    srv = FakeLakeFS()
    yield srv
    srv.httpd.shutdown()


def _env(tmp_path, lakefs, **extra):
    env = dict(os.environ, LAKEFS_ENDPOINT=lakefs.url, LAKEFS_BEARER_TOKEN="wallet", OUTPUTS_REPO="outputs-app",
               OUTPUTS_REF="main", WF_UID="wf1", STEP_NAME="train", KAOS_ATTEMPT="2",
               KAOS_WORK=str(tmp_path / "work"), KAOS_CKPT_FILE=str(tmp_path / "ckpt.ref"))
    env.update(extra)
    return env


def _py(payload, env):
    """Run an on-node payload; retries sleep for nothing."""
    code = "import time; time.sleep = lambda s: None\n" + payload
    return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)


BIG = os.urandom(3 * (1 << 20) + 17)     # more than one 1 MiB chunk, not a multiple of it


FILES = {"model.pt": BIG, "optim/state.bin": b"adam", "meta/epoch": b"7"}


PREFIX = "hpc-checkpoints/wf1/train/attempt-2/"


def _staged_out(tmp_path, lakefs):
    _checkpoint(tmp_path, FILES)
    assert _py(CKPTOUT, _env(tmp_path, lakefs)).returncode == 0
    return (tmp_path / "ckpt.ref").read_text().split()


def _make(path, old):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    if old:
        os.utime(path, (OLD, OLD))


def test_caches_are_cleaned_by_last_use(tmp_path):
    lines = _job_script_lines()
    cleanup = [l for l in lines if l.startswith(("touch -c", "find $SCR/", "apptainer cache clean"))]
    assert len(cleanup) == 5, cleanup
    scr = tmp_path / "scratch"
    _make(scr / "sif-cache" / "old.sif", old=True)
    _make(scr / "sif-cache" / "recent.sif", old=False)
    _make(scr / "sif-cache" / "inuse.sif", old=True)        # this job's image
    _make(scr / "sif-cache" / "dead.sif.part-1-2", old=True)  # a pull killed mid-way (PRD-1609 F-02)
    _make(scr / "sif-cache" / "live.sif.part-3-4", old=False)  # a pull still running
    _make(scr / "data-cache" / "repo" / "c1" / ".complete", old=True)
    _make(scr / "data-cache" / "repo" / "c1" / "f", old=True)
    _make(scr / "data-cache" / "repo" / "c2" / ".complete", old=False)
    _make(scr / "data-cache" / "repo" / "c3" / "partial", old=True)  # no marker: left alone
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text('#!/bin/sh\necho "$@" > "$SCR/apptainer-args"\n')
    (bindir / "apptainer").chmod(0o755)
    env = dict(os.environ, SCR=str(scr), SIF=str(scr / "sif-cache" / "inuse.sif"),
               USER=os.environ.get("USER") or os.getlogin(),
               PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    subprocess.run(["bash", "-c", "\n".join(cleanup)], env=env, check=True)

    assert sorted(p.name for p in (scr / "sif-cache").iterdir()) == ["inuse.sif", "live.sif.part-3-4", "recent.sif"]
    assert sorted(p.name for p in (scr / "data-cache" / "repo").iterdir()) == ["c2", "c3"]
    assert (scr / "apptainer-args").read_text().split() == ["cache", "clean", "--force", "--days", "14"]


def test_this_jobs_image_and_download_are_touched_before_the_cleanup():
    lines = _job_script_lines()
    touch = lines.index('touch -c "$SIF"')
    first_clean = next(i for i, l in enumerate(lines) if l.startswith("find $SCR/"))
    assert touch < first_clean
    assert "(dest / '.complete').touch()" in payloads.STAGEIN


def _pull_block():
    """From `if [ ! -f "$SIF" ]` to its `fi`: the job's image pull."""
    lines = _job_script_lines()
    start = lines.index('if [ ! -f "$SIF" ]; then')
    end = lines.index("fi", start)
    return lines[start:end + 1]


def _run_pull(tmp_path, apptainer_body):
    scr = tmp_path / "scratch"
    (scr / "sif-cache").mkdir(parents=True)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text("#!/bin/bash\n" + apptainer_body)
    (bindir / "apptainer").chmod(0o755)
    sif = scr / "sif-cache" / "abc.sif"
    script = "\n".join(['fail(){ echo "$1" > "$SCR/failed"; exit "$1"; }'] + _pull_block())
    env = dict(os.environ, SCR=str(scr), SIF=str(sif), IMAGE_REF="zot.example/p/app:v1",
               SLURM_JOB_ID="5417591", PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    rc = subprocess.run(["bash", "-c", script], env=env).returncode
    return rc, scr, sif


def test_the_pull_never_writes_the_shared_name(tmp_path):
    # Fake pull: record the target, write it in two chunks.
    rc, scr, sif = _run_pull(tmp_path, 'echo "$2" > "$SCR/target"; printf half > "$2"; printf full >> "$2"\n')
    assert rc == 0
    target = (scr / "target").read_text().strip()
    assert target != str(sif), "a reader must never see a half-written SIF at the shared name"
    assert "5417591" in target, "the temporary name belongs to this job"
    assert sif.read_text() == "halffull"
    assert sorted(p.name for p in (scr / "sif-cache").iterdir()) == ["abc.sif"]


def test_an_image_another_job_finished_first_is_kept(tmp_path):
    # Another job finishes its rename while this one is still pulling.
    rc, scr, sif = _run_pull(tmp_path, 'printf winner > "$SIF"; printf mine > "$2"\n')
    assert rc == 0
    assert sif.read_text() == "winner"
    assert sorted(p.name for p in (scr / "sif-cache").iterdir()) == ["abc.sif"], "no part-file left"


def test_a_failed_pull_leaves_nothing_and_fails_231(tmp_path):
    rc, scr, sif = _run_pull(tmp_path, 'printf partial > "$2"; exit 1\n')
    assert rc == 231 and (scr / "failed").read_text().strip() == "231"
    assert list((scr / "sif-cache").iterdir()) == [], "no half image, no part-file"


def test_stale_part_files_are_cleaned_after_half_a_day():
    lines = _job_script_lines()
    assert any(l.startswith('find $SCR/sif-cache') and '*.sif.part-*' in l and '-mmin +720' in l
               for l in lines), "a pull killed mid-way must not leak its part-file forever"


def _run_job(tmp_path, staged, build_ok=True, pull_ok=True, preexisting_sif=False):
    lines = _job_lines()
    start = lines.index('if [ ! -f "$SIF" ]; then')
    end = lines.index("fi", start)
    scr = tmp_path / "scratch"
    (scr / "sif-cache").mkdir(parents=True)
    tar = scr / "kaos" / "p" / "images" / "k.tar"
    tar.parent.mkdir(parents=True)
    if staged:
        tar.write_bytes(b"tar")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text(
        '#!/bin/bash\necho "$1 $3" >> "$SCR/calls"\n'
        'if [ "$1" = build ]; then [ "%d" = 1 ] || { printf half > "$2"; exit 1; }; fi\n'
        'if [ "$1" = pull ]; then [ "%d" = 1 ] || exit 1; fi\n'
        'printf sif > "$2"\n' % (build_ok, pull_ok))
    (bindir / "apptainer").chmod(0o755)
    sif = scr / "sif-cache" / "abc.sif"
    if preexisting_sif:
        sif.write_text("cached")
    script = "\n".join(['fail(){ echo "$1" > "$SCR/failed"; exit "$1"; }'] + lines[start:end + 1])
    env = dict(os.environ, SCR=str(scr), SIF=str(sif), IMAGE_REF="europe-docker.pkg.dev/p/app@sha256:ab",
               SLURM_JOB_ID="1", REG_TOKEN="expired", PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    if staged is not None:
        env["IMAGE_STAGED"] = str(tar)
    rc = subprocess.run(["bash", "-c", script], env=env, stdout=subprocess.PIPE).returncode
    calls = (scr / "calls").read_text().splitlines() if (scr / "calls").exists() else []
    return rc, calls, scr, sif, tar


def test_job_builds_from_the_staged_layers_and_never_touches_the_registry(tmp_path):
    rc, calls, scr, sif, tar = _run_job(tmp_path, staged=True)
    assert rc == 0 and calls == ["build docker-archive://%s" % tar]
    assert sif.read_text() == "sif"
    assert sorted(p.name for p in sif.parent.iterdir()) == ["abc.sif"], "part-file renamed away"


def test_job_pulls_when_nothing_is_staged(tmp_path):
    rc, calls, *_ = _run_job(tmp_path, staged=None)
    assert rc == 0 and [c.split()[0] for c in calls] == ["pull"]
    rc, calls, *_ = _run_job(tmp_path / "b", staged=False)       # path given, file missing
    assert rc == 0 and [c.split()[0] for c in calls] == ["pull"]


def test_job_falls_back_to_the_pull_when_the_build_fails_and_leaves_no_part_file(tmp_path):
    rc, calls, scr, sif, _ = _run_job(tmp_path, staged=True, build_ok=False)
    assert rc == 0 and [c.split()[0] for c in calls] == ["build", "pull"]
    assert sorted(p.name for p in sif.parent.iterdir()) == ["abc.sif"]
    rc, calls, scr, sif, _ = _run_job(tmp_path / "b", staged=True, build_ok=False, pull_ok=False)
    assert rc == 231 and list(sif.parent.iterdir()) == []


def test_a_cached_sif_needs_neither(tmp_path):
    rc, calls, *_ = _run_job(tmp_path, staged=True, preexisting_sif=True)
    assert rc == 0 and calls == []


def test_per_job_paths_key_on_the_steps_own_job():
    lines = _job_script_lines()
    assert lines.index("set +e") < lines.index("KAOS_JOB_ID=${KAOS_JOB_ID:-$SLURM_JOB_ID}") \
        < next(i for i, l in enumerate(lines) if l.startswith("fail(){"))
    # The batch step's own id is used only by the default itself, the pull's private
    # temp name (unique per process anyway) and the host hook (F-03), which runs in the
    # batch step before any step exists.
    hook = lines[lines.index('if [ -z "$KAOS_IN_STEP" ] && [ -n "$KAOS_SLOTS_B64" ]; then'):]
    hook = hook[:hook.index("fi") + 1]
    uses = [l for l in lines if "$SLURM_JOB_ID" in l and l not in hook]
    assert len(uses) == 2 and any(l.startswith("KAOS_JOB_ID=") for l in uses) \
        and any("SIF_TMP=" in l for l in uses), uses
    assert any("kaos-work/$KAOS_JOB_ID" in l for l in lines)
    assert any("ds-$KAOS_JOB_ID.dir" in l for l in lines)


@pytest.mark.parametrize("commit, expected_prefix", [("c0ffee12", "kaos-commit=c0ffee12 err7:"), ("", "err7:")])
def test_a_failure_comment_keeps_the_dataset_lease(tmp_path, commit, expected_prefix):
    fail = next(l for l in _job_script_lines() if l.startswith("fail(){"))
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "scontrol").write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$OUT"\n')
    (bindir / "scontrol").chmod(0o755)
    (tmp_path / "slurm-4242.out").write_text("pulling\nboom\n")
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], OUT=str(tmp_path / "args"),
               KAOS_JOB_ID="4242", KAOS_COMMIT=commit)
    rc = subprocess.run(["bash", "-c", fail + "\nfail 7"], cwd=tmp_path, env=env).returncode
    assert rc == 7
    args = (tmp_path / "args").read_text().split("\n")
    assert args[:2] == ["update", "JobId=4242"], "the comment goes to the step's own job, also on a host"
    comment = args[2]
    assert comment.startswith("Comment=" + expected_prefix) and "boom" in comment
    assert LEASE_RE.findall(comment) == ([commit] if commit else [])


def test_inputs_ride_in_the_script_and_survive_any_value(tmp_path):
    tricky = "python -c 'print(\"a b\")' && echo $HOME \\\n `x` ; done"
    env = ["PATH=/usr/bin:/bin", "HOME=/home/u", "USER=u", "STEP_CMD=" + tricky,
           "LAKEFS_BEARER_TOKEN=eyJ.a=b.c", "EMPTY="]
    batch = "\n".join(["#!/bin/bash -l", "# kaos-job v2 scope=a1b2c3d4e5", "set +e",
                       "KAOS_JOB_ID=${KAOS_JOB_ID:-$SLURM_JOB_ID}",
                       'printf "%s|%s|%s|%s" "$STEP_CMD" "$LAKEFS_BEARER_TOKEN" "${EMPTY-unset}" "$HOME" > "$OUT"'])
    script, slurm_env = inputs_in_script(batch, env)
    assert slurm_env == ["PATH=/usr/bin:/bin", "HOME=/home/u", "USER=u"], "secrets leave the Slurm environment"
    lines = script.split("\n")
    assert lines[:3] == ["#!/bin/bash -l", "# kaos-job v2 scope=a1b2c3d4e5", "set +e"], "header stays on top"
    assert not any(l.startswith(("export PATH=", "export HOME=", "export USER=")) for l in lines), \
        "the login shell's PATH is never overwritten"
    (tmp_path / "job.sh").write_text(script)
    out = tmp_path / "out"
    subprocess.run(["bash", str(tmp_path / "job.sh")], check=True,
                   env={"PATH": os.environ["PATH"], "HOME": "/home/u", "OUT": str(out)})
    assert out.read_text() == tricky + "|eyJ.a=b.c||/home/u"


def _block(first, last):
    lines = _job_script_lines()
    start = lines.index(first)
    return lines[start:lines.index(last, start) + 1]


def _checkpoint(tmp_path, files):
    root = tmp_path / "work" / "output" / "checkpoint"
    for name, data in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(data)


def _bin(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    (bindir / "scontrol").write_text('#!/bin/bash\nprintf "%s\\n" "$@" >> "$SCONTROL_LOG"\n')
    (bindir / "scontrol").chmod(0o755)
    if not (bindir / "python3").exists():
        (bindir / "python3").symlink_to(sys.executable)
    return bindir


def _end_of_job(tmp_path, lakefs, rc, **extra):
    """The script from the step's exit code to the job's end."""
    block = _block('if [ $rc -eq 85 ]; then', 'exit 0')
    _checkpoint(tmp_path, FILES)
    env = _env(tmp_path, lakefs, PATH=str(_bin(tmp_path)) + os.pathsep + os.environ["PATH"],
               SCONTROL_LOG=str(tmp_path / "scontrol.log"), KAOS_JOB_ID="4242", KAOS_COMMIT="c0ffee12",
               CKPTOUT_B64=base64.b64encode(("import time; time.sleep = lambda s: None\n" + CKPTOUT).encode()).decode(),
               STEP_OUTPUTS="")
    env.pop("KAOS_CKPT_FILE")
    env.update(extra)
    script = "\n".join([FAIL, "rc=%d" % rc] + block)
    p = subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True)
    log = (tmp_path / "scontrol.log").read_text() if (tmp_path / "scontrol.log").exists() else ""
    return p.returncode, log, p.stdout


def _resume(tmp_path, lakefs, **extra):
    block = _block('if [ -n "$KAOS_RESUME_REF" ]; then', "fi")
    env = _env(tmp_path, lakefs, PATH=str(_bin(tmp_path)) + os.pathsep + os.environ["PATH"],
               SCONTROL_LOG=str(tmp_path / "scontrol.log"), KAOS_JOB_ID="4243", KAOS_WORK=str(tmp_path / "next"),
               CKPTIN_B64=base64.b64encode(CKPTIN.encode()).decode())
    env.update(extra)
    script = "\n".join([FAIL] + block + ['echo "resume=$APPTAINERENV_KAOS_RESUME_PATH"'])
    return subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True)


def _run(tmp_path, step_cmd):
    bindir = _bin(tmp_path)
    # Like `apptainer exec [opts] SIF cmd...`: drop everything up to the image, run the rest. First it
    # starts a stand-in for Apptainer's squashfuse in the same process group, with USR1's default
    # action (die): live job 5421507 lost its image when the whole group was signalled.
    (bindir / "apptainer").write_text(
        '#!/bin/bash\nsleep 60 & echo $! > fuse.pid\n'
        'while [ "$1" != "$SIF" ]; do shift; done; shift; exec "$@"\n')
    (bindir / "apptainer").chmod(0o755)
    script = "\n".join(_block('[ -n "$STEP_CMD" ] || STEP_CMD="nvidia-smi -L || nproc"', "trap - USR1")
                       + ['exit $rc'])
    env = dict(os.environ, SIF="img.sif", STEP_CMD=step_cmd, NV="", PWD_OPT="", BIND="",
               PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    p = subprocess.Popen(["bash", "-c", script], cwd=tmp_path, env=env)
    for _ in range(200):
        if (tmp_path / "ready").exists():
            break
        time.sleep(0.05)
    os.kill(p.pid, signal.SIGUSR1)        # what Slurm does: the batch shell only (BATCH_JOB)
    rc = p.wait(timeout=30)
    fuse = int((tmp_path / "fuse.pid").read_text())
    try:
        os.kill(fuse, 0)                  # still alive: the signal only went to handlers
        alive = True
    except ProcessLookupError:
        alive = False
    finally:
        try:
            os.kill(fuse, signal.SIGKILL)
        except ProcessLookupError:
            pass
    assert alive, "a process without a USR1 handler was signalled (Apptainer's squashfuse would die)"
    return rc


FAIL = next(ln for ln in _job_script_lines() if ln.startswith("fail(){"))


HANDLER_STEP = ("python3 -c \"import signal,sys,time,pathlib; "
                "signal.signal(signal.SIGUSR1, lambda s,f: sys.exit(85)); "
                "pathlib.Path('ready').write_text('1'); time.sleep(30); sys.exit(3)\"")


PLAIN_STEP = "python3 -c \"import pathlib,time; pathlib.Path('ready').write_text('1'); time.sleep(2)\"; exit 0"


def test_85_from_a_resumable_step_stages_the_checkpoint_out_before_the_job_ends(tmp_path, lakefs):
    rc, log, out = _end_of_job(tmp_path, lakefs, 85, KAOS_RESUMABLE="1")
    assert rc == 85, out
    (cid,) = lakefs.commits
    assert log.split("\n")[:3] == ["update", "JobId=4242",
                                   "Comment=kaos-commit=c0ffee12 kaos-ckpt=%s kaos-ckpt-path=%s" % (cid, PREFIX)]
    assert not (tmp_path / "work").exists(), "the job's scratch is removed once the checkpoint is committed"


def test_85_from_a_step_that_is_not_resumable_fails(tmp_path, lakefs):
    rc, log, out = _end_of_job(tmp_path, lakefs, 85)
    assert rc == 85 and "not declared resumable" in out
    assert "err85:" in log and "kaos-ckpt=" not in log and not lakefs.commits


def test_85_whose_checkpoint_does_not_upload_fails_235(tmp_path, lakefs):
    lakefs.fail_uploads = 100
    rc, log, _ = _end_of_job(tmp_path, lakefs, 85, KAOS_RESUMABLE="1")
    assert rc == 235 and "kaos-ckpt=" not in log and "err235:" in log


def test_85_without_a_wallet_fails_235(tmp_path, lakefs):
    rc, log, _ = _end_of_job(tmp_path, lakefs, 85, KAOS_RESUMABLE="1", LAKEFS_BEARER_TOKEN="")
    assert rc == 235 and not lakefs.commits


def test_a_step_that_finishes_ends_as_before(tmp_path, lakefs):
    rc, log, _ = _end_of_job(tmp_path, lakefs, 0, KAOS_RESUMABLE="1")
    assert rc == 0 and log == "" and not lakefs.commits


def test_a_continued_job_starts_from_the_checkpoint(tmp_path, lakefs):
    cid, prefix = _staged_out(tmp_path, lakefs)
    p = _resume(tmp_path, lakefs, KAOS_RESUME_REF=cid, KAOS_RESUME_PREFIX=prefix)
    assert p.returncode == 0 and "resume=/work/resume" in p.stdout
    assert (tmp_path / "next" / "resume" / "model.pt").read_bytes() == BIG


def test_a_continued_job_without_its_checkpoint_never_starts_from_scratch(tmp_path, lakefs):
    assert _resume(tmp_path, lakefs, KAOS_RESUME_REF="0" * 64, KAOS_RESUME_PREFIX=PREFIX).returncode == 234
    assert _resume(tmp_path, lakefs, KAOS_RESUME_REF="ab" * 32, KAOS_RESUME_PREFIX=PREFIX,
                   LAKEFS_BEARER_TOKEN="").returncode == 234


def test_a_first_job_has_no_resume_path(tmp_path, lakefs):
    p = _resume(tmp_path, lakefs)
    assert p.returncode == 0 and "resume=\n" in p.stdout


def test_the_stop_signal_reaches_a_step_inside_the_container_and_85_comes_back(tmp_path):
    assert _run(tmp_path, HANDLER_STEP) == 85


def test_a_step_without_a_handler_ignores_the_signal_and_finishes(tmp_path):
    assert _run(tmp_path, PLAIN_STEP) == 0


def test_stage_out_uploads_every_file_and_commits_it(tmp_path, lakefs):
    _checkpoint(tmp_path, FILES)
    r = _py(CKPTOUT, _env(tmp_path, lakefs))
    assert r.returncode == 0, r.stdout + r.stderr
    cid, prefix = (tmp_path / "ckpt.ref").read_text().split()
    assert prefix == PREFIX
    assert lakefs.commits[cid] == {PREFIX + k: v for k, v in FILES.items()}
    assert "hpc-ckpt-wf1-train" in lakefs.branches, "kept on its own branch, never the hand-off branch"
    assert lakefs.commit_meta[cid] == {"kaos.kind": "checkpoint", "workflow_uid": "wf1", "step": "train",
                                       "attempt": "2"}


def test_a_later_attempt_reuses_the_branch(tmp_path, lakefs):
    lakefs.branches.add("hpc-ckpt-wf1-train")          # attempt 1 created it
    _checkpoint(tmp_path, FILES)
    assert _py(CKPTOUT, _env(tmp_path, lakefs)).returncode == 0


def test_a_failed_upload_is_retried(tmp_path, lakefs):
    lakefs.fail_uploads = 2
    _checkpoint(tmp_path, FILES)
    assert _py(CKPTOUT, _env(tmp_path, lakefs)).returncode == 0
    assert len(lakefs.commits) == 1


def test_an_upload_that_keeps_failing_commits_nothing(tmp_path, lakefs):
    lakefs.fail_uploads = 100
    _checkpoint(tmp_path, FILES)
    r = _py(CKPTOUT, _env(tmp_path, lakefs))
    assert r.returncode != 0 and not lakefs.commits and not (tmp_path / "ckpt.ref").exists()


def test_without_a_checkpoint_nothing_is_committed(tmp_path, lakefs):
    (tmp_path / "work" / "output" / "checkpoint").mkdir(parents=True)
    r = _py(CKPTOUT, _env(tmp_path, lakefs))
    assert r.returncode != 0 and "nothing in" in r.stdout and not lakefs.commits


def test_stage_in_restores_the_checkpoint_at_its_commit(tmp_path, lakefs):
    cid, prefix = _staged_out(tmp_path, lakefs)
    lakefs.objects[("hpc-ckpt-wf1-train", PREFIX + "model.pt")] = b"changed after the commit"
    work = tmp_path / "next"
    r = _py(CKPTIN, _env(tmp_path, lakefs, KAOS_WORK=str(work), KAOS_RESUME_REF=cid, KAOS_RESUME_PREFIX=prefix))
    assert r.returncode == 0, r.stdout + r.stderr
    got = {p.relative_to(work / "resume").as_posix(): p.read_bytes()
           for p in (work / "resume").rglob("*") if p.is_file()}
    assert got == FILES, "exactly the committed files, nothing partial"


def test_stage_in_of_a_missing_checkpoint_fails(tmp_path, lakefs):
    r = _py(CKPTIN, _env(tmp_path, lakefs, KAOS_RESUME_REF="0" * 64, KAOS_RESUME_PREFIX=PREFIX))
    assert r.returncode != 0


def test_a_short_download_fails_and_leaves_no_file(tmp_path, lakefs):
    cid, prefix = _staged_out(tmp_path, lakefs)
    lakefs.truncate = True
    work = tmp_path / "next"
    r = _py(CKPTIN, _env(tmp_path, lakefs, KAOS_WORK=str(work), KAOS_RESUME_REF=cid, KAOS_RESUME_PREFIX=prefix))
    assert r.returncode != 0 and "short download" in r.stdout
    assert not [p for p in (work / "resume").rglob("*") if p.is_file() and not p.name.endswith(".part")]
