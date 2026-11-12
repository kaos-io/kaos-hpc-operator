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

CKPT = payloads.CKPT


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
                    # As a real server reads it: the part ends at the FIRST delimiter line of the request's boundary,
                    # and only the closing delimiter may follow.
                    boundary = self.headers["Content-Type"].split("boundary=", 1)[1].encode()
                    delimiter = b"\r\n--" + boundary
                    assert body.startswith(b"--" + boundary + b"\r\n")
                    head, rest = body.split(b"\r\n\r\n", 1)
                    assert b'name="content"' in head
                    content, tail = rest.split(delimiter, 1)
                    if tail != b"--\r\n":
                        return self._send(400, {"message": "malformed multipart body"})
                    srv.objects[(parts[3], q["path"])] = content
                    return self._send(201, {})
                # repositories/<repo>/branches/<b>/commits
                if parts[2] == "branches" and parts[4] == "commits":
                    snap = {p: d for (b, p), d in srv.objects.items() if b == parts[3]}
                    cid = hashlib.sha256(repr(sorted(snap.items())).encode()).hexdigest()
                    srv.commits[cid] = snap
                    srv.commit_meta[cid] = json.loads(body)["metadata"]
                    return self._send(201, {"id": cid})
                self._send(404, {})

            def do_DELETE(self):
                assert self.headers["Authorization"] == "Bearer wallet"
                parts, q = self._parts()
                if parts[2] == "branches" and parts[4:] == ["objects"]:
                    return self._send(204 if srv.objects.pop((parts[3], q["path"]), None) is not None else 404, b"")
                if parts[2] == "branches" and len(parts) == 4:
                    srv.branches.discard(parts[3])
                    for key in [k for k in srv.objects if k[0] == parts[3]]:
                        del srv.objects[key]
                    return self._send(204, b"")
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
                    keys = sorted(k for k in view if k.startswith(q.get("prefix", "")) and k > q.get("after", ""))
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
    env = dict(os.environ, LAKEFS_ENDPOINT=lakefs.url, LAKEFS_BEARER_TOKEN="wallet", KAOS_OUTPUTS_REPO="outputs-app",
               WF_UID="wf1", STEP_NAME="train", KAOS_ATTEMPT="2",
               KAOS_WORK=str(tmp_path / "work"), KAOS_CKPT_FILE=str(tmp_path / "ckpt.ref"))
    env.update(extra)
    return env


def _py(payload, env):
    """Run an on-node payload; retries sleep for nothing."""
    code = "import time; time.sleep = lambda s: None\n" + payload
    return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)


def _make(path, old):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    if old:
        os.utime(path, (OLD, OLD))


def test_caches_are_cleaned_by_last_use(tmp_path):
    lines = _job_script_lines()
    cleanup = [l for l in lines if l.startswith(("touch -c", "find $SCR/", "apptainer cache clean"))]
    assert len(cleanup) == 6, cleanup
    scr = tmp_path / "scratch"
    _make(scr / "sif-cache" / "old.sif", old=True)
    _make(scr / "sif-cache" / "recent.sif", old=False)
    _make(scr / "sif-cache" / "inuse.sif", old=True)        # this job's image
    _make(scr / "sif-cache" / "dead.sif.part-1-2", old=True)  # a pull killed mid-way (PRD-1609 F-02)
    _make(scr / "sif-cache" / "live.sif.part-3-4", old=False)  # a pull still running
    _make(scr / "data-cache" / "repo" / "c1" / ".kaos-complete", old=True)
    _make(scr / "data-cache" / "repo" / "c1" / "f", old=True)
    _make(scr / "data-cache" / "repo" / "c2" / ".kaos-complete", old=False)
    _make(scr / "data-cache" / "repo" / "c3" / "partial", old=True)  # no marker: left alone
    _make(scr / "data-cache" / "repo" / "c4" / ".complete", old=True)   # a copy with only the marker of before
    _make(scr / "data-cache" / "repo" / "c5" / ".complete", old=True)   # a dataset file named .complete ...
    _make(scr / "data-cache" / "repo" / "c5" / ".kaos-complete", old=False)   # ... in a copy used yesterday
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text('#!/bin/sh\necho "$@" > "$SCR/apptainer-args"\n')
    (bindir / "apptainer").chmod(0o755)
    env = dict(os.environ, SCR=str(scr), SIF=str(scr / "sif-cache" / "inuse.sif"),
               USER=os.environ.get("USER") or os.getlogin(),
               PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    subprocess.run(["bash", "-c", "\n".join(cleanup)], env=env, check=True)

    assert sorted(p.name for p in (scr / "sif-cache").iterdir()) == ["inuse.sif", "live.sif.part-3-4", "recent.sif"]
    assert sorted(p.name for p in (scr / "data-cache" / "repo").iterdir()) == ["c2", "c3", "c5"], \
        "and no lock or moved-away folder is left behind"
    assert (scr / "apptainer-args").read_text().split() == ["cache", "clean", "--force", "--days", "1"]


def test_this_jobs_image_and_download_are_touched_before_the_cleanup():
    lines = _job_script_lines()
    touch = lines.index('touch -c "$SIF"')
    first_clean = next(i for i, l in enumerate(lines) if l.startswith("find $SCR/"))
    assert touch < first_clean
    assert "os.utime(str(MARK), None)" in payloads.STAGEIN and "os.replace(tmp, str(MARK))" in payloads.STAGEIN


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
    assert lines.index("set +e") < lines.index("export KAOS_JOB_ID=${KAOS_JOB_ID:-$SLURM_JOB_ID}") \
        < next(i for i, l in enumerate(lines) if l.startswith("fail(){"))
    # The batch step's own id is used only by the default itself, the pull's private
    # temp name (unique per process anyway) and the host hook (F-03), which runs in the
    # batch step before any step exists.
    hook = lines[lines.index('if [ -z "$KAOS_IN_STEP" ] && [ -n "$KAOS_SLOTS_B64" ]; then'):]
    hook = hook[:hook.index("fi") + 1]
    uses = [l for l in lines if "$SLURM_JOB_ID" in l and l not in hook]
    assert len(uses) == 2 and any(l.startswith("export KAOS_JOB_ID=") for l in uses) \
        and any("SIF_TMP=" in l for l in uses), uses
    assert any("kaos-work/$KAOS_JOB_ID" in l for l in lines)
    assert any("ds-$KAOS_JOB_ID-$IN_NAME.dir" in l for l in lines)


@pytest.mark.parametrize("commit, expected_prefix", [("c0ffee12", "kaos-commit=c0ffee12 err7:"), ("", "err7:")])
def test_a_failure_comment_keeps_the_dataset_lease(tmp_path, commit, expected_prefix):
    fail = "\n".join(jobscript.COMMENT_HELPER + [next(l for l in _job_script_lines() if l.startswith("fail(){"))])
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "scontrol").write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$OUT"\n')
    (bindir / "scontrol").chmod(0o755)
    (tmp_path / "slurm-4242.out").write_text("pulling\nboom\n")
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], OUT=str(tmp_path / "args"),
               KAOS_JOB_ID="4242", KAOS_LEASES=("kaos-commit=" + commit) if commit else "")
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


def _fake_site(bindir):
    """A stateful site: `scontrol update JobId=N Comment=X` stores X for job N (and logs the arguments, one per
    line, to $SCONTROL_LOG) exactly as Slurm does; `squeue -j N -o %k` prints it. An update for another job id,
    or an empty one (JobId=), changes nothing: scontrol would refuse it."""
    (bindir / "scontrol").write_text(
        '#!/bin/bash\nprintf "%s\\n" "$@" >> "${SCONTROL_LOG:-/dev/null}"\n'
        '[ "$1" = update ] || exit 0\n'
        'id=${2#JobId=}; [ -n "$id" ] && [ "$id" = "$SITE_JOB" ] || { echo "scontrol: invalid job id" >&2; exit 1; }\n'
        '[ -n "$SITE_FILE" ] || exit 1\n'
        '[ -e "$SITE_FILE.ro" ] && exit 0\n'                 # a post-end / ignored update
        'for a in "$@"; do case "$a" in Comment=*) printf %s "${a#Comment=}" > "$SITE_FILE";; esac; done\n')
    (bindir / "squeue").write_text(
        '#!/bin/bash\ncase "$*" in *" -j "*) cat "$SITE_FILE" 2>/dev/null;; esac\n')
    for n in ("scontrol", "squeue"):
        (bindir / n).chmod(0o755)


def _bin(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    _fake_site(bindir)
    if not (bindir / "python3").exists():
        (bindir / "python3").symlink_to(sys.executable)
    return bindir


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


def _script_head():
    """The script from `set +e` to the fail() definition, as the node runs it: the job id (exported), the
    comment helper, fail(). The tests start every block from here so nothing the real script does not do
    (an exported job id, say) is assumed."""
    lines = _job_script_lines()
    return "\n".join(lines[lines.index("set +e"):next(i for i, ln in enumerate(lines) if ln.startswith("fail(){")) + 1])


FAIL = _script_head()


HANDLER_STEP = ("python3 -c \"import signal,sys,time,pathlib; "
                "signal.signal(signal.SIGUSR1, lambda s,f: sys.exit(85)); "
                "pathlib.Path('ready').write_text('1'); time.sleep(30); sys.exit(3)\"")


PLAIN_STEP = "python3 -c \"import pathlib,time; pathlib.Path('ready').write_text('1'); time.sleep(2)\"; exit 0"


def test_the_stop_signal_reaches_a_step_inside_the_container_and_85_comes_back(tmp_path):
    assert _run(tmp_path, HANDLER_STEP) == 85


def test_a_step_without_a_handler_ignores_the_signal_and_finishes(tmp_path):
    assert _run(tmp_path, PLAIN_STEP) == 0




# ---------------------------------------------------------------- D-74, D-75: the checkpoint folder in the script

def _ckpt_env(tmp_path, **extra):
    """An environment in which the script's checkpoint parts run against the real payload and fake scontrol."""
    work, scr = tmp_path / "work", tmp_path / "scr"
    (work / "ckpt").mkdir(parents=True, exist_ok=True)
    py = work / ".kaos-ckpt.py"
    py.write_text(CKPT)
    # KAOS_JOB_ID is NOT in the environment: the script derives it from SLURM_JOB_ID, as on the node.
    env = dict(os.environ, PATH=str(_bin(tmp_path)) + os.pathsep + os.environ["PATH"],
               SCONTROL_LOG=str(tmp_path / "scontrol.log"), SLURM_JOB_ID="4242", KAOS_LEASES="kaos-commit=c0ffee12",
               SITE_JOB="4242", SITE_FILE=str(tmp_path / "site.comment"),
               KAOS_WORK=str(work), KAOS_CKPT_ROOT=str(scr / "kaos-ckpt"), KAOS_SCR=str(scr), WF_UID="wf1",
               STEP_NAME="train", KAOS_ATTEMPT="1", CKPTPY=str(py), HAVE_CKPT="1", STEP_OUTPUTS="", USER="u1")
    env.update(extra)
    return env


def _bash(tmp_path, lines, env, rc=None):
    script = "\n".join([FAIL] + (["rc=%d" % rc] if rc is not None else []) + lines)
    return subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True)


def _copier(tmp_path, env, mode):
    """The copier as the script starts it: a child python3 of the script's shell (exported job id, exported
    kaos_comment), in the script's own working directory."""
    return subprocess.run(["bash", "-c", FAIL + '\npython3 "$CKPTPY" ' + mode], cwd=tmp_path, env=env,
                          capture_output=True, text=True)


def _comments(tmp_path):
    path = tmp_path / "scontrol.log"
    return path.read_text().splitlines() if path.exists() else []


def test_85_copies_the_folder_once_more_writes_the_comment_and_ends_with_85(tmp_path):
    env = _ckpt_env(tmp_path)
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    r = _bash(tmp_path, _block('if [ $rc -eq 85 ]; then', 'exit 0'), env, rc=85)
    assert r.returncode == 85, r.stdout + r.stderr
    assert not (tmp_path / "work").exists(), "the job's scratch is removed once the copy is written"
    assert (tmp_path / "scr" / "kaos-ckpt" / "wf1" / "train" / "000001.tar").exists(), "the copy outlives the job"
    comment = _comments(tmp_path)[-1]
    assert "kaos-commit=c0ffee12" in comment and "kaos-ckpt=" in comment and "kaos-ckpt-path=kaos-ckpt/wf1/train/000001.tar" in comment


def test_85_whose_final_copy_fails_ends_235_and_the_failure_comment_keeps_the_last_good_copy(tmp_path):
    env = _ckpt_env(tmp_path)
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    assert _copier(tmp_path, env, "final").returncode == 0     # a periodic copy exists
    Path(env["CKPTPY"]).write_text("import sys; sys.exit(1)")                                        # the final copy cannot be made
    r = _bash(tmp_path, _block('if [ $rc -eq 85 ]; then', 'exit 0'), env, rc=85)
    assert r.returncode == 235
    comment = _comments(tmp_path)[-1]
    assert "err235:" in comment and "kaos-ckpt=" in comment, "the failure comment still names the last good copy"


def test_85_without_the_copier_fails_235(tmp_path):
    env = _ckpt_env(tmp_path, HAVE_CKPT="0")
    assert _bash(tmp_path, _block('if [ $rc -eq 85 ]; then', 'exit 0'), env, rc=85).returncode == 235


def test_a_step_that_succeeds_deletes_its_checkpoint_copies_and_its_scratch(tmp_path):
    env = _ckpt_env(tmp_path)
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    assert _copier(tmp_path, env, "final").returncode == 0
    store = tmp_path / "scr" / "kaos-ckpt" / "wf1" / "train"
    assert store.is_dir()
    env["HANDBACK_B64"] = payloads.b64("pass\n")     # the result hand-back is tested in test_step_contract.py
    r = _bash(tmp_path, _block('if [ $rc -eq 0 ]; then', "exit 0"), env, rc=0)
    assert r.returncode == 0 and not store.exists() and not (tmp_path / "work").exists()


def test_a_step_that_fails_keeps_its_copies_for_the_seven_days(tmp_path):
    env = _ckpt_env(tmp_path)
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    assert _copier(tmp_path, env, "final").returncode == 0
    r = _bash(tmp_path, _block('[ $rc -eq 0 ] && rm -rf "$KAOS_WORK" "$KAOS_CKPT_ROOT/$WF_UID/$STEP_NAME"', "exit 0"), env, rc=3)
    assert r.returncode == 3
    assert (tmp_path / "scr" / "kaos-ckpt" / "wf1" / "train" / "000001.tar").exists()
    assert "kaos-ckpt=" in _comments(tmp_path)[-1], "the failure comment carries the copy"


def test_a_continued_job_restores_the_copy_before_the_step_and_never_starts_from_scratch(tmp_path):
    env = _ckpt_env(tmp_path)
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    assert _copier(tmp_path, env, "final").returncode == 0
    sha = json.loads(next((tmp_path / "scr" / "kaos-ckpt" / "wf1" / "train").glob("*.json")).read_text())["tar_sha256"]
    (tmp_path / "work" / "ckpt" / "last.pt").unlink()
    block = _block('if [ -n "$KAOS_RESUME_REF" ]; then', "fi")
    r = _bash(tmp_path, block + ['echo "resume=$APPTAINERENV_KAOS_RESUME_PATH"'], dict(env, KAOS_RESUME_REF=sha))
    assert r.returncode == 0 and "resume=/work/ckpt" in r.stdout
    assert (tmp_path / "work" / "ckpt" / "last.pt").read_bytes() == b"weights"
    r = _bash(tmp_path, block, dict(env, KAOS_RESUME_REF="0" * 64))
    bad = (tmp_path / "scr" / "kaos-ckpt" / "wf1" / "train" / "000001.tar")
    bad.write_bytes(b"torn")
    r = _bash(tmp_path, block, dict(env, KAOS_RESUME_REF=sha))
    assert r.returncode == 234 and "err234:" in _comments(tmp_path)[-1]


def test_a_first_job_has_no_resume_path(tmp_path):
    r = _bash(tmp_path, _block('if [ -n "$KAOS_RESUME_REF" ]; then', "fi") + ['echo "resume=[$APPTAINERENV_KAOS_RESUME_PATH]"'],
              _ckpt_env(tmp_path))
    assert r.returncode == 0 and "resume=[]" in r.stdout


def test_the_script_wires_the_folder_the_watcher_and_the_wall_time_copy():
    lines = _job_script_lines()
    text = "\n".join(lines)
    assert "export APPTAINERENV_KAOS_CHECKPOINT_DIR=/work/ckpt" in text
    assert lines.index("STEP_PID=$!") < lines.index("CKPT_PID=\"\"") < next(
        i for i, l in enumerate(lines) if l.startswith("if [ $HAVE_CKPT = 1 ]; then python3 $CKPTPY watch"))
    assert "kill -USR2 $CKPT_PID" in next(l for l in lines if l.startswith("kaos_stop(){"))
    expire = next(i for i, l in enumerate(lines) if l == "[ $HAVE_CKPT = 1 ] && python3 $CKPTPY expire")
    restore = next(i for i, l in enumerate(lines) if "python3 $CKPTPY restore" in l)
    assert expire < restore < lines.index("STEP_PID=$!")
    assert lines.index('trap - USR1') < next(i for i, l in enumerate(lines) if l.startswith('[ -n "$CKPT_PID" ] && { kill'))
    assert "CKPTOUT" not in text and "CKPTIN" not in text


# ---------------------------------------------------------------- the job comment: one writer, nothing lost (live job 5433318)

SUBMIT_COMMENT = "kaos-commit=c0ffee12 kaos-image=e882f17eaa4ee41b kaos-ckpt-lease=wf1/train"


def _site_comment(tmp_path):
    f = tmp_path / "site.comment"
    return f.read_text() if f.exists() else None


def _seed_site(tmp_path, text=SUBMIT_COMMENT):
    (tmp_path / "site.comment").write_text(text)


def test_the_copier_sees_the_job_id_the_script_derived_from_slurm(tmp_path):
    # Live job 5433318: KAOS_JOB_ID was a plain shell variable, so the child python3 saw an empty id and
    # every `scontrol update JobId= ...` failed silently. The environment of the test has no KAOS_JOB_ID.
    env = _ckpt_env(tmp_path)
    assert "KAOS_JOB_ID" not in env
    r = subprocess.run(["bash", "-c", FAIL + '\npython3 -c "import os; print(os.environ.get(\'KAOS_JOB_ID\', \'MISSING\'))"'],
                       env=env, capture_output=True, text=True)
    assert r.stdout.strip() == "4242"


def test_85_names_the_copy_in_the_job_comment_and_keeps_every_other_word(tmp_path):
    env = _ckpt_env(tmp_path)
    _seed_site(tmp_path)
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    r = _bash(tmp_path, _block('if [ $rc -eq 85 ]; then', 'exit 0'), env, rc=85)
    assert r.returncode == 85, r.stdout + r.stderr
    words = _site_comment(tmp_path).split()
    assert {"kaos-commit=c0ffee12", "kaos-image=e882f17eaa4ee41b", "kaos-ckpt-lease=wf1/train"} <= set(words), \
        "the submit-time leases survive the copier's write"
    assert any(w.startswith("kaos-ckpt=") for w in words) and "kaos-ckpt-path=kaos-ckpt/wf1/train/000001.tar" in words
    assert sum(w.startswith("kaos-ckpt-lease=") for w in words) == 1, "a word is replaced, never duplicated"


def test_a_second_copy_replaces_the_copy_words_instead_of_adding_to_them(tmp_path):
    env = _ckpt_env(tmp_path)
    _seed_site(tmp_path)
    work = tmp_path / "work" / "ckpt"
    work.joinpath("last.pt").write_bytes(b"weights-1")
    assert _copier(tmp_path, env, "final").returncode == 0
    work.joinpath("last.pt").write_bytes(b"weights-2-longer")
    assert _copier(tmp_path, env, "final").returncode == 0
    words = _site_comment(tmp_path).split()
    assert "kaos-ckpt-path=kaos-ckpt/wf1/train/000002.tar" in words
    assert sum(w.startswith(("kaos-ckpt=", "kaos-ckpt-path=", "kaos-ckpt-info=")) for w in words) == 3
    assert "kaos-image=e882f17eaa4ee41b" in words


def test_85_whose_comment_update_is_ignored_by_the_site_ends_235_not_a_silent_85(tmp_path):
    # An update the site accepts but does not keep (the job is already over, or a policy drops it): the
    # words are read back, the copy is not reported as recorded, the job fails loudly.
    env = _ckpt_env(tmp_path, KAOS_COMMENT_RETRY_SLEEP="0")
    _seed_site(tmp_path)
    (tmp_path / "site.comment.ro").write_text("")
    (tmp_path / "work" / "ckpt" / "last.pt").write_bytes(b"weights")
    r = _bash(tmp_path, _block('if [ $rc -eq 85 ]; then', 'exit 0'), env, rc=85)
    assert r.returncode == 235, r.stdout + r.stderr
    assert "the job comment could not be set" in r.stdout
    assert "kaos-ckpt=" not in _site_comment(tmp_path)


def test_the_failure_comment_keeps_the_leases_and_replaces_an_older_error_tail(tmp_path):
    env = _ckpt_env(tmp_path)
    _seed_site(tmp_path, SUBMIT_COMMENT + " kaos-ckpt=" + "a" * 64 + " err1:old tail|")
    (tmp_path / "slurm-4242.out").write_text("boom\n")
    r = subprocess.run(["bash", "-c", FAIL + "\nfail 9"], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert r.returncode == 9
    comment = _site_comment(tmp_path)
    assert comment.startswith(SUBMIT_COMMENT + " kaos-ckpt=" + "a" * 64) and comment.endswith("err9:boom|")
    assert "old tail" not in comment, "one error tail, the newest"


def test_a_comment_that_cannot_be_read_still_keeps_the_dataset_lease(tmp_path):
    env = _ckpt_env(tmp_path)           # no site comment file: squeue prints nothing
    (tmp_path / "slurm-4242.out").write_text("boom\n")
    subprocess.run(["bash", "-c", FAIL + "\nfail 9"], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert _site_comment(tmp_path) == "kaos-commit=c0ffee12 err9:boom|"
