"""PRD kaos#1911: several platform projects share one site project (one account, one scratch, one Unix group), each as
its own site user. Everything a project owns is under its own root <scratch>/kaos/<project> (the runner's SCR); the
folder above it is shared, mode 3770.

A fake site: one scratch folder on the local disk, the generated job script run in bash against fake binaries. A second
Unix user cannot be made here (no chown, no second uid): a second project is another root run by the same user, and what
a folder of another user does (EPERM on chmod, a parent made with the wrong mode) is simulated by a fake `chmod` and by
the folder's mode. What only a live site proves: the group of <scratch>/kaos, the sticky bit between two real users, and
`lfs quota` per user and per project (see docs/job-environment.md, the live checklist).
"""
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from kaos_hpc_runner import jobscript

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from test_slot_manager import Node, SCOPE, _job as _slot_job  # noqa: E402


def _lines(scratch):
    return jobscript.batch_lines("-", str(scratch))


def _chunk(lines):
    """From the project's root setup to the SIF path: what a job does to the scratch before it starts the step."""
    a = lines.index('kaos_root "$SCR" || fail 233')
    b = next(i for i, ln in enumerate(lines) if ln.startswith("SIF=$SCR/sif-cache"))
    return lines[a:b + 1]


def _site(tmp_path):
    scratch = tmp_path / "site" / "p1"          # the site project's scratch: /project/scratch/<account>
    scratch.mkdir(parents=True)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text("#!/bin/sh\nexit 0\n")
    (bindir / "apptainer").chmod(0o755)
    (bindir / "scontrol").write_text('#!/bin/sh\n[ "$1" = write ] && echo "#!/bin/bash" > "$4"\nexit 0\n')
    (bindir / "scontrol").chmod(0o755)
    return scratch, bindir


def _run_job(tmp_path, project, scratch, bindir, *, extra_lines=(), env=None, root=None):
    root = root or scratch / "kaos" / project
    lines = _lines(root)
    script = "\n".join(['fail(){ echo "FAIL $1"; exit "$1"; }', *_root_helper(lines), "SCR=" + str(root), *_chunk(lines),
                        *extra_lines, 'echo DONE'])
    e = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], USER=os.environ.get("USER") or "u",
             SLURM_JOB_ID="7", KAOS_JOB_ID="7", IMAGE_REF="reg.example/proj/app:v1", KAOS_ROOT_RETRY_SLEEP="0.1")
    e.update(env or {})
    e.pop("KAOS_SLOTS_B64", None)
    e.pop("KAOS_IN_STEP", None)
    cmd = ["bash", "-c", "umask 077\n" + script]
    return root, subprocess.run(cmd, env=e, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True,
                                cwd=str(tmp_path), timeout=60)


def _root_helper(lines):
    a = next(i for i, ln in enumerate(lines) if ln.startswith("kaos_root(){"))
    b = next(i for i in range(a, len(lines)) if lines[i] == "}")
    return lines[a:b + 1]


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def _perm(path):
    """The permission bits of a project's own folder: the folder under the setgid shared folder keeps its group, which is
    wanted, so the setgid bit it inherits is not part of 'private'."""
    return _mode(path) & 0o777


def _chmod_log(bindir, log):
    """A `chmod` on the PATH that records every call, then does it."""
    (bindir / "chmod").write_text('#!/bin/sh\necho "$@" >> "%s"\nexec /usr/bin/chmod "$@"\n' % log)
    (bindir / "chmod").chmod(0o755)


def _prepare(scratch, project, mode=0o700):
    """What the project's connection does as the project's user (the stager's `prepare` verb): the shared folder and the root."""
    (scratch / "kaos").mkdir(exist_ok=True)
    (scratch / "kaos").chmod(0o3770)
    (scratch / "kaos" / project).mkdir(exist_ok=True)
    (scratch / "kaos" / project).chmod(mode)
    return scratch / "kaos" / project


def test_a_job_before_the_connection_prepared_its_folder_fails_233_and_creates_nothing(tmp_path):
    scratch, bindir = _site(tmp_path)
    before = sorted(p.name for p in scratch.iterdir())
    log = tmp_path / "chmod.log"
    _chmod_log(bindir, log)
    root, r = _run_job(tmp_path, "alpha", scratch, bindir)
    assert r.returncode == 233 and "FAIL 233" in r.stdout, r.stdout
    assert "has not prepared its folder %s (it does not exist); nothing was changed" % root in r.stdout, r.stdout
    assert sorted(p.name for p in scratch.iterdir()) == before and not (scratch / "kaos").exists(), "no shared folder, no root"
    assert not log.exists() or log.read_text() == ""
    assert r.stdout.count("The project's connection") == 1


def test_a_job_on_a_prepared_root_runs_and_makes_only_its_subfolders(tmp_path):
    scratch, bindir = _site(tmp_path)
    root = _prepare(scratch, "alpha")
    root, r = _run_job(tmp_path, "alpha", scratch, bindir)
    assert r.returncode == 0 and "DONE" in r.stdout, r.stdout
    assert sorted(p.name for p in root.iterdir()) == ["data-cache", "kaos-apptainer-cache", "kaos-tmp", "kaos-work", "sif-cache"]
    for n in ("kaos-work", "kaos-tmp", "kaos-priv", "kaos-ckpt", "kaos-claims", "sif-cache", "data-cache", "kaos-apptainer-cache"):
        assert not (scratch / n).exists(), n + " must not be directly under the shared scratch"
    assert _perm(root) == 0o700 and all(_perm(root / p.name) == 0o700 for p in root.iterdir())


def test_the_job_of_one_project_before_another_is_connected_cannot_affect_it(tmp_path):
    scratch, bindir = _site(tmp_path)
    _prepare(scratch, "beta")                        # B is connected, A is not yet
    root_b, b = _run_job(tmp_path, "beta", scratch, bindir)
    assert b.returncode == 0, b.stdout
    root_a, a = _run_job(tmp_path, "alpha", scratch, bindir)
    assert a.returncode == 233 and not root_a.exists(), "A's folder is not made by anybody's job"
    root_a = _prepare(scratch, "alpha")              # A is connected afterwards
    root_a, a = _run_job(tmp_path, "alpha", scratch, bindir)
    assert a.returncode == 0, a.stdout
    assert sorted(p.name for p in (scratch / "kaos").iterdir()) == ["alpha", "beta"] and _mode(scratch / "kaos") == 0o3770


@pytest.mark.parametrize("mode", [0o755, 0o750, 0o770, 0o707])
def test_a_root_that_is_not_private_is_refused_and_never_chmoded_by_the_job(tmp_path, mode):
    scratch, bindir = _site(tmp_path)
    root = _prepare(scratch, "alpha", mode)
    log = tmp_path / "chmod.log"
    _chmod_log(bindir, log)
    root, r = _run_job(tmp_path, "alpha", scratch, bindir)
    assert r.returncode == 233 and "it has mode %o and must be private (0700)" % mode in r.stdout, r.stdout
    assert _mode(root) == mode and (not log.exists() or log.read_text() == "")


def test_the_bare_scratch_of_an_older_operator_changes_nothing_and_names_no_parent(tmp_path):
    scratch, bindir = _site(tmp_path)
    log = tmp_path / "chmod.log"
    _chmod_log(bindir, log)
    before = (os.stat(scratch).st_mode, os.stat(scratch.parent).st_mode, sorted(p.name for p in scratch.parent.iterdir()))
    for bare in (scratch, scratch / "kaos", Path("/scratch"), Path("relative")):
        root, r = _run_job(tmp_path, "x", scratch, bindir, root=bare)
        assert r.returncode == 233, (bare, r.stdout)
        assert "has not published a per-project folder for this job yet (operator older than this runner); nothing was changed" in r.stdout
        assert str(scratch.parent) not in r.stdout.replace("FAIL", "") and "chgrp" not in r.stdout and "chmod" not in r.stdout
    assert not log.exists() or log.read_text() == "", "no chmod at all"
    assert before == (os.stat(scratch).st_mode, os.stat(scratch.parent).st_mode, sorted(p.name for p in scratch.parent.iterdir()))
    assert not (scratch / "kaos").exists()


def test_a_shared_folder_or_a_project_root_that_is_a_symbolic_link_is_refused_and_nothing_is_chmoded(tmp_path):
    scratch, bindir = _site(tmp_path)
    log = tmp_path / "chmod.log"
    _chmod_log(bindir, log)
    target = tmp_path / "elsewhere"
    (target / "beta").mkdir(parents=True)
    target.chmod(0o755)
    os.symlink(target, scratch / "kaos")
    root, r = _run_job(tmp_path, "beta", scratch, bindir)
    assert r.returncode == 233 and "the shared folder %s is a symbolic link" % (scratch / "kaos") in r.stdout, r.stdout
    os.unlink(scratch / "kaos")
    (scratch / "kaos").mkdir()
    (scratch / "kaos").chmod(0o3770)
    os.symlink(target, scratch / "kaos" / "beta")
    root, r = _run_job(tmp_path, "beta", scratch, bindir)
    assert r.returncode == 233 and "it is a symbolic link" in r.stdout, r.stdout
    assert _mode(target) == 0o755 and (not log.exists() or log.read_text() == "")


def test_a_cache_folder_that_cannot_be_made_private_fails_loudly_naming_it_not_silently(tmp_path):
    scratch, bindir = _site(tmp_path)
    root = _prepare(scratch, "beta")
    victim = root / "sif-cache"
    (bindir / "chmod").write_text('#!/bin/sh\ncase "$*" in *"%s") exit 1;; esac\nexec /usr/bin/chmod "$@"\n' % victim)
    (bindir / "chmod").chmod(0o755)
    root, r = _run_job(tmp_path, "beta", scratch, bindir)
    assert r.returncode == 233 and "The folder %s must be private to the project user" % victim in r.stdout, r.stdout


def test_nothing_under_the_project_root_is_created_before_the_root_is_checked():
    lines = _lines("/project/scratch/p1/kaos/alpha")
    first = lines.index('kaos_root "$SCR" || fail 233')
    for i, ln in enumerate(lines[:first]):
        assert not ("$SCR" in ln and any(w in ln for w in ("mkdir", "chmod", "touch", "> "))), (i, ln)
    after = "\n".join(lines[first:])
    assert 'mkdir -p "$SCR/kaos-priv"' in after
    assert not [ln for ln in lines if "chmod" in ln and "2>/dev/null" in ln and "kaos-" in ln], \
        "a chmod whose failure matters does not hide its error"


def test_the_runner_hands_the_job_the_project_root_as_its_scratch(tmp_path):
    from kaos_hpc_runner import jobscript as js
    assert js.batch_lines("-", "/project/scratch/p1/kaos/alpha")[
        next(i for i, ln in enumerate(js.batch_lines("-", "x")) if ln.startswith("SCR="))] == "SCR=/project/scratch/p1/kaos/alpha"


def test_two_hosts_of_two_projects_keep_their_claims_in_their_own_roots(tmp_path):
    shared = tmp_path / "scratch" / "kaos"
    shared.mkdir(parents=True)
    shared.chmod(0o3770)
    runs = []
    for project in ("alpha", "beta"):
        sub = tmp_path / project
        sub.mkdir()
        root = shared / project
        root.mkdir()
        guests = {"101": _slot_job("kaos-%s-wf1-train" % SCOPE, "echo $KAOS_JOB_ID > $OUT/101\nexit 0\n", comment="kaos-commit=bbbbbbb2")}
        node = Node(sub, guests, extra_env={"KAOS_SCR": str(root)})
        rc, log = node.finish()
        assert rc == 0, log
        runs.append(root)
        assert (node.out / "101").read_text().strip() == "101", log
    for root in runs:
        assert (root / "kaos-claims").is_dir() and _perm(root / "kaos-claims") == 0o700
        assert not list((root / "kaos-claims").glob("101-*")), "each host removes its own locks"
    assert sorted(p.name for p in shared.iterdir()) == ["alpha", "beta"]
    assert not (tmp_path / "scratch" / "kaos-claims").exists()
