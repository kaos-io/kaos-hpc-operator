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


def _run_job(tmp_path, project, scratch, bindir, *, extra_lines=(), env=None):
    root = scratch / "kaos" / project
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


def test_the_first_job_makes_the_shared_folder_and_its_own_root_and_a_second_project_makes_its_own(tmp_path):
    scratch, bindir = _site(tmp_path)
    root_a, a = _run_job(tmp_path, "alpha", scratch, bindir)
    assert a.returncode == 0 and "DONE" in a.stdout, a.stdout
    shared = scratch / "kaos"
    assert _mode(shared) == 0o3770, oct(_mode(shared))
    assert os.stat(shared).st_gid == os.stat(scratch).st_gid, "the group of the site project's scratch"
    assert _perm(root_a) == 0o700
    root_b, b = _run_job(tmp_path, "beta", scratch, bindir)
    assert b.returncode == 0 and "DONE" in b.stdout, b.stdout
    assert _perm(root_b) == 0o700 and _mode(shared) == 0o3770, "the second project changes nothing of the shared folder"
    # Nothing but the shared folder sits directly under the site project's scratch.
    assert sorted(p.name for p in scratch.iterdir()) == ["kaos"]
    assert sorted(p.name for p in shared.iterdir()) == ["alpha", "beta"]


def test_each_project_gets_its_own_work_cache_and_checkpoint_trees_and_none_is_shared(tmp_path):
    scratch, bindir = _site(tmp_path)
    for project in ("alpha", "beta"):
        root, r = _run_job(tmp_path, project, scratch, bindir)
        assert r.returncode == 0, r.stdout
        names = sorted(p.name for p in root.iterdir())
        assert names == ["data-cache", "kaos-apptainer-cache", "kaos-tmp", "kaos-work", "sif-cache"], names
        for n in names:
            assert _perm(root / n) == 0o700, n
    for n in ("kaos-work", "kaos-tmp", "kaos-priv", "kaos-ckpt", "kaos-claims", "sif-cache", "data-cache", "kaos-apptainer-cache"):
        assert not (scratch / n).exists(), n + " must not be directly under the shared scratch"


def test_a_host_job_keeps_its_private_script_folder_in_its_own_root_for_each_project(tmp_path):
    scratch, bindir = _site(tmp_path)
    (tmp_path / "slots.py").write_text("print('slot manager ran')\n")
    host = ['KAOS_SLOTS_B64=$(printf %s "print(1)" | base64 -w0)']
    for project in ("alpha", "beta"):
        root = scratch / "kaos" / project
        lines = _lines(root)
        head = lines[:lines.index('kaos_root "$SCR" || fail 233')]
        # the host branch of the real script, from the root setup to its `fi`
        a = lines.index('kaos_root "$SCR" || fail 233')
        b = lines.index("fi", a)
        script = "\n".join(['fail(){ echo "FAIL $1"; exit "$1"; }', *_root_helper(lines), "SCR=" + str(root),
                            "KAOS_PRIV=", "trap 'echo PRIV=$KAOS_PRIV' EXIT", *host, *lines[a:b + 1], "echo UNREACHED"])
        e = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"], SLURM_JOB_ID="9", KAOS_ROOT_RETRY_SLEEP="0.1",
                 USER=os.environ.get("USER") or "u")
        r = subprocess.run(["bash", "-c", "umask 077\n" + script], env=e, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True, cwd=str(tmp_path), timeout=60)
        assert "UNREACHED" not in r.stdout and "PRIV=%s/kaos-priv/host-9" % root in r.stdout, r.stdout
        assert head, "sanity"
        assert _perm(root / "kaos-priv") == 0o700
    assert not (scratch / "kaos-priv").exists()


def test_a_shared_folder_with_the_wrong_mode_stops_the_job_with_one_plain_sentence_and_changes_nothing(tmp_path):
    scratch, bindir = _site(tmp_path)
    shared = scratch / "kaos"
    shared.mkdir()
    shared.chmod(0o755)          # made by the first user's umask: the second user cannot write in it
    root, r = _run_job(tmp_path, "beta", scratch, bindir)
    assert r.returncode == 233, r.stdout
    assert _mode(shared) == 0o755, "never a silent chmod of a folder the site owner or another user made"
    assert not root.exists()
    sentence = next(ln for ln in r.stdout.splitlines() if ln.startswith("The shared folder"))
    group = subprocess.run(["stat", "-c", "%G", str(scratch)], stdout=subprocess.PIPE, universal_newlines=True).stdout.strip()
    assert "%s has mode 755" % shared in sentence, sentence
    assert "chgrp %s %s && chmod 3770 %s" % (group, shared, shared) in sentence, sentence
    assert sentence.count(". ") == 1 and "FAIL 233" in r.stdout


@pytest.mark.parametrize("mode", [0o2770, 0o1770, 0o3750, 0o3707])
def test_a_shared_folder_missing_sticky_setgid_or_a_group_bit_is_refused(tmp_path, mode):
    scratch, bindir = _site(tmp_path)
    (scratch / "kaos").mkdir()
    (scratch / "kaos").chmod(mode)
    root, r = _run_job(tmp_path, "beta", scratch, bindir)
    assert r.returncode == 233 and "has mode %o" % mode in r.stdout, r.stdout
    assert _mode(scratch / "kaos") == mode


def test_a_folder_another_user_is_still_making_is_given_a_moment(tmp_path):
    scratch, bindir = _site(tmp_path)
    shared = scratch / "kaos"
    shared.mkdir()
    shared.chmod(0o700)          # created, not yet chmod-ed by its creator
    fixer = subprocess.Popen(["sh", "-c", "sleep 0.15; chmod 3770 '%s'" % shared])
    root, r = _run_job(tmp_path, "beta", scratch, bindir, env={"KAOS_ROOT_RETRY_SLEEP": "0.2"})
    fixer.wait()
    assert r.returncode == 0 and "DONE" in r.stdout, r.stdout
    assert root.is_dir()


def test_a_project_root_that_cannot_be_made_private_names_the_folder(tmp_path):
    scratch, bindir = _site(tmp_path)
    root = scratch / "kaos" / "beta"
    root.mkdir(parents=True)
    (scratch / "kaos").chmod(0o3770)
    # what EPERM on a folder of another user looks like
    (bindir / "chmod").write_text('#!/bin/sh\ncase "$*" in *"%s") echo "chmod: changing permissions: Operation not permitted" >&2; exit 1;; esac\n'
                                  'exec /usr/bin/chmod "$@"\n' % root)
    (bindir / "chmod").chmod(0o755)
    root, r = _run_job(tmp_path, "beta", scratch, bindir)
    assert r.returncode == 233, r.stdout
    assert "The project folder %s must be private to the project user and cannot be made so" % root in r.stdout, r.stdout


def test_a_cache_folder_that_cannot_be_made_private_fails_loudly_naming_it_not_silently(tmp_path):
    scratch, bindir = _site(tmp_path)
    (scratch / "kaos").mkdir()
    (scratch / "kaos").chmod(0o3770)
    victim = scratch / "kaos" / "beta" / "sif-cache"
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
