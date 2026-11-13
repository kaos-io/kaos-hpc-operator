#!/usr/bin/env python3
"""PRD kaos#1911: two platform projects, two DIFFERENT Unix users (distinct uids, one shared group), ONE shared scratch
root of the site project. Run as root in a throw-away container (hack/local-lab/two-users.sh) or in any container where
the caller is root: it makes the users and the site's folders, then runs each project's job side as its own uid
(`setpriv`), and asserts who can read, write and remove what.

What runs for each project, as that project's user: the real generated job script from the root setup to the SIF path and
the work folder and checkpoint root; the host's private script folder; the real slot manager (claims) against the fake
Slurm of the runner tests; the real checkpoint copier; and, when STAGER names the staging tool (kubecore-operator
images/meluxina-tokenmint/stager/stager.py), image staging, `usage`, and `clean` (the known-jobs selection).

Not covered here: Lustre quota per user and per project (a fake `lfs` answers), the site's real group and ACL setup,
lakeFS download of datasets (the staged-dataset layout is the same root; covered by the stager's own tests), and the
result hand-back to the lake. A live site checklist is in docs/job-environment.md.
"""
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve()
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "runner"))
sys.path.insert(0, str(REPO / "runner" / "tests"))

GID_SITE = 2000
USERS = {"alpha": 2001, "beta": 2002}
ACCOUNT = "p201342"
SCRATCH = "/project/scratch/" + ACCOUNT
JOBS = {"alpha": "7001", "beta": "7002"}
KEY = {"alpha": "a" * 16, "beta": "b" * 16}
STAGER = os.environ.get("STAGER", "")
WF = "0f0e0d0c-aaaa-bbbb-cccc-0123456789ab"


def say(msg):
    print(msg, flush=True)


def check(cond, msg):
    msg = msg if len(msg) < 240 else msg[:237] + "..."
    if not cond:
        say("FAIL: " + msg)
        sys.exit(1)
    say("ok:   " + msg)


def run_as(project, args, **kw):
    uid = USERS[project]
    cmd = ["setpriv", "--reuid=%d" % uid, "--regid=%d" % uid, "--groups=%d,%d" % (uid, GID_SITE)] + args
    env = dict(os.environ, HOME="/home/" + project, USER=project, LOGNAME=project)
    env.update(kw.pop("env", {}))
    return subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, **kw)


# ---------------------------------------------------------------------------------------------------------- job side

def fake_bin(d):
    d.mkdir(parents=True, exist_ok=True)
    files = {
        "apptainer": "#!/bin/sh\nexit 0\n",
        "scontrol": '#!/bin/sh\n[ "$1" = write ] && echo "#!/bin/bash" > "$4"\nfor a in "$@"; do case "$a" in Comment=*) printf %s "${a#Comment=}" > "$COMMENT_FILE";; esac; done\nexit 0\n',
        "squeue": "#!/bin/sh\ncase \"$*\" in *' -j '*) cat \"$COMMENT_FILE\" 2>/dev/null;; esac\nexit 0\n",
        "lfs": "#!/bin/sh\necho '%s 100 0 0 - 5 0 0 -'\n" % SCRATCH,
    }
    for name, body in files.items():
        (d / name).write_text(body)
        (d / name).chmod(0o755)


def job_side(project):
    os.umask(0o077)   # as the real job script: everything a job writes is private
    from kaos_hpc_runner import jobscript, payloads
    from test_slot_manager import Node, SCOPE, _job
    root = "%s/kaos/%s" % (SCRATCH, project)
    work = Path(tempfile.mkdtemp(prefix="job-", dir="/home/" + project))
    bindir = work / "bin"
    fake_bin(bindir)
    jid = JOBS[project]
    env = dict(os.environ, PATH="%s:%s" % (bindir, os.environ["PATH"]), SLURM_JOB_ID=jid, KAOS_JOB_ID=jid, USER=project,
               IMAGE_REF="reg.example/%s/app:v1" % project, COMMENT_FILE=str(work / "comment"), KAOS_ROOT_RETRY_SLEEP="0.2")
    env.pop("KAOS_SLOTS_B64", None)
    lines = jobscript.batch_lines("-", root)
    a = next(i for i, l in enumerate(lines) if l.startswith("kaos_root(){"))
    helper = lines[a:lines.index("}", a) + 1]
    start = lines.index('kaos_root "$SCR" || fail 233')
    chunk = lines[start:next(i for i, l in enumerate(lines) if l.startswith("SIF=$SCR/sif-cache")) + 1]
    host = lines[start:lines.index("fi", start) + 1]
    extra = [l for l in lines if l.startswith(("export KAOS_WORK=", "export KAOS_CKPT_ROOT="))]
    script = "\n".join(["umask 077", 'fail(){ echo "FAIL $1"; exit "$1"; }', *helper, "SCR=" + root,
                        *chunk, *extra, 'echo "work=$KAOS_WORK"'])
    r = subprocess.run(["bash", "-c", script], env=env, cwd=str(work), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True)
    check(r.returncode == 0, "%s: job script prepares its root, caches, work folder and checkpoint root: %s" % (project, r.stdout.strip()[-200:]))
    kwork = r.stdout.split("work=")[1].split()[0]
    (Path(kwork) / "output").joinpath("result.txt").write_text("result of " + project)
    # the host's private script folder (a shared-node host)
    hs = "\n".join(["umask 077", 'fail(){ echo "FAIL $1"; exit "$1"; }', *helper, "SCR=" + root, "KAOS_PRIV=",
                    "trap 'echo priv=$KAOS_PRIV' EXIT", 'KAOS_SLOTS_B64=$(printf %s "print(1)" | base64 -w0)', *host])
    r = subprocess.run(["bash", "-c", hs], env=dict(env, SLURM_JOB_ID=jid), cwd=str(work), stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, universal_newlines=True)
    check("priv=%s/kaos-priv/host-%s" % (root, jid) in r.stdout, "%s: the host's private script folder is under its own root" % project)
    # the real slot manager: claims
    guests = {"101": _job("kaos-%s-wf1-train" % SCOPE, "echo $KAOS_JOB_ID > $OUT/101\nexit 0\n", comment="kaos-commit=bbbbbbb2")}
    sub = work / "slots"
    sub.mkdir()
    # the fake Slurm is chmod-ed by the harness: give this user its own copy of it
    import test_slot_manager as tsm
    (work / "r" / "tests").mkdir(parents=True)
    shutil.copy(str(REPO / "runner" / "tests" / "fakeslurm.py"), str(work / "r" / "tests" / "fakeslurm.py"))
    tsm.ROOT = work / "r"
    node = Node(sub, guests, extra_env={"KAOS_SCR": root})
    rc, log = node.finish()
    check(rc == 0 and (node.out / "101").read_text().strip() == "101", "%s: the slot manager ran a guest with its claims in its own root" % project)
    # the real checkpoint copier
    ck = work / "ckpt.py"
    ck.write_text(payloads.CKPT)
    (Path(kwork) / "ckpt").mkdir(exist_ok=True)
    (Path(kwork) / "ckpt" / "last.pt").write_bytes(os.urandom(4096))
    shell = "\n".join(jobscript.COMMENT_HELPER + ["export -f kaos_comment", 'exec "$@"'])
    ce = dict(env, KAOS_WORK=kwork, KAOS_CKPT_ROOT=root + "/kaos-ckpt", KAOS_SCR=root, WF_UID=WF, STEP_NAME="train",
              KAOS_ATTEMPT="1", KAOS_LEASES="kaos-commit=c0ffee12", KAOS_PROJID="201342", KAOS_CKPT_BUDGET_PCT="10")
    r = subprocess.run(["bash", "-c", shell, "run", sys.executable, str(ck), "final"], env=ce, cwd=str(work),
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    check(r.returncode == 0 and Path(root, "kaos-ckpt/%s/train/000001.tar" % WF).is_file(), "%s: a checkpoint copy is written under its own root: %s" % (project, r.stdout.strip()[-200:]))
    # the image cache and data cache (what a job's pull would leave)
    Path(root, "sif-cache", KEY[project] + ".sif").write_bytes(b"sif")
    Path(root, "data-cache", "ds", "c" * 64).mkdir(parents=True)
    Path(root, "data-cache", "ds", "c" * 64, ".kaos-complete").write_text("")
    if STAGER:
        data = os.urandom(70000)
        payload = data + (hashlib.sha256(data).hexdigest() + "\n").encode()
        r = subprocess.run([sys.executable, STAGER, "--root", root, "--account", ACCOUNT, "--scratch", SCRATCH, "image",
                            "--key", KEY[project], "--size", str(len(data))], input=payload, env=env, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        check(r.returncode == 0 and Path(root, "images", KEY[project] + ".tar").read_bytes() == data, "%s: the staging tool stores an image under its own root: %s" % (project, r.stdout.decode()[-200:] + r.stderr.decode()[-200:]))
    shutil.rmtree(work / "slots", ignore_errors=True)


# ------------------------------------------------------------------------------------------------------ orchestrator

def can(project, fn_src, *args):
    """True when `fn_src` (python, exit 0 = the operation worked) works for `project`'s user."""
    r = run_as(project, [sys.executable, "-c", fn_src] + list(args))
    return r.returncode == 0, r.stdout


def walk(top):
    for d, dirs, names in os.walk(top):
        for n in dirs + names:
            yield os.path.join(d, n)


def main():
    if os.geteuid() != 0:
        say("needs root (a container): %s" % sys.argv)
        return 2
    subprocess.check_call(["groupadd", "-g", str(GID_SITE), "site"])
    for name, uid in USERS.items():
        subprocess.check_call(["groupadd", "-g", str(uid), name])
        subprocess.check_call(["useradd", "-m", "-u", str(uid), "-g", str(uid), "-G", "site", name])
    # The site: the project's scratch belongs to its group, group-writable, setgid (as LuxProvide lays out a project).
    os.makedirs(SCRATCH)
    os.chown(SCRATCH, 0, GID_SITE)
    os.chmod(SCRATCH, 0o2770)

    say("== control: in the OLD layout (folders directly under the shared scratch) the second user fails")
    r = run_as("alpha", ["bash", "-c", 'umask 077; mkdir -p %s/kaos-priv && chmod 700 %s/kaos-priv' % (SCRATCH, SCRATCH)])
    check(r.returncode == 0, "old layout: alpha creates kaos-priv")
    r = run_as("beta", ["bash", "-c", 'umask 077; mkdir -p %s/kaos-priv && chmod 700 %s/kaos-priv && mkdir %s/kaos-priv/host-1' % (SCRATCH, SCRATCH, SCRATCH)])
    check(r.returncode != 0, "old layout: beta cannot use alpha's kaos-priv (the defect this change removes)")
    shutil.rmtree(SCRATCH + "/kaos-priv")

    say("== the jobs: alpha's job script first makes the shared folder; beta's staging tool (if given) or job finds it")
    for project in ("alpha", "beta"):
        r = run_as(project, [sys.executable, str(HERE), "job", project], env={"STAGER": STAGER, "PYTHONPATH": ""})
        say(r.stdout.rstrip())
        check(r.returncode == 0, "%s: its whole job side ran as uid %d" % (project, USERS[project]))

    say("== modes")
    shared = SCRATCH + "/kaos"
    st = os.stat(shared)
    check(stat.S_IMODE(st.st_mode) == 0o3770 and st.st_gid == GID_SITE, "the shared folder is 3770, group site (%o, gid %d)" % (stat.S_IMODE(st.st_mode), st.st_gid))
    check(sorted(os.listdir(shared)) == ["alpha", "beta"], "the shared folder holds one root per project and nothing else")
    check(sorted(os.listdir(SCRATCH)) == ["kaos"], "nothing else sits directly under the site scratch")
    # Known limit, said here and in docs/job-environment.md: the owner of a sticky folder may rename or remove what is in it.
    # The project user whose job made the shared folder first owns it, so THAT user could move another project's root away.
    # Closing it is the site owner's one-time setup (the folder owned by the site owner, not by a project user).
    check(os.stat(shared).st_uid == USERS["alpha"], "the shared folder is owned by the user whose job made it first (alpha)")
    ok, _ = can("alpha", "import os,sys; os.rename(sys.argv[1], sys.argv[1]+'-moved'); os.rename(sys.argv[1]+'-moved', sys.argv[1])", shared + "/beta")
    check(ok, "KNOWN LIMIT: while a project user owns the shared folder, that user can rename another project's root (sticky only protects from the other users)")
    os.chown(shared, 0, GID_SITE)
    say("-- the site owner takes the shared folder over once (chown root:site, mode 3770 kept): the rest is checked in that setup")
    for project, other in (("alpha", "beta"), ("beta", "alpha")):
        root = "%s/%s" % (shared, project)
        uid = USERS[project]
        entries = [root] + list(walk(root))
        for p in entries:
            check(os.lstat(p).st_uid == uid, "%s owned by %s" % (p, project)) if os.lstat(p).st_uid != uid else None
        check(stat.S_IMODE(os.stat(root).st_mode) & 0o077 == 0, "the root of %s has no group or other bits (%o): this is what closes the tree" % (project, stat.S_IMODE(os.stat(root).st_mode)))
        loose = ["%s %o" % (p, stat.S_IMODE(os.lstat(p).st_mode)) for p in entries
                 if stat.S_IMODE(os.lstat(p).st_mode) & 0o077 and not (os.path.isdir(p) and stat.S_IMODE(os.lstat(p).st_mode) & 0o7077 == 0o2000)]
        check(not loose, "all %d of %d entries under %s carry no group or other permission bits: %s" % (len(entries) - len(loose), len(entries), root, loose[:3]))
        r = run_as(other, [sys.executable, "-c", "import os,sys\nbad=[p for p in sys.stdin.read().split('\\n') if p and (lambda f: f)(os.path.exists(p))]\nprint(len(bad)); sys.exit(1 if bad else 0)"],
                   input="\n".join(entries[1:]) + "\n")
        check(r.returncode == 0, "%s can reach none of the %d entries under %s (stat fails for every one)" % (other, len(entries) - 1, root))
        ok, out = can(other, "import os,sys; os.listdir(sys.argv[1])", root)
        check(not ok, "%s cannot list %s" % (other, root))
        for rel in ("kaos-work/%s/output/result.txt" % JOBS[project], "kaos-priv/host-%s/slots.py" % JOBS[project],
                    "kaos-ckpt/%s/train/000001.tar" % WF, "sif-cache/%s.sif" % KEY[project]):
            ok, _ = can(other, "open(__import__('sys').argv[1],'rb').read()", "%s/%s" % (root, rel))
            check(not ok, "%s cannot read %s" % (other, rel))
        ok, _ = can(other, "open(__import__('sys').argv[1]+'/intruder','w')", root)
        check(not ok, "%s cannot write into %s" % (other, root))
        ok, _ = can(other, "import os,sys; os.rename(sys.argv[1], sys.argv[1]+'-moved')", root)
        check(not ok, "%s cannot rename %s (sticky shared folder): %s" % (other, root, _.strip().splitlines()[-1][-110:]))
    ok, out = can("beta", "import os; print(sorted(os.listdir('%s')))" % shared)
    check(ok and "alpha" in out, "what IS group-readable: the shared folder lists the project names (needed to create and enter one's own root) and nothing below them")
    os.makedirs(shared + "/empty-alpha", exist_ok=False)
    os.chown(shared + "/empty-alpha", USERS["alpha"], GID_SITE)
    ok, _ = can("beta", "import os; os.rmdir('%s/empty-alpha')" % shared)
    check(not ok, "beta cannot remove even an empty folder alpha owns (sticky)")
    os.rmdir(shared + "/empty-alpha")

    if STAGER:
        say("== clean (known-jobs) of each project, by its own user")
        import time
        old = time.time() - 40 * 86400
        for project in ("alpha", "beta"):
            for p in list(walk("%s/%s/kaos-work" % (shared, project))) + list(walk("%s/%s/kaos-ckpt" % (shared, project))):
                os.utime(p, (old, old), follow_symlinks=False)
            for p in ("kaos-work", "kaos-ckpt"):
                os.utime("%s/%s/%s" % (shared, project, p), (old, old))
        fake = tempfile.mkdtemp()
        os.chmod(fake, 0o755)
        fake_bin(Path(fake))
        # beta cleans with ALPHA's job id in its selection: it only ever looks in its own root.
        sel = json.dumps({"keep_jobs": [], "keep_steps": [], "known_jobs": [JOBS["alpha"], JOBS["beta"]], "known_steps": [WF + "/train"],
                          "only_jobs": [JOBS["alpha"], JOBS["beta"]], "only_steps": [WF + "/train"]}) + "\n"
        for project in ("beta", "alpha"):
            r = run_as(project, [sys.executable, STAGER, "--root", "%s/%s" % (shared, project), "--account", ACCOUNT, "--scratch", SCRATCH,
                                 "clean", "--what", "failed-jobs", "--older-than-days", "14", "--selection-stdin"],
                       input=sel, env={"PATH": fake + ":" + os.environ["PATH"], "HOME": "/home/" + project})
            ev = [json.loads(l) for l in r.stdout.splitlines() if l.startswith("{")]
            check(r.returncode == 0 and ev and ev[-1]["event"] == "clean", "%s: clean ran on a shared scratch without failing: %s" % (project, r.stdout[-300:]))
            if project == "beta":
                check(os.path.isdir("%s/alpha/kaos-work/%s" % (shared, JOBS["alpha"])), "beta's clean, which names alpha's job id, left alpha's old work folder alone")
            check(ev[-1]["categories"]["work-folders"]["removed"]["count"] == 1, "%s: clean removed its own one old work folder" % project)
            check(not os.path.exists("%s/%s/kaos-work/%s" % (shared, project, JOBS[project])), "%s: its work folder is gone" % project)
            check(os.path.isfile("%s/%s/sif-cache/%s.sif" % (shared, project, KEY[project])), "%s: its image cache is untouched by failed-jobs" % project)
        r = run_as("alpha", [sys.executable, STAGER, "--root", shared + "/alpha", "--account", ACCOUNT, "--scratch", SCRATCH, "usage"],
                   env={"PATH": fake + ":" + os.environ["PATH"]})
        ev = [json.loads(l) for l in r.stdout.splitlines() if l.startswith("{")]
        check(r.returncode == 0 and ev[-1]["event"] == "usage", "alpha: usage measures its own root")
    say("ALL OK")
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "job":
        job_side(sys.argv[2])
    else:
        sys.exit(main())
