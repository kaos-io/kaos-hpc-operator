"""The checkpoint copier that runs on the compute node (payload/ckpt.py): at most two copies per step, one archive
plus one manifest each, restore with fallback, the space guard, success cleanup and 7-day expiry (D-74, D-75, T-26)."""
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import time
import urllib.parse
from pathlib import Path

import pytest

from kaos_hpc_runner import jobscript, payloads

PAYLOAD = payloads.CKPT
WF, STEP = "wf1", "train"
GB = 1 << 30


class Node:
    """A compute node in a directory: a scratch root, a work folder, fake lfs / scontrol / squeue."""

    def __init__(self, tmp_path, quota=None, squeue=""):
        self.tmp = tmp_path
        self.scr = tmp_path / "scr"
        self.root = self.scr / "kaos-ckpt"
        self.work = tmp_path / "work"
        self.folder = self.work / "ckpt"
        self.folder.mkdir(parents=True)
        self.scontrol_log = tmp_path / "scontrol.log"
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        self.script = tmp_path / "ckpt.py"
        self.script.write_text(PAYLOAD)
        self.set_quota(quota)
        # A stateful site: scontrol update stores the comment (and logs the call), squeue -j shows it.
        (self.bin / "scontrol").write_text(
            '#!/bin/bash\nprintf "%s\\n" "$*" >> "$SCONTROL_LOG"\n'
            'for a in "$@"; do case "$a" in Comment=*) printf %s "${a#Comment=}" > "$COMMENT_FILE";; esac; done\n')
        (self.bin / "scontrol").chmod(0o755)
        self.comment_file = tmp_path / "job.comment"
        self.set_squeue(squeue)

    def set_quota(self, quota):
        """quota: None (lfs fails), or (bytes_used, bytes_limit, files_used, files_limit) as lfs prints it in KiB."""
        lfs = self.bin / "lfs"
        if quota is None:
            lfs.write_text("#!/bin/bash\nexit 1\n")
        else:
            used, limit, fused, flimit = quota
            lfs.write_text("#!/bin/bash\necho '/scr %d %d %d - %d %d %d -'\n" % (used // 1024, limit // 1024, limit // 1024, fused, flimit, flimit))
        lfs.chmod(0o755)

    def set_squeue(self, text, fail=False):
        sq = self.bin / "squeue"
        # `squeue -j <id>` is the job's own comment (what scontrol update stored); `squeue -u` lists everyone's.
        sq.write_text("#!/bin/bash\n%s\n" % ("exit 1" if fail else
                      'case "$*" in *" -j "*) cat "$COMMENT_FILE" 2>/dev/null;; *) cat <<\'EOF\'\n%s\nEOF\n;; esac' % text))
        sq.chmod(0o755)

    def run(self, mode, **env):
        e = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"], KAOS_WORK=str(self.work),
                 KAOS_CKPT_ROOT=str(self.root), KAOS_SCR=str(self.scr), WF_UID=WF, STEP_NAME=STEP, KAOS_ATTEMPT="1",
                 KAOS_JOB_ID="77", SCONTROL_LOG=str(self.scontrol_log), KAOS_COMMIT="c0ffee12", KAOS_PROJID="201342",
                 USER="u1", KAOS_CKPT_BUDGET_PCT="10")
        e.update(env, COMMENT_FILE=str(self.comment_file))
        # The copier runs under the job script's exported kaos_comment, as on the node.
        shell = "\n".join(jobscript.COMMENT_HELPER + ["export -f kaos_comment", 'exec "$@"'])
        return subprocess.run(["bash", "-c", shell, "run", sys.executable, str(self.script), mode], env=e,
                              capture_output=True, text=True)

    def write(self, files):
        for name, data in files.items():
            p = self.folder / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data if isinstance(data, bytes) else data.encode())

    @property
    def store(self):
        return self.root / WF / STEP

    def names(self):
        return sorted(p.name for p in self.store.iterdir()) if self.store.is_dir() else []

    def manifests(self):
        return [json.loads(p.read_text()) for p in sorted(self.store.glob("*.json"))]

    def comment(self):
        lines = self.scontrol_log.read_text().splitlines() if self.scontrol_log.exists() else []
        return lines[-1] if lines else ""


@pytest.fixture
def node(tmp_path):
    return Node(tmp_path, quota=(100 * GB, 1000 * GB, 10_000, 1_000_000))


def test_a_copy_is_one_archive_and_one_manifest_whatever_the_number_of_files(node):
    node.write({"last.pt": os.urandom(5000), **{"logs/e%03d.txt" % i: "x%d" % i for i in range(300)}})
    r = node.run("final")
    assert r.returncode == 0, r.stdout + r.stderr
    assert node.names() == ["000001.json", "000001.tar"], "301 files in the folder, 2 on scratch (file quota)"
    [man] = node.manifests()
    assert len(man["files"]) == 301 and man["tar_bytes"] == (node.store / "000001.tar").stat().st_size
    assert man["tar_sha256"] == hashlib.sha256((node.store / "000001.tar").read_bytes()).hexdigest()
    with tarfile.open(node.store / "000001.tar") as tf:
        assert len(tf.getnames()) == 301


def test_the_job_comment_names_the_copy_its_path_and_its_progress(node):
    node.write({"last.pt": b"w"})
    (node.folder / "kaos-progress.json").write_text(json.dumps({"values": [3, 1200], "text": "epoch=3 step=1200"}))
    assert node.run("final").returncode == 0
    comment = node.comment()
    man = node.manifests()[0]
    assert "JobId=77" in comment and "kaos-commit=c0ffee12" in comment
    assert "kaos-ckpt=" + man["tar_sha256"] in comment and "kaos-ckpt-path=kaos-ckpt/wf1/train/000001.tar" in comment
    assert "kaos-ckpt-lease=wf1/train" in comment
    info = json.loads(urllib.parse.unquote(comment.split("kaos-ckpt-info=")[1].split()[0]))
    assert info["p"] == [3, 1200] and info["pt"] == "epoch=3 step=1200" and info["b"] == man["tar_bytes"]
    assert info["sb"] == man["tar_bytes"] and info["pb"] >= man["tar_bytes"]
    assert (node.work / ".kaos-note").read_text().startswith("kaos-ckpt-lease=wf1/train kaos-ckpt=")


def test_progress_comes_from_the_librarys_own_manifest_when_the_step_reports_none(node):
    node.write({"ckpt-000002/state.txt": "1", "ckpt-000002/manifest.json": json.dumps(
        {"progress_values": [4, 7], "progress_text": "epoch=4 step=7", "files": {}})})
    assert node.run("final").returncode == 0
    assert node.manifests()[0]["progress_values"] == [4, 7]


def test_at_most_two_copies_the_oldest_is_replaced_never_a_third(node):
    for i in range(1, 5):
        node.write({"last.pt": "version %d" % i})
        assert node.run("final").returncode == 0
        assert len(node.names()) <= 4
    assert node.names() == ["000003.json", "000003.tar", "000004.json", "000004.tar"]


def test_an_unchanged_folder_is_not_copied_again_and_same_content_keeps_the_older_copy(node):
    node.write({"last.pt": "same"})
    assert node.run("final").returncode == 0
    node.write({"last.pt": "same"})                # rewritten, same bytes
    os.utime(node.folder / "last.pt", (time.time() + 5, time.time() + 5))
    assert node.run("final").returncode == 0
    assert node.names() == ["000001.json", "000001.tar"]


def test_a_final_copy_of_the_folder_the_wall_time_copy_took_is_skipped_and_logged_as_such(node):
    node.write({"last.pt": "epoch 1111"})
    first = node.run("final")
    assert first.returncode == 0 and "copy 000001" in first.stdout
    before = (node.store / "000001.tar").read_bytes()
    second = node.run("final")                     # the exit-85 copy, a new process (no memory of the first)
    assert second.returncode == 0
    assert "copy 000002" not in second.stdout and "no new copy" in second.stdout and "1 kept" in second.stdout
    assert node.names() == ["000001.json", "000001.tar"]
    assert (node.store / "000001.tar").read_bytes() == before
    assert "kaos-ckpt-path=kaos-ckpt/wf1/train/000001.tar" in node.comment()


def test_a_duplicate_final_copy_never_costs_the_older_copy_its_place(node):
    node.write({"last.pt": "epoch 1"})
    assert node.run("final").returncode == 0
    node.write({"last.pt": "epoch 2"})
    assert node.run("final").returncode == 0
    assert node.names() == ["000001.json", "000001.tar", "000002.json", "000002.tar"]
    assert node.run("final").returncode == 0       # nothing changed since copy 2
    assert node.names() == ["000001.json", "000001.tar", "000002.json", "000002.tar"]


def test_an_empty_folder_makes_no_copy_and_is_not_an_error(node):
    r = node.run("final")
    assert r.returncode == 0 and node.names() == []


def test_a_library_save_in_progress_is_not_copied(node):
    node.write({"last.pt": "done", ".tmp-abc/half.bin": "half"})
    assert node.run("final").returncode == 0
    with tarfile.open(node.store / "000001.tar") as tf:
        assert tf.getnames() == ["last.pt"]


# ---------------------------------------------------------------------------------------------- restore

def test_restore_puts_the_named_copy_back_byte_for_byte(node):
    node.write({"last.pt": os.urandom(4096), "sub/opt.bin": b"adam"})
    expected = {p.relative_to(node.folder).as_posix(): p.read_bytes() for p in node.folder.rglob("*") if p.is_file()}
    assert node.run("final").returncode == 0
    sha = node.manifests()[0]["tar_sha256"]
    import shutil
    shutil.rmtree(node.folder)                         # a new node: an empty work folder
    node.folder.mkdir()
    r = node.run("restore", KAOS_RESUME_REF=sha)
    assert r.returncode == 0, r.stdout
    got = {p.relative_to(node.folder).as_posix(): p.read_bytes() for p in node.folder.rglob("*") if p.is_file()}
    assert got == expected


def test_restore_falls_back_to_the_previous_good_copy_when_the_newest_is_corrupt(node):
    node.write({"last.pt": "one"}); assert node.run("final").returncode == 0
    node.write({"last.pt": "two!"}); assert node.run("final").returncode == 0
    newest = json.loads((node.store / "000002.json").read_text())["tar_sha256"]
    (node.store / "000002.tar").write_bytes(b"torn archive")
    r = node.run("restore", KAOS_RESUME_REF=newest)
    assert r.returncode == 0 and (node.folder / "last.pt").read_text() == "one"
    assert "cannot be used" in r.stdout and "previous" not in r.stderr


def test_restore_fails_loudly_when_no_copy_verifies_never_a_silent_fresh_start(node):
    node.write({"last.pt": "one"}); assert node.run("final").returncode == 0
    (node.store / "000001.tar").write_bytes(b"x")
    r = node.run("restore", KAOS_RESUME_REF="0" * 64)
    assert r.returncode == 1 and "not started from scratch" in r.stdout
    assert node.run("restore", KAOS_RESUME_REF="0" * 64, WF_UID="other").returncode == 1


def test_restore_refuses_an_archive_that_would_write_outside_the_folder(node):
    node.store.mkdir(parents=True)
    evil = io.BytesIO()
    with tarfile.open(fileobj=evil, mode="w") as tf:
        info = tarfile.TarInfo("../escape.txt"); info.size = 1
        tf.addfile(info, io.BytesIO(b"x"))
    data = evil.getvalue()
    (node.store / "000001.tar").write_bytes(data)
    (node.store / "000001.json").write_text(json.dumps({"tar_sha256": hashlib.sha256(data).hexdigest(), "tar_bytes": len(data),
                                                        "files": {"../escape.txt": {"size": 1, "sha256": ""}}}))
    r = node.run("restore", KAOS_RESUME_REF=hashlib.sha256(data).hexdigest())
    assert r.returncode == 1 and not (node.work / "escape.txt").exists() and not (node.tmp / "escape.txt").exists()


# ---------------------------------------------------------------------------------------------- the space guard

def test_a_periodic_copy_is_skipped_over_80_percent_of_the_space_quota_and_the_last_copy_is_kept(node):
    node.write({"last.pt": "one"}); assert node.run("final").returncode == 0
    node.write({"last.pt": os.urandom(4096)})
    node.set_quota((800 * GB - 1024, 1000 * GB, 10, 1_000_000))
    r = node.run("periodic")
    assert r.returncode == 0, "the last copy still stands"
    assert node.names() == ["000001.json", "000001.tar"], "nothing replaced, nothing added"
    assert "skipped the periodic copy: it would take the project over 80% of its scratch space" in r.stdout
    info = json.loads(urllib.parse.unquote(node.comment().split("kaos-ckpt-info=")[1].split()[0]))
    assert info["sk"].startswith("skipped the periodic copy")


def test_a_periodic_copy_is_skipped_over_80_percent_of_the_file_quota(node):
    node.write({"last.pt": "one"})
    node.set_quota((GB, 1000 * GB, 800_000, 1_000_000))
    r = node.run("periodic")
    assert r.returncode == 1 and node.names() == [] and "scratch file quota" in r.stdout


def test_a_periodic_copy_is_skipped_over_the_checkpoint_budget(node):
    big = os.urandom(1 << 20)
    node.write({"last.pt": big})
    node.set_quota((1 << 20, 8 << 20, 10, 1_000_000))              # budget = 10 % of 8 MiB = 0.8 MiB < the 1 MiB copy
    r = node.run("periodic")
    assert r.returncode == 1 and node.names() == [] and "pass their budget (10% of the scratch space)" in r.stdout
    assert node.run("periodic", KAOS_CKPT_BUDGET_PCT="50").returncode == 0, "the budget is configurable"


def test_the_budget_counts_other_steps_copies_of_the_project(node):
    other = node.root / "wfX" / "other"
    other.mkdir(parents=True)
    (other / "000001.tar").write_bytes(b"x" * (700 * 1024))
    node.write({"last.pt": os.urandom(200 * 1024)})
    node.set_quota((1 << 20, 8 << 20, 10, 1_000_000))              # budget 0.8 MiB: 700 KiB held + 200 KiB new > budget
    assert node.run("periodic").returncode == 1


def test_a_periodic_copy_fails_closed_when_the_quota_cannot_be_read(node):
    node.write({"last.pt": "one"})
    node.set_quota(None)
    r = node.run("periodic")
    assert r.returncode == 1 and "quota cannot be read" in r.stdout and node.names() == []


def test_a_periodic_copy_goes_ahead_under_the_limits(node):
    node.write({"last.pt": "one"})
    assert node.run("periodic").returncode == 0 and node.names() == ["000001.json", "000001.tar"]


def test_the_wall_time_copy_ignores_the_guard_but_still_never_adds_a_third(node):
    for i in range(2):
        node.write({"last.pt": "v%d" % i}); assert node.run("final").returncode == 0
    node.set_quota((999 * GB, 1000 * GB, 999_999, 1_000_000))       # the project is full
    node.write({"last.pt": "the wall-time state"})
    assert node.run("final").returncode == 0
    assert node.names() == ["000002.json", "000002.tar", "000003.json", "000003.tar"]


def test_the_guard_counts_the_copy_that_will_be_replaced_as_freed_space(node):
    for i in range(2):
        node.write({"last.pt": os.urandom(400 * 1024)}); assert node.run("final").returncode == 0
    node.write({"last.pt": os.urandom(400 * 1024)})
    # 2 copies of 400 KiB are held; a third would pass the 1 MiB budget (10 % of 10 MiB) but replacing one does not
    node.set_quota((1 << 20, 10 << 20, 10, 1_000_000))
    assert node.run("periodic").returncode == 0 and len(node.names()) == 4


# ---------------------------------------------------------------------------------------------- expiry

def _aged(path, days):
    t = time.time() - days * 86400
    for p in [path, *path.rglob("*")]:
        os.utime(p, (t, t))


def test_abandoned_copies_expire_after_seven_days_unless_a_queued_job_names_them(node):
    for wf, days in (("old", 8), ("young", 6), ("leased", 30)):
        d = node.root / wf / STEP
        d.mkdir(parents=True)
        (d / "000001.tar").write_bytes(b"x")
        _aged(d, days)
    node.set_squeue("kaos-commit=c0ffee12 kaos-ckpt-lease=leased/%s\nkaos-ckpt-lease=other/x" % STEP)
    assert node.run("expire").returncode == 0
    assert not (node.root / "old").exists(), "abandoned for 8 days"
    assert (node.root / "young" / STEP / "000001.tar").exists() and (node.root / "leased" / STEP / "000001.tar").exists()


def test_nothing_expires_when_the_queue_cannot_be_read(node):
    d = node.root / "old" / STEP
    d.mkdir(parents=True)
    (d / "000001.tar").write_bytes(b"x")
    _aged(d, 30)
    node.set_squeue("", fail=True)
    r = node.run("expire")
    assert r.returncode == 0 and (d / "000001.tar").exists() and "expiry skipped" in r.stdout


def test_an_unknown_mode_is_a_usage_error(node):
    assert node.run("bogus").returncode == 2
