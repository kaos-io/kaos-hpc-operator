"""PRD kaos#1911 F-04 / AC-05: the in-job stage-in (the fallback when nothing was staged from a login node)
downloads the whole tree of the pinned commit, whatever its shape, and completes a copy an older stage-in left
with only data.yaml, images/ and labels/ instead of serving it. The real payload runs against a fake lakeFS."""
import json
import os
import pathlib
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from kaos_hpc_runner import payloads

COMMIT = "c0ffee" * 8
YOLO = {"data.yaml": b"names: [x]", "images/a.jpg": b"a" * 100, "labels/a.txt": b"0 0.5"}
REST = {"README.md": b"# notes", "calibration/cam0.json": b'{"fx": 1}'}
NOT_YOLO = {"scenes/0001/points.ply": b"p" * 300, "scenes/0001/poses.json": b"{}",
            "calibration/cam0.json": b'{"fx": 1}', "README.md": b"# a 3D dataset", "splits.csv": b"0001,train\n"}


class _LakeFS:
    """objects/ls and objects of one commit of one repository; counts the downloads."""

    def __init__(self, objects):
        self.objects, self.downloads = objects, []
        self.hook = None       # called with the path before each download is answered
        self.short = set()     # paths answered with one byte less than the listing says
        fake = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                url = urllib.parse.urlparse(self.path)
                q = urllib.parse.parse_qs(url.query)
                if url.path.endswith("/refs/%s/objects/ls" % COMMIT):
                    body = json.dumps({"results": [{"path": p, "path_type": "object", "size_bytes": len(b)}
                                                   for p, b in sorted(fake.objects.items())],
                                       "pagination": {"has_more": False}}).encode()
                elif url.path.endswith("/refs/%s/objects" % COMMIT) and q["path"][0] in fake.objects:
                    fake.downloads.append(q["path"][0])
                    if fake.hook:
                        fake.hook(q["path"][0])
                    body = fake.objects[q["path"][0]]
                    if q["path"][0] in fake.short:
                        body = body[:-1]
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]


@pytest.fixture
def lakefs():
    made = []

    def make(objects):
        made.append(_LakeFS(objects))
        return made[-1]
    yield make
    for fake in made:
        fake.httpd.shutdown()
        fake.httpd.server_close()


def _run(tmp_path, fake):
    env = {"LAKEFS_ENDPOINT": fake.url, "LAKEFS_BEARER_TOKEN": "w", "DATASET_REPO": "ds", "DATASET_COMMIT": COMMIT,
           "DATASET_CACHE": str(tmp_path / "data-cache"), "DATASET_DIRFILE": str(tmp_path / "dirfile"),
           "SLURM_JOB_ID": "9"}
    return subprocess.run([sys.executable, "-"], input=payloads.STAGEIN, env=env, capture_output=True, text=True)


def _stage_in(tmp_path, fake):
    p = _run(tmp_path, fake)
    assert p.returncode == 0, p.stderr
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    assert (tmp_path / "dirfile").read_text() == str(dest)
    return dest, p.stdout


def _tree(dest):
    return {str(f.relative_to(dest)): f.read_bytes() for f in dest.rglob("*")
            if f.is_file() and str(f.relative_to(dest)) != ".kaos-complete"}


def test_a_tree_without_data_yaml_images_or_labels_is_downloaded_whole(tmp_path, lakefs):
    fake = lakefs(NOT_YOLO)
    dest, _ = _stage_in(tmp_path, fake)
    assert _tree(dest) == NOT_YOLO
    assert (dest / ".kaos-complete").exists()
    assert not (dest / ".kaos-commit").exists()


def test_an_old_partial_copy_is_completed_not_served(tmp_path, lakefs):
    """The copy an older stage-in left: the YOLO folders and its marker, `.complete`."""
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    for rel, body in YOLO.items():
        (dest / rel).parent.mkdir(parents=True, exist_ok=True)
        (dest / rel).write_bytes(body)
    (dest / ".complete").write_text("")
    fake = lakefs(dict(YOLO, **REST))
    dest, out = _stage_in(tmp_path, fake)
    assert "cache hit" not in out, "a copy of the YOLO folders only is not the dataset"
    assert _tree(dest) == dict(YOLO, **REST)
    assert sorted(fake.downloads) == sorted(REST), "what the old copy holds is not downloaded again"
    assert (dest / ".kaos-complete").exists() and not (dest / ".complete").exists(), "the old marker is gone"


def test_a_whole_copy_is_a_cache_hit_and_downloads_nothing(tmp_path, lakefs):
    fake = lakefs(NOT_YOLO)
    _stage_in(tmp_path, fake)
    fake.downloads.clear()
    dest, out = _stage_in(tmp_path, fake)
    assert "cache hit" in out and fake.downloads == []


def test_a_dataset_file_named_like_the_old_marker_is_data_and_never_a_marker(tmp_path, lakefs):
    """`.complete` is a name a dataset may use. The marker lives under the reserved `.kaos-` prefix, so the file
    is downloaded like any other and its presence never makes a half copy look whole."""
    tree = dict(NOT_YOLO, **{".complete": b"a file of the dataset"})
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    dest.mkdir(parents=True)
    (dest / ".complete").write_bytes(tree[".complete"])     # a download that was killed after this one file
    fake = lakefs(tree)
    dest, out = _stage_in(tmp_path, fake)
    assert "cache hit" not in out and _tree(dest) == tree
    assert (dest / ".kaos-complete").exists()


@pytest.mark.parametrize("name", [".kaos-complete", ".kaos-commit", ".kaos-x/y.bin"])
def test_a_reserved_root_name_fails_the_stage_in_before_any_download(tmp_path, lakefs, name):
    fake = lakefs(dict(NOT_YOLO, **{name: b"x"}))
    p = _run(tmp_path, fake)
    assert p.returncode != 0 and fake.downloads == []
    assert repr(name) in p.stderr and "names starting with .kaos are reserved for the platform" in p.stderr
    assert not (tmp_path / "data-cache" / "ds" / COMMIT / ".kaos-complete").exists()


@pytest.mark.parametrize("name", ["../escape.txt", "a/../../../escape.txt", "/abs/x", "a//b", "a/./b", "a/b/"])
def test_an_object_path_that_could_leave_the_cache_folder_fails_the_stage_in_before_any_download(
        tmp_path, lakefs, name):
    fake = lakefs(dict(NOT_YOLO, **{name: b"x"}))
    p = _run(tmp_path, fake)
    assert p.returncode != 0 and fake.downloads == []
    assert repr(name) in p.stderr and "ds@" in p.stderr and "outside the dataset's folder" in p.stderr
    assert not (tmp_path / "escape.txt").exists() and not (tmp_path / "data-cache" / "escape.txt").exists()
    assert not (tmp_path / "data-cache" / "ds" / COMMIT / ".kaos-complete").exists()


def test_a_folder_of_the_copy_that_is_a_link_to_elsewhere_is_never_written_through(tmp_path, lakefs):
    """Good names, but `scenes` in the cache folder is a symbolic link to a folder outside it."""
    outside = tmp_path / "outside"
    outside.mkdir()
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    dest.mkdir(parents=True)
    (dest / "scenes").symlink_to(outside, target_is_directory=True)
    p = _run(tmp_path, lakefs(NOT_YOLO))
    assert p.returncode != 0 and "outside the dataset's folder" in p.stderr and "scenes/0001/" in p.stderr
    assert list(outside.iterdir()) == [], "nothing was written through the link"
    assert not (dest / ".kaos-complete").exists()


def test_the_name_rule_agrees_with_the_shared_table(tmp_path, lakefs):
    """The stage-in is a program, not a module: each name of hack/staging-contract/names.json is its own commit of
    one object, and the verdict is what the program does with it. test_staging.py tests the runner's name_rule
    against the same table, the staging tool's repository tests its own."""
    table = json.loads((pathlib.Path(__file__).resolve().parents[2] / "hack" / "staging-contract" / "names.json")
                       .read_text())["names"]
    said = {"unsafe": "outside the dataset's folder", "reserved": "reserved for the platform",
            "long": "longer than 245 bytes"}
    for i, (path, size, verdict) in enumerate(table):
        if "\x00" in path or path == "":
            continue      # not a name the fake lakeFS can serve in a URL; the rule's text is the same in all three
        fake = lakefs({path: b"x" * size})
        (tmp_path / str(i)).mkdir()
        p = _run(tmp_path / str(i), fake)
        dest = tmp_path / str(i) / "data-cache" / "ds" / COMMIT
        if verdict == "":
            assert p.returncode == 0 and _tree(dest) == {path: b"x" * size}, (path[:60], p.stderr)
        elif verdict == "marker":
            assert p.returncode == 0 and _tree(dest) == {} and fake.downloads == [], (path, p.stderr)
        else:
            assert p.returncode != 0 and said[verdict] in p.stderr and fake.downloads == [], (path[:60], p.stderr)


def test_a_file_that_is_also_a_folder_fails_the_stage_in_before_any_download(tmp_path, lakefs):
    fake = lakefs({"a": b"x", "a-b": b"w", "a/b": b"y", "z.txt": b"z"})
    p = _run(tmp_path, fake)
    assert p.returncode != 0 and fake.downloads == []
    assert "an object named 'a'" in p.stderr and "'a/b'" in p.stderr and "a file or a folder" in p.stderr


def test_a_folder_marker_is_skipped_and_the_rest_is_downloaded(tmp_path, lakefs):
    dest, _ = _stage_in(tmp_path, lakefs({"docs/": b"", "docs/a.txt": b"a", "empty/": b""}))
    assert _tree(dest) == {"docs/a.txt": b"a"} and (dest / ".kaos-complete").exists()


# ------------------------------------------------ a copy is marked only when it IS the commit

def test_a_download_shorter_than_the_listing_is_a_failure_not_a_marker(tmp_path, lakefs):
    fake = lakefs(NOT_YOLO)
    fake.short.add("scenes/0001/points.ply")
    p = _run(tmp_path, fake)
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    assert p.returncode != 0 and not (dest / ".kaos-complete").exists()
    assert not (dest / "scenes" / "0001" / "points.ply").exists(), "the short file never took the name"


def test_files_that_vanish_while_the_copy_is_made_are_a_failure_not_a_marker(tmp_path, lakefs):
    """Whatever removes them (review of 2026-11-12: another job's cleanup removed the folder under the stage-in,
    which then marked 3410 of 10001 files as the whole dataset)."""
    fake = lakefs(NOT_YOLO)
    dest = tmp_path / "data-cache" / "ds" / COMMIT

    def remove_what_is_there(path):
        if path == "splits.csv":            # the last in path order: the others are on disk or being written
            for f in dest.glob("scenes/*/*"):
                f.unlink()
    fake.hook = remove_what_is_there
    for _ in range(3):                      # the download order is not fixed: until the hook removed something
        p = _run(tmp_path, fake)
        if p.returncode != 0:
            break
        (dest / ".kaos-complete").unlink()
        (dest / "splits.csv").unlink()
    assert p.returncode != 0 and "is not the commit" in p.stderr and "was not marked complete" in p.stderr
    assert not (dest / ".kaos-complete").exists()


def test_a_file_the_commit_does_not_have_is_a_failure_not_a_marker(tmp_path, lakefs):
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    (dest / "scenes").mkdir(parents=True)
    (dest / "scenes" / "stray.bin").write_bytes(b"?")
    p = _run(tmp_path, lakefs(NOT_YOLO))
    assert p.returncode != 0 and "'scenes/stray.bin'" in p.stderr and not (dest / ".kaos-complete").exists()


def test_half_downloaded_files_do_not_stay_and_are_not_taken_for_the_dataset(tmp_path, lakefs):
    dest, _ = _stage_in(tmp_path, lakefs(NOT_YOLO))
    assert list((dest / ".kaos-parts").iterdir()) == []
    assert not [f for f in dest.rglob("*") if ".part-" in f.name]


def test_a_part_file_a_killed_older_writer_left_is_removed_before_the_copy_is_marked(tmp_path, lakefs):
    """A writer from before .kaos-parts wrote name.part-<job>-<pid> beside the file; killed, it left a truncated
    one inside the copy. It is never a file of the dataset: it goes, then the copy is verified and marked."""
    fake = lakefs(NOT_YOLO)
    dest, _ = _stage_in(tmp_path, fake)
    os.remove(str(dest / ".kaos-complete"))
    for rel in ("scenes/0001/points.ply.part-77-3", "README.md.part-5-9"):
        (dest / rel).write_bytes(b"trunc")
    _, out = _stage_in(tmp_path, fake)
    assert (dest / ".kaos-complete").is_file() and "downloaded 0 of" in out
    assert not [f for f in dest.rglob("*") if ".part-" in f.name]
    assert _tree(dest) == NOT_YOLO


# ------------------------------------------------ the cleanup of unused copies and a stage-in at the same folder

def _cleanup_lines():
    from kaos_hpc_runner import jobscript
    lines = [l for l in jobscript.batch_lines("-", "/scr") if l.startswith("find $SCR/data-cache")]
    assert len(lines) == 2, lines
    return "\n".join(lines)


def _cleanup(tmp_path):
    env = dict(os.environ, SCR=str(tmp_path), USER=os.environ.get("USER") or os.getlogin())
    subprocess.run(["bash", "-c", _cleanup_lines()], env=env, check=True)


def _old_copy(tmp_path, marker=".complete", days=20):
    """An in-job copy of the YOLO folders with its marker last used `days` ago."""
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    for rel, body in YOLO.items():
        (dest / rel).parent.mkdir(parents=True, exist_ok=True)
        (dest / rel).write_bytes(body)
    (dest / marker).write_text("")
    old = time.time() - days * 86400
    os.utime(str(dest / marker), (old, old))
    return dest


@pytest.mark.parametrize("marker", [".complete", ".kaos-complete"])
def test_an_unused_copy_is_removed_by_the_cleanup_and_nothing_is_left_beside_it(tmp_path, marker):
    dest = _old_copy(tmp_path, marker)
    _cleanup(tmp_path)
    assert list(dest.parent.iterdir()) == []


def test_a_copy_that_is_being_completed_is_never_removed_by_another_jobs_cleanup(tmp_path, lakefs):
    """The race of the review: the old copy's .complete is 20 days old; while the stage-in fills the folder, the
    cleanup lines of another job run (here: at every download). The old marker is gone before the first download,
    so the cleanup has nothing to match."""
    dest = _old_copy(tmp_path)
    fake = lakefs(dict(YOLO, **REST))
    seen = []

    def another_jobs_cleanup(path):
        seen.append((dest / ".complete").exists())
        _cleanup(tmp_path)
    fake.hook = another_jobs_cleanup
    p = _run(tmp_path, fake)
    assert p.returncode == 0, p.stderr
    assert seen and not any(seen), "the old marker was removed before the first download"
    assert _tree(dest) == dict(YOLO, **REST) and (dest / ".kaos-complete").exists()
    assert sorted(f.name for f in dest.parent.iterdir()) == [COMMIT]


def test_a_dataset_file_named_complete_that_is_old_does_not_let_the_cleanup_take_the_folder(tmp_path, lakefs):
    tree = dict(YOLO, **{".complete": b"", "more.bin": b"m"})
    dest = _old_copy(tmp_path)              # .complete: empty and 20 days old, and also a file of this dataset
    fake = lakefs(tree)
    fake.hook = lambda path: _cleanup(tmp_path)
    p = _run(tmp_path, fake)
    assert p.returncode == 0, p.stderr
    assert _tree(dest) == {k: v for k, v in tree.items()} and (dest / ".kaos-complete").exists()


def test_a_cleanup_that_holds_the_folder_is_waited_for_and_the_copy_is_made_again(tmp_path, lakefs):
    """The cleanup took the folder (its marker was old), then a job arrives that wants it. The job touches the
    marker, sees the cleanup, waits, and starts over on what is there: nothing, so it downloads."""
    dest = _old_copy(tmp_path, ".kaos-complete")
    lock = pathlib.Path(str(dest) + ".kaos-clean")
    lock.mkdir()
    fake = lakefs(NOT_YOLO)
    env = {"LAKEFS_ENDPOINT": fake.url, "LAKEFS_BEARER_TOKEN": "w", "DATASET_REPO": "ds", "DATASET_COMMIT": COMMIT,
           "DATASET_CACHE": str(tmp_path / "data-cache"), "DATASET_DIRFILE": str(tmp_path / "dirfile"),
           "SLURM_JOB_ID": "9"}
    p = subprocess.Popen([sys.executable, "-"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, env=env, text=True)
    p.stdin.write(payloads.STAGEIN)
    p.stdin.close()
    time.sleep(1.5)
    assert p.poll() is None and fake.downloads == [], "it waits for the cleanup; it does not use the folder"
    os.rename(str(dest), str(dest) + ".kaos-trash-1")       # what the cleanup does next, in this order
    lock.rmdir()
    assert p.wait(60) == 0, p.stderr.read()
    assert "cache hit" not in p.stdout.read() and _tree(dest) == NOT_YOLO and (dest / ".kaos-complete").exists()


def test_a_lock_left_by_a_killed_cleanup_is_not_obeyed_for_ever(tmp_path, lakefs):
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    lock = pathlib.Path(str(dest) + ".kaos-clean")
    lock.mkdir(parents=True)
    old = time.time() - 1200
    os.utime(str(lock), (old, old))
    dest, _ = _stage_in(tmp_path, lakefs(NOT_YOLO))
    assert _tree(dest) == NOT_YOLO
    _old = _old_copy(tmp_path / "x", ".kaos-complete")
    stale = pathlib.Path(str(_old) + ".kaos-clean")
    stale.mkdir()
    os.utime(str(stale), (old, old))
    _cleanup(tmp_path / "x")                # removes the stale lock; the next cleanup takes the folder
    assert not stale.exists() and _old.exists()
    _cleanup(tmp_path / "x")
    assert list(_old.parent.iterdir()) == []


def test_a_cache_hit_renews_both_markers(tmp_path, lakefs):
    """A job script from before .kaos-complete cleans by .complete: a copy this job uses must not look unused
    to it."""
    fake = lakefs(NOT_YOLO)
    dest, _ = _stage_in(tmp_path, fake)
    (dest / ".complete").write_text("")     # written by a job of the older generation that used the copy
    old = time.time() - 20 * 86400
    for m in (".complete", ".kaos-complete"):
        os.utime(str(dest / m), (old, old))
    _, out = _stage_in(tmp_path, fake)
    assert "cache hit" in out
    assert all(time.time() - (dest / m).stat().st_mtime < 60 for m in (".complete", ".kaos-complete"))


_CLEANUP_MOVES_THE_FOLDER = """
import os
_real, _done = os.utime, []
def _utime(p, t=None):
    _real(p, t)
    if str(p).endswith('.kaos-complete') and not _done:
        _done.append(1)
        d = os.path.dirname(str(p))
        os.rename(d, d + '.kaos-trash-777')     # the cleanup's move, right after the marker was renewed
        if %r:
            os.makedirs(os.path.join(d, '.kaos-parts'))   # another stage-in has begun filling the folder
os.utime = _utime
"""


@pytest.mark.parametrize("refilled", [False, True])
def test_a_hit_on_a_folder_the_cleanup_moved_after_the_renewal_is_not_a_hit(tmp_path, lakefs, refilled):
    """The window of rule 3 that is left: the cleanup read the marker's age, the stage-in renewed the marker,
    the cleanup moved the folder and let go, and the stage-in then found no lock. The marker is checked again
    after that look: gone, so the stage-in starts over and ends with a whole copy, never a `cache hit` on a
    folder that is gone (FileNotFoundError) or on one another stage-in has begun to fill."""
    fake = lakefs(NOT_YOLO)
    dest, _ = _stage_in(tmp_path, fake)
    old = time.time() - 20 * 86400
    os.utime(str(dest / ".kaos-complete"), (old, old))
    env = {"LAKEFS_ENDPOINT": fake.url, "LAKEFS_BEARER_TOKEN": "w", "DATASET_REPO": "ds", "DATASET_COMMIT": COMMIT,
           "DATASET_CACHE": str(tmp_path / "data-cache"), "DATASET_DIRFILE": str(tmp_path / "dirfile"),
           "SLURM_JOB_ID": "9"}
    p = subprocess.run([sys.executable, "-"], input=_CLEANUP_MOVES_THE_FOLDER % refilled + payloads.STAGEIN,
                       env=env, capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    assert "cache hit" not in p.stdout
    assert "FileNotFoundError" not in p.stderr
    assert _tree(dest) == NOT_YOLO and (dest / ".kaos-complete").is_file()


def test_the_platforms_own_folder_is_downloaded(tmp_path, lakefs):
    tree = dict(NOT_YOLO, **{".kaos/seal.json": b"{}"})
    dest, _ = _stage_in(tmp_path, lakefs(tree))
    assert _tree(dest) == tree
