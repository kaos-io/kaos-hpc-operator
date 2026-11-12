"""PRD-HPC-1541 F-03/F-06: the runner stages the pinned dataset commit from a login node before it submits, and
the job binds the resident copy. The stager itself (kubecore-operator images/meluxina-tokenmint/stager) is
tested in the operator repo; here it is a stand-in that records what the runner asks for and streams, against a
fake lakeFS."""
import io
import json
import os
import pathlib
import stat
import urllib.request

import helpers
import pytest
from kaos_hpc_runner import staging

COMMIT = "c2" * 32
BASE = "c1" * 32


class _LakeFS:
    """urlopen stand-in: objects/ls and a two-dot diff."""

    def __init__(self, objects, diff=()):
        self.objects, self.diff, self.urls = objects, list(diff), []

    def __call__(self, req, timeout=0):
        url = req.full_url if hasattr(req, "full_url") else req
        self.urls.append(url)
        q = dict(p.split("=", 1) for p in url.split("?", 1)[1].split("&"))
        prefix = urllib.request.unquote(q.get("prefix", ""))
        if "/diff/" in url:
            res = [dict(d, path_type="object") for d in self.diff if d["path"].startswith(prefix)]
        else:
            res = [{"path": p, "path_type": "object", "size_bytes": n}
                   for p, n in self.objects.items() if p.startswith(prefix)]
        return io.BytesIO(json.dumps({"results": res, "pagination": {"has_more": False}}).encode())


class _Stager:
    """Stand-in for the login-node stager: canned version and status, records stage calls."""

    def __init__(self, status, stage_rcs=(0,), key=True, version=(0, 2), status_rc=0):
        self.status, self.stage_rcs, self.calls, self.key = status, list(stage_rcs), [], key
        self.version, self.status_rc = version, status_rc
        # What ssh printed for the last call. Exit 2 from `version` with the tool's own usage error is a tool from
        # before `version`; exit 2 without it is python failing to open the file.
        self.last_error = "kaos-stager: error: forced command accepts: status|verify|stage" if version[0] == 2 else ""

    def have_key(self):
        return self.key

    def __call__(self, words, feed=None, quiet=False):
        records = list(feed) if feed is not None else None
        self.calls.append((words, records))
        if words[0] == "version":
            rc, contract = self.version
            return rc, ([{"event": "version", "contract": contract}] if rc == 0 else [])
        if words[0] == "status":
            if self.status_rc:
                return self.status_rc, [{"event": "contract", "contract": 3, "got": 2}]
            return 0, [dict(self.status, event="status", path="/scr/kaos/p/datasets/ds/" + COMMIT)]
        rc = self.stage_rcs.pop(0)
        return rc, ([{"event": "done", "path": "/scr/kaos/p/datasets/ds/" + COMMIT}] if rc == 0 else [])


def _twin(monkeypatch, lakefs, stager, ref="latest", **env):
    over = {"HPC_INPUTS": json.dumps([{"name": "dataset", "repo": "dataset-poulaki", "ref": ref, "commit": COMMIT}]),
            "LAKEFS_EXTERNAL_URL": "https://lakefs.example/"}
    over.update(env)
    cfg = helpers.settings(**over)
    cfg.check_job()      # the job's one input is cfg.inputs[0]
    monkeypatch.setattr(urllib.request, "urlopen", lakefs)
    monkeypatch.setattr(staging, "ssh_present", lambda: True)
    monkeypatch.setattr(staging.time, "sleep", lambda s: None)
    return cfg


def _stage(cfg, stager, wallet="wallet-jwt"):
    return staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: wallet)


def _listings(lakefs):
    """The listings of the commit's objects."""
    return [u for u in lakefs.urls if "/objects/ls?" in u]


def _stage_call(stager):
    return next((w, r) for w, r in stager.calls if w[0] == "stage")



def test_first_copy_streams_every_dataset_object(monkeypatch):
    lakefs = _LakeFS({"data.yaml": 4, "images/a.jpg": 10, "README.md": 99})
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager).endswith(COMMIT)
    words, records = _stage_call(stager)
    assert "--base" not in words
    assert words[words.index("--need-bytes") + 1] == 113 and words[words.index("--need-files") + 1] == 3
    assert records[0] == {"op": "auth", "authorization": "Bearer wallet-jwt"}
    fetched = {r["path"]: r for r in records if r["op"] == "fetch"}
    assert set(fetched) == {"data.yaml", "images/a.jpg", "README.md"}, "every object of the commit"
    assert fetched["images/a.jpg"]["url"] == (
        "https://lakefs.example/api/v1/repositories/dataset-poulaki/refs/%s/objects?path=images%%2Fa.jpg"
        % COMMIT)
    assert records[-1] == {"op": "end", "files": 3, "bytes": 113, "ref": "main", "prefix": ""}


# ------------------------------------------------ any dataset shape (PRD kaos#1911 F-04, AC-05)

NOT_YOLO = {"scenes/0001/points.ply": 300, "scenes/0001/poses.json": 2, "calibration/cam0.json": 9,
            "README.md": 14, "splits.csv": 20}


def test_a_tree_without_data_yaml_images_or_labels_is_streamed_whole(monkeypatch):
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS(NOT_YOLO), stager)
    info = {}
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "wallet-jwt", info=info).endswith(COMMIT)
    words, records = _stage_call(stager)
    assert {r["path"]: r["size"] for r in records if r["op"] == "fetch"} == NOT_YOLO
    assert records[-1] == {"op": "end", "files": 5, "bytes": 345, "ref": "main", "prefix": ""}, \
        "the stager checks the copy against the whole count before it marks it ready"
    assert (info["kind"], info["files"], info["bytes"]) == ("first", 5, 345)


def test_a_delta_carries_every_changed_path_whatever_its_folder(monkeypatch):
    lakefs = _LakeFS({}, diff=[{"type": "added", "path": "scenes/0002/points.ply", "size_bytes": 5},
                               {"type": "changed", "path": "README.md", "size_bytes": 7},
                               {"type": "removed", "path": "notes/old.txt", "size_bytes": 3}])
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": ""})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager)
    _, records = _stage_call(stager)
    assert [r["path"] for r in records if r["op"] == "fetch"] == ["scenes/0002/points.ply", "README.md"]
    assert {"op": "delete", "path": "notes/old.txt"} in records and records[-1]["files"] == 10


def test_the_filter_is_gone():
    assert not hasattr(staging, "is_dataset_path")


# ------------------------------------------------ names reserved for the platform

@pytest.mark.parametrize("name", [".kaos-ready", ".kaos-commit", ".kaos-complete", ".kaos-notes/a.txt"])
def test_a_dataset_object_named_like_a_marker_of_a_copy_is_staged_like_any_other(monkeypatch, name):
    """The dataset's files live under data/ in a copy and the markers beside that folder, so a dataset may use any
    name that is not unsafe, too long or a half-downloaded file's. The staging tool is asked, and sent the object."""
    lakefs = _LakeFS(dict(NOT_YOLO, **{name: 3}))
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager).endswith(COMMIT)
    assert name in {r["path"] for r in _stage_call(stager)[1] if r["op"] == "fetch"}
    assert not any("prefix=" in u and "prefix=&" not in u for u in lakefs.urls), "no extra look at the root names"


@pytest.mark.parametrize("diff", [False, True])
def test_a_name_ending_in_the_staging_tools_part_suffix_stops_the_run_before_the_copy(monkeypatch, diff):
    name = "scenes/0001/points.ply.kaos-part"
    lakefs = _LakeFS({name: 3, "a.bin": 1}, diff=[{"type": "added", "path": name, "size_bytes": 3}])
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": ""} if diff
                     else {"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager)
    with pytest.raises(staging.ReservedName) as e:
        _stage(cfg, stager)
    assert repr(name) in str(e.value)
    assert [w[0] for w, _ in stager.calls] == ["version", "status"], "refused while planning: no stage call"


UNSAFE = ["../escape.txt", "a/../../escape.txt", "/abs/x", "a//b", "a/./b", "a/b/", "x\x00y"]


@pytest.mark.parametrize("name", UNSAFE)
@pytest.mark.parametrize("key", [True, False])
@pytest.mark.parametrize("diff", [False, True])
def test_an_object_path_that_could_leave_the_dataset_folder_stops_the_run_on_either_staging_path(
        monkeypatch, name, key, diff):
    """A path with `..`, an empty or `.` part, a leading slash or a NUL byte. The staging tool refuses it mid-copy
    and the in-job stage-in would join it to the cache folder. With the staging key it is refused while the copy
    is planned; without it (the job would stage in-job) the runner lists the commit once before it submits."""
    lakefs = _LakeFS({name: 3, "a.bin": 1}, diff=[{"type": "added", "path": name, "size_bytes": 3}])
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": ""} if diff
                     else {"ready": False, "base": None}, key=key)
    cfg = _twin(monkeypatch, lakefs, stager)
    with pytest.raises(staging.ReservedName) as e:
        _stage(cfg, stager)
    assert not any(w[0] == "stage" for w, _ in stager.calls), "nothing is copied"
    msg = str(e.value)
    assert repr(name) in msg and "dataset-poulaki@" in msg and "outside the dataset's folder" in msg
    assert "reserved for the platform" not in msg, "the sentence says what is wrong with this name"
    assert "Nothing was staged and nothing was submitted" in msg


def test_a_job_that_will_stage_in_job_has_its_names_checked_before_it_is_submitted(monkeypatch):
    """Every way to the in-job copy with a wallet in hand: no staging key, no ssh client, a staging tool that
    cannot be reached within the budget."""
    tree = {"ok/a.bin": 1, "a/../../escape.txt": 3}
    for how in ("no key", "no ssh", "unreachable"):
        stager = _Stager({"ready": False, "base": None}, key=how != "no key", version=(255, None))
        cfg = _twin(monkeypatch, _LakeFS(tree), stager)
        if how == "no ssh":
            monkeypatch.setattr(staging, "ssh_present", lambda: False)
        clock = [1000.0]
        monkeypatch.setattr(staging.time, "time", lambda: clock[0])
        monkeypatch.setattr(staging.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
        with pytest.raises(staging.ReservedName):
            _stage(cfg, stager)
    stager = _Stager({"ready": False, "base": None}, key=False)
    lakefs = _LakeFS({"ok/a.bin": 1, ".kaos/seal.json": 2})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager) == "", "good names: the job stages in-job as before"
    assert len(_listings(lakefs)) == 1


def test_the_name_rule_agrees_with_the_shared_table():
    """hack/staging-contract/names.json is the same file next to the staging tool (kubecore-operator
    images/meluxina-tokenmint/stager/names.json), whose own name_rule is tested against it there; the in-job
    stage-in's is tested against it in test_stagein.py."""
    table = json.loads((pathlib.Path(__file__).resolve().parents[2] / "hack" / "staging-contract" / "names.json")
                       .read_text())["names"]
    assert len(table) > 30
    for path, size, verdict in table:
        assert staging.name_rule(path, size) == verdict, repr(path)[:80]


@pytest.mark.parametrize("key", [True, False])
def test_a_file_that_is_also_a_folder_stops_the_run_on_either_staging_path(monkeypatch, key):
    """`a` and `a/b` in one commit: the object store holds both, a file system cannot. In path order `a-b` comes
    between them."""
    tree = {"a": 1, "a-b": 1, "a/b": 1, "z.txt": 1}
    stager = _Stager({"ready": False, "base": None}, key=key)
    cfg = _twin(monkeypatch, _LakeFS(tree), stager)
    with pytest.raises(staging.ReservedName) as e:
        _stage(cfg, stager)
    msg = str(e.value)
    assert "an object named 'a'" in msg and "'a/b'" in msg and "a file or a folder under one name" in msg
    assert not any(w[0] == "stage" for w, _ in stager.calls)


def test_names_that_only_look_like_a_file_and_its_folder_are_staged(monkeypatch):
    tree = {"a": 1, "a-b/c": 1, "a.d/e": 1, "ab": 1, "b/c": 1, "b/c.d": 1}
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS(tree), stager)
    assert _stage(cfg, stager)
    assert {r["path"] for r in _stage_call(stager)[1] if r["op"] == "fetch"} == set(tree)


@pytest.mark.parametrize("name", ["n" * 246, "d/" + "n" * 250, "d" * 256 + "/x"])
def test_a_name_too_long_for_the_site_stops_the_run_before_the_copy(monkeypatch, name):
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({name: 1, "ok": 1}), stager)
    with pytest.raises(staging.ReservedName) as e:
        _stage(cfg, stager)
    assert "longer than 245 bytes" in str(e.value) and "reserved for the platform" not in str(e.value)
    assert not any(w[0] == "stage" for w, _ in stager.calls)


def test_a_folder_marker_is_skipped_not_refused(monkeypatch):
    """Object stores keep zero-byte objects named `docs/` as folder markers. Nothing is sent for one and it is
    not counted, in a first copy and in a delta."""
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({"docs/": 0, "docs/a.txt": 4, "empty/": 0}), stager)
    info = {}
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "wallet-jwt", info=info)
    words, records = _stage_call(stager)
    assert [r["path"] for r in records if r["op"] == "fetch"] == ["docs/a.txt"]
    assert records[-1]["files"] == 1 and words[words.index("--need-files") + 1] == 1 and info["files"] == 1
    lakefs = _LakeFS({}, diff=[{"type": "added", "path": "new/", "size_bytes": 0},
                               {"type": "added", "path": "new/b.txt", "size_bytes": 2},
                               {"type": "removed", "path": "old/", "size_bytes": 0}])
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": ""})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager)
    _, records = _stage_call(stager)
    assert [(r["op"], r.get("path")) for r in records[1:]] == [("fetch", "new/b.txt"), ("end", None)]
    assert records[-1]["files"] == 11, "base count + the one file added; markers are not files"
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({"docs/": 3}), stager)
    with pytest.raises(staging.ReservedName):
        _stage(cfg, stager)                # a name ending in / that has content cannot be a file


def test_the_platforms_own_folder_and_ordinary_dot_names_are_staged(monkeypatch):
    tree = {".kaos/seal.json": 5, ".gitattributes": 2, "a/.kaos-like/b": 1, "part.kaos-partial": 4}
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS(tree), stager)
    assert _stage(cfg, stager)
    assert {r["path"] for r in _stage_call(stager)[1] if r["op"] == "fetch"} == set(tree)


def test_a_listing_that_cannot_be_read_does_not_judge_the_names(monkeypatch):
    stager = _Stager({"ready": True})
    cfg = _twin(monkeypatch, _LakeFS({}), stager)

    def down(req, timeout=0):
        raise IOError("lakeFS 502")
    monkeypatch.setattr(urllib.request, "urlopen", down)
    assert _stage(cfg, stager).endswith(COMMIT), "the copy on the site was checked when it was made"


# ------------------------------------------------ the staging contract of the site's staging tool

def test_the_runner_asks_the_staging_tool_for_its_contract_and_sends_its_own(monkeypatch):
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({"a/b.bin": 1}), stager)
    assert _stage(cfg, stager)
    calls = [w for w, _ in stager.calls]
    assert [w[0] for w in calls] == ["version", "status", "stage"]
    for w in calls[1:]:
        assert w[w.index("--contract") + 1] == staging.CONTRACT == 2


@pytest.mark.parametrize("version", [
    (2, None),   # a staging tool from before `version`: the forced command answers a usage error
    (0, 1),      # another contract
    (0, 3),
    (0, None),   # an answer without a contract
])
def test_a_staging_tool_of_another_contract_stops_the_run_before_anything_is_staged(monkeypatch, version):
    """Not a fallback: the old staging tool keeps only the YOLO folders and marks that copy ready. The run ends
    here, before a copy is made and before node time is spent in-job."""
    lakefs = _LakeFS(NOT_YOLO)
    stager = _Stager({"ready": True}, version=version)
    cfg = _twin(monkeypatch, lakefs, stager)
    with pytest.raises(staging.StagingMismatch) as e:
        _stage(cfg, stager)
    assert [w[0] for w, _ in stager.calls] == ["version"], "no status, no stage, no retry"
    assert _listings(lakefs) == []
    assert "Nothing was staged" in str(e.value) and "staging contract" in str(e.value)


class _Absent(_Stager):
    """The login node cannot run the staging tool: python exits 2 for a file it cannot open, and says so."""

    def __init__(self, status, until=None, rc=2):
        super().__init__(status)
        self.until, self.rc = until, rc
        self.last_error = "/usr/bin/python3: can't open file '/home/users/u1/.kaos/stager-c2.py': [Errno 2]"

    def __call__(self, words, feed=None, quiet=False):
        if words[0] == "version" and (self.until is None or len(self.calls) < self.until):
            self.calls.append((words, None))
            return self.rc, []
        self.last_error = ""
        return super().__call__(words, feed, quiet)


@pytest.mark.parametrize("rc", [2, 126, 127])
def test_a_staging_tool_that_is_not_installed_is_asked_for_a_while_then_the_run_stops_with_its_own_sentence(
        monkeypatch, rc):
    """Not "another contract": no update is coming. The credential refresh installs the tool every 15 minutes, so
    the runner asks for 20; then the run ends before anything is submitted. It is not a fallback to the in-job
    copy, which would cost node time on every job for as long as the install fails."""
    stager = _Absent({"ready": True}, rc=rc)
    cfg = _twin(monkeypatch, _LakeFS(NOT_YOLO), stager)
    clock = [1000.0]
    monkeypatch.setattr(staging.time, "time", lambda: clock[0])
    monkeypatch.setattr(staging.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    with pytest.raises(staging.StagingMismatch) as e:
        _stage(cfg, stager)
    msg = str(e.value)
    assert "is not installed for this project, or cannot be run" in msg and "exit %d" % rc in msg
    assert "credential refresh" in msg and "Nothing was staged and nothing was submitted" in msg
    assert "staging contract" not in msg and "update" not in msg, "no update is coming: do not say to wait for one"
    assert 20 * 60 < clock[0] - 1000.0 <= 30 * 60 and len(stager.calls) > 3
    assert _listings(_LakeFS({})) == []


def test_the_twenty_minutes_of_asking_for_a_missing_tool_count_from_its_first_absent_answer(monkeypatch):
    """The sentence says "for 20 minutes": they start when the login node first said the tool is absent, not when
    staging began (the lakeFS checks and the wallet before it can take minutes)."""
    class _Late(_Absent):
        first = None

        def __call__(self, words, feed=None, quiet=False):
            if self.first is None:
                clock[0] += 10 * 60                     # 10 minutes of staging before the first answer
                self.first = clock[0]
            return super().__call__(words, feed, quiet)

    stager = _Late({"ready": True})
    cfg = _twin(monkeypatch, _LakeFS(NOT_YOLO), stager)
    clock = [1000.0]
    monkeypatch.setattr(staging.time, "time", lambda: clock[0])
    monkeypatch.setattr(staging.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    with pytest.raises(staging.StagingMismatch):
        _stage(cfg, stager)
    assert 20 * 60 < clock[0] - stager.first <= 30 * 60


def test_a_staging_tool_installed_while_the_runner_asks_is_used(monkeypatch):
    stager = _Absent({"ready": True}, until=3)
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    assert _stage(cfg, stager).endswith(COMMIT)
    assert [w[0] for w, _ in stager.calls] == ["version"] * 4 + ["status"]


def test_a_staging_tool_replaced_between_two_calls_stops_the_run_too(monkeypatch):
    stager = _Stager({"ready": False, "base": None}, status_rc=78)
    cfg = _twin(monkeypatch, _LakeFS(NOT_YOLO), stager)
    with pytest.raises(staging.StagingMismatch) as e:
        _stage(cfg, stager)
    assert "3" in str(e.value) and [w[0] for w, _ in stager.calls] == ["version", "status"]
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[78, 0])
    cfg = _twin(monkeypatch, _LakeFS(NOT_YOLO), stager)
    with pytest.raises(staging.StagingMismatch):
        _stage(cfg, stager)
    assert sum(w[0] == "stage" for w, _ in stager.calls) == 1


def test_a_staging_tool_that_cannot_be_asked_is_retried_not_judged(monkeypatch):
    class _Flaky(_Stager):
        def __call__(self, words, feed=None, quiet=False):
            if words[0] == "version" and not self.calls:
                self.calls.append((words, None))
                return 255, []
            return super().__call__(words, feed, quiet)
    stager = _Flaky({"ready": True})
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    assert _stage(cfg, stager).endswith(COMMIT)
    assert [w[0] for w, _ in stager.calls] == ["version", "version", "status"]


def test_delta_lists_only_the_two_dot_diff_against_the_base(monkeypatch):
    lakefs = _LakeFS({"images/a.jpg": 1}, diff=[
        {"type": "added", "path": "images/new.jpg", "size_bytes": 5},
        {"type": "changed", "path": "images/a.jpg", "size_bytes": 7},
        {"type": "removed", "path": "images/gone.jpg", "size_bytes": 3}])
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": ""})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager)
    words, records = _stage_call(stager)
    assert words[words.index("--base") + 1] == BASE
    assert words[words.index("--need-files") + 1] == 2 and words[words.index("--need-bytes") + 1] == 12
    assert {"op": "delete", "path": "images/gone.jpg"} in records
    assert records[-1]["files"] == 10 + 1 - 1, "base count + added - removed"
    assert any("/refs/%s/diff/%s?" % (BASE, COMMIT) in u and "type=two_dot" in u for u in lakefs.urls)
    assert _listings(lakefs) == [], "a delta never lists the whole commit"


def test_a_base_with_another_layout_is_not_used(monkeypatch):
    lakefs = _LakeFS({"images/a.jpg": 1})
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": "dataset/"})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager)
    assert "--base" not in _stage_call(stager)[0]


def test_a_ready_copy_is_never_listed_again(monkeypatch):
    lakefs = _LakeFS({"images/a.jpg": 1})
    stager = _Stager({"ready": True})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager).endswith(COMMIT)
    assert _listings(lakefs) == []
    assert [w[0] for w, _ in stager.calls] == ["version", "status"]


def test_retry_resumes_and_no_space_stops_insufficient_quota(monkeypatch):
    lakefs = _LakeFS({"images/a.jpg": 1})
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[75, 0])
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager), "exit 75 (dropped session, busy lock) is retried"
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[76, 0])
    cfg = _twin(monkeypatch, lakefs, stager)
    with pytest.raises(staging.InsufficientQuota) as e:
        _stage(cfg, stager)
    assert "Nothing was copied" in str(e.value) and "dataset-poulaki@" in str(e.value), "AC-33: a stop, not a fallback"
    assert sum(w[0] == "stage" for w, _ in stager.calls) == 1, "no space is not retried"


def test_a_stager_killed_mid_copy_is_resumed_by_the_next_attempt_with_the_whole_plan(monkeypatch):
    """PRD-HPC-1461 AC-31a, runner side: the login-node stager dying mid-copy (ssh exit 255, no `done`) leaves the
    runner with no path, so the job is never pointed at a half version; the next attempt sends the SAME plan and
    the stager (which keeps what it fetched, see its test_stager.py) finishes it. Only then is the path returned."""
    lakefs = _LakeFS({"data.yaml": 4, "images/a.jpg": 10, "images/b.jpg": 20})
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[255, 0])
    cfg = _twin(monkeypatch, lakefs, stager)
    info = {}
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "wallet-jwt", info=info).endswith(COMMIT)
    stages = [(w, r) for w, r in stager.calls if w[0] == "stage"]
    assert len(stages) == 2, "the killed copy is retried"
    assert stages[0][1] == stages[1][1] and stages[0][1][-1] == {"op": "end", "files": 3, "bytes": 34, "ref": "main",
                                                                  "prefix": ""}, "the whole plan again, end record last"
    assert info["kind"] == "first" and info["files"] == 3, "recorded once the copy is whole, not for the killed try"


def test_a_stager_that_keeps_dying_never_hands_the_job_a_half_copy(monkeypatch):
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[255] * 40)
    cfg = _twin(monkeypatch, _LakeFS({"images/a.jpg": 1}), stager)
    clock = [1000.0]
    monkeypatch.setattr(staging.time, "time", lambda: clock[0])
    monkeypatch.setattr(staging.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    info = {}
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "wallet-jwt", info=info) == "" and info == {}, \
        "no path (the job stages in-job and says so) and no staging record"


def test_without_the_staging_key_the_job_stages_in_job(monkeypatch):
    stager = _Stager({"ready": True}, key=False)
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    assert _stage(cfg, stager) == "" and stager.calls == []


def test_without_a_wallet_the_job_stages_in_job(monkeypatch):
    stager = _Stager({"ready": True})
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    assert _stage(cfg, stager, wallet="") == "" and stager.calls == []


def test_a_failed_feed_ends_the_stream_without_an_end_record(monkeypatch, tmp_path):
    """The real Stager: a lakeFS error mid-listing closes stdin with no end record (the stager keeps
    resumable state) and surfaces the error."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    ssh = bindir / "ssh"
    ssh.write_text('#!/bin/sh\nn=$(wc -l)\necho "{\\"event\\": \\"got\\", \\"lines\\": $n}"\nexit 75\n')
    ssh.chmod(ssh.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ["PATH"])
    stager = staging.Stager(helpers.settings(), staging_dir=str(tmp_path))

    def feed():
        yield {"op": "auth", "authorization": "Bearer x"}
        yield {"op": "fetch", "path": "a", "size": 1, "url": "u"}
        raise IOError("lakeFS 502")
    try:
        stager(["stage", "--repo", "poulaki"], feed())
        raise AssertionError("the feed error must surface")
    except IOError:
        pass
    rc, events = stager(["status"], iter([{"op": "end"}]))
    assert rc == 75 and events == [{"event": "got", "lines": 1}]


def test_the_stager_is_reached_on_the_sites_login_node_as_the_project_user(monkeypatch, tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    ssh = bindir / "ssh"
    ssh.write_text('#!/bin/sh\nprintf "%%s\\n" "$@" > %s/args\n' % tmp_path)
    ssh.chmod(0o755)
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ["PATH"])
    cfg = helpers.settings(HPC_LOGIN_HOST="login.example.org:2222")
    staging.Stager(cfg, staging_dir=str(tmp_path))(["status", "--repo", "r"])
    args = (tmp_path / "args").read_text().split("\n")
    assert args[args.index("-p") + 1] == "2222" and "u1@login.example.org" in args
    assert "UserKnownHostsFile=%s/known_hosts" % tmp_path in args and args[-2] == "status --repo r"


def test_the_runner_never_resolves_a_version(monkeypatch):
    """The commit comes from the job (spec.inputs, pinned by the pipeline's compose step): staging asks
    lakeFS for that commit's objects only, never for what a version points at."""
    lakefs = _LakeFS({"images/a.jpg": 1})
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager, ref="v3")
    assert _stage(cfg, stager).endswith(COMMIT)
    assert lakefs.urls and not any("/commits" in u for u in lakefs.urls)
    assert all("/refs/%s/" % COMMIT in u for u in lakefs.urls)
    assert not hasattr(staging, "resolve_commit")


def _fake_ssh(tmp_path, master_rc=0):
    """A fake ssh: `-O check` succeeds once a master was started, `-N -f` starts one (or fails with
    master_rc), anything else is a stager call; every invocation is logged."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    ssh = bindir / "ssh"
    ssh.write_text('''#!/bin/sh
echo "$*" >> %(d)s/calls
case " $* " in
  *" -O check "*) [ -e %(d)s/master ] && exit 0 || exit 255 ;;
  *" -O exit "*) rm -f %(d)s/master; exit 0 ;;
  *" -N -f "*) [ %(rc)d = 0 ] && touch %(d)s/master; exit %(rc)d ;;
esac
echo '{"event": "status", "ready": true}'
''' % {"d": tmp_path, "rc": master_rc})
    ssh.chmod(0o755)
    return str(bindir)


def test_one_runner_logs_in_once_and_reuses_the_connection(monkeypatch, tmp_path):
    monkeypatch.setenv("PATH", _fake_ssh(tmp_path) + os.pathsep + os.environ["PATH"])
    stager = staging.Stager(helpers.settings(), staging_dir=str(tmp_path))
    for _ in range(3):
        rc, ev = stager(["status", "--repo", "r"])
        assert rc == 0 and ev == [{"event": "status", "ready": True}]
    calls = (tmp_path / "calls").read_text().splitlines()
    masters = [c for c in calls if " -N -f " in " %s " % c]
    stager_calls = [c for c in calls if c.endswith("status --repo r")]
    assert len(masters) == 1, calls
    assert "ControlMaster=yes" in masters[0] and "ControlPersist=300" in masters[0]
    assert len(stager_calls) == 3 and all("ControlMaster=no" in c and "ControlPath=" in c for c in stager_calls)
    stager.close()
    assert any(" -O exit " in " %s " % c for c in (tmp_path / "calls").read_text().splitlines())


def test_without_a_shared_connection_each_call_connects_directly(monkeypatch, tmp_path):
    monkeypatch.setenv("PATH", _fake_ssh(tmp_path, master_rc=255) + os.pathsep + os.environ["PATH"])
    stager = staging.Stager(helpers.settings(), staging_dir=str(tmp_path))
    for _ in range(2):
        rc, _ = stager(["status"])
        assert rc == 0
    calls = (tmp_path / "calls").read_text().splitlines()
    assert len([c for c in calls if " -N -f " in " %s " % c]) == 1, "the master is tried once per runner"
    assert all("ControlPath" not in c for c in calls if c.endswith(" status"))


def test_concurrent_calls_start_one_master(monkeypatch, tmp_path):
    monkeypatch.setenv("PATH", _fake_ssh(tmp_path) + os.pathsep + os.environ["PATH"])
    stager = staging.Stager(helpers.settings(), staging_dir=str(tmp_path))
    import threading as th
    ts = [th.Thread(target=stager, args=(["status"],)) for _ in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    calls = (tmp_path / "calls").read_text().splitlines()
    assert len([c for c in calls if " -N -f " in " %s " % c]) == 1, calls


# ---------------------------------------------------------------- the staging record (status.run.staged)

def _stage_info(cfg, stager):
    info = {}
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "wallet-jwt", info=info)
    return info


def test_a_first_copy_is_recorded_with_its_size_and_the_short_commit(monkeypatch):
    lakefs = _LakeFS({"data.yaml": 4, "images/a.jpg": 10, "README.md": 99})
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage_info(cfg, stager) == {"repo": "dataset-poulaki", "commit": COMMIT[:12], "kind": "first",
                                        "files": 3, "bytes": 113, "seconds": 0}


def test_a_delta_is_recorded_with_what_it_copied(monkeypatch):
    lakefs = _LakeFS({}, diff=[{"type": "added", "path": "images/new.jpg", "size_bytes": 7},
                               {"type": "removed", "path": "images/gone.jpg", "size_bytes": 5}])
    stager = _Stager({"ready": False, "base": BASE, "base_files": 10, "base_prefix": ""})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage_info(cfg, stager) == {"repo": "dataset-poulaki", "commit": COMMIT[:12], "kind": "delta",
                                        "files": 1, "bytes": 7, "seconds": 0}


def test_a_resident_copy_is_recorded_as_cached_and_copied_nothing(monkeypatch):
    stager = _Stager({"ready": True})
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    info = _stage_info(cfg, stager)
    assert info["kind"] == "cached" and (info["files"], info["bytes"], info["seconds"]) == (0, 0, 0)


class _LateStager(_Stager):
    """The stager's lock was held by another runner, which finished the same commit: `done` says cached."""

    def __call__(self, words, feed=None, quiet=False):
        if words[0] != "stage":
            return super().__call__(words, feed, quiet)
        list(feed)
        return 0, [{"event": "done", "cached": True, "path": "/scr/kaos/p/datasets/ds/" + COMMIT}]


def test_a_copy_another_runner_finished_while_this_one_waited_is_recorded_as_cached(monkeypatch):
    stager = _LateStager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({"images/a.jpg": 1}), stager)
    assert _stage_info(cfg, stager)["kind"] == "cached"


def test_the_seconds_cover_a_resumed_staging(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(staging.time, "time", lambda: clock[0])
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[75, 0])
    cfg = _twin(monkeypatch, _LakeFS({"images/a.jpg": 1}), stager)
    monkeypatch.setattr(staging.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    assert _stage_info(cfg, stager)["seconds"] == 60, "the wait before the resumed attempt is part of the staging"


def test_nothing_is_recorded_when_the_job_stages_in_job(monkeypatch):
    stager = _Stager({"ready": True}, key=False)
    cfg = _twin(monkeypatch, _LakeFS({}), stager)
    info = {}
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "w", info=info) == "" and info == {}


def test_the_record_holds_no_path_and_no_site_identity(monkeypatch):
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({"images/a.jpg": 1}), stager)
    text = json.dumps(_stage_info(cfg, stager))
    assert "/scr" not in text and "/" not in text and cfg.identity()["user"] not in text
