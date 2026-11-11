"""PRD-HPC-1541 F-03/F-06: the runner stages the pinned dataset commit from a login node before it submits, and
the job binds the resident copy. The stager itself (kubecore-operator images/meluxina-tokenmint/stager) is
tested in the operator repo; here it is a stand-in that records what the runner asks for and streams, against a
fake lakeFS."""
import io
import json
import os
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
    """Stand-in for the login-node stager: canned status, records stage calls."""

    def __init__(self, status, stage_rcs=(0,), key=True):
        self.status, self.stage_rcs, self.calls, self.key = status, list(stage_rcs), [], key

    def have_key(self):
        return self.key

    def __call__(self, words, feed=None, quiet=False):
        records = list(feed) if feed is not None else None
        self.calls.append((words, records))
        if words[0] == "status":
            return 0, [dict(self.status, event="status", path="/scr/kaos/p/datasets/ds/" + COMMIT)]
        rc = self.stage_rcs.pop(0)
        return rc, ([{"event": "done", "path": "/scr/kaos/p/datasets/ds/" + COMMIT}] if rc == 0 else [])


def _twin(monkeypatch, lakefs, stager, **env):
    over = {"DATASET_REPO": "dataset-poulaki", "DATASET_VERSION": "latest", "DATASET_COMMIT": COMMIT,
            "LAKEFS_EXTERNAL_URL": "https://lakefs.example/"}
    over.update(env)
    cfg = helpers.settings(**over)
    monkeypatch.setattr(urllib.request, "urlopen", lakefs)
    monkeypatch.setattr(staging, "ssh_present", lambda: True)
    monkeypatch.setattr(staging.time, "sleep", lambda s: None)
    return cfg


def _stage(cfg, stager, wallet="wallet-jwt"):
    return staging.stage_dataset(cfg, stager, lambda: wallet)


def _stage_call(stager):
    return next((w, r) for w, r in stager.calls if w[0] == "stage")



def test_first_copy_streams_every_dataset_object(monkeypatch):
    lakefs = _LakeFS({"data.yaml": 4, "images/a.jpg": 10, "README.md": 99})
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager)
    assert _stage(cfg, stager).endswith(COMMIT)
    words, records = _stage_call(stager)
    assert "--base" not in words
    assert words[words.index("--need-bytes") + 1] == 14 and words[words.index("--need-files") + 1] == 2
    assert records[0] == {"op": "auth", "authorization": "Bearer wallet-jwt"}
    fetched = {r["path"]: r for r in records if r["op"] == "fetch"}
    assert set(fetched) == {"data.yaml", "images/a.jpg"}, "only the dataset"
    assert fetched["images/a.jpg"]["url"] == (
        "https://lakefs.example/api/v1/repositories/dataset-poulaki/refs/%s/objects?path=images%%2Fa.jpg"
        % COMMIT)
    assert records[-1] == {"op": "end", "files": 2, "bytes": 14, "ref": "main", "prefix": ""}


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
    assert not any("/objects/ls?" in u for u in lakefs.urls), "a delta never lists the whole commit"


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
    assert lakefs.urls == [] and [w[0] for w, _ in stager.calls] == ["status"]


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
    """The commit comes from the job (spec.dataset, pinned by the pipeline's compose step): staging asks
    lakeFS for that commit's objects only, never for what a version points at."""
    lakefs = _LakeFS({"images/a.jpg": 1})
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, lakefs, stager, DATASET_VERSION="v3")
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
