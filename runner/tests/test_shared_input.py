"""PRD kaos#1911 AC-44, D-61: a step on an HPC class reads a dataset another project shares. The runner stages it
from the OWNER's data store into the reading project's own folder, with the bearer of the reading project and
nothing else, and never lets the job fall back to a download of its own for it."""
import io
import json
import urllib.error
import urllib.request

import helpers
import pytest
from kaos_hpc_runner import staging

COMMIT = "d3" * 32
OWN = "https://lakefs.example"
OWNER_URL = "https://owner.lakefs.example"
PEERS = {"project": "mine", "peers": [
    {"project": "owner", "endpoint": "http://lakefs.owner.svc:8000", "url": OWNER_URL},
    {"project": "nourl", "endpoint": "http://lakefs.nourl.svc:8000", "url": ""}]}


class _Net:
    """urlopen stand-in for two data stores: the own one answers the peers, the owner's answers the listing. `code`
    is what the owner's data store answers for the dataset (200, or a refusal)."""

    def __init__(self, code=200, objects=None, peers=PEERS, own_code=200):
        self.code, self.own_code, self.peers = code, own_code, peers
        self.objects = {"a.csv": 7} if objects is None else objects
        self.seen = []                    # (url, Authorization header)

    def __call__(self, req, timeout=0):
        url = req.full_url
        self.seen.append((url, req.get_header("Authorization")))
        if url.startswith(OWN + "/kaos/v1/peers"):
            if self.own_code != 200:
                raise urllib.error.HTTPError(url, self.own_code, "no", {}, io.BytesIO(b"{}"))
            return io.BytesIO(json.dumps(self.peers).encode())
        assert url.startswith(OWNER_URL + "/api/v1/repositories/dataset-shared/refs/" + COMMIT + "/objects/ls?"), url
        if self.code != 200:
            raise urllib.error.HTTPError(url, self.code, "no", {}, io.BytesIO(b"{}"))
        res = [{"path": p, "path_type": "object", "size_bytes": n} for p, n in self.objects.items()]
        return io.BytesIO(json.dumps({"results": res, "pagination": {"has_more": False}}).encode())


class _Stager:
    def __init__(self, key=True, ready=False, stage_rcs=(0,)):
        self.key, self.ready, self.stage_rcs, self.calls = key, ready, list(stage_rcs), []
        self.last_error = ""

    def have_key(self):
        return self.key

    def __call__(self, words, feed=None, quiet=False):
        records = list(feed) if feed is not None else None
        self.calls.append((words, records))
        if words[0] == "version":
            return 0, [{"event": "version", "contract": staging.CONTRACT}]
        path = "/scr/kaos/p/datasets/%s/%s" % (words[words.index("--repo") + 1], COMMIT)
        if words[0] == "status":
            return 0, [{"event": "status", "ready": self.ready, "base": None, "path": path}]
        rc = self.stage_rcs.pop(0)
        return rc, ([{"event": "done", "path": path}] if rc == 0 else [])


def _cfg(monkeypatch, net, owner="owner"):
    entry = {"name": "shared", "repo": "dataset-shared", "ref": "latest", "commit": COMMIT}
    if owner:
        entry["owner"] = owner
    cfg = helpers.settings(HPC_INPUTS=json.dumps([entry]), LAKEFS_EXTERNAL_URL=OWN)
    cfg.check_job()
    monkeypatch.setattr(urllib.request, "urlopen", net)
    monkeypatch.setattr(staging, "ssh_present", lambda: True)
    monkeypatch.setattr(staging.time, "sleep", lambda s: None)
    return cfg


def _stage(cfg, stager, info=None):
    return staging.stage_dataset(cfg, cfg.inputs[0], stager, lambda: "wallet-jwt", info=info)


def test_a_shared_dataset_is_staged_from_the_owners_data_store_with_the_projects_bearer(monkeypatch):
    net, stager, info = _Net(), _Stager(), {}
    cfg = _cfg(monkeypatch, net)
    assert cfg.inputs[0]["owner"] == "owner"
    assert _stage(cfg, stager, info).endswith("/dataset-shared@owner/" + COMMIT)
    words, records = next(c for c in stager.calls if c[0][0] == "stage")
    # the copy is filed under the owner too: two projects can each have a repository of this name
    assert words[words.index("--repo") + 1] == "dataset-shared@owner"
    assert records[0] == {"op": "auth", "authorization": "Bearer wallet-jwt"}
    fetch = [r for r in records if r["op"] == "fetch"]
    assert [r["url"] for r in fetch] == [OWNER_URL + "/api/v1/repositories/dataset-shared/refs/%s/objects?path=a.csv" % COMMIT]
    assert all(u.startswith((OWN + "/kaos/v1/peers", OWNER_URL)) for u, _ in net.seen), "the own address for peers; the "\
        "owner's PUBLIC address (a login node is outside the platform network) for the data"
    assert all(a == "Bearer wallet-jwt" for _, a in net.seen), "one credential: the project's bearer, no key, no admin token"
    assert (info["repo"], info["commit"], info["kind"]) == ("dataset-shared@owner", COMMIT[:12], "first")


def test_an_own_dataset_keeps_its_own_key_and_never_asks_for_peers(monkeypatch):
    seen = []

    def own(req, timeout=0):
        seen.append(req.full_url)
        return io.BytesIO(json.dumps({"results": [], "pagination": {"has_more": False}}).encode())
    cfg = _cfg(monkeypatch, own, owner=None)
    stager = _Stager()
    _stage(cfg, stager)
    assert next(c for c in stager.calls if c[0][0] == "stage")[0][4] == "dataset-shared"
    assert not [u for u in seen if "/kaos/v1/peers" in u]


@pytest.mark.parametrize("code", [401, 403, 404])
def test_a_share_that_is_not_approved_or_was_withdrawn_stops_the_run_naming_the_dataset_and_the_owner(monkeypatch, code):
    stager = _Stager(ready=True)               # even a copy already on the site is not served without the share
    cfg = _cfg(monkeypatch, _Net(code=code))
    with pytest.raises(staging.SharedDatasetUnavailable) as e:
        _stage(cfg, stager)
    msg = str(e.value)
    assert "dataset-shared" in msg and "owner" in msg and "Ask project owner to share" in msg
    assert "Nothing was staged and nothing was submitted" in msg
    assert not [c for c in stager.calls if c[0][0] in ("status", "stage")], "the site is not asked"


def test_an_owner_the_own_data_store_does_not_know_stops_the_run(monkeypatch):
    cfg = _cfg(monkeypatch, _Net(peers={"project": "mine", "peers": []}))
    with pytest.raises(staging.SharedDatasetUnavailable) as e:
        _stage(cfg, _Stager())
    assert "owner" in str(e.value) and "not a project of this organisation" in str(e.value)


def test_an_owner_without_a_public_address_cannot_be_read_from_a_site(monkeypatch):
    entry = {"name": "shared", "repo": "dataset-shared", "ref": "latest", "commit": COMMIT, "owner": "nourl"}
    cfg = _cfg(monkeypatch, _Net())
    cfg.inputs = [entry]
    with pytest.raises(staging.SharedDatasetUnavailable) as e:
        _stage(cfg, _Stager())
    assert "no public https address" in str(e.value)


def test_an_owner_with_a_plain_http_address_never_receives_the_token(monkeypatch):
    peers = {"project": "mine", "peers": [
        {"project": "owner", "endpoint": "http://lakefs.owner.svc:8000", "url": "http://owner.lakefs.example"}]}
    net = _Net(peers=peers)
    cfg = _cfg(monkeypatch, net)
    with pytest.raises(staging.SharedDatasetUnavailable) as e:
        _stage(cfg, _Stager())
    assert "no public https address" in str(e.value)
    assert [u for u, _ in net.seen] == [OWN + "/kaos/v1/peers"]


def test_a_shared_dataset_is_never_downloaded_by_the_job_itself(monkeypatch):
    """No staging key: an own dataset falls back to the job's download; a shared one must not (the job reads only
    its own project's data store), so the run stops with a sentence before it is submitted."""
    cfg = _cfg(monkeypatch, _Net())
    with pytest.raises(staging.SharedDatasetUnavailable) as e:
        _stage(cfg, _Stager(key=False))
    assert "the job does not download it itself" in str(e.value) and "dataset-shared" in str(e.value)


def test_a_shared_dataset_the_site_cannot_stage_in_the_budget_stops_the_run(monkeypatch):
    cfg = _cfg(monkeypatch, _Net())
    clock = iter([0, 0, 0] + [staging.STAGE_MIN_BUDGET_S + 1] * 50)
    monkeypatch.setattr(staging.time, "time", lambda: next(clock))
    with pytest.raises(staging.SharedDatasetUnavailable) as e:
        _stage(cfg, _Stager(stage_rcs=[75] * 50))
    assert "the job does not download it itself" in str(e.value)


def test_the_same_repository_of_two_owners_is_two_copies(monkeypatch):
    keys = {staging.dataset_key("dataset-x", o) for o in ("a", "b", "")}
    assert keys == {"dataset-x@a", "dataset-x@b", "dataset-x"}


def test_the_job_is_told_the_owner_of_each_input():
    from kaos_hpc_runner import jobscript
    text = "\n".join(jobscript.batch_lines("-", "/scr"))
    assert 'read -r IN_NAME IN_REPO IN_COMMIT IN_STAGED IN_OWNER' in text
    assert '"owner":%s' in text or '"owner":' in text, "the record beside the input names the owner for kaos-lib"
    assert '[ -z "$IN_OWNER" ]' in text, "the in-job download is only for an own dataset"
