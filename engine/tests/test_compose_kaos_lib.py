"""Compose against the real kaos-lib 0.5 interface (PRD kaos#1911): the real `DatasetRef.parse`, `datasets.source`
and `datasets.resolve`, only the HTTP client is a stand-in. The other tests give compose a fake `datasets` object;
this one shows it speaks to the library as the library is written.

kaos-lib is not one of the engine's own libraries, so the test runs when it is importable:
    make test-engine ENGINE_KAOS_LIB=/path/to/kaos-lib      (a checkout of branch step-api, or 0.5 and later)
"""

import sys

import pytest

pytest.importorskip("kaos_lib", reason="set ENGINE_KAOS_LIB to a kaos-lib 0.5 checkout to run this")

from kaos_lib.errors import NotFoundError  # noqa: E402
from kaos_lib.lakefs import LakeFS  # noqa: E402

from paths import ENGINE  # noqa: E402

sys.path.insert(0, str(ENGINE))
from kubecore import compose  # noqa: E402

HEAD, V1, SOME = "a1" * 32, "b2" * 32, "c3" * 32


class Lake:
    """The calls kaos_lib.datasets makes of a lakeFS client, over two repositories."""
    owner = None
    timeout = 0

    def __init__(self):
        self.repos = {"dataset-a": {"branches": {"main": HEAD}, "tags": {"v1": V1}, "commits": {SOME}},
                      "dataset-b": {"branches": {"main": SOME}, "tags": {}, "commits": {SOME}}}

    def repository(self, repo):
        return {"default_branch": "main"} if repo in self.repos else None

    def tags(self, repo):
        return [{"id": tag} for tag in self.repos[repo]["tags"]]

    def branch_commit(self, repo, name):
        try:
            return self.repos[repo]["branches"][name]
        except KeyError:
            raise NotFoundError(f"no branch {name}", 404) from None

    def tag_commit(self, repo, name):
        try:
            return self.repos[repo]["tags"][name]
        except KeyError:
            raise NotFoundError(f"no tag {name}", 404) from None

    def commit_id(self, repo, name):
        if name in self.repos[repo]["commits"]:
            return name
        raise NotFoundError(f"no commit {name}", 404)


@pytest.fixture(autouse=True)
def lake(monkeypatch):
    monkeypatch.setattr(LakeFS, "connect", classmethod(lambda cls, *a, **k: Lake()))


def table(*inputs):
    return [{"name": "train", "kind": "train", "reads": [], "inputs": list(inputs), "needs": [], "produces": []}]


def test_the_real_library_pins_latest_a_tag_and_a_commit():
    reads = compose.pin_inputs({"raw": "dataset-a@latest", "labels": "dataset-a@v1", "frozen": f"dataset-b@{SOME}",
                                "bare": "dataset-b"}, table("raw", "labels", "frozen", "bare"))
    assert {k: (r["repo"], r["ref"], r["commit"], r["owner"]) for k, r in reads.items()} == {
        "raw": ("dataset-a", "latest", HEAD, None), "labels": ("dataset-a", "v1", V1, None),
        "frozen": ("dataset-b", SOME, SOME, None), "bare": ("dataset-b", "latest", SOME, None)}
    assert [r["name"] for r in reads.values()] == ["raw", "labels", "frozen", "bare"]


@pytest.mark.parametrize("value, why", [
    ("dataset-zzz@latest", "repository dataset-zzz does not exist"),
    ("dataset-a@v9", "dataset-a has no"),
    ("dataset-a@nope", "dataset-a has no branch, tag or commit 'nope'"),
    ("dataset-a@@x", "more than one '@'"),
])
def test_what_the_real_library_refuses_ends_the_run_naming_the_input(value, why):
    with pytest.raises(compose.ComposeError) as exc:
        compose.pin_inputs({"raw": value}, table("raw"))
    assert str(exc.value).startswith(f"input 'raw' (step train): cannot use {value!r}: ")
    assert why in str(exc.value)


def test_compose_gives_the_connection_a_short_timeout():
    _, client = compose._kaos_lib()
    assert client.timeout == compose.LAKEFS_TIMEOUT_S
