"""The run waits for the data store to let the app write the datasets its steps produce (kubecore/compose.py
wait_for_grants): the store creates the repository first and marks it `kaos.grant=<app>@<time>` once the app can write."""

import pytest

from kubecore import compose

APP = "hera1"
STEPS = [{"name": "prepare", "kind": "ingest", "reads": [], "inputs": [], "needs": [],
          "produces": [{"name": "clean", "type": "dataset"}, {"name": "model", "type": "model"}]},
         {"name": "features", "kind": "transform", "reads": [], "inputs": [], "needs": [],
          "produces": [{"name": "feats", "type": "dataset"}]}]
NO_DATASET = [{"name": "evaluate", "kind": "evaluate", "reads": [], "inputs": [], "needs": [], "produces": []}]


class Store:
    """A data store: repository metadata that changes as time passes (a grant appears after `after` seconds)."""

    def __init__(self, repos, grants=None):
        self.now = 0.0
        self.repos = {name: dict(md) for name, md in repos.items()}
        self.grants = grants or {}        # repo -> (seconds, label)
        self.calls = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def metadata(self, repo):
        self.calls.append(("metadata", repo))
        if repo not in self.repos:
            raise KeyError(f"{repo} does not exist")
        md = dict(self.repos[repo])
        seconds, label = self.grants.get(repo, (None, None))
        if seconds is not None and self.now >= seconds:
            md["kaos.grant"] = label
        return md

    def repositories(self):
        self.calls.append(("repositories",))
        return [{"id": name} for name in self.repos]


def wait(store, steps=STEPS):
    lines = []
    compose.wait_for_grants(steps, APP, client=store, clock=store.clock, sleep=store.sleep, say=lines.append)
    return lines


def test_a_label_that_appears_after_a_while_ends_the_wait_and_the_line_is_said_once_for_each_dataset():
    store = Store({f"dataset-{APP}-clean": {"kaos.producer": APP}, f"dataset-{APP}-feats": {"kaos.producer": APP}},
                  {f"dataset-{APP}-clean": (9, f"{APP}@2026-11-13T01:00:00Z"),
                   f"dataset-{APP}-feats": (30, f"{APP}@2026-11-13T01:00:20Z")})
    lines = wait(store)
    assert lines == ["waiting for dataset clean to become writable (it was declared a moment ago)",
                     "waiting for dataset feats to become writable (it was declared a moment ago)"]
    assert 30 <= store.now < 40, "polled every few seconds until the last label was there"


def test_a_dataset_that_is_ready_at_once_says_nothing_and_waits_for_nothing():
    store = Store({f"dataset-{APP}-clean": {"kaos.grant": f"{APP}@2026-11-13T00:00:00Z"},
                   f"dataset-{APP}-feats": {"kaos.grant": f"{APP}@2026-11-13T00:00:00Z"}})
    assert wait(store) == [] and store.now == 0


def test_a_grant_that_never_comes_fails_naming_the_dataset_and_what_to_do_when_other_datasets_are_marked():
    store = Store({f"dataset-{APP}-clean": {}, f"dataset-{APP}-feats": {"kaos.grant": f"{APP}@2026-11-13T00:00:00Z"},
                   "dataset-other-thing": {"kaos.grant": "other@2026-11-13T00:00:00Z"}})
    with pytest.raises(compose.ComposeError) as exc:
        wait(store)
    assert str(exc.value) == ("the dataset clean is not writable yet: the data store has not given app 'hera1' the right to "
                              "write it after 3 minutes. Run again in a minute. If it keeps happening, tell the platform team "
                              "the name of the dataset.")
    assert store.now >= compose.GRANT_WAIT_S


def test_a_label_of_another_app_is_not_a_grant_and_the_failure_says_so():
    store = Store({f"dataset-{APP}-clean": {"kaos.grant": "someone-else@2026-11-13T00:00:00Z"},
                   f"dataset-{APP}-feats": {"kaos.grant": f"{APP}@2026-11-13T00:00:00Z"}})
    with pytest.raises(compose.ComposeError) as exc:
        wait(store)
    assert "clean: it is marked for another app (someone-else)" in str(exc.value)


def test_a_label_that_only_starts_like_the_app_name_is_not_this_apps():
    store = Store({f"dataset-{APP}-clean": {"kaos.grant": f"{APP}2@2026-11-13T00:00:00Z"},
                   f"dataset-{APP}-feats": {"kaos.grant": f"{APP}@2026-11-13T00:00:00Z"}})
    with pytest.raises(compose.ComposeError):
        wait(store)


def test_a_store_that_sets_no_label_at_all_is_told_by_that_and_the_run_starts_after_the_wait_with_a_line():
    store = Store({f"dataset-{APP}-clean": {"kaos.producer": APP}, f"dataset-{APP}-feats": {"kaos.producer": APP},
                   "dataset-other-thing": {"kaos.producer": "other"}})
    lines = wait(store)
    assert store.now >= compose.GRANT_WAIT_S, "the wait is the same: a new dataset on a new store also has no label yet"
    assert lines[-1] == ("the data store marks no dataset of this project as writable (no repository carries kaos.grant), "
                         "so it is an older store: starting without waiting for clean, feats")


def test_a_repository_that_cannot_be_read_is_never_started_on_a_guess():
    store = Store({})
    with pytest.raises(compose.ComposeError) as exc:
        wait(store, STEPS[:1])
    assert "clean: KeyError" in str(exc.value)


def test_a_run_that_produces_no_dataset_makes_no_call_at_all():
    store = Store({})
    assert wait(store, NO_DATASET) == [] and store.calls == []
    assert wait(store, [{"name": "t", "produces": [{"name": "m", "type": "model"}, {"name": "r", "type": "artifact"}]}]) == []
    assert store.calls == []


def test_the_entry_point_makes_no_store_call_for_a_run_without_a_produced_dataset(monkeypatch):
    def refused():
        raise AssertionError("the data store was asked")
    monkeypatch.setattr(compose, "_kaos_lib", refused)
    compose.wait_for_grants(NO_DATASET, APP)
