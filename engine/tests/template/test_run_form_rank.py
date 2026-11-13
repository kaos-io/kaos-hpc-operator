"""form_rank (PRD 1533/F-21 and 1533/F-22): sorting by it gives the order enhance.order_form gives today, so the swap
(T6b) changes nothing a developer sees.
Run:  python -m pytest tests/ -v
"""
import copy
import json
import runpy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance, run_form  # noqa: E402
from kubecore.run_form import GROUPS, form_rank, rank_key  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
APP = ROOT / "tests" / "app"


def _ctx():
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    return ctx


def _small():
    with authoring.pipeline("t") as p:
        load = authoring.step("load", kind="ingest", inputs=["dataset"], produces=[authoring.dataset("loaded")])
        train = authoring.step("train", kind="train", needs=[load], gpu=True, walltime="12h", checkpoint_every="10m",
                               produces=[authoring.model("m")])
        authoring.step("report", kind="evaluate", needs=[train])
    return yaml.safe_load(p.wt.to_yaml())


def _app():
    return yaml.safe_load(runpy.run_path(str(APP / "pipeline.py"), run_name="__pipeline__")["p"].wt.to_yaml())


def _both(raw, monkeypatch):
    """(parameters as enhance leaves them before ordering, steps, parameters ordered by order_form)."""
    with monkeypatch.context() as m:
        m.setattr(enhance, "order_form", lambda spec, steps: None)
        spec = enhance.enhance(copy.deepcopy(raw), _ctx())["spec"]
    unordered = spec["arguments"]["parameters"]
    steps = enhance.step_templates(spec)
    ordered = copy.deepcopy(spec)
    enhance.order_form(ordered, steps)
    return unordered, steps, ordered["arguments"]["parameters"]


def _table(steps):
    compose = next(s for s in steps if s["name"] == "compose-and-validate")
    args = compose["container"]["args"]
    return json.loads(args[args.index("--steps") + 1])


@pytest.mark.parametrize("make", [_small, _app], ids=["test_run_form._form", "tests/app"])
def test_sorting_by_form_rank_gives_the_order_form_order(make, monkeypatch):
    unordered, steps, ordered = _both(make(), monkeypatch)
    assert [p["name"] for p in unordered] != [p["name"] for p in ordered] or len(unordered) < 3
    rank = form_rank(unordered, _table(steps), None)
    assert set(rank) == {p["name"] for p in unordered}
    by_rank = sorted(unordered, key=lambda p: rank_key(rank[p["name"]]))
    assert [p["name"] for p in by_rank] == [p["name"] for p in ordered]
    assert by_rank == ordered, "the parameters themselves are untouched"


def test_form_rank_does_not_change_the_parameters(monkeypatch):
    unordered, steps, _ = _both(_small(), monkeypatch)
    before = copy.deepcopy(unordered)
    form_rank(unordered, _table(steps), {"fields": {"dataset": {"group": "g"}}})
    assert unordered == before


# ------------------------------------------------------------------------------------------ new cases

TABLE = [{"name": "load", "inputs": ["dataset"]}, {"name": "train", "inputs": []}]
PARAMS = [{"name": n} for n in (
    "image-load", "config", "promote-x", "train-checkpoint-every", "train-class", "load-walltime", "load-class",
    "dataset", "lr", "experiment-name", "pipeline-info", "batch", "zzz-class")]


def test_each_parameter_lands_in_its_group_and_a_step_field_in_step_order():
    rank = form_rank(PARAMS, TABLE, None)
    assert rank == {
        "pipeline-info": ("info", 0), "experiment-name": ("experiment", 0), "lr": ("settings", 0),
        "batch": ("settings", 0), "dataset": ("datasets", 0), "load-class": ("steps", 0),
        "load-walltime": ("steps", 1), "train-class": ("steps", 3), "train-checkpoint-every": ("steps", 5),
        "promote-x": ("promote", 0), "config": ("advanced", 0), "image-load": ("platform", 0),
        "zzz-class": ("settings", 0)}


def test_the_groups_are_the_fixed_table():
    assert [g for g, _ in GROUPS] == ["info", "experiment", "settings", "datasets", "steps", "promote",
                                      "advanced", "platform"]
    assert dict(GROUPS) == {"info": "hidden", "experiment": "basic", "settings": "basic", "datasets": "basic",
                            "steps": "basic", "promote": "advanced", "advanced": "advanced", "platform": "hidden"}
    assert run_form.FIXED_GROUP_IDS == {g for g, _ in GROUPS}


def test_a_form_yaml_group_sorts_inside_settings_by_order():
    overrides = {"fields": {"lr": {"group": "tuning"}, "batch": {"group": "data-loading"}},
                 "groups": [{"id": "tuning", "order": 2}, {"id": "data-loading", "order": 1}]}
    params = [{"name": n} for n in ("image-load", "dataset", "lr", "plain", "batch", "pipeline-info", "train-class")]
    rank = form_rank(params, TABLE, overrides)
    assert rank["lr"] == ("tuning", 0) and rank["batch"] == ("data-loading", 0) and rank["plain"] == ("settings", 0)
    order = sorted((p["name"] for p in params), key=lambda n: rank_key(rank[n], overrides))
    assert order == ["pipeline-info", "plain", "batch", "lr", "dataset", "train-class", "image-load"]


def test_a_field_can_move_to_a_fixed_group_and_a_stale_group_sorts_last():
    rank = form_rank([{"name": "lr"}, {"name": "x"}], TABLE, {"fields": {"lr": {"group": "advanced"},
                                                                          "x": {"group": "gone"}}})
    assert rank["lr"] == ("advanced", 0) and rank["x"] == ("gone", 0)
    assert rank_key(rank["x"]) > rank_key(("platform", 0)) and rank_key(rank["lr"]) < rank_key(("platform", 0))


def test_an_empty_form_and_no_steps():
    assert form_rank([], [], None) == {}
    assert form_rank([{"name": "a"}], [], {}) == {"a": ("settings", 0)}
