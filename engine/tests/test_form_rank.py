"""PRD kaos#1533 F-21 H2 and H3: `form_rank` (the grouping) and the trusted form data `enhance` exports.

The seal (charts files/run_form.py) reads the role of every Argo parameter from `rank`, the class offers from
`offers`/`pinned`, and the raw render's parameter names from `rendered`.
"""
import copy
import json
import runpy

import yaml

from kubecore import enhance
from kubecore.step_contract import RAW_FORM_ANNOTATION
from paths import APP, ENGINE

CTX = yaml.safe_load((ENGINE / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())

# order_form's output on tests/app before the form_rank refactor (a pure refactor: this must not move).
APP_ORDER = ["pipeline-info", "experiment-name", "experiment-description", "dataset", "dataset-loading-class",
             "dataset-loading-walltime", "hello-world-class", "hello-world-walltime", "reuse-from-run",
             "start-from-step", "promote-loaded", "config"]
LEAVES = ["learning-rate", "optimizer", "batch-size"]    # what derive_tree adds: group dropdown and its leaves


def _raw(extra=()):
    ns = runpy.run_path(str(APP / "pipeline.py"), run_name="__pipeline__")
    wft = yaml.safe_load(ns["p"].wt.to_yaml())
    params = wft["spec"]["arguments"]["parameters"]
    at = [p["name"] for p in params].index("dataset")
    for i, name in enumerate(extra):
        params.insert(at + i, {"name": name, "value": "x"})    # config leaves come after the experiment fields
    return wft


def _enhanced(wft=None, form_data=None):
    wft = _raw() if wft is None else wft
    return enhance.enhance(wft, copy.deepcopy(CTX)) if form_data is None else \
        enhance.enhance(wft, copy.deepcopy(CTX), form_data)


def _steps_of(wft):
    return enhance.step_templates(wft["spec"])


def _names(wft):
    return [p["name"] for p in wft["spec"]["arguments"]["parameters"]]


def test_order_form_is_unchanged_on_the_app():
    assert _names(_enhanced()) == APP_ORDER


def test_rank_covers_every_parameter_and_sort_is_the_order_form_key():
    wft = _enhanced()
    rank = enhance.form_rank(wft["spec"], _steps_of(wft))
    assert sorted(rank) == sorted(_names(wft))
    assert all(rank[n]["role"] in enhance.ROLES for n in rank)
    keys = [rank[n]["sort"] for n in _names(wft)]
    assert keys == sorted(keys)
    assert rank["pipeline-info"] == {"role": "info", "step": None, "sort": (0, 0)}
    assert rank["hello-world-class"]["role"] == "class" and rank["hello-world-class"]["step"] == "hello-world"
    assert rank["hello-world-walltime"]["role"] == "walltime"
    assert rank["dataset-loading-walltime"]["step"] == "dataset-loading"
    assert rank["reuse-from-run"]["role"] == "reuse" and rank["start-from-step"]["role"] == "start"
    assert rank["promote-loaded"]["role"] == "promote"
    assert rank["promote-loaded"]["step"] is not None       # the producer
    assert rank["config"]["role"] == "advanced"
    assert rank["dataset"]["role"] == "input"
    assert rank["experiment-name"]["role"] == "experiment"


def test_without_rendered_everything_unclassified_sorts_as_today_and_is_platform():
    wft = _enhanced()
    wft["spec"]["arguments"]["parameters"].append({"name": "learning-rate", "value": "1"})
    rank = enhance.form_rank(wft["spec"], _steps_of(wft))
    assert rank["learning-rate"] == {"role": "platform", "step": None, "sort": (2, 0)}


def test_config_leaves_and_groups_in_the_raw_render_are_settings():
    raw = _raw(LEAVES)
    rendered = {p["name"] for p in raw["spec"]["arguments"]["parameters"]}
    wft = _enhanced(raw)
    rank = enhance.form_rank(wft["spec"], _steps_of(wft), rendered)
    for name in LEAVES:
        assert rank[name] == {"role": "setting", "step": None, "sort": (2, 0)}
    assert _names(wft)[:1] == ["pipeline-info"]
    assert all(rank[n]["role"] != "setting" for n in _names(wft) if n not in LEAVES)


def test_no_engine_name_is_a_setting_even_when_the_render_declares_it():
    wft = _enhanced()
    every = set(_names(wft))
    rank = enhance.form_rank(wft["spec"], _steps_of(wft), every)
    assert {n for n, r in rank.items() if r["role"] in ("setting", "platform")} == set()


def test_a_name_in_neither_set_is_platform():
    wft = _enhanced()
    wft["spec"]["arguments"]["parameters"].append({"name": "added-by-hand", "value": "1"})
    rank = enhance.form_rank(wft["spec"], _steps_of(wft), {"something-else"})
    assert rank["added-by-hand"]["role"] == "platform" and rank["added-by-hand"]["sort"] == (2, 0)


def test_order_form_passes_no_rendered_and_orders_by_sort():
    wft = _enhanced()
    params = wft["spec"]["arguments"]["parameters"]
    params.append({"name": "zz-extra", "value": "1"})
    params.reverse()
    enhance.order_form(wft["spec"], _steps_of(wft))
    assert _names(wft)[0] == "pipeline-info" and _names(wft)[-1] == "config"
    assert _names(wft).index("zz-extra") < _names(wft).index("dataset")      # (2, 0) is before inputs (3)


def test_form_data_is_filled_only_when_asked_and_the_wft_is_identical():
    plain = _enhanced()
    data: dict = {}
    with_data = _enhanced(form_data=data)
    assert yaml.safe_dump(plain) == yaml.safe_dump(with_data)
    assert data["version"] == 1
    assert set(data) == {"version", "offers", "pinned", "rendered", "rank", "raw"}


def test_form_data_rendered_is_the_raw_names_in_order_and_rank_matches_the_output():
    raw = _raw(LEAVES)
    names = [p["name"] for p in raw["spec"]["arguments"]["parameters"]]
    data: dict = {}
    wft = _enhanced(raw, data)
    assert data["rendered"] == names
    expected = enhance.form_rank(wft["spec"], _steps_of(wft), set(names))
    assert json.loads(json.dumps(data["rank"])) == json.loads(json.dumps(expected))
    assert data["rank"]["learning-rate"]["role"] == "setting"
    json.dumps(data)    # plain JSON types only


def test_form_data_raw_is_the_stripped_raw_form_text():
    raw = _raw()
    raw["metadata"]["annotations"] = {RAW_FORM_ANNOTATION: '{"version": 1}'}
    data: dict = {}
    _enhanced(raw, data)
    assert data["raw"] == '{"version": 1}'
    none: dict = {}
    _enhanced(_raw(), none)
    assert none["raw"] is None


def test_form_data_offers_equal_class_offer_before_gpu_routing_and_pinned_names_the_class():
    raw = _raw()
    probe = copy.deepcopy(raw)
    enhance.strip_render_metadata(probe)
    enhance.enhance_metadata(probe, CTX)
    expected = {}
    for step in enhance.step_templates(probe["spec"]):
        annots = enhance.platform_annotations(step)
        expected[step["name"]] = enhance.class_offer(step, CTX, annots) if "compute-class" not in annots else None
    data: dict = {}
    _enhanced(raw, data)
    assert data["offers"] == expected and expected
    assert data["pinned"] == {}


def test_a_pinned_step_has_offer_none_and_its_class_in_pinned():
    raw = _raw()
    step = enhance.step_templates(raw["spec"])[-1]
    step.setdefault("metadata", {}).setdefault("annotations", {})["platform.kubecore.io/compute-class"] = \
        CTX["computeClasses"]["cpu"]["name"]
    data: dict = {}
    _enhanced(raw, data)
    assert data["offers"][step["name"]] is None
    assert data["pinned"] == {step["name"]: CTX["computeClasses"]["cpu"]["name"]}


def test_main_writes_form_data_only_with_the_flag(tmp_path, monkeypatch):
    raw_path = tmp_path / "raw.yaml"
    raw_path.write_text(yaml.safe_dump(_raw()))
    ctx_path = tmp_path / "ctx.yaml"
    ctx_path.write_text(yaml.safe_dump(CTX))
    base = ["--raw", str(raw_path), "--context", str(ctx_path)]
    enhance.main(base + ["--output", str(tmp_path / "a.yaml")])
    assert not (tmp_path / "form-data.json").exists()
    enhance.main(base + ["--output", str(tmp_path / "b.yaml"), "--form-data", str(tmp_path / "form-data.json")])
    text = (tmp_path / "form-data.json").read_text()
    assert text == json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))    # compact
    assert json.loads(text)["version"] == 1
    assert (tmp_path / "a.yaml").read_text() == (tmp_path / "b.yaml").read_text()
