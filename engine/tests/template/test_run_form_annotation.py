"""The run form annotation (PRD 1533/F-21 and 1533/F-22): fields, groups, check_form and attach, checked against
the schema (docs/run-form.schema.json) and against OSR's rules for serving it.
Run:  python -m pytest tests/ -v
"""
import copy
import json
import re
import runpy
import subprocess
import sys
from pathlib import Path

import jsonschema
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance, run_form  # noqa: E402
from kubecore.run_form import RunFormError  # noqa: E402
from kubecore.step_contract import RUN_FORM_ANNOTATION, RUN_FORM_CAP, STAMP_BY  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
SCHEMA = json.loads((ROOT.parent / "docs" / "run-form.schema.json").read_text())
VALIDATOR = jsonschema.Draft202012Validator(SCHEMA)
APP = ROOT / "tests" / "app"
STAMP = {"by": STAMP_BY, "engine": "0.1.0-dev.g8722848", "commit": "a" * 40, "paramsDigest": "sha256:" + "0" * 64}
# The Argo parameters of tests/app as main enhances them: attach must leave every one of them as it is.
APP_PARAMS = ["pipeline-info", "experiment-name", "experiment-description", "dataset", "dataset-loading-class",
              "dataset-loading-walltime", "hello-world-class", "hello-world-walltime", "promote-loaded", "config",
              "image-compose-and-validate", "image-dataset-loading", "image-hello-world"]


def _ctx(hpc=True):
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    ctx["hpc"]["enabled"] = hpc
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


def _enhanced(make=_small, ctx=None):
    ctx = ctx or _ctx()
    wft = enhance.enhance(make(), ctx)
    steps = enhance.step_templates(wft["spec"])
    args = next(s for s in steps if s["name"] == "compose-and-validate")["container"]["args"]
    offers = json.loads(args[args.index("--class-offers") + 1])
    return wft, ctx, steps, offers


def _raw(**change):
    raw = {"version": 1, "types": {"experiment-name": "string"}, "optionDefaults": {}, "leafOptions": {},
           "when": {"train": "experiment-name != off", "report": "train.score == high"},
           "overrides": {"fields": {"experiment-description": {"label": "Notes", "group": "notes"}},
                         "groups": [{"id": "notes", "label": "Notes", "tier": "basic", "order": 0}]}}
    raw.update(change)
    return raw


def _attached(make=_small, raw=None, ctx=None):
    wft, ctx, steps, offers = _enhanced(make, ctx)
    form = run_form.attach(wft, ctx, steps, offers, raw)
    return wft, form


_BASE_FORM = []


def _base_form():
    """The stamped form of the small pipeline, built once (the mutation tests change copies of it)."""
    if not _BASE_FORM:
        _BASE_FORM.append(_stamped(_attached()[1]))
    return copy.deepcopy(_BASE_FORM[0])


def _stamped(form):
    form = copy.deepcopy(form)
    form["stamp"] = dict(STAMP)
    return form


def _by(form):
    return {f["name"]: f for f in form["fields"]}


# ------------------------------------------------------------------------------------------- the field table

def test_the_field_table_on_tests_app():
    wft, form = _attached(_app)
    f = _by(form)
    assert f["pipeline-info"] == {"name": "pipeline-info", "group": "info", "source": "info", "type": "text",
                                  "tier": "hidden", "platform": True}
    for name in ("image-compose-and-validate", "image-dataset-loading", "image-hello-world"):
        assert f[name] == {"name": name, "group": "platform", "source": "platform", "type": "string",
                           "tier": "hidden", "platform": True}
    assert f["experiment-name"] == {"name": "experiment-name", "group": "experiment", "source": "config",
                                    "type": "string", "tier": "basic", "platform": False}
    assert f["dataset"] == {"name": "dataset", "group": "datasets", "source": "input", "type": "string",
                            "picker": "dataset", "tier": "basic", "platform": False}
    assert f["dataset-loading-class"] == {"name": "dataset-loading-class", "group": "steps", "source": "run-setting",
                                          "type": "enum", "picker": "machine", "tier": "basic", "platform": False,
                                          "step": "dataset-loading", "label": "dataset-loading: machine"}
    assert f["hello-world-walltime"] == {"name": "hello-world-walltime", "group": "steps", "source": "run-setting",
                                         "type": "duration", "tier": "advanced", "platform": False,
                                         "step": "hello-world", "label": "hello-world: time limit",
                                         "pattern": "^[0-9]+[mh]?$", "minMinutes": 1, "maxMinutes": 43200}
    assert f["promote-loaded"] == {"name": "promote-loaded", "group": "promote", "source": "run-setting",
                                   "type": "enum", "tier": "advanced", "platform": False, "step": "dataset-loading",
                                   "label": "Promote loaded"}
    assert f["config"] == {"name": "config", "group": "advanced", "source": "advanced", "type": "yaml",
                           "tier": "advanced", "platform": False}
    assert [g["id"] for g in form["groups"]] == ["info", "experiment", "datasets", "steps", "promote", "advanced",
                                                 "platform"]
    assert [g["order"] for g in form["groups"]] == list(range(7))
    assert {g["id"]: g["tier"] for g in form["groups"]} == {
        "info": "hidden", "experiment": "basic", "datasets": "basic", "steps": "basic", "promote": "advanced",
        "advanced": "advanced", "platform": "hidden"}


def test_a_checkpoint_field_is_a_duration_with_the_platform_pattern_only_there():
    _, form = _attached()
    f = _by(form)
    assert f["train-checkpoint-every"]["type"] == "duration"
    assert f["train-checkpoint-every"]["label"] == "train: checkpoint every"
    with_pattern = {n for n, x in f.items() if "pattern" in x}
    assert with_pattern == {n for n in f if n.endswith("-walltime") or n.endswith("-checkpoint-every")}
    assert {x["pattern"] for n, x in f.items() if n in with_pattern} == {run_form.DURATION_PATTERN}


def test_the_join_is_exact_and_class_enums_equal_offered():
    for make in (_small, _app):
        wft, form = _attached(make)
        params = wft["spec"]["arguments"]["parameters"]
        assert [f["name"] for f in form["fields"]] == [p["name"] for p in params]
        by_param = {p["name"]: p for p in params}
        for s in form["steps"]:
            if s["classField"] is None:
                continue
            assert by_param[s["classField"]]["enum"] == s["offered"]
            assert by_param[s["classField"]]["value"] == s["default"]
            for ref in [s.get("walltimeField"), s.get("checkpointField")] + s.get("promoteFields", []):
                assert ref is None or ref in by_param


def test_the_argo_parameters_are_unchanged():
    for make in (_small, _app):
        wft, ctx, steps, offers = _enhanced(make)
        before = copy.deepcopy(wft["spec"])
        run_form.attach(wft, ctx, steps, offers, None)
        assert wft["spec"] == before
    wft, ctx, steps, offers = _enhanced(_app)
    run_form.attach(wft, ctx, steps, offers, None)
    assert [p["name"] for p in wft["spec"]["arguments"]["parameters"]] == APP_PARAMS
    wft, ctx, steps, offers = _enhanced(_small)
    before = copy.deepcopy(wft["spec"])
    run_form.attach(wft, ctx, steps, offers, _raw())
    assert wft["spec"] == before


# ------------------------------------------------------------------------------------------- showIf

def test_a_step_when_becomes_the_show_if_of_its_fields_and_a_step_value_gives_none():
    _, form = _attached(raw=_raw(when={"train": "experiment-name!='off' and  experiment-name != \"a b\"",
                                       "load": "experiment-name == on", "report": "train.score == high"}))
    f = _by(form)
    show = 'experiment-name != off and experiment-name != "a b"'
    assert f["train-class"]["showIf"] == f["train-walltime"]["showIf"] == f["train-checkpoint-every"]["showIf"] == show
    assert f["promote-loaded"]["showIf"] == "experiment-name == on"
    steps = {s["name"]: s for s in form["steps"]}
    assert steps["train"]["showIf"] == show and steps["load"]["showIf"] == "experiment-name == on"
    # D12: report's when= reads a step's value, so its fields are always shown
    assert "showIf" not in f["report-class"] and "showIf" not in steps["report"]
    assert not any("showIf" in x for x in form["fields"] if x.get("step") is None)


def test_a_config_leaf_under_some_options_is_shown_for_them():
    params = [{"name": "model", "value": "a", "enum": ["a", "b", "c"]}, {"name": "model-lr", "value": "0.1"},
              {"name": "model-depth", "value": "3"}, {"name": "model-head-size", "value": "8"},
              {"name": "other", "value": "1"}]
    raw = {"version": 1, "types": {"model-depth": "integer"}, "leafOptions": {"model-depth": ["b", "c"],
                                                                              "other": ["b"], "model-lr": ["b"]},
           "optionDefaults": {"model": {"b": {"model-lr": "0.2", "model-depth": "4"}, "c": {"model-lr": "0.3"}}},
           "when": {}, "overrides": {"fields": {"model-lr": {"tier": "hidden"}}}}
    run_form.validate_raw(raw, params, [])
    f = _by({"fields": run_form.fields(params, [], raw)})
    assert f["model"]["source"] == "group" and f["model"]["type"] == "enum"
    # model-lr is hidden, so no option carries it; c then sets nothing the form shows
    assert f["model"]["optionDefaults"] == {"b": {"model-depth": "4"}}
    assert f["model-depth"]["showIf"] == "model == b or model == c" and f["model-depth"]["type"] == "integer"
    assert f["model-lr"]["showIf"] == "model == b" and f["model-lr"]["tier"] == "hidden"
    assert "showIf" not in f["other"], "no group prefixes it: always shown"
    assert "showIf" not in f["model-head-size"]


def test_option_defaults_keep_only_served_config_fields():
    params = [{"name": "model", "value": "a", "enum": ["a", "b"]}, {"name": "model-x", "value": "1"},
              {"name": "image-model", "value": "i"}, {"name": "dataset", "value": "d"}]
    raw = {"version": 1, "types": {}, "leafOptions": {}, "when": {}, "overrides": {},
           "optionDefaults": {"model": {"b": {"model-x": "2", "image-model": "j", "dataset": "e"}}}}
    table = [{"name": "s", "inputs": ["dataset"], "produces": []}]
    f = _by({"fields": run_form.fields(params, table, raw)})
    assert f["model"]["optionDefaults"] == {"b": {"model-x": "2"}}


# ------------------------------------------------------------------------------------------- groups, overrides

def test_a_form_yaml_group_sorts_inside_settings_and_carries_its_label():
    _, form = _attached(raw=_raw())
    ids = [g["id"] for g in form["groups"]]
    assert ids.index("experiment") < ids.index("notes") < ids.index("datasets")
    notes = next(g for g in form["groups"] if g["id"] == "notes")
    assert notes == {"id": "notes", "label": "Notes", "tier": "basic", "order": ids.index("notes")}
    assert _by(form)["experiment-description"]["label"] == "Notes"
    assert _by(form)["experiment-description"]["group"] == "notes"


def test_form_yaml_cannot_change_a_platform_field():
    for name in ("image-load", "pipeline-info", "train-class", "promote-loaded", "config"):
        raw = _raw(overrides={"fields": {name: {"tier": "basic"}}})
        with pytest.raises(RunFormError, match="which the platform sets"):
            _attached(raw=raw)


def test_validate_raw_refuses_an_undeclared_group():
    raw = _raw(overrides={"fields": {"experiment-description": {"group": "nowhere"}}})
    with pytest.raises(RunFormError, match="group 'nowhere' is not declared"):
        _attached(raw=raw)
    # a fixed group needs no declaration
    _, form = _attached(raw=_raw(overrides={"fields": {"experiment-description": {"group": "advanced"}}}))
    assert _by(form)["experiment-description"]["group"] == "advanced"


def test_check_form_refuses_a_field_in_a_group_that_is_not_listed():
    _, form = _attached()
    form = _stamped(form)
    form["fields"][1]["group"] = "nowhere"
    assert VALIDATOR.is_valid(form), "the schema alone does not see it"
    with pytest.raises(RunFormError, match="not one of the groups"):
        run_form.check_form(form)


def test_check_form_refuses_an_unknown_step_and_a_repeated_field():
    _, form = _attached()
    form = _stamped(form)
    bad = copy.deepcopy(form)
    next(f for f in bad["fields"] if "step" in f)["step"] = "ghost"
    with pytest.raises(RunFormError, match="not one of the steps"):
        run_form.check_form(bad)
    bad = copy.deepcopy(form)
    bad["fields"].append(copy.deepcopy(bad["fields"][0]))
    with pytest.raises(RunFormError, match="repeats the field"):
        run_form.check_form(bad)


def test_rank_key_ignores_groups_that_are_not_objects():
    assert run_form.rank_key(("x", 0), {"groups": ["x", None, 3, {"id": "x", "order": True}]}) == (2, 1, 0, 3, 0)
    assert run_form.rank_key(("x", 0), {"groups": "x"}) == (len(run_form.GROUPS), 0, 0, 0)
    assert run_form.rank_key(("x", 0), "nonsense") == (len(run_form.GROUPS), 0, 0, 0)
    assert run_form.rank_key(("x", 0), {"groups": [None, {"id": "x", "order": True}]})[:3] == (2, 1, 0)


# ------------------------------------------------------------------------------------------- the schema

def test_the_output_validates_once_given_a_valid_stamp():
    for make, raw, hpc in ((_small, None, True), (_small, _raw(), False), (_app, None, True)):
        _, form = _attached(make, raw, _ctx(hpc))
        VALIDATOR.validate(_stamped(form))
        run_form.check_form(_stamped(form))
        run_form.check_form(form, stamped=False)


def test_the_unsealed_annotation_is_form_invalid_for_osr():
    wft, form = _attached()
    text = wft["metadata"]["annotations"][RUN_FORM_ANNOTATION]
    decoded = json.loads(text)
    assert decoded == form and decoded["stamp"] == {"by": STAMP_BY, "engine": "", "commit": "", "paramsDigest": ""}
    errors = list(VALIDATOR.iter_errors(decoded))
    # OSR's parseRunForm runs the schema; it fails, on the stamp and nowhere else, so OSR answers form_invalid
    assert errors and {tuple(e.absolute_path)[:1] for e in errors} == {("stamp",)}
    assert {tuple(e.absolute_path)[1] for e in errors} == {"engine", "commit", "paramsDigest"}
    with pytest.raises(RunFormError, match="stamp.engine"):
        run_form.check_form(decoded)


def test_the_annotation_is_the_wire_form_and_byte_identical_across_renders():
    one, _ = _attached(raw=_raw())
    two, _ = _attached(raw=_raw())
    a = one["metadata"]["annotations"][RUN_FORM_ANNOTATION]
    assert a == two["metadata"]["annotations"][RUN_FORM_ANNOTATION]
    assert a.isascii() and not any(c in a for c in "&<>$")
    assert a == run_form.encode(json.loads(a))
    assert list(json.loads(a)) == sorted(json.loads(a))


def _set(path, value):
    def change(form):
        node = form
        for key in path[:-1]:
            node = node[key]
        if value is _DELETE:
            del node[path[-1]]
        else:
            node[path[-1]] = value
    return change


_DELETE = object()


def _machine(form, kind):
    return next(i for i, m in enumerate(form["machines"]) if m["kind"] == kind)


def _field(form, name):
    return next(i for i, f in enumerate(form["fields"]) if f["name"] == name)


MUTATIONS = [
    ("version", lambda f: _set(["version"], 2)),
    ("version float", lambda f: _set(["version"], 1.0)),
    ("stamp by", lambda f: _set(["stamp", "by"], "someone")),
    ("stamp commit", lambda f: _set(["stamp", "commit"], "ABC")),
    ("stamp engine plus", lambda f: _set(["stamp", "engine"], "0.4.7+g1")),
    ("stamp extra", lambda f: _set(["stamp", "extra"], "x")),
    ("top extra", lambda f: _set(["extra"], 1)),
    ("no fields", lambda f: _set(["fields"], _DELETE)),
    ("group tier", lambda f: _set(["groups", 0, "tier"], "loud")),
    ("group id", lambda f: _set(["groups", 0, "id"], "Bad")),
    ("group order", lambda f: _set(["groups", 0, "order"], -1)),
    ("group label", lambda f: _set(["groups", 0, "label"], "x" * 81)),
    ("33 groups", lambda f: _set(["groups"], [dict(f["groups"][0], id="g%d" % i) for i in range(33)])),
    ("field type", lambda f: _set(["fields", 0, "type"], "color")),
    ("field source", lambda f: _set(["fields", 0, "source"], "mine")),
    ("field extra", lambda f: _set(["fields", 0, "colour"], "red")),
    ("field platform", lambda f: _set(["fields", 0, "platform"], "yes")),
    ("field name", lambda f: _set(["fields", 0, "name"], "-bad")),
    ("field no tier", lambda f: _set(["fields", 0, "tier"], _DELETE)),
    ("field label", lambda f: _set(["fields", 1, "label"], "x" * 81)),
    ("field label ok", lambda f: _set(["fields", 1, "label"], "x" * 80)),
    ("field showIf", lambda f: _set(["fields", 1, "showIf"], "a == b" * 200)),
    ("field minimum", lambda f: _set(["fields", 1, "minimum"], "1")),
    ("field minimum ok", lambda f: _set(["fields", 1, "minimum"], 1.5)),
    ("minMinutes", lambda f: _set(["fields", _field(f, "train-walltime"), "minMinutes"], 0)),
    ("maxMinutes", lambda f: _set(["fields", _field(f, "train-walltime"), "maxMinutes"], 43201)),
    ("maxMinutes float ok", lambda f: _set(["fields", _field(f, "train-walltime"), "maxMinutes"], 43200.0)),
    ("pattern long", lambda f: _set(["fields", _field(f, "train-walltime"), "pattern"], "x" * 257)),
    ("picker", lambda f: _set(["fields", _field(f, "train-class"), "picker"], "class")),
    ("optionDefaults value", lambda f: _set(["fields", 1, "optionDefaults"], {"a": {"x": 3}})),
    ("optionDefaults ok", lambda f: _set(["fields", 1, "optionDefaults"], {"a": {"x": "3"}})),
    ("cluster with hpc kind", lambda f: _set(["machines", _machine(f, "cluster"), "kind"], "hpc")),
    ("hpc without hpc", lambda f: _set(["machines", _machine(f, "hpc"), "hpc"], _DELETE)),
    ("cluster with hpc", lambda f: _set(["machines", _machine(f, "cluster"), "hpc"],
                                         f["machines"][_machine(f, "hpc")]["hpc"])),
    ("shares node", lambda f: _set(["machines", _machine(f, "hpc"), "hpc", "sharesNode"], False)),
    ("accelerator count", lambda f: _set(["machines", 0, "accelerator"], {"type": None, "count": 0})),
    ("allocatable cpu", lambda f: _set(["machines", 0, "allocatable", "cpu"], None)),
    ("description", lambda f: _set(["machines", 0, "description"], "x" * 513)),
    ("machine tier", lambda f: _set(["machines", 0, "tier"], "tpu")),
    ("offered empty", lambda f: _set(["steps", 0, "offered"], [])),
    ("refused short", lambda f: _set(["steps", 0, "refused"], {"x": ["only a reason"]})),
    ("refused long", lambda f: _set(["steps", 0, "refused"], {"x": ["a", "b", "c"]})),
    ("step kind", lambda f: _set(["steps", 0, "kind"], "deploy")),
    ("class field", lambda f: _set(["steps", 0, "classField"], 3)),
    ("class field null ok", lambda f: _set(["steps", 0, "classField"], None)),
    ("promote fields", lambda f: _set(["steps", 0, "promoteFields"], "promote-loaded")),
]


@pytest.mark.parametrize("name, mutate", MUTATIONS, ids=[m[0] for m in MUTATIONS])
def test_check_form_agrees_with_the_schema(name, mutate):
    form = _base_form()
    mutate(form)(form)
    schema_ok = VALIDATOR.is_valid(form)
    try:
        run_form.check_form(form)
        ours = True
    except RunFormError:
        ours = False
    assert ours == schema_ok, "%s: check_form %s, the schema %s" % (name, ours, schema_ok)


def test_the_mutations_both_pass_and_fail():
    outcomes = set()
    for _, mutate in MUTATIONS:
        form = _base_form()
        mutate(form)(form)
        outcomes.add(VALIDATOR.is_valid(form))
    assert outcomes == {True, False}


# ------------------------------------------------------------------------------------------- the cap

def _big_ctx(count=195):
    ctx = _ctx(hpc=False)
    template = ctx["computeClasses"]["all"][0]
    ctx["computeClasses"]["all"] = [dict(copy.deepcopy(template), name="class-%03d" % i, description="d" * 600)
                                    for i in range(count)]
    return ctx


def test_over_the_cap_the_build_fails_naming_the_largest_section():
    wft, ctx, steps, offers = _enhanced()
    with pytest.raises(RunFormError, match=r"over the %d byte cap; its largest section is machines" % RUN_FORM_CAP):
        run_form.attach(wft, _big_ctx(), steps, offers, None)
    assert RUN_FORM_ANNOTATION not in (wft["metadata"].get("annotations") or {})


def test_descriptions_are_truncated_not_failed(capsys, monkeypatch):
    wft, ctx, steps, offers = _enhanced()
    monkeypatch.setattr(run_form, "RUN_FORM_WARN", 1024)
    form = run_form.attach(wft, _big_ctx(3), steps, offers, None)
    assert all(len(m["description"]) <= 512 for m in form["machines"])
    assert "WARNING: the run form is" in capsys.readouterr().err


# ------------------------------------------------------------------------------------------- OSR's checkJoin

_OSR_TOKEN = re.compile(r"[A-Za-z0-9_.-]+")


def _osr_check_join(form, params):
    """OSR pipeline_runform.go checkJoin + excludedNames, written again here from the Go (not from run_form)."""
    served = {g["id"] for g in form["groups"] if g["tier"] != "hidden"}
    excluded = {f["name"] for f in form["fields"]
                if f["platform"] or f["source"] == "platform" or f["tier"] == "hidden" or f["group"] not in served}

    def withheld(name):
        return name.lower().startswith("image-") or name in excluded

    def names(expr):
        return any(withheld(t) for t in _OSR_TOKEN.findall(expr or ""))

    fields = {f["name"]: f for f in form["fields"]}
    for p in params:
        if p["name"] and not withheld(p["name"]) and p["name"] not in fields:
            return "a template parameter has no entry in the form"
    for name, f in fields.items():
        if name not in excluded and names(f.get("showIf")):
            return "a showIf names a withheld parameter"
    for s in form["steps"]:
        if names(s.get("showIf")):
            return "a step showIf names a withheld parameter"
        refs = [s.get("walltimeField"), s.get("checkpointField"), s.get("classField")] + (s.get("promoteFields") or [])
        if any(r and withheld(r) for r in refs):
            return "a step refers to a withheld parameter"
    return None


def test_the_output_passes_osrs_check_join():
    for make, raw in ((_small, None), (_small, _raw()), (_app, None)):
        wft, form = _attached(make, raw)
        params = wft["spec"]["arguments"]["parameters"]
        assert _osr_check_join(form, params) is None
        withheld = {f["name"] for f in form["fields"] if f["tier"] == "hidden" or f["platform"]}
        assert {f["name"] for f in form["fields"] if f["name"].startswith("image-")} <= withheld
        assert all(f["platform"] and f["tier"] == "hidden" and f["source"] == "platform"
                   for f in form["fields"] if f["name"].startswith("image-"))


def test_a_when_on_a_hidden_field_is_refused_as_osr_would():
    raw = _raw(overrides={"fields": {"experiment-name": {"tier": "hidden"}}})
    with pytest.raises(RunFormError, match="step 'train': its when= reads 'experiment-name'"):
        _attached(raw=raw)


@pytest.mark.parametrize("mutate", [
    lambda f: f["fields"][_field(f, "train-class")].__setitem__("showIf", "image-load == x"),
    lambda f: f["steps"][0].__setitem__("showIf", "mode == pipeline-info"),
    lambda f: f["steps"][0].__setitem__("walltimeField", "image-load"),
    lambda f: f["steps"][0].__setitem__("classField", "pipeline-info"),
    lambda f: f["fields"].pop(_field(f, "experiment-name")),
    lambda f: f["fields"][_field(f, "train-class")].__setitem__("showIf", "experiment-name == 'image-x'"),
], ids=["field-showIf-image", "step-showIf-info", "walltime-image", "class-info", "missing", "quoted-value"])
def test_check_join_and_osr_agree_on_withheld_references(mutate):
    wft, form = _attached()
    params = wft["spec"]["arguments"]["parameters"]
    mutate(form)
    assert _osr_check_join(form, params) is not None
    with pytest.raises(RunFormError):
        run_form.check_join(form, params)


# ------------------------------------------------------------------------------------------- imports

@pytest.mark.parametrize("first, second", [("kubecore.run_form", "kubecore.enhance"),
                                           ("kubecore.enhance", "kubecore.run_form")])
def test_run_form_and_enhance_import_in_either_order(first, second):
    code = "import sys; sys.path.insert(0, %r); import %s; import %s" % (str(ROOT), first, second)
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
