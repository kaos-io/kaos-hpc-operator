"""The seal and its CLI (PRD 1533/F-21 and 1533/F-22): the stamp, the digest, anti-churn, and every refusal.
Run:  python -m pytest tests/ -v
"""
import copy
import json
import re
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
from kubecore.step_contract import (  # noqa: E402
    ENGINE_RUN_LABEL, ENGINE_VERSION_ANNOTATION, RUN_FORM_ANNOTATION, RUN_FORM_CAP, SOURCE_COMMIT_KEY, STAMP_BY)

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
SCHEMA = json.loads((ROOT.parent / "docs" / "run-form.schema.json").read_text())
VALIDATOR = jsonschema.Draft202012Validator(SCHEMA)
ENGINE = "0.1.0-dev.g8722848"
COMMIT = "a" * 40
OTHER = "b" * 40


def _small():
    with authoring.pipeline("t") as p:
        load = authoring.step("load", kind="ingest", inputs=["dataset"], produces=[authoring.dataset("loaded")])
        train = authoring.step("train", kind="train", needs=[load], gpu=True, walltime="12h", checkpoint_every="10m",
                               produces=[authoring.model("m")])
        authoring.step("report", kind="evaluate", needs=[train])
    return yaml.safe_load(p.wt.to_yaml())


def _wft():
    """An enhanced template with its form attached and stamped with the engine, as the chart hands it to the seal."""
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3", enabled=True)
    wft = enhance.enhance(_small(), ctx)
    steps = enhance.step_templates(wft["spec"])
    args = next(s for s in steps if s["name"] == "compose-and-validate")["container"]["args"]
    offers = json.loads(args[args.index("--class-offers") + 1])
    run_form.attach(wft, ctx, steps, offers, None)
    wft["metadata"]["annotations"][ENGINE_VERSION_ANNOTATION] = ENGINE
    wft["spec"].setdefault("workflowMetadata", {}).setdefault("labels", {})[ENGINE_RUN_LABEL] = ENGINE
    return wft


def _form(wft):
    return json.loads(wft["metadata"]["annotations"][RUN_FORM_ANNOTATION])


def _put(wft, form):
    wft["metadata"]["annotations"][RUN_FORM_ANNOTATION] = json.dumps(form)


def _params(wft):
    return wft["spec"]["arguments"]["parameters"]


def _param(wft, name):
    return next(p for p in _params(wft) if p["name"] == name)


def _fail(wft, match, commit=COMMIT):
    with pytest.raises(RunFormError, match=match):
        run_form.seal(wft, commit)


# ----------------------------------------------------------------------------------------- what OSR checks

def osr_parse_run_form(text, engine_annotation):
    """A Python mirror of OSR's parseRunForm (handlers/runform.go): size, UTF-8, the schema, and the stamp."""
    raw = text.encode("utf-8")
    assert len(raw) <= 131072
    form = json.loads(text)
    VALIDATOR.validate(form)
    assert form["stamp"]["engine"] == engine_annotation
    assert form["stamp"]["by"] == STAMP_BY
    assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}", form["stamp"]["engine"])
    assert re.fullmatch(r"[0-9a-f]{40}", form["stamp"]["commit"])
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", form["stamp"]["paramsDigest"])
    names = [f["name"] for f in form["fields"]]
    assert len(set(names)) == len(names)
    return form


def test_the_sealed_output_passes_the_schema_and_osr_s_stamp_checks():
    wft = _wft()
    form = run_form.seal(wft, COMMIT)
    text = wft["metadata"]["annotations"][RUN_FORM_ANNOTATION]
    assert osr_parse_run_form(text, wft["metadata"]["annotations"][ENGINE_VERSION_ANNOTATION]) == form
    assert text == run_form.encode(form)
    run_form.check_form(form)
    run_form.check_join(form, _params(wft))
    assert form["stamp"] == {"by": STAMP_BY, "engine": ENGINE, "commit": COMMIT,
                             "paramsDigest": run_form.params_digest(_params(wft))}


def test_the_seal_writes_the_commit_annotation_and_labels():
    wft = _wft()
    wft["metadata"]["labels"] = {"platform.kubecore.io/project": "proj", "platform.kubecore.io/app": "app",
                                 "other": "x"}
    run_form.seal(wft, COMMIT)
    labels = wft["spec"]["workflowMetadata"]["labels"]
    assert wft["metadata"]["annotations"][SOURCE_COMMIT_KEY] == COMMIT
    assert labels[SOURCE_COMMIT_KEY] == COMMIT and labels[ENGINE_RUN_LABEL] == ENGINE
    assert labels["platform.kubecore.io/project"] == "proj" and labels["platform.kubecore.io/app"] == "app"
    assert "other" not in labels


# ------------------------------------------------------------------------------------------------- the digest

def test_the_digest_does_not_move_when_the_dataset_enum_is_filled_and_moves_when_a_default_changes():
    wft = _wft()
    before = run_form.params_digest(_params(wft))
    _param(wft, "dataset")["enum"] = ["a@latest", "b@latest"]
    _param(wft, "dataset")["description"] = "changed"
    assert run_form.params_digest(_params(wft)) == before
    _param(wft, "dataset")["value"] = "b@latest"
    assert run_form.params_digest(_params(wft)) != before


def test_the_digest_covers_value_from_and_is_the_documented_hash():
    import hashlib
    params = [{"name": "a", "value": "é"}, {"name": "b", "valueFrom": {"configMapKeyRef": {"name": "n", "key": "k"}}}]
    text = ('[{"name":"a","value":"é"},{"name":"b","valueFrom":{"configMapKeyRef":{"key":"k","name":"n"}}}]')
    assert run_form.params_digest(params) == "sha256:" + hashlib.sha256(text.encode()).hexdigest()


# ---------------------------------------------------------------------------------------------- anti-churn

def test_the_previous_commit_is_kept_when_only_the_commit_differs():
    live = _wft()
    run_form.seal(live, COMMIT)
    new = _wft()
    form = run_form.seal(new, OTHER, previous=live)
    assert form["stamp"]["commit"] == COMMIT
    assert new["metadata"]["annotations"][SOURCE_COMMIT_KEY] == COMMIT
    assert new["spec"]["workflowMetadata"]["labels"][SOURCE_COMMIT_KEY] == COMMIT
    assert new["metadata"]["annotations"][RUN_FORM_ANNOTATION] == live["metadata"]["annotations"][RUN_FORM_ANNOTATION]


def test_a_changed_form_takes_the_new_commit():
    live = _wft()
    run_form.seal(live, COMMIT)
    new = _wft()
    _param(new, "dataset")["value"] = "other@latest"
    assert run_form.seal(new, OTHER, previous=live)["stamp"]["commit"] == OTHER


def test_an_unreadable_previous_is_ignored():
    for previous in ({}, {"metadata": {"annotations": {RUN_FORM_ANNOTATION: "{"}}},
                     {"metadata": {"annotations": {RUN_FORM_ANNOTATION: '{"stamp":{"commit":"x"}}'}}}):
        assert run_form.seal(_wft(), OTHER, previous=previous)["stamp"]["commit"] == OTHER


# ------------------------------------------------------------------------------------------ chart-added params

def test_a_chart_added_parameter_gets_a_hidden_platform_field_in_argo_order():
    wft = _wft()
    _params(wft).insert(2, {"name": "dataset-sha", "value": "abc"})
    form = run_form.seal(wft, COMMIT)
    assert [f["name"] for f in form["fields"]] == [p["name"] for p in _params(wft)]
    f = next(f for f in form["fields"] if f["name"] == "dataset-sha")
    assert (f["source"], f["tier"], f["platform"], f["group"]) == ("platform", "hidden", True, "platform")
    assert "platform" in [g["id"] for g in form["groups"]]
    osr_parse_run_form(wft["metadata"]["annotations"][RUN_FORM_ANNOTATION], ENGINE)


def test_any_other_added_parameter_fails():
    wft = _wft()
    _params(wft).append({"name": "surprise", "value": "x"})
    _fail(wft, "surprise")


# ----------------------------------------------------------------------------------------------- refusals

def test_a_missing_annotation_fails():
    wft = _wft()
    del wft["metadata"]["annotations"][RUN_FORM_ANNOTATION]
    _fail(wft, "no ui.kubecore.io/run-form")


@pytest.mark.parametrize("engine", [None, "", "bad value!", "+x", "a" * 64])
def test_a_missing_or_bad_engine_fails(engine):
    wft = _wft()
    if engine is None:
        del wft["metadata"]["annotations"][ENGINE_VERSION_ANNOTATION]
    else:
        wft["metadata"]["annotations"][ENGINE_VERSION_ANNOTATION] = engine
    _fail(wft, "engine version")


def test_an_engine_label_that_does_not_match_fails():
    wft = _wft()
    wft["spec"]["workflowMetadata"]["labels"][ENGINE_RUN_LABEL] = "0.2.0"
    _fail(wft, "workflowMetadata label")
    del wft["spec"]["workflowMetadata"]["labels"][ENGINE_RUN_LABEL]
    _fail(wft, "workflowMetadata label")


@pytest.mark.parametrize("commit", ["", "A" * 40, "a" * 39, "a" * 41, "g" * 40, None])
def test_a_bad_commit_fails(commit):
    _fail(_wft(), "source commit", commit)


def test_a_parameter_without_a_field_fails():
    wft = _wft()
    form = _form(wft)
    form["fields"] = [f for f in form["fields"] if f["name"] != "dataset"]
    _put(wft, form)
    _fail(wft, "'dataset'")


def test_a_field_that_is_not_a_parameter_fails():
    wft = _wft()
    form = _form(wft)
    form["fields"].append(dict(form["fields"][0], name="ghost"))
    _put(wft, form)
    _fail(wft, "ghost")


def test_a_duplicated_field_fails():
    wft = _wft()
    form = _form(wft)
    form["fields"].append(copy.deepcopy(form["fields"][3]))
    _put(wft, form)
    _fail(wft, "more than once")


def test_a_duplicated_parameter_fails():
    wft = _wft()
    _params(wft).append(copy.deepcopy(_params(wft)[0]))
    _fail(wft, "repeat")


def test_a_reordered_field_list_fails():
    wft = _wft()
    form = _form(wft)
    form["fields"][0], form["fields"][1] = form["fields"][1], form["fields"][0]
    _put(wft, form)
    _fail(wft, "order")


def test_an_image_field_that_is_not_marked_platform_fails():
    wft = _wft()
    form = _form(wft)
    next(f for f in form["fields"] if f["name"].startswith("image-"))["platform"] = False
    _put(wft, form)
    _fail(wft, "image-")


def test_a_show_if_on_a_withheld_field_fails():
    wft = _wft()
    form = _form(wft)
    next(f for f in form["fields"] if f["name"] == "experiment-name")["showIf"] = "image-load != x"
    _put(wft, form)
    _fail(wft, "image-load")


def test_a_step_show_if_on_a_withheld_field_fails():
    wft = _wft()
    form = _form(wft)
    form["steps"][0]["showIf"] = "pipeline-info == x"
    _put(wft, form)
    _fail(wft, "pipeline-info")


def test_a_step_reference_to_a_withheld_or_missing_field_fails():
    wft = _wft()
    form = _form(wft)
    form["steps"][0]["walltimeField"] = "pipeline-info"
    _put(wft, form)
    _fail(wft, "pipeline-info")
    form["steps"][0]["walltimeField"] = "nowhere"
    _put(wft, form)
    _fail(wft, "nowhere")


def test_a_class_outside_offered_fails_even_for_a_developer_declared_parameter():
    wft = _wft()
    _param(wft, "train-class")["value"] = "cpu-nonexistent"
    _fail(wft, "train-class")


def test_a_class_enum_that_is_not_offered_fails():
    wft = _wft()
    _param(wft, "train-class")["enum"] = ["a", "b"]
    _fail(wft, "enum")


def test_a_form_over_the_cap_fails():
    wft = _wft()
    form = _form(wft)
    form["fields"][0]["label"] = "x" * (RUN_FORM_CAP + 1)
    _put(wft, form)
    with pytest.raises(RunFormError):
        run_form.seal(wft, COMMIT)


def test_a_form_that_the_schema_refuses_fails():
    wft = _wft()
    form = _form(wft)
    form["fields"][0]["tier"] = "loud"
    _put(wft, form)
    _fail(wft, "tier")


def test_the_parameters_are_unchanged_by_the_seal():
    wft = _wft()
    before = copy.deepcopy(_params(wft))
    run_form.seal(wft, COMMIT)
    assert _params(wft) == before


# --------------------------------------------------------------------------------------------------- the CLI

def _cli(*args, cwd=ROOT):
    return subprocess.run([sys.executable, "-m", "kubecore.run_form", *args], cwd=cwd, capture_output=True,
                          text=True)


def test_the_cli_seals_in_place_and_keeps_the_previous_commit(tmp_path):
    live, new = tmp_path / "live.yaml", tmp_path / "new.yaml"
    live.write_text(yaml.safe_dump(_wft(), sort_keys=False))
    res = _cli("seal", "--wft", str(live), "--source-commit", COMMIT)
    assert res.returncode == 0, res.stderr
    new.write_text(yaml.safe_dump(_wft(), sort_keys=False))
    res = _cli("seal", "--wft", str(new), "--source-commit", OTHER, "--previous", str(live))
    assert res.returncode == 0, res.stderr
    assert yaml.safe_load(new.read_text()) == yaml.safe_load(live.read_text())
    sealed = yaml.safe_load(new.read_text())
    osr_parse_run_form(sealed["metadata"]["annotations"][RUN_FORM_ANNOTATION], ENGINE)
    assert _cli("seal", "--wft", str(new), "--source-commit", OTHER, "--previous", str(tmp_path / "none")).returncode == 0


def test_the_cli_refuses_with_exit_1_and_a_gate_fail_line(tmp_path):
    path = tmp_path / "w.yaml"
    wft = _wft()
    del wft["metadata"]["annotations"][RUN_FORM_ANNOTATION]
    path.write_text(yaml.safe_dump(wft, sort_keys=False))
    before = path.read_text()
    res = _cli("seal", "--wft", str(path), "--source-commit", COMMIT)
    assert res.returncode == 1 and res.stdout == ""
    assert res.stderr.startswith("GATE FAIL: run form: ") and res.stderr.count("\n") == 1
    assert path.read_text() == before
    assert _cli("seal", "--wft", str(tmp_path / "missing.yaml"), "--source-commit", COMMIT).returncode == 1


def test_the_cli_runs_from_the_engine_directory_and_loads_no_renderer(tmp_path):
    path = tmp_path / "w.yaml"
    path.write_text(yaml.safe_dump(_wft(), sort_keys=False))
    code = ("import sys; from kubecore import run_form; rc = run_form.main(sys.argv[1:]); "
            "bad = [m for m in ('hera', 'hydra', 'omegaconf') if m in sys.modules]; "
            "print(bad); sys.exit(rc or (1 if bad else 0))")
    res = subprocess.run([sys.executable, "-c", code, "seal", "--wft", str(path), "--source-commit", COMMIT],
                         cwd=ROOT, capture_output=True, text=True)
    assert res.returncode == 0, res.stdout + res.stderr
    assert res.stdout.strip() == "[]"
