"""The raw form's strip and validation (PRD 1533/F-21 and 1533/F-22), called directly.
Run:  python -m pytest tests/ -v
"""
import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import run_form  # noqa: E402
from kubecore.run_form import RunFormError, take_raw_form, validate_raw  # noqa: E402
from kubecore.step_contract import RAW_FORM_ANNOTATION, RAW_FORM_CAP  # noqa: E402

PARAMS = [
    {"name": "experiment-name", "value": "x"},
    {"name": "train.lr", "value": "0.1"},
    {"name": "train.epochs", "value": "3"},
    {"name": "train.amp", "value": "true"},
    {"name": "model", "value": "a", "enum": ["a", "b"]},
    {"name": "train-class", "value": "gpu", "enum": ["gpu", "cpu"]},
]
GOOD = {
    "version": 1,
    "types": {"train.lr": "number", "train.epochs": "integer", "train.amp": "boolean", "model": "enum",
              "experiment-name": "string"},
    "optionDefaults": {"model": {"b": {"train.lr": "0.2"}}},
    "leafOptions": {"train.lr": {"group": "model", "options": ["a"]}},
    "when": {"train": "train.amp == true"},
    "overrides": {"fields": {"train.epochs": {"label": "Epochs", "tier": "advanced", "minimum": 1, "maximum": 9,
                                              "group": "tuning"}},
                  "groups": [{"id": "tuning", "label": "Tuning", "tier": "basic", "order": 1}]},
}


def _raw(**change):
    raw = copy.deepcopy(GOOD)
    raw.update(change)
    return raw


def _validate(raw, parse_when=None):
    validate_raw(raw, PARAMS, ["train", "report"], parse_when)


# ------------------------------------------------------------------------------------------- the strip

def _wft(text=None):
    annotations = {"keep": "1", "ui.kubecore.io/run-form": "forged", "ui.kubecore.io/other": "x",
                   "platform.kubecore.io/source-commit": "f", "platform.kubecore.io/engine-version": "v",
                   "platform.kubecore.io/params-digest": "d", "platform.kubecore.io/pipeline-version": "p",
                   "platform.kubecore.io/engine-note": "stays"}
    if text is not None:
        annotations[RAW_FORM_ANNOTATION] = text
    return {"metadata": {"annotations": annotations,
                         "labels": {"keep": "1", "ui.kubecore.io/run-form": "forged",
                                    "platform.kubecore.io/source-commit": "f",
                                    "platform.kubecore.io/engine": "stays"}},
            "spec": {"workflowMetadata": {"labels": {"platform.kubecore.io/engine": "e", "ui.kubecore.io/a": "1",
                                                     "mine": "1"},
                                          "annotations": {"platform.kubecore.io/x": "1", "ui.kubecore.io/y": "2",
                                                          "mine": "2"}}}}


def test_take_pops_the_raw_form_and_strips_forged_keys_everywhere():
    wft = _wft(json.dumps(GOOD))
    assert take_raw_form(wft) == GOOD
    assert wft["metadata"]["annotations"] == {"keep": "1", "platform.kubecore.io/engine-note": "stays"}
    assert wft["metadata"]["labels"] == {"keep": "1", "platform.kubecore.io/engine": "stays"}
    assert wft["spec"]["workflowMetadata"] == {"labels": {"mine": "1"}, "annotations": {"mine": "2"}}


def test_take_without_a_raw_form_still_strips():
    wft = _wft()
    assert take_raw_form(wft) is None
    assert "ui.kubecore.io/run-form" not in wft["metadata"]["annotations"]


def test_take_refuses_an_oversized_raw_form():
    with pytest.raises(RunFormError, match="over the"):
        take_raw_form(_wft("[" + "1," * RAW_FORM_CAP + "1]"))


def test_take_counts_utf8_bytes_not_characters():
    text = json.dumps({"a": "é" * (RAW_FORM_CAP // 2 + 10)}, ensure_ascii=False)
    assert len(text) < RAW_FORM_CAP < len(text.encode())
    with pytest.raises(RunFormError, match="over the"):
        take_raw_form(_wft(text))


@pytest.mark.parametrize("text, message", [
    ("{not json", "not JSON"),
    ('{"version": 1, "version": 1}', "raw form repeats the key 'version'"),
    ('{"a": {"b": 1, "b": 2}}', r"raw form\.a repeats the key 'b'"),
    ('{"a": [{"b": 1, "b": 2}]}', r"raw form\.a\[0\] repeats the key 'b'"),
    ('{"version": NaN}', "contains NaN"),
    ('{"version": -Infinity}', "contains -Infinity"),
    ("[1]", "must be a JSON object"),
])
def test_take_refuses(text, message):
    with pytest.raises(RunFormError, match=message):
        take_raw_form(_wft(text))


@pytest.mark.parametrize("edit, message", [
    (lambda w: w["metadata"].update(annotations="x"), "metadata.annotations must be a mapping"),
    (lambda w: w["metadata"].update(labels=["a"]), "metadata.labels must be a mapping"),
    (lambda w: w["metadata"].update(annotations={1: "a"}), "not text"),
    (lambda w: w["spec"].update(workflowMetadata="x"), "workflowMetadata must be a mapping"),
    (lambda w: w["spec"]["workflowMetadata"].update(labels="x"), "workflowMetadata.labels must be a mapping"),
    (lambda w: w["spec"]["workflowMetadata"].update(annotations={2: "x"}), "not text"),
    (lambda w: w.update(metadata="x"), "metadata must be a mapping"),
    (lambda w: w.update(spec=[]), "spec must be a mapping"),
])
def test_take_refuses_a_template_of_the_wrong_shape(edit, message):
    wft = _wft(json.dumps(GOOD))
    edit(wft)
    with pytest.raises(RunFormError, match=message):
        take_raw_form(wft)


# ------------------------------------------------------------------------------------------ validation

def test_a_good_raw_form_passes():
    _validate(_raw())
    seen = []
    _validate(_raw(), parse_when=seen.append)
    assert seen == ["train.amp == true"]


def _with(path, value):
    raw = _raw()
    node = raw
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return raw


def _over(**fields):
    raw = _raw()
    raw["overrides"]["fields"] = fields
    return raw


def _lacking():
    raw = _raw()
    del raw["when"]
    return raw


@pytest.mark.parametrize("raw, message", [
    (_raw(version=2), "version must be 1"),
    (_raw(version=True), "version must be 1"),
    (_raw(extra={}), "unknown keys"),
    (_lacking(), "lacks the keys"),
    (_with(["types", "nope"], "string"), "not a run form parameter"),
    (_with(["types", "train.lr"], "integer"), "is an integer"),
    (_with(["types", "train.amp"], "integer"), "is an integer"),
    (_with(["types", "train.lr"], "boolean"), "is a boolean"),
    (_with(["types", "train.lr"], "enum"), "no enum"),
    (_with(["types", "train.lr"], "color"), "not a type"),
    (_with(["optionDefaults", "model", "c"], {}), "not in the group's options"),
    (_with(["optionDefaults", "model", "b", "ghost"], "1"), "not a run form parameter"),
    (_with(["optionDefaults", "model", "b", "train.lr"], "x" * 4097), "at most 4096"),
    (_with(["optionDefaults", "model", "b", "train.lr"], 3), "at most 4096"),
    (_with(["optionDefaults", "train.lr"], {}), "not a dropdown"),
    (_with(["leafOptions", "ghost"], {"group": "model", "options": ["a"]}), "not a run form parameter"),
    (_with(["leafOptions", "train.lr"], ["a"]), "keys group and options"),
    (_with(["leafOptions", "train.lr"], {"group": "model"}), "keys group and options"),
    (_with(["leafOptions", "train.lr"], {"group": "nope", "options": ["a"]}), "not a config group"),
    (_with(["leafOptions", "train.lr"], {"group": "model", "options": "a"}), "list of option names"),
    (_with(["leafOptions", "train.lr"], {"group": "model", "options": []}), "list of option names"),
    (_with(["leafOptions", "train.lr"], {"group": "model", "options": ["z"]}), "not in the group's options"),
    (_with(["when", "other"], "train.amp"), "not a step of this pipeline"),
    (_with(["when", "train"], 5), "must be text"),
    (_over(**{"train.epochs": {"pattern": "^a$"}}), "pattern is not supported"),
    (_over(**{"train.epochs": {"colour": "red"}}), "unknown keys"),
    (_over(**{"train.epochs": {"label": "x" * 81}}), "label"),
    (_over(**{"train.epochs": {"label": "a\nb"}}), "control characters"),
    (_over(**{"train.epochs": {"tier": "loud"}}), "tier must be"),
    (_over(**{"train.amp": {"minimum": 1}}), "only to integer and number"),
    (_over(**{"train.epochs": {"minimum": 5, "maximum": 2}}), "above maximum"),
    (_over(**{"train.epochs": {"maximum": "9"}}), "must be a number"),
    (_over(**{"train.epochs": {"group": "Bad Id"}}), "must match"),
    (_over(**{"ghost": {"label": "x"}}), "not a run form parameter"),
    (_with(["overrides", "groups"], [{"id": "advanced", "label": "A"}]), "fixed groups"),
    (_with(["overrides", "groups"], [{"id": "BAD"}]), "must match"),
    (_with(["overrides", "groups"], [{"id": "g", "x": 1}]), "unknown keys"),
    (_with(["overrides", "groups"], [{"id": "g"}, {"id": "g"}]), "declared twice"),
    (_with(["overrides", "groups"], [{"id": "g%d" % i} for i in range(33)]), "at most 32"),
    (_with(["overrides", "stuff"], 1), "unknown keys"),
    (_with(["types", "train.lr"], ["number"]), "not a type"),
    (_with(["types", "train.lr"], {"a": 1}), "not a type"),
    (_over(**{"train.epochs": {"tier": ["basic"]}}), "tier must be"),
    (_over(**{"train.epochs": {"tier": {}}}), "tier must be"),
    (_with(["overrides", "groups"], [{"id": "g", "tier": ["basic"]}]), "tier must be"),
])
def test_validate_refuses(raw, message):
    with pytest.raises(RunFormError, match=message):
        _validate(raw)


def test_a_when_text_that_does_not_parse_is_refused():
    def parse(text):
        raise ValueError("bad text")
    with pytest.raises(RunFormError, match="does not parse: bad text"):
        _validate(_raw(), parse_when=parse)


@pytest.mark.parametrize("text", ["train.amp", "mode ==", "a == b or", "(a == b)", "a = b", "{{workflow.parameters.x}} == 1",
                                  "a == b; c == d", ""])
def test_the_default_parser_refuses_an_invalid_when(text):
    with pytest.raises(RunFormError, match=r"when\['train'\] does not parse"):
        _validate(_with(["when", "train"], text))


def test_the_default_parser_accepts_the_grammar():
    _validate(_with(["when", "train"], "train.amp == true and model != 'b' or model == \"a\""))


def test_the_allowed_override_keys_are_the_v1_set():
    assert run_form.OVERRIDE_FIELD_KEYS == {"label", "tier", "minimum", "maximum", "group"}


def test_importing_run_form_leaves_the_render_libraries_out():
    code = ("import sys; sys.path.insert(0, %r); import kubecore.run_form; "
            "bad = [m for m in ('hera', 'hydra', 'omegaconf') if m in sys.modules]; "
            "sys.exit(1 if bad else 0)" % str(ROOT))
    assert subprocess.run([sys.executable, "-c", code]).returncode == 0
