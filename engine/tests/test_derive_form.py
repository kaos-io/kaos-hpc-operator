"""PRD kaos#1533 F-21 H4 and H5: the types, option defaults and leaf options `derive()` records, and the raw form
annotation the render emits. The seal (charts run_form.py) reads them as author data."""

import json
from pathlib import Path

import pytest
import yaml

from kubecore import authoring, derive_tree
from kubecore.step_contract import RAW_FORM_ANNOTATION


def tree(tmp_path: Path, monkeypatch, files: dict) -> None:
    for rel, text in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    monkeypatch.setenv("KUBECORE_CONFIG_DIR", str(tmp_path))


GROUPED = {
    "config.yaml": "defaults:\n  - _self_\n  - model: small\n  - optim: adam\n",
    "model/small.yaml": "width: 64\ndropout: 0.1\nname: tiny\n",
    "model/large.yaml": "width: 512\ndropout: 0.1\nname: big\nlayers: 12\n",
    "optim/adam.yaml": "lr: 0.001\nfused: true\n",
    "optim/sgd.yaml": "lr: 0.1\nfused: true\n",
}


def test_types_follow_the_kind_of_each_leaf(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, {"config.yaml": "defaults:\n  - _self_\n  - sec\n",
                                 "sec.yaml": "sec:\n  count: 3\n  rate: 0.5\n  flag: true\n  label: x\n  empty: null\n"
                                             "  sci: 1.0e-4\n"})
    form = derive_tree.derive()
    assert form.types == {"sec-count": "integer", "sec-rate": "number", "sec-flag": "boolean", "sec-label": "string",
                         "sec-empty": "string", "sec-sci": "number"}


def test_a_structured_enum_field_is_typed_enum(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, {"config.yaml": "defaults:\n  - _self_\n  - quantization_schema\n  - quantization\n",
                                 "quantization.yaml": "quantization:\n  image_size: 320\n"})
    form = derive_tree.derive()
    assert form.types["quantization-mode"] == "enum"
    assert form.types["quantization-image_size"] == "integer"


def test_option_defaults_hold_only_the_deltas(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, GROUPED)
    form = derive_tree.derive()
    assert form.option_defaults == {
        "model": {"small": {}, "large": {"model-width": "512", "model-name": "big", "model-layers": "12"}},
        "optim": {"adam": {}, "sgd": {"optim-lr": "0.1"}},
    }


def test_every_group_has_an_entry_even_when_nothing_differs(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, {"config.yaml": "defaults:\n  - _self_\n  - g: a\n",
                                 "g/a.yaml": "x: 1\n", "g/b.yaml": "x: 1\n"})
    assert derive_tree.derive().option_defaults == {"g": {"a": {}, "b": {}}}


def test_leaf_options_name_the_leaves_that_exist_under_some_options_only(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, GROUPED)
    form = derive_tree.derive()
    # model-layers is only in `large`, which is not the default: no parameter, so no leaf entry
    assert "model-layers" not in form.leaf_options
    assert form.leaf_options == {}
    tree(tmp_path, monkeypatch, {"model/small.yaml": "width: 64\ndropout: 0.1\nname: tiny\nwarmup: 5\n"})
    form = derive_tree.derive()
    assert form.leaf_options == {"model-warmup": {"group": "model", "options": ["small"]}}
    assert form.option_defaults["model"]["large"]["model-layers"] == "12"


def test_a_broken_option_fails_the_render(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, {**GROUPED, "optim/bad.yaml": "defaults:\n  - missing_thing\nlr: 1\n"})
    with pytest.raises(derive_tree.DeriveError):
        derive_tree.derive()


def test_two_derives_are_byte_identical(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, GROUPED)
    a, b = derive_tree.derive(), derive_tree.derive()
    dump = lambda f: json.dumps([f.types, f.option_defaults, f.leaf_options], sort_keys=True)  # noqa: E731
    assert dump(a) == dump(b)


# ---------------------------------------------------------------- H5

def render_app() -> dict:
    with authoring.pipeline("p") as p:
        authoring.step("one", kind="evaluate")
    return yaml.safe_load(p.wt.to_yaml())


def test_the_raw_form_annotation_is_on_the_render():
    wft = render_app()
    raw = json.loads(wft["metadata"]["annotations"][RAW_FORM_ANNOTATION])
    assert set(raw) == {"version", "types", "optionDefaults", "leafOptions"}
    assert raw["version"] == 1
    assert raw["types"]["experiment-name"] == "string"


def test_the_raw_form_text_is_compact_and_sorted(tmp_path, monkeypatch):
    tree(tmp_path, monkeypatch, GROUPED)
    text = render_app()["metadata"]["annotations"][RAW_FORM_ANNOTATION]
    raw = json.loads(text)
    assert text == json.dumps(raw, sort_keys=True, separators=(",", ":"))
    assert raw["optionDefaults"]["optim"]["sgd"] == {"optim-lr": "0.1"}


def test_a_raw_form_over_128_kib_fails_the_render(tmp_path, monkeypatch):
    big = "\n".join(f"  k{i}: {i}" for i in range(9000))
    tree(tmp_path, monkeypatch, {"config.yaml": "defaults:\n  - _self_\n  - sec\n", "sec.yaml": "sec:\n" + big + "\n"})
    with pytest.raises(authoring.AuthoringError, match="run form"):
        render_app()


def test_a_resolver_in_a_non_default_option_fails_the_render_and_is_not_resolved(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBE_SECRET", "s3cr3t")
    tree(tmp_path, monkeypatch, {"config.yaml": "defaults:\n  - _self_\n  - g: a\n",
                                 "g/a.yaml": "x: 1\n", "g/b.yaml": "x: ${oc.env:PROBE_SECRET}\n"})
    with pytest.raises(derive_tree.DeriveError, match="resolver") as exc:
        derive_tree.derive()
    assert "s3cr3t" not in str(exc.value)
