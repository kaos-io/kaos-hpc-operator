"""The raw data the render hands the run form (1533/F-21): leaf types, what each group option changes, which
leaves only some options have, and config/_form.yaml. The Argo parameters themselves do not change."""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import derive_tree, run_form  # noqa: E402
from kubecore.derive_tree import DeriveError  # noqa: E402

TREE = {
    "config.yaml": "defaults:\n  - _self_\n  - model: small\n  - optim: sgd\nname: run\ntrain:\n  epochs: 3\n"
                   "  lr: 0.1\n  amp: false\n  note:\n",
    "model/small.yaml": "width: 8\ndepth: 2\ndropout: 0.1\n",
    "model/big.yaml": "width: 64\ndepth: 2\ndropout: 0.1\nheads: 8\n",
    "model/huge.yaml": "width: 128\ndepth: 4\ndropout: 0.1\nheads: 16\n",
    "optim/sgd.yaml": "momentum: 0.9\n",
    "optim/adam.yaml": "momentum: 0.9\n",
}


def _tree(tmp_path, monkeypatch, files=None):
    for rel, text in {**TREE, **(files or {})}.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    monkeypatch.setattr(derive_tree, "CONFIG_DIR", tmp_path)
    return tmp_path


def _form(tmp_path, monkeypatch, files=None):
    _tree(tmp_path, monkeypatch, files)
    return derive_tree.derive()


def test_types_follow_each_leaf_and_groups_are_enums(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch)
    assert form.types["model"] == "enum" and form.types["optim"] == "enum"
    assert form.types["train-epochs"] == "integer"
    assert form.types["train-lr"] == "number"
    assert form.types["train-amp"] == "boolean"
    assert form.types["train-note"] == "string"
    assert form.types["name"] == "string"
    assert set(form.types) == {p.name for p in form.parameters if p.name != "config"}


def test_the_fixture_app_has_no_groups_and_types_for_every_field():
    form = derive_tree.derive()   # the fixture app's tree (tests/app)
    assert form.option_defaults == {} and form.leaf_options == {} and form.overrides == {}
    assert set(form.types) == {p.name for p in form.parameters if p.name != "config"}


def test_option_defaults_hold_only_what_differs_from_the_default_option(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch)
    assert form.option_defaults["model"] == {
        "big": {"model-width": "64"},
        "huge": {"model-width": "128", "model-depth": "4"},
    }


def test_every_group_has_an_entry_even_when_empty(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch)
    assert form.option_defaults["optim"] == {}
    assert set(form.option_defaults) == {"model", "optim"}


def test_leaf_options_name_their_group_and_only_partial_leaves(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch)
    # model-heads is not in the default option (small), so it is not a form field at all
    assert form.leaf_options == {}


def test_a_leaf_only_some_options_have(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch, {"model/small.yaml": "width: 8\nheads: 2\n",
                                         "model/big.yaml": "width: 64\nheads: 8\n",
                                         "model/huge.yaml": "width: 128\n"})
    assert form.leaf_options == {"model-heads": {"group": "model", "options": ["big", "small"]}}
    assert form.option_defaults["model"] == {"big": {"model-width": "64", "model-heads": "8"},
                                             "huge": {"model-width": "128"}}


def test_nested_group_names_are_mapped(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch, {
        "config.yaml": "defaults:\n  - _self_\n  - train/optim: a\n",
        "train/optim/a.yaml": "lr: 0.1\nwarm: 1\n",
        "train/optim/b.yaml": "lr: 0.2\n",
    })
    assert form.option_defaults == {"train-optim": {"b": {"train-optim-lr": "0.2"}}}
    assert form.leaf_options == {"train-optim-warm": {"group": "train-optim", "options": ["a"]}}


def test_the_argo_parameters_are_unchanged_by_the_raw_data(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch)
    names = [p.name for p in form.parameters]
    assert names[:2] == ["model", "optim"] and names[-1] == "config"
    assert form.tokens[0] == "model={{workflow.parameters.model}}"


def test_the_raw_data_validates_against_the_template(tmp_path, monkeypatch):
    form = _form(tmp_path, monkeypatch)
    params = [{"name": p.name, "value": p.value, **({"enum": p.enum} if p.enum else {})} for p in form.parameters]
    raw = {"version": 1, "types": form.types, "optionDefaults": form.option_defaults,
           "leafOptions": form.leaf_options, "when": {}, "overrides": form.overrides}
    run_form.validate_raw(raw, params, [])
    fields = run_form.fields(params, [], raw)
    assert {f["name"]: f["source"] for f in fields}["optim"] == "group"


# ------------------------------------------------------------------------------------------ _form.yaml

VALID = """\
fields:
  train-epochs: {label: Epochs, tier: advanced, minimum: 1, maximum: 9, group: tuning}
groups:
  - {id: tuning, label: Tuning, tier: basic, order: 1}
"""


def _write_form(tmp_path, monkeypatch, text, name="_form.yaml"):
    _tree(tmp_path, monkeypatch)
    (tmp_path / name).write_bytes(text if isinstance(text, bytes) else text.encode())


def test_a_valid_form_file_is_returned(tmp_path, monkeypatch):
    _write_form(tmp_path, monkeypatch, VALID)
    got = derive_tree.derive().overrides
    assert got == {"fields": {"train-epochs": {"label": "Epochs", "tier": "advanced", "minimum": 1, "maximum": 9,
                                               "group": "tuning"}},
                   "groups": [{"id": "tuning", "label": "Tuning", "tier": "basic", "order": 1}]}


def test_no_form_file_is_no_overrides(tmp_path, monkeypatch):
    _tree(tmp_path, monkeypatch)
    assert derive_tree.read_form_overrides() == {}


def test_an_empty_form_file_is_no_overrides(tmp_path, monkeypatch):
    _write_form(tmp_path, monkeypatch, "# nothing yet\n")
    assert derive_tree.read_form_overrides() == {}


def test_a_symlinked_form_file_is_refused(tmp_path, monkeypatch):
    _tree(tmp_path, monkeypatch)
    target = tmp_path / "elsewhere.txt"
    target.write_text(VALID)
    os.symlink(target, tmp_path / "_form.yaml")
    with pytest.raises(DeriveError, match="regular file, not a link"):
        derive_tree.read_form_overrides()


def test_a_form_file_that_is_not_a_regular_file_is_refused(tmp_path, monkeypatch):
    _tree(tmp_path, monkeypatch)
    (tmp_path / "_form.yaml").mkdir()
    with pytest.raises(DeriveError, match="regular file"):
        derive_tree.read_form_overrides()


def test_a_form_file_over_64_kib_is_refused(tmp_path, monkeypatch):
    _write_form(tmp_path, monkeypatch, "fields: {}\n" + "# " + "x" * 65536 + "\n")
    with pytest.raises(DeriveError, match="over 64 KiB"):
        derive_tree.read_form_overrides()


def test_a_form_file_of_exactly_64_kib_is_read(tmp_path, monkeypatch):
    text = "fields: {}\n"
    _write_form(tmp_path, monkeypatch, text + "#" * (65536 - len(text) - 1) + "\n")
    assert derive_tree.read_form_overrides() == {"fields": {}}


@pytest.mark.parametrize("text", [
    "fields: &a {train-epochs: {label: A}}\ngroups: []\n",
    "x: &a [1]\nfields: *a\n",
    "fields:\n  train-epochs: *x\n",
])
def test_anchors_and_aliases_are_refused(tmp_path, monkeypatch, text):
    _write_form(tmp_path, monkeypatch, text)
    with pytest.raises(DeriveError, match="anchors or aliases"):
        derive_tree.read_form_overrides()


@pytest.mark.parametrize("text, message", [
    ("colours: {}\n", "unknown keys"),
    ("fields:\n  train-epochs: {colour: red}\n", "unknown keys"),
    ("groups:\n  - {id: g, colour: red}\n", "unknown keys"),
    ("fields:\n  train-epochs: {pattern: '^a$'}\n", "pattern is not supported"),
    ("fields:\n  train-epochs: {group: info}\n", "never shows"),
    ("fields:\n  train-epochs: {group: platform}\n", "never shows"),
    ("fields: [a]\n", "mapping"),
    ("fields:\n  train-epochs: 3\n", "must be a mapping"),
    ("groups: {a: 1}\n", "must be a list"),
    ("groups: [a]\n", "must be a mapping"),
    ("- a\n- b\n", "must be a mapping"),
    ("fields: {a: [\n", "not valid YAML"),
    (b"fields: \xff\xfe\n", "not valid YAML"),
])
def test_bad_form_files_fail_the_build(tmp_path, monkeypatch, text, message):
    _write_form(tmp_path, monkeypatch, text)
    with pytest.raises(DeriveError, match=message):
        derive_tree.derive()


def test_a_moved_field_in_a_shown_group_is_allowed(tmp_path, monkeypatch):
    _write_form(tmp_path, monkeypatch, "fields:\n  train-epochs: {group: advanced}\n")
    assert derive_tree.read_form_overrides()["fields"]["train-epochs"] == {"group": "advanced"}


def test_no_comment_is_read_from_the_form_file(tmp_path, monkeypatch):
    _write_form(tmp_path, monkeypatch, "# train.epochs: a comment that must not become a description\n"
                                       "fields:\n  train-epochs: {label: Epochs}  # train.epochs\n")
    assert "train.epochs" not in derive_tree._comments()
    form = derive_tree.derive()
    assert {p.name: p.description for p in form.parameters}["train-epochs"] == ""
