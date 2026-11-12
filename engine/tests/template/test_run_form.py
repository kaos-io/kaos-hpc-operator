"""The run form speaks to the developer, not about the platform (owner ruling 2026-11-12, PRD kaos#1911).
Run:  python -m pytest tests/ -v
"""
import copy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, derive_tree, enhance  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def _form(account=None):
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    if account:
        ctx["hpc"]["account"] = account
    with authoring.pipeline("t") as p:
        load = authoring.step("load", kind="ingest", inputs=["dataset"], produces=[authoring.dataset("loaded")])
        train = authoring.step("train", kind="train", needs=[load], gpu=True, walltime="12h", checkpoint_every="10m",
                               produces=[authoring.model("m")])
        authoring.step("report", kind="evaluate", needs=[train])
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)["spec"]["arguments"]["parameters"]


# ------------------------------------------------------------------ descriptions are the comments of config/

def test_a_setting_is_described_by_the_comment_at_the_end_of_its_line():
    form = {p["name"]: p for p in _form()}
    assert form["experiment-name"]["description"] == "experiment name (used for tracking + artifact path prefixes)"
    assert form["experiment-description"]["description"] == "optional free text stored with the run"


def test_comment_lines_directly_above_a_setting_describe_it():
    form = {p["name"]: p for p in _form()}
    assert form["dataset"]["description"].startswith("The dataset this run uses: <dataset-repo>@<version>.")


def test_comments_are_read_from_the_text_of_the_config_files():
    text = ("# above one\n# above two\na:\n  b: 1  # beside b\n  c: 2\n\n# apart\n\n  d: 3\n"
            "e: 'x # not a comment'\nf: \"y\" # beside f\n")
    assert derive_tree._file_comments(text) == {"a": "above one above two", "a.b": "beside b", "f": "beside f"}
    assert derive_tree._file_comments("t:\n  lr: 0.1  # rate\n", "train") == {"train.t.lr": "rate"}


def test_a_setting_with_no_comment_has_an_empty_description_never_config_dotted_name(tmp_path, monkeypatch):
    (tmp_path / "config.yaml").write_text("defaults:\n  - _self_\n  - plain\n")
    (tmp_path / "plain.yaml").write_text("plain:\n  lr: 0.1\n  steps: 5  # how many\n")
    monkeypatch.setattr(derive_tree, "CONFIG_DIR", tmp_path)
    form = {p.name: p for p in derive_tree.derive().parameters}
    assert (form["plain-lr"].description or "") == ""
    assert form["plain-steps"].description == "how many"
    assert not any((p.description or "").startswith("Config: ") for p in form.values())


def test_the_extra_yaml_field_is_in_plain_words():
    form = {p["name"]: p for p in _form()}
    assert form["config"]["description"] == "Extra settings for this run, as YAML. They replace the values above."


# ------------------------------------------------------------------ platform fields

def test_the_platform_endpoints_and_the_compose_class_are_not_fields():
    names = [p["name"] for p in _form()]
    for gone in ("mlflow-endpoint", "lakefs-endpoint", "lakefs-repo", "compose-and-validate-class"):
        assert gone not in names


def test_the_image_fields_stay_for_the_ci_but_are_last_and_say_the_platform_sets_them():
    form = _form()
    images = [p for p in form if p["name"].startswith("image-")]
    assert [p["name"] for p in images] == ["image-compose-and-validate", "image-load", "image-train", "image-report"]
    assert all(p["description"] == "Set by the platform. Do not change." for p in images)
    assert form[-len(images):] == images


# ------------------------------------------------------------------ order and the text at the top

def test_the_form_is_in_the_order_a_developer_reads_it():
    names = [p["name"] for p in _form()]
    assert names == [
        "pipeline-info", "experiment-name", "experiment-description", "dataset",
        "load-class", "load-walltime",
        "train-class", "train-walltime", "train-checkpoint-every",
        "report-class", "report-walltime",
        "promote-loaded", "config",
        "image-compose-and-validate", "image-load", "image-train", "image-report"]


def test_pipeline_info_lists_the_developers_steps_only_and_names_no_account_or_provider():
    info = next(p for p in _form(account="p201342") if p["name"] == "pipeline-info")["value"]
    assert "Steps: load -> train -> report" in info
    assert "compose" not in info.lower()
    for word in ("p201342", "account", "MeluXina", "MELUXINA", "leave it unchanged", "informational", "KubePool"):
        assert word not in info
    assert "HPC (Slurm)" in info
