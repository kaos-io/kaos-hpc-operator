"""PRD kaos#1533 F-21 H1 / engine issue 80: the trusted enhancer owns the platform's metadata.

A raw WorkflowTemplate from the untrusted render may carry any label or annotation; none under
`ui.kubecore.io/` or `platform.kubecore.io/` survives enhance, and the namespace is the context's.
"""
import copy
import runpy

import yaml

from kubecore import enhance
from kubecore.step_contract import RAW_FORM_ANNOTATION
from paths import APP, ENGINE

CTX = yaml.safe_load((ENGINE / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
PLATFORM = "platform.kubecore.io/"
UI = "ui.kubecore.io/"


def _raw():
    ns = runpy.run_path(str(APP / "pipeline.py"), run_name="__pipeline__")
    return yaml.safe_load(ns["p"].wt.to_yaml())


def _forge(wft):
    meta = wft["metadata"]
    meta["namespace"] = "ml-someone-else"
    meta["labels"] = {
        PLATFORM + "project": "victim-project",
        PLATFORM + "app": "victim-app",
        PLATFORM + "managed-by": "attacker",
        PLATFORM + "unknown-key": "x",
        UI + "other": "y",
        "keep.example.com/label": "kept",
    }
    meta["annotations"] = {
        PLATFORM + "generated-by": "forged",
        PLATFORM + "cost-tracking": "disabled",
        PLATFORM + "engine-version": "9.9.9",
        PLATFORM + "source-commit": "deadbeef",
        UI + "run-form": '{"forged": true}',
        RAW_FORM_ANNOTATION: '{"fields": []}',
        "keep.example.com/annotation": "kept",
    }
    wm = wft["spec"].setdefault("workflowMetadata", {})
    wm["labels"] = {PLATFORM + "project": "victim-project", UI + "x": "1", "keep.example.com/l": "kept"}
    wm["annotations"] = {PLATFORM + "app": "victim-app", UI + "run-form": "forged", "keep.example.com/a": "kept"}
    return wft


def _no_reserved(mapping):
    return [k for k in (mapping or {}) if k.startswith((UI, PLATFORM))]


def test_strip_returns_raw_form_and_removes_every_reserved_key():
    wft = _forge(_raw())
    assert enhance.strip_render_metadata(wft) == '{"fields": []}'
    for m in (wft["metadata"]["labels"], wft["metadata"]["annotations"],
              wft["spec"]["workflowMetadata"]["labels"], wft["spec"]["workflowMetadata"]["annotations"]):
        assert _no_reserved(m) == []
    assert wft["metadata"]["labels"] == {"keep.example.com/label": "kept"}
    assert wft["spec"]["workflowMetadata"]["labels"] == {"keep.example.com/l": "kept"}


def test_strip_without_raw_form_returns_none():
    wft = _raw()
    wft["metadata"]["annotations"] = {UI + "run-form": "x"}
    assert enhance.strip_render_metadata(wft) is None
    assert enhance.strip_render_metadata({"metadata": {}, "spec": {}}) is None


def test_forged_values_never_survive_enhance():
    wft = enhance.enhance(_forge(_raw()), copy.deepcopy(CTX))
    meta = wft["metadata"]
    assert meta["namespace"] == CTX["namespace"]
    assert meta["labels"][PLATFORM + "project"] == CTX["project"]
    assert meta["labels"][PLATFORM + "app"] == CTX["app"]
    assert meta["labels"][PLATFORM + "managed-by"] == "kubecore-ci"
    assert meta["annotations"][PLATFORM + "generated-by"] == "ml-ci-build/hera-render"
    assert meta["annotations"][PLATFORM + "cost-tracking"] == "enabled"
    for key in (PLATFORM + "engine-version", PLATFORM + "source-commit", UI + "run-form", RAW_FORM_ANNOTATION,
                PLATFORM + "unknown-key", UI + "other"):
        assert key not in meta["labels"] and key not in meta["annotations"]
    assert meta["labels"]["keep.example.com/label"] == "kept"
    wm = wft["spec"].get("workflowMetadata") or {}
    assert _no_reserved(wm.get("labels")) == [] and _no_reserved(wm.get("annotations")) == []
    assert "victim" not in yaml.safe_dump(wft)


def test_enhance_metadata_assigns_over_preset_values():
    wft = _raw()
    wft["metadata"]["namespace"] = "elsewhere"
    wft["metadata"]["labels"] = {PLATFORM + "project": "p2"}
    enhance.enhance_metadata(wft, CTX)
    assert wft["metadata"]["namespace"] == CTX["namespace"]
    assert wft["metadata"]["labels"][PLATFORM + "project"] == CTX["project"]


def test_labels_from_keys_under_the_platform_prefixes_are_removed():
    """Argo's controller writes each labelsFrom key onto the Workflow, so a forged one would stamp it."""
    wft = _raw()
    wft["spec"].setdefault("workflowMetadata", {})["labelsFrom"] = {
        PLATFORM + "submitted-by": {"expression": "'attacker'"},
        UI + "run-form": {"expression": "'x'"},
        "keep.example.com/from": {"expression": "workflow.name"},
    }
    out = enhance.enhance(wft, copy.deepcopy(CTX))
    assert out["spec"]["workflowMetadata"]["labelsFrom"] == {"keep.example.com/from": {"expression": "workflow.name"}}


def test_strip_render_metadata_leaves_a_template_without_labels_from_alone():
    wft = _raw()
    wft["spec"].setdefault("workflowMetadata", {}).pop("labelsFrom", None)
    enhance.strip_render_metadata(wft)
    assert "labelsFrom" not in wft["spec"]["workflowMetadata"]
