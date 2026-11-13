"""The run form's machines and steps (PRD 1533/F-21 and 1533/F-22): the mapping from the pipeline context and the
class offers, checked against the schema's own definitions.
Run:  python -m pytest tests/ -v
"""
import copy
import json
import subprocess
import sys
from pathlib import Path

import jsonschema
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance, run_form  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
SCHEMA = json.loads((ROOT.parent / "docs" / "run-form.schema.json").read_text())
FORBIDDEN = {"tolerationKey", "tolerationValue", "maxNodeCount", "diskSizeGb", "account", "user", "scratch", "home",
             "controller", "followerImage", "scope", "limits"}


def _validator(name):
    return jsonschema.Draft202012Validator({"$ref": "#/$defs/" + name, "$defs": SCHEMA["$defs"]})


def _ctx(hpc=True):
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    ctx["hpc"]["enabled"] = hpc
    return ctx


def _keys(node):
    if isinstance(node, dict):
        for k, v in node.items():
            yield k
            yield from _keys(v)
    elif isinstance(node, list):
        for v in node:
            yield from _keys(v)


def _enhanced(ctx):
    with authoring.pipeline("t") as p:
        load = authoring.step("load", kind="ingest", inputs=["dataset"], produces=[authoring.dataset("loaded")])
        train = authoring.step("train", kind="train", needs=[load], gpu=True, walltime="12h", checkpoint_every="10m",
                               produces=[authoring.model("m")])
        authoring.step("report", kind="evaluate", needs=[train])
    wft = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)
    steps = enhance.step_templates(wft["spec"])
    return wft, steps


# ------------------------------------------------------------------------------------------- machines

def test_machines_validate_and_leak_nothing():
    for hpc in (True, False):
        out = run_form.machines(_ctx(hpc))
        for m in out:
            _validator("machine").validate(m)
        assert not FORBIDDEN & set(_keys(out))


def test_cluster_machines_are_the_catalog_plus_missing_tier_defaults_without_duplicates():
    ctx = _ctx(hpc=False)
    ctx["computeClasses"]["all"] = [c for c in ctx["computeClasses"]["all"] if c["name"] != "gpu-t4"]
    out = run_form.machines(ctx)
    assert [m["name"] for m in out] == ["cpu-standard", "kubecorecpu", "gpu-t4"]
    t4 = out[2]
    assert t4["accelerator"] == {"type": "nvidia-tesla-t4", "count": 1}
    assert t4["machineType"] == "n1-standard-8" and t4["kind"] == "cluster" and "hpc" not in t4
    assert t4["allocatable"] == {"cpu": 7, "memoryGiB": 25, "ephemeralStorageGiB": None}
    assert out[0]["accelerator"] is None and out[0]["tier"] == "cpu"


def test_a_missing_allocatable_comes_from_the_tier_default():
    ctx = _ctx(hpc=False)
    del ctx["computeClasses"]["all"][0]["allocatable"]
    assert run_form.machines(ctx)[0]["allocatable"]["cpu"] == 7


def test_hpc_machines_only_when_enabled_and_share_the_node():
    assert all(m["kind"] == "cluster" for m in run_form.machines(_ctx(hpc=False)))
    out = {m["name"]: m for m in run_form.machines(_ctx())}
    gpu = out["meluxina-gpu"]
    assert gpu["kind"] == "hpc" and gpu["machineType"] is None
    assert gpu["accelerator"] == {"type": None, "count": 4}
    assert gpu["allocatable"] == {"cpu": 128, "memoryGiB": 512, "ephemeralStorageGiB": None}
    assert gpu["hpc"] == {"provider": "meluxina", "partition": "gpu", "qos": "default", "gpus": 4,
                          "sharesNode": True}
    assert out["meluxina-cpu"]["accelerator"] is None


def test_hpc_max_time_limit_is_copied_when_the_class_has_one():
    ctx = _ctx()
    ctx["hpc"]["classes"][0]["maxTimeLimitMinutes"] = 2880
    out = run_form.machines(ctx)
    hpc = next(m for m in out if m["name"] == "meluxina-gpu")["hpc"]
    assert hpc["maxTimeLimitMinutes"] == 2880
    _validator("machine").validate(next(m for m in out if m["name"] == "meluxina-gpu"))


def test_a_long_description_is_truncated_not_failed_on_a_utf8_boundary():
    ctx = _ctx(hpc=False)
    ctx["computeClasses"]["all"][0]["description"] = "é" * 600
    desc = run_form.machines(ctx)[0]["description"]
    assert desc.endswith("…") and len(desc.encode("utf-8")) <= 512 and len(desc) <= 512
    assert set(desc[:-1]) == {"é"}
    ctx["computeClasses"]["all"][0]["description"] = "x" * 512
    assert run_form.machines(ctx)[0]["description"] == "x" * 512
    assert run_form.truncate_text("x" * 513) == "x" * 509 + "…"
    _validator("machine").validate(run_form.machines(ctx)[0])


# ---------------------------------------------------------------------------------------------- steps

def _steps(ctx=None):
    ctx = ctx or _ctx()
    wft, steps = _enhanced(ctx)
    params = wft["spec"]["arguments"]["parameters"]
    args = next(s for s in steps if s["name"] == "compose-and-validate")["container"]["args"]
    offers = json.loads(args[args.index("--class-offers") + 1])
    return run_form.steps(steps, offers, ctx, params), offers, steps, params


def test_steps_match_class_offer_and_validate():
    out, offers, _, _ = _steps()
    assert [s["name"] for s in out] == ["load", "train", "report"]
    for s in out:
        _validator("step").validate(s)
        assert s["offered"] == offers[s["name"]]["offered"]
        assert s["refused"] == offers[s["name"]]["refused"]
        assert s["classField"] == s["name"] + "-class"
    by = {s["name"]: s for s in out}
    assert by["load"]["kind"] == "ingest" and by["train"]["kind"] == "train"
    assert by["train"]["default"] == "gpu-t4" and by["report"]["default"] == "cpu-standard"
    assert by["train"]["walltimeField"] == "train-walltime"
    assert by["train"]["checkpointField"] == "train-checkpoint-every"
    assert "checkpointField" not in by["report"]
    assert by["load"]["promoteFields"] == ["promote-loaded"]
    assert "promoteFields" not in by["train"]
    assert all(len(v) == 2 for v in by["load"]["refused"].values())
    assert not FORBIDDEN & set(_keys(out))


def test_without_hpc_no_hpc_class_is_offered():
    out, _, _, _ = _steps(_ctx(hpc=False))
    assert not any(n.startswith("meluxina") for s in out for n in s["offered"])


def test_a_pinned_step_has_no_class_field_and_one_offer():
    ctx = _ctx()
    with authoring.pipeline("t") as p:
        authoring.step("one", kind="evaluate")
    raw = yaml.safe_load(p.wt.to_yaml())
    next(t for t in raw["spec"]["templates"] if t["name"] == "one")["metadata"]["annotations"] = {
        "platform.kubecore.io/compute-class": "kubecorecpu"}
    wft = enhance.enhance(raw, ctx)
    steps = enhance.step_templates(wft["spec"])
    out = run_form.steps(steps, {"one": None}, ctx, wft["spec"]["arguments"]["parameters"])
    assert out[0]["classField"] is None and out[0]["offered"] == ["kubecorecpu"]
    assert out[0]["default"] == "kubecorecpu" and out[0]["refused"] == {}
    _validator("step").validate(out[0])


def test_the_argo_parameters_are_unchanged_by_the_builders():
    ctx = _ctx()
    out, offers, steps, params = _steps(ctx)
    before = copy.deepcopy(params)
    run_form.machines(ctx)
    run_form.steps(steps, offers, ctx, params)
    assert params == before


def test_run_form_imports_no_render_library():
    code = ("import sys, kubecore.run_form; bad = [m for m in sys.modules if m.split('.')[0] in "
            "('hera', 'hydra', 'omegaconf')]; sys.exit(1 if bad else 0)")
    assert subprocess.run([sys.executable, "-c", code], cwd=ROOT).returncode == 0


@pytest.mark.parametrize("name", ["machine", "step"])
def test_the_definitions_exist(name):
    assert name in SCHEMA["$defs"]


def test_an_unpinned_step_missing_from_offers_is_refused_naming_the_step():
    ctx = _ctx()
    _, offers, steps, params = _steps(ctx)
    name = next(iter(offers))
    with pytest.raises(run_form.RunFormError, match="step %r is not pinned" % name):
        run_form.steps(steps, {k: v for k, v in offers.items() if k != name}, ctx, params)
    with pytest.raises(run_form.RunFormError, match="step %r is not pinned" % name):
        run_form.steps(steps, {**offers, name: None}, ctx, params)
