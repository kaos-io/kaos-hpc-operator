"""The step contract through the enhancer (PRD kaos#1911 F-03): the per-kind rules the engine owns, the HPC
variant of a step on the same contract as its pod, and the size of a rendered step.
Run:  python -m pytest tests/ -v
"""
import copy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance  # noqa: E402
from kubecore.step_contract import TEMPLATE_CAP, result_of, template_bytes  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
KINDS_ON_HPC = {"ingest": False, "transform": True, "train": True, "evaluate": True, "publish": False,
                "control": False}


def _context():
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    return ctx


def _enhanced(build, ctx=None):
    with authoring.pipeline("t") as p:
        build()
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx or _context())


def _tasks(wft):
    return {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}


@pytest.mark.parametrize("kind, on_hpc", sorted(KINDS_ON_HPC.items()))
def test_a_kind_that_never_runs_on_hpc_is_offered_no_hpc_class_and_gets_no_hpc_variant(kind, on_hpc):
    wft = _enhanced(lambda: authoring.step("one", kind=kind))
    choice = next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "one-class")
    offered = [c for c in choice["enum"] if c.startswith("meluxina-")]
    assert bool(offered) is on_hpc
    assert ("meluxina-* classes" in choice["description"]) is on_hpc
    assert ("one-hpc" in _tasks(wft)) is on_hpc
    if not on_hpc:
        assert "when" not in _tasks(wft)["one"], "it always runs on the cluster"


def test_the_compose_step_is_offered_no_hpc_class():
    wft = _enhanced(lambda: authoring.step("one", kind="train"))
    choice = next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "compose-and-validate-class")
    assert not [c for c in choice["enum"] if c.startswith("meluxina-")]
    assert "compose-and-validate-hpc" not in _tasks(wft)


def test_the_follower_pod_carries_the_steps_real_kind():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate"))
    twin = {p["name"]: p["value"] for p in _tasks(wft)["one-hpc"]["arguments"]["parameters"]}
    assert twin["step-kind"] == "evaluate"
    follower = next(t for t in wft["spec"]["templates"] if t["name"] == "hpc-follower")
    assert follower["metadata"]["labels"]["platform.kubecore.io/step-kind"] == "{{inputs.parameters.step-kind}}"


def test_a_step_reads_the_result_of_a_step_that_may_run_on_hpc_from_the_pair():
    """Exactly one of `prepare` and `prepare-hpc` ran. The consumer, and the consumer's own HPC variant,
    read whichever did; a step that never runs on HPC is read directly."""
    def build():
        load = authoring.step("load", kind="ingest")
        prepare = authoring.step("prepare", kind="transform", needs=[load])
        authoring.step("train", kind="train", needs=[load, prepare])
    tasks = _tasks(_enhanced(build))
    assert "load-hpc" not in tasks
    for name in ("train", "train-hpc"):
        context = next(p["value"] for p in tasks[name]["arguments"]["parameters"] if p["name"] == "kaos-context")
        assert result_of("prepare", "prepare-hpc") in context, name
        assert result_of("load") in context and "load-hpc" not in context, name
        assert "prepare-hpc" in tasks[name]["depends"], "a task sees the outputs of the tasks it depends on only"
    assert result_of("prepare", "prepare-hpc") == (
        'sprig.default(sprig.default("{\\"version\\":1,\\"status\\":\\"absent\\"}", '
        'tasks["prepare-hpc"].outputs.parameters["kaos-result"]), tasks["prepare"].outputs.parameters["kaos-result"])')


def test_a_rendered_step_is_far_under_the_size_its_definition_may_have():
    def build():
        authoring.step("train", kind="train", gpu=True, resumable=True, inputs=["dataset"],
                       produces=[authoring.model("model")])
    wft = _enhanced(build)
    sizes = {t["name"]: template_bytes(t) for t in wft["spec"]["templates"] if t["name"] in ("train", "hpc-follower")}
    assert all(size < TEMPLATE_CAP // 3 for size in sizes.values()), sizes


def test_a_step_whose_rendered_definition_is_over_the_limit_fails_the_render_naming_it():
    with authoring.pipeline("t") as p:
        authoring.step("train", kind="train")
    raw = yaml.safe_load(p.wt.to_yaml())
    step = next(t for t in raw["spec"]["templates"] if t["name"] == "train")
    step["container"]["env"].append({"name": "BIG", "value": "x" * TEMPLATE_CAP})
    with pytest.raises(enhance.EnhanceError) as exc:
        enhance.enhance(raw, _context())
    message = str(exc.value)
    assert message.startswith("step 'train': its rendered definition is ") and "; the limit is 24576." in message
