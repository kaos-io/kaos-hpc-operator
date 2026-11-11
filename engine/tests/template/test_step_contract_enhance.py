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


def test_an_app_name_an_hpc_job_cannot_hold_fails_the_render():
    ctx = _context()
    ctx["app"] = "a" * 56
    with pytest.raises(enhance.EnhanceError) as exc:
        _enhanced(lambda: authoring.step("one", kind="train"), ctx)
    assert str(exc.value) == (f"the app name '{'a' * 56}' is 56 characters; an app that runs pipelines may have at "
                              "most 55. Create the app under a shorter name.")
    ctx["app"] = "a" * 55
    assert _enhanced(lambda: authoring.step("one", kind="train"), ctx)


def test_the_definition_budget_counts_what_the_workflow_engine_writes():
    """Its encoder writes &, < and > as six bytes each, and other characters as UTF-8."""
    assert template_bytes({"a": "x"}) == len('{"a":"x"}')
    assert template_bytes({"a": "a && b > c"}) == len('{"a":"a && b > c"}') + 3 * 5
    assert template_bytes({"a": "é"}) == len('{"a":"é"}'.encode())


def _pipeline():
    load = authoring.step("load", kind="ingest", inputs=["dataset"])
    prepare = authoring.step("prepare", kind="transform", needs=[load],
                             when="{{workflow.parameters.experiment-name}} == yes")
    authoring.step("train", kind="train", gpu=True, resumable=True, needs=[load, prepare],
                   produces=[authoring.model("model")])
    authoring.step("alone", kind="evaluate")


def test_every_task_an_expression_reads_is_in_the_depends_of_the_task_that_reads_it():
    """The workflow engine shows a task's outputs only to tasks that name it in `depends`: a pair (a step
    and its HPC variant), a conditional step, and a step with no needs= (which still reads compose)."""
    import re
    tasks = _tasks(_enhanced(_pipeline))
    assert {"prepare-hpc", "train-hpc", "alone-hpc"} <= set(tasks) and "load-hpc" not in tasks
    for name, task in tasks.items():
        if name == "compose-and-validate":
            continue
        context = next(p["value"] for p in task["arguments"]["parameters"] if p["name"] == "kaos-context")
        read = set(re.findall(r'tasks\["([^"]+)"\]', context))
        named = set(re.findall(r"[a-z0-9][a-z0-9-]*(?=\.[A-Z]|\b)", task["depends"]))
        assert read and read <= named, (name, read - named)
    assert set(re.findall(r'tasks\["([^"]+)"\]', next(
        p["value"] for p in tasks["train-hpc"]["arguments"]["parameters"] if p["name"] == "kaos-context"))) == {
        "compose-and-validate", "load", "prepare", "prepare-hpc"}


# What a rendered step or follower definition refers to besides its context. Each is a short value the
# platform or the form supplies (an image reference, a class name, a resource quantity, the run's name);
# none is sized by a step's data. A reference that is not listed here fails the test: decide whether its
# value is bounded before adding it.
KNOWN_REFERENCES = {
    "inputs.parameters.kaos-context",
    "workflow.name", "workflow.uid",
    "inputs.parameters.gpus", "inputs.parameters.step-name", "inputs.parameters.image",
    "inputs.parameters.step-command", "inputs.parameters.step-kind", "inputs.parameters.time-limit",
    "inputs.parameters.deadline-seconds", "inputs.parameters.hpc-class", "inputs.parameters.resumable-lead",
    "inputs.parameters.resumable", "inputs.parameters.idle-timeout",
}
KNOWN_FORM_REFERENCES = ("image-", "-class", "-cpu", "-mem", "-disk")   # workflow.parameters.<step>... of the step


def test_a_definition_refers_to_nothing_sized_at_run_time_but_its_context():
    import re
    wft = _enhanced(_pipeline)
    found = set()
    for template in wft["spec"]["templates"]:
        if "container" not in template or template["name"] == "compose-and-validate":
            continue
        text = yaml.safe_dump(template)
        found |= set(re.findall(r"\{\{\s*=?\s*([A-Za-z0-9_.\-\[\]']+?)\s*\}\}", text))
    form = {r for r in found if r.startswith("workflow.parameters.")}
    assert found - form <= KNOWN_REFERENCES, found - form - KNOWN_REFERENCES
    for reference in form:
        name = reference[len("workflow.parameters."):]
        assert name.startswith("image-") or name.endswith(KNOWN_FORM_REFERENCES[1:]), reference
