"""The step contract through the enhancer (PRD kaos#1911 F-03): the per-kind rules the engine owns, the HPC
variant of a step on the same contract as its pod, and the size of a rendered step.
Run:  python -m pytest tests/ -v
"""
import copy
import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from helpers import valid  # noqa: E402
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
    wft = _enhanced(lambda: authoring.step("one", kind=kind, **valid(authoring, kind)))
    choice = next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "one-class")
    offered = [c for c in choice["enum"] if c.startswith("meluxina-")]
    assert bool(offered) is on_hpc
    assert ("meluxina-* classes" in choice["description"]) is on_hpc
    assert ("one-hpc" in _tasks(wft)) is on_hpc
    if not on_hpc:
        assert "when" not in _tasks(wft)["one"], "it always runs on the cluster"


def test_the_compose_step_is_offered_no_hpc_class():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate"))
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
        load = authoring.step("load", kind="ingest", produces=[authoring.dataset("loaded")])
        prepare = authoring.step("prepare", kind="evaluate", needs=[load])
        authoring.step("train", kind="evaluate", needs=[load, prepare])
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
        authoring.step("train", kind="evaluate")
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
        _enhanced(lambda: authoring.step("one", kind="evaluate"), ctx)
    assert str(exc.value) == (f"the app name '{'a' * 56}' is 56 characters; an app that runs pipelines may have at "
                              "most 55. Create the app under a shorter name.")
    ctx["app"] = "a" * 55
    assert _enhanced(lambda: authoring.step("one", kind="evaluate"), ctx)


def test_the_definition_budget_counts_what_the_workflow_engine_writes():
    """Its encoder writes &, < and > as six bytes each, and other characters as UTF-8."""
    assert template_bytes({"a": "x"}) == len('{"a":"x"}')
    assert template_bytes({"a": "a && b > c"}) == len('{"a":"a && b > c"}') + 3 * 5
    assert template_bytes({"a": "é"}) == len('{"a":"é"}'.encode())


def _pipeline():
    load = authoring.step("load", kind="ingest", inputs=["dataset"], produces=[authoring.dataset("loaded")])
    prepare = authoring.step("prepare", kind="evaluate", needs=[load],
                             when="experiment-name == yes")
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
# platform or the form supplies (an image reference, a class name, a number the engine computes, the run's
# name); none is sized by a step's data. A reference that is not listed here fails the test: decide whether its
# value is bounded before adding it.
KNOWN_REFERENCES = {
    "inputs.parameters.kaos-context",
    "workflow.name", "workflow.uid",
    "inputs.parameters.gpus", "inputs.parameters.cpu", "inputs.parameters.mem", "inputs.parameters.disk",
    "inputs.parameters.step-name", "inputs.parameters.image",
    "inputs.parameters.step-command", "inputs.parameters.step-kind", "inputs.parameters.time-limit",
    "inputs.parameters.deadline-seconds", "inputs.parameters.hpc-class", "inputs.parameters.resumable-lead",
    "inputs.parameters.resumable", "inputs.parameters.idle-timeout", "inputs.parameters.deadline",
    "inputs.parameters.checkpoint-every",
}


def references(text: str) -> set:
    """Every `{{...}}` of `text` and every name an expression `{{= ... }}` reads, as dotted names:
    `workflow.parameters['x-class']` is `workflow.parameters.x-class`."""
    import re
    found = set()
    for body in re.findall(r"\{\{(.+?)\}\}", text, re.S):
        for root, tail in re.findall(r"\b(inputs\.parameters|workflow\.parameters|workflow\.name|workflow\.uid|tasks)"
                                     r"((?:\.[A-Za-z0-9_-]+|\[['\"][^'\"]+['\"]\])*)", body):
            found.add(root + re.sub(r"\[['\"]([^'\"]+)['\"]\]", r".\1", tail))
    return found


def test_the_reference_finder_sees_names_inside_expressions():
    expression = "{{=workflow.parameters['train-class'] in ['gpu-t4'] ? '1' : tasks[\"a-b\"].outputs.parameters[\"x\"]}}"
    assert references(expression) == {"workflow.parameters.train-class", "tasks.a-b.outputs.parameters.x"}
    assert references("a {{ inputs.parameters.cpu }} b {{workflow.name}}") == {"inputs.parameters.cpu", "workflow.name"}
    assert references("{{=sprig.trimSuffix(\"}\", inputs.parameters[\"kaos-context\"])}}") == {"inputs.parameters.kaos-context"}


def test_a_definition_refers_to_nothing_sized_at_run_time_but_its_context():
    wft = _enhanced(_pipeline)
    found = set()
    for template in wft["spec"]["templates"]:
        if "container" not in template or template["name"] == "compose-and-validate":
            continue
        found |= references(yaml.safe_dump(template, width=10 ** 6))
    form = {r for r in found if r.startswith("workflow.parameters.")}
    assert found - form <= KNOWN_REFERENCES, found - form - KNOWN_REFERENCES
    for reference in form:
        name = reference[len("workflow.parameters."):]
        assert name.startswith("image-") or name.endswith("-class"), reference   # nothing else is a form field of a step


# ---------------------------------------------------------------- a step gets the node of its class

def _task(wft, name):
    return _tasks(wft)[name]


def _arg(task, name):
    return next(p["value"] for p in task["arguments"]["parameters"] if p["name"] == name)


def _template(wft, name):
    return next(t for t in wft["spec"]["templates"] if t["name"] == name)


def test_the_run_form_has_no_cpu_memory_or_disk_field():
    wft = _enhanced(_pipeline)
    names = [p["name"] for p in wft["spec"]["arguments"]["parameters"]]
    assert not [n for n in names if n.endswith(("-cpu", "-mem", "-disk"))], names
    assert "train-class" in names and "alone-class" in names
    info = next(p["value"] for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "pipeline-info")
    assert "{step}-cpu" not in info and "{step}-mem" not in info and "dial" not in info


def test_a_cpu_step_gets_what_a_node_of_its_class_offers_and_the_numbers_follow_the_class_it_is_given():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate"))
    template = _template(wft, "one")
    resources = template["container"]["resources"]
    assert resources["requests"]["cpu"] == resources["limits"]["cpu"] == "7"
    assert resources["requests"]["memory"] == resources["limits"]["memory"] == "27Gi", "the default class, cpu-standard"
    assert {p["name"]: p.get("default") for p in template["inputs"]["parameters"]} == {
        "kaos-context": None, "deadline": "14400", "cpu": "7", "mem": "27Gi", "disk": "10Gi"}
    patch = yaml.safe_load(template["podSpecPatch"])["containers"][0]
    assert patch == {"name": "main", "resources": {
        "requests": {"cpu": "{{inputs.parameters.cpu}}", "memory": "{{inputs.parameters.mem}}",
                     "ephemeral-storage": "{{inputs.parameters.disk}}"},
        "limits": {"cpu": "{{inputs.parameters.cpu}}", "memory": "{{inputs.parameters.mem}}"}}}
    task = _task(wft, "one")
    assert _arg(task, "cpu") == ('{{=({"cpu-standard": "7", "kubecorecpu": "7", "gpu-t4": "7"}'
                                 "[workflow.parameters['one-class']]) ?? \"7\"}}")
    assert _arg(task, "mem") == ('{{=({"cpu-standard": "27Gi", "kubecorecpu": "27Gi", "gpu-t4": "25Gi"}'
                                 "[workflow.parameters['one-class']]) ?? \"27Gi\"}}")


def test_a_gpu_step_gets_its_gpu_and_the_node_numbers_in_one_patch():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate", gpu=True))
    template = _template(wft, "one")
    assert template["container"]["resources"]["requests"]["memory"] == "25Gi", "the default of a gpu step is gpu-t4"
    assert {p["name"]: p.get("default") for p in template["inputs"]["parameters"]} == {
        "kaos-context": None, "deadline": "14400", "cpu": "7", "mem": "25Gi", "disk": "10Gi",
        "gpus": "1"}
    patch = yaml.safe_load(template["podSpecPatch"])["containers"]
    assert len(patch) == 1 and patch[0]["resources"]["requests"] == {
        "cpu": "{{inputs.parameters.cpu}}", "memory": "{{inputs.parameters.mem}}",
        "ephemeral-storage": "{{inputs.parameters.disk}}", "nvidia.com/gpu": "{{inputs.parameters.gpus}}"}
    assert patch[0]["resources"]["limits"]["nvidia.com/gpu"] == "{{inputs.parameters.gpus}}"
    assert '"?? "25Gi"' not in _arg(_task(wft, "one"), "mem")
    assert _arg(_task(wft, "one"), "mem").endswith('?? "25Gi"}}')


def test_a_step_pinned_to_one_class_has_plain_resources_and_no_runtime_numbers():
    with authoring.pipeline("t") as p:
        authoring.step("pinned", kind="evaluate", gpu=True)
    raw = yaml.safe_load(p.wt.to_yaml())
    _template(raw, "pinned")["metadata"]["annotations"] = {"platform.kubecore.io/compute-class": "gpu-t4"}
    wft = enhance.enhance(raw, _context())
    template = _template(wft, "pinned")
    assert template["container"]["resources"]["requests"] == {
        "cpu": "7", "memory": "25Gi", "ephemeral-storage": "10Gi", "nvidia.com/gpu": "1"}
    assert "podSpecPatch" not in template
    assert [p["name"] for p in template["inputs"]["parameters"]] == ["kaos-context", "deadline"]
    assert not any(p["name"] in ("cpu", "mem") for p in _task(wft, "pinned")["arguments"]["parameters"])
    assert "pinned-class" not in [p["name"] for p in wft["spec"]["arguments"]["parameters"]]


def test_an_hpc_class_is_not_in_the_table_and_its_cluster_task_never_runs():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate"))
    assert "meluxina" not in _arg(_task(wft, "one"), "cpu")
    assert "hasPrefix(workflow.parameters['one-class'], 'meluxina-')" in _task(wft, "one")["when"]
    follower = _template(wft, "hpc-follower")
    assert "cpu" not in [p["name"] for p in follower["inputs"]["parameters"]], "HPC is sized by its own class"


def test_a_pool_with_one_class_per_tier_and_a_pool_with_one_class_only():
    ctx = _context()
    ctx["computeClasses"]["all"] = [c for c in ctx["computeClasses"]["all"] if c["name"] == "cpu-standard"]
    ctx["computeClasses"].pop("gpu")
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate"), ctx)
    assert _arg(_task(wft, "one"), "cpu") == "{{=({\"cpu-standard\": \"7\"}[workflow.parameters['one-class']]) ?? \"7\"}}"


def test_a_class_the_platform_cannot_size_fails_the_render_naming_it():
    ctx = _context()
    ctx["computeClasses"]["all"].append({"name": "arm-big", "tier": "cpu", "machineType": "c7g.4xlarge"})
    with pytest.raises(enhance.EnhanceError) as exc:
        _enhanced(lambda: authoring.step("one", kind="evaluate"), ctx)
    assert str(exc.value) == (
        "the pool offers the compute class 'arm-big' (machine type 'c7g.4xlarge'), but the platform cannot say how "
        "much of such a node a pod may use, so a step could not be sized on it. The operator knows the sizes of the "
        "machine types in its table only: ask the platform team to add the machine type.")
    ctx["computeClasses"]["all"].append({"name": "arm-huge", "tier": "cpu", "machineType": "c7g.8xlarge"})
    with pytest.raises(enhance.EnhanceError, match="classes 'arm-big' .*'arm-huge' .*sized on them"):
        _enhanced(lambda: authoring.step("one", kind="evaluate"), ctx)


def _with_usable_disk(ctx, **gib):
    """Put `allocatable.ephemeralStorageGiB` on the named classes (the tier entries too)."""
    entries = list(ctx["computeClasses"]["all"]) + [ctx["computeClasses"][t] for t in ("cpu", "gpu")]
    for cls in entries:
        if cls["name"] in gib:
            cls["allocatable"]["ephemeralStorageGiB"] = gib[cls["name"]]
    return ctx


def _disk_patch(template):
    return yaml.safe_load(template["podSpecPatch"])["containers"][0]["resources"]


def test_disk_is_refused_in_step_and_never_a_step_option():
    with pytest.raises(authoring.AuthoringError, match="disk= is gone"):
        _enhanced(lambda: authoring.step("big", kind="evaluate", disk="40Gi"))


def test_a_class_without_a_usable_disk_gets_the_flat_ten_gi_and_is_not_refused():
    wft = _enhanced(lambda: authoring.step("small", kind="evaluate"))
    template = _template(wft, "small")
    assert template["container"]["resources"]["requests"]["ephemeral-storage"] == "10Gi"
    assert _disk_patch(template)["requests"]["ephemeral-storage"] == "{{inputs.parameters.disk}}"
    assert "ephemeral-storage" not in _disk_patch(template).get("limits", {}), "a request, never a limit"
    assert {p["name"]: p.get("default") for p in template["inputs"]["parameters"]}["disk"] == "10Gi"
    assert _arg(_task(wft, "small"), "disk").endswith('?? "10Gi"}}')


def test_a_class_with_a_usable_disk_gets_it_less_the_image_reserve_per_class_picked_on_the_form():
    ctx = _with_usable_disk(_context(), **{"cpu-standard": 43, "gpu-t4": 15})
    ctx["computeClasses"]["all"].append({"name": "kubecorecpu-x", "tier": "cpu", "machineType": "e2-standard-8",
                                         "allocatable": {"cpu": 7, "memoryGiB": 27}})
    wft = _enhanced(lambda: (authoring.step("a", kind="evaluate"), authoring.step("g", kind="evaluate", gpu=True)), ctx)
    expression = _arg(_task(wft, "a"), "disk")
    assert '"cpu-standard": "33Gi"' in expression and '"gpu-t4": "5Gi"' in expression
    assert '"kubecorecpu": "10Gi"' in expression and '"kubecorecpu-x": "10Gi"' in expression, "no disk: the flat default"
    assert expression.endswith('?? "33Gi"}}'), "the default class's disk when the class is not in the table"
    assert _arg(_task(wft, "g"), "disk").endswith('?? "5Gi"}}')
    assert {p["name"]: p.get("default") for p in _template(wft, "a")["inputs"]["parameters"]}["disk"] == "33Gi"
    assert "meluxina" not in expression


def test_the_image_reserve_never_asks_for_more_than_the_node_gives():
    ctx = _with_usable_disk(_context(), **{"cpu-standard": 8})   # 8 - 10 < the flat default: still the value less the reserve
    assert enhance.IMAGE_RESERVE_GIB == 10
    assert enhance.node_disks(ctx)["cpu-standard"] == "1Gi", "max(1, 8 - 10)"
    ctx = _with_usable_disk(_context(), **{"cpu-standard": 15})
    assert enhance.node_disks(ctx)["cpu-standard"] == "5Gi"


def test_a_step_pinned_to_one_class_has_that_classs_disk_as_a_static_request():
    ctx = _with_usable_disk(_context(), **{"gpu-t4": 43})
    with authoring.pipeline("t") as p:
        authoring.step("pinned", kind="evaluate", gpu=True)
    raw = yaml.safe_load(p.wt.to_yaml())
    _template(raw, "pinned")["metadata"]["annotations"] = {"platform.kubecore.io/compute-class": "gpu-t4"}
    template = _template(enhance.enhance(raw, ctx), "pinned")
    assert template["container"]["resources"]["requests"]["ephemeral-storage"] == "33Gi"
    assert "disk" not in [p["name"] for p in template["inputs"]["parameters"]]


# The ephemeral-storage key the operator publishes: read in one place, `node_disks`.
def test_the_usable_disk_key_is_read_from_allocatable_ephemeral_storage_gib_only():
    ctx = _context()
    ctx["computeClasses"]["all"][0]["allocatable"]["ephemeral-storage"] = 99
    ctx["computeClasses"]["all"][0]["diskSizeGb"] = 99
    assert enhance.node_disks(ctx)["cpu-standard"] == "10Gi"


# A step that produces a dataset: no HPC class until the jobs there hold the app's own rights (PRD kaos#1911 F-40).

def test_a_step_that_produces_a_dataset_is_offered_no_hpc_class_and_gets_no_hpc_variant():
    wft = _enhanced(lambda: authoring.step("prep", kind="transform", produces=[authoring.dataset("clean")]))
    choice = next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "prep-class")
    assert not [c for c in choice["enum"] if c.startswith("meluxina-")]
    assert "meluxina-* classes" not in choice["description"]
    assert "prep-hpc" not in _tasks(wft)
    assert "when" not in _tasks(wft)["prep"], "it always runs on the cluster"


def test_a_step_that_produces_a_model_or_an_artifact_is_unaffected():
    wft = _enhanced(lambda: authoring.step(
        "train", kind="train", produces=[authoring.model("m"), authoring.artifact("report")]))
    choice = next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "train-class")
    assert [c for c in choice["enum"] if c.startswith("meluxina-")]
    assert "train-hpc" in _tasks(wft)


def test_a_step_that_produces_a_dataset_and_is_pinned_to_an_hpc_class_fails_the_render():
    with authoring.pipeline("t") as p:
        authoring.step("prep", kind="transform", produces=[authoring.dataset("clean")])
    raw = yaml.safe_load(p.wt.to_yaml())
    _template(raw, "prep")["metadata"]["annotations"] = {"platform.kubecore.io/compute-class": "meluxina-cpu"}
    with pytest.raises(enhance.EnhanceError) as exc:
        enhance.enhance(raw, _context())
    assert str(exc.value) == (
        "step 'prep': a step that produces a dataset runs on the cluster for now; HPC (Slurm) classes are offered "
        "to it once jobs there hold the app's own rights.")


def test_the_render_refuses_a_produced_dataset_the_lake_cannot_hold_in_this_app():
    ctx = _context()
    ctx["app"] = "a" * 30

    def build(length):
        return lambda: authoring.step("prepare", kind="transform", produces=[authoring.dataset("n" * length)])
    assert _enhanced(build(17), ctx)                   # 30 + 1 + 17 = 48: the lake's limit
    with pytest.raises(enhance.EnhanceError) as exc:
        _enhanced(build(18), ctx)                      # 49
    assert str(exc.value) == (f"step 'prepare': dataset('{'n' * 18}') in app '{'a' * 30}' is the repository "
                              f"'dataset-{'a' * 30}-{'n' * 18}', and the lake allows 3 to 48 characters after 'dataset-': "
                              f"'{'a' * 30}-{'n' * 18}' has 49. Shorten the dataset name ('{'n' * 18}') or the app's name "
                              f"('{'a' * 30}').")


def test_the_step_table_lists_the_short_name_and_the_context_the_repository():
    wft = _enhanced(lambda: authoring.step("prepare", kind="transform", produces=[authoring.dataset("clean")]))
    compose = _template(wft, "compose-and-validate")["container"]["args"]
    table = json.loads(compose[compose.index("--steps") + 1])
    assert table[0]["produces"] == [{"name": "clean", "type": "dataset"}], "the table has no repository: compose names it"


# The classes a step is offered on the run form: GPU classes only when its code declares gpu=True (owner ruling 2026-11-12).

def _class_choice(wft, step):
    return next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == f"{step}-class")


def _cluster_classes(choice):
    return [c for c in choice["enum"] if not c.startswith("meluxina-")]


def test_a_step_without_gpu_is_offered_only_the_cpu_classes_of_the_cluster_and_its_hpc_classes():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate"))
    choice = _class_choice(wft, "one")
    assert _cluster_classes(choice) == ["cpu-standard", "kubecorecpu"]
    assert [c for c in choice["enum"] if c.startswith("meluxina-")], "its HPC classes are as before"
    assert choice["value"] == "cpu-standard"


def test_the_compose_step_is_offered_only_the_cpu_classes_of_the_cluster():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate", gpu=True))
    assert _cluster_classes(_class_choice(wft, "compose-and-validate")) == ["cpu-standard", "kubecorecpu"]


def test_a_gpu_step_is_still_offered_the_gpu_and_the_cpu_classes_with_its_default_unchanged():
    wft = _enhanced(lambda: authoring.step("one", kind="evaluate", gpu=True))
    choice = _class_choice(wft, "one")
    assert _cluster_classes(choice) == ["cpu-standard", "kubecorecpu", "gpu-t4"]
    assert choice["value"] == "gpu-t4"


def test_a_pool_that_declares_only_gpu_classes_still_offers_a_step_without_gpu_what_exists():
    ctx = _context()
    cc = ctx["computeClasses"]
    cc["all"] = [c for c in cc["all"] if c.get("tier") == "gpu"]
    del cc["cpu"]
    choice = _class_choice(_enhanced(lambda: authoring.step("one", kind="evaluate"), ctx), "one")
    assert _cluster_classes(choice) == ["gpu-t4"] and choice["value"] == "gpu-t4"


def test_a_pool_whose_default_cpu_class_is_missing_from_the_catalog_still_offers_it_first():
    ctx = _context()
    cc = ctx["computeClasses"]
    cc["all"] = [c for c in cc["all"] if c["name"] != cc["cpu"]["name"]]
    choice = _class_choice(_enhanced(lambda: authoring.step("one", kind="evaluate"), ctx), "one")
    assert _cluster_classes(choice) == ["cpu-standard", "kubecorecpu"]


def test_the_form_says_which_classes_a_step_may_pick():
    info = next(p for p in _enhanced(lambda: authoring.step("one", kind="evaluate"))["spec"]["arguments"]["parameters"]
                if p["name"] == "pipeline-info")["value"]
    assert "Only a step that declares gpu=True is offered the GPU classes" in info
