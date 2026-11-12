"""A class a step is not offered is refused when the run starts, not only hidden on the form (owner ruling
2026-11-12, PRD kaos#1911). The form's choice and the check come from one function (enhance.class_offer).
Run:  python -m pytest tests/ -v
"""
import copy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, compose, enhance  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def _enhanced(**step):
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    with authoring.pipeline("t") as p:
        authoring.step("one", **step)
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)


def _compose(wft):
    return next(t for t in wft["spec"]["templates"] if t["name"] == "compose-and-validate")["container"]["args"]


def _offers(wft):
    args = _compose(wft)
    import json
    return json.loads(args[args.index("--class-offers") + 1])


def _refusal(wft, chosen):
    args = _compose(wft)
    table = json = __import__("json").loads(args[args.index("--steps") + 1])
    with pytest.raises(compose.ComposeError) as exc:
        compose.run_classes(table, [f"one={chosen}"], args[args.index("--class-offers") + 1])
    return str(exc.value)


def test_the_form_and_the_check_are_given_by_one_offer():
    wft = _enhanced(kind="evaluate")
    form = next(p for p in wft["spec"]["arguments"]["parameters"] if p["name"] == "one-class")
    assert form["enum"] == _offers(wft)["one"]["offered"]
    args = _compose(wft)
    assert args[args.index("--class") + 1] == "one={{workflow.parameters.one-class}}"
    assert "--class" in args and not any(a.startswith("compose-and-validate=") for a in args)


def test_a_gpu_class_for_a_step_that_does_not_declare_gpu_is_refused():
    assert _refusal(_enhanced(kind="evaluate"), "gpu-t4") == (
        "step 'one' cannot run on class 'gpu-t4': it is a GPU machine and the step does not declare gpu=True. "
        "Add gpu=True to the step in pipeline.py to run it on a GPU machine.")


def test_a_gpu_step_may_run_on_a_gpu_class_and_on_a_cpu_class():
    wft = _enhanced(kind="train", gpu=True, produces=[authoring.model("m")])
    args = _compose(wft)
    table = __import__("json").loads(args[args.index("--steps") + 1])
    for chosen in ("gpu-t4", "cpu-standard", "meluxina-gpu"):
        compose.run_classes(table, [f"one={chosen}"], args[args.index("--class-offers") + 1])


@pytest.mark.parametrize("kind", ["ingest", "publish", "control"])
def test_an_hpc_class_for_a_step_that_never_runs_on_hpc_is_refused(kind):
    kw = {"produces": [authoring.dataset("clean")]} if kind == "ingest" else {}
    wft = _enhanced(kind=kind, **kw)
    message = _refusal(wft, "meluxina-cpu")
    assert message.startswith(f"step 'one' cannot run on class 'meluxina-cpu': a {kind} step never runs on HPC (Slurm). "
                              "Choose one of: cpu-standard")


def test_an_hpc_class_for_a_step_that_produces_a_dataset_is_refused():
    wft = _enhanced(kind="transform", produces=[authoring.dataset("clean")])
    assert _refusal(wft, "meluxina-cpu").startswith(
        "step 'one' cannot run on class 'meluxina-cpu': a step that produces a dataset runs on the cluster for now. "
        "Choose one of: ")


def test_a_name_that_is_no_class_at_all_is_refused():
    assert _refusal(_enhanced(kind="evaluate"), "gpu-x9").startswith(
        "step 'one' cannot run on class 'gpu-x9': it is not a machine class of this cluster. Choose one of: ")


def test_an_offered_class_passes():
    wft = _enhanced(kind="evaluate")
    args = _compose(wft)
    table = __import__("json").loads(args[args.index("--steps") + 1])
    for chosen in ("cpu-standard", "meluxina-cpu"):
        compose.run_classes(table, [f"one={chosen}"], args[args.index("--class-offers") + 1])


def test_a_class_for_a_step_the_pipeline_does_not_have_names_the_form_field():
    wft = _enhanced(kind="evaluate")
    args = _compose(wft)
    table = __import__("json").loads(args[args.index("--steps") + 1])
    with pytest.raises(compose.ComposeError, match=r"^ghost-class: the pipeline has no step 'ghost'"):
        compose.run_classes(table, ["ghost=cpu-standard"], args[args.index("--class-offers") + 1])
