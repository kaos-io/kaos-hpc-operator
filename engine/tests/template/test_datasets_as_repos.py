"""Platform side of datasets as repositories (PRD novelcore/kaos#1283 F-07).

These files are pinned by the platform CI for EVERY ML app: a run picks
`<dataset-repo>@<version>` for each input a step declares, the compose step pins it, the HPC follower
hands the step's context to the controller, and every result goes to `outputs-<app>` on `main`. The
stage-in/stage-out code moved to the controller's runner (PRD-HPC-1461 F-35).
"""
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))
from kubecore.enhance import enhance  # noqa: E402
from kubecore.step_contract import context_expression  # noqa: E402

CTX_PATH = ROOT / "kubecore/local-dev/pipeline-context.yaml"


# ---------------------------------------------------------------- render wiring

def _raw():
    return {
        "apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "metadata": {"name": "x"},
        "spec": {"entrypoint": "p", "arguments": {"parameters": [{"name": "dataset", "value": "dataset-default@latest"}]},
                 "templates": [
            {"name": "p", "dag": {"tasks": [{"name": "model-training", "template": "model-training", "arguments": {
                "parameters": [{"name": "kaos-context",
                                "value": context_expression("compose-and-validate", "model-training", [])}]}}]}},
            {"name": "model-training", "inputs": {"parameters": [{"name": "kaos-context"}]},
             "metadata": {"labels": {"platform.kubecore.io/step-kind": "train"}},
             "container": {"image": "i", "command": ["python", "-m", "app.entry"],
                           "resources": {"requests": {"nvidia.com/gpu": 1}}}},
        ]},
    }


def test_the_runner_is_told_the_apps_outputs_repository():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    ctx["app"] = "myapp"
    out = enhance(_raw(), ctx)
    step = next(t for t in out["spec"]["templates"] if t["name"] == "model-training")
    env = {e["name"]: e.get("value") for e in step["container"]["env"]}
    # the run, the app and the outputs repository reach a step in its context, not in variables of their own
    assert not {"CHECKPOINT_BUCKET", "CHECKPOINT_PREFIX", "KUBECORE_RUN_ID", "KUBECORE_WORKFLOW_NAME"} & set(env)
    mr = next(t for t in out["spec"]["templates"] if t["name"] == "hpc-follower")
    menv = {e["name"]: e.get("value") for e in mr["container"]["env"]}
    assert menv["KAOS_STEP_CONTEXT"] == "{{inputs.parameters.kaos-context}}"
    assert menv["OUTPUTS_REPO"] == "outputs-myapp" and menv["OUTPUTS_REF"] == "main"
    assert "DATASET_REPO" not in menv and "OUTPUTS_BRANCH" not in menv and "DATASET_PIN" not in menv


def _with_compose() -> dict:
    """_raw() plus the platform's compose step as authoring renders it (its outputs)."""
    raw = _raw()
    raw["spec"]["templates"].append({
        "name": "compose-and-validate", "container": {"image": "i", "args": []},
        "metadata": {"labels": {"platform.kubecore.io/step-kind": "control"}},
        "outputs": {"parameters": [{"name": "kaos-ledger"}, {"name": "kaos-context-model-training"}]}})
    tasks = raw["spec"]["templates"][0]["dag"]["tasks"]
    tasks.insert(0, {"name": "compose-and-validate", "template": "compose-and-validate"})
    tasks[1]["depends"] = "compose-and-validate"
    return raw


def test_the_follower_gets_the_steps_context_from_the_compose_step_never_a_run_parameter():
    """The run form's `dataset` can differ from what the compose step resolved (no version, an owner that is
    the own project): the HPC job is created from the step's context, the same value its pod would get."""
    ctx = yaml.safe_load(CTX_PATH.read_text())
    out = enhance(_with_compose(), ctx)
    follower = next(t for t in out["spec"]["templates"] if t["name"] == "hpc-follower")
    env = {e["name"]: e.get("value") for e in follower["container"]["env"]}
    assert env["KAOS_STEP_CONTEXT"] == "{{inputs.parameters.kaos-context}}"
    assert not {"DATASET", "DATASET_PIN", "DATA_LOCK", "STEP_OUTPUTS"} & set(env)
    assert "workflow.parameters.dataset" not in yaml.safe_dump(follower)
    assert follower["container"]["terminationMessagePolicy"] == "FallbackToLogsOnError"
    tasks = {t["name"]: t for t in out["spec"]["templates"][0]["dag"]["tasks"]}
    twin, own = tasks["model-training-hpc"], tasks["model-training"]
    args = {p["name"]: p["value"] for p in twin["arguments"]["parameters"]}
    assert args["kaos-context"] == next(p["value"] for p in own["arguments"]["parameters"] if p["name"] == "kaos-context")
    assert 'tasks["compose-and-validate"].outputs.parameters["kaos-context-model-training"]' in args["kaos-context"]
    assert args["step-command"] == '["python","-m","app.entry"]' and args["step-kind"] == "train"
    assert "compose-and-validate" in twin["depends"], "Argo exposes a task's outputs only to tasks that depend on it"


def test_a_task_that_already_has_the_hpc_variants_name_stops_the_render():
    """authoring refuses such step names; a hand-made template that has one anyway must not render two tasks
    with one name."""
    ctx = yaml.safe_load(CTX_PATH.read_text())
    raw = _with_compose()
    raw["spec"]["templates"].append({"name": "x", "container": {"image": "i", "command": ["c"]}})
    raw["spec"]["templates"][0]["dag"]["tasks"].append({"name": "model-training-hpc", "template": "x"})
    with pytest.raises(ValueError, match="already exists"):
        enhance(raw, ctx)


def test_the_compose_step_has_no_hpc_twin_and_its_class_choice_offers_no_hpc_class():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    out = enhance(_with_compose(), ctx)
    names = [t["name"] for t in out["spec"]["templates"][0]["dag"]["tasks"]]
    assert "compose-and-validate-hpc" not in names and "model-training-hpc" in names
    assert not any(n.endswith("-meluxina") for n in names)
    compose_task = next(t for t in out["spec"]["templates"][0]["dag"]["tasks"] if t["name"] == "compose-and-validate")
    assert "when" not in compose_task, "the compose step always runs on the cluster"
    params = {p["name"]: p for p in out["spec"]["arguments"]["parameters"]}
    hpc = [c for c in params["model-training-class"]["enum"] if c.startswith("meluxina-")]
    assert hpc, "the fixture context has HPC classes"
    assert not [c for c in params["compose-and-validate-class"]["enum"] if c.startswith("meluxina-")]


def test_a_task_without_a_context_cannot_get_an_hpc_variant():
    """Every step authoring renders passes its context. A hand-made template that does not would start a job
    the controller refuses, so the render stops instead."""
    ctx = yaml.safe_load(CTX_PATH.read_text())
    raw = _raw()
    del raw["spec"]["templates"][0]["dag"]["tasks"][0]["arguments"]
    with pytest.raises(ValueError, match="passes no kaos-context"):
        enhance(raw, ctx)
