"""Platform side of datasets as repositories (PRD novelcore/kaos#1283 F-07).

These files are pinned by the platform CI for EVERY ML app: a run picks
`<dataset-repo>@<version>` (the `dataset` parameter), the HPC follower hands it to the controller,
and every result goes to `outputs-<app>` on `main`. The stage-in/stage-out code that read these
values moved to the controller's runner (PRD-HPC-1461 F-35).
"""
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))
from kubecore.enhance import enhance  # noqa: E402

CTX_PATH = ROOT / "kubecore/local-dev/pipeline-context.yaml"


# ---------------------------------------------------------------- render wiring

def _raw():
    return {
        "apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "metadata": {"name": "x"},
        "spec": {"entrypoint": "p", "arguments": {"parameters": [{"name": "dataset", "value": "dataset-default@latest"}]},
                 "templates": [
            {"name": "p", "dag": {"tasks": [{"name": "model-training", "template": "model-training"}]}},
            {"name": "model-training", "container": {"image": "i", "command": ["python", "-m", "train"],
                                                     "resources": {"requests": {"nvidia.com/gpu": 1}}}},
        ]},
    }


def test_results_go_to_the_apps_outputs_repository():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    ctx["app"] = "myapp"
    out = enhance(_raw(), ctx)
    step = next(t for t in out["spec"]["templates"] if t["name"] == "model-training")
    env = {e["name"]: e.get("value") for e in step["container"]["env"]}
    assert env["CHECKPOINT_BUCKET"] == "outputs-myapp"
    assert env["CHECKPOINT_PREFIX"] == "main/checkpoints"
    assert env["KUBECORE_RUN_ID"] == "{{workflow.name}}"
    assert env["KUBECORE_WORKFLOW_NAME"] == "{{workflow.name}}", "names the MLflow run"
    mr = next(t for t in out["spec"]["templates"] if t["name"] == "hpc-follower")
    menv = {e["name"]: e.get("value") for e in mr["container"]["env"]}
    assert menv["DATASET"] == "", "no compose step pinned the dataset: the HPC step asks for none"
    assert menv["OUTPUTS_REPO"] == "outputs-myapp" and menv["OUTPUTS_REF"] == "main"
    assert "DATASET_REPO" not in menv and "OUTPUTS_BRANCH" not in menv


def _with_compose(reads_dataset: bool) -> dict:
    """_raw() plus the platform's compose step as authoring renders it (data-lock output; --require per reads=)."""
    raw = _raw()
    raw["spec"]["templates"].append({
        "name": "compose-and-validate",
        "container": {"image": "i", "args": ["--require=dataset"] if reads_dataset else ["--require=train"]},
        "outputs": {"parameters": [{"name": "params"}, {"name": "data-lock"}]}})
    tasks = raw["spec"]["templates"][0]["dag"]["tasks"]
    tasks.insert(0, {"name": "compose-and-validate", "template": "compose-and-validate"})
    tasks[1]["depends"] = "compose-and-validate"
    return raw


def test_the_follower_gets_the_dataset_and_the_runs_data_lock_only_when_a_step_reads_the_dataset():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    out = enhance(_with_compose(reads_dataset=True), ctx)
    follower = next(t for t in out["spec"]["templates"] if t["name"] == "hpc-follower")
    env = {e["name"]: e.get("value") for e in follower["container"]["env"]}
    assert env["DATASET"] == "{{workflow.parameters.dataset}}"
    assert env["DATA_LOCK"] == "{{inputs.parameters.data-lock}}"
    twin = next(t for t in out["spec"]["templates"][0]["dag"]["tasks"] if t["name"] == "model-training-hpc")
    args = {p["name"]: p["value"] for p in twin["arguments"]["parameters"]}
    assert args["data-lock"] == "{{tasks.compose-and-validate.outputs.parameters.data-lock}}"
    assert "compose-and-validate" in twin["depends"], "Argo exposes a task's outputs only to tasks that depend on it"

    out = enhance(_with_compose(reads_dataset=False), ctx)
    follower = next(t for t in out["spec"]["templates"] if t["name"] == "hpc-follower")
    env = {e["name"]: e.get("value") for e in follower["container"]["env"]}
    assert env["DATASET"] == "", "a dataset in the config that no step reads is not asked of the HPC step"


def test_the_compose_step_has_no_hpc_twin_and_its_class_choice_offers_no_hpc_class():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    out = enhance(_with_compose(reads_dataset=True), ctx)
    names = [t["name"] for t in out["spec"]["templates"][0]["dag"]["tasks"]]
    assert "compose-and-validate-hpc" not in names and "model-training-hpc" in names
    assert not any(n.endswith("-meluxina") for n in names)
    compose_task = next(t for t in out["spec"]["templates"][0]["dag"]["tasks"] if t["name"] == "compose-and-validate")
    assert "when" not in compose_task, "the compose step always runs on the cluster"
    params = {p["name"]: p for p in out["spec"]["arguments"]["parameters"]}
    hpc = [c for c in params["model-training-class"]["enum"] if c.startswith("meluxina-")]
    assert hpc, "the fixture context has HPC classes"
    assert not [c for c in params["compose-and-validate-class"]["enum"] if c.startswith("meluxina-")]


def test_compose_gets_the_outputs_repository_as_platform_checkpoints():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    ctx["app"] = "myapp"
    raw = _raw()
    raw["spec"]["templates"].append({"name": "compose-and-validate", "container": {"image": "i", "args": []}})
    out = enhance(raw, ctx)
    args = next(t for t in out["spec"]["templates"] if t["name"] == "compose-and-validate")["container"]["args"]
    assert "platform.checkpoints.bucket=outputs-myapp" in args
    assert "platform.checkpoints.prefix=main/checkpoints" in args


def test_a_pipeline_without_a_dataset_parameter_runs_data_less_on_hpc():
    ctx = yaml.safe_load(CTX_PATH.read_text())
    raw = _raw()
    raw["spec"]["arguments"]["parameters"] = []
    mr = next(t for t in enhance(raw, ctx)["spec"]["templates"] if t["name"] == "hpc-follower")
    assert {e["name"]: e.get("value") for e in mr["container"]["env"]}["DATASET"] == ""
