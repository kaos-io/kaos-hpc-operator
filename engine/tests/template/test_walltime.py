"""walltime= is the step's time limit wherever it runs (owner ruling 2026-11-12, PRD kaos#1911).

On the cluster it is the step's deadline; on an HPC class it is the job's limit (and the twin's deadline adds
the queue allowance). A step that declares none keeps the default deadline.
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
from kubecore.run_settings import DEFAULT_WALLTIME  # noqa: E402
from helpers import evaluate, valid  # noqa: E402
from kubecore.meluxina import DEFAULT_TIME_LIMIT_MINUTES  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def _context():
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    return ctx


def _enhanced(**keywords):
    with authoring.pipeline("t") as p:
        kind = keywords.pop("kind", "evaluate")
        authoring.step("one", kind=kind, **{**valid(authoring, kind), **keywords})
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), _context())


def _template(wft, name):
    return next(t for t in wft["spec"]["templates"] if t["name"] == name)


def _tasks(wft):
    return {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}


def _arguments(wft, task):
    return {p["name"]: p["value"] for p in _tasks(wft)[task]["arguments"]["parameters"]}


def _form(wft):
    return {p["name"]: p for p in wft["spec"]["arguments"]["parameters"]}


@pytest.mark.parametrize("walltime, minutes", [("10m", 10), ("2h", 120), ("90", 90)])
def test_the_form_value_is_the_deadline_of_the_step_on_the_cluster(walltime, minutes):
    wft = _enhanced(walltime=walltime)
    assert _template(wft, "one")["activeDeadlineSeconds"] == "{{inputs.parameters.deadline}}"
    # the code value is the default of the form field and of the template's input
    assert _form(wft)["one-walltime"]["value"] == walltime
    assert next(p for p in _template(wft, "one")["inputs"]["parameters"] if p["name"] == "deadline")["default"] \
        == str(minutes * 60)
    # the run uses what the person typed, not the code value
    assert evaluate(_arguments(wft, "one")["deadline"], {"one": {"walltime": 45}}) == 45 * 60


@pytest.mark.parametrize("walltime", ["10m", "2h", "90"])
def test_the_form_value_is_the_limit_of_the_job_on_the_hpc_twin(walltime):
    wft = _enhanced(walltime=walltime)
    twin = _arguments(wft, "one-hpc")
    assert evaluate(twin["time-limit"], {"one": {"walltime": 45}}) == 45
    assert evaluate(twin["deadline-seconds"], {"one": {"walltime": 45}}) == 6 * (45 + 480) * 60


@pytest.mark.parametrize("gpu", [False, True])
def test_a_step_with_no_walltime_has_the_field_with_default_4h_and_a_14400_second_deadline(gpu):
    wft = _enhanced(gpu=gpu)       # a GPU step is no longer capped at 90 minutes
    assert _form(wft)["one-walltime"]["value"] == "4h"
    assert _form(wft)["one-walltime"]["description"] == (
        "The longest the step may run, as minutes or hours: 90m, 12h. Default from pipeline.py.")
    one = _template(wft, "one")
    assert next(p for p in one["inputs"]["parameters"] if p["name"] == "deadline")["default"] == "14400"
    assert one["activeDeadlineSeconds"] == "{{inputs.parameters.deadline}}"
    assert evaluate(_arguments(wft, "one")["deadline"], {"one": {"walltime": 240}}) == 14400
    assert evaluate(_arguments(wft, "one")["deadline"], {"one": {"walltime": 600}}) == 36000   # the form overrides
    assert evaluate(_arguments(wft, "one-hpc")["time-limit"], {"one": {"walltime": 240}}) == DEFAULT_TIME_LIMIT_MINUTES


def test_the_default_of_a_step_with_no_walltime_is_the_hpc_default():
    assert DEFAULT_WALLTIME == "4h" and DEFAULT_TIME_LIMIT_MINUTES == 240


def test_a_walltime_in_code_is_the_default_of_the_field_and_the_deadline():
    wft = _enhanced(walltime="10m")
    assert _form(wft)["one-walltime"]["value"] == "10m"
    assert next(p for p in _template(wft, "one")["inputs"]["parameters"] if p["name"] == "deadline")["default"] == "600"


def test_the_compose_step_keeps_a_short_fixed_limit_and_has_no_field():
    wft = _enhanced()
    compose = _template(wft, "compose-and-validate")
    assert compose["activeDeadlineSeconds"] == enhance.COMPOSE_DEADLINE_SECONDS == 1800
    assert "compose-and-validate-walltime" not in _form(wft)
    assert "compose-and-validate" not in [a.split("=")[0] for a in compose["container"]["args"] if a.endswith("-walltime}}")]


@pytest.mark.parametrize("kind", ["ingest", "publish", "control"])
def test_a_step_that_never_runs_on_hpc_may_declare_walltime(kind):
    wft = _enhanced(kind=kind, walltime="20m")
    assert _template(wft, "one")["activeDeadlineSeconds"] == "{{inputs.parameters.deadline}}"


def test_the_form_gets_a_field_per_declared_keyword_with_the_code_value_as_default():
    wft = _enhanced(walltime="12h", checkpoint_every="20m")
    form = _form(wft)
    assert form["one-walltime"]["value"] == "12h"
    assert form["one-walltime"]["description"] == (
        "The longest the step may run, as minutes or hours: 90m, 12h. Default from pipeline.py.")
    assert form["one-checkpoint-every"]["value"] == "20m"
    assert form["one-checkpoint-every"]["description"] == (
        "How often the 'one' step's checkpoint is saved: minutes or hours, for example 10m or 1h.")
    args = _template(wft, "compose-and-validate")["container"]["args"]
    assert ["--walltime", "one={{workflow.parameters.one-walltime}}"] == args[args.index("--walltime"):][:2]
    assert ["--checkpoint-every", "one={{workflow.parameters.one-checkpoint-every}}"] == \
        args[args.index("--checkpoint-every"):][:2]
    assert args[args.index("--settings-output") + 1] == "/work/kaos-run-settings.json"
    outputs = _template(wft, "compose-and-validate")["outputs"]["parameters"]
    assert {"name": "kaos-run-settings", "valueFrom": {"path": "/work/kaos-run-settings.json"}} in outputs


def test_a_checkpoint_every_of_false_has_no_field():
    assert "one-checkpoint-every" not in _form(_enhanced(checkpoint_every=False))


def test_the_checkpoint_copies_follow_the_form_value():
    wft = _enhanced(checkpoint_every="20m", gpu=True)
    sidecar = _template(wft, "one")["sidecars"][0]
    env = {e["name"]: e.get("value") for e in sidecar["env"]}
    assert env["CKPT_EVERY_MIN"] == "{{inputs.parameters.checkpoint-every}}"
    assert next(p for p in _template(wft, "one")["inputs"]["parameters"] if p["name"] == "checkpoint-every")["default"] == "20"
    assert evaluate(_arguments(wft, "one")["checkpoint-every"], {"one": {"checkpoint-every": 30}}) == 30


# ------------------------------------------------------------------ compose checks the form values

@pytest.mark.parametrize("value, minutes", [("10m", 10), ("2h", 120), ("90", 90)])
def test_compose_turns_the_form_values_into_minutes(value, minutes):
    from kubecore import compose
    steps = [{"name": "one"}]
    assert compose.run_settings(steps, [f"one={value}"], [f"one={value}"]) == \
        {"one": {"walltime": minutes, "checkpoint-every": minutes}}


@pytest.mark.parametrize("value", ["", "0", "0m", "1d", "soon", "-5", "1.5h"])
def test_compose_refuses_a_walltime_or_checkpoint_value_that_is_not_minutes_or_hours(value):
    from kubecore import compose
    with pytest.raises(compose.ComposeError) as walltime:
        compose.run_settings([{"name": "one"}], [f"one={value}"], [])
    assert str(walltime.value) == (f"one-walltime is {value!r}. Write minutes or hours, for example 90, 90m or "
                                   f"12h: at least one minute, no other unit.")
    with pytest.raises(compose.ComposeError) as every:
        compose.run_settings([{"name": "one"}], [], [f"one={value}"])
    assert str(every.value).startswith(f"one-checkpoint-every is {value!r}.")


def test_the_checkpoint_interval_reaches_the_step_in_its_context():
    from kubecore import compose
    step = {"name": "one", "kind": "train", "reads": [], "inputs": [], "needs": [], "produces": []}
    out = compose.step_contexts([step], "r", "a", "p", {}, {}, None, {"one": {"checkpoint-every": 20}})
    context = yaml.safe_load(out["one"])
    assert context["checkpoint_every"] == 20 and list(context)[-1] == "checkpoint_every"
    plain = yaml.safe_load(compose.step_contexts([step], "r", "a", "p", {}, {}, None, {})["one"])
    assert "checkpoint_every" not in plain


# ------------------------------------------------------------------ promote-<name>

def _with_dataset(**keywords):
    with authoring.pipeline("t") as p:
        authoring.step("one", kind="transform", produces=[authoring.dataset("embeddings"), authoring.model("m")])
        authoring.step("two", kind="evaluate")
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), _context())


def test_the_form_has_a_promote_choice_for_each_produced_dataset_and_for_nothing_else():
    wft = _with_dataset()
    form = _form(wft)
    assert form["promote-embeddings"] == {
        "name": "promote-embeddings", "value": "false", "enum": ["false", "true"],
        "description": "Make this run's version of embeddings the one other pipelines read as latest."}
    assert [n for n in form if n.startswith("promote-")] == ["promote-embeddings"]    # not the model
    args = _template(wft, "compose-and-validate")["container"]["args"]
    assert args[args.index("--promote") + 1] == "embeddings={{workflow.parameters.promote-embeddings}}"
