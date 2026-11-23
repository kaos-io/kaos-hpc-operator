"""Nothing about time is set by a person (PRD kaos#1911 D-75, ruled 2026-11-19).

The run form has no time field. The deadline of every step of the pipeline is the platform's cap
(`stepCapMinutes` of the pipeline context, seven days when absent); on an HPC class the twin carries the same cap
as its `time-limit`. Every step that may compute gets the checkpoint helper, and one retry rule covers all steps.
Run:  python -m pytest tests/ -v
"""
import copy
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance, meluxina, resilience  # noqa: E402
from kubecore.meluxina import STAGING_ALLOWANCE_MINUTES  # noqa: E402
from helpers import valid  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
DAY = 24 * 60


def _context(**more):
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    ctx.update(more)
    return ctx


def _enhanced(ctx=None, **keywords):
    with authoring.pipeline("t") as p:
        kind = keywords.pop("kind", "evaluate")
        authoring.step("one", kind=kind, **{**valid(authoring, kind), **keywords})
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx or _context())


def _template(wft, name):
    return next(t for t in wft["spec"]["templates"] if t["name"] == name)


def _tasks(wft):
    return {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}


def _arguments(wft, task):
    return {p["name"]: p["value"] for p in _tasks(wft)[task]["arguments"]["parameters"]}


def _form(wft):
    return {p["name"]: p for p in wft["spec"]["arguments"]["parameters"]}


# ------------------------------------------------------------------ the form has no time field

@pytest.mark.parametrize("kind", ["ingest", "transform", "train", "evaluate", "publish", "control"])
def test_should_have_no_form_field_ending_walltime_or_checkpoint_every(kind):
    wft = _enhanced(kind=kind)
    assert [n for n in _form(wft) if n.endswith(("-walltime", "-checkpoint-every"))] == []
    args = _template(wft, "compose-and-validate")["container"]["args"]
    assert not {"--walltime", "--checkpoint-every", "--resumable-lead"} & set(args)


def test_should_write_no_time_annotation_on_a_step():
    with authoring.pipeline("t") as p:
        authoring.step("one", kind="evaluate", idle_timeout="40m")
    annotations = _template(yaml.safe_load(p.wt.to_yaml()), "one")["metadata"]["annotations"]
    assert annotations == {"platform.kubecore.io/idle-timeout": "40"}


@pytest.mark.parametrize("annotation", ["hpc", "hpc-resumable", "checkpoint-every"])
def test_should_refuse_an_old_time_annotation_as_unknown(annotation):
    with authoring.pipeline("t") as p:
        authoring.step("one", kind="evaluate")
    wft = yaml.safe_load(p.wt.to_yaml())
    _template(wft, "one")["metadata"]["annotations"] = {"platform.kubecore.io/" + annotation: "10"}
    with pytest.raises(enhance.EnhanceError, match="unknown platform annotation"):
        enhance.enhance(wft, _context())


# ------------------------------------------------------------------ the deadline is the platform's cap

def test_should_end_a_step_at_the_default_cap_of_seven_days():
    ctx = _context()
    ctx.pop("stepCapMinutes", None)
    template = _template(_enhanced(ctx), "one")
    assert template["activeDeadlineSeconds"] == 7 * DAY * 60 == 604800


def test_should_end_a_step_at_the_cap_the_context_names():
    assert _template(_enhanced(_context(stepCapMinutes=90)), "one")["activeDeadlineSeconds"] == 5400


@pytest.mark.parametrize("value", [0, -5, "90", True, 1.5])
def test_should_refuse_a_cap_that_is_not_a_whole_number_of_minutes_above_zero(value):
    with pytest.raises(ValueError, match="stepCapMinutes"):
        _enhanced(_context(stepCapMinutes=value))


def test_should_keep_the_compose_step_on_its_own_fixed_deadline():
    wft = _enhanced(_context(stepCapMinutes=90))
    assert _template(wft, "compose-and-validate")["activeDeadlineSeconds"] == 1800


def test_should_pass_the_cap_as_the_time_limit_of_the_hpc_twin_and_no_resumable_value():
    wft = _enhanced(_context(stepCapMinutes=90))
    args = _arguments(wft, "one-hpc")
    assert args["time-limit"] == "90"
    assert args["deadline-seconds"] == str((2 * 90 + STAGING_ALLOWANCE_MINUTES) * 60) == "270000"
    assert "resumable" not in args and "resumable-lead" not in args
    twin = _template(wft, meluxina.HPC_TWIN_TEMPLATE)
    assert {p["name"] for p in twin["inputs"]["parameters"]}.isdisjoint({"resumable", "resumable-lead"})
    assert not {"HPC_RESUMABLE", "HPC_RESUMABLE_LEAD", "HPC_MAX_CONTINUATIONS"} & {e["name"] for e in twin["container"]["env"]}


def test_should_default_the_hpc_time_limit_to_the_seven_day_cap():
    ctx = _context()
    ctx.pop("stepCapMinutes", None)
    assert _arguments(_enhanced(ctx), "one-hpc")["time-limit"] == str(7 * DAY)


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


# ------------------------------------------------------------------ one retry rule

def _retry(kind="train", **keywords):
    return _template(_enhanced(kind=kind, **keywords), "one")["retryStrategy"]


def _retried(status, message, exit_code=""):
    """RETRY_EXPRESSION as the workflow engine evaluates it (a Go regexp `matches` is a search anchored by the
    expression's own `.*`)."""
    lost = status == "Error" or re.search(".*(%s).*" % meluxina.POD_LOSS_MESSAGES, message) is not None
    lost = lost and meluxina.NO_RESULT_MESSAGE not in message
    return (lost or exit_code == "85") and not message.startswith(resilience.IDLE_END_PREFIX)


@pytest.mark.parametrize("kind", ["ingest", "transform", "train", "evaluate", "publish", "control"])
def test_should_give_every_step_the_same_retry_rule(kind):
    retry = _retry(kind)
    assert retry["expression"] == resilience.RETRY_EXPRESSION and retry["limit"] == resilience.RETRY_LIMIT
    assert int(retry["limit"]) > 5, "the old count of 5 continuations is no longer the limit"


@pytest.mark.parametrize("what, status, message, exit_code", [
    ("its machine is lost", "Failed", "Pod was rejected: node shutting down", ""),
    ("it asks to be continued", "Error", "Error (exit code 85)", "85"),
    ("it ends itself with 85", "Failed", "Error (exit code 85)", "85"),
])
def test_should_run_a_step_again_when_its_node_is_lost_or_it_exits_85(what, status, message, exit_code):
    assert _retried(status, message, exit_code), what


@pytest.mark.parametrize("what, status, message, exit_code", [
    ("it reaches its deadline (the kubelet's words)", "Failed", "Pod was active on the node longer than the specified deadline", ""),
    ("it reaches its deadline (the workflow engine's words)", "Failed", "Step exceeded its deadline", ""),
    ("it fails by itself", "Failed", "Error (exit code 1)", "1"),
    ("it is killed for memory", "Failed", "OOMKilled (exit code 137)", "137"),
    ("it is ended as idle", "Failed", "KaosIdle: no GPU use", ""),
])
def test_should_not_run_a_step_again_when_it_fails_by_itself_reaches_its_deadline_or_is_idle(what, status, message, exit_code):
    assert not _retried(status, message, exit_code), what


def test_should_name_neither_the_deadline_nor_a_count_of_continuations_in_the_expression():
    assert "deadline" not in resilience.RETRY_EXPRESSION
    assert not hasattr(resilience, "DEADLINE_MESSAGES") and not hasattr(meluxina, "HPC_MAX_CONTINUATIONS")


# ------------------------------------------------------------------ the checkpoint helper

def _helpers(template):
    return {c["name"] for c in template.get("initContainers", []) + template.get("sidecars", [])}


def test_should_give_a_transform_step_the_checkpoint_helper():
    assert _helpers(_template(_enhanced(kind="transform"), "one")) == {resilience.INIT_NAME, resilience.SIDECAR_NAME}


@pytest.mark.parametrize("kind", ["ingest", "train", "evaluate"])
def test_should_give_every_other_computing_kind_the_checkpoint_helper(kind):
    assert _helpers(_template(_enhanced(kind=kind), "one")) == {resilience.INIT_NAME, resilience.SIDECAR_NAME}


@pytest.mark.parametrize("kind", ["control", "publish"])
def test_should_give_a_control_or_publish_step_no_helper_but_the_folder_and_the_retry_rule(kind):
    template = _template(_enhanced(kind=kind), "one")
    assert _helpers(template) == set()
    assert template["retryStrategy"]["expression"] == resilience.RETRY_EXPRESSION
    assert any(e["name"] == resilience.CHECKPOINT_ENV for e in template["container"]["env"])


def test_should_copy_at_the_platforms_interval_not_a_form_value():
    template = _template(_enhanced(kind="transform"), "one")
    env = {e["name"]: e.get("value") for c in template["sidecars"] for e in c["env"]}
    assert env["CKPT_EVERY_MIN"] == str(resilience.DEFAULT_EVERY_MINUTES)


def test_should_not_bound_the_retries_by_time_since_the_first_attempt():
    """Argo's backoff.maxDuration runs from the start of the first attempt: with it a step lost after ten minutes of
    work is not run again. The wait between attempts is bounded by `cap` instead."""
    from kubecore import resilience
    backoff = resilience.retry_strategy()["backoff"]
    assert "maxDuration" not in backoff
    assert backoff == {"duration": "30s", "factor": "2", "cap": "5m"}


def test_should_give_the_hpc_twin_a_deadline_that_is_only_a_backstop_far_beyond_the_cap():
    """The controller ends the step at the cap. A step of 7 days is many jobs, each of them queued: the twin's own
    deadline must never cut it first, so it is twice the cap plus the staging allowance, in both kinds of twin."""
    week = 7 * 24 * 60
    wft = _enhanced(_context(stepCapMinutes=week))
    deadline = int(_arguments(wft, "one-hpc")["deadline-seconds"])
    assert deadline == (2 * week + 72 * 60) * 60
    assert deadline >= 2 * week * 60, "a queue wait as long as the step's whole run time still fits"
    assert meluxina.twin_deadline_seconds(90) == 270000
