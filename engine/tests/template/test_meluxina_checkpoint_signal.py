"""PRD kaos#1911 D-64: every step can be run again, so no step declares it (``resumable=`` is gone).

* A step's HPC variant gets a deadline that covers every continuation, whatever the step is.
* The engine sends the follower no lead and no opt-out: the controller chooses the length of each run, learns it
  from the step's earlier runs, and decides itself whether the step continues.
* ``resumable=`` is refused with the fix, and a step with ``gpu=`` cannot switch its checkpoint copies off.

The signal handling, checkpoint stage-out and continuation themselves run in the HPC controller's
runner (PRD-HPC-1461 F-35 retired the in-pod copies and their tests).
Run:  python -m pytest tests/ -v
"""
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------- authoring and render

def _render(**kw):
    from kubecore import authoring, enhance
    with authoring.pipeline("t") as p:
        authoring.step("train", kind="evaluate", gpu=True, **kw)
    ctx = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
    wft = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)
    for t in wft["spec"]["templates"]:
        for task in (t.get("dag") or {}).get("tasks") or []:
            if task["name"] == "train-hpc":
                return {a["name"]: a["value"] for a in task["arguments"]["parameters"]}
    raise AssertionError("no HPC twin rendered")


@pytest.mark.parametrize("value", [True, False, "20m", "10m"])
def test_resumable_is_refused_with_one_sentence_naming_the_step_and_the_fix(value):
    from kubecore import authoring
    with pytest.raises(authoring.AuthoringError) as err, authoring.pipeline("t"):
        authoring.step("train", kind="evaluate", gpu=True, resumable=value)
    assert str(err.value).startswith("step 'train': delete resumable=; every step can be run again.")


def test_the_follower_gets_no_lead_and_no_opt_out_and_a_deadline_for_every_continuation():
    from kubecore import meluxina
    all_jobs = (1 + meluxina.HPC_MAX_CONTINUATIONS) * (120 + meluxina.QUEUE_ALLOWANCE_MINUTES) * 60 \
        + meluxina.STAGING_ALLOWANCE_MINUTES * 60
    from helpers import evaluate
    twin = _render(walltime="2h")
    assert "resumable-lead" not in twin and "resumable" not in twin
    assert evaluate(twin["deadline-seconds"], {"train": {"walltime": 120}}) == all_jobs


def test_idle_timeout_reaches_the_follower_as_minutes_and_unset_leaves_the_platform_default():
    """step(idle_timeout=...) -> annotation (minutes, "0" = off) -> twin argument idle-timeout ->
    HPC_IDLE_TIMEOUT_MINUTES on the follower -> HPCArgoWorkflowJob spec.idleTimeoutMinutes. Unset: ""."""
    assert _render()["idle-timeout"] == ""
    assert _render(idle_timeout=40)["idle-timeout"] == "40"
    assert _render(idle_timeout="1h")["idle-timeout"] == "60"
    assert _render(idle_timeout=0)["idle-timeout"] == "0"
    assert _render(idle_timeout=False)["idle-timeout"] == "0"


def test_a_cpu_step_has_the_same_follower_as_a_gpu_step():
    from kubecore import authoring
    with authoring.pipeline("t"):
        authoring.step("prep", kind="evaluate", walltime="2h")
