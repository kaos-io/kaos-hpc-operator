"""PRD kaos#1911 D-64: the same rules on every executor, and the engine's rules do not depend on access life, lane
limits, Slurm or spot.

* The same pipeline is accepted or refused once, whatever class it runs on; the cluster template and the HPC variant of
  a step are rendered from the same step, with the same budget.
* The engine's accept/refuse and retry modules name no access life, no lane and no scheduler (the test of the
  separation). The one access-life rule left is the token-cover refusal in compose.py, listed in the report as still in
  the wrong place; it is the only file allowed to name it.
"""
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

SEPARATED = ["authoring.py", "resilience.py", "endings.py"]
FORBIDDEN = [r"\blane", r"\bslurm", r"token.cover", r"TOKEN_COVER", r"\b690\b", r"11 h", r"access life", r"\bQoS\b"]


def _render(**kw):
    from kubecore import authoring, enhance
    with authoring.pipeline("t") as p:
        authoring.step("train", kind="evaluate", **kw)
    ctx = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)


def _templates(wft):
    return {t["name"]: t for t in wft["spec"]["templates"]}


@pytest.mark.parametrize("kw", [{}, {"gpu": True}, {"walltime": "12h"}, {"gpu": True, "walltime": "30h"}])
def test_the_cluster_step_and_its_hpc_variant_come_from_one_step_with_one_budget(kw):
    from kubecore import endings, meluxina, resilience
    wft = _render(**kw)
    train = _templates(wft)["train"]
    strategy = train["retryStrategy"]
    assert int(strategy["limit"]) == endings.MAX_CONTINUATIONS + endings.MAX_RESTARTS
    assert strategy["expression"] == resilience.CONTINUE_EXPRESSION
    env = {e["name"]: e.get("value") for c in train["container"]["env"] for e in [c]}
    assert env[resilience.ATTEMPT_ENV] == resilience.ATTEMPT_VALUE
    assert meluxina.HPC_MAX_CONTINUATIONS == endings.MAX_CONTINUATIONS
    twin_tasks = [task for t in wft["spec"]["templates"] for task in (t.get("dag") or {}).get("tasks") or []
                  if task["name"] == "train-hpc"]
    assert len(twin_tasks) == 1


def test_the_same_refusals_apply_whatever_the_class():
    from kubecore import authoring
    for kw in ({"gpu": True, "checkpoint_every": False}, {"resumable": True}):
        with pytest.raises(authoring.AuthoringError):
            with authoring.pipeline("t"):
                authoring.step("train", kind="evaluate", **kw)


@pytest.mark.parametrize("name", SEPARATED)
def test_the_engines_accept_refuse_and_retry_modules_name_no_access_life_lane_or_scheduler(name):
    text = (ROOT / "kubecore" / name).read_text()
    code = "\n".join(line for line in text.splitlines())
    hits = [p for p in FORBIDDEN if re.search(p, code, re.IGNORECASE)]
    assert not hits, f"{name} names {hits}: that belongs to the executor, not to the engine's rules"


@pytest.mark.parametrize("kind", ["publish", "control"])
def test_a_step_that_acts_on_the_outside_world_is_never_started_again_after_it_may_have_run(kind):
    from kubecore import authoring, enhance, resilience
    with authoring.pipeline("t") as p:
        authoring.step("act", kind=kind)
    ctx = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
    act = _templates(enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx))["act"]
    assert act["retryStrategy"]["expression"] == resilience.NOT_STARTED_EXPRESSION
    assert "sidecars" not in act and "initContainers" not in act
