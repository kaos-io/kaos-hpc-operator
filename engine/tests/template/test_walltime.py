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
from kubecore.meluxina import DEFAULT_TIME_LIMIT_MINUTES  # noqa: E402

BASE = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def _context():
    ctx = copy.deepcopy(BASE)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    return ctx


def _enhanced(**keywords):
    with authoring.pipeline("t") as p:
        authoring.step("one", kind=keywords.pop("kind", "train"), **keywords)
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), _context())


def _template(wft, name):
    return next(t for t in wft["spec"]["templates"] if t["name"] == name)


def _twin(wft):
    task = next(t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]
                if t["name"] == "one-hpc")
    return {p["name"]: p["value"] for p in task["arguments"]["parameters"]}


@pytest.mark.parametrize("walltime, minutes", [("10m", 10), ("2h", 120), ("90", 90)])
def test_walltime_is_the_deadline_of_the_step_on_the_cluster(walltime, minutes):
    wft = _enhanced(walltime=walltime)
    assert _template(wft, "one")["activeDeadlineSeconds"] == minutes * 60


@pytest.mark.parametrize("walltime, minutes", [("10m", 10), ("2h", 120), ("90", 90)])
def test_walltime_is_the_limit_of_the_job_on_the_hpc_twin(walltime, minutes):
    twin = _twin(_enhanced(walltime=walltime))
    assert twin["time-limit"] == str(minutes)


def test_a_step_with_no_walltime_keeps_the_default_deadline_and_the_default_hpc_limit():
    wft = _enhanced()
    assert _template(wft, "one")["activeDeadlineSeconds"] == enhance.PENDING_DEADLINE_SECONDS
    assert _twin(wft)["time-limit"] == str(DEFAULT_TIME_LIMIT_MINUTES)
    assert _template(_enhanced(gpu=True), "one")["activeDeadlineSeconds"] == enhance.GPU_PENDING_DEADLINE_SECONDS


@pytest.mark.parametrize("kind", ["ingest", "publish", "control"])
def test_a_step_that_never_runs_on_hpc_may_declare_walltime(kind):
    wft = _enhanced(kind=kind, walltime="20m")
    assert _template(wft, "one")["activeDeadlineSeconds"] == 20 * 60
