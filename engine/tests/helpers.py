"""Helpers for tests that run the runner in a subprocess (the way CI runs it)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

from paths import APP, ENGINE

RAW = Path("out") / "raw-workflow-template.yaml"


def copy_app(dest: Path) -> Path:
    """A fresh copy of the fixture app (pipeline.py, config/, pyproject.toml) to mutate."""
    shutil.copytree(APP, dest, ignore=shutil.ignore_patterns("out", "__pycache__"))
    return dest


def run_render(repo: Path, profile: str = "hera", extra_args=(), extra_env=None, python: str = sys.executable):
    """Run `python -m kaos_engine.render pipeline.py` against repo; returns the CompletedProcess."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("KUBECORE_", "KAOS_ENGINE", "PYTHONPATH"))}
    env.update({"PYTHONPATH": str(ENGINE), "PYTHONDONTWRITEBYTECODE": "1"}, **(extra_env or {}))
    return subprocess.run([python, "-m", "kaos_engine.render", "pipeline.py", "--repo", str(repo), "--profile", profile,
                           "--engine", str(ENGINE), *extra_args], cwd=repo, env=env, capture_output=True, text=True)


def raw_template(repo: Path) -> dict:
    """The parsed out/raw-workflow-template.yaml the render wrote."""
    return yaml.safe_load((repo / RAW).read_text())


def evaluate(expression: str, settings: dict):
    """The value of a `{{=...}}` argument the engine renders for a number compose wrote (kaos-run-settings):
    the expressions use arithmetic, `int` and `fromJSON` of compose's output only, so Python can read them."""
    import json
    import re

    body = re.fullmatch(r"\{\{=(.*)\}\}", expression, re.S).group(1)
    body = re.sub(r'fromJSON\(tasks\["compose-and-validate"\]\.outputs\.parameters\["kaos-run-settings"\]\)',
                  f"json.loads({json.dumps(settings)!r})", body)
    return eval(body, {"json": json, "int": int})  # noqa: S307 - the engine's own output, in a test


def valid(authoring, kind: str) -> dict:
    """The keywords that make a step of `kind` valid on its own: what its kind requires it to produce
    (docs/step-contract.md, "Step kinds")."""
    if kind == "ingest":
        return {"produces": [authoring.dataset("clean")]}
    if kind == "transform":
        return {"produces": [authoring.artifact("report")]}
    if kind == "train":
        return {"produces": [authoring.model("model")]}
    return {}
