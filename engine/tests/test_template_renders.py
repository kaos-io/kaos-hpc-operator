"""Golden renders of the two official templates (kaos-io templates, branch `dev`; snapshot in tests/templates/):
what every step becomes on the cluster and on an HPC class once every step can be run again (PRD kaos#1911 D-64).
No step in either template writes `resumable=`; yolo's training step asks for 12 hours.

Regenerate: UPDATE_GOLDEN=1 python -m pytest tests/test_template_renders.py
Snapshot: hera-pipeline-template bf7f05b, yolo-pipelines-template 09c2e1e (pipeline.py and config/).
"""
import os
import re
import shutil
from pathlib import Path

import pytest
import yaml

from helpers import raw_template, run_render
from kubecore import enhance, endings, resilience

HERE = Path(__file__).parent
CONTEXT = yaml.safe_load((HERE.parent / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def summary(wft: dict) -> dict:
    """What a reader checks: per step, the cluster template's retry, attempt env and checkpoint helper, and the
    arguments of its HPC variant that carry the time limit."""
    templates = {t["name"]: t for t in wft["spec"]["templates"]}
    tasks = {task["name"]: task for t in wft["spec"]["templates"] for task in (t.get("dag") or {}).get("tasks") or []}
    out = {}
    for name, task in tasks.items():
        if name.endswith("-hpc") or name == "compose-and-validate" or name not in templates:
            continue
        t = templates[name]
        strategy = t.get("retryStrategy") or {}
        env = {e["name"]: e.get("value") for e in t["container"].get("env", [])}
        twin = tasks.get(f"{name}-hpc")
        out[name] = {
            "retry_limit": strategy.get("limit"),
            "retry_continues_at_limit": strategy.get("expression") == resilience.CONTINUE_EXPRESSION,
            "KAOS_ATTEMPT": env.get(resilience.ATTEMPT_ENV),
            "helper": sorted(c["name"] for c in (t.get("initContainers") or []) + (t.get("sidecars") or [])),
            "hpc_variant_arguments": sorted(a["name"] for a in twin["arguments"]["parameters"]) if twin else None,
        }
    return out


@pytest.mark.parametrize("template", ["hera", "yolo"])
def test_the_template_renders_to_the_golden_file(tmp_path, template):
    repo = tmp_path / "app"
    shutil.copytree(HERE / "templates" / template, repo)
    for name in re.findall(r'step\("([\w-]+)"', (repo / "pipeline.py").read_text()):
        folder = repo / "steps" / name.replace("-", "_")
        (folder / "app").mkdir(parents=True)
        (folder / "Dockerfile").write_text("FROM scratch\n")
        (folder / "app" / "entry.py").write_text("")
    res = run_render(repo, profile=template)
    assert res.returncode == 0, res.stderr
    got = summary(enhance.enhance(raw_template(repo), CONTEXT))
    for step in got.values():
        assert step["retry_limit"] in (str(endings.MAX_CONTINUATIONS + endings.MAX_RESTARTS), str(endings.MAX_RESTARTS))
        assert not any("resumable" in a for a in step["hpc_variant_arguments"] or [])
    golden = HERE / "golden" / f"template_{template}.yaml"
    text = yaml.safe_dump(got, sort_keys=True)
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_text(text)
    assert text == golden.read_text()
