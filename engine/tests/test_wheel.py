"""The wheel: engine under kaos_pipeline_engine/, never a top-level kubecore, runnable with `python -I`."""

import subprocess
import sys
import sysconfig
import zipfile
from pathlib import Path

import pytest

from helpers import copy_app, raw_template
from paths import ENGINE

TAG = "v0.9.3-dev-20261110-123456-abc1234"
VERSION = "0.9.3.post1.dev20261110123456+abc1234"


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    out = tmp_path_factory.mktemp("wheel")
    src = tmp_path_factory.mktemp("src") / "engine"  # build in a copy: no build/ or egg-info in the checkout
    subprocess.run(["cp", "-a", str(ENGINE), str(src)], check=True)
    (src.parent / "VERSION").write_text("0.0.1\n")
    res = subprocess.run([sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(out), str(src)],
                         capture_output=True, text=True, env={"KAOS_ENGINE_TAG": TAG, "PATH": "/usr/bin:/bin"})
    assert res.returncode == 0, res.stdout + res.stderr
    return next(out.glob("*.whl"))


def test_should_version_the_wheel_from_the_build_tag(wheel):
    assert f"-{VERSION}-" in wheel.name


def test_should_ship_the_engine_under_kaos_pipeline_engine_and_no_toplevel_kubecore(wheel):
    names = zipfile.ZipFile(wheel).namelist()
    assert "kaos_pipeline_engine/kubecore/authoring.py" in names
    assert "kaos_engine/render.py" in names
    assert not [n for n in names if n.startswith("kubecore/")]
    top = zipfile.ZipFile(wheel).read(next(n for n in names if n.endswith("top_level.txt"))).decode().split()
    assert sorted(top) == ["kaos_engine", "kaos_pipeline_engine"]


def test_should_ship_the_data_files(wheel):
    names = zipfile.ZipFile(wheel).namelist()
    assert "kaos_pipeline_engine/authoring-schema.json" in names
    assert "kaos_pipeline_engine/kubecore/local-dev/pipeline-context.yaml" in names


def test_should_render_isolated_from_an_installed_wheel(wheel, tmp_path):
    """The CI invocation: `python -I -m kaos_engine.render`, no PYTHONPATH, engine found next to the runner."""
    venv = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    py = str(venv / "bin" / "python")
    # the pinned render dependencies come from the interpreter running the tests, via a .pth (no network)
    site = subprocess.run([py, "-c", "import site; print(site.getsitepackages()[0])"], capture_output=True, text=True,
                          check=True).stdout.strip()
    (Path(site) / "deps.pth").write_text(sysconfig.get_paths()["purelib"] + "\n")
    subprocess.run([py, "-m", "pip", "install", "-q", "--no-deps", "--no-index", str(wheel)], check=True)
    repo = copy_app(tmp_path / "app")
    res = subprocess.run([py, "-I", "-m", "kaos_engine.render", "pipeline.py", "--repo", str(repo), "--profile", "hera"],
                         cwd=repo, capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    assert f"version={VERSION}" in res.stderr
    assert "kaos_pipeline_engine" in res.stderr  # the engine path is the wheel's
    assert raw_template(repo)["kind"] == "WorkflowTemplate"
    probe = subprocess.run([py, "-I", "-c", "import kubecore"], capture_output=True, text=True)
    assert probe.returncode != 0, "a top-level kubecore must not be importable after installing the wheel"
