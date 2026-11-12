"""The runner contract (engine/README.md): the engine renders; code of the same name in the app never wins."""

import textwrap

import pytest

from helpers import copy_app, raw_template, run_render

TRAP = '"""Booby trap: a vendored copy that must never be imported."""\nraise RuntimeError("the vendored copy was imported")\n'


@pytest.fixture(scope="module")
def baseline(tmp_path_factory):
    repo = copy_app(tmp_path_factory.mktemp("base") / "app")
    res = run_render(repo)
    assert res.returncode == 0, res.stderr
    return raw_template(repo)


def test_should_render_the_fixture_app_and_print_the_engine_line(tmp_path):
    repo = copy_app(tmp_path / "app")
    res = run_render(repo)
    assert res.returncode == 0, res.stderr
    assert "kaos-engine: version=" in res.stderr and "profile=hera" in res.stderr
    assert raw_template(repo)["kind"] == "WorkflowTemplate"


@pytest.mark.parametrize("module", ["authoring", "derive_tree", "compose", "enhance", "meluxina", "resilience"])
def test_should_render_when_a_vendored_module_raises_on_import(tmp_path, baseline, module):
    repo = copy_app(tmp_path / "app")
    (repo / "kubecore").mkdir()
    (repo / "kubecore" / f"{module}.py").write_text(TRAP)
    res = run_render(repo)
    assert res.returncode == 0, res.stderr
    assert raw_template(repo) == baseline


def test_should_render_when_the_vendored_package_init_raises(tmp_path, baseline):
    repo = copy_app(tmp_path / "app")
    (repo / "kubecore").mkdir()
    (repo / "kubecore" / "__init__.py").write_text(TRAP)
    res = run_render(repo)
    assert res.returncode == 0, res.stderr
    assert raw_template(repo) == baseline


def test_should_render_when_a_toplevel_kubecore_alias_is_on_the_path(tmp_path, baseline):
    """kaos-lib 0.4 installs a top-level `kubecore` alias into site-packages; the engine still wins."""
    alias = tmp_path / "site" / "kubecore"
    alias.mkdir(parents=True)
    (alias / "__init__.py").write_text(TRAP)
    repo = copy_app(tmp_path / "app")
    from paths import ENGINE
    res = run_render(repo, extra_env={"PYTHONPATH": f"{ENGINE}:{alias.parent}"})
    assert res.returncode == 0, res.stderr
    assert raw_template(repo) == baseline


def test_should_exit_3_when_pipeline_py_swaps_an_engine_module(tmp_path):
    repo = copy_app(tmp_path / "app")
    (repo / "pipeline.py").write_text(textwrap.dedent("""
        import sys, types
        import kubecore.authoring
        sys.modules["kubecore.authoring"] = types.ModuleType("kubecore.authoring")
    """))
    res = run_render(repo)
    assert res.returncode == 3, res.stderr
    assert "tampered" in res.stderr


def test_should_exit_2_without_a_profile(tmp_path):
    repo = copy_app(tmp_path / "app")
    res = run_render(repo, profile="")
    assert res.returncode == 2


def test_should_exit_2_for_an_unknown_profile(tmp_path):
    repo = copy_app(tmp_path / "app")
    assert run_render(repo, profile="bogus").returncode == 2


def test_should_read_the_config_tree_of_the_app_not_the_engines(tmp_path):
    repo = copy_app(tmp_path / "app")
    other = tmp_path / "other-config"
    (repo / "config").rename(other)
    res = run_render(repo, extra_args=["--config-dir", str(other)])
    assert res.returncode == 0, res.stderr


def test_should_print_only_the_dependencies_the_engine_does_not_provide(tmp_path):
    repo = copy_app(tmp_path / "app")
    (repo / "pyproject.toml").write_text(textwrap.dedent("""
        [project]
        name = "my-app"
        version = "0.1.0"
        dependencies = ["hera>=5.16,<6", "PyYAML>=6,<7", "no-such-distribution-xyz>=1.2", "hera>=99",
                        "tomli ; python_version < '3.8'"]
    """))
    out = tmp_path / "deps.txt"
    res = run_render(repo, extra_args=["--extra-deps-out", str(out)])
    assert res.returncode == 0, res.stderr
    lines = [ln for ln in res.stderr.splitlines() if ln.startswith("kaos-engine: extra-dep ")]
    assert lines == ["kaos-engine: extra-dep no-such-distribution-xyz>=1.2", "kaos-engine: extra-dep hera>=99"]
    assert out.read_text().splitlines() == ["no-such-distribution-xyz>=1.2", "hera>=99"]


def test_should_never_install_or_write_outside_out(tmp_path):
    repo = copy_app(tmp_path / "app")
    before = {p.relative_to(repo) for p in repo.rglob("*")}
    assert run_render(repo).returncode == 0
    added = {p for p in {p.relative_to(repo) for p in repo.rglob("*")} - before}
    assert {str(p).split("/")[0] for p in added} == {"out"}
