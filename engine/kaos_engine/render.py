"""Render an app's pipeline.py with the PLATFORM engine, never with the app's vendored copy.

    python -I -m kaos_engine.render pipeline.py --repo /workspace/repo --profile hera

Why: a vendored `kubecore/` in a client repo is a snapshot; the platform must be able to change the
rendering code without a commit in the client's repo (PRD-HPC-1461 D-79). So the engine's modules are
imported FIRST, from the engine directory, and the interpreter then finds them in sys.modules whatever
the app's directory contains.

Contract (engine/README.md "The runner contract"):
  1. sys.path = [engine dir, app repo] + the interpreter's own entries (stdlib, site-packages).
  2. `kubecore` and every engine submodule is imported from the engine dir; their __file__ is asserted.
  3. The app's own `kubecore/` is APPENDED to `kubecore.__path__`: extra client modules still import,
     but every engine name wins.
  4. KUBECORE_SCHEMA_PROFILE and KUBECORE_CONFIG_DIR are set before the engine is imported.
  5. pipeline.py runs as `__main__` (it writes out/raw-workflow-template.yaml next to itself).
  6. Afterwards the engine modules must still be the ones in sys.modules; otherwise exit 3 (tampered).
  7. The app's pyproject dependencies that the engine does not provide are printed (installing them is
     CI's job); nothing is pip-installed here and the app is never installed.

Exit codes: 0 rendered, 1 pipeline.py raised, 2 bad invocation, 3 engine tampered with, 4 engine broken.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import os
import pkgutil
import re
import runpy
import sys
import tomllib
from pathlib import Path

from kaos_engine import engine_version

PROFILES = ("hera", "yolo", "legacy-v1", "yolo-v1")
EXIT_BAD_INVOCATION = 2
EXIT_TAMPERED = 3
EXIT_ENGINE_BROKEN = 4
EXTRA_DEP_PREFIX = "kaos-engine: extra-dep "


class EngineError(Exception):
    """The engine could not be set up or was altered; carries the process exit code."""

    def __init__(self, message: str, code: int):
        super().__init__(message)
        self.code = code


def find_engine_dir(explicit: str | None) -> Path:
    """The directory that holds `kubecore/`: --engine, else $KAOS_ENGINE_DIR, else next to this runner.

    Next to the runner means the wheel layout (site-packages/kaos_pipeline_engine) or the repo layout
    (engine/kubecore beside engine/kaos_engine).
    """
    here = Path(__file__).resolve().parent.parent
    candidates = [explicit, os.environ.get("KAOS_ENGINE_DIR"), here / "kaos_pipeline_engine", here]
    for candidate in candidates:
        if candidate and (Path(candidate) / "kubecore" / "__init__.py").is_file():
            return Path(candidate).resolve()
    raise EngineError(f"no engine directory with kubecore/__init__.py (tried {candidates})", EXIT_BAD_INVOCATION)


def build_sys_path(engine: Path, repo: Path, current: list[str]) -> list[str]:
    """engine, repo, then the interpreter's own entries minus the working directory and duplicates."""
    cwd = os.getcwd()
    rest = [p for p in current if p not in ("", cwd) and Path(p).resolve() not in (engine, repo)]
    return [str(engine), str(repo)] + rest


def import_engine(engine: Path) -> dict[str, object]:
    """Import kubecore and all of its submodules from the engine dir; return {name: module}."""
    for name in [m for m in sys.modules if m == "kubecore" or m.startswith("kubecore.")]:
        del sys.modules[name]
    modules = {"kubecore": importlib.import_module("kubecore")}
    for info in pkgutil.iter_modules([str(engine / "kubecore")]):
        if not info.ispkg:
            name = f"kubecore.{info.name}"
            modules[name] = importlib.import_module(name)
    for name, module in modules.items():
        origin = Path(module.__file__).resolve()
        if engine not in origin.parents:
            raise EngineError(f"{name} was imported from {origin}, outside the engine {engine}", EXIT_ENGINE_BROKEN)
    return modules


def append_app_package(kubecore, repo: Path, engine: Path) -> None:
    """Let the app's own kubecore/ files import as kubecore.<x> without ever shadowing an engine name."""
    app_pkg = repo / "kubecore"
    if app_pkg.is_dir() and app_pkg.resolve() != (engine / "kubecore").resolve():
        kubecore.__path__.append(str(app_pkg))


def assert_untouched(modules: dict[str, object]) -> None:
    """Exit-3 check: every engine module is still the one sys.modules resolves (the app did not swap it)."""
    for name, module in modules.items():
        if sys.modules.get(name) is not module:
            raise EngineError(f"{name} was replaced while pipeline.py ran: refusing the render (tampered)", EXIT_TAMPERED)


def _normalise(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def extra_dependencies(repo: Path) -> list[str]:
    """The app's pyproject dependencies the running interpreter does not satisfy."""
    pyproject = repo / "pyproject.toml"
    if not pyproject.is_file():
        return []
    with pyproject.open("rb") as handle:
        project = tomllib.load(handle).get("project", {})
    installed = {_normalise(d.metadata["Name"]): d.version for d in importlib.metadata.distributions()}
    missing = []
    for text in project.get("dependencies", []):
        if not _is_unmet(text, installed, _normalise(project.get("name", ""))):
            continue
        missing.append(text)
    return missing


def _is_unmet(text: str, installed: dict[str, str], own_name: str) -> bool:
    try:
        from packaging.requirements import Requirement  # a hydra-core dependency, so always present
    except ImportError:  # name-only fallback
        match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", text)
        return bool(match) and _normalise(match.group(1)) not in installed
    req = Requirement(text)
    name = _normalise(req.name)
    if name == own_name or (req.marker is not None and not req.marker.evaluate()):
        return False
    version = installed.get(name)
    return version is None or not req.specifier.contains(version, prereleases=True)


def render(pipeline: Path, repo: Path, profile: str, engine: Path, config_dir: Path, deps_out: Path | None) -> None:
    """Steps 1-7 of the module docstring."""
    os.environ["KUBECORE_SCHEMA_PROFILE"] = profile
    os.environ["KUBECORE_CONFIG_DIR"] = str(config_dir)
    sys.path[:] = build_sys_path(engine, repo, sys.path)
    modules = import_engine(engine)
    append_app_package(modules["kubecore"], repo, engine)
    print(f"kaos-engine: version={engine_version()} profile={profile} engine={engine} repo={repo}", file=sys.stderr)
    extras = extra_dependencies(repo)
    for text in extras:
        print(f"{EXTRA_DEP_PREFIX}{text}", file=sys.stderr)
    if deps_out is not None:
        deps_out.write_text("".join(f"{t}\n" for t in extras))
    os.chdir(repo)
    sys.argv = [str(pipeline)]
    runpy.run_path(str(pipeline), run_name="__main__")
    assert_untouched(modules)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="kaos_engine.render", description=__doc__.split("\n\n")[0])
    parser.add_argument("pipeline", help="the app's pipeline.py (relative to --repo)")
    parser.add_argument("--repo", default=".", help="the app repository root (default: the current directory)")
    parser.add_argument("--profile", default=os.environ.get("KUBECORE_SCHEMA_PROFILE"), choices=PROFILES,
                        help="schema profile; default $KUBECORE_SCHEMA_PROFILE")
    parser.add_argument("--engine", help="engine directory holding kubecore/ (default: $KAOS_ENGINE_DIR, else beside the runner)")
    parser.add_argument("--config-dir", help="the app's Hydra config tree (default: <repo>/config)")
    parser.add_argument("--extra-deps-out", help="write the unmet app dependencies here, one requirement per line")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns the process exit code."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.profile is None:
        print("kaos-engine: a schema profile is required (--profile or KUBECORE_SCHEMA_PROFILE)", file=sys.stderr)
        return EXIT_BAD_INVOCATION
    try:
        repo = Path(args.repo).resolve()
        pipeline = (repo / args.pipeline).resolve()
        if not pipeline.is_file():
            raise EngineError(f"{pipeline} does not exist", EXIT_BAD_INVOCATION)
        config_dir = Path(args.config_dir).resolve() if args.config_dir else repo / "config"
        deps_out = Path(args.extra_deps_out) if args.extra_deps_out else None
        render(pipeline, repo, args.profile, find_engine_dir(args.engine), config_dir, deps_out)
    except EngineError as err:
        print(f"kaos-engine: {err}", file=sys.stderr)
        return err.code
    return 0


if __name__ == "__main__":
    sys.exit(main())
