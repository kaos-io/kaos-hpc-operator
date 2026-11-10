"""Small git helpers shared by the engine's history tools (corpus, known-vendored, compat surface)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

TEMPLATES = {"hera": "hera-pipeline-template", "yolo": "yolo-pipelines-template"}
# Private apps for the corpus are never named in this (public) repository: KAOS_ENGINE_CORPUS_LAB lists them as
# comma-separated `org/repo:profile` entries (profile = the template lineage, hera or yolo). The clone of each
# lives in the corpus dir under its repo name; reports call them app-1, app-2, ... in list order.
LAB_ENV = "KAOS_ENGINE_CORPUS_LAB"


def lab_repos() -> list[tuple[str, str]]:
    """[(clone dir name, template profile)] from $KAOS_ENGINE_CORPUS_LAB; empty when it is not set."""
    out = []
    for entry in filter(None, (e.strip() for e in os.environ.get(LAB_ENV, "").split(","))):
        repo, _, profile = entry.rpartition(":")
        out.append((repo.rsplit("/", 1)[-1], profile))
    return out


ENGINE_MODULES = ("authoring", "derive_tree")
# The oldest layout shipped the same two modules as kaos_pipeline/ before the package was renamed.
MODULE_PATHS = {m: (f"kubecore/{m}.py", f"kaos_pipeline/{m}.py") for m in ENGINE_MODULES}


def git(repo: Path, *args: str, check: bool = True) -> str:
    """Run git in repo and return stdout (empty string on a tolerated failure)."""
    res = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} in {repo}: {res.stderr.strip()}")
    return res.stdout


def blob_at(repo: Path, commit: str, path: str) -> str | None:
    """The git blob sha of path at commit, or None if the path does not exist there."""
    out = git(repo, "rev-parse", "--verify", "--quiet", f"{commit}:{path}", check=False).strip()
    return out or None


def show(repo: Path, commit: str, path: str) -> str | None:
    """File content at commit, or None."""
    res = subprocess.run(["git", "-C", str(repo), "show", f"{commit}:{path}"], capture_output=True)
    return res.stdout.decode("utf-8", "replace") if res.returncode == 0 else None


def history(repo: Path, path: str, ref: str | None = None) -> list[tuple[str, str]]:
    """(commit, iso date) newest first for every commit that touched path; all refs unless ref is given."""
    scope = [ref] if ref else ["--all"]
    out = git(repo, "log", *scope, "--format=%H %cs", "--", path)
    return [tuple(line.split(" ", 1)) for line in out.splitlines() if line]


def is_ancestor(repo: Path, commit: str, ref: str) -> bool:
    """True if commit is reachable from ref."""
    res = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, ref], capture_output=True)
    return res.returncode == 0


def checkout_tree(repo: Path, commit: str, dest: Path) -> None:
    """Extract the tree at commit into dest (a clean directory, no .git)."""
    dest.mkdir(parents=True, exist_ok=True)
    arch = subprocess.run(["git", "-C", str(repo), "archive", commit], capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", str(dest)], input=arch.stdout, check=True)
