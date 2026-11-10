"""Generate compat-surface.json: every module-level name that kubecore/authoring.py and
kubecore/derive_tree.py ever had in either template repo (all refs, oldest layout included).

    python engine/tools/build_compat_surface.py --corpus DIR

A vendored copy that a pipeline.py or a client's tests monkeypatch reaches for `authoring.<name>`; the
engine must keep every name (tests/test_compat_surface.py). A name that was deliberately dropped is
listed under "removed" with the reason, never silently.
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

from gitutil import ENGINE_MODULES, MODULE_PATHS, TEMPLATES, blob_at, history, show

OUT = Path(__file__).resolve().parent.parent / "kaos_pipeline_engine" / "compat-surface.json"


def module_names(source: str) -> dict[str, str]:
    """{name: kind} for the top-level definitions, assignments and imports of a module."""
    names: dict[str, str] = {}
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names[node.name] = "function"
        elif isinstance(node, ast.ClassDef):
            names[node.name] = "class"
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for sub in ast.walk(target):
                    if isinstance(sub, ast.Name):
                        names[sub.id] = "assign"
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names[(alias.asname or alias.name).split(".")[0]] = "import"
    return names


def collect(corpus: Path) -> dict:
    """{module: {name: {kind, first_seen: {repo, commit, date}}}} over all history."""
    surface: dict[str, dict] = {m: {} for m in ENGINE_MODULES}
    seen_blobs: set[str] = set()
    for repo_name in TEMPLATES.values():
        repo = corpus / repo_name
        for module in ENGINE_MODULES:
            for path in MODULE_PATHS[module]:
                for commit, date in reversed(history(repo, path)):
                    blob = blob_at(repo, commit, path)
                    if blob is None or blob in seen_blobs:
                        continue
                    seen_blobs.add(blob)
                    for name, kind in module_names(show(repo, commit, path) or "").items():
                        surface[module].setdefault(name, {
                            "kind": kind, "first_seen": {"repo": repo_name, "commit": commit, "date": date}})
    return {"schema": 1, "modules": surface, "removed": {}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--corpus", required=True, type=Path)
    args = parser.parse_args()
    data = collect(args.corpus)
    if OUT.exists():  # keep the human-written reasons for dropped names
        data["removed"] = json.loads(OUT.read_text()).get("removed", {})
    OUT.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n")
    for module, names in data["modules"].items():
        print(f"{module}: {len(names)} names")


if __name__ == "__main__":
    main()
