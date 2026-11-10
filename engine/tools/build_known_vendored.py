"""Generate known-vendored.json: every historical blob of kubecore/{derive_tree,authoring}.py in both
template repos -> the schema profile and the template commits that carried it.

    python engine/tools/build_known_vendored.py --corpus DIR   # DIR holds the two template clones

The chart's `engine.py decide` hashes an app's vendored derive_tree.py and looks it up here to choose
the schema profile; an unknown blob falls back to the KubeApp's template ref. A blob that both
templates carried (their common ancestry) is "ambiguous". `engine_profile` is the engine schema profile whose
DataConfig/QuantizationConfig equal the blob's, or null when none does (older blobs: the app stays legacy).
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

from gitutil import ENGINE_MODULES, MODULE_PATHS, TEMPLATES, blob_at, git, history, is_ancestor, show

ENGINE = Path(__file__).resolve().parent.parent
OUT = ENGINE / "kaos_pipeline_engine" / "known-vendored.json"
SCHEMA_CLASSES = ("DataConfig", "QuantizationConfig")


def _schema_of(nodes: list[ast.stmt]) -> dict[str, list[str]]:
    """{class: ["field=default", ...]} for the structured-config schemas among nodes."""
    out = {}
    for node in nodes:
        if isinstance(node, ast.ClassDef) and node.name in SCHEMA_CLASSES:
            out[node.name] = [f"{b.target.id}={ast.unparse(b.value)}" for b in node.body if isinstance(b, ast.AnnAssign)]
    return out


def blob_schema(source: str) -> dict[str, list[str]]:
    """The DataConfig/QuantizationConfig schemas of a vendored derive_tree.py."""
    return _schema_of(ast.parse(source).body)


def engine_profile_schemas() -> dict[str, dict[str, list[str]]]:
    """The schemas the engine's derive_tree.py carries per profile (its `if/elif SCHEMA_PROFILE == ...` chain)."""
    for node in ast.parse((ENGINE / "kubecore" / "derive_tree.py").read_text()).body:
        if isinstance(node, ast.If) and ast.unparse(node.test).startswith("SCHEMA_PROFILE == "):
            out = {}
            while isinstance(node, ast.If):
                out[ast.literal_eval(node.test.comparators[0])] = _schema_of(node.body)
                node = node.orelse[0] if node.orelse else None
            return out
    raise SystemExit("engine derive_tree.py has no `if SCHEMA_PROFILE == ...` schema chain")


def _behaviour_gap(source: str) -> str | None:
    """Why a blob cannot render equal even with the right schema, else None (the blob must stay legacy).

    The first authoring (2026-07-13/14, three blobs) put the form defaults in `Parameter(default=...)`; every
    later version writes `value=` (Argo reads `value` in arguments.parameters). The engine emits `value=`, so
    its render differs in /spec/arguments/parameters[*] (default -> value). Reproducing the old output would
    reproduce the bug, so these blobs have no engine profile.
    """
    if "value=default" not in source:
        return "emits Parameter(default=...) in arguments.parameters; the engine emits value= (the render differs there)"
    return None


def _engine_profile(repo: Path, commit: str, path: str) -> str | None:
    """The engine profile whose schemas equal this blob's, else None (the app must stay legacy)."""
    source = show(repo, commit, path) or ""
    if _behaviour_gap(source):
        return None
    schema = blob_schema(source)
    return next((p for p, s in engine_profile_schemas().items() if s == schema), None)


def collect(corpus: Path) -> dict:
    """{module.py: {blob: record}} over all refs of both template repos."""
    modules: dict[str, dict[str, dict]] = {f"{m}.py": {} for m in ENGINE_MODULES}
    heads = {}
    for profile, repo_name in TEMPLATES.items():
        repo = corpus / repo_name
        heads[repo_name] = git(repo, "rev-parse", "origin/dev").strip()
        for module in ENGINE_MODULES:
            for path in MODULE_PATHS[module]:
                for commit, date in reversed(history(repo, path)):  # oldest first
                    blob = blob_at(repo, commit, path)
                    if blob is None:  # the commit deleted the file
                        continue
                    rec = modules[f"{module}.py"].setdefault(blob, {"path": path, "templates": {}})
                    tpl = rec["templates"].setdefault(repo_name, {
                        "profile": profile, "first_commit": commit, "first_date": date,
                        "commits": 0, "on_dev": False})
                    tpl["commits"] += 1
                    tpl["last_commit"], tpl["last_date"] = commit, date
                    tpl["on_dev"] = tpl["on_dev"] or is_ancestor(repo, commit, "origin/dev")
    for name, module_blobs in modules.items():
        for rec in module_blobs.values():
            profiles = {t["profile"] for t in rec["templates"].values()}
            rec["profile"] = profiles.pop() if len(profiles) == 1 else "ambiguous"
            first = min(rec["templates"].values(), key=lambda t: t["first_date"])
            repo_name = next(r for r, t in rec["templates"].items() if t is first)
            if name == "derive_tree.py":
                rec["engine_profile"] = _engine_profile(corpus / repo_name, first["first_commit"], rec["path"])
                if rec["engine_profile"] is None:
                    source = show(corpus / repo_name, first["first_commit"], rec["path"]) or ""
                    rec["no_profile_reason"] = _behaviour_gap(source) or "schema equals no engine profile"
            rec["templates"] = [{"repo": r, **t} for r, t in sorted(rec["templates"].items())]
    return {"schema": 1, "generated_from": heads, "modules": modules}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--corpus", required=True, type=Path)
    args = parser.parse_args()
    data = collect(args.corpus)
    OUT.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n")
    for module, blobs in data["modules"].items():
        by_profile: dict[str, int] = {}
        for rec in blobs.values():
            by_profile[rec["profile"]] = by_profile.get(rec["profile"], 0) + 1
        print(f"{module}: {len(blobs)} blobs {by_profile}")


if __name__ == "__main__":
    main()
