"""The render corpus: every historical state of both templates and the current lab apps, rendered twice.

For each sample the app tree is extracted from git and rendered (a) by the repo's OWN vendored kubecore
(`python pipeline.py`, exactly what CI did before D-79) and (b) by the engine runner
(`python -m kaos_engine.render`). The parsed YAML of out/raw-workflow-template.yaml must be equal; a
difference is reported with its cause.

    python engine/tools/corpus.py --corpus DIR [--report engine/CORPUS_REPORT.md]

DIR holds clones of novelcore/hera-pipeline-template, novelcore/yolo-pipelines-template and
the private apps named in $KAOS_ENGINE_CORPUS_LAB (never committed; the report anonymises them as app-N).
The interpreter running this must have the pinned render dependencies (engine/requirements.txt).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from gitutil import TEMPLATES, blob_at, checkout_tree, git, lab_repos

ENGINE = Path(__file__).resolve().parent.parent
KNOWN = ENGINE / "kaos_pipeline_engine" / "known-vendored.json"
RAW = Path("out") / "raw-workflow-template.yaml"
RENDER_TIMEOUT = 300
MAX_DIFFS = 12


@dataclass
class Sample:
    """One app tree to render: a commit of a template repo or the HEAD of a lab app."""

    name: str
    repo: Path
    commit: str
    template_profile: str
    kind: str  # "template" | "lab"
    note: str = ""


@dataclass
class Result:
    """Outcome of one sample."""

    sample: Sample
    verdict: str  # equal | differs | no-profile | both-fail-same | vendored-fails | engine-fails
    profile: str = ""
    supported: bool = True
    cause: str = ""
    diffs: list[str] = field(default_factory=list)


def template_samples(corpus: Path) -> list[Sample]:
    """Every dev-lineage commit that changed pipeline.py, the two engine modules or config/."""
    out = []
    for profile, repo_name in TEMPLATES.items():
        repo = corpus / repo_name
        commits = git(repo, "log", "origin/dev", "--format=%H %cs", "--", "pipeline.py",
                      "kubecore/authoring.py", "kubecore/derive_tree.py", "config").splitlines()
        head = git(repo, "rev-parse", "origin/dev").strip()
        for line in reversed(commits):
            commit, date = line.split()
            out.append(Sample(f"{repo_name}@{commit[:7]} ({date})", repo, commit, profile, "template"))
        if head not in {s.commit for s in out if s.repo == repo}:
            out.append(Sample(f"{repo_name}@{head[:7]} (dev head)", repo, head, profile, "template"))
    return out


def lab_samples(corpus: Path) -> list[Sample]:
    """The current (HEAD) state of each private app named in $KAOS_ENGINE_CORPUS_LAB, anonymised as app-N."""
    out = []
    for n, (repo_name, profile) in enumerate(lab_repos(), start=1):
        repo = corpus / repo_name
        out.append(Sample(f"app-{n} (HEAD)", repo, git(repo, "rev-parse", "HEAD").strip(), profile, "lab"))
    return out


def profile_for(derive_tree_blob: str | None, template_profile: str) -> tuple[str, bool]:
    """(profile, supported) as `engine.py decide` picks them: the blob's engine profile; a known blob with
    no engine profile is unsupported (the app stays legacy); an unknown blob uses the KubeApp's template."""
    known = json.loads(KNOWN.read_text())["modules"]["derive_tree.py"].get(derive_tree_blob or "")
    if known is None:
        return template_profile, True
    return known["engine_profile"] or template_profile, known["engine_profile"] is not None


def _run(cmd: list[str], cwd: Path, env_extra: dict[str, str]) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("KUBECORE_", "KAOS_ENGINE", "PYTHONPATH"))}
    env.update(env_extra, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=RENDER_TIMEOUT)


def _load(tree: Path) -> dict | None:
    path = tree / RAW
    return yaml.safe_load(path.read_text()) if path.is_file() else None


def _last_line(res: subprocess.CompletedProcess) -> str:
    """The error: from the last exception line to the end of the output, on one line."""
    lines = [ln.strip() for ln in (res.stderr or res.stdout).strip().splitlines() if ln.strip()]
    if not lines:
        return f"exit {res.returncode}"
    start = max((i for i, ln in enumerate(lines) if "Error" in ln or "Exception" in ln), default=len(lines) - 1)
    return " / ".join(lines[start:])[:300]


def diff_paths(a, b, path: str = "") -> list[str]:
    """Human-readable differences between two parsed documents."""
    if type(a) is not type(b):
        return [f"{path or '/'}: {_short(a)} -> {_short(b)}"]
    if isinstance(a, dict):
        out = []
        for key in sorted(set(a) | set(b), key=str):
            where = f"{path}/{key}"
            if key not in a:
                out.append(f"{where}: (absent) -> {_short(b[key])}")
            elif key not in b:
                out.append(f"{where}: {_short(a[key])} -> (absent)")
            else:
                out += diff_paths(a[key], b[key], where)
        return out
    if isinstance(a, list):
        if len(a) != len(b):
            return [f"{path}: list length {len(a)} -> {len(b)}"]
        return [d for i, (x, y) in enumerate(zip(a, b, strict=True)) for d in diff_paths(x, y, f"{path}[{i}]")]
    return [] if a == b else [f"{path}: {_short(a)} -> {_short(b)}"]


def _short(value, limit: int = 70) -> str:
    text = json.dumps(value, default=str)
    return text if len(text) <= limit else text[:limit] + "..."


def render_sample(sample: Sample, python: str = sys.executable) -> Result:
    """Render one sample both ways and compare."""
    with tempfile.TemporaryDirectory(prefix="corpus-") as tmp:
        tree = Path(tmp) / "app"
        checkout_tree(sample.repo, sample.commit, tree)
        blob = blob_at(sample.repo, sample.commit, "kubecore/derive_tree.py")
        profile, supported = profile_for(blob, sample.template_profile)
        ref = _run([python, "pipeline.py"], tree, {})
        ref_doc = _load(tree)
        (tree / RAW).unlink(missing_ok=True)
        eng = _run([python, "-m", "kaos_engine.render", "pipeline.py", "--repo", str(tree), "--profile", profile,
                    "--engine", str(ENGINE)], tree, {"PYTHONPATH": str(ENGINE)})
        eng_doc = _load(tree)
    res = Result(sample, "", profile, supported)
    if ref.returncode and eng.returncode:
        same = _last_line(ref) == _last_line(eng)
        res.verdict = "both-fail-same" if same else "both-fail-differ"
        res.cause = f"vendored: {_last_line(ref)} | engine: {_last_line(eng)}"
    elif ref.returncode:
        res.verdict, res.cause = "vendored-fails", _last_line(ref)
    elif eng.returncode:
        res.verdict, res.cause = "engine-fails", _last_line(eng)
        if not supported:
            res.verdict = "no-profile"
            res.cause = f"no engine schema profile for derive_tree {blob[:7]} (the app stays legacy): {res.cause}"
    elif ref_doc == eng_doc:
        res.verdict = "equal"
    else:
        res.verdict = "differs"
        res.diffs = diff_paths(ref_doc, eng_doc)
        res.cause = explain(res.diffs)
    return res


def explain(diffs: list[str]) -> str:
    """A first-pass cause for a difference; every `differs` is still read by a human (CORPUS_REPORT.md)."""
    text = "\n".join(diffs)
    if "platform.kubecore.io/checkpoint-every" in text or "platform.kubecore.io/idle-timeout" in text:
        return "authoring: the vendored copy predates the step() options checkpoint_every / idle_timeout"
    return "unexplained"


def run_all(corpus: Path, python: str = sys.executable) -> list[Result]:
    """All samples, templates oldest first, then the lab apps."""
    return [render_sample(s, python) for s in template_samples(corpus) + lab_samples(corpus)]


def markdown(results: list[Result]) -> str:
    """The report committed as engine/CORPUS_REPORT.md."""
    counts: dict[str, int] = {}
    for r in results:
        counts[r.verdict] = counts.get(r.verdict, 0) + 1
    lines = ["# Engine render corpus", "",
             "Generated by `engine/tools/corpus.py` (do not edit by hand). Each sample is rendered by the repo's own",
             "vendored kubecore (`python pipeline.py`) and by the engine runner; parsed YAML is compared.", "",
             "Totals: " + ", ".join(f"{v} {k}" for k, v in sorted(counts.items())), "",
             "| sample | kind | engine profile | verdict | cause |", "|---|---|---|---|---|"]
    for r in results:
        profile = r.profile if r.supported else "- (none)"
        cause = r.cause.replace("|", "/")
        lines.append(f"| {r.sample.name} | {r.sample.kind} | {profile} | {r.verdict} | {cause} |")
    for r in results:
        if r.diffs:
            lines += ["", f"### {r.sample.name}", "", "```"] + r.diffs[:MAX_DIFFS]
            if len(r.diffs) > MAX_DIFFS:
                lines.append(f"... {len(r.diffs) - MAX_DIFFS} more")
            lines.append("```")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    results = run_all(args.corpus)
    text = markdown(results)
    if args.report:
        args.report.write_text(text)
    print(text)


if __name__ == "__main__":
    main()
