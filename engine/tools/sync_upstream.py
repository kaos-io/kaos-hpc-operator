"""Refresh engine/kubecore/ from a hera-pipeline-template commit (the commit the dev CI chart pins as
`platformEnhancer.ref`) and rewrite UPSTREAM.json.

    python engine/tools/sync_upstream.py --corpus DIR --commit <40-hex>

Every file is byte-copied except derive_tree.py: its pristine copy goes to engine/upstream/derive_tree.py
and the two engine patches (lazy CONFIG_DIR, schema profiles) must be re-applied to
engine/kubecore/derive_tree.py by hand; tests/test_engine_patch.py then proves nothing else differs.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path

from gitutil import git

ENGINE = Path(__file__).resolve().parent.parent
PATCHED = ["derive_tree.py"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    repo = args.corpus / "hera-pipeline-template"
    commit = git(repo, "rev-parse", args.commit).strip()
    files = {}
    for line in git(repo, "ls-tree", "-r", commit, "kubecore/").splitlines():
        meta, path = line.split("\t")
        rel = path.removeprefix("kubecore/")
        files[rel] = meta.split()[2]
        target = ENGINE / ("upstream" if rel in PATCHED else "kubecore") / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(subprocess.run(["git", "-C", str(repo), "cat-file", "blob", meta.split()[2]],
                                          capture_output=True, check=True).stdout)
    on_disk = {p.relative_to(ENGINE / "kubecore") for p in (ENGINE / "kubecore").rglob("*") if p.is_file()}
    stale = on_disk - {Path(f) for f in files}
    for path in stale:
        if "__pycache__" not in path.parts:
            (ENGINE / "kubecore" / path).unlink()
    shutil.rmtree(ENGINE / "kubecore" / "__pycache__", ignore_errors=True)
    data = {"repo": "novelcore/hera-pipeline-template", "commit": commit, "patched": PATCHED, "files": files}
    (ENGINE / "UPSTREAM.json").write_text(json.dumps(data, indent=1, sort_keys=True) + "\n")
    print(f"synced {len(files)} files from {commit}; re-apply the derive_tree.py patches")


if __name__ == "__main__":
    main()
