"""The render corpus (tools/corpus.py): engine render == the vendored render, for every historical state.

Needs clones of both templates (and of the private apps in $KAOS_ENGINE_CORPUS_LAB, optional) in
$KAOS_ENGINE_CORPUS_DIR (skipped otherwise):
    make engine-corpus          # clones into .corpus/ and runs this
"""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import corpus

DIR = os.environ.get("KAOS_ENGINE_CORPUS_DIR")
pytestmark = pytest.mark.skipif(not DIR, reason="KAOS_ENGINE_CORPUS_DIR is not set")
THREADS = 6
# no-profile = the three earliest authoring blobs (Parameter default= instead of value=): the app stays legacy.
ACCEPTED = {"equal", "no-profile", "both-fail-same"}


@pytest.fixture(scope="module")
def results():
    samples = corpus.template_samples(Path(DIR)) + corpus.lab_samples(Path(DIR))
    with ThreadPoolExecutor(THREADS) as pool:
        return {r.sample.name: r for r in pool.map(corpus.render_sample, samples)}


def test_should_cover_both_templates_and_every_listed_app(results):
    names = " ".join(results)
    assert "hera-pipeline-template@" in names and "yolo-pipelines-template@" in names
    for n in range(1, len(corpus.lab_repos()) + 1):
        assert f"app-{n} (HEAD)" in results


def test_should_render_every_state_equal_or_report_why_not(results):
    bad = [f"{r.sample.name}: {r.verdict}: {r.cause}\n    " + "\n    ".join(r.diffs[:6])
           for r in results.values() if r.verdict not in ACCEPTED]
    assert not bad, "engine render differs from the vendored render:\n" + "\n".join(bad)


def test_should_render_every_listed_app_equal(results):
    for r in (r for r in results.values() if r.sample.kind == "lab"):
        assert r.verdict == "equal", f"{r.sample.name}: {r.verdict} {r.cause}"


def test_should_render_every_state_with_a_profile_equal(results):
    """A state whose derive_tree has an engine profile must render equal: no excuses."""
    for r in (r for r in results.values() if r.supported):
        assert r.verdict in ("equal", "both-fail-same"), f"{r.sample.name}: {r.verdict} {r.cause}"


def test_should_only_pick_known_profiles(results):
    for r in (r for r in results.values() if r.supported and r.verdict == "equal"):
        assert r.profile in ("hera", "yolo", "legacy-v1", "yolo-v1")
