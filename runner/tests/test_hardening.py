"""Hardening of the HPC job path before a client release (PRD kaos#1911): what a job script trusts, what the step
sees, what is left on the site, what a failed job shows.

1. The registry token is presented only to a host parsed out of the image reference, and a reference that does not
   parse never reaches a script.
2. The step container gets an allow-list of variables (`--cleanenv`); no token is written to a place that outlives
   the job; the registry token is unset once the image is there.
3. Everything the job creates is private to the site user.
4. The tail of a failed job's output is redacted before it is copied into the job comment.

The whole job script runs here in bash against the stand-in node of test_step_contract (a fake Apptainer, a fake
Slurm), so the script under test is the one the runner submits. No token is printed by any test: they are
compared by value only.
"""
import json
import os
import signal
import stat
import subprocess
import sys
import time
import urllib.request

import helpers
import pytest
import test_step_contract as tsc
from test_jobscript import FakeLakeFS
from test_runner import FakeSlurm, rig  # noqa: F401

from kaos_hpc_runner import jobscript, settings
from kaos_hpc_runner import runner as runmod

WALLET = "WALLET-SECRET-" + "w" * 24          # stands in for the data/MLflow bearer
REGISTRY = "REGISTRY-SECRET-" + "r" * 24       # stands in for the registry token
GAR = "europe-docker.pkg.dev/proj/yolo:v0.0.0"

# ---------------------------------------------------------------- 1. the reference

HOSTILE = [
    "evil.example/x -docker.pkg.dev/p/app:v1",          # a space
    "zot.example/proj/app x:v1",
    "zot.example/p/app:v1\nrm -rf /",                    # a newline
    "zot.example/p/app:v1\n",
    "zot.example/p/app:v1\x1b[0m",                       # a control character
    "zot.example/p/$(id):v1",                            # command substitution
    "zot.example/p/`id`:v1",
    "zot.example/p/app:v1;id",
    "zot.example/p/app:v1'",
    'zot.example/p/app:v1"',
    "zot.example/p/app:v1&&id",
    "zot.example/p/app:v1|id",
    "user@zot.example/p/app:v1",                         # userinfo
    "zot.example/P/app:v1",                              # not a name a registry has
    "zot.example//app:v1",
    "zot.example/../app:v1",
    "zot.example/p/app:v1 --docker-login",
    "zot.example/p/app:v1@sha256:" + "a" * 63,
]


@pytest.mark.parametrize("ref", HOSTILE, ids=range(len(HOSTILE)))
def test_a_hostile_reference_does_not_parse(ref):
    assert settings.image_problem(ref) != "" and settings.parse_image(ref) is None


@pytest.mark.parametrize("ref,parts", [
    ("zot.example/p/app:v1", ("zot.example", "p/app", "v1", None)),
    ("zot.example:5000/app:v1", ("zot.example:5000", "app", "v1", None)),
    ("europe-docker.pkg.dev/p/r/app@sha256:" + "a" * 64, ("europe-docker.pkg.dev", "p/r/app", None, "sha256:" + "a" * 64)),
    ("localhost/app:v1", ("localhost", "app", "v1", None)),
])
def test_a_normal_reference_parses_into_host_path_tag_and_digest(ref, parts):
    assert settings.image_problem(ref) == "" and settings.parse_image(ref) == parts


@pytest.mark.parametrize("host,ok", [
    ("europe-docker.pkg.dev", True), ("us-central1-docker.pkg.dev", True),
    ("evil.example", False), ("zot.example:5000", False), ("", False),
    ("x.europe-docker.pkg.dev", False),                  # a name below a region's, not the region's
    ("europe-docker.pkg.dev.evil.example", False), ("-docker.pkg.dev", False), ("europe-docker.pkg.dev:443", False),
])
def test_the_token_goes_only_to_an_artifact_registry_region_host(host, ok):
    assert settings.registry_token_host(host) is ok


@pytest.mark.parametrize("ref", HOSTILE[:12], ids=range(12))
def test_a_hostile_reference_is_refused_before_any_network_call_or_submit(rig, ref):  # noqa: F811
    calls = []
    rig.monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: calls.append(a) or (_ for _ in ()).throw(OSError("net")))
    slurm = FakeSlurm({})
    rig.make(slurm, IMAGE_REF=ref)
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and calls == [], "no registry, metadata, lakeFS or Slurm call"
    assert rig.kube.run["state"] == "FAILED" and rig.kube.run["exitCode"] == 239


def test_a_script_is_never_written_for_a_reference_that_does_not_parse():
    cfg = helpers.settings(IMAGE_REF="zot.example/p/$(id):v1")
    cfg.check_job()
    r = runmod.Runner(cfg, slurm=object(), stager=object(), reporter=object(), logs=object(), mint=lambda: "w")
    with pytest.raises(ValueError, match="cannot be used"):
        r.site_job(REGISTRY, "")


def test_the_registry_token_is_not_put_in_the_job_for_a_host_that_may_not_have_it():
    for image, has in [(GAR, True), ("zot.example/p/yolo:v0.0.0", False), ("evil.example/europe-docker.pkg.dev/p/a:v1", False)]:
        cfg = helpers.settings(IMAGE_REF=image)
        cfg.check_job()
        r = runmod.Runner(cfg, slurm=object(), stager=object(), reporter=object(), logs=object(), mint=lambda: "w")
        r.wallet = "w"
        old, runmod.wallet.fetch_workdir = runmod.wallet.fetch_workdir, lambda i, t: ""
        try:
            _, env, _, _ = r.site_job(REGISTRY, "")
        finally:
            runmod.wallet.fetch_workdir = old
        assert ("REG_TOKEN=" + REGISTRY in env) is has
