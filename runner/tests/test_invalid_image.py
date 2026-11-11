"""An image reference that can never be pulled ends the attempt before staging and submission, and no credential
resubmit is tried for it (live 2026-11-10: ':probe-learn-latest' cost a site job and three resubmits, exit 231)."""
import pytest
from test_runner import FakeSlurm, acc, rig  # noqa: F401

from kaos_hpc_runner.settings import image_problem


@pytest.mark.parametrize("ref", ["zot.example/p/app:v1", "zot.example:5000/app:v1",
                                 "zot.example/p/app@sha256:" + "a" * 64, "zot.example/p/app:v1@sha256:" + "a" * 64])
def test_should_accept_a_registry_repository_and_tag_or_digest(ref):
    assert image_problem(ref) == ""


@pytest.mark.parametrize("ref", [":probe-learn-latest", "app:v1", "zot.example/p/app", "zot.example/p/app:", "",
                                 "zot.example/p/app@sha256:abc"])
def test_should_refuse_a_reference_that_cannot_be_pulled(ref):
    assert image_problem(ref) != ""


def test_should_report_invalid_image_and_submit_nothing(rig):  # noqa: F811
    slurm = FakeSlurm({})
    rig.make(slurm, IMAGE_REF=":probe-learn-latest")
    staged = []
    rig.monkeypatch.setattr(rig.runner.stager, "have_key", lambda: staged.append(1) or False, raising=False)
    assert rig.runner.run() == 0
    assert slurm.submitted == [] and slurm.cancelled == []
    run = rig.kube.run
    assert run["state"] == "FAILED" and run["exitCode"] == 239 and run["terminal"] is True
    assert ":probe-learn-latest" in run["message"] and "siteJobID" not in {k for k, v in run.items() if v}
    assert staged == [], "nothing is staged for a reference that cannot be pulled"
