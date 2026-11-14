"""The MeluXina twin is retried when its follower pod is lost with its node, and
only then: the retry re-attaches to the same HPCArgoWorkflowJob, a failed job is final.
Argo marks a pod the kubelet terminated (Spot preemption, node pressure) Failed,
not Error, so OnError alone never retried it.
Run:  python -m pytest tests/ -v
"""

import re
import runpy
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import endings, enhance  # noqa: E402

CONTEXT = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())

# Pod messages as the kubelet and Argo write them.
POD_LOST = [
    "Pod was terminated in response to imminent node shutdown.",
    "The node was low on resource: memory. Threshold quantity: 100Mi, available: 9Mi. ",
    "The node had condition: [DiskPressure]. ",
    "Pod was rejected: Node didn't have enough resource: cpu",
    "pod deleted",
]
STEP_FAILED = [
    "Error (exit code 1)",
    "OOMKilled (exit code 137)",
    "Pod was active on the node longer than the specified deadline",
]


def _twin():
    ns = runpy.run_path(str(ROOT / "tests" / "app" / "pipeline.py"), run_name="__pipeline__")
    wf = enhance.enhance(yaml.safe_load(ns["p"].wt.to_yaml()), CONTEXT)
    return next(t for t in wf["spec"]["templates"] if t["name"] == "hpc-follower")


def _retried(status: str, message: str) -> bool:
    """POD_LOSS_RETRY as Argo evaluates it (Go regexp `matches` is a search
    anchored by the expression's own `.*`)."""
    lost = status == "Error" or re.search(".*(%s).*" % endings.POD_LOSS_MESSAGES, message) is not None
    return lost and endings.NO_RESULT_MESSAGE not in message


def test_twin_retries_always_under_the_pod_loss_expression():
    retry = _twin()["retryStrategy"]
    assert retry["retryPolicy"] == "Always"
    assert retry["expression"] == endings.POD_LOSS_RETRY
    assert retry["limit"] == "2"


def test_a_lost_follower_pod_is_retried():
    for message in POD_LOST:
        assert _retried("Failed", message), message
    assert _retried("Error", "")


def test_a_failed_step_is_not_retried():
    for message in STEP_FAILED:
        assert not _retried("Failed", message), message


def test_a_step_that_ends_well_without_its_result_is_not_retried_but_a_lost_node_is():
    """Argo's message for it on v4.0.6: the executor cannot open the output file. It is deterministic, so a
    re-run only spends the GPU again."""
    no_result = ("wait: Error (exit code 64): open /var/run/argo/outputs/parameters/work/output/kaos-result.json: "
                 "no such file or directory")
    assert not _retried("Error", no_result)
    assert _retried("Error", "")
    assert _retried("Error", "some executor error that is not about the result")
    assert _retried("Failed", "Pod was rejected: node shutting down")
    assert "not (lastRetry.message contains 'kaos-result.json')" in endings.POD_LOSS_RETRY
