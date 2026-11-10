"""The twin template the engine renders must satisfy the follower in this repo (D-58/D-63: one version).

The golden is the engine's own render of the follower template (test_meluxina_follower.py keeps it
current). Here its environment is checked against what internal/follower/config.go reads. The Go side
of the same contract is internal/follower/engine_contract_test.go, which feeds the golden to
ConfigFromEnv.
"""

import re

import yaml

from paths import ENGINE

REPO = ENGINE.parent
GOLDEN = ENGINE / "tests" / "template" / "golden" / "meluxina_follower.yaml"
CONFIG_GO = (REPO / "internal" / "follower" / "config.go").read_text()
TYPES_GO = (REPO / "api" / "v1alpha1" / "hpcargoworkflowjob_types.go").read_text()


def _template() -> dict:
    return yaml.safe_load(GOLDEN.read_text())


def _env() -> dict:
    return {e["name"]: e for e in _template()["container"]["env"]}


def _read_by_follower() -> set:
    allowed = re.search(r"AllowedEnvNames = \[\]string\{([^}]*)\}", TYPES_GO).group(1)
    return set(re.findall(r'get\("([A-Z_]+)"\)', CONFIG_GO)) | set(re.findall(r'"([A-Z_]+)"', allowed))


def test_should_run_the_follower_binary_this_repo_ships():
    assert _template()["container"]["command"] == ["/follower"]
    assert 'go build -a -o manager cmd/main.go' in (REPO / "Dockerfile").read_text()
    assert "./cmd/follower" in (REPO / "Dockerfile").read_text()


def test_should_set_every_variable_the_follower_requires():
    required = set(re.findall(r'"([A-Z_]+)": c\.', CONFIG_GO)) | {"SLURM_TIME_LIMIT"}
    assert required <= set(_env()), f"the engine's follower template misses {required - set(_env())}"


def test_should_set_only_variables_the_follower_reads():
    unknown = set(_env()) - _read_by_follower()
    assert not unknown, f"the engine sets {unknown}, which the follower ignores (contract skew)"


def test_should_run_the_step_as_the_submitter_service_account_only_in_this_template():
    assert _template()["serviceAccountName"] == "hpc-submitter"


def test_should_retry_pod_loss_but_not_an_idle_end():
    expression = _template()["retryStrategy"]["expression"]
    assert "KaosIdle" not in expression.replace("lastRetry", "")  # the watchdog's end must not be retried
    assert "imminent node shutdown" in expression
