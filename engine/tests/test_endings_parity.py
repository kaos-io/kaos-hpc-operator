"""One table of endings, two readers (PRD kaos#1911 D-64). The cluster's retry rule is an Argo expression; the HPC
controller's is Go (internal/resume). Both read endings.json; the same cases are fed to both
(this test, and TestClassifyDecidesTheSharedCasesLikeTheEngine in internal/resume)."""
import json
import re
from pathlib import Path

import pytest

from kubecore import endings, resilience

HERE = Path(__file__).parent
CASES = json.loads((HERE / "data" / "endings_cases.json").read_text())
GO_COPY = HERE.parents[1] / "internal" / "resume" / "endings.json"


def evaluate(expression: str, pod: dict) -> bool:
    """The small subset of Argo's expression language the retry rules use, evaluated on one finished pod."""
    last = {"status": pod["status"], "message": pod["message"], "exitCode": pod["exit_code"]}
    text = expression
    text = re.sub(r"lastRetry\.(\w+) matches '([^']*)'",
                  lambda m: "bool(re.search(%r, last[%r]))" % ("^" + m.group(2) + "$", m.group(1)), text)
    text = re.sub(r"lastRetry\.(\w+) contains '([^']*)'", lambda m: "(%r in last[%r])" % (m.group(2), m.group(1)), text)
    text = re.sub(r"lastRetry\.(\w+) startsWith '([^']*)'", lambda m: "last[%r].startswith(%r)" % (m.group(1), m.group(2)), text)
    text = re.sub(r"lastRetry\.(\w+) == '([^']*)'", lambda m: "(last[%r] == %r)" % (m.group(1), m.group(2)), text)
    assert "lastRetry" not in text, text
    return bool(eval(text, {"re": re, "last": last}))  # noqa: S307 - our own expression, test only


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_the_cluster_rule_decides_every_shared_case_as_the_table_says(case):
    pod = case["pod"]
    again_after_loss = evaluate(resilience.RETRY_EXPRESSION, pod)
    again_after_limit = evaluate(resilience.CONTINUE_EXPRESSION, pod)
    assert again_after_loss == (case["decision"] == "machine_lost"), case["name"]
    assert again_after_limit == (case["decision"] in ("machine_lost", "limit")), case["name"]


def test_the_engines_table_and_the_controllers_copy_are_the_same_file():
    assert GO_COPY.read_bytes() == (Path(endings.__file__).with_name("endings.json")).read_bytes()


def test_the_budget_is_one_constant_for_both_executors():
    assert (endings.MAX_CONTINUATIONS, endings.MAX_RESTARTS) == (5, 2)
    assert resilience.retry_strategy()["limit"] == "7"
    assert resilience.retry_strategy(shared=True)["limit"] == "2"
