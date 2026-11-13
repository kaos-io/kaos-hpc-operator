"""A finished pipeline run is expired by the cluster (PRD kaos#1911, cleanup): the delivered WorkflowTemplate carries a
time-to-live and a pod cleanup rule, which every run made from it inherits. 35 days by default (see
run_retention_seconds: the HPC budget month); the pipeline context may
say `runRetentionDays` for one project.
Run:  python -m pytest tests/ -v
"""
import copy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance  # noqa: E402

CONTEXT = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
CONTEXT.pop("hpc")


BASE_HPC = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
BASE_HPC["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")


def spec_of(ctx=None, author=None, steps="control"):
    with authoring.pipeline("t") as p:
        if steps == "hpc":
            authoring.step("train", kind="train", produces=[authoring.model("m")])
        else:
            authoring.step("report", kind="control")
    wft = yaml.safe_load(p.wt.to_yaml())
    if author:
        wft["spec"].update(copy.deepcopy(author))
    return enhance.enhance(wft, ctx or CONTEXT)["spec"]


def test_a_run_expires_after_thirty_five_days_by_default_succeeded_or_failed():
    spec = spec_of()
    assert spec["ttlStrategy"] == {"secondsAfterSuccess": 35 * 86400, "secondsAfterFailure": 35 * 86400}


def test_the_default_outlives_every_calendar_month_and_thirty_one_days_would_not():
    # The HPC budget is a calendar month and the ledger is derived from the job records, which go with their run. A run that
    # ends at t is collected at t + ttl; it must not be collected before the month of t has closed. The worst cases: the first
    # second of a 31-day month (the month closes 31 days later, the run is kept ttl) and the last second of it (it closes at
    # once, so any ttl is enough). Only the first can fail.
    from datetime import datetime, timedelta, timezone

    def closes(t):
        return (t.replace(day=1) + timedelta(days=32)).replace(day=1)

    start = datetime(2026, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    last = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    ttl = enhance.run_retention_seconds({}, hpc_steps=True)
    for ended in (start, last):
        assert ended + timedelta(seconds=ttl) >= closes(ended), ended
    assert start + timedelta(days=31) - timedelta(seconds=1) < closes(start), "31 days minus a second still collects inside the month"
    assert start + timedelta(days=30) < closes(start), "so 30 days (the value before) would collect a run of the 1st inside its month"
    assert enhance.HPC_MIN_RETENTION_DAYS * 86400 >= 31 * 86400


def test_pods_of_a_run_are_kept_seven_days_after_it_ends_succeeded_or_failed_and_then_deleted():
    # The step log lives only in the pod: completion-based collection (not OnWorkflowSuccess, which would delete the
    # pods of a succeeded run at once), delayed by 7 days. The run record outlives its pods (30 days).
    gc = spec_of()["podGC"]
    assert gc == {"strategy": "OnWorkflowCompletion", "deleteDelayDuration": "168h"}
    assert 168 * 3600 < spec_of()["ttlStrategy"]["secondsAfterSuccess"]


def test_ttl_and_podgc_are_fields_of_the_template_a_workflow_is_built_from():
    # Argo (docs, "Workflow Restrictions"): a Workflow with a workflowTemplateRef takes its spec from the template, and
    # ttlStrategy and podGC are among the fields a submission may still override. The delivered object is a
    # WorkflowTemplate, so both sit in spec where a submitted run inherits them.
    wft = {"apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "spec": spec_of()}
    assert {"ttlStrategy", "podGC"} <= set(wft["spec"])


def hpc_ctx(**extra):
    ctx = copy.deepcopy(BASE_HPC)
    ctx.update(extra)
    return ctx


def test_a_project_may_set_the_days_for_a_pipeline_with_no_step_that_can_run_on_hpc():
    spec = spec_of(dict(CONTEXT, runRetentionDays=7))
    assert spec["ttlStrategy"] == {"secondsAfterSuccess": 7 * 86400, "secondsAfterFailure": 7 * 86400}
    # an HPC project whose pipeline has only a control step has none either
    assert spec_of(hpc_ctx(runRetentionDays=1))["ttlStrategy"]["secondsAfterSuccess"] == 86400


def test_runs_with_hpc_steps_are_kept_at_least_32_days():
    for days in (1, 7, 31):
        with pytest.raises(enhance.EnhanceError, match="runs with HPC steps are kept at least 32 days, so the month's HPC spend stays counted"):
            spec_of(hpc_ctx(runRetentionDays=days), steps="hpc")
    assert spec_of(hpc_ctx(runRetentionDays=32), steps="hpc")["ttlStrategy"]["secondsAfterSuccess"] == 32 * 86400
    assert spec_of(hpc_ctx(), steps="hpc")["ttlStrategy"]["secondsAfterSuccess"] == 35 * 86400


@pytest.mark.parametrize("bad", [0, -1, 1.5, "30", True, None, 4000])
def test_a_days_value_that_is_not_a_whole_number_from_1_to_3650_is_refused(bad):
    with pytest.raises(enhance.EnhanceError, match="runRetentionDays"):
        spec_of(dict(CONTEXT, runRetentionDays=bad))


def test_what_the_pipeline_author_already_set_is_kept():
    spec = spec_of(author={"ttlStrategy": {"secondsAfterCompletion": 60}, "podGC": {"strategy": "OnPodCompletion"}})
    assert spec["ttlStrategy"] == {"secondsAfterCompletion": 60}
    assert spec["podGC"] == {"strategy": "OnPodCompletion"}
