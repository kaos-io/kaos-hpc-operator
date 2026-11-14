"""A step on an HPC class that is longer than one run's access to its data and tracking (PRD kaos#1911 F-40, issue
2212): refused by compose before any step starts, unless it is resumable (the controller then runs it as
segments). The figure is the controller's (internal/timelimit/timelimit.go); a test keeps the two equal."""

import re

import pytest

from kubecore import compose
from kubecore.compose import ComposeError
from kubecore.run_settings import TOKEN_COVER_MINUTES
from paths import ENGINE

STEPS = [{"name": "train"}, {"name": "report"}]
HPC = {"meluxina-gpu"}


def check(walltime, chosen="meluxina-gpu", lead=0, step="train"):
    settings = {step: {"walltime": walltime}}
    leads = [f"{step}={lead}"] if lead else []
    compose.run_token_cover(STEPS, settings, [f"{step}={chosen}"] if chosen else [], HPC, leads)


def test_the_cover_is_the_one_the_controller_uses():
    go = (ENGINE.parent / "internal" / "timelimit" / "timelimit.go").read_text()
    life = int(re.search(r"TokenLifeMinutes\s*=\s*(\d+)\s*\*\s*60", go).group(1)) * 60
    margin = int(re.search(r"TokenMarginMinutes\s*=\s*(\d+)", go).group(1))
    assert TOKEN_COVER_MINUTES == life - margin == 690


@pytest.mark.parametrize("walltime", [1, 400, TOKEN_COVER_MINUTES])
def test_a_step_up_to_the_cover_is_accepted(walltime):
    check(walltime)


def test_the_limit_is_written_in_whole_and_half_hours_like_the_controllers():
    with pytest.raises(ComposeError, match=r"train-walltime is 720 minutes \(12 hours\)"):
        check(720)


def test_a_step_one_minute_over_the_cover_is_refused_with_the_limit_the_step_and_both_fixes():
    with pytest.raises(ComposeError) as err:
        check(TOKEN_COVER_MINUTES + 1)
    message = str(err.value)
    assert "train-walltime is 691 minutes (11.5 hours)" in message
    assert "step 'train'" in message
    assert "at most 11.5 hours" in message
    assert "walltime=" in message and "resumable=True" in message and "checkpoint_every=" in message
    for internal in ("token", "wallet", "bearer", "Zitadel", "Argo", "kubectl", "namespace", "lakeFS", "MLflow"):
        assert internal.lower() not in message.lower()


def test_a_step_on_a_cluster_class_is_not_limited():
    check(2880, chosen="gpu-a100")
    check(2880, chosen="")


def test_a_resumable_step_longer_than_the_cover_is_accepted_up_to_its_runs():
    check(3 * TOKEN_COVER_MINUTES, lead=10)
    check(6 * TOKEN_COVER_MINUTES, lead=10)


def test_a_resumable_step_needing_more_runs_than_continue_is_refused():
    with pytest.raises(ComposeError) as err:
        check(6 * TOKEN_COVER_MINUTES + 1, lead=10)
    message = str(err.value)
    assert "train-walltime" in message and "6 runs" in message and "4140 minutes" in message


def test_run_settings_refuses_nothing_itself_so_the_form_values_stay_checked_in_one_place():
    assert compose.run_settings(STEPS, ["train=24h"], [], ["train=10"]) == {"train": {"walltime": 1440}}
