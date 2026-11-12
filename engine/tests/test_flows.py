"""Conditions on a step's value, the one-name rule, and the step's folder (PRD kaos#1911, pipelines authoring UX)."""

import copy
import re
import textwrap
from pathlib import Path

import pytest
import yaml
from test_authoring_contract import refused, render

from helpers import copy_app, run_render
from kubecore import enhance
from kubecore.step_contract import (
    canonical_when_text,
    json_number_text,
    result_of,
    result_value,
    step_folder,
    value_test,
    when_text_problem,
)

CTX = yaml.safe_load((Path(__file__).resolve().parents[1] / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def tasks_of(wft):
    return {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}


def verdict_pipeline(a, when="evaluate.passed == true"):
    evaluate = a.step("evaluate", kind="evaluate", gpu=False)
    a.step("publish", kind="publish", needs=[evaluate], when=when)


# ----------------------------------------------------------------------------------- when= on a step's value

def test_a_when_on_a_needed_steps_value_reads_its_result():
    publish = tasks_of(render(verdict_pipeline))["publish"]
    assert publish["when"] == "{{=" + value_test("evaluate", "passed", "==", "true") + "}}"
    assert 'tasks["evaluate"].outputs.parameters["kaos-result"]' in publish["when"]


def test_a_missing_value_fails_the_condition_with_a_sentence_naming_step_and_key():
    expression = result_value("evaluate", "passed")
    assert "?? int(" in expression and "string(" not in expression, "an absent value must not quietly compare as empty text"
    assert "step 'evaluate' gave no value 'passed'" in expression


def test_the_sentinel_stands_in_for_a_step_that_did_not_run():
    assert result_of("evaluate") in result_value("evaluate", "passed")


def test_step_and_form_terms_join_with_and_and_or():
    def build(a):
        evaluate = a.step("evaluate", kind="evaluate")
        a.step("publish", kind="publish", needs=[evaluate],
               when="evaluate.passed == true and experiment-name != off or evaluate.score != 0")
    when = tasks_of(render(build))["publish"]["when"]
    assert when.count("&&") == 1 and when.count("||") == 1
    assert "workflow.parameters['experiment-name'] != 'off'" in when
    assert when.count('tasks["evaluate"].outputs.parameters["kaos-result"]') == 6, "three readings for each of the two terms"


def test_a_value_of_a_step_that_is_not_needed_is_refused_with_the_fix():
    def build(a):
        a.step("evaluate", kind="evaluate")
        a.step("publish", kind="publish", when="evaluate.passed == true")
    assert refused(build) == (
        "step 'publish': when= reads a value of step 'evaluate', which this step does not need. A step sees the "
        "result of the steps in its needs= only: add evaluate to needs=[...] of step 'publish'.")


def test_a_value_name_that_is_not_a_keyword_name_is_refused():
    def build(a):
        evaluate = a.step("evaluate", kind="evaluate")
        a.step("publish", kind="publish", needs=[evaluate], when="evaluate.pass-rate == 1")
    assert "which is not a name step.result() could have been given" in refused(build)


def test_a_when_on_a_step_name_that_does_not_exist_is_an_unknown_field():
    def build(a):
        a.step("publish", kind="publish", when="evaluat.passed == true")
    assert "names 'evaluat.passed', which is not a field of the run form" in refused(build)


def test_the_refusal_names_both_forms_and_the_operators():
    message = refused(lambda a: a.step("publish", kind="publish", when="score > 0.5"))
    assert 'when="mode != off"' in message and 'when="evaluate.passed == true"' in message
    assert "== or !=" in message and "no < or >" in message


# ------------------------------------------------------- the same condition when the needed step runs on HPC

def hpc_render(build):
    ctx = copy.deepcopy(CTX)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        build(authoring)
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)


def test_the_condition_reads_the_hpc_twin_when_the_needed_step_may_run_there():
    tasks = tasks_of(hpc_render(verdict_pipeline))
    assert "evaluate-hpc" in tasks, "the needed step has an HPC twin in this render"
    when = tasks["publish"]["when"]
    assert result_of("evaluate", "evaluate-hpc") in when
    assert result_of("evaluate") not in when.replace(result_of("evaluate", "evaluate-hpc"), "")
    assert "evaluate-hpc.Succeeded" in tasks["publish"]["depends"], "the twin is in depends, or the engine hides its outputs"
    assert re.search(r"evaluate-hpc\b", tasks["publish"]["depends"])


def test_a_step_that_is_itself_routed_keeps_the_condition_on_both_tasks():
    def build(a):
        evaluate = a.step("evaluate", kind="evaluate")
        a.step("report", kind="transform", needs=[evaluate], when="evaluate.passed == true",
               produces=[a.artifact("report")])
    tasks = tasks_of(hpc_render(build))
    pair = result_of("evaluate", "evaluate-hpc")
    assert pair in tasks["report"]["when"] and pair in tasks["report-hpc"]["when"]


def test_the_value_is_the_same_text_on_the_cluster_and_on_hpc():
    cluster = tasks_of(render(verdict_pipeline))["publish"]["when"]
    hpc = tasks_of(hpc_render(verdict_pipeline))["publish"]["when"]
    assert cluster.replace(result_of("evaluate"), result_of("evaluate", "evaluate-hpc")) == hpc


# --------------------------------------------------------------------------- one name, one meaning in a step

def test_a_name_that_is_an_input_and_an_output_of_one_step_is_refused_with_the_fix():
    def build(a):
        a.step("refine", kind="transform", inputs=["clean"], produces=[a.dataset("clean")])
    assert refused(build) == (
        "step 'refine': clean is both an input (inputs=) of step 'refine' and one of its own outputs (produces=). "
        "The step reads and writes by name, so the two must differ: rename the config value (for example "
        "'previous-clean', in inputs= and in config/) or the produced name.")


def test_a_name_a_needed_step_produces_and_this_step_produces_too_is_refused():
    def build(a):
        first = a.step("first", kind="transform", produces=[a.artifact("report")])
        a.step("second", kind="transform", needs=[first], produces=[a.artifact("report")])
    assert "report is both something the step it needs, 'first', produces and one of its own outputs" in refused(build)


# ------------------------------------------------------------------------------------------ the step folder

def test_the_folder_of_a_step_has_underscores():
    assert step_folder("model-eval") == "steps/model_eval"
    assert step_folder("train") == "steps/train"


def run_app(tmp_path, pipeline_text, folders):
    repo = copy_app(tmp_path / "app")
    for old in (repo / "steps").iterdir():
        for f in old.iterdir():
            f.unlink()
        old.rmdir()
    for folder in folders:
        (repo / "steps" / folder).mkdir(parents=True)
    (repo / "pipeline.py").write_text(textwrap.dedent(pipeline_text))
    return run_render(repo)


PIPELINE = """
    from pathlib import Path
    from kubecore.authoring import pipeline, step
    with pipeline("p") as p:
        step("model-eval", kind="evaluate")
    p.write(Path(__file__).parent / "out" / "raw-workflow-template.yaml")
"""


def test_a_render_without_the_step_folder_names_the_folder(tmp_path):
    done = run_app(tmp_path, PIPELINE, [])
    assert done.returncode == 1
    assert "step 'model-eval' has no folder steps/model_eval/." in done.stderr
    assert "Create steps/model_eval/ with a Dockerfile and app/entry.py." in done.stderr


def test_a_render_with_a_hyphen_folder_says_to_rename_it(tmp_path):
    done = run_app(tmp_path, PIPELINE, ["model-eval"])
    assert done.returncode == 1
    assert "There is a folder steps/model-eval/: rename it to steps/model_eval/." in done.stderr


def test_a_render_with_the_underscore_folder_passes(tmp_path):
    assert run_app(tmp_path, PIPELINE, ["model_eval"]).returncode == 0, "folder is there"


def test_a_refusal_of_the_engine_is_one_sentence_without_a_traceback(tmp_path):
    done = run_app(tmp_path, PIPELINE.replace('kind="evaluate"', 'kind="evaluate", when="score > 3"'), ["model_eval"])
    assert done.returncode == 1
    assert "kaos-engine: step 'model-eval': when='score > 3' is not a condition the platform can read." in done.stderr
    assert "Traceback" not in done.stderr


def test_any_other_error_of_pipeline_py_keeps_its_traceback(tmp_path):
    done = run_app(tmp_path, PIPELINE.replace("step(", "undefined_function("), ["model_eval"])
    assert done.returncode != 0 and "NameError" in done.stderr and "Traceback" in done.stderr


# ----------------------------------------------------------------------- a value is compared exactly

@pytest.mark.parametrize("written, canonical", [
    ("1000000", "1000000"), ("1e6", "1000000"), ("1.0", "1"), ("1e-5", "0.00001"), ("0.00001", "0.00001"),
    ("12345678", "12345678"), ("3000000000", "3000000000"), ("0.5", "0.5"), ("-0.0", "-0"), ("1.5e22", "1.5e+22"),
    ("1e-7", "1e-7"), ("123456789.125", "123456789.125"), ("9007199254740992", "9007199254740992"),
    ("true", "true"), ("false", "false"), ("null", "null"), ("abc", "abc"), ("", ""), ("two words", "two words"),
])
def test_a_text_is_compared_with_a_non_string_value_as_the_workflow_systems_json_writes_it(written, canonical):
    assert when_text_problem(written) == ""
    assert canonical_when_text(written) == canonical


@pytest.mark.parametrize("written", ["a\nb", "a\tb"])
def test_a_control_character_is_a_problem_of_the_text(written):
    assert "control character" in when_text_problem(written)


@pytest.mark.parametrize("written, said", [
    ("1e400", "too large"), (".5", "looks like a number"), ("+1", "looks like a number"), ("01", "looks like a number"),
    ("5.", "looks like a number"), ("1_000", "looks like a number"), ("True", "write true in lowercase"),
    ("NULL", "write null in lowercase"), ("9007199254740993", "an integer above 9007199254740992"),
    ("-9007199254740993", "an integer above"),
])
def test_a_text_that_cannot_be_matched_reliably_is_refused_with_what_to_write(written, said):
    assert said in when_text_problem(written)

    def build(a):
        evaluate = a.step("evaluate", kind="evaluate")
        a.step("publish", kind="publish", needs=[evaluate], when=f"evaluate.rows == '{written}'")
    message = refused(build)
    assert "cannot be matched reliably" in message and said in message


def test_json_numbers_are_written_as_go_writes_them():
    assert [json_number_text(v) for v in (999999.0, 1e6, 12345678.0, 3e9, 0.5, 1.0, 1e-5, 1e-7, 1.5e22, 1e21, 0.0)] == [
        "999999", "1000000", "12345678", "3000000000", "0.5", "1", "0.00001", "1e-7", "1.5e+22", "1e+21", "0"]


def test_the_comparison_reads_a_string_as_text_and_anything_else_as_json_text():
    test = value_test("evaluate", "rows", "==", "1e6")
    assert test.startswith('(type(') and '== "string" ? ' in test and "toJSON(" in test
    assert "string(" not in test.replace("type(", "")
    assert "== '1e6'" in test and "== '1000000'" in test, "the text as written for a string, as JSON writes it otherwise"
    assert value_test("evaluate", "rows", "!=", "1e6") == "!" + test


def test_a_when_with_a_line_break_is_refused_at_render():
    message = refused(lambda a: a.step("publish", kind="publish", when="mode == 'a\nb'"))
    assert "holds a control character" in message
    message = refused(lambda a: a.step("publish", kind="publish", when="mode == a\nand mode == b"))
    assert "holds a control character" in message


# ------------------------------------------------ the time defaults: walltime and the checkpoint interval

@pytest.mark.parametrize("walltime", ["721h", "43201", "43201m", "99999999m"])
def test_a_walltime_the_form_would_refuse_cannot_be_written(walltime):
    message = refused(lambda a: a.step("train", kind="evaluate", walltime=walltime))
    assert message == (f"step 'train': train-walltime is {walltime!r}. Write minutes or hours, for example 90, 90m or 12h: "
                       f"at least one minute, at most 30 days, no other unit.")


@pytest.mark.parametrize("walltime", ["720h", "43200", "43200m", "1m"])
def test_a_walltime_up_to_thirty_days_is_accepted(walltime):
    render(lambda a: a.step("train", kind="evaluate", walltime=walltime))


def test_a_checkpoint_interval_the_form_would_refuse_cannot_be_written():
    message = refused(lambda a: a.step("train", kind="evaluate", checkpoint_every="721h"))
    assert message.startswith("step 'train': train-checkpoint-every is '721h'. Write minutes or hours")


def form_of(build, hpc=False):
    wft = hpc_render(build) if hpc else None
    if wft is None:
        from kubecore import authoring
        ctx = copy.deepcopy(CTX)
        ctx["hpc"]["enabled"] = False
        with authoring.pipeline("p") as p:
            build(authoring)
        wft = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)
    return {p["name"]: p for p in wft["spec"]["arguments"]["parameters"]}, wft


def test_a_resumable_step_gets_the_checkpoint_field_with_the_engine_default():
    form, wft = form_of(lambda a: a.step("train", kind="transform", resumable=True, produces=[a.artifact("r")]))
    assert form["train-checkpoint-every"]["value"] == "10m"
    train = next(t for t in wft["spec"]["templates"] if t["name"] == "train")
    env = {e["name"]: e.get("value") for c in train["initContainers"] for e in c["env"]}
    assert env["CKPT_EVERY_MIN"] == "{{inputs.parameters.checkpoint-every}}", "the cluster's copier reads the form value"
    dag = next(x for x in wft["spec"]["templates"] if x["name"] == "main")["dag"]["tasks"]
    task = next(t for t in dag if t["name"] == "train")
    assert any(p["name"] == "checkpoint-every" for p in task["arguments"]["parameters"])


def test_the_step_default_is_the_forms_default_and_wins_over_the_engine_default():
    form, _ = form_of(lambda a: a.step("train", kind="transform", resumable=True, checkpoint_every="25m",
                                       produces=[a.artifact("r")]))
    assert form["train-checkpoint-every"]["value"] == "25m"


def test_a_step_that_is_neither_resumable_nor_declares_an_interval_has_no_checkpoint_field():
    form, _ = form_of(lambda a: a.step("train", kind="transform", produces=[a.artifact("r")]))
    assert "train-checkpoint-every" not in form


def test_checkpoint_every_false_keeps_the_copies_off_even_for_a_resumable_step():
    form, wft = form_of(lambda a: a.step("train", kind="transform", resumable=True, checkpoint_every=False,
                                         produces=[a.artifact("r")]))
    assert "train-checkpoint-every" not in form
    train = next(t for t in wft["spec"]["templates"] if t["name"] == "train")
    assert "initContainers" not in train, "no copies"
