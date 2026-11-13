"""A step that runs once for each combination of values: matrix= (PRD kaos#1911, docs/step-contract.md section 12)."""

import copy
import json
from pathlib import Path

import pytest
import yaml
from test_authoring_contract import refused, render

from helpers import valid
from kubecore import compose, enhance
from kubecore.run_settings import matrix_items, parallel_count
from kubecore.step_contract import (
    MATRIX_ENTRY_CAP,
    MATRIX_RUN_BYTES,
    MATRIX_VALUES_CAP,
    RESULT_CAP,
    base_context_cap,
    fan_output_bytes,
    fan_template,
    gather_task,
    max_item_values,
    wire,
)

CTX = yaml.safe_load((Path(__file__).resolve().parents[1] / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
GRID = {"lr": [0.01, 0.001], "batch": [16, 32]}


def sweep(a, **extra):
    prepare = a.step("prepare", kind="transform", produces=[a.artifact("prepared")])
    train = a.step("train", kind="train", needs=[prepare], gpu=True, matrix=GRID, produces=[a.model("detector")], **extra)
    a.step("pick-best", kind="evaluate", needs=[train], produces=[a.artifact("winner")])


def template(wft, name):
    return next(t for t in wft["spec"]["templates"] if t["name"] == name)


def tasks_of(wft):
    return {t["name"]: t for t in template(wft, "main")["dag"]["tasks"]}


def enhanced(build=sweep, ctx=CTX):
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        build(authoring)
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), copy.deepcopy(ctx))


# ------------------------------------------------------------------------------------------------ the refusals

def test_a_matrix_needs_values_for_every_name():
    message = refused(lambda a: a.step("t", kind="train", matrix={"lr": []}, produces=[a.model("m")]))
    assert message == "step 't': matrix= gives 'lr' the value []. Give a list with at least one value, for example 'lr': [1, 2]."


def test_a_matrix_that_is_not_a_dict_says_what_to_write():
    message = refused(lambda a: a.step("t", kind="train", matrix=[1, 2], produces=[a.model("m")]))
    assert 'matrix={"lr": [0.01, 0.001], "batch": [16, 32]}' in message


def test_more_than_32_combinations_are_refused_with_the_count():
    message = refused(lambda a: a.step("t", kind="train", matrix={"a": list(range(6)), "b": list(range(6))},
                                       produces=[a.model("m")]))
    assert message == "step 't': matrix= makes 36 combinations; a step runs at most 32. Remove values, or split the step."


@pytest.mark.parametrize("kind", ["ingest", "publish", "control"])
def test_a_step_that_never_runs_on_a_grid_cannot_have_a_matrix(kind):
    def build(a):
        a.step("t", kind=kind, matrix={"x": [1]}, **valid(a, kind))
    assert f"a step of kind {kind} runs once, so it cannot have matrix=" in refused(build)


def test_a_matrix_step_cannot_produce_a_dataset():
    message = refused(lambda a: a.step("t", kind="transform", matrix={"x": [1]}, produces=[a.dataset("clean")]))
    assert "cannot produce a dataset" in message and "let the step after it write the dataset" in message


def test_a_matrix_step_cannot_need_a_matrix_step():
    def build(a):
        one = a.step("one", kind="train", matrix={"x": [1]}, produces=[a.model("m1")])
        a.step("two", kind="train", needs=[one], matrix={"y": [1]}, produces=[a.model("m2")])
    assert "cannot need another step with matrix=" in refused(build)


def test_a_matrix_step_is_never_continued_from_a_checkpoint():
    assert "is not continued from a checkpoint yet" in refused(lambda a: a.step(
        "t", kind="train", matrix={"x": [1]}, resumable=True, produces=[a.model("m")]))
    assert "is not continued from a checkpoint yet" in refused(lambda a: a.step(
        "t", kind="train", matrix={"x": [1]}, checkpoint_every="20m", produces=[a.model("m")]))
    wft = render(sweep)
    annotations = template(wft, "train")["metadata"]["annotations"]
    assert annotations["platform.kubecore.io/hpc-resumable"] == "off"


def test_a_name_must_be_a_settings_name_and_not_a_section_the_step_reads():
    assert "lowercase letters, digits and _" in refused(
        lambda a: a.step("t", kind="train", matrix={"Learning-Rate": [1]}, produces=[a.model("m")]))
    assert "also a section the step reads" in refused(
        lambda a: a.step("t", kind="train", reads=["train"], matrix={"train": [1]}, produces=[a.model("m")]))


def test_a_value_is_a_number_a_boolean_or_short_safe_text():
    assert "is a list" in refused(lambda a: a.step("t", kind="train", matrix={"x": [[1]]}, produces=[a.model("m")]))
    assert "A text value is 1 to 64" in refused(
        lambda a: a.step("t", kind="train", matrix={"x": ["{{workflow.name}}"]}, produces=[a.model("m")]))


def test_a_when_cannot_read_a_value_of_a_matrix_step_but_a_picker_may_have_a_when():
    def build(a):
        train = a.step("train", kind="train", matrix={"x": [1]}, produces=[a.model("m")])
        a.step("pick", kind="evaluate", needs=[train], when="train.x == 1")
    assert "runs once for each combination" in refused(build)
    tasks = tasks_of(render(lambda a: sweep(a, when="mode != off") if False else (
        a.step("train", kind="train", matrix={"x": [1]}, when="experiment-name != off", produces=[a.model("m")]))))
    assert tasks["train"]["when"] == "{{=workflow.parameters['experiment-name'] != 'off'}}"


# --------------------------------------------------------------------------------------------------- the render

def test_a_matrix_step_is_one_task_that_fans_out_over_one_template():
    wft = render(sweep)
    train = tasks_of(wft)["train"]
    assert train["template"] == fan_template("train")
    names = {p["name"]: p["value"] for p in train["arguments"]["parameters"]}
    assert set(names) == {"items", "par", "kaos-context", "kaos-run-settings", "kaos-images"}
    assert '["train"]["matrix"]' in names["items"] and '["train"]["parallel"]' in names["par"]
    assert 'tasks["prepare"]' in names["kaos-context"]
    fan = template(wft, fan_template("train"))
    run, rest = fan["steps"][0][0], fan["steps"][1][0]
    assert run["template"] == "train" and run["continueOn"] == {"failed": True}
    assert "[:int(" in run["withParam"] and rest["template"] == fan_template("train")
    assert run["arguments"]["parameters"][0]["value"] == (
        '{{=inputs.parameters["kaos-context"] + ",\\"matrix\\":" + '
        'fromJSON(inputs.parameters["kaos-run-settings"])["train"]["matrix"][int(item.index)].text + "}"}}')
    assert 'tasks["prepare"]' in names["kaos-context"] and names["kaos-context"].endswith('"}"}}') \
        and not names["kaos-context"].endswith('"}" + "}"}}'), "the task's context is left open for the item to close"


def test_the_fan_template_has_an_empty_list_when_no_item_gave_an_output():
    fan = template(render(sweep), fan_template("train"))
    assert [p["valueFrom"]["default"] for p in fan["outputs"]["parameters"]] == ["[]", "[]"]


def test_the_item_template_always_has_an_output_for_the_gather_task():
    train = template(render(sweep), "train")
    assert {p["name"] for p in train["outputs"]["parameters"]} == {"kaos-result", "kaos-item"}
    result = next(p for p in train["outputs"]["parameters"] if p["name"] == "kaos-result")
    assert json.loads(result["valueFrom"]["default"]) == {"version": 1, "status": "absent"}
    assert "default" not in next(p for p in template(render(sweep), "prepare")["outputs"]["parameters"])["valueFrom"]


def test_the_step_after_a_matrix_step_reads_the_gathered_entry_and_waits_for_it():
    pick = tasks_of(render(sweep))["pick-best"]
    context = next(p["value"] for p in pick["arguments"]["parameters"] if p["name"] == "kaos-context")
    assert f'tasks["{gather_task("train")}"].outputs.parameters["kaos-result"]' in context
    assert 'tasks["train"]' not in context
    assert pick["depends"] == "compose-and-validate && " + gather_task("train")


def test_the_steps_table_names_the_matrix_so_compose_can_check_the_form():
    compose_task = template(render(sweep), "compose-and-validate")
    table = json.loads(compose_task["container"]["args"][compose_task["container"]["args"].index("--steps") + 1])
    assert {e["name"]: e.get("matrix") for e in table} == {"prepare": None, "train": GRID, "pick-best": None}


def test_a_pipeline_without_a_matrix_renders_as_before():
    wft = render(lambda a: a.step("one", kind="evaluate"))
    assert [t["name"] for t in wft["spec"]["templates"]] == ["compose-and-validate", "one", "main"]


# ----------------------------------------------------------------------------------------------------- compose

def test_the_combinations_are_checked_and_numbered():
    items = matrix_items("train", '[{"lr": 0.01, "batch": 16}, {"lr": 0.001, "batch": 32}]', ["lr", "batch"])
    assert items == [{"index": 0, "of": 2, "text": '{"index":0,"of":2,"values":{"lr":0.01,"batch":16}}'},
                     {"index": 1, "of": 2, "text": '{"index":1,"of":2,"values":{"lr":0.001,"batch":32}}'}]


@pytest.mark.parametrize("raw,expected", [
    ("nope", "train-matrix: this is not valid JSON"),
    ("[]", "train-matrix: write a list with at least one combination"),
    ("[1]", "train-matrix: combination 1 is not an object"),
    ('[{"lr": 1, "rate": 2, "batch": 1}]', "combination 1 has the name 'rate', which the step does not declare"),
    ('[{"lr": 1}]', "combination 1 has no value for 'batch'"),
    ('[{"lr": 1, "batch": {"a": 1}}]', "the value of 'batch' in combination 1 is an object"),
    ('[{"lr": NaN, "batch": 1}]', "this is not valid JSON"),
    (json.dumps([{"lr": i, "batch": 1} for i in range(33)]), "holds 33 combinations; a step runs at most 32. Remove 1."),
])
def test_a_wrong_matrix_is_refused_with_a_plain_message(raw, expected):
    with pytest.raises(ValueError) as exc:
        matrix_items("train", raw, ["lr", "batch"])
    assert expected in str(exc.value)


def test_parallel_is_all_or_a_number_from_1_to_32():
    assert parallel_count("t", "all", 4) == 4 and parallel_count("t", "2", 4) == 2 and parallel_count("t", "9", 4) == 4
    for bad in ("0", "33", "two", "-1", ""):
        with pytest.raises(ValueError, match="t-parallel is"):
            parallel_count("t", bad, 4)


def test_compose_writes_the_items_and_the_parallel_count_into_the_settings():
    steps = [{"name": "train", "matrix": GRID, "needs": []}, {"name": "other", "needs": []}]
    out = compose.run_matrix(steps, ['train=[{"lr": 1, "batch": 2}]'], ["train=1"])
    assert out == {"train": {"matrix": [{"index": 0, "of": 1, "text": '{"index":0,"of":1,"values":{"lr":1,"batch":2}}'}],
                             "parallel": 1}}


def test_compose_refuses_a_field_of_a_step_that_is_not_a_matrix_step():
    with pytest.raises(compose.ComposeError, match="other-matrix: step 'other' is not a matrix step"):
        compose.run_matrix([{"name": "other", "needs": []}], ["other=[]"], [])


def test_a_needed_matrix_step_counts_its_shared_room_against_the_base_context():
    assert base_context_cap(2, 1) == base_context_cap(2) - (MATRIX_ENTRY_CAP - 8 * 1024)


# ------------------------------------------------------------------------------------------------ the enhancer

def test_the_run_form_has_a_matrix_and_a_parallel_field_with_a_default_and_a_description():
    params = {p["name"]: p for p in enhanced()["spec"]["arguments"]["parameters"]}
    assert json.loads(params["train-matrix"]["value"]) == [
        {"lr": 0.01, "batch": 16}, {"lr": 0.01, "batch": 32}, {"lr": 0.001, "batch": 16}, {"lr": 0.001, "batch": 32}]
    assert params["train-parallel"]["value"] == "4" and "Default 4" in params["train-parallel"]["description"]
    assert "JSON list" in params["train-matrix"]["description"] and "lr, batch" in params["train-matrix"]["description"]
    assert "At most 32" in params["train-matrix"]["description"]
    assert "other-matrix" not in params and "prepare-matrix" not in params


def test_compose_gets_the_two_fields_and_the_form_shows_them_after_the_steps_class():
    wft = enhanced()
    args = template(wft, "compose-and-validate")["container"]["args"]
    assert "--matrix" in args and "train={{workflow.parameters.train-matrix}}" in args
    assert "--parallel" in args and "train={{workflow.parameters.train-parallel}}" in args
    names = [p["name"] for p in wft["spec"]["arguments"]["parameters"]]
    order = [names.index(f"train-{suffix}") for suffix in ("class", "walltime", "matrix", "parallel")]
    assert order == sorted(order)


def test_an_item_gets_the_same_arguments_as_the_step_does_on_its_own():
    wft = enhanced()
    item = template(wft, fan_template("train"))["steps"][0][0]
    names = {p["name"] for p in item["arguments"]["parameters"]}
    assert {"kaos-context", "kaos-item", "deadline", "gpus", "cpu", "mem", "disk"} <= names
    deadline = next(p["value"] for p in item["arguments"]["parameters"] if p["name"] == "deadline")
    assert 'inputs.parameters["kaos-run-settings"]' in deadline and "tasks[" not in deadline


def test_the_gather_task_joins_the_items_and_the_run_fails_with_it():
    wft = enhanced()
    tasks = tasks_of(wft)
    gather = tasks[gather_task("train")]
    assert gather["depends"] == "compose-and-validate && (train.Succeeded || train.Skipped || train.Omitted)"
    tpl = template(wft, gather_task("train"))
    assert tpl["container"]["command"] == ["python", "-I", "-m", "kaos_engine.gather"]
    env = {e["name"]: e["value"] for e in tpl["container"]["env"] if "value" in e}
    assert env["KAOS_GATHER_STEP"] == "train" and "{{inputs.parameters" in env["KAOS_GATHER_RESULTS"]
    assert tpl["outputs"]["parameters"][0]["name"] == "kaos-result"


def test_a_pipeline_that_promotes_waits_for_the_gather_task_of_a_matrix_step():
    def build(a):
        ingest = a.step("ingest", kind="ingest", produces=[a.dataset("clean")])
        a.step("train", kind="train", needs=[ingest], matrix=GRID, produces=[a.model("detector")])
    promote = tasks_of(enhanced(build))["kaos-promote"]
    assert gather_task("train") + ".Succeeded" in promote["depends"]


# ------------------------------------------------------------------------------------------- the HPC variant

def hpc_ctx():
    ctx = copy.deepcopy(CTX)
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    return ctx


def test_an_item_has_an_hpc_variant_in_its_round_and_the_template_joins_both():
    wft = enhanced(ctx=hpc_ctx())
    fan = template(wft, fan_template("train"))
    run, twin = fan["steps"][0]
    assert twin["name"] == "run-hpc" and twin["template"] == "hpc-follower" and twin["withParam"] == run["withParam"]
    assert run["when"].startswith("{{=!hasPrefix(workflow.parameters['train-class']")
    assert twin["when"].startswith("{{=hasPrefix(workflow.parameters['train-class']")
    args = {p["name"]: p["value"] for p in twin["arguments"]["parameters"]}
    assert args["node-suffix"] == "-{{=item.index}}" and args["kaos-item"] == "{{item.index}}"
    assert args["kaos-context"] == next(p["value"] for p in run["arguments"]["parameters"] if p["name"] == "kaos-context")
    assert "tasks[" not in args["time-limit"] and 'inputs.parameters["kaos-run-settings"]' in args["time-limit"]
    for out in fan["outputs"]["parameters"]:
        expression = out["valueFrom"]["expression"]
        assert "steps[\"run\"]" in expression and "steps[\"run-hpc\"]" in expression and "steps[\"rest\"]" in expression


def test_each_item_of_a_matrix_step_is_its_own_job_named_after_its_index():
    follower = template(enhanced(ctx=hpc_ctx()), "hpc-follower")
    env = {e["name"]: e.get("value") for e in follower["container"]["env"]}
    assert env["NODE_KEY"] == "{{inputs.parameters.step-name}}-hpc{{inputs.parameters.node-suffix}}"
    inputs = {p["name"]: p.get("value") for p in follower["inputs"]["parameters"]}
    assert inputs["node-suffix"] == "" and inputs["kaos-item"] == ""
    assert "kaos-item" in {p["name"] for p in follower["outputs"]["parameters"]}


def test_the_step_after_a_matrix_step_is_not_given_a_pair_of_the_matrix_step():
    tasks = tasks_of(enhanced(ctx=hpc_ctx()))
    assert "train-hpc" not in tasks and "kaos-matrix-train-hpc" not in tasks
    assert tasks["pick-best"]["depends"].count(gather_task("train")) == 1


# ----------------------------------------------------------------- failed items: the default and matrix_allow_failed

def test_a_step_with_matrix_is_like_any_step_unless_it_says_failed_items_are_acceptable():
    def build(a):
        train = a.step("train", kind="train", matrix=GRID, matrix_allow_failed=True, produces=[a.model("m")])
        a.step("pick", kind="evaluate", needs=[train])
    compose_task = template(render(build), "compose-and-validate")["container"]["args"]
    table = json.loads(compose_task[compose_task.index("--steps") + 1])
    assert table[0]["allow_failed"] is True and "allow_failed" not in table[1]
    env = {e["name"]: e["value"] for e in template(enhanced(build), gather_task("train"))["container"]["env"] if "value" in e}
    assert env["KAOS_GATHER_ALLOW_FAILED"] == "true"
    env = {e["name"]: e["value"] for e in template(enhanced(), gather_task("train"))["container"]["env"] if "value" in e}
    assert env["KAOS_GATHER_ALLOW_FAILED"] == "false"


def test_matrix_allow_failed_needs_matrix_and_a_boolean():
    message = refused(lambda a: a.step("t", kind="train", matrix_allow_failed=True, produces=[a.model("m")]))
    assert message == ("step 't': matrix_allow_failed=True is for a step with matrix=: it lets the step after it run when some "
                       "items failed. Add matrix=, or remove matrix_allow_failed.")
    assert "must be True or False" in refused(lambda a: a.step(
        "t", kind="train", matrix={"x": [1]}, matrix_allow_failed="yes", produces=[a.model("m")]))


def test_a_promoting_run_waits_for_the_gather_task_so_a_failed_item_stops_the_promotion_by_default():
    def build(a):
        ingest = a.step("ingest", kind="ingest", produces=[a.dataset("clean")])
        a.step("train", kind="train", needs=[ingest], matrix=GRID, produces=[a.model("detector")])
    depends = tasks_of(enhanced(build))["kaos-promote"]["depends"]
    assert gather_task("train") + ".Succeeded" in depends


# ----------------------------------------------------------------------------------------------- sizes and numbers

def test_a_whole_number_beyond_2_to_the_53_is_refused_everywhere_with_the_fix():
    big = 2 ** 53 + 1
    expected = f'is the number {big}, which is beyond 9007199254740992 (2 to the power 53)'
    assert expected in refused(lambda a: a.step("t", kind="train", matrix={"seed": [big]}, produces=[a.model("m")]))
    with pytest.raises(ValueError, match="Write it as text"):
        matrix_items("t", json.dumps([{"seed": big}]), ["seed"])
    assert matrix_items("t", json.dumps([{"seed": 2 ** 53}]), ["seed"])


def test_a_number_arrives_with_the_text_it_was_written_with():
    items = matrix_items("t", '[{"lr": 1.0, "n": 12, "e": 1e-05, "s": "a b"}]', ["lr", "n", "e", "s"])
    assert items[0]["text"] == '{"index":0,"of":1,"values":{"lr":1.0,"n":12,"e":1e-05,"s":"a b"}}'
    assert json.loads(items[0]["text"])["values"]["lr"] == 1.0


def test_an_items_values_share_16_kib_and_never_exceed_a_result():
    assert max_item_values(1) == RESULT_CAP and max_item_values(2) == 8192 and max_item_values(32) == 512
    assert all(max_item_values(n) * n <= MATRIX_VALUES_CAP for n in range(1, 33))


def test_a_matrix_step_may_read_and_write_at_most_four_things_in_all():
    def build(a):
        a.step("t", kind="train", inputs=["a", "b", "c"], matrix={"x": [1]}, produces=[a.model("m"), a.artifact("r")])
    assert "at most 4 things in all" in refused(build)


def test_a_step_that_needs_a_matrix_step_may_need_four_steps_in_all():
    def build(a):
        train = a.step("train", kind="train", matrix={"x": [1]}, produces=[a.model("m")])
        others = [a.step(f"o{i}", kind="evaluate") for i in range(4)]
        a.step("pick", kind="evaluate", needs=[train] + others)
    assert "may need at most 4 steps in all" in refused(build)


def test_32_items_that_read_two_inputs_and_write_a_model_fit_what_the_run_keeps():
    """The arithmetic: what one item hands back is its values (512 at 32 items), its ledger (two reads, one write,
    sized with the longest names the render allows for a typical dataset) and its entry; 32 of them are the entry a
    step after the matrix step finds in its context, and each round of the fan template holds the items below it."""
    read = {"name": "n" * 20, "repo": "dataset-" + "r" * 40, "owner": None, "ref": "v" * 12, "commit": "c" * 64}
    write = {"name": "detector", "type": "model", "repo": "outputs-" + "a" * 30, "commit": "d" * 64}
    values = {"v": "x" * (max_item_values(32) - 20)}
    result = {"version": 1, "status": "ok", "values": values, "reads": [read, read], "writes": [write]}
    assert len(wire(values)) <= max_item_values(32)
    entry = {"index": 31, "values": {"lr": 0.001, "batch": 32}, "status": "ok", "result": result}
    per_item = len(wire(entry))
    assert 32 * per_item <= MATRIX_ENTRY_CAP, (per_item, 32 * per_item)
    # the workflow system keeps each result once per round above it (the item's index too: 4 bytes)
    one_at_a_time = fan_output_bytes(32, 1, len(wire(result)) + 4)
    four_at_a_time = fan_output_bytes(32, 4, len(wire(result)) + 4)
    assert four_at_a_time == (8 + 7 + 6 + 5 + 4 + 3 + 2 + 1) * 4 * (len(wire(result)) + 4)
    assert fan_output_bytes(32, 1, 1) == sum(range(1, 33)) and fan_output_bytes(32, 32, 1) == 32
    assert four_at_a_time < 400 * 1024 and one_at_a_time < MATRIX_RUN_BYTES, (four_at_a_time, one_at_a_time)


def test_an_item_and_its_hpc_variant_are_given_the_image_the_run_resolved_from_inside_the_fan_template():
    wft = enhanced(ctx=hpc_ctx())
    run, twin = template(wft, fan_template("train"))["steps"][0]
    for call in (run, twin):
        image = next(p["value"] for p in call["arguments"]["parameters"] if p["name"] == "image")
        assert 'inputs.parameters["kaos-images"]' in image and "tasks[" not in image and '["train"]' in image
    fan_task = {p["name"]: p["value"] for p in tasks_of(wft)["train"]["arguments"]["parameters"]}
    assert 'tasks["compose-and-validate"].outputs.parameters["kaos-images"]' in fan_task["kaos-images"]
