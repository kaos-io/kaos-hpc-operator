"""Starting a run from a step with the results of an earlier run (PRD kaos#1911, docs/step-contract.md section 13):
which steps run (the condition every task has), what compose takes from the earlier run, and every refusal, in compose,
before any step starts."""

import json
import re
import types

import pytest
import yaml
from test_authoring_contract import render

from helpers import gated, own_condition
from kubecore import compose, rerun
from kubecore.step_contract import RECORD_DIR, RESULT_CAP, cost, result_of, wire

RUN, NEW, APP = "hera-demo-aaaaa", "hera-demo-bbbbb", "hera-demo"
UID, NEW_UID = "11111111-aaaa-4aaa-8aaa-111111111111", "22222222-bbbb-4bbb-8bbb-222222222222"
SENTINEL_TEXT = '{"version":1,"status":"absent"}'


def tasks_of(wft):
    return {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}


# ------------------------------------------------------------------------- which tasks run: the condition of every task

def run_when(when, **form):
    """What the workflow engine's `when` of a task says for these run form values (the engine's own operators, in Python)."""
    body = re.fullmatch(r"\{\{=(.*)\}\}", when, re.S).group(1)
    body = re.sub(r"workflow\.parameters\['([^']+)'\]", lambda m: repr(form[m.group(1)]), body)
    return eval(body.replace("&&", " and ").replace("||", " or "), {"__builtins__": {}})  # noqa: S307 - the engine's own text


def diamond(a, when=None):
    load = a.step("load", kind="ingest", produces=[a.dataset("loaded")])
    left = a.step("left", kind="transform", needs=[load], produces=[a.artifact("l")])
    right = a.step("right", kind="transform", needs=[load], produces=[a.artifact("r")])
    a.step("join", kind="evaluate", needs=[left, right], when=when)


def test_a_step_runs_when_nothing_is_asked_when_it_is_the_start_or_when_it_is_after_the_start():
    tasks = tasks_of(render(diamond))
    expected = {   # start -> the steps that run again
        "": {"load", "left", "right", "join"}, "load": {"load", "left", "right", "join"},
        "left": {"left", "join"}, "right": {"right", "join"}, "join": {"join"}}
    for start, runs in expected.items():
        got = {name for name, task in tasks.items() if name != "compose-and-validate"
               and run_when(task["when"], **{"start-from-step": start})}
        assert got == runs, start


def test_the_gate_names_every_step_the_step_needs_directly_or_not():
    tasks = tasks_of(render(diamond))
    assert tasks["join"]["when"] == gated("join", "", "left", "load", "right")
    assert tasks["left"]["when"] == gated("left", "", "load")
    assert tasks["load"]["when"] == gated("load")


def test_the_gate_and_compose_agree_on_which_steps_are_not_run_again_for_every_start():
    """Two places decide it: the condition on each task (render) and compose's list (kubecore/rerun.py)."""
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        diamond(authoring)
    wft = yaml.safe_load(p.wt.to_yaml())
    table = json.loads(next(t for t in wft["spec"]["templates"] if t["name"] == "compose-and-validate")["container"]["args"][
        next(i for i, a in enumerate(wft["spec"]["templates"][0]["container"]["args"]) if a == "--steps") + 1])
    tasks = tasks_of(wft)
    for start in [s["name"] for s in table]:
        by_gate = {name for name in tasks if name != "compose-and-validate"
                   and not run_when(tasks[name]["when"], **{"start-from-step": start})}
        assert by_gate == set(rerun.reused_steps(table, start)), start


def test_a_step_keeps_its_own_when_after_the_gate_and_a_step_that_is_not_run_again_never_evaluates_it():
    def build(a):
        diamond(a, when="experiment-name != off")
    task = tasks_of(render(build))["join"]
    assert own_condition(task["when"], "join", "left", "load", "right") == "{{=workflow.parameters['experiment-name'] != 'off'}}"
    for start, form, runs in (("", "on", True), ("", "off", False), ("left", "on", True), ("left", "off", False)):
        assert run_when(task["when"], **{"start-from-step": start, "experiment-name": form}) is runs
    first = task["when"].index("&&")
    assert task["when"].index("start-from-step") < first, "the gate is first: `&&` stops there for a step not run again"


def test_a_sweep_that_is_not_run_ends_skipped_with_its_gather_task_giving_the_sentinel():
    def build(a):
        load = a.step("load", kind="ingest", produces=[a.dataset("loaded")])
        a.step("tune", kind="train", needs=[load], matrix={"lr": [1, 2]}, produces=[a.model("tuned")])
        fit = a.step("fit", kind="train", needs=[load], produces=[a.model("fitted")])
        a.step("score", kind="evaluate", needs=[fit])
    tasks = tasks_of(hpc_render(build))
    assert not run_when(tasks["tune"]["when"], **{"start-from-step": "score"}), "the sweep task is skipped"
    assert tasks["kaos-gather-tune"]["depends"] == "compose-and-validate && (tune.Succeeded || tune.Skipped)"
    assert "when" not in tasks["kaos-gather-tune"], "so the gather task runs, and hands on the sentinel"
    assert "tune" not in tasks["score"]["depends"], "the step that runs does not wait for the sweep"
    from kubecore import gather
    assert gather.gather("tune", "Skipped", [], [], []) == {"version": 1, "status": "absent"}


def test_a_step_that_depends_on_a_step_that_is_not_run_again_still_waits_for_it_the_usual_way():
    tasks = tasks_of(render(diamond))
    assert tasks["join"]["depends"] == "compose-and-validate && left && right", \
        "a bare name accepts a skipped step (checked on the workflow engine); no tolerant form that lets a failure through"


def hpc_render(build):
    import copy

    from kubecore import authoring, enhance
    ctx = copy.deepcopy(yaml.safe_load(open(enhance.__file__.replace("enhance.py", "local-dev/pipeline-context.yaml"))))
    ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    with authoring.pipeline("p") as p:
        build(authoring)
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)


def test_the_hpc_variant_of_a_step_has_the_same_gate():
    def build(a):
        load = a.step("load", kind="ingest", produces=[a.dataset("loaded")])
        a.step("train", kind="train", needs=[load], gpu=True, when="experiment-name != off", produces=[a.model("m")])
    tasks = tasks_of(hpc_render(build))
    gate = "workflow.parameters['start-from-step'] in ['', 'train', 'load']"
    for name in ("train", "train-hpc"):
        assert gate + " && (workflow.parameters['experiment-name'] != 'off')" in tasks[name]["when"], name
    assert tasks["train"]["when"].endswith("!hasPrefix(workflow.parameters['train-class'], 'meluxina-')}}")
    assert tasks["train-hpc"]["when"].endswith("hasPrefix(workflow.parameters['train-class'], 'meluxina-')}}")


# ------------------------------------------------------------------------------- the result a step that is not run again has

def test_compose_declares_the_result_it_took_for_each_step_as_one_output_and_the_form_has_the_two_fields():
    wft = render(diamond)
    compose_tpl = next(t for t in wft["spec"]["templates"] if t["name"] == "compose-and-validate")
    outputs = {o["name"]: o["valueFrom"]["path"] for o in compose_tpl["outputs"]["parameters"]}
    for step in ("load", "left", "right", "join"):
        assert outputs[f"kaos-reused-{step}"] == f"/work/reused/{step}.json"
    assert "--reused-dir" in compose_tpl["container"]["args"]


def test_the_form_fields_default_to_a_normal_run_and_say_what_they_do():
    import copy

    from kubecore import authoring, enhance
    ctx = copy.deepcopy(yaml.safe_load(open(enhance.__file__.replace("enhance.py", "local-dev/pipeline-context.yaml"))))
    with authoring.pipeline("p") as p:
        diamond(authoring)
    wft = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)
    form = {x["name"]: x for x in wft["spec"]["arguments"]["parameters"]}
    assert form["reuse-from-run"]["value"] == "" and form["start-from-step"]["value"] == ""
    assert form["reuse-from-run"]["description"] == (
        "Optional. The name of an earlier run of this app. Steps before the step below are not run again; their results "
        "and outputs are taken from that run. Leave empty for a normal run.")
    assert form["start-from-step"]["description"] == (
        "Optional. The step to run first; it and every step after it run again. Needs the earlier run above. Steps: "
        "load, left, right, join.")
    args = next(t for t in wft["spec"]["templates"] if t["name"] == "compose-and-validate")["container"]["args"]
    assert "--reuse-from-run={{workflow.parameters.reuse-from-run}}" in args
    assert "--start-from-step={{workflow.parameters.start-from-step}}" in args


def evaluate(expression, compose_outputs, tasks=()):
    """The value of a `{{=...}}` expression of the engine, the way the workflow engine reads task outputs: an output of a task
    that was skipped is the empty string."""
    body = re.fullmatch(r"\{\{=(.*)\}\}", expression, re.S).group(1) if expression.startswith("{{=") else expression
    task = lambda params: types.SimpleNamespace(outputs=types.SimpleNamespace(parameters=params))  # noqa: E731
    sprig = types.SimpleNamespace(default=lambda default, value: value or default)
    named = {"compose-and-validate": task(compose_outputs), **{n: task(p) for n, p in dict(tasks).items()}}
    return eval(body, {"__builtins__": {}}, {"sprig": sprig, "tasks": named})  # noqa: S307


def test_the_result_of_a_step_is_golden_text_with_the_reused_result_between_the_sentinel_and_the_step():
    assert result_of("train") == (
        'sprig.default(sprig.default("{\\"version\\":1,\\"status\\":\\"absent\\"}", '
        'tasks["compose-and-validate"].outputs.parameters["kaos-reused-train"]), '
        'tasks["train"].outputs.parameters["kaos-result"])')


def test_the_twin_form_of_the_result_is_golden_text_too():
    assert result_of("train", "train-hpc") == (
        'sprig.default(sprig.default(sprig.default("{\\"version\\":1,\\"status\\":\\"absent\\"}", '
        'tasks["compose-and-validate"].outputs.parameters["kaos-reused-train"]), '
        'tasks["train-hpc"].outputs.parameters["kaos-result"]), tasks["train"].outputs.parameters["kaos-result"])')


@pytest.mark.parametrize("twin", ["", "train-hpc"])
def test_a_step_that_ran_gives_its_own_result_a_step_not_run_again_the_reused_one_and_none_at_all_the_sentinel(twin):
    expression = result_of("train", twin)
    ran, reused = '{"version":1,"status":"ok","values":{"a":1}}', '{"version":1,"status":"ok","values":{"a":2}}'
    tasks = {"train": {"kaos-result": ran}, "train-hpc": {"kaos-result": ""}} if twin else {"train": {"kaos-result": ran}}
    assert evaluate(expression, {"kaos-reused-train": ""}, tasks) == ran
    skipped = {"train": {"kaos-result": ""}, "train-hpc": {"kaos-result": ""}} if twin else {"train": {"kaos-result": ""}}
    assert evaluate(expression, {"kaos-reused-train": reused}, skipped) == reused
    assert evaluate(expression, {"kaos-reused-train": ""}, skipped) == SENTINEL_TEXT


def test_everything_that_reads_a_result_reads_the_fallback_too():
    """The context of a step, a when= on a value, and the final task's plan all go through result_of."""
    def build(a):
        score = a.step("score", kind="evaluate")
        a.step("publish", kind="publish", needs=[score], when="score.passed == true")
    tasks = tasks_of(render(build))
    context = next(p["value"] for p in tasks["publish"]["arguments"]["parameters"] if p["name"] == "kaos-context")
    assert result_of("score") in context
    assert result_of("score") in tasks["publish"]["when"]


def test_the_rendered_pipeline_still_fits_what_a_step_and_the_workflow_may_cost():
    """A step that needs ten steps has ten results in its context expression, each longer now; the enhancer refuses a step whose
    definition is over its cap, and the whole workflow stays far under what the cluster stores."""
    from kubecore.step_contract import TEMPLATE_CAP, template_bytes

    def build(a):
        feeders = [a.step(f"feed{i}", kind="transform", produces=[a.artifact(f"r{i}")]) for i in range(10)]
        a.step("join", kind="evaluate", needs=feeders, when="feed0.ok == true")
    wft = hpc_render(build)
    context = next(p["value"] for p in tasks_of(wft)["join"]["arguments"]["parameters"] if p["name"] == "kaos-context")
    assert all(result_of(f"feed{i}", f"feed{i}-hpc") in context for i in range(10))
    assert len(context) < 8 * 1024
    assert len(yaml.safe_dump(wft)) < 512 * 1024
    steps = [t for t in wft["spec"]["templates"]
             if "kaos-context" in [p["name"] for p in (t.get("inputs") or {}).get("parameters", [])]]
    assert steps and max(template_bytes(t) for t in steps) < TEMPLATE_CAP
    dag = next(t for t in wft["spec"]["templates"] if t["name"] == "main")
    assert template_bytes(dag) < 64 * 1024, template_bytes(dag)


# ----------------------------------------------------------------------------------------- compose: the earlier run

class NotFound(Exception):
    status = 404


class Lake:
    """The calls compose makes of the data store to see that an earlier run's outputs are still there."""

    def __init__(self, commits=(), objects=()):
        self.commits, self.objects = set(commits), list(objects)

    def commit_id(self, repo, commit):
        if (repo, commit) not in self.commits:
            raise NotFound(f"no commit {commit}")
        return commit

    def metadata(self, repo):    # the data store says the app may write the dataset (compose waits for that)
        return {"kaos.grant": f"{APP}@2026-11-13T00:00:00Z"}

    def list_objects(self, repo, ref, prefix=""):
        return iter([{"path": p} for r, c, p in self.objects if (r, c) == (repo, ref) and p.startswith(prefix)])


class Records:
    """kaos_lib.record as compose uses it: `read` of a run's file, `write` of the plan."""

    def __init__(self, docs=None):
        self.docs, self.written = dict(docs or {}), []

    def read(self, client, app, run, name):
        return self.docs.get((run, name))

    def write(self, client, app, run, name, text):
        self.written.append((app, run, name, json.loads(text)))
        self.docs[(run, name)] = json.loads(text)


C = {"clean": "c1" * 32, "model": "d2" * 32}
STEPS = [
    {"name": "prepare", "kind": "ingest", "reads": ["prepare"], "inputs": ["raw"], "needs": [],
     "produces": [{"name": "clean", "type": "dataset"}]},
    {"name": "train", "kind": "train", "reads": ["train"], "inputs": [], "needs": ["prepare"],
     "produces": [{"name": "model", "type": "model"}]},
    {"name": "evaluate", "kind": "evaluate", "reads": ["evaluate"], "inputs": [], "needs": ["train"], "produces": [],
     "when_fields": ["mode"], "when_text": "mode == full"},
    {"name": "publish", "kind": "publish", "reads": [], "inputs": [], "needs": ["evaluate"], "produces": []},
]
PARAMS = {"prepare": {"prepare": {"rows": 10}}, "train": {"train": {"lr": 0.1, "epochs": 3}}, "evaluate": {"evaluate": {"k": 5}},
          "publish": {}}
WHEN = {"mode": "full"}
CLASSES = {"prepare": "cpu-standard", "train": "cpu-standard", "evaluate": "cpu-standard", "publish": "cpu-standard"}
READ_RAW = {"name": "raw", "repo": "dataset-raw", "owner": None, "ref": "v3", "commit": "a1" * 32}
RESULTS = {
    "prepare": {"version": 1, "status": "ok", "values": {"rows": 10}, "reads": [READ_RAW],
                "writes": [{"name": "clean", "type": "dataset", "repo": "dataset-hera-demo-clean", "commit": C["clean"]}]},
    "train": {"version": 1, "status": "ok", "values": {"acc": 0.9}, "reads": [],
              "writes": [{"name": "model", "type": "model", "repo": "outputs-hera-demo", "commit": C["model"]}]},
    "evaluate": {"version": 1, "status": "ok", "values": {"passed": True}, "reads": [], "writes": []},
}


IMAGES = {step["name"]: f"registry.example/hera-demo/{step['name']}@sha256:{'ab' * 32}" for step in STEPS}


def record_of(name, result, instance=UID, run=RUN):
    """What a step keeps: whose it is, around the exact result."""
    return {"version": 1, "app": APP, "run": run, "instance": instance, "step": name, "result": result}


def earlier_run(steps=STEPS, results=RESULTS, params=PARAMS, images=None, **changes):
    """The records of run RUN: its plan (as compose writes it) and the record of each step in `results`."""
    plan = rerun.plan_of(RUN, UID, APP, "vision", "exp", steps, params, WHEN, CLASSES, images or IMAGES,
                         {"raw": READ_RAW})
    plan.update(changes)
    return {(RUN, rerun.PLAN): plan, **{(RUN, name): record_of(name, result) for name, result in results.items()}}


def lake(**more):
    return Lake(commits=[("dataset-hera-demo-clean", C["clean"])],
                objects=[("outputs-hera-demo", C["model"], "model/exp/hera-demo-aaaaa/best.pt")], **more)


def prepare(start="evaluate", docs=None, steps=STEPS, params=None, when=None, classes=None, hpc=(), promotes=None,
            client=None, run=RUN, images=None):
    return rerun.prepare(steps, run, start, APP, params or PARAMS, when or WHEN, classes or CLASSES, set(hpc), promotes or {},
                         images or IMAGES, Records(earlier_run() if docs is None else docs), client or lake())


def refused(**kwargs):
    with pytest.raises(rerun.Refused) as exc:
        prepare(**kwargs)
    return str(exc.value)


def test_the_steps_before_the_start_are_taken_from_the_earlier_run_and_the_rest_run_again():
    got = prepare("evaluate")
    assert got.reused == ["prepare", "train"] and got.run == RUN and got.start == "evaluate"
    assert got.notes[:2] == [f"step prepare: reused from run {RUN}", f"step train: reused from run {RUN}"]
    assert got.pins == {"raw": {"read": READ_RAW, "steps": ["prepare"]}}


def test_a_model_or_an_artifact_of_a_reused_step_lives_under_the_earlier_runs_folder_and_a_dataset_needs_no_path():
    got = prepare("evaluate")
    assert got.entries["train"]["writes"] == [{"name": "model", "type": "model", "repo": "outputs-hera-demo",
                                               "commit": C["model"], "path": f"model/exp/{RUN}/"}]
    assert got.entries["prepare"]["writes"] == RESULTS["prepare"]["writes"]
    assert list(got.entries["train"]) == ["version", "status", "values", "reads", "writes"], "the result, field for field"
    assert cost(wire(got.entries["train"])) <= RESULT_CAP


def test_a_step_on_a_branch_that_does_not_meet_the_start_is_not_run_again_either():
    steps = STEPS + [{"name": "report", "kind": "transform", "reads": [], "inputs": [], "needs": [],
                      "produces": [{"name": "r", "type": "artifact"}]}]
    more = {**PARAMS, "report": {}}
    docs = earlier_run(steps, {**RESULTS, "report": {"version": 1, "status": "ok", "values": {}, "reads": [], "writes": [
        {"name": "r", "type": "artifact", "repo": "outputs-hera-demo", "commit": "e3" * 32}]}}, more,
                      images={**IMAGES, "report": IMAGES["prepare"]})
    got = prepare("train", docs=docs, steps=steps, params=more, images={**IMAGES, "report": IMAGES["prepare"]},
                  client=Lake(commits=[("dataset-hera-demo-clean", C["clean"])]))
    assert got.reused == ["prepare", "report"] and "path" in got.entries["report"]["writes"][0]


def test_a_skipped_step_is_reused_as_skipped():
    skipped = {"version": 1, "status": "skipped", "reason": "not good enough"}
    got = prepare("publish", docs=earlier_run(results={**RESULTS, "evaluate": skipped}))
    assert got.entries["evaluate"] == skipped


# the refusals, each one sentence that starts where the run form is wrong

def test_both_fields_or_neither():
    with pytest.raises(rerun.Refused) as exc:
        rerun.check_fields(STEPS, "", "train")
    assert str(exc.value) == "reuse-from-run and start-from-step go together: reuse-from-run is empty."
    with pytest.raises(rerun.Refused) as exc:
        rerun.check_fields(STEPS, RUN, "")
    assert str(exc.value) == "reuse-from-run and start-from-step go together: start-from-step is empty."
    rerun.check_fields(STEPS, RUN, "train")


def test_the_start_is_a_step_of_the_pipeline():
    with pytest.raises(rerun.Refused) as exc:
        rerun.check_fields(STEPS, RUN, "trian")
    assert str(exc.value) == ("start-from-step: this pipeline has no step 'trian'. Its steps are: prepare, train, evaluate, "
                              "publish.")


def test_a_run_that_saved_no_record_is_refused_without_saying_it_is_too_old():
    expected = ("reuse-from-run: run 'hera-demo-zzzzz' saved no record for this app (its step images may be older than this "
                "feature, or saving failed: see that run's log). Check the name: it must be a run of this app.")
    assert refused(run="hera-demo-zzzzz") == expected
    assert refused(run="../hera-demo-aaaaa").startswith("reuse-from-run: run '../hera-demo-aaaaa' saved no record")
    assert "older than what the platform keeps" not in expected


def test_a_run_that_reused_another_cannot_be_reused():
    message = refused(docs=earlier_run(reused_from={"run": "hera-demo-0zero", "start": "train"}))
    assert message == ("reuse-from-run: run 'hera-demo-aaaaa' took the results of some of its steps from run "
                       "'hera-demo-0zero', so it has no record of them. Reuse run 'hera-demo-0zero' instead, or choose a run "
                       "that ran every step it has.")


@pytest.mark.parametrize("results, why", [
    ({"prepare": RESULTS["prepare"]}, "train"),                                      # train never wrote one: it failed
    ({"train": RESULTS["train"]}, "prepare"),                                         # prepare was not run
    ({**RESULTS, "train": {"version": 1, "status": "absent"}}, "train"),
])
def test_a_step_with_no_result_in_the_earlier_run_is_refused_by_name(results, why):
    message = refused(docs=earlier_run(results=results))
    assert message == (f"run '{RUN}' has no result for step '{why}' (it failed or did not finish). A step before "
                       f"'evaluate' is not run again without one. Start from step '{why}', or choose a run that finished it.")


def test_a_changed_setting_of_a_step_that_is_not_run_again_is_refused_naming_the_keys():
    changed = {**PARAMS, "train": {"train": {"lr": 0.01, "epochs": 3, "warmup": 2}}}
    assert refused(params=changed) == (
        f"step 'train' reads different settings than in run '{RUN}': train.lr: 0.1 -> 0.01, train.warmup: (not set) -> 2. "
        "A step before 'evaluate' is not run again, so its settings cannot change. Set them back, or start from step 'train'.")


def test_a_setting_of_a_step_that_runs_again_may_change():
    got = prepare("evaluate", params={**PARAMS, "evaluate": {"evaluate": {"k": 50}}, "publish": {"x": 1}})
    assert got.reused == ["prepare", "train"]


def test_a_changed_value_of_a_form_field_the_when_of_a_step_not_run_again_names_is_a_changed_setting():
    assert refused(start="publish", when={"mode": "quick"}) == (
        f"step 'evaluate' reads different settings than in run '{RUN}': mode: full -> quick. A step before 'publish' is not "
        "run again, so its settings cannot change. Set them back, or start from step 'evaluate'.")


def test_many_changed_settings_are_listed_a_few_at_a_time():
    changed = {**PARAMS, "train": {"train": {f"k{i}": i for i in range(9)}}}
    message = refused(params=changed)
    assert message.count("(not set) ->") == 5 and message.count(" and 6 more. ") == 1


@pytest.mark.parametrize("change, was_and_is", [
    ({"kind": "evaluate"}, "its kind was train and is evaluate"),
    ({"inputs": ["raw"]}, 'its inputs was [] and is ["raw"]'),
])
def test_a_step_that_is_not_the_step_the_earlier_run_ran_is_refused(change, was_and_is):
    steps = [dict(s, **change) if s["name"] == "train" else s for s in STEPS]
    assert refused(steps=steps) == (f"step 'train' is not the step run '{RUN}' ran: {was_and_is}. A step before 'evaluate' is "
                                    "not run again. Start from step 'train'.")


def test_a_step_the_earlier_run_did_not_have_is_refused():
    old = [s for s in STEPS if s["name"] != "train"]
    assert refused(docs=earlier_run(old)) == (
        f"step 'train' is not in run '{RUN}': the pipeline has changed since. A step before 'evaluate' is not run again. "
        "Start from step 'train'.")


def test_a_step_that_produces_something_else_than_before_is_refused():
    steps = [dict(s, produces=[{"name": "model", "type": "artifact"}]) if s["name"] == "train" else s for s in STEPS]
    assert "its produces was [{" in refused(steps=steps)


def with_sweep(consumer=None, **extra):
    """STEPS with a step `tune` that has a matrix and needs `prepare`; `consumer` (a step name) is made to need it too."""
    sweep = {"name": "tune", "kind": "train", "reads": [], "inputs": [], "needs": ["prepare"], "matrix": {"lr": [1, 2]},
             "produces": [{"name": "tuned", "type": "artifact"}]}
    steps = [dict(s, needs=s["needs"] + ["tune"], **extra) if s["name"] == consumer else s for s in STEPS]
    return steps[:1] + [sweep] + steps[1:]


def test_a_step_with_a_matrix_that_a_step_that_runs_needs_is_refused():
    steps = [dict(s, matrix={"lr": [1, 2]}) if s["name"] == "train" else s for s in STEPS]
    assert refused(steps=steps) == (
        "step 'train' runs once for each combination of its matrix= and this run does not run it again (it starts from "
        "'evaluate'), and step 'evaluate' needs its result: a step with matrix= cannot be reused yet. Start from step 'train'.")
    assert prepare("train", steps=steps, docs=earlier_run(steps)).reused == ["prepare"]    # starting from it is fine


def test_the_refusal_names_every_step_that_runs_and_needs_the_sweep():
    steps = with_sweep("evaluate")
    steps = [dict(s, needs=s["needs"] + ["tune"]) if s["name"] == "publish" else s for s in steps]
    assert refused(steps=steps) == (
        "step 'tune' runs once for each combination of its matrix= and this run does not run it again (it starts from "
        "'evaluate'), and steps 'evaluate' and 'publish' need its result: a step with matrix= cannot be reused yet. "
        "Start from step 'tune'.")


def test_a_step_that_reads_what_the_sweep_wrote_is_refused():
    """What a step reads of an earlier step (its datasets, models, artifacts) comes through needs=; so does the sweep's."""
    steps = with_sweep("evaluate")
    assert "step 'evaluate' needs its result" in refused(steps=steps)


def test_a_step_whose_when_reads_the_value_of_the_sweep_is_refused():
    steps = with_sweep("evaluate", when_text="tune.best == 1", when_fields=[])
    assert "step 'tune' runs once" in refused(steps=steps) and "step 'evaluate' needs its result" in refused(steps=steps)


def test_a_step_with_a_matrix_that_no_step_that_runs_needs_is_not_run_and_is_not_reused():
    steps = with_sweep()
    docs = earlier_run(steps, {**RESULTS}, {**PARAMS, "tune": {}}, images={**IMAGES, "tune": IMAGES["prepare"]})
    got = prepare("evaluate", steps=steps, docs=docs, params={**PARAMS, "tune": {}}, images={**IMAGES, "tune": IMAGES["prepare"]})
    assert got.reused == ["prepare", "train"] and "tune" not in got.entries
    assert "step tune: not run (its matrix is not needed by a step this run runs)" in got.notes
    assert f"step tune: reused from run {RUN}" not in got.notes
    assert "tune" not in [step for entry in got.pins.values() for step in entry["steps"]]


def test_a_sweep_that_is_not_run_needs_no_record_in_the_earlier_run():
    """It does not run, so the earlier run's record of it (failed, or never written) does not matter."""
    steps = with_sweep()
    got = prepare("evaluate", steps=steps, docs=earlier_run(steps, RESULTS, {**PARAMS, "tune": {}}), params={**PARAMS, "tune": {}})
    assert "tune" not in got.entries


def test_a_dataset_made_by_a_sweep_that_is_not_run_cannot_be_promoted_from_this_run():
    steps = with_sweep()
    steps[1] = dict(steps[1], produces=[{"name": "tuned", "type": "dataset"}])
    docs = earlier_run(steps, RESULTS, {**PARAMS, "tune": {}})
    message = refused(steps=steps, docs=docs, params={**PARAMS, "tune": {}}, promotes={"tuned": True})
    assert message.startswith("promote-tuned is true, but the dataset tuned was made by step tune of run")


def test_a_step_that_ran_on_an_hpc_class_in_the_earlier_run_is_refused():
    plan_classes = dict(CLASSES, train="meluxina-gpu")
    docs = earlier_run()
    docs[(RUN, rerun.PLAN)] = rerun.plan_of(RUN, UID, APP, "vision", "exp", STEPS, PARAMS, WHEN, plan_classes, IMAGES,
                                            {"raw": READ_RAW})
    assert refused(docs=docs, hpc={"meluxina-gpu"}) == (
        f"step 'train' ran on the HPC (Slurm) class 'meluxina-gpu' in run '{RUN}': the result of a step that ran there cannot "
        "be reused yet. Start from step 'train'.")


def test_an_hpc_step_that_reads_a_reused_model_is_refused_but_one_that_reads_a_dataset_is_not():
    assert refused(classes=dict(CLASSES, evaluate="meluxina-cpu"), hpc={"meluxina-cpu"}) == (
        "step 'evaluate' is set to run on the HPC (Slurm) class 'meluxina-cpu' and reads 'model' of step 'train', which this "
        f"run takes from run '{RUN}': an HPC step cannot read it yet. Choose a machine class of the cluster for 'evaluate', "
        "or start from step 'train'.")
    steps = STEPS + [{"name": "extra", "kind": "evaluate", "reads": [], "inputs": [], "needs": ["prepare"], "produces": []}]
    more = {**PARAMS, "extra": {}}
    done = {"version": 1, "status": "ok", "values": {}, "reads": [], "writes": []}
    got = prepare("extra", docs=earlier_run(steps, {**RESULTS, "publish": done}, more), steps=steps, params=more,
                  classes={**CLASSES, "extra": "meluxina-cpu"}, hpc={"meluxina-cpu"})
    assert got.reused == ["prepare", "train", "evaluate", "publish"], "`extra` runs on HPC and reads only a dataset"


def test_an_output_that_is_gone_is_refused_naming_the_step_to_start_from():
    assert refused(client=Lake(commits=[("dataset-hera-demo-clean", C["clean"]) ], objects=[])) == (
        f"output 'model' of step 'train' (run '{RUN}') is no longer there: outputs-hera-demo holds no files under "
        f"model/exp/{RUN}/. Run that step again: start from step 'train'.")
    assert refused(start="train", client=Lake()) == (
        f"output 'clean' of step 'prepare' (run '{RUN}') is no longer there: dataset-hera-demo-clean has no commit "
        f"{C['clean'][:12]}. Run that step again: start from step 'prepare'.")


def test_an_output_no_step_that_runs_again_needs_is_not_looked_for():
    assert prepare("publish", client=Lake(commits=[("dataset-hera-demo-clean", C["clean"])],
                                          objects=[("outputs-hera-demo", C["model"], f"model/exp/{RUN}/best.pt")])).reused


def test_a_dataset_a_reused_step_made_cannot_be_promoted_by_this_run():
    assert refused(promotes={"clean": True}) == (
        f"promote-clean is true, but the dataset clean was made by step prepare of run {RUN}, which this run does not run "
        f"again. Promote it from run {RUN}, or start from step prepare.")
    assert prepare("prepare", promotes={"clean": True}).reused == []
    assert prepare(promotes={"clean": False}).reused == ["prepare", "train"]


def test_a_result_that_would_be_over_its_cap_with_the_path_added_is_refused():
    small = cost(wire(dict(RESULTS["train"], values={"x": ""})))
    big = dict(RESULTS["train"], values={"x": "y" * (RESULT_CAP - small - 5)})      # 5 under the cap by itself
    assert cost(wire(big)) == RESULT_CAP - 5
    assert refused(docs=earlier_run(results={**RESULTS, "train": big})).startswith(
        f"the result of step 'train' in run '{RUN}' costs ")
    assert prepare(docs=earlier_run(results={**RESULTS, "train": dict(big, values={"x": "y"})})).reused


def test_the_log_says_the_images_of_the_reused_steps_are_the_earlier_runs():
    assert prepare().notes[-1] == f"the images of the steps that are not run again are the ones run {RUN} used (same digests)"


def test_a_store_that_does_not_answer_ends_the_run_and_the_sentence_has_no_error_text(capsys):
    class Down(Records):
        def read(self, client, app, run, name):
            raise ConnectionError("HTTPConnectionPool(host='lake.internal', port=8000): the data store is down")
    with pytest.raises(rerun.Refused) as exc:
        rerun.prepare(STEPS, RUN, "train", APP, PARAMS, WHEN, CLASSES, set(), {}, IMAGES, Down(), lake())
    assert str(exc.value) == ("reuse-from-run: the records of run 'hera-demo-aaaaa' cannot be read right now. Run again in a "
                              "minute; if it keeps happening, tell the platform team the name of the run.")
    assert "ConnectionError" not in str(exc.value) and "lake.internal" not in str(exc.value)
    assert "ConnectionError: HTTPConnectionPool(host='lake.internal'" in capsys.readouterr().err, "the detail is in the log"


# ----------------------------------------------------------- whose a record is: the run's instance, the plan's version

def test_records_of_another_run_that_had_the_same_name_are_refused():
    docs = earlier_run()
    docs[(RUN, "train")] = record_of("train", RESULTS["train"], instance="99999999-0000-4000-8000-999999999999")
    assert refused(docs=docs) == (
        f"the records of run '{RUN}' do not all belong to one run: the record of step 'train' was saved by another run that "
        "had the same name. Choose another run, or start a run of the whole pipeline.")


@pytest.mark.parametrize("change", [{"step": "prepare"}, {"app": "other"}, {"run": "hera-demo-zzzzz"}, {"version": 2},
                                    {"result": "nope"}])
def test_a_record_that_is_not_the_one_of_its_step_run_and_app_is_refused(change):
    docs = earlier_run()
    docs[(RUN, "train")] = dict(record_of("train", RESULTS["train"]), **change)
    assert "do not all belong to one run" in refused(docs=docs)


@pytest.mark.parametrize("change", [{"version": 2}, {"instance": ""}, {"app": "other"}, {"run": "elsewhere"}, {"version": None}])
def test_a_plan_this_platform_does_not_read_is_refused(change):
    assert refused(docs=earlier_run(**change)) == (
        f"reuse-from-run: the record of run '{RUN}' is not one this platform reads. Choose another run, or start a run of "
        "the whole pipeline.")


def test_a_run_whose_name_an_earlier_run_used_is_refused_when_it_starts_and_its_own_restart_is_not():
    records = Records(earlier_run())
    with pytest.raises(rerun.Refused) as exc:
        rerun.claim(records, lake(), APP, RUN, NEW_UID)
    assert str(exc.value) == (f"a run named '{RUN}' already saved its records for this app. Start the run again under a name "
                              "no earlier run had.")
    rerun.claim(records, lake(), APP, RUN, UID)                    # the same run: compose started again
    rerun.claim(records, lake(), APP, NEW, NEW_UID)                # a name nobody used
    rerun.claim(records, lake(), APP, RUN, "")                     # no instance given: nothing to tell


def test_a_store_that_does_not_answer_does_not_stop_a_run_from_starting(capsys):
    class Down(Records):
        def read(self, client, app, run, name):
            raise ConnectionError("down")
    rerun.claim(Down(), lake(), APP, RUN, UID)
    assert "cannot look for an earlier run named" in capsys.readouterr().err


# ------------------------------------------------------------------------------------------- the code: the image

def test_a_step_built_from_another_image_than_in_the_earlier_run_is_refused():
    now = dict(IMAGES, train=f"registry.example/hera-demo/train@sha256:{'cd' * 32}")
    assert refused(images=now) == (
        f"step 'train' was built from a different image in run '{RUN}' than it has now, so its code changed. Start from "
        "step 'train'.")
    assert prepare("train", images=now).reused == ["prepare"], "starting from it runs it with the new image"


def test_a_step_that_runs_again_may_have_another_image():
    assert prepare(images=dict(IMAGES, evaluate="registry.example/hera-demo/evaluate@sha256:" + "ef" * 32)).reused


@pytest.mark.parametrize("where", ["earlier", "now"])
def test_an_image_without_a_digest_cannot_be_compared_and_is_refused(where, capsys):
    plain = {k: v.split("@")[0] + ":v1" for k, v in IMAGES.items()}
    docs = earlier_run()
    if where == "earlier":
        docs[(RUN, rerun.PLAN)]["steps"][0]["image"] = plain["prepare"]
        got = refused(docs=docs)
    else:
        got = refused(images=plain)
    assert got.endswith("has no image with a digest in run 'hera-demo-aaaaa' or now, so its code cannot be compared. A step "
                        "before 'evaluate' is not run again. Start from step 'prepare'.")
    assert "image in run hera-demo-aaaaa" in capsys.readouterr().err


# ------------------------------------------------------------------------- a step the earlier run skipped by its when=

def no_record_of(step):
    docs = earlier_run()
    del docs[(RUN, step)]
    return docs


def test_a_step_skipped_by_its_when_on_form_fields_is_reused_as_skipped_when_the_earlier_form_made_it_false():
    docs = no_record_of("evaluate")
    docs[(RUN, rerun.PLAN)]["steps"][2]["when"] = {"mode": "off"}
    got = prepare("publish", docs=docs, when={"mode": "off"})
    assert got.entries["evaluate"] == {"version": 1, "status": "absent"}
    assert f"step evaluate: reused from run {RUN} (it was skipped there)" in got.notes
    assert "evaluate" not in {n for p in got.pins.values() for n in p["steps"]}


def test_a_step_with_no_record_whose_when_was_true_did_not_finish():
    message = refused(start="publish", docs=no_record_of("evaluate"))
    assert message.startswith(f"run '{RUN}' has no result for step 'evaluate' (it failed or did not finish).")


def test_a_step_with_no_record_whose_when_reads_a_value_of_another_step_cannot_be_told():
    docs = no_record_of("evaluate")
    docs[(RUN, rerun.PLAN)]["steps"][2]["when_text"] = "train.better == true"
    assert refused(start="publish", docs=docs) == (
        f"step 'evaluate' has no record in run '{RUN}' and its condition reads a value of another step, so it cannot be told "
        "whether it was skipped or did not finish. A step before 'publish' is not run again without one. Start from step "
        "'evaluate'.")


@pytest.mark.parametrize("text, values, want", [
    ("mode == full", {"mode": "full"}, True), ("mode == full", {"mode": "off"}, False), ("mode != off", {"mode": "off"}, False),
    ("mode == 'two words'", {"mode": "two words"}, True), ('mode == "x"', {}, False),
    ("a == 1 or b == 2", {"a": "0", "b": "2"}, True), ("a == 1 and b == 2", {"a": "1", "b": "3"}, False),
    ("a == 1 or b == 2 and c == 3", {"a": "1", "b": "0", "c": "0"}, True),
    ("a == 1 or b == 2 and c == 3", {"a": "0", "b": "2", "c": "0"}, False),
    ("train.better == true", {}, None), ("a == 1 and train.x == 2", {"a": "1"}, None),
    ("not a condition", {}, None), ("", {}, None),
])
def test_what_a_when_on_form_fields_said_in_the_earlier_run(text, values, want):
    assert rerun.when_value(text, values, ["train"]) is want


# ------------------------------------------------------------------------------------------------ settings, with types

def test_a_setting_that_differs_only_by_its_type_is_shown_with_both_types():
    assert rerun.changed_settings({"t": {"n": 1, "b": True, "s": "x"}}, {"t": {"n": "1", "b": 1, "s": "x"}}) == [
        't.n: 1 (number) -> "1" (text)', "t.b: true (boolean) -> 1 (number)"]
    assert rerun.changed_settings({"x": 1}, {"x": 1.0}) == [], "1 and 1.0 are the same number"
    assert rerun.changed_settings({"x": None}, {"x": "null"}) == ['x: null -> "null" (text)']
    assert rerun.changed_settings({"x": "a"}, {"x": "b"}) == ["x: a -> b"], "plain when the type is the same"


# ----------------------------------------------------------------------------------------------------- pinned inputs

class Datasets:
    """kaos-lib's datasets module as compose uses it; `now` is where each reference points now."""

    def __init__(self, now):
        self.now, self.resolved = now, []
        self.DatasetRef = types.SimpleNamespace(parse=self.parse)

    def parse(self, text):
        name, _, version = text.partition("@")
        owner, _, repo = name.rpartition("/")
        return types.SimpleNamespace(repo=repo, version=version, owner=owner or None)

    def source(self, ref, client=None):
        return ref, client

    def resolve(self, ref, client=None):
        self.resolved.append(f"{ref.repo}@{ref.version}")
        return self.now[f"{ref.repo}@{ref.version}"]


NOW = {"dataset-raw@latest": "f9" * 32, "dataset-raw@v3": "a1" * 32, "dataset-raw@v4": "b4" * 32, "dataset-raw@same": "a1" * 32}
HELD = {"raw": {"read": READ_RAW, "steps": ["prepare"], "run": RUN}}


def pins(value, now=NOW):
    ds = Datasets(now)
    return compose.pin_inputs({"raw": value}, STEPS[:1], datasets=ds, client=None, fixed=HELD), ds


def test_an_input_a_step_not_run_again_read_keeps_its_commit_and_never_follows_latest():
    reads, ds = pins("dataset-raw@latest")
    assert reads["raw"] == READ_RAW and reads["raw"]["commit"] == "a1" * 32 != NOW["dataset-raw@latest"]
    assert ds.resolved == [], "nothing is asked of the data store"
    assert pins("dataset-raw")[0]["raw"] == READ_RAW
    assert pins("dataset-raw@v3")[0]["raw"] == READ_RAW


def test_the_form_may_name_the_version_of_the_earlier_run_another_way_if_it_is_the_same_commit():
    reads, ds = pins("dataset-raw@same")
    assert reads["raw"] == READ_RAW and ds.resolved == ["dataset-raw@same"]


def test_the_form_naming_another_version_or_another_dataset_is_refused():
    expected = ("input 'raw': step prepare is not run again and read dataset-raw@v3 (commit a1a1a1a1a1a1) in run "
                "'hera-demo-aaaaa', but the form now names {!r}. Set it back, or leave the version out, or start from step "
                "'prepare'.")
    for value, other in (("dataset-raw@v4", "dataset-raw@v4"), ("dataset-labels@latest", "dataset-labels@latest"),
                         ("dataset-labels", "dataset-labels")):
        with pytest.raises(compose.ComposeError) as exc:
            pins(value, {**NOW, "dataset-labels@latest": "e5" * 32})
        assert str(exc.value) == expected.format(other), value


def test_an_input_read_only_by_steps_that_run_again_is_resolved_fresh():
    ds = Datasets(NOW)
    two = [dict(STEPS[0], inputs=[]), dict(STEPS[1], inputs=["raw"])]
    reads = compose.pin_inputs({"raw": "dataset-raw@latest"}, two, datasets=ds, fixed={})
    assert reads["raw"]["commit"] == NOW["dataset-raw@latest"] and ds.resolved == ["dataset-raw@latest"]


# ------------------------------------------------------------------------------------- the entry point, end to end

def table(*steps):
    return wire(list(steps))


def run_main(tmp_path, monkeypatch, records, lake_client, *args):
    monkeypatch.setattr(compose, "_record", lambda: records)
    monkeypatch.setattr(compose, "_kaos_lib", lambda: (Datasets({"dataset-raw@v3": "a1" * 32, "dataset-raw@latest": "a1" * 32}),
                                                       lake_client))
    monkeypatch.setenv("KAOS_STEP_IMAGES", json.dumps(IMAGES))
    argv = ["--run", NEW, "--run-uid", NEW_UID, "--app", APP, "--project", "vision", "--steps", table(*STEPS),
            "--context-dir", str(tmp_path / "ctx"),
            "--reused-dir", str(tmp_path / "reused"), "--ledger-output", str(tmp_path / "ledger.json"),
            "--advanced", "experiment: {name: exp}\nraw: dataset-raw@latest\nprepare: {rows: 10}\ntrain: {lr: 0.1, epochs: 3}\n"
                          "evaluate: {k: 5}", "--when-field", "mode=full", *args]
    return argv


@pytest.fixture
def app_config(monkeypatch, tmp_path):
    """A config tree whose sections are the ones the steps of these tests read."""
    root = tmp_path / "config"
    root.mkdir()
    (root / "config.yaml").write_text("defaults:\n  - _self_\n")
    (root / "config.yaml").write_text(
        "experiment:\n  name: exp\nraw: dataset-raw@latest\nprepare:\n  rows: 10\ntrain:\n  lr: 0.1\n  epochs: 3\n"
        "evaluate:\n  k: 5\n")
    from kubecore import derive_tree
    monkeypatch.setattr(derive_tree, "CONFIG_DIR", root)
    return root


def test_a_normal_run_writes_an_empty_reused_result_for_every_step_and_keeps_its_plan(tmp_path, monkeypatch, app_config, capsys):
    records = Records()
    compose.main(run_main(tmp_path, monkeypatch, records, lake()))
    for step in STEPS:
        assert (tmp_path / "reused" / f"{step['name']}.json").read_text() == ""
    (app, run, name, plan), = records.written
    assert (app, run, name) == (APP, NEW, "plan") and "reused_from" not in plan and plan["instance"] == NEW_UID
    assert plan["steps"][0]["image"] == IMAGES["prepare"] and plan["steps"][2]["when_text"] == "mode == full"
    assert json.loads((tmp_path / "ctx" / "train.json").read_text())["instance"] == NEW_UID
    assert [s["name"] for s in plan["steps"]] == ["prepare", "train", "evaluate", "publish"]
    assert plan["steps"][1]["params"] == {"train": {"lr": 0.1, "epochs": 3}} and plan["steps"][2]["when"] == {"mode": "full"}
    assert plan["pins"]["raw"]["commit"] == "a1" * 32 and plan["experiment"] == "exp"
    assert f"kept the plan of this run as kaos-runs/{NEW}/plan.json" in capsys.readouterr().out


def test_a_run_from_a_step_writes_the_reused_results_the_pins_and_a_plan_that_says_where_it_came_from(
        tmp_path, monkeypatch, app_config, capsys):
    records = Records(earlier_run())
    compose.main(run_main(tmp_path, monkeypatch, records, lake(), "--reuse-from-run", RUN, "--start-from-step", "evaluate"))
    out = capsys.readouterr().out
    assert f"step prepare: reused from run {RUN}" in out and f"step train: reused from run {RUN}" in out
    assert f"input raw: dataset-raw@v3 is commit {'a1' * 32} (as read in run {RUN})" in out
    reused = {s["name"]: (tmp_path / "reused" / f"{s['name']}.json").read_text() for s in STEPS}
    assert reused["evaluate"] == "" and reused["publish"] == ""
    assert json.loads(reused["train"])["writes"][0]["path"] == f"model/exp/{RUN}/"
    assert reused["train"] == wire(json.loads(reused["train"])), "in wire form"
    assert json.loads(reused["prepare"]) == RESULTS["prepare"]
    ledger = json.loads((tmp_path / "ledger.json").read_text())
    assert ledger["reads"] == [READ_RAW], "the new run reads the data the earlier run read"
    plan = records.written[-1][3]
    assert plan["reused_from"] == {"run": RUN, "start": "evaluate"}


def test_a_refusal_ends_compose_before_any_file_is_written_with_its_sentence_after_the_usual_prefix(
        tmp_path, monkeypatch, app_config, capsys):
    with pytest.raises(SystemExit) as stop:
        compose.main(run_main(tmp_path, monkeypatch, Records(earlier_run()), lake(), "--reuse-from-run", RUN,
                              "--start-from-step", "nope"))
    assert stop.value.code == 1
    assert capsys.readouterr().err == ("compose-and-validate FAILED: start-from-step: this pipeline has no step 'nope'. Its "
                                       "steps are: prepare, train, evaluate, publish.\n")
    assert not (tmp_path / "ctx").exists() and not (tmp_path / "reused").exists()


def test_a_changed_setting_is_refused_before_any_step_by_the_entry_point(tmp_path, monkeypatch, app_config, capsys):
    argv = run_main(tmp_path, monkeypatch, Records(earlier_run()), lake(), "--reuse-from-run", RUN,
                    "--start-from-step", "evaluate")
    argv[argv.index("--advanced") + 1] = argv[argv.index("--advanced") + 1].replace("lr: 0.1", "lr: 0.5")
    with pytest.raises(SystemExit):
        compose.main(argv)
    assert capsys.readouterr().err.startswith(
        f"compose-and-validate FAILED: step 'train' reads different settings than in run '{RUN}': train.lr: 0.1 -> 0.5. ")
    assert not (tmp_path / "ctx").exists()


def test_a_plan_that_cannot_be_kept_is_a_warning_and_the_run_goes_on(tmp_path, monkeypatch, app_config, capsys):
    class Broken(Records):
        def write(self, *a):
            raise RuntimeError("the lake is busy")
    compose.main(run_main(tmp_path, monkeypatch, Broken(), lake()))
    captured = capsys.readouterr()
    assert ("WARNING: the plan of this run could not be kept (the lake is busy). This run cannot be reused from a later step."
            in captured.err)
    assert (tmp_path / "ctx" / "train.json").is_file() and (tmp_path / "reused" / "train.json").read_text() == ""


def test_the_real_library_has_the_record_module_compose_calls():
    record = pytest.importorskip("kaos_lib.record", reason="set ENGINE_KAOS_LIB to a kaos-lib with record.py to run this")
    assert record.path("r", rerun.PLAN) == f"{RECORD_DIR}/r/plan.json"
    assert callable(record.read) and callable(record.write)
