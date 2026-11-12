"""The engine against the step contract's fixtures (docs/step-contract.md, PRD kaos#1911 CON-01).

The fixtures are loaded, never copied. What the engine writes must equal them byte for byte: the wire
form, a step's context as compose writes its base and the DAG task argument joins the upstream results,
the ledger compose writes, and the follower's side of the HPC job.
"""

import json
import types

import pytest
import yaml

from paths import APP, ENGINE

FIXTURES = ENGINE.parent / "docs" / "step-contract" / "fixtures"
COMPOSE = "compose-and-validate"

# The steps of the fixtures' run (fixtures/README.md), as authoring writes them into compose's --steps.
STEPS = {
    "prepare": {"name": "prepare", "kind": "transform", "inputs": ["raw"], "needs": [],
                "produces": [{"name": "embeddings", "type": "dataset"}]},
    "train": {"name": "train", "kind": "train", "inputs": ["labels"], "needs": ["prepare"],
              "produces": [{"name": "model", "type": "model"}]},
    "evaluate": {"name": "evaluate", "kind": "evaluate", "inputs": [], "needs": ["train", "export"],
                 "produces": [{"name": "report", "type": "artifact"}]},
}


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


def wire_of(name):
    return (FIXTURES / f"{name}.wire").read_text()


def evaluate(expression, outputs):
    """What the workflow engine makes of a `{{=...}}` task argument. `outputs` is {task: {parameter: text}};
    the output of a task that was skipped or omitted is the empty string (check 3(b))."""
    assert expression.startswith("{{=") and expression.endswith("}}")
    body = expression[3:-2]
    assert "}}" not in body, "}} inside the expression would end it"

    class Task:
        def __init__(self, parameters):
            self.outputs = types.SimpleNamespace(parameters=parameters)

    sprig = types.SimpleNamespace(
        trimSuffix=lambda suffix, text: text[:-len(suffix)] if text.endswith(suffix) else text,
        default=lambda default, value: value or default)
    return eval(body, {"__builtins__": {}}, {"sprig": sprig, "tasks": {t: Task(p) for t, p in outputs.items()}})


def base_of(context, step, promote=None):
    """The base context compose writes for `step`, from the fixture's own run, params and pinned reads."""
    from kubecore import compose
    reads = {r["name"]: r for r in fixture("ledger-two-reads")["reads"]}
    return compose.base_context(STEPS[step], context["run"], context["app"], context["project"],
                                context["experiment"], context["params"], reads, promote)


# ---------------------------------------------------------------- the wire form

@pytest.mark.parametrize("name", sorted(p.stem for p in FIXTURES.glob("*.wire")))
def test_the_engines_wire_writer_equals_every_wire_fixture_byte_for_byte(name):
    from kubecore.step_contract import wire
    text = wire(fixture(name))
    assert text == wire_of(name)
    assert "{{" not in text and text.isascii() and not text.endswith("\n")


def test_the_sentinel_and_the_names_are_the_contracts():
    from kubecore import step_contract as c
    assert c.SENTINEL == fixture("result-absent") and c.wire(c.SENTINEL) == wire_of("result-absent")
    assert (c.CONTEXT_ENV, c.CONTEXT_PARAM, c.RESULT_PARAM) == ("KAOS_STEP_CONTEXT", "kaos-context", "kaos-result")
    assert c.RESULT_PATH == "/work/output/kaos-result.json"
    assert c.KINDS == ("ingest", "transform", "train", "evaluate", "publish", "control")
    assert c.RESERVED_CONFIG_KEYS == ("plan", "pipeline-version")


def test_a_value_json_cannot_hold_is_refused_not_written():
    from kubecore.step_contract import wire
    for bad in (float("inf"), float("nan"), {1: "a key that is not text"}, object()):
        with pytest.raises((TypeError, ValueError)):
            wire({"params": bad})


def test_the_cost_of_a_value_counts_quotes_and_backslashes_twice():
    """The caps are counted on the cost: the room a value takes inside a step's definition, where it is a
    JSON string. The reference is cost() in hack/contract-checks/wire.py."""
    from kubecore.step_contract import cost
    for name in sorted(p.stem for p in FIXTURES.glob("*.wire")):
        text = wire_of(name)
        assert cost(text) == len(json.dumps(text)) - 2 == len(text) + text.count('"') + text.count("\\")


def test_the_size_budget_is_the_contracts():
    from kubecore import step_contract as c
    assert (c.CONTEXT_CAP, c.RESULT_CAP, c.TEMPLATE_CAP) == (98304, 8192, 24576)
    assert c.base_context_cap(0) == 98304 and c.base_context_cap(1) == 90112 and c.base_context_cap(10) == 16384
    assert c.MAX_NEEDS == 10 and c.base_context_cap(c.MAX_NEEDS) == 16384 and c.base_context_cap(11) < 16384
    # the base is counted as it travels: with the upstream keys, every result still empty
    base = wire_of("context-one-upstream").split(',"upstream"')[0] + "}"
    assert c.base_cost(base, ["prepare"]) == c.cost(base[:-1] + ',"upstream":{"prepare":}}')
    assert c.base_cost(base, []) == c.cost(base[:-1] + ',"upstream":{}}')
    # the guarantee: a full context, a full template and what the workflow engine adds fit one definition
    assert c.CONTEXT_CAP + c.TEMPLATE_CAP + 8 * 1024 == 131072


# ---------------------------------------------------------------- the context, assembled

def test_a_step_with_no_upstream_gets_exactly_the_fixture():
    from kubecore.step_contract import context_expression, wire
    want = fixture("context-no-upstream")
    base = wire(base_of(want, "prepare"))
    got = evaluate(context_expression(COMPOSE, "prepare", []), {COMPOSE: {"kaos-context-prepare": base}})
    assert got == wire_of("context-no-upstream")


def test_a_run_that_asks_to_promote_marks_the_produced_dataset():
    from kubecore.step_contract import context_expression, wire
    want = fixture("context-promote")
    base = wire(base_of(want, "prepare", promote={"embeddings": True}))
    got = evaluate(context_expression(COMPOSE, "prepare", []), {COMPOSE: {"kaos-context-prepare": base}})
    assert got == wire_of("context-promote")
    assert "promote" not in wire(base_of(want, "prepare", promote={"embeddings": False})), "absent means false"


def test_a_step_gets_the_result_of_the_step_it_needs_byte_for_byte():
    from kubecore.step_contract import context_expression, wire
    want = fixture("context-one-upstream")
    base = wire(base_of(want, "train"))
    got = evaluate(context_expression(COMPOSE, "train", ["prepare"]),
                   {COMPOSE: {"kaos-context-train": base}, "prepare": {"kaos-result": wire_of("result-ok")}})
    assert got == wire_of("context-one-upstream")
    assert json.loads(got) == want


def test_a_skipped_upstream_arrives_as_the_sentinel():
    """`export` was skipped by its when=: the workflow engine gives the empty string for its output."""
    from kubecore.step_contract import context_expression, wire
    want = fixture("context-skipped-upstream")
    base = wire(base_of(want, "evaluate"))
    got = evaluate(context_expression(COMPOSE, "evaluate", ["train", "export"]),
                   {COMPOSE: {"kaos-context-evaluate": base}, "train": {"kaos-result": wire_of("result-model")},
                    "export": {"kaos-result": ""}})
    assert got == wire_of("context-skipped-upstream")
    assert json.loads(got)["upstream"]["export"] == fixture("result-absent")


def test_braces_quotes_and_numbers_cross_unchanged():
    from kubecore.step_contract import context_expression, wire
    want = fixture("context-escapes")
    step = {"name": "prepare", "kind": "transform", "inputs": [], "needs": [], "produces": []}
    from kubecore import compose
    base = wire(compose.base_context(step, want["run"], want["app"], want["project"], want["experiment"],
                                     want["params"], {}))
    got = evaluate(context_expression(COMPOSE, "prepare", []), {COMPOSE: {"kaos-context-prepare": base}})
    assert got == wire_of("context-escapes")
    assert "1e-05" in got and "1.0" in got and "12345678901234567890" in got


def test_a_step_that_ran_on_hpc_is_read_from_its_twin_and_a_pair_that_did_not_run_is_the_sentinel():
    from kubecore.step_contract import context_expression, pair_results, wire
    want = fixture("context-one-upstream")
    base = {COMPOSE: {"kaos-context-train": wire(base_of(want, "train"))}}
    expression = pair_results(context_expression(COMPOSE, "train", ["prepare"]), "prepare", "prepare-hpc")
    ran = {"kaos-result": wire_of("result-ok")}
    skipped = {"kaos-result": ""}
    assert evaluate(expression, {**base, "prepare": ran, "prepare-hpc": skipped}) == wire_of("context-one-upstream")
    assert evaluate(expression, {**base, "prepare": skipped, "prepare-hpc": ran}) == wire_of("context-one-upstream")
    neither = json.loads(evaluate(expression, {**base, "prepare": skipped, "prepare-hpc": skipped}))
    assert neither["upstream"] == {"prepare": fixture("result-absent")}


# ---------------------------------------------------------------- the ledger

def test_compose_writes_the_ledger_of_the_fixture():
    from kubecore import compose
    from kubecore.step_contract import wire
    reads = fixture("ledger-two-reads")["reads"]
    assert wire(compose.ledger(reads)) == wire_of("ledger-two-reads")
    assert compose.ledger([])["version"] == 2


def test_the_run_ledger_is_composes_reads_plus_every_result():
    """The union needs no engine code: compose's part and the results already carry the entries."""
    union = fixture("ledger-run-union")
    seen = list(fixture("ledger-two-reads")["reads"])
    for result in (fixture("result-ok"), fixture("result-model")):
        seen += [r for r in result["reads"] if r not in seen]
    assert seen == union["reads"]
    assert fixture("result-ok")["writes"] + fixture("result-model")["writes"] == union["writes"]


# ---------------------------------------------------------------- the rendered pipeline

@pytest.fixture(scope="module")
def rendered():
    import runpy

    from kubecore import enhance
    context = yaml.safe_load((ENGINE / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
    context["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    raw = yaml.safe_load(runpy.run_path(str(APP / "pipeline.py"), run_name="__pipeline__")["p"].wt.to_yaml())
    return enhance.enhance(raw, context)


def test_the_follower_side_of_the_hpc_job_fixture(rendered):
    """spec.command and spec.context of the hpc-job fixture are what the engine hands the follower: the
    step's command, and the same context its pod gets. The job's result is the step's one output."""
    job = fixture("hpc-job")
    templates = {t["name"]: t for t in rendered["spec"]["templates"]}
    tasks = {t["name"]: t for t in templates["main"]["dag"]["tasks"]}
    twin = {p["name"]: p["value"] for p in tasks["hello-world-hpc"]["arguments"]["parameters"]}
    assert json.loads(twin["step-command"]) == job["spec"]["command"]
    own = {p["name"]: p["value"] for p in tasks["hello-world"]["arguments"]["parameters"]}
    assert twin["kaos-context"] == own["kaos-context"], "one context, on a pod and on HPC"
    env = {e["name"]: e.get("value") for e in templates["hpc-follower"]["container"]["env"]}
    assert env["KAOS_STEP_CONTEXT"] == "{{inputs.parameters.kaos-context}}"
    assert job["spec"]["context"] == wire_of("context-one-upstream") and job["status"]["result"] == wire_of("result-model")
    outputs = [o["name"] for o in templates["hpc-follower"]["outputs"]["parameters"]]
    assert outputs == ["slurm-job-id", "kaos-result"]


def test_every_step_runs_the_same_entry_with_one_value_and_hands_back_one_result(rendered):
    templates = {t["name"]: t for t in rendered["spec"]["templates"]}
    for name in ("dataset-loading", "hello-world"):
        step = templates[name]
        assert step["container"]["command"] == ["python", "-m", "app.entry"] and "args" not in step["container"]
        env = {e["name"]: e.get("value") for e in step["container"]["env"]}
        assert env["KAOS_STEP_CONTEXT"] == "{{inputs.parameters.kaos-context}}"
        assert "KAOS_LIB_LOCK" not in env
        sizing = ("gpus", "cpu", "mem", "disk")
        assert [p["name"] for p in step["inputs"]["parameters"] if p["name"] not in sizing] == ["kaos-context"]
        # no default: a step that ends well without a result fails in the workflow engine
        assert step["outputs"]["parameters"] == [{"name": "kaos-result", "valueFrom": {
            "path": "/work/output/kaos-result.json"}}]


def test_compose_writes_one_context_per_step_and_the_runs_ledger(rendered):
    compose = next(t for t in rendered["spec"]["templates"] if t["name"] == COMPOSE)
    outputs = {o["name"]: o["valueFrom"]["path"] for o in compose["outputs"]["parameters"]}
    assert outputs == {"kaos-ledger": "/work/kaos-ledger.json",
                       "kaos-context-dataset-loading": "/work/context/dataset-loading.json",
                       "kaos-context-hello-world": "/work/context/hello-world.json"}
    args = compose["container"]["args"]
    assert args[args.index("--run") + 1] == "{{workflow.name}}"
    assert args[args.index("--app") + 1] == "PLACEHOLDER-app" and args[args.index("--project") + 1]
    assert "--offline" not in args, "a rendered pipeline never composes offline"
    steps = json.loads(args[args.index("--steps") + 1])
    assert steps == [
        {"name": "dataset-loading", "kind": "ingest", "reads": [], "inputs": ["dataset"], "needs": [], "produces": []},
        {"name": "hello-world", "kind": "transform", "reads": ["experiment"], "inputs": ["dataset"],
         "needs": ["dataset-loading"],
         "produces": [{"name": "report", "type": "artifact"}]}]
    assert "{{" not in args[args.index("--steps") + 1]
