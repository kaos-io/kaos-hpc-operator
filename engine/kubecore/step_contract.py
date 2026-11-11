"""The step contract as the engine writes it (docs/step-contract.md, PRD kaos#1911 D-07, D-08).

Names, caps, the wire form and the expression that assembles a step's context. Shared by the three
places that must agree: authoring.py (render), compose.py (run start) and meluxina.py (the HPC twin).
Stdlib only: the compose step imports it in an image without the render libraries.
"""

import json

CONTEXT_ENV = "KAOS_STEP_CONTEXT"
CONTEXT_PARAM = "kaos-context"            # a step's one input, and its DAG task argument
CONTEXT_OUTPUT_PREFIX = "kaos-context-"   # compose's output per step: kaos-context-<step>
CONTEXT_DIR = "/work/context"             # where compose writes them: <dir>/<step>.json
RESULT_PARAM = "kaos-result"              # a step's one output
RESULT_PATH = "/work/output/kaos-result.json"
LEDGER_PARAM = "kaos-ledger"              # compose's output: the run's ledger as the run starts (pinned inputs)
LEDGER_PATH = "/work/kaos-ledger.json"
SENTINEL = {"version": 1, "status": "absent"}
RESERVED_PREFIX = "kaos-"                 # step names and produced names the platform keeps for itself
RESERVED_CONFIG_KEYS = ("plan", "pipeline-version")

CONTEXT_VERSION = 1
LEDGER_VERSION = 2
# The size budget (docs/step-contract.md, "Size caps"). One step's whole definition reaches its pod as one
# environment string, which the kernel caps at 128 KiB; over it the step errors before it runs. The context
# sits in that string as a JSON string, so the caps count a value's cost (see cost()), not its bytes.
CONTEXT_CAP = 96 * 1024        # a whole context: the base compose writes plus one result per needed step
RESULT_CAP = 8 * 1024          # one result (kaos-lib refuses a larger one)
TEMPLATE_CAP = 24 * 1024       # a step's rendered definition without its context, as the workflow engine encodes it
MIN_BASE_CONTEXT_CAP = 16 * 1024   # what must be left for the base: a step may need at most ten steps
MAX_INPUTS = 16                # what one step may read by name: its inputs= plus the datasets its needs= steps produce
MAX_NAME = 40                  # characters of a step, input or produced name
MAX_APP_NAME = 55              # an HPC job's app field holds no more

KINDS = ("ingest", "transform", "train", "evaluate", "publish", "control")
# Kinds that never run on an HPC class: they move data in or out of the platform, or steer the run.
CLUSTER_ONLY_KINDS = ("ingest", "publish", "control")
PRODUCED_TYPES = ("model", "artifact", "dataset")
DATASET_PREFIX = "dataset-"    # a dataset is the repository dataset-<name>
OUTPUTS_PREFIX = "outputs-"    # models and artifacts live in outputs-<app>


# Characters a string never holds as themselves: braces (the workflow engine evaluates `{{...}}` wherever
# it meets it) and the three the workflow engine's own JSON encoder rewrites to six bytes each, which
# would make a value larger inside a step's definition than the cost counted for it.
_ESCAPED = {"{": "\\u007b", "}": "\\u007d", "&": "\\u0026", "<": "\\u003c", ">": "\\u003e"}


def _string(text: str) -> str:
    out = json.dumps(text, ensure_ascii=True)
    for char, escape in _ESCAPED.items():
        out = out.replace(char, escape)
    return out


def wire(value) -> str:
    """The wire form of a JSON value: compact, ASCII, and inside strings no brace, `&`, `<` or `>`.
    The reference writer is hack/contract-checks/wire.py."""
    if isinstance(value, str):
        return _string(value)
    if value is None or isinstance(value, (bool, int, float)):
        return json.dumps(value, allow_nan=False)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(wire(v) for v in value) + "]"
    if isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                raise TypeError("object keys must be strings, got %r" % (key,))
        return "{" + ",".join(_string(k) + ":" + wire(v) for k, v in value.items()) + "}"
    raise TypeError("not a JSON value: %r" % (value,))


def cost(text: str) -> int:
    """What a wire-form value costs inside a step's definition: its length as a JSON string, so every
    quote and backslash counts twice. The caps are counted on it."""
    return len(text) + text.count('"') + text.count("\\")


def base_context_cap(needs: int) -> int:
    """What a step's context may cost before the results of the `needs` steps it needs are joined in:
    the context cap less one full result for each."""
    return CONTEXT_CAP - RESULT_CAP * needs


def base_cost(base: str, needs: list) -> int:
    """What a base context costs as it will travel: the whole context with the result of every step in
    `needs` still empty, so the `upstream` keys are in the count."""
    upstream = ",".join(_string(need) + ":" for need in needs)
    return cost(base[:-1] + ",\"upstream\":{" + upstream + "}}")


def template_bytes(template: dict) -> int:
    """The size of a step's rendered definition as the workflow engine encodes it: compact JSON in UTF-8,
    with `&`, `<` and `>` written as six-byte escapes (so `&&` in a shell argument costs twelve bytes)."""
    text = json.dumps(template, separators=(",", ":"), ensure_ascii=False)
    return len(text.encode()) + 5 * sum(text.count(c) for c in "&<>") + 3 * sum(text.count(c) for c in "\u2028\u2029")


def produced_repo(app: str, kind: str, name: str) -> str:
    """The repository a produced thing is written to."""
    return DATASET_PREFIX + name if kind == "dataset" else OUTPUTS_PREFIX + app


def _quoted(text: str) -> str:
    """`text` as a string literal of the workflow engine's expression language."""
    return json.dumps(text)


def _task_output(task: str, parameter: str) -> str:
    return "tasks[%s].outputs.parameters[%s]" % (_quoted(task), _quoted(parameter))


def result_of(task: str, twin: str = "") -> str:
    """The expression for one upstream step's result. A step that was skipped, omitted or wrote nothing
    yields an empty value, never its declared default, so every result goes through `sprig.default` with
    the sentinel. `twin` is the step's HPC task: exactly one of the two ran, so the pair is asked in turn."""
    value = "sprig.default(%s, %s)" % (_quoted(wire(SENTINEL)), _task_output(twin or task, RESULT_PARAM))
    if twin:
        value = "sprig.default(%s, %s)" % (value, _task_output(task, RESULT_PARAM))
    return value


def context_expression(compose_task: str, step: str, needs: list) -> str:
    """The DAG task argument that joins a step's base context (compose's output) with the results of the
    steps it needs, as text (docs/step-contract.md, section 10). `}}` would end the expression, so the two
    closing braces are separate strings."""
    parts = ["sprig.trimSuffix(\"}\", %s)" % _task_output(compose_task, CONTEXT_OUTPUT_PREFIX + step)]
    for i, need in enumerate(needs):
        parts.append(_quoted(("," if i else ",\"upstream\":{") + _string(need) + ":"))
        parts.append(result_of(need))
    if not needs:
        parts.append(_quoted(",\"upstream\":{"))
    return "{{=" + " + ".join(parts + ["\"}\"", "\"}\""]) + "}}"


def pair_results(expression: str, task: str, twin: str) -> str:
    """`expression` with every result of `task` read from the pair (`task`, its HPC `twin`)."""
    return expression.replace(result_of(task), result_of(task, twin))
