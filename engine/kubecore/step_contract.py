"""The step contract as the engine writes it (docs/step-contract.md, PRD kaos#1911 D-07, D-08).

Names, caps, the wire form and the expression that assembles a step's context. Shared by the three
places that must agree: authoring.py (render), compose.py (run start) and meluxina.py (the HPC twin).
Stdlib only: the compose step imports it in an image without the render libraries.
"""

import json
import math
import re
from decimal import Decimal

CONTEXT_ENV = "KAOS_STEP_CONTEXT"
CONTEXT_PARAM = "kaos-context"            # a step's one input, and its DAG task argument
CONTEXT_OUTPUT_PREFIX = "kaos-context-"   # compose's output per step: kaos-context-<step>
CONTEXT_DIR = "/work/context"             # where compose writes them: <dir>/<step>.json
RESULT_PARAM = "kaos-result"              # a step's one output
RESULT_PATH = "/work/output/kaos-result.json"
IMAGES_PARAM = "kaos-images"              # compose's output: {step: the image the run resolved for it}
IMAGES_PATH = "/work/kaos-images.json"
IMAGE_INPUT = "{{inputs.parameters.image}}"   # a step template's container image: the DAG task gives the input (image_of)
LEDGER_PARAM = "kaos-ledger"              # compose's output: the run's ledger as the run starts (pinned inputs)
LEDGER_PATH = "/work/kaos-ledger.json"
SENTINEL = {"version": 1, "status": "absent"}
COMPOSE_TASK = "compose-and-validate"     # the run's first task; the name a step cannot take
REUSED_OUTPUT_PREFIX = "kaos-reused-"     # compose's output per step: kaos-reused-<step>, the result taken from an earlier run
REUSED_DIR = "/work/reused"               # where compose writes them: <dir>/<step>.json (empty for a step that runs)
RECORD_DIR = "kaos-runs"                  # in outputs-<app>: kaos-runs/<run>/<step>.json and plan.json (kaos-lib's record.py)
REUSE_FIELD = "reuse-from-run"            # the run form's fields for starting from a step with an earlier run's results
START_FIELD = "start-from-step"
RAW_FORM_ANNOTATION = "ui.kubecore.io/raw-form"   # author data the render may leave on the raw template; the trusted enhance step reads and removes it
PROMOTE_STEP = "kaos-promote"             # the run's final task (kubecore/promote.py); a name no step can take
PROMOTE_PARAM = "kaos-promote-plan"       # its one input: what each dataset-producing step wrote, and the form's promote-<name>
RESERVED_PREFIX = "kaos-"                 # step names and produced names the platform keeps for itself
RESERVED_CONFIG_KEYS = ("plan", "pipeline-version")
# A top-level config key the platform used to add and no longer does: an app that defines one itself is
# refused, as one that still names it in its defaults (kubecore/derive_tree.py refuse_platform_group).
REMOVED_CONFIG_KEYS = ("platform",)

CONTEXT_VERSION = 1
LEDGER_VERSION = 2
# The size budget (docs/step-contract.md, "Size caps"). One step's whole definition reaches its pod as one
# environment string, which the kernel caps at 128 KiB; over it the step errors before it runs. The context
# sits in that string as a JSON string, so the caps count a value's cost (see cost()), not its bytes.
CONTEXT_CAP = 96 * 1024        # a whole context: the base compose writes plus one result per needed step
RESULT_CAP = 8 * 1024          # one result (kaos-lib refuses a larger one)
TEMPLATE_CAP = 24 * 1024       # a step's rendered definition without its context, as the workflow engine encodes it
MAX_NEEDS = 10                 # steps one step may need: ten results leave 16 KiB for the rest of the context
MAX_INPUTS = 16                # what one step may read by name: its inputs= plus the datasets its needs= steps produce
MATRIX_MAX_ITEMS = 32          # combinations one matrix step runs
MATRIX_VALUES_CAP = 16 * 1024  # the `values` of all items of a matrix step together; an item may hand on its share (max_item_values)
MATRIX_ENTRY_CAP = 56 * 1024   # the whole entry of a matrix step in `upstream`: every item's values, reads and writes
MATRIX_MIN_BASE = 16 * 1024    # what a step that needs a matrix step keeps for the rest of its context
MATRIX_MAX_LEDGER = 4          # what an item may read and write together (inputs= plus produces=): see tests/test_matrix.py
MATRIX_INT_LIMIT = 2 ** 53     # a whole number in matrix= may not be larger: write it as text
MATRIX_RUN_BYTES = 1024 * 1024 # what fan_output_bytes (the results alone) may reach: the run's whole status is more (contract, section 12)
MATRIX_STATUS = "matrix"       # the status of the entry a matrix step has in `upstream` (docs/step-contract.md, section 12)
MATRIX_CLUSTER_ONLY_KINDS = ("ingest", "publish", "control")
GATHER_STEP_PREFIX = "kaos-gather-"       # the task that joins the results of a matrix step's items: kaos-gather-<step>
FAN_TEMPLATE_PREFIX = "kaos-matrix-"      # the template that runs a matrix step's items: kaos-matrix-<step>
ITEMS_PARAM = "kaos-item"                 # an item's entry, as the item's input and output
MAX_NAME = 40                  # characters of a step, input or produced name
MAX_APP_NAME = 55              # an HPC job's app field holds no more
MIN_DATASET_NAME = 3           # dataset-<name> must be a repository id the lake accepts
MAX_OVERRIDE = 16 * 1024       # bytes of the run's ADVANCED override: compose refuses a larger one
MAX_REF_VERSION = 255          # characters of the version in a dataset reference
MAX_REPO = 63                  # characters of a repository id

# The refusals that identify an app made before the current pipelines end with OLD_APP_NOTICE. The CI chart
# tells such an app from any other render failure by OLD_APP_MARKER (the authoring schema's `old_app_marker`).
OLD_APP_MARKER = "Create it again from the template."
OLD_APP_NOTICE = "This app was made before the current pipelines. " + OLD_APP_MARKER

KINDS = ("ingest", "transform", "train", "evaluate", "publish", "control")
# Kinds that never run on an HPC class: they move data in or out of the platform, or steer the run.
CLUSTER_ONLY_KINDS = ("ingest", "publish", "control")
# A step that produces a dataset is never offered an HPC class and gets no HPC variant: a job on HPC holds a
# token the lake treats as admin, which would bypass the rule that only the producing app writes its dataset.
# PRD kaos#1911 F-40 (jobs that hold the app's own rights) removes this rule: delete the constant and its uses.
DATASET_PRODUCERS_STAY_ON_CLUSTER = True
DATASET_PRODUCER_LABEL = "platform.kubecore.io/produces-dataset"   # on the step's pod, "true"
PRODUCED_TYPES = ("model", "artifact", "dataset")
DATASET_PREFIX = "dataset-"    # a dataset is a repository named dataset-...
MAX_DATASET_SUFFIX = 48        # characters the lake allows after dataset-: <app>-<name> for a produced dataset
OUTPUTS_PREFIX = "outputs-"    # models and artifacts live in outputs-<app>


# Characters a string never holds as themselves: braces (the workflow engine evaluates `{{...}}` wherever
# it meets it) and the three the workflow engine's own JSON encoder rewrites to six bytes each, which
# would make a value larger inside a step's definition than the cost counted for it; and `$`, which
# Kubernetes expands (`$(NAME)`, `$$`) in a container's env value, where the context travels.
_ESCAPED = {"{": "\\u007b", "}": "\\u007d", "&": "\\u0026", "<": "\\u003c", ">": "\\u003e", "$": "\\u0024"}


def _string(text: str) -> str:
    out = json.dumps(text, ensure_ascii=True)
    for char, escape in _ESCAPED.items():
        out = out.replace(char, escape)
    return out


def wire(value) -> str:
    """The wire form of a JSON value: compact, ASCII, and inside strings no brace, `&`, `<`, `>` or `$`.
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


def base_context_cap(needs: int, matrix_needs: int = 0) -> int:
    """What a step's context may cost before the results of the `needs` steps it needs are joined in:
    the context cap less one full result for each; a needed matrix step holds MATRIX_ENTRY_CAP, which
    the results of all its items share (`matrix_needs` of the `needs` are such steps)."""
    return CONTEXT_CAP - RESULT_CAP * (needs - matrix_needs) - MATRIX_ENTRY_CAP * matrix_needs


def max_item_values(items: int) -> int:
    """What the `values` of one item's result may cost: its share of MATRIX_VALUES_CAP, never more than a result."""
    return min(RESULT_CAP, MATRIX_VALUES_CAP // max(items, 1))


def fan_output_bytes(items: int, parallel: int, per_item: int) -> int:
    """The bytes the workflow system keeps of the results of a matrix step's items, as the run's status holds them:
    each item's result and index once, and again in the output of every round above it. A round runs `parallel`
    items, then the template calls itself for the rest, so round k's output holds the items of rounds k to the last."""
    rounds = -(-items // parallel)
    held = sum(min(items, (rounds - k) * parallel) for k in range(rounds))
    return held * per_item


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


APP_REPO_ENV = "KAOS_APP_REPO"   # set by the render runner: the app repository's root, where steps/ is


def step_folder(name: str) -> str:
    """The folder of a step, relative to the app repository: steps/<name with every hyphen as an underscore>."""
    return "steps/" + name.replace("-", "_")


def dataset_repo(app: str, name: str) -> str:
    """The repository of a dataset an app produces: named per app, so two apps of a project that declare the
    same dataset name (every app made from a template does) never share a repository and a writer."""
    return f"{DATASET_PREFIX}{app}-{name}"


def produced_repo(app: str, kind: str, name: str) -> str:
    """The repository a produced thing is written to. The one place a produced repository is named."""
    return dataset_repo(app, name) if kind == "dataset" else OUTPUTS_PREFIX + app


def dataset_name_problem(app: str, step: str, name: str):
    """Why app + name cannot be a dataset repository, or None. The lake allows 3 to 48 characters after
    `dataset-`; the produced name is 3 to 40 by itself, and the app's name comes before it."""
    suffix = f"{app}-{name}"
    if len(suffix) > MAX_DATASET_SUFFIX:
        return (f"step '{step}': dataset('{name}') in app '{app}' is the repository '{dataset_repo(app, name)}', and "
                f"the lake allows {MIN_DATASET_NAME} to {MAX_DATASET_SUFFIX} characters after '{DATASET_PREFIX}': "
                f"'{suffix}' has {len(suffix)}. Shorten the dataset name ('{name}') or the app's name ('{app}').")
    return None


def _quoted(text: str) -> str:
    """`text` as a string literal of the workflow engine's expression language."""
    return json.dumps(text)


def _task_output(task: str, parameter: str) -> str:
    return "tasks[%s].outputs.parameters[%s]" % (_quoted(task), _quoted(parameter))


def reused_output(task: str) -> str:
    """The expression for the result compose took from an earlier run for the step `task`: empty when the step runs."""
    return _task_output(COMPOSE_TASK, REUSED_OUTPUT_PREFIX + task)


def result_of(task: str, twin: str = "") -> str:
    """The expression for one upstream step's result. A step that was skipped, omitted or wrote nothing
    yields an empty value, never its declared default, so every result goes through `sprig.default` with
    the sentinel. A step whose result was taken from an earlier run (the run form's `reuse-from-run`) did not run: its
    result is compose's output `kaos-reused-<step>`, which comes before the sentinel and after the step's own result, so a
    step that runs always gives its own. `twin` is the step's HPC task: exactly one of the two ran, so the pair is asked in
    turn."""
    value = "sprig.default(%s, %s)" % (_quoted(wire(SENTINEL)), reused_output(task))
    if twin:
        value = "sprig.default(%s, %s)" % (value, _task_output(twin, RESULT_PARAM))
    return "sprig.default(%s, %s)" % (value, _task_output(task, RESULT_PARAM))


def result_value(task: str, key: str) -> str:
    """The expression for the value `key` of the result of the needed step `task`, for a `when=` that compares it. A
    needed step that RAN and finished well but gave no such value (a typo in the key, or a value never set, or null) is NOT
    an empty text: the expression fails, and the workflow engine shows the sentence below as the task's error, so the run
    stops with the step and the key named instead of the condition quietly being false. (`int()` of a sentence is the one
    failure the expression language lets us word.) `?.` keeps a result with no values from failing earlier with a message
    that names neither. A step that was skipped has no result at all and never reaches this: see `value_test`."""
    sentence = ("step '%s' ran but gave no value '%s' (a typo in the name, or the value was never set with "
                "step.result(%s=...)): a when= on a step's value needs the value" % (task, key, key))
    return "(fromJSON(%s)?.values?.[%s] ?? int(%s))" % (result_of(task), _quoted(key), _quoted(sentence))


MAX_EXACT_INTEGER = 2 ** 53          # a result's number is a double: above this an integer is not exact
_JSON_NUMBER = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?\Z")
_LOOKS_NUMERIC = re.compile(r"[+-]?([0-9][0-9_]*\.?[0-9_]*|\.[0-9]+)([eE][+-]?[0-9]+)?\Z")


def json_number_text(value: float) -> str:
    """A double as the workflow engine's `toJSON` writes it (Go's encoder): the shortest digits that read back, in
    plain notation from 1e-6 up to 1e21 and in exponent notation (`1e-7`, `1.5e+22`) outside; `1.0` is `1`."""
    if value == 0:
        return "-0" if math.copysign(1, value) < 0 else "0"
    digits = Decimal(repr(value))
    if 1e-6 <= abs(value) < 1e21:
        text = format(digits, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    sign, mantissa, exponent = digits.normalize().as_tuple()
    power = len(mantissa) - 1 + exponent
    body = str(mantissa[0]) + ("." + "".join(map(str, mantissa[1:])) if len(mantissa) > 1 else "")
    return "%s%se%s%d" % ("-" if sign else "", body, "+" if power >= 0 else "-", abs(power))


def when_text_problem(text: str) -> str:
    """Why a text of a `when=` comparison cannot be matched reliably, or ''. A value of a step's result is compared by
    its JSON text when it is not a string, so a text that looks like a number or a keyword but is not written the way
    JSON writes it would never match: it is refused, with what to write."""
    if any(ord(c) < 32 or ord(c) == 127 for c in text):
        return "it holds a control character (a line break, a tab): write the text on one line"
    if text.lower() in ("true", "false", "null") and text not in ("true", "false", "null"):
        return "write %s in lowercase: a true or false value is compared as the word true or false" % text.lower()
    if _JSON_NUMBER.match(text):
        number = float(text)
        if math.isinf(number):
            return "the number is too large for a result to hold"
        if re.fullmatch(r"-?[0-9]+", text) and abs(int(text)) > MAX_EXACT_INTEGER:
            return ("an integer above %d is not held exactly by a result: compare a smaller number, or hand on the "
                    "value as text" % MAX_EXACT_INTEGER)
        return ""
    if _LOOKS_NUMERIC.match(text):
        return "it looks like a number but is not written as one: write it as JSON writes it (for example 0.5, 1, -3 or 1e-5)"
    return ""


def canonical_when_text(text: str) -> str:
    """The text a non-string value is compared with: a number as `toJSON` writes it (`1e6` is `1000000`, `1.0` is `1`),
    anything else as it is (it can only match a string)."""
    if _JSON_NUMBER.match(text) and not math.isinf(float(text)):
        return json_number_text(float(text))
    return text


def _single_quoted(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def value_test(task: str, key: str, op: str, text: str) -> str:
    """The comparison of a step's value with the text of a `when=`, exact for every JSON value: a string is compared as
    text; a number or a boolean by its JSON text (`toJSON`), because the workflow engine's `string()` of a number writes
    `1e+06` for 1000000 and drops the point of `1.0`.

    A needed step that was SKIPPED (by its own `when=`: the absent sentinel, or by `step.skip()`: status `skipped`) has no
    value: the condition is then FALSE for `==` and for `!=` alike (nothing can be said about a value that does not exist),
    so the step that asked is skipped and the run goes on. A result with another status is malformed and fails the task. A
    step that ran well (`ok`) goes through `result_value`: if it did not set the key the task fails, naming step and key."""
    value = result_value(task, key)
    test = '(type(%s) == "string" ? %s == %s : toJSON(%s) == %s)' % (
        value, value, _single_quoted(text), value, _single_quoted(canonical_when_text(text)))
    if op != "==":
        test = "!" + test
    bad = ("step '%s' left a result that is not a result (its status is not ok, skipped or absent): "
           "a when= on a step's value needs a result" % task)
    status = "fromJSON(%s)?.status" % result_of(task)
    return '(%s in ["absent", "skipped"] ? false : (%s == "ok" ? %s : int(%s)))' % (status, status, test, _quoted(bad))


def image_of(step: str, compose_task: str = "compose-and-validate") -> str:
    """The DAG task argument that gives a step (or its HPC variant) the image the run resolved for it: compose read the
    image record once, checked it, and wrote it as the output `kaos-images`. The same string on the cluster and on HPC.
    A step's image is never a workflow parameter, so no submit can set it."""
    return "{{=fromJSON(%s)[%s]}}" % (_task_output(compose_task, IMAGES_PARAM), _quoted(step))


def context_expression(compose_task: str, step: str, needs: list, matrix=(), open_end: bool = False) -> str:
    """The DAG task argument that joins a step's base context (compose's output) with the results of the
    steps it needs, as text (docs/step-contract.md, section 10). `}}` would end the expression, so the two
    closing braces are separate strings. `open_end` leaves out the last brace: the context of a matrix step, which
    each item closes after adding its `matrix` field (authoring.pipeline._fan_template)."""
    parts = ["sprig.trimSuffix(\"}\", %s)" % _task_output(compose_task, CONTEXT_OUTPUT_PREFIX + step)]
    for i, need in enumerate(needs):
        parts.append(_quoted(("," if i else ",\"upstream\":{") + _string(need) + ":"))
        parts.append(gathered_result(need) if need in matrix else result_of(need))
    if not needs:
        parts.append(_quoted(",\"upstream\":{"))
    return "{{=" + " + ".join(parts + (["\"}\""] if open_end else ["\"}\"", "\"}\""])) + "}}"


def gather_task(step: str) -> str:
    return GATHER_STEP_PREFIX + step


def fan_template(step: str) -> str:
    return FAN_TEMPLATE_PREFIX + step


def gathered_result(step: str) -> str:
    """The expression for the result of a matrix step: what its gather task wrote, or the sentinel when the step
    did not run. (`result_of` for a step that runs once.)"""
    return "sprig.default(%s, %s)" % (_quoted(wire(SENTINEL)), _task_output(gather_task(step), RESULT_PARAM))


def fan_outputs(parameter: str, sources: list) -> str:
    """The expression of a fan template's output: the lists the steps `sources` of the template aggregated
    (a step that did not run has none), joined in order. The workflow engine gives a step that ran once per item
    its output as a JSON list; `rest` is the same template again for the items that wait for the next round."""
    parts = ["steps[%s].outputs?.parameters?.[%s] == nil ? [] : fromJSON(steps[%s].outputs.parameters[%s])"
             % (_quoted(s), _quoted(parameter), _quoted(s), _quoted(parameter)) for s in sources]
    return "toJSON(concat(%s))" % ", ".join("(%s)" % p for p in parts)


def pair_results(expression: str, task: str, twin: str) -> str:
    """`expression` with every result of `task` read from the pair (`task`, its HPC `twin`)."""
    return expression.replace(result_of(task), result_of(task, twin))
