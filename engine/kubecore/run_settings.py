"""What the run form says about a step besides the app's config (docs/step-contract.md, "The run form").

The form has `<step>-walltime` for every step and `<step>-checkpoint-every` for a step that declares
`checkpoint_every=` (the value in code is the default; the person who starts the run chooses), for each dataset a step produces `promote-<name>`, and for every step the
class it runs on, `<step>-class`. The enhancer adds the fields and hands every value to compose as an
argument; compose refuses a wrong one before any step starts and writes the numbers the steps need as one
output, `kaos-run-settings`, which the DAG reads. Nothing here needs more than the standard library: compose
imports it in an image without the render libraries.
"""

import json
import re

from kubecore.step_contract import IMAGES_PARAM, MATRIX_INT_LIMIT, wire

SETTINGS_PARAM = "kaos-run-settings"          # compose's output: {step: {"walltime": minutes, "checkpoint-every": minutes}}
SETTINGS_PATH = "/work/kaos-run-settings.json"
_TIME = re.compile(r"^([0-9]+)([mh]?)$")
PROMOTE_CHOICES = ("false", "true")

WALLTIME_EXAMPLE = "90m, 12h"
DEFAULT_WALLTIME = "4h"                       # a step with no walltime= (the HPC path's default, meluxina.py)


def walltime_field(step: str) -> str:
    return f"{step}-walltime"


def checkpoint_field(step: str) -> str:
    return f"{step}-checkpoint-every"


def promote_field(dataset: str) -> str:
    return f"promote-{dataset}"


def class_field(step: str) -> str:
    return f"{step}-class"


def matrix_field(step: str) -> str:
    return f"{step}-matrix"


def parallel_field(step: str) -> str:
    return f"{step}-parallel"


# What one run's access to its data and to tracking covers (PRD kaos#1911 F-40): the life of the access minus the
# margin kept for handing the result back. The controller's figure (internal/timelimit/timelimit.go,
# TokenCoverMinutes); tests/test_token_cover.py keeps the two equal.
TOKEN_COVER_MINUTES = 12 * 60 - 30
LONGEST_MINUTES = 30 * 24 * 60   # 30 days: no step runs longer, and a larger number is a typing mistake


def minutes(value) -> int:
    """'90' / '90m' -> 90, '12h' -> 720; 0 when the text is not minutes or hours, is zero, or is longer than
    30 days (a value that large would not fit what the run is given as its limit)."""
    match = _TIME.match(str(value).strip())
    if not match or len(match.group(1)) > 7:
        return 0
    total = int(match.group(1)) * (60 if match.group(2) == "h" else 1)
    return total if total <= LONGEST_MINUTES else 0


def time_problem(field: str, value) -> str:
    """The refusal for a walltime or checkpoint-every value that is not minutes or hours, or ''."""
    if minutes(value):
        return ""
    return (f"{field} is {str(value)!r}. Write minutes or hours, for example 90, 90m or 12h: at least one "
            f"minute, at most 30 days, no other unit.")


def promote_problem(field: str, value) -> str:
    if str(value).strip().lower() in PROMOTE_CHOICES:
        return ""
    return f"{field} is {str(value)!r}. Choose true or false."


def setting(step: str, key: str) -> str:
    """The expression for one number compose wrote for `step` (a string literal of the engine's language)."""
    document = 'tasks["compose-and-validate"].outputs.parameters[%s]' % json.dumps(SETTINGS_PARAM)
    return "int(fromJSON(%s)[%s][%s])" % (document, json.dumps(step), json.dumps(key))


SETTINGS_DOC = 'tasks["compose-and-validate"].outputs.parameters[%s]' % json.dumps(SETTINGS_PARAM)
NESTED_SETTINGS_DOC = "inputs.parameters[%s]" % json.dumps(SETTINGS_PARAM)   # the same document inside a fan template
IMAGES_DOC = 'tasks["compose-and-validate"].outputs.parameters[%s]' % json.dumps(IMAGES_PARAM)
NESTED_IMAGES_DOC = "inputs.parameters[%s]" % json.dumps(IMAGES_PARAM)


def in_fan_template(expression: str) -> str:
    """`expression` as the steps of a fan template read it: the documents compose wrote are inputs of the template,
    where `tasks[...]` is not available."""
    return expression.replace(SETTINGS_DOC, NESTED_SETTINGS_DOC).replace(IMAGES_DOC, NESTED_IMAGES_DOC)


def matrix_setting(step: str, key: str) -> str:
    """The expression for the list or number compose wrote for the matrix step `step` (a JSON text)."""
    return "toJSON(fromJSON(%s)[%s][%s])" % (SETTINGS_DOC, json.dumps(step), json.dumps(key))


def calls(spec: dict, template: str):
    """Every place that runs `template`: its DAG tasks, and the steps of a steps template (the template of a
    matrix step's items). Yields (call, nested): `nested` is True for a step inside a steps template, where
    `tasks[...]` is not available."""
    for tpl in spec["templates"]:
        for task in (tpl.get("dag") or {}).get("tasks", []):
            if task.get("template") == template:
                yield task, False
        for group in tpl.get("steps") or []:
            for entry in group:
                if entry.get("template") == template:
                    yield entry, True


def class_problem(step: str, chosen: str, offer: dict) -> str:
    """The refusal for a class the form does not offer `step`, or ''. `offer` is what the enhancer wrote
    for the step (kubecore.enhance.class_offer): the classes offered, and a reason and a remedy for each
    class of the platform that it withholds."""
    if chosen in offer["offered"]:
        return ""
    reason, remedy = offer["refused"].get(chosen) or (
        "it is not a machine class of this cluster", f"Choose one of: {', '.join(offer['offered'])}.")
    return f"step '{step}' cannot run on class '{chosen}': {reason}. {remedy}"


def add_task_argument(spec: dict, template: str, name: str, expression: str) -> None:
    """Every call of `template` (calls) is given the argument `name` (unless it has one)."""
    for task, nested in calls(spec, template):
        params = task.setdefault("arguments", {}).setdefault("parameters", [])
        if not any(p.get("name") == name for p in params):
            params.append({"name": name, "value": in_fan_template(expression) if nested else expression})


def add_input(step: dict, name: str, default: str) -> None:
    """The template takes the input `name`, with `default` for a task that passes none."""
    params = step.setdefault("inputs", {}).setdefault("parameters", [])
    if not any(p.get("name") == name for p in params):
        params.append({"name": name, "default": default})


# ----------------------------------------------------------------------------------------------------- matrix

MAX_ITEMS = 32
PARALLEL_ALL = "all"
DEFAULT_PARALLEL = 4   # the form's default: the 1-GPU steps one node holds, so an HPC class packs them on one node
_AXIS = re.compile(r"^[a-z][a-z0-9_]*$")
# What a text value of a combination may hold. The value travels inside the workflow system's expressions and
# the step's environment, where braces, $, &, < and > and quotes would be read as something else.
_SAFE_TEXT = re.compile(r"^[A-Za-z0-9 ._+/:=@,()\[\]#%^~*-]{1,64}\Z")
MATRIX_EXAMPLE = '[{"lr": 0.01, "batch": 16}, {"lr": 0.001, "batch": 32}]'


def axis_problem(step: str, axis) -> str:
    if isinstance(axis, str) and _AXIS.match(axis) and len(axis) <= 40:
        return ""
    return (f"step '{step}': matrix= holds the name {axis!r}. A name is lowercase letters, digits and _, starting "
            f"with a letter, at most 40 characters: it is how the step reads the value (step.params['lr']).")


def value_problem(where: str, value) -> str:
    """Why `value` cannot be a value of a combination, or ''. `where` says whose it is."""
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > MATRIX_INT_LIMIT:
        return (f"{where} is the number {value}, which is beyond {MATRIX_INT_LIMIT} (2 to the power 53): a number that "
                f"large cannot be kept exactly. Write it as text, for example \"{value}\".")
    if isinstance(value, bool) or (isinstance(value, (int, float)) and value == value and abs(value) != float("inf")):
        return ""
    if isinstance(value, str):
        if _SAFE_TEXT.match(value):
            return ""
        return (f"{where} is the text {value[:30]!r}. A text value is 1 to 64 letters, digits, spaces and "
                f"the characters . _ + / : = @ , ( ) [ ] # % ^ ~ * -")
    kind = {list: "a list", dict: "an object", type(None): "empty"}.get(type(value), type(value).__name__)
    return f"{where} is {kind}. A value is a number, true or false, or a short text."


def matrix_items(step: str, raw: str, axes: list) -> list:
    """The combinations of the run form's `<step>-matrix` as compose writes them for the step: [{"index", "of",
    "text"}], or ValueError with a refusal that names the field and the fix. `text` is the item's `matrix` field of
    its context, `{"index","of","values"}` in wire form: each value with the number and the text it was written
    with (`1.0` stays `1.0`), which the workflow system hands over unchanged as a text."""
    field = matrix_field(step)

    def refuse(text):
        raise ValueError(f"{field}: {text}")

    def no_constant(name):
        raise ValueError(f"{name} is not a JSON value")
    try:
        items = json.loads(raw, parse_constant=no_constant)
    except ValueError as exc:
        refuse(f"this is not valid JSON ({exc}). Write a list of combinations, for example {MATRIX_EXAMPLE}")
    if not isinstance(items, list) or not items:
        refuse(f"write a list with at least one combination, for example {MATRIX_EXAMPLE}")
    if len(items) > MAX_ITEMS:
        refuse(f"the list holds {len(items)} combinations; a step runs at most {MAX_ITEMS}. Remove {len(items) - MAX_ITEMS}.")
    out = []
    for i, item in enumerate(items):
        where = f"combination {i + 1}"
        if not isinstance(item, dict):
            refuse(f"{where} is not an object. Write each one as {{{', '.join(repr(a).replace(chr(39), chr(34)) + ': ...' for a in axes)}}}.")
        unknown = [k for k in item if k not in axes]
        if unknown:
            refuse(f"{where} has the name {unknown[0]!r}, which the step does not declare. The step declares: "
                   f"{', '.join(axes)}. Check the spelling.")
        missing = [a for a in axes if a not in item]
        if missing:
            refuse(f"{where} has no value for {missing[0]!r}. Every combination gives every name: {', '.join(axes)}.")
        for axis in axes:
            problem = value_problem(f"the value of '{axis}' in {where}", item[axis])
            if problem:
                refuse(problem)
        out.append({"index": i, "of": len(items), "values": {axis: item[axis] for axis in axes}})
    return [{"index": e["index"], "of": e["of"], "text": wire(e)} for e in out]


def parallel_count(step: str, raw: str, items: int) -> int:
    """How many items run at once: `all` or a whole number from 1 to 32 (more than there are items means all)."""
    text = str(raw).strip().lower()
    if text == PARALLEL_ALL:
        return items
    if text.isdigit() and 1 <= int(text) <= MAX_ITEMS:
        return min(int(text), items)
    raise ValueError(f"{parallel_field(step)} is {str(raw)!r}. Write a whole number from 1 to {MAX_ITEMS}, or {PARALLEL_ALL}.")


def combinations(matrix: dict) -> list:
    """Every combination of a step's matrix= values, the first name changing slowest: the default of `<step>-matrix`."""
    out = [{}]
    for axis, values in matrix.items():
        out = [{**done, axis: value} for done in out for value in values]
    return out
