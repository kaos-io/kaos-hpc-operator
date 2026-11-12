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
    """Every DAG task that runs `template` is given the argument `name` (unless it has one)."""
    for tpl in spec["templates"]:
        for task in (tpl.get("dag") or {}).get("tasks", []):
            if task.get("template") != template:
                continue
            params = task.setdefault("arguments", {}).setdefault("parameters", [])
            if not any(p.get("name") == name for p in params):
                params.append({"name": name, "value": expression})


def add_input(step: dict, name: str, default: str) -> None:
    """The template takes the input `name`, with `default` for a task that passes none."""
    params = step.setdefault("inputs", {}).setdefault("parameters", [])
    if not any(p.get("name") == name for p in params):
        params.append({"name": name, "default": default})
