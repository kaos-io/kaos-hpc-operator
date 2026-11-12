"""What the run form says about a step besides the app's config (docs/step-contract.md, "The run form").

The form has, for a step that declares them in pipeline.py, `<step>-walltime` and `<step>-checkpoint-every`
(the value in code is the default), for each dataset a step produces `promote-<name>`, and for every step the
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

WALLTIME_EXAMPLE = "90, 90m or 12h"


def walltime_field(step: str) -> str:
    return f"{step}-walltime"


def checkpoint_field(step: str) -> str:
    return f"{step}-checkpoint-every"


def promote_field(dataset: str) -> str:
    return f"promote-{dataset}"


def class_field(step: str) -> str:
    return f"{step}-class"


def minutes(value) -> int:
    """'90' / '90m' -> 90, '12h' -> 720; 0 when the text is not minutes or hours, or is zero."""
    match = _TIME.match(str(value).strip())
    if not match:
        return 0
    return int(match.group(1)) * (60 if match.group(2) == "h" else 1)


def time_problem(field: str, value) -> str:
    """The refusal for a walltime or checkpoint-every value that is not minutes or hours, or ''."""
    if minutes(value):
        return ""
    return (f"{field} is {str(value)!r}. Write minutes or hours, for example {WALLTIME_EXAMPLE}: at least one "
            f"minute, no other unit.")


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
