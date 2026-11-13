"""Starting a run from a step, with the results of an earlier run (docs/step-contract.md, section 13).

The run form's `reuse-from-run` names an earlier run of the app and `start-from-step` a step. The step and every step
after it run again. A step before it, or on a branch that does not meet it, is not run again: its result and its outputs
are taken from the earlier run.

The earlier run left two kinds of file in the app's outputs repository (`kaos-runs/<run>/`, kaos-lib's record.py):
`<step>.json`, the exact result each step handed on, and `plan.json`, what compose wrote when the run started (the steps,
each step's settings, the inputs it pinned). Everything here is decided in compose, before any step starts, and every
refusal is one plain sentence that says what to change. What this run takes from the earlier one:

* the result of each step it does not run, as compose's output `kaos-reused-<step>` (step_contract.result_of falls back to
  it), with `path` on every model or artifact written: it lives under the earlier run's folder;
* the commit of every input a step it does not run read: the new run reads the same data, never a newer `latest`.

Not in this version, each a refusal: a step with matrix= that would not run again, a step that ran on an HPC class, an HPC
step that reads a reused model or artifact, a run that itself reused another. The image a step ran with is not compared: a step
that is not run again is whatever ran in the earlier run.

Stdlib only: the compose step imports it in an image without the render libraries.
"""

import re
from dataclasses import dataclass, field

from kubecore.step_contract import (
    REUSE_FIELD,
    RESULT_CAP,
    START_FIELD,
    cost,
    wire,
)

PLAN = "plan"
PLAN_VERSION = 1
_RUN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=-]*$")        # kaos_lib.outputs: a run is one path segment
_SHOWN = 40                                                  # characters of a value in a refusal
_KEYS_SHOWN = 5


class Refused(Exception):
    """A run that cannot start from the step it was asked to start from; the message says what to change."""


@dataclass
class Reuse:
    """What a run that starts from a step takes from the earlier run."""
    run: str                                       # the earlier run
    start: str                                     # the step the run starts from
    reused: list                                   # names of the steps that are not run again, in the pipeline's order
    entries: dict = field(default_factory=dict)    # step -> its result (a dict), with `path` on every model or artifact
    pins: dict = field(default_factory=dict)       # input name -> {"read": the earlier run's read, "steps": [reused steps that read it]}
    notes: list = field(default_factory=list)      # what compose says in its log


# ------------------------------------------------------------------------------------------------ the pipeline's shape
def downstream(steps: list, start: str) -> set:
    """`start` and every step that needs it, directly or through the steps in between."""
    found = {start}
    for step in steps:           # the table is in the pipeline's order: a step comes after the steps it needs
        if found & set(step["needs"]):
            found.add(step["name"])
    return found


def reused_steps(steps: list, start: str) -> list:
    """The steps a run that starts from `start` does not run again: every step that is not `start` or after it, whether
    it comes before `start` or on a branch that never meets it."""
    again = downstream(steps, start)
    return [s["name"] for s in steps if s["name"] not in again]


def check_fields(steps: list, reuse: str, start: str) -> None:
    """Both fields or neither, and a step the pipeline has."""
    for name, value in ((REUSE_FIELD, reuse), (START_FIELD, start)):
        if not value and (reuse or start):
            raise Refused(f"{REUSE_FIELD} and {START_FIELD} go together: {name} is empty.")
    names = [s["name"] for s in steps]
    if start not in names:
        raise Refused(f"{START_FIELD}: this pipeline has no step '{start}'. Its steps are: {', '.join(names)}.")


# ------------------------------------------------------------------------------------------------------- the plan file
def plan_of(run: str, app: str, project: str, experiment: str, steps: list, params: dict, when: dict, classes: dict,
            pins: dict, reuse: "Reuse" = None) -> dict:
    """The run's plan, which compose keeps as kaos-runs/<run>/plan.json when the run starts: for each step what makes it
    the step it is (kind, needs, inputs, produces, matrix), its settings (`params`: the config sections it reads, and `when`:
    the values of the run form fields its `when=` names), the class it was given, and the commit of every input."""
    out = {"version": PLAN_VERSION, "run": run, "app": app, "project": project, "experiment": experiment,
           "steps": [{"name": s["name"], "kind": s["kind"], "needs": list(s["needs"]), "inputs": list(s["inputs"]),
                      "produces": [{"name": e["name"], "type": e["type"]} for e in s["produces"]],
                      **({"matrix": s["matrix"]} if s.get("matrix") else {}),
                      "class": classes.get(s["name"], ""), "params": params[s["name"]],
                      "when": {f: when.get(f, "") for f in s.get("when_fields", [])}} for s in steps],
           "pins": pins}
    if reuse:
        out["reused_from"] = {"run": reuse.run, "start": reuse.start}
    return out


def _flat(value, prefix="") -> dict:
    """{dotted key: value} for the leaves of a settings document (a list is one value)."""
    if isinstance(value, dict) and (value or not prefix):
        out = {}
        for key, item in value.items():
            out.update(_flat(item, f"{prefix}.{key}" if prefix else str(key)))
        return out
    return {prefix: value}


def _shown(value) -> str:
    text = value if isinstance(value, str) else wire(value)
    return text if len(text) <= _SHOWN else text[:_SHOWN - 3] + "..."


def changed_settings(old: dict, new: dict) -> list:
    """['key: old -> new', ...] for each setting that differs, in the order of the new settings, then the ones gone."""
    before, after = _flat(old), _flat(new)
    keys = [k for k in after if k not in before or before[k] != after[k]] + [k for k in before if k not in after]
    return [f"{k}: {_shown(before[k]) if k in before else '(not set)'} -> {_shown(after[k]) if k in after else '(not set)'}"
            for k in keys]


def _listed(items: list) -> str:
    shown = ", ".join(items[:_KEYS_SHOWN])
    return shown + (f" and {len(items) - _KEYS_SHOWN} more" if len(items) > _KEYS_SHOWN else "")


# --------------------------------------------------------------------------------------------------------- the checks
def _same_step(old: dict, new: dict) -> str:
    """What differs in what makes a step the step it is, or ''."""
    for key in ("kind", "needs", "inputs", "produces"):
        if old.get(key) != new.get(key):
            return f"its {key} was {_shown(old.get(key))} and is {_shown(new.get(key))}"
    return ""


def _read(record, client, app: str, run: str, name: str):
    try:
        return record.read(client, app, run, name)
    except Exception as exc:   # noqa: BLE001 - the store may not answer; the run is not started on a guess
        raise Refused(f"{REUSE_FIELD}: cannot read the record of run '{run}' for step '{name}' "
                      f"({type(exc).__name__}: {exc}). Run again in a minute.") from None


def _exists(client, repo: str, write: dict) -> str:
    """Why an output of the earlier run is not there, or '' when it is: a dataset by its commit, a model or an artifact by at
    least one object under its path."""
    try:
        if write["type"] == "dataset":
            client.commit_id(repo, write["commit"])
        elif not next(iter(client.list_objects(repo, write["commit"], write["path"])), None):
            return f"{repo} holds no files under {write['path']}"
    except Exception as exc:   # noqa: BLE001
        if getattr(exc, "status", None) == 404 or type(exc).__name__ == "NotFoundError":
            return f"{repo} has no commit {write['commit'][:12]}" if write["type"] == "dataset" else f"{repo} holds no files under {write['path']}"
        return f"it could not be looked for ({type(exc).__name__}: {exc})"
    return ""


def prepare(steps: list, run: str, start: str, app: str, params: dict, when: dict, classes: dict, hpc: set,
            promotes: dict, record, client) -> Reuse:
    """Everything a run that starts at `start` takes from `run`, checked: a Reuse, or Refused with the sentence that says
    what to change. `steps` is the pipeline's step table, `params` each step's settings now, `when` the values of the run
    form fields the `when=`s name, `classes` the class each step is given, `hpc` the names of the HPC classes and `promotes`
    {dataset: whether the form promotes it}."""
    reused = reused_steps(steps, start)
    by_name = {s["name"]: s for s in steps}
    for name in reused:
        if by_name[name].get("matrix"):
            raise Refused(f"step '{name}' runs once for each combination of its matrix= and this run does not run it again "
                          f"(it starts from '{start}'): a step with matrix= cannot be reused yet. Start from step '{name}'.")
    plan = _read(record, client, app, run, PLAN) if _RUN.match(run) else None
    if plan is None:
        raise Refused(f"{REUSE_FIELD}: no saved record of run '{run}' for this app. It may belong to another app, be "
                      f"misspelled, or be older than what the platform keeps.")
    if plan.get("reused_from"):
        origin = plan["reused_from"].get("run", "an earlier run")
        raise Refused(f"{REUSE_FIELD}: run '{run}' took the results of some of its steps from run '{origin}', so it has no "
                      f"record of them. Reuse run '{origin}' instead, or choose a run that ran every step it has.")
    recorded = {e["name"]: e for e in plan.get("steps", [])}
    experiment = plan.get("experiment") or ""
    out = Reuse(run, start, reused)
    for name in reused:
        entry = _read(record, client, app, run, name)
        if not isinstance(entry, dict) or entry.get("status") not in ("ok", "skipped"):
            raise Refused(f"run '{run}' has no result for step '{name}' (it failed, did not finish or was not run). A step "
                          f"before '{start}' is not run again without one. Start from step '{name}', or choose a run that "
                          f"finished it.")
        old, new = recorded.get(name), dict(by_name[name])
        if old is None:
            raise Refused(f"step '{name}' is not in run '{run}': the pipeline has changed since. A step before '{start}' is "
                          f"not run again. Start from step '{name}'.")
        new["produces"] = [{"name": e["name"], "type": e["type"]} for e in new["produces"]]
        problem = _same_step(old, new)
        if problem:
            raise Refused(f"step '{name}' is not the step run '{run}' ran: {problem}. A step before '{start}' is not run "
                          f"again. Start from step '{name}'.")
        diff = changed_settings(old.get("params", {}), params[name]) + changed_settings(
            old.get("when", {}), {f: when.get(f, "") for f in by_name[name].get("when_fields", [])})
        if diff:
            raise Refused(f"step '{name}' reads different settings than in run '{run}': {_listed(diff)}. A step before "
                          f"'{start}' is not run again, so its settings cannot change. Set them back, or start from step "
                          f"'{name}'.")
        if old.get("class") in hpc:
            raise Refused(f"step '{name}' ran on the HPC (Slurm) class '{old['class']}' in run '{run}': the result of a step "
                          f"that ran there cannot be reused yet. Start from step '{name}'.")
        out.entries[name] = _with_paths(entry, experiment, run)
        cap = cost(wire(out.entries[name]))
        if cap > RESULT_CAP:
            raise Refused(f"the result of step '{name}' in run '{run}' costs {cap} with the folders of its outputs added; "
                          f"the limit is {RESULT_CAP}. Start from step '{name}'.")
        out.notes.append(f"step {name}: reused from run {run}")
        for need in by_name[name]["inputs"]:
            read = (plan.get("pins") or {}).get(need)
            if not isinstance(read, dict):
                raise Refused(f"run '{run}' did not record which version of '{need}' step '{name}' read. Start from step "
                              f"'{name}'.")
            out.pins.setdefault(need, {"read": read, "steps": []})["steps"].append(name)
    again = [s for s in steps if s["name"] not in reused]
    for step in again:
        for need in step["needs"]:
            if need not in out.entries:
                continue
            for write in (out.entries[need].get("writes") or []):
                if write["type"] != "dataset" and classes.get(step["name"]) in hpc:
                    raise Refused(f"step '{step['name']}' is set to run on the HPC (Slurm) class '{classes[step['name']]}' and "
                                  f"reads '{write['name']}' of step '{need}', which this run takes from run '{run}': an HPC "
                                  f"step cannot read it yet. Choose a machine class of the cluster for '{step['name']}', or "
                                  f"start from step '{need}'.")
    for step in again:
        for need in step["needs"]:
            for write in (out.entries.get(need) or {}).get("writes", []):
                why = _exists(client, write["repo"], write)
                if why:
                    raise Refused(f"output '{write['name']}' of step '{need}' (run '{run}') is no longer there: {why}. Run "
                                  f"that step again: start from step '{need}'.")
    for dataset, promoted in promotes.items():
        if not promoted:
            continue
        maker = next((s["name"] for s in steps for e in s["produces"] if e["type"] == "dataset" and e["name"] == dataset), None)
        if maker in reused:
            raise Refused(f"promote-{dataset} is true, but the dataset {dataset} was made by step {maker} of run {run}, "
                          f"which this run does not run again. Promote it from run {run}, or start from step {maker}.")
    out.notes.append("the code of a step that is not run again is whatever ran in run %s: the images are not compared" % run)
    return out


def _with_paths(entry: dict, experiment: str, run: str) -> dict:
    """The result of a step as the earlier run handed it on, and `path` on every model or artifact it wrote: it lives in
    the folder of the earlier run, `<name>/<experiment>/<run>/`."""
    if entry.get("status") != "ok":
        return entry
    writes = [dict(w, path=f"{w['name']}/{experiment}/{run}/") if w.get("type") != "dataset" else w
              for w in entry.get("writes", [])]
    return {**entry, "writes": writes}
