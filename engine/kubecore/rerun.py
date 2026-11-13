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
step that reads a reused model or artifact, a run that itself reused another.

What makes a record trustworthy: the plan and every record carry the run's name, its app and its instance (the workflow's
uid), and compose refuses records of another instance; a run whose name was used before is refused when it starts. The code of
a reused step is compared by its image: the reference compose resolved, with its digest, must be the same as in the earlier run.

Stdlib only: the compose step imports it in an image without the render libraries.
"""

import re
import sys
from dataclasses import dataclass, field

from kubecore.step_contract import (
    REUSE_FIELD,
    SENTINEL,
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
def plan_of(run: str, instance: str, app: str, project: str, experiment: str, steps: list, params: dict, when: dict,
            classes: dict, images: dict, pins: dict, reuse: "Reuse" = None) -> dict:
    """The run's plan, which compose keeps as kaos-runs/<run>/plan.json when the run starts: the run's name and instance (the
    workflow's uid), and for each step what makes it the step it is (kind, needs, inputs, produces, matrix), the class and the
    image reference (with digest) it was given, its settings (`params`: the config sections it reads, `when`: the values of
    the run form fields its `when=` names, `when_text`: the condition as written) and the commit of every input."""
    out = {"version": PLAN_VERSION, "run": run, "instance": instance, "app": app, "project": project, "experiment": experiment,
           "steps": [{"name": s["name"], "kind": s["kind"], "needs": list(s["needs"]), "inputs": list(s["inputs"]),
                      "produces": [{"name": e["name"], "type": e["type"]} for e in s["produces"]],
                      **({"matrix": s["matrix"]} if s.get("matrix") else {}),
                      "class": classes.get(s["name"], ""), "image": images.get(s["name"], ""), "params": params[s["name"]],
                      "when": {f: when.get(f, "") for f in s.get("when_fields", [])},
                      "when_text": s.get("when_text", "")} for s in steps],
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


def _kind(value) -> str:
    """The type a setting has: what JSON tells apart. A boolean, a number and a text are three things, `1` is not `"1"`."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "text"
    return "null" if value is None else "list"


def _shown(value, typed: bool = False) -> str:
    shown = wire(value) if typed or not isinstance(value, str) else value
    shown = shown if len(shown) <= _SHOWN else shown[:_SHOWN - 3] + "..."
    return f"{shown} ({_kind(value)})" if typed and value is not None else shown


def changed_settings(old: dict, new: dict) -> list:
    """['key: old -> new', ...] for each setting that differs, in the order of the new settings, then the ones gone. A value
    is compared with its type: `1`, `true` and `"1"` differ, and are shown with their types when only the type differs."""
    before, after = _flat(old), _flat(new)
    differ = [k for k in after if k not in before or _kind(before[k]) != _kind(after[k]) or before[k] != after[k]]
    out = []
    for k in differ + [k for k in before if k not in after]:
        if k not in before or k not in after:
            out.append(f"{k}: {_shown(before[k]) if k in before else '(not set)'} -> "
                       f"{_shown(after[k]) if k in after else '(not set)'}")
        else:
            typed = _kind(before[k]) != _kind(after[k])
            out.append(f"{k}: {_shown(before[k], typed)} -> {_shown(after[k], typed)}")
    return out


def _listed(items: list) -> str:
    shown = ", ".join(items[:_KEYS_SHOWN])
    return shown + (f" and {len(items) - _KEYS_SHOWN} more" if len(items) > _KEYS_SHOWN else "")


# ---------------------------------------------------------------------------------------- a when= on form fields only
_COMPARE = re.compile(r"\s*([A-Za-z0-9_.-]+)\s*(==|!=)\s*(\"[^\"]*\"|'[^']*'|[A-Za-z0-9_.-]+)\s*")
_JOIN = re.compile(r"(and|or)\s+")


def when_value(text: str, values: dict, steps: list):
    """What a `when=` said in the earlier run: True (the step ran), False (it was skipped) or None (it reads a value of
    another step, or is not a condition this reads, so it cannot be told from the form values). The grammar is the one of
    authoring: `name ==|!= text`, joined by `and` / `or` (`and` binds tighter); `name` is a run form field, or `<step>.<value>`
    for a step the step needs, which is None."""
    parts, joins, at = [], [], 0
    text = text or ""
    while True:
        m = _COMPARE.match(text, at)
        if not m:
            return None
        left, op, right = m.group(1), m.group(2), m.group(3)
        if left.partition(".")[0] in steps and "." in left:
            return None
        right = right[1:-1] if right[:1] in "\"'" else right
        parts.append((str(values.get(left, "")) == right) == (op == "=="))
        at = m.end()
        j = _JOIN.match(text, at)
        if not j:
            return _fold(parts, joins) if at == len(text) else None
        joins.append(j.group(1))
        at = j.end()


def _fold(parts: list, joins: list) -> bool:
    """`and` before `or`."""
    groups, group = [], [parts[0]]
    for join, part in zip(joins, parts[1:]):
        if join == "and":
            group.append(part)
        else:
            groups.append(all(group))
            group = [part]
    groups.append(all(group))
    return any(groups)


# --------------------------------------------------------------------------------------------------------- the checks
def _same_step(old: dict, new: dict) -> str:
    """What differs in what makes a step the step it is, or ''."""
    for key in ("kind", "needs", "inputs", "produces"):
        if old.get(key) != new.get(key):
            return f"its {key} was {_shown(old.get(key))} and is {_shown(new.get(key))}"
    return ""


def _log(text: str) -> None:
    """Detail for the compose log; what a refusal says is never a client error, a host or an exception's name."""
    print(text, file=sys.stderr, flush=True)


def _read(record, client, app: str, run: str, name: str):
    try:
        return record.read(client, app, run, name)
    except Exception as exc:   # noqa: BLE001 - the store may not answer; the run is not started on a guess
        _log(f"cannot read the record '{name}' of run {run}: {type(exc).__name__}: {exc}")
        raise Refused(f"{REUSE_FIELD}: the records of run '{run}' cannot be read right now. Run again in a minute; if it "
                      f"keeps happening, tell the platform team the name of the run.") from None


def _belongs(body, name: str, app: str, run: str, instance: str) -> bool:
    return (isinstance(body, dict) and body.get("version") == 1 and body.get("app") == app and body.get("run") == run
            and body.get("instance") == instance and body.get("step") == name and isinstance(body.get("result"), dict))


def _exists(client, repo: str, write: dict) -> str:
    """Why an output of the earlier run is not there, or '' when it is: a dataset by its commit, a model or an artifact by at
    least one object under its path."""
    gone = f"{repo} has no commit {write['commit'][:12]}" if write["type"] == "dataset" else \
        f"{repo} holds no files under {write.get('path')}"
    try:
        if write["type"] == "dataset":
            client.commit_id(repo, write["commit"])
        elif not next(iter(client.list_objects(repo, write["commit"], write["path"])), None):
            return gone
    except Exception as exc:   # noqa: BLE001
        if getattr(exc, "status", None) == 404 or type(exc).__name__ == "NotFoundError":
            return gone
        _log(f"cannot look for {repo} {write.get('path') or write['commit']}: {type(exc).__name__}: {exc}")
        return "the data store did not answer when it was looked for"
    return ""


def claim(record, client, app: str, run: str, instance: str) -> None:
    """Before a run does anything: refuse it when another run of this app used its name and left a plan. The folder
    `kaos-runs/<run>/` is the run's alone, so a run name that was used before cannot mix its records with this run's. The same
    run (same instance) starting compose again is fine. A store that does not answer is not a refusal: the run goes on, and
    saves what it can."""
    if not instance:
        return
    try:
        plan = record.read(client, app, run, PLAN)
    except Exception as exc:   # noqa: BLE001
        _log(f"cannot look for an earlier run named {run}: {type(exc).__name__}: {exc}")
        return
    if isinstance(plan, dict) and plan.get("instance") != instance:
        raise Refused(f"a run named '{run}' already saved its records for this app. Start the run again under a name no "
                      f"earlier run had.")


def prepare(steps: list, run: str, start: str, app: str, params: dict, when: dict, classes: dict, hpc: set,
            promotes: dict, images: dict, record, client) -> Reuse:
    """Everything a run that starts at `start` takes from `run`, checked: a Reuse, or Refused with the sentence that says
    what to change. `steps` is the pipeline's step table, `params` each step's settings now, `when` the values of the run
    form fields the `when=`s name, `classes` the class each step is given, `hpc` the names of the HPC classes, `promotes`
    {dataset: whether the form promotes it} and `images` {step: the image reference compose resolved for it}."""
    reused = reused_steps(steps, start)
    by_name = {s["name"]: s for s in steps}
    for name in reused:
        if by_name[name].get("matrix"):
            raise Refused(f"step '{name}' runs once for each combination of its matrix= and this run does not run it again "
                          f"(it starts from '{start}'): a step with matrix= cannot be reused yet. Start from step '{name}'.")
    plan = _read(record, client, app, run, PLAN) if _RUN.match(run) else None
    if plan is None:
        raise Refused(f"{REUSE_FIELD}: run '{run}' saved no record for this app (its step images may be older than this "
                      f"feature, or saving failed: see that run's log). Check the name: it must be a run of this app.")
    instance = plan.get("instance")
    if (plan.get("version") != PLAN_VERSION or plan.get("run") != run or plan.get("app") != app
            or not isinstance(instance, str) or not instance or not isinstance(plan.get("steps"), list)):
        raise Refused(f"{REUSE_FIELD}: the record of run '{run}' is not one this platform reads. Choose another run, or "
                      f"start a run of the whole pipeline.")
    if plan.get("reused_from"):
        origin = plan["reused_from"].get("run", "an earlier run")
        raise Refused(f"{REUSE_FIELD}: run '{run}' took the results of some of its steps from run '{origin}', so it has no "
                      f"record of them. Reuse run '{origin}' instead, or choose a run that ran every step it has.")
    recorded = {e["name"]: e for e in plan["steps"]}
    experiment = plan.get("experiment") or ""
    out = Reuse(run, start, reused)
    for name in reused:
        old, new = recorded.get(name), dict(by_name[name])
        if old is None:
            raise Refused(f"step '{name}' is not in run '{run}': the pipeline has changed since. A step before '{start}' is "
                          f"not run again. Start from step '{name}'.")
        body = _read(record, client, app, run, name)
        entry = None
        if body is None:
            # No record: the step failed, did not finish, or its own when= was false. The plan says which it can be told.
            seen = when_value(old.get("when_text", ""), old.get("when", {}), list(old.get("needs", []))) \
                if old.get("when_text") else True
            if seen is False:
                entry = dict(SENTINEL)      # it was skipped: what the earlier run's steps saw
            elif seen is None:
                raise Refused(f"step '{name}' has no record in run '{run}' and its condition reads a value of another step, so "
                              f"it cannot be told whether it was skipped or did not finish. A step before '{start}' is not "
                              f"run again without one. Start from step '{name}'.")
        elif not _belongs(body, name, app, run, instance):
            raise Refused(f"the records of run '{run}' do not all belong to one run: the record of step '{name}' was saved "
                          f"by another run that had the same name. Choose another run, or start a run of the whole pipeline.")
        elif body["result"].get("status") in ("ok", "skipped"):
            entry = body["result"]
        if entry is None:
            raise Refused(f"run '{run}' has no result for step '{name}' (it failed or did not finish). A step before "
                          f"'{start}' is not run again without one. Start from step '{name}', or choose a run that "
                          f"finished it.")
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
        before, now = old.get("image") or "", images.get(name) or ""
        if "@sha256:" not in before or "@sha256:" not in now:
            _log(f"step {name}: image in run {run}: {before or '(none)'}; now: {now or '(none)'}")
            raise Refused(f"step '{name}' has no image with a digest in run '{run}' or now, so its code cannot be compared. "
                          f"A step before '{start}' is not run again. Start from step '{name}'.")
        if before != now:
            raise Refused(f"step '{name}' was built from a different image in run '{run}' than it has now, so its code "
                          f"changed. Start from step '{name}'.")
        out.entries[name] = _with_paths(entry, experiment, run)
        cap = cost(wire(out.entries[name]))
        if cap > RESULT_CAP:
            raise Refused(f"the result of step '{name}' in run '{run}' costs {cap} with the folders of its outputs added; "
                          f"the limit is {RESULT_CAP}. Start from step '{name}'.")
        out.notes.append(f"step {name}: reused from run {run}" + (" (it was skipped there)" if entry == SENTINEL else ""))
        if entry.get("status") == "absent":
            continue
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
    out.notes.append(f"the images of the steps that are not run again are the ones run {run} used (same digests)")
    return out


def _with_paths(entry: dict, experiment: str, run: str) -> dict:
    """The result of a step as the earlier run handed it on, and `path` on every model or artifact it wrote: it lives in
    the folder of the earlier run, `<name>/<experiment>/<run>/`."""
    if entry.get("status") != "ok":
        return entry
    writes = [dict(w, path=f"{w['name']}/{experiment}/{run}/") if w.get("type") != "dataset" else w
              for w in entry.get("writes", [])]
    return {**entry, "writes": writes}
