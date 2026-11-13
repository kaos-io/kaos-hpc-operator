"""kaos-gather-<step>: the task after a matrix step. It joins what the step's items handed back into the one entry
the steps that need the step read in `upstream` (docs/step-contract.md, section 12).

The workflow engine gives the items' outputs as JSON lists: each item's result (KAOS_GATHER_RESULTS) and the entry it
ran for (KAOS_GATHER_ITEMS), in the same order, but not necessarily the order of the items. The run's plan
(KAOS_GATHER_PLAN) is every item as compose wrote it. This task puts them in index order and says for each item:

- `ok`: it handed back a result; the entry carries the result;
- `skipped`: it ended itself with `step.skip(reason)`; the entry carries the reason;
- `failed`: it handed back no result (the item failed, or never ran).

It fails, and so does the run, when an item failed: a matrix step is like any step, and a run in which a step
failed promotes nothing. The message names each failed item, its index and values. A step that declares
`matrix_allow_failed=True` accepts failed items: it then fails only when no item is `ok`, the step that needs it runs
and sees the failed items as they are, and the log says how many failed and which. A matrix step that was skipped as a whole (its `when=` was false) gives the sentinel, as any skipped step does. The
entry may cost MATRIX_ENTRY_CAP; the items share it.
"""

import json
import os
import sys

from kubecore.step_contract import MATRIX_ENTRY_CAP, MATRIX_STATUS, SENTINEL, cost, wire

STEP_ENV = "KAOS_GATHER_STEP"
RESULTS_ENV = "KAOS_GATHER_RESULTS"
ITEMS_ENV = "KAOS_GATHER_ITEMS"
PLAN_ENV = "KAOS_GATHER_PLAN"
STATUS_ENV = "KAOS_GATHER_STATUS"
ALLOW_FAILED_ENV = "KAOS_GATHER_ALLOW_FAILED"
RESULT_FILE = "/tmp/outputs/kaos-result.json"


class GatherError(Exception):
    pass


def _json(text, what):
    try:
        return json.loads(text or "[]")
    except ValueError as exc:
        raise GatherError(f"{what} is not JSON ({exc}); the platform built the run wrongly: report it with the run's "
                          f"name") from None


def _list(text, what):
    value = _json(text, what)
    if not isinstance(value, list):
        raise GatherError(f"{what} is not a list; the platform built the run wrongly: report it with the run's name")
    return value


def _result(raw):
    """(status, result or reason) of one item's result text."""
    try:
        result = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return "failed", "it handed back something that is not a result"
    if result == SENTINEL or not isinstance(result, dict):
        return "failed", "it ended without handing back a result"
    if result.get("status") == "skipped":
        return "skipped", result.get("reason") or ""
    if result.get("status") == "ok" and result.get("version") == 1:
        # The workflow system hands objects back with their names in alphabetical order; the contract's order is
        # the one the fixtures have.
        return "ok", {k: result[k] for k in ("version", "status", "values", "reads", "writes") if k in result}
    return "failed", "it handed back something that is not a result"


def _index(item):
    entry = json.loads(item) if isinstance(item, str) else item
    return entry.get("index") if isinstance(entry, dict) else entry


def _described(entries):
    """"index 1 (batch=32, lr=0.5), index 3 (...)": the items by what they were given."""
    return ", ".join("index %d (%s)" % (e["index"], ", ".join("%s=%s" % kv for kv in e["values"].items()))
                     for e in entries)


def gather(step, status, results, items, plan, allow_failed=False, say=print):
    """The entry of the matrix step in `upstream`, or the sentinel when the step did not run. GatherError when no
    item succeeded or the entry is too large."""
    if status in ("Skipped", "Omitted") or not plan:
        return dict(SENTINEL)
    got = {}
    for raw, item in zip(results, items):
        index = _index(item)
        if isinstance(index, (int, str)) and str(index).isdigit():
            got[int(index)] = _result(raw)
    out = []
    # The plan holds each item as the text of its `matrix` field: its values exactly as they were written.
    plan = [json.loads(e["text"]) for e in plan]
    for item in sorted(plan, key=lambda e: e["index"]):
        what, body = got.get(item["index"], ("failed", "it did not run"))
        entry = {"index": item["index"], "values": item["values"], "status": what}
        if what == "ok":
            entry["result"] = body
        elif what == "skipped":
            entry["reason"] = body
        out.append(entry)
    counts = {w: sum(1 for e in out if e["status"] == w) for w in ("ok", "skipped", "failed")}
    failed = [e for e in out if e["status"] == "failed"]
    if failed and not allow_failed:
        raise GatherError(f"{counts['failed']} of the {len(out)} items of step '{step}' failed: {_described(failed)}. "
                          f"A step that failed fails the run, so nothing after it runs and nothing is promoted. Look at "
                          f"those items' logs. If the step after '{step}' can choose among the items that did not fail, "
                          f"write matrix_allow_failed=True on step '{step}'")
    if not counts["ok"]:
        raise GatherError(f"no item of step '{step}' succeeded ({counts['failed']} failed, {counts['skipped']} "
                          f"skipped). Look at the items' logs: the step that needs '{step}' has nothing to choose from")
    if failed:
        say(f"kaos-gather: {counts['failed']} of the {len(out)} items of step '{step}' failed ({_described(failed)}); "
            f"matrix_allow_failed=True lets the step after it run with the others.")
    document = {"version": 1, "status": MATRIX_STATUS, "items": out}
    size = cost(wire(document))
    if size > MATRIX_ENTRY_CAP:
        raise GatherError(f"the results of the {len(out)} items of step '{step}' cost {size}, more than the "
                          f"{MATRIX_ENTRY_CAP} they share. Hand less on from each item with step.result(...): "
                          f"metrics belong in MLflow, and large values in an artifact")
    return document


def main(argv=None):
    """Exit codes: 0 done, 1 no item succeeded or the results are unusable."""
    step = os.environ.get(STEP_ENV, "?")
    try:
        document = gather(step, os.environ.get(STATUS_ENV, ""), _list(os.environ.get(RESULTS_ENV), RESULTS_ENV),
                          _list(os.environ.get(ITEMS_ENV), ITEMS_ENV), _list(os.environ.get(PLAN_ENV), PLAN_ENV),
                          os.environ.get(ALLOW_FAILED_ENV) == "true")
        text = wire(document)
        os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)
        with open(RESULT_FILE, "w", encoding="ascii") as f:
            f.write(text)
    except GatherError as exc:
        print(f"kaos-gather FAILED: {exc}", file=sys.stderr, flush=True)
        return 1
    ok = len(document.get("items", [])) if document["status"] == MATRIX_STATUS else 0
    print(f"kaos-gather: step '{step}': {ok} item(s) gathered." if ok else f"kaos-gather: step '{step}' did not run.",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
