#!/usr/bin/env python3
"""The wire form of the step contract's JSON values (docs/step-contract.md, "Wire form").

    wire.py dumps FILE.json          print the wire form of a JSON file (no newline at the end)
    wire.py write-fixtures           write FILE.wire beside every fixture and fill hpc-job.json
    wire.py check-fixtures           fail when a fixture, its .wire file or a cap is out of step

The wire form is what crosses a process boundary: the value of KAOS_STEP_CONTEXT, the content of the
result file, spec.context and status.result of an HPC job. It is JSON with five extra rules, so that
every hop (the workflow engine's templating, an environment value, a Slurm job script) carries it
unchanged:

  * no whitespace between tokens and no newline at the end;
  * ASCII only: every other character is written as a \\uXXXX escape, so bytes and characters count the same;
  * inside strings (keys too) "{" and "}" are written as \\u007b and \\u007d, so the text never holds
    "{{", which the workflow engine would evaluate as a template;
  * inside strings "&", "<" and ">" are written as \\u0026, \\u003c and \\u003e: the workflow engine
    writes them that way when it puts the value into a step's definition, so they would cost six bytes
    there and one here;
  * inside strings "$" is written as \\u0024: Kubernetes expands "$(NAME)" and reduces "$$" in a container's
    environment value, so a context or result put there as text would arrive changed. No "$" is ever written.

Any JSON parser reads it. This module is the reference writer; it has no dependency.
"""
import json
import pathlib
import re
import sys

CONTEXT_CAP = 96 * 1024      # the most a step context may cost
RESULT_CAP = 8 * 1024        # the most a step result may cost
MAX_NEEDS = 10               # the most steps one step may name in needs=
TEMPLATE_BUDGET = 24 * 1024  # the most a step's rendered definition may be, without its context
STEP_CEILING = 131057        # the longest definition the workflow engine can hand one step (check 3(c))
KEPT_FREE = 8000             # room for what a pool's configuration adds to every definition

assert CONTEXT_CAP + TEMPLATE_BUDGET + KEPT_FREE <= STEP_CEILING


def base_cap(needs):
    """The most a step's context may cost before any upstream result is in it (every result still to
    come counted at the full RESULT_CAP), for a step that names `needs` steps in needs=."""
    if not 0 <= needs <= MAX_NEEDS:
        raise ValueError("a step may need at most %d steps, got %d" % (MAX_NEEDS, needs))
    return CONTEXT_CAP - RESULT_CAP * needs
ABSENT = {"version": 1, "status": "absent"}

FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "docs" / "step-contract" / "fixtures"


# What is written as a \\uXXXX escape inside a string although JSON does not ask for it.
ESCAPED = "{}&<>$"


def _string(text):
    out = json.dumps(text, ensure_ascii=True)
    for ch in ESCAPED:
        out = out.replace(ch, chr(92) + "u%04x" % ord(ch))
    return out


def dumps(value):
    """The wire form of a JSON value."""
    if isinstance(value, str):
        return _string(value)
    if value is None or isinstance(value, (bool, int, float)):
        return json.dumps(value, allow_nan=False)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(dumps(v) for v in value) + "]"
    if isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                raise TypeError("object keys must be strings, got %r" % (key,))
        return "{" + ",".join(_string(k) + ":" + dumps(v) for k, v in value.items()) + "}"
    raise TypeError("not a JSON value: %r" % (value,))


def cost(wire_text):
    """What a value in wire form costs against its cap: its length as a JSON string, where each quote and
    each backslash counts twice. This is the room it takes in a step's definition (section 7)."""
    return len(wire_text) + wire_text.count('"') + wire_text.count(chr(92))


def _load(name):
    return json.loads((FIXTURES / name).read_text())


def _hpc_job():
    """hpc-job.json is derived: its context and result are the wire forms of two other fixtures."""
    context = _load("context-one-upstream.json")
    inputs = [{"name": r["name"], "repo": r["repo"], "commit": r["commit"], "ref": r["ref"]}
              for r in context["ledger"]["reads"]]
    for result in context["upstream"].values():
        inputs += [{"name": w["name"], "repo": w["repo"], "commit": w["commit"]}
                   for w in result.get("writes", []) if w["type"] == "dataset"]
    return {
        "apiVersion": "hpc.kaos.io/v1alpha1",
        "kind": "HPCArgoWorkflowJob",
        "metadata": {"name": "hpc-0123456789abcdef0123", "namespace": "ml-vision"},
        "spec": {
            "class": "meluxina-gpu",
            "image": "registry.example/vision/hera-demo/train@sha256:" + "5" * 64,
            "command": ["python", "-m", "app.entry"],
            "wallTimeMinutes": 720,
            "stepName": context["step"],
            "workflow": {"name": context["run"], "uid": "0b7f6c1e-1111-2222-3333-444455556666"},
            "app": context["app"],
            "inputs": inputs,
            "context": dumps(context),
        },
        "status": {"phase": "Succeeded", "result": dumps(_load("result-model.json"))},
    }


def write_fixtures():
    (FIXTURES / "hpc-job.json").write_text(json.dumps(_hpc_job(), indent=2) + "\n")
    for path in sorted(FIXTURES.glob("*.json")):
        if path.name != "hpc-job.json":
            path.with_suffix(".wire").write_text(dumps(json.loads(path.read_text())))


READ_FIELDS = ["name", "repo", "owner", "ref", "commit"]
WRITE_FIELDS = ["name", "type", "repo", "commit"]
PRODUCES_FIELDS = ["name", "type", "repo"]
RESULT_FIELDS = ["version", "status", "values", "reads", "writes"]
SKIPPED_FIELDS = ["version", "status", "reason"]
SKIP_REASON_MAX = 300      # characters of the reason a step gives when it ends as skipped
CONTEXT_FIELDS = ["version", "run", "app", "project", "step", "kind", "experiment", "params", "ledger", "produces",
                  "upstream"]
LEDGER_FIELDS = ["version", "reads", "writes"]
KINDS = ("ingest", "transform", "train", "evaluate", "publish", "control")
TYPES = ("model", "artifact", "dataset")
NAME = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
COMMIT = re.compile(r"^[0-9a-f]{64}$")


def _name(text):
    return isinstance(text, str) and len(text) <= 40 and bool(NAME.match(text)) and not text.startswith("kaos-")


def shape_of_read(e):
    """What is wrong with a ledger read entry, "" when nothing is."""
    if not isinstance(e, dict) or list(e) not in (READ_FIELDS, READ_FIELDS + ["line"]):
        return "fields must be %s (and optionally line), in that order" % ", ".join(READ_FIELDS)
    if not _name(e["name"]) or not str(e["repo"]).startswith("dataset-") or not COMMIT.match(str(e["commit"])):
        return "name, repo (dataset-...) or commit (64 hex) is not valid"
    if not (e["owner"] is None or isinstance(e["owner"], str)) or not isinstance(e["ref"], str) or not e["ref"]:
        return "owner must be a project or null, ref a non-empty string"
    if not isinstance(e.get("line", ""), str):
        return "line must be a string"
    return ""


def shape_of_write(e):
    if not isinstance(e, dict) or list(e)[:4] != WRITE_FIELDS or not set(list(e)[4:]) <= {"line", "path"}:
        return "fields must be %s (and optionally line, path), in that order" % ", ".join(WRITE_FIELDS)
    if not _name(e["name"]) or e["type"] not in TYPES or not COMMIT.match(str(e["commit"])):
        return "name, type or commit is not valid"
    if not all(isinstance(e.get(k, ""), str) for k in ("line", "path")):
        return "line and path must be strings"
    repo = str(e["repo"])
    if e["type"] == "dataset":
        # dataset-<app>-<name>: the app is named in the repository, the ledger does not know it, so only the shape is checked
        if not (repo.startswith("dataset-") and repo.endswith("-" + e["name"]) and len(repo) > len("dataset-") + len(e["name"]) + 1):
            return "repo must be dataset-<app>-<name> for a dataset"
    elif not repo.startswith("outputs-"):
        return "repo must be outputs-<app> for anything else"
    return ""


def shape_of_produces(e):
    if not isinstance(e, dict) or list(e) != PRODUCES_FIELDS:
        return "fields must be %s, in that order" % ", ".join(PRODUCES_FIELDS)
    if not _name(e["name"]) or e["type"] not in TYPES:
        return "name or type is not valid"
    return ""


def shape_of_ledger(v):
    if not isinstance(v, dict) or list(v) != LEDGER_FIELDS or v["version"] != 2:
        return "a ledger is {version: 2, reads, writes}"
    bad = [shape_of_read(r) for r in v["reads"]] + [shape_of_write(x) for x in v["writes"]]
    return next((b for b in bad if b), "")


MATRIX_FIELDS = ["version", "status", "items"]
MATRIX_ITEM_STATUSES = ("ok", "failed", "skipped")
MATRIX_ENTRY_FIELDS = ["index", "of", "values"]       # the context's `matrix`: the item this step is, of how many
MATRIX_MAX_ITEMS = 32
MATRIX_RESULT_CAP = 56 * 1024  # the whole entry of a matrix step in upstream


def shape_of_matrix_result(v):
    """The entry a matrix step has in `upstream`: one entry per item, in index order, each with the item's values and
    status; an ok item carries its result, a skipped one its reason."""
    if list(v) != MATRIX_FIELDS or v["version"] != 1 or not isinstance(v["items"], list) or not v["items"]:
        return "a matrix result is {version: 1, status: matrix, items: [one entry per item]}"
    if len(v["items"]) > MATRIX_MAX_ITEMS:
        return "a matrix result holds at most %d items" % MATRIX_MAX_ITEMS
    for i, item in enumerate(v["items"]):
        status = item.get("status") if isinstance(item, dict) else None
        want = ["index", "values", "status"] + {"ok": ["result"], "skipped": ["reason"]}.get(status, [])
        if not isinstance(item, dict) or list(item) != want or item["index"] != i or status not in MATRIX_ITEM_STATUSES \
                or not isinstance(item["values"], dict) or not item["values"]:
            return "item %d: an item is {index (its position), values, status[, result | reason]}" % i
        if status == "ok" and shape_of_result(item["result"]):
            return "item %d: %s" % (i, shape_of_result(item["result"]))
        if status == "skipped" and not (isinstance(item["reason"], str) and 0 < len(item["reason"]) <= SKIP_REASON_MAX):
            return "item %d: a skipped item has a reason of 1 to %d characters" % (i, SKIP_REASON_MAX)
    if not any(item["status"] == "ok" for item in v["items"]):
        return "a matrix result has at least one ok item: the task that joins the items fails the run otherwise"
    return ""


def shape_of_result(v):
    if v == ABSENT:
        return ""
    if isinstance(v, dict) and v.get("status") == "matrix":
        return shape_of_matrix_result(v)
    if isinstance(v, dict) and v.get("status") == "skipped":
        ok = list(v) == SKIPPED_FIELDS and v["version"] == 1 and isinstance(v["reason"], str) \
            and 0 < len(v["reason"]) <= SKIP_REASON_MAX
        return "" if ok else "a skipped result is {version: 1, status: skipped, reason: 1 to %d characters}" % SKIP_REASON_MAX
    if not isinstance(v, dict) or list(v) != RESULT_FIELDS or type(v["version"]) is not int or v["version"] != 1 or v["status"] != "ok":
        return "a result is {version: 1, status: ok, values, reads, writes} or the sentinel"
    if not isinstance(v["values"], dict) or any(k.startswith("kaos-") for k in v["values"]):
        return "values must be an object without kaos- keys"
    bad = [shape_of_read(r) for r in v["reads"]] + [shape_of_write(x) for x in v["writes"]]
    return next((b for b in bad if b), "")


def shape_of_context(v):
    matrix = isinstance(v, dict) and "matrix" in v      # only an item of a matrix step has `matrix`, after `upstream`
    if not isinstance(v, dict) or list(v) != CONTEXT_FIELDS + (["matrix"] if matrix else []) or v["version"] != 1:
        return "fields must be %s, in that order" % ", ".join(CONTEXT_FIELDS + (["matrix"] if matrix else []))
    if matrix:
        entry = v["matrix"]
        if not isinstance(entry, dict) or list(entry) != MATRIX_ENTRY_FIELDS or type(entry["index"]) is not int \
                or type(entry["of"]) is not int or not 0 <= entry["index"] < entry["of"] <= MATRIX_MAX_ITEMS \
                or not isinstance(entry["values"], dict) or not entry["values"]:
            return "matrix is {index, of, values}: index below of, of at most %d" % MATRIX_MAX_ITEMS
    if not _name(v["step"]) or v["kind"] not in KINDS or not isinstance(v["params"], dict):
        return "step, kind or params is not valid"
    if "platform" in v["params"] or "plan" in v["params"]:
        return "params must not hold the platform or plan section"
    if v["ledger"].get("writes") != []:
        return "the ledger of a context has no writes"
    bad = [shape_of_ledger(v["ledger"])] + [shape_of_produces(e) for e in v["produces"]]
    bad += ["upstream.%s: %s" % (k, shape_of_result(r)) for k, r in v["upstream"].items() if shape_of_result(r) or not _name(k)]
    return next((b for b in bad if b), "")


PLAN_FIELDS = ["version", "run", "app", "project", "experiment", "steps", "pins"]
PLAN_STEP_FIELDS = ["name", "kind", "needs", "inputs", "produces", "class", "params", "when"]


def shape_of_plan(v):
    """The plan compose keeps for a run (kaos-runs/<run>/plan.json): the steps, each step's settings, the commit of each input.
    A run that took results from an earlier one also has `reused_from` (after `pins`); a step with matrix= has `matrix`
    (after `produces`)."""
    if not isinstance(v, dict) or list(v) not in (PLAN_FIELDS, PLAN_FIELDS + ["reused_from"]) or v["version"] != 1:
        return "fields must be %s (and optionally reused_from), in that order" % ", ".join(PLAN_FIELDS)
    if "reused_from" in v and (not isinstance(v["reused_from"], dict) or list(v["reused_from"]) != ["run", "start"]):
        return "reused_from is {run, start}"
    for i, e in enumerate(v["steps"]):
        fields = PLAN_STEP_FIELDS[:5] + (["matrix"] if "matrix" in e else []) + PLAN_STEP_FIELDS[5:]
        if not isinstance(e, dict) or list(e) != fields:
            return "step %d: fields must be %s, in that order" % (i, ", ".join(fields))
        if not _name(e["name"]) or e["kind"] not in KINDS or not isinstance(e["params"], dict) or not isinstance(e["when"], dict):
            return "step %d: name, kind, params or when is not valid" % i
        if not all(_name(n) for n in e["needs"] + e["inputs"]) or not all(
                isinstance(p, dict) and list(p) == ["name", "type"] and _name(p["name"]) and p["type"] in TYPES
                for p in e["produces"]):
            return "step %d: needs, inputs or produces is not valid" % i
    bad = [shape_of_read(r) for r in v["pins"].values()] + ["a pin is under another name" for k, r in v["pins"].items()
                                                              if isinstance(r, dict) and r.get("name") != k]
    return next((b for b in bad if b), "")


def check_fixtures():
    problems = []

    def want(ok, text):
        if not ok:
            problems.append(text)

    names = sorted(p.name for p in FIXTURES.glob("*.json"))
    for name in names:
        if name == "hpc-job.json":
            continue
        value = _load(name)
        wire_file = FIXTURES / (name[:-5] + ".wire")
        want(wire_file.exists(), "%s has no .wire file" % name)
        if not wire_file.exists():
            continue
        wire = wire_file.read_bytes()
        want(wire.decode("ascii", "replace") == dumps(value), "%s: the .wire file is not its wire form" % name)
        want(json.loads(wire) == value, "%s: the .wire file does not parse back to the fixture" % name)
        want(b"{{" not in wire and all(b < 128 for b in wire), "%s: the wire form holds {{ or a non-ASCII byte" % name)
        want(not any(c in wire for c in b"&<>$"), "%s: the wire form holds &, <, > or $" % name)
        if name.startswith("context-"):
            want(cost(wire.decode("ascii")) <= CONTEXT_CAP, "%s is over the context cap" % name)
            want(not shape_of_context(value), "%s: %s" % (name, shape_of_context(value)))
        if name.startswith("result-"):
            want(cost(wire.decode("ascii")) <= (MATRIX_RESULT_CAP if _load(name).get("status") == "matrix" else RESULT_CAP),
                 "%s is over the result cap" % name)
            want(not shape_of_result(value), "%s: %s" % (name, shape_of_result(value)))
        if name.startswith("ledger-"):
            want(not shape_of_ledger(value), "%s: %s" % (name, shape_of_ledger(value)))
        if name.startswith("run-plan"):
            want(not shape_of_plan(value), "%s: %s" % (name, shape_of_plan(value)))
    sample = (FIXTURES / "context-escapes.wire").read_text()
    want(all(chr(92) + "u%04x" % ord(c) in sample for c in ESCAPED), "context-escapes.wire does not show every escape")
    want(shape_of_read({"name": "raw", "repo": "raw", "owner": None, "ref": "v1", "commit": "c" * 64}) != ""
         and shape_of_result({"version": 1, "status": "ok"}) != "", "the shape checks accept a wrong entry")
    want(_load("result-absent.json") == ABSENT, "result-absent.json is not the sentinel")
    want(_load("context-one-upstream.json")["upstream"] == {"prepare": _load("result-ok.json")},
         "context-one-upstream.json: upstream.prepare is not result-ok.json")
    want(_load("context-skipped-upstream.json")["upstream"] == {"train": _load("result-model.json"), "export": ABSENT},
         "context-skipped-upstream.json: upstream is not result-model.json plus the sentinel")
    want(_load("context-matrix-item.json")["upstream"] == _load("context-one-upstream.json")["upstream"],
         "context-matrix-item.json: upstream is not that of context-one-upstream.json")
    want(_load("context-matrix-upstream.json")["upstream"] == {"train": _load("result-matrix.json")},
         "context-matrix-upstream.json: upstream.train is not result-matrix.json")
    want(shape_of_matrix_result({"version": 1, "status": "matrix", "items": [{"index": 0, "values": {"a": 1}, "status": "failed"}]}) != "",
         "the shape checks accept a matrix result with no ok item")
    want(_load("result-skipped.json")["status"] == "skipped", "result-skipped.json is not a skipped result")
    want(_load("context-reused-upstream.json")["upstream"] == {"train": _load("result-reused.json"), "export": ABSENT},
         "context-reused-upstream.json: upstream is not result-reused.json plus the sentinel")
    reused = dict(_load("result-model.json"), writes=[dict(w, path="model/baseline/hera-demo-x7k2p/")
                                                        for w in _load("result-model.json")["writes"]])
    want(_load("result-reused.json") == reused, "result-reused.json is not result-model.json with the path of the run that wrote it")
    want(all(p["path"].endswith("/hera-demo-x7k2p/") for p in _load("result-reused.json")["writes"]),
         "result-reused.json: a path is <name>/<experiment>/<run>/ of the run that wrote it")
    want(_load("run-plan.json")["run"] == _load("context-no-upstream.json")["run"], "run-plan.json is not the plan of the fixtures' run")
    want(_load("run-plan.json")["pins"] == {r["name"]: r for r in _load("ledger-two-reads.json")["reads"]},
         "run-plan.json: pins are not the reads of ledger-two-reads.json")
    want(shape_of_plan({"version": 1}) != "" and shape_of_write({"name": "m", "type": "model", "repo": "outputs-a",
                                                                   "commit": "c" * 64, "path": 3}) != "",
         "the shape checks accept a wrong plan or a path that is not text")
    want(len(_load("ledger-two-reads.json")["reads"]) == 2, "ledger-two-reads.json does not hold two reads")
    want(_load("hpc-job.json") == _hpc_job(), "hpc-job.json is not derived from the other fixtures (run write-fixtures)")
    for p in problems:
        print("FAIL ", p)
    if problems:
        return 1
    print("PASS  %d fixtures, each equal to its wire form; caps and cross-references hold" % len(names))
    return 0


def main(argv):
    if len(argv) == 3 and argv[1] == "dumps":
        sys.stdout.write(dumps(json.loads(pathlib.Path(argv[2]).read_text())))
        return 0
    if argv[1:] == ["write-fixtures"]:
        write_fixtures()
        return 0
    if argv[1:] == ["check-fixtures"]:
        return check_fixtures()
    sys.stderr.write(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
