#!/usr/bin/env python3
"""The wire form of the step contract's JSON values (docs/step-contract.md, "Wire form").

    wire.py dumps FILE.json          print the wire form of a JSON file (no newline at the end)
    wire.py write-fixtures           write FILE.wire beside every fixture and fill hpc-job.json
    wire.py check-fixtures           fail when a fixture, its .wire file or a cap is out of step

The wire form is what crosses a process boundary: the value of KAOS_STEP_CONTEXT, the content of the
result file, spec.context and status.result of an HPC job. It is JSON with three extra rules, so that
every hop (the workflow engine's templating, an environment value, a Slurm job script) carries it
unchanged:

  * no whitespace between tokens and no newline at the end;
  * ASCII only: every other character is written as a \\uXXXX escape, so bytes and characters count the same;
  * inside strings (keys too) "{" is written \\u007b and "}" is written \\u007d, so the text never holds
    "{{", which the workflow engine would evaluate as a template.

Any JSON parser reads it. This module is the reference writer; it has no dependency.
"""
import json
import pathlib
import sys

CONTEXT_CAP = 96 * 1024   # the most a step context may cost
RESULT_CAP = 32 * 1024    # the most a step result may cost
ABSENT = {"version": 1, "status": "absent"}

FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "docs" / "step-contract" / "fixtures"


def _string(text):
    return json.dumps(text, ensure_ascii=True).replace("{", "\\u007b").replace("}", "\\u007d")


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
    inputs = [{"name": r["name"], "repo": r["repo"], "ref": r["ref"], "commit": r["commit"]}
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
        if name.startswith("context-"):
            want(cost(wire.decode("ascii")) <= CONTEXT_CAP, "%s is over the context cap" % name)
            want(list(value) == ["version", "run", "app", "project", "step", "kind", "experiment", "params",
                                 "ledger", "produces", "upstream"], "%s: fields differ from the contract" % name)
            want(value["version"] == 1 and value["ledger"]["version"] == 2, "%s: versions" % name)
        if name.startswith("result-"):
            want(cost(wire.decode("ascii")) <= RESULT_CAP, "%s is over the result cap" % name)
        if name.startswith("ledger-"):
            want(value["version"] == 2 and all(r.get("name") for r in value["reads"]), "%s: not a version 2 ledger" % name)
    sample = (FIXTURES / "context-escapes.wire").read_text()
    want(cost(sample) == len(json.dumps(sample)) - 2, "cost() is not the length as a JSON string")
    want(_load("result-absent.json") == ABSENT, "result-absent.json is not the sentinel")
    want(_load("context-one-upstream.json")["upstream"] == {"prepare": _load("result-ok.json")},
         "context-one-upstream.json: upstream.prepare is not result-ok.json")
    want(_load("context-skipped-upstream.json")["upstream"] == {"train": _load("result-model.json"), "export": ABSENT},
         "context-skipped-upstream.json: upstream is not result-model.json plus the sentinel")
    want(_load("context-promote.json")["produces"] == [dict(_load("context-no-upstream.json")["produces"][0], promote=True)],
         "context-promote.json: its produced dataset is not the one of context-no-upstream.json with promote true")
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
