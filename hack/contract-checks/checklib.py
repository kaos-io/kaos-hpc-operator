"""Helpers of the check scripts: what a context should be, and what a step printed."""
import base64
import hashlib
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import wire  # noqa: E402


def fixture(name):
    return json.loads((wire.FIXTURES / name).read_text())


def base_of(context):
    """The base context: every field but `upstream`, which the workflow adds."""
    return {k: v for k, v in context.items() if k != "upstream"}


def b64(value):
    return base64.b64encode(wire.dumps(value).encode()).decode()


def printed(log):
    """BYTES, SHA256 and VALUE as a step printed them."""
    out = {}
    for key in ("BYTES", "SHA256", "VALUE"):
        m = re.search(r"^%s=(.*)$" % key, log, re.M)
        out[key] = m.group(1) if m else None
    return out


def same(label, log, expected_text):
    """True when the step received exactly expected_text; prints one PASS or FAIL line."""
    got = printed(log)
    want_sha = hashlib.sha256(expected_text.encode()).hexdigest()
    ok = got["SHA256"] == want_sha and got["BYTES"] == str(len(expected_text.encode()))
    print("%s  %s: %s bytes, sha256 %s%s" % ("PASS" if ok else "FAIL", label, got["BYTES"], got["SHA256"],
                                              "" if ok else " (expected %d bytes, sha256 %s)" % (len(expected_text), want_sha)))
    if not ok:
        print("      got:      %s" % (got["VALUE"] or "")[:600])
        print("      expected: %s" % expected_text[:600])
    return ok


# ---- check 3(c): a context of an exact size, built the same way by the workflow's compose step

LARGE_HEAD = ('{"version":1,"run":"contract-3c","app":"hera-demo","project":"vision","step":"train","kind":"train",'
              '"experiment":"baseline","params":{"list":[')
LARGE_MID = '""],"blob":"'
LARGE_TAIL = ('"},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{"prepare":'
              '{"version":1,"status":"ok","values":{"rows":1200},"reads":[],"writes":[]}}}')


def chain(fill, seed="contract-3c"):
    """`fill` characters of a sha256 chain: each link is the hex digest of the one before."""
    out, h = [], seed
    while 64 * len(out) < fill:
        h = hashlib.sha256(h.encode()).hexdigest()
        out.append(h)
    return "".join(out)[:fill]


def large_fill(size, items=0):
    """How many filler characters make the whole context exactly `size` bytes, with `items` short strings in it."""
    return size - len(LARGE_HEAD) - 4 * items - len(LARGE_MID) - len(LARGE_TAIL)


def large_context(size, items=0):
    text = LARGE_HEAD + '"x",' * items + LARGE_MID + chain(large_fill(size, items)) + LARGE_TAIL
    assert len(text) == size and json.loads(text)["version"] == 1
    return text


def as_json_string(text):
    """The length of `text` inside a JSON string: what it costs in the step's definition (section 7)."""
    return len(json.dumps(text)) - 2


# ---- check 3(c), the guarantee: a step that needs N steps, its base at its cap, every result at the cap

NEEDS_HEAD = ('{"version":1,"run":"contract-3c","app":"hera-demo","project":"vision","step":"train","kind":"train",'
              '"experiment":"baseline","params":{"blob":"')
NEEDS_TAIL = '"},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[]}'
RESULT_HEAD = '{"version":1,"status":"ok","values":{"blob":"'
RESULT_TAIL = '"},"reads":[],"writes":[]}'


def result_of_cost(cost):
    """A result in wire form that costs exactly `cost`."""
    fill = cost - wire.cost(RESULT_HEAD + RESULT_TAIL)
    text = RESULT_HEAD + chain(fill, "contract-3c-result") + RESULT_TAIL
    assert wire.cost(text) == cost and json.loads(text)["status"] == "ok"
    return text


def needs_case(needs, result_cost):
    """A step that names `needs` steps (u1..uN). Its context without results costs exactly its base cap,
    counted as compose counts it: the context with every result still empty. Returns the base's filler
    length, the result's filler length and the context the step must receive."""
    result = result_of_cost(result_cost)

    def context(fill, results):
        up = ",".join('"u%d":%s' % (i + 1, r) for i, r in enumerate(results))
        return NEEDS_HEAD + chain(fill) + NEEDS_TAIL[:-1] + ',"upstream":{' + up + "}}"

    empty = wire.cost(context(0, [""] * needs))
    fill = wire.base_cap(needs) - empty
    assert wire.cost(context(fill, [""] * needs)) == wire.base_cap(needs)
    text = context(fill, [result] * needs)
    assert json.loads(text)["upstream"]["u%d" % needs]["status"] == "ok"
    return fill, len(result) - len(RESULT_HEAD) - len(RESULT_TAIL), text


def needs_workflow(needs, pad):
    """The Workflow of such a step. `pad` characters in an env value bring the step's own definition up to
    the template budget, so the case is as hard as the contract allows."""
    absent = r'"{\"version\":1,\"status\":\"absent\"}"'
    joined = "".join(' + "%s\\"u%d\\":" + sprig.default(%s, tasks.u%d.outputs.parameters["kaos-result"])'
                     % ("," if i else "", i + 1, absent, i + 1) for i in range(needs))
    expr = ('{{=sprig.trimSuffix("}", tasks.compose.outputs.parameters["kaos-context-train"]) + ",\\"upstream\\":{"'
            + joined + ' + "}" + "}"}}')
    tasks = "".join('''
          - name: u%d
            template: producer
            depends: compose''' % (i + 1) for i in range(needs))
    depends = " && ".join(["compose"] + ["u%d" % (i + 1) for i in range(needs)])
    return '''apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: contract-3c-needs-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: fill
      - name: result-fill
  templates:
    - name: compose
      container:
        image: alpine:3.20
        command: [sh, -c]
        args:
          - |
            set -e
            mkdir -p /work
            fill={{workflow.parameters.fill}}
            h=contract-3c; n=0; : > /work/blob
            while [ $n -lt $fill ]; do
              h=$(printf %%s "$h" | sha256sum | cut -c1-64); printf %%s "$h" >> /work/blob; n=$((n+64))
            done
            { printf %%s '%s'; head -c $fill /work/blob; printf %%s '%s'; } > /work/context.json
      outputs:
        parameters:
          - name: kaos-context-train
            valueFrom: {path: /work/context.json}
    - name: producer
      container:
        image: alpine:3.20
        command: [sh, -c]
        args:
          - |
            set -e
            mkdir -p /work/output
            fill={{workflow.parameters.result-fill}}
            h=contract-3c-result; n=0; : > /work/blob
            while [ $n -lt $fill ]; do
              h=$(printf %%s "$h" | sha256sum | cut -c1-64); printf %%s "$h" >> /work/blob; n=$((n+64))
            done
            { printf %%s '%s'; head -c $fill /work/blob; printf %%s '%s'; } > /work/output/kaos-result.json
      outputs:
        parameters:
          - name: kaos-result
            valueFrom:
              path: /work/output/kaos-result.json
              default: '{"version":1,"status":"absent"}'
    - name: consumer
      inputs:
        parameters:
          - name: kaos-context
      container:
        image: alpine:3.20
        command: [sh, -c]
        args: ['printf "BYTES=%%s\\n" "$(printf %%s "$KAOS_STEP_CONTEXT" | wc -c)"; printf "SHA256=%%s\\n" "$(printf %%s "$KAOS_STEP_CONTEXT" | sha256sum | cut -c1-64)"']
        env:
          - name: KAOS_STEP_CONTEXT
            value: "{{inputs.parameters.kaos-context}}"
          - name: REST_OF_THE_STEP
            value: "%s"
    - name: main
      dag:
        tasks:
          - name: compose
            template: compose%s
          - name: step
            template: consumer
            depends: "%s"
            arguments:
              parameters:
                - name: kaos-context
                  value: %s
''' % (NEEDS_HEAD, NEEDS_TAIL, RESULT_HEAD, RESULT_TAIL, "p" * pad, tasks, depends, json.dumps(expr))
