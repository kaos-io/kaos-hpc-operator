#!/usr/bin/env bash
# Check 3(a) of the step contract: one expression assembles the step context as one JSON value from
# task outputs, as a DAG task argument and as a container env value. See docs/step-contract.md.
source "$(dirname "$0")/lib.sh"
need_lab

# run_case LABEL CONTEXT-FIXTURE: the workflow gets the fixture's base and result-ok.json as the upstream
# result; both consumers must receive the fixture's base with upstream.prepare = result-ok.json.
run_case() {
  local label="$1" fixture="$2" wf logs
  wf="$(run_workflow "$HERE/argo/3a-context-expression.yaml" \
    -p base-b64="$(python3 -c 'import sys,checklib as c; print(c.b64(c.base_of(c.fixture(sys.argv[1]))))' "$fixture")" \
    -p result-b64="$(python3 -c 'import checklib as c; print(c.b64(c.fixture("result-ok.json")))')")"
  say "$label: workflow $wf $(wf_phase "$wf") $(wf_message "$wf")"
  [ "$(wf_phase "$wf")" = Succeeded ] || return 1
  logs="$(mktemp -d)"
  step_log "$wf" argument > "$logs/argument" 2>&1
  step_log "$wf" env > "$logs/env" 2>&1
  python3 - "$fixture" "$label" "$logs" <<'PY'
import pathlib, sys
import checklib as c
fixture, label, logs = sys.argv[1:]
want = dict(c.base_of(c.fixture(fixture)), upstream={"prepare": c.fixture("result-ok.json")})
text = c.wire.dumps(want)
ok = c.same(label + ", assembled in the DAG task argument", pathlib.Path(logs, "argument").read_text(), text)
ok = c.same(label + ", assembled in the container env value", pathlib.Path(logs, "env").read_text(), text) and ok
sys.exit(0 if ok else 1)
PY
}

cd "$HERE"
rc=0
run_case "context-one-upstream" context-one-upstream.json || rc=1
# Strings that hold {{...}}, quotes, a backslash, non-ASCII and numbers a float would change.
run_case "context-escapes" context-escapes.json || rc=1

# Why the wire form escapes braces inside strings: the same base written as ordinary JSON (a string
# holds {{=1+1}} and {{workflow.name}}) is evaluated by the workflow engine on its way to the step.
wf="$(run_workflow "$HERE/argo/3a-context-expression.yaml" \
  -p base-b64="$(python3 -c 'import base64,json,checklib as c
print(base64.b64encode(json.dumps(c.base_of(c.fixture("context-escapes.json")), separators=(",", ":")).encode()).decode())')" \
  -p result-b64="$(python3 -c 'import checklib as c; print(c.b64(c.fixture("result-ok.json")))')")"
if [ "$(wf_phase "$wf")" = Succeeded ]; then
  got="$(step_log "$wf" argument 2>&1 | sed -n 's/^VALUE=//p')"
  case "$got" in
    *'{{=1+1}}'*) say "NOTE  without the escapes the value arrived unchanged: this engine version does not evaluate {{ in values" ;;
    *) say "NOTE  without the escapes the engine changed the value (workflow $wf): $(printf %s "$got" | grep -o '"experiment":"[^"]*"') and $(printf %s "$got" | grep -o '"note":"braces [^ ]* ')" ;;
  esac
else
  say "NOTE  without the escapes the workflow $wf ended $(wf_phase "$wf"): $(wf_message "$wf")"
fi

[ $rc = 0 ] && pass "3(a) the context expression" || fail "3(a) the context expression"
