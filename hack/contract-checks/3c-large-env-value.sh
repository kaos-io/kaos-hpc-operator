#!/usr/bin/env bash
# Check 3(c) of the step contract: a step context at the cap (96 KiB) reaches the container intact in
# KAOS_STEP_CONTEXT; a value over the kernel's limit for one environment string never reaches a step
# shortened. See docs/step-contract.md.
source "$(dirname "$0")/lib.sh"
need_lab
cd "$HERE"

# The cap itself: must arrive byte for byte.
wf="$(run_workflow "$HERE/argo/3c-large-env-value.yaml" -p fill="$(python3 -c "import checklib as c; print(c.large_fill($CONTEXT_CAP))")")"
say "at the cap ($CONTEXT_CAP bytes): workflow $wf $(wf_phase "$wf") $(wf_message "$wf")"
[ "$(wf_phase "$wf")" = Succeeded ] || fail "3(c) the workflow with a context at the cap did not succeed"
log="$(mktemp)"; step_log "$wf" step > "$log" 2>&1
python3 -c "
import sys, checklib as c
sys.exit(0 if c.same('KAOS_STEP_CONTEXT at the cap', open('$log').read(), c.large_context($CONTEXT_CAP)) else 1)" \
  || fail "3(c) the context at the cap did not arrive intact"
say "      the Workflow object holding it: $(kc -n "$CHECK_NAMESPACE" get workflow "$wf" -o json | wc -c) bytes of JSON"

# Over the kernel's limit for one environment string (131072 bytes with its name): the step must not run
# with a shortened value. Either it arrives whole or the step does not start.
over=140000
wf="$(run_workflow "$HERE/argo/3c-large-env-value.yaml" -p fill="$(python3 -c "import checklib as c; print(c.large_fill($over))")")"
phase="$(wf_phase "$wf")"
say "over the kernel limit ($over bytes): workflow $wf $phase"
node="$(kc -n "$CHECK_NAMESPACE" get workflow "$wf" -o json | python3 -c '
import json, sys
for n in json.load(sys.stdin)["status"]["nodes"].values():
    if n.get("displayName") == "step":
        print("%s: %s" % (n.get("phase"), (n.get("message") or "")[:300]))')"
say "      the step: ${node:-did not start}"
if [ "$phase" = Succeeded ]; then
  step_log "$wf" step > "$log" 2>&1
  python3 -c "
import sys, checklib as c
sys.exit(0 if c.same('KAOS_STEP_CONTEXT over the kernel limit', open('$log').read(), c.large_context($over)) else 1)" \
    || fail "3(c) a context over the kernel limit reached the step changed"
else
  case "$node" in
    Succeeded*) fail "3(c) the step ran although the workflow did not succeed" ;;
    *) pass "a context over the kernel limit fails the step before it runs; it is never shortened" ;;
  esac
fi

# What the ceiling counts. Two contexts of the same 98304 bytes: one holds 15000 short strings, the other
# 17000. Inside the step's definition the context is a JSON string, where every quote costs two bytes, so
# the first costs 128368 bytes and the second 132368. The first must arrive intact; the second must not start.
for items in 15000 17000; do
  cost="$(python3 -c "import checklib as c; print(c.as_json_string(c.large_context($CONTEXT_CAP, $items)))")"
  wf="$(run_workflow "$HERE/argo/3c-large-env-value.yaml" -p items=$items \
    -p fill="$(python3 -c "import checklib as c; print(c.large_fill($CONTEXT_CAP, $items))")")"
  phase="$(wf_phase "$wf")"
  say "$CONTEXT_CAP bytes that cost $cost as a JSON string: workflow $wf $phase"
  if [ "$cost" -le 130000 ]; then
    [ "$phase" = Succeeded ] || fail "3(c) a context that costs $cost did not start"
    step_log "$wf" step > "$log" 2>&1
    python3 -c "
import sys, checklib as c
sys.exit(0 if c.same('KAOS_STEP_CONTEXT, $CONTEXT_CAP bytes, cost $cost', open('$log').read(), c.large_context($CONTEXT_CAP, $items)) else 1)" \
      || fail "3(c) a context that costs $cost did not arrive intact"
  else
    [ "$phase" = Succeeded ] && fail "3(c) a context that costs $cost started: the ceiling does not count the JSON string"
    pass "a context of $CONTEXT_CAP bytes that costs $cost as a JSON string does not start: the ceiling counts the cost, not the bytes"
  fi
done

# The guarantee of section 7. A step whose own definition is at the template budget (24 KiB), whose base
# context is at its cap (96 KiB less 8 KiB per needed step) and whose needed steps each hand back a result
# at the result cap must start and receive its context intact: with three needed steps, and with ten, the
# most a step may name. The same three-step case with results of 32 KiB, the cap before, must not start.
template_of() {  # the step's definition as the engine hands it to the pod
  kc -n "$CHECK_NAMESPACE" get pods -l "workflows.argoproj.io/workflow=$1" -o json | python3 -c '
import json, sys
print(max([len(e["value"]) for p in json.load(sys.stdin)["items"] for c in p["spec"].get("initContainers", [])
           for e in c.get("env", []) if e["name"] == "ARGO_TEMPLATE"] or [0]))'
}
for case in "3 8192 start" "10 8192 start" "3 32768 refuse"; do
  set -- $case; needs=$1; result=$2; expect=$3
  file="$(mktemp --suffix=.yaml)"
  python3 -c "import checklib as c; open('$file', 'w').write(c.needs_workflow($needs, 24200))"
  read -r fill result_fill cost <<<"$(python3 -c "
import checklib as c
f, rf, t = c.needs_case($needs, $result); print(f, rf, c.wire.cost(t))")"
  wf="$(run_workflow "$file" -p fill="$fill" -p result-fill="$result_fill")"
  phase="$(wf_phase "$wf")"
  say "a step that needs $needs steps, base at its cap, each result costing $result: the context costs $cost; workflow $wf $phase"
  if [ "$expect" = start ]; then
    [ "$phase" = Succeeded ] || fail "3(c) the step with $needs needed steps did not start: $(wf_message "$wf")"
    step_log "$wf" step > "$log" 2>&1
    python3 -c "
import sys, checklib as c
sys.exit(0 if c.same('the context of a step that needs $needs steps', open('$log').read(), c.needs_case($needs, $result)[2]) else 1)" \
      || fail "3(c) the context of the step with $needs needed steps did not arrive intact"
    whole="$(template_of "$wf")"
    rest=$((whole - cost))
    say "      the step's definition: $whole bytes, of which $rest are not the context (budget 24576); the ceiling is 131072"
    [ "$cost" -eq "$CONTEXT_CAP" ] && [ "$rest" -ge 24576 ] && [ "$whole" -le 131072 ] \
      || fail "3(c) the case is not at the caps: context cost $cost, rest of the definition $rest"
  else
    [ "$phase" = Succeeded ] && fail "3(c) a context that costs $cost started"
    pass "with results of $result the same step does not start: the result cap is what keeps it under the ceiling"
  fi
done
pass "3(c) a 96 KiB env value reaches the container intact when its cost as a JSON string fits"
