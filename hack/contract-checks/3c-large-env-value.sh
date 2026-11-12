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
# most a step may name. "mixed" cases fill the values with the characters that cost more than a byte in a
# step's definition (& < > " \ { }), in wire form. The cost is checked against what the engine really
# built: the step's definition minus the context's cost must be the same number in every case.
template_of() {  # the step's definition as the engine hands it to the pod: the length of ARGO_TEMPLATE
  kc -n "$CHECK_NAMESPACE" get pods -l "workflows.argoproj.io/workflow=$1" -o json | python3 -c '
import json, sys
print(max([len(e["value"].encode()) for p in json.load(sys.stdin)["items"] for c in p["spec"].get("initContainers", [])
           for e in c.get("env", []) if e["name"] == "ARGO_TEMPLATE"] or [0]))'
}
step_message() {
  kc -n "$CHECK_NAMESPACE" get workflow "$1" -o json | python3 -c '
import json, sys
for n in json.load(sys.stdin)["status"]["nodes"].values():
    if n.get("displayName") == "step":
        print("%s: %s" % (n.get("phase"), (n.get("message") or "")[:160]))'
}
# needs_run LABEL NEEDS RESULT-COST UNITS RESULT-UNITS AMPS [CONTEXT-COST]: runs the case; sets wf, phase, cost
needs_run() {
  local file; file="$(mktemp --suffix=.yaml)"
  python3 -c "import checklib as c; open('$file', 'w').write(c.needs_workflow($2, 24200))"
  read -r fill result_fill cost <<<"$(python3 -c "
import checklib as c
f, rf, t = c.needs_case($2, $3, $4, $5, $6, ${7:-None}); print(f, rf, c.wire.cost(t))")"
  wf="$(run_workflow "$file" -p fill="$fill" -p units="$4" -p amps="$6" -p result-fill="$result_fill" -p result-units="$5")"
  phase="$(wf_phase "$wf")"
  say "$1: the context costs $cost; workflow $wf $phase"
}
needs_intact() {  # the step must have received exactly the case's context
  [ "$phase" = Succeeded ] || fail "3(c) $1: the step did not start: $(step_message "$wf")"
  step_log "$wf" step > "$log" 2>&1
  python3 -c "
import sys, checklib as c
sys.exit(0 if c.same('$1', open('$log').read(), c.needs_case($2, $3, $4, $5, $6, ${7:-None})[2]) else 1)" \
    || fail "3(c) $1: the context did not arrive intact"
}
rest_seen=""
for case in "3 0 0" "3 200 40" "10 200 40"; do
  set -- $case
  label="needs $1 steps, base and results at their caps, $([ "$2" = 0 ] && echo "plain filler" || echo "mixed filler")"
  needs_run "$label" "$1" 8192 "$2" "$3" 0
  needs_intact "$label" "$1" 8192 "$2" "$3" 0
  whole="$(template_of "$wf")"; rest=$((whole - cost))
  say "      the step's definition: $whole bytes, of which $rest are not the context (budget 24576)"
  [ "$cost" -eq "$CONTEXT_CAP" ] && [ "$rest" -ge 24576 ] || fail "3(c) the case is not at the caps: cost $cost, rest $rest"
  [ -z "$rest_seen" ] || [ "$rest" -eq "$rest_seen" ] \
    || fail "3(c) cost() does not count what the engine writes: the rest of the definition is $rest here and $rest_seen before"
  rest_seen="$rest"
done
pass "cost() counts the context exactly as the engine writes it: the rest of the definition is $rest_seen bytes in every case"

# What the engine adds to a definition when it starts the step: the definition it built, less the template
# as the Workflow holds it (written with the engine's encoder) with the context in place of its reference.
added="$(kc -n "$CHECK_NAMESPACE" get workflow "$wf" -o json | python3 -c "
import json, sys, checklib as c
t = [t for t in json.load(sys.stdin)['spec']['templates'] if t['name'] == 'consumer'][0]
print($rest_seen - (c.go_json_length(t) - len('{{inputs.parameters.kaos-context}}')))")"
say "      of those $rest_seen bytes the engine added $added; the rest is the template as rendered"

# The controls: what the rules prevent.
needs_run "the same three-step case with results of 32 KiB, the cap before" 3 32768 0 0 0
[ "$phase" = Succeeded ] && fail "3(c) a context that costs $cost started"
pass "it does not start: $(step_message "$wf")"
needs_run "the same three-step case with 2000 raw & in the base (not wire form: cost() counts each as one, the engine writes six)" 3 8192 0 0 2000
[ "$phase" = Succeeded ] && fail "3(c) a base with raw & started: the engine no longer writes & as six bytes"
pass "it does not start, which is why the wire form writes & < > as escapes: $(step_message "$wf")"

# The ceiling itself. The kernel takes ARGO_TEMPLATE=<definition> and its terminator in 131072 bytes, so the
# definition may be 131057. One byte more and the init container cannot start; from 131073 the engine tries
# a ConfigMap instead.
for whole in 131057 131058 131073; do
  needs_run "a definition of $whole bytes" 3 8192 200 40 0 $((whole - rest_seen))
  if [ "$whole" -le 131057 ]; then
    needs_intact "a definition of $whole bytes" 3 8192 200 40 0 $((whole - rest_seen))
    [ "$(template_of "$wf")" -eq "$whole" ] || fail "3(c) the definition is $(template_of "$wf") bytes, not $whole"
  else
    [ "$phase" = Succeeded ] && fail "3(c) a definition of $whole bytes started"
    pass "a definition of $whole bytes does not start: $(step_message "$wf")"
  fi
done
pass "3(c) a 96 KiB env value reaches the container intact when its cost as a JSON string fits"
