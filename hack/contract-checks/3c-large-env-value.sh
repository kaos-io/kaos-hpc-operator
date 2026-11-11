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
pass "3(c) a 96 KiB env value reaches the container intact"
