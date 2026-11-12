#!/usr/bin/env bash
# Check 3(b) of the step contract: inside the context expression, the result of an upstream step that
# was skipped, was omitted, or ran without writing a result is the sentinel {"version":1,"status":"absent"}.
# See docs/step-contract.md.
source "$(dirname "$0")/lib.sh"
need_lab
cd "$HERE"

wf="$(run_workflow "$HERE/argo/3b-skipped-upstream.yaml" \
  -p base-b64="$(python3 -c 'import checklib as c; print(c.b64(c.base_of(c.fixture("context-skipped-upstream.json"))))')" \
  -p result-b64="$(python3 -c 'import checklib as c; print(c.b64(c.fixture("result-model.json")))')")"
say "workflow $wf $(wf_phase "$wf") $(wf_message "$wf")"
argo get "$wf" | sed 's/\x1b\[[0-9;]*m//g' | sed -n '/^STEP/,$p'
[ "$(wf_phase "$wf")" = Succeeded ] || fail "3(b) the workflow did not succeed"

# The rule about depends: a task whose expression reads a task that is not in its depends never starts.
dep="$(argo submit "$HERE/argo/3b-not-in-depends.yaml" -o name)"
dep_deadline=$((SECONDS + 60))

logs="$(mktemp -d)"
for step in evaluate others pairs plain; do step_log "$wf" "$step" > "$logs/$step" 2>&1; done
python3 - "$logs" <<'PY' && pass "3(b) a skipped upstream yields the sentinel" || fail "3(b) a skipped upstream yields the sentinel"
import pathlib, sys
import checklib as c
logs = pathlib.Path(sys.argv[1])
base = c.base_of(c.fixture("context-skipped-upstream.json"))
ok = c.same("ran + skipped (when is false): context-skipped-upstream", (logs / "evaluate").read_text(),
            c.wire.dumps(c.fixture("context-skipped-upstream.json")))
ok = c.same("omitted + ran without a result", (logs / "others").read_text(),
            c.wire.dumps(dict(base, upstream={"after": c.wire.ABSENT, "silent": c.wire.ABSENT}))) and ok
ok = c.same("steps with an HPC variant: the cluster one ran, the HPC one ran, neither ran", (logs / "pairs").read_text(),
            c.wire.dumps(dict(base, upstream={"fine-tune": c.fixture("result-model.json"),
                                              "re-train": c.fixture("result-model.json"), "dry-run": c.wire.ABSENT}))) and ok
# Why the default is in the expression: a plain reference to a skipped step's output is the empty string.
ok = c.same("a plain reference to the skipped step's output, no default: the empty string",
            (logs / "plain").read_text(), "") and ok
sys.exit(0 if ok else 1)
PY
while [ $SECONDS -lt $dep_deadline ]; do sleep 5; done
dep_state="$(kc -n "$CHECK_NAMESPACE" get workflow "$dep" -o json | python3 -c '
import json, sys
wf = json.load(sys.stdin)
names = sorted(n.get("displayName") for n in wf["status"].get("nodes", {}).values() if n.get("type") == "Pod")
print(wf["status"].get("phase"), ",".join(names))')"
argo delete "$dep" >/dev/null 2>&1 || true
[ "$dep_state" = "Running a,b" ] \
  && pass "a step whose expression reads a task that is not in its depends never starts: after 60 s the run is still Running, a and b are done, and there is no error" \
  || fail "3(b) the step that reads a task outside its depends: expected 'Running a,b', got '$dep_state'"
