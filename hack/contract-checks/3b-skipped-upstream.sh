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

logs="$(mktemp -d)"
for step in evaluate others plain; do step_log "$wf" "$step" > "$logs/$step" 2>&1; done
python3 - "$logs" <<'PY' && pass "3(b) a skipped upstream yields the sentinel" || fail "3(b) a skipped upstream yields the sentinel"
import pathlib, sys
import checklib as c
logs = pathlib.Path(sys.argv[1])
base = c.base_of(c.fixture("context-skipped-upstream.json"))
ok = c.same("ran + skipped (when is false): context-skipped-upstream", (logs / "evaluate").read_text(),
            c.wire.dumps(c.fixture("context-skipped-upstream.json")))
ok = c.same("omitted + ran without a result", (logs / "others").read_text(),
            c.wire.dumps(dict(base, upstream={"after": c.wire.ABSENT, "silent": c.wire.ABSENT}))) and ok
# Why the default is in the expression: a plain reference to a skipped step's output is the empty string.
ok = c.same("a plain reference to the skipped step's output, no default: the empty string",
            (logs / "plain").read_text(), "") and ok
sys.exit(0 if ok else 1)
PY
