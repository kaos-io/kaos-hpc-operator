#!/usr/bin/env bash
# Check 5 of the step contract: when the HPC job definition is changed in place (fields removed, the
# contract's fields added), jobs stored before the change stay readable and can still be ended, released
# and deleted. See docs/step-contract.md and README.md here.
set -uo pipefail
out="$("$(dirname "$0")/envtest.sh" TestStoredJobsStayReadableWhenFieldsAreRemoved 2>&1)"
printf '%s\n' "$out"
if printf '%s\n' "$out" | grep -q '^--- PASS'; then
  echo "PASS  5 stored jobs stay readable when fields are removed"
else
  echo "FAIL  5 stored jobs stay readable when fields are removed"
  exit 1
fi
