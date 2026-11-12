#!/usr/bin/env bash
# Runs every check of the step contract and prints one line per check at the end.
set -uo pipefail
cd "$(dirname "$0")"
summary=()
for check in 0-fixtures 3a-context-expression 3b-skipped-upstream 3c-large-env-value 4-hpc-hops 5-crd-upgrade; do
  echo "==== $check"
  if "./$check.sh"; then summary+=("PASS  $check"); else summary+=("FAIL  $check"); fi
done
echo "==== summary"
printf '%s\n' "${summary[@]}"
case "${summary[*]}" in *FAIL*) exit 1 ;; esac
