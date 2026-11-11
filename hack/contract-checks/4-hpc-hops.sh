#!/usr/bin/env bash
# Check 4 of the step contract: a step context at the cap (96 KiB) travels follower, job object, runner
# Job, runner process, Slurm submission and job script, and reaches the step unchanged. A control run
# changes one character on the way and must be seen. See docs/step-contract.md and README.md here for
# what this cannot prove without a live site.
set -uo pipefail
out="$("$(dirname "$0")/envtest.sh" TestA96KiBContextSurvivesEveryHop 2>&1)"
printf '%s\n' "$out"
if printf '%s\n' "$out" | grep -q '^--- PASS'; then
  echo "PASS  4 a 96 KiB value survives every HPC hop"
else
  echo "FAIL  4 a 96 KiB value survives every HPC hop"
  exit 1
fi
