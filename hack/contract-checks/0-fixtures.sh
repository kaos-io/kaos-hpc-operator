#!/usr/bin/env bash
# The fixtures of the step contract agree with the contract and with each other: every fixture equals
# its .wire file, stays under its cap, and the cross-references between them hold. Needs python3 only.
set -euo pipefail
exec python3 "$(dirname "$0")/wire.py" check-fixtures
