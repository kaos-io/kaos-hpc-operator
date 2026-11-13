#!/usr/bin/env bash
# Two platform projects as two different Unix users on one shared scratch (PRD kaos#1911). See two_users.py.
#   hack/local-lab/two-users.sh                       # job side only
#   STAGER=/path/to/stager.py hack/local-lab/two-users.sh   # also the staging tool: image staging, usage, clean
# Runs in a throw-away rootless container (podman, else docker) that is given no network; needs python:3.12-slim locally. The repository is mounted read-only and copied in.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CTR="${CONTAINER:-$(command -v podman || command -v docker)}"
IMAGE="${TWO_USERS_IMAGE:-docker.io/library/python:3.12-slim}"
mounts=(-v "$REPO:/src:ro")
env=()
if [ -n "${STAGER:-}" ]; then mounts+=(-v "$(cd "$(dirname "$STAGER")" && pwd)/$(basename "$STAGER"):/stager/stager.py:ro"); env=(-e STAGER=/stager/stager.py); fi
exec "$CTR" run --rm --network none --security-opt label=disable "${mounts[@]}" "${env[@]}" -e PYTHONDONTWRITEBYTECODE=1 "$IMAGE" sh -c 'cp -r /src /repo && chmod 755 /repo/runner/tests/fakeslurm.py && exec python3 /repo/hack/local-lab/two_users.py'
