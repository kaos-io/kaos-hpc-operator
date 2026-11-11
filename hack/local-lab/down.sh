#!/usr/bin/env bash
# Tears the local lab down: the kind cluster, the lake container and its volume, local state.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
kind delete cluster --name "$LAB_NAME"
docker rm -f "$LAKE_CONTAINER" >/dev/null 2>&1 || true
docker volume rm "$LAKE_VOLUME" >/dev/null 2>&1 || true
docker rmi "$PROBE_IMAGE" "$SLURM_IMAGE" >/dev/null 2>&1 || true
rm -rf "$STATE"
echo "lab $LAB_NAME is down"
