#!/usr/bin/env bash
# Check 3(d) of the step contract: Kubernetes changes `$$` and `$(NAME)` in a container's env value; the wire
# form writes `$` as $, so a context in wire form arrives byte for byte. See docs/step-contract.md, section 6.
#
# Runs one pod in its own namespace (w-dollar) of an existing cluster and deletes the namespace afterwards. It
# creates no cluster. Name the cluster's kubeconfig in KAOS_LAB_KUBECONFIG (or KUBECONFIG); the pod's image is
# IMAGE (default busybox, kaos-lab-probe:prd1911 when the lab has no network).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HERE="$ROOT/hack/contract-checks"
export KUBECONFIG="${KAOS_LAB_KUBECONFIG:-${KUBECONFIG:-}}"
[ -n "$KUBECONFIG" ] || { echo "FAIL  set KAOS_LAB_KUBECONFIG to the kubeconfig of a local cluster"; exit 1; }
IMAGE="${IMAGE:-busybox}"
NS=w-dollar

kubectl get ns "$NS" >/dev/null 2>&1 && { echo "FAIL  namespace $NS exists already; this check owns it, delete it first"; exit 1; }
trap 'kubectl delete ns "$NS" --wait=false >/dev/null 2>&1 || true' EXIT

TEXT='a $$ b $(HPC_JOB_NAME) c'
ESC="$(TEXT="$TEXT" python3 -c 'import os,sys; sys.path.insert(0, sys.argv[1]); import wire
sys.stdout.write(wire.dumps({"p": os.environ["TEXT"]}))' "$HERE")"
case "$ESC" in *'$'*) echo "FAIL  the wire form holds a raw \$: $ESC"; exit 1;; esac

kubectl create ns "$NS" >/dev/null
IMAGE="$IMAGE" ESC="$ESC" python3 - "$HERE/argo/3d-env-expansion-pod.yaml" <<'PY' | kubectl apply -f - >/dev/null
import json, os, sys
text = open(sys.argv[1]).read().replace("IMAGE", os.environ["IMAGE"], 1)
print(text.replace("ESCAPED_VALUE", json.dumps(os.environ["ESC"])))
PY
for _ in $(seq 60); do
  phase="$(kubectl -n "$NS" get pod env-expansion -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$phase" in Succeeded|Failed) break;; esac
  sleep 2
done
[ "$phase" = Succeeded ] || { echo "FAIL  the pod ended $phase"; kubectl -n "$NS" describe pod env-expansion | tail -15; exit 1; }
out="$(kubectl -n "$NS" logs env-expansion)"
printf '%s\n' "$out"
raw="${out#RAW=}"; raw="${raw%%$'\n'ESCAPED=*}"
escaped="${out#*$'\n'ESCAPED=}"
[ "$raw" != "$TEXT" ] || { echo "FAIL  RAW arrived unchanged: this cluster does not expand env values"; exit 1; }
[ "$escaped" = "$ESC" ] || { echo "FAIL  ESCAPED arrived changed: $escaped"; exit 1; }
echo "PASS  3(d) RAW arrived changed ($raw), ESCAPED arrived byte for byte ($escaped)"
