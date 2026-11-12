#!/usr/bin/env bash
# Runs one Go check of hack/contract-checks/hpc against a real API server (envtest: kube-apiserver and
# etcd, no cluster). Usage: envtest.sh TestName
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
make -s setup-envtest >/dev/null
version="$(go list -m -f '{{ .Version }}' k8s.io/api | awk -F'[v.]' '{printf "1.%d", $3}')"
assets="$("$ROOT/bin/setup-envtest" use "$version" --bin-dir "$ROOT/bin" -p path)"
echo "API server: Kubernetes $(basename "$assets" | cut -d- -f1) (envtest)"
KUBEBUILDER_ASSETS="$assets" go test -tags contractcheck ./hack/contract-checks/hpc/ -run "^$1\$" -count=1 -v \
  | grep -E '^\s+[a-z_]+\.go:[0-9]+: |^(--- |ok|FAIL|PASS|panic)' | sed -E 's/^\s+[a-z_]+\.go:[0-9]+: //'
