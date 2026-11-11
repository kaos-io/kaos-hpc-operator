#!/usr/bin/env bash
# Shared setup of the step-contract checks (docs/step-contract.md, section "Checks").
# Sourced by every check script. Nothing here touches a cluster other than the local kind cluster
# named by CONTRACT_CLUSTER.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HERE="$ROOT/hack/contract-checks"
BIN="$ROOT/bin"

# The Argo Workflows the workload clusters of dev run: the pool installs the argo-helm chart
# `argo-workflows` at 1.0.18 (kubecore-operator, internal/operators/kubepool/phases/syncing_tools.go,
# tools.argoWorkflows.version), whose appVersion is v4.0.6.
ARGO_CHART_VERSION="${ARGO_CHART_VERSION:-1.0.18}"
ARGO_VERSION="${ARGO_VERSION:-v4.0.6}"
ARGO_NAMESPACE="argo-workflows"

CONTRACT_CLUSTER="${CONTRACT_CLUSTER:-prd1911-contract}"
KUBECONFIG_FILE="$BIN/contract-checks.kubeconfig"
CHECK_NAMESPACE="contract-checks"

# The cap of the step context, in bytes (docs/step-contract.md, "Size caps").
CONTEXT_CAP=98304

export KUBECONFIG="$KUBECONFIG_FILE"
ARGO="$BIN/argo-$ARGO_VERSION"

say() { printf '%s\n' "$*"; }
pass() { printf 'PASS  %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; exit 1; }

kc() { kubectl --context "kind-$CONTRACT_CLUSTER" "$@"; }
argo() { "$ARGO" --context "kind-$CONTRACT_CLUSTER" -n "$CHECK_NAMESPACE" "$@"; }

# need_lab: the kind cluster with Argo Workflows and the matching argo CLI, created when missing.
need_lab() {
  mkdir -p "$BIN"
  if [ ! -x "$ARGO" ]; then
    say "lab: downloading the argo CLI $ARGO_VERSION into $BIN"
    curl -fsSL "https://github.com/argoproj/argo-workflows/releases/download/$ARGO_VERSION/argo-linux-amd64.gz" \
      | gunzip > "$ARGO.tmp"
    chmod +x "$ARGO.tmp" && mv "$ARGO.tmp" "$ARGO"
  fi
  if ! kind get clusters 2>/dev/null | grep -qx "$CONTRACT_CLUSTER"; then
    say "lab: creating the kind cluster $CONTRACT_CLUSTER (one node)"
    kind create cluster --name "$CONTRACT_CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 120s
  elif [ ! -s "$KUBECONFIG_FILE" ]; then
    kind get kubeconfig --name "$CONTRACT_CLUSTER" > "$KUBECONFIG_FILE"
  fi
  # Before the chart: it creates the controller's Role in the namespace it watches.
  kc get ns "$CHECK_NAMESPACE" >/dev/null 2>&1 || kc create ns "$CHECK_NAMESPACE" >/dev/null
  if ! kc -n "$ARGO_NAMESPACE" get deploy -l app.kubernetes.io/name=argo-workflows-workflow-controller \
      -o name 2>/dev/null | grep -q .; then
    say "lab: installing Argo Workflows $ARGO_VERSION (argo-helm chart $ARGO_CHART_VERSION, controller only)"
    helm --kube-context "kind-$CONTRACT_CLUSTER" upgrade --install argo-workflows argo-workflows \
      --repo https://argoproj.github.io/argo-helm --version "$ARGO_CHART_VERSION" \
      --namespace "$ARGO_NAMESPACE" --create-namespace \
      --set server.enabled=false \
      --set "controller.workflowNamespaces={$CHECK_NAMESPACE}" \
      --set controller.resources.requests.memory=128Mi \
      --wait --timeout 5m >/dev/null
  fi
  # The executor's own rights (it reports a step's outputs as a WorkflowTaskResult).
  kc -n "$CHECK_NAMESPACE" apply -f "$HERE/argo/executor-rbac.yaml" >/dev/null
  local image
  image="$(kc -n "$ARGO_NAMESPACE" get deploy -l app.kubernetes.io/name=argo-workflows-workflow-controller \
    -o jsonpath='{.items[0].spec.template.spec.containers[0].image}')"
  say "lab: cluster kind-$CONTRACT_CLUSTER, controller image $image, CLI $("$ARGO" version --short 2>/dev/null | head -1)"
  case "$image" in *":$ARGO_VERSION"*) ;; *) fail "the controller is not $ARGO_VERSION" ;; esac
}

# run_workflow FILE [argo submit args]: submits, waits, prints the Workflow's name. Never fails on
# the Workflow's own phase: the caller reads it.
run_workflow() {
  local file="$1"; shift
  local name
  name="$(argo submit "$file" "$@" -o name)"
  argo wait "$name" >/dev/null 2>&1 || true
  printf '%s\n' "$name"
}

wf_phase() { kc -n "$CHECK_NAMESPACE" get workflow "$1" -o jsonpath='{.status.phase}'; }
wf_message() { kc -n "$CHECK_NAMESPACE" get workflow "$1" -o jsonpath='{.status.message}'; }

# step_log WORKFLOW TEMPLATE-DISPLAY-NAME: the main container's log of one step.
step_log() {
  local pod
  pod="$(kc -n "$CHECK_NAMESPACE" get workflow "$1" -o json \
    | python3 -c 'import json,sys
wf=json.load(sys.stdin); want=sys.argv[1]
for n in wf["status"]["nodes"].values():
    if n.get("displayName")==want and n.get("type")=="Pod":
        print(n["id"]); break' "$2")"
  [ -n "$pod" ] || { say "no pod for step $2 of $1"; return 1; }
  kc -n "$CHECK_NAMESPACE" logs "$(pod_of "$1" "$pod")" -c main
}

# pod_of WORKFLOW NODE-ID: the pod's name (Argo names it <workflow>-<template>-<hash>).
pod_of() {
  kc -n "$CHECK_NAMESPACE" get pods -l "workflows.argoproj.io/workflow=$1" -o json \
    | python3 -c 'import json,sys
want=sys.argv[1]
for p in json.load(sys.stdin)["items"]:
    if p["metadata"].get("annotations",{}).get("workflows.argoproj.io/node-id")==want:
        print(p["metadata"]["name"]); break' "$2"
}
