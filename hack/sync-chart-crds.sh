#!/usr/bin/env bash
# Copies the generated CRD schemas (config/crd/bases) into the chart templates, keeping each chart
# file's Helm header and footer. Run after `make manifests`; hack/chartsync fails when they differ.
set -euo pipefail
cd "$(dirname "$0")/.."
for g in config/crd/bases/*.yaml; do
  c="dist/chart/templates/crd/$(basename "$g")"
  head=$(awk '/^spec:$/{exit} {print}' "$c")
  { printf '%s\nspec:\n' "$head"; awk 'f{print} /^spec:$/{f=1}' "$g"; printf '{{- end -}}\n'; } >"$c.new"
  mv "$c.new" "$c"
done
