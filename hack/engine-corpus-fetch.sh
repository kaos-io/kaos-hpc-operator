#!/usr/bin/env bash
# Clone (or update) the repos the engine render corpus needs into $1 (engine/tools/corpus.py --corpus $1):
# the two public pipeline templates and the private apps listed in $KAOS_ENGINE_CORPUS_LAB
# (comma-separated `org/repo:profile`, supplied privately, never committed). Full history: the corpus
# walks every historical state.
set -euo pipefail
dir="${1:?usage: engine-corpus-fetch.sh DIR}"
mkdir -p "$dir"
fetch() { # org/repo -> dir/name
  local name="${1##*/}"
  if [ -d "$dir/$name/.git" ]; then git -C "$dir/$name" fetch -q --all --prune; else git clone -q "https://github.com/$1" "$dir/$name"; fi
}
fetch novelcore/hera-pipeline-template
fetch novelcore/yolo-pipelines-template
IFS=',' read -r -a labs <<< "${KAOS_ENGINE_CORPUS_LAB:-}"
for entry in "${labs[@]}"; do
  [ -n "$entry" ] && fetch "${entry%:*}"
done
