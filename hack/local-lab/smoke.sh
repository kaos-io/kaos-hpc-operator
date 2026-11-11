#!/usr/bin/env bash
# Submits a two-step workflow to the lab. Step one writes files to the lake and commits them;
# step two reads them back from that commit and prints checksums. Exits non-zero unless the
# checksums match (and the fake Slurm site answers).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RUN=smoke-$(date +%s)
# shellcheck disable=SC1091
source "$STATE/lake-app.env"

# the fake site answers with its token and CA (what the engine will rely on)
k -n "$LAB_NS" delete pod slurm-ping --ignore-not-found >/dev/null
k -n "$LAB_NS" run slurm-ping --image="$PROBE_IMAGE" --image-pull-policy=Never --restart=Never --command -- \
  sh -c "curl -sf -k -H 'X-SLURM-USER-TOKEN: lab-token' https://fake-slurm.$SLURM_NS.svc/slurm/v0.0.44/ping | jq -e '.' >/dev/null" >/dev/null
k -n "$LAB_NS" wait pod/slurm-ping --for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null \
  || { echo "FAIL: fake Slurm site did not answer ping" >&2; exit 1; }
k -n "$LAB_NS" delete pod slurm-ping >/dev/null
echo "fake Slurm site answers ping"

cat > "$STATE/smoke-wf.yaml" <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: $RUN
  namespace: $LAB_NS
spec:
  serviceAccountName: lab
  entrypoint: main
  templates:
  - name: main
    steps:
    - - {name: write, template: write}
    - - name: read
        template: read
        arguments: {parameters: [{name: commit, value: "{{steps.write.outputs.parameters.commit}}"}]}
  - name: write
    container:
      image: $PROBE_IMAGE
      imagePullPolicy: Never
      envFrom: [{secretRef: {name: lake-app}}]
      command: [sh, -ec]
      args:
      - |
        mkdir -p /work && cd /work
        for n in one two three; do echo "payload \$n $RUN" > \$n.txt; lake put $OUT_REPO main $RUN/\$n.txt \$n.txt; done
        lake commit $OUT_REPO main "$RUN" | tee /tmp/commit
        probe /work | tee /tmp/step1.log
        grep -E '^[0-9a-f]{64}  ' /tmp/step1.log | sort -k2 > /tmp/sums
    outputs:
      parameters:
      - {name: commit, valueFrom: {path: /tmp/commit}}
      - {name: sums, valueFrom: {path: /tmp/sums}}
  - name: read
    inputs: {parameters: [{name: commit}]}
    container:
      image: $PROBE_IMAGE
      imagePullPolicy: Never
      envFrom: [{secretRef: {name: lake-app}}]
      command: [sh, -ec]
      args:
      - |
        lake sync $OUT_REPO {{inputs.parameters.commit}} $RUN/ /in/outputs
        lake sync $DATA_REPO main "" /in/dataset
        probe /in/outputs /in/dataset | tee /tmp/step2.log
        # only the outputs folder is compared with step one
        (cd /in/outputs && find . -type f | sort | while read -r f; do printf '%s  %s\n' "\$(sha256sum "\$f" | cut -d' ' -f1)" "\$f"; done) > /tmp/sums
    outputs:
      parameters:
      - {name: sums, valueFrom: {path: /tmp/sums}}
YAML

"$BIN/argo" --context "$CTX" -n "$LAB_NS" submit "$STATE/smoke-wf.yaml" --wait --log >/dev/null 2>"$STATE/smoke.err" || true
"$BIN/argo" --context "$CTX" -n "$LAB_NS" logs "$RUN" 2>&1 | sed 's/^/  /'
phase=$(k -n "$LAB_NS" get wf "$RUN" -o jsonpath='{.status.phase}')
echo "workflow $RUN: $phase"
[ "$phase" = Succeeded ] || { echo "FAIL: workflow did not succeed" >&2; cat "$STATE/smoke.err" >&2; exit 1; }
param() { k -n "$LAB_NS" get wf "$RUN" -o json | jq -r --arg n "$1" '.status.nodes[]|select(.displayName==$n)|.outputs.parameters[]?|select(.name=="sums")|.value'; }
w=$(param write); r=$(param read)
echo "--- step one checksums"; echo "$w"; echo "--- step two checksums"; echo "$r"
# expected, computed on the host, independent of both steps
exp=$(for n in one two three; do printf 'payload %s %s\n' "$n" "$RUN" | sha256sum | cut -d' ' -f1 | sed "s|\$|  ./$n.txt|"; done | sort -k2)
[ "$w" = "$exp" ] || { echo "FAIL: step one checksums differ from the expected ones" >&2; exit 1; }
[ "$r" = "$exp" ] || { echo "FAIL: step two checksums differ from step one" >&2; exit 1; }
echo "PASS: step two read exactly what step one committed to the lake"
