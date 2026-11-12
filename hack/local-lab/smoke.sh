#!/usr/bin/env bash
# Submits a two-step workflow to the lab. Step one writes files to the lake and commits them;
# step two reads them back from that commit and prints checksums. Exits non-zero unless the
# checksums match (and the fake Slurm site answers).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RUN=smoke-$(date +%s)

# the fake site answers with its token, verified against its CA (no -k)
k -n "$LAB_NS" create configmap slurm-ca --from-file=ca.crt="$STATE/pki/ca.crt" --dry-run=client -o yaml | k apply -f - >/dev/null
k -n "$LAB_NS" delete pod slurm-ping --ignore-not-found >/dev/null
k apply -n "$LAB_NS" -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata: {name: slurm-ping}
spec:
  restartPolicy: Never
  containers:
  - name: ping
    image: $PROBE_IMAGE
    imagePullPolicy: Never
    command: [sh, -c, "curl -sS --fail-with-body --cacert /ca/ca.crt -H 'X-SLURM-USER-TOKEN: lab-token' https://fake-slurm.$SLURM_NS.svc/slurm/v0.0.44/ping | jq -e '.' >/dev/null"]
    volumeMounts: [{name: ca, mountPath: /ca}]
  volumes: [{name: ca, configMap: {name: slurm-ca}}]
YAML
k -n "$LAB_NS" wait pod/slurm-ping --for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null \
  || { echo "FAIL: fake Slurm site did not answer ping (token + CA)" >&2; k -n "$LAB_NS" logs slurm-ping >&2 || true; exit 1; }
k -n "$LAB_NS" delete pod slurm-ping >/dev/null
echo "fake Slurm site answers ping, verified against its CA"

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
        # the outputs folder is compared with step one, the dataset folder with the seeded files
        sums() { (cd "\$1" && find . -type f | sort | while read -r f; do printf '%s  %s\n' "\$(sha256sum "\$f" | cut -d' ' -f1)" "\$f"; done); }
        sums /in/outputs > /tmp/sums
        sums /in/dataset > /tmp/dsums
    outputs:
      parameters:
      - {name: sums, valueFrom: {path: /tmp/sums}}
      - {name: dsums, valueFrom: {path: /tmp/dsums}}
YAML

"$BIN/argo" --context "$CTX" -n "$LAB_NS" submit "$STATE/smoke-wf.yaml" --wait --log >/dev/null 2>"$STATE/smoke.err" || true
"$BIN/argo" --context "$CTX" -n "$LAB_NS" logs "$RUN" 2>&1 | sed 's/^/  /'
phase=$(k -n "$LAB_NS" get wf "$RUN" -o jsonpath='{.status.phase}')
echo "workflow $RUN: $phase"
[ "$phase" = Succeeded ] || { echo "FAIL: workflow did not succeed" >&2; cat "$STATE/smoke.err" >&2; exit 1; }
param() { k -n "$LAB_NS" get wf "$RUN" -o json | jq -r --arg n "$1" --arg p "$2" '.status.nodes[]|select(.displayName==$n)|.outputs.parameters[]?|select(.name==$p)|.value'; }
w=$(param write sums); r=$(param read sums); d=$(param read dsums)
echo "--- step one checksums"; echo "$w"; echo "--- step two checksums"; echo "$r"
# expected, computed on the host, independent of both steps
exp=$(for n in one two three; do printf 'payload %s %s\n' "$n" "$RUN" | sha256sum | cut -d' ' -f1 | sed "s|\$|  ./$n.txt|"; done | LC_ALL=C sort -k2)
# the seeded dataset files (up.sh), checksummed on the host
dexp=$(for f in readme.txt samples/a.csv samples/b.csv; do printf 'lab dataset file %s\n1,2,3\n' "$f" | sha256sum | cut -d' ' -f1 | sed "s|\$|  ./$f|"; done | LC_ALL=C sort -k2)
[ -n "$d" ] || { echo "FAIL: step two read no dataset files" >&2; exit 1; }
echo "--- dataset checksums read by step two"; echo "$d"
[ "$d" = "$dexp" ] || { echo "FAIL: dataset files read by step two differ from the seeded ones" >&2; exit 1; }
[ "$w" = "$exp" ] || { echo "FAIL: step one checksums differ from the expected ones" >&2; exit 1; }
[ "$r" = "$exp" ] || { echo "FAIL: step two checksums differ from step one" >&2; exit 1; }
echo "PASS: step two read exactly what step one committed to the lake"
