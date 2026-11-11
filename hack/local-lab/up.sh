#!/usr/bin/env bash
# Brings the local lab up (idempotent). See README.md.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
mkdir -p "$BIN" "$STATE"
say() { printf '\n== %s\n' "$*"; }

# 1. kind cluster, single node
say "kind cluster $LAB_NAME"
if kind get clusters 2>/dev/null | grep -qx "$LAB_NAME"; then
  echo "cluster exists"
else
  kind create cluster --name "$LAB_NAME" --kubeconfig "$KUBECONFIG" ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"} --wait 120s
fi
[ -s "$KUBECONFIG" ] || kind get kubeconfig --name "$LAB_NAME" > "$KUBECONFIG"

# 2. Argo Workflows at the workload pool's version, and the matching argo CLI
ARGO_VERSION=$(helm show chart argo-workflows --repo "$ARGO_REPO" --version "$ARGO_CHART_VERSION" | sed -n 's/^appVersion: //p')
[ -n "$ARGO_VERSION" ] || { echo "cannot read the app version of chart $ARGO_CHART_VERSION" >&2; exit 1; }
say "argo CLI $ARGO_VERSION (chart $ARGO_CHART_VERSION)"
if [ ! -x "$BIN/argo" ] || ! "$BIN/argo" version --short 2>/dev/null | grep -q "${ARGO_VERSION#v}"; then
  curl -fsSL "https://github.com/argoproj/argo-workflows/releases/download/$ARGO_VERSION/argo-linux-amd64.gz" | gunzip > "$BIN/argo"
  chmod +x "$BIN/argo"
fi
"$BIN/argo" version --short 2>&1 | head -1
say "Argo Workflows chart $ARGO_CHART_VERSION (controller only)"
k create namespace "$LAB_NS" --dry-run=client -o yaml | k apply -f - >/dev/null
deployed=$(helm --kube-context "$CTX" list -n argo-workflows -o json | jq -r --arg c "argo-workflows-$ARGO_CHART_VERSION" '[.[]|select(.chart==$c and .status=="deployed")]|length')
if [ "$deployed" = 1 ]; then
  echo "chart already deployed"
else
helm --kube-context "$CTX" upgrade --install argo-workflows argo-workflows --repo "$ARGO_REPO" --version "$ARGO_CHART_VERSION" \
  --namespace argo-workflows --create-namespace --wait --timeout 5m \
  --set server.enabled=false \
  --set controller.resources.requests.memory=128Mi --set controller.resources.limits.memory=512Mi \
  --set 'controller.workflowNamespaces={argo-workflows,lab}'
fi

k apply -n "$LAB_NS" -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata: {name: lab}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: lab-workflow}
rules:
- {apiGroups: [argoproj.io], resources: [workflowtaskresults], verbs: [create, patch]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: lab-workflow}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: lab-workflow}
subjects: [{kind: ServiceAccount, name: lab}]
YAML

# 3. kaos-lake container on local storage
say "kaos-lake container"
if [ -z "${LAKE_IMAGE:-}" ]; then
  if docker image inspect kaos-lake:prd1911 >/dev/null 2>&1; then
    LAKE_IMAGE=kaos-lake:prd1911
  else
    if docker image inspect "$LAKE_PUBLISHED" >/dev/null 2>&1 || docker pull "$LAKE_PUBLISHED" >/dev/null 2>&1; then
      LAKE_IMAGE=$LAKE_PUBLISHED
    else
      LAKE_IMAGE=$LAKE_UPSTREAM
      echo "!!!!!!!! WARNING: neither kaos-lake:prd1911 nor the published dev image is available." >&2
      echo "!!!!!!!! FALLING BACK to UPSTREAM $LAKE_UPSTREAM: this is NOT kaos-lake." >&2
    fi
  fi
fi
echo "lake image: $LAKE_IMAGE"
# kaos-lake multi-user mode with one declared app user (kaos.app_users, PRD kaos#1217 F-06):
# user kaos-app-lab, read on dataset-*, write on its outputs repository outputs-lab.
mkdir -p "$STATE/apps"
printf '{"user":"kaos-app-lab","access_key_id":"%s","secret_access_key":"%s","outputs_repository":"%s"}\n' \
  "$LAKE_APP_KEY" "$LAKE_APP_SECRET" "$OUT_REPO" > "$STATE/apps/lab.json"
lake_args=(-e LAKEFS_AUTH_UI_CONFIG_RBAC=internal -e LAKEFS_KAOS_APP_USERS_PATH=/apps -e LAKEFS_KAOS_APP_USERS_INTERVAL=5s -v "$STATE/apps":/apps:ro)
if [ "$LAKE_IMAGE" = "$LAKE_UPSTREAM" ]; then lake_args=(); fi   # upstream has no declared users: the app uses the admin key
running=$(docker inspect -f '{{.Config.Image}} {{.State.Running}}' "$LAKE_CONTAINER" 2>/dev/null || true)
if [ "$running" != "$LAKE_IMAGE true" ]; then
  if [ -n "$running" ] && [ "${running% *}" != "$LAKE_IMAGE" ]; then
    # another image: its data format may differ (upstream vs fork, other build), so start on a fresh volume
    echo "lake image changes from ${running% *} to $LAKE_IMAGE: recreating the container on a fresh volume"
    docker rm -f "$LAKE_CONTAINER" >/dev/null
    docker volume rm "$LAKE_VOLUME" >/dev/null 2>&1 || true
  fi
  docker rm -f "$LAKE_CONTAINER" >/dev/null 2>&1 || true
  docker volume create "$LAKE_VOLUME" >/dev/null
  docker run -d --name "$LAKE_CONTAINER" --network kind --memory 1g --user 0 \
    -v "$LAKE_VOLUME":/data "${lake_args[@]}" \
    -e LAKEFS_BLOCKSTORE_TYPE=local -e LAKEFS_BLOCKSTORE_LOCAL_PATH=/data/blocks \
    -e LAKEFS_DATABASE_TYPE=local -e LAKEFS_DATABASE_LOCAL_PATH=/data/kv \
    -e LAKEFS_AUTH_ENCRYPT_SECRET_KEY=lab-encrypt-secret -e LAKEFS_LISTEN_ADDRESS=0.0.0.0:8000 \
    -e LAKEFS_STATS_ENABLED=false -e LAKEFS_LOGGING_LEVEL=WARN \
    "$LAKE_IMAGE" run >/dev/null
fi
LAKE_IP=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$LAKE_CONTAINER")
LAKE_URL=http://$LAKE_IP:8000
for _ in $(seq 60); do curl -sf "$LAKE_URL/_health" >/dev/null && break; sleep 1; done
curl -sf "$LAKE_URL/_health" >/dev/null || { docker logs "$LAKE_CONTAINER" | tail -20; echo "lake did not become healthy" >&2; exit 1; }
# first-time setup: admin with fixed keys (409 when already set up)
curl -s -o /dev/null -X POST "$LAKE_URL/api/v1/setup_lakefs" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"key\":{\"access_key_id\":\"$LAKE_ADMIN_KEY\",\"secret_access_key\":\"$LAKE_ADMIN_SECRET\"}}"
adm() { curl -sf -u "$LAKE_ADMIN_KEY:$LAKE_ADMIN_SECRET" -H 'Content-Type: application/json' "$@"; }
if [ "$LAKE_IMAGE" = "$LAKE_UPSTREAM" ]; then LAKE_APP_KEY=$LAKE_ADMIN_KEY; LAKE_APP_SECRET=$LAKE_ADMIN_SECRET; fi
printf 'LAKE_ACCESS_KEY_ID=%s\nLAKE_SECRET_ACCESS_KEY=%s\n' "$LAKE_APP_KEY" "$LAKE_APP_SECRET" > "$STATE/lake-app.env"
# shellcheck disable=SC1091
source "$STATE/lake-app.env"
app() { curl -sf -u "$LAKE_ACCESS_KEY_ID:$LAKE_SECRET_ACCESS_KEY" -H 'Content-Type: application/json' "$@"; }
for r in "$OUT_REPO" "$DATA_REPO"; do
  adm "$LAKE_URL/api/v1/repositories/$r" >/dev/null 2>&1 || \
    adm -X POST "$LAKE_URL/api/v1/repositories" -d "{\"name\":\"$r\",\"storage_namespace\":\"local://$r\",\"default_branch\":\"main\"}" >/dev/null
done
echo "waiting for the declared app user"
for _ in $(seq 60); do app "$LAKE_URL/api/v1/repositories/$OUT_REPO" >/dev/null 2>&1 && break; sleep 2; done
app "$LAKE_URL/api/v1/repositories/$OUT_REPO" >/dev/null || { echo "app user cannot reach $OUT_REPO" >&2; docker logs "$LAKE_CONTAINER" | tail -20; exit 1; }
seeded=$(adm "$LAKE_URL/api/v1/repositories/$DATA_REPO/refs/main/objects/ls" | jq '.results|length')
if [ "$seeded" = 0 ]; then
  for f in readme.txt samples/a.csv samples/b.csv; do
    printf 'lab dataset file %s\n1,2,3\n' "$f" | adm -X POST --data-binary @- -H 'Content-Type: application/octet-stream' \
      "$LAKE_URL/api/v1/repositories/$DATA_REPO/branches/main/objects?path=$f" >/dev/null
  done
  adm -X POST "$LAKE_URL/api/v1/repositories/$DATA_REPO/branches/main/commits" -d '{"message":"lab dataset"}' >/dev/null
fi
# reachable from pods as lake.lab.svc:8000 (Service without selector -> container IP)
k apply -n "$LAB_NS" -f - <<YAML
apiVersion: v1
kind: Service
metadata: {name: lake}
spec: {ports: [{port: 8000, targetPort: 8000}]}
---
apiVersion: v1
kind: Endpoints
metadata: {name: lake}
subsets: [{addresses: [{ip: $LAKE_IP}], ports: [{port: 8000}]}]
---
apiVersion: v1
kind: Secret
metadata: {name: lake-app}
stringData:
  LAKE_ENDPOINT: http://lake.$LAB_NS.svc:8000
  LAKE_ACCESS_KEY_ID: $LAKE_ACCESS_KEY_ID
  LAKE_SECRET_ACCESS_KEY: $LAKE_SECRET_ACCESS_KEY
YAML
echo "$LAKE_IMAGE" > "$STATE/lake-image"

# 4. fake Slurm site (test/fakeslurmd), started as the repository's e2e starts it
say "fake Slurm site"
if [ ! -f "$STATE/pki/tls.crt" ] || ! openssl x509 -checkend 604800 -noout -in "$STATE/pki/tls.crt" >/dev/null; then
  echo "generating the fake site certificates (valid 365 days)"
  mkdir -p "$STATE/pki"
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=lab-ca" -keyout "$STATE/pki/ca.key" -out "$STATE/pki/ca.crt" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=fake-slurm" -keyout "$STATE/pki/tls.key" -out "$STATE/pki/tls.csr" 2>/dev/null
  openssl x509 -req -in "$STATE/pki/tls.csr" -CA "$STATE/pki/ca.crt" -CAkey "$STATE/pki/ca.key" -CAcreateserial -days 365 \
    -extfile <(printf 'subjectAltName=DNS:fake-slurm.%s.svc,DNS:fake-slurm' "$SLURM_NS") -out "$STATE/pki/tls.crt" 2>/dev/null
fi
docker build -q -t "$SLURM_IMAGE" -f "$REPO_ROOT/test/fakeslurmd/Dockerfile" "$REPO_ROOT" >/dev/null
kind load docker-image "$SLURM_IMAGE" --name "$LAB_NAME"
k create namespace "$SLURM_NS" --dry-run=client -o yaml | k apply -f - >/dev/null
k -n "$SLURM_NS" create secret generic fake-slurm-tls --from-file="$STATE/pki/tls.crt" --from-file="$STATE/pki/tls.key" --from-file="$STATE/pki/ca.crt" --dry-run=client -o yaml | k apply -f - >/dev/null
k apply -n "$SLURM_NS" -f - <<YAML
apiVersion: v1
kind: Secret
metadata: {name: fake-slurm-token}
stringData: {token: lab-token}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: fake-slurm}
spec:
  replicas: 1
  selector: {matchLabels: {app: fake-slurm}}
  template:
    metadata:
      labels: {app: fake-slurm}
      annotations: {lab/tls-sha256: "$(sha256sum "$STATE/pki/tls.crt" | cut -d' ' -f1)"}
    spec:
      containers:
      - name: fakeslurmd
        image: $SLURM_IMAGE
        imagePullPolicy: IfNotPresent
        env:
        - name: FAKE_SLURM_TOKEN
          valueFrom: {secretKeyRef: {name: fake-slurm-token, key: token}}
        ports: [{containerPort: 8443}]
        readinessProbe: {tcpSocket: {port: 8443}}
        volumeMounts: [{name: tls, mountPath: /etc/fakeslurm, readOnly: true}]
      volumes:
      - name: tls
        secret: {secretName: fake-slurm-tls}
---
apiVersion: v1
kind: Service
metadata: {name: fake-slurm}
spec:
  selector: {app: fake-slurm}
  ports: [{port: 443, targetPort: 8443}]
YAML
k -n "$SLURM_NS" rollout status deploy/fake-slurm --timeout=120s

# 5. probe step image
say "probe image"
docker build -q -t "$PROBE_IMAGE" "$LAB_DIR/probe" >/dev/null
kind load docker-image "$PROBE_IMAGE" --name "$LAB_NAME"

say "lab $LAB_NAME is up"
echo "context:     $CTX"
echo "argo:        $BIN/argo (namespace $LAB_NS, service account lab)"
echo "lake:        $LAKE_URL from the host, http://lake.$LAB_NS.svc:8000 from pods; image $LAKE_IMAGE"
echo "lake app:    $STATE/lake-app.env (repos: $OUT_REPO, $DATA_REPO)"
echo "fake slurm:  https://fake-slurm.$SLURM_NS.svc (token lab-token, CA $STATE/pki/ca.crt)"
echo "probe image: $PROBE_IMAGE"
