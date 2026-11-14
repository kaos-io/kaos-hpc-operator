#!/usr/bin/env bash
# PRD kaos#1911 AC-14: what the key a job runs with can reach in the data store. Two declared apps (kaos-app-lab, the
# one up.sh declares, and kaos-app-lab2, declared here), each with its own outputs repository and its own dataset
# repository (what the app produces). The key of an app must get
#   200 on its own outputs repository (read and write) and on reading its own dataset repository,
#   denied (the data store answers 401 for an authorization denial, 403 is accepted too) on the other app's outputs
#       repository, on writing the other app's dataset and on a repository no app declares.
# Writing a produced dataset is not checked here: the data store grants an app the write right on a declared dataset only
# once the app has created it with its main protected (the produced-dataset path of the cluster), which this script does
# not do. Until then the declared dataset is read-only for its app, which the script checks.
# The key is the app's own access key pair, the one the runner obtains by exchanging the app's workflow service account
# token (POST /kaos/v1/credentials). With APP_IDENTITY_EXCHANGE=1 the script gets the pair that way for the first app
# (needs a data store that trusts this cluster's service account tokens); otherwise it uses the declared pair, which is
# the same user with the same policy.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LAKE_URL=${LAKE_URL:-http://$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$LAKE_CONTAINER"):8000}
[ -s "$STATE/lake-app.env" ] || { echo "the lab is not up: hack/local-lab/up.sh first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$STATE/lake-app.env"
A_KEY=$LAKE_ACCESS_KEY_ID; A_SECRET=$LAKE_SECRET_ACCESS_KEY
B_KEY=AKIALABAPP2AAAAAAAAA; B_SECRET=labapp2secretlabapp2secretlabapp2secretl
A_OUT=$OUT_REPO;  A_DATA=dataset-lab-produced
B_OUT=outputs-lab2; B_DATA=dataset-lab2-produced
UNDECLARED=private-lab

adm() { curl -sf -u "$LAKE_ADMIN_KEY:$LAKE_ADMIN_SECRET" -H 'Content-Type: application/json' "$@"; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

# Both apps are declared with the dataset each one produces (`datasets`: the repositories only that app may write, created
# by the data store); the first app is declared again, now with its dataset. The data store re-reads the folder every
# few seconds.
printf '{"user":"kaos-app-lab","access_key_id":"%s","secret_access_key":"%s","outputs_repository":"%s","datasets":["%s"]}\n' \
  "$LAKE_ACCESS_KEY_ID" "$LAKE_SECRET_ACCESS_KEY" "$A_OUT" "$A_DATA" > "$STATE/apps/lab.json"
printf '{"user":"kaos-app-lab2","access_key_id":"%s","secret_access_key":"%s","outputs_repository":"%s","datasets":["%s"]}\n' \
  "$B_KEY" "$B_SECRET" "$B_OUT" "$B_DATA" > "$STATE/apps/lab2.json"
for r in "$UNDECLARED"; do
  adm "$LAKE_URL/api/v1/repositories/$r" >/dev/null 2>&1 || \
    adm -X POST "$LAKE_URL/api/v1/repositories" -d "{\"name\":\"$r\",\"storage_namespace\":\"local://$r\",\"default_branch\":\"main\"}" >/dev/null
done
for _ in $(seq 30); do
  [ "$(code -u "$B_KEY:$B_SECRET" "$LAKE_URL/api/v1/repositories/$B_DATA")" = 200 ] && break; sleep 2
done

if [ "${APP_IDENTITY_EXCHANGE:-0}" = 1 ]; then
  k -n "$LAB_NS" create serviceaccount lab-workflow --dry-run=client -o yaml | k apply -f - >/dev/null
  tok=$(k -n "$LAB_NS" create token lab-workflow --audience kaos-lake --duration 10m)
  pair=$(curl -sf -X POST -H "Authorization: Bearer $tok" -H 'Content-Type: application/json' -d '{}' "$LAKE_URL/kaos/v1/credentials")
  A_KEY=$(jq -r .access_key_id <<<"$pair"); A_SECRET=$(jq -r .secret_access_key <<<"$pair")
  echo "app one's key obtained through /kaos/v1/credentials (expires $(jq -r .expiration <<<"$pair"))"
fi

fails=0
expect() {   # expect <code> <what> <curl args...>
  local want=$1 what=$2; shift 2
  local got; got=$(code "$@")
  if [ "$got" = "$want" ] || { [ "$want" = denied ] && { [ "$got" = 401 ] || [ "$got" = 403 ]; }; }; then echo "ok:   $what -> $got"; else echo "FAIL: $what -> $got, wanted $want" >&2; fails=$((fails + 1)); fi
}
put() { printf 'x' | curl -s -o /dev/null -w '%{http_code}' -u "$1:$2" -X POST --data-binary @- -H 'Content-Type: application/octet-stream' \
  "$LAKE_URL/api/v1/repositories/$3/branches/main/objects?path=probe.txt"; }
expect_put() {   # expect_put <code> <what> <key> <secret> <repo>
  local got; got=$(put "$3" "$4" "$5")
  if [ "$got" = "$1" ] || { [ "$1" = 200 ] && [ "$got" = 201 ]; } || { [ "$1" = denied ] && { [ "$got" = 401 ] || [ "$got" = 403 ]; }; }; then echo "ok:   $2 -> $got"; else echo "FAIL: $2 -> $got, wanted $1" >&2; fails=$((fails + 1)); fi
}
repo() { echo "$LAKE_URL/api/v1/repositories/$1"; }

expect 200 "app one reads its own outputs repository" -u "$A_KEY:$A_SECRET" "$(repo "$A_OUT")"
expect_put 200 "app one writes its own outputs repository" "$A_KEY" "$A_SECRET" "$A_OUT"
expect 200 "app one reads its own dataset repository" -u "$A_KEY:$A_SECRET" "$(repo "$A_DATA")"
expect denied "app one reads app two's outputs repository" -u "$A_KEY:$A_SECRET" "$(repo "$B_OUT")"
expect_put denied "app one writes app two's outputs repository" "$A_KEY" "$A_SECRET" "$B_OUT"
expect_put denied "app one writes app two's dataset" "$A_KEY" "$A_SECRET" "$B_DATA"
expect denied "app one reads a repository no app declares" -u "$A_KEY:$A_SECRET" "$(repo "$UNDECLARED")"
expect_put denied "app one writes a repository no app declares" "$A_KEY" "$A_SECRET" "$UNDECLARED"
expect 200 "app two reads its own outputs repository" -u "$B_KEY:$B_SECRET" "$(repo "$B_OUT")"
expect 200 "app two reads its own dataset repository" -u "$B_KEY:$B_SECRET" "$(repo "$B_DATA")"
expect denied "app two reads app one's outputs repository" -u "$B_KEY:$B_SECRET" "$(repo "$A_OUT")"
[ "$fails" = 0 ] || { echo "FAIL: $fails check(s) failed" >&2; exit 1; }
echo "PASS: each app's key reaches its own repositories and nothing of the other app or of an undeclared repository"
