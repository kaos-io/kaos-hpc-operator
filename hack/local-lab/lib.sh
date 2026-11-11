# Shared settings of the local lab. Sourced by up.sh, down.sh and smoke.sh.
LAB_NAME=${LAB_NAME:-prd1911}
LAB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$LAB_DIR/../.." && pwd)
BIN=$LAB_DIR/.bin
STATE=$LAB_DIR/.state/$LAB_NAME
CTX=kind-$LAB_NAME
ARGO_REPO=https://argoproj.github.io/argo-helm
# the lab has its own kubeconfig: nothing is written to the user's ~/.kube/config
mkdir -p "$STATE"
export KUBECONFIG=$STATE/kubeconfig
LAB_NS=lab
SLURM_NS=fake-slurm
LAKE_CONTAINER=$LAB_NAME-lake
LAKE_VOLUME=$LAB_NAME-lake-data
PROBE_IMAGE=kaos-lab-probe:$LAB_NAME
SLURM_IMAGE=kaos-lab-fakeslurmd:$LAB_NAME
# Pipelines run on the workload pool's Argo Workflows: chart pinned in kubecore-operator
# internal/operators/kubepool/phases/syncing_tools.go (argoWorkflows.version). The CLI version is
# that chart's appVersion, read by up.sh; this is the only version to change.
ARGO_CHART_VERSION=1.0.18
# Published dev image (kubecore-operator origin/dev, projectmlstack/gcp/composition.yaml, ml-lakefs-release).
LAKE_PUBLISHED=europe-west1-docker.pkg.dev/kaos-dev-511509/kaos-pool/kaos-lake:pin-e1cf587c8665@sha256:e1cf587c86657a2b8a3d810b45d087077638d72b1a2df83ae9bdcec09ead3339
LAKE_UPSTREAM=treeverse/lakefs:1.86.0
LAKE_ADMIN_KEY=AKIALABADMINAAAAAAAA
LAKE_ADMIN_SECRET=labadminsecretlabadminsecretlabadminsecr
OUT_REPO=outputs-lab
DATA_REPO=dataset-lab
LAKE_APP_KEY=AKIALABAPPAAAAAAAAAA
LAKE_APP_SECRET=labappsecretlabappsecretlabappsecretlabap
k() { kubectl --context "$CTX" "$@"; }
