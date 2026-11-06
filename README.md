# kaos-hpc-operator

A Kubernetes operator that runs pipeline steps on HPC (Slurm) sites **as a tenant**: no
administrator rights and nothing running on the site outside a job. One operator runs per cluster
and serves every project on it, each in its own namespaces.

The operator decides what a tenant can decide:

- **whether** a step may run: the project's budget share, its limits and the wall time;
- **what it costs**: it books the worst case, then settles to what the site reports;
- **what happens when it ends**: retry, continue from a checkpoint, or fail.

The site's own scheduler still decides when and where a job starts.

## API (`hpc.kaos.io/v1alpha1`)

| Kind | What it is |
|---|---|
| `HPCArgoWorkflowJob` | One Argo Workflow step on a site: class, image, command, wall time. Created by the step, which supplies the platform and run inputs, so it does not run on its own; the operator admits, books and settles it. (`HPCJob` is kept for a future kind that runs without a workflow.) |
| `HPCSite` | One project's whole specification for one site: the site profile (endpoint, classes), the project identity, the budget share, the limits and the at-limit policy. Its status holds the project's ledger |
| `HPCAllocation` | An organisation's node-hours on one site and account, from which project shares are carved |
| `HPCOperation` | A site operation the operator runs itself (staging, cache check, cleanup) |

A job never names its project or its site. The operator reads the project from the
`hpc.kaos.io/project` label of the job's namespace, then picks the project's `HPCSite` that offers
the job's class.

## Admission (phase 0)

| Outcome | When |
|---|---|
| `Admitted` | It fits the share. Nodes × wall time is booked |
| `Waiting` / `WaitingForBudget` | It fits only once running jobs settle |
| `Waiting` / `ConcurrencyLimit` | The project is at its job limit. The job waits; it is never refused for this |
| `Refused` / `BudgetBlocked` | It can never fit this period (policy `block`; `warn` admits it and raises an event) |
| `Refused` / `MultiNodeUnsupported`, `WallTimeOverLimit`, `ClassNotAllowed`, `NoProject`, `NoSite` | It can never run as asked |

The ledger is derived from the jobs themselves every time, never kept as a counter, and admission
runs one job at a time, so concurrent jobs never overbook. A job running as a guest on a node
another job of the same project holds is covered by that job's booking.

See [docs/design.md](docs/design.md) for the full design.

## Serve mode (phase 1, PRD-HPC-1461 D-58, D-64)

An `HPCArgoWorkflowJob` with `spec.stepName` set is *served*: after admission the controller runs it
through a **runner Job per attempt** in the project's HPC namespace (the `HPCSite`'s namespace),
named `hpcr-<hash8(job uid)>-a<attempt>`. A job without `stepName` behaves as in phase 0.

One image holds everything; the entrypoint is chosen by `command`:

| Entrypoint | Who runs it |
|---|---|
| `/manager` | the controller (chart Deployment) |
| `/follower` | the credential-free pipeline step: creates the job (`hpc-<20 hex of sha256(WF_UID/NODE_KEY)>`, owned by the Workflow), prints changes, writes `/tmp/outputs/<n>.json` and `/tmp/slurm-job-id`, exits 0 only on `Succeeded` |
| `python3 -u /runner/main.py` | the runner Job (source in `runner/`) |

The runner's environment is built **only by the controller**: from the spec (`IMAGE_REF`,
`STEP_COMMAND`, `SLURM_TIME_LIMIT`, `STEP_OUTPUTS`, `HPC_CLASS`, `STEP_NAME`, `WF_UID`, `DATASET`,
`HPC_SCOPE`, resume info `KAOS_RESUME_REF`, and the allowlisted `spec.env`: `OUTPUTS_REPO`,
`OUTPUTS_REF`, `CHECKPOINT_BUCKET`, `CHECKPOINT_PREFIX`, `KAOS_LIB_SUPPORT`), from the HPCSite
(`HPC_ACCOUNT`, `HPC_USER`, `HPC_SCRATCH`, `HPC_HOME`, `HPC_CLASSES_JSON`, `HPC_ENDPOINT`), and
from the operator-rendered ConfigMap `hpc-runner-config` (`envFrom`). The CRD (CEL) and the
controller both refuse any other `spec.env` name. Secrets `meluxina-jwt`, `meluxina-staging` and
`mlflow-svc` are mounted from the HPC namespace.

Cancel (D-04): the finalizer `hpc.kaos.io/cancel` holds the job until its runner Job is deleted
(Foreground, so the runner's SIGTERM handler cancels the Slurm job). It fires when the job is
deleted, or its Argo Workflow is deleted, shut down (`spec.shutdown`) or ended while the job had not.
The Workflow is read as unstructured (no Argo dependency) and polled every `ServeResync` (15 s).

RBAC roles shipped by the chart (bound per namespace by kubecore-operator):

| ClusterRole | For | Verbs |
|---|---|---|
| `hpcargoworkflowjob-submitter-role` | pipeline service accounts (follower) | `hpcargoworkflowjobs`: create, delete, get, list, watch. **No status verbs.** |
| `hpcargoworkflowjob-runner-role` | `hpc-runner` in each env namespace | `hpcargoworkflowjobs`: get; `/status`: get, patch |
| `hpcargoworkflowjob-reporter-role` | **phase 0 only**: the in-pod runner's service account, next to the submitter role | `hpcargoworkflowjobs/status`: get, patch |

Metrics (`:8443`, authn/authz-protected; the chart renders a `VMServiceScrape`, else a
`ServiceMonitor`, when the API exists): `kaos_hpc_job_phase`, `kaos_hpc_job_queue_seconds`,
`kaos_hpc_job_run_seconds`, `kaos_hpc_node_minutes_booked`, `kaos_hpc_node_minutes_settled`,
`kaos_hpc_site_node_minutes`, `kaos_hpc_active_jobs`, `kaos_hpc_runner_restarts_total`.

## Development

```sh
make test       # unit tests and envtest (a real API server)
make lint       # golangci-lint
make manifests  # CRDs and RBAC from the Go types
make build      # the manager binary
```

Requires Go 1.24+.

## Licence

Apache License 2.0. See [LICENSE](LICENSE).

## Install

The Helm chart is in `dist/chart` (CRDs, RBAC, the controller). Set the image you built:

```bash
helm install kaos-hpc-operator dist/chart -n kaos-hpc-system --create-namespace \
  --set controllerManager.container.image.repository=<registry>/kaos-hpc-operator
```

The chart also installs `hpcargoworkflowjob-submitter-role`: bind it to the service account of
the workflow steps that run on HPC, in each namespace whose workflows may use it.

After an API change, regenerate the chart with `kubebuilder edit --plugins=helm/v1-alpha`;
`make test` fails while the chart's CRDs or RBAC differ from `config/`.

