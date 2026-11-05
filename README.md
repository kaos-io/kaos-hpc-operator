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

