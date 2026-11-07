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

| Kind | What it is | Served by a controller here |
|---|---|---|
| `HPCArgoWorkflowJob` | One Argo Workflow step on a site: class, image, command, wall time. Created by the step (the follower), which supplies the platform and run inputs, so it does not run on its own; the controller admits, books, runs and settles it. (`HPCJob` is kept for a future kind that runs without a workflow.) | yes |
| `HPCSite` | One project's whole specification for one site: the site profile (endpoint, classes, QoS lanes, reservations), the project identity, the budget, the limits and the at-limit policy. Its status holds the project's ledger and job counts | yes (publishes the ledger) |
| `HPCAllocation` | An organisation's node-hours on one site and account, from which project shares are carved. Hub-side; the sum check runs on the hub | API type only, no controller |
| `HPCOperation` | A site operation the controller runs itself (staging, cache check, cleanup, quota read) | API type only, no controller |

A job never names its project or its site. The controller reads the project from the
`hpc.kaos.io/project` label of the job's namespace, then picks the project's `HPCSite` that offers
the job's class.

## Documentation

| Page | What it holds |
|---|---|
| [docs/controller.md](docs/controller.md) | How a job moves: admission, booking and settlement, runner Job per attempt, follower, learned time limit, lanes, node sharing, idle watchdog, batched site polling, cancel |
| [docs/operations.md](docs/operations.md) | Flags and environment, chart values, RBAC, metrics, status fields, what is in progress |
| [runner/README.md](runner/README.md) | The Python runner of one attempt |

The product decisions behind it are in PRD-HPC-1461 (novelcore/kaos, `knowledge/prds/1461-hpc-controller.md`).

## Admission

| Outcome | When |
|---|---|
| `Admitted` | It fits the budget. Nodes x time limit is booked |
| `Waiting` / `WaitingForBudget` | It fits only once running jobs settle |
| `Waiting` / `ConcurrencyLimit` | The project is at its job limit. The job waits; it is never refused for this |
| `Refused` / `BudgetBlocked` | It can never fit this period (policy `block`; `warn` admits it and raises an event) |
| `Refused` / `MultiNodeUnsupported`, `WallTimeOverLimit`, `ClassNotAllowed`, `NoProject`, `NoSite` | It can never run as asked |
| `Refused` / `InvalidSpec` | Serve mode: `spec.env` names a variable outside the allowlist |

The ledger is derived from the jobs themselves every time, never kept as a counter, and admission
runs one job at a time, so concurrent jobs never overbook. A job running as a guest on a node
another job of the same project holds is covered by that job's booking.

**Booking and settlement.** Admission books the worst case: nodes x the attempt's time limit (the
learned limit when there is one, see the controller page). When an attempt ends it is settled to
what the site charged: the run time from the site's own start and end times. A site time that is
unset, before 2000, more than an hour in the future (Slurm's `NO_VAL`), or an end before the start
is ignored, so the ledger never goes negative or absurd. A shared-node **host** is settled from
the start to the end of its **Slurm job** (the site charges the node until the host releases it, about two
minutes after its last step), not from its own step's end; guests settle 0. The runner reports the
host job's end after the step's result, and the controller only ever raises the settlement from it.

## Serve mode (phase 1, PRD-HPC-1461 D-58, D-64)

An `HPCArgoWorkflowJob` with `spec.stepName` set is *served*: after admission the controller runs it
through a **runner Job per attempt** in the project's HPC namespace (the `HPCSite`'s namespace),
named `hpcr-<hash8(job uid)>-a<attempt>`. A job without `stepName` is only observed and admitted
(phase 0 behaviour, kept for tests; no in-pod runner exists any more on the platform).

One image holds everything; the entrypoint is chosen by `command`:

| Entrypoint | Who runs it |
|---|---|
| `/manager` | the controller (chart Deployment) |
| `/follower` | the credential-free pipeline step: creates the job (`hpc-<20 hex of sha256(WF_UID/NODE_KEY)>`, owned by the Workflow), prints changes, writes `/tmp/outputs/<n>.json` and `/tmp/slurm-job-id`, exits 0 only on `Succeeded` |
| `python3 -u /runner/main.py` | the runner Job (source in `runner/`) |

The follower prints one plain line whenever the placement or waiting state changes, for example
`HPC: waiting · lane standard · time limit 30 min (learned from 5 runs) · queue reason: Resources`,
`HPC: running since 14:02 UTC · lane standard · time limit 30 min (...)`, a notice when the site refused a fast lane, and the reason a step
ended (`HPC: stopped at its time limit`, `HPC: hit its learned time limit; retrying once with the full limit`,
`HPC: stopped: no GPU use and no output for 20 min (idle watchdog)` with the step's last lines).

The runner's environment is built **only by the controller**: from the spec (`IMAGE_REF`,
`STEP_COMMAND`, `SLURM_TIME_LIMIT`, `STEP_OUTPUTS`, `HPC_CLASS`, `STEP_NAME`, `WF_UID`, `DATASET`,
`HPC_SCOPE`, resume info `KAOS_RESUME_REF`, and the allowlisted `spec.env`: `OUTPUTS_REPO`,
`OUTPUTS_REF`, `CHECKPOINT_BUCKET`, `CHECKPOINT_PREFIX`, `KAOS_LIB_SUPPORT`), from the HPCSite
(`HPC_ACCOUNT`, `HPC_USER`, `HPC_SCRATCH`, `HPC_HOME`, `HPC_CLASSES_JSON`, `HPC_ENDPOINT`), and
from the operator-rendered ConfigMap `hpc-runner-config` (`envFrom`). The CRD (CEL) and the
controller both refuse any other `spec.env` name. Secrets `meluxina-jwt`, `meluxina-staging` and
`mlflow-svc` are mounted from the HPC namespace; they exist **only** there (the environment
namespaces hold no site credential, PRD CON-02, F-35).

Cancel (D-04): the finalizer `hpc.kaos.io/cancel` holds the job until its runner Job is deleted
(Foreground, so the runner's SIGTERM handler cancels the Slurm job). It fires when the job is
deleted, or its Argo Workflow is deleted, shut down (`spec.shutdown`) or ended while the job had not.
The controller sets the annotation `hpc.kaos.io/cancel-requested` first; a runner that gets SIGTERM
for another reason (node drain) finds no annotation and exits so that its Job's replacement
re-attaches instead of cancelling. The Workflow is read as unstructured (no Argo dependency) and
polled every `ServeResync` (15 s).

What the controller decides for an attempt, in order (details in [docs/controller.md](docs/controller.md)):

1. **Time limit.** The step's `wallTimeMinutes` is the cap. After 3 successful runs of the same
   step (same namespace, step, class and image repository) a step of at most 2 h asks for
   p95 x 1.5, rounded up to 15 min (at least 15). Longer or resumable steps keep their declared limit.
   A `TIMEOUT` under a learned limit is retried **once at the declared limit**.
2. **Lane.** With `--lane-candidates` set, the first candidate whose QoS the class may use, whose
   limit and reservation fit, and whose per-user job slot is free; else the class's own QoS. The
   runner falls back to the class QoS if the site refuses the lane (`status.placement.refusedQOS`).
3. **Node sharing.** If the job and the site ask for it, the job may host or be hosted on one node.
4. **Idle watchdog.** A step with no GPU/CPU use and no log growth for 20 min (or
   `spec.idleTimeoutMinutes`; `0` = off) is ended as `Failed` / `Idle`.

Site polling (D-36, T-25): the controller reads the state of every non-terminal job's site job with one
`jobs/state` call per HPCSite per `--site-poll-interval` (default 30 s, `$SITE_POLL_INTERVAL`, `0` = off),
with an explicit `job_id` list (plain comma-separated: the site does not decode `%2C`), chunked at 100, each call
through a token bucket per site profile (`--site-rest-rate` 2/s, `--site-rest-burst` 5). If the site
rejects a list because one ID is unknown to it (HTTP 422, for example a purged guest job), the
controller asks for the IDs one by one, so one gone job never hides the others' state. The JWT is the
`meluxina-jwt` Secret of the HPCSite's namespace (read live, never cached). It writes
`status.siteObservation` (job id, state, pending reason and comment when the site reports them,
time, interval). A failed poll writes nothing. Runners read their state from it and call the site
themselves only for submit, cancel and the final record, or every 120 s when the observation is
older than three intervals (controller down).

`HPCSite.status` carries the ledger (`gpu`/`cpu`: used, booked, left node-minutes), `activeJobs`
(= `queuedJobs` + `runningJobs`: queued is admitted, submitted or pending at the site; running is
RUNNING at the site plus guests on a running host). The ledger is republished when the HPCSite
changes and every 10 minutes.

RBAC roles shipped by the chart (bound per namespace by kubecore-operator):

| ClusterRole | For | Verbs |
|---|---|---|
| `hpcargoworkflowjob-submitter-role` | pipeline service accounts (follower) | `hpcargoworkflowjobs`: create, delete, get, list, watch. **No status verbs.** |
| `hpcargoworkflowjob-runner-role` | `hpc-runner` in the HPC namespace | `hpcargoworkflowjobs`: get; `/status`: get, patch |
| `hpcargoworkflowjob-reporter-role` | legacy (phase 0): the former in-pod runner's service account | `hpcargoworkflowjobs/status`: get, patch |

Metrics (`:8443`, authn/authz-protected; the chart renders a `VMServiceScrape`, else a
`ServiceMonitor`, when the API exists): `kaos_hpc_job_phase`, `kaos_hpc_job_queue_seconds`,
`kaos_hpc_job_run_seconds`, `kaos_hpc_node_minutes_booked`, `kaos_hpc_node_minutes_settled`,
`kaos_hpc_site_node_minutes`, `kaos_hpc_active_jobs`, `kaos_hpc_lane_placements_total`,
`kaos_hpc_idle_kills_total`, `kaos_hpc_runner_restarts_total`.

## In progress (open pull requests, not part of `main`)

| PR | What |
|---|---|
| [#23](https://github.com/kaos-io/kaos-hpc-operator/pull/23) | Extreme-case matrix and tests (`docs/extreme-cases.md`); fixes for the HPCSite-loss orphan, a zero-length sample and cancel retry; `--site-missing-grace` (15 min grace before a job whose HPCSite or project label vanished is cancelled); orphan sweep with a cancel-only runner (AC-08) |
| [#24](https://github.com/kaos-io/kaos-hpc-operator/pull/24) | `HPCSite.status.recentJobs[]` (at most 50, active first) for the console's per-step jobs view (F-43, AC-76) |
| [#25](https://github.com/kaos-io/kaos-hpc-operator/pull/25) | Plain-words stop message when a step ran its full declared limit, and `status.placement.warning` before a repeat |

Until they merge, none of these behaviours is present: `HPCSite.status` has no `recentJobs`, no
sweep of orphaned site jobs runs, a job whose HPCSite disappears is not given a grace period, and the
follower prints only the short `HPC: stopped at its time limit`.

## Development

```sh
make test       # unit tests and envtest (a real API server)
make lint       # golangci-lint
make manifests  # CRDs and RBAC from the Go types
make build      # the manager binary
```

Requires Go 1.26+ (see `go.mod`).

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

