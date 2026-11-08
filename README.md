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
| `Refused` / `BudgetBlocked` | It can never fit this period within the project's own share (policy `block`; `warn` admits it and raises `OverShare`) |
| `Refused` / `SiteLimit` | It can never fit this period because the site's own monthly limit on the account is what runs out (same policy rule). A submitted job queued behind that limit (Slurm `AssocGrp...Minutes`) shows `SiteLimit` as well |
| `Failed` / `InsufficientQuota` | Staging the dataset or image would exceed the project's scratch quota (space or files) even after evicting everything unused: the runner stops before any copy and submits nothing |
| `Refused` / `TooLongNotResumable` | The declared limit is above the longest run the class or site allows (2 days on MeluXina `default`) and the step is not resumable (declared or verified, D-74), so it would be cut. The message gives the declared limit, the maximum and the two fixes: make it resumable or shorten it. Never cut silently |
| `Refused` / `TooLongBeyondContinuations` | The step is resumable, but its declared limit needs more runs than the continuation cap allows (cap 5: 6 runs, 12 days on MeluXina `default`) |
| `Refused` / `MultiNodeUnsupported`, `WallTimeOverLimit` (the limit is unknown), `ClassNotAllowed`, `NoProject`, `NoSite` | It can never run as asked |

The ledger is derived from the jobs themselves every time, never kept as a counter, and admission
runs one job at a time, so concurrent jobs never overbook. A job running as a guest on a node
another job of the same project holds is covered by that job's booking.

### Budget sources (`HPCSite.spec.budget.source`)

`status.periodStart` is always the first of the calendar month (UTC). `status.countedFrom` is where the used figures
start: the month start, except for `source: site`, where it is `observedAt` (the site's remainder already counts
everything that ended before it, so earlier attempts are not added again).

| Source | Admission counts against |
|---|---|
| `site` | What the site reported left on the project's account at `observedAt`, less the attempts that ended since and the bookings |
| `share` | The project's own optional cap, `gpuNodeHours` / `cpuNodeHours` per calendar month (UTC), less what its own settled attempts used this month and its bookings. With `siteLeft{gpuNodeHours,cpuNodeHours,observedAt}` the site's remainder is an upper bound too: the limit is `min(share left, site left)` |

For `share`, `used` restarts on the 1st at 00:00 UTC, with no re-render needed. A `siteLeft` observed
before the current month is ignored (the share alone governs until the next observation). A job that
can never fit the lower of the two is `Refused`: `BudgetBlocked` when the project's share is the lower,
`SiteLimit` when the site's remainder is (a project without a share has only the site's limit, so it is
`SiteLimit`); the message says which binds;
one that fits it once bookings settle is `Waiting` / `WaitingForBudget`. Policy `warn` admits beyond
it and raises the `OverShare` event. A zero in a tier means no spend in that tier. There is no org
allocation and no cross-project sum: each project's share is its own.

**Warning threshold (AC-22).** `budget.warnAtPercent` (default 80) is a percentage of what the project
may spend this month: used plus booked, over used plus the ceiling (the lower of share left and site left).
While a tier is at or above it, the `HPCSite` condition `BudgetWarning` is `True` (reason
`ThresholdCrossed`), a `Warning` event `BudgetWarning` is raised when it turns true or another tier crosses,
and `kaos_hpc_budget_warning{site,project,tier}` is 1 (`kaos_hpc_budget_percent` carries the number).
The policy (`budget.atLimit`) decides only what happens at the limit; the warning is raised either way.

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

Site polling (D-36, T-25): the controller reads the state of every non-terminal job's site job with one
`jobs/state` call per HPCSite per `--site-poll-interval` (default 30 s, `$SITE_POLL_INTERVAL`, `0` = off), the
`job_id` list always explicit and chunked (100), each call through a token bucket per site profile
(`--site-rest-rate` 2/s, `--site-rest-burst` 5). The JWT is the `meluxina-jwt` Secret of the HPCSite's
namespace (read live, never cached). It writes `status.siteObservation` (job id, state, pending reason and
comment when the site reports them, time, interval). A failed poll writes nothing. Runners read their state from
it and call the site themselves only for submit, cancel and the final record, or every 120 s when the
observation is older than three intervals (controller down). A list the site rejects with 422 (one unknown
ID) is split in halves until the unknown ID stands alone, so it costs about 2*log2(n) calls, not n.

Site outage (PRD-HPC-1461 AC-28, AC-30): a site that does not answer (connection errors, 5xx, timeouts, slurmctld
down) is a wait, not a failure. The poller keeps each site's health (the `jobs/state` read, and a `ping` for a site
with no site job to read; two failed probes in a row spanning at least 90 s = down (a failed `jobs/state` read is confirmed with a ping first; a ping that answers is not an outage), one answer = up; a site without a ping is never read as
down). While the site is down the controller admits and books nothing and creates no runner Job for it: the job is
`Waiting/SiteUnavailable` (or `Admitted` with reason `SiteUnavailable` when it was booked already), with the job
condition `SiteUnavailable`, in the follower's lines and in `recentJobs`; `HPCSite` carries condition `SiteReachable`
and the metric `kaos_hpc_site_up`. It resumes when the site answers. A runner that is already started waits too
(5, 10 ... 60 s between asks, `pendingReason: SiteUnavailable`). Both are bounded by `--site-outage-max` (default
30 min): then the job ends `Failed/SiteUnavailable`. Auth errors, refusals that name a reason (account, QoS, limit)
and other 4xx still fail at once. Needs the site poller (`--site-poll-interval` > 0).

The ledger (`HPCSite.status`: used, booked, left, recent jobs) is written at most once per
`--ledger-publish-interval` (default 1 s) and site; a change inside the interval is written when it has passed,
and the last job to end publishes again once the cache shows its end.

RBAC roles shipped by the chart (bound per namespace by kubecore-operator):

| ClusterRole | For | Verbs |
|---|---|---|
| `hpcargoworkflowjob-submitter-role` | pipeline service accounts (follower) | `hpcargoworkflowjobs`: create, delete, get, list, watch. **No status verbs.** |
| `hpcargoworkflowjob-runner-role` | `hpc-runner` in each env namespace | `hpcargoworkflowjobs`: get; `/status`: get, patch |
| `hpcargoworkflowjob-reporter-role` | **phase 0 only**: the in-pod runner's service account, next to the submitter role | `hpcargoworkflowjobs/status`: get, patch |
| `meluxina-snapshot` (project's snapshot job, kubecore-operator) | the snapshot CronJob's service account in the HPC namespace | `hpcsites/status`: patch, of its own site only (`resourceNames`) |

`HPCSite.status.storage` (project scratch quota: space and files used/limit, `byCategory` datasets, images, checkpoints, work, other, `observedAt`/`measuredAt`) is **not written by this controller**: the project's snapshot job patches it every 10 minutes from the site stager's `usage` report. The controller's status patches are merge patches of its own fields and leave it alone.

Metrics (`:8443`, authn/authz-protected; the chart renders a `VMServiceScrape`, else a
`ServiceMonitor`, when the API exists): `kaos_hpc_job_phase`, `kaos_hpc_job_queue_seconds`,
`kaos_hpc_job_run_seconds`, `kaos_hpc_node_minutes_booked`, `kaos_hpc_node_minutes_settled`,
`kaos_hpc_site_node_minutes`, `kaos_hpc_active_jobs`, `kaos_hpc_runner_restarts_total`, `kaos_hpc_site_up`.

## Interruptions and checkpoints (PRD-HPC-1461 D-74, D-75, D-76)

- **Tier 1, every step, no code:** a step with no checkpoint is re-run from the start after a node failure,
  maintenance or pre-emption, at most twice (`status.reruns`; reasons `RerunNodeFailure`, `RerunPreempted`,
  `RerunLimit`). Never after the step's own error, an out-of-memory kill, a cancel or a wall-time kill.
- **Tier 2, the checkpoint folder:** every job gets `KAOS_CHECKPOINT_DIR` (`/work/ckpt`, on scratch). The job keeps at
  most two copies of it per step (one tar + one manifest each), copies it periodically and when the wall-time signal
  arrives, and deletes the copies when the step succeeds or after 7 days. A copy whose folder digest equals the newest copy's is skipped (the exit-85 copy right after a wall-time copy). A periodic copy is skipped when it would
  take the project over 80 % of its scratch space or file quota or past its checkpoint budget
  (`HPCSite.spec.limits.checkpointBudgetPercent`, default 10 % of the space quota). Details: `runner/README.md`.
- **Continuation:** on exit 85, wall time, node failure, pre-emption or an idle kill, a step that left a copy continues
  from the newest verified one (cap 5, or `spec.resumable.maxContinuations`). A continuation that does not get past its
  checkpoint ends `NoProgress`; one that does (or that runs to completion, which leaves no copy to compare) marks the step key verified (`status.placement.resumable`), after
  which it keeps its declared limit and gets the wall-time signal. `spec.resumableOptOut` (`resumable=False`) turns
  continuing off; the follower passes it from `HPC_RESUMABLE=0`.
- **Steps longer than the longest lane (D-67, D-74):** a resumable step whose declared `walltime` is above the class or
  site maximum is admitted and run as continuations of at most that maximum each (`status.placement.timeLimitMinutes`,
  one run booked at a time), up to the continuation cap + 1 runs in all; the follower prints "declared N min runs as
  continuations of up to M min each". A step that is not resumable is refused at admission (`TooLongNotResumable`).
  The `long` QoS (6 days, one job per user) is not used for this: `placement="long"` (D-71) is not built.
- **Reported:** `status.run.checkpoint` (progress, size of the newest copy, the step's and the project's total,
  why a copy was skipped) and `HPCSite.status.checkpoints` (scratch held by steps that have not succeeded).

## Idle watchdog for in-cluster GPU steps (PRD-HPC-1461 D-72, D-76)

The same binary can end idle Kubernetes GPU steps on the pool: GPU use below 5 % and no log output
for 20 minutes (per step `platform.kubecore.io/idle-timeout`, minutes, 0 = off). Enable it with the
chart value `idleWatchdog.metricsUrl` (the pool's PromQL API); it then also installs a ClusterRole
to patch the status of and delete Argo workflow pods that hold a GPU. Design, metric sources and the
interplay with the template's re-run: [internal/idlewatch/README.md](internal/idlewatch/README.md).
Metrics: `kaos_k8s_idle_kills_total`, `kaos_k8s_idle_watchdog_errors_total`.

## Development

```sh
make test       # unit tests and envtest (a real API server)
make lint       # golangci-lint
make manifests  # CRDs and RBAC from the Go types
make build      # the manager binary
make scale-test # load test of the real controller against fake Slurm sites (about 10 minutes, test/scale/README.md)
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

