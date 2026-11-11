# kaos-hpc-operator

A Kubernetes operator that runs pipeline steps on HPC (Slurm) sites **as a tenant**: no
administrator rights and nothing running on the site outside a job. One operator runs per cluster
and serves every project on it, each in its own namespaces.

The operator decides what a tenant can decide:

- **whether** a step may run: the project's budget share, its limits and the wall time;
- **what it costs**: it books the worst case, then settles to what the site reports;
- **what happens when it ends**: retry, continue from a checkpoint, or fail.

The site's own scheduler still decides when and where a job starts.

Admission, the ledger, settlement, continuation and the site handling need only a Kubernetes cluster, Argo Workflows (a
step creates the job; the operator reads the Workflow to know when to cancel) and an HPC site with a Slurm REST API
(`slurmrestd`) that the project has an account on. MeluXina is the site it is proven on; the examples below name it only
as an example. The runner's data plane (dataset staging from a lakeFS-style store, a Zitadel-minted data-plane token,
MLflow, the login-node stager) and the objects around it (who renders the `HPCSite`, binds the roles, provides the
Secrets) are today provided by KAOS, the platform this operator was written for; KAOS is one optional integration,
described in [KAOS integration](#kaos-integration-optional). Installing without KAOS is not yet a documented path.
The identifiers `PRD-HPC-1461`, `D-nn`, `AC-nn`, `F-nn` and `T-nn` in this repository's comments and docs point to the
requirements document of that integration; they are traceability labels and nothing here depends on reading them.

## API (`hpc.kaos.io/v1alpha1`)

| Kind | What it is |
|---|---|
| `HPCArgoWorkflowJob` | One Argo Workflow step on a site: class, image, command, wall time. Created by the step, which supplies the platform and run inputs, so it does not run on its own; the operator admits, books and settles it |
| `HPCSite` | One project's whole specification for one site: the site profile (endpoint, classes, `maxSubmitJobs`), the project identity, the budget (one figure per allocation: gpu, cpu, fpga, largemem), the limits and the at-limit policy. Its status holds the project's ledger (`status.gpu`, `cpu`, `fpga`, `largeMem`) |
| `HPCAllocation` | An organisation's node-hours on one site and account. **Installed but not reconciled**: nothing in this repository reads or writes it, and admission uses each project's `HPCSite` budget only |
| `HPCOperation` | A site operation (staging, cache check, cleanup, quota read). **Installed but not reconciled yet**: no controller here creates it; the runner stages and cleans up inside its own job, and the scratch quota is read by whatever patches `HPCSite.status.storage` |

A job never names its project or its site. The operator reads the project from the
`hpc.kaos.io/project` label of the job's namespace, then picks the project's `HPCSite` that offers
the job's class. Every `Reason` the operator writes, what it means and what the user sees is in
[docs/reasons.md](docs/reasons.md).

## Admission (phase 0)

| Outcome | When |
|---|---|
| `Admitted` | It fits the share. Nodes × wall time is booked |
| `Waiting` / `WaitingForBudget` | It fits only once running jobs settle |
| `Waiting` / `ConcurrencyLimit` | The project is at its job limit. The job waits; it is never refused for this |
| `Refused` / `SubmitLimit` | The site caps a user's queued plus running jobs (Slurm `MaxSubmitJobs`, per-user submit limit; `HPCSite.spec.profile.maxSubmitJobs`) and the project's own site jobs (guests on a shared node hold no Slurm job and are not counted) already fill it. It is checked after the concurrency limit, so a lower `maxConcurrentJobs` still makes the job wait. The job **fails** at once, never waits (unlike `ConcurrencyLimit`), with the cap and the current count in the message; resubmit once some jobs finish. When the site itself refuses a submit (`QOSMaxSubmitJobPerUserLimit`, `AssocMaxSubmitJobLimit`, ...), the runner reports exit code 238 and the job ends `Failed` / `SubmitLimit` with the cap and the count it read from the site. Platform site operations (site jobs, staging, cancel) are not judged by KAOS admission and still submit |
| `Refused` / `BudgetBlocked` | It can never fit this period within the project's own share (policy `block`; `warn` admits it and raises `OverShare`) |
| `Refused` / `SiteLimit` | It can never fit this period because the site's own monthly limit on the account is what runs out (same policy rule). A submitted job queued behind that limit (Slurm `AssocGrp...Minutes`) shows `SiteLimit` as well |
| `Failed` / `InsufficientQuota` | Staging the dataset or image would exceed the project's scratch quota (space or files) even after evicting everything unused: the runner stops before any copy and submits nothing |
| `Failed` / `StagingMismatch` | The runner and the site's staging tool speak different staging contracts (the two ship separately; one was updated first). The runner stops before it stages or submits anything, so no partial copy of a dataset is ever built or served; the job ends in seconds and costs no node time. It ends when the other side is updated |
| `Refused` / `TooLongNotResumable` | The declared limit is above the longest run the class or site allows (2 days on MeluXina `default`) and the step is not resumable (declared or verified, D-74), so it would be cut. The message gives the declared limit, the maximum and the two fixes: make it resumable or shorten it. Never cut silently |
| `Refused` / `TooLongBeyondContinuations` | The step is resumable, but its declared limit needs more runs than the continuation cap allows (cap 5: 6 runs, 12 days on MeluXina `default`) |
| `Refused` / `InvalidImage` | The step's image is not `<registry-host>/<repository>:<tag>` (or `@sha256:` digest), so it can never be pulled (a half-rendered `:app-latest`). Nothing is booked, no runner starts, no site job is submitted. The runner checks the same before it stages or submits and reports exit 239 (`Failed` / `InvalidImage`); a bad reference never triggers the fresh-credentials resubmit |
| `Refused` / `MultiNodeUnsupported`, `WallTimeOverLimit` (the class's limit is unknown), `ClassNotAllowed`, `NoProject`, `NoSite`, `InvalidSpec` (an `env` name outside the allowlist) | It can never run as asked |

The ledger is derived from the jobs themselves every time, never kept as a counter, and admission
runs one job at a time, so concurrent jobs never overbook. A job running as a guest on a node
another job of the same project holds is covered by that job's booking.

### Node sharing is planned, not raced (D-26, D-70)

A host can only take a guest that is still queued at the site, so steps that all start at once on free nodes
never pack. The controller therefore plans the packing at admission, one job at a time. A shareable step (the
project's site has `spec.sharing`, the step names a `sharing.scope`, its class has GPUs; resumable steps share too, AC-73) is
given `status.placement.share`:

- **guest** of the open host (running, or queued for less than `HostStartWait`) of its own site, scope and class
  that has a free slot and enough time left (the step plus 6 minutes). The fullest host wins, so N steps use
  `ceil(N / gpusPerNode)` hosts whatever the site's free capacity. A guest has no lane (it never takes a one-job
  lane a host needs). The runner submits it deferred: BeginTime 12 h ahead, so it can never start on a node of its
  own, and the host's slot manager takes it as it takes a job in its offer window.
- **host** when no open host fits. It is placed in a lane that leaves room for a guest of its own size (else the
  class's own QoS if that does, else as before) and its Slurm job limit (`share.jobLimitMinutes`; a resumable host's job is not padded) is recorded.

A guest no host took is planned again (`ShareReplanned` event, `kaos_hpc_share_replans_total`): its host ended, did
not start within `HostStartWait` (20 min), no longer has the time, or runs and did not take it within
`GuestTakeWait` (10 min). It joins another open host or becomes a host; the runner, which reads the plan while it
waits, replaces the deferred job by one that starts at once in the lane the plan names. A guest a host already took
is left to the runner (a lost host resubmits it). Every guest is a Slurm job at the site until its host cancels it
with the result, so it counts toward the site's submit cap.

### Budget sources (`HPCSite.spec.budget.source`)

`status.periodStart` is always the first of the calendar month (UTC). `status.countedFrom` is where the used figures
start: the month start, except for `source: site`, where it is `observedAt` (the site's remainder already counts
everything that ended before it, so earlier attempts are not added again).

| Source | Admission counts against |
|---|---|
| `site` | What the site reported left on the project's account at `observedAt`, less the attempts that ended since and the bookings |
| `share` | The project's own optional cap, `gpuNodeHours` / `cpuNodeHours` / `fpgaNodeHours` / `largeMemNodeHours` per calendar month (UTC), less what its own settled attempts used this month and its bookings. With `siteLeft{gpuNodeHours,cpuNodeHours,fpgaNodeHours,largeMemNodeHours,observedAt}` the site's remainder is an upper bound too: the limit is `min(share left, site left)` |

For `share`, `used` restarts on the 1st at 00:00 UTC, with no re-render needed. A `siteLeft` observed
before the current month is ignored (the share alone governs until the next observation). A job that
can never fit the lower of the two is `Refused`: `BudgetBlocked` when the project's share is the lower,
`SiteLimit` when the site's remainder is (a project without a share has only the site's limit, so it is
`SiteLimit`); the message says which binds;
one that fits it once bookings settle is `Waiting` / `WaitingForBudget`. Policy `warn` admits beyond
it and raises the `OverShare` event. A zero in a tier means no spend in that tier. There is no org
allocation and no cross-project sum: each project's share is its own.

**One budget per allocation (AC-84).** A project has four allocations, each metered by the site on its own
(MeluXina partitions `gpu`, `cpu`, `fpga`, `largemem`; Slurm `GrpTRESMins`, shown as `AssocGrp<TRES>Minutes` when spent):
the class's `tier` (`gpu`, `cpu`, `fpga`, `largemem`) names the allocation a step spends. Each has its own figure in
`spec.budget` (and `siteLeft`), its own ledger in `status.gpu`, `status.cpu`, `status.fpga`, `status.largeMem`
(`usedNodeMinutes`, `bookedNodeMinutes`, `leftNodeMinutes`; the existing fields are unchanged, the two new ones
are additive; `percent` is the warning percentage, -1 when there is none), its own admission check, its own settlement (an attempt settles into the tier it booked) and its own
warning percentage. A spent allocation blocks only the steps that spend it: a CPU allocation with nothing left refuses
(`SiteLimit` / `BudgetBlocked`) or holds (`WaitingForBudget`) CPU steps with a message that names the CPU allocation,
and GPU, FPGA and large-memory steps are admitted as before. Whoever renders the `HPCSite` from the site's accounting
(source `site`, or `siteLeft`) must write a figure per allocation; an allocation `siteLeft` omits (fpga, largemem) means the site gave no figure: no site bound, the share alone
applies; only an explicit 0 (or a negative remainder) means spent. With `source: site` the same rule holds for
`spec.budget`: an omitted `fpgaNodeHours` / `largeMemNodeHours` means no figure, so the allocation is not bounded
(steps are admitted, use is still settled and recorded, and `status.fpga` / `status.largeMem` show `unbounded: true`
with `leftNodeMinutes` 0 and no warning percentage); an explicit value, 0 included, bounds it. With `source: share`
an omitted figure is 0, no spend, as for every tier. GPU and CPU keep their earlier meaning (an omitted 0 is 0 left). A zero is a real figure: the 2026-11-05 incident (CPU limit 0, cpu jobs held forever with
`AssocGrpGRESMinutes`) is now refused at admission with the reason, not queued.

**Warning threshold (AC-22).** `budget.warnAtPercent` (default 80) is a percentage of what the project
may spend this month: used plus booked, over used plus the ceiling (the lower of share left and site left).
While a tier is at or above it, the `HPCSite` condition `BudgetWarning` is `True` (reason
`ThresholdCrossed`), a `Warning` event `BudgetWarning` is raised when it turns true or another tier crosses,
and `kaos_hpc_budget_warning{site,project,tier}` is 1 (`kaos_hpc_budget_percent` carries the number).
The policy (`budget.atLimit`) decides only what happens at the limit; the warning is raised either way.

With `source: site` the figures in `spec.budget` are what is LEFT on the account at `observedAt`, so used-since-then over
what was left says nothing about the month. A site-source budget therefore also carries the month's grant,
`spec.budget.grantNodeHours` (`gpu`, `cpu`, `fpga`, `largeMem`, node-hours, rendered by the integrating platform from the
site's accounting), and the warning percentage of that allocation becomes the share of the grant spent or committed:
`(grant - left at observedAt + used since + booked) / grant`. Example: grant 868 h, 847 h left at `observedAt`, nothing
booked: 21 h of 868 are spent, 2%. Admission and the ceilings do not change: they keep using what is left. No grant
for an allocation (field absent, 0, below what is left, or `observedAt` from before this month) keeps the earlier base,
so an older writer is unaffected, and an older controller ignores the field. `source: share` is unchanged.

**GPU utilisation (AC-69).** The batch script samples `nvidia-smi` every 30 s into the job output
(`KAOS_METRIC` lines). The runner keeps the latest and the highest value per GPU and writes them to
`status.run.gpuUtilization` (`[{gpu, percent, maxPercent}]`) with the log offset. While the attempt's site job
is `RUNNING` the controller exports them as
`kaos_hpc_job_gpu_utilization_percent{namespace,workflow,step,hpc_job,site_job,gpu}` and
`kaos_hpc_job_gpu_utilization_max_percent{...}` (`hpc_job` is the HPCArgoWorkflowJob name, `site_job` the
Slurm job id; `gpu` is the position in the node's `nvidia-smi` list). The series are dropped as soon as
the site job is no longer running, the job is terminal or deleted, so cardinality is bounded by the GPUs
running now. After a runner restart the highest value starts over.

Status for the console (`HPCSite.status.recentJobs`, PRD-HPC-1461 AC-26, AC-46, AC-76, AC-84): every active job of
the project on the site, plus the jobs that ended in the last 7 days, newest first (active first), at most 300
entries and at most 256 KiB of JSON (the end of the list, the oldest ended jobs, is dropped first). Each entry carries the job's
phase, reason and `message` (its `status.message`, cut to 512 characters). A typical entry is about 0.6 KiB, so
300 of them take about 170 KiB, well under the etcd object limit.

Controller resources: the chart default limit is 256Mi memory (it was 128Mi; the scale run measured 86 MB, which
left no headroom for the larger status lists); the request is unchanged (64Mi) as nothing measured a need for more.

## Serve mode (phase 1, PRD-HPC-1461 D-58, D-64)

An `HPCArgoWorkflowJob` with `spec.stepName` set is *served*: after admission the controller runs it
through a **runner Job per attempt** in the project's HPC namespace (the `HPCSite`'s namespace),
named `hpcr-<hash8(job uid)>-a<attempt>`. A job without `stepName` behaves as in phase 0.

One image holds everything; the entrypoint is chosen by `command`:

| Entrypoint | Who runs it |
|---|---|
| `/manager` | the controller (chart Deployment) |
| `/follower` | the credential-free pipeline step: creates the job (`hpc-<20 hex of sha256(WF_UID/NODE_KEY)>`, owned by the Workflow), prints changes (the lane and time limit while it waits, and one `HPC: dataset <repo>@<commit> staged (first copy | delta | already cached), <size>, <seconds>s` line from `status.run.staging`; see `docs/reasons.md`), writes `/tmp/outputs/<n>.json` and `/tmp/slurm-job-id`, exits 0 only on `Succeeded` |
| `python3 -u /runner/main.py` | the runner Job (source in `runner/`) |

The runner's environment is built **only by the controller**: from the spec (`IMAGE_REF`,
`STEP_COMMAND`, `SLURM_TIME_LIMIT`, `STEP_OUTPUTS`, `HPC_CLASS`, `STEP_NAME`, `WF_UID`, `DATASET_REPO`/`DATASET_VERSION`/`DATASET_COMMIT`/`DATASET_OWNER` (the dataset as the pipeline pinned it, from `spec.dataset`),
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

RBAC roles shipped by the chart (bound per namespace by whoever integrates the operator; in KAOS, kubecore-operator):

| ClusterRole | For | Verbs |
|---|---|---|
| `hpcargoworkflowjob-submitter-role` | pipeline service accounts (follower) | `hpcargoworkflowjobs`: create, delete, get, list, watch. **No status verbs.** |
| `hpcargoworkflowjob-runner-role` | `hpc-runner` in each env namespace | `hpcargoworkflowjobs`: get; `/status`: get, patch |
| `hpcargoworkflowjob-reporter-role` | **phase 0 only**: the in-pod runner's service account, next to the submitter role | `hpcargoworkflowjobs/status`: get, patch |
| `meluxina-snapshot` (KAOS: the project's snapshot job, created by kubecore-operator; not part of this chart) | the snapshot CronJob's service account in the HPC namespace | `hpcsites/status`: patch, of its own site only (`resourceNames`) |

`HPCSite.status.storage` (project scratch quota: space and files used/limit, `byCategory` datasets, images, checkpoints, work, other, `observedAt`/`measuredAt`) is **not written by this controller**: the project's snapshot job patches it every 10 minutes from the site stager's `usage` report. The controller's status patches are merge patches of its own fields and leave it alone.

`status.storage.images` (AC-65) splits the images category: SIFs, staged tars, half-written parts, SIFs without a label, what a queued or running job leases, the Apptainer layer cache, the largest 20 (app, step) lineages (`lineagesTotal` counts all), the images budget (`budgetBytes`, default 60 GB) with `overBudget`, and `lastCleanup` (time, bytes freed, count and bytes per reason). About 5 KB at most, so it stays far inside the object's 256 KiB cap. The same snapshot job runs the stager's `gc` before each report: it keeps only what can be used again (every SIF or tar a queued or running job names through the `kaos-image=` lease in its comment; the newest SIF of each app and step) and deletes the rest on the login node, with no node time and also when no job runs: superseded SIFs at once, a tar once its SIF exists or its job is gone, SIFs without a label after 3 days, layer-cache files after 1 day, and over the budget the least recently used SIFs nothing leases. Never what a queued or running job names; an unreadable queue deletes nothing. Details: `docs/extreme-cases.md` section 11.

Metrics (`:8443`, authn/authz-protected; the chart renders a `VMServiceScrape`, else a
`ServiceMonitor`, when the API exists): `kaos_hpc_job_phase`, `kaos_hpc_job_queue_seconds`,
`kaos_hpc_job_run_seconds`, `kaos_hpc_node_minutes_booked`, `kaos_hpc_node_minutes_settled`,
`kaos_hpc_site_node_minutes`, `kaos_hpc_active_jobs`, `kaos_hpc_runner_restarts_total`, `kaos_hpc_site_up`,
`kaos_hpc_lane_placements_total{qos,reservation,fallback}` (`fallback="true"` counts lanes the site refused),
`kaos_hpc_idle_kills_total{tier,project}` (HPC steps ended `Idle`), and the budget and GPU series described above
(`kaos_hpc_budget_warning`, `kaos_hpc_budget_percent`, `kaos_hpc_job_gpu_utilization_percent`,
`kaos_hpc_job_gpu_utilization_max_percent`). The in-cluster idle watchdog adds the two `kaos_k8s_idle_*` series below.

Controller flags (chart `controllerManager.container.args`, `runnerImage`, `runner.scheduling`, `lanes`, `idleWatchdog`):

| Flag | Default | What |
|---|---|---|
| `--runner-image` (`$RUNNER_IMAGE`) | the controller image | image of the runner Jobs (the chart sets it) |
| `--runner-scheduling` (`$RUNNER_SCHEDULING`) | like the controller | JSON `{nodeSelector, tolerations, affinity}` for runner pods |
| `--lane-candidates` (`$LANE_CANDIDATES`) | empty = lanes off | QoS and reservations tried in order for each attempt |
| `--site-poll-interval` (`$SITE_POLL_INTERVAL`) | 30 s | the batched `jobs/state` read per HPCSite; `0` turns the poller off (runners poll themselves, and the site-outage handling needs it) |
| `--site-rest-rate`, `--site-rest-burst` | 2/s, 5 | token bucket per site profile |
| `--site-outage-max` | 30 min | how long a job waits for an unreachable site before `Failed/SiteUnavailable` |
| `--site-missing-grace` | 15 min | how long a booked job survives its HPCSite or project label being gone |
| `--sweep-give-up` | 6 h | how long the orphan sweep waits for the site to confirm a job ended before settling without proof |
| `--ledger-publish-interval` | 1 s | shortest time between two writes of one HPCSite's status; `0` writes on every change |
| `--idle-metrics-url` and `--idle-*` | off | the in-cluster GPU idle watchdog (below) |
| `--leader-elect`, `--metrics-bind-address`, `--metrics-secure`, `--health-probe-bind-address` | off, `0`, true, `:8081` | the usual controller-runtime options (the chart passes `:8443` for metrics) |

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
- **Resumable steps share nodes (AC-73, reverses D-44's gate):** a resumable step may host or be a guest on a shared
  node. Slurm's warning signal is the batch job's and never reaches a guest, so the slot manager sends USR1 to each
  resumable step itself, `lead` before the end of the step's own limit or of the host's job, whichever is first
  (the host's own step included; a plain step is never signalled). A guest whose limit does not fit the host's remaining
  time is still taken when its lead plus a 5-minute margin does. A guest that saves and exits 85 is reported like a step
  alone and continued from its checkpoint, settled 0 (its host's booking covers the node); the continuation may host,
  guest or run alone. A resumable host's end is reported when its Slurm job ends, so the node time it held is settled to
  that end, and its job is not padded to twice its step. Details and tests: `docs/extreme-cases.md` 4.13-4.18.
- **Steps longer than the longest lane (D-67, D-74):** a resumable step whose declared `walltime` is above the class or
  site maximum is admitted and run as continuations of at most that maximum each (`status.placement.timeLimitMinutes`,
  one run booked at a time), up to the continuation cap + 1 runs in all; the follower prints "declared N min runs as
  continuations of up to M min each". A step that is not resumable is refused at admission (`TooLongNotResumable`).
  The `long` QoS (6 days, one job per user) is not used for this: `placement="long"` (D-71) is not built.
- **Reported:** `status.run.checkpoint` (progress, size of the newest copy, the step's and the project's total,
  why a copy was skipped) and `HPCSite.status.checkpoints` (scratch held by steps that have not succeeded).
- **Resumable steps, for the pipeline linter:** `HPCSite.status.resumableSteps` lists, per app and step with evidence
  (at most 100), whether the latest continuation got past its checkpoint (`verified`), when (`since`) and why not
  (`reason`, for example `NoProgress: ...`). The app is the last path segment of the step's image repository. Only names,
  a verdict, a time and a reason: no job details. The latest evidence decides, so a later `NoProgress` clears an earlier
  verification. The hub copies it into the app's platform context.

## Idle watchdog for in-cluster GPU steps (PRD-HPC-1461 D-72, D-76)

The same binary can end idle Kubernetes GPU steps on the pool: GPU use below 5 % and no log output
for 20 minutes (per step `platform.kubecore.io/idle-timeout`, minutes, 0 = off). Enable it with the
chart value `idleWatchdog.metricsUrl` (the pool's PromQL API); it then also installs a ClusterRole
to patch the status of and delete Argo workflow pods that hold a GPU. Design, metric sources and the
interplay with the template's re-run: [internal/idlewatch/README.md](internal/idlewatch/README.md).
Metrics: `kaos_k8s_idle_kills_total`, `kaos_k8s_idle_watchdog_errors_total`.

## KAOS integration (optional)

The controller's own logic (admission, ledger, settlement, continuation) does not depend on KAOS. When the operator runs
inside KAOS:

- `kubecore-operator` renders each project's `HPCSite` (site profile, identity, budget, limits) and labels the
  project's environment and HPC namespaces `hpc.kaos.io/project`; tenants cannot set the label.
- It binds the chart's `hpcargoworkflowjob-*` roles per namespace and runs the project's snapshot job that writes
  `HPCSite.status.storage`. The `hpc-runner-config` ConfigMap and the `meluxina-jwt`, `meluxina-staging` and
  `mlflow-svc` Secrets the runner Jobs mount are provided in the project's HPC namespace by the same integration;
  this operator only reads them.
- The pipeline template's `step(...)` creates the `HPCArgoWorkflowJob` through the follower; `resumable`, `idle_timeout`
  and the Argo `platform.kubecore.io/idle-timeout` annotation are its names for fields and settings documented here.
- The budget figures of an `HPCSite` come from the project's KAOS configuration or from the site's own accounting
  (`budget.source: site`); that rendering is the integrator's job, and a figure per allocation must be written (see
  "One budget per allocation").

## Development

```sh
make test       # unit tests and envtest (a real API server)
make lint       # golangci-lint
make test-engine lint-engine  # the pipeline engine's Python tests and ruff (also run by building engine/Dockerfile.engine)
make engine-corpus  # render every historical template state and the lab apps with the engine (needs network)
make manifests  # CRDs and RBAC from the Go types
make build      # the manager binary
make scale-test # load test of the real controller against fake Slurm sites (about 10 minutes, test/scale/README.md)
```

Requires Go 1.24+ (`make test-engine` also needs Python 3.12 and network).

`engine/` is the platform pipeline engine (D-79): the template `kubecore/` package CI renders every ML app's
pipeline.py with, so a platform change never needs a client commit. It is released at this operator's version;
see engine/README.md.

## Licence

Apache License 2.0. See [LICENSE](LICENSE).

## Install

The Helm chart is in `dist/chart` (CRDs, RBAC, the controller; `values.yaml` is commented). Set the image you built:

```bash
helm install kaos-hpc-operator dist/chart -n kaos-hpc-system --create-namespace \
  --set controllerManager.container.image.repository=<registry>/kaos-hpc-operator
```

### Standalone: no KAOS needed

The controller imports no KAOS internals and the chart assumes none (PRD-HPC-1461 AC-41). It installs
into a plain Kubernetes cluster: no KAOS CRDs, no Crossplane, no cert-manager, no Argo Workflows (the
Workflow of a job is read only when `spec.workflow` names one). Every chart value has a neutral default;
the one required value is the image repository. What a KAOS install renders around the controller you
supply yourself in a standalone cluster, per project:

| object | where | purpose |
|---|---|---|
| two namespaces labelled `hpc.kaos.io/project: <name>` | cluster | the job's namespace and the project's HPC namespace |
| `HPCSite` | HPC namespace, same label | the site (an `https://` slurmrestd endpoint), classes, identity, budget |
| ServiceAccount `hpc-runner`, ConfigMap `hpc-runner-config`, Secrets `meluxina-jwt` (key `token`) and `mlflow-svc` (key `ZITADEL_MACHINE_KEY`) | HPC namespace | what the runner Job of a served job mounts |
| RoleBinding of `hpcargoworkflowjob-runner-role` to `hpc-runner` | job namespace | lets the runner read the job and report its site job |

If the site's certificate is not signed by a public CA, put its CA (`*.crt`) in `certs/` before building the
image: it is added to the trust store at build time (the runner Jobs run the same image).

`make test-e2e` proves it on a plain Kind cluster: it installs this chart with Helm, runs the fake Slurm
site of the scale test (`test/fakeslurmd`) as a Deployment, creates an `HPCSite` and a served
`HPCArgoWorkflowJob`, and asserts the job reaches `Succeeded` through a real runner Job. It needs Docker,
Kind, kubectl and helm. CI (Argo, no Docker) runs the same suite with prebuilt images against a vcluster:
`make test-e2e-cluster` (see the Makefile for `E2E_IMAGE`, `E2E_FAKESLURM_IMAGE`, `E2E_PKI_DIR`). `go test ./test/importrule` (part of `make test`, so of the Argo
`kaos-hpc-operator-validate` and `-build` workflows) fails when any package, test or harness of the module
imports a KAOS-internal module (`novelcore/...`, `github.com/novelcore/...`, any other
`github.com/kaos-io/...` module).

The chart also installs `hpcargoworkflowjob-submitter-role`: bind it to the service account of
the workflow steps that run on HPC, in each namespace whose workflows may use it.

After an API change run `make generate manifests`: it regenerates the CRDs in `config/crd/bases` from the Go type
comments (these comments are the field documentation `kubectl explain` shows) and `hack/sync-chart-crds.sh` copies
them into `dist/chart/templates/crd`. RBAC rules are copied by hand from `config/rbac` into `dist/chart/templates/rbac`
(the chart carries hand-written templates for the runner, lanes and idle watchdog, so it is edited in place and not
regenerated). `make test` fails while the chart's CRDs or RBAC differ from `config/`.

