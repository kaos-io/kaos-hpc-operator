# What expires in the cluster, and the cleanup on the site (PRD kaos#1911)

## In the cluster

| What | Expires | Where it is set |
|---|---|---|
| The record of a pipeline run (the Workflow made from the delivered `{app}-pipeline` WorkflowTemplate) | **35 days** after it ends, succeeded or failed (`ttlStrategy`) | the engine puts `ttlStrategy` on the delivered template (`enhance_spec_top_level`) unless the author set it; the pipeline context may carry `runRetentionDays` (1 to 3650) for one project, but not below 32 for a pipeline with a step that can run on HPC |
| The pods of a run | kept 7 days after the run ends, succeeded or failed, then deleted: `podGC: {strategy: OnWorkflowCompletion, deleteDelayDuration: 168h}` (a step's log lives only in its pod until logs are stored; `deleteDelayDuration` needs Argo Workflows 3.5, the platform runs 4.x) | same place |
| A finished `HPCArgoWorkflowJob` | goes with its Workflow (the follower makes the Workflow its owner, so garbage collection deletes it), or 35 days after it ended and after the budget period it ended in has closed: `--job-retention` (840 h), chart `jobRetention` (`0` keeps them) | `internal/controller/expire.go` |
| Runner Jobs | 1 hour after they finish | `internal/runnerjob` (unchanged) |

**Why 35 days and not 30, and why the HPC budget decides it.** The HPC budget period is a calendar month in UTC
(`ledger.PeriodStart`; a source-site budget counts from the site's `observedAt` instead, `ledger.CountedFrom`; the site has no
other period length). The budget ledger is derived from the job objects (`internal/ledger`): what a project has spent is the sum
of the settled attempts the objects still hold. So no object may be deleted during the period its last attempt ended in. A run
that ends at time t is collected at t + TTL; the worst case is a run ending in the first second of a 31-day month, which closes 31
days later, so the TTL must be more than 31 days: 35 gives a margin, 30 did not (`engine/tests/template/test_run_retention.py`,
`test_the_default_outlives_every_calendar_month_and_thirty_one_days_would_not`). The owner ruled "30 days, settable per
project"; 35 is a deliberate deviation of five days for this reason. A pipeline with a step that can run on HPC (the project has
HPC classes and a step is not an ingest, publish, control or dataset-producing step) cannot be set below 32 days: "runs with HPC
steps are kept at least 32 days, so the month's HPC spend stays counted". A pipeline with no such step may go down to 1.

The controller's own expiry applies the same rule per object: a finished job is deleted only when it is terminal, has no sweep
and no open host attempt, ended more than `--job-retention` ago and ended before `ledger.CountedFrom(site, now)`; otherwise it is
skipped until the next resync (no requeue to the end of the month, no finalizer, no stored state). Nothing holds a finished job,
so a namespace delete, a Workflow garbage collection or a `helm uninstall` with the controller gone completes at once
(`internal/controller/teardown_envtest_test.go`). A job object that holds the old `hpc.kaos.io/budget` finalizer from an earlier
build of this branch loses it at its next reconcile.

A person who deletes a run by hand inside the month removes its spend from the ledger; that is the behaviour today and is not
guarded.

**Which default wins.** The pipeline run's `ttlStrategy` and `podGC` are set on the delivered template. A pool's Argo controller
has no `ttlStrategy`/`podGC` default (kubecore-operator `compositions/apis/kubepool/clusteroperators/composition.yaml`
`workflowDefaults`, ~3644-3657: tolerations only). The dev hub's controller does (kaos `kubetools/system/argo-workflows/values/dev.yaml:34-38`:
`ttlStrategy.secondsAfterCompletion: 86400`, `podGC.strategy: OnWorkflowSuccess`; the release controller
`argo-workflows-release/values/base.yaml:73-78` has `OnPodCompletion` and 86400). Argo merges `workflowDefaults` field by field
under the template and the workflow (Workflow > WorkflowTemplate > defaults; kaos `scripts/apps-config/tests/test_argo_workflows_gc.py`
cites `workflow/util/merge.go JoinWorkflowSpec`), and its GC prefers `secondsAfterSuccess`/`secondsAfterFailure` over
`secondsAfterCompletion`. The engine sets both per-phase fields and `podGC.strategy`, so the template wins on every controller.

Step logs are not kept after a run's pods are collected: [issue #76](https://github.com/kaos-io/kaos-hpc-operator/issues/76).

## On the site

The site's staging tool (shipped from the platform repository, `images/meluxina-tokenmint/stager`) has the verb `clean`;
the project's `hpc-cleanup` form and its suspended schedule call it over the staging key. What accumulates there, what
removes it today and what `clean` adds is in that repository, `docs/mlops/hpc-cleanup.md`. This repository's part is
what the job script and the runner write, all under the platform project's own root on scratch
(`<scratch>/kaos/<project>`, the job script's `SCR`; the layout and who owns what are in `docs/job-environment.md`):
`kaos-work/<job>`, `kaos-tmp/in-<job>`, `kaos-tmp/idle-<job>`,
`kaos-priv/host-<job>`, `kaos-ckpt/<workflow uid>/<step>/<seq>.tar` and the `kaos-ckpt-lease=` / `kaos-commit=` /
`kaos-image=` words of the job comment, which are what `clean` reads to leave a queued or running job alone. Changing
one of those names or words changes what `clean` can match: the stager's name patterns and tests name them.
