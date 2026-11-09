# Reason catalogue

Every `Reason` the controller writes, what it means, and what the person who submitted the step sees. The
constants live in `api/v1alpha1/hpcargoworkflowjob_types.go` and `hpcsite_types.go`; a test-free way to see one is
`kubectl get hpcargoworkflowjobs` (columns `Phase` and `Reason`) and `kubectl describe` (the `Message`, the conditions and the events).

"The follower" is the credential-free pipeline step that creates the job and prints its progress; what it prints
is shown as *Follower*. Where the HPC job is shown on a console (`HPCSite.status.recentJobs`), phase and reason are
copied from the job.

Phases: `Pending` (not judged yet, or the next attempt is about to be judged), `Waiting` (admissible later), `Refused`
(never admissible as asked; terminal), `Admitted` (booked), `Running`, and the terminal `Succeeded`, `Failed`,
`Cancelled`. A `Refused` job was never booked. A job that ended is settled to what the site reports it used.

## Admission (before anything is submitted)

| Reason | Phase | Meaning | What the user sees |
|---|---|---|---|
| `Admitted` | Admitted | The job fits its allocation's budget. Nodes x wall time is booked. | Follower and `kubectl`: "booked N gpu node-minutes" (the message names the allocation: GPU, CPU, FPGA or large memory), with "beyond the project's share (policy warn)" when the policy is `warn` and it overspends. With `warn` an `OverShare` Warning event is raised. |
| `WaitingForBudget` | Waiting | It fits the project's limit this month but not what is free while running jobs hold their bookings. It starts when they settle. | Message: "needs N <allocation> node-minutes; M are free on the <allocation> allocation until running jobs settle". |
| `ConcurrencyLimit` | Waiting | The project is at `spec.limits.maxConcurrentJobs`. The job waits for a slot, it is never refused for this. | Message: "the project runs N jobs, its limit; this one waits for a slot". |
| `SubmitLimit` | Refused | The site caps a user's queued plus running jobs (`spec.profile.maxSubmitJobs`) and the project's own site jobs already fill it. Guests on a shared node hold no Slurm job and are not counted. Fails at once, never waits. Checked after the concurrency limit. | Message with the cap and the current count; resubmit when some jobs finish. See also the Failed form below. |
| `BudgetBlocked` | Refused | It can never fit this period within the project's own share (policy `block`). Only the allocation it spends is named; the project's other allocations are not affected. | Message: "needs N <allocation> node-minutes; the project's share has M left on the <allocation> allocation this period after what was used; steps on the project's other allocations are not affected". |
| `SiteLimit` | Refused, or shown while a submitted job queues | The site's own remaining limit on the account is what runs out, not the project's share. Also shown for a submitted job queued behind it (Slurm `AssocGrp...Minutes`). | Refused: as `BudgetBlocked` but "the site reports the account has M left". Queued: `queue reason: SiteLimit: <text>` in the follower and the reason in `recentJobs`. |
| `TooLongNotResumable` | Refused | The declared wall time is above the longest run the class or site allows and the step is not resumable (declared or verified, and not opted out), so it would be cut. | Message: the declared limit, the maximum, and the two fixes: make it resumable (write checkpoints into `KAOS_CHECKPOINT_DIR`) or shorten it. Nothing is booked or submitted. |
| `TooLongBeyondContinuations` | Refused | The step is resumable, but its declared wall time needs more runs than the continuation cap allows (cap + 1 runs of the longest run). | Message: the runs, the total minutes available, and "shorten it or raise maxContinuations". |
| `WallTimeOverLimit` | Refused | The wall time is above a limit and no longer run is possible: the class or site limit is unknown or zero. | Message: "wall time N min is above the limit of M min". |
| `MultiNodeUnsupported` | Refused | `spec.nodes` above 1: a job runs on one node. | Message with the number asked for. |
| `ClassNotAllowed` | Refused | The class is not in the project's `spec.allowedClasses`. | Message naming the class. |
| `NoSite` | Refused | The project's HPCSite offers no such class, or the project has no HPCSite offering it. | Message naming the class (and the project). |
| `NoProject` | Refused | The job's namespace has no `hpc.kaos.io/project` label. | Message. |
| `InvalidSpec` | Refused | The spec asks for something the API does not allow: an `env` name outside the allowlist (the CRD refuses it too). | Message naming the variable. |
| `HPCSiteMissing` | unchanged while it runs, then `Cancelled` | A booked job's HPCSite (or its namespace's project label) is gone. The job and its booking are kept for a grace period (`--site-missing-grace`, default 15 min); if it is still missing the job is cancelled, and when the HPCSite comes back the reason returns to `Admitted`. | Message "HPCSiteMissing since <time>: ...; the job is cancelled if it stays missing for 15m0s"; `HPCSiteMissing` Warning event. |

## A site that does not answer

| Reason | Phase | Meaning | What the user sees |
|---|---|---|---|
| `SiteUnavailable` | Waiting, Admitted, or Failed | The site's REST API does not answer (connection errors, 5xx, timeouts, slurmctld down) for at least 90 s, confirmed with a ping. While it is down nothing is admitted or booked and no runner starts: a wait, not a failure. After `--site-outage-max` (default 30 min) the job ends `Failed` with this reason. Also the pending reason a runner shows while it waits. | Follower: the wait and its reason. Job condition `SiteUnavailable` (its time is when the wait began), `SiteUnavailable` Warning event, `HPCSite` condition `SiteReachable` False and metric `kaos_hpc_site_up` 0. |
| `RunnerFailed` | Failed | The runner Job ended (its retries used up, or its deadline passed) without a terminal report, so the site job's result is unknown. If a site job may exist, the orphan sweep first has a cancel-only runner stop it and report the site's state, then the attempt is settled. | Message with the runner Job's failure and its last pod message. |

## Interruptions, checkpoints and continuations

| Reason | Phase | Meaning | What the user sees |
|---|---|---|---|
| `Continuing` | Pending | The attempt ended on exit 85, wall time, node failure, pre-emption or an idle kill and left a verified checkpoint copy; the next attempt starts from it. Counted apart from failure retries. | Follower: "continuing from the last checkpoint as attempt N". |
| `ContinuationLimit` | Failed | The step used all its continuations (`spec.resumable.maxContinuations`, default 5). | Message: "the step reached its N continuations". |
| `NoProgress` | Failed | A continuation did not get past the checkpoint it started from (same progress values or folder digest), or ended without saving any. The step is recorded as not verified. | Message with both progress values. |
| `NotResumable` | Failed | The step exited 85 ("saved, continue me") but opted out of resuming (`resumable=False`). | Message. |
| `Timeout` | Failed | Historical name of "checkpoint missing": exit 85 but no verified checkpoint copy exists to continue from. Nothing is ever continued from scratch. | Message: "checkpoint not saved: no verified copy of the checkpoint folder to continue from". |
| `RerunNodeFailure`, `RerunPreempted` | Pending | A step with no checkpoint was interrupted by a node failure (`NODE_FAIL`, `BOOT_FAIL`, `REVOKED`, maintenance) or by pre-emption and is re-run from the start, at most twice (`status.reruns`). Never after the step's own error, an out-of-memory kill, a cancel or a wall-time kill. | Follower: "re-running from the start (n/2)". |
| `RerunLimit` | Failed | The step was interrupted again after two re-runs. | Follower: "stopped: the step was interrupted again after 2 re-runs". |
| `WallTimeReached` | Failed | The step ran to its time limit (Slurm `TIMEOUT`) and was not continued. | Message in plain words (the limit, and what to do), and a `WallTimeReached` Warning event. |
| `TimeLimitRetry` | Pending | The attempt hit a learned time limit (below the declared cap); it runs once more at twice the limit, up to the declared cap. | Follower: "hit its learned time limit; retrying once with the full limit". |
| `Idle` | Failed | The on-node idle watchdog ended the step: GPU (CPU on a CPU class) use below 5 % and no log growth for `spec.idleTimeoutMinutes` (default 20; 0 = off). Exit 236. The node-minutes it saved are recorded. | Follower: "stopped: no GPU use and no output for N min", then the step's last lines. `Idle` Warning event, `kaos_hpc_idle_kills_total`. |
| `InsufficientQuota` | Failed | Staging the dataset or image would exceed the project's scratch quota (space or files) even after evicting everything unused. The runner stops before any copy and submits nothing (exit 237, no site job). | Message: what did not fit, "nothing was copied and nothing in use was deleted", free space or raise the quota. `InsufficientQuota` Warning event. |
| `SubmitLimit` | Failed | The site itself refused a submit for its per-user cap (`QOSMaxSubmitJobPerUserLimit`, `AssocMaxSubmitJobLimit`, ...). The runner reports exit 238 with the cap and the count it read from the site. See the Refused form above. | Message with the cap and the count, "resubmit when the count is below the cap". `SubmitLimit` Warning event. |

Other final states in `status.reason`: `Completed` (Succeeded), `Cancelled` (Cancelled), `Continued` (attempt record
outcome only), `Ended` (the site reported no state), and `Site<STATE>` (the site's own state of an attempt that ended
without one of the reasons above, for example `SiteFAILED` for the step's own non-zero exit or `SiteOUT_OF_MEMORY`).

## Cancelling

A cancel (the job deleted, or its Argo Workflow deleted, shut down or ended while the job had not) never releases the
booking on the controller's word alone. The controller starts a cancel-only runner that adopts the site job by its
name, cancels it, and reports the site's final state. Events: `SweepStarted`; `SiteJobUnconfirmed` (a Warning, when
`--sweep-give-up` (default 6 h) passed without the site's confirmation: the attempt is settled up to now and the job
may still run). See [extreme-cases.md](extreme-cases.md) 2.13 and 2.17.

## Budget warning (HPCSite)

| Condition / reason | Meaning |
|---|---|
| `BudgetWarning` True, reason `ThresholdCrossed` | For some allocation, used plus booked is at or above `budget.warnAtPercent` (default 80) of what the project may spend this month. Raised whatever `budget.atLimit` is. A `BudgetWarning` Warning event on the HPCSite; metrics `kaos_hpc_budget_warning` and `kaos_hpc_budget_percent`. |
| `BudgetWarning` False, reason `BelowThreshold` | Below it, for every allocation that may spend anything. |
| `SiteReachable` False, reason `NotAnswering` | The site's REST API has not answered for at least 90 s. |
| `SiteReachable` True, reason `Answering` | It answers. |

## Not a `status.reason`

- `GPUViolation`: on a shared node, a step computed on a GPU outside its own set and the slot manager stopped that
  step alone (AC-57 / D-29). It is written into the host job's Slurm comment (`kaos-state=done rc=N
  reason=GPUViolation`) and the job log (`gpu: step ... outside its set: stopping it`), not into the status of an
  HPC job. Neighbouring steps keep running. Prevention first: where `nvidia-container-cli` works the step cannot see
  the other GPUs (`gpu fence: nvccli` in the job log).
- Events not tied to a reason constant: `LaneRefused` (the site refused the lane; the attempt ran on the class
  default), `HostSettledToJobEnd`, `ReportedTimeDiscarded` (implausible site times were ignored in settlement),
  `RunnerStartBlocked` (the runner Job cannot start, with the cause).
