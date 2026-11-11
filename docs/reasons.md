# Reason catalogue

Every `Reason` the controller writes, what it means, and what the person who submitted the step sees. The
constants live in `api/v1alpha1/hpcargoworkflowjob_types.go` and `hpcsite_types.go`; a test-free way to see one is
`kubectl get hpcargoworkflowjobs` (columns `Phase` and `Reason`) and `kubectl describe` (the `Message`, the conditions and the events).

"The follower" is the credential-free pipeline step that creates the job and prints its progress; what it prints
is shown as *Follower*. Where the HPC job is shown on a console (`HPCSite.status.recentJobs`), phase, reason and message are
copied from the job (the message cut to 512 characters).

Phases: `Pending` (not judged yet, or the next attempt is about to be judged), `Waiting` (admissible later), `Refused`
(never admissible as asked; terminal), `Admitted` (booked), `Running`, and the terminal `Succeeded`, `Failed`,
`Cancelled`. A `Refused` job was never booked. A job that ended is settled to what the site reports it used.

## Admission (before anything is submitted)

| Reason | Phase | Meaning | What the user sees |
|---|---|---|---|
| `Admitted` | Admitted | The job fits its allocation's budget. Nodes x wall time is booked. | Follower and `kubectl`: "booked N gpu node-minutes" (the message names the allocation: GPU, CPU, FPGA or large memory), with "beyond the project's share (policy warn)" when the policy is `warn` and it overspends. With `warn` an `OverShare` Warning event is raised. |
| `WaitingForBudget` | Waiting | It fits the project's limit this month but not what is free while running jobs hold their bookings. It starts when they settle. | Message: "needs N <allocation> node-minutes; M are free on the <allocation> allocation until running jobs settle". |
| `ConcurrencyLimit` | Waiting | The project is at `spec.limits.maxConcurrentJobs`. The job waits for a slot, it is never refused for this. | Message: "the project runs N jobs, its limit; this one waits for a slot". |
| `SubmitLimit` | Refused | The site caps a user's queued plus running jobs (`spec.profile.maxSubmitJobs`) and the project's own site jobs already fill it. Every step counts, a guest on a shared node too: its own Slurm job stays queued, held by its host, until the host cancels it with the step's result. Fails at once, never waits. Checked after the concurrency limit. | Message with the cap and the current count; resubmit when some jobs finish. See also the Failed form below. |
| `BudgetBlocked` | Refused | It can never fit this period within the project's own share (policy `block`). Only the allocation it spends is named; the project's other allocations are not affected. | Message: "needs N <allocation> node-minutes; the project's share has M left on the <allocation> allocation this period after what was used; steps on the project's other allocations are not affected". |
| `SiteLimit` | Refused, or shown while a submitted job queues | The site's own remaining limit on the account is what runs out, not the project's share. Also shown for a submitted job queued behind it (Slurm `AssocGrp...Minutes`). | Refused: as `BudgetBlocked` but "the site reports the account has M left". Queued: `queue reason: SiteLimit: <text>` in the follower and the reason in `recentJobs`. |
| `TooLongNotResumable` | Refused | The declared wall time is above the longest run the class or site allows and the step is not resumable (declared or verified, and not opted out), so it would be cut. | Message: the declared limit, the maximum, and the two fixes: make it resumable (write checkpoints into `KAOS_CHECKPOINT_DIR`) or shorten it. Nothing is booked or submitted. |
| `TooLongBeyondContinuations` | Refused | The step is resumable, but its declared wall time needs more runs than the continuation cap allows (cap + 1 runs of the longest run). | Message: the runs, the total minutes available, and "shorten it or raise maxContinuations". |
| `WallTimeOverLimit` | Refused | The wall time is above a limit and no longer run is possible: the class or site limit is unknown or zero. | Message: "wall time N min is above the limit of M min". |
| `MultiNodeUnsupported` | Refused | `spec.nodes` above 1: a job runs on one node. | Message with the number asked for. |
| `ClassNotAllowed` | Refused | The class is not in the project's `spec.allowedClasses`. | Message naming the class. |
| `NoSite` | Refused | The project's HPCSite offers no such class, or the project has no HPCSite offering it. | Message naming the class (and the project). |
| `InvalidImage` | Refused, or Failed | The step's image is not `<registry-host>/<repository>:<tag>` (or `@sha256:` digest), so it can never be pulled (live 2026-11-10: a half-rendered `:probe-learn-latest`). Refused at admission with nothing booked and no runner; or Failed when the runner finds it (exit 239, no site job, settled 0). No credential retry is made for it: the fresh-credentials resubmit applies to a failed pull (exit 231) of a valid reference only. | Message: the reference and what is missing; `InvalidImage` Warning event on the Failed form. |
| `NoProject` | Refused | The job's namespace has no `hpc.kaos.io/project` label. | Message. |
| `InvalidSpec` | Refused | The spec asks for something the API does not allow: an `env` name outside the allowlist (the CRD refuses it too). | Message naming the variable. |
| `HPCSiteMissing` | unchanged while it runs, then `Cancelled` | A booked job's HPCSite (or its namespace's project label) is gone. The job and its booking are kept for a grace period (`--site-missing-grace`, default 15 min); if it is still missing the job is cancelled, and when the HPCSite comes back the reason returns to `Admitted`. | Message "HPCSiteMissing since <time>: ...; the job is cancelled if it stays missing for 15m0s"; `HPCSiteMissing` Warning event. |

## Node sharing plan (not a reason: `status.placement.share`)

| Value | Meaning | What the user sees |
|---|---|---|
| `share.role: host` | The step opens a node: its Slurm job starts at once (after the offer window) in the lane the plan names, `share.jobLimitMinutes` long. Its slot manager takes guests. | `kubectl get -o yaml`: `placement.share`. |
| `share.role: guest`, `share.host` | The step is planned onto that host's node. Its Slurm job is deferred (BeginTime 12 h ahead) and never starts on a node of its own; the host takes it. | The follower shows it waiting like a queued job. |
| `ShareReplanned` event | A guest no host took was planned again: its host ended, did not start in 20 min, no longer has the time, or did not take it in 10 min. | Event message: why, and whether it joined another host or became one. |

## A site that does not answer

| Reason | Phase | Meaning | What the user sees |
|---|---|---|---|
| `SiteUnavailable` | Waiting, Admitted, or Failed | The site's REST API does not answer (connection errors, 5xx, timeouts, slurmctld down) for at least 90 s, confirmed with a ping. While it is down nothing is admitted or booked and no runner starts: a wait, not a failure. After `--site-outage-max` (default 30 min) the job ends `Failed` with this reason. Also the pending reason a runner shows while it waits. | Follower: the wait and its reason. Job condition `SiteUnavailable` (its time is when the wait began), `SiteUnavailable` Warning event, `HPCSite` condition `SiteReachable` False and metric `kaos_hpc_site_up` 0. |
| `RunnerFailed` | Failed | The runner Job ended (its retries used up, or its deadline passed) without a terminal report, so the site job's result is unknown. If a site job may exist, the orphan sweep first has a cancel-only runner stop it and report the site's state, then the attempt is settled. | Message with the runner Job's failure and its last pod message. |

## Interruptions, checkpoints and continuations

| Reason | Phase | Meaning | What the user sees |
|---|---|---|---|
| `Continuing` | Pending | The attempt ended on exit 85, wall time, node failure, pre-emption or an idle kill and left a verified checkpoint copy; the next attempt starts from it. Counted apart from failure retries. A resumable step on a shared node continues the same way: a guest that was signalled before its host's job ended, saved and exited 85 is `Continuing` with 0 node-minutes settled for that attempt (its host's booking covers the node); a resumable host's checkpointed end is reported when its Slurm job ends and settled to that end (AC-73). | Follower: "continuing from the last checkpoint as attempt N". |
| `ContinuationLimit` | Failed | The step used all its continuations (`spec.resumable.maxContinuations`, default 5). | Message: "the step reached its N continuations". |
| `NoProgress` | Failed | A continuation did not get past the checkpoint it started from (same progress values or folder digest), or ended without saving any. The step is recorded as not verified. | Message with both progress values. |
| `NotResumable` | Failed | The step exited 85 ("saved, continue me") but opted out of resuming (`resumable=False`). | Message. |
| `Timeout` | Failed | Historical name of "checkpoint missing": exit 85 but no verified checkpoint copy exists to continue from. Nothing is ever continued from scratch. | Message: "checkpoint not saved: no verified copy of the checkpoint folder to continue from". |
| `RerunNodeFailure`, `RerunPreempted` | Pending | A step with no checkpoint was interrupted by a node failure (`NODE_FAIL`, `BOOT_FAIL`, `REVOKED`, maintenance) or by pre-emption and is re-run from the start, at most twice (`status.reruns`). Never after the step's own error, an out-of-memory kill, a cancel or a wall-time kill. | Follower: "re-running from the start (n/2)". |
| `RerunLimit` | Failed | The step was interrupted again after two re-runs. | Follower: "stopped: the step was interrupted again after 2 re-runs". |
| `WallTimeReached` | Failed | The step ran to its time limit (Slurm `TIMEOUT`) and was not continued. On a shared node a step's own limit stops it with `srun --time`, not as a Slurm job `TIMEOUT`: the slot manager then writes `reason=TIMEOUT` into the job comment and the runner reports the attempt as `TIMEOUT`, the same as for a job alone (AC-72; live 2026-11-10, guest 5439976). | Message in plain words (the limit, and what to do), and a `WallTimeReached` Warning event. |
| `TimeLimitRetry` | Pending | The attempt hit a learned time limit (below the declared cap); it runs once more at the declared limit (D-67). A guest on a shared node stopped by its own learned limit is retried the same way. | Follower: "hit its learned time limit; retrying once with the full limit". |
| `Idle` | Failed | The on-node idle watchdog ended the step: GPU (CPU on a CPU class) use below 5 % and no log growth for `spec.idleTimeoutMinutes` (default 20; 0 = off). Exit 236. The node-minutes it saved are recorded. | Follower: "stopped: no GPU use and no output for N min", then the step's last lines. `Idle` Warning event, `kaos_hpc_idle_kills_total`. |
| `InsufficientQuota` | Failed | Staging the dataset or image would exceed the project's scratch quota (space or files) even after evicting everything unused. The runner stops before any copy and submits nothing (exit 237, no site job). | Message: what did not fit, "nothing was copied and nothing in use was deleted", free space or raise the quota. `InsufficientQuota` Warning event. |
| `StagingMismatch` | Failed | The runner and the site's staging tool speak different staging contracts: the two ship separately and one was updated first. The runner asks the tool before it stages; on any difference it stages nothing and submits nothing (exit 241, no site job, settled 0), because a copy made under another contract may hold only part of the dataset. Not a fallback to the in-job copy. | Message: both contract numbers, "nothing was staged and nothing was submitted", run again when the update is complete. `StagingMismatch` Warning event. |
| `ReservedName` | Failed | The dataset version holds an object whose name is reserved for the platform. On the site the markers of a copy live at its root under `.kaos-` (`.kaos-ready`, `.kaos-complete`, `.kaos-commit`, ...) and the staging tool's half-downloaded files end in `.kaos-part`; a dataset object with such a name would be taken for a marker or refused mid-copy. The runner looks for a root name starting with `.kaos-` before it stages (one small listing, whether the copy is made from the login node or in-job) and for the suffix while it plans the copy; it stages nothing and submits nothing (exit 242, no site job, settled 0). The in-job stage-in checks the root names again and ends the job 232 before any download. The `.kaos/` folder is the platform's own inside a dataset and is copied like any other. The same reason covers an object whose path could be written outside the dataset's folder on the site: a `..`, `.` or empty part, a leading `/`, a NUL byte. With the staging key every name is checked while the copy is planned; when the job is about to stage in-job (no key, no ssh client, a staging tool not reached within the budget) the runner lists the commit once before it submits. The in-job stage-in applies the same rule before any download and never writes through a folder of its copy that is a symbolic link to elsewhere. | Message: the dataset, the object's name, and the rule it breaks ("names starting with .kaos are reserved for the platform", or that the path could be written outside the dataset's folder), rename or remove it and commit a new version. `ReservedName` Warning event. |
| `SubmitLimit` | Failed | The site itself refused a submit for its per-user cap (`QOSMaxSubmitJobPerUserLimit`, `AssocMaxSubmitJobLimit`, ...). The runner reports exit 238 with the cap and the count it read from the site. See the Refused form above. | Message with the cap and the count, "resubmit when the count is below the cap". `SubmitLimit` Warning event. |
| `GPUViolation` | Failed | On a node shared by several steps (AC-57 / D-29), the step computed on a GPU outside its own set and the slot manager stopped that step alone; neighbouring steps keep running. The slot manager writes `kaos-state=done rc=N reason=GPUViolation` into the host job's Slurm comment and `gpu: step ... outside its set: stopping it` into the job log; the runner reports exit 240 (live 2026-11-11, job 5447505, read `SiteFAILED` exit 137 before). Prevention first: where `nvidia-container-cli` works the step cannot see the other GPUs (`gpu fence: nvccli` in the job log). | Reason `GPUViolation`; the guest settles 0 node-minutes (its host pays). |

Other final states in `status.reason`: `Completed` (Succeeded), `Cancelled` (Cancelled), `Continued` (attempt record
outcome only), `Ended` (the site reported no state), and `Site<STATE>` (the site's own state of an attempt that ended
without one of the reasons above, for example `SiteFAILED` for the step's own non-zero exit or `SiteOUT_OF_MEMORY`).
A guest on a shared node stopped by anything other than its own time limit (exit 143 before its limit, a memory or
GPU kill) stays `SiteFAILED` and is not retried.

A host on a shared node (D-26) is `Succeeded` or `Failed` when its step ends, but while its Slurm job still holds the
node for guests the message ends "its Slurm job still holds the node for guests: the booking is held until that job
ends" and the booking stays in the ledger. When the runner reports the job's end (`status.run.hostJobEndTime`) the
attempt is settled to it and the message reads "attempt N settled M node-minutes (its Slurm job held the node until
<time>)". `status.run.endTime` stays the step's own end: a learned time limit comes from the step's run time, never
from the time the node was kept for guests (D-67).

## Queue place lost (not a reason: `status.run.queueReplaced`)

A queued site job whose token would expire before it starts is replaced by a fresh one with fresh credentials; its
place in the site's queue is lost, so the runner records each replacement (PRD-HPC-1461 AC-35). It is not a
`status.reason`: the job stays `Admitted` (or `Running`) and starts as soon as the new job is scheduled.

| Field | Meaning |
|---|---|
| `status.run.queueReplaced` | The newest 5 replacements of this attempt, oldest first: `at`, `oldSiteJobID`, `newSiteJobID`, `queuedSeconds` (how long the old job had queued) and `reason` (`TokenRefresh`; also `HostLost` for a guest whose host is gone and `Replanned` for a guest planned as a host). |
| `status.run.queueReplacedCount` | All replacements of this attempt, also the ones no longer listed. |
| `status.attempts[].queueReplaced`, `.queueReplacedCount` | The same, copied when the attempt is settled. |
| `HPCSite.status.recentJobs[].queueReplacedCount` | The total over the job's attempts, for the console to say "queue place lost N times (token refresh)". A count only, so the list stays bounded. |

## Dataset staging (not a reason: `status.run.staging`)

Before it submits the site job the runner stages the pinned dataset commit on the project's scratch (PRD-HPC-1461,
PRD-HPC-1541). It reports one bounded record, and the follower prints one line from it in the step's own log, in the
`HPC: ...` style, so a user does not have to read the runner Job's log:

```
HPC: dataset <repo>@<short commit> staged (first copy | delta | already cached), <size>, <seconds>s
```

(an already cached copy has no size or seconds). The line appears once, as soon as the runner has reported the record
(with its first status report, while the site job queues); a resubmit that finds the copy resident does not print it again.

| Field | Meaning |
|---|---|
| `status.run.staging.repo` | The dataset repository. |
| `status.run.staging.commit` | The pinned commit, the first 12 characters. |
| `status.run.staging.kind` | `first` (no earlier version on scratch: the whole commit was copied), `delta` (only what differs from an earlier version was copied) or `cached` (already on scratch, or finished by another runner while this one waited: nothing copied). |
| `status.run.staging.files`, `.bytes` | What this staging copied (0 when `cached`; a delta counts the files it fetched, not the ones it hard-linked). |
| `status.run.staging.seconds` | How long it took, including a resumed attempt (0 when `cached`). |

The record holds no path and no site identity, and is absent when the step has no dataset or the job stages it itself
(the runner says so in its log). A runner restarted within the same attempt keeps the record its predecessor reported
(its own staging finds the copy resident and would otherwise overwrite "first copy" with "already cached").

## Cancelling

A cancel (the job deleted, or its Argo Workflow deleted, shut down or ended while the job had not) never releases the
booking on the controller's word alone. The controller starts a cancel-only runner that adopts the site job by its
name, cancels it, and reports the site's final state. Events: `SweepStarted`; `SiteJobUnconfirmed` (a Warning, when
`--sweep-give-up` (default 6 h) passed without the site's confirmation: the attempt is settled up to now and the job
may still run). See [extreme-cases.md](extreme-cases.md) 2.13 and 2.17.

## Budget warning (HPCSite)

| Condition / reason | Meaning |
|---|---|
| `BudgetWarning` True, reason `ThresholdCrossed` | For some allocation, used plus booked is at or above `budget.warnAtPercent` (default 80) of what the project may spend this month. With `source: site` and a `budget.grantNodeHours` figure for the allocation, the base is the month's grant: (grant - left at `observedAt` + used since + booked) / grant, so it counts what was spent before the observation too; without a grant, used plus booked over used plus what is left. Raised whatever `budget.atLimit` is. A `BudgetWarning` Warning event on the HPCSite; metrics `kaos_hpc_budget_warning` and `kaos_hpc_budget_percent`. The same figure is in `HPCSite.status.gpu|cpu|fpga|largeMem.percent` (int, rounded down; `-1` when there is nothing to be a percentage of: the allocation is `unbounded` or has no allowance), so a client reads it without the metric or its own sum; it is always written, so 0 means 0 percent. |
| `BudgetWarning` False, reason `BelowThreshold` | Below it, for every allocation that may spend anything. |
| `SiteReachable` False, reason `NotAnswering` | The site's REST API has not answered for at least 90 s. |
| `SiteReachable` True, reason `Answering` | It answers. |

## Not a `status.reason`

- `TIMEOUT` in a host's Slurm comment (`kaos-state=done rc=143 reason=TIMEOUT`): the slot manager's word that a step
  on a shared node was stopped by its own time limit (it ended by TERM or KILL after running its whole `srun --time`).
  The runner turns it into the attempt state `TIMEOUT` (see `WallTimeReached`).
- `memkill` in a host's Slurm comment (`kaos-state=done rc=N reason=memkill`): the slot manager stopped a step on a
  shared node for using more than its memory share. The attempt ends `Failed/SiteFAILED` (never exit 0; live
  2026-11-10, job 5442274).
- Events not tied to a reason constant: `LaneRefused` (the site refused the lane; the attempt ran on the class
  default), `HostSettledToJobEnd` (a host's attempt settled to its Slurm job's end, booking released), `HostEndUnknown`
  (a Warning: the host's runner ended before the job's end was reported; settled up to now, booking released),
  `ReportedTimeDiscarded` (implausible site times were ignored in settlement),
  `RunnerStartBlocked` (the runner Job cannot start, with the cause).
