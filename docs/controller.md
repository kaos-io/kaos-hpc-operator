# How the controller runs a job

State of `main` (PRD-HPC-1461 v0.13.0). Everything here is merged; open work is listed in the
[README](../README.md#in-progress-open-pull-requests-not-part-of-main). Decision numbers (D-nn,
F-nn, AC-nn, T-nn) refer to the PRD in novelcore/kaos, `knowledge/prds/1461-hpc-controller.md`.

The controller is a tenant of the HPC site: it holds no administrator right and puts nothing on the
site outside a job. The site's scheduler decides when and where a job starts; the controller decides
whether it may go, what it asks for, and what happens when it ends.

## 1. The life of one step

```
Argo step (follower)            controller                          runner Job (per attempt)        site
  create HPCArgoWorkflowJob ──►  admit + book (Pending→Admitted)
                                 choose time limit + lane
                                 create runner Job ───────────────►  stage dataset + image
                                                                     submit (or adopt by key) ─────►  Slurm job
                                 poll site: 1 call / site / interval ◄──────────────────────────────  state
                                 (writes status.siteObservation)     follows state, streams log
                                                                     reports status.run ──────────►
                                 settle, release booking
  prints changes, writes outputs ◄ status.phase = Succeeded|Failed|Cancelled
```

Phases: `Pending`, `Waiting`, `Refused`, `Admitted`, `Running`, `Succeeded`, `Failed`, `Cancelled`
(`Refused`, `Succeeded`, `Failed`, `Cancelled` are terminal). `status.reason` says why
(`WaitingForBudget`, `ConcurrencyLimit`, `Continuing`, `TimeLimitRetry`, `Idle`, `WallTimeReached`, ...).

- **Follower** (`/follower`, no credentials). Creates the job with a deterministic name
  (`hpc-<20 hex of sha256(WF_UID/NODE_KEY)>`), so a re-created pod re-attaches instead of duplicating.
  Never fails because of an API error: it keeps trying until the job ends or Argo stops the pod.
  Exits 0 only on `Succeeded`; writes `/tmp/outputs/<n>.json` and `/tmp/slurm-job-id`; prints one
  plain line per change of placement or waiting state. A queued job never times out on the platform
  side: the follower waits as long as the site queue does.
- **Runner Job** (`hpcr-<hash8(uid)>-a<attempt>`, in the HPCSite's namespace). One Kubernetes Job per
  attempt, backoff limit 4. A pod or node loss is a Job retry: the replacement adopts the Slurm job
  by its deterministic name, so a restart never submits twice. The controller counts restarts
  (`kaos_hpc_runner_restarts_total`). If the Job ends without a terminal report the job fails with
  `RunnerFailed`, after a 15 s grace for the report to arrive.
- **Retries inside the runner.** Exit 231 (a registry pull with an expired token after a long queue
  wait) resubmits with fresh credentials, up to 3 times. A shared-node guest whose host died is
  resubmitted, up to 3 times. A node failure or preemption (`NODE_FAIL`, `PREEMPTED`) is **not**
  resubmitted by the controller today: the attempt ends `Failed` with reason `Site<STATE>`.
  (PRD AC-28, the 3-attempt rule and the outage backoff, is pending.)
- **Continuation.** A step with `spec.resumable` that exits 85 after staging out a checkpoint
  (`status.run.checkpointRef`) continues in a new attempt under the same job
  (`Continuing`, `status.continuations`, at most `maxContinuations`, default 5). Exit 85 without
  `resumable`: `NotResumable`; without a checkpoint: `Timeout`; over the cap: `ContinuationLimit`.
  Each continuation is queued, booked and settled as a new attempt.

## 2. Admission, booking, settlement

Admission runs one job at a time (a mutex), so concurrent jobs never overbook.

1. Refuse what can never run: no class on the site (`NoSite`), class not allowed for the project
   (`ClassNotAllowed`), more than one node (`MultiNodeUnsupported`), declared wall time over the lower of the
   class's maximum and the project's `limits.maxWallTimeMinutes` (`WallTimeOverLimit`), or a cost above the share minus what was already used under
   policy `block` (`BudgetBlocked`). Under policy `warn` it is admitted beyond the share with a Warning event `OverShare`.
2. Wait for what can fit later: the project is at `limits.maxConcurrentJobs` (`ConcurrencyLimit`,
   never a refusal) or running jobs must settle first (`WaitingForBudget`).
3. Admit: book **nodes x the attempt's time limit** in node-minutes against the tier of the class
   (gpu or cpu). The ledger is computed from the jobs on every pass, not kept as a counter.

The budget is either a `share` (a fixed number of node-hours per calendar month) or `site`: what the
site says is left on the project's account at `observedAt`; only bookings and attempts that ended
after that time are taken off it. A running job is never stopped for budget; only the next
continuation or job waits.

Settlement happens when an attempt ends: node-minutes = nodes x (site end - site start), rounded up
to the minute.

- A start or end that is unset, before 2000, or more than an hour in the future (Slurm's `NO_VAL`) is
  discarded and a Warning event `ReportedTimeDiscarded` names it. An unknown start settles 0 (the job never ran on its own);
  an unknown end settles up to now. An end before the start is discarded. The ledger never goes
  negative.
- A **guest** on a shared node settles 0: its host's booking covers the node.
- A **host** is settled from its Slurm job's start to the Slurm job's end, not from its step's end
  (the site keeps charging the node while the host waits for guests). The runner follows the host job
  until it is terminal and re-sends its report with the job's end time; the controller raises the
  settlement once, from the attempt record (never incrementing), and emits `HostSettledToJobEnd`.
  A repeated or implausible report changes nothing.

## 3. Time limit (F-41, D-67)

`spec.wallTimeMinutes` is the cap, as in Slurm: the controller only ever asks for less. The limit of
an attempt is decided at admission and written to `status.placement` (`timeLimitMinutes`,
`source`: `declared` | `learned` | `timeout-retry`, `samples`).

- Samples: the run times (site start to end, valid times only) of earlier **Succeeded** jobs of the
  same step: same namespace, step name, class and **image repository** (tag and digest stripped, so a
  rebuild keeps its history), never continued. They are read from the jobs still in the cluster; the
  controller keeps no store of its own.
- Fewer than 3 samples: the declared limit.
- Otherwise: p95 (nearest rank) x 1.5, rounded **up** to 15 min, at least 15, at most the lowest of
  the spec, the class and the site maximum. A learned limit that would reach the cap is the declared
  one.
- **Only steps of at most 2 hours learn a limit.** If the computed limit exceeds 120 min the step
  keeps its declared limit: a long training gains nothing from a shorter limit and could lose hours
  if one run is slower than its history.
- A **resumable** step and any job without `stepName` keep the declared limit.
- **Timeout retry.** A `TIMEOUT` of an attempt that ran under a *learned* limit is retried once
  at the **declared limit** (reason `TimeLimitRetry`, `source: timeout-retry`), not at a multiple of
  the learned one: a step whose run time depends on its inputs would otherwise be killed twice. A step
  is therefore killed at most once, and the next learned limit includes the run. A `TIMEOUT` at the
  declared limit is final (`Failed` / `WallTimeReached`).

## 4. Lanes (F-40, F-42, F-44, D-66, D-68, D-69)

A lane is a (QoS, reservation) pair. The controller knows no site: the ordered candidates come from
`--lane-candidates` (chart `lanes.candidates`), the facts from `HPCSite.spec.profile`
(`qos[]`, `reservations[]`, `classes[].lanes`), which kubecore-operator discovers from the site.

For each serve-mode attempt, after the time limit is known, the **first** candidate that passes all
of these is used; none passing means the class's own QoS (and its partition, no reservation):

1. the class lists the QoS in `lanes` (an empty list does not restrict) and the profile has the QoS;
2. the QoS is not preemptible (a preempted host would take its guests with it);
3. the limit fits the QoS's `maxWallTimeMinutes` (0 = the site reports none);
4. if the candidate names a reservation (`reservationByPartition`), the class's partition has one,
   the profile lists it for that partition, and it has not ended;
5. the QoS's per-user job limit (`maxJobsPerUser`) is not used up. The controller counts, per
   **site identity** (site + project user), the jobs holding a booking in each QoS across every
   HPCSite it sees, plus bookings it made that its cache may not show yet. This is the lane
   semaphore: it stops six parallel jobs from all aiming at a one-job lane.

The choice is stored in `status.placement.qos` / `reservation` and counted in
`kaos_hpc_lane_placements_total{qos,reservation,fallback}`. The controller does not ask the site for
a start estimate: the order of the candidates is the policy.

**Fallback.** If the site refuses the lane at submit (QoS or reservation errors, one-job slot taken
outside KAOS), the runner submits once more on the class's own QoS and reports what it used;
the controller then shows that QoS in `status.placement`, records the lane as `refusedQOS`, emits
`LaneRefused` and counts `fallback="true"`. A lane never fails a step.

Which candidates a deployment passes is the platform's policy (kubecore-operator `HPC_LANES`). With
no candidates, lane placement is off and nothing is counted.

## 5. Node sharing (PRD-1609)

On sites that bill whole nodes, a one-GPU step on a four-GPU node pays for four. With sharing on, the
first of a project's waiting jobs to start (the **host**) takes the project's other waiting jobs of
the same scope one by one, marks each (the guest's Slurm comment `kaos-state=taken`, host job id),
runs its command as a job step on one free GPU with a share of the node's CPU and memory (at most as
many at once as GPUs), writes the result into the guest's comment (`kaos-state=done rc=N`) and
cancels the guest's job. The guest's follower reads its own result. Facts:

- Both sides must ask: `HPCSite.spec.sharing` (site/project decision) and `spec.sharing` on the job
  (`class`, scope). Absent on either: no sharing.
- A host takes only jobs of its own scope whose step fits the host's remaining time; a job that does
  not fit is skipped for good (remaining time only shrinks). The host job asks Slurm for a padded
  time limit so it has room for guests.
- Booking: the host's booking covers the node; a guest books like any job but settles 0
  (`status.run.hostSiteJobID`). A guest no longer holds a slot of its lane.
- A host that dies without handing guests back: each guest notices (`host_gone`) and resubmits as a
  new job, up to 3 times.
- Settlement of the host runs to the end of its Slurm job (section 2).

## 6. Idle watchdog (F-46, D-72)

The job script samples GPU and CPU every 30 s. If a GPU step's utilisation stays below 5 % **and**
its log does not grow for `spec.idleTimeoutMinutes` (default 20; `0` disables), the job script ends
the step with exit 236. The attempt is `Failed` / `Idle`; `status.message` names the idle window, the
node-minutes saved (the time limit minus the time it ran, x nodes; 0 for a guest) and, when the runner
reported them, the step's last log lines (`status.run.lastLog`, at most 2 KiB). A Warning event `Idle`
is raised and `kaos_hpc_idle_kills_total{tier,project}` incremented. CPU classes use CPU utilisation.
On a shared node every slot's script has its own watchdog.

## 7. Batched site polling (D-36, T-25)

One `SitePoller` runs on the leader. Every `--site-poll-interval` it lists the non-terminal jobs whose
current attempt has a known site job id, groups them by HPCSite, and makes **one** `jobs/state` call
per site (chunked at 100 IDs) with an explicit `job_id` list, rate-limited by a token bucket per
site profile. The answer keeps only IDs that were asked for. It then merge-patches
`status.siteObservation` of each job (state, pending reason, comment, `observedAt`, interval),
dropping an observation whose site job id is no longer the runner's. A failed call writes nothing; a
partial result is never written.

- The ID list is a plain comma-separated query value: slurmrestd does not decode `%2C`, which made the
  whole call fail with 422 on the live site.
- If the site rejects a list because one ID is unknown (a purged guest job), the client asks for the IDs
  one by one and skips the unknown ones; any other error stops the poll. An ID the site does not list
  is reported with an empty state, and the runner then reads the final record from accounting.
- Runners read `siteObservation` instead of calling the site. They call it themselves for submit,
  cancel and the final record, and as a fallback when the observation is older than three intervals
  (controller down).
- `--site-poll-interval=0` turns the poller off: runners poll the site themselves.

## 8. Cancel and stop

The finalizer `hpc.kaos.io/cancel` is kept on every non-terminal serve-mode job. A cancel is
triggered by deleting the job, or by its Argo Workflow being deleted, shut down (`spec.shutdown`) or
finished while the job had not (UID checked, so a same-named Workflow counts as deleted). The
controller sets `hpc.kaos.io/cancel-requested`, waits until it is visible in the API, then deletes the
runner Job in the foreground; the runner's SIGTERM handler cancels the Slurm job only if it finds the
annotation. The attempt is settled as `Cancelled` and the booking released. Live evidence:
cancellation within seconds of an Argo stop.

## 9. What the controller does not do (yet)

- Resubmit after `NODE_FAIL` / `PREEMPTED` (the 3-attempt rule); an outage backoff of 30 minutes.
- Reasons beyond `Site<STATE>` for `OOM`, `PullFailed`, `StageInFailed`, `UserError`, `GPUViolation`.
- Pick a lane by the site's start estimate; `status.placement` holds no estimate or alternatives.
- Run `HPCOperation`s (staging, cache check) as controller-owned Jobs; staging runs in the runner.
- Check that project shares fit an `HPCAllocation` (that check is on the hub, in kubecore-operator).
