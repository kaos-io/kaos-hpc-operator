# Scale test

Proves the kaos-hpc-operator is correct and stable at large scale without spending HPC time: a load test
of the **real controller binary** (`cmd/main.go`, started as a child process with its production flags)
against an envtest API server, in-process stand-ins for the runner pods, and fake Slurm REST sites.

```
make scale-test                      # all scenarios, about 10 minutes, writes test/scale/REPORT.md
SCALE_L_JOBS=150 make scale-test     # quick smoke run (L scenarios shrink; M keeps 50 jobs)
go test -tags scale ./test/scale/ -run TestScaleM -v    # one scenario
```

Needs `KUBEBUILDER_ASSETS` (`make scale-test` sets it through setup-envtest). Normal CI is untouched: every
file of the harness has the build tag `scale`; only the small `fakeslurm` package (and its unit tests) is in
`make test`. `SCALE_TMPDIR` moves the scratch files (default `$TMPDIR`), `SCALE_REPORT` the report path. `SCALE_OUTAGE` sets the L-chaos site outage (default 10m); `SCALE_ACCT_LAG` (default 5m) and `SCALE_LATE_JOBS` (default 300) set the L-late scenario.

## What runs

| piece | what it is | file |
|---|---|---|
| controller | the real `manager` binary, built by the test, SIGTERM/restart for the chaos run, scraped on `/metrics` every 2 s for RSS, heap, reconciles, queue depth, API requests | `world_test.go`, `observe_test.go` |
| API server | envtest (real kube-apiserver + etcd) with the repo's CRDs | `world_test.go` |
| fake Slurm sites | `fakeslurm`: slurmrestd + slurmdbd over TLS (see below) | `fakeslurm/server.go` |
| fake runner pods | the in-process "kubelet": plays the runner Job of every attempt | `kubelet_test.go` |
| monitor | reads the API every second and checks the invariants on each snapshot; watches HPCSite writes | `observe_test.go` |
| assertions, report | `verify_test.go`, `scenarios_test.go`, `report_test.go` | |

### The fake Slurm site

The repo already had fakes: `runner/tests/` (a Python Slurm in-process, for the runner's unit tests) and `fakeReader` in
`internal/controller` (the `StateReader` interface). Neither speaks HTTP, and the scale test must drive the real
`sitestate.Client`, the token bucket and the runner's REST calls, so this is a new fake site; the old ones are untouched.

It implements what the controller and the runner call, and nothing else:

| call | by | behaviour |
|---|---|---|
| `GET /slurm/v/jobs/state?job_id=1,2,3` | controller poller | plain comma list, `job_id` as a string; **422 when any ID is unknown** to slurmctld |
| `POST /slurm/v/job/submit` | runner | per-user lane limits (`MaxJobsPerUser`, `MaxWallMinutes` per QoS) refuse with the QoS error text; returns the job id |
| `GET /slurm/v/jobs?name=` | runner (find-by-key) | the real runner lists all jobs and filters by name; the fake filters server side |
| `GET /slurmdb/v/jobs?job_name=&start_time=` | runner (adopt a finished job by name) | every job of that name, finished or not, kept forever |
| `GET /slurm/v/ping` | controller poller (site health) | one healthy controller; 503 like every call while the site is down |
| `GET /slurm/v/job/{id}`, `DELETE` | runner | 404 when slurmctld forgot the job |
| `GET /slurmdb/v/job/{id}` | runner | accounting: kept forever; exit code, dilated start/end, the comment (checkpoint marker) |
| `GET /slurmdb/v/sshare`, `/qos`, `GET /slurm/v/reservations` | hub | **not called by this controller**. Discovery of the site's share, QoS and reservations is hub-side in this design: the harness plays the hub and renders `spec.budget.siteLeft`, `spec.profile.qos` and `reservations` from the same figures. The fake also keeps the account's true consumption (`Charged`), which is what the budget assertion compares to. |

Behaviour knobs: latency and jitter, 5xx rate, hanging requests, a full outage (`SetDown`), spurious 422s
(`SetSpurious422`), how long slurmctld remembers a finished job (`SetMinJobAge`), queue delay, node capacity
(FIFO start), a lost reply of a share of submissions (`LostReplyRate`; `LostReplyQuick` of those start at once and end within a second, before the
runner asks again), a seeded random end per run (`NODE_FAIL`, `PREEMPTED`, `FAILED`), a run that outlasts its limit
(`TIMEOUT`), and per-job hints that the harness puts in the submit environment (`FAKE_RUN_MINUTES`,
`FAKE_FORCE_END`, `FAKE_SEGMENTS`: a checkpointing step that exits 85 with a checkpoint marker in the job
comment until its last segment; `FAKE_HOST_TAIL_MINUTES`: a node-sharing host whose Slurm job keeps the node that
long after its step, its step's result written into the job comment as the slot manager writes it, all of it
charged; `FAKE_GUEST`: a guest that never starts on its own, is taken by a running host of its account whose job
outlasts it and is cancelled with its result, charged nothing). A run is planned from the job name, the attempt and the seed, so a repeated
submission plans the same run.

**Time dilation.** Real seconds are cheap, node-minutes are the budget unit. The site reports job times with
the run time multiplied by 60: a job that really runs 8 s is accounted as 8 node-minutes. The start time is
real (the controller rejects reported times more than an hour in its future), so a dilated run stays below
about 55 minutes. All wall limits in the scenarios are 15 to 45 minutes. The controller itself runs in real
time (its constants such as the 30 s resync of waiting jobs, the 15 s serve resync and the 10-minute ledger
resync are unchanged and part of what is measured).

### The runner pods

In production every attempt is a batch Job (`runnerjob.Build`) whose pod runs `runner/kaos_hpc_runner`. A scale
test cannot run 1,000 Python pods and envtest has no kubelet, so `kubelet_test.go` watches the same runner Jobs
and does what the runner does through the same interfaces: find the attempt's site job by key, adopt it or
submit it, retry a refused lane once on the class QoS, report `status.run` (siteJobID, state, start time, the
one terminal report with exit code, times and checkpoint reference), read the state from
`status.siteObservation` while it is fresh and call the site only for the accounting record of a state change
or an end, or for itself when the observation is stale (the real runner's `STALE_*` rules, scaled to the
test's poll interval). A refused submit (4xx) restarts the "pod" after 10, 20, 40, 80 s and fails the batch Job after
the fifth try, like `backoffLimit: 4`; a site that does not answer is waited for, up to 30 minutes. Not modelled: staging of dataset and image, the job script, logs,
outputs, the cancel-on-SIGTERM path (a cancel-only runner for the orphan sweep is). Node sharing is modelled as the
runner sees it (D-26): a host reports its step's end with `hostJobOpen` and follows its Slurm job to its end; a guest
reports its host while taken and ends with the result its host wrote. GPU slots are not modelled.

## Scenarios

| scenario | what it proves |
|---|---|
| **S** | one job end to end: admitted, booked, runner Job, submit, observed, settled; one submission |
| **S-shared** | node sharing (D-26): 8 hosts whose Slurm job keeps the node 6-14 minutes after the step, 16 guests taken by them: on every snapshot the ledger's `used + booked` covers what the site has charged; every host settled to its job's end, every guest 0; the settled total equals the site's charge (whole minutes, rounded up); the published ledger is current 10 s after the last end. Against the controller before the fix the coverage check fails (`used 8 + booked 0` for 70 charged node-minutes) |
| **S-pack** | deterministic node sharing (D-70): 4, 8 and 10 shareable 1-GPU steps arrive at once on fake sites with ample free nodes (4 GPUs per node, the live run's shape: one class, one scope, 30-minute limits): the site starts exactly ceil(N/4) jobs, the rest are guests taken by their hosts and settle 0, every step succeeds, and the charge is about one node's time per host. The fake site plays the slot manager for jobs that carry a scope and slots, and honours BeginTime; the fake runner submits as the real one (offer window for a host, deferred for a guest, read from the runner Job's environment). Against the controller before the plan the site starts N jobs and charges 28-72 node-minutes |
| **M** | 50 jobs, two classes, a share and siteLeft that admit only part: the rest wait `WaitingForBudget` and are admitted as bookings settle; on every snapshot `used + booked <= min(share, siteLeft)`; the fake site's own charge never exceeds it; every job terminal; no duplicate submission; the published ledger ends with nothing booked |
| **L** | 1,000 jobs, 3 projects on 2 sites (two projects share one site profile and so one token bucket), lanes `test` and `dev` on with one-job-per-user limits at the site: reconcile throughput, `jobs/state` calls per poll cycle (batched, O(sites)), the shared rate-limit bucket, controller RSS and heap over time, `recentJobs` <= 50 and bounded object size, HPCSite write rate, no lane refusal (the controller never submits a second job into a one-job lane) |
| **L-chaos** | L plus: controller killed and restarted mid-run (bookings unchanged, no re-submission), a 10-minute outage of the shared site (all 5xx: jobs wait `Waiting/SiteUnavailable`, no runner Job is created for it while it lasts, none fails, all finish afterwards; `SiteReachable` false, then true), lost submit replies that finish before the retry (found in accounting by name: 0 duplicates), a 422 storm on the other site, 10 % `NODE_FAIL`, jobs that always lose their node (`RerunLimit` after exactly 2 re-runs), checkpointing steps (continue at most 5 times; a cohort needing 8 segments ends `ContinuationLimit`) |
| **L-late** | 300 jobs on one site whose accounting lags slurmctld by 5 minutes (`fakeslurm.Options.AcctLag`; PRD-HPC-1461 AC-58, T-14): for 5 minutes after a job ends, `jobs/state` says so but the accounting record still shows `RUNNING` with no exit code and no end. No job ends `Failed`, all 300 end `Succeeded/Completed` once the record arrives, every job saw a lagged read, nothing is submitted twice, the ledger equals the site's charge, and `jobs/state` stays batched (about 3.5 calls per poll for 300 jobs) inside the token bucket. The fake runner (not the real one) re-reads the accounting record every tick, so the `acct` call count of the site is the stand-in's, not a production rate |

## Assertions that fail the test

Budget invariant on every snapshot; site charge <= min(share, siteLeft); no attempt submitted twice and
submissions == attempts that reached the site; `jobs/state` batched (>= 5 IDs per call, calls per poll per
HPCSite <= ceil(active/100)+1; not asserted in the 422 storm run) and inside the token bucket in every
1 s, 5 s and 30 s window; no `RunnerFailed`; in L-chaos also jobs seen `Waiting/SiteUnavailable`, no runner Job created for the down site after the poller's two probes, `SiteReachable` false during and true after the outage, lost-reply jobs that finished before the retry (> 0) and 0 duplicates; re-runs <= 2, continuations <= 5; `recentJobs` <= 50 and the
HPCSite object < 128 KiB; HPCSite written at most N times in any second; controller RSS not growing after
warmup (+30 % and +30 MB) and below a ceiling; the published ledger shows nothing booked after the last job.

## Reading REPORT.md

`REPORT.md` is regenerated by every run. Numbers are for this machine and this fake site; the assertions
are what protects against regressions, the table is for comparison between runs. "Controller API requests"
come from the controller's own `rest_client_requests_total`. "HPCSite writes" are real changes (MODIFIED
watch events), not patch calls that changed nothing.

## What the first runs found

Fixed in the same PR, one commit and one regression unit test each:

| finding | evidence in the test | fix |
|---|---|---|
| the last job to end left its booking in `HPCSite.status` until the 10-minute resync | M: 10 s after all 50 jobs ended the site still showed `booked 15`, one active job, and used 293 where the jobs said 303. The ledger is published in the reconcile that ends the job, from an informer cache that does not show that write yet; the ended job's own next reconcile returned before publishing | `45bda79` the process remembers the jobs it ended and publishes once from the reconcile their end event wakes |
| `HPCSite.status` was written in step with the reconciles | L: 31 writes in one second, about 5 per second, 23 KiB each | `4a9689f` at most one write per `--ledger-publish-interval` (default 1 s) and site; a request inside the interval is written when it has passed. Now at most 2 in a second |
| one unknown ID made the poll walk its list one ID at a time | 101 calls for 100 IDs; at the production bucket (2 calls/s) that is 50 s against a 30 s interval, so the poll is abandoned and nobody in the list gets an observation while the ID stays in it. L-chaos 422 storm: the bucket of the stormed site is used to 95-98 % in the busiest window | `551314e` the list is split in halves: 15 calls for one unknown ID in 100 |

Found by the first run, fixed in the follow-up PR (`fix(runner,controller): ...`, one regression test each):

| finding | evidence in the test | fix |
|---|---|---|
| **A submission whose answer is lost could be repeated if the job finished before the pod's retry.** The adopt step found only live jobs by key | with the fake holding a lost-reply job for 20 s the harness hit it twice in 2,700 submissions | the job NAME (immutable; the job script rewrites the comment, not the name) is now `kaos-<uid>-<step>-<hpc job>-a<attempt>`, and adopt also searches slurmdbd by that name (newest non-cancelled job above the reported one). Fake: `LostReplyQuick` jobs that finish within a second; L-chaos asserts they happened and that there are 0 duplicates |
| **A site outage longer than about 2.5 minutes failed every job not yet submitted** (`RunnerFailed` after the pod's five tries) | the 2-minute outage passed only by that margin; the second site's extra faults were kept off the shared site | an unreachable site is a wait (PRD AC-28, AC-30): runner waits 5..60 s per ask up to `--site-outage-max` (30 min); the controller books nothing and creates no runner Job for a down site (`Waiting/SiteUnavailable`), resumes when it answers. L-chaos: 10-minute outage, `RunnerFailed` 0, no runner Job created while down, faults on both sites |

Still **not** fixed (design decisions or changes that cannot be proven without the real site):

- **Capacity.** One worker reconciles jobs and each reconcile lists the jobs: the average reconcile costs 0.9 ms with 50 jobs
  and 2.6 ms with 1,000 (`HPCSite` ledger, namespaces and step history are read per reconcile). At 1,000 jobs
  and about 70 reconciles per second the worker is 23-29 % busy (reported as "reconcile worker busy"); the test fails
  above 90 %. The cost grows with the number of jobs, so the headroom is a few times the 1,000 jobs tested, not orders of magnitude.
