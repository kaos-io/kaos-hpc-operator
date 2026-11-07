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
`make test`. `SCALE_TMPDIR` moves the scratch files (default `$TMPDIR`), `SCALE_REPORT` the report path.

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
| `GET /slurm/v/job/{id}`, `DELETE` | runner | 404 when slurmctld forgot the job |
| `GET /slurmdb/v/job/{id}` | runner | accounting: kept forever; exit code, dilated start/end, the comment (checkpoint marker) |
| `GET /slurmdb/v/sshare`, `/qos`, `GET /slurm/v/reservations` | hub | **not called by this controller**. Discovery of the site's share, QoS and reservations is hub-side in this design: the harness plays the hub and renders `spec.budget.siteLeft`, `spec.profile.qos` and `reservations` from the same figures. The fake also keeps the account's true consumption (`Charged`), which is what the budget assertion compares to. |

Behaviour knobs: latency and jitter, 5xx rate, hanging requests, a full outage (`SetDown`), spurious 422s
(`SetSpurious422`), how long slurmctld remembers a finished job (`SetMinJobAge`), queue delay, node capacity
(FIFO start), a seeded random end per run (`NODE_FAIL`, `PREEMPTED`, `FAILED`), a run that outlasts its limit
(`TIMEOUT`), and per-job hints that the harness puts in the submit environment (`FAKE_RUN_MINUTES`,
`FAKE_FORCE_END`, `FAKE_SEGMENTS`: a checkpointing step that exits 85 with a checkpoint marker in the job
comment until its last segment). A run is planned from the job name, the attempt and the seed, so a repeated
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
test's poll interval). A failed submit restarts the "pod" after 10, 20, 40, 80 s and fails the batch Job after
the fifth try, like `backoffLimit: 4`. Not modelled: staging of dataset and image, the job script, logs,
outputs, node sharing, the cancel-on-SIGTERM path (a cancel-only runner for the orphan sweep is).

## Scenarios

| scenario | what it proves |
|---|---|
| **S** | one job end to end: admitted, booked, runner Job, submit, observed, settled; one submission |
| **M** | 50 jobs, two classes, a share and siteLeft that admit only part: the rest wait `WaitingForBudget` and are admitted as bookings settle; on every snapshot `used + booked <= min(share, siteLeft)`; the fake site's own charge never exceeds it; every job terminal; no duplicate submission; the published ledger ends with nothing booked |
| **L** | 1,000 jobs, 3 projects on 2 sites (two projects share one site profile and so one token bucket), lanes `test` and `dev` on with one-job-per-user limits at the site: reconcile throughput, `jobs/state` calls per poll cycle (batched, O(sites)), the shared rate-limit bucket, controller RSS and heap over time, `recentJobs` <= 50 and bounded object size, HPCSite write rate, no lane refusal (the controller never submits a second job into a one-job lane) |
| **L-chaos** | L plus: controller killed and restarted mid-run (bookings unchanged, no re-submission), 2-minute outage of the shared site (all 5xx: jobs wait, none fails), a 422 storm on the other site, 10 % `NODE_FAIL`, jobs that always lose their node (`RerunLimit` after exactly 2 re-runs), checkpointing steps (continue at most 5 times; a cohort needing 8 segments ends `ContinuationLimit`) |

## Assertions that fail the test

Budget invariant on every snapshot; site charge <= min(share, siteLeft); no attempt submitted twice and
submissions == attempts that reached the site; `jobs/state` batched (>= 5 IDs per call, calls per poll per
HPCSite <= ceil(active/100)+1; not asserted in the 422 storm run) and inside the token bucket in every
1 s, 5 s and 30 s window; no `RunnerFailed`; re-runs <= 2, continuations <= 5; `recentJobs` <= 50 and the
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

Found and **not** fixed (design decisions or changes that cannot be proven without the real site):

- **A submission whose answer is lost can be repeated if the job finishes before the pod's retry.** The restarted
  runner adopts only a job that is still pending or running (`find_active`), or the one a previous runner reported.
  If the site accepted the job, the answer timed out, and the job had already ended by the time the pod restarted
  (10 s, then 20, 40, 80), the attempt is submitted a second time. Real jobs outlive 10 s (queue, staging), so
  the fake holds a lost-reply job for 60 s (`LostReplyHold`); with 20 s the harness hit it twice in 2,700 submissions.
  A fix needs an attempt marker (`kaos-attempt=N`) that survives every rewrite of the job comment, which the job script
  does in three places (`fail()`, the slot manager, the checkpoint copier), and the stager reads that comment: not done blind.
- **A site outage longer than about 2.5 minutes fails every job that has not been submitted yet.** The runner stops on a
  5xx at submit and Kubernetes restarts the pod after 10, 20, 40 and 80 s (`backoffLimit: 4`); the fifth try is 150 s
  after the first. The 2-minute outage of L-chaos passes only because of that margin, and the scenario keeps the
  second site's extra faults off the shared site for the same reason (`RunnerFailed` is asserted to be zero). PRD
  AC-28 (30 minutes of retry) and AC-30 (`SiteUnavailable` wait) are the open items that cover it.
- **Capacity.** One worker reconciles jobs and each reconcile lists the jobs: the average reconcile costs 0.9 ms with 50 jobs
  and 2.6 ms with 1,000 (`HPCSite` ledger, namespaces and step history are read per reconcile). At 1,000 jobs
  and about 70 reconciles per second the worker is 23-29 % busy (reported as "reconcile worker busy"); the test fails
  above 90 %. The cost grows with the number of jobs, so the headroom is a few times the 1,000 jobs tested, not orders of magnitude.
