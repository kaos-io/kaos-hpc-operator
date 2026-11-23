# kaos-hpc-operator scale test report

Generated 2026-11-23T15:07:35Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S (small) | PASS |
| S-shared (node sharing) | PASS |
| S-shared-resumable (node sharing with resumable steps) | PASS |
| S-pack (deterministic node sharing) | PASS |
| S-matrix (the items of a matrix step) | PASS |
| S-matrix-budget (a 32-item matrix against a one-node-hour budget) | PASS |
| M (medium) | PASS |
| L-late (late exit record) | PASS |
| L (large) | PASS |
| L-chaos | PASS |

## S (small)

1 job end to end: admit, book, runner Job, submit, observe, settle.

| measure | value |
|---|---|
| duration | 10.034s |
| final phases | map[Succeeded/Completed:1] |
| site submissions (all attempts) | 1 |
| jobs | 1 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 10 |
| peak (used+booked) / min(share, siteLeft) | ps-ml/tier0 6%, ps-ml/tier1 0% |
| project ps: controller used GPU+CPU node-min / site charged / bound | 3 / 3 / 960 |
| reconciles (total / per second) | 7 / 0.7 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 8.5 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 18 / 1.8 |
| controller API PATCHes | 10 |

## S-shared (node sharing)

8 hosts whose Slurm job keeps the node 6-14 minutes after the step for 16 guests that never start on their own: the ledger covers the site's charge at every snapshot and equals it at the end.

| measure | value |
|---|---|
| duration | 20s |
| final phases | map[Succeeded/Completed:24] |
| hosts settled to their job's end / guests settled 0 | 8 of 8 / 16 of 16 |
| guests with a step-time sample (start from the host) | 16 of 16 |
| controller settled node-min / site charged | 111 / 107.6 |
| site submissions (all attempts) | 24 |
| jobs | 24 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 20 |
| peak (used+booked) / min(share, siteLeft) | psh-ml/tier0 13%, psh-ml/tier1 0% |
| project psh: controller used GPU+CPU node-min / site charged / bound | 111 / 108 / 5880 |
| published ledger psh (gpu used/booked, cpu used/booked, active) | 111/0, 0/0, 0 |
| reconciles (total / per second) | 272 / 13.4 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 9.2 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.12 |
| controller API requests (total / per second) | 424 / 20.8 |
| controller API PATCHes | 195 |

## S-shared-resumable (node sharing with resumable steps)

10 hosts (6 resumable with 3 segments, 4 plain) and 30 guests (20 resumable with 2-3 segments, 10 plain) that run 8-16 minutes on hosts that end sooner: resumable guests are signalled and continued, plain ones fit or run alone; the ledger equals the site's charge.

| measure | value |
|---|---|
| duration | 1m44s |
| final phases | map[Succeeded/Completed:40] |
| guests taken by a host / resumable guests signalled before their run was over | 166 / 152 |
| resumable jobs continued / plain jobs that continued | 26 of 26 / 0 |
| controller settled node-min / site charged | 776 / 757.9 |
| site submissions (all attempts) | 208 |
| jobs | 40 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 104 |
| peak (used+booked) / min(share, siteLeft) | psr-ml/tier0 0%, psr-ml/tier1 0% |
| project psr: controller used GPU+CPU node-min / site charged / bound | 776 / 758 / 5400480 |
| published ledger psr (gpu used/booked, cpu used/booked, active) | 776/0, 0/0, 0 |
| reconciles (total / per second) | 2608 / 25.0 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 4.6 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.11 |
| controller API requests (total / per second) | 5004 / 48.0 |
| controller API PATCHes | 2083 |

## S-pack (deterministic node sharing)

4, 8 and 10 shareable 1-GPU steps arrive at once on sites with ample free nodes (4 GPUs per node): each run uses ceil(N/4) nodes, the rest ride as guests; every step succeeds; the site's charge is about one node's time per host, not one per step.

| measure | value |
|---|---|
| duration | 12s |
| final phases | map[Succeeded/Completed:22] |
| 4 steps: plan |  j00000:guest>ppk4-train/j00002@4000000/qos=/host= j00001:guest>ppk4-train/j00002@4000003/qos=/host= j00002:host>@4000002/qos=dev/host= j00003:guest>ppk4-train/j00002@4000001/qos=/host= |
| 4 steps: nodes started at the site / hosts / guests (want 1 nodes) | 1 / 1 / 3 |
| 4 steps: site charge, node-minutes (settled 8) | 7.6 |
| 8 steps: plan |  j00000:host>@4000003/qos=/host= j00001:host>@4000005/qos=dev/host= j00002:guest>ppk8-train/j00000@4000006/qos=/host= j00003:guest>ppk8-train/j00000@4000001/qos=/host= j00004:guest>ppk8-train/j00001@4000004/qos=/host= j00005:guest>ppk8-train/j00001@4000000/qos=/host= j00006:guest>ppk8-train/j00000@4000007/qos=/host= j00007:guest>ppk8-train/j00001@4000002/qos=/host= |
| 8 steps: nodes started at the site / hosts / guests (want 2 nodes) | 2 / 2 / 6 |
| 8 steps: site charge, node-minutes (settled 15) | 13.5 |
| 10 steps: plan |  j00000:guest>ppk10-train/j00009@4000006/qos=/host= j00001:guest>ppk10-train/j00009@4000005/qos=/host= j00002:guest>ppk10-train/j00003@4000003/qos=/host= j00003:host>@4000001/qos=dev/host= j00004:guest>ppk10-train/j00003@4000002/qos=/host= j00005:guest>ppk10-train/j00007@4000004/qos=/host= j00006:guest>ppk10-train/j00009@4000008/qos=/host= j00007:host>@4000007/qos=/host= j00008:guest>ppk10-train/j00003@4000000/qos=/host= j00009:host>@4000009/qos=/host= |
| 10 steps: nodes started at the site / hosts / guests (want 3 nodes) | 3 / 3 / 7 |
| 10 steps: site charge, node-minutes (settled 23) | 20.7 |
| site submissions (all attempts) | 22 |
| jobs | 22 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 12 |
| peak (used+booked) / min(share, siteLeft) | ppk10-ml/tier0 6%, ppk10-ml/tier1 0%, ppk4-ml/tier0 2%, ppk4-ml/tier1 0%, ppk8-ml/tier0 4%, ppk8-ml/tier1 0% |
| project ppk10: controller used GPU+CPU node-min / site charged / bound | 23 / 21 / 5880 |
| project ppk4: controller used GPU+CPU node-min / site charged / bound | 8 / 8 / 5880 |
| project ppk8: controller used GPU+CPU node-min / site charged / bound | 15 / 14 / 5880 |
| published ledger ppk10 (gpu used/booked, cpu used/booked, active) | 23/0, 0/0, 0 |
| published ledger ppk4 (gpu used/booked, cpu used/booked, active) | 8/0, 0/0, 0 |
| published ledger ppk8 (gpu used/booked, cpu used/booked, active) | 15/0, 0/0, 0 |
| reconciles (total / per second) | 226 / 10.2 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 2.1 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 337 / 15.2 |
| controller API PATCHes | 168 |

## S-matrix (the items of a matrix step)

a 4-item matrix step on a 1-GPU class: one node, a host and three guests, every item succeeds with its own result; the same step on a CPU class: four nodes, no guests.

| measure | value |
|---|---|
| duration | 12s |
| final phases | map[Succeeded/Completed:8] |
| 4 steps: plan |  j00000:guest>mxg-train/j00002@4000000/qos=/host= j00001:guest>mxg-train/j00002@4000002/qos=/host= j00002:host>@4000003/qos=dev/host= j00003:guest>mxg-train/j00002@4000001/qos=/host= |
| 4 steps: nodes started at the site / hosts / guests (want 1 nodes) | 1 / 1 / 3 |
| 4 steps: site charge, node-minutes (settled 7) | 7.0 |
| CPU class, 4 items: nodes started / hosts / guests (a CPU class is not shared: want 4 / 4 / 0) | 4 / 4 / 0 |
| site submissions (all attempts) | 8 |
| jobs | 8 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 12 |
| peak (used+booked) / min(share, siteLeft) | mxc-ml/tier0 0%, mxc-ml/tier1 2%, mxg-ml/tier0 2%, mxg-ml/tier1 0% |
| project mxc: controller used GPU+CPU node-min / site charged / bound | 20 / 20 / 5880 |
| project mxg: controller used GPU+CPU node-min / site charged / bound | 7 / 7 / 5880 |
| published ledger mxc (gpu used/booked, cpu used/booked, active) | 0/0, 20/0, 0 |
| published ledger mxg (gpu used/booked, cpu used/booked, active) | 7/0, 0/0, 0 |
| reconciles (total / per second) | 72 / 3.3 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.8 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 114 / 5.2 |
| controller API PATCHes | 59 |

## S-matrix-budget (a 32-item matrix against a one-node-hour budget)

32 items of 30 minutes on a 1-GPU class with 1 GPU node-hour of share: the first items are admitted, the next wait for budget, the rest are refused with BudgetBlocked once the spent minutes leave less than an item needs; the site is never charged more than the share; one more item of 90 minutes is refused as soon as it is seen.

| measure | value |
|---|---|
| duration | 5m1s |
| final phases | map[Refused/BudgetBlocked:15 Succeeded/Completed:18] |
| peak items waiting for budget | 30 |
| a waiting item's message | needs 30 GPU node-minutes; 0 are free on the GPU allocation until running jobs settle |
| the first refused 30-minute item | Refused/BudgetBlocked: needs 15 GPU node-minutes; the project's share has 10 left on the GPU allocation this period after what was used; steps on the project's other allocations are not affected |
| the 90-minute item | Refused/BudgetBlocked: needs 90 GPU node-minutes; the project's share has 60 left on the GPU allocation this period after what was used; steps on the project's other allocations are not affected |
| items succeeded / refused | 18 / 15 |
| site submissions (all attempts) | 18 |
| jobs | 33 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 300 |
| peak (used+booked) / min(share, siteLeft) | mxb-ml/tier0 100%, mxb-ml/tier1 0% |
| project mxb: controller used GPU+CPU node-min / site charged / bound | 50 / 50 / 540 |
| published ledger mxb (gpu used/booked, cpu used/booked, active) | 50/0, 0/0, 0 |
| reconciles (total / per second) | 644 / 2.1 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 4.0 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 1575 / 5.1 |
| controller API PATCHes | 467 |

## M (medium)

50 jobs, 2 classes, a share and siteLeft that admit only part at once: the rest wait (WaitingForBudget) and are admitted as bookings settle.

| measure | value |
|---|---|
| duration | 1m0s |
| final phases | map[Succeeded/Completed:50] |
| peak jobs waiting for budget | 10 |
| site submissions (all attempts) | 50 |
| jobs | 50 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 60 |
| peak (used+booked) / min(share, siteLeft) | pm-ml/tier0 100%, pm-ml/tier1 98% |
| project pm: controller used GPU+CPU node-min / site charged / bound | 755 / 732 / 1020 |
| rate limit meluxina: busiest window as share of the bucket | 24% |
| site calls meluxina | acct=127 acctlist=50 list=50 state=30 submit=50 (peak running 40, peak active 40, 422s 0, injected faults 0) |
| jobs/state calls | 30 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 446 |
| batching factor (IDs per call) | 14.9 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| reconciles (total / per second) | 799 / 13.2 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 3.3 |
| workqueue depth (max / max after the first minute) | 28 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.04 |
| controller API requests (total / per second) | 1342 / 22.2 |
| controller API PATCHes | 629 |
| controller RSS MB (avg / max over run) | 66 / 68 |
| controller heap in use MB (avg / max over run) | 14 / 17 |
| RSS MB: first half / second half after warmup | 66 / 67 |
| heap MB: first half / second half after warmup | 14 / 15 |
| controller goroutines (max) | 90 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 29 / 0.48 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 27814 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L-late (late exit record)

300 jobs on one site whose accounting record lags the job's end by 5m0s: a late record never marks a good job Failed and the call rate stays constant (D-36, T-14).

| measure | value |
|---|---|
| duration | 6m12s |
| final phases | map[Succeeded/Completed:300] |
| accounting reads that lagged (record still RUNNING after the end) | 168317 |
| site submissions (all attempts) | 300 |
| jobs | 300 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 372 |
| peak (used+booked) / min(share, siteLeft) | pl-ml/tier0 3%, pl-ml/tier1 1% |
| project pl: controller used GPU+CPU node-min / site charged / bound | 4994 / 4855 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 97% |
| site calls meluxina | acct=169147 acctlist=300 list=300 state=426 submit=300 (peak running 100, peak active 285, 422s 114, injected faults 0) |
| jobs/state calls | 426 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 33256 |
| batching factor (IDs per call) | 78.1 |
| jobs/state calls per poll cycle per HPCSite (avg) | 3.41 |
| runner submit calls / adoptions of a site job found by key | 300 / 0 |
| runner site reads on a stale observation (fallback) | 1243 |
| observation age of live jobs in pl-train: oldest / share older than 3 poll intervals | 1m1s / 6.3% |
| reconciles (total / per second) | 38251 / 102.8 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 3.0 |
| workqueue depth (max / max after the first minute) | 300 / 6 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.31 |
| controller API requests (total / per second) | 70336 / 189.0 |
| controller API PATCHes | 32351 |
| published ledger pl (gpu used/booked, cpu used/booked, active) | 3548/0, 1446/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 1s |
| duration (create to last terminal) | 3m0s |
| throughput (jobs/s) | 5.54 |
| final phases | map[Succeeded/Completed:1000] |
| site submissions (all attempts) | 1000 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 180 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5154 / 5003 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 5367 / 5213 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 5150 / 4996 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 42% |
| rate limit lumi: busiest window as share of the bucket | 31% |
| site calls meluxina | acct=1833 acctlist=667 list=667 ping=19 state=117 submit=667 (peak running 80, peak active 194, 422s 0, injected faults 0) |
| site calls lumi | acct=825 acctlist=333 list=333 ping=9 state=54 submit=333 (peak running 60, peak active 115, 422s 0, injected faults 0) |
| jobs/state calls | 171 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 7941 |
| batching factor (IDs per call) | 46.4 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.93 |
| runner submit calls / adoptions of a site job found by key | 1000 / 0 |
| runner site reads on a stale observation (fallback) | 9 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 7s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 7s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 6s / 0.0% |
| reconciles (total / per second) | 6703 / 37.1 |
| reconcile errors | 2 |
| reconcile duration avg (ms) | 21.2 |
| workqueue depth (max / max after the first minute) | 1000 / 903 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.79 |
| controller API requests (total / per second) | 21647 / 120.0 |
| controller API PATCHes | 14044 |
| controller RSS MB (avg / max over run) | 111 / 133 |
| controller heap in use MB (avg / max over run) | 51 / 77 |
| RSS MB: first half / second half after warmup | 111 / 125 |
| heap MB: first half / second half after warmup | 51 / 62 |
| controller goroutines (max) | 89 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 65 / 0.36 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 87 / 0.48 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 83 / 0.46 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 144708 |
| lane placement (last attempt of each job) | map[default:980 dev:10 test:10] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 2 / 0 |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00089": the object has been modified; please apply your changes to the latest version and try again |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00098": the object has been modified; please apply your changes to the latest version and try again |
| published ledger pa (gpu used/booked, cpu used/booked, active) | 3591/0, 1563/0, 0 |
| published ledger pb (gpu used/booked, cpu used/booked, active) | 3725/0, 1642/0, 0 |
| published ledger pc (gpu used/booked, cpu used/booked, active) | 3629/0, 1521/0, 0 |

## L-chaos

L plus: controller restart mid-run, a site outage (all 5xx, 10 minutes) on the shared site, 422 storm (slurmctld forgets finished jobs + spurious 422) on the other, 10% NODE_FAIL, jobs that always fail (RerunLimit), checkpointing steps (continuations <= 5, one cohort that needs more).

| measure | value |
|---|---|
| chaos: controller killed (SIGTERM) and restarted at | t+12s, down about 3s |
| chaos: 422 storm on site lumi (jobs forgotten at once, 30% spurious 422) | t+25s, 60s |
| chaos: site meluxina down (all 5xx) | t+35s, 10m0s |
| HPCSite pa-ml SiteReachable (want false) | false |
| HPCSite pa-ml SiteReachable (want true) | true |
| duration (create to last terminal) | 14m10s |
| final phases | map[Failed/RerunLimit:12 Failed/WallTimeReached:12 Succeeded/Completed:976] |
| outage: peak jobs Waiting/SiteUnavailable | 178 |
| outage: runner Jobs created for the down site while it was down | 0 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 32 |
| tier-1 re-runs (total / max per job) | 121 / 2 |
| continuations (total) | 209 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 880 / 0 |
| site submissions (all attempts) | 1330 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 849 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 5%, pa-ml/tier1 2%, pb-ml/tier0 5%, pb-ml/tier1 2%, pc-ml/tier0 4%, pc-ml/tier1 2% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5937 / 5782 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 6068 / 5917 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 6100 / 5933 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 75% |
| rate limit lumi: busiest window as share of the bucket | 97% |
| site calls meluxina | acct=11889 acctlist=912 list=1034 ping=431 state=337 submit=883 (peak running 82, peak active 147, 422s 10, injected faults 10259) |
| site calls lumi | acct=1285 acctlist=471 list=496 ping=219 state=175 submit=454 (peak running 61, peak active 101, 422s 124, injected faults 52) |
| jobs/state calls | 512 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 8452 |
| batching factor (IDs per call) | 16.5 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.60 |
| runner submit calls / adoptions of a site job found by key | 1337 / 65 |
| runner site reads on a stale observation (fallback) | 10841 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 16s / 1.6% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m8s / 86.9% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 29s / 1.4% |
| reconciles (total / per second) | 21368 / 25.1 |
| reconcile errors | 716 |
| reconcile duration avg (ms) | 13.5 |
| workqueue depth (max / max after the first minute) | 1000 / 983 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.34 |
| controller API requests (total / per second) | 43066 / 50.7 |
| controller API PATCHes | 14978 |
| controller RSS MB (avg / max over run) | 122 / 158 |
| controller heap in use MB (avg / max over run) | 59 / 84 |
| RSS MB: first half / second half after warmup | 123 / 130 |
| heap MB: first half / second half after warmup | 59 / 65 |
| controller goroutines (max) | 93 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 187 / 0.22 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 216 / 0.25 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 165 / 0.19 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 151877 |
| lane placement (last attempt of each job) | map[default:979 dev:13 test:8] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 65 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 711 / 0 |
| controller log error sample | writing observation: Patch "https://127.0.0.1:41549/apis/hpc.kaos.io/v1alpha1/namespaces/pa-train/hpcargoworkflowjobs/j00040/status": context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
| controller log error sample | probe: no credentials: get Slurm token Secret: context canceled |
| controller log error sample | Reconciler error: context canceled |
