# kaos-hpc-operator scale test report

Generated 2026-11-10T05:40:05Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S (small) | PASS |
| S-shared (node sharing) | PASS |
| M (medium) | PASS |
| L (large) | FAIL (1 assertions) |
| L-chaos | FAIL (1 assertions) |

## S (small)

1 job end to end: admit, book, runner Job, submit, observe, settle.

| measure | value |
|---|---|
| duration | 10.032s |
| final phases | map[Succeeded/Completed:1] |
| site submissions (all attempts) | 1 |
| jobs | 1 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 10 |
| peak (used+booked) / min(share, siteLeft) | ps-ml/tier0 6%, ps-ml/tier1 0% |
| project ps: controller used GPU+CPU node-min / site charged / bound | 3 / 3 / 960 |
| reconciles (total / per second) | 7 / 0.7 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 9.9 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 13 / 1.3 |
| controller API PATCHes | 10 |

## S-shared (node sharing)

8 hosts whose Slurm job keeps the node 6-14 minutes after the step for 16 guests that never start on their own: the ledger covers the site's charge at every snapshot and equals it at the end.

| measure | value |
|---|---|
| duration | 19s |
| final phases | map[Succeeded/Completed:24] |
| hosts settled to their job's end / guests settled 0 | 8 of 8 / 16 of 16 |
| controller settled node-min / site charged | 111 / 107.6 |
| site submissions (all attempts) | 24 |
| jobs | 24 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 19 |
| peak (used+booked) / min(share, siteLeft) | psh-ml/tier0 13%, psh-ml/tier1 0% |
| project psh: controller used GPU+CPU node-min / site charged / bound | 111 / 108 / 5880 |
| published ledger psh (gpu used/booked, cpu used/booked, active) | 111/0, 0/0, 0 |
| reconciles (total / per second) | 272 / 14.2 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 3.0 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.04 |
| controller API requests (total / per second) | 223 / 11.7 |
| controller API PATCHes | 194 |

## M (medium)

50 jobs, 2 classes, a share and siteLeft that admit only part at once: the rest wait (WaitingForBudget) and are admitted as bookings settle.

| measure | value |
|---|---|
| duration | 1m0s |
| final phases | map[Succeeded/Completed:50] |
| peak jobs waiting for budget | 9 |
| site submissions (all attempts) | 50 |
| jobs | 50 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 60 |
| peak (used+booked) / min(share, siteLeft) | pm-ml/tier0 100%, pm-ml/tier1 98% |
| project pm: controller used GPU+CPU node-min / site charged / bound | 755 / 732 / 1020 |
| rate limit meluxina: busiest window as share of the bucket | 24% |
| site calls meluxina | acct=135 acctlist=50 list=50 state=30 submit=50 (peak running 41, peak active 41, 422s 0, injected faults 0) |
| jobs/state calls | 30 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 450 |
| batching factor (IDs per call) | 15.0 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 2s / 0.0% |
| reconciles (total / per second) | 728 / 12.1 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 0.7 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 633 / 10.5 |
| controller API PATCHes | 594 |
| controller RSS MB (avg / max over run) | 65 / 68 |
| controller heap in use MB (avg / max over run) | 15 / 17 |
| RSS MB: first half / second half after warmup | 65 / 66 |
| heap MB: first half / second half after warmup | 15 / 15 |
| controller goroutines (max) | 88 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 26 / 0.43 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 26645 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 0s |
| duration (create to last terminal) | 2m49s |
| throughput (jobs/s) | 5.92 |
| final phases | map[Succeeded/Completed:1000] |
| site submissions (all attempts) | 1000 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 168 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5154 / 5003 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 5367 / 5213 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 5150 / 4996 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 52% |
| rate limit lumi: busiest window as share of the bucket | 38% |
| site calls meluxina | acct=1920 acctlist=667 list=667 ping=12 state=126 submit=667 (peak running 80, peak active 240, 422s 0, injected faults 0) |
| site calls lumi | acct=861 acctlist=333 list=333 ping=9 state=56 submit=333 (peak running 60, peak active 120, 422s 0, injected faults 0) |
| jobs/state calls | 182 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 9178 |
| batching factor (IDs per call) | 50.4 |
| jobs/state calls per poll cycle per HPCSite (avg) | 1.06 |
| runner submit calls / adoptions of a site job found by key | 1000 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 6s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 6s / 0.0% |
| reconciles (total / per second) | 15505 / 91.9 |
| reconcile errors | 2 |
| reconcile duration avg (ms) | 5.4 |
| workqueue depth (max / max after the first minute) | 1000 / 342 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.49 |
| controller API requests (total / per second) | 16790 / 99.5 |
| controller API PATCHes | 15740 |
| controller RSS MB (avg / max over run) | 111 / 128 |
| controller heap in use MB (avg / max over run) | 52 / 69 |
| RSS MB: first half / second half after warmup | 111 / 122 |
| heap MB: first half / second half after warmup | 52 / 60 |
| controller goroutines (max) | 90 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 90 / 0.53 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 92 / 0.55 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 80 / 0.47 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 137657 |
| lane placement (last attempt of each job) | map[default:979 dev:11 test:10] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 2 / 0 |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00017": the object has been modified; please apply your changes to the latest version and try again |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00079": the object has been modified; please apply your changes to the latest version and try again |
| published ledger pa (gpu used/booked, cpu used/booked, active) | 3591/0, 1563/0, 0 |
| published ledger pb (gpu used/booked, cpu used/booked, active) | 3725/0, 1642/0, 0 |
| published ledger pc (gpu used/booked, cpu used/booked, active) | 3629/0, 1521/0, 0 |

Failed assertions:

- HPCSite object grew to 137657 bytes

## L-chaos

L plus: controller restart mid-run, a site outage (all 5xx, 10 minutes) on the shared site, 422 storm (slurmctld forgets finished jobs + spurious 422) on the other, 10% NODE_FAIL, jobs that always fail (RerunLimit), checkpointing steps (continuations <= 5, one cohort that needs more).

| measure | value |
|---|---|
| chaos: controller killed (SIGTERM) and restarted at | t+12s, down about 3s |
| chaos: 422 storm on site lumi (jobs forgotten at once, 30% spurious 422) | t+25s, 60s |
| chaos: site meluxina down (all 5xx) | t+35s, 10m0s |
| HPCSite pa-ml SiteReachable (want false) | false |
| HPCSite pa-ml SiteReachable (want true) | true |
| duration (create to last terminal) | 13m59s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:13 Succeeded/Completed:975] |
| outage: peak jobs Waiting/SiteUnavailable | 62 |
| outage: runner Jobs created for the down site while it was down | 0 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 36 |
| tier-1 re-runs (total / max per job) | 112 / 2 |
| continuations (total) | 216 |
| jobs ended RerunLimit | 13 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 880 / 0 |
| site submissions (all attempts) | 1328 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 839 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 6094 / 5932 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 5982 / 5841 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 6034 / 5877 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 88% |
| rate limit lumi: busiest window as share of the bucket | 97% |
| site calls meluxina | acct=21959 acctlist=920 list=1230 ping=437 state=539 submit=892 (peak running 84, peak active 225, 422s 17, injected faults 20658) |
| site calls lumi | acct=1139 acctlist=463 list=477 ping=217 state=153 submit=446 (peak running 62, peak active 92, 422s 107, injected faults 38) |
| jobs/state calls | 692 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 8633 |
| batching factor (IDs per call) | 12.5 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.82 |
| runner submit calls / adoptions of a site job found by key | 1338 / 60 |
| runner site reads on a stale observation (fallback) | 20365 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 10m12s / 80.3% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m10s / 87.1% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 30s / 1.3% |
| reconciles (total / per second) | 21566 / 25.7 |
| reconcile errors | 479 |
| reconcile duration avg (ms) | 5.5 |
| workqueue depth (max / max after the first minute) | 1000 / 925 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.14 |
| controller API requests (total / per second) | 18319 / 21.8 |
| controller API PATCHes | 16270 |
| controller RSS MB (avg / max over run) | 128 / 139 |
| controller heap in use MB (avg / max over run) | 65 / 81 |
| RSS MB: first half / second half after warmup | 130 / 131 |
| heap MB: first half / second half after warmup | 67 / 68 |
| controller goroutines (max) | 92 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 159 / 0.19 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 158 / 0.19 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 143 / 0.17 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 147432 |
| lane placement (last attempt of each job) | map[default:974 dev:14 test:12] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 60 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 743 / 0 |
| controller log error sample | writing observation: Patch "https://127.0.0.1:36185/apis/hpc.kaos.io/v1alpha1/namespaces/pb-train/hpcargoworkflowjobs/j00058/status": context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | Reconciler error: Patch "https://127.0.0.1:36185/apis/hpc.kaos.io/v1alpha1/namespaces/pc-train/hpcargoworkflowjobs/j00074/status": context canceled |
| controller log error sample | Reconciler error: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |

Failed assertions:

- HPCSite object grew to 147432 bytes
