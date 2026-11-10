# kaos-hpc-operator scale test report

Generated 2026-11-10T06:09:33Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S (small) | PASS |
| S-shared (node sharing) | PASS |
| M (medium) | PASS |
| L (large) | PASS |
| L-chaos | PASS |

## S (small)

1 job end to end: admit, book, runner Job, submit, observe, settle.

| measure | value |
|---|---|
| duration | 10.015s |
| final phases | map[Succeeded/Completed:1] |
| site submissions (all attempts) | 1 |
| jobs | 1 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 10 |
| peak (used+booked) / min(share, siteLeft) | ps-ml/tier0 6%, ps-ml/tier1 0% |
| project ps: controller used GPU+CPU node-min / site charged / bound | 3 / 3 / 960 |
| reconciles (total / per second) | 7 / 0.7 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 2.6 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.00 |
| controller API requests (total / per second) | 13 / 1.3 |
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
| reconciles (total / per second) | 277 / 13.8 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.6 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 225 / 11.2 |
| controller API PATCHes | 196 |

## M (medium)

50 jobs, 2 classes, a share and siteLeft that admit only part at once: the rest wait (WaitingForBudget) and are admitted as bookings settle.

| measure | value |
|---|---|
| duration | 58s |
| final phases | map[Succeeded/Completed:50] |
| peak jobs waiting for budget | 9 |
| site submissions (all attempts) | 50 |
| jobs | 50 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 58 |
| peak (used+booked) / min(share, siteLeft) | pm-ml/tier0 100%, pm-ml/tier1 100% |
| project pm: controller used GPU+CPU node-min / site charged / bound | 755 / 732 / 1020 |
| rate limit meluxina: busiest window as share of the bucket | 24% |
| site calls meluxina | acct=133 acctlist=50 list=50 state=29 submit=50 (peak running 41, peak active 41, 422s 0, injected faults 0) |
| jobs/state calls | 29 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 451 |
| batching factor (IDs per call) | 15.6 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 2s / 0.0% |
| reconciles (total / per second) | 722 / 12.4 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 0.8 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 627 / 10.8 |
| controller API PATCHes | 589 |
| controller RSS MB (avg / max over run) | 67 / 68 |
| controller heap in use MB (avg / max over run) | 15 / 17 |
| RSS MB: first half / second half after warmup | 67 / 68 |
| heap MB: first half / second half after warmup | 15 / 15 |
| controller goroutines (max) | 89 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 25 / 0.43 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 26552 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 0s |
| duration (create to last terminal) | 2m48s |
| throughput (jobs/s) | 5.94 |
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
| site calls meluxina | acct=1899 acctlist=667 list=667 ping=15 state=123 submit=667 (peak running 80, peak active 240, 422s 0, injected faults 0) |
| site calls lumi | acct=864 acctlist=333 list=333 ping=14 state=51 submit=333 (peak running 60, peak active 120, 422s 0, injected faults 0) |
| jobs/state calls | 174 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 9389 |
| batching factor (IDs per call) | 54.0 |
| jobs/state calls per poll cycle per HPCSite (avg) | 1.02 |
| runner submit calls / adoptions of a site job found by key | 1000 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 6s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 6s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| reconciles (total / per second) | 15955 / 94.7 |
| reconcile errors | 2 |
| reconcile duration avg (ms) | 5.1 |
| workqueue depth (max / max after the first minute) | 1000 / 366 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.49 |
| controller API requests (total / per second) | 16960 / 100.7 |
| controller API PATCHes | 15910 |
| controller RSS MB (avg / max over run) | 112 / 128 |
| controller heap in use MB (avg / max over run) | 51 / 70 |
| RSS MB: first half / second half after warmup | 112 / 123 |
| heap MB: first half / second half after warmup | 51 / 60 |
| controller goroutines (max) | 90 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 82 / 0.49 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 95 / 0.56 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 83 / 0.49 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 137643 |
| lane placement (last attempt of each job) | map[default:980 dev:9 test:11] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 2 / 0 |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00020": the object has been modified; please apply your changes to the latest version and try again |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00123": the object has been modified; please apply your changes to the latest version and try again |
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
| duration (create to last terminal) | 13m52s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:12 Succeeded/Completed:976] |
| outage: peak jobs Waiting/SiteUnavailable | 30 |
| outage: runner Jobs created for the down site while it was down | 0 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 27 |
| tier-1 re-runs (total / max per job) | 138 / 2 |
| continuations (total) | 212 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 880 / 0 |
| site submissions (all attempts) | 1350 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 832 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5994 / 5841 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 6452 / 6293 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 6045 / 5884 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 81% |
| rate limit lumi: busiest window as share of the bucket | 91% |
| site calls meluxina | acct=16782 acctlist=942 list=1645 ping=428 state=550 submit=917 (peak running 82, peak active 228, 422s 17, injected faults 15627) |
| site calls lumi | acct=1254 acctlist=464 list=475 ping=230 state=101 submit=447 (peak running 61, peak active 115, 422s 63, injected faults 42) |
| jobs/state calls | 651 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 9780 |
| batching factor (IDs per call) | 15.0 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.78 |
| runner submit calls / adoptions of a site job found by key | 1364 / 50 |
| runner site reads on a stale observation (fallback) | 14590 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 10m11s / 81.4% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m7s / 77.7% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 22s / 2.4% |
| reconciles (total / per second) | 22447 / 27.0 |
| reconcile errors | 409 |
| reconcile duration avg (ms) | 5.3 |
| workqueue depth (max / max after the first minute) | 1000 / 692 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.14 |
| controller API requests (total / per second) | 20496 / 24.6 |
| controller API PATCHes | 18425 |
| controller RSS MB (avg / max over run) | 130 / 140 |
| controller heap in use MB (avg / max over run) | 67 / 80 |
| RSS MB: first half / second half after warmup | 132 / 133 |
| heap MB: first half / second half after warmup | 69 / 69 |
| controller goroutines (max) | 91 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 140 / 0.17 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 192 / 0.23 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 122 / 0.15 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 147848 |
| lane placement (last attempt of each job) | map[default:973 dev:12 test:15] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 50 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 766 / 0 |
| controller log error sample | writing observation: Patch "https://127.0.0.1:42009/apis/hpc.kaos.io/v1alpha1/namespaces/pa-train/hpcargoworkflowjobs/j00106/status": context canceled |
| controller log error sample | Reconciler error: Patch "https://127.0.0.1:42009/apis/hpc.kaos.io/v1alpha1/namespaces/pc-train/hpcargoworkflowjobs/j00157": context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | Reconciler error: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
