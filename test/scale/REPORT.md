# kaos-hpc-operator scale test report

Generated 2026-11-08T05:42:11Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S (small) | PASS |
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
| reconcile duration avg (ms) | 2.9 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.00 |
| controller API requests (total / per second) | 13 / 1.3 |
| controller API PATCHes | 10 |

## M (medium)

50 jobs, 2 classes, a share and siteLeft that admit only part at once: the rest wait (WaitingForBudget) and are admitted as bookings settle.

| measure | value |
|---|---|
| duration | 58s |
| final phases | map[Succeeded/Completed:50] |
| peak jobs waiting for budget | 10 |
| site submissions (all attempts) | 50 |
| jobs | 50 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 58 |
| peak (used+booked) / min(share, siteLeft) | pm-ml/tier0 100%, pm-ml/tier1 98% |
| project pm: controller used GPU+CPU node-min / site charged / bound | 755 / 732 / 1020 |
| rate limit meluxina: busiest window as share of the bucket | 24% |
| site calls meluxina | acct=132 acctlist=50 list=50 state=29 submit=50 (peak running 40, peak active 40, 422s 0, injected faults 0) |
| jobs/state calls | 29 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 451 |
| batching factor (IDs per call) | 15.6 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| reconciles (total / per second) | 736 / 12.6 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.9 |
| workqueue depth (max / max after the first minute) | 2 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 642 / 11.0 |
| controller API PATCHes | 603 |
| controller RSS MB (avg / max over run) | 66 / 68 |
| controller heap in use MB (avg / max over run) | 15 / 18 |
| RSS MB: first half / second half after warmup | 66 / 67 |
| heap MB: first half / second half after warmup | 14 / 16 |
| controller goroutines (max) | 90 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 26 / 0.44 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 23998 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 0s |
| duration (create to last terminal) | 41s |
| throughput (jobs/s) | 3.70 |
| final phases | map[Succeeded/Completed:150] |
| site submissions (all attempts) | 150 |
| jobs | 150 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 40 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 0%, pa-ml/tier1 0%, pb-ml/tier0 0%, pb-ml/tier1 0%, pc-ml/tier0 0%, pc-ml/tier1 0% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 795 / 772 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 772 / 752 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 741 / 716 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 33% |
| rate limit lumi: busiest window as share of the bucket | 17% |
| site calls meluxina | acct=229 acctlist=100 list=100 state=26 submit=100 (peak running 80, peak active 100, 422s 0, injected faults 0) |
| site calls lumi | acct=134 acctlist=50 list=50 ping=1 state=12 submit=50 (peak running 50, peak active 50, 422s 0, injected faults 0) |
| jobs/state calls | 38 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 1018 |
| batching factor (IDs per call) | 26.8 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.90 |
| runner submit calls / adoptions of a site job found by key | 150 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| reconciles (total / per second) | 2190 / 54.0 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.4 |
| workqueue depth (max / max after the first minute) | 150 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.08 |
| controller API requests (total / per second) | 1614 / 39.8 |
| controller API PATCHes | 1567 |
| controller RSS MB (avg / max over run) | 72 / 74 |
| controller heap in use MB (avg / max over run) | 19 / 21 |
| RSS MB: first half / second half after warmup | 72 / 73 |
| heap MB: first half / second half after warmup | 19 / 20 |
| controller goroutines (max) | 87 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 17 / 0.42 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 16 / 0.39 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 16 / 0.39 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 24097 |
| lane placement (last attempt of each job) | map[default:144 dev:3 test:3] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 0 / 0 |
| published ledger pa (gpu used/booked, cpu used/booked, active) | 534/0, 261/0, 0 |
| published ledger pb (gpu used/booked, cpu used/booked, active) | 584/0, 188/0, 0 |
| published ledger pc (gpu used/booked, cpu used/booked, active) | 479/0, 262/0, 0 |

## L-chaos

L plus: controller restart mid-run, a site outage (all 5xx, 10 minutes) on the shared site, 422 storm (slurmctld forgets finished jobs + spurious 422) on the other, 10% NODE_FAIL, jobs that always fail (RerunLimit), checkpointing steps (continuations <= 5, one cohort that needs more).

| measure | value |
|---|---|
| chaos: controller killed (SIGTERM) and restarted at | t+12s, down about 3s |
| chaos: 422 storm on site lumi (jobs forgotten at once, 30% spurious 422) | t+25s, 60s |
| chaos: site meluxina down (all 5xx) | t+35s, 10m0s |
| HPCSite pa-ml SiteReachable (want false) | false |
| HPCSite pa-ml SiteReachable (want true) | true |
| duration (create to last terminal) | 11m59s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:12 Succeeded/Completed:126] |
| outage: peak jobs Waiting/SiteUnavailable | 16 |
| outage: runner Jobs created for the down site while it was down | 0 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 11 |
| tier-1 re-runs (total / max per job) | 28 / 2 |
| continuations (total) | 220 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 30 / 0 |
| site submissions (all attempts) | 398 |
| jobs | 150 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 719 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 1%, pa-ml/tier1 0%, pb-ml/tier0 1%, pb-ml/tier1 0%, pc-ml/tier0 1%, pc-ml/tier1 0% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 1295 / 1279 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 1290 / 1272 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 1276 / 1264 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 88% |
| rate limit lumi: busiest window as share of the bucket | 94% |
| site calls meluxina | acct=5373 acctlist=277 list=293 ping=417 state=478 submit=269 (peak running 81, peak active 98, 422s 17, injected faults 5578) |
| site calls lumi | acct=309 acctlist=140 list=148 ping=197 state=124 submit=134 (peak running 44, peak active 45, 422s 86, injected faults 20) |
| jobs/state calls | 602 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 1918 |
| batching factor (IDs per call) | 3.2 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.84 |
| runner submit calls / adoptions of a site job found by key | 403 / 16 |
| runner site reads on a stale observation (fallback) | 4933 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 10m8s / 62.6% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m8s / 86.3% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 23s / 2.3% |
| reconciles (total / per second) | 6124 / 8.5 |
| reconcile errors | 1 |
| reconcile duration avg (ms) | 2.7 |
| workqueue depth (max / max after the first minute) | 149 / 14 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 4700 / 6.5 |
| controller API PATCHes | 3687 |
| controller RSS MB (avg / max over run) | 84 / 86 |
| controller heap in use MB (avg / max over run) | 27 / 33 |
| RSS MB: first half / second half after warmup | 84 / 85 |
| heap MB: first half / second half after warmup | 27 / 28 |
| controller goroutines (max) | 88 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 61 / 0.08 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 57 / 0.08 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 70 / 0.10 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 24483 |
| lane placement (last attempt of each job) | map[default:138 dev:9 test:3] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 16 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 454 / 0 |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00049": the object has been modified; please apply your changes to the latest version and try again |
| controller log error sample | writing observation: Patch "https://127.0.0.1:46139/apis/hpc.kaos.io/v1alpha1/namespaces/pb-train/hpcargoworkflowjobs/j00049/status": context canceled |
| controller log error sample | Reconciler error: context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
