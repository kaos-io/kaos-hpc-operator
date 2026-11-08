# kaos-hpc-operator scale test report

Generated 2026-11-08T01:04:19Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

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
| reconcile duration avg (ms) | 3.0 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.00 |
| controller API requests (total / per second) | 13 / 1.3 |
| controller API PATCHes | 10 |

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
| site calls meluxina | acct=132 acctlist=50 list=50 state=30 submit=50 (peak running 40, peak active 40, 422s 0, injected faults 0) |
| jobs/state calls | 30 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 451 |
| batching factor (IDs per call) | 15.0 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 2s / 0.0% |
| reconciles (total / per second) | 733 / 12.2 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 0.9 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 641 / 10.6 |
| controller API PATCHes | 601 |
| controller RSS MB (avg / max over run) | 66 / 68 |
| controller heap in use MB (avg / max over run) | 15 / 17 |
| RSS MB: first half / second half after warmup | 66 / 67 |
| heap MB: first half / second half after warmup | 15 / 15 |
| controller goroutines (max) | 88 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 27 / 0.45 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 23988 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 0s |
| duration (create to last terminal) | 43s |
| throughput (jobs/s) | 3.52 |
| final phases | map[Succeeded/Completed:150] |
| site submissions (all attempts) | 150 |
| jobs | 150 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 42 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 0%, pa-ml/tier1 0%, pb-ml/tier0 0%, pb-ml/tier1 0%, pc-ml/tier0 0%, pc-ml/tier1 0% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 795 / 772 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 772 / 752 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 741 / 716 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 33% |
| rate limit lumi: busiest window as share of the bucket | 17% |
| site calls meluxina | acct=239 acctlist=100 list=100 state=28 submit=100 (peak running 80, peak active 100, 422s 0, injected faults 0) |
| site calls lumi | acct=122 acctlist=50 list=50 ping=2 state=12 submit=50 (peak running 50, peak active 50, 422s 0, injected faults 0) |
| jobs/state calls | 40 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 1013 |
| batching factor (IDs per call) | 25.3 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.89 |
| runner submit calls / adoptions of a site job found by key | 150 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 4s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 4s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 4s / 0.0% |
| reconciles (total / per second) | 2188 / 51.4 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.3 |
| workqueue depth (max / max after the first minute) | 150 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.07 |
| controller API requests (total / per second) | 1592 / 37.4 |
| controller API PATCHes | 1545 |
| controller RSS MB (avg / max over run) | 72 / 74 |
| controller heap in use MB (avg / max over run) | 19 / 23 |
| RSS MB: first half / second half after warmup | 71 / 73 |
| heap MB: first half / second half after warmup | 19 / 20 |
| controller goroutines (max) | 87 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 16 / 0.38 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 16 / 0.38 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 16 / 0.38 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 24033 |
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
| duration (create to last terminal) | 12m53s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:12 Succeeded/Completed:126] |
| outage: peak jobs Waiting/SiteUnavailable | 18 |
| outage: runner Jobs created for the down site while it was down | 0 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 7 |
| tier-1 re-runs (total / max per job) | 31 / 2 |
| continuations (total) | 217 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 30 / 0 |
| site submissions (all attempts) | 398 |
| jobs | 150 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 773 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 1%, pa-ml/tier1 0%, pb-ml/tier0 1%, pb-ml/tier1 0%, pc-ml/tier0 1%, pc-ml/tier1 0% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 1270 / 1255 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 1305 / 1290 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 1306 / 1292 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 50% |
| rate limit lumi: busiest window as share of the bucket | 88% |
| site calls meluxina | acct=5656 acctlist=274 list=311 ping=36 state=478 submit=269 (peak running 80, peak active 96, 422s 0, injected faults 5453) |
| site calls lumi | acct=355 acctlist=140 list=149 ping=199 state=118 submit=133 (peak running 42, peak active 46, 422s 63, injected faults 16) |
| jobs/state calls | 596 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 1785 |
| batching factor (IDs per call) | 3.0 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.77 |
| runner submit calls / adoptions of a site job found by key | 402 / 22 |
| runner site reads on a stale observation (fallback) | 5069 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 10m6s / 89.7% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m0s / 89.5% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 16s / 3.6% |
| reconciles (total / per second) | 6513 / 8.4 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 2.5 |
| workqueue depth (max / max after the first minute) | 150 / 17 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 4891 / 6.3 |
| controller API PATCHes | 3843 |
| controller RSS MB (avg / max over run) | 82 / 86 |
| controller heap in use MB (avg / max over run) | 27 / 33 |
| RSS MB: first half / second half after warmup | 83 / 84 |
| heap MB: first half / second half after warmup | 27 / 28 |
| controller goroutines (max) | 89 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 63 / 0.08 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 73 / 0.09 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 90 / 0.12 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 24304 |
| lane placement (last attempt of each job) | map[default:138 dev:8 test:4] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 22 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 473 / 0 |
| controller log error sample | writing observation: context canceled |
| controller log error sample | Reconciler error: Patch "https://127.0.0.1:39475/apis/hpc.kaos.io/v1alpha1/namespaces/pa-train/hpcargoworkflowjobs/j00026/status": context canceled |
| controller log error sample | Reconciler error: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
| controller log error sample | site poll failed; observations left as they were: jobs/state: HTTP 503: site unavailable site unavailable |
