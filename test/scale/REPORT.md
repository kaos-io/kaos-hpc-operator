# kaos-hpc-operator scale test report

Generated 2026-11-07T23:29:35Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

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
| duration | 10.014s |
| final phases | map[Succeeded/Completed:1] |
| site submissions (all attempts) | 1 |
| jobs | 1 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 10 |
| peak (used+booked) / min(share, siteLeft) | ps-ml/tier0 6%, ps-ml/tier1 0% |
| project ps: controller used GPU+CPU node-min / site charged / bound | 3 / 3 / 960 |
| reconciles (total / per second) | 7 / 0.7 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 3.8 |
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
| peak jobs waiting for budget | 11 |
| site submissions (all attempts) | 50 |
| jobs | 50 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 60 |
| peak (used+booked) / min(share, siteLeft) | pm-ml/tier0 99%, pm-ml/tier1 98% |
| project pm: controller used GPU+CPU node-min / site charged / bound | 755 / 732 / 1020 |
| rate limit meluxina: busiest window as share of the bucket | 24% |
| site calls meluxina | acct=130 list=50 state=30 submit=50 (peak running 39, peak active 39, 422s 0, injected faults 0) |
| jobs/state calls | 30 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 449 |
| batching factor (IDs per call) | 15.0 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| reconciles (total / per second) | 728 / 12.1 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 0.9 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 636 / 10.5 |
| controller API PATCHes | 596 |
| controller RSS MB (avg / max over run) | 66 / 68 |
| controller heap in use MB (avg / max over run) | 14 / 17 |
| RSS MB: first half / second half after warmup | 66 / 67 |
| heap MB: first half / second half after warmup | 14 / 14 |
| controller goroutines (max) | 85 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 27 / 0.45 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 23130 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 0s |
| duration (create to last terminal) | 2m49s |
| throughput (jobs/s) | 5.91 |
| final phases | map[Succeeded/Completed:1000] |
| site submissions (all attempts) | 1000 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 169 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5154 / 5003 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 5367 / 5213 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 5150 / 4996 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 53% |
| rate limit lumi: busiest window as share of the bucket | 25% |
| site calls meluxina | acct=1935 list=667 state=125 submit=667 (peak running 80, peak active 239, 422s 0, injected faults 0) |
| site calls lumi | acct=865 list=333 state=55 submit=333 (peak running 60, peak active 120, 422s 0, injected faults 0) |
| jobs/state calls | 180 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 9511 |
| batching factor (IDs per call) | 52.8 |
| jobs/state calls per poll cycle per HPCSite (avg) | 1.05 |
| runner submit calls / adoptions of a site job found by key | 1000 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 4s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| reconciles (total / per second) | 19131 / 113.0 |
| reconcile errors | 1 |
| reconcile duration avg (ms) | 2.6 |
| workqueue depth (max / max after the first minute) | 1000 / 213 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.29 |
| controller API requests (total / per second) | 17174 / 101.4 |
| controller API PATCHes | 16138 |
| controller RSS MB (avg / max over run) | 104 / 118 |
| controller heap in use MB (avg / max over run) | 46 / 64 |
| RSS MB: first half / second half after warmup | 104 / 115 |
| heap MB: first half / second half after warmup | 46 / 55 |
| controller goroutines (max) | 88 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 70 / 0.41 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 78 / 0.46 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 65 / 0.38 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 23308 |
| lane placement (last attempt of each job) | map[default:979 dev:9 test:12] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 1 / 0 |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00244": the object has been modified; please apply your changes to the latest version and try again |
| published ledger pa (gpu used/booked, cpu used/booked, active) | 3591/0, 1563/0, 0 |
| published ledger pb (gpu used/booked, cpu used/booked, active) | 3725/0, 1642/0, 0 |
| published ledger pc (gpu used/booked, cpu used/booked, active) | 3629/0, 1521/0, 0 |

## L-chaos

L plus: controller restart mid-run, 2-minute site outage (all 5xx) on the shared site, 422 storm (slurmctld forgets finished jobs + spurious 422) on the other, 10% NODE_FAIL, jobs that always fail (RerunLimit), checkpointing steps (continuations <= 5, one cohort that needs more).

| measure | value |
|---|---|
| chaos: controller killed (SIGTERM) and restarted at | t+12s, down about 3s |
| chaos: 422 storm on site lumi (jobs forgotten at once, 30% spurious 422) | t+25s, 60s |
| chaos: site meluxina down (all 5xx) | t+35s, 2m |
| duration (create to last terminal) | 4m59s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:12 Succeeded/Completed:976] |
| tier-1 re-runs (total / max per job) | 134 / 2 |
| continuations (total) | 215 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 880 / 0 |
| site submissions (all attempts) | 1349 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 299 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 6188 / 6028 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 6333 / 6173 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 6201 / 6037 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 50% |
| rate limit lumi: busiest window as share of the bucket | 97% |
| site calls meluxina | acct=5548 list=1214 state=218 submit=906 (peak running 80, peak active 235, 422s 0, injected faults 3424) |
| site calls lumi | acct=1502 list=465 state=184 submit=447 (peak running 60, peak active 115, 422s 129, injected faults 32) |
| jobs/state calls | 402 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 10998 |
| batching factor (IDs per call) | 27.4 |
| jobs/state calls per poll cycle per HPCSite (avg) | 1.34 |
| runner submit calls / adoptions of a site job found by key | 1353 / 11 |
| runner site reads on a stale observation (fallback) | 3883 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 2m3s / 51.1% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 2m6s / 43.5% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 35s / 8.5% |
| reconciles (total / per second) | 22339 / 74.7 |
| reconcile errors | 589 |
| reconcile duration avg (ms) | 3.1 |
| workqueue depth (max / max after the first minute) | 1000 / 152 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.23 |
| controller API requests (total / per second) | 18412 / 61.6 |
| controller API PATCHes | 16898 |
| controller RSS MB (avg / max over run) | 115 / 132 |
| controller heap in use MB (avg / max over run) | 54 / 71 |
| RSS MB: first half / second half after warmup | 116 / 125 |
| heap MB: first half / second half after warmup | 55 / 62 |
| controller goroutines (max) | 89 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 107 / 0.36 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 114 / 0.38 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 124 / 0.41 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 23451 |
| lane placement (last attempt of each job) | map[default:976 dev:10 test:14] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 11 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 419 / 0 |
| controller log error sample | writing observation: Patch "https://127.0.0.1:39687/apis/hpc.kaos.io/v1alpha1/namespaces/pa-train/hpcargoworkflowjobs/j00004/status": context canceled |
| controller log error sample | publishing the ledger: Patch "https://127.0.0.1:39687/apis/hpc.kaos.io/v1alpha1/namespaces/pa-ml/hpcsites/site/status": context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | Reconciler error: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
