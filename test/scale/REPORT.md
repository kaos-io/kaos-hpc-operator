# kaos-hpc-operator scale test report

Generated 2026-11-10T17:16:24Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S (small) | PASS |
| S-shared (node sharing) | PASS |
| S-shared-resumable (node sharing with resumable steps) | PASS |
| M (medium) | PASS |
| L (large) | PASS |
| L-chaos | FAIL (1 assertions) |

## S (small)

1 job end to end: admit, book, runner Job, submit, observe, settle.

| measure | value |
|---|---|
| duration | 8.016s |
| final phases | map[Succeeded/Completed:1] |
| site submissions (all attempts) | 1 |
| jobs | 1 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 8 |
| peak (used+booked) / min(share, siteLeft) | ps-ml/tier0 6%, ps-ml/tier1 0% |
| project ps: controller used GPU+CPU node-min / site charged / bound | 3 / 3 / 960 |
| reconciles (total / per second) | 4 / 0.5 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 5.6 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.00 |
| controller API requests (total / per second) | 10 / 1.2 |
| controller API PATCHes | 7 |

## S-shared (node sharing)

8 hosts whose Slurm job keeps the node 6-14 minutes after the step for 16 guests that never start on their own: the ledger covers the site's charge at every snapshot and equals it at the end.

| measure | value |
|---|---|
| duration | 19s |
| final phases | map[Succeeded/Completed:24] |
| hosts settled to their job's end / guests settled 0 | 8 of 8 / 16 of 16 |
| guests with a step-time sample (start from the host) | 16 of 16 |
| controller settled node-min / site charged | 111 / 107.6 |
| site submissions (all attempts) | 24 |
| jobs | 24 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 19 |
| peak (used+booked) / min(share, siteLeft) | psh-ml/tier0 13%, psh-ml/tier1 0% |
| project psh: controller used GPU+CPU node-min / site charged / bound | 111 / 108 / 5880 |
| published ledger psh (gpu used/booked, cpu used/booked, active) | 111/0, 0/0, 0 |
| reconciles (total / per second) | 275 / 14.4 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 2.5 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.04 |
| controller API requests (total / per second) | 228 / 11.9 |
| controller API PATCHes | 199 |

## S-shared-resumable (node sharing with resumable steps)

10 hosts (6 resumable with 3 segments, 4 plain) and 30 guests (20 resumable with 2-3 segments, 10 plain) that run 8-16 minutes on hosts that end sooner: resumable guests are signalled and continued, plain ones fit or run alone; the ledger equals the site's charge.

| measure | value |
|---|---|
| duration | 1m50s |
| final phases | map[Succeeded/Completed:40] |
| guests taken by a host / resumable guests signalled before their run was over | 171 / 135 |
| resumable jobs continued / plain jobs that continued | 26 of 26 / 0 |
| controller settled node-min / site charged | 708 / 693.4 |
| site submissions (all attempts) | 207 |
| jobs | 40 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 110 |
| peak (used+booked) / min(share, siteLeft) | psr-ml/tier0 0%, psr-ml/tier1 0% |
| project psr: controller used GPU+CPU node-min / site charged / bound | 708 / 693 / 5400480 |
| published ledger psr (gpu used/booked, cpu used/booked, active) | 708/0, 0/0, 0 |
| reconciles (total / per second) | 2638 / 24.0 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 3.1 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.07 |
| controller API requests (total / per second) | 2335 / 21.2 |
| controller API PATCHes | 2074 |

## M (medium)

50 jobs, 2 classes, a share and siteLeft that admit only part at once: the rest wait (WaitingForBudget) and are admitted as bookings settle.

| measure | value |
|---|---|
| duration | 58s |
| final phases | map[Succeeded/Completed:50] |
| peak jobs waiting for budget | 11 |
| site submissions (all attempts) | 50 |
| jobs | 50 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 58 |
| peak (used+booked) / min(share, siteLeft) | pm-ml/tier0 99%, pm-ml/tier1 98% |
| project pm: controller used GPU+CPU node-min / site charged / bound | 755 / 732 / 1020 |
| rate limit meluxina: busiest window as share of the bucket | 24% |
| site calls meluxina | acct=137 acctlist=50 list=50 state=29 submit=50 (peak running 39, peak active 39, 422s 0, injected faults 0) |
| jobs/state calls | 29 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 450 |
| batching factor (IDs per call) | 15.5 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 3s / 0.0% |
| reconciles (total / per second) | 731 / 12.5 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.3 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 635 / 10.9 |
| controller API PATCHes | 595 |
| controller RSS MB (avg / max over run) | 67 / 68 |
| controller heap in use MB (avg / max over run) | 15 / 18 |
| RSS MB: first half / second half after warmup | 67 / 68 |
| heap MB: first half / second half after warmup | 15 / 15 |
| controller goroutines (max) | 88 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 28 / 0.48 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 26552 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 2s |
| duration (create to last terminal) | 3m1s |
| throughput (jobs/s) | 5.51 |
| final phases | map[Succeeded/Completed:1000] |
| site submissions (all attempts) | 1000 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 181 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5154 / 5003 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 5367 / 5213 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 5150 / 4996 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 44% |
| rate limit lumi: busiest window as share of the bucket | 25% |
| site calls meluxina | acct=1877 acctlist=667 list=667 ping=23 state=103 submit=667 (peak running 80, peak active 171, 422s 0, injected faults 0) |
| site calls lumi | acct=901 acctlist=333 list=333 ping=10 state=49 submit=333 (peak running 60, peak active 118, 422s 0, injected faults 0) |
| jobs/state calls | 152 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 6893 |
| batching factor (IDs per call) | 45.3 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.83 |
| runner submit calls / adoptions of a site job found by key | 1000 / 0 |
| runner site reads on a stale observation (fallback) | 240 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 14s / 1.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 12s / 2.6% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 12s / 2.2% |
| reconciles (total / per second) | 5911 / 32.6 |
| reconcile errors | 1 |
| reconcile duration avg (ms) | 26.9 |
| workqueue depth (max / max after the first minute) | 1000 / 999 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.88 |
| controller API requests (total / per second) | 14097 / 77.7 |
| controller API PATCHes | 12950 |
| controller RSS MB (avg / max over run) | 107 / 132 |
| controller heap in use MB (avg / max over run) | 49 / 70 |
| RSS MB: first half / second half after warmup | 106 / 122 |
| heap MB: first half / second half after warmup | 48 / 62 |
| controller goroutines (max) | 82 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 72 / 0.40 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 80 / 0.44 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 82 / 0.45 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 137653 |
| lane placement (last attempt of each job) | map[default:982 dev:9 test:9] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 1 / 0 |
| controller log error sample | Reconciler error: Operation cannot be fulfilled on hpcargoworkflowjobs.hpc.kaos.io "j00219": the object has been modified; please apply your changes to the latest version and try again |
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
| duration (create to last terminal) | 14m59s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:12 Succeeded/Completed:976] |
| outage: peak jobs Waiting/SiteUnavailable | 159 |
| outage: runner Jobs created for the down site while it was down | 130 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 45 |
| tier-1 re-runs (total / max per job) | 113 / 2 |
| continuations (total) | 213 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 880 / 0 |
| site submissions (all attempts) | 1326 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 900 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 3%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5855 / 5702 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 6150 / 5996 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 5904 / 5743 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 88% |
| rate limit lumi: busiest window as share of the bucket | 97% |
| site calls meluxina | acct=8117 acctlist=927 list=3237 ping=433 state=571 submit=895 (peak running 83, peak active 162, 422s 15, injected faults 8872) |
| site calls lumi | acct=1210 acctlist=468 list=486 ping=217 state=175 submit=444 (peak running 61, peak active 95, 422s 97, injected faults 46) |
| jobs/state calls | 746 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 7793 |
| batching factor (IDs per call) | 10.4 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.83 |
| runner submit calls / adoptions of a site job found by key | 1339 / 72 |
| runner site reads on a stale observation (fallback) | 6524 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 10m8s / 20.7% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m5s / 80.9% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 36s / 7.3% |
| reconciles (total / per second) | 25164 / 28.0 |
| reconcile errors | 700 |
| reconcile duration avg (ms) | 13.1 |
| workqueue depth (max / max after the first minute) | 1000 / 999 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.37 |
| controller API requests (total / per second) | 18529 / 20.6 |
| controller API PATCHes | 16339 |
| controller RSS MB (avg / max over run) | 125 / 152 |
| controller heap in use MB (avg / max over run) | 63 / 94 |
| RSS MB: first half / second half after warmup | 128 / 132 |
| heap MB: first half / second half after warmup | 65 / 68 |
| controller goroutines (max) | 95 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 283 / 0.31 / 2 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 289 / 0.32 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 229 / 0.25 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 149961 |
| lane placement (last attempt of each job) | map[default:976 dev:14 test:10] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 72 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 614 / 0 |
| controller log error sample | writing observation: Patch "https://127.0.0.1:39977/apis/hpc.kaos.io/v1alpha1/namespaces/pa-train/hpcargoworkflowjobs/j00077/status": context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
| controller log error sample | probe: no credentials: get Slurm token Secret: context canceled |
| controller log error sample | Reconciler error: context canceled |

Failed assertions:

- 130 runner Jobs were created for the down site during the outage: the controller must stop spawning attempts
