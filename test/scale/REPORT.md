# kaos-hpc-operator scale test report

Generated 2026-11-11T02:06:43Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S (small) | PASS |
| S-shared (node sharing) | PASS |
| S-shared-resumable (node sharing with resumable steps) | PASS |
| S-pack (deterministic node sharing) | PASS |
| M (medium) | PASS |
| L (large) | PASS |
| L-chaos | PASS |

## S (small)

1 job end to end: admit, book, runner Job, submit, observe, settle.

| measure | value |
|---|---|
| duration | 10.012s |
| final phases | map[Succeeded/Completed:1] |
| site submissions (all attempts) | 1 |
| jobs | 1 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 10 |
| peak (used+booked) / min(share, siteLeft) | ps-ml/tier0 6%, ps-ml/tier1 0% |
| project ps: controller used GPU+CPU node-min / site charged / bound | 3 / 3 / 960 |
| reconciles (total / per second) | 7 / 0.7 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 2.3 |
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
| reconciles (total / per second) | 275 / 13.7 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.7 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 226 / 11.2 |
| controller API PATCHes | 197 |

## S-shared-resumable (node sharing with resumable steps)

10 hosts (6 resumable with 3 segments, 4 plain) and 30 guests (20 resumable with 2-3 segments, 10 plain) that run 8-16 minutes on hosts that end sooner: resumable guests are signalled and continued, plain ones fit or run alone; the ledger equals the site's charge.

| measure | value |
|---|---|
| duration | 1m49s |
| final phases | map[Succeeded/Completed:40] |
| guests taken by a host / resumable guests signalled before their run was over | 75 / 26 |
| resumable jobs continued / plain jobs that continued | 26 of 26 / 0 |
| controller settled node-min / site charged | 533 / 523.1 |
| site submissions (all attempts) | 98 |
| jobs | 40 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 108 |
| peak (used+booked) / min(share, siteLeft) | psr-ml/tier0 0%, psr-ml/tier1 0% |
| project psr: controller used GPU+CPU node-min / site charged / bound | 533 / 523 / 5400480 |
| published ledger psr (gpu used/booked, cpu used/booked, active) | 533/0, 0/0, 0 |
| reconciles (total / per second) | 1457 / 13.4 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.5 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.02 |
| controller API requests (total / per second) | 1340 / 12.3 |
| controller API PATCHes | 1189 |

## S-pack (deterministic node sharing)

4, 8 and 10 shareable 1-GPU steps arrive at once on sites with ample free nodes (4 GPUs per node): each run uses ceil(N/4) nodes, the rest ride as guests; every step succeeds; the site's charge is about one node's time per host, not one per step.

| measure | value |
|---|---|
| duration | 12s |
| final phases | map[Succeeded/Completed:22] |
| 4 steps: plan |  j00000:guest>ppk4-train/j00001@4000001/qos=/host= j00001:host>@4000002/qos=dev/host= j00002:guest>ppk4-train/j00001@4000003/qos=/host= j00003:guest>ppk4-train/j00001@4000000/qos=/host= |
| 4 steps: nodes started at the site / hosts / guests (want 1 nodes) | 1 / 1 / 3 |
| 4 steps: site charge, node-minutes (settled 8) | 7.6 |
| 8 steps: plan |  j00000:guest>ppk8-train/j00007@4000000/qos=/host= j00001:guest>ppk8-train/j00007@4000004/qos=/host= j00002:guest>ppk8-train/j00004@4000003/qos=/host= j00003:guest>ppk8-train/j00007@4000002/qos=/host= j00004:host>@4000005/qos=/host= j00005:guest>ppk8-train/j00004@4000006/qos=/host= j00006:guest>ppk8-train/j00004@4000001/qos=/host= j00007:host>@4000007/qos=dev/host= |
| 8 steps: nodes started at the site / hosts / guests (want 2 nodes) | 2 / 2 / 6 |
| 8 steps: site charge, node-minutes (settled 14) | 13.5 |
| 10 steps: plan |  j00000:guest>ppk10-train/j00005@4000003/qos=/host= j00001:guest>ppk10-train/j00009@4000004/qos=/host= j00002:host>@4000002/qos=/host= j00003:guest>ppk10-train/j00002@4000001/qos=/host= j00004:guest>ppk10-train/j00005@4000005/qos=/host= j00005:host>@4000009/qos=dev/host= j00006:guest>ppk10-train/j00009@4000000/qos=/host= j00007:guest>ppk10-train/j00005@4000007/qos=/host= j00008:guest>ppk10-train/j00009@4000006/qos=/host= j00009:host>@4000008/qos=/host= |
| 10 steps: nodes started at the site / hosts / guests (want 3 nodes) | 3 / 3 / 7 |
| 10 steps: site charge, node-minutes (settled 23) | 22.0 |
| site submissions (all attempts) | 22 |
| jobs | 22 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 12 |
| peak (used+booked) / min(share, siteLeft) | ppk10-ml/tier0 6%, ppk10-ml/tier1 0%, ppk4-ml/tier0 2%, ppk4-ml/tier1 0%, ppk8-ml/tier0 4%, ppk8-ml/tier1 0% |
| project ppk10: controller used GPU+CPU node-min / site charged / bound | 23 / 22 / 5880 |
| project ppk4: controller used GPU+CPU node-min / site charged / bound | 8 / 8 / 5880 |
| project ppk8: controller used GPU+CPU node-min / site charged / bound | 14 / 14 / 5880 |
| published ledger ppk10 (gpu used/booked, cpu used/booked, active) | 23/0, 0/0, 0 |
| published ledger ppk4 (gpu used/booked, cpu used/booked, active) | 8/0, 0/0, 0 |
| published ledger ppk8 (gpu used/booked, cpu used/booked, active) | 14/0, 0/0, 0 |
| reconciles (total / per second) | 221 / 10.0 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.1 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 183 / 8.3 |
| controller API PATCHes | 162 |

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
| site calls meluxina | acct=138 acctlist=50 list=50 state=29 submit=50 (peak running 40, peak active 40, 422s 0, injected faults 0) |
| jobs/state calls | 29 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 454 |
| batching factor (IDs per call) | 15.7 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.97 |
| runner submit calls / adoptions of a site job found by key | 50 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pm-train: oldest / share older than 3 poll intervals | 2s / 0.0% |
| reconciles (total / per second) | 734 / 12.6 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 0.7 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.01 |
| controller API requests (total / per second) | 636 / 10.9 |
| controller API PATCHes | 597 |
| controller RSS MB (avg / max over run) | 68 / 69 |
| controller heap in use MB (avg / max over run) | 15 / 19 |
| RSS MB: first half / second half after warmup | 68 / 69 |
| heap MB: first half / second half after warmup | 15 / 15 |
| controller goroutines (max) | 89 |
| HPCSite pm-ml writes (total / avg per s / busiest second) | 26 / 0.45 / 2 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 50 / 26599 |
| published ledger pm (gpu used/booked, cpu used/booked, active) | 519/0, 236/0, 0 |

## L (large)

1,000 jobs, 3 projects, 2 fake sites (2 projects share one site profile and token bucket), lanes test+dev on, per-user one-job lane limits at the site.

| measure | value |
|---|---|
| jobs created in | 0s |
| duration (create to last terminal) | 2m45s |
| throughput (jobs/s) | 6.05 |
| final phases | map[Succeeded/Completed:1000] |
| site submissions (all attempts) | 1000 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 165 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5154 / 5003 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 5367 / 5213 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 5150 / 4996 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 53% |
| rate limit lumi: busiest window as share of the bucket | 25% |
| site calls meluxina | acct=1895 acctlist=667 list=667 ping=13 state=128 submit=667 (peak running 80, peak active 240, 422s 0, injected faults 0) |
| site calls lumi | acct=862 acctlist=333 list=333 ping=11 state=55 submit=333 (peak running 60, peak active 120, 422s 0, injected faults 0) |
| jobs/state calls | 183 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 9445 |
| batching factor (IDs per call) | 51.6 |
| jobs/state calls per poll cycle per HPCSite (avg) | 1.09 |
| runner submit calls / adoptions of a site job found by key | 1000 / 0 |
| runner site reads on a stale observation (fallback) | 0 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 5s / 0.0% |
| reconciles (total / per second) | 19752 / 119.5 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 2.3 |
| workqueue depth (max / max after the first minute) | 1000 / 136 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.27 |
| controller API requests (total / per second) | 17048 / 103.1 |
| controller API PATCHes | 16004 |
| controller RSS MB (avg / max over run) | 112 / 125 |
| controller heap in use MB (avg / max over run) | 51 / 69 |
| RSS MB: first half / second half after warmup | 112 / 122 |
| heap MB: first half / second half after warmup | 51 / 58 |
| controller goroutines (max) | 89 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 69 / 0.42 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 77 / 0.47 / 1 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 62 / 0.38 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 137667 |
| lane placement (last attempt of each job) | map[default:977 dev:11 test:12] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 0 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 0 / 0 |
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
| duration (create to last terminal) | 13m41s |
| final phases | map[Failed/ContinuationLimit:12 Failed/RerunLimit:12 Succeeded/Completed:976] |
| outage: peak jobs Waiting/SiteUnavailable | 220 |
| outage: runner Jobs created for the down site while it was down | 0 |
| jobs that ended SiteUnavailable (the bound is 30 min, the outage 10) | 0 |
| lost-reply submissions that finished before the retry (found in accounting by name, never repeated) | 32 |
| tier-1 re-runs (total / max per job) | 138 / 2 |
| continuations (total) | 224 |
| jobs ended RerunLimit | 12 |
| jobs ended RunnerFailed (a runner pod out of retries) | 0 |
| checkpointing steps (2-3 segments): succeeded / hit the continuation cap through node failures / total | 96 / 0 / 96 |
| ordinary jobs / not succeeded and not RerunLimit | 880 / 0 |
| site submissions (all attempts) | 1362 |
| jobs | 1000 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 820 |
| peak (used+booked) / min(share, siteLeft) | pa-ml/tier0 2%, pa-ml/tier1 1%, pb-ml/tier0 2%, pb-ml/tier1 1%, pc-ml/tier0 2%, pc-ml/tier1 1% |
| project pa: controller used GPU+CPU node-min / site charged / bound | 5969 / 5815 / 420000 |
| project pb: controller used GPU+CPU node-min / site charged / bound | 6254 / 6101 / 420000 |
| project pc: controller used GPU+CPU node-min / site charged / bound | 6204 / 6037 / 420000 |
| rate limit meluxina: busiest window as share of the bucket | 81% |
| rate limit lumi: busiest window as share of the bucket | 95% |
| site calls meluxina | acct=19494 acctlist=924 list=1735 ping=423 state=545 submit=899 (peak running 83, peak active 225, 422s 16, injected faults 18599) |
| site calls lumi | acct=1521 acctlist=491 list=510 ping=220 state=169 submit=474 (peak running 61, peak active 115, 422s 126, injected faults 38) |
| jobs/state calls | 714 |
| job IDs asked for in those calls (= calls a per-job poll would make) | 10380 |
| batching factor (IDs per call) | 14.5 |
| jobs/state calls per poll cycle per HPCSite (avg) | 0.87 |
| runner submit calls / adoptions of a site job found by key | 1373 / 64 |
| runner site reads on a stale observation (fallback) | 18117 |
| observation age of live jobs in pa-train: oldest / share older than 3 poll intervals | 10m9s / 83.2% |
| observation age of live jobs in pb-train: oldest / share older than 3 poll intervals | 10m8s / 81.4% |
| observation age of live jobs in pc-train: oldest / share older than 3 poll intervals | 35s / 10.4% |
| reconciles (total / per second) | 33572 / 40.9 |
| reconcile errors | 234 |
| reconcile duration avg (ms) | 2.7 |
| workqueue depth (max / max after the first minute) | 1000 / 384 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.11 |
| controller API requests (total / per second) | 19955 / 24.3 |
| controller API PATCHes | 17887 |
| controller RSS MB (avg / max over run) | 125 / 141 |
| controller heap in use MB (avg / max over run) | 62 / 88 |
| RSS MB: first half / second half after warmup | 126 / 130 |
| heap MB: first half / second half after warmup | 62 / 66 |
| controller goroutines (max) | 90 |
| HPCSite pa-ml writes (total / avg per s / busiest second) | 165 / 0.20 / 1 |
| HPCSite pb-ml writes (total / avg per s / busiest second) | 166 / 0.20 / 2 |
| HPCSite pc-ml writes (total / avg per s / busiest second) | 124 / 0.15 / 1 |
| recentJobs entries (max) / HPCSite object size (max bytes) | 300 / 145804 |
| lane placement (last attempt of each job) | map[default:969 dev:15 test:16] |
| submissions accepted whose reply was lost (found again by key, never repeated) | 64 |
| lane refusals at the site / jobs showing RefusedQOS | 0 / 0 |
| controller log: error lines / client-side throttling waits | 561 / 0 |
| controller log error sample | writing observation: Patch "https://127.0.0.1:46581/apis/hpc.kaos.io/v1alpha1/namespaces/pc-train/hpcargoworkflowjobs/j00009/status": context canceled |
| controller log error sample | writing observation: context canceled |
| controller log error sample | site poll failed; observations left as they were: get Slurm token Secret: context canceled |
| controller log error sample | Reconciler error: context canceled |
| controller log error sample | publishing the ledger: context canceled |
