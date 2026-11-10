# kaos-hpc-operator scale test report

Generated 2026-11-10T16:47:24Z by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.

| scenario | result |
|---|---|
| S-shared-resumable (node sharing with resumable steps) | PASS |

## S-shared-resumable (node sharing with resumable steps)

10 hosts (6 resumable with 3 segments, 4 plain) and 30 guests (20 resumable with 2-3 segments, 10 plain) that run 8-16 minutes on hosts that end sooner: resumable guests are signalled and continued, plain ones fit or run alone; the ledger equals the site's charge.

| measure | value |
|---|---|
| duration | 1m55s |
| final phases | map[Succeeded/Completed:40] |
| guests taken by a host / resumable guests signalled before their run was over | 141 / 100 |
| resumable jobs continued / plain jobs that continued | 26 of 26 / 0 |
| controller settled node-min / site charged | 631 / 618.2 |
| site submissions (all attempts) | 171 |
| jobs | 40 |
| duplicate submissions (same job, same attempt) | 0 |
| budget invariant checks (snapshots) | 114 |
| peak (used+booked) / min(share, siteLeft) | psr-ml/tier0 0%, psr-ml/tier1 0% |
| project psr: controller used GPU+CPU node-min / site charged / bound | 631 / 618 / 5400480 |
| published ledger psr (gpu used/booked, cpu used/booked, active) | 631/0, 0/0, 0 |
| reconciles (total / per second) | 2236 / 19.5 |
| reconcile errors | 0 |
| reconcile duration avg (ms) | 1.7 |
| workqueue depth (max / max after the first minute) | 0 / 0 |
| reconcile worker busy (reconciles/s x avg duration; 1.0 = saturated) | 0.03 |
| controller API requests (total / per second) | 1975 / 17.2 |
| controller API PATCHes | 1748 |
