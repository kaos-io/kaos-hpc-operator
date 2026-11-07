# idlewatch - idle watchdog for in-cluster GPU steps (PRD-HPC-1461 D-72, D-76)

A GPU step whose GPU utilisation stays below 5 % **and** whose log does not grow for 20 minutes is
ended with the reason `Idle`, so an idle cloud GPU stops costing money. It is the same rule as the
HPC job script. It runs on the pool, inside the HPC controller process, because a sidecar cannot
see a GPU that the device plugin gave exclusively to the main container.

Off unless `--idle-metrics-url` (chart: `idleWatchdog.metricsUrl`) is set.

## What it does, per scan (30 s)

1. **Reap** pods it ended earlier: repeat the Failed status if the kubelet wrote Running back, delete
   the pod once Argo has recorded the step (`Workflow.status.nodes[<node-id>].phase` is final), or
   after `--idle-confirm-timeout` (5 min).
2. **Judge** running pods that have the label `workflows.argoproj.io/workflow` and a `nvidia.com/gpu`
   limit on the `main` container:
   - the main container ran for the whole window;
   - GPU: `max_over_time(<metric>[window])` below 5 % over at least half the expected samples
     (one PromQL query per distinct window, for all pods);
   - log: `pods/log?tailLines=1&timestamps=true` of `main`, the newest line older than the window
     (or no output at all). Read only for pods whose GPU was quiet.
3. **End** an idle pod: label `platform.kubecore.io/idle-ended=true`, annotation `idle-ended-at`, pod
   status `Failed` / reason `Idle` / message `KaosIdle: ended by the idle watchdog - ...`, a Warning
   event, and `kaos_k8s_idle_kills_total{namespace}`.

Unknown is never idle: no metric, too few samples, an unreadable log or a young container all end
nothing (`kaos_k8s_idle_watchdog_errors_total{stage}` counts the failures).

## Why mark Failed instead of deleting

Deleting a pod makes Argo record the step as `Error` ("pod deleted") or a bare exit code, and the
Hera template's pod-loss retry (`retryStrategy`, `kubecore/resilience.py`, `POD_LOSS_RETRY`) re-runs
`Error` steps. A Failed pod with a status message is recorded as a `Failed` step carrying that
message. The message starts with `KaosIdle` and contains none of the retry expression's pod-loss
words (`TestStatusMessageIsNotAPodLossMessage`); the template also excludes `KaosIdle` explicitly.
The kubelet writes its own `Running` status back within about 10 s, so the status is repeated until
Argo has recorded the step, and only then is the pod deleted to free the GPU.

## Per-step override

Pod annotation `platform.kubecore.io/idle-timeout`: whole minutes, `0` turns the watchdog off for the
pod. The Hera template maps `step(idle_timeout=...)` to it. A bad value is ignored (default window).

## Files

| File | Responsibility |
|---|---|
| `config.go` | names shared with the template, defaults, `Config` |
| `policy.go` | which pods, the per-pod window, `Decide` (pure), the status message |
| `promsource.go` | GPU utilisation per pod from the PromQL API |
| `logtail.go` | last log line time of a container |
| `workflows.go` | has Argo recorded the step as finished |
| `ender.go` | mark and fail a pod, delete it |
| `pods.go` | list candidate and ended pods |
| `watchdog.go` | the scan loop (manager Runnable) |

## Metric source

Default: GKE `kubernetes.io/container/accelerator/duty_cycle` through the pool's stackdriver exporter
(`compositions/apis/kubepool/gcp/observability-cost`, present when `costTracking` is on), labels
`namespace_name`, `pod_name`. Cloud Monitoring lags a few minutes, so a hang is ended about 24 minutes
after it started. A pool with a DCGM exporter uses `DCGM_FI_DEV_GPU_UTIL` with labels `namespace` and
`pod`. A pool with neither gets no watchdog (leave `metricsUrl` empty).

## Tests

```sh
KUBEBUILDER_ASSETS=<envtest dir> go test ./internal/idlewatch/...   # the Ender test needs a real API server
```
