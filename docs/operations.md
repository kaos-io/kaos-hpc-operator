# Running the controller

State of `main` (PRD-HPC-1461 v0.13.0). Behaviour is in [controller.md](controller.md).

## Install

The Helm chart is in `dist/chart` (CRDs, RBAC, the controller). In KAOS it is installed on every pool
that sets `spec.operators.hpc` by kubecore-operator (`tools.hpcOperator`), whose single pin
(`HPCOperatorVersion`) is both the chart version and the image tag of the controller, the follower
step and the runner Jobs. Standalone:

```bash
helm install kaos-hpc-operator dist/chart -n kaos-hpc-system --create-namespace \
  --set controllerManager.container.image.repository=<registry>/kaos-hpc-operator
```

The chart installs `hpcargoworkflowjob-submitter-role` and `hpcargoworkflowjob-runner-role`;
kubecore-operator binds them per namespace: the submitter role to the pipeline accounts of each
environment namespace, the runner role to `hpc-runner` of the HPC (ML) namespace. After an API change
regenerate the chart with `kubebuilder edit --plugins=helm/v1-alpha`; `make test` fails while the
chart's CRDs or RBAC differ from `config/`.

## Flags (`cmd/main.go`)

| Flag | Default | Env | Meaning |
|---|---|---|---|
| `--leader-elect` | false (chart: on) | | One active manager. The site poller runs on the leader only |
| `--metrics-bind-address` | `0` (chart: `:8443`) | | Metrics endpoint; `0` disables |
| `--metrics-secure` | true | | HTTPS with authn/authz on the metrics endpoint |
| `--health-probe-bind-address` | `:8081` | | `/healthz`, `/readyz` |
| `--enable-http2` | false | | HTTP/2 stays off (CVE hardening) |
| `--webhook-cert-*`, `--metrics-cert-*` | | | Certificate directories and file names; unset: self-signed |
| `--runner-image` | | `RUNNER_IMAGE` | Image of the runner Jobs. The chart passes the controller's own image (or `runnerImage`) |
| `--runner-scheduling` | | `RUNNER_SCHEDULING` | JSON `{nodeSelector, tolerations, affinity}` for runner pods. The chart builds it from `runner.scheduling`, else from the controller's own scheduling values |
| `--lane-candidates` | empty (lanes off) | `LANE_CANDIDATES` | JSON list of lanes tried in order, for example `[{"qos":"test"},{"qos":"dev","reservationByPartition":{"gpu":"gpudev"}}]`. The chart renders it from `lanes.candidates` |
| `--site-poll-interval` | `30s` | `SITE_POLL_INTERVAL` | One batched `jobs/state` call per HPCSite per interval. `0` turns the poller off and runners poll the site themselves |
| `--site-rest-rate` | `2` | | REST calls per second per site profile (token bucket) |
| `--site-rest-burst` | `5` | | Burst of that bucket |
| zap flags | | | Logging |

The chart has no value for the polling flags; set them through `controllerManager.container.args`
(for example `--site-poll-interval=15s`) or `controllerManager.container.env` (`SITE_POLL_INTERVAL`).
The grace-period flag `--site-missing-grace` and the orphan sweep are in an open PR (README).

## Chart values (`dist/chart/values.yaml`)

| Value | Meaning |
|---|---|
| `controllerManager.container.image.repository` / `.tag` | Required repository; tag defaults to the chart `appVersion` |
| `controllerManager.container.args` | Manager arguments (leader election, metrics, probes) |
| `controllerManager.nodeSelector`, `tolerations`, `affinity` | Where the controller runs (platforms with tainted system nodes set these) |
| `runnerImage` | Runner Job image; empty: the controller image (one image, one version) |
| `runner.scheduling` | `{nodeSelector, tolerations, affinity}` for runner pods; empty: like the controller |
| `lanes.candidates` | Ordered lane candidates (see controller.md section 4). Empty: lane placement is off |
| `rbac.enable`, `crd.enable`, `crd.keep` | RBAC, CRDs, keep CRDs on uninstall |
| `metrics.enable`, `prometheus.enable` | Metrics Service; `VMServiceScrape` if that API exists, else `ServiceMonitor`, else nothing |
| `certmanager.enable`, `networkPolicy.enable` | Off by default |

## What the controller needs around it

- The CRDs of this chart. No Argo CRD is required: the Workflow is read as unstructured.
- In each HPCSite's namespace: the HPCSite (labelled for the project; the namespace carries
  `hpc.kaos.io/project`), the Secret `meluxina-jwt` (key `token`, rotated by the platform; the poller
  reads it live), `meluxina-staging`, `mlflow-svc`, the ConfigMap `hpc-runner-config` and the
  ServiceAccount `hpc-runner`. kubecore-operator renders all of them.
- Network: the controller reaches only the site's REST endpoint (poller). Runners reach the site
  (REST and one forced-command SSH login host), the registry and the data plane.

## Metrics

| Metric | Meaning |
|---|---|
| `kaos_hpc_job_phase` | 1 for the phase a job is in now |
| `kaos_hpc_job_queue_seconds` | Seconds from admission to the site job starting, per attempt |
| `kaos_hpc_job_run_seconds` | Seconds the site job ran, per attempt |
| `kaos_hpc_node_minutes_booked` | Node-minutes booked at admission |
| `kaos_hpc_node_minutes_settled` | Node-minutes actually spent, when an attempt ends (a late host-end correction adds only the difference) |
| `kaos_hpc_site_node_minutes` | A project's node-minutes on a site this period: used, booked, left |
| `kaos_hpc_active_jobs` | Admitted jobs that have not ended |
| `kaos_hpc_lane_placements_total` | Attempts by lane (`qos`, `reservation`, `fallback`); `fallback=true` counts lanes the site refused. Absent while no candidates are configured |
| `kaos_hpc_idle_kills_total` | Steps ended `Idle` by the watchdog (`tier`, `project`) |
| `kaos_hpc_runner_restarts_total` | Runner pods that failed and were retried by their Job |

## Status fields to read when something looks wrong

`HPCArgoWorkflowJob.status`:

| Field | Tells you |
|---|---|
| `phase`, `reason`, `message` | Where the job is and why; the message carries the plain-words explanation |
| `booking` | What admission reserved (site, tier, node-minutes) |
| `attempt`, `attempts[]`, `continuations` | One record per finished site job: outcome, settled node-minutes, `nodeMinutesSaved` for an idle kill |
| `placement` | `timeLimitMinutes`, `source` (declared/learned/timeout-retry), `samples`, `qos`, `reservation`, `refusedQOS` |
| `run` | What the runner reports of the current site job: `siteJobID`, `state`, exit code, times, `hostSiteJobID`, `slot`, `checkpointRef`, `logOffset`, `qos`/`reservation` really used, `lastLog` |
| `siteObservation` | What the controller's batched poll saw (state, pending reason, comment, `observedAt`) |
| `runner` | The runner Job of the current attempt |
| `pendingReason` | The site's queue reason, shown by the follower |

`HPCSite.status`: `periodStart`; `gpu` and `cpu` (`usedNodeMinutes`, `bookedNodeMinutes`,
`leftNodeMinutes`); `activeJobs`, `queuedJobs`, `runningJobs`; conditions. `kubectl get hpcsite` shows
site, GPU minutes left and active jobs.

Events on the job: `LaneRefused`, `HostSettledToJobEnd`, `ReportedTimeDiscarded`, `OverShare`,
`RunnerStartBlocked`, `Idle`.

## Development

```sh
make test       # unit tests and envtest (a real API server)
make lint       # golangci-lint
make manifests  # CRDs and RBAC from the Go types
make build      # the manager binary
python -m pytest runner/tests -q   # the runner (pytest and openssl are test-only)
```
