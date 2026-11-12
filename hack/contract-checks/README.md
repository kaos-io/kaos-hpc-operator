# Step contract checks

The checks behind [`docs/step-contract.md`](../../docs/step-contract.md) (PRD kaos#1911, F-01). Each is a
script that can be run again; `run-all.sh` runs them all and prints one line per check.

| Script | Question | Needs | Result 2026-11-12 |
|---|---|---|---|
| `0-fixtures.sh` | Do the fixtures agree with the contract and with each other? | python3 | Pass |
| `3a-context-expression.sh` | Does one expression assemble the step context as one JSON value from task outputs, as a DAG task argument and as a container env value? | the lab | Pass |
| `3b-skipped-upstream.sh` | Is the result of a skipped upstream step the sentinel inside that expression, also for a step with an HPC variant? | the lab | Pass |
| `3c-large-env-value.sh` | Does a 96 KiB env value reach the container intact? | the lab | Pass when its cost as a JSON string fits |
| `4-hpc-hops.sh` | Does a 96 KiB value survive follower, job object, runner and job environment? | go, python3 | Pass, with the limits below |
| `5-crd-upgrade.sh` | Do stored HPC jobs stay readable when fields are removed from the job definition? | go | Pass |

## The lab

Checks 3(a) to 3(c) run on a local kind cluster, `prd1911-contract`, with one node. `lib.sh` creates it
when it is missing, installs Argo Workflows and downloads the matching `argo` command into `bin/` (not
committed). Nothing else is touched. Delete it with `kind delete cluster --name prd1911-contract`.

The version is the one the workload clusters of dev run: the pool installs the argo-helm chart
`argo-workflows` 1.0.18 (kubecore-operator, `internal/operators/kubepool/phases/syncing_tools.go`,
`tools.argoWorkflows.version`), whose app version is **v4.0.6**. The lab's Kubernetes is 1.27 (kind
0.20); dev runs a newer one. That does not matter for these three checks: the limit they measure is the
kernel's and the workflow engine's.

The dev hub runs another Argo Workflows for CI: chart 1.0.14, app version v4.0.5 (kaos,
`root/apps-config/helm/argo-workflows.yaml:16`). Pipelines do not run there. The three checks were run on it
once all the same, on 2026-11-12, and gave the same results byte for byte:

```bash
ARGO_CHART_VERSION=1.0.14 ARGO_VERSION=v4.0.5 ./3a-context-expression.sh   # on a fresh cluster; then 3b, 3c
```

Checks 4 and 5 need no cluster. They run a real API server (envtest, Kubernetes 1.33, the version the
repository's own tests use) and the repository's follower, runner Job builder, runner modules and fake
Slurm site. They are Go tests in `hpc/`, built only with the tag `contractcheck`, so `make test` does
not run them.

## What each check found

### 3(a) The context expression

`argo/3a-context-expression.yaml`. Compose writes the base context; an upstream step writes its result;
the context is joined once in the DAG task argument and once in the container's env value, and a third
step with no `needs=` gets the base with an empty `upstream`. Each must receive exactly the wire form
expected (from `context-one-upstream`, then `context-escapes`): the script compares the byte count and the
sha256.

It then runs the same base written as ordinary JSON, with no `\u007b` escapes, and **requires** that the
engine changes it: `{{=1+1}}` arrives as `2`, `{{workflow.name}}` as the run's name. Rule 3 of the wire
form rests on that, so the check fails if a later engine stops doing it.

### 3(b) A skipped upstream

`argo/3b-skipped-upstream.yaml`. Upstream steps in every state: one ran, one was skipped by its `when`, one
was omitted because the step it depends on did not run, one ran and wrote no result. The step that needs
them must receive the result of the first and the sentinel for the other three.

The engine itself gives the empty string for the output of a skipped or omitted task, with no error and
without the output's declared default (the check's `plain` step shows it). The sentinel comes from
`sprig.default(...)` in the expression.

Steps with an HPC variant, with hyphens in their names: `fine-tune` ran and `fine-tune-hpc` was skipped;
`re-train` was skipped and `re-train-hpc` ran; `dry-run` and `dry-run-hpc` were both skipped. The pair form
of the expression must give the result, the result and the sentinel.

`argo/3b-not-in-depends.yaml`: a step whose expression reads a task that is not in its `depends`. It never
starts. After 60 seconds the run is still Running, with no error; the check then deletes it.

### 3(c) A large env value

`argo/3c-large-env-value.yaml` and the Workflows `checklib.needs_workflow` writes. Every case builds its
context in the cluster from a sha256 chain; the script builds the same text and compares.

- At the cap (98304 bytes) the context arrives intact. At 140000 bytes the step errors before it runs.
- Two contexts of the same 98304 bytes, one with 15000 short strings in it and one with 17000. Inside the
  step's definition the context is a JSON string, where each quote takes two bytes: the first costs
  128368 bytes and arrives intact, the second costs 132368 and does not start. The ceiling counts the
  context's cost, not its bytes.
- The guarantee of the document's section 7. A step whose own definition is at the template budget
  (24 KiB), whose base is at its cap (96 KiB less 8 KiB per needed step) and whose needed steps each hand
  back a result at the result cap (8 KiB): three needed steps with plain filler, three with mixed filler,
  ten with mixed filler. Mixed filler is the characters that take more than a byte in a definition
  (`&`, `<`, `>`, `"`, `\`, `{`, `}`), in wire form. All three receive their context intact.
- `cost()` against the engine. For each of those cases the script reads the definition the engine built
  (`ARGO_TEMPLATE` on the pod's init container) and subtracts the context's cost. The remainder must be
  the same in every case, and is: 24600 bytes. An earlier version checked `cost()` only against Python's
  own encoder, which proved nothing about the engine.
- What the engine adds: the definition it built is 38 bytes shorter than the template in the Workflow
  with the context in place (it drops the template's `inputs`).
- Two controls, which must not start: the three-step case with results of 32 KiB (the cap before), and the
  three-step case with 2000 raw `&` in the base. The second is within every cap by count; the engine writes
  each `&` as six bytes. That is rule 4 of the wire form.
- The ceiling: a definition of 131057 bytes arrives intact, one of 131058 does not start
  (`argument list too long`), one of 131073 does not start (`configmaps is forbidden`).

### 4 The HPC hops

`hpc/hops_test.go` and `hpc/runner_hop.py`, on the product code and the repository's own job definition.
One context that costs exactly 98304, the cap (98242 bytes), with quotes, a dollar, backticks, a backslash
and a semicolon in a string:

| Hop | How it is checked | Real parts |
|---|---|---|
| 1. Follower to job object | The job the follower builds from its environment is created and its `spec.context` read back. A context of 98305 characters is refused by the definition; the spec stays immutable; `status.result` takes 8192 bytes and refuses 8193. | `follower.ConfigFromEnv`, `Config.Job`, the API server, the repository's job definition (`config/crd/bases`) |
| 2. Job object to runner Job | The runner Job the controller builds from the stored job is created and read back. | `runnerjob.Build`, the API server |
| 3. Runner Job to runner process | A process is started with exactly the environment of the stored Job's container and reports what it sees. | the kernel's exec |
| 4. Runner to site | The site job is built by the code `Runner.submit` sends (`Runner.site_job`), alone (inputs in the Slurm environment) and for a shared node (inputs as export lines in the script), and sent with the runner's client. | `Runner.site_job`, `jobscript.batch_lines`, `inputs_in_script`, the payloads, `Slurm.submit`, the fake Slurm site |
| 5. Job script to step | The whole job script runs under bash with a stand-in for Apptainer; the step reports what it finds in `KAOS_STEP_CONTEXT`. | the job script |

A control run changes one character of the value before it is handed to the job: both steps and the
fake site must show the change. It is there because the first version of this check passed with a
comparison that had not been shown able to fail.

Sizes seen: the runner Job object 104 KiB; the submission 167 KiB alone and 197 KiB for a shared node,
where the job script is 196 KiB.

**What this does not prove, because it needs a live site:**

- The limits of the site's `slurmrestd` and `slurmctld` on a request, on one environment string and on
  a job script. The fake site has none. Slurm's documented default for a script is 4 MiB
  (`max_script_size`); the site's value was not read.
- That Slurm hands the environment to the job on the node unchanged.
- That Apptainer passes a 96 KiB variable into the container. The stand-in only starts the command.
- On a shared node, that the host reads a waiting job's 185 KiB script with `scontrol write batch_script`
  and starts it.
- The site's login shell and profile (`#!/bin/bash -l`).

One job on the site's test lane closes them. It is written down, as a script that sends nothing without
`--submit` and refuses that without `--owner-approved`: [`live-site/`](live-site/README.md). **It has not
been run:** site time is the client's money and it needs the owner's word. It is not part of `run-all.sh`.

Not checked here either: the kubelet starting the runner's container with the value. Check 3(c) shows
the same mechanism (a pod with a 98304-byte env value) on kind.

The check runs on the product code: the definition it loads is the repository's, the follower fills
`spec.context` and the controller's runner Job carries it. The step in the check writes no result, so the job
hands back "no result" to a stand-in for lakeFS; the hand-back itself is tested in
`runner/tests/test_step_contract.py`.

### 5 The job definition upgrade

How the pool installs the definition: the pool's composition creates one Helm release of the chart
`kaos-hpc-operator` at a pinned version (kubecore-operator,
`compositions/apis/kubepool/clusteroperators/composition.yaml`, `hpc-operator-release`). The four
definitions are chart templates (`dist/chart/templates/crd/`, `crd.enable: true`), not a `crds/` folder,
so every chart upgrade rewrites them in place, in the same release as the controller. `crd.keep: true`
keeps them when the release is removed. There is one version, `v1alpha1`; nothing converts or rewrites
stored objects.

`hpc/crd_upgrade_test.go` stores two jobs under the definition of before the step contract (kept as
`hpc/testdata/hpcargoworkflowjobs-before.yaml`: dataset pin, declared outputs, the cancel finalizer; one is
Running with outputs in its status, one is not judged yet), then replaces the definition in place by the
repository's own: `spec.dataset`, `spec.datasetRef`, `spec.outputs` and `status.outputs` removed;
`spec.context` and `spec.app` (both required), `spec.inputs` and `status.result` added. It then runs the
controller on the job that was not judged yet.

Found, on Kubernetes 1.33:

- Stored jobs stay readable, by name and in a list, with the typed client too.
- The removed fields are no longer returned. A job stored with a dataset pin reads back without it.
- The status of a stored job can be written, its finalizer removed, and it can be deleted, although
  `spec.context` is required and the stored job has none.
- The controller refuses the stored job it had not judged yet: phase `Refused`, reason `NoContext`, nothing
  booked; it then drops the finalizer and the job can be deleted.
- A job in the old shape created after the change is refused: with strict field validation, which is what
  the follower does (`unknown field "spec.dataset"`), and without it (`spec.context: Required value`).

What follows for the cutover:

- **Drain before the definition changes.** A job that is still running when its definition changes
  reads back without its dataset. A later attempt of it, built by a controller that does not refuse a
  job without context, would run without its data.
- **The controller refuses a job without context** (reason `NoContext`), so what was not drained ends with
  a reason: before it is booked; before a runner is started for a job that was already booked; and when a
  job that ran to its end under the runner of before reports success.
- `spec.context` is required in the definition. That is safe for stored jobs on this API server version
  and closes the non-strict path. It was not checked on an API server older than 1.30.
- A failed chart upgrade is rolled back by the release (`rollbackLimit: 10`), definitions included.
