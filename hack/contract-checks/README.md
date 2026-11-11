# Step contract checks

The checks behind [`docs/step-contract.md`](../../docs/step-contract.md) (PRD kaos#1911, F-01). Each is a
script that can be run again; `run-all.sh` runs them all and prints one line per check.

| Script | Question | Needs | Result 2026-11-12 |
|---|---|---|---|
| `0-fixtures.sh` | Do the fixtures agree with the contract and with each other? | python3 | Pass |
| `3a-context-expression.sh` | Does one expression assemble the step context as one JSON value from task outputs, as a DAG task argument and as a container env value? | the lab | Pass |
| `3b-skipped-upstream.sh` | Is the result of a skipped upstream step the sentinel inside that expression? | the lab | Pass |
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
the context is joined once in the DAG task argument and once in the container's env value. Both steps
must receive exactly the wire form of the fixture (`context-one-upstream`, then `context-escapes`): the
script compares the byte count and the sha256.

It also runs the same base written as ordinary JSON, with no `\u007b` escapes, and prints what arrives:
the engine evaluates `{{...}}` inside the value (`{{=1+1}}` arrives as `2`, `{{workflow.name}}` as the
run's name). That is why the wire form escapes braces.

### 3(b) A skipped upstream

`argo/3b-skipped-upstream.yaml`. Four upstream steps: one ran, one was skipped by its `when`, one was
omitted because the step it depends on did not run, one ran and wrote no result. The step that needs
them must receive the result of the first and the sentinel for the other three.

The engine itself gives the empty string for the output of a skipped or omitted step, with no error and
without the output's declared default (the check's `plain` step shows it). The sentinel comes from
`sprig.default(...)` in the expression. A step that ran and wrote nothing gets the sentinel from its
output's own default.

### 3(c) A large env value

`argo/3c-large-env-value.yaml`. Compose builds a context of an exact size from a sha256 chain; the
script builds the same text and compares. At the cap (98304 bytes) it arrives intact. At 140000 bytes
the step errors before it runs.

Then two contexts of the same 98304 bytes, one with 15000 short strings in it and one with 17000. Inside
the step's definition the context is a JSON string, where each quote takes two bytes: the first costs
128368 bytes and arrives intact, the second costs 132368 and does not start. So the ceiling counts the
context's cost, not its bytes, and the contract counts its caps the same way (section 7 of the document).
An earlier version of this page said 96 KiB of context always leaves 32 KiB for the rest of the
definition; that holds only for a context with few quotes.

Last, the guarantee of the document's section 7. A step whose own definition is at the template budget
(24 KiB), whose base is at its cap (96 KiB less 8 KiB per needed step) and whose needed steps each hand
back a result at the result cap (8 KiB): with three needed steps and with ten, the most allowed. Both
receive their context intact. The same three-step case with results of 32 KiB, the cap before, does not
start. `checklib.needs_workflow` writes these Workflows.

The ceiling is 131072 bytes for the step's whole definition as the engine hands it to the pod (one
environment string, `ARGO_TEMPLATE`, on the init container; 128729 bytes in the case that passed). Over it
the engine tries to move the definition into a ConfigMap, which the chart's controller role does not
allow, and the step ends with `configmaps is forbidden`. A value is never shortened.

### 4 The HPC hops

`hpc/hops_test.go` and `hpc/runner_hop.py`. One context of 98304 bytes, with quotes, a dollar,
backticks, a backslash and a semicolon in a string:

| Hop | How it is checked | Real parts |
|---|---|---|
| 1. Follower to job object | The job the follower's code builds is created with `spec.context` and read back. One byte more is refused; the spec stays immutable; `status.result` takes 8192 bytes and refuses 8193. | `follower.ConfigFromEnv`, `Config.Job`, the API server, the repository's job definition with the contract's fields added |
| 2. Job object to runner Job | The runner Job the controller's code builds, with the context as one more env value, is created and read back. | `runnerjob.Build`, the API server |
| 3. Runner Job to runner process | A process is started with exactly the environment of the stored Job's container and reports what it sees. | the kernel's exec |
| 4. Runner to site | The site job is built the way `Runner.submit` builds it, alone (inputs in the Slurm environment) and for a shared node (inputs as export lines in the script), and sent with the runner's client. | `jobscript.batch_lines`, `inputs_in_script`, the payloads, `Slurm.submit`, the fake Slurm site |
| 5. Job script to step | The whole job script runs under bash with a stand-in for Apptainer; the step reports what it finds in `KAOS_STEP_CONTEXT`. | the job script |

A control run changes one character of the value before it is handed to the job: both steps and the
fake site must show the change. It is there because the first version of this check passed with a
comparison that had not been shown able to fail.

Sizes seen: the runner Job object 107 KiB; the submission 155 KiB alone and 186 KiB for a shared node,
where the job script is 185 KiB.

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

The contract's fields are not in the product code yet. The check adds `spec.context` to the definition
and the env value to the runner Job itself; the work package that adds them for real (W3) should keep
this check and drop those two additions.

### 5 The job definition upgrade

How the pool installs the definition: the pool's composition creates one Helm release of the chart
`kaos-hpc-operator` at a pinned version (kubecore-operator,
`compositions/apis/kubepool/clusteroperators/composition.yaml`, `hpc-operator-release`). The four
definitions are chart templates (`dist/chart/templates/crd/`, `crd.enable: true`), not a `crds/` folder,
so every chart upgrade rewrites them in place, in the same release as the controller. `crd.keep: true`
keeps them when the release is removed. There is one version, `v1alpha1`; nothing converts or rewrites
stored objects.

`hpc/crd_upgrade_test.go` stores two jobs under today's definition (dataset pin, declared outputs, the
cancel finalizer, outputs in the status), then changes the definition in place: `spec.dataset`,
`spec.datasetRef`, `spec.outputs` and `status.outputs` removed; `spec.context`, `spec.app`,
`spec.inputs` and `status.result` added. Once with `spec.context` optional, once required.

Found, on Kubernetes 1.33:

- Stored jobs stay readable, by name and in a list, with the typed client too.
- The removed fields are no longer returned. A job stored with a dataset pin reads back without it.
- The status of a stored job can be written, its finalizer removed, and it can be deleted, also when
  `spec.context` is required and the stored job has none.
- A job in the old shape created after the change is refused when it is created with strict field
  validation, which is what the follower does (`unknown field "spec.dataset"`). Without strict
  validation and with `context` optional it is accepted and stored without its dataset.

What follows for the cutover:

- **Drain before the definition changes.** A job that is still running when its definition changes
  reads back without its dataset. A later attempt of it, built by a controller that does not refuse a
  job without context, would run without its data.
- **The new controller refuses a job without context**, so what was not drained ends with a reason.
- Making `spec.context` required in the definition is safe for stored jobs on this API server version
  and closes the non-strict path. It was not checked on an API server older than 1.30.
- A failed chart upgrade is rolled back by the release (`rollbackLimit: 10`), definitions included.
