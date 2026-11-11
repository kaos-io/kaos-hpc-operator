# The step contract

What a pipeline step is given, what it hands back, and how both travel. The library (kaos-lib), the
pipeline engine (`engine/`), the HPC follower, controller and runner, and the app templates all build
against this page and the fixtures in [`step-contract/fixtures/`](step-contract/fixtures/README.md).

Source: PRD kaos#1911 (D-07, D-08, D-13, D-15, section 8.1). There is one contract and no older one beside
it: every app moves once.

- [1. Authoring keywords](#1-authoring-keywords)
- [2. Step entry](#2-step-entry)
- [3. The step context](#3-the-step-context)
- [4. The result](#4-the-result)
- [5. The ledger](#5-the-ledger)
- [6. Wire form](#6-wire-form)
- [7. Size caps](#7-size-caps)
- [8. The HPC job](#8-the-hpc-job)
- [9. Reserved names](#9-reserved-names)
- [10. How the engine assembles the context](#10-how-the-engine-assembles-the-context)
- [11. Checks](#11-checks)

## 1. Authoring keywords

```python
step(name, kind=, reads=, inputs=, needs=, produces=, gpu=, disk=, when=, walltime=,
     resumable=, checkpoint_every=, idle_timeout=)
```

| Keyword | Meaning |
|---|---|
| `name` | The step's name: lowercase letters, digits and hyphens. |
| `kind=` | Required. One of `ingest`, `transform`, `train`, `evaluate`, `publish`, `control`. The engine labels every pod and HPC job with it (`platform.kubecore.io/step-kind`). |
| `reads=` | The config sections the step reads. |
| `inputs=` | The names of the config values that hold a dataset reference. Compose pins each to one commit when the run starts. Step code names the input, never a version. |
| `needs=` | The steps this step runs after. Each one's result reaches this step in `upstream`. |
| `produces=` | What the step writes: `model("name")`, `artifact("name")`, `dataset("name")`. |
| `gpu=`, `disk=`, `when=` | Unchanged. |
| `walltime=` | The run limit on an HPC class: `'90m'`, `'12h'` or `'720'` (minutes). |
| `resumable=`, `checkpoint_every=`, `idle_timeout=` | Unchanged. |

Removed, with no alias: `outputs=` (the result replaces it) and `hpc_time_limit=` (`walltime=` replaces it).

Reserved for the branch form of a produced dataset (PRD D-01, after the wave): `dataset("clean", of="raw")`.
Until that ships the engine refuses `of=` with a message that says so.

Within one step, a name in `inputs=` and a name written by a step in `needs=` must differ, and two steps in
`needs=` must not write the same name: all are read by name (section 2). The engine refuses the pipeline when
it renders it, naming both.

## 2. Step entry

The step's command is `python -m app.entry`, with no arguments, on a cluster pod and in an HPC job alike.
It receives one value, the environment variable `KAOS_STEP_CONTEXT`, and hands back one file, the result.
`kaos_lib.step` reads the one and writes the other: `start()`, `input()`, `output()`, `result()`,
`upstream()`.

What `input(name)` resolves. There are two places, and nothing else is looked at:

| The name is | It resolves from | To |
|---|---|---|
| declared in the step's `inputs=` | `ledger.reads` of the context, the entry with that `name` | the dataset at the commit compose pinned |
| a model, artifact or dataset written by a step in `needs=` | `upstream.<step>.writes`, the entry with that `name` | what that step wrote, at the commit it sealed |

The two sets of names never overlap within a step (section 1), so a name has one meaning. A name in neither
is an error. Step code never names a version: every entry `input()` resolves carries an exact commit, and
that commit is what is read.

## 3. The step context

`KAOS_STEP_CONTEXT` holds one JSON object in [wire form](#6-wire-form).

| Field | Type | Content |
|---|---|---|
| `version` | number | `1`. |
| `run` | string | The run's name. The same value for every step of the run. |
| `app` | string | The app. |
| `project` | string | The project. |
| `step` | string | This step's name. |
| `kind` | string | This step's kind (section 1). |
| `experiment` | string | The experiment the run belongs to: the `<experiment>` folder of the run's artifacts, `<name>/<experiment>/<run>/`. |
| `params` | object | The run's resolved configuration, as compose resolved it. The same value for every step of the run. |
| `ledger` | object | A [ledger](#5-the-ledger) with one read for each input this step declares in `inputs=`, at the commit compose pinned. `writes` is empty. |
| `produces` | array | What this step declares in `produces=`: `{"name", "type", "repo"}` each. `type` is `model`, `artifact` or `dataset`; `repo` is the repository it is written to. A `dataset` entry may carry `promote` (boolean; absent means `false`). |
| `upstream` | object | One entry for each step in `needs=`, keyed by step name: that step's [result](#4-the-result). A step that did not run, or wrote no result, is the sentinel `{"version":1,"status":"absent"}`. Empty when the step needs none. |

Rules:

- Every field is always present. A consumer ignores a field it does not know and refuses a `version` it
  does not know.
- The context never holds a credential or an endpoint.
- A dataset an upstream step produced in this run is read from `upstream.<step>.writes`, not from `ledger`.
- `promote` says whether this run moves the dataset's `latest` (the head of its `main` branch) to the version
  the step seals. The run asks for it: compose sets `promote` from the reserved config key
  `plan.promote.<dataset>` of the run request. The step never decides and step code cannot set it. With
  `promote` false or absent the step still seals its version; `latest` stays where it was (PRD D-06).

Fixtures: `context-no-upstream`, `context-promote`, `context-one-upstream`, `context-skipped-upstream`,
`context-escapes`.

## 4. The result

A step hands back one file, `/work/output/kaos-result.json`, written by kaos-lib. The engine reads it as
the step's one output, `kaos-result`. It holds one JSON object in [wire form](#6-wire-form).

| Field | Type | Content |
|---|---|---|
| `version` | number | `1`. |
| `status` | string | `ok`: the step wrote this result. `absent`: there is no result. |
| `values` | object | Small values for the steps that need this one. The app chooses the names. |
| `reads` | array | A ledger read entry for each dataset the step read. |
| `writes` | array | A ledger write entry for each model, artifact or dataset version the step wrote. |

The sentinel is exactly `{"version":1,"status":"absent"}`. A step never writes it. The platform puts it
where a result is missing: the step was skipped, was not reached, or ended well without writing the file.
It is never `{}` and never an empty value.

A step that fails hands back nothing: the steps that need it do not run.

Fixtures: `result-ok` (one read, one write), `result-model`, `result-absent`.

## 5. The ledger

The ledger is the data lock at version 2: the exact commit of every dataset a run read and of everything it
wrote.

```json
{"version": 2, "reads": [...], "writes": [...]}
```

A read:

| Field | Type | Content |
|---|---|---|
| `name` | string | The input's name: the config value named in `inputs=`, or the name of the dataset an upstream step produced. |
| `repo` | string | The dataset's repository, `dataset-<name>`. |
| `owner` | string or null | The project that shares the dataset; `null` for the run's own project. |
| `ref` | string | The version that was asked for (`latest`, a name, a commit). For a dataset produced in this run it is the commit. |
| `commit` | string | The commit that was read: 64 hex characters. |
| `line` | string, optional | Reserved for the branch form of a dataset (PRD D-01). Absent in the wave. |

A write:

| Field | Type | Content |
|---|---|---|
| `name` | string | The name declared in `produces=`. |
| `type` | string | `model`, `artifact` or `dataset`. |
| `repo` | string | The repository written to: `dataset-<name>` for a dataset, `outputs-<app>` for a model or an artifact. |
| `commit` | string | The commit that holds it: 64 hex characters. |
| `line` | string, optional | Reserved, as above. |

A model or an artifact lives in its repository under `<name>/<experiment>/<run>/`; the three parts are in
the entry and the context.

The run's ledger is the union of its steps' results. Compose writes the first part (the pinned inputs);
each step's result adds what it read and wrote. No shared file is rewritten.

Version 1 (`{"version":1,"reads":[{"repo","owner","ref","commit"}]}`) is read only to replay an old run.
Nothing writes it.

Fixtures: `ledger-two-reads` (as compose writes it), `ledger-run-union` (a whole run).

## 6. Wire form

The context and the result cross process boundaries as text: an output of a workflow step, an
environment value, a field of an HPC job, a line of a Slurm job script. They are JSON with three more
rules, so that every hop carries them unchanged:

1. No whitespace between tokens and no newline at the end.
2. ASCII only. Every other character is written as a `\uXXXX` escape, so bytes and characters count the same.
3. Inside strings, keys included, `{` is written `\u007b` and `}` is written `\u007d`.

Rule 3 is not optional. The workflow engine evaluates `{{...}}` wherever it meets it, also inside a value
that passes through: check 3(a) shows a string `{{=1+1}}` arriving as `2`. With rule 3 the text never
holds `{{`.

Any JSON parser reads the wire form. Writers: compose (the context) and kaos-lib (the result). The
reference writer is [`hack/contract-checks/wire.py`](../hack/contract-checks/wire.py); every fixture has
its wire form beside it as `<name>.wire`.

## 7. Size caps

| Value | Cap | Counted on |
|---|---|---|
| Step context | 96 KiB (98304) | its cost |
| Result | 32 KiB (32768) | its cost |

The **cost** of a value is its length as a JSON string: the bytes of the wire form, plus one for each `"`
and one for each `\` in it. It is never less than the bytes; for the fixtures it is about 1.15 times the
bytes. `wire.py` has it as `cost()`.

Why the cost and not the bytes: the workflow engine hands a step its whole definition as one environment
string, and the kernel limits one such string to 128 KiB (131072 bytes). The context sits inside that
definition as a JSON string, where every quote and backslash takes two bytes. Check 3(c) shows it with two
contexts of the same 98304 bytes: the one that costs 128368 arrives intact, the one that costs 132368 does
not start. Over the limit the engine tries to move the definition into a ConfigMap, which its role does not
allow, and the step errors with `configmaps is forbidden` before it runs. A value is never shortened.

The budget of that one string:

| Part | Budget |
|---|---|
| The context, by cost | 96 KiB |
| The rest of the step's rendered definition | 24 KiB |
| Left for what the engine adds when it starts the step | 8 KiB |

Where each is enforced:

| Where | Rule |
|---|---|
| kaos-lib, `result()` | Refuses to write a result that costs over 32 KiB. The step fails. |
| The engine, when it renders the pipeline | Refuses a step whose rendered definition, without the context, is over 24 KiB (24576 bytes of JSON). Today's are about 3 KiB. |
| Compose | Refuses the run when a step's context without its upstream results costs over 64 KiB (the context cap less one full result). |
| kaos-lib, `start()` | Refuses a context that costs over 96 KiB. |
| HPC follower | Refuses to create a job whose context costs over 96 KiB. |
| HPC job definition | `spec.context` at most 98304 characters; `status.result` at most 32768. A value within its cap is within these. |

With the first three rules a step that needs one other step always starts: 24 KiB + 64 KiB + 32 KiB is
under the limit. **What they do not cover:** a step that needs several steps whose results together cost
more than its context has left. Compose cannot see results, so it cannot refuse that run before it starts.
Up to the limit the step starts and kaos-lib refuses the context with a clear message; over the limit the
step ends with the engine's `configmaps is forbidden`. Closing it for good takes a rule that reserves room
per needed step (a step with a 16 KiB base and a 32 KiB result cap could then need two steps), or a smaller
result cap. That is a decision for the owner; this document does not make it.

The caps are per step. The run's Workflow object holds every step's context twice (as compose's output
and as the step's input): one step at the cap makes it 208 KiB. How a run behaves when its contexts add
up to more than the object may hold (about 1 MiB before the engine compresses it) was not checked;
`params` is repeated in every step's context, so it is the value to keep small.

## 8. The HPC job

`HPCArgoWorkflowJob` fields of the contract:

| Field | Type | Content |
|---|---|---|
| `spec.app` | string | The app the step belongs to. |
| `spec.context` | string, at most 98304 | The step context, wire form, exactly as the follower received it in `KAOS_STEP_CONTEXT`. The job's environment gets it under the same name. A job without it is refused. |
| `spec.inputs[]` | array | The datasets to stage before the step starts, each pinned: `{"name", "repo", "commit"}` and, when known, `ref` and `owner` (and `line`, reserved). |
| `status.result` | string, at most 32768 | The step's result, wire form, as the job handed it back. Absent when the step wrote none. |

`spec.inputs[]` is filled by the follower from the context and from nothing else: every entry of
`ledger.reads`, and every `dataset` entry of `upstream.*.writes`. It resolves no version.

Removed: `spec.dataset`, `spec.datasetRef`, `spec.outputs` and `status.outputs`.

Handing the result back: the step writes `/work/output/kaos-result.json` in the job; the runner brings it
to `status.result`; the follower writes it as the step's `kaos-result` output, or the sentinel when
`status.result` is absent. A result file that exists and cannot be brought back, is not a JSON object in
wire form, or is over the cap fails the job: it is never replaced by the sentinel.

Fixture: `hpc-job` (its `spec.context` is `context-one-upstream.wire`, its `status.result` is
`result-model.wire`).

## 9. Reserved names

| What | Reserved |
|---|---|
| Config keys | `plan` (with `plan.use.<artifact>`, `plan.target` and `plan.promote.<dataset>`) and `pipeline-version` |
| Step names and output names | everything that starts with `kaos-`; also `compose-and-validate`, `main`, `hpc-follower` and names ending in `-hpc`, as before |
| Branches of a dataset or outputs repository | `run-*` |
| Tags | `k-*` and `keep-*` |
| Folder at the root of a dataset | `.kaos/` |
| Authoring | `of=` in `dataset(...)` |
| Dataset references | the form `<dataset>.<variant>@<ref>` |

## 10. How the engine assembles the context

This section is for the engine and the follower. It is the form the checks proved on the workflow engine.

Which workflow engine. There are two Argo Workflows on dev, pinned in two places:

| Where | Pin | Chart | App version |
|---|---|---|---|
| The pool's, on every workload cluster: **pipelines run here** | kubecore-operator `dev`, `internal/operators/kubepool/phases/syncing_tools.go:238` (`tools.argoWorkflows.version`) | argo-helm `argo-workflows` 1.0.18 | v4.0.6 |
| The dev hub's, for CI only | kaos `prod`, `root/apps-config/helm/argo-workflows.yaml:16` (`env.dev.version`) | argo-helm `argo-workflows` 1.0.14 | v4.0.5 |

The checks ran on v4.0.6. Checks 3(a), 3(b) and 3(c) were also run once on v4.0.5 (2026-11-12) and gave the
same results, byte for byte.

1. **Compose writes the base.** For each step, compose writes that step's context without `upstream`, in
   wire form, as one output: `kaos-context-<step>`. Compose is the only writer of these fields, so every
   string is escaped in one place and the cap is checked on the real text.
2. **Each step's result is one output**, `kaos-result`, read from `/work/output/kaos-result.json` with the
   sentinel as the default when the file is missing.
3. **The DAG task argument `kaos-context` joins them** with one expression:

   ```yaml
   - name: kaos-context
     value: >-
       {{=sprig.trimSuffix("}", tasks["compose-and-validate"].outputs.parameters["kaos-context-train"])
       + ",\"upstream\":{\"prepare\":"
       + sprig.default("{\"version\":1,\"status\":\"absent\"}", tasks.prepare.outputs.parameters["kaos-result"])
       + "}" + "}"}}
   ```

   One `sprig.default(...)` per step in `needs=`, separated by `,"<step>":`. A step with no `needs=` gets
   `+ ",\"upstream\":{" + "}" + "}"`.
4. **The container reads it unchanged**: `KAOS_STEP_CONTEXT` is `{{inputs.parameters.kaos-context}}`, in
   the step's pod and in the HPC follower's pod.

What the checks found, and the rules that follow:

- The values are joined as text and never parsed and written again, so numbers arrive as compose wrote
  them (`1e-05`, `1.0`, a 20-digit integer).
- The output of a skipped or omitted step is the empty string, not its declared default, and the engine
  reports no error. Every upstream therefore goes through `sprig.default` with the sentinel.
- `}}` may not appear inside the expression (it ends it): the closing braces are written `"}" + "}"`.
- A needed step that is conditional keeps the `depends` form the engine already renders:
  `(<step>.Succeeded || <step>.Skipped || <step>.Omitted)`.
- The same expression works as a container env value over input parameters (check 3(a), second form).
  The DAG argument is the one to use: the pod and the HPC follower then share one input.

## 11. Checks

Each check is a script under [`hack/contract-checks/`](../hack/contract-checks/README.md) that can be run
again. Results of 2026-11-12:

| Check | Question | Result |
|---|---|---|
| 3(a) | Does one expression assemble the context as one JSON value from task outputs, as a DAG task argument and as a container env value? | Pass |
| 3(b) | Does a skipped upstream yield the sentinel inside that expression? | Pass, with `sprig.default`: the engine yields an empty string by itself |
| 3(c) | Does a 96 KiB env value reach the container intact? | Pass when its cost fits: the ceiling is 131072 bytes for the step's whole definition, in which the context is a JSON string (section 7) |
| 4 | Does a 96 KiB value survive follower, job object, runner and job environment? | Pass on the fake site, with a control run. Needs a live site: the list below |
| 5 | Do stored HPC jobs stay readable when fields are removed from the definition? | Pass. They read back without the removed fields, and can still be ended, released and deleted |

What check 4 needs a live site for:

- the limits of the site's `slurmrestd` and `slurmctld` on a request, on one environment string and on a
  job script;
- that Slurm hands the environment to the job on the node unchanged;
- that Apptainer passes a 96 KiB variable into the container;
- on a shared node, that the host reads a waiting job's script whole;
- the site's login shell and profile.

One job closes them: [`hack/contract-checks/live-site/`](../hack/contract-checks/live-site/README.md) holds
it as a script that sends a 98304-byte context both ways and reads back the byte count and sha256 the job
found. **It has not been run and is not part of `run-all.sh`:** it spends site time and needs the owner's
word.

What each check found, and what it could not prove, is in
[`hack/contract-checks/README.md`](../hack/contract-checks/README.md). Two findings bind the cutover:

- A stored job reads back without `spec.dataset` once the definition drops it. HPC jobs are drained before
  the definition changes, and the controller refuses a job without `spec.context`.
- An older follower creates its job with strict field validation, so after the change its job is refused
  with `unknown field "spec.dataset"`; it never becomes a job without data.
