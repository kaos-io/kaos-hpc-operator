# The step contract

What a pipeline step is given, what it hands back, and how both travel. The library (kaos-lib), the
pipeline engine (`engine/`), the HPC follower, controller and runner, and the app templates all build
against this page and the fixtures in [`step-contract/fixtures/`](step-contract/fixtures/README.md).

Source: PRD kaos#1911 (D-01, D-06, D-07, D-08, D-13, D-15, section 8.1). There is one contract and no older
one beside it: every app moves once.

- [0. What exists today](#0-what-exists-today)
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

## 0. What exists today

This page is the target. Sections 1 to 10 are written in the present tense, as rules; most of what they
describe is not on `main` yet.

| On `main` today | State |
|---|---|
| This page, the fixtures, the checks under `hack/contract-checks/` | Here. The checks prove the mechanisms the contract rests on; they do not test an implementation. |
| The engine | The contract before this one: `outputs=`, `hpc_time_limit=`, steps started with `--params` and one argument per output, a data lock at version 1. No `kind=`, `inputs=`, `produces=`, no context, no `kaos-result`. |
| The HPC job definition, follower, controller, runner | `spec.dataset`, `spec.datasetRef`, `spec.outputs`, `status.outputs`. No `spec.context`, `spec.app`, `spec.inputs`, `status.result`. |
| kaos-lib | No `kaos_lib.step`. The data lock is version 1. |

Everything else on this page is being built by the wave (PRD kaos#1911, work packages W1 to W5): the
keywords and render-time refusals, compose's outputs and caps, `kaos_lib.step`, the HPC job fields, the
runner's hand-back and its checks. Where a section says "the engine refuses" or "the runner removes", read
"will, once its work package lands".

## 1. Authoring keywords

```python
step(name, *, kind, reads=, inputs=, needs=, produces=, gpu=, disk=, when=, walltime=,
     resumable=, checkpoint_every=, idle_timeout=)
```

Everything after `name` is keyword-only.

| Keyword | Meaning |
|---|---|
| `name` | The step's name. |
| `kind=` | Required. One of `ingest`, `transform`, `train`, `evaluate`, `publish`, `control`. The engine labels every pod and HPC job with it (`platform.kubecore.io/step-kind`). |
| `reads=` | The config sections the step reads. They are the step's `params`. |
| `inputs=` | The names of the config values that hold a dataset reference. An input name is a top-level config key: no dotted paths. Compose pins each to one commit when the run starts. Step code names the input, never a version. |
| `needs=` | The steps this step runs after, at most ten. Each one's result reaches this step in `upstream`. |
| `produces=` | What the step writes: `model("name")`, `artifact("name")`, `dataset("name")`. `dataset("clean")` names the dataset whose repository is `dataset-clean`. |
| `gpu=`, `disk=`, `when=` | Unchanged. |
| `walltime=` | The run limit on an HPC class: `'90m'`, `'12h'` or `'720'` (minutes). |
| `resumable=`, `checkpoint_every=`, `idle_timeout=` | Unchanged. |

Removed, with no alias: `outputs=` (the result replaces it) and `hpc_time_limit=` (`walltime=` replaces it).

**Names.** A step name, an input name and a `produces` name match `[a-z0-9]([a-z0-9-]*[a-z0-9])?` and are at
most 40 characters. A dataset name must also make `dataset-<name>` a valid repository id. Names that start
with `kaos-` are the platform's (section 9).

**Dataset references.** A reference names the repository, as the dataset picker and `kaosctl` write it:
`dataset-<name>@<ref>`, for example `dataset-raw-images@latest`. The config value an input names holds one.

**What the engine refuses when it renders the pipeline**, each time naming the step and the rule:

- a name that breaks the rule above;
- within one step, a name in `inputs=` that is also written by a step in `needs=`, or two steps in `needs=`
  that write the same name: all are read by name (section 2), so the message names both;
- more than ten steps in `needs=` (section 7);
- more than 16 inputs: the declared ones plus the datasets its needed steps write (section 8);
- a rendered definition over its budget (section 7);
- `dataset("clean", of="raw")`: `of=` is reserved for the branch form of a produced dataset (PRD D-01), and
  the message says it is not available yet.

## 2. Step entry

The step's command is `python -m app.entry`, with no arguments, on a cluster pod and in an HPC job alike.
It receives one value, the environment variable `KAOS_STEP_CONTEXT`, and hands back one file, the result.
`kaos_lib.step` reads the one and writes the other: `start()`, `input()`, `output()`, `result()`,
`upstream()`.

**`start()`** reads and checks the context. When the variable is missing, its `version` is not known, it
costs more than its cap (section 7) or an `upstream` entry does not have the shape of a result, the step
writes one line to stderr that names the cause (and the upstream step, when it is one) and exits with
code 1. No exit code is special to this contract; the checkpoint restart code keeps its meaning.

**`input(name)`** resolves a name from two places, and nothing else is looked at:

| The name is | It resolves from | To |
|---|---|---|
| declared in the step's `inputs=` | `ledger.reads` of the context, the entry with that `name` | the dataset at the commit compose pinned |
| a model, artifact or dataset written by a step in `needs=` | `upstream.<step>.writes`, the entry with that `name` | what that step wrote, at the commit it sealed |

The two sets of names never overlap within a step (section 1), so a name has one meaning. Step code never
names a version: every entry `input()` resolves carries an exact commit, and that commit is what is read.

- A name in neither place is an error that names the name.
- When the step that would have written the name did not run (its `upstream` entry is the sentinel),
  `input(name)` raises, naming the step and the name; `input(name, optional=True)` returns `None`.

**`upstream(step)`** returns that step's result. When the step did not run it raises, naming the step;
`upstream(step, optional=True)` returns `None`.

**`result()`** writes the result file (section 4). It is called once per attempt.

**Retries and continuations.** Every attempt of a step gets the same context. The result of the attempt that
ended well is the step's result. On HPC the runner removes any result file an earlier attempt left before
the next attempt starts, so it can never be handed back as this attempt's.

**A local run** (`kaosctl step run`) follows the same contract: `run` is `local-<UTC timestamp>`,
`experiment` is found by the rule of section 3, and the result is written to `work/output/kaos-result.json`
under the run's work folder.

## 3. The step context

`KAOS_STEP_CONTEXT` holds one JSON object in [wire form](#6-wire-form), with these fields in this order:

| Field | Type | Content |
|---|---|---|
| `version` | number | `1`. |
| `run` | string | The run's name: the Workflow's name. The same value for every step of the run. |
| `app` | string | The app. |
| `project` | string | The project. |
| `step` | string | This step's name. |
| `kind` | string | This step's kind (section 1). |
| `experiment` | string | The experiment the run belongs to: the config value `experiment.name`, else the app's name. It is the `<experiment>` folder of the run's artifacts, `<name>/<experiment>/<run>/`. |
| `params` | object | The config sections this step names in `reads=`, as compose resolved them, and nothing else. It never holds a `plan` section. It never holds a `platform` section either, because the wave removes the `platform` config group altogether: an app whose config still names it fails with the migration message. |
| `ledger` | object | A [ledger](#5-the-ledger) with one read for each input this step declares in `inputs=`, at the commit compose pinned. `writes` is empty. |
| `produces` | array | What this step declares in `produces=`: `{"name", "type", "repo"}` each, in that order. `type` is `model`, `artifact` or `dataset`; `repo` is the repository it is written to. A `dataset` entry may carry `promote` (boolean; absent means `false`) after `repo`. |
| `upstream` | object | One entry for each step in `needs=`, keyed by step name, in the order of `needs=`: that step's [result](#4-the-result), or the sentinel `{"version":1,"status":"absent"}` when it did not run. Empty when the step needs none. Always the last field. |

Rules:

- Every field is always present. A consumer ignores a field it does not know and refuses a `version` it
  does not know.
- The context never holds a credential or an endpoint. Config is not a place for secrets, and nothing in
  this contract carries one.
- A step depends only on what it declares: its `params` are its `reads=` sections, its `ledger` its
  `inputs=`, its `upstream` its `needs=`.
- A dataset an upstream step produced in this run is read from `upstream.<step>.writes`, not from `ledger`.
- `promote` says whether this run moves the dataset's `latest` (the head of its `main` branch) to the version
  the step seals. The run asks for it: compose sets `promote` from the reserved config key
  `plan.promote.<dataset>` of the run request, where `<dataset>` is the `produces` name and the value is
  `true` or `false` in any letter case; compose refuses anything else. The step never decides and step code
  cannot set it. With `promote` false or absent the step still seals its version; `latest` stays where it
  was (PRD D-06).

Fixtures: `context-no-upstream`, `context-promote`, `context-one-upstream`, `context-skipped-upstream`,
`context-escapes`.

## 4. The result

A step hands back one file, `/work/output/kaos-result.json` in every container, written by kaos-lib. The
engine reads it as the step's one output, `kaos-result`. It holds one JSON object in
[wire form](#6-wire-form), with these fields in this order:

| Field | Type | Content |
|---|---|---|
| `version` | number | `1`. |
| `status` | string | `ok`: the step wrote this result. `absent`: there is no result. |
| `values` | object | Small values for the steps that need this one. The app chooses the names; a name may not start with `kaos-`. Metrics and anything large belong in MLflow or in an artifact, not here: the whole result may cost 8 KiB (section 7). |
| `reads` | array | A ledger read entry for every input the step declares, at its pinned commit, whether or not `input()` was called for it; and one for every dataset written upstream that the step actually read. |
| `writes` | array | A ledger write entry for each model, artifact or dataset version the step wrote. |

`values`, `reads` and `writes` are always present in an `ok` result, empty when there is nothing in them.

**The sentinel** is exactly `{"version":1,"status":"absent"}`: 31 bytes, which cost 37. A step never writes
it. It stands for the result of a step that did not run: it was skipped by its `when`, or it was not
reached. A step that ends well always has a result, because kaos-lib exits with code 1 when a step ends
without one. So `absent` means "did not run", never "ran and had nothing to say". It is never `{}` and
never an empty value.

A step that fails hands back nothing: the steps that need it do not run.

**Who checks a result.** In the cluster nothing between two steps does: kaos-lib writes it, and the `start()`
of each step that needs it checks the shape of the entry and fails naming the upstream step. On HPC the
runner checks it before it is handed back (section 8).

Fixtures: `result-ok` (one read, one write), `result-model`, `result-absent`.

## 5. The ledger

The ledger is the data lock at version 2: the exact commit of every dataset a run read and of everything it
wrote.

```json
{"version": 2, "reads": [...], "writes": [...]}
```

A read, with its fields in this order:

| Field | Type | Content |
|---|---|---|
| `name` | string | The input's name: the name in `inputs=`, or the name of the dataset an upstream step wrote. |
| `repo` | string | The repository that was read: `dataset-<name>`. |
| `owner` | string or null | The project that shares the dataset; `null` for the run's own project. |
| `ref` | string | The version that was asked for (`latest`, a name, a commit). For a dataset produced in this run it is the commit. |
| `commit` | string | The commit that was read: 64 hex characters. |
| `line` | string, optional | Reserved for the branch form of a dataset (PRD D-01). Absent in the wave. |

A write, with its fields in this order:

| Field | Type | Content |
|---|---|---|
| `name` | string | The name declared in `produces=`. |
| `type` | string | `model`, `artifact` or `dataset`. |
| `repo` | string | The repository written to: `dataset-<name>` for a dataset, `outputs-<app>` for a model or an artifact. |
| `commit` | string | The commit that holds it: 64 hex characters. |
| `line` | string, optional | Reserved, as above. |
| `path` | string, optional | Reserved for Use (after the wave): where the model or artifact lives when it comes from an earlier run. Absent means `<name>/<experiment>/<run>/` of this run. |

**The run's ledger.** Compose writes one output, `kaos-ledger`: a ledger with no writes and one read for
each declared input name, in the pipeline's order of steps and then the order of declaration. Two input
names on the same repository and commit stay two reads. Each step's result adds its own reads and writes.
The run's ledger is the union of compose's reads and every step result's reads and writes, computed by
whoever reads it; no shared file is rewritten. The union removes only what is the same entry twice: reads
equal in `name`, `repo` and `commit`, and writes equal in step and `name`, the step being the one whose
result holds the write.

Version 1 (`{"version":1,"reads":[{"repo","owner","ref","commit"}]}`) is read only to replay an old run.
Nothing writes it.

Fixtures: `ledger-two-reads` (compose's `kaos-ledger`), `ledger-run-union` (a whole run).

## 6. Wire form

The context and the result cross process boundaries as text: an output of a workflow step, an
environment value, a field of an HPC job, a line of a Slurm job script. They are JSON with these rules, so
that every hop carries them unchanged and counts them the same:

1. No whitespace between tokens and no newline at the end.
2. ASCII only. Every other character is written as a `\uXXXX` escape, so bytes and characters count the same.
3. Inside strings, keys included, `{` is written `\u007b` and `}` is written `\u007d`.
4. Inside strings, keys included, `&` is written `\u0026`, `<` is written `\u003c` and `>` is written
   `\u003e`.
5. Numbers are written as the reference writer writes them. NaN and Infinity are refused.
6. Fields are written in the order the tables of sections 3, 4, 5 and 8 give, with `upstream` last in a
   context. A reader does not rely on the order.

Rule 3 is not optional. The workflow engine evaluates `{{...}}` wherever it meets it, also inside a value
that passes through: check 3(a) shows a string `{{=1+1}}` arriving as `2`. With rule 3 the text never
holds `{{`.

Rule 4 is not optional either. When the workflow engine puts a value into a step's definition it writes
`&`, `<` and `>` as those six-byte escapes itself, so a value that held them raw would take more room there
than its cost says (section 7). Check 3(c) shows a context that is within every cap by count and does not
start because its base holds 2000 raw `&`.

Any JSON parser reads the wire form. Writers: compose (the context, `kaos-ledger`), kaos-lib (the result)
and, on HPC, the runner (the result it hands back). The reference writer is
[`hack/contract-checks/wire.py`](../hack/contract-checks/wire.py); every fixture has its wire form beside
it as `<name>.wire`.

## 7. Size caps

| Value | Cap, by cost |
|---|---|
| Result | 8 KiB (8192) |
| Step context | 96 KiB (98304) |
| Step context before its upstream results are in it (the base) | 96 KiB less 8 KiB for each step in `needs=` |
| Steps in one step's `needs=` | 10 |
| A step's rendered definition | 24 KiB (24576 bytes) |

The **cost** of a value is its length as a JSON string: the bytes of the wire form, plus one for each `"`
and one for each `\` in it. It is never less than the bytes; for the fixtures it is about 1.15 times the
bytes. `wire.py` has it as `cost()`, and the caps as `RESULT_CAP`, `CONTEXT_CAP`, `base_cap(needs)`,
`MAX_NEEDS`, `TEMPLATE_BUDGET` and `STEP_CEILING`.

**Why the cost.** The workflow engine hands a step its whole definition as one environment string,
`ARGO_TEMPLATE`, and the context sits inside it as a JSON string. The cost is exactly what the context
takes there. Check 3(c) measures it against the string the engine really built: with plain filler and with
filler made of `&`, `<`, `>`, `"`, `\`, `{` and `}`, the definition less the context's cost is the same
number of bytes.

**The ceiling.** The kernel takes `ARGO_TEMPLATE=<definition>` and its terminator in 131072 bytes, so a
definition may be **131057 bytes**. Check 3(c) runs both sides: 131057 arrives intact; 131058 does not
start. There are two errors past it, and neither shortens a value:

| Definition | What happens |
|---|---|
| 131058 to 131072 bytes | The init container cannot be started: `argument list too long`. |
| from 131073 bytes | The engine tries to move the definition into a ConfigMap, which its role does not allow: `configmaps is forbidden`. |

**The budget of that one string:**

| Part | At most |
|---|---|
| The base of the context | 96 KiB − 8 KiB × N, for N steps in `needs=` (compose) |
| The N upstream results | 8 KiB × N (kaos-lib `result()`, the runner on HPC; an absent one is the sentinel, which costs 37) |
| So the context | 98304 |
| The rest of the step's rendered definition | 24576 (render) |
| Kept free | 8000 |
| Total | 130880, under the ceiling of 131057 |

**The guarantee.** A pipeline that renders and a run that composes can never reach either error, whatever
its steps hand back. With ten steps in `needs=` the base may still cost 16 KiB; an eleventh would leave
8 KiB, which is why ten is the limit.

**What the 24 KiB budget counts.** The step's template as rendered, written with the workflow engine's own
encoder: compact JSON in which `&`, `<` and `>` take six bytes each, so `&&` or `>` in a shell argument
count twelve and six. Three conditions make the count hold at run time:

- The definition holds no reference to a value whose size is set at run time, other than `kaos-context`.
  A reference to a value with a fixed bound (the run's name, an image reference, a class) is counted at
  its bound.
- The budget applies to every template that carries the context: the step's own, and the `hpc-follower`
  template that carries it for a step on an HPC class.
- What the engine changes when it starts a step is inside the 8000 bytes kept free. In the lab the
  definition the engine built was 38 bytes shorter than the template in the Workflow: it drops the
  template's `inputs`. A pool's configuration can add to every definition through `templateDefaults`; the
  pool sets none (it sets `workflowDefaults.spec.tolerations` only, kubecore-operator
  `compositions/apis/kubepool/clusteroperators/composition.yaml:3644`, which is not part of a step's
  definition). A pool that adds `templateDefaults` has to fit them in what is kept free.

Today's definitions are about 3 KiB.

**Where each rule is enforced:**

| Where | Rule |
|---|---|
| kaos-lib, `result()` | Refuses to write a result that costs over 8 KiB. The step fails, and the message says where large values go: MLflow or an artifact. |
| The engine, when it renders the pipeline | Refuses a step that names more than ten steps in `needs=`, and a step whose rendered definition is over 24 KiB. Both name the step and the limit. |
| Compose, for each step | Refuses the run when the step's base costs over 96 KiB less 8 KiB for each step in its `needs=`. The base is counted as it will travel: the whole context, `upstream` keys included, with every upstream result as zero-length text. |
| kaos-lib, `start()` | Refuses a context that costs over 96 KiB. With the rules above this cannot happen; it is the last line. |
| HPC follower | Refuses to create a job whose context costs over 96 KiB. |
| HPC runner | Refuses to hand back a result that costs over 8 KiB (section 8). |
| HPC job definition | `spec.context` at most 98304 characters; `status.result` at most 8192. A value's length is never more than its cost, so every value within its cap fits. |

The caps are per step. The run's Workflow object holds every step's context twice (as compose's output
and as the step's input): one step at the cap makes it 208 KiB. How a run behaves when its contexts add
up to more than the object may hold (about 1 MiB before the engine compresses it) was not checked. Since
`params` holds only the step's own sections, a context is small unless a step asks for much.

## 8. The HPC job

`HPCArgoWorkflowJob` fields of the contract:

| Field | Type | Content |
|---|---|---|
| `spec.app` | string, at most 55 | The app the step belongs to. |
| `spec.context` | string, at most 98304, required | The step context, wire form, exactly as the follower received it in `KAOS_STEP_CONTEXT`. The job's environment gets it under the same name. |
| `spec.inputs[]` | array, at most 16 | The datasets to stage before the step starts, each pinned: `name` (at most 40), `repo` (at most 63), `commit` (at most 64) and, when known, `ref` (at most 255) and `owner`, in that order (and `line`, reserved). |
| `status.result` | string, at most 8192 | The step's result, wire form, as the job handed it back. Set only on a job that Succeeded. |

`spec.context` is required by the definition, and the controller also refuses a job without it: a job
stored before the definition changed has none (check 5).

`spec.inputs[]` is filled by the follower from the context and from nothing else: every entry of
`ledger.reads`, and every `dataset` entry of `upstream.*.writes`. It resolves no version.

Removed: `spec.dataset`, `spec.datasetRef`, `spec.outputs` and `status.outputs`.

**Handing the result back.** The step writes `/work/output/kaos-result.json` in the job. Before each attempt
the runner removes what an earlier attempt left there. When the step ends well the runner:

1. reads the file and parses it;
2. requires `version` 1, `status` `ok`, the three fields `values`, `reads` and `writes` present, and a cost
   of at most 8 KiB;
3. writes it again in wire form into `status.result`.

Anything else fails the job, naming the cause: a missing file, a file that cannot be brought back, one that
is not such a result. The result is never replaced by the sentinel. A job that Failed never publishes a
result, whatever the step wrote.

The follower writes `status.result` of a Succeeded job as the step's `kaos-result` output.

Fixture: `hpc-job` (its `spec.context` is `context-one-upstream.wire`, its `status.result` is
`result-model.wire`).

## 9. Reserved names

| What | Reserved |
|---|---|
| Config keys | `plan` and `pipeline-version`. Under `plan`: `plan.promote.<dataset>` works in the wave; `plan.use.<artifact>` and `plan.target` are reserved for Use and Target, and until those ship compose refuses every `plan` key other than `plan.promote.<dataset>` with a message that it is not available yet. |
| Step names, input names, `produces` names, `values` keys | everything that starts with `kaos-` |
| Step names | also `compose-and-validate`, `main`, `hpc-follower` and names ending in `-hpc`, as before |
| Output names of a step | `kaos-result`; of compose, `kaos-ledger` and `kaos-context-<step>` |
| Branches of a dataset or outputs repository | `run-*` |
| Tags | `k-*` and `keep-*` |
| Folder at the root of a dataset | `.kaos/` |
| Authoring | `of=` in `dataset(...)` |
| Dataset references | the form `<dataset>.<variant>@<ref>` |
| Ledger entries | `line`; on a write, `path` |

## 10. How the engine assembles the context

This section is for the engine and the follower. It is the form the checks proved on the workflow engine.

Which workflow engine. There are two Argo Workflows on dev, pinned in two places:

| Where | Pin | Chart | App version |
|---|---|---|---|
| The pool's, on every workload cluster: **pipelines run here** | kubecore-operator `dev`, `internal/operators/kubepool/phases/syncing_tools.go:238` (`tools.argoWorkflows.version`) | argo-helm `argo-workflows` 1.0.18 | v4.0.6 |
| The dev hub's, for CI only | kaos `prod`, `root/apps-config/helm/argo-workflows.yaml:16` (`env.dev.version`) | argo-helm `argo-workflows` 1.0.14 | v4.0.5 |

The checks ran on v4.0.6. An earlier version of checks 3(a), 3(b) and 3(c) was also run once on v4.0.5
(2026-11-12) and gave the same results, byte for byte.

1. **Compose writes the base.** For each step, compose writes that step's context without `upstream`, in
   wire form, as one output: `kaos-context-<step>`. Compose is the only writer of these fields, so every
   string is escaped in one place and the cap is checked on the real text. It also writes `kaos-ledger`
   (section 5).
2. **Each step's result is one output**, `kaos-result`, read from `/work/output/kaos-result.json` with the
   sentinel as the default when the file is missing.
3. **The DAG task argument `kaos-context` joins them** with one expression. Task names are always written
   as `tasks["<name>"]`: step names usually hold hyphens.

   For a step that needs `prepare-data`, a step with no HPC variant:

   ```yaml
   - name: kaos-context
     value: >-
       {{=sprig.trimSuffix("}", tasks["compose-and-validate"].outputs.parameters["kaos-context-train"])
       + ",\"upstream\":{\"prepare-data\":"
       + sprig.default("{\"version\":1,\"status\":\"absent\"}", tasks["prepare-data"].outputs.parameters["kaos-result"])
       + "}" + "}"}}
   ```

   A step that can run on an HPC class is two tasks, `<step>` and `<step>-hpc`, of which exactly one
   runs. Its result is the one that is there, so each such step in `needs=` takes the pair form:

   ```
   sprig.default(
     sprig.default("{\"version\":1,\"status\":\"absent\"}", tasks["prepare-data-hpc"].outputs.parameters["kaos-result"]),
     tasks["prepare-data"].outputs.parameters["kaos-result"])
   ```

   The single form on such a step would give the sentinel whenever the step ran as its HPC twin.

   One entry per step in `needs=`, in the order of `needs=`, separated by `,"<step>":`. The key is the
   step's name, never the twin's. A step with no `needs=` gets `+ ",\"upstream\":{" + "}" + "}"`.
4. **The container reads it unchanged**: `KAOS_STEP_CONTEXT` is `{{inputs.parameters.kaos-context}}`, in
   the step's pod and in the HPC follower's pod.

What the checks found, and the rules that follow:

- The values are joined as text and never parsed and written again, so numbers arrive as compose wrote
  them (`1e-05`, `1.0`, a 20-digit integer).
- The output of a skipped or omitted task is the empty string, not its declared default, and the engine
  reports no error. Every upstream therefore goes through `sprig.default` with the sentinel.
- `}}` may not appear inside the expression (it ends it): the closing braces are written `"}" + "}"`.
- **`depends`.** The engine gives a task's expression `tasks["X"]` only for a task X that is in its
  `depends`. A reference to any other task is never resolved: the step never starts and the run stays
  Running with no error (check 3(b)). So the `depends` of a step names compose and every task its
  expression reads:
  - a needed step with no HPC variant and no `when`: `<step>`;
  - a needed step that is conditional (`when=`): `(<step>.Succeeded || <step>.Skipped || <step>.Omitted)`,
    the form the engine already renders;
  - a needed step with an HPC variant: both twins, each in that tolerant form, since one of them is
    always skipped.
- The same expression works as a container env value over input parameters (check 3(a), second form).
  The DAG argument is the one to use: the pod and the HPC follower then share one input.

## 11. Checks

Each check is a script under [`hack/contract-checks/`](../hack/contract-checks/README.md) that can be run
again. Results of 2026-11-12, on Argo Workflows v4.0.6:

| Check | Question | Result |
|---|---|---|
| 3(a) | Does one expression assemble the context as one JSON value from task outputs, as a DAG task argument and as a container env value? | Pass, also for a step with no `needs=`. The same value without the wire form's escapes is changed by the engine: asserted |
| 3(b) | Does a skipped upstream yield the sentinel inside that expression? | Pass, with `sprig.default`: the engine yields an empty string by itself. Pass for steps with an HPC variant: the cluster twin ran, the HPC twin ran, neither ran |
| 3(c) | Does a 96 KiB env value reach the container intact? | Pass when its cost fits. The ceiling is 131057 bytes for the step's whole definition. The caps of section 7 keep every step under it: shown for a step that needs three steps and one that needs ten, all at their caps, with plain filler and with the characters that cost more |
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
