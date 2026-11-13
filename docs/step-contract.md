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
- [12. A matrix step](#12-a-matrix-step)
- [13. Starting a run from a step](#13-starting-a-run-from-a-step)

## 0. What exists today

This page is the contract, and sections 1 to 10 are written in the present tense, as rules. The engine and the HPC
side of it are on `main` of this repository; the library and the app templates are other repositories.

| Where | State |
|---|---|
| This page, the fixtures, the checks under `hack/contract-checks/` | On `main`. The checks prove the mechanisms the contract rests on; the engine's and the HPC side's tests load the fixtures. |
| The engine (`engine/`) | On `main`: `kind=`, `inputs=`, `produces=`, `walltime=`, `matrix=`, the context and the `kaos-result` of every step, the render-time refusals, conditions on a value, the final promotion task. `outputs=`, `hpc_time_limit=` and `disk=` are refused, and so is an app from before the contract (section 1). |
| The HPC job definition, follower, controller, runner (`api/`, `internal/`, `runner/`) | On `main`: `spec.app`, `spec.context`, `spec.inputs`, `status.result`, the hand-back of the result and its checks (section 8). |
| kaos-lib (another repository) | `kaos_lib.step` and the data lock at version 2 (the ledger) are on its `main`, in the release it prepares. |

What is not on this page and not in this repository: the app templates and the console. They follow the contract
from their own repositories; their state is in the PRD (kaos#1911), not here.

## 1. Authoring keywords

```python
step(name, *, kind, reads=, inputs=, needs=, produces=, gpu=, when=, walltime=,
     resumable=, checkpoint_every=, idle_timeout=, matrix=, matrix_allow_failed=)
```

Everything after `name` is keyword-only.

| Keyword | Meaning |
|---|---|
| `name` | The step's name. |
| `kind=` | Required. One of `ingest`, `transform`, `train`, `evaluate`, `publish`, `control`; each has a contract (below). The engine labels every pod and HPC job with it (`platform.kubecore.io/step-kind`). |
| `reads=` | The config sections the step reads. They are the step's `params`. |
| `inputs=` | The names of the config values that hold a dataset reference. An input name is a top-level config key: no dotted paths. Compose pins each to one commit when the run starts. Step code names the input, never a version. Before any step starts, compose also waits (every 3 s, up to 3 minutes) until the data store marks each dataset the steps produce with `kaos.grant=<app>@<time>`, the sign that the app may write it, because the store creates the repository a minute or two before it gives the app the right to write; a store that sets that label on no dataset repository of the project is taken for an older one, and the run then starts after the wait with a line that says so. |
| `needs=` | The steps this step runs after, at most ten. Each one's result reaches this step in `upstream`. |
| `produces=` | What the step writes: `model("name")`, `artifact("name")`, `dataset("name")`. A dataset an app produces is the repository `dataset-<app>-<name>`: `dataset("clean")` in the app `hera1` is `dataset-hera1-clean`. The name is per app because every app made from a template declares the same names and a dataset has exactly one producer; two apps of a project never share one. |
| `gpu=` | Unchanged. |
| `when=` | A condition in plain words: a name, `==` or `!=`, and a value. The name is a field of the run form, `when="quantization-mode != off"` (a setting's path with dashes, as the run form shows it), or a value a step in `needs=` handed on, `when="evaluate.passed == true"`: `<step>.<value>`, where `<value>` is a keyword of that step's `step.result(...)` (letters, digits and `_`; a step that is not in this step's `needs=` is refused, naming the fix). The value is a bare word or number (`off`, `3`) or a quoted string (`"two words"`). Comparisons join with `and` / `or` (`and` binds tighter; no parentheses, no other operators). The engine writes it as one condition on the step's task and on its HPC variant (after the term every task has for starting a run from a step, section 13), comparing the field's value as text: `quantization-mode != off` becomes `{{=workflow.parameters['quantization-mode'] != 'off'}}` (`and` becomes `&&`, `or` becomes a double bar, in the order written). A step's value is read from the needed step's result (`kaos-result`) through the expression of section 10 item 3. **What is compared, exactly:** a value that is a string is compared with the text as text; a value that is a number or a boolean is compared by its JSON text, as the workflow system's `toJSON` writes it (`1000000` is `1000000`, `1e6` written in `when=` is read as `1000000`, `1.0` is `1`, `1e-5` is `0.00001`, `true` is `true`), never through `string()` (which writes `1e+06` for 1000000, `1.2345678e+07` for 12345678 and `1e-05` for 0.00001). Written in `when=`, a number is read the way JSON writes numbers, so `1e6`, `1000000` and `1000000.0` all match the value 1000000. `evaluate.rows != 1000000` therefore skips the step when `rows` is 1000000. A text that cannot be matched reliably is refused when the pipeline is rendered, naming the fix: a number written another way than JSON writes it (`.5`, `+1`, `01`, `5.`), a `True`/`NULL` that is not lowercase, an integer above 9007199254740992 (a result's number is a double), a number too large to hold, and any control character (a line break, a tab; also refused in `when=` as a whole). **A needed step that was skipped has no value, and the condition is false** for `==` and for `!=` alike (nothing can be said about a value that does not exist): the step that asked is skipped too, and the run goes on. That holds for a step skipped by its own `when=` (the absent sentinel), for one that ended itself with `step.skip()` (result `status: skipped`), on the cluster and on the HPC variant (the pair). A needed step that **ran and finished well but did not set the key** (a typo in the name, a value never set, or `null`) fails the task, naming step and key: `int("<sentence>")` of a sentence is the one failure the expression language lets us word, and the workflow system shows `failed to evaluate expression: invalid operation: int(step 'evaluate' ran but gave no value 'passed' (a typo in the name, or the value was never set with step.result(passed=...)): a when= on a step's value needs the value)` as the task's error. A result whose status is none of `ok`, `skipped` and `absent` is malformed and fails the task too. A needed step that failed never reaches the condition (its `depends`). The expression is `(<status> in ["absent", "skipped"] ? false : (<status> == "ok" ? <comparison> : int("<malformed sentence>")))` (checked on the lab, Argo Workflows v4.0.6: 29 comparisons over 999999, 1000000, 12345678, 3000000000, 0.5, 1.0, 1e-5, true, false, an empty string and a string that looks like a number; and the cases: gate passes, gate hands on false, gate skipped by `step.skip()` and by its own `when=` with `==` and `!=`, key missing, malformed result; the cluster shape and the HPC pair). `kaos-promote` is unaffected (its `depends` accept Skipped; a skipped step's dataset is simply not promoted), and a step that needs a matrix step cannot name its value (section 1). The step is skipped when the condition is false. |
| `walltime=` | The step's time limit, on any kind: `'90m'`, `'12h'` or `'720'` (minutes). It is the default of the step's run-form field `<step>-walltime` (below). |
| `matrix=` | `{"lr": [0.01, 0.001], "batch": [16, 32]}`: the step runs once for each combination of the values (section 12). The run form's `<step>-matrix` holds the combinations (every one by default) and `<step>-parallel` how many run at once. At most 32 combinations; the step may not produce a dataset or be `ingest`, `publish` or `control`; it may not need another matrix step. |
| `matrix_allow_failed=` | `True` on a step with `matrix=`: the step after it runs when some items failed and sees which (section 12). By default one failed item fails the step. |
| `resumable=`, `idle_timeout=` | Unchanged. |
| `checkpoint_every=` | How often the step's checkpoint is saved: `'20m'`, `'1h'` or `20` (minutes); `False` for no copies. It is the default of the run-form field `<step>-checkpoint-every`. |

Removed, with no alias: `outputs=` (the result replaces it), `hpc_time_limit=` (`walltime=` replaces it) and
`disk=`: a step is not told its disk, it gets the disk of the machine its class runs on (the usable disk of that
class, less a fixed reserve for the container images, or 10Gi where the platform does not say). A step that
passes one is refused with what to write instead, and then with the sentence for an app from before the contract
(`OLD_APP_NOTICE`: "This app was made before the current pipelines. Create it again from the template."), because a
pipeline that still uses one is an old app: "step 'x': outputs= is gone. Declare what the step writes with produces=[...] and hand it on with step.result(). This app was made before the current pipelines. Create it again from the template."; "step 'x': hpc_time_limit= is gone. Use walltime=, for example walltime=\"12h\". This app was made before the current pipelines. Create it again from the template."; "step 'x': disk= is gone. The step gets the disk of the machine class it runs on. This app was made before the current pipelines. Create it again from the template." Any other unknown keyword is refused with a sentence that lists the keywords that exist.

**Step kinds.** A kind says what the step is for, and the engine refuses a step that does not do it:

| Kind | Must | May not | Runs on |
|---|---|---|---|
| `ingest` | produce at least one dataset | | the cluster only |
| `transform` | produce at least one thing (dataset, model or artifact) | | the cluster or HPC |
| `train` | produce at least one model | | the cluster or HPC |
| `evaluate` | | produce a dataset or a model (artifacts only) | the cluster or HPC |
| `publish` | | produce anything | the cluster only |
| `control` | | take `inputs=` or produce anything | the cluster only |

Each rule has one sentence, naming the step, what its kind requires and what to do:
`step 'x': an ingest step brings data in, so it produces a dataset. Add produces=[dataset("name")].`;
`step 'x': a transform step turns data into something. Add produces=[...] with a dataset, a model or an artifact.`;
`step 'x': a train step produces a model. Add produces=[model("name")], or give the step the kind of the work it does (ingest, transform, evaluate, publish, control).`;
`step 'x': an evaluate step writes reports only, so it may not produce a dataset or a model. Use kind="transform" for a step that makes a dataset, kind="train" for one that makes a model.`;
`step 'x': a publish step delivers a result and produces nothing here. Remove produces=, or give the step the kind of the work it does.`;
and, for `control`, `step 'x': a control step steers the run and handles no data, so it takes no inputs= and no produces=. ...`.
(An ingest step that produces a dataset runs on the cluster either way: section "Placement of a step that produces a dataset".)

**The run form.** The form speaks to the developer, in this order: `pipeline-info` (how to fill the form, the machine
classes, the developer's steps; no account, provider or platform step), the `experiment-*` fields, the other settings
in the order of `config/`, the dataset fields (the config values `inputs=` names), then for each step, in pipeline order,
`<step>-class`, `<step>-walltime` and `<step>-checkpoint-every`, then the `promote-<dataset>` choices (read by the final task `kaos-promote`), `config`
("Extra settings for this run, as YAML. They replace the values above."). There is no field for an image: see
"Where a step's image comes from" below.
A setting's description is the comment written next to it in `config/*.yaml`: the `# ...` at the end of its line, else
the comment lines directly above it with no blank line between; with no comment the description is empty. The
platform's own compose step has no class field (it runs on the default class, fixed in the template) and the platform
endpoints are not form fields.

**Time limit and checkpoint interval on the run form.** Every step of the developer's pipeline has one time limit,
wherever it runs, and the run form shows it: a field `<step>-walltime` for every step, with the description "The
longest the step may run, as minutes or hours: 90m, 12h. Default from pipeline.py." Its default is the step's
`walltime=`, else `4h` (the same default as an HPC class). On a cluster machine the value is the step's deadline; on
an HPC class it is the job's limit. The platform's own compose step has a fixed limit of 30 minutes and no field.
On an HPC class a run keeps its access to its data and to tracking for 11.5 hours at most (a step that runs longer
would lose both partway through and fail late, after spending node-hours). So the form's `<step>-walltime` of a step on an
HPC class that is not resumable is at most 11.5 hours (690 minutes): compose refuses a longer value before any step
starts, naming the step, the limit in hours and the two fixes, shorten `walltime=` or make the step resumable
(`resumable=True` with `checkpoint_every=`). A resumable step may ask for more: it runs as runs that each end inside
that time, each with fresh access and continuing from its checkpoint, and together they stay within the limit (at most
six runs: 69 hours). Waiting for a machine does not count: a step that never gets one is bounded by the platform's pending reaper, not by
this limit. A step that declares `checkpoint_every=` (not `False`) or `resumable=` also has `<step>-checkpoint-every` ("How often
the 'x' step's checkpoint is saved: minutes or hours, for example 10m or 1h."), default the value in code (10 minutes,
`resilience.DEFAULT_EVERY_MINUTES`, for a step that declares `resumable=` alone). Both are
text: minutes (`90`, `90m`) or hours (`12h`), at least one minute; compose refuses any other value before a step
starts, naming the field: `<step>-walltime is '1d'. Write minutes or hours, for example 90, 90m or 12h: at least
one minute, at most 30 days, no other unit.` Compose writes the numbers as its output `kaos-run-settings` (`{"<step>": {"walltime":
minutes, "checkpoint-every": minutes}}`), which the deadline, the HPC job's limit and the checkpoint copies read.
A step with a checkpoint interval has the field `checkpoint_every` (minutes, a number) in its context, after
`produces`; a step without one has no such field.

| | Who sets it | Order, first wins | Default | Read by |
|---|---|---|---|---|
| time limit | the run form's `<step>-walltime` | the form value, then `step(walltime=)` | `walltime=`, else `4h` | the cluster deadline; the HPC job's limit (the follower and the runner) |
| checkpoint interval | the run form's `<step>-checkpoint-every`, a field of a step that declares `checkpoint_every=` or `resumable=` (not `checkpoint_every=False`) | the form value, then `step(checkpoint_every=)`, then the argument of `kaos_lib.checkpoint.register(...)` in code, then the library's adaptive default | `checkpoint_every=`, else 10 minutes for a `resumable=` step | cluster: the helper's `CKPT_EVERY_MIN` (from the form field); HPC: the job's copier (`KAOS_CKPT_EVERY_S`, from `checkpoint_every` of the step context, as the target, with the spacing below); the step: `kaos_lib.checkpoint.tick()` (the same context field) |

**How the copier uses the interval.** The form value is the TARGET interval between copies, counted from the end of the
previous copy. On HPC (Slurm) the copier never starts a periodic copy sooner than the larger of the form value and five
minutes (`MIN_EVERY_S` 300 s), nor sooner than ten times the duration of the previous copy (copying stays under a tenth of the run
time), and it starts none when the time left before the limit is not more than the resumable lead plus the time the copy
takes, so that no periodic copy is in flight when the wall-time signal asks for the final copy (it writes one plain line,
`copying every N minutes, not every M: <why>`, when it copies less often than asked, and one when it stops for the limit).
On the cluster the helper never copies sooner than 60 seconds after the previous copy ended, nor sooner than ten times its
duration, and not less often than hourly; it has no wall-time signal, a stop (SIGTERM) triggers the final copy.

`step(walltime=)` and `step(checkpoint_every=)` are checked when the pipeline is rendered, and by lint, with compose's
sentence above: a default the form would refuse (over 30 days, under a minute) cannot be written. `checkpoint_every=False`
means no field and no copies; `register(checkpoint_every=False)` in code turns the library's periodic checkpoints off only
for a step that has no form value.

**A long job continues instead of starting over.** A step that declares `resumable=` is run again, and continues
from its newest checkpoint, in each of three cases, on a cluster machine and on an HPC class, at most five times
after the first run: it reaches its time limit, its machine is lost, or it asks to be restarted (exit code 85,
`kaos_lib.checkpoint.exit_for_restart`). The time limit applies to each run of the step, not to the sum: on the
cluster each run is a new pod with the full deadline; on an HPC class each continuation is a new job with the full
limit, and the follower's own deadline covers the queue and every continuation. The exception is a limit longer than one run
can be (the longest the site allows, or the 11.5 hours of access above): that limit is then the total, each run gets at most
the longest run and never more than what the earlier runs left, and the step ends with its limit used up. On the cluster the step is run
again by the step's retry rule (a step that does not declare `resumable=` is retried only when its machine is lost)
and restores its checkpoint folder in the init container; on HPC the controller sends the stop signal the declared
lead (`resumable=`) before the limit and submits the continuation. The form's `<step>-walltime` must be longer than
that lead: compose refuses a smaller value, naming the field.

**Classes on the run form.** A step is offered the GPU classes of the cluster only when its code declares
`gpu=True`. A step with `gpu=True` is offered the GPU and the CPU classes (it can run CPU-only on a CPU class); a step
without it, the platform's own compose step included, is offered the CPU classes only, or every class the pool has
when the pool declares no CPU class. The default class of a step does not change. **The same rule holds on HPC:** an HPC
class whose entry has `gpus` above 0 (a GPU partition) is offered only to a step with `gpu=True`, and is refused for any other
step with the same sentence; a step with `gpu=True` is offered every HPC class that its kind may use, GPU and CPU alike (a GPU
step on a CPU partition is the developer's choice, as it is on the cluster). The HPC classes are otherwise offered by the
rules below.

A class the form does not offer a step is refused when the run starts, before the first step, whoever submitted
the run: `step '<name>' cannot run on class '<class>': <reason>. <what to do>`. For a GPU class and a step without
`gpu=True` the sentence is "step 'x' cannot run on class 'gpu-t4': it is a GPU machine and the step does not
declare gpu=True. Add gpu=True to the step in pipeline.py to run it on a GPU machine."; an HPC class and an ingest,
publish or control step, or a step that produces a dataset, "... a publish step never runs on HPC (Slurm). Choose
one of: ..."; a name that is no class of the cluster, "... it is not a machine class of this cluster. Choose one
of: ...". The form's choice and the check come from one function, so they cannot differ.

**Placement of a step that produces a dataset.** A step whose `produces=` holds a `dataset(...)` is not
offered an HPC class and has no HPC variant, the way `ingest`, `publish` and `control` steps are not: a job on
HPC holds a token the lake treats as admin, which would bypass the rule that only the producing app writes a
produced dataset. The step runs on the cluster until jobs there hold the app's own rights (PRD kaos#1911 F-40).
A step that produces a dataset and is pinned to an HPC class is refused. A step that produces only a model or
an artifact is not affected.

**Apps made before the current pipelines.** An app in which no step has a `kind=`, that lists the `platform` config group or defines a
top-level `platform` section, or carries a `kubecore/` folder of its own, is not moved to this contract. The
render refuses it with a message that ends: "This app was made before the current pipelines. Create it again
from the template." A step without `kind=` in a pipeline where other steps have one forgot it: its message ends at
the list of kinds, without that sentence.

**Names.** A step name, an input name and a `produces` name match `[a-z0-9]([a-z0-9-]*[a-z0-9])?` and are at
most 40 characters; a dataset name is also at least 3. The lake allows 3 to 48 characters after `dataset-`, so
`<app>-<name>` of a produced dataset may not exceed 48: the engine refuses it when it renders the pipeline,
naming the step, both names and the limit. Names that start with `kaos-` are the platform's (section 9).

**Dataset references.** A reference names the repository, as the dataset picker and `kaosctl` write it:
`dataset-<name>@<ref>`, for example `dataset-raw-images@latest`. The config value an input names holds one.
A dataset another app produced is referenced by its repository, `dataset-<app>-<name>@<ref>`. Within the app that
produces it the engineer writes the short name: `dataset("clean")`, `step.output("clean")`, `step.input("clean")`,
the run form's `promote-clean`.

**What the engine refuses when it renders the pipeline**, each time naming the step and the rule:

- a name that breaks the rule above;
- a `when=` that is not a name compared with a value (an unknown operator such as `<` or `>`, parentheses, a missing
  value, `{{...}}` written in the workflow system's syntax): the message states what is allowed with one example of
  each form; one that names a field the run form does not have (the message lists the fields it has), or a value of
  a step that is not in its `needs=` (the message says to add it);
- a name that is both an input of a step and one of its own outputs, or an output of a step it needs and one of its
  own: the step reads and writes by name;
- a step without its folder `steps/<name with every hyphen written as an underscore>/`, when the render runner knows
  where the app is (`KAOS_APP_REPO`, set by `kaos_engine.render`);
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
| `instance` | string | The run's own identity: the Workflow's uid. A run's name can be used again; its instance cannot. kaos-lib puts it in the record of the step (section 13). Empty only outside the platform. |
| `params` | object | The config sections this step names in `reads=`, as compose resolved them, and nothing else. It never holds a `plan` section. It never holds a `platform` section either, because the wave removes the `platform` config group altogether: an app whose config still names it is refused as an app made before the current pipelines. |
| `ledger` | object | A [ledger](#5-the-ledger) with one read for each input this step declares in `inputs=`, at the commit compose pinned. `writes` is empty. |
| `produces` | array | What this step declares in `produces=`: `{"name", "type", "repo"}` each, in that order. `type` is `model`, `artifact` or `dataset`; `repo` is the repository it is written to. There is no `promote`: a step only seals a version of a dataset, and the run's final task moves `latest` (below). |
| `upstream` | object | One entry for each step in `needs=`, keyed by step name, in the order of `needs=`: that step's [result](#4-the-result), or the sentinel `{"version":1,"status":"absent"}` when it did not run. Empty when the step needs none. Always the last field. |

Rules:

- Every field is always present. A consumer ignores a field it does not know and refuses a `version` it
  does not know.
- The context never holds a credential or an endpoint. Config is not a place for secrets, and nothing in
  this contract carries one.
- A step depends only on what it declares: its `params` are its `reads=` sections, its `ledger` its
  `inputs=`, its `upstream` its `needs=`.
- A dataset an upstream step produced in this run is read from `upstream.<step>.writes`, not from `ledger`.
- **A step never moves `latest`.** It seals its version (the commit its result names in `writes`) and stops. The run
  form has a choice `promote-<dataset>` (`false` or `true`, default `false`; "Make this run's version of <dataset> the
  one other pipelines read as latest.") for each `dataset(...)` a step produces, `<dataset>` being the `produces` name.
  Compose checks the value (anything but `true` or `false` is refused, naming the field); it is not in any context.
  The run key `plan.promote` of the ADVANCED override is gone: an override that holds it is refused with "'plan.promote'
  is no longer read. Tick the field promote-<dataset> of the run form, one for each dataset a step produces."
  `latest` moves in the run's final task, below (PRD D-06 as amended).

**The final task `kaos-promote`.** A pipeline that produces a dataset ends with one more task, added by the enhancer
(never by `pipeline.py`; `kaos-promote` is not a name a step or a template can take). It runs only when every step of
the run succeeded or was skipped (`depends`: `compose-and-validate && (<step>.Succeeded || <step>.Skipped)` for every
step, through the pair of an HPC step), never on an HPC class, and with the compose template's image, service account,
lake credential and fixed class: the identity a step has. Its input is, for each dataset-producing step, the step's name,
the dataset's name, the form's `promote-<dataset>` and the `writes` of the step's result (`[]` for a step that did not
run). It does two things in this order: (1) it CHECKS every dataset to promote: the sealed commit's first parent is the
head of `main` the step started from, and `main` must still be there; if any dataset's head moved it promotes none and
fails, naming each dataset that moved; (2) it MERGES every sealed version into `main`, each guarded by `expected_head`,
with the metadata `kaos.promoted=<sealed commit id>`. A dataset whose producer did not run, or whose step wrote what
`main` already held, or whose `promote-<dataset>` is not `true`, is left out, and the log says so. **What the task says in every outcome**: which datasets this run promoted, which it did not and why, which it did not
try, and whether trying again can help. **An outage between the merges** (the data store did not answer) leaves the
datasets already merged at the new version and the others at the old one; the task fails and says so, and trying it
again helps: a dataset that this run merged is recognised by a merge commit with `kaos.promoted=<its sealed commit>`
anywhere in `main`'s history since the head the version was sealed on (not only at the head: another run may have moved
the dataset again since) and is left alone, and the others are checked and merged. **Another run promoting the same
dataset** (an overlap) refuses this run whole: when the other run moved a dataset before the check, nothing more is
merged; when it moved one between the check and its merge, the datasets already merged by this run stay merged and the
rest are not tried. In both cases the task says "start a new run to promote <names>", because trying the task again
cannot finish it. Every sealed version stays kept (tagged) and can be read by its exact version.
The task asks a node for 100m CPU and 256Mi, and no pod starts when no `promote-<dataset>` is `true` (the task is Skipped);
the compose step's log then says, for each dataset, that its version stays sealed and `latest` does not move.
Models and artifacts
are not touched: their run-unique folder is saved by their step. The code is `engine/kubecore/promote.py`.

Fixtures: `context-no-upstream`, `context-one-upstream`, `context-skipped-upstream`,
`context-escapes`, `context-matrix-item`, `context-matrix-upstream`.

**Where a step's image comes from.** The image a step runs is never a parameter of the workflow, so it is not on the run
form and a submit cannot set it (an argument named `image-<step>` or `image` in a submit is ignored: nothing reads it).
The platform's record of what CI built, the ConfigMap `<app>-pipeline-images` (labelled
`workflows.argoproj.io/configmap-type: Parameter`, which the workflow engine needs to read it), is read by the templates:

- Compose's template has an input `image` and one input `image-<step>` for each step, each with
  `valueFrom.configMapKeyRef` into the record (key: the step's name, or its `platform.kubecore.io/source`). The engine reads
  them when compose starts, so one reading serves the whole run. A key that the record lacks ends compose's task with
  `unable to retrieve inputs.parameters.image from ConfigMap: ... does not have the key ...` (an `optional` flag does not
  change that on the workflow engine v4.0.6).
- Compose gets the step images as `KAOS_STEP_IMAGES` and refuses the run for a step that was never built (a record
  of `:<step>-latest`: `kubecore/compose.py` `check_step_images`). It writes the images as its output `kaos-images`
  (`/work/kaos-images.json`, `--images-output`).
- Every step's task gives the step its `image` input from that output
  (`step_contract.image_of`: `{{=fromJSON(tasks["compose-and-validate"].outputs.parameters["kaos-images"])["<step>"]}}`),
  and the step's container image is `{{inputs.parameters.image}}`. The HPC variant's task gets the same expression, so
  the same string runs on the cluster and on HPC.
- The final task `kaos-promote` has its own input `image` from the record (key `compose-and-validate`); a step with a
  `platform.kubecore.io/image` annotation keeps that verbatim image and reads no record.

Checked on the lab (workflow engine v4.0.6): the images come from the record, a submit that carries `image-ingest=evil/a:1`,
`image=evil/b:1` and `image-compose-and-validate=evil/c:1` runs the record's images, and the HPC variant gets the step's string.

## 4. The result

A step hands back one file, `/work/output/kaos-result.json` in every container, written by kaos-lib. The
engine reads it as the step's one output, `kaos-result`. It holds one JSON object in
[wire form](#6-wire-form), with these fields in this order:

| Field | Type | Content |
|---|---|---|
| `version` | number | `1`. |
| `status` | string | `ok`: the step wrote this result. `absent`: there is no result. `skipped`: the step ended itself with `step.skip(reason)`; the result then has only `version`, `status` and `reason` (below). |
| `values` | object | Small values for the steps that need this one. The app chooses the names; a name may not start with `kaos-`. Metrics and anything large belong in MLflow or in an artifact, not here: the whole result may cost 8 KiB (section 7). |
| `reads` | array | A ledger read entry for every input the step declares, at its pinned commit, whether or not `input()` was called for it; and one for every dataset written upstream that the step actually read. |
| `writes` | array | A ledger write entry for each model, artifact or dataset version the step wrote. |

`values`, `reads` and `writes` are always present in an `ok` result, empty when there is nothing in them.

**The sentinel** is exactly `{"version":1,"status":"absent"}`: 31 bytes, which cost 37. A step never writes
it. It stands for the result of a step that did not run: it was skipped by its `when`, or it was not
reached. A step that ends well always has a result, because kaos-lib exits with code 1 when a step ends
without one. So `absent` means "did not run", never "ran and had nothing to say". It is never `{}` and
never an empty value.

**A skipped result** is `{"version":1,"status":"skipped","reason":"<1 to 300 characters>"}` (fixture `result-skipped`). A step writes it with `kaos_lib.step.skip(reason)` and ends with exit code 0, so the workflow system shows the task as succeeded; the result, not the task's phase, says it was skipped. It has no `values`, `reads` or `writes`, and the step saves no output. The steps that need it still run (as for a step skipped by `when=`): the context carries this entry in `upstream`, and kaos-lib reads it as "left no result" with the reason (`step.skipped(name)`). A skipped step's datasets get no new version, so the final task promotes none of them. The HPC runner checks a skipped result as its own shape and publishes it as `status.result` (`runner/kaos_hpc_runner/result.py`), so a step skips the same way on an HPC class.

A step that fails hands back nothing: the steps that need it do not run.

**Who checks a result.** In the cluster nothing between two steps does: kaos-lib writes it, and the `start()`
of each step that needs it checks the shape of the entry and fails naming the upstream step. On HPC the
runner checks it before it is handed back (section 8): the whole shape, not the top level. `version` is the
integer 1, `values` is an object, every entry of `reads` and `writes` has the fields and types of section 5, every
number is finite, no object holds a key twice, and a field the contract does not give is refused. A result that
fails any of this fails the job and the message names what is wrong. A skipped result (`status` `skipped`) is checked as
its own shape: `version`, `status` and a `reason` of 1 to 300 characters, and no other field.

Fixtures: `result-ok` (one read, one write), `result-model`, `result-absent`, `result-skipped`, `result-matrix`.

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
| `repo` | string | The repository that was read: `dataset-<name>` for a dataset people uploaded, `dataset-<app>-<name>` for one an app produced. |
| `owner` | string or null | The project that shares the dataset; `null` for the run's own project. |
| `ref` | string | The version that was asked for (`latest`, a name, a commit). For a dataset produced in this run it is the commit. |
| `commit` | string | The commit that was read: 64 hex characters. |
| `line` | string, optional | Reserved for the branch form of a dataset (PRD D-01). Absent in the wave. |

A write, with its fields in this order:

| Field | Type | Content |
|---|---|---|
| `name` | string | The name declared in `produces=`. |
| `type` | string | `model`, `artifact` or `dataset`. |
| `repo` | string | The repository written to: `dataset-<app>-<name>` for a dataset, `outputs-<app>` for a model or an artifact. |
| `commit` | string | The commit that holds it: 64 hex characters. |
| `line` | string, optional | Reserved, as above. |
| `path` | string, optional | Where the model or artifact lives when it comes from an earlier run: `<name>/<experiment>/<run>/` of that run, with a final `/`. Only the platform writes it, in the result it hands on for a step this run does not run again (section 13); a step never does. Absent means `<name>/<experiment>/<run>/` of the run that reads it. Datasets need none: they are read by commit. kaos-lib reads a model or an artifact from `path` when it is there; a kaos-lib that does not know the field looks under this run's folder and fails the step, naming the missing files. |

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
7. Inside strings, keys included, `$` is written `\u0024`. The wire form never holds a `$`.

Rule 3 is not optional. The workflow engine evaluates `{{...}}` wherever it meets it, also inside a value
that passes through: check 3(a) shows a string `{{=1+1}}` arriving as `2`. With rule 3 the text never
holds `{{`.

Rule 4 is not optional either. When the workflow engine puts a value into a step's definition it writes
`&`, `<` and `>` as those six-byte escapes itself, so a value that held them raw would take more room there
than its cost says (section 7). Check 3(c) shows a context that is within every cap by count and does not
start because its base holds 2000 raw `&`.

Rule 7 is not optional either. Kubernetes changes a container's environment value: `$(NAME)` becomes the
value of the earlier variable `NAME` of that container, and `$$` becomes `$`. The context travels as an
environment value (in a step's pod, and in the runner's job), so a run parameter holding `$$` or
`$(HPC_JOB_NAME)` would arrive different, and nothing would notice. With rule 7 the text never holds a `$`,
so nothing can expand. Check 3(d) shows both on a real cluster.

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

The follower writes `status.result` of a Succeeded job as the step's `kaos-result` output. The follower's template
declares that output with no default either: it always writes the file, and a follower that ended without it fails
the twin the same way a step does.

Fixture: `hpc-job` (its `spec.context` is `context-one-upstream.wire`, its `status.result` is
`result-model.wire`).

### Retention of runs that have HPC steps

The HPC job records of a run are deleted with the run (the follower makes the Workflow their owner), and the HPC budget ledger is
derived from them: a month's spend is the sum of the settled attempts the records still hold. A budget period is a calendar
month, so a run is kept longer than any month: the engine's default `ttlStrategy` is 35 days (the pipeline context's
`runRetentionDays` may change it, 1 to 3650), and for a pipeline with a step that can run on HPC the render refuses less than 32
days with "runs with HPC steps are kept at least 32 days, so the month's HPC spend stays counted". See `docs/cleanup.md`.

## 9. Reserved names

| What | Reserved |
|---|---|
| Config keys | `plan` and `pipeline-version`. `plan.use.<artifact>` and `plan.target` are reserved for Use and Target; until those ship compose refuses every `plan` key (`plan.promote` with a message that names the run-form field `promote-<dataset>`, any other with a message that it is not available yet). |
| Step names, input names, `produces` names, `values` keys | everything that starts with `kaos-` |
| Step names | also `compose-and-validate`, `main`, `hpc-follower` and names ending in `-hpc`, as before |
| Output names of a step | `kaos-result`; of compose, `kaos-ledger`, `kaos-context-<step>` and `kaos-reused-<step>` |
| Branches of a dataset or outputs repository | `run-*` |
| Tags | `k-*` and `keep-*` |
| Folder at the root of a dataset | `.kaos/` |
| Authoring | `of=` in `dataset(...)` |
| Dataset references | the form `<dataset>.<variant>@<ref>` |
| Ledger entries | `line`; on a write, `path` (written by the platform only, section 13) |
| Run form | `reuse-from-run` and `start-from-step` (section 13) |
| Files in `outputs-<app>` | `kaos-runs/` (section 13) |

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
2. **Each step's result is one output**, `kaos-result`, read from `/work/output/kaos-result.json`, **with no
   default**. A step that ends well without writing the file fails, loudly. On Argo Workflows v4.0.6 the step
   ends `Error` with the message

   ```
   wait: Error (exit code 64): open /var/run/argo/outputs/parameters/work/output/kaos-result.json: no such file or directory
   ```

   and so does the run (checked on the lab, 2026-11-12). A step that did not run (skipped by `when=`, omitted,
   or the other variant of a pair) has an empty output, which the `sprig.default(<sentinel>, ...)` of the
   expression in item 3 turns into the sentinel: the sentinel comes from there and from nowhere else.
   **Retries.** The retry rule of the engine's step templates retries a step lost with its node, not one that
   ends `Error` for a missing result: it is deterministic, so a re-run would only spend the GPU again. The rule
   excludes a message that names `kaos-result.json`; on the lab the step ran once and the run ended `Error`.
   Every step of the wave uses kaos-lib, which exits 1 in this case, and a step that exits 1 is not retried.
3. **The DAG task argument `kaos-context` joins them** with one expression. Task names are always written
   as `tasks["<name>"]`: step names usually hold hyphens.

   For a step that needs `prepare-data`, a step with no HPC variant:

   ```yaml
   - name: kaos-context
     value: >-
       {{=sprig.trimSuffix("}", tasks["compose-and-validate"].outputs.parameters["kaos-context-train"])
       + ",\"upstream\":{\"prepare-data\":"
       + sprig.default(sprig.default("{\"version\":1,\"status\":\"absent\"}", tasks["compose-and-validate"].outputs.parameters["kaos-reused-prepare-data"]), tasks["prepare-data"].outputs.parameters["kaos-result"])
       + "}" + "}"}}
   ```

   The inner `sprig.default(<sentinel>, ...["kaos-reused-prepare-data"])` is the result a run that starts from a step took
   from an earlier run for a step it does not run again (section 13); it is empty in every other run.

   A step that can run on an HPC class is two tasks, `<step>` and `<step>-hpc`, of which exactly one
   runs. Its result is the one that is there, so each such step in `needs=` takes the pair form:

   ```
   sprig.default(
     sprig.default(
       sprig.default("{\"version\":1,\"status\":\"absent\"}", tasks["compose-and-validate"].outputs.parameters["kaos-reused-prepare-data"]),
       tasks["prepare-data-hpc"].outputs.parameters["kaos-result"]),
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
  - a needed step that is conditional (`when=`): `(<step>.Succeeded || <step>.Skipped)`: it ran well, or its
    condition was false. **Never `Omitted`**: a task is Omitted when something it needs failed, so a form that
    accepts Omitted lets the failure through (the step after a failed step's skipped dependant would run);
  - a needed step with an HPC variant: both twins, in the pair form `((<step>.Succeeded || <step>-hpc.Succeeded) ||
    (<step>.Skipped && <step>-hpc.Skipped))`, since one of them is always skipped.
  The rule: a step runs only if every step it needs ran well or was skipped on purpose. No `depends` of the engine
  (steps, the task that joins a matrix step's items, `kaos-promote`) has an `Omitted` term.
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
| 3(d) | Does Kubernetes change `$$` and `$(NAME)` in a container's env value, and does the wire form arrive unchanged? | Pass on kind: `a $$ b $(HPC_JOB_NAME) c` arrives as `a $ b abc c`; the same text in wire form (`$` as `\u0024`) arrives byte for byte |
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

## 12. A matrix step

```python
train = step("train", kind="train", needs=[prepare], gpu=True,
             matrix={"lr": [0.01, 0.001], "batch": [16, 32]},
             produces=[model("detector")])
best = step("pick-best", kind="evaluate", needs=[train], produces=[artifact("winner")])
```

One step, one folder, one image. The run starts the step once for each combination of the values, an **item**, and the
step after it gets all the results.

**The run form.** `<step>-matrix` holds the combinations as a JSON list of objects, one name per axis: the default is
every combination of the values in `matrix=`, in the order written, the first name changing slowest
(`[{"lr":0.01,"batch":16},{"lr":0.01,"batch":32},...]`). `<step>-parallel` is a whole number from 1 to 32 or `all`: how
many items run at the same time (more than there are items means all). **The default is 4**, the 1-GPU steps one node
holds, so that on an HPC class the items share one node; a larger number asks for more nodes. Both follow the step's
`<step>-class` and `<step>-walltime`. Compose checks them before any step starts and refuses, naming the field: not
JSON; not a list or an empty list; more than 32 combinations ("holds 33 combinations; a step runs at most 32. Remove
1."); a combination that is not an object, has a name the step did not declare, or lacks one; a value that is not a
number, a boolean or a text of 1 to 64 letters, digits, spaces and `. _ + / : = @ , ( ) [ ] # % ^ ~ * -`; a whole number
beyond 2 to the power 53 (write it as text); `<step>-parallel` that is neither `all` nor a number from 1 to 32. Compose
writes the checked combinations into `kaos-run-settings`: `{"<step>": {"matrix": [{"index", "of", "text"}, ...],
"parallel": n}}`, where `text` is the item's `matrix` field in wire form.

**What the engine refuses when it renders**, each naming the step and the fix: `matrix=` that is not a dict of
non-empty lists; a name that is not `[a-z][a-z0-9_]*` or is also a section in `reads=`; a value that is not a number, a
boolean or short text, or a whole number beyond 2 to the power 53; more than 32 combinations; the kinds `ingest`,
`publish`, `control`; a dataset in `produces=`; more than 4 things read and written together (`inputs=` plus
`produces=`), because up to 32 items hand their ledgers back in one shared result; `resumable=` or `checkpoint_every=`;
a matrix step that needs a matrix step; a step that needs two matrix steps, or that needs more than 4 steps in all when
one of them is a matrix step; `matrix_allow_failed=` without `matrix=`; a `when=` that reads `<matrix step>.<value>` (a
`when=` on a matrix step itself is fine: every item is skipped).

**The item's context.** Each item is an ordinary run of the step: the same context (section 3), with one more field
after `upstream`: `matrix`, `{"index": 1, "of": 4, "values": {"lr": 0.001, "batch": 16}}`: `index` counts from 0 in the
order of the combinations; `values` are in the order of `matrix=`, each with the number and the text it was written
with (`1.0` stays `1.0`, `1e-05` stays `1e-05`): compose writes the field as text and the workflow system hands the
text over unchanged. `params` is unchanged; kaos-lib puts `values` into the step's `params` as top-level names, so
`step.params["lr"]` is the item's value and a setting and an axis cannot share a name. The step reads, writes and ends
as any step does.

**Where each item writes.** Compose gives every item the same `produces`; kaos-lib makes the paths and branches of an
item's outputs its own by the index: a model or an artifact `<name>` is written under
`<name>/<experiment>/<run>/<index>/` of `outputs-<app>`, on the branch `run-<run>-<step>-<name>-<index>`, and merged
into `main` as one commit per item, whose metadata also has `kaos.matrix.index` and `kaos.matrix.<name>=<value>` for
every axis. So a model has one name and one version for each item, tagged with the item's values. An item reads
`input(name)` of a matrix step's output with the item's index (`step.input(name, item=2)`).

**The entry in `upstream`.** A step that needs a matrix step finds in `upstream.<step>` not a result but one entry
for all items (fixture `result-matrix`): `{"version":1,"status":"matrix","items":[...]}`, one entry per item in index
order: `{"index", "values" (the item's matrix values), "status"}` and, by status, `"result"` (`ok`: the item's
result, section 4, with its `values`, `reads` and `writes`) or `"reason"` (`skipped`: the item ended itself with
`step.skip(reason)`); a `failed` item (it handed back no result) has neither. kaos-lib's `step.upstream("train")` is
then a list with one entry per item.

**What the next step gets of an item's result.** The workflow system gives the results of a step's items back as
parsed values, so a number in an item's `values` arrives in its usual JSON form (`1.0` as `1`, `1e-05` as `1e-05`, a
whole number beyond 2 to the power 53 changed) and the names of an object in alphabetical order. kaos-lib refuses such a
whole number in an item's result (`step.result()` of a step with `matrix=`): hand it on as text. The item's own `matrix`
values in the entry are exact: they come from compose's text.

**A matrix step is like any step: one failed item fails it.** The task that joins the items fails when an item
failed, naming each by index and values; nothing after it runs, the run is Failed and `kaos-promote` does not run. A
skipped item (it ended itself with `step.skip()`) is not a failure. A step that says `matrix_allow_failed=True` has
declared that failed items are acceptable: the join then fails only when no item is `ok`; the step after it runs and sees
every failed item (`status` `failed`, no result), the join's log says how many failed and which, and the run may end
Succeeded and promote, because the author said so. A matrix step that was skipped as a whole (its `when=` was false)
is the sentinel for the steps that need it, as any skipped step is.

**Size.** The `values` of all items share 16 KiB (`MATRIX_VALUES_CAP`): kaos-lib refuses an item's result when its
`values` cost more than 16 KiB divided by `of` (never more than 8 KiB): 512 bytes at 32 items, 4 KiB at 4. The ledger
(`reads`, `writes`) does not count against that share, only against the whole result (8 KiB). The whole entry of the
matrix step in `upstream` may cost 56 KiB (`MATRIX_ENTRY_CAP`): 32 items that read two inputs and write a model cost about
1.5 KiB each, 49 KiB (`engine/tests/test_matrix.py`). A step that needs a matrix step keeps 56 KiB of its context
budget for it, in place of 8 KiB, which is why it may need at most 4 steps in all. The workflow system keeps each item's
result once in the output of every round above it (`fan_output_bytes`: about 210 KiB of results for 32 items of 1.5 KiB at
`<step>-parallel` 4, about 775 KiB at 1), besides each item's own context and output. Measured on the lab (Argo
Workflows v4.0.6, 32 items that read two inputs and write a model, results of 1.5 KiB): the run's status was 779 KB at
`<step>-parallel` 4 and 1.5 MB at 1; the workflow system compresses it (94 KB at 1) and both runs handed back all 32
results (`hack/local-lab`, evidence in the PRD).

**Checkpoints.** A matrix step is never continued from a checkpoint: its runs would share one checkpoint folder.
`resumable=` and `checkpoint_every=` are refused on it, and the render opts the step out of continuation on HPC. The
step's `walltime=` (the form's `<step>-walltime`) is each item's limit.

**How the engine runs it** (the HPC leg is the same, section 8). The step's task `<step>` calls a template
`kaos-matrix-<step>` that runs the step's container for `items` in rounds of `par`: the first `par` items in parallel,
then the template calls itself for the rest, so the limit needs no engine setting. Each item gets the context of the
task (section 10, left open) with `,"matrix":<the item's text>` and the closing brace added. An item that fails does
not end the template (`continueOn: failed`): its output has a default (the sentinel), so the template always ends with
the list of the `kaos-result` and `kaos-item` of the items that handed an output back (an empty list, its default, when
none did; the gather task also marks every item of the plan that is not in the list as failed). A task
`kaos-gather-<step>` (kubecore/gather.py, the compose template's identity) puts them in index order, marks each item
`ok`, `skipped` or `failed`, applies the rule above, and writes the entry as its `kaos-result`; the steps that need the
matrix step read that output (section 10, item 3, with the gather task in place of the step) and wait for that task.
`kaos-promote` waits for it too. On HPC an item is a second step beside it in the same round, `run-hpc`, with the
complementary condition on the class. The job of an item is named after the step and the index (`NODE_KEY`
`<step>-hpc-<index>`), so the items are separate jobs of one run, each with its own result. What the controller does with a
fan-out larger than the project's budget is in `internal/admission/admission.go` and `test/scale` (`TestScaleMatrixBudget`).

Fixtures: `context-matrix-item`, `context-matrix-upstream`, `result-matrix`.

## 13. Starting a run from a step

A run may start from a step and take the results of an earlier run for the steps before it: the steps that are not
run again are not started, and their results and outputs are the earlier run's. It is a run like any other for what it
runs: its own name, its own ledger, its own `latest` rules.

**The record.** Every step that ends well writes, as its last act, `kaos-runs/<run>/<step>.json` on `main` of the app's
`outputs-<app>`, through kaos-lib (`kaos_lib.record`): on its own branch `run-<run>-<step>-record`, merged into `main` as
one commit, the way a model is saved. The file says whose it is, around the exact result the step handed on (section 4,
wire form): `{version: 1, app, run, instance, step, result}` (fixture `record-train`); a step that ends itself with
`step.skip()` has its skipped result there. A step calls `step.result()` last: what it does after is not in the record.
Writing is best effort: when it fails the step warns in its log ("This run cannot be reused from a later step") and still
succeeds. A step with `matrix=` writes none; a step that failed or was never reached has none, and a step skipped by its
`when=` ran no code and has none. Compose writes the run's plan, `kaos-runs/<run>/plan.json` (fixture `run-plan`), when the
checks pass, before any step starts, also best effort: `{version, run, instance, app, project, experiment, steps, pins}`,
and `reused_from: {run, start}` when the run took results from another. For each step the plan holds `name`, `kind`,
`needs`, `inputs`, `produces` (names and types), `matrix` (when it has one), the class the run form gave it (`class`), the
image reference compose resolved for it (`image`, with its digest), its settings (`params`, the config sections it reads),
`when`, the values of the run form fields its `when=` names (the render writes their names into the step table as
`when_fields`), and `when_text`, the condition as written; `pins` holds the read of every input, as the ledger does.
`kaos-runs/` is in the repository of the outputs, beside the folders of the models.

**Whose a record is.** The folder `kaos-runs/<run>/` belongs to one run instance. A run whose name an earlier run of the
app used, and whose `plan.json` is still there with another `instance`, is refused when it starts ("a run named 'R' already
saved its records for this app. Start the run again under a name no earlier run had."); a run that starts compose again
(same instance) is not. When a run is reused, compose checks that the plan is at `version` 1 and is the run's and the app's,
and that every record it takes has the same app, run, instance and step as the plan; one that does not is refused.

**Retention.** `kaos-runs/` is never removed: a record is the size of a result (at most 8 KiB) and a plan the size of the
step table and the settings, one file for each step of each run, and they stay with the repository. Nothing in the platform
deletes them yet.

**The run form.** Two text fields, empty by default, which mean a normal run:

| Field | Meaning |
|---|---|
| `reuse-from-run` | "Optional. The name of an earlier run of this app. Steps before the step below are not run again; their results and outputs are taken from that run. Leave empty for a normal run." |
| `start-from-step` | "Optional. The step to run first; it and every step after it run again. Needs the earlier run above. Steps: <the steps>." |

Both reach compose as `--reuse-from-run=` and `--start-from-step=`, as `--promote` does, and a render also passes
`--hpc-classes` (the names of the HPC classes) and one `--when-field <field>=<value>` for each form field a `when=` names.

**Which tasks run.** Let X be `start-from-step`. A step is run again when X is empty, or is the step, or is a step it needs
(directly or through others): the step is X or after X. Every other step is not run again, whether it is before X or on a
branch that never meets it. The render gives every task one more term in its `when`, first, so that a step that is not run
again never evaluates its own condition: `{{=workflow.parameters['start-from-step'] in ['', '<step>', '<each step it
needs, directly or not>'] && (<the step's own when>)}}` (without the parenthesis when it has none). The HPC variant of a step
has the same term, as it has the step's own `when`. A task whose `when` is false is Skipped and its `depends` are met by
a bare name (`depends: <step>` accepts Skipped; checked on Argo Workflows v4.0.6), so the steps after it start: no change to
`depends`. (The tolerant form of section 10, `(<step>.Succeeded || <step>.Skipped || <step>.Omitted)`, is for a step that
may be skipped: it is not used here, because a failed step makes the next task Omitted and every task that tolerates Omitted
would then run.)

**The result of a step that is not run again.** Compose declares one more output for each step, `kaos-reused-<step>`
(`/work/reused/<step>.json`): the step's result as the earlier run handed it on, in wire form, with `path` added to every
model or artifact write, and empty for a step that runs. The expression of section 10, item 3 reads a step's result as

```
sprig.default(sprig.default("{\"version\":1,\"status\":\"absent\"}", tasks["compose-and-validate"].outputs.parameters["kaos-reused-<step>"]), tasks["<step>"].outputs.parameters["kaos-result"])
```

and the pair of a step with an HPC variant as `sprig.default(sprig.default(sprig.default(<sentinel>, <reused>),
tasks["<step>-hpc"]...["kaos-result"]), tasks["<step>"]...["kaos-result"])`: a step that ran gives its own result, a step
that is not run again the earlier run's, and a step that did not run the sentinel. Everything that reads a result goes
through that expression and works unchanged: the context of a step, a `when=` on a value (also one that reads a value of a
step not run again, both ways), the final task's plan, the pair of a twin. A result may cost 8 KiB (section 7); compose
refuses a result that costs more with the paths added.

**The inputs.** An input a step that is not run again read is pinned to the commit the earlier run recorded in `pins`, never
resolved again: `latest` in the run form is not looked at again for it, and a form value that names another dataset, or a
version that is another commit, is refused. An input read only by steps that run again is resolved as always.

**The checks.** Compose does all of them before any step starts, and each refusal is one plain sentence after
`compose-and-validate FAILED:` that says what to change:

| Refused | Sentence (the start is X, the run R) |
|---|---|
| one field empty | `reuse-from-run and start-from-step go together: <field> is empty.` |
| X is not a step | `start-from-step: this pipeline has no step 'X'. Its steps are: a, b, c.` |
| a step that is not run again has `matrix=` | `step 'S' runs once for each combination of its matrix= and this run does not run it again (it starts from 'X'): a step with matrix= cannot be reused yet. Start from step 'S'.` |
| no plan of R for this app | `reuse-from-run: run 'R' saved no record for this app (its step images may be older than this feature, or saving failed: see that run's log). Check the name: it must be a run of this app.` |
| the plan is not at version 1, or is not R's or the app's | `reuse-from-run: the record of run 'R' is not one this platform reads. Choose another run, or start a run of the whole pipeline.` |
| a record of R belongs to another instance, app, run or step | `the records of run 'R' do not all belong to one run: the record of step 'S' was saved by another run that had the same name. Choose another run, or start a run of the whole pipeline.` |
| the records of R cannot be read | `reuse-from-run: the records of run 'R' cannot be read right now. Run again in a minute; if it keeps happening, tell the platform team the name of the run.` (what the store answered is in compose's log, never in the sentence) |
| R took results from another run | `reuse-from-run: run 'R' took the results of some of its steps from run 'R0', so it has no record of them. Reuse run 'R0' instead, or choose a run that ran every step it has.` |
| a step that is not run again has no `ok` or `skipped` record in R | `run 'R' has no result for step 'S' (it failed or did not finish). A step before 'X' is not run again without one. Start from step 'S', or choose a run that finished it.` A step with no record whose `when=` is on run form fields only is taken as skipped when that condition is false with the form values R recorded (what R's other steps saw: no result); one whose `when=` reads a value of another step cannot be told: `step 'S' has no record in run 'R' and its condition reads a value of another step, so it cannot be told whether it was skipped or did not finish. ...` |
| the step is not in R's plan | `step 'S' is not in run 'R': the pipeline has changed since. A step before 'X' is not run again. Start from step 'S'.` |
| its kind, needs, inputs or produces differ | `step 'S' is not the step run 'R' ran: its <kind> was <old> and is <new>. A step before 'X' is not run again. Start from step 'S'.` |
| its settings or the form values its `when=` names differ | `step 'S' reads different settings than in run 'R': <key: old -> new, ...>. A step before 'X' is not run again, so its settings cannot change. Set them back, or start from step 'S'.` |
| its image differs from R's | `step 'S' was built from a different image in run 'R' than it has now, so its code changed. Start from step 'S'.` An image reference without a digest, in R or now, cannot be compared: `step 'S' has no image with a digest in run 'R' or now, so its code cannot be compared. ...` |
| it ran on an HPC class in R | `step 'S' ran on the HPC (Slurm) class 'C' in run 'R': the result of a step that ran there cannot be reused yet. Start from step 'S'.` |
| a step that runs again on an HPC class reads a model or an artifact of a step not run again | `step 'T' is set to run on the HPC (Slurm) class 'C' and reads 'M' of step 'S', which this run takes from run 'R': an HPC step cannot read it yet. Choose a machine class of the cluster for 'T', or start from step 'S'.` |
| an input's form value differs from what R read | `input 'I': step S is not run again and read <repo>@<ref> (commit <12>) in run 'R', but the form now names '<value>'. Set it back, or leave the version out, or start from step 'S'.` |
| an output a step that runs again needs is gone (a dataset commit not readable, no object under a model's or artifact's path) | `output 'O' of step 'S' (run 'R') is no longer there: <why>. Run that step again: start from step 'S'.` |
| `promote-<d>` is true and a step not run again made `<d>` | `promote-<d> is true, but the dataset <d> was made by step S of run R, which this run does not run again. Promote it from run R, or start from step S.` |

On success compose logs one line for each step not run again (`step S: reused from run R`, `(it was skipped there)` for one
the earlier run skipped), and each pin taken from R. **The code of a step that is not run again is the code of R, and it is
compared:** the image reference compose resolved for the step, which carries a digest, must be the same as the one in R's
plan. Not in this version: a step with `matrix=` that is not run again, a result from an HPC step, an HPC step that reads
a reused model or artifact, a run that reused another (so the earlier run to name is the one that ran the steps), a step
skipped by its `when=` in R on a value of another step that is not run again (it has no record: start from it).
