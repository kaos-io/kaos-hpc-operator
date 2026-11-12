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
step(name, *, kind, reads=, inputs=, needs=, produces=, gpu=, when=, walltime=,
     resumable=, checkpoint_every=, idle_timeout=)
```

Everything after `name` is keyword-only.

| Keyword | Meaning |
|---|---|
| `name` | The step's name. |
| `kind=` | Required. One of `ingest`, `transform`, `train`, `evaluate`, `publish`, `control`; each has a contract (below). The engine labels every pod and HPC job with it (`platform.kubecore.io/step-kind`). |
| `reads=` | The config sections the step reads. They are the step's `params`. |
| `inputs=` | The names of the config values that hold a dataset reference. An input name is a top-level config key: no dotted paths. Compose pins each to one commit when the run starts. Step code names the input, never a version. |
| `needs=` | The steps this step runs after, at most ten. Each one's result reaches this step in `upstream`. |
| `produces=` | What the step writes: `model("name")`, `artifact("name")`, `dataset("name")`. A dataset an app produces is the repository `dataset-<app>-<name>`: `dataset("clean")` in the app `hera1` is `dataset-hera1-clean`. The name is per app because every app made from a template declares the same names and a dataset has exactly one producer; two apps of a project never share one. |
| `gpu=` | Unchanged. |
| `when=` | A condition in plain words: a name, `==` or `!=`, and a value. The name is a field of the run form, `when="quantization-mode != off"` (a setting's path with dashes, as the run form shows it), or a value a step in `needs=` handed on, `when="evaluate.passed == true"`: `<step>.<value>`, where `<value>` is a keyword of that step's `step.result(...)` (letters, digits and `_`; a step that is not in this step's `needs=` is refused, naming the fix). The value is a bare word or number (`off`, `3`) or a quoted string (`"two words"`). Comparisons join with `and` / `or` (`and` binds tighter; no parentheses, no other operators). The engine writes it as one condition on the step's task and on its HPC variant, comparing the field's value as text: `quantization-mode != off` becomes `{{=workflow.parameters['quantization-mode'] != 'off'}}` (`and` becomes `&&`, `or` becomes a double bar, in the order written). A step's value is read from the needed step's result (`kaos-result`) through the expression of section 10 item 3. **What is compared, exactly:** a value that is a string is compared with the text as text; a value that is a number or a boolean is compared by its JSON text, as the workflow system's `toJSON` writes it (`1000000` is `1000000`, `1e6` written in `when=` is read as `1000000`, `1.0` is `1`, `1e-5` is `0.00001`, `true` is `true`), never through `string()` (which writes `1e+06` for 1000000, `1.2345678e+07` for 12345678 and `1e-05` for 0.00001). Written in `when=`, a number is read the way JSON writes numbers, so `1e6`, `1000000` and `1000000.0` all match the value 1000000. `evaluate.rows != 1000000` therefore skips the step when `rows` is 1000000. A text that cannot be matched reliably is refused when the pipeline is rendered, naming the fix: a number written another way than JSON writes it (`.5`, `+1`, `01`, `5.`), a `True`/`NULL` that is not lowercase, an integer above 9007199254740992 (a result's number is a double), a number too large to hold, and any control character (a line break, a tab; also refused in `when=` as a whole). A `null` or missing value stops the run (below). When the needed step ran on an HPC class the result is read from its HPC variant (the pair, section 10); the text is the same on both. A value that is not there (the step was skipped or did not run, or never set it) fails the task: `int("<sentence>")` of a sentence is the one failure the expression language lets us word, and the workflow system shows `failed to evaluate expression: invalid operation: int(step 'evaluate' gave no value 'passed' (it was skipped, did not run, or never set it): a when= on a step's value needs the value)` as the task's error, so a missing value is never a quiet `false` (checked on the lab, Argo Workflows v4.0.6: 29 comparisons over 999999, 1000000, 12345678, 3000000000, 0.5, 1.0, 1e-5, true, false, an empty string and a string that looks like a number, on the cluster and on the HPC variant). The step is skipped when the condition is false. |
| `walltime=` | The step's time limit, on any kind: `'90m'`, `'12h'` or `'720'` (minutes). It is the default of the step's run-form field `<step>-walltime` (below). |
| `resumable=`, `idle_timeout=` | Unchanged. |
| `checkpoint_every=` | How often the step's checkpoint is saved: `'20m'`, `'1h'` or `20` (minutes); `False` for no copies. It is the default of the run-form field `<step>-checkpoint-every`. |

Removed, with no alias: `outputs=` (the result replaces it), `hpc_time_limit=` (`walltime=` replaces it) and
`disk=`: a step is not told its disk, it gets the disk of the machine its class runs on (the usable disk of that
class, less a fixed reserve for the container images, or 10Gi where the platform does not say). A step that
that passes one is refused with what to write instead: "step 'x': outputs= is gone. Declare what the step writes with produces=[...] and hand it on with step.result()."; "step 'x': hpc_time_limit= is gone. Use walltime=, for example walltime=\"12h\"."; "step 'x': disk= is gone. The step gets the disk of the machine class it runs on." Any other unknown keyword is refused with a sentence that lists the keywords that exist.

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
Waiting for a machine does not count: a step that never gets one is bounded by the platform's pending reaper, not by
this limit. A step that declares `checkpoint_every=` (not `False`) also has `<step>-checkpoint-every` ("How often
the 'x' step's checkpoint is saved: minutes or hours, for example 10m or 1h."), default the value in code. Both are
text: minutes (`90`, `90m`) or hours (`12h`), at least one minute; compose refuses any other value before a step
starts, naming the field: `<step>-walltime is '1d'. Write minutes or hours, for example 90, 90m or 12h: at least
one minute, at most 30 days, no other unit.` Compose writes the numbers as its output `kaos-run-settings` (`{"<step>": {"walltime":
minutes, "checkpoint-every": minutes}}`), which the deadline, the HPC job's limit and the checkpoint copies read.
A step with a checkpoint interval has the field `checkpoint_every` (minutes, a number) in its context, after
`produces`; a step without one has no such field.

**A long job continues instead of starting over.** A step that declares `resumable=` is run again, and continues
from its newest checkpoint, in each of three cases, on a cluster machine and on an HPC class, at most five times
after the first run: it reaches its time limit, its machine is lost, or it asks to be restarted (exit code 85,
`kaos_lib.checkpoint.exit_for_restart`). The time limit applies to each run of the step, not to the sum: on the
cluster each run is a new pod with the full deadline; on an HPC class each continuation is a new job with the full
limit, and the follower's own deadline covers the queue and every continuation. On the cluster the step is run
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
`context-escapes`.

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

Fixtures: `result-ok` (one read, one write), `result-model`, `result-absent`, `result-skipped`.

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

## 9. Reserved names

| What | Reserved |
|---|---|
| Config keys | `plan` and `pipeline-version`. `plan.use.<artifact>` and `plan.target` are reserved for Use and Target; until those ship compose refuses every `plan` key (`plan.promote` with a message that names the run-form field `promote-<dataset>`, any other with a message that it is not available yet). |
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
