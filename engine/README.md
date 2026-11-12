# engine/ - the KAOS pipeline engine

The pipeline authoring, enhancing and compose code (the `kubecore/` package an app's `pipeline.py` imports), plus
the runner that renders an app's `pipeline.py` with it. PRD-HPC-1461 D-79: a platform change to rendering,
enhancing or composing must never need a commit in a client's app repository, so apps carry no copy of this code
and CI runs this one.

It is released by the same build as the controller, at the same version (D-58/D-63): `kaos-hpc-operator:<v>`
(controller, follower, runner, helper), `kaos-pipeline-engine:<v>` (image) and the wheel
`kaos-pipeline-engine==<v as PEP 440>`. This directory only builds and tests them; the CI wiring is elsewhere.

## Layout

| path | what |
|---|---|
| `kubecore/` | the engine. **This is its source** (PRD-HPC-1461 D-80, 2026-11-10): apps carry no copy and there is no upstream to stay equal to. Edit it here |
| `kaos_engine/render.py` | the runner, `python -I -m kaos_engine.render` |
| `kubecore/step_contract.py` | the step contract as the engine writes it: names, caps, the wire form, the expression that assembles a step's context (see "The step contract" below) |
| `kaos_engine/compose.py` | what the compose-and-validate step runs, `python -I -m kaos_engine.compose`: the engine's compose with the engine first on the path. Pins every declared input and writes each step's context and the run's ledger |
| `kaos_engine/version.py` | build tag -> PEP 440 |
| `kaos_pipeline_engine/authoring-schema.json` | `step()` options for the pipeline linter, the removed keywords with the reason each is refused, and `old_app_marker` |
| `Dockerfile.engine`, `pyproject.toml`, `setup.py`, `requirements*.txt` | image and wheel  The image keeps the pinned set at `/etc/kaos-engine/constraints.txt` for images built on top of it |
| `tools/` | the generator of `authoring-schema.json` |
| `tests/` | `template/` = the kubecore tests that came with the template; `app/` = the fixture app they render; the rest are the engine's tests |

## The step contract

The engine builds every pipeline to [`docs/step-contract.md`](../docs/step-contract.md) (PRD kaos#1911). There
is one contract and no older one beside it: a pipeline written for the earlier keywords fails the render.

**Authoring** (`kubecore/authoring.py`):

```python
from kubecore.authoring import artifact, dataset, model, pipeline, step

step(name, *, kind=, reads=, inputs=, needs=, produces=, gpu=, when=, walltime=,
     resumable=, checkpoint_every=, idle_timeout=)
```

| Keyword | Rule the engine enforces at render |
|---|---|
| `kind=` | Required: `ingest`, `transform`, `train`, `evaluate`, `publish` or `control`. It becomes the label `platform.kubecore.io/step-kind` on the step's pod, on its HPC follower pod, and (set by the follower) on its HPC job |
| `name` | Lowercase letters, digits and hyphens, at most 40 characters. Not `compose-and-validate`, `main`, `hpc-follower`, nothing ending in `-hpc`, nothing starting with `kaos-` |
| `reads=` | Config sections that exist in the tree. They are the step's `params`: a step gets these sections and no other |
| `inputs=` | Names of top-level config values that hold a dataset reference. Each must exist in the config tree and be text. Same name rule as a step; not starting with `kaos-` |
| `needs=` | At most ten steps. A name in `inputs=` and a name a needed step produces must differ; two needed steps must not produce the same name. `inputs=` plus the datasets the needed steps produce: at most 16 |
| `produces=` | `model("name")`, `artifact("name")`, `dataset("name")`, same name rule. A dataset has one producing step in the pipeline. `dataset(name, of=...)` is refused until that form ships |
| `when=` | `"<field> == <value>"` or `!=`, joined by `and` / `or` (`and` first, no parentheses). `<field>` must be a field of the run form (a setting's path with dashes); the value is a bare word or number, or quoted. Rendered as `{{=workflow.parameters['<field>'] == '<value>' ...}}` on the task and on its HPC variant. The workflow system's own `{{...}}` form is refused |
| `walltime=` | `'90m'`, `'12h'` or `'720'`: the run limit on an HPC class |

Per kind: `ingest`, `publish` and `control` steps are offered no HPC class and get no HPC variant; a `control`
step takes no `inputs=` and no `produces=`.

A dataset reference names the dataset's repository, `dataset-<name>@<version>`, wherever one is written: on the
form, in `config/`, in `kaosctl`. A dataset an app produces is named per app, so two apps of a project never
collide on one: `produces=[dataset("clean")]` in the app `hera1` is the repository `dataset-hera1-clean`, and the
engine computes that name in one place (`step_contract.produced_repo`) and writes it into the step's context. The
engineer writes the short name everywhere in the app (`dataset("clean")`, `step.output("clean")`, `step.input("clean")`,
`plan.promote.clean`). A later step of the same run reads what was just written by its produced name; any other
run, pipeline or app reads it by its repository, through a config value:

```python
# pipeline A
prepare = step("prepare", kind="transform", inputs=["raw"], produces=[dataset("clean")])
train   = step("train", kind="train", needs=[prepare])    # in the step: step.input("clean")

# pipeline B, config/data.yaml:   clean: dataset-<app>-clean@latest      (the app that produces it; here dataset-hera1-clean@latest)
step("evaluate", kind="evaluate", inputs=["clean"])       # in the step: step.input("clean")
```

Refused at render, each with a message that names the step and what to write: a missing or unknown `kind=` (a missing
`kind=` in a pipeline where no step has one also says the app is from before the contract);
`outputs=`, `hpc_time_limit=` and `disk=` (removed); a step that produces a dataset and is pinned to an HPC class; a step or produced name that
starts with `kaos-`; a config tree that defines `plan` or `pipeline-version`, or a leaf whose form field would
get one of those names; a step (or the HPC follower) whose rendered definition is over 24 KiB, counted as the
workflow engine encodes it (`&`, `<` and `>` cost six bytes each); an app name over 55 characters; a
`config/config.yaml` that still lists the removed `platform` group in its defaults.

The `platform` config group is gone. The platform used to add a `platform` section (endpoints, the outputs
repository) to every app's config; no step can read it now, so the group, its search path and the enhancer's
overrides for it are deleted. An app that still names it fails the render, and compose, with:

```
config/config.yaml lists `- platform: live` in its defaults, and the platform no longer adds a `platform`
section to an app's config. This app was made before the current pipelines. Create it again from the template.
```

**No app is moved to this contract.** Old apps are deleted and created again from the template. The refusals
that identify an app from before the contract each say what is wrong in one sentence and end with
`step_contract.OLD_APP_NOTICE`, "This app was made before the current pipelines. Create it again from the
template." Its second sentence is `step_contract.OLD_APP_MARKER`, published as `old_app_marker` in the authoring
schema. They are: no step of the pipeline with a `kind=` (when at least one step has a `kind=`, a step without it is a
mistake in a new pipeline and gets the plain message, which ends at the list of kinds; the refusal is raised when the
pipeline is assembled, not at the `step(...)` call); `outputs=`, `hpc_time_limit=` or `disk=`; the `platform` group in
`config/config.yaml`; a top-level `platform` config key (render and compose); a `kubecore/` folder in the app.
Every other refusal (an unknown `kind`, a name that is too long or reserved, a wrong `walltime`, ...) is a mistake in
a new pipeline and keeps its plain message without the sentence.

**A step that produces a dataset is not offered an HPC class.** A job on HPC holds a token the lake treats as
admin, which would bypass the rule that only the producing app writes a produced dataset. So a step whose
`produces=` holds a `dataset(...)` gets no HPC class in its `<step>-class` choice and no `<step>-hpc` twin, like
the kinds `ingest`, `publish` and `control` (`meluxina.runs_on_hpc`, the one predicate; the authoring step marks the
pod with `platform.kubecore.io/produces-dataset`). A step that produces such a dataset and is pinned to an HPC class
fails the render ("a step that produces a dataset runs on the cluster for now; HPC (Slurm) classes are offered to
it once jobs there hold the app's own rights"). The rule is `step_contract.DATASET_PRODUCERS_STAY_ON_CLUSTER`; PRD
kaos#1911 F-40 (HPC jobs that hold the app's own rights) removes it. A step that produces only a model or an
artifact is unaffected.

**What a step gets and hands back.** Every step container runs `python -m app.entry` with no arguments. Its one
input parameter is `kaos-context`, which the container reads as `KAOS_STEP_CONTEXT`; its one output parameter is
`kaos-result`, read from `/work/output/kaos-result.json`, with the sentinel `{"version":1,"status":"absent"}` as
the default when the step wrote none. The DAG task argument joins the step's base context (the compose step's
output `kaos-context-<step>`) with the result of every step in `needs=`, as text, each through `sprig.default`
with the sentinel: a skipped or omitted step has an empty output. Section 10 of the contract has the expression;
`step_contract.context_expression` writes it.

**Compose** (`kubecore/compose.py`), before any other step starts:

- pins every input any step declares to one commit, once per name (`<dataset>@<version>`; no version means
  `latest`, the head of the dataset's `main` now), and refuses an empty value, a value that names no dataset, a
  version that does not exist and a store that does not answer, naming the input and the steps that declare it;
- writes each step's base context in wire form (`/work/context/<step>.json`): `params` holds the config
  sections the step names in `reads=`, `ledger` holds the step's own inputs,
  `experiment` is config `experiment.name` or else the app's name, `produces` names each repository;
- writes the run's ledger at version 2, every pinned input, as the output `kaos-ledger`;
- reads the run's own key in the ADVANCED override: `plan.promote.<dataset>: true` (the name in `produces=`;
  `true` or `false` in any letter case) puts `promote: true` on that produced dataset in the producing step's context. `plan.use`, `plan.target` and `pipeline-version` are
  reserved and refused until they ship, so a run never silently ignores one;
- refuses a config that uses a resolver (`${oc.env:NAME}` and every other `${resolver:...}`; the render refuses it
  first, naming the key) and resolves the config with an empty environment besides, because this pod holds the
  platform's credentials and a value must never read them into a step's context;
- refuses an ADVANCED override over 16 KiB, and a dataset reference whose version is over 255 characters or whose
  repository id is over 63;
- refuses a step whose base context costs more than 96 KiB less 8 KiB for each step it needs (cost: the bytes
  plus one for each quote and backslash, what the value takes inside a step's definition).

The enhancer gives compose the run's identity (`--app`, `--project`); authoring gives it the run's name and the
step table (`--run`, `--steps`).

**A step gets the node of its class.** The class is the node. The run form has a class for every step
(`<step>-class`, an enum of the pool's classes, plus the HPC classes for a kind that may run there; a step is offered
the pool's GPU classes only when its code declares `gpu=True`, any other step the CPU classes only, or what the pool
has when it declares no CPU class) and nothing to
type: no cpu, memory or disk field. A step's pod asks for what a node of the class it was given offers a pod,
cpu and memory, and the limits equal the requests.

- **Where the numbers come from.** The operator computes them per class (`allocatable` in the pipeline context,
  `kubecore-operator` `internal/operators/kubeapp/phases/reconciling.go`, `gkeAllocatable`): the machine's capacity
  less GKE's reservation for the node system, the DaemonSets measured on every node (1.22 GiB and 0.586 cpu, which is
  more than the 1.13 GiB and 0.52 cpu a live dev node carries today), a safety margin (0.5 GiB, 0.25 cpu) and, on a
  GPU node, the accelerator's headroom (1.8 GiB, 0.35 cpu), rounded down to whole units. The engine knows no machine
  type and adds nothing: `step_contract` has no table, `enhance.node_sizes` reads the context. The checkpoint
  helper's sidecar (10m, 64Mi) and the workflow executor's containers (no requests) live inside the margin.
- **At run time.** The class is chosen at submit, so the numbers follow it the way the GPU does: the template takes
  inputs `cpu`, `mem` and `disk`, a `podSpecPatch` puts them into the main container's requests (cpu and memory also
  its limits), and each task
  that runs the step computes them from `<step>-class` with an expression over a table of the pool's classes
  (`{"cpu-standard": "7", "gpu-t4": "7"}[workflow.parameters['train-class']]`, with the default class's number as
  the fallback for a class not in the table: an HPC class, whose cluster task never runs). A step pinned to one class
  has the same numbers as plain container resources.
- **Limits equal requests** so a step that overruns its memory is stopped inside its own cgroup instead of pressing on
  the node's daemons, and cannot starve them. A step cannot ask for more than its class has, so no pod waits for a node
  that cannot hold it. Several cluster steps no longer share a node by lowering fields by hand; HPC node sharing is
  unchanged.
- **Disk** follows the class the same way, and the engineer no longer asks for it (`disk=` is refused). The request
  of a step's ephemeral storage is the usable disk of its class less `enhance.IMAGE_RESERVE_GIB` (10 GiB, because
  the container images share that disk; at least 1Gi, never more than the node gives), and 10Gi
  (`DISK_REQUEST_DEFAULT`) for a class whose usable disk the platform does not say: unlike cpu and memory, that does
  not fail the render. It is a request and no limit. **For the operator:** the engine reads
  `computeClasses.{cpu,gpu}.allocatable.ephemeralStorageGiB` and `computeClasses.all[].allocatable.ephemeralStorageGiB`,
  a whole number of GiB (the node's allocatable ephemeral storage less a margin), absent when the disk is unknown,
  in one function, `enhance.node_disks`. The template takes a third input `disk`, each task computes it from
  `<step>-class` like `cpu` and `mem`, and a step pinned to one class has a static request.
- **A class that cannot be sized fails the render.** The operator's table holds GCE machine types; any other (an AWS
  type, a GCE type not in the table) has no `allocatable`, and a pool that offers such a class fails the render naming
  it. Before, the same class crashed the render with a `KeyError` when it was a step's default, and sized the step as
  the default class when it was picked on the form.

**What a definition refers to besides its context, and the limit that leaves.** A step's definition must stay
under what the workflow engine can hand a pod (131057 bytes). The context is bounded by the caps above and the
rendered definition by the 24 KiB budget; what remains are the references the workflow engine fills in when
the run starts. `tests/template/test_step_contract_enhance.py` fails when a rendered step or follower definition
holds a reference that is not in this list:

| In | Reference | What bounds its value |
|---|---|---|
| every step | `inputs.parameters.kaos-context` | the context caps |
| every step | `workflow.parameters.image-<step>` | the app's image record, written by CI |
| every step | `workflow.parameters.<step>-class` | a form field with an `enum`: the pool's classes, plus the HPC classes for a kind that may run there. A step pinned to one class has no such field |
| a step that is not pinned to one class | `inputs.parameters.cpu`, `inputs.parameters.mem` | computed by the engine from the class (a lookup table of the pool's classes written at render): a number and a quantity like `7` and `27Gi` |
| GPU steps | `inputs.parameters.gpus` | `0` or `1`, set by the engine |
| steps with checkpoint copies, the follower | `workflow.name`, `workflow.uid` | the workflow engine |
| the follower | `inputs.parameters.step-name`, `image`, `step-command`, `step-kind`, `time-limit`, `deadline-seconds`, `hpc-class`, `resumable-lead`, `resumable`, `idle-timeout` | written by the engine at render, or one of the form fields above |

The compose step is the one definition that carries the run's free text: every config leaf of the form is one of
its arguments, and so is the ADVANCED override (`workflow.parameters.config`). Compose refuses an override above
16 KiB (`the override is too large; put the values in the app's config`). Its rendered definition is about 5 KiB, so
an override of roughly 60 KiB or more (less when it holds many quotes) makes the compose step too large to start
before it can refuse anything: that run ends with the workflow engine's error, not with a message of ours. Nothing
in the engine can check it, the value is only known when the run is submitted; the charts package looks at
validating the size at submit.

**The step table, for CI and the form.** The compose step's argument after `--steps` is one JSON array, written
at render, with one object per step in pipeline order. It is the place to read what a pipeline declares without
running `pipeline.py` again:

```json
[{"name":"prepare","kind":"transform","reads":[],"inputs":["raw"],"needs":[],
  "produces":[{"name":"clean","type":"dataset"}]},
 {"name":"train","kind":"train","reads":["train"],"inputs":["raw","labels"],"needs":["prepare"],
  "produces":[{"name":"model","type":"model"}]}]
```

```python
compose = next(t for t in wft["spec"]["templates"] if t["name"] == "compose-and-validate")
args = compose["container"]["args"]
steps = json.loads(args[args.index("--steps") + 1])
input_names = sorted({name for step in steps for name in step["inputs"]})     # ["labels", "raw"]
produced = [e["name"] for step in steps for e in step["produces"] if e["type"] == "dataset"]   # ["clean"]
```

Every name in `input_names` is a form field of the same name (`spec.arguments.parameters[].name`): an input is a
top-level config value, and a top-level scalar keeps its name on the form. Those are the fields that hold a
dataset reference, so those are the fields to fill with the catalog's choices (`dataset-<name>@<version>`; for a dataset an app produces, `dataset-<app>-<name>`). The
field named `dataset` is no longer special: a pipeline may have none, or several under other names. `produced`
lists the datasets the pipeline writes (repository `dataset-<app>-<name>`: `step_contract.dataset_repo`), for whoever sets up what the app may write.
The value is in wire form: plain JSON to any parser.

**For the charts package (W4).**

- To tell an app from before the contract from any other render failure, match `step_contract.OLD_APP_MARKER`
  ("Create it again from the template.", `old_app_marker` in `kaos_pipeline_engine/authoring-schema.json`) in the
  render's output. There is no migration guide and no address to match.
- The run form no longer has `<step>-cpu`, `<step>-mem` or `<step>-disk`; anything that fills, validates or lists them
  goes. The class (`<step>-class`) stays an enum.
- The input names are in the step table (above). The field named `dataset` is no longer special.
- `files/engine.py:50,149,151` still names the profiles `legacy-v1` and `yolo-v1` and reads the app's own
  `kubecore/derive_tree.py` to choose one. Both go: an app that carries a `kubecore/` folder now fails the render
  as an app made before the current pipelines whatever profile is passed (the runner checks the folder before it checks the profile),
  and the profile is `yolo` when the config tree has a `quantization` section, else `hera`.
- `tests/connections` fixtures and goldens (14 files) hold the old step shape: `step-outputs`, `dataset-pin`,
  `data-lock`, `compose-and-validate.outputs.parameters.params`. Regenerate them from the engine.
- `_helpers.tpl` `add_step_token` adds a volume (232 bytes with a 100-character audience), a mount (83) and an
  environment value (74) to a step's template after the engine's size check: at most 423 bytes with the keys, against
  the 8000 bytes the budget keeps free (131057 less 96 KiB less 24 KiB is 8177).
- The compose step no longer has an output named `params`; its outputs are `kaos-ledger` and one
  `kaos-context-<step>`. The composed config is written to a file only when `--output` is passed (`kaosctl`'s local
  run does), never in the cluster.

**The HPC variant** (`kubecore/meluxina.py`). The `<step>-hpc` task gets the same `kaos-context` argument as the
step's own task, and the follower receives it as `KAOS_STEP_CONTEXT`. The follower template has two outputs:
`slurm-job-id` and `kaos-result` (`/tmp/outputs/kaos-result.json`, no default: the follower always writes it).
A step that needs a step with an HPC variant reads the result from the pair: exactly one of the two tasks ran.

## GPU driver prelude

`kubecore/enhance.py` (`enhance_gpu_driver`) wraps the container command of every `gpu=True` step that runs on a KAOS GPU
node (GKE, EKS) in a POSIX `sh -c` prelude: the NVIDIA driver folders (`/usr/local/nvidia/lib64`, `/usr/local/nvidia/lib`,
`/usr/local/nvidia/bin`) are PREPENDED to the image's own `LD_LIBRARY_PATH` / `PATH`, a ctypes `cuInit(0)` check
fails the step in its first second with a `KAOS: this GPU step cannot reach the NVIDIA driver (...)` line (skipped
with a notice when the image has no python3, and when the step was given 0 GPUs, a cpu class), then `exec "$@"`
runs the original command unchanged. It runs after the HPC twin is built, so a Slurm step (follower + Apptainer
`--nv`) is never wrapped. The same change is in hera-pipeline-template `kubecore/enhance.py` (#100); `kubecore/` is synced to that template
commit (d167461), so the engine enhancer is byte for byte the legacy one (including `--helper-configmap`, which the
chart has passed since 0.12.149). Tests: `tests/template/test_gpu_driver_prelude.py`.

## Versioning

* Source of truth: the operator build tag. `v0.4.7` -> wheel `0.4.7`; a dev build
  `v0.4.7-dev-20261110-123456-abc1234` -> `0.4.7.post1.dev20261110123456+abc1234` (a post-release, so it sorts after
  the tag it names and before the next tag). `setup.py` reads `$KAOS_ENGINE_TAG`, else the repo's `VERSION`.
* The engine version IS the operator version: the operator's compiled `HPCOperatorVersion` pins the follower, the helper and the
  engine together, one pin per control plane.
* The wheel installs `kaos_engine` and `kaos_pipeline_engine/kubecore/...`. It NEVER installs a top-level
  `kubecore`: kaos-lib 0.4 ships a `kubecore` alias (PRD-1147 T-04/T-14) and the two would collide.
  `tests/test_wheel.py` proves it.
* Image: `docker build -f engine/Dockerfile.engine --build-arg ENGINE_TAG=<tag> -t kaos-pipeline-engine:<v> .` from
  the repository root. `python:3.12.13-slim` by digest (the CI render's Python), engine at `/work/kubecore`, runner
  installed, `KAOS_ENGINE_DIR=/work`, label `io.kaos.engine`. Libraries: `requirements.txt` (hera 5.27.0,
  hydra-core 1.3.8, omegaconf 2.3.1, PyYAML 6.0.3 + transitive pins), what `pip install -e .` of the template's
  `hera>=5.16,<6 hydra-core>=1.3,<1.4 omegaconf>=2.3,<2.4 PyYAML>=6,<7` resolved to on 2026-11-10.

## The runner contract

```
python -I -m kaos_engine.render pipeline.py --repo /workspace/repo --profile hera|yolo \
       [--engine DIR] [--config-dir DIR] [--extra-deps-out FILE]
```

1. `sys.path` = engine dir, app repo, then the interpreter's own entries (stdlib, site-packages). The engine dir is
   `--engine`, else `$KAOS_ENGINE_DIR`, else next to the runner (the wheel's `kaos_pipeline_engine/`).
2. `KUBECORE_SCHEMA_PROFILE` (`--profile`, required) and `KUBECORE_CONFIG_DIR` (`--config-dir`, default
   `<repo>/config`) are set before the engine is imported.
3. `kubecore` and every engine submodule are imported from the engine dir and their `__file__` asserted (else exit 4).
   A `kubecore/` folder in the app is never imported.
4. `pipeline.py` runs with `runpy.run_path(..., run_name="__main__")` in the repo dir; it writes
   `out/raw-workflow-template.yaml` as before. `from kubecore.authoring import pipeline, step` is unchanged.
5. Afterwards every engine module must still be the one in `sys.modules`; if `pipeline.py` swapped one, exit 3
   (tampered).
6. The app's `pyproject.toml` dependencies that the interpreter does not satisfy are printed to stderr as
   `kaos-engine: extra-dep <requirement>` (and to `--extra-deps-out`). Installing them is CI's job, under a
   constraints file; the runner never installs anything and the app is never `pip install -e`'d.

Exit codes: 0 rendered, 1 `pipeline.py` raised, 2 bad invocation, 3 tampered, 4 engine broken.

## Schema profiles

`KUBECORE_SCHEMA_PROFILE` selects the structured-config schemas in `derive_tree.py` (`DataConfig`,
`QuantizationConfig`) that the two templates' config trees name in their defaults list. The templates differ in
two places, so there are two profiles (default `hera`):

| profile | DataConfig | QuantizationConfig.output_prefix |
|---|---|---|
| `hera` | `ref="main"`, no `source`, no `path_override` (no step reads them) | `""` |
| `yolo` | no `ref`, no `source`, no `path_override` (no step reads them) | none |

CI picks the profile from the app's config tree (`yolo` when it has a `quantization` section, else `hera`).
`derive_tree.CONFIG_DIR` is resolved lazily from `KUBECORE_CONFIG_DIR` (a monkeypatched real `Path` still wins).

## Tests

`make test-engine` (a pinned venv under `bin/engine-venv`) and `make lint-engine` (ruff) on a workstation; CI runs both in
the `engine-tests` stage of `Dockerfile.engine` (the Go CI container has no Python), so building the image runs them.

| Tests | What they hold |
|---|---|
| `tests/test_step_contract.py` | Loads `docs/step-contract/fixtures/`: the wire writer equals every `.wire` file; a context assembled from compose's base and upstream results equals the context fixtures byte for byte, a skipped upstream included; the ledger; the follower's side of the `hpc-job` fixture |
| `tests/test_authoring_contract.py` | Every keyword and every render refusal, with its message |
| `tests/test_compose_inputs.py` | Pinned inputs, contexts, `plan`, the size cap, every compose refusal with its message, the compose entry point |
| `tests/template/test_step_contract_enhance.py` | The per-kind HPC rules, the result pair of a step with an HPC variant, the size of a rendered step, the references in a definition, the node sizing of every class shape |
| `tests/test_compose_kaos_lib.py` | Compose against the real kaos-lib 0.5 (skipped without it: `make test-engine ENGINE_KAOS_LIB=<kaos-lib checkout>`) |
| `tests/test_follower_contract.py`, Go `internal/follower/engine_contract_test.go` | The golden follower template against what the follower reads |
| the rest | The enhancer, resilience, the runner contract incl. the booby-trap (a `kubecore/authoring.py` in the app that raises still renders: it is never imported), the wheel, the authoring schema |

Golden files (`tests/template/golden/`) are regenerated with `UPDATE_GOLDEN=1 make test-engine`; read the diff.

## Updating the engine

Edit `engine/kubecore/` directly, regenerate `authoring-schema.json` with `tools/build_authoring_schema.py` when
`step()` changed, run `make test-engine`, and commit. A change to what a step is given or hands back starts in
`docs/step-contract.md` and its fixtures, never here.
