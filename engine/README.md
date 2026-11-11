# engine/ - the KAOS pipeline engine

The platform's own copy of the pipeline authoring and compose code: the `kubecore/` package that the
`hera-pipeline-template` and `yolo-pipelines-template` vendor into every ML app, plus the runner that renders an
app's `pipeline.py` with it. PRD-HPC-1461 D-79: a platform change to rendering, enhancing or composing must never
need a commit in a client's app repository, so CI runs THIS code and ignores the app's vendored copy.

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
| `kaos_pipeline_engine/known-vendored.json` | every historical blob of `derive_tree.py` / `authoring.py` -> profile + template commits |
| `kaos_pipeline_engine/authoring-schema.json` | `step()` options for the pipeline linter, the removed keywords with what to write instead, and the migration guide's address |
| `Dockerfile.engine`, `pyproject.toml`, `setup.py`, `requirements*.txt` | image and wheel  The image keeps the pinned set at `/etc/kaos-engine/constraints.txt` for images built on top of it |
| `tools/` | generators for the two JSON files |
| `tests/` | `template/` = the kubecore tests that came with the template; `app/` = the fixture app they render; the rest are the engine's tests |

## The step contract

The engine builds every pipeline to [`docs/step-contract.md`](../docs/step-contract.md) (PRD kaos#1911). There
is one contract and no older one beside it: a pipeline written for the earlier keywords fails the render.

**Authoring** (`kubecore/authoring.py`):

```python
from kubecore.authoring import artifact, dataset, model, pipeline, step

step(name, *, kind=, reads=, inputs=, needs=, produces=, gpu=, disk=, when=, walltime=,
     resumable=, checkpoint_every=, idle_timeout=)
```

| Keyword | Rule the engine enforces at render |
|---|---|
| `kind=` | Required: `ingest`, `transform`, `train`, `evaluate`, `publish` or `control`. It becomes the label `platform.kubecore.io/step-kind` on the step's pod, on its HPC follower pod, and (set by the follower) on its HPC job |
| `inputs=` | Names of top-level config values that hold a dataset reference. Each must exist in the config tree and be text |
| `needs=` | At most ten steps. A name in `inputs=` and a name a needed step produces must differ; two needed steps must not produce the same name |
| `produces=` | `model("name")`, `artifact("name")`, `dataset("name")`. A dataset has one producing step in the pipeline. `dataset(name, of=...)` is refused until that form ships |
| `walltime=` | `'90m'`, `'12h'` or `'720'`: the run limit on an HPC class |

Per kind: `ingest`, `publish` and `control` steps are offered no HPC class and get no HPC variant; a `control`
step takes no `inputs=` and no `produces=`.

Refused at render, each with a message that names the step and what to write: a missing or unknown `kind=`;
`outputs=` and `hpc_time_limit=` (removed; the message says what replaces them and points at
`authoring.MIGRATION_GUIDE_URL`, a placeholder until the guide has an address); a step or produced name that
starts with `kaos-`; a config tree that defines `plan` or `pipeline-version`, or a leaf whose form field would
get one of those names; a step whose rendered definition is over 24 KiB.

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
- writes each step's base context in wire form (`/work/context/<step>.json`): `params` is the resolved config
  without the `platform` section (the context never holds an endpoint), `ledger` holds the step's own inputs,
  `experiment` is config `experiment.name` or else the app's name, `produces` names each repository;
- writes the run's ledger at version 2, every pinned input, as the output `kaos-ledger`;
- reads the run's own key in the ADVANCED override: `plan.promote.<dataset>: true` puts `promote: true` on that
  produced dataset in the producing step's context. `plan.use`, `plan.target` and `pipeline-version` are
  reserved and refused until they ship, so a run never silently ignores one;
- refuses a step whose base context costs more than 96 KiB less 8 KiB for each step it needs (cost: the bytes
  plus one for each quote and backslash, what the value takes inside a step's definition).

The enhancer gives compose the run's identity (`--app`, `--project`); authoring gives it the run's name and the
step table (`--run`, `--steps`).

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
4. The app's own `kubecore/` is APPENDED to `kubecore.__path__`: extra client modules still import as
   `kubecore.<x>`, but every engine name wins. A vendored `authoring.py` that raises on import is never executed.
5. `pipeline.py` runs with `runpy.run_path(..., run_name="__main__")` in the repo dir; it writes
   `out/raw-workflow-template.yaml` as before. `from kubecore.authoring import pipeline, step` is unchanged.
6. Afterwards every engine module must still be the one in `sys.modules`; if `pipeline.py` swapped one, exit 3
   (tampered).
7. The app's `pyproject.toml` dependencies that the interpreter does not satisfy are printed to stderr as
   `kaos-engine: extra-dep <requirement>` (and to `--extra-deps-out`). Installing them is CI's job, under a
   constraints file; the runner never installs anything and the app is never `pip install -e`'d.

Exit codes: 0 rendered, 1 `pipeline.py` raised, 2 bad invocation, 3 tampered, 4 engine broken.

## Schema profiles

`KUBECORE_SCHEMA_PROFILE` selects the structured-config schemas in `derive_tree.py` (`DataConfig`,
`QuantizationConfig`). The two current templates differ in exactly two places, and `derive_tree.py` has had two
older schema variants, so there are four profiles (default `hera` = upstream):

| profile | DataConfig | QuantizationConfig.output_prefix | seen in |
|---|---|---|---|
| `hera` | `ref="main"` | `""` | hera template since 2026-10-19 (and yolo 2026-10-19) |
| `yolo` | no `ref` | none | yolo template since 2026-11-01 |
| `legacy-v1` | `ref`, `version=""` | `"quantization"` | both templates 2026-07-14 .. 2026-10-19 |
| `yolo-v1` | no `ref` | `""` | yolo 2026-11-01 (one blob) |

`derive_tree.CONFIG_DIR` is resolved lazily from `KUBECORE_CONFIG_DIR` (a monkeypatched real `Path` still wins).

Choosing the profile: hash the app's vendored `kubecore/derive_tree.py` (git blob sha) and look it up in
`known-vendored.json` -> **`engine_profile`** (this is what `decide` must use; the file's `profile` is only the template
lineage, `ambiguous` = both templates carried the blob). An unknown blob falls back to the KubeApp's template.
`engine_profile: null` means the app must stay legacy, with `no_profile_reason`. Today that is exactly three blobs, the
first authoring (2026-07-13/14: ddec976, d40194a, b468ddb): it wrote the form defaults as `Parameter(default=...)`
where every later version writes `value=` (Argo reads `value`), so the engine's render differs in
`/spec/arguments/parameters[*]`. Rendering them "equal" would reproduce a bug, so they are left to the legacy path
and counted in the fleet.

## Tests

`make test-engine` (a pinned venv under `bin/engine-venv`) and `make lint-engine` (ruff) on a workstation; CI runs both in
the `engine-tests` stage of `Dockerfile.engine` (the Go CI container has no Python), so building the image runs them.

| Tests | What they hold |
|---|---|
| `tests/test_step_contract.py` | Loads `docs/step-contract/fixtures/`: the wire writer equals every `.wire` file; a context assembled from compose's base and upstream results equals the context fixtures byte for byte, a skipped upstream included; the ledger; the follower's side of the `hpc-job` fixture |
| `tests/test_authoring_contract.py` | Every keyword and every render refusal, with its message |
| `tests/test_compose_inputs.py` | Pinned inputs, contexts, `plan`, the size cap, every compose refusal with its message, the compose entry point |
| `tests/template/test_step_contract_enhance.py` | The per-kind HPC rules, the result pair of a step with an HPC variant, the size of a rendered step |
| `tests/test_follower_contract.py`, Go `internal/follower/engine_contract_test.go` | The golden follower template against what the follower reads |
| the rest | The enhancer, resilience, the runner contract incl. the booby-trap (a vendored `authoring.py` that raises still renders), the wheel, the authoring schema, known-vendored |

Golden files (`tests/template/golden/`) are regenerated with `UPDATE_GOLDEN=1 make test-engine`; read the diff.

## Updating the engine

Edit `engine/kubecore/` directly, regenerate `authoring-schema.json` with `tools/build_authoring_schema.py` when
`step()` changed, run `make test-engine`, and commit. A change to what a step is given or hands back starts in
`docs/step-contract.md` and its fixtures, never here.
