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
| `kaos_engine/compose.py` | what the compose-and-validate step runs, `python -I -m kaos_engine.compose`: the engine's compose with the engine first on the path. Writes `params.yaml` and the run's data lock (D-81) |
| `kaos_engine/version.py` | build tag -> PEP 440 |
| `kaos_pipeline_engine/known-vendored.json` | every historical blob of `derive_tree.py` / `authoring.py` -> profile + template commits |
| `kaos_pipeline_engine/authoring-schema.json` | `step()` options for the pipeline linter |
| `kaos_pipeline_engine/compat-surface.json` | every module-level name `authoring` / `derive_tree` ever had |
| `Dockerfile.engine`, `pyproject.toml`, `setup.py`, `requirements*.txt` | image and wheel |
| `tools/` | generators for the three JSON files, the corpus |
| `tests/` | `template/` = the template's own kubecore tests, moved in; `app/` = the fixture app they render; the rest are the engine's tests |
| `CORPUS_REPORT.md` | the last corpus run (generated) |

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
the `engine-tests` stage of `Dockerfile.engine` (the Go CI container has no Python), so building the image runs them. They cover the template's kubecore tests, the runner contract incl. the booby-trap (a vendored
`authoring.py` that raises still renders), the wheel (build, contents, `python -I` from an installed wheel), the
compat surface, the run's data lock (`tests/test_data_lock.py`), the authoring schema, known-vendored and the follower contract
(`tests/test_follower_contract.py` and the Go side `internal/follower/engine_contract_test.go`, which feeds the
golden follower template to `ConfigFromEnv`).

`make engine-corpus` clones both public templates and the private apps named in `$KAOS_ENGINE_CORPUS_LAB`
(comma-separated `org/repo:profile`, supplied privately and never committed; needs network) and renders every historical state
twice: with the repo's own vendored `kubecore` (`python pipeline.py`, what CI did) and with the runner; the parsed
YAML must be equal. It rewrites `CORPUS_REPORT.md`, where private apps appear only as `app-N`. Without `KAOS_ENGINE_CORPUS_DIR` the corpus test is skipped.

## Updating the engine

Edit `engine/kubecore/` directly, regenerate `compat-surface.json` and `authoring-schema.json` with their
`tools/build_*.py` when the authoring surface changed, run `make test-engine`, and commit. Skew rule: compose gains flags in release N,
authoring may emit them from N+1.
