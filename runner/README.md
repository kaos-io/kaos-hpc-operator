# kaos-hpc runner

The controller-owned runner of one attempt of an `HPCArgoWorkflowJob` (PRD-HPC-1461 phase 1, D-58/D-64/D-65).
It is the proven in-pod MeluXina waiter (hera-pipeline-template `kubecore/meluxina.py`, dev be9e52b) without its
admission client: it submits the Slurm job (or re-attaches to the one a previous runner of this attempt submitted),
stages dataset and image through the login-node stager, follows the site job to its end (its state comes from the
controller's `status.siteObservation`, D-36; the runner asks the site itself only when that is missing or stale), streams the job log, reports to the job's
status and exits 0. A continuation is the controller's decision, so a new attempt is a new runner Job.

Python 3.12+, stdlib only, nothing installed at run time. Entry: `python3 -u /runner/main.py`
(or `PYTHONPATH=/runner python3 -m kaos_hpc_runner`). Tests: `python -m pytest runner/tests -q` (pytest and
`openssl` are test-only).

## Modules (`kaos_hpc_runner/`)
| module | what |
|---|---|
| `settings.py` | all inputs, read once from the controller's env; identity, class, dataset, sharing, resume |
| `slurm.py` | slurmrestd/slurmdbd client (token re-read per request), comment/share parsing, `host_gone` |
| `wallet.py` | Zitadel wallet mint (RS256 in stdlib, no PyJWT), image WORKDIR lookup |
| `staging.py` | `Stager` (ssh to the forced-command stager), dataset staging |
| `image.py` | registry client, docker-archive streaming, `image_key` (tar name = `kaos-image=` lease) |
| `jobscript.py` | the batch script (+ KAOS_METRIC sampler, tar deletion after SIF build) |
| `payload/` | on-node programs: stagein/stageout/ckpt/slots/idlewatch (`payloads.py` loads them) |
| `sharing.py` | PRD-1609 inputs-in-script |
| `reporter.py` | status merge patches: `status.run`, `status.outputs` (<= 64 KiB), `status.pendingReason` |
| `logs.py` | stager `log` streaming, JSON lines, `status.run.logOffset` |
| `runner.py` | the attempt: submit/adopt, poll loop, resubmits, end |

## Inputs
Env (controller): `HPC_JOB_NAMESPACE`, `HPC_JOB_NAME`, `KAOS_ATTEMPT`, `KAOS_RESUME_REF` (+ `KAOS_RESUME_PREFIX`
or `KAOS_RESUME_FROM_ATTEMPT`, or a `lakefs://` URI), `IMAGE_REF`, `STEP_COMMAND`, `SLURM_TIME_LIMIT`,
`HPC_RESUMABLE_LEAD`, `STEP_OUTPUTS`, `HPC_CLASS`, `STEP_NAME`, `WF_UID`, `KUBECORE_WORKFLOW_NAME`, `KUBECORE_RUN_ID`,
`DATASET`, `HPC_SHARING` (`on`/`1`) + `HPC_SCOPE`, allowlisted `OUTPUTS_REPO`/`OUTPUTS_REF`/`CHECKPOINT_*`/
`KAOS_LIB_SUPPORT`, site: `HPC_ACCOUNT`, `HPC_USER`, `HPC_SCRATCH`, `HPC_HOME`, `HPC_CLASSES_JSON`, `HPC_ENDPOINT`,
`HPC_REST_VERSION`, optional `HPC_LOGIN_HOST` (`host:port`, default login.lxp.lu:8822), `SLURM_TOKEN` (fallback),
and the ConfigMap: `ZITADEL_DOMAIN`, `ZITADEL_MLFLOW_PROJECT_ID`, `MLFLOW_EXTERNAL_URL`, `LAKEFS_EXTERNAL_URL`.
Files: `/etc/meluxina-jwt/token`, `/etc/meluxina-staging/{id_ed25519,known_hosts}`,
`/etc/mlflow-svc/ZITADEL_MACHINE_KEY` (or `ZITADEL_MACHINE_KEY_FILE`), the pod ServiceAccount token.

## Recovery and checkpoints (PRD-HPC-1461 D-74, D-75)

Every step gets a checkpoint folder: `$KAOS_WORK/ckpt` on scratch, `KAOS_CHECKPOINT_DIR=/work/ckpt` in the container.
`payload/ckpt.py` (Python 3.6, stdlib) keeps copies of it, bounded for the project's scratch quota:

| rule | how |
|---|---|
| at most 2 copies per step | the oldest is deleted before a new one is written (also for the wall-time copy) |
| one archive + one manifest per copy | `kaos-ckpt/<workflow uid>/<step>/<seq>.tar` + `<seq>.json` (sha256 of every file and of the tar); never a file-by-file mirror |
| periodic copy | `watch` every N minutes (N = 10x the last copy's time, 5 to 60 min, start 15), only if the folder changed |
| wall-time copy | SIGUSR2 from the batch shell's USR1 trap, and `final` after exit 85; ignores the guard, replaces the oldest copy |
| space guard (periodic only) | skip when the copy would take the project over 80 % of its scratch space or file quota, or past the checkpoint budget (`HPC_CKPT_BUDGET_PERCENT` of the HPCSite, default 10 % of the space quota, counting every step's copies); `lfs quota` unreadable = skip; the last copy is kept and the reason goes to the step log and `status.run.checkpoint.skippedReason` |
| success | the job script deletes the step's copies and scratch |
| abandoned | `expire` at every job start deletes copies untouched for 7 days unless a queued or running job names `kaos-ckpt-lease=<workflow uid>/<step>` in its comment (the runner puts it in the submit comment; the copier keeps it in every update) |
| restore | `restore` verifies the tar's sha256 and every file's size, tries the named copy then the previous one, refuses unsafe members, and exits 1 (job exit 234) if none verifies |

After each copy the job comment carries `kaos-ckpt=<tar sha256> kaos-ckpt-path=<tar path> kaos-ckpt-info=<json>`; the
runner reports them as `status.run.checkpointRef` (`scratch://<path>#<sha256>`) and `status.run.checkpoint` for every
recoverable end (exit 85, TIMEOUT, NODE_FAIL, PREEMPTED, BOOT_FAIL, REVOKED, idle exit 236). The controller decides.
