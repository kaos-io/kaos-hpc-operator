# kaos-hpc runner

The controller-owned runner of one attempt of an `HPCArgoWorkflowJob` (PRD-HPC-1461 phase 1, D-58/D-64/D-65).
It is the proven in-pod MeluXina waiter (hera-pipeline-template `kubecore/meluxina.py`, dev be9e52b) without its
admission client: it submits the Slurm job (or re-attaches to the one a previous runner of this attempt submitted),
stages dataset and image through the login-node stager, polls to the end, streams the job log, reports to the job's
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
| `payload/` | on-node programs: stagein/stageout/ckptin/ckptout/slots (`payloads.py` loads them) |
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
