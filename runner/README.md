# kaos-hpc runner

The controller-owned runner of one attempt of an `HPCArgoWorkflowJob` (PRD-HPC-1461 phase 1, D-58/D-64/D-65).
It is the proven in-pod waiter of the KAOS pipeline template (one optional integration; `kubecore/meluxina.py`, dev be9e52b),
moved under the controller and without its admission client: it submits the Slurm job (or re-attaches to the one a previous runner of this attempt submitted),
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
| `image.py` | registry client, docker-archive streaming, `image_key` (tar name = SIF name = `kaos-image=` lease, always in the job comment), `cleanup_images` (asks the stager's `gc` when an attempt is over; AC-65) |
| `jobscript.py` | the batch script (+ KAOS_METRIC sampler, tar deletion after SIF build) |
| `payload/` | on-node programs: stagein/stageout/ckpt/slots/idlewatch (`payloads.py` loads them) |
| `sharing.py` | PRD-1609 inputs-in-script |
| `reporter.py` | status merge patches: `status.run`, `status.outputs` (<= 64 KiB), `status.pendingReason` |
| `logs.py` | stager `log` streaming, JSON lines, `status.run.logOffset`, last/max GPU use of the sampler lines in `status.run.gpuUtilization` (AC-69) |
| `runner.py` | the attempt: submit/adopt, poll loop, resubmits, end |

## Inputs
Env (controller): `HPC_JOB_NAMESPACE`, `HPC_JOB_NAME`, `KAOS_ATTEMPT`, `KAOS_RESUME_REF` (a
`scratch://<tar path>#<sha256>` checkpoint reference), `HPC_CKPT_BUDGET_PERCENT`, `IMAGE_REF`, `STEP_COMMAND`, `SLURM_TIME_LIMIT`,
`HPC_RESUMABLE_LEAD`, `STEP_OUTPUTS`, `HPC_CLASS`, `STEP_NAME`, `WF_UID`, `KUBECORE_WORKFLOW_NAME`, `KUBECORE_RUN_ID`,
`DATASET`, `HPC_SHARING` (`on`/`1`) + `HPC_SCOPE`, allowlisted `OUTPUTS_REPO`/`OUTPUTS_REF`/`CHECKPOINT_*`/
`KAOS_LIB_SUPPORT`, site: `HPC_ACCOUNT`, `HPC_USER`, `HPC_SCRATCH`, `HPC_HOME`, `HPC_CLASSES_JSON`, `HPC_ENDPOINT`,
`HPC_REST_VERSION`, the lane the controller chose (`HPC_QOS`, `HPC_RESERVATION`, `HPC_QOS_MAX_MINUTES`), the idle window
(`HPC_IDLE_TIMEOUT_MINUTES`, 0 = off, default 20), the site's per-user submit cap when the profile has one
(`HPC_MAX_SUBMIT_JOBS`), optional `HPC_SITE_WAIT_MAX_S` (how long an unreachable site is waited for, default 1800; the controller's `--site-outage-max`), `HPC_LOGIN_HOST` (`host:port`, default login.lxp.lu:8822), `SLURM_TOKEN` (fallback),
and the ConfigMap: `ZITADEL_DOMAIN`, `ZITADEL_MLFLOW_PROJECT_ID`, `MLFLOW_EXTERNAL_URL`, `LAKEFS_EXTERNAL_URL`.
Files: `/etc/meluxina-jwt/token`, `/etc/meluxina-staging/{id_ed25519,known_hosts}`,
`/etc/mlflow-svc/ZITADEL_MACHINE_KEY` (or `ZITADEL_MACHINE_KEY_FILE`), the pod ServiceAccount token.

## Exit codes

The job script and the runner reserve these codes; the controller turns them into the reasons in
[docs/reasons.md](../docs/reasons.md).

| Code | Who | Meaning | Job ends |
|---|---|---|---|
| 85 | the step | saved a checkpoint, continue me (the wall-time signal arrived) | continues from the newest verified copy, else `Timeout` (checkpoint missing) / `NotResumable` |
| 210 | job script | no `apptainer` after loading the module | `SiteFAILED` |
| 231 | job script | the image could not be pulled (the registry token expires while queued: the runner resubmits with a fresh one, bounded) | `SiteFAILED` |
| 232 | job script | in-job dataset staging failed (never run without the data) | `SiteFAILED` |
| 233 | job script | the batch script could not be written for the slot manager | `SiteFAILED` |
| 234 | job script | no verified checkpoint copy to restore | `SiteFAILED` |
| 235 | job script | the final checkpoint copy or the job comment could not be written and verified | `SiteFAILED` |
| 236 | job script | the idle watchdog ended the step | `Idle` |
| 237 | runner | staging does not fit the project's scratch quota (reported FAILED, no site job) | `InsufficientQuota` |
| 238 | runner | the site refused the submit for its per-user cap (reported FAILED, no site job; the message has the cap and the count read from the site) | `SubmitLimit` |

## The site job's name, adopt, and a site that does not answer
The site job is named `kaos-[<scope>-]<workflow uid>-<step>-<first 8 of the HPC job id>-a<attempt>`. A job script rewrites
the job *comment*, never the name, so the name is the attempt's immutable key. The adopt step looks for it, in this order:
the live job (`GET /jobs`), the finished job in accounting (`GET /slurmdb/<v>/jobs?job_name=...`, newest one that was not
cancelled and is newer than the job the previous runner reported), then the job the previous runner reported. A submit the
site accepted whose answer was lost is therefore found again even when the job has already ended: never a second submission.
The previous release's name (no HPC job, no attempt) is still recognised among live jobs.

Before the site job exists, a site that does not answer (connection error, timeout, 429/502/503/504, a nameless 500, a dead
slurmctld) is waited for: ask again after 5, 10, 20 ... 60 s (adopt first, then submit), `pendingReason: SiteUnavailable`,
fail with `SiteUnavailable: ...` only after `HPC_SITE_WAIT_MAX_S`. A 4xx, an auth error or a refusal that names a reason fails at once.

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

The job comment has ONE writer on the node, `kaos_comment` (`jobscript.COMMENT_HELPER`, exported to the copier and used by
`fail()`): it reads the live comment (`squeue -o %k`), replaces only the words it sets and any old `errN:` tail, keeps every
other `kaos-*` word (dataset lease, image lease, checkpoint lease), writes, and reads the words back (3 tries). `KAOS_JOB_ID`
is exported so the copier writes to the right job. The exit-85 final copy runs to the end, comment included, before the
script exits (the job is still live); a comment that cannot be verified ends the job 235. Scratch is out of the runner's
reach (the staging key runs only the stager's fixed commands), so a missing comment word cannot be repaired from the copy
manifest; the runner's message says which of the two is unknown.

## Live lessons

Each lesson of the old in-pod waiter and the test that pins it: [docs/live-lessons.md](../docs/live-lessons.md)
(PRD-HPC-1461 AC-11). A new live incident gets a test named after it, with the live job id, and a row there.

## The step's GPU variables (AC-37, live job 5439721)

Before the step starts, the job script strips whitespace from `CUDA_VISIBLE_DEVICES` and `NVIDIA_VISIBLE_DEVICES`
(on MeluXina a whole-node step got `"0,1,2,3     "`; CUDA tools may drop the last device or reject the value), in
every path: alone or shared, fenced or not (the slot launcher also cleans the list it stamps as `KAOS_STEP_GPUS`).
A step of a G-GPU class also sees `KUBECORE_SLOT_GPUS=G` when it runs alone (`HPC_GPUS`); on a shared node the slot
manager sets it (1). `KUBECORE_SLOT_INDEX` is not set by the runner.

## GPU isolation on a shared node (AC-57 / D-29)

MeluXina has `ConstrainDevices=no`: a step's `CUDA_VISIBLE_DEVICES` steers CUDA but every GPU device file is
still visible. On a shared node (`KUBECORE_SLOT_SHARED=1`) two layers keep a step on its own GPU(s):

- **Prevention (`jobscript.py`).** If `nvidia-container-cli` exists and a throw-away `apptainer exec --nv --nvccli`
  succeeds, the step runs with `--nv --nvccli`, `NVIDIA_VISIBLE_DEVICES` set to its GPUs and
  `CUDA_VISIBLE_DEVICES` rewritten to `0..G-1` (the bound devices are renumbered). The job log says
  `gpu fence: nvccli ...` or why not. Otherwise plain `--nv`.
- **Detection (`payload/slots.py`).** The slot manager reads `nvidia-smi --query-compute-apps`, attributes each pid
  to a step through `SLURM_STEP_ID` and its `KAOS_STEP_GPUS` (the set the step launcher stamped), and cancels a
  step that computes on a GPU outside it. Only that step stops; its host comment gets `reason=GPUViolation`.
  No `nvidia-smi` (CPU classes) or a step with no stamped set: nothing is judged.
