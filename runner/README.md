# kaos-hpc runner

The controller-owned runner of one attempt of an `HPCArgoWorkflowJob` (PRD-HPC-1461 phase 1, D-58/D-64/D-65).
It is the proven in-pod waiter of the KAOS pipeline template (one optional integration; `kubecore/meluxina.py`, dev be9e52b),
moved under the controller and without its admission client: it submits the Slurm job (or re-attaches to the one a previous runner of this attempt submitted),
stages every input (dataset) and the image through the login-node stager, follows the site job to its end (its state comes from the
controller's `status.siteObservation`, D-36; the runner asks the site itself only when that is missing or stale), streams the job log, reports to the job's
status and exits 0. A continuation is the controller's decision, so a new attempt is a new runner Job.

Python 3.12+, stdlib only, nothing installed at run time. Entry: `python3 -u /runner/main.py`
(or `PYTHONPATH=/runner python3 -m kaos_hpc_runner`). Tests: `python -m pytest runner/tests -q` (pytest and
`openssl` are test-only).

## Modules (`kaos_hpc_runner/`)
| module | what |
|---|---|
| `settings.py` | all inputs, read once from the controller's env; identity, class, the step context and the inputs (`check_job`), sharing, resume |
| `slurm.py` | slurmrestd/slurmdbd client (token re-read per request), comment/share parsing, `host_gone` |
| `wallet.py` | Zitadel wallet mint (RS256 in stdlib, no PyJWT), image WORKDIR lookup |
| `staging.py` | `Stager` (ssh to the forced-command stager), dataset staging (every object of the pinned commit, whatever the dataset's shape; `CONTRACT` is the staging contract the runner checks against the staging tool's `version` and sends with every `status` and `stage`; `name_rule` and `Names` refuse, before anything is staged or submitted, the object names that cannot be copied), `staging_record` (one bounded record of `status.run.staged` per input: kind first/delta/cached, files, bytes, seconds; the follower prints one line from each) |
| `image.py` | registry client, docker-archive streaming, `image_key` (tar name = SIF name = `kaos-image=` lease, always in the job comment), `cleanup_images` (asks the stager's `gc` when an attempt is over; AC-65) |
| `jobscript.py` | the batch script (+ KAOS_METRIC sampler, tar deletion after SIF build) |
| `payload/` | on-node programs: stagein/handback/ckpt/slots/idlewatch (`payloads.py` loads them). Stage-in believes a cache hit only if `.kaos-complete` is still in the folder after it looked for the cleanup's lock, and removes `name.part-*` files of a killed older writer that the commit does not list before it marks a copy |
| `result.py` | the step's result on its way back: the check (`version` 1, `status` `ok`, `values`, `reads`, `writes`, 8 KiB by cost) and the wire form it is written in |
| `sharing.py` | PRD-1609 inputs-in-script |
| `reporter.py` | status merge patches: `status.run`, `status.result` (only with the report of a step that ended well), `status.pendingReason` |
| `logs.py` | stager `log` streaming, JSON lines, `status.run.logOffset`, last/max GPU use of the sampler lines in `status.run.gpuUtilization` (AC-69) |
| `runner.py` | the attempt: submit/adopt (`site_job` builds the script and the environment that are submitted), poll loop, resubmits, end (`fetch_result`) |

## Inputs
Env (controller): `HPC_JOB_NAMESPACE`, `HPC_JOB_NAME`, `KAOS_ATTEMPT`, `KAOS_RESUME_REF` (a
`scratch://<tar path>#<sha256>` checkpoint reference), `HPC_CKPT_BUDGET_PERCENT`, `IMAGE_REF`, `STEP_COMMAND`, `SLURM_TIME_LIMIT`,
`HPC_RESUMABLE_LEAD`, `HPC_CLASS`, `STEP_NAME`, `WF_UID`, `WF_NAME` (labels the log lines), `KAOS_APP` (the app: its outputs repository is `outputs-<app>`),
`KAOS_STEP_CONTEXT` (the step context, exactly as the job carries it; the runner never reads into it), `HPC_INPUTS` (the job's inputs as JSON: `name`, `repo`, `commit`, `ref`; the runner never resolves a version), `HPC_SHARING` (`on`/`1`) + `HPC_SCOPE`, allowlisted
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
| 232 | job script | an input could not be staged in the job, or its copy is not complete (never run without the data) | `SiteFAILED` |
| 233 | job script | the batch script could not be written for the slot manager | `SiteFAILED` |
| 234 | job script | no verified checkpoint copy to restore | `SiteFAILED` |
| 235 | job script | the final checkpoint copy or the job comment could not be written and verified | `SiteFAILED` |
| 236 | job script | the idle watchdog ended the step | `Idle` |
| 237 | runner | staging does not fit the project's scratch quota (reported FAILED, no site job) | `InsufficientQuota` |
| 238 | runner | the site refused the submit for its per-user cap (reported FAILED, no site job; the message has the cap and the count read from the site) | `SubmitLimit` |
| 239 | runner | the step's image reference can never be pulled (reported FAILED, no site job) | `InvalidImage` |
| 240 | runner | the slot manager stopped the step for computing on a GPU outside its set, on a node shared by several steps | `GPUViolation` |
| 241 | runner | the site's staging tool speaks another staging contract, or is not installed for the project after 20 minutes of asking (counted from the first answer that it is absent; reported FAILED, nothing staged, no site job) | `StagingMismatch` |
| 242 | runner | the dataset holds an object that cannot be copied to the site: a name reserved for the platform, a path that could leave the dataset's folder, a name too long, or a file that is also a folder (reported FAILED, nothing staged, no site job) | `ReservedName` |
| 243 | job script, or runner | the step ended well but its result could not be handed back: the job could not read or upload the result file (job script); the runner could not fetch it, nothing arrived, or the run has no way to hand a result back, in which case nothing is submitted (runner, reported FAILED) | `ResultNotHandedBack` |
| 244 | runner | the result file the job handed back is not a result of the step contract (reported FAILED) | `InvalidResult` |

## What the step gets and hands back (the step contract)

The site job is built by `Runner.site_job`. The step's command is `python -m app.entry`.

**The context.** `KAOS_STEP_CONTEXT` reaches the step exactly as the job carries it (`spec.context`): in the Slurm
environment for a job alone on its node; on a shared node as an `export` line of the job script (a host can read a
waiting job's script, never its environment), quoted so that no character of it is changed. Nothing in the runner or
the job script parses it.

**The inputs.** Each entry of `HPC_INPUTS` is staged whole, at its pinned commit: from a login node by the site's
staging tool when the runner has the staging key, else in the job (`payload/stagein.py`, on the job's own time). The
job script is told where (`KAOS_INPUTS`, one word per input: `name|repository|commit|resident path`) and presents both
kinds the same way, read-only in the container:

```
$KAOS_STAGED_ROOT/                 /kaos/inputs: a folder of this job
    <input>/                       the copy of the dataset commit, bound here
    <input>.kaos-ready             {"repo": "...", "commit": "...", "full": true}
```

The record is written by the job script only for a copy that is whole (the staging tool's ready marker, or the
in-job copy's `.kaos-complete`). An input that cannot be staged ends the job 232 before the step starts. Every input's
commit is a `kaos-commit=` word of the job comment, so the staging tool evicts none of them while the job queues or
runs. Not staged: a model or an artifact a needed step wrote; kaos-lib downloads those.

Still open: the copy is bound as its cache folder is, and both caches keep their own markers inside it (`.kaos-ready`,
`.kaos-used` in the staging tool's; `.kaos-complete`, `.kaos-parts/` in the in-job one), so the step sees those beside
the dataset's files. `tests/test_step_contract.py` holds the expectation as an expected failure.

**The result.** After a step that exited 0, the job script runs `payload/handback.py`: it uploads
`$KAOS_WORK/output/kaos-result.json`, unread, to `hpc-outputs/<workflow uid>/<step>/<site job id>/kaos-result.json` on the
hand-off branch `hpc-<workflow uid>-<step>` of `outputs-<app>`, or the object `kaos-result.absent` when the step wrote no
file. When it cannot (no token or endpoint, a file that is not a regular file, cannot be read or is over 64 KiB, lakeFS
refusing after three tries) it prints `result hand-back: <cause>` and the job ends 243. The script removes a result file
left in the work folder before the step starts (a requeued job runs again under the same id).

The runner (`fetch_result`) then fetches the object for its own site job id (four tries), checks it (`result.check`),
writes it again in wire form, and sends it as `status.result` in the same patch as the terminal report; it deletes the
branch. The record that the step wrote no result, neither object there, or lakeFS not answering: the attempt is
reported FAILED with exit 243, each with its own sentence. Not a result: FAILED with exit 244. The sentence is in `status.run.message` and becomes the job's message. A run that could never hand a
result back (no data-plane token, no lakeFS endpoint, no app) is reported FAILED 243 before anything is submitted.

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

## The stop signal on a shared node (AC-73)

A step alone is told to save by Slurm (`kill_warning_signal` USR1 to the batch shell, `lead` minutes before the wall
time). On a shared node the batch shell is the slot manager and a guest is only a `srun` step of the host's job, so
the job's signal cannot serve them: the job does not ask for it (`runner.py`), and `payload/slots.py` runs one timer
per step. A step's lead comes from `KAOS_RESUMABLE_LEAD_MIN` (its script, or the host's environment for its own step);
USR1 goes to the step's own shell (its pid, written by the launcher) at `signal_at()` = `min(start + limit, host job
end) - lead`, and the shell does what it does for a step alone (`kaos_stop`: copy now, USR1 only to processes that
handle it). `srun --signal` is not used: see `docs/extreme-cases.md` 4.14. Admission is `fits()`.

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
