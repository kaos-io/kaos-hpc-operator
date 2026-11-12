# The job on the site: environment, folders, credentials (PRD kaos#1911 hardening)

What the runner's job script (`runner/kaos_hpc_runner/jobscript.py`) does to keep a job's credentials and files from
going further than they have to. Proven by `runner/tests/test_hardening.py`, which runs the whole script in bash against
a fake Apptainer and a fake Slurm.

## The image reference
A step's image is parsed once (`settings.parse_image`, and the same grammar in `runnerjob.ValidateImage`):
`<host>/<repository>[:<tag>][@sha256:<digest>]`, lowercase, with a tag or a digest, and only the characters
`A-Za-z0-9._:/@-`. A reference that does not parse (a space, a control character, a quote, `$`, a backtick, `;`, userinfo)
is refused as `InvalidImage` by the controller before anything is booked, and by the runner before any network call;
`Runner.site_job` refuses again before it writes a script. The script quotes every use of the reference.

The registry token is minted for Artifact Registry only. It is presented to a host that is exactly
`<one label>-docker.pkg.dev` (`settings.registry_token_host`; the script applies the same rule to the host it cuts from
the reference, not to a pattern over the whole reference). Any other host is pulled from anonymously, and the token is not
put in the job at all. The script unsets it (and Apptainer's copy of it) as soon as the image exists.

## What the step container receives
`apptainer exec --cleanenv`: the container sees `jobscript.STEP_ENV` and nothing of the job's environment.

| variable | why |
|---|---|
| `KAOS_STEP_CONTEXT` | the step's context |
| `LAKEFS_ENDPOINT`, `LAKEFS_BEARER_TOKEN` | the data store; kaos-lib reads the bearer from the environment only |
| `MLFLOW_TRACKING_URI`, `MLFLOW_TRACKING_TOKEN` | MLflow; its client reads the token from the environment only |
| `KAOS_LIB_SUPPORT` | the one variable a job's `spec.env` may hand over |
| `CUDA_VISIBLE_DEVICES`, `NVIDIA_VISIBLE_DEVICES` | GPU selection (renumbered by the script on a fenced shared node) |
| `KUBECORE_SLOT_GPUS`, `_CPUS`, `_MEM_GIB`, `_SHARED` | the slot's share of the node (`kaos_lib.runtime`) |
| `SLURM_JOB_ID`, `SLURM_STEP_ID`, `KAOS_STEP_GPUS` | the slot manager finds a step's processes in `/proc/<pid>/environ` by these (memory kill, GPU trespass watchdog) |
| `SLURM_CPUS_ON_NODE` | plain fact for sizing workers |
| `KAOS_STAGED_ROOT`, `KAOS_CHECKPOINT_DIR`, `KAOS_RESUME_PATH` | set by the script when there are inputs, a checkpoint folder, a continuation |

The registry token, the checkpoint/hand-back/stage-in programs, the idle watchdog, the job's own settings and every other
variable are not passed. The two bearers (`LAKEFS_BEARER_TOKEN`, `MLFLOW_TRACKING_TOKEN`) are the only credentials in the
container's environment, because the library cannot read either from a file; they are in no file the runner writes.

## Files
* `umask 077` at the top of every script, and in every program the script starts: files are 0600, folders 0700.
* The job's work folder `$SCR/kaos-work/<job>` (and `output/`, `ckpt/`), the checkpoint root, the SIF cache, the Apptainer
  cache and temporary folder, the data cache and the staged-image folder are made 0700 on every job (folders that an older
  job or the login-node staging tool left open are closed). The caches are shared between the jobs of the site user on
  purpose; nobody else can enter them.
* A shared-node host copies its own script for the slot manager to run (a host reads a script, never an environment, so the
  copy holds the job's credentials). The copy and the slot-manager program live in `$SCR/kaos-priv/host-<job>/` (0700,
  files 0600), not in `/tmp`, and the folder is removed on every way out of the script: end, `fail()`, TERM/HUP/INT.
  A guest's script copy in the claim folder is removed when its step ends or is handed back.
* A failed job keeps its work folder (the developer needs it); it holds no credential file.

## What a failed job shows
The last 700 bytes of the job's output go into the job comment after `kaos_redact` replaced Bearer/Basic values, `ya29.`
access tokens, JWTs (`eyJ...`), and the value of any `...key=`, `...token=`, `...password=`, `...secret=` word or JSON
member. The redaction runs on the last 4 KiB, before the last 700 bytes are cut, so a token cut by the window is still
redacted whole.

## What this does not do
* Jobs of one project run as ONE site user. The site sees one user, so no mode separates two jobs of that user from each
  other, and a job can read the folders and the scratch of its project's other jobs. Separating them needs an account per
  app on the site, or the operator refusing to bind one site user to two projects (not in this repository).
* On a shared node the credentials of a waiting guest travel in its script as `export` lines: a host can read a waiting
  job's script, never its environment (PRD-1609 probe), and the runner has no channel that puts a file on the site. They
  are in Slurm's own record of the script, in the host's private copy (removed as above), and in the guest's step
  environment. A file channel needs a verb in the login-node staging tool (not in this repository).
* The login-node staging tool creates the dataset and image folders; the job script closes their modes on every job,
  but the tool should create them 0700 itself.
