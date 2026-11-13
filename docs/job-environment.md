# The job on the site: environment, folders, credentials (PRD kaos#1911 hardening)

What the runner's job script (`runner/kaos_hpc_runner/jobscript.py`) does to keep a job's credentials and files from
going further than they have to. Proven by `runner/tests/test_hardening.py`, which runs the whole script in bash against
a fake Apptainer and a fake Slurm.

## Layout on the site: one folder per platform project
One site project (one account, one scratch root `/project/scratch/<account>`, one Unix group) holds several platform
projects, and each platform project has its OWN site user. So nothing of a project sits directly under the shared scratch:
everything is under the project's own root, which is the job script's `SCR` (the HPCSite's `profile.scratchRoot`, set by the
platform to `<scratch>/kaos/<project>`; the runner reads it from `HPC_SCRATCH` and derives nothing).

```
/project/scratch/<account>/            site project's scratch: the group's, quota is measured here (lfs quota -u/-g/-p)
  kaos/                                SHARED parent: mode 3770 (setgid + sticky), group = the site project's group
    <project-A>/                       0700, owned by project A's site user  = SCR of A's jobs
      kaos-work/<job>/ (output/ ckpt/)   the job's work folder
      kaos-tmp/  in-<job> env-<job> idle-<job>   inputs, step variables, idle marker
      kaos-priv/host-<job>/              a shared-node host's private script copy (holds the job's credentials)
      kaos-claims/                       the slot manager's locks (node sharing)
      kaos-ckpt/<workflow uid>/<step>/   checkpoint copies
      sif-cache/  kaos-apptainer-cache/  data-cache/     image and in-job dataset caches
      images/  datasets/<repo>/<commit>/  locks/         made by the login-node staging tool (--root is this folder)
    <project-B>/                       0700, owned by project B's site user, the same tree
```

Who owns what: a project's root and everything under it belongs to that project's user and is closed to the group (0700 on
the root; the files the job script makes are 0600/0700). The one thing the group can read is the shared parent `kaos/`: it
lists the project names, which is what lets a user create and enter its own root. The tools never loosen anything below it.
A place for data one project would share with another, later, is the receiving project's own subtree
(`<project-B>/datasets/<repo>/<commit>/`): B stages its own copy; nothing is linked across users.

The shared parent `kaos/` is made by whoever comes first (a job script's `kaos_root`, or the staging tool's `ensure_root`)
with mode 3770 and the group of the site project's scratch. A user who finds it with any other mode gets ONE sentence and the
job ends 233 (the staging tool answers an `error` event): `The shared folder <path> has mode 755 and group <g>, but every
user of the site project must be able to create and enter it. The site owner must run once: chgrp <group> <path> && chmod
3770 <path>`. Nothing is ever chmod-ed or chgrp-ed silently on a folder somebody else made, and a `chmod` whose failure
matters is never muted.

**What the site owner sets up once** (recommended, instead of leaving it to the first job): as a site administrator, not
as a project user, `mkdir <scratch>/kaos && chgrp <group> <scratch>/kaos && chmod 3770 <scratch>/kaos`. Why: the owner of a
sticky folder can rename or remove anything in it, so when a project user's job made it, that user (and only that user)
could move another project's root away (shown by `hack/local-lab/two_users.py`). Owned by the site owner, no project user can.

**A site used before this change**: the folders `kaos-work`, `kaos-tmp`, `kaos-priv`, `kaos-ckpt`, `kaos-claims`,
`sif-cache`, `data-cache` and `kaos-apptainer-cache` directly under the site scratch are no longer used. Nothing migrates;
the staging tool's `clean` names them once ("old layout, not cleaned") and the site owner may remove them.

**Live checklist** (a site with two project users, A and B; login node unless said):
1. `ls -ld <scratch>/kaos`: mode `drwxrws--T` (3770), group = the site project's group, owned by the site owner.
2. A job of project A runs to the end; `ls -l <scratch>/kaos` lists `<project-A>` (0700, A's user).
3. A job of project B runs to the end (before this change it failed on `kaos-priv`/`kaos-claims`); `ls -l` now lists both.
4. As A's user: `ls <scratch>/kaos/<project-B>` and `cat` of a file in it are refused; as B's user the reverse.
5. `lfs quota -u <A's user> <scratch>`, `lfs quota -u <B's user> <scratch>` and `lfs quota -p <project id> <scratch>`:
   each user's figure matches its own tree; the project figure is the sum (one quota, shared).
6. Run the cleanup form of A (scope known-jobs): it removes A's old folders, B's are untouched and B's jobs keep working.

Proven without a site: `hack/local-lab/two-users.sh` runs two projects as two different Unix users (distinct uids, one shared
group) on one scratch in a throw-away container (the real job script, slot manager and checkpoint copier; with
`STAGER=<stager.py>` also image staging, `usage` and `clean`), and `runner/tests/test_two_users.py` runs it in the normal
suite where real uids can be had (root in a container, or podman/docker) and is skipped with the reason elsewhere. What only
a live site proves: the site's real group and ACL setup, Lustre quota per user and per project, lakeFS dataset downloads.

## The image reference
A step's image is parsed once (`settings.parse_image`, and the same grammar in `runnerjob.ValidateImage`):
`<host>/<repository>[:<tag>][@sha256:<digest>]`, lowercase, with a tag or a digest, and only the characters
`A-Za-z0-9._:/@-`. A reference that does not parse (a space, a control character, a quote, `$`, a backtick, `;`, userinfo)
is refused as `InvalidImage` by the controller before anything is booked, and by the runner before any network call;
`Runner.site_job` refuses again before it writes a script. The script quotes every use of the reference. The image stager
(`image.parse_image`) and the workdir lookup (`wallet.fetch_workdir`) read host, repository and tag or digest from that same
function; an empty tag before a digest (`host/app:@sha256:...`) is refused everywhere, in Go too.

The registry token is minted for Artifact Registry only. It is presented to a host that is exactly
`<one label>-docker.pkg.dev` (`settings.registry_token_host`; the script applies the same rule to the host it cuts from
the reference, not to a pattern over the whole reference). Any other host is pulled from anonymously, and the token is not
put in the job at all. The script unsets it (and Apptainer's copy of it) as soon as the image exists.

## What the step container receives
`apptainer exec --cleanenv`: the container sees `jobscript.STEP_ENV` (when the job has the variable) and nothing of the
job's environment.

| variable | why |
|---|---|
| `KAOS_STEP_CONTEXT` | the step's context |
| `LAKEFS_ENDPOINT`, `LAKEFS_BEARER_TOKEN` | the data store; kaos-lib reads the bearer from the environment only |
| `MLFLOW_TRACKING_URI`, `MLFLOW_TRACKING_TOKEN` | MLflow; its client reads the token from the environment only |
| `KAOS_LIB_SUPPORT` | the one variable a job's `spec.env` may hand over |
| `CUDA_VISIBLE_DEVICES`, `NVIDIA_VISIBLE_DEVICES` | GPU selection (renumbered by the script on a fenced shared node) |
| `KUBECORE_SLOT_GPUS`, `_CPUS`, `_MEM_GIB`, `_SHARED` | the slot's share of the node (`kaos_lib.runtime`) |
| `SLURM_JOB_ID`, `SLURM_STEP_ID`, `KAOS_STEP_GPUS` | the slot manager finds a step's processes in `/proc/<pid>/environ` by these (memory kill, GPU trespass watchdog) |
| `SLURM_CPUS_ON_NODE`, `SLURM_CPUS_PER_TASK`, `SLURM_GPUS_ON_NODE`, `SLURM_JOB_GPUS`, `SLURM_NTASKS`, `SLURM_MEM_PER_NODE`, `SLURM_JOB_NODELIST`, `SLURM_TMPDIR` | plain facts for sizing workers (see below for a shared node) |
| `http_proxy`, `https_proxy`, `HTTP_PROXY`, `HTTPS_PROXY`, `no_proxy`, `NO_PROXY` | the site's way out to the network |
| `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE` | the site's certificate authority |
| `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `LANG`, `LC_ALL`, `TZ`, `PYTHONUNBUFFERED`, `XDG_CACHE_HOME`, `NVIDIA_DRIVER_CAPABILITIES`, `GPU_DEVICE_ORDINAL` | settings a job sets for its programs |
| `KAOS_STAGED_ROOT`, `KAOS_CHECKPOINT_DIR`, `KAOS_RESUME_PATH` | set by the script when there are inputs, a checkpoint folder, a continuation |

Not passed, on purpose: `HOME` (Apptainer gives the container its own `HOME`, the site user's home folder, bound in; the
host's value would only repeat it), `USER`, `PATH` (the image's own), `LD_LIBRARY_PATH` and `LD_PRELOAD` (they would point
the image's loader at host libraries; `--nv` binds the driver's), the registry token, the checkpoint/hand-back/stage-in
programs, the idle watchdog, the job's own settings and every other variable.

On a shared node the step is not given the host job's Slurm figures (srun hands every step the host job's variables).
`SLURM_CPUS_PER_TASK` and `SLURM_CPUS_ON_NODE` are the slot's cores, `SLURM_MEM_PER_NODE` the slot's memory in MB,
`SLURM_NTASKS` is 1, `SLURM_JOB_GPUS` and `SLURM_GPUS_ON_NODE` are the step's own GPU set; a figure the slot manager did
not give is left out, never replaced by the host's. A job alone on its node gets Slurm's own values.

### How a value gets into the container
Apptainer 1.1.0 and later evaluate an `APPTAINERENV_<name>` value (and an `--env` or `--env-file` value) in the
container's startup shell unless `--no-eval` is given (`--no-eval` appeared in 1.1.0; the Apptainer user guide, "Escaping
and Evaluation of Environment Variables"): `$(...)`, backticks, `$VAR` and backslashes in a value are run or changed. A
step's context carries run parameters, so the script does not hand the dangerous values to Apptainer at all, and does not
depend on the flag or on the site's Apptainer version:

* the context, the two bearers and `KAOS_LIB_SUPPORT` always travel as files, byte for byte;
* any other value travels as `APPTAINERENV_<name>` only when every character is one of `A-Za-z0-9_.,:/+=@%[]-`, which
  evaluation leaves as it is; a value with any other character (a proxy with `$` in the password, a path with a space)
  travels as a file too;
* the files are written with `printf %s` into `$SCR/kaos-tmp/env-<job>/` (folder 0700, one file per variable, 0600), the
  folder is bound read-only at `/kaos/env`, and a small loader in the container's own `/bin/sh` reads each file and exports
  it before the step command runs (`$(cat file; printf x)`, so a trailing newline survives);
* the folder is removed when the step ends and by the job's exit trap; the sweep below removes one a killed job left;
* a value that cannot be written ends the step with exit 233 before it starts.

So for as long as the step runs the two bearers are in a 0600 file of a 0700 folder as well as in the container's
environment (the library reads them from the environment only; nothing the runner writes holds them after the step).

The tests (`runner/tests/test_hardening.py`) run the whole script against a fake Apptainer that models the evaluation, and
prove a 100 KB context holding `$HOME $(...)`, backticks, `\` and `"` arrives byte for byte and runs nothing.

## Files
* `umask 077` at the top of every script, and in every program the script starts: files are 0600, folders 0700.
* The job's work folder `$SCR/kaos-work/<job>` (and `output/`, `ckpt/`), the checkpoint root, the SIF cache, the Apptainer
  cache and temporary folder, the data cache and the staged-image folder are made 0700 on every job (folders that an older
  job or the login-node staging tool left open are closed; a `chmod` that fails ends the job 233 naming the folder). They are
  all under `$SCR`, the project's own root (0700): the caches are shared between the jobs of that project's site user on
  purpose; no other user, including another project's, can enter them.
* A shared-node host copies its own script for the slot manager to run (a host reads a script, never an environment, so the
  copy holds the job's credentials). The copy and the slot-manager program live in `$SCR/kaos-priv/host-<job>/` (0700,
  files 0600), not in `/tmp`, and the folder is removed on every way out of the script: end, `fail()`, TERM/HUP/INT.
  A guest's script copy in the claim folder is removed when its step ends or is handed back.
* A failed job keeps its work folder; it holds no credential file. It is private to the site user (0700): only that
  account, which the platform's staging tool uses, can read it.
* A job killed without a trap (SIGKILL, node loss) leaves `kaos-priv/host-<job>` (the bearer), `kaos-tmp/in-<job>` and
  `kaos-tmp/env-<job>`. Every job script starts with `kaos_sweep "$SCR"` (`jobscript.SWEEP_HELPER`, self-contained shell):
  it removes those whose Slurm job is no longer queued or running. It asks `squeue -u $USER`; when squeue cannot answer, or
  its answer lacks the asking job itself, it removes nothing. Only folders named exactly `host-<digits>`, `in-<digits>`,
  `env-<digits>` directly under `<root>/kaos-priv` and `<root>/kaos-tmp`, owned by the caller, older than
  `KAOS_SWEEP_MIN_AGE_MIN` (10) minutes; a link is never followed; an empty, relative or missing root does nothing. Any
  other caller (a cleanup workflow, the login-node staging tool) can run the same function: define it from
  `SWEEP_HELPER`, call `kaos_sweep <project scratch root>`.

## Exit codes of a host job
The script ends every TERM, HUP and INT with exit 143 (a cancel, the job's time limit), so the Slurm exit code of a
shared-node host job after a TERM is always 143 whatever its step did. The runner does not read that code: the step's own
result is in the job comment (`kaos-state=done rc=...`, written by the slot manager), and that is what the runner reports
(`test_runner.py::test_a_host_job_ended_by_term_is_judged_by_its_comment_not_by_its_exit_code`).

## What a failed job shows
The last 700 bytes of the job's output go into the job comment after `redact.py` (shipped to the node as `REDACT_B64`)
replaced credentials. The same program redacts every line of the live log stream before the runner prints it. What it
replaces, by shape or by exact value and never by "the line has the word key or token in it":

* the exact values of the credentials the job holds (the data and MLflow bearer) and their base64, URL-safe base64, Basic
  (`oauth2accesstoken:<token>`) and percent-encoded forms;
* `Bearer`/`Basic` values of 16 or more characters, the value after `Authorization:`, `Proxy-Authorization:`,
  `X-...-Key/Token/Secret:`, `Api-Key:`, a `Cookie:` header;
* JWTs (`eyJ...`), Google access tokens (`ya29.`, which is also the registry token's shape), GitHub tokens (`ghp_`,
  `github_pat_`...), AWS access key ids (`AKIA...`), the password of `scheme://user:password@host`;
* the value of `NAME_TOKEN=`, `NAME_SECRET=`, `NAME_PASSWORD=`, `NAME_API_KEY=` (upper case) and of the exact words
  `password`, `secret`, `token`, `api_key`, `access_token`, `refresh_token`, `client_secret`, `private_key` in `k=v`, JSON,
  a Python repr, escaped JSON and a query string;
* a private key block (PEM or PGP), also one written as a single JSON string. A block is bounded: it ends at its `END` line
  or at the first line that is not key material, never after 100 lines, and a marker in the middle of prose starts none, so
  a stray `-----BEGIN ... PRIVATE KEY-----` never silences the log stream or the GPU metric lines;
* the held bearers also in hex and with JSON-escaped slashes, signed-URL `X-Goog-Signature=`/`X-Amz-Signature=` values,
  `--password`/`--token` command-line values, `aws_secret_access_key`.

The value after a secret's name is redacted only when it looks like a secret (16 or more characters, or 6 or more with
letters and digits mixed; never a plain word, a number, a path or a boolean). A credential split over two lines is not
recognised.

`max_new_tokens=512`, `tokenizer=...`, `num_keypoints=17`, `monkey=3`, `"primary_key": "id"` and "The Bearer token expired"
are left alone. Every pattern is linear in the length of the line. The redaction runs on the last 64 KiB of the output
before the last 700 bytes are cut, so a token or a key block cut by the small window is still redacted whole. A node with
no `python3` shows none of the output (`[output withheld ...]`): unknown never means show it.

## What this does not do
* Jobs of one project run as ONE site user. The site sees one user, so no mode separates two jobs of that user from each
  other, and a job can read the folders and the scratch of its project's other jobs. Separating them needs an account per
  app on the site. Separate PROJECTS are separated: each has its own site user and its own root (see Layout above).
* On a shared node the credentials of a waiting guest travel in its script as `export` lines: a host can read a waiting
  job's script, never its environment (PRD-1609 probe), and the runner has no channel that puts a file on the site. They
  are in Slurm's own record of the script, in the host's private copy (removed as above), and in the guest's step
  environment. A file channel needs a verb in the login-node staging tool (not in this repository).
* The login-node staging tool creates the dataset and image folders; the job script closes their modes on every job,
  but the tool should create them 0700 itself.
