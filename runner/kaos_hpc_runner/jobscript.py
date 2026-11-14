"""The Slurm batch script of one attempt, as the in-pod waiter built it (PRD-HPC-1461 D-64).

The script is the proven program: Lmod init, SIF cache (private part-file, atomic rename),
input binds, the result hand-back, USR1 pass-through to handlers only, failure tail into the
job comment, the PRD-1609 host hook. D-72 idle watchdog (payload/idlewatch.py) ends a hung step with exit 236. Phase-1 additions: a 30 s sampler writing ``KAOS_METRIC``
lines into the job output (D-65) and deletion of the staged image tar after a successful SIF
build (D-62).

`batch_lines()` returns the script lines; they are joined with newlines by the caller.

PRD kaos#1911 hardening: every script and file the job writes is private to the site user (umask 077, folders 0700);
the step runs with `--cleanenv` and receives only STEP_ENV (below); the registry token is never in the job's environment or
script: it is a file the platform's staging tool places (REG_DIR), read by the pull alone, presented only to a host parsed
out of the reference, and removed as soon as the image is there; the job's private folder (the script copy a
shared-node host keeps) is removed on every way out of the script; the failure tail copied into the job comment is
redacted first. One limit stays: jobs of one project run as ONE site user, and the site sees one user, so no folder
mode separates two of them from each other (see docs/job-environment.md).
"""

from .settings import CONTEXT_ENV

# What the step container receives, and nothing else (Apptainer --cleanenv; see "How a value reaches the step").
#  - the step context, the data store's endpoint, the MLflow endpoint and bearer: kaos-lib reads the tracker's bearer from
#    the ENVIRONMENT only (mlflow.py MLFLOW_TRACKING_TOKEN), so that one credential is in the container's environment and
#    nowhere else. The data store has NO credential in the environment: the app's own key is a FILE (KEY_MOUNT), set by
#    the script itself as APPTAINERENV_LAKEFS_KEY_FILE, which the platform replaces before the key ends;
#  - KAOS_LIB_SUPPORT (the one variable a job's spec.env may hand over);
#  - the GPU selection and the slot's share of the node (kaos_lib.runtime reads KUBECORE_SLOT_*);
#  - SLURM_JOB_ID, SLURM_STEP_ID, KAOS_STEP_GPUS: the slot manager finds a step's processes (memory use, GPU
#    trespass) by these in /proc/<pid>/environ, so without them inside the container the watchdogs go blind;
#  - plain facts apps use to size themselves and to reach the network, passed when the job has them (PASS_ENV).
# KAOS_STAGED_ROOT, KAOS_CHECKPOINT_DIR and KAOS_RESUME_PATH are set by the script itself as APPTAINERENV_*.
# NOT passed, on purpose: HOME (Apptainer gives the container its own HOME, the user's home folder, bound in), USER,
# PATH (the image's own), LD_LIBRARY_PATH (it would point the image's loader at host libraries; --nv binds the driver's),
# and every secret of the job script (the stage-in/hand-back programs, the job's settings). The registry token is not in
# the job at all: it is a file (REG_DIR) that only the image pull reads.
PASS_ENV = ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'no_proxy', 'NO_PROXY',
            'SSL_CERT_FILE', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE',
            'SLURM_CPUS_PER_TASK', 'SLURM_GPUS_ON_NODE', 'SLURM_JOB_GPUS', 'SLURM_NTASKS', 'SLURM_MEM_PER_NODE',
            'SLURM_JOB_NODELIST', 'SLURM_TMPDIR',
            'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'LANG', 'LC_ALL', 'TZ', 'PYTHONUNBUFFERED', 'XDG_CACHE_HOME',
            'NVIDIA_DRIVER_CAPABILITIES', 'GPU_DEVICE_ORDINAL')
STEP_ENV = (CONTEXT_ENV, 'LAKEFS_ENDPOINT', 'MLFLOW_TRACKING_URI', 'MLFLOW_TRACKING_TOKEN',
            'KAOS_LIB_SUPPORT', 'CUDA_VISIBLE_DEVICES', 'NVIDIA_VISIBLE_DEVICES', 'KUBECORE_SLOT_GPUS',
            'KUBECORE_SLOT_CPUS', 'KUBECORE_SLOT_MEM_GIB', 'KUBECORE_SLOT_SHARED', 'KAOS_STEP_GPUS',
            'SLURM_JOB_ID', 'SLURM_STEP_ID', 'SLURM_CPUS_ON_NODE') + PASS_ENV

# How a value reaches the step (Apptainer 1.1.0 and later evaluate APPTAINERENV_<name> values in the container's startup
# shell unless --no-eval is given: `$(...)`, backticks, `$VAR` and backslashes in a value are run or changed; --no-eval
# exists since 1.1.0, and --env-file values are evaluated the same way). The script does not depend on either flag or on
# the site's Apptainer version:
#  - these three values always travel as files, byte for byte: the context (run parameters, any character), the
#    tracker's bearer, and spec.env's KAOS_LIB_SUPPORT;
#  - any other value travels as APPTAINERENV_<name> ONLY when it is made of characters that evaluation leaves as they are
#    (PLAIN_CHARS); one that is not travels as a file too;
#  - a file holds one value, written with printf %s in a private folder (0700, file 0600) bound read-only at /kaos/env;
#    a small loader in the container's own shell reads each file and exports the value before the step command runs.
FILE_ENV = (CONTEXT_ENV, 'MLFLOW_TRACKING_TOKEN', 'KAOS_LIB_SUPPORT')
PLAIN_CHARS = ']A-Za-z0-9_.,:/+=@%[-'      # as a bash `[!...]` set; the `]` is first and the `-` last on purpose
ENV_MOUNT = '/kaos/env'
# The app's key file for the data store: a folder of its own (NOT the folder above: its loader exports every file there as a
# variable), written by the runner through the staging tool, 0700/0600, replaced atomically before the key ends. Bound
# read-only; the step finds the file by LAKEFS_KEY_FILE.
KEY_MOUNT = '/kaos/key'
KEY_FILE_NAME = 'lakefs-key.json'
# The registry's auth file for the image pull (a Docker config), placed by the staging tool's `registry` verb in a folder of its
# own beside the key folder (0700/0600), read by `apptainer pull --authfile`, and removed as soon as the image is there. Never
# bound into the step. The job knows only that one is coming (KAOS_REG_AUTH=1), never its content.
REG_FILE_NAME = 'auth.json'
# POSIX sh, run by the image's /bin/sh. `$(cat f; printf x)` keeps trailing newlines, which `$(cat f)` would drop.
LOADER = ('for f in ' + ENV_MOUNT + '/*; do [ -f "$f" ] || continue; v=$(cat "$f"; printf x); export "${f##*/}=${v%x}"; done;'
          ' unset f v;')

# Credentials are replaced before a line of job output is copied anywhere (the job comment, which the controller and the
# console read). One implementation, redact.py, shipped to the node as REDACT_B64 (and used by the runner's log stream).
# Without python3 or without the program the output is NOT shown: unknown never means "show it".
REDACT_HELPER = [
    'kaos_redact(){ if [ -n "$REDACT_B64" ] && command -v python3 >/dev/null 2>&1; then',
    '  python3 -c "$(printf %s "$REDACT_B64" | base64 -d)" || echo "[output withheld: the redaction program failed]";',
    '  else echo "[output withheld: no redaction program on this node]"; fi; }',
]

# Left behind by a job that was killed (SIGKILL, node loss) before its traps could run: the shared-node host's private
# copy of its script (holds the job's bearer), the folder inputs are presented in, the folder the step's variables are
# written to. `kaos_sweep <project scratch root>` removes those whose Slurm job is no longer queued or running.
# Small and self-contained so the login-node stager and manual cleanups can run the same text. Rules: it removes nothing
# unless squeue answered (unknown never means delete). It lists EVERY job of the user that is not finished: `-a` includes
# hidden partitions and `-t all` replaces squeue's default of only pending, running and completing jobs (Slurm squeue
# manual: "-t: If no state is specified then pending, running, and completing jobs are reported"; "-a: ... partitions that
# are configured as hidden"); the states that mean finished are filtered out here, so a suspended, requeued or otherwise
# unusual live job keeps its folders. An empty answer is believed only when KAOS_SWEEP_SELF names a job that must be in it
# (a caller that is itself a job; set but empty = do nothing) or the caller says KAOS_SWEEP_ALLOW_EMPTY=1 after its own
# queue check. Only folders named exactly host-<digits> / in-<digits> / env-<digits> / key-<digits> / reg-<digits> directly under <root>/kaos-priv and
# <root>/kaos-tmp that belong to the caller and are older than KAOS_SWEEP_MIN_AGE_MIN (default 10) are removed; a link is
# never followed; an empty or root path does nothing.
SWEEP_HELPER = [
    'kaos_sweep(){',
    '  local root=$1 live d id kind',
    '  case "$root" in /?*) ;; *) return 0;; esac',
    '  [ -d "$root" ] && [ ! -L "$root" ] || return 0',
    '  [ -z "${KAOS_SWEEP_SELF+x}" ] || [ -n "$KAOS_SWEEP_SELF" ] || return 0',
    '  live=$(squeue -a -h -u "$USER" -t all -o "%A %T" 2>/dev/null) || return 0',
    '  live=$(printf "%s\n" "$live" | awk \'NF && $2 !~ /^(COMPLETED|CANCELLED|FAILED|TIMEOUT|NODE_FAIL|PREEMPTED|BOOT_FAIL'
    '|DEADLINE|OUT_OF_MEMORY)$/ {print $1}\')',
    '  if [ -n "${KAOS_SWEEP_SELF:-}" ]; then printf "%s\n" "$live" | grep -qx "$KAOS_SWEEP_SELF" || return 0;',
    '  elif [ -z "$live" ] && [ "${KAOS_SWEEP_ALLOW_EMPTY:-0}" != 1 ]; then return 0; fi',
    '  for kind in kaos-priv/host kaos-tmp/in kaos-tmp/env kaos-tmp/key kaos-tmp/reg; do',
    '    [ -d "$root/${kind%/*}" ] && [ ! -L "$root/${kind%/*}" ] || continue',
    '    find "$root/${kind%/*}" -mindepth 1 -maxdepth 1 -type d -name "${kind#*/}-[0-9]*" -user "$USER"'
    ' -mmin +"${KAOS_SWEEP_MIN_AGE_MIN:-10}" 2>/dev/null | while read -r d; do',
    '      id=${d##*/}; id=${id#*-}',
    '      case "$id" in ""|*[!0-9]*) continue;; esac',
    '      printf "%s\n" "$live" | grep -qx "$id" || rm -rf "$d"',
    '    done',
    '  done',
    '  return 0',
    '}',
]


# The project's own root on scratch (SCR). One site project (one account, one scratch, one Unix group) holds several platform
# projects, each as its own site user, so nothing of a project sits directly under the shared scratch: everything is under
# SCR = <scratch>/kaos/<project>, which only this project's user can enter (0700). A JOB NEVER CREATES that folder or the shared
# folder <scratch>/kaos above it: the platform prepares them as the project's own user when the project is connected to the
# site (and at every refresh of the connection; the stager's `prepare` verb, kubecore-operator images/meluxina-tokenmint). The
# job only VERIFIES, and changes nothing:
#  * SCR must be <something>/kaos/<name>: anything else (an operator older than this runner publishes the bare scratch) fails
#    with a sentence saying the platform has not published a per-project folder for this job yet;
#  * `kaos` and SCR must be real folders (never links), SCR must be owned by the caller and have no group or other bits (0700);
#  * otherwise the job ends 233 with ONE sentence saying the project's connection has not prepared its folder, and why.
# Sub-folders of SCR (work, tmp, caches, checkpoints, claims) are created by the job as before. Nothing above `kaos/` is named.
ROOT_HELPER = r"""kaos_root(){
  local root=$1 parent name site me m why
  parent=${root%/*}; name=${root##*/}; site=${parent%/*}; me=${USER:-$(id -un)}
  case "$root" in /*) ;; *) parent=;; esac
  if [ "${parent##*/}" != kaos ] || [ -z "$site" ] || [ -z "$name" ]; then
    echo "The platform has not published a per-project folder for this job yet (operator older than this runner); nothing was changed."
    return 1
  fi
  why=
  if [ -L "$parent" ]; then why="the shared folder $parent is a symbolic link"
  elif [ -L "$root" ]; then why="it is a symbolic link"
  elif [ ! -e "$root" ]; then why="it does not exist"
  elif [ ! -d "$root" ]; then why="it is not a folder"
  elif [ ! -O "$root" ]; then why="it is owned by $(stat -c %U "$root" 2>/dev/null), not by $me"
  else
    m=$(stat -c %a "$root" 2>/dev/null)
    [ $((8#${m:-777} & 8#077)) -eq 0 ] || why="it has mode $m and must be private (0700)"
  fi
  if [ -n "$why" ]; then
    echo "The project's connection to the site has not prepared its folder $root ($why); nothing was changed. The platform prepares it when the connection is refreshed and shows the reason on the project."
    return 1
  fi
  return 0
}""".split("\n")


# The ONLY code that writes this job's comment from the node (fail(), the checkpoint copier). It never
# overwrites blindly: it reads the live comment, drops the words being set and any old err tail, appends the
# new words (and the err text), writes, and reads back that every new word is there, up to 3 tries. Every
# other word (kaos-commit=, kaos-image=, kaos-ckpt-lease=, kaos-ckpt*=, kaos-state=...) is kept. When the
# comment cannot be read the dataset leases from the environment (KAOS_LEASES) are still kept. Returns 1 when a word could
# not be verified: the caller (the final copy) then fails loudly instead of leaving the controller blind.
COMMENT_HELPER = [
    'kaos_comment(){',
    '  local err="" cur new="" t k w try got drop ok toks',
    '  if [ "$1" = "-e" ]; then err=$2; shift 2; fi',
    '  for try in 1 2 3; do',
    '    cur=$(squeue -h -j "$KAOS_JOB_ID" -o %k 2>/dev/null); [ "$cur" = "(null)" ] && cur=""',
    '    [ -z "$cur" ] && [ -n "$KAOS_LEASES" ] && cur="$KAOS_LEASES"',
    '    cur=$(printf %s "$cur" | sed -E "s/(^| )err[0-9]+:.*$//")',
    '    read -r -a toks <<< "$cur"; new=""',
    '    for t in "${toks[@]}"; do',
    '      k=${t%%=*}; drop=0',
    '      for w in "$@"; do [ "${w%%=*}" = "$k" ] && drop=1; done',
    '      [ $drop = 0 ] && new="${new:+$new }$t"',
    '    done',
    '    for w in "$@"; do new="${new:+$new }$w"; done',
    '    [ -n "$err" ] && new="${new:+$new }$err"',
    '    scontrol update JobId=$KAOS_JOB_ID "Comment=$new" 2>/dev/null',
    '    [ $# -eq 0 ] && return 0',
    '    got=" $(squeue -h -j "$KAOS_JOB_ID" -o %k 2>/dev/null) "; ok=1',
    '    for w in "$@"; do case "$got" in *" $w "*) ;; *) ok=0;; esac; done',
    '    [ $ok = 1 ] && return 0',
    '    sleep ${KAOS_COMMENT_RETRY_SLEEP:-2}',
    '  done',
    '  return 1',
    '}',
]


def batch_lines(scope, scratch):
    """scope: the sharing scope ('-' when sharing is off); scratch: the project's scratch root."""
    return [
        '#!/bin/bash -l',
        # PRD-1609 F-01: the script's format and sharing scope ('-': sharing off).
        # A host takes only jobs whose header carries its own scope.
        '# kaos-job v2 scope=' + scope,
        'set +e',
        # Every file and folder this job creates is private to the site user (also what the programs it starts create).
        'umask 077',
        # The job this step belongs to: its own on its own node; on a shared node
        # the host sets it to this step's job. Every per-job path keys on it.
        # Exported: the checkpoint copier (a child python3) writes the job comment for THIS job id (live job
        # 5433318: unexported, it saw an empty id and every comment update failed silently).
        'export KAOS_JOB_ID=${KAOS_JOB_ID:-$SLURM_JOB_ID}',
        # The one writer of the job comment (see COMMENT_HELPER), exported for the copier.
        *COMMENT_HELPER,
        'export -f kaos_comment',
        *REDACT_HELPER,
        *SWEEP_HELPER,
        *ROOT_HELPER,
        'SCR=' + scratch,
        # The job's private folder, only a shared-node host makes one. It is removed on every way out of this
        # script: the end, fail(), a signal (Slurm's TERM at cancel or at the time limit).
        'kaos_cleanup(){ [ -n "$KAOS_PRIV" ] && rm -rf "$KAOS_PRIV"; [ -n "$KAOS_IN_ROOT" ] && rm -rf "$KAOS_IN_ROOT";'
        ' [ -n "$KAOS_ENV_DIR" ] && rm -rf "$KAOS_ENV_DIR";'
        ' [ -n "$KAOS_KEY_DIR" ] && rm -rf "$KAOS_KEY_DIR"; [ -n "$KAOS_REG_DIR" ] && rm -rf "$KAOS_REG_DIR"; return 0; }',
        'trap kaos_cleanup EXIT',
        # TERM/HUP/INT end the script with 143 whatever it was doing (a host job's Slurm exit code after a cancel or
        # the time limit is therefore always 143): the runner never reads a host job's exit code, it reads the step's
        # result from the job comment (kaos-state=done rc=...).
        'trap "exit 143" TERM HUP INT',
        # What a job killed without a trap (SIGKILL, node loss) left behind: the private copy of a host script with
        # the bearer in it, the input and variable folders of jobs that are over. Asks squeue; if it cannot
        # answer, removes nothing. A job's own id must be in the answer or the answer is not trusted.
        # Diagnostics without SSH (live job 5143859: exit 231 with no way to
        # see WHY): on any failure, tail this job's own output file into its
        # job COMMENT — readable over REST from the waiter and by operators.
        # The dataset lease (kaos-commit=, PRD-1541 D-06) stays first.
        'fail(){ KAOS_NOTE=$(cat "$KAOS_WORK/.kaos-note" 2>/dev/null);'
        ' kaos_comment -e "err$1:$(tail -c 65536 slurm-$KAOS_JOB_ID.out 2>/dev/null | kaos_redact | tail -c 700 | tr "\\n" "|")"'
        ' $KAOS_NOTE; exit $1; }',
        # The project's own root is VERIFIED, never made here (see ROOT_HELPER): a refusal is one plain sentence in the job
        # output, which fail() copies into the job comment. Before anything is created under $SCR.
        'kaos_root "$SCR" || fail 233',
        # Nothing is read from $SCR/kaos-tmp (the sweep, the key folder, the inputs) before it is known to be the job user's
        # own real folder: a link or a folder of another user there ends the job, and nothing under it is read.
        'if [ -e "$SCR/kaos-tmp" ] || [ -L "$SCR/kaos-tmp" ]; then { [ -O "$SCR/kaos-tmp" ] && [ ! -L "$SCR/kaos-tmp" ]; } ||'
        ' { echo "AppIdentityUnavailable: $SCR/kaos-tmp is not the job user\'s own folder (or is a link): nothing is read from it"; fail 247; }; fi',
        '[ -z "$SLURM_JOB_ID" ] || KAOS_SWEEP_SELF=$SLURM_JOB_ID kaos_sweep "$SCR"',
        # PRD-1609 F-03: with sharing on, the job's batch step is the host. It runs
        # this same script as one Slurm job step per slot (its own step and taken
        # guests' scripts), each with KAOS_IN_STEP=1 and that step's KAOS_JOB_ID.
        'if [ -z "$KAOS_IN_STEP" ] && [ -n "$KAOS_SLOTS_B64" ]; then',
        # The copy of this script holds the job's credentials (a host reads a script, never an environment), so it
        # lives in a folder only this user can enter, is not left in /tmp, and goes with kaos_cleanup (no exec: the
        # shell stays to run the trap).
        '  KAOS_PRIV=$SCR/kaos-priv/host-$SLURM_JOB_ID',
        '  mkdir -p "$SCR/kaos-priv" && chmod 700 "$SCR/kaos-priv" && rm -rf "$KAOS_PRIV" && mkdir "$KAOS_PRIV" || fail 233',
        '  printf %s "$KAOS_SLOTS_B64" | base64 -d > "$KAOS_PRIV/slots.py"',
        '  scontrol write batch_script $SLURM_JOB_ID "$KAOS_PRIV/job.sh" >/dev/null || fail 233',
        '  chmod 600 "$KAOS_PRIV/job.sh" "$KAOS_PRIV/slots.py"',
        '  python3 "$KAOS_PRIV/slots.py" "$KAOS_PRIV/job.sh"; exit $?',
        'fi',
        'for f in /usr/share/lmod/lmod/init/bash /etc/profile.d/lmod.sh; do'
        ' [ -r "$f" ] && source "$f" && break; done',
        'module load Apptainer 2>/dev/null || module load apptainer 2>/dev/null',
        'command -v apptainer >/dev/null || fail 210',
        'export APPTAINER_CACHEDIR=$SCR/kaos-apptainer-cache'
        ' APPTAINER_TMPDIR=$SCR/kaos-tmp',
        'mkdir -p $APPTAINER_CACHEDIR $APPTAINER_TMPDIR $SCR/sif-cache $SCR/kaos-work $SCR/data-cache',
        # The caches are shared between the jobs of this user on purpose, so they are readable by this user only. The
        # staged-image folder and the data cache are made by the login-node staging tool (not this repository), so
        # their modes are set here, on every job, to what a private cache needs.
        # A chmod that fails says which folder (a folder another user made is never taken silently).
        'for d in "$SCR/kaos-apptainer-cache" "$SCR/kaos-tmp" "$SCR/sif-cache" "$SCR/kaos-work" "$SCR/data-cache"'
        ' ${IMAGE_STAGED:+"${IMAGE_STAGED%/*}"}; do',
        '  [ -d "$d" ] || continue',
        '  chmod 700 "$d" || { echo "The folder $d must be private to the project user and cannot be made so (it belongs to"'
        ' "another user, or the site refuses the change)."; fail 233; }',
        'done; unset d',
        'KEY=$(printf %s "$IMAGE_REF" | sha256sum | cut -c1-16)',
        'SIF=$SCR/sif-cache/$KEY.sif',
        # The step's inputs (spec.inputs), each a whole dataset commit, presented to the step under one root,
        # $KAOS_STAGED_ROOT (/kaos/inputs in the container, read-only): the folder <input>/ holds the dataset's
        # files and nothing else (both kinds of copy keep them under <copy>/data/, beside the files of the platform,
        # and that folder is what is bound), and beside it the record <input>.kaos-ready ({"repo","commit","owner","full":true}) says which repository
        # and commit it is and that it is the whole tree. kaos-lib finds an input by that record. The root is
        # a folder of this job; each copy is bound onto its folder there, whichever way it was staged.
        # KAOS_INPUTS holds one word per input: name|repository|commit|resident path|owner (the data/ folder of the
        # copy staged from the login node, whose markers are beside it).
        # The app's key for the data store: a file the runner places through the staging tool (and replaces before the key
        # ends). The job never starts a step, a stage-in or a hand-back without it, and a job whose key never came ends 247
        # saying so. The file is in the job's own folder and is never copied into this script, its environment or its log.
        'KAOS_KEY_DIR=$SCR/kaos-tmp/key-$KAOS_JOB_ID; export LAKEFS_KEY_FILE=$KAOS_KEY_DIR/' + KEY_FILE_NAME,
        # Before the key is waited for and before every read of it (and before it is bound into the step): the folder and the
        # file, when they exist, must be the job user's own and not links, and the folder must have no group or other bits.
        # What is not so ends the job 247 with one line; nothing is read.
        'kaos_key_check(){ local bad="";',
        '  if [ -e "$KAOS_KEY_DIR" ] || [ -L "$KAOS_KEY_DIR" ]; then',
        '    { [ -O "$KAOS_KEY_DIR" ] && [ ! -L "$KAOS_KEY_DIR" ] && [ -z "$(find "$KAOS_KEY_DIR" -maxdepth 0 -perm /077 2>/dev/null)" ]; } ||'
        ' bad="the key folder is not the job user\'s own private folder (or is a link)"; fi',
        '  if [ -z "$bad" ] && { [ -e "$LAKEFS_KEY_FILE" ] || [ -L "$LAKEFS_KEY_FILE" ]; }; then',
        '    { [ -O "$LAKEFS_KEY_FILE" ] && [ ! -L "$LAKEFS_KEY_FILE" ]; } || bad="the key file is not the job user\'s own (or is a link)"; fi',
        '  [ -z "$bad" ] || { echo "AppIdentityUnavailable: $bad: the app\'s key is not read"; fail 247; }; }',
        'KAOS_KEY_WAIT=0; until kaos_key_check; [ -s "$LAKEFS_KEY_FILE" ]; do',
        '  if [ $KAOS_KEY_WAIT -ge ${KAOS_KEY_WAIT_S:-600} ]; then',
        '    echo "the app\'s data store key was not placed on the site within ${KAOS_KEY_WAIT_S:-600} s: the job cannot read or '
        'write the app\'s data. Run the step again"; fail 247; fi',
        '  sleep 2; KAOS_KEY_WAIT=$((KAOS_KEY_WAIT + 2)); done',
        # The registry's auth file (see REG_FILE_NAME): checked like the key folder before it is read, waited for only when a pull
        # needs it, and removed as soon as the pull is over (and by the exit trap and the sweep, whatever way the job ends).
        'KAOS_REG_DIR=$SCR/kaos-tmp/reg-$KAOS_JOB_ID; KAOS_REG_FILE=$KAOS_REG_DIR/' + REG_FILE_NAME,
        'kaos_reg_check(){ local bad="";',
        '  if [ -e "$KAOS_REG_DIR" ] || [ -L "$KAOS_REG_DIR" ]; then',
        '    { [ -O "$KAOS_REG_DIR" ] && [ ! -L "$KAOS_REG_DIR" ] && [ -z "$(find "$KAOS_REG_DIR" -maxdepth 0 -perm /077 2>/dev/null)" ]; } ||'
        ' bad="the registry credentials folder is not the job user\'s own private folder (or is a link)"; fi',
        '  if [ -z "$bad" ] && { [ -e "$KAOS_REG_FILE" ] || [ -L "$KAOS_REG_FILE" ]; }; then',
        '    { [ -O "$KAOS_REG_FILE" ] && [ -f "$KAOS_REG_FILE" ] && [ ! -L "$KAOS_REG_FILE" ]; } ||'
        ' bad="the registry credentials file is not the job user\'s own (or is a link)"; fi',
        '  [ -z "$bad" ] || { echo "$bad: the registry credentials are not read"; return 1; }; }',
        'BIND=""; KAOS_IN_ROOT=$SCR/kaos-tmp/in-$KAOS_JOB_ID; rm -rf "$KAOS_IN_ROOT"',
        'for KAOS_IN in $KAOS_INPUTS; do',
        '  IFS="|" read -r IN_NAME IN_REPO IN_COMMIT IN_STAGED IN_OWNER <<< "$KAOS_IN"',
        '  IN_DIR=""',
        # PRD-1541 F-06: the copy staged from a login node, when ready. The
        # marker is written last, so its presence means complete.
        '  if [ -n "$IN_STAGED" ] && [ -d "$IN_STAGED" ] && [ -f "${IN_STAGED%/*}/.kaos-ready" ]; then',
        '    IN_DIR=$IN_STAGED; touch "${IN_STAGED%/*}/.kaos-used" 2>/dev/null',
        '    echo "input $IN_NAME: resident copy $IN_DIR"',
        # F-04 stage-in, the kept fallback: the job downloads the commit itself. Fails LOUDLY (exit 232):
        # silently missing data would surface as a cryptic training error hours later. Only for the project's own
        # dataset (no owner): the job reaches only its own project's data store, where a repository of the same
        # name is another dataset. The runner never submits a job whose shared dataset is not staged.
        '  elif [ -z "$IN_OWNER" ] && [ -n "$LAKEFS_ENDPOINT" ] &&'
        ' [ -n "$STAGEIN_B64" ] && command -v python3 >/dev/null; then',
        '    [ -n "$IN_STAGED" ] && echo "input $IN_NAME: WARNING staged copy $IN_STAGED'
        ' not ready - downloading in-job on GPU time"',
        '    export DATASET_REPO=$IN_REPO DATASET_COMMIT=$IN_COMMIT DATASET_CACHE=$SCR/data-cache'
        ' DATASET_DIRFILE=$SCR/kaos-tmp/ds-$KAOS_JOB_ID-$IN_NAME.dir',
        '    kaos_key_check',
        '    printf %s "$STAGEIN_B64" | base64 -d | python3 - || fail 232',
        '    IN_COPY=$(cat "$DATASET_DIRFILE" 2>/dev/null); rm -f "$DATASET_DIRFILE"',
        '    [ -n "$IN_COPY" ] && [ -f "$IN_COPY/.kaos-complete" ] && [ -d "$IN_COPY/data" ] ||'
        ' { echo "input $IN_NAME: the in-job copy is not complete"; fail 232; }',
        '    IN_DIR=$IN_COPY/data',
        '  else',
        '    echo "input $IN_NAME: no staged copy and no way to download it in the job"; fail 232',
        '  fi',
        # Only now, for a copy that is whole: the folder it is bound onto, and its record beside it.
        '  mkdir -p "$KAOS_IN_ROOT/$IN_NAME" || fail 232',
        '  IN_OWNER_JSON=null; [ -n "$IN_OWNER" ] && IN_OWNER_JSON="\\"$IN_OWNER\\""',
        '  printf \'{"repo":"%s","commit":"%s","owner":%s,"full":true}\' "$IN_REPO" "$IN_COMMIT" "$IN_OWNER_JSON"'
        ' > "$KAOS_IN_ROOT/$IN_NAME.kaos-ready" || fail 232',
        '  BIND="$BIND -B $IN_DIR:/kaos/inputs/$IN_NAME:ro"',
        'done',
        # The step's variables do NOT ride the host environment: see "How a value reaches the step" (STEP_ENV).
        'if [ -n "$KAOS_INPUTS" ]; then BIND="-B $KAOS_IN_ROOT:/kaos/inputs:ro$BIND";'
        ' export APPTAINERENV_KAOS_STAGED_ROOT=/kaos/inputs; fi',
        # Writable /work: the SIF is read-only, but steps write /work/output
        # (declared outputs), /work/runs, /work/dataset caches. Per-job scratch
        # on Lustre, removed after a successful stage-out.
        'export KAOS_WORK=$SCR/kaos-work/$KAOS_JOB_ID; mkdir -p $KAOS_WORK/output',
        # A requeued job runs again under the same job id and folder: a result an earlier run left there must
        # never be handed back as this run's.
        'rm -f "$KAOS_WORK/output/kaos-result.json"',
        # D-74/D-75: every step gets a checkpoint folder (KAOS_CHECKPOINT_DIR in the container). The payload
        # keeps at most two copies of it on scratch, each ONE tar archive + manifest (file quota), and
        # restores the newest verified one here when the step is a continuation.
        'export KAOS_CKPT_ROOT=$SCR/kaos-ckpt KAOS_SCR=$SCR; mkdir -p $KAOS_WORK/ckpt "$KAOS_CKPT_ROOT"',
        # This job's work and checkpoint folders are private (they may hold the dataset a step wrote and its checkpoints).
        'chmod 700 "$KAOS_WORK" "$KAOS_WORK/output" "$KAOS_WORK/ckpt" "$KAOS_CKPT_ROOT"',
        'export APPTAINERENV_KAOS_CHECKPOINT_DIR=/work/ckpt',
        'CKPTPY=$KAOS_WORK/.kaos-ckpt.py; HAVE_CKPT=0',
        'if [ -n "$CKPT_B64" ] && command -v python3 >/dev/null; then'
        ' printf %s "$CKPT_B64" | base64 -d > $CKPTPY && HAVE_CKPT=1; fi',
        # Copies of steps abandoned for 7 days go, except those a queued or running job still names.
        '[ $HAVE_CKPT = 1 ] && python3 $CKPTPY expire',
        # A continued job starts from the checkpoint its previous job left, never silently from scratch:
        # no verified copy, no run (exit 234).
        'if [ -n "$KAOS_RESUME_REF" ]; then',
        '  [ $HAVE_CKPT = 1 ] || fail 234',
        '  python3 $CKPTPY restore || fail 234',
        '  export APPTAINERENV_KAOS_RESUME_PATH=/work/ckpt',
        'fi',
        'BIND="$BIND -B $KAOS_WORK:/work"',
        # Last use of a cached SIF, before the cleanup below can see it.
        'touch -c "$SIF"',
        # PRD-HPC-1461 AC-65: the label that tells the stager's cleanup which SIFs are one lineage (the same
        # image repository and step): written once, when a job builds the SIF, so the newest SIF of a
        # lineage is kept and the superseded ones go. repo = IMAGE_REF without the digest and the tag.
        'kaos_sif_label(){ local r=${IMAGE_REF%%@*}; case "${r##*/}" in *:*) r=${r%:*};; esac;'
        ' printf "%s\\t%s\\n" "$r" "$STEP_NAME" > "$SIF.lineage.tmp-$$" 2>/dev/null'
        ' && mv -f "$SIF.lineage.tmp-$$" "$SIF.lineage" 2>/dev/null; rm -f "$SIF.lineage.tmp-$$"; return 0; }',
        'if [ ! -f "$SIF" ]; then',
        # PRD-1609 F-02: build/pull to a name only this job writes, then rename it into
        # the shared cache (atomic on Lustre). Jobs, and steps sharing a node,
        # never run a half-written SIF; an image another job finished first is kept.
        '  SIF_TMP=$SIF.part-$SLURM_JOB_ID-$$',
        # Image pre-staging: the layers the waiter streamed to scratch at submit, while
        # its registry token was fresh. No registry access, so a queue wait of any
        # length is harmless. Only when absent or unusable does the job pull.
        '  if [ -n "$IMAGE_STAGED" ] && [ -f "$IMAGE_STAGED" ]; then',
        '    echo "image: building the SIF from the staged layers $IMAGE_STAGED"',
        '    touch -c "$IMAGE_STAGED"',
        # D-62: the SIF has the same key as the tar, so once it is built the tar is never needed again.
        '    if apptainer build "$SIF_TMP" "docker-archive://$IMAGE_STAGED"; then mv -n "$SIF_TMP" "$SIF"; kaos_sif_label; rm -f "$IMAGE_STAGED"; fi',
        '    rm -f "$SIF_TMP"',
        '  fi',
        '  if [ ! -f "$SIF" ]; then',
        '    [ -n "$IMAGE_STAGED" ] && echo "image: WARNING staged layers missing or unusable - pulling from the registry"',
        # GCP token ONLY for GAR hosts: presenting it to Zot turns an
        # anonymous-OK pull into 401 authentication required (live job
        # 5140432 vs the anonymous F-01 pull that worked on the same repo).
        # The token goes only to Artifact Registry: the HOST of the reference (its text up to the first slash, never
        # a glob over the whole reference) must be <one label>-docker.pkg.dev. Anything else pulls anonymously.
        # The token is a FILE (REG_FILE_NAME): the pull is given its path (--authfile), nothing secret is in a variable or
        # on a command line. The runner places the file right after the submit; a job that starts first waits for it
        # (KAOS_REG_WAIT_S, 120), and a file that never comes means an anonymous pull (which the runner answers, after a
        # long queue wait, as it answers an expired token: a fresh job with fresh credentials, bounded).
        '    KAOS_REG_HOST=${IMAGE_REF%%/*}; KAOS_REG_LABEL=${KAOS_REG_HOST%-docker.pkg.dev}; KAOS_PULL_AUTH=()',
        '    if [ "$KAOS_REG_AUTH" = 1 ] && [ "$KAOS_REG_LABEL" != "$KAOS_REG_HOST" ]; then',
        '      case "$KAOS_REG_LABEL" in ""|-*|*-|*[!a-z0-9-]*) ;; *)',
        '        KAOS_REG_WAIT=0; until kaos_reg_check || fail 231; [ -s "$KAOS_REG_FILE" ] || [ $KAOS_REG_WAIT -ge ${KAOS_REG_WAIT_S:-120} ]; do',
        '          sleep 2; KAOS_REG_WAIT=$((KAOS_REG_WAIT + 2)); done',
        '        if [ -s "$KAOS_REG_FILE" ]; then KAOS_PULL_AUTH=(--authfile "$KAOS_REG_FILE")',
        '        else echo "the registry credentials were not placed on the site within ${KAOS_REG_WAIT_S:-120} s: the image is pulled without them"; fi;; esac',
        '    fi',
        '    apptainer pull "${KAOS_PULL_AUTH[@]}" "$SIF_TMP" "docker://$IMAGE_REF" || { rm -f "$SIF_TMP"; fail 231; }',
        '    mv -n "$SIF_TMP" "$SIF"; kaos_sif_label; rm -f "$SIF_TMP"',
        '  fi',
        'fi',
        # The registry token has done its work (or never will): its file is gone, here, not only when the job ends.
        'rm -rf "$KAOS_REG_DIR"; unset KAOS_PULL_AUTH',
        # PRD-HPC-1461 AC-65: whatever way the SIF came to exist (built here, built by another job first,
        # already cached) the staged tar is useless now, so it goes in every case, not only after this
        # job's own build (live 2026-11-10: tars of jobs that found the SIF cached piled up).
        '[ -n "$IMAGE_STAGED" ] && [ -f "$SIF" ] && rm -f "$IMAGE_STAGED"',
        # PRD-1541: the image and download caches share the allocation's scratch
        # quota with the dataset cache and only ever grew. This user's entries
        # unused for 14 days go: SIFs, Apptainer's layer cache, in-job
        # downloads. What this job uses was touched just above.
        'find $SCR/sif-cache -maxdepth 1 -name "*.sif" -user "$USER" -mtime +14 -delete 2>/dev/null',
        'find $SCR/sif-cache -maxdepth 1 -name "*.sif.part-*" -user "$USER" -mmin +720 -delete 2>/dev/null',
        # Staged layers are only a way to build the SIF (the stager evicts them under
        # space pressure too): unused for 3 days they go.
        '[ -n "$IMAGE_STAGED" ] && find "${IMAGE_STAGED%/*}" -maxdepth 1 -name "*.tar" -user "$USER" -mtime +2 -delete 2>/dev/null',
        # In-job copies: a folder goes when its marker is 14 days old. The marker is .kaos-complete, or .complete
        # for a copy from before it that was never completed since. The stage-in (payload/stagein.py) may be at
        # the same folder, so each one is taken (mkdir <folder>.kaos-clean, atomic; one left by a killed cleanup
        # is removed after 10 minutes), its marker's age is read AGAIN, and the folder is moved away in one
        # rename before the lock is let go. The stage-in changes the marker first and looks for the lock after,
        # so one of the two always sees the other: a folder being filled or just used is never removed.
        'find $SCR/data-cache -mindepth 3 -maxdepth 3 \\( -name .kaos-complete -o -name .complete \\) -user "$USER"'
        ' -mtime +14 -printf "%h\\n" 2>/dev/null | sort -u | while read -r d; do'
        ' mkdir "$d.kaos-clean" 2>/dev/null ||'
        ' { find "$d.kaos-clean" -maxdepth 0 -mmin +10 -exec rmdir {} \\; 2>/dev/null; continue; };'
        ' m="$d/.kaos-complete"; [ -e "$m" ] || m="$d/.complete";'
        ' [ -n "$(find "$m" -maxdepth 0 -mtime +14 2>/dev/null)" ] && mv "$d" "$d.kaos-trash-$$" 2>/dev/null;'
        ' rmdir "$d.kaos-clean" 2>/dev/null; done',
        # The slow part, outside the lock: what the line above (of this or of a killed job) moved away.
        'find $SCR/data-cache -mindepth 2 -maxdepth 2 -name "*.kaos-trash-*" -user "$USER"'
        ' -exec rm -rf {} + 2>/dev/null',
        # AC-65: the layer cache is only a by-product of a pull (the SIF is the cache that counts). Files older
        # than a day go every time (younger ones may belong to a build on another node); the stager's cleanup
        # does the same from the login node when no job runs.
        'apptainer cache clean --force --days 1 >/dev/null 2>&1',
        # Start in the image's WORKDIR (Apptainer ignores it; Docker does not).
        'PWD_OPT=""; [ -n "$STEP_WORKDIR" ] && PWD_OPT="--pwd $STEP_WORKDIR"',
        # --nv only on GPU classes: on the cpu/largemem partitions there is
        # no driver to bind and the flag makes Apptainer warn or fail.
        'NV=""; [ "${HPC_GPUS:-0}" -gt 0 ] && NV="--nv"',
        # PRD-HPC-1461 AC-37 (live job 5439721): CUDA_VISIBLE_DEVICES reached the step as "0,1,2,3     " (trailing
        # blanks); CUDA tools may drop the last device or reject the value. Whatever the path (alone, shared,
        # fenced or not), the step sees a clean comma list.
        'for KAOS_V in CUDA_VISIBLE_DEVICES NVIDIA_VISIBLE_DEVICES; do'
        ' if [ -n "${!KAOS_V}" ]; then KAOS_C=${!KAOS_V//[[:space:]]/}; export "$KAOS_V=$KAOS_C"; fi; done',
        # AC-37: a step of a G-GPU class sees G devices and KUBECORE_SLOT_GPUS=G, alone too. The slot manager
        # sets 1 for a shared slot; a step running alone gets the class's GPU count.
        '[ -z "$KUBECORE_SLOT_GPUS" ] && [ "${HPC_GPUS:-0}" -gt 0 ] && export KUBECORE_SLOT_GPUS=$HPC_GPUS',
        # PRD-HPC-1461 AC-57 / D-29: on a shared node (slot manager: KUBECORE_SLOT_SHARED=1) a step must not see
        # a GPU outside its set. MeluXina has ConstrainDevices=no, so CUDA_VISIBLE_DEVICES only steers CUDA.
        # Prevention: where Apptainer can use nvidia-container-cli (--nvccli, probed with a throw-away start,
        # never assumed), bind only the step's own device files. Inside, they are numbered 0..G-1, so
        # CUDA_VISIBLE_DEVICES is rewritten to match. Without it the slot manager's watchdog still stops a
        # step that touches another GPU (reason GPUViolation).
        'if [ -n "$NV" ] && [ "${KUBECORE_SLOT_SHARED:-0}" = 1 ] && [ -n "$CUDA_VISIBLE_DEVICES" ]'
        ' && command -v nvidia-container-cli >/dev/null 2>&1; then',
        '  export APPTAINERENV_NVIDIA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES',
        '  KAOS_NGPU=$(echo "$CUDA_VISIBLE_DEVICES" | tr "," "\n" | grep -c .)',
        '  export APPTAINERENV_CUDA_VISIBLE_DEVICES=$(seq -s, 0 $((KAOS_NGPU - 1)))',
        '  if apptainer exec --nv --nvccli "$SIF" true >/dev/null 2>&1; then NV="--nv --nvccli";'
        ' echo "gpu fence: nvccli, only GPU(s) $CUDA_VISIBLE_DEVICES are visible to the step"',
        '  else unset APPTAINERENV_NVIDIA_VISIBLE_DEVICES APPTAINERENV_CUDA_VISIBLE_DEVICES;'
        ' echo "gpu fence: --nvccli unusable here, env steering and the slot watchdog only"; fi',
        'elif [ -n "$NV" ] && [ "${KUBECORE_SLOT_SHARED:-0}" = 1 ]; then',
        '  echo "gpu fence: no nvidia-container-cli, env steering and the slot watchdog only"',
        'fi',
        '[ -n "$STEP_CMD" ] || STEP_CMD="nvidia-smi -L || nproc"',
        # PRD-HPC-1461 D-38/D-44: Slurm sends USR1 to this shell about 10 minutes
        # before the time limit. The step runs in its own process group (set -m), and
        # the shell passes USR1 ONLY to the processes of that group that handle it
        # (SigCgt bit 9): kaos-lib's handler (an open run writer) saves progress and
        # exits 85. Never the whole group: Apptainer's squashfuse dies on USR1 and the
        # image vanishes under the step (live job 5421507: Bus error, rc 135). Inside,
        # sh ignores USR1 and so does every process that keeps that setting, so a step
        # without kaos-lib runs to the limit as before. `wait` returns early on the
        # trap, so wait again until the step is really gone.
        # The sampler. nvidia-smi is optional (CPU classes); a value it cannot report is null.
        'kaos_sample(){ trap \'kill $S 2>/dev/null; exit 0\' TERM; while :; do'
        ' g=$(nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null'
        ' | awk -F, \'BEGIN{printf "["} {gsub(/ /,"")} NR>1{printf ","}'
        ' {printf "{\\"util\\":%s,\\"mem_used_mib\\":%s,\\"mem_total_mib\\":%s}",'
        ' ($1~/^[0-9]+$/?$1:"null"),($2~/^[0-9]+$/?$2:"null"),($3~/^[0-9]+$/?$3:"null")} END{printf "]"}\' 2>/dev/null);'
        ' [ -n "$g" ] || g="[]";'
        ' l=$(cut -d" " -f1-3 /proc/loadavg 2>/dev/null | tr " " ",");'
        ' m=$(awk \'/^MemAvailable:/{print $2}\' /proc/meminfo 2>/dev/null);'
        ' printf \'KAOS_METRIC {"t":%s,"gpus":%s,"load":[%s],"mem_available_kb":%s}\\n\''
        ' "$(date +%s)" "$g" "${l:-0,0,0}" "${m:-0}" 2>/dev/null;'
        ' sleep 30 & S=$!; wait $S; done; }',
        'set -m',
        # --cleanenv: the step sees STEP_ENV and nothing of the job's environment (the registry token, the checkpoint
        # and hand-back programs, the job's own settings). How each value gets there is described at STEP_ENV: a plain
        # value as APPTAINERENV_<name>, the context, the bearers and anything with a character that evaluation would
        # change as a file in a private folder that the container reads. A name the script already set as APPTAINERENV_
        # (the GPU fence renumbers CUDA_VISIBLE_DEVICES) is not overwritten. The exports live in the subshell that
        # becomes the step's apptainer, so no other process of the job inherits them. A value that cannot be written
        # ends the step with 233: a step never starts without its context.
        'KAOS_ENV_DIR=$SCR/kaos-tmp/env-$KAOS_JOB_ID; rm -rf "$KAOS_ENV_DIR"; mkdir -m 700 "$KAOS_ENV_DIR" || fail 233',
        'kaos_key_check',
        'BIND="$BIND -B $KAOS_ENV_DIR:' + ENV_MOUNT + ':ro -B $KAOS_KEY_DIR:' + KEY_MOUNT + ':ro"',
        'KAOS_STEP_ENV="' + ' '.join(STEP_ENV) + '"',
        'KAOS_FILE_ENV="' + ' '.join(FILE_ENV) + '"',
        "KAOS_LOADER='" + LOADER + "'",
        '( if [ "${KUBECORE_SLOT_SHARED:-0}" = 1 ]; then',
        # On a shared node the Slurm facts the step sees are its own share, never the host job's (srun hands every
        # step the host job's variables): the slot manager's figures, or nothing.
        '    unset SLURM_CPUS_PER_TASK SLURM_CPUS_ON_NODE SLURM_MEM_PER_NODE SLURM_NTASKS SLURM_GPUS_ON_NODE SLURM_JOB_GPUS;',
        '    case "$KUBECORE_SLOT_CPUS" in ""|*[!0-9]*) ;; *) export SLURM_CPUS_PER_TASK=$KUBECORE_SLOT_CPUS'
        ' SLURM_CPUS_ON_NODE=$KUBECORE_SLOT_CPUS;; esac;',
        '    case "$KUBECORE_SLOT_MEM_GIB" in ""|*[!0-9]*|???????*) ;;'
        ' *) export SLURM_MEM_PER_NODE=$((10#$KUBECORE_SLOT_MEM_GIB * 1024));; esac;',
        '    export SLURM_NTASKS=1; [ -n "$KAOS_STEP_GPUS" ] && export SLURM_JOB_GPUS=$KAOS_STEP_GPUS'
        ' SLURM_GPUS_ON_NODE=$(echo "$KAOS_STEP_GPUS" | tr "," "\\n" | grep -c .);',
        '  fi;',
        '  export APPTAINERENV_LAKEFS_KEY_FILE=' + KEY_MOUNT + '/' + KEY_FILE_NAME + ';',
        '  for KAOS_V in $KAOS_STEP_ENV; do [ -n "${!KAOS_V+x}" ] || continue; KAOS_A=APPTAINERENV_$KAOS_V;',
        '    [ -z "${!KAOS_A+x}" ] || continue;',
        '    case " $KAOS_FILE_ENV " in *" $KAOS_V "*) KAOS_F=1;; *) case "${!KAOS_V}" in *[!' + PLAIN_CHARS + ']*) KAOS_F=1;;'
        ' *) KAOS_F=0;; esac;; esac;',
        '    if [ $KAOS_F = 1 ]; then printf %s "${!KAOS_V}" > "$KAOS_ENV_DIR/$KAOS_V" || exit 233;'
        ' else export "$KAOS_A=${!KAOS_V}"; fi; done;',
        '  exec apptainer exec --cleanenv $NV $PWD_OPT $BIND "$SIF" /bin/sh -c "$KAOS_LOADER trap \'\' USR1; $STEP_CMD" ) &',
        'STEP_PID=$!',
        'set +m',
        # D-65: every 30 s a KAOS_METRIC line in the job output (GPU, load, free memory). Best effort:
        # it never fails the job and dies with it.
        'kaos_sample &',
        'SAMPLER_PID=$!',
        # D-75: copy the checkpoint folder every few minutes while the step runs (guarded by the project's
        # space quota); SIGUSR2 = copy now.
        'CKPT_PID=""',
        'if [ $HAVE_CKPT = 1 ]; then python3 $CKPTPY watch & CKPT_PID=$!; fi',
        # D-72: the idle watchdog of THIS step (on a shared node every slot's script has its own). It
        # sees the step's process group and the job output file, writes IDLE_MARK and ends the step
        # with TERM, a short grace, KILL when the step sat idle for the whole window. 0 = off.
        'IDLE_MARK=$SCR/kaos-tmp/idle-$KAOS_JOB_ID; rm -f "$IDLE_MARK"; WATCHDOG_PID=""',
        'if [ -n "$IDLEWATCH_B64" ] && [ "${KAOS_IDLE_TIMEOUT_MIN:-20}" -gt 0 ] 2>/dev/null && command -v python3 >/dev/null; then',
        '  python3 -c "$(printf %s "$IDLEWATCH_B64" | base64 -d)" "$STEP_PID" "slurm-$KAOS_JOB_ID.out"'
        ' "$IDLE_MARK" "${KAOS_IDLE_TIMEOUT_MIN:-20}" "${HPC_GPUS:-0}" &',
        '  WATCHDOG_PID=$!',
        'fi',
        'kaos_stop(){ echo "stop signal: asking the step to save its progress";'
        ' [ -n "$CKPT_PID" ] && kill -USR2 $CKPT_PID 2>/dev/null;'
        ' for p in $(pgrep -g $STEP_PID); do'
        ' m=$(awk \'/^SigCgt:/{print $2}\' /proc/$p/status 2>/dev/null);'
        ' [ -n "$m" ] && [ $(( 0x$m >> 9 & 1 )) -eq 1 ] && kill -USR1 $p 2>/dev/null; done; }',
        'trap kaos_stop USR1',
        'wait $STEP_PID; rc=$?',
        'while kill -0 $STEP_PID 2>/dev/null; do wait $STEP_PID; rc=$?; done',
        'kill $SAMPLER_PID 2>/dev/null',
        'rm -rf "$KAOS_IN_ROOT" "$KAOS_ENV_DIR"',     # the step is gone: the folders of its inputs and its variables go too
        'trap - USR1',
        '[ -n "$CKPT_PID" ] && { kill $CKPT_PID 2>/dev/null; wait $CKPT_PID 2>/dev/null; }',
        '[ -n "$WATCHDOG_PID" ] && kill $WATCHDOG_PID 2>/dev/null',
        # D-72: the watchdog ended the step. Reason Idle wins over whatever the killed step exited
        # with (even 85: a step that was hung saved no checkpoint). Stage-out and the failure comment
        # below run as for any failed step; the controller records Failed/Idle from exit 236.
        'if [ -f "$IDLE_MARK" ]; then rm -f "$IDLE_MARK"; rc=236; echo "idle watchdog: the step ended as Idle (exit 236)"; fi',
        # What a hung step left in its folder is copied too, so the continuation does not lose it.
        'if [ $rc -eq 236 ] && [ $HAVE_CKPT = 1 ]; then python3 $CKPTPY final; fi',
        # D-44, D-75: 85 = the step saved before the wall time (or was asked to stop). Copy the folder one last
        # time (replaces the oldest copy, never a third); the payload writes the copy into the job comment.
        # Only then may the controller continue the step. The outputs are not final, so nothing else is staged
        # out. A final copy that fails ends the job with 235.
        'if [ $rc -eq 85 ]; then',
        '  echo "step saved its checkpoint before the wall time (exit 85)"',
        '  [ $HAVE_CKPT = 1 ] || fail 235',
        '  python3 $CKPTPY final || fail 235',
        '  rm -rf "$KAOS_WORK"; exit 85',
        'fi',
        # The result hand-back (docs/step-contract.md, section 8; PRD kaos#1911 CON-04). Only a step that ended
        # well hands anything back: its result file, or the record that it wrote none. When that cannot be done
        # (the file cannot be read, lakeFS does not take it, the job has no way to send it) the JOB FAILS with
        # exit 243; the hand-back program's last line says why. A step that failed publishes nothing.
        'if [ $rc -eq 0 ]; then',
        '  if [ -n "$HANDBACK_B64" ] && command -v python3 >/dev/null; then',
        '    printf %s "$HANDBACK_B64" | base64 -d | python3 - || rc=243',
        '  else',
        '    echo "result hand-back: the job has no hand-back program or the node has no python3"; rc=243',
        '  fi',
        'fi',
        # A step that succeeded needs no checkpoint copies: delete them (frees the project's scratch quota).
        '[ $rc -eq 0 ] && rm -rf "$KAOS_WORK" "$KAOS_CKPT_ROOT/$WF_UID/$STEP_NAME"',
        '[ $rc -ne 0 ] && fail $rc',
        'exit 0',
    ]
