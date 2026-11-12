"""The Slurm batch script of one attempt, as the in-pod waiter built it (PRD-HPC-1461 D-64).

The script is the proven program: Lmod init, SIF cache (private part-file, atomic rename),
input binds, the result hand-back, USR1 pass-through to handlers only, failure tail into the
job comment, the PRD-1609 host hook. D-72 idle watchdog (payload/idlewatch.py) ends a hung step with exit 236. Phase-1 additions: a 30 s sampler writing ``KAOS_METRIC``
lines into the job output (D-65) and deletion of the staged image tar after a successful SIF
build (D-62).

`batch_lines()` returns the script lines; they are joined with newlines by the caller.
"""


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
        # Diagnostics without SSH (live job 5143859: exit 231 with no way to
        # see WHY): on any failure, tail this job's own output file into its
        # job COMMENT — readable over REST from the waiter and by operators.
        # The dataset lease (kaos-commit=, PRD-1541 D-06) stays first.
        'fail(){ KAOS_NOTE=$(cat "$KAOS_WORK/.kaos-note" 2>/dev/null); kaos_comment -e "err$1:$(tail -c 700 slurm-$KAOS_JOB_ID.out 2>/dev/null | tr "\\n" "|")" $KAOS_NOTE; exit $1; }',
        # PRD-1609 F-03: with sharing on, the job's batch step is the host. It runs
        # this same script as one Slurm job step per slot (its own step and taken
        # guests' scripts), each with KAOS_IN_STEP=1 and that step's KAOS_JOB_ID.
        'if [ -z "$KAOS_IN_STEP" ] && [ -n "$KAOS_SLOTS_B64" ]; then',
        '  printf %s "$KAOS_SLOTS_B64" | base64 -d > /tmp/kaos-slots-$SLURM_JOB_ID.py',
        '  scontrol write batch_script $SLURM_JOB_ID /tmp/kaos-job-$SLURM_JOB_ID.sh >/dev/null || fail 233',
        '  exec python3 /tmp/kaos-slots-$SLURM_JOB_ID.py /tmp/kaos-job-$SLURM_JOB_ID.sh',
        'fi',
        'for f in /usr/share/lmod/lmod/init/bash /etc/profile.d/lmod.sh; do'
        ' [ -r "$f" ] && source "$f" && break; done',
        'module load Apptainer 2>/dev/null || module load apptainer 2>/dev/null',
        'command -v apptainer >/dev/null || fail 210',
        'SCR=' + scratch,
        'export APPTAINER_CACHEDIR=$SCR/kaos-apptainer-cache'
        ' APPTAINER_TMPDIR=$SCR/kaos-tmp',
        'mkdir -p $APPTAINER_CACHEDIR $APPTAINER_TMPDIR $SCR/sif-cache $SCR/kaos-work $SCR/data-cache',
        # The caches are shared between the jobs of this user on purpose, so they are readable by this user only. The
        # staged-image folder and the data cache are made by the login-node staging tool (not this repository), so
        # their modes are set here, on every job, to what a private cache needs.
        'chmod 700 "$SCR/kaos-apptainer-cache" "$SCR/kaos-tmp" "$SCR/sif-cache" "$SCR/kaos-work" "$SCR/data-cache" 2>/dev/null',
        '[ -n "$IMAGE_STAGED" ] && chmod 700 "${IMAGE_STAGED%/*}" 2>/dev/null',
        'KEY=$(printf %s "$IMAGE_REF" | sha256sum | cut -c1-16)',
        'SIF=$SCR/sif-cache/$KEY.sif',
        # The step's inputs (spec.inputs), each a whole dataset commit, presented to the step under one root,
        # $KAOS_STAGED_ROOT (/kaos/inputs in the container, read-only): the folder <input>/ holds the dataset's
        # files and nothing else (both kinds of copy keep them under <copy>/data/, beside the files of the platform,
        # and that folder is what is bound), and beside it the record <input>.kaos-ready ({"repo","commit","full":true}) says which repository
        # and commit it is and that it is the whole tree. kaos-lib finds an input by that record. The root is
        # a folder of this job; each copy is bound onto its folder there, whichever way it was staged.
        # KAOS_INPUTS holds one word per input: name|repository|commit|resident path (the data/ folder of the
        # copy staged from the login node, whose markers are beside it).
        'BIND=""; KAOS_IN_ROOT=$SCR/kaos-tmp/in-$KAOS_JOB_ID; rm -rf "$KAOS_IN_ROOT"',
        'for KAOS_IN in $KAOS_INPUTS; do',
        '  IFS="|" read -r IN_NAME IN_REPO IN_COMMIT IN_STAGED <<< "$KAOS_IN"',
        '  IN_DIR=""',
        # PRD-1541 F-06: the copy staged from a login node, when ready. The
        # marker is written last, so its presence means complete.
        '  if [ -n "$IN_STAGED" ] && [ -d "$IN_STAGED" ] && [ -f "${IN_STAGED%/*}/.kaos-ready" ]; then',
        '    IN_DIR=$IN_STAGED; touch "${IN_STAGED%/*}/.kaos-used" 2>/dev/null',
        '    echo "input $IN_NAME: resident copy $IN_DIR"',
        # F-04 stage-in, the kept fallback: the job downloads the commit itself. Fails LOUDLY (exit 232):
        # silently missing data would surface as a cryptic training error hours later.
        '  elif [ -n "$LAKEFS_BEARER_TOKEN" ] && [ -n "$LAKEFS_ENDPOINT" ] &&'
        ' [ -n "$STAGEIN_B64" ] && command -v python3 >/dev/null; then',
        '    [ -n "$IN_STAGED" ] && echo "input $IN_NAME: WARNING staged copy $IN_STAGED'
        ' not ready - downloading in-job on GPU time"',
        '    export DATASET_REPO=$IN_REPO DATASET_COMMIT=$IN_COMMIT DATASET_CACHE=$SCR/data-cache'
        ' DATASET_DIRFILE=$SCR/kaos-tmp/ds-$KAOS_JOB_ID-$IN_NAME.dir',
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
        '  printf \'{"repo":"%s","commit":"%s","full":true}\' "$IN_REPO" "$IN_COMMIT" > "$KAOS_IN_ROOT/$IN_NAME.kaos-ready" || fail 232',
        '  BIND="$BIND -B $IN_DIR:/kaos/inputs/$IN_NAME:ro"',
        'done',
        # Apptainer passes the host env through, so the step context and the wallet/endpoint vars reach the step.
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
        '    KAOS_REG_HOST=${IMAGE_REF%%/*}; KAOS_REG_LABEL=${KAOS_REG_HOST%-docker.pkg.dev}',
        '    if [ -n "$REG_TOKEN" ] && [ "$KAOS_REG_LABEL" != "$KAOS_REG_HOST" ]; then',
        '      case "$KAOS_REG_LABEL" in ""|-*|*-|*[!a-z0-9-]*) ;; *)'
        ' export APPTAINER_DOCKER_USERNAME=oauth2accesstoken APPTAINER_DOCKER_PASSWORD="$REG_TOKEN";; esac',
        '    fi',
        '    apptainer pull "$SIF_TMP" "docker://$IMAGE_REF" || { rm -f "$SIF_TMP"; fail 231; }',
        '    mv -n "$SIF_TMP" "$SIF"; kaos_sif_label; rm -f "$SIF_TMP"',
        '  fi',
        'fi',
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
        'apptainer exec $NV $PWD_OPT $BIND "$SIF" /bin/sh -c "trap \'\' USR1; $STEP_CMD" &',
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
        'rm -rf "$KAOS_IN_ROOT"',     # the step is gone: the folder its inputs were presented in goes too
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
