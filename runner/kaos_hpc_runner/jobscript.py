"""The Slurm batch script of one attempt, as the in-pod waiter built it (PRD-HPC-1461 D-64).

The script is the proven program: Lmod init, SIF cache (private part-file, atomic rename),
dataset bind, checkpoint stage-in/out, USR1 pass-through to handlers only, failure tail into the
job comment, the PRD-1609 host hook. D-72 idle watchdog (payload/idlewatch.py) ends a hung step with exit 236. Phase-1 additions: a 30 s sampler writing ``KAOS_METRIC``
lines into the job output (D-65) and deletion of the staged image tar after a successful SIF
build (D-62).

`batch_lines()` returns the script lines; they are joined with newlines by the caller.
"""


def batch_lines(scope, scratch):
    """scope: the sharing scope ('-' when sharing is off); scratch: the project's scratch root."""
    return [
        '#!/bin/bash -l',
        # PRD-1609 F-01: the script's format and sharing scope ('-': sharing off).
        # A host takes only jobs whose header carries its own scope.
        '# kaos-job v2 scope=' + scope,
        'set +e',
        # The job this step belongs to: its own on its own node; on a shared node
        # the host sets it to this step's job. Every per-job path keys on it.
        'KAOS_JOB_ID=${KAOS_JOB_ID:-$SLURM_JOB_ID}',
        # Diagnostics without SSH (live job 5143859: exit 231 with no way to
        # see WHY): on any failure, tail this job's own output file into its
        # job COMMENT — readable over REST from the waiter and by operators.
        # The dataset lease (kaos-commit=, PRD-1541 D-06) stays first.
        'fail(){ scontrol update JobId=$KAOS_JOB_ID Comment="${KAOS_COMMIT:+kaos-commit=$KAOS_COMMIT }err$1:$(tail -c 700 slurm-$KAOS_JOB_ID.out 2>/dev/null | tr "\\n" "|")" 2>/dev/null; exit $1; }',
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
        'mkdir -p $APPTAINER_CACHEDIR $APPTAINER_TMPDIR $SCR/sif-cache',
        'KEY=$(printf %s "$IMAGE_REF" | sha256sum | cut -c1-16)',
        'SIF=$SCR/sif-cache/$KEY.sif',
        # F-04 stage-in: only when the wallet minted AND the run pins a
        # dataset. Fails LOUDLY (exit 232) — silently missing data would
        # surface as a cryptic training error hours later.
        'DATASET_DIR=""',
        # PRD-1541 F-06: the copy staged from a login node, when ready. The
        # marker is written last, so its presence means complete.
        'if [ -n "$DATASET_STAGED" ] && [ -f "$DATASET_STAGED/.kaos-ready" ]; then',
        '  DATASET_DIR=$DATASET_STAGED; touch "$DATASET_DIR/.kaos-used" 2>/dev/null',
        '  echo "dataset: resident copy $DATASET_DIR"',
        'elif [ -n "$LAKEFS_BEARER_TOKEN" ] && [ -n "$LAKEFS_ENDPOINT" ] &&'
        ' [ -n "$DATASET_REPO" ] && [ -n "$DATASET_REF" ] &&'
        ' [ -n "$STAGEIN_B64" ] && command -v python3 >/dev/null; then',
        '  [ -n "$DATASET_STAGED" ] && echo "dataset: WARNING staged copy $DATASET_STAGED'
        ' not ready - downloading in-job on GPU time"',
        '  export DATASET_CACHE=$SCR/data-cache'
        ' DATASET_DIRFILE=$SCR/kaos-tmp/ds-$KAOS_JOB_ID.dir',
        '  printf %s "$STAGEIN_B64" | base64 -d | python3 - || fail 232',
        '  DATASET_DIR=$(cat "$DATASET_DIRFILE" 2>/dev/null)',
        'fi',
        # Apptainer passes the host env through, so the wallet/endpoint vars
        # reach the step; the bind gives it the staged version read-only.
        'BIND=""',
        'if [ -n "$DATASET_DIR" ]; then'
        ' BIND="-B $DATASET_DIR:/kubecore/dataset:ro";'
        ' export APPTAINERENV_KUBECORE_DATASET_DIR=/kubecore/dataset'
        ' APPTAINERENV_KUBECORE_STAGED_DIR=/kubecore/dataset; fi',
        # Writable /work: the SIF is read-only, but steps write /work/output
        # (declared outputs), /work/runs, /work/dataset caches. Per-job scratch
        # on Lustre, removed after a successful stage-out.
        'export KAOS_WORK=$SCR/kaos-work/$KAOS_JOB_ID; mkdir -p $KAOS_WORK/output',
        # D-44: a continued job starts from the checkpoint its previous job staged out, never
        # silently from scratch: no checkpoint, no run (exit 234).
        'if [ -n "$KAOS_RESUME_REF" ]; then',
        '  { [ -n "$LAKEFS_BEARER_TOKEN" ] && [ -n "$CKPTIN_B64" ]; } || fail 234',
        '  rm -rf $KAOS_WORK/resume; printf %s "$CKPTIN_B64" | base64 -d | python3 - || fail 234',
        '  export APPTAINERENV_KAOS_RESUME_PATH=/work/resume',
        'fi',
        'BIND="$BIND -B $KAOS_WORK:/work"',
        # Last use of a cached SIF, before the cleanup below can see it.
        'touch -c "$SIF"',
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
        '    if apptainer build "$SIF_TMP" "docker-archive://$IMAGE_STAGED"; then mv -n "$SIF_TMP" "$SIF"; rm -f "$IMAGE_STAGED"; fi',
        '    rm -f "$SIF_TMP"',
        '  fi',
        '  if [ ! -f "$SIF" ]; then',
        '    [ -n "$IMAGE_STAGED" ] && echo "image: WARNING staged layers missing or unusable - pulling from the registry"',
        # GCP token ONLY for GAR hosts: presenting it to Zot turns an
        # anonymous-OK pull into 401 authentication required (live job
        # 5140432 vs the anonymous F-01 pull that worked on the same repo).
        '    case "$IMAGE_REF" in *-docker.pkg.dev/*)'
        ' [ -n "$REG_TOKEN" ] && export'
        ' APPTAINER_DOCKER_USERNAME=oauth2accesstoken'
        ' APPTAINER_DOCKER_PASSWORD=$REG_TOKEN;; esac',
        '    apptainer pull "$SIF_TMP" docker://$IMAGE_REF || { rm -f "$SIF_TMP"; fail 231; }',
        '    mv -n "$SIF_TMP" "$SIF"; rm -f "$SIF_TMP"',
        '  fi',
        'fi',
        # PRD-1541: the image and download caches share the allocation's scratch
        # quota with the dataset cache and only ever grew. This user's entries
        # unused for 14 days go: SIFs, Apptainer's layer cache, in-job
        # downloads. What this job uses was touched just above.
        'find $SCR/sif-cache -maxdepth 1 -name "*.sif" -user "$USER" -mtime +14 -delete 2>/dev/null',
        'find $SCR/sif-cache -maxdepth 1 -name "*.sif.part-*" -user "$USER" -mmin +720 -delete 2>/dev/null',
        # Staged layers are only a way to build the SIF (the stager evicts them under
        # space pressure too): unused for 3 days they go.
        '[ -n "$IMAGE_STAGED" ] && find "${IMAGE_STAGED%/*}" -maxdepth 1 -name "*.tar" -user "$USER" -mtime +2 -delete 2>/dev/null',
        'find $SCR/data-cache -mindepth 3 -maxdepth 3 -name .complete -user "$USER" -mtime +14'
        ' -printf "%h\\n" 2>/dev/null | xargs -r rm -rf',
        'apptainer cache clean --force --days 14 >/dev/null 2>&1',
        # Start in the image's WORKDIR (Apptainer ignores it; Docker does not).
        'PWD_OPT=""; [ -n "$STEP_WORKDIR" ] && PWD_OPT="--pwd $STEP_WORKDIR"',
        # --nv only on GPU classes: on the cpu/largemem partitions there is
        # no driver to bind and the flag makes Apptainer warn or fail.
        'NV=""; [ "${HPC_GPUS:-0}" -gt 0 ] && NV="--nv"',
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
        ' for p in $(pgrep -g $STEP_PID); do'
        ' m=$(awk \'/^SigCgt:/{print $2}\' /proc/$p/status 2>/dev/null);'
        ' [ -n "$m" ] && [ $(( 0x$m >> 9 & 1 )) -eq 1 ] && kill -USR1 $p 2>/dev/null; done; }',
        'trap kaos_stop USR1',
        'wait $STEP_PID; rc=$?',
        'while kill -0 $STEP_PID 2>/dev/null; do wait $STEP_PID; rc=$?; done',
        'kill $SAMPLER_PID 2>/dev/null',
        'trap - USR1',
        '[ -n "$WATCHDOG_PID" ] && kill $WATCHDOG_PID 2>/dev/null',
        # D-72: the watchdog ended the step. Reason Idle wins over whatever the killed step exited
        # with (even 85: a step that was hung saved no checkpoint). Stage-out and the failure comment
        # below run as for any failed step; the controller records Failed/Idle from exit 236.
        'if [ -f "$IDLE_MARK" ]; then rm -f "$IDLE_MARK"; rc=236; echo "idle watchdog: the step ended as Idle (exit 236)"; fi',
        # D-44 (amended 2026-11-05, T-19): 85 = a resumable step saved its checkpoint. Before
        # the job ends, the checkpoint goes to the outputs repository and its commit into the
        # job comment; only then may the waiter continue the step. The step's outputs are not
        # final, so nothing else is staged out. 85 from a step that is not resumable fails, and
        # so does a checkpoint that did not upload (exit 235).
        'if [ $rc -eq 85 ]; then',
        '  echo "step saved its checkpoint before the wall time (exit 85)"',
        '  [ "$KAOS_RESUMABLE" = 1 ] || { echo "exit 85, but the step is not declared resumable"; fail 85; }',
        '  { [ -n "$LAKEFS_BEARER_TOKEN" ] && [ -n "$CKPTOUT_B64" ]; } || fail 235',
        '  export KAOS_CKPT_FILE=$KAOS_WORK/.kaos-ckpt',
        '  printf %s "$CKPTOUT_B64" | base64 -d | python3 - || fail 235',
        '  read -r CKPT CKPT_PATH < $KAOS_CKPT_FILE',
        '  scontrol update JobId=$KAOS_JOB_ID Comment="${KAOS_COMMIT:+kaos-commit=$KAOS_COMMIT }'
        'kaos-ckpt=$CKPT kaos-ckpt-path=$CKPT_PATH" || fail 235',
        '  rm -rf "$KAOS_WORK"; exit 85',
        'fi',
        'if [ -n "$STEP_OUTPUTS" ] && [ -n "$STAGEOUT_B64" ] && [ -n "$LAKEFS_BEARER_TOKEN" ]; then'
        ' printf %s "$STAGEOUT_B64" | base64 -d | python3 - || echo "stage-out failed (rc=$rc)"; fi',
        '[ $rc -eq 0 ] && rm -rf "$KAOS_WORK"',
        '[ $rc -ne 0 ] && fail $rc',
        'exit 0',
    ]
