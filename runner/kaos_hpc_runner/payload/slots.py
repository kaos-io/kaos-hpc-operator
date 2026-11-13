
import os, re, signal, subprocess, sys, time

# Everything this program and the steps it launches create is private to the site user (the job script sets the
# same umask; jobs of one project share that user, so this protects from other users, not from the same user's other jobs).
os.umask(0o077)

# A step's environment: Slurm's own variables, the GPU it was given and the login
# basics. Nothing of the host's inputs reaches a guest; each script exports its own.
STEP_ENV = re.compile(r'^(SLURM_|CUDA_VISIBLE_DEVICES$|GPU_DEVICE_ORDINAL$|ROCR_VISIBLE_DEVICES$'
                      r'|KUBECORE_SLOT_|PATH$|HOME$|USER$|LANG$|LC_)')

if sys.argv[1:2] == ['--step']:
    import pwd
    script, job = sys.argv[2], sys.argv[3]
    # The slot manager signals this step by pid (D-44, AC-73): the task process becomes the step's bash below.
    pidfile = os.environ.get('KAOS_STEP_PIDFILE')
    if pidfile:
        with open(pidfile, 'w') as f:
            f.write(str(os.getpid()))
    env = dict((k, v) for k, v in os.environ.items() if STEP_ENV.match(k))
    # The login basics, also when Slurm passed none (SLURM_EXPORT_ENV=NONE).
    me = pwd.getpwuid(os.getuid())
    env.setdefault('HOME', me.pw_dir)
    env.setdefault('USER', me.pw_name)
    env.setdefault('PATH', '/usr/bin:/bin:/usr/local/bin')
    env.update(KAOS_IN_STEP='1', KAOS_JOB_ID=job)
    # AC-57: the GPU set Slurm gave this step, stamped where every process of the step (also inside the
    # container, whose own CUDA_VISIBLE_DEVICES may be renumbered) carries it for the GPU watchdog.
    if 'CUDA_VISIBLE_DEVICES' in env:     # Slurm pads the list with blanks (live job 5439721, AC-37)
        env['CUDA_VISIBLE_DEVICES'] = ''.join(env['CUDA_VISIBLE_DEVICES'].split())
    if env.get('CUDA_VISIBLE_DEVICES'):
        env['KAOS_STEP_GPUS'] = env['CUDA_VISIBLE_DEVICES']
    os.chdir(env.get('HOME') or '/')
    if len(sys.argv) > 4:
        # The step writes its own log: srun forwards output late, and fail() reads
        # this file for the job's comment (live 2026-11-03: the hosted tail was empty).
        fd = os.open(sys.argv[4], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
        os.dup2(fd, 1)
        os.dup2(fd, 2)
    os.execve('/bin/bash', ['bash', '-l', script], env)

ME = os.environ['SLURM_JOB_ID']
OWN_SCRIPT = sys.argv[1]
SCOPE = os.environ['KAOS_SCOPE']
PREFIX = 'kaos-' + SCOPE + '-'
HEADER = '# kaos-job v2 scope=' + SCOPE
CLAIMS = os.path.join(os.environ['KAOS_SCR'], 'kaos-claims')
HOME = os.environ.get('HOME') or '/'
GPUS = max(1, int(os.environ.get('HPC_GPUS') or 1))
CPUS = max(1, int(os.environ.get('SLURM_CPUS_ON_NODE') or GPUS) // GPUS)
IDLE_S = int(os.environ.get('KAOS_IDLE_S') or 120)
KILLED_EXIT = 137       # 128 + KILL: the exit code of a step the slot manager stopped itself
CHECKPOINT_EXIT = 85    # the step saved its checkpoint (D-44; resume.Classify)
MARGIN_S = 300          # a guest must end this long before the host's own limit
LEAD_UNIT_S = 60        # a resumable step's lead is declared in minutes
TOKEN_MARGIN_S = 1800   # ... and this long before its own token expires (D-07)
# Not takeable: held (another host took it) or waiting for another job. A job of this
# scope waiting for its BeginTime is in its offer window (OFFER_S): exactly the job a
# host should take, so it is NOT skipped.
HELD = ('JobHeld', 'Dependency')
POLL_S = float(os.environ.get('KAOS_POLL_S') or 2)   # watchdog cadence; queue every 5 polls


def log(*a):
    print(time.strftime('%H:%M:%S'), 'slots:', *a, flush=True)


def run(*cmd):
    p = subprocess.run(list(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True)
    return p.returncode, p.stdout.strip()


def q(job, fmt):
    rc, out = run('squeue', '-h', '-j', job, '-o', fmt)
    return out if rc == 0 else ''


def comment(job):
    c = q(job, '%k')
    return '' if c == '(null)' else c


def set_comment(job, text):
    run('scontrol', 'update', 'JobId=' + job, 'Comment=' + text)


def lease(c):
    # Every kaos- word except the host's own kaos-state= stays on every host write: the dataset lease
    # (kaos-commit=), the image lease (kaos-image=, D-62), the checkpoint lease and copy (kaos-ckpt*=, D-75).
    found = [t for t in c.split() if t.startswith('kaos-') and not t.startswith('kaos-state=')]
    return ''.join(f + ' ' for f in found)


def err_part(c):
    m = re.search(r'(?:^|\s)(err\d+:.*)$', c)
    return ' ' + m.group(1) if m else ''


def attempt(c):
    m = re.search(r'(?:^|\s)attempt=(\d+)', c)
    return int(m.group(1)) if m else 1


def mem_share_bytes():
    mb = int(os.environ.get('SLURM_MEM_PER_NODE') or 0)
    if not mb:
        with open('/proc/meminfo') as f:
            mb = int(f.readline().split()[1]) // 1024
    return mb * 1024 * 1024 // GPUS


def limit_s(text):
    # squeue %l: UNLIMITED, D-HH:MM:SS, HH:MM:SS, MM:SS or MM
    if not text or text in ('UNLIMITED', 'INVALID', 'NOT_SET'):
        return None
    days, _, rest = text.rpartition('-')
    try:
        parts = [int(x) for x in rest.split(':')]
    except ValueError:
        return None
    if len(parts) == 1:
        h, m, s = 0, parts[0], 0
    elif len(parts) == 2:
        h, m, s = 0, parts[0], parts[1]
    else:
        h, m, s = parts[-3:]
    return int(days or 0) * 86400 + h * 3600 + m * 60 + s


def end_epoch():
    e = q(ME, '%e')
    try:
        return time.mktime(time.strptime(e, '%Y-%m-%dT%H:%M:%S'))
    except ValueError:
        return time.time() + 3600


PART = q(ME, '%P')
HOST_END = end_epoch()
SHARE = mem_share_bytes()
PAGE = os.sysconf('SC_PAGE_SIZE')
slots = {}      # slot -> {'job', 'proc', 'claim', 'name', 'memkill', 'gpukill', 'start', 'limit'}
own_rc = None
skip = set()    # jobs looked at and not ours to take


def given_limit(limit):
    # The time limit srun gets for a step, in seconds: whole minutes, rounded up (None: no limit).
    return max(1, (limit + 59) // 60) * 60 if limit else None


def launch(slot, job, script, log_to, limit=None, pidfile=None):
    # --export=ALL: a job with an explicit environment (REST) has SLURM_EXPORT_ENV=NONE,
    # and a plain srun would start the step without HOME or PATH (live 2026-11-03,
    # job 5417729). The launcher then keeps only Slurm's variables and the basics.
    cmd = ['srun', '--export=ALL', '--exact', '-N1', '-n1', '--gpus=1', '-c', str(CPUS),
           '--job-name', 'kaos-' + job]
    if limit:
        cmd.append('--time=%d' % (given_limit(limit) // 60))
    cmd += ['python3', sys.argv[0], '--step', script, job] + ([log_to] if log_to else [])
    out = open(log_to, 'a') if log_to else None
    # PRD-1609 F-05: the step's share of the node, for kaos_lib.runtime (context().slot,
    # usable_cpus()): the GPU is already the only one it sees; cores and memory are not hidden.
    env = dict(os.environ, KUBECORE_SLOT_GPUS='1', KUBECORE_SLOT_CPUS=str(CPUS),
               KUBECORE_SLOT_MEM_GIB=str(SHARE >> 30), KUBECORE_SLOT_SHARED='1')
    if pidfile:
        env['KAOS_STEP_PIDFILE'] = pidfile
    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT if out else None,
                            env=env, preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM}))
    return proc


def step_id(job):
    rc, out = run('squeue', '-s', '-j', ME, '-h', '-o', '%i %j')
    for line in out.splitlines():
        sid, _, name = line.partition(' ')
        if name == 'kaos-' + job:
            return sid
    return None


def pss_bytes(pid):
    # PSS shares a page between the processes that map it, so the forked data-loader
    # workers of one training are not counted many times over (RSS would be).
    try:
        with open('/proc/%s/smaps_rollup' % pid) as f:
            for line in f:
                if line.startswith('Pss:'):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    with open('/proc/%s/statm' % pid) as f:      # kernels without smaps_rollup
        return int(f.read().split()[1]) * PAGE


def step_rss():
    # Memory per step, from /proc (psslurm has no slurmd tooling: scontrol listpids fails).
    rss = {}
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            with open('/proc/%s/environ' % pid, 'rb') as f:
                env = dict(kv.split(b'=', 1) for kv in f.read().split(b'\0') if b'=' in kv)
            if env.get(b'SLURM_JOB_ID') != ME.encode() or b'SLURM_STEP_ID' not in env:
                continue
            used = pss_bytes(pid)
        except (OSError, ValueError, IndexError):
            continue
        sid = env[b'SLURM_STEP_ID'].decode()
        rss[sid] = rss.get(sid, 0) + used
    return rss


def gpu_ids():
    # nvidia-smi's view of the node: {uuid: index}. Empty when there is no nvidia-smi (then nothing is watched).
    try:
        rc, out = run('nvidia-smi', '--query-gpu=index,uuid', '--format=csv,noheader')
    except OSError:
        return {}
    ids = {}
    for line in out.splitlines() if rc == 0 else []:
        idx, _, uuid = [x.strip() for x in line.partition(',')]
        if idx.isdigit() and uuid:
            ids[uuid] = idx
    return ids


def gpu_users():
    # [(pid, gpu uuid)] of every process with a compute context on any GPU of the node.
    rc, out = run('nvidia-smi', '--query-compute-apps=gpu_uuid,pid', '--format=csv,noheader')
    users = []
    for line in out.splitlines() if rc == 0 else []:
        uuid, _, pid = [x.strip() for x in line.partition(',')]
        if pid.isdigit():
            users.append((pid, uuid))
    return users


def step_gpu_claim(pid):
    # (step id, the GPU set it was given) of a process of one of THIS job's steps, else None. The set is the
    # KAOS_STEP_GPUS the launcher stamped; a step without one is never judged (no set, no violation).
    try:
        with open('/proc/%s/environ' % pid, 'rb') as f:
            env = dict(kv.split(b'=', 1) for kv in f.read().split(b'\0') if b'=' in kv)
    except (OSError, ValueError):
        return None
    if env.get(b'SLURM_JOB_ID') != ME.encode() or b'SLURM_STEP_ID' not in env:
        return None
    given = [x.strip() for x in env.get(b'KAOS_STEP_GPUS', b'').decode().split(',') if x.strip()]
    return (env[b'SLURM_STEP_ID'].decode(), set(given)) if given else None


def gpu_trespassers():
    # {step number: [gpu index, ...]} of steps whose processes compute on a GPU outside their set. The set may
    # list indices or UUIDs (Slurm writes either), so a GPU is allowed by its index or its uuid.
    ids = gpu_ids()
    bad = {}
    for pid, uuid in gpu_users() if ids else []:
        claim = step_gpu_claim(pid)
        if claim and ids.get(uuid) not in claim[1] and uuid not in claim[1]:
            bad.setdefault(claim[0], set()).add(ids.get(uuid, uuid))
    return bad


def step_limit_of(text):
    # The step's own time limit in seconds, from its script (KAOS_STEP_LIMIT_MIN). Its job
    # asks Slurm for more (padded, so a host has room for guests); the step needs only this.
    m = re.search(r'^export KAOS_STEP_LIMIT_MIN=(\d+)$', text, re.M)
    return int(m.group(1)) * 60 if m else None


def lead_of(text):
    # D-44 (AC-73): a resumable step's lead in seconds, from its script (KAOS_RESUMABLE_LEAD_MIN); 0 = not resumable.
    m = re.search(r'^export KAOS_RESUMABLE_LEAD_MIN=(\d+)$', text, re.M)
    return int(m.group(1)) * LEAD_UNIT_S if m else 0


def fits(now, limit, lead, token_exp, host_end):
    # May a guest of this step limit (and resumable lead, 0 = none) and token expiry be taken at `now`? It must
    # end MARGIN_S before the host's limit and before its token expires. A resumable guest also fits when only
    # its lead does: it is signalled at host_end - lead (AC-73) and checkpoints, so it needs host_end - now to
    # cover its lead plus the margin; it then ends no later than the host's end.
    if limit is None:
        return False
    ends = now + limit + MARGIN_S
    if ends > host_end and lead:
        if host_end - now < lead + MARGIN_S:
            return False
        ends = host_end
    return ends <= host_end and not (token_exp and ends > token_exp - TOKEN_MARGIN_S)


def signal_at(s, host_end):
    # When a resumable step is sent USR1: its lead before the end of its own limit or of the host's job,
    # whichever comes first. None for a step that is not resumable.
    if not s.get('lead'):
        return None
    ends = min(s['start'] + s['limit'], host_end) if s.get('limit') else host_end
    return ends - s['lead']


def stop_signal(s):
    # The stop signal (D-44): USR1 to the step's own shell, which passes it on to the processes of the step
    # that handle it and tells the checkpoint copier to copy now (jobscript kaos_stop). Sent once.
    try:
        with open(s['pidfile']) as f:
            os.kill(int(f.read().strip()), signal.SIGUSR1)
    except (OSError, ValueError):
        return False        # the step has not started, or is gone: tried again next poll
    s['signalled'] = True
    log('stop signal: USR1 to', s['job'], 'its lead before the end')
    return True


def take(slot, job, name, job_limit):
    # Take one waiting job onto slot. Its script is read first (no side effect); only a
    # job of this scope whose STEP fits the host's remaining time and its own token is
    # locked, held and run. A job that does not fit now never will on this host (its
    # remaining time only shrinks), so it is skipped for good.
    now = time.time()
    if not os.path.isdir(CLAIMS):
        os.makedirs(CLAIMS, mode=0o700, exist_ok=True)
    peek = os.path.join(CLAIMS, '.peek-%s-%s.sh' % (ME, job))
    rc, _ = run('scontrol', 'write', 'batch_script', job, peek)
    text = open(peek).read() if rc == 0 and os.path.exists(peek) else ''
    try:
        os.remove(peek)
    except OSError:
        pass
    if HEADER not in text.split('\n')[:3]:
        skip.add(job)
        log('not taking', job, '(no', HEADER + ')')
        return False
    limit = step_limit_of(text) or job_limit
    lead = lead_of(text)
    m = re.search(r'^export KAOS_TOKEN_EXP=(\d+)$', text, re.M)
    if not fits(now, limit, lead, int(m.group(1)) if m else None, HOST_END):
        skip.add(job)
        return False
    c = comment(job)
    k = attempt(c)
    claim = os.path.join(CLAIMS, '%s-a%d' % (job, k))
    try:
        os.mkdir(claim)
    except OSError:
        return False                       # another host is deciding or runs it
    # The claim folder is a lock: only its owner holds or releases the job, so two
    # hosts never undo each other. A taken job stays held, so it never also starts alone.
    keep = False
    # A TERM between writing `taken` and registering the guest in `slots` would leave the guest held and
    # marked taken with nobody to hand it back: hold the signal until the guest is registered (it is then
    # delivered, and the handler sees the guest).
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
    try:
        run('scontrol', 'hold', job)
        if not q(job, '%T|%r').startswith('PENDING|JobHeld'):
            return False                   # it started on its own node meanwhile
        script = os.path.join(claim, 'job.sh')
        rc, _ = run('scontrol', 'write', 'batch_script', job, script)
        if rc != 0 or HEADER not in open(script).read().split('\n')[:3]:
            return False
        set_comment(job, '%skaos-state=taken host=%s slot=%d attempt=%d' % (lease(c), ME, slot, k))
        pidfile = os.path.join(claim, 'step.pid')
        slots[slot] = {'job': job, 'claim': claim, 'name': name, 'memkill': False, 'attempt': k,
                       'start': time.time(), 'limit': given_limit(limit), 'lead': lead, 'pidfile': pidfile,
                       'proc': launch(slot, job, script, os.path.join(HOME, 'slurm-%s.out' % job), limit, pidfile)}
        keep = True
        log('took', job, name, 'on slot', slot)
        return True
    finally:
        if not keep:
            run('scontrol', 'release', job)
            unlock(claim)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM})


def unlock(claim):
    # Remove a claim folder (the lock and the script copy in it).
    if not claim:
        return
    try:
        for f in os.listdir(claim):
            os.remove(os.path.join(claim, f))
        os.rmdir(claim)
    except OSError:
        pass


def requeue(s):
    # Back to the queue with the next attempt number, still leasing its dataset.
    if q(s['job'], '%T') == 'PENDING':
        c = comment(s['job'])
        set_comment(s['job'], '%skaos-state=requeued attempt=%d' % (lease(c), s['attempt'] + 1))
        run('scontrol', 'release', s['job'])
        log('requeued', s['job'], 'as attempt', s['attempt'] + 1)
    unlock(s.get('claim'))


def timed_out(s, rc):
    # PRD-HPC-1461 D-67, AC-72: the step's own limit (srun --time) stopped it. Slurm stops a step only once it
    # ran its whole limit, with TERM and then KILL, so a step that died of one of them after its limit was
    # stopped by it. Its job then ends as a TIMEOUT, as a job alone does, not as a failure (live 2026-11-10,
    # guest 5439976 on host 5439966: "step 5439966.1 CANCELLED DUE TO TIME LIMIT", rc 143, recorded SiteFAILED).
    killed = rc < 0 or rc in (128 + signal.SIGTERM, 128 + signal.SIGKILL)
    return bool(s.get('limit')) and killed and time.time() - s['start'] >= s['limit']


def finish(slot, rc):
    global own_rc
    s = slots.pop(slot)
    job = s['job']
    if rc == 0 and (s['memkill'] or s.get('gpukill')):
        # A step the slot manager stopped (memory, GPU trespass) did not succeed, whatever srun says: live 2026-11-10
        # (job 5442274) srun returned 0 for a scancelled step and the job ended Succeeded.
        rc = KILLED_EXIT
    reason = (' reason=memkill' if s['memkill'] else ' reason=GPUViolation' if s.get('gpukill')
              else ' reason=TIMEOUT' if timed_out(s, rc) else '')
    if job == ME:
        own_rc = rc
        try:
            os.remove(s['pidfile'])
        except OSError:
            pass
        c = comment(ME)
        set_comment(ME, '%skaos-state=done rc=%d%s%s' % (lease(c), rc, reason, err_part(c)))
        log('own step ended rc=%d%s' % (rc, reason))
        return
    if q(job, '%T') != 'PENDING':
        log('guest', job, 'was stopped; its step ended rc=%d' % rc)
        unlock(s.get('claim'))
        return
    if q(ME, '%T') != 'RUNNING' and rc != CHECKPOINT_EXIT:
        # The host itself is going away (cancelled, out of time): Slurm may end the
        # steps before this program's TERM handler runs (live 2026-11-03, host
        # 5417697). The guest's run did not fail; it goes back to the queue. A step that saved and
        # exited 85 is reported as it is: its checkpoint copy is in its comment (AC-73).
        requeue(s)
        return
    c = comment(job)
    # start=: when the guest's step began. Its own job never starts, so this is the only start Slurm's records
    # give it: a learned limit comes from the step's own run time (D-67).
    set_comment(job, '%skaos-state=done rc=%d host=%s start=%d%s%s' % (lease(c), rc, ME, int(s['start']), reason, err_part(c)))
    run('scancel', job)
    unlock(s.get('claim'))
    log('guest', job, 'done rc=%d%s' % (rc, reason))


def requeue_all(signum, frame):
    # Host cancelled or out of time (Slurm sends TERM, KILL 30 s later): every guest
    # goes back to the queue with the next attempt number, still leasing its dataset.
    for s in list(slots.values()):
        if s['job'] != ME:
            requeue(s)
    sys.exit(own_rc if own_rc is not None else 143)


signal.signal(signal.SIGTERM, requeue_all)
OWN_LIMIT = int(os.environ.get('KAOS_STEP_LIMIT_MIN') or 0) * 60 or None
OWN_PIDFILE = os.path.join(CLAIMS, '.own-%s.pid' % ME)
os.makedirs(CLAIMS, mode=0o700, exist_ok=True)
os.chmod(CLAIMS, 0o700)
slots[0] = {'job': ME, 'claim': None, 'name': 'own', 'memkill': False, 'attempt': 1,
            'start': time.time(), 'limit': given_limit(OWN_LIMIT), 'pidfile': OWN_PIDFILE,
            'lead': int(os.environ.get('KAOS_RESUMABLE_LEAD_MIN') or 0) * LEAD_UNIT_S,
            'proc': launch(0, ME, OWN_SCRIPT, None, OWN_LIMIT, OWN_PIDFILE)}
log('host', ME, 'partition', PART, 'slots', GPUS, 'cpus/slot', CPUS, 'mem/slot %d MiB' % (SHARE >> 20))
idle_since, tick = None, 0
while True:
    time.sleep(POLL_S)
    tick += 1
    for slot, s in list(slots.items()):
        rc = s['proc'].poll()
        if rc is not None:
            finish(slot, rc)
    # AC-73: every resumable step is sent USR1 its lead before its limit or the host job's end, one timer per step.
    for s in list(slots.values()):
        due = signal_at(s, HOST_END)
        if due is not None and not s.get('signalled') and time.time() >= due:
            stop_signal(s)
    rss = step_rss()
    for s in slots.values():
        sid = s.get('sid') or step_id(s['job'])
        s['sid'] = sid
        if sid and rss.get(sid.split('.')[-1], 0) > SHARE and not s['memkill']:
            log('memory: step', sid, 'of', s['job'], 'uses %d MiB > %d MiB share' % (rss[sid.split('.')[-1]] >> 20, SHARE >> 20))
            s['memkill'] = True
            run('scancel', sid)
    # AC-57: a step that computes on a GPU that is not in its set is stopped alone, its neighbours run on.
    bad = gpu_trespassers() if GPUS > 1 else {}
    for s in slots.values():
        sid = s.get('sid')
        if sid and sid.split('.')[-1] in bad and not s.get('gpukill'):
            log('gpu: step', sid, 'of', s['job'], 'uses GPU', ','.join(sorted(bad[sid.split('.')[-1]])), 'outside its set: stopping it')
            s['gpukill'] = True
            run('scancel', sid)
    if tick % 5:
        continue
    for slot, s in list(slots.items()):
        if s['job'] != ME and q(s['job'], '%T') != 'PENDING' and s.get('sid'):
            log('guest', s['job'], 'stopped by its run: cancelling step', s['sid'])
            run('scancel', s['sid'])
    if q(ME, '%T') != 'RUNNING':
        # Cancelled or out of time: take nothing new; leave once the last step is gone.
        if not slots:
            sys.exit(own_rc if own_rc is not None else 143)
        continue
    # AC-73: a host whose own step saved (exit 85) takes nothing new and leaves once its guests are gone: its step's
    # continuation is reported when this job ends, so the job must not be held open for more work.
    free = [] if own_rc == CHECKPOINT_EXIT else [i for i in range(GPUS) if i not in slots]
    if free:
        rc, out = run('squeue', '-u', os.environ.get('USER', ''), '-h', '-t', 'PENDING',
                      '--sort=-p,i', '-o', '%i|%j|%r|%l|%P')
        for line in out.splitlines():
            if not free:
                break
            job, name, reason, lim, part = (line.split('|') + [''] * 5)[:5]
            if (job == ME or job in skip or not name.startswith(PREFIX) or part != PART
                    or reason.startswith(HELD)):
                continue
            if take(free[0], job, name, limit_s(lim)):
                free.pop(0)
    busy = bool(slots)
    if busy:
        idle_since = None
    elif idle_since is None:
        idle_since = time.time()
    elif time.time() - idle_since >= IDLE_S or HOST_END - time.time() < MARGIN_S + 60 or own_rc == CHECKPOINT_EXIT:
        log('idle: releasing the node')
        sys.exit(own_rc or 0)
