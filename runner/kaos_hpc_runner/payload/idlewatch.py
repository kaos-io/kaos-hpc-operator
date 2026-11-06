"""PRD-HPC-1461 D-72 / F-46: the idle watchdog of one step, run on the compute node by the job script.

    python3 -c "$(base64 -d <<< $IDLEWATCH_B64)" <step pgid> <job output file> <marker> <window min> <gpus>

One watchdog watches one step, so on a shared node (PRD-1609) every slot has its own: each step's
script starts it beside its own step, and it judges only that step. Python 3.6, stdlib only (the
node's system python).

The rule: every SAMPLE_S seconds the step is idle when its utilisation is below 5 % AND its log did
not grow since the last sample - GPU utilisation of the step's own GPUs on a GPU class (the indices
or UUIDs in CUDA_VISIBLE_DEVICES / NVIDIA_VISIBLE_DEVICES; every GPU when the step's set cannot be
told), CPU utilisation of the step's processes (below 5 % of one core) on a CPU class. A sample that
cannot be taken is not idle. After `window` consecutive idle minutes the watchdog writes the marker
file (the job script then exits 236, reason Idle), signals the step's process group TERM, waits
GRACE_S and kills what is left. The sampler's own KAOS_METRIC lines are not log growth.
"""
import os
import signal
import subprocess
import sys
import time

IDLE_PCT = 5.0                                            # below this a step is idle
SAMPLE_S = float(os.environ.get('KAOS_IDLE_SAMPLE_S') or 30)   # same cadence as the D-65 sampler
GRACE_S = float(os.environ.get('KAOS_IDLE_GRACE_S') or 30)     # TERM, then KILL this much later
METRIC_PREFIX = b'KAOS_METRIC '
READ_MAX = 1 << 20


def log(msg):
    print('idle watchdog: ' + msg, flush=True)


# ---- the rule

class IdleRule:
    """Idle for `window_s` seconds in a row. update() takes True (idle), False (busy) or None (could
    not tell, counts as busy) per sample and returns True once the window is full."""

    def __init__(self, window_s):
        self.window_s, self.since = window_s, None

    def update(self, now, idle):
        if idle is not True:
            self.since = None
            return False
        if self.since is None:
            self.since = now
        return now - self.since >= self.window_s


# ---- signal 1: the job output file

class LogWatch:
    """Did the step write to the job output since the last call? Lines that start with KAOS_METRIC
    (the D-65 sampler's) do not count; an unfinished line counts when it changed in length."""

    def __init__(self, path):
        self.path = path
        self.offset = self._size()
        self.rem_len = 0

    def _size(self):
        try:
            return os.path.getsize(self.path)
        except OSError:
            return 0

    def grew(self):
        try:
            with open(self.path, 'rb') as f:
                if os.fstat(f.fileno()).st_size < self.offset:    # truncated: start over
                    self.offset, self.rem_len = 0, 0
                f.seek(self.offset)
                data = f.read(READ_MAX)
        except OSError:
            return False
        if len(data) >= READ_MAX:
            return True                       # a megabyte in one interval is never only metrics
        head, sep, rem = data.rpartition(b'\n')
        grew = False
        if sep:
            self.offset += len(head) + 1
            self.rem_len = 0
            grew = any(l.strip() and not l.startswith(METRIC_PREFIX) for l in head.split(b'\n'))
        # The unfinished line stays unconsumed (read again whole next time). It is growth when it is
        # not (the start of) a metric line and its length changed since the last look.
        metric_like = rem.startswith(METRIC_PREFIX) or METRIC_PREFIX.startswith(rem)
        if rem.strip() and not metric_like and len(rem) != self.rem_len:
            grew = True
        self.rem_len = len(rem)
        return grew


# ---- signal 2a: GPU utilisation of the step's GPUs

def gpu_tokens(env):
    """The step's GPUs as named by CUDA_VISIBLE_DEVICES, else NVIDIA_VISIBLE_DEVICES (indices or
    UUIDs), or None when neither names a set ('all', 'void', unset)."""
    for key in ('CUDA_VISIBLE_DEVICES', 'NVIDIA_VISIBLE_DEVICES'):
        raw = (env.get(key) or '').strip()
        if raw and raw.lower() not in ('all', 'void', 'none', 'nodevfiles'):
            return [t.strip() for t in raw.split(',') if t.strip()]
    return None


def parse_gpu_csv(text):
    """[(index, uuid, util or None)] from `nvidia-smi --query-gpu=index,uuid,utilization.gpu`."""
    rows = []
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(',')]
        if len(parts) >= 3:
            rows.append((parts[0], parts[1], int(parts[2]) if parts[2].isdigit() else None))
    return rows


def pick_gpus(rows, tokens):
    """The rows that are the step's: matching an index or a (possibly abbreviated) UUID. When the
    set cannot be told or matches nothing, every GPU of the node - then the step is judged idle
    only when the whole node's GPUs are, which never ends a busy step."""
    if tokens:
        mine = [r for r in rows if r[0] in tokens or any(r[1].startswith(t) for t in tokens if len(t) > 3)]
        if mine:
            return mine
    return rows


def gpu_util(rows):
    """Highest utilisation of the rows, None when there is none or one cannot be read."""
    if not rows or any(r[2] is None for r in rows):
        return None
    return max(r[2] for r in rows)


def query_gpus():
    try:
        p = subprocess.run(['nvidia-smi', '--query-gpu=index,uuid,utilization.gpu',
                            '--format=csv,noheader,nounits'], stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, universal_newlines=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return []
    return parse_gpu_csv(p.stdout) if p.returncode == 0 else []


class GpuProbe:
    def __init__(self, tokens, query=query_gpus):
        self.tokens, self.query = tokens, query

    def util(self):
        return gpu_util(pick_gpus(self.query(), self.tokens))


# ---- signal 2b: CPU utilisation of the step's processes

def step_ticks(pgid):
    """{pid: utime+stime ticks} of the step: processes of its group and their descendants."""
    procs = {}
    for name in os.listdir('/proc'):
        if not name.isdigit():
            continue
        try:
            with open('/proc/%s/stat' % name) as f:
                data = f.read()
            rest = data[data.rindex(')') + 2:].split()      # comm may hold spaces and parentheses
            procs[int(name)] = (int(rest[1]), int(rest[2]), int(rest[11]) + int(rest[12]))
        except (OSError, ValueError, IndexError):
            continue
    mine = set(p for p, v in procs.items() if v[1] == pgid or p == pgid)
    grew = True
    while grew:
        more = set(p for p, v in procs.items() if v[0] in mine and p not in mine)
        grew = bool(more)
        mine |= more
    return dict((p, procs[p][2]) for p in mine)


class CpuProbe:
    """CPU use of the step in percent of one core since the last call (None on the first)."""

    def __init__(self, pgid, ticks=step_ticks, clock=time.monotonic):
        self.pgid, self.ticks, self.clock = pgid, ticks, clock
        self.prev, self.prev_t = None, None
        self.hz = os.sysconf('SC_CLK_TCK')

    def util(self):
        cur, now = self.ticks(self.pgid), self.clock()
        prev, prev_t, self.prev, self.prev_t = self.prev, self.prev_t, cur, now
        if prev is None or now <= prev_t:
            return None
        used = sum(max(t - prev.get(p, 0), 0) for p, t in cur.items())
        return 100.0 * used / self.hz / (now - prev_t)


# ---- the watchdog

class Watchdog:
    def __init__(self, pgid, probe, logw, mark, window_min, clock=time.monotonic, sleep=time.sleep,
                 killpg=os.killpg):
        self.pgid, self.probe, self.logw, self.mark = pgid, probe, logw, mark
        self.rule, self.window_min = IdleRule(window_min * 60), window_min
        self.clock, self.sleep, self.killpg = clock, sleep, killpg
        self.last_util = None

    def tick(self):
        """One sample. True when the step has been idle for the whole window."""
        grew = self.logw.grew()
        util = self.probe.util()
        self.last_util = util
        idle = False if grew or util is None else util < IDLE_PCT
        return self.rule.update(self.clock(), idle)

    def alive(self):
        return os.path.exists('/proc/%d' % self.pgid)

    def end_step(self):
        """The wall-time path: marker first (the job script reads it once the step is gone), TERM to
        the process group, a short grace, then KILL."""
        with open(self.mark, 'w') as f:
            f.write('idle %d min\n' % self.window_min)
        log('utilisation %s and no log output for %d min - ending the step (reason Idle): TERM, then KILL in %ds'
            % ('unknown' if self.last_util is None else '%.0f%%' % self.last_util, self.window_min, GRACE_S))
        self.signal(signal.SIGTERM)
        self.sleep(GRACE_S)
        self.signal(signal.SIGKILL)

    def signal(self, sig):
        try:
            self.killpg(self.pgid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    def run(self):
        while self.alive():
            self.sleep(SAMPLE_S)
            if self.alive() and self.tick():
                self.end_step()
                return


def main(argv, env=os.environ):
    pgid, logpath, mark, window, gpus = int(argv[0]), argv[1], argv[2], int(argv[3]), int(argv[4])
    if window <= 0:
        return 0
    probe = GpuProbe(gpu_tokens(env)) if gpus > 0 else CpuProbe(pgid)
    log('watching the step (%s, window %d min)' % ('GPU' if gpus > 0 else 'CPU', window))
    wd = Watchdog(pgid, probe, LogWatch(logpath), mark, window)
    if env.get('KAOS_IDLE_WINDOW_S'):       # tests only: a window shorter than a minute
        wd.rule.window_s = float(env['KAOS_IDLE_WINDOW_S'])
    wd.run()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
