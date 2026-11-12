"""HPC job logs into the pool's VictoriaLogs (D-65, F-38).

Nothing on the site pushes anywhere. The runner reads the job's output file incrementally through the
stager's read-only `log --job <id> --offset <n> [--max 1048576]` (own account only, CON-01) and prints
each complete line as one JSON object on stdout, which the pool's Vector ships:

    {"hpc_job": "<slurm id>", "step": "...", "workflow": "...", "attempt": N, "msg": "<line>"}

A line `KAOS_METRIC {json}` (written by the batch script's sampler) is printed with "metric": true and
the parsed JSON under "m"; its per-GPU utilisation is also kept (last and highest value per GPU) and
reported in status.run.gpuUtilization with the log offset, which the controller exports as
kaos_hpc_job_gpu_utilization_percent (AC-69). The offset of the last complete line is kept in status.run.logOffset (at
most every 30 s, and when the runner stops), so a restarted runner resumes without printing lines
again (AC-70). A stager without `log` disables streaming with one warning; the job is not affected.

Every line is redacted (redact.py: the credentials this job holds by exact value, and secret shapes) before it is
printed, kept in the tail or sent anywhere: a step's output is as private as the job comment.
"""

import base64
import collections
import json
import threading
import time

from .redact import Redactor
from .staging import EX_USAGE

POLL_S = 30   # one stager call per 30 s per running step, over the shared SSH connection (AC-68: < 60 s)
PERSIST_S = 30
MAX_BYTES = 1 << 20
METRIC_PREFIX = 'KAOS_METRIC '
TAIL_LINES = 20
TAIL_MAX_CHARS = 2000   # status.run.lastLog allows 2048


class GpuSamples:
    """Last and highest utilisation per GPU of one site job, from the sampler's KAOS_METRIC lines. The GPU is
    the position in the sampler's list (nvidia-smi order on the node); a value the sampler could not read
    (null) is skipped."""

    def __init__(self):
        self.last, self.high = {}, {}

    def add(self, gpus):
        for i, g in enumerate(gpus if isinstance(gpus, list) else []):
            util = g.get('util') if isinstance(g, dict) else None
            if isinstance(util, int) and not isinstance(util, bool) and 0 <= util <= 100:
                self.last[i] = util
                self.high[i] = max(util, self.high.get(i, 0))

    def status(self):
        """status.run.gpuUtilization, or None before the first sample."""
        return [{'gpu': str(i), 'percent': self.last[i], 'maxPercent': self.high[i]}
                for i in sorted(self.last)] or None


class LogStreamer:
    def __init__(self, cfg, stager, reporter, out=None, poll_s=POLL_S, persist_s=PERSIST_S,
                 clock=time.monotonic, secrets=lambda: ()):
        self.cfg, self.stager, self.reporter = cfg, stager, reporter
        self.secrets = secrets     # the credential values this job holds, asked for when a line is redacted
        self._held, self._redactor = None, None
        self.out = out or (lambda line: print(line, flush=True))
        self.poll_s, self.persist_s, self.clock = poll_s, persist_s, clock
        self.lock = threading.RLock()
        self.job = None
        self.offset = 0          # next byte to ask for
        self.carry = b''         # bytes of the last, incomplete line
        self.persisted = None    # the committed offset last written to the job
        self.last_persist = clock()
        self.gpu = GpuSamples()
        self.persisted_gpu = None
        self.tail = collections.deque(maxlen=TAIL_LINES)   # last non-metric lines, for an Idle report
        self.disabled = False
        self.active = threading.Event()
        self.stop = threading.Event()
        self.thread = None

    # ---- position

    def committed(self):
        """The offset of the first byte not printed as a complete line: where a restart resumes."""
        return self.offset - len(self.carry)

    def attach(self, job, offset=0):
        """Follow this Slurm job from offset (0 for a new one). Called with the lock free."""
        with self.lock:
            self.job, self.offset, self.carry = str(job), int(offset or 0), b''
            self._redactor = None          # a new job: no private key block is open
            self.persisted = self.offset
            self.gpu, self.persisted_gpu, self.reporter.gpu_util = GpuSamples(), None, None
            self.reporter.log_offset = self.offset if offset else None

    # ---- one read

    def _read(self):
        """(data, next offset) or None when nothing could be read."""
        words = ['log', '--job', self.job, '--offset', self.offset, '--max', MAX_BYTES]
        try:
            rc, events = self.stager(words, quiet=True)
        except Exception as e:
            print('log stream: stager call failed (%s), asking again' % e, flush=True)
            return None
        ev = next((e for e in reversed(events) if e.get('event') == 'log'), None)
        if ev is None:
            if rc == EX_USAGE:
                self.disabled = True
                print('log stream: the stager has no `log` subcommand (usage error) - HPC job logs are '
                      'not streamed; the job itself is not affected', flush=True)
            return None
        try:
            return base64.b64decode(ev.get('data') or ''), int(ev['offset'])
        except Exception as e:
            print('log stream: unreadable stager answer (%s)' % e, flush=True)
            return None

    def _redact(self, text):
        """The line with every credential replaced, or None for a line inside a private key block."""
        held = tuple(v for v in self.secrets() if v)
        if self._redactor is None or held != self._held:
            self._held, self._redactor = held, Redactor(held)
        return self._redactor.line(text)

    def _line(self, raw):
        text = self._redact(raw.decode('utf-8', 'replace').rstrip('\r'))
        if text is None:
            return
        obj = {'hpc_job': self.job, 'step': self.cfg.step, 'workflow': self.cfg.wf_name,
               'attempt': self.cfg.attempt, 'msg': text}
        if text.startswith(METRIC_PREFIX):
            try:
                obj['m'] = json.loads(text[len(METRIC_PREFIX):])
                obj['metric'] = True
                if isinstance(obj['m'], dict):
                    self.gpu.add(obj['m'].get('gpus'))
                    self.reporter.gpu_util = self.gpu.status()
            except ValueError:
                pass
        if not obj.get('metric') and text.strip():
            self.tail.append(text)
        self.out(json.dumps(obj, ensure_ascii=False))

    def tail_text(self):
        """The last printed lines of the step (metric lines left out), at most TAIL_MAX_CHARS."""
        return '\n'.join(self.tail)[-TAIL_MAX_CHARS:]

    def poll(self, final=False):
        """Read what the job wrote since the last call (as many reads as it takes to catch up) and print
        its complete lines. final: the job ended, so the unterminated last line is printed too.
        Returns True when anything was printed."""
        printed = False
        with self.lock:
            if self.disabled or self.job is None:
                return False
            for _ in range(64):
                got = self._read()
                if got is None:
                    break
                data, nxt = got
                if nxt < self.offset:        # the file is shorter than we read: start over from its end
                    self.carry = b''
                self.offset = nxt
                buf = self.carry + data
                *lines, self.carry = buf.split(b'\n')
                for raw in lines:
                    self._line(raw)
                    printed = True
                if len(data) < MAX_BYTES:    # caught up
                    break
            if final and self.carry:
                self._line(self.carry)
                self.carry, printed = b'', True
            self.persist(force=final)
        return printed

    def persist(self, force=False):
        """status.run.logOffset, at most every PERSIST_S unless forced."""
        with self.lock:
            c = self.committed()
            if (c == self.persisted and self.reporter.gpu_util == self.persisted_gpu) or self.job is None:
                return
            if not force and self.clock() - self.last_persist < self.persist_s:
                return
            self.last_persist = self.clock()
            gpu = self.reporter.gpu_util
            if self.reporter.write_log_offset(c):
                self.persisted, self.persisted_gpu = c, gpu

    def finish(self):
        """The job ended: the last of its output, then the offset."""
        self.poll(final=True)
        self.persist(force=True)

    # ---- background

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True, name='log-stream')
        self.thread.start()

    def _run(self):
        while not self.stop.is_set():
            if self.active.is_set():
                try:
                    self.poll()
                except Exception as e:      # never let logs take the runner down
                    print('log stream: %s' % e, flush=True)
            self.stop.wait(self.poll_s)

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(5)
