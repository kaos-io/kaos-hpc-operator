"""The Slurm REST client (slurmrestd + slurmdbd) and the readings the runner takes from a job."""

import json
import os
import re
import time
import urllib.error
import urllib.request

from . import settings


# Tries of one cancel (2 + 4 + 8 + 16 s of backoff between them: inside the runner pod's 60 s grace).
CANCEL_TRIES = 5


class Slurm:
    def __init__(self, cfg, token_file=settings.TOKEN_FILE):
        self.cfg, self.token_file = cfg, token_file

    def tok(self):
        """The MeluXina JWT, read FRESH on every request. The platform mints it with a 60-minute
        lifetime and rotates the Secret every 25 minutes; the mounted file follows the rotation, an
        env var does not (live run: the pod's env token expired while the job still queued)."""
        try:
            with open(self.token_file) as f:
                t = f.read().strip()
                if t:
                    return t
        except OSError:
            pass
        return (os.environ.get('SLURM_TOKEN') or '').strip()

    def hdrs(self):
        return {'X-SLURM-USER-NAME': self.cfg.identity()['user'],
                'X-SLURM-USER-TOKEN': self.tok(),
                'Content-Type': 'application/json'}

    def get(self, url):
        return json.load(urllib.request.urlopen(
            urllib.request.Request(url, headers=self.hdrs()), timeout=30))

    def live(self, jid):
        """slurmctld's view of a job ({} when unreadable is the caller's to handle)."""
        return (self.get(self.cfg.api + '/job/' + str(jid)).get('jobs') or [{}])[0]

    def accounting(self, jid):
        """slurmdbd's record of a job."""
        return (self.get(self.cfg.sdb + '/job/' + str(jid)).get('jobs') or [{}])[0]

    def find_active(self):
        for j in (self.get(self.cfg.api + '/jobs').get('jobs') or []):
            st = j.get('job_state') or []
            if j.get('name') == self.cfg.job_site_name and any(
                    x in ('PENDING', 'RUNNING', 'SUSPENDED') for x in st):
                return j.get('job_id')
        return None

    def cancel(self, jid, sleep=time.sleep):
        """DELETE the site job; retried with backoff while the site errs (PRD D-20: a cancel is retried until
        the site confirms; one 503 during an outage must not leave the job alive). True when the site accepted
        it or no longer knows the job (404); False when every try failed."""
        for n in range(CANCEL_TRIES):
            try:
                urllib.request.urlopen(urllib.request.Request(
                    self.cfg.api + '/job/' + str(jid), headers=self.hdrs(), method='DELETE'), timeout=30)
                print('cancelled job', jid, flush=True)
                return True
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print('cancel: the site does not know job', jid, flush=True)
                    return True
                err = e
            except Exception as e:
                err = e
            print('cancel failed (%d/%d): %s' % (n + 1, CANCEL_TRIES, err), flush=True)
            if n + 1 < CANCEL_TRIES:
                sleep(2 ** (n + 1))
        print('cancel gave up: job', jid, 'may still be alive at the site', flush=True)
        return False

    def submit(self, body):
        """POST a job: the parsed response. A refusal is raised as Refused with the site's reason."""
        req = urllib.request.Request(self.cfg.api + '/job/submit', data=json.dumps(body).encode(),
                                     headers=self.hdrs(), method='POST')
        try:
            return json.load(urllib.request.urlopen(req, timeout=30))
        except urllib.error.HTTPError as e:
            raise Refused(submit_refusal(e))


class Refused(Exception):
    pass


# Why a site refuses a job because of its lane (QoS or reservation) rather than the job itself. The
# texts are Slurm's own (slurm_errno / QOS reasons) and appear in the REST error body; matched
# case-insensitively so a changed wording around them still counts.
LANE_REFUSALS = ('qos',                       # Invalid qos specification, Job violates accounting/QOS policy,
                                              # QOSMaxJobsPerUserLimit, QOSMaxSubmitJobPerUserLimit, ...
                 'reservation',               # Requested reservation is invalid, Access denied to requested reservation
                 'time limit is invalid')     # Requested time limit is invalid (missing or exceeds some limit)


def lane_refusal(text):
    """True when a refusal text names the lane (QoS, reservation or a limit above its maximum)."""
    t = str(text).lower()
    return any(m in t for m in LANE_REFUSALS)


def submit_refusal(e):
    """slurmrestd refuses a job with an HTTP error whose body names why (errors/warnings: an account,
    partition or QOS it rejects, a time limit above the QOS); without it the step dies on a bare
    'HTTP Error 500'."""
    try:
        why = json.loads(e.read().decode(errors='replace') or '{}')
    except Exception:
        why = {}
    if not isinstance(why, dict):
        why = {}
    return 'HTTP %s %s' % (e.code, {k: why.get(k) for k in ('errors', 'warnings')
                                     if why.get(k)} or '(no reason in the body)')


def comment_of(job):
    """The job comment: a string from slurmctld, {'job': ...} from slurmdbd."""
    c = job.get('comment') or ''
    return (c.get('job') or '') if isinstance(c, dict) else c


def share_state(comment):
    """D-04: what a host wrote into this job's comment, as a dict ({} when no host took it):
    kaos-state (taken|done|requeued), host, slot, attempt, rc, reason."""
    return dict(re.findall(r'(?:^|\s)(kaos-state|host|slot|attempt|rc|reason)=(\S+)', comment))


# Slurm's "no value" sentinels: NO_VAL (0xFFFFFFFE) and INFINITE (0xFFFFFFFF) seconds. A job that
# never started carries one as its start; read as a date it is February 2106.
NO_VAL = 4294967294


def site_time(job, key):
    """The job's start/end as an RFC 3339 UTC string, or None when Slurm has no such time: a REST
    time object with set=false or infinite=true, or a number at or above NO_VAL, is absent."""
    t = ((job.get('time') or {}).get(key))
    if isinstance(t, dict):
        if t.get('set') is False or t.get('infinite'):
            return None
        t = t.get('number')
    if not isinstance(t, (int, float)) or isinstance(t, bool) or t <= 0 or t >= NO_VAL:
        return None
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t))


# Slurm states in which a job will not run again; HOST_ALIVE: states in which a host job still lives.
TERMINAL = ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'PREEMPTED', 'OUT_OF_MEMORY')
HOST_ALIVE = ('PENDING', 'RUNNING', 'SUSPENDED', 'COMPLETING')


def host_gone(slurm, host):
    """True only when accounting says the host job ended: an unreadable record is not a death (the
    host keeps running; the next poll asks again)."""
    try:
        h = slurm.accounting(host)
    except Exception:
        return False
    hst = (h.get('state') or {}).get('current') or []
    return bool(hst) and not any(x in HOST_ALIVE for x in hst)
