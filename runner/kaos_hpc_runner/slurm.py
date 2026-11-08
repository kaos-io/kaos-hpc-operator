"""The Slurm REST client (slurmrestd + slurmdbd) and the readings the runner takes from a job."""

import http.client
import json
import os
import re
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

from . import settings


# Tries of one cancel (2 + 4 + 8 + 16 s of backoff between them: inside the runner pod's 60 s grace).
CANCEL_TRIES = 5
# How far back the accounting search for a finished job of this attempt looks (seconds). The job NAME is what
# makes the match exact (it carries the HPC job and the attempt); this only bounds the query. A Slurm job lives
# at most days, so a week covers a submit whose pod came back late.
FINISHED_LOOKBACK_S = 7 * 24 * 3600


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
        # The legacy name (no attempt in it) is also looked for among LIVE jobs: a runner of the previous
        # release may have submitted this attempt's job just before an upgrade.
        names = (self.cfg.job_site_name, self.cfg.legacy_site_name)
        for j in (self.get(self.cfg.api + '/jobs').get('jobs') or []):
            st = j.get('job_state') or []
            if j.get('name') in names and any(x in ('PENDING', 'RUNNING', 'SUSPENDED') for x in st):
                return j.get('job_id')
        return None

    def find_finished(self, after=0, now=time.time):
        """The newest job of this attempt (by NAME) that slurmdbd lists and that was not cancelled, or None.
        A submit whose answer was lost and whose job ended before the runner came back is found here, not
        submitted twice. `after`: only a job id above this one counts (the job a previous runner reported
        and then replaced). A site that rejects the query (4xx) is read as "cannot tell": None, the caller
        then relies on the job the previous runner reported; a site that does not answer raises."""
        url = '%s/jobs?job_name=%s&start_time=%d' % (
            self.cfg.sdb, urllib.parse.quote(self.cfg.job_site_name), int(now()) - FINISHED_LOOKBACK_S)
        try:
            jobs = self.get(url).get('jobs') or []
        except urllib.error.HTTPError as e:
            if e.code < 500 and e.code != 429:
                print('accounting search by name refused (HTTP %s): not used' % e.code, flush=True)
                return None
            raise
        best = None
        for j in jobs:
            st = (j.get('state') or {}).get('current') or []
            jid = j.get('job_id')
            # A job this runner cancelled for a resubmit is not the attempt's result.
            if j.get('name') != self.cfg.job_site_name or not st or 'CANCELLED' in st or not str(jid).isdigit():
                continue
            if int(jid) > int(after or 0) and (best is None or int(jid) > int(best['job_id'])):
                best = j
        return best

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
            why = submit_refusal(e)
            if site_down_reply(e.code, why):
                raise SiteUnavailable(why)
            raise Refused(why)


class Refused(Exception):
    pass


class SiteUnavailable(Exception):
    """The site did not take or answer the request for a reason that is not the job's (AC-28, AC-30): the caller
    waits and asks again, it does not give up on the first one."""


# What slurmrestd says when slurmctld or slurmdbd is down behind it. Matched case-insensitively.
SITE_DOWN_TEXTS = ('unable to contact slurm', 'unable to connect', 'connection refused', 'timed out',
                   'slurmctld', 'slurmdbd', 'not responding')


def site_down_reply(code, why):
    """True when an HTTP error reply means the site is unavailable rather than that it refused the job: a
    gateway or overload code, a 500 that names no reason, or a 500 that names a dead slurmctld/slurmdbd. A
    500 with a real reason (an account, a QoS, a limit) is a refusal and fails at once."""
    if code in (408, 429, 502, 503, 504):
        return True
    if code != 500:
        return False
    return 'no reason in the body' in why or any(t in why.lower() for t in SITE_DOWN_TEXTS)


def is_site_down(e):
    """True when an exception from a site call means the site is unreachable now: no connection, a timeout, a
    broken reply, a gateway or server error. A 4xx (auth, unknown job, bad request) and everything else is not."""
    if isinstance(e, SiteUnavailable):
        return True
    if isinstance(e, urllib.error.HTTPError):
        return e.code >= 500 or e.code in (408, 429)
    return isinstance(e, (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError,
                          http.client.HTTPException, ssl.SSLError))


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
TERMINAL = ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'PREEMPTED', 'OUT_OF_MEMORY', 'BOOT_FAIL', 'REVOKED')
# Ends that are not the step's doing and after which a checkpoint copy, if there is one, continues the step (D-75).
RECOVERABLE = ('TIMEOUT', 'NODE_FAIL', 'PREEMPTED', 'BOOT_FAIL', 'REVOKED')
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
