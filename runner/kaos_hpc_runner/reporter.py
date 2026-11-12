"""What the runner tells the HPCArgoWorkflowJob it belongs to (status subresource, merge patch).

Targets HPC_JOB_NAMESPACE/HPC_JOB_NAME with the pod's own ServiceAccount token (get on
hpcargoworkflowjobs and get/patch on hpcargoworkflowjobs/status, ClusterRole
hpcargoworkflowjob-runner-role). The controller's follow()/endAttempt() stay the source of phase
transitions; the runner only reports facts: status.run of this attempt, status.result of a step that ended
well, and the queue's status.pendingReason.
"""

import json
import ssl
import sys
import time
import urllib.error
import urllib.request

K8S_SA = '/var/run/secrets/kubernetes.io/serviceaccount'
K8S_API = 'https://kubernetes.default.svc'
RUN_FIELDS = ('attempt', 'siteJobID', 'state', 'exitCode', 'startTime', 'endTime', 'terminal',
              'hostSiteJobID', 'slot', 'hostJobOpen', 'hostJobEndTime', 'checkpointRef', 'checkpoint', 'logOffset', 'lastLog', 'message',
              'gpuUtilization', 'staged', 'queueReplaced', 'queueReplacedCount')
QUEUE_REPLACED_KEPT = 5   # status.run.queueReplaced keeps the newest; queueReplacedCount counts all
CANCEL_ANNOTATION = 'hpc.kaos.io/cancel-requested'

class Kube:
    def __init__(self, sa=K8S_SA, api=K8S_API):
        self.sa, self.api = sa, api

    def __call__(self, method, path, body=None, ctype='application/json', timeout=30):
        tok = open(self.sa + '/token').read().strip()          # re-read: projected tokens rotate
        req = urllib.request.Request(
            self.api + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer ' + tok, 'Content-Type': ctype, 'Accept': 'application/json'})
        ctx = ssl.create_default_context(cafile=self.sa + '/ca.crt')
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
            return json.load(r)


class Reporter:
    def __init__(self, cfg, kube=None, sleep=time.sleep):
        self.cfg, self.kube, self.sleep = cfg, kube or Kube(), sleep
        self.attempt = cfg.attempt
        self.log_offset = None      # the log streamer's committed offset, written with every report
        self.gpu_util = None        # the log streamer's per-GPU last/max utilisation (AC-69), written likewise
        self.staged = []            # how each input was staged (staging.staging_record), written with every report
        self.queue_replaced = []    # the newest replacements of a queued site job (AC-35), written with every report
        self.queue_replaced_count = 0   # all of them, also the ones no longer listed
        # The QoS and reservation the site job was submitted with (None: not known to this runner,
        # e.g. it adopted a job; the report then leaves the earlier values alone).
        self.qos = None
        self.reservation = None
        self.last_run = None        # the status.run of the last report that landed (a later end-time report resends it)

    def path(self, sub=''):
        return '/apis/hpc.kaos.io/v1alpha1/namespaces/%s/hpcargoworkflowjobs/%s%s' % (
            self.cfg.namespace, self.cfg.job_name, sub)

    def get_job(self, tries=30):
        """The job, retried while the API server blips. None when it no longer exists (the controller
        is cancelling it); an error for anything the runner cannot fix (no permission)."""
        for n in range(tries):
            try:
                return self.kube('GET', self.path())
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                if e.code == 403:
                    print('this runner may not read its HPCArgoWorkflowJob (403): check the '
                          'hpcargoworkflowjob-runner-role binding', flush=True)
                    sys.exit(1)
                print('HPC job: HTTP %s, retrying' % e.code, flush=True)
            except Exception as e:
                print('HPC job: %s, retrying' % e, flush=True)
            self.sleep(10)
        print('HPC job: the API server did not answer for 5 minutes', flush=True)
        sys.exit(1)

    def site_observation(self):
        """status.siteObservation: the controller's batched reading of the site job (D-36). None when
        the job is unreadable or carries none; the caller then falls back on its own REST poll."""
        try:
            return ((self.kube('GET', self.path(), timeout=10).get('status') or {}).get('siteObservation')) or None
        except Exception as e:
            print('site observation: job not readable (%s)' % e, flush=True)
            return None

    def placement(self):
        """status.placement: the controller's plan for the attempt (lane, time limit, share role). None when the
        job is unreadable or carries none; a waiting guest then keeps waiting."""
        try:
            return ((self.kube('GET', self.path(), timeout=10).get('status') or {}).get('placement')) or None
        except Exception as e:
            print('placement: job not readable (%s)' % e, flush=True)
            return None

    def cancel_requested(self):
        """Did the controller ask for a real cancel (annotation hpc.kaos.io/cancel-requested, set before it
        deletes the runner Job)? A fresh read with a short timeout and 2 retries. None when it cannot be
        read: the caller must then not cancel (fail safe)."""
        for n in range(3):
            try:
                job = self.kube('GET', self.path(), timeout=5)
                return CANCEL_ANNOTATION in ((job.get('metadata') or {}).get('annotations') or {})
            except Exception as e:
                print('cancel check: job not readable (%s)' % e, flush=True)
                self.sleep(1)
        return None

    def patch(self, status):
        self.kube('PATCH', self.path('/status'), {'status': status}, 'application/merge-patch+json')

    def report(self, must=False, result=None, **run):
        """status.run of this attempt, every field written (a merge patch would keep an earlier
        attempt's exit code or checkpoint). result: the step ended well, and its checked result (wire form)
        rides in the same patch as status.result, so the controller never sees run.terminal before the result.
        Only that one report publishes; no other report touches status.result. must: retry until it lands; the
        controller settles from it."""
        run['attempt'] = self.attempt
        if 'logOffset' not in run:
            run['logOffset'] = self.log_offset
        if 'gpuUtilization' not in run:
            run['gpuUtilization'] = self.gpu_util
        run['staged'] = list(self.staged) or None
        # Every report carries the replacements (the run is written whole, a merge patch keeps no list).
        run['queueReplaced'] = list(self.queue_replaced) or None
        run['queueReplacedCount'] = self.queue_replaced_count or None
        status = {'run': {k: run.get(k) for k in RUN_FIELDS}}
        if self.qos is not None:
            status['run'].update(qos=self.qos, reservation=self.reservation or None)
        if result is not None:
            status['result'] = result
        for n in range(60 if must else 1):
            try:
                self.patch(status)
                self.last_run = dict(status['run'])
                return True
            except Exception as e:
                print('HPC job: report failed (%s)%s' % (e, ', retrying' if must else ''), flush=True)
                if must:
                    self.sleep(10)
        if must:
            print('HPC job: the end of the site job could not be reported', flush=True)
            sys.exit(1)
        return False

    def restore_replacements(self, run):
        """A restarted runner of the same attempt carries on from what the earlier one reported."""
        self.queue_replaced = list(run.get('queueReplaced') or [])[-QUEUE_REPLACED_KEPT:]
        self.queue_replaced_count = max(int(run.get('queueReplacedCount') or 0), len(self.queue_replaced))

    def restore_staging(self, run):
        """A restarted runner of the same attempt keeps the records its predecessor reported: its own staging
        finds the copies resident and would otherwise overwrite "first copy" with "already cached"."""
        self.staged = list(run.get('staged') or [])

    def note_staging(self, info):
        """One input's staging record of this runner (empty: the job stages that input in-job). A cache hit never
        replaces the record of the same dataset commit that an earlier runner of this attempt reported."""
        if not info:
            return
        for n, old in enumerate(self.staged):
            if (old.get('repo'), old.get('commit')) == (info['repo'], info['commit']):
                if info.get('kind') != 'cached':
                    self.staged[n] = dict(info)
                return
        self.staged.append(dict(info))

    def note_replacement(self, new, old, reason, queued):
        """A queued site job was replaced (its queue place is lost): keep the newest QUEUE_REPLACED_KEPT
        and count all. Sent with the next report. AC-35."""
        self.queue_replaced.append({'at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'oldSiteJobID': old,
                                    'newSiteJobID': new, 'queuedSeconds': queued, 'reason': reason})
        del self.queue_replaced[:-QUEUE_REPLACED_KEPT]
        self.queue_replaced_count += 1

    def pending_reason(self, reason):
        """Slurm's reason while the job queues; None clears it."""
        try:
            self.patch({'pendingReason': reason or None})
        except Exception as e:
            print('HPC job: pendingReason not written (%s)' % e, flush=True)

    def write_log_offset(self, offset):
        """The offset and the latest GPU readings (a merge patch of status.run keeps its other fields). Best effort."""
        self.log_offset = offset
        run = {'attempt': self.attempt, 'logOffset': offset}
        if self.gpu_util is not None:
            run['gpuUtilization'] = self.gpu_util       # the sampler's GPU readings ride with the offset (AC-69)
        try:
            self.patch({'run': run})
            return True
        except Exception as e:
            print('HPC job: logOffset not written (%s)' % e, flush=True)
            return False


def checkpoint_uri(ckpt):
    """The reference the controller keeps and hands back: the tar's path under the scratch root and its sha256."""
    return 'scratch://%s#%s' % (ckpt['rel'], ckpt['sha'])
