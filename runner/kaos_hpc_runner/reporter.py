"""What the runner tells the HPCArgoWorkflowJob it belongs to (status subresource, merge patch).

Targets HPC_JOB_NAMESPACE/HPC_JOB_NAME with the pod's own ServiceAccount token (get on
hpcargoworkflowjobs and get/patch on hpcargoworkflowjobs/status, ClusterRole
hpcargoworkflowjob-runner-role). The controller's follow()/endAttempt() stay the source of phase
transitions; the runner only reports facts: status.run of this attempt, status.outputs and the queue's
status.pendingReason.
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
              'hostSiteJobID', 'slot', 'checkpointRef', 'logOffset')
OUTPUTS_MAX_BYTES = 64 * 1024   # total of the declared outputs' JSON text (contract section 1)


class OutputsTooLarge(Exception):
    pass


def check_outputs(outputs):
    """The outputs as status.outputs wants them ({name: JSON text}); an error above 64 KiB in total."""
    total = sum(len(v.encode()) for v in outputs.values())
    if total > OUTPUTS_MAX_BYTES:
        raise OutputsTooLarge('the step outputs are %d bytes, the limit is %d (64 KiB in total)'
                              % (total, OUTPUTS_MAX_BYTES))
    return outputs


class Kube:
    def __init__(self, sa=K8S_SA, api=K8S_API):
        self.sa, self.api = sa, api

    def __call__(self, method, path, body=None, ctype='application/json'):
        tok = open(self.sa + '/token').read().strip()          # re-read: projected tokens rotate
        req = urllib.request.Request(
            self.api + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer ' + tok, 'Content-Type': ctype, 'Accept': 'application/json'})
        ctx = ssl.create_default_context(cafile=self.sa + '/ca.crt')
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return json.load(r)


class Reporter:
    def __init__(self, cfg, kube=None, sleep=time.sleep):
        self.cfg, self.kube, self.sleep = cfg, kube or Kube(), sleep
        self.attempt = cfg.attempt
        self.log_offset = None      # the log streamer's committed offset, written with every report

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

    def patch(self, status):
        self.kube('PATCH', self.path('/status'), {'status': status}, 'application/merge-patch+json')

    def report(self, must=False, outputs=None, **run):
        """status.run of this attempt, every field written (a merge patch would keep an earlier
        attempt's exit code or checkpoint). With outputs, status.outputs rides in the same patch, so
        the controller never sees run.terminal before the outputs. must: retry until it lands; the
        controller settles from it."""
        run['attempt'] = self.attempt
        if 'logOffset' not in run:
            run['logOffset'] = self.log_offset
        status = {'run': {k: run.get(k) for k in RUN_FIELDS}}
        if outputs is not None:
            status['outputs'] = check_outputs(outputs)
        for n in range(60 if must else 1):
            try:
                self.patch(status)
                return True
            except Exception as e:
                print('HPC job: report failed (%s)%s' % (e, ', retrying' if must else ''), flush=True)
                if must:
                    self.sleep(10)
        if must:
            print('HPC job: the end of the site job could not be reported', flush=True)
            sys.exit(1)
        return False

    def pending_reason(self, reason):
        """Slurm's reason while the job queues; None clears it."""
        try:
            self.patch({'pendingReason': reason or None})
        except Exception as e:
            print('HPC job: pendingReason not written (%s)' % e, flush=True)

    def write_log_offset(self, offset):
        """Only the offset (a merge patch of status.run keeps its other fields). Best effort."""
        self.log_offset = offset
        try:
            self.patch({'run': {'attempt': self.attempt, 'logOffset': offset}})
            return True
        except Exception as e:
            print('HPC job: logOffset not written (%s)' % e, flush=True)
            return False


def checkpoint_uri(repo, ckpt):
    return 'lakefs://%s/%s/%s' % (repo, ckpt[0], ckpt[1])
