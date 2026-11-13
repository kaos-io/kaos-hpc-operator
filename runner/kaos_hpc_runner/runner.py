"""One attempt of one HPCArgoWorkflowJob on the site (PRD-HPC-1461 phase 1, D-58, D-64).

The in-pod waiter's program, minus its admission client: the controller already admitted and booked
this attempt before it created the runner Job. The runner submits the site job (or adopts the one a
previous runner of this attempt submitted), polls it to its end, reports to the job's status, and
exits. A continuation is the controller's decision (D-44): a new attempt gets a new runner Job.
"""

import calendar
import json
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from . import image as imagemod
from . import jobscript, payloads, result, settings, staging, wallet
from .logs import LogStreamer
from .reporter import Reporter, checkpoint_uri


def checkpoint_every_seconds(context):
    """Seconds between the checkpoint copies the run form chose for this step (`checkpoint_every`, minutes, in the step
    context), or 0 when the context holds none: the copier then adapts its own interval. The same value the step's
    kaos-lib reads (runtime.checkpoint_every_minutes), and the one the cluster's copier is given."""
    try:
        value = json.loads(context or '{}').get('checkpoint_every')
    except (ValueError, AttributeError):
        return 0
    return value * 60 if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0
from .sharing import inputs_in_script
from .slurm import (RECOVERABLE, TERMINAL, Refused, Slurm, comment_of, host_gone, is_site_down, lane_refusal,
                    share_state, site_time, submit_limit_refusal)
from .settings import (CHECKPOINT_EXIT, CONTEXT_ENV, GPU_VIOLATION_EXIT, GUEST_HOLD_S, IDLE_EXIT, INSUFFICIENT_QUOTA_EXIT,
                       INVALID_IMAGE_EXIT, INVALID_RESULT_EXIT, OFFER_S, ROLE_POLL_S, RESERVED_NAME_EXIT,
                       SHARED_DATASET_UNAVAILABLE_EXIT,
                       RESULT_NOT_HANDED_BACK_EXIT, STAGING_MISMATCH_EXIT, SUBMIT_LIMIT_EXIT, Settings, image_problem,
                       registry_token_host)

POLL_S = 30
# --cancel-only (the controller's orphan sweep, PRD AC-08): how long one such runner works before it gives up
# (the controller starts another), how often it looks, and how long a site that no longer knows the job
# must say so before that counts as an answer.
CANCEL_ONLY_BUDGET_S = 30 * 60
CANCEL_ONLY_POLL_S = 10
SITE_UNKNOWN_S = 10 * 60
# A host's runner follows the host job past its step until the job itself is over; this is the margin
# added to the job's time limit before it gives up waiting for that end.
HOST_END_MARGIN_S = 10 * 60

# PRD-HPC-1461 D-36: while the job is pending or running the CONTROLLER reads its state, once per site per
# interval, into status.siteObservation; the runner reads it there instead of calling the site. An
# observation older than STALE_INTERVALS of its own intervals (the controller is down), or none at all for
# STALE_GRACE_S, makes the runner poll the site itself, but only every STALE_POLL_S, so jobs never stall.
STALE_INTERVALS = 3
DEFAULT_INTERVAL_S = 30
STALE_GRACE_S = 90
STALE_POLL_S = 120
# The pending reason is not in every site's batched reading: when it is missing, the runner asks the
# job itself this often (not at every poll).
REASON_POLL_S = 120
# The registry pull token (GCE metadata, 1 h) is minted at submit time and cannot be refreshed inside
# a queued job, so every queue wait longer than an hour costs one failed pull (exit 231) and a
# resubmission. Bounded, not once: a second long wait after the first resubmit must not strand the run.
PULL_RESUBMITS = 3
# PRD-1609: how often this run started over because its host died with it.
LOST_HOST_RESUBMITS = 3
# AC-28/AC-30: an unreachable site is waited for, asking again after SITE_RETRY_FIRST_S, doubling up to SITE_RETRY_MAX_S.
SITE_RETRY_FIRST_S = 5
SITE_RETRY_MAX_S = 60
SITE_UNAVAILABLE = 'SiteUnavailable'   # the pending reason the controller shows, and the prefix of the failure message
# The step's result on its way back (payload/handback.py): fetched this often before the runner gives up.
RESULT_FETCH_TRIES = 4
RESULT_FETCH_WAIT_S = 15
HANDBACK_PREFIX = 'result hand-back: '   # what the job's hand-back program says in the job log before it fails


def parse_time(text):
    """Epoch seconds of an RFC 3339 UTC time ('2026-11-07T12:00:00Z'), or None."""
    try:
        return calendar.timegm(time.strptime(str(text)[:19], '%Y-%m-%dT%H:%M:%S'))
    except (ValueError, TypeError):
        return None


def checkpoint_info(comment):
    """The checkpoint report in a job comment (kaos-ckpt-info=), as status.run.checkpoint wants it, or None."""
    import re
    m = re.search(r'(?:^|\s)kaos-ckpt-info=(\S+)', comment or '')
    if not m:
        return None
    try:
        raw = json.loads(urllib.parse.unquote(m.group(1)))
    except ValueError:
        return None
    names = {'p': 'progressValues', 'pt': 'progressText', 'd': 'digest', 'b': 'bytes', 'sb': 'stepBytes',
             'pb': 'projectBytes', 'sk': 'skippedReason'}
    info = {names[k]: v for k, v in raw.items() if k in names and v not in (None, '', [], 0)}
    return info or None


def parse_checkpoint_note(comment, wf_uid, step):
    """The checkpoint copy a job comment names: {'sha', 'rel', 'info'}, or None. Only this step's own store
    counts (kaos-ckpt/<workflow uid>/<step>/<seq>.tar), and the sha must look like one."""
    import re
    m = dict(re.findall(r'(?:^|\s)(kaos-ckpt|kaos-ckpt-path)=(\S+)', comment or ''))
    sha, rel = m.get('kaos-ckpt', ''), m.get('kaos-ckpt-path', '')
    if not (re.fullmatch('[0-9a-f]{64}', sha)
            and re.fullmatch(r'kaos-ckpt/%s/%s/\d{6}\.tar' % (re.escape(wf_uid), re.escape(step)), rel)):
        return None
    return {'sha': sha, 'rel': rel, 'info': checkpoint_info(comment) or {}}


class View:
    """What one poll tick knows of the site job: an accounting-shaped job, its states, and whether the
    controller's observation supplied it (then the comment and reason may be absent, see Runner.live_of)."""

    def __init__(self, job, st, observed=False, comment=None, reason=''):
        self.job, self.st, self.observed, self.comment, self.reason = job, st, observed, comment, reason


class Runner:
    def __init__(self, cfg, slurm=None, stager=None, reporter=None, logs=None,
                 sleep=time.sleep, mint=None, clock=time.time):
        self.cfg = cfg
        self.sleep = sleep
        self.clock = clock
        self.watch_jid, self.watch_since = None, 0.0     # when this runner began waiting for jid's observation
        self.last_rest, self.last_reason = 0.0, 0.0
        self.slurm = slurm or Slurm(cfg)
        self.stager = stager or staging.Stager(cfg)
        self.reporter = reporter or Reporter(cfg)
        self.logs = logs or LogStreamer(cfg, self.stager, self.reporter, secrets=lambda: (self.wallet,))
        self._mint = mint or (lambda: wallet.mint_wallet(cfg.env))
        self.jid = None
        self.submitted_at = 0.0      # when the current site job was submitted (its queue time starts here)
        self.replacing = None        # the queued job being replaced, noted into the next report
        self.wallet, self.wallet_exp = '', 0.0
        self.staged, self.stage_tried = {}, False   # input name -> its resident path on the site ('' = the job stages it)
        self.resubmits = 0
        self.lost_hosts = 0
        self.reported = None
        self.starts = {}          # site job id -> the start Slurm reported for it at any poll (kept for the end report)
        self.reported_info = 'null'   # the last checkpoint report sent (json), so a change is sent once
        self.pending_reason = None
        self.hosted = None
        self.job_limit_min = None    # the time limit this runner submitted the site job with
        self.saved_host_noted = False   # printed once: a resumable host waits for its job's end (AC-73)
        self.last_role = 0.0         # when a waiting guest last read the controller's plan for it
        self.host_job_open = False   # the terminal report was made while the host's Slurm job still ran

    # ------------------------------------------------------------------ submit

    def submit(self):
        cfg = self.cfg
        hpc = cfg.hpc_class()
        first = not self.stage_tried
        if first:
            # Once per runner: a resubmit reuses the staged copy (or the in-job fallback) instead of
            # spending the staging budget again.
            self.stage_tried = True
            cfg.check_job()
            self.prepare_handback()
            for inp in cfg.inputs:
                info = {}
                self.staged[inp['name']] = staging.stage_dataset(cfg, inp, self.stager, self._mint, info=info)
                self.reporter.note_staging(info)    # carried by every later report; the follower prints it
        reg = ''
        try:
            r = urllib.request.Request(
                'http://metadata.google.internal/computeMetadata/v1/instance/'
                'service-accounts/default/token', headers={'Metadata-Flavor': 'Google'})
            reg = json.load(urllib.request.urlopen(r, timeout=5)).get('access_token', '')
        except Exception as e:
            print('no registry token from metadata server (anonymous pull):', e, flush=True)
        # After the token is minted, as late as possible: the layers move now, the job pulls nothing.
        image_staged = imagemod.stage_image(cfg.image, reg, self.stager, strict=first)
        batch, env, commits, job_limit = self.site_job(reg, image_staged)
        ident = cfg.identity()
        class_qos = hpc.get('qos') or 'default'
        hosting = cfg.sharing and int(hpc.get('gpus') or 0) > 0
        self.job_limit_min = job_limit
        body = {'job': {'name': cfg.job_site_name, 'partition': hpc['partition'],
                        'account': ident['account'],
                        'qos': cfg.qos or class_qos,
                        'time_limit': job_limit,
                        'current_working_directory': ident['home'],
                        'environment': env, 'tasks': 1, 'nodes': '1'},
                'script': batch}
        # The leases: every input's commit (above), and kaos-image= for the staged image tar (D-62).
        lease = list(commits)
        # AC-65: always. The SIF has the tar's key, so the lease also keeps the SIF (staged or not) from the
        # stager's cleanup while this job queues or runs.
        lease.append('kaos-image=' + imagemod.image_key(cfg.image))
        lease.append('kaos-ckpt-lease=%s/%s' % (cfg.wf_uid, cfg.step))   # a queued continuation keeps its copies (D-75)
        body['job']['comment'] = ' '.join(lease)
        if cfg.sharing and int(hpc.get('gpus') or 0) > 0:
            # PRD-1609 offer window (PRD-HPC-1461 N-12): a sharing job starts OFFER_S late, so a
            # running host of the same scope can take it even when free nodes would start it at once.
            # Slurm makes it eligible by itself afterwards: nothing has to release it.
            # D-70: a guest the controller planned onto a host's node is not offered but deferred, far enough
            # ahead that it never starts on a node of its own; its host takes it, or the controller plans it
            # again and this runner submits it as a host (poll_loop, replan_guest).
            window = GUEST_HOLD_S if cfg.share_role == 'guest' else OFFER_S
            body['job']['begin_time'] = {'set': True, 'number': int(time.time()) + window}
        if not hosting:
            # PRD-HPC-1461 D-37: requeue lets the site put a pre-empted job back in the queue with
            # its ID and place (users cannot requeue their own jobs on MeluXina).
            body['job']['requeue'] = True
        if cfg.resumable_lead and not hosting:
            # D-44 (amended 2026-11-05): only a resumable step is told to save. USR1 goes to the batch
            # shell its lead before the wall time, which passes it to the step. A host's batch shell is the
            # slot manager, which would die of it: on a shared node the slot manager signals each step itself
            # (AC-73), the host's own step included, by its own limit and by the host job's end.
            body['job'].update({'kill_warning_signal': 'USR1', 'kill_warning_flags': ['BATCH_JOB'],
                                'kill_warning_delay': {'set': True, 'number': cfg.resumable_lead * 60}})
        if cfg.reservation:
            # Only this job's own submit names the reservation: a guest is a job of its own, in the
            # lane it was given, and never rides on the host's.
            body['job']['reservation'] = cfg.reservation
        resp = self.submit_in_lane(body, class_qos)
        j = resp.get('job_id')
        print('submitted job', j, 'errors:', resp.get('errors'), flush=True)
        if not j:
            settings.fail_start('job submit returned no job id', 1)
        # A resubmit replaces the site job of the same attempt: the attempt does not end.
        self.reporter.log_offset = None
        self.reporter.gpu_util = None
        if self.replacing:
            # The replaced job's record rides in this report, so the new job id and the loss are written together.
            self.reporter.note_replacement(str(j), **self.replacing)
            self.replacing = None
        self.submitted_at = time.time()
        self.reporter.report(siteJobID=str(j), state='PENDING')
        return j

    def site_job(self, reg, image_staged):
        """The site job of this attempt as it is submitted: (job script, Slurm environment, dataset leases, job
        time limit in minutes). The step's inputs and its context are in the environment for a job alone on its
        node, and in the script (export lines) for a shared node, where a host can read a waiting job's script
        but never its environment. reg: the registry token; image_staged: the staged image tar ('' = none)."""
        cfg = self.cfg
        hpc = cfg.hpc_class()
        ident = cfg.identity()
        # Nothing of a reference that does not parse is ever written into a script; the registry token only
        # goes into the job when the reference's host is one it may be presented to.
        problem = image_problem(cfg.image)
        if problem:
            raise ValueError('the step image %r cannot be used: %s' % (cfg.image, problem))
        if not registry_token_host(cfg.image.partition('/')[0]):
            reg = ''
        batch = '\n'.join(jobscript.batch_lines(cfg.scope if cfg.sharing else '-', ident['scratch']))
        env = ['PATH=/usr/bin:/bin:/usr/local/bin', 'HOME=' + ident['home'],
               'USER=' + ident['user'], 'IMAGE_REF=' + cfg.image, 'REG_TOKEN=' + reg,
               'HPC_GPUS=' + str(hpc.get('gpus') or 0),
               'STEP_CMD=' + cfg.command, 'STEP_WORKDIR=' + wallet.fetch_workdir(cfg.image, reg),
               'WF_UID=' + cfg.wf_uid, 'STEP_NAME=' + cfg.step,
               # The step's inputs, one word each: name|repository|commit|resident path ('' = stage in the job)|owner
               # ('' = the project's own dataset; a dataset another project shares is never staged in the job).
               'KAOS_INPUTS=' + ' '.join('|'.join((i['name'], i['repo'], i['commit'], self.staged.get(i['name']) or '', i['owner']))
                                         for i in cfg.inputs),
               'IMAGE_STAGED=' + image_staged,
               'KAOS_ATTEMPT=%d' % cfg.attempt,
               # The step context, exactly as the job carries it: the one value the step receives.
               CONTEXT_ENV + '=' + cfg.context]
        if cfg.resumable_lead:
            env.append('KAOS_RESUMABLE=1')
            # AC-73: a host reads a waiting guest's lead from its script (a step on a shared node is signalled
            # by the slot manager, not by Slurm).
            env.append('KAOS_RESUMABLE_LEAD_MIN=%d' % cfg.resumable_lead)
        # D-72: the idle watchdog the job script starts beside the step (its window; 0 = none).
        env.append('KAOS_IDLE_TIMEOUT_MIN=%d' % cfg.idle_timeout)
        if cfg.idle_timeout > 0:
            env.append('IDLEWATCH_B64=' + payloads.b64(payloads.IDLEWATCH))
        if cfg.resume:
            env += ['KAOS_RESUME_REF=' + cfg.resume[0], 'KAOS_RESUME_PREFIX=' + cfg.resume[1]]
        # D-74, D-75: the checkpoint copier every job carries (the step's KAOS_CHECKPOINT_DIR). Quota ids: the
        # project's Lustre project id is the digits of its account.
        env.append('REDACT_B64=' + payloads.b64(payloads.REDACT))     # the job's failure tail is redacted with it
        env += ['CKPT_B64=' + payloads.b64(payloads.CKPT), 'KAOS_CKPT_BUDGET_PCT=%d' % cfg.ckpt_budget_pct,
                'KAOS_PROJID=' + ''.join(c for c in ident['account'] if c.isdigit())]
        env.append('KAOS_TIME_LIMIT_S=%d' % (cfg.time_limit * 60))   # the copier keeps the last minutes for the final copy
        every = checkpoint_every_seconds(cfg.context)    # the run form's <step>-checkpoint-every, from the step context
        if every:
            env.append('KAOS_CKPT_EVERY_S=%d' % every)
        if cfg.env.get('KAOS_LIB_SUPPORT'):      # the one variable a job's spec.env may hand to the step
            env.append('KAOS_LIB_SUPPORT=' + cfg.env['KAOS_LIB_SUPPORT'])
        w = self.wallet      # minted by prepare_handback, before anything was staged or cancelled
        # Public endpoints only, short-lived bearer only (T-03). MLflow's client honors MLFLOW_TRACKING_TOKEN
        # natively; the stage-in, the result hand-back and any in-step lakeFS API calls ride the same bearer.
        if cfg.mlflow_url:
            env += ['MLFLOW_TRACKING_URI=' + cfg.mlflow_url, 'MLFLOW_TRACKING_TOKEN=' + w]
        env += ['LAKEFS_ENDPOINT=' + cfg.lakefs_url, 'LAKEFS_BEARER_TOKEN=' + w,
                'KAOS_OUTPUTS_REPO=' + cfg.outputs_repo,     # for the result hand-back only
                'STAGEIN_B64=' + payloads.b64(payloads.STAGEIN),
                'HANDBACK_B64=' + payloads.b64(payloads.HANDBACK)]
        # Leases (the stager never evicts what a queued or running job names in its comment, squeue %k):
        # kaos-commit= for every input's commit (PRD-1541 D-06). fail() keeps them when the comment is unreadable.
        commits = ['kaos-commit=' + i['commit'] for i in cfg.inputs]
        if commits:
            env.append('KAOS_LEASES=' + ' '.join(commits))
        step_limit = cfg.time_limit
        job_limit = step_limit
        hosting = cfg.sharing and int(hpc.get('gpus') or 0) > 0
        if hosting:
            # F-03: the batch step becomes the host (slot manager); one slot per GPU.
            env += ['KAOS_SLOTS_B64=' + payloads.b64(payloads.SLOTS),
                    'KAOS_SCOPE=' + cfg.scope, 'KAOS_SCR=' + ident['scratch'],
                    'KAOS_STEP_LIMIT_MIN=%d' % step_limit]
            # A host can only take steps that end before its own time limit, so with equal limits
            # nothing would ever fit: the job asks Slurm for twice its step's time, capped by the
            # class's maximum (partition/QOS). Every step still runs with its own limit, and the
            # host releases the node 2 minutes after its last step. No known maximum: no padding (a
            # request above the QOS cap would pend forever).
            cap = cfg.lane_cap(hpc)
            # AC-73: a resumable host's job is not padded: its guests are resumable steps that fit by their lead,
            # and the job must end soon after its own step (it is reported when its job ends).
            if cap and not cfg.resumable_lead:
                job_limit = max(step_limit, min(2 * step_limit, cap))
            if cfg.share_job_limit:
                job_limit = cfg.share_job_limit      # a guest planned again as a host: the controller's figure
            if self.wallet_exp:
                env.append('KAOS_TOKEN_EXP=%d' % self.wallet_exp)   # the host's fit rule (D-07)
        if cfg.sharing:
            batch, env = inputs_in_script(batch, env)
        return batch, env, commits, job_limit

    def submit_in_lane(self, body, class_qos):
        """POST the job in its lane. A site that refuses the lane (QoS limit, reservation, a limit above
        the lane's maximum) gets the same job once more on the class's own QoS with no reservation: a
        fast lane is opportunistic and never fails a step. Reports the QoS actually used."""
        cfg = self.cfg
        lane = bool(cfg.reservation or (cfg.qos and cfg.qos != class_qos))
        why, resp = None, {}
        try:
            resp = self.slurm.submit(body)
            if not resp.get('job_id') and lane and lane_refusal(resp.get('errors')):
                why = resp.get('errors')
        except Refused as e:
            if not (lane and lane_refusal(e)):
                self.fail_submit(e)
            why = e
        if why is not None:
            print('lane qos=%s reservation=%s refused by the site (%s): submitting again on QoS %s '
                  'without a reservation' % (body['job']['qos'], cfg.reservation or '-', why, class_qos), flush=True)
            body['job']['qos'] = class_qos
            body['job'].pop('reservation', None)
            try:
                resp = self.slurm.submit(body)
            except Refused as e:
                self.fail_submit(e)
        self.reporter.qos = body['job']['qos']
        self.reporter.reservation = body['job'].get('reservation')
        return resp

    def fail_submit(self, err):
        """The site refused the job for good. A refusal for its submit cap (AC-39) is a clear terminal report,
        reason SubmitLimit, naming the cap and the count; any other refusal still fails the start loudly."""
        if not submit_limit_refusal(err):
            settings.fail_start('job submit refused: %s' % err, 1)
        msg = self.submit_limit_message(err)
        print('ERROR: SubmitLimit:', msg, flush=True)
        self.write_termination('SubmitLimit: ' + msg)
        self.reporter.report(must=True, state='FAILED', exitCode=SUBMIT_LIMIT_EXIT, terminal=True,
                             endTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), message=msg[:500])
        sys.exit(0)   # reported: the pod ends cleanly, the controller ends the job SubmitLimit

    def prepare_handback(self):
        """Everything the way back needs, found first: the lakeFS endpoint, the app, and the data-plane token (minted
        here, used by the site job). Before any staging, and before a resubmit cancels the queued job: a run that
        cannot hand its result back ends the attempt with nothing staged, submitted or cancelled."""
        self.check_handback()
        w = self.wallet = self._mint()
        self.wallet_exp = wallet.wallet_exp(w)
        if not w:
            self.fail_handback('no data-plane token could be minted for this job')

    def check_handback(self):
        """A step must be able to hand its result back, or it can never end well (PRD kaos#1911 CON-04: fail
        closed, before GPU). The way back is the hand-off branch of the app's outputs repository in lakeFS."""
        missing = [what for what, have in (('the project\'s lakeFS endpoint', self.cfg.lakefs_url),
                                           ('the app\'s outputs repository (the job names no app)', self.cfg.outputs_repo)) if not have]
        if missing:
            self.fail_handback('the job has no ' + ' and no '.join(missing))

    def fail_handback(self, why):
        """The run has no way to hand a result back: one terminal report (FAILED, exit 243, reason
        ResultNotHandedBack) and nothing is submitted to the site."""
        msg = ('the step\'s result could not be handed back from the site: %s. Nothing was submitted to the site: a '
               'step that cannot hand back its result cannot end well' % why)
        print('ERROR: ResultNotHandedBack:', msg, flush=True)
        self.write_termination('ResultNotHandedBack: ' + msg)
        self.reporter.report(must=True, state='FAILED', exitCode=RESULT_NOT_HANDED_BACK_EXIT, terminal=True,
                             endTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), message=msg[:500])
        sys.exit(0)   # reported: the pod ends cleanly, the controller ends the job ResultNotHandedBack

    def submit_limit_message(self, err):
        """The sentence of a SubmitLimit failure: the site's cap (from the profile) and how many jobs it
        counts now (read from the site; left out when the site cannot be asked)."""
        cap = self.cfg.max_submit_jobs
        try:
            count = self.slurm.active_count(self.cfg.identity()['user'])
        except Exception:
            count = None
        head = 'the site accepts at most %s queued or running jobs per user' % (cap or 'a fixed number of')
        if count is not None:
            head += ' and %d are already queued or running' % count
        return ('%s; a new job is not accepted until some finish. Resubmit when the count is below the cap '
                '(site said: %s)' % (head, str(err)[:200]))

    def resubmit(self, why, queue_lost=None):
        """Replace the site job of this attempt (cancel, then submit a fresh one). queue_lost is the reason
        (TokenRefresh, HostLost, Replanned) when the old job was still queued: its place in the queue is lost,
        and the replacement is recorded in status.run.queueReplaced (PRD-HPC-1461 AC-35)."""
        print(why, flush=True)
        if queue_lost and self.jid:
            self.replacing = {'old': str(self.jid), 'reason': queue_lost,
                              'queued': max(0, int(time.time() - self.submitted_at)) if self.submitted_at else 0}
        self.cfg.check_job()             # before the cancel: a job that cannot be replaced is not cancelled for it
        self.prepare_handback()          # likewise: no token, no cancel
        self.logs.finish()               # what the old job wrote, before its offset is replaced
        self.slurm.cancel(self.jid)
        self.jid = self.submit()
        self.logs.attach(self.jid, 0)
        self.pending_reason = None
        self.reported = None
        self.hosted = None

    def fresh_wallet(self):
        """The wallet minted at submit lives 12 h; a long job ends after that. Outputs and the
        checkpoint check need a valid one, so a wallet about to expire is minted again."""
        if not self.wallet or time.time() > self.wallet_exp - 300:
            w = self._mint()
            if w:
                self.wallet, self.wallet_exp = w, wallet.wallet_exp(w)

    # ------------------------------------------------------------------ outputs and checkpoints

    def handoff(self, method, path):
        """One call on the hand-off branch of the app's outputs repository (payload/handback.py writes there)."""
        base = '%s/api/v1/repositories/%s/branches/hpc-%s-%s' % (
            self.cfg.lakefs_url, urllib.parse.quote(self.cfg.outputs_repo, safe=''), self.cfg.wf_uid, self.cfg.step)
        return urllib.request.urlopen(urllib.request.Request(
            base + path, method=method, headers={'Authorization': 'Bearer ' + self.wallet}), timeout=60).read()

    def handed_back(self, name):
        """The bytes of one object the job handed back for THIS site job, or None when it is not there. Anything
        but a clear "not there" is asked again, then it is ResultNotHandedBack: never "no result"."""
        q = urllib.parse.urlencode({'path': 'hpc-outputs/%s/%s/%s/%s' % (self.cfg.wf_uid, self.cfg.step, self.jid, name)})
        err = None
        for n in range(RESULT_FETCH_TRIES):
            try:
                return self.handoff('GET', '/objects?' + q)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                err = 'lakeFS answered HTTP %d' % e.code
            except Exception as e:
                err = str(e)
            print('result: %s not read (%s)%s' % (name, err, ', asking again' if n < RESULT_FETCH_TRIES - 1 else ''), flush=True)
            if n < RESULT_FETCH_TRIES - 1:
                self.sleep(RESULT_FETCH_WAIT_S)
        raise result.ResultNotHandedBack('the step ended well, but its result could not be fetched from lakeFS after %d '
                                         'tries (%s). No result was published' % (RESULT_FETCH_TRIES, err))

    def fetch_result(self):
        """The result of a step that ended well, as the text for status.result (wire form). The job handed back
        the result file's bytes, or the record that the step wrote none (payload/handback.py); the bytes are
        checked and written again here. Raises ResultNotHandedBack when the step wrote no result or nothing
        arrived, InvalidResult when the file is not a result: the job then fails. A step that ended well always
        has a result; nothing stands in for a missing one (PRD kaos#1911 CON-04)."""
        self.fresh_wallet()
        if not self.wallet:
            raise result.ResultNotHandedBack('the step ended well, but no data-plane token could be minted to fetch its '
                                             'result. No result was published')
        raw = self.handed_back('kaos-result.json')
        if raw is not None:
            return result.check(raw)
        if self.handed_back('kaos-result.absent') is None:
            raise result.ResultNotHandedBack(
                'the step ended well, but the job handed back neither a result nor the record that the step wrote '
                'none (site job %s). No result was published' % self.jid)
        raise result.ResultNotHandedBack(
            'the step ended well but wrote no result (%s): a step hands back its result before it ends '
            '(kaos_lib.step.result()). No result was published' % result.RESULT_FILE)

    def drop_handoff(self):
        """Delete the hand-off branch, once the terminal report was accepted. Never before: a runner that dies
        between the fetch and the report leaves the branch, and the next runner of the attempt reads the result
        from it. A branch left behind is harmless (the result's path names the site job) and goes with the next
        terminal report of the same step, which deletes the whole branch."""
        try:
            self.handoff('DELETE', '')
        except Exception as e:
            print('hand-off branch not deleted:', e, flush=True)

    def handback_failure(self):
        """What the job's hand-back program said in the job log before the job ended 243, as the sentence of the
        report; a general one when the log does not show it."""
        said = [line.split(HANDBACK_PREFIX, 1)[1] for line in self.logs.tail if HANDBACK_PREFIX in line]
        return ('the step ended well, but the job on the site could not hand back its result: %s. No result was published'
                % (said[-1].strip() if said else 'the job log says why'))

    def checkpoint_of(self, job, tries=4):
        """The newest checkpoint copy this job reported ({'sha', 'rel', 'info'}), or None. The copier writes
        kaos-ckpt, kaos-ckpt-path and kaos-ckpt-info into the job comment after each complete copy; accounting
        may lag, so slurmctld is asked too, a few times when the step said it saved (exit 85)."""
        for n in range(tries):
            texts = [comment_of(job)]
            try:
                texts.append(comment_of(self.slurm.live(self.jid)))
            except Exception:
                pass
            for c in texts:
                found = parse_checkpoint_note(c, self.cfg.wf_uid, self.cfg.step)
                if found:
                    return found
            if n < tries - 1:
                self.sleep(20)
                try:
                    job = self.slurm.accounting(self.jid)
                except Exception:
                    pass
        return None

    # ------------------------------------------------------------------ signals

    def on_term(self, signum, frame):
        # SIGTERM is either a real cancel (D-04: the controller sets the cancel-requested annotation on
        # the job BEFORE it deletes this runner Job) or a disruption (eviction, drain, node loss).
        # Real cancel: stop the site job, PENDING or RUNNING, and report it. Anything else, including a
        # job that cannot be read: leave the site job alone and exit without a terminal report, so the
        # replacement pod re-attaches by key.
        cancel = False
        try:
            cancel = self.reporter.cancel_requested() is True
        except Exception:
            pass
        if cancel and self.jid is not None:
            try:
                self.logs.finish()
            except Exception:
                pass
            if self.slurm.cancel(self.jid):
                self.reporter.report(siteJobID=str(self.jid), state='CANCELLED', terminal=True)
                sys.exit(0)
            # The site did not take the cancel: CANCELLED would be a lie. No terminal report: the controller
            # then sweeps (a cancel-only runner keeps trying and reports what the site confirms).
            print('the site job %s could not be cancelled: leaving the confirmation to the controller\'s sweep'
                  % self.jid, flush=True)
            sys.exit(143)
        print('SIGTERM without a cancel request: the site job %s is left alone for the next runner'
              % self.jid, flush=True)
        try:
            self.logs.persist(force=True)    # a restarted runner resumes from here, without duplicates
        except Exception:
            pass
        sys.exit(143)

    # ------------------------------------------------------------------ the attempt

    def adopt(self, job):
        """The site job a previous runner of this attempt submitted, or None. By key: the active job of
        this attempt's name; failing that its finished job in accounting, found by the same NAME (a submit
        whose answer was lost, ended before this runner came back); failing that the job the previous runner
        reported. Never a second submission for the same attempt."""
        run = (job.get('status') or {}).get('run') or {}
        mine = int(run.get('attempt') or 0) == self.cfg.attempt
        prev = run.get('siteJobID') if mine else None
        jid = self.slurm.find_active()
        if jid is not None:
            return str(jid), (int(run.get('logOffset') or 0) if mine and str(jid) == run.get('siteJobID') else 0)
        # Only a job newer than the one already reported counts: that one may have been replaced (cancelled).
        fin = self.slurm.find_finished(after=prev if prev and str(prev).isdigit() else 0)
        if fin is not None:
            fid = str(fin['job_id'])
            print('adopting the finished job %s of this attempt (found by name %s)' % (fid, self.cfg.job_site_name), flush=True)
            return fid, (int(run.get('logOffset') or 0) if fid == prev else 0)
        if prev:
            try:
                rec = self.slurm.accounting(prev)
            except Exception as e:
                rec = {}
                print('previous site job %s unreadable: %s' % (prev, e), flush=True)
            st = (rec.get('state') or {}).get('current') or []
            # A job this runner cancelled for a resubmit is not the attempt's result. The legacy name is the one
            # a runner of the previous release gave the job.
            if rec.get('name') in (self.cfg.job_site_name, self.cfg.legacy_site_name) and st and 'CANCELLED' not in st:
                return str(prev), int(run.get('logOffset') or 0)
        return None, 0

    def adopt_or_submit(self, job):
        """(site job id, log offset, adopted). An unreachable site is waited for, not failed on (AC-28, AC-30):
        every round starts with the adopt step, so a submit the site accepted but did not answer is found again
        instead of being repeated. Real errors (auth, a refused job) end the runner at once."""
        waited, delay = 0, SITE_RETRY_FIRST_S
        while True:
            try:
                jid, offset = self.adopt(job)
                adopted = jid is not None
                if not adopted:
                    jid, offset = self.submit(), 0
            except Exception as e:
                if not is_site_down(e):
                    raise
                if waited >= self.cfg.site_wait_max:
                    settings.fail_start('%s: the site did not answer for %d min (last error: %s)' % (
                        SITE_UNAVAILABLE, waited // 60, str(e)[:300]), 1)
                if waited == 0:
                    self.reporter.pending_reason(SITE_UNAVAILABLE)
                print('the site is unavailable (%s): asking again in %d s (%d s of at most %d)' % (
                    str(e)[:300], delay, waited, self.cfg.site_wait_max), flush=True)
                self.sleep(delay)
                waited, delay = waited + delay, min(delay * 2, SITE_RETRY_MAX_S)
                continue
            if waited:
                self.reporter.pending_reason(None)   # the site answered: the wait is over
            return jid, offset, adopted

    def run(self):
        cfg = self.cfg
        job = self.reporter.get_job()
        if job is None:
            print('the HPCArgoWorkflowJob %s/%s is gone: nothing to run' % (cfg.namespace, cfg.job_name),
                  flush=True)
            return 0
        run = (job.get('status') or {}).get('run') or {}
        if int(run.get('attempt') or 0) == cfg.attempt:
            self.reporter.restore_replacements(run)
            self.reporter.restore_staging(run)
        if int(run.get('attempt') or 0) == cfg.attempt and run.get('terminal') and run.get('hostJobOpen'):
            # A restarted host runner: the step was reported, the host job's end not yet (D-26). Only that is left.
            print('attempt %d reported its step; following its host job to its end' % cfg.attempt, flush=True)
            self.jid, self.host_job_open = str(run['siteJobID']), True
            self.reporter.last_run = run
            self.follow_host_job_end()
            return 0
        if int(run.get('attempt') or 0) == cfg.attempt and run.get('terminal'):
            print('attempt %d already reported its end (%s): nothing to do' % (cfg.attempt, run.get('state')),
                  flush=True)
            return 0
        print('HPC job %s/%s: attempt %d' % (cfg.namespace, cfg.job_name, cfg.attempt), flush=True)
        problem = image_problem(cfg.image)
        if problem:
            return self.end_invalid_image(problem)
        signal.signal(signal.SIGTERM, self.on_term)
        try:
            jid, offset, adopted = self.adopt_or_submit(job)
        except staging.InsufficientQuota as e:
            imagemod.cleanup_images(self.stager)   # AC-65: free what no job names before the next attempt
            return self.end_no_quota(e)
        except staging.StagingMismatch as e:
            return self.end_before_staging('StagingMismatch', STAGING_MISMATCH_EXIT, e)
        except staging.ReservedName as e:
            return self.end_before_staging('ReservedName', RESERVED_NAME_EXIT, e)
        except staging.SharedDatasetUnavailable as e:
            return self.end_before_staging('SharedDatasetUnavailable', SHARED_DATASET_UNAVAILABLE_EXIT, e)
        if adopted:
            print('adopting existing job', jid, flush=True)
        self.jid = jid
        self.logs.attach(self.jid, offset)
        self.logs.start()
        try:
            rc = self.poll_loop()
            self.follow_host_job_end()
            return rc
        finally:
            self.logs.close()
            imagemod.cleanup_images(self.stager)   # AC-65: the job is over: its tar and a superseded SIF go now

    def end_invalid_image(self, problem):
        """The step's image reference cannot be pulled (no registry host, repository, tag or digest): nothing is
        staged and no site job is submitted. The one terminal report says so (exit 239, FAILED); the controller ends
        the job InvalidImage and settles nothing. No credential resubmit can fix a reference."""
        msg = ('the step image %r cannot be pulled: %s. Nothing was submitted to the site. Fix the image the step '
               'names (registry/repository:tag or @sha256 digest)' % (self.cfg.image, problem))
        print('ERROR: InvalidImage:', msg, flush=True)
        self.write_termination('InvalidImage: ' + msg)
        self.reporter.report(must=True, state='FAILED', exitCode=INVALID_IMAGE_EXIT, terminal=True,
                             endTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), message=msg[:500])
        return 0

    def end_no_quota(self, err):
        """Staging does not fit the project's scratch quota (AC-33): no site job was submitted. The one terminal
        report says so (exit code 237, FAILED, a plain message); the controller ends the job InsufficientQuota
        and settles nothing, since no node time was spent."""
        msg = str(err)
        print('ERROR: InsufficientQuota:', msg, flush=True)
        self.write_termination('InsufficientQuota: ' + msg)
        self.reporter.report(must=True, state='FAILED', exitCode=INSUFFICIENT_QUOTA_EXIT, terminal=True,
                             endTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), message=msg[:500])
        return 0

    def end_before_staging(self, reason, code, err):
        """Staging was refused before it began (PRD kaos#1911 F-04): the site's staging tool speaks another staging
        contract than this runner (StagingMismatch, exit code 241), or the dataset holds an object whose name is
        reserved for the platform (ReservedName, exit code 242), or a dataset another project shares cannot be read
        (SharedDatasetUnavailable, exit code 245). Nothing was staged and no site job was submitted.
        The one terminal report says so (FAILED, a plain message); the controller ends the job with the reason and
        settles nothing."""
        msg = str(err)
        print('ERROR: %s:' % reason, msg, flush=True)
        self.write_termination('%s: %s' % (reason, msg))
        self.reporter.report(must=True, state='FAILED', exitCode=code, terminal=True,
                             endTime=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), message=msg[:500])
        return 0

    def cancel_only(self):
        """The orphan sweep's runner (--cancel-only): stop this attempt's site job and report its final state.

        It never submits. It finds the job by key (or the one the dead runner reported), cancels it (the cancel is
        retried), and reports a terminal state only on the site's own word: the job's accounting state, or
        SITE_UNKNOWN once the site has said for SITE_UNKNOWN_S that it does not know the job. When the site does not
        answer within CANCEL_ONLY_BUDGET_S it exits 1 without a report; the controller keeps the booking and starts
        another one."""
        cfg = self.cfg
        job = self.reporter.get_job()
        if job is None:
            print('the HPCArgoWorkflowJob %s/%s is gone: nothing to cancel' % (cfg.namespace, cfg.job_name), flush=True)
            return 0
        run = (job.get('status') or {}).get('run') or {}
        mine = int(run.get('attempt') or 0) == cfg.attempt
        if mine and run.get('terminal'):
            print('attempt %d already reported its end (%s): nothing to cancel' % (cfg.attempt, run.get('state')), flush=True)
            return 0
        prev = str(run['siteJobID']) if mine and run.get('siteJobID') else None
        deadline = self.clock() + CANCEL_ONLY_BUDGET_S
        unknown_since = None
        while True:
            try:
                active = self.slurm.find_active()
                jid = str(active) if active is not None else prev
                if jid is None:
                    # A submit whose answer was lost may have run and ended unreported: settle it from accounting.
                    fin = self.slurm.find_finished()
                    jid = str(fin['job_id']) if fin is not None else None
                if jid is None:
                    return self.report_swept('-', 'NOT_SUBMITTED', None, None, run)
                if active is not None:
                    self.slurm.cancel(jid)
                rec = {}
                try:
                    rec = self.slurm.accounting(jid)
                except urllib.error.HTTPError as e:
                    if e.code != 404:
                        raise
                st = (rec.get('state') or {}).get('current') or []
                if st and any(x in TERMINAL for x in st):
                    rc = ((rec.get('exit_code') or {}).get('return_code') or {}).get('number')
                    return self.report_swept(jid, ','.join(st), rc, rec, run)
                if st or active is not None:
                    unknown_since = None          # the site knows the job: it is not over yet
                else:
                    unknown_since = unknown_since if unknown_since is not None else self.clock()
                    if self.clock() - unknown_since >= SITE_UNKNOWN_S:
                        return self.report_swept(jid, 'SITE_UNKNOWN', None, None, run)
            except SystemExit:
                raise
            except Exception as e:
                print('cancel-only: site error (transient):', e, flush=True)
            if self.clock() >= deadline:
                settings.fail_start('the site did not confirm the end of site job %s within %d min' % (
                    prev or '?', CANCEL_ONLY_BUDGET_S // 60), 1)
            self.sleep(CANCEL_ONLY_POLL_S)

    def report_swept(self, jid, state, rc, rec, run):
        """The one terminal report of a cancel-only runner. Times are the site's; a start the dead runner had
        already reported is kept when the site gives none."""
        start = site_time(rec, 'start') if rec else None
        end = (site_time(rec, 'end') if rec else None) or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self.clock()))
        if not start and state != 'NOT_SUBMITTED':
            start = run.get('startTime')
        print('cancel-only: site job', jid, 'is', state, flush=True)
        self.reporter.report(must=True, siteJobID=jid if jid != '-' else None, state=state, exitCode=rc,
                             startTime=start, endTime=end if state != 'NOT_SUBMITTED' else None, terminal=True)
        return 0

    def poll_loop(self):
        cfg, slurm, reporter = self.cfg, self.slurm, self.reporter
        while True:
            self.sleep(POLL_S)
            try:
                jid = self.jid
                v = self.view()
                if v is None:
                    continue
                job, st = v.job, v.st
                print('state:', st, flush=True)
                share = share_state(comment_of(job))
                info = checkpoint_info(comment_of(job))
                now = (','.join(st), share.get('host'), share.get('slot'))
                info_key = json.dumps(info, sort_keys=True)
                if (now != self.reported or info_key != self.reported_info) and not any(x in TERMINAL for x in st):
                    reporter.report(siteJobID=str(jid), state=now[0], startTime=self.start_of(job),
                                    hostSiteJobID=share.get('host'), checkpoint=info,
                                    slot=int(share['slot']) if (share.get('slot') or '').isdigit() else None)
                    self.reported, self.reported_info = now, info_key
                reason = ''
                if 'PENDING' in st:
                    try:
                        live = self.live_of(v)
                        reason = live.get('state_reason') or ''
                        # The live comment: accounting may lag a host's update.
                        share = share_state(comment_of(live)) or share
                    except Exception:
                        reason = self.pending_reason or ''
                    if reason != self.pending_reason:
                        print('pending reason:', reason, flush=True)
                        self.pending_reason = reason
                        reporter.pending_reason(reason)
                    if share.get('kaos-state') == 'taken' and share != self.hosted:
                        print('running on a shared node: host job', share.get('host'),
                              'slot', share.get('slot'), flush=True)
                        self.hosted = share
                elif 'RUNNING' in st and cfg.sharing:
                    # PRD-1609 F-03: a host keeps its node while its guests run; its own step's result
                    # is in its comment as soon as that step ends.
                    try:
                        if not (v.observed and v.comment is not None):
                            share = share_state(comment_of(slurm.live(jid))) or share
                    except Exception:
                        pass
                if 'PENDING' not in st and self.pending_reason:
                    self.pending_reason = None
                    reporter.pending_reason(None)
                # The job writes its output while it runs (a hosted guest is PENDING in Slurm's eyes
                # while its step runs on the host's node).
                if 'RUNNING' in st or share.get('kaos-state') == 'taken':
                    self.logs.active.set()
                if 'PENDING' in st and share.get('kaos-state') != 'taken' and self.replan_guest():
                    continue
                if ('PENDING' in st and share.get('kaos-state') == 'taken' and share.get('host')
                        and self.lost_hosts < LOST_HOST_RESUBMITS and host_gone(slurm, share['host'])):
                    # PRD-1609: the host died without handing this job back (node failure, crash): it
                    # stays held forever. Start over as a new job - the same cancel + submit as the
                    # wallet resubmit, so no new Slurm call.
                    self.lost_hosts += 1
                    self.logs.active.clear()
                    self.resubmit('host job %s is gone with this step - resubmitting (%d/%d)'
                                  % (share['host'], self.lost_hosts, LOST_HOST_RESUBMITS), 'HostLost')
                    continue
                # A job a host took stays queued (held) while its step runs: never resubmit it. The
                # host checked its token outlives the step (D-07).
                if ('PENDING' in st and share.get('kaos-state') != 'taken' and self.wallet_exp
                        and time.time() > self.wallet_exp - wallet.WALLET_MARGIN_S):
                    self.resubmit('wallet expires within %d min and the job is still queued - resubmitting '
                                  'with a fresh wallet (queue position is lost, credentials are not)'
                                  % (wallet.WALLET_MARGIN_S // 60), 'TokenRefresh')
                    continue
                if self.saved_host_waits(st, share):
                    continue
                if any(x in TERMINAL for x in st) or ('RUNNING' in st and share.get('kaos-state') == 'done'):
                    rc = ((job.get('exit_code') or {}).get('return_code') or {}).get('number')
                    # A host's own step is over while its Slurm job still runs (it waits for more guests):
                    # the step is reported now, the job's end afterwards (follow_host_job_end).
                    self.host_job_open = (not any(x in TERMINAL for x in st) and not share.get('host'))
                    print('terminal:', st, 'exit:', rc, flush=True)
                    print('job comment:', job.get('comment'), flush=True)
                    if share.get('kaos-state') == 'done':
                        # PRD-1609 D-04: a host ran this step on a shared node and then cancelled this
                        # job; the step's result is in the comment.
                        rc = int(share['rc']) if share.get('rc', '').isdigit() else 1
                        if rc == 0 and share.get('reason') in ('memkill', 'GPUViolation'):
                            rc = 1      # a step the slot manager stopped did not succeed, whatever exit code it left
                        if share.get('reason') == 'GPUViolation':
                            rc = GPU_VIOLATION_EXIT   # its own exit code, so the controller says why (not SiteFAILED)
                        print('ran on a shared node: host job', share.get('host'),
                              'exit:', share.get('rc'), 'reason:', share.get('reason'), flush=True)
                        st = ['COMPLETED'] if rc == 0 else ['FAILED']
                        if rc != 0 and share.get('reason') == 'TIMEOUT':
                            # AC-72 / D-67: the step's own time limit stopped it on the shared node (the slot
                            # manager says so): a TIMEOUT like a job alone, so a learned limit is retried.
                            st = ['TIMEOUT']
                    if 'FAILED' in st and rc == 231 and self.resubmits < PULL_RESUBMITS:
                        # A cache-missing pull after a LONG queue wait runs with the registry token
                        # minted at submit time - GCP tokens live 1h, so it can be stale (live job
                        # 5143859: 3.5h queue -> 231). Resubmitting mints everything fresh.
                        self.resubmits += 1
                        self.logs.active.clear()
                        self.resubmit('pull failed - resubmitting with fresh credentials (%d/%d)'
                                      % (self.resubmits, PULL_RESUBMITS))
                        continue
                    return self.end(job, st, rc, share)
            except SystemExit:
                raise
            except Exception as e:
                print('poll error (transient):', e, flush=True)

    def saved_host_waits(self, st, share):
        """AC-73: a resumable host whose own step saved (exit 85) while its Slurm job still holds the node for
        guests is NOT reported yet: the controller continues a checkpointed step at once and releases its
        booking, but the site charges the node until the job ends. One report, at the job's end, settles the
        whole node time (the resumable host's job is not padded, so that end is close, test_core/test_slot_resumable)."""
        waits = (self.cfg.resumable_lead and self.cfg.sharing and 'RUNNING' in st and not any(x in TERMINAL for x in st)
                 and share.get('kaos-state') == 'done' and not share.get('host')
                 and share.get('rc') == str(CHECKPOINT_EXIT))
        if waits and not self.saved_host_noted:
            self.saved_host_noted = True
            print('the step saved (exit 85) on a shared node: reporting when its Slurm job ends, so the node time '
                  'it held for guests is settled', flush=True)
        return waits
    def replan_guest(self):
        """D-70: a deferred guest that no host took asks the controller's plan every ROLE_POLL_S. The controller plans
        it again when its host ended, did not start, no longer fits or did not take it; when the plan now says host,
        this job is replaced by one that starts at once in the lane the plan names (the same cancel and submit as the
        wallet resubmit). True when it did. Nothing is read, nothing changes, while the job is not a waiting guest."""
        cfg = self.cfg
        if cfg.share_role != 'guest' or self.clock() - self.last_role < ROLE_POLL_S:
            return False
        self.last_role = self.clock()
        plan = self.reporter.placement()
        if (((plan or {}).get('share') or {}).get('role')) != 'host':
            return False
        cfg.apply_plan(plan)
        self.logs.active.clear()
        self.resubmit('the controller planned this step again as a host (lane %s) - submitting it to start on a node '
                      'of its own' % (cfg.qos or 'standard'), 'Replanned')
        return True

    def follow_host_job_end(self):
        """A shared-node host settles from its Slurm job, not its step: the site charges the whole node
        until the job ends, ~2 minutes after the last step (it waits for guests). The step's result and
        outputs were reported at its end with hostJobOpen (nothing waits for them; the controller keeps the
        host's booking held, D-26); this follows the job until it is terminal, bounded by its time limit plus
        a margin, and resends the same terminal report with hostJobEndTime, the job's own end, and hostJobOpen
        cleared. endTime stays the step's end: a learned limit is the step's run time, not the node's (D-67).
        A job whose end cannot be read in that time is closed at the latest end Slurm allows it: its start
        plus its time limit, else now (conservative, never below what the site charged)."""
        if not self.host_job_open or not self.reporter.last_run:
            return
        limit_s = (self.job_limit_min or self.cfg.time_limit * 2) * 60
        deadline = time.time() + limit_s + HOST_END_MARGIN_S
        print('following host job', self.jid, 'until it ends (settlement counts the whole node)', flush=True)
        end_time = None
        while time.time() < deadline and end_time is None:
            self.sleep(POLL_S)
            try:
                job = self.slurm.accounting(self.jid)
            except Exception as e:
                print('host job end: poll error (transient):', e, flush=True)
                continue
            if any(x in TERMINAL for x in (job.get('state') or {}).get('current') or []):
                end_time = site_time(job, 'end') or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                print('host job ended at', end_time, flush=True)
        run = {k: v for k, v in self.reporter.last_run.items() if k not in ('attempt', 'logOffset', 'gpuUtilization')}
        if end_time is None:
            start = parse_time(run.get('startTime'))
            latest = start + limit_s if start is not None else time.time()
            end_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(min(latest, time.time())))
            print('host job did not end within its time limit + margin: closed at', end_time,
                  '(its start plus its time limit)', flush=True)
        run.update(hostJobEndTime=end_time, hostJobOpen=None)
        self.reporter.report(must=True, **run)

    def view(self):
        """This tick's reading of the site job, or None to skip the tick (D-36).

        Fresh controller observation of this job: its state, no REST call (accounting is read only when
        the job is over, the host reported it is done, or the state changed and the start time is wanted).
        Otherwise the old path: accounting every tick; but with no observation yet the runner waits
        STALE_GRACE_S for the controller's first poll, and a stale one is replaced by REST every STALE_POLL_S."""
        now = self.clock()
        if self.watch_jid != self.jid:
            self.watch_jid, self.watch_since = self.jid, now
        obs = self.reporter.site_observation()
        mine = obs is not None and str(obs.get('jobID')) == str(self.jid)
        seen = parse_time(obs.get('observedAt')) if mine else None
        if mine and seen is not None and now - seen <= STALE_INTERVALS * (obs.get('intervalSeconds') or DEFAULT_INTERVAL_S):
            if obs.get('state'):
                return self.observed_view(obs)
            # The site no longer lists the job: its record is in accounting.
            return self.rest_view()
        if not mine and now - self.watch_since < STALE_GRACE_S:
            return None
        if now - self.last_rest < STALE_POLL_S:
            return None
        print('no fresh site observation: polling the site myself', flush=True)
        self.last_rest = now
        return self.rest_view()

    def rest_view(self):
        job = self.slurm.accounting(self.jid)
        return View(job, (job.get('state') or {}).get('current') or [])

    def observed_view(self, obs):
        """A View from the controller's observation; the accounting record only where it is needed."""
        st = [x for x in obs['state'].split(',') if x]
        comment = obs.get('comment')
        share = share_state(comment or '')
        job = {'state': {'current': st}, 'comment': {'job': comment or ''}}
        over = any(x in TERMINAL for x in st) or ('RUNNING' in st and share.get('kaos-state') == 'done')
        changed = (','.join(st), share.get('host'), share.get('slot')) != self.reported
        if over or changed:
            try:
                job = self.slurm.accounting(self.jid)
                st = (job.get('state') or {}).get('current') or st
                if comment and not comment_of(job):
                    # Accounting lags the host's newest comment: keep the controller's fresher one.
                    job = dict(job, comment={'job': comment})
            except Exception:
                if over:
                    raise       # the end needs its record: ask again next tick
        return View(job, st, True, comment, obs.get('pendingReason') or '')

    def live_of(self, v):
        """slurmctld's view of a pending job ({state_reason, comment}). From the observation when it
        carries what is needed; otherwise the job is asked: always on the old path, for the comment of a
        sharing job (a host writes into it), and for a missing reason every REASON_POLL_S."""
        if not v.observed:
            return self.slurm.live(self.jid)
        now = self.clock()
        reason_due = not v.reason and now - self.last_reason >= REASON_POLL_S
        if (self.cfg.sharing and v.comment is None) or reason_due:
            if reason_due:
                self.last_reason = now
            return self.slurm.live(self.jid)
        return {'state_reason': v.reason or self.pending_reason or '', 'comment': v.comment or ''}

    def start_of(self, job):
        """The start Slurm reported for the current site job: this record's, else the last one an earlier poll
        saw. The end report rewrites every status.run field, so a record without a start (a host's comment-only
        view, accounting that lags) must not erase a start the controller already had: it settles from it
        (live 2026-11-08, job 5435560: a failed host settled 0 node-minutes for 11 minutes of GPU node)."""
        key = str(self.jid)
        seen = site_time(job, 'start')
        # A queued job's start is Slurm's estimate (or NO_VAL), not a start: only a job that has begun is remembered.
        began = not ({'PENDING', 'CONFIGURING'} & set((job.get('state') or {}).get('current') or []))
        if seen and began:
            self.starts[key] = seen
        return self.starts.get(key)

    def end(self, job, st, rc, share=None):
        """The attempt's site job is over: its last log lines, then the one terminal report. The runner
        exits 0 after it (the controller decides what follows: success, failure or a continuation)."""
        jid = self.jid
        self.logs.finish()
        share = dict(share or {})
        if self.hosted and not share.get('slot') and self.hosted.get('host') == share.get('host'):
            share['slot'] = self.hosted.get('slot')   # the done comment drops the slot the taken one had
        # The end is Slurm's, else when this runner saw the job over: the controller settles the
        # attempt from it and must never be left to guess.
        end_time = site_time(job, 'end') or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time.time()))
        start = self.start_of(job)
        if not start and share.get('host') and (share.get('start') or '').isdigit():
            # A guest's own job never starts: its step's start is the one its host wrote (D-67 samples).
            start = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(int(share['start'])))
        fields = dict(siteJobID=str(jid), state=','.join(st), exitCode=rc,
                      startTime=start, endTime=end_time, terminal=True,
                      # A guest names its host (parsed from the job comment) so the controller
                      # knows the host's booking covers it.
                      hostSiteJobID=share.get('host'),
                      slot=int(share['slot']) if (share.get('slot') or '').isdigit() else None,
                      # D-26: a host's step is over but its Slurm job still holds the node for guests; the
                      # controller keeps the booking held until follow_host_job_end reports the job's end.
                      hostJobOpen=True if self.host_job_open else None)
        if rc == IDLE_EXIT and 'COMPLETED' not in st:
            # D-72: the job ended a hung step; the controller records Failed/Idle and, in its event, the
            # last lines the step wrote (printed above from the same log).
            fields['lastLog'] = self.logs.tail_text() or None
        recoverable = (rc in (CHECKPOINT_EXIT, IDLE_EXIT) or any(x in RECOVERABLE for x in st)) \
            and not any(x in st for x in ('COMPLETED', 'CANCELLED'))
        if recoverable:
            # D-75: every end the step did not cause (and the step's own exit 85) is reported with the newest
            # checkpoint copy, if the job left one. The controller decides: continue from it, re-run, or end.
            ckpt = self.checkpoint_of(job, 4 if rc == CHECKPOINT_EXIT else 1)
            if ckpt:
                print('the job left a checkpoint copy (%s) - the controller decides whether the step continues'
                      % (ckpt['info'].get('progressText') or ckpt['sha'][:12]), flush=True)
                fields.update(checkpointRef=checkpoint_uri(ckpt), checkpoint=ckpt['info'])
            elif rc == CHECKPOINT_EXIT:
                # The comment is all the runner has: the scratch store is out of its reach (the staging key
                # runs only the stager's fixed commands), so say what is known and what is not.
                print('not continuing the step: it exited 85, but the job comment names no checkpoint copy of this '
                      'step (neither Slurm accounting nor the live job has kaos-ckpt=). If the step log shows '
                      '"checkpoint: copy NNNNNN", the copy exists on scratch and only its entry in the job comment '
                      'is missing; if it does not, no copy was made in time (a longer resumable= lead leaves the '
                      'copy more time)', flush=True)
        if rc == RESULT_NOT_HANDED_BACK_EXIT and 'COMPLETED' not in st:
            fields['message'] = self.handback_failure()[:500]
            print('ERROR: ResultNotHandedBack:', fields['message'], flush=True)
        if 'COMPLETED' in st and not rc:
            # The step ended well: its result goes into the same report, or the job fails saying why. A step
            # that ended any other way publishes nothing, whatever it wrote.
            try:
                handed = self.fetch_result()
            except (result.ResultNotHandedBack, result.InvalidResult) as e:
                lost = isinstance(e, result.ResultNotHandedBack)
                reason = 'ResultNotHandedBack' if lost else 'InvalidResult'
                msg = str(e) if lost else '%s. No result was published' % e
                print('ERROR: %s:' % reason, msg, flush=True)
                self.write_termination('%s: %s' % (reason, msg))
                self.reporter.report(must=True, **dict(
                    fields, state='FAILED', message=msg[:500],
                    exitCode=RESULT_NOT_HANDED_BACK_EXIT if lost else INVALID_RESULT_EXIT))
                self.drop_handoff()
                return 0
            print('result: %d bytes' % len(handed), flush=True)
            self.reporter.report(must=True, result=handed, **fields)
            self.drop_handoff()
            return 0
        self.reporter.report(must=True, **fields)
        return 0

    @staticmethod
    def write_termination(msg):
        try:
            with open('/dev/termination-log', 'w') as f:
                f.write(msg[-3000:])
        except OSError:
            pass


def main(env=None, argv=None):
    argv = sys.argv[1:] if argv is None else argv
    cfg = Settings(env)
    if '--cancel-only' in argv:
        return Runner(cfg).cancel_only()
    return Runner(cfg).run()
