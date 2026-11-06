"""One attempt of one HPCArgoWorkflowJob on the site (PRD-HPC-1461 phase 1, D-58, D-64).

The in-pod waiter's program, minus its admission client: the controller already admitted and booked
this attempt before it created the runner Job. The runner submits the site job (or adopts the one a
previous runner of this attempt submitted), polls it to its end, reports to the job's status, and
exits. A continuation is the controller's decision (D-44): a new attempt gets a new runner Job.
"""

import json
import os
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from . import image as imagemod
from . import jobscript, payloads, settings, staging, wallet
from .logs import LogStreamer
from .reporter import OutputsTooLarge, Reporter, checkpoint_uri
from .sharing import inputs_in_script
from .slurm import (TERMINAL, Refused, Slurm, comment_of, host_gone, share_state, site_time)
from .settings import CHECKPOINT_EXIT, OFFER_S, Settings

POLL_S = 30
# The registry pull token (GCE metadata, 1 h) is minted at submit time and cannot be refreshed inside
# a queued job, so every queue wait longer than an hour costs one failed pull (exit 231) and a
# resubmission. Bounded, not once: a second long wait after the first resubmit must not strand the run.
PULL_RESUBMITS = 3
# PRD-1609: how often this run started over because its host died with it.
LOST_HOST_RESUBMITS = 3


class Runner:
    def __init__(self, cfg, slurm=None, stager=None, reporter=None, logs=None,
                 sleep=time.sleep, mint=None):
        self.cfg = cfg
        self.sleep = sleep
        self.slurm = slurm or Slurm(cfg)
        self.stager = stager or staging.Stager(cfg)
        self.reporter = reporter or Reporter(cfg)
        self.logs = logs or LogStreamer(cfg, self.stager, self.reporter)
        self._mint = mint or (lambda: wallet.mint_wallet(cfg.env))
        self.jid = None
        self.wallet, self.wallet_exp = '', 0.0
        self.staged, self.stage_tried, self.pinned = '', False, ''
        self.resubmits = 0
        self.lost_hosts = 0
        self.reported = None
        self.pending_reason = None
        self.hosted = None

    # ------------------------------------------------------------------ submit

    def submit(self):
        cfg = self.cfg
        hpc = cfg.hpc_class()
        if not self.stage_tried:
            # Once per runner: a resubmit reuses the staged copy (or the in-job fallback) instead of
            # spending the staging budget again.
            self.stage_tried = True
            self.staged, self.pinned = staging.stage_dataset(cfg, self.stager, self._mint)
        reg = ''
        try:
            r = urllib.request.Request(
                'http://metadata.google.internal/computeMetadata/v1/instance/'
                'service-accounts/default/token', headers={'Metadata-Flavor': 'Google'})
            reg = json.load(urllib.request.urlopen(r, timeout=5)).get('access_token', '')
        except Exception as e:
            print('no registry token from metadata server (anonymous pull):', e, flush=True)
        # After the token is minted, as late as possible: the layers move now, the job pulls nothing.
        image_staged = imagemod.stage_image(cfg.image, reg, self.stager)
        ident = cfg.identity()
        batch = '\n'.join(jobscript.batch_lines(cfg.scope if cfg.sharing else '-', ident['scratch']))
        env = ['PATH=/usr/bin:/bin:/usr/local/bin', 'HOME=' + ident['home'],
               'USER=' + ident['user'], 'IMAGE_REF=' + cfg.image, 'REG_TOKEN=' + reg,
               'HPC_GPUS=' + str(hpc.get('gpus') or 0),
               'STEP_CMD=' + cfg.command, 'STEP_WORKDIR=' + wallet.fetch_workdir(cfg.image, reg),
               'WF_UID=' + cfg.wf_uid, 'STEP_NAME=' + cfg.step,
               'STEP_OUTPUTS=' + ','.join(cfg.output_names),
               'DATASET_STAGED=' + self.staged, 'IMAGE_STAGED=' + image_staged,
               'KAOS_ATTEMPT=%d' % cfg.attempt]
        if cfg.resumable_lead:
            env.append('KAOS_RESUMABLE=1')
        if cfg.resume:
            env += ['KAOS_RESUME_REF=' + cfg.resume[0], 'KAOS_RESUME_PREFIX=' + cfg.resume[1]]
        # Same outputs location + run id as the in-cluster copy (outputs-<app>). Only when set: step
        # code reads os.environ.get(..., default), so an empty value would override its own default.
        env += [k + '=' + cfg.env[k] for k in
                ('CHECKPOINT_BUCKET', 'CHECKPOINT_PREFIX', 'KUBECORE_RUN_ID', 'KUBECORE_WORKFLOW_NAME',
                 'KAOS_LIB_SUPPORT')
                if cfg.env.get(k)]
        w = self.wallet = self._mint()
        self.wallet_exp = wallet.wallet_exp(w)
        if w:
            # Public endpoints only, short-lived bearer only (T-03). MLflow's client honors
            # MLFLOW_TRACKING_TOKEN natively; the stage-in and any in-step lakeFS API calls ride the
            # same bearer through oauth2-proxy.
            if cfg.mlflow_url:
                env += ['MLFLOW_TRACKING_URI=' + cfg.mlflow_url, 'MLFLOW_TRACKING_TOKEN=' + w]
            if cfg.lakefs_url:
                repo, ref = cfg.dataset_coords()
                env += ['LAKEFS_ENDPOINT=' + cfg.lakefs_url,
                        'LAKEFS_BEARER_TOKEN=' + w,
                        'DATASET_REPO=' + repo, 'DATASET_REF=' + ref,
                        'DATASET_COMMIT=' + (self.pinned or cfg.pinned_commit()),
                        'OUTPUTS_REPO=' + cfg.outputs_repo, 'OUTPUTS_REF=' + cfg.outputs_ref,
                        'STAGEIN_B64=' + payloads.b64(payloads.STAGEIN),
                        'STAGEOUT_B64=' + payloads.b64(payloads.STAGEOUT)]
                if cfg.resumable_lead:
                    env += ['CKPTOUT_B64=' + payloads.b64(payloads.CKPTOUT),
                            'CKPTIN_B64=' + payloads.b64(payloads.CKPTIN)]
        commit = cfg.pinned_commit()
        if commit:
            env.append('KAOS_COMMIT=' + commit)   # fail() keeps the dataset lease
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
            cap = cfg.class_cap(hpc)
            if cap:
                job_limit = max(step_limit, min(2 * step_limit, cap))
            if self.wallet_exp:
                env.append('KAOS_TOKEN_EXP=%d' % self.wallet_exp)   # the host's fit rule (D-07)
        if cfg.sharing:
            batch, env = inputs_in_script(batch, env)
        body = {'job': {'name': cfg.job_site_name, 'partition': hpc['partition'],
                        'account': ident['account'],
                        'qos': hpc.get('qos') or 'default',
                        'time_limit': job_limit,
                        'current_working_directory': ident['home'],
                        'environment': env, 'tasks': 1, 'nodes': '1'},
                'script': batch}
        # Leases (the stager never evicts what a queued or running job names here, squeue %k):
        # kaos-commit= for the dataset commit (PRD-1541 D-06), kaos-image= for the staged image tar (D-62).
        lease = []
        if commit:
            lease.append('kaos-commit=' + commit)
        if image_staged:
            lease.append('kaos-image=' + imagemod.image_key(cfg.image))
        if lease:
            body['job']['comment'] = ' '.join(lease)
        if cfg.sharing and int(hpc.get('gpus') or 0) > 0:
            # PRD-1609 offer window (PRD-HPC-1461 N-12): a sharing job starts OFFER_S late, so a
            # running host of the same scope can take it even when free nodes would start it at once.
            # Slurm makes it eligible by itself afterwards: nothing has to release it.
            body['job']['begin_time'] = {'set': True, 'number': int(time.time()) + OFFER_S}
        if not hosting:
            # PRD-HPC-1461 D-37: requeue lets the site put a pre-empted job back in the queue with
            # its ID and place (users cannot requeue their own jobs on MeluXina).
            body['job']['requeue'] = True
        if cfg.resumable_lead:
            # D-44 (amended 2026-11-05): only a resumable step is told to save. USR1 goes to the batch
            # shell its lead before the wall time, which passes it to the step. A resumable step
            # never shares a node, so this job's limit is its step's.
            body['job'].update({'kill_warning_signal': 'USR1', 'kill_warning_flags': ['BATCH_JOB'],
                                'kill_warning_delay': {'set': True, 'number': cfg.resumable_lead * 60}})
        try:
            resp = self.slurm.submit(body)
        except Refused as e:
            settings.fail_start('job submit refused: %s' % e, 1)
        j = resp.get('job_id')
        print('submitted job', j, 'errors:', resp.get('errors'), flush=True)
        if not j:
            settings.fail_start('job submit returned no job id', 1)
        # A resubmit replaces the site job of the same attempt: the attempt does not end.
        self.reporter.log_offset = None
        self.reporter.report(siteJobID=str(j), state='PENDING')
        return j

    def resubmit(self, why):
        """Replace the site job of this attempt (cancel, then submit a fresh one)."""
        print(why, flush=True)
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

    def fetch_outputs(self):
        """F-05: the step's staged-out outputs, as {name: JSON text} for status.outputs."""
        base = self.cfg.lakefs_url
        repo = self.cfg.outputs_repo
        names = self.cfg.output_names
        got = {}
        if not (names and base and repo and self.wallet):
            return got
        wf, step = self.cfg.wf_uid, self.cfg.step
        branch = 'hpc-%s-%s' % (wf, step)
        for n in names:
            q = urllib.parse.urlencode({'path': 'hpc-outputs/%s/%s/%s.json' % (wf, step, n)})
            try:
                data = urllib.request.urlopen(urllib.request.Request(
                    base + '/api/v1/repositories/%s/refs/%s/objects?%s' % (repo, branch, q),
                    headers={'Authorization': 'Bearer ' + self.wallet}), timeout=60).read()
                got[n] = data.decode('utf-8')
                print('output', n, ':', len(data), 'bytes', flush=True)
            except Exception as e:
                print('output', n, 'not staged out (default {}):', e, flush=True)
        try:   # the hand-off branch has done its job
            urllib.request.urlopen(urllib.request.Request(
                base + '/api/v1/repositories/%s/branches/%s' % (repo, branch), method='DELETE',
                headers={'Authorization': 'Bearer ' + self.wallet}), timeout=60).read()
        except Exception as e:
            print('hand-off branch', branch, 'not deleted:', e, flush=True)
        return got

    def checkpoint_path(self):
        return 'hpc-checkpoints/%s/%s/attempt-%d/' % (self.cfg.wf_uid, self.cfg.step, self.cfg.attempt)

    def checkpoint_of(self, job):
        """(commit, path) of the checkpoint this job staged out before it exited 85, or None. The batch
        script writes them into the job comment after the upload committed; accounting may lag that,
        so slurmctld is asked too, a few times. Only this attempt's path counts."""
        import re
        for n in range(4):
            texts = [comment_of(job)]
            try:
                texts.append(comment_of(self.slurm.live(self.jid)))
            except Exception:
                pass
            for c in texts:
                m = dict(re.findall(r'(?:^|\s)(kaos-ckpt|kaos-ckpt-path)=(\S+)', c))
                if (re.fullmatch('[0-9a-f]{40,64}', m.get('kaos-ckpt', ''))
                        and m.get('kaos-ckpt-path') == self.checkpoint_path()):
                    return m['kaos-ckpt'], m['kaos-ckpt-path']
            if n < 3:
                self.sleep(20)
                try:
                    job = self.slurm.accounting(self.jid)
                except Exception:
                    pass
        return None

    def checkpoint_committed(self, commit):
        """A4: the checkpoint commit exists in the outputs repository. Asked a few times: an
        unreachable lakeFS is not a missing checkpoint, but it never lets the step continue either."""
        base, repo = self.cfg.lakefs_url, self.cfg.outputs_repo
        if not (base and repo and self.wallet):
            return False
        for n in range(4):
            try:
                c = json.load(urllib.request.urlopen(urllib.request.Request(
                    base + '/api/v1/repositories/%s/commits/%s' % (repo, commit),
                    headers={'Authorization': 'Bearer ' + self.wallet}), timeout=60))
                return c.get('id') == commit
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return False
                print('checkpoint commit check:', e, flush=True)
            except Exception as e:
                print('checkpoint commit check:', e, flush=True)
            self.sleep(15 * (n + 1))
        return False

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
            self.slurm.cancel(self.jid)
            self.reporter.report(siteJobID=str(self.jid), state='CANCELLED', terminal=True)
            sys.exit(0)
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
        this step's name; failing that the job the previous runner reported (it may have ended while no
        runner was watching). Never a second submission for the same attempt."""
        run = (job.get('status') or {}).get('run') or {}
        mine = int(run.get('attempt') or 0) == self.cfg.attempt
        jid = self.slurm.find_active()
        if jid is not None:
            return str(jid), (int(run.get('logOffset') or 0) if mine and str(jid) == run.get('siteJobID') else 0)
        prev = run.get('siteJobID') if mine else None
        if prev:
            try:
                rec = self.slurm.accounting(prev)
            except Exception as e:
                rec = {}
                print('previous site job %s unreadable: %s' % (prev, e), flush=True)
            st = (rec.get('state') or {}).get('current') or []
            # A job this runner cancelled for a resubmit is not the attempt's result.
            if rec.get('name') == self.cfg.job_site_name and st and 'CANCELLED' not in st:
                return str(prev), int(run.get('logOffset') or 0)
        return None, 0

    def run(self):
        cfg = self.cfg
        job = self.reporter.get_job()
        if job is None:
            print('the HPCArgoWorkflowJob %s/%s is gone: nothing to run' % (cfg.namespace, cfg.job_name),
                  flush=True)
            return 0
        run = (job.get('status') or {}).get('run') or {}
        if int(run.get('attempt') or 0) == cfg.attempt and run.get('terminal'):
            print('attempt %d already reported its end (%s): nothing to do' % (cfg.attempt, run.get('state')),
                  flush=True)
            return 0
        print('HPC job %s/%s: attempt %d' % (cfg.namespace, cfg.job_name, cfg.attempt), flush=True)
        signal.signal(signal.SIGTERM, self.on_term)
        jid, offset = self.adopt(job)
        if jid is not None:
            print('adopting existing job', jid, flush=True)
            self.jid = jid
        else:
            self.jid = self.submit()
            offset = 0
        self.logs.attach(self.jid, offset)
        self.logs.start()
        try:
            return self.poll_loop()
        finally:
            self.logs.close()

    def poll_loop(self):
        cfg, slurm, reporter = self.cfg, self.slurm, self.reporter
        while True:
            self.sleep(POLL_S)
            try:
                jid = self.jid
                job = slurm.accounting(jid)
                st = (job.get('state') or {}).get('current') or []
                print('state:', st, flush=True)
                share = share_state(comment_of(job))
                now = (','.join(st), share.get('host'), share.get('slot'))
                if now != self.reported and not any(x in TERMINAL for x in st):
                    reporter.report(siteJobID=str(jid), state=now[0], startTime=site_time(job, 'start'),
                                    hostSiteJobID=share.get('host'),
                                    slot=int(share['slot']) if (share.get('slot') or '').isdigit() else None)
                    self.reported = now
                reason = ''
                if 'PENDING' in st:
                    try:
                        live = slurm.live(jid)
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
                if ('PENDING' in st and share.get('kaos-state') == 'taken' and share.get('host')
                        and self.lost_hosts < LOST_HOST_RESUBMITS and host_gone(slurm, share['host'])):
                    # PRD-1609: the host died without handing this job back (node failure, crash): it
                    # stays held forever. Start over as a new job - the same cancel + submit as the
                    # wallet resubmit, so no new Slurm call.
                    self.lost_hosts += 1
                    self.logs.active.clear()
                    self.resubmit('host job %s is gone with this step - resubmitting (%d/%d)'
                                  % (share['host'], self.lost_hosts, LOST_HOST_RESUBMITS))
                    continue
                # A job a host took stays queued (held) while its step runs: never resubmit it. The
                # host checked its token outlives the step (D-07).
                if ('PENDING' in st and share.get('kaos-state') != 'taken' and self.wallet_exp
                        and time.time() > self.wallet_exp - wallet.WALLET_MARGIN_S):
                    self.resubmit('wallet expires within %d min and the job is still queued - resubmitting '
                                  'with a fresh wallet (queue position is lost, credentials are not)'
                                  % (wallet.WALLET_MARGIN_S // 60))
                    continue
                if any(x in TERMINAL for x in st) or ('RUNNING' in st and share.get('kaos-state') == 'done'):
                    rc = ((job.get('exit_code') or {}).get('return_code') or {}).get('number')
                    print('terminal:', st, 'exit:', rc, flush=True)
                    print('job comment:', job.get('comment'), flush=True)
                    if share.get('kaos-state') == 'done':
                        # PRD-1609 D-04: a host ran this step on a shared node and then cancelled this
                        # job; the step's result is in the comment.
                        rc = int(share['rc']) if share.get('rc', '').isdigit() else 1
                        print('ran on a shared node: host job', share.get('host'),
                              'exit:', share.get('rc'), flush=True)
                        st = ['COMPLETED'] if rc == 0 else ['FAILED']
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
        fields = dict(siteJobID=str(jid), state=','.join(st), exitCode=rc,
                      startTime=site_time(job, 'start'), endTime=end_time, terminal=True,
                      # A guest names its host (parsed from the job comment) so the controller
                      # knows the host's booking covers it.
                      hostSiteJobID=share.get('host'),
                      slot=int(share['slot']) if (share.get('slot') or '').isdigit() else None)
        if rc == CHECKPOINT_EXIT and not any(x in st for x in ('COMPLETED', 'CANCELLED')):
            # D-44 (amended 2026-11-05), A2/A4: a checkpoint is reported only for a resumable step, only
            # from a checkpoint that reached the outputs repository. The controller continues the step
            # from checkpointRef (and enforces the continuation cap); without one nothing continues.
            self.fresh_wallet()
            ckpt = self.checkpoint_of(job) if self.cfg.resumable_lead else None
            if not self.cfg.resumable_lead:
                why = 'the step exited 85, but it is not declared resumable'
            elif not ckpt:
                why = ('its checkpoint did not reach the outputs repository before the job ended'
                       ' (a longer resumable= lead leaves the upload more time)')
            elif not self.checkpoint_committed(ckpt[0]):
                why = 'its checkpoint commit %s is not in the outputs repository' % ckpt[0][:12]
                ckpt = None
            else:
                why = ''
            if why:
                print('not continuing the step:', why, flush=True)
            else:
                print('the step saved its checkpoint (%s) before its wall time - the controller decides '
                      'whether it continues' % ckpt[0][:12], flush=True)
            self.reporter.report(must=True, checkpointRef=(checkpoint_uri(self.cfg.outputs_repo, ckpt)
                                                           if ckpt and not why else None), **fields)
            return 0
        if 'COMPLETED' in st:
            self.fresh_wallet()
            outputs = self.fetch_outputs()
            try:
                self.reporter.report(must=True, outputs=outputs, **fields)
            except OutputsTooLarge as e:
                # An output the job cannot carry is a failed step, not a silently empty one.
                print('ERROR:', e, flush=True)
                self.write_termination('step outputs too large: %s' % e)
                self.reporter.report(must=True, **dict(fields, state='FAILED', exitCode=1))
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


def main(env=None):
    cfg = Settings(env)
    return Runner(cfg).run()
