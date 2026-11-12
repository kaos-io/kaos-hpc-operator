"""Everything the runner knows about its job, read once from the environment the controller set.

Nothing else is trusted (contract section 2): identity, scratch and home, the class table and the
site endpoint come from the HPCSite, the data-plane endpoints from the operator-rendered
`hpc-runner-config` ConfigMap, the step from the HPCArgoWorkflowJob spec.
"""

import json
import os
import re
import shlex
import sys

# The site's API when the controller does not name one (MeluXina).
DEFAULT_ENDPOINT = 'https://slurm-api.lxp.lu'
DEFAULT_REST_VERSION = 'v0.0.44'
DEFAULT_LOGIN = 'login.lxp.lu:8822'

# Mounted by the controller (contract section 2).
TOKEN_FILE = '/etc/meluxina-jwt/token'
STAGING_DIR = '/etc/meluxina-staging'
MACHINE_KEY_FILE = '/etc/mlflow-svc/ZITADEL_MACHINE_KEY'

# PRD-HPC-1461 D-44: exit 85 = the step saved its checkpoint.
CHECKPOINT_EXIT = 85
# PRD-HPC-1461 D-72: the job script's exit code for a step the idle watchdog ended (reason Idle).
IDLE_EXIT = 236
INSUFFICIENT_QUOTA_EXIT = 237   # the runner reports it (FAILED, no site job) when staging does not fit the quota (AC-33)
# AC-39: the runner reports it (FAILED, no site job) when the site refused the submit for its submit cap.
SUBMIT_LIMIT_EXIT = 238
# The runner reports it (FAILED, no site job) when the step's image reference is not registry/repository:tag-or-digest.
INVALID_IMAGE_EXIT = 239
# The runner reports it for a step the slot manager stopped for computing on a GPU outside its set (reason GPUViolation).
GPU_VIOLATION_EXIT = 240
# The runner reports it (FAILED, no site job) when the site's staging tool speaks another staging contract.
STAGING_MISMATCH_EXIT = 241
# The runner reports it (FAILED, no site job) when the dataset holds an object whose name is reserved for the platform,
# or whose path could leave the dataset's folder.
RESERVED_NAME_EXIT = 242
# The step ended well but its result could not be handed back: the job script exits with it when it cannot read or
# upload the result file; the runner reports it (FAILED) when it cannot fetch what the job uploaded, or before it
# submits anything when the run has no way to hand a result back (reason ResultNotHandedBack).
RESULT_NOT_HANDED_BACK_EXIT = 243
# The runner reports it (FAILED) when the result file the job handed back is not a result of the step contract
# (reason InvalidResult).
INVALID_RESULT_EXIT = 244
# The one value a step receives: the step context (docs/step-contract.md).
CONTEXT_ENV = 'KAOS_STEP_CONTEXT'
DEFAULT_IDLE_TIMEOUT_MIN = 20
# AC-28: a site that does not answer is waited for this long (the controller passes its --site-outage-max).
DEFAULT_SITE_WAIT_MAX_S = 30 * 60
OFFER_S = 60   # PRD-1609: a sharing job waits this long for a host of its scope to take it
# PRD-HPC-1461 D-70: a guest the controller planned onto a host's node is submitted to begin this far ahead. It is
# eligible only then, so no free node can start it on its own; the host's slot manager takes it like a job in its
# offer window. The controller plans it again (and the runner submits it as a host) long before this passes.
GUEST_HOLD_S = 12 * 3600
ROLE_POLL_S = 30   # how often a waiting guest reads the controller's plan for it


def fail_start(msg, code=2):
    """A start-up failure that retrying cannot fix. Goes to the pod's termination message too."""
    print(msg, flush=True)
    try:
        with open('/dev/termination-log', 'w') as f:
            f.write(msg[-3000:])
    except OSError:
        pass
    sys.exit(code)


def image_problem(ref):
    """Why an image reference cannot be pulled, or '' when it is registry-host/repository with a tag or digest.
    Checked before anything is staged or submitted: a half-rendered reference such as ':probe-learn-latest' would
    otherwise cost a site job and three credential resubmits for a failure no credential fixes."""
    ref = normalize_image(ref or '')
    name, _, digest = ref.partition('@')
    if digest:
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
            return 'the digest %r is not sha256:<64 hex digits>' % digest
    else:
        head, _, last = name.rpartition('/')
        if ':' not in last or not last.partition(':')[2]:
            return 'it has no tag or digest'
        name = head + '/' + last.partition(':')[0]
    host, _, repo = name.partition('/')
    if not (host and repo and not repo.startswith('/') and not repo.endswith('/')) or \
            not ('.' in host or ':' in host or host == 'localhost'):
        return 'it has no registry host and repository (want <registry>/<repository>:<tag>)'
    return ''


def normalize_image(img):
    """Apptainer rejects tag@digest refs ('Docker references with both a tag and digest are
    currently not supported', live job 5140397). The platform pins images as name:tag@sha256:...
    - normalize to the digest-only form, which Apptainer accepts and which is the stronger pin."""
    if '@' in img:
        name, digest = img.split('@', 1)
        if ':' in name.rsplit('/', 1)[-1]:
            name = name.rsplit(':', 1)[0]
        img = name + '@' + digest
    return img


def resume_of(uri):
    """(sha256, path) from a checkpoint reference the runner reported (scratch://<tar path>#<sha256>), or None."""
    m = re.fullmatch(r'scratch://(kaos-ckpt/\S+\.tar)#([0-9a-f]{64})', uri or '')
    return (m.group(2), m.group(1)) if m else None


class Settings:
    def __init__(self, env=None):
        e = self.env = os.environ if env is None else env
        g = e.get
        self.namespace = g('HPC_JOB_NAMESPACE') or ''
        self.job_name = g('HPC_JOB_NAME') or ''
        if not (self.namespace and self.job_name):
            fail_start('HPC_JOB_NAMESPACE / HPC_JOB_NAME missing: the controller starts the runner')
        self.attempt = max(int(g('KAOS_ATTEMPT') or 1), 1)
        self.wf_uid = g('WF_UID') or ''
        self.step = g('STEP_NAME') or ''
        if not (self.wf_uid and self.step and g('IMAGE_REF')):
            fail_start('WF_UID / STEP_NAME / IMAGE_REF missing from the runner environment')
        self.wf_name = g('WF_NAME') or ''     # labels the step's log lines; it is not passed to the step
        self.image = normalize_image(g('IMAGE_REF'))
        # strict=False tolerates control characters inside strings (belt and braces for any
        # substitution path the render-side toJson escaping did not cover).
        self.command_tokens = json.loads(g('STEP_COMMAND') or '[]', strict=False)
        # shlex-quote each argv token: the joined string rides through Slurm env -> sh -c, and a
        # naive join destroys embedded quoting (live job 5140493).
        self.command = ' '.join(shlex.quote(t) for t in self.command_tokens)
        self.time_limit = int(g('SLURM_TIME_LIMIT') or 240)
        # PRD-HPC-1461 D-68: the lane (site QoS, reservation) the controller placed this attempt in;
        # empty = the class's own QoS, no reservation. qos_max is the lane's wall-time maximum (0 = unknown).
        self.qos = g('HPC_QOS') or ''
        self.reservation = g('HPC_RESERVATION') or ''
        self.qos_max = int(g('HPC_QOS_MAX_MINUTES') or 0)
        # PRD-HPC-1461 D-44 (amended 2026-11-05, 2026-11-10): only a step declared resumable gets the stop
        # signal, this many minutes before its wall time; 0 = not resumable. On a shared node the slot
        # manager sends it, step by step (AC-73), so a resumable step may host or be a guest.
        self.resumable_lead = int(g('HPC_RESUMABLE_LEAD') or 0)
        # D-72: minutes of idleness (GPU/CPU below 5 % and a silent log) after which the job ends
        # the step as Idle; 0 = off. The controller always sets it; 20 when it is absent or unreadable.
        raw = (g('HPC_IDLE_TIMEOUT_MINUTES') or '').strip()
        self.idle_timeout = int(raw) if raw.isdigit() else DEFAULT_IDLE_TIMEOUT_MIN
        # AC-39: the site's cap on queued plus running jobs per user, when the profile knows it (0 = unknown).
        raw = (g('HPC_MAX_SUBMIT_JOBS') or '').strip()
        self.max_submit_jobs = int(raw) if raw.isdigit() else 0
        # The step context, exactly as the job carries it (spec.context). The step in the site job gets it
        # under the same name, unchanged; the runner never reads into it.
        self.context = g(CONTEXT_ENV) or ''
        # The step's inputs (spec.inputs): the datasets to stage, each pinned to one commit. The runner stages
        # these commits; it never resolves a version. Read by check_job, never by the cancel-only runner.
        self.inputs_raw = g('HPC_INPUTS') or '[]'
        self.inputs = []
        # PRD-1609 node sharing: off unless the operator turned it on for this project and gave
        # its scope. A host on a shared node takes only jobs whose name and script header carry it.
        self.scope = g('HPC_SCOPE') or ''
        sharing = (g('HPC_SHARING') or '').lower() in ('on', '1', 'true')
        self.sharing = sharing and re.fullmatch('[a-z0-9]{8,16}', self.scope) is not None
        if sharing and not self.sharing:
            print('node sharing: scope %r is not 8-16 lowercase letters or digits - running alone'
                  % self.scope, flush=True)
        # PRD-HPC-1461 D-70: the controller's place for this step in a shared node, "guest" or "host" ("" = no plan:
        # the job is a host, as before the plan existed). A waiting guest reads the plan again (apply_plan).
        self.share_role = (g('HPC_SHARE_ROLE') or '') if self.sharing else ''
        self.share_job_limit = 0   # the host's job limit the plan gave after a re-plan (0: computed from the lane maximum)
        # The site job's NAME is its immutable key (a job script rewrites the comment, never the name). It carries
        # the HPC job's identity and the attempt, so the finished job of a submit whose answer was lost is found
        # again by name (Runner.adopt) and is never taken for another attempt's or another Argo retry's job.
        self.legacy_site_name = 'kaos-' + (self.scope + '-' if self.sharing else '') + self.wf_uid + '-' + self.step
        self.job_site_name = '%s-%s-a%d' % (self.legacy_site_name, re.sub(r'^hpc-', '', self.job_name)[:8], self.attempt)
        # AC-28/AC-30: how long an unreachable site is waited for before the attempt fails (seconds).
        self.site_wait_max = int(g('HPC_SITE_WAIT_MAX_S') or DEFAULT_SITE_WAIT_MAX_S)
        # The checkpoint this attempt continues from (set by the controller from status.resume).
        self.resume = None
        ref = g('KAOS_RESUME_REF')
        if ref:
            self.resume = resume_of(ref)
            if not self.resume:
                fail_start('KAOS_RESUME_REF %r is not a checkpoint reference (scratch://<path>#<sha256>)' % ref)
        # D-75: the share of the project's scratch space the checkpoint copies may hold.
        self.ckpt_budget_pct = int(g('HPC_CKPT_BUDGET_PERCENT') or 10)
        # Site: scheduler API, login node.
        endpoint = (g('HPC_ENDPOINT') or DEFAULT_ENDPOINT).rstrip('/')
        version = g('HPC_REST_VERSION') or DEFAULT_REST_VERSION
        self.api = '%s/slurm/%s' % (endpoint, version)
        self.sdb = '%s/slurmdb/%s' % (endpoint, version)
        self.login_host, _, self.login_port = (g('HPC_LOGIN_HOST') or DEFAULT_LOGIN).partition(':')
        self.login_port = self.login_port or '22'
        # Operator-rendered data-plane endpoints (hpc-runner-config).
        self.lakefs_url = (g('LAKEFS_EXTERNAL_URL') or '').rstrip('/')
        self.mlflow_url = g('MLFLOW_EXTERNAL_URL') or ''
        # The app's outputs repository, from the job's app (spec.app): the step's result is handed back through
        # a hand-off branch of it.
        app = g('KAOS_APP') or ''
        self.outputs_repo = 'outputs-' + app if re.fullmatch('[a-z0-9]([-a-z0-9]*[a-z0-9])?', app) else ''

    def apply_plan(self, placement):
        """A guest that was planned again as a host: take the lane and job limit the controller chose. The
        placement is status.placement of the job."""
        share = (placement or {}).get('share') or {}
        self.share_role = share.get('role') or ''
        self.qos = placement.get('qos') or ''
        self.reservation = placement.get('reservation') or ''
        self.share_job_limit = int(share.get('jobLimitMinutes') or 0)

    # ---- identity and class (never literals: one project must not submit as another)

    def identity(self):
        """The project's HPC identity from the HPCSite: user, account, scratch root and home."""
        ident = {k: (self.env.get(v) or '') for k, v in (
            ('user', 'HPC_USER'), ('account', 'HPC_ACCOUNT'),
            ('scratch', 'HPC_SCRATCH'), ('home', 'HPC_HOME'))}
        missing = [k for k, val in ident.items() if not val]
        if missing:
            fail_start('HPC identity missing from the runner environment: %s - the HPCSite does not carry '
                       'the tenancy (PRD-HPC-1231)' % missing)
        return ident

    def hpc_class(self):
        """The class this job runs on (spec.class), resolved against the HPCSite's table. An unknown
        class is a render/submit mismatch, never a silent default partition."""
        name = self.env.get('HPC_CLASS') or ''
        classes = json.loads(self.env.get('HPC_CLASSES_JSON') or '[]')
        for c in classes:
            if c.get('name') == name:
                return c
        fail_start('unknown HPC class %r - catalog: %s' % (name, [c.get('name') for c in classes]))

    @staticmethod
    def class_cap(cls):
        return int(cls.get('maxTimeLimitMinutes') or cls.get('maxWallTimeMinutes') or 0)

    def lane_cap(self, cls):
        """The longest limit this attempt's job may ask for: the class's maximum, and the lane's when
        the attempt is placed in one (a request above the lane's maximum is refused)."""
        caps = [c for c in (self.class_cap(cls), self.qos_max if self.qos else 0) if c > 0]
        return min(caps) if caps else 0

    # ---- the step contract

    def check_job(self):
        """Refuse to start an attempt that cannot run on the step contract: no step context, or inputs that are
        not pinned datasets. Called before staging and before any submission, and never by the cancel-only runner
        (which must be able to clean up whatever the job asks for). Fills self.inputs. Every name, repository and
        commit is checked here because each one becomes a word of the job script."""
        if not self.context:
            fail_start('this job carries no step context (%s): it was created before the platform update that '
                       'changed how a step is given its inputs. Running it now would run the step without its '
                       'context and its data. Run the pipeline again.' % CONTEXT_ENV, 1)
        try:
            inputs = json.loads(self.inputs_raw)
            if not isinstance(inputs, list):
                raise ValueError('not a list')
            for i in inputs:
                if not (re.fullmatch(INPUT_NAME, i['name']) and len(i['name']) <= 40):
                    raise ValueError('input name %r' % (i['name'],))
                if not re.fullmatch(INPUT_REPO, i['repo']):
                    raise ValueError('input %s: %r is not a dataset repository' % (i['name'], i['repo']))
                if not re.fullmatch('[0-9a-f]{64}', i['commit']):
                    raise ValueError('input %s: dataset %s has no pinned commit (got %r)' % (i['name'], i['repo'], i['commit']))
            if len({i['name'] for i in inputs}) != len(inputs):
                raise ValueError('two inputs have the same name')
        except (ValueError, KeyError, TypeError) as e:
            fail_start('the job\'s inputs (HPC_INPUTS) are not a list of pinned datasets: %s. The pipeline pins every '
                       'input before any step runs; the runner never resolves a version.' % e, 1)
        self.inputs = [{'name': i['name'], 'repo': i['repo'], 'commit': i['commit'],
                        # The label the site's staging tool files the copy under: `main` for `latest` (as
                        # before), else what the run asked for; the commit for a dataset a needed step
                        # wrote. The commit, not this label, decides what is copied.
                        'ref': 'main' if i.get('ref') == 'latest' else (i.get('ref') or i['commit'])}
                       for i in inputs]


INPUT_NAME = '[A-Za-z0-9]([A-Za-z0-9_-]*[A-Za-z0-9])?'
INPUT_REPO = 'dataset-[a-z0-9][a-z0-9-]{0,54}'
