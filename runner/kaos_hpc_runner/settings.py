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
DEFAULT_IDLE_TIMEOUT_MIN = 20
# AC-28: a site that does not answer is waited for this long (the controller passes its --site-outage-max).
DEFAULT_SITE_WAIT_MAX_S = 30 * 60
OFFER_S = 60   # PRD-1609: a sharing job waits this long for a host of its scope to take it


def fail_start(msg, code=2):
    """A start-up failure that retrying cannot fix. Goes to the pod's termination message too."""
    print(msg, flush=True)
    try:
        with open('/dev/termination-log', 'w') as f:
            f.write(msg[-3000:])
    except OSError:
        pass
    sys.exit(code)


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
        self.wf_name = g('KUBECORE_WORKFLOW_NAME') or ''
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
        self.output_names = [n for n in (g('STEP_OUTPUTS') or '').split(',') if n]
        # The step's dataset, pinned by the pipeline's compose step (spec.dataset). The runner stages this
        # commit; it never resolves a version.
        self.dataset_repo = (g('DATASET_REPO') or '').strip()
        self.dataset_version = (g('DATASET_VERSION') or '').strip()
        self.dataset_commit = (g('DATASET_COMMIT') or '').strip()
        self.dataset_owner = (g('DATASET_OWNER') or '').strip()
        # A dataset named the way jobs did before the run's data was pinned (by an older controller as DATASET,
        # or by this one for a job stored before the update): there is no commit to stage.
        self.dataset_unpinned = (g('DATASET_UNPINNED') or g('DATASET') or '').strip()
        # PRD-1609 node sharing: off unless the operator turned it on for this project and gave
        # its scope. A host on a shared node takes only jobs whose name and script header carry it.
        self.scope = g('HPC_SCOPE') or ''
        sharing = (g('HPC_SHARING') or '').lower() in ('on', '1', 'true')
        self.sharing = sharing and re.fullmatch('[a-z0-9]{8,16}', self.scope) is not None
        if sharing and not self.sharing:
            print('node sharing: scope %r is not 8-16 lowercase letters or digits - running alone'
                  % self.scope, flush=True)
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
        self.outputs_repo = g('OUTPUTS_REPO') or ''
        self.outputs_ref = g('OUTPUTS_REF') or ''

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

    # ---- dataset

    def check_dataset(self):
        """Refuse to start an attempt whose dataset cannot be staged as pinned. Called before staging and before
        any submission, and never by the cancel-only runner (which must be able to clean up whatever the job
        asks for)."""
        if self.dataset_unpinned and not self.dataset_repo:
            fail_start('this job names dataset %s without a pinned commit: it was created before the platform '
                       'update that pins a run\'s dataset. Running it now would run the step without its data. '
                       'Run the pipeline again.' % self.dataset_unpinned, 1)
        if self.dataset_repo and not re.fullmatch('[0-9a-f]{64}', self.dataset_commit):
            fail_start('dataset %s@%s has no pinned commit (got %r): the pipeline pins it before any step '
                       'runs, the runner never resolves a version'
                       % (self.dataset_repo, self.dataset_version, self.dataset_commit), 1)
        if self.dataset_owner:
            fail_start('dataset %s/%s@%s is shared by project %s: datasets of another project cannot be '
                       'read on HPC yet. Run this step on a cluster class, or copy the dataset into this '
                       'project.' % (self.dataset_owner, self.dataset_repo, self.dataset_version,
                                     self.dataset_owner), 1)

    @property
    def dataset_ref(self):
        """The label the site's stager files the copy under: `main` for `latest` (as before), else the
        version. The commit, not this label, decides what is copied."""
        return 'main' if self.dataset_version == 'latest' else self.dataset_version

    def data_lock(self):
        """The run's data lock for this step's dataset (kaos-lib's KAOS_LIB_LOCK, version 1): the step's
        kaos-lib then reads the pinned commit. "" when the job has no dataset."""
        if not self.dataset_repo:
            return ''
        return json.dumps({'version': 1, 'reads': [{
            'repo': self.dataset_repo, 'owner': self.dataset_owner or None,
            'ref': self.dataset_version, 'commit': self.dataset_commit}]}, separators=(',', ':'))
