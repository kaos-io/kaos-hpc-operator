"""Dataset staging through the site's login-node stager (PRD-HPC-1541 F-03/F-06).

The runner stages the pinned dataset commit into the project's Lustre cache from a login node
before the GPU job is submitted, so the job reads a resident copy instead of downloading on GPU
time. It talks to the stager the platform installed (~/.kaos/stager-c2.py) through a forced-command key
that can run nothing else (Secret meluxina-staging: key + verified host pin). The stager names a
base (its newest ready commit of the repo); the runner lists only the lakeFS diff base..commit, or
the whole commit for a first copy, and streams one JSON line per file: every object of the commit,
whatever the dataset's shape (PRD kaos#1911 F-04). Unchanged files are hard links of the base. Bytes
come from the lakeFS API with the wallet bearer (sent once on stdin, held in memory only). Any failure
returns '' and the job stages in-job as before, saying so (F-06), with three exceptions that end the run
before anything is submitted: no room in the quota (InsufficientQuota), a staging tool that speaks
another staging contract (StagingMismatch), a dataset object whose name is reserved for the
platform (ReservedName), and a dataset above STAGE_FALLBACK_MAX_BYTES that the site could not stage in its budget
(DatasetNotStaged: the job would download it on GPU time). The budget is stage_budget(bytes of the copy).

A dataset another project shares (AC-44, D-61; the input names an owner) is staged the same way from the OWNER's
data store: its public address comes from the own data store's peers, the credential is the same wallet bearer
(the owner's data store accepts it for exactly what the owner shares with the project, and the project's own
key or admin token is never sent), and the copy is filed under `<repo>@<owner>` on the site (dataset_key). The job never
downloads it itself: a shared dataset the site cannot stage ends the run SharedDatasetUnavailable, before submit.

Names that are not copied. Reserved: the staging tool's half-downloaded files end in `.kaos-part`, beside the
file they become; a dataset object with such a name would be refused by the staging tool mid-copy. (A copy on
the site keeps the dataset's files under `<copy>/data/` and every file of the platform beside that folder, so
no other name of a dataset can collide with one.) Unsafe: a path with a `..`, `.` or empty part, a leading `/` or a NUL byte could be
written outside the dataset's folder. Too long: no room for the suffix of a half-downloaded file. A file that is
also a folder (`a` and `a/b`). All are refused here, by name, before anything is copied or submitted (name_rule
and Names; a zero-byte folder marker such as `docs/` is skipped, not refused):
every name of the plan when the copy is made from the login node, and every name of the commit when the job is
about to stage in-job. A `.kaos/` or `.kaos-*` name inside a dataset is staged like any other.

The staging contract (CONTRACT) is what `status` and `stage` mean to the runner and to the staging tool,
which ship separately. The runner asks the tool for its number (`version`) and sends its own with every
`status` and `stage`; each side refuses the other on any difference.

`Stager` is also what image staging, log streaming and the tar lease go through.
"""

import atexit
import json
import os
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from . import settings, wallet as walletmod

# How long staging goes on asking again (new attempts) before the job stages in-job: stage_budget(bytes). A new attempt
# resumes the copy file by file. The same 72 hours is the allowance of the runner Job's deadline (internal/runnerjob)
# and of the follower's (engine/kubecore/meluxina.py): change the three together.
STAGE_MIN_BUDGET_S = 4 * 3600
STAGE_MAX_BUDGET_S = 72 * 3600
STAGE_UNKNOWN_BUDGET_S = 12 * 3600   # the size could not be read from the data store
STAGE_FLOOR_BPS = 50 * 10 ** 6       # a copy slower than this for hours is not healthy; the budget allows twice the time
STAGE_FALLBACK_MAX_BYTES = 50 * 10 ** 9   # above this a job never downloads the dataset itself, on GPU time
STAGE_NOTE_EVERY_S = 10 * 60         # how often the job's status says how far the copy is
TOKEN_MIN_LEFT_S = 3600              # a data token with less left than this is not used for another attempt
EX_RETRY, EX_NOSPACE, EX_USAGE, EX_CONTRACT = 75, 76, 2, 78
CONTRACT = 3   # 3 = the whole tree of the commit under <copy>/data/, and a dataset another project shares filed as repo@owner; the tool's CONTRACT is the same number


class InsufficientQuota(Exception):
    """Staging stopped for want of scratch quota (space or files), after the stager evicted everything
    unused (PRD-HPC-1461 AC-33). The message is a plain sentence for the job's status and the pipeline
    log; the run ends InsufficientQuota instead of the job failing mid-copy on a full quota."""


class StagingMismatch(Exception):
    """The site's staging tool speaks another staging contract than this runner (one of the two was updated
    first). Nothing is staged with it and the job does not stage in-job either: the run ends StagingMismatch
    before anything is submitted. The message is a plain sentence for the job's status and the pipeline log."""


class SharedDatasetUnavailable(Exception):
    """A dataset another project shares cannot be read for this step: the share is not approved or was withdrawn,
    the owner is not known or has no public address, or the site could not stage it (a job never downloads a
    shared dataset itself). Nothing is staged and nothing is submitted. The message names the dataset and the owner
    and says what to do, for the job's status and the pipeline log."""


class DatasetNotStaged(Exception):
    """The site could not stage a dataset in its budget, and the dataset is too large for the job to download it
    itself on GPU time (STAGE_FALLBACK_MAX_BYTES). Nothing is submitted: the run ends DatasetNotStaged. The message
    names the dataset and its size, for the job's status and the pipeline log."""


class ReservedName(Exception):
    """The dataset commit holds an object whose name cannot be copied to the site: it is reserved for the
    platform there, or its path could leave the dataset's folder. Nothing is staged and the job does not stage
    in-job either: the run ends ReservedName before anything is submitted. The message names the dataset, the
    object and which of the two it is, for the job's status and the pipeline log."""


def name_rule(path, size=0):
    """Why a dataset object is not copied under its own name, or '' when it is.

    ONE RULE IN THREE PLACES, kept the same line for line and tested against one table
    (hack/staging-contract/names.json, and next to the staging tool):
      here                                           runner/kaos_hpc_runner/staging.py           name_rule
      this repository                                runner/kaos_hpc_runner/payload/stagein.py   name_rule
      novelcore/kubecore-operator  images/meluxina-tokenmint/stager/stager.py                    name_rule
    Here it is applied before anything is staged or submitted (ReservedName); the other two are the second line.

      'marker'    a zero-byte object whose name ends in "/": a folder marker, skipped
      'unsafe'    empty, a leading "/", an empty, "." or ".." part, a NUL byte: it could leave the folder
      'reserved'  a file name ending in .kaos-part (the staging tool's half-downloaded files, named beside their
                  target); the dataset's files live under data/, so no name can collide with a marker of a copy
      'long'      a part longer than 255 bytes, or a file name longer than 245 (no room for .kaos-part)
    """
    if not isinstance(path, str):
        return 'unsafe'
    if path.endswith('/') and not size:
        return 'marker'
    parts = path.split('/')
    if not path or '\x00' in path or path.startswith('/') or any(p in ('', '.', '..') for p in parts):
        return 'unsafe'
    if parts[-1].endswith('.kaos-part'):
        return 'reserved'
    sizes = [len(p.encode('utf-8', 'replace')) for p in parts]
    if max(sizes) > 255 or sizes[-1] > 245:
        return 'long'
    return ''


WHY = {
    'unsafe': 'a path that is empty, starts with "/", or has an empty part, a "." or ".." part or a NUL byte could '
              'be written outside the dataset\'s folder on the site',
    'reserved': 'a file name ending in .kaos-part is reserved for the platform (half-downloaded files)',
    'long': 'a file name longer than 245 bytes, or a folder name longer than 255, cannot be written on the site',
}


class Names:
    """Judges the object names of one dataset commit as they come from a listing: take(path, size) is True for
    an object to copy, False for a folder marker to skip, and raises ReservedName for a name that cannot be
    copied (name_rule) or for a file that is also a folder (`a` and `a/b`: a file system holds one or the
    other). lakeFS lists in path order, so the second is found by keeping the files the current path still
    begins with; nothing else of the listing is held."""

    def __init__(self, repo, commit):
        self.repo, self.commit, self.files = repo, commit, []

    def refuse(self, path, why):
        raise ReservedName('dataset %s@%s has an object named %r: %s. Nothing was staged and nothing was submitted '
                           'to the site. Rename or remove the object and commit a new version of the dataset.'
                           % (self.repo, self.commit[:12], path, why))

    def take(self, path, size=0):
        why = name_rule(path, size)
        if why == 'marker':
            return False
        if why:
            self.refuse(path, WHY[why])
        self.files = [f for f in self.files if path.startswith(f)]
        for f in self.files:
            if path.startswith(f + '/'):
                self.refuse(f, 'the dataset also has %r, under a folder of the same name; a file system holds a '
                               'file or a folder under one name, not both' % path)
        self.files.append(path)
        return True


def refuse_names(api, repo, commit, wallet, prefix=''):
    """Raise ReservedName for the first object of the commit (under prefix) that cannot be copied (Names). A
    listing that cannot be read judges nothing: the stage plan and the in-job stage-in check the names again.
    Returns the bytes of the objects listed, or None when the listing could not be read."""
    names, total = Names(repo, commit), 0
    try:
        for o in lakefs_pages(api + '/refs/%s/objects/ls' % commit, wallet, {'prefix': prefix}):
            size = int(o.get('size_bytes') or 0)
            names.take(o['path'], size)
            total += size
    except ReservedName:
        raise
    except Exception as e:
        print('stage: object names not checked (%s)' % e, flush=True)
        return None
    return total


def stage_budget(nbytes):
    """The seconds staging may go on asking again for a dataset of nbytes: the time to copy it at STAGE_FLOOR_BPS,
    twice, and at least STAGE_MIN_BUDGET_S and at most STAGE_MAX_BUDGET_S; STAGE_UNKNOWN_BUDGET_S when the size is
    not known (None)."""
    if nbytes is None:
        return STAGE_UNKNOWN_BUDGET_S
    return int(min(max(2 * nbytes / STAGE_FLOOR_BPS, STAGE_MIN_BUDGET_S), STAGE_MAX_BUDGET_S))


def not_staged_message(repo, commit, nbytes, why):
    return ('dataset %s@%s is %s and %s. A job downloads a dataset itself only up to %s, because that time is GPU '
            'time. Nothing was submitted to the site. Run the step again: the copy continues from the files that are '
            'already on the site (kept for a day).' % (repo, commit[:12], human_bytes(nbytes), why,
                                                      human_bytes(STAGE_FALLBACK_MAX_BYTES)))


def refuse_big_fallback(repo, commit, nbytes, why):
    """Raise DatasetNotStaged when the job would download a dataset of nbytes (None: not known) itself."""
    if nbytes is not None and nbytes > STAGE_FALLBACK_MAX_BYTES:
        raise DatasetNotStaged(not_staged_message(repo, commit, nbytes, why))


def renewed_wallet(mint_wallet, old):
    """The token for the next attempt: minted again, never an old one close to its end. When the mint fails the old
    one is kept while it has TOKEN_MIN_LEFT_S left (a token whose end is unknown is kept); else ''."""
    new = mint_wallet()
    if new:
        return new
    exp = walletmod.wallet_exp(old)
    return '' if exp and exp - time.time() < TOKEN_MIN_LEFT_S else old


class ToolAbsent(Exception):
    """The login node could not run the staging tool at all (no file, not readable, no interpreter). Asked again
    for TOOL_WAIT_S, then the run ends StagingMismatch with absent_message."""


TOOL_WAIT_S = 20 * 60   # longer than one credential refresh (15 min), which is what installs the staging tool


def absent_message(rc):
    return ('the site\'s staging tool is not installed for this project, or cannot be run (the login node answered '
            'exit %s for %d minutes). It is installed by the project\'s HPC credential refresh, which runs every 15 '
            'minutes. Nothing was staged and nothing was submitted to the site. For a project bound to the site in '
            'the last minutes, run again; otherwise the refresh is failing to install it and its log says why (a '
            'platform operator can read it).' % (rc, TOOL_WAIT_S // 60))


def mismatch_message(theirs):
    return ('the site\'s staging tool and this runner speak different staging contracts (staging tool: %s, '
            'runner: %d). Nothing was staged and nothing was submitted to the site. The platform is being updated: '
            'run again when the update is complete.'
            % ('an older one that does not report it' if theirs is None else theirs, CONTRACT))


def human_bytes(n):
    n = float(n)
    for unit in ('B', 'kB', 'MB', 'GB', 'TB'):
        if n < 1000 or unit == 'TB':
            return ('%d %s' % (n, unit)) if unit == 'B' else ('%.1f %s' % (n, unit))
        n /= 1000


def quota_message(what, need_bytes, need_files):
    return ('not enough room on the project\'s scratch for %s (%s, %d files): the quota has no space or file '
            'allowance left for it even after removing every unused copy. Nothing was copied and nothing in '
            'use was deleted. Free space on the project\'s scratch or ask for a larger quota.'
            % (what, human_bytes(need_bytes), need_files))


def lakefs_pages(url, wallet, params):
    # Every object of a paginated lakeFS listing (objects/ls or diff).
    after = ''
    while True:
        q = urllib.parse.urlencode(dict(params, amount=1000, after=after))
        page = json.load(urllib.request.urlopen(urllib.request.Request(
            url + '?' + q, headers={'Authorization': 'Bearer ' + wallet}), timeout=120))
        for r in page.get('results') or []:
            if r.get('path_type') == 'object':
                yield r
        pg = page.get('pagination') or {}
        if not pg.get('has_more'):
            return
        after = pg.get('next_offset') or ''


def stage_plan(api, commit, base, prefix, wallet):
    # (kind, path, size) per file the stager applies: the two-dot diff against
    # the base (three-dot would be wrong when the base is not an ancestor), or
    # every object of the commit for a first copy.
    if base:
        for d in lakefs_pages(api + '/refs/%s/diff/%s' % (base, commit), wallet,
                              {'prefix': prefix, 'type': 'two_dot'}):
            if d['type'] not in ('added', 'changed', 'removed'):
                raise ValueError('unexpected diff entry %s for %s' % (d['type'], d['path']))
            if d['type'] == 'removed' and d['path'].endswith('/'):
                continue                      # a folder marker that is gone: it was never copied
            yield d['type'], d['path'], int(d.get('size_bytes') or 0)
    else:
        for o in lakefs_pages(api + '/refs/%s/objects/ls' % commit, wallet, {'prefix': prefix}):
            yield 'added', o['path'], int(o.get('size_bytes') or 0)


class Stager:
    """One stager subcommand on a login node per call: (exit code, JSON events).

    All calls of one runner share ONE SSH connection to the login node (OpenSSH multiplexing): the
    runner calls the stager many times (staging, log tail every 30 s), and a new login per call loads
    the site's shared login nodes (50 running steps were ~200 logins a minute). The master is started
    explicitly with its output on /dev/null (a ControlPersist master started by a call would inherit
    that call's pipes and hang it), lives at most MASTER_PERSIST_S after the last call, and if it
    cannot be started the runner falls back to one connection per call, as before."""

    MASTER_PERSIST_S = 300
    progress = None     # called with each `progress` event of a fetch (stage_dataset sets it)

    def __init__(self, cfg, staging_dir=settings.STAGING_DIR):
        self.cfg, self.staging_dir = cfg, staging_dir
        self._home = None      # private HOME for ssh (the pod's may not be writable); holds the socket
        self._mux = None       # None: not tried; True: master up; False: multiplexing off for this runner
        self._mux_lock = threading.Lock()   # the log streamer thread and the runner call concurrently
        self.last_error = ''

    def _ssh_home(self):
        if self._home is None:
            self._home = tempfile.mkdtemp(prefix='kaos-ssh-')   # short path: a socket path is <= 104 bytes
            atexit.register(self.close)
        return self._home

    def _base(self):
        d = self.staging_dir
        return ['-F', '/dev/null', '-i', d + '/id_ed25519', '-p', self.cfg.login_port,
                '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                '-o', 'UserKnownHostsFile=' + d + '/known_hosts',
                '-o', 'ConnectTimeout=20', '-o', 'ServerAliveInterval=30',
                '-o', 'ServerAliveCountMax=4']

    def _target(self):
        return self.cfg.identity()['user'] + '@' + self.cfg.login_host

    def _run_quiet(self, args, timeout):
        try:
            return subprocess.run(['ssh'] + args, env=dict(os.environ, HOME=self._ssh_home()),
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL, timeout=timeout).returncode
        except (OSError, subprocess.TimeoutExpired):
            return 255

    def _mux_opts(self):
        """ControlPath options for a call over the shared connection, or [] to connect directly."""
        with self._mux_lock:
            return self._mux_opts_locked()

    def _mux_opts_locked(self):
        if self._mux is False:
            return []
        sock = self._ssh_home() + '/cm'
        path = ['-o', 'ControlPath=' + sock]
        if self._run_quiet(['-F', '/dev/null'] + path + ['-O', 'check', self._target()], 10) != 0:
            rc = self._run_quiet(self._base() + path + ['-o', 'ControlMaster=yes',
                                 '-o', 'ControlPersist=%d' % self.MASTER_PERSIST_S, '-N', '-f',
                                 self._target()], 60)
            if rc != 0:
                if self._mux is None:
                    print('stager: shared SSH connection unavailable (rc %d), one connection per call'
                          % rc, flush=True)
                self._mux = False
                return []
        self._mux = True
        return path + ['-o', 'ControlMaster=no']

    def close(self):
        """Stop the shared connection and remove the private HOME (also run at exit)."""
        if self._home is None:
            return
        if self._mux:
            self._run_quiet(['-F', '/dev/null', '-o', 'ControlPath=' + self._home + '/cm', '-O', 'exit',
                             self._target()], 10)
        shutil.rmtree(self._home, ignore_errors=True)
        self._home, self._mux = None, None

    def have_key(self):
        import os
        return os.path.exists(self.staging_dir + '/id_ed25519')

    def __call__(self, words, feed=None, quiet=False):
        # The feed's records are written while the stager downloads; a feed that fails mid-way closes
        # stdin without the end record, which the stager keeps as resumable partial state (exit 75).
        # quiet: do not echo the events (the log subcommand's data is printed by the log streamer).
        cmd = (['ssh'] + self._base() + self._mux_opts() +
               [self._target(), ' '.join(shlex.quote(str(w)) for w in words)])
        # The pod runs as uid 65532 with a HOME that may not be writable: ssh gets a private one.
        home = self._ssh_home()
        p = subprocess.Popen(cmd, env=dict(os.environ, HOME=home), stdin=subprocess.PIPE if feed is not None else subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        events = []

        def read():
            for line in p.stdout:
                try:
                    ev = json.loads(line)
                except ValueError:
                    if not quiet:
                        print('stager:', line.rstrip(), flush=True)
                    continue
                events.append(ev)
                if self.progress and ev.get('event') == 'progress':
                    self.progress(ev)
                if not quiet:
                    print('stager:', json.dumps(ev, sort_keys=True), flush=True)
        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        failed = None
        if feed is not None:
            try:
                for rec in feed:
                    if isinstance(rec, bytes):
                        p.stdin.flush()
                        p.stdin.buffer.write(rec)
                    else:
                        p.stdin.write(json.dumps(rec) + '\n')
            except BrokenPipeError:
                pass
            except Exception as e:
                failed = e
            try:
                p.stdin.close()
            except BrokenPipeError:
                pass
        rc = p.wait()
        reader.join(30)
        err = p.stderr.read().strip()
        self.last_error = err                 # of the last call: tells a missing tool from an older one (stage_once)
        if err:
            print('stager stderr:', err[-2000:], flush=True)
        if failed is not None:
            raise failed
        return rc, events


def dataset_key(repo, owner=''):
    """What a copy is filed under on the site and in a staging record: the repository, and for a dataset another
    project shares `<repo>@<owner>`, so two projects that each have a repository of one name are two copies. Neither
    a repository nor a project name holds an `@`."""
    return '%s@%s' % (repo, owner) if owner else repo


def _unavailable(repo, owner, why):
    return SharedDatasetUnavailable(
        'dataset %s is shared by project %s, and %s. Nothing was staged and nothing was submitted to the site. '
        'Ask project %s to share it with this project.' % (repo, owner, why, owner))


def _no_job_download(repo, owner, why):
    return SharedDatasetUnavailable(
        'dataset %s is shared by project %s, and %s: the job does not download it itself (a job reads only its own '
        'project\'s data store). Nothing was staged and nothing was submitted to the site. Run the step again; if it '
        'keeps failing, a platform operator can read why the site\'s staging tool is not used.' % (repo, owner, why))


def owner_store(own_url, wallet, owner, repo, commit):
    """The public address of project `owner`'s data store, once it is clear that the owner shares `repo` at `commit`
    with this project. The address is the `url` of the owner in the own data store's peers (`endpoint` is an address
    on the platform's network, which a login node cannot reach). The one credential sent anywhere is the wallet
    bearer; the owner's data store lets it read what the owner shares with the project and nothing else. Raises
    SharedDatasetUnavailable; any other failure is a transient one the caller asks again."""
    def get(url, what):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(
                url, headers={'Authorization': 'Bearer ' + wallet}), timeout=60))
        except urllib.error.HTTPError as e:
            if e.code in (401, 403, 404):
                raise _unavailable(repo, owner, what % e.code) from None
            raise
    peers = get(own_url + '/kaos/v1/peers',
                'this project\'s data store does not list the projects it knows (HTTP %s)')
    found = next((p for p in peers.get('peers') or [] if isinstance(p, dict) and p.get('project') == owner), None)
    if found is None:
        raise _unavailable(repo, owner, '%s is not a project of this organisation that the data store knows' % owner)
    url = (found.get('url') or '').rstrip('/')
    if not url.startswith(('https://', 'http://127.0.0.1:', 'http://localhost:')):   # the bearer never travels in clear
        raise _unavailable(repo, owner, 'project %s\'s data store has no public https address, so it cannot be read from a site' % owner)
    get('%s/api/v1/repositories/%s/refs/%s/objects/ls?amount=1' % (url, urllib.parse.quote(repo, safe=''), commit),
        'project %s\'s data store does not share it with this project (HTTP %%s): it was not approved, or the share was withdrawn' % owner)
    return url


def staging_record(repo, commit, kind, files=0, nbytes=0, seconds=0):
    """One record of status.run.staged (bounded, no paths): how a dataset came to be on scratch. The follower prints it."""
    return {'repo': repo, 'commit': commit[:12], 'kind': kind, 'files': int(files), 'bytes': int(nbytes),
            'seconds': int(seconds)}


def refused(rc, ev):
    # The staging tool refused this runner's contract (it was replaced after it answered `version`).
    if rc == EX_CONTRACT:
        raise StagingMismatch(mismatch_message(
            next((e.get('contract') for e in reversed(ev) if e.get('event') == 'contract'), None)))


def stage_once(stager, repo, ref, commit, lakefs, wallet, info=None, key=None, plan=None):
    # One staging attempt. Returns (path, retry): the ready path, or '' with
    # whether a later attempt can help. info (a dict) receives the staging record when the copy is ready. repo is the
    # repository in the data store at `lakefs`; key (default repo) is what the copy is filed under on the site.
    # plan (a dict) receives the bytes and files the copy needs, once they are listed and before it begins.
    key = key or repo
    rc, ev = stager(['version'])
    theirs = next((e.get('contract') for e in reversed(ev) if e.get('event') == 'version'), None)
    if rc in (EX_USAGE, 126, 127) and theirs is None:
        # Exit 2 with the tool's own usage error is a staging tool from before `version`. Anything else here is
        # the login node failing to run the tool at all (python exits 2 for a file it cannot open).
        if rc == EX_USAGE and 'kaos-stager' in (getattr(stager, 'last_error', '') or ''):
            raise StagingMismatch(mismatch_message(None))
        raise ToolAbsent(rc)
    if rc == 0 and theirs != CONTRACT:
        raise StagingMismatch(mismatch_message(theirs))
    if rc != 0:
        return '', rc in (EX_RETRY, 255)
    rc, ev = stager(['status', '--contract', CONTRACT, '--repo', key, '--commit', commit, '--ref', ref])
    refused(rc, ev)
    st = next((e for e in reversed(ev) if e.get('event') == 'status'), None)
    if rc != 0 or st is None:
        return '', rc in (EX_RETRY, 255)
    if st.get('ready'):
        print('stage: cache hit', st['path'], flush=True)
        if info is not None:
            info.update(staging_record(key, commit, 'cached'))
        return st['path'], False
    api = lakefs + '/api/v1/repositories/' + urllib.parse.quote(repo, safe='')
    # The whole tree of the commit, from the repository root.
    prefix = ''
    base = st.get('base') if st.get('base') and st.get('base_prefix') == prefix else None
    need_bytes = need_files = added = removed = 0
    names, skip = Names(repo, commit), set()
    for kind, path, size in stage_plan(api, commit, base, prefix, wallet):
        if kind == 'removed':
            removed += 1
            continue
        if not names.take(path, size):
            skip.add(path)                    # a folder marker: not sent, not counted
            continue
        need_bytes += size
        need_files += 1
        added += kind == 'added'
    files = (int(st.get('base_files') or 0) if base else 0) + added - removed
    if plan is not None:
        plan.update(bytes=need_bytes, files=need_files)
    print('stage: %s@%s %s: %d files to fetch (%.1f MB), %d to delete, %d in the version'
          % (repo, commit[:12], 'delta on ' + base[:12] if base else 'first copy',
             need_files, need_bytes / 1e6, removed, files), flush=True)

    def feed():
        yield {'op': 'auth', 'authorization': 'Bearer ' + wallet}
        for kind, path, size in stage_plan(api, commit, base, prefix, wallet):
            if kind == 'removed':
                yield {'op': 'delete', 'path': path}
            elif path not in skip:
                yield {'op': 'fetch', 'path': path, 'size': size,
                       'url': api + '/refs/' + commit + '/objects?'
                       + urllib.parse.urlencode({'path': path})}
        yield {'op': 'end', 'files': files, 'bytes': need_bytes, 'ref': ref, 'prefix': prefix}
    words = ['stage', '--contract', CONTRACT, '--repo', key, '--commit', commit, '--ref', ref,
             '--need-bytes', need_bytes, '--need-files', need_files]
    if base:
        words += ['--base', base]
    rc, ev = stager(words, feed())
    refused(rc, ev)
    done = next((e for e in reversed(ev) if e.get('event') == 'done'), None)
    if rc == 0 and done:
        if info is not None:
            # A stage another runner finished while this one waited for the stager's lock is a cache hit.
            info.update(staging_record(key, commit, 'cached') if done.get('cached') else staging_record(
                key, commit, 'delta' if base else 'first', need_files, need_bytes))
        return done['path'], False
    if rc == EX_NOSPACE:
        print('stage: NOT ENOUGH SPACE on MeluXina scratch for this version even after'
              ' evicting unused versions - free space or raise the quota', flush=True)
        # Not a fallback: the job's own copy would hit the same full quota mid-way (AC-33).
        raise InsufficientQuota(quota_message('dataset %s@%s' % (repo, commit[:12]), need_bytes, need_files))
    return '', rc in (EX_RETRY, 255)



def ssh_present():
    """The image ships openssh-client; nothing is installed at run time."""
    import shutil
    return shutil.which('ssh') is not None


def stage_dataset(cfg, inp, stager, mint_wallet, info=None, note=None):
    """The resident path of one input's pinned commit, or '' (the job then stages it in-job). inp is one entry
    of cfg.inputs (spec.inputs): its repository, label and commit; nothing is resolved here. Three failures
    are not a fallback:
    InsufficientQuota when the dataset does not fit the project's scratch quota (AC-33), StagingMismatch
    when the site's staging tool speaks another staging contract, and ReservedName when the dataset holds an
    object whose name is reserved for the platform or whose path could leave the dataset's folder. An input with an
    owner (a dataset another project shares) is staged from the owner's data store and never in-job: whatever
    would make an own dataset fall back to the job's download ends the run SharedDatasetUnavailable instead.
    info (a dict), when given, receives the staging record (staging_record) once the copy is resident; it stays
    empty when the job stages in-job. note(text), when given, is called every STAGE_NOTE_EVERY_S while a copy goes on
    (text says how far it is), and with None when staging is over. The time staging goes on asking again is
    stage_budget(size of the copy); an own dataset above STAGE_FALLBACK_MAX_BYTES that the site did not stage is
    DatasetNotStaged, not an in-job download."""
    repo, ref, commit = inp['repo'], inp['ref'], inp['commit']
    owner = inp.get('owner') or ''
    key = dataset_key(repo, owner)
    lakefs = cfg.lakefs_url
    if not lakefs:
        if owner:
            raise _no_job_download(repo, owner, 'this project has no data store address')
        print('stage: skipped (needs the lakeFS endpoint) - the job stages in-job', flush=True)
        return ''
    wallet = mint_wallet()
    if not wallet:
        if owner:
            raise _no_job_download(repo, owner, 'no data-plane token could be minted')
        return ''
    api = lakefs + '/api/v1/repositories/' + urllib.parse.quote(repo, safe='')
    if not stager.have_key() or not ssh_present():
        if owner:
            raise _no_job_download(repo, owner, 'the site\'s staging tool cannot be used (no staging key, or no ssh client)')
        print('stage: skipped (needs the staging key and an ssh client) - the job stages in-job', flush=True)
        # the in-job copy joins every name to its folder: check them all
        refuse_big_fallback(repo, commit, refuse_names(api, repo, commit, wallet), 'the site\'s staging tool cannot be used')
        return ''
    began = time.time()
    budget, attempt, absent_since, plan, sized, posted = STAGE_UNKNOWN_BUDGET_S, 0, None, {}, False, [0.0]

    def progress(ev):
        # One line, and one note for the job's status, every STAGE_NOTE_EVERY_S; the stager's own events come every 5 s.
        if ev.get('phase') != 'fetch' or not plan or time.time() - posted[0] < STAGE_NOTE_EVERY_S:
            return
        posted[0] = time.time()
        text = ('staging dataset %s: %d of %d files, %s of %s fetched in this attempt, staging since %s UTC'
                % (key, int(ev.get('files') or 0) + int(ev.get('skipped') or 0), plan['files'],
                   human_bytes(ev.get('bytes') or 0), human_bytes(plan['bytes']),
                   time.strftime('%Y-%m-%d %H:%M', time.gmtime(began))))
        print('stage:', text, flush=True)
        if note:
            note(text)
    stager.progress = progress
    while True:
        attempt += 1
        try:
            if attempt > 1:
                wallet = renewed_wallet(mint_wallet, wallet)    # never the token of an earlier attempt that is near its end
                if not wallet:
                    raise RuntimeError('no data-plane token could be minted')
            source = owner_store(lakefs, wallet, owner, repo, commit) if owner else lakefs
            path, retry = stage_once(stager, repo, ref, commit, source, wallet, info, key, plan)
        except (InsufficientQuota, StagingMismatch, ReservedName, SharedDatasetUnavailable):
            raise
        except ToolAbsent as e:
            if absent_since is None:
                absent_since = time.time()      # the 20 minutes count from the first "absent", not from the start
            if time.time() - absent_since > TOOL_WAIT_S:
                raise StagingMismatch(absent_message(e))
            print('stage: the staging tool is not installed or cannot be run (exit %s) - asking again' % e,
                  flush=True)
            path, retry = '', True
        except Exception as e:
            print('stage: attempt', attempt, 'failed:', e, flush=True)
            path, retry = '', True
        size = plan.get('bytes')
        if size is None and attempt == 1 and not path and retry:
            size = refuse_names(api, repo, commit, wallet)    # the stager did not get as far as listing the copy: size the commit
        if size is not None and not sized:
            sized, budget = True, stage_budget(size)
            print('stage: dataset %s is %s; staging may go on for %.1f hours' % (key, human_bytes(size), budget / 3600),
                  flush=True)
        elif size is None and attempt == 1 and not path and retry:
            print('stage: the size of dataset %s could not be read; staging may go on for %.1f hours'
                  % (key, budget / 3600), flush=True)
        if path or not retry or time.time() > began + budget:
            break
        time.sleep(min(60 * attempt, 600))
    stager.progress = None
    if note and posted[0]:
        note(None)
    if path and info and info.get('kind') != 'cached':
        info['seconds'] = int(time.time() - began)    # the whole staging, including resumed attempts
    if not path and owner:
        raise _no_job_download(repo, owner, 'the site could not stage it')
    if not path:
        print('stage: NOT STAGED - the job will download the dataset in-job on GPU time',
              flush=True)
        wallet = renewed_wallet(mint_wallet, wallet) or wallet     # the listing needs a token that is still good
        size = [n for n in (refuse_names(api, repo, commit, wallet), plan.get('bytes')) if n is not None]   # before it is submitted, as above
        refuse_big_fallback(repo, commit, max(size) if size else None, 'the site could not stage it in time')
    return path
