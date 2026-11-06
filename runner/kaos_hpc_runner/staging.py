"""Dataset staging through the site's login-node stager (PRD-HPC-1541 F-03/F-06).

The runner stages the pinned dataset commit into the project's Lustre cache from a login node
before the GPU job is submitted, so the job reads a resident copy instead of downloading on GPU
time. It talks to the stager the platform installed (~/.kaos/stager.py) through a forced-command key
that can run nothing else (Secret meluxina-staging: key + verified host pin). The stager names a
base (its newest ready commit of the repo); the runner lists only the lakeFS diff base..commit, or
the whole dataset for a first copy, and streams one JSON line per file. Unchanged files are hard
links of the base. Bytes come from the lakeFS API with the wallet bearer (sent once on stdin, held in
memory only). Any failure returns '' and the job stages in-job as before, saying so (F-06).

`Stager` is also what image staging, log streaming and the tar lease go through.
"""

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request

from . import settings

STAGE_BUDGET_S = 4 * 3600
EX_RETRY, EX_NOSPACE, EX_USAGE = 75, 76, 2


def resolve_commit(repo, ref, lakefs, wallet):
    """ref -> commit for a first step that has no upstream pin. The commit log
    (amount=1) is the proven endpoint: GET /refs/{ref} does not exist on our
    lakeFS version. Tags and branches both work as {ref}."""
    url = (lakefs + '/api/v1/repositories/' + urllib.parse.quote(repo, safe='')
           + '/refs/' + urllib.parse.quote(ref, safe='') + '/commits?amount=1')
    page = json.load(urllib.request.urlopen(urllib.request.Request(
        url, headers={'Authorization': 'Bearer ' + wallet}), timeout=60))
    return page['results'][0]['id']


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


def is_dataset_path(path):
    # The YOLO layout the steps read: data.yaml, images/, labels/ at the
    # root of the dataset repository. README and notes are not staged.
    return path == 'data.yaml' or path.split('/', 1)[0] in ('images', 'labels')


def stage_plan(api, commit, base, prefix, wallet):
    # (kind, path, size) per file the stager applies: the two-dot diff against
    # the base (three-dot would be wrong when the base is not an ancestor), or
    # every object of the commit for a first copy.
    if base:
        for d in lakefs_pages(api + '/refs/%s/diff/%s' % (base, commit), wallet,
                              {'prefix': prefix, 'type': 'two_dot'}):
            if d['type'] not in ('added', 'changed', 'removed'):
                raise ValueError('unexpected diff entry %s for %s' % (d['type'], d['path']))
            if is_dataset_path(d['path']):
                yield d['type'], d['path'], int(d.get('size_bytes') or 0)
    else:
        for o in lakefs_pages(api + '/refs/%s/objects/ls' % commit, wallet, {'prefix': prefix}):
            if is_dataset_path(o['path']):
                yield 'added', o['path'], int(o.get('size_bytes') or 0)


class Stager:
    """One stager subcommand on a login node per call: (exit code, JSON events)."""

    def __init__(self, cfg, staging_dir=settings.STAGING_DIR):
        self.cfg, self.staging_dir = cfg, staging_dir

    def have_key(self):
        import os
        return os.path.exists(self.staging_dir + '/id_ed25519')

    def __call__(self, words, feed=None, quiet=False):
        # The feed's records are written while the stager downloads; a feed that fails mid-way closes
        # stdin without the end record, which the stager keeps as resumable partial state (exit 75).
        # quiet: do not echo the events (the log subcommand's data is printed by the log streamer).
        d = self.staging_dir
        cmd = ['ssh', '-F', '/dev/null', '-i', d + '/id_ed25519', '-p', self.cfg.login_port,
               '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
               '-o', 'UserKnownHostsFile=' + d + '/known_hosts',
               '-o', 'ConnectTimeout=20', '-o', 'ServerAliveInterval=30',
               '-o', 'ServerAliveCountMax=4',
               self.cfg.identity()['user'] + '@' + self.cfg.login_host,
               ' '.join(shlex.quote(str(w)) for w in words)]
        # The pod runs as uid 65532 with a HOME that may not be writable: ssh gets a private one.
        home = tempfile.mkdtemp(prefix='kaos-ssh-')
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
        shutil.rmtree(home, ignore_errors=True)
        reader.join(30)
        err = p.stderr.read().strip()
        if err:
            print('stager stderr:', err[-2000:], flush=True)
        if failed is not None:
            raise failed
        return rc, events


def stage_once(stager, repo, ref, commit, lakefs, wallet):
    # One staging attempt. Returns (path, retry): the ready path, or '' with
    # whether a later attempt can help.
    rc, ev = stager(['status', '--repo', repo, '--commit', commit, '--ref', ref])
    st = next((e for e in reversed(ev) if e.get('event') == 'status'), None)
    if rc != 0 or st is None:
        return '', rc in (EX_RETRY, 255)
    if st.get('ready'):
        print('stage: cache hit', st['path'], flush=True)
        return st['path'], False
    api = lakefs + '/api/v1/repositories/' + urllib.parse.quote(repo, safe='')
    # The dataset sits at the repository root; is_dataset_path() keeps the rest out.
    prefix = ''
    base = st.get('base') if st.get('base') and st.get('base_prefix') == prefix else None
    need_bytes = need_files = added = removed = 0
    for kind, _, size in stage_plan(api, commit, base, prefix, wallet):
        if kind == 'removed':
            removed += 1
            continue
        need_bytes += size
        need_files += 1
        added += kind == 'added'
    files = (int(st.get('base_files') or 0) if base else 0) + added - removed
    print('stage: %s@%s %s: %d files to fetch (%.1f MB), %d to delete, %d in the version'
          % (repo, commit[:12], 'delta on ' + base[:12] if base else 'first copy',
             need_files, need_bytes / 1e6, removed, files), flush=True)

    def feed():
        yield {'op': 'auth', 'authorization': 'Bearer ' + wallet}
        for kind, path, size in stage_plan(api, commit, base, prefix, wallet):
            if kind == 'removed':
                yield {'op': 'delete', 'path': path}
            else:
                yield {'op': 'fetch', 'path': path, 'size': size,
                       'url': api + '/refs/' + commit + '/objects?'
                       + urllib.parse.urlencode({'path': path})}
        yield {'op': 'end', 'files': files, 'bytes': need_bytes, 'ref': ref, 'prefix': prefix}
    words = ['stage', '--repo', repo, '--commit', commit, '--ref', ref,
             '--need-bytes', need_bytes, '--need-files', need_files]
    if base:
        words += ['--base', base]
    rc, ev = stager(words, feed())
    done = next((e for e in reversed(ev) if e.get('event') == 'done'), None)
    if rc == 0 and done:
        return done['path'], False
    if rc == EX_NOSPACE:
        print('stage: NOT ENOUGH SPACE on MeluXina scratch for this version even after'
              ' evicting unused versions - free space or raise the quota', flush=True)
    return '', rc in (EX_RETRY, 255)



def ssh_present():
    """The image ships openssh-client; nothing is installed at run time."""
    import shutil
    return shutil.which('ssh') is not None


def stage_dataset(cfg, stager, mint_wallet):
    """(resident path of the pinned commit or '' (the job stages in-job), the commit pinned for this
    run: the upstream pin, else resolved here, else '')."""
    repo, ref = cfg.dataset_coords()
    commit = cfg.pinned_commit()
    lakefs = cfg.lakefs_url
    if not (repo and lakefs and stager.have_key()):
        print('stage: skipped (needs the dataset parameter, the lakeFS'
              ' endpoint and the staging key) - the job stages in-job', flush=True)
        return '', commit
    wallet = mint_wallet()
    if not wallet:
        return '', commit
    if not commit:
        # The first step of the run: nothing upstream pinned a commit, so pin it
        # here (the step then resolves the same ref; a vN tag cannot move).
        try:
            commit = resolve_commit(repo, ref, lakefs, wallet)
        except Exception as e:
            print('stage: cannot resolve %s@%s (%s) - the job stages in-job' % (repo, ref, e), flush=True)
            return '', commit
    if not ssh_present():
        print('stage: no ssh client - the job stages in-job', flush=True)
        return '', commit
    deadline, attempt = time.time() + STAGE_BUDGET_S, 0
    while True:
        attempt += 1
        try:
            path, retry = stage_once(stager, repo, ref, commit, lakefs, wallet)
        except Exception as e:
            print('stage: attempt', attempt, 'failed:', e, flush=True)
            path, retry = '', True
        if path or not retry or time.time() > deadline:
            break
        time.sleep(min(60 * attempt, 600))
    if not path:
        print('stage: NOT STAGED - the job will download the dataset in-job on GPU time',
              flush=True)
    return path, commit
