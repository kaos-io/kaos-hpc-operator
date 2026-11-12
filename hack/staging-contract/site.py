"""The runner and the site's staging tool together, on a fake site (see README.md).

The real runner staging code (a runner/ directory of this repository) talks through a fake `ssh` to a real
staging tool (stager.py of the platform repository, in forced-command mode), which downloads from a fake
lakeFS and reads fake `lfs` / `squeue`. No test in either repository imports this file.

usage: site.py <runner dir> <stager.py> <dir of the staging tool's test_stager.py> <scenario>
scenarios: any-shape, old-partial, reserved-name, unsafe-name, uncopyable, folder-marker, file-folder-delta,
           tool-absent (give a stager.py path that does not exist), new-runner-old-tool, old-runner-new-tool
"""
import atexit
import http.server
import json
import os
import shutil
import socketserver
import stat
import sys
import tempfile
import threading
import urllib.parse

runner_dir, stager_py, stager_tests, scenario = sys.argv[1:5]
sys.path.insert(0, runner_dir)
sys.path.insert(0, os.path.join(runner_dir, 'tests'))
sys.path.insert(0, stager_tests)
import helpers            # noqa: E402
import test_stager        # noqa: E402  (FAKE_LFS, FAKE_SQUEUE)
from kaos_hpc_runner import staging   # noqa: E402

C1, C2, C3, C4, C5, C6, C7, C8, C9, CA, CB = ('c%s' % c * 32 for c in '123456789ab')
YOLO = {'data.yaml': b'names: [x]', 'images/a.jpg': b'a' * 100, 'labels/a.txt': b'0 0.5'}
TREES = {
    # not YOLO-shaped: no data.yaml, images/ or labels/
    C1: {'scenes/0001/points.ply': b'p' * 300, 'scenes/0001/poses.json': b'{}',
         'calibration/cam0.json': b'{"fx": 1}', 'README.md': b'# a 3D dataset', 'splits.csv': b'0001,train\n',
         '.kaos/seal.json': b'{}'},          # the platform's own folder inside a dataset: staged like any other
    C2: {'scenes/0001/points.ply': b'p' * 300, 'scenes/0002/points.ply': b'q' * 200,
         'calibration/cam0.json': b'{"fx": 2}', 'README.md': b'# a 3D dataset', 'splits.csv': b'0001,train\n0002,val\n'},
    C3: dict(YOLO, **{'README.md': b'# notes', 'calibration/cam0.json': b'{"fx": 1}'}),
    C4: {'scenes/0001/points.ply': b'p' * 300, '.kaos-ready': b'not a marker'},     # a reserved root name
    C5: {'scenes/0001/points.ply': b'p' * 300, 'a/../../../escape.txt': b'out'},    # a path that leaves the folder
    C6: {'a': b'x', 'a-b': b'w', 'a/b': b'y', 'z.txt': b'z'},                       # a file that is also a folder
    C7: {'d/' + 'n' * 250: b'x', 'ok': b'y'},                                       # a file name too long for the site
    C8: {'docs/': b'', 'docs/a.txt': b'a', 'empty/': b'', 'k': b'2'},               # folder markers
    # between two versions: the folder `a` becomes a file, the file `f` becomes a folder
    C9: {'a/b': b'1', 'a/c/d': b'2', 'f': b'3', 'keep': b'k'},
    CA: {'a': b'now a file', 'f/g': b'now a folder', 'keep': b'k'},
}


class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(url.query)
        parts = url.path.split('/')          # /api/v1/repositories/<repo>/refs/<ref>/...
        ref, rest = parts[6], parts[7:]
        prefix = (q.get('prefix') or [''])[0]
        if rest == ['objects', 'ls']:
            body = json.dumps({'results': [{'path': p, 'path_type': 'object', 'size_bytes': len(b)}
                                           for p, b in sorted(TREES[ref].items()) if p.startswith(prefix)],
                               'pagination': {'has_more': False}}).encode()
        elif rest[:1] == ['diff']:
            a, b = TREES[ref], TREES[rest[1]]
            res = [{'path': p, 'path_type': 'object', 'size_bytes': len(b.get(p, b'')),
                    'type': 'removed' if p not in b else 'added' if p not in a else 'changed'}
                   for p in sorted(set(a) | set(b)) if a.get(p) != b.get(p) and p.startswith(prefix)]
            body = json.dumps({'results': res, 'pagination': {'has_more': False}}).encode()
        else:
            body = TREES[ref][q['path'][0]]
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


httpd = socketserver.ThreadingTCPServer(('127.0.0.1', 0), H)
httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()

tmp = tempfile.mkdtemp(prefix='site-')
atexit.register(shutil.rmtree, tmp, True)
root = os.path.join(tmp, 'kaos', 'proj')
bindir = os.path.join(tmp, 'bin')
os.makedirs(bindir)
ssh = '''#!/bin/sh
# the login node: no shared connection, and the forced command of the staging key
case " $* " in *" -O "*|*" -N -f "*) exit 255 ;; esac
for last; do :; done
SSH_ORIGINAL_COMMAND="$last" exec %s %s --root %s --account p201342 --scratch %s --forced
''' % (sys.executable, stager_py, root, tmp)
for name, body in (('lfs', test_stager.FAKE_LFS), ('squeue', test_stager.FAKE_SQUEUE), ('ssh', ssh)):
    with open(os.path.join(bindir, name), 'w') as f:
        f.write(body)
    os.chmod(os.path.join(bindir, name), stat.S_IRWXU)
os.environ.update(PATH=bindir + os.pathsep + os.environ['PATH'], KAOS_TEST_ROOT=tmp)
keydir = os.path.join(tmp, 'key')
os.makedirs(keydir)
open(os.path.join(keydir, 'id_ed25519'), 'w').close()
staging.time.sleep = lambda s: None


def tree(path):
    out = {}
    for d, _, names in os.walk(path):
        for n in names:
            rel = os.path.relpath(os.path.join(d, n), path)
            if not rel.startswith('.kaos-'):
                with open(os.path.join(d, n), 'rb') as f:
                    out[rel] = f.read()
    return out


def stage(commit):
    cfg = helpers.settings(DATASET_REPO='ds', DATASET_COMMIT=commit,
                           LAKEFS_EXTERNAL_URL='http://127.0.0.1:%d' % httpd.server_address[1])
    info = {}
    path = staging.stage_dataset(cfg, staging.Stager(cfg, staging_dir=keydir), lambda: 'wallet', info=info)
    return path, info


def marker(path):
    with open(os.path.join(path, '.kaos-ready')) as f:
        return json.load(f)


def check(what, ok):
    print('%s: %s' % ('PASS' if ok else 'FAIL', what), flush=True)
    if not ok:
        sys.exit(1)


if scenario == 'any-shape':
    path, info = stage(C1)
    check('a tree with no data.yaml, images/ or labels/ is complete on the site (%d files)' % len(TREES[C1]),
          path and tree(path) == TREES[C1])
    check('its marker says full, and the staging record is a first copy of every file',
          marker(path).get('full') is True and (info['kind'], info['files']) == ('first', len(TREES[C1])))
    path, info = stage(C2)
    check('the next version is a delta on it and is complete too', tree(path) == TREES[C2] and info['kind'] == 'delta')
    path, info = stage(C2)
    check('a whole copy is a cache hit', info['kind'] == 'cached')
elif scenario == 'old-partial':
    old = os.path.join(root, 'datasets', 'ds', C3)
    for rel, body in YOLO.items():
        os.makedirs(os.path.dirname(os.path.join(old, rel)), exist_ok=True)
        with open(os.path.join(old, rel), 'wb') as f:
            f.write(body)
    def partial(commit, age):
        """A copy as the staging tool of before the contract made it: the YOLO folders, a marker without "full"."""
        d = os.path.join(root, 'datasets', 'ds', commit)
        for rel, body in YOLO.items():
            os.makedirs(os.path.dirname(os.path.join(d, rel)), exist_ok=True)
            with open(os.path.join(d, rel), 'wb') as f:
                f.write(body)
        with open(os.path.join(d, '.kaos-ready'), 'w') as f:
            json.dump({'repo': 'ds', 'commit': commit, 'ref': 'main', 'prefix': '', 'base': None, 'files': 3,
                       'bytes': 115, 'staged_at': 1.0, 'host': 'login02'}, f)
        os.utime(os.path.join(d, '.kaos-ready'), (time.time() - age, time.time() - age))
        return d
    import time
    old = partial(C3, 3 * 86400)          # three days old, read by a job of the old runner
    recent = partial(CB, 600)             # an old caller was told "ready" ten minutes ago; its job is not queued yet
    unused = partial(C4, 3 * 86400)       # three days old, no job
    folder = os.stat(old).st_ino
    os.environ['KAOS_TEST_LEASES'] = 'kaos-commit=' + C3
    path, info = stage(C1)
    check('an old partial copy is not the base of another version', info['kind'] == 'first' and tree(path) == TREES[C1])
    check('a leased one and one used in the last day stay; an old unused one goes',
          tree(old) == YOLO and tree(recent) == YOLO and not os.path.exists(unused))
    path, info = stage(C3)
    check('an old partial copy of the same commit is not served as a cache hit', info['kind'] != 'cached')
    check('it is completed where it is: the same folder the job reads, now the whole tree',
          path == old and os.stat(old).st_ino == folder and tree(path) == TREES[C3]
          and marker(path).get('full') is True)
    check('no second copy and no staging folder is left',
          sorted(os.listdir(os.path.join(root, 'datasets', 'ds'))) == sorted([C1, C3, CB]))
elif scenario in ('reserved-name', 'unsafe-name'):
    commit, what = (C4, 'a reserved root name') if scenario == 'reserved-name' else (
        C5, 'an object path that leaves its folder')
    for key in (True, False):         # with the staging key (login-node staging) and without it (in-job)
        if not key:
            os.remove(os.path.join(keydir, 'id_ed25519'))
        try:
            stage(commit)
            check('the run stops', False)
        except staging.ReservedName as e:
            print('message:', e)
            check('a dataset with %s stops the run %s the staging key, nothing staged'
                  % (what, 'with' if key else 'without'),
                  not os.path.exists(os.path.join(root, 'datasets'))
                  and not any('escape' in n for _, _, names in os.walk(tmp) for n in names))
elif scenario == 'uncopyable':
    for commit, what, said in ((C6, 'a file that is also a folder', 'a file or a folder under one name'),
                               (C7, 'a file name too long for the site', 'longer than 245 bytes')):
        for key in (True, False):
            open(os.path.join(keydir, 'id_ed25519'), 'w').close() if key else os.remove(
                os.path.join(keydir, 'id_ed25519'))
            try:
                stage(commit)
                check('the run stops', False)
            except staging.ReservedName as e:
                print('message:', str(e)[:300])
                check('a dataset with %s stops the run %s the staging key, nothing staged'
                      % (what, 'with' if key else 'without'),
                      said in str(e) and not os.path.exists(os.path.join(root, 'datasets')))
elif scenario == 'folder-marker':
    path, info = stage(C8)
    files = {k: v for k, v in TREES[C8].items() if not k.endswith('/')}
    check('folder markers (docs/, empty/) are skipped and the dataset is complete on the site',
          path and tree(path) == files and info['files'] == len(files) and marker(path)['files'] == len(files))
elif scenario == 'file-folder-delta':
    path, info = stage(C9)
    check('the first version is on the site', tree(path) == TREES[C9])
    path, info = stage(CA)
    check('a version where a folder became a file and a file became a folder is staged as a delta, complete',
          info['kind'] == 'delta' and tree(path) == TREES[CA])
    check('the base is untouched', tree(os.path.join(root, 'datasets', 'ds', C9)) == TREES[C9])
elif scenario == 'tool-absent':
    clock = [1000.0]
    staging.time.time = lambda: clock[0]
    staging.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
    try:
        stage(C1)
        check('the run stops', False)
    except staging.StagingMismatch as e:
        print('message:', e)
        check('a staging tool that is not installed: asked for 20 minutes, then the run stops with its own sentence',
              'is not installed for this project' in str(e) and 'staging contract' not in str(e)
              and clock[0] - 1000.0 > 20 * 60 and not os.path.exists(os.path.join(root, 'datasets')))
elif scenario == 'new-runner-old-tool':
    try:
        stage(C1)
        check('the run stops', False)
    except staging.StagingMismatch as e:
        print('message:', e)
        check('new runner, old staging tool: StagingMismatch, nothing staged',
              not os.path.exists(os.path.join(root, 'datasets')))
elif scenario == 'old-runner-new-tool':
    path, info = stage(C3)
    check('old runner, new staging tool: refused, no path, nothing staged',
          path == '' and info == {} and not os.path.exists(os.path.join(root, 'datasets')))
