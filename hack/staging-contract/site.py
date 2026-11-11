"""The runner and the site's staging tool together, on a fake site (see README.md).

The real runner staging code (a runner/ directory of this repository) talks through a fake `ssh` to a real
staging tool (stager.py of the platform repository, in forced-command mode), which downloads from a fake
lakeFS and reads fake `lfs` / `squeue`. No test in either repository imports this file.

usage: site.py <runner dir> <stager.py> <dir of the staging tool's test_stager.py> <scenario>
scenarios: any-shape, old-partial, reserved-name, unsafe-name, new-runner-old-tool, old-runner-new-tool
"""
import http.server
import json
import os
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

C1, C2, C3, C4, C5 = 'c1' * 32, 'c2' * 32, 'c3' * 32, 'c4' * 32, 'c5' * 32
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
    with open(os.path.join(old, '.kaos-ready'), 'w') as f:     # as the old staging tool wrote it: no "full"
        json.dump({'repo': 'ds', 'commit': C3, 'ref': 'main', 'prefix': '', 'base': None, 'files': 3,
                   'bytes': 115, 'staged_at': 1.0, 'host': 'login02'}, f)
    aside = os.path.join(root, 'datasets', 'ds', '.partial-' + C3)
    os.environ['KAOS_TEST_LEASES'] = 'kaos-commit=' + C3       # a job of the old runner still reads it
    path, info = stage(C1)
    check('an old partial copy is not the base of another version, and stays while a job reads it',
          info['kind'] == 'first' and tree(path) == TREES[C1] and tree(old) == YOLO)
    path, info = stage(C3)
    check('an old partial copy of the same commit is not served as a cache hit', info['kind'] != 'cached')
    check('the commit path now holds the whole tree', path == old and tree(path) == TREES[C3]
          and marker(path).get('full') is True)
    check('the job that reads the partial copy keeps its files, out of the way', tree(aside) == YOLO)
    del os.environ['KAOS_TEST_LEASES']
    path, info = stage(C2)
    check('the partial copy is removed once no job leases it', not os.path.exists(aside) and tree(path) == TREES[C2])
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
