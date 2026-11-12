"""A cache hit on a whole copy that the end-of-job cleanup moves away at the worst moment (see README.md).

The copy is whole and its marker is 20 days old. The cleanup has taken the lock and read that age; then the
stage-in (the real payload) renews the marker; then the cleanup moves the folder and lets the lock go; only then
does the stage-in look for the lock. The hook runs the real payload, and right after the first renewal of
`.kaos-complete` does the cleanup's move (the second case: also another stage-in begins filling the folder again).
The stage-in must never fail with FileNotFoundError, never report a cache hit on a folder that is gone or not whole,
and must end with a whole copy.

usage: race-hit.py <runner dir>
"""
import http.server, json, os, shutil, socketserver, subprocess, sys, tempfile, threading, time, urllib.parse
runner = sys.argv[1]
payload = os.path.join(runner, 'kaos_hpc_runner', 'payload', 'stagein.py')
COMMIT = 'c0ffee' * 8
TREE = {'data.yaml': b'names: [x]', 'images/a.jpg': b'a' * 10, 'scenes/0001/p.bin': b'p' * 20}
HOOK = r'''
import os, sys
payload, mode = sys.argv[1], os.environ['RACE_MODE']
real, done = os.utime, []
def utime(p, t=None):
    real(p, t)
    if str(p).endswith('.kaos-complete') and not done:
        done.append(1)
        d = os.path.dirname(str(p))
        os.rename(d, d + '.kaos-trash-777')            # the cleanup's move; its lock goes with it
        if mode == 'moved+refill':
            os.makedirs(os.path.join(d, '.kaos-parts'))  # another stage-in has begun filling the folder
os.utime = utime
exec(compile(open(payload).read(), payload, 'exec'))
'''


class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path.endswith('/objects/ls'):
            body = json.dumps({'results': [{'path': p, 'path_type': 'object', 'size_bytes': len(b)}
                                           for p, b in sorted(TREE.items())],
                               'pagination': {'has_more': False}}).encode()
        else:
            body = TREE[urllib.parse.parse_qs(url.query)['path'][0]]
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


httpd = socketserver.ThreadingTCPServer(('127.0.0.1', 0), H)
httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()
ok = True
for mode in ('moved', 'moved+refill'):
    tmp = tempfile.mkdtemp(prefix='race-hit-')
    dest = os.path.join(tmp, 'cache', 'ds', COMMIT)
    for rel, body in TREE.items():
        os.makedirs(os.path.dirname(os.path.join(dest, 'data', rel)), exist_ok=True)
        with open(os.path.join(dest, 'data', rel), 'wb') as f:
            f.write(body)
    marker = os.path.join(dest, '.kaos-complete')
    with open(marker, 'w') as f:
        f.write('data')
    old = time.time() - 20 * 86400
    os.utime(marker, (old, old))
    env = dict(os.environ, RACE_MODE=mode, LAKEFS_ENDPOINT='http://127.0.0.1:%d' % httpd.server_address[1],
               LAKEFS_BEARER_TOKEN='w', DATASET_REPO='ds', DATASET_COMMIT=COMMIT,
               DATASET_CACHE=os.path.join(tmp, 'cache'), DATASET_DIRFILE=os.path.join(tmp, 'dirfile'),
               SLURM_JOB_ID='9')
    p = subprocess.run([sys.executable, '-c', HOOK, payload], env=env, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, universal_newlines=True, timeout=120)
    data = os.path.join(dest, 'data')
    have = {os.path.relpath(os.path.join(d, n), data) for d, _, ns in os.walk(data) for n in ns} \
        if os.path.isdir(data) else set()
    whole = set(TREE) == have and os.path.isfile(marker)
    crashed = 'FileNotFoundError' in p.stderr
    # `cache hit` is allowed only for a folder that is whole when it is said
    hit_on_nothing = 'cache hit' in p.stdout and not whole
    case_ok = p.returncode == 0 and whole and not crashed and not hit_on_nothing
    print('%s: %s | rc %d | %s | whole copy at the end: %s' % (
        'PASS' if case_ok else 'FAIL', mode, p.returncode,
        (p.stdout.strip().splitlines() or p.stderr.strip().splitlines() or [''])[-1][:100], whole))
    ok = ok and case_ok
    shutil.rmtree(tmp, ignore_errors=True)
sys.exit(0 if ok else 1)
