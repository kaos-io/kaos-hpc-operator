"""An old in-job copy is made again by the stage-in while another job's end-of-job cleanup runs (see README.md).

The copy is of the older layout (its files directly in the folder, the marker .complete), last used 20 days ago,
so the cleanup wants the folder. The stage-in (the real payload) moves it aside and fills a new copy (the files
under data/) from a fake lakeFS; the two data-cache cleanup lines of the real job script run again and again until
the stage-in ends. The folder being filled must never be taken from under the stage-in, and the copy must be
marked only when it is the whole commit. First seen in the review of 2026-11-12: stage-in rc 0,
marker present, 3410 of 10001 objects on disk.

usage: race.py <runner dir> [objects, default 10001]
"""
import atexit, http.server, json, os, shutil, socketserver, subprocess, sys, tempfile, threading, time, urllib.parse
runner = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 10001
sys.path.insert(0, runner)
from kaos_hpc_runner import jobscript   # noqa: E402
payload = os.path.join(runner, 'kaos_hpc_runner', 'payload', 'stagein.py')
COMMIT = 'c0ffee' * 8
third = (N - 1) * 3 // 10
YOLO = {'data.yaml': b'names: [x]'}
YOLO.update({'images/%05d.jpg' % i: b'a' * 10 for i in range(third)})
YOLO.update({'labels/%05d.txt' % i: b'0 0.5' for i in range(third)})
TREE = dict(YOLO)
TREE.update({'scenes/%05d.bin' % i: b'p' * 20 for i in range(N - 1 - 2 * third)})


class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(url.query)
        if url.path.endswith('/objects/ls'):
            body = json.dumps({'results': [{'path': p, 'path_type': 'object', 'size_bytes': len(b)}
                                           for p, b in sorted(TREE.items())],
                               'pagination': {'has_more': False}}).encode()
        else:
            time.sleep(0.004)
            body = TREE[q['path'][0]]
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


httpd = socketserver.ThreadingTCPServer(('127.0.0.1', 0), H)
httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()
tmp = tempfile.mkdtemp(prefix='race-')
_KEY = os.path.join(tmp, 'lakefs-key.json')   # the app's key file, as the site's staging tool places it
with open(_KEY, 'w') as _f:
    json.dump({'access_key_id': 'KAOSXA', 'secret_access_key': 'secret', 'expiration': '2099-01-01T00:00:00Z'}, _f)
atexit.register(shutil.rmtree, tmp, True)
scr = os.path.join(tmp, 'scr')
dest = os.path.join(scr, 'data-cache', 'ds', COMMIT)
for rel, body in YOLO.items():
    os.makedirs(os.path.dirname(os.path.join(dest, rel)), exist_ok=True)
    with open(os.path.join(dest, rel), 'wb') as f:
        f.write(body)
open(os.path.join(dest, '.complete'), 'w').close()
old = time.time() - 20 * 86400
os.utime(os.path.join(dest, '.complete'), (old, old))
env = dict(os.environ, LAKEFS_ENDPOINT='http://127.0.0.1:%d' % httpd.server_address[1], LAKEFS_KEY_FILE=_KEY,
           DATASET_REPO='ds', DATASET_COMMIT=COMMIT, DATASET_CACHE=os.path.join(scr, 'data-cache'),
           DATASET_DIRFILE=os.path.join(tmp, 'dirfile'), SLURM_JOB_ID='9')
lines = [l for l in jobscript.batch_lines('-', scr) if l.startswith('find $SCR/data-cache')]
assert len(lines) == 2, lines
cleanup = '\n'.join(lines)
p = subprocess.Popen([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                     universal_newlines=True)
time.sleep(float(os.environ.get('RACE_DELAY', '0.5')))   # the stage-in is at the folder; before that the copy is unused
runs = 0
while p.poll() is None:
    subprocess.run(['bash', '-c', cleanup], env=dict(os.environ, SCR=scr, USER=os.environ.get('USER', '')))
    runs += 1
    time.sleep(0.05)
out, err = p.communicate()
have = {os.path.relpath(os.path.join(d, n), os.path.join(dest, 'data')) for d, _, ns in os.walk(os.path.join(dest, 'data')) for n in ns}
marked = os.path.isfile(os.path.join(dest, '.kaos-complete'))
print('the cleanup ran %d times while the stage-in filled the folder' % runs)
print('stage-in rc %d, last line: %s' % (p.returncode, (out.strip().splitlines() or err.strip().splitlines() or [''])[-1]))
print('.kaos-complete present: %s, objects on disk under data/: %d of %d' % (marked, len(have & set(TREE)), len(TREE)))
again = 'downloaded %d of %d' % (len(TREE), len(TREE)) in out
leftovers = sorted(n for n in os.listdir(os.path.dirname(dest)) if n != COMMIT)
print('the whole commit was downloaded again, nothing of the old layout reused: %s; left beside the copy: %s'
      % (again, leftovers))
ok = (p.returncode == 0 and marked and have == set(TREE) and runs > 3 and again and not leftovers)
print('%s: a copy that is being made is never removed by another job\'s cleanup, and is marked only whole'
      % ('PASS' if ok else 'FAIL'))
sys.exit(0 if ok else 1)
