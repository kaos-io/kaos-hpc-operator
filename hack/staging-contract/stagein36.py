"""The in-job stage-in payload under the site's Python (3.6, standard library only): fake lakeFS, the real
stagein.py. Run it with a Python 3.6 interpreter, see README.md.

usage: stagein36.py <path to runner/kaos_hpc_runner/payload/stagein.py>
"""
import http.server, json, os, socketserver, subprocess, sys, tempfile, threading, urllib.parse
payload = sys.argv[1]
COMMIT = 'c0ffee' * 8
YOLO = {'data.yaml': b'names: [x]', 'images/a.jpg': b'a' * 100, 'labels/a.txt': b'0 0.5'}
TREE = dict(YOLO, **{'README.md': b'# notes', 'scenes/0001/points.ply': b'p' * 300})
downloads = []
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(url.query)
        if url.path.endswith('/objects/ls'):
            body = json.dumps({'results': [{'path': p, 'path_type': 'object', 'size_bytes': len(b)}
                                           for p, b in sorted(TREE.items())],
                               'pagination': {'has_more': False}}).encode()
        else:
            downloads.append(q['path'][0])
            body = TREE[q['path'][0]]
        self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
httpd = socketserver.ThreadingTCPServer(('127.0.0.1', 0), H); httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()
tmp = tempfile.mkdtemp()
import atexit, shutil
atexit.register(shutil.rmtree, tmp, True)
dest = os.path.join(tmp, 'cache', 'ds', COMMIT)
for rel, body in YOLO.items():                      # the copy an older stage-in left, with its .complete marker
    os.makedirs(os.path.dirname(os.path.join(dest, rel)), exist_ok=True)
    open(os.path.join(dest, rel), 'wb').write(body)
open(os.path.join(dest, '.complete'), 'w').close()
env = dict(os.environ, LAKEFS_ENDPOINT='http://127.0.0.1:%d' % httpd.server_address[1], LAKEFS_BEARER_TOKEN='w',
           DATASET_REPO='ds', DATASET_COMMIT=COMMIT, DATASET_CACHE=os.path.join(tmp, 'cache'),
           DATASET_DIRFILE=os.path.join(tmp, 'dirfile'), SLURM_JOB_ID='9')
def run():
    p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       universal_newlines=True)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()
def tree():
    out = {}
    for d, _, names in os.walk(dest):
        for n in names:
            if n not in ('.kaos-complete', '.kaos-commit'):
                out[os.path.relpath(os.path.join(d, n), dest)] = open(os.path.join(d, n), 'rb').read()
    return out
print(sys.version.split()[0])
print(run())
assert tree() == TREE and sorted(downloads) == ['README.md', 'scenes/0001/points.ply'], downloads
assert os.path.exists(os.path.join(dest, '.kaos-complete')) and not os.path.exists(os.path.join(dest, '.complete'))
print('PASS: the old partial copy was completed, not served; only the missing files were downloaded')
del downloads[:]
out = run(); print(out)
assert 'cache hit' in out and downloads == []
print('PASS: the whole copy is a cache hit')
TREE['.kaos-commit'] = b'a reserved root name'
os.remove(os.path.join(dest, '.kaos-complete'))
p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True)
print(p.stderr.strip())
assert p.returncode != 0 and downloads == [] and 'reserved for the platform' in p.stderr
print('PASS: a reserved root name fails the stage-in before any download')
del TREE['.kaos-commit']
TREE['a/../../../escape.txt'] = b'out'
p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True)
print(p.stderr.strip())
assert p.returncode != 0 and downloads == [] and "outside the dataset's folder" in p.stderr
assert not any('escape' in n for _, _, names in os.walk(tmp) for n in names)
print('PASS: an object path that leaves the cache folder fails the stage-in before any download')
del TREE['a/../../../escape.txt']
outside = os.path.join(tmp, 'outside')
os.mkdir(outside)
os.symlink(outside, os.path.join(dest, 'linked'))
TREE['linked/sub/x.bin'] = b'x'
p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True)
print(p.stderr.strip())
assert p.returncode != 0 and os.listdir(outside) == [] and "outside the dataset's folder" in p.stderr
print('PASS: a folder of the copy that is a link to elsewhere is not written through')
