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
_KEY = os.path.join(tmp, 'lakefs-key.json')   # the app's key file, as the site's staging tool places it
with open(_KEY, 'w') as _f:
    json.dump({'access_key_id': 'KAOSXA', 'secret_access_key': 'secret', 'expiration': '2099-01-01T00:00:00Z'}, _f)
import atexit, shutil
atexit.register(shutil.rmtree, tmp, True)
dest = os.path.join(tmp, 'cache', 'ds', COMMIT)
def old_layout_copy(marker):
    """A copy of the older layout: files directly in the folder (here with a `data/` folder among them)."""
    for rel, body in dict(YOLO, **{'data/stale.bin': b'stale'}).items():
        os.makedirs(os.path.dirname(os.path.join(dest, rel)), exist_ok=True)
        open(os.path.join(dest, rel), 'wb').write(body)
    open(os.path.join(dest, marker), 'w').close()
old_layout_copy('.complete')
env = dict(os.environ, LAKEFS_ENDPOINT='http://127.0.0.1:%d' % httpd.server_address[1], LAKEFS_KEY_FILE=_KEY,
           DATASET_REPO='ds', DATASET_COMMIT=COMMIT, DATASET_CACHE=os.path.join(tmp, 'cache'),
           DATASET_DIRFILE=os.path.join(tmp, 'dirfile'), SLURM_JOB_ID='9')
def run():
    p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       universal_newlines=True)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()
def tree():
    out = {}
    data = os.path.join(dest, 'data')
    for d, _, names in os.walk(data):
        for n in names:
            out[os.path.relpath(os.path.join(d, n), data)] = open(os.path.join(d, n), 'rb').read()
    return out
print(sys.version.split()[0])
for marker in ('.complete', '.kaos-complete'):
    if marker == '.kaos-complete':
        shutil.rmtree(dest); old_layout_copy(marker)
    del downloads[:]
    out = run(); print(out)
    assert 'cache hit' not in out and 'older layout' in out
    assert tree() == TREE and sorted(downloads) == sorted(TREE), downloads
    assert open(os.path.join(dest, '.kaos-complete')).read() == 'data' and not os.path.exists(os.path.join(dest, '.complete'))
    assert sorted(os.listdir(dest)) == ['.kaos-complete', '.kaos-parts', 'data']
    print('PASS: a copy of the older layout (marker %s) was not served: moved aside, made again under data/' % marker)
del downloads[:]
out = run(); print(out)
assert 'cache hit' in out and downloads == []
print('PASS: the whole copy is a cache hit')
TREE['.kaos-commit'] = b'a name of a platform file'
os.remove(os.path.join(dest, '.kaos-complete'))
out = run(); print(out)
assert tree() == TREE and os.path.isfile(os.path.join(dest, '.kaos-complete'))
print('PASS: a dataset file named like a platform file is data under data/')
del TREE['.kaos-commit']
os.remove(os.path.join(dest, '.kaos-complete'))
TREE['a/b.kaos-part'] = b'x'
del downloads[:]
p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True)
print(p.stderr.strip())
assert p.returncode != 0 and downloads == [] and 'reserved for the platform' in p.stderr
print('PASS: a name ending in .kaos-part fails the stage-in before any download')
del TREE['a/b.kaos-part']
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
os.symlink(outside, os.path.join(dest, 'data', 'linked'))
TREE['linked/sub/x.bin'] = b'x'
p = subprocess.run([sys.executable, payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True)
print(p.stderr.strip())
assert p.returncode != 0 and os.listdir(outside) == [] and "outside the dataset's folder" in p.stderr
print('PASS: a folder of the copy that is a link to elsewhere is not written through')
