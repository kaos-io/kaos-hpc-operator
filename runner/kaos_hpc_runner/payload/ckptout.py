
# Checkpoint stage-out (PRD-HPC-1461 D-44 as amended 2026-11-05, T-19): a resumable step saved
# <work>/output/checkpoint/ with kaos_lib.checkpoint and exited 85. Before the job ends, upload
# it to the app's outputs repository on the branch hpc-ckpt-<wf-uid>-<step> (kept, so the
# checkpoint stays in lineage) at hpc-checkpoints/<wf-uid>/<step>/attempt-<n>/, check every
# file's size, commit, and write the commit id to $KAOS_CKPT_FILE. Files are streamed, never
# held in memory. Any failure exits non-zero: no commit id, no continuation.
import http.client, json, os, pathlib, sys, time, urllib.error, urllib.parse, urllib.request
ep = urllib.parse.urlsplit(os.environ['LAKEFS_ENDPOINT'].rstrip('/'))
base = os.environ['LAKEFS_ENDPOINT'].rstrip('/') + '/api/v1'
H = {'Authorization': 'Bearer ' + os.environ['LAKEFS_BEARER_TOKEN']}
repo = os.environ['OUTPUTS_REPO']
wf, step, attempt = os.environ['WF_UID'], os.environ['STEP_NAME'], os.environ.get('KAOS_ATTEMPT') or '1'
branch = 'hpc-ckpt-%s-%s' % (wf, step)
prefix = 'hpc-checkpoints/%s/%s/attempt-%s/' % (wf, step, attempt)
src = pathlib.Path(os.environ['KAOS_WORK']) / 'output' / 'checkpoint'
files = sorted(f for f in src.rglob('*') if f.is_file()) if src.is_dir() else []
if not files:
    print('checkpoint stage-out: nothing in', src, flush=True); sys.exit(1)
def call(method, path, body=None, ok=(200, 201)):
    req = urllib.request.Request(base + path, method=method, headers=dict(H, **{'Content-Type': 'application/json'}),
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read() or b'{}')
    except urllib.error.HTTPError as e:
        return e.code, {}
code, _ = call('POST', '/repositories/%s/branches' % repo, {'name': branch, 'source': os.environ.get('OUTPUTS_REF') or 'main'})
if code not in (200, 201, 409):   # 409: an earlier attempt created it
    print('checkpoint stage-out: cannot create', branch, code, flush=True); sys.exit(1)
def upload(f, path):
    size = f.stat().st_size
    pre = ('--B\r\nContent-Disposition: form-data; name="content"; filename="f"\r\n'
           'Content-Type: application/octet-stream\r\n\r\n').encode()
    post = b'\r\n--B--\r\n'
    q = urllib.parse.urlencode({'path': path})
    conn = (http.client.HTTPSConnection if ep.scheme == 'https' else http.client.HTTPConnection)(ep.netloc, timeout=600)
    conn.putrequest('POST', ep.path + '/api/v1/repositories/%s/branches/%s/objects?%s' % (repo, branch, q))
    for k, v in dict(H, **{'Content-Type': 'multipart/form-data; boundary=B',
                          'Content-Length': str(len(pre) + size + len(post))}).items():
        conn.putheader(k, v)
    conn.endheaders()
    conn.send(pre)
    with open(f, 'rb') as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            conn.send(chunk)
    conn.send(post)
    status = conn.getresponse().status
    conn.close()
    if status not in (200, 201):
        raise IOError('upload %s: HTTP %d' % (path, status))
    code, st = call('GET', '/repositories/%s/refs/%s/objects/stat?%s' % (repo, branch, q))
    if code != 200 or st.get('size_bytes') != size:
        raise IOError('upload %s: stored size %s, local %d' % (path, st.get('size_bytes'), size))
total = 0
for f in files:
    path = prefix + f.relative_to(src).as_posix()
    for n in range(4):
        try:
            upload(f, path)
            break
        except Exception as e:
            if n == 3:
                print('checkpoint stage-out:', e, flush=True); sys.exit(1)
            time.sleep(5 * (n + 1))
    total += f.stat().st_size
code, c = call('POST', '/repositories/%s/branches/%s/commits' % (repo, branch),
               {'message': 'checkpoint: %s attempt %s' % (step, attempt),
                'metadata': {'kaos.kind': 'checkpoint', 'workflow_uid': wf, 'step': step, 'attempt': str(attempt)}})
if code not in (200, 201) or not c.get('id'):
    print('checkpoint stage-out: commit failed', code, flush=True); sys.exit(1)
open(os.environ['KAOS_CKPT_FILE'], 'w').write(c['id'] + ' ' + prefix)
print('checkpoint stage-out: %d files, %d bytes -> %s@%s %s' % (len(files), total, branch, c['id'][:12], prefix), flush=True)
