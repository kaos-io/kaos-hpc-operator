
# F-05 stage-out: the step's declared outputs (/work/output/<name>.json,
# what Argo would have read as outputs.parameters in-cluster) go to lakeFS
# at hpc-outputs/{wf-uid}/{step}/ on a hand-off branch of the app's outputs
# repository (outputs-<app>); the waiter fetches them back into the twin's outputs so
# downstream steps see real values, then deletes the branch. They never touch `main`:
# lakeFS refuses a merge into a branch with uncommitted objects, and the step's real
# results (checkpoints) are written by the step itself, inside the image, with the
# kubecore package's run writer (branch run-<run>-<kind>, merged into main).
import json, os, pathlib, urllib.error, urllib.parse, urllib.request
base = os.environ['LAKEFS_ENDPOINT'].rstrip('/') + '/api/v1'
H = {'Authorization': 'Bearer ' + os.environ['LAKEFS_BEARER_TOKEN']}
repo = os.environ['OUTPUTS_REPO']
branch = 'hpc-%s-%s' % (os.environ['WF_UID'], os.environ['STEP_NAME'])
try:
    urllib.request.urlopen(urllib.request.Request(
        base + '/repositories/%s/branches' % repo, method='POST',
        data=json.dumps({'name': branch, 'source': os.environ.get('OUTPUTS_REF') or 'main'}).encode(),
        headers=dict(H, **{'Content-Type': 'application/json'})), timeout=60).read()
except urllib.error.HTTPError as e:
    if e.code != 409:   # 409: a retry of this stage-out created it already
        raise
prefix = 'hpc-outputs/%s/%s/' % (os.environ['WF_UID'], os.environ['STEP_NAME'])
out = pathlib.Path(os.environ['KAOS_WORK']) / 'output'
for name in [n for n in os.environ.get('STEP_OUTPUTS', '').split(',') if n]:
    f = out / (name + '.json')
    if not f.exists():
        print('stage-out: no', f, flush=True); continue
    data = f.read_bytes()
    body = (b'--B\r\nContent-Disposition: form-data; name="content"; filename="f"\r\n'
            b'Content-Type: application/octet-stream\r\n\r\n' + data + b'\r\n--B--\r\n')
    q = urllib.parse.urlencode({'path': prefix + name + '.json'})
    req = urllib.request.Request(base + '/repositories/%s/branches/%s/objects?%s' % (repo, branch, q),
                                 data=body, method='POST',
                                 headers=dict(H, **{'Content-Type': 'multipart/form-data; boundary=B'}))
    urllib.request.urlopen(req, timeout=120).read()
    print('stage-out:', name, len(data), 'bytes ->', prefix, flush=True)
