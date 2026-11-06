
# Checkpoint stage-in for a continued job: download hpc-checkpoints/.../attempt-<n>/ at the commit the
# previous attempt staged out ($KAOS_RESUME_REF, $KAOS_RESUME_PREFIX) into <work>/resume, each file to a
# part-file renamed when complete and checked against its size. Exits non-zero if anything is
# missing: a continuation never silently starts from scratch.
import json, os, pathlib, sys, urllib.parse, urllib.request
base = os.environ['LAKEFS_ENDPOINT'].rstrip('/') + '/api/v1'
H = {'Authorization': 'Bearer ' + os.environ['LAKEFS_BEARER_TOKEN']}
repo, ref, prefix = os.environ['OUTPUTS_REPO'], os.environ['KAOS_RESUME_REF'], os.environ['KAOS_RESUME_PREFIX']
dest = pathlib.Path(os.environ['KAOS_WORK']) / 'resume'
def get(path):
    return urllib.request.urlopen(urllib.request.Request(base + path, headers=H), timeout=600)
objs, after = [], ''
while True:
    q = urllib.parse.urlencode({'prefix': prefix, 'after': after, 'amount': 1000})
    page = json.load(get('/repositories/%s/refs/%s/objects/ls?%s' % (repo, ref, q)))
    objs += [o for o in page.get('results', []) if o.get('path_type', 'object') == 'object']
    if not page.get('pagination', {}).get('has_more'):
        break
    after = page['pagination']['next_offset']
if not objs:
    print('checkpoint stage-in: nothing at', ref, prefix, flush=True); sys.exit(1)
for o in objs:
    target = dest / o['path'][len(prefix):]
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + '.part')
    q = urllib.parse.urlencode({'path': o['path']})
    with get('/repositories/%s/refs/%s/objects?%s' % (repo, ref, q)) as r, open(str(part), 'wb') as fh:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            fh.write(chunk)
    if part.stat().st_size != o['size_bytes']:
        print('checkpoint stage-in: short download of', o['path'], flush=True); sys.exit(1)
    os.replace(str(part), str(target))
print('checkpoint stage-in: %d files from %s@%s %s' % (len(objs), repo, ref[:12], prefix), flush=True)
