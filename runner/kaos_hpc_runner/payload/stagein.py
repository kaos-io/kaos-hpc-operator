
import concurrent.futures, json, os, pathlib, sys, urllib.parse, urllib.request
EP = os.environ['LAKEFS_ENDPOINT'].rstrip('/')
H = {'Authorization': 'Bearer ' + os.environ['LAKEFS_BEARER_TOKEN']}
REPO = os.environ['DATASET_REPO']
def get(url):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=H), timeout=120)
base = EP + '/api/v1/repositories/' + urllib.parse.quote(REPO, safe='')
# The commit the pipeline pinned for the run, so this step reads exactly what
# the in-cluster steps read. Nothing is resolved here.
commit = os.environ['DATASET_COMMIT']
dest = pathlib.Path(os.environ['DATASET_CACHE']) / REPO / commit
open(os.environ['DATASET_DIRFILE'], 'w').write(str(dest))
if (dest / '.complete').exists():
    (dest / '.complete').touch()            # last use, for the job's cache cleanup
    (dest / '.kaos-commit').write_text(commit)  # the kubecore consumer checks it
    print('stage-in: cache hit', dest, flush=True)
    sys.exit(0)
def ls(prefix):
    objs, after = [], ''
    while True:
        page = json.load(get(base + '/refs/' + commit + '/objects/ls?'
                             + urllib.parse.urlencode(
                                 {'amount': 1000, 'after': after,
                                  'prefix': prefix})))
        objs += [r for r in page.get('results') or []
                 if r.get('path_type') == 'object']
        pg = page.get('pagination') or {}
        if not pg.get('has_more'):
            return objs
        after = pg.get('next_offset') or ''
# Only the dataset: the repository root may also hold other files (README,
# notes) the steps never read.
objs = [o for o in ls('') if o['path'] == 'data.yaml'
        or o['path'].split('/', 1)[0] in ('images', 'labels')]
print('stage-in:', len(objs), 'objects @', commit[:12], flush=True)
def fetch(o):
    p = dest / o['path']
    if p.exists() and p.stat().st_size == o.get('size_bytes'):
        return 0
    p.parent.mkdir(parents=True, exist_ok=True)
    # PRD-1609 F-02: one part-file per writer — another job may stage the same file.
    tmp = '%s.part-%s-%d' % (p, os.environ.get('SLURM_JOB_ID', ''), os.getpid())
    try:
        with get(base + '/refs/' + commit + '/objects?'
                 + urllib.parse.urlencode({'path': o['path']})) as r:
            with open(tmp, 'wb') as f:
                while True:
                    b = r.read(1 << 20)
                    if not b:
                        break
                    f.write(b)
        os.replace(tmp, p)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return 1
with concurrent.futures.ThreadPoolExecutor(8) as ex:
    n = sum(ex.map(fetch, objs))
(dest / '.complete').touch()
(dest / '.kaos-commit').write_text(commit)   # the kubecore consumer checks it
print('stage-in: downloaded', n, 'of', len(objs), '->', dest, flush=True)
