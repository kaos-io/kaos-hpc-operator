
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
# The marker of a whole copy is .kaos-complete: names starting with .kaos- at the root are reserved for the
# platform, so no file of a dataset can be taken for it. A copy that has only .complete was made when stage-in
# kept data.yaml, images/ and labels/ alone: it is completed below, never served as it is.
# What the step finds the copy by (kaos-lib, $KAOS_STAGED_ROOT/<input>/.kaos-ready): which repository, which
# commit, and that it is the whole tree. Written last, under a name only this job writes, then renamed.
def ready():
    tmp = dest / ('.kaos-ready.kaos-part-%s-%d' % (os.environ.get('SLURM_JOB_ID', ''), os.getpid()))
    tmp.write_text(json.dumps({'repo': REPO, 'commit': commit, 'full': True}))
    os.replace(str(tmp), str(dest / '.kaos-ready'))
if (dest / '.kaos-complete').exists():
    (dest / '.kaos-complete').touch()       # last use, for the job's cache cleanup
    ready()
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
# The whole tree of the commit, whatever the dataset's shape.
objs = ls('')
def refuse(path, why):
    sys.exit('stage-in: dataset %s@%s has an object named %r: %s. Nothing was downloaded. Rename or remove the '
             'object and commit a new version of the dataset.' % (REPO, commit[:12], path, why))
OUTSIDE = 'it could be written outside the dataset\'s folder'
# Every name before any download, by the rules of the site's staging tool (and of the runner's name_problem).
for o in objs:
    path = o['path']
    parts = path.split('/')
    if not path or '\x00' in path or path.startswith('/') or any(p in ('', '.', '..') for p in parts):
        refuse(path, 'a path that is empty, starts with "/", or has an empty part, a "." or ".." part or a NUL '
                     'byte is not copied: ' + OUTSIDE)
    if path.startswith('.kaos-'):
        refuse(path, 'names starting with .kaos are reserved for the platform (at the root of a dataset every name '
                     'starting with .kaos-)')
# And where each file really lands: a folder of the copy that is a symbolic link is not written through.
ROOT = os.path.realpath(str(dest))
inside = set()
def check_inside(o, parent):
    if parent not in inside:
        real = os.path.realpath(parent)
        if real != ROOT and not real.startswith(ROOT + os.sep):
            raise SystemExit('stage-in: dataset %s@%s object %r: its folder on the site resolves to %s: %s. Nothing '
                             'was written there.' % (REPO, commit[:12], o['path'], real, OUTSIDE))
        inside.add(parent)
print('stage-in:', len(objs), 'objects @', commit[:12], flush=True)
def fetch(o):
    p = dest / o['path']
    check_inside(o, str(p.parent))          # before the folder is made: nothing is created through a link
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists() and p.stat().st_size == o.get('size_bytes'):
        return 0
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
if not any(o['path'] == '.complete' for o in objs) and (dest / '.complete').exists():
    (dest / '.complete').unlink()           # the marker of a copy made before .kaos-complete
(dest / '.kaos-complete').touch()
ready()
print('stage-in: downloaded', n, 'of', len(objs), '->', dest, flush=True)
