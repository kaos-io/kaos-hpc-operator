
import concurrent.futures, json, os, pathlib, sys, time, urllib.parse, urllib.request
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
# platform, so no file of a dataset can be taken for it. It is written last, after the files on disk were checked
# against the commit's listing. A copy that has only .complete was made when stage-in kept data.yaml, images/ and
# labels/ alone: it is completed below, never served as it is.
#
# The cleanup of unused copies (jobscript.py, the data-cache lines) and this program agree on three things:
#  1. The cleanup matches a folder only by an old marker (.kaos-complete, or .complete with no .kaos-complete
#     beside it). A folder being filled has neither: .complete is removed (or, when it is a file of the dataset,
#     made new) BEFORE the first download.
#  2. The cleanup takes <folder>.kaos-clean (mkdir, atomic), looks at the marker's age AGAIN, moves the folder away
#     in one rename and lets go; the slow delete happens on the moved folder.
#  3. This program changes the marker FIRST and looks for <folder>.kaos-clean AFTER. Whichever of the two came
#     second sees the other: the cleanup sees a new or missing marker and leaves the folder, or this program sees
#     the cleanup, waits the moment it takes, and starts over on what is there then.
#  4. One window is left in 3: the cleanup read the marker's age, this program renewed the marker, the cleanup
#     moved the folder and let go, and only then does this program look for the lock and find none. So a hit is
#     believed only if the marker is STILL in the folder after that look; the cleanup's move takes it away, and a
#     folder another stage-in has begun to fill again has none until that copy is whole. Otherwise: start over,
#     at most HIT_RETRIES times, then fill the folder like a miss (that is always safe).
MARK, OLD_MARK = dest / '.kaos-complete', dest / '.complete'
CLEAN = pathlib.Path(str(dest) + '.kaos-clean')
def cleaning():
    # True while a cleanup holds this folder. One left by a cleanup that was killed is not obeyed for ever.
    waited = False
    while True:
        try:
            if time.time() - CLEAN.stat().st_mtime > 600:
                return waited
        except OSError:
            return waited
        waited = True
        time.sleep(1)
HIT_RETRIES, lost = 5, 0
while True:
    try:
        os.utime(str(MARK), None)           # last use, for the cleanup; also what tells a cleanup to leave it
        hit = True
    except OSError:
        hit = False
        try:
            os.utime(str(OLD_MARK), None)   # an older copy this job is about to complete: not unused any more
        except OSError:
            pass
    if cleaning():
        continue                            # a cleanup was at this folder: look again at what is there now
    if hit and not MARK.is_file():          # the cleanup moved the folder after the renewal (rule 4)
        lost += 1
        if lost <= HIT_RETRIES:
            continue
        hit = False
    if hit:
        try:
            os.utime(str(OLD_MARK), None)   # a job script from before .kaos-complete cleans by this one
        except OSError:
            pass
        print('stage-in: cache hit', dest, flush=True)
        sys.exit(0)
    break
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
def name_rule(path, size=0):
    """Why a dataset object is not copied under its own name, or '' when it is.

    ONE RULE IN THREE PLACES, kept the same line for line and tested against one table
    (hack/staging-contract/names.json, and next to the staging tool):
      here                                           runner/kaos_hpc_runner/payload/stagein.py   name_rule
      this repository                                runner/kaos_hpc_runner/staging.py           name_rule
      novelcore/kubecore-operator  images/meluxina-tokenmint/stager/stager.py                    name_rule
    The runner applies it before it submits (ReservedName), so here it is the second line.

      'marker'    a zero-byte object whose name ends in "/": a folder marker, skipped
      'unsafe'    empty, a leading "/", an empty, "." or ".." part, a NUL byte: it could leave the folder
      'reserved'  a root name starting with .kaos- (the markers of a copy) or a file name ending in
                  .kaos-part (half-downloaded files); the .kaos/ folder is the platform's own and is copied
      'long'      a part longer than 255 bytes, or a file name longer than 245 (no room for .kaos-part)
    """
    if not isinstance(path, str):
        return 'unsafe'
    if path.endswith('/') and not size:
        return 'marker'
    parts = path.split('/')
    if not path or '\x00' in path or path.startswith('/') or any(p in ('', '.', '..') for p in parts):
        return 'unsafe'
    if parts[0].startswith('.kaos-') or parts[-1].endswith('.kaos-part'):
        return 'reserved'
    sizes = [len(p.encode('utf-8', 'replace')) for p in parts]
    if max(sizes) > 255 or sizes[-1] > 245:
        return 'long'
    return ''
OUTSIDE = 'it could be written outside the dataset\'s folder'
WHY = {
    'unsafe': 'a path that is empty, starts with "/", or has an empty part, a "." or ".." part or a NUL byte is '
              'not copied: ' + OUTSIDE,
    'reserved': 'names starting with .kaos are reserved for the platform (at the root of a dataset every name '
                'starting with .kaos-, and anywhere a file name ending in .kaos-part)',
    'long': 'a file name longer than 245 bytes, or a folder name longer than 255, cannot be written on the site',
}
def refuse(path, why):
    sys.exit('stage-in: dataset %s@%s has an object named %r: %s. Nothing was downloaded. Rename or remove the '
             'object and commit a new version of the dataset.' % (REPO, commit[:12], path, why))
# The whole tree of the commit, whatever the dataset's shape. Every name is judged before any download; folder
# markers are left out. The listing is in path order, so a file that is also a folder (a and a/b) is found by
# keeping the files the current path still begins with.
objs, files = [], []
for o in ls(''):
    path = o['path']
    why = name_rule(path, o.get('size_bytes') or 0)
    if why == 'marker':
        continue
    if why:
        refuse(path, WHY[why])
    files = [f for f in files if path.startswith(f)]
    for f in files:
        if path.startswith(f + '/'):
            refuse(f, 'the dataset also has %r, under a folder of the same name; a file system holds a file or a '
                      'folder under one name, not both' % path)
    files.append(path)
    objs.append(o)
print('stage-in:', len(objs), 'objects @', commit[:12], flush=True)
# From here the folder is being filled: no marker a cleanup could match (see 1 and 3 above).
try:
    if any(o['path'] == '.complete' for o in objs):
        os.utime(str(OLD_MARK), None)       # a file of the dataset: made new, downloaded again below if it differs
    else:
        OLD_MARK.unlink()                   # the marker of a copy made before .kaos-complete
except OSError:
    pass
cleaning()                                  # a cleanup that was already at the folder finishes first
# Where each file really lands: a folder of the copy that is a symbolic link is not written through.
ROOT = os.path.realpath(str(dest))
inside = set()
def check_inside(o, parent):
    if parent not in inside:
        real = os.path.realpath(parent)
        if real != ROOT and not real.startswith(ROOT + os.sep):
            raise SystemExit('stage-in: dataset %s@%s object %r: its folder on the site resolves to %s: %s. Nothing '
                             'was written there.' % (REPO, commit[:12], o['path'], real, OUTSIDE))
        inside.add(parent)
# Half-downloaded files live in one folder under a reserved root name, one per writer (another job may stage the
# same file, PRD-1609 F-02), so no name of the dataset is made longer or can be taken for one.
PARTS = dest / '.kaos-parts'
ME = '%s-%d' % (os.environ.get('SLURM_JOB_ID', ''), os.getpid())
def fetch(numbered):
    i, o = numbered
    p = dest / o['path']
    check_inside(o, str(p.parent))          # before the folder is made: nothing is created through a link
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        if p.lstat().st_size == o.get('size_bytes') and p.is_file() and not p.is_symlink():
            return 0
    except OSError:
        pass
    tmp = str(PARTS / ('%s-%d' % (ME, i)))
    try:
        got = 0
        with get(base + '/refs/' + commit + '/objects?'
                 + urllib.parse.urlencode({'path': o['path']})) as r:
            with open(tmp, 'wb') as f:
                while True:
                    b = r.read(1 << 20)
                    if not b:
                        break
                    f.write(b)
                    got += len(b)
        if got != o.get('size_bytes'):
            raise IOError('%r: downloaded %d bytes, the commit says %s' % (o['path'], got, o.get('size_bytes')))
        os.replace(tmp, str(p))
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return 1
PARTS.mkdir(parents=True, exist_ok=True)
with concurrent.futures.ThreadPoolExecutor(8) as ex:
    n = sum(ex.map(fetch, enumerate(objs)))
# The copy is marked only when what is on disk IS the commit: every listed file there with its size, and no file
# the listing does not have. Anything else is a failure, never a marker.
def on_disk(o):
    try:
        st = (dest / o['path']).lstat()
    except OSError:
        return o['path']
    return None if st.st_size == o.get('size_bytes') else o['path']
with concurrent.futures.ThreadPoolExecutor(8) as ex:
    wrong = [p for p in ex.map(on_disk, objs) if p]
listed, extra = set(o['path'] for o in objs), []
for d, dirs, names in os.walk(str(dest)):
    rel = os.path.relpath(d, str(dest))
    if rel == '.':
        dirs[:] = [x for x in dirs if not x.startswith('.kaos-')]
        names = [x for x in names if not x.startswith('.kaos-')]
    # Half-downloaded files of a writer from before .kaos-parts (name.part-<job>-<pid>) are never dataset files (a
    # dataset name of that shape is refused above) unless the commit lists them: one a killed writer left is removed
    # here, before the copy is marked. Not counted either: .complete, which a job script from before .kaos-complete
    # writes into the folder when it ends.
    for name in names:
        x = os.path.normpath(os.path.join(rel, name))
        if '.part-' in name and x not in listed:
            try:
                os.unlink(os.path.join(d, name))
            except OSError:
                extra.append(x)             # it stays and is counted: the copy is then not marked
        elif x not in listed and x != '.complete':
            extra.append(x)
if wrong or extra:
    sys.exit('stage-in: the copy of %s@%s on the site is not the commit: %d of %d files are missing or have another '
             'size (first: %r), %d files are not in the commit (first: %r). The copy was not marked complete. '
             'Another job may have removed the folder while it was filled; run the step again.'
             % (REPO, commit[:12], len(wrong), len(objs), (wrong or [None])[0], len(extra), (extra or [None])[0]))
for leftover in PARTS.iterdir():            # this writer's, and those a killed writer left more than a day ago
    try:
        if leftover.name.startswith(ME + '-') or time.time() - leftover.lstat().st_mtime > 86400:
            leftover.unlink()
    except OSError:
        pass
MARK.touch()
print('stage-in: downloaded', n, 'of', len(objs), '->', dest, flush=True)
