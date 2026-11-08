# Checkpoint copies of a step's checkpoint folder (PRD-HPC-1461 D-74, D-75). Runs on the compute node under the
# system python3 (3.6, stdlib only). The step writes into $KAOS_WORK/ckpt (KAOS_CHECKPOINT_DIR in the container);
# this program keeps at most TWO copies of that folder on scratch, each ONE tar archive plus ONE manifest, under
# $KAOS_CKPT_ROOT/<workflow uid>/<step>/ (never a file-by-file mirror: the project has a file quota).
#
#   watch    run beside the step: every few minutes, copy the folder when it changed and the space guard allows;
#            SIGUSR2 = copy now (the wall-time signal), SIGTERM = stop.
#   final    copy now (the step saved before the wall time, exit 85). Not subject to the guard; it replaces the
#            oldest copy, never adds a third.
#   restore  put the newest verified copy (the one named by KAOS_RESUME_REF / KAOS_RESUME_PREFIX, else the
#            previous good one) back into the folder. Exit 1 if none verifies: never a silent fresh start.
#   expire   delete the copies of steps untouched for 7 days, except those a queued or running job names with
#            kaos-ckpt-lease=<workflow uid>/<step> in its comment.
#
# After each copy the job comment carries: kaos-ckpt=<sha256 of the tar> kaos-ckpt-path=<tar path relative to the
# root> kaos-ckpt-info=<url-quoted JSON>; the runner reads them. The same line is kept in $KAOS_WORK/.kaos-note.
# The comment is written by the job script's exported bash function kaos_comment (read-modify-write, verified);
# `final` exits 1 when the comment could not be set.
import hashlib, json, os, re, shutil, signal, subprocess, sys, tarfile, time, urllib.parse

MODE = sys.argv[1] if len(sys.argv) > 1 else ''
WORK = os.environ.get('KAOS_WORK', '')
ROOT = os.environ.get('KAOS_CKPT_ROOT', '')
WF, STEP = os.environ.get('WF_UID', ''), os.environ.get('STEP_NAME', '')
FOLDER = os.path.join(WORK, 'ckpt')
STORE = os.path.join(ROOT, WF, STEP)
NOTE = os.path.join(WORK, '.kaos-note')
KEEP = 2
EXPIRY_S = 7 * 86400
MIN_EVERY_S, MAX_EVERY_S, DEFAULT_EVERY_S = 300, 3600, 900
SPACE_LIMIT, FILES_LIMIT = 0.8, 0.8
SEQ_RE = re.compile(r'^(\d{6})\.json$')
LEASE_RE = re.compile(r'kaos-ckpt-lease=(\S+)')
state = {'comment_ok': True, 'sig': None, 'digest': None, 'skipped': '', 'every': float(os.environ.get('KAOS_CKPT_EVERY_S') or DEFAULT_EVERY_S)}


def say(msg):
    print('checkpoint: ' + msg, flush=True)


# ------------------------------------------------------------------------------------------- the folder
def files_of(folder):
    out = []
    for dirpath, dirs, names in os.walk(folder):
        dirs[:] = sorted(d for d in dirs if not d.startswith('.tmp-'))   # a library save in progress
        for n in sorted(names):
            p = os.path.join(dirpath, n)
            if os.path.isfile(p) and not os.path.islink(p):
                out.append(os.path.relpath(p, folder))
    return out


def signature(folder, rels):
    sig = []
    for r in rels:
        st = os.stat(os.path.join(folder, r))
        sig.append((r, st.st_size, st.st_mtime_ns))
    return sig


class Hashing(object):
    """A write-only file object that hashes and counts what goes through it."""
    def __init__(self, fh):
        self.fh, self.sha, self.n = fh, hashlib.sha256(), 0

    def write(self, b):
        self.sha.update(b)
        self.n += len(b)
        return self.fh.write(b)


class Reading(object):
    def __init__(self, fh):
        self.fh, self.sha = fh, hashlib.sha256()

    def read(self, n=-1):
        b = self.fh.read(n)
        self.sha.update(b)
        return b


def progress_of(folder):
    """The progress the step reported: kaos-progress.json (checkpoint.progress(n)), else the newest library
    manifest in the folder (ckpt-*/manifest.json, or manifest.json). ([], '') when there is none."""
    try:
        m = json.load(open(os.path.join(folder, 'kaos-progress.json')))
        return [int(v) for v in m.get('values') or []], str(m.get('text') or '')[:150]
    except (OSError, ValueError, TypeError):
        pass
    cands = sorted([d for d in os.listdir(folder) if d.startswith('ckpt-') and os.path.isdir(os.path.join(folder, d))],
                   reverse=True)
    paths = [os.path.join(folder, d, 'manifest.json') for d in cands] + [os.path.join(folder, 'manifest.json')]
    for p in paths:
        try:
            m = json.load(open(p))
            return [int(v) for v in m.get('progress_values') or []], str(m.get('progress_text') or '')[:150]
        except (OSError, ValueError, TypeError):
            continue
    return [], ''


# ------------------------------------------------------------------------------------------- the store
def copies():
    """[(seq, manifest)] of the complete copies, oldest first. A tar without its manifest is not a copy."""
    out = []
    if os.path.isdir(STORE):
        for n in sorted(os.listdir(STORE)):
            m = SEQ_RE.match(n)
            if m:
                try:
                    man = json.load(open(os.path.join(STORE, n)))
                    if os.path.isfile(os.path.join(STORE, m.group(1) + '.tar')):
                        out.append((int(m.group(1)), man))
                except (OSError, ValueError):
                    pass
    return out


def tar_path(seq):
    return os.path.join(STORE, '%06d.tar' % seq)


def drop(seq):
    for ext in ('.json', '.tar'):
        try:
            os.unlink(os.path.join(STORE, '%06d%s' % (seq, ext)))
        except OSError:
            pass


def sweep_parts():
    if os.path.isdir(STORE):
        for n in os.listdir(STORE):
            if n.endswith('.part'):
                try:
                    os.unlink(os.path.join(STORE, n))
                except OSError:
                    pass


def tree_bytes(top):
    total = 0
    for dirpath, _, names in os.walk(top):
        for n in names:
            try:
                total += os.path.getsize(os.path.join(dirpath, n))
            except OSError:
                pass
    return total


# ------------------------------------------------------------------------------------------- space guard
def lfs_numbers(args):
    out = subprocess.check_output(['lfs', 'quota', '-q'] + args + [os.environ.get('KAOS_SCR') or ROOT],
                                  universal_newlines=True, timeout=60)
    tok = out.split()
    if tok and not tok[0][0].isdigit():
        tok = tok[1:]
    nums = [int(t.rstrip('*')) if t.rstrip('*').isdigit() else 0 for t in tok[:7]]
    return {'bytes': nums[0] * 1024, 'bytes_limit': nums[2] * 1024, 'files': nums[4], 'files_limit': nums[6]}


def guard(est_bytes, freed_bytes):
    """Why a PERIODIC copy must be skipped, or '' when it may go ahead. Fail closed: if the quota cannot be
    read, no periodic copy (the wall-time copy does not ask)."""
    projid = os.environ.get('KAOS_PROJID', '')
    try:
        q = lfs_numbers(['-p', projid]) if projid else lfs_numbers(['-u', os.environ.get('USER', '')])
    except Exception as e:
        return 'the project quota cannot be read (%s)' % str(e)[:80]
    need = max(est_bytes - freed_bytes, 0)
    if q['bytes_limit'] and (q['bytes'] + need) > SPACE_LIMIT * q['bytes_limit']:
        return 'it would take the project over %d%% of its scratch space' % int(SPACE_LIMIT * 100)
    if q['files_limit'] and (q['files'] + 3) > FILES_LIMIT * q['files_limit']:
        return 'the project is over %d%% of its scratch file quota' % int(FILES_LIMIT * 100)
    pct = int(os.environ.get('KAOS_CKPT_BUDGET_PCT') or 10)
    if q['bytes_limit']:
        budget = q['bytes_limit'] * pct // 100
        held = tree_bytes(ROOT) - freed_bytes
        if held + est_bytes > budget:
            return 'checkpoint copies would pass their budget (%d%% of the scratch space)' % pct
    return ''


# ------------------------------------------------------------------------------------------- note and comment
def info_of(skipped):
    cs = copies()
    info = {'sb': sum(m.get('tar_bytes', 0) for _, m in cs), 'pb': tree_bytes(ROOT) if ROOT else 0}
    if cs:
        m = cs[-1][1]
        info.update(p=m.get('progress_values') or [], pt=m.get('progress_text') or '', d=m.get('digest', '')[:64],
                    b=m.get('tar_bytes', 0))
    if skipped:
        info['sk'] = skipped[:250]
    return info


def write_note(skipped=''):
    cs = copies()
    toks = ['kaos-ckpt-lease=%s/%s' % (WF, STEP)]
    if cs:
        seq, m = cs[-1]
        toks += ['kaos-ckpt=' + m['tar_sha256'], 'kaos-ckpt-path=%s/%s/%s/%06d.tar' % (os.path.basename(ROOT), WF, STEP, seq)]
    toks.append('kaos-ckpt-info=' + urllib.parse.quote(json.dumps(info_of(skipped), separators=(',', ':')), safe=''))
    note = ' '.join(toks)
    try:
        open(NOTE, 'w').write(note)
    except OSError:
        pass
    # The job script's kaos_comment (exported function) is the one writer of the job comment: it keeps every
    # other word (kaos-commit=, kaos-image=, ...), retries and reads the words back. Never a blind scontrol here.
    try:
        rc = subprocess.call(['bash', '-c', 'kaos_comment "$@"', 'kaos_comment'] + toks,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
    except Exception:
        rc = 1
    state['comment_ok'] = rc == 0
    if rc != 0:
        say('the job comment could not be set (the copy is on scratch, but the job does not name it)')
    return rc == 0


# ------------------------------------------------------------------------------------------- copy
def digest_of(files):
    """The folder digest of a manifest's files ({rel: {'size', 'sha256'}})."""
    return hashlib.sha256(json.dumps(sorted(files.items()), separators=(',', ':')).encode()).hexdigest()


def folder_digest(rels):
    """The digest pack() would record for the folder now, without writing an archive. None when a file
    vanishes meanwhile (the step is saving; the copy is tried later)."""
    files = {}
    try:
        for r in rels:
            h, n = hashlib.sha256(), 0
            with open(os.path.join(FOLDER, r), 'rb') as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b''):
                    h.update(chunk)
                    n += len(chunk)
            files[r] = {'size': n, 'sha256': h.hexdigest()}
    except OSError:
        return None
    return digest_of(files)


class Unstable(Exception):
    pass


def pack(rels, seq):
    """Write <seq>.tar and <seq>.json; the manifest last, so a copy exists only when complete."""
    os.makedirs(STORE, exist_ok=True)
    tmp_tar, tmp_json = tar_path(seq) + '.part', os.path.join(STORE, '%06d.json.part' % seq)
    before = signature(FOLDER, rels)
    files, started = {}, time.time()
    with open(tmp_tar, 'wb') as raw:
        hw = Hashing(raw)
        with tarfile.open(fileobj=hw, mode='w|', format=tarfile.GNU_FORMAT) as tf:
            for r in rels:
                p = os.path.join(FOLDER, r)
                info = tf.gettarinfo(p, arcname=r)
                info.uid = info.gid = 0
                info.uname = info.gname = ''
                with open(p, 'rb') as fh:
                    rd = Reading(fh)
                    tf.addfile(info, rd)
                files[r] = {'size': info.size, 'sha256': rd.sha.hexdigest()}
        raw.flush()
        os.fsync(raw.fileno())
    if signature(FOLDER, rels) != before:
        raise Unstable('the step changed the folder while it was copied')
    digest = digest_of(files)
    values, text = progress_of(FOLDER)
    man = {'version': 1, 'seq': seq, 'time': time.time(), 'attempt': os.environ.get('KAOS_ATTEMPT', ''),
           'tar_sha256': hw.sha.hexdigest(), 'tar_bytes': hw.n, 'files': files, 'digest': digest,
           'progress_values': values, 'progress_text': text, 'seconds': round(time.time() - started, 2)}
    with open(tmp_json, 'w') as jf:
        json.dump(man, jf, separators=(',', ':'))
        jf.flush()
        os.fsync(jf.fileno())
    os.rename(tmp_tar, tar_path(seq))
    os.rename(tmp_json, os.path.join(STORE, '%06d.json' % seq))
    return man


def persist(force):
    """Copy the folder if it holds something new. Returns True when the store holds a copy of its current state."""
    if not os.path.isdir(FOLDER):
        return False
    rels = files_of(FOLDER)
    if not rels:
        return False
    sig = signature(FOLDER, rels)
    if sig == state['sig'] and copies():
        return True
    est = sum(s for _, s, _ in sig)
    cs = copies()
    if not force:
        freed = cs[0][1].get('tar_bytes', 0) if len(cs) >= KEEP else 0
        why = guard(est, freed)
        if why:
            state['skipped'] = 'skipped the periodic copy: ' + why
            say(state['skipped'] + '; the last copy is kept')
            write_note(state['skipped'])
            return bool(cs)
    newest = cs[-1] if cs else None
    if newest and folder_digest(rels) == newest[1].get('digest'):
        # Live 2026-11-08: the wall-time copy and the exit-85 final copy of one folder were both made.
        # Same content as the newest copy: nothing new to keep, and the older copy must not be dropped for it.
        state['sig'], state['skipped'] = sig, ''
        say('no new copy: the folder is the same as copy %06d (%s); %d kept' % (
            newest[0], newest[1].get('progress_text') or 'no progress value', len(cs)))
        write_note()
        return True
    sweep_parts()
    while len(copies()) >= KEEP:            # never a third copy: the oldest goes first, the newest stays
        drop(copies()[0][0])
    seq = max([s for s, _ in copies()] + [0]) + 1
    try:
        man = pack(rels, seq)
    except Unstable as e:
        say('%s; trying again later' % e)
        sweep_parts()
        return bool(copies())
    now = copies()
    if len(now) >= 2 and now[-2][1].get('digest') == man['digest']:
        # Only reached when the folder changed while it was read (the check above saw other content).
        drop(seq)
        say('no new copy: the folder is the same as copy %06d; %d kept' % (now[-2][0], len(copies())))
        write_note()
        return True
    state['sig'], state['skipped'] = sig, ''
    say('copy %06d: %d files, %d bytes, %s; %d kept' % (seq, len(rels), man['tar_bytes'], man.get('progress_text') or 'no progress value',
                                                          len(copies())))
    write_note()
    return True


# ------------------------------------------------------------------------------------------- restore
def verify_and_extract(seq, man):
    tarp = tar_path(seq)
    h = hashlib.sha256()
    with open(tarp, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    if h.hexdigest() != man.get('tar_sha256') or os.path.getsize(tarp) != man.get('tar_bytes'):
        raise ValueError('the archive does not match its checksum')
    shutil.rmtree(FOLDER, ignore_errors=True)
    os.makedirs(FOLDER)
    base = os.path.realpath(FOLDER)
    with tarfile.open(tarp, 'r') as tf:
        for m in tf:
            dest = os.path.realpath(os.path.join(FOLDER, m.name))
            if not m.isfile() or not (dest == base or dest.startswith(base + os.sep)):
                raise ValueError('unexpected member %r' % m.name)
        tf.extractall(FOLDER)
    for r, meta in man['files'].items():
        p = os.path.join(FOLDER, r)
        if not os.path.isfile(p) or os.path.getsize(p) != meta['size']:
            raise ValueError('%s is missing or has another size' % r)


def restore():
    want = os.environ.get('KAOS_RESUME_REF', '')
    cs = sorted(copies(), reverse=True)
    # The one the controller named first, then the previous good one.
    cs.sort(key=lambda c: 0 if c[1].get('tar_sha256') == want else 1)
    for seq, man in cs:
        try:
            verify_and_extract(seq, man)
            say('restored copy %06d (%s)%s' % (seq, man.get('progress_text') or 'no progress value',
                                               '' if man.get('tar_sha256') == want else ' - the newest one did not verify or is gone'))
            return 0
        except Exception as e:
            say('copy %06d cannot be used: %s' % (seq, e))
    say('no verified copy under %s: the step is not started from scratch' % STORE)
    return 1


# ------------------------------------------------------------------------------------------- expire
def leased():
    out = subprocess.check_output(['squeue', '-h', '-u', os.environ.get('USER', ''), '-o', '%k'],
                                  universal_newlines=True, timeout=60)
    return set(LEASE_RE.findall(out))


def expire():
    if not os.path.isdir(ROOT):
        return 0
    try:
        held = leased()
    except Exception as e:
        say('expiry skipped: the queue cannot be read (%s)' % str(e)[:80])
        return 0
    now = time.time()
    for wf in os.listdir(ROOT):
        for step in os.listdir(os.path.join(ROOT, wf)) if os.path.isdir(os.path.join(ROOT, wf)) else []:
            d = os.path.join(ROOT, wf, step)
            newest = max([os.path.getmtime(os.path.join(d, n)) for n in os.listdir(d)] + [os.path.getmtime(d)])
            if now - newest > EXPIRY_S and '%s/%s' % (wf, step) not in held:
                say('deleting the abandoned copies of %s/%s (older than 7 days)' % (wf, step))
                shutil.rmtree(d, ignore_errors=True)
        try:
            os.rmdir(os.path.join(ROOT, wf))
        except OSError:
            pass
    return 0


# ------------------------------------------------------------------------------------------- watch
def watch():
    flags = {'now': False, 'stop': False}
    signal.signal(signal.SIGUSR2, lambda *a: flags.update(now=True))
    signal.signal(signal.SIGTERM, lambda *a: flags.update(stop=True))
    last = time.time()
    while not flags['stop']:
        time.sleep(1)
        if flags['now']:
            flags['now'] = False
            say('the wall-time signal arrived: copying now')
            try:
                persist(True)
            except Exception as e:
                say('copy failed: %s' % e)
            last = time.time()
        elif time.time() - last >= state['every']:
            started = time.time()
            try:
                persist(False)
            except Exception as e:
                say('periodic copy failed: %s' % e)
            took = time.time() - started
            state['every'] = min(max(10 * took, MIN_EVERY_S), MAX_EVERY_S)   # saving stays near 10% of the run time
            last = time.time()
    return 0


if MODE == 'watch':
    sys.exit(watch())
if MODE == 'periodic':      # one periodic cycle (what `watch` does every few minutes), for tests
    sys.exit(0 if persist(False) else 1)
if MODE == 'final':
    ok = persist(True)
    has = bool(os.path.isdir(FOLDER) and files_of(FOLDER))
    # A copy the job comment does not name is as good as none: the controller continues only from the comment.
    sys.exit(0 if (ok and state['comment_ok']) or not has else 1)
if MODE == 'restore':
    sys.exit(restore())
if MODE == 'expire':
    sys.exit(expire())
print('usage: ckpt.py watch|final|restore|expire', file=sys.stderr)
sys.exit(2)
