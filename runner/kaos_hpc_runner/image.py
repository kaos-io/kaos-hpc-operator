"""Image pre-staging (D-62 leases the result).

A job can queue for longer than the registry token minted at submit lives (GCE metadata token, 1 h):
live 2026-11-05, three GPU jobs queued ~70 min, `apptainer pull` then failed "invalid
username/password" (exit 231), and the waiter resubmitted and lost the queue wait again. While the
token is fresh, the runner fetches the image's layers from the registry and streams them to the
stager as a docker-archive tarball ({root}/images/{key}.tar, key = the job script's SIF cache key).
The job builds its SIF from it with no registry access and deletes the tar afterwards. Any failure
returns '' and the job pulls as before. The stager only moves bytes (login-node policy); the runner
does the registry work with the stdlib, streaming: layers are copied as they are (compressed), never
held whole in memory.
"""

import base64
import hashlib
import json
import re
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request

from .staging import EX_NOSPACE, EX_RETRY, InsufficientQuota, human_bytes, ssh_present

IMAGE_STAGE_BUDGET_S = 45 * 60
IMAGE_MANIFEST_ACCEPT = ', '.join([
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json'])


def registry_token_host(host):
    # Same rule as the job script: the GCP token goes to GAR hosts only;
    # presenting it to Zot turns an anonymous-OK pull into 401.
    return host.endswith('-docker.pkg.dev')


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    # Redirects are followed by hand so the registry's Authorization header
    # never reaches the blob store it redirects to.
    def redirect_request(self, *a, **kw):
        return None


class Registry:
    def __init__(self, host, token=''):
        local = host.startswith(('localhost', '127.0.0.1'))
        self.host, self.base = host, ('http://' if local else 'https://') + host
        self.token = token if registry_token_host(host) else ''
        self.auth = None
        self.opener = urllib.request.build_opener(_NoRedirect)

    def _challenge(self, header):
        # Docker token flow: Bearer realm/service/scope -> token endpoint,
        # asked with Basic oauth2accesstoken:<token> (GAR) or anonymously.
        scheme = header.split(' ', 1)[0].lower()
        creds = ('Basic ' + base64.b64encode(
            ('oauth2accesstoken:' + self.token).encode()).decode()) if self.token else None
        if scheme == 'basic':
            return creds
        if scheme != 'bearer':
            return None
        p = dict(re.findall(r'(\w+)="([^"]*)"', header))
        if not p.get('realm'):
            return None
        q = {k: p[k] for k in ('service', 'scope') if p.get(k)}
        req = urllib.request.Request(p['realm'] + ('?' + urllib.parse.urlencode(q) if q else ''),
                                     headers={'Authorization': creds} if creds else {})
        with self.opener.open(req, timeout=60) as r:
            body = json.load(r)
        t = body.get('token') or body.get('access_token')
        return 'Bearer ' + t if t else None

    def open(self, path, accept=None):
        url, challenged = self.base + path, False
        for _ in range(10):
            headers = {'Accept': accept} if accept else {}
            if self.auth and url.startswith(self.base + '/'):
                headers['Authorization'] = self.auth
            try:
                return self.opener.open(urllib.request.Request(url, headers=headers), timeout=60)
            except urllib.error.HTTPError as e:
                if e.code == 401 and not challenged and url.startswith(self.base + '/'):
                    challenged = True
                    self.auth = self._challenge(e.headers.get('WWW-Authenticate') or '')
                    if self.auth:
                        continue
                elif e.code in (301, 302, 303, 307, 308) and e.headers.get('Location'):
                    url = urllib.parse.urljoin(url, e.headers['Location'])
                    continue
                raise
        raise IOError('too many redirects for ' + path)

    def blob_bytes(self, repo, digest, limit=8 << 20):
        with self.open('/v2/%s/blobs/%s' % (repo, digest)) as r:
            body = r.read(limit + 1)
        if len(body) > limit or hashlib.sha256(body).hexdigest() != digest.split(':', 1)[1]:
            raise ValueError('blob %s: too large or digest mismatch' % digest)
        return body

    def manifest(self, repo, ref):
        with self.open('/v2/%s/manifests/%s' % (repo, ref), IMAGE_MANIFEST_ACCEPT) as r:
            body = r.read(8 << 20)
        if ref.startswith('sha256:') and hashlib.sha256(body).hexdigest() != ref[7:]:
            raise ValueError('manifest digest mismatch for ' + ref)
        return json.loads(body)


def parse_image(ref):
    # name@sha256:... (or name:tag) -> (registry host, repository, reference).
    name, _, digest = ref.partition('@')
    tag = ''
    if not digest:
        head, _, last = name.rpartition('/')
        if ':' in last:
            last, _, tag = last.partition(':')
            name = head + '/' + last
    host, _, repo = name.partition('/')
    if not repo or not ('.' in host or ':' in host or host == 'localhost'):
        raise ValueError('not a registry host reference: ' + ref)
    return host, repo, digest or tag or 'latest'


def image_manifest(reg, repo, ref):
    # The single-platform image manifest: an index is resolved to linux/amd64.
    m = reg.manifest(repo, ref)
    if m.get('manifests') is not None:
        pick = next((d for d in m['manifests']
                     if (d.get('platform') or {}).get('os') == 'linux'
                     and (d.get('platform') or {}).get('architecture') == 'amd64'), None)
        if pick is None:
            raise ValueError('no linux/amd64 image in the index')
        m = reg.manifest(repo, pick['digest'])
    if not m.get('layers') or not m.get('config'):
        raise ValueError('unsupported manifest (needs an OCI or Docker v2 image manifest)')
    return m


def layer_suffix(media_type):
    return '.tar.gz' if 'gzip' in media_type else '.tar.zst' if 'zstd' in media_type else '.tar'


def image_plan(reg, repo, m):
    # The docker-archive's entries: (tar name, size, digest or None, bytes or None).
    cfg = m['config']['digest']
    cfg_name = cfg.split(':', 1)[1] + '.json'
    layers = [(d['digest'].split(':', 1)[1] + layer_suffix(d.get('mediaType') or ''), d)
              for d in m['layers']]
    manifest_json = json.dumps([{'Config': cfg_name, 'RepoTags': ['kaos/staged:latest'],
                                 'Layers': [n for n, _ in layers]}]).encode()
    entries = [('manifest.json', len(manifest_json), None, manifest_json)]
    config = reg.blob_bytes(repo, cfg)
    entries.append((cfg_name, len(config), None, config))
    seen = set()
    for name, d in layers:
        if name not in seen:
            seen.add(name)
            entries.append((name, int(d['size']), d['digest'], None))
    total = 1024 + sum(512 + (size + 511) // 512 * 512 for _, size, _, _ in entries)
    return entries, total


def image_feed(reg, repo, entries, deadline):
    # The tar as byte chunks, then the tar's sha256 as one hex line. A layer
    # that does not match its digest raises before the rest is sent: the
    # stager sees a short stream and keeps nothing.
    whole = hashlib.sha256()

    def out(b):
        whole.update(b)
        return b
    for name, size, digest, data in entries:
        info = tarfile.TarInfo(name)
        info.size, info.mode = size, 0o644
        yield out(info.tobuf(tarfile.USTAR_FORMAT))
        got, h = 0, hashlib.sha256()
        if data is not None:
            got = len(data)
            yield out(data)
        else:
            with reg.open('/v2/%s/blobs/%s' % (repo, digest)) as r:
                while True:
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    if time.time() > deadline:
                        raise TimeoutError('image staging budget spent')
                    got += len(chunk)
                    h.update(chunk)
                    yield out(chunk)
            if got != size or 'sha256:' + h.hexdigest() != digest:
                raise ValueError('layer %s: size or digest mismatch (%d of %d bytes)' % (digest, got, size))
        yield out(b'\0' * (-size % 512))
    yield out(b'\0' * 1024)
    yield (whole.hexdigest() + '\n').encode()



def image_key(img):
    """The job script's SIF cache key: sha256sum | cut -c1-16 of IMAGE_REF. It names the staged tar
    and is the `kaos-image=` lease in the job comment."""
    return hashlib.sha256(img.encode()).hexdigest()[:16]


def cleanup_images(stager):
    """Ask the stager for its image cleanup (PRD-HPC-1461 AC-65): superseded SIFs, tars of jobs that
    ended or were cancelled, the layer cache; over the images budget the least recently used SIFs no
    queued or running job names. Deletion on the login node, never what a job leases. The snapshot job runs
    it every ten minutes anyway; this is the prompt one, after this attempt's job is over. Never raises:
    an older stager has no `gc`, and a refused or failed call only means the next periodic run does it."""
    try:
        if not stager.have_key() or not ssh_present():
            return
        rc, ev = stager(['gc'], quiet=True)
        done = next((e for e in reversed(ev) if e.get('event') == 'gc'), None)
        if rc == 0 and done and done.get('freed_bytes'):
            print('image: cleanup freed %s (%s)' % (human_bytes(done['freed_bytes']), ', '.join(
                '%s x%d' % (k, v.get('count', 0)) for k, v in sorted((done.get('by_reason') or {}).items()))),
                flush=True)
    except Exception as e:
        print('image: cleanup skipped (%s)' % e, flush=True)


def stage_image(img, reg_token, stager, strict=False):
    # The staged tar's path on MeluXina, or '' (the job pulls). Never raises, except InsufficientQuota
    # when strict: the layers do not fit the project's scratch quota (AC-33), and the job's own pull
    # and SIF build would hit the same full quota. A resubmit is not strict: its job is already cancelled.
    if not stager.have_key():
        print('image: staging skipped (no staging key) - the job pulls', flush=True)
        return ''
    try:
        if not ssh_present():
            print('image: no ssh client - the job pulls', flush=True)
            return ''
        key = image_key(img)
        rc, ev = stager(['image-status', '--key', key])
        st = next((e for e in reversed(ev) if e.get('event') == 'image_status'), None)
        if st is None:
            print('image: the stager has no image mode (rc %s) - the job pulls' % rc, flush=True)
            return ''
        if st.get('ready'):
            print('image: already staged', st['path'], flush=True)
            return st['path']
        if st.get('sif'):
            # AC-65: the job script builds nothing and would only delete a tar, so none is staged. The
            # job's kaos-image= lease keeps this SIF from the stager's cleanup while the job queues.
            print('image: the SIF is already on scratch - nothing to stage', flush=True)
            return ''
        host, repo, ref = parse_image(img)
        reg = Registry(host, reg_token)
        entries, total = image_plan(reg, repo, image_manifest(reg, repo, ref))
        print('image: staging %s: %d layers, %.1f MB' % (img, len(entries) - 2, total / 1e6), flush=True)
        deadline = time.time() + IMAGE_STAGE_BUDGET_S
        for attempt in (1, 2):
            try:
                rc, ev = stager(['image', '--key', key, '--size', total],
                                image_feed(reg, repo, entries, deadline))
            except Exception as e:
                print('image: attempt', attempt, 'failed:', e, flush=True)
                continue
            done = next((e for e in reversed(ev) if e.get('event') == 'done'), None)
            if rc == 0 and done:
                return done['path']
            if rc == EX_NOSPACE:
                print('image: NOT ENOUGH SPACE on MeluXina scratch for the image layers', flush=True)
                if strict:
                    raise InsufficientQuota(
                        'not enough room on the project\'s scratch for the step image (%s): the quota has no '
                        'space left for it even after removing every unused copy. Nothing was copied and nothing '
                        'in use was deleted. Free space on the project\'s scratch or ask for a larger quota.'
                        % human_bytes(total))
                return ''
            if rc not in (EX_RETRY, 255):
                break
    except InsufficientQuota:
        raise
    except Exception as e:
        print('image: staging failed (%s) - the job pulls' % e, flush=True)
        return ''
    print('image: NOT STAGED - the job pulls from the registry', flush=True)
    return ''

