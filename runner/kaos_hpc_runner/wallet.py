"""The data-plane wallet: a short-lived Zitadel bearer minted from the mounted machine key.

Same JWT-profile recipe as the seeded mlflow_zitadel_auth module (assertion aud = https://{domain};
the project-audience scope is what makes the token acceptable to mlflow-oidc-auth AND carries the
groups claim). The in-pod waiter pip-installed PyJWT + cryptography for the RS256 signature; the
runner image is stdlib only and never installs anything at run time, so the signature is computed
here (RSASSA-PKCS1-v1_5 / SHA-256, RFC 8017 section 8.2, on Python integers). Any failure degrades
loudly to no-wallet (the job still runs, data-plane env omitted) - never the machine key itself into
the Slurm environment (T-03: HPC-side env must only ever see short-lived tokens).
"""

import base64
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request

from . import settings

# Resubmit a still-PENDING job before its wallet (MLflow/lakeFS bearer, 12 h lifetime) expires: a job
# that starts after a queue wait longer than the wallet would train blind. The wallet cannot be
# refreshed inside a queued job, so the job itself is replaced.
WALLET_MARGIN_S = 30 * 60

_SHA256_PREFIX = bytes.fromhex('3031300d060960864801650304020105000420')


def wallet_exp(w):
    """exp claim of a JWT as epoch seconds (0 when unknown)."""
    try:
        seg = w.split('.')[1]
        return float(json.loads(base64.urlsafe_b64decode(seg + '=' * (-len(seg) % 4)))['exp'])
    except Exception:
        return 0.0


def _der(data, pos=0):
    """One DER element at pos: (tag, content, position after it)."""
    tag = data[pos]
    n = data[pos + 1]
    pos += 2
    if n & 0x80:
        k = n & 0x7f
        n = int.from_bytes(data[pos:pos + k], 'big')
        pos += k
    return tag, data[pos:pos + n], pos + n


def _seq(data):
    items, pos = [], 0
    while pos < len(data):
        tag, val, pos = _der(data, pos)
        items.append((tag, val))
    return items


def rsa_private_numbers(pem):
    """(n, d) from a PKCS#1 ('RSA PRIVATE KEY') or PKCS#8 ('PRIVATE KEY') PEM."""
    m = re.search(r'-----BEGIN ([A-Z ]*PRIVATE KEY)-----(.*?)-----END', pem, re.S)
    if not m or m.group(1) not in ('RSA PRIVATE KEY', 'PRIVATE KEY'):
        raise ValueError('not an unencrypted RSA private key PEM')
    der = base64.b64decode(re.sub(r'\s+', '', m.group(2)))
    tag, body, _ = _der(der)
    fields = _seq(body)
    if m.group(1) == 'PRIVATE KEY':        # PKCS#8: version, AlgorithmIdentifier, OCTET STRING(PKCS#1)
        tag, body, _ = _der(fields[2][1])
        fields = _seq(body)
    ints = [int.from_bytes(v, 'big') for _, v in fields[:4]]   # version, n, e, d
    return ints[1], ints[3]


def rs256(pem, signing_input):
    n, d = rsa_private_numbers(pem)
    k = (n.bit_length() + 7) // 8
    t = _SHA256_PREFIX + hashlib.sha256(signing_input).digest()
    em = b'\x00\x01' + b'\xff' * (k - len(t) - 3) + b'\x00' + t
    return pow(int.from_bytes(em, 'big'), d, n).to_bytes(k, 'big')


def _b64(b):
    return base64.urlsafe_b64encode(b).rstrip(b'=').decode()


def jwt_rs256(claims, pem, kid):
    head = _b64(json.dumps({'alg': 'RS256', 'typ': 'JWT', 'kid': kid}, separators=(',', ':')).encode())
    body = _b64(json.dumps(claims, separators=(',', ':')).encode())
    signing = (head + '.' + body).encode()
    return head + '.' + body + '.' + _b64(rs256(pem, signing))


def mint_wallet(env=None, keyfile=settings.MACHINE_KEY_FILE):
    env = os.environ if env is None else env
    keyfile = env.get('ZITADEL_MACHINE_KEY_FILE') or keyfile
    domain = env.get('ZITADEL_DOMAIN') or ''
    if not keyfile or not os.path.exists(keyfile) or not domain:
        print('wallet: machine key / domain absent, data-plane env omitted', flush=True)
        return ''
    try:
        key = json.load(open(keyfile))
        now = int(time.time())
        assertion = jwt_rs256({'iss': key['userId'], 'sub': key['userId'], 'aud': 'https://' + domain,
                               'iat': now, 'exp': now + 60}, key['key'], key['keyId'])
        scope = 'openid email profile urn:zitadel:iam:org:projects:roles'
        pid = env.get('ZITADEL_MLFLOW_PROJECT_ID') or ''
        if pid:
            scope += ' urn:zitadel:iam:org:project:id:' + pid + ':aud'
        req = urllib.request.Request(
            'https://' + domain + '/oauth/v2/token',
            data=urllib.parse.urlencode({
                'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                'scope': scope, 'assertion': assertion}).encode())
        tok = json.load(urllib.request.urlopen(req, timeout=30)).get('access_token', '')
        print('wallet: minted' if tok else 'wallet: empty token response', flush=True)
        return tok
    except Exception as e:
        print('wallet: mint failed, data-plane env omitted:', e, flush=True)
        return ''


def fetch_workdir(image, reg_token):
    """OCI WORKDIR (live job 5148071): Docker honours the image's WorkingDir, Apptainer does NOT -
    `apptainer exec` starts in the host cwd, so a step command like `python -m app.entry` fails with
    "No module named 'app'". Read from the image config in the registry at submit time and passed as
    `--pwd`. Best-effort: any failure yields '' (no --pwd), never a blocked submit."""
    try:
        ref_host, _, rest = image.partition('/')
        if '@' in rest:
            path, ref = rest.split('@', 1)
        elif ':' in rest.rsplit('/', 1)[-1]:
            path, ref = rest.rsplit(':', 1)
        else:
            path, ref = rest, 'latest'
        hdr = {'Accept': ', '.join([
            'application/vnd.oci.image.index.v1+json',
            'application/vnd.docker.distribution.manifest.list.v2+json',
            'application/vnd.oci.image.manifest.v1+json',
            'application/vnd.docker.distribution.manifest.v2+json'])}
        if reg_token and ref_host.endswith('-docker.pkg.dev'):
            hdr['Authorization'] = 'Bearer ' + reg_token
        base = 'https://' + ref_host + '/v2/' + path

        def get(url):
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=hdr), timeout=20))
        m = get(base + '/manifests/' + ref)
        if 'manifests' in m:  # multi-arch index: pick linux/amd64
            pick = next((x for x in m['manifests']
                         if (x.get('platform') or {}).get('architecture') == 'amd64'
                         and (x.get('platform') or {}).get('os', 'linux') == 'linux'),
                        m['manifests'][0])
            m = get(base + '/manifests/' + pick['digest'])
        cfg = get(base + '/blobs/' + m['config']['digest'])
        wd = (cfg.get('config') or {}).get('WorkingDir') or ''
        print('workdir:', wd or '(none)', flush=True)
        return wd
    except Exception as e:
        print('workdir: lookup failed (running from host cwd):', e, flush=True)
        return ''
