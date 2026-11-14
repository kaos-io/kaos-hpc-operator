"""The app's own identity in the data store (PRD kaos#1911, AC-14).

A job reads and writes the data store as its APP, never as the project: the runner asks the cluster for a short-lived token
of the app's workflow service account (Kubernetes TokenRequest, audience `kaos-lake`) and exchanges it at the data store
(`POST /kaos/v1/credentials`) for an access key pair of the app. The pair expires in about an hour; the runner keeps a
fresh one in a file on the site for the whole job (runner.py, the key file). Nothing here mints a token from the
project's machine key for the data store, and nothing falls back to one: when the app has no identity the run is refused
before anything is staged, with a reason that names the app (AppIdentityUnavailable).

Stdlib only; one HTTP call per function, the callers retry.
"""

import base64
import datetime
import json
import urllib.error
import urllib.request

AUDIENCE = 'kaos-lake'
TOKEN_SECONDS = 600           # the shortest a TokenRequest may ask: the token is used at once and never stored
CREDENTIALS_PATH = '/kaos/v1/credentials'
SHARED_CREDENTIALS_PATH = '/kaos/v1/shared-credentials'


class AppIdentityUnavailable(Exception):
    """The app has no identity in the data store. The message is what a client engineer reads."""


class Key:
    """One access key pair of the app (or, for a shared dataset, of the app in the owner's data store)."""

    def __init__(self, key_id, secret, expires, expiration):
        self.key_id, self.secret, self.expires, self.expiration = key_id, secret, expires, expiration

    def auth(self):
        """The value of the Authorization header (HTTP Basic, which the data store's API takes)."""
        return 'Basic ' + base64.b64encode((self.key_id + ':' + self.secret).encode()).decode()

    def record(self):
        """What the key file holds."""
        return {'access_key_id': self.key_id, 'secret_access_key': self.secret, 'expiration': self.expiration}

    def secrets(self):
        return (self.key_id, self.secret, self.key_id + ':' + self.secret)


def epoch(text):
    """An RFC 3339 time as seconds since the epoch; 0 when it is not one."""
    try:
        return datetime.datetime.fromisoformat(str(text).replace('Z', '+00:00')).timestamp()
    except ValueError:
        return 0.0


def _key_of(body):
    try:
        key_id, secret = body['access_key_id'], body['secret_access_key']
        if not (isinstance(key_id, str) and isinstance(secret, str) and key_id and secret):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        return None
    return Key(key_id, secret, epoch(body.get('expiration')), str(body.get('expiration') or ''))


def service_account(app):
    return app + '-workflow'


def request_token(kube, namespace, app):
    """A token of the app's workflow service account, for the data store only. The runner's own service account may
    create it for this one service account only (the controller binds that, see docs)."""
    sa = service_account(app)
    try:
        out = kube('POST', '/api/v1/namespaces/%s/serviceaccounts/%s/token' % (namespace, sa),
                   {'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest',
                    'spec': {'audiences': [AUDIENCE], 'expirationSeconds': TOKEN_SECONDS}})
        token = out['status']['token']
        if not token:
            raise KeyError('token')
        return token
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            raise AppIdentityUnavailable(
                'the app %s has no identity for its data: the service account %s in %s %s. Nothing was staged and nothing '
                'was submitted to the site. Ask a platform engineer to check that the app is deployed to this project' % (
                    app, sa, namespace, 'does not exist' if e.code == 404 else 'may not be used by this run')) from None
        raise
    except (KeyError, TypeError):
        raise AppIdentityUnavailable(
            'the app %s has no identity for its data: the cluster gave no token for %s. Nothing was staged and nothing '
            'was submitted to the site' % (app, sa)) from None


def exchange(lakefs_url, token, app):
    """The app's key pair, from its service account token. AppIdentityUnavailable when the data store does not know
    the app (HTTP 401/403/404); any other failure is transient and the caller asks again."""
    req = urllib.request.Request(lakefs_url.rstrip('/') + CREDENTIALS_PATH, method='POST', data=b'{}',
                                 headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        body = json.load(_open(req))
    except urllib.error.HTTPError as e:
        if e.code in (401, 403, 404, 405, 501):
            raise AppIdentityUnavailable(
                'the app %s has no identity in the data store (HTTP %d): the app is not declared there with its '
                'project, or the data store is too old to know apps. Nothing was staged and nothing was submitted to the '
                'site. Ask a platform engineer to check the app\'s declaration' % (app, e.code)) from None
        raise
    except ValueError:
        raise AppIdentityUnavailable(
            'the app %s has no identity in the data store: it answered something that is not a key (it may be too old to '
            'know apps). Nothing was staged and nothing was submitted to the site' % app) from None
    key = _key_of(body)
    if key is None:
        raise AppIdentityUnavailable(
            'the app %s has no identity in the data store: its answer holds no key. Nothing was staged and nothing was '
            'submitted to the site' % app)
    return key


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None       # a key pair is never sent anywhere but where it was asked for


def _open(req):
    return urllib.request.build_opener(_NoRedirect).open(req, timeout=60)


def app_key(kube, cfg):
    """A fresh key pair of the app of this job."""
    if not cfg.app:
        raise AppIdentityUnavailable('the job names no app, so it has no identity for its data. Nothing was staged and '
                                     'nothing was submitted to the site')
    return exchange(cfg.lakefs_url, request_token(kube, cfg.namespace, cfg.app), cfg.app)


def peer_key(own_url, key, owner):
    """The app's key in the data store of project `owner`, from the own data store (what kaos-lib's peer_key does). The
    key reads exactly what the owner shares with this project, never writes, and expires. Returns None for 'not shared'
    (403) and raises urllib.error.HTTPError for the other answers; the caller says what that means."""
    req = urllib.request.Request(own_url.rstrip('/') + SHARED_CREDENTIALS_PATH, method='POST',
                                 data=json.dumps({'owner': owner}).encode(),
                                 headers={'Authorization': key.auth(), 'Content-Type': 'application/json'})
    return _key_of(json.load(_open(req)))
