"""Credential redaction for job output (PRD kaos#1911 hardening): the ONE implementation.

It runs in two places, from this one file: on the site node, where the job script's `fail()` pipes the tail of the job's
output through it before it is copied into the job comment (the file is shipped base64-encoded, like the other node
programs, so it must stay Python 3.6 and standard library only), and in the runner, where `logs.LogStreamer` passes every
line of step output through it before it is printed to the platform's log stream.

What is redacted (by SHAPE or by exact value, never by "the line has the word key/token in it"):
  * the exact values of the credentials the job itself holds (the data/MLflow bearer, the registry token) and their
    base64 and URL-encoded forms: no guessing needed;
  * Bearer / Basic values of 16+ characters, the value after `Authorization:`, `Proxy-Authorization:`, `X-...-Key/Token/Secret:`,
    `Api-Key:`, a `Cookie:` header;
  * JWTs (`eyJ...`), Google access tokens (`ya29.`), GitHub tokens (`ghp_`, `github_pat_`, ...), AWS access key ids (`AKIA...`);
  * the password in `scheme://user:password@host`;
  * the value of a variable or member named exactly like a secret: `NAME_TOKEN=`, `NAME_SECRET=`, `NAME_PASSWORD=`,
    `NAME_API_KEY=` (upper case, env style) and the exact words `password`, `secret`, `token`, `api_key`, `access_token`,
    `refresh_token`, `client_secret`, `private_key`... in `k=v`, JSON, a Python repr or escaped JSON;
  * a private key block (`-----BEGIN ... PRIVATE KEY-----` to `-----END ...`), also when it is one JSON string.
`max_new_tokens=512`, `tokenizer=...`, `num_keypoints=17`, `monkey=3`, `"primary_key": "id"` and "The Bearer token expired" are
NOT touched: none of them is a secret's shape or a secret's name.

Every pattern is linear in the length of the line (no nested unbounded quantifiers; every run is bounded or a plain
character class), so a hostile line costs time proportional to its length.
"""
import base64
import os
import re
import sys
import urllib.parse

REDACTED = '[redacted]'
MIN_LITERAL = 8       # a held value shorter than this is not a credential, and would blot out ordinary text
# The variables of the job script that hold credentials; their exact values are looked for in the output.
HELD_ENV = ('LAKEFS_BEARER_TOKEN', 'MLFLOW_TRACKING_TOKEN', 'REG_TOKEN', 'APPTAINER_DOCKER_PASSWORD')

# a quoted value up to its closing quote (it may hold spaces), else one word
_VALUE = r'(?!\[redacted)(?:(?<=")[^"\\]{1,2000}|(?<=\')[^\'\\]{1,2000}|[^\s"\'\\,;&|}\])]{1,2000})'
_NAMES = (r'password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret'
          r'|auth[_-]?token|bearer[_-]?token|private[_-]?key|secret[_-]?key|token')
_ENV_NAMES = (r'(?:[A-Z][A-Z0-9_]{0,60}_)?(?:TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|APIKEY|SECRET_KEY|ACCESS_KEY|PRIVATE_KEY'
              r'|ACCESS_TOKEN|SESSION_TOKEN|CREDENTIALS?)')

# (pattern, replacement); applied in order to one line.
_RULES = [
    # a private key written on one line (escaped newlines in a JSON string)
    (re.compile(r'-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----[A-Za-z0-9+/=\\ ]{0,16000}'
                r'(?:-----END [A-Z0-9 ]{0,40}PRIVATE KEY-----)?'), '[redacted: private key]'),
    (re.compile(r'eyJ[A-Za-z0-9_-]{6,}(?:\.[A-Za-z0-9_-]{0,4000}){0,2}'), REDACTED),
    (re.compile(r'ya29\.[A-Za-z0-9._-]+'), REDACTED),
    (re.compile(r'(?<![A-Za-z0-9_])(?:gh[pousr]_[A-Za-z0-9]{20,255}|github_pat_[A-Za-z0-9_]{20,255}'
                r'|xox[abprs]-[A-Za-z0-9-]{10,255})'), REDACTED),
    (re.compile(r'(?<![A-Za-z0-9])(?:AKIA|ASIA)[0-9A-Z]{16}(?![A-Za-z0-9])'), REDACTED),
    # scheme://user:password@host
    (re.compile(r'(?<=://)([^/\s:@]{1,256}):[^/\s@]{1,2000}(?=@)'), r'\1:' + REDACTED),
    # a Cookie header is a list: the rest of the line
    (re.compile(r'(?i)((?:set-)?cookie["\']?\s*:\s*).{1,4000}'), r'\1' + REDACTED),
    # headers whose value is a credential: the optional scheme word, then the value
    (re.compile(r'(?i)((?:proxy-)?authorization|x-[a-z0-9-]{0,40}(?:key|token|secret)|api-key)(\\?["\']?\s*[:=]\s*\\?["\']?)'
                r'(?:(?:bearer|basic|token|digest|negotiate)\s+)?[^\s"\'\\,;}]{1,2000}'), r'\1\2' + REDACTED),
    (re.compile(r'(?i)(?<![A-Za-z0-9_])bearer\s+[A-Za-z0-9._~+/=-]{16,2000}'), 'Bearer ' + REDACTED),
    (re.compile(r'(?<![A-Za-z0-9_])basic\s+[A-Za-z0-9+/=]{16,2000}', re.I), 'Basic ' + REDACTED),
    # NAME_TOKEN=value (upper case, env style)
    (re.compile(r'(?<![A-Za-z0-9_])(' + _ENV_NAMES + r')(\\?["\']?\s*[:=]\s*\\?["\']?)(' + _VALUE + ')'),
     r'\1\2' + REDACTED),
    # the exact words, in k=v, JSON, a Python repr, escaped JSON, a query string
    (re.compile(r'(?i)(?<![A-Za-z0-9_])(' + _NAMES + r')(\\?["\']?\s*[:=]\s*\\?["\']?)(' + _VALUE + ')'),
     r'\1\2' + REDACTED),
]
_PEM_BEGIN = re.compile(r'-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----')
_PEM_END = re.compile(r'-----END [A-Z0-9 ]{0,40}PRIVATE KEY-----')


def forms(value):
    """The spellings of one credential a log line may hold: as is, base64 (standard and URL-safe, with and without
    padding), as the user:password of a Basic header, and percent-encoded."""
    raw = value.encode()
    out = {value}
    for blob in (raw, b'oauth2accesstoken:' + raw):
        for enc in (base64.b64encode, base64.urlsafe_b64encode):
            text = enc(blob).decode()
            out.update((text, text.rstrip('=')))
    out.add(urllib.parse.quote(value, safe=''))
    out.add(urllib.parse.quote_plus(value))
    return {f for f in out if len(f) >= MIN_LITERAL}


class Redactor:
    """Redacts text line by line. `held` are the credential values of this job; their forms are replaced wherever
    they appear. A private key block that spans lines is dropped from its BEGIN line to its END line."""

    def __init__(self, held=()):
        literals = set()
        for v in held:
            if v and len(v) >= MIN_LITERAL:
                literals |= forms(v)
        self._literals = re.compile('|'.join(re.escape(x) for x in sorted(literals, key=len, reverse=True))) if literals else None
        self._in_pem = False

    def line(self, text):
        """The line redacted, or None when it is inside a private key block."""
        if self._in_pem:
            if _PEM_END.search(text):
                self._in_pem = False
            return None
        if _PEM_BEGIN.search(text) and not _PEM_END.search(text):
            self._in_pem = True
            return _PEM_BEGIN.split(text)[0] + '[redacted: private key]'
        if self._literals is not None:
            text = self._literals.sub(REDACTED, text)
        for pattern, repl in _RULES:
            text = pattern.sub(repl, text)
        return text

    def text(self, text):
        """Several lines (keeping their line ends)."""
        out = []
        for ln in text.splitlines(True):
            body = ln.rstrip('\r\n')
            done = self.line(body)
            if done is not None:
                out.append(done + ln[len(body):])
        return ''.join(out)


def main():
    """stdin to stdout, as the job script uses it; the held values come from this process's environment."""
    red = Redactor(os.environ.get(n) for n in HELD_ENV)
    src, dst = sys.stdin.buffer, sys.stdout.buffer
    for raw in src:
        body = raw.rstrip(b'\r\n')
        done = red.line(body.decode('utf-8', 'surrogateescape'))
        if done is not None:
            dst.write(done.encode('utf-8', 'surrogateescape') + raw[len(body):])
    dst.flush()


if __name__ == '__main__':
    main()
