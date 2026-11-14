"""A scan for secret-looking values in what a job is made of: the environment handed to the site, the batch script, the
job name and comment, the command line of any program. Reusable by any test that renders a job.

A finding names WHERE (the surface) and WHAT KIND (an exact held value, or a shape); it never carries the value, so a
failing test does not print a credential.
"""
import re

# Shapes of credentials (what redact.py also knows): a JWT, a Google access token, a GitHub token, an AWS access key id,
# a Bearer/Basic value, a private key block, scheme://user:password@host.
SHAPES = {
    "jwt": re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}"),
    "google access token": re.compile(r"ya29\.[A-Za-z0-9_.-]{12,}"),
    "github token": re.compile(r"\b(?:ghp_|gho_|ghs_|github_pat_)[A-Za-z0-9_]{12,}"),
    "aws access key id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "bearer or basic value": re.compile(r"\b(?:Bearer|Basic)\s+[A-Za-z0-9+/=._-]{16,}"),
    "private key block": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "password in a url": re.compile(r"[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]{3,}@"),
}
# An environment variable whose NAME says it holds a credential.
SECRET_NAME = re.compile(r"(TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|CREDENTIAL)", re.I)


def surfaces(batch="", env=(), comment="", name="", argv=()):
    """The text surfaces of one job: {surface name: text}. env: 'NAME=value' strings as the runner hands them to Slurm."""
    out = {"script": batch or "", "comment": comment or "", "job name": name or ""}
    for e in env:
        k, _, v = e.partition("=")
        out["env " + k] = v
    for i, a in enumerate(argv):
        out["argv %d" % i] = str(a)
    return out


def scan(found_in, secrets=(), allowed_names=()):
    """Findings in `found_in` ({surface: text}). secrets: exact values that must appear nowhere (also their base64
    form). allowed_names: environment variables that are KNOWN to carry a credential and are not a finding (the caller
    says which, so every exception is written down in the test that makes it)."""
    import base64
    exact = []
    for s in secrets:
        if s:
            exact += [s, base64.b64encode(s.encode()).decode()]
    findings = []
    for surface, text in found_in.items():
        for s in exact:
            if s in text:
                findings.append("%s: holds a held credential value" % surface)
                break
        for kind, rx in SHAPES.items():
            if rx.search(text):
                findings.append("%s: holds the shape of a %s" % (surface, kind))
        if surface.startswith("env ") and text:
            var = surface[4:]
            if SECRET_NAME.search(var) and var not in allowed_names:
                findings.append("%s: a credential-named variable with a value" % surface)
    return findings


def assert_clean(found_in, secrets=(), allowed_names=()):
    findings = scan(found_in, secrets, allowed_names)
    assert not findings, "secret-looking values in a job: " + "; ".join(findings)
