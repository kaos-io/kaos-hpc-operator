"""Build tag -> PEP 440 version for the engine wheel (no imports from the rest of the package, so
setup.py can load it by path before anything is installed).

Tags (the operator build's scheme):
    v0.4.7                                 -> 0.4.7
    v0.4.7-dev-20261110-123456-abc1234     -> 0.4.7.post1.dev20261110123456+abc1234

A dev build is AFTER the tag it names, so it is a post-release (0.4.7.post1.dev...), which sorts after
0.4.7 and before the next tag.
"""

import re

_RELEASE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")
_DEV = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)-dev-(\d{8})-(\d{6})-([0-9a-f]{7,40})$")


def pep440_from_tag(tag: str) -> str:
    """Return the PEP 440 version for a build tag; raise ValueError for a tag in neither form."""
    tag = tag.strip()
    match = _RELEASE.match(tag)
    if match:
        return ".".join(match.groups())
    match = _DEV.match(tag)
    if match:
        major, minor, patch, day, clock, sha = match.groups()
        return f"{major}.{minor}.{patch}.post1.dev{day}{clock}+{sha}"
    raise ValueError(f"build tag {tag!r} is neither vX.Y.Z nor vX.Y.Z-dev-YYYYMMDD-HHMMSS-<sha>")
