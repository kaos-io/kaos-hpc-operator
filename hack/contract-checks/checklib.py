"""Helpers of the check scripts: what a context should be, and what a step printed."""
import base64
import hashlib
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import wire  # noqa: E402


def fixture(name):
    return json.loads((wire.FIXTURES / name).read_text())


def base_of(context):
    """The base context: every field but `upstream`, which the workflow adds."""
    return {k: v for k, v in context.items() if k != "upstream"}


def b64(value):
    return base64.b64encode(wire.dumps(value).encode()).decode()


def printed(log):
    """BYTES, SHA256 and VALUE as a step printed them."""
    out = {}
    for key in ("BYTES", "SHA256", "VALUE"):
        m = re.search(r"^%s=(.*)$" % key, log, re.M)
        out[key] = m.group(1) if m else None
    return out


def same(label, log, expected_text):
    """True when the step received exactly expected_text; prints one PASS or FAIL line."""
    got = printed(log)
    want_sha = hashlib.sha256(expected_text.encode()).hexdigest()
    ok = got["SHA256"] == want_sha and got["BYTES"] == str(len(expected_text.encode()))
    print("%s  %s: %s bytes, sha256 %s%s" % ("PASS" if ok else "FAIL", label, got["BYTES"], got["SHA256"],
                                              "" if ok else " (expected %d bytes, sha256 %s)" % (len(expected_text), want_sha)))
    if not ok:
        print("      got:      %s" % (got["VALUE"] or "")[:600])
        print("      expected: %s" % expected_text[:600])
    return ok


# ---- check 3(c): a context of an exact size, built the same way by the workflow's compose step

LARGE_HEAD = ('{"version":1,"run":"contract-3c","app":"hera-demo","project":"vision","step":"train","kind":"train",'
              '"experiment":"baseline","params":{"list":[')
LARGE_MID = '""],"blob":"'
LARGE_TAIL = ('"},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{"prepare":'
              '{"version":1,"status":"ok","values":{"rows":1200},"reads":[],"writes":[]}}}')


def chain(fill, seed="contract-3c"):
    """`fill` characters of a sha256 chain: each link is the hex digest of the one before."""
    out, h = [], seed
    while 64 * len(out) < fill:
        h = hashlib.sha256(h.encode()).hexdigest()
        out.append(h)
    return "".join(out)[:fill]


def large_fill(size, items=0):
    """How many filler characters make the whole context exactly `size` bytes, with `items` short strings in it."""
    return size - len(LARGE_HEAD) - 4 * items - len(LARGE_MID) - len(LARGE_TAIL)


def large_context(size, items=0):
    text = LARGE_HEAD + '"x",' * items + LARGE_MID + chain(large_fill(size, items)) + LARGE_TAIL
    assert len(text) == size and json.loads(text)["version"] == 1
    return text


def as_json_string(text):
    """The length of `text` inside a JSON string: what it costs in the step's definition (section 7)."""
    return len(json.dumps(text)) - 2
