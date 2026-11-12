"""The programs that run on the compute node, shipped base64-encoded in the job's environment.

They are plain files next to this module (`payload/`) because they are not part of the runner:
STAGEIN / HANDBACK / CKPT run under the node's system python3 (3.6, stdlib only), SLOTS
is the PRD-1609 slot manager, IDLEWATCH the D-72 idle watchdog the job script starts beside its step. Byte-for-byte the programs the in-pod waiter embedded, except for the
`kaos-image=` lease SLOTS keeps on every host write (D-62).
"""

import base64
import pathlib

_DIR = pathlib.Path(__file__).resolve().parent / 'payload'


def _read(name):
    return (_DIR / (name + '.py')).read_text()


STAGEIN = _read('stagein')
HANDBACK = _read('handback')
CKPT = _read('ckpt')
SLOTS = _read('slots')
IDLEWATCH = _read('idlewatch')
# One redaction program for the node (the failure tail) and the runner (the log stream): redact.py is a module of the
# runner AND a node program (Python 3.6, stdlib only), so it is read from the package, not from payload/.
REDACT = (_DIR.parent / 'redact.py').read_text()


def b64(text):
    return base64.b64encode(text.encode()).decode()
