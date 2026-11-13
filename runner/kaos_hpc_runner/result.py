"""The step's result on its way from the site to the job's status (docs/step-contract.md, sections 4, 6, 7).

The step writes one file, /work/output/kaos-result.json. The job on the site hands its bytes back unread
(payload/handback.py); the runner checks them here and writes them again in wire form, and only that text
goes into status.result. A file that is not a result of the step contract is never passed on and never
replaced by "no result": the job fails, and the sentence says what is wrong (PRD kaos#1911 CON-04).
"""

import json
import math
import re

RESULT_CAP = 8 * 1024        # the most a result may cost
RESULT_FILE = '/work/output/kaos-result.json'   # where the step writes it, in every container
ABSENT = '{"version":1,"status":"absent"}'   # the result of a step that did not run; a step never writes it


class InvalidResult(Exception):
    """The result file reached the runner but is not a result of the step contract. The message is a plain
    sentence for the job's status and the pipeline log."""


class ResultNotHandedBack(Exception):
    """The step ended well, but what it handed back could not be brought to the job's status (or the run has
    no way to hand a result back at all). The message is a plain sentence naming the cause."""


class _Float(float):
    """A number with a fraction or an exponent, kept as the step wrote it: the result is written again as
    text, and 1e-05 must not come out as 0.00001."""

    def __new__(cls, text):
        self = super().__new__(cls, text)
        if not math.isfinite(self):
            raise ValueError('the number %s is not finite' % text)
        self.text = text
        return self


def _no_constant(name):
    raise ValueError('%s is not a JSON number' % name)


def _no_duplicates(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise ValueError('the key %r appears twice in one object' % k)
        out[k] = v
    return out


def _string(text):
    # Wire form, rules 2 and 3: ASCII only; braces never appear (the workflow engine would evaluate "{{");
    # & < > are written the way Go's encoder writes them, so the cost is the same on every side; $ is never
    # written (Kubernetes expands $(NAME) and $$ in an env value).
    out = json.dumps(text, ensure_ascii=True)
    for ch, esc in (('{', '\\u007b'), ('}', '\\u007d'), ('&', '\\u0026'), ('<', '\\u003c'), ('>', '\\u003e'), ('$', '\\u0024')):
        out = out.replace(ch, esc)
    return out


def wire(value):
    """The wire form of a JSON value: no whitespace, ASCII only, braces, & < > and $ inside strings escaped."""
    if isinstance(value, str):
        return _string(value)
    if isinstance(value, _Float):
        return value.text
    if value is None or isinstance(value, (bool, int, float)):
        return json.dumps(value, allow_nan=False)
    if isinstance(value, list):
        return '[' + ','.join(wire(v) for v in value) + ']'
    if isinstance(value, dict):
        return '{' + ','.join(_string(k) + ':' + wire(v) for k, v in value.items()) + '}'
    raise TypeError('not a JSON value: %r' % (value,))


def cost(wire_text):
    """What a value in wire form costs against its cap: its length as a JSON string (each quote and each
    backslash counts twice)."""
    return len(wire_text) + wire_text.count('"') + wire_text.count('\\')


RESULT_FIELDS = ('version', 'status', 'values', 'reads', 'writes')        # docs/step-contract.md, section 4
SKIPPED_FIELDS = ('version', 'status', 'reason')   # section 4, a skipped result: kaos_lib.step.skip(reason)
SKIP_REASON_MAX = 300
READ_FIELDS = ('name', 'repo', 'owner', 'ref', 'commit', 'line')           # section 5, a read
WRITE_FIELDS = ('name', 'type', 'repo', 'commit', 'line', 'path')          # section 5, a write


def _ordered(obj, fields):
    """obj with its fields in the contract's order (check() has refused any field the contract does not give)."""
    return {k: obj[k] for k in fields if k in obj}


NAME = re.compile(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$')
COMMIT = re.compile(r'^[0-9a-f]{64}$')
TYPES = ('model', 'artifact', 'dataset')


def _name(text):
    return isinstance(text, str) and len(text) <= 40 and bool(NAME.match(text)) and not text.startswith('kaos-')


def _fields(entry, required, optional):
    """What is wrong with the field names of an object ('' when nothing): all of `required`, no field but those
    and `optional`. The order is not asked of the step: the runner writes the contract's order (rule 6)."""
    if not isinstance(entry, dict):
        return 'is not an object'
    missing = [k for k in required if k not in entry]
    extra = [k for k in entry if k not in required and k not in optional]
    if missing or extra:
        return 'must have the fields %s (and optionally %s)%s%s' % (
            ', '.join(required), ', '.join(optional) or 'none',
            ''.join('; it has no `%s`' % k for k in missing), ''.join('; it has an unknown `%s`' % k for k in extra))
    if any(k in entry and not isinstance(entry[k], str) for k in optional):
        return 'has a %s that is not a string' % ' or '.join(optional)
    return ''


def _bad_read(e):
    """docs/step-contract.md, section 5 (a read); the same rules as shape_of_read in hack/contract-checks/wire.py."""
    bad = _fields(e, READ_FIELDS[:5], READ_FIELDS[5:])
    if bad:
        return bad
    if not _name(e['name']) or not isinstance(e['repo'], str) or not e['repo'].startswith('dataset-') \
            or not isinstance(e['commit'], str) or not COMMIT.match(e['commit']):
        return 'has a name, a repo (dataset-...) or a commit (64 hex characters) that is not valid'
    if not (e['owner'] is None or isinstance(e['owner'], str)) or not isinstance(e['ref'], str) or not e['ref']:
        return 'has an owner that is not a project name or null, or a ref that is not a non-empty string'
    return ''


def _bad_write(e):
    """Section 5 (a write); the same rules as shape_of_write in hack/contract-checks/wire.py."""
    bad = _fields(e, WRITE_FIELDS[:4], WRITE_FIELDS[4:])
    if bad:
        return bad
    if not _name(e['name']) or e['type'] not in TYPES or not isinstance(e['commit'], str) or not COMMIT.match(e['commit']):
        return 'has a name, a type (model, artifact or dataset) or a commit (64 hex characters) that is not valid'
    repo = e['repo']
    if not isinstance(repo, str):
        return 'has a repo that is not a string'
    if e['type'] == 'dataset':
        if not (repo.startswith('dataset-') and repo.endswith('-' + e['name']) and len(repo) > len('dataset-') + len(e['name']) + 1):
            return 'is a dataset, so its repo must be dataset-<app>-<name>'
    elif not repo.startswith('outputs-'):
        return 'is not a dataset, so its repo must be outputs-<app>'
    return ''


def check(raw):
    """The result file's bytes as the text for status.result (wire form), or InvalidResult saying what is wrong.
    A result is a JSON object (docs/step-contract.md, section 4) with the integer version 1, status "skipped" and a
    reason, or status "ok", an
    object `values` (no key starting with kaos-), the lists `reads` and `writes` of ledger entries (section 5),
    only finite numbers and no key twice in one object; and it costs at most 8 KiB."""
    try:
        value = json.loads(raw.decode('utf-8'), parse_float=_Float, parse_constant=_no_constant,
                           object_pairs_hook=_no_duplicates)
    except (UnicodeDecodeError, ValueError) as e:
        raise InvalidResult('the step\'s result file is not JSON (%s)' % e)
    if not isinstance(value, dict):
        raise InvalidResult('the step\'s result file holds %s, not a JSON object' % type(value).__name__)
    if type(value.get('version')) is not int or value.get('version') != 1:
        raise InvalidResult('the step\'s result has version %r; this platform reads the integer 1' % (value.get('version'),))
    if value.get('status') == 'skipped':
        # A step that ended itself with step.skip(reason): nothing but the reason (section 4).
        unknown = [k for k in value if k not in SKIPPED_FIELDS]
        reason = value.get('reason')
        if unknown or not isinstance(reason, str) or not 0 < len(reason) <= SKIP_REASON_MAX:
            raise InvalidResult('the step\'s skipped result has only version, status and a reason of 1 to %d characters%s'
                                % (SKIP_REASON_MAX, '; it has the unknown field `%s`' % unknown[0] if unknown else ''))
        return wire(_ordered(value, SKIPPED_FIELDS))
    if value.get('status') != 'ok':
        raise InvalidResult('the step\'s result has status %r; a step hands back status "ok" or "skipped"'
                            % (value.get('status'),))
    for field, kind, word in (('values', dict, 'an object'), ('reads', list, 'a list'), ('writes', list, 'a list')):
        if not isinstance(value.get(field), kind):
            raise InvalidResult('the step\'s result has no `%s` that is %s' % (field, word))
    unknown = [k for k in value if k not in RESULT_FIELDS]
    if unknown:
        raise InvalidResult('the step\'s result has the unknown field `%s`; a result has %s' % (unknown[0], ', '.join(RESULT_FIELDS)))
    reserved = [k for k in value['values'] if k.startswith('kaos-')]
    if reserved:
        raise InvalidResult('the step\'s result has the value `%s`; a name may not start with kaos-' % reserved[0])
    for field, table, bad in (('reads', 'read', _bad_read), ('writes', 'write', _bad_write)):
        for i, entry in enumerate(value[field]):
            why = bad(entry)
            if why:
                raise InvalidResult('the step\'s result: %s[%d] (a ledger %s) %s' % (field, i, table, why))
    # Wire form, rule 6: the fields in the order the contract's tables give them, whatever order the step wrote.
    value = _ordered(value, RESULT_FIELDS)
    value['reads'] = [_ordered(e, READ_FIELDS) for e in value['reads']]
    value['writes'] = [_ordered(e, WRITE_FIELDS) for e in value['writes']]
    text = wire(value)
    if cost(text) > RESULT_CAP:
        raise InvalidResult('the step\'s result costs %d bytes, more than the %d (8 KiB) a result may cost. Metrics '
                            'and anything large belong in MLflow or in an artifact, not in the result'
                            % (cost(text), RESULT_CAP))
    return text
