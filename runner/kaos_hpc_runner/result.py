"""The step's result on its way from the site to the job's status (docs/step-contract.md, sections 4, 6, 7).

The step writes one file, /work/output/kaos-result.json. The job on the site hands its bytes back unread
(payload/handback.py); the runner checks them here and writes them again in wire form, and only that text
goes into status.result. A file that is not a result of the step contract is never passed on and never
replaced by "no result": the job fails, and the sentence says what is wrong (PRD kaos#1911 CON-04).
"""

import json

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
        self.text = text
        return self


def _no_constant(name):
    raise ValueError('%s is not a JSON number' % name)


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
READ_FIELDS = ('name', 'repo', 'owner', 'ref', 'commit', 'line')           # section 5, a read
WRITE_FIELDS = ('name', 'type', 'repo', 'commit', 'line', 'path')          # section 5, a write


def _ordered(obj, fields):
    """obj with the contract's fields first, in the contract's order; a field it does not know stays, after them."""
    out = {k: obj[k] for k in fields if k in obj}
    out.update((k, v) for k, v in obj.items() if k not in out)
    return out


def check(raw):
    """The result file's bytes as the text for status.result (wire form), or InvalidResult saying what is wrong.
    A result is a JSON object with version 1, status "ok", an object `values` and the lists `reads` and
    `writes`, and costs at most 8 KiB."""
    try:
        value = json.loads(raw.decode('utf-8'), parse_float=_Float, parse_constant=_no_constant)
    except (UnicodeDecodeError, ValueError) as e:
        raise InvalidResult('the step\'s result file is not JSON (%s)' % e)
    if not isinstance(value, dict):
        raise InvalidResult('the step\'s result file holds %s, not a JSON object' % type(value).__name__)
    if isinstance(value.get('version'), bool) or value.get('version') != 1:
        raise InvalidResult('the step\'s result has version %r; this platform reads version 1' % (value.get('version'),))
    if value.get('status') != 'ok':
        raise InvalidResult('the step\'s result has status %r; a step hands back status "ok"' % (value.get('status'),))
    for field, kind, word in (('values', dict, 'an object'), ('reads', list, 'a list'), ('writes', list, 'a list')):
        if not isinstance(value.get(field), kind):
            raise InvalidResult('the step\'s result has no `%s` that is %s' % (field, word))
    # Wire form, rule 6: the fields in the order the contract's tables give them, whatever order the step wrote.
    value = _ordered(value, RESULT_FIELDS)
    value['reads'] = [_ordered(e, READ_FIELDS) if isinstance(e, dict) else e for e in value['reads']]
    value['writes'] = [_ordered(e, WRITE_FIELDS) if isinstance(e, dict) else e for e in value['writes']]
    text = wire(value)
    if cost(text) > RESULT_CAP:
        raise InvalidResult('the step\'s result costs %d bytes, more than the %d (8 KiB) a result may cost. Metrics '
                            'and anything large belong in MLflow or in an artifact, not in the result'
                            % (cost(text), RESULT_CAP))
    return text
