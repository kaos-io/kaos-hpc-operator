"""The run form's `when=` grammar (PRD 1533/F-21 and 1533/F-22), Hera-free so trusted code can use it.
Trusted code: standard library only. It never imports hera, hydra or omegaconf (a test asserts that).
"""
import re


class GrammarError(ValueError):
    """The text is not in the `when=` grammar; the message says what is expected."""


# DUPLICATE-OF authoring._parse_when until T2b (1533/F-21)
_WHEN_NAME = r"[A-Za-z0-9_.-]+"
_WHEN_COMPARE = rf"({_WHEN_NAME})\s*(==|!=)\s*(\"[^\"]*\"|'[^']*'|{_WHEN_NAME})"
_WHEN_ALL = re.compile(rf"\s*{_WHEN_COMPARE}(?:\s+(?:and|or)\s+{_WHEN_COMPARE})*\s*")
_WHEN_TOKEN = re.compile(rf"{_WHEN_COMPARE}|\s+(and|or)\s+")
_WHEN_EXAMPLE = 'when="mode != off"'
# Intended difference from authoring until T2b: a control character anywhere is refused (as in open engine PR 79).
_CONTROL = re.compile("[\x00-\x1f\x7f]")


def parse_when(when):
    """when= in plain words: `<field> == <value>` or `!=`, joined by `and` / `or` (`and` binds tighter, no
    parentheses). Returns the (field, operator, value) comparisons and the joiners between them. A control
    character (U+0000 to U+001F, U+007F), a newline or tab included, is refused anywhere in the text."""
    if isinstance(when, str):
        bad = _CONTROL.search(when)
        if bad:
            raise GrammarError(
                f"when= must not contain a control character, found U+{ord(bad.group()):04X} at position "
                f"{bad.start()}. Write the whole comparison on one line with plain spaces."
            )
    if isinstance(when, str) and ("{{" in when or "}}" in when):
        raise GrammarError(
            f"when= is written with the name of a run-form field, for example {_WHEN_EXAMPLE}. "
            f"Write the field's name instead of {{{{workflow.parameters.<field>}}}}."
        )
    if not (isinstance(when, str) and _WHEN_ALL.fullmatch(when)):
        raise GrammarError(
            f"when={when!r} is not a run-form field compared with a value, for example "
            f"{_WHEN_EXAMPLE}. Join comparisons with and / or; no parentheses."
        )
    compares, joiners = [], []
    for m in _WHEN_TOKEN.finditer(when):
        if m.group(4):
            joiners.append(m.group(4))
        else:
            value = m.group(3)
            compares.append((m.group(1), m.group(2), value[1:-1] if value[0] in "\"'" else value))
    return compares, joiners
# END DUPLICATE


_NAME_ONLY = re.compile(_WHEN_NAME)
_TOKEN = re.compile(_WHEN_NAME)


def normalize(compares, joiners) -> str:
    """The canonical text of a parsed `when=`: bare names, values double-quoted when they are not a bare
    name (single-quoted when the value holds a double quote, so the text parses back to the same value)."""
    if len(joiners) != max(len(compares) - 1, 0):
        raise GrammarError("%d comparisons need %d joiners, not %d" % (len(compares), len(compares) - 1, len(joiners)))
    parts = []
    for i, (field, op, value) in enumerate(compares):
        if i:
            parts.append(joiners[i - 1])
        if not _NAME_ONLY.fullmatch(value):
            value = "'%s'" % value if '"' in value else '"%s"' % value
        parts.append("%s %s %s" % (field, op, value))
    return " ".join(parts)


def names(expr: str) -> list:
    """Every token of `expr` that is shaped like a name, in order: OSR's token regex `[A-Za-z0-9_.-]+`. The
    joiners (and, or) and the words of quoted values are tokens too, as they are for OSR."""
    return _TOKEN.findall(expr)


def is_step_value(name: str) -> bool:
    """A name that reads a step's value (`<step>.<output>`), which a form field cannot know at submit time."""
    return "." in name
