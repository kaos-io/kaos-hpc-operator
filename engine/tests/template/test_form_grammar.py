"""The Hera-free `when=` grammar (PRD 1533/F-21 and 1533/F-22), and the check that its copy of authoring's parser
stays honest until T2b removes the duplicate.
Run:  python -m pytest tests/ -v   (the parity test needs hera, as the engine-tests image has)
"""
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import form_grammar  # noqa: E402
from kubecore.form_grammar import GrammarError, is_step_value, names, normalize, parse_when  # noqa: E402

VALID = [
    "mode != off",
    "mode == on",
    'mode == "two words"',
    "mode == 'it\"s'",
    "a == b and c != d",
    "a == b or c == d",
    "a == b and c == d or e != f",
    "  a==b  ",
    "train.amp == true",
    "a-b_c.d == x-y.z",
    'a == ""',
    "a   ==   b   and   c   !=   d",
]
INVALID = [
    "", "mode", "mode ==", "== on", "a = b", "a === b", "a == b and", "or a == b", "a == b c == d",
    "a == b AND c == d", "(a == b)", "a == b && c == d", "a == b,", "{{workflow.parameters.x}} == 1", "a == {{x}}", "a == 'open",
    "a == b\nand c == d", 5, None, ["a == b"],
]


def _authoring():
    pytest.importorskip("hera")
    from kubecore import authoring
    return authoring


@pytest.mark.parametrize("text", VALID + INVALID, ids=repr)
def test_the_copy_and_authorings_parser_agree(text):
    authoring = _authoring()
    try:
        expected = authoring._parse_when("s", text)
    except authoring.AuthoringError as exc:
        with pytest.raises(GrammarError) as got:
            parse_when(text)
        assert "step 's': " + str(got.value) == str(exc)
    else:
        assert parse_when(text) == expected


def test_the_grammar_pieces_equal_authorings():
    authoring = _authoring()
    for piece in ("_WHEN_NAME", "_WHEN_COMPARE", "_WHEN_EXAMPLE"):
        assert getattr(form_grammar, piece) == getattr(authoring, piece)
    for piece in ("_WHEN_ALL", "_WHEN_TOKEN"):
        assert getattr(form_grammar, piece).pattern == getattr(authoring, piece).pattern


def test_parse_returns_comparisons_and_joiners():
    assert parse_when("a == b and c != 'd e' or f == \"g\"") == (
        [("a", "==", "b"), ("c", "!=", "d e"), ("f", "==", "g")], ["and", "or"])


@pytest.mark.parametrize("text", VALID)
def test_normalize_round_trips(text):
    compares, joiners = parse_when(text)
    canonical = normalize(compares, joiners)
    assert parse_when(canonical) == (compares, joiners)
    assert normalize(*parse_when(canonical)) == canonical


def test_normalize_writes_bare_names_and_quotes_the_rest():
    assert normalize([("m", "==", "on"), ("n", "!=", "two words")], ["or"]) == 'm == on or n != "two words"'
    assert normalize([("m", "==", "")], []) == 'm == ""'
    assert normalize([("m", "==", 'it"s')], []) == "m == 'it\"s'"
    assert normalize([], []) == ""
    with pytest.raises(GrammarError):
        normalize([("a", "==", "b"), ("c", "==", "d")], [])


def test_names_and_step_values():
    assert names('train.amp == true and mode != "x y"') == ["train.amp", "true", "and", "mode", "x", "y"]
    assert names("") == []
    assert is_step_value("train.loss") and not is_step_value("mode")


def test_the_grammar_imports_no_render_library():
    code = ("import sys, kubecore.form_grammar; bad = [m for m in sys.modules if m.split('.')[0] in "
            "('hera', 'hydra', 'omegaconf')]; sys.exit(1 if bad else 0)")
    assert subprocess.run([sys.executable, "-c", code], cwd=ROOT).returncode == 0
