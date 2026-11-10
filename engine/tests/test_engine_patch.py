"""The engine is a byte copy of the template's kubecore/ plus two patches in derive_tree.py."""

import difflib
import hashlib
import json
import subprocess
import sys

import pytest

from paths import ENGINE

UPSTREAM = json.loads((ENGINE / "UPSTREAM.json").read_text())


def _blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


@pytest.mark.parametrize("path,sha", sorted(UPSTREAM["files"].items()))
def test_should_keep_every_unpatched_file_byte_identical_to_upstream(path, sha):
    if path in UPSTREAM["patched"]:
        pytest.skip("patched file, see the next test")
    assert _blob_sha((ENGINE / "kubecore" / path).read_bytes()) == sha, f"{path} was edited: engine edits go to a patch"


def test_should_list_every_engine_file_in_upstream_json():
    on_disk = {str(p.relative_to(ENGINE / "kubecore")) for p in (ENGINE / "kubecore").rglob("*")
               if p.is_file() and "__pycache__" not in p.parts}
    assert on_disk == set(UPSTREAM["files"])


def test_should_differ_from_upstream_derive_tree_in_exactly_the_two_documented_places():
    upstream = (ENGINE / "upstream" / "derive_tree.py").read_text().splitlines()
    engine = (ENGINE / "kubecore" / "derive_tree.py").read_text().splitlines()
    assert _blob_sha(("\n".join(upstream) + "\n").encode()) == UPSTREAM["files"]["derive_tree.py"]
    ops = [op for op in difflib.SequenceMatcher(None, upstream, engine, autojunk=False).get_opcodes() if op[0] != "equal"]
    config = next(i for i, ln in enumerate(upstream) if ln.startswith("CONFIG_DIR = "))
    first = next(i for i, ln in enumerate(upstream) if ln.startswith("class DataConfig")) - 1  # its @dataclass
    last = next(i for i, ln in enumerate(upstream) if "output_prefix" in ln)
    allowed = [range(config, config + 1), range(first, last + 1)]
    for _, i1, i2, _, _ in ops:
        assert any(i1 in r and i2 - 1 in r for r in allowed), f"unexpected change of upstream lines {i1 + 1}-{i2}"


def _fields(profile: str) -> dict:
    code = ("import dataclasses, json; from kubecore import derive_tree as d; "
            "print(json.dumps({c.__name__: [f.name for f in dataclasses.fields(c)] "
            "for c in (d.DataConfig, d.QuantizationConfig)}))")
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                         env={"PYTHONPATH": str(ENGINE), "KUBECORE_SCHEMA_PROFILE": profile, "PATH": ""})
    assert res.returncode == 0, res.stderr
    return json.loads(res.stdout)


def test_should_differ_between_profiles_only_by_ref_and_output_prefix():
    hera, yolo = _fields("hera"), _fields("yolo")
    assert set(hera["DataConfig"]) - set(yolo["DataConfig"]) == {"ref"}
    assert set(hera["QuantizationConfig"]) - set(yolo["QuantizationConfig"]) == {"output_prefix"}
    assert [f for f in hera["DataConfig"] if f != "ref"] == yolo["DataConfig"]
    assert [f for f in hera["QuantizationConfig"] if f != "output_prefix"] == yolo["QuantizationConfig"]


def test_should_refuse_an_unknown_profile():
    res = subprocess.run([sys.executable, "-c", "from kubecore import derive_tree"], capture_output=True, text=True,
                         env={"PYTHONPATH": str(ENGINE), "KUBECORE_SCHEMA_PROFILE": "bogus", "PATH": ""})
    assert res.returncode != 0 and "not a known schema profile" in res.stderr


def test_should_resolve_config_dir_lazily_and_honour_a_monkeypatch(monkeypatch, tmp_path):
    from kubecore import derive_tree
    monkeypatch.setenv("KUBECORE_CONFIG_DIR", str(tmp_path / "a"))
    assert str(derive_tree.CONFIG_DIR) == str(tmp_path / "a")
    assert derive_tree.CONFIG_DIR / "x" == tmp_path / "a" / "x"
    monkeypatch.setenv("KUBECORE_CONFIG_DIR", str(tmp_path / "b"))
    assert str(derive_tree.CONFIG_DIR) == str(tmp_path / "b")
    monkeypatch.setattr(derive_tree, "CONFIG_DIR", tmp_path / "c")
    assert derive_tree.CONFIG_DIR == tmp_path / "c"


def test_should_carry_the_two_historical_schema_variants():
    legacy, yolo_v1 = _fields("legacy-v1"), _fields("yolo-v1")
    assert legacy["DataConfig"] == ["ref", "source", "version", "path_override", "sample_size", "seed"]
    assert "output_prefix" in legacy["QuantizationConfig"]
    assert "ref" not in yolo_v1["DataConfig"] and "output_prefix" in yolo_v1["QuantizationConfig"]
