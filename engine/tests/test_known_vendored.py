"""known-vendored.json maps every historical vendored blob to a profile and its template commits."""

import json

import pytest

from paths import ENGINE

DATA = json.loads((ENGINE / "kaos_pipeline_engine" / "known-vendored.json").read_text())
DERIVE = DATA["modules"]["derive_tree.py"]
AUTHORING = DATA["modules"]["authoring.py"]

# Blobs of the current template-seeded apps (checked when the corpus was built).
LAB_DERIVE = {"5e88509fd4229f2f5b64936c1f3f3c3378137685": ("hera", "hera"),
              "7f0b5d7a008b67db499594a540c529ba21db1e3b": ("yolo", "yolo")}
LAB_AUTHORING = ["fc916ea848576d4485ffbc93402f1ec95d727f41", "b107b7cc6d201af4e938b01ad603d74b80e7a586"]


@pytest.mark.parametrize("blob,want", sorted(LAB_DERIVE.items()))
def test_should_know_the_derive_tree_blobs_of_the_apps(blob, want):
    assert (DERIVE[blob]["profile"], DERIVE[blob]["engine_profile"]) == want


@pytest.mark.parametrize("blob", LAB_AUTHORING)
def test_should_know_the_authoring_blobs_of_the_apps(blob):
    assert blob in AUTHORING


def test_should_give_every_blob_a_template_commit():
    for module in DATA["modules"].values():
        for blob, rec in module.items():
            assert len(blob) == 40 and rec["templates"], blob
            for tpl in rec["templates"]:
                assert len(tpl["first_commit"]) == 40 and tpl["profile"] in ("hera", "yolo")


def test_should_mark_a_blob_both_templates_carried_as_ambiguous():
    for rec in DERIVE.values():
        repos = {t["repo"] for t in rec["templates"]}
        assert (rec["profile"] == "ambiguous") == (len(repos) > 1)


def test_should_only_offer_the_engine_profiles():
    assert {r["engine_profile"] for r in DERIVE.values()} <= {"hera", "yolo", "legacy-v1", "yolo-v1", None}


def test_should_map_every_blob_but_the_three_earliest_to_a_profile():
    """Only blobs whose authoring emitted Parameter(default=) stay legacy, each with its reason."""
    none = {b[:7] for b, r in DERIVE.items() if r["engine_profile"] is None}
    assert none == {"ddec976", "d40194a", "b468ddb"}
    assert all("default=" in DERIVE[b]["no_profile_reason"] for b in DERIVE if b[:7] in none)


def test_should_have_a_profile_for_the_engine_schema_itself():
    """The engine's own two schemas are in the table (an app seeded from the current templates)."""
    assert {"hera", "yolo"} <= {r["engine_profile"] for r in DERIVE.values()}
