"""Every module-level name authoring.py / derive_tree.py ever had is still importable from the engine."""

import importlib
import json

import pytest

from paths import ENGINE

SURFACE = json.loads((ENGINE / "kaos_pipeline_engine" / "compat-surface.json").read_text())


@pytest.mark.parametrize("module", sorted(SURFACE["modules"]))
def test_should_keep_every_historical_module_level_name(module):
    mod = importlib.import_module(f"kubecore.{module}")
    removed = SURFACE["removed"].get(module, {})
    missing = [n for n in SURFACE["modules"][module] if not hasattr(mod, n) and n not in removed]
    assert not missing, (f"kubecore.{module} lost {missing}: restore them or list them under 'removed' with the reason "
                         f"in compat-surface.json")


def test_should_cover_both_modules_since_the_oldest_template():
    assert set(SURFACE["modules"]) == {"authoring", "derive_tree"}
    oldest = min(v["first_seen"]["date"] for m in SURFACE["modules"].values() for v in m.values())
    assert oldest <= "2026-07-14"


def test_should_still_expose_the_names_pipeline_py_uses():
    from kubecore import authoring
    assert callable(authoring.pipeline) and callable(authoring.step)
    assert issubclass(authoring.AuthoringError, Exception)
