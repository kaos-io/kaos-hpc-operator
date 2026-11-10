"""Version only: the wheel's version is the operator build tag in PEP 440 form (everything else is in pyproject.toml)."""

import importlib.util
import os
from pathlib import Path

from setuptools import setup

HERE = Path(__file__).resolve().parent


def _version() -> str:
    spec = importlib.util.spec_from_file_location("kaos_engine_version", HERE / "kaos_engine" / "version.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    tag = os.environ.get("KAOS_ENGINE_TAG") or (HERE.parent / "VERSION").read_text().strip()
    return module.pep440_from_tag(tag)


setup(version=_version())
