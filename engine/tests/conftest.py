"""Shared setup for the engine tests.

Puts the engine directory on sys.path (so `kubecore` is the ENGINE's copy, never an installed one) and
points the config tree at the fixture app tests/app/, the way the runner does for a real app.
"""

import os
import sys

from paths import APP, ENGINE, TOOLS

sys.path.insert(0, str(ENGINE))
sys.path.insert(0, str(TOOLS))
os.environ.setdefault("KUBECORE_CONFIG_DIR", str(APP / "config"))
os.environ.pop("KUBECORE_SCHEMA_PROFILE", None)  # the engine default (hera) unless a test spawns a profile
