"""kaos_engine: the runner that renders an app's pipeline.py with the platform engine (D-79).

The engine itself (the `kubecore` package) is NOT in this package: it lives next to it
(`<engine dir>/kubecore`) and must never be installed as a top-level `kubecore` (kaos-lib 0.4 ships a
`kubecore` alias; PRD-1147 T-04/T-14). See engine/README.md.
"""

import os
from importlib import metadata

DIST_NAME = "kaos-pipeline-engine"


def engine_version() -> str:
    """The engine release: the installed wheel's version, else KAOS_ENGINE_VERSION, else "0+unknown"."""
    try:
        return metadata.version(DIST_NAME)
    except metadata.PackageNotFoundError:
        return os.environ.get("KAOS_ENGINE_VERSION", "0+unknown")
