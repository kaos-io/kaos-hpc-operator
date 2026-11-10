"""Compose an app's parameters with the PLATFORM engine: what the compose-and-validate step runs.

    python -I -m kaos_engine.compose <override tokens> --output /work/params.yaml --lock-output /work/data-lock.json

The engine's `kubecore` is put first on the path and imported from the engine directory (asserted), so
the result never depends on what else the image or the working directory holds: kaos-lib, which the
compose image installs to resolve the run's data, still ships a top-level `kubecore` alias. The
arguments are the compose step's own (kubecore/compose.py), passed through unchanged.

Exit codes: kubecore.compose's (0 composed, 1 a compose error), 2 bad invocation, 4 engine broken.
"""

from __future__ import annotations

import sys

from kaos_engine import engine_version
from kaos_engine.render import EngineError, find_engine_dir, import_engine


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        engine = find_engine_dir(None)
        sys.path[:] = [str(engine)] + [p for p in sys.path if p not in ("", str(engine))]
        modules = import_engine(engine)
    except EngineError as exc:
        print(f"kaos-engine: {exc}", file=sys.stderr)
        return exc.code
    print(f"kaos-engine: version={engine_version()} engine={engine}", file=sys.stderr)
    modules["kubecore.compose"].main(argv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
