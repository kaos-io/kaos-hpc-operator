"""The final task of a run, run with the PLATFORM engine: what the kaos-promote task runs.

    python -I -m kaos_engine.promote

Same set-up as kaos_engine.compose (the engine's `kubecore` first on the path, asserted); the task's input is
KAOS_PROMOTE_PLAN (kubecore/promote.py). Exit codes: kubecore.promote's (0 done, 1 refused or failed), 4 engine broken.
"""

from __future__ import annotations

import sys

from kaos_engine import engine_version
from kaos_engine.render import EXIT_ENGINE_BROKEN, EngineError, find_engine_dir, import_engine


def main(argv: list[str] | None = None) -> int:
    try:
        engine = find_engine_dir(None)
        sys.path[:] = [str(engine)] + [p for p in sys.path if p not in ("", str(engine))]
        modules = import_engine(engine)
    except EngineError as exc:
        print(f"kaos-engine: {exc}", file=sys.stderr)
        return exc.code
    if "kubecore.promote" not in modules:
        print(f"kaos-engine: the engine at {engine} has no kubecore/promote.py", file=sys.stderr)
        return EXIT_ENGINE_BROKEN
    print(f"kaos-engine: version={engine_version()} engine={engine}", file=sys.stderr)
    return modules["kubecore.promote"].main(argv)


if __name__ == "__main__":
    sys.exit(main())
