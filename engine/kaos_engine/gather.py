"""The task after a matrix step, run with the PLATFORM engine: what the kaos-gather-<step> task runs.

    python -I -m kaos_engine.gather

Same set-up as kaos_engine.compose (the engine's `kubecore` first on the path, asserted); the task's input is
KAOS_GATHER_RESULTS, KAOS_GATHER_ITEMS and KAOS_GATHER_PLAN (kubecore/gather.py).
Exit codes: kubecore.gather's (0 done, 1 no item succeeded or the results are unusable), 4 engine broken.
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
    if "kubecore.gather" not in modules:
        print(f"kaos-engine: the engine at {engine} has no kubecore/gather.py", file=sys.stderr)
        return EXIT_ENGINE_BROKEN
    print(f"kaos-engine: version={engine_version()} engine={engine}", file=sys.stderr)
    return modules["kubecore.gather"].main(argv)


if __name__ == "__main__":
    sys.exit(main())
