"""The final task of a run, run with the PLATFORM engine: what the kaos-promote task runs.

    python -I -m kaos_engine.promote

Same set-up as kaos_engine.compose (the engine's `kubecore` first on the path, asserted); the task's input is
KAOS_PROMOTE_PLAN (kubecore/promote.py). Exit codes: kubecore.promote's (0 done, 1 refused or failed), 4 engine broken.
"""

from __future__ import annotations

import sys

from kaos_engine import engine_version
from kaos_engine.render import EXIT_ENGINE_BROKEN, EngineError, find_engine_dir


def main(argv: list[str] | None = None) -> int:
    """Only kubecore.promote is imported (not every module of the engine, which would load the render libraries): the task
    asks a node for a few hundred MiB."""
    import importlib
    from pathlib import Path
    try:
        engine = find_engine_dir(None)
        sys.path[:] = [str(engine)] + [p for p in sys.path if p not in ("", str(engine))]
        for name in [m for m in sys.modules if m == "kubecore" or m.startswith("kubecore.")]:
            del sys.modules[name]
        module = importlib.import_module("kubecore.promote")
        if engine not in Path(module.__file__).resolve().parents:
            raise EngineError(f"kubecore.promote was imported from {module.__file__}, outside the engine {engine}",
                              EXIT_ENGINE_BROKEN)
    except (EngineError, ImportError) as exc:
        print(f"kaos-engine: {exc}", file=sys.stderr)
        return getattr(exc, "code", EXIT_ENGINE_BROKEN)
    print(f"kaos-engine: version={engine_version()} engine={engine}", file=sys.stderr)
    return module.main(argv)


if __name__ == "__main__":
    sys.exit(main())
