"""Entrypoint of the runner Job: `python3 -u /runner/main.py` (stdlib only, no install)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kaos_hpc_runner.runner import main  # noqa: E402

sys.exit(main())
