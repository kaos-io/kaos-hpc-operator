"""Where things are, for the engine tests."""

from pathlib import Path

ENGINE = Path(__file__).resolve().parent.parent
APP = ENGINE / "tests" / "app"
TOOLS = ENGINE / "tools"
