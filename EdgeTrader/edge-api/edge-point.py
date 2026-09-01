"""
edge-point.py — server entrypoint for main-api.py.

Kept separate from main-api.py so the route definitions stay a plain
module (useful for testing the FastAPI app directly, e.g. with
TestClient, without booting a real server) and so process-startup
concerns (host/port/log level) live in one place.

main-api.py's filename has a hyphen, so it can't be reached with a normal
`import main-api` statement (that's not a valid Python identifier). This
loads it by file path via importlib instead, then hands the resulting
`app` object straight to uvicorn.run() — no CLI "module:attr" string form
is involved, so the hyphen never has to survive an import statement.

Run with: python edge-point.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import uvicorn

_MAIN_API_PATH = Path(__file__).with_name("main-api.py")


def _load_main_api_app():
    spec = importlib.util.spec_from_file_location("main_api", _MAIN_API_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {_MAIN_API_PATH}")
    module = importlib.util.module_from_spec(spec)
    # Register under a valid (underscored) name so anything inside
    # main-api.py that does `import sys; sys.modules[...]`-style lookups,
    # or a future `from main_api import ...` elsewhere, behaves normally.
    sys.modules["main_api"] = module
    spec.loader.exec_module(module)
    return module.app


app = _load_main_api_app()

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
