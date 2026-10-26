"""FastAPI router package with backward-compatible submodule resolution."""

from __future__ import annotations

import importlib as _importlib
import sys as _sys
from typing import Any

_ROUTER_ALIASES: dict[str, str] = {
    "backend.routers.origin": "backend.routers.network",
    "backend.routers.query": "backend.routers.dashboard",
    "backend.routers.performance": "backend.routers.dashboard",
    "backend.routers.alerts": "backend.routers.control_room",
    "backend.routers.usage": "backend.routers.control_room",
    "backend.routers.share_auth": "backend.routers.share_admin",
    "backend.routers.share_oauth": "backend.routers.share_admin",
}


def __getattr__(name: str) -> Any:
    full = f"{__name__}.{name}"
    if full in _ROUTER_ALIASES:
        mod = _importlib.import_module(_ROUTER_ALIASES[full])
        _sys.modules[full] = mod
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
