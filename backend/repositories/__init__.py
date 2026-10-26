"""Repository layer for analytical queries and rollup planning."""

from __future__ import annotations

import importlib as _importlib
import sys as _sys
from typing import Any

_REPO_ALIASES: dict[str, str] = {
    "backend.repositories.rollup_coverage": "backend.core.rollups.recompute",
    "backend.repositories._rollup_planner": "backend.core.rollups.recompute",
    "backend.repositories.top_n_rollups": "backend.repositories._base",
    "backend.repositories.ip_spread_rollups": "backend.repositories._base",
    "backend.repositories.performance": "backend.repositories.origin",
    "backend.repositories.cmcd": "backend.repositories.origin",
    "backend.repositories.session_scoring": "backend.repositories.sessions",
    "backend.repositories._sql.performance": "backend.repositories.origin",
    "backend.repositories._sql.cmcd": "backend.repositories.origin",
    "backend.repositories._sql.session_scoring": "backend.repositories.sessions",
}

_backend_mod = _sys.modules.get("backend")
if _backend_mod is not None and hasattr(_backend_mod, "_MODULE_ALIASES"):
    _backend_mod._MODULE_ALIASES.update(_REPO_ALIASES)


def __getattr__(name: str) -> Any:
    full = f"{__name__}.{name}"
    if full in _REPO_ALIASES:
        mod = _importlib.import_module(_REPO_ALIASES[full])
        _sys.modules[full] = mod
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
