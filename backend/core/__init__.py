from __future__ import annotations

import importlib
import sys as _sys
from typing import Any

_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.pop("backend.core.faro_versions", None)
    _backend_pkg._MODULE_ALIASES.pop("backend.core.rum_ingest", None)
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.clickhouse_manifest": "backend.core.clickhouse",
            "backend.core.metric_snapshots": "backend.cron.jobs.metric_snapshots",
            "backend.core.duckdb_recycle": "backend.cron.jobs.duckdb_recycle",
        }
    )

_VIRTUAL_SUBMODULES: dict[str, str] = {
    "metric_snapshots": "backend.cron.jobs.metric_snapshots",
    "duckdb_recycle": "backend.cron.jobs.duckdb_recycle",
}


def __getattr__(name: str) -> Any:
    target = _VIRTUAL_SUBMODULES.get(name)
    if target is not None:
        mod = importlib.import_module(target)
        _sys.modules[f"{__name__}.{name}"] = mod
        setattr(_sys.modules[__name__], name, mod)
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
