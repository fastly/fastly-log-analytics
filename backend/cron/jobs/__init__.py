"""Cron job modules package."""

from __future__ import annotations

import importlib
import sys as _sys
from typing import Any

_REAL_JOB_MODULES = (
    "_common",
    "commit",
    "rum_sync",
    "rum_ledger",
    "rum_commit",
    "compaction",
    "maintenance",
    "duckdb_recycle",
    "metric_snapshots",
    "insights_prewarmer",
)

_VIRTUAL_SUBMODULES: dict[str, str] = {
    "metric_snapshot": "backend.cron.jobs.metric_snapshots",
    "optimize": "backend.cron.jobs.maintenance",
    "expire": "backend.cron.jobs.maintenance",
}

_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    for _mod in _REAL_JOB_MODULES:
        _backend_pkg._MODULE_ALIASES.pop(f"backend.cron.jobs.{_mod}", None)
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.query_attribution": "backend.core.request_telemetry",
            "backend.core.request_metrics": "backend.core.request_telemetry",
            "backend.request_metrics_observer": "backend.core.request_telemetry",
            "backend.core.metric_snapshots": "backend.cron.jobs.metric_snapshots",
            "backend.core.duckdb_recycle": "backend.cron.jobs.duckdb_recycle",
            "backend.core.metadata.slow_queries": "backend.core.metadata.usage_log",
            "backend.cron.decorators": "backend.cron.scheduler",
            "backend.cron.lifecycle": "backend.cron.scheduler",
            "backend.cron.jobs.metric_snapshot": "backend.cron.jobs.metric_snapshots",
            "backend.cron.jobs.optimize": "backend.cron.jobs.maintenance",
            "backend.cron.jobs.expire": "backend.cron.jobs.maintenance",
        }
    )


def __getattr__(name: str) -> Any:
    target = _VIRTUAL_SUBMODULES.get(name)
    if target is not None:
        mod = importlib.import_module(target)
        _sys.modules[f"{__name__}.{name}"] = mod
        setattr(_sys.modules[__name__], name, mod)
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
