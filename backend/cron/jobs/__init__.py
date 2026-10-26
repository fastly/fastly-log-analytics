"""Cron job modules package."""

from __future__ import annotations

import sys as _sys

_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.query_attribution": "backend.core.request_telemetry",
            "backend.core.request_metrics": "backend.core.request_telemetry",
            "backend.request_metrics_observer": "backend.core.request_telemetry",
            "backend.core.metric_snapshots": "backend.cron.jobs.metadata",
            "backend.core.duckdb_recycle": "backend.cron.jobs.metadata",
            "backend.cron.jobs.duckdb_recycle": "backend.cron.jobs.metadata",
            "backend.core.metadata.slow_queries": "backend.core.metadata.usage_log",
            "backend.cron.decorators": "backend.cron.scheduler",
            "backend.cron.lifecycle": "backend.cron.scheduler",
            "backend.cron.jobs.insights_prewarmer": "backend.cron.jobs.metadata",
            "backend.cron.jobs.rum_commit": "backend.cron.jobs.rum_sync",
        }
    )
