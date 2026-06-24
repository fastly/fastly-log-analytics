"""Sample every metric the admin overview page polls into one payload.

Bundles seven separate admin endpoints (health-snapshot,
metric-history/batch, queries/summary, slow-queries/count,
log-accounting, metadata-storage, system-jobs) into a single dict
keyed to match the React Query slice keys the frontend reads from.
Used by the ``/api/admin/system-metrics/stream`` SSE endpoint to push
state changes once instead of having the browser run seven independent
polls.

Each component call is wrapped so a single failing helper doesn't tank
the whole snapshot — partial data is still useful for the cards that
loaded successfully, and the next sample tick will retry the failing
ones cleanly.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def sample_system_metrics(service_id: str | None) -> dict[str, Any]:
    """Snapshot every admin-overview metric for ``service_id``.

    Per-service because four of the seven components (log-accounting,
    slow-queries-count, metadata-storage, and indirectly health's
    compaction map) are service-scoped. The other three (health,
    metric-history, queries-summary, system-jobs) are global but are
    returned in the same payload so the frontend only opens one stream.
    """
    return {
        "health_snapshot": _safe(_sample_health_snapshot),
        "metric_history_1h": _safe(_sample_metric_history),
        "queries_summary": _safe(_sample_queries_summary),
        "slow_queries_count": _safe(lambda: _sample_slow_queries_count(service_id)),
        "log_accounting": _safe(lambda: _sample_log_accounting(service_id)),
        "metadata_storage": _safe(lambda: _sample_metadata_storage(service_id)),
        "system_jobs": _safe(_sample_system_jobs),
    }


def _safe(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except Exception:
        logger.debug("system-metrics sampler component failed", exc_info=True)
        return None


def _sample_health_snapshot() -> dict[str, Any]:
    from backend.routers.admin.health import health_snapshot

    return health_snapshot()


def _sample_metric_history() -> dict[str, Any]:
    from backend.core import metric_snapshots

    cutoff = datetime.now(UTC) - timedelta(hours=1)
    return {"series": metric_snapshots.get_batch(since=cutoff)}


def _sample_queries_summary() -> dict[str, Any] | None:
    from backend.core.query_registry import query_registry
    from backend.routers.admin_queries import _ensure_enabled

    _ensure_enabled()
    return query_registry.summary()


def _sample_slow_queries_count(service_id: str | None) -> dict[str, Any] | None:
    if not service_id:
        return None
    from backend.core import metadata as _meta_mod
    from backend.routers.admin_queries import _ensure_enabled

    _ensure_enabled()
    since_utc = time.time() - 24 * 3600
    return {
        "count": _meta_mod.count_slow_queries(service_id, since_utc=since_utc, threshold_ms=1000.0),
        "since_hours": 24,
        "threshold_ms": 1000.0,
    }


def _sample_log_accounting(service_id: str | None) -> dict[str, Any] | None:
    if not service_id:
        return None
    from backend.core import duckdb as db

    src = db.get_source_for_service(service_id)
    if not src:
        return None
    # compute_log_accounting returns a dict that nests pydantic
    # ``LogAccountingBucket`` instances (not JSON-serializable by raw
    # json.dumps). Route the result through the response model the
    # /api/admin/log-accounting endpoint uses so the SSE payload has
    # the same plain-dict wire shape the React Query slice key
    # ['admin','overview','log-accounting'] already expects.
    from backend.models.admin import LogAccountingResponse
    from backend.routers.admin.log_accounting import compute_log_accounting

    result = compute_log_accounting(src, hours=24, by="hour")
    return LogAccountingResponse.with_telemetry(**result).model_dump(mode="json")


def _sample_metadata_storage(service_id: str | None) -> dict[str, Any] | None:
    if not service_id:
        return None
    from backend import config as svcconfig
    from backend.core.metadata import (
        DEFAULT_METADATA_RETENTION,
        get_metadata_storage_stats,
        is_ingested_files_dedup_active,
    )

    stats = get_metadata_storage_stats(service_id)
    cfg = svcconfig.load_config(service_id) or {}
    retention = {**DEFAULT_METADATA_RETENTION, **(cfg.get("metadata_retention") or {})}
    ingested_files_locked = not is_ingested_files_dedup_active(service_id)
    return {**stats, "retention": retention, "ingested_files_locked": ingested_files_locked}


def _sample_system_jobs() -> dict[str, Any]:
    # Job-label table kept identical to backend/routers/admin_usage.py's
    # /admin/system-jobs handler so the SSE payload matches the response
    # shape the SystemStatus card already renders from.
    from backend.scheduler import get_scheduler
    from backend.utils.system_jobs import get_system_job_status

    statuses = get_system_job_status()
    job_labels = {
        "bot_data_refresh": "Bot Data Refresh",
        "rdns_enrichment": "rDNS Enrichment",
        "share_audit_purge": "Share Audit Purge",
    }
    sched = get_scheduler()
    result: list[dict[str, Any]] = []
    for job_id, label in job_labels.items():
        entry: dict[str, Any] = {
            "id": job_id,
            "name": label,
            "next_run_at": None,
            **statuses.get(job_id, {"last_run_at": None, "status": None, "duration_s": None, "detail": ""}),
        }
        if sched is not None:
            try:
                job = sched.get_job(job_id)
            except Exception:
                job = None
            next_run = getattr(job, "next_run_time", None) if job else None
            if next_run:
                from backend.utils.date_utils import iso_z

                entry["next_run_at"] = iso_z(next_run)
        result.append(entry)
    return {"jobs": result}
