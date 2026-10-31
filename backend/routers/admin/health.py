"""System health snapshot, system-metrics sampler, VCL health, DuckDB status, and Celery admin endpoints."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from backend.celery_app import (
    get_celery_status,
    list_ingest_ledger_entries,
    purge_ingest_ledger_entries,
    retry_ingest_ledger_entries,
)
from backend.core import duckdb as _db_mod
from backend.core import duckdb_pool as _pool
from backend.core import metadata as _meta_mod
from backend.core.request_context import require_admin
from backend.core.request_telemetry import query_registry
from backend.core.share_db.settings import get_setting, set_setting
from backend.cron.scheduler import compute_sync_status_cached  # noqa: F401
from backend.models.admin import (
    CeleryStatusResponse,
    DebugSettingsResponse,
    DebugSettingsUpdateBody,
    DuckdbStatusResponse,
    HealthSnapshotResponse,
    IngestLedgerActionRequest,
    IngestLedgerActionResponse,
    IngestLedgerListResponse,
    MetricHistoryBatchResponse,
)
from backend.models.analytics import (
    DuckdbRecycleResponse,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_relative_time_window
from backend.utils.router_utils import (
    bad_request,
    make_error,
)
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
)

from ._helpers import (  # noqa: F401
    _fetch_file_to_zip,
    _QueueFile,
    attach_debug_db_snapshot,
    service_id_from_source,
    stream_zip,
)
from ._router import router
from .admin_queries import _ensure_enabled

_db: Any = _db_mod
logger = logging.getLogger(__name__)


def _get_recycle_mod() -> Any:
    return importlib.import_module("backend.cron.jobs.duckdb_recycle")


def _get_metric_snapshots_mod() -> Any:
    return importlib.import_module("backend.cron.jobs.metric_snapshots")


def __getattr__(name: str) -> Any:
    if name == "_recycle":
        return _get_recycle_mod()
    if name == "metric_snapshots":
        return _get_metric_snapshots_mod()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ---------------------------------------------------------------------------
# System-metrics sampler (folded from backend.system_metrics_sampler)
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 10.0
_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_CACHE_TTL_SECONDS)
_HEALTH_SNAPSHOT_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=16, ttl_seconds=10.0)
_CELERY_STATUS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=8, ttl_seconds=5.0)
_locks: dict[str | None, asyncio.Lock] = {}


async def sample_system_metrics_cached(service_id: str | None) -> dict[str, Any]:
    """Short-TTL, deduped wrapper around :func:`sample_system_metrics`."""
    if _CACHE_TTL_SECONDS > 0:
        cached: dict[str, Any] | None = _cache.get(service_id)
        if cached is not None:
            return cached

    lock = _locks.setdefault(service_id, asyncio.Lock())
    async with lock:
        if _CACHE_TTL_SECONDS > 0:
            cached = _cache.get(service_id)
            if cached is not None:
                return cached
        payload = await asyncio.to_thread(sample_system_metrics, service_id)
        if _CACHE_TTL_SECONDS > 0:
            _cache.set(service_id, payload)
        return payload


def reset_metrics_cache() -> None:
    """Clear the dedup cache + per-key locks."""
    _cache.clear()
    _HEALTH_SNAPSHOT_CACHE.clear()
    _CELERY_STATUS_CACHE.clear()
    _locks.clear()


def sample_system_metrics(service_id: str | None) -> dict[str, Any]:
    """Snapshot every admin-overview metric for ``service_id``."""
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
    return health_snapshot()


def _sample_metric_history() -> dict[str, Any]:
    cutoff = datetime.now(UTC) - timedelta(hours=1)
    return {"series": _get_metric_snapshots_mod().get_batch(since=cutoff)}


def _sample_queries_summary() -> dict[str, Any] | None:
    _ensure_enabled()
    return query_registry.summary()


def _sample_slow_queries_count(service_id: str | None) -> dict[str, Any] | None:
    if not service_id:
        return None
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
    src = _db.get_source_for_service(service_id)
    if not src:
        return None
    from backend.models.admin import LogAccountingResponse
    from backend.routers.admin.log_accounting import _resolve_compute_log_accounting

    compute_fn = _resolve_compute_log_accounting()
    result = compute_fn(src, hours=24, by="hour")
    dumped: dict[str, Any] = LogAccountingResponse.with_telemetry(**result).model_dump(mode="json")
    return dumped


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
    from backend.cron.scheduler import get_scheduler, get_system_job_status

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


_CacheRegistry.register("routers.admin.health._cache", _cache)
_CacheRegistry.register("routers.admin.health._HEALTH_SNAPSHOT_CACHE", _HEALTH_SNAPSHOT_CACHE)
_CacheRegistry.register("routers.admin.health._CELERY_STATUS_CACHE", _CELERY_STATUS_CACHE)
_CacheRegistry.register("routers.admin.health._locks", _locks)

_MAX_LOOKBACK_DAYS = 30
_VALID_WINDOW_RE = re.compile(r"^[1-9]\d*[mhd]\Z", re.IGNORECASE)


@router.get("/admin/system-metrics", response_model=HealthSnapshotResponse)
@router.get("/admin/health", response_model=HealthSnapshotResponse)
@router.get("/admin/health-snapshot", response_model=HealthSnapshotResponse)
def health_snapshot(probe_fos: bool = False) -> dict[str, Any]:
    """One-shot health snapshot for the admin page system health card."""
    import shutil

    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    cache_key = bool(probe_fos)
    if use_cache:
        cached = _HEALTH_SNAPSHOT_CACHE.get(cache_key)
        if cached is not None:
            return dict(cached)

    out: dict[str, Any] = {}

    # ── Load + uptime ─────────────────────────────────────────────────
    try:
        load1, load5, load15 = os.getloadavg()
        out["load"] = {"avg_1m": round(load1, 2), "avg_5m": round(load5, 2), "avg_15m": round(load15, 2)}
    except Exception:
        out["load"] = None

    try:
        import multiprocessing as _mp

        out["vcpus"] = _mp.cpu_count()
    except Exception:
        out["vcpus"] = None

    # ── Memory (Linux /proc/meminfo) ─────────────────────────────────
    try:
        meminfo: dict[str, int] = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, rest = line.partition(":")
                v = rest.strip().split()
                if v and v[0].isdigit():
                    meminfo[k.strip()] = int(v[0]) * 1024  # kB → bytes
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", 0)
        out["memory"] = {
            "total_mb": round(total / 1024 / 1024),
            "available_mb": round(avail / 1024 / 1024),
            "used_pct": round((1 - avail / total) * 100, 1) if total else None,
        }
    except Exception:
        out["memory"] = None

    # ── Data-mount disk usage ────────────────────────────────────────
    for path, label in (("/app/data", "data_mount"), ("/", "root_disk")):
        try:
            d = shutil.disk_usage(path)
            out[label] = {
                "total_gb": round(d.total / 1024 / 1024 / 1024, 1),
                "used_gb": round(d.used / 1024 / 1024 / 1024, 1),
                "free_gb": round(d.free / 1024 / 1024 / 1024, 1),
                "used_pct": round(d.used / d.total * 100, 1) if d.total else None,
            }
        except Exception:
            out[label] = None

    # ── In-flight cron runs ──────────────────────────────────────────
    try:
        from backend.cron.scheduler import list_active_runs

        in_flight = []
        for entry in list_active_runs():
            raw_started = entry.get("started_at")
            started_at = (
                datetime.fromtimestamp(raw_started, tz=UTC).isoformat()
                if isinstance(raw_started, (int, float))
                else raw_started
            )
            in_flight.append(
                {
                    "run_id": entry["run_id"],
                    "service_id": entry.get("service_id"),
                    "task": entry.get("task"),
                    "started_at": started_at,
                }
            )
        out["in_flight_runs"] = in_flight
    except Exception:
        out["in_flight_runs"] = []

    # ── Per-service compaction stats ─────────────────────────────────
    try:
        from backend import config as _svcconfig
        from backend.core.storage import _core as _lc

        stats_by_svc: dict[str, Any] = {}
        for cfg in _svcconfig.list_configs():
            sid = str(cfg.get("service_id") or cfg.get("name") or "")
            try:
                src = _svcconfig.config_to_source(cfg)
                stats_by_svc[sid] = _lc.compaction_stats(src)
            except Exception:
                stats_by_svc[sid] = None
        out["compaction"] = stats_by_svc
    except Exception:
        out["compaction"] = {}

    # ── DuckDB connection-pool wait stats ────────────────────────────
    try:
        from backend.core import duckdb_pool as _pool_mod

        out["pool_wait"] = _pool_mod.get_all_stats()
    except Exception:
        out["pool_wait"] = []

    # ── Scheduler liveness (SRE-06) ──────────────────────────────────
    try:
        out["scheduler_last_tick_age_s"] = _get_metric_snapshots_mod().last_snapshot_age_s()
    except Exception:
        out["scheduler_last_tick_age_s"] = None

    # ── Recent cron failures (SRE-03) ────────────────────────────────
    try:
        from backend import config as _svcconfig
        from backend.core.metadata.usage_log import latest_cron_per_task

        failures: list[dict[str, Any]] = []
        for sid in _svcconfig.list_service_ids():
            try:
                for task, run in latest_cron_per_task(
                    sid, exclude_error_messages=("Process interrupted by server restart",)
                ).items():
                    if run.get("status") == "error":
                        failures.append(
                            {
                                "service_id": sid,
                                "task": task,
                                "status": run.get("status"),
                                "started_at": run.get("started_at"),
                                "error_message": run.get("error_message"),
                            }
                        )
            except Exception:
                continue
        failures.sort(key=lambda f: f.get("started_at") or "", reverse=True)
        out["recent_cron_failures"] = failures[:50]
    except Exception:
        out["recent_cron_failures"] = []

    from backend.config import config_lock_degraded

    out["config_lock_degraded"] = config_lock_degraded()

    # ── Effective observability mode (SRE-20) ────────────────────────
    try:
        from backend.core.request_telemetry import effective_exporter
        from backend.utils.telemetry import effective_format

        out["observability"] = {
            "log_format": effective_format(),
            "otel_exporter": effective_exporter(),
        }
    except Exception:
        out["observability"] = None

    # ── Config-backup freshness (SRE-11 / ADR-13 §2.1) ───────────────
    try:
        import json as _json

        from backend import config as _cfg

        marker_path = os.environ.get("CONFIG_BACKUP_MARKER") or str(
            _cfg.DATA_DIR / "system" / "last_config_backup.json"
        )
        backup: dict[str, Any] | None = None
        if os.path.exists(marker_path):
            try:
                with open(marker_path) as _f:
                    raw = _json.load(_f)
                last_at = raw.get("at")
                age_s: float | None = None
                if last_at:
                    from backend.utils.date_utils import parse_iso_utc

                    dt = parse_iso_utc(str(last_at))
                    if dt is not None:
                        age_s = max(0.0, (datetime.now(UTC) - dt).total_seconds())
                backup = {"last_backup_at": last_at, "age_s": age_s, "source": raw.get("remote")}
            except Exception:
                backup = {"last_backup_at": None, "age_s": None, "source": None}
        out["config_backup"] = backup
    except Exception:
        out["config_backup"] = None

    # ── FOS reachability probe (SRE-13, opt-in) ──────────────────────
    if probe_fos:
        try:
            from backend import config as _svcconfig

            fos_out: dict[str, Any] = {}
            for cfg in _svcconfig.list_configs():
                sid = str(cfg.get("service_id") or cfg.get("name") or "")
                try:
                    src = _svcconfig.config_to_source(cfg)
                    fos_out[sid] = _db.fos_reachable(src)
                except Exception as e:
                    fos_out[sid] = {"reachable": False, "error": str(e)[:200]}
            out["fos"] = fos_out
        except Exception:
            out["fos"] = None

    # ── High-scale disk guard (high-scale topology only) ─────────────
    try:
        from backend import config as _hs_cfg

        if _hs_cfg.is_high_scale_mode():
            from dataclasses import asdict

            from backend.high_scale.archive import (
                disk_snapshot_payload,
                topology_from_environment,
            )

            topology: dict[str, Any] | None
            try:
                topology = asdict(topology_from_environment())
            except ValueError:
                logger.warning("health snapshot: HIGH_SCALE_PROFILE is not a known topology profile")
                topology = None
            out["high_scale"] = {"disk": disk_snapshot_payload(), "topology": topology}
    except Exception:
        logger.warning("health snapshot: high-scale block failed", exc_info=True)

    # ── Celery Metrics (Deprecated) ──────────
    out["celery"] = None

    if use_cache:
        _HEALTH_SNAPSHOT_CACHE.set(cache_key, dict(out))
    return out


@router.get(
    "/admin/metric-history/batch",
    response_model=MetricHistoryBatchResponse,
    dependencies=[Depends(require_admin)],
)
def metric_history_batch(
    since: str = Query(default="1h", description="Lookback window: 30m, 1h, 6h, 24h, 7d, etc."),
) -> JSONResponse:
    """Return every recorded series newer than ``since``."""
    if not since or not _VALID_WINDOW_RE.fullmatch(since.strip()):
        raise HTTPException(
            status_code=400,
            detail=bad_request(
                f"Invalid lookback window '{since}'. Expected <N>m, <N>h, or <N>d (e.g. 30m, 1h, 6h, 24h, 7d)."
            ),
        )
    cutoff = parse_relative_time_window(since, max_lookback_days=_MAX_LOOKBACK_DAYS)
    raw = _get_metric_snapshots_mod().get_batch(since=cutoff)
    return JSONResponse(content=attach_debug_db_snapshot({"series": raw}))


@router.get(
    "/admin/debug-settings",
    response_model=DebugSettingsResponse,
)
def get_debug_settings() -> DebugSettingsResponse:
    """Return the debug settings for query debugging and API call panels."""
    query_vis = get_setting("query_debug_visibility") or "disabled"
    api_vis = get_setting("api_call_debug_visibility") or "disabled"
    return DebugSettingsResponse(
        query_debug_visibility=query_vis,
        api_call_debug_visibility=api_vis,
    )


@router.patch(
    "/admin/debug-settings",
    response_model=DebugSettingsResponse,
)
def update_debug_settings(body: DebugSettingsUpdateBody) -> DebugSettingsResponse:
    """Update the debug settings for query debugging and API call panels."""
    if body.query_debug_visibility is not None:
        if body.query_debug_visibility not in ("disabled", "admins", "analysts", "both"):
            raise HTTPException(
                status_code=400,
                detail=make_error("invalid_query_debug_visibility", "Invalid query_debug_visibility value"),
            )
        set_setting("query_debug_visibility", body.query_debug_visibility)

    if body.api_call_debug_visibility is not None:
        if body.api_call_debug_visibility not in ("disabled", "admins", "analysts", "both"):
            raise HTTPException(
                status_code=400,
                detail=make_error("invalid_api_call_debug_visibility", "Invalid api_call_debug_visibility value"),
            )
        set_setting("api_call_debug_visibility", body.api_call_debug_visibility)

    query_vis = get_setting("query_debug_visibility") or "disabled"
    api_vis = get_setting("api_call_debug_visibility") or "disabled"
    return DebugSettingsResponse(
        query_debug_visibility=query_vis,
        api_call_debug_visibility=api_vis,
    )


@router.post("/admin/duckdb/recycle", response_model=DuckdbRecycleResponse, response_model_exclude_unset=True)
def trigger_duckdb_recycle_endpoint(
    force: bool = Query(False, description="Bypass RSS threshold check and force recycle"),
) -> DuckdbRecycleResponse:
    """Trigger on-demand recycle of DuckDB instances to free parquet metadata cache."""
    recycle_mod = _get_recycle_mod()
    rss_before = _db.current_rss_bytes()
    threshold = recycle_mod._recycle_rss_threshold_bytes()

    if not force and threshold > 0 and rss_before is not None and rss_before < threshold:
        mb_before = rss_before / (1024 * 1024)
        mb_thresh = threshold / (1024 * 1024)
        return DuckdbRecycleResponse(
            ok=True,
            status="skipped",
            reason=f"RSS {mb_before:.0f}MB < threshold {mb_thresh:.0f}MB",
            rss_mb=round(mb_before, 1),
            threshold_mb=round(mb_thresh, 1),
        )

    detail = recycle_mod.recycle_once(reason="manual_admin")
    rss_after = _db.current_rss_bytes()
    freed = (rss_before - rss_after) if (rss_before is not None and rss_after is not None) else None

    return DuckdbRecycleResponse(
        ok=True,
        status="recycled",
        detail=detail,
        rss_before_mb=round(rss_before / (1024 * 1024), 1) if rss_before is not None else None,
        rss_after_mb=round(rss_after / (1024 * 1024), 1) if rss_after is not None else None,
        freed_mb=round(freed / (1024 * 1024), 1) if freed is not None else None,
    )


@router.get("/admin/duckdb/status", response_model=DuckdbStatusResponse, response_model_exclude_unset=True)
def get_duckdb_status_endpoint() -> DuckdbStatusResponse:
    """Return memory metrics, recycle configuration, and active connection pool states."""
    recycle_mod = _get_recycle_mod()
    rss = _db.current_rss_bytes()
    threshold = recycle_mod._recycle_rss_threshold_bytes()
    interval_min = recycle_mod.recycle_interval_min()
    barrier_active = _db.is_recycle_barrier_active()
    pool_stats = _pool.get_pool_status()

    return DuckdbStatusResponse(
        ok=True,
        memory={
            "current_rss_mb": round(rss / (1024 * 1024), 1) if rss is not None else None,
            "recycle_threshold_mb": round(threshold / (1024 * 1024), 1) if threshold > 0 else None,
            "recycle_interval_min": interval_min,
        },
        barrier={
            "is_active": bool(barrier_active),
            "active_paths": list(barrier_active) if isinstance(barrier_active, set) else [],
        },
        pools=pool_stats["pools"],
        retired_pools=pool_stats["retired_pools"],
    )


@router.get(
    "/admin/celery/status",
    response_model=CeleryStatusResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def api_celery_status() -> dict[str, Any]:
    """Expose Celery queue depths, worker status, APScheduler schedule, and
    ingest-ledger summary."""
    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    raw: dict[str, Any] | None = _CELERY_STATUS_CACHE.get("status") if use_cache else None
    if raw is None:
        raw = dict(get_celery_status())
        if use_cache:
            _CELERY_STATUS_CACHE.set("status", dict(raw))
    return attach_debug_db_snapshot(dict(raw))


@router.get(
    "/admin/celery/ledger",
    response_model=IngestLedgerListResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_list(
    status: str | None = Query(
        default="problem",
        description="Filter by ledger status ('problem', 'all', 'dead_letter', 'quarantined', 'discovered', 'claimed', 'committed')",
    ),
    service_id: str | None = Query(default=None, description="Optional service_id filter"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> IngestLedgerListResponse:
    """List entries in ``ingest_ledger`` across services (defaults to dead_letter + quarantined)."""
    return IngestLedgerListResponse(
        **list_ingest_ledger_entries(status=status, service_id=service_id, limit=limit, offset=offset)
    )


@router.post(
    "/admin/celery/ledger/retry",
    response_model=IngestLedgerActionResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_retry(body: IngestLedgerActionRequest) -> IngestLedgerActionResponse:
    """Reset ``dead_letter`` / ``quarantined`` ledger entries back to ``discovered`` and
    best-effort dispatch Celery convert batches."""
    _CELERY_STATUS_CACHE.clear()
    items = [item.model_dump() for item in body.items] if body.items else None
    return IngestLedgerActionResponse(
        **retry_ingest_ledger_entries(items=items, status=body.status, service_id=body.service_id)
    )


@router.post(
    "/admin/celery/ledger/purge",
    response_model=IngestLedgerActionResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_purge(body: IngestLedgerActionRequest) -> IngestLedgerActionResponse:
    """Delete ``dead_letter`` / ``quarantined`` entries from ``ingest_ledger``."""
    _CELERY_STATUS_CACHE.clear()
    items = [item.model_dump() for item in body.items] if body.items else None
    return IngestLedgerActionResponse(
        **purge_ingest_ledger_entries(items=items, status=body.status, service_id=body.service_id)
    )


ROLLUP_BUCKET_COUNTS = (
    "SELECT strftime(date_trunc('{unit}', hour_start), '{fmt}') AS bucket, "
    "  SUM(requests) AS n "
    "FROM read_parquet([{paths_sql}]) "
    "WHERE hour_start >= TIMESTAMPTZ '{start_tz}' "
    "  AND hour_start < TIMESTAMPTZ '{end_tz}' "
    "GROUP BY date_trunc('{unit}', hour_start)"
)

ACTIVE_HOUR_BUCKET_COUNTS = (
    "SELECT strftime(date_trunc('{unit}', timestamp), '{fmt}') AS bucket, COUNT(*) AS n "
    "FROM read_parquet([{paths_sql}], union_by_name=true) "
    "WHERE timestamp >= TIMESTAMP '{start_iso}' "
    "  AND timestamp <  TIMESTAMP '{end_iso}' "
    "GROUP BY date_trunc('{unit}', timestamp)"
)

RAW_BUCKET_COUNTS = (
    "SELECT strftime(date_trunc('{unit}', timestamp), '{fmt}') AS bucket, COUNT(*) AS n "
    "FROM {table_name} "
    "WHERE timestamp >= TIMESTAMP '{start_iso}' "
    "  AND timestamp <  TIMESTAMP '{end_iso}' "
    "GROUP BY date_trunc('{unit}', timestamp)"
)


def rollup_bucket_counts(
    con: Any, *, unit: str, fmt: str, paths_sql: str, start_tz: str, end_tz: str
) -> list[tuple[Any, ...]]:
    """Closed-hour request counts per bucket from the overview rollup parquet files."""
    from backend.utils.telemetry import track_query

    q = ROLLUP_BUCKET_COUNTS.format(unit=unit, fmt=fmt, paths_sql=paths_sql, start_tz=start_tz, end_tz=end_tz)
    with track_query(con, q, [], "log_accounting.rollup_bucket_counts") as cur:
        return list(cur.fetchall())


def active_hour_bucket_counts(
    con: Any, *, unit: str, fmt: str, paths_sql: str, start_iso: str, end_iso: str
) -> list[tuple[Any, ...]]:
    """Row counts per bucket straight from the active-hour committed + buffer parquet files."""
    from backend.utils.telemetry import track_query

    q = ACTIVE_HOUR_BUCKET_COUNTS.format(unit=unit, fmt=fmt, paths_sql=paths_sql, start_iso=start_iso, end_iso=end_iso)
    with track_query(con, q, [], "log_accounting.active_hour_bucket_counts") as cur:
        return list(cur.fetchall())


def raw_bucket_counts(
    con: Any, *, unit: str, fmt: str, table_name: str, start_iso: str, end_iso: str
) -> list[tuple[Any, ...]]:
    """Full raw-table scan fallback for row counts per bucket."""
    from backend.utils.telemetry import track_query

    q = RAW_BUCKET_COUNTS.format(unit=unit, fmt=fmt, table_name=table_name, start_iso=start_iso, end_iso=end_iso)
    with track_query(con, q, [], "log_accounting.raw_bucket_counts") as cur:
        return list(cur.fetchall())
