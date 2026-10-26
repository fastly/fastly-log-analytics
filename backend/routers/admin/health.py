"""Shared admin router helpers, ClickHouse replay/retention routes, and multiplexed SSE channel."""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import sys
import zipfile
from collections.abc import AsyncIterator
from typing import Any, Literal, cast

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from backend import config
from backend.core import metadata
from backend.core.clickhouse import PgManifest, get_clickhouse_client
from backend.core.clickhouse_publication import FosArtifacts, full_rebuild, replay_unpublished
from backend.core.clickhouse_schema import CLICKHOUSE_SCHEMA_VERSION, target_identity
from backend.cron_runs_publisher import publisher as cron_runs_publisher
from backend.deps import get_service_id, require_admin
from backend.high_scale.archive_models import (
    ServiceProfile,
    StorageProfile,
    latest_disk_snapshot,
    recommend_retention,
)
from backend.models._base import DEFAULT_ERROR_RESPONSES, ErrorDetail
from backend.models.admin_status import (
    ClickHouseReplayErrorResponse,
    ClickHouseReplayRequest,
    ClickHouseReplayResponse,
    ClickHouseStatusResponse,
)
from backend.models.high_scale import HighScaleRetentionRecommendationResponse
from backend.sync_status_publisher import publisher as sync_status_publisher
from backend.utils.router_utils import load_service_config_or_503, make_error, require_service_id
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
    iter_with_disconnect_ping,
    sse_response,
)

logger = logging.getLogger(__name__)
_struct_logger = structlog.get_logger(__name__)

# Shared APIRouter instance for the admin package. Lives here so sub-modules
# can import ``router`` without triggering the package-init's sub-module
# imports (which would be a circular dependency).
router: APIRouter = APIRouter(prefix="/api", tags=["admin"], responses=DEFAULT_ERROR_RESPONSES)


# ---------------------------------------------------------------------------
# Shared helpers (folded from backend.routers.admin._helpers)
# ---------------------------------------------------------------------------


class _QueueFile:
    """File-like wrapper around a queue.Queue for streaming ZIP generation."""

    def __init__(self, q: queue.Queue[bytes | None]) -> None:
        self.q = q
        self.offset = 0

    def write(self, b: bytes) -> int:
        self.q.put(b)
        n = len(b)
        self.offset += n
        return n

    def flush(self) -> None:
        pass

    def tell(self) -> int:
        return self.offset


def stream_zip(
    context_label: str,
    service_name: str,
    populate: Any,
) -> Any:
    """Run a streaming ZIP producer in a daemon thread."""

    def zip_worker(q: queue.Queue[bytes | None]) -> None:
        from backend.utils.telemetry import (
            flush_usage_log as _flush,
        )
        from backend.utils.telemetry import (
            process_context_scope as _pcs,
        )
        from backend.utils.telemetry import (
            start_call_tracking as _sct,
        )

        _sct()
        with _pcs(context_label):
            try:
                from typing import cast as _cast

                with zipfile.ZipFile(_cast(Any, _QueueFile(q)), "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    populate(zf)
            except Exception:
                logger.error("Error in ZIP generation", exc_info=True)
            finally:
                try:
                    _flush(service_name)
                except Exception:
                    logger.debug("zip_worker: _flush failed (ignored)", exc_info=True)
                q.put(None)

    return _stream_from_worker(zip_worker)


def _fetch_file_to_zip(
    source: dict[str, Any],
    fos_client: Any,
    cdn: str,
    key: str,
    arcname: str,
    zf: zipfile.ZipFile,
    caller: str,
) -> bool:
    """Fetch a single S3 key into the zip via CDN with fallback to direct FOS."""
    import time as _t
    import urllib.parse
    import urllib.request

    from backend.utils.telemetry import record_cdn_call as _rcdn

    if cdn:
        url = f"{cdn}/{urllib.parse.quote(key)}"
        try:
            req = urllib.request.Request(url)
            if source.get("cdn_secret"):
                req.add_header("x-fastly-key", source["cdn_secret"])
            t0 = _t.time()
            bytes_read = 0
            cdn_headers = None
            with urllib.request.urlopen(req, timeout=30) as response:
                cdn_headers = response.headers
                with zf.open(arcname, "w", force_zip64=True) as dest:
                    while True:
                        chunk = response.read(65536)
                        if not chunk:
                            break
                        bytes_read += len(chunk)
                        dest.write(chunk)
            _rcdn(
                "GET",
                key,
                round((_t.time() - t0) * 1000, 2),
                headers=cdn_headers,
                bytes_count=bytes_read,
                caller=caller,
            )
            return True
        except ClientDisconnected:
            raise
        except Exception:
            logger.warning("CDN fetch failed for %s, falling back to FOS", key, exc_info=True)

    try:
        resp = fos_client.get_object(Bucket=source["bucket"], Key=key)
        with zf.open(arcname, "w", force_zip64=True) as dest:
            body = resp["Body"]
            while True:
                chunk = body.read(65536)
                if not chunk:
                    break
                dest.write(chunk)
        return True
    except ClientDisconnected:
        raise
    except Exception:
        logger.error("Error fetching %s from FOS", key, exc_info=True)
        return False


def attach_debug_db_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach ``_debug_sqlite`` and ``_debug_postgres`` when debug responses are enabled."""
    from backend.models.common import _debug_responses_enabled

    if _debug_responses_enabled():
        from backend.utils.telemetry import get_sqlite_queries

        ds = list(get_sqlite_queries())
        payload["_debug_sqlite"] = ds
        payload["_debug_postgres"] = ds
    return payload


def service_id_from_source(
    source: dict[str, Any],
    service_id: str | None = None,
    *,
    prefer_name: bool = False,
) -> str:
    """Resolve ``service_id`` from an explicit override or the ``source`` dict."""
    if service_id:
        return service_id
    if prefer_name:
        return str(source.get("name") or source.get("service_id") or "")
    return str(source.get("service_id") or source.get("name") or "")


# ---------------------------------------------------------------------------
# ClickHouse replay & High-Scale retention (folded from backend.routers.admin.clickhouse)
# ---------------------------------------------------------------------------


@router.get(
    "/admin/clickhouse/status",
    response_model=ClickHouseStatusResponse,
    responses={503: {"model": ClickHouseStatusResponse}},
    dependencies=[Depends(require_admin)],
)
def api_clickhouse_status(
    response: Response,
    service_id: str | None = Query(default=None, max_length=128),
) -> Any:
    service_id = require_service_id(service_id)
    load_service_config_or_503(service_id)
    try:
        client = get_clickhouse_client()
        if client is None:
            return ClickHouseStatusResponse.with_telemetry(service_id=service_id, enabled=False, health="disabled")
        client.health()
        status = PgManifest(bounded=True).admin_status(service_id, target_identity(client))
        if status["schema_version"] != CLICKHOUSE_SCHEMA_VERSION:
            response.status_code = 503
            return ClickHouseStatusResponse.with_telemetry(
                service_id=service_id, enabled=True, health="unavailable", **status
            )
        return ClickHouseStatusResponse.with_telemetry(service_id=service_id, enabled=True, health="ok", **status)
    except Exception as exc:
        _struct_logger.warning("clickhouse.admin_status", outcome="unavailable", error_kind=type(exc).__name__)
        response.status_code = 503
        return ClickHouseStatusResponse.with_telemetry(service_id=service_id, enabled=True, health="unavailable")


@router.post(
    "/admin/clickhouse/replay",
    response_model=ClickHouseReplayResponse,
    responses={503: {"model": ClickHouseReplayErrorResponse}},
    dependencies=[Depends(require_admin)],
)
def api_clickhouse_replay(req: ClickHouseReplayRequest) -> Any:
    cfg = load_service_config_or_503(req.service_id, writable=True)
    if req.limit > 100:
        raise HTTPException(422, detail=make_error("invalid_replay_limit", "HTTP replay limit must be 1..100"))
    try:
        client = get_clickhouse_client()
        if client is None:
            raise HTTPException(409, detail=make_error("clickhouse_disabled", "ClickHouse prototype disabled"))
        store = PgManifest(bounded=True)
        target = target_identity(client)
        try:
            preview = store.replay_preview(
                req.service_id, dataset_id=req.dataset_id, generation=req.generation, target=target, limit=req.limit
            )
        except LookupError:
            raise HTTPException(
                404, detail=make_error("replay_not_found", "Replay reference not found for service")
            ) from None
        except ValueError:
            raise HTTPException(
                409,
                detail=make_error("replay_ineligible", "Replay reference expired, incompatible, or exceeds HTTP cap"),
            ) from None
        operation: Literal["rebuild", "resume"] = "rebuild" if req.dataset_id else "resume"
        reply = ClickHouseReplayResponse(
            service_id=req.service_id,
            dry_run=req.dry_run,
            operation=operation,
            generation=req.generation,
            limit=req.limit,
            **preview,
        )
        if req.dry_run:
            return reply
        objects = FosArtifacts(config.config_to_source(cfg))
        metadata.record_audit(
            req.service_id,
            event_type="clickhouse_replay",
            details={"action": operation, "phase": "requested", "limit": req.limit},
        )
        try:
            if req.dataset_id:
                result = full_rebuild(
                    req.service_id, req.dataset_id, loader=objects.load, limit=req.limit, store=store, client=client
                )
            else:
                if req.generation is None:
                    raise RuntimeError("req.generation is unexpectedly None")
                result = replay_unpublished(
                    req.service_id,
                    generation=req.generation,
                    loader=objects.load,
                    limit=req.limit,
                    store=store,
                    client=client,
                )
        except Exception:
            metadata.record_audit(
                req.service_id,
                event_type="clickhouse_replay",
                details={"action": operation, "phase": "failed", "limit": req.limit},
            )
            raise
        metadata.record_audit(
            req.service_id,
            event_type="clickhouse_replay",
            details={
                "action": operation,
                "phase": "completed",
                "attempted": result.attempted,
                "published": result.published,
                "activated": result.activated,
            },
        )
        reply.generation = result.generation
        reply.attempted = result.attempted
        reply.published = result.published
        reply.activated = result.activated
        return reply
    except HTTPException:
        raise
    except Exception as exc:
        _struct_logger.warning("clickhouse.admin_replay", outcome="unavailable", error_kind=type(exc).__name__)
        error = ClickHouseReplayErrorResponse(
            detail=ErrorDetail.model_validate(
                make_error("replay_unavailable", "ClickHouse replay unavailable; inspect status before retrying")
            )
        )
        return JSONResponse(status_code=503, content=error.model_dump(by_alias=True))


@router.get(
    "/admin/high-scale/retention-recommendation",
    response_model=HighScaleRetentionRecommendationResponse,
)
def api_high_scale_retention_recommendation(
    service_id: str | None = Query(default=None, max_length=128),
    events_per_second: float = Query(gt=0, description="Observed or estimated request events per second"),
    average_row_bytes: float = Query(default=500.0, gt=0),
    total_bytes: int | None = Query(
        default=None, gt=0, description="Serving volume size; defaults to the sampled volume"
    ),
) -> HighScaleRetentionRecommendationResponse:
    """Recommend hot/warm retention for sizing; an explicit per-service setting always wins."""
    service_id = require_service_id(service_id)
    if not config.is_high_scale_mode():
        raise HTTPException(404, detail=make_error("high_scale_not_enabled", "High-scale mode is not enabled"))
    cfg = load_service_config_or_503(service_id)

    volume_source: Literal["request", "sampled"] = "request"
    volume_bytes = total_bytes
    if volume_bytes is None:
        snapshot = latest_disk_snapshot()
        if snapshot is None:
            raise HTTPException(
                409,
                detail=make_error(
                    "high_scale_volume_unknown",
                    "Serving volume size is not sampled yet; pass total_bytes",
                ),
            )
        volume_bytes = snapshot.total_bytes
        volume_source = "sampled"

    recommendation = recommend_retention(
        ServiceProfile(events_per_second=events_per_second, average_row_bytes=average_row_bytes),
        StorageProfile(total_bytes=volume_bytes),
    )
    explicit = ((cfg.get("provisioning") or {}).get("cron_sync") or {}).get("data_retention_days")
    explicit_days = explicit if isinstance(explicit, int) and not isinstance(explicit, bool) and explicit >= 0 else None
    source: Literal["override", "recommendation"] = "override" if explicit_days is not None else "recommendation"
    effective: float | None
    if explicit_days is None:
        effective = recommendation.warm_days
    else:
        effective = float(explicit_days) if explicit_days > 0 else None

    _struct_logger.info(
        "high_scale.retention_recommended",
        service_id=service_id,
        events_per_second=events_per_second,
        average_row_bytes=average_row_bytes,
        volume_total_bytes=volume_bytes,
        volume_total_source=volume_source,
        recommended_hot_days=round(recommendation.hot_days, 3),
        recommended_warm_days=round(recommendation.warm_days, 3),
        explicit_retention_days=explicit_days,
        source=source,
    )
    return HighScaleRetentionRecommendationResponse(
        service_id=service_id,
        recommended_hot_days=recommendation.hot_days,
        recommended_warm_days=recommendation.warm_days,
        reasoning=recommendation.reasoning,
        volume_total_bytes=volume_bytes,
        volume_total_source=volume_source,
        explicit_retention_days=explicit_days,
        effective_retention_days=effective,
        source=source,
    )


# ── System Health & Query Monitor (backend.routers.admin.health) ──

"""System health snapshot endpoint for the admin page."""


import logging
import os
import re
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query

from backend.celery_app import (
    get_celery_status,
    list_ingest_ledger_entries,
    purge_ingest_ledger_entries,
    retry_ingest_ledger_entries,
)
from backend.core import duckdb as _db_mod
from backend.core import duckdb_pool as _pool
from backend.core import metadata as _meta_mod

_db: Any = _db_mod
from backend.core.request_telemetry import query_registry
from backend.core.share_db.settings import get_setting, set_setting
from backend.cron.jobs import metadata as _recycle
from backend.cron.jobs import metadata as metric_snapshots
from backend.deps import get_source, require_admin
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin import (
    DebugSettingsResponse,
    DebugSettingsUpdateBody,
    HealthSnapshotResponse,
    IngestedFilesResponse,
    LogExtentsResponse,
    MetricHistoryBatchResponse,
    SyncStatusResponse,
)
from backend.models.admin_actions import (
    ActiveQueryRow,
    CancelResponse,
    DuckdbRecycleResponse,
    QueryMonitorConfigResponse,
    SlowQueriesClearResponse,
    SlowQueriesCountResponse,
    SlowQueriesResponse,
    SnapshotResponse,
    SummaryResponse,
    VclHealthResponse,
)
from backend.models.admin_status import (
    CeleryStatusResponse,
    DuckdbStatusResponse,
    IngestLedgerActionRequest,
    IngestLedgerActionResponse,
    IngestLedgerListResponse,
)
from backend.sync_status_snapshot import compute_sync_status_cached  # noqa: F401
from backend.utils.date_utils import parse_relative_time_window
from backend.utils.dir_size import _get_dir_size
from backend.utils.router_utils import (
    bad_request,
    not_found,
    query_errors,
    raise_internal,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System-metrics sampler (folded from backend.system_metrics_sampler)
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 10.0
_cache: dict[str | None, tuple[float, dict[str, Any]]] = {}
_locks: dict[str | None, asyncio.Lock] = {}


async def sample_system_metrics_cached(service_id: str | None) -> dict[str, Any]:
    """Short-TTL, deduped wrapper around :func:`sample_system_metrics`."""
    now = time.monotonic()
    cached = _cache.get(service_id)
    if cached is not None and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    lock = _locks.setdefault(service_id, asyncio.Lock())
    async with lock:
        cached = _cache.get(service_id)
        if cached is not None and time.monotonic() - cached[0] < _CACHE_TTL_SECONDS:
            return cached[1]
        payload = await asyncio.to_thread(sample_system_metrics, service_id)
        _cache[service_id] = (time.monotonic(), payload)
        return payload


def reset_metrics_cache() -> None:
    """Clear the dedup cache + per-key locks."""
    _cache.clear()
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
    return {"series": metric_snapshots.get_batch(since=cutoff)}


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
    from backend.routers.admin.log_accounting import compute_log_accounting

    result = compute_log_accounting(src, hours=24, by="hour")
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
    from backend.cron.decorators import get_system_job_status
    from backend.cron.scheduler import get_scheduler

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


meta_router = APIRouter(prefix="/api", tags=["meta"], responses=DEFAULT_ERROR_RESPONSES)

_MAX_LOOKBACK_DAYS = 30
_VALID_WINDOW_RE = re.compile(r"^[1-9]\d*[mhd]\Z", re.IGNORECASE)


@router.get("/admin/system-metrics", response_model=HealthSnapshotResponse)
@router.get("/admin/health", response_model=HealthSnapshotResponse)
@router.get("/admin/health-snapshot", response_model=HealthSnapshotResponse)
def health_snapshot(probe_fos: bool = False) -> dict[str, Any]:
    """One-shot health snapshot for the admin page system health card.

    Returns CPU load averages, memory, disk usage of the data mount,
    docker container CPU/memory (if reachable), the count of in-flight
    cron runs, the age of the last scheduler tick (SRE-06), the set of
    services+tasks whose latest terminal cron run errored (SRE-03), the
    effective log/exporter mode (SRE-20), and config-backup freshness
    (SRE-11). Uses only stdlib + per-service SQLite (no psutil dep).

    ``probe_fos=1`` additionally issues a per-service FOS reachability
    probe (SRE-13) — a real network round-trip that bills a Class-B op,
    so it is opt-in and off by default (the card never sets it).
    """
    import shutil

    out: dict = {}

    # ── Load + uptime ─────────────────────────────────────────────────
    try:
        load1, load5, load15 = os.getloadavg()
        out["load"] = {"avg_1m": round(load1, 2), "avg_5m": round(load5, 2), "avg_15m": round(load15, 2)}
    except Exception:
        out["load"] = None

    # vCPU count to interpret load (load > vCPU = backlog).
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
    # Use list_active_runs() (which filters out runs whose last event is
    # done/error) instead of iterating _run_metadata directly. The dict
    # holds entries for an hour after completion (the cleanup TTL), so the
    # raw iteration was showing dozens of stale "sync" entries in the
    # System Health card.
    try:
        from backend.cron_progress import list_active_runs

        in_flight = []
        for entry in list_active_runs():
            # cron_progress stores started_at as a float epoch (time.time());
            # HealthInFlightRun.started_at is typed str|None, so coerce to an
            # ISO-8601 string here. Without this, ANY in-flight run makes
            # FastAPI raise ResponseValidationError → 500 on the health card
            # (only reproduces while a cron is actually running, e.g. right
            # after a restart).
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
        from backend.core import local_compaction as _lc

        stats_by_svc: dict = {}
        for cfg in _svcconfig.list_configs():
            sid = cfg.get("service_id") or cfg.get("name")
            try:
                src = _svcconfig.config_to_source(cfg)
                stats_by_svc[sid] = _lc.compaction_stats(src)
            except Exception:
                stats_by_svc[sid] = None
        out["compaction"] = stats_by_svc
    except Exception:
        out["compaction"] = {}

    # ── DuckDB connection-pool wait stats (Phase 6 in-process sampler) ──
    # Backs the Pool Wait card in the admin SystemHealthCard. The same
    # samples also stream to the OTel ``app.thread_wait_ms`` histogram for
    # off-box analysis; this in-process projection is for the UI's 1s poll.
    try:
        from backend.core import duckdb_pool as _pool_mod

        out["pool_wait"] = _pool_mod.get_all_stats()
    except Exception:
        out["pool_wait"] = []

    # ── Scheduler liveness (SRE-06) ──────────────────────────────────
    # Age of the newest metric_snapshots row. That sampler is a global
    # 60s APScheduler job, so a large/None age witnesses a dead scheduler
    # — the signal that distinguishes "no crons running because idle" from
    # "no crons running because the scheduler thread died". None = fresh
    # boot with no samples yet (the card renders "unknown", not an alarm).
    try:
        from backend.cron.jobs import metadata as _ms

        out["scheduler_last_tick_age_s"] = _ms.last_snapshot_age_s()
    except Exception:
        out["scheduler_last_tick_age_s"] = None

    # ── Recent cron failures (SRE-03) ────────────────────────────────
    # The cross-service aggregate ADR-09 §2.3's runbook tells the operator
    # to watch. For each configured service, take the latest *terminal* run
    # per task and surface the ones that ended in error. This catches the
    # non-`sync` crons (commit / optimize / rollup_compact / metadata_sync)
    # that the deep /api/health probe and the per-service Cron History both
    # miss as a single glance.
    try:
        from backend import config as _svcconfig
        from backend.core.metadata.cron_log import latest_cron_per_task

        failures: list[dict] = []
        for sid in _svcconfig.list_service_ids():
            try:
                # Exclude boot-reap rows (a cron the deploy/restart stopped
                # mid-run) BEFORE ranking, so a restart-interrupted cron isn't
                # reported as an environment fault and can't mask the real
                # terminal run beneath it — same classification the deep-health
                # degraded check applies in backend/main.py.
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
                # One unreadable service's metadata.db must not sink the
                # whole aggregate — skip it (same per-probe isolation the
                # rest of this endpoint uses).
                continue
        # Newest-first so the card shows the freshest breakage at the top;
        # cap defensively so a many-service deploy can't bloat the payload.
        failures.sort(key=lambda f: f.get("started_at") or "", reverse=True)
        out["recent_cron_failures"] = failures[:50]
    except Exception:
        out["recent_cron_failures"] = []

    from backend.config import config_lock_degraded

    out["config_lock_degraded"] = config_lock_degraded()

    # ── Effective observability mode (SRE-20) ────────────────────────
    # The runtime truth (vs. the ADR's aspirational wiring): prod runs
    # ConsoleRenderer with OTEL off, so an incident `jq`/`grep` needs to
    # know it's matching bracketed text, not JSON, and that trace fields
    # are empty.
    try:
        from backend.core.request_telemetry import effective_exporter
        from backend.utils.structlog_config import effective_format

        out["observability"] = {
            "log_format": effective_format(),
            "otel_exporter": effective_exporter(),
        }
    except Exception:
        out["observability"] = None

    # ── Config-backup freshness (SRE-11 / ADR-13 §2.1) ───────────────
    # The service-config JSON is the one piece of VM-disk state that's not
    # recoverable from FOS. The backup runs from the operator's workstation
    # → GCS, so the backend can't see GCS directly; instead it reads a
    # VM-local marker the script writes on success. None = no backup ever
    # recorded (the honest "is my only irreplaceable state captured?" answer).
    try:
        import json as _json

        from backend import config as _cfg

        marker_path = os.environ.get("CONFIG_BACKUP_MARKER") or str(
            _cfg.DATA_DIR / "system" / "last_config_backup.json"
        )
        backup: dict | None = None
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
                # Marker present but unparseable — surface "unknown" rather
                # than a false-fresh; an empty dict maps to all-None fields.
                backup = {"last_backup_at": None, "age_s": None, "source": None}
        out["config_backup"] = backup
    except Exception:
        out["config_backup"] = None

    # ── FOS reachability probe (SRE-13, opt-in) ──────────────────────
    if probe_fos:
        try:
            from backend import config as _svcconfig

            fos_out: dict = {}
            for cfg in _svcconfig.list_configs():
                sid = cfg.get("service_id") or cfg.get("name")
                try:
                    src = _svcconfig.config_to_source(cfg)
                    fos_out[sid] = _db.fos_reachable(src)
                except Exception as e:
                    fos_out[sid] = {"reachable": False, "error": str(e)[:200]}
            out["fos"] = fos_out
        except Exception:
            out["fos"] = None

    # ── High-scale disk guard (high-scale topology only) ─────────────
    # Latest serving-volume mode from the 60 s sampler. Absent in standard
    # mode so that payload stays byte-identical.
    try:
        from backend import config as _hs_cfg

        if _hs_cfg.is_high_scale_mode():
            from dataclasses import asdict

            from backend.high_scale.archive_models import (
                disk_snapshot_payload,
                topology_from_environment,
            )

            topology: dict | None
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

    return out


@router.get("/admin/vcl-health", response_model=VclHealthResponse, response_model_exclude_unset=True)
def api_vcl_health(service_id: str | None = Query(default=None)) -> dict[str, Any]:
    """Verify VCL state after migration — check for legacy/consolidated snippet presence.

    Returns a dict with:
    - legacy_snippets_found: count of pre-2.2 consolidated snippets
    - consolidated_snippets_found: count of new consolidated snippets
    - is_clean: True if no legacy snippets remain
    - recommendation: guidance for the operator

    Used by the admin UI post-deploy to show migration status.
    """
    service_id = require_service_id(service_id)
    try:
        from backend import config as svcconfig
        from backend.provision.declarative import fastly_integration

        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return {
                "service_id": service_id,
                "error": f"Service {service_id} not configured",
                "is_clean": False,
            }

        token = cfg.get("admin_token")
        if not token:
            return {
                "service_id": service_id,
                "error": "No Fastly API token in service config",
                "is_clean": False,
            }

        active_version = fastly_integration.fetch_active_version(service_id, token)
        if active_version is None:
            return {
                "service_id": service_id,
                "error": "No active version found",
                "is_clean": False,
            }

        snippets = fastly_integration.fetch_snippets(service_id, active_version, token)

        # Count legacy vs consolidated
        from backend.provision.declarative.reconciler import _is_legacy_snippet

        legacy = [s for s in snippets if _is_legacy_snippet(s.name)]
        consolidated = [s for s in snippets if s.name.startswith("Fastly Log Analytics - ")]

        is_clean = len(legacy) == 0 and len(consolidated) > 0

        return {
            "service_id": service_id,
            "active_version": active_version,
            "legacy_snippets_found": len(legacy),
            "legacy_names": [s.name for s in legacy],
            "consolidated_snippets_found": len(consolidated),
            "consolidated_names": [s.name for s in consolidated],
            "is_clean": is_clean,
            "recommendation": (
                "✓ Clean state — VCL migration complete. Ready for production."
                if is_clean
                else (
                    f"⚠ Migration in progress: {len(legacy)} legacy snippet(s) remain. "
                    "Run reconciliation to complete the migration."
                    if legacy and consolidated
                    else (
                        f"⚠ Pre-migration state: {len(legacy)} legacy snippet(s) found. "
                        "Run reconciliation to migrate to consolidated VCL."
                        if legacy
                        else "⚠ Unexpected state: no snippets found. Service may not be configured."
                    )
                )
            ),
        }
    except Exception as e:
        logger.exception("VCL health check failed")
        return {
            "service_id": service_id,
            "error": str(e),
            "is_clean": False,
        }


@router.get(
    "/admin/metric-history/batch",
    response_model=MetricHistoryBatchResponse,
    dependencies=[Depends(require_admin)],
)
def metric_history_batch(
    since: str = Query(default="1h", description="Lookback window: 30m, 1h, 6h, 24h, 7d, etc."),
) -> JSONResponse:
    """Return every recorded series newer than ``since``.

    Response shape: ``{series: {key: [{ts, value}, ...], ...}}`` where
    ``key`` is ``"metric"`` for global, ``"metric|service_id"`` for
    per-service, ``"metric|service_id|task"`` for per-task.
    """
    if not since or not _VALID_WINDOW_RE.fullmatch(since.strip()):
        raise HTTPException(
            status_code=400,
            detail=bad_request(
                f"Invalid lookback window '{since}'. Expected <N>m, <N>h, or <N>d (e.g. 30m, 1h, 6h, 24h, 7d)."
            ),
        )
    cutoff = parse_relative_time_window(since, max_lookback_days=_MAX_LOOKBACK_DAYS)
    raw = metric_snapshots.get_batch(since=cutoff)
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
) -> dict[str, Any]:
    """Trigger on-demand recycle of DuckDB instances to free parquet metadata cache."""
    rss_before = _db.current_rss_bytes()
    threshold = _recycle._recycle_rss_threshold_bytes()

    if not force and threshold > 0 and rss_before is not None and rss_before < threshold:
        mb_before = rss_before / (1024 * 1024)
        mb_thresh = threshold / (1024 * 1024)
        return {
            "ok": True,
            "status": "skipped",
            "reason": f"RSS {mb_before:.0f}MB < threshold {mb_thresh:.0f}MB",
            "rss_mb": round(mb_before, 1),
            "threshold_mb": round(mb_thresh, 1),
        }

    detail = _recycle.recycle_once(reason="manual_admin")
    rss_after = _db.current_rss_bytes()
    freed = (rss_before - rss_after) if (rss_before is not None and rss_after is not None) else None

    return {
        "ok": True,
        "status": "recycled",
        "detail": detail,
        "rss_before_mb": round(rss_before / (1024 * 1024), 1) if rss_before is not None else None,
        "rss_after_mb": round(rss_after / (1024 * 1024), 1) if rss_after is not None else None,
        "freed_mb": round(freed / (1024 * 1024), 1) if freed is not None else None,
    }


@router.get("/admin/duckdb/status", response_model=DuckdbStatusResponse, response_model_exclude_unset=True)
def get_duckdb_status_endpoint() -> dict[str, Any]:
    """Return memory metrics, recycle configuration, and active connection pool states."""
    rss = _db.current_rss_bytes()
    threshold = _recycle._recycle_rss_threshold_bytes()
    interval_min = _recycle.recycle_interval_min()
    barrier_active = _db.is_recycle_barrier_active()
    pool_stats = _pool.get_pool_status()

    return {
        "ok": True,
        "memory": {
            "current_rss_mb": round(rss / (1024 * 1024), 1) if rss is not None else None,
            "recycle_threshold_mb": round(threshold / (1024 * 1024), 1) if threshold > 0 else None,
            "recycle_interval_min": interval_min,
        },
        "barrier": {
            "is_active": bool(barrier_active),
            "active_paths": list(barrier_active) if isinstance(barrier_active, set) else [],
        },
        "pools": pool_stats["pools"],
        "retired_pools": pool_stats["retired_pools"],
    }


@router.get(
    "/admin/celery/status",
    response_model=CeleryStatusResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def api_celery_status() -> dict[str, Any]:
    """Expose Celery queue depths, worker status, APScheduler schedule, and
    ingest-ledger summary."""
    return attach_debug_db_snapshot(dict(get_celery_status()))


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
) -> dict[str, Any]:
    """List entries in ``ingest_ledger`` across services (defaults to dead_letter + quarantined)."""
    return list_ingest_ledger_entries(status=status, service_id=service_id, limit=limit, offset=offset)


@router.post(
    "/admin/celery/ledger/retry",
    response_model=IngestLedgerActionResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_retry(body: IngestLedgerActionRequest) -> dict[str, Any]:
    """Reset ``dead_letter`` / ``quarantined`` ledger entries back to ``discovered`` and
    best-effort dispatch Celery convert batches."""
    items = [item.model_dump() for item in body.items] if body.items else None
    return retry_ingest_ledger_entries(items=items, status=body.status, service_id=body.service_id)


@router.post(
    "/admin/celery/ledger/purge",
    response_model=IngestLedgerActionResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_purge(body: IngestLedgerActionRequest) -> dict[str, Any]:
    """Delete ``dead_letter`` / ``quarantined`` entries from ``ingest_ledger``."""
    items = [item.model_dump() for item in body.items] if body.items else None
    return purge_ingest_ledger_entries(items=items, status=body.status, service_id=body.service_id)


# ---------------------------------------------------------------------------
# Sync-status & log-extents surface (folded from backend.routers.admin.sync_status)
# ---------------------------------------------------------------------------


@router.get("/admin/sync-status", response_model=SyncStatusResponse)
@router.get("/sync-status", response_model=SyncStatusResponse)
def sync_status(
    service_id: str | None = Depends(get_service_id),
    skip_fos: bool = Query(default=False),
    force: bool = Query(default=False),
) -> SyncStatusResponse:
    from backend import config as svcconfig
    from backend.utils.telemetry import clear_queries

    clear_queries()

    src: dict[str, Any] | None = None
    if service_id:
        src = _db.get_source_for_service(service_id)
    if not src or not service_id:
        resp_empty: SyncStatusResponse = SyncStatusResponse.with_telemetry(configured=False)
        return resp_empty

    try:
        from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

        high_scale_service = get_high_scale_service_registry().resolve(service_id)
        if high_scale_service is not None:
            cached = compute_sync_status_cached(service_id)
            if cached is not None:
                high_scale_response: SyncStatusResponse = SyncStatusResponse.with_telemetry(**cached)
                return high_scale_response

        if skip_fos and not force:
            cached = compute_sync_status_cached(service_id)
            if cached is not None:
                resp_cached: SyncStatusResponse = SyncStatusResponse.with_telemetry(**cached)
                return resp_cached

        is_durable = svcconfig.is_durable_serving_mode(src)
        _con = _db.get_connection(source=src, max_wait=5, skip_view_update=True, read_only=is_durable)
        try:
            status = _db.get_sync_status(_con, src, skip_fos=skip_fos, force=force)
        finally:
            _con.close()

        db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
        db_exists = os.path.exists(db_path)
        db_size = os.path.getsize(db_path) if db_exists else 0

        cache_size = _get_dir_size(_db._cache_dir(src))

        status["duckdb_size_bytes"] = db_size + cache_size
        status["duckdb_exists"] = db_exists

        from backend.cron_progress import get_latest_progress_for_service

        active_run = get_latest_progress_for_service(service_id)
        if active_run:
            status["active_run"] = active_run
            status["busy"] = True

        cfg = svcconfig.load_config(service_id) or {}
        status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")

        resp: SyncStatusResponse = SyncStatusResponse.with_telemetry(**status)
        return resp
    except _db.DBBusyError as e:
        logger.warning("db_busy service_id=%s: %s", service_id, e)
        raise HTTPException(
            status_code=503,
            detail=make_error("db_busy", "Database is busy; retry shortly", busy=True),
        ) from e
    except Exception as e:
        raise_internal(logger, e, code="sync_status_failed")


@meta_router.get("/log-extents", response_model=LogExtentsResponse)
def log_extents(service_id: str | None = Depends(get_service_id)) -> LogExtentsResponse:
    """Return only the earliest/latest log timestamps for the FilterBar."""
    from backend import config as svcconfig

    if not service_id:
        empty1: LogExtentsResponse = LogExtentsResponse.with_telemetry(configured=False)
        return empty1
    src = _db.get_source_for_service(service_id)
    if not src:
        empty2: LogExtentsResponse = LogExtentsResponse.with_telemetry(configured=False)
        return empty2

    try:
        cached = compute_sync_status_cached(service_id)
        if not cached:
            cached = svcconfig.get_status(src["name"]) or {}
    except Exception:
        cached = svcconfig.get_status(src["name"]) or {}
    request_status = cached.get("request")
    latest = request_status.get("latest_log_at") if isinstance(request_status, dict) else cached.get("latest_log_at")
    resp: LogExtentsResponse = LogExtentsResponse.with_telemetry(
        configured=True,
        earliest_log_at=cached.get("earliest_log_at"),
        latest_log_at=latest,
    )
    return resp


@meta_router.get("/log-extents/stream")
async def log_extents_stream(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Analyst-safe SSE projection of the cached sync-status snapshot."""
    service_id = require_service_id(service_id)

    def _project(snap: dict[str, Any] | None) -> dict[str, Any] | None:
        if snap is None:
            return None
        return {
            "latest_log_at": snap.get("latest_log_at"),
            "local_rows": snap.get("local_rows"),
            "rum": snap.get("rum"),
            "request": snap.get("request"),
        }

    async def stream() -> AsyncIterator[str]:
        initial = _project(await asyncio.to_thread(compute_sync_status_cached, service_id))
        if initial is not None:
            yield json.dumps(initial)

        async for payload in iter_with_disconnect_ping(
            sync_status_publisher.subscribe(service_id), request, ping_seconds=15
        ):
            projected = _project(payload)
            if projected is not None:
                yield json.dumps(projected)

    return cast(EventSourceResponse, sse_response(stream()))


@router.get("/admin/ingested-files", response_model=IngestedFilesResponse)
@query_errors(status_code=500)
def ingested_files(source: dict[str, Any] = Depends(get_source)) -> IngestedFilesResponse:
    res = _db.get_ingested_files(None, source)
    response: IngestedFilesResponse = IngestedFilesResponse.with_telemetry(files=res)
    return response


# ---------------------------------------------------------------------------
# Live Query Monitor endpoints (folded from backend.routers.admin_queries)
# ---------------------------------------------------------------------------

admin_queries_router = router


def _enabled() -> bool:
    return os.environ.get("QUERY_MONITOR_ENABLED", "true").strip().lower() not in (
        "false",
        "0",
        "no",
        "off",
        "",
    )


def _ensure_enabled() -> None:
    if not _enabled():
        raise HTTPException(status_code=404, detail=not_found("query_monitor_disabled"))


_CANCEL_RATE_PER_SEC = 10
_CANCEL_WINDOW_S = 1.0
_cancel_history: dict[str, deque[float]] = {}


def _check_cancel_rate(admin_id: str) -> bool:
    now = time.monotonic()
    history = _cancel_history.setdefault(admin_id, deque(maxlen=_CANCEL_RATE_PER_SEC * 2))
    cutoff = now - _CANCEL_WINDOW_S
    while history and history[0] < cutoff:
        history.popleft()
    if len(history) >= _CANCEL_RATE_PER_SEC:
        return False
    history.append(now)
    return True


def _admin_id_from_request(request: Request) -> str:
    """Same logic as :func:`backend.core.request_context._build_attribution_from_request`
    — keep them in sync if the admin-id derivation ever moves."""
    from backend.utils.remote_access import client_ip

    return client_ip(request, default="unknown") or "admin"


@router.get(
    "/admin/queries",
    response_model=SnapshotResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def list_queries(
    since_seq: int = Query(0, ge=0),
    include_completed: bool = Query(False),
    cold_limit: int | None = Query(None, ge=1, le=400),
) -> SnapshotResponse:
    _ensure_enabled()
    snap = query_registry.snapshot(
        since_seq=since_seq,
        full_sql=False,
        include_completed=include_completed,
        all_active=True,
        cold_limit=cold_limit,
    )
    return SnapshotResponse(**snap)


@router.get(
    "/admin/queries/summary",
    response_model=SummaryResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def queries_summary() -> SummaryResponse:
    _ensure_enabled()
    return SummaryResponse(**query_registry.summary())


@router.get(
    "/admin/slow-queries/count",
    response_model=SlowQueriesCountResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def count_persisted_slow_queries(
    service_id: str = Depends(get_service_id),
    since_hours: int = Query(24, ge=1, le=24 * 30),
    threshold_ms: float = Query(1000.0, ge=0.0),
) -> dict[str, Any]:
    """Cheap row-count for the operations-overview card."""
    _ensure_enabled()
    service_id = require_service_id(service_id)
    since_utc = time.time() - since_hours * 3600
    return {
        "count": _meta_mod.count_slow_queries(service_id, since_utc=since_utc, threshold_ms=threshold_ms),
        "since_hours": since_hours,
        "threshold_ms": threshold_ms,
    }


@router.get(
    "/admin/slow-queries",
    response_model=SlowQueriesResponse,
    response_model_exclude_unset=True,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def list_persisted_slow_queries(
    service_id: str = Depends(get_service_id),
    since_hours: int = Query(24, ge=1, le=24 * 30),
    threshold_ms: float = Query(100.0, ge=0.0),
    kind: str | None = Query(None, pattern="^(analyst|admin|cron|system)$"),
    db_type: str | None = Query(None, pattern="^(DuckDB|Postgres|ClickHouse|SQLite)$"),
    sort: str = Query("recent", pattern="^(recent|duration)$"),
    limit: int = Query(200, ge=1, le=2000),
) -> dict[str, Any]:
    """Persistent slow-SQL history from the PostgreSQL ``slow_queries`` table."""
    _ensure_enabled()
    service_id = require_service_id(service_id)
    since_utc = time.time() - since_hours * 3600
    rows = _meta_mod.list_slow_queries(
        service_id,
        since_utc=since_utc,
        threshold_ms=threshold_ms,
        kind=kind,
        db_type=db_type,
        sort_by_duration=(sort == "duration"),
        limit=limit,
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "query_id": r["query_id"],
                "db_type": r["db_type"],
                "service_id": r["service_id"],
                "sql_preview": r["sql_preview"],
                "sql": r["sql_full"],
                "sql_len": r["sql_len"],
                "started_at_utc": r["started_at_utc"],
                "ended_at_utc": r["ended_at_utc"],
                "duration_ms": r["duration_ms"],
                "outcome": r["outcome"],
                "error_type": r["error_type"],
                "error_message": r["error_message"],
                "peak_memory_mb": r["peak_memory_mb"],
                "attribution": {
                    "kind": r["attr_kind"],
                    "label": r["attr_label"],
                    "principal_id": r["attr_principal_id"],
                    "caller_qualname": r["attr_caller_qualname"],
                    "caller_file": r["attr_caller_file"],
                    "request_path": r["attr_request_path"],
                    "request_id": r["attr_request_id"],
                    "cron_job": r["attr_cron_job"],
                    "cron_run_id": r["attr_cron_run_id"],
                    "pool_slot": r["attr_pool_slot"],
                },
            }
        )
    return {"rows": out, "since_hours": since_hours, "threshold_ms": threshold_ms}


@router.delete(
    "/admin/slow-queries",
    response_model=SlowQueriesClearResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def clear_persisted_slow_queries(
    request: Request,
    service_id: str = Depends(get_service_id),
) -> SlowQueriesClearResponse:
    """Clear persisted slow-SQL history for *service_id* and record an audit entry."""
    _ensure_enabled()
    service_id = require_service_id(service_id)
    deleted = _meta_mod.clear_slow_queries(service_id)
    admin_id = _admin_id_from_request(request)
    try:
        _meta_mod.record_audit(
            service_id,
            "slow_queries_cleared",
            {"deleted": deleted, "admin_id": admin_id},
            actor=admin_id,
        )
    except Exception:
        logger.warning("slow_queries_cleared audit write failed", exc_info=True)
    return SlowQueriesClearResponse(deleted=deleted, service_id=service_id)


@router.get(
    "/admin/queries/{qid}",
    response_model=ActiveQueryRow,
    response_model_exclude_unset=True,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def get_query(qid: int) -> dict[str, Any]:
    """Fetch the full SQL + attribution for a single in-flight query."""
    _ensure_enabled()
    active = query_registry.get(qid)
    if active is None:
        raise HTTPException(status_code=404, detail=not_found("query_not_found"))
    snap = query_registry.snapshot(since_seq=qid - 1, full_sql=True)
    row: dict[str, Any] | None = next((r for r in snap["active"] if r["query_id"] == qid), None)
    if row is None:
        raise HTTPException(status_code=404, detail=not_found("query_not_found"))
    return row


@router.post(
    "/admin/queries/{qid}/cancel",
    response_model=CancelResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def cancel_query(qid: int, request: Request) -> CancelResponse:
    _ensure_enabled()
    admin_id = _admin_id_from_request(request)
    if not _check_cancel_rate(admin_id):
        raise HTTPException(
            status_code=429,
            detail=make_error("cancel_rate_limit_exceeded"),
            headers={"Retry-After": "1"},
        )
    active = query_registry.get(qid)
    svc_id = active.service_id if active is not None else None
    state = query_registry.cancel_query(qid, admin_id=admin_id)
    if state == "cancelled" and svc_id:
        try:
            _meta_mod.record_audit(
                svc_id,
                "query_cancelled",
                {"query_id": qid, "admin_id": admin_id},
                actor=admin_id,
            )
        except Exception:
            logger.warning("query_cancelled audit write failed", exc_info=True)
    return CancelResponse(state=state, query_id=qid)


@router.get(
    "/admin/app-config/query-monitor",
    response_model=QueryMonitorConfigResponse,
    tags=["admin", "query-monitor"],
    dependencies=[Depends(require_admin)],
)
def query_monitor_config() -> dict[str, Any]:
    """Tiny config endpoint the frontend hits on mount to decide whether to
    render the Live Query Monitor tab."""
    return {"enabled": _enabled()}


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("routers.admin_queries._cancel_history", _cancel_history)

sys.modules["backend.system_metrics_sampler"] = sys.modules[__name__]
sys.modules["backend.routers.admin.sync_status"] = sys.modules[__name__]
sys.modules["backend.routers.admin_queries"] = sys.modules[__name__]
if "backend.routers" in sys.modules:
    setattr(sys.modules["backend.routers"], "admin_queries", sys.modules[__name__])


# ---------------------------------------------------------------------------
# Multiplexed admin SSE channel (folded from backend.routers.admin.events)
# ---------------------------------------------------------------------------

_health_mod = sys.modules[__name__]

_ORIG_COMPUTE_SYNC_STATUS_CACHED = compute_sync_status_cached
_ORIG_SAMPLE_SYSTEM_METRICS_CACHED = sample_system_metrics_cached

_ADMIN_EVENT_CHANNELS = frozenset({"sync-status", "cron-runs", "system-metrics", "share", "celery-status"})

_METRICS_SAMPLE_INTERVAL_SECONDS = 10.0
_SHARE_SAMPLE_INTERVAL_SECONDS = 10.0
_CELERY_SAMPLE_INTERVAL_SECONDS = 5.0

_FANIN_MAXSIZE = 16


def _offer(q: asyncio.Queue[dict[str, Any]], env: dict[str, Any]) -> None:
    """Non-blocking enqueue with drop-oldest."""
    if q.full():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        q.put_nowait(env)
    except asyncio.QueueFull:
        pass


async def _sync_status_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str) -> None:
    fn = (
        compute_sync_status_cached
        if compute_sync_status_cached is not _ORIG_COMPUTE_SYNC_STATUS_CACHED
        else _health_mod.compute_sync_status_cached
    )
    initial = await asyncio.to_thread(fn, service_id)
    if initial is not None:
        _offer(q, {"channel": "sync-status", "data": initial})
    async for payload in sync_status_publisher.subscribe(service_id):
        _offer(q, {"channel": "sync-status", "data": payload})


async def _cron_runs_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str) -> None:
    async for payload in cron_runs_publisher.subscribe(service_id):
        _offer(q, {"channel": "cron-runs", "data": payload})


async def _system_metrics_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str | None) -> None:
    def _resolve_sampler() -> Any:
        return (
            sample_system_metrics_cached
            if sample_system_metrics_cached is not _ORIG_SAMPLE_SYSTEM_METRICS_CACHED
            else _health_mod.sample_system_metrics_cached
        )

    initial = await _resolve_sampler()(service_id)
    _offer(q, {"channel": "system-metrics", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_METRICS_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await _resolve_sampler()(service_id)
        except Exception:
            logger.exception("system-metrics sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "system-metrics", "data": payload})
            last_payload = payload


async def _celery_status_feeder(q: asyncio.Queue[dict[str, Any]]) -> None:
    from backend.celery_status_sampler import sample_celery_status_cached

    last_payload = None
    try:
        last_payload = await sample_celery_status_cached()
        _offer(q, {"channel": "celery-status", "data": last_payload})
    except Exception:
        logger.exception("celery-status initial sample failed; will retry next tick")
    while True:
        await asyncio.sleep(_CELERY_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await sample_celery_status_cached()
        except Exception:
            logger.exception("celery-status sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "celery-status", "data": payload})
            last_payload = payload


async def _share_feeder(q: asyncio.Queue[dict[str, Any]]) -> None:
    from backend.utils.tunnel import build_share_live_payload

    initial = build_share_live_payload()
    _offer(q, {"channel": "share", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_SHARE_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = build_share_live_payload()
        except Exception:
            logger.exception("share sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "share", "data": payload})
            last_payload = payload


@router.get("/admin/events/stream")
async def admin_events_stream(
    request: Request,
    channels: str = Query(..., description="Comma-separated admin channels to multiplex."),
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Multiplex the requested admin channels onto one SSE connection."""
    requested = [c.strip() for c in channels.split(",") if c.strip()]
    if not requested:
        raise HTTPException(status_code=422, detail=make_error("channels_required"))
    unknown = sorted(c for c in requested if c not in _ADMIN_EVENT_CHANNELS)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=make_error("unknown_channel", f"unknown channel(s): {', '.join(unknown)}"),
        )
    wanted = set(requested)

    async def stream() -> AsyncIterator[str]:
        out_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_FANIN_MAXSIZE)
        feeders: list[asyncio.Task[Any]] = []
        if "system-metrics" in wanted:
            feeders.append(asyncio.ensure_future(_system_metrics_feeder(out_q, service_id)))
        if "share" in wanted:
            feeders.append(asyncio.ensure_future(_share_feeder(out_q)))
        if "celery-status" in wanted:
            feeders.append(asyncio.ensure_future(_celery_status_feeder(out_q)))
        if service_id:
            if "sync-status" in wanted:
                feeders.append(asyncio.ensure_future(_sync_status_feeder(out_q, service_id)))
            if "cron-runs" in wanted:
                feeders.append(asyncio.ensure_future(_cron_runs_feeder(out_q, service_id)))

        async def _q_iter() -> AsyncIterator[dict[str, Any]]:
            while True:
                yield await out_q.get()

        try:
            async for env in iter_with_disconnect_ping(_q_iter(), request, ping_seconds=15):
                yield json.dumps(env)
        finally:
            for t in feeders:
                t.cancel()
            await asyncio.gather(*feeders, return_exceptions=True)

    return cast(EventSourceResponse, sse_response(stream()))


sys.modules["backend.routers.admin._helpers"] = sys.modules[__name__]
sys.modules["backend.routers.admin._router"] = sys.modules[__name__]
sys.modules["backend.routers.admin.clickhouse"] = sys.modules[__name__]
sys.modules["backend.routers.admin.events"] = sys.modules[__name__]
sys.modules["backend.routers.admin.high_scale"] = sys.modules[__name__]
