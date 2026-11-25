"""Log accounting & usage reconciliation admin endpoints."""

from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, HTTPException, Query, Response

from backend.core.fastly.client import api_key_or_mock
from backend.core.request_context import get_source
from backend.high_scale.ingest_controller import get_high_scale_service_registry
from backend.models.admin import (
    LogAccountingBucket,
    LogAccountingResponse,
    LogAccountingTotals,
    SustainedLossAlert,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.bounded_cache import SWRTTLCache
from backend.utils.router_utils import raise_internal

from ._router import router

logger = logging.getLogger(__name__)


_FASTLY_COUNTS_TTL = 60.0
_FASTLY_COUNTS_STALE_TTL = 1800.0
_FASTLY_COUNTS_CACHE: SWRTTLCache = SWRTTLCache(
    maxsize=256, ttl_seconds=_FASTLY_COUNTS_TTL, stale_ttl_seconds=_FASTLY_COUNTS_STALE_TTL
)

_CLICKHOUSE_COUNTS_TTL = 60.0
_CLICKHOUSE_COUNTS_STALE_TTL = 1800.0
_CLICKHOUSE_COUNTS_CACHE: SWRTTLCache = SWRTTLCache(
    maxsize=256, ttl_seconds=_CLICKHOUSE_COUNTS_TTL, stale_ttl_seconds=_CLICKHOUSE_COUNTS_STALE_TTL
)

_LOCAL_COUNTS_TTL = 60.0
_LOCAL_COUNTS_STALE_TTL = 1800.0
_LOCAL_COUNTS_CACHE: SWRTTLCache = SWRTTLCache(
    maxsize=256, ttl_seconds=_LOCAL_COUNTS_TTL, stale_ttl_seconds=_LOCAL_COUNTS_STALE_TTL
)

_LATEST_INGEST_TTL = 60.0
_LATEST_INGEST_STALE_TTL = 1800.0
_LATEST_INGEST_CACHE: SWRTTLCache = SWRTTLCache(
    maxsize=256, ttl_seconds=_LATEST_INGEST_TTL, stale_ttl_seconds=_LATEST_INGEST_STALE_TTL
)

_swr_guard = threading.Lock()
_swr_locks: dict[tuple[int, Any], threading.Lock] = {}
_swr_bg_inflight: set[tuple[int, Any]] = set()
_NONE_SENTINEL = object()


def _swr_fetch[T](
    cache: SWRTTLCache,
    key: Any,
    fn: Callable[[], T],
    *,
    label: str,
    cache_if: Callable[[T], bool] = lambda _v: True,
) -> T:
    """Singleflight + stale-while-revalidate wrapper around an ``SWRTTLCache``."""
    cached = cache.get(key)
    if cached is not None:
        return None if cached is _NONE_SENTINEL else cached  # type: ignore[return-value]

    lock_key = (id(cache), key)
    stale = cache.get_stale(key) if isinstance(cache, SWRTTLCache) else None
    if stale is not None:
        with _swr_guard:
            already_refreshing = lock_key in _swr_bg_inflight
            if not already_refreshing:
                _swr_bg_inflight.add(lock_key)
        if not already_refreshing:

            def _refresh() -> None:
                try:
                    fresh = fn()
                    if cache_if(fresh):
                        cache[key] = _NONE_SENTINEL if fresh is None else fresh
                except Exception:  # pragma: no cover - defensive
                    logger.debug("[log-accounting] background %s refresh failed for %s", label, key, exc_info=True)
                finally:
                    try:
                        from backend.core.metadata import pg_pool

                        pg_pool.release_thread_connection()
                    except Exception:
                        pass
                    with _swr_guard:
                        _swr_bg_inflight.discard(lock_key)

            threading.Thread(target=_refresh, name=f"log-acct-swr-{label}", daemon=True).start()
        return None if stale is _NONE_SENTINEL else stale  # type: ignore[return-value]

    with _swr_guard:
        lock = _swr_locks.get(lock_key)
        if lock is None:
            lock = threading.Lock()
            _swr_locks[lock_key] = lock
    try:
        with lock:
            cached = cache.get(key)
            if cached is not None:
                return None if cached is _NONE_SENTINEL else cached  # type: ignore[return-value]
            val = fn()
            if cache_if(val):
                cache[key] = _NONE_SENTINEL if val is None else val
            return val
    finally:
        with _swr_guard:
            if not lock.locked() and _swr_locks.get(lock_key) is lock:
                _swr_locks.pop(lock_key, None)


def _fetch_fastly_request_counts(
    logging_svc_id: str, api_key: str, from_ts: int, to_ts: int, by: str, edge_only: bool = False
) -> dict[str, int]:
    """Return bucket_iso → Fastly ``requests`` (or ``edge_requests`` if edge_only) count."""
    from backend.core.fastly.client import StatsResponse, fastly_platform_metrics

    cache_key = (logging_svc_id, from_ts, to_ts, by, edge_only)

    def _load() -> dict[str, int]:
        payload = fastly_platform_metrics(
            entity_id=logging_svc_id,
            granularity=by,
            from_epoch=from_ts,
            to_epoch=to_ts,
            metrics=("requests", "edge_requests"),
            token=api_key,
        )

        width = 13 if by == "hour" else 10
        out: dict[str, int] = {}
        for r in StatsResponse.model_validate(payload).data:
            if r.start_time is None:
                continue
            bucket = datetime.fromtimestamp(r.start_time, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")[:width]
            out[bucket] = out.get(bucket, 0) + (r.edge_requests if edge_only else r.requests)
        return out

    return _swr_fetch(_FASTLY_COUNTS_CACHE, cache_key, _load, label="fastly")


LOG_ACCOUNTING_LOSS_THRESHOLD = 0.05
LOG_ACCOUNTING_MIN_RUN = 2


def _clickhouse_row_counts_per_bucket(
    source: dict[str, Any], start: datetime, end: datetime, by: str
) -> dict[str, int]:
    """Per-bucket row counts from ClickHouse ``request_facts`` (falling back to metadata)."""
    service_id = str(source.get("name") or source.get("service_id") or "")
    cache_key = (
        service_id,
        int(start.timestamp()),
        int(end.timestamp()),
        by,
    )

    def _load() -> dict[str, int] | None:
        try:
            from backend.core.clickhouse import get_clickhouse_client

            client = get_clickhouse_client()
            if client is None:
                return None
            trunc_fn = "toStartOfHour" if by == "hour" else "toStartOfDay"
            fmt = "%Y-%m-%dT%H" if by == "hour" else "%Y-%m-%d"
            sql = (
                f"SELECT formatDateTime({trunc_fn}(req_start_time, 'UTC'), '{fmt}') AS bucket, "
                "count() AS n "
                "FROM request_facts "
                "WHERE service_id = {service_id:String} "
                "  AND req_start_time >= {start:DateTime64(3, 'UTC')} "
                "  AND req_start_time < {end:DateTime64(3, 'UTC')} "
                "GROUP BY bucket"
            )
            query_fn = getattr(client, "query", None)
            if callable(query_fn):
                res = query_fn(
                    sql,
                    parameters={
                        "service_id": service_id,
                        "start": start.astimezone(UTC),
                        "end": end.astimezone(UTC),
                    },
                )
                rows = getattr(res, "result_rows", None) or []
                return {str(b): int(n) for b, n in rows}
            dict_rows = client.execute(
                sql.replace("req_start_time", "event_timestamp"),
                {
                    "service_id": service_id,
                    "start": start.astimezone(UTC),
                    "end": end.astimezone(UTC),
                },
            )
            return {str(r["bucket"]): int(r["n"]) for r in (dict_rows or []) if "bucket" in r and "n" in r}
        except Exception as e:
            logger.debug("[log-accounting] ClickHouse counts unavailable, falling back to metadata: %s", e)
            return None

    res = _swr_fetch(_CLICKHOUSE_COUNTS_CACHE, cache_key, _load, label="clickhouse", cache_if=lambda v: v is not None)
    return res if res is not None else {}


def compute_log_accounting(source: dict[str, Any], hours: int = 24, by: str = "hour") -> dict[str, Any]:
    """Pure compute path for ingest accounting (Fastly ``requests`` vs our rows)."""
    from datetime import timedelta

    from backend import config as svcconfig
    from backend.core import metadata as metadata_db
    from backend.repositories.utils.response_shaping import SectionTimer

    timer = SectionTimer()
    service_id = source.get("name", "")
    logging_svc_id = source.get("logging_service_id") or svcconfig.get_fastly_logging_service_id(service_id)
    if not logging_svc_id:
        raise HTTPException(
            status_code=400,
            detail={"error": "no logging_service_id configured for this service"},
        )
    api_key = api_key_or_mock(svcconfig.get_fastly_api_key(service_id))
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail={"error": "no fastly_api_key configured for this service"},
        )

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    if by == "day":
        now = now.replace(hour=0)
    start = now - timedelta(hours=hours)

    cfg = svcconfig.load_config(service_id) or {}
    prov = cfg.get("provisioning") or {}
    edge_only = prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False)
    edge_only = bool(edge_only)
    sample_raw = prov.get("sample_rate", cfg.get("sample_rate"))
    if sample_raw is None:
        sample_raw = 100
    try:
        sample_rate = min(100.0, max(1.0, float(sample_raw)))
    except (TypeError, ValueError):
        sample_rate = 100.0

    created_at_str = cfg.get("created_at")
    if created_at_str:
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            created_at_aligned = created_at.replace(minute=0, second=0, microsecond=0)
            if by == "day":
                created_at_aligned = created_at_aligned.replace(hour=0)
            if start < created_at_aligned:
                start = created_at_aligned
        except Exception as e:
            logger.warning("Failed to parse created_at '%s' for service %s: %s", created_at_str, service_id, e)

    from_ts = int(start.timestamp())
    to_ts = int((now + timedelta(hours=1 if by == "hour" else 24)).timestamp())

    width = 13 if by == "hour" else 10
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    end_clamp = now + timedelta(hours=1 if by == "hour" else 24)
    end_iso = end_clamp.strftime("%Y-%m-%dT%H:%M:%S")
    sql_window_pad = timedelta(hours=2)
    sql_start_iso = (start - sql_window_pad).strftime("%Y-%m-%dT%H:%M:%S")
    sql_end_iso = (end_clamp + sql_window_pad).strftime("%Y-%m-%dT%H:%M:%S")
    start_bucket = start_iso[:width]
    end_bucket = end_iso[:width]

    import contextvars
    import time as _time
    from concurrent.futures import ThreadPoolExecutor

    # High-scale ingest records request files in its source-object ledger; it never writes ``ingested_files``.
    high_scale_control = None
    if get_high_scale_service_registry().resolve(service_id) is not None:
        from backend.high_scale.postgres_control import PostgresControlPlane

        high_scale_control = PostgresControlPlane()

    def _local_counts() -> dict[str, tuple[int, int]]:
        cache_key = (
            service_id,
            high_scale_control is not None,
            int(start.timestamp()),
            int(end_clamp.timestamp()),
            width,
        )

        def _load() -> dict[str, tuple[int, int]]:
            try:
                if high_scale_control is not None:
                    return high_scale_control.request_flush_accounting(service_id, start, end_clamp, width)
                return metadata_db.get_log_accounting_counts(
                    service_id, sql_start_iso, sql_end_iso, width, start_bucket, end_bucket
                )
            finally:
                if high_scale_control is None:
                    try:
                        from backend.core.metadata import pg_pool

                        pg_pool.release_thread_connection()
                    except Exception:
                        pass

        return _swr_fetch(_LOCAL_COUNTS_CACHE, cache_key, _load, label="local")

    def _latest_ingest() -> str | None:
        cache_key = (service_id, high_scale_control is not None)

        def _load() -> str | None:
            try:
                if high_scale_control is not None:
                    latest = high_scale_control.latest_request_ingest(service_id)
                    return latest.astimezone(UTC).isoformat() if latest else None
                return metadata_db.get_latest_ingest_ts(service_id)
            finally:
                if high_scale_control is None:
                    try:
                        from backend.core.metadata import pg_pool

                        pg_pool.release_thread_connection()
                    except Exception:
                        pass

        return _swr_fetch(_LATEST_INGEST_CACHE, cache_key, _load, label="latest_ingest")

    def _run_timed(fn: Any) -> tuple[Any, float]:
        t0 = _time.perf_counter()
        res = fn()
        return res, round((_time.perf_counter() - t0) * 1000, 2)

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="log-accounting") as pool:
        fut_fastly = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            lambda: _fetch_fastly_request_counts(logging_svc_id, api_key, from_ts, to_ts, by, edge_only=edge_only),
        )
        fut_sqlite = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            _local_counts,
        )
        fut_ch = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            lambda: _clickhouse_row_counts_per_bucket(source, start, end_clamp, by),
        )
        fut_latest = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            _latest_ingest,
        )
        try:
            fastly_counts, ms_fastly = fut_fastly.result()
        except Exception as e:
            raise_internal(logger, e, code="fastly_stats_failed", status=502)
        local_counts, ms_sqlite = fut_sqlite.result()
        ch_counts, ms_ch = fut_ch.result()
        latest_ingest_str, ms_latest = fut_latest.result()

    timer.entries.append({"section": "fastly_fetch", "time_ms": ms_fastly})
    timer.entries.append({"section": "sqlite_counts", "time_ms": ms_sqlite})
    timer.entries.append({"section": "clickhouse_counts", "time_ms": ms_ch})
    timer.entries.append({"section": "latest_ingest", "time_ms": ms_latest})

    all_buckets = sorted(set(fastly_counts.keys()) | set(local_counts.keys()) | set(ch_counts.keys()))
    buckets: list[LogAccountingBucket] = []
    total_fastly = 0
    total_ours = 0
    worst_ts: str | None = None
    worst_gap_pct: float | None = None
    for b in all_buckets:
        reported_count = int(fastly_counts.get(b, 0))
        req_count = round(reported_count * sample_rate / 100.0)
        _meta_rows, fcount = local_counts.get(b, (0, 0))
        ours = int(ch_counts.get(b, _meta_rows))
        gap = req_count - ours
        denom = req_count if req_count > 0 else ours
        gap_pct = (gap / denom) if denom > 0 else 0.0
        ts_iso = f"{b}:00:00Z" if by == "hour" else f"{b}T00:00:00Z"
        buckets.append(
            LogAccountingBucket(
                ts=ts_iso,
                fastly_requests=req_count,
                our_rows=ours,
                file_count=fcount,
                gap=gap,
                gap_pct=round(gap_pct, 6),
            )
        )
        total_fastly += req_count
        total_ours += ours
        if gap_pct > (worst_gap_pct or 0.0):
            worst_ts = ts_iso
            worst_gap_pct = gap_pct

    total_gap = total_fastly - total_ours
    total_denom = total_fastly if total_fastly > 0 else total_ours
    total_pct = round((total_gap / total_denom), 6) if total_denom > 0 else 0.0
    totals = LogAccountingTotals(
        fastly_requests=total_fastly,
        our_rows=total_ours,
        gap=total_gap,
        gap_pct=total_pct,
        worst_bucket_ts=worst_ts,
        worst_bucket_gap_pct=(round(worst_gap_pct, 6) if worst_gap_pct is not None else None),
    )

    in_flight_bucket = now.strftime("%Y-%m-%dT%H") if by == "hour" else now.strftime("%Y-%m-%d")
    in_flight_ts = f"{in_flight_bucket}:00:00Z" if by == "hour" else f"{in_flight_bucket}T00:00:00Z"
    completed = [b for b in buckets if b.ts != in_flight_ts]
    sustained: SustainedLossAlert | None = None
    run_start = None
    buckets_with_sentinel: list[LogAccountingBucket | None] = list(completed) + [None]
    for i, bucket in enumerate(buckets_with_sentinel):
        is_loss = bucket is not None and bucket.gap_pct >= LOG_ACCOUNTING_LOSS_THRESHOLD
        if is_loss and run_start is None:
            run_start = i
        elif not is_loss and run_start is not None:
            run = completed[run_start:i]
            if len(run) >= LOG_ACCOUNTING_MIN_RUN and (sustained is None or len(run) > sustained.n_buckets):
                sustained = SustainedLossAlert(
                    started_at=run[0].ts,
                    n_buckets=len(run),
                    max_gap_pct=round(max(rb.gap_pct for rb in run), 6),
                    total_lost_lines=sum(rb.gap for rb in run if rb.gap > 0),
                )
            run_start = None

    catchup: dict[str, Any] | None
    if latest_ingest_str:
        latest_dt = datetime.fromisoformat(latest_ingest_str.replace(" ", "T")).replace(tzinfo=UTC)
        lag_seconds = max(0, int((datetime.now(UTC) - latest_dt).total_seconds()))
        if lag_seconds <= 300:
            status_str = "caught_up"
        elif lag_seconds <= 3600:
            status_str = "backfilling"
        else:
            status_str = "stalled"
        catchup = {
            "latest_ingest_ts": latest_dt.isoformat().replace("+00:00", "Z"),
            "lag_seconds": lag_seconds,
            "status": status_str,
        }
    else:
        catchup = {"latest_ingest_ts": None, "lag_seconds": None, "status": "no_data"}

    return {
        "by": by,
        "from_ts": start_iso + "Z",
        "to_ts": end_iso + "Z",
        "buckets": buckets,
        "totals": totals,
        "sustained_loss": sustained,
        "catchup": catchup,
        "section_timings": timer.entries,
    }


_ORIG_COMPUTE_LOG_ACCOUNTING = compute_log_accounting


def _resolve_compute_log_accounting() -> Any:
    if compute_log_accounting is not _ORIG_COMPUTE_LOG_ACCOUNTING:
        return compute_log_accounting
    admin_pkg = sys.modules.get("backend.routers.admin")
    if admin_pkg is not None:
        pkg_fn = getattr(admin_pkg, "compute_log_accounting", None)
        if pkg_fn is not None and pkg_fn is not _ORIG_COMPUTE_LOG_ACCOUNTING:
            return pkg_fn
    return compute_log_accounting


@router.get("/admin/log-accounting", response_model=LogAccountingResponse)
@router.get("/admin/log-accounting/{service_id}", response_model=LogAccountingResponse)
def api_log_accounting(
    response: Response,
    service_id: str | None = None,
    source: dict[str, Any] = Depends(get_source),
    hours: int = Query(24, ge=1, le=720),
    by: str = Query("hour", pattern="^(hour|day)$"),
) -> LogAccountingResponse:
    """Reconcile Fastly's authoritative ``requests`` count against our
    locally-ingested row counts to surface any gap between what Fastly served
    and what we ingested.
    """
    compute_fn = _resolve_compute_log_accounting()
    result = compute_fn(source, hours=hours, by=by)
    payload: LogAccountingResponse = LogAccountingResponse.with_telemetry(**result)
    response.headers["Cache-Control"] = "private, max-age=30"
    response.headers["Vary"] = "x-service-id"
    return payload


def prewarm_log_accounting(service_id: str) -> None:
    """Best-effort startup pre-warm of 24h hourly log accounting caches."""
    try:
        from backend.core import fos_client as _fos

        src = _fos.get_source_for_service(service_id)
        if not src:
            return
        compute_log_accounting(src, hours=24, by="hour")
    except Exception:
        logger.debug("[log-accounting] pre-warm skipped for %s", service_id, exc_info=True)
    finally:
        try:
            from backend.core.metadata import pg_pool

            pg_pool.release_thread_connection()
        except Exception:
            pass


_CacheRegistry.register("routers.admin.log_accounting._FASTLY_COUNTS_CACHE", _FASTLY_COUNTS_CACHE)
_CacheRegistry.register("routers.admin.log_accounting._CLICKHOUSE_COUNTS_CACHE", _CLICKHOUSE_COUNTS_CACHE)
_CacheRegistry.register("routers.admin.log_accounting._LOCAL_COUNTS_CACHE", _LOCAL_COUNTS_CACHE)
_CacheRegistry.register("routers.admin.log_accounting._LATEST_INGEST_CACHE", _LATEST_INGEST_CACHE)
