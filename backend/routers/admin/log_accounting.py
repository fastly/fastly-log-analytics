"""Log accounting & usage reconciliation admin endpoints."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, HTTPException, Query, Response

from backend.core.fastly.client import api_key_or_mock
from backend.core.request_context import get_source
from backend.models.admin import (
    LogAccountingBucket,
    LogAccountingResponse,
    LogAccountingTotals,
    SustainedLossAlert,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.router_utils import raise_internal

from ._router import router
from .health import (  # noqa: F401
    ACTIVE_HOUR_BUCKET_COUNTS,
    RAW_BUCKET_COUNTS,
    ROLLUP_BUCKET_COUNTS,
    active_hour_bucket_counts,
    raw_bucket_counts,
    rollup_bucket_counts,
)

logger = logging.getLogger(__name__)


_FASTLY_COUNTS_TTL = 45.0
_FASTLY_COUNTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_FASTLY_COUNTS_TTL)

_DUCKDB_COUNTS_TTL = 45.0
_DUCKDB_COUNTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_DUCKDB_COUNTS_TTL)


def _fetch_fastly_request_counts(
    logging_svc_id: str, api_key: str, from_ts: int, to_ts: int, by: str, edge_only: bool = False
) -> dict[str, int]:
    """Return bucket_iso → Fastly ``requests`` (or ``edge_requests`` if edge_only) count."""
    from backend.core.fastly.client import StatsResponse, fastly

    cache_key = (logging_svc_id, from_ts, to_ts, by, edge_only)
    cached: dict[str, int] | None = _FASTLY_COUNTS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    payload = fastly(
        "GET",
        f"/stats/service/{logging_svc_id}?by={by}&from={from_ts}&to={to_ts}",
        token=api_key,
    )

    width = 13 if by == "hour" else 10
    out: dict[str, int] = {}
    for r in StatsResponse.model_validate(payload).data:
        if r.start_time is None:
            continue
        bucket = datetime.fromtimestamp(r.start_time, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")[:width]
        out[bucket] = out.get(bucket, 0) + ((r.edge_requests if edge_only else r.requests) or r.requests)
    _FASTLY_COUNTS_CACHE[cache_key] = out
    return out


LOG_ACCOUNTING_LOSS_THRESHOLD = 0.05
LOG_ACCOUNTING_MIN_RUN = 2


def _duckdb_row_counts_per_bucket(source: dict[str, Any], start: datetime, end: datetime, by: str) -> dict[str, int]:
    """Per-bucket ``COUNT(*)`` from the live DuckDB view."""
    cache_key = (
        source.get("name", ""),
        int(start.timestamp()),
        int(end.timestamp()),
        by,
    )
    cached: dict[str, int] | None = _DUCKDB_COUNTS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    fmt = "%Y-%m-%dT%H" if by == "hour" else "%Y-%m-%d"
    try:
        result = _try_row_counts_from_rollup(source, start, end, fmt)
        if result is None:
            result = _raw_row_counts(source, start, end, fmt)
        _DUCKDB_COUNTS_CACHE[cache_key] = result
        return result
    except Exception as e:
        logger.warning("[log-accounting] DuckDB counts unavailable, falling back to metadata: %s", e)
        return {}


def _try_row_counts_from_rollup(
    source: dict[str, Any],
    start: datetime,
    end: datetime,
    fmt: str,
) -> dict[str, int] | None:
    """Read hourly request counts from the overview rollup parquet."""
    import os

    from backend.core.request_context import _ConnectionHolder
    from backend.core.rollups import OVERVIEW_BUNDLE_FILENAME, _hour_bundled_root
    from backend.core.rollups.rollup_coverage import collect_hourly_bundle_paths

    bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root):
        return None

    collected = collect_hourly_bundle_paths(
        source,
        start,
        end,
        bundled_root,
        OVERVIEW_BUNDLE_FILENAME,
    )
    if collected is None:
        return None
    rollup_paths, crosses_active = collected
    if not rollup_paths and not crosses_active:
        return None

    result: dict[str, int] = {}
    unit = "day" if "%H" not in fmt else "hour"

    if rollup_paths:
        from backend.repositories._base import quote_path_list

        paths_sql = quote_path_list(rollup_paths)
        st_tz = start.astimezone(UTC).isoformat()
        et_tz = end.astimezone(UTC).isoformat()
        with _ConnectionHolder(source, read_only=True) as con:
            rows = rollup_bucket_counts(con, unit=unit, fmt=fmt, paths_sql=paths_sql, start_tz=st_tz, end_tz=et_tz)
        for b, n in rows:
            result[b] = int(n)

    if crosses_active:
        import glob
        from datetime import timedelta

        from backend.core import duckdb as _ddb
        from backend.core.duckdb import escape_sql_literal
        from backend.core.storage._core import buffer_files

        active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(start, active_hour_dt)
        live_start_iso = live_start.strftime("%Y-%m-%d %H:%M:%S")
        end_iso = end.strftime("%Y-%m-%d %H:%M:%S")

        hour_strs: set[str] = set()
        cursor = live_start.replace(minute=0, second=0, microsecond=0)
        while cursor < end:
            hour_strs.add(cursor.strftime("%Y-%m-%d-%H"))
            cursor += timedelta(hours=1)

        cache_dir = _ddb._cache_dir(source)
        committed_files: list[str] = []
        for hour_str in hour_strs:
            active_hour_glob = os.path.join(cache_dir, "data", f"timestamp_hour={hour_str}", "**", "*.parquet")
            committed_files.extend(p for p in glob.glob(active_hour_glob, recursive=True) if os.path.isfile(p))
        buf_files = buffer_files(source)
        active_paths = committed_files + buf_files

        if active_paths:
            paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in active_paths)
            with _ConnectionHolder(source, read_only=True) as con:
                rows = active_hour_bucket_counts(
                    con, unit=unit, fmt=fmt, paths_sql=paths_sql, start_iso=live_start_iso, end_iso=end_iso
                )
            for b, n in rows:
                result[b] = result.get(b, 0) + int(n)

    return result


def _raw_row_counts(
    source: dict[str, Any],
    start: datetime,
    end: datetime,
    fmt: str,
) -> dict[str, int]:
    """Full raw-parquet scan fallback for row counts."""
    from backend.core import duckdb as _ddb
    from backend.core.request_context import _ConnectionHolder

    table_name = _ddb._safe_table_name(source["name"])
    start_iso = start.strftime("%Y-%m-%d %H:%M:%S")
    end_iso = end.strftime("%Y-%m-%d %H:%M:%S")
    unit = "day" if "%H" not in fmt else "hour"
    with _ConnectionHolder(source, read_only=True) as con:
        rows = raw_bucket_counts(con, unit=unit, fmt=fmt, table_name=table_name, start_iso=start_iso, end_iso=end_iso)
    return {b: int(n) for b, n in rows}


def compute_log_accounting(source: dict[str, Any], hours: int = 24, by: str = "hour") -> dict[str, Any]:
    """Pure compute path for ingest accounting (Fastly ``requests`` vs our rows)."""
    from datetime import timedelta

    from backend import config as svcconfig
    from backend.core import metadata as metadata_db
    from backend.repositories._base import SectionTimer

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

    def _run_timed(fn: Any) -> tuple[Any, float]:
        t0 = _time.perf_counter()
        res = fn()
        return res, round((_time.perf_counter() - t0) * 1000, 2)

    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_fastly = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            lambda: _fetch_fastly_request_counts(logging_svc_id, api_key, from_ts, to_ts, by, edge_only=edge_only),
        )
        fut_sqlite = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            lambda: metadata_db.get_log_accounting_counts(
                service_id, sql_start_iso, sql_end_iso, width, start_bucket, end_bucket
            ),
        )
        fut_duckdb = pool.submit(
            contextvars.copy_context().run,
            _run_timed,
            lambda: _duckdb_row_counts_per_bucket(source, start, end_clamp, by),
        )
        try:
            fastly_counts, ms_fastly = fut_fastly.result()
        except Exception as e:
            raise_internal(logger, e, code="fastly_stats_failed", status=502)
        local_counts, ms_sqlite = fut_sqlite.result()
        duckdb_counts, ms_duckdb = fut_duckdb.result()

    timer.entries.append({"section": "fastly_fetch", "time_ms": ms_fastly})
    timer.entries.append({"section": "sqlite_counts", "time_ms": ms_sqlite})
    timer.entries.append({"section": "duckdb_counts", "time_ms": ms_duckdb})

    all_buckets = sorted(set(fastly_counts.keys()) | set(local_counts.keys()) | set(duckdb_counts.keys()))
    buckets: list[LogAccountingBucket] = []
    total_fastly = 0
    total_ours = 0
    worst_ts: str | None = None
    worst_gap_pct: float | None = None
    for b in all_buckets:
        req_count = int(fastly_counts.get(b, 0))
        _meta_rows, fcount = local_counts.get(b, (0, 0))
        ours = int(duckdb_counts.get(b, _meta_rows))
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

    latest_ingest_str = timer.call(
        "latest_ingest",
        lambda: metadata_db.get_latest_ingest_ts(service_id),
    )
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


_CacheRegistry.register("routers.admin.log_accounting._FASTLY_COUNTS_CACHE", _FASTLY_COUNTS_CACHE)
_CacheRegistry.register("routers.admin.log_accounting._DUCKDB_COUNTS_CACHE", _DUCKDB_COUNTS_CACHE)
