"""Log-accounting: Fastly Stats vs locally-ingested counts + backfill.

Hosts the sustained-loss thresholds referenced by both the UI callout
and the gap-heal cron in scheduler.py — see those module's imports.
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from backend.config import service_id_from_source
from backend.core.fastly.client import api_key_or_mock
from backend.deps import get_source, require_admin
from backend.models.admin import (
    LogAccountingBucket,
    LogAccountingResponse,
    LogAccountingTotals,
    SustainedLossAlert,
    SystemJobsResponse,
    UsageLogAggregate,
    UsageLogEntry,
    UsageLoggingConfigResponse,
    UsageLoggingUpdateBody,
    UsageLogResponse,
)
from backend.models.admin_actions import BackfillWindowResponse
from backend.models.common import OkResponse
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.router_utils import raise_internal
from backend.utils.telemetry import track_query

from .health import router

logger = logging.getLogger(__name__)

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
    q = ROLLUP_BUCKET_COUNTS.format(unit=unit, fmt=fmt, paths_sql=paths_sql, start_tz=start_tz, end_tz=end_tz)
    with track_query(con, q, [], "log_accounting.rollup_bucket_counts") as cur:
        return list(cur.fetchall())


def active_hour_bucket_counts(
    con: Any, *, unit: str, fmt: str, paths_sql: str, start_iso: str, end_iso: str
) -> list[tuple[Any, ...]]:
    """Row counts per bucket straight from the active-hour committed + buffer parquet files."""
    q = ACTIVE_HOUR_BUCKET_COUNTS.format(unit=unit, fmt=fmt, paths_sql=paths_sql, start_iso=start_iso, end_iso=end_iso)
    with track_query(con, q, [], "log_accounting.active_hour_bucket_counts") as cur:
        return list(cur.fetchall())


def raw_bucket_counts(
    con: Any, *, unit: str, fmt: str, table_name: str, start_iso: str, end_iso: str
) -> list[tuple[Any, ...]]:
    """Full raw-table scan fallback for row counts per bucket."""
    q = RAW_BUCKET_COUNTS.format(unit=unit, fmt=fmt, table_name=table_name, start_iso=start_iso, end_iso=end_iso)
    with track_query(con, q, [], "log_accounting.raw_bucket_counts") as cur:
        return list(cur.fetchall())


# Short-TTL memo for the Fastly Stats API fetch (the dominant cost
# inside compute_log_accounting: ~1.8 s p95). Key includes the (already
# hour-aligned) from_ts/to_ts so different windows don't collide; the
# admin UsageChart polls at 60 s and the React Query layer staleTime is
# 30 s, so a 30 s server-side TTL is well inside any user-visible
# staleness budget and removes the spinner feel.
_FASTLY_COUNTS_TTL = 45.0
_FASTLY_COUNTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_FASTLY_COUNTS_TTL)

# Same TTL on the per-bucket DuckDB COUNT(*) since the function arguments
# are functions of (service name, window, by) and the answer is stable
# for the same input within the TTL window. Keyed on the same shape so a
# single round of clears would invalidate both halves of the response.
_DUCKDB_COUNTS_TTL = 45.0
_DUCKDB_COUNTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_DUCKDB_COUNTS_TTL)


@router.post("/admin/backfill-window", response_model=BackfillWindowResponse, response_model_exclude_unset=True)
def backfill_window(
    start_time: str = Query(..., description="ISO 8601 UTC start, e.g. '2026-05-31T23:00:00Z'"),
    end_time: str = Query(..., description="ISO 8601 UTC end, e.g. '2026-06-01T01:00:00Z'"),
    source: dict = Depends(get_source),
) -> dict:
    """Force-sync a specific time window from FOS into local cache.

    Use to fill gaps left by ingestion outages (the normal cron pulls
    'since last sync' and won't reach back past its pointer once recovered).
    Idempotent — files already present in the local cache are skipped.
    """
    from backend.core import iceberg as _ice

    return _ice.sync_data(source, start_time=start_time, end_time=end_time)


def _fetch_fastly_request_counts(
    logging_svc_id: str, api_key: str, from_ts: int, to_ts: int, by: str, edge_only: bool = False
) -> dict[str, int]:
    """Return bucket_iso → Fastly ``requests`` (or ``edge_requests`` if edge_only) count.

    ``requests`` (or ``edge_requests``) is the loss denominator: it sits 1:1 with
    our ingested rows (one S3 log line per real client request), so ``requests − our_rows``
    is the honest gap. We deliberately do NOT use Fastly's ``log`` stat — it
    counts ``vcl_log`` RE-EXECUTIONS (bot-challenge / restart paths re-run
    ``vcl_recv``), so it reads 2.7–3.8× ``requests`` on some services and
    produces a permanent phantom gap that is not data loss.

    If edge_only is True, we use ``edge_requests`` to avoid phantom gaps from
    Origin Shielding where forwarded requests are double-counted in aggregate stats.

    Bucket key is the UTC ISO string at the same width the local SQL bucket
    uses (`YYYY-MM-DDTHH` for hour, `YYYY-MM-DD` for day) so the outer-join
    in api_log_accounting can key on string equality directly.

    Memoised for ``_FASTLY_COUNTS_TTL`` s on
    ``(logging_svc_id, from_ts, to_ts, by, edge_only)``. Inputs are hour-aligned, so
    repeats from the admin poll loop (every 30-60 s) hit cache.
    """
    from datetime import UTC, datetime

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


# Sustained-loss thresholds — referenced by both api_log_accounting (so the
# UI callout matches the heal trigger) and the gap-heal cron in scheduler.py.
LOG_ACCOUNTING_LOSS_THRESHOLD = 0.05
LOG_ACCOUNTING_MIN_RUN = 2


def _duckdb_row_counts_per_bucket(source: dict, start: datetime, end: datetime, by: str) -> dict[str, int]:
    """Per-bucket ``COUNT(*)`` from the live DuckDB view — the post-dedup
    truth that should drive the log-accounting comparison.

    Returns ``{bucket_string: count}`` where bucket_string matches the
    SQLite metadata path's format (``YYYY-MM-DDTHH`` for hourly,
    ``YYYY-MM-DD`` for daily) so the loop above can union the keys.

    Prefers reading from the overview rollup parquet (one tiny file per
    closed hour) and only falls back to scanning raw parquet for the
    in-flight hour. Errors collapse to an empty dict so the route still
    degrades to the metadata-only path rather than 500ing.
    """

    cache_key = (
        source.get("name", ""),
        int(start.timestamp()),
        int(end.timestamp()),
        by,
    )
    cached: dict[str, int] | None = _DUCKDB_COUNTS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # Bucket key MUST match metadata_db.get_log_accounting_counts: hourly
    # uses ``YYYY-MM-DDTHH`` (T separator, from the .log.gz basename's
    # ISO prefix); daily uses ``YYYY-MM-DD``. Mismatch here makes the
    # union-by-key loop in compute_log_accounting produce ghost buckets
    # with our_rows but zero fastly_requests.
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
    source: dict,
    start: datetime,
    end: datetime,
    fmt: str,
) -> dict[str, int] | None:
    """Read hourly request counts from the overview rollup parquet.

    Returns ``None`` when the rollup path isn't viable (missing files for
    a closed hour that has data), causing the caller to fall back to raw.
    """
    import os

    from backend.core.rollups import OVERVIEW_BUNDLE_FILENAME, _hour_bundled_root
    from backend.deps import _ConnectionHolder
    from backend.repositories.rollup_coverage import collect_hourly_bundle_paths

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
        from backend.core.iceberg.buffer import buffer_files

        active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_start = max(start, active_hour_dt)
        live_start_iso = live_start.strftime("%Y-%m-%d %H:%M:%S")
        end_iso = end.strftime("%Y-%m-%d %H:%M:%S")

        # Gather YYYY-MM-DD-HH string(s) crossing the active query range
        hour_strs: set[str] = set()
        cursor = live_start.replace(minute=0, second=0, microsecond=0)
        while cursor < end:
            hour_strs.add(cursor.strftime("%Y-%m-%d-%H"))
            cursor += timedelta(hours=1)

        # Locate committed partition subdirectory files & active uncommitted buffer files
        cache_dir = _ddb._cache_dir(source)
        committed_files: list[str] = []
        for hour_str in hour_strs:
            active_hour_glob = os.path.join(cache_dir, "data", f"timestamp_hour={hour_str}", "**", "*.parquet")
            committed_files.extend(p for p in glob.glob(active_hour_glob, recursive=True) if os.path.isfile(p))
        buf_files = buffer_files(source)
        active_paths = committed_files + buf_files

        # If there are no files for the active hour, the row count is exactly 0
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
    source: dict,
    start: datetime,
    end: datetime,
    fmt: str,
) -> dict[str, int]:
    """Full raw-parquet scan fallback for row counts."""
    from backend.core import duckdb as _ddb
    from backend.deps import _ConnectionHolder

    table_name = _ddb._safe_table_name(source["name"])
    start_iso = start.strftime("%Y-%m-%d %H:%M:%S")
    end_iso = end.strftime("%Y-%m-%d %H:%M:%S")
    unit = "day" if "%H" not in fmt else "hour"
    with _ConnectionHolder(source, read_only=True) as con:
        rows = raw_bucket_counts(con, unit=unit, fmt=fmt, table_name=table_name, start_iso=start_iso, end_iso=end_iso)
    return {b: int(n) for b, n in rows}


def compute_log_accounting(source: dict, hours: int = 24, by: str = "hour") -> dict:
    """Pure compute path for ingest accounting (Fastly ``requests`` vs our rows).

    Returns a dict with all the fields api_log_accounting surfaces:
    ``buckets``, ``totals``, ``sustained_loss``, ``from_ts``, ``to_ts``, plus a
    ``section_timings`` list mirroring the SectionTimer pattern used elsewhere
    (the perf harness reads this via the BaseResponse envelope to attribute the
    ~800-1700 ms tail this endpoint shows on cold loads).

    The gap is measured against Fastly's ``requests`` counter, which sits 1:1
    with our ingested rows on every service — NOT the ``log`` counter, which
    counts ``vcl_log`` re-executions and reads a permanent multiple of
    ``requests`` on restart/bot-challenge paths (a phantom gap, not loss).

    Raises HTTPException on configuration error (no logging_service_id /
    no api_key) or on Fastly Stats API failure.

    Extracted so the gap-heal cron can reuse the same Fastly fetch + SQL +
    sustained-loss detection without duplicating the math — drift between
    the two would mean the heal trigger and the UI callout disagree.
    """
    from datetime import UTC, datetime, timedelta

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
            # Align created_at to bucket start boundary
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
    # Upper bound spans the END of the current (in-flight) bucket so newly
    # ingested files in that bucket are included — same span as the Fastly
    # request. Without this, an hour-aligned clamp drops every file ingested
    # after :00 and the latest bucket shows our_rows=0.
    end_clamp = now + timedelta(hours=1 if by == "hour" else 24)
    end_iso = end_clamp.strftime("%Y-%m-%dT%H:%M:%S")
    # We bucket by emission time (from the filename) but the SQL window is on
    # ingested_at, so widen it ±2h to catch files emitted near the window
    # boundary but ingested outside it. Python-side filter trims to the
    # requested emission window afterwards.
    sql_window_pad = timedelta(hours=2)
    sql_start_iso = (start - sql_window_pad).strftime("%Y-%m-%dT%H:%M:%S")
    sql_end_iso = (end_clamp + sql_window_pad).strftime("%Y-%m-%dT%H:%M:%S")
    # ingested_at is stored with a space separator (datetime('now')) while
    # start/end are ISO-T strings, so a raw string comparison silently
    # filters out everything — wrap both sides with datetime() to compare
    # as actual timestamps. See memory: usage_log timestamp formats.
    # Bucket by emission time parsed from the filename (falls back to
    # ingested_at for legacy/test files without an ISO prefix).
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
        # Prefer DuckDB's authoritative live count; fall back to metadata
        # only when DuckDB has no entry (very old buckets that aged out of
        # the local cache but still have an ingested_files row).
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
        # Rank by positive gap only — negative gaps are bucket-edge drift
        # where a request/ingest straddled the boundary. The user cares about
        # "Fastly served more requests than we ingested rows" (real loss), not
        # "we ingested more than Fastly reports yet" (timing artifact).
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

    # Sustained-loss detection: only flag runs of ≥MIN_RUN consecutive completed
    # buckets with one-sided positive gap ≥LOSS_THRESHOLD (Fastly served more
    # requests than we ingested rows). Bucket-edge drift is bidirectional and
    # stays under 2.5%; the in-flight bucket is noisy because Fastly Stats lags
    # our ingest, so we exclude it from the scan. Returns the longest qualifying run.
    in_flight_bucket = now.strftime("%Y-%m-%dT%H") if by == "hour" else now.strftime("%Y-%m-%d")
    in_flight_ts = f"{in_flight_bucket}:00:00Z" if by == "hour" else f"{in_flight_bucket}T00:00:00Z"
    completed = [b for b in buckets if b.ts != in_flight_ts]
    sustained: SustainedLossAlert | None = None
    run_start = None
    # Rename loop var to dodge the str-binding from the earlier
    # ``for b in time_buckets`` chunk in this same function — mypy
    # carries the first binding's type into the second loop.
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

    # Catch-up indicator: derived from the most recent successful ingest
    # (max(ingested_at) on ingested_files). Lag = now - that. The status
    # thresholds match the Fastly delivery promise — typical drop interval
    # is 60s, so >300s lag means we're at least 5 cycles behind. Stalled
    # means >1h (the operator should look at it).
    latest_ingest_str = timer.call(
        "latest_ingest",
        lambda: metadata_db.get_latest_ingest_ts(service_id),
    )
    catchup: dict | None
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

    # No multi-endpoint guard needed: the gap is measured against ``requests``,
    # which is endpoint-independent (it's the count of real client requests,
    # 1:1 with our ingested rows) — unlike the old ``log`` denominator, which
    # summed emissions across every logging endpoint. ``sustained_loss`` here is
    # the single source of truth; the gap-heal cron reads it too.
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


@router.get("/admin/log-accounting", response_model=LogAccountingResponse)
@router.get("/admin/log-accounting/{service_id}", response_model=LogAccountingResponse)
def api_log_accounting(
    response: Response,
    service_id: str | None = None,
    source: dict = Depends(get_source),
    hours: int = Query(24, ge=1, le=720),
    by: str = Query("hour", pattern="^(hour|day)$"),
) -> LogAccountingResponse:
    """Reconcile Fastly's authoritative ``requests`` count against our
    locally-ingested row counts to surface any gap between what Fastly served
    and what we ingested.

    Per-bucket gap is the actionable signal — totals smooth over burst losses.
    """
    result = compute_log_accounting(source, hours=hours, by=by)
    payload: LogAccountingResponse = LogAccountingResponse.with_telemetry(**result)
    # 30 s edge cache aligns with both the backend compute_log_accounting
    # TTL and the frontend React Query staleTime — short-circuits the
    # poll round-trip on each paint after the first within the window.
    # Manual refresh / refetch bypasses HTTP cache via key bumps.
    response.headers["Cache-Control"] = "private, max-age=30"
    # The service is selected via the ``x-service-id`` REQUEST HEADER, not the
    # URL — so without varying on it the browser would serve one service's
    # cached body for another within the 30 s window (every service hits the
    # identical URL). Vary on the header so the cache keys per service; this is
    # what makes the gap card actually change when you switch services. The
    # gzip middleware appends ``Accept-Encoding`` to this on the way out.
    response.headers["Vary"] = "x-service-id"
    return payload


# R-1: drain the two log-accounting TTL caches between tests so a prior
# test's Fastly Stats response doesn't bleed into the next test.
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("routers.admin.log_accounting._FASTLY_COUNTS_CACHE", _FASTLY_COUNTS_CACHE)
_CacheRegistry.register("routers.admin.log_accounting._DUCKDB_COUNTS_CACHE", _DUCKDB_COUNTS_CACHE)


# ── Admin usage-logging + system-jobs endpoints (folded from admin_usage.py) ──


@router.get(
    "/admin/usage-logging",
    response_model=UsageLoggingConfigResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def get_usage_logging_settings() -> dict[str, Any]:
    """Return the usage logging config (global defaults)."""
    from backend import config as svcconfig

    return svcconfig.load_usage_logging_config()


@router.patch(
    "/admin/usage-logging",
    response_model=UsageLoggingConfigResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def update_usage_logging_settings(body: UsageLoggingUpdateBody) -> dict[str, Any]:
    """Update the global usage logging config."""
    from backend import config as svcconfig

    # exclude_unset preserves the partial-update contract: only fields
    # explicitly present in the request body are applied, so callers can
    # PATCH a single rate without nulling the rest.
    updates = body.model_dump(exclude_unset=True)

    # N-9: reject empty / non-positive numeric writes so an admin who hits
    # Save with empty inputs doesn't silently wipe the global rates to 0
    # (which would zero out cost estimates across every service). Boolean
    # toggles (enabled, track_duckdb_httpfs) are allowed through unchanged.
    numeric_fields = (
        "retention_days",
        "class_a_rate_per_1k",
        "class_b_rate_per_10k",
        "cdn_egress_rate_per_gb",
        "storage_rate_per_gb_month",
        "min_billed_days",
    )
    for fld in numeric_fields:
        if fld not in updates:
            continue
        try:
            n = float(updates[fld])
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail={"error": f"{fld} must be a positive number", "field": fld},
            )
        if not (n > 0):
            raise HTTPException(
                status_code=400,
                detail={"error": f"{fld} must be a positive number", "field": fld},
            )

    if "billing_zone" in updates and updates["billing_zone"] is not None:
        zone = str(updates["billing_zone"])
        allowed_zones = ("MBZ100", "MBZ200", "Global")
        if zone not in allowed_zones:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": f"Invalid billing_zone '{zone}'. Must be one of: {', '.join(allowed_zones)}",
                    "field": "billing_zone",
                },
            )
        if "cdn_egress_rate_per_gb" not in updates:
            zone_rates = {"MBZ100": 0.08, "MBZ200": 0.12, "Global": 0.18}
            updates["cdn_egress_rate_per_gb"] = zone_rates[zone]

    current = svcconfig.load_usage_logging_config()
    current.update(updates)
    svcconfig.save_usage_logging_config(current)
    return current


@router.get(
    "/admin/usage-log",
    response_model=UsageLogResponse,
    dependencies=[Depends(require_admin)],
)
def usage_log_endpoint(
    source: dict = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    usage_type: str = Query(default=""),
    process_context: str = Query(default=""),
    operation_type: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
) -> Any:
    """Return paginated _usage_log entries with aggregates for cost analysis from metadata_db (SQLite)."""
    from backend import config as svcconfig
    from backend.core import metadata as metadata_db
    from backend.utils.date_utils import parse_date_window

    ul_cfg = svcconfig.load_usage_logging_config()
    rate_a = float(ul_cfg.get("class_a_rate_per_1k", 0.005))
    rate_b = float(ul_cfg.get("class_b_rate_per_10k", 0.01))
    rate_cdn = float(ul_cfg.get("cdn_egress_rate_per_gb", 0.12))

    start_str, end_str = parse_date_window(start, end)
    service_id = service_id_from_source(source, prefer_name=True)

    rows, total, agg_data = metadata_db.get_usage_logs(
        service_id=service_id,
        start=start_str,
        end=end_str,
        usage_type=usage_type,
        process_context=process_context,
        operation_type=operation_type,
        page=page,
        page_size=page_size,
    )

    total_a = agg_data["total_class_a"]
    total_b = agg_data["total_class_b"]
    total_cdn = agg_data["total_cdn_downloads"]
    cdn_bytes = agg_data["total_cdn_bytes"]
    fos_bytes = agg_data["total_fos_bytes"]

    cost_a = (total_a / 1000) * rate_a
    cost_b = (total_b / 10000) * rate_b
    cost_cdn = (cdn_bytes / (1024**3)) * rate_cdn

    entries = []
    for r in rows:
        op_class = r["operation_class"]
        # `count` is 1 for observed proxy rows and N for reconciliation rows
        # written by fastly.reconciliation (one compact row per (hour, class)
        # gap vs Fastly's /stats/aggregate). The displayed estimated_cost has
        # to scale with N so the per-row cost matches the aggregate totals.
        op_count = int(r["count"] or 1) if "count" in r.keys() else 1
        b = r["bytes"]
        if op_class == "A":
            ec = (op_count / 1000) * rate_a
        elif op_class == "B":
            ec = (op_count / 10000) * rate_b
        elif op_class == "CDN":
            ec = ((b or 0) / (1024**3)) * rate_cdn
        else:
            ec = None

        entries.append(
            UsageLogEntry(
                id=int(r.get("id") or 0),
                timestamp=str(r["timestamp"]),
                operation_class=r["operation_class"],
                operation_type=r["operation_type"],
                url=r["url"],
                bytes=r["bytes"],
                duration_ms=r["duration_ms"],
                function_name=r["function_name"],
                process_context=r["process_context"],
                status=r["status"],
                estimated_cost=round(ec, 8) if ec is not None else None,
                count=op_count,
            )
        )

    aggregate = UsageLogAggregate(
        total_class_a=total_a,
        total_class_b=total_b,
        total_cdn_downloads=total_cdn,
        total_cdn_bytes=cdn_bytes,
        total_fos_bytes=fos_bytes,
        estimated_cost_class_a=round(cost_a, 6),
        estimated_cost_class_b=round(cost_b, 6),
        estimated_cost_cdn=round(cost_cdn, 6),
        estimated_cost_total=round(cost_a + cost_b + cost_cdn, 6),
        class_a_breakdown=agg_data["class_a_breakdown"],
        class_b_breakdown=agg_data["class_b_breakdown"],
    )

    return UsageLogResponse.with_telemetry(
        service_id=service_id,
        entries=entries,
        total=total,
        aggregate=aggregate,
    )


# response_model intentionally omitted: streams CSV (StreamingResponse),
# not a JSON body.
@router.get(
    "/admin/usage-log/export",
    dependencies=[Depends(require_admin)],
)
def usage_log_export(
    source: dict = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    usage_type: str = Query(default=""),
    process_context: str = Query(default=""),
    operation_type: str = Query(default=""),
    max_rows: int = Query(default=100_000, ge=1, le=1_000_000),
) -> StreamingResponse:
    """Export _usage_log as CSV from metadata_db (SQLite).

    Paginated server-side in 5k chunks to keep the worker's resident set
    bounded — the previous implementation pulled the whole result into a
    single Python list before the generator started iterating, which
    materialised ~30+ MB per export and held the SQLite connection open
    the whole time. Now each chunk is fetched, written to the CSV
    generator, and released before the next page is read.

    ``max_rows`` caps the total to prevent an admin clicking the wrong
    filter from pulling tens of millions of rows; the default matches
    the prior implicit cap, the limit is the absolute ceiling.
    """

    from fastapi.responses import StreamingResponse as _StreamingResponse

    from backend.core import metadata as metadata_db
    from backend.utils.date_utils import parse_date_window

    start_str, end_str = parse_date_window(start, end)
    service_id = service_id_from_source(source, prefer_name=True)

    CHUNK = 5_000

    def generate() -> Iterator[str]:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "timestamp",
                "service_id",
                "operation_class",
                "operation_type",
                "url",
                "bytes",
                "duration_ms",
                "function_name",
                "process_context",
                "status",
                "count",
            ]
        )
        # Flush the header before iterating rows so an empty result-set
        # still produces a valid header-only CSV (rather than an empty body).
        buf.seek(0)
        yield buf.read()
        buf.seek(0)
        buf.truncate(0)

        # Keyset (seek) pagination via iter_usage_logs_chunks: skips the
        # OFFSET-N scan cost that grew with page number. Each chunk costs
        # ~constant time regardless of how many chunks have already been
        # emitted, so a 100k-row export drops from O(N²) cumulative scan
        # work to O(N). The helper also handles the empty-result + short-
        # chunk termination so the loop here stays a single for/yield.
        for chunk in metadata_db.iter_usage_logs_chunks(
            service_id=service_id,
            start=start_str,
            end=end_str,
            usage_type=usage_type,
            process_context=process_context,
            operation_type=operation_type,
            chunk_size=CHUNK,
            max_rows=max_rows,
        ):
            for row in chunk:
                row_data = [
                    row["timestamp"],
                    row["service_id"],
                    row["operation_class"],
                    row["operation_type"],
                    row["url"],
                    row["bytes"],
                    row["duration_ms"],
                    row["function_name"],
                    row["process_context"],
                    row["status"],
                    row["count"] if "count" in row.keys() else 1,
                ]
                writer.writerow(
                    [
                        f"'{v}" if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@")) else v
                        for v in row_data
                    ]
                )
            buf.seek(0)
            yield buf.read()
            buf.seek(0)
            buf.truncate(0)

    headers = {"Content-Disposition": "attachment; filename=usage_log.csv"}
    return _StreamingResponse(generate(), media_type="text/csv", headers=headers)


@router.delete(
    "/admin/usage-log",
    response_model=OkResponse,
    dependencies=[Depends(require_admin)],
)
def purge_usage_log_endpoint(source: dict = Depends(get_source)) -> dict[str, Any]:
    """Delete all _usage_log entries for this service from metadata_db (SQLite)."""
    from backend.core import metadata as metadata_db

    metadata_db.clear_usage_log(service_id_from_source(source, prefer_name=True))
    return {"ok": True}


@router.get(
    "/admin/system-jobs",
    response_model=SystemJobsResponse,
    dependencies=[Depends(require_admin)],
)
def get_system_jobs_endpoint() -> Any:
    """Return status and schedule info for global background jobs."""
    from backend.cron.decorators import get_system_job_status
    from backend.cron.scheduler import get_scheduler

    statuses = get_system_job_status()
    result = []
    job_labels = {
        "bot_data_refresh": "Bot Data Refresh",
        "rdns_enrichment": "rDNS Enrichment",
        "share_audit_purge": "Share Audit Purge",
    }
    sched = get_scheduler()
    for job_id, label in job_labels.items():
        entry = {
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
            # ``next_run_time`` is only set when the scheduler is running
            # AND the job has a future fire time. After ``scheduler.shutdown()``
            # (or when the job is paused) the attribute is absent or None,
            # so use getattr() to fail-soft rather than 500 the admin panel.
            next_run = getattr(job, "next_run_time", None) if job else None
            if next_run:
                from backend.utils.date_utils import iso_z

                entry["next_run_at"] = iso_z(next_run)
        result.append(entry)

    return SystemJobsResponse.with_telemetry(jobs=result)
