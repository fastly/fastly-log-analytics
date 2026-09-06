"""Log-accounting: Fastly Stats vs locally-ingested counts + backfill.

Hosts the sustained-loss thresholds referenced by both the UI callout
and the gap-heal cron in scheduler.py — see those module's imports.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

from fastapi import Depends, HTTPException, Query, Response

from backend.deps import get_source
from backend.models.admin import (
    LogAccountingBucket,
    LogAccountingResponse,
    LogAccountingTotals,
    SustainedLossAlert,
)
from backend.utils.router_utils import raise_internal

from ._router import router

logger = logging.getLogger(__name__)

# Short-TTL memo for the Fastly Stats API fetch (the dominant cost
# inside compute_log_accounting: ~1.8 s p95). Key includes the (already
# hour-aligned) from_ts/to_ts so different windows don't collide; the
# admin UsageChart polls at 60 s and the React Query layer staleTime is
# 30 s, so a 30 s server-side TTL is well inside any user-visible
# staleness budget and removes the spinner feel.
_FASTLY_COUNTS_TTL = 45.0
_FASTLY_COUNTS_CACHE: dict[tuple[str, int, int, str, bool], tuple[float, dict[str, int]]] = {}

# Same TTL on the per-bucket DuckDB COUNT(*) since the function arguments
# are functions of (service name, window, by) and the answer is stable
# for the same input within the TTL window. Keyed on the same shape so a
# single round of clears would invalidate both halves of the response.
_DUCKDB_COUNTS_TTL = 45.0
_DUCKDB_COUNTS_CACHE: dict[tuple[str, int, int, str], tuple[float, dict[str, int]]] = {}


@router.post("/admin/backfill-window")
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

    from backend.core.fastly.client import fastly

    cache_key = (logging_svc_id, from_ts, to_ts, by, edge_only)
    now_mono = time.monotonic()
    cached = _FASTLY_COUNTS_CACHE.get(cache_key)
    if cached is not None and (now_mono - cached[0]) < _FASTLY_COUNTS_TTL:
        return cached[1]

    payload = fastly(
        "GET",
        f"/stats/service/{logging_svc_id}?by={by}&from={from_ts}&to={to_ts}",
        token=api_key,
    )

    width = 13 if by == "hour" else 10
    records = payload.get("data", []) or []
    out: dict[str, int] = {}
    stat_key = "edge_requests" if edge_only else "requests"
    for r in records:
        ts = r.get("start_time")
        if ts is None:
            continue
        bucket = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")[:width]
        out[bucket] = out.get(bucket, 0) + int(r.get(stat_key) or r.get("requests") or 0)
    _FASTLY_COUNTS_CACHE[cache_key] = (now_mono, out)
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
    now_mono = time.monotonic()
    cached = _DUCKDB_COUNTS_CACHE.get(cache_key)
    if cached is not None and (now_mono - cached[0]) < _DUCKDB_COUNTS_TTL:
        return cached[1]

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
        _DUCKDB_COUNTS_CACHE[cache_key] = (now_mono, result)
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
    from backend.repositories._base import collect_hourly_bundle_paths

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
            rows = con.execute(
                f"SELECT strftime(date_trunc('{unit}', hour_start), '{fmt}') AS bucket, "
                f"  SUM(requests) AS n "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE hour_start >= TIMESTAMPTZ '{st_tz}' "
                f"  AND hour_start < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY date_trunc('{unit}', hour_start)"
            ).fetchall()
        for b, n in rows:
            result[b] = int(n)

    if crosses_active:
        import glob
        from datetime import timedelta

        from backend.core import duckdb as _ddb
        from backend.core.iceberg.buffer import buffer_files
        from backend.utils.sql_validator import escape_sql_literal

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
                rows = con.execute(
                    f"SELECT strftime(date_trunc('{unit}', timestamp), '{fmt}') AS bucket, COUNT(*) AS n "
                    f"FROM read_parquet([{paths_sql}], union_by_name=true) "
                    f"WHERE timestamp >= TIMESTAMP '{live_start_iso}' "
                    f"  AND timestamp <  TIMESTAMP '{end_iso}' "
                    f"GROUP BY date_trunc('{unit}', timestamp)"
                ).fetchall()
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
        rows = con.execute(
            f"SELECT strftime(date_trunc('{unit}', timestamp), '{fmt}') AS bucket, COUNT(*) AS n "
            f"FROM {table_name} "
            f"WHERE timestamp >= TIMESTAMP '{start_iso}' "
            f"  AND timestamp <  TIMESTAMP '{end_iso}' "
            f"GROUP BY date_trunc('{unit}', timestamp)"
        ).fetchall()
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
    api_key = svcconfig.get_fastly_api_key(service_id)
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

    try:
        fastly_counts = timer.call(
            "fastly_fetch",
            lambda: _fetch_fastly_request_counts(logging_svc_id, api_key, from_ts, to_ts, by, edge_only=edge_only),
        )
    except Exception as e:
        raise_internal(logger, e, code="fastly_stats_failed", status=502)

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
    local_counts = timer.call(
        "sqlite_counts",
        lambda: metadata_db.get_log_accounting_counts(
            service_id, sql_start_iso, sql_end_iso, width, start_bucket, end_bucket
        ),
    )

    # ``our_rows`` comes from the live DuckDB view rather than
    # ``ingested_files.row_count``. Reason: the metadata column reflects
    # rows WRITTEN at ingest time. After ``local_compaction`` deduped by
    # ``rid`` to clean up the buffer-commit-replay dup pattern (fixed
    # 2026-06-12 in PR #21), the metadata column over-counts by the dup
    # factor. Reading from DuckDB matches what the dashboard charts
    # actually show. ``file_count`` stays from the metadata table — it's
    # the count of source .log.gz files ingested, unrelated to dedup.
    duckdb_counts: dict[str, int] = timer.call(
        "duckdb_counts",
        lambda: _duckdb_row_counts_per_bucket(source, start, end_clamp, by),
    )

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
def api_log_accounting(
    response: Response,
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
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("routers.admin.log_accounting._FASTLY_COUNTS_CACHE", _FASTLY_COUNTS_CACHE)
_CacheRegistry.register("routers.admin.log_accounting._DUCKDB_COUNTS_CACHE", _DUCKDB_COUNTS_CACHE)
