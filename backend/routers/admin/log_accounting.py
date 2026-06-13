"""Log-accounting: Fastly Stats vs locally-ingested counts + backfill.

Hosts the sustained-loss thresholds referenced by both the UI callout
and the gap-heal cron in scheduler.py — see those module's imports.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import Depends, HTTPException, Query

from backend.core.fastly.utils import FASTLY_LOG_FIELDS as _FASTLY_LOG_FIELDS
from backend.deps import get_source
from backend.models.admin import (
    LogAccountingBucket,
    LogAccountingResponse,
    LogAccountingTotals,
    SustainedLossAlert,
)

from ._router import router


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


def _fetch_fastly_log_counts(
    logging_svc_id: str, api_key: str, from_ts: int, to_ts: int, by: str
) -> tuple[dict[str, int], str | None]:
    """Return (bucket_iso → log_count, field_name_used or None).

    Bucket key is the UTC ISO string at the same width the local SQL bucket
    uses (`YYYY-MM-DDTHH` for hour, `YYYY-MM-DD` for day) so the outer-join
    in api_log_accounting can key on string equality directly.
    """
    import logging
    from datetime import UTC, datetime

    from backend.core.fastly.client import fastly

    payload = fastly(
        "GET",
        f"/stats/service/{logging_svc_id}?by={by}&from={from_ts}&to={to_ts}",
        token=api_key,
    )

    width = 13 if by == "hour" else 10
    records = payload.get("data", []) or []
    out: dict[str, int] = {}
    field_used: str | None = None
    missing_logged = False
    for r in records:
        ts = r.get("start_time")
        if ts is None:
            continue
        bucket = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")[:width]
        chosen = 0
        for fname in _FASTLY_LOG_FIELDS:
            v = r.get(fname)
            if v:
                chosen = int(v)
                field_used = fname
                break
        if chosen == 0 and field_used is None and not missing_logged:
            logging.getLogger("admin.log_accounting").warning(
                "Fastly /stats/service response has no log-count field; keys present=%s",
                sorted(r.keys()),
            )
            missing_logged = True
        out[bucket] = out.get(bucket, 0) + chosen
    return out, field_used


# Sustained-loss thresholds — referenced by both api_log_accounting (so the
# UI callout matches the heal trigger) and the gap-heal cron in scheduler.py.
LOG_ACCOUNTING_LOSS_THRESHOLD = 0.05
LOG_ACCOUNTING_MIN_RUN = 2


def _duckdb_row_counts_per_bucket(source: dict, start: datetime, end: datetime, by: str) -> dict[str, int]:
    """Per-bucket ``COUNT(*)`` from the live DuckDB view — the post-dedup
    truth that should drive the log-accounting comparison.

    Returns ``{bucket_string: count}`` where bucket_string matches the
    SQLite metadata path's format (``YYYY-MM-DD-HH`` for hourly,
    ``YYYY-MM-DD`` for daily) so the loop above can union the keys.

    Opens its own short-lived read-only connection. Cheap on this query
    (single aggregate, no joins) — ~50-150 ms on a 24h window on prod.
    Errors collapse to an empty dict so the route still degrades to the
    metadata-only path rather than 500ing.
    """
    from backend.core import duckdb as _ddb
    from backend.deps import _ConnectionHolder

    table_name = _ddb._safe_table_name(source["name"])
    # Bucket key MUST match metadata_db.get_log_accounting_counts: hourly
    # uses ``YYYY-MM-DDTHH`` (T separator, from the .log.gz basename's
    # ISO prefix); daily uses ``YYYY-MM-DD``. Mismatch here makes the
    # union-by-key loop in compute_log_accounting produce ghost buckets
    # with our_rows but zero fastly_logs.
    fmt = "%Y-%m-%dT%H" if by == "hour" else "%Y-%m-%d"
    start_iso = start.strftime("%Y-%m-%d %H:%M:%S")
    end_iso = end.strftime("%Y-%m-%d %H:%M:%S")
    try:
        # read_only=True so this uses the pool (cheap, doesn't contend with
        # the cron writer).
        with _ConnectionHolder(source, read_only=True) as con:
            rows = con.execute(
                f"SELECT strftime(timestamp, '{fmt}') AS bucket, COUNT(*) AS n "
                f"FROM {table_name} "
                f"WHERE timestamp >= TIMESTAMP '{start_iso}' "
                f"  AND timestamp <  TIMESTAMP '{end_iso}' "
                f"GROUP BY 1"
            ).fetchall()
        return {b: int(n) for b, n in rows}
    except Exception as e:
        import logging as _logging

        _logging.getLogger(__name__).warning(
            "[log-accounting] DuckDB counts unavailable, falling back to metadata: %s", e
        )
        return {}


def compute_log_accounting(source: dict, hours: int = 24, by: str = "hour") -> dict:
    """Pure compute path for log-line accounting.

    Returns a dict with all the fields api_log_accounting surfaces:
    ``buckets``, ``totals``, ``sustained_loss``, ``fastly_field_used``,
    ``from_ts``, ``to_ts``. Raises HTTPException on configuration error
    (no logging_service_id / no api_key) or on Fastly Stats API failure.

    Extracted so the gap-heal cron can reuse the same Fastly fetch + SQL +
    sustained-loss detection without duplicating the math — drift between
    the two would mean the heal trigger and the UI callout disagree.
    """
    from datetime import UTC, datetime, timedelta

    from backend import config as svcconfig
    from backend.core import metadata_db

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
    from_ts = int(start.timestamp())
    to_ts = int((now + timedelta(hours=1 if by == "hour" else 24)).timestamp())

    try:
        fastly_counts, field_used = _fetch_fastly_log_counts(logging_svc_id, api_key, from_ts, to_ts, by)
    except Exception as e:
        raise HTTPException(status_code=502, detail={"error": f"Fastly Stats API call failed: {e}"})

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
    local_counts = metadata_db.get_log_accounting_counts(
        service_id, sql_start_iso, sql_end_iso, width, start_bucket, end_bucket
    )

    # ``our_rows`` comes from the live DuckDB view rather than
    # ``ingested_files.row_count``. Reason: the metadata column reflects
    # rows WRITTEN at ingest time. After ``local_compaction`` deduped by
    # ``rid`` to clean up the buffer-commit-replay dup pattern (fixed
    # 2026-06-12 in PR #21), the metadata column over-counts by the dup
    # factor. Reading from DuckDB matches what the dashboard charts
    # actually show. ``file_count`` stays from the metadata table — it's
    # the count of source .log.gz files ingested, unrelated to dedup.
    duckdb_counts: dict[str, int] = _duckdb_row_counts_per_bucket(source, start, end_clamp, by)

    all_buckets = sorted(set(fastly_counts.keys()) | set(local_counts.keys()) | set(duckdb_counts.keys()))
    buckets: list[LogAccountingBucket] = []
    total_fastly = 0
    total_ours = 0
    worst_ts: str | None = None
    worst_gap_pct: float | None = None
    for b in all_buckets:
        fastly = int(fastly_counts.get(b, 0))
        _meta_rows, fcount = local_counts.get(b, (0, 0))
        # Prefer DuckDB's authoritative live count; fall back to metadata
        # only when DuckDB has no entry (very old buckets that aged out of
        # the local cache but still have an ingested_files row).
        ours = int(duckdb_counts.get(b, _meta_rows))
        gap = fastly - ours
        denom = fastly if fastly > 0 else ours
        gap_pct = (gap / denom) if denom > 0 else 0.0
        ts_iso = f"{b}:00:00Z" if by == "hour" else f"{b}T00:00:00Z"
        buckets.append(
            LogAccountingBucket(
                ts=ts_iso,
                fastly_logs=fastly,
                our_rows=ours,
                file_count=fcount,
                gap=gap,
                gap_pct=round(gap_pct, 6),
            )
        )
        total_fastly += fastly
        total_ours += ours
        # Rank by positive gap only — negative gaps are bucket-edge drift
        # where one side's emission/ingest straddled the boundary. The user
        # cares about "Fastly emitted more than we ingested" (real loss),
        # not "we ingested more than Fastly reports yet" (timing artifact).
        if gap_pct > (worst_gap_pct or 0.0):
            worst_ts = ts_iso
            worst_gap_pct = gap_pct

    total_gap = total_fastly - total_ours
    total_denom = total_fastly if total_fastly > 0 else total_ours
    total_pct = round((total_gap / total_denom), 6) if total_denom > 0 else 0.0
    totals = LogAccountingTotals(
        fastly_logs=total_fastly,
        our_rows=total_ours,
        gap=total_gap,
        gap_pct=total_pct,
        worst_bucket_ts=worst_ts,
        worst_bucket_gap_pct=(round(worst_gap_pct, 6) if worst_gap_pct is not None else None),
    )

    # Sustained-loss detection: only flag runs of ≥MIN_RUN consecutive completed
    # buckets with one-sided positive gap ≥LOSS_THRESHOLD (Fastly emitted more
    # than we ingested). Bucket-edge drift is bidirectional and stays under
    # 2.5%; the in-flight bucket is noisy because Fastly Stats lags our ingest,
    # so we exclude it from the scan. Returns the longest qualifying run.
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
    latest_ingest_str = metadata_db.get_latest_ingest_ts(service_id)
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

    return {
        "by": by,
        "from_ts": start_iso + "Z",
        "to_ts": end_iso + "Z",
        "fastly_field_used": field_used,
        "buckets": buckets,
        "totals": totals,
        "sustained_loss": sustained,
        "catchup": catchup,
    }


@router.get("/admin/log-accounting", response_model=LogAccountingResponse)
def api_log_accounting(
    source: dict = Depends(get_source),
    hours: int = Query(24, ge=1, le=720),
    by: str = Query("hour", pattern="^(hour|day)$"),
) -> LogAccountingResponse:
    """Reconcile Fastly's authoritative log-line emission count against our
    locally-ingested row counts to surface any gap between emission and ingest.

    Per-bucket gap is the actionable signal — totals smooth over burst losses.
    """
    result = compute_log_accounting(source, hours=hours, by=by)
    response: LogAccountingResponse = LogAccountingResponse.with_telemetry(**result)
    return response
