"""Source registration + FOS/CDN usage telemetry against metadata SQLite.

Covers the ``sources``, ``usage_log``, and ``usage_log_hourly_summary``
tables. The hourly summary is the rolled-up backstop that lets the admin
Usage Log page render against millions of raw usage_log rows without
re-scanning the full table on every request.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta

from backend.core.metadata import usage_log_db as _usage_log_db
from backend.core.metadata.base import get_con
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


def _ul(service_id: str) -> sqlite3.Connection:
    """Thread-local RW connection to the per-service usage_log.db.

    Carved out of metadata.db on 2026-06-12 per the perf audit — keeps
    the cron writer's WAL lock isolated from the admin endpoints that
    read audit_logs / views / scoring_labels off metadata.db. See
    :mod:`backend.core.metadata.usage_log_db` for the rationale.

    Code that reads/writes the ``sources`` table (only consumers
    register_source / get_source_by_name below) continues to use
    :func:`backend.core.metadata.base.get_con` — sources lives in
    metadata.db, not usage_log.db.
    """
    return _usage_log_db.get_con(service_id)


# ── sources ───────────────────────────────────────────────────────────────────


def register_source(service_id: str, name: str, config_json: str, table_name: str) -> None:
    """Idempotently register a source. Returns nothing (callers compute table_name themselves)."""
    con = get_con(service_id)
    con.execute(
        "INSERT OR IGNORE INTO sources (name, config, table_name) VALUES (?, ?, ?)",
        (name, config_json, table_name),
    )
    con.commit()


def get_source_by_name(service_id: str, name: str) -> dict | None:
    con = get_con(service_id)
    row = con.execute(
        "SELECT name, config, table_name FROM sources WHERE name = ?",
        (name,),
    ).fetchone()
    if not row:
        return None
    return {"name": row["name"], "config": row["config"], "table_name": row["table_name"]}


# ── usage_log ─────────────────────────────────────────────────────────────────


def log_usage_calls(service_id: str, calls: list[dict], process_context: str | None = None) -> None:
    if not calls:
        return
    con = _ul(service_id)
    now = iso_z_now()
    rows = []
    for c in calls:
        op_type = (c.get("method") or "").upper()
        details = c.get("details") or ""
        svc = c.get("service", "FOS")

        # FOS classification:
        #   Class A: PUT/POST/COPY/LIST family (mutating writes, multi-object delete via POST ?delete).
        #     Canonical S3 op names land here; so do raw HTTP verbs PUT/POST/COPY,
        #     which is what the telemetry proxy emits via request.method.
        #   Class B: GET/HEAD/single-object DELETE (the default).
        # Note: single-object DELETE (`DELETE /key`) is Class B in Fastly billing;
        # the DeleteObjects batch endpoint arrives as POST and is therefore A.
        op_class = "B"
        if svc == "FOS" and op_type in (
            "PUT_OBJECT",
            "POST_OBJECT",
            "COPY_OBJECT",
            "LIST_OBJECTS_V2",
            "DELETE_OBJECTS",
            "PUT",
            "POST",
            "COPY",
        ):
            op_class = "A"
        elif svc == "CDN":
            op_class = "CDN"
        elif "Class A" in details:
            op_class = "A"

        # Apply shield egress multiplier for CDN operations
        op_bytes = c.get("bytes")
        if op_class == "CDN" and op_bytes is not None:
            # X-Cache values are stored at the beginning of details: "HIT, MISS · duckdb httpfs"
            # Fastly X-Cache order is: Shield POP first, Edge POP second.
            # If there's a comma (multiple POPs) AND the Edge POP (the last value)
            # is MISS or PASS, the Edge fetched the payload from the Shield.
            # This doubles the egress cost (Shield -> Edge -> Client).
            x_cache_part = details.split(" · ")[0] if " · " in details else details
            parts = [p.strip().upper() for p in x_cache_part.split(",") if p.strip()]
            if len(parts) > 1 and parts[-1] in ("MISS", "PASS"):
                op_bytes = op_bytes * 2

        rows.append(
            (
                now,
                service_id,
                op_class,
                c.get("method"),
                c.get("path"),
                str(c.get("status", "OK")),
                c.get("time_ms"),
                c.get("caller"),
                process_context,
                op_bytes,
            )
        )
    try:
        con.executemany(
            "INSERT INTO usage_log "
            "(timestamp, service_id, operation_class, operation_type, url, status, "
            " duration_ms, function_name, process_context, bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        con.commit()
    except Exception as e:
        logger.error("[metadata_db] Failed to log usage calls: %s", e)


def log_synthetic_usage(service_id: str, calls: list[dict]) -> int:
    """Idempotently log synthetic usage rows (e.g. Fastly-edge backfill).

    Dedupes against existing rows where function_name = 'fastly.edge' AND url IN (incoming).
    Returns the number of newly inserted rows.
    """
    if not calls:
        return 0
    con = _ul(service_id)

    urls = [c.get("path") for c in calls if c.get("path")]
    if not urls:
        return 0

    existing: set[str] = set()
    for i in range(0, len(urls), 500):
        chunk = urls[i : i + 500]
        placeholders = ", ".join("?" for _ in chunk)
        cur = con.execute(
            f"SELECT url FROM usage_log WHERE service_id = ? AND function_name = 'fastly.edge' AND url IN ({placeholders})",
            [service_id] + chunk,
        )
        existing.update(r["url"] for r in cur.fetchall())

    new_rows = []
    now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    for c in calls:
        url = c.get("path")
        if not url or url in existing:
            continue
        ts = c.get("_timestamp_override") or now_iso
        new_rows.append(
            (
                ts,
                service_id,
                "A",
                c.get("method", "PUT_OBJECT"),
                url,
                str(c.get("status", "OK")),
                0.0,
                c.get("caller", "fastly.edge"),
                c.get("process_context", "fastly:log_write"),
                c.get("bytes"),
            )
        )

    if not new_rows:
        return 0
    try:
        con.executemany(
            "INSERT INTO usage_log "
            "(timestamp, service_id, operation_class, operation_type, url, status, "
            " duration_ms, function_name, process_context, bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            new_rows,
        )
        con.commit()
        return len(new_rows)
    except Exception as e:
        logger.error("[metadata_db] Synthetic usage log failed: %s", e)
        return 0


def reconcile_fastly_stats(
    service_id: str,
    hourly_records: list[dict],
) -> int:
    """Upsert per-hour reconciliation rows to align local usage_log with Fastly's
    authoritative /stats/aggregate counts.

    Each record in ``hourly_records`` is a dict with::

        {
            "hour_iso": "2026-05-22T13:00:00Z",  # bucket start (UTC, hour-aligned)
            "class_a": <int>,                     # Fastly's reported Class A ops for the hour
            "class_b": <int>,                     # Fastly's reported Class B ops for the hour
        }

    For each (hour, class) pair we compute ``gap = fastly_count - local_sum``
    where ``local_sum`` is SUM(count) over rows in that hour excluding prior
    reconciliation rows. We then DELETE any existing reconciliation rows for
    that hour/class and INSERT one row with ``count = gap`` when gap > 0.

    Reconciliation rows are tagged ``function_name='fastly.reconciliation'`` and
    ``process_context='fastly:reconciliation'`` so they're trivially separable
    from observed rows in queries and excluded from future ``local_sum`` math.

    Returns the number of reconciliation rows written (one per non-zero gap).
    """
    if not hourly_records:
        return 0
    con = _ul(service_id)

    # Normalise the incoming records into {hour_start_iso: {"A": int, "B": int}}.
    by_hour: dict[str, dict[str, int]] = {}
    earliest: datetime | None = None
    latest: datetime | None = None
    for rec in hourly_records:
        hour_iso = rec.get("hour_iso")
        if not hour_iso:
            continue
        try:
            start_dt = datetime.strptime(hour_iso.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
        except (ValueError, AttributeError):
            continue
        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        by_hour[start_str] = {
            "A": int(rec.get("class_a") or 0),
            "B": int(rec.get("class_b") or 0),
        }
        if earliest is None or start_dt < earliest:
            earliest = start_dt
        if latest is None or start_dt > latest:
            latest = start_dt

    if not by_hour or earliest is None or latest is None:
        return 0

    window_start = earliest.strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = (latest + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Single scan covering both classes — substr() truncates the ISO
    # timestamp to its hour prefix; SQLite groups by string equality,
    # which works because we write all rows in the same "%Y-%m-%dT%H:%M:%SZ"
    # format. The supporting index is idx_usage_reconcile (service_id,
    # operation_class, timestamp), so the IN-list still uses the index.
    local_sums: dict[tuple[str, str], int] = {}
    for r in con.execute(
        """
        SELECT operation_class, substr(timestamp, 1, 13), coalesce(sum(count), 0)
        FROM usage_log
        WHERE service_id = ? AND operation_class IN ('A', 'B')
          AND timestamp >= ? AND timestamp < ?
          AND function_name != 'fastly.reconciliation'
        GROUP BY operation_class, 2
        """,
        (service_id, window_start, window_end),
    ):
        local_sums[(r[0], r[1])] = int(r[2] or 0)

    # Wipe prior reconciliation rows in the window in a single range delete
    # spanning both classes, then insert one row per (hour, class) gap > 0.
    con.execute(
        """
        DELETE FROM usage_log
        WHERE service_id = ? AND operation_class IN ('A', 'B')
          AND timestamp >= ? AND timestamp < ?
          AND function_name = 'fastly.reconciliation'
        """,
        (service_id, window_start, window_end),
    )

    written = 0
    insert_rows: list[tuple] = []
    for hour_start, classes in by_hour.items():
        hour_prefix = hour_start[:13]  # "YYYY-MM-DDTHH"
        for op_class, fastly_count in classes.items():
            local_sum = local_sums.get((op_class, hour_prefix), 0)
            gap = fastly_count - local_sum
            if gap > 0:
                insert_rows.append(
                    (
                        hour_start,
                        service_id,
                        op_class,
                        f"RECONCILE_{op_class}",
                        f"fastly://stats/aggregate/{hour_start}",
                        "OK",
                        0.0,
                        "fastly.reconciliation",
                        "fastly:reconciliation",
                        None,
                        gap,
                    )
                )
                written += 1

    if insert_rows:
        con.executemany(
            """
            INSERT INTO usage_log
            (timestamp, service_id, operation_class, operation_type, url, status,
             duration_ms, function_name, process_context, bytes, count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
    con.commit()
    return written


def purge_usage_log(service_id: str, retention_days: int) -> None:
    if retention_days <= 0:
        return
    con = _ul(service_id)
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con.execute("DELETE FROM usage_log WHERE timestamp < ?", (cutoff,))
    con.commit()


def clear_usage_log(service_id: str) -> None:
    con = _ul(service_id)
    con.execute("DELETE FROM usage_log WHERE service_id = ?", (service_id,))
    con.commit()


USAGE_LOG_HOURLY_BACKFILL_NAME = "2026-06-04_usage_log_hourly_summary_backfill"

# Per-process guard so the in-process check doesn't hit SQLite on every read.
# The DB-level marker (applied_data_migrations) is the source of truth across
# restarts; this cache just trims redundant lookups within one process.
_usage_log_backfilled: set[str] = set()
_usage_log_backfill_lock = threading.Lock()


def _ensure_usage_log_hourly_backfilled(con: sqlite3.Connection, service_id: str) -> None:
    """Populate usage_log_hourly_summary for services upgrading from a
    pre-trigger install. Idempotent; runs at most once per service.

    Detection: presence of the named row in ``applied_data_migrations``. The
    trigger handles all NEW inserts; this backfill catches the rows that
    existed before the trigger was added. Synchronous so /admin/usage-log
    returns correct data on first access (typically <1 s for ~1 M rows).

    Post-2026-06-12 carve-out: usage_log now lives in its own SQLite file
    (``data/services/<sid>.usage_log.db``); the ``applied_data_migrations``
    table that tracked the legacy migration stamp lives in the OTHER db
    (``<sid>.metadata.db``). When the marker table isn't present in the
    connection we were handed, the backfill is no-op-safe to skip —
    :func:`backend.core.metadata.usage_log_db.migrate_from_metadata_db`
    already replays the authoritative summary during the one-time copy,
    and the AFTER INSERT trigger keeps it consistent thereafter.
    """
    if service_id in _usage_log_backfilled:
        return
    with _usage_log_backfill_lock:
        if service_id in _usage_log_backfilled:
            return
        try:
            has_marker_table = con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='applied_data_migrations'"
            ).fetchone()
            if has_marker_table is None:
                # usage_log.db — no marker table here; the migration
                # path already populated the summary correctly. Mark in-
                # process so the lookup doesn't repeat for this service.
                _usage_log_backfilled.add(service_id)
                return
            applied = con.execute(
                "SELECT 1 FROM applied_data_migrations WHERE name = ?",
                (USAGE_LOG_HOURLY_BACKFILL_NAME,),
            ).fetchone()
            if applied is None:
                t0 = time.time()
                logger.info("[usage_log] backfilling hourly summary for %s", service_id)
                # Wipe any partial summary rows the trigger may have written
                # for this service since boot — we're rebuilding from raw so
                # the GROUP BY sum is exact, not double-counted on top of
                # trigger-written rows.
                con.execute("DELETE FROM usage_log_hourly_summary WHERE service_id = ?", (service_id,))
                con.execute(
                    """
                    INSERT INTO usage_log_hourly_summary
                        (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
                    SELECT service_id,
                           substr(timestamp, 1, 13),
                           COALESCE(operation_class, ''),
                           COALESCE(operation_type, ''),
                           SUM(COALESCE(count, 1)),
                           SUM(COALESCE(bytes, 0)),
                           datetime('now')
                    FROM usage_log
                    WHERE service_id = ?
                      AND timestamp IS NOT NULL
                      AND length(timestamp) >= 13
                    GROUP BY 1, 2, 3, 4
                    """,
                    (service_id,),
                )
                con.execute(
                    "INSERT OR REPLACE INTO applied_data_migrations "
                    "(name, applied_at, duration_s, status, notes) VALUES (?, ?, ?, ?, ?)",
                    (
                        USAGE_LOG_HOURLY_BACKFILL_NAME,
                        iso_z_now(),
                        time.time() - t0,
                        "success",
                        "rebuilt usage_log_hourly_summary from raw",
                    ),
                )
                con.commit()
                logger.info("[usage_log] hourly backfill complete for %s in %.2fs", service_id, time.time() - t0)
        except Exception as e:
            logger.warning("[usage_log] hourly summary backfill failed for %s: %s", service_id, e)
        _usage_log_backfilled.add(service_id)


def _query_usage_log_aggregate_rollup(
    con: sqlite3.Connection,
    service_id: str,
    start: str,
    end: str,
    usage_type: str,
) -> list[sqlite3.Row]:
    """Compute the (operation_class, operation_type) totals exactly using the
    hourly rollup for fully-contained hours plus raw usage_log for the two
    boundary hours (which usually aren't hour-aligned).

    The rollup PK lookup is sub-millisecond; the boundary raw scans cover at
    most 2 hours of data (~80 k rows in a busy service) and ride the
    idx_usage_service_ts index. Combined cost is typically ~1-2 ms vs the
    600 ms full-window GROUP BY this replaces.
    """
    # Hour bucket prefix is "YYYY-MM-DDTHH" (13 chars). Timestamps in
    # usage_log are stored as ISO strings, so prefix comparison is correct.
    start_hour = (start or "")[:13]
    end_hour = (end or "")[:13]

    class_filter = ""
    class_params: list = []
    if usage_type:
        if usage_type == "CDN":
            class_filter = "AND operation_class = 'CDN'"
        elif usage_type == "FOS-A":
            class_filter = "AND operation_class = 'A'"
        elif usage_type == "FOS-B":
            class_filter = "AND operation_class = 'B'"
        elif usage_type == "FOS":
            class_filter = "AND operation_class IN ('A', 'B')"
        else:
            class_filter = "AND operation_class = ?"
            class_params = [usage_type]

    # Sub-hour range collapses to a single raw scan — no hour bucket fully
    # contained, both boundary parts would target the same hour anyway.
    if start_hour == end_hour:
        rows = con.execute(
            f"""
            SELECT operation_class, operation_type,
                   SUM(count) AS c, SUM(COALESCE(bytes, 0)) AS b
            FROM usage_log
            WHERE service_id = ? AND timestamp >= ? AND timestamp <= ? {class_filter}
            GROUP BY operation_class, operation_type
            """,
            [service_id, start, end] + class_params,
        ).fetchall()
        return rows

    # Boundary range comparisons keyed on timestamp directly (not
    # `substr(timestamp, 1, 13)`) so SQLite can ride idx_usage_service_ts
    # as a pure range scan — substr() forces per-row evaluation, ~5x slower
    # on the end-of-day boundary (18k rows: 90ms with substr vs ~15ms with
    # pure range). The hour boundary is the start of the FOLLOWING hour, so
    # we strip any " " or "T" between date/time and use the ISO Z form to
    # match what writers store.
    def _next_hour_start(hour_prefix: str) -> str:
        # "2026-06-04T23" → "2026-06-05T00:00:00.000Z"
        try:
            dt = datetime.strptime(hour_prefix, "%Y-%m-%dT%H").replace(tzinfo=UTC)
        except ValueError:
            return hour_prefix + ":59:59.999Z"
        nxt = dt + timedelta(hours=1)
        return nxt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _hour_start(hour_prefix: str) -> str:
        return hour_prefix + ":00:00.000Z"

    start_hour_end = _next_hour_start(start_hour)
    end_hour_start = _hour_start(end_hour)

    # Three-part UNION ALL: interior hours from rollup, boundary hours from
    # raw usage_log. SUM(SUM(...)) collapses the two sources into a single
    # (op_class, op_type) tuple per group.
    rollup_class_filter = class_filter  # same syntax works against the rollup
    rows = con.execute(
        f"""
        SELECT operation_class, operation_type,
               SUM(c) AS c, SUM(b) AS b
        FROM (
            SELECT operation_class, operation_type, count AS c, bytes AS b
            FROM usage_log_hourly_summary
            WHERE service_id = ? AND hour > ? AND hour < ? {rollup_class_filter}
            UNION ALL
            SELECT operation_class, operation_type, count AS c, COALESCE(bytes, 0) AS b
            FROM usage_log
            WHERE service_id = ? AND timestamp >= ? AND timestamp < ? {class_filter}
            UNION ALL
            SELECT operation_class, operation_type, count AS c, COALESCE(bytes, 0) AS b
            FROM usage_log
            WHERE service_id = ? AND timestamp >= ? AND timestamp <= ? {class_filter}
        )
        GROUP BY operation_class, operation_type
        """,
        # Interior rollup params
        [service_id, start_hour, end_hour]
        + class_params
        # Start-boundary raw params: [start, next_hour_after_start_hour)
        + [service_id, start, start_hour_end]
        + class_params
        # End-boundary raw params: [start_of_end_hour, end]
        + [service_id, end_hour_start, end]
        + class_params,
    ).fetchall()
    return rows


def get_usage_logs(
    service_id: str,
    start: str,
    end: str,
    *,
    usage_type: str = "",
    process_context: str = "",
    operation_type: str = "",
    page: int = 1,
    page_size: int = 100,
) -> tuple[list[dict], int, dict]:
    """Paginated usage log query with aggregates. Used by the Usage Log page."""
    con = _ul(service_id)
    conditions = ["service_id = ?", "timestamp >= ?", "timestamp <= ?"]
    params: list = [service_id, start, end]

    if usage_type:
        if usage_type == "CDN":
            conditions.append("operation_class = 'CDN'")
        elif usage_type == "FOS-A":
            conditions.append("operation_class = 'A'")
        elif usage_type == "FOS-B":
            conditions.append("operation_class = 'B'")
        elif usage_type == "FOS":
            conditions.append("operation_class IN ('A', 'B')")
        else:
            conditions.append("operation_class = ?")
            params.append(usage_type)

    if process_context:
        conditions.append("process_context LIKE ?")
        params.append(f"%{process_context}%")
    if operation_type:
        conditions.append("operation_type LIKE ?")
        params.append(f"%{operation_type}%")

    where = " AND ".join(conditions)

    # Fold COUNT(*) into the page query via a window function so we don't
    # do two passes over the same (service_id, [start, end]) range. The
    # previous separate COUNT + SELECT pair added ~40-60ms per page load.
    # COUNT(*) OVER () is constant across rows so it's computed once
    # during plan execution rather than per-row.
    offset = (page - 1) * page_size
    cur = con.execute(
        f"SELECT *, COUNT(*) OVER () AS _total FROM usage_log WHERE {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        params + [page_size, offset],
    )
    raw_rows = cur.fetchall()
    if raw_rows:
        total = int(raw_rows[0]["_total"] or 0)
        entries = [{k: v for k, v in dict(r).items() if k != "_total"} for r in raw_rows]
    else:
        # Empty page (no matching rows OR past the last page): fall back
        # to a cheap exact COUNT so totals stay correct for pagination UX.
        total = con.execute(f"SELECT count(*) FROM usage_log WHERE {where}", params).fetchone()[0]
        entries = []

    # Aggregate path: prefer the usage_log_hourly_summary rollup when only the
    # service+timestamp predicates are active (the common admin-page case). The
    # rollup is maintained incrementally by trg_usage_log_summary_insert, so
    # it's always consistent — no scheduler needed. We can only use it when no
    # process_context / operation_type LIKE filters are present (the rollup
    # doesn't carry those columns); the operation_class filter IS supported
    # because the rollup stores it as a normalised key. Backfill of any
    # service that predates the trigger happens lazily on first read.
    rollup_eligible = not process_context and not operation_type
    if rollup_eligible:
        _ensure_usage_log_hourly_backfilled(con, service_id)
        grouped = _query_usage_log_aggregate_rollup(con, service_id, start, end, usage_type)
    else:
        # One GROUP BY (operation_class, operation_type) does the work of both the
        # 5-CASE-WHEN totals query AND the per-class breakdown — they're the same
        # 800K-row scan over usage_log, just shaped differently. Doing both in
        # one query saves a full pass per Usage Log page load (~1s on prod).
        grouped = con.execute(
            f"""
            SELECT operation_class, operation_type,
                   sum(count) AS c, sum(coalesce(bytes, 0)) AS b
            FROM usage_log
            WHERE {where}
            GROUP BY 1, 2
            """,
            params,
        ).fetchall()

    totals = {"A": 0, "B": 0, "CDN": 0}
    bytes_by_class = {"A": 0, "B": 0, "CDN": 0}
    class_a_breakdown: dict[str, int] = {}
    class_b_breakdown: dict[str, int] = {}
    for r in grouped:
        cls, otype, c, b = r["operation_class"], r["operation_type"], int(r["c"] or 0), int(r["b"] or 0)
        if cls in totals:
            totals[cls] += c
            bytes_by_class[cls] += b
        if cls == "A":
            class_a_breakdown[otype] = c
        elif cls == "B":
            class_b_breakdown[otype] = c

    res_agg = {
        "total_class_a": totals["A"],
        "total_class_b": totals["B"],
        "total_cdn_downloads": totals["CDN"],
        "total_cdn_bytes": bytes_by_class["CDN"],
        "total_fos_bytes": bytes_by_class["A"] + bytes_by_class["B"],
        "class_a_breakdown": class_a_breakdown,
        "class_b_breakdown": class_b_breakdown,
    }

    return entries, total, res_agg


# ── Metadata retention / cleanup constants ────────────────────────────────────
# usage_log and ingested_files are append-only and unbounded by default.
# On a long-running deploy they grow without limit (witnessed: 5.7 GB
# metadata.db with 8.25M usage_log rows + 2.35M ingested_files rows). The
# UI doesn't need that history beyond a short window — Usage & Cost pages
# query a configurable window; Data Management shows recent files; cron_runs
# is a short audit trail. Trim by age; keep VACUUM gated to actual deletions
# because a no-op VACUUM still rewrites the whole file.

# Per-table retention windows (days). Override via cfg["metadata_retention"]
# per service. 0 (or negative) disables cleanup for that table / artefact.
#
# rollups_days is not a SQLite table but a per-hour parquet tree under
# ``<cache>/rollups/hour/field=X/hour=Y/``. The cleanup helper deletes
# hour-dirs older than this window. Default 90d gives broad dashboard
# query coverage while bounding disk; set to 0 to keep all history.
DEFAULT_METADATA_RETENTION = {
    "usage_log_days": 1,
    "ingested_files_days": 1,
    "cron_runs_days": 7,
    "rollups_days": 90,
}
