"""Source registration + FOS/CDN usage telemetry against the metadata Postgres database."""

from __future__ import annotations

import logging
import sys
import threading
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict

from backend.core.metadata import pg_pool as pg_connection
from backend.core.metadata.pg_pool import (
    close_all_connections as close_all_connections,
)
from backend.core.metadata.pg_pool import (
    get_con,
)
from backend.core.metadata.pg_pool import (
    get_con_readonly as open_readonly,
)
from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────────────
# SQLite-dialect table / index / trigger definitions; ``pg_schema`` rewrites them to
# Postgres DDL (see the module docstring).

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS usage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        service_id TEXT,
        operation_class TEXT,
        operation_type TEXT,
        url TEXT,
        status TEXT,
        duration_ms REAL,
        function_name TEXT,
        process_context TEXT,
        bytes BIGINT,
        count BIGINT NOT NULL DEFAULT 1
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_timestamp ON usage_log(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_usage_dedup ON usage_log(service_id, function_name, url)",
    "CREATE INDEX IF NOT EXISTS idx_usage_reconcile ON usage_log(service_id, operation_class, timestamp, function_name, count)",
    "CREATE INDEX IF NOT EXISTS idx_usage_process_context_ts ON usage_log(process_context, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_usage_service_ts ON usage_log(service_id, timestamp, operation_class, count, bytes)",
    """CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (
        service_id TEXT NOT NULL,
        hour TEXT NOT NULL,
        operation_class TEXT NOT NULL DEFAULT '',
        operation_type TEXT NOT NULL DEFAULT '',
        count BIGINT NOT NULL DEFAULT 0,
        bytes BIGINT NOT NULL DEFAULT 0,
        last_updated TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (service_id, hour, operation_class, operation_type)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_hourly_svc_hour ON usage_log_hourly_summary(service_id, hour)",
    """CREATE TABLE IF NOT EXISTS telemetry_queries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        page_load_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        engine TEXT NOT NULL,
        sql_query TEXT NOT NULL,
        duration_ms REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_queries_page ON telemetry_queries (page_load_id)",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_queries_ts ON telemetry_queries (timestamp)",
    """CREATE TABLE IF NOT EXISTS telemetry_sections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        page_load_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        section_name TEXT NOT NULL,
        duration_ms REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_sections_page ON telemetry_sections (page_load_id)",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_sections_ts ON telemetry_sections (timestamp)",
    # Bytes the Standard-mode cloud copy deleted from FOS (compaction, retention). FOS bills a deleted object for 30
    # days, so the Usage page needs them; rows older than the billing window are pruned on insert.
    """CREATE TABLE IF NOT EXISTS cloud_copy_deletions (
        service_id TEXT NOT NULL,
        deleted_at TEXT NOT NULL,
        bytes BIGINT NOT NULL DEFAULT 0,
        objects INTEGER NOT NULL DEFAULT 0
    )""",
    "CREATE INDEX IF NOT EXISTS idx_cloud_copy_deletions_svc ON cloud_copy_deletions(service_id, deleted_at)",
]


def _ul(service_id: str) -> pg_connection.PgConnectionWrapper:
    """Thread-local RW connection to the per-service usage_log database."""
    return get_con(service_id)


# ── sources ───────────────────────────────────────────────────────────────────


def register_source(service_id: str, name: str, config_json: str, table_name: str) -> None:
    """Idempotently register a source. Returns nothing (callers compute table_name themselves)."""
    con = get_con(service_id)
    con.execute(
        "INSERT OR IGNORE INTO sources (name, config, table_name) VALUES (?, ?, ?)",
        (name, config_json, table_name),
    )
    con.commit()


# ── usage_log ─────────────────────────────────────────────────────────────────


def _upsert_hourly_summary(
    con: pg_connection.PgConnectionWrapper,
    summary_map: dict[tuple[str, str, str, str], list[int]],
    now_iso: str,
) -> None:
    if not summary_map:
        return
    summary_rows = [
        (sid, hr, oc, ot, counts[0], counts[1], now_iso) for (sid, hr, oc, ot), counts in summary_map.items()
    ]
    con.executemany(
        "INSERT INTO usage_log_hourly_summary "
        "(service_id, hour, operation_class, operation_type, count, bytes, last_updated) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (service_id, hour, operation_class, operation_type) "
        "DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, "
        "              bytes = usage_log_hourly_summary.bytes + EXCLUDED.bytes, "
        "              last_updated = EXCLUDED.last_updated",
        summary_rows,
    )


def log_usage_calls(service_id: str, calls: list[dict], process_context: str | None = None) -> None:
    if not calls:
        return
    con = _ul(service_id)
    now = iso_z_now()
    rows = []
    summary_map: dict[tuple[str, str, str, str], list[int]] = {}
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
        normalized_op = op_type.replace("_", "")
        if svc == "FOS" and (
            normalized_op
            in (
                "PUTOBJECT",
                "POSTOBJECT",
                "COPYOBJECT",
                "LISTOBJECTSV2",
                "DELETEOBJECTS",
                "PUT",
                "POST",
                "COPY",
            )
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

        method = c.get("method")
        rows.append(
            (
                now,
                service_id,
                op_class,
                method,
                c.get("path"),
                str(c.get("status", "OK")),
                c.get("time_ms"),
                c.get("caller"),
                process_context,
                op_bytes,
            )
        )
        if len(now) >= 13 and service_id:
            hour = now[:13]
            key = (service_id, hour, op_class or "", method or "")
            if key not in summary_map:
                summary_map[key] = [0, 0]
            summary_map[key][0] += 1
            summary_map[key][1] += op_bytes or 0

    try:
        con.executemany(
            "INSERT INTO usage_log "
            "(timestamp, service_id, operation_class, operation_type, url, status, "
            " duration_ms, function_name, process_context, bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        _upsert_hourly_summary(con, summary_map, now)
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
    now_iso = iso_z_now()
    summary_map: dict[tuple[str, str, str, str], list[int]] = {}
    for c in calls:
        url = c.get("path")
        if not url or url in existing:
            continue
        ts = c.get("_timestamp_override") or now_iso
        op_type = c.get("method", "PUT_OBJECT")
        op_bytes = c.get("bytes")
        new_rows.append(
            (
                ts,
                service_id,
                "A",
                op_type,
                url,
                str(c.get("status", "OK")),
                0.0,
                c.get("caller", "fastly.edge"),
                c.get("process_context", "fastly:log_write"),
                op_bytes,
            )
        )
        if len(ts) >= 13 and service_id:
            hour = ts[:13]
            key = (service_id, hour, "A", op_type or "")
            if key not in summary_map:
                summary_map[key] = [0, 0]
            summary_map[key][0] += 1
            summary_map[key][1] += op_bytes or 0

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
        _upsert_hourly_summary(con, summary_map, now_iso)
        con.commit()
        return len(new_rows)
    except Exception as e:
        logger.error("[metadata_db] Synthetic usage log failed: %s", e)
        return 0


class _HourlyStatsRecordModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    hour_iso: Annotated[str, BeforeValidator(lambda v: v if isinstance(v, str) else "")] = ""
    class_a: Annotated[int, BeforeValidator(lambda v: int(v or 0))] = 0
    class_b: Annotated[int, BeforeValidator(lambda v: int(v or 0))] = 0


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
    for raw_rec in hourly_records:
        rec = _HourlyStatsRecordModel.model_validate(raw_rec)
        if not rec.hour_iso:
            continue
        start_dt = parse_iso_utc(rec.hour_iso)
        if start_dt is None:
            continue
        start_str = iso_z(start_dt)
        by_hour[start_str] = {
            "A": rec.class_a,
            "B": rec.class_b,
        }
        if earliest is None or start_dt < earliest:
            earliest = start_dt
        if latest is None or start_dt > latest:
            latest = start_dt

    if not by_hour or earliest is None or latest is None:
        return 0

    window_start = iso_z(earliest)
    window_end = iso_z(latest + timedelta(hours=1))

    # Single scan covering both classes — substr() truncates the ISO
    # timestamp to its hour prefix; SQLite groups by string equality,
    # which works because we write all rows in the same "%Y-%m-%dT%H:%M:%SZ"
    # format. The supporting index is idx_usage_reconcile (service_id,
    # operation_class, timestamp, function_name, count), which functions as a
    # fully covering index for sub-millisecond query evaluation.
    local_sums: dict[tuple[str, str], int] = {}
    for r in con.execute(
        """
        SELECT operation_class, hour, sum(count)
        FROM usage_log_hourly_summary
        WHERE service_id = ? AND operation_class IN ('A', 'B')
          AND hour >= substr(?, 1, 13) AND hour < substr(?, 1, 13)
          AND operation_type NOT LIKE ?
        GROUP BY operation_class, hour
        """,
        (service_id, window_start, window_end, "RECONCILE_%"),
    ):
        local_sums[(r[0], r[1])] = int(r[2] or 0)

    # Find previous reconciliation rows before deleting to adjust the hourly summary
    cur = con.execute(
        """
        SELECT substring(timestamp, 1, 13) AS hour, operation_class, operation_type,
               SUM(count) AS c, SUM(COALESCE(bytes, 0)) AS b
        FROM usage_log
        WHERE service_id = ? AND operation_class IN ('A', 'B')
          AND timestamp >= ? AND timestamp < ?
          AND function_name = 'fastly.reconciliation'
        GROUP BY substring(timestamp, 1, 13), operation_class, operation_type
        """,
        (service_id, window_start, window_end),
    )
    deletes_to_adjust = cur.fetchall()

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

    now_iso = iso_z_now()
    if deletes_to_adjust:
        adjust_params = [
            (
                int(r["c"] or 0),
                int(r["b"] or 0),
                now_iso,
                service_id,
                r["hour"],
                r["operation_class"],
                r["operation_type"],
            )
            for r in deletes_to_adjust
        ]
        con.executemany(
            """
            UPDATE usage_log_hourly_summary
            SET count = count - ?,
                bytes = bytes - ?,
                last_updated = ?
            WHERE service_id = ?
              AND hour = ?
              AND operation_class = ?
              AND operation_type = ?
            """,
            adjust_params,
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
        summary_map: dict[tuple[str, str, str, str], list[int]] = {}
        for r in insert_rows:
            ts = r[0]
            hr = ts[:13]
            op_class = r[2]
            op_type = r[3]
            gap_count = r[10]
            key = (service_id, hr, op_class, op_type)
            if key not in summary_map:
                summary_map[key] = [0, 0]
            summary_map[key][0] += gap_count

        _upsert_hourly_summary(con, summary_map, now_iso)
    con.commit()
    return written


def purge_usage_log(service_id: str, retention_days: int) -> None:
    if retention_days <= 0:
        return
    flush_telemetry_buffer(only_service=service_id)
    con = _ul(service_id)
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con.execute("DELETE FROM usage_log WHERE service_id = ? AND timestamp < ?", (service_id, cutoff))
    con.execute("DELETE FROM telemetry_queries WHERE timestamp < ?", (cutoff,))
    con.execute("DELETE FROM telemetry_sections WHERE timestamp < ?", (cutoff,))
    con.commit()


_FOS_MIN_BILLING_DAYS = 30


def record_cloud_copy_deletion(service_id: str, deleted_bytes: int, objects: int) -> None:
    """Remember bytes the cloud copy deleted from FOS; they keep billing for the 30-day minimum."""
    if objects <= 0:
        return
    con = _ul(service_id)
    now = datetime.now(UTC)
    con.execute(
        "INSERT INTO cloud_copy_deletions (service_id, deleted_at, bytes, objects) VALUES (?, ?, ?, ?)",
        (service_id, iso_z(now), int(deleted_bytes), int(objects)),
    )
    con.execute(
        "DELETE FROM cloud_copy_deletions WHERE service_id = ? AND deleted_at < ?",
        (service_id, iso_z(now - timedelta(days=_FOS_MIN_BILLING_DAYS + 5))),
    )
    con.commit()


def cloud_copy_deleted_bytes_in_billing_window(service_id: str) -> int:
    """Bytes the cloud copy deleted in the last 30 days: deleted, but still billed."""
    con = _ul(service_id)
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=_FOS_MIN_BILLING_DAYS))
    row = con.execute(
        "SELECT COALESCE(SUM(bytes), 0) FROM cloud_copy_deletions WHERE service_id = ? AND deleted_at >= ?",
        (service_id, cutoff),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def clear_usage_log(service_id: str) -> None:
    con = _ul(service_id)
    con.execute("DELETE FROM usage_log WHERE service_id = ?", (service_id,))
    con.execute("DELETE FROM usage_log_hourly_summary WHERE service_id = ?", (service_id,))
    con.execute("DELETE FROM cloud_copy_deletions WHERE service_id = ?", (service_id,))
    con.commit()


def _usage_class_predicate(usage_type: str) -> tuple[str, list]:
    """Map a usage_type filter to a SQL ``operation_class`` predicate + params.

    Single source of truth for the usage_type → operation_class mapping that
    ``_query_usage_log_aggregate_rollup``, ``get_usage_logs``, and
    ``iter_usage_logs_chunks`` each used to inline. Returns the bare predicate
    (no leading ``AND``) and any positional params; callers splice it into
    their WHERE/conditions list. Empty usage_type → ``("", [])``.
    """
    if not usage_type:
        return "", []
    fixed = {
        "CDN": "operation_class = 'CDN'",
        "FOS-A": "operation_class = 'A'",
        "FOS-B": "operation_class = 'B'",
        "FOS": "operation_class IN ('A', 'B')",
    }
    if usage_type in fixed:
        return fixed[usage_type], []
    return "operation_class = ?", [usage_type]


def _query_usage_log_aggregate_rollup(
    con: Any,
    service_id: str,
    start: str,
    end: str,
    usage_type: str,
) -> list[Any]:
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

    pred, pred_params = _usage_class_predicate(usage_type)
    class_filter = f"AND {pred}" if pred else ""
    class_params: list = pred_params

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
        ) AS combined
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


def _build_usage_log_where(
    service_id: str,
    start: str,
    end: str,
    usage_type: str,
    process_context: str,
    operation_type: str,
) -> tuple[str, list[Any]]:
    conditions = ["service_id = ?", "timestamp >= ?", "timestamp <= ?"]
    params: list[Any] = [service_id, start, end]

    pred, pred_params = _usage_class_predicate(usage_type)
    if pred:
        conditions.append(pred)
        params.extend(pred_params)

    if process_context:
        conditions.append("process_context LIKE ?")
        params.append(f"%{process_context}%")
    if operation_type:
        conditions.append("operation_type LIKE ?")
        params.append(f"%{operation_type}%")

    return " AND ".join(conditions), params


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
    where, params = _build_usage_log_where(service_id, start, end, usage_type, process_context, operation_type)

    # Open a short-lived RO connection so a slow paginated SELECT can't
    # queue behind the cron writer's WAL commit.
    rollup_eligible = not process_context and not operation_type

    try:
        con = open_readonly(service_id)
    except Exception:
        # File doesn't exist yet (first run before any log_usage_calls).
        return (
            [],
            0,
            {
                "total_class_a": 0,
                "total_class_b": 0,
                "total_cdn_downloads": 0,
                "total_cdn_bytes": 0,
                "total_fos_bytes": 0,
                "class_a_breakdown": {},
                "class_b_breakdown": {},
            },
        )

    try:
        # Bare paginated SELECT against the (service_id, timestamp DESC)
        # index — no window function, no COUNT(*) OVER () (which forced a
        # full filtered scan of the underlying range to materialise the
        # count column on every page request). The total count comes from
        # ``grouped`` below — the same aggregate path already runs per
        # request and its sum-of-counts is the row total. Saves ~4 s p95
        # on the page query at 500-row windows.
        offset = (page - 1) * page_size
        raw_rows = con.execute(
            f"SELECT * FROM usage_log WHERE {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()
        entries = [dict(r) for r in raw_rows]

        # Aggregate path: prefer the usage_log_hourly_summary rollup when only the
        # service+timestamp predicates are active (the common admin-page case). The
        # rollup is maintained incrementally by trg_usage_log_summary_insert, so
        # it's always consistent — no scheduler needed. We can only use it when no
        # process_context / operation_type LIKE filters are present (the rollup
        # doesn't carry those columns); the operation_class filter IS supported
        # because the rollup stores it as a normalised key.
        if rollup_eligible:
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
    finally:
        try:
            con.close()
        except Exception:
            logger.debug("get_usage_logs: con.close failed (ignored)", exc_info=True)

    totals = {"A": 0, "B": 0, "CDN": 0}
    bytes_by_class = {"A": 0, "B": 0, "CDN": 0}
    class_a_breakdown: dict[str, int] = {}
    class_b_breakdown: dict[str, int] = {}
    total = 0
    for r in grouped:
        cls = r["operation_class"]
        otype = r["operation_type"]
        c = int(r["c"] or 0)
        b = int(r["b"] or 0)
        total += c
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


def iter_usage_logs_chunks(
    service_id: str,
    start: str,
    end: str,
    *,
    usage_type: str = "",
    process_context: str = "",
    operation_type: str = "",
    chunk_size: int = 5_000,
    max_rows: int = 100_000,
):
    """Yield row chunks from usage_log via keyset (seek) pagination.

    Used by the CSV export — the UI's paginated reader still calls
    ``get_usage_logs`` with ``page=N``. Seek pagination avoids the
    ``OFFSET N`` scan-skip cost that linearly grows with page number, so
    a 100k-row export across 20 chunks costs ~constant per chunk
    instead of N×(N+1)/2 cumulative scan work.

    The cursor is ``(timestamp DESC, id DESC)`` — both columns are
    needed because ``timestamp`` isn't unique (multiple rows per second
    on burst writes) and the id tiebreaker keeps the ordering stable
    across chunk boundaries. ``idx_usage_service_ts`` covers the
    WHERE+ORDER predicate; the id comparison hits the integer primary
    key.

    Stops early when fewer than ``chunk_size`` rows come back (final
    chunk) or when the running total hits ``max_rows``. Yields lists of
    dicts; callers iterate them and stream out without materialising
    the whole result.
    """
    where, params = _build_usage_log_where(service_id, start, end, usage_type, process_context, operation_type)

    try:
        con = open_readonly(service_id)
    except Exception:
        # File doesn't exist (first run before any log_usage_calls).
        return

    try:
        last_ts: str | None = None
        last_id: int | None = None
        emitted = 0
        while emitted < max_rows:
            limit = min(chunk_size, max_rows - emitted)
            if last_ts is None:
                sql = f"SELECT * FROM usage_log WHERE {where} ORDER BY timestamp DESC, id DESC LIMIT ?"
                bind = [*params, limit]
            else:
                sql = (
                    f"SELECT * FROM usage_log WHERE {where} "
                    "AND (timestamp, id) < (?, ?) "
                    "ORDER BY timestamp DESC, id DESC LIMIT ?"
                )
                bind = [*params, last_ts, last_id, limit]
            raw_rows = con.execute(sql, bind).fetchall()
            if not raw_rows:
                return
            chunk = [dict(r) for r in raw_rows]
            yield chunk
            emitted += len(chunk)
            last_ts = chunk[-1]["timestamp"]
            last_id = chunk[-1]["id"]
            if len(chunk) < limit:
                # Short chunk == no more rows in the window.
                return
    finally:
        try:
            con.close()
        except Exception:
            logger.debug("iter_usage_logs_chunks: con.close failed (ignored)", exc_info=True)


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
    # Persistent slow-query history. 7 days matches cron_runs — both
    # exist for incident-debug "what happened last week?" use cases.
    # Set to 0 to disable persistence at the cleanup layer (the
    # query_registry persistence threshold is the other knob, via
    # QUERY_REGISTRY_PERSIST_THRESHOLD_MS).
    "slow_queries_days": 7,
}


_TELEMETRY_FLUSH_INTERVAL_S = 2.0
_TELEMETRY_FLUSH_BATCH_SIZE = 50
_telemetry_lock = threading.Lock()
_telemetry_query_buf: dict[str, list[tuple[str, str, str, str, str, float]]] = {}
_telemetry_section_buf: dict[str, list[tuple[str, str, str, str, float]]] = {}
_telemetry_flush_timer: threading.Timer | None = None
_telemetry_flush_in_progress = False


def _schedule_telemetry_flush() -> None:
    global _telemetry_flush_timer
    if _telemetry_flush_timer is not None:
        return
    _telemetry_flush_timer = threading.Timer(_TELEMETRY_FLUSH_INTERVAL_S, flush_telemetry_buffer)
    setattr(sys.modules[__name__], "_telemetry_flush_timer", _telemetry_flush_timer)
    _telemetry_flush_timer.daemon = True
    _telemetry_flush_timer.start()


def flush_telemetry_buffer(*, only_service: str | None = None) -> None:
    """Flush buffered telemetry query and section rows in a single batch per service."""
    global _telemetry_flush_timer, _telemetry_flush_in_progress
    with _telemetry_lock:
        if only_service is not None:
            q_rows = _telemetry_query_buf.pop(only_service, None)
            s_rows = _telemetry_section_buf.pop(only_service, None)
            if not q_rows and not s_rows:
                return
            pending_q = {only_service: q_rows} if q_rows else {}
            pending_s = {only_service: s_rows} if s_rows else {}
        else:
            _telemetry_flush_timer = None
            setattr(sys.modules[__name__], "_telemetry_flush_timer", None)
            if not _telemetry_query_buf and not _telemetry_section_buf:
                _telemetry_flush_in_progress = False
                setattr(sys.modules[__name__], "_telemetry_flush_in_progress", False)
                return
            pending_q = dict(_telemetry_query_buf)
            pending_s = dict(_telemetry_section_buf)
            _telemetry_query_buf.clear()
            _telemetry_section_buf.clear()

    services = set(pending_q.keys()) | set(pending_s.keys())
    try:
        for sid in services:
            try:
                con = _ul(sid)
                q_batch = pending_q.get(sid)
                if q_batch:
                    con.executemany(
                        "INSERT INTO telemetry_queries (timestamp, page_load_id, request_id, engine, sql_query, duration_ms) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        q_batch,
                    )
                s_batch = pending_s.get(sid)
                if s_batch:
                    con.executemany(
                        "INSERT INTO telemetry_sections (timestamp, page_load_id, request_id, section_name, duration_ms) "
                        "VALUES (?, ?, ?, ?, ?)",
                        s_batch,
                    )
                con.commit()
            except Exception:
                logger.debug("flush_telemetry_buffer: commit failed for %s (ignored)", sid, exc_info=True)
    finally:
        if only_service is None:
            pg_connection.release_pg_thread_connection()
            with _telemetry_lock:
                _telemetry_flush_in_progress = False
                setattr(sys.modules[__name__], "_telemetry_flush_in_progress", False)
                if _telemetry_query_buf or _telemetry_section_buf:
                    _schedule_telemetry_flush()


def _enqueue_telemetry_row(target_buf: dict[str, list[Any]], service_id: str, row: tuple[Any, ...]) -> None:
    global _telemetry_flush_in_progress
    with _telemetry_lock:
        target_buf.setdefault(service_id, []).append(row)
        total = sum(len(v) for v in _telemetry_query_buf.values()) + sum(
            len(v) for v in _telemetry_section_buf.values()
        )
        if total >= _TELEMETRY_FLUSH_BATCH_SIZE and not _telemetry_flush_in_progress:
            _telemetry_flush_in_progress = True
            setattr(sys.modules[__name__], "_telemetry_flush_in_progress", True)
            needs_immediate = True
        else:
            needs_immediate = False
            if not _telemetry_flush_in_progress:
                _schedule_telemetry_flush()
    if needs_immediate:
        threading.Thread(target=flush_telemetry_buffer, daemon=True).start()


def log_telemetry_query(
    service_id: str,
    page_load_id: str,
    request_id: str,
    engine: str,
    sql: str,
    duration_ms: float,
) -> None:
    _enqueue_telemetry_row(
        _telemetry_query_buf,
        service_id,
        (iso_z_now(), page_load_id, request_id, engine, sql, duration_ms),
    )


def log_telemetry_section(
    service_id: str,
    page_load_id: str,
    request_id: str,
    section_name: str,
    duration_ms: float,
) -> None:
    _enqueue_telemetry_row(
        _telemetry_section_buf,
        service_id,
        (iso_z_now(), page_load_id, request_id, section_name, duration_ms),
    )


def get_page_telemetry(service_id: str, page_load_id: str) -> dict:
    flush_telemetry_buffer(only_service=service_id)
    con = _ul(service_id)

    # 1. Fetch queries
    cur = con.execute(
        "SELECT engine, sql_query, duration_ms FROM telemetry_queries WHERE page_load_id = ? ORDER BY timestamp ASC",
        (page_load_id,),
    )
    queries = [dict(r) for r in cur.fetchall()]

    # 2. Fetch sections
    cur = con.execute(
        "SELECT section_name, duration_ms FROM telemetry_sections WHERE page_load_id = ? ORDER BY timestamp ASC",
        (page_load_id,),
    )
    sections = [dict(r) for r in cur.fetchall()]

    # 3. Fetch proxy FOS/CDN network calls (mapped from usage_log process_context containing '#page_load_id')
    cur = con.execute(
        "SELECT operation_type, url, status, duration_ms, function_name, bytes, operation_class "
        "FROM usage_log "
        "WHERE process_context LIKE ? ORDER BY timestamp ASC",
        (f"%#{page_load_id}",),
    )
    calls = [
        {
            "service": "CDN" if r["operation_class"] == "CDN" else "FOS",
            "method": r["operation_type"],
            "path": r["url"],
            "status": r["status"],
            "time_ms": r["duration_ms"],
            "caller": r["function_name"],
            "bytes": r["bytes"],
        }
        for r in cur.fetchall()
    ]

    return {
        "queries": queries,
        "sections": sections,
        "calls": calls,
    }
