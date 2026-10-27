"""Source registration + FOS/CDN usage telemetry against the metadata Postgres database.

Covers the ``sources``, ``usage_log``, and ``usage_log_hourly_summary``
tables. The hourly summary is the rolled-up backstop that lets the admin
Usage Log page render against millions of raw usage_log rows without
re-scanning the full table on every request.
"""

from __future__ import annotations

import atexit
import logging
import os
import sys
import threading
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict

from backend.core.metadata import pg_connection
from backend.core.metadata.base import (
    _DATA_DIR,
    _validate_service_id_or_raise,
    get_con,
)
from backend.core.metadata.base import (
    close_all_connections as close_all_connections,
)
from backend.core.metadata.base import (
    get_con_readonly as open_readonly,
)
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


def db_path(service_id: str) -> str:
    """Absolute path to the per-service usage_log SQLite file (legacy helper)."""
    _validate_service_id_or_raise(service_id)
    return os.path.join(_DATA_DIR, f"{service_id}.usage_log.db")


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
        bytes INTEGER,
        count INTEGER NOT NULL DEFAULT 1
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
        count INTEGER NOT NULL DEFAULT 0,
        bytes INTEGER NOT NULL DEFAULT 0,
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
        try:
            start_dt = datetime.strptime(rec.hour_iso.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
        except (ValueError, AttributeError):
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
            if not _telemetry_query_buf and not _telemetry_section_buf:
                _telemetry_flush_in_progress = False
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


# ── Slow Queries (folded from backend.core.metadata.slow_queries) ────────────

from backend.core.metadata.base import release_thread_connection  # noqa: E402

_INSERT_SQL = """
    INSERT INTO slow_queries (
        query_id, db_type, service_id, started_at_utc, ended_at_utc,
        duration_ms, outcome, sql_preview, sql_full, sql_len,
        attr_kind, attr_label, attr_principal_id,
        attr_caller_qualname, attr_caller_file,
        attr_request_path, attr_request_id,
        attr_cron_job, attr_cron_run_id, attr_pool_slot,
        error_type, error_message, peak_memory_mb
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?
    )
"""

_FLUSH_INTERVAL_S = 5.0
_FLUSH_BATCH_SIZE = 50

# Keyed by service_id → list of param dicts
_buffer: dict[str, list[dict[str, Any]]] = {}
_buffer_lock = threading.Lock()
_flush_timer: threading.Timer | None = None
_flush_in_progress = False


_OrUnknown = Annotated[Any, BeforeValidator(lambda v: v or "unknown")]
_OrSystem = Annotated[Any, BeforeValidator(lambda v: v or "system")]
_OrZeroFloat = Annotated[Any, BeforeValidator(lambda v: v or 0.0)]
_OrZeroInt = Annotated[Any, BeforeValidator(lambda v: v or 0)]
_OrEmptyStr = Annotated[Any, BeforeValidator(lambda v: v or "")]
_OptStr = Annotated[str | None, BeforeValidator(lambda v: str(v) if v is not None else None)]


class _SlowQueryRowModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query_id: Any
    db_type: _OrUnknown = "unknown"
    service_id: Any = None
    started_at_utc: _OrZeroFloat = 0.0
    ended_at_utc: _OrZeroFloat = 0.0
    duration_ms: _OrZeroFloat = 0.0
    outcome: _OrUnknown = "unknown"
    sql_preview: _OrEmptyStr = ""
    sql_full: Any = None
    sql_len: _OrZeroInt = 0
    attr_kind: _OrSystem = "system"
    attr_label: _OrEmptyStr = ""
    attr_principal_id: Any = None
    attr_caller_qualname: _OrEmptyStr = ""
    attr_caller_file: _OrEmptyStr = ""
    attr_request_path: Any = None
    attr_request_id: Any = None
    attr_cron_job: Any = None
    attr_cron_run_id: _OptStr = None
    attr_pool_slot: Any = None
    error_type: Any = None
    error_message: Any = None
    peak_memory_mb: Any = None


def _normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    return _SlowQueryRowModel.model_validate(row).model_dump()


def _insert_params(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(_SlowQueryRowModel.model_validate(row).model_dump().values())


def _schedule_flush() -> None:
    global _flush_timer
    if _flush_timer is not None:
        return
    _flush_timer = threading.Timer(_FLUSH_INTERVAL_S, _flush_all)
    _flush_timer.daemon = True
    _flush_timer.start()


def _flush_all(*, only_service: str | None = None) -> None:
    """Flush buffered slow-query rows to PostgreSQL (METADATA_DSN).

    If *only_service* is given, flush just that service's buffer (used by
    read functions to ensure consistency). Otherwise flush everything.
    """
    global _flush_timer, _flush_in_progress
    with _buffer_lock:
        if only_service is not None:
            rows = _buffer.pop(only_service, None)
            if rows is None:
                return
            pending = {only_service: rows}
        else:
            _flush_timer = None
            if not _buffer:
                _flush_in_progress = False
                return
            pending = dict(_buffer)
            _buffer.clear()

    try:
        for service_id, rows in pending.items():
            try:
                con = get_con(service_id)
                con.executemany(_INSERT_SQL, [_insert_params(_normalise_row(row)) for row in rows])
                con.commit()
            except Exception:
                logger.debug("_flush_all: con.commit failed (ignored)", exc_info=True)
            finally:
                release_thread_connection()
    finally:
        if only_service is None:
            with _buffer_lock:
                _flush_in_progress = False
                if _buffer:
                    _schedule_flush()


def insert_slow_query(service_id: str, row: dict[str, Any]) -> None:
    """Buffer a completed-query row for batch insertion."""
    global _flush_in_progress
    params = _normalise_row(row)
    with _buffer_lock:
        _buffer.setdefault(service_id, []).append(params)
        total = sum(len(v) for v in _buffer.values())
        if total >= _FLUSH_BATCH_SIZE and not _flush_in_progress:
            _flush_in_progress = True
            needs_immediate = True
        else:
            needs_immediate = False
            if not _flush_in_progress:
                _schedule_flush()

    if needs_immediate:
        threading.Thread(target=_flush_all, daemon=True).start()


def flush_slow_query_buffer() -> None:
    """Force-flush the buffer. Called at shutdown."""
    _flush_all()


atexit.register(flush_slow_query_buffer)


def list_slow_queries(
    service_id: str,
    *,
    since_utc: float,
    until_utc: float | None = None,
    threshold_ms: float = 0.0,
    kind: str | None = None,
    db_type: str | None = None,
    limit: int = 200,
    sort_by_duration: bool = False,
) -> list[dict[str, Any]]:
    """Return slow-query rows for a service in a time window."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    sql = ["SELECT * FROM slow_queries WHERE service_id = ? AND started_at_utc >= ?"]
    args: list[Any] = [service_id, since_utc]
    if until_utc is not None:
        sql.append("AND started_at_utc < ?")
        args.append(until_utc)
    if threshold_ms > 0:
        sql.append("AND duration_ms >= ?")
        args.append(threshold_ms)
    if kind is not None:
        sql.append("AND attr_kind = ?")
        args.append(kind)
    if db_type is not None:
        sql.append("AND db_type = ?")
        args.append(db_type)
    sql.append("ORDER BY duration_ms DESC" if sort_by_duration else "ORDER BY started_at_utc DESC")
    sql.append("LIMIT ?")
    args.append(limit)
    rows = con.execute(" ".join(sql), args).fetchall()
    return [dict(r) for r in rows]


def count_slow_queries(
    service_id: str,
    *,
    since_utc: float,
    threshold_ms: float = 0.0,
) -> int:
    """Cheap count of persisted slow queries in a window."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    row = con.execute(
        "SELECT COUNT(*) AS n FROM slow_queries WHERE service_id = ? AND started_at_utc >= ? AND duration_ms >= ?",
        (service_id, since_utc, threshold_ms),
    ).fetchone()
    return int(row["n"] or 0)


def clear_slow_queries(service_id: str) -> int:
    """Delete all persisted slow-query rows for *service_id*."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    cur = con.execute(
        "DELETE FROM slow_queries WHERE service_id = ?",
        (service_id,),
    )
    con.commit()
    return int(cur.rowcount or 0)


sys.modules["backend.core.metadata.slow_queries"] = sys.modules[__name__]
_meta_pkg = sys.modules.get("backend.core.metadata")
if _meta_pkg is not None:
    setattr(_meta_pkg, "slow_queries", sys.modules[__name__])
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES["backend.core.metadata.slow_queries"] = "backend.core.metadata.usage_log"

# ── Cron & ingest job run tracking (folded from cron_log.py) ──

import json
import logging
import statistics
from typing import Annotated, Any

import psycopg
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter, ValidationError

from backend.core.metadata.base import get_con_readonly
from backend.utils.date_utils import parse_iso_utc


class CronLeaseHeldError(RuntimeError):
    """The (service_id, task) lease is held by another run; callers should skip, not fail."""


class _OutcomeCountersModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    valid_records: int = Field(default=0, ge=0)
    malformed_records: int = Field(default=0, ge=0)
    corrupt_containers: int = Field(default=0, ge=0)
    quarantine_capture_failures: int = Field(default=0, ge=0)
    source_delete_failures: int = Field(default=0, ge=0)
    cap_evictions: int = Field(default=0, ge=0)
    objects_processed: int = Field(default=0, ge=0)
    objects_successful: int = Field(default=0, ge=0)
    objects_partial: int = Field(default=0, ge=0)
    objects_failed: int = Field(default=0, ge=0)


OUTCOME_COUNTER_KEYS = tuple(_OutcomeCountersModel.model_fields.keys())


def _normalize_outcome_counters(counters: dict[str, int] | None) -> dict[str, int]:
    values = counters or {}
    unknown = set(values) - set(OUTCOME_COUNTER_KEYS)
    if unknown:
        raise ValueError(f"unknown ingestion outcome counter(s): {', '.join(sorted(unknown))}")
    try:
        return _OutcomeCountersModel.model_validate(values).model_dump()
    except ValidationError as exc:
        loc = exc.errors()[0].get("loc", ("counter",))[0]
        raise ValueError(f"ingestion outcome counter {loc!r} must be a non-negative integer") from exc


def _decode_outcome_counters(raw: str | None) -> dict[str, int]:
    if not raw:
        return _normalize_outcome_counters(None)
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeError("invalid outcome_counters JSON in metadata") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("outcome_counters metadata must be a JSON object")
    try:
        return _normalize_outcome_counters(parsed)
    except ValueError as exc:
        raise RuntimeError("invalid outcome_counters values in metadata") from exc


def _begin_metadata_write(con: Any) -> None:
    con.execute("BEGIN")


def _lock_cron_run(con: Any, service_id: str, run_id: int) -> None:
    con.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))",
        (service_id, run_id),
    )


def _clear_cron_schedule_cache() -> None:
    try:
        from backend.utils.bounded_cache import CacheRegistry

        CacheRegistry.clear("services.core._cron_schedule_cache")
    except Exception:
        logger.debug("cron_log: CacheRegistry.clear failed (ignored)", exc_info=True)


def _publish_cron_run_change(
    service_id: str,
    run_id: int,
    task: str,
    status: str,
    *,
    started_at: str | None = None,
    duration_s: float | None = None,
    ts: str | None = None,
) -> None:
    try:
        from backend.cron_runs_publisher import publisher

        payload: dict[str, Any] = {
            "event": "cron_run_changed",
            "run_id": run_id,
            "task": task,
            "status": status,
            "ts": ts if ts is not None else iso_z_now(),
        }
        if started_at is not None:
            payload["started_at"] = started_at
        if duration_s is not None:
            payload["duration_s"] = duration_s
        publisher.publish(service_id, payload)
    except Exception:
        logger.exception("[cron_log] cron-runs SSE publish failed for %s/%s", service_id, task)


def start_cron_run(service_id: str, task: str) -> int:
    import time

    con = get_con(service_id)
    started_at = iso_z_now()
    now = time.time()

    def _write() -> int:
        # Reap orphans based on heartbeat.
        # Any cron_runs row that is 'running' but has no active lease is an
        # orphan. Idempotent under concurrent callers (SQLite same-thread;
        # Postgres multi-pod): a second UPDATE hitting the same rows after
        # the first already flipped them just matches zero rows.
        con.execute(
            """UPDATE cron_runs SET status = 'error',
               error_message = COALESCE(error_message, 'Process interrupted (heartbeat lost)')
               WHERE service_id = ? AND task = ? AND status = 'running'
                 AND NOT EXISTS (
                     SELECT 1 FROM job_runs
                     WHERE service_id = cron_runs.service_id
                       AND job_name = cron_runs.task
                       AND status = 'running'
                       AND (heartbeat_at + lease_ttl_s) >= ?
                 )""",
            (service_id, task, now),
        )

        # Also clean up expired job_runs — reaping an EXPIRED lease first is
        # what makes the atomic insert below correct: by the time it runs,
        # any row still literally status='running' is non-expired (had it
        # expired, this statement would already have reaped it), so the
        # unique-index conflict below means a genuinely active lease, not a
        # stale one blocking a legitimate new run.
        con.execute(
            """UPDATE job_runs SET status = 'reaped'
               WHERE service_id = ? AND job_name = ? AND status = 'running'
                 AND (heartbeat_at + lease_ttl_s) < ?""",
            (service_id, task, now),
        )

        # Acquire the lease atomically. idx_job_runs_running_lease (a unique
        # index on (service_id, job_name) WHERE status='running') is the
        # conflict target — ON CONFLICT DO NOTHING means at most one of any
        # number of concurrent callers gets rowcount=1; everyone else gets
        # rowcount=0 and must back off. This replaces a check-then-insert
        # (SELECT count, then INSERT) that only stayed race-free because a
        # single SQLite writer connection serializes both statements inside
        # one transaction — a guarantee autocommit-mode Postgres does NOT
        # give multiple pods racing the same (service_id, task).
        lease_ttl_s = 60
        lease_cur = con.execute(
            "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
            "VALUES (?, ?, ?, ?, ?, 'running') "
            "ON CONFLICT (service_id, job_name) WHERE status = 'running' DO NOTHING",
            (service_id, task, now, now, lease_ttl_s),
        )
        if not lease_cur.rowcount:
            con.commit()
            raise CronLeaseHeldError(f"Task '{task}' is already running for this service.")

        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, ?, 0.0, 'running', '[]')",
            (service_id, task, started_at),
        )
        con.commit()
        return int(cur.lastrowid or 0)

    # Run the whole reap→guard→insert unit so a converging cron minute
    # can't crash the tick.
    new_id = _write()

    _clear_cron_schedule_cache()

    # Tickle any connected /api/cron-runs/stream subscribers that a run
    # just started. Same defensive contract as the sync-status publish
    # hook (backend/cron/jobs/sync.py): broad except so a publish
    # failure never blocks cron bookkeeping.
    _publish_cron_run_change(
        service_id,
        new_id,
        task,
        "running",
        started_at=started_at,
        duration_s=0.0,
        ts=started_at,
    )

    return new_id


def log_cron_run(
    service_id: str,
    task: str,
    duration_s: float,
    status: str,
    *,
    error_message: str | None = None,
    files_downloaded: int = 0,
    files_deleted_fos: int = 0,
    rows_ingested: int = 0,
    corrupt_rows: int = 0,
    parquet_files_created: int = 0,
    parquet_files_optimized: int = 0,
    parquet_keys: list | None = None,
    outcome_counters: dict[str, int] | None = None,
    summary: str | None = None,
    log_output: str | None = None,
    run_id: int | None = None,
) -> None:
    """Update an existing cron_run row by id, or insert a new completed one.

    When ``run_id`` is provided (the common case — start_cron_run created the
    row), this UPDATEs in place. Otherwise INSERTs a fresh terminal row
    (used by paths that didn't go through start_cron_run, e.g. retries).
    """
    con = get_con(service_id)
    started_at = iso_z(datetime.now(UTC) - timedelta(seconds=max(duration_s, 0)))
    keys_json = json.dumps(parquet_keys or [])
    outcome_counters = _normalize_outcome_counters(outcome_counters) if outcome_counters is not None else None
    final_status = status
    row_started_at = started_at

    def _write() -> None:
        nonlocal final_status, row_started_at
        import time

        final_rows_ingested = rows_ingested
        final_corrupt_rows = corrupt_rows
        _begin_metadata_write(con)
        try:
            if run_id is not None:
                _lock_cron_run(con, service_id, run_id)
                lock_clause = " FOR UPDATE"
                existing = con.execute(
                    "SELECT started_at, status, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ?"
                    + lock_clause,
                    (run_id, service_id),
                ).fetchone()
                if existing and existing["started_at"]:
                    row_started_at = str(existing["started_at"])
                existing_raw_counters = existing["outcome_counters"] if existing else None
                has_existing_counters = bool(existing_raw_counters and json.loads(existing_raw_counters))
                existing_counters = _decode_outcome_counters(existing_raw_counters) if has_existing_counters else None
                current_status = existing["status"] if existing else None
                if outcome_counters is None:
                    merged_counters = existing_counters
                elif existing_counters is not None and current_status == "running":
                    merged_counters = {
                        key: existing_counters[key] + outcome_counters[key] for key in OUTCOME_COUNTER_KEYS
                    }
                else:
                    merged_counters = existing_counters or outcome_counters
                final_status = "error" if current_status == "error" else status
            else:
                merged_counters = outcome_counters

            now = time.time()
            con.execute(
                "UPDATE job_runs SET status = ?, finished_at = ? "
                "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                (final_status, now, service_id, task),
            )
            if run_id is not None:
                persisted_counters = merged_counters
                if persisted_counters is not None:
                    final_rows_ingested = persisted_counters["valid_records"]
                    final_corrupt_rows = (
                        persisted_counters["malformed_records"] + persisted_counters["corrupt_containers"]
                    )
                con.execute(
                    """UPDATE cron_runs SET
                        duration_s = ?, status = ?, error_message = COALESCE(error_message, ?),
                        files_downloaded = ?, files_deleted_fos = ?, rows_ingested = ?, corrupt_rows = ?,
                        parquet_files_created = ?, parquet_files_optimized = ?,
                        parquet_keys = ?, outcome_counters = ?, summary = ?, log_output = ?
                       WHERE id = ? AND service_id = ?""",
                    (
                        duration_s,
                        final_status,
                        error_message,
                        files_downloaded,
                        files_deleted_fos,
                        final_rows_ingested,
                        final_corrupt_rows,
                        parquet_files_created,
                        parquet_files_optimized,
                        keys_json,
                        json.dumps(persisted_counters or {}),
                        summary,
                        log_output,
                        run_id,
                        service_id,
                    ),
                )
            else:
                con.execute(
                    """INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, error_message,
                        files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                        parquet_files_created, parquet_files_optimized, parquet_keys, outcome_counters, summary, log_output)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        service_id,
                        task,
                        started_at,
                        duration_s,
                        status,
                        error_message,
                        files_downloaded,
                        files_deleted_fos,
                        final_rows_ingested,
                        final_corrupt_rows,
                        parquet_files_created,
                        parquet_files_optimized,
                        keys_json,
                        json.dumps(outcome_counters or {}),
                        summary,
                        log_output,
                    ),
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    # A leaked terminal write is worse than a leaked start (the row stays
    # 'running' and blocks the task until the orphan cutoff — the 2026-06-19
    # incident).
    _write()

    _clear_cron_schedule_cache()

    # Tickle subscribers that this run reached a terminal status.
    # Broad except so an SSE-channel failure never breaks cron
    # bookkeeping.
    if run_id is not None:
        _publish_cron_run_change(
            service_id,
            run_id,
            task,
            final_status,
            started_at=row_started_at,
            duration_s=duration_s,
        )


def _apply_cron_run_outcome_deltas(
    con: Any,
    service_id: str,
    run_id: int,
    expected_task: str | None,
    deltas: dict[str, int],
) -> tuple[str | None, str | None]:
    """Lock ``cron_runs`` row ``run_id``, apply ``deltas`` to ``outcome_counters``,
    and return ``(prev_run_status, new_run_status)``.
    """
    _lock_cron_run(con, service_id, run_id)
    run_row = con.execute(
        "SELECT task, status, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ? FOR UPDATE",
        (run_id, service_id),
    ).fetchone()
    if run_row is None:
        raise RuntimeError(f"originating cron run {run_id} is missing for ledger object")
    if expected_task != run_row["task"]:
        raise RuntimeError(f"ledger origin task does not match cron run {run_id}")
    prev_run_status = str(run_row["status"]) if run_row["status"] is not None else None

    totals = _decode_outcome_counters(run_row["outcome_counters"])
    for key in OUTCOME_COUNTER_KEYS:
        totals[key] += deltas[key]
        if totals[key] < 0:
            raise RuntimeError(f"outcome counter {key!r} would become negative")
    has_failure = any(
        totals[key]
        for key in (
            "malformed_records",
            "corrupt_containers",
            "quarantine_capture_failures",
            "source_delete_failures",
            "objects_partial",
            "objects_failed",
        )
    )
    new_run_status = "error" if has_failure or run_row["status"] == "error" else run_row["status"]
    con.execute(
        "UPDATE cron_runs SET outcome_counters = ?, status = ?, "
        "rows_ingested = ?, corrupt_rows = ?, "
        "error_message = CASE WHEN ? = 'error' "
        "THEN COALESCE(error_message, 'One or more ingestion objects had failures') "
        "ELSE error_message END WHERE id = ? AND service_id = ?",
        (
            json.dumps(totals),
            new_run_status,
            totals["valid_records"],
            totals["malformed_records"] + totals["corrupt_containers"],
            new_run_status,
            run_id,
            service_id,
        ),
    )
    return prev_run_status, new_run_status


def _record_ingest_object_outcome_inner(
    service_id: str,
    object_key: str,
    outcome_counters: dict[str, int],
) -> tuple[bool, tuple[int, str, str] | None]:
    """Commit one ledger object's outcome and return ``(updated, publish_info)``."""

    new_counters = _normalize_outcome_counters(outcome_counters)
    con = get_con(service_id)
    _begin_metadata_write(con)
    try:
        ledger_row = con.execute(
            "SELECT originating_task, originating_run_id, outcome_counters "
            "FROM ingest_ledger WHERE service_id = ? AND object_key = ? FOR UPDATE",
            (service_id, object_key),
        ).fetchone()
        if ledger_row is None:
            con.execute("ROLLBACK")
            return False, None

        task = ledger_row["originating_task"]
        run_id = ledger_row["originating_run_id"]
        previous = _decode_outcome_counters(ledger_row["outcome_counters"])
        prev_run_status: str | None = None
        new_run_status: str | None = None
        if run_id is not None:
            run_id = int(run_id)
            deltas = {key: new_counters[key] - previous[key] for key in OUTCOME_COUNTER_KEYS}
            prev_run_status, new_run_status = _apply_cron_run_outcome_deltas(con, service_id, run_id, task, deltas)

        con.execute(
            "UPDATE ingest_ledger SET outcome_counters = ? WHERE service_id = ? AND object_key = ?",
            (json.dumps(new_counters), service_id, object_key),
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    if run_id is not None and task is not None and new_run_status is not None and new_run_status != prev_run_status:
        return True, (run_id, task, new_run_status)
    return True, None


def record_ingest_object_outcome(
    service_id: str,
    object_key: str,
    outcome_counters: dict[str, int],
    *,
    publish: bool = True,
) -> bool:
    """Replace one ledger object's contribution in its originating cron run.

    The ledger snapshot and aggregated cron counters commit together, so task
    retries and later source-deletion outcomes cannot double-count an object.
    """
    updated, pub_info = _record_ingest_object_outcome_inner(service_id, object_key, outcome_counters)
    if publish and pub_info is not None:
        run_id, task, new_run_status = pub_info
        _publish_cron_run_change(service_id, run_id, task, new_run_status)
    return updated


def record_ingest_batch_outcomes(
    service_id: str,
    outcomes_by_key: dict[str, dict[str, int]],
) -> int:
    """Record ledger object outcomes for a batch of objects in a single
    transaction, coalescing SSE ``cron_run_changed`` publications to at most
    one per originating cron run.

    Without batch-level coalescing, a 50-file ``convert_batch_objects`` call
    emits 50 back-to-back ``cron_run_changed`` events ~25ms apart for the same
    ``log_discovery`` run, driving a refetch + ``net::ERR_ABORTED`` storm on
    connected browsers.
    """
    if not outcomes_by_key:
        return 0
    normalized_by_key: dict[str, dict[str, int]] = {}
    for object_key, counters in outcomes_by_key.items():
        if not object_key:
            raise ValueError("object_key is required")
        normalized_by_key[object_key] = _normalize_outcome_counters(counters)

    con = get_con(service_id)
    _begin_metadata_write(con)
    pending_publishes: dict[tuple[int, str], str] = {}
    try:
        keys = list(normalized_by_key.keys())
        placeholders = ",".join("?" for _ in keys)
        ledger_rows = con.execute(
            f"SELECT object_key, originating_task, originating_run_id, outcome_counters "
            f"FROM ingest_ledger WHERE service_id = ? AND object_key IN ({placeholders}) "
            f"ORDER BY object_key FOR UPDATE",
            (service_id, *keys),
        ).fetchall()
        if not ledger_rows:
            con.execute("ROLLBACK")
            return 0

        deltas_by_run: dict[int, tuple[str | None, dict[str, int]]] = {}
        ledger_updates: list[tuple[str, str, str]] = []
        for ledger_row in ledger_rows:
            obj_key = ledger_row["object_key"]
            new_counters = normalized_by_key[obj_key]
            task = ledger_row["originating_task"]
            run_id = ledger_row["originating_run_id"]
            previous = _decode_outcome_counters(ledger_row["outcome_counters"])
            if run_id is not None:
                run_id_int = int(run_id)
                existing_entry = deltas_by_run.get(run_id_int)
                if existing_entry is None:
                    deltas = {k: 0 for k in OUTCOME_COUNTER_KEYS}
                    deltas_by_run[run_id_int] = (task, deltas)
                else:
                    if existing_entry[0] != task:
                        raise RuntimeError(f"ledger origin task does not match cron run {run_id_int}")
                    deltas = existing_entry[1]
                for key in OUTCOME_COUNTER_KEYS:
                    deltas[key] += new_counters[key] - previous[key]
            ledger_updates.append((json.dumps(new_counters), service_id, obj_key))

        for run_id_int in sorted(deltas_by_run):
            task, deltas = deltas_by_run[run_id_int]
            prev_run_status, new_run_status = _apply_cron_run_outcome_deltas(con, service_id, run_id_int, task, deltas)
            if task is not None and new_run_status is not None and new_run_status != prev_run_status:
                pending_publishes[(run_id_int, task)] = new_run_status

        con.executemany(
            "UPDATE ingest_ledger SET outcome_counters = ? WHERE service_id = ? AND object_key = ?",
            ledger_updates,
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    for (run_id_int, task), new_run_status in pending_publishes.items():
        _publish_cron_run_change(service_id, run_id_int, task, new_run_status)
    return len(ledger_updates)


def finalize_cron_run_if_running(
    service_id: str,
    task: str,
    run_id: int | None,
    *,
    duration_s: float = 0.0,
    summary: str | None = None,
    error_message: str | None = None,
) -> bool:
    """Backstop: finalize a cron_runs row as ``error`` ONLY if it's still
    ``running``. Idempotent — no-op when the row is missing or already
    terminal.

    The normal lifecycle is ``start_cron_run`` (inserts ``running``) →
    ``log_cron_run`` (sets the terminal status). If a job exits between those
    two without recording a status — e.g. an ingest generator that returns no
    terminal ``done`` event, or an exception on a path that skips
    ``log_cron_run`` — the row leaks as ``running`` and blocks every
    subsequent run of the same task until the orphan cutoff. That froze prod
    ingestion ~20 min on 2026-06-19. Call this from a ``finally`` so the row
    can never be orphaned by a code path.

    The UPDATE is guarded with ``AND status = 'running'`` so a row that became
    terminal between the read and the write is never clobbered. Returns True
    iff it flipped the row.
    """
    if run_id is None:
        return False
    con = get_con(service_id)

    def _write() -> bool:
        import time

        row = con.execute(
            "SELECT status FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, service_id)
        ).fetchone()
        if not row or row["status"] != "running":
            return False
        cur = con.execute(
            "UPDATE cron_runs SET status = 'error', duration_s = ?, "
            "error_message = COALESCE(error_message, ?), summary = COALESCE(summary, ?) "
            "WHERE id = ? AND service_id = ? AND status = 'running'",
            (max(duration_s, 0.0), error_message, summary, run_id, service_id),
        )
        if cur.rowcount:
            con.execute(
                "UPDATE job_runs SET status = 'error', finished_at = ? "
                "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                (time.time(), service_id, task),
            )
        con.commit()
        return bool(cur.rowcount)

    if not _write():
        return False

    _clear_cron_schedule_cache()

    # Tickle subscribers that this run reached a terminal status — same
    # contract (and broad except) as log_cron_run's publish hook.
    _publish_cron_run_change(service_id, run_id, task, "error")
    return True


def update_cron_duration(
    service_id: str,
    run_id: int,
    duration_s: float,
    log_output: str | None = None,
) -> None:
    con = get_con(service_id)
    if log_output is None:
        con.execute(
            "UPDATE cron_runs SET duration_s = ? WHERE id = ? AND service_id = ?",
            (duration_s, run_id, service_id),
        )
    else:
        con.execute(
            "UPDATE cron_runs SET duration_s = ?, log_output = ? WHERE id = ? AND service_id = ?",
            (duration_s, log_output, run_id, service_id),
        )
    con.commit()


def delete_cron_run(service_id: str, run_id: int) -> None:
    con = get_con(service_id)
    con.execute("DELETE FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, service_id))
    con.commit()


def purge_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    days: int | None = None,
) -> None:
    con = get_con(service_id)
    where: list[str] = ["service_id = ?"]
    params: list = [service_id]
    if task and task != "all":
        where.append("task = ?")
        params.append(task)
    if days is not None:
        cutoff = iso_z(datetime.now(UTC) - timedelta(days=days))
        where.append("started_at < ?")
        params.append(cutoff)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    con.execute(f"DELETE FROM cron_runs {where_sql}", params)
    con.commit()


def record_scoring_audit(
    service_id: str,
    action: str,
    *,
    actor: str = "operator",
    details: dict | None = None,
) -> None:
    """Append an operator-attribution row to the scoring_audit log.

    Called from every scoring-config-mutating endpoint (enable, disable,
    threshold commit + enforce, retrain, rotate-key, matrix-rollback).
    Best-effort: any DB failure is logged at DEBUG and swallowed so a
    transient Postgres blip doesn't block the actual operator action.
    """
    try:
        con = get_con(service_id)
        con.execute(
            "INSERT INTO scoring_audit (service_id, action, actor, details) VALUES (?, ?, ?, ?)",
            (service_id, action, actor, json.dumps(details) if details else None),
        )
        con.commit()
    except psycopg.Error as e:
        logger.debug("[metadata_db] record_scoring_audit(%s, %s) failed: %s", service_id, action, e)


_JSON_ANY_ADAPTER: TypeAdapter[Any] = TypeAdapter(Any)
_JSON_LIST_ADAPTER: TypeAdapter[list[Any]] = TypeAdapter(list[Any])


def _try_parse_json(v: Any) -> Any:
    if isinstance(v, (str, bytes)) and v:
        try:
            return _JSON_ANY_ADAPTER.validate_json(v)
        except (ValueError, TypeError):
            return v
    return v


class _ScoringAuditRowModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    details: Annotated[Any, BeforeValidator(_try_parse_json)] = None


def list_scoring_audit(
    service_id: str,
    *,
    limit: int = 100,
    since: str | None = None,
) -> list[dict]:
    """Most-recent first. Optional ISO ``since`` timestamp lower bound."""
    try:
        con = get_con(service_id)
        if since:
            rows = con.execute(
                "SELECT id, timestamp, action, actor, details FROM scoring_audit "
                "WHERE service_id = ? AND timestamp >= ? ORDER BY id DESC LIMIT ?",
                (service_id, since, limit),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id, timestamp, action, actor, details FROM scoring_audit "
                "WHERE service_id = ? ORDER BY id DESC LIMIT ?",
                (service_id, limit),
            ).fetchall()
        return [_ScoringAuditRowModel.model_validate(dict(r)).model_dump() for r in rows]
    except psycopg.Error as e:
        logger.debug("[metadata_db] list_scoring_audit(%s) failed: %s", service_id, e)
        return []


def get_cron_run_status(service_id: str, run_id: int) -> str | None:
    """Return the status string for a single cron_runs row, or None if
    the row doesn't exist. Used by cron_progress.list_active_runs to
    cross-check the in-memory state against the DB-of-truth (catches
    abandoned-worker-thread zombies that completed log_cron_run but
    never fired end_progress).

    Narrowed exception scope: catches psycopg.Error (DB unreachable,
    table missing, connection lost) and logs at DEBUG so the next 'why
    isn't the cross-check firing?' triage isn't flying blind. Returns None
    on any DB failure so list_active_runs falls back to the in-memory
    signal (we'd rather show a false in-flight than miss a real one).
    """
    import contextlib

    try:
        with contextlib.closing(get_con_readonly(service_id)) as con:
            row = con.execute(
                "SELECT status FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, service_id)
            ).fetchone()
            return row["status"] if row else None
    except psycopg.Error as e:
        logger.debug("[metadata_db] get_cron_run_status(%s, %s) failed: %s", service_id, run_id, e)
        return None


def get_cron_run_result(service_id: str, run_id: int) -> dict | None:
    """Return ``{status, log_output}`` for a cron_runs row, or ``None`` if
    the row doesn't exist. Used by the SSE progress stream when the
    in-memory progress cache has rolled off (completed/historical runs).

    Distinct from ``get_cron_run_status`` because the SSE stream also
    needs the log_output to replay the run's terminal lines."""
    import contextlib

    try:
        with contextlib.closing(get_con_readonly(service_id)) as con:
            row = con.execute(
                "SELECT status, log_output FROM cron_runs WHERE id = ? AND service_id = ?",
                (run_id, service_id),
            ).fetchone()
            if row is None:
                return None
            return {"status": row["status"], "log_output": row["log_output"]}
    except psycopg.Error as e:
        logger.debug("[metadata_db] get_cron_run_result(%s, %s) failed: %s", service_id, run_id, e)
        return None


def _parse_parquet_keys(v: Any) -> list[Any]:
    if isinstance(v, (str, bytes)):
        return _JSON_LIST_ADAPTER.validate_json(v or "[]")
    if v:
        return list(v)
    return []


class _CronRunEntryModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    parquet_keys: Annotated[
        list[Any],
        BeforeValidator(_parse_parquet_keys),
    ] = Field(default_factory=list)


def get_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
    since_id: int | None = None,
    with_total: bool = True,
) -> tuple[int, list[dict]]:
    """Paginated cron run history. Used by repositories/cron.py.

    ``since_id`` enables delta polling: when provided, rows are returned only
    if ``id > since_id`` OR ``status = 'running'``. The ``status = 'running'``
    branch keeps long-lived in-progress runs visible across polls (otherwise
    a sync that started 60 s ago would drop out once its id <= since_id),
    AND keeps the row visible for the single poll where it transitions from
    running to completed (so the client can observe the status change and
    update its toast). Once a row is observed completed (id <= since_id AND
    status != 'running'), it falls out of the response.
    """
    con = get_con(service_id)
    where: list[str] = ["service_id = ?"]
    params: list = [service_id]
    if task and task != "all":
        if "," in task:
            task_list = [t.strip() for t in task.split(",") if t.strip()]
            placeholders = ", ".join("?" for _ in task_list)
            where.append(f"task IN ({placeholders})")
            params.extend(task_list)
        elif task in ("rum_sync", "rum_discovery"):
            where.append("task IN ('rum_sync', 'rum_discovery')")
        else:
            where.append("task = ?")
            params.append(task)
    if status and status != "all":
        where.append("status = ?")
        params.append(status)
    if since_id is not None:
        where.append("(id > ? OR status = 'running')")
        params.append(since_id)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Skip the count(*) precount for delta polls — the frontend cron poll
    # doesn't read total on the since_id branch (only the cron-history
    # page's full-load path uses it), and the writer-side lock contention
    # this query competes with happens precisely when delta polls are
    # firing fastest. Caller opts out via with_total=False. When skipped,
    # ``total`` is filled in from ``len(entries)`` below so callers (and
    # tests) see a truthful row count for the page they got back, rather
    # than a misleading 0.
    if with_total:
        total_row = con.execute(f"SELECT count(*) AS n FROM cron_runs {where_sql}", params).fetchone()
        total = int(total_row["n"]) if total_row else 0
    else:
        total = None  # filled in after rows are fetched

    valid_sort_cols = {"started_at", "duration_s", "task", "status"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "started_at"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    offset = (page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, task, started_at, duration_s, status, error_message,
                   files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                   parquet_files_created, parquet_files_optimized, parquet_keys, summary
            FROM cron_runs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [_CronRunEntryModel.model_validate(dict(r)).model_dump() for r in rows]
    if total is None:
        total = len(entries)
    return total, entries


def latest_cron_per_task(service_id: str, exclude_error_messages: tuple[str, ...] = ()) -> dict[str, dict]:
    """Return {task: latest_completed_run_dict} for the sync-status endpoint.

    Single window-function pass: ROW_NUMBER() OVER (PARTITION BY task) keeps
    the latest non-`running` row per task in one scan of the
    `idx_cron_task_started(task, started_at)` index. The previous
    DISTINCT-tasks + correlated-subquery shape did a btree-seek per task,
    taking ~12.9 ms — fast in absolute terms but per-task overhead added
    up on services with many task types. Mirrors the same pattern used
    by `cron_summary_for_tasks` below.

    ``exclude_error_messages`` (default none) drops rows carrying one of
    these exact error_message sentinels BEFORE the per-task ranking, so an
    excluded row cannot become a task's "latest" and cannot mask the real
    terminal run beneath it. The fault-surfacing caller
    (``recent_cron_failures`` in ``routers/admin/health.py``) passes the
    boot-reap sentinel ``"Process interrupted by server restart"`` here, so a
    cron the deploy restarted mid-run isn't reported as an environment fault —
    matching the identical exclusion the deep-health degraded check already
    applies in ``backend/main.py`` (see its comment on boot-reap rows being
    lifecycle artifacts, not failing crons). Default callers are unaffected.
    """
    con = get_con(service_id)

    exclusion_sql = ""
    params: list[Any] = [service_id]
    if exclude_error_messages:
        placeholders = ",".join(["?"] * len(exclude_error_messages))
        exclusion_sql = f" AND COALESCE(error_message, '') NOT IN ({placeholders})"
        params.extend(exclude_error_messages)

    sql = f"""
    WITH RECURSIVE tasks AS (
        (
            SELECT task FROM cron_runs WHERE service_id = ? ORDER BY service_id, task LIMIT 1
        )
        UNION ALL
        SELECT (
            SELECT task FROM cron_runs WHERE service_id = ? AND task > t.task ORDER BY service_id, task LIMIT 1
        )
        FROM tasks t WHERE t.task IS NOT NULL
    )
    SELECT r.task, r.started_at, r.status, r.duration_s, r.summary, r.error_message
    FROM tasks t
    CROSS JOIN LATERAL (
        SELECT task, started_at, status, duration_s, summary, error_message
        FROM cron_runs
        WHERE service_id = ? AND task = t.task AND status != 'running'{exclusion_sql}
        ORDER BY service_id, task, started_at DESC, id DESC LIMIT 1
    ) r WHERE t.task IS NOT NULL
    """
    cte_params: list[Any] = [service_id, service_id, service_id]
    if exclude_error_messages:
        cte_params.extend(exclude_error_messages)
    rows = con.execute(sql, tuple(cte_params)).fetchall()
    latest_by_task: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_dict = dict(row)
        task_name = row_dict.pop("task")
        latest_by_task[task_name] = row_dict
    return latest_by_task


def adaptive_stale_minutes(
    con: Any,
    *,
    default_minutes: int,
    service_id: str | None = None,
    min_samples: int = 10,
    percentile: float = 0.95,
    safety_multiplier: float = 2.0,
    max_minutes: int = 1440,
) -> int:
    """SRE-22: widen (never narrow) ``default_minutes`` to this service's own
    p95 gap between non-empty ingests, so a low-traffic service's organic
    quiet periods don't trip ``/api/health``'s staleness probe. See
    ``backend.main.health_check``.

    Takes an already-open connection (rather than self-fetching via
    ``get_con(service_id)`` like most of this module) because its only
    caller, ``health_check``, already has one open for the same service —
    and reusing it, rather than a second ``get_con`` call, is what lets a
    test double the caller injects (e.g. an in-memory sqlite fixture) also
    cover this query, instead of silently falling through to the real
    per-service DB.

    Computed from the gaps between consecutive successful ``sync`` runs with
    ``files_downloaded > 0 OR rows_ingested > 0`` in ``cron_runs``. Falls
    back to ``default_minutes`` unchanged when there are fewer than
    ``min_samples`` gaps (new service, or ``cron_runs_days`` retention
    hasn't accumulated enough history yet), or on any error — this must
    never make the probe LESS reliable than the fixed check it widens.
    """
    try:
        sid = service_id or getattr(con, "_service_id", None)
        rows = None
        if sid:
            try:
                rows = con.execute(
                    # 'sync' (pre-v3 name) kept so pre-upgrade history still feeds the
                    # percentile until enough log_discovery samples accumulate.
                    "SELECT started_at FROM cron_runs "
                    "WHERE (service_id = ? OR service_id IS NULL) "
                    "AND task IN ('sync', 'log_discovery') AND status = 'success' "
                    "AND (files_downloaded > 0 OR rows_ingested > 0) "
                    "ORDER BY started_at",
                    (sid,),
                ).fetchall()
            except Exception:
                rows = None
        if rows is None:
            rows = con.execute(
                "SELECT started_at FROM cron_runs "
                "WHERE task IN ('sync', 'log_discovery') AND status = 'success' "
                "AND (files_downloaded > 0 OR rows_ingested > 0) "
                "ORDER BY started_at"
            ).fetchall()
        timestamps = [t for t in (parse_iso_utc(r["started_at"]) for r in rows) if t is not None]
        if len(timestamps) < min_samples + 1:
            return default_minutes
        gaps = sorted((b - a).total_seconds() / 60.0 for a, b in zip(timestamps, timestamps[1:]))
        cut_points = statistics.quantiles(gaps, n=100, method="inclusive")
        p_gap = cut_points[min(len(cut_points) - 1, max(0, int(percentile * 100) - 1))]
        return int(min(max_minutes, max(default_minutes, round(p_gap * safety_multiplier))))
    except Exception:
        return default_minutes


def reap_running_cron_runs(service_id: str, reason: str = "Process interrupted by server restart") -> int:
    con = get_con(service_id)
    # ``con`` is already scoped to this service's own per-service SQLite
    # file (get_con(service_id)), so every row in it belongs to this
    # service regardless of what its `service_id` column holds. Some
    # legacy rows (pre-dating the column's consistent population — seen
    # in production as NULL-service_id 'sync'/'metadata_sync' rows) fail
    # an exact `service_id = ?` match, which is NULL-unsafe in SQL (NULL
    # = anything is NULL, never true) and left them permanently stuck in
    # status='running' across every restart — the health probe's stuck-
    # sync detector then degrades the service forever. Match NULL too so
    # the reaper actually clears every orphan in this file.
    con.execute(
        "UPDATE job_runs SET status = 'error', detail = ? "
        "WHERE (service_id = ? OR service_id IS NULL) AND status = 'running'",
        (reason, service_id),
    )
    cur = con.execute(
        "UPDATE cron_runs SET status = 'error', error_message = COALESCE(error_message, ?) "
        "WHERE (service_id = ? OR service_id IS NULL) AND status = 'running'",
        (reason, service_id),
    )
    con.commit()
    return int(cur.rowcount or 0)


def cron_busy(service_id: str) -> bool:
    import time

    con = get_con(service_id)
    now = time.time()
    row = con.execute(
        "SELECT count(*) AS n FROM job_runs WHERE service_id = ? AND status = 'running' AND (heartbeat_at + lease_ttl_s) >= ?",
        (service_id, now),
    ).fetchone()
    return bool(row and row["n"] > 0)


_self_usage = sys.modules[__name__]
for _mod_name in (
    "backend.core.metadata.usage_log_db",
    "backend.core.metadata.slow_queries",
    "backend.core.metadata.cron_log",
):
    sys.modules[_mod_name] = _self_usage
if "backend.core.metadata" in sys.modules:
    _meta_pkg = sys.modules["backend.core.metadata"]
    for _attr in ("usage_log_db", "slow_queries", "cron_log"):
        setattr(_meta_pkg, _attr, _self_usage)
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    for _mod_name in (
        "backend.core.metadata.usage_log_db",
        "backend.core.metadata.slow_queries",
        "backend.core.metadata.cron_log",
    ):
        sys.modules["backend"]._MODULE_ALIASES[_mod_name] = __name__
