"""Ingested-files tracking + dedup + activity reporting in the metadata Postgres database.

Covers the ``ingested_files``, ``ingested_files_summary``, ``ingest_in_flight``,
and ``local_compacted_files`` tables. Also exposes the helpers powering the
log-accounting / log-activity dashboards which read from these tables and
join against ``usage_log`` (for the unbackfilled-edge-files sweep).
"""

from __future__ import annotations

import logging
from typing import Any

from backend import config
from backend.core.metadata import base as pg_connection
from backend.core.metadata import quarantine_evidence as _quarantine_evidence
from backend.core.metadata.base import (
    _ingested_filenames_cache,
    _ingested_filenames_cache_lock,
    _parse_file_date,
    get_con,
    get_con_readonly,
)
from backend.core.metadata.quarantine_evidence import (
    _MAX_ERROR_TEXT_LENGTH,
    _QUARANTINE_EVIDENCE_COLUMNS,
    _RUM_TABLES_SQL,
    QUARANTINE_ITEM_CAP,
    _advisory_key,
    _capture_evidence_locked,
    _evidence_dir,
    _evidence_path,
    _quarantine_file_lock,
    _quarantine_root,
    _service_lock,
    _service_locks,
    _service_locks_guard,
    _unlink_evidence,
    capture_evidence,
    clear_in_flight,
    count_quarantine_evidence,
    delete_quarantine_evidence,
    get_node_count_avg,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    get_quarantine_storage_total,
    get_quarantine_summary,
    get_rum_beacons_avg_per_day,
    get_rum_beacons_by_date,
    insert_quarantine_evidence,
    list_in_flight,
    list_quarantine_evidence,
    purge_evidence,
    read_evidence,
    record_in_flight,
)
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import atomic_write_bytes

logger = logging.getLogger(__name__)


def get_ingested_filenames(service_id: str, limit: int | None = None, table_name: str = "logs") -> set[str]:
    """Retrieve the set of file keys already registered to ``ingested_files``.

    By default, lists log-side files (``table_name='logs'``). Pass the target table name
    explicitly when deduping files for custom tables.

    Bounded calls (``limit`` is not ``None``) read from a process-wide
    in-memory cache populated on first call and kept in sync by
    ``insert_ingested_files``. Cuts per-tick wall time by ~640 ms on
    services with >1 M ingested_files (1.66 s sync tick → ~1.0 s).
    Unbounded calls always hit SQLite for ground truth and invalidate the
    cache.
    """
    cache_key = (service_id, table_name)
    if limit is None:
        with _ingested_filenames_cache_lock:
            _ingested_filenames_cache.pop(cache_key, None)
        con = get_con(service_id)
        rows = con.execute(
            "SELECT file_name FROM ingested_files WHERE source_name = ? AND table_name = ?",
            (service_id, table_name),
        ).fetchall()
        return {r["file_name"] for r in rows}

    with _ingested_filenames_cache_lock:
        cached = _ingested_filenames_cache.get(cache_key)
        if cached is not None:
            return cached.copy()

    con = get_con(service_id)
    rows = con.execute(
        "SELECT file_name FROM ingested_files WHERE source_name = ? AND table_name = ? ORDER BY ingested_at DESC LIMIT ?",
        (service_id, table_name, limit),
    ).fetchall()
    fresh = {r["file_name"] for r in rows}
    with _ingested_filenames_cache_lock:
        _ingested_filenames_cache[cache_key] = fresh
    return fresh.copy()


def get_reclaimable_strand_filenames(service_id: str, candidates: set[str], min_ingested_at: str) -> set[str]:
    """Return the subset of ``candidates`` that the stranded-delete reconcile may
    SAFELY delete: ledger rows with DURABLE data (``row_count > 0``) that were
    ingested at/after ``min_ingested_at``.

    Default-deny — anything not positively proven safe is left alone:

      * ``row_count == 0`` → a no-data MARKER (rows all filtered out by a time bound
        or all corrupt, so NO buffer/DuckLake data was written). Its raw .gz may be
        the only surviving copy, so deleting it would be data loss, not a reclaim.

      * ``ingested_at < min_ingested_at`` → a row written BEFORE the durability fix
        shipped, when a fully-filtered file was (wrongly) recorded with its
        PRE-filter ``row_count`` (> 0). ``row_count`` is not a trustworthy durability
        signal for those, so the reconcile must not delete their raw. They drain via
        the normal 1-day ledger trim instead.

    ``ingested_at`` is stored as SQLite ``datetime('now')`` ("YYYY-MM-DD HH:MM:SS",
    UTC), so ``min_ingested_at`` must use that same lexicographically-chronological
    format. ``candidates`` keeps the scan cheap — the reconcile only passes the
    (small) set of files re-seen in the current LIST.
    """
    if not candidates:
        return set()
    con = get_con(service_id)
    cand = list(candidates)
    out: set[str] = set()
    # Chunk the IN-list to stay well under SQLite's variable limit (999).
    step = 900
    for i in range(0, len(cand), step):
        part = cand[i : i + step]
        placeholders = ",".join("?" * len(part))
        rows = con.execute(
            f"SELECT file_name FROM ingested_files "
            f"WHERE source_name = ? AND row_count > 0 AND ingested_at >= ? "
            f"AND file_name IN ({placeholders})",
            (service_id, min_ingested_at, *part),
        ).fetchall()
        out.update(r["file_name"] for r in rows)
    return out


def list_ingested_files(service_id: str, limit: int = 10000) -> list[dict]:
    """Return up to ``limit`` most-recent ingested files for a service.

    Capped at 10000 by default because the admin Ingestion-History DataTable
    renders client-side — pulling millions of rows over HTTP just to paginate
    them in JS was the 5s+ load time on busy services. 10000 rows still covers
    weeks of normal ingestion volume; admins who need older data can drop the
    cap explicitly.
    """
    con = get_con(service_id)
    rows = con.execute(
        "SELECT file_name, ingested_at, row_count, file_size_bytes FROM ingested_files "
        "WHERE source_name = ? ORDER BY ingested_at DESC LIMIT ?",
        (service_id, limit),
    ).fetchall()
    return [
        {
            "file_name": r["file_name"],
            "ingested_at": str(r["ingested_at"]) if r["ingested_at"] is not None else "",
            "row_count": r["row_count"],
            "file_size_bytes": r["file_size_bytes"],
        }
        for r in rows
    ]


def _query_ingested_files_aggregate(con: Any, service_id: str, table_name: str | None = None) -> dict[str, Any]:
    """Run the aggregate + latest-filename queries against ``ingested_files``."""
    where_sql = "WHERE source_name = ?"
    params: tuple[Any, ...] = (service_id,)
    if table_name is not None:
        where_sql += " AND table_name = ?"
        params = (service_id, table_name)
    agg = con.execute(
        f"SELECT COUNT(*) AS file_count, COALESCE(SUM(row_count), 0) AS total_rows, "
        f"COALESCE(SUM(file_size_bytes), 0) AS total_bytes, COUNT(file_size_bytes) AS count_with_bytes, "
        f"MAX(ingested_at) AS last_ingested FROM ingested_files {where_sql}",
        params,
    ).fetchone()
    # Order chronologically by extracting the timestamp following '.gz' in the filename
    latest_fn_row = con.execute(
        f"SELECT file_name FROM ingested_files {where_sql} "
        "ORDER BY substr(file_name, instr(file_name, '.gz') + 3, 19) DESC, file_name DESC LIMIT 1",
        params,
    ).fetchone()
    return {
        "file_count": (agg["file_count"] if agg else 0) or 0,
        "total_rows": (agg["total_rows"] if agg else 0) or 0,
        "total_bytes": (agg["total_bytes"] if agg else 0) or 0,
        "count_with_bytes": (agg["count_with_bytes"] if agg else 0) or 0,
        "last_ingested": (agg["last_ingested"] if agg else None),
        "latest_file_name": (latest_fn_row["file_name"] if latest_fn_row else None),
    }


def _bootstrap_ingested_files_summary(con: Any, service_id: str) -> dict:
    """One-time SQL aggregate to seed ``ingested_files_summary`` from existing rows.

    Pays the full ~4 s scan ONCE per service per app lifetime so subsequent
    ``get_ingested_files_status_summary`` calls are O(1) lookups against the
    rollup row. Called from the summary getter when the rollup is missing.
    """
    summary = _query_ingested_files_aggregate(con, service_id)
    con.execute(
        "INSERT INTO ingested_files_summary AS summary "
        "(source_name, file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source_name) DO UPDATE SET "
        "file_count = excluded.file_count, total_rows = excluded.total_rows, total_bytes = excluded.total_bytes, "
        "count_with_bytes = excluded.count_with_bytes, latest_file_name = excluded.latest_file_name, "
        "last_ingested = excluded.last_ingested",
        (
            service_id,
            summary["file_count"],
            summary["total_rows"],
            summary["total_bytes"],
            summary["count_with_bytes"],
            summary["latest_file_name"],
            summary["last_ingested"],
        ),
    )
    con.commit()
    return summary


def recompute_ingested_files_summary(con: Any, service_id: str) -> dict:
    """Recompute the ``ingested_files_summary`` rollup from the current table.

    Public entry point for callers that mutate ``ingested_files`` *outside* of
    ``insert_ingested_files`` and must keep the rollup honest. The summary is
    otherwise maintained as an incremental delta (incremented on ingest) and
    has no decrement path, so a bulk ``DELETE`` — notably the retention trim in
    ``cleanup_metadata`` — would leave it over-counting (cumulative-ever)
    forever. Callers pass the same connection they ran the DELETE on so the
    overwrite lands in the same per-service writer.

    Delegates to the full-aggregate upsert used to seed a missing rollup.
    """
    return _bootstrap_ingested_files_summary(con, service_id)


def get_ingested_files_status_summary(service_id: str, table_name: str = "logs") -> dict:
    """O(1) rollup read for ``get_sync_status`` header fields.

    Replaces the per-tick ``list_ingested_files_for_status`` fetchall + Python
    sum/max loop that scaled with table size and hit ~5 s on services with
    >1 M ingested files. Maintained transactionally by
    ``insert_ingested_files``; bootstrapped lazily from a one-time aggregate
    scan if the rollup row is missing (e.g. first read after upgrade).

    Returns ``{file_count, total_rows, total_bytes, count_with_bytes,
    last_ingested, latest_file_name}`` with zero/None defaults when no files
    are ingested yet.
    """
    con = get_con(service_id)
    if table_name != "logs":
        return _query_ingested_files_aggregate(con, service_id, table_name=table_name)

    row = con.execute(
        "SELECT file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested "
        "FROM ingested_files_summary WHERE source_name = ?",
        (service_id,),
    ).fetchone()
    if row is None:
        return _bootstrap_ingested_files_summary(con, service_id)

    return {
        "file_count": row["file_count"] or 0,
        "total_rows": row["total_rows"] or 0,
        "total_bytes": row["total_bytes"] or 0,
        "count_with_bytes": row["count_with_bytes"] or 0,
        "last_ingested": row["last_ingested"],
        "latest_file_name": row["latest_file_name"],
    }


def get_log_accounting_counts(
    service_id: str,
    sql_start: str,
    sql_end: str,
    width: int,
    start_bucket: str,
    end_bucket: str,
) -> dict[str, tuple[int, int]]:
    """Return ``{bucket: (rows, files)}`` for log-accounting reconciliation.

    The compute_log_accounting endpoint used to pull every row in the padded
    ±2h window into Python and run a per-row regex to extract the emission
    bucket from the filename — ~100K rows × regex/dict ops per render of the
    log-accounting panel. Pushing the bucket extraction and group-by into
    SQLite returns ~N rows where N is the bucket count (24-72 for a typical
    window), letting the index do the heavy lifting.

    The CASE matches the Python ``_bucket_for_file`` fallback chain: if the
    full path contains a 'T' preceded by a YYYY-MM-DD prefix we slice the
    emission bucket out of the filename; otherwise we fall back to
    ``ingested_at`` (covers legacy/test files without an ISO basename).

    Fast/slow split — the WHERE used to filter on ``datetime(ingested_at)``,
    which can't use any index (the wrapping function defeats
    ``idx_ingested_files_source_ingested_at``) and forces a full source-
    partition scan: 1533 ms on a 24 h window on prod 2026-06-05.
    The fast UNION arm uses ``file_date`` (populated by ``_migration_002``
    from the canonical Fastly basename), which IS covered by the
    composite ``idx_ingested_files_source_date`` index — range scan
    instead of full scan. Rows whose filename doesn't match the canonical
    pattern (``file_date IS NULL`` — legacy data, tests, ad-hoc
    backfills) fall through to the original ``ingested_at`` scan; that
    arm typically returns zero rows in production but keeps semantic
    equivalence with the pre-change behavior.
    """
    import contextlib

    start_date, end_date = sql_start[:10], sql_end[:10]
    sql_start_space, sql_end_space = sql_start.replace("T", " "), sql_end.replace("T", " ")
    date_pattern_check = " ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'"

    # Optimize fast arm SELECT expression and parameters based on width
    fast_params: tuple[int, ...] = () if width == 10 else (width,)
    fast_bucket_expr = "file_date" if width == 10 else "substr(file_name, instr(file_name, 'T') - 10, ?)"

    # Optimize slow arm bucket CASE expression and parameters based on width
    slow_params: tuple[int, ...]
    if width == 10:
        slow_bucket_expr = (
            "CASE WHEN file_date IS NOT NULL THEN file_date "
            "WHEN ingested_at IS NOT NULL THEN substr(replace(ingested_at, ' ', 'T'), 1, 10) ELSE NULL END"
        )
        slow_params = ()
    else:
        slow_bucket_expr = (
            f"CASE WHEN instr(file_name, 'T') >= 11 AND substr(file_name, instr(file_name, 'T') - 10, 10)"
            f"{date_pattern_check} THEN substr(file_name, instr(file_name, 'T') - 10, ?) "
            "WHEN ingested_at IS NOT NULL THEN substr(replace(ingested_at, ' ', 'T'), 1, ?) ELSE NULL END"
        )
        slow_params = (width, width)

    query = f"""
        SELECT bucket, sum(rc) AS rows, sum(fc) AS files FROM (
            -- Fast arm: file_date index range scan. file_date IS NOT NULL
            -- implies the basename matches the canonical Fastly pattern
            -- per _migration_002, so the bucket substr/column will always succeed.
            SELECT {fast_bucket_expr} AS bucket, sum(row_count) AS rc, count(*) AS fc
            FROM ingested_files
            WHERE source_name = ? AND file_date IS NOT NULL
              AND file_date >= ? AND file_date <= ? AND file_name != '__seeding_attempted__'
            GROUP BY 1
            UNION ALL
            -- Slow arm: rows without a parseable basename (file_date NULL).
            -- Keeps the full CASE so the ingested_at fallback continues
            -- to count test fixtures + legacy uploads.
            -- Removing INDEXED BY allows SQLite to naturally use the optimal index on ingested_at.
            SELECT {slow_bucket_expr} AS bucket, sum(row_count) AS rc, count(*) AS fc
            FROM ingested_files
            WHERE source_name = ? AND file_date IS NULL
              AND ingested_at >= ? AND ingested_at <= ? AND file_name != '__seeding_attempted__'
            GROUP BY 1
        ) AS combined
        GROUP BY bucket
        HAVING bucket IS NOT NULL AND bucket >= ? AND bucket <= ?
    """
    params = (
        *fast_params,
        service_id,
        start_date,
        end_date,
        *slow_params,
        service_id,
        sql_start_space,
        sql_end_space,
        start_bucket,
        end_bucket,
    )
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(query, params).fetchall()
    return {r["bucket"]: (int(r["rows"] or 0), int(r["files"] or 0)) for r in rows}


def get_storage_stats_window(service_id: str, start_str: str, end_str: str) -> tuple[int, int]:
    """Return (file_count, total_bytes) for ingested_files in [start, end].

    Cost panel previously pulled every row (`list_ingested_files_for_status`)
    and filtered/summed in Python — millions of rows per service over HTTP +
    O(N) loop. Pushing COUNT/SUM into SQL lets it run against the source_name
    index and return two integers.
    """
    con = get_con(service_id)
    row = con.execute(
        "SELECT count(*) AS n, coalesce(sum(file_size_bytes), 0) AS bytes FROM ingested_files "
        "WHERE source_name = ? AND ingested_at >= ? AND ingested_at <= ?",
        (service_id, start_str, end_str),
    ).fetchone()
    if not row:
        return 0, 0
    return int(row["n"] or 0), int(row["bytes"] or 0)


def list_unbackfilled_fastly_edge_files(
    service_id: str,
    since: str | None = None,
) -> list[tuple[str, str, int | None, int | None]]:
    """Return ingested_files rows that DON'T yet have a matching ``fastly.edge``
    row in ``usage_log``. Powers the incremental fast path in
    ``backfill_fastly_edge_writes`` so we stop re-checking ~7500 already-
    backfilled files on every cron tick.

    ``since`` (ISO timestamp string) bounds the outer scan via
    ``ingested_at >= since`` so the cron hot path doesn't pay the N-row
    semi-join cost on million-row services where every file is already
    backfilled. Pass ``None`` for an unbounded scan (rare — admin sweep,
    repair tools).

    Cross-database semi-join: ``ingested_files`` lives in metadata.db,
    ``usage_log`` lives in its own ``usage_log.db`` (carved out
    2026-06-12 so cron-writer locks don't block admin readers). SQLite
    can't NOT-EXISTS across separate files, so this implements the
    same predicate as two queries Python-joined into a set difference.
    idx_ingested_files_source_ingested_at + idx_usage_dedup still serve
    both sides individually.
    """
    con = get_con(service_id)
    if since is None:
        rows = con.execute(
            "SELECT file_name, ingested_at, row_count, file_size_bytes FROM ingested_files "
            "WHERE source_name = ? AND file_name != '__seeding_attempted__'",
            (service_id,),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT file_name, ingested_at, row_count, file_size_bytes FROM ingested_files "
            "WHERE source_name = ? AND ingested_at >= ? AND file_name != '__seeding_attempted__'",
            (service_id, since),
        ).fetchall()

    # Pull the already-backfilled file names from usage_log.db once and
    # do the anti-join in Python. The set membership check is O(1) per
    # row; the SELECT on usage_log uses idx_usage_dedup keyed on
    # (service_id, function_name, url).
    if not rows:
        return []

    from backend.core.metadata import usage_log as _usage_log_db

    backfilled: set[str] = set()
    try:
        ul_con = _usage_log_db.open_readonly(service_id)
    except Exception:
        # usage_log.db doesn't exist yet → no rows are backfilled → all
        # ingested_files qualify. Matches the SQL semantics (NOT EXISTS
        # against an empty table returns every outer row).
        ul_con = None
    if ul_con is not None:
        # Scope the lookup to the outer rows' names: loading every historical
        # fastly.edge URL (~450k on long-lived services) cost ~5 s per tick.
        names = [r["file_name"] for r in rows]
        try:
            for i in range(0, len(names), 500):
                chunk = names[i : i + 500]
                placeholders = ",".join("?" * len(chunk))
                backfilled.update(
                    r[0]
                    for r in ul_con.execute(
                        "SELECT url FROM usage_log WHERE service_id = ? AND function_name = 'fastly.edge'"
                        f" AND url IN ({placeholders})",
                        (service_id, *chunk),
                    ).fetchall()
                )
        finally:
            try:
                ul_con.close()
            except Exception:
                logger.debug("list_unbackfilled_fastly_edge_files: ul_con.close failed (ignored)", exc_info=True)

    return [
        (r["file_name"], r["ingested_at"], r["row_count"], r["file_size_bytes"])
        for r in rows
        if r["file_name"] not in backfilled
    ]


def get_latest_ingest_ts(service_id: str) -> str | None:
    """Return the ISO string for the most recent successful ingest
    (``max(ingested_at)`` on ``ingested_files``), or ``None`` if the
    service has never ingested. Powers the dashboard catch-up indicator.

    Filters out the sentinel ``__seeding_attempted__`` row so a
    never-actually-ingested service reads as ``None`` rather than as
    "caught up at the moment we tried to seed"."""
    con = get_con(service_id)
    row = con.execute(
        "SELECT max(ingested_at) AS latest FROM ingested_files "
        "WHERE source_name = ? AND file_name != '__seeding_attempted__'",
        (service_id,),
    ).fetchone()
    if not row or not row["latest"]:
        return None
    return row["latest"]


def get_latest_reconciliation_ts(service_id: str) -> str | None:
    """Return ISO timestamp of the most recent ``fastly.reconciliation`` row
    for the service, or ``None`` if none exist. Used by
    ``reconcile_fastly_stats`` to gate hourly so we don't burn Fastly API
    quota + run the per-class SUBSTR scans on every cron tick.

    Reconciliation rows live in the per-service usage_log SQLite (since
    the v2.0 cutover); the legacy metadata.db.usage_log table is gone.
    """
    from backend.core.metadata import usage_log as _usage_log_db

    try:
        con = _usage_log_db.open_readonly(service_id)
    except Exception:
        # Fresh service before the writer has created the table — no rows.
        return None
    try:
        row = con.execute(
            "SELECT max(timestamp) AS latest FROM usage_log "
            "WHERE service_id = ? AND function_name = 'fastly.reconciliation'",
            (service_id,),
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None
    return row["latest"] if row["latest"] else None


def insert_ingested_files(service_id: str, rows: list[tuple[str, int, int | None]], table_name: str = "logs") -> None:
    """Bulk-insert/upsert (file_name, row_count, file_size_bytes) rows for a service.

    Also maintains the ``ingested_files_summary`` rollup in the same
    transaction so dashboard refresh stays O(1) instead of scanning the full
    1M+ row table on every cron tick. Reads existing values for any rows that
    would upsert so the delta is correct (re-ingest of the same file must not
    double-count its bytes).
    """
    if not rows:
        return
    con = get_con(service_id)

    # Bootstrap the rollup if missing — without this, the delta UPSERT below
    # would seed the rollup with only THIS batch's counts when ingested_files
    # already had a million rows (first insert after upgrade on a populated
    # service). The bootstrap commits in its own statement; the delta update
    # below then correctly adds this batch on top.
    if con.execute("SELECT 1 FROM ingested_files_summary WHERE source_name = ?", (service_id,)).fetchone() is None:
        _bootstrap_ingested_files_summary(con, service_id)

    # Snapshot existing values for rows that already exist, so we can compute
    # accurate (new - old) deltas for the rollup even when this batch upserts.
    file_names = [fn for fn, _, _ in rows]
    existing: dict[str, tuple[int | None, int | None]] = {}
    chunk = 500  # SQLite default expression-tree depth allows ~1000 params
    for i in range(0, len(file_names), chunk):
        batch = file_names[i : i + chunk]
        placeholders = ",".join(["?"] * len(batch))
        for r in con.execute(
            f"SELECT file_name, row_count, file_size_bytes FROM ingested_files "
            f"WHERE source_name = ? AND table_name = ? AND file_name IN ({placeholders})",
            (service_id, table_name, *batch),
        ).fetchall():
            existing[r["file_name"]] = (r["row_count"], r["file_size_bytes"])

    file_count_delta = rows_delta = bytes_delta = count_with_bytes_delta = 0
    # Find the file with the maximum chronological timestamp embedded after '.gz'
    latest_file_name = None
    max_ts = ""
    import re

    pat = re.compile(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})")
    for fn in file_names:
        m = pat.search(fn.split("/")[-1])
        if m:
            ts = f"{m.group(1)}T{m.group(2).replace('-', ':').replace('.', ':')}"
            if ts > max_ts:
                max_ts = ts
                latest_file_name = fn
    if latest_file_name is None:
        latest_file_name = max(file_names)
    for fn, rc, sz in rows:
        if fn in existing:
            old_rc, old_sz = existing[fn]
            rows_delta += (rc or 0) - (old_rc or 0)
            bytes_delta += (sz or 0) - (old_sz or 0)
            had_size, has_size = old_sz is not None, sz is not None
            if has_size and not had_size:
                count_with_bytes_delta += 1
            elif had_size and not has_size:
                count_with_bytes_delta -= 1
        else:
            file_count_delta += 1
            rows_delta += rc or 0
            bytes_delta += sz or 0
            if sz is not None:
                count_with_bytes_delta += 1

    con.executemany(
        """INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date, table_name)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(file_name, source_name, table_name) DO UPDATE SET
               row_count = excluded.row_count,
               file_size_bytes = excluded.file_size_bytes,
               file_date = COALESCE(ingested_files.file_date, excluded.file_date)""",
        [(fn, service_id, rc, sz, _parse_file_date(fn), table_name) for (fn, rc, sz) in rows],
    )
    # Postgres needs an explicit target alias to disambiguate the aggregate
    # row from EXCLUDED.
    # Use the just-applied DB clock so last_ingested matches the row's
    # ingested_at default (datetime('now')) — keeps the rollup honest.
    now_str = con.execute("SELECT datetime('now')").fetchone()[0]
    con.execute(
        """INSERT INTO ingested_files_summary AS summary
               (source_name, file_count, total_rows, total_bytes, count_with_bytes, latest_file_name, last_ingested)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(source_name) DO UPDATE SET
               file_count       = summary.file_count + excluded.file_count,
               total_rows       = summary.total_rows + excluded.total_rows,
               total_bytes      = summary.total_bytes + excluded.total_bytes,
               count_with_bytes = summary.count_with_bytes + excluded.count_with_bytes,
               latest_file_name = CASE
                   WHEN summary.latest_file_name IS NULL OR
                        substr(excluded.latest_file_name, instr(excluded.latest_file_name, '.gz') + 3, 19) >
                        substr(summary.latest_file_name, instr(summary.latest_file_name, '.gz') + 3, 19)
                       THEN excluded.latest_file_name
                   ELSE summary.latest_file_name
               END,
               last_ingested = CASE
                   WHEN summary.last_ingested IS NULL OR excluded.last_ingested > summary.last_ingested
                       THEN excluded.last_ingested
                   ELSE summary.last_ingested
               END""",
        (
            service_id,
            file_count_delta,
            rows_delta,
            bytes_delta,
            count_with_bytes_delta,
            latest_file_name,
            now_str,
        ),
    )
    con.commit()

    # Keep the dedup cache in sync. Only extend if the cache is already
    # populated — seeding it here would prematurely cap a fresh process's
    # cache to just this batch when ingested_files already had millions of
    # rows.
    cache_key = (service_id, table_name)
    with _ingested_filenames_cache_lock:
        cached = _ingested_filenames_cache.get(cache_key)
        if cached is not None:
            cached.update(file_names)


def get_log_activity(service_id: str, start_iso: str, end_iso: str, by: str) -> dict:
    """Return time-bucketed log activity (rows + bytes ingested per bucket).

    SQLite has no DATE_TRUNC, so we bucket via SUBSTR on the ISO timestamp.
    Used by /api/usage/log-activity.
    """
    width_map = {"second": 19, "minute": 16, "hour": 13, "day": 10}
    width = width_map.get(by, 13)

    con = get_con(service_id)
    # Day-bucket path uses the file_date column + composite
    # idx_ingested_files_source_date index added by _migration_002.
    # Skips the per-row substr() on ingested_at + uses an index range
    # scan instead of a full source-partition walk. Falls back to the
    # substr path for rows where file_date is NULL (filenames that
    # don't match the canonical Fastly YYYY-MM-DDTHH:MM:SS format) so
    # legacy data without parseable basenames still counts. The non-day
    # buckets keep the original shape because file_date has only date
    # granularity.
    if by == "day":
        start_date, end_date = start_iso[:10], end_iso[:10]
        rows = con.execute(
            """SELECT bucket, sum(rc) AS rc, sum(bs) AS bs FROM (
                SELECT CAST(file_date AS text) AS bucket, sum(row_count) AS rc, sum(file_size_bytes) AS bs
                FROM ingested_files
                WHERE source_name = ? AND file_date IS NOT NULL
                  AND file_date >= ? AND file_date <= ? AND file_name != '__seeding_attempted__'
                GROUP BY file_date
                UNION ALL
                SELECT substr(replace(ingested_at, ' ', 'T'), 1, 10) AS bucket,
                       sum(row_count) AS rc, sum(file_size_bytes) AS bs
                FROM ingested_files
                WHERE source_name = ? AND file_date IS NULL AND file_name != '__seeding_attempted__'
                  AND ingested_at >= ? AND ingested_at <= ?
                GROUP BY bucket
            ) AS combined GROUP BY bucket ORDER BY bucket""",
            (service_id, start_date, end_date, service_id, start_iso, end_iso),
        ).fetchall()
    else:
        rows = con.execute(
            f"""SELECT substr(replace(ingested_at, ' ', 'T'), 1, {width}) AS bucket,
                       sum(row_count) AS rc, sum(file_size_bytes) AS bs
                FROM ingested_files
                WHERE source_name = ? AND file_name != '__seeding_attempted__'
                  AND ingested_at >= ? AND ingested_at <= ?
                GROUP BY bucket ORDER BY bucket""",
            (service_id, start_iso, end_iso),
        ).fetchall()

    def _normalize(bucket: str) -> str:
        return bucket + ":00" if by == "hour" else bucket

    points: list[dict] = []
    total_rows = total_bytes = 0
    for r in rows:
        if r["bucket"] is None:
            continue
        rc, bs = int(r["rc"] or 0), int(r["bs"] or 0)
        points.append({"time": _normalize(str(r["bucket"])), "row_count": rc, "bytes": bs})
        total_rows += rc
        total_bytes += bs
    return {"data": points, "total_rows": total_rows, "total_bytes": total_bytes, "granularity": by}


__all__ = [
    "QUARANTINE_ITEM_CAP",
    "_MAX_ERROR_TEXT_LENGTH",
    "_QUARANTINE_EVIDENCE_COLUMNS",
    "_RUM_TABLES_SQL",
    "_advisory_key",
    "_bootstrap_ingested_files_summary",
    "_capture_evidence_locked",
    "_evidence_dir",
    "_evidence_path",
    "_quarantine_file_lock",
    "_quarantine_root",
    "_query_ingested_files_aggregate",
    "_service_lock",
    "_service_locks",
    "_service_locks_guard",
    "_unlink_evidence",
    "atomic_write_bytes",
    "capture_evidence",
    "clear_in_flight",
    "config",
    "count_quarantine_evidence",
    "delete_quarantine_evidence",
    "get_ingested_filenames",
    "get_ingested_files_status_summary",
    "get_latest_ingest_ts",
    "get_latest_reconciliation_ts",
    "get_log_accounting_counts",
    "get_log_activity",
    "get_node_count_avg",
    "get_quarantine_evidence",
    "get_quarantine_evidence_summary",
    "get_quarantine_storage_total",
    "get_quarantine_summary",
    "get_reclaimable_strand_filenames",
    "get_rum_beacons_avg_per_day",
    "get_rum_beacons_by_date",
    "get_storage_stats_window",
    "insert_ingested_files",
    "insert_quarantine_evidence",
    "list_in_flight",
    "list_ingested_files",
    "list_quarantine_evidence",
    "list_unbackfilled_fastly_edge_files",
    "pg_connection",
    "purge_evidence",
    "read_evidence",
    "recompute_ingested_files_summary",
    "record_in_flight",
]

install_shim_module(__name__, (_quarantine_evidence,), bidirectional_seed=True)
