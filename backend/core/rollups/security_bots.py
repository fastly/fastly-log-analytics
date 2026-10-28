"""Fastly NGWAF API client, bot cache helpers, and well-known bots rollup
writer/reader (split from :mod:`backend.core.rollups.security_dims`).
"""

from __future__ import annotations

import logging
import math
import os
import uuid
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

from backend.core.fastly.client import fastly_paginate
from backend.core.metadata.base import (
    get_pg_readonly_connection,
    get_pg_thread_connection,
)
from backend.core.sql_validator import escape_sql_literal
from backend.utils.date_utils import iso_z, iso_z_now, parse_hour_key_strict, parse_iso_utc

from ._common import (
    _hour_bundled_root,
    _safe_table_for,
    acquire_rollup_connection_and_cols,
    parquet_zstd_target,
    parse_hour_token,
    quote_path_list,
    unlink_quiet,
)
from .security_dims import _parse_iso_to_hour

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fastly NGWAF API client & bot cache helpers (formerly backend.utils.ngwaf)
# ---------------------------------------------------------------------------

_API_PREFIX = "/ngwaf/v1"


def fetch_verified_bots_paged(
    api_key: str,
    workspace_id: str,
    from_ts: str,
    until_ts: str | None = None,
    page_limit: int = 500,
) -> Generator[tuple[list[dict], str | None, int]]:
    """Yield (page_records, latest_timestamp, raw_page_count) for each page of VERIFIED-BOT requests."""
    from backend.core.fastly.client import is_mock_mode

    if is_mock_mode():
        yield [], None, 0
        return

    relative_from = _get_relative_time_range(from_ts)

    q = f"tag:VERIFIED-BOT from:{relative_from}"
    if until_ts:
        relative_until = _get_relative_time_range(until_ts)
        q += f" until:{relative_until}"

    for data in fastly_paginate(
        f"{_API_PREFIX}/workspaces/{workspace_id}/requests",
        token=api_key,
        style="cursor",
        params={"q": q, "limit": page_limit},
        page_delay=0.1,
    ):
        raw_data = data.get("data", [])
        records: list[dict] = []
        latest_ts: str | None = None

        for r in raw_data:
            record = _extract_bot_record(r)
            if record:
                records.append(record)
                ts = record.get("timestamp")
                if ts and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts

        yield records, latest_ts, len(raw_data)


def _get_relative_time_range(ts_str: str) -> str:
    """Convert an ISO timestamp to a rounded-up relative NGWAF time string (e.g. -5d, -15min)."""
    ts = parse_iso_utc(ts_str)
    if ts is None:
        if ts_str.startswith("-"):
            return ts_str
        return "-1d"

    delta = datetime.now(UTC) - ts
    seconds = delta.total_seconds()

    if seconds > 86400:
        days = math.ceil(seconds / 86400)
        return f"-{days}d"

    if seconds > 3600:
        hours = math.ceil(seconds / 3600)
        return f"-{hours}h"

    mins = math.ceil(seconds / 900) * 15
    return f"-{max(mins, 1)}min"


def _extract_bot_record(r: dict) -> dict | None:
    """Extract bot info from a single NGWAF request record. Returns None if no VERIFIED-BOT signal."""
    signals = r.get("signals", [])

    verified = next((s for s in signals if s.get("id") == "VERIFIED-BOT"), None)
    if not verified:
        return None

    subcat = next(
        (s for s in signals if s.get("id", "").startswith("VERIFIED-BOT.") and s["id"] != "VERIFIED-BOT"),
        None,
    )

    bot_name = verified.get("value")
    category = subcat["id"].replace("VERIFIED-BOT.", "") if subcat else None

    user_agent = r.get("user_agent") or next(
        (
            h["value"]
            for h in r.get("request_headers", [])
            if isinstance(h, dict) and h.get("name", "").lower() == "user-agent"
        ),
        None,
    )

    return {
        "waf_req_id": r.get("id"),
        "bot_name": bot_name,
        "category": category,
        "user_agent": user_agent,
        "server_name": r.get("server_name"),
        "timestamp": r.get("timestamp"),
    }


def oldest_unenriched_timestamp(src: dict) -> str | None:
    """Return the oldest log-table timestamp that has a waf_req_id not yet in
    the bot cache. Returns None if the query fails or the log table has no
    unenriched rows.
    """
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    table_name = _safe_table(src["name"])

    try:
        con = get_connection(source=src, max_wait=5, skip_view_update=True, read_only=True)
        try:
            from backend.repositories._base import attach_ngwaf_cache

            with attach_ngwaf_cache(con, ["waf_req_id"], alias="_ngwaf_oldest") as attached:
                if attached:
                    row = con.execute(f"""
                        SELECT MIN(t.timestamp)
                        FROM {table_name} t
                        LEFT JOIN temp._ngwaf_oldest_ngwaf_bots nb USING (waf_req_id)
                        WHERE t.waf_req_id IS NOT NULL
                          AND t.waf_req_id != ''
                          AND t.waf_sig LIKE '%VERIFIED-BOT%'
                          AND nb.waf_req_id IS NULL
                    """).fetchone()
                    ts = row[0] if row and row[0] else None
                    if ts:
                        if hasattr(ts, "strftime"):
                            return iso_z(ts)
                        return str(ts)
        finally:
            con.close()
    except Exception:
        logger.debug("oldest_unenriched_timestamp: con.close failed (ignored)", exc_info=True)

    return None


def _get_conn():
    return get_pg_thread_connection()


def _get_readonly_conn():
    return get_pg_readonly_connection()


def get_last_timestamp(workspace_id: str) -> str | None:
    """Return last_timestamp_synced for workspace, or None if no sync has run yet."""
    con = _get_readonly_conn()
    try:
        row = con.execute(
            "SELECT last_timestamp_synced FROM ngwaf_sync_state WHERE workspace_id = ?",
            (workspace_id,),
        ).fetchone()
        if row and row["last_timestamp_synced"]:
            return row["last_timestamp_synced"]
        return None
    finally:
        con.close()


def update_sync_watermark(workspace_id: str, until_ts: str) -> None:
    """Advance the high-water mark to until_ts after a completed scan."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO ngwaf_sync_state (workspace_id, last_timestamp_synced)
            VALUES (?, ?)
            ON CONFLICT (workspace_id) DO UPDATE SET last_timestamp_synced = EXCLUDED.last_timestamp_synced
            """,
            (workspace_id, until_ts),
        )
        con.commit()
    finally:
        con.close()


def upsert_bots(records: list[dict], workspace_id: str, latest_timestamp: str | None) -> None:
    """Insert or replace bot records and update sync state in one transaction. Idempotent."""
    con = _get_conn()
    now = iso_z_now()
    rows = [
        (
            r["waf_req_id"],
            r.get("bot_name"),
            r.get("category"),
            r.get("wellknown_bot_id"),
            r.get("wellknown_bot_name"),
            now,
        )
        for r in records
        if r.get("waf_req_id")
    ]
    try:
        if rows:
            con.executemany(
                """
                INSERT INTO ngwaf_bots
                    (waf_req_id, bot_name, category, wellknown_bot_id, wellknown_bot_name, synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (waf_req_id) DO UPDATE SET
                    bot_name = EXCLUDED.bot_name,
                    category = EXCLUDED.category,
                    wellknown_bot_id = EXCLUDED.wellknown_bot_id,
                    wellknown_bot_name = EXCLUDED.wellknown_bot_name,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )
        if latest_timestamp:
            try:
                _pts = parse_iso_utc(latest_timestamp)
                next_ts = iso_z(_pts + timedelta(seconds=1)) if _pts else latest_timestamp
            except ValueError:
                next_ts = latest_timestamp
            con.execute(
                """
                INSERT INTO ngwaf_sync_state (workspace_id, last_timestamp_synced)
                VALUES (?, ?)
                ON CONFLICT (workspace_id) DO UPDATE SET last_timestamp_synced = EXCLUDED.last_timestamp_synced
                """,
                (workspace_id, next_ts),
            )
        con.commit()
    finally:
        con.close()


def cleanup_old_bots(retention_days: int) -> int:
    """Delete rows with synced_at older than retention_days. Returns deleted row count."""
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con = _get_conn()
    try:
        cur = con.execute("DELETE FROM ngwaf_bots WHERE synced_at < ?", (cutoff,))
        con.commit()
        return cur.rowcount or 0
    finally:
        con.close()


def get_cache_stats() -> dict:
    """Return summary statistics of cached NGWAF bot records."""
    con = _get_readonly_conn()
    try:
        cur = con.execute("SELECT count(*) FROM ngwaf_bots")
        total_bots = cur.fetchone()[0]

        workspaces: dict[str, str | None] = {}
        cur = con.execute("SELECT workspace_id, last_timestamp_synced FROM ngwaf_sync_state")
        for r in cur.fetchall():
            workspaces[r["workspace_id"]] = r["last_timestamp_synced"]

        cur = con.execute(
            "SELECT bot_name, count(*) AS cnt FROM ngwaf_bots WHERE bot_name IS NOT NULL GROUP BY bot_name ORDER BY count(*) DESC LIMIT 10"
        )
        top_bots = {r["bot_name"]: r["cnt"] for r in cur.fetchall()}

        return {
            "total_cached_bots": total_bots,
            "workspaces": workspaces,
            "top_bots": top_bots,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Wellknown-bots rollup writer + reader (folded from backend.core.rollups.wellknown_bots)
# ---------------------------------------------------------------------------


def _wellknown_bots_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "wellknown_bots")


def recompute_wellknown_bots_rollup(
    service_id: str,
    source: dict,
    hours: set[str] | list[str],
) -> int:
    """Pre-materialise (ua, ip, count) rows for the wellknown_bots
    request-path query for each touched hour."""
    if not hours:
        return 0

    from backend.core.storage._core import _get_service_lock
    from backend.utils.bot_sources import get_bot_regex_pattern, get_pattern_set_version

    version = get_pattern_set_version()
    if not version:
        return 0
    pattern = get_bot_regex_pattern(500)
    if not pattern:
        return 0

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    parsed: list[str] = []
    for h in hours:
        if h == active_hour:
            continue
        if parse_hour_token(h) is None:
            continue
        parsed.append(h)
    if not parsed:
        return 0

    table_ident = _safe_table_for(source)
    if not table_ident:
        return 0

    bots_root = _wellknown_bots_root(source)
    lock_key = source.get("name", "default")
    pattern_sql = escape_sql_literal(pattern)
    version_sql = escape_sql_literal(version)

    rebuilt = 0
    con, cols, owns_con = acquire_rollup_connection_and_cols(
        source, table_ident, logger=logger, log_label="wellknown_bots"
    )
    try:
        if cols is None or "ua" not in cols or "ip" not in cols:
            return 0

        from backend.core.storage import execute_with_stale_view_retry

        os.makedirs(bots_root, exist_ok=True)
        for hour in parsed:
            hour_dt = parse_hour_key_strict(hour)
            start_iso = hour_dt.isoformat()
            end_iso = (hour_dt + timedelta(hours=1)).isoformat()
            hour_dir = os.path.join(bots_root, f"hour={hour}")
            os.makedirs(hour_dir, exist_ok=True)
            tmp_path = os.path.join(hour_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            tmp_path_sql = escape_sql_literal(tmp_path)
            copy_sql = (
                f"COPY ("
                f"  SELECT ua, ip, count(*) AS request_count, "
                f"         '{version_sql}' AS pattern_set_version "
                f"  FROM {table_ident} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
                f"    AND timestamp <  TIMESTAMPTZ '{end_iso}' "
                f"    AND ua IS NOT NULL AND ip IS NOT NULL "
                f"    AND regexp_matches(ua, '{pattern_sql}') "
                f"  GROUP BY ua, ip "
                f"  ORDER BY request_count DESC "
                f"  LIMIT 50000"
                f") {parquet_zstd_target(tmp_path_sql)}"
            )
            try:
                try:
                    execute_with_stale_view_retry(con, source, lambda c, q=copy_sql: c.execute(q))
                except Exception as e:  # noqa: BLE001 — DuckDB errors may arrive wrapped by the stale-view retry hop
                    logger.warning("[rollups] %s: bot-rollup COPY failed for hour=%s: %s", service_id, hour, e)
                    continue

                with _get_service_lock(lock_key):
                    try:
                        for fname in os.listdir(hour_dir):
                            if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                                unlink_quiet(os.path.join(hour_dir, fname))
                    except OSError:
                        pass
                    final_path = os.path.join(hour_dir, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
                    try:
                        os.replace(tmp_path, final_path)
                    except OSError as e:
                        logger.warning(
                            "[rollups] %s: bot-rollup publish failed for hour=%s: %s",
                            service_id,
                            hour,
                            e,
                        )
                        continue
                rebuilt += 1
            finally:
                if os.path.exists(tmp_path):
                    unlink_quiet(tmp_path)
    finally:
        if owns_con:
            con.close()

    return rebuilt


def backfill_wellknown_bots_rollup(
    service_id: str,
    source: dict,
    *,
    lookback_days: int | None = None,
) -> int:
    """Self-heal the wellknown_bots rollup tree against the CURRENT
    ``pattern_set_version``: rebuild what it can, drop what it can't."""
    from backend.core.storage._core import _get_service_lock
    from backend.utils.bot_sources import get_pattern_set_version

    version = get_pattern_set_version()
    if not version:
        return 0

    bots_root = _wellknown_bots_root(source)
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    data_hours: set[str] = set()
    bundled_root = _hour_bundled_root(source)
    try:
        for entry in os.listdir(bundled_root):
            if not entry.startswith("hour="):
                continue
            h = entry[len("hour=") :]
            if (
                h < active_hour
                and parse_hour_token(h) is not None
                and (
                    os.path.isfile(os.path.join(bundled_root, entry, "all_fields.parquet"))
                    or os.path.isfile(os.path.join(bundled_root, entry, "time_series.parquet"))
                )
            ):
                data_hours.add(h)
    except OSError:
        data_hours = set()

    hour_paths: dict[str, list[str]] = {}
    if os.path.isdir(bots_root):
        try:
            for entry in os.listdir(bots_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[len("hour=") :]
                hd = os.path.join(bots_root, entry)
                files = [
                    os.path.join(hd, f) for f in os.listdir(hd) if f.endswith(".parquet") and not f.startswith(".tmp_")
                ]
                if files:
                    hour_paths[hour] = files
        except OSError:
            hour_paths = {}

    current_hours: set[str] = set()
    stale_hours: set[str] = set()
    if hour_paths:
        import duckdb

        all_paths = [p for paths in hour_paths.values() for p in paths]
        from backend.core.duckdb import get_memory_connection

        con = get_memory_connection()
        try:
            paths_sql = quote_path_list(all_paths)
            version_sql = escape_sql_literal(version)
            rows = con.execute(
                f"SELECT DISTINCT regexp_extract(filename, 'hour=([0-9-]+)', 1) AS hr, "
                f"  (pattern_set_version = '{version_sql}') AS is_current "
                f"FROM read_parquet([{paths_sql}], filename=true)"
            ).fetchall()
            for hr, is_current in rows:
                if not hr:
                    continue
                if is_current:
                    current_hours.add(hr)
                else:
                    stale_hours.add(hr)
            for hr, files in hour_paths.items():
                if hr not in current_hours and hr not in stale_hours:
                    try:
                        import pyarrow.parquet as pq

                        if all(pq.read_metadata(f).num_rows == 0 for f in files):
                            current_hours.add(hr)
                    except Exception:
                        logger.debug("[rollups] metadata check failed for hour=%s", hr, exc_info=True)
        except duckdb.Error:
            current_hours = set()
            stale_hours = set()
        finally:
            con.close()

    cutoff_hour = (
        (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d-%H")
        if lookback_days is not None
        else None
    )
    missing = sorted(
        (h for h in data_hours if h not in current_hours and (cutoff_hour is None or h >= cutoff_hour)),
        reverse=True,
    )
    n_written = recompute_wellknown_bots_rollup(service_id, source, missing) if missing else 0

    lock_key = source.get("name", "default")
    n_deleted = 0
    for hour in sorted(stale_hours):
        if hour in current_hours or hour in data_hours or hour >= active_hour:
            continue
        with _get_service_lock(lock_key):
            removed_any = False
            for p in hour_paths.get(hour, []):
                try:
                    os.remove(p)
                    removed_any = True
                except OSError:
                    pass
            try:
                os.rmdir(os.path.join(bots_root, f"hour={hour}"))
            except OSError:
                pass
        if removed_any:
            n_deleted += 1
    if n_deleted:
        logger.info(
            "[rollups] %s: swept %d unrebuildable stale wellknown hour(s) (raw data aged out)",
            service_id,
            n_deleted,
        )

    return n_written


def read_wellknown_bots_rollup(
    source: dict,
    start_time: str,
    end_time: str,
) -> list[tuple[str, str, int]] | None:
    """Return ``[(ua, ip, request_count), ...]`` for the request window
    by reading the wellknown_bots rollup parquet partitions."""
    from backend.utils.bot_sources import get_pattern_set_version

    current_version = get_pattern_set_version()
    if not current_version:
        return None

    bots_root = _wellknown_bots_root(source)
    if not os.path.isdir(bots_root):
        return None

    try:
        start_dt = _parse_iso_to_hour(start_time)
        end_dt = _parse_iso_to_hour(end_time)
    except Exception:
        return None
    if start_dt is None or end_dt is None:
        return None
    if end_dt - start_dt > timedelta(days=366):
        return None
    if end_dt - start_dt < timedelta(hours=23):
        return None

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    closed_hours: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        h = cur.strftime("%Y-%m-%d-%H")
        if h < active_hour:
            closed_hours.append(h)
        cur += timedelta(hours=1)
    if not closed_hours:
        return None

    paths: list[str] = []
    covered = 0
    for h in closed_hours:
        hour_dir = os.path.join(bots_root, f"hour={h}")
        if not os.path.isdir(hour_dir):
            continue
        files = [f for f in os.listdir(hour_dir) if f.endswith(".parquet") and not f.startswith(".tmp_")]
        if not files:
            continue
        covered += 1
        paths.extend(os.path.join(hour_dir, f) for f in files)
    if not paths or covered < (len(closed_hours) + 1) // 2:
        return None

    import duckdb

    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        paths_sql = quote_path_list(paths)
        try:
            rows = con.execute(
                f"SELECT ua, ip, request_count, "
                f"MIN(min_row_ver) OVER () AS min_ver, "
                f"MAX(max_row_ver) OVER () AS max_ver "
                f"FROM (SELECT ua, ip, SUM(request_count) AS request_count, "
                f"MIN(pattern_set_version) AS min_row_ver, "
                f"MAX(pattern_set_version) AS max_row_ver "
                f"FROM read_parquet([{paths_sql}]) "
                f"GROUP BY ua, ip) "
                f"ORDER BY request_count DESC LIMIT 10000"
            ).fetchall()
        except duckdb.Error:
            return None
        if not rows:
            return []
        min_ver, max_ver = rows[0][3], rows[0][4]
        if min_ver != current_version or max_ver != current_version:
            return None
        return [(r[0], r[1], int(r[2])) for r in rows]
    finally:
        con.close()
