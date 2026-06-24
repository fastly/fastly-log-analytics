"""Wellknown-bots rollup writer + reader.

Pre-materialises the regex-prefiltered (ua, ip, count) rows that the
/api/security/aggregates "wellknown_bots" block reads. The 500-pattern
RE2 prefilter against every UA in the temp_table is the dominant cost
in that endpoint (~155 ms / 12% of wall time per the 2026-06-11 perf
audit). Amortising it at ingest time means request-path workers do a
cheap parquet read of an already-narrowed list instead of re-running
the regex over the full window on every dashboard load.

Schema: ``cache/<svc>/rollups/wellknown_bots/hour=YYYY-MM-DD-HH/
compacted_<uuid>.parquet`` with columns ``(ua, ip, request_count,
pattern_set_version)``. The bot_id and FCrDNS classification stay in
Python — they're cheap given pre-filtered input and avoid having to
rewrite the rollup when classify() / matcher semantics change.

Version invalidation: every row carries the
:func:`backend.utils.bot_sources.get_pattern_set_version` value
captured at write time. The reader compares against the current
version and falls back to the live SQL path for any hour whose
rollup is stale or missing — correctness over speed.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta

from ._common import _safe_table_for, describe_columns, parse_hour_token, quote_path_list

logger = logging.getLogger(__name__)


def _wellknown_bots_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "wellknown_bots")


def recompute_wellknown_bots_rollup(
    service_id: str,
    source: dict,
    hours: set[str] | list[str],
) -> int:
    """Pre-materialise (ua, ip, count) rows for the wellknown_bots
    request-path query for each touched hour.

    Skips the active hour — its per-field rebuild may still be in
    flight at this point in the sync, and the security reader already
    serves the active hour via the live SQL path. Returns the number
    of hour partitions written.

    Best-effort: any failure logs and returns the count written so
    far. The sync caller should NOT raise on a rollup failure (the
    reader's live-SQL fallback covers any missing hour).
    """
    if not hours:
        return 0
    import duckdb

    from backend.core.duckdb import get_connection
    from backend.core.iceberg.view import _get_service_lock
    from backend.utils.bot_sources import get_bot_regex_pattern, get_pattern_set_version

    version = get_pattern_set_version()
    if not version:
        # No source files cached yet — nothing to materialise. The
        # reader's live-SQL fallback handles this correctly.
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
    os.makedirs(bots_root, exist_ok=True)
    lock_key = source.get("name", "default")
    pattern_sql = pattern.replace("'", "''")
    version_sql = version.replace("'", "''")

    rebuilt = 0
    con = get_connection(source=source, read_only=True)
    try:
        # Validate the source has the columns we need before per-hour
        # work — saves N×(failed COPY) on services without UA/IP fields.
        cols = describe_columns(con, source, table_ident, logger=logger, log_label="bot-rollup DESCRIBE failed")
        if cols is None:
            return 0
        if "ua" not in cols or "ip" not in cols:
            return 0

        for hour in parsed:
            # COPY ... TO '<path>' targets a SINGLE FILE when no
            # PARTITION_BY clause is present (a directory target only
            # works alongside PARTITION_BY — observed 2026-06-12: an
            # earlier draft used a tmp directory and DuckDB raised
            # "Cannot open file: Is a directory"). Write to a unique
            # tmp file under the final hour-partition dir, then rename
            # to the canonical compacted_ name under the iceberg lock.
            hour_dir = os.path.join(bots_root, f"hour={hour}")
            os.makedirs(hour_dir, exist_ok=True)
            tmp_path = os.path.join(hour_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            try:
                con.execute(
                    f"COPY ("
                    f"  SELECT ua, ip, count(*) AS request_count, "
                    f"         '{version_sql}' AS pattern_set_version "
                    f"  FROM {table_ident} "
                    f"  WHERE strftime(timestamp, '%Y-%m-%d-%H') = '{hour}' "
                    f"    AND ua IS NOT NULL AND ip IS NOT NULL "
                    f"    AND regexp_matches(ua, '{pattern_sql}') "
                    f"  GROUP BY ua, ip "
                    f"  ORDER BY request_count DESC "
                    f"  LIMIT 50000"
                    f") TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
            except duckdb.Error as e:
                logger.warning("[rollups] %s: bot-rollup COPY failed for hour=%s: %s", service_id, hour, e)
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            # Atomic publish under the per-service iceberg lock.
            # Serializes against concurrent rebuilds (backfill +
            # post-sync overlap). Sweep any pre-existing parquets
            # for the hour FIRST so a reader scanning the dir can't
            # see a stale-version row alongside the freshly-written
            # one (the version check on read would catch it, but
            # eliminating the window is cheaper).
            with _get_service_lock(lock_key):
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            try:
                                os.remove(os.path.join(hour_dir, fname))
                            except OSError:
                                pass
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
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                    continue
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


def read_wellknown_bots_rollup(
    source: dict,
    start_time: str,
    end_time: str,
) -> list[tuple[str, str, int]] | None:
    """Return ``[(ua, ip, request_count), ...]`` for the request window
    by reading the wellknown_bots rollup parquet partitions.

    Returns ``None`` (callers should fall back to the live SQL path) when
    ANY of:
    - The request window includes the active hour (no rollup written
      for in-progress hours).
    - The rollup directory doesn't exist (writer hasn't run yet).
    - Any hour in the window lacks a parquet partition.
    - The cached parquet was written under a different
      ``pattern_set_version`` than the currently-loaded bot sources
      (a sources refresh has happened since the last rollup).

    The hour-mix fallback is intentionally all-or-nothing per request
    rather than "rollup-for-some + live-for-rest": the live SQL
    already does the regex over the whole window's temp_table for
    pennies on the dollar, and returning a half-rollup-half-live union
    would risk double-counting if the live path's prefilter includes
    rows the rollup also covered for an overlapping bucket boundary.
    Better one path or the other.
    """
    from backend.utils.bot_sources import get_pattern_set_version

    current_version = get_pattern_set_version()
    if not current_version:
        return None

    bots_root = _wellknown_bots_root(source)
    if not os.path.isdir(bots_root):
        return None

    # Enumerate every closed hour in the request window. The window
    # comes in as ISO timestamps; we round start DOWN to the hour and
    # end UP so a 24h window of [2026-06-11T00:00, 2026-06-12T00:00)
    # asks for 24 hour-partitions.
    try:
        start_dt = _parse_iso_to_hour(start_time)
        end_dt = _parse_iso_to_hour(end_time)
    except Exception:
        return None
    if start_dt is None or end_dt is None:
        return None
    if end_dt - start_dt > timedelta(days=366):
        return None
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    hours_needed: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        h = cur.strftime("%Y-%m-%d-%H")
        if h >= active_hour:
            # Request window includes the active hour — fall back so
            # the live SQL path picks up in-progress traffic.
            return None
        hours_needed.append(h)
        cur += timedelta(hours=1)
    if not hours_needed:
        return None

    paths: list[str] = []
    for h in hours_needed:
        hour_dir = os.path.join(bots_root, f"hour={h}")
        if not os.path.isdir(hour_dir):
            return None
        files = [f for f in os.listdir(hour_dir) if f.endswith(".parquet") and not f.startswith(".tmp_")]
        if not files:
            return None
        paths.extend(os.path.join(hour_dir, f) for f in files)

    # Single read_parquet across the whole window; DuckDB's :memory:
    # connection avoids contending with the per-service writer pool.
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        paths_sql = quote_path_list(paths)
        try:
            # First check: is the rollup version current? Pull a single
            # distinct version value — if any row's version mismatches
            # the current set, fall back (the writer guarantees one
            # version per partition, but a stale partition from before
            # a source refresh could still be on disk).
            version_row = con.execute(
                f"SELECT DISTINCT pattern_set_version FROM read_parquet([{paths_sql}]) LIMIT 2"
            ).fetchall()
        except duckdb.Error:
            return None
        if not version_row:
            # Empty rollup — no bot traffic in window. Safe to return
            # an empty list (the reader handles that the same as a
            # live SQL returning zero rows).
            return []
        versions = {r[0] for r in version_row}
        if len(versions) > 1 or current_version not in versions:
            return None

        rows = con.execute(
            f"SELECT ua, ip, request_count FROM read_parquet([{paths_sql}]) ORDER BY request_count DESC LIMIT 10000"
        ).fetchall()
        return [(r[0], r[1], int(r[2])) for r in rows]
    finally:
        con.close()


def _parse_iso_to_hour(iso: str) -> datetime | None:
    """Parse an ISO-ish timestamp string into a UTC datetime truncated
    to the hour. Accepts both ``2026-06-11T00:00:00Z`` and
    ``2026-06-11T00:00:00+00:00``. Returns ``None`` on parse failure
    (caller falls back to the live SQL path).
    """
    if not iso:
        return None
    try:
        s = iso.rstrip("Z")
        if "+" not in s and len(s) > 10 and s[-3] == ":":
            # Hand-formatted offset like "+00:00" → already handled
            pass
        dt = datetime.fromisoformat(s).replace(tzinfo=UTC)
    except ValueError:
        return None
    return dt.replace(minute=0, second=0, microsecond=0)
