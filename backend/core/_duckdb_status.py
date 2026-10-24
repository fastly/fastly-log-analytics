"""Sync-status, schema, ASN, usage-log helpers for backend.core.duckdb.

Carved out of ``backend/core/duckdb.py`` (the 2110-line monolith) so the
main module stays under the 1500-line tech-debt threshold. Every name
defined here is re-imported back into ``backend.core.duckdb`` at the
bottom of that module so external callers (e.g.
``from backend.core.duckdb import get_sync_status``) continue to work
unchanged.

The helpers here all depend on the connection / pool / config primitives
defined in the first ~1070 lines of duckdb.py. Late-import them inside
functions where possible; module-level imports happen below.

Carve scope: ``get_sync_status`` (1072) through the end of the original
file (purge_usage_log, ~2110). ~1040 lines total — covers status
refresh, schema cache, ASN name resolution, usage-log writes, Fastly
edge backfill / reconcile.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend import config as svcconfig
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.date_utils import safe_iso as _safe_iso  # noqa: E402
from backend.utils.log_throttle import log_once_per

logger = logging.getLogger(__name__.replace("_duckdb_status", "duckdb"))

# Pull helpers from the main duckdb module. Late-binding via the module
# object dodges the circular import (this file is loaded BY duckdb.py
# at the bottom of its own load, so the partial module already has every
# helper defined in lines 1..1071 of the original file).
from backend.core import duckdb as _db_main


# These are passthrough wrappers around backend.core.duckdb helpers,
# deferred-binding via _db_main so the lookup happens at call-time (avoids
# circular-import issues since this module is imported BY duckdb.py at the
# bottom of its own load).
def _cache_dir(*a, **kw):
    return _db_main._cache_dir(*a, **kw)


def _safe_table_name(*a, **kw):
    return _db_main._safe_table_name(*a, **kw)


def get_connection(*a, **kw):
    return _db_main.get_connection(*a, **kw)


def is_configured(*a, **kw):
    return _db_main.is_configured(*a, **kw)


# ``_DEFAULT_SOURCE`` CAN be re-bound by ``reload_default_source`` — expose it
# via a property-like getter so the bare-name reads inside this module's
# functions always see the current main-module value (tests that swap it for
# fixture data work unchanged).
def __getattr__(name: str):
    if name == "_DEFAULT_SOURCE":
        return _db_main._DEFAULT_SOURCE
    raise AttributeError(name)


# Cache for the data-side half of the get_sync_status COUNT/MIN/MAX query —
# the second-largest cost in the sync cron path (~240 ms warm with ~1.7 k
# small parquets, called every tick via refresh_config_status). Keyed by a
# cheap data-dir mtime fingerprint; buffer-side stats are recomputed fresh
# each call (~1 ms, <100 files) and merged. Process-local — workers don't
# share — which is fine because the cost we avoid is per-process anyway.
#
# Why data-only: an earlier version of this fingerprint also covered the
# buffer dir, but buffer writes land every tick (~1 s) on any busy service
# and the cron only runs every ~10 s, so the fingerprint changed on virtually
# every poll and the cache effectively never hit. The data dir only mutates
# on commit/optimize (rare), so a data-only fingerprint hits ~every tick and
# the split-out fresh buffer query keeps the merged count exactly accurate.
_data_stats_cache: dict[str, tuple[tuple, int, Any, Any]] = {}
_data_stats_cache_lock = threading.Lock()

# Cache for update_top_values output. Same data-only fingerprint as the
# get_sync_status split-stats cache: top values are dominated by committed
# parquets (RESERVOIR samples 100 k rows from a view that scans every data
# parquet's footer + sample), so when the data dir hasn't changed there is
# nothing new to compute. Buffer churn is excluded deliberately — buffer
# files contribute <0.1 % of a 100 k sample on any non-trivial service, and
# filter-picker autocomplete already falls back to a live query when the
# user types a string that isn't in the cached top-200 (get_field_values).
# Process-local: refresh_config_status only runs in the APScheduler thread.
_top_values_cache: dict[str, tuple] = {}
_top_values_cache_lock = threading.Lock()


def _data_stats_fingerprint(source: dict) -> tuple | None:
    """Return a cheap fingerprint of the committed (data-side) view state.

    Sums the mtime of every partition dir under ``cache/<bucket>/data/``
    along with the count. New commits update partition dir mtimes (parquet
    adds/removes); compaction/optimize also moves the mtimes. Cost is
    ~0.5 ms for ~150 stat calls vs ~155 ms for the data-side COUNT/MIN/MAX
    over the underlying parquets.

    Buffer files are deliberately excluded — they churn every tick and would
    bust the cache pointlessly. The buffer-side stats are merged in fresh by
    the caller.

    Returns None if the data dir is missing — callers should treat that as
    "no cache" and fall back to the full view query.
    """
    cache_dir = _cache_dir(source)
    data_dir = os.path.join(cache_dir, "data")
    if not os.path.isdir(data_dir):
        return None
    data_sum = 0
    data_count = 0
    try:
        for entry in os.scandir(data_dir):
            try:
                data_sum += entry.stat().st_mtime_ns
                data_count += 1
            except OSError:
                pass
    except OSError:
        return None
    return (data_sum, data_count)


# In-memory cache for DuckLake committed table stats:
# (source_name, lake_table) -> (snapshot_id, count, min_timestamp, max_timestamp)
_ducklake_stats_cache: dict[tuple[str, str], tuple[int, int, Any, Any]] = {}
_ducklake_stats_cache_lock = threading.Lock()


def clear_ducklake_stats_cache(source_key: str | None = None) -> None:
    """Clear DuckLake table stats cache for a source or globally."""
    with _ducklake_stats_cache_lock:
        if source_key is None:
            _ducklake_stats_cache.clear()
        else:
            for k in list(_ducklake_stats_cache.keys()):
                if k[0] == source_key:
                    _ducklake_stats_cache.pop(k, None)
    with _db_main._data_stats_cache_lock:
        if source_key is None:
            _db_main._data_stats_cache.clear()
        else:
            _db_main._data_stats_cache.pop(source_key, None)


def _query_buffer_stats(con, src) -> tuple[int, Any, Any]:
    """Read (count, min_ts, max_ts) across live local buffer parquet files."""
    from backend.core import iceberg as _ice
    from backend.utils.sql_validator import escape_sql_literal as _esl

    buf_paths = [p for p in _ice.buffer_files(src) if os.path.isfile(p)]
    if not buf_paths:
        return (0, None, None)
    paths_sql = ", ".join(f"'{_esl(p)}'" for p in buf_paths)
    row = con.execute(
        "SELECT count(*), min(timestamp), max(timestamp) "
        f"FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"
    ).fetchone()
    if not row:
        return (0, None, None)
    return ((row[0] or 0), row[1], row[2])


def _authoritative_direct_stats(
    con,
    src,
    *,
    buffer_stats: tuple[int, Any, Any] | None = None,
) -> tuple[int, Any, Any] | None:
    """Count/min/max read STRAIGHT from the committed DuckLake table plus the
    live buffer parquet, bypassing a possibly-poisoned per-connection baked
    iceberg view.

    Committed DuckLake stats are cached in memory keyed by snapshot ID, avoiding
    repeated WAN S3 scans on every sync tick while preserving full consistency.

    Returns ``(count, min_ts, max_ts)`` or ``None`` when nothing is readable.
    """
    counts: list[int] = []
    mins: list[Any] = []
    maxs: list[Any] = []
    src_name = src.get("name", "default")
    try:
        from backend.core.iceberg._ducklake import ducklake_current_snapshot_id, ducklake_table_name

        lake_table = ducklake_table_name(src)
        exists = con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
            [lake_table],
        ).fetchone()
        if exists:
            snap_id = ducklake_current_snapshot_id(con)
            cached = None
            if snap_id is not None:
                with _ducklake_stats_cache_lock:
                    cached = _ducklake_stats_cache.get((src_name, lake_table))
                if (cached is None or cached[0] != snap_id) and lake_table == "logs":
                    with _db_main._data_stats_cache_lock:
                        data_cached = _db_main._data_stats_cache.get(src_name)
                    if data_cached is not None and data_cached[0] == ("ducklake", snap_id):
                        cached = (snap_id, data_cached[1], data_cached[2], data_cached[3])
                        with _ducklake_stats_cache_lock:
                            _ducklake_stats_cache[(src_name, lake_table)] = cached

            if cached is not None and cached[0] == snap_id:
                lake_count, lake_min, lake_max = cached[1], cached[2], cached[3]
            else:
                row = con.execute(
                    f'SELECT count(*), min(timestamp), max(timestamp) FROM lake."{lake_table}"'
                ).fetchone()
                lake_count = (row[0] or 0) if row else 0
                lake_min = row[1] if row else None
                lake_max = row[2] if row else None
                if snap_id is not None:
                    with _ducklake_stats_cache_lock:
                        _ducklake_stats_cache[(src_name, lake_table)] = (snap_id, lake_count, lake_min, lake_max)
                    if lake_table == "logs":
                        with _db_main._data_stats_cache_lock:
                            _db_main._data_stats_cache[src_name] = (
                                ("ducklake", snap_id),
                                lake_count,
                                lake_min,
                                lake_max,
                            )

            if lake_count > 0:
                counts.append(lake_count)
                if lake_min is not None:
                    mins.append(lake_min)
                if lake_max is not None:
                    maxs.append(lake_max)
    except Exception as e:
        logger.debug("[sync-status] direct lake stats unavailable: %s", e)
    try:
        b_cnt, b_min, b_max = buffer_stats if buffer_stats is not None else _query_buffer_stats(con, src)
        if b_cnt > 0:
            counts.append(b_cnt)
        if b_min is not None:
            mins.append(b_min)
        if b_max is not None:
            maxs.append(b_max)
    except Exception as e:
        logger.debug("[sync-status] direct buffer stats unavailable: %s", e)

    total = sum(counts)
    if total <= 0:
        return None
    return (total, min(mins) if mins else None, max(maxs) if maxs else None)


def get_sync_status(
    con: duckdb.DuckDBPyConnection, source: dict | None = None, skip_fos: bool = False, force: bool = False
) -> dict:
    """Check sync state for a source.

    skip_fos=True skips the S3 object listing (Class A operations) and returns
    only local-DB-derived fields. Use this for lightweight header status checks
    on pages that don't need the new-file count.

    force=True performs a fresh listing.
    """
    src = source or _db_main._DEFAULT_SOURCE
    configured = is_configured(src)

    if not configured:
        return {
            "configured": False,
            "local_rows": 0,
            "ingested": 0,
            "fos_total": 0,
            "storage_mode": "cloud",
            "access_level": "read_write",
        }

    # Attempt to return cached status from config if possible

    cached_status = svcconfig.get_status(src["name"])
    if cached_status and not force:
        # If we just want a lightweight status (skip_fos=True),
        # return it immediately without hitting the DB or S3.
        # The background cron job keeps this cache fresh every minute.
        if skip_fos:
            # Re-inject current runtime fields that might have changed
            cached_status["access_level"] = src.get("access_level", "read_write")
            cached_status["storage_mode"] = src.get("storage_mode", "cloud")
            cached_status["configured"] = True
            return cached_status
    table_name = _safe_table_name(src["name"])

    # Pull the ingested-files snapshot from per-service SQLite metadata.
    # The aggregate summary reads a single rollup row (O(1)) rather than
    # scanning the full ingested_files table — on busy services with >1 M
    # files, the legacy fetchall+Python-sum hit ~5 s per cron tick and
    # dominated the post-ingest housekeeping budget.
    try:
        from backend.core import metadata as metadata_db

        summary = metadata_db.get_ingested_files_status_summary(src["name"])
    except Exception:
        summary = {
            "file_count": 0,
            "total_rows": 0,
            "total_bytes": 0,
            "count_with_bytes": 0,
            "last_ingested": None,
            "latest_file_name": None,
        }

    file_count = summary["file_count"]
    local_rows_ingested = summary["total_rows"]
    last_ingested = summary["last_ingested"]
    latest_file_name = summary["latest_file_name"]
    total_bytes = summary["total_bytes"]
    count_with_bytes = summary["count_with_bytes"]
    avg_log_size_kb = (total_bytes / count_with_bytes / 1024.0) if count_with_bytes > 0 else None

    # Parse timestamp from most recently ingested filename (YYYY-MM-DDTHH-MM-SS pattern)
    latest_ingested_file_at = None
    if latest_file_name:
        fname = latest_file_name.split("/")[-1]
        m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", fname)
        if m:
            latest_ingested_file_at = f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"

    # The iceberg view is always the source of truth for row counts.
    # We fetch row counts and time extents if the table exists, even if skip_fos=True,
    # because these are derived from local metadata (Iceberg manifests) and are
    # relatively cheap. This allows the UI to auto-range correctly even during
    # lightweight status polls.
    #
    # The split-path query inside the try block reads parquet DIRECTLY via
    # read_parquet() and doesn't need the iceberg view to exist in the
    # current connection.
    # This matters because sync-status opens a fresh RO connection that
    # doesn't yet have the per-session view; without this, every sync-
    # status poll fell through to ingested_files.row_count (which sums
    # raw FOS line counts BEFORE the timestamp filter and consistently
    # over-reports ~2-3×).
    latest_log_at = None
    earliest_log_at = None
    local_rows = local_rows_ingested

    try:
        # Fetch row count and time extents. The view is built with
        # read_parquet('cache/<bucket>/data/**/*.parquet') UNION ALL
        # read_parquet([buffer_paths]) — DuckDB opens every parquet
        # footer (~150 µs × 1.7 k data files = ~155 ms warm) plus the
        # cheap buffer side. Split the query: cache the data-side
        # count/min/max keyed by a data-dir mtime fingerprint (only
        # changes on commit/optimize), run the buffer side fresh each
        # call (~1 ms for <100 files), then merge. Cache hits go from
        # ~240 ms full-view query down to ~1 ms (data cached + buffer
        # query + fingerprint stat).
        stats = None
        data_fp = _data_stats_fingerprint(src)
        lake_table: str | None = None
        if data_fp is None:
            try:
                from backend.core.iceberg._ducklake import ducklake_current_snapshot_id, ducklake_table_name

                lt = ducklake_table_name(src)
                exists = con.execute(
                    "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
                    [lt],
                ).fetchone()
                if exists and exists[0]:
                    snap_id = ducklake_current_snapshot_id(con)
                    if snap_id is not None:
                        data_fp = ("ducklake", snap_id)
                        lake_table = lt
            except Exception:
                logger.debug("get_sync_status: step failed (ignored)", exc_info=True)

        cache_key = src["name"]
        direct_stats = None
        queried_buffer_stats: tuple[int, Any, Any] | None = None
        if data_fp is not None:
            try:
                with _db_main._data_stats_cache_lock:
                    cached = _db_main._data_stats_cache.get(cache_key)
                if (
                    (cached is None or cached[0] != data_fp)
                    and lake_table is not None
                    and isinstance(data_fp, tuple)
                    and len(data_fp) == 2
                    and data_fp[0] == "ducklake"
                ):
                    with _ducklake_stats_cache_lock:
                        dl_cached = _ducklake_stats_cache.get((cache_key, lake_table))
                    if dl_cached is not None and dl_cached[0] == data_fp[1]:
                        cached = (data_fp, dl_cached[1], dl_cached[2], dl_cached[3])
                        with _db_main._data_stats_cache_lock:
                            _db_main._data_stats_cache[cache_key] = cached
                if cached is not None and cached[0] == data_fp:
                    d_count, d_min, d_max = cached[1], cached[2], cached[3]
                else:
                    if lake_table is not None:
                        d_row = con.execute(
                            f'SELECT count(*), min(timestamp), max(timestamp) FROM lake."{lake_table}"'
                        ).fetchone()
                    else:
                        from backend.utils.sql_validator import escape_sql_literal

                        data_glob = os.path.join(_cache_dir(src), "data", "**", "*.parquet")
                        safe_glob = escape_sql_literal(data_glob)
                        d_row = con.execute(
                            "SELECT count(*), min(timestamp), max(timestamp) "
                            f"FROM read_parquet('{safe_glob}', union_by_name=true, hive_partitioning=false)"
                        ).fetchone()
                    d_count = (d_row[0] or 0) if d_row else 0
                    d_min = d_row[1] if d_row else None
                    d_max = d_row[2] if d_row else None
                    with _db_main._data_stats_cache_lock:
                        _db_main._data_stats_cache[cache_key] = (data_fp, d_count, d_min, d_max)
                    if (
                        lake_table is not None
                        and isinstance(data_fp, tuple)
                        and len(data_fp) == 2
                        and data_fp[0] == "ducklake"
                    ):
                        with _ducklake_stats_cache_lock:
                            _ducklake_stats_cache[(cache_key, lake_table)] = (data_fp[1], d_count, d_min, d_max)

                b_count, b_min, b_max = _query_buffer_stats(con, src)
                queried_buffer_stats = (b_count, b_min, b_max)

                mins = [m for m in (d_min, b_min) if m is not None]
                maxs = [m for m in (d_max, b_max) if m is not None]
                stats = (
                    d_count + b_count,
                    min(mins) if mins else None,
                    max(maxs) if maxs else None,
                )
                if lake_table is not None and stats[0] > 0:
                    direct_stats = stats
            except Exception as split_err:
                # Bust the data cache so we don't pin a half-built result.
                with _db_main._data_stats_cache_lock:
                    _db_main._data_stats_cache.pop(cache_key, None)
                # Stale-cache failure modes ("No files found", missing
                # catalog entries) must flow to the outer view-rebuild
                # handler below — the cure is the same. Re-raise here
                # rather than swallowing, so the existing recovery path
                # still triggers clear_source_caches+update_iceberg_view.
                err_str = str(split_err)
                if (
                    "No files found" in err_str
                    or "Catalog Error: Table with name" in err_str
                    or "does not exist" in err_str
                    or "No such file or directory" in err_str
                ):
                    raise
                logger.debug("[sync-status] split-stats query failed, falling back to view: %s", split_err)

        if stats is None:
            stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
        if stats:
            view_rows = stats[0] if stats[0] is not None else 0
            # When the view returns a real (non-zero) count, trust it
            # as the source of truth — it reflects the rows actually
            # queryable in Iceberg. ingested_files.row_count records
            # the raw JSON line count from each FOS file BEFORE the
            # `WHERE timestamp IS NOT NULL` filter and any time-range
            # filter, and never reflects post-compaction dedup, so it
            # consistently over-reports. Only fall back when the view
            # itself is empty (the "WHERE false" transient-failure
            # fallback) — there we degrade to the metadata sum so the
            # header doesn't read 0 while we have data on disk.
            if view_rows > 0:
                local_rows = view_rows
                earliest_log_at = stats[1]
                latest_log_at = stats[2]
                # A skip_view_update status connection can hold a stale-but-
                # NON-EMPTY baked view (or a retention-trimmed local data
                # mirror) whose max(timestamp) lags the committed lake — the
                # GCE header flap that read an 11-day-old latest while the lake
                # committed fresh rows every few minutes (trap #33/#35). The
                # empty-view self-heal below never fires here because view_rows
                # > 0. The committed DuckLake table is the latest snapshot by
                # construction, so reconcile the extents against it: never
                # report a latest older, or an earliest newer, than it holds.
                # Timestamp-only — the view's non-zero count stays the row
                # source of truth, and the lake min/max are catalog-stat reads
                # (no parquet-footer scan), so the split-stats fast path's
                # perf win is preserved.
                direct = (
                    direct_stats
                    if direct_stats is not None
                    else _authoritative_direct_stats(con, src, buffer_stats=queried_buffer_stats)
                )
                if direct is not None:
                    d_earliest, d_latest = direct[1], direct[2]
                    if d_latest is not None and (latest_log_at is None or d_latest > latest_log_at):
                        latest_log_at = d_latest
                    if d_earliest is not None and (earliest_log_at is None or d_earliest < earliest_log_at):
                        earliest_log_at = d_earliest
            else:
                # Transient empty view (catalog mid-rebuild / "WHERE false").
                # Before preserving a possibly-ancient prior, read the
                # authoritative committed DuckLake table (+ live buffer)
                # DIRECTLY — it is the latest snapshot by construction and
                # bypasses a poisoned per-connection baked view. Without this,
                # a skip_view_update status connection holding an empty view
                # persists a stale latest_log_at forever (the GCE header flap
                # that read Sept-22 while the lake was fresh — trap #33/#35).
                direct = (
                    direct_stats
                    if direct_stats is not None
                    else _authoritative_direct_stats(con, src, buffer_stats=queried_buffer_stats)
                )
                if direct is not None:
                    local_rows = direct[0]
                    earliest_log_at = direct[1]
                    latest_log_at = direct[2]
                else:
                    # The metadata rollup is a poor proxy for queryable rows —
                    # it's the retention-trimmed ingested_files sum (≈1 day),
                    # not the parquet lake — so prefer the last-known-good
                    # persisted local_rows from the previous successful poll
                    # (the real parquet count). The badge "sticks" at the true
                    # value until the view recovers instead of flapping. Fall
                    # back to the metadata sum only on a first-ever poll.
                    prior_rows = (cached_status or {}).get("local_rows") or 0
                    local_rows = prior_rows if prior_rows > 0 else local_rows_ingested
    except Exception as e:
        if (
            "No files found" in str(e)
            or "Catalog Error: Table with name" in str(e)
            or "does not exist" in str(e)
            or "No such file or directory" in str(e)
        ):
            try:
                from backend.core import iceberg

                # Bust the cached view SQL FIRST. Without this, when ingest
                # is mid-commit and holding the per-service lock,
                # update_iceberg_view falls back to executing the cached
                # SQL — which is exactly the stale SQL that referenced
                # the missing parquet, looping us right back into the same
                # error. Clearing the cache forces a real rebuild on the
                # next view-update window (possibly the next poll).
                #
                # ``keep_snapshot_cache=True``: do NOT also wipe the
                # snapshot/path cache. If we wipe both, then a transient
                # catalog-load failure (FOS rate limit, network blip)
                # causes update_iceberg_view to fall through to its
                # empty-view branch — "WHERE false" — which then sticks
                # in _view_cache and shows the user "Total Logs: 0"
                # despite millions of rows being in the table.
                iceberg.clear_source_caches(src.get("name", "default"), keep_snapshot_cache=True)
                iceberg.update_iceberg_view(con, src)
                stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
                if stats:
                    local_rows = stats[0] if stats[0] is not None else 0
                    earliest_log_at = stats[1]
                    latest_log_at = stats[2]
            except Exception as retry_e:
                # The fallback to ``local_rows_ingested`` below is the
                # designed degradation path — when the cache is mid-
                # rebuild and we couldn't acquire the lock, ``local_rows``
                # still reflects the row count we tracked at ingest time.
                # Demoted from print/warning to debug because the cascade
                # spams stderr on every sync-status poll until ingest
                # releases the lock; the bust above breaks the loop on
                # the next attempt regardless.
                logger.debug("[sync-status] log stats unavailable mid-rebuild: %s", retry_e)
                local_rows = local_rows_ingested
        else:
            # Unexpected exception — this one is worth keeping as a
            # warning since it doesn't match any of the known "stale
            # cache" patterns above and the fallback may hide real bugs.
            logger.warning("[sync-status] Failed to get log stats from view: %s", e)
            local_rows = local_rows_ingested

    # Latest available filename mirrors latest_file_name since FOS LIST is
    # not consulted here (comment above explains why). Reuse the summary's
    # latest_file_name directly — both fields tracked the same thing.
    latest_available_file_at = latest_ingested_file_at

    try:
        cron_stats = {}
        time_cutoff = (
            (datetime.now(UTC) - timedelta(minutes=_db_main._STATUS_BUSY_WINDOW_MINS))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

        from backend.core.metadata.cron_log import latest_cron_per_task

        service_id = str(src.get("name") or src.get("service_id") or "")
        crons = latest_cron_per_task(service_id)

        busy = False
        for t, data in crons.items():
            st = data.get("started_at")
            if data.get("status") == "running" and st and str(st) > time_cutoff:
                busy = True

            # Format to what UI expects
            cron_stats[t] = {
                "last_run": _safe_iso(data.get("started_at")),
                "duration_s": data.get("duration_s"),
                "status": data.get("status"),
                "error_message": data.get("error_message"),
                "summary": data.get("summary"),
            }

    except Exception:
        busy = False
        cron_stats = {}

    return {
        "busy": busy,
        "fos_total": file_count,
        "ingested": file_count,
        "local_rows": local_rows,
        "ingested_bytes": total_bytes,
        "avg_log_size_kb": avg_log_size_kb,
        "table_name": table_name,
        "last_ingested_at": _safe_iso(last_ingested),
        "latest_log_at": _safe_iso(latest_log_at),
        "earliest_log_at": _safe_iso(earliest_log_at),
        "latest_ingested_file_at": latest_ingested_file_at,
        "latest_available_file_at": latest_available_file_at,
        "access_level": src.get("access_level", "read_write"),
        "configured": is_configured(src),
        "storage_mode": src.get("storage_mode", "cloud"),
        "logging_service_id": src.get("logging_service_id", ""),
        "cdn_service_id": src.get("cdn_service_id", ""),
        "cron_stats": cron_stats,
    }


def refresh_config_status(service_id: str, include_top_values: bool = True):
    """Fetch latest stats from DuckDB and write them into the service config JSON.

    This allows the UI to read 'latest update' info without having to open the DB
    and risk locking issues when a cron/ingest is busy.

    ``include_top_values`` gates the heavy reservoir-sample + 24-field GROUP BY
    that backs the filter-picker autocomplete cache. The cheap status fields
    (ingested count, latest file, buffer size, iceberg row counts) populate
    regardless, so the dashboard header stays current. Callers from a high-
    cadence cron path (1s log_period → 5s tick) should pass False on most
    ticks and True every ~60s.
    """

    src = svcconfig.load_config(service_id)
    if not src:
        return

    source = svcconfig.config_to_source(src)
    from backend.high_scale.registry import get_high_scale_service_registry

    if get_high_scale_service_registry().resolve(service_id) is not None:
        logger.info("[refresh_status] %s: high-scale service uses ClickHouse status", service_id)
        return

    durable_serving = svcconfig.is_durable_serving_mode(source)
    # ── 1. Non-DuckDB I/O (runs outside / before the exclusive WAL lock) ─────
    cached_status = src.get("status") or {}
    buf_bytes = None
    if not include_top_values and "buffer_size_bytes" in cached_status:
        buf_bytes = cached_status.get("buffer_size_bytes")
    else:
        try:
            from backend.utils.dir_size import _get_dir_size

            buf_dir = _cache_dir(source)
            if os.path.isdir(buf_dir):
                buf_bytes = _get_dir_size(buf_dir)
        except Exception:
            logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

    iceberg_bytes = None
    iceberg_files = None
    if not include_top_values and "iceberg_bytes" in cached_status and "iceberg_files" in cached_status:
        iceberg_bytes = cached_status.get("iceberg_bytes")
        iceberg_files = cached_status.get("iceberg_files")
    else:
        try:
            from backend.core import iceberg as _db_iceberg

            info = _db_iceberg.get_table_info(source)
            if not info.get("error"):
                iceberg_bytes = int(info.get("size_bytes", 0) or 0)
                iceberg_files = int(info.get("data_files", 0) or 0)
        except Exception:
            logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

    con = None
    try:
        # Connect in read-only mode to avoid locking. (Comment was here but the
        # code passed neither flag, so this cron actually took an exclusive
        # writer lock every minute and serialised with ingest.) We also
        # skip_view_update because:
        #   - on RO, CREATE OR REPLACE VIEW would fail silently anyway
        #   - if the cached view is stale, get_sync_status' retry path busts
        #     the view cache so the NEXT writer connection rebuilds clean
        con = get_connection(source, skip_view_update=not durable_serving, read_only=True)
        # skip_fos=False so we do the full Parquet scan for accurate row counts
        # and timestamps. force=True bypasses any stale config-file cache.
        # Durable request metrics must never pass through the local-file
        # estimates / stale-count fallback in get_sync_status. Keep this
        # heavy refresh for RUM, schema and suggestions; the shared seam below
        # owns all durable request fields and serializes with the observer.
        status = {} if durable_serving else get_sync_status(con, source, skip_fos=False, force=True)

        if buf_bytes is not None:
            status["buffer_size_bytes"] = buf_bytes
        if iceberg_bytes is not None:
            status["iceberg_bytes"] = iceberg_bytes
        if iceberg_files is not None:
            status["iceberg_files"] = iceberg_files

        # Cache edge_ratio so /api/usage/prefill's fast-path branch can
        # short-circuit the 3.6 s cold query. The router already reads
        # ``cached_status.get("edge_ratio")`` and returns it directly when
        # populated; the SELECT only fires when this cache miss. Cheap to
        # compute here (one COUNT … FILTER over the active hour) since we
        # already hold the read-only connection.
        if not include_top_values and "edge_ratio" in cached_status:
            status["edge_ratio"] = cached_status.get("edge_ratio")
        else:
            try:
                from backend.repositories import usage as _usage_repo

                ratio, _ = _usage_repo.get_edge_ratio(con, source)
                if ratio is not None:
                    status["edge_ratio"] = ratio
            except Exception:
                logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

        # Schema (SUMMARIZE over the iceberg view) costs ~800 ms because
        # update_iceberg_view runs post-ingest on every tick and clears the
        # schema cache. Only refresh schema on the heavy tick (~once/min):
        # the underlying columns rarely change, the per-column min/max/count
        # stats already lag the live data by up to a tick, and update_status
        # uses dict.update() so the prior status['schema'] stays intact when
        # we omit the key. Bootstrap reads from cache (bootstrap.py:135) or
        # falls back to a fresh get_schema() if cache is empty, so freshness
        # remains bounded by the 60 s heavy cadence either way.
        if include_top_values:
            status["schema"] = get_schema(con, source)

        # Separate RUM and REQUEST metrics computed in the background
        rum_latest = None
        rum_total = 0
        rum_last_sync = None
        request_total = status.get("local_rows", 0)
        request_latest = status.get("latest_log_at")
        request_last_sync = None

        try:
            from backend.core.metadata.cron_log import latest_cron_per_task

            cron_data = latest_cron_per_task(service_id)
            rum_last_sync = cron_data.get("rum_sync", {}).get("started_at")
            request_last_sync = cron_data.get("log_discovery", {}).get("started_at") or cron_data.get("sync", {}).get(
                "started_at"
            )
        except Exception:
            logger.debug("refresh_config_status: cron_data.get failed (ignored)", exc_info=True)

        # RUM freshness/count is persisted by the rum_commit cron (which already
        # attaches the RUM lake + scans it during the aggregate recompute). We
        # read it from the status doc here instead of opening a NESTED RUM
        # connection + live DuckLake scan on the request-ingest cron critical
        # path — that nested scan could wedge log_discovery when the RUM catalog
        # transiently stalled (GCE Remote-Standard freeze; AGENTS.md Trap #39).
        # RUM data only becomes queryable after a rum_commit, so a value
        # persisted at commit time is as fresh as the data itself.
        rum_enabled = svcconfig.is_rum_enabled(source)
        if rum_enabled:
            try:
                persisted_rum = (src.get("status") or {}).get("rum") or {}
                rum_total = persisted_rum.get("total_rows", 0) or 0
                rum_latest = persisted_rum.get("latest_log_at")
                if persisted_rum.get("last_sync_at"):
                    rum_last_sync = persisted_rum.get("last_sync_at")
            except Exception:
                logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)

        status["rum"] = {
            "latest_log_at": rum_latest,
            "total_rows": rum_total,
            "last_sync_at": rum_last_sync,
        }
        if not durable_serving:
            status["request"] = {
                "latest_log_at": request_latest,
                "total_rows": request_total,
                "last_sync_at": request_last_sync,
            }

        if durable_serving:
            from backend.core.request_metrics import refresh_durable_request_metrics

            refresh_durable_request_metrics(source, extra_status=status)
        else:
            svcconfig.update_status(service_id, status)

        # Also update the top values cache for fast filter suggestions
        if include_top_values:
            logger.info("[refresh_status] %s: Updating top-values cache for filter suggestions...", service_id)
            update_top_values(con, source)
    except Exception:
        logger.warning("Failed to refresh config status for %s", service_id, exc_info=True)
    finally:
        if con:
            con.close()


def update_top_values(con: duckdb.DuckDBPyConnection, source: dict):
    """Pre-calculate top values for filter suggestions and save to local cache.

    Scans the Iceberg + buffer view exactly ONCE with a RESERVOIR sample of at
    most 100 000 rows (small enough to be fast even for million-row tables), then
    computes per-field top-200 lists from that in-memory temp table.  This avoids
    N separate S3 scans — one round-trip for all fields.
    """
    service_id = source["name"]
    table_name = _safe_table_name(service_id)

    # Skip the 100 k reservoir + 24-field GROUP BY entirely when the committed
    # data hasn't changed since the last successful regeneration. The cached
    # top_values.json on disk is still valid; nothing in the heavy path needs
    # to read it during the cron tick. See _top_values_cache docstring above
    # for why buffer-side changes are intentionally not invalidated.
    #
    # Run this BEFORE the "SELECT 1 FROM view LIMIT 1" existence check — that
    # probe is ~150 ms on a multi-thousand-parquet service (DuckDB cracks the
    # view definition open), and we already have proof-of-life (cache file +
    # non-None fingerprint) without touching DuckDB.
    cached_top_values_path = os.path.join(_cache_dir(source), "top_values.json")
    data_fp = _data_stats_fingerprint(source)
    if data_fp is None:
        try:
            from backend.core.iceberg._ducklake import ducklake_current_snapshot_id

            snap_id = ducklake_current_snapshot_id(con)
            if snap_id is not None:
                data_fp = ("ducklake", snap_id)
        except Exception:
            logger.debug("update_top_values: step failed (ignored)", exc_info=True)
    if data_fp is not None and os.path.exists(cached_top_values_path):
        with _db_main._top_values_cache_lock:
            prior_fp = _db_main._top_values_cache.get(service_id)
        if prior_fp == data_fp:
            return

    # Check if table exists / has data
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:
        return

    fields = [
        "ip",
        "country",
        "city",
        "host",
        "url",
        "method",
        "ua",
        "status",
        "cache",
        "waf",
        "waf_resp",
        "waf_ms",
        "waf_sig",
        "waf_sig_ind",
        "ja3",
        "ja4",
        "asn",
        "edge",
        "proto",
        "tls",
        "referer",
        "p_type",
        "p_desc",
        "backend",
        "pop",
    ]

    schema_cols = {f["name"] for f in get_schema(con, source, stats=False)}
    fields = [f for f in fields if f in schema_cols or (f == "waf_sig_ind" and "waf_sig" in schema_cols)]

    if not fields:
        return

    # Build the SELECT list: ordinary fields + waf_sig for waf_sig_ind
    select_parts = []
    for f in fields:
        col = "waf_sig" if f == "waf_sig_ind" else f
        if col in schema_cols:
            select_parts.append(f'"{col}"')

    sel = ", ".join(dict.fromkeys(select_parts))  # deduplicate waf_sig

    sample_table = f"_top_sample_{service_id.replace('-', '_')}"
    top_values: dict = {}

    try:
        # Single scan — reservoir sample capped at 100 000 rows
        con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
        try:
            con.execute(
                f"CREATE TEMP TABLE {sample_table} AS "
                f"SELECT {sel} FROM {table_name} USING SAMPLE reservoir(100000 ROWS)"
            )
        except Exception as _e:
            if (
                "No files found" in str(_e)
                or "Catalog Error: Table with name" in str(_e)
                or "does not exist" in str(_e)
                or "No such file or directory" in str(_e)
            ):
                # Buffer file deleted by a commit job — refresh the view and retry
                from backend.core import iceberg

                iceberg.update_iceberg_view(con, source)
                con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
                con.execute(
                    f"CREATE TEMP TABLE {sample_table} AS "
                    f"SELECT {sel} FROM {table_name} USING SAMPLE reservoir(100000 ROWS)"
                )
            else:
                raise

        queries = []
        field_order = []
        for f in fields:
            col = "waf_sig" if f == "waf_sig_ind" else f
            if col not in schema_cols:
                continue
            if f == "waf_sig_ind":
                queries.append(f"""
                    (SELECT '{f}' AS _field, trim(signal) AS _value, count(*) AS _cnt
                     FROM (SELECT unnest(string_split("{col}", ',')) AS signal
                           FROM {sample_table}
                           WHERE "{col}" IS NOT NULL AND "{col}" != '')
                     WHERE trim(signal) != ''
                     GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)
                """)
            else:
                queries.append(f"""
                    (SELECT '{f}' AS _field, CAST("{col}" AS VARCHAR) AS _value, count(*) AS _cnt
                     FROM {sample_table}
                     WHERE "{col}" IS NOT NULL
                     GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)
                """)
            field_order.append(f)

        if queries:
            union_sql = " UNION ALL ".join(queries)
            rows = con.execute(union_sql).fetchall()
            for fname in field_order:
                top_values[fname] = []
            for fname, fval, fcnt in rows:
                if fname in top_values:
                    if len(top_values[fname]) < 200:
                        top_values[fname].append({"value": fval, "count": fcnt})

    except Exception:
        logger.warning("Failed to build top-values index", exc_info=True)
    finally:
        try:
            con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
        except Exception:
            if log_once_per("update_top_values: DROP TABLE failed (ignored)", 60):
                logger.debug("update_top_values: DROP TABLE failed (ignored)", exc_info=True)

    if top_values:
        cache_dir = _cache_dir(source)
        os.makedirs(cache_dir, exist_ok=True)
        # Don't reuse the name ``f`` — an earlier loop binding in this
        # function already typed ``f`` as ``str``, so reusing it here
        # trips mypy's narrow assignment check on the file handle.
        with open(os.path.join(cache_dir, "top_values.json"), "w") as fp:
            json.dump(top_values, fp)
        # Re-read the fingerprint AFTER the write — using the pre-work
        # fingerprint would let a commit that landed mid-sample lock the
        # cache to a stale value. _data_stats_fingerprint is ~0.5 ms.
        post_fp = _data_stats_fingerprint(source)
        if post_fp is None:
            try:
                from backend.core.iceberg._ducklake import ducklake_current_snapshot_id

                snap_id = ducklake_current_snapshot_id(con)
                if snap_id is not None:
                    post_fp = ("ducklake", snap_id)
            except Exception:
                logger.debug("update_top_values: step failed (ignored)", exc_info=True)
        if post_fp is not None:
            with _db_main._top_values_cache_lock:
                _db_main._top_values_cache[service_id] = post_fp


def get_ingested_files(con: duckdb.DuckDBPyConnection | None, source: dict | None = None) -> list[dict]:
    """Return list of ingested files for a source.

    The ``con`` argument is kept for signature compatibility but unused — the
    data lives in per-service SQLite metadata.
    """
    src = source or _db_main._DEFAULT_SOURCE
    from backend.core import metadata as metadata_db

    return metadata_db.list_ingested_files(src["name"])


# (source_name, table_name, stats) -> schema_list
# The heavy refresh_config_status path fires SUMMARIZE every 60 s. With the
# previous 60 s TTL the cache aged out at exactly the heavy-tick interval —
# now-ts hit 60.0 right when the next call landed, so we missed every time
# and paid ~800 ms per heavy tick (and per any /schema endpoint call landing
# at a similar phase). 300 s gives heavy ticks a comfortable hit window
# (5 ticks per refresh) and per-page-load /schema calls land on a hit on the
# common case. The cached values are SUMMARIZE-over-100k-sample stats
# (min/max/null_percentage/approx_unique), which drift slowly enough that a
# 5-minute lag is acceptable for the autocomplete + filter-picker UI that
# consumes them. Schema column adds/removes still invalidate immediately via
# the column-set comparison in update_iceberg_view.
_SCHEMA_CACHE_TTL = 300
_schema_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_SCHEMA_CACHE_TTL)


def _clear_schema_cache(source_name: str | None = None):
    """Clear the schema cache. If source_name is provided, only clear that source."""
    if source_name:
        for k in list(_schema_cache.keys()):
            if k[0] == source_name:
                _schema_cache.pop(k, None)
    else:
        _schema_cache.clear()


def get_schema(
    con: duckdb.DuckDBPyConnection,
    source: dict | None = None,
    stats: bool = True,
) -> list[dict]:
    """Return column names and types for a source's table."""
    src = source or _db_main._DEFAULT_SOURCE
    source_name = src["name"]
    table_name = _safe_table_name(source_name)

    cache_key = (source_name, table_name, stats)
    cached = _schema_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        # COUNT(*) always returns one row — fetchone is None-typed in the
        # DuckDB stubs because the generic shape is row-or-none, but a count
        # query is guaranteed to produce a row. Assert to narrow.
        row = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        if row is None:
            raise RuntimeError("row is unexpectedly None")
        table_exists = row[0] > 0
        if not table_exists:
            return []

        if not stats:
            # SRE-22: Instant catalog schema reflection via DESCRIBE bypasses heavy data scans
            result = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema = [{"name": r[0], "type": r[1]} for r in result]
            if len(schema) == 1 and schema[0]["name"] == "timestamp":
                schema = []
            _schema_cache[cache_key] = schema
            return schema

        # Use SUMMARIZE to get rich metadata instead of just DESCRIBE.
        # 10_000 rows is enough sample for the precision the UI displays
        # (null % to 1 decimal, approx_unique relative error ~3%); the prior
        # 100_000 limit paid 5–10× the cold-miss latency without changing
        # what users see. The 300s in-memory _schema_cache above absorbs
        # repeat calls — the cost is the cold miss after restart or TTL.
        result = con.execute(f"SUMMARIZE SELECT * FROM {table_name} LIMIT 10000").fetchall()
        schema = []
        for row in result:
            count = row[10]
            null_pct = float(row[11]) if row[11] is not None else (100.0 if count == 0 else 0.0)
            schema.append(
                {
                    "name": row[0],
                    "type": row[1],
                    "min": str(row[2]) if row[2] is not None else None,
                    "max": str(row[3]) if row[3] is not None else None,
                    "approx_unique": row[4],
                    "null_percentage": null_pct,
                    "count": count,
                }
            )

        if len(schema) == 1 and schema[0]["name"] == "timestamp":
            schema = []

        _schema_cache[cache_key] = schema
        return schema
    except Exception:
        # If SUMMARIZE fails, fallback to DESCRIBE
        try:
            result = con.execute(f"DESCRIBE {table_name}").fetchall()
            schema = [{"name": row[0], "type": row[1]} for row in result]
            _schema_cache[cache_key] = schema
            return schema
        except Exception:
            return []


# ---------------------------------------------------------------------------
# ASN name resolution
# ---------------------------------------------------------------------------

ASN_CACHE_TTL_DAYS = 30


def get_asn_names(service_id: str, asns: list) -> dict:
    """Return {asn: name} for all requested ASNs.

    Reads the per-service asn_names SQLite cache first; resolves stale or
    unknown entries via cymruwhois (Team Cymru DNS whois, batch, no API key)
    and writes them back to the cache. Falls back to 'AS{number}' on failure.
    """
    if not asns:
        return {}

    asns_clean = [int(a) for a in asns if a is not None]
    if not asns_clean or not service_id:
        return {}

    from backend.core import metadata as metadata_db

    try:
        cached = metadata_db.lookup_asn_names(service_id, asns_clean, max_age_days=ASN_CACHE_TTL_DAYS)
    except Exception:
        cached = {}

    need = [a for a in asns_clean if a not in cached]
    resolved: dict[int, str] = {}

    if need:
        try:
            import socket

            import cymruwhois  # type: ignore

            c = cymruwhois.Client()
            orig_connect = getattr(c, "_connect", None)
            if callable(orig_connect):

                def _safe_connect() -> None:
                    host = getattr(c, "host", None)
                    port = getattr(c, "port", None)
                    if isinstance(host, str) and isinstance(port, int):
                        sock = socket.create_connection((host, port), timeout=2.0)
                        c.socket = sock
                        c.file = sock.makefile("r", encoding="utf-8", errors="replace")
                    else:
                        orig_connect()
                        sock_obj = getattr(c, "socket", None)
                        if sock_obj is not None and hasattr(sock_obj, "settimeout"):
                            sock_obj.settimeout(2.0)
                        reconf = getattr(getattr(c, "file", None), "reconfigure", None)
                        if callable(reconf):
                            reconf(encoding="utf-8", errors="replace")

                c._connect = _safe_connect
            queries = [f"AS{asn}" for asn in need]
            for result in c.lookupmany(queries):
                if result and result.asn:
                    asn_int = int(result.asn)
                    raw_owner = result.owner or f"AS{asn_int}"
                    if " - " in raw_owner:
                        name = raw_owner.split(" - ", 1)[1]
                    else:
                        name = raw_owner
                    resolved[asn_int] = name
        except Exception as exc:
            logger.warning("ASN resolution failed: %s", type(exc).__name__)

        # Negatively cache any ASN in `need` that cymruwhois did not resolve
        # (including on WHOIS socket timeout/exception) so unallocated/synthetic
        # ASNs or blocked port-43 environments do not trigger a 2s WHOIS timeout
        # on every request.
        for asn in need:
            if asn not in resolved:
                resolved[asn] = f"AS{asn}"

        if resolved:
            try:
                metadata_db.upsert_asn_names(service_id, resolved)
            except Exception:
                logger.warning("get_asn_names: metadata_db.upsert_asn_names failed (ignored)", exc_info=True)

    result = {**cached, **resolved}
    for asn in need:
        if asn not in result:
            result[asn] = f"AS{asn}"

    return result


def format_asn_label(asn: int, name: str) -> str:
    """Format an ASN for display: 'Comcast Cable Communications (7922)' or 'AS7922'."""
    if not name or (name.startswith("AS") and name[2:].isdigit()):
        return f"AS{asn}"
    return f"{name} ({asn})"


def enrich_asn_labels(values: list[dict], service_id: str) -> list[dict]:
    """Resolve ASN names and set a 'label' key on matching value dicts in-place.

    Each dict in `values` must have a 'value' key. Dicts whose value is a
    digit string are treated as ASN numbers and enriched with a formatted label.
    Returns the same list (mutated in place).
    """
    asn_list = [int(v["value"]) for v in values if str(v["value"]).isdigit()]
    if not asn_list:
        return values
    # Resolve via the main module's re-export so tests that
    # ``mock.patch("backend.core.duckdb.get_asn_names")`` reach this
    # caller — the carved-here function reference would otherwise be
    # the literal local one and bypass the patch entirely.
    names_map = _db_main.get_asn_names(service_id, asn_list)
    for v in values:
        if str(v["value"]).isdigit():
            v["label"] = format_asn_label(int(v["value"]), names_map.get(int(v["value"]), ""))
    return values


def update_cron_duration(
    source: dict,
    run_id: int,
    duration_s: float,
    log_output: str | None = None,
):
    """Update the duration of a specific cron run record.

    Optionally refresh log_output too — useful when post-ingest phases emit
    status events after the initial log_cron_run snapshot.
    """
    from backend.core import metadata as metadata_db

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return
    metadata_db.update_cron_duration(service_id, run_id, duration_s, log_output=log_output)


def log_usage_calls(source: dict, calls: list[dict], process_context: str | None = None):
    """Persist tracked calls to the per-service SQLite usage log via metadata_db.

    Only writes when usage_logging is enabled globally.
    Skips gracefully on any error so it never breaks the calling path.
    """

    if not svcconfig.is_usage_logging_enabled():
        return

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return
    from backend.core import metadata as metadata_db

    metadata_db.log_usage_calls(service_id, calls, process_context=process_context)


def backfill_fastly_edge_writes(source: dict) -> int:
    """Synthesise one Class A PUT_OBJECT row per ingested file in the usage log.

    Each raw log file in FOS was written by Fastly's edge — that's a billable
    Class A op the user pays for, but we never observe it directly. Idempotent:
    deduplicates against existing 'fastly.edge' rows by URL.
    """

    if not svcconfig.is_usage_logging_enabled():
        return 0

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return 0

    try:
        from backend.core import metadata as metadata_db

        # Incremental: NOT EXISTS join skips files that already have a
        # 'fastly.edge' row in usage_log. Steady-state this returns 0 rows
        # so we avoid the 15-chunk 500-IN dedup scan in log_synthetic_usage.
        # Bounded outer scan to the last hour — unbackfilled files only
        # accumulate when the cron tick that ingested them failed to backfill,
        # which is a same-tick concern. Older unbackfilled rows would only
        # appear if the backfill step crashed; admin sweep tools can call
        # without a `since` bound to repair. Without this bound, the outer
        # scan paid ~7 s per tick on services with >1 M ingested_files even
        # when 0 rows needed work.
        since = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        files = metadata_db.list_unbackfilled_fastly_edge_files(service_id, since=since)
        if not files:
            return 0

        import re as _re

        calls = []
        for f_name, f_ingested, _row_count, f_size in files:
            if f_name == "__seeding_attempted__":
                continue
            ts_match = _re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", f_name)
            ts = (ts_match.group(1) + "Z") if ts_match else f_ingested

            calls.append(
                {
                    "method": "PUT_OBJECT",
                    "path": f_name,
                    "service": "FOS",
                    "details": "Class A · synthesized from ingest",
                    "bytes": f_size,
                    "status": "OK",
                    "caller": "fastly.edge",
                    "time_ms": 0,
                    "_timestamp_override": ts,
                }
            )

        return metadata_db.log_synthetic_usage(service_id, calls)
    except Exception as e:
        logger.debug("[usage_log] Fastly-edge write backfill failed: %s", e)
        return 0


def reconcile_fastly_stats(source: dict, hours_back: int = 12) -> int:
    """Pull Fastly's authoritative hourly /stats/aggregate counts and write one
    reconciliation row per (hour, class) gap into usage_log.

    Why: our synthetic `fastly.edge` backfill counts 1 PUT_OBJECT per ingested
    file, but Fastly's multipart upload pattern actually emits ~3 Class A ops
    per file (CREATE_MULTIPART + UPLOAD_PART + COMPLETE_MULTIPART) and
    additional bookkeeping. The proxy never observes those — they happen
    inside Fastly's edge before any download path. To make the Usage Log page
    agree with Fastly's invoice, we periodically pull /stats/aggregate and
    write a compact reconciliation delta per hour. See
    [metadata_db.reconcile_fastly_stats][] for the per-hour upsert math.

    Idempotent: re-running for an overlapping window replaces prior
    reconciliation rows for those hours rather than stacking them. The
    aggregate is account-wide (Fastly cannot scope FOS ops to a CDN service),
    so this attributes ALL Fastly object-storage ops to the current service.
    For a single-service deployment this is exact; for multi-service the
    estimate is documented as inflated by the /stats/aggregate note already
    surfaced on the Usage Operations chart.
    """

    if not svcconfig.is_usage_logging_enabled():
        return 0

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return 0

    logging_svc_id = source.get("logging_service_id", "")
    if not logging_svc_id:
        return 0

    api_key = svcconfig.get_fastly_api_key(logging_svc_id)
    if not api_key:
        return 0

    try:
        from datetime import UTC, datetime, timedelta

        from backend.core import metadata as metadata_db
        from backend.core.fastly.client import fastly
        from backend.core.fastly.models import StatsResponse

        # Hourly gate — Fastly's hourly /stats/aggregate snaps to the wall
        # clock so re-pulling more than once per hour is pure waste, and the
        # per-class SUBSTR scan over `usage_log` for the 26h window costs
        # ~700ms per call on a populated DB. Skip if we already reconciled
        # within the last hour.
        now_dt = datetime.now(UTC)
        latest_recon = metadata_db.get_latest_reconciliation_ts(service_id)
        if latest_recon:
            try:
                latest_dt = datetime.strptime(latest_recon.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
                if (now_dt - latest_dt) < timedelta(hours=1):
                    return 0
            except (ValueError, AttributeError):
                pass

        now = now_dt.replace(minute=0, second=0, microsecond=0)
        from_ts = int((now - timedelta(hours=hours_back)).timestamp())
        to_ts = int((now + timedelta(hours=1)).timestamp())

        payload = fastly(
            "GET", f"/stats/aggregate?by=hour&from={from_ts}&to={to_ts}", token=api_key, max_retries=0, timeout=30
        )

        hourly: list[dict] = []
        for rec in StatsResponse.model_validate(payload).data:
            if rec.start_time is None:
                continue
            hour_iso = datetime.fromtimestamp(rec.start_time, tz=UTC).strftime("%Y-%m-%dT%H:00:00Z")
            class_a, class_b = rec.fos_ops()
            hourly.append({"hour_iso": hour_iso, "class_a": class_a, "class_b": class_b})

        return metadata_db.reconcile_fastly_stats(service_id, hourly)
    except Exception as e:
        logger.debug("[usage_log] Fastly stats reconciliation failed: %s", e)
        return 0


def purge_usage_log(source: dict):
    """Delete usage logs older than the retention period via metadata_db."""

    ul_cfg = svcconfig.load_usage_logging_config()
    retention_days = int(ul_cfg.get("retention_days", 30))

    service_id = source.get("name") or source.get("service_id", "")
    if not service_id:
        return

    from backend.core import metadata as metadata_db

    metadata_db.purge_usage_log(service_id, retention_days)


# R-1: drain the per-source schema cache between tests so a previous
# test's resolved column list doesn't carry into the next test that
# reuses the same source name.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("core._duckdb_status._schema_cache", _schema_cache)
