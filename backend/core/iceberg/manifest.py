"""Iceberg manifest cache + table-info helpers.

Carved out of ``backend/core/iceberg/_core.py`` (v2.0 file-size sweep
part 1/3) so the historical monolith stays under 1500 lines.

Contains:
- ``_manifest_metadata_cache`` + lock: per-manifest aggregates that
  survive process restarts.
- ``_load_manifest_metadata_cache`` / ``_save_manifest_metadata_cache``:
  persistence helpers.
- ``_get_scan_lock`` + ``_get_cached_or_scan_metadata``: pyiceberg-era scan
  dedup + caching layer. No longer called by ``get_table_info`` /
  ``get_snapshot_calendar`` (see below) post-v3, but left in place — see
  the DuckLake-native section's comment for why.
- ``get_table_info`` + ``get_snapshot_calendar`` + ``ducklake_table_exists``:
  DuckLake-native (v3) public-API surface consumed by admin endpoints and
  ``iceberg/lake_info.py``.
- ``_align_to_schema`` / ``_arrow_to_duckdb`` / ``_prune_empty_dirs``:
  leaf utilities used during commit and view setup.

All names are re-exported back into ``backend.core.iceberg._core`` at
the bottom of that module so the package proxy keeps mirroring
``monkeypatch.setattr("backend.core.iceberg.X", ...)`` writes to the
real binding here.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import UTC, datetime, timedelta

import pyarrow as pa

logger = logging.getLogger("backend.core.iceberg._core")

# Late-bound from the main _core module to dodge the circular import
# (this file is loaded as part of _core.py's module body via the
# bottom-of-file re-import; everything above the carve point is
# already bound on _core's partial module).
from backend.core.iceberg import _core as _core_mod


def __getattr__(name: str):
    """Fallback to _core for any module-level constants/helpers we
    didn't re-import explicitly above."""
    return getattr(_core_mod, name)


_manifest_metadata_cache: dict[str, tuple] = {}
_manifest_metadata_cache_lock = threading.Lock()
_manifest_metadata_loaded: set[str] = set()
_manifest_metadata_loaded_lock = threading.Lock()


def _load_manifest_metadata_cache(source: dict) -> None:
    """Restore persisted per-manifest aggregates into the in-memory cache.

    Per-manifest aggregates are deterministic functions of an immutable
    manifest .avro, so they survive process restarts. Without this load,
    every restart's first `_get_cached_or_scan_metadata` call cold-scans
    every manifest in the current snapshot — a ~1250-GET burst in the
    steady state.
    """
    source_key = source.get("name", "default")
    with _manifest_metadata_loaded_lock:
        if source_key in _manifest_metadata_loaded:
            return
        _manifest_metadata_loaded.add(source_key)

    cache_file = _core_mod._get_cache_file(source, "manifest_metadata_cache.json")
    if not os.path.exists(cache_file):
        return
    try:
        with open(cache_file) as f:
            data = json.load(f)
    except Exception:
        return

    with _manifest_metadata_cache_lock:
        for manifest_path, entry in data.items():
            if manifest_path in _manifest_metadata_cache:
                continue
            try:
                m_calendar = entry.get("calendar") or {}
                m_min_raw = entry.get("min_ts")
                m_max_raw = entry.get("max_ts")
                m_min = datetime.fromisoformat(m_min_raw) if m_min_raw else None
                m_max = datetime.fromisoformat(m_max_raw) if m_max_raw else None
                m_files = int(entry.get("files", 0))
                m_size = int(entry.get("size", 0))
                _manifest_metadata_cache[manifest_path] = (m_calendar, m_min, m_max, m_files, m_size)
            except Exception:
                continue


def _save_manifest_metadata_cache(source: dict, live_manifest_paths: list[str]) -> None:
    """Persist the current snapshot's manifest aggregates to disk.

    Filtering to `live_manifest_paths` prunes manifests dropped by snapshot
    expiry so the file stays bounded by the current snapshot's manifest count.
    """

    cache_file = _core_mod._get_cache_file(source, "manifest_metadata_cache.json")
    payload: dict[str, dict] = {}

    with _manifest_metadata_cache_lock:
        for manifest_path in live_manifest_paths:
            entry = _manifest_metadata_cache.get(manifest_path)
            if entry is None:
                continue
            m_calendar, m_min, m_max, m_files, m_size = entry
            payload[manifest_path] = {
                "calendar": m_calendar,
                "min_ts": m_min.isoformat() if m_min else None,
                "max_ts": m_max.isoformat() if m_max else None,
                "files": m_files,
                "size": m_size,
            }
        # Mirror the on-disk prune in memory. Pre-fix this dict was only
        # ever appended to (lines 3428, 2656) — entries for manifests
        # dropped by snapshot expiry or compaction stayed resident
        # forever, growing into multi-hundred-MB RSS over days of uptime
        # and compounding the host-OOM problem. Compute the live set
        # ONCE outside the loop so the cost is O(live + cache) rather
        # than O(live × cache).
        live_set = set(live_manifest_paths)
        dead_keys = [k for k in _manifest_metadata_cache if k not in live_set]
        for k in dead_keys:
            _manifest_metadata_cache.pop(k, None)

    try:
        tmp = cache_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, cache_file)
    except Exception:
        pass


def _get_scan_lock(source_key: str) -> threading.Lock:
    with _core_mod._ui_metadata_scan_locks_lock:
        if source_key not in _core_mod._ui_metadata_scan_locks:
            _core_mod._ui_metadata_scan_locks[source_key] = threading.Lock()
        return _core_mod._ui_metadata_scan_locks[source_key]


def _get_cached_or_scan_metadata(source: dict, table) -> tuple[int, int, dict, str | None, str | None]:
    """Scan the Iceberg table for file counts, sizes, calendar, and min/max timestamps.

    Optimized to read manifest files directly rather than planning all data files,
    which is significantly faster.
    """
    source_key = source.get("name", "default")
    metadata_loc = table.metadata_location

    # Check cache by metadata location (version-specific)
    cached = _core_mod._ui_metadata_cache.get(source_key)
    if cached and cached[0] == metadata_loc:
        return cached[1]

    # Restore persisted per-manifest aggregates before the scan so a
    # post-restart scan only fetches the new manifest, not every manifest.
    _load_manifest_metadata_cache(source)

    # Use a lock to prevent concurrent redundant scans for the same service
    with _get_scan_lock(source_key):
        # Re-check cache inside the lock in case another thread finished the scan while we waited
        cached = _core_mod._ui_metadata_cache.get(source_key)
        if cached and cached[0] == metadata_loc:
            return cached[1]

        data_files = 0
        size_bytes = 0
        calendar: dict[str, dict] = {}
        min_ts: datetime | None = None
        max_ts: datetime | None = None
        live_manifest_paths: list[str] = []

        t0 = time.time()
        logger.info(
            "%s %s: Scanning table metadata for calendar (location: %s)...",
            _core_mod._ICE,
            source_key,
            metadata_loc.split("/")[-1],
        )
        try:
            current_snap = table.current_snapshot()
            if current_snap:
                # Quick totals from summary
                data_files = int(current_snap.summary.get("total-data-files", 0))
                size_bytes = int(current_snap.summary.get("total-files-size", 0))

                # Detailed calendar from manifests
                io = table.io

                def scan_manifest(manifest):
                    # Per-manifest cache hit: immutable manifests never change
                    # their entry set, so the previously-computed aggregate
                    # is still correct. Skips the .avro GET entirely.
                    manifest_key = getattr(manifest, "manifest_path", None) or repr(manifest)
                    with _manifest_metadata_cache_lock:
                        cached_agg = _manifest_metadata_cache.get(manifest_key)
                    if cached_agg is not None:
                        return cached_agg

                    m_calendar = {}
                    m_min = None
                    m_max = None
                    m_files = 0
                    m_size = 0

                    manifest_file = manifest.fetch_manifest_entry(io)
                    for entry in manifest_file:
                        if entry.status.name == "DELETED" or not entry.data_file:
                            continue

                        f = entry.data_file
                        m_files += 1
                        m_size += f.file_size_in_bytes

                        # Calendar building via partition values
                        try:
                            # f.partition is a Record. For our spec, field 0 is timestamp_hour
                            hour_val = f.partition[0] if f.partition else None
                            if hour_val is not None:
                                dt = datetime.fromtimestamp(hour_val * 3600, tz=UTC)
                                if m_min is None or dt < m_min:
                                    m_min = dt
                                # Add 1 hour to max_ts if using partition value to cover the full range
                                dt_end = dt + timedelta(hours=1)
                                if m_max is None or dt_end > m_max:
                                    m_max = dt_end

                                date_str = dt.strftime("%Y-%m-%d")
                            else:
                                date_str = "unknown"
                        except Exception:
                            date_str = "unknown"

                        if date_str not in m_calendar:
                            m_calendar[date_str] = {"data_files": 0, "size_bytes": 0}
                        m_calendar[date_str]["data_files"] += 1
                        m_calendar[date_str]["size_bytes"] += f.file_size_in_bytes

                    result = (m_calendar, m_min, m_max, m_files, m_size)
                    with _manifest_metadata_cache_lock:
                        _manifest_metadata_cache[manifest_key] = result
                    return result

                manifests = [m for m in current_snap.manifests(io) if m.has_added_files or m.has_existing_files]
                live_manifest_paths = [getattr(m, "manifest_path", None) or repr(m) for m in manifests]

                # Split cached vs uncached BEFORE touching the thread pool.
                # The top-level scan cache above is keyed on the exact
                # metadata_location, so it invalidates on every commit —
                # fanning ALL manifests (even pure cache hits) through
                # ThreadPoolExecutor(max_workers=16) on every post-commit
                # scan oversubscribes this 4-core VM: thread-scheduling
                # overhead for ~3400 near-instant dict lookups cost 40-60s+
                # per scan despite a 99%-warm per-manifest cache (prod
                # 2026-07-30). Only genuinely new manifests need a real
                # network fetch and benefit from parallelism; resolve
                # cached ones with a plain sequential lookup instead.
                cached_results = []
                uncached_manifests = []
                with _manifest_metadata_cache_lock:
                    for m in manifests:
                        key = getattr(m, "manifest_path", None) or repr(m)
                        agg = _manifest_metadata_cache.get(key)
                        if agg is not None:
                            cached_results.append(agg)
                        else:
                            uncached_manifests.append(m)

                # Use parallel execution to speed up S3/CDN manifest fetches
                from concurrent.futures import ThreadPoolExecutor

                if uncached_manifests:
                    with ThreadPoolExecutor(max_workers=16) as executor:
                        fetched_results = list(executor.map(scan_manifest, uncached_manifests))
                else:
                    fetched_results = []
                results = cached_results + fetched_results

                # Merge results
                total_scanned_files = 0
                total_scanned_size = 0
                for m_cal, m_min, m_max, m_files, m_size in results:
                    total_scanned_files += m_files
                    total_scanned_size += m_size
                    if m_min and (min_ts is None or m_min < min_ts):
                        min_ts = m_min
                    if m_max and (max_ts is None or m_max > max_ts):
                        max_ts = m_max
                    for date_str, stats in m_cal.items():
                        if date_str not in calendar:
                            calendar[date_str] = {"data_files": 0, "size_bytes": 0}
                        calendar[date_str]["data_files"] += stats["data_files"]
                        calendar[date_str]["size_bytes"] += stats["size_bytes"]

                # If summary stats were missing or lower than what we scanned, update them
                if total_scanned_files > data_files:
                    data_files = total_scanned_files
                    size_bytes = total_scanned_size

        except Exception as e:
            logger.warning("[iceberg] %s: Metadata scan failed: %s", source_key, e)

        elapsed = time.time() - t0
        logger.info(
            "%s %s: Metadata scan completed in %.2fs (%d files, %d bytes)",
            _core_mod._ICE,
            source_key,
            elapsed,
            data_files,
            size_bytes,
        )

        result = (
            data_files,
            size_bytes,
            calendar,
            min_ts.isoformat() if min_ts else None,
            max_ts.isoformat() if max_ts else None,
        )
        _core_mod._ui_metadata_cache[source_key] = (metadata_loc, result)

        # Persist the current snapshot's manifest aggregates so the next
        # process restart skips the cold scan.
        if live_manifest_paths:
            try:
                _save_manifest_metadata_cache(source, live_manifest_paths)
            except Exception:
                pass

        return result


# ---------------------------------------------------------------------------
# DuckLake-native table-info / calendar (v3 write-path cutover)
#
# The pyiceberg write path (above: `_get_cached_or_scan_metadata` and the
# two functions that used to drive it) is no longer written to — every
# commit goes through `backend.core.iceberg.buffer._commit_buffer_impl`'s
# DuckLake attach instead (see `backend/core/iceberg/_ducklake.py` module
# docstring). A pyiceberg `catalog.load_table()` for any service now
# returns a real, permanently-frozen table object with `snapshots=0` from
# before the cutover, so `get_table_info`/`get_snapshot_calendar` below
# read DuckLake state directly. `_get_cached_or_scan_metadata` itself is
# left in place: `iceberg/view.py`'s delta-cache pre-seed path still
# writes into `_manifest_metadata_cache`, and it has direct test coverage
# in test_iceberg_helpers.py — whether it's still load-bearing anywhere
# else is a separate question for a separate task.

_ducklake_table_metadata_cache: dict[tuple[str, str], tuple[int | None, tuple[int, str | None, str | None, dict]]] = {}
_ducklake_table_metadata_cache_lock = threading.Lock()

from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("iceberg._ducklake_table_metadata_cache", _ducklake_table_metadata_cache)


def _lake_ident(lake_table: str) -> str:
    return 'lake."{}"'.format(lake_table.replace('"', '""'))


def _scan_ducklake_table_metadata(con, lake_table: str) -> tuple[int, str | None, str | None, dict]:
    """Full scan of ``lake.<lake_table>`` for row count, min/max timestamp,
    and a per-day row-count calendar.

    DuckLake exposes no manifest/partition-value API at the SQL level the
    way pyiceberg's manifests did, and small commits get "inlined" into the
    catalog metadata rather than written as parquet files — so there are no
    reliable per-file stats to read either way (see
    ``backend/core/iceberg/_ducklake.py`` module docstring). A direct scan
    is the only approach that stays correct in both cases. Callers should
    go through ``_get_cached_or_scan_ducklake_metadata`` rather than call
    this directly — it's O(table size) and this codebase treats "MANY
    small files, cheap incremental reads" as the standing perf contract.
    """
    ident = _lake_ident(lake_table)
    rows = con.execute(
        f"SELECT date_trunc('day', timestamp)::DATE AS d, count(*) AS n, "
        f"min(timestamp) AS mn, max(timestamp) AS mx "
        f"FROM {ident} WHERE timestamp IS NOT NULL GROUP BY 1 ORDER BY 1"
    ).fetchall()
    calendar: dict[str, dict] = {}
    total = 0
    min_ts: datetime | None = None
    max_ts: datetime | None = None
    for d, n, mn, mx in rows:
        n = int(n)
        calendar[d.isoformat()] = {"data_files": n, "size_bytes": 0}
        total += n
        if mn is not None and (min_ts is None or mn < min_ts):
            min_ts = mn
        if mx is not None and (max_ts is None or mx > max_ts):
            max_ts = mx
    return (
        total,
        min_ts.isoformat() if min_ts else None,
        max_ts.isoformat() if max_ts else None,
        calendar,
    )


def _get_cached_or_scan_ducklake_metadata(
    con, source: dict, lake_table: str
) -> tuple[int, str | None, str | None, dict]:
    """Cached wrapper around :func:`_scan_ducklake_table_metadata`.

    Cached by the DuckLake catalog's current snapshot id (catalog-wide,
    same conservative-but-never-stale token ``ducklake_current_snapshot_id``
    documents) so a status poller calling ``get_table_info`` on a ~60s
    cadence (see ``backend/core/_duckdb_status.py::refresh_config_status``)
    only re-scans after a NEW commit, not on every tick.
    """
    from backend.core.iceberg._ducklake import ducklake_current_snapshot_id

    source_key = source.get("service_id") or source.get("name") or "default"
    cache_key = (str(source_key), lake_table)
    snap_id = ducklake_current_snapshot_id(con)

    with _ducklake_table_metadata_cache_lock:
        cached = _ducklake_table_metadata_cache.get(cache_key)
    if cached is not None and cached[0] == snap_id:
        return cached[1]

    result = _scan_ducklake_table_metadata(con, lake_table)
    with _ducklake_table_metadata_cache_lock:
        _ducklake_table_metadata_cache[cache_key] = (snap_id, result)
    return result


def ducklake_table_exists(source: dict, table_name: str = "logs") -> bool:
    """Whether ``table_name``'s DuckLake table has been created for ``source``.

    Existence means a row in ``ducklake_table_info`` — the table exists in
    the catalog schema — regardless of whether it has any physical parquet
    files yet. Small commits land "inlined" directly in the catalog
    metadata with zero files (see ``_ducklake.py`` module docstring), so a
    file-count-based check would wrongly report "not found" for a table
    that genuinely has committed rows. Raises on a real attach failure
    (bad credentials/config) rather than swallowing it, so callers can
    distinguish "not found" from "couldn't check".
    """
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name

    con = get_connection(source, skip_view_update=True)
    try:
        if not _ducklake_attach(con, source, read_only=True):
            raise RuntimeError("failed to attach DuckLake catalog")
        lake_table = ducklake_table_name(source, table_name)
        row = con.execute("SELECT 1 FROM ducklake_table_info('lake') WHERE table_name = ?", [lake_table]).fetchone()
        return row is not None
    finally:
        try:
            con.close()
        except Exception:
            pass


def get_table_info(source: dict, table=None, table_name: str = "logs") -> dict:
    """Return snapshot count, data file count, total size, and latest snapshot time.

    DuckLake-native (v3 write-path cutover). ``table`` is accepted only for
    call-site backward compatibility — a pyiceberg ``Table`` object no
    longer carries meaningful state, so it's ignored.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import (
        _default_data_path,
        _ducklake_attach,
        ducklake_table_name,
    )

    zeroed = {
        "table_name": table_name,
        "snapshots": 0,
        "data_files": 0,
        "size_bytes": 0,
        "latest_snapshot_at": None,
        "buffer_files": 0,
        "buffer_size_bytes": 0,
        "table_location": None,
        "region": source.get("region"),
        "min_timestamp": None,
        "max_timestamp": None,
    }

    con = None
    try:
        con = get_connection(source, skip_view_update=True)
        if not _ducklake_attach(con, source, read_only=True):
            raise RuntimeError("failed to attach DuckLake catalog")

        lake_table = ducklake_table_name(source, table_name)
        info_row = con.execute(
            "SELECT table_id, file_count, file_size_bytes FROM ducklake_table_info('lake') WHERE table_name = ?",
            [lake_table],
        ).fetchone()
        if info_row is None:
            return {**zeroed, "error": f"DuckLake table not found: {lake_table}"}

        table_id, file_count, file_size_bytes = info_row
        file_count = int(file_count or 0)
        file_size_bytes = int(file_size_bytes or 0)

        snap_row = con.execute(
            "SELECT count(*), max(snapshot_time) FROM ducklake_snapshots('lake') "
            "WHERE list_contains(flatten(map_values(changes)), ?)",
            [str(table_id)],
        ).fetchone()
        snapshot_count = int(snap_row[0] or 0) if snap_row else 0
        latest_ts = snap_row[1].isoformat() if snap_row and snap_row[1] is not None else None

        row_count, min_ts, max_ts, _calendar = _get_cached_or_scan_ducklake_metadata(con, source, lake_table)

        # Small inserts land "inlined" in the catalog metadata rather than
        # as parquet files (see module-level comment above and
        # `_ducklake.py`'s docstring). `ducklake_table_info` then reports
        # file_count=0 for a table that genuinely has committed rows,
        # which would otherwise show a misleading "0 files" until the next
        # compaction flushes them — fall back to the scanned row count so
        # the panel reflects reality in the meantime.
        data_files = file_count if file_count > 0 else row_count

        buf = _core_mod.buffer_files(source, table_name=table_name)
        buf_size = sum(os.path.getsize(p) for p in buf if os.path.exists(p))

        data_path = svcconfig.DUCKLAKE_DATA_PATH or _default_data_path(source)

        return {
            "table_name": table_name,
            "snapshots": snapshot_count,
            "data_files": data_files,
            "size_bytes": file_size_bytes,
            "latest_snapshot_at": latest_ts,
            "buffer_files": len(buf),
            "buffer_size_bytes": buf_size,
            "table_location": data_path,
            "region": source.get("region"),
            "min_timestamp": min_ts,
            "max_timestamp": max_ts,
        }
    except Exception as e:
        return {**zeroed, "error": str(e)}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def get_snapshot_calendar(source: dict, table=None, table_name: str = "logs") -> dict:
    """Return per-date row counts scanned directly from the DuckLake table.

    DuckLake-native (v3 write-path cutover) — see ``get_table_info`` and the
    module-level comment above. ``data_files`` in each day's entry is a row
    count, not a physical file count: DuckLake exposes no per-file
    manifest/partition-value API, and small commits may not have any
    physical files yet (inlined). Accepted simplification — this no longer
    silently returns stale pyiceberg partition data.
    """
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name

    con = None
    try:
        con = get_connection(source, skip_view_update=True)
        if not _ducklake_attach(con, source, read_only=True):
            return {}

        lake_table = ducklake_table_name(source, table_name)
        exists = con.execute("SELECT 1 FROM ducklake_table_info('lake') WHERE table_name = ?", [lake_table]).fetchone()
        if exists is None:
            return {}

        _row_count, _min_ts, _max_ts, calendar = _get_cached_or_scan_ducklake_metadata(con, source, lake_table)
        return calendar
    except Exception:
        return {}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Internal helpers


def _align_to_schema(table: pa.Table, target_schema: pa.Schema | None = None, source: dict | None = None) -> pa.Table:
    """Align a PyArrow table to a target schema (or dynamically generated if none provided)."""
    if target_schema is not None:
        schema = target_schema
    else:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) if source else None
        log_fields_config = cfg.get("log_fields", {}) if cfg else None
        schema = _core_mod.get_arrow_schema(log_fields_config)

    dynamic_schema_field_names = {f.name for f in schema}
    existing = {f.name: table.schema.field(f.name) for f in table.schema if f.name in dynamic_schema_field_names}
    arrays = {}
    for field in schema:
        name = field.name
        if name in existing:
            col = table.column(name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type, safe=False)
                except Exception:
                    try:
                        col = col.cast(field.type, safe=True)
                    except Exception:
                        col = pa.nulls(len(table), type=field.type)
            arrays[name] = col
        else:
            arrays[name] = pa.nulls(len(table), type=field.type)
    return pa.table(arrays, schema=schema)


def _arrow_to_duckdb(arrow_type: pa.DataType) -> str:
    """Map a PyArrow type to a DuckDB type string for the empty-view fallback."""
    mapping = {
        pa.string(): "VARCHAR",
        pa.bool_(): "BOOLEAN",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float32(): "FLOAT",
        pa.float64(): "DOUBLE",
    }
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMPTZ"
    return mapping.get(arrow_type, "VARCHAR")


def _prune_empty_dirs(root: str) -> None:
    """Remove empty subdirectories under root (bottom-up)."""
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        if dirpath == root:
            continue
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
            except Exception:
                pass
    pass
