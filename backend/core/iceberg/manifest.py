"""Iceberg manifest cache + table-info helpers.

Carved out of ``backend/core/iceberg/_core.py`` (v2.0 file-size sweep
part 1/3) so the historical monolith stays under 1500 lines.

Contains:
- ``_manifest_metadata_cache`` + lock: per-manifest aggregates that
  survive process restarts.
- ``_load_manifest_metadata_cache`` / ``_save_manifest_metadata_cache``:
  persistence helpers.
- ``_get_scan_lock`` + ``_get_cached_or_scan_metadata``: scan dedup
  + caching layer used by table-info readers.
- ``get_table_info`` + ``get_snapshot_calendar``: public-API surface
  consumed by admin endpoints.
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

                # Use parallel execution to speed up S3/CDN manifest fetches
                from concurrent.futures import ThreadPoolExecutor

                with ThreadPoolExecutor(max_workers=16) as executor:
                    results = list(executor.map(scan_manifest, manifests))

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


def get_table_info(source: dict, table=None) -> dict:
    """Return snapshot count, data file count, total size, and latest snapshot time."""
    try:
        if table is None:
            catalog = _core_mod._get_catalog(source)
            identifier = _core_mod._table_identifier(source)

            # Ensure our local view of the table is up-to-date with FOS
            _core_mod._refresh_local_catalog_metadata(catalog, source, identifier)

            table = _core_mod._load_table_cached(source, identifier, catalog)
    except Exception as e:
        return {
            "error": str(e),
            "snapshots": 0,
            "data_files": 0,
            "size_bytes": 0,
            "table_name": source.get("name", "unknown"),
        }

    snapshots = list(table.snapshots())
    current = table.current_snapshot()

    # Pre-populate total stats from snapshot summary if available (O(1) vs O(N) scan)
    summary_data_files = 0
    summary_size_bytes = 0
    if current:
        summary_data_files = int(current.summary.get("total-data-files", 0))
        summary_size_bytes = int(current.summary.get("total-files-size", 0))

    # Fetch (or scan) for calendar and min/max timestamps
    data_files, size_bytes, _, min_ts, max_ts = _get_cached_or_scan_metadata(source, table)

    # Use the more accurate summary stats if the scan was partial or failed
    if summary_data_files > data_files:
        data_files = summary_data_files
        size_bytes = summary_size_bytes

    latest_ts = None
    if current:
        latest_ts = datetime.fromtimestamp(current.timestamp_ms / 1000, tz=UTC).isoformat()

    buf = _core_mod.buffer_files(source)
    buf_size = sum(os.path.getsize(p) for p in buf if os.path.exists(p))

    return {
        "table_name": source.get("name", "unknown"),
        "snapshots": len(snapshots),
        "data_files": data_files,
        "size_bytes": size_bytes,
        "latest_snapshot_at": latest_ts,
        "buffer_files": len(buf),
        "buffer_size_bytes": buf_size,
        "table_location": table.location() if snapshots else None,
        "region": source.get("region"),
        "min_timestamp": min_ts,
        "max_timestamp": max_ts,
    }


def get_snapshot_calendar(source: dict, table=None) -> dict:
    """Return per-date file counts derived from Iceberg partition metadata."""
    try:
        if table is None:
            catalog = _core_mod._get_catalog(source)
            identifier = _core_mod._table_identifier(source)

            _core_mod._refresh_local_catalog_metadata(catalog, source, identifier)

            table = _core_mod._load_table_cached(source, identifier, catalog)
    except Exception:
        return {}

    _, _, calendar, _, _ = _get_cached_or_scan_metadata(source, table)
    return calendar


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
