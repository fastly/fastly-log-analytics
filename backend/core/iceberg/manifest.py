"""DuckLake table-info helpers and schema alignment utilities.

Contains:
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

import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any

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


# ---------------------------------------------------------------------------
# DuckLake-native table-info / calendar (v3 write-path cutover)
#
# Every commit goes through `backend.core.iceberg.buffer._commit_buffer_impl`'s
# DuckLake attach (see `backend/core/iceberg/_ducklake.py` module docstring),
# and `get_table_info`/`get_snapshot_calendar` below read DuckLake state
# directly.

_ducklake_table_metadata_cache: dict[tuple[str, str], tuple[int | None, tuple[int, str | None, str | None, dict]]] = {}
_ducklake_table_metadata_cache_lock = threading.Lock()
_ducklake_session_local = threading.local()

from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("iceberg._ducklake_table_metadata_cache", _ducklake_table_metadata_cache)


@contextmanager
def ducklake_session(source: dict):
    """Share a single read-only DuckDB connection and DuckLake attachment across
    multiple ``ducklake_table_exists`` / ``get_table_info`` / ``get_snapshot_calendar``
    calls on the current thread. Connection creation and catalog attachment are
    deferred until the first unmocked helper actually needs a connection.
    """
    prev = getattr(_ducklake_session_local, "current", None)
    state: dict = {
        "source": source,
        "con": None,
        "attached": False,
        "existing_tables": set(),
    }
    _ducklake_session_local.current = state
    try:
        yield state
    finally:
        _ducklake_session_local.current = prev
        con = state.get("con")
        if con is not None:
            try:
                con.close()
            except Exception:
                logger.debug("ducklake_session: con.close failed (ignored)", exc_info=True)


def _acquire_ducklake_con(source: dict, con=None) -> tuple[Any, bool, bool, set[str] | None]:
    """Return ``(con, attached, owns_con, existing_tables)`` for *source*.

    Reuses an explicit *con* or an active :func:`ducklake_session` when present;
    otherwise opens a fresh read-only connection that the caller must close.
    """
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach

    if con is not None:
        attached = bool(_ducklake_attach(con, source, read_only=True))
        return con, attached, False, None

    session = getattr(_ducklake_session_local, "current", None)
    if session is not None and session.get("source") is source:
        s_con = session.get("con")
        if s_con is None:
            s_con = get_connection(source, read_only=True, skip_view_update=True)
            session["con"] = s_con
            session["attached"] = bool(_ducklake_attach(s_con, source, read_only=True))
        return s_con, bool(session.get("attached")), False, session.get("existing_tables")

    fresh_con = get_connection(source, read_only=True, skip_view_update=True)
    try:
        attached = bool(_ducklake_attach(fresh_con, source, read_only=True))
    except Exception:
        try:
            fresh_con.close()
        except Exception:
            logger.debug("_acquire_ducklake_con: fresh_con.close failed (ignored)", exc_info=True)
        raise
    return fresh_con, attached, True, None


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


def ducklake_table_exists(source: dict, table_name: str = "logs", *, con=None) -> bool:
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
    from backend.core.iceberg._ducklake import ducklake_table_name

    active_con, attached, owns_con, existing_tables = _acquire_ducklake_con(source, con)
    try:
        if not attached:
            raise RuntimeError("failed to attach DuckLake catalog")
        lake_table = ducklake_table_name(source, table_name)
        row = active_con.execute(
            "SELECT 1 FROM ducklake_table_info('lake') WHERE table_name = ?", [lake_table]
        ).fetchone()
        exists = row is not None
        if exists and existing_tables is not None:
            existing_tables.add(lake_table)
        return exists
    finally:
        if owns_con and active_con is not None:
            try:
                active_con.close()
            except Exception:
                logger.debug("ducklake_table_exists: con.close failed (ignored)", exc_info=True)


def get_table_info(source: dict, table=None, table_name: str = "logs", *, con=None) -> dict:
    """Return snapshot count, data file count, total size, and latest snapshot time.

    DuckLake-native (v3 write-path cutover). ``table`` is accepted only for
    call-site backward compatibility — a pyiceberg ``Table`` object no
    longer carries meaningful state, so it's ignored.
    """
    from backend import config as svcconfig
    from backend.core.iceberg._ducklake import (
        _default_data_path,
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

    active_con = None
    owns_con = False
    try:
        active_con, attached, owns_con, existing_tables = _acquire_ducklake_con(source, con)
        if not attached:
            raise RuntimeError("failed to attach DuckLake catalog")

        lake_table = ducklake_table_name(source, table_name)
        info_row = active_con.execute(
            "SELECT table_id, file_count, file_size_bytes FROM ducklake_table_info('lake') WHERE table_name = ?",
            [lake_table],
        ).fetchone()
        if info_row is None:
            return {**zeroed, "error": f"DuckLake table not found: {lake_table}"}
        if existing_tables is not None:
            existing_tables.add(lake_table)

        table_id, file_count, file_size_bytes = info_row
        file_count = int(file_count or 0)
        file_size_bytes = int(file_size_bytes or 0)

        snap_row = active_con.execute(
            "SELECT count(*), max(snapshot_time) FROM ducklake_snapshots('lake') "
            "WHERE list_contains(flatten(map_values(changes)), ?)",
            [str(table_id)],
        ).fetchone()
        snapshot_count = int(snap_row[0] or 0) if snap_row else 0
        latest_ts = snap_row[1].isoformat() if snap_row and snap_row[1] is not None else None

        row_count, min_ts, max_ts, _calendar = _get_cached_or_scan_ducklake_metadata(active_con, source, lake_table)

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
        if owns_con and active_con is not None:
            try:
                active_con.close()
            except Exception:
                logger.debug("get_table_info: con.close failed (ignored)", exc_info=True)


def get_snapshot_calendar(source: dict, table=None, table_name: str = "logs", *, con=None) -> dict:
    """Return per-date row counts scanned directly from the DuckLake table.

    DuckLake-native (v3 write-path cutover) — see ``get_table_info`` and the
    module-level comment above. ``data_files`` in each day's entry is a row
    count, not a physical file count: DuckLake exposes no per-file
    manifest/partition-value API, and small commits may not have any
    physical files yet (inlined). Accepted simplification — this no longer
    silently returns stale pyiceberg partition data.
    """
    from backend.core.iceberg._ducklake import ducklake_table_name

    active_con = None
    owns_con = False
    try:
        active_con, attached, owns_con, existing_tables = _acquire_ducklake_con(source, con)
        if not attached:
            return {}

        lake_table = ducklake_table_name(source, table_name)
        if existing_tables is None or lake_table not in existing_tables:
            exists = active_con.execute(
                "SELECT 1 FROM ducklake_table_info('lake') WHERE table_name = ?", [lake_table]
            ).fetchone()
            if exists is None:
                return {}
            if existing_tables is not None:
                existing_tables.add(lake_table)

        _row_count, _min_ts, _max_ts, calendar = _get_cached_or_scan_ducklake_metadata(active_con, source, lake_table)
        return calendar
    except Exception:
        return {}
    finally:
        if owns_con and active_con is not None:
            try:
                active_con.close()
            except Exception:
                logger.debug("get_snapshot_calendar: con.close failed (ignored)", exc_info=True)


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
                logger.debug("_prune_empty_dirs: os.rmdir failed (ignored)", exc_info=True)
