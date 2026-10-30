"""DuckDB view stitching over DuckLake + local buffer, stale-view self-heal, and table/snapshot calendar queries."""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from backend.core.duckdb import escape_sql_literal
from backend.core.retry import retry_call
from backend.core.storage.buffer import buffer_files
from backend.core.storage.catalog import (
    _arrow_to_duckdb,
    _default_data_path,
    _ducklake_attach,
    _get_service_lock,
    _view_cache,
    clear_source_caches,
    configure_duckdb_s3,
    ducklake_current_snapshot_id,
    ducklake_table_name,
    get_arrow_schema,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

logger = logging.getLogger("backend.core.storage._core")

# ── DuckLake-native table-info / calendar + schema alignment (folded from manifest.py) ──
#
# Every commit goes through `backend.core.storage.buffer._commit_buffer_impl`'s
# DuckLake attach (see `backend/core/storage/_ducklake.py` module docstring),
# and `get_table_info`/`get_snapshot_calendar` below read DuckLake state
# directly.

_ducklake_table_metadata_cache: dict[tuple[str, str], tuple[int | None, tuple[int, str | None, str | None, dict]]] = {}
_ducklake_table_metadata_cache_lock = threading.Lock()
_ducklake_session_local = threading.local()

_CacheRegistry.register("storage._ducklake_table_metadata_cache", _ducklake_table_metadata_cache)


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


def _qualified_lake_ident(lake_table: str) -> str:
    return 'lake."{}"'.format(lake_table.replace('"', '""'))


def _scan_ducklake_table_metadata(con, lake_table: str) -> tuple[int, str | None, str | None, dict]:
    """Full scan of ``lake.<lake_table>`` for row count, min/max timestamp,
    and a per-day row-count calendar.

    DuckLake exposes no partition-value summary API at the SQL level, and
    small commits get "inlined" into the
    catalog metadata rather than written as parquet files — so there are no
    reliable per-file stats to read either way (see
    ``backend/core/storage/_ducklake.py`` module docstring). A direct scan
    is the only approach that stays correct in both cases. Callers should
    go through ``_get_cached_or_scan_ducklake_metadata`` rather than call
    this directly — it's O(table size) and this codebase treats "MANY
    small files, cheap incremental reads" as the standing perf contract.
    """
    ident = _qualified_lake_ident(lake_table)
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
    """Whether ``table_name``'s DuckLake table has been created for ``source``."""

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
    """Return snapshot count, data file count, total size, and latest snapshot time."""
    from backend import config as svcconfig

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

        data_files = file_count if file_count > 0 else row_count

        buf = buffer_files(source, table_name=table_name)
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
    """Return per-date row counts scanned directly from the DuckLake table."""

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


# ── View binding + stale-view self-heal (folded from view.py) ──


def _source_variant_fp(source: dict) -> tuple:
    """Hashable fingerprint of the per-source inputs that materially
    affect the generated view SQL beyond the schema fields.

    Finding 002 (2026-06-15): ``_update_storage_view_locked`` appends a
    WHERE clause derived from ``source["time_range"]``. Without it in the
    ``_view_cache`` key tuple, a cached SQL string from one variant
    could be served back to a request that wrote a different time
    range — silently applying the wrong WHERE clause to the result
    set. Cheap defense in depth: include it in the key.

    ``provisioning.cron_sync.enabled`` is also folded in because the
    same WHERE-clause branch fires when cron sync is disabled.
    """
    tr = source.get("time_range")
    if isinstance(tr, dict):
        tr_fp: tuple = (tr.get("start"), tr.get("end"))
    else:
        tr_fp = ()
    cron_enabled = bool(((source.get("provisioning") or {}).get("cron_sync") or {}).get("enabled", True))
    return (tr_fp, cron_enabled)


# Per-source rebuild signal: source_key -> Event set when an in-progress
# slow-path rebuild finishes. Lets cold parallel waiters wake and use
# fast-path-without-lock instead of stepping through the lock serially.
_rebuild_signals: dict[str, threading.Event] = {}
_rebuild_signals_lock = threading.Lock()


def is_stale_view_error(exc: BaseException) -> bool:
    """Return True when ``exc`` looks like the storage view referencing a
    buffer parquet the commit cycle has already swept.

    Mirror of :func:`backend.repositories._base._is_stale_view_error` —
    promoted here so non-repository call paths (background crons /
    discovery / rollup writers) can share the same detection without
    importing through the repositories layer (would invert the
    architecture). The repositories alias still resolves the same way;
    new sites should import :func:`is_stale_view_error` directly from
    :mod:`backend.core.storage`.
    """
    msg = str(exc)
    return (
        "No files found" in msg
        or "Catalog Error: Table with name" in msg
        or "No such file or directory" in msg
        or "Cannot open file" in msg
    )


def execute_with_stale_view_retry(con, source: dict, fn, *args, table_name="logs", **kwargs):
    """Run ``fn(con, *args, **kwargs)`` with one stale-view self-heal retry.

    On a :func:`is_stale_view_error`-shaped failure, bust the cached view
    SQL via :func:`clear_source_caches` (keep_snapshot_cache=True, same
    pattern QueryRunner.execute uses), force-rebind the view via
    :func:`update_storage_view`, then re-invoke ``fn`` once.

    Non-stale errors propagate immediately. Second-attempt failures
    (including non-stale ones) propagate too — the caller decides
    whether to log + fall through or treat as fatal.

    Use this in background-job code paths that open a raw DuckDB
    connection and don't have QueryRunner's built-in retry. Three
    documented sites today (one in rdns_cache discovery, two in
    rollups DESCRIBE) all surfaced the same buffer-deletion-race
    symptom in prod on 2026-06-10 between the deploy at 06:49 UTC
    and an external restart at 14:39 UTC.

    The arguments mirror the inline retry pattern in
    :meth:`QueryRunner.execute_with_retry` so a caller refactor can
    swap one for the other.
    """

    def _recover(_exc: BaseException) -> None:
        clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
        update_storage_view(con, source, force=True, target_table=table_name)

    return retry_call(
        lambda: fn(con, *args, **kwargs),
        retry_on=is_stale_view_error,
        attempts=2,
        before_retry=_recover,
    )


def get_last_view_stats(source: dict) -> dict:
    source_key = source.get("name", "default")
    cached = _view_cache.get(source_key)
    if cached and len(cached) >= 6:
        return {"sql": cached[3], "time_ms": cached[4], "was_fast_path": cached[5]}
    return {}


def inject_view_debug(debug_list: list, source: dict):
    stats = get_last_view_stats(source)
    if stats and stats.get("sql"):
        # Apply the same path-list compaction as the per-query recorder
        # in repositories/_base. The view-build SQL is the WORST offender
        # because it inlines every buffer file twice (in the UNION ALL
        # RHS) — pre-compaction it accounted for ~30 KB on its own in
        # the dashboard response.
        from backend.repositories._base import _compact_sql_for_debug

        mode = (
            "FAST PATH (Local Cache / Buffer Match)"
            if stats.get("was_fast_path")
            else "SLOW PATH (S3 Read / Manifest Resolve)"
        )
        debug_list.insert(
            0,
            {
                "sql": _compact_sql_for_debug(f"-- DuckDB Storage View Resolution [{mode}] --\n{stats['sql']}"),
                "time_ms": stats["time_ms"],
                "engine": "DuckDB",
            },
        )


def _view_target_name(source: dict, target_table: str) -> str:
    """The DuckDB view name both paths bind — repositories query this name
    via ``_safe_table(src["name"])``, so fast path and slow path MUST agree
    (the fast-path reconstruction previously bound the literal ``logs``)."""
    from backend.core.duckdb import _safe_table_name

    if target_table != "logs":
        return target_table
    return _safe_table_name(source.get("name") or source.get("service_id") or "default")


def _ducklake_view_token(con) -> str | None:
    """Staleness token for the cached view SQL, derived from the DuckLake
    catalog's current snapshot id. A commit anywhere in the catalog bumps
    it, forcing a rebuild; in the steady state (no commit since the last
    build) the token matches and reads stay on the lock-free fast path.
    Returns None when the token can't be derived (lake not attached) —
    callers must treat None as "cannot verify freshness" and rebuild.
    """

    snap = ducklake_current_snapshot_id(con)
    return None if snap is None else f"ducklake:{snap}"


def _empty_schema_select(dynamic_arrow_schema) -> str:
    """Zero-row SELECT carrying every schema column (typed NULLs) so a
    service with no lake table and no buffer still binds a queryable view."""
    cols = ", ".join(
        'NULL::{} AS "{}"'.format(_arrow_to_duckdb(f.type), f.name.replace('"', '""')) for f in dynamic_arrow_schema
    )
    return f"SELECT {cols} WHERE false"


def _existing_union_cols(con, committed_parts: list[str], buf_probe_path: str | None) -> set[str]:
    """Column names present in the raw (lake ∪ buffer) union output.

    Probes each committed part with ``LIMIT 0`` (catalog-only, no data
    read) plus a SINGLE buffer parquet (all buffer files are aligned to
    the same schema by ``write_to_buffer``, so one probe suffices — the
    same trick that avoids schema-binding every
    buffer file).
    """
    cols: set[str] = set()
    for part in committed_parts:
        try:
            desc = con.execute(f"SELECT * FROM ({part}) LIMIT 0").description or []
            cols.update(d[0] for d in desc)
        except Exception as e:
            logger.info("[storage] view column probe failed for committed part: %s", e)
    if buf_probe_path:
        try:
            desc = (
                con.execute(
                    f"SELECT * FROM read_parquet('{escape_sql_literal(buf_probe_path)}', hive_partitioning=false) LIMIT 0"
                ).description
                or []
            )
            cols.update(d[0] for d in desc)
        except Exception as e:
            logger.info("[storage] view column probe failed for buffer parquet %s: %s", buf_probe_path, e)
    return cols


def _finalize_view_sql(
    union_sql: str,
    source: dict,
    target_table: str,
    dynamic_schema_field_names: set[str],
    existing_cols: set[str],
) -> str:
    """Shared projection + clamp applied to the raw (lake ∪ buffer) union.

    Single source of truth for BOTH the lock-free fast-path reconstruction
    and the locked slow-path rebuild, so the two can never drift. Restores
    everything the pre-DuckLake view guaranteed downstream:

    - ``timestamp_hour`` / ``dt`` computed columns (partition-pruning
      WHEREs in alerts.py and the rollup readers depend on them);
    - the ``c_speed`` / ``p_type`` / ``p_desc`` decode CASEs and the
      ``ttl`` / ``age`` integer rounding (logs table only);
    - the ``time_range`` WHERE clamp for cron-disabled manual imports.
    """
    sql = union_sql

    # 1. Computed partition columns. Strip any real column of the same
    #    name first (hive-partition reads may carry one) so the
    #    computed definition wins.
    cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in existing_cols)
    exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
    sql = (
        f"SELECT *{exclude_clause}, "
        f"CAST(strftime(timestamp, '%Y-%m-%d-%H') AS VARCHAR) as timestamp_hour, "
        f"CAST(strftime(timestamp, '%Y-%m-%d') AS VARCHAR) as dt "
        f"FROM ({sql})"
    )

    # 2. Storage-code decodes + rounding (logs only).
    if target_table == "logs":
        from backend.core import log_fields as fc

        exclude_cols: list[str] = []
        select_extras: list[str] = []
        for col, encode_map in (
            ("c_speed", fc.CONN_SPEED_ENCODE),
            ("p_type", fc.PROXY_TYPE_ENCODE),
            ("p_desc", fc.PROXY_DESC_ENCODE),
        ):
            if col in existing_cols:
                exclude_cols.append(col)
                select_extras.append(f"{fc.duckdb_decode_case(col, encode_map)} AS {col}")
        if "ttl" in existing_cols and "ttl" in dynamic_schema_field_names:
            exclude_cols.append("ttl")
            select_extras.append('CAST(ROUND("ttl") AS INTEGER) AS ttl')
        if "age" in existing_cols and "age" in dynamic_schema_field_names:
            exclude_cols.append("age")
            select_extras.append('CAST(ROUND("age") AS INTEGER) AS age')
        if exclude_cols:
            sql = f"SELECT * EXCLUDE ({', '.join(exclude_cols)}), {', '.join(select_extras)} FROM ({sql})"

    # 3. Manual-import time clamp.
    tr = source.get("time_range")
    if tr and not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True):
        import dateutil.parser as _dt

        where_clauses = []
        if tr.get("start"):
            try:
                start_iso = _dt.isoparse(str(tr["start"])).isoformat()
            except (ValueError, TypeError) as e:
                raise ValueError(f"invalid time_range start: {e}") from e
            where_clauses.append(f"timestamp >= '{escape_sql_literal(start_iso)}'::TIMESTAMPTZ")
        if tr.get("end"):
            try:
                end_iso = _dt.isoparse(str(tr["end"])).isoformat()
            except (ValueError, TypeError) as e:
                raise ValueError(f"invalid time_range end: {e}") from e
            where_clauses.append(f"timestamp <= '{escape_sql_literal(end_iso)}'::TIMESTAMPTZ")
        if where_clauses:
            sql = f"SELECT * FROM ({sql}) WHERE {' AND '.join(where_clauses)}"

    return sql


def _try_fast_path_view(con, source: dict, target_table: str = "logs") -> bool:
    """Bind the per-service view from cache without acquiring the lock.

    Returns True if the view was bound; False if a slow-path rebuild is
    needed. Safe to call concurrently — all reads are race-free against
    a concurrent slow-path writer (cached tuple refs are stable; the
    only write here is a benign timestamp update on _view_cache).

    This split exists so 6 parallel dashboard requests for the same
    source don't serialize on the per-service RLock that ingest also
    holds during buffer commits.
    """
    t_start = time.time()
    source_key = source.get("name", "default")
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key

    from backend.core.duckdb_pool import _get_conn_state, _set_conn_state

    fp_key = f"fast_path_view_fp_{target_table}"
    if _get_conn_state(con, fp_key) is None:
        configure_duckdb_s3(con)

    from backend import config as svcconfig

    durable_serving = svcconfig.is_durable_serving_mode(source)
    if durable_serving:
        buf_files = []
    else:
        try:
            buf_files = buffer_files(source, table_name=target_table)
        except TypeError:
            buf_files = buffer_files(source)
    buf_set = frozenset(buf_files)

    # DuckLake-aware staleness token (the old metadata_location
    # pointer never matched the slow path's cached
    # literal post-DuckLake — every read paid the RLock + full rebuild,
    # the 12s+ page-load class regression this fast path exists to avoid).
    metadata_loc = _ducklake_view_token(con)
    if metadata_loc is None:
        return False

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = get_arrow_schema(log_fields_config, table_name=target_table)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    cached = _view_cache.get(cache_key)

    schema_fp = tuple(sorted(dynamic_schema_field_names))
    variant_fp = _source_variant_fp(source)

    only_buf_changed = False
    if (
        cached
        and len(cached) > 7
        and cached[0] == metadata_loc
        and cached[1] != buf_set
        and cached[2] == schema_fp
        and (len(cached) > 6 and cached[6] == variant_fp)
    ):
        only_buf_changed = True

    if not only_buf_changed and not (
        cached
        and cached[0] == metadata_loc
        and cached[1] == buf_set
        and cached[2] == schema_fp
        and (len(cached) > 6 and cached[6] == variant_fp)
    ):
        return False

    if only_buf_changed:
        if cached is None:
            raise RuntimeError("cached is unexpectedly None")
        try:
            committed_sql_parts = cached[7]
            new_parts = list(committed_sql_parts) if committed_sql_parts else []

            active_buf_files = [p for p in buf_files if os.path.isfile(p)]
            if active_buf_files:
                paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in active_buf_files)
                new_parts.append(
                    f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"
                )

            if not new_parts:
                union_sql = _empty_schema_select(dynamic_arrow_schema)
                existing_cols = set(dynamic_schema_field_names)
            else:
                union_sql = " UNION ALL BY NAME ".join(new_parts)
                if len(cached) > 8 and cached[8] and (cached[1] or cached[7]):
                    existing_cols = set(cached[8])
                else:
                    existing_cols = _existing_union_cols(
                        con,
                        list(committed_sql_parts) if committed_sql_parts else [],
                        active_buf_files[0] if active_buf_files else None,
                    )

            final_sql = _finalize_view_sql(union_sql, source, target_table, dynamic_schema_field_names, existing_cols)

            create_stmt = f"CREATE OR REPLACE VIEW {_view_target_name(source, target_table)} AS {final_sql}"

            _view_cache[cache_key] = (
                metadata_loc,
                buf_set,
                schema_fp,
                create_stmt,
                round((time.time() - t_start) * 1000, 2),
                True,
                variant_fp,
                committed_sql_parts,
                frozenset(existing_cols),
            )
            cached = _view_cache[cache_key]
        except Exception as e:
            logger.warning("[storage] Incremental fast-path reconstruction failed: %s", e)
            return False

    if cached is None:
        raise RuntimeError("cached is unexpectedly None")
    view_sql = cached[3]
    committed_sql_parts = cached[7] if len(cached) > 7 else ()
    cached_existing_cols = cached[8] if len(cached) > 8 else frozenset()
    if view_sql:
        bound_fp = _get_conn_state(con, fp_key)
        target_fp = (metadata_loc, buf_set, schema_fp, variant_fp, target_table)
        if bound_fp != target_fp:
            exec_sql = view_sql
            if view_sql.startswith("CREATE OR REPLACE VIEW "):
                exec_sql = view_sql.replace("CREATE OR REPLACE VIEW ", "CREATE OR REPLACE TEMP VIEW ", 1)
            try:
                con.execute(exec_sql)
            except Exception as e:
                logger.warning("[storage] fast-path view re-bind failed for %s: %s", source_key, e)
                return False
            _set_conn_state(con, **{fp_key: target_fp})

    if not cached[5]:
        t_end = time.time()
        _view_cache[cache_key] = (
            metadata_loc,
            buf_set,
            tuple(sorted(dynamic_schema_field_names)),
            view_sql,
            round((t_end - t_start) * 1000, 2),
            True,
            variant_fp,
            committed_sql_parts,
            cached_existing_cols,
        )
    return True


def _rebuild_locked(con, source: dict, source_key: str, target_table: str = "logs", force: bool = False) -> None:
    """Run the slow path under the lock and signal completion."""
    ev = threading.Event()
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key
    with _rebuild_signals_lock:
        _rebuild_signals[cache_key] = ev
    try:
        try:
            _update_storage_view_locked(con, source, target_table=target_table, force=force)
        except TypeError:
            _update_storage_view_locked(con, source)
    finally:
        ev.set()
        with _rebuild_signals_lock:
            if _rebuild_signals.get(cache_key) is ev:
                del _rebuild_signals[cache_key]


def update_storage_view(
    con, source: dict, lock_timeout: float = 5.0, force: bool = False, target_table: str = "logs"
) -> None:
    """Refresh the per-service DuckDB view over the DuckLake table + buffer."""
    source_key = source.get("name", "default")
    if source_key.endswith("::rum") and target_table == "logs":
        update_storage_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_vitals")
        update_storage_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_errors")
        return

    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key

    if not force and _try_fast_path_view(con, source, target_table=target_table):
        return

    lock = _get_service_lock(source_key)

    if not lock.acquire(blocking=False):
        with _rebuild_signals_lock:
            ev = _rebuild_signals.get(cache_key)
        if ev is not None and ev.wait(timeout=lock_timeout):
            if _try_fast_path_view(con, source, target_table=target_table):
                return
        if not lock.acquire(timeout=lock_timeout):
            cached = _view_cache.get(cache_key)
            if cached and cached[3] and len(cached) > 6 and cached[6] == _source_variant_fp(source):
                try:
                    con.execute(cached[3])
                except Exception:
                    logger.debug("update_storage_view: con.execute failed (ignored)", exc_info=True)
                return
            if not force and _persistent_view_exists(con, source, target_table=target_table):
                return
            logger.info(
                "[storage] %s: cache empty and no persistent view for %s; extending lock "
                "wait to avoid 'table not found' on caller",
                source_key,
                target_table,
            )
            if not lock.acquire(timeout=60.0):
                logger.warning(
                    "[storage] %s: extended 60s lock wait timed out; view %s rebuild deferred",
                    source_key,
                    target_table,
                )
                return
            try:
                _rebuild_locked(con, source, source_key, target_table=target_table, force=force)
            finally:
                lock.release()
            return
    try:
        _rebuild_locked(con, source, source_key, target_table=target_table, force=force)
    finally:
        lock.release()


def _persistent_view_exists(con, source: dict, target_table: str = "logs") -> bool:
    """Return True if the per-service storage view already exists on this
    connection's database."""
    try:
        from backend.core.duckdb import _safe_table_name

        table_name = (
            target_table
            if target_table != "logs"
            else _safe_table_name(source.get("name") or source.get("service_id") or "default")
        )
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _update_storage_view_locked(con, source: dict, target_table: str = "logs", force: bool = False) -> None:
    t_start = time.time()
    view_name = _view_target_name(source, target_table)

    is_read_only = False
    try:
        res = con.execute(
            "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp','lake') LIMIT 1"
        ).fetchone()
        is_read_only = bool(res and res[0])
    except Exception as e:
        logger.error("[storage] duckdb_databases probe failed: %s", e)

    lake_attached = con.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake' LIMIT 1").fetchone()
    if not lake_attached:
        _ducklake_attach(con, source, read_only=is_read_only)

    from backend import config as svcconfig

    durable_serving = svcconfig.is_durable_serving_mode(source)
    if durable_serving:
        buf_files = []
    else:
        try:
            buf_files = buffer_files(source, table_name=target_table)
        except TypeError:
            buf_files = buffer_files(source)

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = get_arrow_schema(log_fields_config, table_name=target_table)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    lake_table = ducklake_table_name(source, target_table)
    committed_parts: list[str] = []
    token = _ducklake_view_token(con)
    res = con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
        [lake_table],
    ).fetchone()
    if res:
        snapshot_id = ducklake_current_snapshot_id(con)
        if snapshot_id is None:
            raise RuntimeError("Cannot verify the DuckLake snapshot for the committed view")
        token = f"ducklake:{snapshot_id}"
        committed_parts.append(f'SELECT * FROM lake."{lake_table}" AT (VERSION => {snapshot_id})')

    parts = list(committed_parts)
    if buf_files:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in buf_files)
        parts.append(f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)")

    if not parts:
        union_sql = _empty_schema_select(dynamic_arrow_schema)
        existing_cols = set(dynamic_schema_field_names)
    else:
        union_sql = " UNION ALL BY NAME ".join(parts)
        existing_cols = _existing_union_cols(con, committed_parts, buf_files[0] if buf_files else None)

    final_sql = _finalize_view_sql(union_sql, source, target_table, dynamic_schema_field_names, existing_cols)

    if is_read_only or durable_serving:
        create_stmt = f"CREATE OR REPLACE TEMP VIEW {view_name} AS {final_sql}"
    else:
        try:
            con.execute(f"DROP VIEW IF EXISTS temp.main.{view_name}")
        except Exception:
            logger.debug("_update_storage_view_locked: drop temp view %s failed (ignored)", view_name, exc_info=True)
        create_stmt = f"CREATE OR REPLACE VIEW {view_name} AS {final_sql}"

    try:
        con.execute(create_stmt)
    except Exception as e:
        if "write-write conflict" in str(e).lower():
            try:
                con.rollback()
            except Exception:
                logger.debug("_update_storage_view_locked: rollback failed (ignored)", exc_info=True)
            try:
                con.execute(create_stmt)
            except Exception:
                try:
                    con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS {final_sql}")
                except Exception as fallback_err:
                    logger.error("[storage] Failed to create view %s: %s", view_name, fallback_err)
                    if durable_serving:
                        raise
        else:
            logger.error("[storage] Failed to create view %s: %s", view_name, e)
            if durable_serving:
                raise

    source_key = source.get("name", "default")
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key
    variant_fp = _source_variant_fp(source)
    schema_fp = tuple(sorted(dynamic_schema_field_names))
    _view_cache[cache_key] = (
        token,
        frozenset(buf_files),
        schema_fp,
        create_stmt,
        round((time.time() - t_start) * 1000, 2),
        False,
        variant_fp,
        tuple(committed_parts),
        frozenset(existing_cols),
    )
    try:
        from backend.core.duckdb_pool import _set_conn_state

        fp_key = f"fast_path_view_fp_{target_table}"
        _set_conn_state(con, **{fp_key: (token, frozenset(buf_files), schema_fp, variant_fp, target_table)})
    except Exception:
        logger.debug("_update_storage_view_locked: _set_conn_state failed (ignored)", exc_info=True)
