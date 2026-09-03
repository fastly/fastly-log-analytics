"""Iceberg view binding + snapshot cache + stale-view self-heal.

Carved out of ``backend/core/iceberg/_core.py`` (v2.0 file-size sweep
part 3/3). Holds the read-path view machinery:

- ``configure_duckdb_s3``: per-connection DuckDB extension/secret setup.
- ``_get_service_lock``: per-service iceberg-write lock.
- ``is_stale_view_error`` / ``execute_with_stale_view_retry``: self-heal
  helper used by rdns_cache, rollups DESCRIBE, and /api/query (added
  after the 2026-06-10 stale-buffer prod incident).
- ``clear_source_caches``: bust the cached view SQL + snapshot dicts.
- Persistent snapshot cache: ``_load_persistent_cache`` /
  ``_save_persistent_cache`` / ``_update_snapshot_cache_from_delta`` /
  ``_reconcile_snapshot_cache_after_sync``.
- ``get_last_view_stats`` / ``inject_view_debug``: debug-panel surface.
- ``_try_fast_path_view`` / ``_rebuild_locked`` / ``update_iceberg_view``:
  the actual TEMP VIEW DDL + cache invalidation logic.
- ``_persistent_view_exists`` / ``_update_iceberg_view_locked``: the
  slow-path rebuild guarded by ``_get_service_lock``.

All public names are re-exported back into ``backend.core.iceberg._core``
at the bottom of that module so the package proxy + test monkeypatch
sites keep resolving to the same canonical binding.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import UTC, datetime, timedelta

logger = logging.getLogger("backend.core.iceberg._core")

# Library + util imports the carved code references at module scope.
# Late-bind helpers from the main _core module — bare-name resolution
# for any unmoved global falls through __getattr__ to _core.
from backend.core.iceberg import _core as _core_mod
from backend.utils.sql_validator import escape_sql_literal


def __getattr__(name: str):
    return getattr(_core_mod, name)


def configure_duckdb_s3(con) -> None:
    """Install/load DuckDB extensions for Iceberg + httpfs.

    The fos_proxy SECRET (created in backend.core.duckdb._configure_fos) is
    the sole S3 routing config; this function used to also `SET s3_endpoint`
    etc., but those settings would clobber the proxy's endpoint scoping for
    unmatched URLs and silently bypass telemetry.
    """
    try:
        con.execute("LOAD iceberg; LOAD avro; LOAD httpfs; LOAD parquet;")
    except Exception:
        try:
            con.execute("INSTALL iceberg; INSTALL avro; INSTALL httpfs; INSTALL parquet;")
            con.execute("LOAD iceberg; LOAD avro; LOAD httpfs; LOAD parquet;")
        except Exception:
            # Don't swallow silently: a connection left without the iceberg/
            # httpfs extensions surfaces later as a confusing "iceberg_scan
            # does not exist" error far from here. Log + re-raise so the real
            # setup failure is visible at the point it happens.
            logger.exception("configure_duckdb_s3: failed to INSTALL/LOAD DuckDB extensions")
            raise


# Per-service locks to avoid global bottleneck during S3 manifest scans
_service_locks: dict[str, threading.RLock] = {}
_service_locks_lock = threading.Lock()


def _get_service_lock(source_key: str) -> threading.RLock:
    with _service_locks_lock:
        if source_key not in _service_locks:
            _service_locks[source_key] = threading.RLock()
        return _service_locks[source_key]


# Per-source view cache: source_key -> (metadata_loc, buf_set, schema_fields_tuple, view_sql, time_ms, was_fast_path, source_variant_fp)
#
# ``source_variant_fp`` (added 2026-06-15, finding 002) is a short tuple
# capturing the per-source-dict inputs that materially affect the
# generated view SQL beyond the schema fields: ``access_level`` and
# ``time_range``. Both come from the per-service config today, but the
# cron's manual-import path mutates ``time_range`` temporarily on the
# in-memory source dict during analyst-driven backfills — without the
# fingerprint, a concurrent dashboard call could read back the cached
# SQL from a moment when the WHERE clause had a different time bound.
# Cheap defense-in-depth: cache miss on any drift, cache hit when the
# variant is stable. See ``_source_variant_fp`` below.
_view_cache: dict[str, tuple] = {}


def _source_variant_fp(source: dict) -> tuple:
    """Hashable fingerprint of the per-source inputs that materially
    affect the generated view SQL beyond the schema fields.

    Finding 002 (2026-06-15): ``_update_iceberg_view_locked`` appends a
    WHERE clause derived from ``source["time_range"]`` and the analyst
    flag derived from ``source["access_level"]``. Without these in the
    ``_view_cache`` key tuple, a cached SQL string from one variant
    could be served back to a request that wrote a different time
    range — silently applying the wrong WHERE clause to the result
    set. Cheap defense in depth: include both in the key.

    ``provisioning.cron_sync.enabled`` is also folded in because the
    same WHERE-clause branch fires when cron sync is disabled.
    """
    tr = source.get("time_range")
    if isinstance(tr, dict):
        tr_fp: tuple = (tr.get("start"), tr.get("end"))
    else:
        tr_fp = ()
    access = source.get("access_level") or ""
    cron_enabled = bool(((source.get("provisioning") or {}).get("cron_sync") or {}).get("enabled", True))
    return (access, tr_fp, cron_enabled)


# Per-source files cache: source_key -> (metadata_loc, snapshot_id, iceberg_loc, local_iceberg_files)
_snapshot_files_cache: dict[str, tuple] = {}

# Per-source rebuild signal: source_key -> Event set when an in-progress
# slow-path rebuild finishes. Lets cold parallel waiters wake and use
# fast-path-without-lock instead of stepping through the lock serially.
_rebuild_signals: dict[str, threading.Event] = {}
_rebuild_signals_lock = threading.Lock()


def is_stale_view_error(exc: BaseException) -> bool:
    """Return True when ``exc`` looks like the iceberg view referencing a
    buffer parquet the commit cycle has already swept.

    Mirror of :func:`backend.repositories._base._is_stale_view_error` —
    promoted here so non-repository call paths (background crons /
    discovery / rollup writers) can share the same detection without
    importing through the repositories layer (would invert the
    architecture). The repositories alias still resolves the same way;
    new sites should import :func:`is_stale_view_error` directly from
    :mod:`backend.core.iceberg`.
    """
    msg = str(exc)
    return "No files found" in msg or "Catalog Error: Table with name" in msg or "No such file or directory" in msg


def execute_with_stale_view_retry(con, source: dict, fn, *args, table_name="logs", **kwargs):
    """Run ``fn(con, *args, **kwargs)`` with one stale-view self-heal retry.

    On a :func:`is_stale_view_error`-shaped failure, bust the cached view
    SQL via :func:`clear_source_caches` (keep_snapshot_cache=True, same
    pattern QueryRunner.execute uses), force-rebind the view via
    :func:`update_iceberg_view`, then re-invoke ``fn`` once.

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
    try:
        return fn(con, *args, **kwargs)
    except Exception as e:
        if not is_stale_view_error(e):
            raise
        _core_mod.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
        _core_mod.update_iceberg_view(con, source, force=True, target_table=table_name)
        return fn(con, *args, **kwargs)


def clear_source_caches(source_key: str, *, keep_snapshot_cache: bool = False) -> None:
    """Remove in-memory cache entries for a service."""
    # Pop the primary and any sub-table view cache keys (e.g. client_vitals, client_errors)
    for k in list(_view_cache.keys()):
        if k == source_key or k.startswith(f"{source_key}::"):
            _view_cache.pop(k, None)

    if not keep_snapshot_cache:
        for k in list(_snapshot_files_cache.keys()):
            if k == source_key or k.startswith(f"{source_key}::"):
                _snapshot_files_cache.pop(k, None)


def _get_cache_file(source: dict, name: str) -> str:
    from backend.core.duckdb import _cache_dir

    d = _cache_dir(source)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def _load_persistent_cache(source: dict):
    source_key = source.get("name", "default")
    if source_key in _snapshot_files_cache:
        return

    cache_file = _get_cache_file(source, "snapshot_files_cache.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                data = json.load(f)
                # metadata_loc, snapshot_id, iceberg_loc, local_iceberg_files
                _snapshot_files_cache[source_key] = (
                    data.get("metadata_loc"),
                    data.get("snapshot_id"),
                    data.get("iceberg_loc"),
                    data.get("local_iceberg_files", []),
                )
        except Exception:
            pass


def _save_persistent_cache(source: dict):
    source_key = source.get("name", "default")
    if source_key not in _snapshot_files_cache:
        return

    cache_file = _get_cache_file(source, "snapshot_files_cache.json")
    data = {
        "metadata_loc": _snapshot_files_cache[source_key][0],
        "snapshot_id": _snapshot_files_cache[source_key][1],
        "iceberg_loc": _snapshot_files_cache[source_key][2],
        "local_iceberg_files": _snapshot_files_cache[source_key][3],
    }
    try:
        with open(cache_file, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _update_snapshot_cache_from_delta(source: dict, table) -> bool:
    """Apply a just-committed snapshot's added-files delta to _snapshot_files_cache.

    Iceberg manifests are immutable: a commit only ADDS a new manifest listing
    the files this snapshot added. By reading only that one new manifest
    (typically ~1 .avro file) instead of re-scanning all manifests via
    ``tbl.scan().plan_files()`` (which re-reads ~1080 .avro files in the
    steady state of this service), we get the same "list of files in the
    table" answer at a fraction of the cloud I/O.

    Only applies the delta when the cached snapshot is the direct parent of
    the new one — if we missed an intermediate commit (concurrent writers,
    process restart between commits, etc.) we'd silently lose files, so fall
    back to the full scan in that case.

    Returns True if the cache was updated (caller can skip its own
    plan_files); False if the caller should let the normal full-scan path
    rebuild the cache.
    """
    source_key = source.get("name", "default")
    snap = table.current_snapshot()
    if snap is None:
        return False

    new_metadata_loc = table.metadata_location
    new_snapshot_id = snap.snapshot_id
    iceberg_loc = table.location()

    prev = _snapshot_files_cache.get(source_key)
    if not prev:
        return False

    prev_metadata_loc, prev_snapshot_id, _prev_iceberg_loc, prev_files = prev

    # No-op commit: same snapshot (shouldn't really happen after a successful
    # append, but guard for safety) — just refresh metadata_loc.
    if prev_snapshot_id == new_snapshot_id:
        _snapshot_files_cache[source_key] = (new_metadata_loc, new_snapshot_id, iceberg_loc, list(prev_files))
        try:
            _save_persistent_cache(source)
        except Exception:
            pass
        return True

    # Linear-history check: the cached snapshot must be the direct parent of
    # the new one. If not, we may have skipped intermediate snapshots whose
    # added files we never recorded — refuse the shortcut.
    parent_id = getattr(snap, "parent_snapshot_id", None)
    if parent_id is not None and parent_id != prev_snapshot_id:
        logger.info(
            "%s %s: skipping delta cache update — cached snapshot %s is not parent of new snapshot %s (parent=%s)",
            _core_mod._ICE,
            source_key,
            prev_snapshot_id,
            new_snapshot_id,
            parent_id,
        )
        return False

    io = table.io
    try:
        new_manifests = [
            m
            for m in snap.manifests(io)
            if getattr(m, "added_snapshot_id", None) == new_snapshot_id and m.has_added_files
        ]
    except Exception as e:
        logger.warning("[iceberg] %s: delta cache update failed reading manifests: %s", source_key, e)
        return False

    if not new_manifests:
        # Snapshot exists but added no data files (e.g., schema-only change).
        # Reuse the previous file list, just refresh metadata_loc/snapshot_id.
        _snapshot_files_cache[source_key] = (new_metadata_loc, new_snapshot_id, iceberg_loc, list(prev_files))
        try:
            _save_persistent_cache(source)
        except Exception:
            pass
        return True

    from pyiceberg.manifest import ManifestEntryStatus

    from backend.core.duckdb import _cache_dir

    cache_dir = os.path.join(_cache_dir(source), "data")
    is_analyst = source.get("access_level") == "read_only"

    added: list[str] = []
    # Pre-seed per-manifest aggregates while we have the entries open — saves
    # `_get_cached_or_scan_metadata` (which fires after every commit via
    # `_write_table_summary_async`) from re-GETting the same .avro seconds
    # later. A fresh-commit manifest contains only ADDED entries, so the
    # ADDED-only sweep here produces the same aggregate scan_manifest would.
    per_manifest_agg: dict[str, tuple[dict, datetime | None, datetime | None, int, int]] = {}
    try:
        for manifest in new_manifests:
            manifest_key = getattr(manifest, "manifest_path", None) or repr(manifest)
            m_calendar: dict[str, dict] = {}
            m_min: datetime | None = None
            m_max: datetime | None = None
            m_files = 0
            m_size = 0
            for entry in manifest.fetch_manifest_entry(io):
                if entry.status != ManifestEntryStatus.ADDED:
                    continue
                uri = entry.data_file.file_path
                local = _core_mod._cloud_uri_to_local_path(uri, cache_dir)
                if local is None:
                    continue
                # Match the same local-vs-URI selection rule used by
                # _update_iceberg_view_locked: prefer local file when present,
                # else fall back to the cloud URI for admins (analysts never
                # see URIs to avoid surprise S3 GETs).
                if os.path.exists(local):
                    added.append(local)
                elif not is_analyst:
                    added.append(uri)

                f = entry.data_file
                m_files += 1
                m_size += f.file_size_in_bytes
                try:
                    hour_val = f.partition[0] if f.partition else None
                    if hour_val is not None:
                        dt = datetime.fromtimestamp(hour_val * 3600, tz=UTC)
                        if m_min is None or dt < m_min:
                            m_min = dt
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
            per_manifest_agg[manifest_key] = (m_calendar, m_min, m_max, m_files, m_size)
    except Exception as e:
        logger.warning("[iceberg] %s: delta cache update failed reading entries: %s", source_key, e)
        return False

    with _core_mod._manifest_metadata_cache_lock:
        for manifest_key, agg in per_manifest_agg.items():
            _core_mod._manifest_metadata_cache.setdefault(manifest_key, agg)

    updated_files = list(prev_files) + added
    _snapshot_files_cache[source_key] = (new_metadata_loc, new_snapshot_id, iceberg_loc, updated_files)
    try:
        _save_persistent_cache(source)
    except Exception:
        pass

    logger.info(
        "%s %s: snapshot cache +%d via delta (was %d, now %d) snapshot=%s parent=%s",
        _core_mod._ICE,
        source_key,
        len(added),
        len(prev_files),
        len(updated_files),
        new_snapshot_id,
        prev_snapshot_id,
    )
    return True


def _reconcile_snapshot_cache_after_sync(source: dict, table_name: str = "logs") -> None:
    """Convert any s3:// URI entries in the cache to local paths for files
    that have since been downloaded. Called after sync_data finishes a batch
    so subsequent view builds see the local paths (avoids the URI-vs-glob
    inconsistency that would silently leave us on the iceberg_scan fallback).
    """
    source_key = source.get("name", "default")
    cache_key = f"{source_key}::{table_name}" if table_name != "logs" else source_key
    cached = _snapshot_files_cache.get(cache_key)
    if not cached:
        return

    from backend.core.duckdb import _cache_dir

    sub_dir = f"data_{table_name}" if table_name != "logs" else "data"
    cache_dir = os.path.join(_cache_dir(source), sub_dir)
    metadata_loc, snapshot_id, iceberg_loc, files = cached

    changed = False
    new_entries: list[str] = []
    for p in files:
        if p.startswith("s3://"):
            local = _core_mod._cloud_uri_to_local_path(p, cache_dir)
            if local is None:
                continue
            if os.path.exists(local):
                new_entries.append(local)
                changed = True
            else:
                new_entries.append(p)
        else:
            new_entries.append(p)

    if changed:
        _snapshot_files_cache[cache_key] = (metadata_loc, snapshot_id, iceberg_loc, new_entries)
        try:
            _save_persistent_cache(source)
        except Exception:
            pass


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
                "sql": _compact_sql_for_debug(f"-- DuckDB Iceberg View Resolution [{mode}] --\n{stats['sql']}"),
                "time_ms": stats["time_ms"],
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
    from backend.core.iceberg._ducklake import ducklake_current_snapshot_id

    snap = ducklake_current_snapshot_id(con)
    return None if snap is None else f"ducklake:{snap}"


def _empty_schema_select(dynamic_arrow_schema) -> str:
    """Zero-row SELECT carrying every schema column (typed NULLs) so a
    service with no lake table and no buffer still binds a queryable view."""
    cols = ", ".join(
        'NULL::{} AS "{}"'.format(_core_mod._arrow_to_duckdb(f.type), f.name.replace('"', '""'))
        for f in dynamic_arrow_schema
    )
    return f"SELECT {cols} WHERE false"


def _existing_union_cols(con, committed_parts: list[str], buf_probe_path: str | None) -> set[str]:
    """Column names present in the raw (lake ∪ buffer) union output.

    Probes each committed part with ``LIMIT 0`` (catalog-only, no data
    read) plus a SINGLE buffer parquet (all buffer files are aligned to
    the same schema by ``write_to_buffer``, so one probe suffices — the
    same trick the pre-v3 fast path used to avoid schema-binding every
    buffer file).
    """
    cols: set[str] = set()
    for part in committed_parts:
        try:
            desc = con.execute(f"SELECT * FROM ({part}) LIMIT 0").description or []
            cols.update(d[0] for d in desc)
        except Exception as e:
            logger.info("[iceberg] view column probe failed for committed part: %s", e)
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
            logger.info("[iceberg] view column probe failed for buffer parquet %s: %s", buf_probe_path, e)
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
    - the ``time_range`` WHERE clamp for ``read_only`` (analyst) sources
      and cron-disabled manual imports — a DATA-SCOPING boundary, not an
      optimization: an analyst share must never expose rows outside the
      granted window.
    """
    sql = union_sql

    # 1. Computed partition columns. Strip any real column of the same
    #    name first (legacy hive-partition reads may carry one) so the
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
        from backend.utils import field_codes as fc

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

    # 3. Analyst / manual-import time clamp.
    tr = source.get("time_range")
    is_analyst = source.get("access_level") == "read_only"
    if tr and (is_analyst or not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)):
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

    configure_duckdb_s3(con)

    try:
        buf_files = _core_mod.buffer_files(source, table_name=target_table)
    except TypeError:
        buf_files = _core_mod.buffer_files(source)
    buf_set = frozenset(buf_files)

    # DuckLake-aware staleness token (was: the pyiceberg metadata_location
    # from iceberg_catalog.db, which never matched the slow path's cached
    # literal post-DuckLake — every read paid the RLock + full rebuild,
    # the 12s+ page-load class regression this fast path exists to avoid).
    metadata_loc = _ducklake_view_token(con)
    if metadata_loc is None:
        return False

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = _core_mod.get_arrow_schema(log_fields_config, table_name=target_table)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    cached = _view_cache.get(cache_key)

    # We rely on the background ingest/commit cron jobs to naturally migrate views from
    # S3 to local reads when files are downloaded. Removing the synchronous glob/check
    # here prevents parallel reader threads from getting serialized and waiting on the RLock
    # (which multiplied page load times up to 12s+ on dashboard reloads). It also avoids
    # running expensive recursive filesystem glob scans on every read-side query checkout.

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
        assert cached is not None
        try:
            # Reconstruct the view SQL with the new local buffer files
            # instantly — no lock, no catalog walk. cached[7] holds the RAW
            # committed parts (the lake SELECT) written by the slow path;
            # append a fresh raw buffer part and run the SAME finalize
            # helper the slow path uses.
            committed_sql_parts = cached[7]
            new_parts = list(committed_sql_parts) if committed_sql_parts else []

            # Recheck buffer files existence
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
                existing_cols = _existing_union_cols(
                    con,
                    list(committed_sql_parts) if committed_sql_parts else [],
                    active_buf_files[0] if active_buf_files else None,
                )

            final_sql = _finalize_view_sql(union_sql, source, target_table, dynamic_schema_field_names, existing_cols)

            # Format the CREATE OR REPLACE VIEW statement
            create_stmt = f"CREATE OR REPLACE VIEW {_view_target_name(source, target_table)} AS {final_sql}"

            # Update cache so the view_sql below matches our reconstructed version!
            _view_cache[cache_key] = (
                metadata_loc,
                buf_set,
                schema_fp,
                create_stmt,
                round((time.time() - t_start) * 1000, 2),
                True,  # was_fast_path = True
                variant_fp,
                committed_sql_parts,
            )
            cached = _view_cache[cache_key]
        except Exception as e:
            logger.warning("[iceberg] Incremental fast-path reconstruction failed: %s", e)
            return False

    assert cached is not None
    view_sql = cached[3]
    committed_sql_parts = cached[7] if len(cached) > 7 else ()
    if view_sql:
        # Per-connection cache: if THIS connection has already bound the
        # same (metadata_loc, buf_set, schema_fp) on the fast path, skip
        # the CREATE OR REPLACE TEMP VIEW execute entirely. The view
        # SQL is deterministic from the fingerprint tuple, so re-running
        # it produces the same DuckDB state — at ~70 ms of catalog work
        # per call that adds up across every read-side request.
        from backend.core.duckdb_pool import _get_conn_state, _set_conn_state

        fp_key = f"fast_path_view_fp_{target_table}"
        bound_fp = _get_conn_state(con, fp_key)
        target_fp = (metadata_loc, buf_set, schema_fp, variant_fp, target_table)
        if bound_fp == target_fp:
            t_end = time.time()
            _view_cache[cache_key] = (
                metadata_loc,
                buf_set,
                schema_fp,
                view_sql,
                round((t_end - t_start) * 1000, 2),
                True,
                variant_fp,
                committed_sql_parts,
            )
            return True

        # Always bind as a TEMP view on the fast path — the persistent view
        # is maintained by the locked rebuild path.  Concurrent fast-path
        # callers (pool checkouts) would otherwise race on the shared catalog
        # and trigger "write-write conflict on alter".
        exec_sql = view_sql
        if view_sql.startswith("CREATE OR REPLACE VIEW "):
            exec_sql = view_sql.replace("CREATE OR REPLACE VIEW ", "CREATE OR REPLACE TEMP VIEW ", 1)
        try:
            con.execute(exec_sql)
        except Exception as e:
            logger.warning("[iceberg] fast-path view re-bind failed for %s: %s", source_key, e)
            return False
        _set_conn_state(con, **{fp_key: target_fp})

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
            _core_mod._update_iceberg_view_locked(con, source, target_table=target_table, force=force)
        except TypeError:
            _core_mod._update_iceberg_view_locked(con, source)
    finally:
        ev.set()
        with _rebuild_signals_lock:
            if _rebuild_signals.get(cache_key) is ev:
                del _rebuild_signals[cache_key]


def update_iceberg_view(
    con, source: dict, lock_timeout: float = 5.0, force: bool = False, target_table: str = "logs"
) -> None:
    """Refresh the per-service DuckDB view over the Iceberg table + buffer.

    ``lock_timeout`` (default 5s) caps how long we wait on the per-service
    RLock that ingest also acquires for buffer commits. Prior default was
    1s, which was often shorter than a buffer-commit cycle — when callers
    landed in that window, this function fell back to executing the
    cached view SQL, which after a recent commit could reference a
    just-deleted buffer parquet and surface as ``No files found that
    match the pattern …/buffer/batch_*.parquet`` on the next read. Five
    seconds is long enough to outlast a typical commit without making
    sync-status polls feel sticky.

    ``force=True`` skips the lock-free fast path and goes straight to a
    full rebuild under the lock. The QueryRunner self-heal path uses
    this: when a query already failed with a stale-view IOException,
    the fast path can't help — its buf_set check might match cached
    state that's still inconsistent with what the DuckDB query planner
    just saw on disk, OR (the symptom-from-prod) the cached view SQL
    has hardcoded file paths and re-executing it just re-binds the same
    bad SQL. Force-rebuild reads disk fresh under the lock and
    regenerates the SQL.
    """
    source_key = source.get("name", "default")
    if source_key.endswith("::rum") and target_table == "logs":
        update_iceberg_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_vitals")
        update_iceberg_view(con, source, lock_timeout=lock_timeout, force=force, target_table="client_errors")
        return

    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key

    # Lock-free fast path first. Parallel dashboard reads (6+ endpoints
    # per page load) only need the lock when a real rebuild is required.
    # Skipped on ``force=True`` (see self-heal path in QueryRunner).
    if not force and _try_fast_path_view(con, source, target_table=target_table):
        return

    lock = _get_service_lock(source_key)

    # If the lock is held, another caller is rebuilding. Wait on their
    # completion signal, then retry the fast path WITHOUT the lock — N
    # cold-parallel waiters can then run fast-path concurrently instead
    # of stepping through the lock serially.
    if not lock.acquire(blocking=False):
        with _rebuild_signals_lock:
            ev = _rebuild_signals.get(cache_key)
        if ev is not None and ev.wait(timeout=lock_timeout):
            if _try_fast_path_view(con, source, target_table=target_table):
                return
        # Either we raced ahead of _rebuild_locked setting the signal,
        # or the rebuild produced no fast-path-cacheable result. Fall
        # through to the original blocking-acquire path.
        if not lock.acquire(timeout=lock_timeout):
            # Ingest is still holding the lock. Fallback order:
            #   1. Cached view SQL → re-execute on this connection.
            #   2. Persistent view on this DB → no-op (slightly stale).
            #   3. Neither — extend the lock wait so the caller has a
            #      view to query (production-observed: restart-during-
            #      sync left RO sessions with "table not found").
            cached = _view_cache.get(cache_key)
            if cached and cached[3] and len(cached) > 6 and cached[6] == _source_variant_fp(source):
                try:
                    con.execute(cached[3])
                except Exception:
                    pass
                return
            if _persistent_view_exists(con, source, target_table=target_table):
                return
            logger.info(
                "[iceberg] %s: cache empty and no persistent view for %s; extending lock "
                "wait to avoid 'table not found' on caller",
                source_key,
                target_table,
            )
            if not lock.acquire(timeout=60.0):
                logger.warning(
                    "[iceberg] %s: extended 60s lock wait timed out; view %s rebuild deferred",
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
    """Return True if the per-service Iceberg view already exists on this
    connection's database. Used by ``update_iceberg_view`` to skip the
    extended lock wait when the caller can already query the view (even
    if it's slightly stale)."""
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


def _update_iceberg_view_locked(con, source: dict, target_table: str = "logs", force: bool = False) -> None:
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name

    t_start = time.time()
    view_name = _view_target_name(source, target_table)

    # Determine the connection's actual read-only mode up front — this
    # function also runs on read-only, POOLED request connections (the
    # slow-path rebuild triggered when the fast-path view token can't be
    # verified), not only on cron's dedicated read-write connections.
    is_read_only = False
    try:
        res = con.execute(
            "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp','lake') LIMIT 1"
        ).fetchone()
        is_read_only = bool(res and res[0])
    except Exception as e:
        logger.error("[iceberg] duckdb_databases probe failed: %s", e)

    # Only attach "lake" if it isn't already attached on this connection,
    # and match the connection's real read-only mode when we do. Blindly
    # re-attaching with read_only=False (the old behavior) on a connection
    # where "lake" is already attached READ_ONLY throws "database with
    # name 'lake' already exists" — caught below as a harmless no-op by
    # _ducklake_attach's broad "already exists" match, but the mode-
    # mismatched re-attach attempt itself corrupts the internal ducklake
    # metadata catalog as a side effect, leaving every later
    # ducklake_snapshots('lake') call on this (pooled, long-lived)
    # connection failing with "Catalog __ducklake_metadata_lake does not
    # exist" for the rest of its life. See AGENTS.md trap #33.
    lake_attached = con.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake' LIMIT 1").fetchone()
    if not lake_attached:
        _ducklake_attach(con, source, read_only=is_read_only)

    try:
        buf_files = _core_mod.buffer_files(source, table_name=target_table)
    except TypeError:
        buf_files = _core_mod.buffer_files(source)

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = _core_mod.get_arrow_schema(log_fields_config, table_name=target_table)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    # Committed rows live in the per-service DuckLake table (NOT a bare
    # lake.logs — under a shared catalog that would mix tenants).
    lake_table = ducklake_table_name(source, target_table)
    committed_parts: list[str] = []
    try:
        res = con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
            [lake_table],
        ).fetchone()
        if res:
            committed_parts.append(f'SELECT * FROM lake."{lake_table}"')
    except Exception as e:
        logger.warning("[iceberg] %s: lake table probe failed for %s: %s", source.get("name"), lake_table, e)

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

    if is_read_only:
        create_stmt = f"CREATE OR REPLACE TEMP VIEW {view_name} AS {final_sql}"
    else:
        create_stmt = f"CREATE OR REPLACE VIEW {view_name} AS {final_sql}"

    try:
        con.execute(create_stmt)
    except Exception as e:
        logger.error("[iceberg] Failed to create view %s: %s", view_name, e)

    source_key = source.get("name", "default")
    cache_key = f"{source_key}::{target_table}" if target_table != "logs" else source_key
    _view_cache[cache_key] = (
        _ducklake_view_token(con),
        frozenset(buf_files),
        tuple(sorted(dynamic_schema_field_names)),
        create_stmt,
        round((time.time() - t_start) * 1000, 2),
        False,
        _source_variant_fp(source),
        tuple(committed_parts),
    )


# ---------------------------------------------------------------------------
# Admin / UI metadata
