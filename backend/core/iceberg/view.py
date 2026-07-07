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


def execute_with_stale_view_retry(con, source: dict, fn, *args, **kwargs):
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
        _core_mod.update_iceberg_view(con, source, force=True)
        return fn(con, *args, **kwargs)


def clear_source_caches(source_key: str, *, keep_snapshot_cache: bool = False) -> None:
    """Remove in-memory cache entries for a service.

    ``keep_snapshot_cache=True`` is used by the get_sync_status retry path
    when the cached view SQL points at a since-deleted buffer parquet. We
    want to force the view SQL to be regenerated, but we MUST NOT wipe
    ``_snapshot_files_cache`` — that's the snapshot/path cache that lets
    ``_update_iceberg_view_locked`` skip a catalog reload. Without it, a
    transient catalog-load failure (FOS rate limit, network blip) causes
    ``_update_iceberg_view_locked`` to fall into its empty-view branch and
    downgrade the working view to "WHERE false", which then sticks until
    a writer cron eventually re-fetches the catalog successfully.

    Defaults match the original semantics (full wipe) so teardown still
    clears everything.
    """
    _view_cache.pop(source_key, None)
    if not keep_snapshot_cache:
        _snapshot_files_cache.pop(source_key, None)


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


def _reconcile_snapshot_cache_after_sync(source: dict) -> None:
    """Convert any s3:// URI entries in the cache to local paths for files
    that have since been downloaded. Called after sync_data finishes a batch
    so subsequent view builds see the local paths (avoids the URI-vs-glob
    inconsistency that would silently leave us on the iceberg_scan fallback).
    """
    source_key = source.get("name", "default")
    cached = _snapshot_files_cache.get(source_key)
    if not cached:
        return

    from backend.core.duckdb import _cache_dir

    cache_dir = os.path.join(_cache_dir(source), "data")
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
        _snapshot_files_cache[source_key] = (metadata_loc, snapshot_id, iceberg_loc, new_entries)
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


def _try_fast_path_view(con, source: dict) -> bool:
    """Bind the per-service view from cache without acquiring the lock.

    Returns True if the view was bound; False if a slow-path rebuild is
    needed. Safe to call concurrently — all reads are race-free against
    a concurrent slow-path writer (cached tuple refs are stable; the
    only write here is a benign timestamp update on _view_cache).

    This split exists so 6 parallel dashboard requests for the same
    source don't serialize on the per-service RLock that ingest also
    holds during buffer commits.
    """
    import sqlite3

    from backend.core.duckdb import _cache_dir

    t_start = time.time()
    source_key = source.get("name", "default")
    cache_dir = _cache_dir(source)
    catalog_db_path = os.path.join(cache_dir, "iceberg_catalog.db")

    configure_duckdb_s3(con)

    buf_files = _core_mod.buffer_files(source)
    buf_set = frozenset(buf_files)

    metadata_loc = None
    try:
        if os.path.exists(catalog_db_path):
            with sqlite3.connect(catalog_db_path, timeout=5.0) as cat_con:
                row = cat_con.execute(
                    "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'default' AND table_name = 'logs'"
                ).fetchone()
                if row:
                    metadata_loc = row[0]
    except Exception:
        pass

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    dynamic_arrow_schema = _core_mod.get_arrow_schema(log_fields_config)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    cached = _view_cache.get(source_key)

    # See matching block in _update_iceberg_view_locked: if cached SQL is
    # S3-based but local parquets exist, refuse fast path so caller takes
    # slow path under the lock and rebuilds to local reads.
    if cached and cached[3] and "iceberg_scan(" in cached[3]:
        try:
            import glob

            data_dir = os.path.join(cache_dir, "data")
            if glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True):
                return False
        except Exception:
            pass

    schema_fp = tuple(sorted(dynamic_schema_field_names))
    variant_fp = _source_variant_fp(source)
    if not (
        cached
        and cached[0] == metadata_loc
        and cached[1] == buf_set
        and cached[2] == schema_fp
        and (len(cached) > 6 and cached[6] == variant_fp)
    ):
        return False

    view_sql = cached[3]
    if view_sql:
        # Per-connection cache: if THIS connection has already bound the
        # same (metadata_loc, buf_set, schema_fp) on the fast path, skip
        # the CREATE OR REPLACE TEMP VIEW execute entirely. The view
        # SQL is deterministic from the fingerprint tuple, so re-running
        # it produces the same DuckDB state — at ~70 ms of catalog work
        # per call that adds up across every read-side request.
        from backend.core.duckdb_pool import _get_conn_state, _set_conn_state

        bound_fp = _get_conn_state(con, "fast_path_view_fp")
        target_fp = (metadata_loc, buf_set, schema_fp, variant_fp)
        if bound_fp == target_fp:
            t_end = time.time()
            _view_cache[source_key] = (
                metadata_loc,
                buf_set,
                schema_fp,
                view_sql,
                round((t_end - t_start) * 1000, 2),
                True,
                variant_fp,
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
        _set_conn_state(con, fast_path_view_fp=target_fp)

    t_end = time.time()
    _view_cache[source_key] = (
        metadata_loc,
        buf_set,
        tuple(sorted(dynamic_schema_field_names)),
        view_sql,
        round((t_end - t_start) * 1000, 2),
        True,
        variant_fp,
    )
    return True


def _rebuild_locked(con, source: dict, source_key: str) -> None:
    """Run the slow path under the lock and signal completion."""
    ev = threading.Event()
    with _rebuild_signals_lock:
        _rebuild_signals[source_key] = ev
    try:
        _core_mod._update_iceberg_view_locked(con, source)
    finally:
        ev.set()
        with _rebuild_signals_lock:
            if _rebuild_signals.get(source_key) is ev:
                del _rebuild_signals[source_key]


def update_iceberg_view(con, source: dict, lock_timeout: float = 5.0, force: bool = False) -> None:
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

    # Lock-free fast path first. Parallel dashboard reads (6+ endpoints
    # per page load) only need the lock when a real rebuild is required.
    # Skipped on ``force=True`` (see self-heal path in QueryRunner).
    if not force and _try_fast_path_view(con, source):
        return

    lock = _get_service_lock(source_key)

    # If the lock is held, another caller is rebuilding. Wait on their
    # completion signal, then retry the fast path WITHOUT the lock — N
    # cold-parallel waiters can then run fast-path concurrently instead
    # of stepping through the lock serially.
    if not lock.acquire(blocking=False):
        with _rebuild_signals_lock:
            ev = _rebuild_signals.get(source_key)
        if ev is not None and ev.wait(timeout=lock_timeout):
            if _try_fast_path_view(con, source):
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
            cached = _view_cache.get(source_key)
            if cached and cached[3] and len(cached) > 6 and cached[6] == _source_variant_fp(source):
                try:
                    con.execute(cached[3])
                except Exception:
                    pass
                return
            if _persistent_view_exists(con, source):
                return
            logger.info(
                "[iceberg] %s: cache empty and no persistent view; extending lock "
                "wait to avoid 'table not found' on caller",
                source_key,
            )
            if not lock.acquire(timeout=60.0):
                logger.warning(
                    "[iceberg] %s: extended 60s lock wait timed out; view rebuild deferred",
                    source_key,
                )
                return
            try:
                _rebuild_locked(con, source, source_key)
            finally:
                lock.release()
            return
    try:
        _rebuild_locked(con, source, source_key)
    finally:
        lock.release()


def _persistent_view_exists(con, source: dict) -> bool:
    """Return True if the per-service Iceberg view already exists on this
    connection's database. Used by ``update_iceberg_view`` to skip the
    extended lock wait when the caller can already query the view (even
    if it's slightly stale)."""
    try:
        from backend.core.duckdb import _safe_table_name

        table_name = _safe_table_name(source["name"])
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _update_iceberg_view_locked(con, source: dict) -> None:
    import sqlite3

    from backend.core.duckdb import _cache_dir, _safe_table_name

    # Re-check the fast path under the lock — state may have become
    # cacheable while we waited (a concurrent slow-path writer just
    # finished and primed _view_cache).
    if _try_fast_path_view(con, source):
        return

    t_start = time.time()
    table_name = _safe_table_name(source["name"])
    source_key = source.get("name", "default")
    cache_dir = _cache_dir(source)
    catalog_db_path = os.path.join(cache_dir, "iceberg_catalog.db")

    configure_duckdb_s3(con)

    buf_files = _core_mod.buffer_files(source)
    buf_set = frozenset(buf_files)

    metadata_loc = None
    try:
        if os.path.exists(catalog_db_path):
            with sqlite3.connect(catalog_db_path, timeout=5.0) as cat_con:
                row = cat_con.execute(
                    "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'default' AND table_name = 'logs'"
                ).fetchone()
                if row:
                    metadata_loc = row[0]
    except Exception:
        pass

    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None

    dynamic_arrow_schema = _core_mod.get_arrow_schema(log_fields_config)
    dynamic_schema_field_names = {f.name for f in dynamic_arrow_schema}

    logger.info("▶️  %s %s: View refresh started...", _core_mod._ICE_PLAIN, source_key)

    # Try to load from persistent cache if memory cache is empty
    _load_persistent_cache(source)

    iceberg_loc = None
    local_iceberg_files = []

    # We can skip reading from S3 entirely if ONLY the buffer changed.
    cached_files = _snapshot_files_cache.get(source_key)
    if cached_files and cached_files[0] == metadata_loc:
        snapshot_id = cached_files[1]
        iceberg_loc = cached_files[2]
        local_iceberg_files = cached_files[3]
    elif metadata_loc is None:
        # Never-committed service: the local SQLite catalog has no metadata_location
        # row for this table, so there is no Iceberg snapshot to fetch. Skipping
        # the S3 round-trip here saves 6-14s on every cold dashboard query for
        # services that haven't ingested anything (or whose init_iceberg_table
        # call silently failed to write metadata.json to FOS — observed when
        # fos_endpoint is unreachable, e.g. local dev / load-test services).
        # The view will be built from buffer files only (if any) below, or
        # downgraded to an empty WHERE-false view by the existing fall-through.
        snapshot_id = None
        tbl = None
        snap = None
    else:
        # The table committed (new metadata_loc) or we had a full cache miss.
        try:
            catalog = _core_mod._get_catalog(source)
            tbl = _core_mod._load_table_cached(source, _core_mod._table_identifier(source), catalog)
            snap = tbl.current_snapshot()
            snapshot_id = snap.snapshot_id if snap else None
        except Exception:
            snapshot_id = None
            tbl = None
            snap = None

        if tbl is not None and snap is not None:
            try:
                iceberg_loc = tbl.location()
                data_dir = os.path.join(cache_dir, "data")

                scan = tbl.scan()
                tr = source.get("time_range")
                if tr:
                    import dateutil.parser

                    if tr.get("start"):
                        from backend.utils.iceberg_expr import gte

                        st_dt = dateutil.parser.isoparse(tr["start"])
                        if st_dt.tzinfo is None:
                            st_dt = st_dt.replace(tzinfo=UTC)
                        scan = scan.filter(gte("timestamp", st_dt.isoformat()))

                    # For Analysts (read_only), we always honor end_time to bound their manual imports.
                    # For Admins, we usually don't filter by end_time to allow new logs to stream in,
                    # unless they have explicitly disabled cron sync.
                    is_analyst = source.get("access_level") == "read_only"
                    if tr.get("end") and (
                        is_analyst or not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)
                    ):
                        from backend.utils.iceberg_expr import lte

                        et_dt = dateutil.parser.isoparse(tr["end"])
                        if et_dt.tzinfo is None:
                            et_dt = et_dt.replace(tzinfo=UTC)
                        scan = scan.filter(lte("timestamp", et_dt.isoformat()))

                for f in scan.plan_files():
                    uri = f.file.file_path
                    if uri.startswith("file://"):
                        # Local-only warehouse: the URI IS the local path.
                        # Skip the FOS-style /data/ rewrite and just use it.
                        local_path = uri[len("file://") :]
                        if os.path.exists(local_path):
                            local_iceberg_files.append(local_path)
                        continue
                    local_path = _core_mod._cloud_uri_to_local_path(uri, data_dir)
                    if local_path is None:
                        continue
                    if os.path.exists(local_path):
                        local_iceberg_files.append(local_path)
                    elif source.get("access_level") != "read_only":
                        # Admins fall back to S3 so they can query immediately.
                        # Analysts only query what they have explicitly synced to avoid massive S3 GET costs.
                        local_iceberg_files.append(uri)

                # Cache by metadata_loc instead of snapshot_id
                _snapshot_files_cache[source_key] = (metadata_loc, snapshot_id, iceberg_loc, local_iceberg_files)
                _save_persistent_cache(source)
            except Exception as e:
                logger.warning("[iceberg] plan_files() failed for %s: %s", source_key, e)

    if not iceberg_loc and not buf_files and not local_iceberg_files:
        # All three "data source" channels are empty. There are two reasons
        # this happens:
        #   (a) genuinely fresh service — no data anywhere yet. Empty view
        #       is correct.
        #   (b) transient catalog-load failure (FOS rate limit / network
        #       blip / lock contention). We previously HAD a working
        #       snapshot, but the in-memory cache was wiped and the
        #       re-fetch failed this attempt.
        #
        # In case (b) we must NOT downgrade — replacing a working view
        # with "WHERE false" makes the dashboard show 0 logs and persists
        # in _view_cache until a writer cron eventually rebuilds. Two
        # signals tell us this is case (b):
        #
        # 1. _view_cache already has a non-empty entry. Cheapest check;
        #    catches the steady-state recurrence.
        # 2. The service's ingest sqlite metadata shows files with rows.
        #    Catches the post-process-restart case where _view_cache is
        #    empty even though we have real data on disk / in the table.
        #    Without this, a transient FOS failure on the FIRST poll after
        #    a restart poisons the persistent view to "WHERE false" and
        #    no future poll can recover (the next "prior_was_empty" check
        #    lets the same downgrade happen again).
        prior = _view_cache.get(source_key)
        prior_sql = prior[3] if prior else None
        prior_was_empty = (not prior_sql) or ("WHERE false" in prior_sql)
        if prior_sql and not prior_was_empty:
            logger.info(
                "[iceberg] %s: skipping empty-view downgrade (catalog re-fetch "
                "returned no data but cached view is non-empty — likely transient)",
                source_key,
            )
            return

        # Second signal: ingest metadata. We have rows recorded as ingested
        # → refuse to overwrite with WHERE false. The data exists; this
        # poll is just blind.
        try:
            from backend.core import metadata as _meta

            _summary = _meta.get_ingested_files_status_summary(source_key)
            ingested_rows = _summary["total_rows"]
            ingested_files = _summary["file_count"]
        except Exception:
            ingested_rows = 0
            ingested_files = 0

        # The ingest rollup is retention-trimmed (~1 day, see
        # ingested_files_days), so a service that hasn't ingested recently can
        # read 0 here while the parquet lake still holds data. Cross-check the
        # last-known-good persisted row count so a transient blind poll on such
        # a service doesn't poison the view to "WHERE false". That value is the
        # real parquet count maintained by _duckdb_status.get_sync_status.
        last_good_rows = 0
        if ingested_rows <= 0:
            try:
                from backend import config as svcconfig

                last_good_rows = (svcconfig.get_status(source_key) or {}).get("local_rows") or 0
            except Exception:
                last_good_rows = 0

        if ingested_rows > 0 or last_good_rows > 0:
            logger.info(
                "[iceberg] %s: skipping empty-view downgrade — have data "
                "(ingest rollup %d rows / %d files, last-known-good %d rows); "
                "catalog blind this poll, not a fresh service",
                source_key,
                ingested_rows,
                ingested_files,
                last_good_rows,
            )
            return

        empty_sql: str | None = None
        try:
            cols = ", ".join(
                'NULL::{} AS "{}"'.format(_core_mod._arrow_to_duckdb(f.type), f.name.replace('"', '""'))
                for f in dynamic_arrow_schema
            )
            empty_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT {cols} WHERE false"
            con.execute(empty_sql)
        except Exception:
            empty_sql = None
        t_end = time.time()
        _view_cache[source_key] = (
            metadata_loc,
            buf_set,
            tuple(sorted(dynamic_schema_field_names)),
            empty_sql,
            round((t_end - t_start) * 1000, 2),
            False,
            _source_variant_fp(source),
        )
        return

    parts: list[str] = []

    local_paths = [p for p in local_iceberg_files if not p.startswith("s3://")]
    s3_paths = [p for p in local_iceberg_files if p.startswith("s3://")]

    # Belt-and-suspenders against costly S3 fallback: even if local_paths is
    # empty (because plan_files happened to run before sync_data finished),
    # check the local data_dir directly. If it has parquet files on disk, we
    # MUST use them — otherwise dashboard queries route through iceberg_scan
    # over S3 and rack up Class B reads on every poll.
    #
    # Local-only (file://) warehouse: Iceberg writes data files under
    # warehouse/<namespace>/<table>/data/ rather than cache/{bucket}/data/.
    # Point data_dir at the actual on-disk location so the glob below and the
    # eventual read_parquet view SQL hit real files.
    if _core_mod._is_local_only_source(source) and iceberg_loc and iceberg_loc.startswith("file://"):
        data_dir = os.path.join(iceberg_loc[len("file://") :], "data")
    else:
        data_dir = os.path.join(cache_dir, "data")
    if not local_paths:
        try:
            import glob as _glob

            disk_parquets = _glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
            if disk_parquets:
                # Synthesize a sentinel so the local-read branch fires below
                local_paths = disk_parquets[:1]
                logger.info(
                    "[iceberg] %s: plan_files returned 0 local paths but data/ has %d parquets — "
                    "using local glob anyway to avoid cloud reads",
                    source_key,
                    len(disk_parquets),
                )
        except Exception:
            pass

    # Defensive: some parquet files may already include the computed
    # timestamp_hour / dt columns (e.g., after a PyIceberg-routed compaction
    # that preserves partition columns in the output file). If we then add
    # `, ... AS timestamp_hour` in the outer SELECT, the resulting view
    # branch has TWO columns named timestamp_hour and UNION ALL BY NAME
    # fails with a Binder Error. EXCLUDE them defensively before re-adding.
    def _strip_computed(read_parquet_expr: str) -> str:
        try:
            probe = con.execute(f"SELECT * FROM {read_parquet_expr} LIMIT 0").description or []
            existing = {d[0] for d in probe}
        except Exception:
            existing = set()
        cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in existing)
        exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
        return (
            f"SELECT *{exclude_clause}, "
            f"CAST(strftime(timestamp, '%Y-%m-%d-%H') AS VARCHAR) as timestamp_hour, "
            f"CAST(strftime(timestamp, '%Y-%m-%d') AS VARCHAR) as dt "
            f"FROM {read_parquet_expr}"
        )

    if local_paths:
        from backend.utils.sql_validator import escape_sql_literal as _esl

        safe_data_dir = _esl(f"{data_dir}/**/*.parquet")
        parts.append(
            _strip_computed(
                f"read_parquet('{safe_data_dir}', union_by_name=true, filename=true, hive_partitioning=false)"
            )
        )

    # Use iceberg_scan when:
    # (a) plan_files() returned S3 URIs and no local files are cached yet, OR
    # (b) plan_files() failed silently but iceberg_loc is known (avoids WHERE false view)
    if (
        iceberg_loc
        and not local_paths
        and (s3_paths or not local_iceberg_files)
        and source.get("access_level") != "read_only"
    ):
        parts.append(_strip_computed(f"iceberg_scan('{escape_sql_literal(iceberg_loc)}', allow_moved_paths=true)"))
        logger.info(
            "%s Falling back to iceberg_scan for %s (s3_paths=%d, local_iceberg_files=%d).",
            _core_mod._ICE,
            source_key,
            len(s3_paths),
            len(local_iceberg_files),
        )
    elif s3_paths:
        # Demoted from INFO to DEBUG (2026-06-01): this fires on every
        # view refresh whenever the local cache lags the iceberg manifest
        # (very common during catch-up / right after a commit). Useful for
        # debugging stale-view issues, not useful as a routine signal —
        # was spamming the prod VM backend log every few seconds with no
        # actionable content.
        logger.debug(
            "%s Skipping %d missing cloud files in view (local files present, CDN sync pending).",
            _core_mod._ICE,
            len(s3_paths),
        )

    # Re-check existence: commit_buffer() may have deleted files during the metadata
    # scan above (which can take seconds), causing an IO Error in CREATE VIEW.
    buf_files = [p for p in buf_files if os.path.isfile(p)]

    if buf_files:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in buf_files)
        parts.append(_strip_computed(f"read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"))

    if not parts:
        cols = ", ".join(
            'NULL::{} AS "{}"'.format(_core_mod._arrow_to_duckdb(f.type), f.name.replace('"', '""'))
            for f in dynamic_arrow_schema
        )
        union_sql = f"SELECT {cols} WHERE false"
    else:
        union_sql = " UNION ALL BY NAME ".join(parts)

        from backend.utils import field_codes as fc

        c_speed_case = fc.duckdb_decode_case("c_speed", fc.CONN_SPEED_ENCODE)
        p_type_case = fc.duckdb_decode_case("p_type", fc.PROXY_TYPE_ENCODE)
        p_desc_case = fc.duckdb_decode_case("p_desc", fc.PROXY_DESC_ENCODE)

        # ttl/age are stored as FLOAT in iceberg (Fastly emits jittery
        # microsecond-precision values, e.g. "3600.027s"), but they're integer
        # seconds semantically. Surface them as INTEGER so Top-N GROUP BY
        # buckets cleanly instead of fragmenting into ~10 sub-second values.
        # Only EXCLUDE columns that exist in the schema — group B is optional.
        exclude_cols = ["c_speed", "p_type", "p_desc"]
        select_extras = [
            f"{c_speed_case} AS c_speed",
            f"{p_type_case} AS p_type",
            f"{p_desc_case} AS p_desc",
        ]
        if "ttl" in dynamic_schema_field_names:
            exclude_cols.append("ttl")
            select_extras.append('CAST(ROUND("ttl") AS INTEGER) AS ttl')
        if "age" in dynamic_schema_field_names:
            exclude_cols.append("age")
            select_extras.append('CAST(ROUND("age") AS INTEGER) AS age')

        # Wrap the union to decode any previously ingested raw enum values
        # and coerce float-stored integer fields to integer.
        union_sql = f"SELECT * EXCLUDE ({', '.join(exclude_cols)}), {', '.join(select_extras)} FROM ({union_sql})"

        # Apply strict time-bounding for analyst manual imports so they don't see
        # the "ragged edges" of the underlying hourly files.
        tr = source.get("time_range")
        is_analyst = source.get("access_level") == "read_only"

        if tr and (is_analyst or not source.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)):
            # Security: validate via isoparse before interpolation. Without
            # this, an attacker-controlled tr["start"] / tr["end"] dict value
            # (these come from saved-view JSON which originates from the
            # frontend) is interpolated raw into DuckDB SQL — a payload like
            #   "2024-01-01'; ATTACH '/tmp/x.db' AS y; --"
            # would execute multi-statement SQL against the connection.
            # isoparse rejects anything that isn't a valid ISO-8601 timestamp;
            # we then interpolate the canonical .isoformat() output, which
            # contains only digits, ":", "-", "T", "+", and "Z".
            import dateutil.parser as _dt

            where_clauses = []
            if tr.get("start"):
                try:
                    start_iso = _dt.isoparse(str(tr["start"])).isoformat()
                except (ValueError, TypeError) as e:
                    raise ValueError(f"invalid time_range start: {e}") from e
                where_clauses.append(f"timestamp >= '{start_iso}'::TIMESTAMPTZ")
            if tr.get("end"):
                try:
                    end_iso = _dt.isoparse(str(tr["end"])).isoformat()
                except (ValueError, TypeError) as e:
                    raise ValueError(f"invalid time_range end: {e}") from e
                where_clauses.append(f"timestamp <= '{end_iso}'::TIMESTAMPTZ")
            if where_clauses:
                union_sql = f"SELECT * FROM ({union_sql}) WHERE {' AND '.join(where_clauses)}"

    view_sql_created: str | None = None
    try:
        # Detect read-only mode so we can switch to CREATE OR REPLACE TEMP VIEW
        # (which works on RO connections — regular CREATE VIEW does not).
        #
        # The previous detection used `PRAGMA database_list` and checked
        # `row[2] == "read-only"` — but row[2] is the FILE PATH, not a
        # readonly flag (database_list returns (seq, name, file)). The check
        # was always False, so RO connections always tried CREATE VIEW and
        # surfaced "ERROR Failed to create view … Cannot execute statement
        # of type CREATE on database … attached in read-only mode!" on every
        # dashboard query. Result: the view was effectively never refreshed
        # from any RO connection, and reads against the stale/empty view
        # showed "No data available" on the dashboard.
        #
        # `duckdb_databases()` is the documented system function for this;
        # it has a `readonly` boolean column.
        is_read_only = False
        try:
            res = con.execute(
                "SELECT readonly FROM duckdb_databases() WHERE database_name NOT IN ('system','temp') LIMIT 1"
            ).fetchone()
            if res is not None and bool(res[0]):
                is_read_only = True
        except Exception:
            pass

        if is_read_only:
            create_stmt = f"CREATE OR REPLACE TEMP VIEW {table_name} AS {union_sql}"
        else:
            create_stmt = f"CREATE OR REPLACE VIEW {table_name} AS {union_sql}"

        con.execute(create_stmt)

        view_sql_created = create_stmt
        if not is_read_only:
            # Clear the schema cache only when the column set actually
            # changed. Previously this was unconditional, but the post-ingest
            # view refresh runs on a writer connection every cron tick where
            # rows_inserted > 0 (i.e. virtually every tick on a busy
            # service), which blew away duckdb._schema_cache and made its
            # 60 s TTL irrelevant. Result: the next heavy refresh_config_status
            # paid the full ~800 ms SUMMARIZE every minute even though the
            # underlying columns are stable across hundreds of ticks.
            # Comparing tuple(sorted(field_names)) against the prior cache
            # entry catches all column add/remove/rename cases (the only
            # thing get_schema cares about); per-row data churn doesn't
            # invalidate column metadata, so it's safe to keep the cache.
            try:
                new_columns = tuple(sorted(dynamic_schema_field_names))
                prior = _view_cache.get(source_key)
                prior_columns = prior[2] if prior else None
                if prior_columns != new_columns:
                    from backend.core.duckdb import _clear_schema_cache

                    _clear_schema_cache(source_key)
            except Exception:
                pass
    except Exception as e:
        logger.error("[iceberg] Failed to create view %s: %s", table_name, e)

    t_end = time.time()
    duration_ms = (t_end - t_start) * 1000
    logger.info("⏹️  %s %s: View refresh complete (%.0f ms).", _core_mod._ICE_PLAIN, source_key, duration_ms)
    _view_cache[source_key] = (
        metadata_loc,
        buf_set,
        tuple(sorted(dynamic_schema_field_names)),
        view_sql_created,
        round((t_end - t_start) * 1000, 2),
        False,
        _source_variant_fp(source),
    )


# ---------------------------------------------------------------------------
# Admin / UI metadata
