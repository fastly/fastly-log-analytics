"""Iceberg buffer + commit + optimize + cloud-maintenance helpers.

Carved out of ``backend/core/iceberg/_core.py`` (v2.0 file-size sweep
part 2/3). Holds the write-path lifecycle:

- Buffer tombstoning + sweep (read-side race protection).
- ``write_to_buffer`` / ``commit_buffer``: per-tick ingest into Iceberg.
- ``optimize_table``: small-file compaction.
- ``run_cloud_maintenance``: snapshot expiry + orphan cleanup.
- Quarantine helpers for corrupt buffer parquet.

All public names are re-exported back into ``backend.core.iceberg._core``
at the bottom of that module so the package proxy + test
``monkeypatch.setattr("backend.core.iceberg.X", …)`` patterns keep
reaching the live binding.

Cross-module helpers (``_core_mod._get_catalog``, ``_core_mod._load_table_cached``,
``update_iceberg_view``, ``clear_source_caches``, …) are resolved via
late-bound ``_core_mod.X`` calls so test patches on those names still
flow through.
"""

from __future__ import annotations

import glob as _glob
import hashlib
import logging
import os
import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa

logger = logging.getLogger("backend.core.iceberg._core")


# Library + util imports the carved code references. Some pyiceberg
# names also appear as inline imports inside specific functions; we
# add the top-level ones here so the bare-name lookup works.
import pyarrow.parquet as pq

from backend.core import metadata as _meta_mod

# Late-bind helpers from the main _core module (it's mid-load when this
# file imports). __getattr__ catches any bare-name resolution that
# falls through manifest.py's pattern.
from backend.core.iceberg import _core as _core_mod


def __getattr__(name: str):
    return getattr(_core_mod, name)


# ---------------------------------------------------------------------------
# Buffer management
# ---------------------------------------------------------------------------


_TOMBSTONE_SUFFIX = ".consumed-"  # Followed by an integer Unix-epoch seconds value.
_TOMBSTONE_GRACE_SECONDS = 300  # See tombstone_buffer_files docstring for the rationale.

# Snapshot-summary property namespace for the buffer-commit marker. Each
# successful ``table.append`` tags its snapshot with one of these per
# buffer file in the chunk. On retry, the commit-recovery sweep scans
# recent snapshots for these markers and treats any matching buffer as
# already-committed — durable proof of append that survives even the
# SQLite committed_buffers checkpoint being lost (disk full, DB locked
# at the wrong moment, etc). Without this, the only durable record of
# "I appended this batch" is the SQLite write, which has a millisecond
# gap after table.append where a crash produces duplicate rows.
#
# Window: scan only snapshots from the last ``_COMMIT_MARKER_LOOKBACK_S``
# seconds so this stays cheap on long-lived tables with thousands of
# snapshots.
_COMMIT_MARKER_PREFIX = "app.buffer_commit_marker."
_COMMIT_MARKER_LOOKBACK_S = 3600  # 1 hour — far exceeds any plausible retry window


def _buffer_basename_marker(basename: str) -> str:
    """Deterministic short marker for a buffer file basename.

    Iceberg snapshot summary keys land in metadata.json so we want them
    short (12 hex chars = 48 bits, collision-free per chunk size).
    """
    return hashlib.sha256(basename.encode("utf-8")).hexdigest()[:12]


def _recent_snapshot_markers(table: Any, since_ms: int) -> set[str]:
    """Return the set of buffer-commit markers attached to snapshots
    since ``since_ms`` (unix epoch ms). The complementary half of
    ``_buffer_basename_marker``: post-restart, if a basename's marker
    appears here, ``table.append`` succeeded for that buffer regardless
    of whether the SQLite checkpoint landed.

    Defensive — any exception (transient catalog read failure, metadata
    incompatibility on an older table) returns an empty set so the
    caller falls back to the SQLite-only recovery path.
    """
    out: set[str] = set()
    try:
        for snap in table.snapshots():
            ts = getattr(snap, "timestamp_ms", 0) or 0
            if ts < since_ms:
                continue
            summary = getattr(snap, "summary", None)
            if summary is None:
                continue
            # snap.summary may expose either ``additional_properties``
            # (dict) or behave dict-like — handle both.
            props = getattr(summary, "additional_properties", None) or {}
            if not props and hasattr(summary, "__iter__"):
                try:
                    props = dict(summary)
                except Exception:
                    props = {}
            for k in props.keys():
                if isinstance(k, str) and k.startswith(_COMMIT_MARKER_PREFIX):
                    out.add(k[len(_COMMIT_MARKER_PREFIX) :])
    except Exception as e:
        logger.warning("%s _recent_snapshot_markers raised (continuing): %s", _core_mod._ICE, e)
    return out


def _tombstone_marker_path(parquet_path: str, ts: int) -> str:
    return f"{parquet_path}{_TOMBSTONE_SUFFIX}{ts}"


def _is_tombstone_marker(name: str) -> bool:
    """True iff ``name`` is a tombstone sidecar (``<basename>.parquet.consumed-<ts>``).

    Centralised so the glob filter, sweeper, and tests all share one
    definition. We only check the ``.parquet.consumed-`` substring to
    avoid being fooled by partial matches on bucket-name-like substrings.
    """
    if _TOMBSTONE_SUFFIX not in name:
        return False
    head, _, tail = name.rpartition(_TOMBSTONE_SUFFIX)
    return head.endswith(".parquet") and tail.isdigit()


def _tombstoned_parquet_paths(buf_dir: str, recursive: bool = True) -> set[str]:
    """Return the set of buffer parquet paths that have an active tombstone
    sibling. Used by ``buffer_files()`` to keep tombstoned files out of
    new view binds — they stay on disk for the grace window so any view
    bound BEFORE the tombstone can still read them."""
    tombstoned: set[str] = set()
    if not os.path.isdir(buf_dir):
        return tombstoned
    pattern = (
        os.path.join(buf_dir, "**", "*" + _TOMBSTONE_SUFFIX + "*")
        if recursive
        else os.path.join(buf_dir, "*" + _TOMBSTONE_SUFFIX + "*")
    )
    for p in _glob.glob(pattern, recursive=recursive):
        base = os.path.basename(p)
        if not _is_tombstone_marker(base):
            continue
        # Strip ``.consumed-<ts>`` to recover the original ``.parquet`` path.
        parquet_path = p.rsplit(_TOMBSTONE_SUFFIX, 1)[0]
        tombstoned.add(parquet_path)
    return tombstoned


def tombstone_buffer_files(source: dict, paths: list[str], *, ts: int | None = None) -> list[str]:
    """Mark buffer parquet files as logically consumed without unlinking them.

    Replaces the post-commit ``os.remove(path)`` race with a two-phase
    scheme:

    1. **Tombstone** (this function): write an empty sidecar file
       ``<path>.consumed-<unix_seconds>`` next to the original ``.parquet``.
       The original file stays on disk untouched. ``buffer_files()`` now
       filters it out via ``_tombstoned_parquet_paths``, so subsequent
       view rebuilds will not bind it. Crucially, any DuckDB view ALREADY
       bound to that path continues to work because the file is still
       readable.
    2. **Sweep** (``sweep_tombstoned_buffer_files``): after the
       ``_TOMBSTONE_GRACE_SECONDS`` (300 s) grace window elapses, the
       next commit run unlinks both the parquet and its tombstone
       sidecar. By then no view should reference the file — typical
       bind-to-execute windows are milliseconds, and the grace window
       comfortably exceeds the slowest cold query.

    Readers that bind at query time must EXCLUDE tombstoned files: their
    rows already live in the committed hourly partitions, so reading both
    double-counts (the 2026-07-07 active-hour live-slice bug —
    ``buffer_files()`` below and ``_create_active_hour_temp_direct`` in
    ``backend/repositories/_base.py`` both filter via
    ``_tombstoned_parquet_paths``). The on-disk grace copy exists ONLY
    for views bound before the commit.

    **Why this fixes the 2026-06-05 incident:** the previous code did
    ``os.remove(path)`` inline at commit time. A dashboard query whose
    view was bound BEFORE the commit would then hit "No files found"
    when DuckDB resolved the bound paths against disk. The
    ``QueryRunner.execute`` self-heal exists for this case but had its
    own race (cached-SQL re-bind under lock contention; see
    ``backend/repositories/_base.py:288``). Tombstoning closes the race
    at its source so the self-heal essentially never has to fire.

    Tombstone creation uses ``open(..., "x")`` to fail loudly on
    collisions instead of silently overwriting timing metadata. Errors
    during tombstoning are logged and skipped (no immediate-unlink
    fallback) — the grace window is the primary defense against
    in-flight queries that bound the path before this commit, and
    unlinking before the grace expires re-opens that race. The buffer
    file persists until the next commit cycle retries successfully.

    Returns the subset of ``paths`` that were successfully tombstoned.
    Callers that need atomicity should compare lengths.
    """
    if ts is None:
        ts = int(time.time())
    tombstoned: list[str] = []
    for path in paths:
        try:
            marker = _tombstone_marker_path(path, ts)
            with open(marker, "x"):
                pass
            tombstoned.append(path)
        except FileExistsError:
            # A previous commit at the exact same second already
            # tombstoned this file — already-consumed is fine, skip.
            tombstoned.append(path)
        except Exception as e:
            logger.warning(
                "%s Failed to tombstone buffer file %s — will retry next sweep. Error: %s",
                _core_mod._ICE,
                path,
                e,
            )
            # No immediate-unlink fallback: the 300s grace period is the
            # primary defense against in-flight queries that bound this
            # path before the commit. Unlinking now would re-open the
            # race the tombstone mechanism was added to close. Retry on
            # the next commit cycle (~5 min); if errors persist, the
            # buffer dir size monitor becomes the operational signal.
    return tombstoned


def sweep_tombstoned_buffer_files(
    source: dict, *, table_name: str = "logs", grace_seconds: int = _TOMBSTONE_GRACE_SECONDS, now: int | None = None
) -> int:
    """Unlink tombstoned buffer parquets whose grace window has elapsed.

    Called at the start of ``commit_buffer`` so the sweep cadence is
    naturally tied to the commit cron (no new cron registration). When
    a tombstone marker is at least ``grace_seconds`` old, both the
    parquet and the marker are unlinked. Younger tombstones are left
    alone — the corresponding parquet may still be referenced by an
    in-flight query bound before the tombstone was written.

    Returns the number of parquet files actually unlinked.
    """
    if now is None:
        now = int(time.time())
    buf = _core_mod._buffer_dir(source, table_name=table_name)
    if not os.path.isdir(buf):
        return 0
    swept = 0
    purged_basenames: list[str] = []
    recursive = table_name != "logs"
    pattern = (
        os.path.join(buf, "**", "*" + _TOMBSTONE_SUFFIX + "*")
        if recursive
        else os.path.join(buf, "*" + _TOMBSTONE_SUFFIX + "*")
    )
    for marker in _glob.glob(pattern, recursive=recursive):
        base = os.path.basename(marker)
        if not _is_tombstone_marker(base):
            continue
        try:
            ts = int(marker.rsplit(_TOMBSTONE_SUFFIX, 1)[1])
        except (ValueError, IndexError):
            continue
        if now - ts < grace_seconds:
            continue
        parquet_path = marker.rsplit(_TOMBSTONE_SUFFIX, 1)[0]
        # Unlink the parquet first so a partial failure doesn't leave
        # the file visible without its tombstone (which would re-bind
        # it into the next view rebuild).
        try:
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
        except Exception as e:
            logger.warning("%s Sweep failed to unlink %s: %s", _core_mod._ICE, parquet_path, e)
            continue
        try:
            os.remove(marker)
        except Exception as e:
            logger.warning("%s Sweep failed to unlink tombstone %s: %s", _core_mod._ICE, marker, e)
        purged_basenames.append(os.path.basename(parquet_path))
        swept += 1
    # Drop the matching committed_buffers checkpoint rows once the
    # parquet they referenced is gone from disk — keeps that table
    # bounded over time. Done in one batched DELETE per sweep to avoid
    # 1k tiny commits on a busy service.
    if purged_basenames:
        try:
            service_id = source.get("service_id") or source.get("name", "")
            _meta_mod.purge_committed_buffer_rows(service_id, purged_basenames)
        except Exception as e:
            logger.warning(
                "%s Sweep failed to purge committed_buffers rows (will retry next tick): %s",
                _core_mod._ICE,
                e,
            )
    return swept


def buffer_files(source: dict, table_name: str = "logs") -> list[str]:
    """Return sorted list of Parquet files currently in the local buffer.

    Excludes files that have been tombstoned by ``tombstone_buffer_files``
    so view rebuilds don't bind paths that are about to be swept. The
    tombstoned files remain on disk for the grace window so any view
    bound BEFORE the tombstone can still read them.
    """
    buf = _core_mod._buffer_dir(source, table_name=table_name)
    if not os.path.isdir(buf):
        return []
    recursive = table_name != "logs"
    tombstoned = _tombstoned_parquet_paths(buf, recursive=recursive)
    pattern = os.path.join(buf, "**", "*.parquet") if recursive else os.path.join(buf, "*.parquet")
    return sorted(
        p
        for p in _glob.glob(pattern, recursive=recursive)
        if os.path.isfile(p) and p not in tombstoned and not _is_tombstone_marker(os.path.basename(p))
    )


_QUARANTINE_SUBDIR = ".quarantine"


def _quarantine_dir(source: dict, table_name: str = "logs") -> str:
    """Path to the quarantine bucket for unreadable buffer parquet files.
    Lives under the buffer dir so the path is bucket-scoped and survives
    re-mount of the cache root."""
    return os.path.join(_core_mod._buffer_dir(source, table_name=table_name), _QUARANTINE_SUBDIR)


def _quarantine_buffer_file(source: dict, path: str, error: BaseException, table_name: str = "logs") -> str | None:
    """Move a corrupt buffer parquet into the quarantine subdir with a
    timestamped name and a sidecar JSON describing the failure.

    Why: without this, ``commit_buffer`` would re-read the same unreadable
    file on every cron tick forever, re-logging the same warning. Quarantine
    keeps the file on disk for human inspection (we never lose data) while
    removing it from the active commit path.

    Returns the new path, or None on failure (in which case the file is left
    in place — quarantine MUST NOT propagate exceptions back to commit_buffer).
    """
    try:
        import json
        from datetime import UTC, datetime

        qdir = _quarantine_dir(source, table_name=table_name)
        os.makedirs(qdir, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        base = os.path.basename(path)
        new_path = os.path.join(qdir, f"{ts}__{base}")
        # If a same-timestamp collision happens (extreme edge case), append a
        # counter rather than overwriting evidence.
        if os.path.exists(new_path):
            i = 1
            while os.path.exists(f"{new_path}.{i}"):
                i += 1
            new_path = f"{new_path}.{i}"
        os.rename(path, new_path)
        sidecar = new_path + ".json"
        from backend.utils.date_utils import iso_z_now

        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "original_path": path,
                    "quarantined_at": iso_z_now(),
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:2000],
                },
                f,
                indent=2,
            )
        logger.error(
            "%s Quarantined unreadable buffer parquet %s -> %s (%s: %s)",
            _core_mod._ICE,
            path,
            new_path,
            type(error).__name__,
            str(error)[:200],
        )
        return new_path
    except Exception as quarantine_err:
        logger.error(
            "%s Failed to quarantine buffer file %s — leaving in place. Quarantine error: %s",
            _core_mod._ICE,
            path,
            quarantine_err,
        )
        return None


def buffer_backlog_stats(source: dict, table_name: str = "logs") -> dict:
    """Snapshot of the local buffer right now: file count, total bytes, and
    age of the oldest file in seconds.

    Why: a healthy buffer is drained on every commit cycle. If commits start
    failing silently — catalog perms revoked, FOS unreachable, persistent
    schema mismatch — the buffer fills up and the only visible signal is
    growing disk usage. Surfacing oldest_age + file count lets the cron
    summary line shout when the drain is stuck.
    """
    files = buffer_files(source, table_name=table_name)
    if not files:
        return {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None}
    now = time.time()
    total_bytes = 0
    oldest_mtime = now
    oldest_path = files[0]
    for p in files:
        try:
            st = os.stat(p)
        except OSError:
            continue
        total_bytes += st.st_size
        if st.st_mtime < oldest_mtime:
            oldest_mtime = st.st_mtime
            oldest_path = p
    return {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "oldest_age_seconds": int(max(0, now - oldest_mtime)),
        "oldest_path": oldest_path,
    }


def write_to_buffer(source: dict, arrow_table: pa.Table, filename: str, table_name: str = "logs") -> str:
    """Write a PyArrow table to the local buffer as a Parquet file.

    Called by ingest() for each batch of processed rows. The file is written
    with ZSTD level 1 (fast) since it is short-lived hot data.

    Returns the path of the written file.
    """
    buf = _core_mod._buffer_dir(source, table_name=table_name)
    os.makedirs(buf, exist_ok=True)
    path = os.path.join(buf, filename)
    from backend import config as svcconfig

    cfg = svcconfig.load_config(source.get("service_id") or source.get("name"))
    log_fields_config = cfg.get("log_fields", {}) if cfg else None
    target_schema = _core_mod.get_arrow_schema(log_fields_config, table_name=table_name)
    aligned = _core_mod._align_to_schema(arrow_table, target_schema=target_schema, source=source)
    if "timestamp" in aligned.column_names:
        sort_keys = [("timestamp", "ascending")]
        if "ip" in aligned.column_names:
            sort_keys.append(("ip", "ascending"))
        aligned = aligned.sort_by(sort_keys)

    tmp_path = path + ".tmp"
    pq.write_table(aligned, tmp_path, compression="zstd", compression_level=1)
    os.rename(tmp_path, path)
    return path


# Max number of buffer parquets read+concatenated into a single
# table.append() call. At the project's typical row sizes a 50-file chunk
# materializes ~500-800 MB of pyarrow data in memory — large enough to
# amortize commit overhead, small enough to avoid OOM on a cron host with
# limited heap. Overridable via the BUFFER_COMMIT_CHUNK_SIZE env var so a
# user with a large machine + huge backlog can crank it without a deploy.
_BUFFER_COMMIT_CHUNK_SIZE = int(os.environ.get("BUFFER_COMMIT_CHUNK_SIZE", "50") or "50")


_CLOUD_WRITE_LOCK_TIMEOUT_S = 30.0


def commit_buffer(source: dict, progress_callback=None, table_name: str = "logs") -> dict:
    """Append all local buffer files to the Iceberg table.

    Acquires the per-service lock (the same one ``reset_service_logs``
    holds for its whole run) before touching FOS. Without this, a commit
    already past ``reset_service_logs``'s one-time ``cron_busy()`` check
    could append a new snapshot while the reset's ``iceberg/`` purge is
    concurrently deleting under the same prefix — corrupting the
    freshly-recreated table with dangling manifest references (even the
    post-reset CURRENT snapshot became unreadable in a production
    incident). If the lock is held by a reset, this cycle is skipped
    cleanly — the next commit tick retries, same as any other transient
    commit skip.
    """
    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s commit_buffer: service lock held (reset or another writer in progress) — skipping this cycle",
            _core_mod._ICE,
        )
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    try:
        return _commit_buffer_impl(source, progress_callback, table_name=table_name)
    finally:
        lock.release()


def _commit_buffer_impl(source: dict, progress_callback=None, table_name: str = "logs") -> dict:
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
    from backend.utils.sql_validator import escape_sql_literal

    # Sweep tombstones whose grace window elapsed — the sweep cadence is
    # tied to the commit cron on purpose (no separate cron registration).
    sweep_tombstoned_buffer_files(source, table_name=table_name)

    files = buffer_files(source, table_name=table_name)
    if not files:
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    con = get_connection(source)

    # Detach and re-attach as read-write
    try:
        con.execute("DETACH lake")
    except Exception:
        pass

    if not _ducklake_attach(con, source, read_only=False):
        logger.error("%s Failed to attach DuckLake in read-write mode", _core_mod._ICE)
        con.close()
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}

    lake_table = ducklake_table_name(source, table_name)
    lake_ident = 'lake."{}"'.format(lake_table.replace('"', '""'))

    rows_committed = 0
    committed_paths: list[str] = []
    try:
        for f in files:
            path_lit = escape_sql_literal(f)
            try:
                # Ensure the per-service table exists (schema cloned from the
                # buffer parquet — buffer files are already schema-aligned).
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {lake_ident} AS SELECT * FROM read_parquet('{path_lit}') LIMIT 0"
                )

                # Sync schema: add missing columns from parquet to the table
                table_cols: set[str] = set()
                try:
                    table_cols = {r[0] for r in con.execute(f"DESCRIBE {lake_ident}").fetchall()}
                    parquet_cols_res = con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{path_lit}') LIMIT 0"
                    ).fetchall()
                    parquet_cols = {r[0] for r in parquet_cols_res}
                    for p_col, p_type, *_ in parquet_cols_res:
                        if p_col not in table_cols:
                            col_ident = '"{}"'.format(p_col.replace('"', '""'))
                            con.execute(f"ALTER TABLE {lake_ident} ADD COLUMN {col_ident} {p_type}")
                            table_cols.add(p_col)
                except Exception as e:
                    logger.warning("%s Failed to sync schema for %s: %s", _core_mod._ICE, f, e)
                    parquet_cols = set()

                # Idempotent per-file commit: DELETE any rows previously
                # committed from the same raw FOS source keys, then INSERT —
                # inside ONE DuckLake transaction. A crash between INSERT and
                # tombstone used to duplicate the whole file on the next
                # cycle; now the retry replaces instead of appending. Keyed
                # on ``_source_file`` (the raw FOS object key ingest stamps
                # on every row); skipped when either side lacks the column
                # (e.g. RUM tables) — plain append, same as before.
                delete_by_source = "_source_file" in table_cols and "_source_file" in parquet_cols
                con.execute("BEGIN TRANSACTION")
                try:
                    if delete_by_source:
                        con.execute(
                            f"DELETE FROM {lake_ident} WHERE _source_file IN "
                            f"(SELECT DISTINCT _source_file FROM read_parquet('{path_lit}'))"
                        )
                    res = con.execute(
                        f"INSERT INTO {lake_ident} BY NAME SELECT * FROM read_parquet('{path_lit}')"
                    ).fetchone()
                    con.execute("COMMIT")
                except Exception:
                    try:
                        con.execute("ROLLBACK")
                    except Exception as rb_err:
                        logger.warning("%s ROLLBACK failed after commit error on %s: %s", _core_mod._ICE, f, rb_err)
                    raise
                if res:
                    rows_committed += res[0]
                committed_paths.append(f)
            except Exception as e:
                logger.error("%s Commit buffer error on %s: %s", _core_mod._ICE, f, e)
    finally:
        # Always restore to read-only for the pool
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        _ducklake_attach(con, source, read_only=True)
        con.close()

    # Tombstone (NOT unlink) the committed buffer parquets: views bound
    # BEFORE this commit still reference these paths, and a hard unlink
    # surfaces as "No files found" on their next read (the 2026-06-05
    # incident class). buffer_files() filters tombstoned paths, so new
    # view binds exclude them; the sweep above reclaims them after the
    # grace window.
    if committed_paths:
        tombstone_buffer_files(source, committed_paths)

    if rows_committed > 0:
        try:
            _core_mod._sync_metadata_pointer_from_discovery(source, table_name)
        except Exception as e:
            logger.warning("%s metadata pointer sync after commit failed: %s", _core_mod._ICE, e)

    return {
        "files_committed": len(committed_paths),
        "rows_committed": rows_committed,
        "snapshot_id": "ducklake",
        "quarantined_files": 0,
    }


def optimize_table(
    source: dict, target_file_size_mb: int = 128, min_files_per_partition: int | None = None, table_name: str = "logs"
) -> dict:
    """Compact small Iceberg data files into larger ones using rewrite_data_files.

    Acquires the per-service lock (see :func:`commit_buffer`) before
    touching FOS — rewrite_data_files commits a new snapshot, and racing
    that against a concurrent ``reset_service_logs`` purge has the same
    dangling-manifest corruption risk.
    """
    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s optimize_table: service lock held (reset or another writer in progress) — skipping this cycle",
            _core_mod._ICE,
        )
        return {"error": "service busy (reset or another writer in progress)", "files_rewritten": 0}
    try:
        return _optimize_table_impl(source, target_file_size_mb, min_files_per_partition, table_name=table_name)
    finally:
        lock.release()


def _optimize_table_impl(
    source: dict, target_file_size_mb: int = 128, min_files_per_partition: int | None = None, table_name: str = "logs"
) -> dict:
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach

    con = None
    try:
        con = get_connection(source)
        # Pool connections hold a READ-ONLY lake attach — re-attach
        # read-write for the rewrite (same dance as _commit_buffer_impl).
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        if not _ducklake_attach(con, source, read_only=False):
            return {"error": "Failed to attach DuckLake", "files_rewritten": 0}

        # DURABILITY, not an optimization: DuckLake "inlines" small commits
        # straight into the metadata catalog instead of writing parquet, and
        # NEITHER ducklake_rewrite_data_files NOR ducklake_merge_adjacent_files
        # promotes inlined rows — both only touch already-materialized files.
        # A table whose every commit was inlined therefore stays at
        # file_count = 0 forever, leaving the ONLY copy of the data inside the
        # catalog DB (the raw .gz is deleted after ingest). flush first so the
        # rewrite below has real files to compact.
        con.execute("CALL ducklake_flush_inlined_data('lake')").fetchall()

        # DuckLake rewrites small files directly, capped by the catalog's
        # target_file_size (pinned to LOCAL_COMPACT_MAX_PARTITION_MB at
        # attach — never collapse to fewer-larger files past the cap).
        con.execute("CALL ducklake_rewrite_data_files('lake')").fetchall()
        try:
            _core_mod._sync_metadata_pointer_from_discovery(source, table_name)
        except Exception as e:
            logger.warning("%s metadata pointer sync after rewrite failed: %s", _core_mod._ICE, e)
        return {"files_rewritten": -1, "files_added": -1, "eligible_partitions": 1, "partition_errors": []}
    except Exception as e:
        return {"error": str(e), "files_rewritten": 0}
    finally:
        if con:
            try:
                con.execute("DETACH lake")
            except Exception:
                pass
            _ducklake_attach(con, source, read_only=True)
            con.close()


def run_cloud_maintenance(source: dict) -> dict:
    """Run weekly maintenance: expire old metadata, delete old data, and purge old local cache.

    Acquires the per-service lock (see :func:`commit_buffer`) before
    touching FOS — both the retention delete and expire_snapshots commit
    against the table, with the same reset-race risk.
    """
    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s run_cloud_maintenance: service lock held (reset or another writer in progress) — skipping this cycle",
            _core_mod._ICE,
        )
        return {"error": "service busy (reset or another writer in progress)"}
    try:
        return _run_cloud_maintenance_impl(source)
    finally:
        lock.release()


# ---------------------------------------------------------------------------
# DuckLake retention + snapshot expiry (weekly maintenance steps 1 and 2)
# ---------------------------------------------------------------------------

# Retention operates on DuckLake tables via plain SQL, so the table name is
# interpolated (DuckDB cannot bind an identifier). ``ducklake_table_name``
# already sanitizes to ``[a-z0-9_]``, but Trap #4 says validate at the point
# of interpolation — a future naming change must fail loudly, not build SQL.
_LAKE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# RUM beacon telemetry lives in these DuckLake tables (see
# ``backend/core/iceberg/rum_schema.py``), keyed by ``cid``. ``logs.rum_cid``
# is only the CDN-side correlation key for the same session.
_RUM_BEACON_TABLES = ("client_vitals", "client_errors")


def _lake_ident(name: str) -> str:
    """Validate a DuckLake table name before interpolating it into SQL."""
    if not _LAKE_IDENT_RE.match(name or ""):
        raise ValueError(f"unsafe ducklake table name: {name!r}")
    return name


def _lake_columns(con, table: str) -> set[str]:
    """Column names of ``lake.<table>``, or an empty set if it doesn't exist.

    Probing with DESCRIBE (rather than catching a BinderException from the
    DELETE itself) is what keeps retention working on a schema that predates
    a column: an absent ``rum_cid`` falls back to the flat delete instead of
    aborting the whole step.
    """
    try:
        rows = con.execute(f'DESCRIBE lake."{_lake_ident(table)}"').fetchall()
    except Exception:
        return set()
    return {str(r[0]) for r in rows}


def _lake_delete_before(con, table: str, cutoff: datetime, extra_predicate: str = "") -> int:
    """``DELETE FROM lake.<table> WHERE timestamp < <cutoff> [AND <extra>]``.

    Returns the row count DuckDB reports for the delete. The timestamp bound
    is a real bound parameter and is ALWAYS present — this helper is the only
    place the maintenance job emits a DELETE, so there is no path here that
    can produce an unbounded one.
    """
    where = "timestamp < ?"
    if extra_predicate:
        where = f"{where} AND {extra_predicate}"
    row = con.execute(f'DELETE FROM lake."{_lake_ident(table)}" WHERE {where}', [cutoff]).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _ducklake_retention_delete(con, source: dict, data_retention_days: int, rum_retention_days: int) -> dict:
    """Step 1: enforce ``data_retention_days`` / ``rum_retention_days``.

    ``0`` means "keep forever" for BOTH knobs, and each knob only ever gates
    its own data:

    - ``data_retention_days`` governs the ``logs`` table. When
      ``rum_retention_days`` is LONGER, the prune keeps rows carrying a
      ``rum_cid`` (they are the request-side join partner for RUM telemetry
      that outlives them) and the ceiling pass drops them at the RUM cutoff.
      When it isn't longer, the prune is flat.
    - ``rum_retention_days`` governs RUM data: the ``client_vitals`` /
      ``client_errors`` beacon tables, plus that ``logs`` ceiling.

    The pyiceberg original reached the conditional-prune branch whenever
    ``rum_retention_days > data_retention_days``, INCLUDING when
    ``data_retention_days == 0`` — which resolved the cutoff to "now" and
    would have deleted every non-RUM row in the table. That never fired
    because the whole function was dead against DuckLake data; gating on
    ``data_retention_days > 0`` keeps "0 == forever" true now that it does.
    """
    from backend.core.iceberg._ducklake import ducklake_table_name

    out: dict[str, Any] = {}
    now = datetime.now(UTC)
    logs_table = ducklake_table_name(source, table_name="logs")
    deleted_anything = False

    if data_retention_days > 0:
        cutoff = now - timedelta(days=data_retention_days)
        predicate = ""
        if rum_retention_days > data_retention_days and "rum_cid" in _lake_columns(con, logs_table):
            predicate = "rum_cid IS NULL"
        out["data_deleted_before_days"] = data_retention_days
        out["data_rows_deleted"] = _lake_delete_before(con, logs_table, cutoff, predicate)
        deleted_anything = True

        # Ceiling expiration: once the RUM window closes, the rum_cid rows the
        # conditional prune spared go too. Scoped to services that actually
        # enabled log retention so a "keep logs forever" service can't have its
        # logs capped by the RUM knob.
        if rum_retention_days > 0:
            out["rum_deleted_before_days"] = rum_retention_days
            out["rum_log_rows_deleted"] = _lake_delete_before(con, logs_table, now - timedelta(days=rum_retention_days))

    if rum_retention_days > 0:
        rum_cutoff = now - timedelta(days=rum_retention_days)
        beacon_rows = 0
        beacon_tables = 0
        for t_name in _RUM_BEACON_TABLES:
            tbl = ducklake_table_name(source, table_name=t_name)
            if not _lake_columns(con, tbl):
                continue  # RUM was never provisioned for this service
            beacon_rows += _lake_delete_before(con, tbl, rum_cutoff)
            beacon_tables += 1
        if beacon_tables:
            out["rum_deleted_before_days"] = rum_retention_days
            out["rum_beacon_rows_deleted"] = beacon_rows
            deleted_anything = True

    if deleted_anything:
        # Retention removed rows from the committed table — the view-SQL and
        # snapshot-file caches for this service (and its sub-table keys) both
        # describe the pre-delete membership.
        _core_mod.clear_source_caches(source.get("name", "default"))
    return out


def _ducklake_expire_snapshots(con, source: dict, keep_snapshot_days: int) -> dict:
    """Step 2: prune DuckLake catalog snapshots older than ``keep_snapshot_days``.

    Verified signature (duckdb 1.5.4 + ducklake extension)::

        ducklake_expire_snapshots(catalog VARCHAR, dry_run BOOLEAN,
                                  versions UBIGINT[],
                                  older_than TIMESTAMP WITH TIME ZONE)

    No CAS-retry loop: the pyiceberg version needed one because a concurrent
    commit could advance the FOS metadata pointer between load and commit.
    DuckLake commits through a transactional catalog (SQLite file or Postgres)
    with its own conflict handling, ``run_cloud_maintenance`` already holds the
    per-service write lock, and neither ``CommitFailedException`` nor the
    "Snapshot … does not exist" ValueError can be raised from here — a retry
    keyed on those shapes would be dead code.

    Under a shared ``DUCKLAKE_CATALOG`` (Postgres, the celery topology) the
    snapshot log is catalog-WIDE, so the before/after counts and the expiry
    itself span every tenant in that catalog, not just ``source``.
    """
    out: dict[str, Any] = {}
    cutoff = datetime.now(UTC) - timedelta(days=keep_snapshot_days)

    before_row = con.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()
    snapshots_before = int(before_row[0]) if before_row else 0
    out["snapshots_before"] = snapshots_before

    if snapshots_before == 0:
        out["snapshots_expired_before_days"] = keep_snapshot_days
        out["snapshots_after"] = 0
        out["snapshots_expired_count"] = 0
        out["snapshot_expiry_note"] = "no snapshots present in the DuckLake catalog"
        logger.info("[ducklake] %s: no snapshots present to expire", source.get("name"))
        return out

    con.execute("CALL ducklake_expire_snapshots('lake', older_than => ?)", [cutoff]).fetchall()

    after_row = con.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()
    snapshots_after = int(after_row[0]) if after_row else 0
    snapshots_expired = max(0, snapshots_before - snapshots_after)
    out["snapshots_expired_before_days"] = keep_snapshot_days
    out["snapshots_after"] = snapshots_after
    out["snapshots_expired_count"] = snapshots_expired

    # Measured, not assumed: expiry alone reclaims NO bytes. It deletes catalog
    # snapshot rows and moves the parquet it unreferenced onto the catalog's
    # scheduled-for-deletion queue; ducklake_cleanup_old_files is what unlinks
    # those. That queue is swept on EVERY run, not only when this run expired
    # something — a file is scheduled at expiry time, so with the cutoff below
    # it is always a LATER run that reclaims it.
    #
    # cleanup_old_files only ever touches files the catalog itself already
    # marked deleted. ducklake_delete_orphaned_files, by contrast, sweeps the
    # data path by listing and would happily eat local-compaction output that
    # the catalog hasn't caught up with — never call that one here.
    #
    # Bounded by the same cutoff so an unreferenced file stays on disk for
    # keep_snapshot_days (a long-running reader bound to a pre-expiry snapshot
    # is the reason for the delay). Note a snapshot cannot be expired while a
    # live data file still anchors to it, so most reclamation only becomes
    # possible after the daily optimize job's ducklake_rewrite_data_files
    # supersedes those files — which is also what physically removes rows that
    # step 1 deleted (until then they persist behind delete files).
    files_cleaned: int | None = None
    try:
        cleaned = con.execute("CALL ducklake_cleanup_old_files('lake', older_than => ?)", [cutoff]).fetchall()
        files_cleaned = len(cleaned)
        out["data_files_cleaned"] = files_cleaned
    except Exception as e:
        logger.warning("[ducklake] %s: old-file cleanup after expiry failed: %s", source.get("name"), e)
        out["data_file_cleanup_error"] = str(e)

    if snapshots_expired > 0 or files_cleaned:
        out["snapshot_expiry_note"] = (
            "catalog metadata entries only; parquet is not deleted by the expiry itself — "
            f"ducklake_cleanup_old_files (older_than={keep_snapshot_days}d) unlinked "
            f"{files_cleaned if files_cleaned is not None else 'unknown'} file(s), and rows deleted "
            "by retention persist physically until the daily ducklake_rewrite_data_files rewrite"
        )
        logger.info(
            "[ducklake] %s: expired %d snapshots (%d -> %d), unlinked %s file(s)",
            source.get("name"),
            snapshots_expired,
            snapshots_before,
            snapshots_after,
            files_cleaned,
        )
    return out


def _run_ducklake_maintenance(
    source: dict, *, data_retention_days: int, rum_retention_days: int, keep_snapshot_days: int
) -> dict:
    """Run steps 1 and 2 against DuckLake on one read-write ``lake`` attach.

    Pool connections hold a READ-ONLY lake attach, so this performs the same
    DETACH / read-write re-attach / restore dance as ``_optimize_table_impl``.
    Each step is isolated: one failing records its own ``*_error`` key (which
    the cron wrapper turns into a ``warning`` run) and the other still runs.
    """
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import _ducklake_attach

    con = None
    try:
        con = get_connection(source)
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        if not _ducklake_attach(con, source, read_only=False):
            raise RuntimeError("Failed to attach DuckLake")
    except Exception as e:
        logger.warning("[ducklake] %s: maintenance could not open the lake: %s", source.get("name"), e)
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        return {"data_deletion_error": str(e), "snapshot_expiry_error": str(e)}

    out: dict[str, Any] = {}
    try:
        if data_retention_days > 0 or rum_retention_days > 0:
            try:
                out.update(_ducklake_retention_delete(con, source, data_retention_days, rum_retention_days))
            except Exception as e:
                logger.warning("[ducklake] Data deletion skipped: %s", e)
                out["data_deletion_error"] = str(e)
        try:
            out.update(_ducklake_expire_snapshots(con, source, keep_snapshot_days))
        except Exception as e:
            logger.warning("[ducklake] Snapshot expiry skipped: %s", e)
            out["snapshot_expiry_error"] = str(e)
    finally:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        _ducklake_attach(con, source, read_only=True)
        try:
            con.close()
        except Exception:
            pass
    return out


def _run_cloud_maintenance_impl(source: dict) -> dict:
    """Run weekly maintenance: retention deletion, snapshot expiry, local purges.

    1. Deletes rows from the DuckLake tables older than ``data_retention_days``
       (default 30) / ``rum_retention_days``.
    2. Expires DuckLake catalog snapshots older than ``keep_snapshot_days``
       (default 7) and unlinks the parquet that expiry unreferenced.
    3. Deletes local cache Parquet older than ``cache_retention_days`` (90).
    4. Deletes local rollup Parquet older than ``rollup_retention_months`` (12).

    Steps 1 and 2 were pyiceberg-based until v3.0.0. Since the commit path
    moved to DuckLake they operated on a catalog that receives no commits, so
    customer retention deletion silently never ran.
    """
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        data_retention_days = int(cron_sync.get("data_retention_days", 30))
        rum_retention_days = int(cron_sync.get("rum_retention_days", data_retention_days))
        cache_retention_days = int(cron_sync.get("cache_retention_days", 90))
        # Snapshot-history window. This — not the job's cadence — sets the
        # steady-state snapshot count, and therefore the catalog's snapshot
        # table size and per-commit cost. Lowering it speeds up commits but
        # trades away time-travel: the 2026-08 metadata rollback was only
        # recoverable because old snapshots were still around. Keep 7 unless
        # you have a specific reason.
        keep_snapshot_days = max(1, int(cron_sync.get("keep_snapshot_days", 7)))
    except Exception as e:
        return {"error": str(e)}

    results: dict[str, Any] = {}

    # 1. Retention deletion + 2. snapshot expiry, both against DuckLake.
    results.update(
        _run_ducklake_maintenance(
            source,
            data_retention_days=data_retention_days,
            rum_retention_days=rum_retention_days,
            keep_snapshot_days=keep_snapshot_days,
        )
    )

    # 3. Clean up local cache
    if cache_retention_days > 0:
        try:
            from backend.core.duckdb import _cache_dir

            cache_dir = os.path.join(_cache_dir(source), "data")
            if os.path.exists(cache_dir):
                cache_cutoff = datetime.now(UTC) - timedelta(days=cache_retention_days)
                deleted_files = 0
                for root, _, files in os.walk(cache_dir):
                    for file in files:
                        if not file.endswith(".parquet"):
                            continue
                        filepath = os.path.join(root, file)
                        mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=UTC)
                        if mtime < cache_cutoff:
                            try:
                                os.remove(filepath)
                                deleted_files += 1
                            except Exception:
                                pass
                _core_mod._prune_empty_dirs(cache_dir)
                results["local_cache_files_deleted"] = deleted_files
        except Exception as e:
            logger.warning("[iceberg] Local cache cleanup skipped: %s", e)
            results["local_cache_error"] = str(e)

    # 4. Clean up old rollups
    rollup_retention_months = int(cron_sync.get("rollup_retention_months") or 12)
    if rollup_retention_months > 0:
        try:
            from backend.core.duckdb import _cache_dir

            rollup_dir = os.path.join(_cache_dir(source), "rollups")
            if os.path.exists(rollup_dir):
                # Approximation: 30 days per month
                rollup_cutoff = datetime.now(UTC) - timedelta(days=rollup_retention_months * 30)
                deleted_rollups = 0
                for root, _, files in os.walk(rollup_dir):
                    for file in files:
                        if not file.endswith(".parquet"):
                            continue
                        filepath = os.path.join(root, file)
                        mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=UTC)
                        if mtime < rollup_cutoff:
                            try:
                                os.remove(filepath)
                                deleted_rollups += 1
                            except Exception:
                                pass
                _core_mod._prune_empty_dirs(rollup_dir)
                results["local_rollup_files_deleted"] = deleted_rollups
        except Exception as e:
            logger.warning("[iceberg] Local rollup cleanup skipped: %s", e)
            results["local_rollup_error"] = str(e)

    return results


# ---------------------------------------------------------------------------
# DuckDB integration
# ---------------------------------------------------------------------------
