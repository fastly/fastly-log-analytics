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
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa

logger = logging.getLogger("backend.core.iceberg._core")


# Library + util imports the carved code references. Some pyiceberg
# names also appear as inline imports inside specific functions; we
# add the top-level ones here so the bare-name lookup works.
import pyarrow.parquet as pq
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table.name_mapping import create_mapping_from_schema

# Late-bind helpers from the main _core module (it's mid-load when this
# file imports). __getattr__ catches any bare-name resolution that
# falls through manifest.py's pattern.
from backend.core.iceberg import _core as _core_mod
from backend.utils.sql_validator import escape_sql_literal


def __getattr__(name: str):
    return getattr(_core_mod, name)


# ---------------------------------------------------------------------------
# Buffer management
# ---------------------------------------------------------------------------


_TOMBSTONE_SUFFIX = ".consumed-"  # Followed by an integer Unix-epoch seconds value.
_TOMBSTONE_GRACE_SECONDS = 300  # See tombstone_buffer_files docstring for the rationale.


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


def _tombstoned_parquet_paths(buf_dir: str) -> set[str]:
    """Return the set of buffer parquet paths that have an active tombstone
    sibling. Used by ``buffer_files()`` to keep tombstoned files out of
    new view binds — they stay on disk for the grace window so any view
    bound BEFORE the tombstone can still read them."""
    tombstoned: set[str] = set()
    if not os.path.isdir(buf_dir):
        return tombstoned
    for p in _glob.glob(os.path.join(buf_dir, "**", "*" + _TOMBSTONE_SUFFIX + "*"), recursive=True):
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
    2. **Sweep** (``sweep_tombstoned_buffer_files``): after a grace
       window (default 60 s) elapses, the next commit run unlinks both
       the parquet and its tombstone sidecar. By then no view should
       reference the file — typical bind-to-execute windows are
       milliseconds, and 60 s comfortably exceeds the slowest cold query.

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
    during tombstoning are swallowed (logged) — losing a tombstone just
    means the file MIGHT be retained until a manual cleanup, never that
    the wrong file gets unlinked.

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
                "%s Failed to tombstone buffer file %s — falling back to immediate unlink. Error: %s",
                _core_mod._ICE,
                path,
                e,
            )
            # If tombstoning fails (disk full, permission flap), preserve
            # the prior behaviour rather than letting the buffer file
            # accumulate forever. The race we're fixing is preferable
            # to an unbounded buffer dir.
            try:
                os.remove(path)
                tombstoned.append(path)
            except Exception:
                pass
    return tombstoned


def sweep_tombstoned_buffer_files(
    source: dict, *, grace_seconds: int = _TOMBSTONE_GRACE_SECONDS, now: int | None = None
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
    buf = _core_mod._buffer_dir(source)
    if not os.path.isdir(buf):
        return 0
    swept = 0
    for marker in _glob.glob(os.path.join(buf, "**", "*" + _TOMBSTONE_SUFFIX + "*"), recursive=True):
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
        swept += 1
    return swept


def buffer_files(source: dict) -> list[str]:
    """Return sorted list of Parquet files currently in the local buffer.

    Excludes files that have been tombstoned by ``tombstone_buffer_files``
    so view rebuilds don't bind paths that are about to be swept. The
    tombstoned files remain on disk for the grace window so any view
    bound BEFORE the tombstone can still read them.
    """
    buf = _core_mod._buffer_dir(source)
    if not os.path.isdir(buf):
        return []
    tombstoned = _tombstoned_parquet_paths(buf)
    return sorted(
        p
        for p in _glob.glob(os.path.join(buf, "**", "*.parquet"), recursive=True)
        if os.path.isfile(p) and p not in tombstoned and not _is_tombstone_marker(os.path.basename(p))
    )


_QUARANTINE_SUBDIR = ".quarantine"


def _quarantine_dir(source: dict) -> str:
    """Path to the quarantine bucket for unreadable buffer parquet files.
    Lives under the buffer dir so the path is bucket-scoped and survives
    re-mount of the cache root."""
    return os.path.join(_core_mod._buffer_dir(source), _QUARANTINE_SUBDIR)


def _quarantine_buffer_file(source: dict, path: str, error: BaseException) -> str | None:
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

        qdir = _quarantine_dir(source)
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
        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "original_path": path,
                    "quarantined_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
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


def buffer_backlog_stats(source: dict) -> dict:
    """Snapshot of the local buffer right now: file count, total bytes, and
    age of the oldest file in seconds.

    Why: a healthy buffer is drained on every commit cycle. If commits start
    failing silently — catalog perms revoked, FOS unreachable, persistent
    schema mismatch — the buffer fills up and the only visible signal is
    growing disk usage. Surfacing oldest_age + file count lets the cron
    summary line shout when the drain is stuck.
    """
    files = buffer_files(source)
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


def write_to_buffer(source: dict, arrow_table: pa.Table, filename: str) -> str:
    """Write a PyArrow table to the local buffer as a Parquet file.

    Called by ingest() for each batch of processed rows. The file is written
    with ZSTD level 1 (fast) since it is short-lived hot data.

    Returns the path of the written file.
    """
    buf = _core_mod._buffer_dir(source)
    os.makedirs(buf, exist_ok=True)
    path = os.path.join(buf, filename)
    aligned = _core_mod._align_to_schema(arrow_table, source=source)
    if "timestamp" in aligned.column_names:
        sort_keys = [("timestamp", "ascending")]
        if "ip" in aligned.column_names:
            sort_keys.append(("ip", "ascending"))
        aligned = aligned.sort_by(sort_keys)
    pq.write_table(aligned, path, compression="zstd", compression_level=1)
    return path


# Max number of buffer parquets read+concatenated into a single
# table.append() call. At the project's typical row sizes a 50-file chunk
# materializes ~500-800 MB of pyarrow data in memory — large enough to
# amortize commit overhead, small enough to avoid OOM on a cron host with
# limited heap. Overridable via the BUFFER_COMMIT_CHUNK_SIZE env var so a
# user with a large machine + huge backlog can crank it without a deploy.
_BUFFER_COMMIT_CHUNK_SIZE = int(os.environ.get("BUFFER_COMMIT_CHUNK_SIZE", "50") or "50")


def commit_buffer(source: dict, progress_callback=None) -> dict:
    """Append all local buffer files to the Iceberg table.

    Splits the buffer into chunks of ``_core_mod._BUFFER_COMMIT_CHUNK_SIZE`` files,
    appending each chunk as its own Iceberg snapshot. Why chunked:
      * **Memory bound** — the old code concatenated every buffer file
        into a single in-process pa.Table. At 200+ files this OOM'd the
        commit cron. Chunking caps peak memory at one chunk's worth.
      * **Crash safety** — each chunk that lands becomes a durable
        snapshot, and its files are deleted from the buffer immediately.
        If the process dies mid-loop, the next commit cron picks up the
        un-committed remainder rather than redoing work.

    Returns ``{files_committed, rows_committed, snapshot_id, quarantined_files}``.
    ``snapshot_id`` is the LAST snapshot id produced by the loop (the one
    the metadata pointer now references).
    """
    # Sweep any tombstoned buffers whose grace window has elapsed before
    # we scan for fresh work. Co-locating the sweep with the commit cron
    # avoids a separate scheduler registration; the cadence (every commit
    # tick) easily covers the 60 s grace window.
    try:
        swept = sweep_tombstoned_buffer_files(source)
        if swept:
            logger.info("%s Swept %d tombstoned buffer file(s) past grace window", _core_mod._ICE, swept)
    except Exception as sweep_err:
        # Sweep failures must NEVER block a commit — the file just stays
        # on disk until the next sweep tick.
        logger.warning("%s Tombstone sweep raised (continuing with commit): %s", _core_mod._ICE, sweep_err)

    files = buffer_files(source)
    if not files:
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}

    if progress_callback:
        progress_callback("status", f"Found {len(files)} buffer file(s) to commit")

    table = _core_mod._init_iceberg_table_locked(source, create=False)
    if not table:
        table = _core_mod.init_iceberg_table(source)

    try:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        target_arrow_schema = schema_to_pyarrow(table.schema())
    except Exception as e:
        logger.warning(f"[iceberg] Failed to extract arrow schema from iceberg table: {e}")
        target_arrow_schema = None

    # Apply name-mapping once up-front so we don't repeat the check per chunk.
    if "schema.name-mapping.default" not in table.properties:
        if progress_callback:
            progress_callback("status", "Updating table name-mapping...")
        from backend import config as _cfg_mod

        _cfg = _cfg_mod.load_config(source.get("service_id") or source.get("name"))
        _lf_cfg = _cfg.get("log_fields", {}) if _cfg else None
        _mapping = create_mapping_from_schema(_core_mod.get_iceberg_schema(_lf_cfg)).model_dump_json()
        table.transaction().set_properties({"schema.name-mapping.default": _mapping}).commit()

    chunk_size = max(1, _core_mod._BUFFER_COMMIT_CHUNK_SIZE)
    total_files = len(files)
    total_chunks = (total_files + chunk_size - 1) // chunk_size
    total_rows = 0
    total_committed_paths: list[str] = []
    quarantined_count = 0
    snapshot_id: int | None = None

    for chunk_idx in range(total_chunks):
        chunk_paths = files[chunk_idx * chunk_size : (chunk_idx + 1) * chunk_size]
        if progress_callback:
            progress_callback(
                "status",
                f"Reading chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_paths)} files)...",
            )
        tables: list[pa.Table] = []
        chunk_successful: list[str] = []
        for path in chunk_paths:
            try:
                t = pq.read_table(path)
                tables.append(_core_mod._align_to_schema(t, target_schema=target_arrow_schema, source=source))
                chunk_successful.append(path)
            except Exception as e:
                _quarantine_buffer_file(source, path, e)
                quarantined_count += 1
        if not tables:
            continue
        combined = pa.concat_tables(tables, promote_options="default")
        chunk_rows = len(combined)
        if progress_callback:
            progress_callback(
                "status",
                f"Appending chunk {chunk_idx + 1}/{total_chunks} ({chunk_rows:,} rows) to Iceberg table in FOS...",
            )
        table.append(combined)
        # Free the chunk's in-memory tables before the next iteration so
        # peak RSS doesn't accumulate across chunks.
        del tables, combined
        snapshot_id = table.current_snapshot().snapshot_id if table.current_snapshot() else snapshot_id
        total_rows += chunk_rows
        # Per-chunk tombstone: if we crash on a later chunk, the next
        # commit cron only re-processes the un-committed remainder
        # (tombstoned files are excluded from buffer_files()). The
        # actual ``os.remove`` is deferred to ``sweep_tombstoned_buffer_files``
        # after a grace window so concurrent dashboard queries whose
        # view was bound BEFORE this commit don't crash on
        # "No files found ... batch_X.parquet". See
        # ``tombstone_buffer_files`` docstring for the full rationale.
        tombstone_buffer_files(source, chunk_successful)
        total_committed_paths.extend(chunk_successful)

    if not total_committed_paths:
        return {
            "files_committed": 0,
            "rows_committed": 0,
            "snapshot_id": snapshot_id,
            "quarantined_files": quarantined_count,
        }

    # Cache the post-commit table so the metadata_sync that fires next on this
    # thread (scheduler.py: _run_metadata_sync → _core_mod.init_iceberg_table) reuses it
    # instead of paying another ~865 KB metadata.json GET for the file we
    # just PUT seconds ago. Pointer-mismatch in _core_mod._load_table_cached protects
    # cross-process correctness.
    _core_mod._set_cached_table(source, _core_mod._table_identifier(source), table)

    # Apply the new snapshot's added-files delta to _core_mod._snapshot_files_cache
    # BEFORE _core_mod._write_metadata_pointer spawns the async table-summary thread.
    # Order matters: the async thread races straight into _get_cached_or_scan_metadata
    # which reads _manifest_metadata_cache; the delta path pre-seeds that cache for
    # the new manifest, eliminating a redundant ~10 KB .avro GET per commit. Without
    # the swap, the async worker can scan the manifest before the delta seed lands.
    # The delta also avoids the next _core_mod.sync_data's full tbl.scan().plan_files() —
    # re-reading ~1080 immutable manifest files just to find the handful we added.
    try:
        _core_mod._update_snapshot_cache_from_delta(source, table)
    except Exception as e:
        logger.warning("[iceberg] snapshot cache delta update raised: %s", e)

    _core_mod._write_metadata_pointer(source, table.metadata_location, table=table)

    if progress_callback:
        progress_callback("status", "Cleaning up local buffer files...")
    _core_mod._prune_empty_dirs(_core_mod._buffer_dir(source))

    if quarantined_count:
        logger.warning(
            "%s Committed %d rows from %d buffer file(s) in %d chunk(s); quarantined %d unreadable file(s), snapshot %s",
            _core_mod._ICE,
            total_rows,
            len(total_committed_paths),
            total_chunks,
            quarantined_count,
            snapshot_id,
        )
    else:
        logger.info(
            "%s Committed %d rows from %d buffer file(s) in %d chunk(s), snapshot %s",
            _core_mod._ICE,
            total_rows,
            len(total_committed_paths),
            total_chunks,
            snapshot_id,
        )
    return {
        "files_committed": len(total_committed_paths),
        "rows_committed": total_rows,
        "snapshot_id": snapshot_id,
        "quarantined_files": quarantined_count,
    }


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------


def optimize_table(source: dict, target_file_size_mb: int = 128, min_files_per_partition: int | None = None) -> dict:
    """Compact small Iceberg data files into larger ones using rewrite_data_files.

    Identifies partitions with too many small files and rewrites them into
    single larger files to maintain metadata health and query performance.

    Args:
      min_files_per_partition: only partitions with strictly more than this
        many files are eligible for compaction. When None (default), the
        threshold is auto-derived from observed file counts so the cron
        self-tunes to traffic volume:

          - Low-traffic site (avg ~3 files/partition): threshold ~2, very
            aggressive — every multi-file partition gets compacted.
          - High-traffic site (avg ~50 files/partition): threshold scales
            up so we don't churn freshly-written files that the next sync
            will append to anyway.

        Pass an explicit number to override (e.g. 1 for a one-shot
        aggressive cleanup on first migration).
    """
    try:
        catalog = _core_mod._get_catalog(source)
        table = _core_mod._load_table_cached(source, _core_mod._table_identifier(source), catalog)
    except Exception as e:
        if "does not exist" in str(e):
            return {"error": "Iceberg table does not exist.", "files_rewritten": 0}
        return {"error": str(e), "files_rewritten": 0}

    # 1. Group files by partition to identify candidates for compaction
    partition_groups: dict[tuple, list] = {}  # partition_values -> [DataFile]

    try:
        for f in table.scan().plan_files():
            # partition is a Record of values like Record[492000]
            # We convert it to a tuple to use as a dict key
            p_val = tuple(f.file.partition)
            if p_val not in partition_groups:
                partition_groups[p_val] = []
            partition_groups[p_val].append(f.file)
    except Exception as e:
        return {"error": f"Failed to scan partitions: {e}", "files_rewritten": 0}

    # Auto-derive threshold from observed file counts when not pinned by the
    # caller. Use the median: robust against outlier hot partitions (e.g. a
    # spike during DDoS) skewing the threshold up. Floor at 2 so we always
    # compact ANY partition with 3+ files; ceiling at 50 to avoid silly
    # numbers from extreme spikes.
    if min_files_per_partition is None:
        sizes = sorted(len(files) for files in partition_groups.values())
        if sizes:
            median = sizes[len(sizes) // 2]
            min_files_per_partition = max(2, min(50, median))
        else:
            min_files_per_partition = 10
        logger.info(
            "🗜️  [optimize] %s: auto-derived threshold=%d (median files/partition=%d across %d partitions)",
            source.get("name"),
            min_files_per_partition,
            sizes[len(sizes) // 2] if sizes else 0,
            len(sizes),
        )

    total_rewritten = 0
    total_added = 0
    partition_errors: list[str] = []
    eligible_partitions = sum(1 for files in partition_groups.values() if len(files) > min_files_per_partition)

    from backend.core.duckdb import get_connection

    # optimize_table only uses DuckDB to read parquet files for partition
    # rewrites; the actual writes happen through PyIceberg's overwrite path.
    # RO + skip-view avoids contending with the writer lock and the view
    # refresh that we don't need here.
    con = get_connection(source, skip_view_update=True, read_only=True)

    try:
        for p_val, files in partition_groups.items():
            if len(files) <= min_files_per_partition:
                continue

            # We want to rewrite these files.
            # We'll use DuckDB to read them and PyIceberg's overwrite logic.
            # But wait, PyIceberg's overwrite() with a filter is the safest way.
            # We need to build a filter for this specific partition.

            # Since we only partition by timestamp_hour (ID 1000):
            hour_val = p_val[0]
            # Convert hour since epoch back to a timestamp for the filter
            from datetime import datetime

            start_ts = datetime.fromtimestamp(hour_val * 3600, tz=UTC)
            end_ts = datetime.fromtimestamp((hour_val + 1) * 3600, tz=UTC)

            try:
                overwrite_filter = f"timestamp >= '{start_ts.isoformat()}' AND timestamp < '{end_ts.isoformat()}'"
                _CAS_RETRIES = 3
                for _retry in range(_CAS_RETRIES):
                    # Use DuckDB to read only these files (most efficient)
                    paths = [f.file_path for f in files]
                    paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in paths)

                    # Read into PyArrow. Must materialise to a Table — pyiceberg's
                    # overwrite() rejects RecordBatchReader with
                    # "Expected PyArrow table". DuckDB 1.5.x's .arrow() now returns
                    # a streaming reader, so use to_arrow_table() (or the older
                    # fetch_arrow_table() alias) to force materialisation. Skipping
                    # this turned every nightly optimize run into a silent no-op
                    # — the ValueError got logged as a warning to stderr and the
                    # cron recorded success with 0 files rewritten.
                    # ``union_by_name=True``: when a partition contains files
                    # written before AND after a schema bump (e.g. ``edge_sid``
                    # / ``edge_cookie_compliance`` / ``edge_score*`` added
                    # mid-day on 2026-06-01), the default positional union
                    # raises ``Schema mismatch ... try setting
                    # union_by_name=True`` and the partition lands in
                    # ``partition_errors``. With union-by-name DuckDB merges
                    # the column sets and fills missing columns with NULL,
                    # matching how Iceberg already presents the merged schema
                    # to readers. Verified prod incident 2026-06-06: two
                    # partitions (494541, 494542) had been stuck at ~14 files
                    # each since the schema bump because every nightly
                    # optimize attempt raised here. (#optimize-cron-warning)
                    arrow_table = con.execute(
                        f"SELECT * FROM read_parquet([{paths_sql}], hive_partitioning=false, union_by_name=true)"
                    ).to_arrow_table()

                    # Perform an atomic overwrite of the specific time range.
                    # In Iceberg, this will delete the old files and add the
                    # new one. Wrapped in a small retry that reloads the
                    # table on the sequence-number CAS conflict that fires
                    # when an ingest commit lands between our plan_files
                    # read and this overwrite — pyiceberg refuses with
                    # ``ValueError: Cannot add snapshot with sequence
                    # number N older than last sequence number N``. The
                    # retry just refetches the table head and tries once
                    # more; ingest's 5-min cadence makes the contention
                    # window small enough that a single retry almost always
                    # wins.
                    try:
                        table.overwrite(df=arrow_table, overwrite_filter=overwrite_filter)
                        break
                    except ValueError as cas_err:
                        if "older than last sequence number" not in str(cas_err):
                            raise
                        if _retry == _CAS_RETRIES - 1:
                            raise
                        # Refresh the table to pick up the new head.
                        # Bypass _core_mod._load_table_cached (which short-circuits
                        # on pointer match) by going straight to the
                        # catalog — we need the absolute latest snapshot
                        # to commit on top of, not whatever's cached.
                        logger.warning(
                            "[optimize] %s: CAS conflict on hour %d (attempt %d/%d), reloading table and retrying: %s",
                            source.get("name"),
                            hour_val,
                            _retry + 1,
                            _CAS_RETRIES,
                            cas_err,
                        )
                        try:
                            table = catalog.load_table(_core_mod._table_identifier(source))
                            _core_mod._set_cached_table(source, _core_mod._table_identifier(source), table)
                            files = [f.file for f in table.scan().plan_files() if tuple(f.file.partition) == p_val]
                            if not files:
                                raise cas_err
                        except Exception as reload_err:
                            logger.warning(
                                "[optimize] %s: table reload failed after CAS conflict, giving up on this partition: %s",
                                source.get("name"),
                                reload_err,
                            )
                            raise cas_err from reload_err
                _core_mod._set_cached_table(source, _core_mod._table_identifier(source), table)
                _core_mod._write_metadata_pointer(source, table.metadata_location, table=table)

                # File rewrites can't be cleanly delta-tracked (old files are
                # marked DELETED, a new file is ADDED — the cache's prev_files
                # list now contains stale entries). Invalidate so the next
                # _core_mod.sync_data falls into the slow path and rebuilds from scratch.
                _core_mod._snapshot_files_cache.pop(source.get("name", "default"), None)
                _core_mod._view_cache.pop(source.get("name", "default"), None)

                total_rewritten += len(files)
                total_added += 1
                logger.info(
                    "🗜️ \x1b[92m[optimize]\x1b[0m %s: Compacted %d files into 1 for hour %d",
                    source.get("name"),
                    len(files),
                    hour_val,
                )

                # Immediately cache the newly rewritten large file
                try:
                    _core_mod.sync_data(source)
                except Exception as e:
                    logger.warning("[iceberg] Failed to eagerly sync data after optimize: %s", e)
            except Exception as e:
                logger.warning("[iceberg] Failed to compact partition %s: %s", p_val, e)
                partition_errors.append(f"partition {p_val}: {type(e).__name__}: {e}")
                continue

    finally:
        con.close()

    result: dict[str, Any] = {"files_rewritten": total_rewritten, "files_added": total_added}
    # Surface partial failures so the cron wrapper can flag them — silent
    # per-partition warnings turned a real regression (pyiceberg rejecting
    # DuckDB's RecordBatchReader from .arrow()) into a week of "Rewrote 0
    # files into 0 files" successes.
    if partition_errors:
        result["partition_errors"] = partition_errors
        result["eligible_partitions"] = eligible_partitions
    return result


def run_cloud_maintenance(source: dict) -> dict:
    """Run weekly maintenance: expire old metadata, delete old data, and purge old local cache.

    1. Deletes log data from Iceberg older than `data_retention_days` (default 30).
    2. Deletes local Parquet files older than `cache_retention_days` (default 90).
    3. Expires Iceberg snapshots older than 7 days to reclaim metadata storage.
    """
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        data_retention_days = int(cron_sync.get("data_retention_days", 30))
        cache_retention_days = int(cron_sync.get("cache_retention_days", 90))

        catalog = _core_mod._get_catalog(source)
        table = _core_mod._load_table_cached(source, _core_mod._table_identifier(source), catalog)
    except Exception as e:
        return {"error": str(e)}

    results: dict[str, Any] = {}

    # 1. Delete old data from Iceberg table
    if data_retention_days > 0:
        data_cutoff_ms = int((datetime.now(UTC) - timedelta(days=data_retention_days)).timestamp() * 1000)
        try:
            # Delete directly from the table using the timestamp column
            from backend.utils.iceberg_expr import lt

            table.delete(lt("timestamp", (datetime.now(UTC) - timedelta(days=data_retention_days)).isoformat()))
            _core_mod._set_cached_table(source, _core_mod._table_identifier(source), table)
            results["data_deleted_before_days"] = data_retention_days
            # Retention delete removes files from the snapshot — the cache's
            # prev_files list would still reference them. Invalidate so the
            # next _core_mod.sync_data rebuilds from a fresh manifest scan.
            _core_mod._snapshot_files_cache.pop(source.get("name", "default"), None)
            _core_mod._view_cache.pop(source.get("name", "default"), None)
        except Exception as e:
            logger.warning("[iceberg] Data deletion skipped: %s", e)
            results["data_deletion_error"] = str(e)

    # 2. Expire snapshots (keep last 7 days of metadata).
    #    pyiceberg 0.11.1: table.maintenance.expire_snapshots().older_than(datetime).commit()
    #    — maintenance is a @property (no parens); older_than takes a tz-aware datetime
    #    (not int millis). Only removes snapshot METADATA entries — the underlying
    #    data/manifest files on the object store are NOT garbage-collected; a separate
    #    remove_orphan_files sweep is required for byte reclamation (deferred until
    #    pyiceberg >= 0.12, which gains that API).
    #
    #    Cache hygiene: intentionally do NOT pop _core_mod._snapshot_files_cache / _core_mod._view_cache
    #    here — expire drops only old snapshot metadata; the current snapshot's file
    #    membership is unchanged, so the snapshot fast-path stays valid. (Contrast
    #    with step 1's data-delete and the optimize-table path, which do invalidate.)
    keep_snapshot_days = 7
    snapshot_cutoff = datetime.now(UTC) - timedelta(days=keep_snapshot_days)
    try:
        # Load fresh from the catalog. Note: catalog is the FosSqlCatalog
        # whose load_table consults _read_metadata_pointer (2-sec in-process
        # cache); freshness here is bounded by _POINTER_CACHE_TTL_SEC, not
        # "the absolute latest head". For the FIRST attempt this is fine —
        # the cache entry will be ≤2s old, plenty fresh for a weekly cron.
        # The retry loop below explicitly invalidates the cache before each
        # reload so back-to-back retries actually see post-conflict state.
        fresh_table = catalog.load_table(_core_mod._table_identifier(source))
        snapshots_before = len(fresh_table.metadata.snapshots)
        results["snapshots_before"] = snapshots_before

        # Concurrent writers can race us in two shapes that the retry can
        # self-heal:
        #   (a) CommitFailedException — catalog-level pointer race (another
        #       commit advanced the metadata pointer between our load_table
        #       and our commit).
        #   (b) ValueError("Snapshot with snapshot id N does not exist") —
        #       another expire run (admin re-trigger overlapping the scheduled
        #       run) already removed snapshots that are still in our expire
        #       set. Reloading and re-calling older_than rebuilds the expire
        #       set against the post-overlap snapshot list, so the next attempt
        #       targets only still-present snapshots.
        # The sequence-number ValueError that optimize_table catches cannot
        # fire here — ExpireSnapshots stages only AssertTableUUID (no
        # AssertRefSnapshotId), so we narrow the ValueError check to the
        # "does not exist" message to avoid masking unrelated bugs.
        _EXPIRE_RETRIES = 3
        for _retry in range(_EXPIRE_RETRIES):
            try:
                fresh_table.maintenance.expire_snapshots().older_than(snapshot_cutoff).commit()
                break
            except (CommitFailedException, ValueError) as cas_err:
                msg = str(cas_err)
                is_recoverable = isinstance(cas_err, CommitFailedException) or "does not exist" in msg
                if not is_recoverable or _retry == _EXPIRE_RETRIES - 1:
                    raise
                logger.warning(
                    "[iceberg] %s: CAS conflict expiring snapshots (attempt %d/%d), reloading and retrying: %s",
                    source.get("name"),
                    _retry + 1,
                    _EXPIRE_RETRIES,
                    cas_err,
                )
                try:
                    # Invalidate the FosSqlCatalog pointer cache so the reload
                    # bypasses the 2-sec _POINTER_CACHE_TTL_SEC and actually
                    # re-resolves the post-conflict metadata pointer. Without
                    # this, all retries finish within microseconds and read
                    # the same pre-conflict cache entry.
                    _core_mod._pointer_cache_invalidate(source, _core_mod._table_identifier(source))
                    fresh_table = catalog.load_table(_core_mod._table_identifier(source))
                except Exception as reload_err:
                    raise cas_err from reload_err
                # Re-pin the baseline against the reloaded head so the diff
                # below reflects expirations only, not concurrent additions.
                snapshots_before = len(fresh_table.metadata.snapshots)
                results["snapshots_before"] = snapshots_before

        snapshots_after = len(fresh_table.metadata.snapshots)
        snapshots_expired = max(0, snapshots_before - snapshots_after)

        _core_mod._set_cached_table(source, _core_mod._table_identifier(source), fresh_table)
        _core_mod._write_metadata_pointer(source, fresh_table.metadata_location, table=fresh_table)
        # Keep the outer-scope `table` consistent for the local-cache cleanup
        # step below (currently doesn't use it, but a future addition between
        # steps 2 and 3 would expect the post-expire handle).
        table = fresh_table

        results["snapshots_expired_before_days"] = keep_snapshot_days
        results["snapshots_after"] = snapshots_after
        results["snapshots_expired_count"] = snapshots_expired
        if snapshots_expired > 0:
            results["snapshot_expiry_note"] = (
                "metadata entries only; underlying data/manifest files are not deleted by pyiceberg 0.11.1"
            )
            logger.info(
                "[iceberg] %s: expired %d snapshots (%d -> %d)",
                source.get("name"),
                snapshots_expired,
                snapshots_before,
                snapshots_after,
            )
    except Exception as e:
        logger.warning("[iceberg] Snapshot expiry skipped: %s", e)
        results["snapshot_expiry_error"] = str(e)

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
                        # Use file modification time as a proxy for file age
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

    return results


# ---------------------------------------------------------------------------
# DuckDB integration
# ---------------------------------------------------------------------------
