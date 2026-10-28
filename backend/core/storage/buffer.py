"""Local Parquet buffer staging, tombstones, quarantine, and promotion/commit to DuckLake."""

from __future__ import annotations

import glob as _glob
import logging
import os
import time
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from backend import config
from backend.core.duckdb import escape_sql_literal
from backend.core.storage.catalog import (
    _CLOUD_WRITE_LOCK_TIMEOUT_S,
    _ICE,
    RUM_NATURAL_KEYS,
    _align_to_schema,
    _ducklake_write_connection,
    _get_service_lock,
    clear_source_caches,
    ducklake_table_name,
    get_arrow_schema,
)

logger = logging.getLogger("backend.core.storage._core")


def _buffer_dir(source: dict, table_name: str = "logs") -> str:
    from backend.core.duckdb import _cache_dir

    base = os.path.join(_cache_dir(source), "buffer")
    return os.path.join(base, table_name) if table_name != "logs" else base


_TOMBSTONE_SUFFIX = ".consumed-"
_TOMBSTONE_GRACE_SECONDS = 300


def _tombstone_marker_path(parquet_path: str, ts: int) -> str:
    return f"{parquet_path}{_TOMBSTONE_SUFFIX}{ts}"


def _is_tombstone_marker(name: str) -> bool:
    """True iff ``name`` is a tombstone sidecar (``<basename>.parquet.consumed-<ts>``)."""
    if _TOMBSTONE_SUFFIX not in name:
        return False
    head, _, tail = name.rpartition(_TOMBSTONE_SUFFIX)
    return head.endswith(".parquet") and tail.isdigit()


def _iter_tombstone_markers(buf_dir: str, recursive: bool = True) -> list[str]:
    if not os.path.isdir(buf_dir):
        return []
    pat = (
        os.path.join(buf_dir, "**", f"*{_TOMBSTONE_SUFFIX}*")
        if recursive
        else os.path.join(buf_dir, f"*{_TOMBSTONE_SUFFIX}*")
    )
    return [p for p in _glob.glob(pat, recursive=recursive) if _is_tombstone_marker(os.path.basename(p))]


def _tombstoned_parquet_paths(buf_dir: str, recursive: bool = True) -> set[str]:
    """Return buffer parquet paths that have an active tombstone sibling."""
    return {p.rsplit(_TOMBSTONE_SUFFIX, 1)[0] for p in _iter_tombstone_markers(buf_dir, recursive=recursive)}


def tombstone_buffer_files(source: dict, paths: list[str], *, ts: int | None = None) -> list[str]:
    """Mark buffer parquet files as logically consumed via ``.consumed-<ts>`` sidecars without unlinking."""
    if ts is None:
        ts = int(time.time())
    tombstoned: list[str] = []
    for path in paths:
        try:
            with open(_tombstone_marker_path(path, ts), "x"):
                pass
            tombstoned.append(path)
        except FileExistsError:
            tombstoned.append(path)
        except Exception as e:
            logger.warning(
                "%s Failed to tombstone buffer file %s — will retry next sweep. Error: %s",
                _ICE,
                path,
                e,
            )
    return tombstoned


def sweep_tombstoned_buffer_files(
    source: dict, *, table_name: str = "logs", grace_seconds: int = _TOMBSTONE_GRACE_SECONDS, now: int | None = None
) -> int:
    """Unlink tombstoned buffer parquets whose grace window has elapsed."""
    if now is None:
        now = int(time.time())
    buf = _buffer_dir(source, table_name=table_name)
    swept = 0
    for marker in _iter_tombstone_markers(buf, recursive=(table_name != "logs")):
        try:
            ts = int(marker.rsplit(_TOMBSTONE_SUFFIX, 1)[1])
        except (ValueError, IndexError):
            continue
        if now - ts < grace_seconds:
            continue
        parquet_path = marker.rsplit(_TOMBSTONE_SUFFIX, 1)[0]
        try:
            if os.path.exists(parquet_path):
                os.remove(parquet_path)
        except Exception as e:
            logger.warning("%s Sweep failed to unlink %s: %s", _ICE, parquet_path, e)
            continue
        try:
            os.remove(marker)
        except Exception as e:
            logger.warning("%s Sweep failed to unlink tombstone %s: %s", _ICE, marker, e)
        swept += 1
    return swept


def buffer_files(source: dict, table_name: str = "logs") -> list[str]:
    """Return sorted list of non-tombstoned Parquet files currently in the local buffer."""
    buf = _buffer_dir(source, table_name=table_name)
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
    return os.path.join(_buffer_dir(source, table_name=table_name), _QUARANTINE_SUBDIR)


def _quarantine_buffer_file(source: dict, path: str, error: BaseException, table_name: str = "logs") -> str | None:
    """Move a corrupt buffer parquet into ``.quarantine/`` with a JSON sidecar."""
    try:
        import json

        from backend.utils.date_utils import iso_z_now

        qdir = _quarantine_dir(source, table_name=table_name)
        os.makedirs(qdir, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        new_path = os.path.join(qdir, f"{ts}__{os.path.basename(path)}")
        if os.path.exists(new_path):
            i = 1
            while os.path.exists(f"{new_path}.{i}"):
                i += 1
            new_path = f"{new_path}.{i}"
        os.rename(path, new_path)
        with open(new_path + ".json", "w", encoding="utf-8") as f:
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
            _ICE,
            path,
            new_path,
            type(error).__name__,
            str(error)[:200],
        )
        return new_path
    except Exception as quarantine_err:
        logger.error(
            "%s Failed to quarantine buffer file %s — leaving in place. Quarantine error: %s",
            _ICE,
            path,
            quarantine_err,
        )
        return None


def buffer_backlog_stats(source: dict, table_name: str = "logs") -> dict:
    """Snapshot of the local buffer: file count, total bytes, and oldest file age in seconds."""
    files = buffer_files(source, table_name=table_name)
    if not files:
        return {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None}
    now, total_bytes, oldest_mtime, oldest_path = time.time(), 0, time.time(), files[0]
    for p in files:
        try:
            st = os.stat(p)
        except OSError:
            continue
        total_bytes += st.st_size
        if st.st_mtime < oldest_mtime:
            oldest_mtime, oldest_path = st.st_mtime, p
    return {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "oldest_age_seconds": int(max(0, now - oldest_mtime)),
        "oldest_path": oldest_path,
    }


def write_to_buffer(source: dict, arrow_table: pa.Table, filename: str, table_name: str = "logs") -> str:
    """Write a PyArrow table to the local buffer as a ZSTD(1) Parquet file."""
    buf = _buffer_dir(source, table_name=table_name)
    os.makedirs(buf, exist_ok=True)
    path = os.path.join(buf, filename)
    cfg = config.load_config(source.get("service_id") or source.get("name"))
    target_schema = get_arrow_schema(cfg.get("log_fields", {}) if cfg else None, table_name=table_name)
    aligned = _align_to_schema(arrow_table, target_schema=target_schema, source=source)
    if "timestamp" in aligned.column_names:
        sort_keys = [("timestamp", "ascending")] + ([("ip", "ascending")] if "ip" in aligned.column_names else [])
        aligned = aligned.sort_by(sort_keys)
    tmp_path = path + ".tmp"
    pq.write_table(aligned, tmp_path, compression="zstd", compression_level=1)
    os.rename(tmp_path, path)
    return path


_BUFFER_COMMIT_CHUNK_SIZE = int(os.environ.get("BUFFER_COMMIT_CHUNK_SIZE", "50") or "50")


def commit_buffer(source: dict, progress_callback=None, table_name: str = "logs") -> dict:
    """Append all local buffer files to the DuckLake table under the per-service lock."""
    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s commit_buffer: service lock held (reset or another writer in progress) — skipping this cycle",
            _ICE,
        )
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}
    try:
        return _commit_buffer_impl(source, progress_callback, table_name=table_name)
    finally:
        lock.release()


def _commit_one_chunk(
    source: dict, lake_ident: str, files: list[str], table_name: str
) -> tuple[int, list[str], int, list[str]]:
    """Commit a single bounded chunk of buffer files under one DuckLake write-attach."""
    rows_committed, committed_paths, quarantined_files, file_errors = 0, [], 0, []

    with _ducklake_write_connection(source) as con:

        def _commit_paths(paths: list[str]) -> int:
            paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in paths)
            scan_sql = f"read_parquet([{paths_sql}], union_by_name=true)"
            row_count_res = con.execute(
                f"SELECT COALESCE(SUM(num_rows), 0) FROM parquet_file_metadata([{paths_sql}])"
            ).fetchone()
            batch_rows = int(row_count_res[0]) if row_count_res and row_count_res[0] is not None else 0
            con.execute(f"CREATE TABLE IF NOT EXISTS {lake_ident} AS SELECT * FROM {scan_sql} LIMIT 0")
            table_cols: set[str] = set()
            try:
                table_cols = {r[0] for r in con.execute(f"DESCRIBE {lake_ident}").fetchall()}
                parquet_cols_res = con.execute(f"DESCRIBE SELECT * FROM {scan_sql} LIMIT 0").fetchall()
                parquet_cols = {r[0] for r in parquet_cols_res}
                for p_col, p_type, *_ in parquet_cols_res:
                    if p_col not in table_cols:
                        col_ident = '"{}"'.format(p_col.replace('"', '""'))
                        con.execute(f"ALTER TABLE {lake_ident} ADD COLUMN {col_ident} {p_type}")
                        table_cols.add(p_col)
            except Exception as e:
                logger.warning("%s Failed to sync schema for paths: %s", _ICE, e)
                parquet_cols = set()

            delete_by_source = "_source_file" in table_cols and "_source_file" in parquet_cols
            nat_keys = RUM_NATURAL_KEYS.get(table_name)
            delete_by_natural = (
                not delete_by_source
                and nat_keys is not None
                and all(k in table_cols and k in parquet_cols for k in nat_keys)
            )
            con.execute("BEGIN TRANSACTION")
            try:
                if delete_by_source:
                    con.execute(
                        f"DELETE FROM {lake_ident} WHERE _source_file IN (SELECT DISTINCT _source_file FROM {scan_sql})"
                    )
                    con.execute(f"INSERT INTO {lake_ident} BY NAME SELECT * FROM {scan_sql}")
                elif delete_by_natural and nat_keys:
                    join_cond = " AND ".join(f'lake_tbl."{k}" IS NOT DISTINCT FROM incoming."{k}"' for k in nat_keys)
                    con.execute(
                        f"DELETE FROM {lake_ident} AS lake_tbl WHERE EXISTS (SELECT 1 FROM {scan_sql} AS incoming WHERE {join_cond})"
                    )
                    nat_cols_expr = ", ".join(f'"{k}"' for k in nat_keys)
                    con.execute(
                        f"INSERT INTO {lake_ident} BY NAME SELECT * EXCLUDE (_dup_rn) FROM ("
                        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {nat_cols_expr} ORDER BY timestamp) AS _dup_rn FROM {scan_sql}"
                        f") WHERE _dup_rn = 1"
                    )
                else:
                    con.execute(f"INSERT INTO {lake_ident} BY NAME SELECT * FROM {scan_sql}")
                con.execute("COMMIT")
            except Exception as e:
                con.execute("ROLLBACK")
                raise e
            return batch_rows

        try:
            batch_rows = _commit_paths(files)
            rows_committed, committed_paths = batch_rows, list(files)
        except Exception as batch_err:
            logger.warning(
                "%s Full-batch commit failed (%s) — falling back to per-file commits with quarantine",
                _ICE,
                batch_err,
            )
            for f in files:
                try:
                    file_rows = pq.read_metadata(f).num_rows
                    _commit_paths([f])
                    rows_committed += file_rows
                    committed_paths.append(f)
                except Exception as inner_err:
                    logger.error("%s Commit buffer error on %s: %s", _ICE, f, inner_err)
                    if (
                        isinstance(inner_err, pa.ArrowInvalid)
                        or any(
                            tok in str(inner_err)
                            for tok in ("is not a valid Parquet file", "No files found", "Missing page")
                        )
                    ) and _quarantine_buffer_file(source, f, inner_err, table_name):
                        quarantined_files += 1
                        continue
                    file_errors.append(f"{os.path.basename(f)}: {inner_err}")

    return rows_committed, committed_paths, quarantined_files, file_errors


def _commit_buffer_impl(source: dict, progress_callback=None, table_name: str = "logs") -> dict:
    swept = sweep_tombstoned_buffer_files(source, table_name=table_name)
    files = buffer_files(source, table_name=table_name)
    if not files:
        if swept:
            clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
        return {"files_committed": 0, "rows_committed": 0, "snapshot_id": None, "quarantined_files": 0}

    lake_table = ducklake_table_name(source, table_name)
    lake_ident = 'lake."{}"'.format(lake_table.replace('"', '""'))
    rows_committed, committed_paths, quarantined_files, commit_errors = 0, [], 0, []

    for i in range(0, len(files), _BUFFER_COMMIT_CHUNK_SIZE):
        chunk = files[i : i + _BUFFER_COMMIT_CHUNK_SIZE]
        try:
            chunk_rows, chunk_paths, chunk_quarantined, chunk_errors = _commit_one_chunk(
                source, lake_ident, chunk, table_name
            )
        except Exception as chunk_err:
            logger.error("%s DuckLake buffer chunk failed: %s", _ICE, chunk_err)
            commit_errors.append(str(chunk_err))
            break
        rows_committed += chunk_rows
        committed_paths.extend(chunk_paths)
        quarantined_files += chunk_quarantined
        commit_errors.extend(chunk_errors)

    if committed_paths:
        tombstone_buffer_files(source, committed_paths)
        clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)

    result = {
        "files_committed": len(committed_paths),
        "rows_committed": rows_committed,
        "snapshot_id": "ducklake",
        "quarantined_files": quarantined_files,
    }
    if commit_errors:
        result["error"] = "; ".join(commit_errors)
    return result
