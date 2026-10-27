"""Cloud copy of committed DuckLake Parquet, and the restore that rebuilds local data from it.

Standard mode keeps DuckLake data on local disk. After every commit the files the catalog considers live
are pushed to ``{prefix}/ducklake/<path relative to the data root>`` in the service's FOS bucket, and keys
whose file the catalog no longer references (compaction, retention) are deleted. The FOS listing is the
only state: nothing is recorded in Postgres, so the copy stays usable when the catalog database is lost
along with the local disk. ``restore_from_cloud`` downloads whatever is missing locally and registers the
files in DuckLake again.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from pathlib import Path

import duckdb

from backend.config import is_high_scale_mode
from backend.utils.sql_validator import escape_sql_literal

logger = logging.getLogger(__name__)

BACKUP_TABLES = ("logs", "client_vitals", "client_errors")
_ADD_BATCH = 200
_DELETE_BATCH = 1000

_lock = threading.Lock()
_running: set[str] = set()
_pending: dict[str, tuple[dict, tuple[str, ...]]] = {}


def _service_id(source: dict) -> str:
    return str(source.get("service_id") or source.get("name") or "default")


def _key_root(source: dict) -> str:
    prefix = (source.get("prefix") or "").strip().strip("/")
    return f"{prefix}/ducklake/" if prefix else "ducklake/"


def _local_root(source: dict) -> str | None:
    """The local DuckLake data root, or None when data already lives in FOS (High-Scale) or there is no bucket."""
    from backend.core.storage._core import _resolve_data_path

    if not source.get("bucket"):
        return None
    root = _resolve_data_path(source, _service_id(source))
    return None if root.startswith(("s3://", "http://", "https://")) else os.path.realpath(root)


def _live_files(con, table: str) -> list[str]:
    """Data files the catalog's current snapshot references for ``table`` (empty if the table is absent)."""
    try:
        rows = con.execute(f"SELECT data_file FROM ducklake_list_files('lake', '{escape_sql_literal(table)}')")
        return [r[0] for r in rows.fetchall()]
    except Exception:
        return []


def _list_keys(client, bucket: str, prefix: str) -> dict[str, int]:
    keys: dict[str, int] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", ()):
            keys[obj["Key"]] = int(obj.get("Size", 0))
    return keys


def push_to_cloud(source: dict, tables: tuple[str, ...] = BACKUP_TABLES) -> dict:
    """Mirror the live DuckLake data files of ``tables`` to FOS. Returns upload/delete counts or a skip reason."""
    from backend.core.duckdb import _get_fos_client
    from backend.core.storage._core import _ducklake_attach, ducklake_table_name

    root = _local_root(source)
    if root is None:
        return {"skipped": "data is not on local disk or no bucket is configured"}
    bucket, key_root = source["bucket"], _key_root(source)
    client = _get_fos_client(source)
    uploaded = deleted = uploaded_bytes = 0
    con = duckdb.connect(":memory:")
    try:
        if not _ducklake_attach(con, source, read_only=True):
            raise RuntimeError("could not attach the DuckLake catalog")
        for name in tables:
            table = ducklake_table_name(source, name)
            live = {}
            for path in _live_files(con, table):
                real = os.path.realpath(path)
                if os.path.isfile(real) and os.path.commonpath([root, real]) == root:
                    live[key_root + Path(os.path.relpath(real, root)).as_posix()] = real
            table_keys = f"{key_root}main/{table}/"
            remote = _list_keys(client, bucket, table_keys)
            for key, path in live.items():
                size = os.path.getsize(path)
                if remote.get(key) == size:
                    continue
                with open(path, "rb") as fh:
                    client.put_object(Bucket=bucket, Key=key, Body=fh, ContentLength=size)
                uploaded += 1
                uploaded_bytes += size
            # An empty live set means a wiped or unreadable catalog, not "everything was compacted away".
            stale = [k for k in remote if k not in live] if live else []
            for i in range(0, len(stale), _DELETE_BATCH):
                batch = stale[i : i + _DELETE_BATCH]
                client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
                deleted += len(batch)
    finally:
        con.close()
    return {"uploaded": uploaded, "uploaded_bytes": uploaded_bytes, "deleted": deleted}


def schedule_cloud_push(source: dict, tables: tuple[str, ...] = BACKUP_TABLES) -> None:
    """Run ``push_to_cloud`` on a per-service single-flight daemon thread; requests made meanwhile coalesce into one rerun."""
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons() or not source.get("bucket") or is_high_scale_mode(source):
        return
    service_id = _service_id(source)
    with _lock:
        if service_id in _running:
            prev = _pending.get(service_id, (source, ()))[1]
            _pending[service_id] = (source, tuple(dict.fromkeys(prev + tables)))
            return
        _running.add(service_id)
    threading.Thread(
        target=_push_worker, args=(service_id, source, tables), name=f"cloud-push:{service_id}", daemon=True
    ).start()


def _push_worker(service_id: str, source: dict, tables: tuple[str, ...]) -> None:
    from backend.utils.telemetry import process_context_scope

    while True:
        try:
            with process_context_scope("cron.cloud_push"):
                result = push_to_cloud(source, tables)
            if result.get("uploaded") or result.get("deleted"):
                logger.info("[cloud-push] %s: %s", service_id, result)
        except Exception:
            logger.warning("[cloud-push] %s: push to FOS failed; the next commit retries", service_id, exc_info=True)
        with _lock:
            nxt = _pending.pop(service_id, None)
            if nxt is None:
                _running.discard(service_id)
                return
            source, tables = nxt


def _normalize(path: str) -> str:
    return path if path.startswith(("s3://", "http://", "https://")) else os.path.realpath(path)


def _rebuild_ingest_index(con, service_id: str, name: str, ident: str) -> int:
    """Re-record every raw object whose rows are in the restored table, so discovery does not ingest it again.

    ``ingested_files`` lives in Postgres and is lost together with the catalog; each row carries the raw object it
    came from in ``_source_file``. Objects that produced no rows are not recoverable and are simply re-read.
    """
    from backend.core.metadata.ingest_log import insert_ingested_files

    try:
        rows = con.execute(
            f"SELECT _source_file, count(*) FROM {ident} WHERE _source_file IS NOT NULL GROUP BY 1"
        ).fetchall()
    except Exception:
        return 0
    for i in range(0, len(rows), _ADD_BATCH * 5):
        insert_ingested_files(service_id, [(f, int(n), None) for f, n in rows[i : i + _ADD_BATCH * 5]], table_name=name)
    return len(rows)


def restore_from_cloud(source: dict, progress: Callable[[str], None] | None = None) -> dict:
    """Rebuild local DuckLake data from the FOS copy: download missing files, then register them in DuckLake.

    Works with an intact catalog (only the local files are gone) and with an empty one (a fresh Postgres):
    tables are created from the Parquet schema and the files added without rewriting them. The Postgres
    ``ingested_files`` index is rebuilt from the restored rows' ``_source_file`` so raw objects still in the bucket are
    not ingested twice.
    """
    from backend.core.duckdb import _get_fos_client
    from backend.core.storage._core import _ducklake_attach, clear_source_caches, ducklake_table_name

    say = progress or (lambda _m: None)
    root = _local_root(source)
    if root is None:
        raise RuntimeError("restore needs a bucket and local DuckLake storage (Standard mode)")
    bucket, key_root = source["bucket"], _key_root(source)
    client = _get_fos_client(source)
    remote = _list_keys(client, bucket, key_root)
    downloaded = 0
    for key, size in remote.items():
        dest = Path(root) / key[len(key_root) :]
        if dest.is_file() and dest.stat().st_size == size:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".restoring")
        client.download_file(bucket, key, str(tmp))
        os.replace(tmp, dest)
        downloaded += 1
    say(f"Downloaded {downloaded} of {len(remote)} files from FOS")

    registered = reindexed = 0
    con = duckdb.connect(":memory:")
    try:
        if not _ducklake_attach(con, source, read_only=False):
            raise RuntimeError("could not attach the DuckLake catalog")
        for name in BACKUP_TABLES:
            table = ducklake_table_name(source, name)
            files = sorted(os.path.realpath(p) for p in (Path(root) / "main" / table).glob("*.parquet"))
            if not files:
                continue
            ident = 'lake."{}"'.format(table.replace('"', '""'))
            row = con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ?", [table]
            ).fetchone()
            exists = bool(row and row[0])
            if not exists:
                lits = ", ".join(f"'{escape_sql_literal(f)}'" for f in files)
                con.execute(
                    f"CREATE TABLE {ident} AS SELECT * FROM read_parquet([{lits}], union_by_name = true) LIMIT 0"
                )
            known = {_normalize(p) for p in _live_files(con, table)}
            missing = [f for f in files if f not in known]
            for i in range(0, len(missing), _ADD_BATCH):
                lits = ", ".join(f"'{escape_sql_literal(f)}'" for f in missing[i : i + _ADD_BATCH])
                con.execute(
                    f"CALL ducklake_add_data_files('lake', '{escape_sql_literal(table)}', [{lits}], "
                    "allow_missing := true, ignore_extra_columns := true)"
                )
            registered += len(missing)
            say(f"{name}: registered {len(missing)} file(s)")
            reindexed += _rebuild_ingest_index(con, _service_id(source), name, ident)
    finally:
        con.close()
    clear_source_caches(source.get("name", "default"))
    return {"downloaded": downloaded, "registered": registered, "remote_files": len(remote), "reindexed": reindexed}
