"""Cloud copy of committed DuckLake Parquet, and the restore that rebuilds local data from it.

Standard mode keeps DuckLake data on local disk. After every commit the files the catalog considers live
are pushed to ``{prefix}/ducklake/<path relative to the data root>`` in the service's FOS bucket, and keys
whose file the catalog no longer references (compaction, retention) are deleted. The FOS listing is the
only state of the copy, so it stays usable when the catalog database is lost along with the local disk.
``restore_from_cloud`` downloads whatever is missing locally and registers the files in DuckLake again.

Deleting needs care after such a loss: a fresh catalog knows almost nothing, so "not live in this catalog"
would describe the whole old backup. A push therefore only deletes keys the current catalog can have
produced (uploaded after its oldest retained snapshot), until the catalog has *adopted* the copy: a
``_catalog.json`` marker naming the catalog's identity, written when no foreign key exists (a new install,
or an established deployment whose copy only holds its own files) and by ``restore_from_cloud``.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import psycopg
from botocore.exceptions import ClientError

from backend.config import is_high_scale_mode
from backend.core.metadata.usage_log import record_cloud_copy_deletion
from backend.utils.sql_validator import escape_sql_literal
from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)

BACKUP_TABLES = ("logs", "client_vitals", "client_errors")
_ADD_BATCH = 200
_DELETE_BATCH = 1000
_MARKER_NAME = "_catalog.json"

_lock = threading.Lock()
_running: set[str] = set()
_pending: dict[str, dict] = {}


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


def _list_keys(client, bucket: str, prefix: str) -> dict[str, tuple[int, datetime]]:
    keys: dict[str, tuple[int, datetime]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", ()):
            keys[obj["Key"]] = (int(obj.get("Size", 0)), obj["LastModified"])
    return keys


def _catalog_identity(source: dict) -> tuple[str, datetime] | None:
    """(uuid of the catalog's ``main`` schema, oldest retained snapshot time), or None for a non-Postgres catalog."""
    from backend.core.storage._core import _resolve_catalog_dsn

    dsn = _resolve_catalog_dsn(_service_id(source))
    if not dsn:
        return None
    with psycopg.connect(dsn.removeprefix("postgres:")) as pg:
        schema = pg.execute(
            "SELECT schema_uuid FROM ducklake_schema WHERE schema_name = 'main' AND end_snapshot IS NULL "
            "ORDER BY schema_id LIMIT 1"
        ).fetchone()
        snap = pg.execute("SELECT min(snapshot_time) FROM ducklake_snapshot").fetchone()
    if not schema or not schema[0] or not snap or not snap[0]:
        return None
    return str(schema[0]), snap[0]


def _read_marker(client, bucket: str, key: str) -> dict | None:
    try:
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return None
        raise


def _write_marker(client, bucket: str, key: str, catalog_uuid: str) -> None:
    body = json.dumps({"catalog_uuid": catalog_uuid, "adopted_at": datetime.now(UTC).isoformat()}).encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def push_to_cloud(source: dict) -> dict:
    """Mirror the live DuckLake data files of every table to FOS. Returns counts, or a skip reason."""
    from backend.core.duckdb import _get_fos_client
    from backend.core.storage._core import _ducklake_attach, ducklake_table_name

    root = _local_root(source)
    if root is None:
        return {"skipped": "data is not on local disk or no bucket is configured"}
    bucket, key_root = source["bucket"], _key_root(source)
    marker_key = key_root + _MARKER_NAME
    client = _get_fos_client(source)
    identity = _catalog_identity(source)
    marker = _read_marker(client, bucket, marker_key)
    adopted = identity is not None and marker is not None and marker.get("catalog_uuid") == identity[0]
    uploaded = deleted = uploaded_bytes = foreign = deleted_bytes = 0
    con = duckdb.connect(":memory:")
    try:
        if not _ducklake_attach(con, source, read_only=True):
            raise RuntimeError("could not attach the DuckLake catalog")
        for name in BACKUP_TABLES:
            table = ducklake_table_name(source, name)
            live = {}
            for path in _live_files(con, table):
                real = os.path.realpath(path)
                if os.path.isfile(real) and os.path.commonpath([root, real]) == root:
                    live[key_root + Path(os.path.relpath(real, root)).as_posix()] = real
            remote = _list_keys(client, bucket, f"{key_root}main/{table}/")
            for key, path in live.items():
                size = os.path.getsize(path)
                if remote.get(key, (None,))[0] == size:
                    continue
                with open(path, "rb") as fh:
                    client.put_object(Bucket=bucket, Key=key, Body=fh, ContentLength=size)
                uploaded += 1
                uploaded_bytes += size
            stale = [k for k in remote if k not in live]
            if not adopted:
                # Without adoption only keys uploaded after this catalog's oldest snapshot are provably its own.
                own = [k for k in stale if identity is not None and remote[k][1] >= identity[1]]
                foreign += len(stale) - len(own)
                stale = own
            # An empty live set means a wiped or unreadable catalog, not "everything was compacted away".
            stale = stale if live else []
            for i in range(0, len(stale), _DELETE_BATCH):
                batch = stale[i : i + _DELETE_BATCH]
                client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
                deleted += len(batch)
                deleted_bytes += sum(remote[k][0] for k in batch)
    finally:
        con.close()
    if deleted:
        try:
            record_cloud_copy_deletion(_service_id(source), deleted_bytes, deleted)
        except Exception:
            logger.warning(
                "[cloud-push] %s: could not record deleted bytes for billing", _service_id(source), exc_info=True
            )
    if identity is not None and not adopted:
        if foreign == 0:
            _write_marker(client, bucket, marker_key, identity[0])
            adopted = True
        elif log_once_per(f"cloud-push-foreign:{_service_id(source)}", interval_s=3600.0):
            logger.warning(
                "[cloud-push] %s: the cloud copy holds %d file(s) this catalog did not write (a copy from before "
                "a data loss?). They are kept and never deleted; run `restore-data` to adopt them.",
                _service_id(source),
                foreign,
            )
    return {
        "uploaded": uploaded,
        "uploaded_bytes": uploaded_bytes,
        "deleted": deleted,
        "deleted_bytes": deleted_bytes,
        "foreign": foreign,
        "adopted": adopted,
    }


def schedule_cloud_push(source: dict) -> None:
    """Run ``push_to_cloud`` on a per-service single-flight daemon thread; requests made meanwhile coalesce into one rerun."""
    dev_no_crons = os.environ.get("FLA_DEV_NO_CRONS", "").lower() in ("1", "true", "yes")
    if dev_no_crons or not source.get("bucket") or is_high_scale_mode(source):
        return
    service_id = _service_id(source)
    with _lock:
        if service_id in _running:
            _pending[service_id] = source
            return
        _running.add(service_id)
    threading.Thread(
        target=_push_worker, args=(service_id, source), name=f"cloud-push:{service_id}", daemon=True
    ).start()


def _push_worker(service_id: str, source: dict) -> None:
    from backend.utils.telemetry import process_context_scope

    while True:
        try:
            with process_context_scope("cron.cloud_push"):
                result = push_to_cloud(source)
            if result.get("uploaded") or result.get("deleted"):
                logger.info("[cloud-push] %s: %s", service_id, result)
        except Exception as exc:
            logger.warning("[cloud-push] %s: push to FOS failed (%s); the next commit retries", service_id, exc)
        with _lock:
            nxt = _pending.pop(service_id, None)
            if nxt is None:
                _running.discard(service_id)
                return
            source = nxt


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


def restore_admin_state(source: dict) -> int:
    """Re-save the saved views from the ``meta/admin_state.json`` snapshot; returns how many were restored.

    The snapshot is exported after every settings change, so after a full loss it is the only copy of the saved views
    (the audit log and custom fields in it are history and config, not restored). Views are keyed by id, so this is
    idempotent.
    """
    from types import SimpleNamespace

    from backend.core.duckdb import _get_fos_client
    from backend.core.metadata.base import get_admin_state_key, save_view

    if not source.get("bucket"):
        return 0
    try:
        body = json.loads(
            _get_fos_client(source).get_object(Bucket=source["bucket"], Key=get_admin_state_key(source))["Body"].read()
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return 0
        raise
    restored = 0
    for view in body.get("_views") or []:
        if not view.get("name"):
            continue
        save_view(
            _service_id(source),
            SimpleNamespace(
                **{
                    k: view.get(k)
                    for k in ("id", "name", "filters_json", "time_range_type", "start_time", "end_time", "page")
                }
            ),
        )
        restored += 1
    return restored


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
    remote.pop(key_root + _MARKER_NAME, None)
    for key, (size, _modified) in remote.items():
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
    identity = _catalog_identity(source)
    if identity is not None:
        _write_marker(client, bucket, key_root + _MARKER_NAME, identity[0])
    views = 0
    try:
        views = restore_admin_state(source)
        say(f"Restored {views} saved view(s) from the admin state snapshot")
    except Exception as exc:
        logger.warning("[restore] %s: could not restore saved views: %s", _service_id(source), exc)
    clear_source_caches(source.get("name", "default"))
    return {
        "downloaded": downloaded,
        "registered": registered,
        "remote_files": len(remote),
        "reindexed": reindexed,
        "views_restored": views,
    }
