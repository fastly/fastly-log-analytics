"""Service log and RUM reset/cleanup utilities."""

from __future__ import annotations

import gc
import logging
import os
import shutil
import time
from collections.abc import Callable, Generator, Iterator

import duckdb
import psycopg
from tenacity import Retrying, retry_if_result, wait_fixed

from backend import config as svcconfig
from backend.core.metadata.ingest_log import purge_evidence

logger = logging.getLogger(__name__)

_LOCK_TIMEOUT_S = 10.0
_DRAIN_TIMEOUT_S = 5.0
_DRAIN_GRACE_S = 5.0
_RUM_TABLES = ("client_vitals", "client_errors")
_RUM_CH_TABLES = ("rum_vitals_facts", "rum_error_facts", "rum_vitals_aggregates", "rum_error_aggregates")


def _purge_prefix(
    fos_client, bucket: str, prefix: str, delete_fn, *, exclude_prefix: str | None = None, label: str = "object(s)"
) -> Generator[dict, None, int]:
    paginator = fos_client.get_paginator("list_objects_v2")
    total_deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if page_keys := [
            obj["Key"]
            for obj in page.get("Contents", [])
            if not (exclude_prefix and obj["Key"].startswith(exclude_prefix))
        ]:
            total_deleted += delete_fn(fos_client, bucket, page_keys)
            yield {"type": "status", "message": f"Deleted {total_deleted:,} {label} so far..."}
    return total_deleted


def _remove_tree_with_progress(path: str, *, batch_size: int = 2000) -> Generator[dict, None, int]:
    removed = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            try:
                os.remove(os.path.join(root, name))
                removed += 1
                if removed % batch_size == 0:
                    yield {"type": "status", "message": f"Removed {removed:,} local cache files so far..."}
            except OSError:
                continue
        for name in dirs:
            try:
                os.rmdir(os.path.join(root, name))
            except OSError:
                continue
    try:
        os.rmdir(path)
    except OSError:
        pass
    return removed


def _wipe_ducklake_table_dirs(ducklake_root: str, tbl: str) -> Generator[dict]:
    if not ducklake_root.startswith(("s3://", "http://", "https://")):
        for sub in (os.path.join(ducklake_root, "main", tbl), os.path.join(ducklake_root, tbl)):
            if os.path.exists(sub):
                yield from _remove_tree_with_progress(sub)
                shutil.rmtree(sub, ignore_errors=True)


def _purge_ducklake_remote_dirs(
    fos_client, ducklake_root: str, tbl: str, source_bucket: str, delete_fn, source_prefix: str = ""
) -> Generator[dict, None, int]:
    """Delete a table's DuckLake data files from FOS: the live data (High-Scale ``s3://`` root) or the cloud copy of local data."""
    if ducklake_root.startswith("s3://"):
        bucket, _, key = ducklake_root[len("s3://") :].partition("/")
        if bucket != source_bucket:
            yield {
                "type": "status",
                "message": f"Skipping cloud DuckLake purge: data path bucket {bucket!r} is not this service's bucket.",
            }
            return 0
        base = f"{key.rstrip('/')}/" if key.strip("/") else ""
    else:
        bucket, prefix = source_bucket, source_prefix.strip("/")
        base = f"{prefix}/ducklake/" if prefix else "ducklake/"
    deleted = 0
    for sub in (f"{base}main/{tbl}/", f"{base}{tbl}/"):
        deleted += yield from _purge_prefix(fos_client, bucket, sub, delete_fn, label=f"object(s) under {sub}")
    return deleted


def _drain_and_wipe_db_files(db_path: str, pool_key: str, wipe_msg: str) -> Generator[dict, None, dict]:
    from backend.core import duckdb as _db
    from backend.core import duckdb_pool as _pool

    _db.set_recycle_barrier(db_path, True)
    retired = _pool.begin_drain_pools([pool_key])
    _pool.wait_pools_drained(retired, _DRAIN_TIMEOUT_S)
    gc.collect()
    deadline = time.monotonic() + _DRAIN_GRACE_S
    if _db.live_connection_count(db_path) > 0 and time.monotonic() < deadline:
        Retrying(
            retry=retry_if_result(lambda count: count > 0),
            stop=lambda _rs: time.monotonic() >= deadline,
            wait=wait_fixed(0.05),
            sleep=lambda s: time.sleep(s),
            retry_error_callback=lambda _rs: 0,
        )(lambda: (gc.collect(), _db.live_connection_count(db_path))[1])
    yield {"type": "status", "message": wipe_msg}
    for f in (db_path, db_path + ".wal", db_path + ".shm"):
        if os.path.exists(f):
            os.remove(f)
    _db.clear_initialization_state(db_path)
    return retired


def _reinit_ducklake_tables(
    con_source: dict, source: dict, service_id: str, tables: tuple[str | None, ...], fn_label: str
) -> None:
    from backend.core import duckdb as _db
    from backend.core import storage as storage_core
    from backend.core.storage._core import _ducklake_attach, ducklake_table_name, update_storage_view

    for t_name in tables:
        storage_core.init_storage_table(source, create=True, **({} if t_name is None else {"table_name": t_name}))
    con2 = _db.get_connection(con_source)
    try:
        try:
            if _ducklake_attach(con2, source, read_only=False):
                for t_name in tables:
                    con2.execute(f'DROP TABLE IF EXISTS lake."{ducklake_table_name(source, t_name or "logs")}"')
        except Exception as e:
            logger.debug("%s(%s): DuckLake table drop skipped: %s", fn_label, service_id, e)
        for t_name in tables:
            if t_name is None:
                update_storage_view(con2, source, force=True)
            else:
                update_storage_view(con2, source, force=True, target_table=t_name)
    finally:
        con2.close()


def _purge_dropped_catalog_rows(service_id: str, table_names: list[str]) -> int:
    """Delete the catalog rows of already-dropped DuckLake tables, atomically."""
    from backend.core.storage._core import _resolve_catalog_dsn

    if not (dsn := _resolve_catalog_dsn(service_id)):
        return 0
    deleted = 0
    with psycopg.connect(dsn.removeprefix("postgres:")) as pg:
        ids = [
            r[0]
            for r in pg.execute(
                "SELECT table_id FROM ducklake_table WHERE table_name = ANY(%s) AND end_snapshot IS NOT NULL",
                (table_names,),
            )
        ]
        if not ids:
            return 0
        children = [
            r[0]
            for r in pg.execute(
                "SELECT table_name FROM information_schema.columns WHERE table_schema = 'public' "
                "AND table_name LIKE 'ducklake\\_%' AND column_name = 'table_id' AND table_name <> 'ducklake_table'"
            )
        ]
        for child in children:
            deleted += pg.execute(f'DELETE FROM "{child}" WHERE table_id = ANY(%s)', (ids,)).rowcount
        deleted += pg.execute("DELETE FROM ducklake_table WHERE table_id = ANY(%s)", (ids,)).rowcount
    return deleted


def drop_service_ducklake_state(service_id: str) -> dict[str, int]:
    """Teardown helper: drop a service's DuckLake tables and delete its local DuckLake data files."""
    from backend.core import duckdb as _db
    from backend.core.storage._core import _ducklake_attach, _resolve_data_path, ducklake_table_name

    if not (source := _db.get_source_for_service(service_id)):
        return {"tables_dropped": 0, "local_dirs_removed": 0}
    tables = ("logs", *_RUM_TABLES)
    dropped, errors = 0, []
    con = duckdb.connect(":memory:")
    try:
        if not _ducklake_attach(con, source, read_only=False):
            raise RuntimeError(f"could not attach the DuckLake catalog for service {service_id!r}")
        for t_name in tables:
            try:
                con.execute(f'DROP TABLE IF EXISTS lake."{ducklake_table_name(source, t_name)}"')
                dropped += 1
            except Exception as exc:
                errors.append(f"{t_name}: {exc}")
    finally:
        con.close()
    if errors:
        raise RuntimeError(f"DuckLake table drop failed for service {service_id!r}: {'; '.join(errors)}")
    purged = _purge_dropped_catalog_rows(service_id, [ducklake_table_name(source, t) for t in tables])
    root = _resolve_data_path(source, service_id)
    removed = 0
    if not root.startswith(("s3://", "http://", "https://")):
        service_lake_dir = svcconfig.SERVICES_DATA_DIR / str(service_id) / "ducklake"
        if os.path.abspath(root) == os.path.abspath(service_lake_dir):
            if service_lake_dir.exists():
                shutil.rmtree(service_lake_dir, ignore_errors=True)
                removed += 1
            try:
                service_lake_dir.parent.rmdir()
            except OSError:
                pass
        else:
            for t_name in tables:
                tbl = ducklake_table_name(source, t_name)
                existing = [d for d in (os.path.join(root, "main", tbl), os.path.join(root, tbl)) if os.path.exists(d)]
                for _ in _wipe_ducklake_table_dirs(root, tbl):
                    pass
                removed += len(existing)
    return {"tables_dropped": dropped, "local_dirs_removed": removed, "catalog_rows_purged": purged}


def _purge_clickhouse_tables_gen(
    service_id: str, tables_and_clauses: list[tuple[str, str]], start_msg: str, done_prefix: str, err_label: str
) -> Generator[dict]:
    try:
        from backend.core.clickhouse import get_clickhouse_client

        yield {"type": "status", "message": start_msg}
        if client := get_clickhouse_client():
            for table, extra in tables_and_clauses:
                client.execute(
                    f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}}{extra} SETTINGS mutations_sync=2",
                    {"service_id": service_id},
                )
            yield {"type": "status", "message": f"{done_prefix} {len(tables_and_clauses)} ClickHouse table(s)."}
    except Exception as e:
        logger.error("Failed to purge %s: %s", err_label, e)
        yield {"type": "status", "message": f"Warning: Failed to purge {err_label}: {e}"}


def reset_service_logs(
    service_id: str,
    *,
    delete_raw_logs: bool = False,
    preserve_usage_history: bool = True,
    actor: str = "ui",
    reload_scheduler: Callable[[], None] | None = None,
) -> Iterator[dict]:
    from backend.core import duckdb as _db
    from backend.core import duckdb_pool as _pool
    from backend.core import metadata as metadata_db
    from backend.core.ingest import _delete_objects_robust
    from backend.core.metadata.base import is_ingested_files_dedup_active
    from backend.core.storage._core import (
        _get_service_lock,
        _resolve_data_path,
        clear_source_caches,
        ducklake_table_name,
    )

    if not (source := _db.get_source_for_service(service_id)):
        raise ValueError(f"No service found with id {service_id!r}")
    service_key = source.get("name", service_id)
    ducklake_root = _resolve_data_path(source, service_id)
    db_path = os.path.abspath(source.get("duckdb_path") or svcconfig.duckdb_path(service_id))
    lock = _get_service_lock(service_key)
    if not lock.acquire(timeout=_LOCK_TIMEOUT_S):
        raise RuntimeError("Another operation is in progress for this service. Please wait and try again.")
    paused_cron, original_cron_sync_enabled = False, True
    try:
        if metadata_db.cron_busy(service_id):
            raise RuntimeError(
                "Active sync or commit is currently running. Please wait for it to finish before deleting logs."
            )
        yield {"type": "status", "message": "Pausing background sync for this service..."}
        cfg = svcconfig.load_config(service_id) or {}
        original_cron_sync_enabled = cfg.setdefault("provisioning", {}).setdefault("cron_sync", {}).get("enabled", True)
        if original_cron_sync_enabled:

            def _pause_cron_sync(c: dict) -> None:
                nonlocal original_cron_sync_enabled
                fresh = c.setdefault("provisioning", {}).setdefault("cron_sync", {})
                original_cron_sync_enabled = fresh.get("enabled", True)
                fresh["enabled"] = False

            svcconfig.mutate_config(service_id, _pause_cron_sync)
            paused_cron = True
            if reload_scheduler:
                try:
                    reload_scheduler()
                except Exception as e:
                    logger.warning("reset_service_logs(%s): scheduler reload (pause) failed: %s", service_id, e)
        if delete_raw_logs and not is_ingested_files_dedup_active(service_id):
            yield {
                "type": "status",
                "message": (
                    "Warning: this source keeps raw logs after ingest (delete_after=False). "
                    "Deleting raw cloud logs now means the next full sync has no dedup record "
                    "left and will re-ingest this service's ENTIRE request/RUM history."
                ),
            }
        yield {"type": "status", "message": "Draining local database connections..."}
        retired_pools = {}
        try:
            retired_pools = yield from _drain_and_wipe_db_files(
                db_path, service_key, "Wiping local database and cache..."
            )
            if os.path.exists(cache_dir := _db._cache_dir(source)):
                removed = yield from _remove_tree_with_progress(cache_dir)
                if os.path.exists(cache_dir):
                    shutil.rmtree(cache_dir, ignore_errors=True)
                yield {"type": "status", "message": f"Removed {removed:,} local cache file(s)."}
            yield from _wipe_ducklake_table_dirs(ducklake_root, ducklake_table_name(source, "logs"))
        finally:
            _pool.end_drain_pools(retired_pools or [service_key])
            _db.set_recycle_barrier(db_path, False)

        yield {"type": "status", "message": "Purging cloud DuckLake log data..."}
        fos, bucket, prefix = _db._get_fos_client(source), source.get("bucket", ""), source.get("prefix", "")
        pfx = f"{prefix}/" if prefix else ""
        deleted = yield from _purge_ducklake_remote_dirs(
            fos,
            ducklake_root,
            ducklake_table_name(source, "logs"),
            bucket,
            _delete_objects_robust,
            prefix,
        )
        if deleted:
            yield {"type": "status", "message": f"Deleted {deleted:,} cloud DuckLake data object(s)."}
        if deleted_errors := (
            yield from _purge_prefix(
                fos, bucket, f"{pfx}errors/", _delete_objects_robust, label="quarantined object(s) under errors/"
            )
        ):
            yield {"type": "status", "message": f"Deleted {deleted_errors:,} quarantined object(s) under errors/."}
        if delete_raw_logs:
            deleted_raw = yield from _purge_prefix(
                fos,
                bucket,
                f"{pfx}raw/request/",
                _delete_objects_robust,
                label="request raw object(s) under raw/request/",
            )
            yield {"type": "status", "message": f"Deleted {deleted_raw:,} request raw object(s)."}

        yield {"type": "status", "message": "Purging local quarantine evidence..."}
        purge_evidence(service_id)
        yield {"type": "status", "message": "Truncating local ingestion indexes..."}
        con = metadata_db.get_con(service_id)
        for sql in (
            "DELETE FROM ingested_files WHERE source_name = ? AND table_name = 'logs'",
            "DELETE FROM ingest_in_flight WHERE source_name = ? AND table_name = 'logs'",
            "DELETE FROM quarantined_files WHERE source_name = ?",
            "DELETE FROM ingested_files_summary WHERE source_name = ?",
        ):
            con.execute(sql, (service_id,))
        con.execute(
            "DELETE FROM ingest_ledger WHERE service_id = ? AND object_key NOT LIKE ? AND object_key NOT LIKE ? AND object_key NOT LIKE ?",
            (service_id, "%/raw/rum/%", "raw/rum/%", "rum/%"),
        )
        con.commit()
        if con.execute("SELECT 1 FROM ingested_files WHERE source_name = ? LIMIT 1", (service_id,)).fetchone():
            metadata_db.recompute_ingested_files_summary(con, service_id)
        if not preserve_usage_history:
            metadata_db.clear_usage_log(service_id)
            yield {"type": "status", "message": "Cleared usage-log (Class A/B) history."}
        svcconfig.update_status(service_id, {"local_rows": 0})

        yield {"type": "status", "message": "Re-initializing storage catalog..."}
        clear_source_caches(service_key)
        _reinit_ducklake_tables(source, source, service_id, (None,), "reset_service_logs")
        metadata_db.record_audit(
            service_id,
            "logs_reset",
            {"delete_raw_logs": delete_raw_logs, "preserve_usage_history": preserve_usage_history},
            actor=actor,
        )
        if svcconfig.is_high_scale_mode(source):
            from backend.core.clickhouse import CLICKHOUSE_HIGH_SCALE_TABLES

            req_specs = [
                (t, " AND domain != 'rum'" if t == "high_scale_batch_publications" else "")
                for t in CLICKHOUSE_HIGH_SCALE_TABLES
                if t not in _RUM_CH_TABLES
            ]
            yield from _purge_clickhouse_tables_gen(
                service_id,
                req_specs,
                "Purging ClickHouse high-scale tables...",
                "Cleared data from",
                "ClickHouse tables",
            )
        yield {
            "type": "done",
            "message": "Log history deleted. New logs from the live edge will begin populating immediately.",
        }
    finally:
        if paused_cron:
            try:

                def _resume_cron_sync(c: dict) -> None:
                    c.setdefault("provisioning", {}).setdefault("cron_sync", {})["enabled"] = original_cron_sync_enabled

                svcconfig.mutate_config(service_id, _resume_cron_sync)
                if reload_scheduler:
                    reload_scheduler()
            except Exception as e:
                logger.error("reset_service_logs(%s): failed to resume scheduler: %s", service_id, e)
        try:
            lock.release()
        except RuntimeError:
            pass


def _pause_rum_flags(cfg: dict) -> tuple[bool | None, bool] | None:
    rum_cfg = cfg.setdefault("rum", {})
    original_rum = rum_cfg.get("enabled")
    original_root = bool(cfg.get("rum_enabled", False))
    if not (original_rum or original_root):
        return None
    rum_cfg["enabled"] = False
    cfg["rum_enabled"] = False
    return original_rum, original_root


def _restore_rum_flags(cfg: dict, paused: tuple[bool | None, bool]) -> None:
    original_rum, original_root = paused
    rum_cfg = cfg.setdefault("rum", {})
    if original_rum is None:
        rum_cfg.pop("enabled", None)
    else:
        rum_cfg["enabled"] = original_rum
    cfg["rum_enabled"] = original_root


def reset_service_rum(
    service_id: str,
    *,
    delete_raw_logs: bool = False,
    actor: str = "ui",
    reload_scheduler: Callable[[], None] | None = None,
) -> Iterator[dict]:
    from backend.core import duckdb as _db
    from backend.core import duckdb_pool as _pool
    from backend.core import metadata as metadata_db
    from backend.core.ingest import _delete_objects_robust
    from backend.core.storage._core import (
        _get_service_lock,
        _resolve_data_path,
        clear_source_caches,
        ducklake_table_name,
    )

    if not (source := _db.get_source_for_service(service_id)):
        raise ValueError(f"No service found with id {service_id!r}")
    service_key, rum_source = source.get("name", service_id), _db.rum_source_for(source)
    ducklake_root = _resolve_data_path(source, service_id)
    rum_db_path, rum_service_key = os.path.abspath(rum_source["duckdb_path"]), f"{service_key}::rum"
    lock = _get_service_lock(service_key)
    if not lock.acquire(timeout=_LOCK_TIMEOUT_S):
        raise RuntimeError("Another operation is in progress for this service. Please wait and try again.")
    paused_cron, paused_flags = False, None
    try:
        if metadata_db.cron_busy(service_id):
            raise RuntimeError(
                "Active RUM sync or commit is currently running. Please wait for it to finish before deleting RUM logs."
            )
        yield {"type": "status", "message": "Pausing background RUM sync for this service..."}
        if _pause_rum_flags(svcconfig.load_config(service_id) or {}) is not None:

            def _pause_rum(c: dict) -> None:
                nonlocal paused_flags
                paused_flags = _pause_rum_flags(c)

            svcconfig.mutate_config(service_id, _pause_rum)
            paused_cron = True
            if reload_scheduler:
                try:
                    reload_scheduler()
                except Exception as e:
                    logger.warning("reset_service_rum(%s): scheduler reload (pause) failed: %s", service_id, e)

        yield {"type": "status", "message": "Draining local RUM database connections..."}
        retired_rum_pools = {}
        try:
            retired_rum_pools = yield from _drain_and_wipe_db_files(
                rum_db_path, rum_service_key, "Wiping local RUM database..."
            )
            yield {"type": "status", "message": "Wiping local RUM cache & buffer..."}
            rum_cache_dir = _db._cache_dir(source)
            for t_name in _RUM_TABLES:
                if os.path.exists(data_dir := os.path.join(rum_cache_dir, f"data_{t_name}")):
                    removed = yield from _remove_tree_with_progress(data_dir)
                    shutil.rmtree(data_dir, ignore_errors=True)
                    yield {"type": "status", "message": f"Removed {removed:,} local cache file(s) for RUM {t_name}."}
                if os.path.exists(buf_dir := os.path.join(rum_cache_dir, "buffer", t_name)):
                    shutil.rmtree(buf_dir, ignore_errors=True)
                yield from _wipe_ducklake_table_dirs(ducklake_root, ducklake_table_name(source, t_name))
        finally:
            _pool.end_drain_pools(retired_rum_pools or [rum_service_key])
            _db.set_recycle_barrier(rum_db_path, False)

        yield {"type": "status", "message": "Purging cloud DuckLake RUM data..."}
        fos, bucket, prefix = _db._get_fos_client(source), source.get("bucket", ""), source.get("prefix", "")
        pfx = f"{prefix}/" if prefix else ""
        for t_name in _RUM_TABLES:
            deleted = yield from _purge_ducklake_remote_dirs(
                fos, ducklake_root, ducklake_table_name(source, t_name), bucket, _delete_objects_robust, prefix
            )
            if deleted:
                yield {"type": "status", "message": f"Deleted {deleted:,} cloud object(s) for RUM {t_name}."}
        if delete_raw_logs:
            deleted_rum = yield from _purge_prefix(
                fos, bucket, f"{pfx}raw/rum/", _delete_objects_robust, label="RUM raw object(s) under raw/rum/"
            )
            yield {"type": "status", "message": f"Deleted {deleted_rum:,} RUM raw object(s)."}

        yield {"type": "status", "message": "Truncating local ingestion indexes for RUM..."}
        con = metadata_db.get_con(service_id)
        for tbl_name in ("ingested_files", "ingest_in_flight"):
            con.execute(
                f"DELETE FROM {tbl_name} WHERE source_name = ? AND table_name IN ('client_vitals', 'client_errors')",
                (service_id,),
            )
        con.execute(
            "DELETE FROM ingest_ledger WHERE service_id = ? AND (object_key LIKE ? OR object_key LIKE ? OR object_key LIKE ?)",
            (service_id, "%/raw/rum/%", "raw/rum/%", "rum/%"),
        )
        con.commit()
        if con.execute("SELECT 1 FROM ingested_files WHERE source_name = ? LIMIT 1", (service_id,)).fetchone():
            metadata_db.recompute_ingested_files_summary(con, service_id)
        else:
            con.execute("DELETE FROM ingested_files_summary WHERE source_name = ?", (service_id,))
            con.commit()
        svcconfig.update_status(service_id, {"rum_total_events": 0, "rum_vitals_count": 0, "rum_errors_count": 0})

        yield {"type": "status", "message": "Re-initializing RUM storage catalogs..."}
        clear_source_caches(rum_service_key)
        _reinit_ducklake_tables(rum_source, source, service_id, _RUM_TABLES, "reset_service_rum")
        metadata_db.record_audit(service_id, "rum_reset", {"delete_raw_logs": delete_raw_logs}, actor=actor)
        if svcconfig.is_high_scale_mode(source):
            rum_specs = [*((t, "") for t in _RUM_CH_TABLES), ("high_scale_batch_publications", " AND domain='rum'")]
            yield from _purge_clickhouse_tables_gen(
                service_id,
                rum_specs,
                "Purging ClickHouse high-scale RUM tables...",
                "Cleared RUM data from",
                "ClickHouse RUM tables",
            )
        yield {
            "type": "done",
            "message": "RUM history deleted. New RUM beacons from the live edge will populate on next sync.",
        }
    finally:
        if paused_cron:
            try:
                if paused_flags is not None:
                    svcconfig.mutate_config(service_id, lambda c: _restore_rum_flags(c, paused_flags))
                if reload_scheduler:
                    reload_scheduler()
            except Exception as e:
                logger.error("reset_service_rum(%s): failed to resume scheduler: %s", service_id, e)
        try:
            lock.release()
        except RuntimeError:
            pass
