"""Service log and RUM reset/cleanup utilities."""

from __future__ import annotations

import logging
import os
import shutil
import threading
from collections.abc import Callable, Generator, Iterator

from backend import config as svcconfig
from backend.core.metadata.ingest_log import purge_evidence

logger = logging.getLogger(__name__)

_LOCK_TIMEOUT_S = 10.0
_RUM_CH_TABLES = ("rum_vitals_facts", "rum_error_facts", "rum_vitals_aggregates", "rum_error_aggregates")

_reset_locks: dict[str, threading.Lock] = {}
_reset_locks_guard = threading.Lock()


def _get_service_lock(service_key: str) -> threading.Lock:
    with _reset_locks_guard:
        lock = _reset_locks.get(service_key)
        if lock is None:
            lock = threading.Lock()
            _reset_locks[service_key] = lock
        return lock


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
        raise RuntimeError(f"Failed to purge {err_label}: {e}") from e


def reset_service_logs(
    service_id: str,
    *,
    delete_raw_logs: bool = False,
    preserve_usage_history: bool = True,
    actor: str = "ui",
    reload_scheduler: Callable[[], None] | None = None,
) -> Iterator[dict]:
    from backend.core import fos_client as _db
    from backend.core import metadata as metadata_db
    from backend.core.fos_client import _delete_objects_robust

    if not (source := _db.get_source_for_service(service_id)):
        raise ValueError(f"No service found with id {service_id!r}")
    service_key = source.get("name", service_id)
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

        if os.path.exists(cache_dir := _db._cache_dir(source)):
            removed = yield from _remove_tree_with_progress(cache_dir)
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir, ignore_errors=True)
            yield {"type": "status", "message": f"Removed {removed:,} local cache file(s)."}

        fos, bucket, prefix = _db._get_fos_client(source), source.get("bucket", ""), source.get("prefix", "")
        pfx = f"{prefix}/" if prefix else ""
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
            deleted_archive = yield from _purge_prefix(
                fos,
                bucket,
                f"{pfx}archive/high-scale/",
                _delete_objects_robust,
                label="high-scale archive object(s) under archive/high-scale/",
            )
            if deleted_archive:
                yield {"type": "status", "message": f"Deleted {deleted_archive:,} high-scale archive object(s)."}

        yield {"type": "status", "message": "Purging local quarantine evidence..."}
        purge_evidence(service_id)
        yield {"type": "status", "message": "Truncating local ingestion indexes..."}
        con = metadata_db.get_con(service_id)
        for sql in (
            "DELETE FROM ingested_files WHERE source_name = ? AND table_name = 'logs'",
            "DELETE FROM quarantined_files WHERE source_name = ?",
            "DELETE FROM ingested_files_summary WHERE source_name = ?",
        ):
            con.execute(sql, (service_id,))
        con.execute(
            "DELETE FROM ingest_ledger WHERE service_id = ? AND object_key NOT LIKE ? AND object_key NOT LIKE ? AND object_key NOT LIKE ?",
            (service_id, "%/raw/rum/%", "raw/rum/%", "rum/%"),
        )
        con.commit()
        if not preserve_usage_history:
            metadata_db.clear_usage_log(service_id)
            yield {"type": "status", "message": "Cleared usage-log (Class A/B) history."}
        svcconfig.update_status(service_id, {"local_rows": 0})

        metadata_db.record_audit(
            service_id,
            "logs_reset",
            {"delete_raw_logs": delete_raw_logs, "preserve_usage_history": preserve_usage_history},
            actor=actor,
        )
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
    delete_fos: bool = False,
    actor: str = "ui",
    reload_scheduler: Callable[[], None] | None = None,
) -> Iterator[dict]:
    from backend.core import fos_client as _db
    from backend.core import metadata as metadata_db
    from backend.core.fos_client import _delete_objects_robust

    if not (source := _db.get_source_for_service(service_id)):
        raise ValueError(f"No service found with id {service_id!r}")
    service_key = source.get("name", service_id)
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

        if delete_raw_logs or delete_fos:
            fos, bucket, prefix = _db._get_fos_client(source), source.get("bucket", ""), source.get("prefix", "")
            pfx = f"{prefix}/" if prefix else ""
            deleted_rum = yield from _purge_prefix(
                fos, bucket, f"{pfx}raw/rum/", _delete_objects_robust, label="RUM raw object(s) under raw/rum/"
            )
            yield {"type": "status", "message": f"Deleted {deleted_rum:,} RUM raw object(s)."}

        yield {"type": "status", "message": "Truncating local ingestion indexes for RUM..."}
        con = metadata_db.get_con(service_id)
        con.execute(
            "DELETE FROM ingested_files WHERE source_name = ? AND table_name IN ('client_vitals', 'client_errors')",
            (service_id,),
        )
        con.execute(
            "DELETE FROM ingest_ledger WHERE service_id = ? AND (object_key LIKE ? OR object_key LIKE ? OR object_key LIKE ?)",
            (service_id, "%/raw/rum/%", "raw/rum/%", "rum/%"),
        )
        con.commit()
        if not con.execute("SELECT 1 FROM ingested_files WHERE source_name = ? LIMIT 1", (service_id,)).fetchone():
            con.execute("DELETE FROM ingested_files_summary WHERE source_name = ?", (service_id,))
            con.commit()
        svcconfig.update_status(service_id, {"rum_total_events": 0, "rum_vitals_count": 0, "rum_errors_count": 0})

        metadata_db.record_audit(service_id, "rum_reset", {"delete_raw_logs": delete_raw_logs}, actor=actor)
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
