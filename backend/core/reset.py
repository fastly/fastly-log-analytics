"""Project Log Reset — surgically wipe one service's log data back to a
0-state while preserving its configuration.

Wipes: the cloud Iceberg log table (``iceberg/`` except ``iceberg/meta/``),
quarantined-file cloud copies (``errors/``), the local DuckDB analytical
file + cache dir, and the SQLite ingestion ledgers (``ingested_files``,
``ingested_files_summary``, ``ingest_in_flight``, ``committed_buffers``,
``local_compacted_files``, ``quarantined_files``).

Preserves: ``sources``, ``views``, ``alerts``, ``audit_logs``,
``scoring_labels``, ``scoring_audit``, ``cron_runs``, ``slow_queries``,
``asn_names``, and everything under ``iceberg/meta/`` (admin_state.json,
scoring_matrix.json). Raw logs under ``raw/`` are left alone by default —
see the re-ingestion-storm warning below.

See ``local-docs/log_reset_design_plan.md`` for the full design rationale.
Written as a generator so both the CLI and the SSE route can drive the
same progress stream.
"""

from __future__ import annotations

import gc
import logging
import os
import shutil
import time
from collections.abc import Callable, Generator, Iterator

logger = logging.getLogger(__name__)

_LOCK_TIMEOUT_S = 10.0
_DRAIN_TIMEOUT_S = 5.0
_DRAIN_GRACE_S = 5.0


def _purge_prefix(
    fos_client,
    bucket: str,
    prefix: str,
    delete_fn,
    *,
    exclude_prefix: str | None = None,
    label: str = "object(s)",
) -> Generator[dict, None, int]:
    """List + delete every object under ``prefix``, one listed page at a
    time (rather than buffering the whole prefix into memory before
    deleting anything) so a large prefix — an Iceberg table with tens of
    thousands of parquet/manifest files — reports live progress instead
    of going silent for minutes. ``exclude_prefix`` skips keys that start
    with it — used to carve ``iceberg/meta/`` (config backup) out of an
    ``iceberg/`` purge.

    Yields ``{"type": "status", ...}`` progress events; returns the total
    deleted count (retrieve via ``deleted = yield from _purge_prefix(...)``).
    """
    paginator = fos_client.get_paginator("list_objects_v2")
    total_deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        page_keys = [
            obj["Key"]
            for obj in page.get("Contents", [])
            if not (exclude_prefix and obj["Key"].startswith(exclude_prefix))
        ]
        if page_keys:
            total_deleted += delete_fn(fos_client, bucket, page_keys)
            yield {"type": "status", "message": f"Deleted {total_deleted:,} {label} so far..."}
    return total_deleted


def _remove_tree_with_progress(path: str, *, batch_size: int = 2000) -> Generator[dict, None, int]:
    """Recursively delete ``path``, yielding a status event every
    ``batch_size`` files removed so a cache dir with 100k+ small parquet
    files shows live progress instead of one silent multi-minute step.

    Best-effort like the ``shutil.rmtree`` this replaces: a concurrent
    writer (an in-flight cron tick APScheduler couldn't cancel — see the
    pause-scheduler comment above) can race a handful of files into
    existence mid-walk; those are skipped, not retried, matching
    ``_walk_dir_stats``'s tolerance for the same race. Returns the number
    of files removed.
    """
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


def reset_service_logs(
    service_id: str,
    *,
    delete_raw_logs: bool = False,
    preserve_usage_history: bool = True,
    actor: str = "ui",
    reload_scheduler: Callable[[], None] | None = None,
) -> Iterator[dict]:
    """Wipe ``service_id``'s log data back to a 0-state. Yields progress events.

    Event shapes: ``{"type": "status", "message": str}``,
    ``{"type": "done", "message": str}``. Raises on precondition failure
    (busy lock, active sync) or on an unrecoverable step — callers should
    catch and surface as an ``{"type": "error", ...}`` event (see the SSE
    route / CLI handler), matching the ``metadata_cleanup_now`` convention.

    ``reload_scheduler``: called (if provided) right after flipping the
    ``cron_sync.enabled`` config flag, both to pause and to resume. Passed
    in rather than imported here — ``backend.cron.scheduler`` transitively
    imports ``backend.routers.admin`` (via ``cron.jobs.sync``), and
    ``backend.core`` must not depend on ``backend.routers`` (import-linter
    contract). Callers (the SSE route, the CLI) sit above that boundary and
    can pass ``lambda: get_scheduler().reload()``.
    """
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core import duckdb_pool as _pool
    from backend.core import metadata as metadata_db
    from backend.core.iceberg import _core as iceberg_core
    from backend.core.iceberg.view import _get_service_lock, clear_source_caches, update_iceberg_view
    from backend.core.ingest import _delete_objects_robust
    from backend.core.metadata.reconciliation import is_ingested_files_dedup_active

    source = _db.get_source_for_service(service_id)
    if not source:
        raise ValueError(f"No service found with id {service_id!r}")

    service_key = source.get("name", service_id)
    db_path = os.path.abspath(source.get("duckdb_path") or svcconfig.duckdb_path(service_id))

    lock = _get_service_lock(service_key)
    if not lock.acquire(timeout=_LOCK_TIMEOUT_S):
        raise RuntimeError("Another operation is in progress for this service. Please wait and try again.")

    paused_cron = False
    original_cron_sync_enabled = True

    try:
        if metadata_db.cron_busy(service_id):
            raise RuntimeError(
                "Active sync or commit is currently running. Please wait for it to finish before deleting logs."
            )

        # Pause the service's own sync/commit ticks so nothing writes into the
        # cache dir or FOS while we're wiping it. Toggle the existing
        # cron_sync.enabled config flag (the same one `_sync_jobs` already
        # reads) rather than deleting the service's config file — the
        # teardown route's unschedule-via-delete trick doesn't apply here
        # because the service must keep existing after a reset.
        yield {"type": "status", "message": "Pausing background sync for this service..."}
        cfg = svcconfig.load_config(service_id) or {}
        prov = cfg.setdefault("provisioning", {})
        cron_sync_cfg = prov.setdefault("cron_sync", {})
        original_cron_sync_enabled = cron_sync_cfg.get("enabled", True)
        if original_cron_sync_enabled:
            cron_sync_cfg["enabled"] = False
            svcconfig.save_config(service_id, cfg)
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
                    "left and will re-ingest this service's ENTIRE history from raw/."
                ),
            }

        # Drain the DuckDB connection pool and barrier new connections while
        # we delete the local database file. Mirrors duckdb_recycle's
        # `_recycle_db_path` sequence. The barrier + drain are only needed
        # for this window (deleting the live file out from under a reader) —
        # correctness of the catalog rebuild below is already guaranteed by
        # the `_get_service_lock` held for this whole function, so the
        # barrier is cleared as soon as the file is gone rather than held
        # through re-init (which would make our own re-init's
        # `get_connection` block on the barrier IT raised).
        yield {"type": "status", "message": "Draining local database connections..."}
        _db.set_recycle_barrier(db_path, True)
        try:
            _pool.begin_drain_pools([service_key])
            _pool.wait_pools_drained([service_key], _DRAIN_TIMEOUT_S)
            gc.collect()
            deadline = time.monotonic() + _DRAIN_GRACE_S
            while _db.live_connection_count(db_path) > 0 and time.monotonic() < deadline:
                time.sleep(0.05)
                gc.collect()

            yield {"type": "status", "message": "Wiping local database and cache..."}
            for f in (db_path, db_path + ".wal", db_path + ".shm"):
                if os.path.exists(f):
                    os.remove(f)
            _db.clear_initialization_state(db_path)

            cache_dir = _db._cache_dir(source)
            if os.path.exists(cache_dir):
                removed = yield from _remove_tree_with_progress(cache_dir)
                if os.path.exists(cache_dir):
                    # Best-effort sweep for anything the per-file walk
                    # couldn't remove (e.g. a directory that raced a
                    # concurrent writer into non-empty) — matches the old
                    # implementation's tolerance for the same race.
                    shutil.rmtree(cache_dir, ignore_errors=True)
                yield {"type": "status", "message": f"Removed {removed:,} local cache file(s)."}
        finally:
            _pool.end_drain_pools([service_key])
            _db.set_recycle_barrier(db_path, False)

        yield {"type": "status", "message": "Purging cloud Iceberg log table..."}
        fos = _db._get_fos_client(source)
        bucket = source.get("bucket", "")
        prefix = source.get("prefix", "")
        iceberg_prefix = f"{prefix}/iceberg/default/logs/" if prefix else "iceberg/default/logs/"
        deleted = yield from _purge_prefix(
            fos,
            bucket,
            iceberg_prefix,
            _delete_objects_robust,
            label="object(s) under iceberg/default/logs/",
        )
        yield {"type": "status", "message": f"Deleted {deleted:,} object(s) under iceberg/default/logs/."}

        errors_prefix = f"{prefix}/errors/" if prefix else "errors/"
        deleted_errors = yield from _purge_prefix(
            fos, bucket, errors_prefix, _delete_objects_robust, label="quarantined object(s) under errors/"
        )
        if deleted_errors:
            yield {"type": "status", "message": f"Deleted {deleted_errors:,} quarantined object(s) under errors/."}

        if delete_raw_logs:
            raw_prefix = f"{prefix}/raw/" if prefix else "raw/"
            deleted_raw = yield from _purge_prefix(
                fos, bucket, raw_prefix, _delete_objects_robust, label="raw object(s) under raw/"
            )
            yield {"type": "status", "message": f"Deleted {deleted_raw:,} raw object(s) under raw/."}

        yield {"type": "status", "message": "Truncating local ingestion indexes..."}
        con = metadata_db.get_con(service_id)
        con.execute("DELETE FROM ingested_files WHERE source_name = ? AND table_name = 'logs'", (service_id,))
        con.execute("DELETE FROM ingest_in_flight WHERE source_name = ? AND table_name = 'logs'", (service_id,))
        con.execute(
            "DELETE FROM committed_buffers WHERE buffer_filename NOT LIKE 'client_vitals%' AND buffer_filename NOT LIKE 'client_errors%'"
        )
        con.execute(
            "DELETE FROM local_compacted_files WHERE file_name NOT LIKE 'client_vitals%' AND file_name NOT LIKE 'client_errors%'"
        )
        con.execute("DELETE FROM quarantined_files WHERE source_name = ?", (service_id,))
        con.execute("DELETE FROM ingested_files_summary WHERE source_name = ?", (service_id,))
        con.commit()

        if not preserve_usage_history:
            metadata_db.clear_usage_log(service_id)
            yield {"type": "status", "message": "Cleared usage-log (Class A/B) history."}

        svcconfig.update_status(service_id, {"local_rows": 0})

        yield {"type": "status", "message": "Re-initializing storage catalog..."}
        clear_source_caches(service_key)
        iceberg_core.init_iceberg_table(source, create=True)
        con2 = _db.get_connection(source)
        try:
            update_iceberg_view(con2, source, force=True)
        finally:
            con2.close()

        metadata_db.record_audit(
            service_id,
            "logs_reset",
            {"delete_raw_logs": delete_raw_logs, "preserve_usage_history": preserve_usage_history},
            actor=actor,
        )

        yield {
            "type": "done",
            "message": "Log history deleted. New logs from the live edge will begin populating immediately.",
        }
    finally:
        if paused_cron:
            try:
                cfg = svcconfig.load_config(service_id) or {}
                prov = cfg.setdefault("provisioning", {})
                cron_sync_cfg = prov.setdefault("cron_sync", {})
                cron_sync_cfg["enabled"] = original_cron_sync_enabled
                svcconfig.save_config(service_id, cfg)
                if reload_scheduler:
                    reload_scheduler()
            except Exception as e:
                logger.error("reset_service_logs(%s): failed to resume scheduler: %s", service_id, e)
        try:
            lock.release()
        except RuntimeError:
            pass


def reset_service_rum(
    service_id: str,
    *,
    delete_raw_logs: bool = False,
    actor: str = "ui",
    reload_scheduler: Callable[[], None] | None = None,
) -> Iterator[dict]:
    """Wipe ``service_id``'s RUM telemetry data back to a 0-state while preserving config.

    Yields progress events matching the reset_service_logs contract.
    """
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core import duckdb_pool as _pool
    from backend.core import metadata as metadata_db
    from backend.core.iceberg import _core as iceberg_core
    from backend.core.iceberg.view import _get_service_lock, clear_source_caches, update_iceberg_view
    from backend.core.ingest import _delete_objects_robust

    source = _db.get_source_for_service(service_id)
    if not source:
        raise ValueError(f"No service found with id {service_id!r}")

    service_key = source.get("name", service_id)
    rum_source = _db.rum_source_for(source)
    rum_db_path = os.path.abspath(rum_source["duckdb_path"])
    rum_service_key = f"{service_key}::rum"

    lock = _get_service_lock(service_key)
    if not lock.acquire(timeout=_LOCK_TIMEOUT_S):
        raise RuntimeError("Another operation is in progress for this service. Please wait and try again.")

    paused_cron = False
    original_rum_enabled = False
    original_rum_enabled_root = False

    try:
        if metadata_db.cron_busy(service_id):
            raise RuntimeError(
                "Active RUM sync or commit is currently running. Please wait for it to finish before deleting RUM logs."
            )

        # Pause background RUM sync/commit ticks
        yield {"type": "status", "message": "Pausing background RUM sync for this service..."}
        cfg = svcconfig.load_config(service_id) or {}
        rum_cfg = cfg.setdefault("rum", {})
        original_rum_enabled = rum_cfg.get("enabled", False)
        original_rum_enabled_root = cfg.get("rum_enabled", False)

        if original_rum_enabled or original_rum_enabled_root:
            rum_cfg["enabled"] = False
            cfg["rum_enabled"] = False
            svcconfig.save_config(service_id, cfg)
            paused_cron = True
            if reload_scheduler:
                try:
                    reload_scheduler()
                except Exception as e:
                    logger.warning("reset_service_rum(%s): scheduler reload (pause) failed: %s", service_id, e)

        # Drain the RUM DuckDB connection pool and barrier connections
        yield {"type": "status", "message": "Draining local RUM database connections..."}
        _db.set_recycle_barrier(rum_db_path, True)
        try:
            _pool.begin_drain_pools([rum_service_key])
            _pool.wait_pools_drained([rum_service_key], _DRAIN_TIMEOUT_S)
            gc.collect()
            deadline = time.monotonic() + _DRAIN_GRACE_S
            while _db.live_connection_count(rum_db_path) > 0 and time.monotonic() < deadline:
                time.sleep(0.05)
                gc.collect()

            yield {"type": "status", "message": "Wiping local RUM database..."}
            for f in (rum_db_path, rum_db_path + ".wal", rum_db_path + ".shm"):
                if os.path.exists(f):
                    os.remove(f)
            _db.clear_initialization_state(rum_db_path)

            yield {"type": "status", "message": "Wiping local RUM cache & buffer..."}
            rum_cache_dir = _db._cache_dir(source)
            for t_name in ("client_vitals", "client_errors"):
                # Clean up local cache directory
                data_dir = os.path.join(rum_cache_dir, f"data_{t_name}")
                if os.path.exists(data_dir):
                    removed = yield from _remove_tree_with_progress(data_dir)
                    shutil.rmtree(data_dir, ignore_errors=True)
                    yield {"type": "status", "message": f"Removed {removed:,} local cache file(s) for RUM {t_name}."}

                # Clean up local write buffer directory
                buf_dir = os.path.join(rum_cache_dir, "buffer", t_name)
                if os.path.exists(buf_dir):
                    shutil.rmtree(buf_dir, ignore_errors=True)
        finally:
            _pool.end_drain_pools([rum_service_key])
            _db.set_recycle_barrier(rum_db_path, False)

        yield {"type": "status", "message": "Purging cloud Iceberg RUM tables..."}
        fos = _db._get_fos_client(source)
        bucket = source.get("bucket", "")
        prefix = source.get("prefix", "")
        for t_name in ("client_vitals", "client_errors"):
            iceberg_prefix = f"{prefix}/iceberg/default/{t_name}/" if prefix else f"iceberg/default/{t_name}/"
            deleted = yield from _purge_prefix(
                fos,
                bucket,
                iceberg_prefix,
                _delete_objects_robust,
                label=f"object(s) under iceberg/default/{t_name}/",
            )
            yield {"type": "status", "message": f"Deleted {deleted:,} cloud object(s) for RUM {t_name}."}

        if delete_raw_logs:
            # We don't have distinct raw RUM log prefixes; RUM is usually combined in raw or disjoint.
            # If RUM-specific raw prefix existed, we'd delete it, but by default we preserve raw.
            pass

        yield {"type": "status", "message": "Truncating local ingestion indexes for RUM..."}
        con = metadata_db.get_con(service_id)
        con.execute(
            "DELETE FROM ingested_files WHERE source_name = ? AND table_name IN ('client_vitals', 'client_errors')",
            (service_id,),
        )
        con.execute(
            "DELETE FROM ingest_in_flight WHERE source_name = ? AND table_name IN ('client_vitals', 'client_errors')",
            (service_id,),
        )
        con.execute(
            "DELETE FROM committed_buffers WHERE buffer_filename LIKE 'client_vitals%' OR buffer_filename LIKE 'client_errors%'"
        )
        con.execute(
            "DELETE FROM local_compacted_files WHERE file_name LIKE 'client_vitals%' OR file_name LIKE 'client_errors%'"
        )
        con.commit()

        metadata_db.recompute_ingested_files_summary(con, service_id)

        yield {"type": "status", "message": "Re-initializing RUM storage catalogs..."}
        clear_source_caches(rum_service_key)
        for t_name in ("client_vitals", "client_errors"):
            iceberg_core.init_iceberg_table(source, create=True, table_name=t_name)

        # Pre-warm/initialize views on a standard connection
        con2 = _db.get_connection(rum_source)
        try:
            update_iceberg_view(con2, source, force=True, target_table="client_vitals")
            update_iceberg_view(con2, source, force=True, target_table="client_errors")
        finally:
            con2.close()

        metadata_db.record_audit(
            service_id,
            "rum_reset",
            {"delete_raw_logs": delete_raw_logs},
            actor=actor,
        )

        yield {
            "type": "done",
            "message": "RUM history deleted. New RUM beacons from the live edge will populate on next sync.",
        }
    finally:
        if paused_cron:
            try:
                cfg = svcconfig.load_config(service_id) or {}
                rum_cfg = cfg.setdefault("rum", {})
                rum_cfg["enabled"] = original_rum_enabled
                cfg["rum_enabled"] = original_rum_enabled_root
                svcconfig.save_config(service_id, cfg)
                if reload_scheduler:
                    reload_scheduler()
            except Exception as e:
                logger.error("reset_service_rum(%s): failed to resume scheduler: %s", service_id, e)
        try:
            lock.release()
        except RuntimeError:
            pass
