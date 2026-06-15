"""Commit cron — drains the local buffer to the shared Iceberg table.

Single job (``_run_commit``) that runs on the user-tunable
``commit_interval_mins`` cadence (default 5 min). Decoupled from ingest so
the freshness/cost tradeoff can be tuned independently of the Fastly logging
endpoint period.

After a successful commit the function calls ``_run_metadata_sync`` through
the ``backend.scheduler`` shim so legacy test patches at
``backend.scheduler._run_metadata_sync`` continue to intercept the call.
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    _check_buffer_backlog,
    _check_disk_space,
    _extract_log_text,
    _log_and_add_progress,
)

logger = logging.getLogger("backend.scheduler")


@cron_task("cron_compact")
def _run_commit(service_id: str, force: bool = False, run_id: int | None = None) -> None:
    """Commit the local buffer to the shared Iceberg table in FOS.

    Runs on its own cadence (commit_interval_mins) — independent of how often
    raw files are ingested. This lets the user control cloud data freshness
    without changing the Fastly logging endpoint period.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    if src.get("access_level") == "read_only" and not force:
        return

    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})
    if not sync_cfg.get("enabled", True) and not force:
        return

    try:
        if run_id is None:
            run_id = start_cron_run(src, "commit")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[commit]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    # Disk pre-check: commits write manifest cache + cloud-staged parquet
    # locally before upload. A full disk during commit can corrupt the
    # iceberg state midway, which is much worse than refusing to start.
    from backend.core.duckdb import _cache_dir as _commit_cache_dir

    ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "commit")
    if not ok:
        log_cron_run(
            src,
            "commit",
            0.0,
            "error",
            run_id=run_id,
            error_message=disk_msg,
            summary=f"Commit aborted: {disk_msg}",
        )
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="commit")
    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[95m[commit]\x1b[0m %s: Commit job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="commit",
        event={"type": "status", "message": "Committing local buffer to Iceberg snapshot..."},
    )

    start_time = time.time()
    try:
        from backend.core import iceberg as db_iceberg

        def _commit_progress(type, msg):
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": type, "message": msg})

        result = db_iceberg.commit_buffer(src, progress_callback=_commit_progress)
        duration = time.time() - start_time
        quarantined = int(result.get("quarantined_files", 0) or 0)
        quarantine_suffix = f" ⚠ quarantined {quarantined} unreadable file(s)" if quarantined else ""
        # Post-commit backlog probe: if anything is still in the buffer after a
        # successful commit, the next commit was racing with a fresh ingest OR
        # the drain is genuinely stuck (catalog perms, schema mismatch, etc.).
        # The threshold scales with commit_interval_mins so "stuck" means
        # "older than what a single commit cycle could reasonably leave behind."
        backlog_suffix = _check_buffer_backlog(
            src, service_id, commit_interval_mins=int(sync_cfg.get("commit_interval_mins", 5))
        )
        if result.get("files_committed", 0) > 0:
            summary = (
                f"Committed {result['files_committed']} buffer file(s) "
                f"({result['rows_committed']} rows) → snapshot {result.get('snapshot_id')}.{quarantine_suffix}{backlog_suffix}"
            )
            log_cron_run(
                src,
                "commit",
                duration,
                "success",
                run_id=run_id,
                rows_ingested=result["rows_committed"],
                summary=summary,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "done", "message": summary})

            # ── Post-commit view refresh + pool warm ──
            # commit_buffer drained the buffer (buf_set changed) and advanced
            # the Iceberg snapshot (metadata_loc changed). Without this hop,
            # the next reader on every pool slot would take the slow-path
            # rebuild under a lock that ingest also contends for. Doing both
            # the cache refresh and the pool warm on the commit thread keeps
            # the request path on the fast path.
            from backend.cron.jobs._common import refresh_view_and_warm_pool

            refresh_view_and_warm_pool(
                src,
                service_id,
                log_prefix="",
                progress_log=lambda ev: _log_and_add_progress(run_id, service_id, job_name="commit", event=ev),
            )

            # ── On-demand Sync ──
            # Since we just committed new data to the cloud, trigger a sync
            # immediately so the local cache/Data Lake view is updated. Route
            # through the ``backend.scheduler`` shim so legacy patches at
            # ``backend.scheduler._run_metadata_sync`` still intercept.
            try:
                import backend.scheduler as _shim

                _shim._run_metadata_sync(service_id)
            except Exception as e:
                _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "warning", "message": e})

            # ── Compact-on-sync ──
            # New parquet files just landed in the local cache. Fire local
            # compaction immediately to merge them rather than waiting up
            # to 2 min for the cron tick. Cheap and keeps the small-file
            # count as low as possible for the next dashboard render.
            # Wrapped in a fresh thread so a slow merge doesn't extend
            # the sync cron's wall-clock and risk the watchdog.
            try:
                import threading as _t

                from backend.core import local_compaction as _lc

                _t.Thread(
                    target=lambda: _lc.compact_local_partitions(src),
                    name=f"local-compact-on-sync:{service_id}",
                    daemon=True,
                ).start()
            except Exception as e:
                logger.warning("[scheduler] %s: post-sync local compaction failed to launch: %s", service_id, e)
        else:
            summary = "No new data to commit" + quarantine_suffix + backlog_suffix
            log_cron_run(
                src,
                "commit",
                duration,
                "success",
                run_id=run_id,
                summary=summary,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "done", "message": summary})
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "commit",
            duration,
            "error",
            run_id=run_id,
            error_message=str(e),
            summary="Buffer commit failed",
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: buffer commit failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    from backend.cron.jobs._common import finalize_cron_duration

    finalize_cron_duration(src, run_id, start_time)

    logger.info("⏹️  \x1b[95m[commit]\x1b[0m %s: Commit job finished.", _display)
