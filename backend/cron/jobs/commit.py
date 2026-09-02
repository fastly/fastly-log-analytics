"""Commit cron — drains the local buffer to the shared Iceberg table.

Single job (``_run_commit``) that runs on the user-tunable
``commit_interval_mins`` cadence (default 5 min). Decoupled from ingest so
the freshness/cost tradeoff can be tuned independently of the Fastly logging
endpoint period.

After a successful commit the function calls ``_run_metadata_sync`` resolved
off :mod:`backend.cron.jobs.metadata` at call time, so test patches at
``backend.cron.jobs.metadata._run_metadata_sync`` intercept the call.
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


@cron_task("cron_log_ingest", job_name="log_ingest")
def _run_log_ingest(service_id: str, force: bool = False, run_id: int | None = None) -> None:
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
            run_id = start_cron_run(src, "log_ingest")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[log_ingest]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    if svcconfig.INGEST_MODE == "celery":
        # Celery/ledger data plane: converts commit to DuckLake per insert, so
        # this job's role is adjacent-small-file compaction. Run it INLINE
        # (this job already executes on a worker in external mode) so the
        # cron_runs lease is held for the duration — a dispatch-and-forget
        # released the mutual-exclusion lease before the merge ran, letting
        # overlapping ticks run concurrent merges — and the row records the
        # real outcome instead of a fake instant success.
        from backend.core.ingest import finalize_committed_raw, merge_lake_files
        from backend.core.metadata.base import get_con

        merge_started = time.time()
        try:
            merge_lake_files(service_id)

            # Honest per-run counts for the cron row: how many files the
            # convert workers landed since the previous log_ingest tick, and
            # how many durable raw .gz files we deleted (delete_after).
            meta_con = get_con(service_id)
            prev = meta_con.execute(
                "SELECT started_at FROM cron_runs WHERE service_id = ? AND task = 'log_ingest' "
                "AND status != 'running' ORDER BY id DESC LIMIT 1",
                (service_id,),
            ).fetchone()
            since_epoch = 0.0
            if prev and prev["started_at"]:
                from backend.utils.date_utils import parse_iso_utc

                prev_dt = parse_iso_utc(prev["started_at"])
                if prev_dt is not None:
                    since_epoch = prev_dt.timestamp()
            files_ingested = meta_con.execute(
                "SELECT count(*) FROM ingest_ledger WHERE service_id = ? AND committed_at >= ?",
                (service_id, since_epoch),
            ).fetchone()[0]

            raw = finalize_committed_raw(service_id)

            summary = f"Ingested {files_ingested} file(s); merged small lake files"
            if raw["delete_after"]:
                summary += f"; deleted {raw['deleted']} raw file(s)"
            else:
                summary += "; raw deletion disabled (delete_after=false)"
            log_cron_run(
                src,
                "log_ingest",
                time.time() - merge_started,
                "success",
                run_id=run_id,
                files_downloaded=files_ingested,
                files_deleted_fos=raw["deleted"],
                summary=summary,
            )
        except Exception as e:
            log_cron_run(
                src,
                "log_ingest",
                time.time() - merge_started,
                "error",
                run_id=run_id,
                error_message=str(e),
                summary="DuckLake merge / raw finalization failed",
            )
            logger.exception("[ledger] %s: DuckLake merge / raw finalization failed", service_id)
        return

    # Disk pre-check: commits write manifest cache + cloud-staged parquet
    # locally before upload. A full disk during commit can corrupt the
    # iceberg state midway, which is much worse than refusing to start.
    from backend.core.duckdb import _cache_dir as _commit_cache_dir

    ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "log_ingest")
    if not ok:
        log_cron_run(
            src,
            "log_ingest",
            0.0,
            "error",
            run_id=run_id,
            error_message=disk_msg,
            summary=f"Commit aborted: {disk_msg}",
        )
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="log_ingest")
    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("🏎️  \x1b[95m[log_ingest]\x1b[0m %s: Ingest Logs job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="log_ingest",
        event={"type": "status", "message": "Committing local buffer to Iceberg snapshot..."},
    )

    start_time = time.time()
    try:
        from backend.core import iceberg as db_iceberg

        def _commit_progress(type, msg):
            _log_and_add_progress(run_id, service_id, job_name="log_ingest", event={"type": type, "message": msg})

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
                "log_ingest",
                duration,
                "success",
                run_id=run_id,
                rows_ingested=result["rows_committed"],
                summary=summary,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(run_id, service_id, job_name="log_ingest", event={"type": "done", "message": summary})

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
                progress_log=lambda ev: _log_and_add_progress(run_id, service_id, job_name="log_ingest", event=ev),
            )

            # ── On-demand Sync ──
            # Since we just committed new data to the cloud, trigger a sync
            # immediately so the local cache/Data Lake view is updated.
            # Resolved off the metadata jobs module at call time so patches
            # at ``backend.cron.jobs.metadata._run_metadata_sync`` intercept.
            try:
                from backend.cron.jobs import metadata as _metadata_jobs

                _metadata_jobs._run_metadata_sync(service_id)
            except Exception as e:
                _log_and_add_progress(
                    run_id, service_id, job_name="log_ingest", event={"type": "warning", "message": e}
                )

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
                "log_ingest",
                duration,
                "success",
                run_id=run_id,
                summary=summary,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(run_id, service_id, job_name="log_ingest", event={"type": "done", "message": summary})
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "log_ingest",
            duration,
            "error",
            run_id=run_id,
            error_message=str(e),
            summary="Buffer commit failed",
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="log_ingest", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: buffer commit failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    from backend.cron.jobs._common import finalize_cron_duration

    finalize_cron_duration(src, run_id, start_time)

    try:
        from backend.sync_status_publisher import publisher as _sync_status_publisher
        from backend.sync_status_snapshot import compute_sync_status_cached

        _snapshot = compute_sync_status_cached(service_id)
        if _snapshot is not None:
            _sync_status_publisher.publish(service_id, _snapshot)
    except Exception:
        logger.exception("[%s] %s: sync-status SSE publish failed", "scheduler", service_id)

    logger.info("🏁  \x1b[95m[log_ingest]\x1b[0m %s: Ingest Logs job finished.", _display)
