"""DuckLake buffer commit cron job (`_run_commit`)."""

from __future__ import annotations

import logging

from backend import config as svcconfig
from backend.core.storage.backup import schedule_cloud_push
from backend.cron.jobs._common import (
    _buffer_result_error,
    _orig_publish_sync_status_safe,
    _orig_refresh_status_after_commit,
    _orig_refresh_view_and_warm_pool,
    _orig_spawn_local_compaction,
    _refresh_status_after_commit,
    _resolve_sync_hook,
    _spawn_local_compaction,
    get_service_display_label,
    publish_sync_status_safe,
    refresh_view_and_warm_pool,
)
from backend.cron.scheduler import (
    _check_buffer_backlog,
    _check_disk_space,
    _log_and_add_progress,
    cron_run,
    cron_task,
)

logger = logging.getLogger("backend.scheduler")

_orig_check_disk_space = _check_disk_space
_orig_log_and_add_progress = _log_and_add_progress
_orig_schedule_cloud_push = schedule_cloud_push


@cron_task("cron_commit", job_name="commit")
def _run_commit(service_id: str, force: bool = False, run_id: int | None = None) -> None:
    """Commit the local buffer to the DuckLake table."""
    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    src = get_source_for_service(service_id)
    if src is None:
        return
    sync_cfg = cfg.get("provisioning", {}).get("cron_sync", {})
    if not sync_cfg.get("enabled", True) and not force:
        return

    _display = get_service_display_label(service_id, cfg)
    progress_fn = _resolve_sync_hook("_log_and_add_progress", _log_and_add_progress, _orig_log_and_add_progress)
    with cron_run(
        service_id,
        "commit",
        source=src,
        run_id=run_id,
        error_summary="Buffer commit failed",
        finalize_duration=True,
        skip_log_prefix="commit",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        if svcconfig.is_high_scale_mode(src):
            from backend.core.ingest import finalize_committed_raw, merge_lake_files
            from backend.core.metadata.base import get_con
            from backend.utils.date_utils import parse_iso_utc

            run.include_log_output = False
            try:
                merge_lake_files(service_id)
                meta_con = get_con(service_id)
                prev = meta_con.execute(
                    "SELECT started_at FROM cron_runs WHERE service_id = ? AND task = 'commit' "
                    "AND status != 'running' ORDER BY started_at DESC, id DESC LIMIT 1",
                    (service_id,),
                ).fetchone()
                prev_dt = parse_iso_utc(prev["started_at"]) if prev and prev["started_at"] else None
                since_epoch = prev_dt.timestamp() if prev_dt is not None else 0.0
                files_ingested = meta_con.execute(
                    "SELECT count(*) FROM ingest_ledger WHERE service_id = ? AND committed_at >= ?",
                    (service_id, since_epoch),
                ).fetchone()[0]
                raw = finalize_committed_raw(service_id)
                raw_suffix = (
                    f"; deleted {raw['deleted']} raw file(s)"
                    if raw["delete_after"]
                    else "; raw deletion disabled (delete_after=false)"
                )
                run.success(
                    f"Ingested {files_ingested} file(s); merged small lake files{raw_suffix}",
                    files_downloaded=files_ingested,
                    files_deleted_fos=raw["deleted"],
                )
            except Exception as e:
                run.fail("DuckLake merge / raw finalization failed", error_message=str(e))
                logger.exception("[ledger] %s: DuckLake merge / raw finalization failed", service_id)
            return

        from backend.core.duckdb import _cache_dir as _commit_cache_dir

        disk_check_fn = _resolve_sync_hook("_check_disk_space", _check_disk_space, _orig_check_disk_space)
        ok, disk_msg = disk_check_fn(_commit_cache_dir(src), service_id, "commit")
        if not ok:
            run.include_log_output = False
            run.fail(f"Commit aborted: {disk_msg}", error_message=disk_msg)
            return

        logger.info("🏎️  \x1b[95m[commit]\x1b[0m %s: Commit job started.", _display)
        progress_fn(
            run_id,
            service_id,
            job_name="commit",
            event={"type": "status", "message": "Committing local buffer to a DuckLake snapshot..."},
        )

        try:
            from backend.core import storage as db_storage

            result = db_storage.commit_buffer(
                src,
                progress_callback=lambda t, m: progress_fn(
                    run_id, service_id, job_name="commit", event={"type": t, "message": m}
                ),
            )
            quarantined = int(result.get("quarantined_files", 0) or 0)
            quarantine_suffix = f" ⚠ quarantined {quarantined} unreadable file(s)" if quarantined else ""
            commit_error = _buffer_result_error(result, "") or ""
            incomplete_suffix = f"; commit incomplete: {commit_error}" if commit_error else ""
            backlog_suffix = _check_buffer_backlog(
                src, service_id, commit_interval_mins=int(sync_cfg.get("commit_interval_mins", 5))
            )
            files_committed = result.get("files_committed", 0)
            if files_committed > 0:
                summary = (
                    f"Committed {files_committed} buffer file(s) "
                    f"({result['rows_committed']} rows) → snapshot {result.get('snapshot_id')}.{quarantine_suffix}"
                    f"{incomplete_suffix}{backlog_suffix}"
                )
                record_kw = {"rows_ingested": result["rows_committed"]}
            else:
                summary_start = "No buffer files committed" if commit_error else "No new data to commit"
                summary = summary_start + quarantine_suffix + incomplete_suffix + backlog_suffix
                record_kw = {}

            run.record(
                "error" if commit_error else "success", summary=summary, error_message=commit_error or None, **record_kw
            )
            progress_fn(
                run_id,
                service_id,
                job_name="commit",
                event={"type": "error" if commit_error else "done", "message": summary},
            )
            if files_committed > 0:
                refresh_view_fn = _resolve_sync_hook(
                    "refresh_view_and_warm_pool", refresh_view_and_warm_pool, _orig_refresh_view_and_warm_pool
                )
                refresh_view_fn(
                    src,
                    service_id,
                    log_prefix="",
                    progress_log=lambda ev: progress_fn(run_id, service_id, job_name="commit", event=ev),
                )
                try:
                    refresh_status_fn = _resolve_sync_hook(
                        "_refresh_status_after_commit",
                        _refresh_status_after_commit,
                        _orig_refresh_status_after_commit,
                    )
                    refresh_status_fn(service_id, src)
                except Exception as e:
                    progress_fn(run_id, service_id, job_name="commit", event={"type": "warning", "message": e})
                spawn_compact_fn = _resolve_sync_hook(
                    "_spawn_local_compaction", _spawn_local_compaction, _orig_spawn_local_compaction
                )
                spawn_compact_fn(src, service_id)
                push_fn = _resolve_sync_hook("schedule_cloud_push", schedule_cloud_push, _orig_schedule_cloud_push)
                push_fn(src, ("logs",))
        except Exception as e:
            run.fail("Buffer commit failed", error_message=str(e))
            progress_fn(run_id, service_id, job_name="commit", event={"type": "error", "message": str(e)})
            logger.exception("[scheduler] %s: buffer commit failed: %s", service_id, e)

    publish_fn = _resolve_sync_hook(
        "publish_sync_status_safe", publish_sync_status_safe, _orig_publish_sync_status_safe
    )
    publish_fn(service_id, "scheduler")
    logger.info("🏁  \x1b[95m[commit]\x1b[0m %s: Commit job finished.", _display)
