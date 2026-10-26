"""Ingest-class cron jobs.

  * ``_run_service_cron`` — per-tick ingest of new raw .gz files from FOS
    into the local buffer (does NOT commit to Iceberg).
  * ``_run_full_sweep`` — daily catch-net that LISTs the full raw/request/ prefix
    to pick up late-arriving files outside the incremental window.
  * ``_run_gap_heal`` — periodic gap detector that triggers a full sweep
    when sustained loss is observed between Fastly stats and our ingest.

The gap-heal trigger reads its collaborators (``_run_full_sweep`` and the
throttle-marker helpers) from module globals at call time, so tests patch
them at ``backend.cron.jobs.sync.<name>``.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from backend.cron.scheduler import (
    _check_buffer_backlog,
    _check_disk_space,
    _claim_heavy_refresh,
    _display_label,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
    cron_run,
    cron_task,
)

logger = logging.getLogger("backend.scheduler")


def _refuse_if_dev_no_crons(service_id: str, label: str) -> bool:
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — %s refused.", service_id, label)
        return True
    return False


def _spawn_local_compaction(
    src: dict,
    service_id: str,
    *,
    compact_active_hour: bool = False,
    refresh_on_merge: bool = False,
) -> None:
    def _worker() -> None:
        try:
            from backend.core import local_compaction as _lc

            res = (
                _lc.compact_local_partitions(src, compact_active_hour=True)
                if compact_active_hour
                else _lc.compact_local_partitions(src)
            )
            if refresh_on_merge and isinstance(res, dict) and res.get("files_merged", 0) > 0:
                refresh_view_and_warm_pool(src, service_id, log_prefix="[compact-on-sync] ")
        except Exception as e:
            logger.warning("[scheduler] %s: post-sync local compaction failed: %s", service_id, e)

    try:
        threading.Thread(
            target=_worker,
            name=f"local-compact-on-sync:{service_id}",
            daemon=True,
        ).start()
    except Exception as e:
        logger.warning("[scheduler] %s: post-sync local compaction failed to launch: %s", service_id, e)


# ── Shared cron job helpers (formerly backend.cron.jobs._common) ─────────────


def refresh_view_and_warm_pool(
    source: dict,
    service_id: str,
    *,
    log_prefix: str,
    progress_log: Callable[[dict], None] | None = None,
) -> None:
    """Force-refresh the Iceberg DuckDB view and warm the connection pool.

    Called on the cron writer thread after a commit or a sync tick so the
    next request-path checkout finds a pre-bound view (avoiding the
    slow-path rebuild under the per-service lock).

    Emits a single status message via ``progress_log`` ON SUCCESS ONLY.
    Failures are logged at WARNING through this module's logger; the
    progress feed stays quiet because the prior shape (sync.py) put the
    success message OUTSIDE the try/except and so reported "View refresh +
    warm: Xms" even when the work raised. Fixes that latent mis-log.
    """
    t0 = time.time()
    try:
        from backend.core import iceberg as _ice
        from backend.core.duckdb import get_connection as _get_conn
        from backend.core.duckdb_pool import warm_pool_for_service as _warm

        con_v = _get_conn(source=source, read_only=True)
        try:
            _ice.update_iceberg_view(con_v, source, force=True)
            _warm(service_id, source)
        finally:
            con_v.close()
        if progress_log is not None:
            progress_log(
                {"type": "status", "message": f"{log_prefix}View refresh + warm: {int((time.time() - t0) * 1000)}ms"}
            )
    except Exception as e:
        logger.warning("[scheduler] %s: post-%s view refresh failed: %s", service_id, log_prefix.strip(": "), e)


def finalize_cron_duration(
    src: dict,
    run_id: int | None,
    t_start: float,
    *,
    log_output: str | None = None,
    silent: bool = True,
    clock: Callable[[], float] = time.time,
) -> None:
    """Update the cron-run row's ``duration_s`` (and optionally ``log_output``)."""
    if run_id is None:
        return
    try:
        elapsed = clock() - t_start
        from backend.core.duckdb import update_cron_duration

        if log_output is not None:
            update_cron_duration(src, run_id, elapsed, log_output=log_output)
        else:
            update_cron_duration(src, run_id, elapsed)
    except Exception as e:
        if not silent:
            logger.warning("Failed to update full cron duration: %s", e)


def get_service_display_label(service_id: str, cfg: dict | None = None) -> str:
    """Return ``"{name} ({service_id})"`` when the service name differs from ``service_id``."""
    if cfg is None:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
    svc_name = (cfg.get("service_name") or cfg.get("name") or service_id) if cfg else service_id
    return f"{svc_name} ({service_id})" if svc_name and svc_name != service_id else service_id


def has_sufficient_disk_space(
    path: str,
    job_label: str,
    service_id: str,
    *,
    min_free_bytes: int = 50 * 1024 * 1024,
) -> tuple[bool, str]:
    """Return ``(ok, msg)`` checking that ``path`` (or ``.``) has at least ``min_free_bytes`` free."""
    import shutil

    check_dir = path if os.path.isdir(path) else "."
    try:
        free_bytes = shutil.disk_usage(check_dir).free
        if free_bytes < min_free_bytes:
            msg = f"Disk space critically low (< {min_free_bytes // (1024 * 1024)}MB free: {free_bytes / (1024 * 1024):.1f}MB)"
            logger.warning("⚠️  [%s] %s: %s", job_label, service_id, msg)
            return False, msg
    except Exception as e:
        logger.warning("[%s] %s: failed to check disk space: %s", job_label, service_id, e)
    return True, ""


def publish_sync_status_safe(service_id: str, log_label: str = "scheduler") -> None:
    """Compute and publish the cached sync-status SSE snapshot for ``service_id`` (best-effort)."""
    try:
        from backend.sync_status_publisher import publisher as _sync_status_publisher
        from backend.sync_status_snapshot import compute_sync_status_cached

        snapshot = compute_sync_status_cached(service_id)
        if snapshot is not None:
            _sync_status_publisher.publish(service_id, snapshot)
    except Exception:
        logger.exception("[%s] %s: sync-status SSE publish failed", log_label, service_id)


def _buffer_result_error(result: dict, default_error: str | None = None) -> str | None:
    """Extract combined error and quarantined-file message from a ``commit_buffer`` result dict."""
    parts: list[str] = []
    if result.get("error"):
        parts.append(str(result["error"]))
    quarantined = int(result.get("quarantined_files", 0) or 0)
    if quarantined:
        parts.append(f"{quarantined} unreadable buffer file(s) quarantined")
    return "; ".join(parts) if parts else default_error


def apply_time_range_pinning(
    service_id: str,
    cfg: dict,
    src: dict,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    is_manual: bool = False,
    fallback_to_created_at: bool = False,
    pin_explicit_range: bool = False,
    get_scheduler_fn: Callable | None = None,
) -> tuple[str | None, str | None]:
    """Resolve and persist provisioning ``time_range`` bounds for sync/metadata jobs."""
    from backend import config as svcconfig

    prov = cfg.get("provisioning", {})
    should_pin = False
    if not start_time and not is_manual:
        tr = prov.get("time_range")
        if tr and tr.get("start"):
            start_time = tr["start"]
            logger.info("[scheduler] %s: Using configured start_time limit: %s", service_id, start_time)
            should_pin = pin_explicit_range
        elif fallback_to_created_at and cfg.get("created_at"):
            start_time = cfg["created_at"]
            logger.info(
                "[scheduler] %s: Fallback to service creation time as start_time limit: %s", service_id, start_time
            )
            should_pin = True
    elif pin_explicit_range and (start_time or end_time):
        should_pin = True

    if should_pin and (start_time or end_time):

        def _pin_time_range(c: dict) -> None:
            time_range = c.setdefault("provisioning", {}).setdefault("time_range", {})
            if start_time:
                time_range["start"] = start_time
            if end_time:
                time_range["end"] = end_time

        _pin_time_range(cfg)
        svcconfig.mutate_config(service_id, _pin_time_range)
        src["time_range"] = cfg["provisioning"]["time_range"]
        if fallback_to_created_at:
            logger.info("[scheduler] %s: Saved service creation time as start_time limit to config.", service_id)
    elif is_manual and not start_time and not end_time:
        if "time_range" in prov:
            del prov["time_range"]
            cfg["provisioning"] = prov
            svcconfig.mutate_config(service_id, lambda c: c.get("provisioning", {}).pop("time_range", None))
            src["time_range"] = None
            logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

    return start_time, end_time


_INGEST_ERROR_OUTCOME_KEYS = (
    "malformed_records",
    "corrupt_containers",
    "quarantine_capture_failures",
    "source_delete_failures",
    "objects_failed",
)


def _has_ingest_outcome_errors(outcome_counters: dict | None, corrupt_rows: int = 0) -> bool:
    return bool(
        corrupt_rows > 0
        or (outcome_counters and any(outcome_counters.get(key, 0) for key in _INGEST_ERROR_OUTCOME_KEYS))
    )


def _finish_post_ingest_view_and_rollups(
    src: dict,
    service_id: str,
    run_id: int | None,
    job_name: str,
    done_event: dict,
    elapsed_fn: Callable[[], str],
    *,
    already_published: bool = False,
) -> None:
    if done_event.get("rows_inserted", 0) > 0 and not already_published:
        from backend.utils.active_requests import yield_to_api

        yield_to_api()
        refresh_view_and_warm_pool(
            src,
            service_id,
            log_prefix=f"{elapsed_fn()} ",
            progress_log=lambda ev: _log_and_add_progress(run_id, service_id, job_name=job_name, event=ev),
        )
    if touched_hours := done_event.get("touched_hours", []):
        schedule_post_ingest_rollups(service_id, src, set(touched_hours))
        _log_and_add_progress(
            run_id,
            service_id,
            job_name=job_name,
            event={"type": "status", "message": f"{elapsed_fn()} Rollups queued for {len(touched_hours)} hour(s)"},
        )


def _ingest_with_adaptive_followups(
    ingest_fn: Callable[..., Iterator[dict]],
    *,
    adaptive: bool,
    after_pass: Callable[[], None] | None = None,
    **ingest_kwargs,
) -> Iterator[dict]:
    from backend.core.ingest import _new_object_outcome

    started = time.monotonic()
    pass_number = 0
    processed_files = 0
    pass_rows = 0
    totals: dict[str, Any] = {
        "new_files": 0,
        "rows_inserted": 0,
        "corrupt_rows": 0,
        "deleted_files": 0,
        "quarantined_files": 0,
        "corrupt_details": [],
        "touched_hours": set(),
        "outcome_counters": _new_object_outcome(processed=0),
    }
    max_files = ingest_kwargs.get("max_files")
    pass_new_files = 0
    adaptive_deadline_s = float(os.environ.get("INGEST_INCREMENTAL_ADAPTIVE_DEADLINE_S", "5.0"))

    while True:
        if pass_number:
            sleep_time = 0.0 if (max_files and pass_new_files >= max_files) else 0.5
            if not adaptive or pass_number >= 3 or time.monotonic() - started + sleep_time > adaptive_deadline_s:
                break
            # Publish this pass's rows now; the caller's final refresh only
            # runs after the whole polling window.
            if after_pass is not None and pass_rows > 0:
                after_pass()
            if time.monotonic() - started + sleep_time > adaptive_deadline_s:
                break
            if sleep_time > 0:
                time.sleep(sleep_time)
            yield {"type": "status", "message": "Adaptive polling: checking for newly arrived log files."}

        pass_done: dict = {}
        pass_new_files = 0
        for event in ingest_fn(**ingest_kwargs):
            event = dict(event)
            if event.get("type") == "file_done":
                event["current"] = processed_files + event.get("current", 0)
                event["total"] = processed_files + event.get("total", 0)
            elif event.get("type") == "done":
                pass_done = event
                pass_new_files = event.get("new_files", 0)
                pass_rows = event.get("rows_inserted", 0)
                processed_files += pass_new_files
                for key in ("new_files", "rows_inserted", "corrupt_rows", "deleted_files", "quarantined_files"):
                    totals[key] += event.get(key, 0)
                totals["corrupt_details"].extend(event.get("corrupt_details", []))
                totals["touched_hours"].update(event.get("touched_hours", []))
                for key, value in event.get("outcome_counters", {}).items():
                    totals["outcome_counters"][key] = totals["outcome_counters"].get(key, 0) + value
                event.update(totals)
                event["touched_hours"] = sorted(totals["touched_hours"])
            yield event
            if event.get("type") == "error":
                return

        if not pass_done or pass_new_files == 0:
            return
        pass_number += 1


# ── _run_service_cron (per-tick ingest) ──────────────────────────────────────


_rollup_lock = threading.Lock()
_rollup_pending: dict[str, set[str]] = {}
_rollup_sources: dict[str, dict[str, Any]] = {}
_rollup_workers: dict[str, threading.Thread] = {}


def _post_ingest_rollup_worker(service_id: str) -> None:
    from backend.core import rollups
    from backend.utils.active_requests import yield_to_api

    while True:
        with _rollup_lock:
            hours = _rollup_pending.pop(service_id, set())
            src = _rollup_sources.get(service_id)
            if not hours or src is None:
                _rollup_workers.pop(service_id, None)
                return
        try:
            yield_to_api()
            rollups.recompute_touched_hours(service_id, src, hours)
        except Exception as e:
            logger.warning("[scheduler] %s: post-sync rollup recompute failed: %s", service_id, e)
        # Wellknown-bots rollup is best-effort: the security reader has a
        # live-SQL fallback for any hour without one.
        try:
            yield_to_api()
            rollups.recompute_wellknown_bots_rollup(service_id, src, hours)
        except Exception as e:
            logger.warning("[scheduler] %s: post-sync bot rollup failed: %s", service_id, e)


def schedule_post_ingest_rollups(service_id: str, src: dict[str, Any], hours: set[str]) -> None:
    """Queue closed-hour rollup recompute on a per-service single-flight worker.

    Hours queued while a pass runs coalesce into one follow-up pass.
    """
    if not hours:
        return
    with _rollup_lock:
        _rollup_pending.setdefault(service_id, set()).update(hours)
        _rollup_sources[service_id] = src
        if service_id in _rollup_workers:
            return
        worker = threading.Thread(
            target=_post_ingest_rollup_worker,
            args=(service_id,),
            name=f"post-ingest-rollups:{service_id}",
            daemon=True,
        )
        _rollup_workers[service_id] = worker
    worker.start()


@cron_task("cron.log_discovery", job_name="log_discovery")
def _run_log_discovery_cron(
    service_id: str,
    force: bool = False,
    delete_after: bool | None = None,
    run_id: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> None:
    """Ingest new raw .gz files from FOS into the local buffer."""
    if _refuse_if_dev_no_crons(service_id, "sync"):
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, refresh_config_status
    from backend.core.ingest import _new_object_outcome, ingest

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        logger.warning("[scheduler] %s: config not found, skipping.", service_id)
        return

    src = get_source_for_service(service_id)
    if src is None:
        logger.warning("[scheduler] %s: source not found, skipping.", service_id)
        return

    if src.get("access_level") == "read_only" and not force:
        return

    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})
    sync_enabled = sync_cfg.get("enabled", True)
    delete_after = svcconfig.resolve_raw_delete_after(cfg, delete_after)
    _display = get_service_display_label(service_id, cfg)

    # ── 1. Ingest ─────────────────────────────────────────────────────────────
    if sync_enabled or force:
        is_manual = run_id is not None
        start_time, _ = apply_time_range_pinning(
            service_id,
            cfg,
            src,
            start_time=start_time,
            is_manual=is_manual,
            fallback_to_created_at=True,
        )

        with cron_run(
            service_id,
            "log_discovery",
            source=src,
            run_id=run_id,
            error_summary="Log discovery failed",
            skip_log_prefix="log_discovery",
            finalize_running_backstop=True,
            backstop_summary="Sync exited without recording a terminal status",
            backstop_error_message="orphaned run auto-finalized (no terminal ingest event)",
        ) as run:
            if not run.ok:
                return
            run_id = run.run_id

            if svcconfig.is_high_scale_mode(src):
                from datetime import datetime, timedelta

                from backend.core.ingest import discover_prefix
                from backend.provision.log_paths import minute_list_prefix

                run.include_log_output = False
                if not svcconfig.CELERY_BROKER_URL:
                    run.fail(
                        "Celery ingest misconfigured: no broker URL",
                        error_message="DEPLOYMENT_MODE=high_scale requires CELERY_BROKER_URL",
                    )
                    return

                try:
                    discovered = 0
                    adaptive = sync_cfg.get("polling_mode", "adaptive") == "adaptive" and not is_manual
                    poll_started = time.monotonic()
                    for poll_index in range(3):
                        if poll_index:
                            if not adaptive or poll_index > 2 or time.monotonic() - poll_started + 3 > 20:
                                break
                            time.sleep(3)
                        now = datetime.now(UTC)
                        pass_discovered = sum(
                            discover_prefix(
                                service_id,
                                prefix_subpath=minute_list_prefix(now - timedelta(minutes=i)),
                                run_id=run_id,
                            )
                            for i in range(5)
                        )
                        discovered += pass_discovered
                        if pass_discovered == 0:
                            break
                    run.success(
                        f"Discovered {discovered} new file(s); dispatched to ingest workers"
                        if discovered
                        else "No new files",
                        files_downloaded=discovered,
                    )
                except Exception as e:
                    run.fail("Log discovery failed", error_message=str(e))
                    logger.exception("[ledger] %s: log discovery failed: %s", service_id, e)
                return

            from backend.core.duckdb import _cache_dir

            ok, disk_msg = _check_disk_space(_cache_dir(src), service_id, "log_discovery")
            if not ok:
                run.include_log_output = False
                run.fail(f"Sync aborted: {disk_msg}", error_message=disk_msg)
                return

            logger.info("▶️  \x1b[94m[log_discovery]\x1b[0m %s: Log Discovery job started.", _display)
            start_time_exec = time.time()

            def elapsed() -> str:
                return _elapsed_since(start_time_exec)

            def _progress_status(message: str) -> None:
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="log_discovery",
                    event={"type": "status", "message": f"{elapsed()} {message}"},
                )

            msg = "Starting log discovery..."
            if start_time or end_time:
                msg += f" (Range: {start_time or 'Start'} to {end_time or 'End'})"
            _progress_status(msg)

            done_event: dict = {}
            ingest_outcomes = _new_object_outcome(processed=0)
            processed_files = 0
            inserted_rows = 0
            corrupt_rows = 0

            max_incremental_files = int(os.environ.get("INGEST_INCREMENTAL_MAX_FILES", "250"))
            pass_max_files = max_incremental_files if not is_manual else 5000
            pass_max_seconds = 20 if not is_manual else 240
            published_pass_rows = 0

            def _publish_pass_rows() -> None:
                nonlocal published_pass_rows
                refresh_view_and_warm_pool(
                    src,
                    service_id,
                    log_prefix=f"{elapsed()} [adaptive] ",
                    progress_log=lambda ev: _log_and_add_progress(
                        run_id, service_id, job_name="log_discovery", event=ev
                    ),
                )
                publish_sync_status_safe(service_id, "scheduler")
                published_pass_rows = inserted_rows

            try:
                for event in _ingest_with_adaptive_followups(
                    ingest,
                    adaptive=sync_cfg.get("polling_mode", "adaptive") == "adaptive" and not is_manual,
                    after_pass=_publish_pass_rows,
                    source=src,
                    delete_after=delete_after,
                    max_files=pass_max_files,
                    max_seconds=pass_max_seconds,
                    start_time=start_time,
                    end_time=end_time,
                    incremental_only=not is_manual,
                ):
                    _log_and_add_progress(run_id, service_id, job_name="log_discovery", event=event)
                    ev_type = event.get("type")
                    if ev_type == "file_done":
                        processed_files = event.get("current", processed_files)
                        inserted_rows = event.get("total_inserted", inserted_rows)
                        corrupt_rows = event.get("total_corrupt", corrupt_rows)
                    elif ev_type == "done":
                        done_event = event
                        ingest_outcomes = event.get("outcome_counters", ingest_outcomes)
                    elif ev_type == "error":
                        summary = "Ingestion failed"
                        if processed_files > 0:
                            summary += f" after processing {processed_files} files ({inserted_rows} rows)"
                        run.fail(
                            summary,
                            error_message=event.get("message"),
                            files_downloaded=processed_files,
                            rows_ingested=inserted_rows,
                            corrupt_rows=corrupt_rows,
                            outcome_counters=ingest_outcomes,
                        )
                        _log_and_add_progress(
                            run_id,
                            service_id,
                            job_name="log_discovery",
                            event={"type": "error", "message": event.get("message")},
                        )
                        break
                else:
                    if done_event:
                        if done_event.get("new_files", 0) == 0:
                            reclaimed = done_event.get("deleted_files", 0)
                            if reclaimed:
                                summary = (
                                    f"No new files; reclaimed {reclaimed} raw file(s) left by an interrupted prior run"
                                )
                                done_msg = f"{elapsed()} {summary}."
                            else:
                                summary = "No new log files found in bucket"
                                done_msg = f"{elapsed()} No new log files found in bucket."
                            run.success(
                                summary,
                                files_deleted_fos=reclaimed,
                                outcome_counters=done_event.get("outcome_counters"),
                            )
                            _log_and_add_progress(
                                run_id,
                                service_id,
                                job_name="log_discovery",
                                event={"type": "done", "message": done_msg},
                            )
                        else:
                            summary = (
                                f"Ingested {done_event.get('new_files', 0)} files, "
                                f"{done_event.get('rows_inserted', 0)} rows."
                            )
                            if done_event.get("corrupt_rows"):
                                summary += f" Skipped {done_event.get('corrupt_rows')} corrupted/invalid lines."
                            if done_event.get("quarantined_files"):
                                summary += f" Quarantined {done_event.get('quarantined_files')} files with errors."
                            if done_event.get("deleted_files"):
                                summary += f" Deleted {done_event.get('deleted_files')} raw files."
                            corrupt_details = done_event.get("corrupt_details", [])
                            corrupt_message = "\n".join(corrupt_details) if corrupt_details else None
                            outcome_counters = done_event.get("outcome_counters")
                            has_ingest_errors = _has_ingest_outcome_errors(outcome_counters)

                            run.record(
                                "error" if has_ingest_errors else "success",
                                summary=summary,
                                error_message=corrupt_message,
                                files_downloaded=done_event.get("new_files", 0),
                                files_deleted_fos=done_event.get("deleted_files", 0),
                                rows_ingested=done_event.get("rows_inserted", 0),
                                corrupt_rows=done_event.get("corrupt_rows", 0),
                                outcome_counters=outcome_counters,
                            )

                            already_published = bool(
                                published_pass_rows > 0 and published_pass_rows == done_event.get("rows_inserted", 0)
                            )
                            if done_event.get("rows_inserted", 0) > 0 and not already_published:
                                _finish_post_ingest_view_and_rollups(
                                    src,
                                    service_id,
                                    run_id,
                                    "log_discovery",
                                    {**done_event, "touched_hours": []},
                                    elapsed,
                                )

                            if done_event.get("new_files", 0) > 1:
                                _spawn_local_compaction(
                                    src, service_id, compact_active_hour=True, refresh_on_merge=True
                                )

                            if done_event.get("touched_hours"):
                                _finish_post_ingest_view_and_rollups(
                                    src,
                                    service_id,
                                    run_id,
                                    "log_discovery",
                                    done_event,
                                    elapsed,
                                    already_published=True,
                                )

            except Exception as e:
                summary = "Ingestion crashed"
                if processed_files > 0:
                    summary += f" after processing {processed_files} files ({inserted_rows} rows)"
                    _log_and_add_progress(
                        run_id,
                        service_id,
                        job_name="log_discovery",
                        event={
                            "type": "status",
                            "message": f"Crash occurred. Successfully ingested {processed_files} files so far.",
                        },
                    )
                run.fail(
                    summary,
                    error_message=str(e),
                    files_downloaded=processed_files,
                    rows_ingested=inserted_rows,
                    corrupt_rows=corrupt_rows,
                    outcome_counters=ingest_outcomes,
                )
                logger.exception("[scheduler] %s: unexpected ingest error.", service_id)
                _log_and_add_progress(
                    run_id, service_id, job_name="log_discovery", event={"type": "error", "message": str(e)}
                )

    # ── 2. Refresh cached status ──────────────────────────────────────────────
    do_heavy_refresh = _claim_heavy_refresh(service_id) or bool(force)
    should_refresh_status = bool(done_event.get("rows_inserted", 0) > 0 or do_heavy_refresh or force)
    if should_refresh_status:
        if (sync_enabled or force) and run_id is not None:
            _msg_suffix = "+ filter suggestions" if do_heavy_refresh else "(header only)"
            _progress_status(f"Refreshing sync status {_msg_suffix}...")
        _t0 = time.time()
        try:
            refresh_config_status(service_id, include_top_values=False)
        except Exception:
            pass
        publish_sync_status_safe(service_id, "scheduler")
        if run_id is not None:
            _progress_status(f"refresh_config_status: {int((time.time() - _t0) * 1000)}ms")

    if do_heavy_refresh:
        _t0 = time.time()
        try:
            refresh_config_status(service_id, include_top_values=True)
        except Exception:
            pass
        if run_id is not None:
            _progress_status(f"refresh_config_status (heavy): {int((time.time() - _t0) * 1000)}ms")

    # ── 3. Invalidate dashboard cache ─────────────────────────────────────────
    _t0 = time.time()
    _invalidated = 0
    try:
        from backend.repositories.dashboard import DASHBOARD_CACHE_TTL

        if DASHBOARD_CACHE_TTL > 0:
            from backend.repositories.dashboard import _dashboard_cache, invalidate_service

            src_name = src.get("name", "")
            _invalidated = sum(1 for k in list(_dashboard_cache) if k.endswith(f":{src_name}"))
            invalidate_service(src_name)
    except Exception:
        pass
    if run_id is not None and _invalidated:
        _progress_status(f"dashboard cache invalidate ({_invalidated} keys): {int((time.time() - _t0) * 1000)}ms")

    # ── 4. Usage log bookkeeping ──────────────────────────────────────────────
    if (sync_enabled or force) and run_id is not None and do_heavy_refresh:
        _progress_status("Updating usage log (Fastly-edge writes, in-process calls, retention purge)...")

    def _usage_log_phase() -> None:
        from backend.core.duckdb import backfill_fastly_edge_writes, reconcile_fastly_stats
        from backend.utils.telemetry import run_usage_log_cleanup

        def _run_usage_step(fn: Callable[[], int], msg_fn: Callable[[int], str], log_label: str) -> None:
            try:
                count = fn()
                if count:
                    if run_id is not None:
                        _log_and_add_progress(
                            run_id, service_id, job_name="usage_log", event={"type": "status", "message": msg_fn(count)}
                        )
                    else:
                        logger.info("[usage_log] %s: %s", service_id, msg_fn(count))
            except Exception as e:
                logger.warning("[usage_log] %s failed for %s: %s", log_label, service_id, e)

        _run_usage_step(
            lambda: backfill_fastly_edge_writes(src),
            lambda n: f"Backfilled {n} Fastly-edge PUTs to usage log",
            "backfill",
        )
        _run_usage_step(
            lambda: reconcile_fastly_stats(src, hours_back=26),
            lambda n: f"Reconciled {n} hourly Fastly stats gap(s)",
            "Fastly stats reconciliation",
        )
        run_usage_log_cleanup(service_id)

    if do_heavy_refresh:
        _t0 = time.time()
        try:
            _usage_log_phase()
        except Exception as e:
            logger.warning("[scheduler] %s: usage_log phase failed: %s", service_id, e)
        if run_id is not None:
            _progress_status(f"usage_log phase: {int((time.time() - _t0) * 1000)}ms")

    # ── 5. Final duration record ──────────────────────────────────────────────
    if sync_enabled or force:
        finalize_cron_duration(
            src,
            run_id,
            start_time_exec,
            log_output=_extract_log_text(run_id) if run_id is not None else None,
            silent=False,
        )
    logger.info("⏹️  \x1b[94m[log_discovery]\x1b[0m %s: Log Discovery job finished.", _display)


# ── _run_full_sweep (daily catch-net) ────────────────────────────────────────

_FULL_SWEEP_DEFAULT_MAX_FILES = 20_000
_FULL_SWEEP_DEFAULT_MAX_SECONDS = 900


@cron_task("cron.full_sync", job_name="full_sync")
def _run_full_sweep(
    service_id: str,
    max_files: int = _FULL_SWEEP_DEFAULT_MAX_FILES,
    max_seconds: int = _FULL_SWEEP_DEFAULT_MAX_SECONDS,
    run_id: int | None = None,
    force: bool = False,
) -> None:
    """Daily catch-net: full LIST over raw/request/ to pick up late-arriving files."""
    if _refuse_if_dev_no_crons(service_id, "full_sweep"):
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.ingest import ingest
    from backend.utils.active_requests import should_defer_cron

    cfg = svcconfig.load_config(service_id) or {}
    delete_after = svcconfig.resolve_raw_delete_after(cfg)

    src = get_source_for_service(service_id)
    if src is None or src.get("access_level") == "read_only":
        return

    if not force and should_defer_cron("full_sync", service_id):
        return

    _display = _display_label(src, service_id)
    with cron_run(
        service_id,
        "full_sync",
        source=src,
        run_id=run_id,
        error_summary="Full-sweep crashed",
        skip_log_prefix="full_sync",
        finalize_duration=True,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id

        full_sweep_cfg = cfg.get("provisioning", {}).get("cron_full_sweep", {})
        configured_max_files = full_sweep_cfg.get("max_files")
        configured_max_seconds = full_sweep_cfg.get("max_seconds")

        effective_max_files = max_files
        effective_max_seconds = max_seconds

        if max_files == _FULL_SWEEP_DEFAULT_MAX_FILES:
            if configured_max_files is not None:
                effective_max_files = int(configured_max_files)
            elif svcconfig.is_high_scale_mode(src):
                try:
                    from backend.celery_status import celery_queue_depths

                    queues, broker_ok = celery_queue_depths()
                    total_queue = sum(queues.values()) if broker_ok else 0
                    if total_queue > 5_000:
                        effective_max_files = min(effective_max_files, 5_000)
                    elif total_queue < 500:
                        effective_max_files = max(effective_max_files, 50_000)
                except Exception:
                    pass
            else:
                try:
                    from backend.core import iceberg as db_iceberg

                    stats = db_iceberg.buffer_backlog_stats(src)
                    buf_files = int(stats.get("file_count", 0) or 0)
                    if buf_files > 2_000:
                        effective_max_files = min(effective_max_files, 5_000)
                        effective_max_seconds = min(effective_max_seconds, 300)
                    elif buf_files < 200:
                        effective_max_files = max(effective_max_files, 50_000)
                        effective_max_seconds = max(effective_max_seconds, 1200)
                except Exception:
                    pass

        if max_seconds == _FULL_SWEEP_DEFAULT_MAX_SECONDS and configured_max_seconds is not None:
            effective_max_seconds = int(configured_max_seconds)

        if svcconfig.is_high_scale_mode(src):
            from backend.core.ingest import discover_prefix

            run.include_log_output = False
            try:
                discovered = discover_prefix(service_id, run_id=run_id, originating_task="full_sync")
                run.success(
                    f"Full-prefix sweep discovered {discovered} unseen file(s); dispatched to ingest workers"
                    if discovered
                    else "Full-prefix sweep: no unseen files",
                    files_downloaded=discovered,
                )
            except Exception as e:
                run.fail("Full-prefix ledger sweep failed", error_message=str(e))
                logger.exception("[full_sync] %s: ledger sweep failed", service_id)
            return

        logger.info(
            "▶️  \x1b[95m[full_sync]\x1b[0m %s: Full-LIST sweep started (max_files=%d, max_seconds=%d).",
            _display,
            effective_max_files,
            effective_max_seconds,
        )

        def elapsed() -> str:
            return f"[{run.elapsed():.1f}s]"

        processed_files = 0
        inserted_rows = 0
        corrupt_rows = 0
        done_event: dict = {}

        try:
            for event in ingest(
                source=src,
                delete_after=delete_after,
                max_files=effective_max_files,
                max_seconds=effective_max_seconds,
                incremental_only=False,
            ):
                _log_and_add_progress(run_id, service_id, job_name="full_sync", event=event)
                if event.get("type") == "file_done":
                    processed_files = event.get("current", processed_files)
                    inserted_rows = event.get("total_inserted", inserted_rows)
                    corrupt_rows = event.get("total_corrupt", corrupt_rows)
                elif event.get("type") == "done":
                    done_event = event
                elif event.get("type") == "error":
                    run.fail(
                        "Full-sweep failed",
                        error_message=event.get("message"),
                        files_downloaded=processed_files,
                        rows_ingested=inserted_rows,
                        corrupt_rows=corrupt_rows,
                        outcome_counters=event.get("outcome_counters"),
                    )
                    return

            new_files = done_event.get("new_files", 0)
            rows = done_event.get("rows_inserted", 0)
            corrupt_rows_cnt = done_event.get("corrupt_rows", 0)
            corrupt_details = done_event.get("corrupt_details", [])
            corrupt_message = "\n".join(corrupt_details) if corrupt_details else None
            outcome_counters = done_event.get("outcome_counters")
            has_ingest_errors = _has_ingest_outcome_errors(outcome_counters, corrupt_rows_cnt)
            reclaimed = done_event.get("deleted_files", 0)
            summary = (
                "No late-arriving files found"
                if new_files == 0
                else f"Backfilled {new_files} late-arriving file(s), {rows} row(s)"
            )
            if corrupt_rows_cnt > 0:
                summary += f"; ⚠ {corrupt_rows_cnt} corrupt row(s) quarantined"
            if reclaimed:
                summary += f"; reclaimed {reclaimed} raw file(s) left by an interrupted prior run"
            status = "error" if has_ingest_errors else "success"
            run.record(
                status,
                summary=summary,
                error_message=corrupt_message,
                files_downloaded=new_files,
                files_deleted_fos=reclaimed,
                rows_ingested=rows,
                corrupt_rows=corrupt_rows_cnt,
                outcome_counters=outcome_counters,
            )
            _log_and_add_progress(run_id, service_id, job_name="full_sync", event={"type": "done", "message": summary})
            _finish_post_ingest_view_and_rollups(src, service_id, run_id, "full_sync", done_event, elapsed)
        except Exception as e:
            run.fail(
                "Full-sweep crashed",
                error_message=str(e),
                files_downloaded=processed_files,
                rows_ingested=inserted_rows,
                corrupt_rows=corrupt_rows,
            )
            logger.exception("[full_sync] %s: unexpected error", service_id)

    logger.info("⏹️  \x1b[95m[full_sync]\x1b[0m %s: Daily full-LIST sweep finished.", _display)


# ── _run_gap_heal (periodic detector → triggers full_sweep) ──────────────────

GAP_HEAL_THROTTLE_HOURS = 4


@dataclass(frozen=True)
class _GapHealSeverityBand:
    name: str
    min_gap_pct: float
    min_lost_lines: int
    throttle_hours: float
    sweep_max_files: int
    sweep_max_seconds: int


_GAP_HEAL_SEVERITY_BANDS: tuple[_GapHealSeverityBand, ...] = (
    _GapHealSeverityBand(
        name="critical",
        min_gap_pct=0.80,
        min_lost_lines=500_000,
        throttle_hours=0.0,
        sweep_max_files=100_000,
        sweep_max_seconds=1800,
    ),
    _GapHealSeverityBand(
        name="severe",
        min_gap_pct=0.50,
        min_lost_lines=100_000,
        throttle_hours=0.25,
        sweep_max_files=50_000,
        sweep_max_seconds=1500,
    ),
    _GapHealSeverityBand(
        name="elevated",
        min_gap_pct=0.10,
        min_lost_lines=10_000,
        throttle_hours=2.0,
        sweep_max_files=_FULL_SWEEP_DEFAULT_MAX_FILES,
        sweep_max_seconds=_FULL_SWEEP_DEFAULT_MAX_SECONDS,
    ),
    _GapHealSeverityBand(
        name="mild",
        min_gap_pct=0.0,
        min_lost_lines=0,
        throttle_hours=GAP_HEAL_THROTTLE_HOURS,
        sweep_max_files=_FULL_SWEEP_DEFAULT_MAX_FILES,
        sweep_max_seconds=_FULL_SWEEP_DEFAULT_MAX_SECONDS,
    ),
)


def _gap_heal_severity(max_gap_pct: float, total_lost_lines: int) -> _GapHealSeverityBand:
    """Return the first severity band whose floor either field clears."""
    for band in _GAP_HEAL_SEVERITY_BANDS:
        if max_gap_pct >= band.min_gap_pct or total_lost_lines >= band.min_lost_lines:
            return band
    return _GAP_HEAL_SEVERITY_BANDS[-1]


_GAP_HEAL_LAST_TRIGGER: dict[str, float] = {}


def _last_successful_gap_heal_trigger(service_id: str) -> float | None:
    return _GAP_HEAL_LAST_TRIGGER.get(service_id)


def _mark_gap_heal_triggered(service_id: str) -> None:
    _GAP_HEAL_LAST_TRIGGER[service_id] = time.time()


@cron_task("gap_heal", job_name="gap_heal")
def _run_gap_heal(
    service_id: str,
    run_id: int | None = None,
    force: bool = False,
) -> None:
    """Periodic gap detector that triggers a full_sweep when sustained loss is observed."""
    if _refuse_if_dev_no_crons(service_id, "gap_heal"):
        return

    from backend.core.duckdb import get_source_for_service
    from backend.utils.active_requests import should_defer_cron

    src = get_source_for_service(service_id)
    if src is None or src.get("access_level") == "read_only":
        return

    if not force and should_defer_cron("gap_heal", service_id):
        return

    _display = _display_label(src, service_id)

    with cron_run(
        service_id,
        "gap_heal",
        source=src,
        run_id=run_id,
        error_summary="Gap-heal evaluation crashed",
        skip_log_prefix="gap_heal",
        finalize_duration=True,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        try:
            from fastapi import HTTPException

            from backend.routers.admin import compute_log_accounting

            try:
                result = compute_log_accounting(src, hours=24, by="hour")
            except HTTPException as exc:
                if "fastly_stats_failed" in str(exc.detail):
                    summary = "Fastly stats unavailable for service (skipped)"
                    run.success(summary)
                    _log_and_add_progress(
                        run_id,
                        service_id,
                        job_name="gap_heal",
                        event={"type": "done", "message": summary},
                    )
                    logger.info("⏭️  \x1b[95m[gap_heal]\x1b[0m %s: %s", service_id, summary)
                    return
                raise

            sustained = result.get("sustained_loss")
            if sustained is None:
                run.success("No sustained loss detected")
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="gap_heal",
                    event={"type": "done", "message": "No sustained loss detected"},
                )
                return

            band = _gap_heal_severity(sustained.max_gap_pct, sustained.total_lost_lines)
            last_heal = _last_successful_gap_heal_trigger(service_id)
            if not force and band.throttle_hours > 0 and last_heal is not None:
                elapsed_hours = (time.time() - last_heal) / 3600.0
                if elapsed_hours < band.throttle_hours:
                    msg = (
                        f"Sustained loss detected ({sustained.n_buckets} bucket(s), "
                        f"max gap {sustained.max_gap_pct:.1%}, "
                        f"{sustained.total_lost_lines} lost line(s), severity={band.name}) "
                        f"— throttled, last heal {elapsed_hours:.1f}h ago "
                        f"(< {band.throttle_hours:g}h)"
                    )
                    run.warn(msg)
                    _log_and_add_progress(
                        run_id, service_id, job_name="gap_heal", event={"type": "done", "message": msg}
                    )
                    return

            msg = (
                f"Sustained loss detected ({sustained.n_buckets} bucket(s) "
                f"from {sustained.started_at}, max gap {sustained.max_gap_pct:.1%}, "
                f"{sustained.total_lost_lines} lost line(s), severity={band.name}) — "
                f"triggering full_sweep (max_files={band.sweep_max_files}, "
                f"max_seconds={band.sweep_max_seconds})"
            )
            logger.warning("🩹 \x1b[33m[gap_heal]\x1b[0m %s: %s", _display, msg)
            _log_and_add_progress(run_id, service_id, job_name="gap_heal", event={"type": "status", "message": msg})
            run.warn(msg)
            _mark_gap_heal_triggered(service_id)
            _run_full_sweep(
                service_id,
                max_files=band.sweep_max_files,
                max_seconds=band.sweep_max_seconds,
                force=force,
            )
        except Exception as e:
            run.fail("Gap-heal evaluation crashed", error_message=str(e))
            logger.exception("[gap_heal] %s: unexpected error", service_id)


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron.jobs.sync._GAP_HEAL_LAST_TRIGGER", _GAP_HEAL_LAST_TRIGGER)


# ── _run_commit (formerly backend.cron.jobs.commit) ──────────────────────────


@cron_task("cron_commit", job_name="commit")
def _run_commit(service_id: str, force: bool = False, run_id: int | None = None) -> None:
    """Commit the local buffer to the shared Iceberg table in FOS."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service

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

    _display = get_service_display_label(service_id, cfg)
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

            run.include_log_output = False
            try:
                merge_lake_files(service_id)

                meta_con = get_con(service_id)
                prev = meta_con.execute(
                    "SELECT started_at FROM cron_runs WHERE service_id = ? AND task = 'commit' "
                    "AND status != 'running' ORDER BY started_at DESC, id DESC LIMIT 1",
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
                run.success(summary, files_downloaded=files_ingested, files_deleted_fos=raw["deleted"])
            except Exception as e:
                run.fail("DuckLake merge / raw finalization failed", error_message=str(e))
                logger.exception("[ledger] %s: DuckLake merge / raw finalization failed", service_id)
            return

        from backend.core.duckdb import _cache_dir as _commit_cache_dir

        ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "commit")
        if not ok:
            run.include_log_output = False
            run.fail(f"Commit aborted: {disk_msg}", error_message=disk_msg)
            return

        logger.info("🏎️  \x1b[95m[commit]\x1b[0m %s: Commit job started.", _display)
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="commit",
            event={"type": "status", "message": "Committing local buffer to Iceberg snapshot..."},
        )

        try:
            from backend.core import iceberg as db_iceberg

            def _commit_progress(type, msg):
                _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": type, "message": msg})

            result = db_iceberg.commit_buffer(src, progress_callback=_commit_progress)
            quarantined = int(result.get("quarantined_files", 0) or 0)
            quarantine_suffix = f" ⚠ quarantined {quarantined} unreadable file(s)" if quarantined else ""
            commit_error = _buffer_result_error(result, "") or ""
            incomplete_suffix = f"; commit incomplete: {commit_error}" if commit_error else ""
            backlog_suffix = _check_buffer_backlog(
                src, service_id, commit_interval_mins=int(sync_cfg.get("commit_interval_mins", 5))
            )
            if result.get("files_committed", 0) > 0:
                summary = (
                    f"Committed {result['files_committed']} buffer file(s) "
                    f"({result['rows_committed']} rows) → snapshot {result.get('snapshot_id')}.{quarantine_suffix}"
                    f"{incomplete_suffix}{backlog_suffix}"
                )
                run.record(
                    "error" if commit_error else "success",
                    summary=summary,
                    error_message=commit_error or None,
                    rows_ingested=result["rows_committed"],
                )
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="commit",
                    event={"type": "error" if commit_error else "done", "message": summary},
                )

                refresh_view_and_warm_pool(
                    src,
                    service_id,
                    log_prefix="",
                    progress_log=lambda ev: _log_and_add_progress(run_id, service_id, job_name="commit", event=ev),
                )

                try:
                    from backend.cron.jobs import metadata as _metadata_jobs

                    _metadata_jobs.dispatch_post_commit_metadata_sync(service_id)
                except Exception as e:
                    _log_and_add_progress(
                        run_id, service_id, job_name="commit", event={"type": "warning", "message": e}
                    )

                _spawn_local_compaction(src, service_id)
            else:
                summary_start = "No buffer files committed" if commit_error else "No new data to commit"
                summary = summary_start + quarantine_suffix + incomplete_suffix + backlog_suffix
                run.record("error" if commit_error else "success", summary=summary, error_message=commit_error or None)
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="commit",
                    event={"type": "error" if commit_error else "done", "message": summary},
                )
        except Exception as e:
            run.fail("Buffer commit failed", error_message=str(e))
            _log_and_add_progress(run_id, service_id, job_name="commit", event={"type": "error", "message": str(e)})
            logger.exception("[scheduler] %s: buffer commit failed: %s", service_id, e)

    publish_sync_status_safe(service_id, "scheduler")

    logger.info("🏁  \x1b[95m[commit]\x1b[0m %s: Commit job finished.", _display)


import sys as _sys  # noqa: E402

_sys.modules.setdefault("backend.cron.jobs._common", _sys.modules[__name__])
_sys.modules.setdefault("backend.cron.jobs.commit", _sys.modules[__name__])
