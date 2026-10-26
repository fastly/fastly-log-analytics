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


# ── RUM Sync, Discovery, Commit & Ledger Sweep (folded from backend.cron.jobs.rum_sync) ──

"""RUM sync cron job — ingest raw beacon logs from FOS.

Registered in scheduler as cron_rum_sync. Runs periodically (configurable interval).
"""


import logging
import sys as _sys
import time
from datetime import datetime, timedelta

from backend.core.rum_ingest import ingest_rum_logs

logger = logging.getLogger(__name__)


def _faro_purge_surrogate_key(logging_service_id: str, token: str) -> None:
    """Fire-and-forget surrogate-key purge on the logging service after a Faro bundle (re)upload.

    Purges on ``logging_service_id`` — NOT the CDN service fronting FOS for
    log downloads (F-1 audit finding). ``Surrogate-Key: rum-faro-sdk`` is
    set only in the RUM fetch-cache snippet, which is deployed to the
    instrumented *logging* service, never the CDN service — a purge
    targeting the CDN service can never match anything there. Mirrors the
    fire-and-forget convention of ``_purge_surrogate_key`` in
    ``backend.core.iceberg._core`` (which purges a different, unrelated key
    on the CDN service, where that IS the correct target): a missing
    service id/token or a purge failure must never surface — the FOS
    upload it follows already succeeded.
    """
    if not logging_service_id or not token:
        return
    try:
        from backend.core.fastly.client import fastly

        fastly("POST", f"/service/{logging_service_id}/purge/rum-faro-sdk", token=token, expect_empty=True)
    except Exception:
        logger.warning("Faro surrogate-key purge failed for %s (non-fatal)", logging_service_id, exc_info=True)


def _reconcile_faro_bundle(service_id: str, run_id: int | None) -> bool:
    """Keep the operator's pinned Faro bundle present and intact in FOS.

    Two deliberately different cadences: a cheap FOS HEAD every tick (catches
    a wiped bucket or an upload interrupted mid-write), and an upstream drift
    check throttled to once per ``faro_upstream_check_hours`` (catches unpkg
    re-releasing the same version string under the same tag) — the latter
    downloads a bundle from unpkg on every call, so running it every tick
    would mean an unpkg round trip per service per cron tick.

    This never bumps ``faro_version`` to a *different* version — upgrades are
    an explicit operator action. The one exception is adopting
    ``DEFAULT_FARO_VERSION`` for a service that is RUM-enabled but has no
    version pinned at all: since the tracker JS unconditionally loads the
    first-party ``/js/faro-sdk.js`` (no CDN fallback), an unpinned service has
    no bundle behind that path and would 404 for every visitor. That state
    only exists for services enabled before this default existed (self-heal,
    one-time — the next tick sees a pinned version and skips straight to the
    normal integrity check, so this never repeats or thrashes).

    That one-time self-heal also calls ``reconcile_vcl_state`` (F-4 audit
    finding): a service in this state was enabled before ``faro_version``
    existed, so its LIVE VCL was generated with ``faro_version=None`` — the
    ``/js/faro-sdk.js`` route, the SigV4 object-path rewrite, and the
    fetch-cache snippet are all absent from it. Uploading the bundle alone
    would leave it unreachable from any deployed route; the reconcile call
    is what actually makes ``/js/faro-sdk.js`` resolve. Every other branch
    below (integrity restore, upstream drift resync) re-uploads bytes for a
    version that was ALREADY pinned and reconciled when it was pinned, so
    those never need to touch VCL.

    Each ``download_and_upload_faro`` call below passes
    ``expected_current_version`` — a compare-and-set guard (F-5 audit
    finding) against an operator's concurrent ``upgrade_faro_version`` call
    moving the pin while this (possibly slow, unpkg-round-trip-bearing)
    call is in flight; see that function's docstring.

    Must never raise: wrapped end to end so a transient unpkg/FOS outage
    degrades to "bundle not refreshed this tick", never to "beacon ingest
    stopped." Not itself a cron run — it borrows the ingest run's run_id
    (if any) purely to surface progress messages; it starts no cron_runs
    row of its own.
    """
    import asyncio

    from backend import config as svcconfig
    from backend.core.faro_versions import DEFAULT_FARO_VERSION
    from backend.cron_progress import add_progress
    from backend.provision.rum_assets import (
        detect_faro_version_change,
        download_and_upload_faro,
        faro_bundle_intact,
    )

    def report(msg: str) -> None:
        logger.info(msg)
        if run_id:
            add_progress(run_id, {"type": "status", "message": msg})

    try:
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return True
        rum_cfg = cfg.get("rum")
        rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
        rum_enabled = svcconfig.is_rum_enabled(cfg)
        if not rum_enabled:
            return True
        token = cfg.get("fastly_api_key", "")

        pinned_version = rum_cfg.get("faro_version")
        if not pinned_version:
            # Self-heal: a service enabled before every enable path pinned a
            # version by default. Adopt the default rather than leave
            # /js/faro-sdk.js 404ing for every visitor.
            report(f"RUM enabled with no pinned Faro version; adopting default v{DEFAULT_FARO_VERSION}")
            try:
                asyncio.run(
                    download_and_upload_faro(service_id, DEFAULT_FARO_VERSION, token, expected_current_version=None)
                )
                _faro_purge_surrogate_key(service_id, token)
                report(f"Faro v{DEFAULT_FARO_VERSION} adopted and uploaded to FOS")
                pinned_version = DEFAULT_FARO_VERSION
                # Reload: download_and_upload_faro just persisted
                # faro_version/faro_content_hash/faro_fos_etag_md5 via its
                # own save_config call, which the stale in-memory cfg above
                # wouldn't reflect. Without this, the integrity check right
                # below would see no stored etag, treat the bundle it just
                # uploaded as "not intact", and re-upload it a second time
                # this same tick.
                cfg = svcconfig.load_config(service_id) or cfg
            except Exception as e:
                msg = str(e)
                if "403" in msg or "401" in msg or "unauthorized" in msg.lower():
                    logger.warning(
                        "Faro default-version adoption failed for %s (S3/FOS write credentials unauthorized: %s)",
                        service_id,
                        e,
                    )
                else:
                    logger.warning("Faro default-version adoption failed for %s", service_id, exc_info=True)
                return False

            # The bundle now exists in FOS, but the deployed VCL for this
            # service may still be missing the routes for it entirely (see
            # the docstring above) — reconcile now so /js/faro-sdk.js
            # actually resolves. Best-effort: a reconcile failure here must
            # not abort the bundle-ingest cron tick; the next tick retries
            # since pinned_version is now set but the live VCL wasn't
            # reconciled to match, so `rum_vcl_fingerprint`-based drift
            # detection (/rum/status) will keep reporting drift until an
            # operator-triggered reconcile (or a future tick, if this is
            # ever made retryable) succeeds.
            try:
                from backend.provision.declarative.reconciler import reconcile_vcl_state
                from backend.provision.rum_orchestrator_v2 import rum_vcl_fingerprint

                reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
                report(f"VCL reconciled: /js/faro-sdk.js now routes to v{DEFAULT_FARO_VERSION}")
                # #2 audit finding: refresh the stored fingerprint now that the
                # live VCL actually matches what the generator produces for
                # this pin — without this, /rum/status's vcl_drift would keep
                # reporting drift forever after this self-heal, even though
                # it just successfully reconciled. Mirrors the refresh
                # upgrade_faro_version does after its own activation.
                svcconfig.mutate_config(
                    service_id, lambda c: c.__setitem__("rum_vcl_sha", rum_vcl_fingerprint(service_id, c))
                )
            except Exception:
                logger.warning(
                    "Faro default-version VCL reconcile failed for %s "
                    "(bundle uploaded to FOS, but /js/faro-sdk.js may still 404 until the next successful reconcile)",
                    service_id,
                    exc_info=True,
                )

        # 1. Cheap integrity check — every tick.
        if not faro_bundle_intact(cfg, pinned_version):
            report(f"Faro bundle v{pinned_version} missing/corrupt in FOS, restoring")
            try:
                asyncio.run(
                    download_and_upload_faro(service_id, pinned_version, token, expected_current_version=pinned_version)
                )
                _faro_purge_surrogate_key(service_id, token)
                report(f"Faro bundle v{pinned_version} restored to FOS")
            except Exception as e:
                msg = str(e)
                if "403" in msg or "401" in msg or "unauthorized" in msg.lower():
                    logger.warning(
                        "Faro bundle restore failed for %s (S3/FOS write credentials unauthorized: %s)",
                        service_id,
                        e,
                    )
                else:
                    logger.warning("Faro bundle restore failed for %s", service_id, exc_info=True)
                return False

        # 2. Throttled upstream drift check. Reload cfg first in case the
        # restore above just rewrote faro_content_hash, so the timestamp
        # write below doesn't clobber it with a stale in-memory copy.
        cfg = svcconfig.load_config(service_id) or cfg
        rum_cfg = dict(cfg.get("rum") or {})
        last_check = rum_cfg.get("faro_last_upstream_check") or 0
        check_interval_s = rum_cfg.get("faro_upstream_check_hours", 24) * 3600
        now = time.time()

        if now - last_check >= check_interval_s:
            drift = False
            try:
                drift = asyncio.run(detect_faro_version_change(service_id, pinned_version))
            except Exception:
                logger.warning("Faro upstream drift check failed for %s", service_id, exc_info=True)
            finally:
                # Write the attempt timestamp even on failure — otherwise a
                # persistently failing upstream turns into a per-tick
                # download storm instead of once per window.
                svcconfig.mutate_config(
                    service_id,
                    lambda c: c.__setitem__("rum", {**(c.get("rum") or {}), "faro_last_upstream_check": now}),
                )

            if drift:
                report(f"Faro upstream drift detected for v{pinned_version}, re-syncing")
                try:
                    asyncio.run(
                        download_and_upload_faro(
                            service_id, pinned_version, token, expected_current_version=pinned_version
                        )
                    )
                    _faro_purge_surrogate_key(service_id, token)
                    report(f"Faro bundle v{pinned_version} re-synced from upstream")
                except Exception:
                    logger.warning("Faro upstream re-sync failed for %s", service_id, exc_info=True)
                    return False
        return True
    except Exception:
        logger.warning("Faro reconcile failed for %s", service_id, exc_info=True)
        return False


@cron_task("cron_rum_sync", job_name="rum_sync")
def _run_rum_sync(service_id: str, **kwargs) -> None:
    """Sync RUM beacon logs from FOS raw/rum/ into local DuckDB tables.

    Calls ingest_rum_logs generator which handles orphan-row safety internally.
    """
    # No start-of-tick active-request gate here (mirroring 5a7541fd): a deferred
    # tick waits a full interval, and dashboard/SSE polling kept it tripped.
    logger.info(f"RUM sync starting for {service_id}")

    is_manual = kwargs.get("is_manual", False) or kwargs.get("run_id") is not None
    pass_max_seconds = 20 if not is_manual else 240

    from backend.core import metadata
    from backend.cron.lifecycle import cron_run
    from backend.cron_progress import add_progress

    had_warning = False
    # The wrapper owns the lease + progress lifecycle; ingest_rum_logs reuses the
    # leased run_id and still writes its own terminal cron_runs rows.
    # The backstop flips a row ingest left in ``running`` (e.g. the body raised before
    # ingest_rum_logs wrote its terminal row) instead of waiting for the next tick's reap.
    with cron_run(
        service_id,
        "rum_sync",
        run_id=kwargs.get("run_id"),
        include_log_output=False,
        finalize_running_backstop=True,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        try:
            for event in ingest_rum_logs(service_id, max_seconds=pass_max_seconds, run_id=run_id):
                if event[0] == "started":
                    add_progress(run_id, {"type": "status", "message": f"RUM sync starting for {service_id}"})
                    if not _reconcile_faro_bundle(service_id, run_id):
                        had_warning = True
                elif event[0] == "file_done":
                    _, filename, count = event
                    msg = f"{filename}: {count} rows"
                    logger.debug(f"  {msg}")
                    add_progress(run_id, {"type": "status", "message": msg})
                elif event[0] == "error":
                    _, location, msg = event
                    logger.warning(f"  Error in {location}: {msg}")
                    add_progress(run_id, {"type": "error", "message": f"Error in {location}: {msg}"})
                elif event[0] == "cleanup_done":
                    _, cleanup_files, cleanup_bytes = event
                    msg = f"RUM cleanup deleted {cleanup_files} files, freed {cleanup_bytes / (1024 * 1024):.2f} MB"
                    logger.info(msg)
                    add_progress(run_id, {"type": "status", "message": msg})
                elif event[0] == "done":
                    _, total = event
                    msg = f"RUM sync complete: {total} total rows"
                    if had_warning:
                        msg += " (with warnings)"
                    logger.info(msg)
                    add_progress(run_id, {"type": "status", "message": msg})
                    if had_warning:
                        try:
                            con = metadata.get_con(service_id)
                            con.execute(
                                "UPDATE cron_runs SET status = 'warning' "
                                "WHERE id = ? AND service_id = ? AND status = 'success'",
                                (run_id, service_id),
                            )
                            con.commit()
                        except Exception:
                            logger.warning(
                                "Could not mark RUM cron run as warning",
                                extra={"service_id": service_id, "run_id": run_id},
                                exc_info=True,
                            )

                    if total > 0:
                        try:
                            from backend.cron.jobs.rum_commit import _run_rum_commit

                            logger.info(
                                "⚡ [rum_sync] %s: triggering post-sync RUM commit for %d new rows", service_id, total
                            )
                            _run_rum_commit(service_id)
                        except Exception as commit_err:
                            logger.warning("RUM post-sync commit failed for %s: %s", service_id, commit_err)

        except Exception as e:
            logger.error(f"RUM sync failed: {e}", exc_info=True)
            add_progress(run_id, {"type": "error", "message": f"RUM sync failed: {e}"})


# ── Celery/ledger-mode RUM ingest cron jobs (formerly backend.cron.jobs.rum_ledger) ──


@cron_task("cron_rum_discovery", job_name="rum_discovery")
def _run_rum_discovery_cron(service_id: str, run_id: int | None = None) -> None:
    """Discover new ``raw/rum/`` beacon files and dispatch batched
    ``convert_batch_rum_files`` calls covering them. Runs inline so the
    cron_runs row carries the real per-tick outcome instead of a fake instant
    success.
    """
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — RUM discovery refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    rum_cfg = cfg.get("rum")
    rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
    rum_enabled = svcconfig.is_rum_enabled(cfg)
    if not rum_enabled:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return
    if src.get("access_level") == "read_only":
        return
    if not svcconfig.is_high_scale_mode(src):
        return

    with cron_run(
        service_id,
        "rum_discovery",
        source=src,
        run_id=run_id,
        error_summary="RUM discovery failed",
        finalize_running_backstop=True,
        include_log_output=False,
        skip_log_prefix="rum_discovery",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id

        faro_ok = True
        faro_err = None
        try:
            faro_ok = _reconcile_faro_bundle(service_id, run_id)
        except Exception as fe:
            faro_ok = False
            faro_err = str(fe)
            logger.warning(
                "[rum_discovery] %s: Faro bundle reconcile failed (non-fatal): %s", service_id, fe, exc_info=True
            )

        if not svcconfig.CELERY_BROKER_URL:
            run.fail(
                "Celery RUM ingest misconfigured: no broker URL",
                error_message="DEPLOYMENT_MODE=high_scale requires CELERY_BROKER_URL",
            )
            return

        from backend.core.ingest import discover_rum_prefix
        from backend.provision.log_paths import rum_minute_list_prefix

        now = datetime.now(UTC)
        discovered = 0
        for i in range(5):
            prefix = rum_minute_list_prefix(now - timedelta(minutes=i))
            discovered += discover_rum_prefix(service_id, prefix_subpath=prefix, run_id=run_id)

        if not faro_ok:
            status = "warning"
            warn_msg = f"Faro bundle reconcile issue ({faro_err or 'drift/restore warning'})"
            summary = (
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers ({warn_msg})"
                if discovered
                else f"No new RUM files ({warn_msg})"
            )
            error_message = faro_err or "Faro bundle reconcile failed"
        else:
            status = "success"
            summary = (
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers"
                if discovered
                else "No new RUM files"
            )
            error_message = None

        run.record(status, summary=summary, error_message=error_message, files_downloaded=discovered)


def _run_ledger_sweep_impl(
    service_id: str,
    run_id: int | None,
    *,
    job_name: str,
    sweep_fn,
    label: str,
    require_rum: bool = False,
    finalize_running_backstop: bool = False,
    include_warning_error_message: bool = False,
) -> None:
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — %s refused.", service_id, label)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    if require_rum and not svcconfig.is_rum_enabled(cfg):
        return
    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_scale_mode(src) or src.get("access_level") == "read_only":
        return

    row_prefix = "RUM " if require_rum else ""
    with cron_run(
        service_id,
        job_name,
        source=src,
        run_id=run_id,
        error_summary=f"{label[0].upper()}{label[1:]} failed",
        finalize_duration=True,
        finalize_running_backstop=finalize_running_backstop,
        include_log_output=False,
    ) as run:
        if not run.ok:
            return
        run.log_progress({"type": "status", "message": f"Starting {label} for {service_id}..."})

        summary = sweep_fn(service_id, run_id=run.run_id)
        warnings = []
        if not summary.get("broker_ok", True):
            warnings.append("Celery broker/queue depth probe failed")
        if summary.get("dead_letter", 0) > 0:
            warnings.append(f"{summary['dead_letter']} dead-letter/quarantined {row_prefix}row(s)")
        run_status = "warning" if warnings else "success"

        summary_msg = (
            f"reclaimed={summary.get('reclaimed', 0)} redispatched={summary.get('redispatched', 0)} "
            f"discovered={summary.get('discovered', 0)}"
        )
        if warnings:
            summary_msg += f" ({'; '.join(warnings)})"

        record_kwargs: dict = {
            "summary": summary_msg,
            "files_downloaded": summary.get("discovered", 0),
        }
        if include_warning_error_message:
            record_kwargs["error_message"] = "; ".join(warnings) if warnings else None
        run.record(run_status, **record_kwargs)
        run.log_progress({"type": "done", "message": summary_msg})


@cron_task("ledger_sweep", job_name="ledger_sweep")
def _run_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net: reclaim stale ledger claims, re-dispatch stuck
    rows, and diff a lookback FOS LIST against the ledger."""
    from backend.core.ingest import sweep_ledger_once

    _run_ledger_sweep_impl(
        service_id,
        run_id,
        job_name="ledger_sweep",
        sweep_fn=sweep_ledger_once,
        label="ledger sweep",
    )


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net for the RUM ledger pipeline."""
    from backend.core.ingest import sweep_rum_ledger_once

    _run_ledger_sweep_impl(
        service_id,
        run_id,
        job_name="ledger_rum_sweep",
        sweep_fn=sweep_rum_ledger_once,
        label="RUM ledger sweep",
        require_rum=True,
        finalize_running_backstop=True,
        include_warning_error_message=True,
    )


# ── RUM commit job (folded from backend.cron.jobs.rum_commit) ────────────────


@cron_task("cron_rum_commit", job_name="rum_commit")
def _run_rum_commit(service_id: str, force: bool = False, run_id: int | None = None, **kwargs) -> None:
    """Compact RUM tables from DuckDB cache to Iceberg/FOS."""
    from backend import config as svcconfig
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service
    from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

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

    # No start-of-tick active-request gate here (mirroring 5a7541fd): a deferred
    # tick waits a full interval, and dashboard/SSE polling kept it tripped.
    from backend.core.duckdb import _cache_dir as _commit_cache_dir
    from backend.cron.lifecycle import cron_run
    from backend.cron.scheduler import _check_disk_space

    with cron_run(
        service_id,
        "rum_commit",
        source=src,
        run_id=run_id,
        error_summary="RUM commit failed",
        suppress_exceptions=False,
        finalize_running_backstop=True,
        include_log_output=False,
        skip_log_prefix="rum_commit",
    ) as run:
        if not run.ok:
            return

        ok, disk_msg = _check_disk_space(_commit_cache_dir(src), service_id, "rum_commit")
        if not ok:
            run.fail(f"RUM commit aborted: {disk_msg}", error_message=disk_msg)
            return

        boto3_token = _BOTO3_CALLER_HINT.set("rum_commit")
        try:
            logger.info(f"RUM commit starting for {service_id}")

            total_committed_vitals = 0
            total_committed_errors = 0
            vitals_err = None
            errors_err = None
            vitals_res = {}
            errors_res = {}

            # Commit client_vitals
            try:
                vitals_res = db_iceberg.commit_buffer(src, table_name="client_vitals")
                vitals_err = _buffer_result_error(vitals_res)
                if vitals_res.get("files_committed", 0) > 0:
                    total_committed_vitals = vitals_res.get("rows_committed", 0)
                    # Sync client_vitals view/metadata
                    db_iceberg.sync_data(src, table_name="client_vitals")
                if vitals_err:
                    logger.warning("[rum_commit] %s: client_vitals commit incomplete: %s", service_id, vitals_err)
            except Exception as e:
                vitals_err = "; ".join(part for part in (vitals_err, str(e)) if part)
                logger.warning("[rum_commit] %s: client_vitals commit failed: %s", service_id, e)

            # Commit client_errors
            try:
                errors_res = db_iceberg.commit_buffer(src, table_name="client_errors")
                errors_err = _buffer_result_error(errors_res)
                if errors_res.get("files_committed", 0) > 0:
                    total_committed_errors = errors_res.get("rows_committed", 0)
                    # Sync client_errors view/metadata
                    db_iceberg.sync_data(src, table_name="client_errors")
                if errors_err:
                    logger.warning("[rum_commit] %s: client_errors commit incomplete: %s", service_id, errors_err)
            except Exception as e:
                errors_err = "; ".join(part for part in (errors_err, str(e)) if part)
                logger.warning("[rum_commit] %s: client_errors commit failed: %s", service_id, e)

            # Raw RUM objects may only be deleted after both DuckLake commit paths
            # have completed successfully and their publication is durable.
            if vitals_err is None and errors_err is None:
                from backend.core.ingest import _mark_ledger_published

                _mark_ledger_published(service_id, rum=True)

                # Update/recompute precomputed RUM aggregates
                try:
                    from contextlib import closing
                    from datetime import UTC, datetime, timedelta

                    from backend.core.duckdb import get_connection, rum_source_for
                    from backend.core.iceberg import execute_with_stale_view_retry
                    from backend.core.iceberg._ducklake import _ducklake_attach
                    from backend.core.rollups.rum import (
                        backfill_missing_rum_aggregates,
                        recompute_rum_aggregates,
                    )

                    rum_src = rum_source_for(src)
                    with closing(get_connection(rum_src, read_only=False)) as rum_con:
                        # Attach standard lake catalog so standard client_vitals / client_errors views can resolve
                        try:
                            _ducklake_attach(rum_con, src, read_only=True)
                        except Exception as attach_err:
                            logger.warning(
                                "[rum_commit] %s: Failed to attach lake catalog to RUM connection: %s",
                                service_id,
                                attach_err,
                            )

                        # Find hours that had data in the last 2 hours to do a fast incremental recompute
                        # (48h on every 10s RUM commit caused 48 full client_vitals/client_errors scans per tick).
                        recent_hours = []
                        since = datetime.now(UTC) - timedelta(hours=2)
                        try:
                            res_v = rum_con.execute(
                                "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_vitals WHERE timestamp >= ?",
                                [since],
                            ).fetchall()
                            recent_hours.extend([r[0] for r in res_v if r[0]])
                        except Exception:
                            pass

                        try:
                            res_e = rum_con.execute(
                                "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_errors WHERE timestamp >= ?",
                                [since],
                            ).fetchall()
                            recent_hours.extend([r[0] for r in res_e if r[0]])
                        except Exception:
                            pass

                        target_hours = list(set(recent_hours)) if recent_hours else None
                        execute_with_stale_view_retry(
                            rum_con,
                            rum_src,
                            lambda c: recompute_rum_aggregates(c, service_id, hours=target_hours),
                            table_name="client_vitals",
                        )

                        # Also heal any historical hours (up to 30d) missing from rum_vitals_aggregates
                        # or lacking the synthetic 'total_beacons' row added in 3fcbcd27.
                        try:
                            execute_with_stale_view_retry(
                                rum_con,
                                rum_src,
                                lambda c: backfill_missing_rum_aggregates(c, service_id, lookback_days=30),
                                table_name="client_vitals",
                            )
                        except Exception as heal_err:
                            logger.warning(
                                "[rum_commit] %s: Failed to backfill missing RUM aggregate hours: %s",
                                service_id,
                                heal_err,
                            )

                        # Persist RUM freshness/count into the status doc so the
                        # request-ingest status refresh reads it cheaply instead of
                        # opening a nested RUM-lake connection (AGENTS.md Trap #39).
                        try:
                            from backend.core.rollups.rum import rum_count_and_latest

                            rum_cnt, rum_latest = execute_with_stale_view_retry(
                                rum_con, rum_src, rum_count_and_latest, table_name="client_vitals"
                            )
                            rum_latest_iso = (
                                rum_latest.isoformat()
                                if isinstance(rum_latest, datetime)
                                else (str(rum_latest) if rum_latest else None)
                            )
                            svcconfig.update_status(
                                service_id,
                                {
                                    "rum": {
                                        "latest_log_at": rum_latest_iso,
                                        "total_rows": int(rum_cnt or 0),
                                        "last_sync_at": datetime.now(UTC).isoformat(),
                                    }
                                },
                            )
                        except Exception as persist_err:
                            logger.warning("[rum_commit] %s: RUM freshness persist failed: %s", service_id, persist_err)
                except Exception as agg_err:
                    logger.warning(
                        "[rum_commit] %s: RUM aggregates update failed: %s", service_id, agg_err, exc_info=True
                    )

            # Also launch local compaction for BOTH tables with error logging
            try:
                import threading as _t

                from backend.core import local_compaction as _lc

                _t.Thread(
                    target=lambda: _lc.compact_local_partitions(src, table_name="client_vitals"),
                    name=f"local-compact-rum-vitals:{service_id}",
                    daemon=True,
                ).start()
                _t.Thread(
                    target=lambda: _lc.compact_local_partitions(src, table_name="client_errors"),
                    name=f"local-compact-rum-errors:{service_id}",
                    daemon=True,
                ).start()
            except Exception as lc_err:
                logger.warning("[rum_commit] %s: post-commit local compaction failed to launch: %s", service_id, lc_err)

            v_files = vitals_res.get("files_committed", 0)
            e_files = errors_res.get("files_committed", 0)
            total_rows = total_committed_vitals + total_committed_errors

            error_message: str | None = None
            if vitals_err and errors_err:
                status = "error"
                summary = f"RUM commit failed for both tables: vitals ({vitals_err}), errors ({errors_err})"
                error_message = summary
            elif vitals_err or errors_err:
                status = "warning"
                failed_tab = "vitals" if vitals_err else "errors"
                err_details = str(vitals_err or errors_err or "")
                summary = (
                    f"Partial RUM commit: {v_files} vitals ({total_committed_vitals} rows) "
                    f"and {e_files} errors ({total_committed_errors} rows); {failed_tab} failed: {err_details}"
                )
                error_message = err_details
            else:
                status = "success"
                summary = (
                    f"Committed {v_files} vitals files ({total_committed_vitals} rows) "
                    f"and {e_files} errors files ({total_committed_errors} rows)"
                )

            run.record(
                status,
                summary=summary,
                error_message=error_message,
                rows_ingested=total_rows,
            )
            logger.info("RUM commit complete: %s", summary)

            publish_sync_status_safe(service_id, "rum_commit")

        finally:
            _BOTO3_CALLER_HINT.reset(boto3_token)


_self_mod = _sys.modules[__name__]
for _alias in (
    "backend.cron.jobs._common",
    "backend.cron.jobs.commit",
    "backend.cron.jobs.rum_sync",
    "backend.cron.jobs.rum_ledger",
    "backend.cron.jobs.rum_commit",
):
    _sys.modules[_alias] = _self_mod
_jobs_pkg = _sys.modules.get("backend.cron.jobs")
if _jobs_pkg is not None:
    for _attr in ("_common", "commit", "rum_sync", "rum_ledger", "rum_commit"):
        setattr(_jobs_pkg, _attr, _self_mod)
_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.cron.jobs._common": "backend.cron.jobs.sync",
            "backend.cron.jobs.commit": "backend.cron.jobs.sync",
            "backend.cron.jobs.rum_sync": "backend.cron.jobs.sync",
            "backend.cron.jobs.rum_ledger": "backend.cron.jobs.sync",
            "backend.cron.jobs.rum_commit": "backend.cron.jobs.sync",
        }
    )
