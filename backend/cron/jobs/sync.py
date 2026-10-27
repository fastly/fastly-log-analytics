"""Ingest-class cron jobs.

  * ``_run_service_cron`` — per-tick ingest of new raw .gz files from FOS
    into the local buffer (does NOT commit to DuckLake).
  * ``_run_full_sweep`` — daily catch-net that LISTs the full raw/request/ prefix
    to pick up late-arriving files outside the incremental window.
  * ``_run_gap_heal`` — periodic gap detector that triggers a full sweep
    when sustained loss is observed between Fastly stats and our ingest.

The gap-heal trigger reads its collaborators (``_run_full_sweep`` and the
throttle-marker helpers) from module globals at call time, so tests patch
them at ``backend.cron.jobs.sync.<name>``.
"""

from __future__ import annotations

import importlib as _importlib
import logging
import os
import sys as _sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from backend import config as svcconfig
from backend.core.rum_ingest import ingest_rum_logs  # noqa: F401
from backend.core.storage.backup import schedule_cloud_push  # noqa: F401
from backend.cron.jobs import _common
from backend.cron.jobs._common import (
    _INGEST_ERROR_OUTCOME_KEYS,  # noqa: F401
    _has_ingest_outcome_errors,
    _orig_finalize_cron_duration,
    _orig_finish_post_ingest_view_and_rollups,
    _orig_ingest_with_adaptive_followups,
    _orig_publish_sync_status_safe,
    _orig_refresh_view_and_warm_pool,
    _orig_spawn_local_compaction,
    _refuse_if_dev_no_crons,
    _resolve_sync_hook,
)
from backend.cron.scheduler import (
    _check_buffer_backlog,  # noqa: F401
    _check_disk_space,
    _claim_heavy_refresh,
    _display_label,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
    cron_run,
    cron_task,
    dev_mode_no_crons,  # noqa: F401
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

if TYPE_CHECKING:
    from backend.cron.jobs._common import (
        _buffer_result_error as _buffer_result_error,
    )
    from backend.cron.jobs._common import (
        _finish_post_ingest_view_and_rollups as _finish_post_ingest_view_and_rollups,
    )
    from backend.cron.jobs._common import (
        _ingest_with_adaptive_followups as _ingest_with_adaptive_followups,
    )
    from backend.cron.jobs._common import (
        _refresh_status_after_commit as _refresh_status_after_commit,
    )
    from backend.cron.jobs._common import (
        _spawn_local_compaction as _spawn_local_compaction,
    )
    from backend.cron.jobs._common import (
        apply_time_range_pinning as apply_time_range_pinning,
    )
    from backend.cron.jobs._common import (
        finalize_cron_duration as finalize_cron_duration,
    )
    from backend.cron.jobs._common import (
        get_service_display_label as get_service_display_label,
    )
    from backend.cron.jobs._common import (
        has_sufficient_disk_space as has_sufficient_disk_space,
    )
    from backend.cron.jobs._common import (
        publish_sync_status_safe as publish_sync_status_safe,
    )
    from backend.cron.jobs._common import (
        refresh_view_and_warm_pool as refresh_view_and_warm_pool,
    )
    from backend.cron.jobs.commit import _run_commit as _run_commit
    from backend.cron.jobs.rum_commit import _run_rum_commit as _run_rum_commit
    from backend.cron.jobs.rum_ledger import (
        _run_ledger_sweep as _run_ledger_sweep,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_ledger_sweep_impl as _run_ledger_sweep_impl,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_rum_discovery_cron as _run_rum_discovery_cron,
    )
    from backend.cron.jobs.rum_ledger import (
        _run_rum_ledger_sweep as _run_rum_ledger_sweep,
    )
    from backend.cron.jobs.rum_sync import (
        _faro_purge_surrogate_key as _faro_purge_surrogate_key,
    )
    from backend.cron.jobs.rum_sync import (
        _reconcile_faro_bundle as _reconcile_faro_bundle,
    )
    from backend.cron.jobs.rum_sync import (
        _run_rum_sync as _run_rum_sync,
    )

logger = logging.getLogger("backend.scheduler")


# ── _run_service_cron (per-tick ingest) ──────────────────────────────────────


_rollup_lock = threading.Lock()
_rollup_pending: dict[str, set[str]] = {}
_rollup_sources: dict[str, dict[str, Any]] = {}
_rollup_workers: dict[str, threading.Thread] = {}


def _post_ingest_rollup_worker(service_id: str) -> None:
    from backend.core import rollups
    from backend.utils.telemetry import yield_to_api

    while True:
        with _rollup_lock:
            hours = _rollup_pending.pop(service_id, set())
            src = _rollup_sources.get(service_id)
            if not hours or src is None:
                _rollup_workers.pop(service_id, None)
                return
        # Wellknown-bots rollup is best-effort: the security reader has a
        # live-SQL fallback for any hour without one.
        for fn, label in (
            (rollups.recompute_touched_hours, "rollup recompute"),
            (rollups.recompute_wellknown_bots_rollup, "bot rollup"),
        ):
            try:
                yield_to_api()
                fn(service_id, src, hours)
            except Exception as e:
                logger.warning("[scheduler] %s: post-sync %s failed: %s", service_id, label, e)


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
            target=_post_ingest_rollup_worker, args=(service_id,), name=f"post-ingest-rollups:{service_id}", daemon=True
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

    from backend.core.duckdb import _cache_dir, get_source_for_service, refresh_config_status
    from backend.core.ingest import _new_object_outcome, ingest

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        logger.warning("[scheduler] %s: config not found, skipping.", service_id)
        return
    src = get_source_for_service(service_id)
    if src is None:
        logger.warning("[scheduler] %s: source not found, skipping.", service_id)
        return
    sync_cfg = cfg.get("provisioning", {}).get("cron_sync", {})
    sync_enabled = sync_cfg.get("enabled", True)
    delete_after = svcconfig.resolve_raw_delete_after(cfg, delete_after)
    _display = _common.get_service_display_label(service_id, cfg)

    # ── 1. Ingest ─────────────────────────────────────────────────────────────
    if sync_enabled or force:
        is_manual = run_id is not None
        start_time, _ = _common.apply_time_range_pinning(
            service_id, cfg, src, start_time=start_time, is_manual=is_manual, fallback_to_created_at=True
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
                from backend.core.ingest import discover_prefix
                from backend.provision.cli import minute_list_prefix

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
                                service_id, prefix_subpath=minute_list_prefix(now - timedelta(minutes=i)), run_id=run_id
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
            processed_files = inserted_rows = corrupt_rows = published_pass_rows = 0
            max_incremental_files = int(os.environ.get("INGEST_INCREMENTAL_MAX_FILES", "250"))
            pass_max_files = max_incremental_files if not is_manual else 5000
            pass_max_seconds = 20 if not is_manual else 240

            def _publish_pass_rows() -> None:
                nonlocal published_pass_rows
                refresh_fn = _resolve_sync_hook(
                    "refresh_view_and_warm_pool",
                    _common.refresh_view_and_warm_pool,
                    _orig_refresh_view_and_warm_pool,
                )
                refresh_fn(
                    src,
                    service_id,
                    log_prefix=f"{elapsed()} [adaptive] ",
                    progress_log=lambda ev: _log_and_add_progress(
                        run_id, service_id, job_name="log_discovery", event=ev
                    ),
                )
                publish_fn = _resolve_sync_hook(
                    "publish_sync_status_safe", _common.publish_sync_status_safe, _orig_publish_sync_status_safe
                )
                publish_fn(service_id, "scheduler")
                published_pass_rows = inserted_rows

            try:
                adaptive_fn = _resolve_sync_hook(
                    "_ingest_with_adaptive_followups",
                    _common._ingest_with_adaptive_followups,
                    _orig_ingest_with_adaptive_followups,
                )
                for event in adaptive_fn(
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
                        summary = "Ingestion failed" + (
                            f" after processing {processed_files} files ({inserted_rows} rows)"
                            if processed_files > 0
                            else ""
                        )
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
                            summary = (
                                f"No new files; reclaimed {reclaimed} raw file(s) left by an interrupted prior run"
                                if reclaimed
                                else "No new log files found in bucket"
                            )
                            done_msg = f"{elapsed()} {summary}."
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
                            summary = f"Ingested {done_event.get('new_files', 0)} files, {done_event.get('rows_inserted', 0)} rows."
                            if done_event.get("corrupt_rows"):
                                summary += f" Skipped {done_event.get('corrupt_rows')} corrupted/invalid lines."
                            if done_event.get("quarantined_files"):
                                summary += f" Quarantined {done_event.get('quarantined_files')} files with errors."
                            if done_event.get("deleted_files"):
                                summary += f" Deleted {done_event.get('deleted_files')} raw files."
                            corrupt_details = done_event.get("corrupt_details", [])
                            outcome_counters = done_event.get("outcome_counters")
                            run.record(
                                "error" if _has_ingest_outcome_errors(outcome_counters) else "success",
                                summary=summary,
                                error_message="\n".join(corrupt_details) if corrupt_details else None,
                                files_downloaded=done_event.get("new_files", 0),
                                files_deleted_fos=done_event.get("deleted_files", 0),
                                rows_ingested=done_event.get("rows_inserted", 0),
                                corrupt_rows=done_event.get("corrupt_rows", 0),
                                outcome_counters=outcome_counters,
                            )
                            already_published = bool(
                                published_pass_rows > 0 and published_pass_rows == done_event.get("rows_inserted", 0)
                            )
                            finish_fn = _resolve_sync_hook(
                                "_finish_post_ingest_view_and_rollups",
                                _common._finish_post_ingest_view_and_rollups,
                                _orig_finish_post_ingest_view_and_rollups,
                            )
                            if done_event.get("rows_inserted", 0) > 0 and not already_published:
                                finish_fn(
                                    src,
                                    service_id,
                                    run_id,
                                    "log_discovery",
                                    {**done_event, "touched_hours": []},
                                    elapsed,
                                )
                            if done_event.get("new_files", 0) > 1:
                                spawn_compact_fn = _resolve_sync_hook(
                                    "_spawn_local_compaction",
                                    _common._spawn_local_compaction,
                                    _orig_spawn_local_compaction,
                                )
                                spawn_compact_fn(src, service_id, compact_active_hour=True, refresh_on_merge=True)
                            if done_event.get("touched_hours"):
                                finish_fn(
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
    if bool(done_event.get("rows_inserted", 0) > 0 or do_heavy_refresh or force):
        if (sync_enabled or force) and run_id is not None:
            _progress_status(
                f"Refreshing sync status {'+ filter suggestions' if do_heavy_refresh else '(header only)'}..."
            )
        _t0 = time.time()
        try:
            refresh_config_status(service_id, include_top_values=False)
        except Exception:
            pass
        publish_fn = _resolve_sync_hook(
            "publish_sync_status_safe", _common.publish_sync_status_safe, _orig_publish_sync_status_safe
        )
        publish_fn(service_id, "scheduler")
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
        from backend.repositories.dashboard import DASHBOARD_CACHE_TTL, _dashboard_cache, invalidate_service

        if DASHBOARD_CACHE_TTL > 0:
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

    if do_heavy_refresh:
        _t0 = time.time()
        try:
            from backend.core.duckdb import backfill_fastly_edge_writes, reconcile_fastly_stats
            from backend.utils.telemetry import run_usage_log_cleanup

            for fn, msg_fn, log_label in (
                (
                    lambda: backfill_fastly_edge_writes(src),
                    lambda n: f"Backfilled {n} Fastly-edge PUTs to usage log",
                    "backfill",
                ),
                (
                    lambda: reconcile_fastly_stats(src, hours_back=26),
                    lambda n: f"Reconciled {n} hourly Fastly stats gap(s)",
                    "Fastly stats reconciliation",
                ),
            ):
                try:
                    if count := fn():
                        if run_id is not None:
                            _log_and_add_progress(
                                run_id,
                                service_id,
                                job_name="usage_log",
                                event={"type": "status", "message": msg_fn(count)},
                            )
                        else:
                            logger.info("[usage_log] %s: %s", service_id, msg_fn(count))
                except Exception as e:
                    logger.warning("[usage_log] %s failed for %s: %s", log_label, service_id, e)
            run_usage_log_cleanup(service_id)
        except Exception as e:
            logger.warning("[scheduler] %s: usage_log phase failed: %s", service_id, e)
        if run_id is not None:
            _progress_status(f"usage_log phase: {int((time.time() - _t0) * 1000)}ms")

    # ── 5. Final duration record ──────────────────────────────────────────────
    if sync_enabled or force:
        finalize_fn = _resolve_sync_hook(
            "finalize_cron_duration", _common.finalize_cron_duration, _orig_finalize_cron_duration
        )
        finalize_fn(
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

    from backend.core.duckdb import get_source_for_service
    from backend.core.ingest import ingest
    from backend.utils.telemetry import should_defer_cron

    cfg = svcconfig.load_config(service_id) or {}
    delete_after = svcconfig.resolve_raw_delete_after(cfg)
    src = get_source_for_service(service_id)
    if src is None or (not force and should_defer_cron("full_sync", service_id)):
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
        effective_max_files, effective_max_seconds = max_files, max_seconds

        if max_files == _FULL_SWEEP_DEFAULT_MAX_FILES:
            if configured_max_files is not None:
                effective_max_files = int(configured_max_files)
            elif svcconfig.is_high_scale_mode(src):
                try:
                    from backend.celery_app import celery_queue_depths

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
                    from backend.core import storage as db_storage

                    buf_files = int(db_storage.buffer_backlog_stats(src).get("file_count", 0) or 0)
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

        processed_files = inserted_rows = corrupt_rows = 0
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
            outcome_counters = done_event.get("outcome_counters")
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
            run.record(
                "error" if _has_ingest_outcome_errors(outcome_counters, corrupt_rows_cnt) else "success",
                summary=summary,
                error_message="\n".join(corrupt_details) if corrupt_details else None,
                files_downloaded=new_files,
                files_deleted_fos=reclaimed,
                rows_ingested=rows,
                corrupt_rows=corrupt_rows_cnt,
                outcome_counters=outcome_counters,
            )
            _log_and_add_progress(run_id, service_id, job_name="full_sync", event={"type": "done", "message": summary})
            finish_fn = _resolve_sync_hook(
                "_finish_post_ingest_view_and_rollups",
                _common._finish_post_ingest_view_and_rollups,
                _orig_finish_post_ingest_view_and_rollups,
            )
            finish_fn(src, service_id, run_id, "full_sync", done_event, elapsed)
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
    _GapHealSeverityBand("critical", 0.80, 500_000, 0.0, 100_000, 1800),
    _GapHealSeverityBand("severe", 0.50, 100_000, 0.25, 50_000, 1500),
    _GapHealSeverityBand("elevated", 0.10, 10_000, 2.0, _FULL_SWEEP_DEFAULT_MAX_FILES, _FULL_SWEEP_DEFAULT_MAX_SECONDS),
    _GapHealSeverityBand(
        "mild", 0.0, 0, GAP_HEAL_THROTTLE_HOURS, _FULL_SWEEP_DEFAULT_MAX_FILES, _FULL_SWEEP_DEFAULT_MAX_SECONDS
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
def _run_gap_heal(service_id: str, run_id: int | None = None, force: bool = False) -> None:
    """Periodic gap detector that triggers a full_sweep when sustained loss is observed."""
    if _refuse_if_dev_no_crons(service_id, "gap_heal"):
        return

    from backend.core.duckdb import get_source_for_service
    from backend.utils.telemetry import should_defer_cron

    src = get_source_for_service(service_id)
    if src is None or (not force and should_defer_cron("gap_heal", service_id)):
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
                        run_id, service_id, job_name="gap_heal", event={"type": "done", "message": summary}
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
            _run_full_sweep(service_id, max_files=band.sweep_max_files, max_seconds=band.sweep_max_seconds, force=force)
        except Exception as e:
            run.fail("Gap-heal evaluation crashed", error_message=str(e))
            logger.exception("[gap_heal] %s: unexpected error", service_id)


_CacheRegistry.register("cron.jobs.sync._GAP_HEAL_LAST_TRIGGER", _GAP_HEAL_LAST_TRIGGER)


_DELEGATED_EXPORTS: dict[str, str] = {
    "_buffer_result_error": "backend.cron.jobs._common",
    "_finish_post_ingest_view_and_rollups": "backend.cron.jobs._common",
    "_ingest_with_adaptive_followups": "backend.cron.jobs._common",
    "_refresh_status_after_commit": "backend.cron.jobs._common",
    "_spawn_local_compaction": "backend.cron.jobs._common",
    "apply_time_range_pinning": "backend.cron.jobs._common",
    "finalize_cron_duration": "backend.cron.jobs._common",
    "get_service_display_label": "backend.cron.jobs._common",
    "has_sufficient_disk_space": "backend.cron.jobs._common",
    "publish_sync_status_safe": "backend.cron.jobs._common",
    "refresh_view_and_warm_pool": "backend.cron.jobs._common",
    "_run_commit": "backend.cron.jobs.commit",
    "_faro_purge_surrogate_key": "backend.cron.jobs.rum_sync",
    "_reconcile_faro_bundle": "backend.cron.jobs.rum_sync",
    "_run_rum_sync": "backend.cron.jobs.rum_sync",
    "_run_rum_discovery_cron": "backend.cron.jobs.rum_ledger",
    "_run_ledger_sweep_impl": "backend.cron.jobs.rum_ledger",
    "_run_ledger_sweep": "backend.cron.jobs.rum_ledger",
    "_run_rum_ledger_sweep": "backend.cron.jobs.rum_ledger",
    "_run_rum_commit": "backend.cron.jobs.rum_commit",
}


class _SyncModule(_sys.modules[__name__].__class__):  # type: ignore[misc]
    _orig_delegated: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if (mod_name := _DELEGATED_EXPORTS.get(name)) is not None:
            return getattr(_importlib.import_module(mod_name), name)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        if (mod_name := _DELEGATED_EXPORTS.get(name)) is not None:
            mod = _importlib.import_module(mod_name)
            if name not in self._orig_delegated:
                self._orig_delegated[name] = getattr(mod, name)
            setattr(mod, name, value)
            if value is self._orig_delegated.get(name):
                self._orig_delegated.pop(name, None)
            self.__dict__.pop(name, None)
            return
        super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        if (mod_name := _DELEGATED_EXPORTS.get(name)) is not None and name in self._orig_delegated:
            mod = _importlib.import_module(mod_name)
            setattr(mod, name, self._orig_delegated.pop(name))
            self.__dict__.pop(name, None)
            return
        super().__delattr__(name)


_sys.modules[__name__].__class__ = _SyncModule
