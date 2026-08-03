"""Ingest-class cron jobs.

  * ``_run_service_cron`` — per-tick ingest of new raw .gz files from FOS
    into the local buffer (does NOT commit to Iceberg).
  * ``_run_full_sweep`` — daily catch-net that LISTs the full raw/ prefix
    to pick up late-arriving files outside the incremental window.
  * ``_run_gap_heal`` — periodic gap detector that triggers a full sweep
    when sustained loss is observed between Fastly stats and our ingest.

The gap-heal trigger reads its collaborators (``_run_full_sweep`` and the
throttle-marker helpers) from module globals at call time, so tests patch
them at ``backend.cron.jobs.sync.<name>``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    _check_disk_space,
    _claim_heavy_refresh,
    _display_label,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
)

logger = logging.getLogger("backend.scheduler")


# ── _run_service_cron (per-tick ingest) ──────────────────────────────────────


@cron_task("cron_sync")
def _run_service_cron(
    service_id: str,
    force: bool = False,
    delete_after: bool | None = None,
    run_id: int | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> None:
    """Ingest new raw .gz files from FOS into the local buffer.

    Does NOT commit to Iceberg — that is handled by the separate commit_{id} job
    so ingest cadence and cloud-freshness can be tuned independently.
    """
    from backend.cron.scheduler import dev_mode_no_crons

    # Defense-in-depth: even if a job somehow gets scheduled OR a manual
    # /admin/ingest-logs call comes in with force=True, refuse to ingest
    # when the dev kill switch is set. force=True intentionally bypasses
    # the provisioning.cron_sync.enabled config — this env var beats both.
    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — sync refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import (
        finalize_cron_run_if_running,
        get_source_for_service,
        log_cron_run,
        refresh_config_status,
        start_cron_run,
    )
    from backend.core.ingest import ingest
    from backend.utils.active_requests import should_defer_cron

    # Active-request gate (perf #84): defer the sync tick if API requests
    # are in flight — DuckDB pool slot + Fastly bandwidth contention. Bound
    # by a 30 s starvation guard so a sustained-traffic service still gets
    # ticks eventually.
    if should_defer_cron("sync", service_id):
        return

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

    if delete_after is None:
        delete_after = sync_cfg.get("delete_after", True)

    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id

    # ── 1. Ingest ─────────────────────────────────────────────────────────────
    if sync_enabled or force:
        # For manual runs (run_id is not None), we ignore the default limit unless
        # it was explicitly passed in.
        is_manual = run_id is not None

        if not start_time and not is_manual:
            tr = prov.get("time_range")
            if tr and tr.get("start"):
                start_time = tr["start"]
                logger.info("[scheduler] %s: Using configured start_time limit: %s", service_id, start_time)
            # time_range.end is intentionally NOT re-applied here. It is only used for
            # the initial import or an explicit manual backfill. Applying it every cron
            # run would permanently freeze ingestion at the original import end date.
        elif is_manual and not start_time:
            # Manual "Sync All": clear any previously pinned range
            prov = cfg.get("provisioning", {})
            if "time_range" in prov:
                del prov["time_range"]
                cfg["provisioning"] = prov
                svcconfig.save_config(service_id, cfg)
                src["time_range"] = None
                logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

        try:
            if run_id is None:
                run_id = start_cron_run(src, "sync")
        except RuntimeError as e:
            logger.info("[scheduler] %s: skipping sync — %s", service_id, str(e))
            return

        # Disk pre-check: refuse to start if free space is below the floor.
        # Avoids the "pull from FOS, write fails, repeat next tick" cost loop.
        from backend.core.duckdb import _cache_dir

        ok, disk_msg = _check_disk_space(_cache_dir(src), service_id, "sync")
        if not ok:
            log_cron_run(
                src,
                "sync",
                0.0,
                "error",
                run_id=run_id,
                error_message=disk_msg,
                summary=f"Sync aborted: {disk_msg}",
            )
            return

        from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

        cleanup_progress_and_reap()
        start_progress(run_id, service_id=service_id, task="sync")
        logger.info("▶️  \x1b[94m[sync]\x1b[0m %s: Sync job started.", _display)

        start_time_exec = time.time()

        def elapsed() -> str:
            return _elapsed_since(start_time_exec)

        msg = "Starting sync..."
        if start_time or end_time:
            msg += f" (Range: {start_time or 'Start'} to {end_time or 'End'})"
        _log_and_add_progress(
            run_id, service_id, job_name="sync", event={"type": "status", "message": f"{elapsed()} {msg}"}
        )

        done_event: dict = {}
        processed_files = 0
        inserted_rows = 0
        corrupt_rows = 0

        try:
            for event in ingest(
                source=src,
                delete_after=delete_after,
                max_files=5000,
                max_seconds=240,
                start_time=start_time,
                end_time=end_time,
                incremental_only=not is_manual,
            ):
                _log_and_add_progress(run_id, service_id, job_name="sync", event=event)

                if event.get("type") == "file_done":
                    processed_files = event.get("current", processed_files)
                    inserted_rows = event.get("total_inserted", inserted_rows)
                    corrupt_rows = event.get("total_corrupt", corrupt_rows)
                elif event.get("type") == "done":
                    done_event = event
                elif event.get("type") == "error":
                    summary = "Ingestion failed"
                    if processed_files > 0:
                        summary += f" after processing {processed_files} files ({inserted_rows} rows)"
                    log_text = _extract_log_text(run_id)
                    log_cron_run(
                        src,
                        "sync",
                        time.time() - start_time_exec,
                        "error",
                        run_id=run_id,
                        error_message=event.get("message"),
                        summary=summary,
                        files_downloaded=processed_files,
                        rows_ingested=inserted_rows,
                        corrupt_rows=corrupt_rows,
                        log_output=log_text,
                    )
                    _log_and_add_progress(
                        run_id, service_id, job_name="sync", event={"type": "error", "message": event.get("message")}
                    )
                    break
            else:
                if done_event:
                    log_text = _extract_log_text(run_id)
                    if done_event.get("new_files", 0) == 0:
                        # The reconcile can reclaim strands (raw files left by an
                        # interrupted prior run) on an otherwise-idle tick — surface
                        # that count + message so the self-heal is visible in history.
                        reclaimed = done_event.get("deleted_files", 0)
                        if reclaimed:
                            summary = (
                                f"No new files; reclaimed {reclaimed} raw file(s) left by an interrupted prior run"
                            )
                            done_msg = f"{elapsed()} {summary}."
                        else:
                            summary = "No new log files found in bucket"
                            done_msg = f"{elapsed()} No new log files found in bucket."
                        log_cron_run(
                            src,
                            "sync",
                            time.time() - start_time_exec,
                            "success",
                            summary=summary,
                            files_deleted_fos=reclaimed,
                            run_id=run_id,
                            log_output=log_text,
                        )
                        _log_and_add_progress(
                            run_id,
                            service_id,
                            job_name="sync",
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

                        log_cron_run(
                            src,
                            "sync",
                            time.time() - start_time_exec,
                            "success",
                            files_downloaded=done_event.get("new_files", 0),
                            files_deleted_fos=done_event.get("deleted_files", 0),
                            rows_ingested=done_event.get("rows_inserted", 0),
                            corrupt_rows=done_event.get("corrupt_rows", 0),
                            summary=summary,
                            error_message=corrupt_message,
                            run_id=run_id,
                            log_output=log_text,
                        )

                        # Republish the persistent DuckDB view so dashboard reads pick
                        # up the buffer parquets we just wrote. Dashboard reads use
                        # read_only=True + skip_view_update=True (commit 19dfffc) and
                        # never refresh the view themselves. The only other writer-side
                        # update_iceberg_view caller is metadata_sync, which runs right
                        # after commit_buffer drains the buffer — so without this hop,
                        # the view is always republished buffer-less and dashboard lag
                        # is bounded by commit_interval_mins instead of the sync
                        # cadence. CREATE OR REPLACE VIEW is metadata-only (no cloud
                        # reads), so this is cheap.
                        if done_event.get("rows_inserted", 0) > 0:
                            from backend.cron.jobs._common import refresh_view_and_warm_pool
                            from backend.utils.active_requests import yield_to_api

                            # Cooperative yield before each post-ingest CPU stage
                            # so a concurrent dashboard query gets a chance to
                            # finish. Each stage (view refresh, rollup recompute,
                            # bot rollup) is CPU-bound and would otherwise pile
                            # straight onto whatever sync just spent in ingest.
                            yield_to_api()

                            refresh_view_and_warm_pool(
                                src,
                                service_id,
                                log_prefix=f"{elapsed()} ",
                                progress_log=lambda ev: _log_and_add_progress(
                                    run_id, service_id, job_name="sync", event=ev
                                ),
                            )

                        touched_hours = done_event.get("touched_hours", [])
                        if touched_hours:
                            from backend.utils.active_requests import yield_to_api

                            _t_roll = time.time()
                            try:
                                from backend.core.rollups import recompute_touched_hours

                                yield_to_api()
                                recompute_touched_hours(service_id, src, set(touched_hours))
                                _log_and_add_progress(
                                    run_id,
                                    service_id,
                                    job_name="sync",
                                    event={
                                        "type": "status",
                                        "message": f"{elapsed()} Rollups computed: {int((time.time() - _t_roll) * 1000)}ms",
                                    },
                                )
                            except Exception as _re:
                                logger.warning(
                                    "[scheduler] %s: post-sync rollup recompute failed: %s",
                                    service_id,
                                    _re,
                                )

                            # Wellknown-bots rollup: pre-materialises the
                            # 500-pattern UA-regex pre-filter that the
                            # /api/security/aggregates wellknown_bots block
                            # would otherwise re-run on the full window on
                            # every request. Best-effort — the security
                            # reader has a live-SQL fallback for any hour
                            # that lacks a rollup, so a failure here only
                            # forgoes the optimisation, not correctness.
                            _t_bot = time.time()
                            try:
                                from backend.core.rollups import recompute_wellknown_bots_rollup

                                yield_to_api()
                                _bn = recompute_wellknown_bots_rollup(service_id, src, set(touched_hours))
                                if _bn:
                                    _log_and_add_progress(
                                        run_id,
                                        service_id,
                                        job_name="sync",
                                        event={
                                            "type": "status",
                                            "message": f"{elapsed()} Bot rollups: {_bn} hours in "
                                            f"{int((time.time() - _t_bot) * 1000)}ms",
                                        },
                                    )
                            except Exception as _be:
                                logger.warning(
                                    "[scheduler] %s: post-sync bot rollup failed: %s",
                                    service_id,
                                    _be,
                                )

        except Exception as e:
            log_text = _extract_log_text(run_id)
            summary = "Ingestion crashed"
            if processed_files > 0:
                summary += f" after processing {processed_files} files ({inserted_rows} rows)"
                _log_and_add_progress(
                    run_id,
                    service_id,
                    job_name="sync",
                    event={
                        "type": "status",
                        "message": f"Crash occurred. Successfully ingested {processed_files} files so far.",
                    },
                )
            log_cron_run(
                src,
                "sync",
                time.time() - start_time_exec,
                "error",
                files_downloaded=processed_files,
                rows_ingested=inserted_rows,
                corrupt_rows=corrupt_rows,
                error_message=str(e),
                summary=summary,
                run_id=run_id,
                log_output=log_text,
            )
            logger.exception("[scheduler] %s: unexpected ingest error.", service_id)
            _log_and_add_progress(run_id, service_id, job_name="sync", event={"type": "error", "message": str(e)})
        finally:
            end_progress(run_id)
            # Backstop: guarantee the cron_runs row never stays 'running'. The
            # branches above finalize it on done/error, but if ingest() returns
            # without a terminal 'done' event the for/else leaves done_event
            # empty and NOTHING logs a status — the row leaks as 'running' and
            # the start_cron_run guard then skips every subsequent sync tick
            # until the orphan cutoff. That froze prod ingestion ~20 min on
            # 2026-06-19. Idempotent: no-op once the row is already terminal.
            finalize_cron_run_if_running(
                src,
                "sync",
                run_id,
                duration_s=time.time() - start_time_exec,
                summary="Sync exited without recording a terminal status",
                error_message="orphaned run auto-finalized (no terminal ingest event)",
            )

    # Push a fresh snapshot to any connected SSE subscribers (admin browsers
    # watching the header badge) BEFORE the slow full-table refresh below.
    # compute_sync_status_cached's SQLite overlay already has the latest
    # ingested-file timestamp + row count cheaply, so the live "Latest Log"
    # badge shouldn't wait on refresh_config_status's uncached parquet
    # rescan (skip_fos=False, force=True), which runs 10-25+s on a busy
    # service and is serialized behind max_instances=1 — badge updates were
    # arriving a full tick or more after the sync that produced them, well
    # after "Last Sync" (fed by the fast cron-runs event) had already
    # flipped. Same payload shape as GET /api/sync-status?skip_fos=true so
    # the React Query cache update is byte-compatible with a polled
    # response. Broad except so a publish failure NEVER breaks the
    # ingestion cron.
    try:
        from backend.routers.admin.sync_status import compute_sync_status_cached
        from backend.sync_status_publisher import publisher as _sync_status_publisher

        _snapshot = compute_sync_status_cached(service_id)
        if _snapshot is not None:
            _sync_status_publisher.publish(service_id, _snapshot)
    except Exception:
        logger.exception("[scheduler] %s: sync-status SSE publish failed", service_id)

    # ── 2. Refresh cached status ──────────────────────────────────────────────
    # Single 60s window covers both the heavy refresh (top_values cache) and
    # the heavy usage-log phase (reconcile_fastly_stats) — claim once per tick
    # and share the verdict so they don't drift relative to each other.
    do_heavy_refresh = _claim_heavy_refresh(service_id) or bool(force)
    if (sync_enabled or force) and run_id is not None:
        _msg_suffix = "+ filter suggestions" if do_heavy_refresh else "(header only)"
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} Refreshing sync status {_msg_suffix}...",
            },
        )
    _t0 = time.time()
    try:
        refresh_config_status(service_id, include_top_values=do_heavy_refresh)
    except Exception:
        pass
    if run_id is not None:
        _heavy = " (heavy)" if do_heavy_refresh else ""
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} refresh_config_status{_heavy}: {int((time.time() - _t0) * 1000)}ms",
            },
        )

    # ── 3. Invalidate dashboard cache ─────────────────────────────────────────
    # Gate on DASHBOARD_CACHE_TTL > 0: when the TTL is 0 (the current
    # default), the cache is never written, so the iteration is always
    # over an empty list. Skip the import + work entirely so the cron
    # tick doesn't pay for permanently-no-op accounting.
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
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} dashboard cache invalidate ({_invalidated} keys): {int((time.time() - _t0) * 1000)}ms",
            },
        )

    # ── 4. Usage log bookkeeping ──────────────────────────────────────────────
    # Each ingested raw log file = 1 billable Class A PutObject by Fastly's edge.
    # Synthesise those rows + flush in-process FOS/CDN calls + purge old entries.
    # Idempotent — safe to call after every sync, including after a retry.
    if (sync_enabled or force) and run_id is not None:
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} Updating usage log (Fastly-edge writes, in-process calls, retention purge)...",
            },
        )

    def _usage_log_phase() -> None:
        from backend.core.duckdb import backfill_fastly_edge_writes, reconcile_fastly_stats
        from backend.utils.usage_logger import run_usage_log_cleanup

        try:
            inserted = backfill_fastly_edge_writes(src)
            if inserted:
                if run_id is not None:
                    _log_and_add_progress(
                        run_id,
                        service_id,
                        job_name="usage_log",
                        event={"type": "status", "message": f"Backfilled {inserted} Fastly-edge PUTs to usage log"},
                    )
                else:
                    logger.info("[usage_log] %s: backfilled %d Fastly-edge PUTs", service_id, inserted)
        except Exception as e:
            logger.warning("[usage_log] backfill failed for %s: %s", service_id, e)

        # Pull Fastly /stats/aggregate to reconcile per-hour op counts. Closes
        # the multipart-upload + bookkeeping gap that backfill_fastly_edge_writes
        # cannot observe (it counts 1 op per file; Fastly emits ~3+). Writes one
        # compact row per hour/class gap via SUM(count) aggregation.
        # Window is 26h so the Usage Log page's 24h view always shows fully
        # reconciled data (and survives a small clock-skew buffer). One
        # Fastly API call covers the whole window regardless of hours_back.
        # Gated by do_heavy_refresh so a 1s log_period (5s tick) doesn't fire
        # this every 5s — Usage Log reads at hourly grain so 60s lag is invisible.
        if do_heavy_refresh:
            try:
                written = reconcile_fastly_stats(src, hours_back=26)
                if written:
                    if run_id is not None:
                        _log_and_add_progress(
                            run_id,
                            service_id,
                            job_name="usage_log",
                            event={"type": "status", "message": f"Reconciled {written} hourly Fastly stats gap(s)"},
                        )
                    else:
                        logger.info("[usage_log] %s: reconciled %d hourly stats gap(s)", service_id, written)
            except Exception as e:
                logger.warning("[usage_log] Fastly stats reconciliation failed for %s: %s", service_id, e)

        run_usage_log_cleanup(service_id)

    # Run _usage_log_phase inline. Pre-fix this was wrapped in a NESTED
    # ThreadPoolExecutor — but ``_run_service_cron`` is itself already
    # running inside the ``@cron_task`` executor (one layer up). On the
    # 30s timeout path the old code called ``shutdown(wait=False)``,
    # which abandons the worker thread + everything it pinned (DuckDB
    # connections, aiohttp sessions, Fastly API state). On a 50-service
    # deployment with reconcile_fastly_stats hitting the API in lockstep,
    # the inner timeout fired routinely and each leak orphaned an 8-12MB
    # stack plus whatever Python state was live. Over hours: multi-GB
    # unbounded growth — a confirmed contributor to the recurring host
    # OOM-kills.
    #
    # Running inline drops the leak and matches every other phase in
    # this cron body. If a per-phase timeout is needed in the future,
    # use a cooperative cancel token through the I/O layer rather than
    # abandoning a thread.
    _t0 = time.time()
    try:
        _usage_log_phase()
    except Exception as e:
        logger.warning("[scheduler] %s: usage_log phase failed: %s", service_id, e)
    if run_id is not None:
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="sync",
            event={
                "type": "status",
                "message": f"{elapsed()} usage_log phase: {int((time.time() - _t0) * 1000)}ms",
            },
        )

    # ── 5. Final duration record ──────────────────────────────────────────────
    # The initial log_cron_run snapshot was taken before phases 1.5-4 (view
    # refresh, refresh_config_status, cache invalidate, usage_log) emitted
    # their per-phase timing events — refresh log_output too. silent=False
    # so a failed update surfaces in the log stream (other cron jobs swallow,
    # but sync is the high-frequency tick where divergence matters).
    if sync_enabled or force:
        from backend.cron.jobs._common import finalize_cron_duration

        finalize_cron_duration(
            src,
            run_id,
            start_time_exec,
            log_output=_extract_log_text(run_id) if run_id is not None else None,
            silent=False,
        )

    logger.info("⏹️  \x1b[94m[sync]\x1b[0m %s: Sync job finished.", _display)


# ── _run_full_sweep (daily catch-net) ────────────────────────────────────────


# Default budget for a full sweep — bounded enough that one run can't pin a
# pod for >15 min and can't burn through too many S3 GETs at once. Heal-
# triggered invocations override these via ``_run_full_sweep(...,
# max_files=N, max_seconds=N)`` when sustained loss is severe (see
# ``_run_gap_heal``); the daily catch-net keeps the conservative defaults.
_FULL_SWEEP_DEFAULT_MAX_FILES = 20_000
_FULL_SWEEP_DEFAULT_MAX_SECONDS = 900


@cron_task("full_sync")
def _run_full_sweep(
    service_id: str,
    max_files: int = _FULL_SWEEP_DEFAULT_MAX_FILES,
    max_seconds: int = _FULL_SWEEP_DEFAULT_MAX_SECONDS,
) -> None:
    """Daily catch-net: full LIST over raw/ to pick up late-arriving files.

    The minute-cadence sync uses a 4h ``StartAfter`` lookback to bound LIST
    cost. If a Fastly POP backfills logs older than that window (recovery,
    timestamp skew, manual replay), the incremental scan never sees them.
    This sweep lists the entire raw/ prefix once a day and ingests anything
    not already in ``ingested_files``. Logged as task=``full_sync`` so users
    can distinguish catch-net runs from regular sync in the cron history.

    ``max_files`` / ``max_seconds`` are exposed so ``_run_gap_heal`` can
    push a bigger budget through during severe sustained-loss recovery —
    healing 200k missing files at the default 20k/run would take >40 hours
    of throttled cycles.
    """
    from backend.cron.scheduler import dev_mode_no_crons

    # Defense-in-depth — gap_heal can invoke this directly, and that
    # path was the leading bypass-candidate for the dev-ingestion
    # surprise. The kill switch beats the gap_heal trigger.
    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — full_sweep refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.ingest import ingest

    cfg = svcconfig.load_config(service_id) or {}
    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})
    delete_after = sync_cfg.get("delete_after", True)

    src = get_source_for_service(service_id)
    if src is None or src.get("access_level") == "read_only":
        return

    try:
        run_id = start_cron_run(src, "full_sync")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[full_sync]\x1b[0m %s: skipping — %s", service_id, e)
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="full_sync")
    _display = _display_label(src, service_id)
    logger.info(
        "▶️  \x1b[95m[full_sync]\x1b[0m %s: Full-LIST sweep started (max_files=%d, max_seconds=%d).",
        _display,
        max_files,
        max_seconds,
    )

    start_time_exec = time.time()
    processed_files = 0
    inserted_rows = 0
    corrupt_rows = 0
    done_event: dict = {}

    try:
        for event in ingest(
            source=src,
            delete_after=delete_after,
            max_files=max_files,
            max_seconds=max_seconds,
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
                log_cron_run(
                    src,
                    "full_sync",
                    time.time() - start_time_exec,
                    "error",
                    error_message=event.get("message"),
                    summary="Full-sweep failed",
                    files_downloaded=processed_files,
                    rows_ingested=inserted_rows,
                    corrupt_rows=corrupt_rows,
                    run_id=run_id,
                    log_output=_extract_log_text(run_id),
                )
                end_progress(run_id)
                return

        new_files = done_event.get("new_files", 0)
        rows = done_event.get("rows_inserted", 0)
        # full_sync is the whole-bucket backstop for the stranded-delete reconcile,
        # so it can reclaim strands of any age; record + surface that count too.
        reclaimed = done_event.get("deleted_files", 0)
        summary = (
            "No late-arriving files found"
            if new_files == 0
            else f"Backfilled {new_files} late-arriving file(s), {rows} row(s)"
        )
        if reclaimed:
            summary += f"; reclaimed {reclaimed} raw file(s) left by an interrupted prior run"
        log_cron_run(
            src,
            "full_sync",
            time.time() - start_time_exec,
            "success",
            files_downloaded=new_files,
            files_deleted_fos=reclaimed,
            rows_ingested=rows,
            corrupt_rows=done_event.get("corrupt_rows", 0),
            summary=summary,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="full_sync", event={"type": "done", "message": summary})
    except Exception as e:
        log_cron_run(
            src,
            "full_sync",
            time.time() - start_time_exec,
            "error",
            error_message=str(e),
            summary="Full-sweep crashed",
            files_downloaded=processed_files,
            rows_ingested=inserted_rows,
            corrupt_rows=corrupt_rows,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        logger.exception("[full_sync] %s: unexpected error", service_id)
    finally:
        end_progress(run_id)

    logger.info("⏹️  \x1b[95m[full_sync]\x1b[0m %s: Daily full-LIST sweep finished.", _display)


# ── _run_gap_heal (periodic detector → triggers full_sweep) ──────────────────


# Default throttle between gap-heal-triggered full_sweep invocations — used
# for mild loss only. ``_gap_heal_severity`` shortens it (and bumps the
# sweep budget) as the loss gets worse so a 200k-line burst doesn't take
# 40+ hours to drain at 20k files/run.
GAP_HEAL_THROTTLE_HOURS = 4


# Severity bands. Lower bound is "any loss the detector flagged" (≥5% gap
# over ≥2 buckets — already filtered by ``compute_log_accounting``); each
# band tightens the throttle and widens the sweep budget so heal can keep
# pace with the burst.
@dataclass(frozen=True)
class _GapHealSeverityBand:
    name: str
    # Sustained-loss thresholds — entering this band requires EITHER the
    # gap_pct or the total_lost_lines floor to be hit.
    min_gap_pct: float
    min_lost_lines: int
    # How long to throttle between heal-triggered sweeps. The detector
    # itself still runs every 30 min; this just bounds how often it
    # actually invokes a sweep.
    throttle_hours: float
    # Sweep budget overrides — passed to ``_run_full_sweep``. Larger sweeps
    # cost more S3 calls per run but drain backlog faster.
    sweep_max_files: int
    sweep_max_seconds: int


_GAP_HEAL_SEVERITY_BANDS: tuple[_GapHealSeverityBand, ...] = (
    # Severest first — first match wins.
    _GapHealSeverityBand(
        name="critical",
        min_gap_pct=0.80,
        min_lost_lines=500_000,
        throttle_hours=0.0,  # every detector tick is allowed to trigger
        sweep_max_files=100_000,
        sweep_max_seconds=1800,
    ),
    _GapHealSeverityBand(
        name="severe",
        min_gap_pct=0.50,
        min_lost_lines=100_000,
        throttle_hours=0.25,  # 15 min
        sweep_max_files=50_000,
        sweep_max_seconds=1500,
    ),
    _GapHealSeverityBand(
        name="elevated",
        min_gap_pct=0.10,
        min_lost_lines=10_000,
        throttle_hours=1.0,
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
    """Return the first severity band whose floor either field clears.

    Tested via threshold matrix in ``test_gap_heal_severity.py`` — keep the
    band tuple sorted severest-first so the bisection here works.
    """
    for band in _GAP_HEAL_SEVERITY_BANDS:
        if max_gap_pct >= band.min_gap_pct or total_lost_lines >= band.min_lost_lines:
            return band
    return _GAP_HEAL_SEVERITY_BANDS[-1]  # defensive — last band is the catch-all


# Tracks the wall-clock time of the most recent gap_heal that actually
# triggered a full_sweep. Lives in-process so a service restart clears it
# (acceptable: a restart implies the operator is paying attention; one
# extra sweep at startup is fine). Keyed by service_id.
_GAP_HEAL_LAST_TRIGGER: dict[str, float] = {}


def _last_successful_gap_heal_trigger(service_id: str) -> float | None:
    return _GAP_HEAL_LAST_TRIGGER.get(service_id)


def _mark_gap_heal_triggered(service_id: str) -> None:
    _GAP_HEAL_LAST_TRIGGER[service_id] = time.time()


@cron_task("gap_heal")
def _run_gap_heal(service_id: str) -> None:
    """Periodic gap detector that triggers a full_sweep when sustained loss
    is observed between Fastly's authoritative ``requests`` counts and
    our ingested rows.

    Sustained loss = ≥LOG_ACCOUNTING_MIN_RUN consecutive completed hourly
    buckets with gap_pct ≥ LOG_ACCOUNTING_LOSS_THRESHOLD. The in-flight
    bucket is excluded (Fastly Stats lags ingest), matching the UI callout.

    Throttled to one heal per GAP_HEAL_THROTTLE_HOURS hours so that a
    persistent gap (e.g. Fastly→FOS transport loss we cannot recover from)
    doesn't thrash the scheduler.
    """
    from backend.cron.scheduler import dev_mode_no_crons

    # Defense-in-depth: gap_heal's whole job is to call _run_full_sweep
    # when it sees a sustained gap. Local dev compared against prod's
    # FOS bucket sees a 100% "gap" by construction. Refuse the heal in
    # dev-mode to avoid kicking off a full sweep over the prod data.
    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — gap_heal refused.", service_id)
        return

    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    src = get_source_for_service(service_id)
    if src is None or src.get("access_level") == "read_only":
        return

    try:
        run_id = start_cron_run(src, "gap_heal")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[95m[gap_heal]\x1b[0m %s: skipping — %s", service_id, e)
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="gap_heal")
    _display = _display_label(src, service_id)

    start_time_exec = time.time()
    try:
        from backend.routers.admin import compute_log_accounting

        result = compute_log_accounting(src, hours=24, by="hour")
        sustained = result.get("sustained_loss")
        if sustained is None:
            log_cron_run(
                src,
                "gap_heal",
                time.time() - start_time_exec,
                "success",
                summary="No sustained loss detected",
                run_id=run_id,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="gap_heal",
                event={"type": "done", "message": "No sustained loss detected"},
            )
            return

        # Sustained loss observed — apply severity-scaled throttle to the
        # actual heal trigger. Worse loss = shorter throttle + bigger sweep
        # budget. See ``_gap_heal_severity`` for the bands.
        band = _gap_heal_severity(sustained.max_gap_pct, sustained.total_lost_lines)
        # Module-global lookup at call time so patches on
        # ``backend.cron.jobs.sync._last_successful_gap_heal_trigger``
        # intercept the call.
        last_heal = _last_successful_gap_heal_trigger(service_id)
        if band.throttle_hours > 0 and last_heal is not None:
            elapsed_hours = (time.time() - last_heal) / 3600.0
            if elapsed_hours < band.throttle_hours:
                msg = (
                    f"Sustained loss detected ({sustained.n_buckets} bucket(s), "
                    f"max gap {sustained.max_gap_pct:.1%}, "
                    f"{sustained.total_lost_lines} lost line(s), severity={band.name}) "
                    f"— throttled, last heal {elapsed_hours:.1f}h ago "
                    f"(< {band.throttle_hours:g}h)"
                )
                log_cron_run(
                    src,
                    "gap_heal",
                    time.time() - start_time_exec,
                    "success",
                    summary=msg,
                    run_id=run_id,
                    log_output=_extract_log_text(run_id),
                )
                _log_and_add_progress(run_id, service_id, job_name="gap_heal", event={"type": "done", "message": msg})
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
        log_cron_run(
            src,
            "gap_heal",
            time.time() - start_time_exec,
            "success",
            summary=msg,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        # Mark heal trigger BEFORE invoking the sweep so a long-running sweep
        # doesn't itself trip a second gap_heal tick into re-triggering. Both
        # resolve from module globals at call time so patches at
        # ``backend.cron.jobs.sync._mark_gap_heal_triggered`` /
        # ``backend.cron.jobs.sync._run_full_sweep`` keep intercepting.
        _mark_gap_heal_triggered(service_id)
        _run_full_sweep(
            service_id,
            max_files=band.sweep_max_files,
            max_seconds=band.sweep_max_seconds,
        )
    except Exception as e:
        log_cron_run(
            src,
            "gap_heal",
            time.time() - start_time_exec,
            "error",
            error_message=str(e),
            summary="Gap-heal evaluation crashed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        logger.exception("[gap_heal] %s: unexpected error", service_id)
    finally:
        end_progress(run_id)


# R-1: drain the gap-heal trigger timestamp dict between tests so an
# earlier test's last-trigger doesn't suppress a sweep in the next.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron.jobs.sync._GAP_HEAL_LAST_TRIGGER", _GAP_HEAL_LAST_TRIGGER)
