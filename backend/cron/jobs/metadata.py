"""Metadata-class cron jobs.

Covers everything that isn't ingest/commit/compaction proper:

  * ``_run_metadata_sync`` — analyst pull-to-local refresh + bootstrap
    helper called by :meth:`Scheduler.start` and by ``_run_commit``
    after a successful flush.
  * ``_run_ngwaf_bot_sync`` — per-service NGWAF VERIFIED-BOT pull.
  * ``_run_bot_data_refresh`` — global daily bot-source cache refresh.
  * ``_run_rdns_enrichment`` — every-5-min rDNS lookup batcher.
  * ``_run_share_audit_purge`` — daily remote-share audit log purge.
  * ``_run_service_alerts_evaluation`` — per-service alert evaluation.
  * ``_run_metadata_cleanup`` — daily SQLite retention trim + VACUUM.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_name,
    _elapsed_since,
    _log_and_add_progress,
)

logger = logging.getLogger("backend.scheduler")


# ── _run_metadata_sync (no @cron_task — also called as a bootstrap helper) ────


def _run_metadata_sync(
    service_id: str, run_id: int | None = None, start_time: str | None = None, end_time: str | None = None
) -> None:
    """Refresh Iceberg table metadata and DuckDB view for read-only services.

    Called for 'Analyst' users who don't ingest raw logs but need to see
    new snapshots committed by Admin users.
    """
    from backend import config as svcconfig
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import (
        get_connection,
        get_source_for_service,
        log_cron_run,
        refresh_config_status,
        start_cron_run,
    )
    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    is_manual = run_id is not None

    if run_id is None:
        try:
            run_id = start_cron_run(src, "metadata_sync")
        except RuntimeError as e:
            logger.info("[scheduler] %s: skipping metadata_sync — %s", service_id, str(e))
            return

    cleanup_progress_and_reap()

    # For manual runs (run_id is not None), we ignore the default limit unless
    # it was explicitly passed in. If a manual run is triggered without
    # start_time, it means "Import All", so we should clear any existing limit.

    if not start_time and not is_manual:
        prov = cfg.get("provisioning", {})
        tr = prov.get("time_range")
        if tr and tr.get("start"):
            start_time = tr["start"]
            logger.info("[scheduler] %s: Using configured start_time limit: %s", service_id, start_time)

    start_time_exec = time.time()

    def elapsed() -> str:
        return _elapsed_since(start_time_exec)

    start_progress(run_id, service_id=service_id, task="metadata_sync")
    _svc_name = cfg.get("name", service_id) if cfg else service_id
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="metadata_sync",
        event={"type": "status", "message": f"{elapsed()} Starting metadata sync..."},
    )

    try:
        # 1. Refresh Iceberg catalog from cloud
        # In PyIceberg SqlCatalog, load_table() will verify metadata from S3
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={"type": "status", "message": f"{elapsed()} Checking cloud for new Iceberg snapshots..."},
        )
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={
                "type": "status",
                "message": f"{elapsed()}   ↳ Downloading and parsing the latest catalog metadata (this may take 5-10 seconds)...",
            },
        )
        try:
            db_iceberg.init_iceberg_table(src, create=False)
        except Exception as e:
            # If the table doesn't exist yet, it's not an error we need to log as a failure.
            # This happens for brand new services that haven't committed logs yet.
            err_str = str(e).lower()
            if "not found" in err_str or "does not exist" in err_str or "nosuchtable" in err_str:
                msg = "Iceberg table not found, skipping sync until data is committed."
                _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"message": msg})
                _log_and_add_progress(
                    run_id, service_id, job_name="metadata_sync", event={"type": "status", "message": msg}
                )
                log_cron_run(src, "metadata_sync", time.time() - start_time_exec, "success", summary=msg, run_id=run_id)
                _log_and_add_progress(
                    run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": msg}
                )
                end_progress(run_id)
                return
            raise

        # 2. Sync data files (Pull-to-Local caching)
        msg = "Scanning Iceberg table for new data files..."
        if start_time or end_time:
            msg += f" (Range: {start_time or 'Start'} to {end_time or 'End'})"

            # Save the manually requested range so the DuckDB view can strictly bound to it
            prov = cfg.get("provisioning", {})
            if "time_range" not in prov:
                prov["time_range"] = {}
            if start_time:
                prov["time_range"]["start"] = start_time
            if end_time:
                prov["time_range"]["end"] = end_time
            cfg["provisioning"] = prov
            svcconfig.save_config(service_id, cfg)
            # Update local src reference since we mutated cfg
            src["time_range"] = prov["time_range"]
        elif is_manual:
            # Manual "Sync All": clear any previously pinned range
            prov = cfg.get("provisioning", {})
            if "time_range" in prov:
                del prov["time_range"]
                cfg["provisioning"] = prov
                svcconfig.save_config(service_id, cfg)
                src["time_range"] = None
                logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "status", "message": msg})

        def _sync_progress(downloaded: int, total: int, filename: str, rows: int) -> None:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="metadata_sync",
                event={
                    "type": "status",
                    "message": f"Downloading file {downloaded}/{total}: {filename} ({rows:,} rows)",
                },
            )

        data_res = db_iceberg.sync_data(src, progress_callback=_sync_progress, start_time=start_time, end_time=end_time)
        files_cached = data_res.get("files_downloaded", 0)
        rows_cached = data_res.get("rows_downloaded", 0)

        if files_cached == 0:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="metadata_sync",
                event={"type": "status", "message": "No new Iceberg files to sync — already up to date."},
            )
        else:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="metadata_sync",
                event={
                    "type": "status",
                    "message": f"Synced {files_cached} Iceberg data file(s) to local cache, {rows_cached:,} rows.",
                },
            )

        # 3. Update DuckDB view
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={"type": "status", "message": "Updating DuckDB views..."},
        )
        con = get_connection(source=src, read_only=False)
        try:
            db_iceberg.update_iceberg_view(con, src)
        finally:
            con.close()

        # 4. Import shared history and views/alerts from Admin
        try:
            from backend.state_sync import import_admin_state

            import_admin_state(service_id)
        except Exception as e:
            _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "warning", "message": e})

        # 5. Refresh cached status (row count, etc)
        refresh_config_status(service_id)

        # ── 6. Invalidate dashboard cache ─────────────────────────────────────
        try:
            from backend.repositories.dashboard import invalidate_service

            invalidate_service(src["name"])
        except Exception:
            pass

        duration = time.time() - start_time_exec
        summary = "Refreshed metadata"
        if files_cached > 0:
            verb = "downloaded" if src.get("access_level") == "read_only" else "synced"
            summary += f" and {verb} {files_cached} new Iceberg data file(s)"

        log_cron_run(
            src,
            "metadata_sync",
            duration,
            "success",
            files_downloaded=files_cached,
            rows_ingested=rows_cached,
            summary=summary,
            run_id=run_id,
        )
        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": summary})

    except Exception as e:
        duration = time.time() - start_time_exec
        log_cron_run(
            src, "metadata_sync", duration, "error", error_message=str(e), summary="Metadata sync failed", run_id=run_id
        )
        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: Metadata sync failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    from backend.cron.jobs._common import finalize_cron_duration

    finalize_cron_duration(src, run_id, start_time_exec)

    logger.info("⏹️  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job finished.", _display)


# ── _run_ngwaf_bot_sync ──────────────────────────────────────────────────────


@cron_task("sync_ngwaf_bots")
def _run_ngwaf_bot_sync(service_id: str) -> None:
    """Fetch NGWAF VERIFIED-BOT records and upsert into the local SQLite cache.

    Skips silently if ngwaf_workspace_id is not configured for the service.
    Resumes from last_timestamp_synced so restarts after a crash don't lose progress.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.utils.ngwaf import fetch_verified_bots_paged
    from backend.utils.ngwaf_bot_cache import cleanup_old_bots, ensure_schema, upsert_bots

    # Make sure the cache file + tables exist before anything else touches it.
    # Otherwise the planner query in oldest_unenriched_timestamp throws on the
    # very first run and the cron exits without ever populating data.
    try:
        ensure_schema()
    except Exception:
        pass

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
    if not workspace_id:
        return  # Not configured — skip silently

    src = get_source_for_service(service_id)
    if src is None:
        return

    api_key = cfg.get("fastly_api_key", "")
    if not api_key:
        logger.warning("[ngwaf_sync] %s: no fastly_api_key configured, skipping.", service_id)
        return

    try:
        run_id = start_cron_run(src, "ngwaf_sync")
    except RuntimeError as e:
        logger.info("[ngwaf_sync] %s: skipping — %s", service_id, e)
        return

    svc_display = cfg.get("name", service_id)
    logger.info("▶️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job started.", svc_display)

    prov = cfg.get("provisioning", {})
    retention_days = int(prov.get("cron_ngwaf", {}).get("log_retention_days", 30))
    server_name_filter = cfg.get("server_name") or None

    from backend.utils.bot_sources import build_matcher
    from backend.utils.ngwaf_bot_cache import get_last_timestamp, update_sync_watermark

    matcher = build_matcher()
    # Watermark-only resume path. upsert_bots() advances last_timestamp_synced
    # after every successful page, so steady state reads from local SQLite with
    # zero cloud I/O. On first-ever sync the watermark is NULL — seed it with
    # "now" and skip this cycle so the next one starts cleanly from "now".
    # We don't enrich pre-provisioning log rows (rarely the user's intent) and
    # we don't fall back to a cloud planner that scans every iceberg manifest.
    from_ts = get_last_timestamp(workspace_id)
    if not from_ts:
        now_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        update_sync_watermark(workspace_id, now_ts)
        summary = (
            f"First sync — seeded watermark at {now_ts}. Next cycle will fetch new bot records from this point forward."
        )
        log_cron_run(src, "ngwaf_sync", 0.0, "success", summary=summary, run_id=run_id)
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
        logger.info("⏹️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)
        return

    total_records = 0
    start_time = time.time()
    # Budget: page for up to 4 minutes per execution. Each page is committed so
    # a crash or budget cut never loses partially-synced data.
    max_runtime_secs = 240
    budget_exceeded = False

    try:
        for page_records, page_latest_ts, _raw_count in fetch_verified_bots_paged(api_key, workspace_id, from_ts):
            if server_name_filter:
                page_records = [
                    r for r in page_records if not r.get("server_name") or r["server_name"] == server_name_filter
                ]

            enriched: list[dict] = []
            for r in page_records:
                ua = r.get("user_agent")
                wk_matches = matcher(ua) if ua else ()
                wk_match = wk_matches[0] if wk_matches else None
                enriched.append(
                    {
                        **r,
                        "wellknown_bot_id": wk_match.get("id") if wk_match else None,
                        "wellknown_bot_name": wk_match.get("name") if wk_match else None,
                    }
                )

            if enriched or page_latest_ts:
                upsert_bots(enriched, workspace_id, page_latest_ts)
            total_records += len(enriched)

            if time.time() - start_time >= max_runtime_secs:
                budget_exceeded = True
                break

        deleted = cleanup_old_bots(retention_days)
        if budget_exceeded:
            summary = f"Synced {total_records} bot record(s) (budget reached — will continue next run), cleaned {deleted} old row(s)."
        else:
            summary = f"Synced {total_records} bot record(s), cleaned {deleted} old row(s)."
        log_cron_run(src, "ngwaf_sync", time.time() - start_time, "success", summary=summary, run_id=run_id)
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
    except Exception as e:
        log_cron_run(
            src,
            "ngwaf_sync",
            time.time() - start_time,
            "error",
            error_message=str(e),
            summary="NGWAF sync failed",
            run_id=run_id,
        )
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "error", "message": str(e)})
        logger.exception("[ngwaf_sync] %s: sync failed: %s", svc_display, e)

    logger.info("⏹️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)


# ── _run_bot_data_refresh / _run_rdns_enrichment / _run_share_audit_purge ────


def _run_bot_data_refresh() -> None:
    """Fetch and cache all enabled bot sources (nightly 02:00 UTC)."""
    from backend.utils.bot_sources import refresh_all_sources
    from backend.utils.system_jobs import record_job_run

    logger.info("▶️  \x1b[36m[bots]\x1b[0m Bot data refresh job started.")
    start = time.monotonic()
    try:
        results = refresh_all_sources()
        total = sum(r.get("entry_count", 0) for r in results)
        record_job_run(
            "bot_data_refresh",
            "success",
            time.monotonic() - start,
            f"Updated {len(results)} source(s), {total} total entries",
        )
        logger.info("✅ \x1b[36m[bots]\x1b[0m Refreshed %d source(s), %d total entries", len(results), total)
    except Exception as e:
        record_job_run("bot_data_refresh", "error", time.monotonic() - start, str(e))
        logger.error("[bot_data_refresh] Failed: %s", e)

    logger.info("⏹️  \x1b[36m[bots]\x1b[0m Bot data refresh job finished.")


def _run_rdns_enrichment() -> None:
    """Resolve pending rDNS lookups and discover new IPs (every 5 min)."""
    from backend.utils.rdns_cache import enrich_batch
    from backend.utils.system_jobs import record_job_run

    logger.info("▶️  \x1b[34m[rdns]\x1b[0m rDNS enrichment job started.")
    start = time.monotonic()
    try:
        summary = enrich_batch()
        record_job_run(
            "rdns_enrichment",
            "success",
            time.monotonic() - start,
            f"resolved={summary['resolved']} errors={summary['errors']} discovered={summary['discovered']}",
        )
    except Exception as e:
        record_job_run("rdns_enrichment", "error", time.monotonic() - start, str(e))
        logger.error("[rdns_enrichment] Failed: %s", e)

    logger.info("⏹️  \x1b[34m[rdns]\x1b[0m rDNS enrichment job finished.")


def _run_share_audit_purge() -> None:
    """Drop remote-share audit rows older than the retention window (daily 03:45 UTC).

    Retention is read from the `share_audit_retention_days` setting, defaulting
    to 90 days. The companion endpoint is `share_db.purge_old_audit_logs`.
    """
    from backend.core import share_db
    from backend.utils.system_jobs import record_job_run

    logger.info("▶️  \x1b[35m[share_audit_purge]\x1b[0m Share audit purge job started.")
    start = time.monotonic()
    try:
        raw = share_db.get_setting(share_db.SHARE_AUDIT_RETENTION_DAYS_KEY, "90")
        try:
            retention = max(1, int(raw or "90"))
        except (TypeError, ValueError):
            retention = 90
        deleted = share_db.purge_old_audit_logs(retention_days=retention)
        record_job_run(
            "share_audit_purge",
            "success",
            time.monotonic() - start,
            f"deleted={deleted} retention_days={retention}",
        )
        logger.info(
            "✅ \x1b[35m[share_audit_purge]\x1b[0m Deleted %d row(s) older than %d days.",
            deleted,
            retention,
        )
    except Exception as e:
        record_job_run("share_audit_purge", "error", time.monotonic() - start, str(e))
        logger.error("[share_audit_purge] Failed: %s", e)

    logger.info("⏹️  \x1b[35m[share_audit_purge]\x1b[0m Share audit purge job finished.")


# ── _run_service_alerts_evaluation ───────────────────────────────────────────


@cron_task("evaluate_alerts")
def _run_service_alerts_evaluation(service_id: str) -> None:
    """Evaluate all enabled alerts for a specific service."""
    from backend.core.duckdb import get_connection, get_source_for_service, log_cron_run, start_cron_run
    from backend.repositories import alerts as alert_repo

    start = time.monotonic()

    src = get_source_for_service(service_id)
    if not src:
        logger.warning("Could not find source for service_id %s", service_id)
        return

    task_name = "alerts"
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job started.", _display)

    # Fetch alerts from per-service metadata SQLite (no DuckDB needed).
    alerts = alert_repo.get_alerts(service_id=service_id)
    enabled_alerts = [a for a in alerts if a["enabled"]]
    # DuckDB connection is only needed if we actually have alerts to evaluate.
    con_ro = get_connection(src, read_only=True) if enabled_alerts else None

    if not enabled_alerts:
        logger.info("🔔 \x1b[93m[alerts]\x1b[0m %s: No alerts configured, skipping.", _display)
        log_cron_run(src, task_name, time.monotonic() - start, "skipped", summary="No alerts configured")
        logger.info("⏹️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job finished.", _display)
        return
    # Past this point enabled_alerts is non-empty, so con_ro was opened
    # on line 532 — narrow for mypy.
    assert con_ro is not None
    run_id = None
    try:
        run_id = start_cron_run(src, task_name)
    except Exception as e:
        if con_ro is not None:
            con_ro.close()
        logger.debug("[scheduler] Could not start alerts evaluation for %s: %s", service_id, e)
        return

    try:
        s_name = _display_name(src, service_id)
        display_name = f"{s_name} ({service_id})" if s_name != service_id else service_id

        # (alert_id, webhook_url, payload, max_ts) for each alert that should fire
        triggered_items: list[tuple[str, str | None, dict | None, str | None]] = []

        for alert in enabled_alerts:
            try:
                fired, webhook_url, payload, max_ts = alert_repo.evaluate_alert(
                    con_ro, src, alert, display_name=display_name, service_id=service_id
                )
                if fired:
                    triggered_items.append((alert["id"], webhook_url, payload, max_ts))
                    logger.info("🚨  \x1b[93m[alerts]\x1b[0m %s: Alert triggered: %s", display_name, alert["name"])
            except Exception as e:
                logger.error(
                    "%s Failed to evaluate alert %s for %s: %s",
                    JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                    alert["id"],
                    display_name,
                    e,
                )
    finally:
        if con_ro is not None:
            con_ro.close()

    try:
        # Second pass: write timestamps first, then dispatch webhooks so a crash
        # between the two doesn't cause duplicate notifications on the next run.
        if triggered_items:
            for alert_id, _, _, max_ts in triggered_items:
                alert_repo.update_last_triggered(service_id, alert_id, max_ts)

            # Export updated state before sending webhooks so the quiet-period
            # timestamp is durable even if a webhook call hangs or fails.
            from backend.state_sync import export_admin_state

            export_admin_state(service_id)

            import httpx

            for alert_id, webhook_url, payload, _ in triggered_items:
                if webhook_url and payload:
                    try:
                        httpx.post(webhook_url, json=payload, timeout=5)
                    except Exception as e:
                        logger.error(
                            "%s Failed to send webhook for alert %s: %s",
                            JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                            alert_id,
                            e,
                        )

        n_eval = len(enabled_alerts)
        n_trig = len(triggered_items)
        summary = (
            f"Evaluated {n_eval} {'alert' if n_eval == 1 else 'alerts'}. "
            f"{n_trig} {'alert' if n_trig == 1 else 'alerts'} triggered."
        )

        log_cron_run(
            src,
            task_name,
            time.monotonic() - start,
            "success",
            summary=summary,
            files_downloaded=n_eval,
            rows_ingested=n_trig,
            run_id=run_id,
        )

    except Exception as e:
        import traceback

        err_msg = traceback.format_exc()
        logger.error(
            "%s Failed during alerts evaluation job for %s: %s\n%s",
            JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
            service_id,
            e,
            err_msg,
        )
        log_cron_run(
            src,
            task_name,
            time.monotonic() - start,
            "error",
            summary=f"Alerts evaluation failed: {e}",
            error_message=err_msg,
            files_downloaded=0,
            rows_ingested=0,
            run_id=run_id,
        )
    finally:
        from backend.cron.jobs._common import finalize_cron_duration

        finalize_cron_duration(src, run_id, start, clock=time.monotonic)


# ── _run_metadata_cleanup ────────────────────────────────────────────────────


@cron_task("metadata_cleanup")
def _run_metadata_cleanup(service_id: str) -> None:
    """Daily: trim usage_log + ingested_files + cron_runs per service retention cfg.

    Retention defaults to 1 day for usage_log/ingested_files, 7 days for
    cron_runs (see ``metadata_db.DEFAULT_METADATA_RETENTION``). Override
    per service via cfg["metadata_retention"]:

        {"metadata_retention": {"usage_log_days": 7, "ingested_files_days": 30,
                                "cron_runs_days": 30}}

    A value of 0 (or negative) disables cleanup for that table — useful for
    a long-retention analyst service that wants the full audit trail.

    VACUUM only runs when something was actually deleted. On a healthy
    daily cadence this means: first run trims everything older than
    retention, subsequent runs are mostly no-ops (only that day's
    just-aged rows to trim), and VACUUM happens cheaply on small deltas.

    Writes a row to the cron_runs audit table on completion so the run
    shows up on the Data Management cron schedule + history grid alongside
    the other tasks. The cron_runs row itself becomes part of the next
    cleanup's trimming target (capped at cron_runs_days retention).
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.metadata_db import cleanup_metadata

    src = get_source_for_service(service_id)
    if src is None:
        return

    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    color = JOB_COLORS.get("metadata_cleanup", "")
    label = f"{color}[metadata_cleanup]{RESET_COLOR}"
    logger.info("▶️  %s %s: Starting metadata cleanup.", label, _display)

    start_ts = time.time()
    run_id = start_cron_run(src, "metadata_cleanup")
    try:
        result = cleanup_metadata(service_id, retention)
    except Exception as e:
        logger.exception("%s %s: cleanup failed: %s", label, _display, e)
        log_cron_run(
            src,
            "metadata_cleanup",
            time.time() - start_ts,
            "error",
            error_message=str(e),
            summary=f"cleanup failed: {e}",
            run_id=run_id,
        )
        return

    total_deleted = sum(result["deleted"].values())
    summary_parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
    summary = (
        (
            f"Trimmed {total_deleted:,} rows ({', '.join(summary_parts)}). "
            f"VACUUM={'yes' if result['vacuumed'] else 'skipped (no deletions)'}."
        )
        if total_deleted
        else "No rows older than retention windows."
    )

    if total_deleted:
        logger.info(
            "🧹 %s %s: deleted %d rows (%s) vacuumed=%s in %.2fs",
            label,
            _display,
            total_deleted,
            ", ".join(summary_parts),
            result["vacuumed"],
            result["duration_s"],
        )
    else:
        logger.info("⏹️  %s %s: no rows to trim (took %.2fs)", label, _display, result["duration_s"])

    log_cron_run(
        src,
        "metadata_cleanup",
        time.time() - start_ts,
        "success",
        summary=summary,
        # Repurpose the rows_ingested column for the count of rows trimmed —
        # the schema is shared across all cron tasks, and "rows_ingested" is
        # the closest semantic fit (each task interprets it by context).
        rows_ingested=total_deleted,
        run_id=run_id,
    )
