"""Metadata-class cron jobs.

Covers everything that isn't ingest/commit/compaction proper:

  * ``_run_metadata_sync`` — analyst pull-to-local refresh + bootstrap
    helper called by :meth:`Scheduler.start`, scheduled metadata jobs, and
    the coalesced post-commit dispatcher.
  * ``_run_ngwaf_bot_sync`` — per-service NGWAF VERIFIED-BOT pull.
  * ``_run_bot_data_refresh`` — global daily bot-source cache refresh.
  * ``_run_rdns_enrichment`` — every-5-min rDNS lookup batcher.
  * ``_run_share_audit_purge`` — daily remote-share audit log purge.
  * ``_run_service_alerts_evaluation`` — per-service alert evaluation.
  * ``_run_metadata_cleanup`` — daily PostgreSQL operational metadata retention trim.
"""

from __future__ import annotations

import logging
import threading
import time

import httpx
import tenacity

from backend.core import duckdb as _db
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_label,
    _elapsed_since,
    _log_and_add_progress,
    cron_run,
    cron_task,
    global_job,
    record_skip_row,
)

logger = logging.getLogger("backend.scheduler")

_post_commit_metadata_sync_lock = threading.Lock()
_post_commit_metadata_sync_rerun: dict[str, bool] = {}


_RETRYABLE_WEBHOOK_STATUS = (429, 500, 502, 503, 504)


@tenacity.retry(
    retry=tenacity.retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def _post_json(url: str, payload: dict) -> None:
    response = httpx.post(url, json=payload, timeout=5)
    if response.status_code in _RETRYABLE_WEBHOOK_STATUS:
        response.raise_for_status()


# Old names kept as aliases: the alerts job and tests patch/import them by name.
_post_alert_webhook = _post_json
_post_slack_notification = _post_json


def _post_pagerduty_notification(url: str, alert_name: str, metric: str, msg_val: str, service_name: str) -> None:
    payload = {
        "event_action": "trigger",
        "client": "Fastly Log Analytics Engine",
        "payload": {
            "summary": f"Alert {alert_name} triggered on {service_name}: {metric} {msg_val}",
            "source": service_name,
            "severity": "critical",
            "class": "cdn metric",
            "component": metric,
        },
    }
    _post_json(url, payload)


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
        refresh_config_status,
    )

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    is_manual = run_id is not None

    if not is_manual:
        from backend.utils.active_requests import should_defer_cron

        if should_defer_cron("metadata_sync", service_id):
            return

    # For manual runs (run_id is not None), we ignore the default limit unless
    # it was explicitly passed in. If a manual run is triggered without
    # start_time, it means "Import All", so we should clear any existing limit.

    from backend.cron.jobs.sync import apply_time_range_pinning, get_service_display_label

    start_time_exec = time.time()

    def elapsed() -> str:
        return _elapsed_since(start_time_exec)

    _display = get_service_display_label(service_id, cfg)
    with cron_run(
        service_id,
        "metadata_sync",
        source=src,
        run_id=run_id,
        error_summary="Metadata sync failed",
        finalize_duration=True,
        include_log_output=False,
        skip_log_prefix="scheduler",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        logger.info("🏎️  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job started.", _display)
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_sync",
            event={"type": "status", "message": f"{elapsed()} Starting metadata sync..."},
        )

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
        # "No data committed yet" is a normal state for a brand-new service and
        # must be reported as a success, not an error — otherwise the very
        # first sync writes a misleading failure to the system-jobs panel.
        #
        # Under v3 that condition is a FALSE return from
        # ``ducklake_table_exists``, not an exception: pre-v3 this leaned on
        # pyiceberg raising ``NoSuchTableError``, which the DuckLake-backed
        # ``init_iceberg_table`` no longer does (it returns None on an attach
        # failure and True otherwise), so the graceful branch had gone dead
        # and a fresh service fell through into the data sync instead. The
        # string-matching ``except`` is kept as a belt for any caller/patch
        # that still raises the old shape.
        table_present: bool
        try:
            if db_iceberg.init_iceberg_table(src, create=False) is None:
                raise RuntimeError(f"could not attach the DuckLake catalog for {service_id}")
            table_present = db_iceberg.ducklake_table_exists(src)
        except Exception as e:
            err_str = str(e).lower()
            if "not found" in err_str or "does not exist" in err_str or "nosuchtable" in err_str:
                table_present = False
            else:
                raise

        if not table_present:
            msg = "Iceberg table not found, skipping sync until data is committed."
            _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"message": msg})
            _log_and_add_progress(
                run_id, service_id, job_name="metadata_sync", event={"type": "status", "message": msg}
            )
            run.success(msg)
            _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": msg})
            return

        # 2. Sync data files (Pull-to-Local caching)
        start_time, end_time = apply_time_range_pinning(
            service_id,
            cfg,
            src,
            start_time=start_time,
            end_time=end_time,
            is_manual=is_manual,
            pin_explicit_range=True,
        )
        msg = "Scanning Iceberg table for new data files..."
        if start_time or end_time:
            msg += f" (Range: {start_time or 'Start'} to {end_time or 'End'})"

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
        # read_only=False is load-bearing in standard mode: a read-only connection
        # makes the slow-path rebuild bind a TEMP view that dies with this connection
        # moments later — the cron would pay the full rebuild cost for a
        # no-op and the persistent per-service view would never refresh.
        # In durable serving mode, connections are ephemeral in-memory, so read-only
        # is required by the durable serving guard.
        durable_serving = svcconfig.is_durable_serving_mode(src)
        con = get_connection(source=src, read_only=durable_serving)
        try:
            db_iceberg.update_iceberg_view(con, src)
        finally:
            con.close()

        # 4. Import shared history and views/alerts from Admin
        import_error: str | None = None
        try:
            from backend.state_sync import import_admin_state

            import_admin_state(service_id)
        except Exception as e:
            import_error = str(e)
            _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "warning", "message": e})

        # 5. Refresh cached status (row count, etc)
        refresh_config_status(service_id)

        # ── 6. Invalidate dashboard cache ─────────────────────────────────────
        try:
            from backend.repositories.dashboard import invalidate_service

            invalidate_service(src["name"])
        except Exception:
            pass

        summary = "Refreshed metadata"
        if files_cached > 0:
            verb = "downloaded" if src.get("access_level") == "read_only" else "synced"
            summary += f" and {verb} {files_cached} new Iceberg data file(s)"
        if import_error:
            summary += f" (admin state import warning: {import_error})"
            run_status = "warning"
        else:
            run_status = "success"

        run.record(run_status, summary=summary, files_downloaded=files_cached, rows_ingested=rows_cached)
        _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": summary})

    logger.info("🏁  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job finished.", _display)


@cron_task("metadata_sync", job_name="metadata_sync")
def _run_post_commit_metadata_sync(service_id: str) -> None:
    _run_metadata_sync(service_id)


def _post_commit_metadata_sync_worker(service_id: str) -> None:
    while True:
        try:
            _run_post_commit_metadata_sync(service_id)
        except Exception:
            logger.exception("[scheduler] %s: post-commit metadata sync failed unexpectedly", service_id)

        with _post_commit_metadata_sync_lock:
            if _post_commit_metadata_sync_rerun.get(service_id, False):
                _post_commit_metadata_sync_rerun[service_id] = False
                continue
            _post_commit_metadata_sync_rerun.pop(service_id, None)
            return


def dispatch_post_commit_metadata_sync(service_id: str) -> None:
    """Run metadata sync off the commit path, coalescing requests per service."""
    with _post_commit_metadata_sync_lock:
        if service_id in _post_commit_metadata_sync_rerun:
            _post_commit_metadata_sync_rerun[service_id] = True
            return

        _post_commit_metadata_sync_rerun[service_id] = False
        worker = threading.Thread(
            target=_post_commit_metadata_sync_worker,
            args=(service_id,),
            name=f"metadata-sync-on-commit:{service_id}",
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            _post_commit_metadata_sync_rerun.pop(service_id, None)
            raise


# ── _run_ngwaf_bot_sync ──────────────────────────────────────────────────────


@cron_task("sync_ngwaf_bots", job_name="ngwaf_sync")
def _run_ngwaf_bot_sync(service_id: str) -> None:
    """Fetch NGWAF VERIFIED-BOT records and upsert into the PostgreSQL ngwaf_bots table.

    Skips silently if ngwaf_workspace_id is not configured for the service.
    Resumes from last_timestamp_synced so restarts after a crash don't lose progress.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.cron.jobs.sync import get_service_display_label
    from backend.utils.ngwaf import cleanup_old_bots, fetch_verified_bots_paged, upsert_bots

    if dev_mode_no_crons():
        logger.info("⏸️  \x1b[36m[ngwaf_sync]\x1b[0m %s: dev_mode_no_crons active, skipping NGWAF sync.", service_id)
        return

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

    svc_display = get_service_display_label(service_id, cfg)

    prov = cfg.get("provisioning", {})
    retention_days = int(prov.get("cron_ngwaf", {}).get("log_retention_days", 30))
    server_name_filter = cfg.get("server_name") or None

    with cron_run(
        service_id,
        "ngwaf_sync",
        source=src,
        error_summary="NGWAF sync failed",
        finalize_duration=True,
        include_log_output=False,
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        logger.info("🏎️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job started.", svc_display)
        total_records = 0
        try:
            from backend.utils.bot_sources import build_matcher
            from backend.utils.ngwaf import get_last_timestamp, update_sync_watermark

            matcher = build_matcher()
            # Watermark-only resume path. upsert_bots() advances last_timestamp_synced
            # after every successful page, so steady state reads from PostgreSQL with
            # zero cloud I/O. On first-ever sync the watermark is NULL — seed it with
            # "now" and skip this cycle so the next one starts cleanly from "now".
            from_ts = get_last_timestamp(workspace_id)
            if not from_ts:
                from backend.utils.date_utils import iso_z_now

                now_ts = iso_z_now()
                update_sync_watermark(workspace_id, now_ts)
                summary = f"First sync — seeded watermark at {now_ts}. Next cycle will fetch new bot records from this point forward."
                run.success(summary)
                _log_and_add_progress(
                    run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary}
                )
                logger.info("🏁  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)
                return

            max_runtime_secs = 240
            budget_exceeded = False

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

                if run.elapsed() >= max_runtime_secs:
                    budget_exceeded = True
                    break

            deleted = cleanup_old_bots(retention_days)
            if budget_exceeded:
                summary = f"Synced {total_records} bot record(s) (budget reached — will continue next run), cleaned {deleted} old row(s)."
            else:
                summary = f"Synced {total_records} bot record(s), cleaned {deleted} old row(s)."
            run.success(summary, files_downloaded=total_records)
            _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})

            # Adaptive scheduling under load:
            # If budget was reached or high bot volume (>= 500 records), tighten interval to 2 minutes
            # to rapidly catch up. When backlog subsides, restore baseline configured interval (default 5m).
            try:
                from backend.cron.scheduler import get_scheduler

                sched = get_scheduler()
                job = sched.get_job(f"ngwaf_sync_{service_id}")
                if job:
                    base_mins = max(1, int(prov.get("cron_ngwaf", {}).get("interval_mins", 5)))
                    if budget_exceeded or total_records >= 500:
                        expedited_mins = max(1, min(2, base_mins))
                        job.reschedule("interval", minutes=expedited_mins)
                        logger.info(
                            "👾 [ngwaf_sync] %s: High NGWAF volume (%d records) — expedited next run to %dm.",
                            svc_display,
                            total_records,
                            expedited_mins,
                        )
                    else:
                        job.reschedule("interval", minutes=base_mins)
            except Exception:
                pass
        except Exception as e:
            err_msg = str(e)
            is_auth_error = any(
                code in err_msg for code in ("401", "403", "Unauthorized", "Forbidden", "invalid_api_key")
            )
            if is_auth_error:
                summary = "NGWAF sync failed: authentication error (check Fastly API key / workspace permissions)"
            else:
                summary = f"NGWAF sync failed: {err_msg}"
            run.fail(summary, error_message=err_msg, files_downloaded=total_records)
            _log_and_add_progress(
                run_id, service_id, job_name="ngwaf_sync", event={"type": "error", "message": summary}
            )
            logger.exception("[ngwaf_sync] %s: sync failed: %s", svc_display, err_msg)

    logger.info("🏁  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)


# ── _run_bot_data_refresh / _run_rdns_enrichment / _run_share_audit_purge ────


@global_job("bot_data_refresh", color="36", tag="bots", label="Bot data refresh")
def _run_bot_data_refresh() -> tuple[str, str]:
    """Fetch and cache all enabled bot sources (nightly 02:00 UTC)."""
    from backend.cron.decorators import dev_mode_no_crons
    from backend.utils.bot_sources import refresh_all_sources

    if dev_mode_no_crons():
        logger.info("⏸️  \x1b[36m[bots]\x1b[0m dev_mode_no_crons active, skipping bot data refresh.")
        return ("skipped", "skipped (dev_mode_no_crons)")

    results = refresh_all_sources()
    failed = [r for r in results if r.get("failed")]
    succeeded = [r for r in results if not r.get("failed")]
    total = sum(r.get("entry_count", 0) for r in succeeded)
    logger.info("✅ \x1b[36m[bots]\x1b[0m Refreshed %d source(s), %d total entries", len(succeeded), total)
    detail = f"Updated {len(succeeded)} source(s), {total} total entries"

    if failed and not succeeded:
        failed_ids = ", ".join(r.get("id", "unknown") for r in failed)
        return ("error", f"{detail} (All {len(failed)} bot sources failed: {failed_ids})")
    if failed:
        failed_ids = ", ".join(r.get("id", "unknown") for r in failed)
        return ("warning", f"{detail} ({len(failed)} source(s) failed: {failed_ids})")
    return ("success", detail)


@global_job("rdns_enrichment", color="34", tag="rdns", label="rDNS enrichment")
def _run_rdns_enrichment() -> tuple[str, str]:
    """Resolve pending rDNS lookups and discover new IPs (every 5 min)."""
    from backend.cron.decorators import dev_mode_no_crons
    from backend.utils.rdns_cache import enrich_batch

    if dev_mode_no_crons():
        logger.info("⏸️  \x1b[34m[rdns]\x1b[0m dev_mode_no_crons active, skipping rDNS enrichment.")
        return ("skipped", "skipped (dev_mode_no_crons)")

    summary = enrich_batch()
    detail = f"resolved={summary['resolved']} errors={summary['errors']} discovered={summary['discovered']}"
    if summary["errors"] > 0 and summary["resolved"] == 0:
        return ("warning", f"{detail} (DNS lookup failures detected)")
    return ("success", detail)


@global_job("share_audit_purge", color="35", tag="share_audit_purge", label="Share audit purge")
def _run_share_audit_purge() -> tuple[str, str]:
    """Drop remote-share audit rows older than retention, expired invites, tokens, and stale sessions (daily 03:45 UTC).

    Retention is read from the `share_audit_retention_days` setting, defaulting
    to 90 days. The companion endpoint is `POST /api/admin/share/purge`.
    """
    from backend.core import share_db
    from backend.cron.decorators import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.info("⏸️  \x1b[35m[share_audit_purge]\x1b[0m dev_mode_no_crons active, skipping share audit purge.")
        return ("skipped", "skipped (dev_mode_no_crons)")

    raw = share_db.get_setting(share_db.SHARE_AUDIT_RETENTION_DAYS_KEY, "90")
    try:
        retention = max(1, int(raw or "90"))
    except (TypeError, ValueError):
        retention = 90
    deleted = share_db.purge_old_audit_logs(retention_days=retention)
    stale_res = share_db.purge_stale_share_records(max_idle_session_days=30)
    logger.info(
        "✅ \x1b[35m[share_audit_purge]\x1b[0m Deleted %d audit row(s), %d expired invite(s), %d stale session(s), %d claim token(s).",
        deleted,
        stale_res.get("deleted_expired_invites", 0),
        stale_res.get("deleted_stale_sessions", 0),
        stale_res.get("deleted_claim_tokens", 0),
    )
    detail = (
        f"deleted={deleted} retention_days={retention} "
        f"expired_invites={stale_res.get('deleted_expired_invites', 0)} "
        f"stale_sessions={stale_res.get('deleted_stale_sessions', 0)} "
        f"claim_tokens={stale_res.get('deleted_claim_tokens', 0)}"
    )
    return ("success", detail)


# ── _run_service_alerts_evaluation ───────────────────────────────────────────


@cron_task("evaluate_alerts", job_name="alerts")
def _run_service_alerts_evaluation(service_id: str) -> None:
    """Evaluate all enabled alerts for a specific service."""
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.repositories import alerts as alert_repo
    from backend.utils.active_requests import should_defer_cron

    if dev_mode_no_crons():
        logger.info("⏸️  \x1b[93m[alerts]\x1b[0m %s: dev_mode_no_crons active, skipping alerts evaluation.", service_id)
        return

    start = time.monotonic()

    src = get_source_for_service(service_id)
    if not src:
        logger.warning("Could not find source for service_id %s", service_id)
        return

    if should_defer_cron("alerts", service_id):
        return

    task_name = "alerts"
    _display = _display_label(src, service_id)
    logger.info("🏎️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job started.", _display)

    # Fetch alerts from PostgreSQL metadata (no DuckDB needed).
    alerts = alert_repo.get_alerts(service_id=service_id)
    enabled_alerts = [a for a in alerts if a["enabled"]]
    if not enabled_alerts:
        logger.info("🔔 \x1b[93m[alerts]\x1b[0m %s: No alerts configured, skipping.", _display)
        record_skip_row(src, task_name, "No alerts configured", duration_s=time.monotonic() - start)
        logger.info("🏁  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job finished.", _display)
        return

    with cron_run(
        service_id,
        task_name,
        source=src,
        error_summary=lambda exc: f"Alerts evaluation failed: {exc}",
        finalize_duration=True,
        include_log_output=False,
        skip_log_prefix="alerts",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        # DuckDB connection is only needed (and only opened) once the lease is held.
        con_ro = get_connection(src, read_only=True)

        webhook_failures: list[str] = []

        try:
            display_name = _display_label(src, service_id)

            # (alert, webhook_url, payload, max_ts) for each alert that should fire
            triggered_items: list[tuple[dict, str | None, dict | None, str | None]] = []

            for alert in enabled_alerts:
                try:
                    fired, webhook_url, payload, max_ts = alert_repo.evaluate_alert(
                        con_ro, src, alert, display_name=display_name, service_id=service_id
                    )
                    if fired:
                        triggered_items.append((alert, webhook_url, payload, max_ts))
                        logger.info("🚨  \x1b[93m[alerts]\x1b[0m %s: Alert triggered: %s", display_name, alert["name"])
                        _log_and_add_progress(
                            run_id,
                            service_id,
                            job_name=task_name,
                            event={"type": "status", "message": f"Alert triggered: {alert['name']}"},
                        )
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
                for alert, _, _, max_ts in triggered_items:
                    alert_repo.update_last_triggered(service_id, alert["id"], max_ts)

                # Export updated state before sending webhooks so the quiet-period
                # timestamp is durable even if a webhook call hangs or fails.
                from backend.state_sync import export_admin_state

                export_admin_state(service_id)

                for alert, webhook_url, payload, _ in triggered_items:
                    # 1. Post legacy webhook if configured
                    if webhook_url and payload:
                        try:
                            _post_alert_webhook(webhook_url, payload)
                        except Exception as e:
                            err_str = f"webhook for {alert['name']}: {e}"
                            webhook_failures.append(err_str)
                            logger.error(
                                "%s Failed to send webhook for alert %s: %s",
                                JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                                alert["id"],
                                e,
                            )

                    # 2. Post to modern alert channels if configured
                    channels = alert.get("channels") or []
                    for channel in channels:
                        try:
                            chan_type = channel.get("type")
                            chan_url = channel.get("url")
                            if not chan_url:
                                continue
                            if chan_type == "slack":
                                slack_payload = {
                                    "blocks": [
                                        {
                                            "type": "section",
                                            "text": {
                                                "type": "mrkdwn",
                                                "text": f"🚨 *Alert Triggered: {alert['name']}*",
                                            },
                                        },
                                        {
                                            "type": "fields",
                                            "fields": [
                                                {"type": "mrkdwn", "text": f"*Service:* {display_name}"},
                                                {"type": "mrkdwn", "text": f"*Metric:* {alert['metric']}"},
                                                {"type": "mrkdwn", "text": f"*Operator:* {alert['operator']}"},
                                                {"type": "mrkdwn", "text": f"*Threshold:* {alert['threshold']}"},
                                            ],
                                        },
                                    ]
                                }
                                _post_slack_notification(chan_url, slack_payload)
                            elif chan_type == "pagerduty":
                                _post_pagerduty_notification(
                                    chan_url,
                                    alert_name=alert["name"],
                                    metric=alert["metric"],
                                    msg_val=f"{alert['operator']} {alert['threshold']}",
                                    service_name=display_name,
                                )
                            elif chan_type == "webhook":
                                _post_alert_webhook(chan_url, payload or {"alert": alert})
                        except Exception as e:
                            err_str = f"channel {channel.get('type')} for {alert['name']}: {e}"
                            webhook_failures.append(err_str)
                            logger.error(
                                "%s Failed to send notifications to channel %s for alert %s: %s",
                                JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                                channel.get("type"),
                                alert["id"],
                                e,
                            )

            n_eval = len(enabled_alerts)
            n_trig = len(triggered_items)
            status = "warning" if (n_trig > 0 or webhook_failures) else "success"
            summary = (
                f"Evaluated {n_eval} {'alert' if n_eval == 1 else 'alerts'}. "
                f"{n_trig} {'alert' if n_trig == 1 else 'alerts'} triggered."
            )
            if webhook_failures:
                summary += f" ({len(webhook_failures)} notification failure{'s' if len(webhook_failures) > 1 else ''}: {', '.join(webhook_failures)})"

            run.record(status, summary=summary, files_downloaded=n_eval, rows_ingested=n_trig)
            _log_and_add_progress(
                run_id,
                service_id,
                job_name=task_name,
                event={"type": "done", "message": summary},
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
            run.fail(f"Alerts evaluation failed: {e}", error_message=err_msg, files_downloaded=0, rows_ingested=0)


# ── _run_metadata_cleanup ────────────────────────────────────────────────────


@cron_task("metadata_cleanup", job_name="metadata_cleanup")
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
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata import cleanup_metadata
    from backend.cron.decorators import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

    color = JOB_COLORS.get("metadata_cleanup", "")
    label = f"{color}[metadata_cleanup]{RESET_COLOR}"

    if dev_mode_no_crons():
        logger.info("⏸️  %s %s: dev_mode_no_crons active, skipping metadata cleanup.", label, service_id)
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    if should_defer_cron("metadata_cleanup", service_id):
        return

    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    _display = _display_label(src, service_id)

    with cron_run(
        service_id,
        "metadata_cleanup",
        source=src,
        error_summary=lambda exc: f"cleanup failed: {exc}",
        finalize_duration=True,
        include_log_output=False,
        skip_log_prefix="metadata_cleanup",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        logger.info("🏎️  %s %s: Starting metadata cleanup.", label, _display)

        def _on_event(event: dict) -> None:
            _log_and_add_progress(run_id, service_id, job_name="metadata_cleanup", event=event)

        result = cleanup_metadata(service_id, retention, on_event=_on_event)
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
            logger.info("🏁  %s %s: no rows to trim (took %.2fs)", label, _display, result["duration_s"])

        # Also trim the global system_metrics.db retention window. Idempotent
        # across per-service runs (the second call this day deletes 0 rows
        # because the first one already cleared them), and the DELETE rides
        # the (metric, ts) index so the cost is microseconds even when there's
        # nothing to do. 30-day window matches the in-app Trends tab range
        # (1h / 24h / 7d) plus a buffer.
        try:
            purge_old(retention_days=30)
        except Exception as e:
            logger.debug("[metadata_cleanup] metric_snapshots purge failed: %s", e)

        # Repurpose the rows_ingested column for the count of rows trimmed —
        # the schema is shared across all cron tasks, and "rows_ingested" is
        # the closest semantic fit (each task interprets it by context).
        run.success(summary, rows_ingested=total_deleted)
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="metadata_cleanup",
            event={"type": "done", "message": summary},
        )


# ── Metric Snapshots (folded from backend.core.metric_snapshots) ─────────────

import gc  # noqa: E402
import os  # noqa: E402
import signal  # noqa: E402
import sys as _sys  # noqa: E402
from contextlib import closing  # noqa: E402
from datetime import UTC, datetime, timedelta  # noqa: E402
from typing import Any  # noqa: E402

from opentelemetry.metrics import CallbackOptions, Observation  # noqa: E402

from backend import config as svcconfig  # noqa: E402
from backend.core import duckdb_pool as _pool  # noqa: E402
from backend.core.request_telemetry import get_meter  # noqa: E402
from backend.utils.date_utils import iso_z, iso_z_now, parse_relative_time_window  # noqa: E402

_EMPTY = ""


def _pg_connection():
    from backend.core.metadata.pg_connection import get_pg_readonly_connection

    return get_pg_readonly_connection()


def _pg_write_connection():
    from backend.core.metadata.pg_connection import get_pg_thread_connection

    return get_pg_thread_connection()


# ── Write path ───────────────────────────────────────────────────────────────


def record_snapshot(
    metric: str,
    value: float,
    *,
    service_id: str | None = None,
    task: str | None = None,
    ts: str | None = None,
) -> None:
    """Insert one metric sample. Idempotent on ``(metric, service_id, task, ts)``."""
    if not metric:
        raise ValueError("metric is required")
    ts_str = ts or iso_z_now()
    try:
        con = _pg_write_connection()
        con.execute(
            """
            INSERT INTO metric_snapshots (ts, metric, service_id, task, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (metric, service_id, task, ts)
            DO UPDATE SET value = EXCLUDED.value
            """,
            (ts_str, metric, service_id or _EMPTY, task or _EMPTY, float(value)),
        )
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres insert failed for %s: %s", metric, e)


# ── Read path ────────────────────────────────────────────────────────────────


def get_batch(*, since: datetime | str) -> dict:
    """Return every series newer than ``since``, grouped by series key.

    Series key shape: ``"{metric}"`` for global, ``"{metric}|{service_id}"``
    for per-service, ``"{metric}|{service_id}|{task}"`` for per-task. The
    admin Trends page does one round-trip; the frontend partitions
    by metric prefix.
    """
    if isinstance(since, str):
        since = parse_relative_time_window(since)
    try:
        with closing(_pg_connection()) as con:
            rows = con.execute(
                """
                SELECT metric, service_id, task, ts, value
                FROM metric_snapshots
                WHERE ts >= %s
                ORDER BY ts ASC
                """,
                (iso_z(since),),
            ).fetchall()
        return _group_batch_rows(rows)
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres batch read failed: %s", e)
        return {}


# ── Liveness ─────────────────────────────────────────────────────────────────


def last_snapshot_age_s() -> float | None:
    """Seconds since the most recent metric_snapshots row, or ``None`` if empty.

    SRE-06: the minute-cadence sampler (:mod:`backend.cron.jobs.metric_snapshot`)
    is a *global* APScheduler interval job, so a stale ``max(ts)`` is a direct
    witness that the scheduler thread has stopped ticking.
    """
    try:
        with closing(_pg_connection()) as con:
            row = con.execute("SELECT max(ts) AS latest FROM metric_snapshots").fetchone()
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres liveness read failed: %s", e)
        return None
    latest = row["latest"] if row else None
    if not latest:
        return None
    from backend.utils.date_utils import parse_iso_utc

    dt = parse_iso_utc(latest)
    if dt is None:
        return None
    return max(0.0, (datetime.now(UTC) - dt).total_seconds())


# ── Retention ────────────────────────────────────────────────────────────────


def purge_old(retention_days: int = 30) -> int:
    """Delete rows older than ``retention_days``. Returns the row count."""
    if retention_days <= 0:
        return 0
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    try:
        con = _pg_write_connection()
        cur = con.execute("DELETE FROM metric_snapshots WHERE ts < %s", (cutoff,))
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres purge failed: %s", e)
        return 0


# ── Test / teardown helpers ──────────────────────────────────────────────────


def close_all_connections() -> None:
    """Close any thread-local connection (no-op under Postgres)."""
    pass


_MAX_BATCH_POINTS_PER_SERIES = 240


def _downsample_rows(series_rows: list, max_points: int = _MAX_BATCH_POINTS_PER_SERIES) -> list[dict]:
    n = len(series_rows)
    if n <= max_points or max_points < 4:
        return [{"ts": r["ts"], "value": r["value"]} for r in series_rows]
    step = (n - 1) / float(max_points - 3)
    indices = {int(round(i * step)) for i in range(max_points - 2)}
    indices.add(0)
    indices.add(n - 1)
    min_idx = min(range(n), key=lambda idx: series_rows[idx]["value"])
    max_idx = max(range(n), key=lambda idx: series_rows[idx]["value"])
    indices.add(min_idx)
    indices.add(max_idx)
    return [{"ts": series_rows[idx]["ts"], "value": series_rows[idx]["value"]} for idx in sorted(indices)]


def _group_batch_rows(rows) -> dict[str, list[dict]]:
    raw_grouped: dict[str, list] = {}
    for r in rows:
        key = r["metric"]
        svc = r["service_id"]
        if svc:
            key = f"{key}|{svc}"
            task = r["task"]
            if task:
                key = f"{key}|{task}"
        raw_grouped.setdefault(key, []).append(r)
    return {key: _downsample_rows(series_rows) for key, series_rows in raw_grouped.items()}


def teardown() -> None:
    """Clean up snapshots for test isolation."""
    try:
        con = _pg_write_connection()
        con.execute("TRUNCATE TABLE metric_snapshots")
    except Exception:
        logger.debug("teardown: con.execute failed (ignored)", exc_info=True)


# ── Latest operational samples exported through the configured OTel meter ────

_lock = threading.Lock()
_values: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}
_instruments: dict[str, Any] = {}

_METRICS = {
    "celery_queue_depth": ("Celery broker queue depth", "{messages}"),
    "celery_broker_reachable": ("Celery broker reachability", "1"),
    "celery_active_workers": ("Active Celery workers", "{workers}"),
    "celery_active_tasks": ("Active Celery tasks", "{tasks}"),
    "ingest_ledger_rows": ("Ingest ledger rows by state", "{rows}"),
    "high_scale_source_objects": ("High-scale source objects by state", "{objects}"),
    "high_scale_oldest_active_age_seconds": ("Age of the oldest active high-scale source object", "s"),
    "high_scale_publication_lag_seconds": ("Age of the oldest pending high-scale publication", "s"),
    "high_scale_pending_manifests": ("Pending high-scale publication manifests", "{manifests}"),
    "high_scale_rows_published_total": ("High-scale rows published", "{rows}"),
    "high_scale_disk_mode_level": ("High-scale serving-volume disk mode (0 normal .. 3 emergency)", "1"),
    "high_scale_disk_free_fraction": ("High-scale serving-volume free-space fraction", "1"),
}


def _instrument(metric: str) -> Any:
    instrument = _instruments.get(metric)
    if instrument is not None:
        return instrument

    description, unit = _METRICS[metric]

    def callback(_options: CallbackOptions, metric_name: str = metric) -> list[Observation]:
        with _lock:
            samples = dict(_values.get(metric_name, {}))
        return [Observation(value, dict(attributes)) for attributes, value in samples.items()]

    instrument = get_meter().create_observable_gauge(
        name=f"fla_{metric}",
        callbacks=[callback],
        description=description,
        unit=unit,
    )
    _instruments[metric] = instrument
    return instrument


def record(metric: str, value: float, **attributes: str) -> None:
    """Publish the latest value for an operational metric and its labels."""
    if metric not in _METRICS:
        raise ValueError(f"unsupported operational metric: {metric}")
    key = tuple(sorted((name, str(value)) for name, value in attributes.items()))
    with _lock:
        _values.setdefault(metric, {})[key] = float(value)
    _instrument(metric)


def reset_for_tests() -> None:
    """Clear cached samples and instruments between isolated unit tests."""
    with _lock:
        _values.clear()
        _instruments.clear()


# ── DuckDB Recycle (folded from backend.core.duckdb_recycle) ─────────────────


def recycle_interval_min() -> float:
    """Interval (minutes) between recycles. 0 = disabled (the scheduler then
    never registers the job). Default 0."""
    raw = os.getenv("DUCKDB_RECYCLE_INTERVAL_MIN", "0")
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.0


def _recycle_rss_threshold_bytes() -> int:
    """Only recycle when current RSS exceeds this (MB → bytes); 0 = always
    recycle on the interval. Lets a quiet process skip needless cache drops."""
    raw = os.getenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "0")
    try:
        return max(0, int(raw)) * 1024 * 1024
    except (TypeError, ValueError):
        return 0


def _recycle_drain_timeout_s() -> float:
    raw = os.getenv("DUCKDB_RECYCLE_DRAIN_TIMEOUT_MS", "5000")
    try:
        return max(0.1, float(raw) / 1000.0)
    except (TypeError, ValueError):
        return 5.0


def _recycle_grace_s() -> float:
    raw = os.getenv("DUCKDB_RECYCLE_GRACE_MS", "3000")
    try:
        return max(0.0, float(raw) / 1000.0)
    except (TypeError, ValueError):
        return 3.0


def _recycle_lock_timeout_s() -> float:
    raw = os.getenv("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "2000")
    try:
        return max(0.0, float(raw) / 1000.0)
    except (TypeError, ValueError):
        return 2.0


def _sources_by_db_path() -> dict[str, list[dict]]:
    """Group every configured service's source dict by its abspath DuckDB file.

    Services sharing a db file share the DuckDB instance + its object cache, so
    they must be drained together for the instance to be destroyed.
    """
    groups: dict[str, list[dict]] = {}
    for cfg in svcconfig.list_configs():
        try:
            src = _db._source_from_config(cfg)
        except Exception:
            continue
        groups.setdefault(_db.db_path_for_source(src), []).append(src)

        rum_cfg = cfg.get("rum")
        rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
        rum_enabled = svcconfig.is_rum_enabled(cfg)
        if rum_enabled:
            try:
                from backend.core.duckdb import rum_source_for

                rum_src = rum_source_for(src)
                groups.setdefault(_db.db_path_for_source(rum_src), []).append(rum_src)
            except Exception:
                logger.debug("_sources_by_db_path: groups.setdefault failed (ignored)", exc_info=True)
    return groups


def _service_key(src: dict) -> str:
    return src.get("name") or src.get("service_id") or "default"


def _lock_key(src: dict) -> str:
    # Matches iceberg.view._get_service_lock keying (source.get("name", "default")).
    return src.get("name", "default")


def _trim_memory() -> None:
    """Trigger Python GC and C allocator memory trim (glibc malloc_trim) if available."""
    gc.collect()
    try:
        import ctypes

        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        logger.debug("_trim_memory: ctypes.CDLL failed (ignored)", exc_info=True)


def _recycle_db_path(db_path: str, sources: list[dict]) -> dict:
    """Recycle the DuckDB instance for one db file. Returns a status dict."""
    from backend.core.iceberg.view import _get_service_lock

    drain_timeout = _recycle_drain_timeout_s()
    grace = _recycle_grace_s()
    lock_timeout = _recycle_lock_timeout_s()
    service_keys = [_service_key(s) for s in sources]

    # Acquire write/rebind locks in a deterministic global order (sorted lock
    # keys) so recycle can never deadlock against a writer holding one of them.
    lock_keys = sorted({_lock_key(s) for s in sources})
    acquired: list = []
    rss_before = _db.current_rss_bytes()
    try:
        for lk in lock_keys:
            lock = _get_service_lock(lk)
            if not lock.acquire(timeout=lock_timeout):
                logger.info(
                    "[recycle] %s: skip — write lock %r busy (a sync/commit is in flight)",
                    db_path,
                    lk,
                )
                return {"db_path": db_path, "status": "skipped_locked", "lock": lk}
            acquired.append(lock)

        # Locks held → raise the barrier so no NEW connection opens for this file.
        _db.set_recycle_barrier(db_path, True)
        retired_pools = {}
        try:
            retired_pools = _pool.begin_drain_pools(service_keys)
            drained = _pool.wait_pools_drained(retired_pools, drain_timeout)

            # gc so closed-but-not-yet-collected conn wrappers leave the WeakSet.
            _trim_memory()
            deadline = time.monotonic() + grace
            while _db.live_connection_count(db_path) > 0 and time.monotonic() < deadline:
                time.sleep(0.05)
                _trim_memory()
            live = _db.live_connection_count(db_path)
        finally:
            _pool.end_drain_pools(retired_pools or service_keys)
            _db.set_recycle_barrier(db_path, False)
    finally:
        for lock in reversed(acquired):
            try:
                lock.release()
            except RuntimeError:
                pass

    _trim_memory()
    rss_after = _db.current_rss_bytes()
    freed = (rss_before - rss_after) if (rss_before is not None and rss_after is not None) else None
    status = "recycled" if (drained and live == 0) else "incomplete"
    result = {
        "db_path": db_path,
        "status": status,
        "drained": drained,
        "live_after": live,
        "rss_before": rss_before,
        "rss_after": rss_after,
        "freed_bytes": freed,
    }
    freed_mb = f"{freed / 1e6:.0f}MB" if freed is not None else "n/a"
    logger.info(
        "[recycle] %s: %s (drained=%s live_after=%d freed=%s)",
        db_path,
        status,
        drained,
        live,
        freed_mb,
    )
    return result


def recycle_once(reason: str = "interval") -> str:
    """Recycle every distinct DuckDB instance once. Returns a one-line detail.

    Never raises — each db file is recycled independently and abort-safe.
    """
    groups = _sources_by_db_path()
    if not groups:
        return "no services configured"
    results = []
    for db_path, sources in groups.items():
        try:
            results.append(_recycle_db_path(db_path, sources))
        except Exception as e:  # defensive — a recycle must never crash the job
            logger.warning("[recycle] %s: errored: %s", db_path, e, exc_info=True)
            results.append({"db_path": db_path, "status": "error", "error": str(e)})
    recycled = sum(1 for r in results if r.get("status") == "recycled")
    freed = sum(r.get("freed_bytes") or 0 for r in results if (r.get("freed_bytes") or 0) > 0)
    incomplete = sum(1 for r in results if r.get("status") in ("incomplete", "skipped_locked", "error"))
    extra = f" (incomplete: {incomplete})" if incomplete else ""
    return f"{reason}: recycled {recycled}/{len(results)} instance(s), freed ~{freed / 1e6:.0f}MB{extra}"


def graceful_restart_rss_threshold_bytes() -> int:
    """Restart threshold (``BACKEND_GRACEFUL_RESTART_RSS_MB``, MB→bytes).

    0 (default) disables the guard entirely.
    """
    raw = os.getenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "0")
    try:
        return max(0, int(raw)) * 1024 * 1024
    except (TypeError, ValueError):
        return 0


def maybe_graceful_restart() -> bool:
    """SIGTERM self for a clean docker restart if RSS is at/over the threshold.

    Returns True iff a restart was triggered. Never raises — a guard that
    crashes the scheduler tick would defeat its purpose.
    """
    threshold = graceful_restart_rss_threshold_bytes()
    if threshold <= 0:
        return False
    try:
        from backend.core.duckdb import current_rss_bytes

        rss = current_rss_bytes()
    except Exception:
        return False
    if rss is None or rss < threshold:
        return False
    logger.warning(
        "\U0001f501 [memguard] RSS %.0fMB >= %.0fMB restart threshold — sending "
        "SIGTERM for a graceful restart (uvicorn drains + exits 0; docker "
        "restart:unless-stopped brings up a fresh process). STOPGAP for the "
        "unresolved memory growth; see backend-oom-restart-loop.",
        rss / 1e6,
        threshold / 1e6,
    )
    os.kill(os.getpid(), signal.SIGTERM)
    return True


# ── run_duckdb_recycle ───────────────────────────────────────────────────────


def _maybe_adjust_recycle_schedule(*, expedite: bool) -> None:
    """Dynamically adjust recycle interval under memory pressure (e.g. tighten to 2m, or relax back to baseline)."""
    try:
        from backend.cron.scheduler import get_scheduler

        sched = get_scheduler()
        job = sched.get_job("duckdb_recycle")
        if not job:
            return

        baseline_mins = max(1.0, recycle_interval_min() or 60.0)
        target_mins = min(2.0, baseline_mins) if expedite else baseline_mins

        current_trigger = getattr(job, "trigger", None)
        current_interval_s = getattr(getattr(current_trigger, "interval", None), "total_seconds", lambda: 0)()

        if int(current_interval_s) != int(target_mins * 60):
            job.reschedule("interval", minutes=target_mins)
            logger.info(
                "⚡ [duckdb_recycle] Dynamically adjusted recycle interval to %.1fm (expedite=%s)",
                target_mins,
                expedite,
            )
    except Exception as e:
        logger.debug("[duckdb_recycle] Could not adjust recycle schedule: %s", e)


@global_job("duckdb_recycle", color="35", tag="recycle", label="DuckDB recycle")
def run_duckdb_recycle() -> str:
    """Scheduler entry: recycle on the interval, gated by the optional RSS
    threshold (skip when the process is still small — avoids needless cold
    caches). Wrapped by ``global_job`` for start/end logging + record_job_run.
    """
    # Process-level OOM stopgap FIRST and independent of the recycle threshold:
    # if RSS is already over the restart threshold, a clean self-restart is the
    # only thing that reliably reclaims it (recycle frees ~0 of the real leak).
    if maybe_graceful_restart():
        return "graceful restart triggered (RSS over restart threshold)"
    threshold = _recycle_rss_threshold_bytes()
    rss = _db.current_rss_bytes()
    if threshold > 0 and rss is not None:
        if rss < threshold:
            if rss >= int(threshold * 0.8):
                _maybe_adjust_recycle_schedule(expedite=True)
            elif rss < int(threshold * 0.5):
                _maybe_adjust_recycle_schedule(expedite=False)
            return f"skipped: RSS {rss / (1024 * 1024):.0f}MB < threshold {threshold / (1024 * 1024):.0f}MB"

    res = recycle_once(reason="interval")

    # Evaluate post-recycle memory to adapt schedule for next tick
    rss_after = _db.current_rss_bytes()
    if "incomplete" in res:
        _maybe_adjust_recycle_schedule(expedite=True)
    elif threshold > 0 and rss_after is not None:
        if rss_after >= int(threshold * 0.8):
            _maybe_adjust_recycle_schedule(expedite=True)
        elif rss_after < int(threshold * 0.5):
            _maybe_adjust_recycle_schedule(expedite=False)
    elif threshold == 0:
        _maybe_adjust_recycle_schedule(expedite=False)

    return res


# ── Insights prewarmer (folded from backend.cron.jobs.insights_prewarmer) ────

from backend.utils.bounded_cache import CacheRegistry  # noqa: E402

_MAX_ANALYST_SHAPES = 8
_INGEST_TASKS = ("log_discovery", "rum_sync")
DEFAULT_PREWARM_IDLE_MINUTES = 30.0

_last_full_warm: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_full_warm", _last_full_warm)

_last_insights_request: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_insights_request", _last_insights_request)


def record_insights_request(service_id: str | None) -> None:
    """Record that ``/api/insights`` was requested for ``service_id`` so the
    demand-driven prewarmer keeps the cache warm for the configured idle window."""
    if service_id:
        _last_insights_request[service_id] = time.monotonic()


def _prewarm_idle_minutes(service_id: str, src: dict | None = None) -> float:
    """Return the idle timeout in minutes before prewarming pauses for ``service_id``."""
    prov = (src or {}).get("provisioning") if isinstance(src, dict) else None
    if not isinstance(prov, dict):
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id) or {}
        prov = cfg.get("provisioning") if isinstance(cfg.get("provisioning"), dict) else {}
    cron_ip = prov.get("cron_insights_prewarmer") if isinstance(prov, dict) else None
    if isinstance(cron_ip, dict) and cron_ip.get("idle_minutes") is not None:
        try:
            return float(cron_ip["idle_minutes"])
        except (TypeError, ValueError):
            pass

    env_val = os.environ.get("INSIGHTS_PREWARM_IDLE_MINUTES", "").strip()
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            pass

    return DEFAULT_PREWARM_IDLE_MINUTES


def is_insights_active(service_id: str, src: dict | None = None, *, idle_minutes: float | None = None) -> bool:
    """Return True if ``/api/insights`` was requested for ``service_id`` within
    ``idle_minutes`` (or if ``idle_minutes <= 0``)."""
    limit_mins = _prewarm_idle_minutes(service_id, src) if idle_minutes is None else float(idle_minutes)
    if limit_mins <= 0:
        return True
    last_req = _last_insights_request.get(service_id)
    if last_req is None and isinstance(src, dict):
        alt_id = src.get("service_id") or src.get("name")
        if isinstance(alt_id, str) and alt_id:
            last_req = _last_insights_request.get(alt_id)
    if last_req is None:
        return False
    return (time.monotonic() - last_req) < (limit_mins * 60.0)


def _running_ingest(service_id: str) -> str | None:
    from backend.cron_progress import active_task_for_service

    return active_task_for_service(service_id, _INGEST_TASKS)


def _cache_cold(service_id: str) -> bool:
    """True when the oldest prewarmed entry has reached the cache TTL (or
    nothing was ever fully warmed here) — the tick must not yield to ingest."""
    from backend.repositories.insights.repository import INSIGHTS_CACHE_TTL

    last = _last_full_warm.get(service_id)
    return last is None or time.monotonic() - last >= INSIGHTS_CACHE_TTL


def _analyst_prewarm_enabled() -> bool:
    """Analyst-shape prewarm is on by default; ``INSIGHTS_PREWARM_ANALYST=0``
    is a belt-and-braces kill switch (re-read each call so tests can flip it)."""
    return os.environ.get("INSIGHTS_PREWARM_ANALYST", "1").strip().lower() not in ("0", "false", "no", "off")


def _active_analyst_shapes(service_id: str) -> list[tuple[str | None, str | None, int | None, bool]]:
    """Distinct ``(query_start_time, query_end_time, query_window_hours,
    mask_ips)`` tuples across the service's active (non-revoked, non-expired)
    analyst invites."""
    from backend.core import share_db
    from backend.utils.date_utils import parse_iso_utc

    now = datetime.now(UTC)
    shapes: set[tuple[str | None, str | None, int | None, bool]] = set()
    for inv in share_db.get_remote_invites():
        if inv.get("revoked"):
            continue
        exp = inv.get("expires_at")
        if exp:
            pe = parse_iso_utc(exp)
            if pe is not None and pe < now:
                continue
        if service_id not in (inv.get("service_ids") or []):
            continue
        policy = inv.get("pii_policy")
        mask = bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
        shapes.add(
            (
                inv.get("query_start_time"),
                inv.get("query_end_time"),
                inv.get("query_window_hours"),
                mask,
            )
        )

    ordered = sorted(shapes, key=lambda s: (s[0] or "", s[1] or "", s[2] or 0, s[3]))
    if len(ordered) > _MAX_ANALYST_SHAPES:
        logger.warning(
            "[insights-prewarmer] %s: %d analyst shapes, warming first %d",
            service_id,
            len(ordered),
            _MAX_ANALYST_SHAPES,
        )
        ordered = ordered[:_MAX_ANALYST_SHAPES]
    return ordered


@cron_task("insights_prewarmer", job_name="insights_prewarmer")
def _run_insights_prewarmer(service_id: str) -> None:
    """Warm the default insights selection — the pair the adaptive frontend
    picker will request for this service's history, for the admin/unclamped
    entry plus each active analyst clamp shape — so the first user (admin OR
    analyst) lands on a cache hit instead of the cold path."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.repositories.insights import get_insights
    from backend.utils.active_requests import should_defer_cron, yield_to_api
    from backend.utils.remote_access import resolve_analyst_insights_clamp
    from backend.utils.time_window import history_hours_from_earliest, pick_insights_default
    from backend.utils.tunnel import get_tunnel_manager

    src = get_source_for_service(service_id)
    if src is None:
        return

    if should_defer_cron("insights_prewarmer", service_id):
        return

    idle_mins = _prewarm_idle_minutes(service_id, src)
    if not is_insights_active(service_id, src, idle_minutes=idle_mins):
        logger.info(
            "[insights-prewarmer] %s: skipping — idle (no /api/insights requests in the last %gm)",
            service_id,
            idle_mins,
        )
        return

    must_run = _cache_cold(service_id)
    if not must_run:
        ingest_task = _running_ingest(service_id)
        if ingest_task is not None:
            logger.info(
                "[insights-prewarmer] %s: deferring — %s in flight and cache still warm",
                service_id,
                ingest_task,
            )
            return

    _display = _display_label(src, service_id)

    con = None
    with cron_run(
        service_id,
        "insights_prewarmer",
        source=src,
        error_summary=lambda exc: f"Prewarm failed: {exc}",
        finalize_duration=True,
        include_log_output=False,
        skip_log_prefix="insights-prewarmer",
    ) as run:
        if not run.ok:
            return
        try:
            cached_status = svcconfig.get_status(src["name"]) or {}
            history_hours = history_hours_from_earliest(cached_status.get("earliest_log_at"))
            window_hours, baseline_hours = pick_insights_default(history_hours)

            con = get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)

            # Serial insight queries leave headroom for foreground requests.
            # Never change DuckDB thread count or memory_limit here:
            # both are instance-global, so they'd clobber every concurrent query, and
            # changing thread count mid-flight deadlocks DuckDB (AGENTS.md Trap #42).

            yield_to_api(max_wait_secs=1.0)

            get_insights(
                con,
                src,
                window_hours=window_hours,
                baseline_hours=baseline_hours,
                service_id=service_id,
                force_refresh=True,
                max_workers=1,
            )

            analyst_warmed = 0
            stopped_for: str | None = None
            complete = True
            if _analyst_prewarm_enabled() and get_tunnel_manager().is_sharing_active():
                for qs, qe, qwh, mask in _active_analyst_shapes(service_id):
                    if not must_run:
                        stopped_for = _running_ingest(service_id)
                        if stopped_for is not None:
                            logger.info(
                                "[insights-prewarmer] %s: stopping early — %s started mid-tick",
                                service_id,
                                stopped_for,
                            )
                            complete = False
                            break
                    if should_defer_cron("insights_prewarmer", service_id):
                        logger.info(
                            "[insights-prewarmer] %s: deferring remaining analyst prewarms due to active API requests",
                            service_id,
                        )
                        complete = False
                        break
                    yield_to_api(max_wait_secs=0.5)
                    try:
                        cs, ce, ck = resolve_analyst_insights_clamp(
                            qs,
                            qe,
                            qwh,
                            baseline_hours=baseline_hours,
                            window_hours=window_hours,
                        )
                    except ValueError:
                        continue
                    get_insights(
                        con,
                        src,
                        window_hours=window_hours,
                        baseline_hours=baseline_hours,
                        service_id=service_id,
                        clamp_start=cs,
                        clamp_end=ce,
                        mask_ips=mask,
                        clamp_cache_key=ck,
                        force_refresh=True,
                        max_workers=1,
                    )
                    analyst_warmed += 1

            duration = run.elapsed()
            summary = (
                f"Prewarmed default insights selection ({window_hours:g}h/{baseline_hours:g}h: "
                f"admin + {analyst_warmed} analyst shape{'' if analyst_warmed == 1 else 's'})"
            )
            if stopped_for is not None:
                summary += f"; stopped early: {stopped_for} in flight"
            if complete:
                _last_full_warm[service_id] = time.monotonic()
            run.success(summary)
            run.log_progress({"type": "done", "message": summary})
            logger.info(
                "✅ [insights-prewarmer] %s: prewarmed %gh/%gh in %.2fs (admin + %d analyst)",
                _display,
                window_hours,
                baseline_hours,
                duration,
                analyst_warmed,
            )
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass


_self_mod = _sys.modules[__name__]
for _alias in (
    "backend.core.metric_snapshots",
    "backend.core.duckdb_recycle",
    "backend.cron.jobs.duckdb_recycle",
    "backend.cron.jobs.insights_prewarmer",
):
    _sys.modules[_alias] = _self_mod
_core_pkg = _sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "metric_snapshots", _self_mod)
    setattr(_core_pkg, "duckdb_recycle", _self_mod)
_jobs_pkg = _sys.modules.get("backend.cron.jobs")
if _jobs_pkg is not None:
    setattr(_jobs_pkg, "duckdb_recycle", _self_mod)
    setattr(_jobs_pkg, "insights_prewarmer", _self_mod)
_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.metric_snapshots": "backend.cron.jobs.metadata",
            "backend.core.duckdb_recycle": "backend.cron.jobs.metadata",
            "backend.cron.jobs.duckdb_recycle": "backend.cron.jobs.metadata",
            "backend.cron.jobs.insights_prewarmer": "backend.cron.jobs.metadata",
        }
    )
