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

import httpx
import tenacity

from backend.cron.decorators import cron_task, global_job
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_label,
    _elapsed_since,
    _log_and_add_progress,
)

logger = logging.getLogger("backend.scheduler")


_RETRYABLE_WEBHOOK_STATUS = (429, 500, 502, 503, 504)


def _is_retryable_webhook_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_WEBHOOK_STATUS
    return isinstance(exc, httpx.TransportError)


@tenacity.retry(
    retry=tenacity.retry_if_exception(_is_retryable_webhook_error),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def _post_alert_webhook(url: str, payload: dict) -> None:
    response = httpx.post(url, json=payload, timeout=5)
    if response.status_code in _RETRYABLE_WEBHOOK_STATUS:
        response.raise_for_status()


@tenacity.retry(
    retry=tenacity.retry_if_exception(_is_retryable_webhook_error),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def _post_slack_notification(url: str, payload: dict) -> None:
    response = httpx.post(url, json=payload, timeout=5)
    if response.status_code in _RETRYABLE_WEBHOOK_STATUS:
        response.raise_for_status()


@tenacity.retry(
    retry=tenacity.retry_if_exception(_is_retryable_webhook_error),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
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
    response = httpx.post(url, json=payload, timeout=5)
    if response.status_code in _RETRYABLE_WEBHOOK_STATUS:
        response.raise_for_status()


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
    logger.info("🏎️  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job started.", _display)
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
            log_cron_run(src, "metadata_sync", time.time() - start_time_exec, "success", summary=msg, run_id=run_id)
            _log_and_add_progress(run_id, service_id, job_name="metadata_sync", event={"type": "done", "message": msg})
            end_progress(run_id)
            return

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
        # read_only=False is load-bearing: a read-only connection makes the
        # slow-path rebuild bind a TEMP view that dies with this connection
        # moments later — the cron would pay the full rebuild cost for a
        # no-op and the persistent per-service view would never refresh.
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

    logger.info("🏁  \x1b[96m[metadata_sync]\x1b[0m %s: Metadata sync job finished.", _display)


# _run_metadata_sync can't take @cron_task (it doubles as a bootstrap helper
# with extra kwargs), but external/Celery scheduling requires a registered
# task — without this wrapper, RedBeat dispatches an unregistered name and
# read-only (analyst) services silently stop refreshing.
from backend.celery_app import app as _celery_app  # noqa: E402


@_celery_app.task(name=f"{_run_metadata_sync.__module__}._run_metadata_sync_celery", bind=True)
def _run_metadata_sync_celery(self, service_id: str, *args, **kwargs):
    return _run_metadata_sync(service_id, *args, **kwargs)


_run_metadata_sync.celery_task = _run_metadata_sync_celery  # type: ignore[attr-defined]
_run_metadata_sync.delay = _run_metadata_sync_celery.delay  # type: ignore[attr-defined]


# ── _run_ngwaf_bot_sync ──────────────────────────────────────────────────────


@cron_task("sync_ngwaf_bots", job_name="ngwaf_sync")
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
    logger.info("🏎️  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job started.", svc_display)

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
        from backend.utils.date_utils import iso_z_now

        now_ts = iso_z_now()
        update_sync_watermark(workspace_id, now_ts)
        summary = (
            f"First sync — seeded watermark at {now_ts}. Next cycle will fetch new bot records from this point forward."
        )
        log_cron_run(src, "ngwaf_sync", 0.0, "success", summary=summary, run_id=run_id)
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
        logger.info("🏁  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)
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
        log_cron_run(
            src,
            "ngwaf_sync",
            time.time() - start_time,
            "success",
            files_downloaded=total_records,
            summary=summary,
            run_id=run_id,
        )
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
    except Exception as e:
        log_cron_run(
            src,
            "ngwaf_sync",
            time.time() - start_time,
            "error",
            files_downloaded=total_records if "total_records" in locals() else 0,
            error_message=str(e),
            summary="NGWAF sync failed",
            run_id=run_id,
        )
        _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "error", "message": str(e)})
        logger.exception("[ngwaf_sync] %s: sync failed: %s", svc_display, e)

    logger.info("🏁  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)


# ── _run_bot_data_refresh / _run_rdns_enrichment / _run_share_audit_purge ────


@global_job("bot_data_refresh", color="36", tag="bots", label="Bot data refresh")
def _run_bot_data_refresh() -> str:
    """Fetch and cache all enabled bot sources (nightly 02:00 UTC)."""
    from backend.utils.bot_sources import refresh_all_sources

    results = refresh_all_sources()
    total = sum(r.get("entry_count", 0) for r in results)
    logger.info("✅ \x1b[36m[bots]\x1b[0m Refreshed %d source(s), %d total entries", len(results), total)
    return f"Updated {len(results)} source(s), {total} total entries"


@global_job("rdns_enrichment", color="34", tag="rdns", label="rDNS enrichment")
def _run_rdns_enrichment() -> str:
    """Resolve pending rDNS lookups and discover new IPs (every 5 min)."""
    from backend.utils.rdns_cache import enrich_batch

    summary = enrich_batch()
    return f"resolved={summary['resolved']} errors={summary['errors']} discovered={summary['discovered']}"


@global_job("share_audit_purge", color="35", tag="share_audit_purge", label="Share audit purge")
def _run_share_audit_purge() -> str:
    """Drop remote-share audit rows older than the retention window (daily 03:45 UTC).

    Retention is read from the `share_audit_retention_days` setting, defaulting
    to 90 days. The companion endpoint is `share_db.purge_old_audit_logs`.
    """
    from backend.core import share_db

    raw = share_db.get_setting(share_db.SHARE_AUDIT_RETENTION_DAYS_KEY, "90")
    try:
        retention = max(1, int(raw or "90"))
    except (TypeError, ValueError):
        retention = 90
    deleted = share_db.purge_old_audit_logs(retention_days=retention)
    logger.info(
        "✅ \x1b[35m[share_audit_purge]\x1b[0m Deleted %d row(s) older than %d days.",
        deleted,
        retention,
    )
    return f"deleted={deleted} retention_days={retention}"


# ── _run_service_alerts_evaluation ───────────────────────────────────────────


@cron_task("evaluate_alerts", job_name="alerts")
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
    _display = _display_label(src, service_id)
    logger.info("🏎️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job started.", _display)

    # Fetch alerts from per-service metadata SQLite (no DuckDB needed).
    alerts = alert_repo.get_alerts(service_id=service_id)
    enabled_alerts = [a for a in alerts if a["enabled"]]
    # DuckDB connection is only needed if we actually have alerts to evaluate.
    con_ro = get_connection(src, read_only=True) if enabled_alerts else None

    if not enabled_alerts:
        logger.info("🔔 \x1b[93m[alerts]\x1b[0m %s: No alerts configured, skipping.", _display)
        log_cron_run(src, task_name, time.monotonic() - start, "skipped", summary="No alerts configured")
        logger.info("🏁  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job finished.", _display)
        return
    # Past this point enabled_alerts is non-empty, so con_ro was opened
    # above — narrow for mypy.
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
                                        "text": {"type": "mrkdwn", "text": f"🚨 *Alert Triggered: {alert['name']}*"},
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
                        logger.error(
                            "%s Failed to send notifications to channel %s for alert %s: %s",
                            JOB_COLORS["alerts"] + "[alerts]" + RESET_COLOR,
                            channel.get("type"),
                            alert["id"],
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
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.metadata import cleanup_metadata

    src = get_source_for_service(service_id)
    if src is None:
        return

    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    _display = _display_label(src, service_id)
    color = JOB_COLORS.get("metadata_cleanup", "")
    label = f"{color}[metadata_cleanup]{RESET_COLOR}"
    logger.info("🏎️  %s %s: Starting metadata cleanup.", label, _display)

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
        logger.info("🏁  %s %s: no rows to trim (took %.2fs)", label, _display, result["duration_s"])

    # Also trim the global system_metrics.db retention window. Idempotent
    # across per-service runs (the second call this day deletes 0 rows
    # because the first one already cleared them), and the DELETE rides
    # the (metric, ts) index so the cost is microseconds even when there's
    # nothing to do. 30-day window matches the in-app Trends tab range
    # (1h / 24h / 7d) plus a buffer.
    try:
        from backend.core import metric_snapshots

        metric_snapshots.purge_old(retention_days=30)
    except Exception as e:
        logger.debug("[metadata_cleanup] metric_snapshots purge failed: %s", e)

    try:
        from backend.core.metadata.quarantine import delete_quarantined_rows, get_expired_quarantined_files

        expired = get_expired_quarantined_files(service_id, retention_days=14)
        if expired:
            from backend.core.duckdb import _get_fos_client
            from backend.core.ingest import _delete_objects_robust

            fos_client = _get_fos_client(src)
            keys_to_delete = []
            ids_to_delete = []
            for row in expired:
                keys_to_delete.append(row["error_key"])
                keys_to_delete.append(row["meta_key"])
                ids_to_delete.append(row["id"])
            if keys_to_delete:
                _delete_objects_robust(fos_client, src["bucket"], keys_to_delete)
            if ids_to_delete:
                delete_quarantined_rows(service_id, ids_to_delete)
            logger.info("[metadata_cleanup] %s: purged %d expired quarantined files", service_id, len(expired))
    except Exception as e:
        logger.debug("[metadata_cleanup] quarantine purge failed: %s", e)

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
