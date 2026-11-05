"""Metadata sync, bot enrichment, alert evaluation, and compatibility shims."""

from __future__ import annotations

import importlib
import logging
import math
import os
import shutil
import sys as _sys
import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import TYPE_CHECKING, Any

import httpx
import tenacity

from backend import config as svcconfig
from backend.core.fastly.client import fastly_paginate
from backend.core.fos_client import get_source_for_service
from backend.core.metadata.base import (
    get_pg_readonly_connection,
    get_pg_thread_connection,
)
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_label,
    _log_and_add_progress,
    cron_run,
    cron_task,
    global_job,
    record_skip_row,
)
from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc

if TYPE_CHECKING:
    from backend.cron.jobs.insights_prewarmer import _run_insights_prewarmer as _run_insights_prewarmer
    from backend.cron.jobs.maintenance import (
        _run_metadata_cleanup as _run_metadata_cleanup,
    )

_os = os
_shutil = shutil
logger = logging.getLogger("backend.scheduler")
_RETRYABLE_WEBHOOK_STATUS = (429, 500, 502, 503, 504)
_API_PREFIX = "/ngwaf/v1"


# ── Fastly NGWAF API client & Postgres bot cache helpers ─────────────────────


def _get_relative_time_range(ts_str: str) -> str:
    """Convert an ISO timestamp to a rounded-up relative NGWAF time string (e.g. -5d, -15min)."""
    ts = parse_iso_utc(ts_str)
    if ts is None:
        if ts_str.startswith("-"):
            return ts_str
        return "-1d"

    delta = datetime.now(UTC) - ts
    seconds = delta.total_seconds()

    if seconds > 86400:
        days = math.ceil(seconds / 86400)
        return f"-{days}d"

    if seconds > 3600:
        hours = math.ceil(seconds / 3600)
        return f"-{hours}h"

    mins = math.ceil(seconds / 900) * 15
    return f"-{max(mins, 1)}min"


def _extract_bot_record(r: dict) -> dict | None:
    """Extract bot info from a single NGWAF request record. Returns None if no VERIFIED-BOT signal."""
    signals = r.get("signals", [])

    verified = next((s for s in signals if s.get("id") == "VERIFIED-BOT"), None)
    if not verified:
        return None

    subcat = next(
        (s for s in signals if s.get("id", "").startswith("VERIFIED-BOT.") and s["id"] != "VERIFIED-BOT"),
        None,
    )

    bot_name = verified.get("value")
    category = subcat["id"].replace("VERIFIED-BOT.", "") if subcat else None

    user_agent = r.get("user_agent") or next(
        (
            h["value"]
            for h in r.get("request_headers", [])
            if isinstance(h, dict) and h.get("name", "").lower() == "user-agent"
        ),
        None,
    )

    return {
        "waf_req_id": r.get("id"),
        "bot_name": bot_name,
        "category": category,
        "user_agent": user_agent,
        "server_name": r.get("server_name"),
        "timestamp": r.get("timestamp"),
    }


def fetch_verified_bots_paged(
    api_key: str,
    workspace_id: str,
    from_ts: str,
    until_ts: str | None = None,
    page_limit: int = 500,
) -> Generator[tuple[list[dict], str | None, int]]:
    """Yield (page_records, latest_timestamp, raw_page_count) for each page of VERIFIED-BOT requests."""
    from backend.core.fastly.client import is_mock_mode

    if is_mock_mode():
        yield [], None, 0
        return

    relative_from = _get_relative_time_range(from_ts)

    q = f"tag:VERIFIED-BOT from:{relative_from}"
    if until_ts:
        relative_until = _get_relative_time_range(until_ts)
        q += f" until:{relative_until}"

    for data in fastly_paginate(
        f"{_API_PREFIX}/workspaces/{workspace_id}/requests",
        token=api_key,
        style="cursor",
        params={"q": q, "limit": page_limit},
        page_delay=0.1,
    ):
        raw_data = data.get("data", [])
        records: list[dict] = []
        latest_ts: str | None = None

        for r in raw_data:
            record = _extract_bot_record(r)
            if record:
                records.append(record)
                ts = record.get("timestamp")
                if ts and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts

        yield records, latest_ts, len(raw_data)


def oldest_unenriched_timestamp(src: dict) -> str | None:
    """Return the oldest request_facts timestamp with a VERIFIED-BOT waf_req_id."""
    try:
        from backend.core.clickhouse import get_clickhouse_client
        from backend.high_scale._common import visible_publication_fence_sql

        service_id = str(src.get("service_id") or src.get("name") or "")
        client = get_clickhouse_client()
        if client is None or not service_id:
            return None
        fence = visible_publication_fence_sql("request", client=client, service_id=service_id)
        rows = client.execute(
            f"SELECT min(event_timestamp) AS min_ts FROM request_facts "
            f"WHERE service_id={{service_id:String}} AND {fence} "
            f"AND custom_fields['waf_req_id'] != '' "
            f"AND position(custom_fields['waf_sig'], 'VERIFIED-BOT') > 0",
            {"service_id": service_id},
        )
        ts = rows[0].get("min_ts") if rows else None
        if ts:
            return iso_z(ts) if hasattr(ts, "strftime") else str(ts)
    except Exception:
        logger.debug("oldest_unenriched_timestamp: query failed (ignored)", exc_info=True)
    return None


def get_last_timestamp(workspace_id: str) -> str | None:
    """Return last_timestamp_synced for workspace, or None if no sync has run yet."""
    con = get_pg_readonly_connection()
    try:
        row = con.execute(
            "SELECT last_timestamp_synced FROM ngwaf_sync_state WHERE workspace_id = ?",
            (workspace_id,),
        ).fetchone()
        if row and row["last_timestamp_synced"]:
            return row["last_timestamp_synced"]
        return None
    finally:
        con.close()


def update_sync_watermark(workspace_id: str, until_ts: str) -> None:
    """Advance the high-water mark to until_ts after a completed scan."""
    con = get_pg_thread_connection()
    try:
        con.execute(
            """
            INSERT INTO ngwaf_sync_state (workspace_id, last_timestamp_synced)
            VALUES (?, ?)
            ON CONFLICT (workspace_id) DO UPDATE SET last_timestamp_synced = EXCLUDED.last_timestamp_synced
            """,
            (workspace_id, until_ts),
        )
        con.commit()
    finally:
        con.close()


def upsert_bots(records: list[dict], workspace_id: str, latest_timestamp: str | None) -> None:
    """Insert or replace bot records and update sync state in one transaction. Idempotent."""
    con = get_pg_thread_connection()
    now = iso_z_now()
    rows = [
        (
            r["waf_req_id"],
            r.get("bot_name"),
            r.get("category"),
            r.get("wellknown_bot_id"),
            r.get("wellknown_bot_name"),
            now,
        )
        for r in records
        if r.get("waf_req_id")
    ]
    try:
        if rows:
            con.executemany(
                """
                INSERT INTO ngwaf_bots
                    (waf_req_id, bot_name, category, wellknown_bot_id, wellknown_bot_name, synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (waf_req_id) DO UPDATE SET
                    bot_name = EXCLUDED.bot_name,
                    category = EXCLUDED.category,
                    wellknown_bot_id = EXCLUDED.wellknown_bot_id,
                    wellknown_bot_name = EXCLUDED.wellknown_bot_name,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )
        if latest_timestamp:
            try:
                _pts = parse_iso_utc(latest_timestamp)
                next_ts = iso_z(_pts + timedelta(seconds=1)) if _pts else latest_timestamp
            except ValueError:
                next_ts = latest_timestamp
            con.execute(
                """
                INSERT INTO ngwaf_sync_state (workspace_id, last_timestamp_synced)
                VALUES (?, ?)
                ON CONFLICT (workspace_id) DO UPDATE SET last_timestamp_synced = EXCLUDED.last_timestamp_synced
                """,
                (workspace_id, next_ts),
            )
        con.commit()
    finally:
        con.close()


def cleanup_old_bots(retention_days: int) -> int:
    """Delete rows with synced_at older than retention_days. Returns deleted row count."""
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con = get_pg_thread_connection()
    try:
        cur = con.execute("DELETE FROM ngwaf_bots WHERE synced_at < ?", (cutoff,))
        con.commit()
        return cur.rowcount or 0
    finally:
        con.close()


def get_cache_stats() -> dict:
    """Return summary statistics of cached NGWAF bot records."""
    con = get_pg_readonly_connection()
    try:
        cur = con.execute("SELECT count(*) FROM ngwaf_bots")
        total_bots = cur.fetchone()[0]

        workspaces: dict[str, str | None] = {}
        cur = con.execute("SELECT workspace_id, last_timestamp_synced FROM ngwaf_sync_state")
        for r in cur.fetchall():
            workspaces[r["workspace_id"]] = r["last_timestamp_synced"]

        cur = con.execute(
            "SELECT bot_name, count(*) AS cnt FROM ngwaf_bots WHERE bot_name IS NOT NULL GROUP BY bot_name ORDER BY count(*) DESC LIMIT 10"
        )
        top_bots = {r["bot_name"]: r["cnt"] for r in cur.fetchall()}

        return {
            "total_cached_bots": total_bots,
            "workspaces": workspaces,
            "top_bots": top_bots,
        }
    finally:
        con.close()


# ── Webhook notifications ────────────────────────────────────────────────────


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


_post_alert_webhook = _post_json
_post_slack_notification = _post_json


def _post_pagerduty_notification(url: str, alert_name: str, metric: str, msg_val: str, service_name: str) -> None:
    _post_json(
        url,
        {
            "event_action": "trigger",
            "client": "Fastly Log Analytics Engine",
            "payload": {
                "summary": f"Alert {alert_name} triggered on {service_name}: {metric} {msg_val}",
                "source": service_name,
                "severity": "critical",
                "class": "cdn metric",
                "component": metric,
            },
        },
    )


@cron_task("sync_ngwaf_bots", job_name="ngwaf_sync")
def _run_ngwaf_bot_sync(service_id: str) -> None:
    from backend.cron.jobs.sync import get_service_display_label
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.utils.bot_sources import build_matcher

    if dev_mode_no_crons():
        logger.info("⏸️  \x1b[36m[ngwaf_sync]\x1b[0m %s: dev_mode_no_crons active, skipping NGWAF sync.", service_id)
        return
    cfg = svcconfig.load_config(service_id)
    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id) if cfg else None
    src = get_source_for_service(service_id) if workspace_id else None
    if not cfg or not workspace_id or src is None:
        return  # Not configured — skip silently
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
            matcher = build_matcher()
            from_ts = get_last_timestamp(workspace_id)
            if not from_ts:
                now_ts = iso_z_now()
                update_sync_watermark(workspace_id, now_ts)
                summary = f"First sync — seeded watermark at {now_ts}. Next cycle will fetch new bot records from this point forward."
                run.success(summary)
                _log_and_add_progress(
                    run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary}
                )
                logger.info("🏁  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)
                return
            budget_exceeded = False
            for page_records, page_latest_ts, _raw_count in fetch_verified_bots_paged(api_key, workspace_id, from_ts):
                if server_name_filter:
                    page_records = [
                        r for r in page_records if not r.get("server_name") or r["server_name"] == server_name_filter
                    ]
                enriched: list[dict] = []
                for r in page_records:
                    wk_matches = matcher(r["user_agent"]) if r.get("user_agent") else ()
                    wk = wk_matches[0] if wk_matches else {}
                    enriched.append({**r, "wellknown_bot_id": wk.get("id"), "wellknown_bot_name": wk.get("name")})
                if enriched or page_latest_ts:
                    upsert_bots(enriched, workspace_id, page_latest_ts)
                total_records += len(enriched)
                if run.elapsed() >= 240:
                    budget_exceeded = True
                    break
            deleted = cleanup_old_bots(retention_days)
            budget_note = " (budget reached — will continue next run)" if budget_exceeded else ""
            summary = f"Synced {total_records} bot record(s){budget_note}, cleaned {deleted} old row(s)."
            run.success(summary, files_downloaded=total_records)
            _log_and_add_progress(run_id, service_id, job_name="ngwaf_sync", event={"type": "done", "message": summary})
            try:
                from backend.cron.scheduler import get_scheduler

                job = get_scheduler().get_job(f"ngwaf_sync_{service_id}")
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
            is_auth = any(c in err_msg for c in ("401", "403", "Unauthorized", "Forbidden", "invalid_api_key"))
            summary = (
                "NGWAF sync failed: authentication error (check Fastly API key / workspace permissions)"
                if is_auth
                else f"NGWAF sync failed: {err_msg}"
            )
            run.fail(summary, error_message=err_msg, files_downloaded=total_records)
            _log_and_add_progress(
                run_id, service_id, job_name="ngwaf_sync", event={"type": "error", "message": summary}
            )
            logger.exception("[ngwaf_sync] %s: sync failed: %s", svc_display, err_msg)
    logger.info("🏁  \x1b[36m[ngwaf_sync]\x1b[0m %s: NGWAF sync job finished.", svc_display)


@global_job("bot_data_refresh", color="36", tag="bots", label="Bot data refresh")
def _run_bot_data_refresh() -> tuple[str, str]:
    from backend.cron.scheduler import dev_mode_no_crons
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
    if failed:
        failed_ids = ", ".join(r.get("id", "unknown") for r in failed)
        if not succeeded:
            return ("error", f"{detail} (All {len(failed)} bot sources failed: {failed_ids})")
        return ("warning", f"{detail} ({len(failed)} source(s) failed: {failed_ids})")
    return ("success", detail)


@global_job("rdns_enrichment", color="34", tag="rdns", label="rDNS enrichment")
def _run_rdns_enrichment() -> tuple[str, str]:
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.utils.bot_sources import enrich_batch

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
    from backend.core import share_db
    from backend.cron.scheduler import dev_mode_no_crons

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
    exp_inv = stale_res.get("deleted_expired_invites", 0)
    stale_sess = stale_res.get("deleted_stale_sessions", 0)
    claim_tok = stale_res.get("deleted_claim_tokens", 0)
    logger.info(
        "✅ \x1b[35m[share_audit_purge]\x1b[0m Deleted %d audit row(s), %d expired invite(s), %d stale session(s), %d claim token(s).",
        deleted,
        exp_inv,
        stale_sess,
        claim_tok,
    )
    return (
        "success",
        f"deleted={deleted} retention_days={retention} expired_invites={exp_inv} stale_sessions={stale_sess} claim_tokens={claim_tok}",
    )


@cron_task("evaluate_alerts", job_name="alerts")
def _run_service_alerts_evaluation(service_id: str) -> None:
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.repositories import assets as alert_repo
    from backend.utils.telemetry import should_defer_cron

    _evaluate_alert = importlib.import_module("backend.routers.alerts")._evaluate_alert

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
    alert_tag = f"{JOB_COLORS['alerts']}[alerts]{RESET_COLOR}"
    _display = _display_label(src, service_id)
    logger.info("🏎️  \x1b[93m[alerts]\x1b[0m %s: Alerts evaluation job started.", _display)
    enabled_alerts = [a for a in alert_repo.get_alerts(service_id=service_id) if a["enabled"]]
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
        webhook_failures: list[str] = []
        display_name = _display_label(src, service_id)
        triggered_items: list[tuple[dict, str | None, dict | None, str | None]] = []
        repo_eval = getattr(alert_repo, "evaluate_alert", None)
        for alert in enabled_alerts:
            try:
                if repo_eval is not None:
                    fired, webhook_url, payload, max_ts = repo_eval(
                        None, src, alert, display_name=display_name, service_id=service_id
                    )
                else:
                    fired, webhook_url, payload, max_ts = _evaluate_alert(
                        service_id, src, alert, display_name=display_name
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
                logger.error("%s Failed to evaluate alert %s for %s: %s", alert_tag, alert["id"], display_name, e)
        try:
            if triggered_items:
                for alert, _, _, max_ts in triggered_items:
                    alert_repo.update_last_triggered(service_id, alert["id"], max_ts)
                from backend.core.metadata.base import export_admin_state

                export_admin_state(service_id)
                for alert, webhook_url, payload, _ in triggered_items:
                    if webhook_url and payload:
                        try:
                            _post_alert_webhook(webhook_url, payload)
                        except Exception as e:
                            webhook_failures.append(f"webhook for {alert['name']}: {e}")
                            logger.error("%s Failed to send webhook for alert %s: %s", alert_tag, alert["id"], e)
                    for channel in alert.get("channels") or []:
                        try:
                            chan_type, chan_url = channel.get("type"), channel.get("url")
                            if not chan_url:
                                continue
                            if chan_type == "slack":
                                fields = [
                                    {"type": "mrkdwn", "text": f"*{k}:* {v}"}
                                    for k, v in (
                                        ("Service", display_name),
                                        ("Metric", alert["metric"]),
                                        ("Operator", alert["operator"]),
                                        ("Threshold", alert["threshold"]),
                                    )
                                ]
                                _post_slack_notification(
                                    chan_url,
                                    {
                                        "blocks": [
                                            {
                                                "type": "section",
                                                "text": {
                                                    "type": "mrkdwn",
                                                    "text": f"🚨 *Alert Triggered: {alert['name']}*",
                                                },
                                            },
                                            {"type": "fields", "fields": fields},
                                        ]
                                    },
                                )
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
                            webhook_failures.append(f"channel {channel.get('type')} for {alert['name']}: {e}")
                            logger.error(
                                "%s Failed to send notifications to channel %s for alert %s: %s",
                                alert_tag,
                                channel.get("type"),
                                alert["id"],
                                e,
                            )
            n_eval, n_trig = len(enabled_alerts), len(triggered_items)
            status = "warning" if (n_trig > 0 or webhook_failures) else "success"
            summary = (
                f"Evaluated {n_eval} {'alert' if n_eval == 1 else 'alerts'}. "
                f"{n_trig} {'alert' if n_trig == 1 else 'alerts'} triggered."
            )
            if webhook_failures:
                summary += f" ({len(webhook_failures)} notification failure{'s' if len(webhook_failures) > 1 else ''}: {', '.join(webhook_failures)})"
            run.record(status, summary=summary, files_downloaded=n_eval, rows_ingested=n_trig)
            _log_and_add_progress(run_id, service_id, job_name=task_name, event={"type": "done", "message": summary})
        except Exception as e:
            import traceback

            err_msg = traceback.format_exc()
            logger.error("%s Failed during alerts evaluation job for %s: %s\n%s", alert_tag, service_id, e, err_msg)
            run.fail(f"Alerts evaluation failed: {e}", error_message=err_msg, files_downloaded=0, rows_ingested=0)


_DELEGATED_EXPORTS: dict[str, str] = {
    "DEFAULT_PREWARM_IDLE_MINUTES": "backend.cron.jobs.insights_prewarmer",
    "_INGEST_TASKS": "backend.cron.jobs.insights_prewarmer",
    "_MAX_ANALYST_SHAPES": "backend.cron.jobs.insights_prewarmer",
    "_active_analyst_shapes": "backend.cron.jobs.insights_prewarmer",
    "_analyst_prewarm_enabled": "backend.cron.jobs.insights_prewarmer",
    "_cache_cold": "backend.cron.jobs.insights_prewarmer",
    "_last_full_warm": "backend.cron.jobs.insights_prewarmer",
    "_last_insights_request": "backend.cron.jobs.insights_prewarmer",
    "_prewarm_idle_minutes": "backend.cron.jobs.insights_prewarmer",
    "_run_insights_prewarmer": "backend.cron.jobs.insights_prewarmer",
    "_run_metadata_cleanup": "backend.cron.jobs.maintenance",
    "_running_ingest": "backend.cron.jobs.insights_prewarmer",
    "is_insights_active": "backend.cron.jobs.insights_prewarmer",
    "record_insights_request": "backend.cron.jobs.insights_prewarmer",
}


class _MetadataModule(ModuleType):
    def __getattr__(self, name: str) -> Any:
        target_mod = _DELEGATED_EXPORTS.get(name)
        if target_mod is not None:
            mod = importlib.import_module(target_mod)
            return getattr(mod, name)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        target_mod = _DELEGATED_EXPORTS.get(name)
        if target_mod is not None:
            mod = importlib.import_module(target_mod)
            setattr(mod, name, value)
            return
        super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        target_mod = _DELEGATED_EXPORTS.get(name)
        if target_mod is not None:
            mod = importlib.import_module(target_mod)
            delattr(mod, name)
            return
        super().__delattr__(name)


_self_mod = _sys.modules[__name__]
_self_mod.__class__ = _MetadataModule
