from __future__ import annotations

import gc
import logging
import os
import shutil
import signal
import sys as _sys
import threading
import time
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import tenacity
from opentelemetry.metrics import CallbackOptions, Observation

from backend import config as svcconfig
from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool
from backend.core.request_telemetry import get_meter
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
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.date_utils import iso_z, iso_z_now, parse_relative_time_window

_os = os
_shutil = shutil
logger = logging.getLogger("backend.scheduler")
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
    from backend.core.duckdb import get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.cron.jobs.sync import get_service_display_label
    from backend.utils.bot_sources import build_matcher
    from backend.utils.ngwaf import (
        cleanup_old_bots,
        fetch_verified_bots_paged,
        get_last_timestamp,
        update_sync_watermark,
        upsert_bots,
    )

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
    if failed:
        failed_ids = ", ".join(r.get("id", "unknown") for r in failed)
        if not succeeded:
            return ("error", f"{detail} (All {len(failed)} bot sources failed: {failed_ids})")
        return ("warning", f"{detail} ({len(failed)} source(s) failed: {failed_ids})")
    return ("success", detail)


@global_job("rdns_enrichment", color="34", tag="rdns", label="rDNS enrichment")
def _run_rdns_enrichment() -> tuple[str, str]:
    from backend.cron.decorators import dev_mode_no_crons
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
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.repositories import alerts as alert_repo  # type: ignore[attr-defined]
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
        con_ro = get_connection(src, read_only=True)
        webhook_failures: list[str] = []
        try:
            display_name = _display_label(src, service_id)
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
                    logger.error("%s Failed to evaluate alert %s for %s: %s", alert_tag, alert["id"], display_name, e)
        finally:
            if con_ro is not None:
                con_ro.close()
        try:
            if triggered_items:
                for alert, _, _, max_ts in triggered_items:
                    alert_repo.update_last_triggered(service_id, alert["id"], max_ts)
                from backend.state_sync import export_admin_state

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


@cron_task("metadata_cleanup", job_name="metadata_cleanup")
def _run_metadata_cleanup(service_id: str) -> None:
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata import cleanup_metadata
    from backend.cron.decorators import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

    label = f"{JOB_COLORS.get('metadata_cleanup', '')}[metadata_cleanup]{RESET_COLOR}"
    if dev_mode_no_crons():
        logger.info("⏸️  %s %s: dev_mode_no_crons active, skipping metadata cleanup.", label, service_id)
        return
    src = get_source_for_service(service_id)
    if src is None or should_defer_cron("metadata_cleanup", service_id):
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
        result = cleanup_metadata(
            service_id,
            retention,
            on_event=lambda ev: _log_and_add_progress(run_id, service_id, job_name="metadata_cleanup", event=ev),
        )
        total_deleted = sum(result["deleted"].values())
        summary_parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
        if total_deleted:
            vac_str = "yes" if result["vacuumed"] else "skipped (no deletions)"
            summary = f"Trimmed {total_deleted:,} rows ({', '.join(summary_parts)}). VACUUM={vac_str}."
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
            summary = "No rows older than retention windows."
            logger.info("🏁  %s %s: no rows to trim (took %.2fs)", label, _display, result["duration_s"])
        try:
            purge_old(retention_days=30)
        except Exception as e:
            logger.debug("[metadata_cleanup] metric_snapshots purge failed: %s", e)
        run.success(summary, rows_ingested=total_deleted)
        _log_and_add_progress(
            run_id, service_id, job_name="metadata_cleanup", event={"type": "done", "message": summary}
        )


_EMPTY = ""


def _pg_connection():
    from backend.core.metadata.base import get_pg_readonly_connection

    return get_pg_readonly_connection()


def _pg_write_connection():
    from backend.core.metadata.base import get_pg_thread_connection

    return get_pg_thread_connection()


def record_snapshot(
    metric: str, value: float, *, service_id: str | None = None, task: str | None = None, ts: str | None = None
) -> None:
    if not metric:
        raise ValueError("metric is required")
    try:
        _pg_write_connection().execute(
            "INSERT INTO metric_snapshots (ts, metric, service_id, task, value) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (metric, service_id, task, ts) DO UPDATE SET value = EXCLUDED.value",
            (ts or iso_z_now(), metric, service_id or _EMPTY, task or _EMPTY, float(value)),
        )
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres insert failed for %s: %s", metric, e)


def get_batch(*, since: datetime | str) -> dict:
    if isinstance(since, str):
        since = parse_relative_time_window(since)
    try:
        with closing(_pg_connection()) as con:
            rows = con.execute(
                "SELECT metric, service_id, task, ts, value FROM metric_snapshots WHERE ts >= %s ORDER BY ts ASC",
                (iso_z(since),),
            ).fetchall()
        return _group_batch_rows(rows)
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres batch read failed: %s", e)
        return {}


def last_snapshot_age_s() -> float | None:
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
    return max(0.0, (datetime.now(UTC) - dt).total_seconds()) if dt is not None else None


def purge_old(retention_days: int = 30) -> int:
    if retention_days <= 0:
        return 0
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    try:
        cur = _pg_write_connection().execute("DELETE FROM metric_snapshots WHERE ts < %s", (cutoff,))
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("[metric_snapshots] Postgres purge failed: %s", e)
        return 0


def close_all_connections() -> None:
    pass


_MAX_BATCH_POINTS_PER_SERIES = 240


def _downsample_rows(series_rows: list, max_points: int = _MAX_BATCH_POINTS_PER_SERIES) -> list[dict]:
    n = len(series_rows)
    if n <= max_points or max_points < 4:
        return [{"ts": r["ts"], "value": r["value"]} for r in series_rows]
    step = (n - 1) / float(max_points - 3)
    indices = {
        0,
        n - 1,
        min(range(n), key=lambda idx: series_rows[idx]["value"]),
        max(range(n), key=lambda idx: series_rows[idx]["value"]),
        *(int(round(i * step)) for i in range(max_points - 2)),
    }
    return [{"ts": series_rows[idx]["ts"], "value": series_rows[idx]["value"]} for idx in sorted(indices)]


def _group_batch_rows(rows) -> dict[str, list[dict]]:
    raw_grouped: dict[str, list] = {}
    for r in rows:
        key = r["metric"]
        if r["service_id"]:
            key = f"{key}|{r['service_id']}" + (f"|{r['task']}" if r["task"] else "")
        raw_grouped.setdefault(key, []).append(r)
    return {key: _downsample_rows(series_rows) for key, series_rows in raw_grouped.items()}


def teardown() -> None:
    try:
        _pg_write_connection().execute("TRUNCATE TABLE metric_snapshots")
    except Exception:
        logger.debug("teardown: con.execute failed (ignored)", exc_info=True)


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
    if (instrument := _instruments.get(metric)) is not None:
        return instrument
    description, unit = _METRICS[metric]

    def callback(_options: CallbackOptions, metric_name: str = metric) -> list[Observation]:
        with _lock:
            samples = dict(_values.get(metric_name, {}))
        return [Observation(value, dict(attributes)) for attributes, value in samples.items()]

    instrument = get_meter().create_observable_gauge(
        name=f"fla_{metric}", callbacks=[callback], description=description, unit=unit
    )
    _instruments[metric] = instrument
    return instrument


def record(metric: str, value: float, **attributes: str) -> None:
    if metric not in _METRICS:
        raise ValueError(f"unsupported operational metric: {metric}")
    key = tuple(sorted((name, str(val)) for name, val in attributes.items()))
    with _lock:
        _values.setdefault(metric, {})[key] = float(value)
    _instrument(metric)


def reset_for_tests() -> None:
    with _lock:
        _values.clear()
        _instruments.clear()


def _env_mb_bytes(name: str) -> int:
    try:
        return max(0, int(os.getenv(name, "0"))) * 1024 * 1024
    except (TypeError, ValueError):
        return 0


def _env_ms_to_s(name: str, default_ms: str, min_s: float, fallback_s: float) -> float:
    try:
        return max(min_s, float(os.getenv(name, default_ms)) / 1000.0)
    except (TypeError, ValueError):
        return fallback_s


def recycle_interval_min() -> float:
    try:
        return max(0.0, float(os.getenv("DUCKDB_RECYCLE_INTERVAL_MIN", "0")))
    except (TypeError, ValueError):
        return 0.0


def _recycle_rss_threshold_bytes() -> int:
    return _env_mb_bytes("DUCKDB_RECYCLE_RSS_THRESHOLD_MB")


def _recycle_drain_timeout_s() -> float:
    return _env_ms_to_s("DUCKDB_RECYCLE_DRAIN_TIMEOUT_MS", "5000", 0.1, 5.0)


def _recycle_grace_s() -> float:
    return _env_ms_to_s("DUCKDB_RECYCLE_GRACE_MS", "3000", 0.0, 3.0)


def _recycle_lock_timeout_s() -> float:
    return _env_ms_to_s("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "2000", 0.0, 2.0)


def _sources_by_db_path() -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for cfg in svcconfig.list_configs():
        try:
            src = _db._source_from_config(cfg)
        except Exception:
            continue
        groups.setdefault(_db.db_path_for_source(src), []).append(src)
        if svcconfig.is_rum_enabled(cfg):
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
    return src.get("name", "default")


def _trim_memory() -> None:
    gc.collect()
    try:
        import ctypes

        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        logger.debug("_trim_memory: ctypes.CDLL failed (ignored)", exc_info=True)


def _recycle_db_path(db_path: str, sources: list[dict]) -> dict:
    from backend.core.storage._core import _get_service_lock

    drain_timeout, grace, lock_timeout = _recycle_drain_timeout_s(), _recycle_grace_s(), _recycle_lock_timeout_s()
    service_keys = [_service_key(s) for s in sources]
    lock_keys = sorted({_lock_key(s) for s in sources})
    acquired: list = []
    rss_before = _db.current_rss_bytes()
    try:
        for lk in lock_keys:
            lock = _get_service_lock(lk)
            if not lock.acquire(timeout=lock_timeout):
                logger.info("[recycle] %s: skip — write lock %r busy (a sync/commit is in flight)", db_path, lk)
                return {"db_path": db_path, "status": "skipped_locked", "lock": lk}
            acquired.append(lock)
        _db.set_recycle_barrier(db_path, True)
        retired_pools = {}
        try:
            retired_pools = _pool.begin_drain_pools(service_keys)
            drained = _pool.wait_pools_drained(retired_pools, drain_timeout)
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
    freed_mb = f"{freed / 1e6:.0f}MB" if freed is not None else "n/a"
    logger.info("[recycle] %s: %s (drained=%s live_after=%d freed=%s)", db_path, status, drained, live, freed_mb)
    return {
        "db_path": db_path,
        "status": status,
        "drained": drained,
        "live_after": live,
        "rss_before": rss_before,
        "rss_after": rss_after,
        "freed_bytes": freed,
    }


def recycle_once(reason: str = "interval") -> str:
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
    return _env_mb_bytes("BACKEND_GRACEFUL_RESTART_RSS_MB")


def maybe_graceful_restart() -> bool:
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


def _maybe_adjust_recycle_schedule(*, expedite: bool) -> None:
    try:
        from backend.cron.scheduler import get_scheduler

        job = get_scheduler().get_job("duckdb_recycle")
        if not job:
            return
        baseline_mins = max(1.0, recycle_interval_min() or 60.0)
        target_mins = min(2.0, baseline_mins) if expedite else baseline_mins
        current_interval_s = getattr(
            getattr(getattr(job, "trigger", None), "interval", None), "total_seconds", lambda: 0
        )()
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
    if maybe_graceful_restart():
        return "graceful restart triggered (RSS over restart threshold)"
    threshold = _recycle_rss_threshold_bytes()
    rss = _db.current_rss_bytes()
    if threshold > 0 and rss is not None and rss < threshold:
        if rss >= int(threshold * 0.8):
            _maybe_adjust_recycle_schedule(expedite=True)
        elif rss < int(threshold * 0.5):
            _maybe_adjust_recycle_schedule(expedite=False)
        return f"skipped: RSS {rss / (1024 * 1024):.0f}MB < threshold {threshold / (1024 * 1024):.0f}MB"
    res = recycle_once(reason="interval")
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


_MAX_ANALYST_SHAPES = 8
_INGEST_TASKS = ("log_discovery", "rum_sync")
DEFAULT_PREWARM_IDLE_MINUTES = 30.0
_last_full_warm: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_full_warm", _last_full_warm)
_last_insights_request: dict[str, float] = {}
CacheRegistry.register("insights_prewarmer._last_insights_request", _last_insights_request)


def record_insights_request(service_id: str | None) -> None:
    if service_id:
        _last_insights_request[service_id] = time.monotonic()


def _prewarm_idle_minutes(service_id: str, src: dict | None = None) -> float:
    prov = (src or {}).get("provisioning") if isinstance(src, dict) else None
    if not isinstance(prov, dict):
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id) or {}
        prov = cfg.get("provisioning") if isinstance(cfg.get("provisioning"), dict) else {}
    cron_ip = prov.get("cron_insights_prewarmer") if isinstance(prov, dict) else None
    for candidate in (
        cron_ip.get("idle_minutes") if isinstance(cron_ip, dict) else None,
        os.environ.get("INSIGHTS_PREWARM_IDLE_MINUTES", "").strip() or None,
    ):
        if candidate is not None:
            try:
                return float(candidate)
            except (TypeError, ValueError):
                pass
    return DEFAULT_PREWARM_IDLE_MINUTES


def is_insights_active(service_id: str, src: dict | None = None, *, idle_minutes: float | None = None) -> bool:
    limit_mins = _prewarm_idle_minutes(service_id, src) if idle_minutes is None else float(idle_minutes)
    if limit_mins <= 0:
        return True
    last_req = _last_insights_request.get(service_id)
    if last_req is None and isinstance(src, dict):
        alt_id = src.get("service_id") or src.get("name")
        if isinstance(alt_id, str) and alt_id:
            last_req = _last_insights_request.get(alt_id)
    return last_req is not None and (time.monotonic() - last_req) < (limit_mins * 60.0)


def _running_ingest(service_id: str) -> str | None:
    from backend.cron_progress import active_task_for_service

    return active_task_for_service(service_id, _INGEST_TASKS)


def _cache_cold(service_id: str) -> bool:
    from backend.repositories.insights.repository import INSIGHTS_CACHE_TTL

    last = _last_full_warm.get(service_id)
    return last is None or time.monotonic() - last >= INSIGHTS_CACHE_TTL


def _analyst_prewarm_enabled() -> bool:
    return os.environ.get("INSIGHTS_PREWARM_ANALYST", "1").strip().lower() not in ("0", "false", "no", "off")


def _active_analyst_shapes(service_id: str) -> list[tuple[str | None, str | None, int | None, bool]]:
    from backend.core import share_db
    from backend.utils.date_utils import parse_iso_utc

    now = datetime.now(UTC)
    shapes: set[tuple[str | None, str | None, int | None, bool]] = set()
    for inv in share_db.get_remote_invites():
        if inv.get("revoked") or service_id not in (inv.get("service_ids") or []):
            continue
        if (exp := inv.get("expires_at")) and (pe := parse_iso_utc(exp)) is not None and pe < now:
            continue
        policy = inv.get("pii_policy")
        mask = bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
        shapes.add((inv.get("query_start_time"), inv.get("query_end_time"), inv.get("query_window_hours"), mask))
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
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.repositories.insights import get_insights
    from backend.utils.active_requests import should_defer_cron, yield_to_api
    from backend.utils.remote_access import resolve_analyst_insights_clamp
    from backend.utils.time_window import history_hours_from_earliest, pick_insights_default
    from backend.utils.tunnel import get_tunnel_manager

    src = get_source_for_service(service_id)
    if src is None or should_defer_cron("insights_prewarmer", service_id):
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
    if not must_run and (ingest_task := _running_ingest(service_id)) is not None:
        logger.info("[insights-prewarmer] %s: deferring — %s in flight and cache still warm", service_id, ingest_task)
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
            window_hours, baseline_hours = pick_insights_default(
                history_hours_from_earliest(cached_status.get("earliest_log_at"))
            )
            con = get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)
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
                    if not must_run and (stopped_for := _running_ingest(service_id)) is not None:
                        logger.info(
                            "[insights-prewarmer] %s: stopping early — %s started mid-tick", service_id, stopped_for
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
                            qs, qe, qwh, baseline_hours=baseline_hours, window_hours=window_hours
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


metric_snapshots = _sys.modules[__name__]


def _safe_record(
    metric: str, value: float, *, service_id: str | None = None, task: str | None = None, **attributes: str
) -> bool:
    recorded = False
    try:
        metric_snapshots.record_snapshot(metric, value, service_id=service_id, task=task)
        recorded = True
    except Exception as e:
        logger.debug("[metric_snapshot] record %s failed: %s", metric, e)
    try:
        operational_metrics = _sys.modules[__name__]
        if metric in {"celery_active_tasks", "celery_active_workers", "celery_broker_reachable"}:
            operational_metrics.record(metric, value)
        elif metric.startswith("celery_queue_depth_"):
            operational_metrics.record("celery_queue_depth", value, queue=metric.removeprefix("celery_queue_depth_"))
        elif metric == "celery_queue_depth":
            operational_metrics.record(metric, value, queue="all")
        elif metric.startswith("ingest_ledger_"):
            operational_metrics.record("ingest_ledger_rows", value, status=metric.removeprefix("ingest_ledger_"))
        elif metric.startswith("high_scale_"):
            if service_id is not None:
                attributes["service_id"] = service_id
            if task is not None:
                attributes["task"] = task
            operational_metrics.record(metric, value, **attributes)
    except Exception as e:
        logger.debug("[metric_snapshot] operational metric %s failed: %s", metric, e)
    return recorded


def _sample_pool_wait() -> int:
    count = 0
    try:
        from backend.core import duckdb_pool

        for stats in duckdb_pool.get_all_stats():
            p95 = (stats.get("wait") or {}).get("p95_ms")
            if (
                p95 is not None
                and stats.get("service")
                and _safe_record("pool_wait_p95_ms", float(p95), service_id=stats["service"])
            ):
                count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] pool_wait sample failed: %s", e)
    return count


def _sample_cron_duration() -> int:
    count = 0
    try:
        from backend import config as svcconfig
        from backend.core.metadata.base import get_con

        sql_by_svc = (
            "SELECT task, duration_s FROM cron_runs WHERE service_id = ? AND status IN ('success', 'warning', 'error') "
            "AND duration_s IS NOT NULL AND id IN (SELECT max(id) FROM cron_runs WHERE service_id = ? "
            "AND status IN ('success', 'warning', 'error') GROUP BY service_id, task)"
        )
        sql_legacy = (
            "SELECT task, duration_s FROM cron_runs WHERE status IN ('success', 'warning', 'error') "
            "AND duration_s IS NOT NULL AND id IN (SELECT max(id) FROM cron_runs "
            "WHERE status IN ('success', 'warning', 'error') GROUP BY task)"
        )
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                con = get_con(service_id)
                try:
                    rows = con.execute(sql_by_svc, (service_id, service_id)).fetchall()
                except Exception as probe_exc:
                    if "service_id" not in str(probe_exc):
                        raise
                    rows = con.execute(sql_legacy).fetchall()
                for r in rows:
                    if r["task"] and r["duration_s"] is not None:
                        if _safe_record(
                            "cron_duration_ms", float(r["duration_s"]) * 1000.0, service_id=service_id, task=r["task"]
                        ):
                            count += 1
            except Exception as e:
                logger.debug("[metric_snapshot] cron_duration for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] cron_duration sample failed: %s", e)
    return count


def _sample_ingest_lag() -> int:
    count = 0
    try:
        from backend import config as svcconfig
        from backend.core import metadata as metadata_db
        from backend.utils.date_utils import parse_iso_utc

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                latest = metadata_db.get_latest_ingest_ts(service_id)
                latest_dt = parse_iso_utc(latest) if latest else None
                if latest_dt is not None:
                    lag_s = max(0.0, (datetime.now(UTC) - latest_dt).total_seconds())
                    if _safe_record("ingest_lag_s", lag_s, service_id=service_id):
                        count += 1
            except Exception as e:
                logger.debug("[metric_snapshot] ingest_lag for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] ingest_lag sample failed: %s", e)
    return count


def _sample_active_queries() -> int:
    try:
        from backend.core.request_telemetry import query_registry

        summary = query_registry.summary()
        return 1 if _safe_record("active_query_count", float(summary.get("active_total", 0))) else 0
    except Exception as e:
        logger.debug("[metric_snapshot] active_query sample failed: %s", e)
        return 0


def _sample_ducklake_admission() -> int:
    count = 0
    try:
        from backend.core.storage._ducklake import get_admission_stats

        for service_id, stats in get_admission_stats().items():
            acquisitions = stats.get("acquisitions", 0.0)
            if acquisitions <= 0:
                continue
            for metric, val in (
                ("ducklake_admission_wait_avg_ms", stats.get("wait_ms_total", 0.0) / acquisitions),
                ("ducklake_admission_hold_avg_ms", stats.get("hold_ms_total", 0.0) / acquisitions),
                ("ducklake_admission_timeouts", stats.get("timeouts", 0.0)),
            ):
                if _safe_record(metric, val, service_id=service_id):
                    count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] ducklake_admission sample failed: %s", e)
    return count


def _sample_os_vitals() -> int:
    count = 0
    try:
        load1, _, _ = _os.getloadavg()
        if _safe_record("cpu_load_1m", float(load1)):
            count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] cpu_load sample failed: %s", e)
    try:
        with open("/proc/meminfo") as f:
            meminfo: dict[str, int] = {}
            for line in f:
                k, _, rest = line.partition(":")
                v = rest.strip().split()
                if v and v[0].isdigit():
                    meminfo[k.strip()] = int(v[0]) * 1024
        total, avail = meminfo.get("MemTotal", 0), meminfo.get("MemAvailable", 0)
        if total and _safe_record("mem_used_pct", round((1 - avail / total) * 100, 2)):
            count += 1
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug("[metric_snapshot] mem sample failed: %s", e)
    for path, metric in (("/app/data", "disk_used_pct"), ("/", "disk_used_pct_root")):
        try:
            d = _shutil.disk_usage(path)
            if d.total and _safe_record(metric, round(d.used / d.total * 100, 2)):
                count += 1
        except FileNotFoundError:
            continue
        except Exception as e:
            logger.debug("[metric_snapshot] disk sample for %s failed: %s", path, e)
    return count


def _sample_celery_queues() -> int:
    count = 0
    try:
        from backend.celery_status import KNOWN_QUEUES, celery_queue_depths

        queues, broker_reachable = celery_queue_depths()
        if _safe_record("celery_broker_reachable", 1.0 if broker_reachable else 0.0):
            count += 1
        total_depth = 0.0
        for queue in sorted(set(KNOWN_QUEUES) | set(queues)):
            depth = float(queues.get(queue, 0))
            total_depth += depth
            if _safe_record(f"celery_queue_depth_{queue}", depth):
                count += 1
        if _safe_record("celery_queue_depth", total_depth):
            count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] celery_queues sample failed: %s", e)
    return count


def _sample_ingest_ledger() -> int:
    count = 0
    try:
        from backend.celery_status import ingest_ledger_summary

        for status, count_val in ingest_ledger_summary().items():
            if _safe_record(f"ingest_ledger_{status}", float(count_val)):
                count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] ingest_ledger sample failed: %s", e)
    return count


def _sample_high_scale() -> int:
    from backend import config as svcconfig

    if not svcconfig.is_high_scale_mode() or (
        _os.getenv("HIGH_SCALE_ENABLED", "").lower() not in {"1", "true", "yes", "on"}
        and svcconfig.DEPLOYMENT_MODE != "high_scale"
    ):
        return 0
    count = 0
    try:
        from backend.high_scale.postgres_control import PostgresControlPlane

        control = PostgresControlPlane()
        configured_domains = {d.strip() for d in _os.getenv("HIGH_SCALE_DOMAINS", "").split(",") if d.strip()}
        source_statuses = (
            "discovered",
            "claimed",
            "appended",
            "archived",
            "acknowledged",
            "source_deleted",
            "source_missing",
        )
        pub_metrics = (
            ("high_scale_pending_manifests", "pending"),
            ("high_scale_publication_lag_seconds", "lag_seconds"),
            ("high_scale_rows_published_total", "published_rows"),
        )
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            snapshot = control.operational_snapshot(service_id)
            domains = configured_domains | {
                row["domain"] for row in snapshot["source_objects"] + snapshot["source_age"] + snapshot["publications"]
            }
            present_status = {(row["domain"], row["status"]) for row in snapshot["source_objects"]}
            present_age = {row["domain"] for row in snapshot["source_age"]}
            present_pub = {row["domain"] for row in snapshot["publications"]}
            try:
                operational_metrics = _sys.modules[__name__]
                for domain in domains:
                    for status in source_statuses:
                        if (domain, status) not in present_status:
                            operational_metrics.record(
                                "high_scale_source_objects", 0.0, service_id=service_id, domain=domain, status=status
                            )
                    if domain not in present_age:
                        operational_metrics.record(
                            "high_scale_oldest_active_age_seconds", 0.0, service_id=service_id, domain=domain
                        )
                    if domain not in present_pub:
                        for m_name, _ in pub_metrics:
                            operational_metrics.record(m_name, 0.0, service_id=service_id, domain=domain)
            except Exception as e:
                logger.debug("[metric_snapshot] high_scale zero-seed failed: %s", e)
            for row in snapshot["source_objects"]:
                if _safe_record(
                    "high_scale_source_objects",
                    float(row["count"]),
                    service_id=service_id,
                    domain=row["domain"],
                    status=row["status"],
                ):
                    count += 1
            for row in snapshot["source_age"]:
                if _safe_record(
                    "high_scale_oldest_active_age_seconds",
                    row["age_seconds"],
                    service_id=service_id,
                    domain=row["domain"],
                ):
                    count += 1
            for row in snapshot["publications"]:
                for m_name, col in pub_metrics:
                    val = row[col] if col == "lag_seconds" else float(row[col])
                    if _safe_record(m_name, val, service_id=service_id, domain=row["domain"]):
                        count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] high_scale sample failed: %s", e)
    return count


def _sample_high_scale_disk() -> int:
    from backend import config as svcconfig

    if not svcconfig.is_high_scale_mode():
        return 0
    count = 0
    try:
        from backend.core.clickhouse import get_clickhouse_client
        from backend.high_scale.archive_models import MODE_LEVELS, sample_disk_state

        client = get_clickhouse_client()
        if client is None:
            return 0
        snapshot = sample_disk_state(client)
        if _safe_record("high_scale_disk_mode_level", float(MODE_LEVELS[snapshot.state.mode])):
            count += 1
        if _safe_record("high_scale_disk_free_fraction", snapshot.state.free_fraction):
            count += 1
    except Exception as e:
        import structlog

        structlog.get_logger(__name__).warning("high_scale.disk_sample_failed", error_kind=type(e).__name__)
    return count


def _sample_duckdb_size() -> int:
    count = 0
    try:
        from backend import config as svcconfig

        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            try:
                db_path = svcconfig.duckdb_path(service_id)
                if _os.path.exists(db_path) and _safe_record(
                    "duckdb_file_bytes", float(_os.path.getsize(db_path)), service_id=service_id
                ):
                    count += 1
            except Exception as e:
                logger.debug("[metric_snapshot] duckdb_size for %s failed: %s", service_id, e)
    except Exception as e:
        logger.debug("[metric_snapshot] duckdb_size sample failed: %s", e)
    return count


def _sample_celery_workers() -> int:
    count = 0
    try:
        from backend.celery_app import app

        i = app.control.inspect(timeout=0.5)
        stats = i.stats() or {}
        if _safe_record("celery_active_workers", float(len(stats))):
            count += 1
        active = (i.active() or {}) if stats else {}
        if _safe_record("celery_active_tasks", float(sum(len(tasks) for tasks in active.values()))):
            count += 1
    except Exception as e:
        logger.debug("[metric_snapshot] celery_workers sample failed: %s", e)
    return count


@global_job("metric_snapshot", color="32", tag="metric_snapshot", label="Metric snapshot")
def _run_metric_snapshot() -> str:
    total = sum(
        fn()
        for fn in (
            _sample_pool_wait,
            _sample_cron_duration,
            _sample_ingest_lag,
            _sample_duckdb_size,
            _sample_active_queries,
            _sample_ducklake_admission,
            _sample_high_scale,
            _sample_high_scale_disk,
            _sample_os_vitals,
            _sample_celery_queues,
            _sample_ingest_ledger,
            _sample_celery_workers,
        )
    )
    return f"sampled={total} metrics"


"""Local + rollup compaction crons.

* ``_run_local_compact`` — frequent merge of small parquet files in the
  LOCAL CACHE only (does NOT touch FOS). Free in terms of cloud cost, so
  we run it on a 2 min interval.
* ``_run_rollup_hour_heal`` — hourly self-heal that rebuilds hour bundles
  for closed hours the per-sync recompute missed. The per-sync path only
  fires when a sync batch ingests rows STAMPED in an already-closed hour
  (delivery-lag straddling the boundary); on bursty/low-traffic services
  the burst ends mid-hour, nothing straddles, and the closed hour never
  gets a rollup — the top-N reader then silently under-counts every
  window touching that hour until the nightly pass. Hourly cadence caps
  that staleness at ~1 hour.
* ``_run_rollup_compact_daily`` — consolidates per-hour rollup parquet
  into per-day files for closed days, slashing file-open overhead on
  7-day dashboard queries. Also runs the same self-heal with a 30-day
  lookback as a deep pass.
"""


def _ledger_touched_hours(service_id: str, lookback_s: float) -> set[str]:
    import re

    from backend.core.metadata.base import get_con

    rows = (
        get_con(service_id)
        .execute(
            "SELECT DISTINCT object_key FROM ingest_ledger WHERE service_id = ? AND committed_at >= ?",
            (service_id, time.time() - lookback_s),
        )
        .fetchall()
    )
    return {
        f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}"
        for (key,) in rows
        if (m := re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})", key))
    }


@cron_task("local_compact", job_name="local_compact")
def _run_local_compact(service_id: str) -> None:
    from backend.core import local_compaction as _lc  # type: ignore[attr-defined]
    from backend.core.duckdb import get_source_for_service
    from backend.utils.active_requests import should_defer_cron

    if should_defer_cron("local_compact", service_id) or (src := get_source_for_service(service_id)) is None:
        return
    with cron_run(
        service_id,
        "local_compact",
        source=src,
        skip_log_prefix="local-compact",
        error_summary="local compaction failed",
    ) as run:
        if not run.ok:
            return
        _display = _display_label(src, service_id)
        logger.info("🏎️  \x1b[96m[local-compact]\x1b[0m %s: Local compaction started.", _display)
        run.log_progress({"type": "status", "message": "Scanning local cache partitions..."})
        result = _lc.compact_local_partitions(src)
        errors = result.get("errors") or []
        merged, removed, partitions = (
            result.get("files_merged", 0),
            result.get("files_removed", 0),
            result.get("partitions_compacted", 0),
        )
        summary = f"Compacted {partitions} partition(s): merged {merged} small file(s) into {partitions} (removed {removed} originals)"
        if svcconfig.is_high_scale_mode(src):
            try:
                if rollup_hours := _ledger_touched_hours(service_id, lookback_s=15 * 60):
                    from backend.core.rollups.recompute import recompute_touched_hours

                    recompute_touched_hours(service_id, src, rollup_hours)
                    summary += f"; rollups recomputed for {len(rollup_hours)} ledger hour(s)"
            except Exception as e:
                errors = list(errors) + [f"high-scale rollup recompute failed: {e}"]
                logger.warning("[local-compact] %s: celery rollup recompute failed: %s", service_id, e)
        if errors:
            err_preview = "\n".join(errors[:3]) + (f"\n... ({len(errors) - 3} more)" if len(errors) > 3 else "")
            summary += f" — {len(errors)} partition error(s)"
            run.warn(summary, error_message=err_preview, files_downloaded=merged)
        else:
            run.success(summary, files_downloaded=merged)
        run.log_progress({"type": "status", "message": summary})
        logger.info("🏁  \x1b[96m[local-compact]\x1b[0m %s: %s in %.2fs", _display, summary, run.elapsed())


@cron_task("rollup_hour_heal", job_name="rollup_hour_heal")
def _run_rollup_hour_heal(service_id: str, manual: bool = False, run_id: int | None = None) -> dict:
    from backend.core.duckdb import get_source_for_service
    from backend.core.rollups import backfill_missing_hour_bundles
    from backend.core.rollups._common import _hour_bundled_root
    from backend.cron.jobs.sync import has_sufficient_disk_space
    from backend.utils.active_requests import should_defer_cron

    if not manual and should_defer_cron("rollup_hour_heal", service_id):
        logger.info("⏸️  [rollup-heal] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}
    ok_disk, msg = has_sufficient_disk_space(_hour_bundled_root(src), "rollup-heal", service_id)
    if not ok_disk:
        record_skip_row(
            src, "rollup_hour_heal", "Skipped due to low disk space", status="warning", error_message=msg, run_id=run_id
        )
        return {"status": "warning", "service_id": service_id, "summary": msg}
    _display = _display_label(src, service_id)
    outcome: dict = {}
    with cron_run(
        service_id,
        "rollup_hour_heal",
        source=src,
        run_id=run_id,
        error_summary="hour-bundle self-heal failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error_message": str(exc),
            "summary": "hour-bundle self-heal failed",
            "duration_s": run.elapsed(),
            "run_id": run.run_id,
        },
        skip_log_prefix="rollup-heal",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.aborted:
            return dict(run.result or {})
        durable_mode = svcconfig.is_durable_serving_mode(src)
        if durable_mode:
            from backend.core.rollups._common import rollup_coverage_ready

            coverage_ready = rollup_coverage_ready(service_id)
        else:
            coverage_ready = True
        lookback_days = 2
        heal = backfill_missing_hour_bundles(
            service_id,
            src,
            lookback_days=lookback_days,
            max_missing_hours=1 if durable_mode and not coverage_ready else None,
        )
        if not durable_mode:
            try:
                from backend.core.rollups import backfill_security_proxies_bundles, backfill_wellknown_bots_rollup

                backfill_wellknown_bots_rollup(service_id, src, lookback_days=lookback_days)
                backfill_security_proxies_bundles(service_id, src)
            except Exception as sec_heal_err:
                logger.warning(
                    "[rollup-heal] %s: security rollup self-heal skipped/failed: %s", service_id, sec_heal_err
                )
        try:
            cfg = svcconfig.load_config(service_id) or {}
            if cfg.get("rum", {}).get("enabled") and not durable_mode:
                from contextlib import closing

                from backend.core.duckdb import get_connection, rum_source_for
                from backend.core.rollups.rum import backfill_missing_rum_aggregates
                from backend.core.storage import execute_with_stale_view_retry
                from backend.core.storage._ducklake import _ducklake_attach

                rum_src = rum_source_for(src)
                with closing(get_connection(rum_src, read_only=False)) as rum_con:
                    try:
                        _ducklake_attach(rum_con, src, read_only=True)
                    except Exception:
                        pass
                    execute_with_stale_view_retry(
                        rum_con,
                        rum_src,
                        lambda c: backfill_missing_rum_aggregates(c, service_id, lookback_days=30),
                        table_name="client_vitals",
                    )
        except Exception as rum_heal_err:
            logger.warning("[rollup-heal] %s: RUM aggregate self-heal skipped/failed: %s", service_id, rum_heal_err)
        if durable_mode and not coverage_ready and heal.get("coverage_verified", False):
            try:
                from backend.core.rollups._common import mark_rollup_coverage_ready

                mark_rollup_coverage_ready(service_id)
            except Exception as e:
                logger.warning("[rollup-heal] %s: could not mark rollup coverage ready: %s", service_id, e)
        summary = (
            f"Healed {heal.get('missing', 0)} missing hour(s): "
            f"{heal.get('rebuilt_fields', 0)} field rollup(s) rebuilt, "
            f"{heal.get('bundled', 0)} hour(s) bundled, "
            f"{heal.get('stamped_empty', 0)} empty hour(s) stamped"
        )
        duration = run.elapsed()
        run.success(summary)
        if heal.get("missing", 0) or heal.get("stamped_empty", 0):
            logger.info("🏁  [rollup-heal] %s: %s in %.2fs", _display, summary, duration)
        outcome = {
            "status": "success",
            "service_id": service_id,
            "missing": heal.get("missing", 0),
            "rebuilt_fields": heal.get("rebuilt_fields", 0),
            "bundled": heal.get("bundled", 0),
            "stamped_empty": heal.get("stamped_empty", 0),
            "coverage_verified": heal.get("coverage_verified", False),
            "duration_s": duration,
            "summary": summary,
            "run_id": run.run_id,
        }
    return run.result if run.result is not None else outcome


def _call_with_lookback(fn: Any, *args: Any, lookback_days: int = 30) -> Any:
    return fn(*args, lookback_days=lookback_days)


@cron_task("rollup_compact_daily", job_name="rollup_compact_daily")
def _run_rollup_compact_daily(service_id: str, manual: bool = False, run_id: int | None = None) -> dict:
    from backend.core import rollups
    from backend.core.duckdb import get_source_for_service
    from backend.core.rollups import (
        backfill_day_bundles,
        backfill_missing_hour_bundles,
        backfill_missing_hour_ip_spread,
        compact_closed_days_to_daily,
        retire_compacted_hour_bundles,
    )
    from backend.core.rollups._common import _day_bundled_root
    from backend.core.rollups.recompute import DAY_COMPACTORS, cleanup_old_rollups
    from backend.cron.jobs.sync import has_sufficient_disk_space
    from backend.utils.active_requests import should_defer_cron

    if not manual and should_defer_cron("rollup_compact", service_id):
        logger.info("⏸️  [rollup-compact] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}
    ok_disk, msg = has_sufficient_disk_space(_day_bundled_root(src), "rollup-compact", service_id)
    if not ok_disk:
        record_skip_row(
            src,
            "rollup_compact_daily",
            "Skipped due to low disk space",
            status="warning",
            error_message=msg,
            run_id=run_id,
        )
        return {"status": "warning", "service_id": service_id, "summary": msg}
    _display = _display_label(src, service_id)
    outcome: dict = {}
    with cron_run(
        service_id,
        "rollup_compact_daily",
        source=src,
        run_id=run_id,
        error_summary="Daily rollup compaction failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "summary": str(exc),
            "errors": [str(exc)],
        },
        include_log_output=False,
        progress_task="rollup_compact",
        skip_log_prefix="rollup-compact",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.aborted:
            return dict(run.result) if run.result is not None else outcome
        logger.info("🏎️  [rollup-compact] %s: Daily rollup compaction started.", _display)
        subsystem_errors: list[str] = []
        for label, fn in (
            ("missing_hour_bundles", backfill_missing_hour_bundles),
            ("ip_spread", backfill_missing_hour_ip_spread),
        ):
            try:
                _call_with_lookback(fn, service_id, src, lookback_days=30)
            except Exception as e:
                subsystem_errors.append(f"{label}: {e}")
                logger.warning("[rollup-compact] %s: %s self-heal failed: %s", _display, label, e)
        rebuilt = _call_with_lookback(compact_closed_days_to_daily, service_id, src, lookback_days=30)
        try:
            bundled = _call_with_lookback(backfill_day_bundles, service_id, src, lookback_days=30)
        except Exception as e:
            subsystem_errors.append(f"day_bundles: {e}")
            logger.warning("[rollup-compact] %s: day-bundle backfill failed: %s", _display, e)
            bundled = 0
        for spec in DAY_COMPACTORS:
            try:
                _call_with_lookback(getattr(rollups, spec.compactor), service_id, src, lookback_days=30)
            except Exception as e:
                subsystem_errors.append(f"{spec.label}: {e}")
                logger.warning("[rollup-compact] %s: %s failed: %s", _display, spec.label, e)
        retired_files, retired_bytes = 0, 0
        try:
            retired_files, retired_bytes = _call_with_lookback(
                retire_compacted_hour_bundles, service_id, src, lookback_days=30
            )
        except Exception as e:
            subsystem_errors.append(f"retirement: {e}")
            logger.warning("[rollup-compact] %s: retirement failed: %s", _display, e)
        try:
            retention_months = int(src.get("rollup_retention_months", 12))
            max_age_days = int(src.get("rollups_days", retention_months * 30))
            if max_age_days > 0:
                cleanup_old_rollups(service_id, src, max_age_days=max_age_days, hour_bundle_max_age_days=14)
        except Exception as e:
            logger.warning("[rollup-compact] %s: cleanup pass failed: %s", _display, e)
        summary = (
            f"Rebuilt {rebuilt} (field, day) file(s); bundled {bundled} day(s); "
            f"retired {retired_files} hourly file(s) ({retired_bytes / 1024:.1f}KB)."
        )
        status = "warning" if subsystem_errors else "success"
        run.record(status, summary=summary, error_message="; ".join(subsystem_errors) if subsystem_errors else None)
        outcome = {
            "status": status,
            "service_id": service_id,
            "rebuilt": rebuilt,
            "bundled": bundled,
            "retired_files": retired_files,
            "retired_bytes": retired_bytes,
            "duration_s": round(run.elapsed(), 2),
            "summary": summary,
            "errors": subsystem_errors if subsystem_errors else None,
        }
    return run.result if run.result is not None else outcome


@cron_task("partial_hour_merge", job_name="partial_hour_merge")
def _run_partial_hour_merge(service_id: str) -> None:
    from backend.core.duckdb import get_source_for_service
    from backend.core.log_fields import derived
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
    from backend.repositories.dashboard import FIELDS
    from backend.utils.active_requests import should_defer_cron

    if should_defer_cron("partial_hour_merge", service_id) or (src := get_source_for_service(service_id)) is None:
        return
    with cron_run(
        service_id,
        "partial_hour_merge",
        source=src,
        skip_log_prefix="partial-hour",
        error_summary="partial-hour merge failed",
    ) as run:
        if run.skipped:
            return
        logger.info("🕐  [partial-hour] %s: partial-hour merge started.", _display_label(src, service_id))
        derived_fields = {f.code for f in derived()}
        merge_fields = [f for f in FIELDS if f not in _LIVE_TOPN_SKIP_FIELDS and f not in derived_fields]
        stats = merge_partial_hour(service_id, src, merge_fields)
        removed = gc_stale_partial_hours(src)
        summary = f"Merged {stats['new_files']} new file(s) into hour={stats['hour']}; GC'd {removed} stale hour(s)"
        run.success(summary)
        if stats["new_files"]:
            run.log_progress({"type": "status", "message": summary})


@cron_task("cron:expire_snapshots", job_name="expire_snapshots")
def _run_expire_snapshots(service_id: str, *, manual: bool = False) -> dict:
    from backend.core import storage as db_storage
    from backend.core.duckdb import get_source_for_service
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

    if dev_mode_no_crons():
        return {"status": "skipped", "service_id": service_id, "summary": "FLA_DEV_NO_CRONS=1"}
    if not manual and should_defer_cron("expire_snapshots", service_id):
        return {"status": "deferred", "service_id": service_id, "summary": "active requests"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "skipped", "service_id": service_id, "summary": "service not found"}
    display_name = _display_label(src, str(src.get("service_id", "unknown")))
    with cron_run(
        service_id,
        "expire_snapshots",
        source=src,
        skip_log_prefix="expire",
        finalize_duration=True,
        finalize_running_backstop=True,
        error_summary="Maintenance raised an uncaught exception",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error": str(exc),
            "summary": "Maintenance raised an uncaught exception",
        },
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.ok:
            logger.info("🏎️  \x1b[90m[expire]\x1b[0m %s: Maintenance job started.", display_name)
            run.log_progress({"type": "status", "message": "Starting snapshot expiry and retention cleanup..."})
            result = db_storage.run_cloud_maintenance(src)
            if "error" in result:
                logger.warning(
                    "%s %s: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, result["error"]
                )
                run.record("error", error_message=str(result["error"]), summary="Maintenance failed at catalog load")
                run.result = {
                    **result,
                    "status": "error",
                    "service_id": service_id,
                    "summary": "Maintenance failed at catalog load",
                }
                run.log_progress({"type": "error", "message": str(result["error"])})
            else:
                result = {
                    **result,
                    "retention_deleted_rows": int(
                        result.get("retention_deleted_rows", 0)
                        or sum(
                            int(result.get(k, 0) or 0)
                            for k in ("data_rows_deleted", "rum_log_rows_deleted", "rum_beacon_rows_deleted")
                        )
                    ),
                    "snapshots_expired": int(
                        result.get("snapshots_expired", result.get("snapshots_expired_count", 0)) or 0
                    ),
                    "files_unlinked": int(result.get("files_unlinked", result.get("data_files_cleaned", 0)) or 0),
                }
                summary_parts = [f"{k}={v}" for k, v in result.items() if not k.endswith("_error")]
                sub_errors = [f"{k}={v}" for k, v in result.items() if k.endswith("_error")]
                summary = ", ".join(summary_parts) if summary_parts else "no work to do"
                status = "warning" if sub_errors else "success"
                logger.info("🗑️ \x1b[90m[expire]\x1b[0m %s: Maintenance completed. %s", display_name, result)
                run.record(
                    status,
                    error_message="; ".join(sub_errors) if sub_errors else None,
                    summary=summary,
                    files_deleted_fos=result["files_unlinked"],
                )
                run.result = {**result, "status": status, "service_id": service_id, "summary": summary}
                run.log_progress({"type": "done" if status == "success" else status, "message": summary})
    logger.info("🏁  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)
    return dict(run.result) if isinstance(run.result, dict) else {}


@cron_task("cron.optimize", job_name="optimize")
def _run_optimize(service_id: str, manual: bool = False) -> dict | None:
    from backend.core import storage as db_storage
    from backend.core.duckdb import get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.cron.jobs.sync import publish_sync_status_safe
    from backend.utils.active_requests import should_defer_cron

    if not manual and dev_mode_no_crons():
        logger.warning("🚫 [scheduler] %s: FLA_DEV_NO_CRONS=1 — optimize refused.", service_id)
        return {"status": "skipped", "service_id": service_id, "summary": "Skipped due to FLA_DEV_NO_CRONS=1"}
    if not manual and should_defer_cron("optimize", service_id):
        logger.info("⏸️  [optimize] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}
    _display = _display_label(src, service_id)
    outcome: dict | None = None
    with cron_run(
        service_id,
        "optimize",
        source=src,
        error_summary="DuckLake optimize failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error": str(exc),
            "files_rewritten": 0,
            "files_added": 0,
            "summary": "DuckLake optimize failed",
        },
        finalize_duration=True,
        skip_log_prefix="optimize",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        logger.info("🏎️  \x1b[92m[optimize]\x1b[0m %s: Optimize job started.", _display)
        run.log_progress({"type": "status", "message": "Scanning DuckLake table for small files to compact..."})
        result = db_storage.optimize_table(src, min_files_per_partition=10)
        if "error" in result:
            run.fail("DuckLake optimize failed", error_message=result["error"])
            run.log_progress({"type": "error", "message": result["error"]})
            run.log_progress({"type": "warning", "message": result["error"]})
            outcome = {
                "status": "error",
                "service_id": service_id,
                "error": result["error"],
                "files_rewritten": 0,
                "files_added": 0,
                "summary": "DuckLake optimize failed",
            }
        else:
            summary = f"Rewrote {result.get('files_rewritten', 0)} files into {result.get('files_added', 0)} files"
            partition_errors = result.get("partition_errors") or []
            if partition_errors:
                summary += f" — {len(partition_errors)}/{result.get('eligible_partitions', 0)} partitions failed"
                err_preview: str | None = "\n".join(partition_errors[:3]) + (
                    f"\n... ({len(partition_errors) - 3} more)" if len(partition_errors) > 3 else ""
                )
                status = "error" if result.get("files_added", 0) == 0 else "warning"
            else:
                err_preview, status = None, "success"
            run.record(
                status,
                summary=summary,
                error_message=err_preview,
                parquet_files_optimized=result.get("files_rewritten", 0),
                parquet_files_created=result.get("files_added", 0),
            )
            run.log_progress({"type": "done" if status == "success" else status, "message": summary})
            logger.info("[scheduler] %s: optimize complete — %s", service_id, summary)
            outcome = {
                "status": status,
                "service_id": service_id,
                "files_rewritten": result.get("files_rewritten", 0),
                "files_added": result.get("files_added", 0),
                "eligible_partitions": result.get("eligible_partitions", 0),
                "partition_errors": partition_errors,
                "summary": summary,
            }
    if run.result is not None:
        outcome = run.result
    publish_sync_status_safe(service_id, "scheduler")
    logger.info("🏁  \x1b[92m[optimize]\x1b[0m %s: Optimize job finished.", _display)
    return outcome


_self_mod = _sys.modules[__name__]
_METADATA_ALIASES = (
    "backend.core.metric_snapshots",
    "backend.core.duckdb_recycle",
    "backend.cron.jobs.duckdb_recycle",
    "backend.cron.jobs.insights_prewarmer",
    "backend.cron.jobs.metric_snapshot",
    "backend.cron.jobs.compaction",
    "backend.cron.jobs.maintenance",
    "backend.cron.jobs.optimize",
    "backend.cron.jobs.expire",
)
for _alias in _METADATA_ALIASES:
    _sys.modules[_alias] = _self_mod
if (_core_pkg := _sys.modules.get("backend.core")) is not None:
    _core_pkg.metric_snapshots = _self_mod  # type: ignore[attr-defined]
    _core_pkg.duckdb_recycle = _self_mod  # type: ignore[attr-defined]
if (_jobs_pkg := _sys.modules.get("backend.cron.jobs")) is not None:
    for _attr in (
        "duckdb_recycle",
        "insights_prewarmer",
        "metric_snapshot",
        "compaction",
        "maintenance",
        "optimize",
        "expire",
    ):
        setattr(_jobs_pkg, _attr, _self_mod)
if (_backend_pkg := _sys.modules.get("backend")) is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(dict.fromkeys(_METADATA_ALIASES, "backend.cron.jobs.metadata"))
