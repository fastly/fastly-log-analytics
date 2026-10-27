"""Services CRUD, credentials, cron schedule/settings, and NGWAF sync endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Iterator

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.core import metadata as metadata_db
from backend.core.duckdb import ensure_cid_salt
from backend.core.request_context import get_service_id, get_source
from backend.models.common import (
    CredentialsUpdateResponse,
    CronScheduleResponse,
    OkMessageResponse,
    ServiceCredentialsBody,
    ServiceCronSettingsBody,
    ServicesListResponse,
)
from backend.utils.auth import require_service_in_scope
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    mutate_service_config,
    raise_internal,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

router = APIRouter()

_CRON_SCHEDULE_TTL = 5.0
_cron_schedule_cache: dict[str, tuple[float, dict]] = {}

# Whitelisted fields safe to surface to a remote analyst on GET /api/services.
_ANALYST_SAFE_SERVICE_FIELDS = frozenset({"service_id", "name", "is_active"})


def _sse(type_: str, message: str | None = None, **extra: object) -> str:
    payload: dict[str, object] = {"type": type_}
    if message is not None:
        payload["message"] = message
    payload.update(extra)
    return json.dumps(payload)


def _trim_for_analyst(services: list[dict], allowed_ids: set[str]) -> list[dict]:
    return [
        {k: v for k, v in svc.items() if k in _ANALYST_SAFE_SERVICE_FIELDS}
        for svc in services
        if svc.get("service_id", "") in allowed_ids
    ]


def _record_audit_best_effort(service_id: str, event_type: str, details: dict, caller: str) -> None:
    try:
        metadata_db.record_audit(service_id=service_id, event_type=event_type, details=details)
    except Exception:
        logger.warning("%s: metadata_db.record_audit failed (ignored)", caller, exc_info=True)


def _store_fos_credentials(cfg: dict, access_key: str, secret_key: str) -> None:
    cfg["fos_access_key_id"] = access_key
    cfg["fos_secret_access_key"] = secret_key
    cfg.setdefault("provisioning", {})["fos_key_id"] = access_key


@router.get("/services", response_model=ServicesListResponse)
def api_services_list(request: Request, service_id: str | None = Depends(get_service_id)):
    from backend.services.service_manager import get_enriched_services

    result = get_enriched_services(service_id)
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        result = _trim_for_analyst(result, set(analyst_session.service_ids or []))
    return ServicesListResponse.with_telemetry(services=result, debug_queries=[])


@router.post("/services/{service_id}/cron-settings")
def api_service_cron_settings(request: Request, service_id: str, body: ServiceCronSettingsBody):
    from backend import config as svcconfig

    require_service_in_scope(request, service_id)
    body_payload = body.model_dump(exclude_unset=True)

    def stream():
        try:
            yield _sse("progress", current=0, total=4)
            yield _sse("status", f"Applying configuration for {service_id}...")
            if not svcconfig.load_config(service_id):
                raise ValueError("Service not found")
            yield _sse("progress", current=1, total=4)
            yield _sse("status", "Updating sync and compaction schedules...")

            def _apply(cfg: dict) -> None:
                prov = cfg.setdefault("provisioning", {})
                for cron_key in ("cron_sync", "cron_compact", "cron_ngwaf"):
                    incoming = body_payload.get(cron_key)
                    if incoming is None:
                        continue
                    existing = prov.get(cron_key, {})
                    sent = {k: v for k, v in incoming.items() if v is not None}
                    if "interval_seconds" in sent and "interval_mins" not in sent:
                        existing.pop("interval_mins", None)
                    existing.update(sent)
                    prov[cron_key] = existing

                rum_incoming = body_payload.get("rum")
                if rum_incoming is not None:
                    rum_config = cfg.setdefault("rum", {})
                    sent_rum = {k: v for k, v in rum_incoming.items() if v is not None}
                    if "log_period" in sent_rum and "sync_interval_seconds" not in sent_rum:
                        rum_config.pop("sync_interval_seconds", None)
                    rum_config.update(sent_rum)
                    ensure_cid_salt(rum_config)
                    cfg["rum"] = rum_config
                    if "log_period" in sent_rum:
                        cfg["rum_log_period"] = sent_rum["log_period"]

            yield _sse("progress", current=2, total=4)
            yield _sse("status", "Saving settings to local database...")
            if svcconfig.mutate_config(service_id, _apply) is None:
                raise ValueError("Service not found")
            yield _sse("progress", current=3, total=4)
            yield _sse("status", "Synchronizing APScheduler...")
            try:
                from backend.provision import _sync_crontab

                _sync_crontab()
            except Exception as e:
                yield _sse("status", f"Warning: Cron sync failed: {e}")
            _record_audit_best_effort(service_id, "cron_settings_update", body_payload, "stream")
            yield _sse("progress", current=4, total=4)
            yield _sse("done", "Successfully applied changes.")
        except Exception as e:
            yield _sse("error", str(e))

    return sse_response(stream())


@router.delete(
    "/services/{service_id}/time-range",
    response_model=OkMessageResponse,
    response_model_exclude_unset=True,
)
def api_service_clear_time_range(service_id: str):
    """Remove the persisted time_range filter from a service config."""
    cfg = load_service_config(service_id)
    if "time_range" not in cfg.get("provisioning", {}):
        return {"ok": True, "message": "No time_range was set."}
    mutate_service_config(service_id, lambda c: c.setdefault("provisioning", {}).pop("time_range", None))
    _record_audit_best_effort(service_id, "time_range_cleared", {}, "api_service_clear_time_range")
    return {"ok": True, "message": "time_range cleared. Cron will now ingest all available data."}


@router.get("/cron-runs/{run_id}/stream")
async def cron_logs_stream(run_id: int, service_id: str | None = Depends(get_service_id)):
    from backend.cron.scheduler import get_progress

    def _norm(s: str) -> str:
        return "done" if s in ("success", "done") else "error" if s in ("failed", "error") else s

    _valid_tags = ("status", "error", "done", "warning", "info", "success", "failed")

    async def stream():
        last_idx, retries = 0, 0
        while True:
            evs = get_progress(run_id, last_idx, service_id=service_id)
            if evs is None:
                if last_idx == 0:
                    try:
                        if service_id:
                            row = await asyncio.to_thread(metadata_db.get_cron_run_result, service_id, run_id)
                            if row:
                                status, log_output = row["status"], row["log_output"]
                                norm_status = _norm(status)
                                if norm_status in ("done", "error") or log_output:
                                    if log_output:
                                        has_terminal = False
                                        for line in log_output.split("\n"):
                                            if not line.strip():
                                                continue
                                            t, msg = "status", line
                                            if line.startswith("[") and "]" in line:
                                                b_end = line.find("]")
                                                tag = line[1:b_end].lower()
                                                if tag in _valid_tags:
                                                    t, msg = _norm(tag), line[b_end + 1 :].strip()
                                            if t in ("done", "error"):
                                                has_terminal = True
                                            yield _sse(t, msg)
                                        if not has_terminal and norm_status in ("done", "error"):
                                            yield _sse(norm_status, f"Run completed with status {status}.")
                                    else:
                                        task_name = (
                                            row.get("task") if isinstance(row, dict) else (row["task"] if row else None)
                                        ) or "unknown task"
                                        msg = (
                                            "No new RUM logs found."
                                            if norm_status == "done" and task_name == "rum_sync"
                                            else f"{task_name} completed with no data."
                                            if norm_status == "done"
                                            else f"Run completed with status {status}."
                                        )
                                        yield _sse(norm_status, msg)
                                    return
                    except Exception as e:
                        logger.error(f"Error fetching historical logs for run {run_id}: {e}")

                if last_idx == 0 and retries < 6:
                    retries += 1
                    await asyncio.sleep(0.5)
                    continue
                if last_idx == 0:
                    yield _sse(
                        "error",
                        f"No live progress for run {run_id} (run orphaned — interrupted "
                        "by a server restart, or it exited without recording logs). "
                        "Trigger a new sync to start fresh.",
                    )
                break
            if evs:
                for ev in evs:
                    yield json.dumps(ev)
                    if ev.get("type") in ("done", "error"):
                        return
                last_idx += len(evs)
            await asyncio.sleep(0.5)

    return sse_response(stream())


@router.get("/cron-schedule", response_model=CronScheduleResponse, response_model_exclude_unset=True)
def api_cron_schedule(source: dict = Depends(get_source)):
    from backend.cron.scheduler import build_cron_schedule_payload

    service_id = source["name"]
    now_mono = time.monotonic()
    cached = _cron_schedule_cache.get(service_id)
    if cached is not None and (now_mono - cached[0]) < _CRON_SCHEDULE_TTL:
        return cached[1]
    payload = build_cron_schedule_payload(source)
    _cron_schedule_cache[service_id] = (now_mono, payload)
    return payload


@router.patch(
    "/services/{service_id}/credentials",
    response_model=CredentialsUpdateResponse,
    response_model_exclude_unset=True,
)
def api_service_update_credentials(request: Request, service_id: str, body: ServiceCredentialsBody):
    """Rotate or validate and update FOS credentials for a service."""
    import botocore.exceptions

    from backend.core.duckdb import _get_fos_client

    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket", "")
    endpoint = cfg.get("fos_endpoint") or f"{region}.object.fastlystorage.app"
    api_token, access_key, secret_key = body.api_token.strip(), body.access_key.strip(), body.secret_key.strip()
    if api_token:
        from backend.core.fastly.client import fastly

        desc = f"fos-log-analysis-{service_id}-{int(time.time())}"
        try:
            key = fastly(
                "POST",
                "/resources/object-storage/access-keys",
                {"permission": "read-write-objects", "description": desc, "buckets": [bucket]},
                token=api_token,
            )
        except RuntimeError as e:
            raise_internal(logger, e, code="fos_key_create_failed", status=400)
        old_key_id = cfg.get("provisioning", {}).get("fos_key_id")
        if old_key_id and old_key_id != key["access_key"]:
            try:
                fastly(
                    "DELETE", f"/resources/object-storage/access-keys/{old_key_id}", token=api_token, expect_empty=True
                )
            except RuntimeError:
                pass
        mutate_service_config(service_id, lambda c: _store_fos_credentials(c, key["access_key"], key["secret_key"]))
        return {"ok": True, "message": "Key rotated via Fastly API", "access_key_id": key["access_key"]}
    if not access_key or not secret_key:
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "fos_credentials_missing", "Provide either api_token (admin) or both access_key and secret_key"
            ),
        )
    src = {
        "bucket": bucket,
        "endpoint": endpoint,
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "region": region,
        "storage_mode": "cloud",
    }
    try:
        _get_fos_client(src).list_objects_v2(Bucket=bucket, MaxKeys=1)
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = (
            "Validation failed: access denied. Check the key and secret."
            if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch")
            else str(code)
        )
        raise HTTPException(status_code=400, detail=make_error("fos_credentials_invalid", msg))
    except Exception as e:
        raise_internal(logger, e, code="fos_credentials_failed", status=400)
    mutate_service_config(service_id, lambda c: _store_fos_credentials(c, access_key, secret_key))
    return {"ok": True, "message": "Credentials updated successfully"}


@router.post("/services/{service_id}/ngwaf-sync")
def api_ngwaf_sync(service_id: str):
    """Manually trigger an NGWAF bot-sync run for a service, streamed as SSE."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.rollups.security_dims import (
        cleanup_old_bots,
        fetch_verified_bots_paged,
        oldest_unenriched_timestamp,
        upsert_bots,
    )
    from backend.utils.bot_sources import build_matcher
    from backend.utils.date_utils import iso_z_now

    def stream() -> Iterator[str]:
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            yield _sse("error", "Service not found")
            return
        workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
        if not workspace_id:
            yield _sse("error", "No NGWAF workspace configured for this service")
            return
        api_key = stored_fastly_token(cfg)
        if not api_key:
            yield _sse("error", "No Fastly API key stored for this service")
            return
        src = get_source_for_service(service_id)
        if src is None:
            yield _sse("error", "Service source not found")
            return
        try:
            run_id = start_cron_run(src, "ngwaf_sync")
        except RuntimeError as e:
            yield _sse("error", str(e))
            return
        retention_days = int(cfg.get("provisioning", {}).get("cron_ngwaf", {}).get("log_retention_days", 30))
        server_name_filter = cfg.get("server_name") or None
        matcher = build_matcher()
        from_ts = oldest_unenriched_timestamp(src)
        if not from_ts:
            summary = "All requests are already enriched. Nothing to sync."
            log_cron_run(src, "ngwaf_sync", 0.0, "success", summary=summary, run_id=run_id)
            yield _sse("done", summary)
            return
        until_ts = iso_z_now()
        yield _sse("status", f"Scanning {from_ts} → {until_ts}...")
        total_records, total_raw, page_num = 0, 0, 0
        start_time = time.time()
        try:
            for page_records, page_latest_ts, raw_count in fetch_verified_bots_paged(
                api_key, workspace_id, from_ts, until_ts=until_ts
            ):
                page_num += 1
                total_raw += raw_count
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
                yield _sse(
                    "status",
                    f"Page {page_num}: {raw_count} API records, {len(enriched)} verified-bot ({total_records} total so far)...",
                )
                if time.time() - start_time >= 240:
                    summary = f"Synced {total_records} bot record(s) from {total_raw} API records across {page_num} page(s) — budget reached, run again to continue."
                    break
            else:
                deleted = cleanup_old_bots(retention_days)
                summary = f"Synced {total_records} bot record(s) from {total_raw} API records across {page_num} page(s), cleaned {deleted} old row(s)."
            log_cron_run(src, "ngwaf_sync", time.time() - start_time, "success", summary=summary, run_id=run_id)
            yield _sse("done", summary)
        except Exception as e:
            log_cron_run(src, "ngwaf_sync", time.time() - start_time, "error", error_message=str(e), run_id=run_id)
            yield _sse("error", str(e))

    return sse_response(stream())
