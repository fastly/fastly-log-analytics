"""Services management router — list, cron settings, cron logs, logging settings, log fields."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import urllib.parse
from collections.abc import Iterator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from backend.core import duckdb as _db
from backend.core import field_registry as lf_module
from backend.core import metadata as metadata_db
from backend.core.duckdb import ensure_cid_salt
from backend.core.log_fields import _filter_user_custom_fields, _is_system_field
from backend.deps import get_service_id, get_source
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.common import (
    AuditLogsResponse,
    CmcdSettingsResponse,
    CredentialsUpdateResponse,
    CronRunsResponse,
    CronScheduleResponse,
    CustomFieldCreate,
    CustomFieldResponse,
    CustomFieldsExportFile,
    CustomFieldsImportBody,
    CustomFieldsImportResponse,
    CustomFieldsListResponse,
    CustomFieldUpdate,
    LogFieldsResponse,
    LogFieldsUpdateRequest,
    LogFieldsUpdateResponse,
    LoggingSettingsResponse,
    OkMessageResponse,
    ServiceCredentialsBody,
    ServiceCronSettingsBody,
    ServicesListResponse,
    VclLintRequest,
    VclLintResponse,
)
from backend.repositories import assets as service_stats_repo
from backend.repositories._base import SectionTimer
from backend.repositories.alerts import delete_cron_log, get_cron_logs, purge_cron_logs
from backend.utils.auth import require_service_in_scope
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    mutate_service_config,
    query_errors,
    raise_internal,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

core_router = APIRouter(prefix="/api", tags=["services"], responses=DEFAULT_ERROR_RESPONSES)
audit_router = APIRouter(prefix="/api/audit-logs", tags=["audit-logs"], responses=DEFAULT_ERROR_RESPONSES)
cron_router = APIRouter(prefix="/api/cron-runs", tags=["cron-runs"], responses=DEFAULT_ERROR_RESPONSES)

_CRON_SCHEDULE_TTL = 5.0
_cron_schedule_cache: dict[str, tuple[float, dict]] = {}

_LOGGING_SETTINGS_CACHE_TTL = 300.0
_logging_settings_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_LOGGING_SETTINGS_CACHE_TTL)

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


@core_router.get("/services", response_model=ServicesListResponse)
def api_services_list(request: Request, service_id: str | None = Depends(get_service_id)):
    from backend.services.service_manager import get_enriched_services

    result = get_enriched_services(service_id)
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        result = _trim_for_analyst(result, set(analyst_session.service_ids or []))
    return ServicesListResponse.with_telemetry(services=result, debug_queries=[])


@core_router.post("/services/{service_id}/cron-settings")
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


@core_router.delete(
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


@core_router.get("/cron-runs/{run_id}/stream")
async def cron_logs_stream(run_id: int, service_id: str | None = Depends(get_service_id)):
    from backend.cron_progress import get_progress

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


@core_router.get("/cron-schedule", response_model=CronScheduleResponse, response_model_exclude_unset=True)
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


def _store_fos_credentials(cfg: dict, access_key: str, secret_key: str) -> None:
    cfg["fos_access_key_id"] = access_key
    cfg["fos_secret_access_key"] = secret_key
    cfg.setdefault("provisioning", {})["fos_key_id"] = access_key


@core_router.patch(
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


def _cmcd_settings_response(
    cfg: dict,
    enabled: bool | None = None,
    mode: str | None = None,
    version: int | None = None,
) -> CmcdSettingsResponse:
    cmcd_block = cfg.get("cmcd") or {}
    return CmcdSettingsResponse(
        enabled=bool(enabled) if enabled is not None else bool(cmcd_block.get("enabled")),
        mode=mode if mode is not None else cmcd_block.get("mode"),
        version=version if version is not None else cmcd_block.get("version"),
    )


@core_router.get("/services/{service_id}/logging-settings", response_model=LoggingSettingsResponse)
@query_errors(500, code="logging_settings_save_failed", logger_obj=logger)
def api_service_logging_settings(service_id: str):
    timer = SectionTimer()
    cached_fields = _logging_settings_cache.get(service_id)
    if cached_fields is not None:
        return LoggingSettingsResponse.with_telemetry(ok=True, section_timings=[], is_cached=True, **cached_fields)

    cfg = load_service_config(service_id)
    token = stored_fastly_token(cfg)
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    from backend.core.fastly.client import fastly, find_condition, get_active_version

    _t = time.perf_counter()
    active_ver = get_active_version(service_id, token)
    timer.mark("get_active_version", _t)
    if not active_ver:
        raise HTTPException(status_code=400, detail={"error": "No active version found"})
    encoded_name = urllib.parse.quote(endpoint_name, safe="")
    _t = time.perf_counter()
    ep = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/s3/{encoded_name}", token=token)
    timer.mark("get_logging_endpoint", _t)
    sample_rate, edge_only, custom_condition = 100, False, ""
    cond_name = ep.get("response_condition")
    if cond_name:
        _t = time.perf_counter()
        cond = find_condition(cond_name, service_id, active_ver, token)
        timer.mark("find_condition", _t)
        stmt = cond.get("statement", "") if cond else ""
        if m := re.search(r"randombool\((\d+),", stmt):
            sample_rate = int(m.group(1))
        edge_only = "req.restarts == 0" in stmt
        if mc := re.search(r" && \((?!req\.restarts == 0)(.+)\)$", stmt):
            custom_condition = mc.group(1)
    if not custom_condition:
        custom_condition = cfg.get("provisioning", {}).get("custom_condition", "")
    m_pref = re.search(r"\A/?(.*?)/raw/", ep.get("path", ""))
    prefix = m_pref.group(1).strip("/") if m_pref else ""
    format_match = True
    try:
        from backend.provision import load_log_format

        format_match = (ep.get("format") or "").strip() == load_log_format(cfg.get("log_fields"))
    except Exception:
        logger.debug("api_service_logging_settings: step failed (ignored)", exc_info=True)

    cacheable = {
        "prefix": prefix,
        "period": ep.get("period", 60),
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "custom_condition": custom_condition,
        "format_match": format_match,
        "version": active_ver,
        "cmcd": _cmcd_settings_response(cfg),
    }
    _logging_settings_cache[service_id] = cacheable
    return LoggingSettingsResponse.with_telemetry(ok=True, section_timings=timer.entries, **cacheable)


@core_router.get("/services/{service_id}/log-fields", response_model=LogFieldsResponse)
def api_service_log_fields_get(service_id: str):
    cfg = load_service_config(service_id)
    log_fields_config = lf_module.get_lf_config(cfg)
    if not log_fields_config.get("groups"):
        log_fields_config = {"groups": lf_module.PRESETS["standard"]["groups"], "field_overrides": {}}
    waf_warning = False
    if "J" in log_fields_config.get("groups", []):
        try:
            src = _db.get_source_for_service(service_id)
            if src:
                c = _db.get_connection(source=src, read_only=True, max_wait=5.0, skip_view_update=True)
                try:
                    actual_cols = {col["name"] for col in _db.get_schema(c, src, stats=False)}
                    if "waf" in actual_cols:
                        res = service_stats_repo.get_waf_flag_counts_7d(c, _db._safe_table_name(src["name"]))
                        if res and res[1] and (res[1] >= 10000) and ((res[0] or 0) == 0):
                            waf_warning = True
                finally:
                    c.close()
        except Exception:
            logger.debug("api_service_log_fields_get: step failed (ignored)", exc_info=True)
    history = []
    try:
        _, history_rows = metadata_db.get_audit_logs(service_id, event_type="log_format_change", page=1, per_page=20)
        for r in history_rows:
            d = r["details"] if isinstance(r["details"], dict) else {}
            history.append(
                {
                    "changed_at": r["timestamp"],
                    "preset": d.get("preset"),
                    "groups_before": d.get("groups_before") or [],
                    "groups_after": d.get("groups_after") or [],
                    "fields_added": d.get("fields_added") or [],
                    "fields_removed": d.get("fields_removed") or [],
                    "changed_by": r["actor"],
                }
            )
    except Exception:
        logger.debug("api_service_log_fields_get: step failed (ignored)", exc_info=True)

    return LogFieldsResponse.with_telemetry(
        log_fields=log_fields_config,
        waf_warning=waf_warning,
        history=history,
        estimate=lf_module.estimate_log_line_bytes(log_fields_config),
        line_budget_warning=lf_module.check_log_line_budget(log_fields_config),
    )


@core_router.post(
    "/services/{service_id}/log-fields",
    response_model=LogFieldsUpdateResponse,
    response_model_exclude_unset=True,
)
def api_service_log_fields_set(request: Request, service_id: str, body: LogFieldsUpdateRequest):
    from backend.provision.orchestrator import reconcile_system_custom_fields

    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    raw_lf = body.log_fields
    new_lf: dict = (
        raw_lf.model_dump(exclude_none=True) if hasattr(raw_lf, "model_dump") else (dict(raw_lf) if raw_lf else {})
    )
    if not new_lf:
        raise HTTPException(status_code=400, detail={"error": "log_fields is required"})
    old_lf = cfg.get("log_fields", {})
    # Merge guard: preserve existing custom_fields unless caller explicitly sent a non-empty list,
    # then re-assert system custom fields (scoring + CMCD) keyed on current service state.
    if not new_lf.get("custom_fields") and old_lf.get("custom_fields"):
        new_lf["custom_fields"] = old_lf["custom_fields"]
    new_lf["custom_fields"] = reconcile_system_custom_fields(
        new_lf.get("custom_fields"),
        scoring_enabled=bool(cfg.get("scoring", {}).get("enabled")),
        cmcd_enabled=bool(cfg.get("cmcd", {}).get("enabled")),
    )
    new_lf["schema_version"] = 2
    old_hash = old_lf.get("format_hash")
    new_hash = lf_module.format_hash(new_lf)
    if old_hash == new_hash:
        return {"ok": True, "message": "No changes detected"}
    new_lf["format_hash"] = new_hash
    new_lf["format_updated_at"] = datetime.now(UTC).isoformat()
    cfg["log_fields"] = new_lf
    mutate_service_config(service_id, lambda c: c.__setitem__("log_fields", new_lf))
    from backend.repositories.query import invalidate_presets_cache

    invalidate_presets_cache(service_id, old_hash)
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(service_id)
    except Exception:
        logger.warning("api_service_log_fields_set: export_admin_state failed (ignored)", exc_info=True)
    return {
        "ok": True,
        "estimate": lf_module.estimate_log_line_bytes(new_lf),
        "line_budget_warning": lf_module.check_log_line_budget(new_lf),
    }


@core_router.post("/services/{service_id}/logging-settings/update")
def api_service_update_logging_settings(
    request: Request,
    service_id: str,
    period: int | None = Query(default=None),
    sample_rate: int | None = Query(default=None),
    prefix: str | None = Query(default=None),
    edge_only: bool | None = Query(default=None),
    custom_condition: str | None = Query(default=None),
    update_format: bool = Query(default=False),
    cmcd_enabled: bool | None = Query(default=None),
    cmcd_mode: str | None = Query(default=None),
    cmcd_version: int | None = Query(default=None),
):
    from backend import config as svcconfig
    from backend.provision.cli import analytics_log_path

    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    prov = cfg.setdefault("provisioning", {})
    old_period = int(cfg.get("log_period", 60))
    old_sample_rate = int(prov.get("sample_rate", 100))
    old_prefix = ""
    old_edge_only = prov.get("edge_only", False)
    old_custom_condition = prov.get("custom_condition", "")

    period = old_period if period is None else period
    sample_rate = old_sample_rate if sample_rate is None else sample_rate
    if prefix:
        raise HTTPException(status_code=400, detail={"error": "Custom FOS prefixes are not supported in v3"})
    prefix = ""
    edge_only = old_edge_only if edge_only is None else edge_only
    custom_condition = old_custom_condition if custom_condition is None else custom_condition
    if not 1 <= period <= 86400:
        raise HTTPException(status_code=400, detail={"error": "Rotation period must be between 1 and 86400 seconds"})
    if not 1 <= sample_rate <= 100:
        raise HTTPException(status_code=400, detail={"error": "Sample rate must be between 1 and 100"})
    token = stored_fastly_token(cfg)
    endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
    path = analytics_log_path()

    def stream():
        try:
            from backend.provision import update_logging_endpoint

            update_cfg = {
                "logging_service_id": service_id,
                "endpoint_name": endpoint_name,
                "log_period": period,
                "fos_path": path,
                "sample_rate": sample_rate,
                "edge_only": edge_only,
                "custom_condition": custom_condition,
                "update_format": update_format,
                "cmcd_enabled": cmcd_enabled,
                "cmcd_mode": cmcd_mode,
                "cmcd_version": cmcd_version,
            }
            for event in update_logging_endpoint(update_cfg, token):
                if event.get("type") == "done":
                    new_ver = int(event.get("version") or 0)
                    if event.get("changed", False):
                        if update_format and new_ver:
                            _logging_settings_cache[service_id] = {
                                "prefix": "",
                                "period": period,
                                "sample_rate": sample_rate,
                                "edge_only": edge_only,
                                "custom_condition": custom_condition,
                                "format_match": True,
                                "version": new_ver,
                                "cmcd": _cmcd_settings_response(cfg, cmcd_enabled, cmcd_mode, cmcd_version),
                            }
                        else:
                            _logging_settings_cache.pop(service_id, None)

                    def _apply_logging_settings(fresh_cfg: dict) -> None:
                        fresh_prov = fresh_cfg.setdefault("provisioning", {})
                        fresh_prov["sample_rate"] = sample_rate
                        fresh_prov["edge_only"] = edge_only
                        fresh_prov["custom_condition"] = custom_condition
                        fresh_cfg["log_period"] = period
                        fresh_cfg["fos_prefix"] = ""
                        if "cron_sync" in fresh_prov:
                            if period < 60:
                                fresh_prov["cron_sync"]["interval_seconds"] = period
                                fresh_prov["cron_sync"].pop("interval_mins", None)
                            else:
                                fresh_prov["cron_sync"]["interval_mins"] = max(1, period // 60)
                                fresh_prov["cron_sync"].pop("interval_seconds", None)

                    svcconfig.mutate_config(service_id, _apply_logging_settings)
                    if event.get("changed", False):
                        _details: dict = {
                            k: {"from": old_v, "to": new_v}
                            for k, old_v, new_v in (
                                ("period", old_period, period),
                                ("sample_rate", old_sample_rate, sample_rate),
                                ("prefix", old_prefix, prefix),
                                ("edge_only", old_edge_only, edge_only),
                                ("custom_condition", old_custom_condition, custom_condition),
                            )
                            if old_v != new_v
                        }
                        for k, v in (
                            ("cmcd_enabled", cmcd_enabled),
                            ("cmcd_mode", cmcd_mode),
                            ("cmcd_version", cmcd_version),
                        ):
                            if v is not None:
                                _details[k] = v
                        if update_format:
                            _details["log_fields_deployed"] = True
                        _record_audit_best_effort(service_id, "logging_settings_update", _details, "stream")
                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()
                    except Exception:
                        logger.warning("stream: _sync_crontab failed (ignored)", exc_info=True)
                yield json.dumps(event)
        except Exception as e:
            yield _sse("error", str(e))

    return sse_response(stream())


@core_router.post("/services/{service_id}/ngwaf-sync")
def api_ngwaf_sync(service_id: str):
    """Manually trigger an NGWAF bot-sync run for a service, streamed as SSE."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.utils.bot_sources import build_matcher
    from backend.utils.date_utils import iso_z_now
    from backend.utils.ngwaf import (
        cleanup_old_bots,
        fetch_verified_bots_paged,
        oldest_unenriched_timestamp,
        upsert_bots,
    )

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


@core_router.get("/services/{service_id}/custom-fields", response_model=CustomFieldsListResponse)
def api_list_custom_fields(request: Request, service_id: str) -> CustomFieldsListResponse:
    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    return CustomFieldsListResponse(fields=_filter_user_custom_fields(lf.get("custom_fields", [])))  # type: ignore


def _committed_duckdb_field_types(src: dict) -> dict[str, str]:
    """Fallback schema lookup via the service's DuckDB/DuckLake schema."""
    db_path = _db.db_path_for_source(src)
    if not os.path.exists(db_path):
        return {}
    con = _db.get_connection(source=src, read_only=True, max_wait=2.0, skip_view_update=True)
    try:
        return {
            str(col.get("name") or col.get("column")): str(col.get("type"))
            for col in _db.get_schema(con, src, stats=False)
            if (col.get("name") or col.get("column")) and col.get("type")
        }
    finally:
        con.close()


def _check_storage_type_lock(
    service_id: str, field_name: str, new_duckdb_type: str | None = None, new_value_type: str | None = None
) -> None:
    """Ensure we don't mutate the core type of an existing field in the committed table."""
    from backend.core import storage as _storage

    src = _db.get_source_for_service(service_id)
    if not src or src.get("storage_mode", "cloud") == "local":
        return
    try:
        duckdb_to_arrow = getattr(_storage, "_DUCKDB_TO_ARROW", {})
        existing_duckdb_type = _committed_duckdb_field_types(src).get(field_name)
        if existing_duckdb_type and new_duckdb_type:
            current_mapped = duckdb_to_arrow.get(existing_duckdb_type.upper(), existing_duckdb_type.upper())
            target_mapped = duckdb_to_arrow.get(new_duckdb_type.upper(), new_duckdb_type.upper())
            if current_mapped != target_mapped:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "errors": [
                            f"Cannot change data type for '{field_name}' (currently {current_mapped}) after it has been created in the database. Please create a new field instead."
                        ]
                    },
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={
                "errors": [
                    f"Could not verify the committed type for '{field_name}' "
                    f"({type(e).__name__}); the change was not applied. Please retry."
                ]
            },
        ) from e


def _locked_storage_field_names(src: dict | None, *, error_lead: str, error_tail: str) -> set[str]:
    """Return committed DuckLake/DuckDB-schema field names for ``src``; fail closed (503) on error."""
    if not src or src.get("storage_mode", "cloud") == "local":
        return set()
    try:
        return set(_committed_duckdb_field_types(src).keys())
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"errors": [f"{error_lead} ({type(e).__name__}); {error_tail}"]},
        ) from e


def _persist_log_fields(lf: dict, new_custom: list, candidate_lf: dict, now: str, *, full: bool = True) -> dict:
    """Build the canonical persisted ``log_fields`` dict for a custom-field write."""
    out: dict = {**lf, "custom_fields": new_custom}
    if full:
        out["groups"] = lf.get("groups", [])
        out["field_overrides"] = lf.get("field_overrides", {})
        out["field_limits"] = lf.get("field_limits", {})
    out["format_hash"] = lf_module.format_hash(candidate_lf)
    out["format_updated_at"] = now
    out["schema_version"] = 2
    return out


def _validate_log_format_version(candidate_lf: dict) -> None:
    from backend import provision

    fmt_errors = provision.validate_log_format(candidate_lf)
    if any("LOG_FORMAT_TOO_LONG" in e for e in fmt_errors):
        raise HTTPException(status_code=422, detail={"errors": fmt_errors})


def _persist_custom_fields_change(
    service_id: str, cfg: dict, lf: dict, new_custom: list, candidate_lf: dict, now: str, *, full: bool = True
) -> None:
    new_lf = _persist_log_fields(lf, new_custom, candidate_lf, now, full=full)
    cfg["log_fields"] = new_lf
    mutate_service_config(service_id, lambda c: c.__setitem__("log_fields", new_lf))


def _load_modifiable_custom_fields(
    request: Request, service_id: str, field_name: str, action_verb: str
) -> tuple[dict, dict, list]:
    require_service_in_scope(request, service_id)
    if _is_system_field(field_name):
        raise HTTPException(
            status_code=422,
            detail={"errors": [f"Field '{field_name}' is system-managed and cannot be {action_verb}."]},
        )
    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    return cfg, lf, lf.get("custom_fields", [])


def _split_custom_field_warnings(items: list[str], *, strip_prefix: bool = False) -> tuple[list[str], list[str]]:
    hard_errors: list[str] = []
    warnings: list[str] = []
    for item in items:
        if item.startswith("WARN"):
            warnings.append(item.removeprefix("WARN: ") if strip_prefix else item)
        else:
            hard_errors.append(item)
    return hard_errors, warnings


def _validate_and_save_custom_field(
    service_id: str, cfg: dict, lf: dict, field_dict: dict, other_names: list[str], new_custom: list[dict], now: str
) -> dict:
    hard_errors, warnings = _split_custom_field_warnings(lf_module.validate_custom_field(field_dict, other_names))
    if hard_errors:
        raise HTTPException(status_code=422, detail={"errors": hard_errors})
    candidate_lf = {**lf, "custom_fields": new_custom}
    _validate_log_format_version(candidate_lf)
    _persist_custom_fields_change(service_id, cfg, lf, new_custom, candidate_lf, now)
    return {"ok": True, "field": field_dict, "warnings": warnings}


@core_router.post("/services/{service_id}/custom-fields", response_model=CustomFieldResponse)
def api_create_custom_field(request: Request, service_id: str, body: CustomFieldCreate):
    field_dict = body.model_dump()
    cfg, lf, existing = _load_modifiable_custom_fields(
        request, service_id, field_dict.get("name", ""), "created manually"
    )
    _check_storage_type_lock(
        service_id, field_dict["name"], field_dict.get("duckdb_type"), field_dict.get("value_type")
    )
    now = datetime.now(UTC).isoformat()
    field_dict["created_at"] = field_dict["updated_at"] = now
    return _validate_and_save_custom_field(
        service_id, cfg, lf, field_dict, [cf["name"] for cf in existing], existing + [field_dict], now
    )


@core_router.patch("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_update_custom_field(request: Request, service_id: str, field_name: str, body: CustomFieldUpdate):
    cfg, lf, existing = _load_modifiable_custom_fields(request, service_id, field_name, "edited")
    idx = next((i for i, cf in enumerate(existing) if cf["name"] == field_name), None)
    if idx is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    updates = body.model_dump(exclude_unset=True)
    old_field = existing[idx]
    changing_type = ("duckdb_type" in updates and updates["duckdb_type"] != old_field.get("duckdb_type")) or (
        "value_type" in updates and updates["value_type"] != old_field.get("value_type")
    )
    if changing_type:
        src = _db.get_source_for_service(service_id)
        schema_fields = _locked_storage_field_names(
            src,
            error_lead=f"Could not verify the committed schema for '{field_name}'",
            error_tail="the change was not applied. Please retry.",
        )
        if field_name in schema_fields:
            raise HTTPException(
                status_code=422,
                detail={
                    "errors": [
                        "Cannot change 'duckdb_type' or 'value_type' after the field has been created in the database. Please create a new field instead."
                    ]
                },
            )
    now = datetime.now(UTC).isoformat()
    updated = {**old_field, **{k: v for k, v in updates.items() if v is not None}, "updated_at": now}
    other_names = [cf["name"] for cf in existing if cf["name"] != field_name]
    new_custom = existing[:idx] + [updated] + existing[idx + 1 :]
    return _validate_and_save_custom_field(service_id, cfg, lf, updated, other_names, new_custom, now)


@core_router.delete("/services/{service_id}/custom-fields/{field_name}", response_model=CustomFieldResponse)
def api_delete_custom_field(request: Request, service_id: str, field_name: str):
    cfg, lf, existing = _load_modifiable_custom_fields(request, service_id, field_name, "deleted")
    field = next((cf for cf in existing if cf["name"] == field_name), None)
    if field is None:
        raise HTTPException(status_code=404, detail={"error": f"Custom field '{field_name}' not found"})
    new_custom = [cf for cf in existing if cf["name"] != field_name]
    now = datetime.now(UTC).isoformat()
    _persist_custom_fields_change(service_id, cfg, lf, new_custom, {**lf, "custom_fields": new_custom}, now)
    return {"ok": True, "field": field}


@core_router.post("/services/{service_id}/custom-fields/validate-vcl", response_model=VclLintResponse)
def api_validate_custom_vcl(request: Request, service_id: str, body: VclLintRequest):
    from backend import provision

    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    lf = lf_module.get_lf_config(cfg)
    candidate = {
        "name": "lint_check",
        "label": "Lint Check",
        "vcl_log_expression": body.vcl_log_expression,
        "collection_stage": body.collection_stage,
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
        "enabled": True,
    }
    errors, warnings = _split_custom_field_warnings(lf_module.validate_custom_field(candidate, []), strip_prefix=True)
    base_lf = body.log_fields_config if body.log_fields_config else lf
    candidate_lf = {**base_lf, "custom_fields": lf.get("custom_fields", []) + [candidate]}
    fmt_hard_errors, fmt_warnings = _split_custom_field_warnings(provision.validate_log_format(candidate_lf))
    errors.extend(fmt_hard_errors)
    warnings.extend(fmt_warnings)
    fmt = provision.load_log_format(candidate_lf) if not errors else None
    return VclLintResponse(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        format_length=len(fmt) if fmt else None,
        format_length_limit=provision.fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX,
    )


@core_router.get(
    "/services/{service_id}/custom-fields/export",
    response_class=StreamingResponse,
    responses={
        200: {
            "model": CustomFieldsExportFile,
            "description": "JSON file download (Content-Disposition: attachment) holding the user-defined custom fields.",
        }
    },
)
def api_export_custom_fields(request: Request, service_id: str):
    require_service_in_scope(request, service_id)
    lf = lf_module.get_lf_config(load_service_config(service_id))
    user_fields = _filter_user_custom_fields(lf.get("custom_fields", []))
    return StreamingResponse(
        iter([json.dumps({"custom_fields": user_fields})]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=custom_fields_{service_id}.json"},
    )


@core_router.post(
    "/services/{service_id}/custom-fields/import",
    response_model=CustomFieldsImportResponse,
    response_model_exclude_unset=True,
)
def api_import_custom_fields(request: Request, service_id: str, body: CustomFieldsImportBody):
    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    fields_to_import = body.custom_fields
    lf = lf_module.get_lf_config(cfg)
    existing_map = {cf["name"]: cf for cf in lf.get("custom_fields", [])}
    locked_field_names = _locked_storage_field_names(
        _db.get_source_for_service(service_id),
        error_lead="Could not verify committed fields for import",
        error_tail="no changes were applied. Please retry.",
    )
    new_custom_map = {**existing_map}
    now = datetime.now(UTC).isoformat()
    type_lock_errors: list[str] = []
    validation_errors: list[str] = []
    for field_dict in fields_to_import:
        if "name" not in field_dict:
            continue
        fname = field_dict["name"]
        if _is_system_field(fname):
            validation_errors.append(f"{fname}: cannot import system-managed field")
            continue
        existing_field = existing_map.get(fname, {})
        if fname in locked_field_names:
            for tk in ("duckdb_type", "value_type"):
                if field_dict.get(tk) and field_dict[tk] != existing_field.get(tk):
                    type_lock_errors.append(
                        f"Cannot change '{tk}' of '{fname}': field is already committed to the database."
                    )
                    break
            else:
                pass
            if any(fname in err for err in type_lock_errors):
                continue
        other_names = [n for n in new_custom_map if n != fname]
        for err in lf_module.validate_custom_field(field_dict, other_names):
            if not err.startswith("WARN:"):
                validation_errors.append(f"{fname}: {err}")
        field_dict.pop("created_at", None)
        field_dict.pop("updated_at", None)
        field_dict["created_at"] = existing_field.get("created_at", now)
        field_dict["updated_at"] = now
        new_custom_map[fname] = field_dict
    if type_lock_errors:
        raise HTTPException(status_code=422, detail={"errors": type_lock_errors})
    if validation_errors:
        raise HTTPException(status_code=422, detail={"errors": validation_errors})
    new_custom = list(new_custom_map.values())
    candidate_lf = {**lf, "custom_fields": new_custom}
    _validate_log_format_version(candidate_lf)
    _persist_custom_fields_change(service_id, cfg, lf, new_custom, candidate_lf, now, full=False)
    return {"ok": True, "imported_count": len(fields_to_import)}


# ── Audit logs & Cron runs (folded from audit.py and cron.py) ─────────────────


@audit_router.get("", response_model=AuditLogsResponse, response_model_exclude_unset=True)
@query_errors(500, code="audit_logs_read_failed", logger_obj=logger)
def api_audit_logs(
    source: dict = Depends(get_source),
    event_type: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, le=1000),
    sort: str = Query(default="timestamp"),
    dir: str = Query(default="DESC"),
):
    from backend.utils.telemetry import debug_envelope

    total, entries = metadata_db.get_audit_logs(
        source["name"], event_type=event_type, page=page, per_page=per_page, sort_col=sort, sort_dir=dir
    )
    return debug_envelope(
        {"total": total, "page": page, "per_page": per_page, "entries": entries},
        gated=True,
        is_cached=None,
        inject_calls=True,
    )


@cron_router.get("", response_model=CronRunsResponse, response_model_exclude_unset=True)
@query_errors(500, code="cron_logs_read_failed", logger_obj=logger)
def api_cron_logs(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, le=1000),
    sort: str = Query(default="started_at"),
    dir: str = Query(default="DESC"),
    since_id: int | None = Query(default=None, ge=0),
    skip_total: bool = Query(default=False),
):
    total, entries = get_cron_logs(
        source["name"], task, status, page, per_page, sort, dir, since_id=since_id, skip_total=skip_total
    )
    return {"total": total, "page": page, "per_page": per_page, "entries": entries}


@cron_router.delete("/{log_id}", status_code=204)
@query_errors(500, code="cron_log_delete_failed", logger_obj=logger)
def api_cron_log_delete(log_id: int, source: dict = Depends(get_source)):
    delete_cron_log(source["name"], log_id)
    return Response(status_code=204)


@cron_router.delete("", status_code=204)
@query_errors(500, code="cron_logs_purge_failed", logger_obj=logger)
def api_cron_logs_purge(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    days: int | None = Query(default=None),
):
    purge_cron_logs(source["name"], task, days)
    return Response(status_code=204)


_CacheRegistry.register("services.core._cron_schedule_cache", _cron_schedule_cache)
_CacheRegistry.register("services.core._logging_settings_cache", _logging_settings_cache)


import sys as _sys

router = APIRouter()
router.include_router(core_router)
router.include_router(audit_router)
router.include_router(cron_router)


# --- Merged from backend/routers/provision.py ---


import datetime as _dt
import io
import logging
import secrets
import shutil
import urllib.error
import urllib.request
import zipfile
from typing import Any

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import StreamingResponse

from backend.core.fastly.rum_provisioning import validate_rum_custom_condition
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin import ProvisionService
from backend.models.common import (
    CheckFosRequest,
    NgwafWorkspaceCreateRequest,
    NgwafWorkspaceSetResponse,
    NgwafWorkspacesResponse,
    ProvisionCheckConfigResponse,
    ProvisionCheckDomainResponse,
    ProvisionCheckFosResponse,
    ProvisionConfigRequest,
    ProvisionExecuteRequest,
    ProvisionIngestResponse,
    ProvisionReconcileRequest,
    ProvisionTeardownRequest,
    ProvisionValidateRequest,
    ProvisionValidateResponse,
    TerraformPreviewResponse,
)
from backend.provision.orchestrator import _get_fos_s3_client
from backend.utils.router_utils import require_json_content_type, require_service_id

logger = logging.getLogger(__name__)
provision_router = APIRouter(prefix="/api/provision", tags=["provision"], responses=DEFAULT_ERROR_RESPONSES)

_FASTLY_HTTP_ERROR_RE = re.compile(r"HTTP (\d+) ")


def _invalidate_service_credentials(service_id: str) -> None:
    """Drop in-process FOS client + pooled DuckDB S3 SECRET caches for ``service_id``."""
    try:
        from backend.core import duckdb as _ddb
        from backend.core import duckdb_pool as _ddb_pool

        _ddb.clear_fos_client(service_id)
        _ddb_pool.reset_pool_for_service(service_id)
    except Exception:
        logger.warning("[provision] credential cache invalidation failed for %s", service_id, exc_info=True)


def _check_domain_available(domain: str, timeout: int = 10) -> tuple[bool, str | None]:
    from backend.utils.telemetry import tracked_call

    with tracked_call("GET", domain, service="Domain Check"):
        try:
            req = urllib.request.Request(f"https://{domain}", method="GET")
            try:
                with urllib.request.urlopen(req, timeout=timeout):
                    return False, "Domain already in use (returned 200)"
            except urllib.error.HTTPError as exc:
                body = exc.read().decode(errors="replace")
                if "Please check that this domain has been added to a service." in body:
                    return True, None
                return False, "Domain already registered or in use"
            except urllib.error.URLError as exc:
                return True, f"DNS/Connection error (likely available): {exc.reason}"
        except Exception as exc:
            return False, str(exc)


@provision_router.get("/services", response_model=list[ProvisionService])
def provision_list_services(token: str = Query(...)):
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly
    from backend.provision.fos_setup import object_storage_enabled

    existing_ids = set(svcconfig.list_service_ids())
    try:
        services = fastly("GET", "/service", token=token)
    except Exception as e:
        raise_internal(logger, e, code="list_services_failed", status=400)

    if not object_storage_enabled(token):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "object_storage_not_enabled",
                "Object Storage isn't enabled on this Fastly account, and it's "
                "required to store logs. Enable the Object Storage product for your "
                "account (in the Fastly control panel, or ask your Fastly account "
                "team), then click Fetch Services again.",
            ),
        )

    return [
        {"id": s["id"], "name": s["name"], "provisioned": s["id"] in existing_ids}
        for s in services
        if s.get("type", "vcl") == "vcl"
    ]


@provision_router.post("/validate", response_model=ProvisionValidateResponse, response_model_exclude_unset=True)
def provision_validate(body: ProvisionValidateRequest):
    from backend.core.fastly.client import fastly
    from backend.utils.telemetry import debug_envelope

    token, service_id = body.token, body.service_id
    if not token or not service_id:
        raise HTTPException(status_code=400, detail={"error": "Token and Service ID are required"})

    try:
        token_info = {}
        try:
            token_data = fastly("GET", "/tokens/self", token=token)
            token_info = {
                "id": token_data.get("id"),
                "name": token_data.get("name"),
                "user_id": token_data.get("user_id"),
                "type": "user" if token_data.get("user_id") else "automation",
            }
        except Exception as e:
            logger.warning("[provision-validate] Failed to fetch token info: %s", e)

        svc = fastly("GET", f"/service/{service_id}", token=token)
        safe_service_id = re.sub(r"-+", "-", re.sub(r"[^a-zA-Z0-9]", "-", service_id)).strip("-")
        return debug_envelope(
            {
                "service_name": svc.get("name", service_id),
                "token_info": token_info,
                "defaults": {
                    "endpoint_name": "Fastly Object Storage Logs",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": f"fos-{safe_service_id.lower()}-logs",
                    "fos_prefix": "",
                    "sample_rate": 100,
                    "edge_only": True,
                    "log_period": "1 minute",
                    "cdn_service_name": f"Log Analysis CDN Service for {service_id}",
                    "cdn_prefix": f"fos-{safe_service_id}-logs",
                },
            },
            gated=True,
            is_cached=None,
            inject_calls=True,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(logger, e, code="provision_validate_failed", status=400)


@provision_router.get("/check-domain", response_model=ProvisionCheckDomainResponse, response_model_exclude_unset=True)
def provision_check_domain(prefix: str = Query(...), is_custom: bool = Query(False)):
    if not prefix:
        raise HTTPException(status_code=400, detail={"error": "Prefix is required"})
    if is_custom:
        domain = prefix.strip()
        if not domain or "." not in domain or not re.fullmatch(r"^[a-zA-Z0-9.-]+$", domain):
            return {
                "available": False,
                "reason": "Invalid custom domain. Must be non-empty, contain at least one dot, and consist only of letters, digits, hyphens, and dots.",
            }
    else:
        if not re.fullmatch(r"(?i)^([a-z0-9][a-z0-9-]*[a-z0-9]|[a-z0-9])$", prefix):
            return {
                "available": False,
                "reason": "Prefix must be alphanumeric and may contain hyphens (cannot start/end with hyphen)",
            }
        domain = f"{prefix}.global.ssl.fastly.net"

    from backend.utils.telemetry import debug_envelope

    available, reason = _check_domain_available(domain)
    result: dict[str, Any] = {"available": available}
    if reason:
        result["reason" if not available else "note"] = reason
    return debug_envelope(result, gated=True, is_cached=None, inject_calls=True)


@provision_router.post("/check-fos", response_model=ProvisionCheckFosResponse, response_model_exclude_unset=True)
def provision_check_fos(req: CheckFosRequest):
    """Validate FOS credentials against the same raw prefixes ingest reads."""
    import botocore.exceptions

    from backend.utils.telemetry import debug_envelope

    bucket, region, endpoint = req.bucket, req.region, f"{req.region}.object.fastlystorage.app"
    try:
        client = _get_fos_s3_client(
            req.access_key,
            req.secret_key,
            region,
            bucket_name=bucket,
            context="provision:check-fos",
        )
        representative: tuple[str, str] | None = None
        for prefix in ("raw/request/", "raw/rum/"):
            page = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            contents = page.get("Contents") or []
            if contents:
                key = str(contents[0]["Key"])
                client.get_object(Bucket=bucket, Key=key, Range="bytes=0-0")["Body"].close()
                representative = (prefix, key)
                break
        prefix, key = representative or ("raw/request/", "")
        logger.info(
            "[provision] FOS credential probe succeeded bucket=%s region=%s endpoint=%s prefix=%s key=%s",
            bucket,
            region,
            endpoint,
            prefix,
            key or "<none>",
        )
        return debug_envelope({"ok": True}, gated=True, is_cached=None, inject_calls=True)
    except Exception as e:
        err_msg = str(e)
        logger.warning(
            "[provision] FOS credential probe failed bucket=%s region=%s endpoint=%s: %s",
            bucket,
            region,
            endpoint,
            err_msg[:200],
        )
        if isinstance(e, botocore.exceptions.ClientError):
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                err_msg = "Access Denied. Please check your Access Key and Secret Key."
            elif code == "NoSuchBucket":
                err_msg = "Bucket not found. Please check the Bucket Name."
            elif code == "IllegalLocationConstraintException":
                err_msg = "Region mismatch. Please check the FOS Region."
        if "EndpointConnectionError" in err_msg:
            err_msg = "Connection failed. Please verify the FOS Region is correct."
        return debug_envelope({"ok": False, "error": err_msg}, gated=True, is_cached=None, inject_calls=True)


@provision_router.post("/teardown", dependencies=[Depends(require_json_content_type)])
def provision_teardown(req: Request, body: ProvisionTeardownRequest | None = None):
    """Destructive service teardown over SSE."""
    body = body or ProvisionTeardownRequest()
    token, service_id = body.token, body.service_id
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.provision import _sync_crontab, perform_teardown
    from backend.utils.fastly_auth import validate_destructive_token

    state = None
    svc_cfg = svcconfig.load_config(service_id) if service_id else None
    if service_id and svc_cfg:
        prov = svc_cfg.get("provisioning", {})
        state = {
            "logging_service_id": service_id,
            "fos_bucket_name": svc_cfg.get("fos_bucket", ""),
            "fos_region": svc_cfg.get("fos_region", "us-east-1"),
            "fos_prefix": svc_cfg.get("fos_prefix", ""),
            "fos_access_key": svc_cfg.get("fos_access_key_id", ""),
            "fos_secret_key": svc_cfg.get("fos_secret_access_key", ""),
            "endpoint_name": prov.get("endpoint_name", "Fastly Object Storage Logs"),
            "cdn_service_id": prov.get("cdn_service_id", ""),
            "cdn_service_name": prov.get("cdn_service_name") or f"Log Analysis CDN Service for {service_id}",
            "cdn_url": prov.get("cdn_url", ""),
            "cdn_secret": svc_cfg.get("cdn_secret", ""),
            "scoring": svc_cfg.get("scoring") or {},
        }

    if not state:
        raise HTTPException(status_code=404, detail={"error": "No service config found."})

    validate_destructive_token(token, service_id=service_id or "")
    opts = {
        "remove_logging": body.remove_logging,
        "remove_rum": body.remove_rum,
        "remove_cdn": body.remove_cdn,
        "remove_bucket": body.remove_bucket,
        "remove_cloud_files": body.remove_cloud_files,
        "remove_scoring": body.remove_scoring,
        "remove_fos_tokens": body.remove_fos_tokens,
    }

    def stream():
        sid = state.get("logging_service_id") or service_id
        try:
            if sid:
                yield _sse("status", f"Stopping background sync for service {sid}...")
                cfg_path = svcconfig.config_path(sid)
                if os.path.exists(cfg_path):
                    os.remove(cfg_path)
                    from backend.utils.bounded_cache import CacheRegistry

                    CacheRegistry.clear("routers.bootstrap._bootstrap_cache")
                    yield _sse("status", f"Removed configs/{sid}.json")

                try:
                    _sync_crontab()
                    from backend.cron.scheduler import get_scheduler

                    get_scheduler().reload()
                    if body.remove_cron:
                        yield _sse("status", "Cron jobs updated")
                except Exception as e:
                    if body.remove_cron:
                        yield _sse("status", f"Warning: Failed to update cron jobs: {e}")

                try:
                    from backend.core import storage as db_storage

                    _cache_src = svcconfig.config_to_source(svc_cfg) if svc_cfg else {"name": sid}
                    db_storage.invalidate_service_caches(_cache_src)
                except Exception:
                    logger.debug("stream: db_storage.invalidate_service_caches failed (ignored)", exc_info=True)

                _invalidate_service_credentials(sid)

            yield _sse("status", "Starting teardown of Fastly resources...")
            for event in perform_teardown(state, token, opts=opts):
                yield json.dumps(event)

            if body.remove_cache:
                db_path = svcconfig.duckdb_path(sid) if sid else _db.DUCKDB_PATH
                for f in [db_path, db_path + ".wal"]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                            _db.clear_initialization_state(f)
                        except Exception as e:
                            yield _sse("status", f"Warning: could not remove {os.path.basename(f)}: {e}")

                src_mock = {"bucket": state.get("fos_bucket_name", ""), "prefix": state.get("fos_prefix", "")}
                if src_mock["bucket"]:
                    svc_cache_dir = _db._cache_dir(src_mock)
                    if os.path.exists(svc_cache_dir):
                        rmtree_err = None
                        for _attempt in range(6):
                            try:
                                shutil.rmtree(svc_cache_dir)
                                rmtree_err = None
                                break
                            except FileNotFoundError:
                                rmtree_err = None
                                break
                            except OSError as e:
                                rmtree_err = e
                                time.sleep(0.5)
                        if rmtree_err is not None:
                            yield _sse("status", f"Warning: could not remove cache dir: {rmtree_err}")

                if sid:
                    try:
                        from backend.core import metadata as metadata_db

                        metadata_db.teardown(sid)
                    except Exception as e:
                        yield _sse("status", f"Warning: could not clear metadata: {e}")

                yield _sse("status", "Removed local database and cache")

            if sid and not body.remove_cache:
                try:
                    from backend.core import metadata as metadata_db

                    metadata_db.record_audit(
                        service_id=sid,
                        event_type="teardown",
                        details={
                            "removed_logging": body.remove_logging,
                            "removed_cdn": body.remove_cdn,
                            "removed_bucket": body.remove_bucket,
                        },
                    )
                except Exception:
                    logger.warning("stream: metadata_db.record_audit failed (ignored)", exc_info=True)

            if sid:
                logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "logs", sid))
                if os.path.exists(logs_dir):
                    try:
                        shutil.rmtree(logs_dir)
                        yield _sse("status", "Removed logs directory")
                    except Exception as e:
                        yield _sse("status", f"Warning: could not remove logs directory: {e}")

            _db.reload_default_source()
            yield _sse("done", "Teardown complete.")
        except Exception as e:
            yield _sse("error", str(e))

    return sse_response(stream())


@provision_router.post("/execute")
def provision_execute(req: ProvisionExecuteRequest):
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.provision import parse_period
    from backend.utils.pop_utils import fetch_pop_locations

    fetch_pop_locations(req.token)
    service_name = req.service_name or svcconfig.fetch_service_name(req.service_id, req.token) or req.service_id

    cfg: dict[str, Any] = {
        "admin_token": req.token,
        "logging_service_id": req.service_id,
        "name": service_name,
        "endpoint_name": req.endpoint_name,
        "fos_region": req.fos_region,
        "fos_bucket_name": req.fos_bucket_name,
        "fos_prefix": req.fos_prefix,
        "sample_rate": req.sample_rate,
        "edge_only": req.edge_only,
        "custom_condition": req.custom_condition,
        "log_period": req.log_period,
        "cdn_service_name": req.cdn_service_name,
        "cdn_url": req.cdn_url,
        "cdn_shield": req.cdn_shield,
        "enable_cron_sync": req.enable_cron_sync,
        "delete_after": req.delete_after,
        "commit_interval_mins": req.commit_interval_mins,
        "enable_cron_compact": req.enable_cron_compact,
        "log_retention_days": req.log_retention_days,
        "rum_retention_days": req.rum_retention_days,
        "logging_enabled": req.logging_enabled,
        "rum_enabled": req.rum_enabled,
    }

    if req.log_fields:
        try:
            cfg["log_fields"] = json.loads(req.log_fields)
        except Exception:
            logger.debug("provision_execute: step failed (ignored)", exc_info=True)

    if req.cmcd_enabled:
        cfg["cmcd"] = {
            "enabled": True,
            "mode": req.cmcd_mode or "query_string",
            "version": req.cmcd_version if req.cmcd_version is not None else 1,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
        }

    if getattr(req, "rum_enabled", False):
        from backend.core.faro_versions import DEFAULT_FARO_VERSION

        rum_cond = (getattr(req, "rum_custom_condition", None) or "").strip()
        cond_err = validate_rum_custom_condition(rum_cond)
        if cond_err:
            raise HTTPException(status_code=400, detail=make_error("invalid_rum_condition", cond_err))
        cfg["rum"] = {
            "enabled": True,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            "faro_version": req.faro_version or DEFAULT_FARO_VERSION,
            "custom_condition": rum_cond,
        }
        if getattr(req, "rum_log_period", None) is not None:
            try:
                rum_period_val = parse_period(str(req.rum_log_period))
                cfg["rum"]["log_period"] = rum_period_val
                cfg["rum_log_period"] = rum_period_val
            except ValueError as e:
                raise HTTPException(status_code=400, detail=make_error("invalid_rum_log_period", str(e)))

    try:
        cfg["log_period"] = parse_period(cfg["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))

    bucket = cfg["fos_bucket_name"]
    if not re.fullmatch(r"^[A-Za-z0-9](?!.*--)[A-Za-z0-9-]{1,61}[A-Za-z0-9]$", bucket):
        raise HTTPException(
            status_code=400,
            detail=make_error(
                "invalid_bucket",
                f"Invalid bucket name: '{bucket}'. Use 3-63 alphanumeric characters or single hyphens.",
            ),
        )

    prefix = cfg.get("fos_prefix", "")
    if prefix and not re.fullmatch(r"^[A-Za-z0-9/_-]*$", prefix):
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_prefix", "Invalid prefix. Use alphanumerics, /, _, -."),
        )

    if cfg.get("cdn_url"):
        domain = cfg["cdn_url"].replace("https://", "")
        available, reason = _check_domain_available(domain, timeout=5)
        if not available and reason and "DNS" not in reason:
            raise HTTPException(
                status_code=400,
                detail=make_error("domain_unavailable", reason, domain=domain),
            )

    cfg["cdn_secret"] = secrets.token_urlsafe(24)

    def stream():
        error_already_emitted = False
        try:
            from backend.provision import provision

            for event in provision(cfg):
                if event.get("type") == "error":
                    error_already_emitted = True
                yield json.dumps(event)

                if event.get("type") == "done":
                    _db.reload_default_source()
                    from backend.utils.bounded_cache import CacheRegistry

                    CacheRegistry.clear("routers.bootstrap._bootstrap_cache")
                    _invalidate_service_credentials(cfg.get("logging_service_id") or cfg.get("service_id") or "")

                    try:
                        from backend.core import metadata as metadata_db

                        metadata_db.record_audit(
                            service_id=cfg["logging_service_id"],
                            event_type="provision",
                            details={
                                "bucket": cfg.get("fos_bucket_name"),
                                "prefix": cfg.get("fos_prefix"),
                                "region": cfg.get("fos_region"),
                                "sample_rate": cfg.get("sample_rate"),
                                "cdn_url": cfg.get("cdn_url"),
                                "cdn_shield": cfg.get("cdn_shield"),
                                "edge_only": cfg.get("edge_only"),
                                "log_period": cfg.get("log_period"),
                                "enable_cron_sync": cfg.get("enable_cron_sync"),
                                "delete_after": cfg.get("delete_after"),
                                "commit_interval_mins": cfg.get("commit_interval_mins"),
                                "enable_cron_compact": cfg.get("enable_cron_compact"),
                                "log_retention_days": cfg.get("log_retention_days"),
                                "log_fields": cfg.get("log_fields"),
                            },
                        )
                    except Exception:
                        logger.warning("stream: metadata_db.record_audit failed (ignored)", exc_info=True)

                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()
                    except Exception:
                        logger.debug("stream: step failed (ignored)", exc_info=True)
        except Exception as e:
            if not error_already_emitted:
                yield _sse("error", str(e))

    return sse_response(stream())


@provision_router.post("/reconcile")
def provision_reconcile(body: ProvisionReconcileRequest):
    """Reconcile VCL state for a service and stream status via SSE."""
    if not body.service_id or not body.token:
        raise HTTPException(status_code=400, detail=make_error("service_id and token are required"))

    from backend.provision.declarative.reconciler import reconcile_vcl_state
    from backend.utils.sse import stream_sse_from_worker

    def worker(events):
        try:
            result = reconcile_vcl_state(
                service_id=body.service_id,
                token=body.token,
                dry_run=False,
                status_cb=lambda msg: events.put({"type": "status", "message": msg}),
            )
            events.put(
                {
                    "type": "reconciliation_result",
                    "service_id": result.service_id,
                    "activated_version": result.activated_version,
                    "changes_applied": result.changes_applied,
                    "duration_ms": result.duration_ms,
                    "error": result.error,
                }
            )
            events.put({"type": "done", "success": result.error is None})
        except Exception as e:
            events.put({"type": "error", "message": str(e)})

    return stream_sse_from_worker(worker)


def _render_terraform(body: ProvisionConfigRequest) -> dict[str, str]:
    from backend.provision.orchestrator import generate_terraform

    cfg = body.model_dump(exclude_none=True)
    access_key = cfg.get("fos_access_key") or "YOUR_FOS_ACCESS_KEY"
    secret_key = cfg.get("fos_secret_key") or "YOUR_FOS_SECRET_KEY"
    return generate_terraform(cfg, access_key, secret_key)


@provision_router.post("/terraform/preview", response_model=TerraformPreviewResponse)
def provision_terraform_preview(body: ProvisionConfigRequest):
    return _render_terraform(body)


@provision_router.post(
    "/terraform/export",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "ZIP archive of the generated Terraform files (Content-Disposition: attachment).",
            "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}},
        }
    },
)
def provision_terraform_export(body: ProvisionConfigRequest):
    tf_files = _render_terraform(body)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, file_content in tf_files.items():
            zip_file.writestr("README.md" if file_name == "instructions" else file_name, file_content)
    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="fastly-log-analysis-terraform.zip"'},
    )


@provision_router.post("/ingest", response_model=ProvisionIngestResponse, response_model_exclude_unset=True)
def provision_ingest(payload: ProvisionConfigRequest):
    from backend import config as svcconfig
    from backend.provision import ensure_fos_access_key, find_fos_key, parse_period, write_service_config
    from backend.utils.fastly_auth import validate_destructive_token
    from backend.utils.pop_utils import fetch_pop_locations

    body: dict[str, Any] = payload.model_dump(exclude_none=False)
    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail={"error": "Token is required"})

    logging_service_id = body.get("service_id") or body.get("logging_service_id") or ""
    validate_destructive_token(token, service_id=logging_service_id)
    fetch_pop_locations(token)

    existing_cfg = svcconfig.load_config(logging_service_id) or {}
    existing_fos_proxy = existing_cfg.get("fos_proxy")
    if not isinstance(existing_fos_proxy, dict):
        existing_fos_proxy = {}

    try:
        if body.get("log_period"):
            body["log_period"] = parse_period(body["log_period"])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_log_period", str(e)))

    fos_access_key = body.get("fos_access_key")
    fos_secret_key = body.get("fos_secret_key")
    fos_key_id = ""

    if not fos_access_key or not fos_secret_key:
        desc = f"fos-log-analysis-{body.get('fos_bucket_name')}"
        existing = find_fos_key(desc, token)
        existing_secret = existing.get("secret_key") if existing else None
        if existing and existing_secret:
            fos_access_key = existing["access_key"]
            fos_secret_key = existing_secret
            fos_key_id = existing["access_key"]
        elif existing:
            raise HTTPException(
                status_code=409,
                detail=make_error(
                    "fos_key_secret_unavailable",
                    f"An access key '{desc}' already exists but its secret is not retrievable; "
                    "provide fos_access_key + fos_secret_key explicitly.",
                ),
            )
        else:
            try:
                bucket_name = body.get("fos_bucket_name")
                if not bucket_name:
                    raise HTTPException(status_code=400, detail={"error": "fos_bucket_name required"})
                new_key = ensure_fos_access_key(desc, body, token, buckets=[bucket_name])
                fos_access_key = new_key["access_key"]
                fos_secret_key = new_key["secret_key"]
                fos_key_id = new_key["id"]
            except Exception as e:
                raise_internal(logger, e, code="ensure_access_key_failed", status=400)

    if not body.get("cdn_secret"):
        body["cdn_secret"] = (
            existing_cfg.get("cdn_secret") or existing_fos_proxy.get("secret") or secrets.token_urlsafe(24)
        )

    state = {
        "admin_token": token,
        "logging_service_id": body.get("service_id") or body.get("logging_service_id"),
        "name": body.get("service_name") or body.get("service_id"),
        "endpoint_name": body.get("endpoint_name", "Fastly Object Storage Logs"),
        "fos_region": body.get("fos_region", "us-east-1"),
        "fos_bucket_name": body.get("fos_bucket_name"),
        "fos_prefix": body.get("fos_prefix", ""),
        "fos_access_key_id": fos_access_key,
        "fos_secret_access_key": fos_secret_key,
        "sample_rate": body.get("sample_rate", "100"),
        "edge_only": body.get("edge_only", True),
        "custom_condition": body.get("custom_condition", ""),
        "log_period": body.get("log_period", 60),
        "cdn_service_name": body.get("cdn_service_name"),
        "cdn_url": body.get("cdn_url"),
        "cdn_shield": body.get("cdn_shield", "none"),
        "cdn_secret": body.get("cdn_secret"),
        "enable_cron_sync": body.get("enable_cron_sync", True),
        "delete_after": body.get("delete_after", True),
        "commit_interval_mins": body.get("commit_interval_mins", 5),
        "enable_cron_compact": body.get("enable_cron_compact", True),
        "log_retention_days": body.get("log_retention_days", 30),
        "rum_retention_days": body.get("rum_retention_days", 30),
        "provisioning": {"fos_key_id": fos_key_id},
    }

    log_fields_raw = body.get("log_fields")
    if log_fields_raw:
        try:
            state["log_fields"] = json.loads(log_fields_raw) if isinstance(log_fields_raw, str) else log_fields_raw
        except Exception:
            logger.debug("provision_ingest: step failed (ignored)", exc_info=True)

    now_iso = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
    if any(k in body for k in ("cmcd_enabled", "cmcd_mode", "cmcd_version")) or isinstance(body.get("cmcd"), dict):
        existing_cmcd = body.get("cmcd", {}) if isinstance(body.get("cmcd"), dict) else {}
        state["cmcd"] = {
            "enabled": body.get("cmcd_enabled", existing_cmcd.get("enabled", False)),
            "mode": body.get("cmcd_mode") or existing_cmcd.get("mode", "query_string"),
            "version": body.get("cmcd_version")
            if body.get("cmcd_version") is not None
            else existing_cmcd.get("version", 1),
            "enabled_at": existing_cmcd.get("enabled_at") or now_iso,
        }

    scoring_keys = (
        "scoring_enabled",
        "scoring_domain",
        "scoring_request_secret",
        "scoring_exclude_url_regex",
        "scoring_enforce_status_code",
    )
    if any(k in body for k in scoring_keys) or isinstance(body.get("scoring"), dict):
        existing_scoring = body.get("scoring", {}) if isinstance(body.get("scoring"), dict) else {}
        state["scoring"] = {
            "enabled": body.get("scoring_enabled", existing_scoring.get("enabled", False)),
            "domain": body.get("scoring_domain") or existing_scoring.get("domain", ""),
            "request_secret": body.get("scoring_request_secret", existing_scoring.get("request_secret", "")),
            "exclude_url_regex": body.get("scoring_exclude_url_regex", existing_scoring.get("exclude_url_regex", "")),
            "enforce_status_code": body.get(
                "scoring_enforce_status_code", existing_scoring.get("enforce_status_code", 429)
            ),
            "enabled_at": existing_scoring.get("enabled_at") or now_iso,
        }

    if body.get("rum_enabled"):
        existing_rum = body.get("rum", {}) if isinstance(body.get("rum"), dict) else {}
        state["rum"] = {
            "enabled": True,
            "enabled_at": existing_rum.get("enabled_at") or now_iso,
            "custom_condition": body.get("rum_custom_condition") or existing_rum.get("custom_condition") or "",
        }
        on_disk_rum = existing_cfg.get("rum")
        faro_src: dict[str, Any] = (
            existing_rum if existing_rum.get("faro_version") else (on_disk_rum if isinstance(on_disk_rum, dict) else {})
        )
        if faro_src.get("faro_version"):
            for k in ("faro_version", "faro_content_hash", "faro_fos_etag_md5", "faro_last_upstream_check"):
                if k in faro_src:
                    state["rum"][k] = faro_src[k]

    write_service_config(state)

    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
        logger.info("[provision_ingest] Successfully reloaded scheduler with new intervals! 🔄")
    except Exception as e:
        logger.warning("[provision_ingest] Failed to reload scheduler after ingest: %s", e)

    _invalidate_service_credentials(state.get("logging_service_id") or "")

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        logger.warning("provision_ingest: _sync_crontab failed (ignored)", exc_info=True)

    return {"ok": True, "service_id": state["logging_service_id"]}


@provision_router.get("/check-config", response_model=ProvisionCheckConfigResponse, response_model_exclude_unset=True)
def provision_check_config(
    token: str = Query(...),
    service_id: str | None = Query(default=None),
    cdn_service_id: str = Query(...),
    bucket: str = Query(...),
):
    """Verify that both services are correctly configured for log analysis."""
    service_id = require_service_id(service_id)
    from backend.core.fastly.client import fastly

    results = {
        "logging_service": {"ok": False, "details": ""},
        "cdn_service": {"ok": False, "details": ""},
    }

    try:
        try:
            active_ver = fastly("GET", f"/service/{service_id}/version/active", token=token).get("number")
            endpoints = fastly("GET", f"/service/{service_id}/version/{active_ver}/logging/s3", token=token)
            endpoint = next((ep for ep in endpoints if ep.get("bucket_name") == bucket), None)

            if not endpoint:
                results["logging_service"]["details"] = f"No S3 logging endpoint found pointing to bucket '{bucket}'"
            else:
                snippets = fastly("GET", f"/service/{service_id}/version/{active_ver}/snippet", token=token)
                snip_names = {s.get("name") for s in snippets}
                required_snippets = {
                    "Fastly Log Analysis Capture",
                    "Fastly Log Analysis Miss",
                    "Fastly Log Analysis Pass",
                }
                optional_snippets = {
                    "Fastly Log Analysis Origin Fetch",
                    "Fastly Log Analysis Origin Error",
                    "Fastly Log Analysis Origin Deliver",
                }
                missing_required = required_snippets - snip_names
                found_optional = optional_snippets & snip_names

                if missing_required:
                    results["logging_service"]["details"] = (
                        f"Found endpoint '{endpoint.get('name')}', but missing CORE snippets: {', '.join(missing_required)}"
                    )
                else:
                    msg = "Service has endpoint and core snippets"
                    if found_optional:
                        msg += f" (plus {len(found_optional)} origin metric snippets)"
                    if endpoint.get("response_condition") is None:
                        results["logging_service"] = {
                            "ok": True,
                            "details": f"Warning: {msg}, but no response condition (sampling might be disabled)",
                        }
                    else:
                        results["logging_service"] = {"ok": True, "details": f"{msg} and active sampling condition"}
        except Exception as e:
            results["logging_service"]["details"] = f"Error checking logging service: {e}"

        try:
            dicts = fastly("GET", f"/service/{cdn_service_id}/version/active/dictionary", token=token)
            missing_dicts = {"fos_credentials", "cdn_auth"} - {d.get("name") for d in dicts}

            if missing_dicts:
                results["cdn_service"]["details"] = f"Missing required dictionaries: {', '.join(missing_dicts)}"
            else:
                backends = fastly("GET", f"/service/{cdn_service_id}/version/active/backend", token=token)
                if not any(".fastlystorage.app" in b.get("address", "") for b in backends):
                    results["cdn_service"]["details"] = (
                        "No backend found pointing to Fastly Object Storage (*.fastlystorage.app)"
                    )
                else:
                    active_ver_cdn = fastly("GET", f"/service/{cdn_service_id}/version/active", token=token).get(
                        "number"
                    )
                    snippets_cdn = fastly(
                        "GET", f"/service/{cdn_service_id}/version/{active_ver_cdn}/snippet", token=token
                    )
                    required_cdn = {
                        "cdn-swr-shield-disable",
                        "cdn-race-condition-generation",
                        "cdn-no-cache-404",
                    }
                    missing_cdn = required_cdn - {s.get("name") for s in snippets_cdn}
                    if missing_cdn:
                        results["cdn_service"]["details"] = (
                            f"Dictionaries/Backends OK, but missing snippets: {', '.join(missing_cdn)}"
                        )
                    else:
                        results["cdn_service"] = {
                            "ok": True,
                            "details": "Service fully configured with dictionaries, backends, and performance snippets",
                        }
        except Exception as e:
            results["cdn_service"]["details"] = f"Error checking CDN service: {e}"
    except Exception as e:
        logger.error("[provision-check-config] Global error: %s", e)

    return results


def _require_ngwaf_token(
    service_id: str,
    token: str,
    authorization: str | None,
    *,
    missing_message: str = "A Fastly API token is required.",
) -> str:
    """Resolve and validate a caller-supplied Fastly token for NGWAF operations."""
    from backend.utils.fastly_auth import validate_destructive_token

    if authorization and authorization.lower().startswith("bearer "):
        resolved = authorization[len("bearer ") :].strip()
    else:
        resolved = (token or "").strip()
    if not resolved:
        raise HTTPException(
            status_code=401,
            detail={"error": "token_required", "message": missing_message},
        )
    validate_destructive_token(resolved, service_id=service_id)
    return resolved


@provision_router.get("/ngwaf-workspaces", response_model=NgwafWorkspacesResponse, response_model_exclude_unset=True)
def provision_ngwaf_workspaces(
    service_id: str | None = Query(default=None),
    token: str = Query(default=""),
    authorization: str | None = Header(default=None),
):
    """List NGWAF workspaces for a service (requires validated Fastly token)."""
    from backend.core.fastly.client import NgwafWorkspacesResponse
    from backend.provision import fastly
    from backend.utils.router_utils import format_debug_request

    service_id = require_service_id(service_id)
    token = _require_ngwaf_token(
        service_id,
        token,
        authorization,
        missing_message="A Fastly API token is required to list NGWAF workspaces.",
    )
    _ngwaf_url = "https://api.fastly.com/ngwaf/v1/workspaces"
    _req_headers = {"Fastly-Key": token, "Accept": "application/json"}

    try:
        data = fastly("GET", "/ngwaf/v1/workspaces", token=token, max_retries=0, timeout=10)
        raw_body = json.dumps(data)
        workspaces = NgwafWorkspacesResponse.model_validate(data).workspaces_list()
        result: dict[str, Any] = {"workspaces": workspaces}
        if not workspaces:
            try:
                token_data = fastly("GET", "/tokens/self", token=token)
                if not token_data.get("user_id"):
                    result["error_hint"] = (
                        "You are using an Automation Token (Service Account). "
                        "Fastly Automation Tokens do not have access to Next-Gen WAF (NGWAF) data. "
                        "To use NGWAF features, please use a Personal API Token from a human user with Security permissions."
                    )
            except Exception:
                logger.debug("provision_ngwaf_workspaces: step failed (ignored)", exc_info=True)
            req_debug = format_debug_request("GET", _ngwaf_url, _req_headers)
            result["_debug_raw"] = f"{req_debug}\n\n--- Response (200) ---\n{raw_body[:500]}"
        return result
    except RuntimeError as exc:
        http_match = _FASTLY_HTTP_ERROR_RE.match(str(exc))
        if http_match is None:
            raise_internal(logger, exc, code="ngwaf_workspaces_failed", status=400)
        status_code = int(http_match.group(1))
        logger.warning("[ngwaf-workspaces] HTTPError %s body=%s", status_code, str(exc)[:300])
        if status_code == 401:
            raise HTTPException(
                status_code=400,
                detail=make_error("ngwaf_token_invalid", "Invalid API token or missing Edge Security permissions."),
            )
        raise_internal(logger, exc, code="ngwaf_api_error", status=400)
    except Exception as e:
        raise_internal(logger, e, code="ngwaf_workspaces_failed", status=400)


@provision_router.patch(
    "/services/{service_id}/ngwaf-workspace",
    response_model=NgwafWorkspaceSetResponse,
    response_model_exclude_unset=True,
)
def provision_set_ngwaf_workspace(
    service_id: str,
    body: NgwafWorkspaceCreateRequest,
    token: str = Query(default=""),
    authorization: str | None = Header(default=None),
):
    """Persist the NGWAF workspace ID for a service and reload the scheduler."""
    from backend.utils.router_utils import load_service_config, mutate_service_config

    load_service_config(service_id)
    _require_ngwaf_token(service_id, token, authorization)

    workspace_id = (body.ngwaf_workspace_id or "").strip() or None
    mutate_service_config(service_id, lambda c: c.__setitem__("ngwaf_workspace_id", workspace_id))

    try:
        from backend.provision import _sync_crontab

        _sync_crontab()
    except Exception:
        logger.warning("provision_set_ngwaf_workspace: _sync_crontab failed (ignored)", exc_info=True)

    return {"ok": True, "ngwaf_workspace_id": workspace_id}


# --- Merged from backend/routers/bootstrap.py ---


import logging
import sys

import duckdb
from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from backend.core import request_telemetry as web_vitals_store
from backend.deps import get_con, get_service_id, get_source
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin_actions import PageTelemetryResponse
from backend.models.common import (
    BootstrapResponse,
    ClearSqliteResponse,
    DebugStateResponse,
    LogFieldsCatalogResponse,
    OkResponse,
    RecentSqliteResponse,
    SchemaResponse,
    UxEventPayload,
    WebVitalsPayload,
)
from backend.repositories import rum as rum_repo
from backend.utils import sqlite_profiler
from backend.utils.auth import mask_ips_for
from backend.utils.date_utils import iso_z_now
from backend.utils.router_utils import query_errors

logger = logging.getLogger(__name__)
web_vitals_logger = logging.getLogger("backend.web_vitals")
ux_logger = logging.getLogger("backend.ux_events")

bootstrap_router = APIRouter(prefix="/api", tags=["bootstrap"], responses=DEFAULT_ERROR_RESPONSES)


# Single-flight + short-TTL cache for the loopback ADMIN bootstrap path.
# The admin branch runs ~6 SQLite reads against the per-service metadata.db
# that the sync cron concurrently writes; under a reload storm those reads
# stack behind SQLite's 30s busy_timeout and bootstrap wall time balloons to
# 100s+ (prod outage 2026-06-23). Coalescing concurrent identical admin
# bootstraps into ONE computation, plus a short TTL, caps the blast radius.
#
# ONLY the loopback admin path is cached. Analyst / anonymous (remote)
# callers are never cached: their payload is per-session (scoped services,
# email/name) AND they skip every contended
# admin-only section, so they're already cheap and must not share an
# admin-shaped body. Keyed by active service_id — the loopback admin is a
# single identity.
from cachetools import TTLCache  # noqa: E402

_BOOTSTRAP_CACHE_TTL_S = 3.0
_bootstrap_cache: TTLCache = TTLCache(maxsize=128, ttl=_BOOTSTRAP_CACHE_TTL_S)
_bootstrap_inflight: dict[str, asyncio.Future] = {}

# SRE-X: Service-level cache for the expensive real-time DuckDB/SQLite queries
# inside header_badge_and_extents. Caps the query overhead on cold page loads
# and remote analyst requests (which don't hit the short-TTL _bootstrap_cache).
_HEADER_BADGE_CACHE_TTL_S = 30.0
_header_badge_cache: TTLCache = TTLCache(maxsize=256, ttl=_HEADER_BADGE_CACHE_TTL_S)

from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("routers.bootstrap._bootstrap_cache", _bootstrap_cache)
_CacheRegistry.register("routers.bootstrap._bootstrap_inflight", _bootstrap_inflight)
_CacheRegistry.register("routers.bootstrap._header_badge_cache", _header_badge_cache)
# NB: the short-TTL admin bootstrap response is derived from the on-disk
# config set, so ANY mutation of that set (provision writes / teardown removes
# a config) must drop this cache or the change lingers for up to
# _BOOTSTRAP_CACHE_TTL_S (a torn-down service keeps appearing; a new one stays
# missing). The provision router does that via
# CacheRegistry.clear("routers.bootstrap._bootstrap_cache") — going through the
# registry rather than importing this router keeps them import-independent.


def _startup_stub_response(request: Request) -> BootstrapResponse:
    """Minimal response served while the per-service background warm-up
    (``backend.main._background_startup``) is still draining.

    Fields that depend on that warm-up (the real ``services`` list, per-service
    ``schema``/``settings``, anything sourced from DuckLake/DuckDB) are withheld
    here — they stay `[]`/`None` until ``startup_complete`` flips.
    """
    from backend.core.request_telemetry import collection_enabled as _web_vitals_enabled

    return BootstrapResponse.with_telemetry(
        active_service_id=None,
        services=[],
        settings={
            "initializing": True,
            "is_remote_analyst": False,
            "needs_login": False,
            "web_vitals_enabled": _web_vitals_enabled(),
        },
    )


@bootstrap_router.get("/bootstrap", response_model=BootstrapResponse)
async def bootstrap(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):

    main_module = sys.modules.get("backend.main")
    startup_complete = getattr(main_module, "_startup_complete", False) if main_module else False

    if "pytest" in sys.modules:
        startup_complete = True

    if not startup_complete:
        return _startup_stub_response(request)
    # NB: kept docstring-free on purpose — FastAPI publishes a route
    # docstring as the OpenAPI `description`, and this handler had none.
    # Coalesce + short-TTL cache the loopback admin bootstrap; run the
    # analyst/anonymous path fresh. The heavy work lives in _bootstrap_sync
    # (a sync def run in the threadpool).
    is_remote = getattr(request.state, "is_remote", False)
    # Analyst / anonymous: per-session, cheap — never cached.
    if is_remote or bool(request.cookies.get("analyst_session_id")):
        return await run_in_threadpool(_bootstrap_sync, request, service_id)

    # Loopback admin: the check-cache / check-inflight / create-future
    # critical section below contains NO await, so it runs atomically on the
    # single-threaded event loop — race-free without a lock.
    key = service_id or ""
    cached = _bootstrap_cache.get(key)
    if cached is not None:
        return cached[1] if isinstance(cached, tuple) else cached
    inflight = _bootstrap_inflight.get(key)
    if inflight is None:
        inflight = asyncio.ensure_future(_bootstrap_compute_and_cache(request, service_id, key))
        _bootstrap_inflight[key] = inflight
    return await inflight


async def _bootstrap_compute_and_cache(request: Request, service_id: str | None, key: str) -> BootstrapResponse:
    try:
        result = await run_in_threadpool(_bootstrap_sync, request, service_id)
        _bootstrap_cache[key] = result
        return result
    finally:
        # Always release the in-flight slot. Errors propagate to every
        # awaiter and are never cached, so the next request recomputes.
        _bootstrap_inflight.pop(key, None)


def _bootstrap_sync(
    request: Request,
    service_id: str | None,
) -> BootstrapResponse:
    from backend.core import duckdb as _db
    from backend.services.service_manager import get_enriched_services
    from backend.utils.countries import COUNTRY_MAP
    from backend.utils.pop_utils import get_pop_lat_lon_map, get_pop_location_map

    # Cold-path attribution: time each major phase so the harness can pin
    # which section owns the bootstrap wall time. Each entry is
    # {"section": str, "time_ms": float} and surfaces via
    # BootstrapResponse._section_timings.
    timer = SectionTimer()
    section_timings = timer.entries

    # /api/bootstrap is in _UNAUTH_ANALYST_PATHS so anonymous remote visitors
    # can get a stub response telling the frontend to redirect them to
    # /share-login. The middleware therefore SKIPS session validation for
    # this path — we validate the cookie manually here so authenticated
    # analysts still get the full response with their scoped services.
    analyst_session = getattr(request.state, "analyst_session", None)
    is_remote = getattr(request.state, "is_remote", False)
    if analyst_session is None:
        sid = request.cookies.get("analyst_session_id")
        if sid:
            from backend.utils.tunnel import get_tunnel_manager

            def _validate():
                return get_tunnel_manager().validate_session(sid)

            analyst_session = timer.call("validate_analyst_session", _validate)
            if analyst_session is not None:
                request.state.analyst_session = analyst_session

    # Anonymous remote visitor: return a minimal stub so the frontend hides
    # admin nav and redirects to /share-login.
    #
    # Telemetry is intentionally omitted from this path: shipping the
    # SectionTimer entries to an unauthenticated caller gives them a
    # microsecond-precision oracle on validate_analyst_session execution
    # time, which strips network jitter from any timing attack on the
    # session cookie. The authenticated path below still emits
    # section_timings for the dev/admin perf harness.
    if is_remote and analyst_session is None:
        from backend.core.request_telemetry import collection_enabled as _web_vitals_enabled

        return BootstrapResponse.with_telemetry(
            active_service_id=None,
            services=[],
            settings={
                "is_remote_analyst": True,
                "needs_login": True,
                # Even the pre-login page mounts WebVitalsReporter; mirror the
                # collection flag so it stays silent unless explicitly enabled.
                "web_vitals_enabled": _web_vitals_enabled(),
            },
        )

    services = timer.call("get_enriched_services", lambda: get_enriched_services(service_id))

    # Analyst path: filter services to those scoped on the invite.
    if analyst_session is not None:
        allowed = set(analyst_session.service_ids or [])
        services = [s for s in services if s.get("service_id") in allowed]
        if service_id not in allowed:
            # Reset active id to the first allowed service (if any).
            service_id = next((s.get("service_id") for s in services), None)

    # Validate the provided service_id exists; if not, fallback to the first one
    valid_active_id = service_id
    if service_id and not any(s.get("service_id") == service_id for s in services):
        valid_active_id = services[0].get("service_id") if services else None

    # Resolve once and reuse across closures. Previously each closure called
    # get_source_for_service(valid_active_id) independently — 3 redundant
    # stat() + json.loads passes on the cold-load admin bootstrap path.
    active_src = _db.get_source_for_service(valid_active_id) if valid_active_id else None

    schema: list = []
    table_name: str | None = None

    # Use cached schema from config to avoid acquiring a DB lock.
    # table_name mirrors /api/schema's ``_safe_table_name(source.name)``
    # lookup so the frontend can seed its ['admin', 'schema', sid] cache
    # with the same {schema, table_name} payload the dedicated endpoint
    # returns. Both are folded together because they share the
    # valid_active_id gate and downstream consumers always need both.
    def _resolve_schema_and_table() -> None:
        nonlocal schema, table_name
        if not valid_active_id:
            return
        active_svc = next((s for s in services if s.get("service_id") == valid_active_id), None)
        if active_svc and active_svc.get("status"):
            schema = active_svc["status"].get("schema", []) or []
        from backend.core.duckdb import _safe_table_name

        if active_src:
            table_name = _safe_table_name(active_src["name"])

    timer.call("schema_lookup", _resolve_schema_and_table)

    # NOTE: the previous fallback opened a read-only DuckDB connection here
    # and ran get_schema() against the source on cold-cache loads. That call
    # acquired the per-service lock + did a parquet glob, costing 1-3s on
    # the very first /api/bootstrap after a backend restart and blocking
    # the whole admin UI from rendering. With the status-refresh cron
    # populating active_svc["status"]["schema"], the cache is the source
    # of truth — drop the fallback. If schema is empty here, the dashboard
    # renders without a hint banner; the user can refresh once the cron
    # has run (typically <60s after startup).

    pops = timer.call("get_pop_lat_lon_map", get_pop_lat_lon_map)
    pop_geo = timer.call("get_pop_location_map", get_pop_location_map)

    # Per the perf audit (F6): bootstrap's custom_fields_catalog was a
    # ~10-15 KB duplicate of what every chart page already fetches
    # separately from /api/log-fields/catalog, so the field was dropped
    # from the response (v2.1.0). The dashboard card hook only needs
    # custom_dashboard_cards and active_log_field_ids — both derived
    # from the catalog here.
    custom_dashboard_cards: list[dict] = []
    active_log_field_ids: list[str] = []
    ngwaf_configured = False

    def _resolve_custom_fields():
        nonlocal custom_dashboard_cards, active_log_field_ids, ngwaf_configured
        if not valid_active_id:
            return
        from backend import config as svcconfig
        from backend.core import log_fields as _lf

        active_cfg = svcconfig.load_config(valid_active_id)
        if not active_cfg:
            return
        ngwaf_configured = bool(active_cfg.get("ngwaf_workspace_id"))
        lf_config = _lf.get_lf_config(active_cfg)
        catalog_entries = _lf.get_custom_fields_catalog_entries(lf_config)
        custom_dashboard_cards = [
            {"id": f["id"], "label": f["label"]} for f in catalog_entries if f.get("show_in_dashboard")
        ]
        active_log_field_ids = sorted(_lf.resolve_enabled_fields(lf_config)) + [
            cf["name"] for cf in lf_config.get("custom_fields", []) if cf.get("enabled", True)
        ]

    timer.call("custom_fields_catalog", _resolve_custom_fields)

    # Perf audit Phase D: fold the log-fields catalog into the
    # bootstrap response so the frontend can seed its
    # ['log-fields-catalog', service_id] React Query cache from the
    # same payload (mirrors how `views` is already seeded). Saves one
    # HTTP round-trip + ~35 KB transfer on every cold page load
    # without changing the dedicated /api/log-fields/catalog endpoint
    # (other consumers / direct callers still work). Analyst-scope is
    # already enforced for valid_active_id above.
    log_fields_catalog_payload: dict | None = None

    def _resolve_log_fields_catalog():
        nonlocal log_fields_catalog_payload
        if not valid_active_id:
            return
        log_fields_catalog_payload = _compute_log_fields_catalog(valid_active_id)

    timer.call("log_fields_catalog", _resolve_log_fields_catalog)

    # Phase D-2: fold the cached sync-status into bootstrap for admin
    # callers. The dedicated /api/sync-status endpoint is admin-only
    # (RemoteAccessMiddleware blocks analysts) — same restriction
    # applies here. Frontend seeds its ['sync-status', service_id]
    # React Query cache so SyncStatusBadge / useLogsPageState hit
    # cache on first call instead of paying a round-trip.
    #
    # Only emit when the analyst gate would let the dedicated endpoint
    # return data: admin caller AND a valid_active_id with cached
    # status persisted. Analyst sessions get None, matching their 403
    # on the dedicated endpoint.
    sync_status_payload: dict | None = None

    def _resolve_sync_status():
        nonlocal sync_status_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        from backend.routers.admin import compute_sync_status_cached

        sync_status_payload = compute_sync_status_cached(valid_active_id)

    timer.call("sync_status", _resolve_sync_status)

    # Phase D-3: fold the lean share-status banner into bootstrap so
    # the header banner has its initial state on first render and
    # skips the first ~80 B / 1-RTT poll. Polling continues on its
    # 15-s cadence inside useShareStatusBanner for ongoing updates.
    # Admin-only — analyst sessions don't manage sharing.
    share_banner_payload: dict | None = None

    def _resolve_share_banner():
        nonlocal share_banner_payload
        if analyst_session is not None:
            return
        try:
            from backend.utils.tunnel import get_tunnel_manager

            mgr = get_tunnel_manager()
            share_banner_payload = {
                "sharing_active": mgr.is_sharing_active(),
                "public_url": mgr.public_url(),
            }
        except Exception:
            # Banner is non-essential UX; never break /api/bootstrap
            # if the tunnel manager is in a transient state.
            logger.debug("[bootstrap] share banner unavailable", exc_info=True)

    timer.call("share_banner", _resolve_share_banner)

    # Header badge + log extents: analyst-safe payloads projected
    # from the cached sync-status snapshot. Both available to BOTH
    # admin AND analyst.
    #   - header_badge: {latest_log_at, local_rows} — what
    #     SyncStatusBadge renders in the global header (closes the
    #     missing-header-for-analyst gap).
    #   - log_extents: {earliest_log_at, latest_log_at, configured} —
    #     what the FilterBar's auto-range snap-to-extents UX needs.
    #     Same shape /api/log-extents returns.
    header_badge_payload: dict | None = None
    log_extents_payload: dict | None = None

    def _resolve_header_badge_and_extents():
        nonlocal header_badge_payload, log_extents_payload
        if not valid_active_id:
            return

        # svcconfig.get_status is keyed on the service NAME, not the
        # service_id. They're often identical, but resolving via the
        # source dict matches the dedicated /api/sync-status handler
        # exactly so analyst/admin both look in the same place.
        if not active_src:
            return

        from backend import config as svcconfig
        from backend.sync_status_snapshot import compute_sync_status_cached

        # Reuse the live snapshot already computed for the admin bootstrap
        # payload. The config status is refreshed by discovery asynchronously
        # and can lag the dedicated sync-status endpoint, which made the
        # first-paint header show stale freshness even when ingestion was
        # current.
        cached_status = (
            sync_status_payload
            or compute_sync_status_cached(valid_active_id)
            or svcconfig.get_status(active_src["name"])
            or {}
        )

        # Get RUM and REQUEST metrics directly from cached_status!
        rum_payload = cached_status.get("rum")
        request_payload = cached_status.get("request")

        # Complete request snapshots are cheap to project and authoritative,
        # including explicit nulls/zeros after reset. Cache only the legacy
        # fallback scans; don't let their TTL hide a newer (or older) snapshot.
        metric_inputs = {
            key: cached_status.get(key) for key in ("request", "rum", "local_rows", "earliest_log_at", "latest_log_at")
        }
        cached = _header_badge_cache.get(valid_active_id)
        if request_payload is None and cached is not None:
            hb_cached, le_cached, inputs_cached = cached[-3:]
            not_expired = len(cached) == 3 or time.monotonic() < cached[0]
            if not_expired and inputs_cached == metric_inputs:
                header_badge_payload, log_extents_payload = hb_cached, le_cached
                return

        if request_payload is not None:
            rum_payload = rum_payload or {}
            rum_latest = rum_payload.get("latest_log_at")
            rum_total = rum_payload.get("total_rows", 0)
            rum_last_sync = rum_payload.get("last_sync_at")

            request_latest = request_payload.get("latest_log_at")
            request_total = request_payload.get("total_rows", 0)
            request_last_sync = request_payload.get("last_sync_at")
        else:
            # Fallback if the bg cron hasn't populated them yet
            rum_latest = None
            rum_total = 0
            rum_last_sync = None
            request_latest = None
            request_total = 0
            request_last_sync = None

            try:
                from backend.core.metadata.cron_log import latest_cron_per_task

                # RUM metrics from DuckDB RUM views
                try:
                    from backend.core.duckdb import rum_source_for
                    from backend.core.storage import execute_with_stale_view_retry
                    from backend.deps import _ConnectionHolder

                    rum_source = rum_source_for(active_src)
                    with _ConnectionHolder(rum_source, read_only=True) as rum_con:
                        rum_count, rum_last_dt = execute_with_stale_view_retry(
                            rum_con, rum_source, rum_repo.get_header_stats
                        )

                    if rum_count > 0:
                        rum_total = rum_count
                        if rum_last_dt:
                            if isinstance(rum_last_dt, datetime):
                                rum_latest = rum_last_dt.isoformat()
                            else:
                                rum_latest = str(rum_last_dt)
                except Exception:
                    logger.warning("[bootstrap] RUM header fallback scan failed; reporting zero", exc_info=True)

                # REQUEST metrics from DuckDB main table
                try:
                    from backend.core.duckdb import _safe_table_name, get_connection

                    duckdb_con = get_connection(active_src)
                    try:
                        req_count, req_latest = service_stats_repo.get_request_row_stats(
                            duckdb_con, _safe_table_name(active_src["name"])
                        )
                        if req_count > 0:
                            request_total = req_count
                            # RUM beacons are in metadata SQLite, so REQUEST is everything in DuckDB
                            # Get latest log timestamp from DuckDB
                            if req_latest:
                                request_latest = (
                                    req_latest.isoformat() if hasattr(req_latest, "isoformat") else req_latest
                                )
                    finally:
                        duckdb_con.close()
                except Exception:
                    logger.warning("[bootstrap] request header fallback scan failed; reporting zero", exc_info=True)

                # Get last sync times for each type. 'sync' is the pre-v3
                # task name — keep it as a fallback so pre-upgrade history
                # still renders, but prefer the current 'log_discovery' rows
                # (reading only 'sync' froze the SSR-rendered Last Sync at
                # whatever the pre-rename history ended with).
                cron_data = latest_cron_per_task(valid_active_id)
                rum_last_sync = (
                    cron_data.get("rum_discovery", {}).get("started_at")
                    or cron_data.get("rum_sync", {}).get("started_at")
                    or cron_data.get("ledger_rum_sweep", {}).get("started_at")
                )
                request_last_sync = (
                    cron_data.get("log_discovery", {}).get("started_at")
                    or cron_data.get("sync", {}).get("started_at")
                    or cron_data.get("commit", {}).get("started_at")
                    or cron_data.get("ledger_sweep", {}).get("started_at")
                )

            except Exception:
                logger.warning("[bootstrap] header badge fallback failed; reporting zero", exc_info=True)

        earliest = cached_status.get("earliest_log_at")
        latest = request_latest if request_payload is not None else request_latest or cached_status.get("latest_log_at")

        # Align request_total with local_rows if DuckDB count is smaller or stale (e.g. on analyst instances or sync lag)
        local_rows_total = cached_status.get("local_rows")
        if request_payload is None and local_rows_total is not None:
            expected_request_total = max(0, local_rows_total - rum_total)
            if request_total < expected_request_total:
                request_total = expected_request_total
                if not request_latest and cached_status.get("latest_log_at"):
                    request_latest = cached_status.get("latest_log_at")

        total_rows = rum_total + request_total
        local_rows = local_rows_total if local_rows_total is not None else total_rows

        if latest is not None or local_rows is not None:
            header_badge_payload = {
                "rum": {
                    "latest_log_at": rum_latest,
                    "total_rows": rum_total,
                    "last_sync_at": rum_last_sync,
                },
                "request": {
                    "latest_log_at": request_latest,
                    "total_rows": request_total,
                    "last_sync_at": request_last_sync,
                },
                "latest_log_at": latest,  # Request-event extent for backward compat
                "local_rows": local_rows,
            }

        # log_extents: emit even when both are None (with configured=True)
        log_extents_payload = {
            "configured": True,
            "earliest_log_at": earliest,
            "latest_log_at": latest,
        }

        # Store in service-level cache
        _header_badge_cache[valid_active_id] = (
            header_badge_payload,
            log_extents_payload,
            metric_inputs,
        )

    timer.call("header_badge_and_extents", _resolve_header_badge_and_extents)

    # Admin DiagnosticsPanel dims its debug toggles when DEBUG_RESPONSES
    # is off on the backend. Folding the flag in here skips the
    # dedicated /api/debug/state round-trip on every admin page load.
    debug_state_payload: dict | None = None

    def _resolve_debug_state():
        nonlocal debug_state_payload
        from backend.core.share_db import get_setting
        from backend.models.common import _debug_responses_enabled

        query_vis = get_setting("query_debug_visibility", "disabled")
        api_vis = get_setting("api_call_debug_visibility", "disabled")

        debug_state_payload = {
            "debug_responses_enabled": _debug_responses_enabled(),
            "query_debug_visibility": query_vis,
            "api_call_debug_visibility": api_vis,
        }

    timer.call("debug_state", _resolve_debug_state)

    # Seed for the admin OperationsOverview three cards. Each sub-key
    # mirrors the payload shape the card's useQuery already expects, so
    # the frontend just calls ``queryClient.setQueryData(['admin',
    # 'overview', ...], seed)`` on bootstrap resolve and the cards paint
    # on first render. ADMIN ONLY — analysts can't reach /admin so the
    # seed isn't needed (and the underlying endpoints are 403 for them).
    ops_overview_payload: dict | None = None

    def _resolve_ops_overview() -> None:
        nonlocal ops_overview_payload
        if analyst_session is not None:
            return
        from backend.core import metadata as _meta_mod
        from backend.core.request_telemetry import query_registry as _query_registry

        out: dict = {}
        try:
            out["queries_summary"] = _query_registry.summary()
        except Exception:
            logger.debug("[bootstrap] queries_summary seed unavailable", exc_info=True)
        if valid_active_id:
            try:
                import time as _t

                since_utc = _t.time() - 24 * 3600
                out["slow_queries_count"] = {
                    "count": _meta_mod.count_slow_queries(valid_active_id, since_utc=since_utc, threshold_ms=1000.0),
                    "since_hours": 24,
                    "threshold_ms": 1000.0,
                }
            except Exception:
                logger.debug("[bootstrap] slow_queries_count seed unavailable", exc_info=True)
            # ``log_accounting`` is intentionally NOT seeded here. Its
            # ``compute_log_accounting`` helper chains up to 3 Fastly
            # Stats API calls on cache miss (~800 ms), which would
            # dominate every cold bootstrap. The IngestHealthCard's
            # existing 30 s ``refetchInterval`` + #13's TTL cache mean
            # the card paints with real data inside one tick of mount
            # without paying the cost on the bootstrap hot path.
        if out:
            ops_overview_payload = out

    timer.call("ops_overview", _resolve_ops_overview)

    # P1#5 (perf audit): cron_schedule is NOT folded into bootstrap.
    # build_cron_schedule_payload cost ~2.6s p50 / 3.2s p95 and sat on the
    # admin SSR first-paint critical path. It's a lazy-load instead: the
    # /logs cron tab refetches GET /api/cron-schedule on mount (see
    # frontend/app/logs/_state.ts — enabled on activeTab==='cron'), ONE
    # round-trip on the only page that needs it, with no feature loss.
    cron_runs_first_page_payload: dict | None = None

    def _resolve_cron_runs_first_page() -> None:
        nonlocal cron_runs_first_page_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        try:
            from backend.core.metadata.cron_log import get_cron_runs

            total, entries = get_cron_runs(valid_active_id, page=1, per_page=10, with_total=False)
            cron_runs_first_page_payload = {
                "total": total,
                "page": 1,
                "per_page": 10,
                "entries": entries,
            }
        except Exception:
            logger.debug("[bootstrap] cron_runs first page unavailable", exc_info=True)

    timer.call("cron_runs_first_page", _resolve_cron_runs_first_page)

    last_sync_payload: dict | None = None

    def _resolve_last_sync() -> None:
        nonlocal last_sync_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        try:
            from backend.core.metadata.cron_log import latest_cron_per_task

            # 'sync' kept as the pre-v3-rename fallback for pre-upgrade history.
            per_task = latest_cron_per_task(valid_active_id)
            sync_row = per_task.get("log_discovery") or per_task.get("sync")
            if sync_row:
                last_sync_payload = {
                    "started_at": sync_row.get("started_at"),
                    "status": sync_row.get("status"),
                    "duration_s": sync_row.get("duration_s"),
                }
        except Exception:
            # Header badge is a UX nicety — never break bootstrap.
            logger.debug("[bootstrap] last_sync unavailable", exc_info=True)

    timer.call("last_sync", _resolve_last_sync)

    scoring_labels_payload: dict | None = None

    def _resolve_scoring_labels() -> None:
        nonlocal scoring_labels_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        try:
            from backend.scoring import labels as _labels

            scoring_labels_payload = {
                "labels": _labels.list_labels(valid_active_id, limit=500),
                "counts": _labels.counts_by_label(valid_active_id),
            }
        except Exception:
            # Labels UI is a nicety — never break bootstrap.
            logger.debug("[bootstrap] scoring labels unavailable", exc_info=True)

    timer.call("scoring_labels", _resolve_scoring_labels)

    # P1#5 (perf audit): share_status is NOT folded into bootstrap.
    # build_share_status cost ~2.1s and sat on the admin SSR first-paint
    # critical path. It's a lazy-load instead: the /admin/share page
    # refetches GET /api/admin/share/status on mount (see
    # frontend/app/admin/share/page.tsx — unconditional mount-time useQuery
    # on SHARE_STATUS_QUERY_KEY), ONE round-trip on the only page that
    # needs it, with no feature loss. The small global share_banner
    # ({sharing_active, public_url}) is KEPT above for the header.
    views: list[dict] = []

    def _resolve_views() -> list[dict]:
        if not valid_active_id:
            return []
        from backend.repositories import alerts as _views_repo

        try:
            return _views_repo.get_views(valid_active_id)
        except Exception:
            # Views are a UX nicety, not a correctness gate. A repo error
            # must not break /api/bootstrap.
            logger.debug("[bootstrap] saved views unavailable", exc_info=True)
            return []

    views = timer.call("views", _resolve_views)

    from backend.core.request_telemetry import collection_enabled as _web_vitals_collection_enabled

    storage_mode = active_src.get("storage_mode", "cloud") if active_src else "cloud"

    # Analysts need the capability flag, not the operator's workspace ID.
    response_services = services
    if analyst_session is not None:
        response_services = [{k: v for k, v in svc.items() if k != "ngwaf_workspace_id"} for svc in services]
        if mask_ips_for(analyst_session):
            from backend.core.share_db.validation import apply_pii_policy

            schema = apply_pii_policy(schema, {"mask_ips": True})
            response_services = apply_pii_policy(response_services, {"mask_ips": True})

    return BootstrapResponse.with_telemetry(
        active_service_id=valid_active_id,
        services=response_services,
        schema=schema,
        table_name=table_name,
        countries=COUNTRY_MAP,
        pops=pops,
        pop_geo=pop_geo,
        settings={
            "storage_mode": storage_mode,
            "is_remote_analyst": analyst_session is not None,
            "analyst_email": analyst_session.email if analyst_session else None,
            "analyst_name": analyst_session.name if analyst_session else None,
            # Per-invite PII masking flag, surfaced so the frontend can hide
            # IP drill-down affordances for masking analysts. The server-side
            # filter lock (RemoteAccessMiddleware) is the real guarantee; this
            # is the UX signal. False for admins / non-masking analysts.
            "mask_ips": mask_ips_for(analyst_session),
            # Mirror the WEB_VITALS_COLLECT env flag so WebVitalsReporter
            # only POSTs when collection is enabled (default off).
            "web_vitals_enabled": _web_vitals_collection_enabled(),
        },
        custom_dashboard_cards=custom_dashboard_cards,
        active_log_field_ids=active_log_field_ids,
        ngwaf_configured=ngwaf_configured,
        views=views,
        log_fields_catalog=log_fields_catalog_payload,
        sync_status=sync_status_payload,
        share_banner=share_banner_payload,
        header_badge=header_badge_payload,
        log_extents=log_extents_payload,
        debug_state=debug_state_payload,
        ops_overview=ops_overview_payload,
        cron_runs_first_page=cron_runs_first_page_payload,
        last_sync=last_sync_payload,
        scoring_labels=scoring_labels_payload,
        section_timings=section_timings,
    )


@bootstrap_router.get("/schema", response_model=SchemaResponse, response_model_exclude_unset=True)
@query_errors(status_code=500)
def schema_endpoint(
    request: Request,
    source: dict = Depends(get_source),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    from backend import config as svcconfig
    from backend.core.duckdb import _safe_table_name, get_schema

    # Cross-tenant guard: an analyst session scoped to ``svc-A`` must not
    # be able to read ``svc-B``'s schema (custom-field names, types, and
    # PII flags). Mirrors the check in ``log_fields_catalog``.
    require_service_in_scope(request, source.get("name"))

    # Try cache first
    cached_status = svcconfig.get_status(source["name"])
    raw_schema = cached_status["schema"] if (cached_status and "schema" in cached_status) else get_schema(con, source)
    analyst_session = getattr(request.state, "analyst_session", None)
    if mask_ips_for(analyst_session):
        from backend.core.share_db.validation import apply_pii_policy

        raw_schema = apply_pii_policy(raw_schema, {"mask_ips": True})
    return {"schema": raw_schema, "table_name": _safe_table_name(source["name"])}


def _compute_log_fields_catalog(service_id: str | None) -> dict:
    """Build the log-fields catalog payload for ``service_id``.

    Extracted so /api/bootstrap can fold the catalog into its response
    (page-shell composite, perf audit Phase D) without paying a second
    HTTP round-trip on every cold page load.

    Caller is responsible for analyst-scope enforcement on ``service_id``
    before invoking — this helper trusts the caller.
    """
    from backend.core import field_registry as fr
    from backend.core import log_fields as lf

    field_limits: dict = {}
    custom_entries: list = []
    if service_id:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
        if cfg:
            lf_config = lf.get_lf_config(cfg)
            field_limits = lf_config.get("field_limits", {})
            custom_entries = lf.get_custom_fields_catalog_entries(lf_config)

    fields = fr.get_catalog_for_api(field_limits) + custom_entries

    return {
        "groups": fr.get_groups_for_api(),
        "fields": fields,
        "insights": fr.INSIGHT_DEFINITIONS,
        "presets": {
            name: {"label": p["label"], "description": p["description"], "groups": p["groups"]}
            for name, p in fr.PRESETS.items()
        },
    }


@bootstrap_router.get("/log-fields/catalog", response_model=LogFieldsCatalogResponse, response_model_exclude_unset=True)
@query_errors(status_code=500)
def log_fields_catalog(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    """Return the log-fields catalog for the requested service.

    Security: enforce analyst session scope on the requested
    ``service_id``. Without this, an analyst scoped to ``svc-A`` can pass
    ``?service_id=svc-B`` and read svc-B's custom field configuration
    (including PII-related field configs).
    """
    # Only gate when a specific service was requested; a None service_id
    # falls through to the default/admin catalog as before.
    if service_id is not None:
        require_service_in_scope(request, service_id)

    return _compute_log_fields_catalog(service_id)


from backend.models.analytics import InsightsAvailabilityResponse


@bootstrap_router.get("/insight-availability", response_model=InsightsAvailabilityResponse)
@query_errors(status_code=500)
def insight_availability(
    request: Request,
    source: dict = Depends(get_source),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    from backend.core.duckdb import get_schema

    # Cross-tenant guard: insight availability discloses which fields are
    # populated (presence/absence of optional columns), so it needs the
    # same scope check as the schema endpoint.
    require_service_in_scope(request, source.get("name"))

    # Prefer the cached schema snapshot maintained by the status-refresh
    # cron — same source of truth the /schema endpoint and /bootstrap
    # already use. Saves ~300 ms per /insight-availability call because
    # we skip the per-service lock + parquet glob that get_schema would
    # otherwise pay on cold cache, especially when /insights is in
    # flight concurrently.
    from backend import config as svcconfig

    actual_cols: set[str] = set()
    cached_status = svcconfig.get_status(source["name"])
    if cached_status and "schema" in cached_status:
        actual_cols = {col["name"] for col in cached_status["schema"]}
    if not actual_cols:
        # Fallback: cron hasn't populated status yet (cold-start
        # within the first ~60s after backend boot). Do the live
        # lookup so first-load isn't a 503 — subsequent calls hit
        # the cron-populated cache.
        actual_cols = {col["name"] for col in get_schema(con, source, stats=False)}
    from backend.core.log_fields import INSIGHT_DEFINITIONS

    result = []
    for d in INSIGHT_DEFINITIONS:
        req_cols = d.get("required_fields", [])
        available = all(c in actual_cols for c in req_cols)
        missing_fields = [c for c in req_cols if c not in actual_cols]
        result.append({**d, "available": available, "missing_fields": missing_fields if not available else None})
    unavailable = [i for i in result if not i["available"]]
    return {"insights": result, "unavailable": unavailable, "available": True}


# ── Debug & telemetry endpoints (folded from backend.routers.debug) ─────────

debug_router = APIRouter(prefix="/api", responses=DEFAULT_ERROR_RESPONSES)


@debug_router.get("/debug/recent-postgres", tags=["debug"], response_model=RecentSqliteResponse)
@debug_router.get("/debug/recent-sqlite", tags=["debug"], response_model=RecentSqliteResponse)
def recent_sqlite(
    request: Request,
    limit: int = Query(200, ge=1, le=1000),
    since_seq: int = Query(0, ge=0),
):
    """Return up to ``limit`` most-recent Postgres statements captured since
    ``since_seq``. The Debug Panel polls this every 5s when SQL debug is on.
    """
    is_remote = getattr(request.state, "is_remote", False)
    if is_remote:
        from backend.core.share_db import get_setting

        q_vis = get_setting("query_debug_visibility", "disabled")
        if q_vis not in ("analysts", "both"):
            from backend.utils.router_utils import make_error

            raise HTTPException(
                status_code=403, detail=make_error("sql_debug_disabled", "SQL debug is disabled for analysts")
            )

    return sqlite_profiler.get_recent(limit=limit, since_seq=since_seq)


@debug_router.post("/debug/clear-postgres", tags=["debug"], response_model=ClearSqliteResponse)
@debug_router.post("/debug/clear-sqlite", tags=["debug"], response_model=ClearSqliteResponse)
def clear_sqlite(request: Request):
    """Drain the Postgres ring buffer. Manual reset for the Debug Panel."""
    is_remote = getattr(request.state, "is_remote", False)
    if is_remote:
        from backend.utils.router_utils import make_error

        raise HTTPException(
            status_code=403, detail=make_error("admin_only", "Only administrators can clear the SQLite debug buffer")
        )

    sqlite_profiler.clear()
    return {"ok": True, **sqlite_profiler.buffer_stats()}


@debug_router.get("/debug/state", tags=["debug"], response_model=DebugStateResponse, response_model_exclude_unset=True)
def debug_state():
    """Report whether the backend will include ``_debug_queries`` /
    ``_debug_calls`` arrays in API responses.

    Controlled by the process-level ``DEBUG_RESPONSES`` env var (defaults
    OFF in production for security; ON in local-dev ``.env``). The admin
    page calls this to dim the "Query debugging panel" + "API call panel"
    toggles when the backend won't populate them — so the operator gets
    a clear tooltip explaining why their toggle has no effect, instead of
    silently flipping a switch that does nothing.
    """
    from backend.models.common import _debug_responses_enabled

    return {"debug_responses_enabled": _debug_responses_enabled()}


@debug_router.get(
    "/debug/page-telemetry", tags=["debug"], response_model=PageTelemetryResponse, response_model_exclude_unset=True
)
def get_page_telemetry_endpoint(
    page_load_id: str = Query(..., description="Unique page-load ID generated by the frontend"),
):
    """Retrieve all unified SQLite queries, DuckDB queries, FOS calls, and section timings
    matching the page_load_id.
    """
    from backend import config as svcconfig
    from backend.core.metadata import usage_log

    sid = svcconfig.get_active_service_id()
    if not sid:
        return {"queries": [], "sections": [], "calls": []}

    return usage_log.get_page_telemetry(sid, page_load_id)


@debug_router.post("/web-vitals", tags=["web-vitals"], response_model=OkResponse, response_model_exclude_unset=True)
def report_web_vitals(payload: WebVitalsPayload, request: Request) -> dict:
    """Record one Web Vitals metric event. Returns ``{"ok": true}``.

    When collection is disabled the sample is dropped quietly (still a
    200 so the fire-and-forget client never sees an error). When enabled
    it's both logged (structlog key/value emission for log-analysis) and
    appended to the JSONL sink for ``scripts/analyze_web_vitals.py``.
    """
    if not web_vitals_store.collection_enabled():
        return {"ok": True}

    analyst_session = getattr(request.state, "analyst_session", None)
    cohort = "analyst" if analyst_session is not None else "admin"

    web_vitals_logger.info(
        "web_vitals",
        extra={
            "web_vitals_name": payload.name,
            "web_vitals_value": payload.value,
            "web_vitals_rating": payload.rating,
            "web_vitals_id": payload.id,
            "web_vitals_pathname": payload.pathname,
            "web_vitals_navigation_type": payload.navigation_type,
            "web_vitals_delta": payload.delta,
            "web_vitals_cohort": cohort,
        },
    )

    web_vitals_store.append_sample(
        {
            "ts": iso_z_now(),
            "name": payload.name,
            "value": payload.value,
            "rating": payload.rating,
            "id": payload.id,
            "pathname": payload.pathname,
            "navigation_type": payload.navigation_type,
            "delta": payload.delta,
            "cohort": cohort,
        }
    )
    return {"ok": True}


@debug_router.post("/ux-events", tags=["ux-events"], response_model=OkResponse, response_model_exclude_unset=True)
def report_ux_event(payload: UxEventPayload, request: Request) -> dict:
    """Log one UX event. Returns ``{"ok": true}``.

    Same structlog-extra shape as web_vitals so log slicing patterns
    transfer (filter by cohort / pathname / event)."""
    analyst_session = getattr(request.state, "analyst_session", None)
    cohort = "analyst" if analyst_session is not None else "admin"

    ux_logger.info(
        "ux_event",
        extra={
            "ux_event": payload.event,
            "ux_pathname": payload.pathname,
            "ux_component_id": payload.component_id,
            "ux_details": payload.details,
            "ux_cohort": cohort,
        },
    )
    return {"ok": True}


router.include_router(provision_router)
router.include_router(bootstrap_router)

# Back-compat submodule aliases for callers/tests patching legacy module paths.
_self_mod = _sys.modules[__name__]
core = _self_mod
audit = _self_mod
cron = _self_mod
for _mod_name in (
    "backend.routers.services.core",
    "backend.routers.services.audit",
    "backend.routers.services.cron",
    "backend.routers.provision",
    "backend.routers.bootstrap",
    "backend.routers.debug",
    "backend.routers.system",
    "backend.routers.health",
):
    _sys.modules[_mod_name] = _self_mod
if "backend.routers" in _sys.modules:
    for _attr in ("provision", "bootstrap", "debug", "system", "health"):
        setattr(_sys.modules["backend.routers"], _attr, _self_mod)
if "backend" in _sys.modules and hasattr(_sys.modules["backend"], "_MODULE_ALIASES"):
    for _alias in (
        "backend.routers.provision",
        "backend.routers.bootstrap",
        "backend.routers.debug",
        "backend.routers.system",
        "backend.routers.health",
    ):
        _sys.modules["backend"]._MODULE_ALIASES[_alias] = __name__
