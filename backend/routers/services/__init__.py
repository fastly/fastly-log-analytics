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
    AnalystInvite,
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
    ProvisionLakeInfoResponse,
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
_ANALYST_SAFE_SERVICE_FIELDS = frozenset({"service_id", "name", "access_level", "is_active"})


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


@core_router.get(
    "/services/{service_id}/lake-info",
    response_model=ProvisionLakeInfoResponse,
    response_model_exclude_unset=True,
)
def get_service_lake_info(source: dict = Depends(get_source)):
    """Return Iceberg table range and calendar for a configured service."""
    from backend.core.iceberg.lake_info import fetch_lake_info

    return fetch_lake_info(source, use_temp_cache=False)


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
        if cfg.get("access_level") != "read_write":
            raise HTTPException(
                status_code=403,
                detail={"error": "API token rotation is only available for admin (read_write) services"},
            )
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
    m_pref = re.match(r"^/?(.*?)/raw/", ep.get("path", ""))
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


@core_router.post("/services/{service_id}/generate-viewer-key", response_model=AnalystInvite)
def api_invite_analyst(service_id: str):
    from backend.provision import generate_analyst_invite

    try:
        return AnalystInvite.with_telemetry(**generate_analyst_invite(service_id))
    except RuntimeError as e:
        msg, low = str(e), str(e).lower()
        status = (
            404
            if "not found" in low
            else 403
            if "read_write" in low
            else 409
            if "independent analyst invites are unavailable" in low
            else 400
        )
        raise HTTPException(status_code=status, detail={"error": msg})


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


def _check_iceberg_type_lock(
    service_id: str, field_name: str, new_duckdb_type: str | None = None, new_value_type: str | None = None
) -> None:
    """Ensure we don't mutate the core type of an existing field in the committed table."""
    from backend.core import iceberg as _iceberg

    src = _db.get_source_for_service(service_id)
    if not src or src.get("storage_mode", "cloud") == "local":
        return
    try:
        duckdb_to_arrow = getattr(_iceberg, "_DUCKDB_TO_ARROW", {})
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


def _locked_iceberg_field_names(src: dict | None, *, error_lead: str, error_tail: str) -> set[str]:
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
    _check_iceberg_type_lock(
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
        schema_fields = _locked_iceberg_field_names(
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
    locked_field_names = _locked_iceberg_field_names(
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

_self_mod = _sys.modules[__name__]
core = _self_mod
audit = _self_mod
cron = _self_mod
_sys.modules["backend.routers.services.core"] = _self_mod
_sys.modules["backend.routers.services.audit"] = _self_mod
_sys.modules["backend.routers.services.cron"] = _self_mod
