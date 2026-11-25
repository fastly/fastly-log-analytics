"""VCL generation, logging settings, log fields, and log format history endpoints."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from backend.core import log_fields as lf_module
from backend.core import metadata as metadata_db
from backend.core.fastly.client import fastly
from backend.core.request_context import require_admin
from backend.core.service_defaults import DEFAULT_LOG_PERIOD_SECONDS
from backend.models.common import (
    CmcdSettingsResponse,
    LogFieldsResponse,
    LogFieldsUpdateRequest,
    LogFieldsUpdateResponse,
    LoggingSettingsResponse,
    LoggingSettingsUpdateBody,
    ServiceRollbackBody,
    ServiceRollbackResponse,
)
from backend.repositories.utils.response_shaping import SectionTimer
from backend.utils.auth import require_service_in_scope
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    mutate_service_config,
    query_errors,
    redact_error_text,
    require_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

from .crud import _record_audit_best_effort, _sse

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_admin)])

_LOGGING_SETTINGS_CACHE_TTL = 300.0
_logging_settings_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_LOGGING_SETTINGS_CACHE_TTL)
_CacheRegistry.register("services.vcl._logging_settings_cache", _logging_settings_cache)


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


@router.get("/services/{service_id}/logging-settings", response_model=LoggingSettingsResponse)
@query_errors(500, code="logging_settings_save_failed", logger_obj=logger)
def api_service_logging_settings(request: Request, service_id: str):
    require_service_in_scope(request, service_id)
    timer = SectionTimer()
    cached_fields = _logging_settings_cache.get(service_id)
    if cached_fields is not None:
        return LoggingSettingsResponse.with_telemetry(ok=True, section_timings=[], is_cached=True, **cached_fields)

    _t = time.perf_counter()
    cfg = load_service_config(service_id)
    timer.mark("load_service_config", _t)
    prov = cfg.get("provisioning") or {}
    cacheable = {
        "prefix": "",
        "period": int(cfg.get("log_period", DEFAULT_LOG_PERIOD_SECONDS)),
        "sample_rate": int(prov.get("sample_rate", 100)),
        "edge_only": bool(prov.get("edge_only", False)),
        "custom_condition": str(prov.get("custom_condition") or ""),
        "format_match": True,
        "version": cfg.get("last_activated_version"),
        "cmcd": _cmcd_settings_response(cfg),
    }
    _logging_settings_cache[service_id] = cacheable
    return LoggingSettingsResponse.with_telemetry(ok=True, section_timings=timer.entries, **cacheable)


@router.get("/services/{service_id}/log-fields", response_model=LogFieldsResponse)
def api_service_log_fields_get(request: Request, service_id: str):
    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    log_fields_config = lf_module.get_lf_config(cfg)
    if not log_fields_config.get("groups"):
        log_fields_config = {"groups": lf_module.PRESETS["standard"]["groups"], "field_overrides": {}}
    waf_warning = False
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


def _normalize_log_fields_payload(raw_lf: Any, cfg: dict) -> dict:
    from backend.config import is_rum_enabled
    from backend.core.catalog import validate_custom_field_entry
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, reconcile_system_custom_fields
    from backend.provision.rum_orchestrator import _RUM_CUSTOM_FIELDS, _RUM_FIELD_NAMES
    from backend.provision.session_scoring.orchestrate import _SCORING_FIELD_NAMES

    new_lf: dict = (
        raw_lf.model_dump(exclude_none=True) if hasattr(raw_lf, "model_dump") else (dict(raw_lf) if raw_lf else {})
    )
    if not new_lf:
        raise HTTPException(status_code=400, detail={"error": "log_fields is required"})
    caller_cfs = (
        raw_lf.custom_fields
        if hasattr(raw_lf, "custom_fields")
        else (raw_lf.get("custom_fields") if isinstance(raw_lf, dict) else None)
    )
    if caller_cfs:
        system_names = set(_CMCD_FIELD_NAMES) | set(_RUM_FIELD_NAMES) | set(_SCORING_FIELD_NAMES)
        seen_cf_names: list[str] = []
        all_hard_errors: list[str] = []
        for cf in caller_cfs:
            cf_dict = cf.model_dump(exclude_none=True) if hasattr(cf, "model_dump") else cf
            if not isinstance(cf_dict, dict):
                all_hard_errors.append("Custom field entry must be an object")
                continue
            all_hard_errors.extend(validate_custom_field_entry(cf_dict, seen_cf_names, system_names=system_names))
            cf_name = cf_dict.get("name")
            if isinstance(cf_name, str):
                seen_cf_names.append(cf_name)
        if all_hard_errors:
            raise HTTPException(status_code=422, detail={"errors": all_hard_errors})
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
    if any(isinstance(cf, dict) and cf.get("name") in _RUM_FIELD_NAMES for cf in new_lf["custom_fields"]):
        if not is_rum_enabled(cfg):
            new_lf["custom_fields"] = [
                cf
                for cf in new_lf["custom_fields"]
                if not (isinstance(cf, dict) and cf.get("name") in _RUM_FIELD_NAMES)
            ]
        else:
            rum_by_name = {cf["name"]: cf for cf in _RUM_CUSTOM_FIELDS}
            canonicalized_cfs: list[dict] = []
            for cf in new_lf["custom_fields"]:
                if isinstance(cf, dict) and cf.get("name") in rum_by_name:
                    canon_rum = dict(rum_by_name[cf["name"]])
                    if "enabled" in cf and isinstance(cf["enabled"], bool):
                        canon_rum["enabled"] = cf["enabled"]
                    canonicalized_cfs.append(canon_rum)
                else:
                    canonicalized_cfs.append(cf)
            new_lf["custom_fields"] = canonicalized_cfs
    if isinstance(new_lf.get("custom_fields"), list):
        new_lf["custom_fields"] = [
            {**cf, "description": ""}
            if isinstance(cf, dict) and "description" in cf and cf["description"] is None
            else cf
            for cf in new_lf["custom_fields"]
        ]
    new_lf["schema_version"] = 2
    return new_lf


@router.post(
    "/services/{service_id}/log-fields",
    response_model=LogFieldsUpdateResponse,
    response_model_exclude_unset=True,
)
def api_service_log_fields_set(request: Request, service_id: str, body: LogFieldsUpdateRequest):
    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    old_lf = cfg.get("log_fields", {})
    new_lf = _normalize_log_fields_payload(body.log_fields, cfg)
    old_hash = old_lf.get("format_hash")
    new_hash = lf_module.format_hash(new_lf)
    if old_hash == new_hash:
        return {"ok": True, "message": "No changes detected"}
    new_lf["format_hash"] = new_hash
    new_lf["format_updated_at"] = datetime.now(UTC).isoformat()
    cfg["log_fields"] = new_lf
    mutate_service_config(service_id, lambda c: c.__setitem__("log_fields", new_lf))
    _logging_settings_cache.pop(service_id, None)
    try:
        from backend.core.metadata.base import export_admin_state

        export_admin_state(service_id)
    except Exception:
        logger.warning("api_service_log_fields_set: export_admin_state failed (ignored)", exc_info=True)
    return {
        "ok": True,
        "estimate": lf_module.estimate_log_line_bytes(new_lf),
        "line_budget_warning": lf_module.check_log_line_budget(new_lf),
    }


@router.post("/services/{service_id}/logging-settings/update")
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
    body: LoggingSettingsUpdateBody | None = Body(default=None),
):
    from backend import config as svcconfig
    from backend.provision.cli import analytics_log_path

    require_service_in_scope(request, service_id)
    cfg = load_service_config(service_id)
    prov = cfg.setdefault("provisioning", {})
    old_period = int(cfg.get("log_period", DEFAULT_LOG_PERIOD_SECONDS))
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
    if custom_condition is not None:
        from backend.core.catalog import validate_vcl_custom_condition

        cond_msg = validate_vcl_custom_condition(custom_condition)
        if cond_msg:
            raise HTTPException(status_code=400, detail=make_error("invalid_custom_condition", cond_msg))
        custom_condition = custom_condition.strip()
    if not 1 <= period <= 3600:
        raise HTTPException(status_code=400, detail={"error": "Rotation period must be between 1 and 3600 seconds"})
    if not 1 <= sample_rate <= 100:
        raise HTTPException(status_code=400, detail={"error": "Sample rate must be between 1 and 100"})

    body_log_fields: dict | None = None
    raw_body_log_fields = getattr(body, "log_fields", None)
    if raw_body_log_fields is not None:
        body_log_fields = _normalize_log_fields_payload(raw_body_log_fields, cfg)
        body_log_fields["format_hash"] = lf_module.format_hash(body_log_fields)
        body_log_fields["format_updated_at"] = datetime.now(UTC).isoformat()

    effective_update_format = bool(update_format or body_log_fields is not None)
    token = stored_fastly_token(cfg)
    endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
    path = analytics_log_path()

    def stream():
        try:
            from backend.provision import update_logging_endpoint

            update_cfg: dict[str, Any] = {
                "logging_service_id": service_id,
                "endpoint_name": endpoint_name,
                "log_period": period,
                "fos_path": path,
                "sample_rate": sample_rate,
                "edge_only": edge_only,
                "custom_condition": custom_condition,
                "update_format": effective_update_format,
                "cmcd_enabled": cmcd_enabled,
                "cmcd_mode": cmcd_mode,
                "cmcd_version": cmcd_version,
            }
            if body_log_fields is not None:
                update_cfg["log_fields"] = body_log_fields

            for event in update_logging_endpoint(update_cfg, token):
                if event.get("type") == "done":
                    new_ver = int(event.get("version") or 0)
                    if event.get("changed", False):
                        _logging_settings_cache.pop(service_id, None)
                    if event.get("changed", False) and effective_update_format and new_ver:
                        cfg_after = svcconfig.load_config(service_id) or cfg
                        _logging_settings_cache[service_id] = {
                            "prefix": "",
                            "period": period,
                            "sample_rate": sample_rate,
                            "edge_only": edge_only,
                            "custom_condition": custom_condition,
                            "format_match": True,
                            "version": new_ver,
                            "cmcd": _cmcd_settings_response(cfg_after, cmcd_enabled, cmcd_mode, cmcd_version),
                        }
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
                        if effective_update_format:
                            _details["log_fields_deployed"] = True
                        _record_audit_best_effort(service_id, "logging_settings_update", _details, "stream")
                    try:
                        from backend.provision import _sync_crontab

                        _sync_crontab()
                    except Exception:
                        logger.warning("stream: _sync_crontab failed (ignored)", exc_info=True)
                yield json.dumps(event)
        except Exception as e:
            _logging_settings_cache.pop(service_id, None)
            yield _sse("error", redact_error_text(e))

    return sse_response(stream())


@router.post(
    "/services/{service_id}/rollback",
    response_model=ServiceRollbackResponse,
    response_model_exclude_unset=True,
)
def api_service_rollback(
    request: Request,
    service_id: str,
    body: ServiceRollbackBody | None = Body(default=None),
) -> dict[str, Any]:
    """Roll back a service's Fastly active version and persisted deployed config snapshot."""
    from backend import config as svcconfig
    from backend.provision.declarative.fastly_ops import acquire_vcl_lock

    require_service_in_scope(request, service_id)
    load_service_config(service_id)
    token = require_fastly_token(service_id, (body.token or "").strip() if body else "")

    with acquire_vcl_lock(service_id):
        peek = svcconfig.peek_rollback_snapshot(service_id)

        if peek is not None:
            has_snapshot, prev_ver = peek
            if not has_snapshot:
                raise HTTPException(
                    status_code=400,
                    detail=make_error(
                        "no_rollback_snapshot",
                        f"No previous deployed config snapshot recorded for {service_id}",
                    ),
                )
            if prev_ver is not None:
                try:
                    fastly("PUT", f"/service/{service_id}/version/{prev_ver}/activate", token=token)
                except Exception as exc:
                    raise HTTPException(
                        status_code=502,
                        detail=make_error(
                            "rollback_activate_failed",
                            redact_error_text(f"Failed to reactivate Fastly version {prev_ver}: {exc}"),
                        ),
                    ) from exc
            _restored, activated_ver = svcconfig.rollback_deployed_config(service_id)
        else:
            try:
                _restored, activated_ver = svcconfig.rollback_deployed_config(service_id)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=make_error(
                        "no_rollback_snapshot",
                        f"No previous deployed config snapshot recorded for {service_id}",
                    ),
                ) from exc
            if activated_ver is not None:
                try:
                    fastly("PUT", f"/service/{service_id}/version/{activated_ver}/activate", token=token)
                except Exception as exc:
                    raise HTTPException(
                        status_code=502,
                        detail=make_error(
                            "rollback_activate_failed",
                            redact_error_text(f"Failed to reactivate Fastly version {activated_ver}: {exc}"),
                        ),
                    ) from exc

    _logging_settings_cache.pop(service_id, None)
    _record_audit_best_effort(service_id, "service_rollback", {"activated_version": activated_ver}, "rollback")
    msg = (
        f"Rolled back service {service_id} to Fastly version {activated_ver}."
        if activated_ver is not None
        else f"Rolled back service {service_id} configuration snapshot."
    )
    return {"ok": True, "activated_version": activated_ver, "message": msg}
