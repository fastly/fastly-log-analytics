"""VCL generation, logging settings, log fields, and log format history endpoints."""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.parse
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query, Request

from backend.core import duckdb as _db
from backend.core import log_fields as lf_module
from backend.core import metadata as metadata_db
from backend.models.common import (
    CmcdSettingsResponse,
    LogFieldsResponse,
    LogFieldsUpdateRequest,
    LogFieldsUpdateResponse,
    LoggingSettingsResponse,
)
from backend.repositories import assets as service_stats_repo
from backend.repositories._base import SectionTimer
from backend.utils.auth import require_service_in_scope
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.router_utils import (
    load_service_config,
    mutate_service_config,
    query_errors,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

from .crud import _record_audit_best_effort, _sse

logger = logging.getLogger(__name__)

router = APIRouter()

_LOGGING_SETTINGS_CACHE_TTL = 300.0
_logging_settings_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_LOGGING_SETTINGS_CACHE_TTL)


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


@router.get("/services/{service_id}/log-fields", response_model=LogFieldsResponse)
def api_service_log_fields_get(request: Request, service_id: str):
    require_service_in_scope(request, service_id)
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


@router.post(
    "/services/{service_id}/log-fields",
    response_model=LogFieldsUpdateResponse,
    response_model_exclude_unset=True,
)
def api_service_log_fields_set(request: Request, service_id: str, body: LogFieldsUpdateRequest):
    from backend.provision.cmcd_fields import reconcile_system_custom_fields

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
