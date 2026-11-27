"""RUM provisioning, status, SDK version management, and capture settings routes."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Request
from sse_starlette.sse import EventSourceResponse

from backend import config as svcconfig
from backend.core.faro_versions import fetch_available_faro_versions, get_faro_versions_cached
from backend.core.request_context import (
    get_service_id,
    require_admin,
    require_service_access,
)
from backend.core.vcl_generator import validate_destructive_token
from backend.models.analytics import (
    RumSettingsUpdateResponse,
    RumStatusResponse,
)
from backend.models.common import (
    RumDisableRequest,
    RumEnableRequest,
    RumSettingsUpdateRequest,
    RumUpgradeRequest,
    RumVersionsResponse,
)
from backend.provision.feature_fields import claim_feature_fields
from backend.provision.orchestrator import run_with_events
from backend.provision.rum_assets import DEFAULT_RESOURCE_SAMPLE_RATE
from backend.provision.rum_orchestrator import (
    disable_rum,
    enable_rum,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from backend.utils.auth import require_service_in_scope
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    raise_internal,
    redact_error_text,
    require_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

_RUM_SETTING_KEYS = (
    "capture_vitals",
    "capture_performance",
    "capture_errors",
    "capture_events",
    "capture_resources",
    "sample_rate",
    "error_sample_rate",
    "resource_sample_rate",
    "custom_condition",
)

rum_provisioning_router = APIRouter()


def _reload_rum_scheduler_and_cfg(service_id: str, action: str) -> dict[str, Any]:
    """Reload the scheduler after RUM enable/disable and return the updated service config."""
    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception as se:
        logger.error(f"[rum] Failed to reload scheduler after {action}: {se}")
    return svcconfig.load_config(service_id) or {}


def _normalize_sec(val: float | int | None) -> float | None:
    """Convert a metric value to seconds rounded to 2 decimal places when > 20ms."""
    if val is None:
        return None
    return round(val / 1000.0, 2) if val > 20 else round(val, 2)


@rum_provisioning_router.post("/{service_id}/rum/enable", dependencies=[Depends(require_admin)])
async def enable_rum_handler(
    request: Request,
    service_id: str = Path(...),
    body: RumEnableRequest | None = None,
) -> EventSourceResponse:
    """Enable RUM for a service (admin-only, SSE stream)."""
    require_service_in_scope(request, service_id)
    body = body or RumEnableRequest()
    token = body.token

    cfg = load_service_config(service_id)

    stored_token = stored_fastly_token(cfg)
    if not token:
        token = stored_token

    if not token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token is required to enable RUM."})

    latest_faro_version = None
    try:
        available_versions = await fetch_available_faro_versions()
        if available_versions:
            latest_faro_version = available_versions[0]
    except Exception as e:
        logger.warning(f"Could not fetch latest Faro version dynamically, falling back to DEFAULT_FARO_VERSION: {e}")

    def stream():
        yield json.dumps({"type": "status", "message": f"Enabling Real User Monitoring (RUM) for {service_id}..."})
        try:
            for event in run_with_events(
                enable_rum,
                service_id,
                token,
                activate=body.activate,
                faro_version=latest_faro_version,
                raise_on_error=True,
            ):
                yield json.dumps(event)

            if token != stored_token:
                svcconfig.mutate_config(service_id, lambda c: c.__setitem__("fastly_api_key", token))

            updated_cfg = _reload_rum_scheduler_and_cfg(service_id, "enable")
            yield json.dumps(
                {
                    "type": "done",
                    "message": "Real User Monitoring (RUM) enabled successfully!"
                    if body.activate
                    else "Real User Monitoring (RUM) draft configuration compiled and validated successfully!",
                    "rum": {
                        "enabled": svcconfig.is_rum_enabled(updated_cfg),
                        "enabled_at": updated_cfg.get("rum_enabled_at", ""),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": redact_error_text(e)})

    return sse_response(stream())


@rum_provisioning_router.post("/{service_id}/rum/disable", dependencies=[Depends(require_admin)])
async def disable_rum_handler(
    request: Request,
    service_id: str = Path(...),
    body: RumDisableRequest | None = None,
) -> EventSourceResponse:
    """Disable RUM for a service (admin-only, SSE stream)."""
    require_service_in_scope(request, service_id)
    body = body or RumDisableRequest()
    token = require_fastly_token(service_id, body.token)

    def stream():
        yield json.dumps({"type": "status", "message": f"Disabling Real User Monitoring (RUM) for {service_id}..."})
        try:
            for event in run_with_events(
                disable_rum,
                service_id,
                token,
                remove_cloud_files=body.remove_cloud_files,
                remove_bucket=body.remove_bucket,
                activate=body.activate,
                raise_on_error=True,
            ):
                yield json.dumps(event)

            updated_cfg = _reload_rum_scheduler_and_cfg(service_id, "disable")
            yield json.dumps(
                {
                    "type": "done",
                    "message": "Real User Monitoring (RUM) disabled successfully!"
                    if body.activate
                    else "Real User Monitoring (RUM) draft configuration compiled and validated successfully!",
                    "rum": {
                        "enabled": svcconfig.is_rum_enabled(updated_cfg),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": redact_error_text(e)})

    return sse_response(stream())


def _require_rum_status_access(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> str | None:
    analyst_session = getattr(getattr(request, "state", None), "analyst_session", None)
    if analyst_session is None:
        return service_id
    raw_service_ids = getattr(analyst_session, "service_ids", None)
    if raw_service_ids is None or isinstance(raw_service_ids, (list, tuple, set, frozenset)):
        if hasattr(request, "path_params"):
            return require_service_access(request, service_id)
        allowed = set(raw_service_ids or [])
        if service_id not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": service_id},
            )
    return service_id


def _rum_status_sync(request: Request, service_id: str) -> dict[str, Any]:
    _require_rum_status_access(request, service_id)
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    enabled = svcconfig.is_rum_enabled(cfg)
    enabled_at = cfg.get("rum_enabled_at") or rum_cfg.get("enabled_at")

    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        return {"enabled": enabled, "enabled_at": enabled_at}

    deployed_sha = cfg.get("rum_vcl_sha") or rum_cfg.get("deployed_vcl_sha")
    current_sha = rum_vcl_fingerprint(service_id)

    if enabled and not deployed_sha:
        deployed_sha = current_sha

    vcl_drift = deployed_sha != current_sha if enabled else False

    return {
        "enabled": enabled,
        "enabled_at": enabled_at,
        "deployed_vcl_sha": deployed_sha,
        "current_vcl_sha": current_sha,
        "vcl_drift": vcl_drift,
        "capture_vitals": rum_cfg.get("capture_vitals", True),
        "capture_performance": rum_cfg.get("capture_performance", True),
        "capture_errors": rum_cfg.get("capture_errors", True),
        "capture_events": rum_cfg.get("capture_events", True),
        "capture_resources": rum_cfg.get("capture_resources", True),
        "sample_rate": rum_cfg.get("sample_rate", 1.0),
        "error_sample_rate": rum_cfg.get("error_sample_rate", 1.0),
        "resource_sample_rate": rum_cfg.get("resource_sample_rate", DEFAULT_RESOURCE_SAMPLE_RATE),
        "custom_condition": rum_cfg.get("custom_condition", ""),
        "supports_storage_purge": True,
    }


@rum_provisioning_router.get(
    "/{service_id}/rum/status", response_model=RumStatusResponse, response_model_exclude_unset=True
)
async def rum_status(
    request: Request,
    service_id: str = Path(...),
    _access: str | None = Depends(_require_rum_status_access),
) -> dict[str, Any]:
    """Get RUM enable/disable status and VCL drift detection."""
    return await asyncio.to_thread(_rum_status_sync, request, service_id)


@rum_provisioning_router.get(
    "/{service_id}/rum/versions",
    response_model=RumVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
async def rum_versions(request: Request, service_id: str = Path(...)) -> RumVersionsResponse:
    """List available Faro Web SDK versions + the pinned/latest state (admin-only)."""
    require_service_in_scope(request, service_id)
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}
    current = rum_cfg.get("faro_version")

    available = await get_faro_versions_cached()

    latest = available[0] if available else None
    update_available = bool(current and latest and current != latest)

    return RumVersionsResponse(
        available=available,
        current=current,
        latest=latest,
        update_available=update_available,
    )


@rum_provisioning_router.post("/{service_id}/rum/upgrade", dependencies=[Depends(require_admin)])
async def upgrade_rum_handler(
    request: Request,
    service_id: str = Path(...),
    body: RumUpgradeRequest = Body(...),
) -> EventSourceResponse:
    """Upgrade the pinned Faro Web SDK version for a service (admin-only, SSE stream)."""
    require_service_in_scope(request, service_id)
    token = require_fastly_token(service_id, body.token)
    if body.token and body.token.strip():
        validate_destructive_token(body.token.strip(), service_id=service_id)

    try:
        available = await fetch_available_faro_versions()
    except ValueError as e:
        raise_internal(
            logger,
            e,
            code="faro_registry_unavailable",
            status=503,
            message="Faro version registry unavailable",
        )

    if body.version not in available:
        raise HTTPException(
            status_code=400,
            detail=make_error("unknown_faro_version", f"Unknown Faro Web SDK version: {body.version}"),
        )

    def stream():
        yield json.dumps(
            {"type": "status", "message": f"Upgrading Faro Web SDK to v{body.version} for {service_id}..."}
        )
        try:
            for event in run_with_events(
                upgrade_faro_version,
                service_id,
                body.version,
                token,
                activate=body.activate,
                raise_on_error=True,
            ):
                yield json.dumps(event)

            updated_cfg = svcconfig.load_config(service_id) or {}
            updated_rum_cfg = updated_cfg.get("rum") or {}
            yield json.dumps(
                {
                    "type": "done",
                    "message": f"Faro Web SDK upgraded to v{body.version} successfully!"
                    if body.activate
                    else "Faro Web SDK upgrade draft compiled and validated successfully!",
                    "rum": {
                        "faro_version": updated_rum_cfg.get("faro_version"),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": redact_error_text(e)})

    return sse_response(stream())


@rum_provisioning_router.post(
    "/{service_id}/rum/settings",
    response_model=RumSettingsUpdateResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def update_rum_settings(
    request: Request,
    service_id: str = Path(...),
    body: RumSettingsUpdateRequest = Body(...),
) -> dict[str, Any]:
    """Update RUM capture settings and re-deploy rum-tracker.js (admin-only)."""
    require_service_in_scope(request, service_id)
    from backend.core.fastly.client import validate_rum_custom_condition

    raw_cond = body.custom_condition
    cond_msg = validate_rum_custom_condition(raw_cond)
    if cond_msg:
        raise HTTPException(status_code=400, detail=make_error("invalid_rum_condition", cond_msg))

    cfg = load_service_config(service_id)
    token = require_fastly_token(service_id, body.token)
    if body.token and body.token.strip():
        validate_destructive_token(body.token.strip(), service_id=service_id)

    import copy

    from backend.core.fastly.client import fastly, get_active_version
    from backend.provision.declarative.reconciler import _purge_rum_surrogate_key, reconcile_vcl_state
    from backend.provision.rum_assets import upload_rum_tracker_js

    original_cfg = copy.deepcopy(cfg)
    baseline_raw = svcconfig.snapshot_config(service_id)
    prev_cond = (cfg.get("rum") or {}).get("custom_condition", "")
    prev_faro_version = (cfg.get("rum") or {}).get("faro_version")
    prev_fp = rum_vcl_fingerprint(service_id, cfg)
    prev_active_fields = svcconfig.active_log_field_ids(cfg)
    prev_ver = get_active_version(service_id, token)

    target_faro_version = body.faro_version.strip() if body.faro_version and body.faro_version.strip() else None
    faro_changed = bool(target_faro_version and target_faro_version != prev_faro_version)
    if faro_changed:
        try:
            available_faro = asyncio.run(fetch_available_faro_versions())
        except ValueError as e:
            raise_internal(
                logger,
                e,
                code="faro_registry_unavailable",
                status=503,
                message="Faro version registry unavailable",
            )
        if target_faro_version not in available_faro:
            raise HTTPException(
                status_code=400,
                detail=make_error("unknown_faro_version", f"Unknown Faro Web SDK version: {target_faro_version}"),
            )

    def _apply_settings(c: dict) -> None:
        rum_block = c.setdefault("rum", {})
        rum_block["capture_vitals"] = body.capture_vitals
        rum_block["capture_performance"] = body.capture_performance
        rum_block["capture_errors"] = body.capture_errors
        rum_block["capture_events"] = body.capture_events
        rum_block["capture_resources"] = body.capture_resources
        rum_block["sample_rate"] = body.sample_rate
        rum_block["error_sample_rate"] = body.error_sample_rate
        rum_block["resource_sample_rate"] = body.resource_sample_rate
        rum_block["custom_condition"] = body.custom_condition
        if svcconfig.is_rum_enabled(c):
            # Client timings are matched to request logs by `rid`, so RUM keeps that field on.
            claim_feature_fields(c, "rum")

    def _restore_original_cfg() -> None:
        if baseline_raw is not None:
            svcconfig.restore_config_snapshot(service_id, baseline_raw)
            return

        def _reset(c: dict) -> None:
            c.clear()
            c.update(copy.deepcopy(original_cfg))

        svcconfig.mutate_config(service_id, _reset)

    def _refresh_vcl_sha(c: dict) -> None:
        if svcconfig.is_rum_enabled(c):
            c["rum_vcl_sha"] = rum_vcl_fingerprint(service_id, c)

    updated_cfg = svcconfig.mutate_config(service_id, _apply_settings) or cfg
    vcl_changed = (
        (body.custom_condition != prev_cond)
        or (rum_vcl_fingerprint(service_id, updated_cfg) != prev_fp)
        or (svcconfig.active_log_field_ids(updated_cfg) != prev_active_fields)
    )
    vcl_reconciled = False
    new_ver: int | None = None

    try:
        if faro_changed:
            assert target_faro_version is not None
            upgrade_res = upgrade_faro_version(service_id, target_faro_version, token, activate=True)
            vcl_reconciled = True
            new_ver = upgrade_res.get("logging_service_active_version")
        elif vcl_changed:
            reconcile_res = reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
            vcl_reconciled = True
            new_ver = getattr(reconcile_res, "activated_version", None)
        res = upload_rum_tracker_js(service_id, token, overwrite=True)
        _purge_rum_surrogate_key(service_id, token)
        svcconfig.mutate_config(service_id, _refresh_vcl_sha)
        if baseline_raw is not None:
            svcconfig.record_deployed_config(
                service_id,
                new_ver,
                previous_version=(
                    prev_ver if vcl_reconciled and new_ver and prev_ver and new_ver != prev_ver else None
                ),
            )
    except Exception as e:
        if vcl_reconciled and prev_ver:
            try:
                cur_ver = get_active_version(service_id, token)
                if cur_ver and cur_ver != prev_ver:
                    fastly("PUT", f"/service/{service_id}/version/{prev_ver}/activate", token=token)
            except Exception as fastly_rb_err:
                logger.error(f"Failed to reactivate Fastly version {prev_ver} for {service_id}: {fastly_rb_err}")
        try:
            _restore_original_cfg()
        except Exception as rollback_err:
            logger.error(f"Failed to rollback service config for {service_id}: {rollback_err}")
        raise_internal(
            logger,
            e,
            code="rum_reconcile_failed" if vcl_changed else "rum_tracker_upload_failed",
            message="Failed to update RUM settings and reconcile Fastly configuration",
        )

    return {
        "ok": True,
        "message": "RUM capture settings updated, wrapper re-deployed, and Fastly configuration reconciled!",
        "skipped": res.get("skipped", False),
    }
