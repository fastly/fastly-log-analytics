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
from backend.provision.orchestrator import run_with_events
from backend.provision.rum_orchestrator_v2 import (
    disable_rum,
    enable_rum,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    raise_internal,
    require_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

_RUM_SETTING_KEYS = ("capture_vitals", "capture_performance", "capture_errors", "capture_events", "custom_condition")

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
    service_id: str = Path(...),
    body: RumEnableRequest | None = None,
) -> EventSourceResponse:
    """Enable RUM for a service (admin-only, SSE stream)."""
    body = body or RumEnableRequest()
    token = body.token

    cfg = load_service_config(service_id)

    stored_token = stored_fastly_token(cfg)
    if not token:
        token = stored_token

    if not token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token is required to enable RUM."})

    if token != stored_token:
        cfg["fastly_api_key"] = token
        svcconfig.mutate_config(service_id, lambda c: c.__setitem__("fastly_api_key", token))

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
            yield json.dumps({"type": "error", "message": str(e)})

    return sse_response(stream())


@rum_provisioning_router.post("/{service_id}/rum/disable", dependencies=[Depends(require_admin)])
async def disable_rum_handler(
    service_id: str = Path(...),
    body: RumDisableRequest | None = None,
) -> EventSourceResponse:
    """Disable RUM for a service (admin-only, SSE stream)."""
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
            yield json.dumps({"type": "error", "message": str(e)})

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
        "custom_condition": rum_cfg.get("custom_condition", ""),
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
async def rum_versions(service_id: str = Path(...)) -> RumVersionsResponse:
    """List available Faro Web SDK versions + the pinned/latest state (admin-only)."""
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
    service_id: str = Path(...),
    body: RumUpgradeRequest = Body(...),
) -> EventSourceResponse:
    """Upgrade the pinned Faro Web SDK version for a service (admin-only, SSE stream)."""
    token = require_fastly_token(service_id, body.token)

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
            yield json.dumps({"type": "error", "message": str(e)})

    return sse_response(stream())


@rum_provisioning_router.post(
    "/{service_id}/rum/settings",
    response_model=RumSettingsUpdateResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def update_rum_settings(
    service_id: str = Path(...),
    body: RumSettingsUpdateRequest = Body(...),
) -> dict[str, Any]:
    """Update RUM capture settings and re-deploy rum-tracker.js (admin-only)."""
    cfg = load_service_config(service_id)

    import copy

    original_cfg = copy.deepcopy(cfg)

    def _apply_settings(c: dict) -> None:
        rum_block = c.setdefault("rum", {})
        rum_block["capture_vitals"] = body.capture_vitals
        rum_block["capture_performance"] = body.capture_performance
        rum_block["capture_errors"] = body.capture_errors
        rum_block["capture_events"] = body.capture_events
        rum_block["custom_condition"] = body.custom_condition

    def _roll_back_settings(c: dict) -> None:
        original_rum = original_cfg.get("rum") or {}
        rum_block = c.setdefault("rum", {})
        for key in _RUM_SETTING_KEYS:
            if key in original_rum:
                rum_block[key] = original_rum[key]
            else:
                rum_block.pop(key, None)

    svcconfig.mutate_config(service_id, _apply_settings)

    token = require_fastly_token(service_id, body.token)

    from backend.provision.rum_assets import upload_rum_tracker_js

    try:
        res = upload_rum_tracker_js(service_id, token, overwrite=True)
    except Exception as e:
        try:
            svcconfig.mutate_config(service_id, _roll_back_settings)
        except Exception as rollback_err:
            logger.error(f"Failed to rollback service config for {service_id}: {rollback_err}")
        raise_internal(
            logger,
            e,
            code="rum_tracker_upload_failed",
            message="Failed to upload RUM tracker wrapper to FOS",
        )

    orig_rum = original_cfg.get("rum") or {}
    rum_was_enabled = svcconfig.is_rum_enabled(original_cfg)
    condition_changed = orig_rum.get("custom_condition") != body.custom_condition

    needs_reconciliation = not rum_was_enabled or condition_changed

    if needs_reconciliation:
        from backend.provision.declarative.reconciler import reconcile_vcl_state

        try:
            reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
        except Exception as e:
            try:
                svcconfig.mutate_config(service_id, _roll_back_settings)
            except Exception as rollback_err:
                logger.error(f"Failed to rollback service config for {service_id}: {rollback_err}")
            raise_internal(
                logger,
                e,
                code="rum_reconcile_failed",
                message="Failed to reconcile and deploy updated RUM conditions to Fastly",
            )
    else:
        logger.info(
            f"[rum] Bypassing Fastly VCL reconciliation for service {service_id} "
            "because RUM is already enabled and custom_condition is unchanged."
        )

    return {
        "ok": True,
        "message": "RUM capture settings updated, wrapper re-deployed, and Fastly configuration reconciled!",
        "skipped": res.get("skipped", False),
    }
