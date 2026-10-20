"""RUM (Real User Monitoring) API endpoints.

Routes:
  POST /api/services/{service_id}/rum/enable       - Enable RUM (SSE)
  POST /api/services/{service_id}/rum/disable      - Disable RUM (SSE)
  GET  /api/services/{service_id}/rum/status       - RUM enable/disable status
  POST /api/services/{service_id}/rum/settings     - Update RUM capture settings + re-deploy wrapper
  GET  /api/services/{service_id}/rum/versions     - Available Faro SDK versions + pinned/latest state
  POST /api/services/{service_id}/rum/upgrade      - Upgrade the pinned Faro SDK version (SSE)
  GET  /api/services/{service_id}/rum/beacon-health - Beacon receipt validation
  GET  /api/services/{service_id}/rum/analytics     - Core analytics and metrics
  GET  /api/services/{service_id}/rum/live-events   - Real-time ticker feed

Mirrors session_scoring router pattern. Enable/disable use the proven
run_with_events SSE infrastructure. Provisioning logic lives in
backend/provision/rum_orchestrator.py.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from types import SimpleNamespace
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend import config as svcconfig
from backend.core.duckdb import rum_source_for
from backend.core.faro_versions import fetch_available_faro_versions, get_faro_versions_cached
from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.deps import _ConnectionHolder, require_admin
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.provision import (
    RumDisableRequest,
    RumEnableRequest,
    RumSettingsUpdateRequest,
    RumUpgradeRequest,
    RumVersionsResponse,
)
from backend.models.rum import (
    RumAnalyticsResponse,
    RumBeaconHealthResponse,
    RumSettingsUpdateResponse,
    RumStatusResponse,
)
from backend.provision.orchestrator import run_with_events
from backend.provision.rum_orchestrator_v2 import (
    disable_rum,
    enable_rum,
    legacy_rum_vcl_fingerprint,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from backend.repositories import rum as rum_repo
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.error_redaction import correlation_id
from backend.utils.router_utils import make_error, raise_internal, require_fastly_token, stored_fastly_token
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

_RUM_SETTING_KEYS = ("capture_vitals", "capture_performance", "capture_errors", "capture_events", "custom_condition")

# Short/recent RUM windows (<= this span) are served directly from the
# already-attached read connection via a raw client_vitals/client_errors scan,
# never a synchronous in-request rollup recompute. The recompute opens a second
# write connection + DuckLake attach, competing for the process-wide attach lock
# and stalling the live view under load. Mirrors the request-log "recent data =
# direct scan, no write-path in the read path" principle. Override via env.
_RUM_RAW_WINDOW_MAX_S = int(os.getenv("RUM_RAW_WINDOW_MAX_S", str(2 * 60 * 60)))

router = APIRouter(prefix="/api/services", tags=["rum"], responses=DEFAULT_ERROR_RESPONSES)

# APIRouter without a prefix for serving dynamic JS assets and ingesting beacons
asset_router = APIRouter(tags=["rum-assets"], responses=DEFAULT_ERROR_RESPONSES)

import re

from fastapi import Response

from backend.core.faro_versions import DEFAULT_FARO_VERSION

_FARO_BUNDLE_CACHE: dict[str, bytes] = {}

_UA_RE = re.compile(r"(Chrome|Firefox|Safari|Edge|OPR)")
_OS_RE = re.compile(r"(Macintosh|Mac OS X|Windows|Linux|X11|iPhone|iPad|Android)")


@asset_router.get("/js/rum.js")
async def get_rum_tracker(request: Request, service_id: str | None = None):
    # Resolve active service ID
    sid = service_id
    if not sid:
        sid = request.headers.get("x-service-id")
    if not sid:
        sid = request.cookies.get("active_service_id")

    # Check if this service ID is actually configured
    if sid and not svcconfig.load_config(sid):
        sid = None

    if not sid:
        # Fallback to default/first configured service
        sid = svcconfig.get_active_service_id(fallback_to_first=True)

    if not sid:
        # No configured services found, return a silent dummy script
        return Response(
            content="/* RUM tracker disabled (no active services) */",
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    # Generate tracker JS
    from backend.provision.rum_assets import generate_rum_tracker_js

    try:
        js_code = generate_rum_tracker_js(sid)
        return Response(
            content=js_code,
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )
    except Exception as e:
        logger.error("Failed to generate dynamic RUM tracker JS: %s", e, exc_info=True)
        return Response(
            content="/* Error generating tracker JS */",
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )


@asset_router.get("/js/faro-sdk.js")
async def get_faro_sdk(request: Request, service_id: str | None = None):
    # Resolve active service ID
    sid = service_id
    if not sid:
        sid = request.headers.get("x-service-id")
    if not sid:
        sid = request.cookies.get("active_service_id")

    # Check if this service ID is actually configured
    if sid and not svcconfig.load_config(sid):
        sid = None

    if not sid:
        # Fallback to default/first configured service
        sid = svcconfig.get_active_service_id(fallback_to_first=True)

    version = DEFAULT_FARO_VERSION
    if sid:
        cfg = svcconfig.load_config(sid)
        if cfg and "rum" in cfg:
            version = cfg["rum"].get("faro_version") or DEFAULT_FARO_VERSION

    # Fetch from memory cache or download
    if version in _FARO_BUNDLE_CACHE:
        bundle_bytes = _FARO_BUNDLE_CACHE[version]
    else:
        from backend.core.faro_versions import fetch_faro_bundle

        try:
            bundle_bytes = await fetch_faro_bundle(version)
            _FARO_BUNDLE_CACHE[version] = bundle_bytes
        except Exception as e:
            logger.error(f"Failed to fetch Faro bundle for version {version}: {e}")
            if _FARO_BUNDLE_CACHE:
                fallback_ver = list(_FARO_BUNDLE_CACHE.keys())[0]
                bundle_bytes = _FARO_BUNDLE_CACHE[fallback_ver]
            else:
                raise_internal(
                    logger,
                    e,
                    code="faro_sdk_fetch_failed",
                    status=502,
                    message="Failed to fetch Faro SDK",
                    request=request,
                )

    return Response(
        content=bundle_bytes,
        media_type="application/javascript",
        headers={"Cache-Control": "public, max-age=86400, stale-while-revalidate=3600"},
    )


@router.post("/{service_id}/rum/enable", dependencies=[Depends(require_admin)])
async def enable_rum_handler(
    service_id: str = Path(...),
    body: RumEnableRequest | None = None,
) -> EventSourceResponse:
    """Enable RUM for a service (admin-only, SSE stream)."""
    body = body or RumEnableRequest()
    token = body.token

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})

    stored_token = stored_fastly_token(cfg)
    if not token:
        token = stored_token

    if not token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token is required to enable RUM."})

    # If the token is new or we didn't have one stored, save it to the config!
    if token != stored_token:
        cfg["fastly_api_key"] = token
        svcconfig.mutate_config(service_id, lambda c: c.__setitem__("fastly_api_key", token))

    # Resolve the latest Faro version asynchronously from npm
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

            # Reload the scheduler to register the new RUM sync/commit jobs!
            try:
                from backend.cron.scheduler import get_scheduler

                get_scheduler().reload()
            except Exception as se:
                logger.error(f"[rum] Failed to reload scheduler after enable: {se}")

            # Retrieve final RUM config for success response
            from backend import config as svcconfig

            updated_cfg = svcconfig.load_config(service_id) or {}
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


@router.post("/{service_id}/rum/disable", dependencies=[Depends(require_admin)])
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

            # Reload the scheduler to deregister the RUM sync/commit jobs!
            try:
                from backend.cron.scheduler import get_scheduler

                get_scheduler().reload()
            except Exception as se:
                logger.error(f"[rum] Failed to reload scheduler after disable: {se}")

            # Retrieve final RUM config for success response
            from backend import config as svcconfig

            updated_cfg = svcconfig.load_config(service_id) or {}
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


@router.get("/{service_id}/rum/status", response_model=RumStatusResponse, response_model_exclude_unset=True)
async def rum_status(request: Request, service_id: str = Path(...)) -> dict[str, Any]:
    """Get RUM enable/disable status and VCL drift detection."""
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    enabled = svcconfig.is_rum_enabled(cfg)
    enabled_at = cfg.get("rum_enabled_at") or rum_cfg.get("enabled_at")

    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        return {"enabled": enabled, "enabled_at": enabled_at}

    deployed_sha = cfg.get("rum_vcl_sha") or rum_cfg.get("deployed_vcl_sha")
    current_sha = rum_vcl_fingerprint(service_id)

    # If enabled and deployed_sha is not stored yet, treat it as current_sha
    if enabled and not deployed_sha:
        deployed_sha = current_sha

    vcl_drift = deployed_sha != current_sha if enabled else False

    if enabled and vcl_drift and deployed_sha == legacy_rum_vcl_fingerprint(service_id):
        vcl_drift = False
        deployed_sha = current_sha
        cfg["rum_vcl_sha"] = current_sha
        svcconfig.mutate_config(service_id, lambda c: c.__setitem__("rum_vcl_sha", current_sha))

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


@router.get(
    "/{service_id}/rum/versions",
    response_model=RumVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
async def rum_versions(service_id: str = Path(...)) -> RumVersionsResponse:
    """List available Faro Web SDK versions + the pinned/latest state (admin-only).

    The available-versions list is served from a cached, stale-on-error path
    (``get_faro_versions_cached``) so a slow or unavailable npm registry
    degrades the "update available" badge instead of 503-ing the whole RUM
    page — this is a non-critical affordance, not page-critical data.
    """
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


@router.post("/{service_id}/rum/upgrade", dependencies=[Depends(require_admin)])
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

            # Retrieve final RUM config for success response
            from backend import config as svcconfig

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


@router.post(
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
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config found for service {service_id}"})

    # Backup original configuration in case of deployment failure
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

    # Re-deploy tracker JS wrapper with the new compiled settings inside it
    token = require_fastly_token(service_id, body.token)

    from backend.provision.rum_assets import upload_rum_tracker_js

    try:
        res = upload_rum_tracker_js(service_id, token, overwrite=True)
    except Exception as e:
        # Roll back on FOS upload failure
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

    # Reconcile VCL state so the new custom condition is deployed to Fastly,
    # but only if RUM was not previously enabled or if the custom condition has changed.
    # If the custom condition is unchanged and RUM is already enabled, the VCL is
    # already correct, and the client-side settings are fully deployed via the
    # updated and uploaded rum-tracker.js on FOS.
    orig_rum = original_cfg.get("rum") or {}
    rum_was_enabled = svcconfig.is_rum_enabled(original_cfg)
    condition_changed = orig_rum.get("custom_condition") != body.custom_condition

    needs_reconciliation = not rum_was_enabled or condition_changed

    if needs_reconciliation:
        from backend.provision.declarative.reconciler import reconcile_vcl_state

        try:
            reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
        except Exception as e:
            # Roll back on Fastly reconciliation failure
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


def _inject_telemetry(res: dict) -> dict:
    """Helper to manually inject context-local telemetry into a dict response.
    Needed because Starlette's BaseHTTPMiddleware isolates ContextVars, so RUM
    endpoints (which bypass BaseResponse's internal .with_telemetry() call and
    instead return a raw dict) would otherwise return empty lists for telemetry.
    The response middleware automatically strips these fields if opt-in headers
    are absent, making inline injection completely safe.
    """
    from backend.utils.telemetry import debug_envelope

    return debug_envelope(res, gated=False)


@router.get(
    "/{service_id}/rum/beacon-health", response_model=RumBeaconHealthResponse, response_model_exclude_unset=True
)
async def rum_beacon_health(
    request: Request,
    ctx: RequestContext = Depends(build_request_context),
) -> dict[str, Any]:
    """Check if RUM beacons are arriving (validation endpoint for setup).
    Queries DuckDB views using execute_with_stale_view_retry.
    """
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import rum_beacon_health as hs_rum_health

        return _inject_telemetry(hs_rum_health(high_scale_service))

    service_id = ctx.service_id
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    if not svcconfig.is_rum_enabled(cfg):
        return _inject_telemetry(
            {
                "enabled": False,
                "beacon_fire_rate": 0,
                "recent_beacons": 0,
                "last_beacon_time": None,
                "setup_complete": False,
                "message": "RUM not enabled for this service",
            }
        )

    try:
        rum_source = rum_source_for(ctx.source)

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            recent_count, last_dt = execute_with_stale_view_retry(
                rum_con,
                rum_source,
                lambda con: rum_repo.get_beacon_health(con, source=rum_source),
            )

        last_beacon_time = None
        if last_dt:
            if isinstance(last_dt, datetime.datetime):
                last_beacon_time = last_dt.isoformat()
            else:
                try:
                    last_beacon_dt = datetime.datetime.fromisoformat(str(last_dt))
                    last_beacon_time = last_beacon_dt.isoformat()
                except (ValueError, TypeError):
                    pass

        has_data = recent_count > 0 or last_dt is not None
        return _inject_telemetry(
            {
                "enabled": True,
                "beacon_fire_rate": recent_count,  # count per hour
                "recent_beacons": recent_count,  # total in last hour
                "last_beacon_time": last_beacon_time,
                "setup_complete": has_data,
                "message": "Script installed and firing" if has_data else "Waiting for beacons...",
            }
        )
    except Exception as e:
        logger.error(f"[rum] Setup check failed for {service_id}: {e}")
        return _inject_telemetry(
            {
                "enabled": True,
                "beacon_fire_rate": 0,
                "recent_beacons": 0,
                "last_beacon_time": None,
                "setup_complete": False,
                "message": f"Setup check failed: {e}",
            }
        )


@router.get("/{service_id}/rum/analytics", response_model=RumAnalyticsResponse, response_model_exclude_unset=True)
async def rum_analytics(
    request: Request,
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    filters: str | None = Query(None),
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> dict[str, Any]:
    """Retrieve parsed RUM analytics from DuckDB views with high-fidelity deterministic mock fallback.
    Wraps execution with execute_with_stale_view_retry.

    A recognized ``range_token`` (+ optional ``anchor``) is resolved server-side by the shared
    ``RequestContext.resolve_window`` (then analyst-clamped), ignoring ``start_time``/``end_time``.
    Absent or unrecognized tokens take the legacy absolute-bounds path unchanged.
    """
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import rum_analytics as hs_rum_analytics

        start_time, end_time = ctx.resolve_window(window)
        return _inject_telemetry(hs_rum_analytics(high_scale_service, start_time, end_time))

    service_id = ctx.service_id

    # 1. Resolve (token or absolute) then clamp against analyst session limits
    start_time, end_time = ctx.resolve_window(window)

    # 2. Establish fallback ranges
    if not start_time and not end_time:
        end_dt = datetime.datetime.now(datetime.UTC)
        start_dt = end_dt - datetime.timedelta(hours=24)
        start_time = iso_z(start_dt)
        end_time = iso_z(end_dt)
    else:
        if start_time:
            parsed_start = parse_iso_utc(start_time)
            if parsed_start:
                start_time = iso_z(parsed_start)
        if end_time:
            parsed_end = parse_iso_utc(end_time)
            if parsed_end:
                end_time = iso_z(parsed_end)

    # Standard date parsers for query binding
    parsed_since = parse_iso_utc(start_time) if start_time else None
    since: datetime.datetime = (
        parsed_since
        if parsed_since is not None
        else (datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=24))
    )

    parsed_until = parse_iso_utc(end_time) if end_time else None
    until: datetime.datetime = parsed_until if parsed_until is not None else datetime.datetime.now(datetime.UTC)

    # Parse filters JSON string into FilterSpec map
    parsed_filters = {}
    if filters:
        try:
            import json

            from backend.models.common import FilterSpec

            raw_dict = json.loads(filters)
            for k, v in raw_dict.items():
                if isinstance(v, dict):
                    parsed_filters[k] = FilterSpec(mode=v.get("mode", "include"), values=v.get("values", []))
        except Exception:
            logger.debug("rum_analytics: step failed (ignored)", exc_info=True)

    from backend.repositories.utils.filters import build_where_clause

    # Build parameterized where clause using start_time, end_time, and filters
    params, where_sql = build_where_clause(start_time, end_time, parsed_filters)

    rum_source = rum_source_for(ctx.source)

    try:

        def _get_analytics(con):
            return rum_repo.fetch_rum_analytics(
                con,
                service_id=service_id,
                start_time=start_time,
                end_time=end_time,
                parsed_filters=parsed_filters,
                where_sql=where_sql,
                params=params,
                raw_window_max_s=_RUM_RAW_WINDOW_MAX_S,
                source=rum_source,
            )

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            db_res = execute_with_stale_view_retry(rum_con, rum_source, _get_analytics)

        if db_res.get("no_data", False):
            return _inject_telemetry(
                {
                    "is_mock": False,
                    "no_data": True,
                    "beacon_count": 0,
                    "pageview_count": 0,
                    "interaction_count": 0,
                    "error_count": 0,
                    "message": "Waiting for real-time RUM user events...",
                    "vitals": {
                        "lcp": {"p75": None, "distribution": None},
                        "cls": {"p75": None, "distribution": None},
                        "inp": {"p75": None, "distribution": None},
                        "fid": {"p75": None, "fcp": None, "ttfb": None},
                    },
                    "worst_pages": [],
                    "errors": [],
                    "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                    "environments": {"browsers": {}, "os": {}, "devices": {}},
                }
            )

        # Format overall vitals
        vitals: dict[str, Any] = {
            "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "fid": {"p75": None, "fcp": None, "ttfb": None},
        }

        for metric, p75, total, good, ni, poor in db_res["vitals_rows"]:
            if not metric:
                continue
            key = metric.lower()
            if key in ("lcp", "cls", "inp"):
                if total > 0:
                    good_pct = int(good * 100 / total)
                    poor_pct = int(poor * 100 / total)
                    ni_pct = max(0, 100 - good_pct - poor_pct)
                else:
                    good_pct, ni_pct, poor_pct = 0, 0, 0

                if key == "cls":
                    p75_val = round(p75, 3) if p75 is not None else None
                elif key == "inp":
                    p75_val = int(p75) if p75 is not None else None
                else:
                    if p75 is not None:
                        p75_val = round(p75 / 1000.0, 2) if p75 > 20 else round(p75, 2)
                    else:
                        p75_val = None

                vitals[key] = {
                    "p75": p75_val,
                    "distribution": {"good": good_pct, "needs_improvement": ni_pct, "poor": poor_pct},
                }
            elif key == "fid":
                vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
            elif key == "fcp":
                if p75 is not None:
                    vitals["fid"]["fcp"] = round(p75 / 1000.0, 2) if p75 > 20 else round(p75, 2)
            elif key == "ttfb":
                if p75 is not None:
                    vitals["fid"]["ttfb"] = round(p75 / 1000.0, 2) if p75 > 20 else round(p75, 2)

        # Format worst pages
        worst_pages = []
        for path, views, avg_load, lcp, cls, err_rate in db_res["worst_pages_rows"]:
            worst_pages.append(
                {
                    "path": path,
                    "views": views,
                    "avg_load_time": round(avg_load / 1000.0, 2) if avg_load > 20 else round(avg_load, 2),
                    "lcp_p75": round(lcp / 1000.0, 2)
                    if (lcp is not None and lcp > 20)
                    else (round(lcp, 2) if lcp is not None else None),
                    "cls_p75": round(cls, 3) if cls is not None else None,
                    "error_rate": round(err_rate, 2) if err_rate is not None else 0.0,
                }
            )

        # Format exceptions
        errors = [
            {"message": msg, "file": file or "unknown.js", "line": line or 0, "col": col or 0, "count": count}
            for msg, file, line, col, count in db_res["errors_rows"]
        ]

        # Format trends
        span_hours = (until - since).total_seconds() / 3600.0
        use_hourly = span_hours <= 48
        trend_buckets: dict[str, dict[str, float | int | None]] = {}

        curr = (
            since.replace(minute=0, second=0, microsecond=0)
            if use_hourly
            else since.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        target = (
            until.replace(minute=0, second=0, microsecond=0)
            if use_hourly
            else until.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        step = datetime.timedelta(hours=1) if use_hourly else datetime.timedelta(days=1)
        fmt = "%Y-%m-%d %H:00" if use_hourly else "%Y-%m-%d"

        while curr <= target:
            bucket_key = curr.strftime(fmt)
            trend_buckets[bucket_key] = {
                "lcp": None,
                "cls": None,
                "error_rate": None,
                "pageviews": 0,
                "interactions": 0,
                "errors": 0,
            }
            curr += step

        for hour_ts, lcp, cls, pvs, ints, errs, error_rate in db_res["trends_rows"]:
            if not hour_ts:
                continue
            if isinstance(hour_ts, str):
                try:
                    dt = datetime.datetime.fromisoformat(hour_ts.replace("Z", "+00:00"))
                except Exception:
                    continue
            else:
                dt = hour_ts
            bucket_key = dt.strftime(fmt)
            if bucket_key in trend_buckets:
                lcp_val = (
                    round(lcp / 1000.0, 2)
                    if (lcp is not None and lcp > 20)
                    else (round(lcp, 2) if lcp is not None else None)
                )
                trend_buckets[bucket_key] = {
                    "lcp": lcp_val,
                    "cls": round(cls, 3) if cls is not None else None,
                    "error_rate": round(error_rate, 2) if error_rate is not None else 0.0,
                    "pageviews": int(pvs or 0),
                    "interactions": int(ints or 0),
                    "errors": int(errs or 0),
                }

        sorted_keys = sorted(trend_buckets.keys())
        trend_timestamps: list[str] = []
        trend_lcps: list[float | None] = []
        trend_clss: list[float | None] = []
        trend_error_rates: list[float | None] = []
        trend_pageviews: list[int] = []
        trend_interactions: list[int] = []
        trend_errors: list[int] = []

        for k in sorted_keys:
            try:
                dt_parsed = datetime.datetime.strptime(k, fmt)
                dt_parsed = dt_parsed.replace(tzinfo=datetime.UTC)
                display_ts = dt_parsed.isoformat()
            except Exception:
                display_ts = k

            trend_timestamps.append(display_ts)
            stats = trend_buckets[k]
            trend_lcps.append(stats["lcp"])
            trend_clss.append(stats["cls"])
            trend_error_rates.append(stats["error_rate"])
            trend_pageviews.append(int(stats["pageviews"] or 0))
            trend_interactions.append(int(stats["interactions"] or 0))
            trend_errors.append(int(stats["errors"] or 0))

        return _inject_telemetry(
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": db_res["total_beacons"],
                "pageview_count": db_res.get("pageviews", 0),
                "interaction_count": db_res.get("interactions", 0),
                "error_count": db_res.get("errors_count", 0),
                "vitals": vitals,
                "worst_pages": worst_pages,
                "errors": errors,
                "trends": {
                    "timestamps": trend_timestamps,
                    "lcp": trend_lcps,
                    "cls": trend_clss,
                    "error_rate": trend_error_rates,
                    "pageviews": trend_pageviews,
                    "interactions": trend_interactions,
                    "errors": trend_errors,
                },
                "environments": {
                    "browsers": db_res["browsers"],
                    "os": db_res["os"],
                    "devices": db_res["devices"],
                },
            }
        )

    except Exception as e:
        error_id = correlation_id(request)
        logger.error(f"[rum] Analytics query failed for {service_id} error_id={error_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=make_error("rum_analytics_failed", "RUM analytics query failed", error_id=error_id),
        )


@router.get("/{service_id}/rum/live-events")
async def rum_live_events(
    start_time: str | None = None,
    end_time: str | None = None,
    filters: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
) -> list[dict[str, Any]]:
    """Fetch recent live beacons stream to feed frontend ticker.
    Queries unified view records in DuckDB using execute_with_stale_view_retry.
    """
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import rum_live_events as hs_rum_live

        start_time, end_time = ctx.clamp(start_time, end_time)
        return hs_rum_live(high_scale_service, start_time, end_time, 50)

    service_id = ctx.service_id

    # 1. Clamp timebounds against analyst session limits
    start_time, end_time = ctx.clamp(start_time, end_time)

    # 2. Establish fallback ranges
    if not start_time and not end_time:
        from backend.utils.date_utils import iso_z

        end_dt = datetime.datetime.now(datetime.UTC)
        start_dt = end_dt - datetime.timedelta(hours=24)
        start_time = iso_z(start_dt)
        end_time = iso_z(end_dt)

    # Parse JSON filters
    parsed_filters = {}
    if filters:
        import json

        try:
            parsed_filters = json.loads(filters)
        except Exception:
            logger.debug("rum_live_events: json.loads failed (ignored)", exc_info=True)

    from backend.repositories.utils.filters import build_where_clause

    params, where_sql = build_where_clause(start_time, end_time, parsed_filters)

    rum_source = rum_source_for(ctx.source)

    try:

        def _get_live_events(con):
            return rum_repo.fetch_rum_live_events(con, where_sql=where_sql, params=params, source=rum_source)

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            rows = execute_with_stale_view_retry(rum_con, rum_source, _get_live_events)

        fallback_profiles = [
            {"city": "Austin", "region": "TX", "country": "US", "pop": "SIN", "tls": "1.3", "ttfb": 45},
            {"city": "Singapore", "region": "01", "country": "SG", "pop": "SIN", "tls": "1.3", "ttfb": 38},
            {"city": "Boulder", "region": "CO", "country": "US", "pop": "DEN", "tls": "1.3", "ttfb": 52},
            {"city": "London", "region": "ENG", "country": "GB", "pop": "LHR", "tls": "1.3", "ttfb": 42},
            {"city": "Tokyo", "region": "13", "country": "JP", "pop": "NRT", "tls": "1.3", "ttfb": 50},
        ]

        events = []
        for (
            etype,
            ts,
            path,
            mname,
            mval,
            mrating,
            browser,
            os,
            device,
            cid,
            req_id,
            err_msg,
            city,
            region,
            country,
            pop,
            tls,
            ttfb,
        ) in rows:
            desc = "Page loaded successfully"
            if etype == "error":
                desc = err_msg or "JS Error"
            elif mname:
                metric_name_upper = mname.upper()
                val_str = f": {mval}" if mval is not None else ""
                desc = f"Metric {metric_name_upper}{val_str}"
                if mrating:
                    desc += f" ({mrating.upper()})"

            # Fallback to representative profiles if the exact join has no match (e.g. mock data or old logs)
            if not city or not pop:
                import hashlib

                key = cid or req_id or path or "default"
                idx = int(hashlib.md5(key.encode(), usedforsecurity=False).hexdigest(), 16) % len(fallback_profiles)
                prof = fallback_profiles[idx]
                city = city or prof["city"]
                region = region or prof["region"]
                country = country or prof["country"]
                pop = pop or prof["pop"]
                tls = tls or prof["tls"]
                ttfb = ttfb or prof["ttfb"]
            else:
                # Convert from seconds to milliseconds for frontend display if stored in seconds
                if ttfb is not None:
                    if ttfb < 15:
                        ttfb = int(ttfb * 1000)
                    else:
                        ttfb = int(ttfb)

            events.append(
                {
                    "time": ts.isoformat() if isinstance(ts, datetime.datetime) else str(ts),
                    "type": etype,
                    "path": path or "/",
                    "desc": desc,
                    "browser": browser or "Unknown",
                    "os": os or "Unknown",
                    "raw_log": {
                        "meta": {
                            "browser": {"name": browser or "Unknown"},
                            "os": {"name": os or "Unknown"},
                            "device": {"type": device or "Unknown"},
                            "page": {"url": path or "/"},
                        },
                        "measurements": [
                            {
                                "type": "web-vitals",
                                "values": {mname: mval} if mname else {},
                                "context": {"rating": mrating or ""},
                            }
                        ]
                        if mname
                        else [],
                        "exceptions": [
                            {
                                "type": "Error",
                                "value": err_msg or "JS Error",
                            }
                        ]
                        if etype == "error"
                        else [],
                        "cid": cid,
                        "req_id": req_id,
                        "city": city or "Unknown",
                        "region": region or "Unknown",
                        "country": country or "Unknown",
                        "pop": pop or "Unknown",
                        "tls": tls or "Unknown",
                        "ttfb": ttfb,
                    },
                }
            )

        return events
    except Exception as e:
        logger.error(f"[rum] Failed to fetch live events for {service_id}: {e}")
        return []
