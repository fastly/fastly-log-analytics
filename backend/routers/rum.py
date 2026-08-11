"""RUM (Real User Monitoring) API endpoints.

Routes:
  POST /api/services/{service_id}/rum/enable       - Enable RUM (SSE)
  POST /api/services/{service_id}/rum/disable      - Disable RUM (SSE)
  GET  /api/services/{service_id}/rum/status       - RUM enable/disable status
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
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend import config as svcconfig
from backend.core.duckdb import rum_source_for
from backend.core.faro_versions import fetch_available_faro_versions
from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.deps import _ConnectionHolder
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.provision import (
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
    legacy_rum_vcl_fingerprint,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, make_error

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/services", tags=["rum"], responses=DEFAULT_ERROR_RESPONSES)


def _get_fastly_token(service_id: str) -> str:
    """Get Fastly API token from service config."""
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    token = cfg.get("fastly_api_key", "")
    if not token:
        raise HTTPException(status_code=400, detail={"error": f"No Fastly API key for service {service_id}"})
    return token


@router.post("/{service_id}/rum/enable")
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

    stored_token = cfg.get("fastly_api_key", "")
    if not token:
        token = stored_token

    if not token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token is required to enable RUM."})

    # If the token is new or we didn't have one stored, save it to the config!
    if token != stored_token:
        cfg["fastly_api_key"] = token
        svcconfig.save_config(service_id, cfg)

    def stream():

        yield json.dumps({"type": "status", "message": f"Enabling Real User Monitoring (RUM) for {service_id}..."})
        try:
            for event in run_with_events(
                enable_rum,
                service_id,
                token,
                activate=body.activate,
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
                        "enabled": updated_cfg.get("rum_enabled", False),
                        "enabled_at": updated_cfg.get("rum_enabled_at", ""),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.post("/{service_id}/rum/disable")
async def disable_rum_handler(
    service_id: str = Path(...),
    body: RumDisableRequest | None = None,
) -> EventSourceResponse:
    """Disable RUM for a service (admin-only, SSE stream)."""
    body = body or RumDisableRequest()
    token = body.token or _get_fastly_token(service_id)

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
                        "enabled": updated_cfg.get("rum_enabled", False),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.get("/{service_id}/rum/status")
async def rum_status(request: Request, service_id: str = Path(...)) -> dict[str, Any]:
    """Get RUM enable/disable status and VCL drift detection."""
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    enabled = cfg.get("rum_enabled", False) or rum_cfg.get("enabled", False)
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
        svcconfig.save_config(service_id, cfg)

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


@router.get("/{service_id}/rum/versions", response_model=RumVersionsResponse, response_model_exclude_unset=True)
async def rum_versions(service_id: str = Path(...)) -> RumVersionsResponse:
    """List available Faro Web SDK versions + the pinned/latest state (admin-only)."""
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}
    current = rum_cfg.get("faro_version")

    try:
        available = await fetch_available_faro_versions()
    except ValueError as e:
        raise HTTPException(status_code=503, detail=make_error("faro_registry_unavailable", str(e))) from e

    latest = available[0] if available else None
    update_available = bool(current and latest and current != latest)

    return RumVersionsResponse(
        available=available,
        current=current,
        latest=latest,
        update_available=update_available,
    )


@router.post("/{service_id}/rum/upgrade")
async def upgrade_rum_handler(
    service_id: str = Path(...),
    body: RumUpgradeRequest = Body(...),
) -> EventSourceResponse:
    """Upgrade the pinned Faro Web SDK version for a service (admin-only, SSE stream)."""
    token = body.token or _get_fastly_token(service_id)

    try:
        available = await fetch_available_faro_versions()
    except ValueError as e:
        raise HTTPException(status_code=503, detail=make_error("faro_registry_unavailable", str(e))) from e

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

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.post("/{service_id}/rum/settings")
async def update_rum_settings(
    service_id: str = Path(...),
    body: RumSettingsUpdateRequest = Body(...),
) -> dict[str, Any]:
    """Update RUM capture settings and re-deploy rum-tracker.js (admin-only)."""
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config found for service {service_id}"})

    if "rum" not in cfg:
        cfg["rum"] = {}

    cfg["rum"]["capture_vitals"] = body.capture_vitals
    cfg["rum"]["capture_performance"] = body.capture_performance
    cfg["rum"]["capture_errors"] = body.capture_errors
    cfg["rum"]["capture_events"] = body.capture_events
    cfg["rum"]["custom_condition"] = body.custom_condition

    svcconfig.save_config(service_id, cfg)

    # Re-deploy tracker JS wrapper with the new compiled settings inside it
    token = body.token or _get_fastly_token(service_id)

    from backend.provision.rum_assets import upload_rum_tracker_js

    try:
        res = upload_rum_tracker_js(service_id, token, overwrite=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"Failed to upload RUM tracker wrapper to FOS: {e}"})

    # Reconcile VCL state so the new custom condition is deployed to Fastly
    from backend.provision.declarative.reconciler import reconcile_vcl_state

    try:
        reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": f"Failed to reconcile and deploy updated RUM conditions to Fastly: {e}"},
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
    from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

    res["_debug_queries"] = get_queries()
    res["_debug_sqlite"] = list(get_sqlite_queries())
    res["_debug_calls"] = get_tracked_calls()
    res["_is_cached"] = False
    return res


@router.get("/{service_id}/rum/beacon-health")
async def rum_beacon_health(
    request: Request,
    ctx: RequestContext = Depends(build_request_context),
) -> dict[str, Any]:
    """Check if RUM beacons are arriving (validation endpoint for setup).
    Queries DuckDB views using execute_with_stale_view_retry.
    """
    service_id = ctx.service_id
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    if not cfg.get("rum_enabled", False) and not rum_cfg.get("enabled", False):
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

        def _get_health(con):
            one_hour_ago = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=1)
            from backend.utils.telemetry import track_query

            # Count recent beacons in the last 1 hour
            with track_query(
                con,
                "SELECT COUNT(*) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors) WHERE timestamp > ?",
                [one_hour_ago],
                "rum_health_recent",
            ) as cur_recent:
                recent_count = cur_recent.fetchone()[0] or 0

            # Get max timestamp overall
            with track_query(
                con,
                "SELECT MAX(timestamp) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors)",
                [],
                "rum_health_last",
            ) as cur_last:
                last_dt = cur_last.fetchone()[0]

            return recent_count, last_dt

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            recent_count, last_dt = execute_with_stale_view_retry(rum_con, rum_source, _get_health)

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


@router.get("/{service_id}/rum/analytics")
async def rum_analytics(
    request: Request,
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    filters: str | None = Query(None),
    ctx: RequestContext = Depends(build_request_context),
) -> dict[str, Any]:
    """Retrieve parsed RUM analytics from DuckDB views with high-fidelity deterministic mock fallback.
    Wraps execution with execute_with_stale_view_retry.
    """
    service_id = ctx.service_id

    # 1. Clamp timebounds against analyst session limits
    start_time, end_time = ctx.clamp(start_time, end_time)

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
            pass

    from backend.repositories.utils.filters import build_where_clause

    # Build parameterized where clause using start_time, end_time, and filters
    params, where_sql = build_where_clause(start_time, end_time, parsed_filters)

    rum_source = rum_source_for(ctx.source)

    try:

        def _get_analytics(con):
            from backend.utils.telemetry import track_query

            # A. Check if any data exists at all
            with track_query(
                con,
                """
                SELECT CASE WHEN EXISTS (SELECT 1 FROM client_vitals) OR EXISTS (SELECT 1 FROM client_errors) THEN 1 ELSE 0 END
                """,
                [],
                "rum_any_data",
            ) as cur_any:
                if not cur_any.fetchone()[0]:
                    return {"no_data": True}

            distinct_id = "COALESCE(NULLIF(req_id, ''), concat(cid, '_', cast(timestamp as varchar)))"

            # B. Total match count within bounds
            with track_query(
                con,
                f"""
                SELECT COUNT(DISTINCT {distinct_id}) FROM (
                    SELECT req_id, cid, timestamp, browser, os, device, pathname FROM client_vitals
                    UNION ALL
                    SELECT req_id, cid, timestamp, browser, os, device, pathname FROM client_errors
                )
                WHERE {where_sql}
                """,
                params,
                "rum_total_beacons",
            ) as cur_total:
                total_beacons = cur_total.fetchone()[0] or 0

            # B2. Pageviews: beacons where there is at least one row in client_vitals that is not an event
            with track_query(
                con,
                f"""
                SELECT COUNT(DISTINCT {distinct_id})
                FROM client_vitals
                WHERE {where_sql} AND metric_name NOT LIKE 'event_%'
                """,
                params,
                "rum_pageviews",
            ) as cur_pvs:
                pageviews = cur_pvs.fetchone()[0] or 0

            # B3. Interactions: beacons where there is at least one custom event row
            with track_query(
                con,
                f"""
                SELECT COUNT(DISTINCT {distinct_id})
                FROM client_vitals
                WHERE {where_sql} AND metric_name LIKE 'event_%'
                """,
                params,
                "rum_interactions",
            ) as cur_ints:
                interactions = cur_ints.fetchone()[0] or 0

            # B4. Errors: beacons from client_errors
            with track_query(
                con,
                f"""
                SELECT COUNT(DISTINCT {distinct_id})
                FROM client_errors
                WHERE {where_sql}
                """,
                params,
                "rum_errors_count",
            ) as cur_errs:
                errors_count = cur_errs.fetchone()[0] or 0

            # C. Vitals metrics, percentiles & distribution
            with track_query(
                con,
                f"""
                SELECT
                    metric_name,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS p75,
                    COUNT(*) AS total_count,
                    COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count,
                    COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count,
                    COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count
                FROM client_vitals
                WHERE {where_sql}
                GROUP BY metric_name
                """,
                params,
                "rum_vitals_summary",
            ) as cur_vitals:
                vitals_rows = cur_vitals.fetchall()

            # D. Environments
            with track_query(
                con,
                f"""
                SELECT COALESCE(browser, 'Unknown'), COUNT(DISTINCT {distinct_id})
                FROM client_vitals
                WHERE {where_sql}
                GROUP BY browser
                """,
                params,
                "rum_browsers_distribution",
            ) as cur_browsers:
                browsers = {row[0]: row[1] for row in cur_browsers.fetchall() if row[0]}

            with track_query(
                con,
                f"""
                SELECT COALESCE(os, 'Unknown'), COUNT(DISTINCT {distinct_id})
                FROM client_vitals
                WHERE {where_sql}
                GROUP BY os
                """,
                params,
                "rum_os_distribution",
            ) as cur_os:
                os_dict = {row[0]: row[1] for row in cur_os.fetchall() if row[0]}

            with track_query(
                con,
                f"""
                SELECT COALESCE(device, 'Unknown'), COUNT(DISTINCT {distinct_id})
                FROM client_vitals
                WHERE {where_sql}
                GROUP BY device
                """,
                params,
                "rum_devices_distribution",
            ) as cur_devices:
                devices = {row[0]: row[1] for row in cur_devices.fetchall() if row[0]}

            # E. Worst pages
            with track_query(
                con,
                f"""
                WITH page_metrics AS (
                    SELECT
                        pathname AS path,
                        COUNT(DISTINCT {distinct_id}) AS views,
                        COALESCE(AVG(metric_value) FILTER (WHERE metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')), 0.0) AS avg_load_time,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('LCP', 'lcp')) AS lcp,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('CLS', 'cls')) AS cls
                    FROM client_vitals
                    WHERE {where_sql} AND pathname IS NOT NULL
                    GROUP BY pathname
                ),
                page_errors AS (
                    SELECT
                        pathname AS path,
                        COUNT(DISTINCT {distinct_id}) AS error_count
                    FROM client_errors
                    WHERE {where_sql} AND pathname IS NOT NULL
                    GROUP BY pathname
                )
                SELECT
                    m.path,
                    m.views,
                    m.avg_load_time,
                    m.lcp,
                    m.cls,
                    COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
                FROM page_metrics m
                LEFT JOIN page_errors e ON m.path = e.path
                ORDER BY error_rate DESC, m.avg_load_time DESC
                LIMIT 5
                """,
                params + params,
                "rum_worst_pages",
            ) as cur_pages:
                worst_pages_rows = cur_pages.fetchall()

            # F. Top Exceptions
            with track_query(
                con,
                f"""
                SELECT
                    error_message,
                    error_file,
                    error_line,
                    error_col,
                    COUNT(*) AS count
                FROM client_errors
                WHERE {where_sql}
                GROUP BY error_message, error_file, error_line, error_col
                ORDER BY count DESC
                LIMIT 3
                """,
                params,
                "rum_top_exceptions",
            ) as cur_errors:
                errors_rows = cur_errors.fetchall()

            # G. Trend buckets
            with track_query(
                con,
                f"""
                WITH hourly_vitals AS (
                    SELECT
                        DATE_TRUNC('hour', timestamp) AS hour,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('LCP', 'lcp')) AS lcp,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('CLS', 'cls')) AS cls,
                        COUNT(DISTINCT {distinct_id}) AS views,
                        COUNT(DISTINCT {distinct_id}) FILTER (WHERE metric_name NOT LIKE 'event_%') AS pageviews,
                        COUNT(DISTINCT {distinct_id}) FILTER (WHERE metric_name LIKE 'event_%') AS interactions
                    FROM client_vitals
                    WHERE {where_sql}
                    GROUP BY hour
                ),
                hourly_errors AS (
                    SELECT
                        DATE_TRUNC('hour', timestamp) AS hour,
                        COUNT(DISTINCT {distinct_id}) AS errors
                    FROM client_errors
                    WHERE {where_sql}
                    GROUP BY hour
                )
                SELECT
                    COALESCE(v.hour, e.hour) AS hour_ts,
                    v.lcp,
                    v.cls,
                    COALESCE(v.pageviews, 0) AS pageviews,
                    COALESCE(v.interactions, 0) AS interactions,
                    COALESCE(e.errors, 0) AS errors,
                    COALESCE(e.errors, 0) * 100.0 / NULLIF(COALESCE(v.views, 0) + COALESCE(e.errors, 0), 0) AS error_rate
                FROM hourly_vitals v
                FULL OUTER JOIN hourly_errors e ON v.hour = e.hour
                ORDER BY hour_ts DESC
                """,
                params + params,
                "rum_hourly_trends",
            ) as cur_trends:
                trends_rows = cur_trends.fetchall()

            return {
                "no_data": False,
                "total_beacons": total_beacons,
                "pageviews": pageviews,
                "interactions": interactions,
                "errors_count": errors_count,
                "vitals_rows": vitals_rows,
                "browsers": browsers,
                "os": os_dict,
                "devices": devices,
                "worst_pages_rows": worst_pages_rows,
                "errors_rows": errors_rows,
                "trends_rows": trends_rows,
            }

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
        logger.error(f"[rum] Analytics query failed for {service_id}: {e}", exc_info=True)
        from fastapi import HTTPException

        raise HTTPException(
            status_code=500,
            detail={
                "error": "rum_analytics_failed",
                "message": f"RUM analytics query failed: {str(e)}",
            },
        )


@router.get("/{service_id}/rum/live-events")
async def rum_live_events(
    ctx: RequestContext = Depends(build_request_context),
) -> list[dict[str, Any]]:
    """Fetch recent live beacons stream to feed frontend ticker.
    Queries unified view records in DuckDB using execute_with_stale_view_retry.
    """
    service_id = ctx.service_id
    rum_source = rum_source_for(ctx.source)

    try:

        def _get_live_events(con):
            from backend.utils.telemetry import track_query

            with track_query(
                con,
                """
                SELECT
                    'pageview' AS type,
                    timestamp,
                    pathname,
                    metric_name,
                    metric_value,
                    metric_rating,
                    browser,
                    os,
                    device,
                    cid,
                    req_id,
                    CAST(NULL AS VARCHAR) AS error_message
                FROM client_vitals
                UNION ALL
                SELECT
                    'error' AS type,
                    timestamp,
                    pathname,
                    CAST(NULL AS VARCHAR) AS metric_name,
                    CAST(NULL AS DOUBLE) AS metric_value,
                    CAST(NULL AS VARCHAR) AS metric_rating,
                    browser,
                    os,
                    device,
                    cid,
                    req_id,
                    error_message
                FROM client_errors
                ORDER BY timestamp DESC
                LIMIT 10
                """,
                [],
                "rum_live_events",
            ) as cur:
                return cur.fetchall()

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            rows = execute_with_stale_view_retry(rum_con, rum_source, _get_live_events)

        events = []
        for etype, ts, path, mname, mval, mrating, browser, os, device, cid, req_id, err_msg in rows:
            desc = "Page loaded successfully"
            if etype == "error":
                desc = err_msg or "JS Error"
            elif mname:
                metric_name_upper = mname.upper()
                val_str = f": {mval}" if mval is not None else ""
                desc = f"Metric {metric_name_upper}{val_str}"
                if mrating:
                    desc += f" ({mrating.upper()})"

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
                    },
                }
            )

        return events
    except Exception as e:
        logger.error(f"[rum] Failed to fetch live events for {service_id}: {e}")
        return []
