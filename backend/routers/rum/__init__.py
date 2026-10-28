"""Real User Monitoring (RUM) router package — provisioning, analytics, live events, and JS assets."""

from __future__ import annotations

from fastapi import APIRouter

from backend import config as svcconfig
from backend.core.duckdb import rum_source_for
from backend.core.faro_versions import (
    DEFAULT_FARO_VERSION,
    fetch_available_faro_versions,
    get_faro_versions_cached,
)
from backend.core.request_context import (
    RequestContext,
    _ConnectionHolder,
    build_request_context,
    get_service_id,
    require_admin,
    require_service_access,
)
from backend.core.storage import execute_with_stale_view_retry
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.provision.orchestrator import run_with_events
from backend.provision.rum_orchestrator_v2 import (
    disable_rum,
    enable_rum,
    legacy_rum_vcl_fingerprint,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from backend.repositories import rum as rum_repo
from backend.routers.rum import analytics as _analytics
from backend.routers.rum import beacon_ingest as _beacon_ingest
from backend.routers.rum import provisioning as _provisioning
from backend.routers.rum.analytics import (
    _RUM_FILTERS_MESSAGE,
    _RUM_RAW_WINDOW_MAX_S,
    _inject_telemetry,
    _live_events_cache,
    _parse_rum_filter_specs,
    _rum_beacon_health_sync,
    repo,
    rum_analytics,
    rum_analytics_router,
    rum_beacon_health,
    rum_live_events,
)
from backend.routers.rum.beacon_ingest import (
    _FARO_BUNDLE_CACHE,
    _resolve_rum_js_service_id,
    asset_router,
    get_faro_sdk,
    get_rum_tracker,
    receive_rum_beacon_fallback,
)
from backend.routers.rum.provisioning import (
    _RUM_SETTING_KEYS,
    _normalize_sec,
    _reload_rum_scheduler_and_cfg,
    _require_rum_status_access,
    _rum_status_sync,
    disable_rum_handler,
    enable_rum_handler,
    rum_provisioning_router,
    rum_status,
    rum_versions,
    update_rum_settings,
    upgrade_rum_handler,
)
from backend.utils.module_shim import install_shim_module
from backend.utils.router_utils import (
    correlation_id,
    load_service_config,
    make_error,
    raise_internal,
    require_fastly_token,
    stored_fastly_token,
)

rum_router = APIRouter(prefix="/api/services", tags=["rum"], responses=DEFAULT_ERROR_RESPONSES)
rum_router.include_router(rum_provisioning_router)
rum_router.include_router(rum_analytics_router)

router = APIRouter()
router.include_router(rum_router)
router.include_router(asset_router)

install_shim_module(
    __name__,
    (_beacon_ingest, _provisioning, _analytics),
    skip_attrs=frozenset({"router", "rum_router", "asset_router"}),
    bidirectional_seed=True,
)

__all__ = [
    "DEFAULT_ERROR_RESPONSES",
    "DEFAULT_FARO_VERSION",
    "RequestContext",
    "_ConnectionHolder",
    "_FARO_BUNDLE_CACHE",
    "_RUM_FILTERS_MESSAGE",
    "_RUM_RAW_WINDOW_MAX_S",
    "_RUM_SETTING_KEYS",
    "_inject_telemetry",
    "_live_events_cache",
    "_normalize_sec",
    "_parse_rum_filter_specs",
    "_reload_rum_scheduler_and_cfg",
    "_require_rum_status_access",
    "_resolve_rum_js_service_id",
    "_rum_beacon_health_sync",
    "_rum_status_sync",
    "asset_router",
    "build_request_context",
    "correlation_id",
    "disable_rum",
    "disable_rum_handler",
    "enable_rum",
    "enable_rum_handler",
    "execute_with_stale_view_retry",
    "fetch_available_faro_versions",
    "get_faro_sdk",
    "get_faro_versions_cached",
    "get_rum_tracker",
    "get_service_id",
    "legacy_rum_vcl_fingerprint",
    "load_service_config",
    "make_error",
    "raise_internal",
    "receive_rum_beacon_fallback",
    "repo",
    "require_admin",
    "require_fastly_token",
    "require_service_access",
    "router",
    "rum_analytics",
    "rum_analytics_router",
    "rum_beacon_health",
    "rum_live_events",
    "rum_provisioning_router",
    "rum_repo",
    "rum_router",
    "rum_source_for",
    "rum_status",
    "rum_vcl_fingerprint",
    "rum_versions",
    "run_with_events",
    "stored_fastly_token",
    "svcconfig",
    "update_rum_settings",
    "upgrade_faro_version",
    "upgrade_rum_handler",
]
