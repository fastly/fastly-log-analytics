"""RUM analytics, beacon health, and live events endpoints."""

from __future__ import annotations

import asyncio
import logging
import os
from types import SimpleNamespace
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend import config as svcconfig
from backend.core.request_context import (
    RequestContext,
    build_request_context,
)
from backend.models.analytics import (
    RumAnalyticsResponse,
    RumBeaconHealthResponse,
    RumLiveEventItem,
)
from backend.repositories.utils.response_cache import bucket_time_to_minute, digest_cache_key
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.router_utils import make_error

logger = logging.getLogger(__name__)

_RUM_RAW_WINDOW_MAX_S = int(os.getenv("RUM_RAW_WINDOW_MAX_S", str(6 * 60 * 60)))

_RUM_FILTERS_MESSAGE = (
    "filters must be a JSON object over browser, os, device, country, pathname, city, region, pop or tls"
)

rum_analytics_router = APIRouter()

_analytics_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=60)
CacheRegistry.register("rum._analytics_cache", _analytics_cache)


def _inject_telemetry(res: dict, *, is_cached: bool = False) -> dict:
    from backend.models.common import _debug_responses_enabled
    from backend.utils.telemetry import debug_envelope

    payload = dict(res)
    if not _debug_responses_enabled():
        return payload
    payload["_is_cached"] = is_cached
    return debug_envelope(payload, gated=False)


def _parse_rum_filter_specs(filters: str | None) -> dict[str, Any]:
    """Parse and validate RUM ``filters`` query parameter into ``FilterSpec`` objects."""
    if not filters:
        return {}
    from backend.high_scale.rum import _FILTER_COLUMNS, parse_rum_filters
    from backend.models.common import FilterSpec
    from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key

    try:
        raw_dict = parse_rum_filters(filters)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc

    parsed_filters: dict[str, Any] = {}
    unsupported: list[str] = []
    for raw_key, spec in raw_dict.items():
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if value is not None and not (isinstance(value, str) and value.strip() == "")
        ]
        column = normalize_filter_key(raw_key)
        if values and column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        if isinstance(spec, dict):
            parsed_filters[raw_key] = FilterSpec(mode=spec.get("mode", "include"), values=spec.get("values", []))
        elif isinstance(spec, FilterSpec):
            parsed_filters[raw_key] = spec
    if unsupported:
        raise HTTPException(status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE))
    return parsed_filters


def _rum_beacon_health_sync(ctx: RequestContext) -> dict[str, Any]:
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import rum_beacon_health as hs_rum_health

        try:
            return hs_rum_health(high_scale_service, source=ctx.source)
        except TypeError:
            return hs_rum_health(high_scale_service)

    service_id = ctx.service_id
    cfg = svcconfig.load_config(service_id) or {}
    if not svcconfig.is_rum_enabled(cfg):
        return {
            "enabled": False,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": "RUM not enabled for this service",
        }

    return {
        "enabled": True,
        "beacon_fire_rate": 0,
        "recent_beacons": 0,
        "last_beacon_time": None,
        "setup_complete": False,
        "message": "Waiting for beacons...",
    }


@rum_analytics_router.get(
    "/{service_id}/rum/beacon-health", response_model=RumBeaconHealthResponse, response_model_exclude_unset=True
)
async def rum_beacon_health(
    request: Request,
    ctx: RequestContext = Depends(build_request_context),
) -> dict[str, Any]:
    """Check if RUM beacons are arriving (validation endpoint for setup)."""
    return _inject_telemetry(await asyncio.to_thread(_rum_beacon_health_sync, ctx))


@rum_analytics_router.get(
    "/{service_id}/rum/analytics", response_model=RumAnalyticsResponse, response_model_exclude_unset=True
)
async def rum_analytics(
    request: Request,
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    filters: str | None = Query(None),
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> dict[str, Any]:
    """Retrieve parsed RUM analytics from ClickHouse."""
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is None:
        raise HTTPException(
            status_code=503,
            detail=make_error("high_scale_unavailable", "ClickHouse query service is not available for this service."),
        )
    from backend.high_scale.rum import parse_rum_filters
    from backend.high_scale.rum import rum_analytics as hs_rum_analytics

    start_time, end_time = ctx.resolve_window(window)
    try:
        parsed_hs_filters = parse_rum_filters(filters)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc
    hs_wm = getattr(high_scale_service, "watermark", None)
    hs_cache_key = digest_cache_key(
        {
            "mode": "hs",
            "svc": ctx.service_id,
            "s": bucket_time_to_minute(start_time) if start_time else "",
            "e": bucket_time_to_minute(end_time) if end_time else "",
            "f": parsed_hs_filters,
            "fn": id(hs_rum_analytics),
            "cl": id(getattr(high_scale_service, "client", None)),
            "wm": repr(hs_wm),
        },
        ctx.source,
    )
    cached_hs = _analytics_cache.get(hs_cache_key)
    if cached_hs is not None:
        return _inject_telemetry(cached_hs, is_cached=True)
    try:
        hs_result = await asyncio.to_thread(
            hs_rum_analytics, high_scale_service, start_time, end_time, parsed_hs_filters
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc
    if not hs_result.get("no_data") and (hs_result.get("beacon_count") or 0) > 0:
        _analytics_cache[hs_cache_key] = dict(hs_result)
    return _inject_telemetry(hs_result, is_cached=False)


_live_events_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=15)
CacheRegistry.register("rum._live_events_cache", _live_events_cache)


@rum_analytics_router.get("/{service_id}/rum/live-events", response_model=list[RumLiveEventItem])
async def rum_live_events(
    start_time: str | None = None,
    end_time: str | None = None,
    filters: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> list[dict[str, Any]]:
    """Fetch recent live beacons stream to feed frontend ticker."""
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is None:
        return []
    from backend.high_scale.rum import parse_rum_filters
    from backend.high_scale.rum import rum_live_events as hs_rum_live

    start_time, end_time = ctx.resolve_window(window)
    try:
        parsed_hs_filters = parse_rum_filters(filters)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc
    hs_wm = getattr(high_scale_service, "watermark", None)
    hs_live_key = digest_cache_key(
        {
            "mode": "hs_live",
            "svc": ctx.service_id,
            "s": bucket_time_to_minute(start_time) if start_time else "",
            "e": bucket_time_to_minute(end_time) if end_time else "",
            "f": parsed_hs_filters,
            "fn": id(hs_rum_live),
            "cl": id(getattr(high_scale_service, "client", None)),
            "wm": repr(hs_wm),
        },
        ctx.source,
    )
    cached_hs_live = _live_events_cache.get(hs_live_key)
    if cached_hs_live:
        return list(cached_hs_live)
    try:
        hs_events = await asyncio.to_thread(
            hs_rum_live, high_scale_service, start_time, end_time, 50, parsed_hs_filters
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc
    if hs_events:
        _live_events_cache[hs_live_key] = hs_events
    return hs_events
