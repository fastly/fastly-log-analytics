"""RUM dynamic JS asset serving (`/js/rum.js`, `/js/faro-sdk.js`) and `/rum-beacon` fallback."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Request, Response

from backend import config as svcconfig
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.router_utils import raise_internal

logger = logging.getLogger(__name__)

# APIRouter without a prefix for serving dynamic JS assets and ingesting beacons
asset_router = APIRouter(tags=["rum-assets"], responses=DEFAULT_ERROR_RESPONSES)

_FARO_BUNDLE_CACHE: dict[str, bytes] = {}
CacheRegistry.register("rum._FARO_BUNDLE_CACHE", _FARO_BUNDLE_CACHE)


@asset_router.api_route(
    "/rum-beacon",
    methods=["GET", "POST", "HEAD", "OPTIONS"],
    include_in_schema=False,
)
async def receive_rum_beacon_fallback() -> Response:
    """No-op 204 endpoint when `/rum-beacon` reaches the origin directly (local dev / direct tunnel)."""
    return Response(status_code=204)


def _resolve_rum_js_service_id(request: Request, service_id: str | None) -> str | None:
    """Resolve active service ID from query param, header, cookie, or default configured service."""
    sid = service_id
    if not sid:
        sid = request.headers.get("x-service-id")
    if not sid:
        sid = request.cookies.get("active_service_id")

    if sid and not svcconfig.load_config(sid):
        sid = None

    if not sid:
        sid = svcconfig.get_active_service_id(fallback_to_first=True)
    return sid


@asset_router.get("/js/rum.js")
async def get_rum_tracker(request: Request, service_id: str | None = None):
    def _build_tracker() -> tuple[str | None, str | None]:
        sid = _resolve_rum_js_service_id(request, service_id)
        if not sid:
            return None, None
        from backend.provision.rum_assets import generate_rum_tracker_js

        return sid, generate_rum_tracker_js(sid)

    try:
        sid, js_code = await asyncio.to_thread(_build_tracker)
    except Exception as e:
        logger.error("Failed to generate dynamic RUM tracker JS: %s", e, exc_info=True)
        return Response(
            content="/* Error generating tracker JS */",
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    if not sid or js_code is None:
        return Response(
            content="/* RUM tracker disabled (no active services) */",
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    return Response(
        content=js_code,
        media_type="application/javascript",
        headers={"Cache-Control": "private, no-cache, must-revalidate"},
    )


@asset_router.get("/js/faro-sdk.js")
async def get_faro_sdk(request: Request, service_id: str | None = None):
    sid = _resolve_rum_js_service_id(request, service_id)

    version = DEFAULT_FARO_VERSION
    if sid:
        cfg = svcconfig.load_config(sid)
        if cfg and "rum" in cfg:
            version = cfg["rum"].get("faro_version") or DEFAULT_FARO_VERSION

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
