"""Explicit high-scale request-fact query surface.

This namespace is intentionally separate from the standard and
high-throughput analytics routes. Analysts may use the read-only POST query
through ``_ANALYST_ALLOWED_WRITE_PREFIXES`` in ``remote_access.py``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.core.share_db.validation import mask_ip
from backend.high_scale.query_service import query_request_facts
from backend.high_scale.registry import HighScaleServiceRegistryProtocol, get_high_scale_service_registry
from backend.models.errors import DEFAULT_ERROR_RESPONSES, ErrorEnvelope
from backend.models.high_scale import (
    HighScaleRequestFactRequest,
    HighScaleRequestFactResponse,
    QueryResponseMetadataResponse,
    ServingWatermarkResponse,
)
from backend.utils.auth import mask_ips_for
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import make_error, query_errors

router = APIRouter(prefix="/api/high-scale", tags=["high-scale"], responses=DEFAULT_ERROR_RESPONSES)


@router.post(
    "/services/{service_id}/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def request_facts(
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    service = registry.resolve(ctx.service_id)
    if service is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_service_not_configured", "Service is not registered for high-scale queries"),
        )
    if service.service_id != ctx.service_id:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "high_scale_service_mismatch", "High-scale service binding does not match the requested service"
            ),
        )

    start_raw, end_raw = ctx.clamp(req.start_time, req.end_time)
    start = parse_iso_utc(start_raw)
    end = parse_iso_utc(end_raw)
    if start is None or end is None:
        raise ValueError("start_time and end_time are required ISO-8601 timestamps")

    page = query_request_facts(
        service.client,
        service_id=ctx.service_id,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=service.watermark(),
        limit=req.limit,
        cursor=req.cursor,
    )
    rows = [dict(row) for row in page.rows]
    if mask_ips_for(ctx.analyst_session):
        for row in rows:
            value = row.get("client_ip")
            if isinstance(value, str):
                row["client_ip"] = mask_ip(value)

    metadata = page.metadata
    return HighScaleRequestFactResponse.with_telemetry(
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=QueryResponseMetadataResponse(
            status=metadata.status,
            exact=metadata.exact,
            coverage=metadata.coverage,
            freshness_lag_seconds=metadata.freshness_lag_seconds,
            watermark=ServingWatermarkResponse(
                service_id=metadata.watermark.service_id,
                domain=metadata.watermark.domain,
                owner_epoch=metadata.watermark.owner_epoch,
                coverage_start=metadata.watermark.coverage_start,
                coverage_end=metadata.watermark.coverage_end,
                last_accepted_cursor=metadata.watermark.last_accepted_cursor,
                last_archived_event_id=metadata.watermark.last_archived_event_id,
                last_visible_event_id=metadata.watermark.last_visible_event_id,
                exact=metadata.watermark.exact,
            ),
            approximation_error=metadata.approximation_error,
            error=metadata.error,
        ),
    )
