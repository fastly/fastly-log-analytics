"""Explicit high-scale request-fact query surface.

This namespace is intentionally separate from the standard and
high-throughput analytics routes. Analysts may use the read-only POST query
through ``_ANALYST_ALLOWED_WRITE_PREFIXES`` in ``remote_access.py``.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from backend.core import metadata
from backend.core.request_context import RequestContext, build_request_context
from backend.core.share_db.validation import mask_ip
from backend.deps import require_admin
from backend.high_scale.exports import ExportJob, ExportManager, get_export_manager
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts
from backend.high_scale.registry import HighScaleServiceRegistryProtocol, get_high_scale_service_registry
from backend.models.errors import DEFAULT_ERROR_RESPONSES, ErrorEnvelope
from backend.models.high_scale import (
    HighScaleExportCancelResponse,
    HighScaleExportRequest,
    HighScaleExportResponse,
    HighScaleRequestFactRequest,
    HighScaleRequestFactResponse,
    HighScaleRumFactResponse,
    QueryResponseMetadataResponse,
    ServingWatermarkResponse,
)
from backend.utils.auth import mask_ips_for
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import make_error, query_errors

router = APIRouter(prefix="/api/high-scale", tags=["high-scale"], responses=DEFAULT_ERROR_RESPONSES)

_export_owners: dict[str, str] = {}
_export_owners_lock = threading.Lock()


def _resolve_export_service(
    service_id: str,
    ctx: RequestContext,
    registry: HighScaleServiceRegistryProtocol,
):
    if service_id != ctx.service_id:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "high_scale_service_mismatch", "High-scale service binding does not match the requested service"
            ),
        )
    service = registry.resolve(service_id)
    if service is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_service_not_configured", "Service is not registered for high-scale queries"),
        )
    if service.service_id != service_id:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "high_scale_service_mismatch", "High-scale service binding does not match the requested service"
            ),
        )
    return service


def _export_response(service_id: str, job: ExportJob, *, cancelled: bool | None = None):
    payload = {
        "export_id": job.job_id,
        "service_id": service_id,
        "state": job.state.value,
        "rows_written": job.rows_written,
        "bytes_written": job.bytes_written,
        "error": job.error,
    }
    if cancelled is None:
        return HighScaleExportResponse.with_telemetry(**payload)
    return HighScaleExportCancelResponse.with_telemetry(cancelled=cancelled, **payload)


def _query_export_rows(
    req: HighScaleExportRequest,
    service,
    *,
    start: datetime,
    end: datetime,
):
    if req.domain == "request":
        return query_request_facts(
            service.client,
            service_id=service.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=service.watermark_for("request"),
            limit=req.limit,
            cursor=req.cursor,
        ).rows
    if req.domain == "cmcd":
        return query_cmcd_facts(
            service.client,
            service_id=service.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=service.watermark_for("cmcd"),
            limit=req.limit,
            cursor=req.cursor,
        ).rows
    return query_rum_facts(
        service.client,
        service_id=service.service_id,
        domain=req.domain,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=service.watermark_for(req.domain),
        limit=req.limit,
        cursor=req.cursor,
    ).rows


def _serialize_export_row(row: dict) -> bytes:
    return json.dumps(row, default=str, sort_keys=True, separators=(",", ":")).encode("utf-8")


@router.post(
    "/services/{service_id}/exports",
    response_model=HighScaleExportResponse,
    dependencies=[Depends(require_admin)],
)
@query_errors()
def request_export(
    service_id: str,
    req: HighScaleExportRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    manager: ExportManager = Depends(get_export_manager),
):
    service = _resolve_export_service(service_id, ctx, registry)
    start_raw, end_raw = ctx.clamp(req.start_time, req.end_time)
    start = parse_iso_utc(start_raw)
    end = parse_iso_utc(end_raw)
    if start is None or end is None:
        raise ValueError("start_time and end_time are required ISO-8601 timestamps")
    rows = _query_export_rows(req, service, start=start, end=end)
    export_id = uuid4().hex
    manager.submit(export_id, rows, _serialize_export_row)
    with _export_owners_lock:
        _export_owners[export_id] = service_id
    metadata.record_audit(
        service_id,
        event_type="high_scale_export",
        details={"action": "requested", "export_id": export_id, "domain": req.domain, "row_limit": req.limit},
    )
    return _export_response(service_id, manager.get(export_id))


@router.get(
    "/services/{service_id}/exports/{export_id}",
    response_model=HighScaleExportResponse,
)
@query_errors()
def export_status(
    service_id: str,
    export_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    manager: ExportManager = Depends(get_export_manager),
):
    _resolve_export_service(service_id, ctx, registry)
    with _export_owners_lock:
        owner = _export_owners.get(export_id)
    if owner != service_id:
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found"))
    try:
        job = manager.get(export_id)
    except KeyError:
        with _export_owners_lock:
            _export_owners.pop(export_id, None)
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found")) from None
    return _export_response(service_id, job)


@router.post(
    "/services/{service_id}/exports/{export_id}/cancel",
    response_model=HighScaleExportCancelResponse,
    dependencies=[Depends(require_admin)],
)
@query_errors()
def cancel_export(
    service_id: str,
    export_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    manager: ExportManager = Depends(get_export_manager),
):
    _resolve_export_service(service_id, ctx, registry)
    with _export_owners_lock:
        owner = _export_owners.get(export_id)
    if owner != service_id:
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found"))
    try:
        cancelled = manager.cancel(export_id)
        job = manager.get(export_id)
    except KeyError:
        with _export_owners_lock:
            _export_owners.pop(export_id, None)
        raise HTTPException(status_code=404, detail=make_error("export_not_found", "Export not found")) from None
    if cancelled:
        metadata.record_audit(
            service_id,
            event_type="high_scale_export",
            details={"action": "cancelled", "export_id": export_id},
        )
    return _export_response(service_id, job, cancelled=cancelled)


@router.post(
    "/services/{service_id}/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def request_facts(
    service_id: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
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


@router.post(
    "/services/{service_id}/rum-facts/{domain}",
    response_model=HighScaleRumFactResponse,
)
@query_errors()
def rum_facts(
    service_id: str,
    domain: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    if domain not in {"rum_vitals", "rum_errors"}:
        raise HTTPException(status_code=404, detail=make_error("not_found", "RUM domain not found"))
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

    try:
        watermark = service.watermark_for(domain)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "RUM domain is not configured for this service"),
        ) from None
    page = query_rum_facts(
        service.client,
        service_id=ctx.service_id,
        domain=domain,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=watermark,
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
    return HighScaleRumFactResponse.with_telemetry(
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


@router.post(
    "/services/{service_id}/cmcd-facts",
    response_model=HighScaleRequestFactResponse,
)
@query_errors()
def cmcd_facts(
    service_id: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    service = registry.resolve(ctx.service_id)
    if service is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_service_not_configured", "Service is not registered for high-scale queries"),
        )
    start_raw, end_raw = ctx.clamp(req.start_time, req.end_time)
    start = parse_iso_utc(start_raw)
    end = parse_iso_utc(end_raw)
    if start is None or end is None:
        raise ValueError("start_time and end_time are required ISO-8601 timestamps")
    try:
        watermark = service.watermark_for("cmcd")
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "CMCD is not configured for this service"),
        ) from None
    page = query_cmcd_facts(
        service.client,
        service_id=ctx.service_id,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=watermark,
        limit=req.limit,
        cursor=req.cursor,
    )
    metadata = page.metadata
    return HighScaleRequestFactResponse.with_telemetry(
        rows=[dict(row) for row in page.rows],
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
