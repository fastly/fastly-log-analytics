"""High-scale control plane, facts, aggregates, exports, and historical job endpoints."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from backend.core import metadata
from backend.core.request_context import RequestContext, build_request_context, require_admin
from backend.core.request_telemetry import HighScaleSections
from backend.core.share_db import mask_ip
from backend.high_scale._common import raw_query_producer
from backend.high_scale.aggregate_query import (
    DEFAULT_AGGREGATE_DIMENSIONS,
    AggregateRequest,
    query_clickhouse_aggregate,
)
from backend.high_scale.archive import (
    ExportJob,
    ExportManager,
    HistoricalJobManager,
    get_export_manager,
    get_historical_job_manager,
)
from backend.high_scale.ingest_controller import (
    HighScaleService,
    HighScaleServiceRegistryProtocol,
    get_high_scale_service_registry,
)
from backend.high_scale.query_service import (
    QueryIntent,
    QueryRouter,
    QueryTier,
    query_cmcd_facts,
    query_request_facts,
    query_rum_facts,
)
from backend.high_scale.query_service import QueryRequest as RouterQueryRequest
from backend.models.admin import (
    HighScaleAggregateRequest,
    HighScaleAggregateResponse,
    HighScaleAggregateValue,
    HighScaleExportCancelResponse,
    HighScaleExportRequest,
    HighScaleExportResponse,
    HighScaleHistoricalJobCancelResponse,
    HighScaleHistoricalJobResponse,
    HighScaleQueryPlanResponse,
    HighScaleQueryRequest,
    HighScaleQueryResponse,
    HighScaleRequestFactRequest,
    HighScaleRequestFactResponse,
    HighScaleRumFactResponse,
    QueryResponseMetadataResponse,
    ServingWatermarkResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES, ErrorEnvelope
from backend.utils.auth import mask_ips_for
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import make_error, query_errors

high_scale_router = APIRouter(prefix="/api/high-scale", tags=["high-scale"], responses=DEFAULT_ERROR_RESPONSES)

_export_owners: BoundedTTLCache = BoundedTTLCache(maxsize=1024, ttl_seconds=3600.0)
CacheRegistry.register("high_scale._export_owners", _export_owners)
_export_owners_lock = threading.Lock()

_REQUEST_FACTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=15.0)
CacheRegistry.register("high_scale._REQUEST_FACTS_CACHE", _REQUEST_FACTS_CACHE)


def _export_response(service_id: str, job: ExportJob, *, cancelled: bool | None = None) -> Any:
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


def _clamp_and_parse_window(
    ctx: RequestContext, start_time: str | None, end_time: str | None
) -> tuple[datetime, datetime]:
    start_raw, end_raw = ctx.clamp(start_time, end_time)
    start = parse_iso_utc(start_raw)
    end = parse_iso_utc(end_raw)
    if start is None or end is None:
        raise ValueError("start_time and end_time are required ISO-8601 timestamps")
    return start, end


def _serialize_export_row(row: dict[str, Any]) -> bytes:
    return json.dumps(row, default=str, sort_keys=True, separators=(",", ":")).encode("utf-8")


@high_scale_router.post(
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
) -> Any:
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True, mismatch_on_scope=True)
    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)
    rows = _fetch_facts_page(service, req.domain, start=start, end=end, limit=req.limit, cursor=req.cursor).rows
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


@high_scale_router.get(
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
) -> Any:
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True, mismatch_on_scope=True)
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


@high_scale_router.post(
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
) -> Any:
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True, mismatch_on_scope=True)
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


def _execute_request_facts_query(
    ctx: RequestContext,
    req: HighScaleRequestFactRequest,
    registry: HighScaleServiceRegistryProtocol,
    *,
    default_window_if_missing: bool,
) -> HighScaleRequestFactResponse:
    from datetime import UTC, timedelta

    from backend.utils.date_utils import iso_z

    req_start = req.start_time
    req_end = req.end_time
    if default_window_if_missing and (not req_start or not req_end):
        now_dt = datetime.now(UTC).replace(second=0, microsecond=0)
        if not req_end:
            req_end = iso_z(now_dt)
        if not req_start:
            req_start = iso_z(now_dt - timedelta(hours=24))

    start, end = _clamp_and_parse_window(ctx, req_start, req_end)

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

    import os

    from backend.repositories.utils.response_cache import coalesce

    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    cache_key = (
        ctx.service_id,
        start.isoformat(),
        end.isoformat(),
        int(req.limit),
        req.cursor or "",
        str(req.status if req.status is not None else ""),
        str(req.domain or "").strip(),
        str(req.pop or "").strip(),
        str(req.country or "").strip(),
        str(req.batch_id or "").strip(),
        bool(mask_ips_for(ctx.analyst_session)),
        "clickhouse",
    )
    if use_cache:
        cached_resp = _REQUEST_FACTS_CACHE.get(cache_key)
        if cached_resp is not None:
            return cached_resp.model_copy(update={"is_cached": True})

    def _compute_ch() -> HighScaleRequestFactResponse:
        if use_cache and (cached_inner := _REQUEST_FACTS_CACHE.get(cache_key)) is not None:
            return cached_inner.model_copy(update={"is_cached": True})
        page = query_request_facts(
            service.client,
            service_id=ctx.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=service.watermark(),
            limit=req.limit,
            cursor=req.cursor,
            status=req.status,
            domain_filter=req.domain,
            pop=req.pop,
            country=req.country,
            batch_id=req.batch_id,
        )
        rows = [dict(row) for row in page.rows]
        for row in rows:
            cf = row.get("custom_fields")
            if isinstance(cf, dict):
                for k in ("status", "pop", "host"):
                    if k not in row and cf.get(k) is not None:
                        row[k] = cf[k]
        _mask_client_ip_rows(rows, ctx)

        meta = page.metadata
        resp_out: HighScaleRequestFactResponse = HighScaleRequestFactResponse.with_telemetry(
            rows=rows,
            next_cursor=page.next_cursor,
            metadata=_build_query_metadata_response(meta, mode="clickhouse"),
        )
        if use_cache:
            _REQUEST_FACTS_CACHE[cache_key] = resp_out
        return resp_out

    if use_cache:
        resp_final, is_leader = coalesce(f"hs_req_facts:{cache_key}", _compute_ch)
        if not is_leader:
            return resp_final.model_copy(update={"is_cached": True})
        return resp_final
    return _compute_ch()


@high_scale_router.get(
    "/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def get_request_facts_root(
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
    status: str | None = None,
    domain: str | None = None,
    pop: str | None = None,
    country: str | None = None,
    batch_id: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> HighScaleRequestFactResponse:
    req = HighScaleRequestFactRequest(
        start_time=start_time,
        end_time=end_time,
        limit=max(1, min(500, limit)),
        cursor=cursor,
        status=status,
        domain=domain,
        pop=pop,
        country=country,
        batch_id=batch_id,
    )
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        default_window_if_missing=True,
    )


@high_scale_router.post(
    "/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def post_request_facts_root(
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> HighScaleRequestFactResponse:
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        default_window_if_missing=True,
    )


@high_scale_router.post(
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
) -> HighScaleRequestFactResponse:
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        default_window_if_missing=False,
    )


def _build_query_metadata_response(
    metadata_obj: Any,
    *,
    mode: str | None = None,
) -> QueryResponseMetadataResponse:
    kwargs: dict[str, Any] = {
        "status": metadata_obj.status,
        "exact": metadata_obj.exact,
        "coverage": metadata_obj.coverage,
        "freshness_lag_seconds": metadata_obj.freshness_lag_seconds,
        "watermark": ServingWatermarkResponse(
            service_id=metadata_obj.watermark.service_id,
            domain=metadata_obj.watermark.domain,
            owner_epoch=metadata_obj.watermark.owner_epoch,
            coverage_start=metadata_obj.watermark.coverage_start,
            coverage_end=metadata_obj.watermark.coverage_end,
            last_accepted_cursor=metadata_obj.watermark.last_accepted_cursor,
            last_archived_event_id=metadata_obj.watermark.last_archived_event_id,
            last_visible_event_id=metadata_obj.watermark.last_visible_event_id,
            exact=metadata_obj.watermark.exact,
        ),
        "approximation_error": metadata_obj.approximation_error,
        "error": metadata_obj.error,
    }
    if mode is not None:
        kwargs["mode"] = mode
    return QueryResponseMetadataResponse(**kwargs)


def _resolve_scoped_high_scale_service(
    service_id: str,
    ctx: RequestContext,
    registry: HighScaleServiceRegistryProtocol,
    *,
    check_binding: bool = True,
    mismatch_on_scope: bool = False,
) -> HighScaleService:
    if service_id != ctx.service_id:
        if mismatch_on_scope:
            raise HTTPException(
                status_code=409,
                detail=make_error(
                    "high_scale_service_mismatch",
                    "High-scale service binding does not match the requested service",
                ),
            )
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    service = registry.resolve(ctx.service_id)
    if service is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_service_not_configured", "Service is not registered for high-scale queries"),
        )
    if check_binding and service.service_id != ctx.service_id:
        raise HTTPException(
            status_code=409,
            detail=make_error(
                "high_scale_service_mismatch", "High-scale service binding does not match the requested service"
            ),
        )
    return service


@high_scale_router.post(
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
) -> Any:
    if service_id != ctx.service_id:
        raise HTTPException(status_code=403, detail=make_error("service_access_denied", "Service access denied"))
    if domain not in {"rum_vitals", "rum_errors"}:
        raise HTTPException(status_code=404, detail=make_error("not_found", "RUM domain not found"))
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True)

    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)

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
    rows = _mask_client_ip_rows([dict(row) for row in page.rows], ctx)
    return HighScaleRumFactResponse.with_telemetry(
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
    )


@high_scale_router.post(
    "/services/{service_id}/cmcd-facts",
    response_model=HighScaleRequestFactResponse,
)
@query_errors()
def cmcd_facts(
    service_id: str,
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> Any:
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)
    trace = HighScaleSections(ctx.service_id, "cmcd")
    try:
        watermark = trace.run(
            "watermark",
            lambda: service.watermark_for("cmcd"),
            source="projected_fact",
            activity="overhead",
        )
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "CMCD is not configured for this service"),
        ) from None
    page = trace.run(
        "facts",
        lambda: query_cmcd_facts(
            service.client,
            service_id=ctx.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=watermark,
            limit=req.limit,
            cursor=req.cursor,
        ),
        source="projected_fact",
    )
    return HighScaleRequestFactResponse.with_telemetry(
        rows=_mask_client_ip_rows([dict(row) for row in page.rows], ctx),
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
        section_timings=trace.timings(),
    )


@high_scale_router.post(
    "/services/{service_id}/aggregates",
    response_model=HighScaleAggregateResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def aggregates(
    service_id: str,
    req: HighScaleAggregateRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> Any:
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True)

    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)

    try:
        watermark = service.watermark_for(req.domain)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "Domain is not configured for this service"),
        ) from None

    dimension = req.dimension or DEFAULT_AGGREGATE_DIMENSIONS[req.domain]
    response = query_clickhouse_aggregate(
        service.client,
        AggregateRequest(service_id=ctx.service_id, domain=req.domain, start=start, end=end, dimension=dimension),
        watermark=watermark,
    )
    top_values = list(response.top_values)
    if dimension == "client_ip" and mask_ips_for(ctx.analyst_session):
        top_values = [(mask_ip(value), count) for value, count in top_values]
    return HighScaleAggregateResponse.with_telemetry(
        service_id=response.service_id,
        domain=response.domain,
        dimension=dimension,
        request_count=response.request_count,
        top_values=[HighScaleAggregateValue(value=value, count=count) for value, count in top_values],
        coverage=response.coverage,
        freshness_lag_seconds=response.freshness_lag_seconds,
        exact=response.exact,
        approximation_error=response.approximation_error,
    )


def _fetch_facts_page(
    service: HighScaleService,
    domain: str,
    *,
    start: datetime,
    end: datetime,
    limit: int,
    cursor: str | None,
) -> Any:
    watermark = service.watermark_for(domain)
    if domain == "request":
        return query_request_facts(
            service.client,
            service_id=service.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=watermark,
            limit=limit,
            cursor=cursor,
        )
    if domain == "cmcd":
        return query_cmcd_facts(
            service.client,
            service_id=service.service_id,
            start=start,
            end=end,
            cursor_secret=service.cursor_secret,
            watermark=watermark,
            limit=limit,
            cursor=cursor,
        )
    return query_rum_facts(
        service.client,
        service_id=service.service_id,
        domain=domain,
        start=start,
        end=end,
        cursor_secret=service.cursor_secret,
        watermark=watermark,
        limit=limit,
        cursor=cursor,
    )


def _mask_client_ip_rows(rows: list[dict[str, Any]], ctx: RequestContext) -> list[dict[str, Any]]:
    if mask_ips_for(ctx.analyst_session):
        for row in rows:
            value = row.get("client_ip")
            if isinstance(value, str):
                row["client_ip"] = mask_ip(value)
    return rows


@high_scale_router.post(
    "/services/{service_id}/queries",
    response_model=HighScaleQueryResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def submit_query(
    service_id: str,
    req: HighScaleQueryRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    jobs: HistoricalJobManager = Depends(get_historical_job_manager),
) -> Any:
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=True)

    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)

    plan = QueryRouter().plan(
        RouterQueryRequest(
            service_id=ctx.service_id, domain=req.domain, start=start, end=end, intent=QueryIntent.RAW, limit=req.limit
        )
    )
    plan_response = HighScaleQueryPlanResponse(
        tier=plan.tier.value,
        sources=list(plan.sources),
        queued=plan.queued,
        bounded=plan.bounded,
        max_rows=plan.max_rows,
        reason=plan.reason,
    )

    if plan.tier is QueryTier.COLD_JOB:
        if service.manifest_catalog is None or service.archive is None:
            raise HTTPException(
                status_code=404,
                detail=make_error(
                    "high_scale_cold_query_not_configured",
                    "Cold-tier historical queries are not configured for this service",
                ),
            )
        producer = raw_query_producer(
            service.manifest_catalog,
            service.archive,
            service_id=ctx.service_id,
            domain=req.domain,
            start=start,
            end=end,
        )
        job_id = jobs.submit(plan, producer)
        return HighScaleQueryResponse.with_telemetry(
            service_id=ctx.service_id,
            domain=req.domain,
            plan=plan_response,
            rows=None,
            next_cursor=None,
            metadata=None,
            job_id=job_id,
        )

    try:
        page = _fetch_facts_page(service, req.domain, start=start, end=end, limit=req.limit, cursor=req.cursor)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=make_error("high_scale_domain_not_configured", "Domain is not configured for this service"),
        ) from None
    rows = _mask_client_ip_rows([dict(row) for row in page.rows], ctx)
    return HighScaleQueryResponse.with_telemetry(
        service_id=ctx.service_id,
        domain=req.domain,
        plan=plan_response,
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
        job_id=None,
    )


@high_scale_router.get(
    "/services/{service_id}/queries/{job_id}",
    response_model=HighScaleHistoricalJobResponse,
)
@query_errors()
def query_job_status(
    service_id: str,
    job_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    jobs: HistoricalJobManager = Depends(get_historical_job_manager),
) -> Any:
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    job = jobs.get(job_id)
    if job is None or job.plan.service_id != ctx.service_id:
        raise HTTPException(status_code=404, detail=make_error("historical_job_not_found", "Historical job not found"))
    rows = _mask_client_ip_rows([dict(row) for row in job.rows], ctx)
    return HighScaleHistoricalJobResponse.with_telemetry(
        job_id=job.job_id,
        state=job.state.value,
        rows=rows,
        row_count=job.row_count,
        output_bytes=job.output_bytes,
        truncated=job.truncated,
        error=job.error,
        created_at=job.created_at,
        expires_at=job.expires_at,
        terminal_at=job.terminal_at,
    )


@high_scale_router.post(
    "/services/{service_id}/queries/{job_id}/cancel",
    response_model=HighScaleHistoricalJobCancelResponse,
    dependencies=[Depends(require_admin)],
)
@query_errors()
def cancel_query_job(
    service_id: str,
    job_id: str,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
    jobs: HistoricalJobManager = Depends(get_historical_job_manager),
) -> Any:
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    job = jobs.get(job_id)
    if job is None or job.plan.service_id != ctx.service_id:
        raise HTTPException(status_code=404, detail=make_error("historical_job_not_found", "Historical job not found"))
    cancelled = jobs.cancel(job_id)
    return HighScaleHistoricalJobCancelResponse.with_telemetry(job_id=job_id, cancelled=cancelled)


router = high_scale_router
