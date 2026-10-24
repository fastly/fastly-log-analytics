"""Explicit high-scale request-fact query surface.

This namespace is intentionally separate from the standard
analytics routes. Analysts may use the read-only POST query
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
from backend.high_scale.aggregate_query import (
    DEFAULT_AGGREGATE_DIMENSIONS,
    AggregateRequest,
    query_clickhouse_aggregate,
)
from backend.high_scale.exports import ExportJob, ExportManager, get_export_manager
from backend.high_scale.historical_jobs import HistoricalJobManager, get_historical_job_manager
from backend.high_scale.query_router import QueryIntent, QueryRouter, QueryTier
from backend.high_scale.query_router import QueryRequest as RouterQueryRequest
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts
from backend.high_scale.raw_query import raw_query_producer
from backend.high_scale.registry import (
    HighScaleService,
    HighScaleServiceRegistryProtocol,
    get_high_scale_service_registry,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES, ErrorEnvelope
from backend.models.high_scale import (
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
from backend.utils.auth import mask_ips_for
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.cache_registry import CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import make_error, query_errors

router = APIRouter(prefix="/api/high-scale", tags=["high-scale"], responses=DEFAULT_ERROR_RESPONSES)

_export_owners: BoundedTTLCache = BoundedTTLCache(maxsize=1024, ttl_seconds=3600.0)
CacheRegistry.register("high_scale._export_owners", _export_owners)
_export_owners_lock = threading.Lock()


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


def _clamp_and_parse_window(
    ctx: RequestContext, start_time: str | None, end_time: str | None
) -> tuple[datetime, datetime]:
    start_raw, end_raw = ctx.clamp(start_time, end_time)
    start = parse_iso_utc(start_raw)
    end = parse_iso_utc(end_raw)
    if start is None or end is None:
        raise ValueError("start_time and end_time are required ISO-8601 timestamps")
    return start, end


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


def _normalize_ts_iso(val: object) -> str:
    if isinstance(val, datetime):
        from backend.utils.date_utils import iso_z

        return iso_z(val)
    if val is None:
        return ""
    s = str(val)
    if s.endswith("+00:00"):
        return s[:-6] + "Z"
    return s


def _emulate_standard_request_facts(
    ctx: RequestContext,
    req: HighScaleRequestFactRequest,
    *,
    start: datetime,
    end: datetime,
) -> HighScaleRequestFactResponse:
    import os
    import uuid
    from datetime import UTC

    from backend.core.duckdb import _safe_table_name
    from backend.core.iceberg import execute_with_stale_view_retry
    from backend.high_scale.pagination import KeysetCursor
    from backend.repositories import request_facts as request_facts_repo

    secret = (os.getenv("HIGH_SCALE_CURSOR_SECRET", "").strip() or "standard-emulation-cursor-secret").encode("utf-8")
    decoded = KeysetCursor.decode(req.cursor, secret) if req.cursor else None
    if decoded and (decoded.service_id != ctx.service_id or decoded.domain != "request"):
        raise ValueError("cursor does not belong to this service and domain")

    candidate_tables = [_safe_table_name(str(ctx.source.get("name") or "default"))]
    if "logs" not in candidate_tables:
        candidate_tables.append("logs")

    def _run_emulation(con):
        return request_facts_repo.fetch_request_facts_emulation(
            con,
            candidate_tables,
            req,
            start=start,
            end=end,
            cursor_ts=decoded.timestamp if decoded is not None else None,
            service_id=ctx.service_id,
        )

    raw_rows, cov_start, cov_end = execute_with_stale_view_retry(ctx.con, ctx.source, _run_emulation)

    page_tuples = raw_rows[: req.limit]
    rows: list[dict[str, object]] = []
    mask = mask_ips_for(ctx.analyst_session)

    for idx, tup in enumerate(page_tuples):
        ts_val, country_val, ip_val, url_val, status_val, pop_val, host_val = tup
        ts_iso = _normalize_ts_iso(ts_val)
        ev_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{ctx.service_id}|{ts_iso}|{idx}|{url_val or ''}"))
        ip_str = str(ip_val or "")
        if mask and ip_str:
            ip_str = mask_ip(ip_str)
        status_str = str(status_val or "")
        status_out: int | str = int(status_str) if status_str.isdigit() else status_str
        pop_str = str(pop_val or "")
        host_str = str(host_val or "")
        rows.append(
            {
                "event_id": ev_id,
                "timestamp": ts_iso,
                "service_id": ctx.service_id,
                "source_object_key": "ducklake://local",
                "source_object_version": "v1",
                "line_ordinal": idx,
                "batch_id": "duckdb-emulation",
                "country": str(country_val or ""),
                "client_ip": ip_str,
                "url": str(url_val or ""),
                "status": status_out,
                "pop": pop_str,
                "host": host_str,
                "custom_fields": {
                    "status": status_str,
                    "pop": pop_str,
                    "host": host_str,
                },
                "cmcd": {},
            }
        )

    next_cursor: str | None = None
    if len(raw_rows) > req.limit and rows:
        last_row = rows[-1]
        last_ts = parse_iso_utc(str(last_row["timestamp"])) or end
        next_cursor = KeysetCursor(last_ts, str(last_row["event_id"]), ctx.service_id, "request").encode(secret)

    cov_start_dt = parse_iso_utc(_normalize_ts_iso(cov_start)) if cov_start is not None else start
    cov_end_dt = parse_iso_utc(_normalize_ts_iso(cov_end)) if cov_end is not None else end
    now_utc = datetime.now(UTC)
    lag = max(0.0, (now_utc - cov_end_dt).total_seconds()) if cov_end_dt else 0.0
    last_ev = str(rows[-1]["event_id"]) if rows else None

    return HighScaleRequestFactResponse.with_telemetry(
        rows=rows,
        next_cursor=next_cursor,
        metadata=QueryResponseMetadataResponse(
            status="complete",
            exact=True,
            coverage=1.0,
            freshness_lag_seconds=round(lag, 2),
            watermark=ServingWatermarkResponse(
                service_id=ctx.service_id,
                domain="request",
                owner_epoch=1,
                coverage_start=cov_start_dt,
                coverage_end=cov_end_dt,
                last_accepted_cursor=next_cursor,
                last_archived_event_id=last_ev,
                last_visible_event_id=last_ev,
                exact=True,
            ),
            approximation_error=None,
            error=None,
            mode="duckdb_emulation",
        ),
    )


def _execute_request_facts_query(
    ctx: RequestContext,
    req: HighScaleRequestFactRequest,
    registry: HighScaleServiceRegistryProtocol,
    *,
    allow_standard_emulation: bool,
    default_window_if_missing: bool,
) -> HighScaleRequestFactResponse:
    from datetime import UTC, timedelta

    from backend.utils.date_utils import iso_z

    req_start = req.start_time
    req_end = req.end_time
    if default_window_if_missing and (not req_start or not req_end):
        now_dt = datetime.now(UTC)
        if not req_end:
            req_end = iso_z(now_dt)
        if not req_start:
            req_start = iso_z(now_dt - timedelta(hours=24))

    start, end = _clamp_and_parse_window(ctx, req_start, req_end)

    service = registry.resolve(ctx.service_id)
    if service is None:
        if allow_standard_emulation or req.emulate_standard:
            return _emulate_standard_request_facts(ctx, req, start=start, end=end)
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
    return HighScaleRequestFactResponse.with_telemetry(
        rows=rows,
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(meta, mode="clickhouse"),
    )


@router.get(
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
    emulate_standard: bool = True,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
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
        emulate_standard=emulate_standard,
    )
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        allow_standard_emulation=emulate_standard,
        default_window_if_missing=True,
    )


@router.post(
    "/request-facts",
    response_model=HighScaleRequestFactResponse,
    responses={409: {"model": ErrorEnvelope, "description": "Service binding conflict"}},
)
@query_errors()
def post_request_facts_root(
    req: HighScaleRequestFactRequest,
    ctx: RequestContext = Depends(build_request_context),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        allow_standard_emulation=True,
        default_window_if_missing=True,
    )


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
    return _execute_request_facts_query(
        ctx,
        req,
        registry,
        allow_standard_emulation=req.emulate_standard,
        default_window_if_missing=False,
    )


def _build_query_metadata_response(
    metadata,
    *,
    mode: str | None = None,
) -> QueryResponseMetadataResponse:
    kwargs: dict = {
        "status": metadata.status,
        "exact": metadata.exact,
        "coverage": metadata.coverage,
        "freshness_lag_seconds": metadata.freshness_lag_seconds,
        "watermark": ServingWatermarkResponse(
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
        "approximation_error": metadata.approximation_error,
        "error": metadata.error,
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
    service = _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    start, end = _clamp_and_parse_window(ctx, req.start_time, req.end_time)
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
    return HighScaleRequestFactResponse.with_telemetry(
        rows=_mask_client_ip_rows([dict(row) for row in page.rows], ctx),
        next_cursor=page.next_cursor,
        metadata=_build_query_metadata_response(page.metadata),
    )


@router.post(
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
):
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
):
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


def _mask_client_ip_rows(rows: list[dict], ctx: RequestContext) -> list[dict]:
    if mask_ips_for(ctx.analyst_session):
        for row in rows:
            value = row.get("client_ip")
            if isinstance(value, str):
                row["client_ip"] = mask_ip(value)
    return rows


@router.post(
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
):
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


@router.get(
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
):
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


@router.post(
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
):
    _resolve_scoped_high_scale_service(service_id, ctx, registry, check_binding=False)
    job = jobs.get(job_id)
    if job is None or job.plan.service_id != ctx.service_id:
        raise HTTPException(status_code=404, detail=make_error("historical_job_not_found", "Historical job not found"))
    cancelled = jobs.cancel(job_id)
    return HighScaleHistoricalJobCancelResponse.with_telemetry(job_id=job_id, cancelled=cancelled)
