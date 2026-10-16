"""Live Log Streaming router — real-time SSE access log tailing."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend.core.share_db.validation import mask_ip
from backend.deps import ServiceId, require_service_access
from backend.high_scale.registry import (
    HighScaleServiceRegistryProtocol,
    get_high_scale_service_registry,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS

logger = logging.getLogger(__name__)

router = APIRouter(tags=["streaming"], responses=DEFAULT_ERROR_RESPONSES)


def _format_stream_event(row: dict[str, Any], mask_ips: bool) -> dict[str, Any]:
    raw_ip = str(row.get("client_ip") or row.get("ip") or row.get("remote_addr") or "")
    client_ip = mask_ip(raw_ip) if (mask_ips and raw_ip) else raw_ip

    raw_status = row.get("status")
    try:
        status = int(raw_status) if raw_status is not None else 200
    except (ValueError, TypeError):
        status = 200

    raw_elapsed = row.get("time_elapsed") or row.get("elapsed") or row.get("time_to_first_byte") or 0.0
    try:
        time_elapsed = float(raw_elapsed)
    except (ValueError, TypeError):
        time_elapsed = 0.0

    ts = str(row.get("timestamp") or "")
    method = str(row.get("request_method") or row.get("method") or "GET")
    url = str(row.get("url") or row.get("req_url") or "/")
    pop = str(row.get("pop") or row.get("fastly_pop") or "")

    event_id = str(row.get("event_id") or row.get("request_id") or row.get("rid") or "")
    if not event_id:
        import hashlib

        h = hashlib.md5(f"{ts}|{raw_ip}|{method}|{url}|{status}|{pop}".encode()).hexdigest()
        event_id = h[:16]

    clean_attrs = dict(row)
    if mask_ips:
        for k in ("client_ip", "ip", "remote_addr", "server_ip"):
            if k in clean_attrs and clean_attrs[k]:
                clean_attrs[k] = mask_ip(str(clean_attrs[k]))

    return {
        "event_id": event_id,
        "timestamp": ts,
        "client_ip": client_ip,
        "method": method,
        "url": url,
        "status": status,
        "pop": pop,
        "time_elapsed": time_elapsed,
        "attributes": clean_attrs,
    }


def _fetch_standard_stream_logs(
    source: dict[str, Any],
    limit: int = 25,
    search: str | None = None,
    filters: dict[str, Any] | None = None,
    mask_ips: bool = False,
) -> list[dict[str, Any]]:
    from backend.core import duckdb as _db
    from backend.repositories import logs as logs_repo

    con = _db.get_connection(source=source, read_only=True)
    try:
        res = logs_repo.query_logs(
            con=con,
            src=source,
            start_time=None,
            end_time=None,
            filters=filters or {},
            limit=limit,
            offset=0,
            sort_dir="DESC",
            search=search,
            mask_ips=mask_ips,
        )
        return res.get("rows", [])
    finally:
        con.close()


def _fetch_high_scale_stream_logs(
    service: Any,
    limit: int = 25,
    search: str | None = None,
    filters: dict[str, Any] | None = None,
    mask_ips: bool = False,
) -> list[dict[str, Any]]:
    from backend.high_scale.logs import logs_query_endpoint
    from backend.models.logs import LogsQueryRequest

    req = LogsQueryRequest(
        limit=limit,
        offset=0,
        sort_dir="desc",
        search=search,
        filters=filters or {},
    )
    res = logs_query_endpoint(
        service=service,
        req=req,
        start_time=None,
        end_time=None,
        mask_ips=mask_ips,
    )
    return res.get("rows", [])


@router.get("/api/stream/{service_id}")
@router.get("/api/services/{service_id}/stream")
async def live_log_stream(
    request: Request,
    service_id: ServiceId,
    filter: str | None = Query(default=None),
    status: str | None = Query(default=None),
    pop: str | None = Query(default=None),
    client_ip: str | None = Query(default=None),
    _access: str | None = Depends(require_service_access),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> EventSourceResponse:
    """Stream live Fastly access log lines via Server-Sent Events (SSE).

    Supports pause/resume on the client, client-side regex search, and deep JSON
    inspection. Analyst sessions with IP masking receive masked client IPs.
    """
    from backend.core import duckdb as _db

    source = _db.get_source_for_service(service_id)
    high_scale_service = registry.resolve(service_id)
    mask_ips = mask_ips_for(getattr(request.state, "analyst_session", None))

    query_filters: dict[str, Any] = {}
    if status:
        query_filters["status"] = {"mode": "include", "values": [str(status)]}
    if pop:
        query_filters["pop"] = {"mode": "include", "values": [str(pop)]}
    if client_ip:
        query_filters["client_ip"] = {"mode": "include", "values": [str(client_ip)]}

    logger.info("Live stream client connected for service %s (mask_ips=%s)", service_id, mask_ips)

    async def event_generator() -> AsyncIterator[dict[str, str]]:
        seen: set[str] = set()
        first_tick = True
        try:
            while not await request.is_disconnected():
                try:
                    limit = 25 if first_tick else 15
                    if high_scale_service is not None:
                        rows = await asyncio.to_thread(
                            _fetch_high_scale_stream_logs,
                            high_scale_service,
                            limit,
                            filter,
                            query_filters,
                            mask_ips,
                        )
                    elif source is not None:
                        rows = await asyncio.to_thread(
                            _fetch_standard_stream_logs,
                            source,
                            limit,
                            filter,
                            query_filters,
                            mask_ips,
                        )
                    else:
                        rows = []

                    # Process in ascending order (oldest to newest)
                    events = [_format_stream_event(r, mask_ips) for r in reversed(rows)]
                    for ev in events:
                        eid = ev["event_id"]
                        if eid in seen:
                            continue
                        seen.add(eid)
                        if len(seen) > 1000:
                            seen = set(list(seen)[-500:])
                        yield {"event": "log_event", "data": json.dumps(ev)}

                    first_tick = False
                except Exception:
                    logger.warning("Error fetching live stream logs for %s", service_id, exc_info=True)

                await asyncio.sleep(1.5)
        finally:
            logger.info("Live stream client disconnected for service %s", service_id)

    return EventSourceResponse(event_generator(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)
