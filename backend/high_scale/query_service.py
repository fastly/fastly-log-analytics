"""Bounded ClickHouse fact queries for explicitly high-scale callers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.pagination import MAX_PAGE_SIZE, KeysetCursor
from backend.high_scale.query_contracts import QueryResponseMetadata


class QueryClient(Protocol):
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class RequestFactPage:
    rows: tuple[dict[str, Any], ...]
    next_cursor: str | None
    metadata: QueryResponseMetadata


def query_request_facts(
    client: QueryClient,
    *,
    service_id: str,
    start: datetime,
    end: datetime,
    cursor_secret: bytes,
    watermark: ServingWatermark,
    limit: int = MAX_PAGE_SIZE,
    cursor: str | None = None,
    now: datetime | None = None,
) -> RequestFactPage:
    if not service_id:
        raise ValueError("service_id is required")
    if not cursor_secret:
        raise ValueError("cursor signing secret is required")
    if limit <= 0 or limit > MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)
    if end_utc <= start_utc:
        raise ValueError("query end must follow query start")
    decoded = KeysetCursor.decode(cursor, cursor_secret) if cursor else None
    if decoded and (decoded.service_id != service_id or decoded.domain != "request"):
        raise ValueError("cursor does not belong to this service and domain")

    params: dict[str, Any] = {
        "service_id": service_id,
        "start": start_utc,
        "end": end_utc,
        "limit": limit + 1,
    }
    cursor_clause = ""
    if decoded:
        params.update({"cursor_timestamp": decoded.timestamp, "cursor_event_id": decoded.event_id})
        cursor_clause = (
            " AND (event_timestamp > {cursor_timestamp:DateTime64(3)} "
            "OR (event_timestamp = {cursor_timestamp:DateTime64(3)} "
            "AND event_id > {cursor_event_id:UUID}))"
        )
    rows = client.execute(
        "SELECT event_id, event_timestamp AS timestamp, service_id, source_object_key, "
        "source_object_version, line_ordinal, country, client_ip, url, custom_fields, cmcd "
        "FROM request_facts "
        "WHERE service_id={service_id:String} "
        "AND event_timestamp >= {start:DateTime64(3)} "
        "AND event_timestamp < {end:DateTime64(3)} "
        "AND publication_state = 'visible'"
        f"{cursor_clause} "
        "ORDER BY event_timestamp ASC, event_id ASC "
        "LIMIT {limit:UInt32}",
        params,
    )
    page_rows = tuple(rows[:limit])
    next_cursor = None
    if len(rows) > limit:
        last = page_rows[-1]
        timestamp = _timestamp(last)
        next_cursor = KeysetCursor(timestamp, str(last["event_id"]), service_id, "request").encode(cursor_secret)

    observed = (now or datetime.now(UTC)).astimezone(UTC)
    lag = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
    metadata = QueryResponseMetadata(
        status="complete",
        exact=True,
        coverage=1.0,
        freshness_lag_seconds=lag,
        watermark=watermark,
        approximation_error=None,
        error=None,
    )
    metadata.validate()
    return RequestFactPage(page_rows, next_cursor, metadata)


def _timestamp(row: dict[str, Any]) -> datetime:
    value = row.get("timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value).astimezone(UTC)
    raise ValueError("ClickHouse query row is missing timestamp")
