"""Bounded ClickHouse fact queries for explicitly high-scale callers."""

from __future__ import annotations

import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal, Protocol

from backend.high_scale._common import MAX_PAGE_SIZE, KeysetCursor
from backend.high_scale.archive_models import ServingWatermark
from backend.utils.telemetry import _QUERIES, _RECORDING_TELEMETRY, get_page_load_id, get_queries

QueryStatus = Literal["complete", "partial", "queued", "running", "failed"]


@dataclass(frozen=True)
class QueryResponseMetadata:
    status: QueryStatus
    exact: bool
    coverage: float
    freshness_lag_seconds: float
    watermark: ServingWatermark
    approximation_error: float | None
    error: str | None

    def validate(self) -> None:
        if not 0 <= self.coverage <= 1:
            raise ValueError("query coverage must be between 0 and 1")
        if self.freshness_lag_seconds < 0:
            raise ValueError("query freshness lag must be non-negative")
        if self.exact and self.approximation_error is not None:
            raise ValueError("exact results cannot report approximation error")
        if not self.exact and self.approximation_error is None:
            raise ValueError("approximate results require approximation error")
        if self.status == "failed" and not self.error:
            raise ValueError("failed queries require an error")
        if self.status != "failed" and self.error is not None:
            raise ValueError("only failed queries may report an error")
        self.watermark.validate()


class QueryClient(Protocol):
    def execute(
        self, sql: str, params: dict[str, Any] | None = None, *, settings: Mapping[str, str] | None = None
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class RequestFactPage:
    rows: tuple[dict[str, Any], ...]
    next_cursor: str | None
    metadata: QueryResponseMetadata


_RUM_TABLES = {
    "rum_vitals": (
        "rum_vitals_facts",
        "client_id, request_event_id, metric_name, metric_value, metric_rating, pathname, country",
    ),
    "rum_errors": (
        "rum_error_facts",
        "client_id, request_event_id, error_message, error_file, pathname, country",
    ),
}


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
    status: str | int | None = None,
    domain_filter: str | None = None,
    pop: str | None = None,
    country: str | None = None,
    batch_id: str | None = None,
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

    return _query_facts(
        client,
        service_id=service_id,
        domain="request",
        table="request_facts",
        columns="source_object_key, source_object_version, line_ordinal, country, client_ip, url, custom_fields, cmcd",
        start=start,
        end=end,
        cursor_secret=cursor_secret,
        watermark=watermark,
        limit=limit,
        cursor=cursor,
        now=now,
        status=status,
        domain_filter=domain_filter,
        pop=pop,
        country=country,
        batch_id=batch_id,
    )


def query_rum_facts(
    client: QueryClient,
    *,
    service_id: str,
    domain: str,
    start: datetime,
    end: datetime,
    cursor_secret: bytes,
    watermark: ServingWatermark,
    limit: int = MAX_PAGE_SIZE,
    cursor: str | None = None,
    now: datetime | None = None,
) -> RequestFactPage:
    try:
        table, columns = _RUM_TABLES[domain]
    except KeyError as exc:
        raise ValueError("unsupported RUM domain") from exc
    return _query_facts(
        client,
        service_id=service_id,
        domain=domain,
        table=table,
        columns=columns,
        start=start,
        end=end,
        cursor_secret=cursor_secret,
        watermark=watermark,
        limit=limit,
        cursor=cursor,
        now=now,
    )


def query_cmcd_facts(
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
    return _query_facts(
        client,
        service_id=service_id,
        domain="cmcd",
        table="cmcd_projection_facts",
        columns="request_event_id, cmcd",
        event_column="projection_key",
        start=start,
        end=end,
        cursor_secret=cursor_secret,
        watermark=watermark,
        limit=limit,
        cursor=cursor,
        now=now,
    )


def _record_clickhouse_telemetry(service_id: str, sql: str, elapsed_ms: float) -> None:
    queries = get_queries()
    queries.append({"sql": sql.strip(), "time_ms": elapsed_ms, "engine": "ClickHouse"})
    _QUERIES.set(queries)
    plid = get_page_load_id()
    if plid and not _RECORDING_TELEMETRY.get():
        token = _RECORDING_TELEMETRY.set(True)
        try:
            from backend.core.metadata import usage_log

            usage_log.log_telemetry_query(
                service_id=service_id,
                page_load_id=plid,
                request_id="unknown",
                engine="clickhouse",
                sql=sql.strip(),
                duration_ms=elapsed_ms,
            )
        except Exception:
            pass
        finally:
            _RECORDING_TELEMETRY.reset(token)


def _query_facts(
    client: QueryClient,
    *,
    service_id: str,
    domain: str,
    table: str,
    columns: str,
    start: datetime,
    end: datetime,
    cursor_secret: bytes,
    watermark: ServingWatermark,
    limit: int,
    cursor: str | None,
    now: datetime | None,
    event_column: str = "event_id",
    status: str | int | None = None,
    domain_filter: str | None = None,
    pop: str | None = None,
    country: str | None = None,
    batch_id: str | None = None,
) -> RequestFactPage:
    if not service_id:
        raise ValueError("service_id is required")
    if watermark.service_id != service_id or watermark.domain != domain:
        raise ValueError("watermark does not belong to this service and domain")
    if not cursor_secret:
        raise ValueError("cursor signing secret is required")
    if limit <= 0 or limit > MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)
    if end_utc <= start_utc:
        raise ValueError("query end must follow query start")
    decoded = KeysetCursor.decode(cursor, cursor_secret) if cursor else None
    if decoded and (decoded.service_id != service_id or decoded.domain != domain):
        raise ValueError("cursor does not belong to this service and domain")

    params: dict[str, Any] = {
        "service_id": service_id,
        "domain": domain,
        "start": start_utc,
        "end": end_utc,
        "limit": limit + 1,
    }
    filter_clauses: list[str] = []
    if country is not None and str(country).strip():
        params["filter_country"] = str(country).strip()
        filter_clauses.append(" AND country = {filter_country:String}")
    if status is not None and str(status).strip():
        params["filter_status"] = str(status).strip()
        filter_clauses.append(" AND custom_fields['status'] = {filter_status:String}")
    if pop is not None and str(pop).strip():
        params["filter_pop"] = str(pop).strip()
        filter_clauses.append(" AND custom_fields['pop'] = {filter_pop:String}")
    if domain_filter is not None and str(domain_filter).strip():
        dom_val = str(domain_filter).strip()
        params["filter_domain"] = dom_val
        params["filter_domain_like"] = f"%{dom_val}%"
        filter_clauses.append(
            " AND (custom_fields['host'] = {filter_domain:String} OR url ILIKE {filter_domain_like:String})"
        )
    if batch_id is not None and str(batch_id).strip():
        params["filter_batch_id"] = str(batch_id).strip()
        filter_clauses.append(" AND toString(batch_id) = {filter_batch_id:String}")

    cursor_clause = ""
    if decoded:
        params.update({"cursor_timestamp": decoded.timestamp, "cursor_event_id": decoded.event_id})
        cursor_clause = (
            f" AND (event_timestamp > {{cursor_timestamp:DateTime64(3)}} "
            "OR (event_timestamp = {cursor_timestamp:DateTime64(3)} "
            f"AND {event_column} > {{cursor_event_id:UUID}}))"
        )
    extra_filter_sql = "".join(filter_clauses)
    sql_query = (
        f"SELECT {event_column} AS event_id, event_timestamp AS timestamp, service_id, {columns} "
        f"FROM {table} "
        "WHERE service_id={service_id:String} "
        "AND event_timestamp >= {start:DateTime64(3)} "
        "AND event_timestamp < {end:DateTime64(3)} "
        "AND publication_state = 'visible' "
        "AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={domain:String} "
        "AND publication_state = 'visible'"
        ")"
        f"{extra_filter_sql}"
        f"{cursor_clause} "
        f"ORDER BY event_timestamp ASC, {event_column} ASC "
        "LIMIT {limit:UInt32}"
    )
    before_queries = len(get_queries())
    t0 = time.perf_counter()
    rows = client.execute(sql_query, params)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    if len(get_queries()) == before_queries:
        _record_clickhouse_telemetry(service_id, sql_query, elapsed_ms)

    page_rows = tuple(rows[:limit])
    next_cursor = None
    if len(rows) > limit:
        last = page_rows[-1]
        timestamp = _timestamp(last)
        next_cursor = KeysetCursor(timestamp, str(last["event_id"]), service_id, domain).encode(cursor_secret)

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


class QueryTier(StrEnum):
    """Storage or execution tier selected for a query."""

    TRIAGE_AGGREGATE = "triage_aggregate"
    RECENT_FACTS = "recent_facts"
    WARM_FACTS_ARCHIVE = "warm_facts_archive"
    COLD_JOB = "cold_job"


class QueryIntent(StrEnum):
    """Whether the caller needs a fast triage summary or raw rows."""

    TRIAGE = "triage"
    RAW = "raw"


@dataclass(frozen=True)
class QueryRouterConfig:
    """Retention and safety bounds used by :class:`QueryRouter`."""

    aggregate_retention: timedelta = timedelta(hours=24)
    recent_facts_retention: timedelta = timedelta(days=7)
    warm_retention: timedelta = timedelta(days=30)
    max_cold_range: timedelta = timedelta(days=7)
    max_cold_rows: int = MAX_PAGE_SIZE

    def validate(self) -> None:
        if self.aggregate_retention <= timedelta(0):
            raise ValueError("aggregate retention must be positive")
        if self.recent_facts_retention < self.aggregate_retention:
            raise ValueError("recent facts retention must include aggregate retention")
        if self.warm_retention < self.recent_facts_retention:
            raise ValueError("warm retention must include recent facts retention")
        if self.max_cold_range <= timedelta(0):
            raise ValueError("maximum cold range must be positive")
        if self.max_cold_rows <= 0 or self.max_cold_rows > MAX_PAGE_SIZE:
            raise ValueError(f"maximum cold rows must be between 1 and {MAX_PAGE_SIZE}")


@dataclass(frozen=True)
class QueryRequest:
    """Validated, side-effect-free input to the planner."""

    service_id: str
    domain: str
    start: datetime
    end: datetime
    intent: QueryIntent = QueryIntent.RAW
    limit: int = MAX_PAGE_SIZE

    def validate(self) -> None:
        if not self.service_id.strip():
            raise ValueError("service id is required")
        if self.domain not in {"request", "rum_vitals", "rum_errors", "cmcd"}:
            raise ValueError("unsupported high-scale query domain")
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("query range must be timezone-aware")
        if self.end.astimezone(UTC) <= self.start.astimezone(UTC):
            raise ValueError("query end must follow query start")
        if self.limit <= 0 or self.limit > MAX_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")

    @property
    def start_utc(self) -> datetime:
        return self.start.astimezone(UTC)

    @property
    def end_utc(self) -> datetime:
        return self.end.astimezone(UTC)


@dataclass(frozen=True)
class QueryPlan:
    """Explicit execution metadata returned by the pure planner."""

    service_id: str
    domain: str
    intent: QueryIntent
    tier: QueryTier
    start: datetime
    end: datetime
    sources: tuple[str, ...]
    queued: bool
    bounded: bool
    max_rows: int
    reason: str

    def validate(self) -> None:
        if self.end <= self.start:
            raise ValueError("query plan end must follow query plan start")
        if not self.sources:
            raise ValueError("query plan must identify at least one source")
        if self.max_rows <= 0:
            raise ValueError("query plan row bound must be positive")
        if self.tier is QueryTier.COLD_JOB and not self.queued:
            raise ValueError("cold query plans must be queued")
        if self.tier is not QueryTier.COLD_JOB and self.queued:
            raise ValueError("only cold query plans may be queued")


class QueryRouter:
    """Select a high-scale query tier without performing any I/O."""

    def __init__(self, config: QueryRouterConfig | None = None) -> None:
        self._config = config or QueryRouterConfig()
        self._config.validate()

    def plan(self, request: QueryRequest, *, now: datetime | None = None) -> QueryPlan:
        request.validate()
        observed = now or datetime.now(UTC)
        if observed.tzinfo is None:
            raise ValueError("planner time must be timezone-aware")
        observed = observed.astimezone(UTC)
        if request.end_utc > observed:
            raise ValueError("query end cannot be in the future")

        age = observed - request.start_utc
        if request.intent is QueryIntent.TRIAGE and age <= self._config.aggregate_retention:
            return self._build(request, QueryTier.TRIAGE_AGGREGATE, ("aggregates",), False, False, "recent triage")
        if age <= self._config.recent_facts_retention:
            return self._build(request, QueryTier.RECENT_FACTS, ("clickhouse_facts",), False, False, "recent raw facts")
        if age <= self._config.warm_retention:
            return self._build(
                request,
                QueryTier.WARM_FACTS_ARCHIVE,
                ("clickhouse_facts", "fos_archive"),
                False,
                False,
                "warm history spans facts and archive",
            )
        if request.end_utc - request.start_utc > self._config.max_cold_range:
            raise ValueError("cold query range exceeds configured bound")
        return self._build(
            request,
            QueryTier.COLD_JOB,
            ("queued_query_job",),
            True,
            True,
            "cold history requires a bounded queued job",
        )

    def _build(
        self,
        request: QueryRequest,
        tier: QueryTier,
        sources: tuple[str, ...],
        queued: bool,
        bounded: bool,
        reason: str,
    ) -> QueryPlan:
        max_rows = min(request.limit, self._config.max_cold_rows) if tier is QueryTier.COLD_JOB else request.limit
        plan = QueryPlan(
            service_id=request.service_id,
            domain=request.domain,
            intent=request.intent,
            tier=tier,
            start=request.start_utc,
            end=request.end_utc,
            sources=sources,
            queued=queued,
            bounded=bounded,
            max_rows=max_rows,
            reason=reason,
        )
        plan.validate()
        return plan


_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.query_router"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "query_router", _self_mod)
