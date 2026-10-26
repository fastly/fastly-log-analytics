"""Bounded ClickHouse fact queries, console SQL execution, and lossless source-object decoding for high-scale."""

from __future__ import annotations

import base64
import gzip
import json
import math
import re
import sys
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, Protocol
from urllib.parse import parse_qs, urlparse

import duckdb

from backend.core.field_registry import DuckType, try_get
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip_values
from backend.high_scale._common import MAX_PAGE_SIZE, KeysetCursor
from backend.high_scale._common import range_value as _range_value
from backend.high_scale.archive import ArchiveSourceObject, ServingWatermark
from backend.high_scale.publication import (
    build_event_id,
    build_request_event_id,
    build_rum_event_id,
    row_matches_serving_domain,
)
from backend.models.dashboard import QueryRequest as DashboardQueryRequest
from backend.repositories._base import SectionTimer, _compact_sql_for_debug, _safe_table
from backend.repositories.query import MAX_QUERY_ROWS
from backend.utils.sql_validator import SAFE_IDENT_RE, SQLValidationError, validate_user_sql
from backend.utils.telemetry import _QUERIES, _RECORDING_TELEMETRY, get_page_load_id, get_queries, get_tracked_calls

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService
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
    "rum_errors": ("rum_error_facts", "client_id, request_event_id, error_message, error_file, pathname, country"),
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
            request, QueryTier.COLD_JOB, ("queued_query_job",), True, True, "cold history requires a bounded queued job"
        )

    def _build(
        self, request: QueryRequest, tier: QueryTier, sources: tuple[str, ...], queued: bool, bounded: bool, reason: str
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


# ── Lossless Source-Object Decoder (folded from decoder.py) ───────────────────
@dataclass(frozen=True)
class DeadLetterRecord:
    source_object_key: str
    line_ordinal: int
    raw_line: bytes
    reason: str

    @property
    def raw_line_base64(self) -> str:
        return base64.b64encode(self.raw_line).decode("ascii")


@dataclass(frozen=True)
class DecodeResult:
    source: ArchiveSourceObject
    events: tuple[dict[str, Any], ...]
    dead_letters: tuple[DeadLetterRecord, ...]
    decoded_rows: int
    accepted_rows: int
    quarantined_rows: int


def decode_source_object(
    source: ArchiveSourceObject, payload: bytes, *, transform_version: str, domain: str | None = None
) -> DecodeResult:
    selected_domain = domain or source.domain
    if selected_domain not in {"request", "rum_vitals", "rum_errors"}:
        raise ValueError("source decoder supports request, rum_vitals, and rum_errors domains")
    raw_payload = _decode_payload(payload)
    events: list[dict[str, Any]] = []
    dead_letters: list[DeadLetterRecord] = []
    source_version = source.version or source.checksum
    lines = raw_payload.splitlines()
    for line_ordinal, raw_line in enumerate(lines):
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise ValueError("record is not a JSON object")
            raw_json_str = raw_line.decode("utf-8")
            base_meta = {
                "service_id": source.service_id,
                "domain": selected_domain,
                "source_object_key": source.object_key,
                "source_object_version": source_version,
                "line_ordinal": line_ordinal,
                "transform_version": transform_version,
                "raw_json": raw_json_str,
            }
            expanded_faro = (
                _expand_faro_rum_events(value, selected_domain)
                if selected_domain in {"rum_vitals", "rum_errors"}
                else []
            )
            if expanded_faro:
                for sub_ordinal, faro_fields in enumerate(expanded_faro):
                    event = dict(value)
                    event.update(faro_fields)
                    event.update(base_meta)
                    _normalize_serving_fields(event, selected_domain)
                    event["event_id"] = _event_id(event, selected_domain, sub_ordinal=sub_ordinal)
                    events.append(event)
            else:
                event = dict(value)
                event.update(base_meta)
                _normalize_serving_fields(event, selected_domain)
                if selected_domain in {"rum_vitals", "rum_errors"} and not row_matches_serving_domain(
                    selected_domain, event
                ):
                    sibling = "rum_errors" if selected_domain == "rum_vitals" else "rum_vitals"
                    other = dict(value)
                    _normalize_serving_fields(other, sibling)
                    if not row_matches_serving_domain(sibling, other):
                        raise ValueError("RUM record has neither a valid metric nor an error discriminator")
                event["event_id"] = _event_id(event, selected_domain)
                events.append(event)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            dead_letters.append(DeadLetterRecord(source.object_key, line_ordinal, raw_line, str(exc)))
    return DecodeResult(
        source=source,
        events=tuple(events),
        dead_letters=tuple(dead_letters),
        decoded_rows=len(lines),
        accepted_rows=len(events),
        quarantined_rows=len(dead_letters),
    )


def _decode_payload(payload: bytes) -> bytes:
    if payload[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(payload)
        except (OSError, EOFError) as exc:
            raise ValueError("source gzip payload is invalid") from exc
    return payload


_FIELD_ALIASES: dict[str, str] = {
    "resp_body_size": "resp_bytes",
    "req_body_size": "req_size",
    "response_bytes": "resp_bytes",
    "status_code": "status",
    "client_ip": "ip",
}


def _normalize_serving_fields(event: dict[str, Any], domain: str) -> None:
    if domain == "request":
        custom_fields = event.setdefault("custom_fields", {})
        cmcd = event.setdefault("cmcd", {})
        keys_to_check = [
            k
            for k in list(event.keys())
            if k
            not in {
                "service_id",
                "domain",
                "source_object_key",
                "source_object_version",
                "line_ordinal",
                "transform_version",
                "raw_json",
                "event_id",
                "custom_fields",
                "cmcd",
            }
        ]
        for k in keys_to_check:
            val = event[k]
            if k.startswith("cmcd_") or k.startswith("cmcd."):
                cmcd[k] = str(val) if val is not None else ""
                short_k = k[5:]
                if short_k:
                    cmcd[short_k] = str(val) if val is not None else ""
                continue
            field = try_get(k) or try_get(_FIELD_ALIASES.get(k, ""))
            if field is not None:
                if field.duck_type in (
                    DuckType.UTINYINT,
                    DuckType.USMALLINT,
                    DuckType.UINTEGER,
                    DuckType.UBIGINT,
                    DuckType.BIGINT,
                    DuckType.INTEGER,
                ):
                    if val not in (None, ""):
                        try:
                            event[k] = int(val)
                        except (ValueError, TypeError):
                            pass
                elif field.duck_type in (DuckType.FLOAT, DuckType.DOUBLE):
                    if val not in (None, ""):
                        try:
                            event[k] = float(val)
                        except (ValueError, TypeError):
                            pass
                elif field.duck_type == DuckType.BOOLEAN:
                    if isinstance(val, str):
                        event[k] = val.lower() in ("true", "1", "t", "yes")
                    elif val is not None:
                        event[k] = bool(val)
                elif field.duck_type == DuckType.VARCHAR:
                    if val is not None and not isinstance(val, str):
                        event[k] = str(val)
            else:
                if k in {"timestamp", "time_stamp", "client_ip", "ip", "url", "fastly_pop"}:
                    continue
                custom_fields[k] = str(val) if val is not None else ""
    elif domain == "rum_vitals":
        client_id = event.get("rum_cid") or event.get("client_id") or ""
        metric_name = event.get("rum_metric_name") or event.get("metric_name") or ""
        raw_metric_value = event.get("rum_metric_value", event.get("metric_value"))
        metric_rating = event.get("rum_metric_rating") or event.get("metric_rating") or ""
        pathname = event.get("rum_pathname") or event.get("pathname") or ""
        if not metric_name or raw_metric_value in (None, ""):
            qparams = _rum_querystring_params(event)
            if qparams:
                metric_name = metric_name or _first_qparam(qparams, "rum_metric_name")
                if raw_metric_value in (None, ""):
                    raw_metric_value = _first_qparam(qparams, "rum_metric_value")
                metric_rating = metric_rating or _first_qparam(qparams, "rum_metric_rating")
                client_id = client_id or _first_qparam(qparams, "cid")
                pathname = pathname or _first_qparam(qparams, "rum_pathname")
        event["client_id"] = client_id
        event["metric_name"] = metric_name
        if raw_metric_value in (None, ""):
            event["metric_value"] = None
        elif isinstance(raw_metric_value, (int, float)) and not isinstance(raw_metric_value, bool):
            event["metric_value"] = float(raw_metric_value)
        elif isinstance(raw_metric_value, str):
            event["metric_value"] = float(raw_metric_value)
        else:
            event["metric_value"] = None
        event["metric_rating"] = metric_rating
        event["pathname"] = pathname
    elif domain == "rum_errors":
        event["client_id"] = event.get("rum_cid") or event.get("client_id") or ""
        event["error_message"] = event.get("rum_error_message") or event.get("error_message") or ""
        if not event["error_message"] and (
            event.get("_rum_kind") == "errors"
            or event.get("rum_metric_name", event.get("metric_name")) == "exception"
            or event.get("rum_error_stack")
            or event.get("error_stack")
            or event.get("rum_error_file")
            or event.get("error_file")
        ):
            event["error_message"] = "Unknown error"
        event["error_file"] = event.get("rum_error_file") or event.get("error_file") or ""
        event["pathname"] = event.get("rum_pathname") or event.get("pathname") or ""
        raw_line = event.get("rum_error_line", event.get("error_line"))
        if raw_line is not None and raw_line != "":
            try:
                event["error_line"] = int(raw_line)
            except (ValueError, TypeError):
                pass
        raw_col = event.get("rum_error_col", event.get("error_col"))
        if raw_col is not None and raw_col != "":
            try:
                event["error_col"] = int(raw_col)
            except (ValueError, TypeError):
                pass


def _is_faro_exception_metric(metric: dict[str, Any]) -> bool:
    return (metric.get("metric_name") == "exception") or bool(metric.get("error_message"))


def _expand_faro_rum_events(log_data: dict[str, Any], domain: str) -> list[dict[str, Any]]:
    raw_body = log_data.get("rum_body")
    if not raw_body:
        return []
    from backend.core.rum_ingest import extract_metrics_from_faro_payload

    try:
        payload = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            raise ValueError("Faro RUM body must be an object")
        extracted = extract_metrics_from_faro_payload(payload, log_data)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("invalid Faro RUM body") from exc
    if not extracted:
        return []
    if domain == "rum_vitals":
        domain_items = [m for m in extracted if not _is_faro_exception_metric(m)]
    else:
        domain_items = [m for m in extracted if _is_faro_exception_metric(m)]
    selected_items = domain_items if domain_items else extracted
    rows: list[dict[str, Any]] = []
    for metric in selected_items:
        is_exc = _is_faro_exception_metric(metric)
        browser_val = metric.get("browser") or log_data.get("browser") or "Chrome"
        os_val = metric.get("os") or log_data.get("os") or "macOS"
        device_val = metric.get("device") or log_data.get("device") or "Desktop"
        cid_val = metric.get("cid") or log_data.get("rum_cid") or log_data.get("cid") or ""
        pathname_val = metric.get("pathname") or log_data.get("rum_pathname") or "/"
        base_fields: dict[str, Any] = {
            "_rum_kind": "errors" if is_exc else "vitals",
            "client_id": cid_val,
            "rum_cid": cid_val,
            "pathname": pathname_val,
            "rum_pathname": pathname_val,
            "browser": browser_val,
            "os": os_val,
            "device": device_val,
        }
        if is_exc:
            raw_line = metric.get("error_line")
            raw_col = metric.get("error_col")
            try:
                err_line = int(raw_line) if raw_line is not None and raw_line != "" else 0
            except (ValueError, TypeError):
                err_line = 0
            try:
                err_col = int(raw_col) if raw_col is not None and raw_col != "" else 0
            except (ValueError, TypeError):
                err_col = 0
            err_msg = metric.get("error_message") or "Unknown error"
            if not isinstance(err_msg, str):
                raise ValueError("Faro error_message must be text")
            err_file = metric.get("error_file") or "unknown.js"
            base_fields.update(
                {
                    "error_message": err_msg,
                    "rum_error_message": err_msg,
                    "error_file": err_file,
                    "rum_error_file": err_file,
                    "error_line": err_line,
                    "rum_error_line": err_line,
                    "error_col": err_col,
                    "rum_error_col": err_col,
                    "metric_name": "",
                    "rum_metric_name": "",
                    "metric_value": None,
                    "rum_metric_value": None,
                    "metric_rating": "",
                    "rum_metric_rating": "",
                }
            )
        else:
            raw_val = metric.get("metric_value")
            if raw_val is None or raw_val == "" or isinstance(raw_val, bool):
                raise ValueError("Faro metric_value must be finite")
            val = float(raw_val)
            if not math.isfinite(val):
                raise ValueError("Faro metric_value must be finite")
            m_name = metric.get("metric_name") or "unknown"
            m_rating = metric.get("metric_rating") or ""
            base_fields.update(
                {
                    "metric_name": m_name,
                    "rum_metric_name": m_name,
                    "metric_value": val,
                    "rum_metric_value": val,
                    "metric_rating": m_rating,
                    "rum_metric_rating": m_rating,
                    "error_message": "",
                    "rum_error_message": "",
                }
            )
        rows.append(base_fields)
    return rows


def _rum_querystring_params(event: dict[str, Any]) -> dict[str, list[str]]:
    raw_url = event.get("url") or event.get("rum_raw_query") or ""
    if not raw_url:
        return {}
    try:
        return parse_qs(urlparse(raw_url).query)
    except ValueError:
        return {}


def _first_qparam(qparams: dict[str, list[str]], key: str) -> str:
    values = qparams.get(key)
    return values[0] if values else ""


def _event_id(event: dict[str, Any], domain: str, *, sub_ordinal: int | None = None) -> str:
    if domain == "request":
        return build_request_event_id(event)
    if domain == "rum_vitals":
        return build_rum_event_id(event, rum_kind="vitals", sub_ordinal=sub_ordinal)
    if domain == "rum_errors":
        return build_rum_event_id(event, rum_kind="errors", sub_ordinal=sub_ordinal)
    return build_event_id(event, domain=domain)


# ── SQL Console Query Endpoint (folded from query.py) ─────────────────────────
_FACT_DOMAINS = {"request_facts": "request", "rum_vitals_facts": "rum_vitals", "rum_error_facts": "rum_errors"}


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _instant(value: datetime) -> str:
    return f"toDateTime64({_literal(value.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f'))}, 6, 'UTC')"


def _table_filters(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    tables: Iterable[str] | Mapping[str, str],
    *,
    scoped_by_view: bool = False,
) -> str | None:
    """``additional_table_filters`` value for the fact tables the query reads, or None when nothing applies."""
    domains = tables if isinstance(tables, Mapping) else {table: _FACT_DOMAINS[table] for table in tables}
    start = _range_value(start_time)
    end = _range_value(end_time)
    entries = []
    for table, domain in domains.items():
        clauses: list[str] = []
        if not scoped_by_view:
            clauses += [
                f"service_id = {_literal(service_id)}",
                "publication_state = 'visible'",
                "batch_id IN (SELECT batch_id FROM high_scale_batch_publications FINAL "
                f"WHERE service_id = {_literal(service_id)} AND domain = '{domain}' AND publication_state = 'visible')",
            ]
        if start is not None:
            clauses.append(f"event_timestamp >= {_instant(start)}")
        if end is not None:
            clauses.append(f"event_timestamp <= {_instant(end)}")
        if clauses:
            entries.append(f"{_literal(table)}: {_literal(' AND '.join(clauses))}")
    return "{" + ", ".join(entries) + "}" if entries else None


def _assert_table_filters_enforced(client: Any) -> None:
    """Fail closed unless this ClickHouse honours ``additional_table_filters``."""
    if getattr(client, "_table_filters_verified", False):
        return
    rows = client.execute(
        "SELECT count() AS n FROM system.one", settings={"additional_table_filters": "{'system.one': 'dummy = 1'}"}
    )
    if not rows or int(rows[0].get("n", 1)) != 0:
        raise RuntimeError("the SQL console is disabled: ClickHouse is not enforcing additional_table_filters")
    try:
        client._table_filters_verified = True
    except AttributeError:
        pass


_ALLOWED_FUNCTIONS = frozenset(
    """
    count sum avg min max any anylast argmin argmax uniq uniqexact uniqcombined countif sumif avgif minif maxif
    anyif uniqif quantile quantiles quantileexact quantileexactweighted quantiletiming quantiletdigest median
    stddevpop stddevsamp varpop varsamp topk grouparray grouparrayif groupuniqarray
    if multiif coalesce ifnull nullif isnull isnotnull assumenotnull tonullable
    lower upper lowerutf8 upperutf8 length lengthutf8 char_length substring substr substringutf8 concat concatws
    replace replaceone replaceall replaceregexpone replaceregexpall position positioncaseinsensitive startswith
    endswith trim trimleft trimright ltrim rtrim splitbychar splitbystring extract extractall match like ilike
    tostring format reverse arraystringconcat
    abs round floor ceil ceiling trunc sqrt pow power exp log log2 log10 intdiv modulo greatest least sign
    cast toint8 toint16 toint32 toint64 touint8 touint16 touint32 touint64 tofloat32 tofloat64
    toint32ornull toint64ornull touint32ornull touint64ornull tofloat64ornull toint32orzero toint64orzero
    touint32orzero touint64orzero tofloat64orzero todate todatetime todatetime64 todatetimebesteffort
    parsedatetimebesteffort parsedatetime64besteffort
    tostartofminute tostartoffiveminute tostartoftenminutes tostartoffifteenminutes tostartofhour tostartofday
    tostartofweek tostartofmonth tostartofinterval date_trunc datetrunc now today tounixtimestamp fromunixtimestamp
    formatdatetime tohour tominute tosecond todayofweek todayofmonth tomonth toyear dateadd datesub datediff
    tointervalsecond tointervalminute tointervalhour tointervalday
    array arrayelement has hasany hasall empty notempty map mapkeys mapvalues mapcontains tuple
    row_number rank dense_rank lag lead
    """.split()
)
_SQL_KEYWORDS = frozenset(
    "in as and or not over where from join on select with when then else case end between like ilike using "
    "partition by order group having union all distinct exists any interval limit offset on inner "
    "full cross intersect except array global prewhere final sample".split()
)
_SQL_TOKEN = re.compile(
    r"""
    '(?:[^'\\]|\\.|'')*'            # string literal
    |"(?:[^"\\]|\\.)*"\s*\(        # quoted identifier used as a function name
    |--[^\n]*                         # line comment
    |/\*.*?\*/                       # block comment
    |\b(IDENT)\s*\(                   # function-call candidate
    """.replace("IDENT", SAFE_IDENT_RE.pattern),
    re.VERBOSE | re.DOTALL,
)
_LITERAL_OR_COMMENT = re.compile(r"'(?:[^'\\]|\\.|'')*'|--[^\n]*|/\*.*?\*/", re.DOTALL)


def _code_only(sql: str) -> str:
    """``sql`` with string literals and comments blanked, so keyword checks only see real code."""
    return _LITERAL_OR_COMMENT.sub(" ", sql)


_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_QUOTE_SCAN = re.compile(r"""'(?:[^']|'')*'|\"""")
_COMMENT_OR_ESCAPE = re.compile(r"--|/\*|\*/|#|\$|`")
_RESULT_CLAUSES = re.compile(
    r"\bSETTINGS\b|\bFORMAT\b(?!\s*\()|\bINTO\s+OUTFILE\b|\bINFILE\b|\bRECURSIVE\b", re.IGNORECASE
)


def _reject_ambiguous_syntax(sql: str, table_names: frozenset[str]) -> None:
    """Reject text DuckDB (which validates the SQL) and ClickHouse (which runs it) could tokenise differently."""
    if "\\" in sql:
        raise PermissionError("backslashes are not allowed in console queries")
    for token in _QUOTE_SCAN.finditer(sql):
        if token.group(0) == '"':
            raise PermissionError("double-quoted identifiers are not allowed in console queries")
    code = _STRING_LITERAL.sub(" ", sql)
    if "'" in code:
        raise PermissionError("unterminated string literal")
    names = "|".join(sorted(re.escape(name) for name in table_names | frozenset(_FACT_DOMAINS)))
    for pat, msg, flags in (
        (_COMMENT_OR_ESCAPE, "comments, '$' and backticks are not allowed in console queries", 0),
        (r"[^\x20-\x7e\n\r\t]", "non-ASCII characters are only allowed inside string literals", 0),
        (_RESULT_CLAUSES, "SETTINGS, FORMAT, INTO OUTFILE and RECURSIVE are not allowed", 0),
        (rf"\.\s*(?:{names})\b", "schema-qualified table names are not allowed", re.IGNORECASE),
    ):
        if re.search(pat, code, flags=flags):
            raise PermissionError(msg)


def _reject_unsafe_functions(sql: str) -> None:
    for match in _SQL_TOKEN.finditer(sql):
        token = match.group(0)
        name = match.group(1)
        if name is None:
            if token.startswith('"'):
                raise PermissionError("quoted function names are not allowed")
            continue
        lowered = name.lower()
        if lowered in _SQL_KEYWORDS or lowered in _ALLOWED_FUNCTIONS:
            continue
        raise PermissionError(f"function {name!r} is not allowed in console queries")


def query_endpoint(
    service: HighScaleService,
    req: DashboardQueryRequest,
    start_time: str | None,
    end_time: str | None,
    mask_ips: bool,
    *,
    session_id: str | None = None,
    client: Any | None = None,
    tables: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run a console query."""
    client = client if client is not None else service.client
    names = {canonical: (tables or {}).get(canonical, canonical) for canonical in _FACT_DOMAINS}
    timer = SectionTimer()
    section_timings = timer.entries
    raw_sql = req.sql.strip()
    max_rows = max(1, min(int(req.max_rows), MAX_QUERY_ROWS))
    svc_table = _safe_table(service.service_id)
    allowed_tables = frozenset({"logs", "client_vitals", "client_errors", svc_table.lower()})
    _t_val = time.perf_counter()
    _reject_ambiguous_syntax(raw_sql, allowed_tables | frozenset(names.values()))
    with duckdb.connect(":memory:") as parser_con:
        try:
            validate_user_sql(
                raw_sql,
                parser_con=parser_con,
                session_id=session_id,
                service_id=service.service_id,
                allowed_tables=allowed_tables,
            )
        except SQLValidationError as exc:
            raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t_val)
    sql = raw_sql.rstrip().rstrip(";")
    for pat, repl in (
        *([(rf"\b{re.escape(svc_table)}\b", names["request_facts"])] if svc_table != "logs" else []),
        (r"\blogs\b", names["request_facts"]),
        (r"\bclient_vitals\b", names["rum_vitals_facts"]),
        (r"\bclient_errors\b", names["rum_error_facts"]),
        (r"(?<!AS\s)(?<!')\btimestamp\b(?!')", "event_timestamp"),
        (r"(?<!AS\s)(?<!')\bip\b(?!')", "client_ip"),
    ):
        sql = re.sub(pat, repl, sql, flags=re.IGNORECASE)

    def _normalize_iso_literal(match: re.Match[str]) -> str:
        raw = match.group(1)
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        except ValueError:
            return match.group(0)

    sql = re.sub(
        r"'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)'", _normalize_iso_literal, sql
    )
    for field in _FIELD_DIMENSIONS.values():
        if field in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            continue
        sql = re.sub(rf"(?<!')\b{field}\b(?!')", f"custom_fields['{field}']", sql)
    for b_field in ("edge", "is_ipv6", "waf"):
        for pat, repl in (
            (r"IN\s*\(\s*'true'\s*\)", "IN ('true', '1')"),
            (r"NOT\s+IN\s*\(\s*'true'\s*\)", "NOT IN ('true', '1')"),
            (r"IN\s*\(\s*'false'\s*\)", "IN ('false', '0', '')"),
            (r"=\s*'true'", "IN ('true', '1')"),
            (r"=\s*'false'", "IN ('false', '0', '')"),
        ):
            sql = re.sub(
                rf"custom_fields\['{b_field}'\]\s*{pat}",
                f"lower(custom_fields['{b_field}']) {repl}",
                sql,
                flags=re.IGNORECASE,
            )
    for pat, repl in (
        (r"(custom_fields\['[a-zA-Z0-9_]+'\])\s*(=|!=|<>)\s*(-?\d+(?:\.\d+)?)\b", r"\1 \2 '\3'"),
        (r"(custom_fields\['[a-zA-Z0-9_]+'\])\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\b", r"toFloat64OrZero(\1) \2 \3"),
    ):
        sql = re.sub(pat, repl, sql)
    fact_tables = {
        names[canonical]: domain
        for canonical, domain in _FACT_DOMAINS.items()
        if re.search(rf"\b{re.escape(names[canonical])}\b", sql, flags=re.IGNORECASE)
    }
    has_explicit_limit = bool(re.search(r"\bLIMIT\b", _code_only(sql), flags=re.IGNORECASE))
    _reject_unsafe_functions(sql)
    row_cap = (MAX_QUERY_ROWS if has_explicit_limit else max_rows) + 1
    sql = f"SELECT * FROM (\n{sql}\n) LIMIT {row_cap}"
    settings = {
        "max_result_rows": str(row_cap),
        "result_overflow_mode": "break",
        "date_time_input_format": "best_effort",
    }
    if fact_tables:
        _assert_table_filters_enforced(client)
        scope = _table_filters(service.service_id, start_time, end_time, fact_tables, scoped_by_view=tables is not None)
        if scope is not None:
            settings["additional_table_filters"] = scope
    t0 = time.perf_counter()
    try:
        res = client.execute(sql, settings=settings)
    except Exception as e:
        raise RuntimeError(str(e)) from e
    timer.mark("execute", t0)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    compact_sql = _compact_sql_for_debug(sql.strip())
    _debug_queries = [{"sql": compact_sql, "time_ms": elapsed_ms, "engine": "ClickHouse"}]
    fetched_rows = len(res) if res else 0
    truncated = fetched_rows > max_rows
    if truncated:
        res = res[:max_rows]
    total_rows = -1 if (truncated and not has_explicit_limit) else fetched_rows
    _t_serialize = time.perf_counter()
    data: list[dict[str, Any]] = []
    cols = list(res[0].keys()) if res else []
    redact_keys = IP_FAMILY_KEYS | SESSION_ID_KEYS | {"client_ip"}
    for d in res:
        row_copy = dict(d)
        for k, v in row_copy.items():
            if hasattr(v, "isoformat"):
                val = v.isoformat()
                row_copy[k] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
            elif mask_ips and k.lower() in redact_keys and v:
                row_copy[k] = "[redacted]"
        data.append(row_copy)
    if mask_ips:
        data = mask_ip_values(data)
    timer.mark("serialize_json", _t_serialize)
    return {
        "columns": cols,
        "data": data,
        "row_count": len(data),
        "total_rows": total_rows,
        "truncated": truncated,
        "elapsed_ms": int(elapsed_ms),
        "debug_queries": _debug_queries,
        "_debug_queries": _debug_queries,
        "debug_calls": get_tracked_calls(),
        "section_timings": section_timings,
    }


import contextvars
import urllib.parse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from backend.core.clickhouse_client import ClickHouseError
from backend.core.share_db.validation import mask_ip
from backend.high_scale._common import (
    build_clickhouse_filters,
    parse_time_window,
    range_value,
    request_filter_clauses,
    resolve_asn_display_labels,
    visible_fact_clauses,
    visible_publication_fence_sql,
)
from backend.high_scale._common import format_iso_utc as _iso
from backend.high_scale._common import format_iso_utc_or_empty as _iso_or_empty
from backend.high_scale.insight_queries import (
    AUTH_REGEX,
    BOT_UA_REGEX,
    COLUMNS,
    COMPRESSIBLE_REGEX,
    DETECTIONS,
    POP_CODE,
    PROBE_REGEX,
    Detection,
    botnet_grouping,
    impossible_distance,
    repeated_patterns_fp,
)
from backend.models.dashboard import SessionDetailRequest, SessionsRequest
from backend.models.insights import CacheCollapseDetailResponse, InsightsResponse
from backend.repositories.insights import InsightDefinition, registry  # noqa: F401  (registers the detections)
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    client_cache_token,
    digest_cache_key,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc

_MAX_WORKERS = 12
_FIELD_SAMPLE_ROWS = 1000
_BASE_FIELDS = frozenset({"timestamp", "url", "country", "ip"})
_ASN_INSIGHT_MARKERS = ("asn", "metro")
_INSIGHTS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=200, ttl_seconds=300)
_CacheRegistry.register("high_scale.insights._INSIGHTS_CACHE", _INSIGHTS_CACHE)


def _window(
    window_hours: float, baseline_hours: float, clamp_start: str | None, clamp_end: str | None
) -> tuple[datetime, datetime, datetime]:
    """``(baseline_start, window_start, now)``, honouring the analyst clamp exactly like the standard path."""
    now = datetime.now(UTC)
    if clamp_end:
        ceiling = parse_iso_utc(clamp_end)
        if ceiling and ceiling < now:
            now = ceiling
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(hours=baseline_hours + window_hours)
    if clamp_start:
        floor = parse_iso_utc(clamp_start)
        if floor:
            baseline_start = max(baseline_start, floor)
            window_start = max(window_start, floor)
    return baseline_start, window_start, now


def _base_sql(detection: Detection, insight_id: str) -> str:
    projection = ", ".join(f"{COLUMNS[column]} AS {column}" for column in detection.columns)
    lower = "{window_start:DateTime64(3)}" if detection.window_only else "{baseline_start:DateTime64(3)}"
    row_filter = f" AND ({detection.row_filter})" if detection.row_filter else ""
    scope = " AND ".join(visible_fact_clauses("request"))
    return (
        f"/* insight:{insight_id} */ WITH base AS (SELECT {projection}, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b, "
        "event_timestamp >= {window_start:DateTime64(3)} AS is_w "
        f"FROM request_facts WHERE {scope} AND event_timestamp >= {lower} "
        f"AND event_timestamp <= {{window_end:DateTime64(3)}}{row_filter}) {detection.sql}"
    )


def _fingerprint_column(present: set[str]) -> str | None:
    for column in ("ja4", "ja3"):
        if column in present:
            return column
    return None


def _detection_for(insight_id: str, present: set[str]) -> Detection | None:
    """The ClickHouse query for ``insight_id``, or None when this service cannot compute it."""
    fingerprint = _fingerprint_column(present)
    if insight_id == "botnet_grouping":
        return botnet_grouping(fingerprint) if fingerprint else None
    if insight_id == "repeated_patterns_fp":
        return repeated_patterns_fp(fingerprint) if fingerprint else None
    if insight_id == "impossible_distance":
        from backend.utils.pop_utils import get_pop_lat_lon_map

        pops = {
            code: (float(lat), float(lon))
            for code, (lat, lon) in get_pop_lat_lon_map().items()
            if lat is not None and lon is not None and POP_CODE.fullmatch(code)
        }
        if not pops or not fingerprint:
            return None
        return impossible_distance(pops, fingerprint, edge_only="edge" in present)
    return DETECTIONS.get(insight_id)


def _make_investigate_url(
    service_id: str, window_start: datetime, now: datetime
) -> Callable[[dict[str, Any] | None], str]:
    def build(filters: dict[str, Any] | None = None) -> str:
        pairs: list[tuple[str, str]] = [
            ("service", service_id),
            ("start", window_start.isoformat()),
            ("end", now.isoformat()),
        ]
        pairs.extend((f"filter_{column}", str(value)) for column, value in (filters or {}).items() if value is not None)
        return "/dashboard?" + urllib.parse.urlencode(pairs)

    return build


def _severity(items: list[dict[str, Any]]) -> str:
    if not items:
        return "clean"
    return "critical" if any(item.get("severity") == "critical" for item in items) else "warning"


def _summary(definition: InsightDefinition, items: list[dict[str, Any]]) -> str:
    if not items:
        return f"No {definition.title.lower()} detected"
    if definition.id == "error_spikes":
        return f"{len(items)} URLs with elevated server error rates"
    if definition.id == "botnet_grouping":
        return f"{len(items)} fingerprints with suspicious IP spread"
    return f"{len(items)} anomalies detected"


def _card(definition: InsightDefinition, severity: str, summary: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": definition.id,
        "title": definition.title,
        "description": definition.description,
        "category": str(definition.category),
        "severity": severity,
        "summary": summary,
        "items": items,
    }


def _failure_message(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ClickHouseError) else type(exc).__name__


def _asn_context(service_id: str, rows: list[tuple[Any, ...]], insight_id: str) -> dict[str, Any]:
    if not rows or not any(marker in insight_id for marker in _ASN_INSIGHT_MARKERS):
        return {}
    from backend.core import duckdb as _db

    context: dict[str, Any] = {"asn_names": _db.get_asn_names(service_id, [row[0] for row in rows if row])}
    if "metro" in insight_id:
        context["dma_map"] = _db._get_dma_map()
    return context


def insights_endpoint(
    service: HighScaleService,
    req: Any,
    clamp_start: str | None,
    clamp_end: str | None,
    mask_ips: bool = False,
    clamp_cache_key: str | None = None,
    force_refresh: bool = False,
) -> InsightsResponse:
    window_hours = float(req.window_size_hrs)
    baseline_hours = float(req.baseline_hours)
    req_filters = getattr(req, "filters", None)
    filter_clause, filter_params = build_clickhouse_filters(req_filters) if req_filters else ("", {})
    if clamp_cache_key is not None:
        key_clamp = clamp_cache_key
    elif clamp_start or clamp_end:
        key_clamp = f"{bucket_time_to_minute(clamp_start) or ''}:{bucket_time_to_minute(clamp_end) or ''}"
    else:
        key_clamp = ""
    cache_key = (
        service.service_id,
        id(service.client),
        window_hours,
        baseline_hours,
        key_clamp,
        int(mask_ips),
        filter_clause,
        tuple(sorted((k, str(v)) for k, v in filter_params.items())),
    )
    from backend.high_scale.ingest_controller import HighScaleService

    use_cache = isinstance(service, HighScaleService)
    if use_cache and not force_refresh:
        cached = _INSIGHTS_CACHE.get(cache_key)
        if cached is not None:
            return InsightsResponse.with_telemetry(**cached, is_cached=True, cache_age_seconds=0)
    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    scope = " AND ".join(visible_fact_clauses("request"))
    if filter_clause:
        scope = f"{scope} AND ({filter_clause})"
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "probe_regex": PROBE_REGEX,
        "compress_regex": COMPRESSIBLE_REGEX,
        "auth_regex": AUTH_REGEX,
        "bot_ua_regex": BOT_UA_REGEX,
        **filter_params,
    }
    in_range = "event_timestamp >= {baseline_start:DateTime64(3)} AND event_timestamp <= {window_end:DateTime64(3)}"
    history_sql = (
        "/* insight:history */ SELECT min(event_timestamp) AS earliest, "
        "countIf(event_timestamp >= {window_start:DateTime64(3)}) AS window_total "
        f"FROM request_facts WHERE {scope} AND {in_range}"
    )
    sample_sql = (
        "/* insight:fields */ SELECT groupUniqArrayArray(mapKeys(custom_fields)) AS fields FROM ("
        f"SELECT custom_fields FROM request_facts WHERE {scope} AND {in_range} "
        f"ORDER BY event_timestamp DESC LIMIT {_FIELD_SAMPLE_ROWS})"
    )
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-insights-meta") as meta_pool:
        fut_history = meta_pool.submit(
            contextvars.copy_context().run, lambda: service.client.execute(history_sql, params)
        )
        fut_sample = meta_pool.submit(
            contextvars.copy_context().run, lambda: service.client.execute(sample_sql, params)
        )
        history = fut_history.result()
        sample = fut_sample.result()
    earliest = history[0].get("earliest") if history else None
    window_total = int(history[0].get("window_total") or 0) if history else 0
    earliest_dt = parse_iso_utc(earliest) if isinstance(earliest, str) else earliest
    if isinstance(earliest_dt, datetime):
        earliest_dt = earliest_dt.astimezone(UTC) if earliest_dt.tzinfo else earliest_dt.replace(tzinfo=UTC)
        history_hours = (now - earliest_dt).total_seconds() / 3600.0
    else:
        history_hours = 0.0
    present = set(_BASE_FIELDS)
    present.update(sample[0].get("fields") or [] if sample else [])
    fingerprint = _fingerprint_column(present) or "ja3"
    make_investigate_url = _make_investigate_url(service.service_id, window_start, now)

    def run(definition: InsightDefinition, detection: Detection) -> dict[str, Any]:
        if detection.needs_baseline and history_hours < baseline_hours:
            available = max(0.1, round(history_hours, 1))
            return _card(
                definition,
                "info",
                f"Requires {int(baseline_hours)}h of historical data (only {available}h available)",
                [],
            )
        try:
            sql = _base_sql(detection, definition.id)
            if filter_clause:
                sql = sql.replace(
                    "AND event_timestamp <= {window_end:DateTime64(3)}",
                    f"AND event_timestamp <= {{window_end:DateTime64(3)}} AND ({filter_clause})",
                )
            rows = [tuple(row.values()) for row in service.client.execute(sql, params)]
            context = {
                "window_hours": window_hours,
                "baseline_hours": baseline_hours,
                "fp_col": fingerprint,
                "actual_cols": sorted(present),
                "mask_ips": mask_ips,
                **_asn_context(service.service_id, rows, definition.id),
            }
            items: list[dict[str, Any]] = []
            for row in rows:
                item = definition.row_processor(row, definition, context) if definition.row_processor else {}
                if "investigate_url" not in item:
                    item["investigate_url"] = make_investigate_url(item.get("meta", {}).get("filters"))
                items.append(item)
        except Exception as exc:
            return _card(definition, "error", f"Query failed: {_failure_message(exc)}", [])
        severity = definition.severity_logic(items) if items and definition.severity_logic else _severity(items)
        return _card(definition, severity, _summary(definition, items), items)

    candidates: list[tuple[InsightDefinition, Detection]] = []
    for definition in registry.get_all():
        detection = _detection_for(definition.id, present)
        if detection is not None and all(column in present for column in definition.required_fields):
            candidates.append((definition, detection))
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="hs-insights") as pool:
        futures = [pool.submit(contextvars.copy_context().run, lambda pair=pair: run(*pair)) for pair in candidates]
        cards = [future.result() for future in futures]
    computed_at = now.isoformat()
    payload = {
        "insights": cards,
        "window_start": window_start.isoformat(),
        "window_end": computed_at,
        "baseline_start": baseline_start.isoformat(),
        "baseline_end": window_start.isoformat(),
        "computed_at": computed_at,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "window_total_requests": window_total,
    }
    if use_cache and not any(c.get("severity") == "error" for c in cards):
        _INSIGHTS_CACHE[cache_key] = payload
    return InsightsResponse.with_telemetry(**payload)


def _bucket_interval(total_hours: float) -> str:
    if total_hours <= 2:
        return "INTERVAL 1 MINUTE"
    if total_hours <= 12:
        return "INTERVAL 5 MINUTE"
    if total_hours <= 48:
        return "INTERVAL 15 MINUTE"
    if total_hours <= 168:
        return "INTERVAL 1 HOUR"
    if total_hours <= 720:
        return "INTERVAL 4 HOUR"
    return "INTERVAL 1 DAY"


def cache_collapse_detail_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None, mask_ips: bool = False
) -> CacheCollapseDetailResponse:
    """Hit/miss timeline, disposition breakdown and recent misses for one URL, as on the standard path."""
    window_hours = float(req.window_size_hrs)
    baseline_hours = float(req.baseline_hours)
    baseline_start, window_start, now = _window(window_hours, baseline_hours, clamp_start, clamp_end)
    scope = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "url = {url:String}",
            "event_timestamp >= {baseline_start:DateTime64(3)}",
            "event_timestamp <= {window_end:DateTime64(3)}",
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "url": req.url,
        "baseline_start": baseline_start,
        "window_start": window_start,
        "window_end": now,
    }
    cache = "custom_fields['cache']"
    hit, miss, passed = (f"startsWith({cache}, '{kind}')" for kind in ("HIT", "MISS", "PASS"))
    rates = service.client.execute(
        "/* insight:cache_collapse_rates */ SELECT "
        f"countIf(is_w AND {hit}) AS w_hits, countIf(is_w AND {miss}) AS w_miss, "
        f"countIf(is_w AND {passed}) AS w_pass, countIf(is_w) AS w_total, "
        f"countIf(is_b AND {hit}) AS b_hits, countIf(is_b AND {miss}) AS b_miss, "
        f"countIf(is_b AND {passed}) AS b_pass, countIf(is_b) AS b_total "
        "FROM (SELECT custom_fields, event_timestamp >= {window_start:DateTime64(3)} AS is_w, "
        "event_timestamp < {window_start:DateTime64(3)} AS is_b "
        f"FROM request_facts WHERE {scope})",
        params,
    )[0]
    w_hits, w_miss, w_pass, w_total, b_hits, b_miss, b_pass, b_total = (
        int(rates[key] or 0)
        for key in ("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total")
    )
    w_cacheable, b_cacheable = w_hits + w_miss, b_hits + b_miss
    w_rate = w_hits / w_cacheable if w_cacheable else 0.0
    b_rate = b_hits / b_cacheable if b_cacheable else 0.0
    timeline_rows = service.client.execute(
        "/* insight:cache_collapse_timeline */ SELECT "
        f"toStartOfInterval(event_timestamp, {_bucket_interval(window_hours + baseline_hours)}) AS bucket, "
        f"count() AS total_requests, countIf({hit}) AS real_hits, countIf({miss}) AS misses "
        f"FROM (SELECT event_timestamp, custom_fields FROM request_facts WHERE {scope}) "
        "GROUP BY bucket ORDER BY bucket ASC",
        params,
    )
    timeline = []
    for row in timeline_rows:
        real_hits, misses = int(row["real_hits"] or 0), int(row["misses"] or 0)
        cacheable = real_hits + misses
        timeline.append(
            {
                "bucket": _iso_or_empty(row["bucket"]),
                "expected_hits": float(cacheable * b_rate),
                "real_hits": real_hits,
                "misses": misses,
                "total_requests": int(row["total_requests"] or 0),
                "hit_rate": float(real_hits / cacheable) if cacheable else 0.0,
            }
        )
    miss_rows = service.client.execute(
        "/* insight:cache_collapse_misses */ SELECT event_timestamp AS timestamp, "
        f"{cache} AS cache, custom_fields['pop'] AS pop, client_ip AS ip, toInt32OrNull(custom_fields['status']) AS status "
        f"FROM request_facts WHERE {scope} AND {miss} ORDER BY event_timestamp DESC LIMIT 50",
        params,
    )
    recent_misses = [
        {
            "timestamp": _iso_or_empty(row["timestamp"]),
            "cache": row.get("cache") or "UNKNOWN",
            "pop": row.get("pop") or None,
            "ip": mask_ip(row["ip"]) if mask_ips and row.get("ip") else row.get("ip"),
            "status": int(row["status"]) if row.get("status") is not None else None,
        }
        for row in miss_rows
    ]
    return CacheCollapseDetailResponse.with_telemetry(
        url=req.url,
        has_data=bool(w_total + b_total),
        timeline=timeline,
        recent_misses=recent_misses,
        breakdown={
            "hits": w_hits,
            "misses": w_miss,
            "passes": w_pass,
            "other": max(0, w_total - w_hits - w_miss - w_pass),
        },
        baseline_hit_rate=float(b_rate) * 100,
        window_hit_rate=float(w_rate) * 100,
        baseline_pass_rate=float(b_pass / b_total) * 100 if b_total else 0.0,
        window_pass_rate=float(w_pass / w_total) * 100 if w_total else 0.0,
    )


_sessions_memo = ResponseMemo(ttl=30.0, maxsize=128)
_CacheRegistry.register("high_scale.sessions._response_cache", _sessions_memo.cache)
_DEFAULT_MIN_REQS_FLAG = 1000
_DEFAULT_MIN_4XX_PCT_FLAG = 20.0
_SORTABLE = frozenset(
    {
        "session_start",
        "session_end",
        "req_count",
        "edge_count",
        "shield_count",
        "unique_urls",
        "median_rtt_ms",
        "total_bytes",
    }
)
_HELPER_COLUMNS = (
    "sid",
    "total_sessions",
    "any_rtt",
    "any_ja4",
    "any_edge",
    "any_edge_sid",
    "any_cmcd",
    "streaming_reqs",
    "has_rtt_row",
    "has_edge_sid_row",
)
_DETAIL_LIMIT = 500
_FULL_CTE_SORTS = frozenset({"unique_urls", "median_rtt_ms"})
_SESSIONS_FAST_CTE = """
    WITH raw_events AS (
        SELECT
            client_ip AS ip,
            custom_fields['ja4'] AS ja4,
            event_timestamp AS ts,
            custom_fields['edge'] AS edge,
            toUInt8(custom_fields['edge_sid'] != '') AS has_edge_sid_ev,
            toInt32OrZero(custom_fields['status']) AS status,
            toInt64OrZero(custom_fields['resp_bytes']) AS resp_bytes,
            toUInt8(toFloat64OrNull(custom_fields['tcp_rtt']) IS NOT NULL) AS has_rtt_ev,
            cmcd['sid'] AS cmcd_sid
        FROM request_facts
        WHERE {where}
    ),
    base AS (
        SELECT
            ip,
            ja4,
            toStartOfMinute(ts) AS bucket_ts,
            min(ts) AS min_ts,
            max(ts) AS max_ts,
            count() AS req_count,
            sum(if(status >= 400 AND status < 500, 1, 0)) AS reqs_4xx,
            sum(if(status >= 500, 1, 0)) AS reqs_5xx,
            sum(resp_bytes) AS total_bytes,
            sum(if(lower(edge) IN ('1', 'true'), 1, 0)) AS edge_count,
            sum(if(lower(edge) IN ('0', 'false'), 1, 0)) AS shield_count,
            max(has_rtt_ev) AS has_rtt_row,
            max(has_edge_sid_ev) AS has_edge_sid_row,
            sum(if(cmcd_sid != '', 1, 0)) AS streaming_reqs
        FROM raw_events
        GROUP BY ip, ja4, bucket_ts
    ),
    gaps AS (
        SELECT
            *,
            dateDiff('second', lagInFrame(max_ts) OVER (PARTITION BY ip, ja4 ORDER BY min_ts), min_ts) AS gap
        FROM base
    ),
    sessions_raw AS (
        SELECT
            *,
            sum(if(gap IS NULL OR gap > 1800, 1, 0)) OVER (
                PARTITION BY ip, ja4 ORDER BY min_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS sid
        FROM gaps
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            min(min_ts) AS session_start,
            max(max_ts) AS session_end,
            sum(req_count) AS req_count,
            sum(reqs_4xx) AS reqs_4xx,
            sum(reqs_5xx) AS reqs_5xx,
            sum(total_bytes) AS total_bytes,
            sum(edge_count) AS edge_count,
            sum(shield_count) AS shield_count,
            max(has_rtt_row) AS has_rtt_row,
            max(has_edge_sid_row) AS has_edge_sid_row,
            sum(streaming_reqs) AS streaming_reqs,
            sid
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    ),
    flagged_sessions AS (
        SELECT
            *,
            ((req_count >= {{min_reqs_flag:UInt32}})
                OR (req_count > 0 AND (reqs_4xx * 100.0 / req_count) >= {{min_4xx_pct_flag:Float64}})) AS flagged,
            (streaming_reqs > 0) AS is_streaming
        FROM sessions_agg
    )
"""
_SESSIONS_CTE = """
    WITH raw_events AS (
        SELECT
            client_ip AS ip,
            custom_fields['ja4'] AS ja4,
            event_timestamp AS ts,
            country,
            toInt32OrNull(custom_fields['asn']) AS asn,
            url,
            custom_fields['edge'] AS edge,
            custom_fields['edge_sid'] AS edge_sid,
            custom_fields['ua'] AS ua,
            toInt32OrZero(custom_fields['status']) AS status,
            toInt64OrZero(custom_fields['resp_bytes']) AS resp_bytes,
            toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt,
            cmcd['sid'] AS cmcd_sid
        FROM request_facts
        WHERE {where}
    ),
    base AS (
        SELECT
            ip,
            ja4,
            toStartOfMinute(ts) AS bucket_ts,
            min(ts) AS min_ts,
            max(ts) AS max_ts,
            count() AS req_count,
            min(asn) AS asn,
            min(country) AS country,
            uniqState(url) AS url_state,
            min(ua) AS ua,
            sum(if(status >= 400 AND status < 500, 1, 0)) AS reqs_4xx,
            sum(if(status >= 500, 1, 0)) AS reqs_5xx,
            sum(resp_bytes) AS total_bytes,
            medianState(tcp_rtt) AS rtt_state,
            sum(if(lower(edge) IN ('1', 'true'), 1, 0)) AS edge_count,
            sum(if(lower(edge) IN ('0', 'false'), 1, 0)) AS shield_count,
            max(edge_sid) AS edge_sid,
            sum(if(cmcd_sid != '', 1, 0)) AS streaming_reqs
        FROM raw_events
        GROUP BY ip, ja4, bucket_ts
    ),
    gaps AS (
        SELECT
            *,
            dateDiff('second', lagInFrame(max_ts) OVER (PARTITION BY ip, ja4 ORDER BY min_ts), min_ts) AS gap
        FROM base
    ),
    sessions_raw AS (
        SELECT
            *,
            sum(if(gap IS NULL OR gap > 1800, 1, 0)) OVER (
                PARTITION BY ip, ja4 ORDER BY min_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS sid
        FROM gaps
    ),
    sessions_agg AS (
        SELECT
            ip,
            ja4,
            min(min_ts) AS session_start,
            max(max_ts) AS session_end,
            sum(req_count) AS req_count,
            min(asn) AS asn,
            min(country) AS country,
            uniqMerge(url_state) AS unique_urls,
            min(ua) AS ua,
            sum(reqs_4xx) AS reqs_4xx,
            sum(reqs_5xx) AS reqs_5xx,
            sum(total_bytes) AS total_bytes,
            medianMerge(rtt_state) / 1000.0 AS median_rtt_ms,
            sum(edge_count) AS edge_count,
            sum(shield_count) AS shield_count,
            max(edge_sid) AS edge_sid,
            toUInt8(medianMerge(rtt_state) IS NOT NULL) AS has_rtt_row,
            toUInt8(max(edge_sid) != '') AS has_edge_sid_row,
            sum(streaming_reqs) AS streaming_reqs,
            sid
        FROM sessions_raw
        GROUP BY ip, ja4, sid
    ),
    flagged_sessions AS (
        SELECT
            *,
            ((req_count >= {{min_reqs_flag:UInt32}})
                OR (req_count > 0 AND (reqs_4xx * 100.0 / req_count) >= {{min_4xx_pct_flag:Float64}})) AS flagged,
            (streaming_reqs > 0) AS is_streaming
        FROM sessions_agg
    )
"""
_FILTERED = """
    FROM flagged_sessions
    WHERE (({streaming_only:UInt8} = 0) OR is_streaming = 1)
      AND (({flagged_only:UInt8} = 0) OR flagged = 1)
"""
_asn_label_map = resolve_asn_display_labels


def _as_dt(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        return range_value(val) if val else None
    return None


def _enrich_paginated_sessions(
    service: HighScaleService, rows: list[dict[str, Any]], where: str, params: dict[str, Any]
) -> None:
    tuples_sql_parts: list[str] = []
    enrich_params: dict[str, Any] = dict(params)
    min_start: datetime | None = None
    max_end: datetime | None = None
    unique_ips: list[str] = []
    seen_ips: set[str] = set()
    for idx, row in enumerate(rows):
        row.setdefault("asn", None)
        row.setdefault("country", "")
        row.setdefault("unique_urls", 0)
        row.setdefault("ua", "")
        row.setdefault("median_rtt_ms", None)
        row.setdefault("edge_sid", "")
        s_dt = _as_dt(row.get("session_start"))
        e_dt = _as_dt(row.get("session_end"))
        if s_dt is None or e_dt is None:
            continue
        if min_start is None or s_dt < min_start:
            min_start = s_dt
        if max_end is None or e_dt > max_end:
            max_end = e_dt
        ip_val = str(row.get("ip") or "")
        ja4_val = str(row.get("ja4") or "")
        if ip_val not in seen_ips:
            seen_ips.add(ip_val)
            unique_ips.append(ip_val)
        enrich_params[f"en_ip_{idx}"] = ip_val
        enrich_params[f"en_ja4_{idx}"] = ja4_val
        enrich_params[f"en_s_{idx}"] = s_dt
        enrich_params[f"en_e_{idx}"] = e_dt
        tuples_sql_parts.append(
            f"tuple(toUInt32({idx}), {{en_ip_{idx}:String}}, {{en_ja4_{idx}:String}}, "
            f"{{en_s_{idx}:DateTime64(3)}}, {{en_e_{idx}:DateTime64(3)}})"
        )
    if not tuples_sql_parts or not unique_ips:
        return
    if min_start is not None:
        enrich_params["start_time"] = min_start
    if max_end is not None:
        enrich_params["end_time"] = max_end
    ip_placeholders: list[str] = []
    for i, ip_val in enumerate(unique_ips):
        enrich_params[f"en_uip_{i}"] = ip_val
        ip_placeholders.append(f"{{en_uip_{i}:String}}")
    enrich_sql = (
        "/* sessions:enrich */ "
        "WITH page_keys AS ("
        "    SELECT t.1 AS pk_idx, t.2 AS pk_ip, t.3 AS pk_ja4, t.4 AS pk_start, t.5 AS pk_end "
        f"    FROM (SELECT arrayJoin([{', '.join(tuples_sql_parts)}]) AS t)"
        ") "
        "SELECT "
        "    pk.pk_idx AS row_idx, "
        "    min(toInt32OrNull(custom_fields['asn'])) AS asn, "
        "    min(country) AS country, "
        "    uniq(url) AS unique_urls, "
        "    min(custom_fields['ua']) AS ua, "
        "    median(toFloat64OrNull(custom_fields['tcp_rtt'])) / 1000.0 AS median_rtt_ms, "
        "    max(custom_fields['edge_sid']) AS edge_sid "
        "FROM request_facts "
        "INNER JOIN page_keys AS pk "
        "    ON client_ip = pk.pk_ip AND custom_fields['ja4'] = pk.pk_ja4 "
        f"WHERE {where} "
        f"  AND client_ip IN ({', '.join(ip_placeholders)}) "
        "  AND event_timestamp >= pk.pk_start "
        "  AND event_timestamp <= pk.pk_end "
        "GROUP BY pk.pk_idx"
    )
    enriched_rows = service.client.execute(enrich_sql, enrich_params)
    for en_row in enriched_rows:
        idx = int(en_row["row_idx"])
        if 0 <= idx < len(rows):
            rows[idx]["asn"] = en_row.get("asn")
            rows[idx]["country"] = en_row.get("country") or ""
            rows[idx]["unique_urls"] = int(en_row.get("unique_urls") or 0)
            rows[idx]["ua"] = en_row.get("ua") or ""
            rows[idx]["median_rtt_ms"] = en_row.get("median_rtt_ms")
            rows[idx]["edge_sid"] = en_row.get("edge_sid") or ""


def sessions_endpoint(
    service: HighScaleService, req: SessionsRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    start_dt, end_dt = parse_time_window(start_time, end_time, timedelta(days=7))
    min_reqs_flag = req.min_reqs_flag if req.min_reqs_flag is not None else _DEFAULT_MIN_REQS_FLAG
    min_4xx_pct_flag = req.min_4xx_pct_flag if req.min_4xx_pct_flag is not None else _DEFAULT_MIN_4XX_PCT_FLAG
    filter_clauses, filter_params = request_filter_clauses(req.filters)
    where = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "event_timestamp >= {start_time:DateTime64(3)}",
            "event_timestamp <= {end_time:DateTime64(3)}",
            *filter_clauses,
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
        "flagged_only": 1 if req.flagged_only else 0,
        "streaming_only": 1 if req.streaming_only else 0,
        **filter_params,
    }
    sort_by = req.sort_by if req.sort_by in _SORTABLE else "session_start"
    sort_dir = "ASC" if str(req.sort_dir).lower() == "asc" else "DESC"
    fast_cte = _SESSIONS_FAST_CTE.format(where=where)
    cte = _SESSIONS_CTE.format(where=where) if sort_by in _FULL_CTE_SORTS else fast_cte
    memo_key = digest_cache_key(
        {
            "cid": client_cache_token(service.client),
            "s": bucket_time_to_minute(start_time) if start_time else start_dt.strftime("%Y-%m-%dT%H:%M"),
            "e": bucket_time_to_minute(end_time) if end_time else end_dt.strftime("%Y-%m-%dT%H:%M"),
            "p": req.page,
            "l": req.limit,
            "sb": sort_by,
            "sd": sort_dir,
            "mr": min_reqs_flag,
            "m4": min_4xx_pct_flag,
            "fo": bool(req.flagged_only),
            "so": bool(req.streaming_only),
            "fc": list(filter_clauses),
            "fp": sorted((k, str(v)) for k, v in filter_params.items()),
        },
        {"service_id": service.service_id},
    )
    cached = _sessions_memo.get(memo_key)
    if cached is not None:
        return cached
    rows = [
        dict(r)
        for r in service.client.execute(
            "/* sessions:page */ "
            + cte
            + """
    SELECT *,
           count() OVER () AS total_sessions,
           max(has_rtt_row) OVER () AS any_rtt,
           max(ja4 != '') OVER () AS any_ja4,
           max(edge_count + shield_count > 0) OVER () AS any_edge,
           max(has_edge_sid_row) OVER () AS any_edge_sid,
           max(streaming_reqs > 0) OVER () AS any_cmcd
    FROM flagged_sessions
    WHERE (({streaming_only:UInt8} = 0) OR is_streaming = 1)
      AND (({flagged_only:UInt8} = 0) OR flagged = 1)
    """
            + f"ORDER BY {sort_by} {sort_dir} LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            {**params, "limit": req.limit, "offset": (req.page - 1) * req.limit},
        )
    ]
    if rows and "unique_urls" not in rows[0]:
        _enrich_paginated_sessions(service, rows, where, params)
    if rows:
        total = int(rows[0]["total_sessions"])
    elif req.page > 1:
        counted = service.client.execute(
            "/* sessions:count */ " + fast_cte + "SELECT count() AS total_sessions" + _FILTERED, params
        )
        total = int(counted[0]["total_sessions"]) if counted else 0
    else:
        total = 0
    first = rows[0] if rows else {}
    sessions: list[dict[str, Any]] = []
    for row in rows:
        session = {key: value for key, value in row.items() if key not in _HELPER_COLUMNS}
        session["session_start"] = _iso(session.get("session_start"))
        session["session_end"] = _iso(session.get("session_end"))
        session["flagged"] = bool(session.get("flagged"))
        session["is_streaming"] = bool(session.get("is_streaming"))
        sessions.append(session)
    asns = sorted({int(session["asn"]) for session in sessions if session.get("asn") is not None})
    result = {
        "sessions": sessions,
        "asn_names": _asn_label_map(service.service_id, asns) if asns else {},
        "total": total,
        "page": req.page,
        "limit": req.limit,
        "has_rtt": bool(first.get("any_rtt")),
        "has_ja4": bool(first.get("any_ja4")),
        "has_edge": bool(first.get("any_edge")),
        "has_edge_sid": bool(first.get("any_edge_sid")),
        "has_cmcd": bool(first.get("any_cmcd")),
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
    }
    _sessions_memo.put(memo_key, result)
    return result


def sessions_detail(
    service: HighScaleService,
    req: SessionDetailRequest,
    start_time: str | None,
    end_time: str | None,
    ip: str,
    ja4: str | None,
) -> dict[str, Any]:
    start_dt, end_dt = parse_time_window(start_time, end_time, timedelta(days=7))
    clauses = [
        *visible_fact_clauses("request"),
        "client_ip = {ip:String}",
        "event_timestamp >= {start_time:DateTime64(3)}",
        "event_timestamp <= {end_time:DateTime64(3)}",
    ]
    params: dict[str, Any] = {"service_id": service.service_id, "start_time": start_dt, "end_time": end_dt, "ip": ip}
    if ja4:
        clauses.append("custom_fields['ja4'] = {ja4:String}")
        params["ja4"] = ja4
    data = service.client.execute(
        """
    /* sessions:detail */
    SELECT
        client_ip AS ip,
        custom_fields['ja4'] AS ja4,
        event_timestamp AS timestamp,
        country,
        url,
        toInt32OrZero(custom_fields['status']) AS status,
        custom_fields['method'] AS method,
        custom_fields['protocol'] AS protocol,
        toInt64OrZero(custom_fields['resp_bytes']) AS resp_bytes,
        toFloat64OrZero(custom_fields['tcp_rtt']) AS tcp_rtt,
        custom_fields['edge'] AS edge,
        custom_fields['edge_sid'] AS edge_sid,
        custom_fields['ua'] AS ua,
        toInt32OrNull(custom_fields['asn']) AS asn,
        nullIf(cmcd['ot'], '') AS cmcd_ot,
        toInt64OrNull(cmcd['br']) AS cmcd_br,
        toInt64OrNull(cmcd['bl']) AS cmcd_bl,
        nullIf(cmcd['sf'], '') AS cmcd_sf,
        nullIf(cmcd['st'], '') AS cmcd_st,
        toInt64OrNull(cmcd['d']) AS cmcd_d,
        toInt64OrNull(cmcd['mtp']) AS cmcd_mtp,
        toInt64OrNull(cmcd['tb']) AS cmcd_tb,
        if(cmcd['bs'] = '', NULL, lower(cmcd['bs']) IN ('true', '1')) AS cmcd_bs,
        if(cmcd['su'] = '', NULL, lower(cmcd['su']) IN ('true', '1')) AS cmcd_su,
        nullIf(cmcd['cid'], '') AS cmcd_cid,
        nullIf(cmcd['sid'], '') AS cmcd_sid,
        toInt32OrNull(cmcd['v']) AS cmcd_v
    FROM request_facts
    WHERE """
        + " AND ".join(clauses)
        + f" ORDER BY timestamp ASC LIMIT {_DETAIL_LIMIT}",
        params,
    )
    for row in data:
        if row.get("timestamp"):
            row["timestamp"] = _iso(row["timestamp"])
    return {"data": data, "columns": list(data[0].keys()) if data else []}


@dataclass(frozen=True)
class AggregateResponse:
    service_id: str
    domain: str
    request_count: int
    top_values: tuple[tuple[str, int], ...]
    coverage: float
    watermark: ServingWatermark
    freshness_lag_seconds: float
    exact: bool
    approximation_error: float | None
    counters: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class AggregateRequest:
    service_id: str
    domain: str
    start: datetime | None = None
    end: datetime | None = None
    dimension: str | None = None

    def validate(self) -> None:
        if not self.service_id.strip():
            raise ValueError("service id is required")
        if self.domain not in {"request", "rum_vitals", "rum_errors", "cmcd"}:
            raise ValueError("unsupported aggregate domain")
        if (self.start is None) != (self.end is None):
            raise ValueError("aggregate range must include both start and end")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("aggregate range is inverted")


class AggregateQueryClient(Protocol):
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


_DOMAIN_TABLES = {
    "request": ("request_aggregates", "request_count"),
    "rum_vitals": ("rum_vitals_aggregates", "event_count"),
    "rum_errors": ("rum_error_aggregates", "error_count"),
    "cmcd": ("cmcd_aggregates", "event_count"),
}
_INTERNAL_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
DEFAULT_AGGREGATE_DIMENSIONS = {
    "request": "url",
    "rum_vitals": "metric_name",
    "rum_errors": "error_message",
    "cmcd": "cmcd_session",
}
_FACT_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
    "cmcd": "cmcd_projection_facts",
}
_REQUEST_CUSTOM_FIELD_DIMENSIONS = (
    "asn",
    "host",
    "method",
    "status",
    "cache",
    "proto",
    "ua",
    "referer",
    "cookie_session",
    "resp_header_content_encoding",
    "ttl",
    "age",
    "hits",
    "digest",
    "city",
    "region",
    "metro",
    "transport",
    "c_speed",
    "c_type",
    "pop",
    "backend",
    "edge",
    "server_region",
    "tls",
    "is_ipv6",
    "conn_requests",
    "waf",
    "waf_resp",
    "waf_ms",
    "p_type",
    "p_desc",
    "ja3",
    "ja4",
    "tls_ciphers_sha",
    "io_input_format",
    "io_output_format",
)
_FACT_DIMENSION_EXPRESSIONS = {
    "request": {
        "url": "url",
        "country": "country",
        "client_ip": "client_ip",
        **{dimension: f"custom_fields['{dimension}']" for dimension in _REQUEST_CUSTOM_FIELD_DIMENSIONS},
        "cmcd_session": "cmcd['sid']",
    },
    "rum_vitals": {"metric_name": "metric_name"},
    "rum_errors": {"error_message": "error_message"},
    "cmcd": {"cmcd_session": "cmcd['sid']"},
}


def _is_partial_minute_window(start: datetime | None, end: datetime | None) -> bool:
    if start is None or end is None:
        return False
    return any(value.astimezone(UTC).second != 0 or value.astimezone(UTC).microsecond != 0 for value in (start, end))


def _require_window_bounds(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    if start is None or end is None:
        raise ValueError("aggregate fact query requires both range bounds")
    return start, end


def _query_fact_aggregates(
    client: AggregateQueryClient, *, service_id: str, domain: str, dimensions: list[str], start: datetime, end: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    table = _FACT_TABLES[domain]
    expressions = _FACT_DIMENSION_EXPRESSIONS[domain]
    if any(dimension not in expressions for dimension in dimensions):
        raise ValueError("aggregate dimension cannot be read exactly from event-time facts")
    dimension_values: list[str] = []
    params: dict[str, Any] = {"service_id": service_id, "start": start.astimezone(UTC), "end": end.astimezone(UTC)}
    for idx, dimension in enumerate(dimensions):
        key = f"fact_dimension_{idx}"
        dimension_values.append(f"tuple({{{key}:String}}, toString({expressions[dimension]}))")
        params[key] = dimension
    end_operator = "<" if domain == "request" else "<="
    clauses = [
        "service_id={service_id:String}",
        "event_timestamp >= {start:DateTime64(3)}",
        f"event_timestamp {end_operator} {{end:DateTime64(3)}}",
        visible_publication_fence_sql(domain),
    ]
    fact_values = (
        "SELECT tupleElement(dimension_value, 1) AS dimension, "
        "tupleElement(dimension_value, 2) AS value "
        f"FROM {table} ARRAY JOIN [{', '.join(dimension_values)}] AS dimension_value "
        f"WHERE {' AND '.join(clauses)}"
    )
    totals = client.execute(f"SELECT dimension, count() AS total_count FROM ({fact_values}) GROUP BY dimension", params)
    rows = client.execute(
        f"SELECT dimension, value, count() AS aggregate_count FROM ({fact_values}) "
        "GROUP BY dimension, value "
        "ORDER BY dimension ASC, aggregate_count DESC, value ASC "
        "LIMIT 10 BY dimension",
        params,
    )
    return totals, rows


def query_clickhouse_aggregate(
    client: AggregateQueryClient, request: AggregateRequest, *, watermark: ServingWatermark, now: datetime | None = None
) -> AggregateResponse:
    """Read visible aggregate rows while preserving the aggregate contract."""
    request.validate()
    watermark.validate()
    if watermark.service_id != request.service_id or watermark.domain != request.domain:
        raise ValueError("watermark does not belong to this service and domain")
    table, metric = _DOMAIN_TABLES[request.domain]
    dimension = request.dimension or DEFAULT_AGGREGATE_DIMENSIONS[request.domain]
    if not dimension or _INTERNAL_IDENTIFIER.fullmatch(dimension) is None:
        raise ValueError("aggregate dimension must be an internal identifier")
    clauses = [
        "service_id={service_id:String}",
        "dimension={dimension:String}",
        visible_publication_fence_sql(f"{request.domain}_aggregate"),
    ]
    params: dict[str, Any] = {"service_id": request.service_id, "dimension": dimension}
    partial_minute_window = _is_partial_minute_window(request.start, request.end)
    if partial_minute_window:
        if dimension not in _FACT_DIMENSION_EXPRESSIONS[request.domain]:
            raise ValueError("aggregate dimension cannot be read exactly from event-time facts")
    elif request.start is not None and request.end is not None:
        start = request.start.astimezone(UTC)
        end = request.end.astimezone(UTC)
        end_operator = "<" if request.domain == "request" else "<="
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", f"bucket_start {end_operator} {{end:DateTime64(3)}}"])
        params.update({"start": start, "end": end})
    try:
        if partial_minute_window:
            start, end = _require_window_bounds(request.start, request.end)
            totals, rows = _query_fact_aggregates(
                client,
                service_id=request.service_id,
                domain=request.domain,
                dimensions=[dimension],
                start=start,
                end=end,
            )
        else:
            totals = client.execute(
                f"SELECT sum({metric}) AS total_count FROM {table} WHERE {' AND '.join(clauses)}", params
            )
            rows = client.execute(
                f"SELECT value, sum({metric}) AS aggregate_count "
                f"FROM {table} WHERE {' AND '.join(clauses)} "
                "GROUP BY value ORDER BY aggregate_count DESC, value ASC LIMIT 10",
                params,
            )
    except ClickHouseError as e:
        raise ValueError(f"ClickHouse server query failed (S3/FOS access credentials may be expired): {e}")
    values = tuple((str(row["value"]), max(0, int(row["aggregate_count"]))) for row in rows)
    total_row = totals[0] if totals else {}
    request_count = max(0, int(total_row.get("total_count") or 0))
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
    return AggregateResponse(
        service_id=request.service_id,
        domain=request.domain,
        request_count=request_count,
        top_values=values,
        coverage=1.0 if values or watermark.coverage_end is not None else 0.0,
        watermark=watermark,
        freshness_lag_seconds=freshness,
        exact=True,
        approximation_error=None,
        counters=((f"{request.domain}_events", request_count),),
    )


def query_clickhouse_aggregates_batch(
    client: AggregateQueryClient,
    *,
    service_id: str,
    domain: str,
    dimensions: list[str] | tuple[str, ...],
    start: datetime | None,
    end: datetime | None,
    watermark: ServingWatermark,
    now: datetime | None = None,
) -> dict[str, AggregateResponse]:
    """Read visible aggregate rows for multiple dimensions in two batched ClickHouse queries."""
    unique_dims = list(dict.fromkeys(dimensions))
    if not unique_dims:
        return {}
    if len(unique_dims) == 1:
        dim = unique_dims[0]
        return {
            dim: query_clickhouse_aggregate(
                client,
                AggregateRequest(service_id=service_id, domain=domain, start=start, end=end, dimension=dim),
                watermark=watermark,
                now=now,
            )
        }
    watermark.validate()
    if watermark.service_id != service_id or watermark.domain != domain:
        raise ValueError("watermark does not belong to this service and domain")
    if domain not in _DOMAIN_TABLES:
        raise ValueError(f"unsupported aggregate domain: {domain}")
    table, metric = _DOMAIN_TABLES[domain]
    partial_minute_window = _is_partial_minute_window(start, end)
    dim_placeholders: list[str] = []
    params: dict[str, Any] = {"service_id": service_id}
    for idx, dim in enumerate(unique_dims):
        if not dim or _INTERNAL_IDENTIFIER.fullmatch(dim) is None:
            raise ValueError("aggregate dimension must be an internal identifier")
        key = f"dim_{idx}"
        dim_placeholders.append(f"{{{key}:String}}")
        params[key] = dim
    if partial_minute_window:
        start, end = _require_window_bounds(start, end)
        try:
            totals_rows, top_rows = _query_fact_aggregates(
                client, service_id=service_id, domain=domain, dimensions=unique_dims, start=start, end=end
            )
        except ClickHouseError as e:
            raise ValueError(f"ClickHouse server query failed (S3/FOS access credentials may be expired): {e}")
        fact_totals_by_dim = {
            str(row.get("dimension") or ""): max(0, int(row.get("total_count") or 0)) for row in totals_rows
        }
        fact_tops_by_dim: dict[str, list[tuple[str, int]]] = {dimension: [] for dimension in unique_dims}
        for row in top_rows:
            dimension = str(row.get("dimension") or "")
            if dimension in fact_tops_by_dim and len(fact_tops_by_dim[dimension]) < 10:
                fact_tops_by_dim[dimension].append((str(row["value"]), max(0, int(row["aggregate_count"]))))
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
        return {
            dimension: AggregateResponse(
                service_id=service_id,
                domain=domain,
                request_count=fact_totals_by_dim.get(dimension, 0),
                top_values=tuple(fact_tops_by_dim.get(dimension, ())),
                coverage=1.0 if fact_tops_by_dim.get(dimension) or watermark.coverage_end is not None else 0.0,
                watermark=watermark,
                freshness_lag_seconds=freshness,
                exact=True,
                approximation_error=None,
                counters=((f"{domain}_events", fact_totals_by_dim.get(dimension, 0)),),
            )
            for dimension in unique_dims
        }
    clauses = [
        "service_id={service_id:String}",
        f"dimension IN ({', '.join(dim_placeholders)})",
        visible_publication_fence_sql(f"{domain}_aggregate"),
    ]
    if start is not None and end is not None:
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        end_operator = "<" if domain == "request" else "<="
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", f"bucket_start {end_operator} {{end:DateTime64(3)}}"])
        params.update({"start": start_utc, "end": end_utc})
    try:
        totals_rows = client.execute(
            f"SELECT dimension, sum({metric}) AS total_count "
            f"FROM {table} WHERE {' AND '.join(clauses)} "
            "GROUP BY dimension",
            params,
        )
        if totals_rows and "dimension" not in totals_rows[0]:
            return {
                dim: query_clickhouse_aggregate(
                    client,
                    AggregateRequest(service_id=service_id, domain=domain, start=start, end=end, dimension=dim),
                    watermark=watermark,
                    now=now,
                )
                for dim in unique_dims
            }
        top_rows = client.execute(
            f"SELECT dimension, value, sum({metric}) AS aggregate_count "
            f"FROM {table} WHERE {' AND '.join(clauses)} "
            "GROUP BY dimension, value "
            "ORDER BY dimension ASC, aggregate_count DESC, value ASC "
            "LIMIT 10 BY dimension",
            params,
        )
        if top_rows and "dimension" not in top_rows[0]:
            return {
                dim: query_clickhouse_aggregate(
                    client,
                    AggregateRequest(service_id=service_id, domain=domain, start=start, end=end, dimension=dim),
                    watermark=watermark,
                    now=now,
                )
                for dim in unique_dims
            }
    except ClickHouseError as e:
        raise ValueError(f"ClickHouse server query failed (S3/FOS access credentials may be expired): {e}")
    totals_by_dim: dict[str, int] = {}
    for row in totals_rows:
        d = str(row.get("dimension") or "")
        totals_by_dim[d] = max(0, int(row.get("total_count") or 0))
    tops_by_dim: dict[str, list[tuple[str, int]]] = {d: [] for d in unique_dims}
    for row in top_rows:
        d = str(row.get("dimension") or "")
        if d in tops_by_dim and len(tops_by_dim[d]) < 10:
            tops_by_dim[d].append((str(row["value"]), max(0, int(row["aggregate_count"]))))
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
    out: dict[str, AggregateResponse] = {}
    for dim in unique_dims:
        values = tuple(tops_by_dim.get(dim, ()))
        req_count = totals_by_dim.get(dim, 0)
        out[dim] = AggregateResponse(
            service_id=service_id,
            domain=domain,
            request_count=req_count,
            top_values=values,
            coverage=1.0 if values or watermark.coverage_end is not None else 0.0,
            watermark=watermark,
            freshness_lag_seconds=freshness,
            exact=True,
            approximation_error=None,
            counters=((f"{domain}_events", req_count),),
        )
    return out


import hashlib
import heapq
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as _dc_field

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import bucket as _bucket
from backend.high_scale._common import build_published_facts_where as _facts_where
from backend.high_scale._common import build_published_projection_where as _where
from backend.high_scale._common import finite_float as _finite_float
from backend.high_scale._common import percentiles as _percentiles
from backend.high_scale._common import resolve_asn_display_labels
from backend.high_scale._common import text as _text
from backend.models.performance import PerformanceAggregatesResponse
from backend.repositories.utils.response_cache import ResponseMemo, serialize_filters_for_key
from backend.utils.time_window import CacheRegistry

SCATTER_SAMPLE_SIZE = 300


@dataclass(frozen=True)
class PerformanceProjection:
    dimension_rows: tuple[dict[str, Any], ...]
    summary_rows: tuple[dict[str, Any], ...] = ()
    scatter_rows: tuple[dict[str, Any], ...] = ()


@dataclass
class _Accumulator:
    requests: int = 0
    latencies: list[float] = _dc_field(default_factory=list)


@dataclass
class _WaterfallSums:
    count: int = 0
    edge_processing: float = 0.0
    origin_wait: float = 0.0
    origin_download: float = 0.0
    client_download: float = 0.0


def _asn_text(value: Any) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return _text(value)


def build_performance_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> PerformanceProjection:
    dimensions: dict[tuple[datetime, str, str], _Accumulator] = defaultdict(_Accumulator)
    waterfalls: dict[datetime, _WaterfallSums] = defaultdict(_WaterfallSums)
    scatter_candidates: list[tuple[int, int, dict[str, Any]]] = []
    for index, row in enumerate(rows):
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        elapsed = _finite_float(row.get("elapsed"))
        _add_waterfall_and_scatter(row, index, bucket, elapsed, waterfalls, scatter_candidates)
        url = _text(row.get("url"))
        if url:
            acc = dimensions[(bucket, "url", url)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)
        asn = _asn_text(row.get("asn"))
        if asn:
            acc = dimensions[(bucket, "asn", asn)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)
        bot = _text(row.get("_ngwaf_bot_name"))
        if bot:
            acc = dimensions[(bucket, "bot", bot)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)
    return PerformanceProjection(
        dimension_rows=tuple(
            _dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
        summary_rows=tuple(_summary_row(bucket, sums) for bucket, sums in sorted(waterfalls.items())),
        scatter_rows=tuple(
            row for _, _, row in heapq.nsmallest(SCATTER_SAMPLE_SIZE, scatter_candidates, key=lambda c: c[:2])
        ),
    )


def _add_waterfall_and_scatter(
    row: Mapping[str, Any],
    index: int,
    bucket: datetime,
    elapsed_us: float | None,
    waterfalls: dict[datetime, _WaterfallSums],
    scatter_candidates: list[tuple[int, int, dict[str, Any]]],
) -> None:
    """Mirror the standard ``components`` CTE: rows need both ``ttfb`` and ``elapsed``."""
    ttfb_s = _finite_float(row.get("ttfb"))
    if elapsed_us is None or ttfb_s is None:
        return
    elapsed_ms = elapsed_us / 1000.0
    ttfb_ms = ttfb_s * 1000.0
    ottfb_ms = (_finite_float(row.get("ottfb")) or 0.0) / 1000.0
    ottlb_ms = (_finite_float(row.get("ottlb")) or 0.0) / 1000.0
    sums = waterfalls[bucket]
    sums.count += 1
    sums.edge_processing += max(0.0, ttfb_ms - ottfb_ms)
    sums.origin_wait += ottfb_ms
    sums.origin_download += max(0.0, ottlb_ms - ottfb_ms)
    sums.client_download += max(0.0, elapsed_ms - max(ottlb_ms, ttfb_ms))
    edge_ms = elapsed_ms - ttfb_ms
    if edge_ms < 0:
        return
    sample_key = _sample_key(row, index)
    scatter_candidates.append(
        (
            sample_key,
            index,
            {
                "bucket_start": bucket.isoformat(),
                "sample_key": sample_key,
                "origin_ms": ttfb_ms,
                "edge_ms": edge_ms,
                "cache": _text(row.get("cache")),
            },
        )
    )


def _sample_key(row: Mapping[str, Any], index: int) -> int:
    identity = _text(row.get("event_id")) or f"{row.get('timestamp')}|{row.get('url')}|{index}"
    return int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big")


def _summary_row(bucket: datetime, sums: _WaterfallSums) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "component_count": sums.count,
        "edge_processing_sum_ms": sums.edge_processing,
        "origin_wait_sum_ms": sums.origin_wait,
        "origin_download_sum_ms": sums.origin_download,
        "client_download_sum_ms": sums.client_download,
    }


def _dimension_row(bucket: datetime, dimension: str, value: str, acc: _Accumulator) -> dict[str, Any]:
    lat_p50, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.95, 0.99))
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "latency_count": len(acc.latencies),
        "latency_sum_ms": sum(acc.latencies) if acc.latencies else None,
        "latency_p50_ms": lat_p50,
        "latency_p95_ms": lat_p95,
        "latency_p99_ms": lat_p99,
    }


_HS_PERF_CACHE_TTL = float(os.getenv("HS_PERF_CACHE_TTL_SECONDS", "30"))
_perf_memo = ResponseMemo(ttl=_HS_PERF_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_performance_aggregates_memo", _perf_memo.cache)
_ALL_SECTIONS = frozenset({"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"})
_SORT_COLUMNS = {"avg": "avg_ms", "p50": "p50_ms", "p95": "p95_ms", "p99": "p99_ms"}
_MIN_URL_REQUESTS = 5
_MIN_ASN_REQUESTS = 10
_TOP_N = 20
_TTL_EDGES = (
    (0, "0s"),
    (10, "<10s"),
    (30, "<30s"),
    (60, "<1m"),
    (300, "<5m"),
    (600, "<10m"),
    (1800, "<30m"),
    (3600, "<1h"),
    (10800, "<3h"),
    (21600, "<6h"),
    (43200, "<12h"),
    (86400, "<1d"),
    (259200, "<3d"),
    (604800, "<1w"),
    (1209600, "<2w"),
    (2592000, "<30d"),
    (7776000, "<90d"),
    (31536000, "<1y"),
)


def _ttl_bucket_expr(column: str) -> str:
    from backend.high_scale._common import _bucket_case

    return _bucket_case(column, _TTL_EDGES, ">1y")


_ELAPSED_MS = "toFloat64OrNull(custom_fields['elapsed']) / 1000.0"
_TTFB_MS = "toFloat64OrNull(custom_fields['ttfb']) * 1000.0"
_OTTFB_MS = "coalesce(toFloat64OrNull(custom_fields['ottfb']), 0) / 1000.0"
_OTTLB_MS = "coalesce(toFloat64OrNull(custom_fields['ottlb']), 0) / 1000.0"


def _active_filters(req: Any) -> dict[str, Any]:
    """Filters that actually constrain rows; a field with no facts mapping is rejected, never ignored."""
    active: dict[str, Any] = {}
    for field_name, config in (getattr(req, "filters", None) or {}).items():
        values = config.get("values") if isinstance(config, Mapping) else getattr(config, "values", None)
        if not values:
            continue
        if field_name not in _FIELD_DIMENSIONS:
            raise ValueError(f"unsupported Performance filter field for high-scale: {field_name}")
        active[field_name] = config
    return active


def _weighted(column: str, count_column: str = "latency_count") -> str:
    return f"sum({column} * {count_column}) / nullIf(sum(if({column} IS NOT NULL, {count_column}, 0)), 0)"


def _optional_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _projection_percentile_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    sort_column: str,
    min_requests: int,
) -> list[dict[str, Any]]:
    where, params = _where(service.service_id, "performance_dimensions", start_time, end_time, dimension=dimension)
    params["limit"] = _TOP_N
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, "
        "sum(requests) AS req_count, "
        "sum(latency_sum_ms) / nullIf(sum(latency_count), 0) / 1000.0 AS avg_ms, "
        f"{_weighted('latency_p50_ms')} / 1000.0 AS p50_ms, "
        f"{_weighted('latency_p95_ms')} / 1000.0 AS p95_ms, "
        f"{_weighted('latency_p99_ms')} / 1000.0 AS p99_ms "
        f"FROM performance_minute_dimensions WHERE {where} GROUP BY value "
        f"HAVING req_count > {min_requests} ORDER BY {sort_column} DESC NULLS LAST LIMIT {{limit:UInt32}}",
        params,
    )


def _facts_percentile_rows(
    service: HighScaleService,
    *,
    marker: str,
    key_expr: str,
    where: str,
    params: dict[str, Any],
    sort_column: str,
    min_requests: int,
) -> list[dict[str, Any]]:
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, count() AS req_count, "
        "avg(elapsed_ms) AS avg_ms, "
        "quantile(0.5)(elapsed_ms) AS p50_ms, "
        "quantile(0.95)(elapsed_ms) AS p95_ms, "
        "quantile(0.99)(elapsed_ms) AS p99_ms "
        f"FROM (SELECT {key_expr} AS value, {_ELAPSED_MS} AS elapsed_ms FROM request_facts WHERE {where}) "
        f"WHERE value != '' AND elapsed_ms IS NOT NULL GROUP BY value "
        f"HAVING req_count > {min_requests} ORDER BY {sort_column} DESC NULLS LAST LIMIT {{limit:UInt32}}",
        {**params, "limit": _TOP_N},
    )


def _latency_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requests": int(row["req_count"]),
        "avg": _optional_float(row["avg_ms"]),
        "p50": _optional_float(row["p50_ms"]),
        "p95": _optional_float(row["p95_ms"]),
        "p99": _optional_float(row["p99_ms"]),
    }


def _asn_rows(service: HighScaleService, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach the same ``label`` the standard path's ``_resolve_asns`` adds."""
    asn_labels = resolve_asn_display_labels(
        service.service_id, [int(row["value"]) for row in rows if str(row["value"]).isdigit()]
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        asn: int | str = int(row["value"]) if str(row["value"]).isdigit() else row["value"]
        label = asn_labels.get(asn, f"AS{asn}") if isinstance(asn, int) else str(asn)
        out.append({"asn": asn, "label": label, **_latency_row(row)})
    return out


def _ttl_dist(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        source = f"SELECT toFloat64OrNull(custom_fields['ttl']) AS ttl, 1 AS weight FROM request_facts WHERE {where}"
    else:
        where, params = _where(service.service_id, "request_aggregate", start_time, end_time, dimension="ttl")
        source = f"SELECT toFloat64OrNull(value) AS ttl, request_count AS weight FROM request_aggregates WHERE {where}"
    rows = service.client.execute(
        f"/* perf:ttl_dist */ SELECT {_ttl_bucket_expr('ttl')} AS bucket, sum(weight) AS count, min(ttl) AS min_ttl "
        f"FROM ({source}) WHERE ttl IS NOT NULL GROUP BY bucket ORDER BY min_ttl",
        params,
    )
    return [{"bucket": row["bucket"], "count": int(row["count"])} for row in rows]


def _waterfall(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any]
) -> dict[str, Any]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        sql = (
            "/* perf:waterfall */ SELECT count() AS component_count, "
            "sum(greatest(0, ttfb_ms - ottfb_ms)) / nullIf(count(), 0) AS edge_processing, "
            "sum(ottfb_ms) / nullIf(count(), 0) AS origin_wait, "
            "sum(greatest(0, ottlb_ms - ottfb_ms)) / nullIf(count(), 0) AS origin_download, "
            "sum(greatest(0, elapsed_ms - greatest(ottlb_ms, ttfb_ms))) / nullIf(count(), 0) AS client_download "
            f"FROM (SELECT {_ELAPSED_MS} AS elapsed_ms, {_TTFB_MS} AS ttfb_ms, "
            f"{_OTTFB_MS} AS ottfb_ms, {_OTTLB_MS} AS ottlb_ms FROM request_facts WHERE {where}) "
            "WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL"
        )
    else:
        where, params = _where(service.service_id, "performance_summary", start_time, end_time)
        sql = (
            "/* perf:waterfall */ SELECT sum(component_count) AS n_components, "
            "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0) AS edge_processing, "
            "sum(origin_wait_sum_ms) / nullIf(sum(component_count), 0) AS origin_wait, "
            "sum(origin_download_sum_ms) / nullIf(sum(component_count), 0) AS origin_download, "
            "sum(client_download_sum_ms) / nullIf(sum(component_count), 0) AS client_download "
            f"FROM performance_minute_summary WHERE {where}"
        )
    rows = service.client.execute(sql, params)
    row = rows[0] if rows else {}
    return {
        "avg": {
            "edge_processing": _optional_float(row.get("edge_processing")),
            "origin_wait": _optional_float(row.get("origin_wait")),
            "origin_download": _optional_float(row.get("origin_download")),
            "client_download": _optional_float(row.get("client_download")),
        }
    }


def _scatter(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        sql = (
            "/* perf:scatter */ SELECT ttfb_ms AS origin_ms, elapsed_ms - ttfb_ms AS edge_ms, cache "
            f"FROM (SELECT event_id, {_ELAPSED_MS} AS elapsed_ms, {_TTFB_MS} AS ttfb_ms, "
            f"custom_fields['cache'] AS cache FROM request_facts WHERE {where}) "
            "WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL AND elapsed_ms - ttfb_ms >= 0 "
            "ORDER BY cityHash64(event_id) LIMIT {scatter_limit:UInt32}"
        )
    else:
        where, params = _where(service.service_id, "performance_scatter", start_time, end_time)
        sql = (
            "/* perf:scatter */ SELECT origin_ms, edge_ms, cache "
            f"FROM performance_scatter_samples WHERE {where} "
            "ORDER BY sample_key LIMIT {scatter_limit:UInt32}"
        )
    rows = service.client.execute(sql, {**params, "scatter_limit": SCATTER_SAMPLE_SIZE})
    return [
        {
            "origin": _optional_float(row["origin_ms"]),
            "edge": _optional_float(row["edge_ms"]),
            "cache": row["cache"] or None,
        }
        for row in rows
    ]


def performance_aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None = None,
) -> PerformanceAggregatesResponse:
    wanted = _ALL_SECTIONS if sections is None else sections
    filters = _active_filters(req)
    sort_by = getattr(req, "sort_by", "p99")
    sort_column = _SORT_COLUMNS.get(sort_by, "p99_ms")
    cache_key: str | None = None
    if _HS_PERF_CACHE_TTL > 0 and start_time is not None and end_time is not None:
        cache_key = digest_cache_key(
            {
                "ep": "hs_performance_aggregates",
                "svc": service.service_id,
                "cid": client_cache_token(service.client),
                "s": bucket_time_to_minute(start_time),
                "e": bucket_time_to_minute(end_time),
                "f": serialize_filters_for_key(filters),
                "sb": sort_by,
                "sec": sorted(wanted),
            },
            {"service_id": service.service_id},
        )
        cached = _perf_memo.get_with_telemetry(cache_key)
        if cached is not None:
            return PerformanceAggregatesResponse.with_telemetry(**cached)
    facts_where_params: tuple[str, dict[str, Any]] | None = None
    if filters and (wanted & {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}):
        facts_where_params = _facts_where(service.service_id, start_time, end_time, filters)
    trace = HighScaleSections(service.service_id, "performance")
    tasks: list[tuple[str, Any]] = []
    if "top_urls" in wanted:

        def _fetch_top_urls() -> list[dict[str, Any]]:
            if facts_where_params is not None:
                where, params = facts_where_params
                rows = _facts_percentile_rows(
                    service,
                    marker="top_urls",
                    key_expr="url",
                    where=where,
                    params=params,
                    sort_column=sort_column,
                    min_requests=_MIN_URL_REQUESTS,
                )
            else:
                rows = _projection_percentile_rows(
                    service,
                    marker="top_urls",
                    dimension="url",
                    start_time=start_time,
                    end_time=end_time,
                    sort_column=sort_column,
                    min_requests=_MIN_URL_REQUESTS,
                )
            return [{"url": row["value"], **_latency_row(row)} for row in rows]

        tasks.append(("top_urls", _fetch_top_urls))
    if "top_asns" in wanted:

        def _fetch_top_asns() -> list[dict[str, Any]]:
            if facts_where_params is not None:
                where, params = facts_where_params
                rows = _facts_percentile_rows(
                    service,
                    marker="top_asns",
                    key_expr="custom_fields['asn']",
                    where=where,
                    params=params,
                    sort_column=sort_column,
                    min_requests=_MIN_ASN_REQUESTS,
                )
            else:
                rows = _projection_percentile_rows(
                    service,
                    marker="top_asns",
                    dimension="asn",
                    start_time=start_time,
                    end_time=end_time,
                    sort_column=sort_column,
                    min_requests=_MIN_ASN_REQUESTS,
                )
            return rows

        tasks.append(("top_asns", _fetch_top_asns))
    if "ttl_dist" in wanted:
        tasks.append(("ttl_dist", lambda: _ttl_dist(service, start_time, end_time, filters)))
    if "waterfall" in wanted:
        tasks.append(("waterfall", lambda: _waterfall(service, start_time, end_time, filters)))
    if "scatter" in wanted:
        tasks.append(("scatter", lambda: _scatter(service, start_time, end_time, filters)))
    result = trace.run_parallel(tasks, source="fact" if filters else "projection")
    if "top_asns" in result:
        result["top_asns"] = trace.run(
            "asn_labels", lambda: _asn_rows(service, result["top_asns"]), activity="overhead"
        )
    payload = {**result, "_approx": not filters, "section_timings": trace.timings()}
    if cache_key is not None:
        _perf_memo.put(cache_key, payload)
    return PerformanceAggregatesResponse.with_telemetry(
        **_perf_memo.result(payload, state="miss" if cache_key is not None else "bypass")
    )


from datetime import timedelta

from backend.high_scale.publication import build_cmcd_projection_key
from backend.models.performance import CmcdRequest
from backend.repositories.utils.response_cache import ResponseMemo
from backend.utils.bounded_cache import CacheRegistry


def project_cmcd(request_event: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_fields = request_event.get("cmcd")
    if raw_fields is None:
        return None
    if not isinstance(raw_fields, Mapping):
        raise ValueError("CMCD fields must be a mapping")
    request_event_id = request_event.get("event_id")
    if not isinstance(request_event_id, str) or not request_event_id:
        raise ValueError("CMCD projection requires request_event_id")
    service_id = request_event.get("service_id")
    if not isinstance(service_id, str) or not service_id:
        raise ValueError("CMCD projection requires service_id")
    projection = {
        "projection_key": build_cmcd_projection_key({"request_event_id": request_event_id}),
        "request_event_id": request_event_id,
        "service_id": service_id,
        "timestamp": request_event.get("timestamp"),
        "cmcd_version": request_event.get("cmcd_version"),
        "sid": request_event.get("sid"),
        "country": _country(request_event.get("country")),
        "asn": _asn(request_event.get("asn")),
        "fields": dict(raw_fields),
    }
    return projection


def _country(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _asn(value: object) -> int | None:
    """A positive AS number, or None: the analytics group by ASN, never by a malformed label."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip()) or None
    return None


_memo = ResponseMemo(ttl=30.0, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_cmcd_aggregates_memo", _memo.cache)
_DEFAULT_WINDOW = timedelta(days=1)
_BUCKET_FORMAT = "%Y-%m-%d %H:%M:%S"
_DURATION_BUCKETS = {1: "<30s", 2: "30s–2m", 3: "2–5m", 4: "5–10m", 5: "10–30m", 6: "30m–1h", 7: ">1h"}
_FILTER_COLUMNS = {"country": "country", "asn": "asn"}
_EVENTS = """
    SELECT event_timestamp AS ts,
           cmcd['sid'] AS sid,
           cmcd['cid'] AS cid,
           cmcd['ot'] AS ot,
           cmcd['sf'] AS sf,
           country,
           asn,
           toFloat64OrNull(cmcd['bl']) AS bl,
           toFloat64OrNull(cmcd['br']) AS br,
           toFloat64OrNull(cmcd['mtp']) AS mtp,
           toFloat64OrNull(cmcd['tb']) AS tb,
           if(cmcd['bs'] = '', NULL, lower(cmcd['bs']) IN ('true', '1')) AS bs,
           if(cmcd['su'] = '', NULL, lower(cmcd['su']) IN ('true', '1')) AS su
    FROM cmcd_projection_facts
    WHERE {where}
"""
_BUCKET = (
    "formatDateTime(toStartOfInterval({column}, INTERVAL {{bucket_seconds:UInt32}} SECOND), '%Y-%m-%d %H:%i:%S', 'UTC')"
)
_VIDEO = "ot = 'v'"


def _asn_names(service_id: str, asns: list[int]) -> dict[int, str]:
    from backend.core import duckdb as _db

    return _db.get_asn_names(service_id, asns)


def _asn_label(asn: int, names: Mapping[int, str]) -> str:
    from backend.core import duckdb as _db

    return _db.format_asn_label(asn, names.get(asn, ""))


def _spec(config: Any) -> tuple[str, list[Any]]:
    if isinstance(config, Mapping):
        return str(config.get("mode", "include")), list(config.get("values") or [])
    return str(getattr(config, "mode", "include")), list(getattr(config, "values", None) or [])


def _filter_clauses(filters: Mapping[str, Any]) -> tuple[list[str], dict[str, Any]]:
    unsupported = sorted(name for name, config in filters.items() if _spec(config)[1] and name not in _FILTER_COLUMNS)
    if unsupported:
        raise ValueError(
            f"high-scale CMCD projections cannot filter on: {', '.join(unsupported)} (supported: country, asn)"
        )
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, (name, config) in enumerate(filters.items()):
        mode, values = _spec(config)
        if not values:
            continue
        keys: list[str] = []
        for j, value in enumerate(values):
            param = f"flt_{i}_{j}"
            if name == "asn":
                if not str(value).strip().isdigit():
                    raise ValueError("asn filter values must be numeric AS numbers")
                params[param] = int(str(value).strip())
                keys.append(f"{{{param}:UInt32}}")
            else:
                params[param] = str(value)
                keys.append(f"{{{param}:String}}")
        operator = "NOT IN" if mode == "exclude" else "IN"
        clauses.append(f"{_FILTER_COLUMNS[name]} {operator} ({', '.join(keys)})")
    return clauses, params


def _cmcd_scope(
    service: HighScaleService, req: CmcdRequest, start: datetime, end: datetime
) -> tuple[str, dict[str, Any]]:
    filter_clauses, filter_params = _filter_clauses(req.filters or {})
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain='cmcd' AND publication_state='visible')",
        "event_timestamp >= {start:DateTime64(3)}",
        "event_timestamp <= {end:DateTime64(3)}",
        "cmcd['sid'] != ''",
        *filter_clauses,
    ]
    params = {
        "service_id": service.service_id,
        "start": start,
        "end": end,
        "bucket_seconds": int(req.bucket_seconds),
        "top_n": int(req.top_n),
        **filter_params,
    }
    return " AND ".join(clauses), params


def _round(value: Any) -> float | None:
    return None if value is None else round(float(value), 2)


def _pad(
    start: datetime, end: datetime, bucket_seconds: int, rows: dict[str, dict[str, Any]], default: dict[str, Any]
) -> list[dict[str, Any]]:
    first = int(start.timestamp())
    last = int(end.timestamp())
    cursor = first - first % bucket_seconds
    final = last - last % bucket_seconds
    padded: list[dict[str, Any]] = []
    while cursor <= final:
        bucket = datetime.fromtimestamp(cursor, tz=UTC).strftime(_BUCKET_FORMAT)
        padded.append({"bucket": bucket, **rows.get(bucket, default)})
        cursor += bucket_seconds
    return padded


def _sessions_ts(rows: Iterable[dict[str, Any]], new_sessions: Mapping[str, int]) -> dict[str, dict[str, Any]]:
    return {
        str(row["bucket"]): {
            "concurrent_sessions": int(row["concurrent_sessions"] or 0),
            "rebuffer_session_pct": _round(row.get("rebuffer_session_pct")),
            "new_sessions": new_sessions.get(str(row["bucket"]), 0),
        }
        for row in rows
    }


def cmcd_aggregates(
    service: HighScaleService,
    req: CmcdRequest,
    start_time: str | None,
    end_time: str | None,
    sections: set[Any] | None,
    mask_ips: bool,
) -> dict[str, Any]:
    """Return the standard CMCD response shape; ``mask_ips`` is moot because no section emits an IP."""
    memo_key = digest_cache_key(
        {
            "ep": "hs_cmcd_aggregates",
            "cid": client_cache_token(service.client),
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "b": int(req.bucket_seconds),
            "n": int(req.top_n),
            "sec": sorted(str(s) for s in sections) if sections else None,
            "m": bool(mask_ips),
            "f": serialize_filters_for_key(req.filters or {}),
        },
        {"name": service.service_id, "service_id": service.service_id},
    )
    cached_resp = _memo.get(memo_key)
    if cached_resp is not None:
        return dict(cached_resp)

    def _compute() -> dict[str, Any]:
        return _cmcd_aggregates_compute(service, req, start_time, end_time, sections, mask_ips)

    result = _memo.coalesce(f"hs_cmcd_agg:{memo_key}", _compute)
    _memo.put(memo_key, result, strip=("section_timings",))
    return result


def _cmcd_aggregates_compute(
    service: HighScaleService,
    req: CmcdRequest,
    start_time: str | None,
    end_time: str | None,
    sections: set[Any] | None,
    mask_ips: bool,
) -> dict[str, Any]:
    start, end = parse_time_window(start_time, end_time, _DEFAULT_WINDOW, utc=True)
    where, params = _cmcd_scope(service, req, start, end)
    events = _EVENTS.format(where=where)
    execute = service.client.execute

    def wanted(name: str) -> bool:
        return sections is None or name in sections

    trace = HighScaleSections(service.service_id, "cmcd")
    probe = trace.run(
        "probe",
        lambda: execute(
            "/* cmcd:probe */ SELECT count() AS events, uniqExact(sid) AS sessions, "
            "countIf(bl IS NOT NULL) AS has_bl, countIf(br IS NOT NULL) AS has_br, countIf(bs IS NOT NULL) AS has_bs, "
            "countIf(mtp IS NOT NULL) AS has_mtp, countIf(ot != '') AS has_ot, countIf(sf != '') AS has_sf, "
            "countIf(su IS NOT NULL) AS has_su, countIf(tb IS NOT NULL) AS has_tb, countIf(cid != '') AS has_cid, "
            "countIf(country != '') AS has_country, countIf(asn IS NOT NULL) AS has_asn "
            f"FROM ({events})",
            params,
        ),
        source="projected_fact",
        activity="overhead",
    )[0]
    if not int(probe["events"] or 0):
        return {"available": True, "has_data": False, "section_timings": trace.timings()}
    has = {key: bool(int(probe[f"has_{key}"] or 0)) for key in ("bl", "br", "bs", "mtp", "ot", "sf", "su", "tb", "cid")}
    has_country = bool(int(probe["has_country"] or 0))
    has_asn = bool(int(probe["has_asn"] or 0))
    has_video_filter = has["ot"]
    video = _VIDEO if has_video_filter else "1"
    result: dict[str, Any] = {"available": True, "has_data": True}
    tasks: list[tuple[str, Callable[[], dict[str, Any]]]] = []
    if wanted("overview"):

        def _run_overview() -> dict[str, Any]:
            row = execute(
                "/* cmcd:overview */ SELECT uniqExact(sid) AS active_sessions, "
                "if(countIf(bs IS NOT NULL) = 0, NULL, "
                "uniqExactIf(sid, bs = 1) * 100.0 / nullIf(uniqExact(sid), 0)) AS rebuffer_rate, "
                f"avgIf(br, {video}) AS avg_bitrate, avgIf(bl, {video}) AS avg_buffer_length, "
                f"quantileTDigestIf(0.5)(mtp, {video}) AS median_throughput FROM ({events})",
                params,
            )[0]
            peak = execute(
                "/* cmcd:overview_peak */ SELECT max(viewers) AS peak_viewers FROM ("
                "SELECT uniqExact(sid) AS viewers FROM (" + events + ") "
                f"GROUP BY {_BUCKET.format(column='ts')})",
                params,
            )
            duration = execute(
                "/* cmcd:overview_duration */ SELECT avg(duration) AS avg_duration FROM ("
                "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                "FROM (" + events + ") GROUP BY sid HAVING duration > 0)",
                params,
            )
            avg_duration = duration[0].get("avg_duration") if duration else None
            return {
                "overview": {
                    "active_sessions": int(row["active_sessions"] or 0),
                    "rebuffer_rate": _round(row.get("rebuffer_rate")) if has["bs"] else None,
                    "avg_bitrate": _round(row.get("avg_bitrate")) if has["br"] else None,
                    "avg_buffer_length": _round(row.get("avg_buffer_length")) if has["bl"] else None,
                    "median_throughput": _round(row.get("median_throughput")) if has["mtp"] else None,
                    "peak_viewers": int((peak[0].get("peak_viewers") if peak else 0) or 0),
                    "avg_session_duration": int(round(float(avg_duration))) if avg_duration else None,
                }
            }

        tasks.append(("overview", _run_overview))
    want_ts = {
        "sessions_ts": wanted("sessions_ts"),
        "buffer_health_ts": wanted("buffer_health_ts") and has["bl"],
        "bitrate_ts": wanted("bitrate_ts") and has["br"],
        "throughput_ts": wanted("throughput_ts") and has["mtp"],
        "startup_ts": wanted("startup_ts") and has["su"],
    }
    if any(want_ts.values()):

        def _run_timeseries() -> dict[str, Any]:
            ts_out: dict[str, Any] = {}
            select = [f"{_BUCKET.format(column='ts')} AS bucket"]
            if want_ts["sessions_ts"]:
                select += [
                    "uniqExact(sid) AS concurrent_sessions",
                    "if(countIf(bs IS NOT NULL) = 0, NULL, "
                    "uniqExactIf(sid, bs = 1) * 100.0 / nullIf(uniqExact(sid), 0)) AS rebuffer_session_pct",
                ]
            if want_ts["buffer_health_ts"]:
                select += [
                    f"quantileTDigestIf(0.5)(bl, {video}) AS p50_buffer",
                    f"quantileTDigestIf(0.95)(bl, {video}) AS p95_buffer",
                    "if(countIf(bs IS NOT NULL) = 0, NULL, countIf(bs = 1) * 100.0 / nullIf(count(), 0)) "
                    "AS starvation_rate",
                ]
            if want_ts["bitrate_ts"]:
                select += [
                    f"avgIf(br, {video}) AS avg_bitrate",
                    f"avgIf(br / tb, {video} AND tb > 0) AS utilization_ratio",
                ]
            if want_ts["throughput_ts"]:
                select += [
                    f"quantileTDigestIf(0.5)(mtp, {video}) AS p50_throughput",
                    f"quantileTDigestIf(0.95)(mtp, {video}) AS p95_throughput",
                    f"quantileTDigestIf(0.99)(mtp, {video}) AS p99_throughput",
                ]
            if want_ts["startup_ts"]:
                select.append(
                    "if(countIf(su IS NOT NULL) = 0, NULL, countIf(su = 1) * 100.0 / nullIf(count(), 0)) AS startup_ratio"
                )
            rows = execute(
                f"/* cmcd:timeseries */ SELECT {', '.join(select)} FROM ({events}) GROUP BY bucket ORDER BY bucket",
                params,
            )
            by_bucket = {str(row["bucket"]): row for row in rows}

            def padded(field_map: dict[str, tuple[str, Any]], default: dict[str, Any]) -> list[dict[str, Any]]:
                return _pad(
                    start,
                    end,
                    int(req.bucket_seconds),
                    {
                        bucket: {out: convert(row.get(src)) for out, (src, convert) in field_map.items()}
                        for bucket, row in by_bucket.items()
                    },
                    default,
                )

            if want_ts["sessions_ts"]:
                first_seen = execute(
                    "/* cmcd:new_sessions */ SELECT "
                    f"{_BUCKET.format(column='first_ts')} AS bucket, count() AS new_sessions FROM ("
                    f"SELECT sid, min(ts) AS first_ts FROM ({events}) GROUP BY sid) GROUP BY bucket",
                    params,
                )
                new_sessions = {str(row["bucket"]): int(row["new_sessions"] or 0) for row in first_seen}
                ts_out["sessions_ts"] = _pad(
                    start,
                    end,
                    int(req.bucket_seconds),
                    _sessions_ts(rows, new_sessions),
                    {"concurrent_sessions": 0, "rebuffer_session_pct": None, "new_sessions": 0},
                )
            if want_ts["buffer_health_ts"]:
                ts_out["buffer_health_ts"] = padded(
                    {
                        "p50_buffer": ("p50_buffer", _round),
                        "p95_buffer": ("p95_buffer", _round),
                        "starvation_rate": ("starvation_rate", _round),
                    },
                    {"p50_buffer": None, "p95_buffer": None, "starvation_rate": None},
                )
            if want_ts["bitrate_ts"]:
                ts_out["bitrate_ts"] = padded(
                    {"avg_bitrate": ("avg_bitrate", _round), "utilization_ratio": ("utilization_ratio", _round)},
                    {"avg_bitrate": None, "utilization_ratio": None},
                )
            if want_ts["throughput_ts"]:
                ts_out["throughput_ts"] = padded(
                    {
                        "p50": ("p50_throughput", _round),
                        "p95": ("p95_throughput", _round),
                        "p99": ("p99_throughput", _round),
                    },
                    {"p50": None, "p95": None, "p99": None},
                )
            if want_ts["startup_ts"]:
                ts_out["startup_ts"] = padded({"startup_ratio": ("startup_ratio", _round)}, {"startup_ratio": None})
            return ts_out

        tasks.append(("timeseries", _run_timeseries))
    if wanted("top_content") and has["cid"]:

        def _run_top_content() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:top_content */ SELECT cid AS content_id, count() AS session_count, "
                "sum(had_rebuffer) * 100.0 / nullIf(count(), 0) AS rebuffer_rate, avg(avg_br) AS avg_br, "
                "avg(avg_bl) AS avg_bl FROM ("
                "SELECT cid, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
                f"avgIf(br, {video}) AS avg_br, avgIf(bl, {video}) AS avg_bl "
                f"FROM ({events}) WHERE cid != '' GROUP BY cid, sid) "
                "GROUP BY cid ORDER BY session_count DESC, content_id LIMIT {top_n:UInt32}",
                params,
            )
            return {
                "top_content": [
                    {
                        "content_id": row["content_id"],
                        "session_count": int(row["session_count"]),
                        "rebuffer_rate": _round(row.get("rebuffer_rate")),
                        "avg_bitrate": _round(row.get("avg_br")),
                        "avg_buffer_length": _round(row.get("avg_bl")),
                    }
                    for row in rows
                ]
            }

        tasks.append(("top_content", _run_top_content))
    if wanted("rebuffer_by_country") and has_country and has["bs"]:

        def _run_rebuffer_country() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:rebuffer_by_country */ SELECT country, "
                "sum(had_rebuffer) * 100.0 / nullIf(count(), 0) AS rebuffer_rate, count() AS session_count, "
                "avg(median_mtp) AS median_mtp FROM ("
                "SELECT country, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
                f"quantileTDigestIf(0.5)(mtp, {video}) AS median_mtp "
                f"FROM ({events}) WHERE country != '' GROUP BY country, sid) "
                "GROUP BY country ORDER BY session_count DESC, country LIMIT {top_n:UInt32}",
                params,
            )
            return {
                "rebuffer_by_country": [
                    {
                        "country": row["country"],
                        "rebuffer_rate": _round(row.get("rebuffer_rate")),
                        "session_count": int(row["session_count"]),
                        "median_throughput": _round(row.get("median_mtp")),
                    }
                    for row in rows
                ]
            }

        tasks.append(("rebuffer_country", _run_rebuffer_country))
    if wanted("rebuffer_by_asn") and has_asn and has["bs"]:

        def _run_rebuffer_asn() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:rebuffer_by_asn */ SELECT asn, "
                "sum(had_rebuffer) * 100.0 / nullIf(count(), 0) AS rebuffer_rate, count() AS session_count, "
                "avg(avg_br) AS avg_br, avg(avg_bl) AS avg_bl FROM ("
                "SELECT asn, sid, max(if(bs = 1, 1, 0)) AS had_rebuffer, "
                f"avgIf(br, {video}) AS avg_br, avgIf(bl, {video}) AS avg_bl "
                f"FROM ({events}) WHERE asn IS NOT NULL GROUP BY asn, sid) "
                "GROUP BY asn ORDER BY session_count DESC, asn LIMIT {top_n:UInt32}",
                params,
            )
            return {
                "rebuffer_by_asn": [
                    {
                        "asn": int(row["asn"]),
                        "label": "",
                        "rebuffer_rate": _round(row.get("rebuffer_rate")),
                        "session_count": int(row["session_count"]),
                        "avg_bitrate": _round(row.get("avg_br")),
                        "avg_buffer_length": _round(row.get("avg_bl")),
                    }
                    for row in rows
                ]
            }

        tasks.append(("rebuffer_asn", _run_rebuffer_asn))
    if wanted("object_type_dist") and has["ot"]:

        def _run_object_type() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:object_type_dist */ SELECT ot AS object_type, count() AS request_count "
                f"FROM ({events}) WHERE ot != '' GROUP BY ot ORDER BY request_count DESC, ot",
                params,
            )
            return {
                "object_type_dist": [
                    {"object_type": row["object_type"], "request_count": int(row["request_count"])} for row in rows
                ]
            }

        tasks.append(("object_type", _run_object_type))
    if wanted("streaming_format_dist") and has["sf"]:

        def _run_streaming_format() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:streaming_format_dist */ SELECT sf AS streaming_format, uniqExact(sid) AS session_count "
                f"FROM ({events}) WHERE sf != '' GROUP BY sf ORDER BY session_count DESC, sf",
                params,
            )
            return {
                "streaming_format_dist": [
                    {"streaming_format": row["streaming_format"], "session_count": int(row["session_count"])}
                    for row in rows
                ]
            }

        tasks.append(("streaming_format", _run_streaming_format))
    if wanted("session_duration_dist"):

        def _run_session_duration() -> dict[str, Any]:
            rows = execute(
                "/* cmcd:session_duration_dist */ SELECT sort_order, count() AS session_count FROM ("
                "SELECT multiIf(duration < 30, 1, duration < 120, 2, duration < 300, 3, duration < 600, 4, "
                "duration < 1800, 5, duration < 3600, 6, 7) AS sort_order FROM ("
                "SELECT (toUnixTimestamp64Milli(max(ts)) - toUnixTimestamp64Milli(min(ts))) / 1000.0 AS duration "
                f"FROM ({events}) GROUP BY sid)) GROUP BY sort_order ORDER BY sort_order",
                params,
            )
            return {
                "session_duration_dist": [
                    {
                        "duration_bucket": _DURATION_BUCKETS[int(row["sort_order"])],
                        "session_count": int(row["session_count"]),
                    }
                    for row in rows
                ]
            }

        tasks.append(("session_duration", _run_session_duration))
    for d in trace.run_parallel(tasks, source="projected_fact", max_workers=8).values():
        result.update(d)
    if "rebuffer_by_asn" in result:

        def _label_asns() -> None:
            rows = result["rebuffer_by_asn"]
            names = _asn_names(service.service_id, [row["asn"] for row in rows])
            for row in rows:
                row["label"] = _asn_label(row["asn"], names)

        trace.run("asn_labels", _label_asns, activity="overhead")
    result["section_timings"] = trace.timings()
    return result


# ── Folded from backend/high_scale/dashboard.py ──────────────────────────────

import concurrent.futures
import os
from functools import partial

from backend.high_scale._common import _column, inclusive_minute_end
from backend.models.common import ResponseCacheObservation
from backend.models.dashboard import (
    AggregatesRequest,
    AggregatesResponse,
    FieldAggregate,
    FieldTopEntry,
    FieldValuesRequest,
    FieldValuesResponse,
    MapPoint,
    RawRequest,
    TimeSeriesPoint,
)
from backend.repositories.utils.response_cache import ResponseMemo, cache_result
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.date_utils import safe_iso

_HS_DASHBOARD_CACHE_TTL = float(os.getenv("HS_DASHBOARD_CACHE_TTL_SECONDS", "30"))
_dashboard_memo = ResponseMemo(ttl=_HS_DASHBOARD_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_dashboard_aggregates_memo", _dashboard_memo.cache)

_FIELD_DIMENSIONS = {
    "url": "url",
    "country": "country",
    "ip": "client_ip",
    "asn": "asn",
    "host": "host",
    "method": "method",
    "status": "status",
    "cache": "cache",
    "proto": "proto",
    "ua": "ua",
    "referer": "referer",
    "cookie_session": "cookie_session",
    "resp_header_content_encoding": "resp_header_content_encoding",
    "ttl": "ttl",
    "age": "age",
    "hits": "hits",
    "digest": "digest",
    "city": "city",
    "region": "region",
    "metro": "metro",
    "transport": "transport",
    "c_speed": "c_speed",
    "c_type": "c_type",
    "pop": "pop",
    "backend": "backend",
    "edge": "edge",
    "server_region": "server_region",
    "tls": "tls",
    "is_ipv6": "is_ipv6",
    "conn_requests": "conn_requests",
    "waf": "waf",
    "waf_resp": "waf_resp",
    "waf_ms": "waf_ms",
    "p_type": "p_type",
    "p_desc": "p_desc",
    "ja3": "ja3",
    "ja4": "ja4",
    "tls_ciphers_sha": "tls_ciphers_sha",
    "io_input_format": "io_input_format",
    "io_output_format": "io_output_format",
}
_HEADER_TABLES = {"request": "request_facts", "rum_vitals": "rum_vitals_facts", "rum_errors": "rum_error_facts"}


def _watermark_extent(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _query_source_extents(
    client: Any,
    source_id: str,
    table_name: str,
    *,
    domain: str,
    count_expr: str = "count()",
    ts_col: str = "event_timestamp",
) -> tuple[int, str | None, str | None]:
    rows = client.execute(
        f"SELECT {count_expr} AS total_rows, min({ts_col}) AS earliest_log_at, max({ts_col}) AS latest_log_at "
        f"FROM {table_name} "
        "WHERE service_id={service_id:String} "
        "AND publication_state = 'visible' "
        "AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={domain:String} "
        "AND publication_state = 'visible'"
        ")",
        {"service_id": source_id, "domain": domain},
    )
    row = rows[0] if rows else {}
    total_rows = int(row.get("total_rows") or 0)
    if total_rows == 0:
        return 0, None, None
    earliest_value: object = row.get("earliest_log_at")
    latest_value: object = row.get("latest_log_at")
    if isinstance(earliest_value, datetime):
        earliest_value = earliest_value.isoformat()
    if isinstance(latest_value, datetime):
        latest_value = latest_value.isoformat()
    return (
        total_rows,
        str(earliest_value) if earliest_value is not None else None,
        str(latest_value) if latest_value is not None else None,
    )


def header_metrics(service: HighScaleService) -> dict[str, Any]:
    """Return visible ClickHouse row counts and event-time extents for the header."""
    totals: dict[str, int] = {}
    earliest: dict[str, str | None] = {}
    latest: dict[str, str | None] = {}
    for domain, table in _HEADER_TABLES.items():
        totals[domain], earliest[domain], latest[domain] = _query_source_extents(
            service.client, service.service_id, table, domain=domain
        )

    if totals.get("rum_errors", 0) == 0:
        agg_total, agg_earliest, agg_latest = _query_source_extents(
            service.client,
            service.service_id,
            "rum_error_aggregates",
            domain="rum_errors_aggregate",
            count_expr="sum(error_count)",
            ts_col="bucket_start",
        )
        if agg_total > 0:
            totals["rum_errors"] = agg_total
            earliest["rum_errors"] = agg_earliest
            latest["rum_errors"] = agg_latest

    rum_total = totals["rum_vitals"] + totals["rum_errors"]
    rum_earliest_values = [earliest["rum_vitals"], earliest["rum_errors"]]
    rum_latest_values = [latest["rum_vitals"], latest["rum_errors"]]
    rum_earliest = min((value for value in rum_earliest_values if value is not None), default=None)
    rum_latest = max((value for value in rum_latest_values if value is not None), default=None)
    request_total = totals["request"]
    request_earliest = earliest["request"]
    request_latest = latest["request"]

    req_sync: str | None = None
    rum_sync: str | None = None
    try:
        from backend.core.metadata.cron_log import latest_cron_per_task

        latest_crons = latest_cron_per_task(service.service_id)
        req_sync = max(
            (
                str(latest_crons[t]["started_at"])
                for t in ("log_discovery", "commit", "ledger_sweep")
                if t in latest_crons and latest_crons[t].get("started_at")
            ),
            default=None,
        )
        rum_sync = max(
            (
                str(latest_crons[t]["started_at"])
                for t in ("rum_discovery", "rum_sync", "ledger_rum_sweep")
                if t in latest_crons and latest_crons[t].get("started_at")
            ),
            default=None,
        )
    except Exception:
        pass

    return {
        "request": {
            "earliest_log_at": request_earliest,
            "latest_log_at": request_latest,
            "total_rows": request_total,
            "last_sync_at": req_sync,
        },
        "rum": {
            "earliest_log_at": rum_earliest,
            "latest_log_at": rum_latest,
            "total_rows": rum_total,
            "last_sync_at": rum_sync,
        },
        "earliest_log_at": request_earliest,
        "latest_log_at": request_latest,
        "local_rows": request_total + rum_total,
    }


def _aggregate(service: HighScaleService, *, start_time: str | None, end_time: str | None, dimension: str, watermark):
    start = _range_value(start_time)
    end = _range_value(end_time)
    return query_clickhouse_aggregate(
        service.client,
        AggregateRequest(service_id=service.service_id, domain="request", start=start, end=end, dimension=dimension),
        watermark=watermark,
    )


def _normalize_chart_interval(chart_interval: str | None, default: str = "1 minute") -> str:
    if not chart_interval:
        return default
    ci = chart_interval.strip().lower()
    mapping = {
        "1 second": "1 second",
        "1 seconds": "1 second",
        "1s": "1 second",
        "second": "1 second",
        "seconds": "1 second",
        "1 minute": "1 minute",
        "1 minutes": "1 minute",
        "1m": "1 minute",
        "minute": "1 minute",
        "minutes": "1 minute",
        "5 minute": "5 minutes",
        "5 minutes": "5 minutes",
        "5m": "5 minutes",
        "1 hour": "1 hour",
        "1 hours": "1 hour",
        "1h": "1 hour",
        "hour": "1 hour",
        "hours": "1 hour",
        "1 day": "1 day",
        "1 days": "1 day",
        "1d": "1 day",
        "day": "1 day",
        "days": "1 day",
    }
    return mapping.get(ci, default)


def _time_series(
    service: HighScaleService, start_time: str | None, end_time: str | None, chart_interval: str
) -> list[TimeSeriesPoint]:
    start = _range_value(start_time)
    end = _range_value(end_time)
    clauses = [
        "service_id={service_id:String}",
        "dimension='url'",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain='request_aggregate' "
        "AND publication_state='visible')",
    ]
    interval_norm = _normalize_chart_interval(chart_interval, default="1 minute")
    interval_map = {"1 second": 1, "1 minute": 60, "5 minutes": 300, "1 hour": 3600, "1 day": 86400}
    bucket_seconds = interval_map.get(interval_norm, 60)
    params: dict[str, Any] = {"service_id": service.service_id, "bucket_seconds": bucket_seconds}
    if start is not None:
        clauses.append("bucket_start >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("bucket_start < {end:DateTime64(3)}")
        params["end"] = end
    rows = service.client.execute(
        "SELECT toStartOfInterval(bucket_start, INTERVAL {bucket_seconds:UInt32} SECOND) AS chart_bucket, "
        "sum(request_count) AS value "
        "FROM request_aggregates "
        f"WHERE {' AND '.join(clauses)} "
        "GROUP BY chart_bucket ORDER BY chart_bucket",
        params,
    )
    return [
        TimeSeriesPoint(time=safe_iso(row["chart_bucket"]) or str(row["chart_bucket"]), value=float(row["value"]))
        for row in rows
    ]


def _panels(req: AggregatesRequest) -> tuple[bool, bool, bool]:
    """``(time_series, map_data, top_n)`` gates, derived exactly as the standard router derives them."""
    if req.sections:
        sections = set(req.sections)
        return "core" in sections, "core" in sections, "topten" in sections
    return req.include_time_series is not False, req.include_map_data is not False, True


def _field_sql_column(field: str) -> str:
    col_name = _FIELD_DIMENSIONS[field]
    if col_name in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
        return col_name
    if col_name in {"age", "ttl"}:
        return f"toString(toInt32(round(toFloat64OrZero(custom_fields['{col_name}']))))"
    return f"custom_fields['{col_name}']"


def _fields_to_fetch(req: AggregatesRequest, want_map: bool, want_top_n: bool) -> tuple[list[str], list[str]]:
    """``(card_fields, fields_to_query)``; the map is the country card's top values, so it may need an extra field."""
    card_fields = [field for field in (req.fields or list(_FIELD_DIMENSIONS)) if field in _FIELD_DIMENSIONS]
    top_fields = card_fields if want_top_n else []
    to_query = list(top_fields)
    if want_map and "country" not in to_query:
        to_query.append("country")
    return top_fields, to_query


def _facts_time_series(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    norm_interval: str,
    metric: str,
    where_clause: str,
    params: dict[str, Any],
) -> list[TimeSeriesPoint]:
    interval_sql = "toStartOfMinute(event_timestamp)"
    if norm_interval == "1 second":
        interval_sql = "toStartOfSecond(event_timestamp)"
    elif norm_interval == "5 minutes":
        interval_sql = "toStartOfFiveMinutes(event_timestamp)"
    elif norm_interval == "1 hour":
        interval_sql = "toStartOfInterval(event_timestamp, INTERVAL 1 hour)"
    elif norm_interval == "1 day":
        interval_sql = "toStartOfDay(event_timestamp)"

    if metric == "5xx":
        metric_sql = "countIf(custom_fields['status'] >= '500' AND custom_fields['status'] <= '599')"
    elif metric == "4xx":
        metric_sql = "countIf(custom_fields['status'] >= '400' AND custom_fields['status'] <= '499')"
    elif metric == "hit_rate":
        metric_sql = "round(100.0 * countIf(custom_fields['cache'] IN ('HIT', 'HIT-CLUSTER', 'HIT-STALE')) / nullIf(count(), 0), 2)"
    else:
        metric_sql = "count()"

    ts_rows = service.client.execute(
        f"SELECT {interval_sql} AS bucket_start, {metric_sql} AS value FROM request_facts "
        f"WHERE {where_clause} GROUP BY bucket_start ORDER BY bucket_start",
        params,
    )
    return [
        TimeSeriesPoint(time=safe_iso(r["bucket_start"]) or str(r["bucket_start"]), value=float(r["value"]))
        for r in ts_rows
    ]


def _filtered_aggregates(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
):
    start = _range_value(start_time)
    end = _range_value(end_time)
    trace = HighScaleSections(service.service_id, "dashboard")
    request_watermark = trace.run("watermark", lambda: service.watermark_for("request"), activity="overhead")
    want_time_series, want_map, want_top_n = _panels(req)

    if not req.filters:
        filter_dict = {}
    elif hasattr(req.filters, "model_dump"):
        filter_dict = req.filters.model_dump()
    else:
        filter_dict = req.filters
    filter_clauses, filter_params = request_filter_clauses(filter_dict)

    clauses = [*visible_fact_clauses("request"), *filter_clauses]
    params: dict[str, Any] = {"service_id": service.service_id, **filter_params}

    if start is not None and end is not None:
        end_upper = end
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    where_clause = " AND ".join(clauses)

    top_fields, fields_to_query = _fields_to_fetch(req, want_map, want_top_n)

    def fetch_field(field: str):
        sql_col = _field_sql_column(field)
        rows = service.client.execute(
            f"SELECT {sql_col} AS v, count() AS c FROM request_facts WHERE {where_clause} AND {sql_col} != '' "
            "GROUP BY v ORDER BY c DESC LIMIT 10",
            params,
        )
        return field, [(str(r["v"]), int(r["c"])) for r in rows]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = [trace.submit(ex, f"top_n.{f}", partial(fetch_field, f), source="fact") for f in fields_to_query]
        field_results = dict(f.result() for f in futs)

    total_rows_result = trace.run(
        "count",
        lambda: service.client.execute(f"SELECT count() AS c FROM request_facts WHERE {where_clause}", params),
        source="fact",
    )
    total_count = int(total_rows_result[0]["c"]) if total_rows_result else 0

    data = {
        field: FieldAggregate(top=[FieldTopEntry(value=v, count=c) for v, c in field_results[field]], total=total_count)
        for field in top_fields
    }

    map_data = [MapPoint(country=v, count=c) for v, c in field_results.get("country", [])] if want_map else []

    norm_interval = _normalize_chart_interval(req.chart_interval)
    time_series = (
        trace.run(
            "time_series",
            lambda: _facts_time_series(
                service, start_time, end_time, norm_interval, req.chart_metric or "requests", where_clause, params
            ),
            source="fact",
        )
        if want_time_series
        else []
    )

    return AggregatesResponse.with_telemetry(
        data=data,
        time_series=time_series,
        map_data=map_data,
        where_clause="high-scale ClickHouse (filtered)",
        section_timings=trace.timings(),
        interval=norm_interval,
        metric=req.chart_metric,
        total_rows=total_count,
        total_rows_total=total_count,
        earliest_log_at=_watermark_extent(request_watermark.coverage_start),
        latest_log_at=_watermark_extent(request_watermark.coverage_end),
    )


def _aggregates_batch(
    service: HighScaleService, *, start_time: str | None, end_time: str | None, fields: list[str], watermark
):
    start = _range_value(start_time)
    end = _range_value(end_time)
    dims = [_FIELD_DIMENSIONS[f] for f in fields]
    by_dim = query_clickhouse_aggregates_batch(
        service.client,
        service_id=service.service_id,
        domain="request",
        dimensions=dims,
        start=start,
        end=end,
        watermark=watermark,
    )
    return {f: by_dim[_FIELD_DIMENSIONS[f]] for f in fields}


def _cache_agg_resp(cache_key: str | None, resp: AggregatesResponse) -> AggregatesResponse:
    if cache_key is not None:
        _dashboard_memo.put(
            cache_key,
            {
                "data": resp.data,
                "time_series": resp.time_series,
                "map_data": resp.map_data,
                "where_clause": resp.where_clause,
                "interval": resp.interval,
                "metric": resp.metric,
                "total_rows": resp.total_rows,
                "total_rows_total": resp.total_rows_total,
                "earliest_log_at": resp.earliest_log_at,
                "latest_log_at": resp.latest_log_at,
                "section_timings": resp.section_timings,
            },
        )
    resp.response_cache = ResponseCacheObservation.model_validate(
        _dashboard_memo.result({}, state="miss" if cache_key is not None else "bypass")["_response_cache"]
    )
    return resp


def dashboard_aggregates(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
):
    norm_interval = _normalize_chart_interval(req.chart_interval)
    start = _range_value(start_time)
    end = _range_value(end_time)
    cache_key: str | None = None
    if _HS_DASHBOARD_CACHE_TTL > 0 and start_time is not None and end_time is not None:
        raw_filters = (
            req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
        )
        cache_key = digest_cache_key(
            {
                "ep": "hs_dashboard_aggregates",
                "svc": service.service_id,
                "cid": client_cache_token(service.client),
                "s": start.isoformat() if start is not None else None,
                "e": end.isoformat() if end is not None else None,
                "f": serialize_filters_for_key(raw_filters),
                "ci": norm_interval,
                "cm": req.chart_metric,
                "fl": sorted(req.fields) if req.fields else None,
                "sec": sorted(req.sections) if req.sections else None,
                "its": req.include_time_series,
                "icr": req.include_conn_requests,
                "imd": req.include_map_data,
            },
            {"service_id": service.service_id},
        )
        cached = _dashboard_memo.get_with_telemetry(cache_key)
        if cached is not None:
            return AggregatesResponse.with_telemetry(**cached)

    partial_minutes = (
        start is not None
        and end is not None
        and any(bound.second != 0 or bound.microsecond != 0 for bound in (start, end))
    )
    if req.filters or norm_interval == "1 second" or partial_minutes:
        resp = _filtered_aggregates(service, req, start_time, end_time)
        return _cache_agg_resp(cache_key, resp)

    trace = HighScaleSections(service.service_id, "dashboard")
    want_time_series, want_map, want_top_n = _panels(req)
    top_fields, fields_to_query = _fields_to_fetch(req, want_map, want_top_n)
    request_watermark = trace.run("watermark", lambda: service.watermark_for("request"), activity="overhead")
    all_fields_to_query = list(fields_to_query)
    if "url" not in all_fields_to_query:
        all_fields_to_query.append("url")
    responses = trace.run(
        "aggregates_batch",
        lambda: _aggregates_batch(
            service, start_time=start_time, end_time=end_time, fields=all_fields_to_query, watermark=request_watermark
        ),
        source="projection",
    )
    total = responses["url"]

    data = {
        field: FieldAggregate(
            top=[FieldTopEntry(value=value, count=count) for value, count in responses[field].top_values],
            total=responses[field].request_count,
        )
        for field in top_fields
    }
    map_response = responses.get("country")
    map_data = (
        [MapPoint(country=value, count=count) for value, count in map_response.top_values]
        if want_map and map_response is not None
        else []
    )
    if not want_time_series:
        time_series = []
    elif req.chart_metric and req.chart_metric != "requests":
        start = _range_value(start_time)
        end = _range_value(end_time)
        clauses = list(visible_fact_clauses("request"))
        params: dict[str, Any] = {"service_id": service.service_id}
        if start is not None and end is not None:
            end_upper = end
            clauses.append("event_timestamp >= {start:DateTime64(3)}")
            clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
            params.update({"start": start, "end_upper": end_upper})
        time_series = trace.run(
            "time_series",
            lambda: _facts_time_series(
                service, start_time, end_time, norm_interval, req.chart_metric, " AND ".join(clauses), params
            ),
            source="fact",
        )
    else:
        time_series = trace.run(
            "time_series", lambda: _time_series(service, start_time, end_time, norm_interval), source="projection"
        )
    resp = AggregatesResponse.with_telemetry(
        data=data,
        time_series=time_series,
        map_data=map_data,
        where_clause="high-scale ClickHouse",
        section_timings=trace.timings(),
        interval=norm_interval,
        metric=req.chart_metric,
        total_rows=total.request_count,
        total_rows_total=total.request_count,
        earliest_log_at=_watermark_extent(request_watermark.coverage_start),
        latest_log_at=_watermark_extent(request_watermark.coverage_end),
    )
    return _cache_agg_resp(cache_key, resp)


def bundle(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    from backend.high_scale import security as hs_security
    from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

    sections = set(req.sections) if req.sections else None
    want_aggregates = sections is None or bool(sections & {"core", "topten"})
    want_bots = sections is None or "bots" in sections
    res_aggregates = aggregates(service, req, start_time, end_time) if want_aggregates else None
    res_top_bots = hs_security.top_bots(service, req, start_time, end_time) if want_bots else None

    if res_aggregates is not None and hasattr(res_aggregates, "debug_queries"):
        res_aggregates.debug_queries = []
        res_aggregates.debug_calls = []
    if res_top_bots is not None and hasattr(res_top_bots, "debug_queries"):
        res_top_bots.debug_queries = []
        res_top_bots.debug_calls = []

    combined_timings = [
        t for r in (res_aggregates, res_top_bots) if r is not None for t in (getattr(r, "section_timings", None) or [])
    ]
    return {
        **cache_result({}, state="bypass", prewarmer=False),
        "aggregates": res_aggregates,
        "top_bots": res_top_bots,
        "section_timings": combined_timings,
        "debug_queries": get_queries(),
        "debug_calls": get_tracked_calls(),
        "debug_sqlite": list(get_sqlite_queries()),
    }


def field_values(
    service: HighScaleService, req: FieldValuesRequest, start_time: str | None, end_time: str | None
) -> FieldValuesResponse:
    """Return top values for a single field in high-scale ClickHouse mode."""
    from backend.repositories.utils.filters import normalize_filter_key

    clean_field = "".join(ch for ch in req.field if ch.isalnum() or ch == "_")
    if not clean_field:
        raise ValueError("Invalid field name")

    key = normalize_filter_key(clean_field)
    if not key:
        raise ValueError("Invalid field name")

    raw_filters = (
        req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
    )
    filters_excl = {k: v for k, v in raw_filters.items() if k != req.field and normalize_filter_key(k) != key}
    filter_clauses, filter_params = request_filter_clauses(filters_excl)

    limit = max(1, min(int(req.limit or 100), 1000))
    start = _range_value(start_time)
    end = _range_value(end_time)

    if not req.search and not filter_clauses and key in _FIELD_DIMENSIONS:
        agg = _aggregate(
            service,
            start_time=start_time,
            end_time=end_time,
            dimension=_FIELD_DIMENSIONS[key],
            watermark=service.watermark_for("request"),
        )
        vals: list[dict[str, Any]] = [{"value": str(val), "count": int(cnt)} for val, cnt in agg.top_values[:limit]]
        if key == "asn" and vals:
            try:
                from backend.core.duckdb import enrich_asn_labels

                enrich_asn_labels(vals, service.service_id)
            except Exception:
                pass
        return FieldValuesResponse.with_telemetry(values=[FieldTopEntry(**v) for v in vals], field=req.field)

    if key in {"_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind", "edge_score_reason_ind"}:
        return FieldValuesResponse.with_telemetry(values=[], field=req.field)

    params: dict[str, Any] = {"service_id": service.service_id, "limit": limit, **filter_params}
    sql_col = _column(key, params, "target_field")
    clauses = [*visible_fact_clauses("request"), *filter_clauses, f"{sql_col} != ''"]

    if start is not None and end is not None:
        end_upper = inclusive_minute_end(end)
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    if req.search:
        params["search_pat"] = f"%{req.search}%"
        clauses.append(f"{sql_col} ILIKE {{search_pat:String}}")

    where_clause = " AND ".join(clauses)
    rows = service.client.execute(
        f"SELECT {sql_col} AS v, count() AS c FROM request_facts "
        f"WHERE {where_clause} GROUP BY v ORDER BY c DESC LIMIT {{limit:UInt32}}",
        params,
    )
    vals_facts: list[dict[str, Any]] = [{"value": str(r["v"]), "count": int(r["c"])} for r in rows]
    if key == "asn" and vals_facts:
        try:
            from backend.core.duckdb import enrich_asn_labels

            enrich_asn_labels(vals_facts, service.service_id)
        except Exception:
            pass
    return FieldValuesResponse.with_telemetry(values=[FieldTopEntry(**v) for v in vals_facts], field=req.field)


def raw_df(
    service: HighScaleService, req: RawRequest, start_time: str | None, end_time: str | None, *, limit: int = 50000
):
    """Fetch raw rows from ClickHouse ``request_facts`` as a pandas DataFrame for CSV export."""
    import pandas as pd

    from backend.utils.bot_sources import enrich_bot_metadata

    raw_filters = (
        req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
    )
    filter_clauses, filter_params = request_filter_clauses(raw_filters)
    clauses = [*visible_fact_clauses("request"), *filter_clauses]
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "limit": max(1, min(int(limit), 50000)),
        **filter_params,
    }

    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None and end is not None:
        end_upper = inclusive_minute_end(end)
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    where_clause = " AND ".join(clauses)
    rows = service.client.execute(
        "SELECT event_timestamp AS timestamp, client_ip AS ip, url, country, custom_fields "
        f"FROM request_facts WHERE {where_clause} "
        "ORDER BY event_timestamp DESC LIMIT {limit:UInt32}",
        params,
    )
    if not rows:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for r in rows:
        cf = r.get("custom_fields")
        custom = dict(cf) if isinstance(cf, dict) else {}
        ts = r.get("timestamp")
        records.append(
            {
                **custom,
                "timestamp": safe_iso(ts) if isinstance(ts, datetime) else str(ts or ""),
                "ip": str(r.get("ip") or ""),
                "url": str(r.get("url") or ""),
                "country": str(r.get("country") or ""),
            }
        )

    df = pd.DataFrame(records)
    enrich_bot_metadata(df)

    if req.columns:
        existing_cols = [c for c in req.columns if c in df.columns]
        if "_bot_name" in req.columns and "_bot_name" not in existing_cols:
            df["_bot_name"] = "null"
            existing_cols.append("_bot_name")
        if "_ngwaf_bot_name" in req.columns and "_ngwaf_bot_name" not in existing_cols:
            df["_ngwaf_bot_name"] = None
            existing_cols.append("_ngwaf_bot_name")
        df = df[existing_cols]

    return df


# ── Folded from backend/high_scale/origin.py ─────────────────────────────────

import sys
from dataclasses import dataclass

from backend.high_scale._common import _TIMESERIES_METRICS, RELATIVE_ERROR, build_published_facts_where
from backend.high_scale._common import optional_int as _integer
from backend.high_scale._common import sketch as _sketch

_EMPTY = {
    "summary": {},
    "timeseries": {"has_data": False, "series": []},
    "slow_urls": {"has_data": False, "rows": []},
    "status_codes": {"has_data": False, "rows": []},
    "path_breakdown": {"has_data": False, "shielding_detected": False, "rows": []},
    "pop_latency": {"has_data": False, "requires_group_c": False, "rows": []},
    "ip_health": {"has_data": False, "rows": []},
}

_APPROX_NOTE = (
    "Latency percentiles are merged from per-minute log-histogram sketches "
    f"(relative error at most {RELATIVE_ERROR:.0%}). Request counts and error rates are exact."
)
_APPROX = {"_approx": True, "_approx_note": _APPROX_NOTE}
_NOT_APPROX = {"_approx": False}

_BASE_COLUMNS = (
    "event_timestamp, url, "
    "coalesce(toFloat64OrNull(custom_fields['ottfb']), toFloat64OrNull(custom_fields['ttfb']) * 1000000.0) "
    "AS latency_us, "
    "toInt32OrNull(custom_fields['ost']) AS status, "
    "upper(custom_fields['cache']) AS cache_u, "
    "toFloat64OrNull(custom_fields['ottlb']) AS ottlb_us, "
    "toFloat64OrNull(custom_fields['elapsed']) AS elapsed_us, "
    "toInt64OrNull(custom_fields['obytes']) AS obytes, "
    "custom_fields['pop'] AS pop, "
    "multiIf(lower(custom_fields['edge']) IN ('true', '1'), 'true', "
    "lower(custom_fields['edge']) IN ('false', '0'), 'false', '') AS edge_s, "
    "custom_fields['oip'] AS oip"
)
_ORDER_BY_P95 = "ifNotFinite(latency_q[2], -1.0) DESC"
_LATENCY_DIMENSIONS = {
    "edge": ("edge_s", "latency_us IS NOT NULL AND edge_s != ''", 0),
    "pop": ("pop", "latency_us IS NOT NULL AND pop != ''", 0),
    "oip": ("oip", "status IS NOT NULL AND oip != ''", 10),
}


@dataclass(frozen=True)
class FactsScope:
    base: str
    params: dict[str, Any]


def _facts_scope(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any] | None
) -> FactsScope:
    where_sql, params = build_published_facts_where(service.service_id, start_time, end_time, filters)
    return FactsScope(f"(SELECT {_BASE_COLUMNS} FROM request_facts WHERE {where_sql})", params)


def _sketch_select(alias: str, column: str, quantiles: tuple[float, ...]) -> str:
    """Merge the per-minute ``{column}_sk_v/_sk_w`` histograms and read ``quantiles`` from the union."""
    levels = ", ".join(str(level) for level in quantiles)
    return (
        f"quantilesExactWeightedArray({levels})({column}_sk_v, {column}_sk_w) AS {alias}_q, "
        f"sumArray({column}_sk_w) AS {alias}_n"
    )


def _quantile(row: Mapping[str, Any], alias: str, index: int, count_key: str | None = None) -> float | None:
    """The ``index``-th requested quantile in milliseconds-ready microseconds; None with no observations."""
    if int(row.get(count_key or f"{alias}_n") or 0) == 0:
        return None
    values = row.get(alias if count_key else f"{alias}_q") or []
    return _float_or_none(values[index]) if index < len(values) else None


def _resolve_timeseries_metric(req: Any) -> tuple[str, str, float]:
    if req.split_by_leg:
        raise ValueError("high-scale Origin timeseries does not support split_by_leg")
    if req.bucket_minutes < 1:
        raise ValueError("high-scale Origin timeseries requires bucket_minutes of at least 1")
    metric = _TIMESERIES_METRICS.get((req.timeseries_metric, req.timeseries_percentile))
    if metric is None:
        raise ValueError("high-scale Origin timeseries does not support the requested metric and percentile")
    fact_col, level = metric
    proj_col = "latency" if fact_col == "latency_us" else "ttlb"
    return fact_col, proj_col, level


def _origin_summary(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    where, params = _where(service.service_id, "origin_summary", start_time, end_time)
    rows = service.client.execute(
        "/* origin:summary */ SELECT "
        "sum(requests) AS requests, sum(misses) AS misses, sum(passes) AS passes, "
        "sum(origin_5xx) AS origin_5xx, sum(status_count) AS status_count, "
        f"{_sketch_select('latency', 'latency', (0.5, 0.75, 0.95, 0.99))}, "
        f"{_sketch_select('ttlb', 'ttlb', (0.5, 0.95))}, "
        f"{_sketch_select('overhead', 'overhead', (0.5,))}, "
        f"{_sketch_select('bytes', 'origin_bytes', (0.5,))} "
        f"FROM origin_minute_summary WHERE {where}",
        params,
    )
    row = rows[0] if rows else {}
    requests = int(row.get("requests") or 0)
    latency_p50 = _quantile(row, "latency", 0)
    if requests == 0 or latency_p50 is None:
        return {
            "has_data": False,
            "total_misses": None,
            "total_passes": None,
            "ottfb_p50_ms": None,
            "ottfb_p75_ms": None,
            "ottfb_p95_ms": None,
            "ottfb_p99_ms": None,
            "ottlb_p50_ms": None,
            "ottlb_p95_ms": None,
            "cdn_overhead_p50_ms": None,
            "origin_error_rate": None,
            "obytes_p50": None,
        }
    return {
        "has_data": True,
        "total_misses": int(row.get("misses") or 0),
        "total_passes": int(row.get("passes") or 0),
        "ottfb_p50_ms": _milliseconds(latency_p50),
        "ottfb_p75_ms": _milliseconds(_quantile(row, "latency", 1)),
        "ottfb_p95_ms": _milliseconds(_quantile(row, "latency", 2)),
        "ottfb_p99_ms": _milliseconds(_quantile(row, "latency", 3)),
        "ottlb_p50_ms": _milliseconds(_quantile(row, "ttlb", 0)),
        "ottlb_p95_ms": _milliseconds(_quantile(row, "ttlb", 1)),
        "cdn_overhead_p50_ms": _milliseconds(_quantile(row, "overhead", 0)),
        "origin_error_rate": (
            int(row.get("origin_5xx") or 0) / int(row["status_count"])
            if int(row.get("status_count") or 0) > 0
            else None
        ),
        "obytes_p50": _quantile(row, "bytes", 0),
        **_APPROX,
    }


def _timeseries(service: HighScaleService, req: Any, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    _, metric_column, level = _resolve_timeseries_metric(req)
    where, params = _where(service.service_id, "origin_summary", start_time, end_time)
    params["bucket_minutes"] = int(req.bucket_minutes)
    rows = service.client.execute(
        "/* origin:timeseries */ SELECT "
        "toStartOfInterval(bucket_start, toIntervalMinute({bucket_minutes:UInt32})) AS time, "
        "sum(misses) AS miss_count, "
        f"{_sketch_select('value', metric_column, (level,))} "
        f"FROM origin_minute_summary WHERE {where} "
        "GROUP BY time ORDER BY time",
        params,
    )
    return {
        "has_data": bool(rows),
        "series": [
            {
                "time": safe_iso(row["time"]),
                "miss_count": int(row.get("miss_count") or 0),
                "value": _milliseconds(_quantile(row, "value", 0)),
            }
            for row in rows
        ],
        **_APPROX,
    }


def _facts_summary(service: HighScaleService, facts: FactsScope) -> dict[str, Any]:
    rows = service.client.execute(
        "/* origin:facts_summary */ SELECT "
        "countIf(latency_us IS NOT NULL) AS requests, "
        "countIf(latency_us IS NOT NULL AND cache_u LIKE 'MISS%') AS misses, "
        "countIf(latency_us IS NOT NULL AND cache_u LIKE 'PASS%') AS passes, "
        "countIf(latency_us IS NOT NULL AND status BETWEEN 500 AND 599) AS origin_5xx, "
        "countIf(latency_us IS NOT NULL AND status IS NOT NULL) AS status_count, "
        "quantilesExact(0.5, 0.75, 0.95, 0.99)(latency_us) AS latency_q, "
        "countIf(latency_us IS NOT NULL AND ottlb_us IS NOT NULL) AS ttlb_n, "
        "quantilesExactIf(0.5, 0.95)(ottlb_us, latency_us IS NOT NULL AND ottlb_us IS NOT NULL) AS ttlb_q, "
        "countIf(latency_us IS NOT NULL AND ottlb_us IS NOT NULL AND elapsed_us IS NOT NULL) AS overhead_n, "
        "quantileExactIf(0.5)(elapsed_us - ottlb_us, "
        "latency_us IS NOT NULL AND ottlb_us IS NOT NULL AND elapsed_us IS NOT NULL) AS overhead_q, "
        "countIf(latency_us IS NOT NULL AND obytes >= 0) AS bytes_n, "
        "quantileExactIf(0.5)(toFloat64(obytes), latency_us IS NOT NULL AND obytes >= 0) AS bytes_q "
        f"FROM {facts.base}",
        facts.params,
    )
    row = rows[0] if rows else {}
    requests = int(row.get("requests") or 0)
    if requests == 0:
        return {
            "has_data": False,
            "total_misses": None,
            "total_passes": None,
            "ottfb_p50_ms": None,
            "ottfb_p75_ms": None,
            "ottfb_p95_ms": None,
            "ottfb_p99_ms": None,
            "ottlb_p50_ms": None,
            "ottlb_p95_ms": None,
            "cdn_overhead_p50_ms": None,
            "origin_error_rate": None,
            "obytes_p50": None,
            **_NOT_APPROX,
        }
    status_count = int(row.get("status_count") or 0)
    overhead_n = int(row.get("overhead_n") or 0)
    bytes_n = int(row.get("bytes_n") or 0)
    return {
        "has_data": True,
        "total_misses": int(row.get("misses") or 0),
        "total_passes": int(row.get("passes") or 0),
        "ottfb_p50_ms": _milliseconds(_quantile(row, "latency_q", 0, "requests")),
        "ottfb_p75_ms": _milliseconds(_quantile(row, "latency_q", 1, "requests")),
        "ottfb_p95_ms": _milliseconds(_quantile(row, "latency_q", 2, "requests")),
        "ottfb_p99_ms": _milliseconds(_quantile(row, "latency_q", 3, "requests")),
        "ottlb_p50_ms": _milliseconds(_quantile(row, "ttlb_q", 0, "ttlb_n")),
        "ottlb_p95_ms": _milliseconds(_quantile(row, "ttlb_q", 1, "ttlb_n")),
        "cdn_overhead_p50_ms": _milliseconds(float(row["overhead_q"])) if overhead_n else None,
        "origin_error_rate": int(row.get("origin_5xx") or 0) / status_count if status_count else None,
        "obytes_p50": float(row["bytes_q"]) if bytes_n else None,
        **_NOT_APPROX,
    }


def _facts_timeseries(service: HighScaleService, facts: FactsScope, req: Any) -> dict[str, Any]:
    column, _, level = _resolve_timeseries_metric(req)
    rows = service.client.execute(
        "/* origin:facts_timeseries */ SELECT "
        "toStartOfInterval(event_timestamp, toIntervalMinute({bucket_minutes:UInt32})) AS time, "
        "countIf(cache_u LIKE 'MISS%') AS miss_count, "
        f"countIf({column} IS NOT NULL) AS value_n, "
        f"quantileExact({level})({column}) AS value "
        f"FROM {facts.base} WHERE latency_us IS NOT NULL GROUP BY time ORDER BY time",
        {**facts.params, "bucket_minutes": int(req.bucket_minutes)},
    )
    return {
        "has_data": bool(rows),
        "series": [
            {
                "time": safe_iso(row["time"]),
                "miss_count": int(row.get("miss_count") or 0),
                "value": _milliseconds(row["value"]) if int(row.get("value_n") or 0) else None,
            }
            for row in rows
        ],
        **_NOT_APPROX,
    }


def _facts_slow_urls(service: HighScaleService, facts: FactsScope, req: Any) -> dict[str, Any]:
    rows = service.client.execute(
        "/* origin:facts_slow_urls */ SELECT url AS value, count() AS requests, "
        "quantilesExact(0.5, 0.95, 0.99)(latency_us) AS latency_q "
        f"FROM {facts.base} WHERE latency_us IS NOT NULL AND url != '' GROUP BY url "
        f"HAVING requests >= {{min_requests:UInt64}} ORDER BY {_ORDER_BY_P95} LIMIT {{limit:UInt32}}",
        {**facts.params, "min_requests": int(req.slow_urls_min_requests), "limit": int(req.slow_urls_limit)},
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "url": row["value"],
                "requests": int(row["requests"]),
                "p50_ms": _milliseconds(_quantile(row, "latency_q", 0, "requests")),
                "p95_ms": _milliseconds(_quantile(row, "latency_q", 1, "requests")),
                "p99_ms": _milliseconds(_quantile(row, "latency_q", 2, "requests")),
            }
            for row in rows
        ],
        **_NOT_APPROX,
    }


def _facts_status_codes(service: HighScaleService, facts: FactsScope) -> dict[str, Any]:
    rows = service.client.execute(
        "/* origin:facts_status_codes */ SELECT "
        "toInt32(multiIf(status BETWEEN 100 AND 599, status, -1)) AS value, count() AS requests "
        f"FROM {facts.base} WHERE status IS NOT NULL GROUP BY value ORDER BY requests DESC",
        facts.params,
    )
    total = sum(int(row["requests"]) for row in rows)
    return {
        "has_data": bool(rows),
        "rows": [
            {"status": int(row["value"]), "count": int(row["requests"]), "pct": int(row["requests"]) * 100.0 / total}
            for row in rows
            if total > 0
        ],
        **_NOT_APPROX,
    }


def _facts_latency_dimension(
    service: HighScaleService, facts: FactsScope, *, marker: str, dimension: str, limit: int | None = None
) -> list[dict[str, Any]]:
    column, predicate, minimum = _LATENCY_DIMENSIONS[dimension]
    params = dict(facts.params)
    suffix = f" HAVING requests >= {minimum}" if minimum else ""
    suffix += f" ORDER BY {_ORDER_BY_P95}"
    if limit is not None:
        params["limit"] = int(limit)
        suffix += " LIMIT {limit:UInt32}"
    return service.client.execute(
        f"/* origin:facts_{marker} */ SELECT {column} AS value, count() AS requests, "
        "countIf(status BETWEEN 500 AND 599) AS origin_5xx, "
        "countIf(latency_us IS NOT NULL) AS latency_n, "
        "quantilesExact(0.5, 0.95)(latency_us) AS latency_q "
        f"FROM {facts.base} WHERE {predicate} GROUP BY {column}{suffix}",
        params,
    )


def _dimension_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    select: str,
    order_by: str,
    limit: int | None = None,
    having: str | None = None,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    where, params = _where(service.service_id, "origin_dimensions", start_time, end_time, dimension=dimension)
    suffix = f" HAVING {having}" if having else ""
    params.update(extra_params or {})
    if limit is not None:
        params["limit"] = int(limit)
        suffix += " ORDER BY " + order_by + " LIMIT {limit:UInt32}"
    else:
        suffix += " ORDER BY " + order_by
    return service.client.execute(
        f"/* origin:{marker} */ SELECT value, {select} "
        f"FROM origin_minute_dimensions WHERE {where} GROUP BY value{suffix}",
        params,
    )


def _slow_urls(service: HighScaleService, req: Any, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    rows = _dimension_rows(
        service,
        marker="slow_urls",
        dimension="url",
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(origin_minute_dimensions.requests) AS requests, "
            f"{_sketch_select('latency', 'latency', (0.5, 0.95, 0.99))}"
        ),
        having="sum(origin_minute_dimensions.requests) >= {min_requests:UInt64}",
        order_by="latency_q[2] DESC",
        limit=req.slow_urls_limit,
        extra_params={"min_requests": int(req.slow_urls_min_requests)},
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "url": row["value"],
                "requests": int(row["requests"]),
                "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                "p99_ms": _milliseconds(_quantile(row, "latency", 2)),
            }
            for row in rows
        ],
        **_APPROX,
    }


def _status_codes(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    rows = _dimension_rows(
        service,
        marker="status_codes",
        dimension="status",
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(origin_minute_dimensions.requests) AS requests, "
            "sum(sum(origin_minute_dimensions.requests)) OVER () AS total_requests"
        ),
        order_by="requests DESC",
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "status": int(row["value"]),
                "count": int(row["requests"]),
                "pct": (int(row["requests"]) * 100.0 / int(row["total_requests"])),
            }
            for row in rows
            if int(row.get("total_requests") or 0) > 0
        ],
    }


def _latency_dimension(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    limit: int | None = None,
    having: str | None = None,
) -> list[dict[str, Any]]:
    return _dimension_rows(
        service,
        marker=marker,
        dimension=dimension,
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(origin_minute_dimensions.requests) AS requests, "
            "sum(origin_5xx) AS origin_5xx, "
            f"{_sketch_select('latency', 'latency', (0.5, 0.95))}"
        ),
        order_by="latency_q[2] DESC",
        limit=limit,
        having=having,
    )


def _milliseconds(value: object) -> float | None:
    parsed = _float_or_none(value)
    return parsed / 1000.0 if parsed is not None and math.isfinite(parsed) else None


def _float_or_none(value: object) -> float | None:
    return float(str(value)) if value is not None else None


def origin_aggregates(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None, *, sections: set[str] | None
) -> dict[str, Any]:
    filter_sql, _ = build_clickhouse_filters(req.filters)
    facts = _facts_scope(service, start_time, end_time, req.filters) if filter_sql != "1=1" else None
    approx = _APPROX if facts is None else _NOT_APPROX

    def latency_rows(marker: str, dimension: str, limit: int | None = None, having: str | None = None):
        if facts is not None:
            return _facts_latency_dimension(service, facts, marker=marker, dimension=dimension, limit=limit)
        return _latency_dimension(
            service,
            marker=marker,
            dimension=dimension,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            having=having,
        )

    sections = sections or set(_EMPTY)

    def _build_path_breakdown() -> dict[str, Any]:
        rows = latency_rows("path_breakdown", "edge")
        return {
            "has_data": bool(rows),
            "shielding_detected": any(row["value"] == "false" for row in rows),
            "rows": [
                {
                    "edge": row["value"] == "true",
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                    "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                }
                for row in rows
            ],
            **approx,
        }

    def _build_pop_latency() -> dict[str, Any]:
        rows = latency_rows("pop_latency", "pop", limit=req.pop_latency_limit)
        p95s = [value for row in rows if (value := _milliseconds(_quantile(row, "latency", 1))) is not None]
        p95s.sort()
        median_p95 = p95s[len(p95s) // 2] if p95s else 0
        return {
            "has_data": bool(rows),
            "requires_group_c": False,
            "median_p95_ms": median_p95,
            "rows": [
                {
                    "pop": row["value"],
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                    "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                    "elevated": _is_elevated(_quantile(row, "latency", 1), median_p95),
                }
                for row in rows
            ],
            **approx,
        }

    def _build_ip_health() -> dict[str, Any]:
        rows = latency_rows(
            "ip_health", "oip", limit=req.ip_health_limit, having="sum(origin_minute_dimensions.requests) >= 10"
        )
        return {
            "has_data": bool(rows),
            "rows": [
                {
                    "oip": row["value"],
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                    "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                    "error_pct": int(row.get("origin_5xx") or 0) * 100.0 / int(row["requests"]),
                }
                for row in rows
                if int(row["requests"]) > 0
            ],
            **approx,
        }

    tasks: list[tuple[str, Any]] = []
    if "summary" in sections:
        tasks.append(
            (
                "summary",
                lambda: (
                    _origin_summary(service, start_time, end_time) if facts is None else _facts_summary(service, facts)
                ),
            )
        )
    if "timeseries" in sections:
        tasks.append(
            (
                "timeseries",
                lambda: (
                    _timeseries(service, req, start_time, end_time)
                    if facts is None
                    else _facts_timeseries(service, facts, req)
                ),
            )
        )
    if "slow_urls" in sections:
        tasks.append(
            (
                "slow_urls",
                lambda: (
                    _slow_urls(service, req, start_time, end_time)
                    if facts is None
                    else _facts_slow_urls(service, facts, req)
                ),
            )
        )
    if "status_codes" in sections:
        tasks.append(
            (
                "status_codes",
                lambda: (
                    _status_codes(service, start_time, end_time)
                    if facts is None
                    else _facts_status_codes(service, facts)
                ),
            )
        )
    if "path_breakdown" in sections:
        tasks.append(("path_breakdown", _build_path_breakdown))
    if "pop_latency" in sections:
        tasks.append(("pop_latency", _build_pop_latency))
    if "ip_health" in sections:
        tasks.append(("ip_health", _build_ip_health))

    trace = HighScaleSections(service.service_id, "origin")
    result = trace.run_parallel(tasks, source="projection" if facts is None else "fact")

    summary = result.get("summary", _EMPTY["summary"])
    return {"has_data": bool(summary.get("has_data")), **result, "section_timings": trace.timings()}


def _is_elevated(value_us: object, median_p95_ms: float) -> bool:
    value_ms = _milliseconds(value_us)
    return value_ms is not None and value_ms > median_p95_ms * 2


# ── Pure request-event projection (folded from backend.high_scale.origin_projection) ──


@dataclass(frozen=True)
class OriginProjection:
    summary_rows: tuple[dict[str, Any], ...]
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _OriginAccumulator:
    requests: int = 0
    misses: int = 0
    passes: int = 0
    origin_5xx: int = 0
    status_count: int = 0
    origin_bytes: int = 0
    latencies: list[float] = _dc_field(default_factory=list)
    ttlb_values: list[float] = _dc_field(default_factory=list)
    overhead_values: list[float] = _dc_field(default_factory=list)
    byte_values: list[float] = _dc_field(default_factory=list)


def build_origin_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> OriginProjection:
    summaries: dict[datetime, _OriginAccumulator] = defaultdict(_OriginAccumulator)
    dimensions: dict[tuple[datetime, str, str], _OriginAccumulator] = defaultdict(_OriginAccumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        latency = _origin_latency_us(row)
        status = _integer(row.get("ost"))
        origin_bytes = _non_negative_integer(row.get("obytes"))
        if latency is not None:
            summary = summaries[bucket]
            _add_latency_metrics(summary, row, latency, status, origin_bytes)

        if status is not None:
            status_value = str(status if 100 <= status <= 599 else -1)
            status_acc = dimensions[(bucket, "status", status_value)]
            status_acc.requests += 1
            status_acc.origin_5xx += int(500 <= status < 600)

        for dimension, value in _latency_dimensions(row):
            if latency is None:
                continue
            acc = dimensions[(bucket, dimension, value)]
            acc.requests += 1
            _add_dimension_metrics(acc, latency, status, origin_bytes)

        origin_ip = _text(row.get("oip"))
        if origin_ip and status is not None:
            acc = dimensions[(bucket, "oip", origin_ip)]
            acc.requests += 1
            _add_dimension_metrics(acc, latency, status, origin_bytes)

    return OriginProjection(
        summary_rows=tuple(_origin_summary_row(bucket, acc) for bucket, acc in sorted(summaries.items())),
        dimension_rows=tuple(
            _origin_dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
    )


def _add_latency_metrics(
    acc: _OriginAccumulator, row: Mapping[str, Any], latency: float, status: int | None, origin_bytes: int | None
) -> None:
    acc.requests += 1
    acc.latencies.append(latency)
    cache = _text(row.get("cache")).upper()
    acc.misses += int(cache.startswith("MISS"))
    acc.passes += int(cache.startswith("PASS"))
    acc.origin_5xx += int(status is not None and 500 <= status < 600)
    acc.status_count += int(status is not None)
    if origin_bytes is not None:
        acc.origin_bytes += origin_bytes
        acc.byte_values.append(float(origin_bytes))
    ttlb = _finite_float(row.get("ottlb"))
    if ttlb is not None:
        acc.ttlb_values.append(ttlb)
        elapsed = _finite_float(row.get("elapsed"))
        if elapsed is not None:
            acc.overhead_values.append(elapsed - ttlb)


def _add_dimension_metrics(
    acc: _OriginAccumulator, latency: float | None, status: int | None, origin_bytes: int | None
) -> None:
    if latency is not None:
        acc.latencies.append(latency)
    acc.origin_5xx += int(status is not None and 500 <= status < 600)
    if origin_bytes is not None:
        acc.origin_bytes += origin_bytes


def _origin_summary_row(bucket: datetime, acc: _OriginAccumulator) -> dict[str, Any]:
    lat_p50, lat_p75, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.75, 0.95, 0.99))
    ttlb_p50, ttlb_p95 = _percentiles(acc.ttlb_values, (0.50, 0.95))
    (overhead_p50,) = _percentiles(acc.overhead_values, (0.50,))
    (bytes_p50,) = _percentiles(acc.byte_values, (0.50,))
    latency_sk = _sketch(acc.latencies)
    ttlb_sk = _sketch(acc.ttlb_values)
    overhead_sk = _sketch(acc.overhead_values)
    bytes_sk = _sketch(acc.byte_values)
    return {
        "bucket_start": bucket.isoformat(),
        "requests": acc.requests,
        "misses": acc.misses,
        "passes": acc.passes,
        "origin_5xx": acc.origin_5xx,
        "status_count": acc.status_count,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "ttlb_count": len(acc.ttlb_values),
        "overhead_count": len(acc.overhead_values),
        "origin_bytes_count": len(acc.byte_values),
        "latency_p50_us": lat_p50,
        "latency_p75_us": lat_p75,
        "latency_p95_us": lat_p95,
        "latency_p99_us": lat_p99,
        "ttlb_p50_us": ttlb_p50,
        "ttlb_p95_us": ttlb_p95,
        "cdn_overhead_p50_us": overhead_p50,
        "origin_bytes_p50": bytes_p50,
        "latency_sk_v": latency_sk[0],
        "latency_sk_w": latency_sk[1],
        "ttlb_sk_v": ttlb_sk[0],
        "ttlb_sk_w": ttlb_sk[1],
        "overhead_sk_v": overhead_sk[0],
        "overhead_sk_w": overhead_sk[1],
        "origin_bytes_sk_v": bytes_sk[0],
        "origin_bytes_sk_w": bytes_sk[1],
    }


def _origin_dimension_row(bucket: datetime, dimension: str, value: str, acc: _OriginAccumulator) -> dict[str, Any]:
    lat_p50, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.95, 0.99))
    latency_sk = _sketch(acc.latencies)
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "origin_5xx": acc.origin_5xx,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "latency_p50_us": lat_p50,
        "latency_p95_us": lat_p95,
        "latency_p99_us": lat_p99,
        "latency_sk_v": latency_sk[0],
        "latency_sk_w": latency_sk[1],
    }


def _latency_dimensions(row: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = []
    for dimension, source_field in (("url", "url"), ("pop", "pop")):
        value = _text(row.get(source_field))
        if value:
            values.append((dimension, value))
    edge = row.get("edge")
    if isinstance(edge, bool):
        values.append(("edge", "true" if edge else "false"))
    return tuple(values)


def _origin_latency_us(row: Mapping[str, Any]) -> float | None:
    ottfb = _finite_float(row.get("ottfb"))
    if ottfb is not None:
        return ottfb
    ttfb = _finite_float(row.get("ttfb"))
    return ttfb * 1_000_000.0 if ttfb is not None else None


def _non_negative_integer(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed >= 0 else None


# ── Folded from backend/high_scale/security.py ───────────────────────────────

from dataclasses import dataclass
from typing import Literal

from backend.core.high_scale_sections import clickhouse_source
from backend.high_scale._common import _bucket_case
from backend.high_scale._common import build_clickhouse_filters as _build_clickhouse_filters
from backend.models.dashboard import SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.repositories.security import _BOT_UA_CLASSIFY_LIMIT, classify_ua_counts, summarize_wellknown_bot_rows
from backend.repositories.utils.response_cache import ResponseMemo
from backend.utils.bounded_cache import CacheRegistry

_security_memo = ResponseMemo(ttl=30.0, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_security_aggregates_memo", _security_memo.cache)

_WHERE = "__WHERE__"

_REQ_HEADER_BUCKETS = (
    (256, "0-256B"),
    (512, "256-512B"),
    (768, "512-768B"),
    (1024, "768B-1KB"),
    (1536, "1-1.5KB"),
    (2048, "1.5-2KB"),
    (3072, "2-3KB"),
    (4096, "3-4KB"),
    (6144, "4-6KB"),
    (8192, "6-8KB"),
    (12288, "8-12KB"),
    (16384, "12-16KB"),
    (24576, "16-24KB"),
    (32768, "24-32KB"),
)
_REQ_HEADER_LAST = ">32KB"
_CONN_REUSE_BUCKETS = ((1, "1 (None)"), (5, "2-5"), (20, "6-20"), (100, "21-100"))
_CONN_REUSE_LAST = ">100"

from backend.utils.pop_utils import _effective_pop_lat_lon_map

_POP_COORDS = tuple((code, lat, lon) for code, (lat, lon) in sorted(_effective_pop_lat_lon_map().items()))
_POP_DEFAULT = (38.0, -97.0)

_IS_IPV6 = "lower(custom_fields['is_ipv6']) IN ('1', 'true', 't', 'yes')"
_IS_BOT = "lower(custom_fields['is_bot']) IN ('1', 'true', 't', 'yes')"


def _pop_coord(index: int, default: float) -> str:
    arms = ", ".join(f"pop = '{name}', {(lat, lon)[index]}" for name, lat, lon in _POP_COORDS)
    return f"multiIf({arms}, {default})"


_POP_LAT = _pop_coord(0, _POP_DEFAULT[0])
_POP_LON = _pop_coord(1, _POP_DEFAULT[1])
_DISTANCE_KM = (
    "6371 * 2 * asin(sqrt(pow(sin(radians(lat - pop_lat) / 2), 2) "
    "+ cos(radians(pop_lat)) * cos(radians(lat)) * pow(sin(radians(lon - pop_lon) / 2), 2)))"
)
_IMPOSSIBLE_DISTANCE = (
    f"multiIf(rtt_min IS NULL OR lat IS NULL OR lon IS NULL, 0, "
    f"ifNull({_DISTANCE_KM} > (rtt_min / 1000.0 * 100.0 + 150.0), 0))"
)
_TUNNEL = "ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.1, 0)"


@dataclass(frozen=True)
class _Scope:
    service_id: str
    start: datetime
    end: datetime
    where: str
    params: dict[str, Any]
    filtered: bool


def _scope(service: HighScaleService, req: Any, start_time: str | None, end_time: str | None) -> _Scope:
    start, end = parse_time_window(start_time, end_time)
    filter_sql, filter_params = _build_clickhouse_filters(getattr(req, "filters", None) or {})
    where = (
        f"service_id={{service_id:String}} AND {visible_publication_fence_sql('request')} "
        "AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)} "
        f"AND {filter_sql}"
    )
    return _Scope(
        service_id=service.service_id,
        start=start,
        end=end,
        where=where,
        params={"service_id": service.service_id, "start": start, "end": end, **filter_params},
        filtered=filter_sql != "1=1",
    )


def _run(service: HighScaleService, scope: _Scope, tag: str, sql: str, **extra: Any) -> list[dict[str, Any]]:
    source: Literal["fact", "projection"] = (
        "projection" if tag in {"ngwaf_bots", "ngwaf_verified_bots_ts"} and not scope.filtered else "fact"
    )
    with clickhouse_source(source):
        return service.client.execute(
            f"/* hs_security:{tag} */ {sql.replace(_WHERE, scope.where)}", {**scope.params, **extra}
        )


def _sec_iso(ts: Any) -> str | None:
    return safe_iso(datetime.fromtimestamp(int(ts), UTC).replace(tzinfo=None))


def _bucket_seconds(req: Any) -> int:
    return max(1, int(getattr(req, "bucket_seconds", 300) or 300))


def _bucket_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"bucket": r["bucket"], "count": int(r["count"])} for r in rows]


def _tls_fingerprints(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "tls_fingerprints",
        """
        SELECT custom_fields['tls_ciphers_sha'] AS fingerprint,
               uniqExact(client_ip) AS ip_count,
               count() AS request_count
        FROM request_facts
        WHERE __WHERE__ AND custom_fields['tls_ciphers_sha'] != ''
        GROUP BY fingerprint ORDER BY request_count DESC LIMIT 20
        """,
    )
    return [
        {"fingerprint": r["fingerprint"], "ip_count": int(r["ip_count"]), "request_count": int(r["request_count"])}
        for r in rows
    ]


def _fingerprint_coverage(service: HighScaleService, scope: _Scope) -> dict[str, float]:
    rows = _run(
        service,
        scope,
        "fingerprint_coverage",
        """
        SELECT count() AS total_rows, countIf(custom_fields['tls_ciphers_sha'] != '') AS populated
        FROM request_facts WHERE __WHERE__
        """,
    )
    total = int(rows[0]["total_rows"]) if rows else 0
    populated = int(rows[0]["populated"]) if rows else 0
    return {"tls_ciphers_sha": populated / total if total > 0 else 0.0}


def _req_size_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    case = _bucket_case("h", _REQ_HEADER_BUCKETS, _REQ_HEADER_LAST)
    return _bucket_rows(
        _run(
            service,
            scope,
            "req_size_dist",
            f"""
            SELECT {case} AS bucket, count() AS count, min(h) AS min_val
            FROM (
                SELECT toUInt64OrNull(custom_fields['req_header_bytes']) AS h
                FROM request_facts WHERE __WHERE__
            )
            WHERE h IS NOT NULL
            GROUP BY bucket ORDER BY min_val
            """,
        )
    )


def _top_ips_header(service: HighScaleService, scope: _Scope, mask_ips: bool) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "top_ips_header",
        """
        SELECT client_ip AS ip, max(h) AS max_header
        FROM (
            SELECT client_ip, toUInt64OrNull(custom_fields['req_header_bytes']) AS h
            FROM request_facts WHERE __WHERE__ AND client_ip != ''
        )
        WHERE h IS NOT NULL
        GROUP BY ip ORDER BY max_header DESC LIMIT 10
        """,
    )
    return [{"ip": _maybe_mask(r["ip"], mask_ips), "max_header": int(r["max_header"])} for r in rows]


def _conn_reuse_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    case = _bucket_case("c", _CONN_REUSE_BUCKETS, _CONN_REUSE_LAST)
    return _bucket_rows(
        _run(
            service,
            scope,
            "conn_reuse_dist",
            f"""
            SELECT {case} AS bucket, count() AS count, min(c) AS min_val
            FROM (
                SELECT toUInt64OrNull(custom_fields['conn_requests']) AS c
                FROM request_facts WHERE __WHERE__
            )
            WHERE c IS NOT NULL AND c > 0
            GROUP BY bucket ORDER BY min_val
            """,
        )
    )


def _ipv6_adoption(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "ipv6_adoption",
        f"""
        SELECT toUnixTimestamp(toStartOfHour(event_timestamp)) AS bucket_ts,
               countIf({_IS_IPV6}) * 100.0 / count() AS pct
        FROM request_facts WHERE __WHERE__
        GROUP BY bucket_ts ORDER BY bucket_ts
        """,
    )
    return [{"time": _sec_iso(r["bucket_ts"]), "pct": float(r["pct"])} for r in rows]


def _proxy_dist(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "proxy_dist",
        """
        SELECT custom_fields['p_type'] AS type, count() AS count
        FROM request_facts WHERE __WHERE__ AND custom_fields['p_type'] != ''
        GROUP BY type ORDER BY count DESC
        """,
    )
    return [{"type": r["type"], "count": int(r["count"])} for r in rows]


def _verified_bots_ts(service: HighScaleService, scope: _Scope, bucket_seconds: int) -> list[dict[str, Any]]:
    rows = _run(
        service,
        scope,
        "verified_bots_ts",
        f"""
        SELECT intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds} AS bucket_ts,
               replaceOne(tag, 'VERIFIED-BOT.', '') AS bot_type,
               count() AS count
        FROM request_facts
        ARRAY JOIN splitByChar(',', custom_fields['waf_sig']) AS tag
        WHERE __WHERE__
          AND positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0
          AND startsWith(tag, 'VERIFIED-BOT.')
        GROUP BY bucket_ts, bot_type ORDER BY bucket_ts, bot_type
        """,
    )
    return [{"time": _sec_iso(r["bucket_ts"]), "bot_type": r["bot_type"], "count": int(r["count"])} for r in rows]


_NGWAF_PROJECTION_WHERE = (
    f"service_id={{service_id:String}} AND {visible_publication_fence_sql('security_dimensions')} AND dimension='ngwaf_bot' "
    "AND bucket_start >= toStartOfMinute({start:DateTime64(3)}) AND bucket_start < {end:DateTime64(3)}"
)


def _ngwaf_summary(service: HighScaleService, scope: _Scope, limit: int | None = None) -> list[dict[str, Any]]:
    suffix = f" LIMIT {int(limit)}" if limit is not None else ""
    if scope.filtered:
        sql = f"""
            SELECT custom_fields['ngwaf_bot'] AS bot_name,
                   anyIf(custom_fields['ngwaf_wellknown_bot'], custom_fields['ngwaf_wellknown_bot'] != '') AS wk_name,
                   anyIf(custom_fields['ngwaf_bot_category'], custom_fields['ngwaf_bot_category'] != '') AS bot_cat,
                   count() AS request_count
            FROM request_facts WHERE __WHERE__ AND custom_fields['ngwaf_bot'] != ''
            GROUP BY bot_name ORDER BY request_count DESC{suffix}
            """
    else:
        sql = f"""
            SELECT value AS bot_name,
                   anyIf(wellknown_bot_name, wellknown_bot_name != '') AS wk_name,
                   anyIf(bot_category, bot_category != '') AS bot_cat,
                   sum(requests) AS request_count
            FROM security_minute_dimensions WHERE {_NGWAF_PROJECTION_WHERE}
            GROUP BY bot_name ORDER BY request_count DESC{suffix}
            """
    return _run(service, scope, "ngwaf_bots", sql)


def _ngwaf_verified_bots(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    return [
        {
            "bot_name": r["bot_name"],
            "wellknown_bot_name": r["wk_name"] or None,
            "category": r["bot_cat"] or None,
            "request_count": int(r["request_count"]),
        }
        for r in _ngwaf_summary(service, scope)
    ]


def _ngwaf_verified_bots_ts(service: HighScaleService, scope: _Scope, bucket_seconds: int) -> list[dict[str, Any]]:
    if scope.filtered:
        sql = f"""
            SELECT intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds} AS bucket_ts,
                   custom_fields['ngwaf_bot'] AS bot_name, count() AS count
            FROM request_facts WHERE __WHERE__ AND custom_fields['ngwaf_bot'] != ''
            GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name
            """
    else:
        sql = f"""
            SELECT intDiv(toUnixTimestamp(bucket_start), {bucket_seconds}) * {bucket_seconds} AS bucket_ts,
                   value AS bot_name, sum(requests) AS count
            FROM security_minute_dimensions WHERE {_NGWAF_PROJECTION_WHERE}
            GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name
            """
    rows = _run(service, scope, "ngwaf_verified_bots_ts", sql)
    return [{"time": _sec_iso(r["bucket_ts"]), "bot_name": r["bot_name"], "count": int(r["count"])} for r in rows]


def _ngwaf_configured(service: HighScaleService) -> bool:
    try:
        from backend import config as svcconfig

        return bool(svcconfig.get_ngwaf_workspace_id(service.service_id))
    except Exception:
        return False


def _wellknown_bots(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    from backend.utils.bot_sources import get_bot_regex_pattern

    pattern = get_bot_regex_pattern(500)
    prefilter = " AND match(custom_fields['ua'], {bot_pattern:String})" if pattern else ""
    rows = _run(
        service,
        scope,
        "wellknown_bots",
        f"""
        SELECT custom_fields['ua'] AS ua, client_ip AS ip, count() AS cnt
        FROM request_facts
        WHERE __WHERE__ AND custom_fields['ua'] != '' AND client_ip != ''{prefilter}
        GROUP BY ua, ip ORDER BY cnt DESC LIMIT 10000
        """,
        **({"bot_pattern": pattern} if pattern else {}),
    )
    return summarize_wellknown_bot_rows([(r["ua"], r["ip"], int(r["cnt"])) for r in rows])


def _bot_efficiency(service: HighScaleService, scope: _Scope) -> list[dict[str, Any]]:
    from backend.repositories.security import summarize_bot_efficiency_rows

    rows = _run(
        service,
        scope,
        "bot_efficiency",
        """
        WITH ua_all AS (
            SELECT
                custom_fields['ua'] AS key,
                count() AS cnt,
                ifNull(sum(toInt64OrZero(custom_fields['resp_bytes'])), 0) AS resp_bytes,
                sum(count()) OVER () AS site_cnt,
                ifNull(sum(sum(toInt64OrZero(custom_fields['resp_bytes']))) OVER (), 0) AS site_resp_bytes
            FROM request_facts
            WHERE __WHERE__
            GROUP BY key
        ),
        ua_agg AS (
            SELECT
                'ua' AS kind,
                key,
                cnt,
                resp_bytes,
                site_cnt,
                site_resp_bytes
            FROM ua_all
            WHERE key != ''
            ORDER BY resp_bytes DESC, cnt DESC
            LIMIT 2000
        ),
        ref_extracted AS (
            SELECT
                client_ip AS ip,
                if(
                    (
                        positionCaseInsensitive(custom_fields['ua'], '-user') > 0
                        OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0
                    ) AND extract(lower(custom_fields['ua']), '([a-z0-9]+)-(?:user|externalfetcher)') != '',
                    extract(lower(custom_fields['ua']), '([a-z0-9]+)-(?:user|externalfetcher)'),
                    extract(lower(custom_fields['referer']), 'https?://(?:www\\.)?([^/.:;\\s]+)')
                ) AS ref_stem
            FROM request_facts
            WHERE __WHERE__
              AND client_ip != ''
              AND (
                  (custom_fields['referer'] != '' AND custom_fields['referer'] != '-')
                  OR positionCaseInsensitive(custom_fields['ua'], '-user') > 0
                  OR positionCaseInsensitive(custom_fields['ua'], '-externalfetcher') > 0
              )
              AND NOT match(lower(custom_fields['ua']), '(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|headless)')
        ),
        ref_agg AS (
            SELECT
                'ref' AS kind,
                ref_stem AS key,
                uniqExact(ip) AS cnt,
                toInt64(0) AS resp_bytes,
                toUInt64(0) AS site_cnt,
                toInt64(0) AS site_resp_bytes
            FROM ref_extracted
            WHERE ref_stem != ''
            GROUP BY ref_stem
        )
        SELECT kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes FROM ua_agg
        UNION ALL
        SELECT kind, key, cnt, resp_bytes, site_cnt, site_resp_bytes FROM ref_agg
        """,
    )
    tagged: list[tuple[str, str, int, int]] = []
    has_site_row = any(str(r.get("kind") or "") == "site" for r in rows)
    if not has_site_row:
        for r in rows:
            if str(r.get("kind") or "") == "ua" and ("site_cnt" in r or "site_resp_bytes" in r):
                tagged.append(("site", "", int(r.get("site_cnt") or 0), int(r.get("site_resp_bytes") or 0)))
                break
    for r in rows:
        tagged.append((str(r["kind"] or ""), str(r["key"] or ""), int(r["cnt"] or 0), int(r["resp_bytes"] or 0)))
    return summarize_bot_efficiency_rows(tagged)


def _maybe_mask(ip: Any, mask_ips: bool) -> Any:
    if not mask_ips or not ip:
        return ip
    from backend.core.share_db.validation import mask_ip

    return mask_ip(str(ip))


def security_aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> SecurityAggregatesResponse:
    bucket_seconds = _bucket_seconds(req)
    memo_key = digest_cache_key(
        {
            "ep": "hs_security_aggregates",
            "cid": client_cache_token(service.client),
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
            "b": int(bucket_seconds),
            "sec": sorted(sections) if sections is not None else None,
            "m": bool(mask_ips),
        },
        {"name": service.service_id, "service_id": service.service_id},
    )
    cached = _security_memo.get_with_telemetry(memo_key)
    if cached is not None:
        return SecurityAggregatesResponse.with_telemetry(**cached)

    def _compute() -> dict[str, Any]:
        cached_inner = _security_memo.get(memo_key)
        if cached_inner is not None:
            return cached_inner

        scope = _scope(service, req, start_time, end_time)

        def want(name: str) -> bool:
            return sections is None or name in sections

        tasks: list[tuple[str, Callable[[], Any]]] = []
        if want("tls_fingerprints"):
            tasks.append(("tls_fingerprints", lambda: _tls_fingerprints(service, scope)))
        if want("fingerprint_coverage"):
            tasks.append(("fingerprint_coverage", lambda: _fingerprint_coverage(service, scope)))
        if want("req_size_dist"):
            tasks.append(("req_size_dist", lambda: _req_size_dist(service, scope)))
        if want("top_ips_header"):
            tasks.append(("top_ips_header", lambda: _top_ips_header(service, scope, mask_ips)))
        if want("ipv6_adoption"):
            tasks.append(("ipv6_adoption", lambda: _ipv6_adoption(service, scope)))
        if want("proxy_dist"):
            tasks.append(("proxy_dist", lambda: _proxy_dist(service, scope)))
        if want("conn_reuse_dist"):
            tasks.append(("conn_reuse_dist", lambda: _conn_reuse_dist(service, scope)))
        if want("verified_bots_ts"):
            tasks.append(("verified_bots_ts", lambda: _verified_bots_ts(service, scope, bucket_seconds)))
        if want("ngwaf_verified_bots") or want("ngwaf_verified_bots_ts"):
            tasks.append(("ngwaf_configured", lambda: _ngwaf_configured(service)))
            tasks.append(("ngwaf_verified_bots", lambda: _ngwaf_verified_bots(service, scope)))
            tasks.append(("ngwaf_verified_bots_ts", lambda: _ngwaf_verified_bots_ts(service, scope, bucket_seconds)))
        if want("wellknown_bots"):
            tasks.append(("wellknown_bots", lambda: _wellknown_bots(service, scope)))
        if want("bot_efficiency"):
            tasks.append(("bot_efficiency", lambda: _bot_efficiency(service, scope)))
        if want("top_bots"):
            tasks.append(("_top_bots", lambda: _top_bots(service, scope)))
        if want("proxies"):
            tasks.append(("_proxies", lambda: _proxies(service, scope, mask_ips)))

        trace = HighScaleSections(service.service_id, "security")

        def source_for(key: str) -> Literal["fact", "projection", "mixed", "unknown"]:
            if key == "ngwaf_configured":
                return "unknown"
            if key == "_top_bots" and not scope.filtered:
                return "mixed"
            return "projection" if key.startswith("ngwaf_") and not scope.filtered else "fact"

        res: dict[str, Any] = {}
        for k, val in trace.run_parallel(
            tasks,
            source=source_for,
            activity=lambda k: "overhead" if k == "ngwaf_configured" else "compute",
            max_workers=8,
            thread_name_prefix="hs-sec",
        ).items():
            if k == "_top_bots":
                res["bots"] = val["bots"]
                res["ngwaf_bots"] = val["ngwaf_bots"]
            elif k == "_proxies":
                res.update(val)
            else:
                res[k] = val
        res["section_timings"] = trace.timings()
        _security_memo.put(memo_key, dict(res))
        return res

    res = _security_memo.coalesce(f"hs_security_agg:{memo_key}", _compute)
    return SecurityAggregatesResponse.with_telemetry(**res)


def _top_bots(service: HighScaleService, scope: _Scope, n: int = 10) -> dict[str, list[dict[str, Any]]]:
    ua_rows = _run(
        service,
        scope,
        "top_bots_ua",
        """
        SELECT custom_fields['ua'] AS ua, count() AS cnt
        FROM request_facts WHERE __WHERE__ AND custom_fields['ua'] != ''
        GROUP BY ua ORDER BY cnt DESC LIMIT {ua_limit:UInt32}
        """,
        ua_limit=_BOT_UA_CLASSIFY_LIMIT,
    )
    bots = classify_ua_counts([(r["ua"], int(r["cnt"])) for r in ua_rows], n)
    ngwaf_bots = [
        {"name": r["bot_name"], "category": r["bot_cat"] or None, "request_count": int(r["request_count"])}
        for r in _ngwaf_summary(service, scope, limit=n)
    ]
    return {"bots": bots, "ngwaf_bots": ngwaf_bots}


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    scope = _scope(service, req, start_time, end_time)
    trace = HighScaleSections(service.service_id, "security")
    result = trace.run("top_bots", lambda: _top_bots(service, scope), source="fact" if scope.filtered else "mixed")
    return SecurityTopBotsResponse.with_telemetry(
        **cache_result(result, state="bypass", prewarmer=False), section_timings=trace.timings()
    )


_PROXY_SOURCE = f"""
    FROM (
        SELECT client_ip AS ip,
               custom_fields['pop'] AS pop,
               country,
               custom_fields['city'] AS city,
               toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min,
               toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt,
               toFloat64OrNull(custom_fields['lat']) AS lat,
               toFloat64OrNull(custom_fields['lon']) AS lon,
               toInt64OrNull(custom_fields['asn']) AS asn,
               {_POP_LAT} AS pop_lat,
               {_POP_LON} AS pop_lon
        FROM request_facts WHERE __WHERE__ AND custom_fields['rtt_min'] != ''
    )
"""


def _proxies(service: HighScaleService, scope: _Scope, mask_ips: bool) -> dict[str, Any]:
    def _q_stats():
        return _run(
            service,
            scope,
            "proxy_stats",
            f"""
            SELECT uniqExactIf(ip, {_IMPOSSIBLE_DISTANCE} OR {_TUNNEL}) AS active_proxies_count,
                   countIf({_IMPOSSIBLE_DISTANCE} OR {_TUNNEL}) AS total_requests_count,
                   uniqExactIf(ip, {_IMPOSSIBLE_DISTANCE}) AS distance_mismatches_count
            {_PROXY_SOURCE}
            """,
        )

    def _q_quality():
        return _run(
            service,
            scope,
            "traffic_quality",
            """
            SELECT multiIf(
                       rtt_min IS NULL OR tcp_rtt IS NULL, 'Direct Connection',
                       tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.1, 'Active Tunnel / Proxy',
                       tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.7, 'WiFi / Mobile',
                       'Direct Connection') AS type,
                   count() AS count
            FROM (
                SELECT toFloat64OrNull(custom_fields['rtt_min']) AS rtt_min,
                       toFloat64OrNull(custom_fields['tcp_rtt']) AS tcp_rtt
                FROM request_facts WHERE __WHERE__
            )
            GROUP BY type ORDER BY count DESC
            """,
        )

    def _q_isps():
        return _run(
            service,
            scope,
            "suspicious_isps",
            f""" SELECT asn, count() AS count {_PROXY_SOURCE} WHERE {_TUNNEL} GROUP BY asn ORDER BY count DESC LIMIT 10 """,
        )

    def _q_clients():
        return _run(
            service,
            scope,
            "active_clients",
            f"""
            SELECT ip, asn, rtt_min / 1000.0 AS rtt_min_ms, tcp_rtt / 1000.0 AS tcp_rtt_ms,
                   {_DISTANCE_KM} AS distance_km, pop,
                   lat AS client_lat, lon AS client_lon, pop_lat, pop_lon,
                   {_IMPOSSIBLE_DISTANCE} AS impossible_distance,
                   multiIf({_IMPOSSIBLE_DISTANCE}, 'High',
                           {_TUNNEL}, 'High',
                           ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0), 'Medium',
                           'Low') AS risk_level,
                   country, city
            {_PROXY_SOURCE}
            WHERE rtt_min IS NOT NULL
              AND ({_IMPOSSIBLE_DISTANCE} OR ifNull(tcp_rtt > 0 AND rtt_min / tcp_rtt < 0.2, 0))
            ORDER BY multiIf(risk_level = 'High', 0, risk_level = 'Medium', 1, 2), distance_km DESC LIMIT 100
            """,
        )

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="hs-proxies") as pool:
        f_stats = pool.submit(contextvars.copy_context().run, _q_stats)
        f_qual = pool.submit(contextvars.copy_context().run, _q_quality)
        f_isps = pool.submit(contextvars.copy_context().run, _q_isps)
        f_cli = pool.submit(contextvars.copy_context().run, _q_clients)
        stats = f_stats.result()
        quality_rows = f_qual.result()
        isp_rows = f_isps.result()
        client_rows = f_cli.result()

    stat = stats[0] if stats else {}
    quality_total = sum(int(r["count"]) for r in quality_rows)
    traffic_quality = (
        [{"label": str(r["type"]), "value": round(int(r["count"]) / quality_total * 100, 1)} for r in quality_rows]
        if quality_total > 0
        else []
    )

    from backend.core.duckdb import get_asn_names

    asns = {int(r["asn"]) for r in (*isp_rows, *client_rows) if r.get("asn") is not None}
    asn_names = get_asn_names(scope.service_id, sorted(asns))

    def _asn_name(asn: int | None) -> str:
        return asn_names.get(asn) or (f"AS{asn}" if asn else "Unknown ISP")

    suspicious_isps = []
    for r in isp_rows:
        asn = int(r["asn"]) if r.get("asn") is not None else None
        suspicious_isps.append({"isp": _asn_name(asn), "asn": asn, "count": int(r["count"])})

    active_clients = []
    for r in client_rows:
        asn = int(r["asn"]) if r.get("asn") is not None else None
        active_clients.append(
            {
                "ip": _maybe_mask(r["ip"], mask_ips),
                "rtt_min_ms": round(float(r["rtt_min_ms"]), 2),
                "tcp_rtt_ms": round(float(r["tcp_rtt_ms"]), 2) if r["tcp_rtt_ms"] is not None else None,
                "distance_km": round(float(r["distance_km"]), 1) if r["distance_km"] is not None else None,
                "pop": r["pop"] or None,
                "client_lat": round(float(r["client_lat"]), 4) if r["client_lat"] is not None else None,
                "client_lon": round(float(r["client_lon"]), 4) if r["client_lon"] is not None else None,
                "pop_lat": float(r["pop_lat"]),
                "pop_lon": float(r["pop_lon"]),
                "impossible_distance": bool(int(r["impossible_distance"])),
                "risk_level": r["risk_level"],
                "country": r["country"] or None,
                "city": r["city"] or None,
                "asn_name": _asn_name(asn),
            }
        )

    return {
        "active_proxies_count": int(stat.get("active_proxies_count", 0)),
        "tunnel_requests_count": int(stat.get("total_requests_count", 0)),
        "distance_mismatches_count": int(stat.get("distance_mismatches_count", 0)),
        "traffic_quality": traffic_quality,
        "suspicious_isps": suspicious_isps,
        "active_clients": active_clients,
    }


def get_proxies_data(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None, *, mask_ips: bool = False
) -> dict[str, Any]:
    scope = _scope(service, req, start_time, end_time)
    trace = HighScaleSections(service.service_id, "security")
    result = trace.run("proxies", lambda: _proxies(service, scope, mask_ips), source="fact")
    return {**result, "_section_timings": trace.timings()}


def get_security_threat_intel(
    service: HighScaleService, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    scope = _scope(service, None, start_time, end_time)
    rows = _run(
        service,
        scope,
        "threat_intel",
        f"""
        SELECT multiIf(custom_fields['ja3'] != '', custom_fields['ja3'], custom_fields['ja4']) AS tls_fingerprint,
               count() AS requests,
               topK(1)(country)[1] AS top_country,
               countIf(custom_fields['waf_logged_rules'] != '' OR custom_fields['waf_blocked_rules'] != '')
                   AS matched_waf_rules_count,
               round(countIf({_IS_BOT}) * 100.0 / count(), 2) AS bot_percentage,
               max(custom_fields['p_type'] != '') AS is_anonymized_proxy
        FROM request_facts
        WHERE __WHERE__ AND tls_fingerprint != ''
        GROUP BY tls_fingerprint ORDER BY requests DESC LIMIT 50
        """,
    )
    return {
        "data": [
            {
                "tls_fingerprint": r["tls_fingerprint"],
                "requests": int(r["requests"]),
                "top_country": r["top_country"] or "Unknown",
                "matched_waf_rules_count": int(r["matched_waf_rules_count"]),
                "bot_percentage": float(r["bot_percentage"] or 0.0),
                "is_anonymized_proxy": bool(int(r["is_anonymized_proxy"])),
            }
            for r in rows
        ]
    }


@dataclass(frozen=True)
class SecurityProjection:
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _SecurityAccumulator:
    requests: int = 0
    wellknown_bot_name: str = ""
    bot_category: str = ""
    verified_count: int = 0
    impersonator_count: int = 0
    unverified_count: int = 0


def build_security_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> SecurityProjection:
    dimensions: dict[tuple[datetime, str, str], _SecurityAccumulator] = defaultdict(_SecurityAccumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue

        ngwaf_bot = _text(row.get("_ngwaf_bot_name"))
        if ngwaf_bot:
            state = _text(row.get("ngwaf_bot_verification_state", "unverified"))
            acc = dimensions[(bucket, "ngwaf_bot", ngwaf_bot)]
            acc.requests += 1
            if state == "verified":
                acc.verified_count += 1
            elif state == "impersonator":
                acc.impersonator_count += 1
            else:
                acc.unverified_count += 1

            if not acc.wellknown_bot_name:
                acc.wellknown_bot_name = _text(row.get("_ngwaf_wellknown_bot_name"))
            if not acc.bot_category:
                acc.bot_category = _text(row.get("_ngwaf_bot_category"))

    return SecurityProjection(
        dimension_rows=tuple(
            _security_dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        )
    )


def _security_dimension_row(bucket: datetime, dimension: str, value: str, acc: _SecurityAccumulator) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "wellknown_bot_name": acc.wellknown_bot_name,
        "bot_category": acc.bot_category,
        "verified_count": acc.verified_count,
        "impersonator_count": acc.impersonator_count,
        "unverified_count": acc.unverified_count,
    }


_AGG_UNSET = object()


def aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None | object = _AGG_UNSET,
) -> Any:
    if sections is not _AGG_UNSET or hasattr(req, "pop_latency_limit") or not hasattr(req, "chart_interval"):
        return origin_aggregates(
            service,
            req,
            start_time,
            end_time,
            sections=None if sections is _AGG_UNSET else sections,  # type: ignore[arg-type]
        )
    return dashboard_aggregates(service, req, start_time, end_time)


_QUERY_SERVICE_ALIASES = (
    "query_router",
    "decoder",
    "query",
    "clickhouse",
    "insights",
    "sessions",
    "aggregate_query",
    "performance",
    "performance_projection",
    "cmcd",
    "dashboard",
    "origin",
    "origin_projection",
    "security",
)
for _alias_name in _QUERY_SERVICE_ALIASES:
    sys.modules[f"backend.high_scale.{_alias_name}"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    for _alias_name in _QUERY_SERVICE_ALIASES:
        setattr(_hs_pkg, _alias_name, _self_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    for _alias_name in _QUERY_SERVICE_ALIASES:
        _backend_pkg._MODULE_ALIASES[f"backend.high_scale.{_alias_name}"] = __name__
