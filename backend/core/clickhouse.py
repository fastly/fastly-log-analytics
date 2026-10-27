"""Bounded, synchronous ClickHouse HTTP boundary (no serving-path fallback).

``execute`` accepts TRUSTED internal SQL, including DDL, using ClickHouse typed
placeholders: ``SELECT ... WHERE service_id = {service_id:String}``. Parameters
are values only, never identifiers. Do not pass user-authored SQL here.
``insert_rows`` accepts only the internal identifiers below. The later schema
module imports/extends these constants; this module must never import schema.
"""

from __future__ import annotations

import json
import math
import re
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import cache, partial
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID, uuid4

import httpx
import structlog
from opentelemetry.metrics import Observation

from backend import config
from backend.core.request_telemetry import current_clickhouse_scope, get_meter, query_registry
from backend.utils.telemetry import _QUERIES, get_queries, record_call

logger = structlog.get_logger(__name__)

CLICKHOUSE_FACT_TABLE = "log_facts"
CLICKHOUSE_ALLOWED_TABLES = frozenset({CLICKHOUSE_FACT_TABLE})
CLICKHOUSE_FACT_COLUMNS = (
    "service_id",
    "batch_id",
    "source_identity",
    "row_ordinal",
    "generation",
    "timestamp",
    "country",
    "ip",
    "url",
    "conn_requests",
)
CLICKHOUSE_HIGH_SCALE_TABLES = frozenset(
    {
        "high_scale_batch_publications",
        "request_facts",
        "request_aggregates",
        "rum_vitals_facts",
        "rum_error_facts",
        "cmcd_projection_facts",
        "rum_vitals_aggregates",
        "rum_error_aggregates",
        "cmcd_aggregates",
        "origin_minute_summary",
        "origin_minute_dimensions",
        "performance_minute_dimensions",
        "performance_minute_summary",
        "performance_scatter_samples",
        "security_minute_dimensions",
        "network_minute_dimensions",
    }
)
CLICKHOUSE_HIGH_SCALE_COLUMNS = frozenset(
    {
        "batch_id",
        "service_id",
        "domain",
        "generation",
        "batch_digest",
        "expected_rows",
        "visible_rows",
        "quorum_acked",
        "publication_state",
        "manifest_version",
        "updated_at",
        "event_id",
        "event_timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "country",
        "client_ip",
        "url",
        "custom_fields",
        "cmcd",
        "client_id",
        "request_event_id",
        "metric_name",
        "metric_value",
        "metric_rating",
        "pathname",
        "error_message",
        "error_file",
        "projection_key",
        "request_count",
        "event_count",
        "error_count",
        "session_count",
        "value_sum",
        "bucket_start",
        "dimension",
        "value",
        "requests",
        "misses",
        "passes",
        "origin_5xx",
        "status_count",
        "origin_bytes",
        "latency_count",
        "ttlb_count",
        "overhead_count",
        "origin_bytes_count",
        "latency_p50_us",
        "latency_p75_us",
        "latency_p95_us",
        "latency_p99_us",
        "ttlb_p50_us",
        "ttlb_p95_us",
        "cdn_overhead_p50_us",
        "origin_bytes_p50",
        "latency_sk_v",
        "latency_sk_w",
        "ttlb_sk_v",
        "ttlb_sk_w",
        "overhead_sk_v",
        "overhead_sk_w",
        "origin_bytes_sk_v",
        "origin_bytes_sk_w",
        "asn",
        "browser",
        "os",
        "device",
        "city",
        "region",
        "pop",
        "tls",
        "ttfb",
        "error_line",
        "error_col",
        "latency_sum_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "wellknown_bot_name",
        "bot_category",
        "verified_count",
        "impersonator_count",
        "unverified_count",
        "c_speed",
        "errors",
        "tcp_rtt_count",
        "tcp_rtt_sum",
        "tcp_rtt_p50_us",
        "tcp_rtt_p95_us",
        "tcp_rtt_p99_us",
        "ttfb_p50_us",
        "ttfb_p95_us",
        "ploss_sum",
        "ploss_count",
        "component_count",
        "edge_processing_sum_ms",
        "origin_wait_sum_ms",
        "origin_download_sum_ms",
        "client_download_sum_ms",
        "sample_key",
        "origin_ms",
        "edge_ms",
        "cache",
    }
)
CLICKHOUSE_ALLOWED_TABLES = frozenset({CLICKHOUSE_FACT_TABLE}) | CLICKHOUSE_HIGH_SCALE_TABLES
CLICKHOUSE_TABLE_COLUMNS = {
    CLICKHOUSE_FACT_TABLE: frozenset(CLICKHOUSE_FACT_COLUMNS),
    **{table: CLICKHOUSE_HIGH_SCALE_COLUMNS for table in CLICKHOUSE_HIGH_SCALE_TABLES},
}
_PARAM_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DDL_PASSWORD = re.compile(r"(\bIDENTIFIED\s+WITH\s+\w+\s+BY\s+)'(?:[^'\\]|\\.|'')*'", re.IGNORECASE)


def _redact_ddl_secrets(sql: str) -> str:
    """Debug telemetry keeps SQL text; user DDL carries the password, which must never be recorded."""
    return _DDL_PASSWORD.sub(r"\1'[REDACTED]'", sql)


# Per-query settings callers may add. They travel as HTTP settings (never as SQL text, where a user
# comment could swallow them) and may only narrow what a query can read or return, never widen it.
_QUERY_SETTINGS = frozenset(
    {
        "additional_table_filters",
        "date_time_input_format",
        "max_result_rows",
        "result_overflow_mode",
        "max_rows_to_read",
        "read_overflow_mode",
    }
)
_PARAM_ESCAPES = {
    ord("\\"): "\\\\",
    ord("\n"): "\\n",
    ord("\t"): "\\t",
    ord("\r"): "\\r",
    ord("\0"): "\\0",
    ord("\b"): "\\b",
    ord("\f"): "\\f",
}


class ClickHouseError(RuntimeError):
    """Sanitized failure; insert timeouts can mean an ambiguously committed write."""

    def __init__(self, reason: str, *, query_id: str, status_code: int | None = None):
        self.query_id = query_id
        self.status_code = status_code
        self.reason = reason
        suffix = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"ClickHouse {reason}{suffix}; query_id={query_id}")


def _date_value(value: datetime | date) -> str:
    if isinstance(value, datetime):
        # Naive datetimes are interpreted as UTC, matching the fact timestamp.
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return value.isoformat()


def _json_value(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return _date_value(value)
    if isinstance(value, (UUID, IPv4Address, IPv6Address, Decimal)):
        return str(value)
    raise ValueError("unsupported ClickHouse insert value type")


def _parameter(value: Any) -> str:
    """HTTP param_* values use ClickHouse's escaped-text scalar representation."""
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (datetime, date)):
        return _date_value(value)
    if isinstance(value, (int, float, Decimal)):
        if not math.isfinite(value):
            raise ValueError("ClickHouse parameters must be finite")
        return str(value)
    if isinstance(value, (str, UUID, IPv4Address, IPv6Address)):
        return str(value).translate(_PARAM_ESCAPES)
    raise ValueError("unsupported ClickHouse parameter type; use typed scalar parameters")


def _server_elapsed_ms(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        elapsed = float(value) * 1000
    except OverflowError:
        return None
    return elapsed if math.isfinite(elapsed) and elapsed >= 0 else None


def _record_operation(**stats: Any) -> None:
    logger.info("clickhouse.operation", **stats)
    record_call(
        "POST",
        f"clickhouse/{stats['operation']}",
        stats["duration_ms"],
        status=stats["status_code"] or stats["outcome"],
        service="ClickHouse",
        details=json.dumps(stats),
        bytes_count=stats["response_bytes"],
    )


def _safe_count(value: Any) -> int:
    try:
        return max(0, int(value))
    except (ValueError, TypeError, OverflowError):
        return 0


class ClickHouseClient:
    """One reusable HTTP pool; bounded admission also applies to injected transports."""

    def __init__(self, settings: config.ClickHouseConfig, *, transport: httpx.BaseTransport | None = None):
        self._settings = settings
        pool_max_size = int(getattr(settings, "pool_max_size", 24))
        pool_timeout_s = float(getattr(settings, "pool_timeout_s", 30.0))
        self._slots = threading.BoundedSemaphore(pool_max_size)
        self._state_lock = threading.Lock()
        self._closed = False
        self._active = 0
        self._http = httpx.Client(
            base_url=httpx.URL(
                scheme="https" if settings.secure else "http",
                host=settings.host,
                port=settings.port,
            ),
            auth=httpx.BasicAuth(settings.user, settings.password),
            limits=httpx.Limits(max_connections=pool_max_size, max_keepalive_connections=pool_max_size),
            timeout=httpx.Timeout(settings.query_timeout_s, connect=settings.connect_timeout_s, pool=pool_timeout_s),
            transport=transport,
            follow_redirects=False,
            trust_env=False,
        )

    def execute(self, sql: str, params: dict | None = None, *, settings: Mapping[str, str] | None = None) -> list[dict]:
        """Run internal SQL. SELECT-like statements return JSON rows; DDL returns [].

        ``settings`` adds allow-listed per-query ClickHouse settings (see ``_QUERY_SETTINGS``).
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("ClickHouse SQL must be nonempty")
        for name, value in (settings or {}).items():
            if name not in _QUERY_SETTINGS:
                raise ValueError(f"ClickHouse query setting {name!r} is not allowed")
            if not isinstance(value, str) or not value:
                raise ValueError(f"ClickHouse query setting {name!r} needs a nonempty string value")
        if re.search(r"\{[^{}]+:\s*Identifier\s*\}", sql, re.IGNORECASE):
            raise ValueError("ClickHouse identifier parameters are forbidden")
        form = {"query": sql}
        for name, value in (params or {}).items():
            if not isinstance(name, str) or not _PARAM_NAME.fullmatch(name):
                raise ValueError("invalid ClickHouse parameter name")
            form[f"param_{name}"] = _parameter(value)
        return self._request("execute", data=form, settings=settings)

    def insert_rows(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        """Insert a caller-bounded batch synchronously, without any automatic retry."""
        if table not in CLICKHOUSE_ALLOWED_TABLES:
            raise ValueError("ClickHouse table is not internally allowlisted")
        if (
            not columns
            or any(c not in CLICKHOUSE_TABLE_COLUMNS[table] for c in columns)
            or len(set(columns)) != len(columns)
        ):
            raise ValueError("ClickHouse columns must be unique internal identifiers")
        if any(len(row) != len(columns) for row in rows):
            raise ValueError("ClickHouse row width does not match columns")
        if not rows:
            return
        try:
            content = "".join(
                json.dumps(row, default=_json_value, allow_nan=False, ensure_ascii=False) + "\n" for row in rows
            ).encode("utf-8")
        except (ValueError, TypeError, UnicodeError):
            raise ValueError("invalid ClickHouse insert value") from None
        quoted_columns = ", ".join(f"`{c}`" for c in columns)
        sql = f"INSERT INTO `{table}` ({quoted_columns}) FORMAT JSONCompactEachRow"
        self._request("insert", sql=sql, content=content, rows_written=len(rows))

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        """Synchronously remove a failed high-scale batch before a retry."""
        if table not in CLICKHOUSE_HIGH_SCALE_TABLES:
            raise ValueError("ClickHouse table is not internally allowlisted")
        self.execute(
            f"ALTER TABLE `{table}` DELETE WHERE batch_id={{batch_id:UUID}} SETTINGS mutations_sync=2",
            {"batch_id": batch_id},
        )

    def health(self) -> dict:
        """Exercise authenticated database access; unavailable is an explicit exception."""
        started = time.perf_counter()
        self._request("health", data={"query": "SELECT 1 AS ok"})
        return {"ok": True, "duration_ms": (time.perf_counter() - started) * 1000}

    def storage_health(self) -> dict:
        """Cheap authenticated probe; no fact scan or readiness proof."""
        rows = self._request(
            "observe",
            data={"query": "SELECT free_space, total_space FROM system.disks WHERE name = 'default'"},
        )
        if len(rows) != 1:
            raise ValueError("ClickHouse default disk missing")
        free, total = int(rows[0]["free_space"]), int(rows[0]["total_space"])
        if not 0 <= free <= total or total <= 0:
            raise ValueError("ClickHouse disk capacity invalid")
        return {"up": 1, "disk_free": free, "disk_total": total}

    def close(self) -> None:
        """Stop admission immediately; the last active request closes its own pool."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            if not self._active:
                self._close_http()

    def _close_http(self) -> None:
        try:
            self._http.close()
        except httpx.HTTPError:
            raise ClickHouseError("close error", query_id=uuid4().hex) from None

    def _request(
        self,
        operation: str,
        *,
        data: dict | None = None,
        sql: str | None = None,
        content: bytes | None = None,
        rows_written: int = 0,
        settings: Mapping[str, str] | None = None,
    ) -> list[dict]:
        started = time.perf_counter()
        query_id = uuid4().hex
        registry_id = -1
        acquired = False
        active = False
        stats: dict[str, Any] = {
            **current_clickhouse_scope(),
            "admission_wait_ms": None,
            "network_ms": None,
            "decode_ms": None,
            "server_cpu_ms": None,
            "server_elapsed_ms": None,
            "server_elapsed_source": None,
            "timing_kind": "client_operation_wall",
            "operation": operation,
            "query_id": query_id,
            "outcome": "error",
            "status_code": None,
            "rows_read": 0,
            "bytes_read": 0,
            "rows_written": 0,
            "response_bytes": 0,
        }
        try:
            if operation in {"execute", "insert"}:
                try:
                    # No client/connection handle: shared HTTP pools cannot be
                    # safely interrupted. Avoid SQL values in the monitor too.
                    registry_id = query_registry.register(
                        "ClickHouse",
                        f"ClickHouse {operation}; query_id={query_id}",
                        service_id=stats["service_id"],
                        con=None,
                    )
                except Exception:
                    logger.debug("_request: query_registry.register failed (ignored)", exc_info=True)
            with self._state_lock:
                if self._closed:
                    raise ClickHouseError("client closed", query_id=query_id)
            admission_started = time.perf_counter()
            try:
                acquired = self._slots.acquire(timeout=self._settings.pool_timeout_s)
            finally:
                stats["admission_wait_ms"] = (time.perf_counter() - admission_started) * 1000
            if not acquired:
                raise ClickHouseError("pool acquisition timeout", query_id=query_id)
            with self._state_lock:
                if self._closed:
                    raise ClickHouseError("client closed", query_id=query_id)
                self._active += 1
                active = True
            timeout = self._settings.insert_timeout_s if operation == "insert" else self._settings.query_timeout_s
            query = {
                "database": self._settings.database,
                "query_id": query_id,
                "max_execution_time": str(timeout),
                "timeout_overflow_mode": "throw",
                "timeout_before_checking_execution_speed": "0",
                "wait_end_of_query": "1",
                "default_format": "JSON",
                "output_format_json_quote_64bit_integers": "0",
                "max_memory_usage": "1073741824",
                "max_threads": "2",
                "memory_overcommit_ratio_denominator": "0",
                "memory_overcommit_ratio_denominator_for_user": "0",
                "max_bytes_before_external_group_by": "134217728",
                "max_bytes_before_external_sort": "134217728",
            }
            if settings:
                query.update(settings)
            if operation == "insert":
                query.update(
                    {
                        "query": sql or "",
                        "async_insert": "0",
                        "date_time_input_format": "best_effort",
                        "max_memory_usage": "536870912",
                        "max_threads": "2",
                        "max_insert_threads": "1",
                        "max_block_size": "2048",
                    }
                )
            network_started = time.perf_counter()
            try:
                response = self._http.post(
                    "/",
                    params=query,
                    # ClickHouse parses POST query parameters as multipart forms,
                    # not URL-encoded SQL bodies. Keep values out of URLs/logs.
                    files={key: (None, value) for key, value in data.items()} if data else None,
                    content=content,
                    timeout=httpx.Timeout(
                        timeout, connect=self._settings.connect_timeout_s, pool=self._settings.pool_timeout_s
                    ),
                )
            finally:
                stats["network_ms"] = (time.perf_counter() - network_started) * 1000
            stats["status_code"] = response.status_code
            stats["response_bytes"] = len(response.content)
            if not response.is_success or response.headers.get("X-ClickHouse-Exception-Code", "0") != "0":
                raise ClickHouseError("server error", query_id=query_id, status_code=response.status_code)
            decode_started = time.perf_counter()
            try:
                result = []
                if response.content.strip():
                    payload = response.json()
                    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                        raise ValueError("invalid response shape")
                    result = payload["data"]
                    if not all(isinstance(row, dict) for row in result):
                        raise ValueError("invalid row shape")
                    summary = payload.get("statistics") or {}
                    if isinstance(summary, dict):
                        stats["rows_read"] = _safe_count(summary.get("rows_read"))
                        stats["bytes_read"] = _safe_count(summary.get("bytes_read"))
                        stats["server_elapsed_ms"] = _server_elapsed_ms(summary.get("elapsed"))
                        if stats["server_elapsed_ms"] is not None:
                            stats["server_elapsed_source"] = "statistics.elapsed"
                elif data and re.match(
                    r"\s*(SELECT|WITH|SHOW|DESCRIBE|EXPLAIN|EXISTS)\b", data["query"], re.IGNORECASE
                ):
                    raise ValueError("missing query response")
                if operation == "health" and result != [{"ok": 1}]:
                    raise ValueError("invalid health response")
            finally:
                stats["decode_ms"] = (time.perf_counter() - decode_started) * 1000
            summary_header = response.headers.get("X-ClickHouse-Summary")
            if summary_header:
                # Statistics are observational: malformed metadata cannot break a write.
                try:
                    summary = json.loads(summary_header)
                    stats["rows_read"] = _safe_count(summary.get("read_rows"))
                    stats["bytes_read"] = _safe_count(summary.get("read_bytes"))
                except (ValueError, AttributeError):
                    pass
            stats.update(outcome="success", rows_written=rows_written)
            return result
        except ClickHouseError as exc:
            if "timeout" in exc.reason:
                stats["outcome"] = "timeout"
            raise
        except httpx.TimeoutException:
            stats["outcome"] = "timeout"
            raise ClickHouseError("timeout", query_id=query_id) from None
        except httpx.HTTPError:
            raise ClickHouseError("transport error", query_id=query_id) from None
        except (ValueError, UnicodeError):
            raise ClickHouseError("invalid response", query_id=query_id, status_code=stats["status_code"]) from None
        finally:
            try:
                if active:
                    with self._state_lock:
                        self._active -= 1
                        if self._closed and not self._active:
                            self._close_http()
            finally:
                if acquired:
                    self._slots.release()
                stats["duration_ms"] = (time.perf_counter() - started) * 1000
                stats["client_elapsed_ms"] = stats["duration_ms"]
                if operation == "execute" and data and "query" in data:
                    try:
                        redacted_sql = _redact_ddl_secrets(data["query"].strip())
                        dur_ms = round(stats["duration_ms"], 2)
                        queries = get_queries()
                        queries.append(
                            {
                                "sql": redacted_sql,
                                "time_ms": dur_ms,
                                "engine": "ClickHouse",
                                "clickhouse": {
                                    key: stats[key]
                                    for key in (
                                        "query_id",
                                        "request_id",
                                        "page_load_id",
                                        "service_id",
                                        "component",
                                        "section",
                                        "section_id",
                                        "parent_section_id",
                                        "source",
                                        "outcome",
                                        "admission_wait_ms",
                                        "network_ms",
                                        "decode_ms",
                                        "client_elapsed_ms",
                                        "server_cpu_ms",
                                        "server_elapsed_ms",
                                        "server_elapsed_source",
                                        "timing_kind",
                                    )
                                },
                            }
                        )
                        _QUERIES.set(queries)
                        from backend.utils.telemetry import record_telemetry_query

                        record_telemetry_query(
                            "clickhouse",
                            redacted_sql,
                            dur_ms,
                            service_id=stats["service_id"],
                            request_id=stats["request_id"],
                            allow_active_service_fallback=False,
                        )
                    except Exception:
                        logger.debug("_request: _QUERIES.set failed (ignored)", exc_info=True)
                try:
                    _record_operation(**stats)
                except Exception:
                    logger.debug("_request: _record_operation failed (ignored)", exc_info=True)
                try:
                    record_operation(stats)
                except Exception:
                    logger.debug("_request: record_operation failed (ignored)", exc_info=True)
                try:
                    query_registry.deregister(
                        registry_id,
                        error=None if stats["outcome"] == "success" else RuntimeError(stats["outcome"]),
                    )
                except Exception:
                    logger.debug("_request: query_registry.deregister failed (ignored)", exc_info=True)


_client: ClickHouseClient | None = None
_client_lock = threading.Lock()


def get_clickhouse_client() -> ClickHouseClient | None:
    """Lazy process singleton; disabled means no client and no socket."""
    global _client
    with _client_lock:
        settings = config.load_clickhouse_config()
        if settings is None:
            return None
        if _client is None:
            _client = ClickHouseClient(settings)
        return _client


def close_clickhouse_client() -> None:
    global _client
    with _client_lock:
        client, _client = _client, None
    if client is not None:
        client.close()


# ── ClickHouse OTel Observer & Metrics (formerly backend.core.clickhouse_observer) ──


@cache
def _instrument(name: str, kind: str, unit: str = ""):
    meter = get_meter()
    create = meter.create_histogram if kind == "histogram" else meter.create_counter
    return create(f"app.clickhouse_{name}", unit=unit)


def record_operation(stats: dict) -> None:
    if stats["operation"] not in {"execute", "insert"}:
        return
    try:
        insert = stats["operation"] == "insert"
        _instrument("insert_duration_ms" if insert else "query_duration_ms", "histogram", "ms").record(
            stats["duration_ms"], {"outcome": stats["outcome"]}
        )
        for phase in ("admission_wait_ms", "network_ms", "decode_ms", "server_elapsed_ms"):
            value = stats.get(phase)
            if value is not None:
                _instrument(phase, "histogram", "ms").record(
                    value, {"operation": stats["operation"], "outcome": stats["outcome"]}
                )
        if not insert:
            _instrument("queries_total", "counter").add(1, {"outcome": stats["outcome"]})
        elif stats["outcome"] == "success":
            # Physical acknowledged rows, including identical retry copies.
            _instrument("rows_inserted_total", "counter").add(stats["rows_written"])
        _instrument("bytes_read", "counter", "By").add(stats["bytes_read"])
    except Exception:
        logger.debug("record_operation: _instrument failed (ignored)", exc_info=True)


def record_publication(outcome: str) -> None:
    try:
        _instrument("publications_total", "counter").add(1, {"outcome": outcome})
    except Exception:
        logger.debug("record_publication: _instrument failed (ignored)", exc_info=True)


class ClickHouseObserver:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._client: ClickHouseClient | None = None
        self._registered = False
        self._sampled_at = 0.0
        self._values: dict[str, float] = {}

    def start(self, client: ClickHouseClient) -> None:
        with self._lock:
            if self._client is not None:
                client.close()
                return
            self._client = client
            self._sampled_at = 0
            if not self._registered:
                meter = get_meter()
                for key, name, unit in (
                    ("up", "up", ""),
                    ("disk_free", "disk_free", "By"),
                    ("disk_total", "disk_total", "By"),
                    ("lag", "publication_lag_seconds", "s"),
                ):
                    meter.create_observable_gauge(
                        f"app.clickhouse_{name}",
                        callbacks=[partial(self.observe, key)],
                        unit=unit,
                    )
                self._registered = True

    def observe(self, key: str, options=None) -> list[Observation]:
        # All four callbacks run on the exporter, never the serving path.
        with self._lock:
            if self._client is None:
                return []
            if time.monotonic() - self._sampled_at >= 10:
                values: dict[str, float] = {"up": 0}
                try:
                    values.update(self._client.storage_health())
                except Exception as exc:
                    self._log_error("storage", exc)
                try:
                    from backend.core.clickhouse import PgManifest as _PgManifest

                    manifest_cls = globals().get("PgManifest", _PgManifest)
                    values["lag"] = manifest_cls().publication_lag_seconds()
                except Exception as exc:
                    self._log_error("manifest", exc)
                self._values = values
                self._sampled_at = time.monotonic()
            value = self._values.get(key)
            return [] if value is None else [Observation(value)]

    @staticmethod
    def _log_error(probe: str, exc: Exception) -> None:
        try:
            logger.warning("clickhouse.observe_failed", probe=probe, error_kind=type(exc).__name__)
        except Exception:
            logger.debug("_log_error: logger.warning failed (ignored)", exc_info=True)

    def stop(self) -> None:
        with self._lock:
            client, self._client = self._client, None
            self._values = {}
        if client is not None:
            client.close()


_observer = ClickHouseObserver()


def start_clickhouse_observer() -> None:
    try:
        settings = config.load_clickhouse_config()
        if settings is None:
            return
        client_cls = globals().get("ClickHouseClient", ClickHouseClient)
        # Do not contend with serving admission or inherit long query timeouts.
        client = client_cls(
            replace(
                settings,
                pool_max_size=1,
                connect_timeout_s=2,
                query_timeout_s=2,
                pool_timeout_s=2,
            )
        )
        _observer.start(client)
    except Exception as exc:
        stop_clickhouse_observer()
        _observer._log_error("startup", exc)


def stop_clickhouse_observer() -> None:
    try:
        _observer.stop()
    except Exception as exc:
        _observer._log_error("shutdown", exc)


_this_module = sys.modules[__name__]
sys.modules["backend.core.clickhouse_observer"] = _this_module
sys.modules["backend.core.clickhouse_metrics"] = _this_module
_core_pkg = sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "clickhouse_observer", _this_module)
    setattr(_core_pkg, "clickhouse_metrics", _this_module)


"""Transactional Postgres manifest; never use the SQLite-shaped commit shim."""


from contextlib import contextmanager
from dataclasses import dataclass
from typing import cast

import structlog
from psycopg import Connection
from psycopg_pool import ConnectionPool

from backend.core.metadata.base import get_pg_pool

logger = structlog.get_logger(__name__)


from backend.core.metadata import pg_connection

CLICKHOUSE_SCHEMA_VERSION = 2


def require_postgres() -> None:
    if not pg_connection.is_postgres():
        raise RuntimeError("ClickHouse prototype requires Postgres METADATA_DSN; SQLite is unsupported")


@dataclass(frozen=True)
class Dataset:
    service_id: str
    dataset_id: str
    catalog_identity: str
    source_table: str
    source_snapshot: int
    coverage_start: datetime
    coverage_end: datetime
    expires_at: datetime
    expected_rows: int
    canonical_digest: str
    schema_version: int = CLICKHOUSE_SCHEMA_VERSION


@dataclass(frozen=True)
class Artifact:
    service_id: str
    dataset_id: str
    batch_id: str
    artifact_uri: str
    content_hash: str
    canonical_digest: str
    byte_size: int
    ordinal_start: int
    row_count: int


def _construct(cls, row):
    return cls(**{name: row[name] for name in cls.__dataclass_fields__})


class PgManifest:
    def __init__(self, *, bounded: bool = False):
        require_postgres()
        self.pool = cast(ConnectionPool[Connection[dict[str, Any]]], get_pg_pool())
        self.bounded = bounded

    @contextmanager
    def transaction(self):
        started = time.monotonic()
        outcome = "error"
        try:
            connection = self.pool.connection(timeout=2) if self.bounded else self.pool.connection()
            with connection as con, con.transaction():
                if self.bounded:
                    con.execute("SET LOCAL statement_timeout = '2s'")
                    con.execute("SET LOCAL lock_timeout = '2s'")
                yield con
            outcome = "success"
        finally:
            logger.info("clickhouse.manifest", outcome=outcome, duration_ms=(time.monotonic() - started) * 1000)

    def admin_status(self, service_id: str, target: str) -> dict:
        """Service-scoped metadata only; counts include expired/retired history.

        Pending age is actionable backlog only. An expired selected dataset
        remains visible, but is never described as ready to serve.
        """
        with self.pool.connection(timeout=2) as con, con.transaction():
            con.execute("SET TRANSACTION READ ONLY")
            con.execute("SET LOCAL statement_timeout = '2s'")
            schema = con.execute(
                "SELECT schema_version FROM clickhouse_schema_state WHERE target_identity=%s", (target,)
            ).fetchone()
            counts = con.execute(
                "SELECT status,count(*) AS n FROM clickhouse_publications WHERE service_id=%s GROUP BY status",
                (service_id,),
            ).fetchall()
            freshness = con.execute(
                "SELECT MAX(p.published_at) AS last_published_at,"
                "MAX(EXTRACT(EPOCH FROM (clock_timestamp()-p.created_at))) FILTER "
                "(WHERE p.status IN ('pending','claimed','failed') AND g.status='building' "
                "AND d.expires_at>clock_timestamp() AND s.revision=g.selection_revision) AS pending_age "
                "FROM clickhouse_publications p JOIN clickhouse_generations g USING(service_id,generation,dataset_id) "
                "JOIN clickhouse_datasets d USING(service_id,dataset_id) "
                "LEFT JOIN clickhouse_service_selection s ON s.service_id=p.service_id WHERE p.service_id=%s",
                (service_id,),
            ).fetchone()
            active = con.execute(
                "SELECT g.generation,g.dataset_id,g.target_identity,d.coverage_start,d.coverage_end,d.expires_at,"
                "d.expires_at<=clock_timestamp() AS expired,"
                "GREATEST(0,EXTRACT(EPOCH FROM (clock_timestamp()-d.coverage_end))) AS coverage_age_seconds "
                "FROM clickhouse_service_selection s JOIN clickhouse_generations g USING(service_id,generation) "
                "JOIN clickhouse_datasets d USING(service_id,dataset_id) WHERE s.service_id=%s AND g.status='active'",
                (service_id,),
            ).fetchone()
            expired = con.execute(
                "SELECT count(*) AS n FROM clickhouse_datasets WHERE service_id=%s AND expires_at<=clock_timestamp()",
                (service_id,),
            ).fetchone()
        if freshness is None or expired is None:
            raise RuntimeError("freshness / expired is unexpectedly None")
        active_response = None
        if active:
            active_response = {
                "generation": active["generation"],
                "dataset_id": active["dataset_id"],
                "coverage_start": active["coverage_start"].isoformat(),
                "coverage_end": active["coverage_end"].isoformat(),
                "expires_at": active["expires_at"].isoformat(),
                "expired": active["expired"],
                "target_matches": active["target_identity"] == target,
                "coverage_age_seconds": float(active["coverage_age_seconds"]),
            }
        return {
            "schema_version": schema["schema_version"] if schema else None,
            "publication_counts": {
                **dict.fromkeys(("pending", "claimed", "failed", "published"), 0),
                **{row["status"]: row["n"] for row in counts},
            },
            "oldest_pending_age_seconds": (
                max(0.0, float(freshness["pending_age"])) if freshness["pending_age"] is not None else None
            ),
            "last_published_at": (
                freshness["last_published_at"].isoformat() if freshness["last_published_at"] else None
            ),
            "active_generation": active_response,
            "expired_dataset_count": expired["n"],
        }

    def replay_preview(
        self, service_id: str, *, dataset_id: str | None, generation: str | None, target: str, limit: int
    ) -> dict:
        """Read-only reference validation, also bounding final activation work.

        Activation verifies the entire dataset, not just this request's pending
        slice. Refuse datasets above the HTTP cap before allocating a generation.
        """
        if not 1 <= limit <= 100:
            raise ValueError("HTTP replay limit exceeded")
        with self.pool.connection(timeout=2) as con, con.transaction():
            con.execute("SET TRANSACTION READ ONLY")
            con.execute("SET LOCAL statement_timeout = '2s'")
            if generation:
                gen = con.execute(
                    "SELECT g.dataset_id,g.status,g.target_identity,"
                    "g.selection_revision=s.revision AS current_revision "
                    "FROM clickhouse_generations g JOIN clickhouse_service_selection s USING(service_id) "
                    "WHERE g.service_id=%s AND g.generation=%s",
                    (service_id, generation),
                ).fetchone()
                if gen is None:
                    raise LookupError("reference not found")
                if gen["status"] != "building" or not gen["current_revision"] or gen["target_identity"] != target:
                    raise ValueError("generation cannot resume")
                dataset_id = gen["dataset_id"]
            dataset = con.execute(
                "SELECT expires_at,schema_version FROM clickhouse_datasets WHERE service_id=%s AND dataset_id=%s",
                (service_id, dataset_id),
            ).fetchone()
            if dataset is None:
                raise LookupError("reference not found")
            if dataset["expires_at"] <= datetime.now(UTC) or dataset["schema_version"] != CLICKHOUSE_SCHEMA_VERSION:
                raise ValueError("dataset expired or unsupported")
            count_row = con.execute(
                "SELECT count(*) AS n FROM "
                "(SELECT 1 FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s LIMIT 101) a",
                (service_id, dataset_id),
            ).fetchone()
            if count_row is None:
                raise RuntimeError("count_row is unexpectedly None")
            count = count_row["n"]
            if count > 100:
                raise ValueError("dataset exceeds HTTP replay cap")
            pending = count
            if generation:
                pending_row = con.execute(
                    "SELECT count(*) AS n FROM clickhouse_publications "
                    "WHERE service_id=%s AND generation=%s AND status!='published'",
                    (service_id, generation),
                ).fetchone()
                if pending_row is None:
                    raise RuntimeError("pending_row is unexpectedly None")
                pending = pending_row["n"]
        return {"dataset_id": dataset_id, "artifact_count": count, "planned_artifacts": min(pending, limit)}

    def publication_lag_seconds(self) -> float:
        """Oldest actionable backlog age, not the age of retired/expired work.

        NULL after a successful aggregate means no backlog. Database errors
        propagate so observers omit unknown lag instead of reporting zero.
        """
        with self.pool.connection(timeout=2) as con, con.transaction():
            con.execute("SET LOCAL statement_timeout = '2s'")
            row = con.execute(
                "SELECT MAX(EXTRACT(EPOCH FROM (clock_timestamp()-p.created_at))) AS lag_seconds "
                "FROM clickhouse_publications p "
                "JOIN clickhouse_generations g ON g.service_id=p.service_id AND g.generation=p.generation "
                "JOIN clickhouse_datasets d ON d.service_id=g.service_id AND d.dataset_id=g.dataset_id "
                "JOIN clickhouse_service_selection s ON s.service_id=g.service_id "
                "AND s.revision=g.selection_revision "
                "WHERE p.status IN ('pending', 'claimed', 'failed') AND g.status='building' "
                "AND d.expires_at>clock_timestamp()"
            ).fetchone()
        if row is None:
            raise RuntimeError("row is unexpectedly None")
        return max(0.0, float(row["lag_seconds"])) if row["lag_seconds"] is not None else 0.0

    def dataset(self, service_id: str, dataset_id: str) -> Dataset:
        with self.transaction() as con:
            row = con.execute(
                "SELECT * FROM clickhouse_datasets WHERE service_id=%s AND dataset_id=%s",
                (service_id, dataset_id),
            ).fetchone()
        if row is None:
            raise ValueError("dataset not found")
        dataset = _construct(Dataset, row)
        if dataset.expires_at <= datetime.now(UTC):
            raise ValueError("dataset expired")
        if dataset.schema_version != CLICKHOUSE_SCHEMA_VERSION:
            raise ValueError("dataset schema unsupported")
        return dataset

    def artifacts(self, service_id: str, dataset_id: str) -> list[Artifact]:
        with self.transaction() as con:
            rows = con.execute(
                "SELECT * FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s ORDER BY ordinal_start",
                (service_id, dataset_id),
            ).fetchall()
        return [_construct(Artifact, row) for row in rows]

    def artifact(self, service_id: str, generation: str, batch_id: str) -> Artifact:
        gen = self.generation(service_id, generation)
        self.dataset(service_id, gen["dataset_id"])
        with self.transaction() as con:
            row = con.execute(
                "SELECT * FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s AND batch_id=%s",
                (service_id, gen["dataset_id"], batch_id),
            ).fetchone()
        if row is None:
            raise ValueError("artifact not in generation")
        return _construct(Artifact, row)

    def generation(self, service_id: str, generation: str) -> dict:
        with self.transaction() as con:
            row = con.execute(
                "SELECT * FROM clickhouse_generations WHERE service_id=%s AND generation=%s",
                (service_id, generation),
            ).fetchone()
        if row is None:
            raise ValueError("generation not found")
        return dict(row)

    def begin_generation(self, service_id: str, dataset_id: str, target: str) -> str:
        self.dataset(service_id, dataset_id)
        generation = uuid4().hex
        with self.transaction() as con:
            con.execute(
                "INSERT INTO clickhouse_service_selection(service_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (service_id,),
            )
            revision = con.execute(
                "SELECT revision FROM clickhouse_service_selection WHERE service_id=%s FOR UPDATE",
                (service_id,),
            ).fetchone()["revision"]
            con.execute(
                "INSERT INTO clickhouse_generations(service_id,generation,dataset_id,target_identity,selection_revision)"
                " VALUES (%s,%s,%s,%s,%s)",
                (service_id, generation, dataset_id, target, revision),
            )
            con.execute(
                "INSERT INTO clickhouse_publications(service_id,generation,dataset_id,batch_id) "
                "SELECT service_id,%s,dataset_id,batch_id FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s",
                (generation, service_id, dataset_id),
            )
        return generation

    def claim(self, service_id: str, generation: str, batch_id: str, *, lease_seconds: int = 120) -> int | None:
        if not 1 <= lease_seconds <= 300:
            raise ValueError("lease must be bounded")
        with self.transaction() as con:
            row = con.execute(
                "UPDATE clickhouse_publications p SET status='claimed', lease_fence=lease_fence+1,"
                "attempt_count=attempt_count+1, lease_until=clock_timestamp()+%s * interval '1 second',"
                "updated_at=clock_timestamp() FROM clickhouse_generations g, clickhouse_datasets d "
                "WHERE p.service_id=%s AND p.generation=%s AND p.batch_id=%s "
                "AND g.service_id=p.service_id AND g.generation=p.generation AND g.status='building' "
                "AND d.service_id=p.service_id AND d.dataset_id=p.dataset_id AND d.expires_at>clock_timestamp() "
                "AND p.status!='published' AND (p.status!='claimed' OR p.lease_until<=clock_timestamp()) "
                "RETURNING p.lease_fence",
                (lease_seconds, service_id, generation, batch_id),
            ).fetchone()
        return row["lease_fence"] if row else None

    def finish(
        self,
        service_id: str,
        generation: str,
        batch_id: str,
        fence: int,
        *,
        digest: str | None,
        error: str | None = None,
    ) -> bool:
        with self.transaction() as con:
            result = con.execute(
                "UPDATE clickhouse_publications p SET status=%s,verified_digest=%s,"
                "published_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,"
                "first_error=COALESCE(first_error,%s),last_error=%s,updated_at=clock_timestamp() "
                "FROM clickhouse_datasets d WHERE p.service_id=%s AND p.generation=%s AND p.batch_id=%s "
                "AND p.lease_fence=%s AND p.status='claimed' AND p.lease_until>clock_timestamp() "
                "AND d.service_id=p.service_id AND d.dataset_id=p.dataset_id AND d.expires_at>clock_timestamp()",
                (
                    "published" if digest else "failed",
                    digest,
                    bool(digest),
                    error,
                    error,
                    service_id,
                    generation,
                    batch_id,
                    fence,
                ),
            )
            return result.rowcount == 1

    def activate(self, service_id: str, generation: str, target: str) -> bool:
        """Caller verified the target; lock + revision CAS prevents late promotion."""
        with self.transaction() as con:
            selection = con.execute(
                "SELECT * FROM clickhouse_service_selection WHERE service_id=%s FOR UPDATE",
                (service_id,),
            ).fetchone()
            gen = con.execute(
                "SELECT g.*, d.expires_at FROM clickhouse_generations g JOIN clickhouse_datasets d "
                "USING(service_id,dataset_id) WHERE g.service_id=%s AND g.generation=%s FOR UPDATE OF g",
                (service_id, generation),
            ).fetchone()
            if (
                not selection
                or not gen
                or (
                    gen["status"] != "building"
                    or gen["target_identity"] != target
                    or gen["selection_revision"] != selection["revision"]
                    or gen["expires_at"] <= datetime.now(UTC)
                )
            ):
                return False
            missing = con.execute(
                "SELECT count(*) AS n FROM clickhouse_artifacts a LEFT JOIN clickhouse_publications p "
                "ON p.service_id=a.service_id AND p.dataset_id=a.dataset_id AND p.batch_id=a.batch_id "
                "AND p.generation=%s WHERE a.service_id=%s AND a.dataset_id=%s "
                "AND (p.status IS DISTINCT FROM 'published' OR p.verified_digest IS DISTINCT FROM a.canonical_digest)",
                (generation, service_id, gen["dataset_id"]),
            ).fetchone()["n"]
            if missing:
                return False
            con.execute(
                "UPDATE clickhouse_generations SET status='retired' WHERE service_id=%s AND status='active'",
                (service_id,),
            )
            con.execute(
                "UPDATE clickhouse_generations SET status='active',activated_at=clock_timestamp() "
                "WHERE service_id=%s AND generation=%s",
                (service_id, generation),
            )
            con.execute(
                "UPDATE clickhouse_service_selection SET generation=%s,revision=revision+1 WHERE service_id=%s",
                (generation, service_id),
            )
            return True

    def selected(self, service_id: str) -> dict | None:
        with self.transaction() as con:
            row = con.execute(
                "SELECT g.* FROM clickhouse_service_selection s JOIN clickhouse_generations g "
                "USING(service_id,generation) WHERE s.service_id=%s AND g.status='active'",
                (service_id,),
            ).fetchone()
        return dict(row) if row else None

    def pending(self, service_id: str, generation: str, limit: int) -> list[str]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        with self.transaction() as con:
            rows = con.execute(
                "SELECT p.batch_id FROM clickhouse_publications p JOIN clickhouse_artifacts a "
                "USING(service_id,dataset_id,batch_id) WHERE p.service_id=%s AND p.generation=%s "
                "AND p.status!='published' ORDER BY a.ordinal_start LIMIT %s",
                (service_id, generation, limit),
            ).fetchall()
        return [r["batch_id"] for r in rows]


# ── ClickHouse Publication & Schema (folded from backend.core.clickhouse_publication) ──

import hashlib
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.exceptions import ClientError

from backend.core.duckdb import _get_fos_client
from backend.core.metadata.pg_schema import CLICKHOUSE_CONTROL_DDL
from backend.utils.telemetry import ensure_utc, parse_iso_utc

__all__ = [
    "ARTIFACT_SCHEMA",
    "CLICKHOUSE_FACT_DDL",
    "CLICKHOUSE_SCHEMA_VERSION",
    "DurableBatch",
    "FACT_TYPES",
    "FosArtifacts",
    "MAX_ARTIFACT_BYTES",
    "MAX_BATCH_ROWS",
    "MAX_DATASET_ROWS",
    "MAX_ROW_BYTES",
    "PAYLOAD_COLUMNS",
    "PublicationResult",
    "ReplayResult",
    "_record_schema",
    "activate_generation",
    "build_batch_id",
    "canonical_bytes",
    "canonical_row",
    "create_clickhouse_schema",
    "digest_rows",
    "full_rebuild",
    "main",
    "maintain_clickhouse_system_tables",
    "publish_batch",
    "replay_unpublished",
    "require_postgres",
    "target_identity",
    "utc",
    "verify_generation",
]
logger = structlog.get_logger(__name__)

CLICKHOUSE_SCHEMA_VERSION = 2
FACT_TYPES = (
    ("service_id", "String"),
    ("batch_id", "String"),
    ("source_identity", "String"),
    ("row_ordinal", "UInt64"),
    ("generation", "String"),
    ("timestamp", "DateTime64(6, 'UTC')"),
    ("country", "Nullable(String)"),
    ("ip", "Nullable(String)"),
    ("url", "Nullable(String)"),
    ("conn_requests", "Nullable(Int64)"),
)
CLICKHOUSE_FACT_DDL = """
CREATE TABLE IF NOT EXISTS log_facts (
    service_id String,
    batch_id String,
    source_identity String,
    row_ordinal UInt64,
    generation String,
    timestamp DateTime64(6, 'UTC'),
    country Nullable(String),
    ip Nullable(String),
    url Nullable(String),
    conn_requests Nullable(Int64)
) ENGINE = ReplacingMergeTree
PARTITION BY (service_id, generation)
ORDER BY (service_id, generation, batch_id, row_ordinal)
"""


def target_identity(client: ClickHouseClient) -> str:
    """Proof of the actual table, not a host name (a replacement volume differs)."""
    tables = client.execute(
        "SELECT toString(uuid) AS uuid, engine, sorting_key, partition_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = 'log_facts'"
    )
    if (
        len(tables) != 1
        or tables[0].get("engine") != "ReplacingMergeTree"
        or tables[0].get("sorting_key") != "service_id, generation, batch_id, row_ordinal"
        or tables[0].get("partition_key") != "(service_id, generation)"
        or tables[0].get("uuid") in (None, "", "00000000-0000-0000-0000-000000000000")
    ):
        raise RuntimeError("ClickHouse fact schema mismatch or missing table identity")
    columns = client.execute(
        "SELECT name, type FROM system.columns "
        "WHERE database = currentDatabase() AND table = 'log_facts' ORDER BY position"
    )
    if [(r["name"], r["type"]) for r in columns] != list(FACT_TYPES):
        raise RuntimeError("ClickHouse fact schema columns mismatch")
    return tables[0]["uuid"]


def _record_schema(identity: str) -> None:
    require_postgres()
    # The SQLite-shaped wrapper's commit() is a no-op. Use a real psycopg
    # transaction even though the underlying pool connections are autocommit.
    with pg_connection.get_pg_pool().connection() as con, con.transaction():
        con.execute("SELECT pg_advisory_xact_lock(2080406)")
        for ddl in CLICKHOUSE_CONTROL_DDL:
            con.execute(ddl)
        con.execute(
            "INSERT INTO clickhouse_schema_state(target_identity, schema_version) VALUES (%s, %s) "
            "ON CONFLICT (target_identity) DO UPDATE SET "
            "schema_version = EXCLUDED.schema_version, checked_at = clock_timestamp()",
            (identity, CLICKHOUSE_SCHEMA_VERSION),
        )


def maintain_clickhouse_system_tables(client: ClickHouseClient) -> None:
    """Ensure internal system tables do not accumulate unbounded parts and trigger merge OOMs."""
    log = logging.getLogger(__name__)
    for profiler_tbl in ("trace_log", "processors_profile_log"):
        try:
            client.execute(f"TRUNCATE TABLE IF EXISTS system.{profiler_tbl}")
        except Exception as e:
            log.debug("Failed to truncate system.%s: %s", profiler_tbl, e)

    for tbl in ("metric_log", "asynchronous_metric_log", "text_log", "query_log", "part_log", "error_log", "trace_log"):
        try:
            client.execute(f"ALTER TABLE system.{tbl} MODIFY TTL event_date + INTERVAL 1 DAY")
        except Exception as e:
            log.debug("Failed to alter TTL on system.%s: %s", tbl, e)


def create_clickhouse_schema(client: ClickHouseClient) -> None:
    require_postgres()
    maintain_clickhouse_system_tables(client)
    client.execute(CLICKHOUSE_FACT_DDL)
    _record_schema(target_identity(client))


def main() -> None:
    client = get_clickhouse_client()
    if client is None:
        raise RuntimeError("ClickHouse prototype is disabled")
    try:
        create_clickhouse_schema(client)

        try:
            from backend.high_scale.publication import install_clickhouse_schema

            install_clickhouse_schema()
        except ImportError:
            pass

        logging.getLogger(__name__).info("ClickHouse schema v%s verified", CLICKHOUSE_SCHEMA_VERSION)
    finally:
        client.close()
        if pg_connection.is_postgres():
            pg_connection.get_pg_pool().close()


if "backend.core" in sys.modules:
    setattr(sys.modules["backend.core"], "clickhouse_schema", sys.modules[__name__])


def _ensure_manifest_symbols() -> None:
    if "PgManifest" not in globals():
        from backend.core.clickhouse import Artifact as _A
        from backend.core.clickhouse import Dataset as _D
        from backend.core.clickhouse import PgManifest as _M

        globals()["Artifact"] = _A
        globals()["Dataset"] = _D
        globals()["PgManifest"] = _M


MAX_DATASET_ROWS = 1_000_000
MAX_BATCH_ROWS = 10_000
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_ROW_BYTES = 64 * 1024
PAYLOAD_COLUMNS = ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests")
ARTIFACT_SCHEMA = pa.schema(
    [
        ("source_identity", pa.string()),
        ("row_ordinal", pa.uint64()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("country", pa.string()),
        ("ip", pa.string()),
        ("url", pa.string()),
        ("conn_requests", pa.int64()),
    ]
)


def utc(value: str | datetime) -> datetime:
    if isinstance(value, str):
        parsed = parse_iso_utc(value)
        if parsed is None:
            raise ValueError(f"Invalid ISO format string: {value!r}")
        return parsed
    return ensure_utc(value)


def canonical_row(row: dict[str, Any]) -> tuple:
    ordinal = row["row_ordinal"]
    if type(ordinal) is not int or not 0 <= ordinal < MAX_DATASET_ROWS:
        raise ValueError("invalid row ordinal")
    timestamp = utc(row["timestamp"])
    if not datetime(1970, 1, 1, tzinfo=UTC) <= timestamp < datetime(2262, 1, 1, tzinfo=UTC):
        raise ValueError("timestamp outside prototype range")
    values = [row["source_identity"], ordinal, timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")]
    if not isinstance(values[0], str):
        raise ValueError("invalid source identity")
    for name in ("country", "ip", "url"):
        value = row[name]
        if value is not None and not isinstance(value, str):
            raise ValueError("invalid nullable dimension")
        values.append(value)
    conn = row["conn_requests"]
    if conn is not None and (type(conn) is not int or not -(2**63) <= conn < 2**63):
        raise ValueError("invalid conn_requests")
    result = (*values, conn)
    if len(canonical_bytes(result)) > MAX_ROW_BYTES:
        raise ValueError("oversized row")
    return result


def canonical_bytes(row: tuple) -> bytes:
    return (json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest_rows(rows) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_bytes(row))
    return digest.hexdigest()


def build_batch_id(service_id: str, source_identity: str, content_hash: str) -> str:
    return hashlib.sha256(canonical_bytes((service_id, source_identity, content_hash))).hexdigest()


class FosArtifacts:
    """Bucket/prefix confinement and bounded hash-verified I/O, through existing telemetry."""

    def __init__(self, source: dict, *, client=None):
        self.service_id = source["service_id"]
        self.bucket = source["bucket"]
        if not self.bucket:
            raise ValueError("configured Fastly Object Storage bucket required")
        root = (source.get("prefix") or "").strip("/")
        tenant = hashlib.sha256(self.service_id.encode()).hexdigest()
        self.prefix = f"{root + '/' if root else ''}clickhouse-prototype/{tenant}/"
        self.client = client if client is not None else _get_fos_client(source)

    def _key(self, artifact: Artifact) -> str:
        uri = urlsplit(artifact.artifact_uri)
        if uri.scheme != "s3" or uri.netloc != self.bucket or uri.query or uri.fragment:
            raise ValueError("artifact bucket must match configured Fastly Object Storage bucket")
        key = uri.path.lstrip("/")
        expected = f"{self.prefix}{artifact.dataset_id}/{artifact.content_hash}.parquet"
        if key != expected or artifact.service_id != self.service_id:
            raise ValueError("artifact outside service prototype prefix")
        return key

    def put(self, dataset: Dataset, rows: tuple, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> Artifact:
        _ensure_manifest_symbols()
        table_rows = [dict(zip(PAYLOAD_COLUMNS, (*row[:2], utc(row[2]), *row[3:]), strict=True)) for row in rows]
        table = pa.Table.from_pylist(table_rows, schema=ARTIFACT_SCHEMA)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd", row_group_size=MAX_BATCH_ROWS, version="2.6")
        payload = sink.getvalue().to_pybytes()
        if len(payload) > max_bytes:
            raise ValueError("artifact exceeds byte limit")
        content_hash = hashlib.sha256(payload).hexdigest()
        artifact = Artifact(
            dataset.service_id,
            dataset.dataset_id,
            build_batch_id(dataset.service_id, dataset.dataset_id, content_hash),
            f"s3://{self.bucket}/{self.prefix}{dataset.dataset_id}/{content_hash}.parquet",
            content_hash,
            digest_rows(rows),
            len(payload),
            rows[0][1],
            len(rows),
        )
        key = self._key(artifact)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=payload,
                IfNoneMatch="*",
                ContentType="application/vnd.apache.parquet",
                Metadata={"sha256": content_hash, "expires-at": dataset.expires_at.isoformat()},
            )
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                raise RuntimeError("Fastly Object Storage artifact PUT failed") from None
            # A content-addressed retry may encounter an already-present object.
            # It is success only if its bytes and metadata are exactly correct.
            self.load(artifact)
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        if head["ContentLength"] != artifact.byte_size or head.get("Metadata", {}).get("sha256") != content_hash:
            raise ValueError("artifact HEAD verification failed")
        logger.info("clickhouse.artifact", operation="put", rows=len(rows), bytes=artifact.byte_size)
        return artifact

    def load(self, artifact: Artifact) -> DurableBatch:
        if not 1 <= artifact.byte_size <= MAX_ARTIFACT_BYTES or not 1 <= artifact.row_count <= MAX_BATCH_ROWS:
            raise ValueError("artifact exceeds bounds")
        key = self._key(artifact)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"]
        try:
            if response["ContentLength"] != artifact.byte_size:
                raise ValueError("artifact size mismatch")
            payload = body.read(MAX_ARTIFACT_BYTES + 1)
        finally:
            body.close()
        if len(payload) != artifact.byte_size or hashlib.sha256(payload).hexdigest() != artifact.content_hash:
            raise ValueError("artifact hash mismatch")
        parquet = pq.ParquetFile(pa.BufferReader(payload))
        if (
            parquet.metadata.num_rows != artifact.row_count
            or parquet.schema_arrow != ARTIFACT_SCHEMA
            or sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
            > MAX_ARTIFACT_BYTES
        ):
            raise ValueError("artifact decoded schema or size mismatch")
        rows = tuple(canonical_row(row) for batch in parquet.iter_batches(batch_size=1000) for row in batch.to_pylist())
        if digest_rows(rows) != artifact.canonical_digest:
            raise ValueError("artifact canonical hash mismatch")
        logger.info("clickhouse.artifact", operation="get", rows=len(rows), bytes=artifact.byte_size)
        return DurableBatch(artifact, rows)


@dataclass(frozen=True)
class DurableBatch:
    artifact: Artifact
    rows: tuple[tuple, ...]


@dataclass(frozen=True)
class PublicationResult:
    batch_id: str
    status: str
    row_count: int = 0
    fence: int | None = None


@dataclass(frozen=True)
class ReplayResult:
    generation: str
    attempted: int
    published: int
    activated: bool


def _require_client(client: ClickHouseClient | None) -> ClickHouseClient:
    result = client if client is not None else get_clickhouse_client()
    if result is None:
        raise RuntimeError("ClickHouse prototype disabled")
    return result


def _target_rows(client: ClickHouseClient, service_id: str, generation: str, artifact: Artifact) -> tuple:
    rows = client.execute(
        "SELECT source_identity,row_ordinal,timestamp,country,ip,url,conn_requests FROM log_facts FINAL "
        "WHERE service_id={service:String} AND generation={generation:String} AND batch_id={batch:String} "
        "ORDER BY row_ordinal LIMIT 10001",
        {"service": service_id, "generation": generation, "batch": artifact.batch_id},
    )
    return tuple(canonical_row(row) for row in rows)


def _validate_payload(artifact: Artifact, rows: tuple) -> None:
    if len(rows) != artifact.row_count or len(rows) > MAX_BATCH_ROWS:
        raise ValueError("artifact payload row count mismatch")
    for index, row in enumerate(rows):
        if (
            canonical_row(dict(zip(PAYLOAD_COLUMNS, row, strict=True))) != row
            or row[1] != artifact.ordinal_start + index
        ):
            raise ValueError("artifact payload ordinals or types mismatch")
    if digest_rows(rows) != artifact.canonical_digest:
        raise ValueError("artifact payload digest mismatch")


def publish_batch(
    service_id: str,
    batch: DurableBatch,
    *,
    generation: str,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> PublicationResult:
    started = time.monotonic()
    outcome = "failed"
    error_kind = None
    try:
        result = _publish_batch(service_id, batch, generation=generation, store=store, client=client)
        outcome = result.status
        return result
    except Exception as exc:
        error_kind = type(exc).__name__
        raise
    finally:
        # The outer boundary includes config, manifest, payload and claim
        # failures. Observers must never replace an original result/error.
        try:
            record_publication(outcome)
        except Exception:
            logger.debug("publish_batch: record_publication failed (ignored)", exc_info=True)
        try:
            log = logger.error if outcome == "failed" else logger.info
            log(
                "clickhouse.publication",
                outcome=outcome,
                error_kind=error_kind,
                batch_id=batch.artifact.batch_id,
                rows=len(batch.rows),
                duration_ms=(time.monotonic() - started) * 1000,
            )
        except Exception:
            logger.debug("publish_batch: log failed (ignored)", exc_info=True)


def _publish_batch(
    service_id: str,
    batch: DurableBatch,
    *,
    generation: str,
    store: PgManifest | None,
    client: ClickHouseClient | None,
) -> PublicationResult:
    _ensure_manifest_symbols()
    store = store or PgManifest()
    client = _require_client(client)
    artifact = store.artifact(service_id, generation, batch.artifact.batch_id)
    if artifact != batch.artifact:
        raise ValueError("artifact differs from immutable manifest")
    _validate_payload(artifact, batch.rows)
    gen = store.generation(service_id, generation)
    if target_identity(client) != gen["target_identity"]:
        raise ValueError("target changed; full rebuild required")
    fence = store.claim(service_id, generation, artifact.batch_id)
    if fence is None:
        return PublicationResult(artifact.batch_id, "not_claimed")
    try:
        # Each chunk is at most ~4 MiB of canonical payload; rows are already
        # artifact-validated, so a delayed worker cannot insert different values.
        chunk: list[tuple] = []
        size = 0
        for row in batch.rows:
            if size >= 4 * 1024 * 1024 or len(chunk) >= 1000:
                client.insert_rows("log_facts", list(CLICKHOUSE_FACT_COLUMNS), chunk)
                chunk, size = [], 0
            chunk.append((service_id, artifact.batch_id, row[0], row[1], generation, *row[2:]))
            size += len(canonical_bytes(row))
        client.insert_rows("log_facts", list(CLICKHOUSE_FACT_COLUMNS), chunk)
        actual = _target_rows(client, service_id, generation, artifact)
        if actual != batch.rows:
            raise ValueError("ordered target payload mismatch")
        if target_identity(client) != gen["target_identity"]:
            raise ValueError("target changed during publication")
        published = store.finish(service_id, generation, artifact.batch_id, fence, digest=artifact.canonical_digest)
    except Exception as exc:
        kind = type(exc).__name__
        try:
            store.finish(service_id, generation, artifact.batch_id, fence, digest=None, error=kind)
        except Exception:
            logger.warning("_publish_batch: store.finish failed (ignored)", exc_info=True)
        raise RuntimeError(f"ClickHouse publication failed: {kind}") from None
    return PublicationResult(artifact.batch_id, "published" if published else "stale", len(batch.rows), fence)


def verify_generation(service_id: str, generation: str, *, store: PgManifest, client: ClickHouseClient) -> Dataset:
    gen = store.generation(service_id, generation)
    dataset = store.dataset(service_id, gen["dataset_id"])
    if target_identity(client) != gen["target_identity"]:
        raise ValueError("target changed; full rebuild required")
    digest = hashlib.sha256()
    count = 0
    for artifact in store.artifacts(service_id, dataset.dataset_id):
        if artifact.ordinal_start != count:
            raise ValueError("manifest coverage mismatch")
        rows = _target_rows(client, service_id, generation, artifact)
        _validate_payload(artifact, rows)
        for row in rows:
            digest.update(canonical_bytes(row))
        count += len(rows)
    actual_count = client.execute(
        "SELECT count() AS n FROM log_facts FINAL WHERE service_id={service:String} AND generation={generation:String}",
        {"service": service_id, "generation": generation},
    )[0]["n"]
    if actual_count != count or count != dataset.expected_rows or digest.hexdigest() != dataset.canonical_digest:
        raise ValueError("generation coverage or digest mismatch")
    return dataset


def activate_generation(service_id: str, generation: str, *, store: PgManifest, client: ClickHouseClient) -> bool:
    verify_generation(service_id, generation, store=store, client=client)
    return store.activate(service_id, generation, target_identity(client))


def replay_unpublished(
    service_id: str,
    *,
    generation: str,
    loader: Callable[[Artifact], DurableBatch],
    limit: int = 100,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> ReplayResult:
    """Resume this building generation only. Rebuild uses full_rebuild instead."""
    _ensure_manifest_symbols()
    store = store or PgManifest()
    client = _require_client(client)
    gen = store.generation(service_id, generation)
    store.dataset(service_id, gen["dataset_id"])
    batches = store.pending(service_id, generation, limit)
    published = 0
    for batch_id in batches:
        result = publish_batch(
            service_id,
            loader(store.artifact(service_id, generation, batch_id)),
            generation=generation,
            store=store,
            client=client,
        )
        published += result.status == "published"
    activated = False
    if not store.pending(service_id, generation, 1):
        activated = activate_generation(service_id, generation, store=store, client=client)
    return ReplayResult(generation, len(batches), published, activated)


def full_rebuild(
    service_id: str,
    dataset_id: str,
    *,
    loader: Callable[[Artifact], DurableBatch],
    limit: int = 100,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> ReplayResult:
    """Always allocate a new target generation, including previously published rows."""
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be 1..1000")
    _ensure_manifest_symbols()
    store = store or PgManifest()
    client = _require_client(client)
    generation = store.begin_generation(service_id, dataset_id, target_identity(client))
    return replay_unpublished(service_id, generation=generation, loader=loader, limit=limit, store=store, client=client)


def __getattr__(name: str):
    if name == "readiness":
        return None
    if name in ("Artifact", "Dataset", "PgManifest"):
        _ensure_manifest_symbols()
        return globals()[name]
    raise AttributeError(name)


_self_mod = sys.modules[__name__]
_core_pkg = sys.modules.get("backend.core")
_backend_pkg = sys.modules.get("backend")
for _ch_alias in (
    "clickhouse_client",
    "clickhouse_observer",
    "clickhouse_metrics",
    "clickhouse_rows",
    "clickhouse_manifest",
    "clickhouse_publication",
    "clickhouse_schema",
):
    if _ch_alias != "clickhouse_schema":
        sys.modules[f"backend.core.{_ch_alias}"] = _self_mod
    if _core_pkg is not None:
        setattr(_core_pkg, _ch_alias, _self_mod)
    if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
        _backend_pkg._MODULE_ALIASES[f"backend.core.{_ch_alias}"] = __name__


if __name__ == "__main__":
    main()
