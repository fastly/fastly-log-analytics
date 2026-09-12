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
import threading
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Any
from uuid import UUID, uuid4

import httpx
import structlog

from backend import config
from backend.core.clickhouse_metrics import record_operation
from backend.core.query_registry import query_registry
from backend.utils.telemetry import record_call

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
    }
)
CLICKHOUSE_ALLOWED_TABLES = frozenset({CLICKHOUSE_FACT_TABLE}) | CLICKHOUSE_HIGH_SCALE_TABLES
CLICKHOUSE_ALLOWED_COLUMNS = frozenset(CLICKHOUSE_FACT_COLUMNS) | CLICKHOUSE_HIGH_SCALE_COLUMNS
CLICKHOUSE_TABLE_COLUMNS = {
    CLICKHOUSE_FACT_TABLE: frozenset(CLICKHOUSE_FACT_COLUMNS),
    **{table: CLICKHOUSE_HIGH_SCALE_COLUMNS for table in CLICKHOUSE_HIGH_SCALE_TABLES},
}
_PARAM_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
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
        self._slots = threading.BoundedSemaphore(settings.pool_max_size)
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
            limits=httpx.Limits(
                max_connections=settings.pool_max_size, max_keepalive_connections=settings.pool_max_size
            ),
            timeout=httpx.Timeout(
                settings.query_timeout_s, connect=settings.connect_timeout_s, pool=settings.pool_timeout_s
            ),
            transport=transport,
            follow_redirects=False,
            trust_env=False,
        )

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        """Run internal SQL. SELECT-like statements return JSON rows; DDL returns []."""
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("ClickHouse SQL must be nonempty")
        if re.search(r"\{[^{}]+:\s*Identifier\s*\}", sql, re.IGNORECASE):
            raise ValueError("ClickHouse identifier parameters are forbidden")
        form = {"query": sql}
        for name, value in (params or {}).items():
            if not isinstance(name, str) or not _PARAM_NAME.fullmatch(name):
                raise ValueError("invalid ClickHouse parameter name")
            form[f"param_{name}"] = _parameter(value)
        return self._request("execute", data=form)

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
    ) -> list[dict]:
        started = time.perf_counter()
        query_id = uuid4().hex
        registry_id = -1
        acquired = False
        active = False
        stats: dict[str, Any] = {
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
                        "ClickHouse", f"ClickHouse {operation}; query_id={query_id}", con=None
                    )
                except Exception:
                    pass
            with self._state_lock:
                if self._closed:
                    raise ClickHouseError("client closed", query_id=query_id)
            acquired = self._slots.acquire(timeout=self._settings.pool_timeout_s)
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
            }
            if operation == "insert":
                query.update({"query": sql or "", "async_insert": "0", "date_time_input_format": "best_effort"})
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
            stats["status_code"] = response.status_code
            stats["response_bytes"] = len(response.content)
            if not response.is_success or response.headers.get("X-ClickHouse-Exception-Code", "0") != "0":
                raise ClickHouseError("server error", query_id=query_id, status_code=response.status_code)
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
            elif data and re.match(r"\s*(SELECT|WITH|SHOW|DESCRIBE|EXPLAIN|EXISTS)\b", data["query"], re.IGNORECASE):
                raise ValueError("missing query response")
            if operation == "health" and result != [{"ok": 1}]:
                raise ValueError("invalid health response")
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
                try:
                    _record_operation(**stats)
                except Exception:
                    pass
                try:
                    record_operation(stats)
                except Exception:
                    pass
                try:
                    query_registry.deregister(
                        registry_id,
                        error=None if stats["outcome"] == "success" else RuntimeError(stats["outcome"]),
                    )
                except Exception:
                    pass


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
