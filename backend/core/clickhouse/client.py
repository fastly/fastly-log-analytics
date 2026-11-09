"""ClickHouse HTTP client, connection pool, OTel metrics, and FOS artifact storage."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import threading
import time
from collections.abc import Mapping
from functools import cache
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.exceptions import ClientError

from backend import config
from backend.core.clickhouse.constants import (
    _PARAM_NAME,
    _QUERY_SETTINGS,
    ARTIFACT_SCHEMA,
    CLICKHOUSE_ALLOWED_TABLES,
    CLICKHOUSE_HIGH_SCALE_TABLES,
    CLICKHOUSE_TABLE_COLUMNS,
    MAX_ARTIFACT_BYTES,
    MAX_BATCH_ROWS,
    PAYLOAD_COLUMNS,
    Artifact,
    ClickHouseError,
    Dataset,
    DurableBatch,
)
from backend.core.clickhouse.params import (
    _json_value,
    _parameter,
    _redact_ddl_secrets,
    build_batch_id,
    canonical_row,
    digest_rows,
    utc,
)
from backend.core.fos_client import _get_fos_client
from backend.core.request_telemetry import current_clickhouse_scope, get_meter, query_registry
from backend.utils.telemetry import _QUERIES, get_queries, record_call

logger = structlog.get_logger("backend.core.clickhouse")


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


_DOMAIN_TABLES: dict[str, tuple[str, ...]] = {
    "request": ("request_facts",),
    "request_aggregate": ("request_aggregates",),
    "cmcd": ("cmcd_projection_facts",),
    "network_dimensions": ("network_minute_dimensions",),
    "performance_dimensions": ("performance_minute_dimensions",),
    "rum": ("rum_vitals_facts",),
}

_MATERIALIZED_CF_KEYS: frozenset[str] = frozenset(
    {
        "_bot_name",
        "age",
        "asn",
        "backend",
        "bytes",
        "c_speed",
        "c_type",
        "cache",
        "cid",
        "city",
        "conn_requests",
        "cookie_session",
        "delivery_rate",
        "digest",
        "edge",
        "edge_score_reason",
        "edge_sid",
        "elapsed",
        "hits",
        "host",
        "io_input_format",
        "io_output_format",
        "ipv6",
        "is_bot",
        "is_ipv6",
        "is_shield",
        "is_ssl",
        "ja3",
        "ja4",
        "lat",
        "lon",
        "method",
        "metro",
        "ngwaf_bot",
        "ngwaf_bot_category",
        "ngwaf_wellknown_bot",
        "obytes",
        "oconnect_ms",
        "oip",
        "oretries",
        "ost",
        "ottfb",
        "ottlb",
        "p_desc",
        "p_type",
        "ploss",
        "pop",
        "prid",
        "proto",
        "protocol",
        "referer",
        "region",
        "req_header_bytes",
        "resp_bytes",
        "resp_header_content_encoding",
        "retrans",
        "rid",
        "rtt_min",
        "rtt_var",
        "server_region",
        "status",
        "tcp_rtt",
        "tls",
        "tls_ciphers_sha",
        "transport",
        "ttfb",
        "ttl",
        "ua",
        "waf",
        "waf_blocked_rules",
        "waf_logged_rules",
        "waf_ms",
        "waf_resp",
        "waf_sig",
    }
)
_CF_LITERAL_RE = re.compile(r"custom_fields\['([a-zA-Z0-9_]+)'\]")
_CF_PARAM_RE = re.compile(r"custom_fields\[\{([a-zA-Z0-9_]+):String\}\]")
_PUB_FENCE_RE = re.compile(
    r"batch_id\s+IN\s*\(\s*SELECT\s+batch_id\s+FROM\s+high_scale_batch_publications\s+FINAL\s+WHERE\s+([^()]+?)\)"
)


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
        self._pub_fence_cache: dict[str, tuple[float, dict[str, tuple[str, ...]] | None]] = {}
        self._pub_fence_refreshing: set[str] = set()
        self._empty_stale_batches: set[tuple[str, str, str]] = set()
        self._non_empty_stale_batches: set[tuple[str, str, str]] = set()
        self._cf_materialized_cache: tuple[float, bool] | None = None
        self._pub_fence_lock = threading.Lock()
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

    def _filter_empty_stale_batches(self, service_id: str, domain: str, stale_bids: list[str]) -> set[str]:
        uncached = [
            bid
            for bid in stale_bids
            if (service_id, domain, bid) not in self._empty_stale_batches
            and (service_id, domain, bid) not in self._non_empty_stale_batches
        ]
        if uncached and (tables := _DOMAIN_TABLES.get(domain)):
            try:
                quoted = ", ".join(f"'{bid}'" for bid in uncached)
                non_empty: set[str] = set()
                for tbl in tables:
                    prewhere = (
                        f"PREWHERE dimension = 'status' AND batch_id IN ({quoted})"
                        if tbl == "request_aggregates"
                        else f"PREWHERE batch_id IN ({quoted})"
                    )
                    rows = self._request(
                        "execute",
                        data={
                            "query": (
                                f"SELECT DISTINCT batch_id FROM `{tbl}` "
                                f"{prewhere} WHERE service_id = {{service_id:String}}"
                            ),
                            "param_service_id": _parameter(service_id),
                        },
                        settings=None,
                    )
                    for r in rows or []:
                        if b := str(r.get("batch_id") or ""):
                            non_empty.add(b)
                for bid in uncached:
                    if bid not in non_empty:
                        self._empty_stale_batches.add((service_id, domain, bid))
                    else:
                        self._non_empty_stale_batches.add((service_id, domain, bid))
            except Exception as exc:
                structlog.get_logger(__name__).debug("clickhouse_empty_stale_batch_probe_failed", error=str(exc))
        return {bid for bid in stale_bids if (service_id, domain, bid) in self._empty_stale_batches}

    def _compute_pending_batches(self, service_id: str) -> dict[str, tuple[str, ...]] | None:
        try:
            check_form = {
                "query": (
                    "SELECT domain, batch_id, "
                    "toUInt8(argMax(updated_at, manifest_version) < now() - INTERVAL 5 MINUTE) AS is_stale "
                    "FROM high_scale_batch_publications "
                    "PREWHERE (domain, generation, batch_id) IN ("
                    "SELECT domain, generation, batch_id FROM high_scale_batch_publications "
                    "PREWHERE publication_state != 'visible' "
                    "WHERE service_id = {service_id:String}"
                    ") "
                    "WHERE service_id = {service_id:String} "
                    "GROUP BY domain, generation, batch_id "
                    "HAVING argMax(publication_state, manifest_version) != 'visible'"
                ),
                "param_service_id": _parameter(service_id),
            }
            rows = self._request("execute", data=check_form, settings=None)
            candidates_by_dom: dict[str, list[tuple[str, bool]]] = {}
            stale_by_dom: dict[str, list[str]] = {}
            for r in rows or []:
                dom = str(r.get("domain") or "")
                bid = str(r.get("batch_id") or "")
                if dom and re.fullmatch(r"[0-9a-fA-F-]+", bid):
                    is_stale = int(r.get("is_stale") or 0) == 1
                    candidates_by_dom.setdefault(dom, []).append((bid, is_stale))
                    if is_stale:
                        stale_by_dom.setdefault(dom, []).append(bid)
            empty_stale_by_dom = {
                dom: self._filter_empty_stale_batches(service_id, dom, bids)
                for dom, bids in stale_by_dom.items()
                if len(candidates_by_dom.get(dom, ())) > 50
            }
            by_dom: dict[str, list[str]] = {}
            for dom, cands in candidates_by_dom.items():
                empty_set = empty_stale_by_dom.get(dom, set())
                kept = [bid for bid, is_stale in cands if not (is_stale and bid in empty_set)]
                if kept:
                    by_dom[dom] = kept
            return {k: tuple(v) for k, v in by_dom.items()}
        except Exception:
            return None

    def _refresh_pending_batches_bg(self, service_id: str) -> None:
        try:
            pending_map = self._compute_pending_batches(service_id)
            with self._pub_fence_lock:
                self._pub_fence_cache[service_id] = (time.monotonic(), pending_map)
        finally:
            with self._pub_fence_lock:
                self._pub_fence_refreshing.discard(service_id)

    def _get_pending_batches(self, service_id: str) -> dict[str, tuple[str, ...]] | None:
        if isinstance(self._http._transport, httpx.MockTransport):
            return None
        with self._pub_fence_lock:
            now = time.monotonic()
            cached = self._pub_fence_cache.get(service_id)
            if cached is not None:
                if (now - cached[0]) < 30.0:
                    return cached[1]
                if service_id not in self._pub_fence_refreshing:
                    self._pub_fence_refreshing.add(service_id)
                    threading.Thread(target=self._refresh_pending_batches_bg, args=(service_id,), daemon=True).start()
                return cached[1]
            pending_map = self._compute_pending_batches(service_id)
            self._pub_fence_cache[service_id] = (time.monotonic(), pending_map)
            return pending_map

    def _has_materialized_cf_columns(self) -> bool:
        if isinstance(self._http._transport, httpx.MockTransport):
            return bool(getattr(self, "_enable_cf_rewrite_in_tests", False))
        if os.getenv("PYTEST_CURRENT_TEST") and not getattr(self, "_enable_cf_rewrite_in_tests", False):
            return False
        with self._pub_fence_lock:
            now = time.monotonic()
            if self._cf_materialized_cache is not None:
                ttl = 60.0 if self._cf_materialized_cache[1] else 10.0
                if (now - self._cf_materialized_cache[0]) < ttl:
                    return self._cf_materialized_cache[1]
            ready = False
            try:
                rows = self._request(
                    "execute",
                    data={
                        "query": (
                            "SELECT "
                            "(SELECT count() FROM system.columns "
                            " WHERE database = currentDatabase() AND table = 'request_facts' AND name LIKE '_cf_%') AS cols, "
                            "(SELECT count() FROM system.mutations "
                            " WHERE database = currentDatabase() AND table = 'request_facts' AND is_done = 0 "
                            " AND command LIKE '%MATERIALIZE COLUMN%') AS muts"
                        ),
                    },
                    settings=None,
                )
                if rows:
                    cols = int(rows[0].get("cols") or 0)
                    muts = int(rows[0].get("muts") or 0)
                    ready = cols >= len(_MATERIALIZED_CF_KEYS) and muts == 0
            except Exception:
                ready = False
            self._cf_materialized_cache = (time.monotonic(), ready)
            return ready

    def rewrite_publication_fence(self, sql: str, params: dict | None = None) -> str:
        """Rewrite ``batch_id IN (SELECT ... FROM high_scale_batch_publications FINAL ...)`` when pending set is small."""
        svc_id = (params or {}).get("service_id")
        if (
            isinstance(svc_id, str)
            and svc_id
            and "high_scale_batch_publications FINAL" in sql
            and _PUB_FENCE_RE.search(sql)
            and (pending_map := self._get_pending_batches(svc_id)) is not None
        ):

            def _replace_fence(match: re.Match[str]) -> str:
                where_body = match.group(1)
                dom: str | None = None
                if m_lit := re.search(r"domain\s*=\s*'([a-z0-9_]+)'", where_body):
                    dom = m_lit.group(1)
                elif m_par := re.search(r"domain\s*=\s*\{([a-zA-Z0-9_]+):String\}", where_body):
                    dom = str((params or {}).get(m_par.group(1)) or "")
                pending_ids = (
                    pending_map.get(dom, ())
                    if dom
                    else tuple(sorted({bid for bids in pending_map.values() for bid in bids}))
                )
                if not pending_ids:
                    return "1=1"
                if len(pending_ids) <= 250:
                    quoted = ", ".join(f"'{bid}'" for bid in pending_ids)
                    return f"batch_id NOT IN ({quoted})"
                return match.group(0)

            sql = _PUB_FENCE_RE.sub(_replace_fence, sql)

        upper_sql = sql.lstrip().upper()
        is_fact_query = "request_facts" in sql and upper_sql.startswith(("SELECT", "WITH", "/*"))
        is_where_fragment = not upper_sql.startswith(
            ("SELECT", "WITH", "/*", "INSERT", "ALTER", "CREATE", "DROP", "OPTIMIZE", "SYSTEM", "TRUNCATE")
        )
        if (
            (is_fact_query or is_where_fragment)
            and "custom_fields[" in sql
            and "SELECT custom_fields FROM" not in sql
            and "SELECT event_timestamp, custom_fields FROM" not in sql
            and self._has_materialized_cf_columns()
        ):
            sql = _CF_LITERAL_RE.sub(
                lambda m: f"`_cf_{m.group(1)}`" if m.group(1) in _MATERIALIZED_CF_KEYS else m.group(0),
                sql,
            )
            if params:
                sql = _CF_PARAM_RE.sub(
                    lambda m: (
                        f"`_cf_{params[m.group(1)]}`"
                        if m.group(1) in params and str(params[m.group(1)]) in _MATERIALIZED_CF_KEYS
                        else m.group(0)
                    ),
                    sql,
                )
        return sql

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
        sql = self.rewrite_publication_fence(sql, params)
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
            chunks: list[bytes] = []
            for row in rows:
                for v in row:
                    if isinstance(v, float) and not math.isfinite(v):
                        raise ValueError("invalid ClickHouse insert value")
                chunks.append(
                    orjson.dumps(
                        row,
                        default=_json_value,
                        option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_PASSTHROUGH_DATETIME,
                    )
                )
            content = b"".join(chunks)
        except (ValueError, TypeError, UnicodeError):
            raise ValueError("invalid ClickHouse insert value") from None
        quoted_columns = ", ".join(f"`{c}`" for c in columns)
        sql = f"INSERT INTO `{table}` ({quoted_columns}) FORMAT JSONCompactEachRow"
        if table == "high_scale_batch_publications":
            with self._pub_fence_lock:
                self._pub_fence_cache.clear()
        elif table in CLICKHOUSE_HIGH_SCALE_TABLES and "batch_id" in columns:
            b_idx = columns.index("batch_id")
            inserted_bids = {str(r[b_idx]) for r in rows}
            with self._pub_fence_lock:
                if self._empty_stale_batches:
                    before = len(self._empty_stale_batches)
                    self._empty_stale_batches = {
                        item for item in self._empty_stale_batches if item[2] not in inserted_bids
                    }
                    if len(self._empty_stale_batches) != before:
                        self._pub_fence_cache.clear()
        self._request("insert", sql=sql, content=content, rows_written=len(rows))

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        """Synchronously remove a failed high-scale batch before a retry."""
        if table not in CLICKHOUSE_HIGH_SCALE_TABLES:
            raise ValueError("ClickHouse table is not internally allowlisted")
        # request_insight_hourly is filled by a materialized view on request_facts, so it holds the batch too.
        for target in (table, "request_insight_hourly") if table == "request_facts" else (table,):
            self.execute(
                f"ALTER TABLE `{target}` DELETE WHERE batch_id={{batch_id:UUID}} SETTINGS mutations_sync=2",
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
                "max_memory_usage": os.getenv("CLICKHOUSE_QUERY_MAX_MEMORY_BYTES", "1073741824"),
                "max_threads": os.getenv("CLICKHOUSE_QUERY_MAX_THREADS", "2"),
                "memory_overcommit_ratio_denominator": "0",
                "memory_overcommit_ratio_denominator_for_user": "0",
                "max_bytes_before_external_group_by": os.getenv(
                    "CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY", "134217728"
                ),
                "max_bytes_before_external_sort": os.getenv("CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_SORT", "134217728"),
            }
            if settings:
                query.update(settings)
                if (
                    "additional_table_filters" in settings
                    and " IN (SELECT " in str(settings["additional_table_filters"])
                    and "MockTransport" not in type(self._http._transport).__name__
                    and not getattr(self, "_analyzer_mandatory", False)
                ):
                    query.setdefault("enable_analyzer", "0")
            if operation == "insert":
                is_mock = "MockTransport" in type(self._http._transport).__name__
                query.update(
                    {
                        "query": sql or "",
                        "async_insert": "0",
                        "date_time_input_format": "best_effort",
                        "max_memory_usage": os.getenv("CLICKHOUSE_INSERT_MAX_MEMORY_BYTES", "536870912"),
                        "max_threads": "2" if is_mock else os.getenv("CLICKHOUSE_INSERT_MAX_THREADS", "4"),
                        "max_insert_threads": "1" if is_mock else os.getenv("CLICKHOUSE_INSERT_THREADS", "2"),
                        "max_block_size": "2048" if is_mock else os.getenv("CLICKHOUSE_INSERT_MAX_BLOCK_SIZE", "65536"),
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
                if (
                    response.status_code == 500
                    and query.get("enable_analyzer") == "0"
                    and (
                        response.headers.get("X-ClickHouse-Exception-Code") == "452"
                        or "enable_analyzer" in response.text
                    )
                ):
                    self._analyzer_mandatory = True
                    query.pop("enable_analyzer", None)
                    response = self._http.post(
                        "/",
                        params=query,
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
            new_client = ClickHouseClient(settings)
            _client = new_client
            setattr(sys.modules[__name__], "_client", new_client)
        return _client


def close_clickhouse_client() -> None:
    global _client
    with _client_lock:
        client, _client = _client, None
        setattr(sys.modules[__name__], "_client", None)
    if client is not None:
        client.close()


def _require_client(client: ClickHouseClient | None) -> ClickHouseClient:
    result = client if client is not None else get_clickhouse_client()
    if result is None:
        raise RuntimeError("ClickHouse prototype disabled")
    return result


# ── ClickHouse OTel Metrics ──


@cache
def _instrument(name: str, kind: str, unit: str = "") -> Any:
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


class FosArtifacts:
    """Bucket/prefix confinement and bounded hash-verified I/O, through existing telemetry."""

    def __init__(self, source: dict, *, client: Any = None):
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
