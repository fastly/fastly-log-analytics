"""ClickHouse HTTP client, connection pool, OTel metrics, and FOS artifact storage."""

from __future__ import annotations

import hashlib
import json
import math
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
from backend.core.duckdb import _get_fos_client
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
