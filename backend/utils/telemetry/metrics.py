from __future__ import annotations

import itertools
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime
from types import FrameType
from typing import Any

import structlog
from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from backend.utils.bounded_cache import _MISSING
from backend.utils.date_utils import iso_z
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import log_flusher as _log_flusher
from backend.utils.telemetry import proxy_server as _proxy_server
from backend.utils.telemetry.log_flusher import (
    _IN_FLIGHT_LOCK as _IN_FLIGHT_LOCK,
)
from backend.utils.telemetry.log_flusher import (
    _IN_FLIGHT_REQUESTS as _IN_FLIGHT_REQUESTS,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_BATCH_MAX_INTERVAL_S as _LOG_BATCH_MAX_INTERVAL_S,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_BATCH_MAX_ROWS as _LOG_BATCH_MAX_ROWS,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_FLUSHER_LOCK as _LOG_FLUSHER_LOCK,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_FLUSHER_STOP as _LOG_FLUSHER_STOP,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_FLUSHER_THREAD as _LOG_FLUSHER_THREAD,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_QUEUE as _LOG_QUEUE,
)
from backend.utils.telemetry.log_flusher import (
    PUBLISH_STATS as PUBLISH_STATS,
)
from backend.utils.telemetry.log_flusher import (
    ValkeyPublisher as ValkeyPublisher,
)
from backend.utils.telemetry.log_flusher import (
    _ensure_log_flusher_running as _ensure_log_flusher_running,
)
from backend.utils.telemetry.log_flusher import (
    _flush_batch as _flush_batch,
)
from backend.utils.telemetry.log_flusher import (
    _flush_log_writes_for_tests as _flush_log_writes_for_tests,
)
from backend.utils.telemetry.log_flusher import (
    _FlushBarrier as _FlushBarrier,
)
from backend.utils.telemetry.log_flusher import (
    _get_sync_redis as _get_sync_redis,
)
from backend.utils.telemetry.log_flusher import (
    _log_flusher_loop as _log_flusher_loop,
)
from backend.utils.telemetry.log_flusher import (
    _submit_log_write as _submit_log_write,
)
from backend.utils.telemetry.log_flusher import (
    _sync_redis as _sync_redis,
)
from backend.utils.telemetry.log_flusher import (
    _sync_redis_lock as _sync_redis_lock,
)
from backend.utils.telemetry.logging_config import _log_rate_limited as _log_rate_limited
from backend.utils.telemetry.proxy_server import (
    _BOTO3_CALLER_HINT as _BOTO3_CALLER_HINT,
)
from backend.utils.telemetry.proxy_server import (
    _HOP_BY_HOP as _HOP_BY_HOP,
)
from backend.utils.telemetry.proxy_server import (
    _LOOP as _LOOP,
)
from backend.utils.telemetry.proxy_server import (
    _LOOPBACK_HOSTS as _LOOPBACK_HOSTS,
)
from backend.utils.telemetry.proxy_server import (
    _POOL_PER_HOST as _POOL_PER_HOST,
)
from backend.utils.telemetry.proxy_server import (
    _PORT as _PORT,
)
from backend.utils.telemetry.proxy_server import (
    _READY as _READY,
)
from backend.utils.telemetry.proxy_server import (
    _RETRYABLE_CONNECTION_ERRORS as _RETRYABLE_CONNECTION_ERRORS,
)
from backend.utils.telemetry.proxy_server import (
    _RUNNER as _RUNNER,
)
from backend.utils.telemetry.proxy_server import (
    _SERVER_THREAD as _SERVER_THREAD,
)
from backend.utils.telemetry.proxy_server import (
    _SESSION as _SESSION,
)
from backend.utils.telemetry.proxy_server import (
    _START_LOCK as _START_LOCK,
)
from backend.utils.telemetry.proxy_server import (
    _TELEMETRY_HEADERS as _TELEMETRY_HEADERS,
)
from backend.utils.telemetry.proxy_server import (
    _UPSTREAM_KEEPALIVE_S as _UPSTREAM_KEEPALIVE_S,
)
from backend.utils.telemetry.proxy_server import (
    _UPSTREAM_TIMEOUT as _UPSTREAM_TIMEOUT,
)
from backend.utils.telemetry.proxy_server import (
    _UPSTREAM_TIMEOUT_TOTAL_S as _UPSTREAM_TIMEOUT_TOTAL_S,
)
from backend.utils.telemetry.proxy_server import (
    _build_details as _build_details,
)
from backend.utils.telemetry.proxy_server import (
    _build_upstream_headers as _build_upstream_headers,
)
from backend.utils.telemetry.proxy_server import (
    _bust_config_cache as _bust_config_cache,
)
from backend.utils.telemetry.proxy_server import (
    _cdn_host_for as _cdn_host_for,
)
from backend.utils.telemetry.proxy_server import (
    _create_session as _create_session,
)
from backend.utils.telemetry.proxy_server import (
    _handle_request_inner as _handle_request_inner,
)
from backend.utils.telemetry.proxy_server import (
    _is_target_host_allowed as _is_target_host_allowed,
)
from backend.utils.telemetry.proxy_server import (
    _load_config_cached as _load_config_cached,
)
from backend.utils.telemetry.proxy_server import (
    _reset_for_tests as _reset_for_tests,
)
from backend.utils.telemetry.proxy_server import (
    _run_server as _run_server,
)
from backend.utils.telemetry.proxy_server import (
    _scheme_host as _scheme_host,
)
from backend.utils.telemetry.proxy_server import (
    _service_for_target as _service_for_target,
)
from backend.utils.telemetry.proxy_server import (
    _sign_request as _sign_request,
)
from backend.utils.telemetry.proxy_server import (
    _strip_port_for_loopback_check as _strip_port_for_loopback_check,
)
from backend.utils.telemetry.proxy_server import (
    handle_healthz as handle_healthz,
)
from backend.utils.telemetry.proxy_server import (
    handle_request as handle_request,
)
from backend.utils.telemetry.proxy_server import (
    install_boto3_proxy_hook as install_boto3_proxy_hook,
)
from backend.utils.telemetry.proxy_server import (
    proxy_endpoint as proxy_endpoint,
)
from backend.utils.telemetry.proxy_server import (
    start_proxy_server as start_proxy_server,
)
from backend.utils.telemetry.proxy_server import (
    stop_proxy_server as stop_proxy_server,
)
from backend.utils.telemetry.system_stats import (
    _reset_active_requests_for_tests as _reset_active_requests_for_tests,
)

logger = logging.getLogger("backend.utils.telemetry_proxy")
_os = os
_CALLS = globals().get("_CALLS") or ContextVar("_CALLS", default=None)
_QUERIES = globals().get("_QUERIES") or ContextVar("_QUERIES", default=None)
_SQLITE_QUERIES = globals().get("_SQLITE_QUERIES") or ContextVar("_SQLITE_QUERIES", default=None)
_REQUEST_START_TS = globals().get("_REQUEST_START_TS") or ContextVar("_REQUEST_START_TS", default=None)
_IOTHREAD_QUERIED = globals().get("_IOTHREAD_QUERIED") or ContextVar("_IOTHREAD_QUERIED", default=False)
_PROCESS_CONTEXT = globals().get("_PROCESS_CONTEXT") or ContextVar("_PROCESS_CONTEXT", default=None)
_PAGE_LOAD_ID = globals().get("_PAGE_LOAD_ID") or ContextVar("_PAGE_LOAD_ID", default=None)
_RECORDING_TELEMETRY = globals().get("_RECORDING_TELEMETRY") or ContextVar("_RECORDING_TELEMETRY", default=False)
_ACTIVE_CONTEXTS: list[str] = []
_LATEST_PROCESS_CONTEXT: str | None = None
_LATEST_PROCESS_CONTEXT_LOCK = threading.Lock()


def _get_ctx_list(var: ContextVar[list[dict] | None]) -> list[dict]:
    res = var.get()
    if res is None:
        res = []
        var.set(res)
    return res


def _add_span_event(name: str, attributes: dict[str, Any]) -> None:
    try:
        span = trace.get_current_span()
        if span is not None and span.is_recording():
            span.add_event(name=name, attributes=attributes)
    except Exception:
        logger.debug("_add_span_event(%s) failed (ignored)", name, exc_info=True)


def _set_process_context_for_tests(ctx: str | None) -> None:
    global _LATEST_PROCESS_CONTEXT
    _PROCESS_CONTEXT.set(ctx)
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _LATEST_PROCESS_CONTEXT = ctx
        setattr(sys.modules[__name__], "_LATEST_PROCESS_CONTEXT", ctx)


def get_process_context() -> str | None:
    return _PROCESS_CONTEXT.get()


def get_page_load_id() -> str | None:
    return _PAGE_LOAD_ID.get()


def set_page_load_id(page_load_id: str | None) -> None:
    _PAGE_LOAD_ID.set(page_load_id)


def get_process_context_with_fallback() -> str | None:
    ctx = _PROCESS_CONTEXT.get()
    if ctx is not None:
        return ctx
    with _LATEST_PROCESS_CONTEXT_LOCK:
        return _LATEST_PROCESS_CONTEXT


@contextmanager
def process_context_scope(name: str):
    global _LATEST_PROCESS_CONTEXT
    token = _PROCESS_CONTEXT.set(name)
    prev_attribution = None
    attribution_set = False
    try:
        from backend.core.request_telemetry import current_attribution, derive_from_process_context

        attribution = derive_from_process_context(name)
        if attribution is not None:
            prev_attribution = current_attribution.get()
            current_attribution.set(attribution)
            attribution_set = True
    except Exception:
        attribution_set = False
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _ACTIVE_CONTEXTS.append(name)
        _LATEST_PROCESS_CONTEXT = name
        setattr(sys.modules[__name__], "_LATEST_PROCESS_CONTEXT", name)
    try:
        yield
    finally:
        try:
            _PROCESS_CONTEXT.reset(token)
        except ValueError:
            _PROCESS_CONTEXT.set(None)
        if attribution_set:
            try:
                from backend.core.request_telemetry import current_attribution

                current_attribution.set(prev_attribution)
            except Exception:
                logger.debug("process_context_scope: current_attribution.set failed (ignored)", exc_info=True)
        with _LATEST_PROCESS_CONTEXT_LOCK:
            for i in range(len(_ACTIVE_CONTEXTS) - 1, -1, -1):
                if _ACTIVE_CONTEXTS[i] == name:
                    del _ACTIVE_CONTEXTS[i]
                    break
            _LATEST_PROCESS_CONTEXT = _ACTIVE_CONTEXTS[-1] if _ACTIVE_CONTEXTS else None
            setattr(sys.modules[__name__], "_LATEST_PROCESS_CONTEXT", _LATEST_PROCESS_CONTEXT)


def start_call_tracking():
    _CALLS.set([])
    _QUERIES.set([])
    _SQLITE_QUERIES.set([])
    _REQUEST_START_TS.set(time.time())
    _IOTHREAD_QUERIED.set(False)


def get_tracked_calls() -> list[dict]:
    res = _get_ctx_list(_CALLS)
    if not _IOTHREAD_QUERIED.get():
        _IOTHREAD_QUERIED.set(True)
        iothread = _query_iothread_calls_from_usage_log()
        if iothread:
            res = list(res) + iothread
            _CALLS.set(res)
    return res


def _query_iothread_calls_from_usage_log() -> list[dict]:
    try:
        from backend.models.common import _debug_responses_enabled

        if not _debug_responses_enabled():
            return []
        start_ts = _REQUEST_START_TS.get()
        ctx = get_process_context_with_fallback()
        if start_ts is None or not ctx or not ctx.startswith("api:"):
            return []
        from backend import config as svcconfig

        if not svcconfig.is_usage_logging_enabled():
            return []
        sid = svcconfig.get_active_service_id()
        if not sid:
            return []
        from backend.core.metadata import usage_log as _usage_log_db

        start_iso = iso_z(datetime.fromtimestamp(start_ts, UTC))
        try:
            con = _usage_log_db.open_readonly(sid)
        except Exception:
            return []
        try:
            rows = con.execute(
                "SELECT operation_type, url, status, duration_ms, function_name, bytes, operation_class "
                "FROM usage_log "
                "WHERE service_id = ? AND process_context = ? AND timestamp >= ? "
                "ORDER BY timestamp ASC LIMIT 25",
                (sid, ctx, start_iso),
            ).fetchall()
        finally:
            with suppress(Exception):
                con.close()
        return [
            {
                "service": "CDN" if r[6] == "CDN" else "FOS",
                "method": r[0],
                "path": r[1],
                "status": r[2],
                "time_ms": r[3],
                "caller": r[4],
                "bytes": r[5],
                "details": "iothread (via usage_log)",
            }
            for r in rows
        ]
    except Exception as e:
        logger.debug("[telemetry] iothread call query failed: %s", e)
        return []


def get_queries() -> list[dict]:
    return _get_ctx_list(_QUERIES)


def record_sqlite_query(entry: dict) -> None:
    if _RECORDING_TELEMETRY.get():
        return
    entries = _SQLITE_QUERIES.get()
    if entries is not None:
        entries.append(entry)
    record_telemetry_query(
        engine="sqlite",
        sql=entry.get("sql", ""),
        duration_ms=entry.get("time_ms", 0.0),
    )


def get_sqlite_queries() -> list[dict]:
    return _get_ctx_list(_SQLITE_QUERIES)


def record_telemetry_query(
    engine: str,
    sql: str,
    duration_ms: float,
    service_id: str | None = None,
    *,
    request_id: str | None = None,
    allow_active_service_fallback: bool = True,
) -> None:
    if _RECORDING_TELEMETRY.get():
        return
    plid = get_page_load_id()
    if not plid:
        return
    token = _RECORDING_TELEMETRY.set(True)
    try:
        sid = service_id
        if not sid and allow_active_service_fallback:
            from backend import config as svcconfig

            sid = svcconfig.get_active_service_id()
        if sid:
            from backend.core.metadata import usage_log

            usage_log.log_telemetry_query(
                service_id=sid,
                page_load_id=plid,
                request_id=request_id or "unknown",
                engine=engine,
                sql=sql,
                duration_ms=duration_ms,
            )
    except Exception as e:
        logger.debug("Failed to record %s query to telemetry: %s", engine, e)
    finally:
        _RECORDING_TELEMETRY.reset(token)


def debug_envelope(res: dict, *, gated: bool, is_cached: bool | None = False, inject_calls: bool = False) -> dict:
    if not gated:
        res["_debug_queries"] = get_queries()
        res["_debug_sqlite"] = list(get_sqlite_queries())
        res["_debug_calls"] = get_tracked_calls()
        res["_is_cached"] = is_cached
        return res
    from backend.models.common import _debug_responses_enabled

    out = dict(res)
    if is_cached is not None:
        out["_is_cached"] = is_cached
    if not _debug_responses_enabled():
        for key in ("_debug_queries", "_debug_calls", "debug_queries", "debug_calls"):
            out.pop(key, None)
    elif inject_calls:
        out["_debug_calls"] = get_tracked_calls()
    return out


def clear_queries():
    _QUERIES.set([])


def record_call(
    method: str,
    path: str,
    time_ms: float,
    status: int | str | None = None,
    service: str = "Fastly API",
    details: str | None = None,
    caller: str | None = None,
    bytes_count: int | None = None,
):
    if caller is None:
        try:
            frame: FrameType | None = sys._getframe(1)
            while frame:
                code = getattr(frame, "f_code", None)
                if not code:
                    break
                fn = code.co_filename
                if "telemetry.py" in fn or "/telemetry/" in fn or "contextlib.py" in fn or "duckdb.py" in fn:
                    frame = frame.f_back
                    continue
                caller = code.co_name
                break
        except Exception:
            logger.debug("record_call: caller walk failed (ignored)", exc_info=True)
    calls = _get_ctx_list(_CALLS)
    calls.append(
        {
            "service": service,
            "method": method,
            "path": path,
            "time_ms": round(time_ms, 2),
            "status": status,
            "details": details,
            "caller": caller,
            "bytes": bytes_count,
        }
    )
    attrs: dict[str, Any] = {
        "app.call.method": method,
        "app.call.path": path,
        "app.call.time_ms": float(time_ms),
        "app.call.service": service,
    }
    if status is not None:
        attrs["app.call.status"] = str(status)
    if details:
        attrs["app.call.details"] = details
    if caller:
        attrs["app.call.caller"] = caller
    if bytes_count is not None:
        attrs["app.call.bytes"] = int(bytes_count)
    _add_span_event("external_call", attrs)


class track_query:
    def __init__(self, con, query: str, params: list, label: str = "query"):
        self.con = con
        self.query = query
        self.params = params
        self.label = label
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        return self.con.execute(self.query, self.params)

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = round((time.time() - self.t0) * 1000, 2)
        sql_stripped = self.query.strip()
        get_queries().append({"sql": sql_stripped, "time_ms": elapsed, "engine": "DuckDB"})
        record_telemetry_query(engine="duckdb", sql=sql_stripped, duration_ms=elapsed)
        _add_span_event(
            "db.query",
            {
                "db.statement": sql_stripped[:4000],
                "db.elapsed_ms": float(elapsed),
                "db.label": self.label,
            },
        )


def is_full_miss(x_cache: str | None) -> bool:
    if not x_cache:
        return False
    parts = [p.strip().upper() for p in x_cache.split(",") if p.strip()]
    return bool(parts) and all(p in ("MISS", "PASS") for p in parts)


def build_cdn_miss_synth_details(bytes_count: int | None) -> str:
    if bytes_count is not None:
        return f"{bytes_count:,} bytes · Class B · synthesized from CDN MISS"
    return "Class B · synthesized from CDN MISS"


def record_cdn_call(
    method: str,
    key: str,
    elapsed_ms: float,
    headers: dict | None = None,
    bytes_count: int | None = None,
    caller: str | None = None,
    status: str | int | None = "OK",
):
    x_cache = age = cache_control = None
    if headers is not None:
        try:
            x_cache = headers.get("X-Cache") or headers.get("x-cache")
            age = headers.get("Age") or headers.get("age")
            cache_control = headers.get("Cache-Control") or headers.get("cache-control")
        except Exception:
            logger.debug("record_cdn_call: headers.get failed (ignored)", exc_info=True)
    detail_parts = [p for p in (x_cache, f"age={age}s" if age else None, cache_control) if p]
    cdn_details = " · ".join(detail_parts) if detail_parts else None
    record_call(
        method,
        key,
        elapsed_ms,
        status=status,
        service="CDN",
        details=cdn_details,
        caller=caller,
        bytes_count=bytes_count,
    )
    if is_full_miss(x_cache):
        record_call(
            "GET_OBJECT",
            key,
            elapsed_ms,
            status=status,
            service="FOS",
            details=build_cdn_miss_synth_details(bytes_count),
            caller=caller or "cdn.miss",
            bytes_count=bytes_count,
        )


class tracked_call:
    def __init__(self, method: str, path: str, service: str = "Fastly API", details: str | None = None):
        self.method = method
        self.path = path
        self.service = service
        self.details = details
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (time.time() - self.t0) * 1000
        record_call(
            self.method,
            self.path,
            elapsed,
            status="Error" if exc_type else "OK",
            service=self.service,
            details=self.details,
        )


def _load_service_source(service_id: str) -> dict | None:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    return svcconfig.config_to_source(cfg) if cfg else None


def flush_usage_log(service_id: str) -> None:
    try:
        from backend import config as svcconfig

        if not svcconfig.is_usage_logging_enabled():
            return
        calls = _CALLS.get()
        if not calls:
            return
        source = _load_service_source(service_id)
        if not source:
            return
        from backend.core.duckdb import log_usage_calls

        log_usage_calls(source, calls, process_context=get_process_context())
    except Exception as e:
        logger.warning("[usage_logger] flush failed: %s", e)


def run_usage_log_cleanup(service_id: str) -> None:
    try:
        source = _load_service_source(service_id)
        if not source:
            return
        from backend.core.duckdb import purge_usage_log

        purge_usage_log(source)
    except Exception as e:
        logger.warning("[usage_logger] cleanup failed: %s", e)


_BUFFER_CAP = 1000
_SQL_TRUNCATE = 4096
_buffer: deque[dict[str, Any]] = deque(maxlen=_BUFFER_CAP)
_seq = itertools.count(1)
_dropped = 0


def _record(sql: str, params: Any, duration_ms: float, rowcount: int, op: str) -> None:
    global _dropped
    try:
        if _buffer.maxlen and len(_buffer) == _buffer.maxlen:
            _dropped += 1
            setattr(sys.modules[__name__], "_dropped", _dropped)
        entry = {
            "seq": next(_seq),
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "sql": _summarize_sql(sql),
            "params_kind": _describe_params(params),
            "time_ms": round(duration_ms, 3),
            "rows": rowcount,
            "op": op,
        }
        _buffer.append(entry)
        record_sqlite_query(entry)
    except Exception:
        logger.debug("statement_profiler record failed", exc_info=True)


def _summarize_sql(sql: str) -> str:
    if not isinstance(sql, str):
        sql = str(sql)
    if len(sql) > _SQL_TRUNCATE:
        return sql[:_SQL_TRUNCATE] + f"… [+{len(sql) - _SQL_TRUNCATE} chars]"
    return sql


def _describe_params(params: Any) -> str:
    if params is None:
        return "none"
    if isinstance(params, (list, tuple)):
        return f"seq[{len(params)}]"
    if isinstance(params, dict):
        return f"map[{len(params)}]"
    return type(params).__name__


def _live_register(db_type: str, sql: Any, con: Any) -> int:
    try:
        from backend.core.request_telemetry import query_registry

        qid = query_registry.register(db_type, str(sql), service_id=getattr(con, "_service_id", None), con=con)
        if qid >= 0:
            structlog.contextvars.bind_contextvars(query_id=qid)
        return qid
    except Exception:
        logger.debug("live-registry register failed", exc_info=True)
        return -1


def _live_deregister(qid: int, error: BaseException | None) -> None:
    if qid < 0:
        return
    try:
        from backend.core.request_telemetry import query_registry

        query_registry.deregister(qid, error=error)
    except Exception:
        logger.debug("live-registry deregister failed", exc_info=True)
    finally:
        with suppress(Exception):
            structlog.contextvars.unbind_contextvars("query_id")


def get_recent(limit: int = 200, since_seq: int = 0) -> dict[str, Any]:
    snapshot = [e for e in _buffer if e["seq"] > since_seq] if since_seq > 0 else list(_buffer)
    if limit and len(snapshot) > limit:
        snapshot = snapshot[-limit:]
    return {
        "queries": snapshot,
        "buffer_size": len(_buffer),
        "buffer_cap": _BUFFER_CAP,
        "dropped": _dropped,
        "last_seq": _buffer[-1]["seq"] if _buffer else 0,
    }


def clear() -> None:
    global _dropped
    _buffer.clear()
    _dropped = 0
    setattr(sys.modules[__name__], "_dropped", 0)


def buffer_stats() -> dict[str, int]:
    return {"buffer_size": len(_buffer), "buffer_cap": _BUFFER_CAP, "dropped": _dropped}


clear_recent_sqlite_queries = clear
get_recent_sqlite_queries = get_recent
get_sqlite_stats = buffer_stats


def get_page_telemetry(page_load_id: str | None = None) -> dict[str, Any]:
    from backend import config as svcconfig
    from backend.core.metadata import usage_log

    sid = svcconfig.get_active_service_id()
    if not sid or not page_load_id:
        return {"queries": [], "sections": [], "calls": []}
    return usage_log.get_page_telemetry(sid, page_load_id)


if "pytest" not in sys.modules:
    logger.info("statement_profiler active — capturing up to %d recent statements", _BUFFER_CAP)


_JSON_CONTENT_TYPES: tuple[str, ...] = ("application/json",)
_STREAMING_CONTENT_TYPES: tuple[str, ...] = (
    "text/event-stream",
    "application/octet-stream",
    "application/x-ndjson",
    "application/jsonl",
)


def _content_type(response: Response) -> str:
    return (response.headers.get("content-type") or response.media_type or "").lower()


def _is_json_response(response: Response) -> bool:
    media = _content_type(response)
    return any(media.startswith(t) for t in _JSON_CONTENT_TYPES)


def _is_streaming_content_type(response: Response) -> bool:
    media = _content_type(response)
    return any(media.startswith(t) for t in _STREAMING_CONTENT_TYPES)


class TelemetryResponseBodyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        try:
            from backend.models.common import _DEBUG_ENVELOPE_KEYS, _debug_responses_enabled
        except Exception:
            return response
        master_enabled = _debug_responses_enabled()
        is_remote = getattr(request.state, "is_remote", False)
        analyst_query_enabled = analyst_api_enabled = False
        if is_remote and master_enabled:
            from backend.utils.remote_access import analyst_debug_visibility

            analyst_query_enabled, analyst_api_enabled = analyst_debug_visibility(request)
        if _is_streaming_content_type(response) or not _is_json_response(response):
            return response
        opt_in = master_enabled and (
            request.headers.get("x-debug-responses") == "1" or _os.getenv("DEBUG_RESPONSES_FORCE_INCLUDE") == "1"
        )
        if is_remote and opt_in and getattr(request.state, "_analyst_envelope_complete", False):
            return response
        try:
            body = b"".join([chunk async for chunk in response.body_iterator])
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to read response body: %s", e)
            return response
        if not body:
            return _reconstruct(response, body)
        try:
            parsed: Any = json.loads(body)
        except (ValueError, json.JSONDecodeError) as e:
            logger.debug("[telemetry-middleware] body not JSON-parseable: %s", e)
            return _reconstruct(response, body)
        if not isinstance(parsed, dict):
            return _reconstruct(response, body)
        if not opt_in:
            keys_to_strip = (*_DEBUG_ENVELOPE_KEYS, "_is_cached") if master_enabled else _DEBUG_ENVELOPE_KEYS
            stripped = [parsed.pop(k, _MISSING) for k in keys_to_strip]
            if all(v is _MISSING for v in stripped):
                return _reconstruct(response, body)
            try:
                return _reconstruct(response, json.dumps(parsed, default=str).encode("utf-8"))
            except Exception as e:
                logger.warning("[telemetry-middleware] failed to strip telemetry: %s", e)
                return _reconstruct(response, body)
        if "_debug_queries" in parsed:
            return _reconstruct(response, body)
        try:
            inject_queries = analyst_query_enabled if is_remote else True
            inject_api_calls = analyst_api_enabled if is_remote else True
            if inject_queries:
                parsed["_debug_queries"] = get_queries()
                pg_queries = list(get_sqlite_queries())
                parsed["_debug_postgres"] = pg_queries
                parsed["_debug_sqlite"] = pg_queries
            if inject_api_calls:
                parsed["_debug_calls"] = get_tracked_calls()
            if not is_remote or inject_queries or inject_api_calls:
                parsed.setdefault("_is_cached", False)
            return _reconstruct(response, json.dumps(parsed, default=str).encode("utf-8"))
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to inject telemetry: %s", e)
            return _reconstruct(response, body)


def _reconstruct(original: Response, body: bytes) -> Response:
    drop = (b"content-length", b"content-encoding", b"content-type")
    media_type = original.media_type
    if media_type is None:
        for k, v in original.raw_headers:
            if k.lower() == b"content-type":
                with suppress(UnicodeDecodeError):
                    media_type = v.decode("ascii")
                break
    new = Response(content=body, status_code=original.status_code, media_type=media_type)
    new.raw_headers.extend((k, v) for k, v in original.raw_headers if k.lower() not in drop)
    return new


install_shim_module(
    __name__,
    (_proxy_server, _log_flusher),
    extra_module_names=("backend.utils.telemetry",),
    skip_attrs=(
        "logging_config",
        "metrics",
        "system_stats",
        "proxy_server",
        "log_flusher",
        "_logging_config",
        "_metrics",
        "_system_stats",
        "_proxy_server",
        "_log_flusher",
        "_self",
    ),
    delegated_attrs={
        "_sync_redis": _log_flusher,
        "_LOG_FLUSHER_THREAD": _log_flusher,
        "_IN_FLIGHT_REQUESTS": _proxy_server,
        "_PORT": _proxy_server,
        "_SERVER_THREAD": _proxy_server,
        "_RUNNER": _proxy_server,
        "_LOOP": _proxy_server,
        "_SESSION": _proxy_server,
        "_UPSTREAM_TIMEOUT_TOTAL_S": _proxy_server,
        "_UPSTREAM_TIMEOUT": _proxy_server,
        "_UPSTREAM_KEEPALIVE_S": _proxy_server,
    },
)
