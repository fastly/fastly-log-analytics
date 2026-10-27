from __future__ import annotations

import asyncio
import contextvars
import itertools
import json
import logging
import os
import queue
import sys
import threading
import time
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime
from types import FrameType
from typing import Any

import aiohttp
import structlog
import yarl
from aiohttp import web
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from backend.utils.bounded_cache import _MISSING
from backend.utils.date_utils import iso_z
from backend.utils.telemetry.logging_config import _log_rate_limited
from backend.utils.telemetry.system_stats import _reset_active_requests_for_tests

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
        logger.debug("sqlite_profiler record failed", exc_info=True)


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
    logger.info("sqlite_profiler active — capturing up to %d recent statements", _BUFFER_CAP)
PUBLISH_STATS: dict[str, int] = {"published": 0, "publish_errors": 0, "decode_errors": 0}
_sync_redis_lock = threading.Lock()
_sync_redis: Any = None


def _get_sync_redis() -> Any:
    global _sync_redis
    with _sync_redis_lock:
        if _sync_redis is None:
            from redis import Redis as SyncRedis

            broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            _sync_redis = SyncRedis.from_url(
                broker_url, socket_connect_timeout=1.0, socket_timeout=2.0, decode_responses=True
            )
        return _sync_redis


class ValkeyPublisher:
    def __init__(self, channel_prefix: str = "sync-status", history_size: int = 0) -> None:
        self._channel_prefix = channel_prefix
        self._history_size = history_size
        self._loop: asyncio.AbstractEventLoop | None = None
        self._redis: Any = None

    def _channel(self, service_id: str) -> str:
        return f"sse:{self._channel_prefix}:{service_id}"

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        from redis.asyncio import Redis

        self._loop = loop
        self._redis = Redis.from_url(
            os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"), decode_responses=True
        )

    def publish(self, service_id: str, payload: dict[str, Any]) -> None:
        payload_json = json.dumps(payload)
        channel = self._channel(service_id)
        if self._loop is not None and self._redis is not None:
            self._publish_async(channel, payload_json)
        else:
            self._publish_sync(channel, payload_json)

    def _apply_history(self, pipe: Any, channel: str, payload_json: str) -> None:
        if self._history_size > 0:
            history_key = f"{channel}:history"
            pipe.rpush(history_key, payload_json)
            pipe.ltrim(history_key, -self._history_size, -1)

    def _build_publish_pipe(self, client: Any, channel: str, payload_json: str) -> Any:
        pipe = client.pipeline()
        pipe.publish(channel, payload_json)
        self._apply_history(pipe, channel, payload_json)
        return pipe

    def _publish_sync(self, channel: str, payload_json: str) -> None:
        try:
            self._build_publish_pipe(_get_sync_redis(), channel, payload_json).execute()
            PUBLISH_STATS["published"] += 1
        except Exception as e:
            PUBLISH_STATS["publish_errors"] += 1
            _log_rate_limited("publish_sync", "[valkey] publish to %s failed (sync): %s", channel, e)

    def _publish_async(self, channel: str, payload_json: str) -> None:
        async def _do() -> None:
            try:
                if self._redis is None:
                    raise RuntimeError("self._redis is unexpectedly None")
                await self._build_publish_pipe(self._redis, channel, payload_json).execute()
                PUBLISH_STATS["published"] += 1
            except Exception as e:
                PUBLISH_STATS["publish_errors"] += 1
                _log_rate_limited("publish_async", "[valkey] publish to %s failed: %s", channel, e)

        try:
            if self._loop is None:
                raise RuntimeError("self._loop is unexpectedly None")
            self._loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_do()))
        except RuntimeError as e:
            _log_rate_limited("loop_closed", "[valkey] loop unavailable (%s); publishing sync", e)
            self._publish_sync(channel, payload_json)

    async def subscribe(self, service_id: str) -> AsyncIterator[dict[str, Any]]:
        if self._redis is None:
            raise RuntimeError(
                "ValkeyPublisher.subscribe() before bind_loop() — the SSE process "
                "must bind the publisher during lifespan startup"
            )
        channel = self._channel(service_id)
        backoff = 1.0
        while True:
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(channel)
                backoff = 1.0
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    try:
                        yield json.loads(message["data"])
                    except (TypeError, ValueError) as e:
                        PUBLISH_STATS["decode_errors"] += 1
                        _log_rate_limited("decode", "[valkey] undecodable payload on %s: %s", channel, e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log_rate_limited("subscribe", "[valkey] subscription to %s lost (%s); reconnecting", channel, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 15.0)
            finally:
                try:
                    await pubsub.unsubscribe(channel)
                    await pubsub.close()
                except Exception:
                    logger.debug("subscribe: pubsub.close failed (ignored)", exc_info=True)

    def get_recent_ticks(self, service_id: str, count: int = 60) -> list[dict[str, Any]]:
        if self._history_size == 0:
            return []
        try:
            items = _get_sync_redis().lrange(f"{self._channel(service_id)}:history", -count, -1)
            return [json.loads(item) for item in items]
        except Exception as e:
            _log_rate_limited("history", "[valkey] history read failed for %s: %s", service_id, e)
            return []

    def subscriber_count(self, service_id: str) -> int:
        try:
            result = _get_sync_redis().pubsub_numsub(self._channel(service_id))
            if result:
                return int(result[0][1])
        except Exception as e:
            _log_rate_limited("numsub", "[valkey] subscriber_count failed for %s: %s", service_id, e)
        return 0


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


# ── Local HTTP S3/CDN telemetry proxy (folded from telemetry_proxy.py) ──

# Per-call caller-hint override. When set, the boto3 before-send hook
# (install_boto3_proxy_hook) writes this value into X-Telemetry-Caller
# instead of the default ``boto3.<op>``. Callers that want their hint to
# survive across the boto3 paginator's internal threading should keep the
# token in scope for the entire paginate() iteration — see
# backend/core/duckdb.py:_ProxyPaginatorShim.
_BOTO3_CALLER_HINT: contextvars.ContextVar[str | None] = contextvars.ContextVar("fos_boto3_caller_hint", default=None)


# Module-level state (intentionally process-singleton; reset in tests via
# _reset_for_tests()).
_PORT: int | None = None
_SERVER_THREAD: threading.Thread | None = None
_RUNNER: web.AppRunner | None = None
_LOOP: asyncio.AbstractEventLoop | None = None
_SESSION: aiohttp.ClientSession | None = None
_READY = threading.Event()
_START_LOCK = threading.Lock()

_UPSTREAM_TIMEOUT_TOTAL_S = float(os.getenv("FOS_PROXY_UPSTREAM_TIMEOUT_S", "90"))
_UPSTREAM_TIMEOUT = aiohttp.ClientTimeout(
    total=_UPSTREAM_TIMEOUT_TOTAL_S,
    sock_connect=5,
    sock_read=60,
)

_POOL_PER_HOST = 128

_UPSTREAM_KEEPALIVE_S = float(os.getenv("FOS_PROXY_KEEPALIVE_S", "4"))

_RETRYABLE_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientConnectionResetError,
    aiohttp.ClientOSError,
)

_LOG_BATCH_MAX_ROWS = 50
_LOG_BATCH_MAX_INTERVAL_S = 0.1
_LOG_QUEUE: queue.Queue = queue.Queue()
_LOG_FLUSHER_STOP = threading.Event()
_LOG_FLUSHER_THREAD: threading.Thread | None = None
_LOG_FLUSHER_LOCK = threading.Lock()


class _FlushBarrier:
    __slots__ = ("event",)

    def __init__(self) -> None:
        self.event = threading.Event()


_IN_FLIGHT_REQUESTS = 0
_IN_FLIGHT_LOCK = threading.Lock()


def _flush_batch(items: list[tuple[str, dict, str | None]]) -> None:
    """Group queued rows by (service_id, process_context) and call
    log_usage_calls once per group so each group is a single executemany."""
    if not items:
        return
    grouped: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
    for service_id, row, ctx in items:
        grouped[(service_id, ctx)].append(row)
    for (service_id, ctx), rows in grouped.items():
        try:
            from backend.core import metadata as metadata_db

            metadata_db.log_usage_calls(service_id, rows, process_context=ctx)
        except Exception as e:
            # Telemetry must NEVER break the request path it's tracking.
            logger.debug("[telemetry-proxy] batched usage log failed (%d rows): %s", len(rows), e)


def _log_flusher_loop(log_queue: queue.Queue, stop_event: threading.Event) -> None:
    pending: list[tuple[str, dict, str | None]] = []
    deadline = time.monotonic() + _LOG_BATCH_MAX_INTERVAL_S
    while True:
        timeout = max(0.001, deadline - time.monotonic())
        try:
            item = log_queue.get(timeout=timeout)
        except queue.Empty:
            item = None  # interval timed out, no new row

        if isinstance(item, _FlushBarrier):
            _flush_batch(pending)
            pending = []
            deadline = time.monotonic() + _LOG_BATCH_MAX_INTERVAL_S
            item.event.set()
            if stop_event.is_set():
                # Drain anything left and exit.
                while True:
                    try:
                        leftover = log_queue.get_nowait()
                    except queue.Empty:
                        break
                    if not isinstance(leftover, _FlushBarrier):
                        pending.append(leftover)
                    else:
                        leftover.event.set()
                _flush_batch(pending)
                return
            continue

        if item is not None:
            pending.append(item)

        if pending and (len(pending) >= _LOG_BATCH_MAX_ROWS or time.monotonic() >= deadline):
            _flush_batch(pending)
            pending = []
            deadline = time.monotonic() + _LOG_BATCH_MAX_INTERVAL_S


def _ensure_log_flusher_running() -> None:
    global _LOG_FLUSHER_THREAD
    if _LOG_FLUSHER_THREAD is not None and _LOG_FLUSHER_THREAD.is_alive():
        return
    with _LOG_FLUSHER_LOCK:
        if _LOG_FLUSHER_THREAD is not None and _LOG_FLUSHER_THREAD.is_alive():
            return
        _LOG_FLUSHER_STOP.clear()
        _LOG_FLUSHER_THREAD = threading.Thread(
            target=_log_flusher_loop,
            args=(_LOG_QUEUE, _LOG_FLUSHER_STOP),
            name="proxy-log-flusher",
            daemon=True,
        )
        _LOG_FLUSHER_THREAD.start()


def _submit_log_write(service_id: str, row: dict, process_context: str | None) -> None:
    _ensure_log_flusher_running()
    _LOG_QUEUE.put((service_id, row, process_context))


def _flush_log_writes_for_tests(timeout: float = 15.0) -> None:
    """Block until all in-flight proxy requests finish their handler AND
    all queued log rows are written (or `timeout`)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with _IN_FLIGHT_LOCK:
            in_flight = _IN_FLIGHT_REQUESTS
        if in_flight == 0:
            break
        time.sleep(0.005)
    if _LOG_FLUSHER_THREAD is not None and _LOG_FLUSHER_THREAD.is_alive():
        barrier = _FlushBarrier()
        _LOG_QUEUE.put(barrier)
        barrier.event.wait(timeout=timeout)


# RFC 7230 §6.1 hop-by-hop headers — must not be forwarded by a proxy.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)
# Our own telemetry headers — strip so they don't propagate to upstream.
_TELEMETRY_HEADERS = frozenset(
    {
        "x-fos-target",
        "x-telemetry-context",
        "x-telemetry-caller",
        "x-telemetry-service-id",
    }
)


def _load_config_cached(service_id: str) -> dict | None:
    from backend import config as _config

    return _config.load_config(service_id)


def _bust_config_cache(service_id: str | None = None) -> None:
    from backend import config as _config

    with _config._config_cache_lock:
        if service_id is None:
            _config._config_cache.clear()
        else:
            _config._config_cache.pop(service_id, None)


def _scheme_host(value: str) -> str:
    return value.replace("https://", "").replace("http://", "").split("/", 1)[0].lower()


def _cdn_host_for(cfg: dict) -> str | None:
    cdn_url = (cfg.get("cdn_url") or "").strip()
    if not cdn_url:
        return None
    return _scheme_host(cdn_url)


_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "0.0.0.0"})


def _strip_port_for_loopback_check(target_host: str) -> str:
    if target_host.startswith("[") and "]" in target_host:
        return target_host[1 : target_host.index("]")]
    if target_host.count(":") == 1:
        return target_host.split(":", 1)[0]
    return target_host


def _is_target_host_allowed(target_host: str, cdn_host: str | None, fos_native: str | None) -> bool:
    t = target_host.lower()
    if _strip_port_for_loopback_check(t) in _LOOPBACK_HOSTS:
        return True
    if cdn_host and t == cdn_host:
        return True
    if fos_native and t == fos_native.lower():
        return True
    return False


def _sign_request(method: str, url: str, headers: dict, body: bytes, service_id: str) -> dict:
    cfg = _load_config_cached(service_id)
    if not cfg:
        logger.warning(
            "[telemetry-proxy] cannot sign %s %s: no config for service_id=%r — forwarding UNSIGNED (will 403)",
            method,
            url,
            service_id,
        )
        return headers

    target_host = _scheme_host(url)
    cdn_host = _cdn_host_for(cfg)
    if cdn_host and target_host == cdn_host:
        if cfg.get("cdn_secret"):
            headers["x-fastly-key"] = cfg["cdn_secret"]
        return headers

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    region = cfg.get("fos_region", "us-east-1")
    if not access_key or not secret_key:
        logger.warning(
            "[telemetry-proxy] cannot sign %s %s: service_id=%r is missing fos_access_key_id/fos_secret_access_key"
            " — forwarding UNSIGNED (will 403)",
            method,
            url,
            service_id,
        )
        return headers

    if (
        ("?delete" in url or b"<Delete" in body)
        and body
        and "Content-MD5" not in headers
        and "content-md5" not in headers
    ):
        import base64
        import hashlib

        headers["Content-MD5"] = base64.b64encode(
            hashlib.md5(body, usedforsecurity=False).digest()  # nosec B324
        ).decode("ascii")

    credentials = Credentials(access_key, secret_key)
    aws_req = AWSRequest(method=method, url=url, headers=headers, data=body)
    S3SigV4Auth(credentials, "s3", region).add_auth(aws_req)
    headers.update(dict(aws_req.headers))
    return headers


def _service_for_target(target_host: str, cdn_host: str | None) -> str:
    return "CDN" if cdn_host and target_host == cdn_host else "FOS"


def _build_details(x_cache: str | None, caller: str) -> str:
    parts: list[str] = []
    if x_cache:
        parts.append(x_cache)
    parts.append(caller)
    return " · ".join(parts)


def _build_upstream_headers(client_headers) -> dict:
    out: dict[str, str] = {}
    for k, v in client_headers.items():
        kl = k.lower()
        if kl in _HOP_BY_HOP or kl in _TELEMETRY_HEADERS or kl == "host":
            continue
        out[k] = v
    return out


async def handle_healthz(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def handle_request(request: web.Request) -> web.StreamResponse:
    global _IN_FLIGHT_REQUESTS
    with _IN_FLIGHT_LOCK:
        _IN_FLIGHT_REQUESTS += 1
    try:
        return await _handle_request_inner(request)
    finally:
        with _IN_FLIGHT_LOCK:
            _IN_FLIGHT_REQUESTS -= 1


async def _handle_request_inner(request: web.Request) -> web.StreamResponse:
    target_host = request.headers.get("X-Fos-Target")
    if not target_host:
        return web.Response(status=400, text="Missing X-Fos-Target header")

    service_id = request.headers.get("X-Telemetry-Service-Id")
    is_mutation = request.method in ("PUT", "POST", "DELETE")
    if service_id and is_mutation:
        _cfg = _load_config_cached(service_id)
        if _cfg:
            _fos_native = (_cfg.get("fos_native_endpoint") or _cfg.get("fos_endpoint") or "").strip()
            if _fos_native:
                target_host = _fos_native.replace("https://", "").replace("http://", "").rstrip("/")

    if target_host.startswith(("http://", "https://")):
        upstream_url = f"{target_host.rstrip('/')}{request.path_qs}"
        target_host_for_classify = target_host.replace("https://", "").replace("http://", "").rstrip("/").lower()
    else:
        upstream_url = f"https://{target_host}{request.path_qs}"
        target_host_for_classify = target_host.lower()
    out_headers = _build_upstream_headers(request.headers)
    service_id = request.headers.get("X-Telemetry-Service-Id")
    caller = request.headers.get("X-Telemetry-Caller", "telemetry-proxy")
    process_context = request.headers.get("X-Telemetry-Context")
    cdn_host: str | None = None
    fos_native: str | None = None
    cfg: dict | None = None
    if service_id:
        cfg = _load_config_cached(service_id)
        if cfg:
            cdn_host = _cdn_host_for(cfg)
            fos_native = (cfg.get("fos_native_endpoint") or cfg.get("fos_endpoint") or "").strip() or None

    if service_id and cfg is not None and not _is_target_host_allowed(target_host_for_classify, cdn_host, fos_native):
        logger.warning(
            "[telemetry-proxy] rejected disallowed X-Fos-Target=%r for service_id=%r (cdn=%r fos=%r)",
            target_host_for_classify,
            service_id,
            cdn_host,
            fos_native,
        )
        return web.Response(status=400, text="X-Fos-Target not allowed for service")

    data: Any
    if service_id and request.can_read_body:
        body = await request.read()
        data = body
        out_headers = _sign_request(request.method, upstream_url, out_headers, body, service_id)
    elif service_id:
        out_headers = _sign_request(request.method, upstream_url, out_headers, b"", service_id)
        data = None
    else:
        logger.warning(
            "[telemetry-proxy] forwarding UNSIGNED %s %s: no X-Telemetry-Service-Id header (caller=%s, will 403 against FOS)",
            request.method,
            upstream_url,
            caller,
        )
        data = request.content if request.can_read_body else None

    t0 = time.time()
    bytes_received = 0
    upstream_status: int | None = None
    x_cache: str | None = None
    proxy_resp: web.StreamResponse | web.Response
    client_response_started = False
    _IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")
    attempt = 0

    try:
        while True:
            try:
                wire_url = yarl.URL(upstream_url, encoded=True)
                if _SESSION is None:
                    raise RuntimeError("telemetry-proxy session not initialised")
                async with _SESSION.request(
                    method=request.method,
                    url=wire_url,
                    headers=out_headers,
                    data=data,
                    allow_redirects=False,
                ) as upstream_resp:
                    upstream_status = upstream_resp.status
                    x_cache = upstream_resp.headers.get("X-Cache")
                    proxy_resp = web.StreamResponse(
                        status=upstream_resp.status,
                        headers={k: v for k, v in upstream_resp.headers.items() if k.lower() not in _HOP_BY_HOP},
                    )
                    await proxy_resp.prepare(request)
                    client_response_started = True
                    is_head = request.method == "HEAD"
                    try:
                        async for chunk in upstream_resp.content.iter_chunked(65536):
                            bytes_received += len(chunk)
                            if not is_head:
                                await proxy_resp.write(chunk)
                        await proxy_resp.write_eof()
                    except ConnectionResetError as ce:
                        logger.warning(
                            "[telemetry-proxy] client disconnect mid-stream for %s %s after %d bytes: %s: %s",
                            request.method,
                            upstream_url,
                            bytes_received,
                            type(ce).__name__,
                            ce,
                        )
                    return proxy_resp
            except TimeoutError:
                upstream_status = 504
                logger.error("[telemetry-proxy] upstream timeout for %s %s", request.method, upstream_url)
                return web.Response(status=504, text="Upstream Timeout")
            except _RETRYABLE_CONNECTION_ERRORS as e:
                if attempt == 0 and not client_response_started and request.method in _IDEMPOTENT_METHODS:
                    attempt += 1
                    bytes_received = 0
                    logger.debug(
                        "[telemetry-proxy] retrying %s %s after connection error: %s",
                        request.method,
                        upstream_url,
                        e,
                    )
                    continue
                upstream_status = 502
                logger.error("[telemetry-proxy] upstream request failed: %s", e)
                return web.Response(status=502, text="Bad Gateway")
            except Exception as e:
                upstream_status = 502
                logger.error("[telemetry-proxy] upstream request failed: %s", e)
                return web.Response(status=502, text="Bad Gateway")
    finally:
        if service_id:
            elapsed_ms = round((time.time() - t0) * 1000, 2)
            service = _service_for_target(target_host_for_classify, cdn_host)
            status_str = "OK" if (upstream_status and 200 <= upstream_status < 300) else f"HTTP {upstream_status}"
            billing_method = request.method
            if service == "FOS" and request.method == "GET" and "list-type=" in request.query_string:
                billing_method = "LIST_OBJECTS_V2"
            row = {
                "method": billing_method,
                "path": request.path_qs,
                "bytes": bytes_received,
                "status": status_str,
                "service": service,
                "details": _build_details(x_cache, caller),
                "caller": caller,
                "time_ms": elapsed_ms,
            }
            _submit_log_write(service_id, row, process_context)

            if (
                service == "FOS"
                and request.method in ("GET", "HEAD")
                and (process_context or "").startswith("api:")
                and not any(h in (process_context or "") for h in ("/api/admin/health", "/api/health"))
            ):
                logger.warning(
                    "[telemetry-proxy] dashboard context hitting FOS: %s %s ctx=%s",
                    request.method,
                    request.path_qs,
                    process_context,
                )

            from backend.utils.telemetry import build_cdn_miss_synth_details, is_full_miss

            if service == "CDN" and is_full_miss(x_cache):
                synth_row = {
                    "method": "GET_OBJECT",
                    "path": request.path_qs,
                    "bytes": bytes_received,
                    "status": status_str,
                    "service": "FOS",
                    "details": build_cdn_miss_synth_details(bytes_received or None),
                    "caller": "cdn.miss",
                    "time_ms": elapsed_ms,
                }
                _submit_log_write(service_id, synth_row, process_context)


async def _create_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=256,
            limit_per_host=_POOL_PER_HOST,
            enable_cleanup_closed=True,
            keepalive_timeout=_UPSTREAM_KEEPALIVE_S,
        ),
        timeout=_UPSTREAM_TIMEOUT,
    )


def _run_server() -> None:
    global _PORT, _RUNNER, _LOOP, _SESSION

    _LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_LOOP)

    _SESSION = _LOOP.run_until_complete(_create_session())

    app = web.Application(client_max_size=4 * 1024 * 1024 * 1024)
    app.router.add_get("/healthz", handle_healthz)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    _RUNNER = web.AppRunner(app, access_log=None)
    _LOOP.run_until_complete(_RUNNER.setup())

    site = web.TCPSite(_RUNNER, "127.0.0.1", 0)
    _LOOP.run_until_complete(site.start())

    _server = site._server
    if _server is None:
        raise RuntimeError("_server is unexpectedly None")
    _PORT = _server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    _READY.set()

    _LOOP.run_forever()


def start_proxy_server() -> None:
    global _SERVER_THREAD
    if _PORT is not None and _SERVER_THREAD is not None and _SERVER_THREAD.is_alive():
        return
    with _START_LOCK:
        if _SERVER_THREAD is None or not _SERVER_THREAD.is_alive():
            _READY.clear()
            _SERVER_THREAD = threading.Thread(target=_run_server, daemon=True, name="telemetry-proxy")
            _SERVER_THREAD.start()
    if not _READY.wait(timeout=2.0):
        raise RuntimeError("telemetry proxy failed to start within 2s")


def stop_proxy_server() -> None:
    global _RUNNER, _LOOP, _SERVER_THREAD, _SESSION, _PORT
    if _LOOP is not None:
        if _SESSION is not None:
            try:
                asyncio.run_coroutine_threadsafe(_SESSION.close(), _LOOP).result(timeout=2.0)
            except Exception:
                logger.debug("stop_proxy_server: asyncio.run_coroutine_threadsafe failed (ignored)", exc_info=True)
        if _RUNNER is not None:
            try:
                asyncio.run_coroutine_threadsafe(_RUNNER.cleanup(), _LOOP).result(timeout=2.0)
            except Exception:
                logger.debug("stop_proxy_server: asyncio.run_coroutine_threadsafe failed (ignored)", exc_info=True)
        _LOOP.call_soon_threadsafe(_LOOP.stop)
    if _SERVER_THREAD is not None:
        _SERVER_THREAD.join(timeout=2.0)


def proxy_endpoint() -> str:
    if _PORT is None:
        raise RuntimeError("proxy server is not running")
    return f"http://127.0.0.1:{_PORT}"


def install_boto3_proxy_hook(client, source: dict) -> None:
    native_target = source.get("fos_native_endpoint") or source["endpoint"]
    cdn_url = (source.get("cdn_url") or "").strip()
    edge_only = source.get("provisioning", {}).get("edge_only")
    if edge_only is None:
        edge_only = source.get("edge_only")
    cdn_target = _scheme_host(cdn_url) if (cdn_url and edge_only is not False) else None
    cdn_secret = source.get("cdn_secret") or ""
    service_id = source.get("service_id") or source.get("name", "default")
    _CDN_OPS = {"getobject", "headobject"}

    def _inject(request, event_name: str = "", **_kwargs):
        try:
            from backend.utils.telemetry import get_process_context_with_fallback

            ctx = get_process_context_with_fallback() or ""
        except Exception:
            ctx = ""
        op = event_name.rsplit(".", 1)[-1].lower() if event_name else "unknown"

        if cdn_target and op in _CDN_OPS:
            request.headers["X-Fos-Target"] = cdn_target
            if cdn_secret:
                request.headers["x-fastly-key"] = cdn_secret
        else:
            request.headers["X-Fos-Target"] = native_target

        request.headers["X-Telemetry-Service-Id"] = service_id
        hint = _BOTO3_CALLER_HINT.get()
        request.headers["X-Telemetry-Caller"] = hint if hint else f"boto3.{op}"
        if not ctx:
            import threading as _threading

            ctx = f"untagged:{_threading.current_thread().name}"
        request.headers["X-Telemetry-Context"] = ctx

    client.meta.events.register("before-send.s3.*", _inject)


def _reset_for_tests() -> None:
    global _PORT, _SERVER_THREAD, _RUNNER, _LOOP, _SESSION, _LOG_FLUSHER_THREAD
    _reset_active_requests_for_tests()
    try:
        _flush_log_writes_for_tests(timeout=1.0)
    except Exception:
        logger.debug("_reset_for_tests: _flush_log_writes_for_tests failed (ignored)", exc_info=True)
    while True:
        try:
            _LOG_QUEUE.get_nowait()
        except queue.Empty:
            break
    flusher = _LOG_FLUSHER_THREAD
    if flusher is not None and flusher.is_alive():
        _LOG_FLUSHER_STOP.set()
        barrier = _FlushBarrier()
        _LOG_QUEUE.put(barrier)
        if not barrier.event.wait(timeout=1.0):
            raise RuntimeError("telemetry log flusher did not drain during test reset")
        flusher.join(timeout=1.0)
        if flusher.is_alive():
            raise RuntimeError("telemetry log flusher did not stop during test reset")
    _LOG_FLUSHER_THREAD = None
    _LOG_FLUSHER_STOP.clear()
    _PORT = None
    _SERVER_THREAD = None
    _RUNNER = None
    _LOOP = None
    _SESSION = None
    _READY.clear()
