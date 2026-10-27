from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import logging
import math
import os
import random
import re
import struct
import sys
import tempfile
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, MutableMapping
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any

import cachetools
import httpx
import structlog
import tenacity
from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

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
        from backend.core.query_attribution import current_attribution, derive_from_process_context

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
                from backend.core.query_attribution import current_attribution

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
        from backend.core.metadata import usage_log_db as _usage_log_db

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
                if "telemetry.py" in fn or "contextlib.py" in fn or "duckdb.py" in fn:
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


_MAX_KEYS = 1024
_last: dict[str, float] = {}
_log_once_lock = threading.Lock()


def log_once_per(key: str, interval_s: float = 60.0) -> bool:
    now = time.monotonic()
    with _log_once_lock:
        prev = _last.get(key)
        if prev is not None and now - prev < interval_s:
            return False
        if len(_last) >= _MAX_KEYS:
            cutoff = now - interval_s
            for k in [k for k, t in _last.items() if t < cutoff]:
                del _last[k]
            if len(_last) >= _MAX_KEYS:
                _last.clear()
        _last[key] = now
        return True


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
_LOG_EVERY_S = 60.0
_sync_redis_lock = threading.Lock()
_sync_redis: Any = None


def _log_rate_limited(key: str, msg: str, *args: Any) -> None:
    if log_once_per(key, _LOG_EVERY_S):
        logger.warning(msg, *args)


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


DEFAULT_PRECISION = 8
SERIALIZATION_VERSION = 1
_HASH_BITS = 64
HLL_SKETCH_NAMES: tuple[str, ...] = ("hll_sketch",)


def _alpha(m: int) -> float:
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1.0 + 1.079 / m)


def _hash64(item: str | bytes) -> int:
    if isinstance(item, str):
        item = item.encode("utf-8")
    digest = hashlib.md5(item, usedforsecurity=False).digest()  # noqa: S324
    return struct.unpack("<Q", digest[:8])[0]


def _leading_zero_count(value: int, width_bits: int) -> int:
    if value == 0:
        return width_bits
    rank = 1
    msb = 1 << (width_bits - 1)
    while (value & msb) == 0 and rank < width_bits:
        rank += 1
        value <<= 1
    return rank


class HyperLogLog:
    __slots__ = ("_p", "_m", "_buckets")

    def __init__(self, precision: int = DEFAULT_PRECISION) -> None:
        if not 4 <= precision <= 16:
            raise ValueError(f"precision must be in [4, 16], got {precision}")
        self._p = precision
        self._m = 1 << precision
        self._buckets = bytearray(self._m)

    @property
    def precision(self) -> int:
        return self._p

    @property
    def m(self) -> int:
        return self._m

    def add(self, item: str | bytes) -> None:
        h = _hash64(item)
        bucket = h >> (_HASH_BITS - self._p)
        observed = h & ((1 << (_HASH_BITS - self._p)) - 1)
        rank = _leading_zero_count(observed, _HASH_BITS - self._p)
        if rank > self._buckets[bucket]:
            self._buckets[bucket] = rank

    def update(self, items: Iterable[str | bytes]) -> None:
        for item in items:
            self.add(item)

    def count(self) -> int:
        m = self._m
        raw_sum = sum(2.0**-b for b in self._buckets)
        if raw_sum == 0.0:
            return m
        estimate = _alpha(m) * m * m / raw_sum
        if estimate <= 2.5 * m:
            zeros = sum(1 for b in self._buckets if b == 0)
            if zeros > 0:
                estimate = m * math.log(m / zeros)
        if estimate > (1 << 64) / 30.0:
            estimate = -(1 << 64) * math.log(1 - estimate / (1 << 64))
        return int(round(estimate))

    def merge(self, other: HyperLogLog) -> None:
        if other._p != self._p:
            raise ValueError(f"merge precision mismatch: self.p={self._p} vs other.p={other._p}")
        for i in range(self._m):
            if other._buckets[i] > self._buckets[i]:
                self._buckets[i] = other._buckets[i]

    def to_bytes(self) -> bytes:
        return bytes([SERIALIZATION_VERSION, self._p]) + bytes(self._buckets)

    @classmethod
    def from_bytes(cls, blob: bytes | bytearray) -> HyperLogLog:
        if len(blob) < 2:
            raise ValueError(f"sketch blob too short: {len(blob)} bytes")
        version, precision = blob[0], blob[1]
        if version != SERIALIZATION_VERSION:
            raise ValueError(f"unknown HLL serialization version {version}; expected {SERIALIZATION_VERSION}")
        if not 4 <= precision <= 16:
            raise ValueError(f"invalid precision in blob: {precision}")
        expected_len = 2 + (1 << precision)
        if len(blob) != expected_len:
            raise ValueError(f"sketch blob length mismatch: got {len(blob)}, expected {expected_len}")
        sketch = cls(precision=precision)
        sketch._buckets = bytearray(blob[2:])
        return sketch

    def __len__(self) -> int:
        return self.count()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic only
        return f"HyperLogLog(p={self._p}, estimate≈{self.count()})"


def get_hll_sketch(precision: int = DEFAULT_PRECISION) -> HyperLogLog:
    return HyperLogLog(precision=precision)


def merge_hll_sketches(blobs: Iterable[bytes | bytearray], precision: int = DEFAULT_PRECISION) -> int:
    acc: HyperLogLog | None = None
    for blob in blobs:
        if not blob:
            continue
        sketch = HyperLogLog.from_bytes(blob)
        if acc is None:
            acc = sketch
        else:
            acc.merge(sketch)
    return acc.count() if acc is not None else 0


query_hll_distinct = merge_hll_sketches
query_hll_distinct_filtered = merge_hll_sketches


def query_hll_distinct_by_bucket(rows: Iterable[tuple[Any, bytes | bytearray]]) -> dict[Any, int]:
    buckets: dict[Any, HyperLogLog] = {}
    for key, blob in rows:
        if not blob:
            continue
        sketch = HyperLogLog.from_bytes(blob)
        if key not in buckets:
            buckets[key] = sketch
        else:
            buckets[key].merge(sketch)
    return {k: v.count() for k, v in buckets.items()}


STATIC_DEFAULT: tuple[float, float] = (24.0, 168.0)
_BANDS: tuple[tuple[float, float, float], ...] = (
    (1.0, 0.25, 1.0),
    (4.0, 1.0, 1.0),
    (24.0, 4.0, 1.0),
    (48.0, 4.0, 24.0),
    (168.0, 24.0, 24.0),
    (math.inf, 24.0, 168.0),
)
ANCHOR_QUANTUM_SECONDS = 60
_FIXED_TOKEN_DELTAS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "3h": timedelta(hours=3),
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "24h": timedelta(hours=24),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}
VALID_RANGE_TOKENS: frozenset[str] = frozenset(_FIXED_TOKEN_DELTAS) | {"auto"}


def history_hours_from_earliest(earliest: str | None, *, now: datetime | None = None) -> float | None:
    dt = parse_iso_utc(earliest)
    if dt is None:
        return None
    anchor = now if now is not None else datetime.now(UTC)
    return max(0.0, (anchor - dt).total_seconds() / 3600.0)


def pick_insights_default(history_hours: float | None) -> tuple[float, float]:
    if history_hours is None or not math.isfinite(history_hours):
        return STATIC_DEFAULT
    for upper, window, baseline in _BANDS:
        if history_hours < upper:
            return (window, baseline)
    return STATIC_DEFAULT


def is_valid_range_token(token: str | None) -> bool:
    return token in VALID_RANGE_TOKENS


def quantize_anchor(anchor: str | None, *, now: datetime | None = None) -> str:
    dt = (parse_iso_utc(anchor) if anchor else None) or (now if now is not None else datetime.now(UTC))
    epoch = int(dt.timestamp())
    return iso_z(datetime.fromtimestamp(epoch - (epoch % ANCHOR_QUANTUM_SECONDS), tz=UTC))


def _pick_auto_token(earliest_log_at: str | None, *, now: datetime) -> str:
    history_hours = history_hours_from_earliest(earliest_log_at, now=now)
    if history_hours is None:
        return "7d"
    if history_hours < 7 * 24:
        return "24h"
    return "7d" if history_hours < 30 * 24 else "30d"


def resolve_window(
    range_token: str | None,
    anchor: str | None,
    *,
    earliest_log_at: str | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    if not is_valid_range_token(range_token) or range_token is None:
        raise ValueError(f"unrecognized range_token: {range_token!r}")
    now_dt = now if now is not None else datetime.now(UTC)
    anchor_dt = parse_iso_utc(quantize_anchor(anchor, now=now_dt))
    if anchor_dt is None:
        raise RuntimeError("anchor_dt is unexpectedly None")
    token = _pick_auto_token(earliest_log_at, now=now_dt) if range_token == "auto" else range_token
    return iso_z(anchor_dt - _FIXED_TOKEN_DELTAS[token]), iso_z(anchor_dt)


def invite_clamp_fingerprint(analyst_session: object | None) -> str | None:
    if analyst_session is None:
        return None
    qs = getattr(analyst_session, "query_start_time", None)
    qe = getattr(analyst_session, "query_end_time", None)
    qw = getattr(analyst_session, "query_window_hours", None)
    return hashlib.sha256(f"{qs or ''}|{qe or ''}|{qw or ''}".encode()).hexdigest()[:16]


_MISSING = object()


class BoundedTTLCache:
    def __init__(
        self,
        maxsize: int | None = None,
        ttl_seconds: float = 60.0,
        *,
        max_entries: int | None = None,
        name: str | None = None,
    ) -> None:
        resolved_max = max_entries if max_entries is not None else (256 if maxsize is None else maxsize)
        if resolved_max <= 0:
            raise ValueError("maxsize must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.maxsize = resolved_max
        self.ttl = self.ttl_seconds = float(ttl_seconds)
        self.name = name
        self._cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=resolved_max, ttl=self.ttl, timer=lambda: time.monotonic()
        )
        self.lock = threading.RLock()

    def __contains__(self, key: Any) -> bool:
        with self.lock:
            return key in self._cache

    def __getitem__(self, key: Any) -> Any:
        with self.lock:
            return self._cache[key]

    def get(self, key: Any, default: Any = None) -> Any:
        with self.lock:
            return self._cache.get(key, default)

    def __setitem__(self, key: Any, value: Any) -> None:
        with self.lock:
            self._cache[key] = value

    def set(self, key: Any, value: Any) -> None:
        self[key] = value

    def put(self, key: Any, value: Any) -> None:
        self[key] = value

    def __delitem__(self, key: Any) -> None:
        with self.lock:
            del self._cache[key]

    def delete(self, key: Any) -> bool:
        with self.lock:
            return self._cache.pop(key, _MISSING) is not _MISSING

    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        with self.lock:
            return self._cache.pop(key) if default is _MISSING else self._cache.pop(key, default)

    def clear(self) -> None:
        with self.lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self.lock:
            return len(self._cache)

    def __iter__(self) -> Iterator[Any]:
        with self.lock:
            return iter(list(self._cache.keys()))

    def keys(self) -> list[Any]:
        with self.lock:
            return list(self._cache.keys())

    def values(self) -> list[Any]:
        with self.lock:
            return list(self._cache.values())

    def items(self) -> list[tuple[Any, Any]]:
        with self.lock:
            return list(self._cache.items())


BoundedCache = BoundedTTLCache


def _try_clear_cache(cache: Any) -> bool:
    clear_fn = getattr(cache, "clear", None)
    if not callable(clear_fn):
        return False
    try:
        clear_fn()
        return True
    except Exception:  # pragma: no cover — best-effort drain
        return False


_PREV_CACHE_ENTRIES: dict[str, Any] = getattr(globals().get("CacheRegistry"), "_entries", {})


class CacheRegistry:
    _entries: dict[str, Any] = _PREV_CACHE_ENTRIES
    _lock = threading.Lock()

    @classmethod
    def register(cls, name: str, cache: Any) -> None:
        with cls._lock:
            cls._entries[name] = cache

    @classmethod
    def clear_all(cls) -> None:
        with cls._lock:
            caches = list(cls._entries.values())
        for cache in caches:
            _try_clear_cache(cache)

    @classmethod
    def clear(cls, name: str) -> bool:
        with cls._lock:
            cache = cls._entries.get(name)
        return _try_clear_cache(cache) if cache is not None else False

    @classmethod
    def names(cls) -> list[str]:
        with cls._lock:
            return list(cls._entries.keys())

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls._entries.clear()


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


_active_requests_logger = logging.getLogger("backend.utils.active_requests")
_lock = threading.Lock()
_active_count = 0
_defer_started_at: dict[tuple[str, str], float] = {}


def increment_active_requests() -> None:
    global _active_count
    with _lock:
        _active_count += 1


def decrement_active_requests() -> None:
    global _active_count
    with _lock:
        if _active_count > 0:
            _active_count -= 1


def active_request_count() -> int:
    with _lock:
        return _active_count


def should_defer_cron(job_name: str, service_id: str, *, max_defer_secs: float = 30.0) -> bool:
    key = (job_name, service_id)
    with _lock:
        if _active_count == 0:
            _defer_started_at.pop(key, None)
            return False
        now = time.monotonic()
        started = _defer_started_at.get(key)
        if started is None:
            _defer_started_at[key] = now
            return True
        if (now - started) >= max_defer_secs:
            _defer_started_at.pop(key, None)
            _active_requests_logger.warning(
                "[active-request-gate] %s/%s ran after %.1fs of active-request deferrals — "
                "sustained traffic on this service; consider raising max_defer_secs.",
                job_name,
                service_id,
                now - started,
            )
            return False
        return True


def yield_to_api(
    *,
    max_wait_secs: float = 1.0,
    poll_interval: float = 0.1,
    sleep_fn: Callable[[float], None] | None = None,
) -> float:
    if active_request_count() == 0:
        return 0.0
    sleep = sleep_fn or time.sleep
    deadline = time.monotonic() + max_wait_secs
    slept = 0.0
    while time.monotonic() < deadline:
        sleep(poll_interval)
        slept += poll_interval
        if active_request_count() == 0:
            return slept
    return slept


def _reset_active_requests_for_tests() -> None:
    global _active_count
    with _lock:
        _active_count = 0
        _defer_started_at.clear()


class _ActiveRequestsResetAdapter:
    @staticmethod
    def clear() -> None:
        _reset_active_requests_for_tests()


CacheRegistry.register("utils.active_requests", _ActiveRequestsResetAdapter())


def _add_otel_trace_context(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        ctx = span.get_span_context()
        if ctx.is_valid:
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _drop_uvicorn_color_message(
    _logger: Any, _method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict.pop("color_message", None)
    return event_dict


def effective_format() -> str:
    return "json" if os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json" else "console"


def configure_structlog() -> None:
    use_json = effective_format() == "json"
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_otel_trace_context,
        structlog.processors.StackInfoRenderer(),
    ]
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(
            colors=sys.stderr.isatty(),
            sort_keys=True,
            exception_formatter=structlog.dev.RichTracebackFormatter(
                color_system="truecolor" if sys.stderr.isatty() else None,
                show_locals=False,
            ),
        )
    )
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared_processors, structlog.stdlib.ExtraAdder(), _drop_uvicorn_color_message],
        processor=renderer,
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def bridge_uvicorn_loggers() -> None:
    for name in _UVICORN_LOGGERS:
        uv_logger = logging.getLogger(name)
        uv_logger.handlers = []
        uv_logger.propagate = True


audit_log = structlog.get_logger("audit")


def _mkstemp_sibling(target: str | Path, prefix: str | None, suffix: str) -> tuple[Path, int, str]:
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{dst.name}.",
        suffix=suffix,
        dir=str(dst.parent),
    )
    return dst, fd, tmp_name


def _write_stream(f: Any, data: bytes, fsync: bool) -> None:
    f.write(data)
    if fsync:
        f.flush()
        os.fsync(f.fileno())


@contextmanager
def atomic_temp_path(
    target: str | Path,
    *,
    prefix: str | None = None,
    suffix: str = ".tmp",
    mode: int | None = None,
) -> Iterator[Path]:
    dst, fd, tmp_name = _mkstemp_sibling(target, prefix, suffix)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        if mode is not None:
            os.chmod(tmp_path, mode)
        yield tmp_path
        if tmp_path.exists():
            os.replace(tmp_path, dst)
    finally:
        with suppress(OSError):
            if tmp_path.exists():
                tmp_path.unlink()


def atomic_write_bytes(
    target: str | Path,
    data: bytes,
    *,
    fsync: bool = False,
    mode: int | None = None,
    prefix: str | None = None,
    suffix: str = ".tmp",
    fallback_in_place_errnos: set[int] | None = None,
) -> None:
    dst, fd, tmp_name = _mkstemp_sibling(target, prefix, suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            _write_stream(f, data, fsync)
        if mode is not None:
            os.chmod(tmp_name, mode)
        os.replace(tmp_name, dst)
    except OSError as e:
        with suppress(OSError):
            os.unlink(tmp_name)
        err_msg = str(e).lower()
        if fallback_in_place_errnos and (
            e.errno in fallback_in_place_errnos or "busy" in err_msg or "permission" in err_msg
        ):
            with open(dst, "wb") as f:
                _write_stream(f, data, fsync)
            return
        raise
    except BaseException:
        with suppress(OSError):
            os.unlink(tmp_name)
        raise


def atomic_write_json(
    target: str | Path,
    data: Any,
    *,
    indent: int | None = 2,
    sort_keys: bool = False,
    separators: tuple[str, str] | None = None,
    default: Callable[[Any], Any] | None = None,
    fsync: bool = False,
    prefix: str | None = None,
    suffix: str = ".tmp",
    fallback_in_place_errnos: set[int] | None = None,
) -> None:
    payload = json.dumps(data, indent=indent, sort_keys=sort_keys, separators=separators, default=default).encode(
        "utf-8"
    )
    atomic_write_bytes(
        target,
        payload,
        fsync=fsync,
        prefix=prefix,
        suffix=suffix,
        fallback_in_place_errnos=fallback_in_place_errnos,
    )


ABS_PATH_RE = re.compile(r"(?<![A-Za-z0-9])/(?:srv|var|tmp|data|home|opt|usr|mnt|app)\b(?:/[A-Za-z0-9_./-]*)?")
CID_SALT_LITERAL_RE = re.compile(r"(sha256\(\s*CONCAT\(\s*')[^']*(')", re.IGNORECASE)
REDACTED = "[redacted]"
_MIN_SALT_LEN = 8
_known_salts: set[str] = set()
_salts_lock = threading.Lock()


def register_salt(salt: str | None) -> None:
    if isinstance(salt, str) and len(salt) >= _MIN_SALT_LEN:
        with _salts_lock:
            _known_salts.add(salt)


def scrub_secrets(text: str) -> str:
    with _salts_lock:
        salts = sorted(_known_salts, key=len, reverse=True)
    for salt in salts:
        text = text.replace(salt, REDACTED)
    return CID_SALT_LITERAL_RE.sub(r"\1[redacted-salt]\2", text)


def redact_error_text(msg: object) -> str:
    return ABS_PATH_RE.sub("<path>", scrub_secrets(str(msg)))


_DEFAULT_UA = "fastly-log-analysis/1.0"


def http_get_text(url: str, *, headers: dict[str, str] | None = None, timeout: float = 30.0) -> str:
    hdrs = {"User-Agent": _DEFAULT_UA}
    if headers:
        hdrs.update(headers)
    resp = httpx.get(url, headers=hdrs, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp.content.decode()


def make_client(
    timeout: float | httpx.Timeout | None = 5.0,
    *,
    verify: Any = None,
    transport: httpx.BaseTransport | None = None,
    **kwargs: Any,
) -> httpx.Client:
    if verify is not None:
        kwargs["verify"] = verify
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.Client(timeout=timeout, **kwargs)


def retry_call[T](
    fn: Callable[[], T],
    *,
    retry_on: Callable[[BaseException], bool],
    attempts: int,
    base_delay: float = 0.0,
    max_delay: float | None = None,
    multiplier: float = 2.0,
    jitter: float = 0.0,
    max_elapsed: float | None = None,
    before_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def _wait(retry_state: tenacity.RetryCallState) -> float:
        delay = base_delay * (multiplier ** (retry_state.attempt_number - 1))
        if max_delay is not None:
            delay = min(delay, max_delay)
        if jitter:
            delay += random.uniform(0, jitter)
        return max(delay, 0.0)

    def _before_sleep(retry_state: tenacity.RetryCallState) -> None:
        if before_retry is None:
            return
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None else None
        if exc is not None:
            before_retry(exc)

    stop: tenacity.stop.stop_base = tenacity.stop_after_attempt(attempts)
    if max_elapsed is not None:
        stop = stop | tenacity.stop_after_delay(max_elapsed)
    retrying = tenacity.Retrying(
        retry=tenacity.retry_if_exception(retry_on),
        stop=stop,
        wait=_wait,
        before_sleep=_before_sleep if before_retry is not None else None,
        reraise=True,
        sleep=sleep,
    )
    return retrying(fn)


def parse_iso_utc(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.astimezone(UTC) if d.tzinfo is not None else d.replace(tzinfo=UTC)
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            return None


def parse_window(start: str | None, end: str | None) -> tuple[datetime, datetime] | None:
    st = parse_iso_utc(start)
    et = parse_iso_utc(end)
    if st is None or et is None or et <= st:
        return None
    return st, et


def parse_hour_key_strict(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d-%H").replace(tzinfo=UTC)


def parse_hour_key(s: str) -> datetime | None:
    try:
        return parse_hour_key_strict(s)
    except ValueError:
        return None


def parse_day_key_strict(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)


def ensure_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_z_now() -> str:
    return iso_z(datetime.now(UTC))


def _parse_dt(s: str, default: datetime) -> datetime:
    dt = parse_iso_utc(s)
    return dt if dt is not None else default


def parse_date_window(start: str, end: str, default_days: int = 7) -> tuple[str, str]:
    now = datetime.now(UTC)
    start_dt = _parse_dt(start, now - timedelta(days=default_days))
    end_dt = _parse_dt(end, now)
    if len(end) <= 10:
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
    return iso_z(start_dt), iso_z(end_dt)


def safe_iso(dt) -> str | None:
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        s = dt.isoformat()
        if not s.endswith("Z") and "+" not in s and s.count("-") <= 2:
            s += "Z"
        return s
    s = str(dt).strip()
    if not s:
        return None
    if " " in s:
        s = s.replace(" ", "T")
        if not s.endswith("Z") and "+" not in s:
            s += "Z"
    elif "T" in s and not s.endswith("Z") and "+" not in s:
        s += "Z"
    return s


def parse_window_str_to_dt(s: str) -> datetime:
    dt = parse_iso_utc(s)
    if dt is None:
        raise ValueError(f"Invalid date window string: {s}")
    return dt


def window_to_epoch(start: str, end: str) -> tuple[str, str, int, int]:
    s, e = parse_date_window(start, end)
    return s, e, int(parse_window_str_to_dt(s).timestamp()), int(parse_window_str_to_dt(e).timestamp())


def parse_relative_time_window(since: str, max_lookback_days: int = 30) -> datetime:
    now = datetime.now(UTC)
    if not since:
        return now - timedelta(hours=1)
    s = since.strip().lower()
    try:
        if s.endswith("h"):
            hours = max(1, min(int(s[:-1]), max_lookback_days * 24))
            return now - timedelta(hours=hours)
        if s.endswith("d"):
            days = max(1, min(int(s[:-1]), max_lookback_days))
            return now - timedelta(days=days)
        if s.endswith("m"):
            minutes = max(1, min(int(s[:-1]), max_lookback_days * 24 * 60))
            return now - timedelta(minutes=minutes)
    except ValueError:
        pass
    return now - timedelta(hours=1)


# ── Local HTTP S3/CDN telemetry proxy (folded from telemetry_proxy.py) ──

import contextvars
import logging
import os
import queue
import threading
import time
from collections import defaultdict

import aiohttp
import yarl
from aiohttp import web
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

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
# Serialises the "is the server already up / do we need to start it"
# decision in :func:`start_proxy_server`. Concurrent first-callers used
# to race: thread A would see ``_SERVER_THREAD is None``, spawn the
# server, and start waiting on _READY; thread B would see the just-
# spawned thread alive, early-return without waiting, then hit
# ``proxy_endpoint()`` while ``_PORT`` was still None — surfacing as
# "proxy server is not running" on every concurrent first-caller after
# the first.
_START_LOCK = threading.Lock()

# Upstream call timeouts. The wall-clock `total` is the safety net for
# requests that get wedged past Fastly's 60s first_byte_timeout (a stuck
# proxy used to hold a 300s connection open on 2026-05-20). 90s default
# leaves room for one full Fastly TTFB attempt plus byte streaming; tune
# via FOS_PROXY_UPSTREAM_TIMEOUT_S if you regularly fetch very large
# parquets through the proxy. sock_read=60 catches mid-stream stalls.
_UPSTREAM_TIMEOUT_TOTAL_S = float(os.getenv("FOS_PROXY_UPSTREAM_TIMEOUT_S", "90"))
_UPSTREAM_TIMEOUT = aiohttp.ClientTimeout(
    total=_UPSTREAM_TIMEOUT_TOTAL_S,
    sock_connect=5,
    sock_read=60,
)

# Connection-pool cap per upstream host. Boto3 sticks to ~10; DuckDB does
# many parallel Range GETs against the same parquet host so 128 gives some
# headroom without exhausting ephemeral ports.
_POOL_PER_HOST = 128

# Idle keep-alive cap on pooled upstream connections. aiohttp's default is
# 15s; Fastly's edge keep-alive is typically 5-10s. The mismatch caused the
# 2026-05-20 "Cannot write to closing transport" storm — the pool handed out
# a connection that Fastly had already half-closed, the write failed, and we
# returned 502. boto3's legacy-retry then re-fetched (97% of those retries
# succeeded), doubling the wire-byte cost on every affected GET. 4s keeps us
# below any plausible upstream keep-alive while still letting bursty
# back-to-back GETs reuse a connection. Pair this with enable_cleanup_closed
# on the connector to reap half-dead sockets the pool can't detect.
_UPSTREAM_KEEPALIVE_S = float(os.getenv("FOS_PROXY_KEEPALIVE_S", "4"))

# Connection-error class set we treat as eligible for one internal retry.
# Empirically (2026-05-20 telemetry): ServerDisconnectedError dominates;
# ClientConnectionResetError shows up under load; ClientOSError covers
# generic socket errors (broken pipe, ECONNRESET) that aren't already a
# ServerDisconnectedError. We do NOT retry on ClientResponseError (the
# upstream actually responded — propagate verbatim) or on auth errors.
_RETRYABLE_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientConnectionResetError,
    aiohttp.ClientOSError,
)

# usage_log write coalescer. Every proxied FOS/CDN call used to submit a
# single-row INSERT to a ThreadPoolExecutor — at 200 cdn ops/cron-tick that
# was 200 separate INSERTs (~10ms each, ~2s SQLite write time and 200 ring-
# buffer entries that drowned out genuine page SQLite). A single background
# thread now drains a queue and batches rows into executemany() per
# (service_id, process_context) group. Flush whenever the batch hits
# _LOG_BATCH_MAX_ROWS OR _LOG_BATCH_MAX_INTERVAL_S elapses since the last
# flush — whichever comes first.
_LOG_BATCH_MAX_ROWS = 50
_LOG_BATCH_MAX_INTERVAL_S = 0.1
_LOG_QUEUE: queue.Queue = queue.Queue()
_LOG_FLUSHER_STOP = threading.Event()
_LOG_FLUSHER_THREAD: threading.Thread | None = None
_LOG_FLUSHER_LOCK = threading.Lock()
# Pair sentinel ↔ event. _flush_log_writes_for_tests pushes an event-wrapping
# sentinel; the flusher drains everything up to (and including) it, then signals
# the event so the test can return deterministically — no sleep guesswork.


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
    # Queue + stop event are passed as parameters (not read from module
    # globals) so a future importlib.reload(telemetry_proxy) — done by the
    # FOS_PROXY_UPSTREAM_TIMEOUT_S env-override test — rebinds the module's
    # _LOG_QUEUE without leaving this still-alive daemon thread racing the
    # post-reload flusher for items from the SAME (new) queue. Each thread
    # owns its queue reference for life; the post-reload thread starts fresh
    # against its new queue, and this zombie just waits on its now-orphaned
    # one until the process exits.
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
    all queued log rows are written (or `timeout`).

    Tests that assert on mock-captured log writes (or caplog records from
    the proxy's warning path) must call this BEFORE exiting the
    `with patch(...)` / `with caplog.at_level(...)` context, otherwise:
      - the handler's finally block may run AFTER the patch is torn down
        and invoke the un-patched real log_usage_calls (capture empty).
      - the warning may be emitted AFTER caplog stops capturing.
    """
    # Two separate timeout budgets: in-flight wait gets the full `timeout`,
    # barrier wait gets its own `timeout` budget. Sharing them caused
    # intermittent test flakes — when in-flight polling consumed most of the
    # budget, the barrier wait shrank to ~10ms and timed out before the
    # flusher's executemany returned, leaving captured_rows empty.
    deadline = time.time() + timeout
    while time.time() < deadline:
        with _IN_FLIGHT_LOCK:
            in_flight = _IN_FLIGHT_REQUESTS
        if in_flight == 0:
            break
        time.sleep(0.005)
    # Push a barrier; the flusher signals its event AFTER draining every row
    # queued before the barrier. Deterministic — no sleep guesswork.
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
    # Delegates to backend.config.load_config, which has its own
    # mtime-revalidated cache. The wrapper exists to localize the import
    # (avoids circular import at module load) and to give tests a single
    # patch point.
    from backend import config as _config

    return _config.load_config(service_id)


def _bust_config_cache(service_id: str | None = None) -> None:
    # Clears the central cache in backend.config so tests that patch
    # load_config() see the patched return value immediately. Production
    # callers don't need this — load_config revalidates via st_mtime_ns
    # on every call, so credential rotations pick up automatically.
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


# Hosts treated as machine-local — connections to any of these from the
# backend itself reach loopback and so cannot escalate privilege via the
# proxy. ``0.0.0.0`` is the wildcard-bind address that ``moto`` and other
# in-process test servers commonly use; in production it would never be a
# legitimate X-Fos-Target value.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "0.0.0.0"})


def _strip_port_for_loopback_check(target_host: str) -> str:
    """Return the bare host portion of ``target_host`` for comparison
    against the loopback set. Handles three forms commonly seen as
    X-Fos-Target values:
      * ``[::1]:9999`` → ``::1`` (IPv6 literal with port, RFC 3986)
      * ``host:port`` / ``127.0.0.1:9999`` → host portion before the colon
      * ``::1`` / bare hostname → returned unchanged
    """
    if target_host.startswith("[") and "]" in target_host:
        return target_host[1 : target_host.index("]")]
    # A single colon means host:port. Multiple colons (and no brackets) is
    # a bare IPv6 literal that must not be split.
    if target_host.count(":") == 1:
        return target_host.split(":", 1)[0]
    return target_host


def _is_target_host_allowed(target_host: str, cdn_host: str | None, fos_native: str | None) -> bool:
    """Defense-in-depth gate on the value of X-Fos-Target.

    Without this gate, anyone who can reach the proxy AND supplies a
    valid X-Telemetry-Service-Id can use the proxy as an AWS-signed
    forwarder to arbitrary internet hosts (SSRF + service-credential
    exposure). The proxy binds to loopback so the realistic attacker
    today already has admin, but defense-in-depth wants the request to
    be rejected before signing rather than relying on the network
    binding.

    Allowed per-request:
      * the service's configured ``cdn_host`` (from ``cfg.cdn_url``),
        for CDN reads that use ``?key=`` auth and skip SigV4;
      * the service's configured ``fos_native_endpoint`` (defaults to
        ``{region}.object.fastlystorage.app``), for FOS reads/writes
        that the proxy SigV4-signs;
      * 127.0.0.1 / localhost / ::1 / 0.0.0.0 (loopback / wildcard-bind),
        because the proxy runs co-located with the backend and routing a
        loopback request *through* the proxy adds telemetry but not
        privilege.

    Region isolation is enforced — a service whose ``fos_endpoint`` is
    ``us-east-1.object.fastlystorage.app`` cannot use the proxy to
    target ``eu-west-1.object.fastlystorage.app`` (different region's
    credentials would not work anyway, but the gate cuts off the probe).
    """
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
        # Silent unsigned forwarding here was the root cause of the unexplained
        # 'early-startup 403 on LIST /<bucket>/?prefix=raw/' — the proxy
        # received the request, couldn't resolve the service config, dropped
        # signing, and FOS rejected the unsigned request. Log loudly so the
        # caller is identifiable next time.
        logger.warning(
            "[telemetry-proxy] cannot sign %s %s: no config for service_id=%r — forwarding UNSIGNED (will 403)",
            method,
            url,
            service_id,
        )
        return headers

    target_host = _scheme_host(url)
    cdn_host = _cdn_host_for(cfg)
    # CDN routing uses ?key= auth — do NOT inject SigV4 there.
    if cdn_host and target_host == cdn_host:
        if cfg.get("cdn_secret"):
            headers["x-fastly-key"] = cfg["cdn_secret"]
        return headers

    # Deliberately NO cdn_secret below this line. The native FOS endpoint
    # authenticates with SigV4 over the FOS access keys; it neither reads
    # nor needs the CDN service's secret, so forwarding it would hand a
    # credential to a host that has no use for it. It also does not lift
    # FOS's own rate limits — those are a property of the storage account,
    # not of the CDN in front of it. Pinned by the *_native tests in
    # tests/utils/test_telemetry_proxy_phase2.py.
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

    # S3 bulk delete (POST /<bucket>?delete) requires Content-MD5. When unsigned
    # boto3 sends this request, Content-MD5 may be missing from client headers.
    # Compute and add it before signing so SigV4 covers it and FOS accepts bulk delete.
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
    # S3SigV4Auth (not bare SigV4Auth) adds X-Amz-Content-SHA256, which
    # S3 requires for every signed request.
    S3SigV4Auth(credentials, "s3", region).add_auth(aws_req)
    headers.update(dict(aws_req.headers))
    return headers


def _service_for_target(target_host: str, cdn_host: str | None) -> str:
    # 'CDN' if the target matches the service's cdn_url, else 'FOS'.
    # op_class is derived inside metadata_db.log_usage_calls from this
    # + method, so we don't pass it ourselves.
    return "CDN" if cdn_host and target_host == cdn_host else "FOS"


def _build_details(x_cache: str | None, caller: str) -> str:
    # X-Cache MUST be the first `· `-separated chunk so the shield-egress
    # doubling logic at backend.core.metadata.usage_log.log_usage_calls picks it up correctly.
    # Caller goes second.
    parts: list[str] = []
    if x_cache:
        parts.append(x_cache)
    parts.append(caller)
    return " · ".join(parts)


def _build_upstream_headers(client_headers) -> dict:
    # Strip hop-by-hop headers (RFC 7230 §6.1), the synthetic Host of
    # 127.0.0.1:<port>, and our internal telemetry routing headers.
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

    # X-Fos-Target is normally a bare host (production: HTTPS implied).
    # Tests using moto run an http-only upstream on 127.0.0.1, so we
    # honor an explicit scheme prefix when one is present.
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
            # ``fos_native_endpoint`` only exists on the derived source dict
            # produced by ``backend.config.to_source_dict()`` — the raw
            # service-config JSON loaded here has ``fos_endpoint`` instead.
            # Without the fallback, every signed FOS request (sync's
            # ListObjectsV2, commit's PutObject, etc.) reads ``fos=None``
            # at the allowlist gate and gets rejected with a 400, stalling
            # ingestion silently. Fall through to ``fos_endpoint`` so the
            # gate's region-isolation guarantee still holds against the
            # value the rest of the codebase already treats as canonical.
            fos_native = (cfg.get("fos_native_endpoint") or cfg.get("fos_endpoint") or "").strip() or None

    # F6 defense-in-depth: when we have a service config the gate is
    # active. See ``_is_target_host_allowed`` for the rationale and the
    # allow-set. The check is skipped when no service_id is supplied
    # (the proxy already forwards unsigned in that branch — FOS will
    # 403, so there is no credential exposure to gate) or when the
    # service has no loadable config (existing path logs and forwards
    # unsigned for the same 403 fate).
    if service_id and cfg is not None and not _is_target_host_allowed(target_host_for_classify, cdn_host, fos_native):
        logger.warning(
            "[telemetry-proxy] rejected disallowed X-Fos-Target=%r for service_id=%r (cdn=%r fos=%r)",
            target_host_for_classify,
            service_id,
            cdn_host,
            fos_native,
        )
        return web.Response(status=400, text="X-Fos-Target not allowed for service")

    # SigV4 requires SHA256 of the body, which forces buffering when we
    # sign. botocore.auth.SigV4Auth doesn't support the streaming signed-
    # payload variant out of the box, so large PUTs (multi-GB compacted
    # commits) would buffer fully in memory if routed through the proxy.
    # Today's compaction flow uploads directly to FOS — only metadata.json
    # and small avro manifests transit the proxy (kilobytes each), so the
    # buffering is bounded. If a future flow routes bulk payloads through
    # here, switch to STREAMING-AWS4-HMAC-SHA256-PAYLOAD chunked signing
    # before increasing the request-body size limit.
    # ``data`` is either a fully-buffered ``bytes`` (signed paths) or a
    # streaming ``aiohttp.StreamReader`` (unsigned fallback) — aiohttp's
    # client accepts both, so ``Any`` keeps the union narrow at the
    # callsite without forcing a buffer-up that would defeat streaming.
    data: Any
    if service_id and request.can_read_body:
        body = await request.read()
        data = body
        out_headers = _sign_request(request.method, upstream_url, out_headers, body, service_id)
    elif service_id:
        out_headers = _sign_request(request.method, upstream_url, out_headers, b"", service_id)
        data = None
    else:
        # No X-Telemetry-Service-Id means we can't look up credentials and
        # the request goes upstream unsigned. For a private FOS bucket that's
        # a guaranteed 403. Log so the missing-header bug at the caller is
        # identifiable instead of debugging the upstream rejection.
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
    # Track whether response bytes have started flowing to the client. The
    # internal retry below MUST NOT fire after we've called prepare(), or we'd
    # be retrying a response the client has already partially observed.
    client_response_started = False
    # Idempotent methods are safe to replay against the upstream. The retry
    # exists to absorb keep-alive races (see _RETRYABLE_CONNECTION_ERRORS):
    # the pool hands out a connection the upstream already half-closed, the
    # write fails, we retry once on a fresh connection. Pre-fix telemetry
    # showed 97% of the resulting 502s were immediately followed by a
    # successful boto3 retry — moving that retry inside the proxy hides the
    # blip from boto3 and avoids the doubled wire-cost we were paying.
    _IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")
    attempt = 0

    try:
        while True:
            try:
                # encoded=True preserves the URL verbatim on the wire. botocore's
                # S3SigV4Auth derives the canonical URI from urlsplit(url).path
                # without re-encoding, so a path containing %3D (e.g. hive-style
                # partition keys like timestamp_hour%3D2026-05-19-23) signs the
                # %3D form. aiohttp's default URL(str, encoded=False) would DECODE
                # %3D -> = on the wire, leaving the canonical URI we signed
                # diverged from what R2 verifies — producing
                # HTTP 403 'The calculated signature does not match'.
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
                    # RFC 7231 §4.3.2: HEAD responses MUST NOT include a body.
                    # Drain the upstream body for byte-counting + telemetry,
                    # but never forward to the client. aiohttp 3.14's stricter
                    # parser otherwise rejects HEAD-with-body as BadStatusLine.
                    is_head = request.method == "HEAD"
                    try:
                        async for chunk in upstream_resp.content.iter_chunked(65536):
                            bytes_received += len(chunk)
                            if not is_head:
                                await proxy_resp.write(chunk)
                        await proxy_resp.write_eof()
                    except ConnectionResetError as ce:
                        # Client (e.g. aiobotocore) closed its socket
                        # mid-stream. The upstream GET to FOS already
                        # completed and will bill us — record the row with
                        # the upstream status (typically 200) we captured
                        # above. Reporting this as a synthetic 502 would
                        # both over-count failures in our telemetry and
                        # under-report the upstream-billable success.
                        # boto3 will retry on its end if it needs the body.
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
                # Only safe to retry while no bytes have crossed back to the
                # client and the method is naturally idempotent. We retry at most
                # once — repeated failures indicate something worse than a
                # pool-races-server-FIN blip (e.g. upstream down) and we should
                # surface it instead of doubling the latency budget.
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
        # Write the telemetry row regardless of success/failure — errored
        # requests are still billable in FOS.
        if service_id:
            elapsed_ms = round((time.time() - t0) * 1000, 2)
            service = _service_for_target(target_host_for_classify, cdn_host)
            status_str = "OK" if (upstream_status and 200 <= upstream_status < 300) else f"HTTP {upstream_status}"
            # Row schema MUST match what metadata_db.log_usage_calls reads.
            # The X-Cache value MUST be the first `· `-separated chunk of
            # `details` — the shield-egress doubling at backend.core.metadata.usage_log.log_usage_calls
            # parses it from there.
            # Translate the raw HTTP verb to the S3 op name when we can
            # recognise the shape — log_usage_calls keys Class A vs Class B
            # off the S3 op name (LIST_OBJECTS_V2 = A), so a bare `GET`
            # would otherwise misclassify every boto3 list_objects_v2 call
            # as a Class B read. Only LIST is common enough to bother with;
            # other S3 ops keep their raw HTTP verb (PUT/POST/COPY are
            # already in the Class A list, HEAD/DELETE/GET-of-object are
            # correctly Class B).
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

            # Cost guardrail: dashboard reads MUST NOT hit FOS directly.
            # Warn loudly so the regression class (the iceberg_scan vs
            # read_parquet incident) is visible. Do not block — visibility
            # over enforcement until Phase 3 wires clients to the proxy.
            # Health check probes (e.g. /api/admin/health-snapshot) intentionally test
            # bucket reachability and are exempt from dashboard cost guardrails.
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

            # CDN MISS → synth a Class B FOS GET_OBJECT row. Fastly's
            # behavior on a cache miss against an object-storage origin
            # is to fetch the full body regardless of the client's HTTP
            # method (so subsequent reads hit cache), so the underlying
            # FOS op is always GET_OBJECT, not HEAD_OBJECT. The MISS, HIT
            # chain (edge missed, shield hit) does NOT touch FOS.
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
    # Built once on startup so two concurrent requests don't both lazy-init
    # and leak the loser.
    #
    # enable_cleanup_closed reaps SSL connections that the upstream has
    # closed but whose FIN we haven't observed yet; without it, aiohttp can
    # hand a half-dead socket to the next request and the write fails with
    # "Cannot write to closing transport". keepalive_timeout below the
    # upstream's keep-alive bounds the window in which that race is even
    # possible. The internal-retry loop in _handle_request_inner is the
    # third line of defense for the residual race.
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

    # Cap request bodies at 4GB. aiohttp's default 1MB cap is too small
    # for our use case (DuckLake commit multiparts), but the previous
    # ``client_max_size=0`` (unlimited) made the proxy a credible OOM
    # vector: ``await request.read()`` buffers the whole body, so two
    # concurrent multi-GB PUTs through the proxy could blow past the
    # 12GB container limit by themselves. 4GB covers any realistic
    # single multipart upload part (S3 individual part max is 5GB but
    # we never write parts that big) while bounding worst-case buffer
    # bloat. A 413 above 4GB is the right failure mode — callers can
    # split into smaller parts.
    app = web.Application(client_max_size=4 * 1024 * 1024 * 1024)
    app.router.add_get("/healthz", handle_healthz)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    # access_log=None silences aiohttp.access's per-request INFO spam.
    # Every FOS GET/HEAD/PUT routes through this proxy — at typical ingest
    # rates that's hundreds of lines/minute of noise burying real signal.
    # We still capture each request's metadata via the telemetry hooks
    # below, which write to usage_log with full context.
    _RUNNER = web.AppRunner(app, access_log=None)
    _LOOP.run_until_complete(_RUNNER.setup())

    site = web.TCPSite(_RUNNER, "127.0.0.1", 0)
    _LOOP.run_until_complete(site.start())

    # OS-assigned port becomes available only after .start().
    # asyncio's AbstractServer base class doesn't declare ``sockets`` but
    # every concrete implementation (Server/UnixServer) provides it; the
    # site is started so `_server` is the concrete subclass at runtime.
    _server = site._server
    if _server is None:
        raise RuntimeError("_server is unexpectedly None")
    _PORT = _server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    _READY.set()

    _LOOP.run_forever()


def start_proxy_server() -> None:
    global _SERVER_THREAD
    # Fast path: server is up and serving. ``_PORT`` is set inside
    # ``_run_server`` after the server has bound, so testing it (not just
    # ``_SERVER_THREAD.is_alive()``) is what tells us a concurrent caller
    # is safe to read ``proxy_endpoint()`` immediately.
    if _PORT is not None and _SERVER_THREAD is not None and _SERVER_THREAD.is_alive():
        return
    with _START_LOCK:
        # Re-check under the lock. The first caller spawns the thread;
        # every subsequent caller falls through to ``_READY.wait`` below
        # without re-spawning so we don't have N threads each starting
        # their own server (which would also leak module globals).
        if _SERVER_THREAD is None or not _SERVER_THREAD.is_alive():
            _READY.clear()
            _SERVER_THREAD = threading.Thread(target=_run_server, daemon=True, name="telemetry-proxy")
            _SERVER_THREAD.start()
    # Wait OUTSIDE the lock so concurrent callers all block in parallel
    # rather than serialising. Anything beyond ~2s is a bind failure;
    # fail loud rather than racing.
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
    """Attach a before-send event handler that injects the proxy's required
    headers on every boto3 S3 request:
      - X-Fos-Target: CDN host for object downloads when cdn_url is set,
        else the native FOS endpoint. Writes/lists always native because
        the CDN is read-only and LIST isn't reliably CDN-served.
      - x-fastly-key: CDN auth header, injected only when targeting CDN
        (mirrors _configure_fos's DuckDB-httpfs pattern at duckdb.py:253-255).
      - X-Telemetry-Service-Id: for credential lookup at sign time
      - X-Telemetry-Caller: f"boto3.<operation>" (parsed from event_name)
      - X-Telemetry-Context: current process_context ContextVar (per-request)

    boto3's before-send event passes only `event_name` (e.g.
    "before-send.s3.HeadBucket"); operation name is the trailing segment.
    The request is an AWSPreparedRequest whose `.headers` is mutable.
    """
    native_target = source.get("fos_native_endpoint") or source["endpoint"]
    cdn_url = (source.get("cdn_url") or "").strip()
    edge_only = source.get("provisioning", {}).get("edge_only")
    if edge_only is None:
        edge_only = source.get("edge_only")
    cdn_target = _scheme_host(cdn_url) if (cdn_url and edge_only is not False) else None
    cdn_secret = source.get("cdn_secret") or ""
    service_id = source.get("service_id") or source.get("name", "default")
    # Only flip object downloads. LIST is HTTP GET too but the CDN
    # service isn't guaranteed to serve list responses correctly, and
    # mutations stay native because the CDN is read-only.
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
            # Native FOS only: SigV4 authenticates these, so the CDN secret
            # is not attached — see the note in _sign_request.
            request.headers["X-Fos-Target"] = native_target

        request.headers["X-Telemetry-Service-Id"] = service_id
        hint = _BOTO3_CALLER_HINT.get()
        request.headers["X-Telemetry-Caller"] = hint if hint else f"boto3.{op}"
        # Always tag context: fall back to thread name so untagged work is
        # at least attributable to *some* thread instead of dropping into
        # the NULL bucket that blocks cost attribution.
        if not ctx:
            import threading as _threading

            ctx = f"untagged:{_threading.current_thread().name}"
        request.headers["X-Telemetry-Context"] = ctx

    client.meta.events.register("before-send.s3.*", _inject)


def _reset_for_tests() -> None:
    global _PORT, _SERVER_THREAD, _RUNNER, _LOOP, _SESSION, _LOG_FLUSHER_THREAD
    _reset_active_requests_for_tests()
    # Drain any rows the prior test left in the coalescer queue so the next
    # test's log_usage_calls mock doesn't capture stale rows from an
    # already-torn-down patch context.
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


_FOLDED_TELEMETRY_SUBMODULES: tuple[str, ...] = (
    "sqlite_profiler",
    "valkey_publisher",
    "hll",
    "log_throttle",
    "usage_logger",
    "time_window",
    "bounded_cache",
    "cache_registry",
    "telemetry_response_middleware",
    "active_requests",
    "structlog_config",
    "atomic_write",
    "error_redaction",
    "http",
    "date_utils",
    "telemetry_proxy",
)
_this_telemetry_mod = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
for _submod_name in _FOLDED_TELEMETRY_SUBMODULES:
    sys.modules[f"backend.utils.{_submod_name}"] = _this_telemetry_mod
    if _utils_pkg is not None:
        setattr(_utils_pkg, _submod_name, _this_telemetry_mod)
sys.modules["backend.core.retry"] = _this_telemetry_mod
_core_pkg = sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "retry", _this_telemetry_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {f"backend.utils.{_submod_name}": "backend.utils.telemetry" for _submod_name in _FOLDED_TELEMETRY_SUBMODULES}
    )
    _backend_pkg._MODULE_ALIASES["backend.core.retry"] = "backend.utils.telemetry"
