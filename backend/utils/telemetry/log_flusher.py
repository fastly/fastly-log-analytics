from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import sys
import threading
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any

from backend.utils.telemetry.logging_config import _log_rate_limited

logger = logging.getLogger("backend.utils.telemetry_proxy")

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
            setattr(sys.modules[__name__], "_sync_redis", _sync_redis)
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
        thread = threading.Thread(
            target=_log_flusher_loop,
            args=(_LOG_QUEUE, _LOG_FLUSHER_STOP),
            name="proxy-log-flusher",
            daemon=True,
        )
        _LOG_FLUSHER_THREAD = thread
        setattr(sys.modules[__name__], "_LOG_FLUSHER_THREAD", thread)
        thread.start()


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
