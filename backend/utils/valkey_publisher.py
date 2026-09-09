"""Valkey-backed pub/sub for SSE fan-out across pods.

Replaces ``_InProcessPublisher`` when ``SSE_BACKPLANE=valkey``.

Design constraints this class must honor:

- **Publishers run in two very different contexts.** In the FastAPI process
  the lifespan calls ``bind_loop()`` and publishes ride the asyncio loop; in
  a Celery worker there is no loop and ``bind_loop`` is never called — those
  publishes go through a shared *sync* Redis client instead of being
  silently dropped (worker-originated cron/sync SSE events are the reason
  the backplane exists).
- **No silent failure.** Every dropped or failed publish increments a
  counter in ``PUBLISH_STATS`` and logs (rate-limited) — a dead backplane
  must be diagnosable from logs, not inferred from a frozen badge.
- **Subscribers must survive a Valkey restart.** ``subscribe()`` reconnects
  with backoff instead of letting the generator die and the SSE stream hang.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

# Failure visibility: keyed counters, cheap to read from a health snapshot.
PUBLISH_STATS: dict[str, int] = {
    "published": 0,
    "publish_errors": 0,
    "decode_errors": 0,
}

_LOG_EVERY_S = 60.0
_last_error_log: dict[str, float] = {}


def _log_rate_limited(key: str, msg: str, *args) -> None:
    now = time.monotonic()
    if now - _last_error_log.get(key, 0.0) >= _LOG_EVERY_S:
        _last_error_log[key] = now
        logger.warning(msg, *args)


_sync_redis_lock = threading.Lock()
_sync_redis = None


def _get_sync_redis():
    """One shared sync Redis client per process (publishers + seed reads)."""
    global _sync_redis
    with _sync_redis_lock:
        if _sync_redis is None:
            from redis import Redis as SyncRedis

            broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            _sync_redis = SyncRedis.from_url(
                broker_url,
                socket_connect_timeout=1.0,
                socket_timeout=2.0,
                decode_responses=True,
            )
        return _sync_redis


class ValkeyPublisher:
    def __init__(self, channel_prefix: str = "sync-status", history_size: int = 0) -> None:
        self._channel_prefix = channel_prefix
        self._history_size = history_size
        self._loop: asyncio.AbstractEventLoop | None = None
        self._redis = None  # async client, bound in bind_loop()

    def _channel(self, service_id: str) -> str:
        return f"sse:{self._channel_prefix}:{service_id}"

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind to the FastAPI asyncio loop and create the async client.
        Optional — processes without a loop (Celery workers) publish through
        the shared sync client instead."""
        from redis.asyncio import Redis

        broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
        self._loop = loop
        self._redis = Redis.from_url(broker_url, decode_responses=True)

    # ── publish ───────────────────────────────────────────────────────────────

    def publish(self, service_id: str, payload: dict) -> None:
        payload_json = json.dumps(payload)
        channel = self._channel(service_id)

        if self._loop is not None and self._redis is not None:
            self._publish_async(channel, payload_json)
        else:
            self._publish_sync(channel, payload_json)

    def _apply_history(self, pipe, channel: str, payload_json: str) -> None:
        if self._history_size > 0:
            history_key = f"{channel}:history"
            pipe.rpush(history_key, payload_json)
            pipe.ltrim(history_key, -self._history_size, -1)

    def _publish_sync(self, channel: str, payload_json: str) -> None:
        try:
            r = _get_sync_redis()
            pipe = r.pipeline()
            pipe.publish(channel, payload_json)
            self._apply_history(pipe, channel, payload_json)
            pipe.execute()
            PUBLISH_STATS["published"] += 1
        except Exception as e:
            PUBLISH_STATS["publish_errors"] += 1
            _log_rate_limited("publish_sync", "[valkey] publish to %s failed (sync): %s", channel, e)

    def _publish_async(self, channel: str, payload_json: str) -> None:
        async def _do() -> None:
            try:
                assert self._redis is not None
                pipe = self._redis.pipeline()
                pipe.publish(channel, payload_json)
                self._apply_history(pipe, channel, payload_json)
                await pipe.execute()
                PUBLISH_STATS["published"] += 1
            except Exception as e:
                PUBLISH_STATS["publish_errors"] += 1
                _log_rate_limited("publish_async", "[valkey] publish to %s failed: %s", channel, e)

        try:
            assert self._loop is not None
            self._loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_do()))
        except RuntimeError as e:
            # Loop shutting down — fall back to the sync client rather than drop.
            _log_rate_limited("loop_closed", "[valkey] loop unavailable (%s); publishing sync", e)
            self._publish_sync(channel, payload_json)

    # ── subscribe ─────────────────────────────────────────────────────────────

    async def subscribe(self, service_id: str) -> AsyncIterator[dict]:
        """Yield published payloads for ``service_id``; reconnects on broker
        loss. Requires ``bind_loop()`` — subscribing is only meaningful in the
        SSE-serving (FastAPI) process."""
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
                    pass

    # ── seed / introspection (sync callers) ───────────────────────────────────

    def get_recent_ticks(self, service_id: str, count: int = 60) -> list[dict]:
        """Recent history for initial SSE seed. Sync — call via to_thread from
        async routes."""
        if self._history_size == 0:
            return []
        try:
            items = _get_sync_redis().lrange(f"{self._channel(service_id)}:history", -count, -1)
            return [json.loads(item) for item in items]
        except Exception as e:
            _log_rate_limited("history", "[valkey] history read failed for %s: %s", service_id, e)
            return []

    def subscriber_count(self, service_id: str) -> int:
        """Subscribers on this channel across all pods. Sync — call via
        to_thread from async contexts."""
        try:
            result = _get_sync_redis().pubsub_numsub(self._channel(service_id))
            if result:
                return int(result[0][1])
        except Exception as e:
            _log_rate_limited("numsub", "[valkey] subscriber_count failed for %s: %s", service_id, e)
        return 0
