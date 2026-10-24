"""Realtime metrics polling coordinator.

Polls ``rt.fastly.com`` every second per service, transforms the
response, and publishes to :data:`publisher`. Suspends automatically
when no SSE subscribers are connected; resumes with a backfill window
on the next ``ensure_polling()`` call.

The poller runs in a dedicated daemon thread per service — not on the
FastAPI event loop — so DuckDB queries and cron work cannot starve the
1-second cadence. A pooled ``httpx.Client`` (``utils.http.make_client``) provides HTTP keep-alive so each
poll reuses the TLS connection (~100ms vs ~800ms per request).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from backend.core.realtime import publisher
from backend.core.realtime.transform import (
    error_tick_payload,
    gap_tick_payload,
    transform_rt_response,
    transform_single_second,
)
from backend.core.retry import retry_call
from backend.utils.http import make_client

logger = logging.getLogger(__name__)

RT_BASE = "https://rt.fastly.com"
POLL_INTERVAL = 1

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

LONGPOLL_READ_TIMEOUT = 1.2


class _LongPollTimeout(Exception):
    """rt.fastly.com /ts/ endpoint didn't respond within LONGPOLL_READ_TIMEOUT."""


def _retry_sleep(seconds: float) -> None:
    time.sleep(seconds)


def _is_retryable_fetch_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUS
    return isinstance(exc, httpx.TransportError)


@dataclass
class _ServiceState:
    cursor: int = 0
    thread: threading.Thread | None = None
    last_error: str | None = None
    last_good: dict | None = None
    last_good_at: datetime | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)


class RealtimePoller:
    def __init__(self) -> None:
        self._services: dict[str, _ServiceState] = {}
        self._lock = threading.Lock()

    def ensure_polling(self, service_id: str) -> None:
        with self._lock:
            state = self._services.get(service_id)
            if state and state.thread and state.thread.is_alive():
                return
            if state is None:
                state = _ServiceState()
                self._services[service_id] = state
            else:
                state.stop_event.clear()
            t = threading.Thread(
                target=self._poll_loop,
                args=(service_id, state),
                daemon=True,
                name=f"rt-poller-{service_id[:8]}",
            )
            state.thread = t
            t.start()

    def _poll_loop(self, service_id: str, state: _ServiceState) -> None:
        idle_cycles = 0
        session = make_client()

        # Setup redis for leader election if valkey is active
        redis_client = None
        pod_id = ""
        from backend.config import SSE_BACKPLANE

        if SSE_BACKPLANE == "valkey":
            import os
            import uuid

            from redis import Redis

            broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            redis_client = Redis.from_url(broker_url, decode_responses=True)
            pod_id = str(uuid.uuid4())

        try:
            while not state.stop_event.is_set():
                t0 = time.monotonic()
                try:
                    if publisher.subscriber_count(service_id) == 0:
                        idle_cycles += 1
                        if idle_cycles > 1:
                            logger.debug("RT poller suspending for %s (no subscribers)", service_id)
                            state.thread = None
                            return
                    else:
                        idle_cycles = 0

                    is_leader = True
                    if redis_client:
                        # Leader election. TTL must exceed the worst-case RT
                        # long-poll (the API can block several seconds) or the
                        # lease expires mid-fetch and a second pod becomes a
                        # concurrent leader publishing duplicate ticks. The
                        # extend is a single atomic compare-and-set — a plain
                        # GET-then-SET can steal the lock back after another
                        # pod legitimately acquired it post-expiry (TOCTOU).
                        lock_key = f"sse:rt-poller-lock:{service_id}"
                        extended = redis_client.eval(
                            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                            "  return redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2], 'XX') "
                            "else return false end",
                            1,
                            lock_key,
                            pod_id,
                            "15",
                        )
                        if not extended:
                            is_leader = bool(redis_client.set(lock_key, pod_id, ex=15, nx=True))

                    if not is_leader:
                        # We are not the leader, just sleep for the interval
                        pass
                    else:
                        is_backfill = state.cursor == 0
                        rt_json = self._fetch_realtime(session, service_id, state.cursor)
                        if rt_json is not None:
                            state.cursor = rt_json.get("Timestamp", state.cursor)
                            data_points = rt_json.get("Data") or []
                            if is_backfill and len(data_points) > 1:
                                base_ts = state.cursor - len(data_points)
                                for i, point in enumerate(data_points):
                                    ts = datetime.fromtimestamp(base_ts + i, tz=UTC).isoformat()
                                    tick = transform_single_second(point, ts)
                                    publisher.publish(service_id, tick)
                                state.last_good = tick
                                state.last_good_at = datetime.now(UTC)
                            else:
                                tick = transform_rt_response(rt_json)
                                state.last_good = tick
                                state.last_good_at = datetime.now(UTC)
                                if not is_backfill:
                                    publisher.publish(service_id, tick)
                            state.last_error = None
                        else:
                            tick = error_tick_payload(state.last_good)
                            publisher.publish(service_id, tick)

                except _LongPollTimeout:
                    publisher.publish(service_id, gap_tick_payload())

                except Exception as exc:
                    state.last_error = str(exc)
                    logger.warning("RT poll error for %s: %s", service_id, exc)
                    tick = error_tick_payload(state.last_good)
                    publisher.publish(service_id, tick)

                elapsed = time.monotonic() - t0
                remaining = max(0, POLL_INTERVAL - elapsed)
                if remaining > 0:
                    state.stop_event.wait(timeout=remaining)
        finally:
            session.close()
            if redis_client:
                redis_client.close()

    def _fetch_realtime(self, session: httpx.Client, service_id: str, cursor: int) -> dict | None:
        from backend import config

        api_key = config.get_fastly_api_key(service_id)
        fastly_service_id = config.get_fastly_service_id(service_id)
        if not api_key or not fastly_service_id:
            return None

        if cursor == 0:
            path = f"/v1/channel/{fastly_service_id}/ts/h?limit=120"
        else:
            path = f"/v1/channel/{fastly_service_id}/ts/{cursor}"

        url = RT_BASE + path
        headers = {"Fastly-Key": api_key}
        is_longpoll = cursor != 0
        timeout = httpx.Timeout(10.0, connect=3.0, read=LONGPOLL_READ_TIMEOUT) if is_longpoll else httpx.Timeout(10.0)

        def _attempt() -> dict:
            try:
                resp = session.get(url, headers=headers, timeout=timeout)
            except httpx.ReadTimeout:
                if is_longpoll:
                    raise _LongPollTimeout from None
                raise
            resp.raise_for_status()
            return resp.json()

        return retry_call(
            _attempt,
            retry_on=_is_retryable_fetch_error,
            attempts=3,
            base_delay=1.0,
            multiplier=2.0,
            max_delay=4.0,
            sleep=_retry_sleep,
        )


poller = RealtimePoller()
