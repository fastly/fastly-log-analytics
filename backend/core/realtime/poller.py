"""Realtime metrics polling coordinator.

Polls ``rt.fastly.com`` every second per service, transforms the
response, and publishes to :data:`publisher`. Suspends automatically
when no SSE subscribers are connected; resumes with a backfill window
on the next ``ensure_polling()`` call.

The poller runs in a dedicated daemon thread per service — not on the
FastAPI event loop — so DuckDB queries and cron work cannot starve the
1-second cadence. ``requests.Session`` provides HTTP keep-alive so each
poll reuses the TLS connection (~100ms vs ~800ms per request).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import requests as req_lib

from backend.core.realtime.publisher import publisher
from backend.core.realtime.transform import (
    error_tick_payload,
    gap_tick_payload,
    transform_rt_response,
    transform_single_second,
)

logger = logging.getLogger(__name__)

RT_BASE = "https://rt.fastly.com"
POLL_INTERVAL = 1

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

LONGPOLL_READ_TIMEOUT = 1.2


class _LongPollTimeout(Exception):
    """rt.fastly.com /ts/ endpoint didn't respond within LONGPOLL_READ_TIMEOUT."""


@dataclass
class _ServiceState:
    cursor: int = 0
    thread: threading.Thread | None = None
    last_error: str | None = None
    last_good: dict | None = None
    last_good_at: datetime | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    backfill_done: threading.Event = field(default_factory=threading.Event)


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
                state.backfill_done.clear()
            t = threading.Thread(
                target=self._poll_loop,
                args=(service_id, state),
                daemon=True,
                name=f"rt-poller-{service_id[:8]}",
            )
            state.thread = t
            t.start()

    def wait_for_backfill(self, service_id: str, timeout: float = 10.0) -> bool:
        with self._lock:
            state = self._services.get(service_id)
        if state is None:
            return False
        return state.backfill_done.wait(timeout=timeout)

    def _poll_loop(self, service_id: str, state: _ServiceState) -> None:
        idle_cycles = 0
        session = req_lib.Session()
        session.headers["Fastly-Key"] = ""

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
                            state.backfill_done.set()
                        else:
                            tick = transform_rt_response(rt_json)
                            state.last_good = tick
                            state.last_good_at = datetime.now(UTC)
                            if not is_backfill:
                                publisher.publish(service_id, tick)
                            else:
                                state.backfill_done.set()
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

    def _fetch_realtime(self, session: req_lib.Session, service_id: str, cursor: int) -> dict | None:
        from backend.core.fastly.mock_fixtures import is_mock_mode

        if is_mock_mode():
            return _mock_rt_response()

        from backend import config

        api_key = config.get_fastly_api_key(service_id)
        cdn_service_id = config.get_fastly_service_id(service_id)
        if not api_key or not cdn_service_id:
            return None

        if cursor == 0:
            path = f"/v1/channel/{cdn_service_id}/ts/h?limit=120"
        else:
            path = f"/v1/channel/{cdn_service_id}/ts/{cursor}"

        session.headers["Fastly-Key"] = api_key
        url = RT_BASE + path
        is_longpoll = cursor != 0
        timeout: float | tuple[float, float] = (3, LONGPOLL_READ_TIMEOUT) if is_longpoll else 10

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                resp = session.get(url, timeout=timeout)
                if resp.status_code in _RETRYABLE_STATUS:
                    last_exc = req_lib.HTTPError(response=resp)
                    if attempt < 2:
                        time.sleep(min(2**attempt, 4))
                    continue
                resp.raise_for_status()
                return resp.json()
            except req_lib.ReadTimeout:
                if is_longpoll:
                    raise _LongPollTimeout
                last_exc = req_lib.ReadTimeout()
                if attempt < 2:
                    time.sleep(min(2**attempt, 4))
                continue
            except (req_lib.ConnectionError, req_lib.Timeout) as exc:
                last_exc = exc
                if attempt < 2:
                    time.sleep(min(2**attempt, 4))
                continue
        if last_exc:
            raise last_exc
        return None


def _mock_rt_response() -> dict:
    """Canned rt.fastly.com response for FASTLY_MOCK_MODE."""
    return {
        "Data": [
            {
                "aggregated": {
                    "requests": 150,
                    "status_2xx": 140,
                    "status_3xx": 5,
                    "status_4xx": 3,
                    "status_5xx": 2,
                    "hits": 120,
                    "miss": 30,
                    "pass": 0,
                    "synth": 0,
                    "resp_body_bytes": 5_000_000,
                    "resp_header_bytes": 50_000,
                    "shield": 10,
                    "shield_resp_body_bytes": 2_000_000,
                    "shield_resp_header_bytes": 20_000,
                    "bereq_header_bytes": 10_000,
                    "bereq_body_bytes": 0,
                    "waf_blocked": 1,
                    "waf_logged": 2,
                    "waf_passed": 147,
                    "origin_offload": 0.78,
                    "hit_time": 0.001,
                    "miss_time": 0.045,
                    "pass_time": 0.032,
                    "http2": 95,
                    "http3": 40,
                    "ipv6": 20,
                    "tls_v12": 15,
                    "tls_v13": 120,
                    "status_200": 130,
                    "status_204": 5,
                    "status_301": 3,
                    "status_304": 2,
                    "status_404": 2,
                    "status_503": 1,
                    "object_size_10k": 45,
                    "object_size_100k": 50,
                    "object_size_1m": 25,
                    "origin_fetches": 15,
                    "origin_revalidations": 8,
                    "origin_cache_fetches": 5,
                    "shield_hit_requests": 8,
                    "shield_miss_requests": 2,
                    "shield_revalidations": 3,
                    "shield_fetch_body_bytes": 500_000,
                    "request_collapse_usable_count": 12,
                    "request_collapse_unusable_count": 3,
                    "segblock_origin_fetches": 2,
                    "segblock_shield_fetches": 1,
                    "bot_challenge_starts": 5,
                    "bot_challenges_issued": 4,
                    "bot_challenges_succeeded": 3,
                    "bot_challenges_failed": 1,
                    "bot_edge_requests_detected_count": 8,
                    "bot_edge_requests_verified_count": 2,
                    "bot_edge_requests_ai_crawler_count": 1,
                    "compute_execution_time_ms": 12.5,
                    "compute_request_time_ms": 18.3,
                    "compute_ram_used": 4_200_000,
                    "compute_bereq_errors": 0,
                    "compute_guest_errors": 0,
                    "restarts": 0,
                    "recv_sub_count": 150,
                    "recv_sub_time": 0.002,
                    "fetch_sub_count": 30,
                    "fetch_sub_time": 0.045,
                    "deliver_sub_count": 150,
                    "deliver_sub_time": 0.001,
                    "error_sub_count": 2,
                    "error_sub_time": 0.001,
                    "miss_histogram": {"10": 5, "20": 8, "30": 6, "60": 4, "120": 3, "250": 2},
                },
                "datacenter": {
                    "SJC": {
                        "requests": 80,
                        "status_5xx": 1,
                        "hits": 65,
                        "miss": 15,
                    },
                    "DCA": {
                        "requests": 70,
                        "status_5xx": 1,
                        "hits": 55,
                        "miss": 15,
                    },
                },
            }
        ],
        "Timestamp": 1720000000,
        "AggregateDelay": 3,
    }


poller = RealtimePoller()
