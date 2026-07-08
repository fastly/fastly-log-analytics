"""Realtime metrics polling coordinator.

Polls ``rt.fastly.com`` every 5 seconds per service, transforms the
response, and publishes to :data:`publisher`. Suspends automatically
when no SSE subscribers are connected; resumes with a backfill window
on the next ``ensure_polling()`` call.

The poller runs as an asyncio task on the FastAPI event loop — no
background threads, no scheduler dependency.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime

import tenacity

from backend.core.realtime.publisher import publisher
from backend.core.realtime.transform import error_tick_payload, transform_rt_response

logger = logging.getLogger(__name__)

RT_BASE = "https://rt.fastly.com"
POLL_INTERVAL = 5


@dataclass
class _ServiceState:
    cursor: int = 0
    task: asyncio.Task | None = None
    last_error: str | None = None
    last_good: dict | None = None
    last_good_at: datetime | None = None


class RealtimePoller:
    def __init__(self) -> None:
        self._services: dict[str, _ServiceState] = {}

    def ensure_polling(self, service_id: str) -> None:
        state = self._services.get(service_id)
        if state and state.task and not state.task.done():
            return
        if state is None:
            state = _ServiceState()
            self._services[service_id] = state
        state.task = asyncio.ensure_future(self._poll_loop(service_id, state))

    async def _poll_loop(self, service_id: str, state: _ServiceState) -> None:
        idle_cycles = 0
        while True:
            try:
                if publisher.subscriber_count(service_id) == 0:
                    idle_cycles += 1
                    if idle_cycles > 1:
                        logger.debug("RT poller suspending for %s (no subscribers)", service_id)
                        state.task = None
                        return
                else:
                    idle_cycles = 0

                rt_json = await self._fetch_realtime(service_id, state.cursor)
                if rt_json is not None:
                    state.cursor = rt_json.get("Timestamp", state.cursor)
                    tick = transform_rt_response(rt_json)
                    state.last_good = tick
                    state.last_good_at = datetime.now(UTC)
                    state.last_error = None
                    publisher.publish(service_id, tick)
                else:
                    tick = error_tick_payload(state.last_good)
                    publisher.publish(service_id, tick)

            except Exception as exc:
                state.last_error = str(exc)
                logger.warning("RT poll error for %s: %s", service_id, exc)
                tick = error_tick_payload(state.last_good)
                publisher.publish(service_id, tick)

            await asyncio.sleep(POLL_INTERVAL)

    async def _fetch_realtime(self, service_id: str, cursor: int) -> dict | None:
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

        return await asyncio.to_thread(_fetch_rt_sync, path, api_key)


def _fetch_rt_sync(path: str, api_key: str) -> dict:
    url = RT_BASE + path

    try:
        from backend.utils.telemetry import tracked_call

        ctx = tracked_call("GET", path, service="Fastly RT")
    except ImportError:
        ctx = None

    def _do() -> dict:
        result: dict = {}
        for attempt in tenacity.Retrying(
            retry=tenacity.retry_if_exception(
                lambda exc: (
                    isinstance(exc, (urllib.error.URLError, ConnectionError, TimeoutError))
                    or (isinstance(exc, urllib.error.HTTPError) and exc.code in (429, 500, 502, 503, 504))
                )
            ),
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_exponential(multiplier=1, min=1, max=8) + tenacity.wait_random(min=0, max=2),
            reraise=True,
        ):
            with attempt:
                req = urllib.request.Request(url, headers={"Fastly-Key": api_key}, method="GET")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode())
        return result

    if ctx:
        with ctx:
            return _do()
    return _do()


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
