"""Realtime poller + transform unit tests.

Covers:
- transform_rt_response aggregation math
- error_tick_payload shape
- mock mode returns valid shape
- poller lifecycle (start / suspend on no subscribers)
- cursor advances between polls
"""

from __future__ import annotations

import asyncio

import pytest

from backend.core.realtime.transform import error_tick_payload, transform_rt_response

# ── transform_rt_response ───────────────────────────────────────────────────


class TestTransformRtResponse:
    def test_basic_aggregation(self):
        rt = {
            "Data": [
                {
                    "aggregated": {
                        "requests": 100,
                        "status_2xx": 90,
                        "status_4xx": 7,
                        "status_5xx": 3,
                        "hits": 80,
                        "miss": 20,
                        "pass": 0,
                        "synth": 0,
                        "resp_body_bytes": 1_000_000,
                        "resp_header_bytes": 10_000,
                        "shield": 10,
                        "shield_resp_body_bytes": 200_000,
                        "shield_resp_header_bytes": 2000,
                        "bereq_header_bytes": 5000,
                        "bereq_body_bytes": 0,
                        "waf_blocked": 1,
                        "waf_logged": 2,
                        "waf_passed": 97,
                    }
                }
            ],
            "Timestamp": 1720000000,
            "AggregateDelay": 3,
        }
        tick = transform_rt_response(rt)

        assert tick["event"] == "metrics_tick"
        assert tick["event_schema_version"] == 2
        assert tick["status"] == "ok"
        assert tick["aggregate_delay"] == 3

        data = tick["data"]
        assert data["requests_per_second"] == 100.0
        assert data["error_rate"] == pytest.approx(0.1, abs=0.001)
        assert data["cache_hit_ratio"] == pytest.approx(0.8, abs=0.001)
        assert data["bandwidth_mbps"] > 0
        assert "status_4xx" in data["status_breakdown"]
        assert "status_5xx" in data["status_breakdown"]
        assert data["estimated_cost_usd"] >= 0
        assert data["total_requests"] == 100
        assert data["total_hits"] == 80
        assert data["total_miss"] == 20
        assert data["total_pass"] == 0
        assert data["total_errors"] == 10
        assert data["origin_requests_per_second"] == 20.0
        assert data["shield_hit_ratio"] == pytest.approx(0.1, abs=0.001)
        assert data["waf_blocked"] == 1
        assert data["waf_logged"] == 2
        assert data["pop_count"] == 0

    def test_multiple_data_points(self):
        rt = {
            "Data": [
                {
                    "aggregated": {
                        "requests": 50,
                        "hits": 40,
                        "miss": 10,
                        "resp_body_bytes": 500_000,
                        "resp_header_bytes": 5000,
                    }
                },
                {
                    "aggregated": {
                        "requests": 50,
                        "hits": 40,
                        "miss": 10,
                        "resp_body_bytes": 500_000,
                        "resp_header_bytes": 5000,
                    }
                },
            ],
            "Timestamp": 1720000001,
        }
        tick = transform_rt_response(rt)
        data = tick["data"]
        assert data["requests_per_second"] == 50.0
        assert data["cache_hit_ratio"] == pytest.approx(0.8, abs=0.001)

    def test_empty_data_array(self):
        tick = transform_rt_response({"Data": [], "Timestamp": 0})
        data = tick["data"]
        assert data["requests_per_second"] == 0
        assert data["error_rate"] == 0.0
        assert data["cache_hit_ratio"] == 0.0
        assert data["bandwidth_mbps"] == 0.0

    def test_missing_data_key(self):
        tick = transform_rt_response({"Timestamp": 0})
        assert tick["data"]["requests_per_second"] == 0

    def test_datacenter_format(self):
        rt = {
            "Data": [
                {
                    "datacenter": {
                        "SJC": {
                            "requests": 200,
                            "status_5xx": 0,
                            "hits": 180,
                            "miss": 20,
                            "resp_body_bytes": 2_000_000,
                            "resp_header_bytes": 20_000,
                        },
                        "DCA": {
                            "requests": 100,
                            "status_5xx": 10,
                            "hits": 80,
                            "miss": 20,
                            "resp_body_bytes": 1_000_000,
                            "resp_header_bytes": 10_000,
                        },
                    }
                }
            ],
            "Timestamp": 1720000002,
        }
        tick = transform_rt_response(rt)
        data = tick["data"]
        assert data["pop_count"] == 2
        assert "DCA" in data["degraded_pops"]
        assert "SJC" not in data["degraded_pops"]


# ── error_tick_payload ──────────────────────────────────────────────────────


class TestErrorTickPayload:
    def test_no_last_good(self):
        tick = error_tick_payload()
        assert tick["status"] == "rt_down"
        assert tick["data"]["requests_per_second"] == 0

    def test_with_last_good(self):
        last_good = {
            "data": {
                "requests_per_second": 42,
                "error_rate": 0.01,
                "cache_hit_ratio": 0.95,
                "bandwidth_mbps": 1.5,
                "status_breakdown": {},
                "estimated_cost_usd": 0.001,
            }
        }
        tick = error_tick_payload(last_good)
        assert tick["status"] == "rt_down"
        assert tick["data"]["requests_per_second"] == 42


# ── mock mode ───────────────────────────────────────────────────────────────


def _test_mock_rt_response() -> dict:
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


def test_mock_rt_response_valid_shape():
    rt = _test_mock_rt_response()
    assert "Data" in rt
    assert "Timestamp" in rt
    assert len(rt["Data"]) > 0

    tick = transform_rt_response(rt)
    assert tick["status"] == "ok"
    assert tick["data"]["requests_per_second"] > 0


# ── poller lifecycle ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_poller_starts_and_suspends(monkeypatch):
    """Poller starts on ensure_polling, suspends after 2 idle cycles."""
    from backend.core.realtime import poller as poller_mod
    from backend.core.realtime.publisher import RealtimeMetricsPublisher

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())
    monkeypatch.setattr(poller_mod, "publisher", test_publisher)
    monkeypatch.setattr(poller_mod, "POLL_INTERVAL", 0)

    fetch_count = 0
    original_fetch = poller_mod.RealtimePoller._fetch_realtime

    def _fake_fetch(self, session, service_id, cursor):
        nonlocal fetch_count
        fetch_count += 1
        return _test_mock_rt_response()

    monkeypatch.setattr(poller_mod.RealtimePoller, "_fetch_realtime", _fake_fetch)

    p = poller_mod.RealtimePoller()
    p.ensure_polling("test-svc")

    await asyncio.sleep(0.3)

    state = p._services.get("test-svc")
    assert state is not None
    assert state.thread is None or not state.thread.is_alive()
    assert fetch_count >= 1


async def _drain(sub_iter):
    """Consume published ticks until cancelled."""
    try:
        async for _ in sub_iter:
            pass
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_cursor_advances(monkeypatch):
    """Cursor from RT response is used in subsequent polls."""
    import threading

    from backend.core.realtime import poller as poller_mod
    from backend.core.realtime.publisher import RealtimeMetricsPublisher

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())
    monkeypatch.setattr(poller_mod, "publisher", test_publisher)
    monkeypatch.setattr(poller_mod, "POLL_INTERVAL", 0)

    cursors_seen: list[int] = []
    got_two = threading.Event()

    def _fake_fetch(self, session, service_id, cursor):
        cursors_seen.append(cursor)
        resp = _test_mock_rt_response()
        resp["Timestamp"] = 1720000000 + len(cursors_seen)
        if len(cursors_seen) >= 2:
            got_two.set()
        return resp

    monkeypatch.setattr(poller_mod.RealtimePoller, "_fetch_realtime", _fake_fetch)

    p = poller_mod.RealtimePoller()

    # Keep a subscriber alive by draining ticks in the background so the
    # poller thread doesn't suspend between polls.
    sub_iter = test_publisher.subscribe("test-svc")
    drain_task = asyncio.ensure_future(_drain(sub_iter))
    # Yield so the drain coroutine enters `async for` and registers the
    # subscriber queue — otherwise the thread starts and sees
    # subscriber_count=0 before the event loop has a tick.
    await asyncio.sleep(0)

    p.ensure_polling("test-svc")

    # Wait for the thread to poll at least twice without blocking the
    # asyncio loop (publish uses call_soon_threadsafe, which needs the
    # loop to be running for the drain subscriber to consume ticks and
    # keep subscriber_count > 0).
    await asyncio.get_running_loop().run_in_executor(None, got_two.wait, 2.0)

    drain_task.cancel()
    try:
        await drain_task
    except (asyncio.CancelledError, StopAsyncIteration):
        pass

    assert len(cursors_seen) >= 2
    assert cursors_seen[0] == 0
    assert cursors_seen[1] > 0
