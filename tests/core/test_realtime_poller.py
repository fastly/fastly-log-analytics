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
                        "resp_body_bytes": 1_000_000,
                        "resp_header_bytes": 10_000,
                    }
                }
            ],
            "Timestamp": 1720000000,
            "AggregateDelay": 3,
        }
        tick = transform_rt_response(rt)

        assert tick["event"] == "metrics_tick"
        assert tick["event_schema_version"] == 1
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
                            "hits": 180,
                            "miss": 20,
                            "resp_body_bytes": 2_000_000,
                            "resp_header_bytes": 20_000,
                        }
                    }
                }
            ],
            "Timestamp": 1720000002,
        }
        tick = transform_rt_response(rt)
        assert tick["data"]["requests_per_second"] == 200.0


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


def test_mock_rt_response_valid_shape():
    from backend.core.realtime.poller import _mock_rt_response

    rt = _mock_rt_response()
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

    async def _fake_fetch(self, service_id, cursor):
        nonlocal fetch_count
        fetch_count += 1
        return poller_mod._mock_rt_response()

    monkeypatch.setattr(poller_mod.RealtimePoller, "_fetch_realtime", _fake_fetch)

    p = poller_mod.RealtimePoller()
    p.ensure_polling("test-svc")

    await asyncio.sleep(0.1)

    state = p._services.get("test-svc")
    assert state is not None
    assert state.task is None or state.task.done()
    assert fetch_count >= 1


@pytest.mark.asyncio
async def test_cursor_advances(monkeypatch):
    """Cursor from RT response is used in subsequent polls."""
    from backend.core.realtime import poller as poller_mod
    from backend.core.realtime.publisher import RealtimeMetricsPublisher

    test_publisher = RealtimeMetricsPublisher()
    test_publisher.bind_loop(asyncio.get_running_loop())
    monkeypatch.setattr(poller_mod, "publisher", test_publisher)
    monkeypatch.setattr(poller_mod, "POLL_INTERVAL", 0)

    cursors_seen: list[int] = []

    async def _fake_fetch(self, service_id, cursor):
        cursors_seen.append(cursor)
        resp = poller_mod._mock_rt_response()
        resp["Timestamp"] = 1720000000 + len(cursors_seen)
        return resp

    monkeypatch.setattr(poller_mod.RealtimePoller, "_fetch_realtime", _fake_fetch)

    p = poller_mod.RealtimePoller()

    # Add a subscriber so the poller doesn't suspend
    sub_iter = test_publisher.subscribe("test-svc")
    sub_task = asyncio.ensure_future(sub_iter.__anext__())

    p.ensure_polling("test-svc")
    await asyncio.sleep(0.05)

    # Cancel subscriber to let poller suspend
    sub_task.cancel()
    try:
        await sub_task
    except (asyncio.CancelledError, StopAsyncIteration):
        pass

    await asyncio.sleep(0.05)

    assert len(cursors_seen) >= 2
    assert cursors_seen[0] == 0
    assert cursors_seen[1] > 0


@pytest.mark.asyncio
async def test_fetch_mock_mode(monkeypatch):
    """FASTLY_MOCK_MODE returns valid shape from _fetch_realtime."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")

    from backend.core.realtime.poller import RealtimePoller

    p = RealtimePoller()
    result = await p._fetch_realtime("test-svc", 0)
    assert result is not None
    assert "Data" in result
