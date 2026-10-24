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

from backend.core.realtime.transform import error_tick_payload, gap_tick_payload, transform_rt_response


# ── Merged from tests/core/test_realtime_poller.py ──
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
    """Sanity-check the shared test fixture used to fake RT responses below."""
    rt = _test_mock_rt_response()
    assert "Data" in rt
    assert "Timestamp" in rt
    assert len(rt["Data"]) > 0

    tick = transform_rt_response(rt)
    assert tick["status"] == "ok"
    assert tick["data"]["requests_per_second"] > 0


def test_poller_fetch_without_credentials_skips_network(monkeypatch):
    """No Fastly credentials configured -> short-circuit, never touch rt.fastly.com.

    This is the real (non-mocked) behavior E2E relies on when a service has no
    real Fastly API key/service id seeded: the poller must not retry against
    the live API with garbage credentials (that caused a 403 retry storm that
    cascaded into unrelated E2E page-load timeouts).
    """
    from backend import config as config_mod
    from backend.core.realtime import poller as poller_mod

    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda service_id: None)
    monkeypatch.setattr(config_mod, "get_fastly_logging_service_id", lambda service_id: None)

    class _NoNetworkSession:
        headers: dict[str, str] = {}

        def get(self, *args, **kwargs):
            raise AssertionError("fetch with no credentials attempted network I/O")

    rt = poller_mod.RealtimePoller()._fetch_realtime(_NoNetworkSession(), "svc", 0)

    assert rt is None


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

    state = p._services.get("test-svc")
    assert state is not None
    for _ in range(100):
        await asyncio.sleep(0.05)
        if state.thread is None or not state.thread.is_alive():
            break
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


# ── Merged from tests/core/test_realtime_transform.py ──
FULL_AGG = {
    "requests": 1000,
    "hits": 800,
    "miss": 150,
    "pass": 40,
    "synth": 10,
    "status_1xx": 0,
    "status_2xx": 900,
    "status_3xx": 50,
    "status_4xx": 30,
    "status_5xx": 20,
    "resp_body_bytes": 5000000,
    "resp_header_bytes": 50000,
    "bereq_header_bytes": 10000,
    "bereq_body_bytes": 1000,
    "shield": 200,
    "shield_resp_body_bytes": 1000000,
    "shield_resp_header_bytes": 10000,
    "waf_blocked": 5,
    "waf_logged": 3,
    "waf_passed": 992,
    "origin_offload": 0.85,
    "hit_time": 0.001,
    "miss_time": 0.15,
    "pass_time": 0.08,
    "http2": 600,
    "http3": 300,
    "ipv6": 150,
    "tls_v12": 200,
    "tls_v13": 800,
    "ddos_action_blackhole": 2,
    "ddos_action_tarpit": 1,
    "ddos_action_close": 3,
    "ddos_action_downgrade": 0,
    "ddos_protection_requests_detect_count": 10,
    "ddos_protection_requests_mitigate_count": 6,
    "status_200": 850,
    "status_204": 50,
    "status_301": 20,
    "status_302": 15,
    "status_304": 15,
    "status_400": 10,
    "status_401": 5,
    "status_403": 8,
    "status_404": 5,
    "status_429": 2,
    "status_500": 8,
    "status_502": 5,
    "status_503": 4,
    "status_504": 3,
    "object_size_1k": 100,
    "object_size_10k": 300,
    "object_size_100k": 250,
    "object_size_1m": 200,
    "object_size_10m": 100,
    "object_size_100m": 30,
    "object_size_1g": 5,
    "object_size_other": 15,
    "origin_fetches": 160,
    "origin_revalidations": 40,
    "origin_cache_fetches": 20,
    "shield_hit_requests": 180,
    "shield_miss_requests": 20,
    "shield_revalidations": 10,
    "shield_fetch_body_bytes": 500000,
    "request_collapse_usable_count": 50,
    "request_collapse_unusable_count": 5,
}

RT_RESPONSE = {
    "Data": [{"aggregated": FULL_AGG, "datacenter": {}}],
    "Timestamp": 1720000000,
    "AggregateDelay": 7,
}


def test_new_fields_extracted():
    result = transform_rt_response(RT_RESPONSE)
    d = result["data"]

    # P4: Byte-Hit-Ratio
    assert d["origin_offload"] == 0.85

    # P1: Cache Path Latency (s → ms)
    assert d["hit_latency_ms"] == 1.0
    assert d["miss_latency_ms"] == 150.0
    assert d["pass_latency_ms"] == 80.0

    # T1: Protocol Adoption — raw counts
    assert d["http2"] == 600
    assert d["http3"] == 300
    assert d["ipv6"] == 150
    assert d["tls_v12"] == 200
    assert d["tls_v13"] == 800

    # T1: Protocol Adoption — percentages
    assert d["h2_pct"] == 60.0
    assert d["h3_pct"] == 30.0
    assert d["ipv6_pct"] == 15.0
    assert d["tls12_pct"] == 20.0
    assert d["tls13_pct"] == 80.0

    # S1: DDoS Mitigation
    assert d["ddos_action_blackhole"] == 2
    assert d["ddos_action_tarpit"] == 1
    assert d["ddos_action_close"] == 3
    assert d["ddos_action_downgrade"] == 0
    assert d["ddos_detect"] == 10
    assert d["ddos_mitigate"] == 6

    # S3: Individual Status Codes
    assert d["status_detail"]["200"] == 850
    assert d["status_detail"]["204"] == 50
    assert d["status_detail"]["301"] == 20
    assert d["status_detail"]["404"] == 5
    assert d["status_detail"]["503"] == 4

    # P3: Object Size Distribution
    assert d["object_size_distribution"]["1k"] == 100
    assert d["object_size_distribution"]["10k"] == 300
    assert d["object_size_distribution"]["100k"] == 250
    assert d["object_size_distribution"]["1g"] == 5

    # O1: Origin Fetch Detail
    assert d["origin_fetches"] == 160
    assert d["origin_revalidations"] == 40
    assert d["origin_cache_fetches"] == 20

    # O2: Shield Efficiency
    assert d["shield_hit_requests"] == 180
    assert d["shield_miss_requests"] == 20
    assert d["shield_revalidations"] == 10
    assert d["shield_fetch_body_bytes"] == 500000

    # T3: Request Collapse
    assert d["request_collapse_usable"] == 50
    assert d["request_collapse_unusable"] == 5


def test_gap_tick_has_new_fields():
    gap = gap_tick_payload()
    d = gap["data"]

    assert d["origin_offload"] == 0.0
    assert d["hit_latency_ms"] == 0.0
    assert d["miss_latency_ms"] == 0.0
    assert d["pass_latency_ms"] == 0.0
    assert d["http2"] == 0
    assert d["http3"] == 0
    assert d["ipv6"] == 0
    assert d["tls_v12"] == 0
    assert d["tls_v13"] == 0
    assert d["h2_pct"] == 0.0
    assert d["h3_pct"] == 0.0
    assert d["ipv6_pct"] == 0.0
    assert d["tls12_pct"] == 0.0
    assert d["tls13_pct"] == 0.0
    assert d["ddos_action_blackhole"] == 0
    assert d["ddos_action_tarpit"] == 0
    assert d["ddos_action_close"] == 0
    assert d["ddos_action_downgrade"] == 0
    assert d["ddos_detect"] == 0
    assert d["ddos_mitigate"] == 0
    assert d["status_detail"] == {}
    assert d["object_size_distribution"] == {}
    assert d["origin_fetches"] == 0
    assert d["origin_revalidations"] == 0
    assert d["origin_cache_fetches"] == 0
    assert d["shield_hit_requests"] == 0
    assert d["shield_miss_requests"] == 0
    assert d["shield_revalidations"] == 0
    assert d["shield_fetch_body_bytes"] == 0
    assert d["request_collapse_usable"] == 0
    assert d["request_collapse_unusable"] == 0


def test_missing_new_fields_default_to_zero():
    minimal_agg = {
        "requests": 500,
        "hits": 400,
        "miss": 80,
        "pass": 15,
        "synth": 5,
        "status_1xx": 0,
        "status_2xx": 450,
        "status_3xx": 20,
        "status_4xx": 20,
        "status_5xx": 10,
        "resp_body_bytes": 2000000,
        "resp_header_bytes": 20000,
        "bereq_header_bytes": 5000,
        "bereq_body_bytes": 500,
        "shield": 100,
        "shield_resp_body_bytes": 500000,
        "shield_resp_header_bytes": 5000,
        "waf_blocked": 2,
        "waf_logged": 1,
        "waf_passed": 497,
    }
    rt = {
        "Data": [{"aggregated": minimal_agg, "datacenter": {}}],
        "Timestamp": 1720000000,
        "AggregateDelay": 5,
    }
    result = transform_rt_response(rt)
    d = result["data"]

    assert d["origin_offload"] == 0.0
    assert d["hit_latency_ms"] == 0.0
    assert d["miss_latency_ms"] == 0.0
    assert d["pass_latency_ms"] == 0.0
    assert d["http2"] == 0
    assert d["http3"] == 0
    assert d["ipv6"] == 0
    assert d["tls_v12"] == 0
    assert d["tls_v13"] == 0
    assert d["h2_pct"] == 0.0
    assert d["h3_pct"] == 0.0
    assert d["ipv6_pct"] == 0.0
    assert d["tls12_pct"] == 0.0
    assert d["tls13_pct"] == 0.0
    assert d["ddos_action_blackhole"] == 0
    assert d["ddos_action_tarpit"] == 0
    assert d["ddos_action_close"] == 0
    assert d["ddos_action_downgrade"] == 0
    assert d["ddos_detect"] == 0
    assert d["ddos_mitigate"] == 0
    assert d["status_detail"] == {}
    assert d["object_size_distribution"] == {}
    assert d["origin_fetches"] == 0
    assert d["origin_revalidations"] == 0
    assert d["origin_cache_fetches"] == 0
    assert d["shield_hit_requests"] == 0
    assert d["shield_miss_requests"] == 0
    assert d["shield_revalidations"] == 0
    assert d["shield_fetch_body_bytes"] == 0
    assert d["request_collapse_usable"] == 0
    assert d["request_collapse_unusable"] == 0


def test_multi_second_aggregation():
    agg1 = {
        "requests": 1000,
        "hits": 800,
        "miss": 150,
        "pass": 40,
        "synth": 10,
        "status_2xx": 900,
        "status_4xx": 30,
        "status_5xx": 20,
        "resp_body_bytes": 5000000,
        "resp_header_bytes": 50000,
        "bereq_header_bytes": 10000,
        "bereq_body_bytes": 1000,
        "shield": 200,
        "shield_resp_body_bytes": 1000000,
        "shield_resp_header_bytes": 10000,
        "waf_blocked": 5,
        "waf_logged": 3,
        "waf_passed": 992,
        "origin_offload": 0.90,
        "hit_time": 0.002,
        "miss_time": 0.10,
        "pass_time": 0.06,
        "http2": 600,
        "http3": 300,
        "ipv6": 100,
        "tls_v12": 200,
        "tls_v13": 800,
        "ddos_action_blackhole": 2,
        "ddos_action_tarpit": 1,
        "ddos_action_close": 3,
        "ddos_action_downgrade": 0,
        "ddos_protection_requests_detect_count": 10,
        "ddos_protection_requests_mitigate_count": 6,
        "status_200": 800,
        "status_404": 5,
        "object_size_1k": 100,
        "object_size_10k": 200,
        "origin_fetches": 100,
        "origin_revalidations": 30,
        "origin_cache_fetches": 10,
        "shield_hit_requests": 150,
        "shield_miss_requests": 30,
        "shield_revalidations": 5,
        "shield_fetch_body_bytes": 400000,
        "request_collapse_usable_count": 40,
        "request_collapse_unusable_count": 3,
    }
    agg2 = {
        "requests": 1000,
        "hits": 700,
        "miss": 200,
        "pass": 80,
        "synth": 20,
        "status_2xx": 850,
        "status_4xx": 50,
        "status_5xx": 30,
        "resp_body_bytes": 6000000,
        "resp_header_bytes": 60000,
        "bereq_header_bytes": 12000,
        "bereq_body_bytes": 2000,
        "shield": 180,
        "shield_resp_body_bytes": 900000,
        "shield_resp_header_bytes": 9000,
        "waf_blocked": 3,
        "waf_logged": 2,
        "waf_passed": 995,
        "origin_offload": 0.80,
        "hit_time": 0.004,
        "miss_time": 0.20,
        "pass_time": 0.10,
        "http2": 400,
        "http3": 200,
        "ipv6": 200,
        "tls_v12": 300,
        "tls_v13": 700,
        "ddos_action_blackhole": 1,
        "ddos_action_tarpit": 0,
        "ddos_action_close": 2,
        "ddos_action_downgrade": 1,
        "ddos_protection_requests_detect_count": 8,
        "ddos_protection_requests_mitigate_count": 4,
        "status_200": 750,
        "status_404": 10,
        "object_size_1k": 50,
        "object_size_10k": 300,
        "origin_fetches": 120,
        "origin_revalidations": 20,
        "origin_cache_fetches": 15,
        "shield_hit_requests": 130,
        "shield_miss_requests": 40,
        "shield_revalidations": 8,
        "shield_fetch_body_bytes": 600000,
        "request_collapse_usable_count": 30,
        "request_collapse_unusable_count": 7,
    }

    rt = {
        "Data": [
            {"aggregated": agg1, "datacenter": {}},
            {"aggregated": agg2, "datacenter": {}},
        ],
        "Timestamp": 1720000000,
        "AggregateDelay": 5,
    }
    result = transform_rt_response(rt)
    d = result["data"]

    # Count fields are SUMMED
    assert d["http2"] == 1000
    assert d["http3"] == 500
    assert d["ipv6"] == 300
    assert d["tls_v12"] == 500
    assert d["tls_v13"] == 1500
    assert d["ddos_action_blackhole"] == 3
    assert d["ddos_action_tarpit"] == 1
    assert d["ddos_action_close"] == 5
    assert d["ddos_action_downgrade"] == 1
    assert d["ddos_detect"] == 18
    assert d["ddos_mitigate"] == 10
    assert d["origin_fetches"] == 220
    assert d["origin_revalidations"] == 50
    assert d["origin_cache_fetches"] == 25
    assert d["shield_hit_requests"] == 280
    assert d["shield_miss_requests"] == 70
    assert d["shield_revalidations"] == 13
    assert d["shield_fetch_body_bytes"] == 1000000
    assert d["request_collapse_usable"] == 70
    assert d["request_collapse_unusable"] == 10

    # Status detail summed
    assert d["status_detail"]["200"] == 1550
    assert d["status_detail"]["404"] == 15

    # Object size distribution summed
    assert d["object_size_distribution"]["1k"] == 150
    assert d["object_size_distribution"]["10k"] == 500

    # Latency fields are AVERAGED (sum / n * 1000)
    assert d["hit_latency_ms"] == round((0.002 + 0.004) / 2 * 1000, 2)  # 3.0
    assert d["miss_latency_ms"] == round((0.10 + 0.20) / 2 * 1000, 2)  # 150.0
    assert d["pass_latency_ms"] == round((0.06 + 0.10) / 2 * 1000, 2)  # 80.0

    # Origin offload averaged
    assert d["origin_offload"] == round((0.90 + 0.80) / 2, 4)  # 0.85

    # Percentages computed from totals (total_requests = 2000)
    assert d["h2_pct"] == 50.0  # 1000/2000 * 100
    assert d["h3_pct"] == 25.0  # 500/2000 * 100
    assert d["ipv6_pct"] == 15.0  # 300/2000 * 100
    assert d["tls12_pct"] == 25.0  # 500/2000 * 100
    assert d["tls13_pct"] == 75.0  # 1500/2000 * 100
