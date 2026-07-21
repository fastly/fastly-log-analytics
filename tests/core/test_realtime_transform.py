"""Tests for the RT API → SSE metrics_tick transform."""

from __future__ import annotations

from backend.core.realtime.transform import gap_tick_payload, transform_rt_response

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
