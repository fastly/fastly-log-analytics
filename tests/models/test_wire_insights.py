"""Insight card/meta models keep every producer's wire bytes (``InsightItem.meta`` per detection id)."""

from __future__ import annotations

import json
import typing
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest

from backend.high_scale.insights import insights_endpoint as hs_insights_endpoint
from backend.models.insights import InsightCard, InsightsResponse
from backend.repositories._base import _safe_table
from backend.repositories.insights import _insights_cache, get_insights
from backend.repositories.insights.registry import registry
from tests._helpers.wire import wire
from tests.repositories.test_insights import (
    _MOCK_POP_MAP,
    _create_impossible_distance_table,
    _seed_network_edge_data,
    _seed_track_c_data,
)

_TAGS = {
    t.model_fields["id"].annotation.__args__[0]  # type: ignore[union-attr]
    for t in typing.get_args(typing.get_args(InsightCard)[0])
}

_CONTEXT: dict[str, Any] = {
    "window_hours": 24.0,
    "baseline_hours": 168.0,
    "fp_col": "ja4",
    "actual_cols": {"ja4", "ip"},
    "mask_ips": False,
    "asn_names": {64500: "Example ISP"},
    "dma_map": {"501": "New York"},
}

_GEO = ("Berlin, DE", "Berlin", "BE", "DE")

# One realistic SQL row per processor (schemas are the ``row schema`` comments in definitions.py);
# variants exercise the None / zero branches that change a meta value's JSON type.
_ROWS: dict[str, list[tuple]] = {
    "error_spikes": [("/api/x", 0.6, 0.01, 30, 50, 500)],
    "botnet_grouping": [("abc123", 80, 900, 2, 12.34)],
    "new_country_traffic": [("DE", 25, 0)],
    "city_surges": [(*_GEO, 500, 100, 5.55)],
    "city_error_spikes": [(*_GEO, 0.7, 0.01, 70, 100, 1000)],
    "city_latency_regressions": [(*_GEO, 6000.0, 1200.0, 300, 3000), (*_GEO, 6000.0, 0.0, 300, 3000)],
    "new_city_traffic": [(*_GEO, 60, 0), ("Tokyo, JP", "Tokyo", "", "JP", 60, 0)],
    "ua_monoculture": [("curl/8.0", 600, 10, 1000, 1000)],
    "new_probe_urls": [("/.env", 12, 0, 83.3), ("/.git", 12, 0, None)],
    "waf_signal_spikes": [("SQLI", 100, 20, 12.5)],
    "proxy_surge": [("vpn", 300, 10, 1000, 5000)],
    "asn_concentration": [(64500, 800, 20, 1000, 1000)],
    "asn_metro_performance": [(64500, 501, 150.0, 30.0, 400, 4000), (64500, 501, 150.0, 0.0, 400, 4000)],
    "cache_collapse": [("/a", 0.05, 0.9, 120, 1500)],
    "cacheability_regression": [("/a", 0.9, 0.05, 130, 1600)],
    "latency_regression": [("/a", 6000.0, 1000.0, 120, 1500), ("/a", 6000.0, 0.0, 120, 1500)],
    "impossible_distance": [
        ("fp1", 4, 7000.0, 8880.0, 200.0, "JFK", "1.2.3.4", 0.0, 0.0, 40.641, -73.778, 1000, "US", "Nowhere"),
        ("fp2", 2, 7000.0, 8880.0, 200.0, "JFK", None, 1.5, 2.5, 40.641, -73.778, 1000, None, None),
    ],
    "tail_latency": [("/a", 900.0, 50.0, 18.0, 40)],
    "cipher_spread": [("a" * 40, 60, 900, 12)],
    "request_size_anomaly": [("1.2.3.4", 70000, 5000.5, 30, 4000.0)],
    "connection_abuse": [("1.2.3.4", 700, 20.4, 30, 15.0)],
    "region_latency": [("us-east", 6000.0, 1000.0, 100, 1000, 200.5), ("us-west", 6000.0, 0.0, 100, 1000, None)],
    "cache_ttl_mismatch": [("/a", 86400.0, 1.2, 300.0, 40)],
    "image_optimization_opportunities": [
        ("/i.png", 500, 40_000_000, 80.5, 0.62),
        ("/j.png", 5, 3_000_000, 600.0, None),
    ],
    "origin_latency_spike": [("/a", 900.0, 900.0, 100.0, 80)],
    "origin_error_rate": [(503, 40, 400, 4000, 40, 5)],
    "origin_retries": [("/a", 40, 1.8, 5)],
    "origin_ip_failure": [("203.0.113.1", 300, 35.2, 1.04)],
    "shield_path_degradation": [("SJC", "IAD", 90.0, 20.0, 70), ("SJC", "IAD", 90.0, 0.0, 70)],
    "repeated_patterns": [
        ("1.2.3.4", 40, 41, 30.0, 0.5, 0.02, 0.95, 1, 1230, 30),
        ("1.2.3.5", 4, 5, 30.5, 9.5, 0.4, 0.2, 2, 0, None),
    ],
    "repeated_patterns_fp": [
        ("t13d1516h2_8daaf6152771", 40, 41, 30.0, 0.5, 0.02, 0.95, 7, 1230, 30),
        ("t13d1516h2_other", 4, 5, 30.5, 9.5, 0.4, 0.2, 2, 0, None),
    ],
    "low_and_slow": [("1.2.3.4", 12, 6, 3600, 0.0033)],
    "credential_enumeration": [("1.2.3.4", 150, 400, 5, 3), ("1.2.3.5", 5, 0, 1, 0)],
    "network_asn_health": [(64500, 0.08, 0.001, 5000.0, 1000.0, 2.5, 0.1, 60)],
    "content_discovery": [("1.2.3.4", 120, 300, 80, 2), ("1.2.3.5", 5, 0, 1, 0)],
    "referer_monoculture": [("https://example.com/", 600, 5, 1000, 1000)],
    "method_drift": [("POST", 300, 5, 1000, 1000), (None, 300, 5, 1000, 1000)],
    "new_asn_traffic": [(64501, 150, 0)],
    "asn_hosting_shift": [(64502, 80, 100, 10, 100)],
    "metro_delivery_degradation": [(501, 1_000_000.0, 10_000_000.0, 150)],
    "connection_type_mix": [("cellular", "mobile", 120, 5, 150, 300), (None, None, 120, 5, 150, 300)],
    "pop_latency_regression": [("JFK", 6000.0, 1000.0, 150, 900), ("JFK", 6000.0, 0.0, 150, 900)],
    "http3_fallback": [(0, 150, 240, 300)],
    "cache_hit_cliff": [(0, 150, 210, 300)],
    "payload_compression_regression": [("/app.js", 0.95, 0.02, 120, 300)],
    "session_harvesting": [("1.2.3.4", 120, 600, 3)],
    "timeout_split": [(500.0, 5.0, 60.0, 50.0, 60), (5.0, 5.0, 560.0, 60.0, 60)],
    "crawler_disparity": [
        ("GPTBot", 5000, 2, 10_240_000, 2500.0, 100.0, 5_120_000.0, 200_000.0),
        (None, 50, 0, 0, 50.0, 1.0, 0.0, 0.0),
    ],
    "bot_bandwidth_disparity": [
        ("Common Crawl (CCBot)", 500, 25_000_000, 42.5, 0, 500.0, 25_000_000.0),
        (None, 100, 5_000_000, 8.5, 2, 50.0, 2_500_000.0),
    ],
    "stale_browser_version": [("Chrome/90", 300, 12, 6.5)],
    "orphaned_deep_crawl": [("1.2.3.4", 600, 20, 10, 30.0, 12.5)],
    "residential_fingerprint_dispersion": [("t13d1516h2_8daaf6152771", 40, 12, 900, 3, 4.5)],
}

_ENVELOPE: dict[str, Any] = {
    "window_start": "2026-09-14T00:00:00Z",
    "window_end": "2026-09-15T00:00:00Z",
    "baseline_start": "2026-09-07T00:00:00Z",
    "baseline_end": "2026-09-14T00:00:00Z",
    "computed_at": "2026-09-15T00:00:00Z",
    "window_hours": 24.0,
    "baseline_hours": 168.0,
}


def _dump_cards(cards: list[dict]) -> list[dict]:
    model = InsightsResponse.model_validate({**_ENVELOPE, "insights": cards})
    return json.loads(model.model_dump_json(by_alias=True))["insights"]


def _assert_cards_wire_identical(cards: list[dict]) -> int:
    """Every item's ``meta`` serializes to the producer's exact bytes; the other item keys match by value."""
    dumped_cards = _dump_cards(cards)
    checked = 0
    for card, dumped in zip(cards, dumped_cards, strict=True):
        assert dumped["id"] == card["id"]
        assert len(dumped["items"]) == len(card["items"])
        for item, out in zip(card["items"], dumped["items"], strict=True):
            assert wire(out["meta"]) == wire(item["meta"]), f"{card['id']} meta wire bytes differ"
            for key, value in item.items():
                if key != "meta":
                    assert out[key] == pytest.approx(value) if isinstance(value, float) else out[key] == value
            checked += 1
    return checked


def _card_for(insight_id: str, items: list[dict]) -> dict:
    definition = registry.get(insight_id)
    assert definition is not None
    return {
        "id": insight_id,
        "title": definition.title,
        "description": definition.description,
        "category": str(definition.category),
        "severity": "warning",
        "summary": f"{len(items)} anomalies detected",
        "items": items,
    }


def test_every_registered_insight_has_a_card_variant_and_a_row_case():
    registered = {d.id for d in registry.get_all()}
    assert registered == set(_ROWS), "add a synthetic producer row for the new insight"
    assert registered | {"high_scale_5xx"} == _TAGS, "add an InsightCard variant for the new insight"


@pytest.mark.parametrize("insight_id", sorted(_ROWS))
def test_processor_meta_wire_identical(insight_id):
    definition = registry.get(insight_id)
    assert definition is not None and definition.row_processor is not None
    items = []
    for row in _ROWS[insight_id]:
        item = definition.row_processor(row, definition, dict(_CONTEXT))
        item["investigate_url"] = "/dashboard?service=svc&start=a&end=b"
        items.append(item)
    assert _assert_cards_wire_identical([_card_for(insight_id, items)]) == len(items)


def test_masked_ip_processors_keep_wire_bytes():
    for insight_id in ("request_size_anomaly", "repeated_patterns", "session_harvesting", "impossible_distance"):
        definition = registry.get(insight_id)
        assert definition is not None and definition.row_processor is not None
        item = definition.row_processor(_ROWS[insight_id][0], definition, {**_CONTEXT, "mask_ips": True})
        assert _assert_cards_wire_identical([_card_for(insight_id, [item])]) == 1


def test_unknown_insight_id_is_rejected():
    with pytest.raises(ValueError, match="insights"):
        _dump_cards([{**_card_for("error_spikes", []), "id": "not_an_insight"}])


def test_extra_meta_keys_survive_for_forward_compat():
    card = _card_for(
        "error_spikes", [{"label": "/a", "meta": {"requests": 1, "errors": 1, "filters": {}, "future": [1]}}]
    )
    assert _dump_cards([card])[0]["items"][0]["meta"]["future"] == [1]


def test_repository_impossible_distance_wire_identical(in_memory_duckdb, test_service_source):
    _create_impossible_distance_table(in_memory_duckdb, _safe_table(test_service_source["name"]))
    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_MOCK_POP_MAP):
        res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    cards = [c for c in res["insights"] if c["id"] == "impossible_distance"]
    assert _assert_cards_wire_identical(cards) > 0


def test_repository_network_edge_and_track_c_wire_identical(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        '"timestamp" TIMESTAMPTZ, "metro" USMALLINT, "delivery_rate" UBIGINT, "c_type" VARCHAR, '
        '"c_speed" VARCHAR, "pop" VARCHAR, "elapsed" UBIGINT, "transport" VARCHAR, "cache" VARCHAR, '
        '"url" VARCHAR, "status" USMALLINT, "resp_bytes" UBIGINT, "resp_header_content_encoding" VARCHAR, '
        '"ip" VARCHAR, "cookie_session" VARCHAR, "oconnect_ms" UINTEGER, "ottfb" UBIGINT)'
    )
    _seed_network_edge_data(in_memory_duckdb, table)
    _seed_track_c_data(in_memory_duckdb, table)
    _insights_cache.clear()
    res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
    cards = [c for c in res["insights"] if c["items"]]
    fired = {c["id"] for c in cards}
    assert {
        "metro_delivery_degradation",
        "connection_type_mix",
        "pop_latency_regression",
        "http3_fallback",
        "cache_hit_cliff",
        "payload_compression_regression",
        "session_harvesting",
        "timeout_split",
    } <= fired
    assert _assert_cards_wire_identical(cards) >= 8


def test_repository_network_asn_health_and_low_and_slow_wire_identical(in_memory_duckdb, test_service_source):
    from datetime import timedelta

    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table} "
        '("timestamp" TIMESTAMPTZ, "asn" INTEGER, "ploss" DOUBLE, "rtt_var" INTEGER, "retrans" DOUBLE, '
        '"ip" VARCHAR, "url" VARCHAR, "ja3" VARCHAR)'
    )
    now = datetime.now(UTC)
    for _ in range(60):
        in_memory_duckdb.execute(
            f"INSERT INTO {table} VALUES (?, 64500, 0.08, 1000, 0.0, NULL, NULL, NULL)",
            [(now - timedelta(minutes=30)).isoformat()],
        )
    for _ in range(120):
        in_memory_duckdb.execute(
            f"INSERT INTO {table} VALUES (?, 64500, 0.001, 1000, 0.0, NULL, NULL, NULL)",
            [(now - timedelta(minutes=90)).isoformat()],
        )
    probes = ["/admin", "/.env", "/.git", "/wp-login.php", "/phpmyadmin/index.php", "/config.json"]
    for path, mins in zip(probes, [110, 88, 66, 44, 22, 6], strict=True):
        in_memory_duckdb.execute(
            f"INSERT INTO {table} VALUES (?, NULL, NULL, NULL, NULL, '192.0.2.55', ?, NULL)",
            [(now - timedelta(minutes=mins)).isoformat(), path],
        )
    _insights_cache.clear()
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)
    cards = [c for c in res["insights"] if c["id"] in {"network_asn_health", "low_and_slow"}]
    assert {c["id"] for c in cards if c["items"]} == {"network_asn_health", "low_and_slow"}
    assert _assert_cards_wire_identical(cards) == 2


def test_high_scale_cards_are_the_standard_cards_and_wire_identical():
    class _Client:
        def execute(self, query, params):
            if "insight:history" in query:
                return [{"earliest": params["window_end"].replace(year=2020), "window_total": 9}]
            if "insight:fields" in query:
                return [{"fields": ["status", "elapsed", "cache"]}]
            if "insight:error_spikes" in query:
                return [{"url": "/a", "w_rate": 0.7, "b_rate": 0.0, "w_errors": 7, "w_total": 10, "b_total": 99}]
            return []

    service = type("Svc", (), {"client": _Client(), "service_id": "svc"})()
    req = type("Req", (), {"window_size_hrs": 24.0, "baseline_hours": 168.0})()
    res = hs_insights_endpoint(service, req, None, None)  # type: ignore[arg-type]
    cards = {card["id"]: card for card in json.loads(res.model_dump_json(by_alias=True))["insights"]}
    assert "high_scale_5xx" not in cards
    card = cards["error_spikes"]
    assert card["items"][0]["label"] == "/a"
    assert card["items"][0]["meta"] == {"requests": 10, "errors": 7, "filters": {"url": "/a"}}
    assert card["severity"] == "critical" and card["category"] == "origin"
