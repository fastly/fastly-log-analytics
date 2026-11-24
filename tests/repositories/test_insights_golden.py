"""Characterization and unit tests for registered InsightDefinitions, row processors, and shaping."""

from __future__ import annotations

import re

import pytest

import backend.repositories.insights.definitions as SQL
from backend.core.log_fields import INSIGHT_DEFINITIONS
from backend.core.share_db import mask_ip
from backend.repositories.insights import (
    InsightCategory,
    registry,
)
from backend.repositories.insights import (
    definitions as defs,
)
from backend.repositories.insights.shaping import (
    make_investigate_url_builder,
    shape_insight_card_from_rows,
)
from backend.repositories.security.watchdog import summarize_bot_efficiency_rows
from backend.utils import bot_sources
from backend.utils.bot_sources import load_source, resolve_bot_display_name


def test_resolve_bot_display_name_and_efficiency_without_runtime_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", tmp_path)
    with bot_sources._matcher_lock:
        bot_sources._matcher_cache.clear()
    with bot_sources._source_cache_lock:
        bot_sources._source_cache.clear()
    assert load_source("well-known-bots") == []
    assert resolve_bot_display_name("claude-searchbot") == "Anthropic Crawler Search"
    assert resolve_bot_display_name("googlebot") == "Google Crawler"
    assert resolve_bot_display_name("ccbot") == "Commoncrawl Crawler"
    rows = [
        ("site", "", 100, 500_000),
        ("ua", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", 10, 100_000),
        ("ref", "google", 2, 0),
        ("ua", "CCBot/2.0 (https://commoncrawl.org/faq/)", 5, 100_000),
    ]
    by_name = {item["bot_name"]: item for item in summarize_bot_efficiency_rows(rows)}
    assert by_name["Google Crawler"]["referrals"] == 2 and by_name["Commoncrawl Crawler"]["referrals"] == 0


def test_repeated_patterns_processors_and_metadata():
    p = defs.repeated_patterns_processor
    assert p(("1.1.1.1", 29, 30, 30.0, 0.0, 0.0, 1.0, 30, 870, 30), None, {})["severity"] == "critical"
    assert p(("1.1.1.1", 19, 20, 60.0, 0.0, 0.0, 1.0, 1, 1140, 60), None, {})["severity"] == "warning"
    info_out = p(("5.5.5.5", 40, 50, 3.0, 2.0, 1.0, 0.5, 5, 150, 3), None, {})
    assert info_out["meta"]["score"] < 70 and info_out["severity"] == "info"
    above = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.1, 1.0, 1, 2400, 60), None, {})
    below = p(("1.1.1.1", 40, 40, 60.0, 1.0, 0.2, 1.0, 1, 2400, 60), None, {})
    assert above["meta"]["score"] == 94 and above["severity"] == "critical"
    assert below["meta"]["score"] == 88 and below["severity"] == "warning"

    masked = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx" and masked["meta"]["filters"]["ip"] == "203.0.113.xxx"
    raw = p(("203.0.113.42", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, 60), None, {})
    assert raw["label"] == "203.0.113.42" and raw["meta"]["filters"]["ip"] == "203.0.113.42"
    none_mode = p(("1.1.1.1", 34, 35, 60.0, 0.0, 0.0, 1.0, 5, 2040, None), None, {})
    assert none_mode["meta"]["rps"] == round(35 / 2040, 4) and none_mode["meta"]["mode_gap_s"] is None

    assert defs.repeated_patterns_severity([]) == "clean"
    assert defs.repeated_patterns_severity([{"severity": "warning"}]) == "warning"
    assert defs.repeated_patterns_severity([{"severity": "info"}, {"severity": "critical"}]) == "critical"
    assert "?" not in SQL.REPEATED_BOT_UA_REGEX
    assert (
        SQL.REPEATED_PATTERNS.format(table_name="t", ua_col='"ua"', bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX).count("?")
        == 1
    )
    assert (
        SQL.REPEATED_PATTERNS_FP.format(
            table_name="t", fp_col="ja4", ua_col='"ua"', bot_ua_regex=SQL.REPEATED_BOT_UA_REGEX
        ).count("?")
        == 1
    )
    assert "MODE(gap) AS mode_gap" in SQL.REPEATED_PATTERNS

    for iid, title in [
        ("repeated_patterns", "Scripted Traffic Patterns"),
        ("repeated_patterns_fp", "Scripted Traffic Patterns (by TLS Fingerprint)"),
    ]:
        d = registry.get(iid)
        assert d is not None and d.title == title and d.required_fields == ["ip", "timestamp"]
        assert d.row_processor is not None and d.severity_logic is not None
        entry = next((e for e in INSIGHT_DEFINITIONS if e["id"] == iid), None)
        assert entry is not None and entry["required_fields"] == ["ip"] and entry["required_groups"] == []

    p_fp = defs.repeated_patterns_fp_processor
    crit_fp = p_fp(("aaaa", 29, 30, 60.0, 0.0, 0.0, 1.0, 10, 1740, 60), None, {})
    assert (
        crit_fp["severity"] == "critical" and crit_fp["meta"]["score"] == 100 and crit_fp["meta"]["distinct_ip"] == 10
    )
    assert p_fp(("aaaa", 19, 20, 60.0, 0.0, 0.0, 1.0, 5, 1140, 60), None, {})["severity"] == "warning"
    assert p_fp(("xyz", 29, 30, 60.0, 0.0, 0.0, 1.0, 10, 1740, 60), None, {"fp_col": "ja3"})["meta"]["filters"] == {
        "ja3": "xyz"
    }


@pytest.mark.security_regression
def test_coalesced_ip_security_masks_ip_end_to_end_for_analyst():
    raw_ips = {"10.0.0.1", "10.0.0.2", "10.0.0.3"}
    rows_by_id = {
        "low_and_slow": [("10.0.0.1", 6, 6, 1200, 0.005)],
        "credential_enumeration": [("10.0.0.2", 22, 25, 1, 0)],
        "content_discovery": [("10.0.0.3", 32, 35, 32, 0)],
    }
    url_builder = make_investigate_url_builder("svc-test", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")
    found_cards = []
    for iid, rows in rows_by_id.items():
        d = registry.get(iid)
        assert d is not None
        card = shape_insight_card_from_rows(
            d,
            rows,
            service_id="svc-test",
            window_hours=1.0,
            baseline_hours=24.0,
            fp_col="ja4",
            actual_cols=["timestamp", "ip", "url", "status"],
            mask_ips=True,
            investigate_url_builder=url_builder,
        )
        found_cards.append(card)

    assert len(found_cards) == 3
    for card in found_cards:
        assert card["items"], f"expected items for {card['id']}"
        for item in card["items"]:
            assert item["label"] not in raw_ips
            assert item["label"] in {mask_ip(ip) for ip in raw_ips}
            for raw_ip in raw_ips:
                assert raw_ip not in item.get("investigate_url", "")


_EXPECTED_INSIGHT_IDS = {
    "error_spikes",
    "botnet_grouping",
    "new_country_traffic",
    "city_surges",
    "city_error_spikes",
    "city_latency_regressions",
    "new_city_traffic",
    "ua_monoculture",
    "new_probe_urls",
    "waf_signal_spikes",
    "proxy_surge",
    "asn_concentration",
    "asn_metro_performance",
    "cache_collapse",
    "latency_regression",
    "impossible_distance",
    "tail_latency",
    "cipher_spread",
    "request_size_anomaly",
    "connection_abuse",
    "region_latency",
    "cache_ttl_mismatch",
    "image_optimization_opportunities",
    "origin_latency_spike",
    "origin_error_rate",
    "origin_retries",
    "origin_ip_failure",
    "shield_path_degradation",
    "repeated_patterns",
    "low_and_slow",
    "credential_enumeration",
    "network_asn_health",
    "content_discovery",
    "referer_monoculture",
    "method_drift",
    "new_asn_traffic",
    "asn_hosting_shift",
    "metro_delivery_degradation",
    "connection_type_mix",
    "pop_latency_regression",
    "http3_fallback",
    "cache_hit_cliff",
    "payload_compression_regression",
    "session_harvesting",
    "timeout_split",
}


def test_registry_and_catalog_contracts():
    valid = {c.value for c in InsightCategory}
    all_defs = registry.get_all()
    for d in all_defs:
        assert str(d.category) in valid, f"{d.id} has bad category {d.category!r}"
        assert len(d.required_fields) == len(set(d.required_fields)), f"{d.id} has duplicate required_fields"

    reg = {d.id: str(d.category) for d in all_defs}
    legacy = {d["id"]: d.get("category") for d in INSIGHT_DEFINITIONS}
    assert not sorted(set(reg) - set(legacy))
    assert not sorted(i for i, c in legacy.items() if not c)
    assert not {i: (reg[i], legacy[i]) for i in reg if i in legacy and reg[i] != legacy[i]}
    assert not (_EXPECTED_INSIGHT_IDS - set(reg))

    definition = registry.get("error_spikes")
    assert definition is not None and definition.title == "Error Spikes"
    assert "url" in definition.required_fields and "status" in definition.required_fields
    assert "{table_name}" in definition.sql_template and definition.row_processor is not None
    item = definition.row_processor(("/test", 0.1, 0.02, 10, 100, 500), definition, {})
    assert item["label"] == "/test" and item["current_val"] == 10.0 and item["severity"] == "warning"
    assert definition.row_processor(("/crit", 0.6, 0.02, 60, 100, 500), definition, {})["severity"] == "critical"


# ── Row processor severity boundary & contract matrix ───────────────────────


@pytest.mark.parametrize(
    ("proc", "low_row", "high_row", "ctx", "low_sev", "high_sev"),
    [
        (
            defs.error_spikes_processor,
            ("/x", 0.49, 0.02, 49, 100, 500),
            ("/x", 0.5, 0.02, 50, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.botnet_grouping_processor,
            ("ja3fp", 49, 200, 5, 9.8),
            ("ja3fp", 50, 200, 5, 10.0),
            {},
            "warning",
            "critical",
        ),
        (defs.new_country_traffic_processor, ("ZW", 9, 0), ("ZW", 10, 0), {}, "info", "warning"),
        (
            defs.city_surges_processor,
            ("x", "x", "", "X", 100, 10, 9.5),
            ("x", "x", "", "X", 100, 10, 10.0),
            {"baseline_hours": 1, "window_hours": 1},
            "info",
            "warning",
        ),
        (
            defs.city_error_spikes_processor,
            ("L", "L", "", "X", 0.49, 0.02, 49, 100, 500),
            ("L", "L", "", "X", 0.5, 0.02, 50, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.city_latency_processor,
            ("L", "L", "", "X", 4999.0, 1000.0, 100, 500),
            ("L", "L", "", "X", 5000.0, 1000.0, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.new_city_traffic_processor,
            ("L", "L", "", "X", 49, 0),
            ("L", "L", "", "X", 50, 0),
            {},
            "info",
            "warning",
        ),
        (
            defs.ua_monoculture_processor,
            ("bot/1.0", 49, 0, 100, 100),
            ("bot/1.0", 50, 0, 100, 100),
            {},
            "warning",
            "critical",
        ),
        (defs.new_probe_urls_processor, ("/.env", 4, 0, 100.0), ("/.env", 5, 0, 100.0), {}, "warning", "critical"),
        (
            defs.waf_signal_spikes_processor,
            ("S", 5, 1, 9.9),
            ("S", 5, 1, 10.0),
            {"baseline_hours": 1, "window_hours": 1},
            "warning",
            "critical",
        ),
        (defs.asn_concentration_processor, (1, 49, 5, 100, 100), (1, 50, 5, 100, 100), {}, "warning", "critical"),
        (
            defs.asn_metro_performance_processor,
            (1, "M", 99.0, 0.0, 100, 500),
            (1, "M", 100.0, 0.0, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.cache_collapse_processor,
            ("/img", 0.11, 0.65, 100, 500),
            ("/img", 0.05, 0.65, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.cacheability_regression_processor,
            ("/api", 0.60, 0.15, 200, 800),
            ("/api", 0.95, 0.02, 200, 800),
            {},
            "warning",
            "critical",
        ),
        (
            defs.latency_regression_processor,
            ("/x", 4999.0, 1000.0, 100, 500),
            ("/x", 5000.0, 1000.0, 100, 500),
            {},
            "warning",
            "critical",
        ),
        (
            defs.impossible_distance_processor,
            ("ja3", 5, 5000.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC"),
            ("ja3", 5, 5001.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC"),
            {},
            "warning",
            "critical",
        ),
        (
            defs.tail_latency_processor,
            ("/x", 5000, 500, 10.0, 100),
            ("/x", 5000, 500, 10.1, 100),
            {},
            "warning",
            "critical",
        ),
        (
            defs.cipher_spread_processor,
            ("abc", 49, 100, 5),
            ("abc", 50, 100, 5),
            {"baseline_hours": 1, "window_hours": 1},
            "warning",
            "critical",
        ),
        (
            defs.request_size_anomaly_processor,
            ("1.2.3.4", 64000, 30000, 5, 1000),
            ("1.2.3.4", 64001, 30000, 5, 1000),
            {},
            "warning",
            "critical",
        ),
        (
            defs.connection_abuse_processor,
            ("1.2.3.4", 500, 100, 50, 100),
            ("1.2.3.4", 501, 100, 50, 100),
            {},
            "warning",
            "critical",
        ),
        (
            defs.region_latency_processor,
            ("r", 4999.0, 1000.0, 100, 500, None),
            ("r", 5000.0, 1000.0, 100, 500, None),
            {},
            "warning",
            "critical",
        ),
        (
            defs.origin_latency_spike_processor,
            ("/x", 50.0, 90.0, 10.0, 100),
            ("/x", 100.0, 90.0, 10.0, 100),
            {},
            "warning",
            "critical",
        ),
        (
            defs.origin_error_rate_processor,
            (503, 5, 100, 100, 5, 1),
            (503, 10, 100, 100, 6, 1),
            {},
            "warning",
            "critical",
        ),
        (
            defs.origin_ip_failure_processor,
            ("10.0.0.1", 100, 20.0, 5.0),
            ("10.0.0.1", 100, 21.0, 5.0),
            {},
            "warning",
            "critical",
        ),
        (
            defs.shield_path_degradation_processor,
            ("LAX", "SFO", 299.0, 100.0, 10),
            ("LAX", "SFO", 300.0, 100.0, 10),
            {},
            "warning",
            "critical",
        ),
        (
            defs.low_and_slow_processor,
            ("1.2.3.4", 20, 9, 7200, 0.001),
            ("1.2.3.4", 20, 10, 7200, 0.001),
            {},
            "warning",
            "critical",
        ),
        (
            defs.credential_enumeration_processor,
            ("1.2.3.4", 99, 120, 3, 0),
            ("1.2.3.4", 100, 120, 3, 0),
            {},
            "warning",
            "critical",
        ),
        (
            defs.content_discovery_processor,
            ("1.2.3.4", 99, 120, 40, 0),
            ("1.2.3.4", 100, 120, 40, 0),
            {},
            "warning",
            "critical",
        ),
        (defs.referer_monoculture_processor, ("r", 49, 0, 100, 100), ("r", 50, 0, 100, 100), {}, "warning", "critical"),
        (defs.method_drift_processor, ("POST", 24, 0, 100, 100), ("POST", 25, 0, 100, 100), {}, "warning", "critical"),
        (defs.new_asn_traffic_processor, (7922, 99, 0), (7922, 150, 0), {}, "info", "warning"),
        (defs.asn_hosting_shift_processor, (1, 599, 1000, 0, 100), (1, 60, 100, 0, 100), {}, "warning", "critical"),
        (
            defs.metro_delivery_degradation_processor,
            (501, 4_000_000, 10_000_000, 60, 200),
            (501, 1_000_000, 10_000_000, 60, 200),
            {},
            "warning",
            "critical",
        ),
        (
            defs.pop_latency_regression_processor,
            ("JFK", 3000.0, 50.0, 60, 200),
            ("JFK", 5000.0, 50.0, 60, 200),
            {},
            "warning",
            "critical",
        ),
        (defs.http3_fallback_processor, (550, 1000, 700, 1000), (0, 1000, 800, 1000), {}, "warning", "critical"),
        (defs.cache_hit_cliff_processor, (550, 1000, 700, 1000), (10, 1000, 700, 1000), {}, "warning", "critical"),
        (
            defs.payload_compression_regression_processor,
            ("/app.js", 0.8, 0.02, 40, 200),
            ("/app.js", 0.90, 0.0, 40, 200),
            {},
            "warning",
            "critical",
        ),
        (
            defs.session_harvesting_processor,
            ("203.0.113.5", 60, 200, 3),
            ("203.0.113.5", 100, 300, 3),
            {},
            "warning",
            "critical",
        ),
        (
            defs.network_asn_health_processor,
            (1, 0.049, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120),
            (1, 0.05, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120),
            {},
            "warning",
            "critical",
        ),
    ],
)
def test_processor_severity_thresholds(proc, low_row, high_row, ctx, low_sev, high_sev):
    assert proc(low_row, None, ctx)["severity"] == low_sev
    assert proc(high_row, None, ctx)["severity"] == high_sev


def test_processor_labels_fallbacks_and_edge_cases():
    assert defs.error_spikes_processor((None, 0.1, 0.0, 5, 50, 100), None, {})["label"] == "(empty)"
    assert defs.ua_monoculture_processor((None, 10, 0, 100, 100), None, {})["label"] == "(empty)"
    assert defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja4"})["meta"]["filters"] == {
        "ja4": "fp123"
    }
    assert defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja3"})["meta"]["filters"] == {
        "ja3": "fp123"
    }
    assert defs.new_country_traffic_processor(("XX", 50, 0), None, {})["baseline_val"] == 0
    assert (
        defs.city_surges_processor(
            ("LA, US", "LA", "CA", "US", 100, 240, 4.2), None, {"baseline_hours": 24, "window_hours": 1}
        )["baseline_val"]
        == 10.0
    )
    assert (
        defs.city_error_spikes_processor(("L", "L", "", "US", 0.3, 0.1, 30, 100, 500), None, {})["meta"]["filters"][
            "region"
        ]
        is None
    )
    assert (
        defs.city_latency_processor(("L", "L", "", "X", 100.0, 0.0, 100, 500), None, {})["meta"]["regression_ratio"]
        == 0
    )
    assert defs.new_probe_urls_processor(("/x", 1, 0, None), None, {})["meta"]["error_pct"] == 0
    assert r"\.env" in SQL.NEW_PROBE_REGEX and r"\.git" in SQL.NEW_PROBE_REGEX and "(?i)" not in SQL.NEW_PROBE_REGEX
    re.compile(SQL.NEW_PROBE_REGEX)
    probe_sql = next(d.sql_template for d in defs.registry.get_all() if d.id == "new_probe_urls")
    assert "regexp_matches" in probe_sql and "'i'" in probe_sql

    assert (
        defs.waf_signal_spikes_processor(("SQLI", 50, 240, 12.0), None, {"baseline_hours": 24, "window_hours": 1})[
            "baseline_val"
        ]
        == 10.0
    )
    ps = defs.proxy_surge_processor(("VPN", 250, 50, 1000, 800), None, {})
    assert ps["current_val"] == 25.0 and ps["severity"] == "warning"
    assert defs.proxy_surge_processor(("VPN", 5, 0, 0, 0), None, {})["current_val"] == 500.0
    assert defs.proxy_surge_severity([]) == "clean" and defs.proxy_surge_severity([{"label": "VPN"}]) == "warning"

    assert (
        defs.asn_concentration_processor((16509, 50, 5, 100, 100), None, {"asn_names": {16509: "AMAZON-02"}})["label"]
        == "AS16509 (AMAZON-02)"
    )
    assert defs.asn_concentration_processor((16509, 50, 5, 100, 100), None, {})["label"] == "AS16509"
    assert (
        defs.asn_metro_performance_processor(
            (7922, "803", 150.0, 30.0, 100, 500),
            None,
            {"dma_map": {"803": "Los Angeles, CA"}, "asn_names": {7922: "COMCAST"}},
        )["label"]
        == "AS7922 (COMCAST) in Los Angeles, CA"
    )
    unk_dma = defs.asn_metro_performance_processor((7922, "999", 150.0, 30.0, 100, 500), None, {})["label"]
    assert "DMA 999" in unk_dma and "AS7922" in unk_dma

    cr = defs.cacheability_regression_processor(("/api", 0.95, 0.02, 200, 800), None, {})
    assert (cr["label"], cr["unit"], cr["current_val"], cr["baseline_val"], cr["meta"]["filters"]) == (
        "/api",
        "% PASS",
        95.0,
        2.0,
        {"url": "/api"},
    )
    assert defs.latency_regression_processor(("/x", 100.0, 0.0, 100, 500), None, {})["meta"]["regression_ratio"] == 0
    assert (
        "ja3"
        in defs.impossible_distance_processor(
            ("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"), None, {}
        )["meta"]["filters"]
    )
    assert (
        "ja4"
        in defs.impossible_distance_processor(
            ("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"), None, {"fp_col": "ja4"}
        )["meta"]["filters"]
    )

    cs_ctx = {"baseline_hours": 1, "window_hours": 1}
    assert (
        defs.cipher_spread_processor(("abcdef1234567890abcdef", 10, 100, 5), None, cs_ctx)["label"] == "abcdef123456…"
    )
    assert defs.cipher_spread_processor(("shorthash", 10, 100, 5), None, cs_ctx)["label"] == "shorthash"
    assert defs.cipher_spread_processor(("", 10, 100, 5), None, cs_ctx)["label"] == "(unknown)"
    rsa_none = defs.request_size_anomaly_processor((None, None, None, 3, None), None, {})
    assert rsa_none["current_val"] == 0 and rsa_none["baseline_val"] == 0

    assert (
        defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, 30.0), None, {})["meta"]["ottfb_p95_ms"]
        == 30.0
    )
    assert (
        "ottfb_p95_ms"
        not in defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, None), None, {})["meta"]
    )
    ctm = defs.cache_ttl_mismatch_processor((123, 3600, 1.5, 60, 100), None, {})
    assert ctm["label"] == "123" and ctm["severity"] == "warning"

    assert (
        defs.image_optimization_processor(("/x.jpg", 100, 10 * 1024 * 1024 + 1, 500.0, 0.3), None, {})["severity"]
        == "critical"
    )
    assert (
        defs.image_optimization_processor(("/x.jpg", 5, 1024 * 1024 * 5, 1025.0, 0.3), None, {})["severity"]
        == "critical"
    )
    assert (
        defs.image_optimization_processor(("/x.jpg", 50, 3 * 1024 * 1024, 60.0, 0.6), None, {})["severity"] == "warning"
    )
    assert defs.image_optimization_processor(("/x.jpg", 5, 600 * 1024, 100.0, 0.1), None, {})["severity"] == "info"
    assert defs.image_optimization_processor(("/x", 1, 600 * 1024, 100.0, None), None, {})["meta"]["mobile_ratio"] == 0

    assert defs.origin_error_rate_processor((503, 10, 100, 100, 6, 1), None, {})["label"] == "HTTP 503"
    assert defs.origin_retries_processor(("/x", 100, 2.5, 5), None, {})["severity"] == "warning"
    assert defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 0.0, 10), None, {})["meta"]["ratio"] == 0
    assert defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 50.0, 10), None, {})["label"] == "LAX → SFO"


def test_phase3_and_phase4_processors_happy_paths():
    ls = defs.low_and_slow_processor(("198.51.100.7", 42, 6, 7200, 0.0058), None, {})
    assert (ls["label"], ls["current_val"], ls["baseline_val"], ls["unit"]) == (
        "198.51.100.7",
        6,
        None,
        "sensitive paths",
    )
    assert ls["meta"] == {
        "hits": 42,
        "distinct_paths": 6,
        "span_s": 7200,
        "rps": 0.0058,
        "filters": {"ip": "198.51.100.7"},
    }
    ls_none = defs.low_and_slow_processor(("1.2.3.4", 5, 3, None, None), None, {})
    assert ls_none["meta"]["span_s"] == 0 and ls_none["meta"]["rps"] == 0.0

    ce = defs.credential_enumeration_processor(("203.0.113.9", 80, 100, 4, 2), None, {})
    assert (ce["label"], ce["current_val"], ce["baseline_val"], ce["unit"]) == (
        "203.0.113.9",
        80,
        2,
        "denied (401/403)",
    )
    assert ce["meta"] == {
        "attempts": 100,
        "denied": 80,
        "fail_rate_pct": 80.0,
        "distinct_paths": 4,
        "filters": {"ip": "203.0.113.9"},
    }
    assert defs.credential_enumeration_processor(("1.2.3.4", 0, 0, 0, 0), None, {})["meta"]["fail_rate_pct"] == 0.0

    cd = defs.content_discovery_processor(("198.51.100.7", 60, 75, 40, 3), None, {})
    assert (cd["label"], cd["current_val"], cd["baseline_val"], cd["unit"]) == ("198.51.100.7", 60, 3, "404s")
    assert cd["meta"] == {
        "requests": 75,
        "not_found": 60,
        "not_found_rate_pct": 80.0,
        "distinct_404_urls": 40,
        "filters": {"ip": "198.51.100.7"},
    }
    assert defs.content_discovery_processor(("1.2.3.4", 0, 0, 0, 0), None, {})["meta"]["not_found_rate_pct"] == 0.0

    rm = defs.referer_monoculture_processor(("http://spam.example", 40, 5, 500, 100), None, {})
    assert (rm["label"], rm["current_val"], rm["baseline_val"], rm["unit"]) == (
        "http://spam.example",
        40.0,
        1.0,
        "% of traffic",
    )
    assert rm["meta"]["requests"] == 40 and rm["meta"]["filters"] == {"referer": "http://spam.example"}
    rm_empty = defs.referer_monoculture_processor((None, 0, 0, 0, 0), None, {})
    assert rm_empty["label"] == "(empty)" and rm_empty["current_val"] == 0.0

    md = defs.method_drift_processor(("POST", 30, 2, 100, 200), None, {})
    assert (md["label"], md["current_val"], md["baseline_val"], md["unit"], md["meta"]["filters"]) == (
        "POST",
        30.0,
        1.0,
        "% of traffic",
        {"method": "POST"},
    )

    na = defs.new_asn_traffic_processor((16509, 60, 0), None, {})
    assert (
        na["label"],
        na["current_val"],
        na["baseline_val"],
        na["unit"],
        na["meta"]["asn"],
        na["meta"]["filters"],
    ) == ("AS16509", 60, 0, "requests", 16509, {"asn": 16509})
    assert (
        defs.new_asn_traffic_processor((7922, 150, 0), None, {"asn_names": {7922: "Example ISP"}})["label"]
        == "AS7922 (Example ISP)"
    )

    ahs = defs.asn_hosting_shift_processor((16509, 45, 100, 5, 200), None, {})
    assert (ahs["label"], ahs["current_val"], ahs["baseline_val"], ahs["unit"]) == ("AS16509", 45.0, 2.5, "% hosting")
    assert (ahs["meta"]["asn"], ahs["meta"]["filters"], ahs["meta"]["window_hosting"], ahs["meta"]["window_total"]) == (
        16509,
        {"asn": 16509},
        45,
        100,
    )
    assert (
        defs.asn_hosting_shift_processor((7922, 30, 60, 2, 100), None, {"asn_names": {7922: "Comcast"}})["label"]
        == "AS7922 (Comcast)"
    )
    ahs_zero = defs.asn_hosting_shift_processor((1, 0, 0, 0, 0), None, {})
    assert ahs_zero["current_val"] == 0.0 and ahs_zero["baseline_val"] == 0.0

    mdd = defs.metro_delivery_degradation_processor((501, 1_000_000, 10_000_000, 60, 200), None, {})
    assert (mdd["label"], mdd["current_val"], mdd["baseline_val"], mdd["unit"], mdd["meta"]["filters"]) == (
        "DMA 501",
        8.0,
        80.0,
        "Mbps (median)",
        {"metro": 501},
    )
    assert (
        defs.metro_delivery_degradation_processor(
            (501, 1_000_000, 10_000_000, 60, 200), None, {"dma_map": {"501": "New York City"}}
        )["label"]
        == "New York City"
    )

    ct_mix = defs.connection_type_mix_processor(("cellular", "mobile", 60, 5, 100, 500), None, {})
    assert (ct_mix["label"], ct_mix["current_val"], ct_mix["baseline_val"], ct_mix["meta"]["filters"]) == (
        "cellular / mobile",
        60.0,
        1.0,
        {"c_type": "cellular", "c_speed": "mobile"},
    )

    plr = defs.pop_latency_regression_processor(("JFK", 3000.0, 50.0, 60, 200), None, {})
    assert (plr["label"], plr["current_val"], plr["baseline_val"], plr["unit"], plr["meta"]["filters"]) == (
        "JFK",
        3000.0,
        50.0,
        "ms (P95)",
        {"pop": "JFK"},
    )

    h3 = defs.http3_fallback_processor((0, 1000, 800, 1000), None, {})
    assert (h3["current_val"], h3["baseline_val"], h3["unit"]) == (0.0, 80.0, "% QUIC")

    chc = defs.cache_hit_cliff_processor((10, 1000, 700, 1000), None, {})
    assert (chc["current_val"], chc["baseline_val"], chc["unit"]) == (1.0, 70.0, "% HIT")

    pcr = defs.payload_compression_regression_processor(("/app.js", 0.8, 0.02, 40, 200), None, {})
    assert (pcr["label"], pcr["current_val"], pcr["baseline_val"], pcr["unit"], pcr["meta"]["filters"]) == (
        "/app.js",
        80.0,
        2.0,
        "% uncompressed",
        {"url": "/app.js"},
    )

    sh = defs.session_harvesting_processor(("203.0.113.5", 60, 200, 3), None, {})
    assert (sh["label"], sh["current_val"], sh["baseline_val"], sh["unit"], sh["meta"]["filters"]) == (
        "203.0.113.5",
        60,
        3,
        "distinct sessions",
        {"ip": "203.0.113.5"},
    )
    assert "cookie_session" not in repr(sh) and "sess" not in sh["label"] and isinstance(sh["current_val"], int)

    ts_conn = defs.timeout_split_processor((500.0, 5.0, 60.0, 55.0, 100, 300), None, {})
    assert (
        ts_conn["label"],
        ts_conn["current_val"],
        ts_conn["baseline_val"],
        ts_conn["meta"]["phase"],
        ts_conn["severity"],
    ) == ("Origin connect P95", 500.0, 5.0, "connect", "critical")
    ts_read = defs.timeout_split_processor((10.0, 8.0, 900.0, 100.0, 100, 300), None, {})
    assert (ts_read["label"], ts_read["meta"]["phase"], ts_read["current_val"]) == ("Origin read P95", "read", 900.0)

    nah = defs.network_asn_health_processor((16509, 0.08, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120), None, {})
    assert (nah["label"], nah["current_val"], nah["baseline_val"], nah["unit"]) == (
        "AS16509",
        8.0,
        0.1,
        "% packet loss",
    )
    assert (
        nah["meta"]["window_packet_loss_pct"],
        nah["meta"]["requests"],
        nah["meta"]["asn"],
        nah["meta"]["filters"],
    ) == (8.0, 60, 16509, {"asn": 16509})
    assert (
        defs.network_asn_health_processor(
            (7922, 0.06, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120), None, {"asn_names": {7922: "Example ISP"}}
        )["label"]
        == "AS7922 (Example ISP)"
    )


@pytest.mark.parametrize(
    ("proc", "row"),
    [
        (defs.request_size_anomaly_processor, ("203.0.113.42", 70000, 100, 50, 1000)),
        (defs.connection_abuse_processor, ("203.0.113.42", 800, 10, 50, 100)),
        (defs.session_harvesting_processor, ("203.0.113.42", 60, 200, 3)),
        (defs.low_and_slow_processor, ("203.0.113.42", 42, 6, 7200, 0.0058)),
        (defs.credential_enumeration_processor, ("203.0.113.42", 80, 100, 4, 2)),
        (defs.content_discovery_processor, ("203.0.113.42", 60, 75, 40, 3)),
    ],
)
def test_ip_keyed_processors_admin_and_masked_contract(proc, row):
    raw = proc(row, None, {})
    assert raw["label"] == "203.0.113.42" and raw["meta"]["filters"]["ip"] == "203.0.113.42"
    masked = proc(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_session_harvesting_masks_ip_when_mask_ips():
    masked = defs.session_harvesting_processor(("203.0.113.42", 60, 200, 3), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_low_and_slow_masks_ip_when_mask_ips():
    masked = defs.low_and_slow_processor(("203.0.113.42", 42, 6, 7200, 0.0058), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_credential_enumeration_masks_ip_when_mask_ips():
    masked = defs.credential_enumeration_processor(("203.0.113.42", 80, 100, 4, 2), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected


@pytest.mark.security_regression
def test_content_discovery_masks_ip_when_mask_ips():
    masked = defs.content_discovery_processor(("203.0.113.42", 60, 75, 40, 3), None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected and masked["meta"]["filters"]["ip"] == expected
