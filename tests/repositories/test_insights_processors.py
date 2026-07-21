"""Tests for ``backend.repositories.insights.definitions`` row processors.

This file is mostly a wall of registered ``InsightDefinition`` instances
— each with a ``row_processor`` that converts a DB row tuple into the
shape the frontend Insights panel renders.

The SQL templates need DuckDB integration to test meaningfully (see
``tests/repositories/test_insights.py``). The processors, by contrast,
are pure functions: tuple in, dict out. Their job is small but
mistake-prone — they decide severity thresholds (which colour the
insight card), label formatting (which the user reads), and meta-shape
(which downstream filter pills key on).

A regression in any processor would silently mis-render insights with
wrong severity colours or unfilterable result links, so each one gets
both a happy-path assertion and at least one boundary test for its
severity threshold.
"""

from __future__ import annotations

import pytest

from backend.core.share_db.validation import mask_ip
from backend.repositories.insights import definitions as defs
from backend.repositories.insights.registry import registry

# ── Registry contract: every documented insight is registered ───────────────


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


def test_every_expected_insight_is_registered():
    """The frontend keys on these exact IDs; dropping or renaming one
    here would silently make the matching card disappear from the UI."""
    registered = {d.id for d in registry.get_all()}
    missing = _EXPECTED_INSIGHT_IDS - registered
    assert not missing, f"missing from registry: {missing}"


def test_no_duplicate_required_fields_in_any_definition():
    """Trivial sanity — a typo'd required_fields list could cause the
    field-existence check to filter the insight out for valid services."""
    for d in registry.get_all():
        assert len(d.required_fields) == len(set(d.required_fields)), (
            f"{d.id} has duplicate required_fields: {d.required_fields}"
        )


# ── error_spikes ────────────────────────────────────────────────────────────


def test_error_spikes_critical_threshold_at_50_percent():
    """At ``w_rate=0.5`` the processor flips warning → critical."""
    p = defs.error_spikes_processor
    just_under = p(("/x", 0.49, 0.02, 49, 100, 500), None, {})
    at_boundary = p(("/x", 0.5, 0.02, 50, 100, 500), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_error_spikes_empty_label_falls_back_to_placeholder():
    """An empty/None URL → ``(empty)`` so the card still renders.
    Pinned because a None label would crash the frontend list."""
    out = defs.error_spikes_processor((None, 0.1, 0.0, 5, 50, 100), None, {})
    assert out["label"] == "(empty)"


# ── botnet_grouping ─────────────────────────────────────────────────────────


def test_botnet_grouping_severity_threshold_at_50_ips():
    p = defs.botnet_grouping_processor
    just_under = p(("ja3fp", 49, 200, 5, 9.8), None, {})
    at_boundary = p(("ja3fp", 50, 200, 5, 10.0), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_botnet_grouping_filters_use_actual_fp_col():
    """The filter must use the column that was actually queried (from
    context['fp_col']), not both ja3 and ja4 — setting both creates an
    AND filter on the dashboard that matches nothing."""
    out = defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja4"})
    assert out["meta"]["filters"] == {"ja4": "fp123"}

    out = defs.botnet_grouping_processor(("fp123", 10, 100, 5, 2.0), None, {"fp_col": "ja3"})
    assert out["meta"]["filters"] == {"ja3": "fp123"}


# ── new_country_traffic ─────────────────────────────────────────────────────


def test_new_country_traffic_severity_threshold_at_10():
    p = defs.new_country_traffic_processor
    assert p(("ZW", 9, 0), None, {})["severity"] == "info"
    assert p(("ZW", 10, 0), None, {})["severity"] == "warning"


def test_new_country_traffic_baseline_is_always_zero():
    """By definition this insight only fires when baseline_val=0; that
    invariant is encoded both in the SQL HAVING clause AND the processor."""
    out = defs.new_country_traffic_processor(("XX", 50, 0), None, {})
    assert out["baseline_val"] == 0


# ── city_surges ─────────────────────────────────────────────────────────────


def test_city_surges_normalises_baseline_to_window_size():
    """``baseline_val`` is rescaled from baseline_hours → window_hours
    so the card shows comparable numbers (not "this hour vs the last
    week's total")."""
    context = {"baseline_hours": 24, "window_hours": 1}
    out = defs.city_surges_processor(("LA, US", "LA", "CA", "US", 100, 240, 4.2), None, context)
    assert out["baseline_val"] == 10.0  # 240 / 24 * 1


def test_city_surges_severity_threshold_at_ratio_10():
    p = defs.city_surges_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    assert p(("x", "x", "", "X", 100, 10, 9.5), None, ctx)["severity"] == "info"
    assert p(("x", "x", "", "X", 100, 10, 10.0), None, ctx)["severity"] == "warning"


# ── city_error_spikes ──────────────────────────────────────────────────────


def test_city_error_spikes_critical_above_50_percent_error_rate():
    p = defs.city_error_spikes_processor
    just_under = p(("L", "L", "", "X", 0.49, 0.02, 49, 100, 500), None, {})
    at_boundary = p(("L", "L", "", "X", 0.5, 0.02, 50, 100, 500), None, {})
    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_city_error_spikes_region_filter_null_when_empty():
    """Empty region string must surface as ``None`` in the filter dict
    — the frontend's filter pill renderer drops None keys, but would
    include an empty-string filter literally (producing zero results)."""
    out = defs.city_error_spikes_processor(("L", "L", "", "US", 0.3, 0.1, 30, 100, 500), None, {})
    assert out["meta"]["filters"]["region"] is None


# ── city_latency_regressions ───────────────────────────────────────────────


def test_city_latency_critical_at_5000ms():
    p = defs.city_latency_processor
    assert p(("L", "L", "", "X", 4999.0, 1000.0, 100, 500), None, {})["severity"] == "warning"
    assert p(("L", "L", "", "X", 5000.0, 1000.0, 100, 500), None, {})["severity"] == "critical"


def test_city_latency_regression_ratio_zero_when_baseline_zero():
    """``b_p95=0`` would divide-by-zero; the processor guards with
    ``if row[5] else 0``."""
    out = defs.city_latency_processor(("L", "L", "", "X", 100.0, 0.0, 100, 500), None, {})
    assert out["meta"]["regression_ratio"] == 0


# ── new_city_traffic ────────────────────────────────────────────────────────


def test_new_city_severity_threshold_at_50():
    p = defs.new_city_traffic_processor
    assert p(("L", "L", "", "X", 49, 0), None, {})["severity"] == "info"
    assert p(("L", "L", "", "X", 50, 0), None, {})["severity"] == "warning"


# ── ua_monoculture ──────────────────────────────────────────────────────────


def test_ua_monoculture_critical_at_50_percent():
    p = defs.ua_monoculture_processor
    just_under = p(("bot/1.0", 49, 0, 100, 100), None, {})
    at_boundary = p(("bot/1.0", 50, 0, 100, 100), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_ua_monoculture_empty_label_falls_back():
    out = defs.ua_monoculture_processor((None, 10, 0, 100, 100), None, {})
    assert out["label"] == "(empty)"


# ── new_probe_urls ──────────────────────────────────────────────────────────


def test_new_probe_urls_critical_at_5_hits():
    p = defs.new_probe_urls_processor
    just_under = p(("/.env", 4, 0, 100.0), None, {})
    at_boundary = p(("/.env", 5, 0, 100.0), None, {})

    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


def test_new_probe_urls_error_pct_defaults_to_zero_when_none():
    """SQL may return None for error_pct on a single-row group; the
    processor must coerce to 0 so the meta dict is JSON-serialisable."""
    out = defs.new_probe_urls_processor(("/x", 1, 0, None), None, {})
    assert out["meta"]["error_pct"] == 0


def test_new_probes_list_is_regex_escaped():
    """Every probe in NEW_PROBES is passed through ``re.escape`` before
    joining — pinned because a probe like ``.env`` contains regex
    metacharacters that would otherwise match almost any URL."""
    import re

    # ``.env`` → ``\.env`` in the compiled regex
    assert r"\.env" in defs.NEW_PROBE_REGEX
    assert r"\.git" in defs.NEW_PROBE_REGEX
    # Sanity: the compiled regex compiles
    re.compile(defs.NEW_PROBE_REGEX)


def test_new_probes_regex_is_case_insensitive_via_duckdb_flag():
    """Case-insensitivity is supplied by the ``'i'`` flag arg passed to
    ``regexp_matches`` in the SQL template — NOT by an inline ``(?i)``
    prefix in NEW_PROBE_REGEX itself.

    The inline ``(?i)`` was removed because its literal ``?`` was
    miscounted by the repository's ``sql.count("?")`` placeholder
    heuristic, producing one excess bound parameter and a
    ``Parameter argument/count mismatch`` error at execution. Pinned
    here so future refactors don't regress to ``(?i)``."""
    # Regex itself must NOT contain inline flags (would break param counting).
    assert "(?i)" not in defs.NEW_PROBE_REGEX
    # SQL template must pass the 'i' flag as the third arg to regexp_matches.
    sql = next(d.sql_template for d in defs.registry.get_all() if d.id == "new_probe_urls")
    assert "regexp_matches" in sql
    assert "'i'" in sql, "Case-insensitive flag missing from regexp_matches call"


# ── waf_signal_spikes ──────────────────────────────────────────────────────


def test_waf_signal_spikes_normalises_baseline_to_window():
    ctx = {"baseline_hours": 24, "window_hours": 1}
    out = defs.waf_signal_spikes_processor(("SQLI", 50, 240, 12.0), None, ctx)
    assert out["baseline_val"] == 10.0


def test_waf_signal_spikes_critical_at_ratio_10():
    p = defs.waf_signal_spikes_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    assert p(("S", 5, 1, 9.9), None, ctx)["severity"] == "warning"
    assert p(("S", 5, 1, 10.0), None, ctx)["severity"] == "critical"


# ── proxy_surge ─────────────────────────────────────────────────────────────


def test_proxy_surge_processor_computes_percent_of_total():
    """Severity is fixed to warning for the row processor; the
    *insight-level* severity is what ``proxy_surge_severity`` returns."""
    out = defs.proxy_surge_processor(("VPN", 250, 50, 1000, 800), None, {})
    assert out["current_val"] == 25.0
    assert out["severity"] == "warning"


def test_proxy_surge_processor_handles_zero_total_safely():
    """``max(w_total_all, 1)`` guards against division by zero."""
    out = defs.proxy_surge_processor(("VPN", 5, 0, 0, 0), None, {})
    assert out["current_val"] == 500.0  # 5 * 100 / max(0, 1) = 500


def test_proxy_surge_severity_clean_when_no_items():
    """Empty result → 'clean' (rendered as a green badge by the UI).
    Non-empty → 'warning' (the items themselves are above threshold)."""
    assert defs.proxy_surge_severity([]) == "clean"
    assert defs.proxy_surge_severity([{"label": "VPN"}]) == "warning"


# ── asn_concentration ───────────────────────────────────────────────────────


def test_asn_concentration_attaches_name_when_available():
    """``context['asn_names']`` is an optional lookup the repo may
    populate from the asn_names cache. When present, the label gets
    ``(name)`` appended — without it, just ``AS{n}``."""
    p = defs.asn_concentration_processor
    out_with_name = p(
        (16509, 50, 5, 100, 100),
        None,
        {"asn_names": {16509: "AMAZON-02"}},
    )
    out_without = p((16509, 50, 5, 100, 100), None, {})

    assert out_with_name["label"] == "AS16509 (AMAZON-02)"
    assert out_without["label"] == "AS16509"


def test_asn_concentration_critical_above_50_percent():
    p = defs.asn_concentration_processor
    assert p((1, 49, 5, 100, 100), None, {})["severity"] == "warning"
    assert p((1, 50, 5, 100, 100), None, {})["severity"] == "critical"


# ── asn_metro_performance ──────────────────────────────────────────────────


def test_asn_metro_performance_label_combines_asn_and_metro():
    """The label uses ``dma_map`` to translate DMA codes to names; when
    a DMA isn't in the map it falls back to ``DMA {code}`` so the card
    is still readable."""
    ctx = {"dma_map": {"803": "Los Angeles, CA"}, "asn_names": {7922: "COMCAST"}}
    out = defs.asn_metro_performance_processor((7922, "803", 150.0, 30.0, 100, 500), None, ctx)
    assert out["label"] == "AS7922 (COMCAST) in Los Angeles, CA"


def test_asn_metro_performance_unknown_dma_falls_back_to_code():
    out = defs.asn_metro_performance_processor((7922, "999", 150.0, 30.0, 100, 500), None, {})
    assert "DMA 999" in out["label"]
    assert "AS7922" in out["label"]


def test_asn_metro_performance_severity_at_baseline_plus_100():
    """Critical when ``w_med >= b_med + 100``."""
    p = defs.asn_metro_performance_processor
    just_under = p((1, "M", 99.0, 0.0, 100, 500), None, {})
    at_boundary = p((1, "M", 100.0, 0.0, 100, 500), None, {})
    assert just_under["severity"] == "warning"
    assert at_boundary["severity"] == "critical"


# ── cache_collapse ──────────────────────────────────────────────────────────


def test_cache_collapse_critical_when_dropped_from_above_60_to_below_10():
    p = defs.cache_collapse_processor
    # Critical: w<=0.10 AND b>=0.60
    crit = p(("/img", 0.05, 0.65, 100, 500), None, {})
    # Warning: above threshold OR below baseline
    warn = p(("/img", 0.11, 0.65, 100, 500), None, {})

    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


# ── cacheability_regression ────────────────────────────────────────────────


def test_cacheability_regression_processor_shape_and_severity():
    p = defs.cacheability_regression_processor
    # row schema: [url, w_pass_rate, b_pass_rate, w_total, b_total]
    out = p(("/api", 0.95, 0.02, 200, 800), None, {})
    assert out["label"] == "/api"
    assert out["unit"] == "% PASS"
    assert out["current_val"] == 95.0
    assert out["baseline_val"] == 2.0
    assert out["meta"]["filters"] == {"url": "/api"}
    # Critical: w>=0.80 AND b<=0.10
    assert out["severity"] == "critical"
    # Warning: surge present but not extreme enough for critical.
    assert p(("/api", 0.60, 0.15, 200, 800), None, {})["severity"] == "warning"


# ── latency_regression ─────────────────────────────────────────────────────


def test_latency_regression_critical_at_5000ms():
    p = defs.latency_regression_processor
    assert p(("/x", 4999.0, 1000.0, 100, 500), None, {})["severity"] == "warning"
    assert p(("/x", 5000.0, 1000.0, 100, 500), None, {})["severity"] == "critical"


def test_latency_regression_ratio_zero_when_baseline_zero():
    out = defs.latency_regression_processor(("/x", 100.0, 0.0, 100, 500), None, {})
    assert out["meta"]["regression_ratio"] == 0


# ── impossible_distance ────────────────────────────────────────────────────


def test_impossible_distance_critical_when_excess_over_5000km():
    p = defs.impossible_distance_processor
    row = ("ja3", 5, 5001.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC")
    crit = p(row, None, {})
    warn = p(
        ("ja3", 5, 5000.0, 9000.0, 2000.0, "SJC", "1.2.3.4", 40.7, -74.0, 37.7, -122.4, 20000, "US", "NYC"), None, {}
    )
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


def test_impossible_distance_fp_col_filter_uses_context_default():
    """``fp_col`` defaults to 'ja3' when context doesn't specify — used
    by the click-through to apply the right column filter."""
    p = defs.impossible_distance_processor
    out_default = p(("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"), None, {})
    out_ja4 = p(
        ("fpval", 1, 1.0, 1.0, 1.0, "SJC", "ip", 1, 1, 1, 1, 1, "US", "NYC"),
        None,
        {"fp_col": "ja4"},
    )
    assert "ja3" in out_default["meta"]["filters"]
    assert "ja4" in out_ja4["meta"]["filters"]


# ── tail_latency ────────────────────────────────────────────────────────────


def test_tail_latency_critical_above_ratio_10():
    p = defs.tail_latency_processor
    assert p(("/x", 5000, 500, 10.0, 100), None, {})["severity"] == "warning"
    assert p(("/x", 5000, 500, 10.1, 100), None, {})["severity"] == "critical"


# ── cipher_spread ──────────────────────────────────────────────────────────


def test_cipher_spread_label_truncates_long_sha():
    """SHAs are long; the label is truncated to 12 chars + ellipsis so
    the card title doesn't wrap."""
    p = defs.cipher_spread_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    long_sha = "abcdef1234567890abcdef"
    short_sha = "shorthash"

    out_long = p((long_sha, 10, 100, 5), None, ctx)
    out_short = p((short_sha, 10, 100, 5), None, ctx)

    assert out_long["label"] == "abcdef123456…"
    assert out_short["label"] == "shorthash"  # short stays unchanged


def test_cipher_spread_unknown_label_when_sha_empty():
    p = defs.cipher_spread_processor
    out = p(("", 10, 100, 5), None, {"baseline_hours": 1, "window_hours": 1})
    # Empty string → fallback to "(unknown)"
    assert out["label"] == "(unknown)"


def test_cipher_spread_severity_critical_at_50_ips():
    p = defs.cipher_spread_processor
    ctx = {"baseline_hours": 1, "window_hours": 1}
    assert p(("abc", 49, 100, 5), None, ctx)["severity"] == "warning"
    assert p(("abc", 50, 100, 5), None, ctx)["severity"] == "critical"


# ── request_size_anomaly ───────────────────────────────────────────────────


def test_request_size_anomaly_critical_above_64kb():
    p = defs.request_size_anomaly_processor
    assert p(("1.2.3.4", 64000, 30000, 5, 1000), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 64001, 30000, 5, 1000), None, {})["severity"] == "critical"


def test_request_size_anomaly_handles_none_bytes():
    """``int(None or 0) == 0`` — no AttributeError."""
    out = defs.request_size_anomaly_processor((None, None, None, 3, None), None, {})
    assert out["current_val"] == 0
    assert out["baseline_val"] == 0


# ── connection_abuse ───────────────────────────────────────────────────────


def test_connection_abuse_critical_above_500_reqs_per_conn():
    p = defs.connection_abuse_processor
    assert p(("1.2.3.4", 500, 100, 50, 100), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 501, 100, 50, 100), None, {})["severity"] == "critical"


# ── region_latency ─────────────────────────────────────────────────────────


def test_region_latency_includes_ottfb_p95_when_provided():
    """The ottfb_p95 column is OPTIONAL (LEFT JOIN in SQL); when None
    the meta dict must NOT have an ``ottfb_p95_ms`` key. Pinned because
    the frontend distinguishes "no data" (don't render the badge) from
    "0 ms" (render but in red — that's literally instant)."""
    with_ottfb = defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, 30.0), None, {})
    without_ottfb = defs.region_latency_processor(("us-east-1", 200.0, 50.0, 100, 500, None), None, {})

    assert with_ottfb["meta"]["ottfb_p95_ms"] == 30.0
    assert "ottfb_p95_ms" not in without_ottfb["meta"]


def test_region_latency_critical_at_5000ms():
    p = defs.region_latency_processor
    assert p(("r", 4999.0, 1000.0, 100, 500, None), None, {})["severity"] == "warning"
    assert p(("r", 5000.0, 1000.0, 100, 500, None), None, {})["severity"] == "critical"


# ── cache_ttl_mismatch ─────────────────────────────────────────────────────


def test_cache_ttl_mismatch_label_coerces_to_str():
    """``q_col`` may be any DuckDB-castable type; the processor wraps
    in ``str()`` so a numeric label doesn't crash the JSON serialiser."""
    out = defs.cache_ttl_mismatch_processor((123, 3600, 1.5, 60, 100), None, {})
    assert out["label"] == "123"  # not int(123)


def test_cache_ttl_mismatch_severity_is_always_warning():
    """This insight doesn't have a critical tier — pinned so a future
    refactor that adds one doesn't silently change the threshold for
    existing dashboards."""
    out = defs.cache_ttl_mismatch_processor(("/x", 3600, 0.5, 60, 100), None, {})
    assert out["severity"] == "warning"


# ── image_optimization ─────────────────────────────────────────────────────


def test_image_optimization_critical_when_total_over_10MB():
    p = defs.image_optimization_processor
    crit_total = p(("/x.jpg", 100, 10 * 1024 * 1024 + 1, 500.0, 0.3), None, {})
    assert crit_total["severity"] == "critical"


def test_image_optimization_critical_when_avg_over_1MB():
    p = defs.image_optimization_processor
    crit_avg = p(("/x.jpg", 5, 1024 * 1024 * 5, 1025.0, 0.3), None, {})
    assert crit_avg["severity"] == "critical"


def test_image_optimization_warning_when_mobile_ratio_above_50_percent():
    """Mobile-heavy traffic should escalate to warning even if total
    size is moderate — mobile users feel image bloat more acutely."""
    p = defs.image_optimization_processor
    out = p(("/x.jpg", 50, 3 * 1024 * 1024, 60.0, 0.6), None, {})
    assert out["severity"] == "warning"


def test_image_optimization_info_severity_for_small_total():
    p = defs.image_optimization_processor
    out = p(("/x.jpg", 5, 600 * 1024, 100.0, 0.1), None, {})
    assert out["severity"] == "info"


def test_image_optimization_mobile_ratio_defaults_to_zero_when_none():
    out = defs.image_optimization_processor(("/x", 1, 600 * 1024, 100.0, None), None, {})
    assert out["meta"]["mobile_ratio"] == 0


# ── origin_latency_spike ───────────────────────────────────────────────────


def test_origin_latency_spike_critical_when_5x_baseline():
    p = defs.origin_latency_spike_processor
    crit = p(("/x", 100.0, 90.0, 10.0, 100), None, {})  # 100 > 10*5
    warn = p(("/x", 50.0, 90.0, 10.0, 100), None, {})  # 50 == 10*5 → not >
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


# ── origin_error_rate ──────────────────────────────────────────────────────


def test_origin_error_rate_critical_above_5_percent():
    p = defs.origin_error_rate_processor
    # row: status, w_cnt, w_total, b_total, w_5xx, b_5xx
    crit = p((503, 10, 100, 100, 6, 1), None, {})  # 6%
    warn = p((503, 5, 100, 100, 5, 1), None, {})  # 5% → not > 5
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


def test_origin_error_rate_label_includes_http_status():
    out = defs.origin_error_rate_processor((503, 10, 100, 100, 6, 1), None, {})
    assert out["label"] == "HTTP 503"


# ── origin_retries ─────────────────────────────────────────────────────────


def test_origin_retries_severity_always_warning():
    """Retries are noteworthy but not critical — pinned so adding a
    critical tier requires updating this test (and presumably the UI)."""
    out = defs.origin_retries_processor(("/x", 100, 2.5, 5), None, {})
    assert out["severity"] == "warning"


# ── origin_ip_failure ──────────────────────────────────────────────────────


def test_origin_ip_failure_critical_above_20_percent_errors():
    p = defs.origin_ip_failure_processor
    assert p(("10.0.0.1", 100, 20.0, 5.0), None, {})["severity"] == "warning"
    assert p(("10.0.0.1", 100, 21.0, 5.0), None, {})["severity"] == "critical"


# ── shield_path_degradation ────────────────────────────────────────────────


def test_shield_path_degradation_critical_at_3x_ratio():
    p = defs.shield_path_degradation_processor
    crit = p(("LAX", "SFO", 300.0, 100.0, 10), None, {})  # ratio=3.0
    warn = p(("LAX", "SFO", 299.0, 100.0, 10), None, {})
    assert crit["severity"] == "critical"
    assert warn["severity"] == "warning"


def test_shield_path_degradation_handles_zero_baseline():
    """``b_p50=0`` would div-by-zero; guard with truthiness check."""
    out = defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 0.0, 10), None, {})
    assert out["meta"]["ratio"] == 0


def test_shield_path_degradation_label_uses_arrow_format():
    out = defs.shield_path_degradation_processor(("LAX", "SFO", 100.0, 50.0, 10), None, {})
    assert out["label"] == "LAX → SFO"


# ── M3: IP-keyed processors mask the client IP when the analyst sets mask_ips ──


def test_request_size_anomaly_masks_ip_when_mask_ips():
    # row schema: [ip, max_bytes, avg_bytes, w_total, b_p95]
    row = ("203.0.113.42", 70000, 100, 50, 1000)
    masked = defs.request_size_anomaly_processor(row, None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx"
    assert masked["meta"]["filters"]["ip"] == "203.0.113.xxx"  # also feeds investigate_url


def test_request_size_anomaly_keeps_ip_for_admin():
    row = ("203.0.113.42", 70000, 100, 50, 1000)
    raw = defs.request_size_anomaly_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


def test_connection_abuse_masks_ip_when_mask_ips():
    # row schema: [ip, max_reqs, avg_reqs, w_total, b_p95]
    row = ("203.0.113.42", 800, 10, 50, 100)
    masked = defs.connection_abuse_processor(row, None, {"mask_ips": True})
    assert masked["label"] == "203.0.113.xxx"
    assert masked["meta"]["filters"]["ip"] == "203.0.113.xxx"


def test_connection_abuse_keeps_ip_for_admin():
    row = ("203.0.113.42", 800, 10, 50, 100)
    raw = defs.connection_abuse_processor(row, None, {})
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


# ── Phase-3: low_and_slow ────────────────────────────────────────────────────


def test_low_and_slow_happy_path():
    """current_val is the distinct sensitive-path count; hits/span/rps land
    in meta and the IP keys both the label and the investigate filter."""
    # row schema: [ip, hits, distinct_paths, span_s, rps]
    out = defs.low_and_slow_processor(("198.51.100.7", 42, 6, 7200, 0.0058), None, {})
    assert out["label"] == "198.51.100.7"
    assert out["current_val"] == 6
    assert out["baseline_val"] is None
    assert out["unit"] == "sensitive paths"
    assert out["meta"]["hits"] == 42
    assert out["meta"]["distinct_paths"] == 6
    assert out["meta"]["span_s"] == 7200
    assert out["meta"]["rps"] == 0.0058
    assert out["meta"]["filters"] == {"ip": "198.51.100.7"}


def test_low_and_slow_critical_at_10_distinct_paths():
    """distinct_paths flips warning → critical at 10 sensitive paths."""
    p = defs.low_and_slow_processor
    assert p(("1.2.3.4", 20, 9, 7200, 0.001), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 20, 10, 7200, 0.001), None, {})["severity"] == "critical"


def test_low_and_slow_handles_none_span_and_rps():
    """A single-timestamp group can yield NULL span/rps; the processor
    coerces to 0 so the meta dict stays JSON-serialisable."""
    out = defs.low_and_slow_processor(("1.2.3.4", 5, 3, None, None), None, {})
    assert out["meta"]["span_s"] == 0
    assert out["meta"]["rps"] == 0.0


# ── Phase-3: credential_enumeration ──────────────────────────────────────────


def test_credential_enumeration_happy_path():
    """current_val is the window denied count; baseline_val is the baseline
    denied count; fail_rate_pct = denied/attempts."""
    # row schema: [ip, w_denied, w_attempts, w_paths, b_denied]
    out = defs.credential_enumeration_processor(("203.0.113.9", 80, 100, 4, 2), None, {})
    assert out["label"] == "203.0.113.9"
    assert out["current_val"] == 80
    assert out["baseline_val"] == 2
    assert out["unit"] == "denied (401/403)"
    assert out["meta"]["attempts"] == 100
    assert out["meta"]["denied"] == 80
    assert out["meta"]["fail_rate_pct"] == 80.0  # 80 / 100 * 100
    assert out["meta"]["distinct_paths"] == 4
    assert out["meta"]["filters"] == {"ip": "203.0.113.9"}


def test_credential_enumeration_critical_at_100_denied():
    """w_denied flips warning → critical at 100 denied auth responses."""
    p = defs.credential_enumeration_processor
    assert p(("1.2.3.4", 99, 120, 3, 0), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 100, 120, 3, 0), None, {})["severity"] == "critical"


def test_credential_enumeration_fail_rate_zero_when_no_attempts():
    """``w_attempts=0`` would divide-by-zero; the processor guards it."""
    out = defs.credential_enumeration_processor(("1.2.3.4", 0, 0, 0, 0), None, {})
    assert out["meta"]["fail_rate_pct"] == 0.0


# ── Phase-4 (Track A): content_discovery ─────────────────────────────────────


def test_content_discovery_happy_path():
    """current_val is the window 404 count; baseline_val the baseline 404
    count; not_found_rate_pct = 404s/total; the IP keys label + filter."""
    # row schema: [ip, w_404, w_total, distinct_404, b_404]
    out = defs.content_discovery_processor(("198.51.100.7", 60, 75, 40, 3), None, {})
    assert out["label"] == "198.51.100.7"
    assert out["current_val"] == 60
    assert out["baseline_val"] == 3
    assert out["unit"] == "404s"
    assert out["meta"]["requests"] == 75
    assert out["meta"]["not_found"] == 60
    assert out["meta"]["not_found_rate_pct"] == 80.0  # 60 / 75 * 100
    assert out["meta"]["distinct_404_urls"] == 40
    assert out["meta"]["filters"] == {"ip": "198.51.100.7"}


def test_content_discovery_critical_at_100_not_found():
    """w_404 flips warning → critical at 100 window 404s."""
    p = defs.content_discovery_processor
    assert p(("1.2.3.4", 99, 120, 40, 0), None, {})["severity"] == "warning"
    assert p(("1.2.3.4", 100, 120, 40, 0), None, {})["severity"] == "critical"


def test_content_discovery_rate_zero_when_no_total():
    """``w_total=0`` would divide-by-zero; the processor guards it."""
    out = defs.content_discovery_processor(("1.2.3.4", 0, 0, 0, 0), None, {})
    assert out["meta"]["not_found_rate_pct"] == 0.0


# ── Phase-4 (Track B): referer_monoculture ───────────────────────────────────


def test_referer_monoculture_happy_path():
    # row schema: [referer, w_cnt, b_cnt, b_total, w_total]
    out = defs.referer_monoculture_processor(("http://spam.example", 40, 5, 500, 100), None, {})
    assert out["label"] == "http://spam.example"
    assert out["current_val"] == 40.0  # 40/100 * 100
    assert out["baseline_val"] == 1.0  # 5/500 * 100
    assert out["unit"] == "% of traffic"
    assert out["meta"]["requests"] == 40
    assert out["meta"]["filters"] == {"referer": "http://spam.example"}


def test_referer_monoculture_critical_at_50_percent():
    p = defs.referer_monoculture_processor
    assert p(("r", 49, 0, 100, 100), None, {})["severity"] == "warning"
    assert p(("r", 50, 0, 100, 100), None, {})["severity"] == "critical"


def test_referer_monoculture_empty_label_and_zero_total_guard():
    out = defs.referer_monoculture_processor((None, 0, 0, 0, 0), None, {})
    assert out["label"] == "(empty)"
    assert out["current_val"] == 0.0  # w_total=0 guarded


# ── Phase-4 (Track B): method_drift ──────────────────────────────────────────


def test_method_drift_happy_path():
    # row schema: [method, w_cnt, b_cnt, w_total, b_total]
    out = defs.method_drift_processor(("POST", 30, 2, 100, 200), None, {})
    assert out["label"] == "POST"
    assert out["current_val"] == 30.0  # 30/100 * 100
    assert out["baseline_val"] == 1.0  # 2/200 * 100
    assert out["unit"] == "% of traffic"
    assert out["meta"]["filters"] == {"method": "POST"}


def test_method_drift_critical_at_25_percent():
    p = defs.method_drift_processor
    assert p(("POST", 24, 0, 100, 100), None, {})["severity"] == "warning"
    assert p(("POST", 25, 0, 100, 100), None, {})["severity"] == "critical"


# ── Phase-4 (Track B): new_asn_traffic ───────────────────────────────────────


def test_new_asn_traffic_happy_path():
    # row schema: [asn, w_cnt, b_cnt]
    out = defs.new_asn_traffic_processor((16509, 60, 0), None, {})
    assert out["label"] == "AS16509"
    assert out["current_val"] == 60
    assert out["baseline_val"] == 0
    assert out["unit"] == "requests"
    assert out["meta"]["asn"] == 16509
    assert out["meta"]["filters"] == {"asn": 16509}


def test_new_asn_traffic_attaches_name_and_severity_floor():
    p = defs.new_asn_traffic_processor
    with_name = p((7922, 150, 0), None, {"asn_names": {7922: "Example ISP"}})
    assert with_name["label"] == "AS7922 (Example ISP)"
    assert with_name["severity"] == "warning"  # >= 100
    assert p((7922, 99, 0), None, {})["severity"] == "info"  # < 100


# ── Phase-4 (Track B): asn_hosting_shift ──────────────────────────────────────


def test_asn_hosting_shift_happy_path():
    # row schema: [asn, w_hosting, w_total, b_hosting, b_total]
    out = defs.asn_hosting_shift_processor((16509, 45, 100, 5, 200), None, {})
    assert out["label"] == "AS16509"
    assert out["current_val"] == 45.0  # 45/100 = 45%
    assert out["baseline_val"] == 2.5  # 5/200 = 2.5%
    assert out["unit"] == "% hosting"
    assert out["meta"]["asn"] == 16509
    assert out["meta"]["filters"] == {"asn": 16509}
    assert out["meta"]["window_hosting"] == 45
    assert out["meta"]["window_total"] == 100


def test_asn_hosting_shift_attaches_name():
    out = defs.asn_hosting_shift_processor((7922, 30, 60, 2, 100), None, {"asn_names": {7922: "Comcast"}})
    assert out["label"] == "AS7922 (Comcast)"


def test_asn_hosting_shift_severity_boundary():
    p = defs.asn_hosting_shift_processor
    # 60% → critical
    assert p((1, 60, 100, 0, 100), None, {})["severity"] == "critical"
    # 59.9% → warning
    assert p((1, 599, 1000, 0, 100), None, {})["severity"] == "warning"


def test_asn_hosting_shift_zero_total_safety():
    out = defs.asn_hosting_shift_processor((1, 0, 0, 0, 0), None, {})
    assert out["current_val"] == 0.0
    assert out["baseline_val"] == 0.0


# ── Phase-4 (Track B2): metro_delivery_degradation ───────────────────────────


def test_metro_delivery_degradation_happy_path():
    # row schema: [metro, w_med(bytes/s), b_med(bytes/s), w_total, b_total]
    # 1_000_000 B/s = 8.0 Mbps; 10_000_000 B/s = 80.0 Mbps.
    out = defs.metro_delivery_degradation_processor((501, 1_000_000, 10_000_000, 60, 200), None, {})
    assert out["current_val"] == 8.0
    assert out["baseline_val"] == 80.0
    assert out["unit"] == "Mbps (median)"
    assert out["meta"]["filters"] == {"metro": 501}
    assert out["label"] == "DMA 501"  # no dma_map in context


def test_metro_delivery_degradation_uses_dma_map_and_severity():
    p = defs.metro_delivery_degradation_processor
    named = p((501, 1_000_000, 10_000_000, 60, 200), None, {"dma_map": {"501": "New York City"}})
    assert named["label"] == "New York City"
    assert named["severity"] == "critical"  # 8 <= 80 * 0.25
    mild = p((501, 4_000_000, 10_000_000, 60, 200), None, {})  # 40 vs 80 → not <= 25%
    assert mild["severity"] == "warning"


# ── Phase-4 (Track B2): connection_type_mix ──────────────────────────────────


def test_connection_type_mix_happy_path():
    # row schema: [c_type, c_speed, w_cnt, b_cnt, w_total, b_total]
    out = defs.connection_type_mix_processor(("cellular", "mobile", 60, 5, 100, 500), None, {})
    assert out["label"] == "cellular / mobile"
    assert out["current_val"] == 60.0  # 60/100
    assert out["baseline_val"] == 1.0  # 5/500
    assert out["meta"]["filters"] == {"c_type": "cellular", "c_speed": "mobile"}


# ── Phase-4 (Track B2): pop_latency_regression ───────────────────────────────


def test_pop_latency_regression_happy_path_and_severity():
    # row schema: [pop, w_p95(ms), b_p95(ms), w_total, b_total]
    p = defs.pop_latency_regression_processor
    out = p(("JFK", 3000.0, 50.0, 60, 200), None, {})
    assert out["label"] == "JFK"
    assert out["current_val"] == 3000.0
    assert out["baseline_val"] == 50.0
    assert out["unit"] == "ms (P95)"
    assert out["meta"]["filters"] == {"pop": "JFK"}
    assert out["severity"] == "warning"  # < 5000
    assert p(("JFK", 5000.0, 50.0, 60, 200), None, {})["severity"] == "critical"


# ── Phase-4 (Track B2): http3_fallback ───────────────────────────────────────


def test_http3_fallback_happy_path_and_severity():
    # row schema: [w_quic, w_total, b_quic, b_total]
    p = defs.http3_fallback_processor
    out = p((0, 1000, 800, 1000), None, {})
    assert out["current_val"] == 0.0  # 0% QUIC in window
    assert out["baseline_val"] == 80.0  # 80% QUIC baseline
    assert out["unit"] == "% QUIC"
    assert out["severity"] == "critical"  # drop >= 40 pts
    mild = p((550, 1000, 700, 1000), None, {})  # 55% vs 70% → 15pt drop
    assert mild["severity"] == "warning"


# ── Phase-4 (Track B2): cache_hit_cliff ──────────────────────────────────────


def test_cache_hit_cliff_happy_path_and_severity():
    # row schema: [w_hits, w_cacheable, b_hits, b_cacheable]
    p = defs.cache_hit_cliff_processor
    out = p((10, 1000, 700, 1000), None, {})
    assert out["current_val"] == 1.0  # 10/1000
    assert out["baseline_val"] == 70.0  # 700/1000
    assert out["unit"] == "% HIT"
    assert out["severity"] == "critical"  # drop >= 30 pts
    mild = p((550, 1000, 700, 1000), None, {})  # 55 vs 70 → 15pt drop
    assert mild["severity"] == "warning"


# ── Phase-4 (Track C): payload_compression_regression ────────────────────────


def test_payload_compression_regression_happy_path_and_severity():
    # row schema: [url, w_rate, b_rate, w_total, b_total]  (rates = uncompressed share)
    p = defs.payload_compression_regression_processor
    out = p(("/app.js", 0.8, 0.02, 40, 200), None, {})
    assert out["label"] == "/app.js"
    assert out["current_val"] == 80.0
    assert out["baseline_val"] == 2.0
    assert out["unit"] == "% uncompressed"
    assert out["meta"]["filters"] == {"url": "/app.js"}
    assert out["severity"] == "warning"  # < 0.90
    assert p(("/app.js", 0.90, 0.0, 40, 200), None, {})["severity"] == "critical"


# ── Phase-4 (Track C): session_harvesting ────────────────────────────────────


def test_session_harvesting_happy_path_and_severity():
    # row schema: [ip, w_sessions, w_reqs, b_sessions]
    p = defs.session_harvesting_processor
    out = p(("203.0.113.5", 60, 200, 3), None, {})
    assert out["label"] == "203.0.113.5"
    assert out["current_val"] == 60
    assert out["baseline_val"] == 3
    assert out["unit"] == "distinct sessions"
    assert out["meta"]["filters"] == {"ip": "203.0.113.5"}
    assert out["severity"] == "warning"  # < 100
    assert p(("203.0.113.5", 100, 300, 3), None, {})["severity"] == "critical"


def test_session_harvesting_never_surfaces_a_session_id():
    """The cookie_session hash is only counted upstream; the card must expose a
    COUNT, never a session id, so no session token can leak even unmasked."""
    out = defs.session_harvesting_processor(("203.0.113.5", 60, 200, 3), None, {})
    blob = repr(out)
    assert "cookie_session" not in blob and "sess" not in out["label"]
    assert isinstance(out["current_val"], int)


@pytest.mark.security_regression
def test_session_harvesting_masks_ip_when_mask_ips():
    # row schema: [ip, w_sessions, w_reqs, b_sessions]
    row = ("203.0.113.42", 60, 200, 3)
    masked = defs.session_harvesting_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected


def test_session_harvesting_keeps_ip_for_admin():
    raw = defs.session_harvesting_processor(("203.0.113.42", 60, 200, 3), None, {})
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


# ── Phase-4 (Track C): timeout_split ─────────────────────────────────────────


def test_timeout_split_connect_phase_dominant():
    # row schema: [w_conn, b_conn, w_read, b_read, w_total, b_total]  (ms P95)
    out = defs.timeout_split_processor((500.0, 5.0, 60.0, 55.0, 100, 300), None, {})
    assert out["label"] == "Origin connect P95"
    assert out["current_val"] == 500.0
    assert out["baseline_val"] == 5.0
    assert out["meta"]["phase"] == "connect"
    assert out["severity"] == "critical"  # 500 >= 5*3


def test_timeout_split_read_phase_dominant():
    # read regressed, connect steady → read is the headline
    out = defs.timeout_split_processor((10.0, 8.0, 900.0, 100.0, 100, 300), None, {})
    assert out["label"] == "Origin read P95"
    assert out["meta"]["phase"] == "read"
    assert out["current_val"] == 900.0


# ── Phase-3: network_asn_health ──────────────────────────────────────────────


def test_network_asn_health_happy_path():
    """current_val / baseline_val are packet-loss percentages (ploss * 100),
    rounded to 2 dp; the ASN keys both the label and the filter."""
    # row schema: [asn, w_ploss, b_ploss, w_jitter, b_jitter, w_retrans, b_retrans, w_total, b_total]
    row = (16509, 0.08, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    out = defs.network_asn_health_processor(row, None, {})
    assert out["label"] == "AS16509"
    assert out["current_val"] == 8.0  # round(0.08 * 100, 2)
    assert out["baseline_val"] == 0.1  # round(0.001 * 100, 2)
    assert out["unit"] == "% packet loss"
    assert out["meta"]["window_packet_loss_pct"] == 8.0
    assert out["meta"]["requests"] == 60
    assert out["meta"]["asn"] == 16509
    assert out["meta"]["filters"] == {"asn": 16509}


def test_network_asn_health_attaches_name_when_available():
    """``context['asn_names']`` appends the resolved ISP name to the label;
    without it, the label is just ``AS{n}``. NOT IP-keyed, so no masking."""
    p = defs.network_asn_health_processor
    row = (7922, 0.06, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    with_name = p(row, None, {"asn_names": {7922: "Example ISP"}})
    without = p(row, None, {})
    assert with_name["label"] == "AS7922 (Example ISP)"
    assert without["label"] == "AS7922"


def test_network_asn_health_critical_at_5_percent_packet_loss():
    """w_ploss flips warning → critical at 0.05 (5% packet loss)."""
    p = defs.network_asn_health_processor
    warn = (1, 0.049, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    crit = (1, 0.05, 0.001, 5000.0, 1000.0, 1.2, 0.1, 60, 120)
    assert p(warn, None, {})["severity"] == "warning"
    assert p(crit, None, {})["severity"] == "critical"


# ── Phase-3: IP-keyed processors mask the client IP when the analyst sets ─────
# mask_ips. ANALYST = adversary: the raw IP must not survive in the label OR in
# meta.filters.ip (which seeds investigate_url). Mirrors the request_size_anomaly
# / connection_abuse mask contract above.


@pytest.mark.security_regression
def test_low_and_slow_masks_ip_when_mask_ips():
    # row schema: [ip, hits, distinct_paths, span_s, rps]
    row = ("203.0.113.42", 42, 6, 7200, 0.0058)
    masked = defs.low_and_slow_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected  # also feeds investigate_url


def test_low_and_slow_keeps_ip_for_admin():
    row = ("203.0.113.42", 42, 6, 7200, 0.0058)
    raw = defs.low_and_slow_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


@pytest.mark.security_regression
def test_credential_enumeration_masks_ip_when_mask_ips():
    # row schema: [ip, w_denied, w_attempts, w_paths, b_denied]
    row = ("203.0.113.42", 80, 100, 4, 2)
    masked = defs.credential_enumeration_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected  # also feeds investigate_url


def test_credential_enumeration_keeps_ip_for_admin():
    row = ("203.0.113.42", 80, 100, 4, 2)
    raw = defs.credential_enumeration_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"


@pytest.mark.security_regression
def test_content_discovery_masks_ip_when_mask_ips():
    # row schema: [ip, w_404, w_total, distinct_404, b_404]
    row = ("203.0.113.42", 60, 75, 40, 3)
    masked = defs.content_discovery_processor(row, None, {"mask_ips": True})
    expected = mask_ip("203.0.113.42")
    assert masked["label"] == expected
    assert masked["meta"]["filters"]["ip"] == expected  # also feeds investigate_url


def test_content_discovery_keeps_ip_for_admin():
    row = ("203.0.113.42", 60, 75, 40, 3)
    raw = defs.content_discovery_processor(row, None, {})  # no mask_ips
    assert raw["label"] == "203.0.113.42"
    assert raw["meta"]["filters"]["ip"] == "203.0.113.42"
