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
