"""Template-render tests for `backend.repositories._sql.insights`.

Phase 5b — string-level renders only (no DuckDB needed). For each
template constant we assert the rendered output contains the expected
fragments and pin the exact set of format placeholders. Plus one
parity test that asserts every registered insight's ``sql_template``
points to the matching module-level constant.
"""

from __future__ import annotations

from backend.repositories._sql import insights as SQL
from backend.repositories.insights.registry import registry


def _placeholders(template: str) -> list[str]:
    """Return the sorted unique list of ``{name}`` placeholders in ``template``.

    Strips empty positional braces (``{}``) so f-string-built literals
    with escaped ``{{}}`` brace pairs don't surface as bogus placeholders.
    """
    names = {p.split("}")[0] for p in template.split("{")[1:] if "}" in p}
    names.discard("")
    return sorted(names)


# ── Registry parity ───────────────────────────────────────────────────────────


def test_registry_sql_templates_match_module_constants():
    """Every registered insight's sql_template must be a constant in this module.

    Catches drift where a future edit changes the template inline in
    ``definitions.py`` instead of in ``_sql/insights.py``.
    """
    # id → expected SQL constant name
    expected = {
        "error_spikes": "ERROR_SPIKES",
        "botnet_grouping": "BOTNET_GROUPING",
        "new_country_traffic": "NEW_COUNTRY_TRAFFIC",
        "city_surges": "CITY_SURGES",
        "city_error_spikes": "CITY_ERROR_SPIKES",
        "city_latency_regressions": "CITY_LATENCY_REGRESSIONS",
        "new_city_traffic": "NEW_CITY_TRAFFIC",
        "ua_monoculture": "UA_MONOCULTURE",
        "new_probe_urls": "NEW_PROBE_URLS",
        "waf_signal_spikes": "WAF_SIGNAL_SPIKES",
        "proxy_surge": "PROXY_SURGE",
        "asn_concentration": "ASN_CONCENTRATION",
        "asn_metro_performance": "ASN_METRO_PERFORMANCE",
        "cache_collapse": "CACHE_COLLAPSE",
        "latency_regression": "LATENCY_REGRESSION",
        "impossible_distance": "IMPOSSIBLE_DISTANCE",
        "tail_latency": "TAIL_LATENCY",
        "cipher_spread": "CIPHER_SPREAD",
        "request_size_anomaly": "REQUEST_SIZE_ANOMALY",
        "connection_abuse": "CONNECTION_ABUSE",
        "region_latency": "REGION_LATENCY",
        "cache_ttl_mismatch": "CACHE_TTL_MISMATCH",
        "image_optimization_opportunities": "IMAGE_OPTIMIZATION_OPPORTUNITIES",
        "origin_latency_spike": "ORIGIN_LATENCY_SPIKE",
        "origin_error_rate": "ORIGIN_ERROR_RATE",
        "origin_retries": "ORIGIN_RETRIES",
        "origin_ip_failure": "ORIGIN_IP_FAILURE",
        "shield_path_degradation": "SHIELD_PATH_DEGRADATION",
    }

    for insight_id, const_name in expected.items():
        d = registry.get(insight_id)
        assert d is not None, f"insight {insight_id} not registered"
        # Compare by value rather than identity — pydantic v2 may copy strings
        # when validating model fields, so `is` would be flaky.
        assert d.sql_template == getattr(SQL, const_name), (
            f"insight {insight_id} sql_template diverged from SQL.{const_name}"
        )


# ── definitions.py templates ──────────────────────────────────────────────────


def test_error_spikes_renders_and_pins_placeholders():
    rendered = SQL.ERROR_SPIKES.format(table_name="t_logs")
    assert "FROM t_logs" in rendered
    assert "CAST(? AS TIMESTAMPTZ)" in rendered
    assert "HAVING w_total >= 3" in rendered
    assert "ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.ERROR_SPIKES) == ["table_name"]


def test_botnet_grouping_renders_and_pins_placeholders():
    rendered = SQL.BOTNET_GROUPING.format(
        table_name="t_logs", fp_col="ja4", baseline_hours=24, window_hours=1
    )
    assert 'FROM t_logs WHERE "ja4" IS NOT NULL' in rendered
    assert "GREATEST(24, 1.0) * 1" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.BOTNET_GROUPING) == sorted(
        ["table_name", "fp_col", "baseline_hours", "window_hours"]
    )


def test_new_country_traffic_renders_and_pins_placeholders():
    rendered = SQL.NEW_COUNTRY_TRAFFIC.format(table_name="t_logs")
    assert 'WHERE "country" IS NOT NULL' in rendered
    assert "HAVING w_cnt >= 3 AND b_cnt = 0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.NEW_COUNTRY_TRAFFIC) == ["table_name"]


def test_city_surges_renders_and_pins_placeholders():
    rendered = SQL.CITY_SURGES.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
        loc_cols='"country", "region"',
        baseline_hours=24,
        window_hours=1,
    )
    assert "FROM t_logs" in rendered
    assert 'WHERE "city" IS NOT NULL' in rendered
    assert "ORDER BY spike_ratio DESC LIMIT 15" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.CITY_SURGES) == sorted(
        [
            "table_name",
            "label_expr",
            "region_sel",
            "country_sel",
            "loc_cols",
            "baseline_hours",
            "window_hours",
        ]
    )


def test_city_error_spikes_renders_and_pins_placeholders():
    rendered = SQL.CITY_ERROR_SPIKES.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
        loc_cols='"country", "region"',
    )
    assert "WITH base AS" in rendered
    assert 'WHERE "city" IS NOT NULL' in rendered
    assert "HAVING w_total >= 10 AND w_rate >= 0.10" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.CITY_ERROR_SPIKES) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]
    )


def test_city_latency_regressions_renders_and_pins_placeholders():
    rendered = SQL.CITY_LATENCY_REGRESSIONS.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
        loc_cols='"country", "region"',
    )
    assert "PERCENTILE_CONT(0.95)" in rendered
    assert "w_p95 >= b_p95 * 3.0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.CITY_LATENCY_REGRESSIONS) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]
    )


def test_new_city_traffic_renders_and_pins_placeholders():
    rendered = SQL.NEW_CITY_TRAFFIC.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
        loc_cols='"country", "region"',
    )
    assert "HAVING w_cnt >= 5 AND b_cnt = 0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.NEW_CITY_TRAFFIC) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]
    )


def test_ua_monoculture_renders_and_pins_placeholders():
    rendered = SQL.UA_MONOCULTURE.format(table_name="t_logs")
    assert 'FROM t_logs GROUP BY "ua"' in rendered
    assert rendered.count("?") == 4
    assert _placeholders(SQL.UA_MONOCULTURE) == ["table_name"]


def test_new_probe_urls_bakes_regex_and_pins_placeholders():
    rendered = SQL.NEW_PROBE_URLS.format(table_name="t_logs")
    assert "FROM t_logs" in rendered
    # Regex is f-string-baked at import time. ``re.escape('.env')`` → ``\\.env``.
    assert "regexp_matches(" in rendered
    assert "\\.env" in rendered
    assert "admin" in rendered
    assert "'i'" in rendered  # case-insensitive flag passed via SQL arg
    assert rendered.count("?") == 3
    assert _placeholders(SQL.NEW_PROBE_URLS) == ["table_name"]


def test_waf_signal_spikes_renders_and_pins_placeholders():
    rendered = SQL.WAF_SIGNAL_SPIKES.format(
        table_name="t_logs", baseline_hours=24, window_hours=1
    )
    assert "WITH all_signals AS" in rendered
    assert "BOT-ANALYSIS" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.WAF_SIGNAL_SPIKES) == sorted(
        ["table_name", "baseline_hours", "window_hours"]
    )


def test_proxy_surge_renders_and_pins_placeholders():
    rendered = SQL.PROXY_SURGE.format(table_name="t_logs")
    assert 'FROM t_logs WHERE "p_type" IS NOT NULL' in rendered
    assert "totals AS" in rendered
    assert rendered.count("?") == 4
    assert _placeholders(SQL.PROXY_SURGE) == ["table_name"]


def test_asn_concentration_renders_and_pins_placeholders():
    rendered = SQL.ASN_CONCENTRATION.format(table_name="t_logs")
    assert 'GROUP BY "asn"' in rendered
    assert "w_cnt * 1.0 / w_total >= 0.20" in rendered
    assert rendered.count("?") == 4
    assert _placeholders(SQL.ASN_CONCENTRATION) == ["table_name"]


def test_asn_metro_performance_renders_and_pins_placeholders():
    rendered = SQL.ASN_METRO_PERFORMANCE.format(table_name="t_logs")
    assert '"country" = \'US\'' in rendered
    assert "w_med >= b_med * 1.5" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.ASN_METRO_PERFORMANCE) == ["table_name"]


def test_cache_collapse_renders_and_pins_placeholders():
    rendered = SQL.CACHE_COLLAPSE.format(table_name="t_logs")
    assert "cache ILIKE 'HIT%'" in rendered
    assert "b_rate >= 0.40" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.CACHE_COLLAPSE) == ["table_name"]


def test_latency_regression_renders_and_pins_placeholders():
    rendered = SQL.LATENCY_REGRESSION.format(table_name="t_logs")
    assert "PERCENTILE_CONT(0.95)" in rendered
    assert "w_p95 >= b_p95 * 2.0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.LATENCY_REGRESSION) == ["table_name"]


def test_impossible_distance_renders_and_pins_placeholders():
    rendered = SQL.IMPOSSIBLE_DISTANCE.format(
        table_name="t_logs",
        fp_col="ja4",
        pop_values="('SJC', 37.0::DOUBLE, -121.0::DOUBLE)",
        edge_filter='AND t."edge" = true',
    )
    assert "WITH pop_coords(pop_code, pop_lat, pop_lon) AS (VALUES ('SJC'" in rendered
    assert "RADIANS" in rendered
    assert 'AND t."edge" = true' in rendered
    assert rendered.count("?") == 1
    assert _placeholders(SQL.IMPOSSIBLE_DISTANCE) == sorted(
        ["table_name", "fp_col", "pop_values", "edge_filter"]
    )


def test_tail_latency_renders_and_pins_placeholders():
    rendered = SQL.TAIL_LATENCY.format(table_name="t_logs")
    assert "PERCENTILE_CONT(0.99)" in rendered
    assert "PERCENTILE_CONT(0.50)" in rendered
    assert "ratio > 5" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(SQL.TAIL_LATENCY) == ["table_name"]


def test_cipher_spread_renders_and_pins_placeholders():
    rendered = SQL.CIPHER_SPREAD.format(
        table_name="t_logs", baseline_hours=24, window_hours=1
    )
    assert '"tls_ciphers_sha" IS NOT NULL' in rendered
    assert "COUNT(DISTINCT \"ip\")" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.CIPHER_SPREAD) == sorted(
        ["table_name", "baseline_hours", "window_hours"]
    )


def test_request_size_anomaly_renders_and_pins_placeholders():
    rendered = SQL.REQUEST_SIZE_ANOMALY.format(table_name="t_logs")
    assert "req_header_bytes > 0" in rendered
    assert "max_bytes > b_p95 * 3" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.REQUEST_SIZE_ANOMALY) == ["table_name"]


def test_connection_abuse_renders_and_pins_placeholders():
    rendered = SQL.CONNECTION_ABUSE.format(table_name="t_logs")
    assert "conn_requests > 0" in rendered
    assert "max_reqs > b_p95 * 3 AND max_reqs >= 50" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.CONNECTION_ABUSE) == ["table_name"]


def test_region_latency_renders_and_pins_placeholders():
    rendered = SQL.REGION_LATENCY.format(table_name="t_logs")
    assert "region_stats AS" in rendered
    assert "origin_stats AS" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.REGION_LATENCY) == ["table_name"]


def test_cache_ttl_mismatch_renders_and_pins_placeholders():
    rendered = SQL.CACHE_TTL_MISMATCH.format(table_name="t_logs", q_col='"url"')
    assert 'SELECT "url" AS label' in rendered
    assert 'AVG("hits") < 2 AND AVG("ttl") > 60' in rendered
    assert rendered.count("?") == 1
    assert _placeholders(SQL.CACHE_TTL_MISMATCH) == sorted(["table_name", "q_col"])


def test_image_optimization_opportunities_renders_and_pins_placeholders():
    rendered = SQL.IMAGE_OPTIMIZATION_OPPORTUNITIES.format(
        table_name="t_logs", ua_mobile_sel="0"
    )
    assert 'WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "status" = 200' in rendered
    assert "(0) AS mobile_ratio" in rendered
    assert "%.jpg%" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(SQL.IMAGE_OPTIMIZATION_OPPORTUNITIES) == sorted(
        ["table_name", "ua_mobile_sel"]
    )


def test_origin_latency_spike_renders_and_pins_placeholders():
    rendered = SQL.ORIGIN_LATENCY_SPIKE.format(table_name="t_logs", url_col='"url"')
    assert "overall_stats AS" in rendered
    assert "url_stats AS" in rendered
    assert "o.w_p95 > o.b_p95 * 2" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.ORIGIN_LATENCY_SPIKE) == sorted(["table_name", "url_col"])


def test_origin_error_rate_renders_and_pins_placeholders():
    rendered = SQL.ORIGIN_ERROR_RATE.format(table_name="t_logs")
    assert '"ost" AS status' in rendered
    assert "status >= 500" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.ORIGIN_ERROR_RATE) == ["table_name"]


def test_origin_retries_renders_and_pins_placeholders():
    rendered = SQL.ORIGIN_RETRIES.format(table_name="t_logs", url_col='"url"')
    assert 'AVG("oretries")' in rendered
    assert "requests >= 5" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(SQL.ORIGIN_RETRIES) == sorted(["table_name", "url_col"])


def test_origin_ip_failure_renders_and_pins_placeholders():
    rendered = SQL.ORIGIN_IP_FAILURE.format(table_name="t_logs")
    assert '"oip" IS NOT NULL' in rendered
    assert "median_calc AS" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(SQL.ORIGIN_IP_FAILURE) == ["table_name"]


def test_shield_path_degradation_renders_and_pins_placeholders():
    rendered = SQL.SHIELD_PATH_DEGRADATION.format(table_name="t_logs")
    assert "edge_logs AS" in rendered
    assert "shield_logs AS" in rendered
    assert "'Direct to Origin'" in rendered
    assert rendered.count("?") == 3
    assert _placeholders(SQL.SHIELD_PATH_DEGRADATION) == ["table_name"]


# ── repository.py coalesced templates ─────────────────────────────────────────


def test_coalesced_city_aggregates_renders_and_pins_placeholders():
    rendered = SQL.COALESCED_CITY_AGGREGATES.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
    )
    assert "FROM t_logs" in rendered
    assert 'WHERE "city" IS NOT NULL' in rendered
    assert "GROUP BY ALL" in rendered
    assert "w_lat_total" in rendered
    assert "b_lat_total" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.COALESCED_CITY_AGGREGATES) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel"]
    )


def test_coalesced_url_aggregates_renders_and_pins_placeholders():
    rendered = SQL.COALESCED_URL_AGGREGATES.format(table_name="t_logs")
    assert 'FROM t_logs' in rendered
    assert 'WHERE "url" IS NOT NULL' in rendered
    assert "w_5xx" in rendered
    assert "w_hits" in rendered
    assert "w_p99" in rendered
    assert "w_p50" in rendered
    assert 'GROUP BY "url"' in rendered
    assert rendered.count("?") == 2
    assert _placeholders(SQL.COALESCED_URL_AGGREGATES) == ["table_name"]


# ── NEW_PROBE_REGEX sanity ────────────────────────────────────────────────────


def test_new_probe_regex_contains_all_probes_escaped():
    """Each entry in NEW_PROBES must appear in the regex, properly escaped."""
    import re

    for probe in SQL.NEW_PROBES:
        assert re.escape(probe) in SQL.NEW_PROBE_REGEX, f"probe {probe!r} missing from regex"
