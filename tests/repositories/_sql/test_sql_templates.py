from __future__ import annotations

from backend.repositories._sql import alerts as alerts_sql
from backend.repositories._sql import dashboard as dashboard_sql
from backend.repositories._sql import insights as insights_sql
from backend.repositories._sql import network as network_sql
from backend.repositories._sql import origin as origin_sql
from backend.repositories._sql import query as query_sql
from backend.repositories._sql import security as security_sql
from backend.repositories._sql import sessions as sessions_sql
from backend.repositories._sql import usage as usage_sql
from backend.repositories.insights.registry import registry

# ── From test_alerts.py ──

"""Template-render tests for `backend.repositories._sql.alerts`.

Phase 5a — verifies the format-template structure (no DuckDB needed).
"""


def test_max_timestamp_renders_with_table_name():
    rendered = alerts_sql.MAX_TIMESTAMP.format(table='"logs_xyz"')
    assert rendered == 'SELECT max(timestamp) FROM "logs_xyz"'


def test_max_timestamp_template_pins_only_table_placeholder():
    placeholders = sorted(p.split("}")[0] for p in alerts_sql.MAX_TIMESTAMP.split("{")[1:] if "}" in p)
    assert placeholders == ["table"]


# ── From test_dashboard.py ──


def _placeholders(template: str) -> list[str]:
    """Extract the unique ``{name}``-style format placeholders from ``template``."""
    return sorted({p.split("}")[0] for p in template.split("{")[1:] if "}" in p and p.split("}")[0]})


def _placeholders_all(template: str) -> list[str]:
    """Return the sorted list (preserving multiplicity) of ``{name}`` placeholders in ``template``."""
    return sorted(p.split("}")[0] for p in template.split("{")[1:] if "}" in p)


def test_virtual_field_exploded_top_n_renders_with_all_inputs():
    rendered = dashboard_sql.VIRTUAL_FIELD_EXPLODED_TOP_N.format(
        backing_col="waf_sig",
        table_name='"logs_xyz"',
        where_clause="1=1",
        requests_metric="COUNT(*)",
    )
    assert "unnest(string_split(\"waf_sig\", ','))" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "WITH split_data AS" in rendered
    assert "CROSS JOIN total_count" in rendered
    assert "LIMIT 10" in rendered


def test_virtual_field_exploded_top_n_placeholders_pinned():
    assert _placeholders(dashboard_sql.VIRTUAL_FIELD_EXPLODED_TOP_N) == sorted(
        [
            "backing_col",
            "table_name",
            "where_clause",
            "requests_metric",
        ]
    )


def test_conn_requests_bucket_renders_with_all_inputs():
    rendered = dashboard_sql.CONN_REQUESTS_BUCKET.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
    )
    # Bucket labels must use en-dash (U+2013) — the frontend matches the exact strings.
    assert "'2–5'" in rendered
    assert "'6–20'" in rendered
    assert "'21+'" in rendered
    assert 'ORDER BY MIN("conn_requests")' in rendered
    assert 'FROM "logs_xyz"' in rendered


def test_conn_requests_bucket_placeholders_pinned():
    assert _placeholders(dashboard_sql.CONN_REQUESTS_BUCKET) == sorted(
        [
            "requests_metric",
            "table_name",
            "where_clause",
        ]
    )


def test_time_series_renders_without_extra_where():
    rendered = dashboard_sql.TIME_SERIES.format(
        time_bucket_select="time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
        value_expr="COUNT(*)",
        table_name='"logs_xyz"',
        extra_where="",
        where_clause="status = 200",
    )
    assert "time_bucket(INTERVAL '1 minute', timestamp)" in rendered
    assert "COUNT(*) AS value" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "WHERE timestamp IS NOT NULL AND status = 200" in rendered
    assert "GROUP BY 1 ORDER BY 1" in rendered


def test_time_series_renders_with_extra_where_for_latency():
    rendered = dashboard_sql.TIME_SERIES.format(
        time_bucket_select="time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
        value_expr="PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0",
        table_name='"logs_xyz"',
        extra_where=' AND "elapsed" IS NOT NULL',
        where_clause="1=1",
    )
    # Extra-where injects between the timestamp gate and the main WHERE clause.
    assert 'WHERE timestamp IS NOT NULL AND "elapsed" IS NOT NULL AND 1=1' in rendered


def test_time_series_placeholders_pinned():
    assert _placeholders(dashboard_sql.TIME_SERIES) == sorted(
        [
            "time_bucket_select",
            "value_expr",
            "table_name",
            "extra_where",
            "where_clause",
        ]
    )


def test_map_data_by_country_renders():
    rendered = dashboard_sql.MAP_DATA_BY_COUNTRY.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
    )
    assert 'SELECT "country" AS country' in rendered
    assert "COUNT(*) AS count" in rendered
    assert 'WHERE "country" IS NOT NULL AND 1=1' in rendered
    assert "GROUP BY 1" in rendered


def test_map_data_by_country_placeholders_pinned():
    assert _placeholders(dashboard_sql.MAP_DATA_BY_COUNTRY) == sorted(
        [
            "requests_metric",
            "table_name",
            "where_clause",
        ]
    )


def test_field_values_bot_ua_renders_with_filter():
    rendered = dashboard_sql.FIELD_VALUES_BOT_UA.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        ua_filter="AND regexp_matches(ua, 'bot|crawl')",
    )
    assert "SELECT ua, COUNT(*) AS cnt" in rendered
    assert "WHERE 1=1 AND ua IS NOT NULL AND regexp_matches(ua, 'bot|crawl')" in rendered
    assert "GROUP BY ua" in rendered
    assert "ORDER BY cnt DESC" in rendered
    assert "LIMIT 5000" in rendered


def test_field_values_bot_ua_renders_without_filter():
    rendered = dashboard_sql.FIELD_VALUES_BOT_UA.format(
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        ua_filter="",
    )
    assert "WHERE 1=1 AND ua IS NOT NULL " in rendered


def test_field_values_bot_ua_placeholders_pinned():
    assert _placeholders(dashboard_sql.FIELD_VALUES_BOT_UA) == sorted(
        [
            "requests_metric",
            "table_name",
            "where_clause",
            "ua_filter",
        ]
    )


def test_field_values_virtual_signals_renders_with_search():
    rendered = dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS.format(
        requests_metric="COUNT(*)",
        backing_col="waf_sig",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond="AND trim(signal) ILIKE ?",
        limit=20,
    )
    assert "unnest(string_split(\"waf_sig\", ','))" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "WHERE trim(signal) != '' AND trim(signal) ILIKE ?" in rendered
    assert "LIMIT 20" in rendered


def test_field_values_virtual_signals_renders_without_search():
    rendered = dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS.format(
        requests_metric="COUNT(*)",
        backing_col="edge_score_reason",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond="",
        limit=10,
    )
    assert "unnest(string_split(\"edge_score_reason\", ','))" in rendered
    assert "WHERE trim(signal) != '' " in rendered
    assert "LIMIT 10" in rendered


def test_field_values_virtual_signals_placeholders_pinned():
    assert _placeholders(dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS) == sorted(
        [
            "requests_metric",
            "backing_col",
            "table_name",
            "where_clause",
            "search_cond",
            "limit",
        ]
    )


def test_field_values_native_column_renders_with_search():
    rendered = dashboard_sql.FIELD_VALUES_NATIVE_COLUMN.format(
        clean_field="country",
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond='AND CAST("country" AS VARCHAR) ILIKE ?',
        limit=50,
    )
    assert 'SELECT "country" AS value' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert 'WHERE 1=1 AND CAST("country" AS VARCHAR) ILIKE ?' in rendered
    assert "GROUP BY 1 ORDER BY 2 DESC LIMIT 50" in rendered


def test_field_values_native_column_renders_without_search():
    rendered = dashboard_sql.FIELD_VALUES_NATIVE_COLUMN.format(
        clean_field="asn",
        requests_metric="COUNT(*)",
        table_name='"logs_xyz"',
        where_clause="1=1",
        search_cond="",
        limit=10,
    )
    assert 'SELECT "asn" AS value' in rendered
    assert "WHERE 1=1 " in rendered
    assert "LIMIT 10" in rendered


def test_field_values_native_column_placeholders_pinned():
    assert _placeholders(dashboard_sql.FIELD_VALUES_NATIVE_COLUMN) == sorted(
        [
            "clean_field",
            "requests_metric",
            "table_name",
            "where_clause",
            "search_cond",
            "limit",
        ]
    )


def test_all_templates_exported():
    """Each template constant must appear in ``__all__`` so the renaming /
    deletion of a template surfaces as an import error in callers."""
    assert set(dashboard_sql.__all__) == {
        "VIRTUAL_FIELD_EXPLODED_TOP_N",
        "CONN_REQUESTS_BUCKET",
        "TIME_SERIES",
        "MAP_DATA_BY_COUNTRY",
        "FIELD_VALUES_BOT_UA",
        "FIELD_VALUES_VIRTUAL_SIGNALS",
        "FIELD_VALUES_NATIVE_COLUMN",
    }


# ── From test_insights.py ──


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
        "crawler_disparity": "CRAWLER_DISPARITY",
        "stale_browser_version": "STALE_BROWSER_VERSION",
        "orphaned_deep_crawl": "ORPHANED_DEEP_CRAWL",
        "residential_fingerprint_dispersion": "RESIDENTIAL_FINGERPRINT_DISPERSION",
    }

    for insight_id, const_name in expected.items():
        d = registry.get(insight_id)
        assert d is not None, f"insight {insight_id} not registered"
        # Compare by value rather than identity — pydantic v2 may copy strings
        # when validating model fields, so `is` would be flaky.
        assert d.sql_template == getattr(insights_sql, const_name), (
            f"insight {insight_id} sql_template diverged from insights_sql.{const_name}"
        )


def test_error_spikes_renders_and_pins_placeholders():
    rendered = insights_sql.ERROR_SPIKES.format(table_name="t_logs")
    assert "FROM t_logs" in rendered
    assert "CAST(? AS TIMESTAMPTZ)" in rendered
    assert "HAVING w_total >= 3" in rendered
    assert "ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.ERROR_SPIKES) == ["table_name"]


def test_botnet_grouping_renders_and_pins_placeholders():
    rendered = insights_sql.BOTNET_GROUPING.format(table_name="t_logs", fp_col="ja4", baseline_hours=24, window_hours=1)
    assert 'FROM t_logs WHERE "ja4" IS NOT NULL' in rendered
    assert "GREATEST(24, 1.0) * 1" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.BOTNET_GROUPING) == sorted(
        ["table_name", "fp_col", "baseline_hours", "window_hours"]
    )


def test_new_country_traffic_renders_and_pins_placeholders():
    rendered = insights_sql.NEW_COUNTRY_TRAFFIC.format(table_name="t_logs")
    assert 'WHERE "country" IS NOT NULL' in rendered
    assert "HAVING w_cnt >= 3 AND b_cnt = 0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.NEW_COUNTRY_TRAFFIC) == ["table_name"]


def test_city_surges_renders_and_pins_placeholders():
    rendered = insights_sql.CITY_SURGES.format(
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
    assert _placeholders(insights_sql.CITY_SURGES) == sorted(
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
    rendered = insights_sql.CITY_ERROR_SPIKES.format(
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
    assert _placeholders(insights_sql.CITY_ERROR_SPIKES) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]
    )


def test_city_latency_regressions_renders_and_pins_placeholders():
    rendered = insights_sql.CITY_LATENCY_REGRESSIONS.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
        loc_cols='"country", "region"',
    )
    assert "PERCENTILE_CONT(0.95)" in rendered
    assert "w_p95 >= b_p95 * 3.0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.CITY_LATENCY_REGRESSIONS) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]
    )


def test_new_city_traffic_renders_and_pins_placeholders():
    rendered = insights_sql.NEW_CITY_TRAFFIC.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
        loc_cols='"country", "region"',
    )
    assert "HAVING w_cnt >= 5 AND b_cnt = 0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.NEW_CITY_TRAFFIC) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]
    )


def test_ua_monoculture_renders_and_pins_placeholders():
    rendered = insights_sql.UA_MONOCULTURE.format(table_name="t_logs")
    assert 'FROM t_logs GROUP BY "ua"' in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.UA_MONOCULTURE) == ["table_name"]


def test_new_probe_urls_bakes_regex_and_pins_placeholders():
    rendered = insights_sql.NEW_PROBE_URLS.format(table_name="t_logs")
    assert "FROM t_logs" in rendered
    # Regex is f-string-baked at import time. ``re.escape('.env')`` → ``\\.env``.
    assert "regexp_matches(" in rendered
    assert "\\.env" in rendered
    assert "admin" in rendered
    assert "'i'" in rendered  # case-insensitive flag passed via SQL arg
    assert rendered.count("?") == 3
    assert _placeholders(insights_sql.NEW_PROBE_URLS) == ["table_name"]


def test_waf_signal_spikes_renders_and_pins_placeholders():
    # Reads from {waf_table} (hydrated with table_name in repository.py)
    # and groups distinct waf_sig combinations before unnesting.
    rendered = insights_sql.WAF_SIGNAL_SPIKES.format(waf_table="insights_waf_1", baseline_hours=24, window_hours=1)
    assert "FROM insights_waf_1" in rendered
    assert "GROUP BY signal" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.WAF_SIGNAL_SPIKES) == sorted(["waf_table", "baseline_hours", "window_hours"])


def test_proxy_surge_renders_and_pins_placeholders():
    rendered = insights_sql.PROXY_SURGE.format(table_name="t_logs")
    assert 'FROM t_logs WHERE "p_type" IS NOT NULL' in rendered
    assert "totals AS" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.PROXY_SURGE) == ["table_name"]


def test_asn_concentration_renders_and_pins_placeholders():
    rendered = insights_sql.ASN_CONCENTRATION.format(table_name="t_logs")
    assert 'GROUP BY "asn"' in rendered
    assert "w_cnt * 1.0 / w_total >= 0.20" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.ASN_CONCENTRATION) == ["table_name"]


def test_asn_metro_performance_renders_and_pins_placeholders():
    rendered = insights_sql.ASN_METRO_PERFORMANCE.format(table_name="t_logs")
    assert "\"country\" = 'US'" in rendered
    assert "w_med >= b_med * 1.5" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.ASN_METRO_PERFORMANCE) == ["table_name"]


def test_cache_collapse_renders_and_pins_placeholders():
    rendered = insights_sql.CACHE_COLLAPSE.format(table_name="t_logs")
    assert "starts_with(cache, 'HIT')" in rendered
    assert "b_rate >= 0.40" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.CACHE_COLLAPSE) == ["table_name"]


def test_latency_regression_renders_and_pins_placeholders():
    rendered = insights_sql.LATENCY_REGRESSION.format(table_name="t_logs")
    assert "PERCENTILE_CONT(0.95)" in rendered
    assert "w_p95 >= b_p95 * 2.0" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.LATENCY_REGRESSION) == ["table_name"]


def test_impossible_distance_renders_and_pins_placeholders():
    rendered = insights_sql.IMPOSSIBLE_DISTANCE.format(
        table_name="t_logs",
        fp_col="ja4",
        pop_values="('SJC', 37.0::DOUBLE, -121.0::DOUBLE)",
        edge_filter='AND t."edge" = true',
    )
    assert "WITH pop_coords(pop_code, pop_lat, pop_lon) AS (VALUES ('SJC'" in rendered
    assert "RADIANS" in rendered
    assert 'AND t."edge" = true' in rendered
    assert rendered.count("?") == 1
    assert _placeholders(insights_sql.IMPOSSIBLE_DISTANCE) == sorted(
        ["table_name", "fp_col", "pop_values", "edge_filter"]
    )


def test_tail_latency_renders_and_pins_placeholders():
    rendered = insights_sql.TAIL_LATENCY.format(table_name="t_logs")
    assert "PERCENTILE_CONT(0.99)" in rendered
    assert "PERCENTILE_CONT(0.50)" in rendered
    assert "ratio > 5" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(insights_sql.TAIL_LATENCY) == ["table_name"]


def test_cipher_spread_renders_and_pins_placeholders():
    rendered = insights_sql.CIPHER_SPREAD.format(table_name="t_logs", baseline_hours=24, window_hours=1)
    assert '"tls_ciphers_sha" IS NOT NULL' in rendered
    assert 'COUNT(DISTINCT "ip")' in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.CIPHER_SPREAD) == sorted(["table_name", "baseline_hours", "window_hours"])


def test_request_size_anomaly_renders_and_pins_placeholders():
    rendered = insights_sql.REQUEST_SIZE_ANOMALY.format(table_name="t_logs")
    assert "req_header_bytes > 0" in rendered
    assert "max_bytes > b_p95 * 3" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.REQUEST_SIZE_ANOMALY) == ["table_name"]


def test_connection_abuse_renders_and_pins_placeholders():
    rendered = insights_sql.CONNECTION_ABUSE.format(table_name="t_logs")
    assert "conn_requests > 0" in rendered
    assert "max_reqs > b_p95 * 3 AND max_reqs >= 50" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.CONNECTION_ABUSE) == ["table_name"]


def test_region_latency_renders_and_pins_placeholders():
    rendered = insights_sql.REGION_LATENCY.format(table_name="t_logs")
    assert "stats AS" in rendered
    assert "ottfb_p95" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.REGION_LATENCY) == ["table_name"]


def test_cache_ttl_mismatch_renders_and_pins_placeholders():
    rendered = insights_sql.CACHE_TTL_MISMATCH.format(table_name="t_logs", q_col='"url"')
    assert 'SELECT "url" AS label' in rendered
    assert 'AVG("hits") < 2 AND AVG("ttl") > 60' in rendered
    assert rendered.count("?") == 1
    assert _placeholders(insights_sql.CACHE_TTL_MISMATCH) == sorted(["table_name", "q_col"])


def test_image_optimization_opportunities_renders_and_pins_placeholders():
    rendered = insights_sql.IMAGE_OPTIMIZATION_OPPORTUNITIES.format(table_name="t_logs", ua_mobile_sel="0")
    assert 'WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ) AND t."status" = 200' in rendered
    assert "(0) AS mobile_ratio" in rendered
    assert "%.jpg%" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(insights_sql.IMAGE_OPTIMIZATION_OPPORTUNITIES) == sorted(["table_name", "ua_mobile_sel"])


def test_origin_latency_spike_renders_and_pins_placeholders():
    rendered = insights_sql.ORIGIN_LATENCY_SPIKE.format(table_name="t_logs", url_col='"url"')
    assert "overall_stats AS" in rendered
    assert "url_stats AS" in rendered
    assert "o.w_p95 > o.b_p95 * 2" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.ORIGIN_LATENCY_SPIKE) == sorted(["table_name", "url_col"])


def test_origin_error_rate_renders_and_pins_placeholders():
    rendered = insights_sql.ORIGIN_ERROR_RATE.format(table_name="t_logs")
    assert '"ost" AS status' in rendered
    assert "status >= 500" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.ORIGIN_ERROR_RATE) == ["table_name"]


def test_origin_retries_renders_and_pins_placeholders():
    rendered = insights_sql.ORIGIN_RETRIES.format(table_name="t_logs", url_col='"url"')
    assert 'AVG("oretries")' in rendered
    assert "requests >= 5" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(insights_sql.ORIGIN_RETRIES) == sorted(["table_name", "url_col"])


def test_origin_ip_failure_renders_and_pins_placeholders():
    rendered = insights_sql.ORIGIN_IP_FAILURE.format(table_name="t_logs")
    assert '"oip" IS NOT NULL' in rendered
    assert "median_calc AS" in rendered
    assert rendered.count("?") == 1
    assert _placeholders(insights_sql.ORIGIN_IP_FAILURE) == ["table_name"]


def test_shield_path_degradation_renders_and_pins_placeholders():
    rendered = insights_sql.SHIELD_PATH_DEGRADATION.format(table_name="t_logs")
    assert "edge_logs AS" in rendered
    assert "shield_logs AS" in rendered
    assert "'Direct to Origin'" in rendered
    assert rendered.count("?") == 3
    assert _placeholders(insights_sql.SHIELD_PATH_DEGRADATION) == ["table_name"]


def test_coalesced_city_aggregates_renders_and_pins_placeholders():
    rendered = insights_sql.COALESCED_CITY_AGGREGATES.format(
        table_name="t_logs",
        label_expr="'l'",
        region_sel='"region"',
        country_sel='"country"',
    )
    assert "FROM t_logs" in rendered
    assert 'WHERE "city" IS NOT NULL' in rendered
    assert 'GROUP BY "city", region, country' in rendered
    assert "w_lat_total" in rendered
    assert "b_lat_total" in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.COALESCED_CITY_AGGREGATES) == sorted(
        ["table_name", "label_expr", "region_sel", "country_sel"]
    )


def test_coalesced_url_aggregates_renders_and_pins_placeholders():
    rendered = insights_sql.COALESCED_URL_AGGREGATES.format(table_name="t_logs")
    assert "FROM t_logs" in rendered
    assert 'WHERE "url" IS NOT NULL' in rendered
    assert "w_5xx" in rendered
    assert "w_hits" in rendered
    assert "w_p99" in rendered
    assert "w_p50" in rendered
    assert 'GROUP BY "url"' in rendered
    assert rendered.count("?") == 2
    assert _placeholders(insights_sql.COALESCED_URL_AGGREGATES) == ["table_name"]


def test_coalesced_asn_and_scalar_aggregates_render_and_pin_placeholders():
    asn_rendered = insights_sql.COALESCED_ASN_AGGREGATES.format(
        table_name="t_logs",
        ploss_sel='"ploss"',
        rtt_var_sel='"rtt_var"',
        retrans_sel='"retrans"',
        p_type_sel='"p_type"',
    )
    assert "FROM t_logs" in asn_rendered
    assert "GROUP BY asn" in asn_rendered
    assert asn_rendered.count("?") == 2
    assert _placeholders(insights_sql.COALESCED_ASN_AGGREGATES) == sorted(
        ["table_name", "ploss_sel", "rtt_var_sel", "retrans_sel", "p_type_sel"]
    )


def test_new_probe_regex_contains_all_probes_escaped():
    """Each entry in NEW_PROBES must appear in the regex, properly escaped."""
    import re

    for probe in insights_sql.NEW_PROBES:
        assert re.escape(probe) in insights_sql.NEW_PROBE_REGEX, f"probe {probe!r} missing from regex"


def test_crawler_disparity_renders():
    rendered = insights_sql.CRAWLER_DISPARITY.format(table_name="t_logs")
    assert "crawler_traffic" in rendered
    assert "w_bytes" in rendered
    assert "bytes_per_referral" in rendered
    assert "bandwidth_share_pct" in rendered
    assert rendered.count("?") == 4
    assert _placeholders(insights_sql.CRAWLER_DISPARITY) == ["table_name"]


def test_no_hardcoded_bot_brands_in_disparity_or_efficiency_sql():
    from backend.repositories._sql import security as sec_sql

    forbidden = [
        "anthropic",
        "claudebot",
        "gptbot",
        "openai",
        "perplexity",
        "googlebot",
        "bingbot",
        "amazonbot",
        "bytespider",
        "semrush",
        "ahrefs",
        "ccbot",
        "petalbot",
    ]
    for name, template in [
        ("CRAWLER_DISPARITY", insights_sql.CRAWLER_DISPARITY),
        ("BOT_EFFICIENCY", sec_sql.BOT_EFFICIENCY),
    ]:
        lower_sql = template.lower()
        for brand in forbidden:
            assert brand not in lower_sql, f"{name} must not hardcode bot/vendor name {brand!r}"


def test_stale_browser_version_renders():
    rendered = insights_sql.STALE_BROWSER_VERSION.format(table_name="t_logs", baseline_hours=24, window_hours=1)
    assert "version_aggregates" in rendered
    assert rendered.count("?") == 2


def test_orphaned_deep_crawl_renders():
    rendered = insights_sql.ORPHANED_DEEP_CRAWL.format(table_name="t_logs", baseline_hours=24, window_hours=1)
    assert "deep_requests" in rendered
    assert rendered.count("?") == 2


def test_residential_fingerprint_renders():
    rendered = insights_sql.RESIDENTIAL_FINGERPRINT_DISPERSION.format(table_name="t_logs")
    assert "fingerprint_asn_maps" in rendered
    assert rendered.count("?") == 2


# ── From test_network.py ──


def test_heatmap_by_asn_bucket_renders_with_all_inputs():
    rendered = network_sql.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=300000,
        rtt_min_expr="MEDIAN(rtt_min)",
        congestion_expr="MEDIAN(COALESCE(tcp_rtt, 0) - COALESCE(rtt_min, 0))",
        ploss_expr="AVG(ploss)",
        rtt_var_expr="MEDIAN(rtt_var)",
        table='"_tmp_xyz"',
        where="1=1",
        row_limit=6000,
    )
    assert "EPOCH_MS" in rendered
    assert "APPROX_QUANTILE(tcp_rtt, 0.5)" in rendered
    assert "MEDIAN(rtt_min)" in rendered
    assert "AVG(ploss)" in rendered
    assert "MEDIAN(rtt_var)" in rendered
    assert 'FROM "_tmp_xyz"' in rendered
    assert "WHERE 1=1" in rendered
    # 2-pass CTE: top_cells groups bare (asn, bucket); the outer query
    # joins back and groups by the top_cells alias columns.
    assert "GROUP BY tc.asn, tc.bucket, tc.reqs, tc.err_count" in rendered
    assert "LIMIT 6000" in rendered
    # The bucket-ms value appears in one EPOCH_MS arithmetic block, which
    # uses the value twice (numerator + multiplier) — once in
    # the CTE. Total: 2 occurrences.
    assert rendered.count("300000") == 2


def test_heatmap_by_asn_bucket_renders_with_null_column_exprs():
    """When schema columns are absent, callers pass ``"NULL"`` — verify."""
    rendered = network_sql.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=60000,
        rtt_min_expr="NULL",
        congestion_expr="NULL",
        ploss_expr="NULL",
        rtt_var_expr="NULL",
        table='"_tmp_x"',
        where="1=1",
        row_limit=200,
    )
    assert "NULL           AS rtt_baseline_us" in rendered
    assert "NULL             AS avg_ploss" in rendered


def test_heatmap_by_asn_bucket_placeholders_pinned():
    # 2-pass CTE shape: precomputed CTE limits work, then the outer SELECT
    # joins back to the precomputed CTE. ``{bucket_ms}`` appears 2x
    # (one EPOCH_MS block, using numerator + multiplier),
    # ``{table}`` 4x (precomputed FROM, outer FROM as {table}, JOIN ON's two refs), and
    # ``{where}`` 2x (precomputed + top_cells).
    assert _placeholders_all(network_sql.HEATMAP_BY_ASN_BUCKET) == sorted(
        [
            "bucket_ms",
            "bucket_ms",
            "rtt_min_expr",
            "congestion_expr",
            "ploss_expr",
            "rtt_var_expr",
            "table",
            "table",
            "table",
            "table",
            "where",
            "where",
            "row_limit",
        ]
    )


def test_map_by_country_bucket_renders_with_all_inputs():
    rendered = network_sql.MAP_BY_COUNTRY_BUCKET.format(
        city_col="city",
        lat_col="lat",
        lon_col="lon",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_lat_col='"_tmp_x".lat',
        join_lon_col='"_tmp_x".lon',
        join_metro_col='"_tmp_x".metro',
        bucket_ms=300000,
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1",
    )
    assert 'APPROX_QUANTILE("_tmp_x".tcp_rtt, 0.5)' in rendered
    assert "AVG(ploss)" in rendered
    assert 'FROM "_tmp_x"' in rendered
    # 2-pass CTE: outer SELECT joins back to top_cells and groups by
    # its tc.* aliases (plus the carried-through tc.reqs / tc.err_count
    # so the divisions in error_pct don't break aggregation rules).
    assert "GROUP BY tc.country, tc.city, tc.lat, tc.lon, tc.metro, tc.bucket, tc.reqs, tc.err_count" in rendered
    assert "LIMIT 5000" in rendered
    assert "ORDER BY bucket, reqs DESC" in rendered
    # bucket_ms appears in one EPOCH_MS block (precomputed CTE),
    # using numerator + multiplier → 2 occurrences total post-optimization.
    assert rendered.count("300000") == 2


def test_map_by_country_bucket_renders_with_extended_where_for_map_asn():
    """Callers append ``" AND asn = ?"`` when ``map_asn`` is specified;
    the template's WHERE substitution must accept that shape."""
    rendered = network_sql.MAP_BY_COUNTRY_BUCKET.format(
        city_col="city",
        lat_col="lat",
        lon_col="lon",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_lat_col='"_tmp_x".lat',
        join_lon_col='"_tmp_x".lon',
        join_metro_col='"_tmp_x".metro',
        bucket_ms=60000,
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1 AND asn = ?",
    )
    assert "WHERE 1=1 AND asn = ?" in rendered


def test_map_by_country_bucket_placeholders_pinned():
    # 2-pass CTE shape: precomputed CTE projects bare column refs (city_col,
    # lat_col, lon_col, metro_col); the outer JOIN ON references the
    # same logical columns but qualified with the temp-table name to
    # disambiguate from top_cells' aliases — those are join_* siblings.
    # ``{bucket_ms}`` 2x (one EPOCH_MS block × 2 uses), ``{table}``
    # 6x (precomputed FROM, outer FROM as {table}, JOIN ON and WHERE refs), ``{where}`` 2x.
    assert _placeholders_all(network_sql.MAP_BY_COUNTRY_BUCKET) == sorted(
        [
            "city_col",
            "lat_col",
            "lon_col",
            "metro_col",
            "join_city_col",
            "join_lat_col",
            "join_lon_col",
            "join_metro_col",
            "bucket_ms",
            "bucket_ms",
            "ploss_expr",
            "table",
            "table",
            "table",
            "table",
            "table",
            "table",
            "table",
            "where",
            "where",
        ]
    )


def test_metro_leaderboard_renders_with_all_inputs():
    rendered = network_sql.METRO_LEADERBOARD.format(
        city_col="city",
        region_col="region",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_region_col='"_tmp_x".region',
        join_metro_col='"_tmp_x".metro',
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1",
    )
    assert "APPROX_QUANTILE(tcp_rtt, 0.5)" in rendered
    assert "AVG(ploss)" in rendered
    assert 'FROM "_tmp_x"' in rendered
    # 2-pass CTE: outer SELECT groups by tc.* (top_cells aliases) plus
    # the carried-through tc.reqs / tc.err_count.
    assert "GROUP BY tc.country, tc.city, tc.region, tc.metro, tc.reqs, tc.err_count" in rendered
    assert "LIMIT 100" in rendered


def test_metro_leaderboard_placeholders_pinned():
    # Same shape as MAP_BY_COUNTRY_BUCKET above: bare *_col in the CTE
    # projection, join_*_col in the JOIN ON. No bucket_ms here (no time
    # bucketing — top-100 by total reqs). ``{table}`` 3x, ``{where}`` 2x.
    assert _placeholders_all(network_sql.METRO_LEADERBOARD) == sorted(
        [
            "city_col",
            "region_col",
            "metro_col",
            "join_city_col",
            "join_region_col",
            "join_metro_col",
            "ploss_expr",
            "table",
            "table",
            "table",
            "where",
            "where",
        ]
    )


def test_speed_distribution_by_asn_renders_with_placeholders():
    rendered = network_sql.SPEED_DISTRIBUTION_BY_ASN.format(
        table='"_tmp_x"',
        where="1=1",
        placeholders="?,?,?",
    )
    assert "SELECT asn, c_speed, COUNT(*)" in rendered
    assert "asn IN (?,?,?)" in rendered
    assert 'FROM "_tmp_x"' in rendered
    assert "GROUP BY asn, c_speed" in rendered


def test_speed_distribution_by_asn_placeholders_pinned():
    assert _placeholders_all(network_sql.SPEED_DISTRIBUTION_BY_ASN) == sorted(
        [
            "table",
            "where",
            "placeholders",
        ]
    )


def test_rtt_percentiles_by_asn_renders_with_placeholders():
    rendered = network_sql.RTT_PERCENTILES_BY_ASN.format(
        table='"_tmp_x"',
        where="1=1",
        placeholders="?,?",
    )
    assert "APPROX_QUANTILE(tcp_rtt, 0.95)" in rendered
    assert "APPROX_QUANTILE(tcp_rtt, 0.99)" in rendered
    assert "asn IN (?,?)" in rendered
    assert "GROUP BY asn" in rendered


def test_rtt_percentiles_by_asn_placeholders_pinned():
    assert _placeholders_all(network_sql.RTT_PERCENTILES_BY_ASN) == sorted(
        [
            "table",
            "where",
            "placeholders",
        ]
    )


def test_quality_bar_by_group_renders_without_extra_where():
    rendered = network_sql.QUALITY_BAR_BY_GROUP.format(
        group_col="country",
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where="",
    )
    assert '"country" AS label' in rendered
    assert "APPROX_QUANTILE(tcp_rtt, 0.5) / 1000.0 AS rtt_ms" in rendered
    assert 'GROUP BY "country"' in rendered
    assert "LIMIT 25" in rendered
    assert "ORDER BY reqs DESC" in rendered


def test_quality_bar_by_group_renders_with_extra_where():
    """The region rollup appends ``" AND country = ?"`` and binds a value."""
    rendered = network_sql.QUALITY_BAR_BY_GROUP.format(
        group_col="region",
        table='"logs_xyz"',
        rtt_filter="tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where=" AND country = ?",
    )
    assert "WHERE tcp_rtt IS NOT NULL AND tcp_rtt > 0 AND country = ?" in rendered
    assert '"region" AS label' in rendered


def test_quality_bar_by_group_placeholders_pinned():
    assert _placeholders_all(network_sql.QUALITY_BAR_BY_GROUP) == sorted(
        [
            "group_col",
            "group_col",
            "group_col",
            "group_col",
            "table",
            "rtt_filter",
            "extra_where",
        ]
    )


def test_quality_countries_distinct_renders_with_all_inputs():
    rendered = network_sql.QUALITY_COUNTRIES_DISTINCT.format(
        table='"logs_xyz"',
        where_clause="ts BETWEEN '2026-01-01' AND '2026-01-02'",
    )
    assert "SELECT DISTINCT country" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "country IS NOT NULL AND country != ''" in rendered
    assert "ORDER BY country" in rendered


def test_quality_countries_distinct_placeholders_pinned():
    assert _placeholders_all(network_sql.QUALITY_COUNTRIES_DISTINCT) == sorted(
        [
            "table",
            "where_clause",
        ]
    )


def test_quality_scatter_renders_with_all_inputs():
    rendered = network_sql.QUALITY_SCATTER.format(
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
    )
    assert "tcp_rtt / 1000.0 AS rtt_ms" in rendered
    assert "ttfb * 1000.0 AS ttfb_ms" in rendered
    assert "COALESCE(cache, 'UNKNOWN') AS cache_state" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "USING SAMPLE 2000" in rendered
    assert "ttfb IS NOT NULL AND ttfb > 0" in rendered


def test_quality_scatter_placeholders_pinned():
    assert _placeholders_all(network_sql.QUALITY_SCATTER) == sorted(
        [
            "table",
            "rtt_filter",
        ]
    )


# ── From test_origin.py ──


def test_summary_rollup_renders():
    """Pinned shape of SUMMARY_ROLLUP after the GROUPING SETS dual-pass
    was retired (the per-edge ``by_leg`` rows were never read by the
    frontend; ``split_by_leg: false`` is hard-coded on /origin). The
    template now lays down a single ``GROUP BY``-less aggregate.
    """
    rendered = origin_sql.SUMMARY_ROLLUP.format(
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)',
        ottlb_p50='MEDIAN("ottlb") / 1000.0',
        ottlb_p95='APPROX_QUANTILE("ottlb", 0.95) / 1000.0',
        cdn_ovh='MEDIAN("elapsed" - "ottlb") / 1000.0',
        ost_5xx='COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)',
        obytes_p50='MEDIAN("obytes")',
        total_misses="1",
        total_passes="1",
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'FROM "logs_xyz"' in rendered
    assert 'MEDIAN(COALESCE("ottfb", "ttfb" * 1000000.0)) / 1000.0' in rendered
    assert "AS ottfb_p99_ms" in rendered
    # The dual-pass GROUPING SETS path was deleted — make sure nobody
    # silently brought it back.
    assert "GROUPING SETS" not in rendered
    assert 'GROUPING("edge")' not in rendered


def test_summary_rollup_placeholders_pinned():
    assert _placeholders(origin_sql.SUMMARY_ROLLUP) == sorted(
        [
            "lat_val",
            "ottlb_p50",
            "ottlb_p95",
            "cdn_ovh",
            "ost_5xx",
            "obytes_p50",
            "total_misses",
            "total_passes",
            "table",
            "where",
        ]
    )


def test_timeseries_bucketed_renders():
    rendered = origin_sql.TIMESERIES_BUCKETED.format(
        interval="INTERVAL '5' minutes",
        agg_expr='APPROX_QUANTILE("ottfb", 0.95)',
        unit_conv="/ 1000.0",
        edge_col=', "edge"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        lat_expr='"ottfb"',
        edge_group=', "edge"',
    )
    assert "time_bucket(INTERVAL '5' minutes, \"timestamp\")" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert 'APPROX_QUANTILE("ottfb", 0.95) / 1000.0' in rendered
    assert "GROUP BY ts" in rendered
    assert "ORDER BY ts" in rendered


def test_timeseries_bucketed_placeholders_pinned():
    assert _placeholders(origin_sql.TIMESERIES_BUCKETED) == sorted(
        [
            "interval",
            "agg_expr",
            "unit_conv",
            "edge_col",
            "table",
            "where",
            "lat_expr",
            "edge_group",
        ]
    )


def test_slow_urls_renders():
    rendered = origin_sql.SLOW_URLS.format(
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'SELECT\n          "url"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "HAVING COUNT(*) >= ?" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_slow_urls_placeholders_pinned():
    assert _placeholders(origin_sql.SLOW_URLS) == sorted(["lat_val", "table", "where"])


def test_status_codes_renders():
    rendered = origin_sql.STATUS_CODES.format(
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert '"ost"' in rendered
    assert "OVER ()" in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "ORDER BY count DESC" in rendered


def test_status_codes_placeholders_pinned():
    assert _placeholders(origin_sql.STATUS_CODES) == sorted(["table", "where"])


def test_path_breakdown_renders():
    rendered = origin_sql.PATH_BREAKDOWN.format(
        lat_val='"ottfb"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'GROUP BY "edge"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert 'MEDIAN("ottfb")' in rendered


def test_path_breakdown_placeholders_pinned():
    assert _placeholders(origin_sql.PATH_BREAKDOWN) == sorted(["lat_val", "table", "where"])


def test_pop_latency_renders():
    rendered = origin_sql.POP_LATENCY.format(
        lat_val='"ottfb"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'GROUP BY "pop"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "ORDER BY p95_ms DESC" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_pop_latency_placeholders_pinned():
    assert _placeholders(origin_sql.POP_LATENCY) == sorted(["lat_val", "table", "where"])


def test_ip_health_renders():
    rendered = origin_sql.IP_HEALTH.format(
        lat_val='"ottfb"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'GROUP BY "oip"' in rendered
    assert 'FROM "logs_xyz"' in rendered
    assert "HAVING COUNT(*) >= 10" in rendered
    assert "ORDER BY error_pct DESC" in rendered
    assert rendered.rstrip().endswith("LIMIT ?")


def test_ip_health_placeholders_pinned():
    assert _placeholders(origin_sql.IP_HEALTH) == sorted(["lat_val", "table", "where"])


def test_shielding_analysis_renders():
    rendered = origin_sql.SHIELDING_ANALYSIS.format(
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        time_where="timestamp BETWEEN ? AND ?",
    )
    assert "WITH edge_logs AS" in rendered
    assert "shield_logs AS" in rendered
    assert "INNER JOIN shield_logs s ON s.prid = e.rid" in rendered
    assert "PERCENTILE_CONT(0.50)" in rendered
    # M1 (shielding audit 2026-06-30): the flat ``LIMIT ?`` was replaced by a
    # ranked CTE that keeps BOTH the top-by-requests and the top-by-overhead
    # routes (so low-volume anomalies aren't buried) and exposes the full
    # distinct-pair count for "Top N of M" truncation reporting.
    assert "COUNT(*) OVER () " in rendered  # total_routes
    assert "ROW_NUMBER() OVER (ORDER BY requests DESC)" in rendered
    assert "ROW_NUMBER() OVER (ORDER BY p50_ms DESC NULLS LAST)" in rendered
    assert "WHERE rn_requests <= ? OR rn_overhead <= ?" in rendered
    assert "AS total_routes" in rendered
    assert rendered.rstrip().endswith("ORDER BY requests DESC")
    assert "LIMIT ?" not in rendered


def test_shielding_analysis_placeholders_pinned():
    assert _placeholders(origin_sql.SHIELDING_ANALYSIS) == sorted(["table", "where", "time_where"])


def test_aggregates_create_temp_renders():
    rendered = origin_sql.AGGREGATES_CREATE_TEMP.format(
        temp_table="t_origin_deadbeef",
        select_cols='"timestamp", "cache", "edge"',
        lat_us_expr='COALESCE("ottfb", "ttfb" * 1000000.0)',
        table='"logs_xyz"',
        where_clause="timestamp BETWEEN '2026-06-09T00:00:00Z' AND '2026-06-09T01:00:00Z'",
    )
    # Catalog table (not TEMP) so the parallel-read pool connections all
    # see the materialization. Cleanup hygiene: try/finally DROP in
    # get_aggregates + pool catalog sweep in duckdb_pool._Pool.release.
    assert rendered.startswith("CREATE TABLE t_origin_deadbeef AS ")
    assert "CREATE TEMP TABLE" not in rendered
    assert 'SELECT "timestamp", "cache", "edge",' in rendered
    assert 'COALESCE("ottfb", "ttfb" * 1000000.0) AS lat_us' in rendered
    assert 'FROM "logs_xyz" WHERE' in rendered


def test_aggregates_create_temp_placeholders_pinned():
    assert _placeholders(origin_sql.AGGREGATES_CREATE_TEMP) == sorted(
        [
            "temp_table",
            "select_cols",
            "lat_us_expr",
            "table",
            "where_clause",
        ]
    )


# ── From test_query.py ──


def test_explain_wrapper_renders_with_user_sql():
    rendered = query_sql.EXPLAIN_WRAPPER.format(sql="SELECT 1")
    assert rendered == "EXPLAIN SELECT 1"


def test_explain_wrapper_pins_placeholders():
    assert _placeholders(query_sql.EXPLAIN_WRAPPER) == ["sql"]


def test_auto_limit_wrapper_renders_with_inner_and_limit():
    rendered = query_sql.AUTO_LIMIT_WRAPPER.format(
        inner="SELECT * FROM logs_svc ORDER BY id",
        limit=1001,
    )
    assert "SELECT * FROM (SELECT * FROM logs_svc ORDER BY id) AS _q" in rendered
    assert "LIMIT 1001" in rendered


def test_auto_limit_wrapper_uses_underscore_q_alias():
    """The ``AS _q`` alias is load-bearing — the wrapper relies on it for
    DuckDB to plan the outer LIMIT against the inner SELECT's top-k."""
    rendered = query_sql.AUTO_LIMIT_WRAPPER.format(inner="SELECT 1", limit=10)
    assert "AS _q" in rendered


def test_auto_limit_wrapper_pins_placeholders():
    assert _placeholders(query_sql.AUTO_LIMIT_WRAPPER) == ["inner", "limit"]


def test_preset_sample_rows_renders_with_table():
    rendered = query_sql.PRESET_SAMPLE_ROWS.format(table="logs_myservice")
    assert rendered == "SELECT * FROM logs_myservice LIMIT 100"


def test_preset_sample_rows_has_no_order_by():
    """Regression pin — the preset must NOT force a sort on the
    full table (would make the preview feel broken on 1.6M-row tables)."""
    rendered = query_sql.PRESET_SAMPLE_ROWS.format(table="logs_myservice")
    assert "ORDER BY" not in rendered.upper()


def test_preset_sample_rows_pins_placeholders():
    assert _placeholders(query_sql.PRESET_SAMPLE_ROWS) == ["table"]


def test_preset_row_count_renders_with_table():
    rendered = query_sql.PRESET_ROW_COUNT.format(table="logs_myservice")
    assert rendered == "SELECT count(*) AS total_rows FROM logs_myservice"


def test_preset_row_count_pins_placeholders():
    assert _placeholders(query_sql.PRESET_ROW_COUNT) == ["table"]


def test_preset_column_stats_renders_with_table():
    rendered = query_sql.PRESET_COLUMN_STATS.format(table="logs_myservice")
    assert rendered == "SUMMARIZE logs_myservice"


def test_preset_column_stats_pins_placeholders():
    assert _placeholders(query_sql.PRESET_COLUMN_STATS) == ["table"]


# ── From test_security.py ──


def test_top_uas_by_count_renders_with_temp_table():
    rendered = security_sql.TOP_UAS_BY_COUNT.format(temp_table="t_filtered_xyz", limit=2000)
    assert "SELECT ua, count(*) AS cnt" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE ua IS NOT NULL" in rendered
    assert "GROUP BY ua" in rendered
    assert "ORDER BY cnt DESC" in rendered
    assert "LIMIT 2000" in rendered


def test_top_uas_by_count_pins_placeholders():
    assert _placeholders(security_sql.TOP_UAS_BY_COUNT) == ["limit", "temp_table"]


def test_ngwaf_top_bots_join_renders_with_temp_table_and_n():
    rendered = security_sql.NGWAF_TOP_BOTS_JOIN.format(temp_table="t_filtered_xyz", n=15)
    assert "SELECT nb.bot_name, nb.category, count(*) AS cnt" in rendered
    assert "FROM t_filtered_xyz t" in rendered
    assert "INNER JOIN temp.ngwaf_top_ngwaf_bots nb USING (waf_req_id)" in rendered
    assert "WHERE nb.bot_name IS NOT NULL" in rendered
    assert "LIMIT 15" in rendered


def test_ngwaf_top_bots_join_pins_placeholders():
    assert _placeholders(security_sql.NGWAF_TOP_BOTS_JOIN) == ["n", "temp_table"]


def test_verified_bots_ts_renders_with_bucket_and_temp_table():
    rendered = security_sql.VERIFIED_BOTS_TS.format(bucket_seconds=300, temp_table="t_filtered_xyz")
    assert "time_bucket(INTERVAL '300 seconds', timestamp)" in rendered
    assert "replace(tag, 'VERIFIED-BOT.', '')" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'" in rendered
    assert "GROUP BY 1, 2" in rendered
    assert "ORDER BY 1, 2" in rendered


def test_verified_bots_ts_pins_placeholders():
    assert _placeholders(security_sql.VERIFIED_BOTS_TS) == ["bucket_seconds", "temp_table"]


def test_ngwaf_verified_bots_renders_with_temp_table():
    rendered = security_sql.NGWAF_VERIFIED_BOTS.format(temp_table="t_filtered_xyz")
    assert "nb.bot_name" in rendered
    assert "nb.wellknown_bot_name" in rendered
    assert "nb.category" in rendered
    assert "count(*) AS request_count" in rendered
    assert "FROM t_filtered_xyz t" in rendered
    assert "INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id)" in rendered
    assert "GROUP BY 1, 2, 3" in rendered
    assert "ORDER BY 4 DESC" in rendered


def test_ngwaf_verified_bots_pins_placeholders():
    assert _placeholders(security_sql.NGWAF_VERIFIED_BOTS) == ["temp_table"]


def test_ngwaf_verified_bots_ts_renders_with_bucket_and_temp_table():
    rendered = security_sql.NGWAF_VERIFIED_BOTS_TS.format(bucket_seconds=60, temp_table="t_filtered_xyz")
    assert "time_bucket(INTERVAL '60 seconds', t.timestamp)" in rendered
    assert "FROM t_filtered_xyz t" in rendered
    assert "INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id)" in rendered
    assert "WHERE nb.bot_name IS NOT NULL" in rendered
    assert "GROUP BY 1, 2" in rendered


def test_ngwaf_verified_bots_ts_pins_placeholders():
    assert _placeholders(security_sql.NGWAF_VERIFIED_BOTS_TS) == ["bucket_seconds", "temp_table"]


def test_fingerprint_top_n_renders_for_tls_column():
    rendered = security_sql.FINGERPRINT_TOP_N.format(temp_table="t_filtered_xyz", col="tls_ciphers_sha")
    assert 'SELECT "tls_ciphers_sha"' in rendered
    assert "count(DISTINCT ip) as ip_count" in rendered
    assert "count(*) as req_count" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert 'WHERE "tls_ciphers_sha" IS NOT NULL' in rendered
    assert "GROUP BY 1 ORDER BY 3 DESC LIMIT 20" in rendered


def test_fingerprint_top_n_pins_placeholders():
    assert _placeholders(security_sql.FINGERPRINT_TOP_N) == ["col", "temp_table"]


def test_req_header_size_dist_renders_with_temp_table():
    rendered = security_sql.REQ_HEADER_SIZE_DIST.format(temp_table="t_filtered_xyz")
    assert "WHEN req_header_bytes <= 256 THEN '0-256B'" in rendered
    assert "WHEN req_header_bytes <= 32768 THEN '24-32KB'" in rendered
    assert "ELSE '>32KB'" in rendered
    assert "MIN(req_header_bytes) as min_val" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE req_header_bytes IS NOT NULL" in rendered
    assert "GROUP BY 1 ORDER BY min_val" in rendered


def test_req_header_size_dist_pins_placeholders():
    assert _placeholders(security_sql.REQ_HEADER_SIZE_DIST) == ["temp_table"]


def test_top_ips_by_max_header_renders_with_temp_table():
    rendered = security_sql.TOP_IPS_BY_MAX_HEADER.format(temp_table="t_filtered_xyz")
    assert "SELECT ip, MAX(req_header_bytes) as max_header" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE ip IS NOT NULL AND req_header_bytes IS NOT NULL" in rendered
    assert "GROUP BY 1 ORDER BY 2 DESC LIMIT 10" in rendered


def test_top_ips_by_max_header_pins_placeholders():
    assert _placeholders(security_sql.TOP_IPS_BY_MAX_HEADER) == ["temp_table"]


def test_ipv6_adoption_ts_renders_with_time_bucket_and_temp_table():
    rendered = security_sql.IPV6_ADOPTION_TS.format(
        time_bucket_select="time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
        temp_table="t_filtered_xyz",
    )
    assert "time_bucket(INTERVAL '1 hour', timestamp) AS bucket" in rendered
    assert "SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) * 100.0 / count(*) as ipv6_pct" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "GROUP BY 1 ORDER BY 1" in rendered


def test_ipv6_adoption_ts_pins_placeholders():
    assert _placeholders(security_sql.IPV6_ADOPTION_TS) == ["temp_table", "time_bucket_select"]


def test_proxy_type_dist_renders_with_temp_table():
    rendered = security_sql.PROXY_TYPE_DIST.format(temp_table="t_filtered_xyz")
    assert "SELECT p_type, count(*) as count" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE p_type IS NOT NULL AND p_type != ''" in rendered
    assert "GROUP BY 1 ORDER BY 2 DESC" in rendered


def test_proxy_type_dist_pins_placeholders():
    assert _placeholders(security_sql.PROXY_TYPE_DIST) == ["temp_table"]


def test_conn_reuse_dist_renders_with_temp_table():
    rendered = security_sql.CONN_REUSE_DIST.format(temp_table="t_filtered_xyz")
    assert "WHEN conn_requests = 1 THEN '1 (None)'" in rendered
    assert "WHEN conn_requests <= 5 THEN '2-5'" in rendered
    assert "WHEN conn_requests <= 100 THEN '21-100'" in rendered
    assert "ELSE '>100'" in rendered
    assert "MIN(conn_requests) as min_val" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE conn_requests IS NOT NULL AND conn_requests > 0" in rendered
    assert "GROUP BY 1 ORDER BY min_val" in rendered


def test_conn_reuse_dist_pins_placeholders():
    assert _placeholders(security_sql.CONN_REUSE_DIST) == ["temp_table"]


def test_wellknown_bots_ua_ip_renders_with_minimal_prefilter():
    rendered = security_sql.WELLKNOWN_BOTS_UA_IP.format(
        temp_table="t_filtered_xyz",
        prefilter="WHERE ua IS NOT NULL AND ip IS NOT NULL",
    )
    assert "SELECT ua, ip, count(*) AS cnt" in rendered
    assert "FROM t_filtered_xyz" in rendered
    assert "WHERE ua IS NOT NULL AND ip IS NOT NULL" in rendered
    assert "GROUP BY ua, ip" in rendered
    assert "ORDER BY cnt DESC" in rendered
    assert "LIMIT 10000" in rendered


def test_wellknown_bots_ua_ip_renders_with_regex_prefilter():
    prefilter = "WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '(googlebot|bingbot)')"
    rendered = security_sql.WELLKNOWN_BOTS_UA_IP.format(
        temp_table="t_filtered_xyz",
        prefilter=prefilter,
    )
    assert "regexp_matches(ua, '(googlebot|bingbot)')" in rendered
    assert "FROM t_filtered_xyz" in rendered


def test_wellknown_bots_ua_ip_pins_placeholders():
    assert _placeholders(security_sql.WELLKNOWN_BOTS_UA_IP) == ["prefilter", "temp_table"]


# ── From test_sessions.py ──


def test_sessions_cte_pipeline_renders_with_all_inputs():
    rendered = sessions_sql.SESSIONS_CTE_PIPELINE.format(
        group_key='"ip", "ja4"',
        ua_proj=', "ua"',
        status_proj=', "status"',
        resp_bytes_proj=', "resp_bytes"',
        rtt_proj=', "tcp_rtt"',
        asn_proj=', "asn"',
        country_proj=', "country"',
        url_proj=', "url"',
        edge_proj=', "edge"',
        edge_sid_proj=', "edge_sid"',
        cmcd_sid_proj=', "cmcd_sid"',
        table_name='"logs_xyz"',
        where_clause="timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)",
        part_key='"ip", "ja4"',
        extra_aggs=', SUM("resp_bytes") AS total_bytes, MAX("edge_sid") AS edge_sid',
    )
    # Five CTE stages must be present.
    assert "WITH base AS" in rendered
    assert "gaps AS" in rendered
    assert "marks AS" in rendered
    assert "sessions_raw AS" in rendered
    assert "sessions_agg AS" in rendered
    # Window-function shape pinned.
    assert "LAG(ts) OVER (PARTITION BY" in rendered
    assert "INTERVAL 30 MINUTES" in rendered
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in rendered
    # User-input fields are bound through ? params, NOT formatted in.
    assert "CAST(? AS TIMESTAMPTZ)" in rendered
    # Optional projections + aggregates substituted.
    assert '"ja4"' in rendered
    assert '"ua"' in rendered
    assert '"edge_sid"' in rendered
    assert '"cmcd_sid"' in rendered
    assert 'SUM("resp_bytes") AS total_bytes' in rendered
    assert 'MAX("edge_sid") AS edge_sid' in rendered
    # Table identifier substituted.
    assert 'FROM "logs_xyz"' in rendered


def test_sessions_cte_pipeline_renders_with_empty_optional_projections():
    """When optional columns are absent, projections collapse to empty
    strings — the CTE must still be valid SQL shape."""
    rendered = sessions_sql.SESSIONS_CTE_PIPELINE.format(
        group_key='"ip"',
        ua_proj="",
        status_proj="",
        resp_bytes_proj="",
        rtt_proj="",
        asn_proj="",
        country_proj="",
        url_proj="",
        edge_proj="",
        edge_sid_proj="",
        cmcd_sid_proj="",
        table_name='"logs_xyz"',
        where_clause="1=1",
        part_key='"ip"',
        extra_aggs="",
    )
    assert "WITH base AS" in rendered
    assert 'PARTITION BY "ip"' in rendered
    # No ja4 / ua / status / edge_sid leakage when those columns are absent.
    assert '"ja4"' not in rendered
    assert '"ua"' not in rendered
    assert '"status"' not in rendered
    assert '"edge_sid"' not in rendered


def test_sessions_cte_pipeline_pins_all_expected_placeholders():
    """Pin the set of substitution names so accidental new placeholders
    in the template raise an immediate test failure."""
    assert set(_placeholders_all(sessions_sql.SESSIONS_CTE_PIPELINE)) == {
        "group_key",  # used in base SELECT, sessions_agg SELECT, GROUP BY
        "ua_proj",
        "status_proj",
        "resp_bytes_proj",
        "rtt_proj",
        "asn_proj",
        "country_proj",
        "url_proj",
        "edge_proj",
        "edge_sid_proj",
        "cmcd_sid_proj",
        "table_name",
        "where_clause",
        "part_key",  # used in gaps + sessions_raw window functions
        "extra_aggs",
    }
    # And pin reuse counts so a stage drop / accidental duplication is caught.
    placeholders = _placeholders_all(sessions_sql.SESSIONS_CTE_PIPELINE)
    assert placeholders.count("group_key") == 3
    assert placeholders.count("part_key") == 2


def test_sessions_page_select_renders_with_all_inputs():
    rendered = sessions_sql.SESSIONS_PAGE_SELECT.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count, NULL AS sid)",
        flag_expr="(req_count >= 1000)",
        flagged_filter="WHERE flagged = true",
        sort_by="session_start",
        sort_dir="DESC",
        limit=50,
        offset=100,
    )
    assert "WITH sessions_agg AS" in rendered
    # Template wraps the flag expr in parens to keep precedence safe.
    assert "((req_count >= 1000)) AS flagged" in rendered
    assert "FROM sessions_agg" in rendered
    assert "WHERE flagged = true" in rendered
    assert "ORDER BY session_start DESC" in rendered
    assert "LIMIT 50 OFFSET 100" in rendered


def test_sessions_page_select_handles_empty_flagged_filter():
    """When ``flagged_only`` is False, ``flagged_filter`` is an empty
    string and the ORDER BY must still render correctly."""
    rendered = sessions_sql.SESSIONS_PAGE_SELECT.format(
        cte_prefix="",
        flag_expr="(req_count >= 1000)",
        flagged_filter="",
        sort_by="req_count",
        sort_dir="ASC",
        limit=20,
        offset=0,
    )
    assert "ORDER BY req_count ASC" in rendered
    assert "LIMIT 20 OFFSET 0" in rendered
    # No spurious WHERE clause when flagged_filter is empty.
    assert "WHERE flagged" not in rendered


def test_sessions_page_select_pins_all_expected_placeholders():
    assert _placeholders_all(sessions_sql.SESSIONS_PAGE_SELECT) == sorted(
        [
            "cte_prefix",
            "flag_expr",
            "flagged_filter",
            "sort_by",
            "sort_dir",
            "limit",
            "offset",
        ]
    )


def test_sessions_count_wrapper_renders_with_all_inputs():
    rendered = sessions_sql.SESSIONS_COUNT_WRAPPER.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count)",
        flag_expr="(req_count >= 1000) OR ((reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0)",
        flagged_filter="WHERE flagged = true",
    )
    assert "WITH sessions_agg AS" in rendered
    assert "SELECT COUNT(*) FROM (SELECT" in rendered
    assert "AS flagged FROM sessions_agg) sub" in rendered
    assert "WHERE flagged = true" in rendered
    assert "(reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0" in rendered


def test_sessions_count_wrapper_pins_all_expected_placeholders():
    assert _placeholders_all(sessions_sql.SESSIONS_COUNT_WRAPPER) == sorted(
        [
            "cte_prefix",
            "flag_expr",
            "flagged_filter",
        ]
    )


# ── From test_usage.py ──


def test_edge_ratio_pct_renders_with_table_name():
    rendered = usage_sql.EDGE_RATIO_PCT.format(table='"logs_xyz"')
    assert "count(*) FILTER (WHERE edge = true)" in rendered
    assert 'FROM "logs_xyz"' in rendered


def test_edge_ratio_pct_template_has_no_raw_user_input_placeholders():
    """The only format placeholder is ``{table}`` (trusted identifier).
    A SQL parameter binding would use ``?``, not a format placeholder."""
    placeholders = [p for p in usage_sql.EDGE_RATIO_PCT.split("{")[1:] if "}" in p]
    names = [p.split("}")[0] for p in placeholders]
    assert names == ["table"]
