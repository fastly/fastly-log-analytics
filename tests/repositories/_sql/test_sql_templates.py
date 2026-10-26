from __future__ import annotations

import copy
import re

import pytest

from backend.repositories import alerts as alerts_sql
from backend.repositories import dashboard as dashboard_sql
from backend.repositories import query as query_sql
from backend.repositories import sessions as sessions_sql
from backend.repositories import usage as usage_sql
from backend.repositories._sql import insights as insights_sql
from backend.repositories._sql import network as network_sql
from backend.repositories._sql import origin as origin_sql
from backend.repositories._sql import security as security_sql
from backend.repositories.insights import registry


def _placeholders(template: str) -> list[str]:
    """Extract the unique ``{name}``-style format placeholders from ``template``."""
    return sorted({p.split("}")[0] for p in template.split("{")[1:] if "}" in p and p.split("}")[0]})


def _placeholders_all(template: str) -> list[str]:
    """Return the sorted list (preserving multiplicity) of ``{name}`` placeholders in ``template``."""
    return sorted(p.split("}")[0] for p in template.split("{")[1:] if "}" in p)


# ── Consolidated Placeholder Pinning ──


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        (alerts_sql.MAX_TIMESTAMP, ["table"]),
        (
            dashboard_sql.VIRTUAL_FIELD_EXPLODED_TOP_N,
            ["backing_col", "table_name", "where_clause", "requests_metric"],
        ),
        (dashboard_sql.CONN_REQUESTS_BUCKET, ["requests_metric", "table_name", "where_clause"]),
        (
            dashboard_sql.TIME_SERIES,
            ["time_bucket_select", "value_expr", "table_name", "extra_where", "where_clause"],
        ),
        (dashboard_sql.MAP_DATA_BY_COUNTRY, ["requests_metric", "table_name", "where_clause"]),
        (dashboard_sql.FIELD_VALUES_BOT_UA, ["requests_metric", "table_name", "where_clause", "ua_filter"]),
        (
            dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS,
            ["requests_metric", "backing_col", "table_name", "where_clause", "search_cond", "limit"],
        ),
        (
            dashboard_sql.FIELD_VALUES_NATIVE_COLUMN,
            ["clean_field", "requests_metric", "table_name", "where_clause", "search_cond", "limit"],
        ),
        (
            origin_sql.SUMMARY_ROLLUP,
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
            ],
        ),
        (
            origin_sql.TIMESERIES_BUCKETED,
            ["interval", "agg_expr", "unit_conv", "edge_col", "table", "where", "lat_expr", "edge_group"],
        ),
        (origin_sql.SLOW_URLS, ["lat_val", "table", "where"]),
        (origin_sql.STATUS_CODES, ["table", "where"]),
        (origin_sql.PATH_BREAKDOWN, ["lat_val", "table", "where"]),
        (origin_sql.POP_LATENCY, ["lat_val", "table", "where"]),
        (origin_sql.IP_HEALTH, ["lat_val", "table", "where"]),
        (origin_sql.SHIELDING_ANALYSIS, ["table", "where", "time_where"]),
        (
            origin_sql.AGGREGATES_CREATE_TEMP,
            ["temp_table", "select_cols", "lat_us_expr", "table", "where_clause"],
        ),
        (query_sql.EXPLAIN_WRAPPER, ["sql"]),
        (query_sql.AUTO_LIMIT_WRAPPER, ["inner", "limit"]),
        (query_sql.PRESET_SAMPLE_ROWS, ["table"]),
        (query_sql.PRESET_ROW_COUNT, ["table"]),
        (query_sql.PRESET_COLUMN_STATS, ["table"]),
        (security_sql.TOP_UAS_BY_COUNT, ["limit", "temp_table"]),
        (security_sql.NGWAF_TOP_BOTS_JOIN, ["n", "temp_table"]),
        (security_sql.VERIFIED_BOTS_TS, ["bucket_seconds", "temp_table"]),
        (security_sql.NGWAF_VERIFIED_BOTS, ["temp_table"]),
        (security_sql.NGWAF_VERIFIED_BOTS_TS, ["bucket_seconds", "temp_table"]),
        (security_sql.FINGERPRINT_TOP_N, ["col", "temp_table"]),
        (security_sql.REQ_HEADER_SIZE_DIST, ["temp_table"]),
        (security_sql.TOP_IPS_BY_MAX_HEADER, ["temp_table"]),
        (security_sql.IPV6_ADOPTION_TS, ["temp_table", "time_bucket_select"]),
        (security_sql.PROXY_TYPE_DIST, ["temp_table"]),
        (security_sql.CONN_REUSE_DIST, ["temp_table"]),
        (security_sql.WELLKNOWN_BOTS_UA_IP, ["prefilter", "temp_table"]),
        (usage_sql.EDGE_RATIO_PCT, ["table"]),
    ],
)
def test_unique_placeholders_pinned(template: str, expected: list[str]):
    assert _placeholders(template) == sorted(expected)


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        (
            network_sql.HEATMAP_BY_ASN_BUCKET,
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
            ],
        ),
        (
            network_sql.MAP_BY_COUNTRY_BUCKET,
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
            ],
        ),
        (
            network_sql.METRO_LEADERBOARD,
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
            ],
        ),
        (network_sql.SPEED_DISTRIBUTION_BY_ASN, ["table", "where", "placeholders"]),
        (network_sql.RTT_PERCENTILES_BY_ASN, ["table", "where", "placeholders"]),
        (
            network_sql.QUALITY_BAR_BY_GROUP,
            ["group_col", "group_col", "group_col", "group_col", "table", "rtt_filter", "extra_where"],
        ),
        (network_sql.QUALITY_COUNTRIES_DISTINCT, ["table", "where_clause"]),
        (network_sql.QUALITY_SCATTER, ["table", "rtt_filter"]),
        (
            sessions_sql.SESSIONS_PAGE_SELECT,
            ["cte_prefix", "flag_expr", "flagged_filter", "sort_by", "sort_dir", "limit", "offset"],
        ),
        (sessions_sql.SESSIONS_COUNT_WRAPPER, ["cte_prefix", "flag_expr", "flagged_filter"]),
        (usage_sql.EDGE_RATIO_PCT, ["table"]),
    ],
)
def test_multiplicity_placeholders_pinned(template: str, expected: list[str]):
    assert _placeholders_all(template) == sorted(expected)


# ── From test_alerts.py & test_dashboard.py ──


def test_max_timestamp_renders_with_table_name():
    assert alerts_sql.MAX_TIMESTAMP.format(table='"logs_xyz"') == 'SELECT max(timestamp) FROM "logs_xyz"'


@pytest.mark.parametrize(
    ("template", "fmt_kwargs", "expected_substrings"),
    [
        (
            dashboard_sql.VIRTUAL_FIELD_EXPLODED_TOP_N,
            {
                "backing_col": "waf_sig",
                "table_name": '"logs_xyz"',
                "where_clause": "1=1",
                "requests_metric": "COUNT(*)",
            },
            [
                "unnest(string_split(\"waf_sig\", ','))",
                'FROM "logs_xyz"',
                "WITH split_data AS",
                "CROSS JOIN total_count",
                "LIMIT 10",
            ],
        ),
        (
            dashboard_sql.CONN_REQUESTS_BUCKET,
            {"requests_metric": "COUNT(*)", "table_name": '"logs_xyz"', "where_clause": "1=1"},
            ["'2–5'", "'6–20'", "'21+'", 'ORDER BY MIN("conn_requests")', 'FROM "logs_xyz"'],
        ),
        (
            dashboard_sql.TIME_SERIES,
            {
                "time_bucket_select": "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
                "value_expr": "COUNT(*)",
                "table_name": '"logs_xyz"',
                "extra_where": "",
                "where_clause": "status = 200",
            },
            [
                "time_bucket(INTERVAL '1 minute', timestamp)",
                "COUNT(*) AS value",
                'FROM "logs_xyz"',
                "WHERE timestamp IS NOT NULL AND status = 200",
                "GROUP BY 1 ORDER BY 1",
            ],
        ),
        (
            dashboard_sql.TIME_SERIES,
            {
                "time_bucket_select": "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
                "value_expr": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0",
                "table_name": '"logs_xyz"',
                "extra_where": ' AND "elapsed" IS NOT NULL',
                "where_clause": "1=1",
            },
            ['WHERE timestamp IS NOT NULL AND "elapsed" IS NOT NULL AND 1=1'],
        ),
        (
            dashboard_sql.MAP_DATA_BY_COUNTRY,
            {"requests_metric": "COUNT(*)", "table_name": '"logs_xyz"', "where_clause": "1=1"},
            ['SELECT "country" AS country', "COUNT(*) AS count", 'WHERE "country" IS NOT NULL AND 1=1', "GROUP BY 1"],
        ),
        (
            dashboard_sql.FIELD_VALUES_BOT_UA,
            {
                "requests_metric": "COUNT(*)",
                "table_name": '"logs_xyz"',
                "where_clause": "1=1",
                "ua_filter": "AND regexp_matches(ua, 'bot|crawl')",
            },
            [
                "SELECT ua, COUNT(*) AS cnt",
                "WHERE 1=1 AND ua IS NOT NULL AND regexp_matches(ua, 'bot|crawl')",
                "GROUP BY ua",
                "ORDER BY cnt DESC",
                "LIMIT 5000",
            ],
        ),
        (
            dashboard_sql.FIELD_VALUES_BOT_UA,
            {"requests_metric": "COUNT(*)", "table_name": '"logs_xyz"', "where_clause": "1=1", "ua_filter": ""},
            ["WHERE 1=1 AND ua IS NOT NULL "],
        ),
        (
            dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS,
            {
                "requests_metric": "COUNT(*)",
                "backing_col": "waf_sig",
                "table_name": '"logs_xyz"',
                "where_clause": "1=1",
                "search_cond": "AND trim(signal) ILIKE ?",
                "limit": 20,
            },
            [
                "unnest(string_split(\"waf_sig\", ','))",
                'FROM "logs_xyz"',
                "WHERE trim(signal) != '' AND trim(signal) ILIKE ?",
                "LIMIT 20",
            ],
        ),
        (
            dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS,
            {
                "requests_metric": "COUNT(*)",
                "backing_col": "edge_score_reason",
                "table_name": '"logs_xyz"',
                "where_clause": "1=1",
                "search_cond": "",
                "limit": 10,
            },
            ["unnest(string_split(\"edge_score_reason\", ','))", "WHERE trim(signal) != '' ", "LIMIT 10"],
        ),
        (
            dashboard_sql.FIELD_VALUES_NATIVE_COLUMN,
            {
                "clean_field": "country",
                "requests_metric": "COUNT(*)",
                "table_name": '"logs_xyz"',
                "where_clause": "1=1",
                "search_cond": 'AND CAST("country" AS VARCHAR) ILIKE ?',
                "limit": 50,
            },
            [
                'SELECT "country" AS value',
                'FROM "logs_xyz"',
                'WHERE 1=1 AND CAST("country" AS VARCHAR) ILIKE ?',
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 50",
            ],
        ),
        (
            dashboard_sql.FIELD_VALUES_NATIVE_COLUMN,
            {
                "clean_field": "asn",
                "requests_metric": "COUNT(*)",
                "table_name": '"logs_xyz"',
                "where_clause": "1=1",
                "search_cond": "",
                "limit": 10,
            },
            ['SELECT "asn" AS value', "WHERE 1=1 ", "LIMIT 10"],
        ),
    ],
)
def test_dashboard_templates_render(template: str, fmt_kwargs: dict, expected_substrings: list[str]):
    rendered = template.format(**fmt_kwargs)
    for substr in expected_substrings:
        assert substr in rendered


def test_all_templates_exported():
    """Each template constant must be defined on the dashboard module."""
    for name in (
        "VIRTUAL_FIELD_EXPLODED_TOP_N",
        "CONN_REQUESTS_BUCKET",
        "TIME_SERIES",
        "MAP_DATA_BY_COUNTRY",
        "FIELD_VALUES_BOT_UA",
        "FIELD_VALUES_VIRTUAL_SIGNALS",
        "FIELD_VALUES_NATIVE_COLUMN",
    ):
        assert isinstance(getattr(dashboard_sql, name, None), str)


# ── From test_insights.py ──


def test_registry_sql_templates_match_module_constants():
    """Every registered insight's sql_template must be a constant in this module."""
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
        assert d.sql_template == getattr(insights_sql, const_name), (
            f"insight {insight_id} sql_template diverged from insights_sql.{const_name}"
        )


_CITY_LOC_KWARGS = {
    "table_name": "t_logs",
    "label_expr": "'l'",
    "region_sel": '"region"',
    "country_sel": '"country"',
    "loc_cols": '"country", "region"',
}
_CITY_LOC_KEYS = ["table_name", "label_expr", "region_sel", "country_sel", "loc_cols"]


@pytest.mark.parametrize(
    ("template", "fmt_kwargs", "expected_substrings", "q_count", "expected_placeholders"),
    [
        (
            insights_sql.ERROR_SPIKES,
            {"table_name": "t_logs"},
            [
                "FROM t_logs",
                "CAST(? AS TIMESTAMPTZ)",
                "HAVING w_total >= 3",
                "ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15",
            ],
            2,
            ["table_name"],
        ),
        (
            insights_sql.BOTNET_GROUPING,
            {"table_name": "t_logs", "fp_col": "ja4", "baseline_hours": 24, "window_hours": 1},
            ['FROM t_logs WHERE "ja4" IS NOT NULL', "GREATEST(24, 1.0) * 1"],
            2,
            ["table_name", "fp_col", "baseline_hours", "window_hours"],
        ),
        (
            insights_sql.NEW_COUNTRY_TRAFFIC,
            {"table_name": "t_logs"},
            ['WHERE "country" IS NOT NULL', "HAVING w_cnt >= 3 AND b_cnt = 0"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.CITY_SURGES,
            {**_CITY_LOC_KWARGS, "baseline_hours": 24, "window_hours": 1},
            ["FROM t_logs", 'WHERE "city" IS NOT NULL', "ORDER BY spike_ratio DESC LIMIT 15"],
            2,
            [*_CITY_LOC_KEYS, "baseline_hours", "window_hours"],
        ),
        (
            insights_sql.CITY_ERROR_SPIKES,
            _CITY_LOC_KWARGS,
            ["WITH base AS", 'WHERE "city" IS NOT NULL', "HAVING w_total >= 10 AND w_rate >= 0.10"],
            2,
            _CITY_LOC_KEYS,
        ),
        (
            insights_sql.CITY_LATENCY_REGRESSIONS,
            _CITY_LOC_KWARGS,
            ["PERCENTILE_CONT(0.95)", "w_p95 >= b_p95 * 3.0"],
            2,
            _CITY_LOC_KEYS,
        ),
        (
            insights_sql.NEW_CITY_TRAFFIC,
            _CITY_LOC_KWARGS,
            ["HAVING w_cnt >= 5 AND b_cnt = 0"],
            2,
            _CITY_LOC_KEYS,
        ),
        (insights_sql.UA_MONOCULTURE, {"table_name": "t_logs"}, ['FROM t_logs GROUP BY "ua"'], 2, ["table_name"]),
        (
            insights_sql.NEW_PROBE_URLS,
            {"table_name": "t_logs"},
            ["FROM t_logs", "regexp_matches(", "\\.env", "admin", "'i'"],
            3,
            ["table_name"],
        ),
        (
            insights_sql.WAF_SIGNAL_SPIKES,
            {"waf_table": "insights_waf_1", "baseline_hours": 24, "window_hours": 1},
            ["FROM insights_waf_1", "GROUP BY signal"],
            2,
            ["waf_table", "baseline_hours", "window_hours"],
        ),
        (
            insights_sql.PROXY_SURGE,
            {"table_name": "t_logs"},
            ['FROM t_logs WHERE "p_type" IS NOT NULL', "totals AS"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.ASN_CONCENTRATION,
            {"table_name": "t_logs"},
            ['GROUP BY "asn"', "w_cnt * 1.0 / w_total >= 0.20"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.ASN_METRO_PERFORMANCE,
            {"table_name": "t_logs"},
            ["\"country\" = 'US'", "w_med >= b_med * 1.5"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.CACHE_COLLAPSE,
            {"table_name": "t_logs"},
            ["starts_with(cache, 'HIT')", "b_rate >= 0.40"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.LATENCY_REGRESSION,
            {"table_name": "t_logs"},
            ["PERCENTILE_CONT(0.95)", "w_p95 >= b_p95 * 2.0"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.IMPOSSIBLE_DISTANCE,
            {
                "table_name": "t_logs",
                "fp_col": "ja4",
                "pop_values": "('SJC', 37.0::DOUBLE, -121.0::DOUBLE)",
                "edge_filter": 'AND t."edge" = true',
            },
            ["WITH pop_coords(pop_code, pop_lat, pop_lon) AS (VALUES ('SJC'", "RADIANS", 'AND t."edge" = true'],
            1,
            ["table_name", "fp_col", "pop_values", "edge_filter"],
        ),
        (
            insights_sql.TAIL_LATENCY,
            {"table_name": "t_logs"},
            ["PERCENTILE_CONT(0.99)", "PERCENTILE_CONT(0.50)", "ratio > 5"],
            1,
            ["table_name"],
        ),
        (
            insights_sql.CIPHER_SPREAD,
            {"table_name": "t_logs", "baseline_hours": 24, "window_hours": 1},
            ['"tls_ciphers_sha" IS NOT NULL', 'COUNT(DISTINCT "ip")'],
            2,
            ["table_name", "baseline_hours", "window_hours"],
        ),
        (
            insights_sql.REQUEST_SIZE_ANOMALY,
            {"table_name": "t_logs"},
            ["req_header_bytes > 0", "max_bytes > b_p95 * 3"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.CONNECTION_ABUSE,
            {"table_name": "t_logs"},
            ["conn_requests > 0", "max_reqs > b_p95 * 3 AND max_reqs >= 50"],
            2,
            ["table_name"],
        ),
        (insights_sql.REGION_LATENCY, {"table_name": "t_logs"}, ["stats AS", "ottfb_p95"], 2, ["table_name"]),
        (
            insights_sql.CACHE_TTL_MISMATCH,
            {"table_name": "t_logs", "q_col": '"url"'},
            ['SELECT "url" AS label', 'AVG("hits") < 2 AND AVG("ttl") > 60'],
            1,
            ["table_name", "q_col"],
        ),
        (
            insights_sql.IMAGE_OPTIMIZATION_OPPORTUNITIES,
            {"table_name": "t_logs", "ua_mobile_sel": "0"},
            ['WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ) AND t."status" = 200', "(0) AS mobile_ratio", "%.jpg%"],
            1,
            ["table_name", "ua_mobile_sel"],
        ),
        (
            insights_sql.ORIGIN_LATENCY_SPIKE,
            {"table_name": "t_logs", "url_col": '"url"'},
            ["overall_stats AS", "url_stats AS", "o.w_p95 > o.b_p95 * 2"],
            2,
            ["table_name", "url_col"],
        ),
        (
            insights_sql.ORIGIN_ERROR_RATE,
            {"table_name": "t_logs"},
            ['"ost" AS status', "status >= 500"],
            2,
            ["table_name"],
        ),
        (
            insights_sql.ORIGIN_RETRIES,
            {"table_name": "t_logs", "url_col": '"url"'},
            ['AVG("oretries")', "requests >= 5"],
            1,
            ["table_name", "url_col"],
        ),
        (
            insights_sql.ORIGIN_IP_FAILURE,
            {"table_name": "t_logs"},
            ['"oip" IS NOT NULL', "median_calc AS"],
            1,
            ["table_name"],
        ),
        (
            insights_sql.SHIELD_PATH_DEGRADATION,
            {"table_name": "t_logs"},
            ["edge_logs AS", "shield_logs AS", "'Direct to Origin'"],
            3,
            ["table_name"],
        ),
        (
            insights_sql.COALESCED_CITY_AGGREGATES,
            {"table_name": "t_logs", "label_expr": "'l'", "region_sel": '"region"', "country_sel": '"country"'},
            [
                "FROM t_logs",
                'WHERE "city" IS NOT NULL',
                'GROUP BY "city", region, country',
                "w_lat_total",
                "b_lat_total",
            ],
            2,
            ["table_name", "label_expr", "region_sel", "country_sel"],
        ),
        (
            insights_sql.COALESCED_URL_AGGREGATES,
            {"table_name": "t_logs"},
            ["FROM t_logs", 'WHERE "url" IS NOT NULL', "w_5xx", "w_hits", "w_p99", "w_p50", 'GROUP BY "url"'],
            2,
            ["table_name"],
        ),
        (
            insights_sql.COALESCED_ASN_AGGREGATES,
            {
                "table_name": "t_logs",
                "ploss_sel": '"ploss"',
                "rtt_var_sel": '"rtt_var"',
                "retrans_sel": '"retrans"',
                "p_type_sel": '"p_type"',
            },
            ["FROM t_logs", "GROUP BY asn"],
            2,
            ["table_name", "ploss_sel", "rtt_var_sel", "retrans_sel", "p_type_sel"],
        ),
        (
            insights_sql.CRAWLER_DISPARITY,
            {"table_name": "t_logs"},
            ["crawler_traffic", "w_bytes", "bytes_per_referral", "bandwidth_share_pct"],
            4,
            ["table_name"],
        ),
        (
            insights_sql.STALE_BROWSER_VERSION,
            {"table_name": "t_logs", "baseline_hours": 24, "window_hours": 1},
            ["version_aggregates"],
            2,
            None,
        ),
        (
            insights_sql.ORPHANED_DEEP_CRAWL,
            {"table_name": "t_logs", "baseline_hours": 24, "window_hours": 1},
            ["deep_requests"],
            2,
            None,
        ),
        (insights_sql.RESIDENTIAL_FINGERPRINT_DISPERSION, {"table_name": "t_logs"}, ["fingerprint_asn_maps"], 2, None),
    ],
)
def test_insights_templates_render_and_pin_placeholders(
    template: str,
    fmt_kwargs: dict,
    expected_substrings: list[str],
    q_count: int,
    expected_placeholders: list[str] | None,
):
    rendered = template.format(**fmt_kwargs)
    for substr in expected_substrings:
        assert substr in rendered
    assert rendered.count("?") == q_count
    if expected_placeholders is not None:
        assert _placeholders(template) == sorted(expected_placeholders)


def test_new_probe_regex_contains_all_probes_escaped():
    for probe in insights_sql.NEW_PROBES:
        assert re.escape(probe) in insights_sql.NEW_PROBE_REGEX, f"probe {probe!r} missing from regex"


def test_no_hardcoded_bot_brands_in_disparity_or_efficiency_sql():
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
        ("BOT_EFFICIENCY", security_sql.BOT_EFFICIENCY),
    ]:
        lower_sql = template.lower()
        for brand in forbidden:
            assert brand not in lower_sql, f"{name} must not hardcode bot/vendor name {brand!r}"


# ── From test_network.py ──


def test_heatmap_by_asn_bucket_renders_with_all_and_null_inputs():
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
    for expected in (
        "EPOCH_MS",
        "APPROX_QUANTILE(tcp_rtt, 0.5)",
        "MEDIAN(rtt_min)",
        "AVG(ploss)",
        "MEDIAN(rtt_var)",
        'FROM "_tmp_xyz"',
        "WHERE 1=1",
        "GROUP BY tc.asn, tc.bucket, tc.reqs, tc.err_count",
        "LIMIT 6000",
    ):
        assert expected in rendered
    assert rendered.count("300000") == 2

    rendered_null = network_sql.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=60000,
        rtt_min_expr="NULL",
        congestion_expr="NULL",
        ploss_expr="NULL",
        rtt_var_expr="NULL",
        table='"_tmp_x"',
        where="1=1",
        row_limit=200,
    )
    assert "NULL           AS rtt_baseline_us" in rendered_null
    assert "NULL             AS avg_ploss" in rendered_null


def test_map_by_country_bucket_and_metro_leaderboard_render():
    base_kwargs = {
        "city_col": "city",
        "lat_col": "lat",
        "lon_col": "lon",
        "metro_col": "metro",
        "join_city_col": '"_tmp_x".city',
        "join_lat_col": '"_tmp_x".lat',
        "join_lon_col": '"_tmp_x".lon',
        "join_metro_col": '"_tmp_x".metro',
        "bucket_ms": 300000,
        "ploss_expr": "AVG(ploss)",
        "table": '"_tmp_x"',
        "where": "1=1",
    }
    rendered = network_sql.MAP_BY_COUNTRY_BUCKET.format(**base_kwargs)
    for expected in (
        'APPROX_QUANTILE("_tmp_x".tcp_rtt, 0.5)',
        "AVG(ploss)",
        'FROM "_tmp_x"',
        "GROUP BY tc.country, tc.city, tc.lat, tc.lon, tc.metro, tc.bucket, tc.reqs, tc.err_count",
        "LIMIT 5000",
        "ORDER BY bucket, reqs DESC",
    ):
        assert expected in rendered
    assert rendered.count("300000") == 2

    rendered_asn = network_sql.MAP_BY_COUNTRY_BUCKET.format(
        **{**base_kwargs, "bucket_ms": 60000, "where": "1=1 AND asn = ?"}
    )
    assert "WHERE 1=1 AND asn = ?" in rendered_asn

    metro = network_sql.METRO_LEADERBOARD.format(
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
    for expected in (
        "APPROX_QUANTILE(tcp_rtt, 0.5)",
        "AVG(ploss)",
        'FROM "_tmp_x"',
        "GROUP BY tc.country, tc.city, tc.region, tc.metro, tc.reqs, tc.err_count",
        "LIMIT 100",
    ):
        assert expected in metro


def test_speed_rtt_and_quality_queries_render():
    speed = network_sql.SPEED_DISTRIBUTION_BY_ASN.format(table='"_tmp_x"', where="1=1", placeholders="?,?,?")
    for expected in ("SELECT asn, c_speed, COUNT(*)", "asn IN (?,?,?)", 'FROM "_tmp_x"', "GROUP BY asn, c_speed"):
        assert expected in speed

    rtt = network_sql.RTT_PERCENTILES_BY_ASN.format(table='"_tmp_x"', where="1=1", placeholders="?,?")
    for expected in (
        "APPROX_QUANTILE(tcp_rtt, 0.95)",
        "APPROX_QUANTILE(tcp_rtt, 0.99)",
        "asn IN (?,?)",
        "GROUP BY asn",
    ):
        assert expected in rtt

    bar_no_extra = network_sql.QUALITY_BAR_BY_GROUP.format(
        group_col="country",
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where="",
    )
    for expected in (
        '"country" AS label',
        "APPROX_QUANTILE(tcp_rtt, 0.5) / 1000.0 AS rtt_ms",
        'GROUP BY "country"',
        "LIMIT 25",
        "ORDER BY reqs DESC",
    ):
        assert expected in bar_no_extra

    bar_extra = network_sql.QUALITY_BAR_BY_GROUP.format(
        group_col="region",
        table='"logs_xyz"',
        rtt_filter="tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where=" AND country = ?",
    )
    assert "WHERE tcp_rtt IS NOT NULL AND tcp_rtt > 0 AND country = ?" in bar_extra
    assert '"region" AS label' in bar_extra

    countries = network_sql.QUALITY_COUNTRIES_DISTINCT.format(
        table='"logs_xyz"', where_clause="ts BETWEEN '2026-01-01' AND '2026-01-02'"
    )
    for expected in (
        "SELECT DISTINCT country",
        'FROM "logs_xyz"',
        "country IS NOT NULL AND country != ''",
        "ORDER BY country",
    ):
        assert expected in countries

    scatter = network_sql.QUALITY_SCATTER.format(
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
    )
    for expected in (
        "tcp_rtt / 1000.0 AS rtt_ms",
        "ttfb * 1000.0 AS ttfb_ms",
        "COALESCE(cache, 'UNKNOWN') AS cache_state",
        'FROM "logs_xyz"',
        "USING SAMPLE 2000",
        "ttfb IS NOT NULL AND ttfb > 0",
    ):
        assert expected in scatter


# ── From test_origin.py ──


def test_summary_rollup_and_timeseries_bucketed_render():
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
    assert "GROUPING SETS" not in rendered and 'GROUPING("edge")' not in rendered

    ts = origin_sql.TIMESERIES_BUCKETED.format(
        interval="INTERVAL '5' minutes",
        agg_expr='APPROX_QUANTILE("ottfb", 0.95)',
        unit_conv="/ 1000.0",
        edge_col=', "edge"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        lat_expr='"ottfb"',
        edge_group=', "edge"',
    )
    for expected in (
        "time_bucket(INTERVAL '5' minutes, \"timestamp\")",
        'FROM "logs_xyz"',
        'APPROX_QUANTILE("ottfb", 0.95) / 1000.0',
        "GROUP BY ts",
        "ORDER BY ts",
    ):
        assert expected in ts


def test_origin_table_and_shielding_queries_render():
    slow = origin_sql.SLOW_URLS.format(
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?"
    )
    assert 'SELECT\n          "url"' in slow and 'FROM "logs_xyz"' in slow and "HAVING COUNT(*) >= ?" in slow
    assert slow.rstrip().endswith("LIMIT ?")

    status = origin_sql.STATUS_CODES.format(table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('"ost"', "OVER ()", 'FROM "logs_xyz"', "ORDER BY count DESC"):
        assert expected in status

    path_bd = origin_sql.PATH_BREAKDOWN.format(lat_val='"ottfb"', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('GROUP BY "edge"', 'FROM "logs_xyz"', 'MEDIAN("ottfb")'):
        assert expected in path_bd

    pop = origin_sql.POP_LATENCY.format(lat_val='"ottfb"', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('GROUP BY "pop"', 'FROM "logs_xyz"', "ORDER BY p95_ms DESC"):
        assert expected in pop
    assert pop.rstrip().endswith("LIMIT ?")

    ip_h = origin_sql.IP_HEALTH.format(lat_val='"ottfb"', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('GROUP BY "oip"', 'FROM "logs_xyz"', "HAVING COUNT(*) >= 10", "ORDER BY error_pct DESC"):
        assert expected in ip_h
    assert ip_h.rstrip().endswith("LIMIT ?")

    shield = origin_sql.SHIELDING_ANALYSIS.format(
        table='"logs_xyz"', where="timestamp BETWEEN ? AND ?", time_where="timestamp BETWEEN ? AND ?"
    )
    for expected in (
        "WITH edge_logs AS",
        "shield_logs AS",
        "INNER JOIN shield_logs s ON s.prid = e.rid",
        "PERCENTILE_CONT(0.50)",
        "COUNT(*) OVER () ",
        "ROW_NUMBER() OVER (ORDER BY requests DESC)",
        "ROW_NUMBER() OVER (ORDER BY p50_ms DESC NULLS LAST)",
        "WHERE rn_requests <= ? OR rn_overhead <= ?",
        "AS total_routes",
    ):
        assert expected in shield
    assert shield.rstrip().endswith("ORDER BY requests DESC") and "LIMIT ?" not in shield

    temp = origin_sql.AGGREGATES_CREATE_TEMP.format(
        temp_table="t_origin_deadbeef",
        select_cols='"timestamp", "cache", "edge"',
        lat_us_expr='COALESCE("ottfb", "ttfb" * 1000000.0)',
        table='"logs_xyz"',
        where_clause="timestamp BETWEEN '2026-06-09T00:00:00Z' AND '2026-06-09T01:00:00Z'",
    )
    assert temp.startswith("CREATE TABLE t_origin_deadbeef AS ") and "CREATE TEMP TABLE" not in temp
    assert 'SELECT "timestamp", "cache", "edge",' in temp
    assert 'COALESCE("ottfb", "ttfb" * 1000000.0) AS lat_us' in temp
    assert 'FROM "logs_xyz" WHERE' in temp


# ── From test_query.py ──


def test_query_templates_render():
    assert query_sql.EXPLAIN_WRAPPER.format(sql="SELECT 1") == "EXPLAIN SELECT 1"
    auto = query_sql.AUTO_LIMIT_WRAPPER.format(inner="SELECT * FROM logs_svc ORDER BY id", limit=1001)
    assert "SELECT * FROM (SELECT * FROM logs_svc ORDER BY id) AS _q" in auto and "LIMIT 1001" in auto
    assert "AS _q" in query_sql.AUTO_LIMIT_WRAPPER.format(inner="SELECT 1", limit=10)

    sample = query_sql.PRESET_SAMPLE_ROWS.format(table="logs_myservice")
    assert sample == "SELECT * FROM logs_myservice LIMIT 100" and "ORDER BY" not in sample.upper()
    assert (
        query_sql.PRESET_ROW_COUNT.format(table="logs_myservice") == "SELECT count(*) AS total_rows FROM logs_myservice"
    )
    assert query_sql.PRESET_COLUMN_STATS.format(table="logs_myservice") == "SUMMARIZE logs_myservice"


# ── From test_security.py ──


@pytest.mark.parametrize(
    ("template", "fmt_kwargs", "expected_substrings"),
    [
        (
            security_sql.TOP_UAS_BY_COUNT,
            {"temp_table": "t_filtered_xyz", "limit": 2000},
            [
                "SELECT ua, count(*) AS cnt",
                "FROM t_filtered_xyz",
                "WHERE ua IS NOT NULL",
                "GROUP BY ua",
                "ORDER BY cnt DESC",
                "LIMIT 2000",
            ],
        ),
        (
            security_sql.NGWAF_TOP_BOTS_JOIN,
            {"temp_table": "t_filtered_xyz", "n": 15},
            [
                "SELECT nb.bot_name, nb.category, count(*) AS cnt",
                "FROM t_filtered_xyz t",
                "INNER JOIN temp.ngwaf_top_ngwaf_bots nb USING (waf_req_id)",
                "WHERE nb.bot_name IS NOT NULL",
                "LIMIT 15",
            ],
        ),
        (
            security_sql.VERIFIED_BOTS_TS,
            {"bucket_seconds": 300, "temp_table": "t_filtered_xyz"},
            [
                "time_bucket(INTERVAL '300 seconds', timestamp)",
                "replace(tag, 'VERIFIED-BOT.', '')",
                "FROM t_filtered_xyz",
                "WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'",
                "GROUP BY 1, 2",
                "ORDER BY 1, 2",
            ],
        ),
        (
            security_sql.NGWAF_VERIFIED_BOTS,
            {"temp_table": "t_filtered_xyz"},
            [
                "nb.bot_name",
                "nb.wellknown_bot_name",
                "nb.category",
                "count(*) AS request_count",
                "FROM t_filtered_xyz t",
                "INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id)",
                "GROUP BY 1, 2, 3",
                "ORDER BY 4 DESC",
            ],
        ),
        (
            security_sql.NGWAF_VERIFIED_BOTS_TS,
            {"bucket_seconds": 60, "temp_table": "t_filtered_xyz"},
            [
                "time_bucket(INTERVAL '60 seconds', t.timestamp)",
                "FROM t_filtered_xyz t",
                "INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id)",
                "WHERE nb.bot_name IS NOT NULL",
                "GROUP BY 1, 2",
            ],
        ),
        (
            security_sql.FINGERPRINT_TOP_N,
            {"temp_table": "t_filtered_xyz", "col": "tls_ciphers_sha"},
            [
                'SELECT "tls_ciphers_sha"',
                "count(DISTINCT ip) as ip_count",
                "count(*) as req_count",
                "FROM t_filtered_xyz",
                'WHERE "tls_ciphers_sha" IS NOT NULL',
                "GROUP BY 1 ORDER BY 3 DESC LIMIT 20",
            ],
        ),
        (
            security_sql.REQ_HEADER_SIZE_DIST,
            {"temp_table": "t_filtered_xyz"},
            [
                "WHEN req_header_bytes <= 256 THEN '0-256B'",
                "WHEN req_header_bytes <= 32768 THEN '24-32KB'",
                "ELSE '>32KB'",
                "MIN(req_header_bytes) as min_val",
                "FROM t_filtered_xyz",
                "WHERE req_header_bytes IS NOT NULL",
                "GROUP BY 1 ORDER BY min_val",
            ],
        ),
        (
            security_sql.TOP_IPS_BY_MAX_HEADER,
            {"temp_table": "t_filtered_xyz"},
            [
                "SELECT ip, MAX(req_header_bytes) as max_header",
                "FROM t_filtered_xyz",
                "WHERE ip IS NOT NULL AND req_header_bytes IS NOT NULL",
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            ],
        ),
        (
            security_sql.IPV6_ADOPTION_TS,
            {
                "time_bucket_select": "time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
                "temp_table": "t_filtered_xyz",
            },
            [
                "time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
                "SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) * 100.0 / count(*) as ipv6_pct",
                "FROM t_filtered_xyz",
                "GROUP BY 1 ORDER BY 1",
            ],
        ),
        (
            security_sql.PROXY_TYPE_DIST,
            {"temp_table": "t_filtered_xyz"},
            [
                "SELECT p_type, count(*) as count",
                "FROM t_filtered_xyz",
                "WHERE p_type IS NOT NULL AND p_type != ''",
                "GROUP BY 1 ORDER BY 2 DESC",
            ],
        ),
        (
            security_sql.CONN_REUSE_DIST,
            {"temp_table": "t_filtered_xyz"},
            [
                "WHEN conn_requests = 1 THEN '1 (None)'",
                "WHEN conn_requests <= 5 THEN '2-5'",
                "WHEN conn_requests <= 100 THEN '21-100'",
                "ELSE '>100'",
                "MIN(conn_requests) as min_val",
                "FROM t_filtered_xyz",
                "WHERE conn_requests IS NOT NULL AND conn_requests > 0",
                "GROUP BY 1 ORDER BY min_val",
            ],
        ),
        (
            security_sql.WELLKNOWN_BOTS_UA_IP,
            {"temp_table": "t_filtered_xyz", "prefilter": "WHERE ua IS NOT NULL AND ip IS NOT NULL"},
            [
                "SELECT ua, ip, count(*) AS cnt",
                "FROM t_filtered_xyz",
                "WHERE ua IS NOT NULL AND ip IS NOT NULL",
                "GROUP BY ua, ip",
                "ORDER BY cnt DESC",
                "LIMIT 10000",
            ],
        ),
        (
            security_sql.WELLKNOWN_BOTS_UA_IP,
            {
                "temp_table": "t_filtered_xyz",
                "prefilter": "WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '(googlebot|bingbot)')",
            },
            ["regexp_matches(ua, '(googlebot|bingbot)')", "FROM t_filtered_xyz"],
        ),
    ],
)
def test_security_templates_render(template: str, fmt_kwargs: dict, expected_substrings: list[str]):
    rendered = template.format(**fmt_kwargs)
    for substr in expected_substrings:
        assert substr in rendered


# ── From test_sessions.py & test_usage.py ──


def test_sessions_cte_pipeline_renders_and_pins_placeholders():
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
    for expected in (
        "WITH base AS",
        "gaps AS",
        "marks AS",
        "sessions_raw AS",
        "sessions_agg AS",
        "LAG(ts) OVER (PARTITION BY",
        "INTERVAL 30 MINUTES",
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        "CAST(? AS TIMESTAMPTZ)",
        '"ja4"',
        '"ua"',
        '"edge_sid"',
        '"cmcd_sid"',
        'SUM("resp_bytes") AS total_bytes',
        'MAX("edge_sid") AS edge_sid',
        'FROM "logs_xyz"',
    ):
        assert expected in rendered

    empty_rendered = sessions_sql.SESSIONS_CTE_PIPELINE.format(
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
    assert "WITH base AS" in empty_rendered and 'PARTITION BY "ip"' in empty_rendered
    for absent in ('"ja4"', '"ua"', '"status"', '"edge_sid"'):
        assert absent not in empty_rendered

    placeholders = _placeholders_all(sessions_sql.SESSIONS_CTE_PIPELINE)
    assert set(placeholders) == {
        "group_key",
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
        "part_key",
        "extra_aggs",
    }
    assert placeholders.count("group_key") == 3 and placeholders.count("part_key") == 2


def test_sessions_page_select_and_count_wrapper_render():
    rendered = sessions_sql.SESSIONS_PAGE_SELECT.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count, NULL AS sid)",
        flag_expr="(req_count >= 1000)",
        flagged_filter="WHERE flagged = true",
        sort_by="session_start",
        sort_dir="DESC",
        limit=50,
        offset=100,
    )
    for expected in (
        "WITH sessions_agg AS",
        "((req_count >= 1000)) AS flagged",
        "FROM sessions_agg",
        "WHERE flagged = true",
        "ORDER BY session_start DESC",
        "LIMIT 50 OFFSET 100",
    ):
        assert expected in rendered

    unflagged = sessions_sql.SESSIONS_PAGE_SELECT.format(
        cte_prefix="",
        flag_expr="(req_count >= 1000)",
        flagged_filter="",
        sort_by="req_count",
        sort_dir="ASC",
        limit=20,
        offset=0,
    )
    assert "ORDER BY req_count ASC" in unflagged and "LIMIT 20 OFFSET 0" in unflagged
    assert "WHERE flagged" not in unflagged

    count_sql = sessions_sql.SESSIONS_COUNT_WRAPPER.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count)",
        flag_expr="(req_count >= 1000) OR ((reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0)",
        flagged_filter="WHERE flagged = true",
    )
    for expected in (
        "WITH sessions_agg AS",
        "SELECT COUNT(*) FROM (SELECT",
        "AS flagged FROM sessions_agg) sub",
        "WHERE flagged = true",
        "(reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0",
    ):
        assert expected in count_sql


def test_edge_ratio_pct_renders_with_table_name():
    rendered = usage_sql.EDGE_RATIO_PCT.format(table='"logs_xyz"')
    assert "count(*) FILTER (WHERE edge = true)" in rendered and 'FROM "logs_xyz"' in rendered


# ── Merged from tests/core/test_repository_sql_snapshots.py ──

_DEFAULT_PLACEHOLDERS: dict[str, str] = {
    "table_name": "t_logs",
    "table": '"t_logs"',
    "temp_table": "t_origin_tmp",
    "waf_table": "insights_waf_1",
    "where_clause": "1=1",
    "where": "1=1",
    "total_misses": "NULL",
    "total_passes": "NULL",
    "extra_where": "",
    "search_clause": "",
    "search_cond": "",
    "ua_filter": "",
    "time_where": "1=1",
    "grouping_clause": "",
    "column": '"status"',
    "clean_field": "status",
    "backing_col": "status",
    "cache_col": '"cache"',
    "select_cols": "*",
    "requests_metric": "COUNT(*)",
    "chart_metric_expr": "COUNT(*)",
    "value_expr": "COUNT(*)",
    "time_bucket_select": "time_bucket(INTERVAL '5 minutes', timestamp) AS bucket",
    "agg_expr": "AVG(elapsed)",
    "unit_conv": "/ 1000.0",
    "edge_col": "",
    "edge_select": "''",
    "edge_group": "''",
    "grouping_expr": "0",
    "lat_val": "elapsed",
    "lat_expr": "elapsed",
    "lat_us_expr": "elapsed",
    "cdn_ovh": "0",
    "fp_col": "ja4",
    "ottlb_p50": "NULL",
    "ottlb_p95": "NULL",
    "obytes_p50": "NULL",
    "ost_5xx": "NULL",
    "bucket_seconds": "300",
    "interval": "INTERVAL '5 minutes'",
    "baseline_hours": "24",
    "window_hours": "1",
    "limit": "100",
    "bucket_ms": "300000",
    "city_col": '"city"',
    "congestion_expr": "NULL",
    "group_col": "country",
    "lat_col": "lat",
    "lon_col": "lon",
    "metro_col": "NULL",
    "placeholders": "?,?,?",
    "ploss_expr": "NULL",
    "region_col": '"region"',
    "row_limit": "1000",
    "rtt_filter": "tcp_rtt IS NOT NULL AND tcp_rtt > 0",
    "rtt_min_expr": "MEDIAN(rtt_min)",
    "rtt_var_expr": "MEDIAN(rtt_var)",
    "join_city_col": '"city"',
    "join_lat_col": "lat",
    "join_lon_col": "lon",
    "join_metro_col": "NULL",
    "join_region_col": '"region"',
    "col": '"ja4"',
    "n": "50",
    "prefilter": "1=1",
    "agg_cols": "COUNT(*) AS hits",
    "asn_proj": "",
    "country_proj": "",
    "cte_prefix": "",
    "edge_proj": "",
    "edge_sid_proj": "",
    "cmcd_sid_proj": "",
    "extra_aggs": "",
    "flag_expr": "req_count >= 1000",
    "flagged_filter": "",
    "group_key": '"ip"',
    "offset": "0",
    "part_key": '"ip"',
    "resp_bytes_proj": "",
    "rtt_proj": "",
    "sort_by": "req_count",
    "sort_dir": "DESC",
    "status_proj": "",
    "ua_proj": "",
    "url_proj": "",
}


def _render_snapshot(template: str, **overrides: str) -> str:
    placeholders = set(re.findall(r"\{([a-z_][a-z_0-9]*)\}", template))
    values: dict[str, str] = {}
    for name in placeholders:
        if name in overrides:
            values[name] = overrides[name]
        elif name in _DEFAULT_PLACEHOLDERS:
            values[name] = _DEFAULT_PLACEHOLDERS[name]
        else:  # pragma: no cover
            raise KeyError(f"no default for SQL placeholder {{{name}}}; add to _DEFAULT_PLACEHOLDERS")
    return template.format(**values)


def test_insights_error_spikes_sql(snapshot):
    assert _render_snapshot(insights_sql.ERROR_SPIKES) == snapshot


def test_insights_botnet_grouping_sql(snapshot):
    assert _render_snapshot(insights_sql.BOTNET_GROUPING) == snapshot


def test_insights_waf_signal_spikes_sql(snapshot):
    assert _render_snapshot(insights_sql.WAF_SIGNAL_SPIKES) == snapshot


def test_insights_latency_regression_sql(snapshot):
    assert _render_snapshot(insights_sql.LATENCY_REGRESSION) == snapshot


def test_insights_origin_error_rate_sql(snapshot):
    assert _render_snapshot(insights_sql.ORIGIN_ERROR_RATE) == snapshot


def test_insights_proxy_surge_sql(snapshot):
    assert _render_snapshot(insights_sql.PROXY_SURGE) == snapshot


def test_insights_asn_concentration_sql(snapshot):
    assert _render_snapshot(insights_sql.ASN_CONCENTRATION) == snapshot


def test_dashboard_time_series_sql(snapshot):
    assert _render_snapshot(dashboard_sql.TIME_SERIES) == snapshot


def test_dashboard_map_data_by_country_sql(snapshot):
    assert _render_snapshot(dashboard_sql.MAP_DATA_BY_COUNTRY) == snapshot


def test_dashboard_field_values_bot_ua_sql(snapshot):
    assert _render_snapshot(dashboard_sql.FIELD_VALUES_BOT_UA) == snapshot


def test_dashboard_field_values_native_column_sql(snapshot):
    assert _render_snapshot(dashboard_sql.FIELD_VALUES_NATIVE_COLUMN) == snapshot


def test_origin_summary_rollup_sql(snapshot):
    assert _render_snapshot(origin_sql.SUMMARY_ROLLUP) == snapshot


def test_origin_timeseries_bucketed_sql(snapshot):
    assert _render_snapshot(origin_sql.TIMESERIES_BUCKETED) == snapshot


def test_origin_slow_urls_sql(snapshot):
    assert _render_snapshot(origin_sql.SLOW_URLS) == snapshot


def test_origin_status_codes_sql(snapshot):
    assert _render_snapshot(origin_sql.STATUS_CODES) == snapshot


def test_origin_path_breakdown_sql(snapshot):
    assert _render_snapshot(origin_sql.PATH_BREAKDOWN) == snapshot


def test_origin_pop_latency_sql(snapshot):
    assert _render_snapshot(origin_sql.POP_LATENCY) == snapshot


def test_network_heatmap_by_asn_bucket_sql(snapshot):
    assert _render_snapshot(network_sql.HEATMAP_BY_ASN_BUCKET) == snapshot


def test_network_map_by_country_bucket_sql(snapshot):
    assert _render_snapshot(network_sql.MAP_BY_COUNTRY_BUCKET) == snapshot


def test_network_metro_leaderboard_sql(snapshot):
    assert _render_snapshot(network_sql.METRO_LEADERBOARD) == snapshot


def test_network_speed_distribution_by_asn_sql(snapshot):
    assert _render_snapshot(network_sql.SPEED_DISTRIBUTION_BY_ASN) == snapshot


def test_network_rtt_percentiles_by_asn_sql(snapshot):
    assert _render_snapshot(network_sql.RTT_PERCENTILES_BY_ASN) == snapshot


def test_network_quality_bar_by_group_sql(snapshot):
    assert _render_snapshot(network_sql.QUALITY_BAR_BY_GROUP) == snapshot


def test_network_quality_countries_distinct_sql(snapshot):
    assert _render_snapshot(network_sql.QUALITY_COUNTRIES_DISTINCT) == snapshot


def test_network_quality_scatter_sql(snapshot):
    assert _render_snapshot(network_sql.QUALITY_SCATTER) == snapshot


def test_security_top_uas_by_count_sql(snapshot):
    assert _render_snapshot(security_sql.TOP_UAS_BY_COUNT) == snapshot


def test_security_ngwaf_top_bots_join_sql(snapshot):
    assert _render_snapshot(security_sql.NGWAF_TOP_BOTS_JOIN) == snapshot


def test_security_verified_bots_ts_sql(snapshot):
    assert _render_snapshot(security_sql.VERIFIED_BOTS_TS) == snapshot


def test_security_ngwaf_verified_bots_sql(snapshot):
    assert _render_snapshot(security_sql.NGWAF_VERIFIED_BOTS) == snapshot


def test_security_ngwaf_verified_bots_ts_sql(snapshot):
    assert _render_snapshot(security_sql.NGWAF_VERIFIED_BOTS_TS) == snapshot


def test_security_fingerprint_top_n_sql(snapshot):
    assert _render_snapshot(security_sql.FINGERPRINT_TOP_N) == snapshot


def test_security_fingerprint_coverage_bulk_sql(snapshot):
    assert _render_snapshot(security_sql.FINGERPRINT_COVERAGE_BULK) == snapshot


def test_security_req_header_size_dist_sql(snapshot):
    assert _render_snapshot(security_sql.REQ_HEADER_SIZE_DIST) == snapshot


def test_security_top_ips_by_max_header_sql(snapshot):
    assert _render_snapshot(security_sql.TOP_IPS_BY_MAX_HEADER) == snapshot


def test_security_proxy_type_dist_sql(snapshot):
    assert _render_snapshot(security_sql.PROXY_TYPE_DIST) == snapshot


def test_security_conn_reuse_dist_sql(snapshot):
    assert _render_snapshot(security_sql.CONN_REUSE_DIST) == snapshot


def test_security_wellknown_bots_ua_ip_sql(snapshot):
    assert _render_snapshot(security_sql.WELLKNOWN_BOTS_UA_IP) == snapshot


def test_sessions_cte_pipeline_sql(snapshot):
    assert _render_snapshot(sessions_sql.SESSIONS_CTE_PIPELINE) == snapshot


def test_sessions_page_select_sql(snapshot):
    assert _render_snapshot(sessions_sql.SESSIONS_PAGE_SELECT) == snapshot


def test_sessions_count_wrapper_sql(snapshot):
    assert _render_snapshot(sessions_sql.SESSIONS_COUNT_WRAPPER) == snapshot


def _strip_volatile(payload: dict) -> dict:
    out = copy.deepcopy(payload)
    for key in ("_debug_queries", "_debug_calls", "_debug_sqlite", "_debug_postgres", "_section_timings", "_is_cached"):
        out.pop(key, None)
    if isinstance(out.get("version"), str):
        out["version"] = "<scrubbed>"
    return out


def test_api_health_response_shape(client, snapshot):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert _strip_volatile(r.json()) == snapshot


def test_api_bootstrap_empty_response_shape(client, snapshot, monkeypatch):
    r = client.get("/api/bootstrap")
    assert r.status_code == 200
    body = _strip_volatile(r.json())
    skeleton = {
        "active_service_id_is_none_or_str": body.get("active_service_id") is None
        or isinstance(body.get("active_service_id"), str),
        "services_is_list": isinstance(body.get("services"), list),
        "services_empty_in_sandbox": body.get("services") == [],
        "has_top_level_keys": sorted([k for k in body if not k.startswith("_") and k != "version"]),
    }
    assert skeleton == snapshot
