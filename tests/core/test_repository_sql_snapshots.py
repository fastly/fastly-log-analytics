"""R-4: SQL-string snapshot tests.

The repository contract tests pin Pydantic response shapes — they fail
when a column gets renamed or a field disappears. They do NOT fail when
a query is rewritten in a way that keeps the response shape valid but
alters semantics (e.g. dropping a WHERE clause, swapping a JOIN type,
flipping an inner-vs-left). This module fills that gap by snapshotting
the rendered SQL strings emitted by the highest-traffic templates.

How to add a new snapshot:
  1. Add a fixture below for the template.
  2. ``uv run pytest tests/core/test_repository_sql_snapshots.py --snapshot-update``
     once, review the diff, commit ``__snapshots__/test_repository_sql_snapshots.ambr``.
  3. CI runs WITHOUT --snapshot-update, so any subsequent SQL rewrite
     fails with a readable diff.

Why syrupy: ambr (Amber) format is one line per byte → diffs are
readable in the GitHub PR view, not a click-through binary blob.

Pinned templates (highest-traffic per audit §3.1.4):
  - insights: ERROR_SPIKES, BOTNET_GROUPING, WAF_SIGNAL_SPIKES,
    LATENCY_REGRESSION, ORIGIN_ERROR_RATE
  - dashboard: TIME_SERIES, MAP_DATA_BY_COUNTRY, FIELD_VALUES_*
  - origin: SUMMARY_GROUPING_SETS, TIMESERIES_BUCKETED, SLOW_URLS,
    STATUS_CODES, PATH_BREAKDOWN, POP_LATENCY
"""

from __future__ import annotations

import re

from backend.repositories._sql import dashboard as DASH_SQL
from backend.repositories._sql import insights as INS_SQL
from backend.repositories._sql import network as NET_SQL
from backend.repositories._sql import origin as ORG_SQL
from backend.repositories._sql import security as SEC_SQL
from backend.repositories._sql import sessions as SESS_SQL

# Auto-fill stub for any {placeholder} in a SQL template. Each value is
# a syntactically harmless string that the snapshot text can carry
# without DuckDB parser games.
_DEFAULT_PLACEHOLDERS: dict[str, str] = {
    # tables / temp tables
    "table_name": "t_logs",
    "table": '"t_logs"',
    "temp_table": "t_origin_tmp",
    "waf_table": "insights_waf_1",
    # filters / where
    "where_clause": "1=1",
    "where": "1=1",
    "extra_where": "",
    "search_clause": "",
    "search_cond": "",
    "ua_filter": "",
    "time_where": "1=1",
    "grouping_clause": "",
    # columns / expressions
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
    # NULL-able branch placeholders the repo passes "NULL" or a MEDIAN(...) for
    "ottlb_p50": "NULL",
    "ottlb_p95": "NULL",
    "obytes_p50": "NULL",
    "ost_5xx": "NULL",
    # numbers
    "bucket_seconds": "300",
    "interval": "INTERVAL '5 minutes'",
    "baseline_hours": "24",
    "window_hours": "1",
    "limit": "100",
    # ── Network templates ──
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
    # ``join_*`` variants are the join-table-qualified versions of the
    # bare column placeholders above, used in the 2-pass CTE map /
    # metro-leaderboard templates' JOIN ON clauses (``IS NOT DISTINCT
    # FROM tc.<col>``). For snapshot stability, any consistent string
    # works — match the bare-column stubs so a future reviewer can map
    # them by sight.
    "join_city_col": '"city"',
    "join_lat_col": "lat",
    "join_lon_col": "lon",
    "join_metro_col": "NULL",
    "join_region_col": '"region"',
    # ── Security templates ──
    "col": '"ja4"',
    "n": "50",
    "prefilter": "1=1",
    "agg_cols": "COUNT(*) AS hits",
    # ── Sessions templates ──
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


def _render(template: str, **overrides: str) -> str:
    """Fill a SQL template's ``{placeholder}`` slots with stable stubs.

    Snapshot tests care about the structural string, not realistic values
    — using a fixed dict keeps every test's stand-ins identical so a
    diff between snapshot and code is unambiguously about the SQL itself.
    """
    placeholders = set(re.findall(r"\{([a-z_][a-z_0-9]*)\}", template))
    values: dict[str, str] = {}
    for name in placeholders:
        if name in overrides:
            values[name] = overrides[name]
        elif name in _DEFAULT_PLACEHOLDERS:
            values[name] = _DEFAULT_PLACEHOLDERS[name]
        else:  # pragma: no cover — surfaces as a clear KeyError if a new placeholder lands
            raise KeyError(f"no default for SQL placeholder {{{name}}}; add to _DEFAULT_PLACEHOLDERS")
    return template.format(**values)


# ── Insights ──────────────────────────────────────────────────────────


def test_insights_error_spikes_sql(snapshot):
    assert _render(INS_SQL.ERROR_SPIKES) == snapshot


def test_insights_botnet_grouping_sql(snapshot):
    assert _render(INS_SQL.BOTNET_GROUPING) == snapshot


def test_insights_waf_signal_spikes_sql(snapshot):
    assert _render(INS_SQL.WAF_SIGNAL_SPIKES) == snapshot


def test_insights_latency_regression_sql(snapshot):
    assert _render(INS_SQL.LATENCY_REGRESSION) == snapshot


def test_insights_origin_error_rate_sql(snapshot):
    assert _render(INS_SQL.ORIGIN_ERROR_RATE) == snapshot


def test_insights_proxy_surge_sql(snapshot):
    assert _render(INS_SQL.PROXY_SURGE) == snapshot


def test_insights_asn_concentration_sql(snapshot):
    assert _render(INS_SQL.ASN_CONCENTRATION) == snapshot


# ── Dashboard ─────────────────────────────────────────────────────────


def test_dashboard_time_series_sql(snapshot):
    assert _render(DASH_SQL.TIME_SERIES) == snapshot


def test_dashboard_map_data_by_country_sql(snapshot):
    assert _render(DASH_SQL.MAP_DATA_BY_COUNTRY) == snapshot


def test_dashboard_field_values_bot_ua_sql(snapshot):
    assert _render(DASH_SQL.FIELD_VALUES_BOT_UA) == snapshot


def test_dashboard_field_values_native_column_sql(snapshot):
    assert _render(DASH_SQL.FIELD_VALUES_NATIVE_COLUMN) == snapshot


# ── Origin ────────────────────────────────────────────────────────────


def test_origin_summary_rollup_sql(snapshot):
    # SUMMARY_GROUPING_SETS was renamed to SUMMARY_ROLLUP when the
    # never-read per-edge ``by_leg`` rows were dropped (the /origin page
    # hard-codes split_by_leg: false). Snapshot pinned against the new
    # single-pass shape.
    assert _render(ORG_SQL.SUMMARY_ROLLUP) == snapshot


def test_origin_timeseries_bucketed_sql(snapshot):
    assert _render(ORG_SQL.TIMESERIES_BUCKETED) == snapshot


def test_origin_slow_urls_sql(snapshot):
    assert _render(ORG_SQL.SLOW_URLS) == snapshot


def test_origin_status_codes_sql(snapshot):
    assert _render(ORG_SQL.STATUS_CODES) == snapshot


def test_origin_path_breakdown_sql(snapshot):
    assert _render(ORG_SQL.PATH_BREAKDOWN) == snapshot


def test_origin_pop_latency_sql(snapshot):
    assert _render(ORG_SQL.POP_LATENCY) == snapshot


# ── Network ───────────────────────────────────────────────────────────


def test_network_heatmap_by_asn_bucket_sql(snapshot):
    assert _render(NET_SQL.HEATMAP_BY_ASN_BUCKET) == snapshot


def test_network_map_by_country_bucket_sql(snapshot):
    assert _render(NET_SQL.MAP_BY_COUNTRY_BUCKET) == snapshot


def test_network_metro_leaderboard_sql(snapshot):
    assert _render(NET_SQL.METRO_LEADERBOARD) == snapshot


def test_network_speed_distribution_by_asn_sql(snapshot):
    assert _render(NET_SQL.SPEED_DISTRIBUTION_BY_ASN) == snapshot


def test_network_rtt_percentiles_by_asn_sql(snapshot):
    assert _render(NET_SQL.RTT_PERCENTILES_BY_ASN) == snapshot


def test_network_quality_bar_by_group_sql(snapshot):
    assert _render(NET_SQL.QUALITY_BAR_BY_GROUP) == snapshot


def test_network_quality_countries_distinct_sql(snapshot):
    assert _render(NET_SQL.QUALITY_COUNTRIES_DISTINCT) == snapshot


def test_network_quality_scatter_sql(snapshot):
    assert _render(NET_SQL.QUALITY_SCATTER) == snapshot


# ── Security ──────────────────────────────────────────────────────────


def test_security_top_uas_by_count_sql(snapshot):
    assert _render(SEC_SQL.TOP_UAS_BY_COUNT) == snapshot


def test_security_ngwaf_top_bots_join_sql(snapshot):
    assert _render(SEC_SQL.NGWAF_TOP_BOTS_JOIN) == snapshot


def test_security_verified_bots_ts_sql(snapshot):
    assert _render(SEC_SQL.VERIFIED_BOTS_TS) == snapshot


def test_security_ngwaf_verified_bots_sql(snapshot):
    assert _render(SEC_SQL.NGWAF_VERIFIED_BOTS) == snapshot


def test_security_ngwaf_verified_bots_ts_sql(snapshot):
    assert _render(SEC_SQL.NGWAF_VERIFIED_BOTS_TS) == snapshot


def test_security_fingerprint_top_n_sql(snapshot):
    assert _render(SEC_SQL.FINGERPRINT_TOP_N) == snapshot


def test_security_fingerprint_coverage_bulk_sql(snapshot):
    assert _render(SEC_SQL.FINGERPRINT_COVERAGE_BULK) == snapshot


def test_security_req_header_size_dist_sql(snapshot):
    assert _render(SEC_SQL.REQ_HEADER_SIZE_DIST) == snapshot


def test_security_top_ips_by_max_header_sql(snapshot):
    assert _render(SEC_SQL.TOP_IPS_BY_MAX_HEADER) == snapshot


def test_security_proxy_type_dist_sql(snapshot):
    assert _render(SEC_SQL.PROXY_TYPE_DIST) == snapshot


def test_security_conn_reuse_dist_sql(snapshot):
    assert _render(SEC_SQL.CONN_REUSE_DIST) == snapshot


def test_security_wellknown_bots_ua_ip_sql(snapshot):
    assert _render(SEC_SQL.WELLKNOWN_BOTS_UA_IP) == snapshot


# ── Sessions ──────────────────────────────────────────────────────────


def test_sessions_cte_pipeline_sql(snapshot):
    assert _render(SESS_SQL.SESSIONS_CTE_PIPELINE) == snapshot


def test_sessions_page_select_sql(snapshot):
    # cte_prefix is rendered SESSIONS_CTE_PIPELINE at runtime — keep it
    # empty here so the snapshot pins only the wrapper SQL (the CTE body
    # is already covered by test_sessions_cte_pipeline_sql).
    assert _render(SESS_SQL.SESSIONS_PAGE_SELECT) == snapshot


def test_sessions_count_wrapper_sql(snapshot):
    assert _render(SESS_SQL.SESSIONS_COUNT_WRAPPER) == snapshot
