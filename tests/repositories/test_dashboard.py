"""Phase 4 /dashboard performance & security audit golden and regression tests."""

from __future__ import annotations

import gc
import hashlib
import ipaddress
import json
import json as _json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from backend.core.query_instrumentation import InstrumentedDuckDBConnection
from backend.core.query_registry import query_registry
from backend.core.share_db.validation import apply_pii_policy
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.models.dashboard import AggregatesRequest, FieldValuesRequest, RawRequest
from backend.repositories.cmcd import get_cmcd_aggregates
from backend.repositories.performance import get_performance_aggregates
from backend.repositories.query import _ANALYST_WINDOW_VIEW, MAX_QUERY_ROWS, execute_query, get_presets

from backend import config as svcconfig
from backend.core import duckdb as db_core
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata import usage_log
from backend.core.rollups._common import (
    OVERVIEW_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
)
from backend.core.rollups.day_bundles import compact_time_series_closed_days_to_daily
from backend.high_scale import dashboard as high_scale_dashboard
from backend.models.common import FilterSpec
from backend.repositories import _base, rollup_coverage
from backend.repositories import _base as repo_base
from backend.repositories import dashboard as dash
from backend.repositories import query as q
from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories.dashboard import (
    DASHBOARD_CACHE_TTL,
    FIELDS,
    _add_bot_columns,
    _raw_extent_reaches_window_start,
    get_aggregates,
    get_field_values,
    get_raw_df,
)
from backend.utils import bot_sources, rdns_cache
from backend.utils.telemetry import set_page_load_id
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID


def _src_without_table():
    return {"name": "nonexistent_service", "service_id": "nonexistent_service"}


def _empty_logs_table(con, src):
    con.execute("SET TimeZone='UTC';")
    table_name = _safe_table(src["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    con.execute(f"CREATE TABLE {table_name} ({schema_def})")
    return table_name


def test_filtered_dashboard_and_empty_and_populated_aggregates(in_memory_duckdb, test_service_source):
    empty_res = get_aggregates(in_memory_duckdb, _src_without_table(), None, None, {}, "1 minute", "requests")
    assert empty_res["total_rows"] == 0 and empty_res["time_series"] == [] and empty_res["map_data"] == []
    for field in FIELDS:
        assert empty_res["data"][field] == {"top": [], "total": 0}

    logs = generate_mock_logs(test_service_source, num_logs=60, hours_ago=1)
    for log in logs[:5]:
        log["status"] = 500
    for log in logs[5:10]:
        log["status"] = 404
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert result["total_rows"] > 0 and result["metric"] == "requests"
    obs = {row["reader"]: row for row in result["reader_coverage"]}
    for reader in ("dashboard.count", "dashboard.top_n", "dashboard.time_series"):
        assert obs[reader]["eligibility_reason"] == "filtered" and obs[reader]["eligible"] is False
    assert result["history"]["window_rows"] == result["total_rows"]
    assert result["history"]["window_count_source"] == "filtered_temp"
    for f in ("url", "ip", "status", "country"):
        assert result["data"][f]["total"] > 0
    assert len(result["debug_queries"]) > 0 and "sql" in result["debug_queries"][0]


@pytest.mark.parametrize(
    "metric,expected_metric",
    [
        ("5xx", "5xx"),
        ("4xx", "4xx"),
        ("hit_rate", "hit_rate"),
        ("p95_latency", "p95_latency"),
        ("p50_latency", "p50_latency"),
        ("p99_latency", "p99_latency"),
        ("throughput", "throughput"),
        ("ttfb", "ttfb"),
        ("gibberish_not_a_metric", "requests"),
    ],
)
def test_get_aggregates_chart_metrics(in_memory_duckdb, test_service_source, metric, expected_metric):
    logs = generate_mock_logs(test_service_source, num_logs=30)
    for log in logs[:5]:
        log["status"] = 500
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    flt = (
        {"status": FilterSpec(mode="include", values=["200", "404", "500"])}
        if metric != "gibberish_not_a_metric"
        else {}
    )
    res = get_aggregates(in_memory_duckdb, test_service_source, None, None, flt, "1 minute", metric)
    assert res["metric"] == expected_metric
    if metric == "hit_rate" and res["time_series"]:
        assert 0.0 <= res["time_series"][0]["value"] <= 100.0


def test_self_heal_fires_once_when_stale_and_skips_when_outside_window_or_filtered(
    in_memory_duckdb, test_service_source, monkeypatch
):
    table_name = _empty_logs_table(in_memory_duckdb, test_service_source)
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _name: {"local_rows": 30, "earliest_log_at": start_time, "latest_log_at": latest},
    )
    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)
    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", lambda *a, **k: ([("url", "some_url", 10)], ["url"]))

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        rebuild_calls["n"] += 1
        insert_mock_logs(con, table_name, logs)

    monkeypatch.setattr(dash, "force_rebuild_view", fake_rebuild)
    result = dash.get_aggregates(
        in_memory_duckdb, test_service_source, start_time, end_time, {}, "1 minute", "requests"
    )
    assert rebuild_calls["n"] == 1 and result["total_rows"] == 30 and result["data"]["url"]["total"] > 0

    rebuild_calls["n"] = 0
    old_start = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old_end = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    res_outside = dash.get_aggregates(
        in_memory_duckdb, test_service_source, old_start, old_end, {}, "1 minute", "requests"
    )
    assert rebuild_calls["n"] == 0 and res_outside["total_rows"] == 0

    res_filtered = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        start_time,
        end_time,
        {"country": FilterSpec(mode="include", values=["ZZ"])},
        "1 minute",
        "requests",
    )
    assert rebuild_calls["n"] == 0 and res_filtered["total_rows"] == 0


def test_get_aggregates_rollup_path_map_data_and_section_selectors(in_memory_duckdb, test_service_source, monkeypatch):
    insert_mock_logs(
        in_memory_duckdb, _safe_table(test_service_source["name"]), generate_mock_logs(test_service_source, num_logs=40)
    )
    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)
    calls: list[tuple] = []

    def spy_top_n(self, fields, start_time, end_time, limit=10, per_field_limits=None, _phase_log=None, **_kwargs):
        calls.append((tuple(fields), limit, dict(per_field_limits or {})))
        country_entries = [("country", f"C{i:02d}", 100 - i) for i in range(12)]
        return country_entries + [("url", "/page1", 50), ("url", "/page2", 30)], list(fields)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", spy_top_n)
    result = dash.get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, "1 minute", "requests")
    assert len(calls) == 1
    fields_called, limit_called, pfl_called = calls[0]
    assert "country" in fields_called and pfl_called.get("country") == 500 and limit_called == 10
    assert len(result["data"]["country"]["top"]) == 10 and len(result["map_data"]) == 12

    calls.clear()
    res_topten = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        "1 minute",
        "requests",
        include_time_series=False,
        include_conn_requests=False,
        include_map_data=False,
        include_top_n=True,
    )
    assert len(calls) == 1 and len(calls[0][0]) > 5
    assert res_topten["data"]["url"]["top"] and res_topten["time_series"] == [] and res_topten["map_data"] == []

    for map_enabled, expected_calls in ((True, 1), (False, 0)):
        calls.clear()
        dash.get_aggregates(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {},
            "1 minute",
            "requests",
            include_time_series=True,
            include_conn_requests=True,
            include_map_data=map_enabled,
            include_top_n=False,
        )
        assert len(calls) == expected_calls


@pytest.mark.skipif(DASHBOARD_CACHE_TTL == 0, reason="Dashboard cache disabled")
def test_get_aggregates_result_is_cached(in_memory_duckdb, test_service_source):
    insert_mock_logs(
        in_memory_duckdb, _safe_table(test_service_source["name"]), generate_mock_logs(test_service_source, num_logs=10)
    )
    r1 = get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, "1 minute", "requests")
    r2 = get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, "1 minute", "requests")
    assert r2.get("is_cached") is True and r1["total_rows"] == r2["total_rows"]


def test_get_raw_df_and_add_bot_columns(in_memory_duckdb, test_service_source):
    assert get_raw_df(in_memory_duckdb, _src_without_table(), None, None, {}, limit=100, columns=[]).empty
    insert_mock_logs(
        in_memory_duckdb,
        _safe_table(test_service_source["name"]),
        generate_mock_logs(test_service_source, num_logs=5, hours_ago=1),
    )
    out = get_raw_df(in_memory_duckdb, test_service_source, None, None, {}, limit=5, columns=["timestamp", "status"])
    assert isinstance(out, pd.DataFrame) and len(out) <= 5 and set(out.columns) <= {"timestamp", "status"}

    select_cols = ['"status"']
    assert _add_bot_columns({"ua", "ip", "waf_req_id"}, ["status"], select_cols) == (False, False)
    select_cols = ['"timestamp"']
    assert _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols) == (True, False)
    assert '"ua"' in select_cols and '"ip"' in select_cols
    select_cols = ['"ua"']
    _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols)
    assert select_cols.count('"ua"') == 1
    select_cols = ['"status"']
    assert _add_bot_columns(set(), ["_bot_name"], select_cols) == (True, False) and select_cols == ['"status"']
    select_cols = ['"timestamp"']
    assert _add_bot_columns({"waf_req_id"}, ["_ngwaf_bot_name"], select_cols) == (False, True)
    assert '"waf_req_id"' in select_cols and '"ua"' not in select_cols


@pytest.mark.parametrize(
    ("window_start", "earliest_event_at", "expected"),
    [
        ("2026-10-01T00:00:00Z", "2026-09-25T05:38:42Z", True),
        ("2026-10-01T00:00:00Z", "2026-10-01T00:00:01Z", False),
        ("2026-10-01T00:00:00Z", None, None),
    ],
)
def test_raw_extent_start_coverage_is_nullable_and_boundary_aware(window_start, earliest_event_at, expected):
    assert _raw_extent_reaches_window_start(window_start, earliest_event_at) is expected


def test_get_field_values_validation_and_country_and_cache(in_memory_duckdb, test_service_source, tmp_path):
    with pytest.raises(ValueError, match="Invalid field name"):
        get_field_values(in_memory_duckdb, test_service_source, "\";'''", "", 10, None, None, {})

    for fld in ("country", "_bot_name"):
        assert get_field_values(in_memory_duckdb, _src_without_table(), fld, "", 10, None, None, {})["values"] == []

    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 20 else "GB"
        log["waf_sig"] = "SQLI,XSS" if i % 2 == 0 else "SQLI"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    for bad_field in ('"; DROP TABLE foo', "completely_unknown_field"):
        with pytest.raises(LookupError):
            get_field_values(in_memory_duckdb, test_service_source, bad_field, "", 10, None, None, {})

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            "country",
            "",
            10,
            None,
            None,
            {"country": FilterSpec(mode="include", values=["US"])},
        )
        vals = {v["value"]: v["count"] for v in out["values"]}
        assert vals["US"] == 20 and vals["GB"] == 10

        out_united = get_field_values(in_memory_duckdb, test_service_source, "country", "united", 10, None, None, {})
        assert {"US", "GB"} <= {v["value"] for v in out_united["values"]}

        out_waf = get_field_values(in_memory_duckdb, test_service_source, "waf_sig_ind", "", 10, None, None, {})
        assert {v["value"]: v["count"] for v in out_waf["values"]} == {"SQLI": 30, "XSS": 15}

        out_waf_search = get_field_values(
            in_memory_duckdb, test_service_source, "waf_sig_ind", "SQL", 10, None, None, {}
        )
        assert {v["value"] for v in out_waf_search["values"]} == {"SQLI"}

        (tmp_path / "top_values.json").write_text(_json.dumps({"country": [{"value": "ZZ", "count": 999}]}))
        out_cached = get_field_values(in_memory_duckdb, test_service_source, "country", "", 10, None, None, {})
        assert out_cached["values"][0]["value"] == "ZZ"


def test_get_field_values_bot_name_and_asn_search(in_memory_duckdb, test_service_source, tmp_path):
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["ua"] = "Bingbot/2.0"
        log["asn"] = 13335 if i < 6 else 32934
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch(
            "backend.utils.bot_sources.build_matcher",
            side_effect=lambda: lambda _ua: [{"id": "bingbot", "name": "Microsoft Bingbot"}],
        ),
    ):
        out = get_field_values(in_memory_duckdb, test_service_source, "_bot_name", "microsoft", 10, None, None, {})
        assert out["values"][0]["value"] == "bingbot" and out["values"][0]["count"] == 10
        out_miss = get_field_values(in_memory_duckdb, test_service_source, "_bot_name", "google", 10, None, None, {})
        assert out_miss["values"] == []

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", return_value=[13335]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out_asn = get_field_values(in_memory_duckdb, test_service_source, "asn", "cloudflare", 10, None, None, {})
        assert {v["value"] for v in out_asn["values"]} == {13335}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", side_effect=RuntimeError("db locked")),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out_fallback = get_field_values(in_memory_duckdb, test_service_source, "asn", "133", 10, None, None, {})
        assert {v["value"] for v in out_fallback["values"]} == {13335}


def test_get_aggregates_rollup_temp_avoidance_virtual_fields_and_conn_requests_fallback(
    in_memory_duckdb, test_service_source, monkeypatch
):
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, generate_mock_logs(test_service_source, num_logs=4))
    in_memory_duckdb.execute(f"UPDATE {table_name} SET conn_requests = 1")
    in_memory_duckdb.execute(
        f"UPDATE {table_name} SET conn_requests = 7 WHERE rowid IN (SELECT rowid FROM {table_name} LIMIT 1)"
    )

    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)
    monkeypatch.setattr(
        QueryRunner, "execute_top_n_rollups", lambda self, fields, s, e, **kw: ([("url", "/p", 5)], fields)
    )

    sentinel = {"top": [{"value": "1", "count": 5}, {"value": "21+", "count": 2}], "total": 7}
    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", lambda self, s, e, **kw: dict(sentinel))
    virtual_calls: list[str] = []
    monkeypatch.setattr(
        QueryRunner,
        "try_virtual_field_top_n_from_rollup",
        lambda self, vid, bcol, s, e, **kw: (virtual_calls.append(vid), {"top": [], "total": 0})[1],
    )
    temp_creates: list[str] = []
    orig_ctt = QueryRunner.create_temp_table
    monkeypatch.setattr(
        QueryRunner,
        "create_temp_table",
        lambda self, sql, params=None: (temp_creates.append(sql), orig_ctt(self, sql, params))[1],
    )

    result = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
        {},
        "1 minute",
        "requests",
    )
    assert temp_creates == [] and result["data"]["conn_requests"] == sentinel
    assert "waf_sig_ind" in virtual_calls and result["data"]["waf_sig_ind"] == {"top": [], "total": 0}

    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", lambda self, s, e, **kw: None)
    st = (datetime.now(UTC) - timedelta(hours=26)).isoformat()
    et = (datetime.now(UTC) + timedelta(hours=26)).isoformat()
    res_fb = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        st,
        et,
        {"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        "1 minute",
        "requests",
    )
    assert res_fb["data"]["conn_requests"] == {
        "top": [{"value": "1", "count": 3}, {"value": "6–20", "count": 1}],
        "total": 4,
    }


def test_get_aggregates_prunes_non_top_n_fields(in_memory_duckdb, test_service_source, monkeypatch):
    in_memory_duckdb.execute(
        "CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ, status VARCHAR, host VARCHAR, req_bytes INTEGER, resp_bytes INTEGER)"
    )
    rollup_calls, batch_calls = [], []
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, **kw: (rollup_calls.append(list(fields)), ([], list(fields)))[1],
    )
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_batch",
        lambda self, fields, *a, **kw: (batch_calls.append(list(fields)), ([], []))[1],
    )

    for use_rollups, target, expected in (
        (True, rollup_calls, {"status", "host", "country"}),
        (False, batch_calls, {"status", "host"}),
    ):
        monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src, ur=use_rollups: ur)
        dash.get_aggregates(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {} if use_rollups else {"status": FilterSpec(mode="include", values=["200"])},
            "1 minute",
            "requests",
            fields_filter=["status", "req_bytes", "resp_bytes", "host"],
            include_time_series=False,
            include_conn_requests=False,
            include_map_data=False,
            include_top_n=True,
        )
        assert len(target) == 1 and set(target[0]) == expected


def _write_time_series_hour(con: duckdb.DuckDBPyConnection, hour_root: str, hour_str: str, reqs: int = 10) -> None:
    ts_dir = Path(hour_root) / f"hour={hour_str}"
    ts_dir.mkdir(parents=True, exist_ok=True)
    out = ts_dir / TIME_SERIES_BUNDLE_FILENAME
    bucket = f"{hour_str[:10]} {hour_str[11:13]}:00:00Z"
    con.execute(
        f"COPY (SELECT TIMESTAMPTZ '{bucket}' AS bucket, CAST({reqs} AS BIGINT) AS requests, "
        "CAST(0 AS BIGINT) AS status_4xx, CAST(0 AS BIGINT) AS status_5xx, CAST(5 AS BIGINT) AS hits, "
        "CAST(8 AS BIGINT) AS cache_total, CAST(1000 AS BIGINT) AS resp_bytes_sum, "
        f"CAST(2.0 AS DOUBLE) AS ttfb_sum, CAST(10 AS BIGINT) AS ttfb_count) TO '{out}' (FORMAT PARQUET)"
    )


def _write_day_overview(con: duckdb.DuckDBPyConnection, day_root: str, day_str: str, reqs_per_hour: int = 10) -> None:
    day_dir = Path(day_root) / f"day={day_str}"
    day_dir.mkdir(parents=True, exist_ok=True)
    out = day_dir / OVERVIEW_BUNDLE_FILENAME
    rows = ", ".join(f"('{day_str}T{h:02d}', CAST({reqs_per_hour} AS BIGINT))" for h in range(24))
    con.execute(f"COPY (SELECT * FROM (VALUES {rows}) AS t(hour_start, requests)) TO '{out}' (FORMAT PARQUET)")


def test_compact_time_series_closed_days_and_30d_and_rolling_window_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    con = duckdb.connect(":memory:")
    src = {"name": "svc_30d", "service_id": "svc_30d"}
    con.execute(
        "CREATE TABLE logs_svc_30d (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    hour_root, day_root = _hour_bundled_root(src), _day_bundled_root(src)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    st = (now - timedelta(days=29)).replace(hour=0)
    cur_day, yesterday = st.date(), (now - timedelta(days=1)).date()
    while cur_day < yesterday:
        _write_day_overview(con, day_root, cur_day.isoformat(), reqs_per_hour=10)
        cur_day += timedelta(days=1)
    for h in range(24):
        _write_time_series_hour(con, hour_root, f"{yesterday.isoformat()}-{h:02d}", reqs=20)

    assert compact_time_series_closed_days_to_daily("svc_30d", src, lookback_days=5) == 1
    assert (Path(day_root) / f"day={yesterday.isoformat()}" / TIME_SERIES_BUNDLE_FILENAME).is_file()
    for h in range(now.hour):
        _write_time_series_hour(con, hour_root, f"{now.date().isoformat()}-{h:02d}", reqs=30)

    rollup_coverage._listdir_cache.clear()
    runner = repo_base.QueryRunner(con, src)
    total = runner.try_count_from_rollup(
        st.isoformat(), now.isoformat(), "logs_svc_30d", "1=1", [], unfiltered_window=True
    )
    assert total is not None and total > 0
    ts_rows = runner.try_time_series_from_rollup(
        "requests", "1 hour", st.isoformat(), now.isoformat(), "logs_svc_30d", "1=1", [], unfiltered_window=True
    )
    assert ts_rows is not None and len(ts_rows) > 24


def test_rolling_window_uses_daily_time_series_bundle_when_day0_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    service = "svc_rolling_ts"
    src = {"name": service, "service_id": service}
    con = duckdb.connect(":memory:")
    con.execute(
        f"CREATE TABLE logs_{service} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    hour_root = _hour_bundled_root(src)
    now = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    days = [(now - timedelta(days=i)).date().isoformat() for i in range(7, 0, -1)]
    for d in days:
        for h in range(24):
            _write_time_series_hour(con, hour_root, f"{d}-{h:02d}", reqs=5)
    assert compact_time_series_closed_days_to_daily(service, src, lookback_days=10) == 7

    rollup_coverage._listdir_cache.clear()
    runner = repo_base.QueryRunner(con, src)
    ts = runner.try_time_series_from_rollup(
        "requests",
        "1 hour",
        f"{days[0]}T12:00:00Z",
        f"{now.date().isoformat()}T00:00:00Z",
        f"logs_{service}",
        "1=1",
        [],
        unfiltered_window=True,
    )
    assert ts is not None and sum(int(pt["value"]) for pt in ts) == (12 + 6 * 24) * 5
    assert any("read_parquet([7 files])" in q["sql"] for q in runner.telemetry()["debug_queries"])


def test_clickhouse_aggregates_field_values_and_raw_df(monkeypatch: pytest.MonkeyPatch) -> None:
    executed_queries: list[str] = []

    class FakeClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            executed_queries.append(sql)
            p = params or {}
            dims = [v for k, v in sorted(p.items()) if k.startswith("dim_")]
            if "AS chart_bucket" in sql:
                return [{"chart_bucket": "2026-10-01 00:00:00", "value": 1000}]
            if "AS bucket_start" in sql or "SELECT bucket_start" in sql:
                return [{"bucket_start": "2026-10-01 00:00:00", "value": 1000}]
            if "GROUP BY dimension" in sql and "GROUP BY dimension, value" not in sql:
                return [{"dimension": d, "total_count": 2} for d in dims]
            if "LIMIT 10 BY dimension" in sql:
                return [{"dimension": d, "value": f"{d}_v1", "aggregate_count": 100} for d in dims]
            if "request_aggregates" in sql and "AS total_count" in sql:
                return [{"total_count": 2}]
            if "request_aggregates" in sql:
                return [{"value": "/api/v1", "aggregate_count": 50}, {"value": "/health", "aggregate_count": 10}]
            if "GROUP BY v" in sql:
                return [{"v": "/api/v1", "c": 50}]
            if "FROM request_facts" in sql:
                return [
                    {
                        "timestamp": datetime(2026, 10, 20, 12, 0, tzinfo=UTC),
                        "ip": "198.51.100.1",
                        "url": "/api/v1",
                        "country": "US",
                        "custom_fields": {"custom_header": "abc", "status": "200"},
                    }
                ]
            return [{"value": "US", "aggregate_count": 100, "total_count": 100}]

    service = HighScaleService(
        service_id="svc1",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc1",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 20, tzinfo=UTC),
            coverage_end=datetime(2026, 10, 20, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    res = high_scale_dashboard.aggregates(
        service,
        AggregatesRequest(fields=["url", "status", "country"], chart_interval="1 hour", chart_metric="requests"),
        "2026-09-20T00:00:00Z",
        "2026-10-20T00:00:00Z",
    )
    assert res.data["url"].total == 2 and res.data["url"].top[0].value == "url_v1" and len(executed_queries) <= 6

    monkeypatch.setattr(bot_sources, "enrich_bot_metadata", lambda df: None)
    unfiltered_fv = high_scale_dashboard.field_values(
        service, FieldValuesRequest(field="url", limit=10), "2026-10-19T00:00:00Z", "2026-10-20T00:00:00Z"
    )
    assert len(unfiltered_fv.values) == 2 and unfiltered_fv.values[0].value == "/api/v1"
    search_fv = high_scale_dashboard.field_values(
        service, FieldValuesRequest(field="url", search="api", limit=10), "2026-10-19T00:00:00Z", "2026-10-20T00:00:00Z"
    )
    assert len(search_fv.values) == 1 and search_fv.values[0].value == "/api/v1"
    df = high_scale_dashboard.raw_df(
        service, RawRequest(limit=10), "2026-10-19T00:00:00Z", "2026-10-20T00:00:00Z", limit=10
    )
    assert len(df) == 1 and df.loc[0, "custom_header"] == "abc" and "custom_fields" not in df.columns


def test_pii_policy_page_telemetry_invalidate_and_bot_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "schema": [
            {"column": "host", "type": "VARCHAR", "min": "example.com", "max": "z.example.com"},
            {"column": "client_ip", "type": "VARCHAR", "min": "100.1.232.227", "max": "199.232.65.91"},
        ],
        "services": [
            {
                "service_id": "svc1",
                "status": {
                    "schema": [
                        {"column": "client_ip", "type": "VARCHAR", "min": "99.95.121.49", "max": "199.232.65.91"}
                    ]
                },
            }
        ],
    }
    masked = apply_pii_policy(payload, {"mask_ips": True})
    assert (
        masked["schema"][1]["min"] == "100.1.232.xxx"
        and masked["services"][0]["status"]["schema"][0]["min"] == "99.95.121.xxx"
    )

    recorded: list[dict] = []
    monkeypatch.setattr(usage_log, "log_telemetry_query", lambda **kw: recorded.append(kw))
    con = duckdb.connect(":memory:")
    set_page_load_id("test-page-load-01")
    try:
        repo_base.QueryRunner(con, {"name": "svc", "service_id": "svc"}).execute("SELECT 1 AS x")
    finally:
        set_page_load_id(None)
    assert len(recorded) == 1 and recorded[0]["page_load_id"] == "test-page-load-01"

    dash._dashboard_cache["abc123:svc_test"] = (1.0, {"total_rows": 10})
    dash._dashboard_cache["top_bots:def456:svc_test"] = (1.0, {"bots": [{"name": "Bot"}]})
    dash._dashboard_cache["other:svc_keep"] = (1.0, {"total_rows": 5})
    dash.invalidate_service("svc_test")
    assert "abc123:svc_test" not in dash._dashboard_cache and "other:svc_keep" in dash._dashboard_cache
    dash.invalidate_service("svc_keep")

    fake_feeds = {
        "openai-gptbot": [ipaddress.ip_network(f"198.51.{i}.0/24") for i in range(100)],
        "anthropic-ai": [ipaddress.ip_network("203.0.113.0/24"), ipaddress.ip_network("2001:db8::/32")],
    }
    monkeypatch.setattr(bot_sources, "_get_compiled_ip_feeds", lambda: fake_feeds)
    monkeypatch.setattr(bot_sources, "_compiled_ip_feeds", {})
    monkeypatch.setattr(rdns_cache, "get_hostnames", lambda ips: {})
    df = pd.DataFrame(
        [
            {"ua": "Mozilla/5.0 (compatible; GPTBot/1.0)", "ip": "198.51.42.10", "waf_req_id": ""},
            {"ua": "Mozilla/5.0", "ip": "203.0.113.99", "waf_req_id": ""},
            {"ua": "Mozilla/5.0", "ip": "192.0.2.1", "waf_req_id": ""},
        ]
        * 2000
    )
    t0 = time.perf_counter()
    bot_sources.enrich_bot_metadata(df)
    assert time.perf_counter() - t0 < 5.0
    assert (
        "openai-gptbot" in str(df.loc[0, "_bot_name"])
        and "IP-Match" in str(df.loc[1, "_bot_name"])
        and df.loc[2, "_bot_name"] == "null"
    )


# ── Performance & CMCD aggregates tests (folded from test_performance.py) ──

_ALL_PERF_SECTIONS = {"waterfall", "top_urls", "top_asns", "ttl_dist", "scatter"}
_T0 = datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC)


def _perf_latency_hit(*_a, **kw):
    value = "/rolled" if kw.get("dimension") == "url" else 7922
    return {"rows": [{"value": value, "requests": 100, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}]}


def test_performance_aggregates(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=50, hours_ago=1)
    for i, log in enumerate(logs[:5]):
        log["elapsed"] = 500000 + (i * 10000)
        log["ttfb"] = f"{(400000 + (i * 10000)) / 1000000:.6f}"
        log["url"] = f"/slow-path-{i}"
        log["asn"] = 7922
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, {})
    assert isinstance(result["top_urls"], list) and isinstance(result["top_asns"], list)
    if result["top_urls"]:
        assert result["top_urls"][0]["p99"] > 0


def test_get_performance_aggregates_returns_empty_arrays_for_unknown_schema(in_memory_duckdb, test_service_source):
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, {})
    for key in ("top_urls", "top_asns", "ttl_dist", "scatter"):
        assert result[key] == []


@pytest.mark.parametrize(
    ("sort_by", "expected_first"),
    [(None, "/high-tail"), ("p50", "/slow-median")],
)
def test_get_performance_aggregates_top_urls_sorting(in_memory_duckdb, test_service_source, sort_by, expected_first):
    logs = generate_mock_logs(test_service_source, num_logs=80, hours_ago=1)
    for i, log in enumerate(logs):
        if sort_by == "p50":
            log["url"] = "/slow-median" if i < 40 else "/spiky"
            log["elapsed"] = 200_000 if i < 40 else (50_000 if i % 5 != 0 else 800_000)
        else:
            log["url"] = "/high-tail" if i < 40 else "/steady"
            log["elapsed"] = (100_000 if i % 10 != 0 else 1_000_000) if i < 40 else 100_000
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    kw = {"sort_by": sort_by} if sort_by else {}
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, {}, **kw)
    if len(result["top_urls"]) >= 2:
        assert result["top_urls"][0]["url"] == expected_first


def test_get_performance_aggregates_ttl_dist_and_scatter(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    ttl_values = [5, 25, 50, 200, 500, 3600, 86400 * 2]
    for i, log in enumerate(logs):
        log["ttl"] = ttl_values[i % len(ttl_values)]
        log["elapsed"] = 100_000
        log["ttfb"] = f"{0.05:.6f}"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_performance_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200", "404", "500"])},
    )
    buckets = {row["bucket"] for row in result["ttl_dist"]}
    assert any(b in buckets for b in ("<10s", "<30s", "<1m", "<5m", "<10m", "<1h", "<3d"))
    assert all(pt["edge"] >= 0 for pt in result["scatter"])


@pytest.mark.parametrize(
    ("sections", "filtered", "expected_present", "expected_timers", "blocked_timers"),
    [
        (None, False, _ALL_PERF_SECTIONS, set(), set()),
        (
            {"ttl_dist"},
            True,
            {"ttl_dist"},
            {"ttl_dist_query"},
            {"top_urls_query", "top_asns_query", "scatter_waterfall_query"},
        ),
        ({"top_urls", "top_asns"}, True, {"top_urls", "top_asns"}, {"top_urls_query", "top_asns_query"}, set()),
        (
            {"top_urls", "top_asns", "ttl_dist"},
            False,
            {"top_urls", "top_asns", "ttl_dist"},
            set(),
            {"scatter_waterfall_query"},
        ),
        (
            {"waterfall", "scatter"},
            True,
            {"waterfall", "scatter"},
            {"scatter_waterfall_query"},
            {"top_urls_query", "top_asns_query", "ttl_dist_query"},
        ),
    ],
)
def test_performance_aggregates_section_filtering(
    in_memory_duckdb, test_service_source, sections, filtered, expected_present, expected_timers, blocked_timers
):
    logs = generate_mock_logs(test_service_source, num_logs=25)
    for log in logs:
        log["ttl"] = 60
        log["elapsed"] = 200_000
        log["ttfb"] = f"{0.1:.6f}"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    flt = {"status": FilterSpec(mode="include", values=["200", "404", "500"])} if filtered else {}
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, flt, sections=sections)
    assert (_ALL_PERF_SECTIONS & result.keys()) == expected_present
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert expected_timers.issubset(timer_names)
    assert not (blocked_timers & timer_names)
    if sections == {"top_urls", "top_asns"}:
        assert sum(1 for t in result.get("section_timings", []) if t["section"] == "temp_table_create") == 1


def test_performance_aggregates_temp_skipped_and_narrowed_with_rollups(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ttl"] = 60
        log["elapsed"] = 200_000
        log["ttfb"] = f"{0.1:.6f}"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", side_effect=_perf_latency_hit),
        patch(
            "backend.repositories._base.QueryRunner.try_perf_ttl_dist_from_rollup",
            return_value=[{"bucket": "<1m", "count": 5}],
        ),
    ):
        result = get_performance_aggregates(
            in_memory_duckdb,
            test_service_source,
            "2026-01-01T00:00:00Z",
            "2026-01-08T00:00:00Z",
            {},
            sections={"top_urls", "top_asns", "ttl_dist"},
        )
        names = {t["section"] for t in result["section_timings"]}
        assert "perf:temp_skipped" in names and "temp_table_create" not in names
        assert result["ttl_dist"] == [{"bucket": "<1m", "count": 5}]
        assert result["top_urls"][0]["url"] == "/rolled" and result.get("approx") is True
        for row in result["section_timings"]:
            assert row["current_execution"] is True
            if row["section"] in {"top_urls_query_rollup", "top_asns_query_rollup", "ttl_dist_rollup"}:
                assert row["source"] == "rollup" and row["activity"] == "compute"
        labels = [row for row in result["section_timings"] if row["section"] == "performance.asn_labels"]
        assert len(labels) == 1 and labels[0]["activity"] == "overhead"

        res_scatter = get_performance_aggregates(
            in_memory_duckdb,
            test_service_source,
            "2026-01-01T00:00:00Z",
            "2026-01-08T00:00:00Z",
            {"status": FilterSpec(mode="include", values=["200", "404", "500"])},
            sections=None,
        )
        names2 = {t["section"] for t in res_scatter["section_timings"]}
        assert "perf:temp_skipped" in names2 and "temp_table_create" not in names2
        assert "ttl_dist_rollup" in names2 and "ttl_dist_query" not in names2
        assert res_scatter["ttl_dist"] == [{"bucket": "<1m", "count": 5}]
        assert "scatter" in res_scatter and "waterfall" in res_scatter


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _make_table(con, name: str, rows: list[tuple]) -> dict:
    table = _safe_table(name)
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, cmcd_sid VARCHAR, cmcd_bs BOOLEAN, "
        "cmcd_mtp INTEGER, cmcd_ot VARCHAR, country VARCHAR, asn INTEGER)"
    )
    con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    return {"name": name, "service_id": f"svc-{name}"}


def test_rebuffer_by_country_and_cmcd_unavailable(con, monkeypatch):
    rows = [
        (_T0, "s1", False, 100, "v", "US", 1),
        (_T0, "s1", True, 300, "v", "US", 1),
        (_T0, "s1", False, 500, "v", "US", 1),
        (_T0, "s1", False, 9999, "a", "US", 1),
        (_T0, "s2", False, 1000, "v", "US", 2),
        (_T0, "s3", False, 7777, "a", "GB", 3),
    ]
    src = _make_table(con, "cmcd_country", rows)
    seen: list[str] = []
    original = _base.QueryRunner.execute
    monkeypatch.setattr(
        _base.QueryRunner, "execute", lambda self, sql, *a, **kw: (seen.append(sql), original(self, sql, *a, **kw))[1]
    )

    result = get_cmcd_aggregates(
        con, src, "2026-10-21T00:00:00Z", "2026-10-22T00:00:00Z", {}, sections={"rebuffer_by_country"}
    )
    by_country = {r["country"]: r for r in result["rebuffer_by_country"]}
    assert by_country["US"]["session_count"] == 2 and by_country["US"]["rebuffer_rate"] == 50.0
    assert by_country["US"]["median_throughput"] == 650
    assert by_country["GB"]["session_count"] == 1 and by_country["GB"]["median_throughput"] is None
    live = [row for row in result["section_timings"] if row.get("source") == "live"]
    assert {row["section"] for row in live} == {"temp_table_create", "rebuffer_by_country"}
    assert all(row["activity"] == "compute" and row["current_execution"] for row in live)

    per_session = [s for s in seen if "per_session" in s and "country" in s]
    assert per_session and all("APPROX_QUANTILE" not in s.upper() for s in per_session)

    table = _safe_table("cmcd_none")
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, country VARCHAR)")
    assert (
        get_cmcd_aggregates(con, {"name": "cmcd_none", "service_id": "svc-none"}, None, None, {})["available"] is False
    )


def test_cmcd_aggregates_router_endpoints(client, in_memory_duckdb, test_service_source, monkeypatch):
    mock_ret = {"available": True, "has_data": True, "overview": {"active_sessions": 10, "peak_viewers": 4}}
    with patch("backend.repositories.cmcd.get_cmcd_aggregates", return_value=mock_ret) as mock_get:
        resp = client.post(
            "/api/cmcd/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "start_time": "2026-08-19T12:00:00Z",
                "end_time": "2026-08-19T13:00:00Z",
                "sections": ["overview"],
            },
        )
        assert resp.status_code == 200 and resp.json()["overview"] == {"active_sessions": 10, "peak_viewers": 4}
        mock_get.assert_called_once()

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})
    with patch(
        "backend.repositories.cmcd.get_cmcd_aggregates",
        return_value={
            "available": True,
            "has_data": False,
            "section_timings": [{"section": "temp_table_create", "time_ms": 1.5}],
        },
    ) as mock_get2:
        resp2 = client.post(
            "/api/cmcd/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "range_token": "last_24h",
                "anchor": "2026-08-19T12:00:00Z",
                "sections": ["bitrate_ts"],
            },
        )
        assert resp2.status_code == 200 and resp2.json()["available"] is True and resp2.json()["has_data"] is False
        mock_get2.assert_called_once()
        names = [row["section"] for row in resp2.json()["_section_timings"]]
        assert names == ["request_setup", "connection_acquire", "temp_table_create"]


# ── Merged from tests/repositories/test_query.py ──


class TestLogsTableAlias:
    """execute_query rewrites 'logs' to the real table name when the source name differs."""

    def _con_with_table(self, table_name: str) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE {table_name} (id INTEGER, val VARCHAR)")
        con.execute(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
        return con

    @pytest.mark.parametrize(
        ("table_name", "src", "sql", "expected_rows"),
        [
            ("logs_myservice", {"name": "myservice"}, "SELECT * FROM logs ORDER BY id", 2),
            ("logs", {"name": "default"}, "SELECT * FROM logs ORDER BY id", 2),
            ("logs", None, "SELECT * FROM logs", 2),
            ("logs_svc", {"name": "svc"}, "select * from LOGS order by id", 2),
            ("logs_svc2", {"name": "svc2"}, "WITH logs AS (SELECT id FROM logs WHERE id = 1) SELECT * FROM logs", 1),
        ],
    )
    def test_rewrites_logs_variants(self, table_name, src, sql, expected_rows):
        con = self._con_with_table(table_name)
        result = execute_query(con, src, sql, max_rows=100, want_explain=False)
        assert result["row_count"] == expected_rows

    def test_logs_word_boundary_not_matched_in_longer_names(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc3 (id INTEGER); CREATE TABLE my_logs (id INTEGER)")
        con.execute("INSERT INTO logs_svc3 VALUES (1); INSERT INTO my_logs VALUES (99)")
        with pytest.raises(PermissionError, match="not in the allowed set"):
            execute_query(con, {"name": "svc3"}, "SELECT * FROM my_logs", max_rows=100, want_explain=False)


class TestGetPresets:
    def test_presets_use_actual_table_name_and_sample_rows_has_no_order_by(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ, val INTEGER)")
        presets = get_presets(test_service_source, in_memory_duckdb)
        assert presets
        for p in presets:
            assert "logs_test_service" in p["sql"]
            assert "FROM logs " not in p["sql"] and not p["sql"].endswith("FROM logs")
        sample = next(p for p in presets if p["name"] == "Sample rows")
        assert "ORDER BY" not in sample["sql"].upper()


class TestAutoLimitPushdown:
    def _con(self, rows: int = 50) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (id INTEGER, val VARCHAR)")
        con.executemany("INSERT INTO logs VALUES (?, ?)", [(i, f"v{i}") for i in range(rows)])
        return con

    @pytest.mark.parametrize(
        ("rows", "sql", "max_rows", "expected_count", "expected_trunc", "expected_total"),
        [
            (50, "SELECT * FROM logs ORDER BY id", 10, 10, True, -1),
            (50, "SELECT * FROM logs ORDER BY id LIMIT 5", 100, 5, False, 5),
            (5, "SELECT * FROM logs ORDER BY id", 100, 5, False, 5),
            (42, "SELECT COUNT(*) AS n FROM logs", 100, 1, False, 1),
            (50, "/* This is a comment */ SELECT * FROM logs", 10, 10, True, -1),
        ],
    )
    def test_limit_pushdown_cases(self, rows, sql, max_rows, expected_count, expected_trunc, expected_total):
        con = self._con(rows=rows)
        result = execute_query(con, None, sql, max_rows=max_rows, want_explain=False)
        assert result["row_count"] == expected_count
        assert result["truncated"] is expected_trunc
        assert result["total_rows"] == expected_total

    def test_summarize_is_rejected_at_validator(self):
        con = self._con(rows=10)
        with pytest.raises(PermissionError, match="SHOW / DESCRIBE / SUMMARIZE"):
            execute_query(con, None, "SUMMARIZE logs", max_rows=100, want_explain=False)


class TestAnalystTimeWindow:
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")

    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.executemany(
            "INSERT INTO logs_svc VALUES (?, ?, ?)",
            [
                ("2026-06-17T10:00:00+00:00", "1.2.3.4", "/a"),
                ("2026-06-17T10:30:00+00:00", "5.6.7.8", "/b"),
                ("2026-06-01T00:00:00+00:00", "9.9.9.9", "/old"),
            ],
        )
        return con

    def test_admin_and_analyst_window_filtering(self):
        con = self._con()
        assert (
            execute_query(con, self.SRC, "SELECT * FROM logs ORDER BY timestamp", max_rows=100, want_explain=False)[
                "row_count"
            ]
            == 3
        )

        res = execute_query(
            con,
            self.SRC,
            "SELECT * FROM logs ORDER BY timestamp",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
        )
        assert res["row_count"] == 2 and all("9.9.9.9" not in str(row) for row in res["data"])

        for sql in (
            "SELECT count(*) AS n FROM logs",
            "WITH logs AS (SELECT * FROM logs WHERE url LIKE '/%') SELECT count(*) AS n FROM logs",
        ):
            r = execute_query(con, self.SRC, sql, max_rows=100, want_explain=False, time_filter=self.WINDOW)
            assert r["data"][0]["n"] == 2

        widen = "SELECT * FROM logs WHERE timestamp >= TIMESTAMPTZ '1970-01-01T00:00:00+00:00'"
        assert (
            execute_query(con, self.SRC, widen, max_rows=100, want_explain=False, time_filter=self.WINDOW)["row_count"]
            == 2
        )

    def test_default_service_table_and_view_cleanup(self):
        con_def = duckdb.connect(":memory:")
        con_def.execute("CREATE TABLE logs (timestamp TIMESTAMPTZ, ip VARCHAR)")
        con_def.executemany(
            "INSERT INTO logs VALUES (?, ?)",
            [("2026-06-17T10:00:00+00:00", "1.1.1.1"), ("2026-06-01T00:00:00+00:00", "2.2.2.2")],
        )
        res = execute_query(
            con_def,
            {"name": "default"},
            "SELECT count(*) AS n FROM logs",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
        )
        assert res["data"][0]["n"] == 1

        con = self._con()
        execute_query(con, self.SRC, "SELECT * FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW)
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {_ANALYST_WINDOW_VIEW}").fetchall()

        with pytest.raises(Exception):  # noqa: B017
            execute_query(
                con, self.SRC, "SELECT no_such_col FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW
            )
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {_ANALYST_WINDOW_VIEW}").fetchall()


class TestValueShapeMasking:
    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.execute("INSERT INTO logs VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', '/path')")
        return con

    def test_mask_ips_survives_column_aliasing_and_false_returns_raw(self):
        con = self._con()
        res_masked = execute_query(
            con, None, "SELECT ip AS addr, url FROM logs", max_rows=100, want_explain=False, mask_ips=True
        )
        assert res_masked["data"][0]["addr"] == "1.2.3.xxx" and res_masked["data"][0]["url"] == "/path"
        res_raw = execute_query(
            con, None, "SELECT ip AS addr FROM logs", max_rows=100, want_explain=False, mask_ips=False
        )
        assert res_raw["data"][0]["addr"] == "1.2.3.4"


class TestSessionIdRedaction:
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")
    HASH = "a1b2c3d4deadbeefcafef00dfeedface"

    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, cookie_session VARCHAR, url VARCHAR)")
        con.executemany(
            "INSERT INTO logs_svc VALUES (?, ?, ?, ?)",
            [
                ("2026-06-17T10:00:00+00:00", "1.2.3.4", self.HASH, "/a"),
                ("2026-06-17T10:30:00+00:00", "5.6.7.8", "", "/b"),
            ],
        )
        return con

    @pytest.mark.security_regression
    def test_mask_ips_redacts_aliased_cookie_session(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT cookie_session AS s, ip AS addr, url FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["s"] for r in result["data"]] == ["[redacted]", ""]
        assert [r["addr"] for r in result["data"]] == ["[redacted]", "[redacted]"]
        assert self.HASH not in str(result["data"])

    @pytest.mark.security_regression
    def test_mask_ips_redacts_cookie_session_via_string_concat(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT 'x' || cookie_session AS c FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["c"] for r in result["data"]] == ["x[redacted]", "x"]
        assert self.HASH not in str(result["data"])

    @pytest.mark.security_regression
    def test_mask_ips_redacts_ip_via_string_concat_and_group_by(self):
        con = self._con()
        concat = execute_query(
            con,
            self.SRC,
            "SELECT 'x' || ip AS a FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["a"] for r in concat["data"]] == ["x[redacted]", "x[redacted]"]
        grouped = execute_query(
            con,
            self.SRC,
            "SELECT 'x' || ip AS a, count(*) AS c FROM logs GROUP BY ip",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert {r["a"] for r in grouped["data"]} == {"x[redacted]"}
        for raw in ("1.2.3.4", "5.6.7.8"):
            assert raw not in str(concat["data"]) and raw not in str(grouped["data"])

    @pytest.mark.security_regression
    def test_mask_ips_false_returns_raw_ip_on_query(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT ip AS a FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=False,
        )
        assert result["data"][0]["a"] == "1.2.3.4"

    @pytest.mark.security_regression
    def test_mask_ips_redacts_cookie_session_via_select_star(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT * FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["cookie_session"] for r in result["data"]] == ["[redacted]", ""]
        assert [r["url"] for r in result["data"]] == ["/a", "/b"]
        assert self.HASH not in str(result["data"])

    @pytest.mark.security_regression
    def test_mask_ips_false_returns_raw_cookie_session(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT cookie_session AS s FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=False,
        )
        assert result["data"][0]["s"] == self.HASH

    def test_missing_cookie_session_column_does_not_break_query(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.execute("INSERT INTO logs_svc VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', '/a')")
        result = execute_query(
            con,
            self.SRC,
            "SELECT * FROM logs",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert result["data"][0]["ip"] == "[redacted]" and result["data"][0]["url"] == "/a"


class TestMaxRowsClamp:
    def test_max_rows_clamped_to_ceiling(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (id INTEGER)")
        con.executemany("INSERT INTO logs VALUES (?)", [(i,) for i in range(MAX_QUERY_ROWS + 50)])
        result = execute_query(con, None, "SELECT * FROM logs ORDER BY id", max_rows=10**9, want_explain=False)
        assert result["row_count"] == MAX_QUERY_ROWS and result["truncated"] is True


@pytest.mark.security_regression
class TestCidSaltNeverRecorded:
    SALT = "ZZSALTMARKER" + "ab" * 10
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")

    def _con(self):
        raw = duckdb.connect(":memory:")
        raw.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, cid VARCHAR, url VARCHAR)")
        raw.execute("INSERT INTO logs_svc VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', 'abc', '/p')")
        return InstrumentedDuckDBConnection(raw, service_id="svc")

    def _run(self, con, sql, explain=False):
        with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=self.SALT):
            return execute_query(
                con,
                self.SRC,
                sql,
                max_rows=10,
                want_explain=explain,
                service_id="svc",
                time_filter=self.WINDOW,
                mask_ips=True,
            )

    def test_hash_is_unchanged(self):
        result = self._run(self._con(), "SELECT cid FROM logs")
        assert result["data"][0]["cid"] == hashlib.sha256((self.SALT + "abc").encode()).hexdigest()

    @pytest.mark.parametrize(
        "sql,explain",
        [
            ("SELECT cid FROM logs", True),
            ("SELECT cid::INT FROM logs", False),
            ("SELECT nope FROM logs", False),
            ("SELECT sql FROM duckdb_views()", False),
            ("SELECT * FROM information_schema.views", False),
            ("SELECT * FROM duckdb_temporary_files()", False),
        ],
    )
    def test_no_analyst_visible_channel_carries_the_salt(self, sql, explain):
        try:
            blob = repr(self._run(self._con(), sql, explain))
        except Exception as exc:
            blob = f"{type(exc).__name__}: {exc}"
        assert self.SALT not in blob

    def test_query_registry_does_not_record_the_salt(self):
        con = self._con()
        self._run(con, "SELECT cid FROM logs")
        gc.collect()
        snap = query_registry.snapshot(include_completed=True, full_sql=True, all_active=True)
        blob = json.dumps(snap, default=str)
        assert "_analyst_window_logs" in blob and self.SALT not in blob


# ── Merged from tests/repositories/test_query_cid_salt_not_recorded.py ──
SALT = "SECRETSALT-9f3a1c7e5b"
SVC = "svc"
WINDOW = ("2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z")
SRC = {"name": SVC, "service_id": SVC}


@pytest.fixture
def raw_con():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR, status INT)")
    con.execute(
        "INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4', 200), (now(), 'client-b', '5.6.7.8', 500),"
        " (now(), '', '9.9.9.9', 200), (now(), NULL, '8.8.8.8', 200)"
    )
    yield con
    con.close()


def _analyst_query(con, sql: str, *, want_explain: bool = False) -> dict:
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        return q.execute_query(con, SRC, sql, 100, want_explain, service_id=SVC, mask_ips=True, time_filter=WINDOW)


def _expected_hash(cid: str) -> str:
    return hashlib.sha256((SALT + cid).encode()).hexdigest()


def test_cid_salt_hash_parity_and_redaction_channels(raw_con, tmp_path):
    res = _analyst_query(raw_con, "SELECT cid FROM logs")
    assert sorted((r["cid"] or "") for r in res["data"]) == sorted(
        ["", "", _expected_hash("client-a"), _expected_hash("client-b")]
    )
    assert sum(r["cid"] is None for r in res["data"]) == 1

    legacy = raw_con.execute(f"SELECT sha256(CONCAT('{SALT}', cid)) FROM logs_svc WHERE status = 500").fetchone()[0]
    assert _analyst_query(raw_con, "SELECT cid FROM logs WHERE status = 500")["data"] == [{"cid": legacy}]
    assert {r["ip"] for r in _analyst_query(raw_con, "SELECT ip FROM logs ORDER BY ip")["data"]} == {"[redacted]"}

    con = InstrumentedDuckDBConnection(raw_con, service_id=SVC)
    _analyst_query(con, "SELECT cid, count(*) AS n FROM logs GROUP BY cid")
    texts = json.dumps(
        query_registry.snapshot(since_seq=0, full_sql=True, include_completed=True, all_active=True), default=str
    )
    assert "_analyst_window_logs" in texts and SALT not in texts

    persisted: list[tuple] = []
    with (
        patch("backend.core.query_registry._SLOW_QUERY_PERSIST_THRESHOLD_MS", 0.0),
        patch("backend.core.query_registry._SLOW_QUERY_PERSIST_DISABLED", False),
        patch("backend.core.metadata.insert_slow_query", side_effect=lambda sid, row: persisted.append((sid, row))),
    ):
        _analyst_query(con, "SELECT cid FROM logs")
    assert persisted and SALT not in json.dumps(persisted, default=str)
    assert SALT not in json.dumps(_analyst_query(con, "SELECT cid FROM logs", want_explain=True), default=str)

    raw_con.execute("CREATE TABLE logs_nots (cid VARCHAR, ip VARCHAR)")
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(duckdb.Error) as exc:
            q.execute_query(
                con,
                {"name": "nots", "service_id": SVC},
                "SELECT cid FROM logs",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )
    assert SALT not in str(exc.value)

    leftovers = raw_con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE temporary AND table_name != 'logs_nots' UNION ALL "
        "SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal"
    ).fetchall()
    assert leftovers == []

    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(PermissionError):
            q.execute_query(
                raw_con,
                SRC,
                f"SELECT * FROM {q._ANALYST_CID_SALT_TABLE}",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )
        with pytest.raises(ValueError, match="invalid analyst window bounds"):
            q.execute_query(
                raw_con,
                SRC,
                "SELECT cid FROM logs",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=("not-a-date", "also-not"),
            )
    assert raw_con.execute("SELECT count(*) FROM duckdb_tables() WHERE temporary").fetchone()[0] == 0

    path = str(tmp_path / "ro.duckdb")
    seed = duckdb.connect(path)
    seed.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR, status INT)")
    seed.execute("INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4', 200)")
    seed.close()
    ro = duckdb.connect(path, read_only=True)
    try:
        assert _analyst_query(ro, "SELECT cid FROM logs")["data"] == [{"cid": _expected_hash("client-a")}]
    finally:
        ro.close()

    class _FailViewDrop:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            if sql.startswith("DROP VIEW"):
                raise RuntimeError("interrupted")
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    _analyst_query(_FailViewDrop(raw_con), "SELECT cid FROM logs")
    assert (
        raw_con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = '_analyst_cid_salt'").fetchone()[0]
        == 0
    )
