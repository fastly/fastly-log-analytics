"""Phase 4 /dashboard performance & security audit golden and regression tests."""

from __future__ import annotations

import ipaddress
import json as _json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from backend.high_scale.registry_bootstrap import HighScaleService

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
from backend.core.share_db.validation import apply_pii_policy
from backend.high_scale import dashboard as high_scale_dashboard
from backend.high_scale.archive_models import ServingWatermark
from backend.models.common import FilterSpec
from backend.models.dashboard import AggregatesRequest, FieldValuesRequest, RawRequest
from backend.repositories import _base as repo_base
from backend.repositories import dashboard as dash
from backend.repositories import rollup_coverage
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
    empty_res = get_aggregates(
        con=in_memory_duckdb,
        src=_src_without_table(),
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
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
    res = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])}
        if metric != "gibberish_not_a_metric"
        else {},
        chart_interval="1 minute",
        chart_metric=metric,
    )
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
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start_time,
        end_time=end_time,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert rebuild_calls["n"] == 1 and result["total_rows"] == 30 and result["data"]["url"]["total"] > 0

    # Case (b): outside window -> no rebuild
    rebuild_calls["n"] = 0
    old_start = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old_end = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    res_outside = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=old_start,
        end_time=old_end,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert rebuild_calls["n"] == 0 and res_outside["total_rows"] == 0

    # Case (c): filters applied -> no rebuild
    res_filtered = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start_time,
        end_time=end_time,
        filters={"country": FilterSpec(mode="include", values=["ZZ"])},
        chart_interval="1 minute",
        chart_metric="requests",
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
        url_entries = [("url", "/page1", 50), ("url", "/page2", 30)]
        return country_entries + url_entries, list(fields)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", spy_top_n)
    result = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert len(calls) == 1
    fields_called, limit_called, pfl_called = calls[0]
    assert "country" in fields_called and pfl_called.get("country") == 500 and limit_called == 10
    assert len(result["data"]["country"]["top"]) == 10 and len(result["map_data"]) == 12

    # topten only
    calls.clear()
    res_topten = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
        include_time_series=False,
        include_conn_requests=False,
        include_map_data=False,
        include_top_n=True,
    )
    assert len(calls) == 1 and len(calls[0][0]) > 5
    assert res_topten["data"]["url"]["top"] and res_topten["time_series"] == [] and res_topten["map_data"] == []

    # core only with map enabled -> 1 call; with map disabled -> 0 calls
    calls.clear()
    dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
        include_time_series=True,
        include_conn_requests=True,
        include_map_data=True,
        include_top_n=False,
    )
    assert len(calls) == 1
    calls.clear()
    dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
        include_time_series=True,
        include_conn_requests=True,
        include_map_data=False,
        include_top_n=False,
    )
    assert len(calls) == 0


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
        get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="\";'''",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )

    assert (
        get_field_values(
            in_memory_duckdb,
            _src_without_table(),
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )["values"]
        == []
    )
    assert (
        get_field_values(
            in_memory_duckdb,
            _src_without_table(),
            field="_bot_name",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )["values"]
        == []
    )

    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 20 else "GB"
        log["waf_sig"] = "SQLI,XSS" if i % 2 == 0 else "SQLI"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    for bad_field in ('"; DROP TABLE foo', "completely_unknown_field"):
        with pytest.raises(LookupError):
            get_field_values(
                in_memory_duckdb,
                test_service_source,
                field=bad_field,
                search="",
                limit=10,
                start_time=None,
                end_time=None,
                filters={},
            )

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={"country": FilterSpec(mode="include", values=["US"])},
        )
        vals = {v["value"]: v["count"] for v in out["values"]}
        assert vals["US"] == 20 and vals["GB"] == 10

        out_united = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="united",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert {"US", "GB"} <= {v["value"] for v in out_united["values"]}

        out_waf = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="waf_sig_ind",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        waf_vals = {v["value"]: v["count"] for v in out_waf["values"]}
        assert waf_vals == {"SQLI": 30, "XSS": 15}

        out_waf_search = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="waf_sig_ind",
            search="SQL",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert {v["value"] for v in out_waf_search["values"]} == {"SQLI"}

        # Cache hit path
        (tmp_path / "top_values.json").write_text(_json.dumps({"country": [{"value": "ZZ", "count": 999}]}))
        out_cached = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="country",
            search="",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
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
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="microsoft",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert out["values"][0]["value"] == "bingbot" and out["values"][0]["count"] == 10
        out_miss = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="_bot_name",
            search="google",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert out_miss["values"] == []

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", return_value=[13335]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out_asn = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="cloudflare",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert {v["value"] for v in out_asn["values"]} == {13335}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", side_effect=RuntimeError("db locked")),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out_fallback = get_field_values(
            in_memory_duckdb,
            test_service_source,
            field="asn",
            search="133",
            limit=10,
            start_time=None,
            end_time=None,
            filters={},
        )
        assert {v["value"] for v in out_fallback["values"]} == {13335}


def test_get_aggregates_rollup_temp_avoidance_virtual_fields_and_conn_requests_fallback(
    in_memory_duckdb, test_service_source, monkeypatch
):
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=4)
    insert_mock_logs(in_memory_duckdb, table_name, logs)
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
        con=in_memory_duckdb,
        src=test_service_source,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-02T00:00:00Z",
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert temp_creates == [] and result["data"]["conn_requests"] == sentinel
    assert "waf_sig_ind" in virtual_calls and result["data"]["waf_sig_ind"] == {"top": [], "total": 0}

    # Fallback when try_conn_requests_hist_from_rollup returns None
    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", lambda self, s, e, **kw: None)
    st = (datetime.now(UTC) - timedelta(hours=26)).isoformat()
    et = (datetime.now(UTC) + timedelta(hours=26)).isoformat()
    res_fb = dash.get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=st,
        end_time=et,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        chart_interval="1 minute",
        chart_metric="requests",
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
            con=in_memory_duckdb,
            src=test_service_source,
            start_time=None,
            end_time=None,
            filters={} if use_rollups else {"status": FilterSpec(mode="include", values=["200"])},
            chart_interval="1 minute",
            chart_metric="requests",
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
    assert time.perf_counter() - t0 < 0.5
    assert (
        "openai-gptbot" in str(df.loc[0, "_bot_name"])
        and "IP-Match" in str(df.loc[1, "_bot_name"])
        and df.loc[2, "_bot_name"] == "null"
    )
