"""Parity tests for High-Scale /dashboard Top-10 dimension cards and drill-down filters."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale._common import request_filter_clauses
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.dashboard import aggregates, field_values
from backend.high_scale.ingest_controller import HighScaleService, compute_dimension_counts
from backend.models.analytics import AggregatesRequest, FieldValuesRequest
from backend.models.common import FilterSpec

pytestmark = pytest.mark.no_postgres

_START = "2026-09-15T00:00:00Z"
_END = "2026-09-15T01:00:00Z"


def _watermark() -> ServingWatermark:
    return ServingWatermark(
        service_id="svc-cards",
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 9, 14, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )


def test_compute_dimension_counts_normalizes_values_and_splits_waf_sig_ind() -> None:
    rows = (
        {
            "timestamp": "2026-09-15T00:00:10Z",
            "url": "/a",
            "country": "US",
            "ip": "1.1.1.1",
            "status": 200,
            "ttl": 120.077,
            "age": 0.4,
            "conn_requests": 3,
            "edge": True,
            "is_ipv6": False,
            "waf": None,
            "waf_resp": None,
            "c_speed": "B",
            "p_type": "H",
            "p_desc": "c",
            "waf_sig": "SQLI, XSS , SQLI",
        },
        {
            "timestamp": "2026-09-15T00:00:20Z",
            "url": "/b",
            "country": "CA",
            "ip": "1.1.1.2",
            "status": 404,
            "ttl": None,
            "age": "",
            "conn_requests": 25,
            "edge": False,
            "is_ipv6": True,
            "waf": "True",
            "waf_resp": 406,
            "c_speed": "broadband",
            "p_type": "",
            "p_desc": None,
            "waf_sig": "",
        },
    )
    by_dim_val: dict[tuple[str, str], int] = {}
    for item in compute_dimension_counts("request", rows):
        key = (item["dimension"], item["value"])
        by_dim_val[key] = by_dim_val.get(key, 0) + item["count"]

    assert by_dim_val[("ttl", "120")] == 1
    assert by_dim_val[("age", "0")] == 1
    assert by_dim_val[("conn_requests", "2\u20135")] == 1
    assert by_dim_val[("conn_requests", "21+")] == 1
    assert by_dim_val[("edge", "true")] == 1 and by_dim_val[("edge", "false")] == 1
    assert by_dim_val[("waf", "true")] == 1
    assert by_dim_val[("c_speed", "broadband")] == 2
    assert by_dim_val[("p_type", "hosting")] == 1
    assert by_dim_val[("p_desc", "cloud")] == 1
    assert by_dim_val[("waf_sig_ind", "SQLI")] == 2
    assert by_dim_val[("waf_sig_ind", "XSS")] == 1
    assert ("status", "200") in by_dim_val
    for dim, val in by_dim_val:
        assert val not in ("", "None", "null"), f"unexpected sentinel value {(dim, val)!r}"


def test_unfiltered_dashboard_aggregates_normalizes_legacy_rows_enriches_asn_and_derives_waf_sig_ind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_rows: dict[str, list[tuple[str, int]]] = {
        "url": [("/a", 10)],
        "ttl": [("None", 4), ("120.077", 3), ("120.2", 3)],
        "age": [("", 5), ("0.4", 5)],
        "conn_requests": [("1", 5), ("2", 3), ("4", 2), ("10", 2)],
        "c_speed": [("None", 2), ("B", 4), ("broadband", 4)],
        "p_type": [("None", 6), ("H", 4)],
        "p_desc": [("", 7), ("c", 3)],
        "edge": [("True", 6), ("true", 2), ("False", 2)],
        "asn": [("15169", 7), ("None", 3)],
        "waf_sig": [("SQLI, XSS", 4), ("None", 6)],
        "waf_sig_ind": [],
    }
    captured_limits: list[dict[str, int] | None] = []

    def _fake_batch(client, *, dimensions, limits=None, **kwargs):
        captured_limits.append(dict(limits) if limits else None)
        out = {}
        for d in dimensions:
            entries = legacy_rows.get(d, [])
            tot = sum(c for _, c in entries)
            out[d] = SimpleNamespace(top_values=tuple(entries), request_count=tot)
        return out

    monkeypatch.setattr("backend.high_scale.dashboard.query_clickhouse_aggregates_batch", _fake_batch)
    monkeypatch.setattr(
        "backend.core.fos_client.get_asn_names",
        lambda svc_id, asns: {15169: "Google LLC"},
    )

    svc = HighScaleService(
        service_id="svc-cards",
        client=SimpleNamespace(execute=lambda sql, params=None: []),
        cursor_secret=b"secret",
        request_watermark=_watermark(),
    )
    resp = aggregates(
        svc,
        AggregatesRequest(
            fields=["ttl", "age", "conn_requests", "c_speed", "p_type", "p_desc", "edge", "asn", "waf_sig_ind"],
            include_time_series=False,
            include_map_data=False,
        ),
        _START,
        _END,
    )
    assert resp.data["ttl"].total == 6
    assert [(e.value, e.count) for e in resp.data["ttl"].top] == [(120, 6)]
    assert resp.data["age"].total == 5
    assert [(e.value, e.count) for e in resp.data["age"].top] == [(0, 5)]
    assert [(e.value, e.count) for e in resp.data["conn_requests"].top] == [
        ("1", 5),
        ("2\u20135", 5),
        ("6\u201320", 2),
    ]
    assert resp.data["c_speed"].total == 8
    assert [(e.value, e.count) for e in resp.data["c_speed"].top] == [("broadband", 8)]
    assert resp.data["p_type"].total == 4
    assert [(e.value, e.count) for e in resp.data["p_type"].top] == [("hosting", 4)]
    assert resp.data["p_desc"].total == 3
    assert [(e.value, e.count) for e in resp.data["p_desc"].top] == [("cloud", 3)]
    assert [(e.value, e.count) for e in resp.data["edge"].top] == [(True, 8), (False, 2)]
    assert resp.data["asn"].total == 7
    assert [(e.value, e.count, e.label) for e in resp.data["asn"].top] == [(15169, 7, "Google LLC (15169)")]
    assert Counter((e.value, e.count) for e in resp.data["waf_sig_ind"].top) == Counter({("SQLI", 4), ("XSS", 4)})
    assert captured_limits and "ttl" in (captured_limits[0] or {})


def test_filtered_dashboard_aggregates_and_field_values_waf_sig_ind_and_ttl_null_guard() -> None:
    calls: list[str] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            calls.append(sql)
            if "SELECT count() AS c FROM request_facts" in sql:
                return [{"c": 10}]
            if "splitByChar(',', custom_fields['waf_sig'])" in sql:
                return [{"v": "SQLI", "c": 4, "nn_total": 8}, {"v": "XSS", "c": 4, "nn_total": 8}]
            if "AS v, count() AS c" in sql:
                return [{"v": "120", "c": 3, "nn_total": 3}]
            return []

    svc = HighScaleService(
        service_id="svc-cards",
        client=_Client(),
        cursor_secret=b"secret",
        request_watermark=_watermark(),
    )
    resp = aggregates(
        svc,
        AggregatesRequest(
            fields=["ttl", "waf_sig_ind"],
            filters={"country": FilterSpec(mode="include", values=["US"])},
            include_time_series=False,
            include_map_data=False,
        ),
        _START,
        _END,
    )
    ttl_sql = next(s for s in calls if "custom_fields['ttl']" in s and "AS v" in s)
    assert "toFloat64OrNull" in ttl_sql and "toFloat64OrZero" not in ttl_sql
    assert resp.data["ttl"].total == 3
    assert [(e.value, e.count) for e in resp.data["ttl"].top] == [(120, 3)]
    assert resp.data["waf_sig_ind"].total == 8
    assert [(e.value, e.count) for e in resp.data["waf_sig_ind"].top] == [("SQLI", 4), ("XSS", 4)]

    fv = field_values(
        svc,
        FieldValuesRequest(field="waf_sig_ind", search="SQL"),
        _START,
        _END,
    )
    assert [(v.value, v.count) for v in fv.values] == [("SQLI", 4), ("XSS", 4)]


def test_request_filter_clauses_supports_conn_requests_buckets_and_ttl_zero_guard() -> None:
    inc_clauses, inc_params = request_filter_clauses(
        {"conn_requests": {"mode": "include", "values": ["2\u20135", "21+"]}}
    )
    assert len(inc_clauses) == 1
    assert "BETWEEN 2 AND 5" in inc_clauses[0] and ">= 21" in inc_clauses[0]
    assert inc_params["flt_k0"] == "conn_requests"

    exc_clauses, _ = request_filter_clauses({"conn_requests": {"mode": "exclude", "values": ["6-20"]}})
    assert len(exc_clauses) == 1
    assert "BETWEEN 6 AND 20" in exc_clauses[0] and "NOT" in exc_clauses[0]

    ttl0_clauses, _ = request_filter_clauses({"ttl": {"mode": "include", "values": [0]}})
    assert len(ttl0_clauses) == 1 and "!= ''" in ttl0_clauses[0]


def _normalize_sql_shape(sql: str) -> str:
    import re

    return re.sub(r"\{[^}]+\}", "?", re.sub(r"\s+", " ", sql)).strip()


def test_aggregates_batch_custom_limits_batched_without_duplicates_minute_path() -> None:
    from datetime import UTC, datetime

    from backend.high_scale.aggregate_query import query_clickhouse_aggregates_batch
    from backend.high_scale.archive import ServingWatermark

    calls: list[str] = []

    class _MinuteClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append(sql)
            if "request_aggregates_1h" in sql or "max(" in sql.lower() or "min(" in sql.lower():
                return []
            if "AS total_count" in sql and "GROUP BY dimension" in sql:
                return [
                    {"dimension": "url", "total_count": 100},
                    {"dimension": "country", "total_count": 200},
                    {"dimension": "edge", "total_count": 50},
                    {"dimension": "hits", "total_count": 80},
                ]
            rows: list[dict] = []
            for i in range(15):
                rows.append({"dimension": "url", "value": f"/p{i}", "aggregate_count": 100 - i})
            for i in range(15):
                rows.append({"dimension": "country", "value": f"C{i:02d}", "aggregate_count": 200 - i})
            for i in range(12):
                rows.append({"dimension": "edge", "value": f"E{i}", "aggregate_count": 50 - i})
            for i in range(11):
                rows.append({"dimension": "hits", "value": str(i), "aggregate_count": 80 - i})
            return rows

    wm = ServingWatermark(
        "svc-batch",
        "request",
        1,
        datetime(2026, 10, 23, 0, 0, tzinfo=UTC),
        datetime(2026, 10, 24, 1, 0, tzinfo=UTC),
        "cursor",
        "evt-1",
        "evt-1",
        True,
    )
    res = query_clickhouse_aggregates_batch(
        _MinuteClient(),  # type: ignore[arg-type]
        service_id="svc-batch",
        domain="request",
        dimensions=["url", "country", "edge", "hits"],
        start=datetime(2026, 10, 24, 0, 0, tzinfo=UTC),
        end=datetime(2026, 10, 24, 1, 0, tzinfo=UTC),
        watermark=wm,
        limits={"country": 250, "edge": 16, "hits": 64},
    )

    agg_calls = [c for c in calls if "request_aggregates" in c and "request_aggregates_1h" not in c]
    assert len(agg_calls) == 2
    shapes = [_normalize_sql_shape(c) for c in agg_calls]
    assert len(shapes) == len(set(shapes))
    top_sql = next(c for c in agg_calls if "aggregate_count" in c)
    assert "row_number()" in top_sql and "CASE dimension" in top_sql
    assert len(res["url"].top_values) == 10
    assert len(res["country"].top_values) == 15
    assert len(res["edge"].top_values) == 12
    assert len(res["hits"].top_values) == 11
    assert res["country"].request_count == 200
    assert res["edge"].request_count == 50


def test_aggregates_batch_custom_limits_batched_without_duplicates_hourly_path(monkeypatch) -> None:
    from datetime import UTC, datetime

    import backend.high_scale.aggregate_query as aq
    from backend.high_scale.archive import ServingWatermark

    start = datetime(2026, 10, 23, 0, 0, tzinfo=UTC)
    end = datetime(2026, 10, 24, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(aq, "can_use_hourly_rollup", lambda *_args, **_kwargs: (start, end))

    calls: list[str] = []

    class _HourlyClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append(sql)
            if "UNION ALL (SELECT" in sql or "LIMIT 500)" in sql:
                return [
                    {"dimension": "ttl", "value": str(i), "aggregate_count": 500 - i, "total_count": 900}
                    for i in range(14)
                ] + [
                    {"dimension": "age", "value": str(i), "aggregate_count": 400 - i, "total_count": 850}
                    for i in range(12)
                ]
            return [
                {"dimension": "url", "value": f"/u{i}", "aggregate_count": 100 - i, "total_count": 1000}
                for i in range(15)
            ] + [
                {"dimension": "country", "value": f"C{i:02d}", "aggregate_count": 200 - i, "total_count": 1000}
                for i in range(18)
            ]

    wm = ServingWatermark(
        "svc-hourly",
        "request",
        1,
        start,
        end,
        "cursor",
        "evt-1",
        "evt-1",
        True,
    )
    res = aq.query_clickhouse_aggregates_batch(
        _HourlyClient(),  # type: ignore[arg-type]
        service_id="svc-hourly",
        domain="request",
        dimensions=["url", "country", "ttl", "age"],
        start=start,
        end=end,
        watermark=wm,
        limits={"country": 250, "ttl": 500, "age": 500},
    )

    assert len(calls) == 2
    shapes = [_normalize_sql_shape(c) for c in calls]
    assert len(shapes) == len(set(shapes))
    heavy_sql = next(c for c in calls if "LIMIT 500)" in c)
    fast_sql = next(c for c in calls if "CASE dimension" in c)
    assert "sum(sum(request_count)) OVER () AS total_count" in heavy_sql
    assert "row_number()" in fast_sql
    assert len(res["url"].top_values) == 10
    assert len(res["country"].top_values) == 18
    assert len(res["ttl"].top_values) == 14
    assert len(res["age"].top_values) == 12
    assert res["ttl"].request_count == 900
    assert res["age"].request_count == 850
