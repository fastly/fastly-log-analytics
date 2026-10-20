"""High-scale RUM: real environments, error positions, live-event fields and filters."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService
from backend.high_scale.rum import rum_analytics, rum_live_events

START = "2026-09-01T00:00:00Z"
END = "2026-09-01T02:00:00Z"


class _Client:
    def __init__(self, *, live_rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._live_rows = live_rows or []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "rum_analytics_counts" in sql:
            return [{"beacons": 3, "pageviews": 2, "interactions": 0, "errors": 1}]
        if "rum_analytics_environments" in sql:
            return [
                {
                    "env_browser": "Chrome",
                    "env_os": "Unknown",
                    "env_device": "Unknown",
                    "beacons": 5,
                    "g_browser": 0,
                    "g_os": 1,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "Unknown",
                    "env_device": "Unknown",
                    "beacons": 1,
                    "g_browser": 0,
                    "g_os": 1,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "macOS",
                    "env_device": "Unknown",
                    "beacons": 4,
                    "g_browser": 1,
                    "g_os": 0,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "Unknown",
                    "env_device": "Desktop",
                    "beacons": 6,
                    "g_browser": 1,
                    "g_os": 1,
                    "g_device": 0,
                },
            ]
        if "rum_analytics_exceptions" in sql:
            return [
                {"error_message": "boom", "error_file": "app.js", "error_line": 12, "error_col": 34, "count": 4},
                {"error_message": "oops", "error_file": "", "error_line": 0, "error_col": 0, "count": 1},
            ]
        if "rum_live_events" in sql:
            return self._live_rows
        return []

    def sql(self, marker: str) -> tuple[str, dict[str, Any]]:
        return next((sql, params) for sql, params in self.calls if marker in sql)


def _service(client: Any) -> HighScaleService:
    return HighScaleService(
        "TestRum", client, b"secret", ServingWatermark("TestRum", "request", 0, None, None, None, None, None, True)
    )


def test_environments_are_counted_per_distinct_beacon_from_the_vitals_facts() -> None:
    client = _Client()

    result = rum_analytics(_service(client), START, END)

    assert result["environments"] == {
        "browsers": {"Chrome": 5, "Unknown": 1},
        "os": {"macOS": 4},
        "devices": {"Desktop": 6},
    }
    sql, _ = client.sql("rum_analytics_environments")
    assert "FROM rum_vitals_facts" in sql
    assert "GROUPING SETS" in sql and "uniqExact(" in sql
    assert "if(browser = '', 'Unknown', browser)" in sql


def test_blank_environment_values_are_reported_as_unknown_like_the_standard_path() -> None:
    class Blank(_Client):
        def execute(self, sql, params=None):
            if "rum_analytics_environments" in sql:
                return [
                    {
                        "env_browser": "Unknown",
                        "env_os": "Unknown",
                        "env_device": "Unknown",
                        "beacons": 2,
                        "g_browser": 0,
                        "g_os": 1,
                        "g_device": 1,
                    }
                ]
            return super().execute(sql, params)

    assert rum_analytics(_service(Blank()), START, END)["environments"]["browsers"] == {"Unknown": 2}


def test_top_exceptions_carry_their_real_line_and_column() -> None:
    result = rum_analytics(_service(_Client()), START, END)

    assert result["errors"] == [
        {"message": "boom", "file": "app.js", "line": 12, "col": 34, "count": 4},
        {"message": "oops", "file": "unknown.js", "line": 0, "col": 0, "count": 1},
    ]
    client = _Client()
    rum_analytics(_service(client), START, END)
    sql, _ = client.sql("rum_analytics_exceptions")
    assert "error_line" in sql and "error_col" in sql
    assert "GROUP BY error_message, error_file, error_line, error_col" in sql


def test_filters_apply_to_every_analytics_query() -> None:
    client = _Client()

    rum_analytics(
        _service(client),
        START,
        END,
        filters={
            "browser": {"mode": "include", "values": ["Chrome", "Firefox"]},
            "country": {"mode": "exclude", "values": ["RU"]},
        },
    )

    data_queries = [(sql, params) for sql, params in client.calls if "rum_" in sql and "any_data" not in sql]
    assert len(data_queries) >= 5
    for sql, params in data_queries:
        assert "browser IN ({flt_0_0:String}, {flt_0_1:String})" in sql
        assert "country NOT IN ({flt_1_0:String})" in sql
        assert params["flt_0_0"] == "Chrome" and params["flt_0_1"] == "Firefox" and params["flt_1_0"] == "RU"


def test_filters_rum_facts_cannot_express_fail_loudly() -> None:
    with pytest.raises(ValueError, match="cannot filter on"):
        rum_analytics(_service(_Client()), START, END, filters={"metric_name": {"mode": "include", "values": ["LCP"]}})


def test_blank_filters_are_ignored() -> None:
    client = _Client()

    rum_analytics(_service(client), START, END, filters={"browser": {"mode": "include", "values": []}})

    for sql, _ in client.calls:
        assert "browser IN" not in sql


LIVE_ROWS = [
    {
        "timestamp": datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        "type": "pageview",
        "pathname": "/checkout",
        "metric_name": "LCP",
        "metric_value": 2400.0,
        "metric_rating": "good",
        "browser": "Firefox",
        "os": "Linux",
        "device": "Desktop",
        "cid": "client-1",
        "req_id": "req-1",
        "error_message": "",
        "city": "Berlin",
        "region": "BE",
        "country": "DE",
        "pop": "FRA",
        "tls": "1.3",
        "ttfb": 0.042,
    },
    {
        "timestamp": "2026-09-01 01:29:00.000",
        "type": "error",
        "pathname": "",
        "metric_name": "",
        "metric_value": None,
        "metric_rating": "",
        "browser": "",
        "os": "",
        "device": "",
        "cid": "client-2",
        "req_id": "",
        "error_message": "TypeError: x is undefined",
        "city": "",
        "region": "",
        "country": "",
        "pop": "",
        "tls": "",
        "ttfb": None,
    },
]


def test_live_events_carry_the_beacon_environment_instead_of_placeholders() -> None:
    client = _Client(live_rows=LIVE_ROWS)

    events = rum_live_events(_service(client), START, END, 50)

    first = events[0]
    assert first["type"] == "pageview" and first["path"] == "/checkout"
    assert first["browser"] == "Firefox" and first["os"] == "Linux"
    assert first["desc"] == "Metric LCP: 2400.0 (GOOD)"
    assert first["time"] == "2026-09-01T01:30:00Z"
    raw = first["raw_log"]
    assert raw["meta"] == {
        "browser": {"name": "Firefox"},
        "os": {"name": "Linux"},
        "device": {"type": "Desktop"},
        "page": {"url": "/checkout"},
    }
    assert (raw["city"], raw["region"], raw["country"], raw["pop"], raw["tls"]) == ("Berlin", "BE", "DE", "FRA", "1.3")
    assert raw["ttfb"] == 42
    assert raw["cid"] == "client-1" and raw["req_id"] == "req-1"
    assert raw["measurements"] == [{"type": "web-vitals", "values": {"LCP": 2400.0}, "context": {"rating": "good"}}]


def test_live_error_events_report_unknown_only_for_genuinely_missing_values() -> None:
    events = rum_live_events(_service(_Client(live_rows=LIVE_ROWS)), START, END, 50)

    error = events[1]
    assert error["type"] == "error" and error["desc"] == "TypeError: x is undefined"
    assert error["time"] == "2026-09-01T01:29:00.000Z"
    assert error["browser"] == "Unknown" and error["path"] == "/"
    raw = error["raw_log"]
    assert raw["exceptions"] == [{"type": "Error", "value": "TypeError: x is undefined"}]
    assert raw["city"] == "Unknown" and raw["ttfb"] is None


def test_live_events_query_is_bounded_visible_and_parameterised() -> None:
    client = _Client(live_rows=[])

    rum_live_events(_service(client), START, END, 25, filters={"os": {"mode": "include", "values": ["Linux"]}})

    sql, params = client.sql("rum_live_events")
    assert "FROM rum_vitals_facts" in sql and "FROM rum_error_facts" in sql
    assert "publication_state='visible'" in sql
    assert "high_scale_batch_publications FINAL" in sql
    assert "domain='rum_vitals'" in sql and "domain='rum_errors'" in sql
    # ClickHouse binds a trailing ORDER BY/LIMIT to the last SELECT only, so the union must be wrapped.
    assert "SELECT * FROM (" in sql and sql.index("SELECT * FROM (") < sql.index("UNION ALL")
    assert sql.index("UNION ALL") < sql.index(")\n    ORDER BY timestamp DESC")
    assert "LIMIT {limit:UInt32}" in sql and params["limit"] == 25
    assert sql.count("os IN ({flt_0_0:String})") == 2 and params["flt_0_0"] == "Linux"


def test_live_event_query_failures_propagate() -> None:
    class Broken(_Client):
        def execute(self, sql, params=None):
            raise RuntimeError("clickhouse unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        rum_live_events(_service(Broken()), START, END, 50)
