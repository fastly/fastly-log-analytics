"""High-scale raw logs explorer: filters, visibility fence, columns and failure behaviour."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale.logs import logs_query_endpoint
from backend.models.logs import LogsQueryRequest, LogsQueryResponse

START = "2026-09-15T00:00:00Z"
END = "2026-09-16T00:00:00Z"
TS = datetime(2026, 9, 15, 12, 0, 0, tzinfo=UTC)


class _Client:
    def __init__(self, *, total: int = 2, fail: str | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._total = total
        self._fail = fail

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if self._fail and self._fail in sql:
            raise RuntimeError("clickhouse unavailable")
        if "logs:count" in sql:
            return [{"cnt": self._total}]
        if "logs:columns" in sql:
            return [{"keys": ["status", "pop", "ua", "cookie_session", "ja4"]}]
        if "logs:page" in sql:
            return [
                {
                    "timestamp": TS,
                    "client_ip": "203.0.113.7",
                    "url": "/a",
                    "country": "US",
                    "custom_fields": {"status": "200", "pop": "SJC", "cookie_session": "abc", "ua": "curl"},
                },
                {
                    "timestamp": "2026-09-15 11:59:00.000",
                    "client_ip": "203.0.113.8",
                    "url": "/b",
                    "country": "DE",
                    "custom_fields": {"status": "404", "pop": "IAD"},
                },
            ]
        raise AssertionError(sql)

    def sql(self, marker: str) -> tuple[str, dict]:
        return next((sql, params) for sql, params in self.calls if marker in sql)


def _run(client: _Client, req: LogsQueryRequest | None = None, *, mask_ips: bool = False) -> dict[str, Any]:
    service = SimpleNamespace(service_id="svc", client=client)
    return logs_query_endpoint(service, req or LogsQueryRequest(), START, END, mask_ips)  # type: ignore[arg-type]


def test_filters_are_applied_to_the_count_and_the_page() -> None:
    client = _Client()
    req = LogsQueryRequest(
        filters={
            "country": {"mode": "include", "values": ["US", "CA"]},
            "status": {"mode": "exclude", "values": ["404"]},
        }
    )

    _run(client, req)

    for marker in ("logs:count", "logs:page", "logs:columns"):
        sql, params = client.sql(marker)
        assert "country IN ({flt_0_0:String}, {flt_0_1:String})" in sql
        assert "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
        assert params["flt_0_0"] == "US" and params["flt_0_1"] == "CA"
        assert params["flt_k1"] == "status" and params["flt_1_0"] == "404"


def test_a_hostile_filter_key_is_a_parameter_never_sql_text() -> None:
    client = _Client()
    req = LogsQueryRequest(filters={"status'] = '' OR 1=1 --": {"mode": "include", "values": ["200"]}})

    _run(client, req)

    for sql, _ in client.calls:
        assert "OR 1=1" not in sql and "--" not in sql


def test_filters_the_fact_table_cannot_express_fail_loudly() -> None:
    with pytest.raises(ValueError, match="do not support filtering"):
        _run(_Client(), LogsQueryRequest(filters={"_bot_name": {"mode": "include", "values": ["googlebot"]}}))


def test_every_query_is_fenced_to_visible_published_batches_and_the_window() -> None:
    client = _Client()

    _run(client)

    for sql, params in client.calls:
        assert "service_id={service_id:String}" in sql
        assert "publication_state='visible'" in sql
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert "event_timestamp >= {start_time:DateTime64(3)}" in sql
        assert "event_timestamp <= {end_time:DateTime64(3)}" in sql
        assert params["service_id"] == "svc"


def test_paging_and_sort_direction_are_typed_parameters() -> None:
    client = _Client()

    _run(client, LogsQueryRequest(limit=25, offset=50, sort_dir="asc"))

    sql, params = client.sql("logs:page")
    assert "ORDER BY event_timestamp ASC" in sql
    assert "LIMIT {limit:UInt32} OFFSET {offset:UInt32}" in sql
    assert params["limit"] == 25 and params["offset"] == 50


def test_search_term_is_a_bound_parameter_across_the_searchable_fields() -> None:
    client = _Client()

    _run(client, LogsQueryRequest(search="  /checkout%  "))

    for marker in ("logs:count", "logs:page"):
        sql, params = client.sql(marker)
        assert "url ILIKE {search_term:String}" in sql
        assert params["search_term"] == "%/checkout%%"


def test_total_and_has_more_come_from_the_count_query() -> None:
    result = _run(_Client(total=500), LogsQueryRequest(limit=2, offset=0))

    assert result["total_count"] == 500
    assert result["has_more"] is True
    assert _run(_Client(total=2), LogsQueryRequest(limit=2, offset=0))["has_more"] is False


def test_timestamps_are_iso_utc_whether_the_driver_returns_datetimes_or_strings() -> None:
    result = _run(_Client())

    assert result["rows"][0]["timestamp"] == "2026-09-15T12:00:00Z"
    assert result["rows"][1]["timestamp"] == "2026-09-15T11:59:00.000Z"


def test_columns_are_stable_across_pages_and_include_sampled_custom_fields() -> None:
    result = _run(_Client())

    assert result["columns"] == sorted(
        ["timestamp", "client_ip", "url", "country", "status", "pop", "ua", "cookie_session", "ja4"]
    )
    types = {column["name"]: column["type"] for column in result["schema_columns"]}
    assert types["timestamp"] == "TIMESTAMP"
    assert types["status"] == "VARCHAR"


def test_requested_columns_project_the_rows_and_drop_unknown_names() -> None:
    result = _run(_Client(), LogsQueryRequest(columns=["url", "status", "no_such_column"]))

    assert result["columns"] == ["timestamp", "url", "status"]
    assert set(result["rows"][0]) == {"timestamp", "url", "status"}
    assert result["rows"][0]["status"] == "200"


def test_masking_hides_ips_and_session_identifiers() -> None:
    result = _run(_Client(), mask_ips=True)

    assert result["rows"][0]["client_ip"] != "203.0.113.7"
    assert result["rows"][0]["cookie_session"] == "[redacted]"


def test_clickhouse_failures_propagate_instead_of_reporting_zero_rows() -> None:
    with pytest.raises(RuntimeError, match="unavailable"):
        _run(_Client(fail="logs:count"))


def test_output_validates_against_the_standard_response_model() -> None:
    model = LogsQueryResponse.model_validate(_run(_Client()))

    assert model.total_count == 2 and len(model.rows) == 2
