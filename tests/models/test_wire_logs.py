"""Raw-log rows keep every producer's wire bytes (live DuckDB and high-scale ClickHouse paths)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale.logs import logs_query_endpoint as hs_logs_query
from backend.models.logs import LogsQueryRequest, LogsQueryResponse
from backend.repositories._base import _safe_table
from backend.repositories.logs import query_logs
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_repository_rows_wire_identical_across_every_log_column(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table, generate_mock_logs(test_service_source, num_logs=40, hours_ago=1))

    res = query_logs(in_memory_duckdb, test_service_source, None, None, {}, limit=40)

    raw_ids = {f["id"] for f in LOG_FIELD_CATALOG if f.get("vcl") is not None}
    assert raw_ids <= set(res["columns"]), "fixture must exercise every raw log column"
    kinds = {type(v) for row in res["rows"] for v in row.values()}
    assert {str, int, float} <= kinds, "fixture must exercise string, integer and float cells"
    assert_sections_wire_identical(LogsQueryResponse, res, non_empty=("rows", "columns", "schema_columns"))


def test_repository_rows_keep_booleans_nulls_and_masking(in_memory_duckdb):
    src = {"name": "logs_wire_bool", "service_id": "logs_wire_bool"}
    table = _safe_table(src["name"])
    now = datetime.now(UTC)
    in_memory_duckdb.execute(
        f'CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, "client_ip" VARCHAR, "flag" BOOLEAN, "n" BIGINT, "x" DOUBLE)'
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table} VALUES (?, '203.0.113.7', TRUE, 5, 2.0), (?, NULL, FALSE, NULL, 0.5)",
        [now.isoformat(), (now - timedelta(seconds=1)).isoformat()],
    )
    res = query_logs(in_memory_duckdb, src, None, None, {}, mask_ips=True)
    assert [r["flag"] for r in res["rows"]] == [True, False]
    assert res["rows"][0]["client_ip"] != "203.0.113.7"
    assert_sections_wire_identical(LogsQueryResponse, res, non_empty=("rows",))


def test_high_scale_rows_wire_identical():
    ts = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    class _Client:
        def execute(self, query, params):
            if "count()" in query:
                return [{"cnt": 2}]
            return [
                {
                    "timestamp": ts,
                    "client_ip": "203.0.113.7",
                    "url": "/a",
                    "country": "US",
                    "custom_fields": {"status": "200", "pop": "SJC", "request_method": "GET"},
                },
                {
                    "timestamp": ts + timedelta(seconds=1),
                    "client_ip": "203.0.113.8",
                    "url": "/b",
                    "country": "DE",
                    "custom_fields": {"status": "404", "pop": "IAD"},
                },
            ]

    service = type("Svc", (), {"client": _Client(), "service_id": "svc"})()
    res = hs_logs_query(service, LogsQueryRequest(), None, None, mask_ips=False)  # type: ignore[arg-type]
    assert res["rows"][0]["timestamp"].endswith("Z")
    assert_sections_wire_identical(LogsQueryResponse, res, non_empty=("rows", "columns", "schema_columns"))


def test_non_scalar_cells_are_rejected():
    with pytest.raises(ValueError, match="rows"):
        LogsQueryResponse.model_validate({"rows": [{"a": {"nested": 1}}]})
