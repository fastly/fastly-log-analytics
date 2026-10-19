"""Session-detail rows keep every producer's wire bytes (live DuckDB and high-scale ClickHouse paths)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale.sessions import sessions_detail as hs_sessions_detail
from backend.models.dashboard import SessionDetailRequest, SessionDetailResponse
from backend.repositories._base import _safe_table
from backend.repositories.sessions import get_session_detail
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs

_START = "2020-01-01T00:00:00+00:00"
_END = "2100-01-01T00:00:00+00:00"


def test_repository_detail_wire_identical_across_every_log_column(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    for log in logs:
        log["ip"] = "203.0.113.7"
    insert_mock_logs(in_memory_duckdb, table, logs)

    res = get_session_detail(in_memory_duckdb, test_service_source, "203.0.113.7", _START, _END)

    raw_ids = {f["id"] for f in LOG_FIELD_CATALOG if f.get("vcl") is not None}
    assert raw_ids <= set(res["columns"]), "fixture must exercise every raw log column"
    kinds = {type(v) for row in res["data"] for v in row.values()}
    assert {str, int, float} <= kinds, "fixture must exercise string, integer and float cells"
    assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data", "columns"))


def test_repository_detail_keeps_booleans_and_nulls(in_memory_duckdb):
    src = {"name": "sess_wire_bool", "service_id": "sess_wire_bool"}
    table = _safe_table(src["name"])
    now = datetime.now(UTC)
    in_memory_duckdb.execute(
        f'CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "flag" BOOLEAN, "n" BIGINT, "x" DOUBLE, "s" VARCHAR)'
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table} VALUES (?, '203.0.113.7', TRUE, 5, 2.0, NULL), (?, '203.0.113.7', FALSE, NULL, 0.5, 'a')",
        [now.isoformat(), (now + timedelta(seconds=1)).isoformat()],
    )
    res = get_session_detail(in_memory_duckdb, src, "203.0.113.7", _START, _END)
    assert [r["flag"] for r in res["data"]] == [True, False]
    assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data",))


def test_high_scale_detail_wire_identical():
    ts = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    def _row(offset: int, url: str, status: int, asn: int | None) -> dict:
        return {
            "ip": "203.0.113.7",
            "ja4": "t13d1516h2_x",
            "timestamp": ts + timedelta(seconds=offset),
            "country": "US",
            "url": url,
            "status": status,
            "method": "GET",
            "protocol": "h2",
            "resp_bytes": 512,
            "tcp_rtt": 12.5,
            "edge": "1",
            "edge_sid": "sid-1",
            "ua": "curl/8",
            "asn": asn,
        }

    class _Client:
        def execute(self, query, params):
            return [_row(0, "/a", 200, 64500), _row(2, "/b", 404, None)]

    service = type("Svc", (), {"client": _Client(), "service_id": "svc"})()
    req = SessionDetailRequest(ip="203.0.113.7")
    res = hs_sessions_detail(service, req, _START, _END, "203.0.113.7", None)  # type: ignore[arg-type]
    assert res["data"][0]["timestamp"].endswith("Z")
    assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data", "columns"))


def test_non_scalar_cells_are_rejected():
    with pytest.raises(ValueError, match="data"):
        SessionDetailResponse.model_validate({"columns": ["a"], "data": [{"a": [1, 2]}]})
