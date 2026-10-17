"""CMCD streaming row models keep every producer's wire bytes (repository + high-scale)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd_aggregates
from backend.models.cmcd import CmcdAggregatesResponse
from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS
from backend.repositories._base import _safe_table
from backend.repositories.cmcd import _response_cache, get_cmcd_aggregates
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs

SECTIONS = (
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "startup_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "session_duration_dist",
)


@pytest.fixture(autouse=True)
def _clear_cache():
    _response_cache.clear()
    yield
    _response_cache.clear()


def _seed(con, src, n=60):
    table = _safe_table(src["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    parts = [f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields]
    parts += [f'"{cf["name"]}" {cf["duckdb_type"]}' for cf in _CMCD_CUSTOM_FIELDS]
    con.execute(f"CREATE TABLE {table} ({', '.join(parts)})")
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["cmcd_sid"] = f"session_{i % 5}"
        log["cmcd_bl"] = 15 + i
        log["cmcd_br"] = 4500 + i * 100
        log["cmcd_bs"] = i % 4 == 0
        log["cmcd_mtp"] = 6000 + i * 200
        log["cmcd_ot"] = ["v", "a"][i % 2]
        log["cmcd_sf"] = "h"
        log["cmcd_su"] = i % 10 == 0
        log["cmcd_tb"] = 5000 + i * 100
        log["cmcd_rtp"] = 4800 + i * 100
        log["cmcd_cid"] = f"content_{i % 3}"
        log["cmcd_dl"] = 50 + i
        log["country"] = "US" if i % 2 == 0 else "GB"
        log["asn"] = 7922 if i % 2 == 0 else 3320
    insert_mock_logs(con, table, logs)


@pytest.mark.parametrize("windowed", [False, True])
def test_repository_wire_identical(in_memory_duckdb, test_service_source, monkeypatch, windowed):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
    now = datetime.now(UTC)
    start = (now - timedelta(hours=3)).isoformat() if windowed else None
    end = now.isoformat() if windowed else None
    res = get_cmcd_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=start,
        end_time=end,
        filters={},
        bucket_seconds=600,
    )
    assert_sections_wire_identical(CmcdAggregatesResponse, res, non_empty=SECTIONS)


def test_repository_unavailable_wire_identical(in_memory_duckdb):
    src = {"name": "cmcd_wire_missing", "service_id": "cmcd_wire_missing"}
    res = get_cmcd_aggregates(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
    assert_sections_wire_identical(CmcdAggregatesResponse, res)


def test_high_scale_wire_identical():
    class _Client:
        def execute(self, sql, params=None):
            return [{"sum(event_count)": 12}]

    service = SimpleNamespace(service_id="svc", client=_Client())
    res = hs_cmcd_aggregates(service, SimpleNamespace(), None, None, None, False)
    assert res["has_data"] is True
    assert_sections_wire_identical(CmcdAggregatesResponse, res)
