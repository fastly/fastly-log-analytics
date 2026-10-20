"""CMCD streaming row models keep every producer's wire bytes (repository + high-scale)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd_aggregates
from backend.models.cmcd import CmcdAggregatesResponse, CmcdRequest
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


def test_high_scale_wire_identical(monkeypatch):
    monkeypatch.setattr("backend.high_scale.cmcd._asn_names", lambda sid, asns: {7922: "Comcast"})
    present = {"events": 9, "sessions": 3}
    present.update(
        {f"has_{key}": 1 for key in ("bl", "br", "bs", "mtp", "ot", "sf", "su", "tb", "cid", "country", "asn")}
    )
    bucket = "2026-09-14 10:05:00"

    class _Client:
        def execute(self, sql, params=None):
            if "cmcd:probe" in sql:
                return [present]
            if "cmcd:overview_peak" in sql:
                return [{"peak_viewers": 3}]
            if "cmcd:overview_duration" in sql:
                return [{"avg_duration": 90.0}]
            if "cmcd:overview" in sql:
                return [
                    {
                        "active_sessions": 3,
                        "rebuffer_rate": 33.3,
                        "avg_bitrate": 4000.0,
                        "avg_buffer_length": 20.0,
                        "median_throughput": 7000.0,
                    }
                ]
            if "cmcd:timeseries" in sql:
                return [
                    {
                        "bucket": bucket,
                        "concurrent_sessions": 3,
                        "rebuffer_session_pct": 33.3,
                        "p50_buffer": 10.0,
                        "p95_buffer": 20.0,
                        "starvation_rate": 5.0,
                        "avg_bitrate": 4000.0,
                        "utilization_ratio": 0.5,
                        "p50_throughput": 7000.0,
                        "p95_throughput": 9000.0,
                        "p99_throughput": 9500.0,
                        "startup_ratio": 2.0,
                    }
                ]
            if "cmcd:new_sessions" in sql:
                return [{"bucket": bucket, "new_sessions": 3}]
            if "cmcd:top_content" in sql:
                return [
                    {"content_id": "c1", "session_count": 3, "rebuffer_rate": 10.0, "avg_br": 4000.0, "avg_bl": 9.0}
                ]
            if "cmcd:rebuffer_by_country" in sql:
                return [{"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_mtp": 7000.0}]
            if "cmcd:rebuffer_by_asn" in sql:
                return [{"asn": 7922, "rebuffer_rate": 10.0, "session_count": 3, "avg_br": 4000.0, "avg_bl": 9.0}]
            if "cmcd:object_type_dist" in sql:
                return [{"object_type": "v", "request_count": 5}]
            if "cmcd:streaming_format_dist" in sql:
                return [{"streaming_format": "h", "session_count": 3}]
            if "cmcd:session_duration_dist" in sql:
                return [{"sort_order": 3, "session_count": 3}]
            raise AssertionError(sql)

    service = SimpleNamespace(service_id="svc", client=_Client())
    res = hs_cmcd_aggregates(
        service, CmcdRequest(bucket_seconds=300), "2026-09-14T10:00:00Z", "2026-09-14T10:10:00Z", None, False
    )
    assert res["has_data"] is True
    assert_sections_wire_identical(
        CmcdAggregatesResponse,
        res,
        non_empty=("overview",) + tuple(section for section in SECTIONS if section != "overview"),
    )
