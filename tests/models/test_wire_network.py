"""Network health/quality row models keep every producer's wire bytes (repository + high-scale)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale import network as hs_network
from backend.high_scale.registry import HighScaleService
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.repositories._base import _safe_table
from backend.repositories.network import get_health, get_quality
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=2)
    for i, log in enumerate(logs):
        log["tcp_rtt"] = 20_000 + (i * 997) % 90_000
        log["rtt_min"] = 10_000 + (i * 131) % 8_000
        log["rtt_var"] = 500 + (i * 53) % 5_000
        log["ploss"] = 0.0 if i % 7 else 0.02
        log["asn"] = [7922, 15169, 701][i % 3]
        log["status"] = 200 if i % 9 else 503
        log["pop"] = ["SJC", "IAD", "LAX"][i % 3]
        log["region"] = ["CA", "NY"][i % 2]
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _health(con, src, **kw):
    return get_health(lambda: con, src, None, None, {}, **kw)


def test_repository_health_wire_identical(in_memory_duckdb, test_service_source, monkeypatch):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast", 15169: "Google"})
    res = _health(in_memory_duckdb, test_service_source)
    assert_sections_wire_identical(
        NetworkHealthResponse,
        res,
        non_empty=("heatmap", "leaderboard", "map_buckets", "cities", "metro_leaderboard", "summary", "buckets"),
    )


def test_repository_health_dense_window_wire_identical(in_memory_duckdb, test_service_source):
    _seed(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    res = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        (now - timedelta(hours=3)).isoformat(),
        now.isoformat(),
        {},
        bucket_seconds=600,
        top_n=2,
    )
    assert_sections_wire_identical(NetworkHealthResponse, res, non_empty=("heatmap", "leaderboard"))


def test_repository_quality_wire_identical(in_memory_duckdb, test_service_source, monkeypatch):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
    res = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {}, region_country="US")
    assert_sections_wire_identical(
        NetworkQualityResponse, res, non_empty=("by_country", "by_asn", "by_pop", "scatter", "countries")
    )


def test_repository_unavailable_shapes_wire_identical(in_memory_duckdb):
    src = {"name": "net_wire_missing", "service_id": "net_wire_missing"}
    assert_sections_wire_identical(
        NetworkQualityResponse, get_quality(lambda: in_memory_duckdb, src, None, None, {}, region_country="US")
    )
    assert_sections_wire_identical(NetworkHealthResponse, get_health(lambda: in_memory_duckdb, src, None, None, {}))


_START = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
_END = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
_B0 = int(_START.timestamp())
_B1 = _B0 + 300


class _Req:
    start_time = _START.isoformat().replace("+00:00", "Z")
    end_time = _END.isoformat().replace("+00:00", "Z")
    bucket_seconds = 300
    top_n = 30
    region_country = "US"


class _HsClient:
    def execute(self, sql, params=None):
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": _B0,
                    "reqs": 100,
                    "rtt_med_us": 20000.0,
                    "rtt_baseline_us": 18000.0,
                    "rtt_congestion_us": 2000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 15169,
                    "bucket_ts": _B1,
                    "reqs": 60,
                    "rtt_med_us": 90000.0,
                    "rtt_baseline_us": 30000.0,
                    "rtt_congestion_us": 150000.0,
                    "avg_ploss": 0.03,
                    "rtt_jitter_us": 80000.0,
                    "error_pct": 8.0,
                },
            ]
        if "AS lat" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "lat": 37.33,
                    "lon": -121.89,
                    "metro": "807",
                    "bucket_ts": _B0,
                    "rtt_med_us": 20000.0,
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "reqs": 100,
                }
            ]
        if "AS region" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "region": "California",
                    "metro": "807",
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "total_reqs": 100,
                }
            ]
        if "GROUP BY asn, c_speed" in sql:
            return [{"asn": 7922, "c_speed": "fast", "cnt": 150}, {"asn": 15169, "c_speed": "slow", "cnt": 60}]
        if "quantile(0.95)" in sql:
            return [{"asn": 7922, "p95": 40000.0, "p99": 60000.0}, {"asn": 15169, "p95": 180000.0, "p99": 260000.0}]
        if "AS label" in sql:
            return [{"value": "US", "label": "US", "rtt_ms": 21.0, "reqs": 160}]
        return []


def _service() -> HighScaleService:
    return HighScaleService(service_id="svc", client=_HsClient(), cursor_secret=b"secret", request_watermark=None)


@pytest.mark.parametrize(
    "fn, model", [("network_health", NetworkHealthResponse), ("network_quality", NetworkQualityResponse)]
)
def test_high_scale_wire_identical(monkeypatch, fn, model):
    captured: dict = {}

    def _capture(cls, **kwargs):
        captured.update(kwargs)
        return cls(**kwargs)

    monkeypatch.setattr(model, "with_telemetry", classmethod(_capture))
    getattr(hs_network, fn)(_service(), _Req(), _Req.start_time, _Req.end_time)
    non_empty = (
        ("heatmap", "leaderboard", "map_buckets", "cities", "metro_leaderboard", "summary")
        if fn == "network_health"
        else ("by_country", "by_region")
    )
    assert_sections_wire_identical(model, captured, non_empty=non_empty)
