from datetime import UTC, datetime

from backend.high_scale.network import network_health, network_quality
from backend.high_scale.registry import HighScaleService

# Window: exactly one hour, default 300s buckets → 13 bucket boundaries.
_START = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
_END = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
_B0 = int(_START.timestamp())
_B1 = _B0 + 300


class _Req:
    """Minimal stand-in for NetworkHealthRequest / NetworkQualityRequest."""

    start_time = _START.isoformat().replace("+00:00", "Z")
    end_time = _END.isoformat().replace("+00:00", "Z")
    bucket_seconds = 300
    top_n = 30
    region_country = "US"


class FakeClient:
    """Routes each ClickHouse query to representative rows by a unique marker."""

    def execute(self, sql, params=None):
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            # Per-(asn, bucket) heatmap inputs. AS7922 is clean; AS15169 is
            # congested + lossy so the computed health score is < 100.
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
                    "asn": 7922,
                    "bucket_ts": _B1,
                    "reqs": 80,
                    "rtt_med_us": 21000.0,
                    "rtt_baseline_us": 18500.0,
                    "rtt_congestion_us": 2500.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1200.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 15169,
                    "bucket_ts": _B0,
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
                },
                {
                    "country": "US",
                    "city": "boston",
                    "lat": 42.36,
                    "lon": -71.06,
                    "metro": "506",
                    "bucket_ts": _B1,
                    "rtt_med_us": 90000.0,
                    "avg_ploss": 0.03,
                    "error_pct": 8.0,
                    "reqs": 60,
                },
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
                },
                {
                    "country": "US",
                    "city": "boston",
                    "region": "Massachusetts",
                    "metro": "506",
                    "avg_ploss": 0.03,
                    "error_pct": 8.0,
                    "total_reqs": 60,
                },
            ]
        if "GROUP BY asn, c_speed" in sql:
            return [
                {"asn": 7922, "c_speed": "fast", "cnt": 150},
                {"asn": 7922, "c_speed": "medium", "cnt": 30},
                {"asn": 15169, "c_speed": "slow", "cnt": 60},
            ]
        if "quantile(0.95)" in sql:
            return [
                {"asn": 7922, "p95": 40000.0, "p99": 60000.0},
                {"asn": 15169, "p95": 180000.0, "p99": 260000.0},
            ]
        # network_quality bar lists: dispatch by the grouped dimension alias.
        if "AS label" in sql:
            return [
                {"value": "US", "label": "US", "rtt_ms": 21.0, "reqs": 160},
            ]
        return []


def _service() -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=None,
    )


def test_network_health_builds_full_clickhouse_payload():
    resp = network_health(_service(), _Req(), _Req.start_time, _Req.end_time)

    # Full bucket axis generated start→end (13 boundaries at 300s over 1h).
    assert resp.buckets
    assert len(resp.buckets) == 13

    # Per-ASN NESTED heatmap (not the flat stub shape).
    assert resp.heatmap
    first = resp.heatmap[0]
    assert set(first.keys()) >= {"asn", "label", "total_reqs", "buckets"}
    assert isinstance(first["buckets"], list) and first["buckets"]
    assert first["asn"] == 7922  # ordered by total_reqs desc

    # Leaderboard carries real per-ASN health + trend + percentiles + speed mix.
    assert resp.leaderboard
    lead = {row["asn"]: row for row in resp.leaderboard}
    assert lead[7922]["health_score_now"] is not None
    assert lead[15169]["health_score_now"] < lead[7922]["health_score_now"]
    assert lead[7922]["p95_rtt_us"] == 40000.0
    assert lead[7922]["c_speed_mix"]

    # World map + interned cities.
    assert resp.map_buckets
    assert resp.cities
    assert any(c.name for c in resp.cities)

    # Metro leaderboard.
    assert resp.metro_leaderboard

    # Summary health score is REAL (computed), never the hardcoded 100.0.
    assert resp.summary is not None
    assert resp.summary.global_health_score != 100.0
    assert resp.summary.total_reqs == 240
    assert resp.summary.worst_asn is not None


def test_network_quality_populates_by_region():
    resp = network_quality(_service(), _Req(), _Req.start_time, _Req.end_time)
    assert resp.by_region
    assert resp.by_country
    assert resp.by_region[0]["rtt_ms"] > 0
