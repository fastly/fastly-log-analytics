from copy import deepcopy

from backend.high_scale.origin_projection import build_origin_projection_rows


def test_builds_minute_summary_and_dimension_rows_without_mutating_events():
    events = (
        {
            "timestamp": "2026-09-16T14:00:01Z",
            "cache": "MISS",
            "ost": 200,
            "ottfb": 1000,
            "ottlb": 1800,
            "elapsed": 2300,
            "obytes": 100,
            "url": "/a",
            "pop": "SJC",
            "oip": "192.0.2.10",
            "edge": True,
        },
        {
            "timestamp": "2026-09-16T14:00:40Z",
            "cache": "PASS",
            "ost": 503,
            "ttfb": 0.002,
            "ottlb": 2800,
            "elapsed": 3400,
            "obytes": 300,
            "url": "/a",
            "pop": "SJC",
            "oip": "192.0.2.10",
            "edge": False,
        },
        {
            "timestamp": "2026-09-16T14:01:00Z",
            "cache": "HIT",
            "ost": 829,
            "ottfb": "",
            "url": "/b",
            "pop": "IAD",
            "oip": "192.0.2.20",
            "edge": True,
        },
    )
    original = deepcopy(events)

    projection = build_origin_projection_rows(events)

    assert events == original
    assert projection.summary_rows == (
        {
            "bucket_start": "2026-09-16T14:00:00+00:00",
            "requests": 2,
            "misses": 1,
            "passes": 1,
            "origin_5xx": 1,
            "status_count": 2,
            "origin_bytes": 400,
            "latency_count": 2,
            "ttlb_count": 2,
            "overhead_count": 2,
            "origin_bytes_count": 2,
            "latency_p50_us": 1500.0,
            "latency_p75_us": 1750.0,
            "latency_p95_us": 1950.0,
            "latency_p99_us": 1990.0,
            "ttlb_p50_us": 2300.0,
            "ttlb_p95_us": 2750.0,
            "cdn_overhead_p50_us": 550.0,
            "origin_bytes_p50": 200.0,
        },
    )
    url = next(row for row in projection.dimension_rows if row["dimension"] == "url" and row["value"] == "/a")
    assert url["requests"] == 2
    assert url["origin_5xx"] == 1
    assert url["latency_p50_us"] == 1500.0
    status = next(row for row in projection.dimension_rows if row["dimension"] == "status" and row["value"] == "-1")
    assert status["requests"] == 1
    assert status["latency_count"] == 0


def test_skips_rows_without_parseable_timestamps_and_non_finite_numbers():
    projection = build_origin_projection_rows(
        (
            {"timestamp": "invalid", "ottfb": 10},
            {"timestamp": "2026-09-16T14:00:00Z", "ottfb": float("nan"), "ost": "bad"},
        )
    )

    assert projection.summary_rows == ()
    assert projection.dimension_rows == ()
