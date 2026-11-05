from datetime import UTC, datetime, timedelta

import pytest

from backend.repositories.utils.response_shaping import SectionTimer
from backend.repositories.value import (
    _build_io_metrics,
    _io_monthly_cost,
    _shape_caching_and_overview_buckets,
    _shape_raw_network_buckets,
)


@pytest.mark.parametrize(
    ("requests", "expected"),
    [
        (0, 0.0),
        (100_000, 0.0),
        (10_000_000, 495.0),
        (50_000_000, 495.0 + 40_000_000 / 10_000 * 0.15),
        (150_000_000, 495.0 + 600.0 + 100_000_000 / 10_000 * 0.07),
        (6_000_000_000, 10_845.0),
    ],
)
def test_io_monthly_cost_tiers(requests: int, expected: float) -> None:
    assert _io_monthly_cost(requests) == pytest.approx(expected)


def test_value_summary_shaping_supports_proto_tls_ipv6_and_caching() -> None:
    prev_hour = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    st_iso = prev_hour.isoformat().replace("+00:00", "Z")
    next_iso = (prev_hour + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

    net_buckets = [
        {
            "bucket": st_iso,
            "http3_cnt": 1,
            "h2_cnt": 1,
            "h11_cnt": 1,
            "other_proto_cnt": 1,
            "tls_cnt": 3,
            "ipv6_cnt": 2,
        }
    ]
    net = _shape_raw_network_buckets(
        net_buckets,
        4,
        has_protocol=True,
        has_is_ssl=True,
        has_ipv6=True,
    )
    assert net["total_requests"] == 4
    assert net["http3_pct"] == 25.0
    assert net["h2_pct"] == 25.0
    assert net["tls_pct"] == 75.0
    assert net["ipv6_pct"] == 50.0
    assert len(net["protocol_time_series"]) == 4

    caching_buckets = [
        {
            "bucket": st_iso,
            "total": 4,
            "hit_cnt": 3,
            "miss_cnt": 1,
            "pass_cnt": 0,
            "synth_cnt": 0,
            "shield_hit": 2,
            "shield_total": 4,
            "bandwidth_saved": 3000,
            "total_bytes": 5000,
            "threats_cnt": 1,
            "origin_cnt": 1,
            "sum_hit_elapsed": 3000.0,
            "cnt_hit_elapsed": 3,
            "sum_miss_elapsed": 4000.0,
            "cnt_miss_elapsed": 1,
        }
    ]
    shaped = _shape_caching_and_overview_buckets(
        caching_buckets,
        include_overview=True,
        has_resp_bytes=True,
        has_shield=True,
        has_waf=True,
        has_elapsed=True,
        include_performance=True,
    )
    assert shaped["caching"]["origin_offload_pct"] == 75.0
    assert shaped["caching"]["shield_effectiveness_pct"] == 50.0
    assert shaped["overview"]["cache_acceleration_factor"] == 4.0
    assert shaped["performance_from_buckets"]["cache_accel_factor"] == 4.0

    io_metrics = _build_io_metrics(
        {
            "data": [
                {
                    "start_time": int(prev_hour.timestamp()),
                    "imgopto": 200_000,
                    "imgopto_resp_body_bytes": 1_024_000,
                    "imgopto_shield_resp_body_bytes": 512_000,
                    "requests": 400_000,
                    "imgopto_webp_count": 150_000,
                    "imgopto_jpeg_count": 50_000,
                }
            ],
            "next": next_iso,
        },
        SectionTimer(),
    )
    assert io_metrics["io_transforms"] == 200_000
    assert io_metrics["modern_format_pct"] == 75.0
    assert io_metrics["io_pct_of_traffic"] == 50.0
