"""Regression tests for Opt 18 ClickHouse primary-key granule-skipping network sampling."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.network import (
    _network_json_cache,
    _network_memo,
    _network_model_cache,
    _network_time_sample_clause,
    get_pop_health,
    get_transport_metrics,
    network_health,
    network_quality,
)
from backend.models.analytics import NetworkHealthRequest, NetworkQualityRequest

pytestmark = pytest.mark.no_postgres

_RANGE_RE = re.compile(
    r"\(event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)\)"
)


class _RecordingCHClient:
    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=SimpleNamespace())

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params or {}), dict(settings) if settings else None))
        if callable(self._handler):
            return self._handler(sql, params or {})
        return list(self._handler or [])


def _clear_network_caches() -> None:
    _network_memo.cache.clear()
    _network_model_cache.clear()
    _network_json_cache.clear()


def _parse_windows(clause: str) -> list[tuple[int, int]]:
    return [(int(a), int(b)) for a, b in _RANGE_RE.findall(clause)]


def test_network_time_sample_clause_emits_monotonic_pk_ranges_for_30d_and_buckets() -> None:
    end = datetime(2026, 11, 25, 13, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)
    start_ts, end_ts = int(start_30d.timestamp()), int(end.timestamp())

    # Non-bucketed 30d (stride=100): 24 windows totaling exact 1% (25,920s)
    clause = _network_time_sample_clause(100, start=start_30d, end=end)
    assert "%" not in clause
    assert "intDiv" not in clause
    assert "line_ordinal" not in clause
    windows = _parse_windows(clause)
    assert len(windows) == 24
    assert sum(b - a for a, b in windows) == (end_ts - start_ts) // 100
    assert all(start_ts <= a < b <= end_ts for a, b in windows)
    assert all(windows[i][1] <= windows[i + 1][0] for i in range(len(windows) - 1))
    # Diurnal rotation: sampled windows should span multiple distinct UTC hours
    assert len({datetime.fromtimestamp(a, tz=UTC).hour for a, _ in windows}) >= 4

    # Bucketed 30d with 1h buckets (720 buckets, stride=100): 1 36s slice per hour
    b_clause = _network_time_sample_clause(100, bucket_seconds=3600, start=start_30d, end=end)
    assert "%" not in b_clause
    assert "intDiv" not in b_clause
    b_windows = _parse_windows(b_clause)
    assert len(b_windows) == 720
    assert sum(b - a for a, b in b_windows) == (end_ts - start_ts) // 100

    # High bucket count (30d at 300s = 8640 buckets) caps windows <= 360 to stay well under AST limits
    capped = _network_time_sample_clause(100, bucket_seconds=300, start=start_30d, end=end)
    c_windows = _parse_windows(capped)
    assert 1 < len(c_windows) <= 360

    # Naive datetimes must be treated as UTC, not host local timezone
    naive_clause = _network_time_sample_clause(
        100,
        start=start_30d.replace(tzinfo=None),
        end=end.replace(tzinfo=None),
    )
    assert naive_clause == clause


def test_network_endpoints_use_pk_range_sampling_for_30d_and_clean_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 25, 13, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    # 1. get_pop_health: 30d uses PK ranges + clean fallback when sample returns no fact rows
    pop_client = _RecordingCHClient(
        lambda sql, _p: (
            [
                {
                    "pop": "SJC",
                    "requests": 1000,
                    "errors": 5,
                    "p50_rtt_us": 15000.0,
                    "p95_ttfb_ms": 50.0,
                    "cache_hits": 700,
                    "bandwidth_bytes": 100_000,
                }
            ]
            if "UNION ALL" not in sql
            else []
        )
    )
    pop_svc = HighScaleService(service_id="svc-pop-30d", client=pop_client, cursor_secret=b"s", request_watermark=None)
    pop_res = get_pop_health(pop_svc, start_30d, end)
    assert len(pop_client.calls) == 2
    assert _RANGE_RE.search(pop_client.calls[0][0])
    assert "% 100" not in pop_client.calls[0][0]
    assert not _RANGE_RE.search(pop_client.calls[1][0])
    assert pop_res.data[0].pop == "SJC"

    # 2. network_quality: 30d region branch uses PK ranges, no modulo
    q_client = _RecordingCHClient(
        lambda sql, _p: (
            [
                {"dimension": "country", "label": "US", "rtt_ms": 20.0, "reqs": 1000},
                {"dimension": "asn", "label": "13335", "rtt_ms": 20.0, "reqs": 1000},
                {"dimension": "region", "label": "California", "rtt_ms": 18.0, "reqs": 500},
            ]
            if "FROM network_minute_dimensions" in sql
            else [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 200
        )
    )
    q_svc = HighScaleService(service_id="svc-q-30d", client=q_client, cursor_secret=b"s", request_watermark=None)
    network_quality(q_svc, NetworkQualityRequest(), start_30d.isoformat(), end.isoformat())
    bars_sql = next(s for s, _, _ in q_client.calls if "FROM network_minute_dimensions" in s)
    assert len(_parse_windows(bars_sql)) == 24
    assert "% 100" not in bars_sql

    # 3. network_health: 30d heatmap & map use 720 bucket-aligned PK ranges + clean fallback
    nh_client = _RecordingCHClient(
        lambda sql, _p: (
            [{"asn": 13335, "cnt": 1000}]
            if "FROM request_aggregates" in sql
            else (
                [
                    {
                        "asn": 13335,
                        "bucket_ts": int(start_30d.timestamp()),
                        "reqs": 50,
                        "throughput_bps": 1000.0,
                        "rtt_avg_us": 20000.0,
                        "rtt_quants": [10000.0, 20000.0, 40000.0, 60000.0],
                        "rtt_baseline_us": 15000.0,
                        "rtt_congestion_us": 5000.0,
                        "avg_ploss": 0.0,
                        "rtt_jitter_us": 1000.0,
                        "error_pct": 0.0,
                        "speed_map": {},
                    }
                ]
                if "AS rtt_quants" in sql and "GROUP BY asn, bucket_ts" in sql and "toDateTime(" not in sql
                else []
            )
        )
    )
    nh_svc = HighScaleService(service_id="svc-nh-30d", client=nh_client, cursor_secret=b"s", request_watermark=None)
    nh_resp = network_health(
        nh_svc,
        NetworkHealthRequest(sections=["heatmap", "map_buckets"], bucket_seconds=3600),
        start_30d.isoformat(),
        end.isoformat(),
    )
    hm_sampled = [s for s, _, _ in nh_client.calls if "GROUP BY asn, bucket_ts" in s and _RANGE_RE.search(s)]
    hm_fallback = [s for s, _, _ in nh_client.calls if "GROUP BY asn, bucket_ts" in s and not _RANGE_RE.search(s)]
    assert len(hm_sampled) == 1 and len(_parse_windows(hm_sampled[0])) == 720
    assert "% 100" not in hm_sampled[0]
    assert len(hm_fallback) == 1
    assert nh_resp.available is True

    # 4. get_transport_metrics: 30d uses 24 PK ranges, no modulo
    tr_client = _RecordingCHClient(
        [
            {
                "total_reqs": 50,
                "total_bytes": 50000,
                "rtt_quants": [10000.0, 20000.0, 30000.0, 40000.0],
                "avg_retrans": 0.1,
                "avg_ploss_pct": 0.05,
                "avg_del_rate_mbps": 12.5,
                "proto_map": {"HTTP/2": 50},
            }
        ]
    )
    tr_svc = HighScaleService(service_id="svc-tr-30d", client=tr_client, cursor_secret=b"s", request_watermark=None)
    get_transport_metrics(tr_svc, start_30d.isoformat(), end.isoformat(), {})
    tr_sql = tr_client.calls[0][0]
    assert len(_parse_windows(tr_sql)) == 24
    assert "% 100" not in tr_sql
