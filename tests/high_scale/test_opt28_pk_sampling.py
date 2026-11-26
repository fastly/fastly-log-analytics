"""Unit tests for Round 28 ClickHouse PK sampling caps and dense-timeseries modulo fallback."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.insights.sql_queries import _pk_range_windows, build_pk_time_sample_clause
from backend.high_scale.network import _network_sample_stride, _network_time_sample_clause

pytestmark = pytest.mark.no_postgres

_RANGE_RE = re.compile(
    r"event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)"
)


def _parse_windows(clause: str) -> list[tuple[int, int]]:
    return [(int(a), int(b)) for a, b in _RANGE_RE.findall(clause)]


def test_network_sample_stride_20d_and_30d_use_1000_keeping_14d_at_500() -> None:
    assert _network_sample_stride(1_209_600) == 500
    assert _network_sample_stride(1_727_999) == 500
    assert _network_sample_stride(1_728_000) == 1000
    assert _network_sample_stride(2_592_000) == 1000


def test_network_time_sample_clause_restores_modulo_for_dense_buckets_and_caps_pk_windows() -> None:
    end = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
    start_24h = end - timedelta(hours=24)
    start_30d = end - timedelta(days=30)

    # Dense timeseries (> 24 buckets) falls back to modulo sampling instead of huge OR trees.
    dense_24h = _network_time_sample_clause(20, bucket_seconds=300, start=start_24h, end=end)
    assert dense_24h == " AND line_ordinal % 20 = 0"

    dense_30d = _network_time_sample_clause(1000, bucket_seconds=3600, start=start_30d, end=end)
    assert dense_30d == " AND line_ordinal % 1000 = 0"

    # Sparse bucketed timeseries (<= 24 buckets) keeps monotonic PK range clauses.
    sparse_24h = _network_time_sample_clause(20, bucket_seconds=3600, start=start_24h, end=end)
    sparse_windows = _parse_windows(sparse_24h)
    assert 1 <= len(sparse_windows) <= 24
    assert "toDateTime(" in sparse_24h
    assert "line_ordinal" not in sparse_24h

    # Few buckets (<= 16) also emit <= 16 OR branches.
    sparse_12 = _network_time_sample_clause(20, bucket_seconds=7200, start=start_24h, end=end)
    assert len(_parse_windows(sparse_12)) <= 12

    # Unbucketed queries cap at <= 16 wide (>=120s) monotonic PK range slices.
    for span_days, stride in ((1, 20), (7, 100), (14, 500), (30, 1000)):
        start = end - timedelta(days=span_days)
        clause = _network_time_sample_clause(stride, start=start, end=end)
        windows = _parse_windows(clause)
        assert 1 <= len(windows) <= 16
        assert clause.count(" OR ") <= 15
        assert "line_ordinal" not in clause
        assert "intDiv" not in clause
        assert all(b - a >= 120 for a, b in windows)
        expected_sampled = (span_days * 86400) // stride
        assert abs(sum(b - a for a, b in windows) - expected_sampled) <= len(windows)


def test_pk_range_windows_capped_at_16_and_insights_combined_under_22() -> None:
    end = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
    end_ts = int(end.timestamp())

    for span_days, stride in ((7, 8), (14, 8), (30, 8)):
        span_s = span_days * 86400
        start_ts = end_ts - span_s
        windows = _pk_range_windows(start_ts, end_ts, stride)
        assert 1 <= len(windows) <= 16
        assert all(start_ts <= a < b <= end_ts for a, b in windows)
        assert all(windows[i][1] <= windows[i + 1][0] for i in range(len(windows) - 1))
        total_s = sum(b - a for a, b in windows)
        expected_s = span_s // stride
        assert abs(total_s - expected_s) <= len(windows)

    # Combined 14d baseline + 24h window emits at most 16 + 6 = 22 PK OR branches.
    w_start = end - timedelta(hours=24)
    b_start = end - timedelta(days=14)
    clause = build_pk_time_sample_clause(
        b_stride=8,
        w_stride=4,
        baseline_start=b_start,
        window_start=w_start,
        window_end=end,
    )
    windows = _parse_windows(clause)
    assert len(windows) <= 22
    assert clause.count(" OR ") <= 21
