"""Tests for opt31 insights fingerprint_spread & low_and_slow sampling and background threads."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import _build_fingerprint_spread
from backend.high_scale.insights.sql_queries import _PROBE_PREWHERE, DETECTIONS, _base_sql


def test_fingerprint_spread_samples_long_baseline_and_168h_window() -> None:
    present = {"ip", "ja4", "asn", "p_type", "tls_ciphers_sha"}
    active = {"botnet_grouping", "cipher_spread", "residential_fingerprint_dispersion"}
    now = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)

    default_sql, _ = _build_fingerprint_spread(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "toDateTime(" not in default_sql
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in default_sql
    assert "countIf(is_w) AS w_reqs" in default_sql
    assert "countIf(is_w AND p_type != 'hosting') AS res_w_reqs" in default_sql

    w_start_24 = now - timedelta(hours=24)
    b_start_24 = w_start_24 - timedelta(hours=168)
    sql_24_168, _ = _build_fingerprint_spread(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
        baseline_start=b_start_24,
        window_start=w_start_24,
        window_end=now,
    )
    assert "event_timestamp >= {window_start:DateTime64(3)} OR ((event_timestamp >= toDateTime(" in sql_24_168
    assert "countIf(is_w) AS w_reqs" in sql_24_168

    w_start_168 = now - timedelta(hours=168)
    b_start_168 = w_start_168 - timedelta(hours=720)
    sql_168_720, _ = _build_fingerprint_spread(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=720.0,
        window_hours=168.0,
        baseline_start=b_start_168,
        window_start=w_start_168,
        window_end=now,
    )
    assert "toDateTime(" in sql_168_720
    assert "countIf(is_w) * 12 AS w_reqs" in sql_168_720
    assert "countIf(is_w AND p_type != 'hosting') * 12 AS res_w_reqs" in sql_168_720


def test_low_and_slow_samples_baseline_only_while_keeping_probe_prewhere() -> None:
    det = DETECTIONS["low_and_slow"]
    assert det.baseline_sampled is True
    assert det.window_sampled is False

    now = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)
    w_start = now - timedelta(hours=24)
    b_start = w_start - timedelta(hours=168)
    sql_168 = _base_sql(
        det,
        "low_and_slow",
        baseline_hours=168.0,
        window_hours=24.0,
        baseline_start=b_start,
        window_start=w_start,
        window_end=now,
    )
    assert f"PREWHERE {_PROBE_PREWHERE}" in sql_168
    assert "event_timestamp >= {window_start:DateTime64(3)} OR ((event_timestamp >= toDateTime(" in sql_168

    b_start_10 = w_start - timedelta(hours=10)
    sql_10 = _base_sql(
        det,
        "low_and_slow",
        baseline_hours=10.0,
        window_hours=2.0,
        baseline_start=b_start_10,
        window_start=now - timedelta(hours=2),
        window_end=now,
    )
    assert f"PREWHERE {_PROBE_PREWHERE}" in sql_10
    assert "toDateTime(" not in sql_10
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in sql_10


def test_background_insights_default_max_threads_is_three() -> None:
    assert evaluator._BACKGROUND_QUERY_MAX_THREADS == "3"
