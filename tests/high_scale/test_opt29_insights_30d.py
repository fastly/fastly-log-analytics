"""Tests for 30d insights sampling strides and subquery window sampling (opt29)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale.insights import evaluator
from backend.high_scale.insights.sql_queries import (
    DETECTIONS,
    _base_sql,
    baseline_sample_stride,
    window_sample_stride,
)
from backend.models.analytics import InsightsRequest


def test_insights_sampling_strides_cover_7d_14d_30d() -> None:
    assert window_sample_stride(12.0) == 1
    assert window_sample_stride(24.0) == 4
    assert window_sample_stride(48.0) == 6
    assert window_sample_stride(168.0) == 12
    assert window_sample_stride(336.0) == 24
    assert window_sample_stride(720.0) == 40

    assert baseline_sample_stride(12.0) == 1
    assert baseline_sample_stride(24.0) == 4
    assert baseline_sample_stride(48.0) == 6
    assert baseline_sample_stride(168.0) == 8
    assert baseline_sample_stride(336.0) == 24
    assert baseline_sample_stride(720.0) == 48


def test_crawler_disparity_samples_site_resp_total_on_large_windows_and_keeps_filters() -> None:
    now = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)
    w_start_720 = now - timedelta(hours=720)
    b_start_720 = w_start_720 - timedelta(hours=720)
    sql_720 = _base_sql(
        DETECTIONS["crawler_disparity"],
        "crawler_disparity",
        baseline_hours=720.0,
        window_hours=720.0,
        baseline_start=b_start_720,
        window_start=w_start_720,
        window_end=now,
    )
    site_total_720 = sql_720.split("AS site_resp_total")[0].rsplit("greatest(", 1)[1]
    assert ") * 40, 1" in f"{site_total_720}, 1"
    assert "toDateTime(" in site_total_720
    assert "__W_SAMPLE__" not in sql_720
    assert "__W_SCALE__" not in sql_720

    w_start_12 = now - timedelta(hours=12)
    b_start_12 = w_start_12 - timedelta(hours=168)
    sql_12 = _base_sql(
        DETECTIONS["crawler_disparity"],
        "crawler_disparity",
        baseline_hours=168.0,
        window_hours=12.0,
        baseline_start=b_start_12,
        window_start=w_start_12,
        window_end=now,
    )
    site_total_12 = sql_12.split("AS site_resp_total")[0].rsplit("greatest(", 1)[1]
    assert "toDateTime(" not in site_total_12
    assert "* " not in site_total_12

    calls: list[tuple[str, dict[str, Any]]] = []

    class _FakeCH:
        _enable_cf_rewrite_in_tests = True

        def _has_materialized_cf_columns(self) -> bool:
            return True

        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            calls.append((sql, params or {}))
            if "/* insight:history */" in sql:
                return [{"earliest": datetime(2026, 10, 1, tzinfo=UTC), "window_total": 1000, "hours": []}]
            if "/* insight:fields */" in sql:
                return [{"fields": ["ua", "referer", "ip", "resp_bytes", "country"]}]
            return []

    class _Svc:
        service_id = "svc-opt29-crawler"
        client = _FakeCH()

    evaluator.insights_endpoint(
        _Svc(),  # type: ignore[arg-type]
        InsightsRequest(
            window_size_hrs=168,
            baseline_hours=720,
            filters={"country": {"mode": "include", "values": ["US"]}},
        ),
        None,
        None,
        force_refresh=True,
    )
    crawler_q = next(q for q, _ in calls if "/* insight:crawler_disparity */" in q)
    eval_site_total = crawler_q.split("AS site_resp_total")[0].rsplit("greatest(", 1)[1]
    assert "_cf_resp_bytes" in eval_site_total
    assert "country IN (" in eval_site_total
    assert "* 12" in eval_site_total


def test_stale_browser_version_samples_window_and_guard_on_24h_plus() -> None:
    now = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)
    w_start_24 = now - timedelta(hours=24)
    b_start_24 = w_start_24 - timedelta(hours=168)
    sql_24 = _base_sql(
        DETECTIONS["stale_browser_version"],
        "stale_browser_version",
        baseline_hours=168.0,
        window_hours=24.0,
        baseline_start=b_start_24,
        window_start=w_start_24,
        window_end=now,
    )
    assert "countIf(is_w) * 4 AS w_reqs" in sql_24
    assert "HAVING count() * 4 >= 500)" in sql_24
    assert "__W_SAMPLE__" not in sql_24
    assert "toDateTime(" in sql_24.split("HAVING count() * 4 >= 500")[0].rsplit("chrome_version IN (", 1)[1]

    w_start_12 = now - timedelta(hours=12)
    b_start_12 = w_start_12 - timedelta(hours=168)
    sql_12 = _base_sql(
        DETECTIONS["stale_browser_version"],
        "stale_browser_version",
        baseline_hours=168.0,
        window_hours=12.0,
        baseline_start=b_start_12,
        window_start=w_start_12,
        window_end=now,
    )
    assert "countIf(is_w) AS w_reqs" in sql_12
    assert "HAVING count() >= 500)" in sql_12
