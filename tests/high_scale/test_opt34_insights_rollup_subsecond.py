"""Unit tests for Round 34 7d+ Insights rollup PK sampling & fast rollup_coverage settings."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.rollup import ROLLUP_SHAPES, SAMPLED_ROLLUP_DETECTIONS, rollup_base_sql
from backend.high_scale.insights.sql_queries import DETECTIONS

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


class _FakeClickHouseClient(ClickHouseClient):
    """Fake ClickHouseClient capturing executed SQL and query settings."""

    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())

    def _has_materialized_cf_columns(self) -> bool:
        return True

    def _get_pending_batches(self, _service_id: str) -> dict[str, tuple[str, ...]] | None:
        return None

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Mapping[str, str] | None = None,
        rewrite_cf: bool = True,
    ) -> list[dict[str, Any]]:
        rewritten = self.rewrite_publication_fence(sql, params, rewrite_cf=rewrite_cf)
        self.calls.append((rewritten, dict(params or {}), dict(settings) if settings else None))
        if callable(self._handler):
            return self._handler(rewritten, params or {})
        return list(self._handler or [])


def _hours(start: datetime, end: datetime) -> list[int]:
    hour = int(start.timestamp()) // 3600 * 3600
    return list(range(hour, int(end.timestamp()) + 1, 3600))


def test_rollup_base_sql_samples_hour_pk_windows_on_long_sampled_spans() -> None:
    t0 = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    span_168h = (t0, t0 + timedelta(hours=168))

    for insight_id, shape in (("error_spikes", "url_status"), ("referer_monoculture", "referer")):
        assert insight_id in SAMPLED_ROLLUP_DETECTIONS
        sampled_sql = rollup_base_sql(
            DETECTIONS[insight_id],
            insight_id,
            shape,
            baseline_hours=168.0,
            window_hours=24.0,
            rollup_span=span_168h,
        )
        rollup_part = sampled_sql.split("UNION ALL", 1)[1]
        windows = re.findall(
            r"bucket_start >= toDateTime\((\d+), 'UTC'\) AND bucket_start < toDateTime\((\d+), 'UTC'\)",
            rollup_part,
        )
        assert 1 <= len(windows) <= 14
        sampled_hours = sum((int(hi) - int(lo)) // 3600 for lo, hi in windows)
        assert sampled_hours == 42
        assert "request_count * 4 AS n" in rollup_part

        # Non-divisible span (e.g., 167h -> 41h) scales safely via round(...).
        non_div = rollup_base_sql(
            DETECTIONS[insight_id],
            insight_id,
            shape,
            baseline_hours=168.0,
            window_hours=24.0,
            rollup_span=(t0, t0 + timedelta(hours=167)),
        )
        non_div_rollup = non_div.split("UNION ALL", 1)[1]
        assert "toUInt64(round(request_count * " in non_div_rollup

        # Without rollup_span, or on < 72h baselines, rollup branch stays unsampled.
        for label_sql in (
            rollup_base_sql(
                DETECTIONS[insight_id],
                insight_id,
                shape,
                baseline_hours=168.0,
                window_hours=24.0,
                rollup_span=None,
            ),
            rollup_base_sql(
                DETECTIONS[insight_id],
                insight_id,
                shape,
                baseline_hours=48.0,
                window_hours=6.0,
                rollup_span=(t0, t0 + timedelta(hours=72)),
            ),
        ):
            r_branch = label_sql.split("UNION ALL", 1)[1]
            assert "toDateTime(" not in r_branch
            assert "request_count AS n" in r_branch

    # Non-sampled rollup detections never sample the rollup branch even on 168h spans.
    unsampled_sql = rollup_base_sql(
        DETECTIONS["new_probe_urls"],
        "new_probe_urls",
        "url_status",
        baseline_hours=168.0,
        window_hours=24.0,
        rollup_span=span_168h,
    )
    unsampled_rollup = unsampled_sql.split("UNION ALL", 1)[1]
    assert "toDateTime(" not in unsampled_rollup
    assert "request_count AS n" in unsampled_rollup


def test_evaluator_passes_rollup_span_and_uses_fast_coverage_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_COVERAGE_CACHE.clear()

    now = datetime(2026, 11, 27, 12, 0, tzinfo=UTC)

    def _handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            hours = _hours(params["baseline_start"], params["window_end"])
            return [
                {
                    "earliest": now - timedelta(days=10),
                    "window_total": 5000,
                    "hours": hours,
                    "hour_rows": [10] * len(hours),
                    "hour_checksums": [str(h * 7) for h in hours],
                }
            ]
        if "/* insight:fields */" in sql:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:rollup_coverage */" in sql:
            hours = _hours(params["coverage_start"], params["coverage_end"] - timedelta(seconds=1))
            return [
                {"shape": shape, "hour": str(h), "row_count": "10", "batch_checksum": str(h * 7)}
                for shape in ROLLUP_SHAPES
                for h in hours
            ]
        return []

    client = _FakeClickHouseClient(_handler)
    svc = HighScaleService(
        service_id="svc-opt34",
        client=client,
        cursor_secret=b"opt34-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )

    evaluator._evaluate(
        svc,
        24.0,
        168.0,
        None,
        now.isoformat(),
        False,
        "",
        {},
        frozenset(ALL_FIELDS),
        query_settings={"max_threads": "3"},
    )

    cov_call = next(c for c in client.calls if "/* insight:rollup_coverage */" in c[0])
    assert cov_call[2] == {"max_threads": "6", "optimize_aggregation_in_order": "1"}

    for marker in ("/* insight:error_spikes */", "/* insight:referer_monoculture */"):
        sql, _params, settings = next(c for c in client.calls if marker in c[0])
        assert settings == {"max_threads": "3"}
        rollup_branch = sql.split("UNION ALL", 1)[1]
        assert "bucket_start >= toDateTime(" in rollup_branch
