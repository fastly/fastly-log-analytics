"""Unit tests for Round 38 ClickHouse sub-second fixes on rollup_base_sql and batch_network_asn."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import _build_network_asn
from backend.high_scale.insights.rollup import ROLLUP_SHAPES, rollup_base_sql
from backend.high_scale.insights.sql_queries import DETECTIONS

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


class _FakeClickHouseClient(ClickHouseClient):
    """Fake ClickHouseClient capturing executed SQL and query parameters."""

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


def test_rollup_base_sql_uses_monotonic_pk_range_sampling_when_bounds_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    b_start = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    w_start = datetime(2026, 11, 27, 12, 0, tzinfo=UTC)
    w_end = datetime(2026, 11, 28, 12, 0, tzinfo=UTC)
    r_start = datetime(2026, 11, 20, 14, 0, tzinfo=UTC)
    r_end = datetime(2026, 11, 27, 11, 0, tzinfo=UTC)

    sql = rollup_base_sql(
        DETECTIONS["referer_monoculture"],
        "referer_monoculture",
        "referer",
        baseline_hours=168.0,
        window_hours=24.0,
        rollup_span=(r_start, r_end),
        baseline_start=b_start,
        window_start=w_start,
        window_end=w_end,
    )
    assert "toDateTime(" in sql
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in sql

    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_COVERAGE_CACHE.clear()

    def _handler(q: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            hours = _hours(params["baseline_start"], params["window_end"])
            return [
                {
                    "earliest": b_start - timedelta(days=2),
                    "window_total": 5000,
                    "hours": hours,
                    "hour_rows": [10] * len(hours),
                    "hour_checksums": [str(h * 7) for h in hours],
                }
            ]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:rollup_coverage */" in q:
            cov_hours = _hours(params["coverage_start"], params["coverage_end"] - timedelta(hours=1))
            return [
                {"shape": shape, "hour": str(h), "row_count": "10", "batch_checksum": str(h * 7)}
                for shape in ROLLUP_SHAPES
                for h in cov_hours
            ]
        return []

    client = _FakeClickHouseClient(_handler)
    svc = HighScaleService(
        service_id="svc-opt38-referer",
        client=client,
        cursor_secret=b"opt38-test-secret-32-bytes-long!",
        request_watermark=lambda: w_end,
    )
    evaluator._evaluate(
        svc,
        24.0,
        168.0,
        None,
        w_end.isoformat(),
        False,
        "",
        {},
        frozenset(ALL_FIELDS),
    )
    ref_sql = next(q for q, _, _ in client.calls if "/* insight:referer_monoculture */" in q)
    assert "toDateTime(" in ref_sql
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in ref_sql


def test_batch_network_asn_avoids_tdigest_and_gates_group_quantiles() -> None:
    sql, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=set(ALL_FIELDS),
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
        candidates={"asn": ["15169"], "metro": ["501"]},
        candidate_params={},
    )
    assert "quantileTDigestIf(" not in sql
    assert "quantileIf(0.95)(rtt_var, is_w AND asn_str != '') AS w_jitter" in sql
    assert "quantileIf(0.95)(rtt_var, is_b AND asn_str != '') AS b_jitter" in sql
    assert (
        "quantileIf(0.50)(tcp_rtt, is_w AND asn_str != '' AND metro != '' AND tcp_rtt > 0 AND country = 'US') / 1000.0 AS amp_w_med"
        in sql
    )
    assert (
        "quantileIf(0.50)(tcp_rtt, is_b AND asn_str != '' AND metro != '' AND tcp_rtt > 0 AND country = 'US') / 1000.0 AS amp_b_med"
        in sql
    )
    assert "quantileIf(0.5)(delivery_rate, is_w AND metro != '' AND delivery_rate > 0) AS mdd_w_med" in sql
    assert "quantileIf(0.5)(delivery_rate, is_b AND metro != '' AND delivery_rate > 0) AS mdd_b_med" in sql
    assert "countIf(is_w) * 4 AS nah_w_total" in sql
    assert "countIf(is_w AND tcp_rtt > 0 AND country = 'US') * 4 AS amp_w_total" in sql
    assert "countIf(is_w AND delivery_rate > 0) * 4 AS mdd_w_total" in sql
