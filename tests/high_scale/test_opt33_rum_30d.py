"""Tests for Round 33 7d+ RUM ClickHouse PK range sampling and empty-sample fallback."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest

from backend.high_scale import rum as hs_rum
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.ingest_controller import HighScaleService
from backend.models.analytics import RumAnalyticsResponse

pytestmark = pytest.mark.no_postgres


class _FakeRealTransport:
    pass


class _FakeRealHttp:
    _transport = _FakeRealTransport()


class _FakeRealCHClient:
    def __init__(self, handler: Any) -> None:
        self._http = _FakeRealHttp()
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._handler = handler

    def execute(self, sql: str, params: dict[str, Any] | None = None, **_kwargs: Any) -> list[dict[str, Any]]:
        p = params or {}
        self.calls.append((sql, p))
        return self._handler(sql, p)


def _service(client: Any, service_id: str = "svc-opt33-rum") -> HighScaleService:
    wm = ServingWatermark(service_id, "request", 0, None, None, None, None, None, True)
    return HighScaleService(service_id, client, b"test-secret", wm)


def test_rum_sample_stride_tiers() -> None:
    assert hs_rum._rum_sample_stride(2 * 3600) == 1
    assert hs_rum._rum_sample_stride(86_400) == 1
    assert hs_rum._rum_sample_stride(6 * 86_400) == 1
    assert hs_rum._rum_sample_stride(7 * 86_400) == 4
    assert hs_rum._rum_sample_stride(14 * 86_400) == 8
    assert hs_rum._rum_sample_stride(30 * 86_400) == 16


def test_rum_pk_sample_clauses_30d_covers_every_utc_day_and_bounds_request_facts() -> None:
    start = datetime(2026, 10, 28, 0, 0, tzinfo=UTC)
    end = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)
    rum_clause, req_clause = hs_rum._rum_pk_sample_clauses(16, start=start, end=end)
    assert rum_clause != "1" and req_clause != "1"

    rum_wins = [
        (int(a), int(b))
        for a, b in re.findall(
            r"event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)",
            rum_clause,
        )
    ]
    req_wins = [
        (int(a), int(b))
        for a, b in re.findall(
            r"event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)",
            req_clause,
        )
    ]
    assert len(rum_wins) == 30
    assert len(req_wins) == 30
    for i, ((ws, we), (rs, re_)) in enumerate(zip(rum_wins, req_wins, strict=True)):
        assert we - ws == 5400
        assert rs == ws - 600 and re_ == we + 600
        if i > 0:
            assert ws > rum_wins[i - 1][1]
            assert rs > req_wins[i - 1][1]

    rum_covered_s = sum(we - ws for ws, we in rum_wins)
    req_covered_s = sum(re_ - rs for rs, re_ in req_wins)
    span_s = (end - start).total_seconds()
    assert rum_covered_s / span_s < 0.07
    assert req_covered_s / span_s < 0.10


def test_rum_analytics_30d_samples_pk_uses_tdigest_and_scales_weights(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    def _handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* rum_analytics_counts_trends_pages */" in sql:
            return [
                {
                    "kind": "count",
                    "bucket": "1970-01-01 00:00:00",
                    "path": "",
                    "beacons": 160,
                    "views": 128,
                    "pageviews": 96,
                    "interactions": 32,
                    "errors": 32,
                    "sampled": 1,
                    "avg_load": 900.0,
                    "lcp": 2100.0,
                    "cls": 0.05,
                    "fcp": 1000.0,
                    "ttfb": 300.0,
                    "error_rate": 20.0,
                    "page_rn": 1,
                }
            ]
        if "/* rum_analytics_vitals */" in sql:
            return [{"metric_name": "lcp", "p75": 2100.0, "total": 128, "good": 96, "ni": 32, "poor": 0}]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client)
    with patch("backend.repositories.utils.response_shaping.asn.resolve_asn_names_map", return_value={}):
        res = hs_rum.rum_analytics(svc, "2026-10-28T00:00:00Z", "2026-11-27T00:00:00Z")

    assert len(client.calls) == 5
    for sql, _ in client.calls:
        assert "event_timestamp >= toDateTime(" in sql
        assert "* 16" in sql
        if "rum_analytics_exceptions" not in sql and "rum_analytics_environments" not in sql:
            assert "quantileTDigest" in sql
            assert "quantileExactInclusive" not in sql

    slowest_sql = next(sql for sql, _ in client.calls if "/* rum_analytics_slowest */" in sql)
    req_section = slowest_sql.split("FROM request_facts PREWHERE", 1)[1].split("AND _cf_rid != ''", 1)[0]
    assert "event_timestamp >= toDateTime(" in req_section

    model = RumAnalyticsResponse.model_validate(res)
    assert model.estimated is True
    assert model.beacon_count == 160

    # Short (< 7d) windows keep exact quantiles and no multi-day PK sampling.
    short_client = _FakeRealCHClient(_handler)
    with patch("backend.repositories.utils.response_shaping.asn.resolve_asn_names_map", return_value={}):
        hs_rum.rum_analytics(_service(short_client, "svc-opt33-24h"), "2026-11-26T00:00:00Z", "2026-11-27T00:00:00Z")
    ctp_short = next(sql for sql, _ in short_client.calls if "/* rum_analytics_counts_trends_pages */" in sql)
    assert "event_timestamp >= toDateTime(" not in ctp_short
    assert "quantileExactInclusive" in ctp_short


def test_rum_analytics_30d_empty_sample_falls_back_to_unsampled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    calls: list[str] = []

    def _handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append(sql)
        sampled = "event_timestamp >= toDateTime(" in sql
        if "/* rum_analytics_counts_trends_pages */" in sql:
            if sampled:
                return [
                    {
                        "kind": "count",
                        "bucket": "1970-01-01 00:00:00",
                        "path": "",
                        "beacons": 0,
                        "views": 0,
                        "pageviews": 0,
                        "interactions": 0,
                        "errors": 0,
                        "sampled": 0,
                    }
                ]
            return [
                {
                    "kind": "count",
                    "bucket": "1970-01-01 00:00:00",
                    "path": "",
                    "beacons": 7,
                    "views": 7,
                    "pageviews": 7,
                    "interactions": 0,
                    "errors": 0,
                    "sampled": 0,
                }
            ]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt33-fallback")
    with patch("backend.repositories.utils.response_shaping.asn.resolve_asn_names_map", return_value={}):
        res = hs_rum.rum_analytics(svc, "2026-10-28T00:00:00Z", "2026-11-27T00:00:00Z")

    assert len(client.calls) == 10
    assert any("event_timestamp >= toDateTime(" in sql for sql in calls[:5])
    assert all("event_timestamp >= toDateTime(" not in sql for sql in calls[5:])
    assert res["beacon_count"] == 7
    assert res["estimated"] is False
