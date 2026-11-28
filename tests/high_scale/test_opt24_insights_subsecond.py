"""Unit tests for Round 24 High-Scale ClickHouse insights sub-second optimizations."""

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
from backend.high_scale.insights.batches import (
    _FP_SPREAD_CANDIDATE_LIMIT,
    DETECTION_BATCHES,
    _build_fingerprint_spread,
    _build_fingerprint_spread_candidates,
    _build_ip_outlier,
)
from backend.high_scale.insights.rollup import ROLLUP_SHAPES, SAMPLED_ROLLUP_DETECTIONS, rollup_base_sql
from backend.high_scale.insights.sql_queries import (
    _PROBE_PREWHERE,
    DETECTIONS,
    _base_sql,
    window_sample_stride,
)
from backend.models.analytics import InsightsRequest
from backend.repositories.insights.shaping import NEW_PROBES
from backend.utils.bounded_cache import CacheRegistry

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


class _FakeRealClickHouseClient(ClickHouseClient):
    """Fake ClickHouseClient that tracks SQL calls and avoids real HTTP."""

    def __init__(self, handler: Any = None, *, materialized_cf: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())
        self._materialized_cf = materialized_cf

    def _has_materialized_cf_columns(self) -> bool:
        return self._materialized_cf

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


def _service(client: Any, sid: str = "svc-opt24") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt24-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


@pytest.fixture(autouse=True)
def _stub_config_and_clear_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_COVERAGE_CACHE.clear()
    evaluator._INSIGHTS_LAST_RESULT.clear()
    evaluator._INSIGHTS_REFRESH_FAILURES.clear()


def test_probe_prewhere_uses_multi_search_before_match() -> None:
    assert _PROBE_PREWHERE.startswith("multiSearchAnyCaseInsensitive(url, [")
    assert _PROBE_PREWHERE.endswith("])")
    assert "match(" not in _PROBE_PREWHERE
    assert "{probe_regex:String}" not in _PROBE_PREWHERE
    assert "{window_start" not in _PROBE_PREWHERE
    assert "{window_end" not in _PROBE_PREWHERE
    for needle in NEW_PROBES:
        assert f"'{needle}'" in _PROBE_PREWHERE

    for insight_id in ("new_probe_urls", "low_and_slow"):
        assert DETECTIONS[insight_id].prewhere == _PROBE_PREWHERE
        assert "match(url, {probe_regex:String})" in DETECTIONS[insight_id].row_filter
        sql = _base_sql(DETECTIONS[insight_id], insight_id)
        pre = re.search(r"FROM request_facts PREWHERE (.*?) WHERE ", sql)
        assert pre is not None
        assert pre.group(1) == _PROBE_PREWHERE
        assert sql.count("{probe_regex:String}") == 1
        assert sql.index("WHERE ") < sql.index("{probe_regex:String}")


def test_rollup_base_sql_samples_window_and_pushes_sample_to_prewhere() -> None:
    assert window_sample_stride(12.0) == 1
    assert window_sample_stride(24.0) == 4
    assert window_sample_stride(48.0) == 6

    combined_sample = (
        "((event_timestamp >= {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        " OR (event_timestamp < {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0))"
    )
    expected_weight = "if(event_timestamp < {window_start:DateTime64(3)}, toUInt64(8), toUInt64(4)) AS n"

    for insight_id, shape in (
        ("error_spikes", "url_status"),
        ("ua_monoculture", "ua"),
        ("referer_monoculture", "referer"),
        ("waf_signal_spikes", "waf"),
    ):
        assert insight_id in SAMPLED_ROLLUP_DETECTIONS
        sampled_sql = rollup_base_sql(
            DETECTIONS[insight_id], insight_id, shape, baseline_hours=168.0, window_hours=24.0
        )
        raw_part, rollup_part = sampled_sql.split("UNION ALL", 1)
        assert f"FROM request_facts PREWHERE {combined_sample} WHERE " in raw_part
        assert expected_weight in raw_part
        assert "intDiv(toUnixTimestamp(event_timestamp)" not in rollup_part
        assert "request_count AS n" in rollup_part

        unsampled_sql = rollup_base_sql(DETECTIONS[insight_id], insight_id, shape, baseline_hours=0.0, window_hours=0.0)
        assert "PREWHERE" not in unsampled_sql
        assert "intDiv(toUnixTimestamp(event_timestamp)" not in unsampled_sql
        assert "toUInt64(1) AS n" in unsampled_sql

    probe_sql = rollup_base_sql(
        DETECTIONS["new_probe_urls"], "new_probe_urls", "url_status", baseline_hours=168.0, window_hours=24.0
    )
    assert f"PREWHERE {_PROBE_PREWHERE}" in probe_sql
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in probe_sql
    assert "toUInt64(1) AS n" in probe_sql


def test_coverage_cache_reuses_when_checksums_match_and_invalidates_on_change() -> None:
    assert "high_scale.insights._INSIGHTS_COVERAGE_CACHE" in CacheRegistry.names()
    assert evaluator._INSIGHTS_COVERAGE_CACHE.maxsize == 128
    assert evaluator._INSIGHTS_COVERAGE_CACHE.ttl_seconds == 900.0

    def _hours(start: datetime, end: datetime) -> list[int]:
        hour = int(start.timestamp()) // 3600 * 3600
        return list(range(hour, int(end.timestamp()) + 1, 3600))

    state = {"checksum_mult": 7, "fail_coverage": False}

    def _handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            hours = _hours(params["baseline_start"], params["window_end"])
            return [
                {
                    "earliest": datetime.now(UTC) - timedelta(days=10),
                    "window_total": 5000,
                    "hours": hours,
                    "hour_rows": [10] * len(hours),
                    "hour_checksums": [str(h * state["checksum_mult"]) for h in hours],
                }
            ]
        if "/* insight:fields */" in sql:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:rollup_coverage */" in sql:
            if state["fail_coverage"]:
                raise RuntimeError("coverage boom")
            hours = _hours(params["coverage_start"], params["coverage_end"] - timedelta(seconds=1))
            return [
                {
                    "shape": shape,
                    "hour": str(h),
                    "row_count": "10",
                    "batch_checksum": str(h * state["checksum_mult"]),
                }
                for shape in ROLLUP_SHAPES
                for h in hours
            ]
        return []

    client = _FakeRealClickHouseClient(_handler)
    svc = _service(client, "svc-opt24-cov")

    # 1. Cold cache -> queries rollup_coverage once and caches it.
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 1
    ref_sql_1 = next(q for q, _, _ in client.calls if "/* insight:referer_monoculture */" in q)
    assert "toDateTime(" in ref_sql_1
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in ref_sql_1
    assert "toUInt64(4)" in ref_sql_1

    # 2. Same checksums -> hits _INSIGHTS_COVERAGE_CACHE without querying rollup_coverage.
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 1

    # 3. Changed checksums -> cache miss, queries rollup_coverage again.
    state["checksum_mult"] = 11
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 2

    # 4. bypass_history_cache=True (force_refresh) -> queries rollup_coverage even with unchanged checksums.
    evaluator._evaluate(
        svc,
        24.0,
        168.0,
        None,
        "2026-11-26T12:00:00Z",
        False,
        "",
        {},
        frozenset(ALL_FIELDS),
        bypass_history_cache=True,
    )
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 3

    # 5. Exception in read_coverage does not poison _INSIGHTS_COVERAGE_CACHE.
    state["checksum_mult"] = 13
    state["fail_coverage"] = True
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 4

    state["fail_coverage"] = False
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 5


def test_fingerprint_spread_hashes_ip_once_and_tightens_candidates() -> None:
    assert _FP_SPREAD_CANDIDATE_LIMIT == 2000
    present = {"ip", "ja4", "asn", "p_type", "tls_ciphers_sha"}
    active = {"botnet_grouping", "cipher_spread", "residential_fingerprint_dispersion"}

    cand_sql = _build_fingerprint_spread_candidates(active, present=present, rollup_span=None, filter_clause="")
    assert cand_sql.count("HAVING uniqCombined(client_ip) >= 5 LIMIT 2001") == 2
    assert ">= 4" not in cand_sql

    batch = next(b for b in DETECTION_BATCHES if b.name == "fingerprint_spread")
    assert batch.parse_candidates is not None
    at_limit = [{"kind": "fp", "k": f"fp-{i}"} for i in range(2000)] + [{"kind": "cipher", "k": "c-0"}]
    parsed_ok = batch.parse_candidates(active, at_limit)
    assert parsed_ok == {"fp": [f"fp-{i}" for i in range(2000)], "cipher": ["c-0"]}
    above_limit = [{"kind": "fp", "k": f"fp-{i}"} for i in range(2001)]
    assert batch.parse_candidates(active, above_limit) == {"fp": None, "cipher": []}

    fp_sql, _ = _build_fingerprint_spread(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        candidates={"fp": ["fp-1"], "cipher": ["c-1"]},
        candidate_params={},
    )
    assert "cityHash64(client_ip) AS ip_h" in fp_sql
    assert "client_ip AS ip" not in fp_sql
    assert "uniqCombinedIf(ip_h, is_w) AS w_ips" in fp_sql
    assert "uniqCombinedIf(ip_h, is_b) AS b_ips" in fp_sql
    assert "uniqCombinedIf(ip_h, is_w AND p_type != 'hosting') AS res_w_ips" in fp_sql
    assert "uniqCombinedIf(ip_h, is_b AND p_type != 'hosting') AS res_b_ips" in fp_sql
    assert "uniqCombinedIf(ip," not in fp_sql


def test_ip_outlier_samples_24h_and_48h_windows() -> None:
    present = {"ip", "req_header_bytes", "conn_requests"}
    active = {"request_size_anomaly", "connection_abuse"}

    sql_24h, _ = _build_ip_outlier(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
        candidates={"ip": ["198.51.100.1"]},
        candidate_params={},
    )
    assert (
        "((event_timestamp >= {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        " OR (event_timestamp < {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0))"
        in sql_24h
    )
    assert "countIf(is_w AND req_header_bytes > 0) * 4 AS rs_w_total" in sql_24h
    assert "countIf(is_b AND req_header_bytes > 0) * 8 AS rs_b_total" in sql_24h
    assert "countIf(is_w AND conn_requests > 0) * 4 AS ca_w_total" in sql_24h
    assert "countIf(is_b AND conn_requests > 0) * 8 AS ca_b_total" in sql_24h

    sql_48h, _ = _build_ip_outlier(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=48.0,
        candidates={"ip": ["198.51.100.1"]},
        candidate_params={},
    )
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 6 = 0" in sql_48h
    assert "countIf(is_w AND req_header_bytes > 0) * 6 AS rs_w_total" in sql_48h
    assert "countIf(is_w AND conn_requests > 0) * 6 AS ca_w_total" in sql_48h

    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:batch_ip_outlier_candidates */" in q:
            return [{"k": "198.51.100.1"}]
        return []

    client = _FakeRealClickHouseClient(_handler)
    evaluator.insights_endpoint(
        _service(client, "svc-opt24-ip"),
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    ip_out_q = next(q for q, _, _ in client.calls if "/* insight:batch_ip_outlier */" in q)
    assert "countIf(is_w AND req_header_bytes > 0) * 4 AS rs_w_total" in ip_out_q
    assert "countIf(is_w AND conn_requests > 0) * 4 AS ca_w_total" in ip_out_q
