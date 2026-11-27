"""Unit tests for Round 32 ClickHouse network PK sampling, 24h CMCD, and 24h security sampling."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import network as hs_network
from backend.high_scale import security as hs_security
from backend.high_scale.ingest_controller import HighScaleService
from backend.models.analytics import CmcdRequest, NetworkQualityRequest

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]
_RANGE_RE = re.compile(
    r"\(event_timestamp >= toDateTime\((\d+), 'UTC'\) AND event_timestamp < toDateTime\((\d+), 'UTC'\)\)"
)
_DIM_RANGE_RE = re.compile(
    r"\(bucket_start >= toDateTime\((\d+), 'UTC'\) AND bucket_start < toDateTime\((\d+), 'UTC'\)\)"
)


class _FakeRealCHClient(ClickHouseClient):
    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())

    def _has_materialized_cf_columns(self) -> bool:
        return False

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


def _service(client: Any, sid: str = "svc-opt32") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt32-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


def _clear_all_caches() -> None:
    hs_network._network_memo.cache.clear()
    hs_network._network_memo._telemetry_cache.clear()
    hs_network._network_model_cache.clear()
    hs_network._network_json_cache.clear()
    hs_network._pop_rollup_cache.clear()
    hs_network._pop_fact_cache.clear()
    hs_network._SHIELD_ACTIVE_CACHE.clear()
    hs_cmcd._memo.cache.clear()
    hs_security._security_memo.cache.clear()
    hs_security._security_memo._telemetry_cache.clear()


def test_network_time_sample_clause_30d_dense_buckets_uses_pk_ranges_and_24h_dense_keeps_modulo() -> None:
    end = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    for bs in (3600, 300):
        clause = hs_network._network_time_sample_clause(1000, bucket_seconds=bs, start=start_30d, end=end)
        windows = [(int(a), int(b)) for a, b in _RANGE_RE.findall(clause)]
        assert 1 <= len(windows) <= 16
        assert "line_ordinal" not in clause
        assert "intDiv" not in clause

    # Non-hour-aligned 24h window produces 25 hour buckets and stays on monotonic PK windows.
    end_unaligned = datetime(2026, 11, 26, 12, 17, tzinfo=UTC)
    start_24h_unaligned = end_unaligned - timedelta(hours=24)
    unaligned_clause = hs_network._network_time_sample_clause(
        20, bucket_seconds=3600, start=start_24h_unaligned, end=end_unaligned
    )
    unaligned_windows = [(int(a), int(b)) for a, b in _RANGE_RE.findall(unaligned_clause)]
    assert len(unaligned_windows) == 25
    assert "line_ordinal" not in unaligned_clause

    # 24h dense 5m buckets (288 > 25) still keeps line_ordinal % 20 = 0.
    start_24h = end - timedelta(hours=24)
    dense_24h = hs_network._network_time_sample_clause(20, bucket_seconds=300, start=start_24h, end=end)
    assert dense_24h == " AND line_ordinal % 20 = 0"


def test_shielding_analysis_30d_probe_uses_pk_sample_and_caches_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_all_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    client = _FakeRealCHClient(lambda _sql, _params: [{"n": 0}])
    svc = _service(client, "svc-opt32-shield")

    res1 = hs_network._shielding_analysis(svc, start_30d, end, None)
    assert res1.has_data is False
    assert res1.edge_only is True
    assert len(client.calls) == 1
    probe_sql = client.calls[0][0]
    assert "LIMIT 1" in probe_sql
    assert "event_timestamp >= toDateTime(" in probe_sql
    windows = _RANGE_RE.findall(probe_sql)
    assert 1 <= len(windows) <= 16
    sampled_s = sum(int(b) - int(a) for a, b in windows)
    expected_min_s = (30 * 86400) // 50 - 32
    assert sampled_s >= expected_min_s

    client.calls.clear()
    res2 = hs_network._shielding_analysis(svc, start_30d, end, None)
    assert res2.has_data is False
    assert res2.edge_only is True
    assert len(client.calls) == 0


def test_network_minute_dimensions_7d_and_30d_pk_sampling_and_scaling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    monkeypatch.setattr("backend.core.fos_client.get_asn_names", lambda *_a, **_kw: {13335: "Cloudflare"})

    assert hs_network._network_dim_sample_stride(86_400) == 1
    assert hs_network._network_dim_sample_stride(604_800) == 1
    assert hs_network._network_dim_sample_stride(691_200) == 5
    assert hs_network._network_dim_sample_stride(1_209_600) == 10
    assert hs_network._network_dim_sample_stride(1_728_000) == 20
    assert hs_network._network_dim_sample_stride(2_592_000) == 20

    end = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    def _quality_handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM network_minute_dimensions" in sql:
            return [
                {
                    "dimension": "country",
                    "label": "US",
                    "rtt_ms": 20.0,
                    "reqs": 5000,
                    "pop_reqs": 0,
                    "pop_errors": 0,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                },
                {
                    "dimension": "asn",
                    "label": "13335",
                    "rtt_ms": 21.0,
                    "reqs": 4000,
                    "pop_reqs": 0,
                    "pop_errors": 0,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                },
                {
                    "dimension": "pop",
                    "label": "SJC",
                    "rtt_ms": 18.0,
                    "reqs": 4500,
                    "pop_reqs": 5000,
                    "pop_errors": 40,
                    "p50_rtt_us": 18000.0,
                    "p95_ttfb_ms": 42.0,
                },
            ]
        return [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 60

    q_client = _FakeRealCHClient(_quality_handler)
    q_res = hs_network.network_quality(
        _service(q_client, "svc-opt32-nq"),
        NetworkQualityRequest(),
        start_30d.isoformat(),
        end.isoformat(),
    )
    dim_sql = next(sql for sql, _, _ in q_client.calls if "FROM network_minute_dimensions" in sql)
    dim_windows = [(int(a), int(b)) for a, b in _DIM_RANGE_RE.findall(dim_sql)]
    assert 1 <= len(dim_windows) <= 16
    assert all(b > a and (b - a) % 60 == 0 for a, b in dim_windows)
    assert "* 20" in dim_sql
    assert q_res.by_country[0]["reqs"] == 5000

    # Empty sample fallback in get_pop_health retries unsampled without bucket_start disjunction.
    _clear_all_caches()

    def _pop_fallback_handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "UNION ALL" in sql and _DIM_RANGE_RE.search(sql):
            return [
                {
                    "kind": "fact",
                    "pop": "SJC",
                    "req_count": 100,
                    "errors": None,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                    "cache_hits": 80,
                    "bandwidth_bytes": 102400,
                }
            ]
        if "FROM network_minute_dimensions" in sql:
            return [{"pop": "SJC", "req_count": 2000, "errors": 4, "p50_rtt_us": 15000.0, "p95_ttfb_ms": 38.0}]
        return []

    pop_client = _FakeRealCHClient(_pop_fallback_handler)
    pop_res = hs_network.get_pop_health(_service(pop_client, "svc-opt32-pop"), start_30d, end)
    dim_calls = [sql for sql, _, _ in pop_client.calls if "FROM network_minute_dimensions" in sql]
    assert len(dim_calls) == 2
    assert 1 <= len(_DIM_RANGE_RE.findall(dim_calls[0])) <= 16
    assert "* 20" in dim_calls[0]
    assert not _DIM_RANGE_RE.search(dim_calls[1])
    assert pop_res.data[0].pop == "SJC"
    assert pop_res.data[0].requests == 2000


def test_cmcd_24h_samples_sessions_by_4_and_leaves_sub_24h_unsampled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    monkeypatch.setattr("backend.config.is_cmcd_enabled", lambda _cfg: True)
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda _sid, _asns: {7922: "Comcast"})

    assert hs_cmcd._cmcd_sample_stride(3600) == 1
    assert hs_cmcd._cmcd_sample_stride(43200) == 1
    assert hs_cmcd._cmcd_sample_stride(86400) == 4
    assert hs_cmcd._cmcd_sample_stride(604799) == 4
    assert hs_cmcd._cmcd_sample_stride(604800) == 8
    assert hs_cmcd._cmcd_sample_stride(1209599) == 8
    assert hs_cmcd._cmcd_sample_stride(1209600) == 10
    assert hs_cmcd._cmcd_sample_stride(2592000) == 10

    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_24h = end - timedelta(hours=24)
    bucket_epoch = int((end - timedelta(hours=1)).timestamp())

    def _cmcd_handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* cmcd:timeseries */" in sql:
            return [
                {
                    "is_probe": 0,
                    "bucket_ts": bucket_epoch,
                    "events": 25,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_su": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "concurrent_sessions": 5,
                    "_reb_sess": 1,
                    "rebuffer_session_pct": 20.0,
                    "p50_buffer": 10.0,
                    "p95_buffer": 18.0,
                    "p50_buffer_all": 10.0,
                    "p95_buffer_all": 18.0,
                    "starvation_rate": 2.0,
                    "avg_bl_v": 10.0,
                    "avg_bl_all": 10.0,
                    "avg_bitrate": 3000.0,
                    "avg_bitrate_all": 3000.0,
                    "utilization_ratio": 0.5,
                    "utilization_ratio_all": 0.5,
                    "p50_throughput": 8000.0,
                    "p95_throughput": 9000.0,
                    "p99_throughput": 9500.0,
                    "p50_throughput_all": 8000.0,
                    "p95_throughput_all": 9000.0,
                    "p99_throughput_all": 9500.0,
                    "startup_ratio": 1.0,
                },
                {
                    "is_probe": 1,
                    "bucket_ts": 0,
                    "events": 25,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_su": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "concurrent_sessions": 10,
                    "_reb_sess": 2,
                    "avg_bl_v": 10.0,
                    "avg_bl_all": 10.0,
                    "avg_bitrate": 3000.0,
                    "avg_bitrate_all": 3000.0,
                    "p50_throughput": 8000.0,
                    "p50_throughput_all": 8000.0,
                },
            ]
        if "/* cmcd:sessions */" in sql:
            return [
                {"kind": "new_sessions", "k": str(bucket_epoch), "session_count": 3},
                {
                    "kind": "duration",
                    "k": "2",
                    "session_count": 10,
                    "positive_duration_sum": 900.0,
                    "positive_duration_count": 10,
                },
            ]
        if "/* cmcd:session_breakdowns */" in sql:
            return [
                {
                    "kind": "content",
                    "k": "c1",
                    "session_count": 4,
                    "rebuffer_rate": 25.0,
                    "avg_br": 3000.0,
                    "avg_bl": 10.0,
                }
            ]
        if "/* cmcd:breakdowns */" in sql:
            return [{"kind": "ot", "k": "v", "session_count": 25, "request_count": 25}]
        return []

    client = _FakeRealCHClient(_cmcd_handler)
    res_24h = hs_cmcd.cmcd_aggregates(
        _service(client, "svc-opt32-cmcd-24h"),
        CmcdRequest(bucket_seconds=3600, top_n=10),
        start_24h.isoformat(),
        end.isoformat(),
        None,
        False,
    )
    assert client.calls
    for sql, _, _ in client.calls:
        assert "cityHash64(cmcd['sid']) % 4 = 0" in sql
    assert res_24h["overview"]["active_sessions"] == 40
    assert res_24h["overview"]["peak_viewers"] == 20
    assert res_24h["session_duration_dist"][0]["session_count"] == 40
    assert res_24h["top_content"][0]["session_count"] == 16
    assert res_24h["object_type_dist"][0]["request_count"] == 100

    # Sub-24h stays completely unsampled.
    _clear_all_caches()
    client_6h = _FakeRealCHClient(_cmcd_handler)
    res_6h = hs_cmcd.cmcd_aggregates(
        _service(client_6h, "svc-opt32-cmcd-6h"),
        CmcdRequest(bucket_seconds=300, top_n=10),
        (end - timedelta(hours=6)).isoformat(),
        end.isoformat(),
        None,
        False,
    )
    for sql, _, _ in client_6h.calls:
        assert "cityHash64(cmcd['sid']) %" not in sql
    assert res_6h["overview"]["active_sessions"] == 10


def test_security_24h_sampling_and_bot_efficiency_pk_sampling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_all_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: "")
    monkeypatch.setattr("backend.core.fos_client.get_asn_names", lambda *_a, **_kw: {})

    assert hs_security._security_sample_stride(3600) == 1
    assert hs_security._security_sample_stride(43_200) == 1
    assert hs_security._security_sample_stride(86_400) == 4
    assert hs_security._security_sample_stride(259_200) == 10
    assert hs_security._security_sample_stride(604_800) == 40
    assert hs_security._security_sample_stride(1_209_600) == 100
    assert hs_security._security_sample_stride(2_592_000) == 200

    end = datetime(2026, 11, 27, 0, 0, tzinfo=UTC)
    start_24h = end - timedelta(hours=24)
    bucket_epoch = int((end - timedelta(hours=1)).timestamp())

    def _sec_handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* hs_security:overview_5in1 */" in sql:
            return [
                {
                    "bucket_ts": bucket_epoch,
                    "total_rows": 100,
                    "tls_populated": 80,
                    "ipv6_rows": 25,
                    "h_map": {"0-256B": 100},
                    "c_map": {"1 (New)": 100},
                    "p_map": {"hosting": 10},
                    "vb_map": {},
                }
            ]
        if "/* hs_security:tls_and_ips */" in sql:
            return [
                {"kind": "fp", "k": "sha-1", "v1": 4, "v2": 50},
                {"kind": "ip", "k": "203.0.113.5", "v1": 8192, "v2": 0},
            ]
        if "/* hs_security:proxies_combined */" in sql:
            return [
                {
                    "active_proxies_count": 3,
                    "total_requests_count": 15,
                    "distance_mismatches_count": 2,
                    "q_tunnel": 10,
                    "q_wifi": 20,
                    "q_total": 100,
                    "isp_map": {13335: 7},
                    "top_clients": [],
                }
            ]
        if "/* hs_security:bot_efficiency */" in sql:
            return [
                {
                    "kind": "ua",
                    "key": "GPTBot/1.0",
                    "cnt": 25,
                    "resp_bytes": 5000,
                    "site_cnt": 1000,
                    "site_resp_bytes": 100000,
                },
                {
                    "kind": "ref",
                    "key": "chatgpt",
                    "cnt": 5,
                    "resp_bytes": 0,
                    "site_cnt": 0,
                    "site_resp_bytes": 0,
                },
            ]
        return []

    client_24h = _FakeRealCHClient(_sec_handler)
    req = SimpleNamespace(filters={}, bucket_seconds=3600)
    res_24h = hs_security.security_aggregates(
        _service(client_24h, "svc-opt32-sec-24h"),
        req,
        start_24h.isoformat(),
        end.isoformat(),
        sections={
            "bot_efficiency",
            "proxies",
            "tls_fingerprints",
            "top_ips_header",
            "req_size_dist",
            "ipv6_adoption",
        },
    ).model_dump()

    overview_sql = next(q for q, _, _ in client_24h.calls if "/* hs_security:overview_5in1 */" in q)
    assert "AND line_ordinal % 4 = 0" in overview_sql

    bot_sql = next(q for q, _, _ in client_24h.calls if "/* hs_security:bot_efficiency */" in q)
    ua_part, ref_part = bot_sql.split("ref_extracted AS", 1)
    assert 1 <= len(_RANGE_RE.findall(ua_part)) <= 16
    assert 1 <= len(_RANGE_RE.findall(ref_part)) <= 16

    assert res_24h["req_size_dist"] == [{"bucket": "0-256B", "count": 400}]
    assert res_24h["tls_fingerprints"] == [{"fingerprint": "sha-1", "ip_count": 4, "request_count": 200}]
    assert res_24h["bot_efficiency"][0]["request_count"] == 100
    assert res_24h["bot_efficiency"][0]["resp_bytes"] == 20000
    assert res_24h["bot_efficiency"][0]["referrals"] == 20
    assert res_24h["bot_efficiency"][0]["bandwidth_pct"] == 5.0
    assert res_24h["bot_efficiency"][0]["requests_per_referral"] == 5.0
    assert res_24h["bot_efficiency"][0]["bytes_per_referral"] == 1000.0

    # 6h remains completely unsampled.
    _clear_all_caches()
    client_6h = _FakeRealCHClient(_sec_handler)
    res_6h = hs_security.security_aggregates(
        _service(client_6h, "svc-opt32-sec-6h"),
        req,
        (end - timedelta(hours=6)).isoformat(),
        end.isoformat(),
        sections={"bot_efficiency", "proxies", "tls_fingerprints", "top_ips_header", "req_size_dist", "ipv6_adoption"},
    ).model_dump()
    for q, _, _ in client_6h.calls:
        assert "line_ordinal %" not in q
        assert "toDateTime(" not in q
    assert res_6h["bot_efficiency"][0]["request_count"] == 25
    assert res_6h["bot_efficiency"][0]["referrals"] == 5
