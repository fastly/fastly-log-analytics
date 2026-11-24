from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.network import _network_json_cache, _network_memo, _network_model_cache, network_health
from backend.models.analytics import NetworkHealthRequest
from backend.repositories.utils.response_shaping import (
    compute_network_global_summary,
    compute_network_health_score,
)


class _RecordingClient:
    def __init__(self, responder):
        self._responder = responder
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._http = SimpleNamespace(_transport=SimpleNamespace())

    def execute(self, sql: str, params: dict[str, Any] | None = None):
        self.calls.append((sql, dict(params or {})))
        return self._responder(sql, params or {})

    def matching(self, needle: str) -> list[tuple[str, dict[str, Any]]]:
        return [(s, p) for s, p in self.calls if needle in s]


def _clear_network_caches() -> None:
    _network_memo.cache.clear()
    _network_model_cache.clear()
    _network_json_cache.clear()


@pytest.mark.no_postgres
def test_compute_network_health_score_4arg_matches_cloud_formula() -> None:
    # Matches frontend/lib/cloud-engine/network.ts computeNetworkHealthScore (4-arg):
    # rtt_ratio = 40_000 / 20_000 = 2.0 -> rtt_score = 50.0
    # ploss = 0.01 (1%) -> ploss_score = 80.0
    # error_pct = 2.0 -> error_score = 80.0
    # composite = 0.4*50 + 0.35*80 + 0.25*80 = 68.0
    assert compute_network_health_score(40_000.0, 20_000.0, 0.01, 2.0) == 68.0
    # Equivalent when ploss is already > 1 (percentage form, e.g. 2.0 = 2%)
    # rtt_ratio = 1.0 -> 100; ploss_pct = 2.0 -> 60; error_pct = 0 -> 100; 40 + 21 + 25 = 86.0
    assert compute_network_health_score(20_000.0, 20_000.0, 2.0, 0.0) == 86.0
    # High RTT ratio alone (ploss=0, error=0) still penalizes health score
    # rtt_ratio = 3.0 -> rtt_score = 0; composite = 0 + 35 + 25 = 60.0
    assert compute_network_health_score(60_000.0, 20_000.0, 0.0, 0.0) == 60.0


@pytest.mark.no_postgres
def test_compute_network_global_summary_weights_avg_rtt_by_requests() -> None:
    summary = compute_network_global_summary(
        leaderboard=[],
        rtt_values=[20_000.0, 300_000.0],
        rtt_weights=[10_000, 10],
        total_reqs=10_010,
        zero_as_int=False,
    )
    # Request-weighted: (20*10000 + 300*10) / 10010 = 20.28ms -> 20.3ms (unweighted would be 160.0ms)
    assert summary["avg_rtt_ms"] == 20.3


@pytest.mark.no_postgres
def test_network_health_map_and_metro_scores_use_rtt_and_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start_s, end_s = "2026-09-14T00:00:00Z", "2026-09-15T00:00:00Z"
    b0 = int(datetime(2026, 9, 14, 0, 0, tzinfo=UTC).timestamp())

    def responder(sql: str, params: dict[str, Any]):
        if "FROM request_aggregates" in sql:
            return [{"asn": 7922, "cnt": 3000}, {"asn": 15169, "cnt": 30}]
        if "FROM network_minute_dimensions" in sql:
            return []
        if "AS throughput_bps" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": b0,
                    "reqs": 100,
                    "throughput_bps": 1_500_000.0,
                    "rtt_med_us": 20_000.0,
                    "rtt_baseline_us": 18_000.0,
                    "rtt_congestion_us": 2_000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1_000.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 15169,
                    "bucket_ts": b0,
                    "reqs": 1,
                    "throughput_bps": 1_500_000.0,
                    "rtt_med_us": 300_000.0,
                    "rtt_baseline_us": 100_000.0,
                    "rtt_congestion_us": 200_000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1_000.0,
                    "error_pct": 0.0,
                },
            ]
        if "AS lat" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "region": "California",
                    "metro": "807",
                    "bucket_ts": b0,
                    "lat": "37.33",
                    "lon": "-121.89",
                    "rtt_med_us": 60_000.0,
                    "rtt_p10_us": 20_000.0,
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "reqs": 50,
                }
            ]
        if "SELECT count() AS n" in sql:
            return [{"n": 0}]
        return []

    client = _RecordingClient(responder)
    svc = HighScaleService(service_id="svc-parity-all", client=client, cursor_secret=b"secret", request_watermark=None)
    req = NetworkHealthRequest(start_time=start_s, end_time=end_s, bucket_seconds=3600, top_n=30)
    resp = network_health(svc, req, start_s, end_s)

    # SQL for map selects quantileTDigest median + p10 RTT
    ((map_sql, _),) = client.matching("AS lat")
    assert "quantileTDigest(0.5)(_rtt) AS rtt_med_us" in map_sql
    assert "quantileTDigest(0.1)(_rtt) AS rtt_p10_us" in map_sql

    # sample_stride = 10 (cityHash64) on 24h window scales heatmap, map_buckets, metro_leaderboard, and summary
    assert resp.heatmap[0].buckets[0].reqs == 1000
    assert resp.summary is not None and resp.summary.total_reqs == 1010
    city_cell = resp.map_buckets[0].cities[0]
    assert city_cell.reqs == 500
    assert resp.metro_leaderboard[0].total_reqs == 500

    # Map & metro health score reflect RTT ratio 60k/20k = 3.0 -> 60.0 (not 100.0!)
    assert city_cell.health_score == 60.0
    assert resp.metro_leaderboard[0].health_score == 60.0

    # Summary avg_rtt_ms is request-weighted: (20*1000 + 300*10) / 1010 = 22.77ms -> 22.8ms
    assert resp.summary.avg_rtt_ms == 22.8


@pytest.mark.no_postgres
def test_network_health_separate_metro_query_scales_and_uses_rtt(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start_s, end_s = "2026-09-14T00:00:00Z", "2026-09-15T00:00:00Z"
    b0 = int(datetime(2026, 9, 14, 0, 0, tzinfo=UTC).timestamp())

    def responder(sql: str, params: dict[str, Any]):
        if "FROM request_aggregates" in sql:
            return [{"asn": 7922, "cnt": 3000}]
        if "FROM network_minute_dimensions" in sql:
            return []
        if "AS throughput_bps" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": b0,
                    "reqs": 100,
                    "throughput_bps": 1_500_000.0,
                    "rtt_med_us": 20_000.0,
                    "rtt_baseline_us": 20_000.0,
                    "rtt_congestion_us": 0.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 0.0,
                    "error_pct": 0.0,
                }
            ]
        if "AS lat" in sql:
            return []
        if "AS total_reqs" in sql:
            return [
                {
                    "country": "US",
                    "city": "denver",
                    "region": "Colorado",
                    "metro": "751",
                    "rtt_med_us": 40_000.0,
                    "rtt_p10_us": 20_000.0,
                    "avg_ploss": 0.01,
                    "error_pct": 2.0,
                    "total_reqs": 40,
                }
            ]
        if "SELECT count() AS n" in sql:
            return [{"n": 0}]
        return []

    client = _RecordingClient(responder)
    svc = HighScaleService(service_id="svc-parity-asn", client=client, cursor_secret=b"secret", request_watermark=None)
    req = NetworkHealthRequest(start_time=start_s, end_time=end_s, bucket_seconds=3600, top_n=30, map_asn="7922")
    resp = network_health(svc, req, start_s, end_s)

    ((metro_sql, _),) = client.matching("AS total_reqs")
    assert "quantileTDigest(0.5)(_rtt) AS rtt_med_us" in metro_sql
    assert "quantileTDigest(0.1)(_rtt) AS rtt_p10_us" in metro_sql

    assert resp.metro_leaderboard[0].total_reqs == 400
    assert resp.metro_leaderboard[0].health_score == 68.0


@pytest.mark.no_postgres
def test_compute_network_global_summary_country_agg_and_weighted_health() -> None:
    summary = compute_network_global_summary(
        leaderboard=[
            {"label": "Big ISP", "total_reqs": 9900, "health_score_now": 80.0},
            {"label": "Tiny ISP", "total_reqs": 50, "health_score_now": 20.0},
        ],
        rtt_values=[20_000.0],
        rtt_weights=[9950],
        total_reqs=9950,
        map_buckets=[
            {
                "bucket_idx": 0,
                "bucket": "2026-09-14T00:00:00Z",
                "cities": [
                    {"country": "US", "city_idx": 0, "health_score": 82.0, "reqs": 8000},
                    {"country": "FR", "city_idx": 1, "health_score": 55.0, "reqs": 500},
                    {"country": "KR", "city_idx": 2, "health_score": 10.0, "reqs": 20},
                ],
            }
        ],
        cities_list=[
            {"name": "Chicago", "lat": 41.8, "lon": -87.6},
            {"name": "", "lat": 48.8, "lon": 2.3},
            {"name": "Seoul", "lat": 37.5, "lon": 126.9},
        ],
        zero_as_int=False,
    )
    assert summary["global_health_score"] == 79.7
    assert summary["worst_country"] == {"label": "France", "score": 55.0}


@pytest.mark.no_postgres
def test_network_health_no_periodic_minute_sampling_and_no_trailing_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start_s, end_s = "2026-09-14T12:00:00Z", "2026-09-14T18:00:00Z"
    b0 = int(datetime(2026, 9, 14, 12, 0, tzinfo=UTC).timestamp())

    def responder(sql: str, params: dict[str, Any]):
        if "FROM request_aggregates" in sql:
            return [{"asn": 7922, "cnt": 5000}]
        if "FROM network_minute_dimensions" in sql:
            return []
        if "AS throughput_bps" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": b0,
                    "reqs": 5000,
                    "throughput_bps": 1_500_000.0,
                    "rtt_med_us": 40_000.0,
                    "rtt_p10_us": 20_000.0,
                    "rtt_baseline_us": 20_000.0,
                    "rtt_congestion_us": 20_000.0,
                    "avg_ploss": 0.01,
                    "rtt_jitter_us": 1_000.0,
                    "error_pct": 2.0,
                }
            ]
        if "AS lat" in sql:
            return [
                {
                    "country": "US",
                    "city": "chicago",
                    "region": "Illinois",
                    "metro": "602",
                    "bucket_ts": b0,
                    "lat": "41.88",
                    "lon": "-87.63",
                    "rtt_med_us": 40_000.0,
                    "rtt_p10_us": 20_000.0,
                    "avg_ploss": 0.01,
                    "error_pct": 2.0,
                    "reqs": 5000,
                }
            ]
        return []

    client = _RecordingClient(responder)
    svc = HighScaleService(service_id="svc-6h-exact", client=client, cursor_secret=b"secret", request_watermark=None)
    req = NetworkHealthRequest(start_time=start_s, end_time=end_s, bucket_seconds=3600, top_n=30)
    resp = network_health(svc, req, start_s, end_s)

    assert len(resp.buckets) == 6
    assert resp.buckets[-1] == "2026-09-14T17:00:00"
    ((hm_sql, _),) = client.matching("AS throughput_bps")
    assert "toMinute" not in hm_sql
    assert "toSecond" not in hm_sql
    assert "quantileTDigest(0.1)" in hm_sql
    assert resp.heatmap[0].buckets[0].health_score == 68.0
    assert resp.summary is not None and resp.summary.worst_country is not None
    assert resp.summary.worst_country.label == "United States"


@pytest.mark.no_postgres
def test_get_pop_health_parity_errors_hits_quantiles_and_no_minute_stride(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.high_scale.network import get_pop_health

    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    end = datetime(2026, 9, 14, 18, 0, tzinfo=UTC)

    def responder(sql: str, params: dict[str, Any]):
        if "FROM network_minute_dimensions" in sql:
            return [{"pop": "SJC", "exact_requests": 1000, "p50_rtt_us": 20000.0, "p95_ttfb_ms": 999.0}]
        if "FROM request_facts" in sql:
            return [
                {
                    "pop": "SJC",
                    "requests": 1000,
                    "errors": 5,
                    "p50_rtt_us": 20000.0,
                    "p95_ttfb_ms": 85.0,
                    "cache_hits": 250,
                    "bandwidth_bytes": 123456,
                }
            ]
        return []

    client = _RecordingClient(responder)
    svc = HighScaleService(service_id="svc-pop-parity", client=client, cursor_secret=b"secret", request_watermark=None)
    resp = get_pop_health(svc, start, end)
    ((fact_sql, _),) = client.matching("FROM request_facts")
    assert "toMinute" not in fact_sql
    assert ">= 500" in fact_sql and ">= 400" not in fact_sql
    assert "startsWith(upper(custom_fields['cache']), 'HIT')" in fact_sql
    assert "quantileTDigest(0.5)" in fact_sql and "quantileTDigest(0.95)" in fact_sql
    assert resp.data[0].p95_ttfb_ms == 85.0


@pytest.mark.no_postgres
def test_get_transport_metrics_retrans_pct_and_no_minute_stride(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.high_scale.network import get_transport_metrics

    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start_s, end_s = "2026-09-14T12:00:00Z", "2026-09-14T18:00:00Z"

    def responder(sql: str, params: dict[str, Any]):
        return [
            {
                "total_reqs": 1000,
                "total_bytes": 50000,
                "rtt_quants": [20000.0, 40000.0, 50000.0, 80000.0],
                "avg_retrans": 0.4,
                "avg_ploss_pct": 5.0,
                "avg_del_rate_mbps": 12.0,
                "proto_map": {"HTTP/2": 1000},
            }
        ]

    client = _RecordingClient(responder)
    svc = HighScaleService(service_id="svc-tr-parity", client=client, cursor_secret=b"secret", request_watermark=None)
    resp = get_transport_metrics(svc, start_s, end_s)
    ((tr_sql, _),) = client.matching("FROM request_facts")
    assert "toMinute" not in tr_sql
    assert "countIf(toFloat64OrNull(custom_fields['retrans']) > 0) * 100.0" in tr_sql
    assert resp.retrans_rate_pct == 0.4


@pytest.mark.no_postgres
def test_network_quality_mean_rtt_country_labels_and_hash_scatter(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.high_scale.network import network_quality
    from backend.models.analytics import NetworkQualityRequest

    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start_s, end_s = "2026-09-14T12:00:00Z", "2026-09-14T18:00:00Z"

    def responder(sql: str, params: dict[str, Any]):
        if "FROM network_minute_dimensions" in sql:
            return [
                {"dimension": "country", "label": "US", "rtt_ms": 33.4, "reqs": 500},
                {"dimension": "asn", "label": "7922", "rtt_ms": 22.1, "reqs": 500},
                {"dimension": "pop", "label": "SJC", "rtt_ms": 18.2, "reqs": 500},
            ]
        if "custom_fields['region']" in sql:
            return [{"label": "California", "rtt_ms": 19.5, "reqs": 500}]
        if "cache_state" in sql:
            return [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 200
        return []

    client = _RecordingClient(responder)
    svc = HighScaleService(service_id="svc-nq-parity", client=client, cursor_secret=b"secret", request_watermark=None)
    resp = network_quality(svc, NetworkQualityRequest(), start_s, end_s)

    ((rollup_sql, _),) = client.matching("FROM network_minute_dimensions")
    assert "tcp_rtt_sum" in rollup_sql
    ((reg_sql, _),) = client.matching("custom_fields['region']")
    assert "toMinute" not in reg_sql
    assert "avg(" in reg_sql and "median(" not in reg_sql
    assert rollup_sql == reg_sql
    sc_sql = client.matching("cache_state")[0][0]
    assert "cityHash64(event_id)" in sc_sql and "toUnixTimestamp64Milli" not in sc_sql
    assert resp.by_country[0].label == "United States" and resp.by_country[0].value == "US"
    assert len(client.calls) == 2


@pytest.mark.no_postgres
def test_network_page_query_consolidation_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.high_scale.network import get_pop_health

    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    start_s, end_s = "2026-09-14T00:00:00Z", "2026-09-15T00:00:00Z"
    b0 = int(datetime(2026, 9, 14, 0, 0, tzinfo=UTC).timestamp())

    def health_responder(sql: str, params: dict[str, Any]):
        if "FROM request_aggregates" in sql:
            return [{"asn": 7922, "cnt": 1000}]
        if "AS throughput_bps" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": b0,
                    "reqs": 1000,
                    "throughput_bps": 1_500_000.0,
                    "rtt_med_us": 40_000.0,
                    "rtt_p10_us": 20_000.0,
                    "rtt_p95_us": 90_000.0,
                    "rtt_p99_us": 120_000.0,
                    "rtt_baseline_us": 20_000.0,
                    "rtt_congestion_us": 20_000.0,
                    "avg_ploss": 0.01,
                    "rtt_jitter_us": 1_000.0,
                    "error_pct": 1.0,
                    "speed_map": {"broadband": 800, "cable": 200},
                }
            ]
        if "AS lat" in sql:
            return []
        return []

    health_client = _RecordingClient(health_responder)
    svc = HighScaleService(
        service_id="svc-nq-count", client=health_client, cursor_secret=b"secret", request_watermark=None
    )
    resp = network_health(
        svc, NetworkHealthRequest(start_time=start_s, end_time=end_s, bucket_seconds=3600, top_n=30), start_s, end_s
    )
    assert len(health_client.calls) == 4
    assert resp.leaderboard[0].p95_rtt_us == 90000.0
    assert resp.leaderboard[0].p99_rtt_us == 120000.0
    assert resp.leaderboard[0].c_speed_mix == {"broadband": 0.8, "cable": 0.2}

    # get_pop_health on 7d (pop_stride > 1) merges rollup + facts into 1 UNION ALL query.
    def pop_responder(sql: str, params: dict[str, Any]):
        if "UNION ALL" in sql:
            return [
                {
                    "kind": "rollup",
                    "pop": "SJC",
                    "requests": 5000,
                    "errors": 0,
                    "p50_rtt_us": 21_000.0,
                    "p95_ttfb_ms": 0.0,
                    "cache_hits": 0,
                    "bandwidth_bytes": 0,
                },
                {
                    "kind": "fact",
                    "pop": "SJC",
                    "requests": 500,
                    "errors": 10,
                    "p50_rtt_us": 25_000.0,
                    "p95_ttfb_ms": 80.0,
                    "cache_hits": 250,
                    "bandwidth_bytes": 10000,
                },
            ]
        return []

    pop_client = _RecordingClient(pop_responder)
    pop_svc = HighScaleService(
        service_id="svc-pop-7d", client=pop_client, cursor_secret=b"secret", request_watermark=None
    )
    pop_resp = get_pop_health(pop_svc, datetime(2026, 9, 8, 0, 0, tzinfo=UTC), datetime(2026, 9, 15, 0, 0, tzinfo=UTC))
    assert len(pop_client.calls) == 1
    assert pop_resp.data[0].requests == 5000
    assert pop_resp.data[0].errors == 100
    assert pop_resp.data[0].p50_rtt_us == 25000.0
    assert pop_resp.data[0].p95_ttfb_ms == 80.0
