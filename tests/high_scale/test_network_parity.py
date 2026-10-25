"""High-scale Network endpoints return real data with the standard path's contract.

Each test drives the ClickHouse-backed functions through a recording client so
the emitted SQL/params (filters, windows, sections) are asserted alongside the
shaped response.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from backend.high_scale.network import (
    get_pop_health,
    get_transport_metrics,
    network_bundle,
    network_health,
    network_quality,
)
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.models.common import FilterSpec
from backend.models.network import NetworkBundleRequest, NetworkHealthRequest, NetworkQualityRequest

_START = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
_END = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
_START_S = "2026-09-15T00:00:00Z"
_END_S = "2026-09-15T01:00:00Z"
_B0 = int(_START.timestamp())
_ASN_NAMES = {7922: "Comcast Cable Communications"}


class RecordingClient:
    def __init__(self, responder):
        self.calls: list[tuple[str, dict]] = []
        self._responder = responder

    def execute(self, sql, params=None):
        self.calls.append((sql, dict(params or {})))
        return self._responder(sql, params or {})

    def matching(self, needle: str) -> list[tuple[str, dict]]:
        return [c for c in self.calls if needle in c[0]]


def _service(responder=lambda sql, params: []) -> tuple[HighScaleService, RecordingClient]:
    client = RecordingClient(responder)
    return HighScaleService(service_id="svc", client=client, cursor_secret=b"s", request_watermark=None), client


@pytest.fixture(autouse=True)
def _no_asn_network_lookup():
    def lookup(_service_id, asns):
        return {a: _ASN_NAMES.get(a, f"AS{a}") for a in asns}

    with patch("backend.core.duckdb.get_asn_names", side_effect=lookup):
        yield


def test_quality_scatter_returns_sampled_rtt_vs_ttfb_points():
    def responder(sql, params):
        if "AS cache_state" in sql:
            return [
                {"rtt_ms": 20.123, "ttfb_ms": 55.557, "cache_state": "HIT"},
                {"rtt_ms": 31.0, "ttfb_ms": 140.0, "cache_state": "UNKNOWN"},
            ]
        return []

    service, client = _service(responder)
    resp = network_quality(service, NetworkQualityRequest(start_time=_START_S, end_time=_END_S), _START_S, _END_S)

    assert [(p.rtt_ms, p.ttfb_ms, p.cache) for p in resp.scatter] == [(20.12, 55.56, "HIT"), (31.0, 140.0, "UNKNOWN")]
    ((sql, params),) = client.matching("AS cache_state")
    assert "LIMIT 2000" in sql
    assert "toFloat64OrNull(custom_fields['ttfb']) > 0" in sql
    assert "toFloat64OrNull(custom_fields['tcp_rtt']) > 0" in sql
    assert params["service_id"] == "svc"


def test_quality_honors_filters_on_every_query_and_enriches_asn_labels():
    def responder(sql, params):
        if "toInt32OrNull(custom_fields['asn']) AS label" in sql:
            return [{"label": 7922, "rtt_ms": 20.0, "reqs": 10}]
        return []

    service, client = _service(responder)
    req = NetworkQualityRequest(
        start_time=_START_S, end_time=_END_S, filters={"country": FilterSpec(mode="include", values=["US", "CA"])}
    )
    resp = network_quality(service, req, _START_S, _END_S)

    assert len(client.calls) == 5  # four bars + scatter
    for sql, params in client.calls:
        assert "country IN ({filter_0_0:String}, {filter_0_1:String})" in sql
        assert params["filter_0_0"] == "US" and params["filter_0_1"] == "CA"
    assert resp.by_asn[0].value == "7922"
    assert resp.by_asn[0].label == "Comcast Cable Communications (7922)"


def test_quality_scatter_query_failure_is_not_masked_as_empty():
    def responder(sql, params):
        if "AS cache_state" in sql:
            raise RuntimeError("clickhouse down")
        return []

    service, _ = _service(responder)
    with pytest.raises(RuntimeError, match="clickhouse down"):
        network_quality(service, NetworkQualityRequest(), _START_S, _END_S)


def test_pop_health_aggregates_per_pop_like_the_standard_live_sql():
    def responder(sql, params):
        return [
            {
                "pop": "SJC",
                "requests": 200,
                "errors": 10,
                "p50_rtt_us": 18000.0,
                "p95_ttfb_ms": 320.5,
                "cache_hits": 150,
                "bandwidth_bytes": 4096,
            },
            {
                "pop": "LHR",
                "requests": 50,
                "errors": 0,
                "p50_rtt_us": None,
                "p95_ttfb_ms": None,
                "cache_hits": 0,
                "bandwidth_bytes": None,
            },
        ]

    service, client = _service(responder)
    resp = get_pop_health(service, _START, _END)

    sjc, lhr = resp.data
    assert (sjc.pop, sjc.requests, sjc.errors, sjc.error_rate) == ("SJC", 200, 10, 5.0)
    assert (sjc.p50_rtt_us, sjc.p95_ttfb_ms, sjc.cache_hit_rate, sjc.bandwidth_bytes) == (18000.0, 320.5, 75.0, 4096)
    assert (lhr.p50_rtt_us, lhr.p95_ttfb_ms, lhr.cache_hit_rate, lhr.bandwidth_bytes) == (None, None, 0.0, 0)
    ((sql, params),) = client.calls
    assert "countIf(toInt32OrZero(custom_fields['status']) >= 400 OR toInt32OrZero(custom_fields['status']) = 0)" in sql
    assert "'HIT', 'HIT-STALE'" in sql
    assert params["start"] == _START and params["end"] == _END


def test_pop_health_defaults_to_trailing_24h_and_treats_naive_bounds_as_utc():
    service, client = _service()
    get_pop_health(service, None, None)
    _, params = client.calls[0]
    assert (params["end"] - params["start"]).total_seconds() == 86400

    service, client = _service()
    get_pop_health(service, datetime(2026, 9, 15, 0, 0), datetime(2026, 9, 15, 1, 0))
    _, params = client.calls[0]
    assert params["start"].tzinfo is UTC and params["end"].tzinfo is UTC


def test_health_threads_filters_map_asn_and_reports_throughput():
    def responder(sql, params):
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": _B0,
                    "reqs": 100,
                    "throughput_bps": 1_250_000.4,
                    "rtt_med_us": 20000.0,
                    "rtt_baseline_us": 18000.0,
                    "rtt_congestion_us": 2000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                }
            ]
        return []

    service, client = _service(responder)
    req = NetworkHealthRequest(
        start_time=_START_S,
        end_time=_END_S,
        sections=["summary", "heatmap", "map_buckets"],
        map_asn="7922",
        filters={
            "country": FilterSpec(mode="exclude", values=["CN"]),
            "asn": FilterSpec(mode="include", values=["15169"]),
        },
    )
    resp = network_health(service, req, _START_S, _END_S)

    assert resp.heatmap[0].buckets[0].throughput_bps == 1_250_000.0
    assert resp.heatmap[0].label == "Comcast Cable Communications (7922)"
    assert resp.leaderboard[0].label == "Comcast Cable Communications (7922)"

    assert len(client.calls) == 5
    for sql, params in client.calls:
        assert "country NOT IN ({filter_0_0:String})" in sql
        assert "15169" not in params.values(), "the asn filter is replaced by map_asn"
    ((map_sql, map_params),) = client.matching("AS lat")
    assert "toInt32OrNull(custom_fields['asn']) = {map_asn:Int32}" in map_sql
    assert map_params["map_asn"] == 7922
    for needle in ("AS region", "GROUP BY asn, c_speed", "quantile(0.95)"):
        assert "map_asn" not in client.matching(needle)[0][0]


def test_health_shielding_analysis_is_computed_from_rid_prid_pairs():
    def responder(sql, params):
        if "INNER JOIN" in sql:
            return [
                {
                    "edge_pop": "DEN",
                    "shield_pop": "IAD",
                    "requests": 400,
                    "p50_ms": 40.0,
                    "p95_ms": 90.0,
                    "p99_ms": 120.0,
                },
                {
                    "edge_pop": "LHR",
                    "shield_pop": "IAD",
                    "requests": 10,
                    "p50_ms": 300.0,
                    "p95_ms": 500.0,
                    "p99_ms": None,
                },
            ]
        return []

    service, client = _service(responder)
    req = NetworkHealthRequest(
        start_time=_START_S,
        end_time=_END_S,
        sections=["shielding_analysis"],
        filters={"pop": FilterSpec(mode="include", values=["DEN"])},
    )
    pop_coords = {"DEN": (39.86, -104.67), "IAD": (38.94, -77.46), "LHR": (51.47, -0.45)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=pop_coords):
        resp = network_health(service, req, _START_S, _END_S)

    shield = resp.shielding_analysis
    assert shield is not None and shield.has_data is True
    assert [(r.edge_pop, r.shield_pop, r.requests) for r in shield.rows] == [("DEN", "IAD", 400), ("LHR", "IAD", 10)]
    assert shield.rows[0].p95_ms == 90.0 and shield.rows[1].p99_ms is None
    assert shield.rows[0].distance_km is not None  # POP geometry enrichment ran
    assert (shield.total_routes, shield.truncated) == (2, False)
    # Core scans are skipped when only shielding is requested.
    assert len(client.calls) == 1
    sql, params = client.calls[0]
    edge_cte, shield_cte = sql.split("INNER JOIN")
    assert "custom_fields[{filter_k0:String}] IN ({filter_0_0:String})" in edge_cte
    assert params["filter_k0"] == "pop"
    assert "filter_0_0" not in shield_cte, "edge filters must not strip the shield hit"


def test_health_shielding_distinguishes_edge_only_from_unmatched():
    def edge_only(sql, params):
        return [{"n": 0}] if "SELECT count() AS n" in sql else []

    service, _ = _service(edge_only)
    resp = network_health(service, NetworkHealthRequest(sections=["shielding_analysis"]), _START_S, _END_S)
    assert resp.shielding_analysis.has_data is False and resp.shielding_analysis.edge_only is True

    def unmatched(sql, params):
        return [{"n": 1}] if "SELECT count() AS n" in sql else []

    service, _ = _service(unmatched)
    resp = network_health(service, NetworkHealthRequest(sections=["shielding_analysis"]), _START_S, _END_S)
    assert resp.shielding_analysis.has_data is False and resp.shielding_analysis.edge_only is None


def test_health_skips_shielding_when_only_core_sections_requested():
    service, client = _service()
    network_health(service, NetworkHealthRequest(sections=["map_buckets", "cities"]), _START_S, _END_S)
    assert client.calls
    assert not client.matching("INNER JOIN")


def test_transport_honors_filters_and_reports_missing_fields():
    def responder(sql, params):
        if "seen_tcp_rtt" in sql:
            return [
                {
                    "total_reqs": 100,
                    "total_bytes": 10,
                    "rtt_p50": 10.0,
                    "rtt_p90": 20.0,
                    "rtt_p95": 30.0,
                    "rtt_p99": 40.0,
                    "avg_retrans": None,
                    "avg_ploss_pct": 0.5,
                    "avg_del_rate_mbps": None,
                    "seen_tcp_rtt": 100,
                    "seen_retrans": 0,
                    "seen_ploss": 80,
                    "seen_delivery_rate": 0,
                }
            ]
        return []

    service, client = _service(responder)
    resp = get_transport_metrics(service, _START_S, _END_S, {"pop": FilterSpec(mode="include", values=["SJC"])})

    assert resp.missing_fields == ["retrans", "delivery_rate"]
    assert resp.has_transport_data is True
    assert len(client.calls) == 2
    for sql, params in client.calls:
        assert "custom_fields[{filter_k0:String}] IN ({filter_0_0:String})" in sql
        assert params["filter_k0"] == "pop"
        assert params["filter_0_0"] == "SJC"


def test_transport_with_no_logged_fields_flags_no_transport_data():
    def responder(sql, params):
        if "seen_tcp_rtt" in sql:
            return [{"total_reqs": 5, "seen_tcp_rtt": 0, "seen_retrans": 0, "seen_ploss": 0, "seen_delivery_rate": 0}]
        return []

    service, _ = _service(responder)
    resp = get_transport_metrics(service, _START_S, _END_S)
    assert resp.has_transport_data is False
    assert resp.missing_fields == ["tcp_rtt", "retrans", "ploss", "delivery_rate"]


def test_bundle_returns_real_pop_health_and_scatter_and_passes_filters():
    def responder(sql, params):
        if "AS cache_state" in sql:
            return [{"rtt_ms": 10.0, "ttfb_ms": 50.0, "cache_state": "MISS"}]
        if "AS bandwidth_bytes" in sql:
            return [
                {
                    "pop": "SJC",
                    "requests": 10,
                    "errors": 1,
                    "p50_rtt_us": 1.0,
                    "p95_ttfb_ms": 2.0,
                    "cache_hits": 5,
                    "bandwidth_bytes": 7,
                }
            ]
        return []

    service, client = _service(responder)
    req = NetworkBundleRequest(
        start_time=_START_S,
        end_time=_END_S,
        filters={"country": FilterSpec(mode="include", values=["US"])},
    )
    resp = network_bundle(service, req, _START_S, _END_S)

    assert resp.pop_health and resp.pop_health[0].pop == "SJC"
    assert resp.quality.scatter[0].cache == "MISS"
    assert resp.transport is not None and resp.health is not None
    for sql, params in client.matching("AS cache_state") + client.matching("seen_tcp_rtt"):
        assert params["filter_0_0"] == "US"
    ((_, pop_params),) = client.matching("AS bandwidth_bytes")
    assert "filter_0_0" not in pop_params, "pop-health has no filters on the standard path"


def test_bundle_honors_section_selection():
    service, client = _service()
    req = NetworkBundleRequest(start_time=_START_S, end_time=_END_S, sections=["pop_health"])
    resp = network_bundle(service, req, _START_S, _END_S)
    assert resp.health is None and resp.quality is None and resp.transport is None
    assert resp.pop_health == []
    assert len(client.calls) == 1
    observation = resp.component_observations["pop_health"]
    assert observation.response_cache.state == "bypass"
    assert observation.section_timings
    assert observation.section_timings[0].model_extra["source"] == "fact"


class _FailingClient:
    """Raises on statements containing ``marker`` (every statement when empty); answers the rest with no rows."""

    def __init__(self, marker: str = "") -> None:
        self._marker = marker

    def execute(self, sql, params=None):
        if self._marker in sql:
            raise RuntimeError("clickhouse down")
        return []


def _failing_service(marker: str = "") -> HighScaleService:
    return HighScaleService(service_id="svc", client=_FailingClient(marker), cursor_secret=b"s", request_watermark=None)


@pytest.mark.parametrize(
    "call",
    [
        lambda s: network_health(
            s, NetworkHealthRequest(start_time=_START_S, end_time=_END_S, sections=["heatmap"]), _START_S, _END_S
        ),
        lambda s: network_quality(s, NetworkQualityRequest(start_time=_START_S, end_time=_END_S), _START_S, _END_S),
        lambda s: get_transport_metrics(s, _START_S, _END_S),
    ],
    ids=["health", "quality", "transport"],
)
def test_clickhouse_failures_propagate_instead_of_reading_as_empty_data(call) -> None:
    with pytest.raises(RuntimeError, match="clickhouse down"):
        call(_failing_service())


@pytest.mark.parametrize(
    ("marker", "call"),
    [
        (
            "GROUP BY label",
            lambda s: network_quality(s, NetworkQualityRequest(start_time=_START_S, end_time=_END_S), _START_S, _END_S),
        ),
        ("proto_clean", lambda s: get_transport_metrics(s, _START_S, _END_S)),
    ],
    ids=["quality-dimension", "transport-protocol-share"],
)
def test_a_single_failing_query_is_not_hidden_behind_a_partial_response(marker, call) -> None:
    with pytest.raises(RuntimeError, match="clickhouse down"):
        call(_failing_service(marker))


# ── Merged from test_network_health.py ───────────────────────────────


class _Req:
    """Minimal stand-in for NetworkHealthRequest / NetworkQualityRequest."""

    start_time = _START.isoformat().replace("+00:00", "Z")
    end_time = _END.isoformat().replace("+00:00", "Z")
    bucket_seconds = 300
    top_n = 30
    region_country = "US"


_B0 = int(datetime(2026, 9, 15, 0, 0, tzinfo=UTC).timestamp())
_B1 = int(datetime(2026, 9, 15, 0, 15, tzinfo=UTC).timestamp())


class FakeClient:
    """Routes each ClickHouse query to representative rows by a unique marker."""

    def execute(self, sql, params=None):
        if "INNER JOIN" in sql:  # shielding self-join: covered in test_network_parity.py
            return []
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            # Per-(asn, bucket) heatmap inputs. AS7922 is clean; AS15169 is
            # congested + lossy so the computed health score is < 100.
            return [
                {
                    "asn": 7922,
                    "bucket_ts": _B0,
                    "reqs": 100,
                    "rtt_med_us": 20000.0,
                    "rtt_baseline_us": 18000.0,
                    "rtt_congestion_us": 2000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 7922,
                    "bucket_ts": _B1,
                    "reqs": 80,
                    "rtt_med_us": 21000.0,
                    "rtt_baseline_us": 18500.0,
                    "rtt_congestion_us": 2500.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1200.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 15169,
                    "bucket_ts": _B0,
                    "reqs": 60,
                    "rtt_med_us": 90000.0,
                    "rtt_baseline_us": 30000.0,
                    "rtt_congestion_us": 150000.0,
                    "avg_ploss": 0.03,
                    "rtt_jitter_us": 80000.0,
                    "error_pct": 8.0,
                },
            ]
        if "AS lat" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "lat": 37.33,
                    "lon": -121.89,
                    "metro": "807",
                    "bucket_ts": _B0,
                    "rtt_med_us": 20000.0,
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "reqs": 100,
                },
                {
                    "country": "US",
                    "city": "boston",
                    "lat": 42.36,
                    "lon": -71.06,
                    "metro": "506",
                    "bucket_ts": _B1,
                    "rtt_med_us": 90000.0,
                    "avg_ploss": 0.03,
                    "error_pct": 8.0,
                    "reqs": 60,
                },
            ]
        if "AS region" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "region": "California",
                    "metro": "807",
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "total_reqs": 100,
                },
                {
                    "country": "US",
                    "city": "boston",
                    "region": "Massachusetts",
                    "metro": "506",
                    "avg_ploss": 0.03,
                    "error_pct": 8.0,
                    "total_reqs": 60,
                },
            ]
        if "GROUP BY asn, c_speed" in sql:
            return [
                {"asn": 7922, "c_speed": "fast", "cnt": 150},
                {"asn": 7922, "c_speed": "medium", "cnt": 30},
                {"asn": 15169, "c_speed": "slow", "cnt": 60},
            ]
        if "quantile(0.95)" in sql:
            return [
                {"asn": 7922, "p95": 40000.0, "p99": 60000.0},
                {"asn": 15169, "p95": 180000.0, "p99": 260000.0},
            ]
        # network_quality bar lists: dispatch by the grouped dimension alias.
        if "AS label" in sql:
            return [
                {"value": "US", "label": "US", "rtt_ms": 21.0, "reqs": 160},
            ]
        return []


def _health_service() -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=None,
    )


def test_network_health_builds_full_clickhouse_payload():
    resp = network_health(_health_service(), _Req(), _Req.start_time, _Req.end_time)

    # Full bucket axis generated start→end (13 boundaries at 300s over 1h).
    assert resp.buckets
    assert len(resp.buckets) == 13

    # Per-ASN NESTED heatmap (not the flat stub shape).
    assert resp.heatmap
    first = resp.heatmap[0]
    assert first.buckets
    assert first.asn == 7922  # ordered by total_reqs desc

    # Leaderboard carries real per-ASN health + trend + percentiles + speed mix.
    assert resp.leaderboard
    lead = {row.asn: row for row in resp.leaderboard}
    assert lead[7922].health_score_now is not None
    assert lead[15169].health_score_now < lead[7922].health_score_now
    assert lead[7922].p95_rtt_us == 40000.0
    assert lead[7922].c_speed_mix

    # World map + interned cities.
    assert resp.map_buckets
    assert resp.cities
    assert any(c.name for c in resp.cities)

    # Metro leaderboard.
    assert resp.metro_leaderboard

    # Summary health score is REAL (computed), never the hardcoded 100.0.
    assert resp.summary is not None
    assert resp.summary.global_health_score != 100.0
    assert resp.summary.total_reqs == 240
    assert resp.summary.worst_asn is not None


def test_network_quality_populates_by_region():
    resp = network_quality(_health_service(), _Req(), _Req.start_time, _Req.end_time)
    assert resp.by_region
    assert resp.by_country
    assert resp.by_region[0].rtt_ms > 0
