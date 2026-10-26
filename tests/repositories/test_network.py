from __future__ import annotations

import contextlib
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from backend.models.network import NetworkHealthSummary, NetworkWorstEntry
from backend.utils.countries import format_city_label
from backend.utils.time_window import invite_clamp_fingerprint, resolve_window

from backend.core.rollups._common import NETWORK_GEO_BUNDLE_FILENAME, NETWORK_HEATMAP_BUNDLE_FILENAME
from backend.models.common import FilterSpec
from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories.network import (
    _avg_hs,
    _has_signal,
    _health_score,
    _response_cache_key,
    _rtt_congestion_expr,
    get_health,
    get_quality,
)
from backend.utils.remote_access import MAX_ANALYST_QUERY_SPAN, _time_bounds_from_params
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests._helpers.sources import make_source

_F200 = {"status": FilterSpec(mode="include", values=["200"])}
_HEALTH_KEYS = ("summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets")


def _net_logs(source, n=20, **overrides):
    logs = generate_mock_logs(source, num_logs=n)
    for log in logs:
        log.update({"tcp_rtt": 25_000, "asn": 7922, "country": "US", "city": "Boston", **overrides})
    return logs


def _seed_net(con, source, n=20, **overrides):
    insert_mock_logs(con, _safe_table(source["name"]), _net_logs(source, n=n, **overrides))


def test_format_city_label():
    assert format_city_label("New York", "US", "NY") in ["New York, NY, United States", "New York, NY, US"]
    assert format_city_label("London", "GB") in ["London, United Kingdom", "London, GB"]
    assert format_city_label("", "US") in ["United States", "US"]
    assert format_city_label("Paris", "FR", "IDF") in ["Paris, IDF, France", "Paris, IDF, FR"]
    assert format_city_label("", "", "") == "Unknown"


def test_health_score():
    assert (
        _health_score(throughput_bps=1000000, rtt_congestion_us=0, avg_ploss=0, rtt_jitter_us=0, error_pct=0) == 100.0
    )
    assert _health_score(0, 200000, 0.05, 100000, 10.0) == 0.0
    assert _health_score(None, None, None, None, None) == 100.0


def test_network_health_summary_accepts_worst_entry_dicts():
    summary = NetworkHealthSummary(
        global_health_score=85.0,
        avg_rtt_ms=42.0,
        total_reqs=1000,
        worst_asn={"label": "Comcast (AS7922)", "score": 72.3},
        worst_country={"label": "United States", "score": 80.1},
    )
    assert isinstance(summary.worst_asn, NetworkWorstEntry)
    assert summary.worst_asn.label == "Comcast (AS7922)" and summary.worst_asn.score == 72.3
    assert isinstance(summary.worst_country, NetworkWorstEntry) and summary.worst_country.label == "United States"

    empty = NetworkHealthSummary(global_health_score=0, avg_rtt_ms=0, total_reqs=0)
    assert empty.worst_asn is None and empty.worst_country is None


def test_avg_hs_returns_none_when_no_buckets_match():
    assert _avg_hs({"k1": {"health_score": 80.0}}, ["k2", "k3"]) is None


def test_avg_hs_skips_buckets_with_none_health_score():
    buckets = {"k1": {"health_score": 80.0}, "k2": {"health_score": None}, "k3": {"health_score": 60.0}}
    assert _avg_hs(buckets, ["k1", "k2", "k3"]) == 70.0


def test_avg_hs_rounds_to_one_decimal_place():
    assert _avg_hs({"k1": {"health_score": 33.333}, "k2": {"health_score": 66.667}}, ["k1", "k2"]) == 50.0


def test_health_score_packet_loss_weight_is_40_percent():
    assert _health_score(None, 0, 0.05, 0, 0) == 60.0


def test_health_score_congestion_weight_is_30_percent():
    assert _health_score(None, 200_000, 0, 0, 0) == 70.0


def test_health_score_jitter_weight_is_20_percent():
    assert _health_score(None, 0, 0, 100_000, 0) == 80.0


def test_health_score_error_weight_is_10_percent():
    assert _health_score(None, 0, 0, 0, 10.0) == 90.0


def test_health_score_clamps_packet_loss_at_5_percent():
    assert _health_score(None, 0, 0.5, 0, 0) == 60.0


def test_get_health_returns_available_false_when_required_cols_missing(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table} (timestamp TIMESTAMP, status INTEGER)")
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert out["available"] is False and "Groups F" in out["reason"] and "G" in out["reason"]


def test_get_health_strips_asn_filter_when_map_asn_specified(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source, n=10)
    out = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"asn": {"mode": "exclude", "values": ["7922"]}},
        map_asn="7922",
    )
    assert out.get("available") is not False


def test_get_health_handles_unsigned_rtt_subtraction(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMP, asn UINTEGER, tcp_rtt UINTEGER, rtt_min UINTEGER, "
        f"rtt_var UINTEGER, ploss FLOAT, country VARCHAR, status INTEGER, cache VARCHAR, elapsed UBIGINT, resp_bytes UBIGINT)"
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table} SELECT TIMESTAMP '2026-09-10 20:00:00' + INTERVAL (i * 10) SECOND, "
        f"7922, 76570, 96261, 100, 0.01, 'US', 200, 'HIT', 100000, 50000 FROM range(10) AS r(i)"
    )
    assert in_memory_duckdb.execute(f"SELECT {_rtt_congestion_expr()} FROM {table}").fetchone() == (-19691,)


def test_get_quality_returns_available_false_when_tcp_rtt_missing(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table} (timestamp TIMESTAMP, status INTEGER)")
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    assert out["available"] is False and out["region_country"] == "US"
    for key in ("by_country", "by_asn", "by_region", "by_pop", "scatter", "countries"):
        assert out[key] == []


def test_get_quality_returns_bars_grouped_by_country_when_data_present(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    for i, log in enumerate(logs):
        log["tcp_rtt"] = 20_000 + (i * 1000)
        log["country"] = "US" if i < 20 else "GB"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    by_country = out["by_country"]
    assert len(by_country) >= 1 and "US" in {r["label"] for r in by_country}
    for r in by_country:
        assert 0 < r["rtt_ms"] < 200 and r["reqs"] > 0
    attempts = [row for row in out["section_timings"] if row["section"] == "network_quality_rollup"]
    assert len(attempts) == 1 and attempts[0]["outcome"] == "fallback" and attempts[0]["activity"] == "overhead"
    assert any(
        row["source"] == "live" and row["activity"] == "compute" and row["current_execution"]
        for row in out["section_timings"]
        if row.get("source") == "live"
    )


def test_get_quality_retains_rollup_dispatch_timings(in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setattr(
        QueryRunner,
        "try_network_quality_from_rollup",
        lambda self, *args, **kwargs: {"available": True, "by_country": []},
    )
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    (row,) = out["section_timings"]
    assert (row["source"], row["activity"], row["outcome"]) == ("rollup", "compute", "served")
    assert row["current_execution"] is True


def test_get_quality_respects_region_country_param(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source, n=10, country="GB", region="London")
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {}, region_country="GB")
    assert out["region_country"] == "GB"


def test_get_quality_enriches_asn_labels_and_keeps_pop_as_code(in_memory_duckdb, test_service_source, monkeypatch):
    _seed_net(in_memory_duckdb, test_service_source, n=20, pop="SJC")
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names",
        lambda service_id, asns: {7922: "Comcast Cable Communications"},
    )
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    asn_row = next(r for r in out["by_asn"] if r["value"] == "7922")
    assert asn_row["label"] == "Comcast Cable Communications (7922)"
    pop_row = next(r for r in out["by_pop"] if r["value"] == "SJC")
    assert pop_row["label"] == "SJC"
    for r in out["by_country"]:
        assert r["value"] == r["label"]


def test_get_health_full_response_when_sections_none(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    for key in _HEALTH_KEYS:
        assert key in out, f"section {key} missing from default response"


def test_get_health_selector_drops_unrequested_keys(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"summary"})
    assert "summary" in out
    for blocked in ("heatmap", "leaderboard", "metro_leaderboard"):
        assert blocked not in out, f"{blocked} leaked through summary-only selector"


def test_get_health_metro_only_skips_heatmap_query(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"metro_leaderboard"})
    timings = {t["section"] for t in out.get("section_timings", [])}
    assert "heatmap_query" not in timings and "map_query" not in timings and "metro_query" in timings
    assert "metro_leaderboard" in out


def test_get_health_summary_selector_pulls_dependent_queries(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"summary"})
    timings = {t["section"] for t in out.get("section_timings", [])}
    assert "heatmap_query" in timings and "map_query" in timings and "summary" in out


def test_get_health_selector_skips_response_cache_write(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"summary"})
    full = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    for key in _HEALTH_KEYS:
        assert key in full, f"selector poisoned the cache; full response missing {key}"


@pytest.mark.security_regression
def test_cache_key_partitions_by_service():
    src_a = {"name": "svc_a", "service_id": "a"}
    src_b = {"name": "svc_b", "service_id": "b"}
    tail = ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None, False)
    assert _response_cache_key(src_a, *tail) != _response_cache_key(src_b, *tail)


@pytest.mark.security_regression
def test_cache_key_partitions_by_mask_ips():
    src = {"name": "svc", "service_id": "s"}
    args = (src, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None)
    assert _response_cache_key(*args, False) != _response_cache_key(*args, True)


@pytest.mark.security_regression
def test_cache_key_partitions_by_sections():
    src = {"name": "svc", "service_id": "s"}
    base = (src, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all")
    core = _response_cache_key(*base, {"summary", "leaderboard", "metro_leaderboard"}, False)
    mp = _response_cache_key(*base, {"heatmap", "buckets", "cities", "map_buckets"}, False)
    full = _response_cache_key(*base, None, False)
    assert len({core, mp, full}) == 3
    assert _response_cache_key(*base, {"leaderboard", "metro_leaderboard", "summary"}, False) == core


@pytest.mark.security_regression
def test_cache_key_is_anchor_faithful_not_span_quantized():
    src = {"name": "svc", "service_id": "s"}
    tail = ({}, "health_score", 5, 30, "all", None, False)
    now_anchored = _response_cache_key(src, "2026-06-29T00:00:00Z", "2026-06-30T00:00:00Z", *tail)
    extent_anchored = _response_cache_key(src, "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", *tail)
    assert now_anchored != extent_anchored


@pytest.mark.security_regression
def test_cache_key_minute_bucketed_seconds_collapse():
    src = {"name": "svc", "service_id": "s"}
    tail = ({}, "health_score", 5, 30, "all", None, False)
    k_10s = _response_cache_key(src, "2026-06-29T00:00:10Z", "2026-06-30T00:00:20Z", *tail)
    k_45s = _response_cache_key(src, "2026-06-29T00:00:45Z", "2026-06-30T00:00:55Z", *tail)
    k_next_min = _response_cache_key(src, "2026-06-29T00:01:10Z", "2026-06-30T00:00:20Z", *tail)
    assert k_10s == k_45s and k_10s != k_next_min


@pytest.mark.security_regression
def test_cache_read_write_round_trip(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    first = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert not first.get("is_cached")
    second = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert second.get("is_cached") is True


@pytest.mark.security_regression
def test_force_refresh_skips_read_but_writes(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    refreshed = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, force_refresh=True)
    assert not refreshed.get("is_cached")
    after = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert after.get("is_cached") is True


@pytest.mark.security_regression
def test_stale_empty_result_is_not_cached(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    empty_window = ("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")
    out = get_health(lambda: in_memory_duckdb, test_service_source, *empty_window, {})
    assert not _has_signal(out)
    second = get_health(lambda: in_memory_duckdb, test_service_source, *empty_window, {})
    assert not second.get("is_cached"), "a zero-signal window was cached (poison risk)"


@pytest.mark.security_regression
def test_has_signal_guard():
    assert _has_signal({"summary": {"total_reqs": 5}})
    assert _has_signal({"heatmap": [{"asn": 1}]})
    assert _has_signal({"leaderboard": [{"asn": 1}]})
    assert not _has_signal({"summary": {"total_reqs": 0}, "heatmap": [], "leaderboard": []})
    assert not _has_signal({"available": True})


class _FakeAnalystSession:
    def __init__(self, query_start_time=None, query_end_time=None, query_window_hours=None):
        self.query_start_time = query_start_time
        self.query_end_time = query_end_time
        self.query_window_hours = query_window_hours


def _stable_key(src, *, range_token="30d", quantized_anchor="2026-06-29T00:00:00Z", icf=None, mask_ips=False):
    return _response_cache_key(
        src,
        "2026-06-29T00:00:00Z",
        "2026-06-29T12:34:56Z",
        {},
        "health_score",
        5,
        30,
        "all",
        None,
        mask_ips,
        range_token=range_token,
        quantized_anchor=quantized_anchor,
        invite_clamp_fingerprint=icf,
    )


@pytest.mark.security_regression
def test_stable_key_partitions_by_service():
    assert _stable_key({"name": "svc_a", "service_id": "a"}) != _stable_key({"name": "svc_b", "service_id": "b"})


@pytest.mark.security_regression
def test_stable_key_partitions_by_mask_ips():
    src = {"name": "svc", "service_id": "s"}
    assert _stable_key(src, mask_ips=False) != _stable_key(src, mask_ips=True)


@pytest.mark.security_regression
def test_stable_key_partitions_by_invite_clamp_fingerprint():
    src = {"name": "svc", "service_id": "s"}
    icf_open = invite_clamp_fingerprint(None)
    icf_restricted = invite_clamp_fingerprint(
        _FakeAnalystSession(query_start_time="2026-01-01T00:00:00Z", query_end_time="2026-02-01T00:00:00Z")
    )
    icf_windowed = invite_clamp_fingerprint(_FakeAnalystSession(query_window_hours=24))
    assert icf_open is None and icf_restricted is not None and icf_windowed is not None
    assert (
        len({_stable_key(src, icf=icf_open), _stable_key(src, icf=icf_restricted), _stable_key(src, icf=icf_windowed)})
        == 3
    )


@pytest.mark.security_regression
def test_stable_key_ignores_fe_supplied_bounds():
    src = {"name": "svc", "service_id": "s"}
    kw = dict(range_token="30d", quantized_anchor="2026-06-29T00:00:00Z", invite_clamp_fingerprint=None)
    tail = ({}, "health_score", 5, 30, "all", None, False)
    k1 = _response_cache_key(src, "2026-06-29T00:00:01Z", "2026-06-29T11:11:11Z", *tail, **kw)
    k2 = _response_cache_key(src, "2026-06-29T00:43:59Z", "2026-06-29T22:22:22Z", *tail, **kw)
    assert k1 == k2


@pytest.mark.security_regression
def test_stable_key_partitions_by_range_token_and_anchor():
    src = {"name": "svc", "service_id": "s"}
    assert (
        len(
            {
                _stable_key(src, range_token="30d"),
                _stable_key(src, range_token="7d"),
                _stable_key(src, range_token="24h"),
            }
        )
        == 3
    )
    assert _stable_key(src, quantized_anchor="2026-06-29T00:00:00Z") != _stable_key(
        src, quantized_anchor="2026-06-29T00:01:00Z"
    )


@pytest.mark.security_regression
def test_stable_key_disjoint_from_legacy_key():
    src = {"name": "svc", "service_id": "s"}
    legacy = _response_cache_key(
        src, "2026-05-30T00:00:00Z", "2026-06-29T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    assert legacy != _stable_key(src, range_token="30d", quantized_anchor="2026-06-29T00:00:00Z")


@pytest.mark.security_regression
def test_clamp_ceiling_still_enforced_when_token_supplied():
    now = datetime(2026, 6, 29, 12, 0, 0, tzinfo=UTC)
    tb = _time_bounds_from_params(None, None, 24, now=now)
    resolved_start, resolved_end = resolve_window("30d", now.isoformat(), now=now)
    rs = datetime.fromisoformat(resolved_start.replace("Z", "+00:00"))
    re_ = datetime.fromisoformat(resolved_end.replace("Z", "+00:00"))
    assert re_ - rs >= timedelta(days=29)
    clamped_start, clamped_end = tb.clamp(rs, re_, max_span=MAX_ANALYST_QUERY_SPAN)
    assert clamped_end - clamped_start <= timedelta(hours=24) and clamped_start > rs


@pytest.mark.security_regression
def test_keyed_path_cache_hit_round_trip(in_memory_duckdb, test_service_source):
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    _seed_net(in_memory_duckdb, test_service_source)

    now = datetime.now(UTC)
    anchor = now.strftime("%Y-%m-%dT%H:%M:00Z")
    win_start_a = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:01Z")
    win_start_b = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:59Z")
    win_end = (now + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    first = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        win_start_a,
        win_end,
        _F200,
        range_token="30d",
        quantized_anchor=anchor,
    )
    assert not first.get("is_cached") and _has_signal(first)
    second = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        win_start_b,
        win_end,
        _F200,
        range_token="30d",
        quantized_anchor=anchor,
    )
    assert second.get("is_cached") is True


def _open_two_network_connections(tmp_path, source, logs):
    db_path = str(tmp_path / "network_health.duckdb")
    setup = duckdb.connect(db_path)
    insert_mock_logs(setup, _safe_table(source["name"]), logs)
    setup.close()
    return duckdb.connect(db_path, read_only=True), duckdb.connect(db_path, read_only=True)


def test_get_health_coalesces_concurrent_core_and_map_temp_table_builds(monkeypatch, tmp_path, test_service_source):
    con_core, con_map = _open_two_network_connections(
        tmp_path, test_service_source, _net_logs(test_service_source, n=30)
    )
    original_build = QueryRunner.create_filtered_temp_table
    call_count = 0
    count_lock = threading.Lock()
    build_started = threading.Event()
    release_leader = threading.Event()

    def slow_build(self, *args, **kwargs):
        nonlocal call_count
        with count_lock:
            call_count += 1
        build_started.set()
        assert release_leader.wait(timeout=5), "test deadlocked waiting for release"
        return original_build(self, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", slow_build)
    results: dict[str, dict] = {}
    errors: list[BaseException] = []

    def run(label, con, sections):
        try:
            results[label] = get_health(lambda: con, test_service_source, None, None, _F200, sections=sections)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    try:
        core_thread = threading.Thread(
            target=run, args=("core", con_core, {"summary", "leaderboard", "metro_leaderboard"})
        )
        core_thread.start()
        assert build_started.wait(timeout=5)
        map_thread = threading.Thread(
            target=run, args=("map", con_map, {"heatmap", "buckets", "cities", "map_buckets"})
        )
        map_thread.start()
        time.sleep(0.2)
        release_leader.set()
        core_thread.join(timeout=5)
        map_thread.join(timeout=5)
    finally:
        con_core.close()
        con_map.close()

    assert not errors and call_count == 1
    assert results["core"]["leaderboard"] and results["core"]["metro_leaderboard"]
    assert results["map"]["heatmap"] and results["map"]["map_buckets"]


def test_get_health_does_not_coalesce_requests_with_different_top_n(monkeypatch, tmp_path, test_service_source):
    con_a, con_b = _open_two_network_connections(tmp_path, test_service_source, _net_logs(test_service_source, n=30))
    original_build = QueryRunner.create_filtered_temp_table
    call_count = 0
    count_lock = threading.Lock()

    def counting_build(self, *args, **kwargs):
        nonlocal call_count
        with count_lock:
            call_count += 1
        return original_build(self, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", counting_build)
    results: dict[str, dict] = {}

    def run(label, con, top_n):
        results[label] = get_health(
            lambda: con, test_service_source, None, None, _F200, sections={"heatmap"}, top_n=top_n
        )

    try:
        t1 = threading.Thread(target=run, args=("a", con_a, 10))
        t2 = threading.Thread(target=run, args=("b", con_b, 50))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
    finally:
        con_a.close()
        con_b.close()

    assert call_count == 2


def test_get_health_coalesced_temp_table_failure_surfaces_to_both_waiters(monkeypatch, tmp_path, test_service_source):
    con_core, con_map = _open_two_network_connections(
        tmp_path, test_service_source, _net_logs(test_service_source, n=10)
    )
    build_started = threading.Event()
    release_leader = threading.Event()

    def failing_build(self, *args, **kwargs):
        build_started.set()
        assert release_leader.wait(timeout=5)
        return None

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", failing_build)
    results: dict[str, dict] = {}

    def run(label, con, sections):
        results[label] = get_health(lambda: con, test_service_source, None, None, _F200, sections=sections)

    try:
        t1 = threading.Thread(target=run, args=("core", con_core, {"summary", "leaderboard", "metro_leaderboard"}))
        t1.start()
        assert build_started.wait(timeout=5)
        t2 = threading.Thread(target=run, args=("map", con_map, {"heatmap", "buckets", "cities", "map_buckets"}))
        t2.start()
        time.sleep(0.2)
        release_leader.set()
        t1.join(timeout=5)
        t2.join(timeout=5)
    finally:
        con_core.close()
        con_map.close()

    assert results["core"]["available"] is False and results["map"]["available"] is False


def _hours_back(n_hours: int, span_hours: int) -> tuple[str, str, list[str]]:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    end = now - timedelta(hours=n_hours)
    start = end - timedelta(hours=span_hours)
    hours = [(start + timedelta(hours=i)).strftime("%Y-%m-%d-%H") for i in range(span_hours)]
    return start.isoformat(), end.isoformat(), hours


def _write_heatmap_bundle(bundled_root: Path, hour_str: str, asns: list[int] | None = None) -> None:
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    out = hour_dir / NETWORK_HEATMAP_BUNDLE_FILENAME
    hour_dt = datetime.strptime(hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    rows_sql = ", ".join(
        f"({asn}, TIMESTAMPTZ '{hour_dt.isoformat()}', 100, 5, 1000000.0, 25000.0, 20000.0, 5000.0, 95, 0.001, 90)"
        for asn in (asns or [1234, 5678])
    )
    with contextlib.closing(duckdb.connect()) as con:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {rows_sql}) AS t(asn, hour_ts, reqs, errors, resp_bytes_sum, "
            f"rtt_p50_us, rtt_min_p50_us, rtt_var_p50_us, rtt_count, ploss_sum, ploss_count)) TO '{out}' (FORMAT PARQUET)"
        )


def _write_geo_bundle(bundled_root: Path, hour_str: str) -> None:
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    out = hour_dir / NETWORK_GEO_BUNDLE_FILENAME
    hour_dt = datetime.strptime(hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    rows_sql = (
        f"('US', 'San Jose', 37.34, -121.89, '807', TIMESTAMPTZ '{hour_dt.isoformat()}', 50, 2, 0.01, 45, 1250000.0, 48),"
        f"('DE', 'Berlin', 52.52, 13.41, NULL, TIMESTAMPTZ '{hour_dt.isoformat()}', 30, 1, 0.005, 28, 800000.0, 30)"
    )
    with contextlib.closing(duckdb.connect()) as con:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {rows_sql}) AS t(country, city, lat, lon, metro, hour_ts, "
            f"reqs, errors, ploss_sum, ploss_count, rtt_sum, rtt_count)) TO '{out}' (FORMAT PARQUET)"
        )


def _write_all_fields_marker(bundled_root: Path, hour_str: str) -> None:
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    marker = hour_dir / "all_fields.parquet"
    with contextlib.closing(duckdb.connect()) as con:
        con.execute(
            f"COPY (SELECT 'marker' AS field, '{hour_str}' AS hour, 'v' AS value, 1 AS count) TO '{marker}' (FORMAT PARQUET)"
        )


class TestBuildNetworkHeatmapBundles:
    def test_writes_parquet_for_closed_hour(self, tmp_path):
        cache_dir = tmp_path / "cache" / "test_svc"
        bundled_root = cache_dir / "rollups" / "hour_bundled"
        hour_str = (datetime.now(UTC) - timedelta(hours=3)).strftime("%Y-%m-%d-%H")
        _write_heatmap_bundle(bundled_root, hour_str)

        out_path = bundled_root / f"hour={hour_str}" / NETWORK_HEATMAP_BUNDLE_FILENAME
        assert out_path.is_file()
        with contextlib.closing(duckdb.connect()) as con:
            rows = con.execute(f"SELECT * FROM '{out_path}'").fetchall()
            cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM '{out_path}'").fetchall()}
        assert rows
        expected = set(
            "asn hour_ts reqs errors resp_bytes_sum rtt_p50_us rtt_min_p50_us rtt_var_p50_us rtt_count ploss_sum ploss_count".split()
        )
        assert expected.issubset(cols)

    def test_skip_active_hour_token_is_filtered(self, tmp_path):
        from backend.core.rollups.network_health_heatmap import build_network_heatmap_bundles

        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = tmp_path / "cache" / "test_svc"
        cache_dir.mkdir(parents=True)
        src = make_source(str(cache_dir))
        try:
            n = build_network_heatmap_bundles("test", src, [active_hour])
        except Exception:
            n = 0
        assert n == 0


class TestBuildNetworkGeoBundles:
    def test_writes_parquet_with_correct_schema(self, tmp_path):
        hour_str = (datetime.now(UTC) - timedelta(hours=3)).strftime("%Y-%m-%d-%H")
        bundled_root = tmp_path / "cache" / "test_svc" / "rollups" / "hour_bundled"
        _write_geo_bundle(bundled_root, hour_str)
        out_path = bundled_root / f"hour={hour_str}" / NETWORK_GEO_BUNDLE_FILENAME
        assert out_path.is_file()
        with contextlib.closing(duckdb.connect()) as con:
            cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM '{out_path}'").fetchall()}
        expected = set("country city lat lon metro hour_ts reqs errors ploss_sum ploss_count rtt_sum rtt_count".split())
        assert expected.issubset(cols)


@pytest.fixture
def heatmap_rollup_layout(tmp_path):
    cache_dir = tmp_path / "cache" / "test_svc"
    bundled = cache_dir / "rollups" / "hour_bundled"
    bundled.mkdir(parents=True)
    start_iso, end_iso, hours = _hours_back(n_hours=2, span_hours=72)
    for h in hours:
        _write_heatmap_bundle(bundled, h)
        _write_all_fields_marker(bundled, h)
    return bundled, make_source(str(cache_dir)), start_iso, end_iso, hours


class TestTryNetworkHeatmapFromRollup:
    def test_returns_none_when_window_too_short(self, heatmap_rollup_layout):
        _, src, _, _, _ = heatmap_rollup_layout
        now = datetime.now(UTC)
        with contextlib.closing(duckdb.connect()) as con:
            res = QueryRunner(con, src).try_network_heatmap_from_rollup(
                (now - timedelta(hours=12)).isoformat(), now.isoformat(), has_filters=False
            )
        assert res is None

    def test_returns_none_when_has_filters(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=True) is None

    def test_returns_rows_for_eligible_window(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None and len(result) > 0

    def test_serves_hourly_rollup_for_finer_requested_bucket(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            rows = QueryRunner(con, src).try_network_heatmap_from_rollup(
                start_iso, end_iso, has_filters=False, bucket_seconds=300
            )
        assert rows is not None and len(rows) > 0

    def test_returns_none_for_coarser_than_hourly_bucket(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            rows = QueryRunner(con, src).try_network_heatmap_from_rollup(
                start_iso, end_iso, has_filters=False, bucket_seconds=7200
            )
        assert rows is None

    def test_row_shape_matches_live_heatmap_loop(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            rows = QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=False)
        assert rows is not None and len(rows) > 0
        r = rows[0]
        assert len(r) == 10 and int(r[0]) > 0 and int(r[9]) > 0
        bucket = r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1])
        assert len(bucket) > 0

    def test_returns_none_when_no_bundles_exist(self, tmp_path):
        cache_dir = tmp_path / "cache" / "test_svc"
        (cache_dir / "rollups" / "hour_bundled").mkdir(parents=True)
        src = make_source(str(cache_dir))
        start_iso, end_iso, _ = _hours_back(n_hours=2, span_hours=72)
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=False) is None


@pytest.fixture
def geo_rollup_layout(tmp_path):
    cache_dir = tmp_path / "cache" / "test_svc"
    bundled = cache_dir / "rollups" / "hour_bundled"
    bundled.mkdir(parents=True)
    start_iso, end_iso, hours = _hours_back(n_hours=2, span_hours=72)
    for h in hours:
        _write_geo_bundle(bundled, h)
        _write_all_fields_marker(bundled, h)
    return bundled, make_source(str(cache_dir)), start_iso, end_iso, hours


class TestTryNetworkGeoFromRollup:
    def test_returns_none_when_window_too_short(self, geo_rollup_layout):
        _, src, _, _, _ = geo_rollup_layout
        now = datetime.now(UTC)
        with contextlib.closing(duckdb.connect()) as con:
            res = QueryRunner(con, src).try_network_geo_from_rollup(
                (now - timedelta(hours=12)).isoformat(), now.isoformat(), has_filters=False
            )
        assert res is None

    def test_returns_none_when_has_filters(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=True) is None

    def test_returns_none_when_map_asn_not_all(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            res = QueryRunner(con, src).try_network_geo_from_rollup(
                start_iso, end_iso, map_asn="12345", has_filters=False
            )
        assert res is None

    def test_returns_tuple_for_eligible_window(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None
        map_rows, metro_rows = result
        assert len(map_rows) > 0 and len(metro_rows) > 0

    def test_map_rows_shape_matches_live_loop(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None
        r = result[0][0]
        assert len(r) == 10 and isinstance(r[0], str) and len(r[0]) == 2 and int(r[9]) > 0
        bucket = r[5].isoformat() if hasattr(r[5], "isoformat") else str(r[5])
        assert len(bucket) > 0

    def test_metro_rows_shape_matches_live_loop(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None
        r = result[1][0]
        assert len(r) == 8 and isinstance(r[0], str) and r[2] == "" and int(r[7]) > 0

    def test_returns_none_when_no_bundles_exist(self, tmp_path):
        cache_dir = tmp_path / "cache" / "test_svc"
        (cache_dir / "rollups" / "hour_bundled").mkdir(parents=True)
        src = make_source(str(cache_dir))
        start_iso, end_iso, _ = _hours_back(n_hours=2, span_hours=72)
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False) is None

    def test_returns_tuple_when_window_includes_active_hour(self, geo_rollup_layout):
        bundled, src, _, _, _ = geo_rollup_layout
        now = datetime.now(UTC)
        start_iso, end_iso = (now - timedelta(hours=48)).isoformat(), now.isoformat()
        cur = (now - timedelta(hours=48)).replace(minute=0, second=0, microsecond=0)
        active_start = now.replace(minute=0, second=0, microsecond=0)
        while cur < active_start:
            h_str = cur.strftime("%Y-%m-%d-%H")
            _write_geo_bundle(bundled, h_str)
            _write_all_fields_marker(bundled, h_str)
            cur += timedelta(hours=1)

        with contextlib.closing(duckdb.connect()) as con:
            con.execute(
                "CREATE TABLE logs_test (country VARCHAR, city VARCHAR, lat DOUBLE, lon DOUBLE, "
                "metro VARCHAR, tcp_rtt BIGINT, status INT, timestamp TIMESTAMPTZ)"
            )
            con.execute("INSERT INTO logs_test VALUES ('US', 'Denver', 39.7, -104.9, '751', 25000, 200, now())")
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None and len(result[0]) > 0 and len(result[1]) > 0
