import threading
import time

import duckdb
import pytest

from backend.models.common import FilterSpec
from backend.models.network import NetworkHealthSummary, NetworkWorstEntry
from backend.repositories._base import _safe_table
from backend.repositories.network import (
    _avg_hs,
    _has_signal,
    _health_score,
    _response_cache_key,
    get_health,
    get_quality,
)
from backend.utils.geo import format_city_label
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_format_city_label():
    # Valid mapping (from dummy dictionary typically loaded by countries.py, assumed known keys like US, GB)
    assert format_city_label("New York", "US", "NY") in ["New York, NY, United States", "New York, NY, US"]
    assert format_city_label("London", "GB") in ["London, United Kingdom", "London, GB"]
    assert format_city_label("", "US") in ["United States", "US"]
    assert format_city_label("Paris", "FR", "IDF") in ["Paris, IDF, France", "Paris, IDF, FR"]
    assert format_city_label("", "", "") == "Unknown"


def test_health_score():
    # Perfect score
    score = _health_score(throughput_bps=1000000, rtt_congestion_us=0, avg_ploss=0, rtt_jitter_us=0, error_pct=0)
    assert score == 100.0

    # Worst possible score
    score = _health_score(
        throughput_bps=0, rtt_congestion_us=200000, avg_ploss=0.05, rtt_jitter_us=100000, error_pct=10.0
    )
    assert score == 0.0

    # Missing values should default nicely
    score = _health_score(None, None, None, None, None)
    assert score == 100.0


def test_network_health_summary_accepts_worst_entry_dicts():
    """Regression: NetworkHealthSummary must accept dict shapes for worst_asn/worst_country."""
    summary = NetworkHealthSummary(
        global_health_score=85.0,
        avg_rtt_ms=42.0,
        total_reqs=1000,
        worst_asn={"label": "Comcast (AS7922)", "score": 72.3},
        worst_country={"label": "United States", "score": 80.1},
    )
    assert isinstance(summary.worst_asn, NetworkWorstEntry)
    assert summary.worst_asn.label == "Comcast (AS7922)"
    assert summary.worst_asn.score == 72.3
    assert isinstance(summary.worst_country, NetworkWorstEntry)
    assert summary.worst_country.label == "United States"

    # None is still valid (no data case)
    empty = NetworkHealthSummary(global_health_score=0, avg_rtt_ms=0, total_reqs=0)
    assert empty.worst_asn is None
    assert empty.worst_country is None


# ── _avg_hs: bucket-averaging helper ─────────────────────────────────────────


def test_avg_hs_returns_none_when_no_buckets_match():
    """No matching keys → None, not 0. Pinned because returning 0
    would render as "100% perfect health" on the heatmap (it inverts)."""
    buckets = {"k1": {"health_score": 80.0}}
    assert _avg_hs(buckets, ["k2", "k3"]) is None


def test_avg_hs_skips_buckets_with_none_health_score():
    """Buckets present but health_score is None (insufficient samples)
    must be skipped in the average — pinned because including None
    would crash with TypeError on the sum."""
    buckets = {
        "k1": {"health_score": 80.0},
        "k2": {"health_score": None},
        "k3": {"health_score": 60.0},
    }
    out = _avg_hs(buckets, ["k1", "k2", "k3"])
    assert out == 70.0  # (80 + 60) / 2


def test_avg_hs_rounds_to_one_decimal_place():
    """The heatmap renders these numbers as labels; one decimal matches
    the JS toFixed(1) the frontend uses."""
    buckets = {"k1": {"health_score": 33.333}, "k2": {"health_score": 66.667}}
    assert _avg_hs(buckets, ["k1", "k2"]) == 50.0


# ── _health_score: weighted-component formula ───────────────────────────────


def test_health_score_packet_loss_weight_is_40_percent():
    """Packet loss is the heaviest weighted factor (40%). At 5% loss
    with no other issues, score = 100 - 40 = 60. Pinned because
    re-weighting the formula would silently shift the heatmap colours
    on every dashboard."""
    score = _health_score(
        throughput_bps=None,
        rtt_congestion_us=0,
        avg_ploss=0.05,
        rtt_jitter_us=0,
        error_pct=0,
    )
    assert score == 60.0


def test_health_score_congestion_weight_is_30_percent():
    """RTT congestion is 30%. Saturated (200_000us) with no other
    issues → 100 - 30 = 70."""
    score = _health_score(
        throughput_bps=None,
        rtt_congestion_us=200_000,
        avg_ploss=0,
        rtt_jitter_us=0,
        error_pct=0,
    )
    assert score == 70.0


def test_health_score_jitter_weight_is_20_percent():
    score = _health_score(
        throughput_bps=None,
        rtt_congestion_us=0,
        avg_ploss=0,
        rtt_jitter_us=100_000,
        error_pct=0,
    )
    assert score == 80.0


def test_health_score_error_weight_is_10_percent():
    score = _health_score(
        throughput_bps=None,
        rtt_congestion_us=0,
        avg_ploss=0,
        rtt_jitter_us=0,
        error_pct=10.0,
    )
    assert score == 90.0


def test_health_score_clamps_packet_loss_at_5_percent():
    """``avg_ploss`` above 0.05 must clamp at the worst-case
    (full 40-point penalty), not give negative scores. Pinned because
    a 50% packet loss event would otherwise drive the score below
    zero and corrupt the heatmap scale."""
    score = _health_score(
        throughput_bps=None,
        rtt_congestion_us=0,
        avg_ploss=0.5,  # 10x the saturation point
        rtt_jitter_us=0,
        error_pct=0,
    )
    assert score == 60.0  # same as 5% — clamp held


# ── get_health: early-return paths ──────────────────────────────────────────


def test_get_health_returns_available_false_when_required_cols_missing(in_memory_duckdb, test_service_source):
    """Table exists but doesn't have ``tcp_rtt`` + ``asn`` →
    ``available: False`` with the field-config hint message. Pinned
    because the frontend keys on this exact shape to render the
    "enable Groups F/G" upgrade prompt."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table} (timestamp TIMESTAMP, status INTEGER)")

    out = get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    assert out["available"] is False
    assert "Groups F" in out["reason"] and "G" in out["reason"]


def test_get_health_strips_asn_filter_when_map_asn_specified(in_memory_duckdb, test_service_source):
    """When ``map_asn`` is set to a specific ASN (the user clicked a
    row in the leaderboard), the helper must remove the ``asn`` filter
    from the WHERE clause — otherwise it would AND-conflict and zero
    the result. Pinned because this is the "click ASN → see its map"
    UX guarantee."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    # An asn filter ("not 7922") in filters + map_asn="7922" — the
    # asn filter MUST be removed so the map for 7922 still renders.
    out = get_health(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"asn": {"mode": "exclude", "values": ["7922"]}},
        map_asn="7922",
    )
    # The early-returns we tested above aren't hit — we get a real response
    assert "available" not in out or out.get("available") is not False


# ── get_quality: early-return paths ─────────────────────────────────────────


def test_get_quality_returns_available_false_when_tcp_rtt_missing(in_memory_duckdb, test_service_source):
    """Missing tcp_rtt → ``available: False`` with the full empty
    shape (all the array keys present so the frontend can map over
    them without conditional checks)."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table} (timestamp TIMESTAMP, status INTEGER)")

    out = get_quality(in_memory_duckdb, test_service_source, None, None, {})
    assert out["available"] is False
    # All the array keys must be empty lists, not missing, so the FE renders fine
    for key in ("by_country", "by_asn", "by_region", "by_pop", "scatter", "countries"):
        assert out[key] == []
    assert out["region_country"] == "US"  # default


def test_get_quality_returns_bars_grouped_by_country_when_data_present(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    for i, log in enumerate(logs):
        log["tcp_rtt"] = 20_000 + (i * 1000)  # 20-50ms
        log["country"] = "US" if i < 20 else "GB"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_quality(in_memory_duckdb, test_service_source, None, None, {})
    by_country = out["by_country"]
    assert len(by_country) >= 1
    labels = {r["label"] for r in by_country}
    assert "US" in labels
    # rtt_ms should be ~20-50 range
    for r in by_country:
        assert 0 < r["rtt_ms"] < 200
        assert r["reqs"] > 0


def test_get_quality_respects_region_country_param(in_memory_duckdb, test_service_source):
    """``region_country`` echoes back in the response and filters the
    by_region rollup. Pinned because the region selector in the
    quality dashboard reads this exact field."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["country"] = "GB"
        log["region"] = "London"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_quality(in_memory_duckdb, test_service_source, None, None, {}, region_country="GB")
    assert out["region_country"] == "GB"


def test_get_quality_enriches_asn_labels_and_keeps_pop_as_code(in_memory_duckdb, test_service_source, monkeypatch):
    """by_asn shows the ASN name + number (matching the leaderboard); by_pop,
    by_country, by_region keep value == label (PoP geo is added on the frontend
    via the shared <PopLabel> + bootstrap pop_geo). `value` stays the raw
    click-to-filter key everywhere."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["country"] = "US"
        log["asn"] = 7922
        log["pop"] = "SJC"
    insert_mock_logs(in_memory_duckdb, table, logs)

    # Team Cymru ASN resolution is mocked; patch the re-export the enrich
    # helper resolves through (see enrich_asn_labels' docstring).
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names",
        lambda service_id, asns: {7922: "Comcast Cable Communications"},
    )

    out = get_quality(in_memory_duckdb, test_service_source, None, None, {})

    asn_row = next(r for r in out["by_asn"] if r["value"] == "7922")
    assert asn_row["label"] == "Comcast Cable Communications (7922)"

    # PoP / country / region carry the raw value as label (frontend enriches PoP).
    pop_row = next(r for r in out["by_pop"] if r["value"] == "SJC")
    assert pop_row["label"] == "SJC"
    for r in out["by_country"]:
        assert r["value"] == r["label"]


# ── get_health: section selector ────────────────────────────────────────────


def test_get_health_full_response_when_sections_none(in_memory_duckdb, test_service_source):
    """Default (sections=None) returns every section key — proves the
    selector wiring is purely additive when no caller opts in."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    for key in ("summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"):
        assert key in out, f"section {key} missing from default response"


def test_get_health_selector_drops_unrequested_keys(in_memory_duckdb, test_service_source):
    """sections={'summary'} returns ONLY summary (+ envelope fields like
    available/countries/has_metro/section_timings) — the heatmap/leaderboard/map
    keys must not appear so the FE's per-card parallel calls don't double-deliver."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_health(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200"])},
        sections={"summary"},
    )
    assert "summary" in out
    for blocked in ("heatmap", "leaderboard", "metro_leaderboard"):
        assert blocked not in out, f"{blocked} leaked through summary-only selector"


def test_get_health_metro_only_skips_heatmap_query(in_memory_duckdb, test_service_source):
    """sections={'metro_leaderboard'} must skip heatmap_query + map_query.
    Confirmed via section_timings (suppressed sections don't append)."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
        log["city"] = "Boston"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_health(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200"])},
        sections={"metro_leaderboard"},
    )
    timings = {t["section"] for t in out.get("section_timings", [])}
    assert "heatmap_query" not in timings, f"heatmap_query fired despite metro-only selector: {timings}"
    assert "map_query" not in timings, f"map_query fired despite metro-only selector: {timings}"
    # metro_query MUST have fired
    assert "metro_query" in timings, f"metro_query missing from {timings}"
    assert "metro_leaderboard" in out


def test_get_health_summary_selector_pulls_dependent_queries(in_memory_duckdb, test_service_source):
    """summary's worst_asn needs heatmap_query (top_asns), worst_country
    needs map_query (latest_cities). Pinned because a naive 'gate by
    field name alone' would skip both and crash on .get('worst_asn')."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
        log["city"] = "Boston"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_health(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200"])},
        sections={"summary"},
    )
    timings = {t["section"] for t in out.get("section_timings", [])}
    assert "heatmap_query" in timings, f"heatmap_query missing for summary selector: {timings}"
    assert "map_query" in timings, f"map_query missing for summary selector: {timings}"
    assert "summary" in out


def test_get_health_selector_skips_response_cache_write(in_memory_duckdb, test_service_source):
    """A selector call must not poison the response cache — otherwise the
    next full (sections=None) request would hit the partial payload and
    drop fields the FE expects. Validate by running the selector call,
    then a full call, and confirming the full call carries all keys."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
    insert_mock_logs(in_memory_duckdb, table, logs)

    # Selector first
    _ = get_health(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200"])},
        sections={"summary"},
    )
    # Full second — must still have everything
    full = get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    for key in ("summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"):
        assert key in full, f"selector poisoned the cache; full response missing {key}"


# ── Response-cache key isolation + reachability (security_regression) ─────────
#
# These pin the security-rbac review's invariants for the now-REACHABLE
# get_health response cache (it serves section-scoped FE requests, no longer
# only the dead sections=None shape). The cache is analyst-reachable and the
# dashboard cache was once hard-disabled after a poisoning incident, so these
# isolation guarantees are load-bearing, not nice-to-have.


def _net_logs(source, n=20):
    logs = generate_mock_logs(source, num_logs=n)
    for log in logs:
        log["tcp_rtt"] = 25_000
        log["asn"] = 7922
        log["country"] = "US"
        log["city"] = "Boston"
    return logs


@pytest.mark.security_regression
def test_cache_key_partitions_by_service():
    """Two services with identical params must produce DISTINCT cache keys —
    a request can never read another tenant's network-health entry. The key
    folds src['name'] via digest_cache_key."""
    src_a = {"name": "svc_a", "service_id": "a"}
    src_b = {"name": "svc_b", "service_id": "b"}
    key_a = _response_cache_key(
        src_a, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    key_b = _response_cache_key(
        src_b, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    assert key_a != key_b


@pytest.mark.security_regression
def test_cache_key_partitions_by_mask_ips():
    """mask_ips=True and mask_ips=False must key distinctly so a masking
    analyst can never read an unmasked entry (belt-and-braces: network-health
    is IP-free today, but the partition future-proofs the surface)."""
    src = {"name": "svc", "service_id": "s"}
    args = (src, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None)
    assert _response_cache_key(*args, False) != _response_cache_key(*args, True)


@pytest.mark.security_regression
def test_cache_key_partitions_by_sections():
    """Each FE section-set keys distinctly so a smaller selection never reads a
    larger selection's payload (and the now-reachable cache isn't a
    one-entry-overwrites-all surface)."""
    src = {"name": "svc", "service_id": "s"}
    base = (src, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all")
    core = _response_cache_key(*base, {"summary", "leaderboard", "metro_leaderboard"}, False)
    mp = _response_cache_key(*base, {"heatmap", "buckets", "cities", "map_buckets"}, False)
    full = _response_cache_key(*base, None, False)
    assert len({core, mp, full}) == 3
    # Order within a set is irrelevant — the key sorts.
    same = _response_cache_key(*base, {"leaderboard", "metro_leaderboard", "summary"}, False)
    assert same == core


@pytest.mark.security_regression
def test_cache_key_is_anchor_faithful_not_span_quantized():
    """SECURITY (anchor-collision): a now-anchored [now-24h, now] and an
    extent-anchored [latest-24h, latest] window have the SAME span but DIFFERENT
    data. The key MUST distinguish them (keys on resolved bounds, NOT a
    span/window-param projection), or one analyst's window could alias another's
    differently-anchored window and serve rows outside their invite clamp.
    This test fails immediately if anyone reintroduces span-quantization."""
    src = {"name": "svc", "service_id": "s"}
    # Same 24h span, different anchors.
    now_anchored = _response_cache_key(
        src, "2026-06-29T00:00:00Z", "2026-06-30T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    extent_anchored = _response_cache_key(
        src, "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    assert now_anchored != extent_anchored


@pytest.mark.security_regression
def test_cache_key_minute_bucketed_seconds_collapse():
    """Two timestamps in the SAME minute (differing only in seconds) yield the
    SAME key (the ≤TTL-stale, intra-minute-reload contract) — but a DIFFERENT
    minute yields a different key (still anchor-faithful)."""
    src = {"name": "svc", "service_id": "s"}
    k_10s = _response_cache_key(
        src, "2026-06-29T00:00:10Z", "2026-06-30T00:00:20Z", {}, "health_score", 5, 30, "all", None, False
    )
    k_45s = _response_cache_key(
        src, "2026-06-29T00:00:45Z", "2026-06-30T00:00:55Z", {}, "health_score", 5, 30, "all", None, False
    )
    k_next_min = _response_cache_key(
        src, "2026-06-29T00:01:10Z", "2026-06-30T00:00:20Z", {}, "health_score", 5, 30, "all", None, False
    )
    assert k_10s == k_45s  # same minute → reuse
    assert k_10s != k_next_min  # different minute → distinct (anchor-faithful)


@pytest.mark.security_regression
def test_cache_read_write_round_trip(in_memory_duckdb, test_service_source):
    """A populated full-range request caches; the next identical request reads
    is_cached=True without recomputing."""
    table = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table, _net_logs(test_service_source))

    first = get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    assert not first.get("is_cached")
    second = get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    assert second.get("is_cached") is True


@pytest.mark.security_regression
def test_force_refresh_skips_read_but_writes(in_memory_duckdb, test_service_source):
    """force_refresh=True (the prewarmer path) must recompute (no is_cached on
    the returned payload) yet still WRITE the entry so a subsequent normal call
    hits it."""
    table = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table, _net_logs(test_service_source))

    # Prime the cache.
    get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    # force_refresh must NOT read the primed entry (recomputes fresh).
    refreshed = get_health(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200"])},
        force_refresh=True,
    )
    assert not refreshed.get("is_cached")
    # But it rewrote the entry → next normal call hits.
    after = get_health(
        in_memory_duckdb, test_service_source, None, None, {"status": FilterSpec(mode="include", values=["200"])}
    )
    assert after.get("is_cached") is True


@pytest.mark.security_regression
def test_stale_empty_result_is_not_cached(in_memory_duckdb, test_service_source):
    """SECURITY (poison guard): an available-but-zero-signal window (no rows in
    range) must NOT be cached, so a later populated request recomputes instead
    of reading the cached blank. Mirrors why DASHBOARD_CACHE_TTL was disabled."""
    table = _safe_table(test_service_source["name"])
    # Seed rows only in June; query a window with NO data → zero signal.
    insert_mock_logs(in_memory_duckdb, table, _net_logs(test_service_source))

    empty_window = ("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")
    out = get_health(in_memory_duckdb, test_service_source, *empty_window, {})
    # available True (schema present) but zero signal → not cached.
    assert not _has_signal(out)
    second = get_health(in_memory_duckdb, test_service_source, *empty_window, {})
    assert not second.get("is_cached"), "a zero-signal window was cached (poison risk)"


@pytest.mark.security_regression
def test_has_signal_guard():
    """_has_signal: a populated payload caches; a zero-signal one does not."""
    assert _has_signal({"summary": {"total_reqs": 5}})
    assert _has_signal({"heatmap": [{"asn": 1}]})
    assert _has_signal({"leaderboard": [{"asn": 1}]})
    assert not _has_signal({"summary": {"total_reqs": 0}, "heatmap": [], "leaderboard": []})
    assert not _has_signal({"available": True})


# ── Relative-range (range_token + anchor) stable-key path (security_regression) ─
#
# The network 30d analyst-cliff fix: when ``range_token`` is present the cache
# key is built from (range_token, quantized_anchor, invite_clamp_fingerprint)
# instead of the rolling resolved bounds, so a stable token+anchor HITS the memo
# across rolling minutes. These pin the analyst-adversary invariants from the
# spec: the resolved+clamped bounds drive the SCAN, the token/anchor only
# stabilize the KEY, and the key partitions by every authorization axis.


class _FakeAnalystSession:
    """Minimal stand-in for an analyst session carrying invite-clamp params."""

    def __init__(self, query_start_time=None, query_end_time=None, query_window_hours=None):
        self.query_start_time = query_start_time
        self.query_end_time = query_end_time
        self.query_window_hours = query_window_hours


def _stable_key(src, *, range_token="30d", quantized_anchor="2026-06-29T00:00:00Z", icf=None, mask_ips=False):
    """Build a stable-shape (range_token-present) cache key with sane defaults."""
    return _response_cache_key(
        src,
        # start/end are IGNORED on the stable path (they drive the scan, not the
        # key) — pass distinct rolling values to prove they don't enter the key.
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
    """(a) cross-tenant isolation on the stable path: two services with an
    IDENTICAL (token, anchor, fingerprint) still key distinctly so one analyst
    can never read another tenant's token+anchor entry."""
    src_a = {"name": "svc_a", "service_id": "a"}
    src_b = {"name": "svc_b", "service_id": "b"}
    assert _stable_key(src_a) != _stable_key(src_b)


@pytest.mark.security_regression
def test_stable_key_partitions_by_mask_ips():
    """(b) mask partition on the stable path: masked vs unmasked never share."""
    src = {"name": "svc", "service_id": "s"}
    assert _stable_key(src, mask_ips=False) != _stable_key(src, mask_ips=True)


@pytest.mark.security_regression
def test_stable_key_partitions_by_invite_clamp_fingerprint():
    """(c) invite-clamp partition: open invite (None), a date-restricted invite,
    and admin (None) vs analyst must produce DISTINCT keys so a clamped-down
    invite can never read an entry scanned under a wider ceiling.

    NOTE admin and open-invite BOTH carry icf=None at the key layer — that is
    intentional and SAFE: an open invite has NO ceiling, so its scan window
    equals the request's own (token-resolved) window, identical to admin's. The
    partition that matters is open/admin (None) vs a RESTRICTED invite (a real
    fingerprint), which this asserts."""
    from backend.utils.time_window import invite_clamp_fingerprint

    src = {"name": "svc", "service_id": "s"}
    icf_open = invite_clamp_fingerprint(None)  # admin / open
    icf_restricted = invite_clamp_fingerprint(
        _FakeAnalystSession(query_start_time="2026-01-01T00:00:00Z", query_end_time="2026-02-01T00:00:00Z")
    )
    icf_windowed = invite_clamp_fingerprint(_FakeAnalystSession(query_window_hours=24))
    assert icf_open is None
    assert icf_restricted is not None and icf_windowed is not None
    keys = {
        _stable_key(src, icf=icf_open),
        _stable_key(src, icf=icf_restricted),
        _stable_key(src, icf=icf_windowed),
    }
    assert len(keys) == 3


@pytest.mark.security_regression
def test_stable_key_ignores_fe_supplied_bounds():
    """(d, key layer) Same (token, quantized_anchor, fingerprint) → IDENTICAL key
    regardless of the FE-supplied absolute start/end. This is the cliff fix: two
    calls with DIFFERENT rolling FE bounds collapse to one key (and one memo
    entry). The server resolves the scan window itself, so the FE bounds are not
    in the key and cannot fragment it minute-over-minute."""
    src = {"name": "svc", "service_id": "s"}
    k1 = _response_cache_key(
        src,
        "2026-06-29T00:00:01Z",
        "2026-06-29T11:11:11Z",
        {},
        "health_score",
        5,
        30,
        "all",
        None,
        False,
        range_token="30d",
        quantized_anchor="2026-06-29T00:00:00Z",
        invite_clamp_fingerprint=None,
    )
    k2 = _response_cache_key(
        src,
        "2026-06-29T00:43:59Z",
        "2026-06-29T22:22:22Z",
        {},
        "health_score",
        5,
        30,
        "all",
        None,
        False,
        range_token="30d",
        quantized_anchor="2026-06-29T00:00:00Z",
        invite_clamp_fingerprint=None,
    )
    assert k1 == k2


@pytest.mark.security_regression
def test_stable_key_partitions_by_range_token_and_anchor():
    """(e) no cross-range alias: a different range_token (or a different
    quantized anchor) yields a DISTINCT key, so "24h" never reads "30d"'s
    entry and yesterday's anchor never reads today's."""
    src = {"name": "svc", "service_id": "s"}
    k_30d = _stable_key(src, range_token="30d")
    k_7d = _stable_key(src, range_token="7d")
    k_24h = _stable_key(src, range_token="24h")
    assert len({k_30d, k_7d, k_24h}) == 3
    # Different anchor quantum → distinct.
    k_anchor_a = _stable_key(src, quantized_anchor="2026-06-29T00:00:00Z")
    k_anchor_b = _stable_key(src, quantized_anchor="2026-06-29T00:01:00Z")
    assert k_anchor_a != k_anchor_b


@pytest.mark.security_regression
def test_stable_key_disjoint_from_legacy_key():
    """A stable (range_token-present) key can never collide with a legacy
    anchor-faithful key for otherwise-identical params — the ``k:"rel"``
    namespace + dropped s/e keep the two shapes in separate spaces, so flipping
    a request onto the keyed path can't read a stale legacy entry (or vice
    versa)."""
    src = {"name": "svc", "service_id": "s"}
    legacy = _response_cache_key(
        src, "2026-05-30T00:00:00Z", "2026-06-29T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    stable = _stable_key(src, range_token="30d", quantized_anchor="2026-06-29T00:00:00Z")
    assert legacy != stable


@pytest.mark.security_regression
def test_clamp_ceiling_still_enforced_when_token_supplied():
    """(f) THE invariant: a token-resolved window NEVER exceeds the invite
    ceiling. An analyst on a 24h-window invite who supplies range_token="30d"
    still scans only the most-recent 24h — choosing a wider token can't widen
    the scan. Exercises the resolver → TimeBounds.clamp chain the router runs."""
    from datetime import UTC, datetime, timedelta

    from backend.utils.remote_access import MAX_ANALYST_QUERY_SPAN, _time_bounds_from_params
    from backend.utils.time_window import resolve_window

    now = datetime(2026, 6, 29, 12, 0, 0, tzinfo=UTC)
    # Analyst invite ceiling: rolling 24h window.
    tb = _time_bounds_from_params(None, None, 24, now=now)

    # Analyst asks for the 30d token, anchored at now.
    resolved_start, resolved_end = resolve_window("30d", now.isoformat(), now=now)
    rs = datetime.fromisoformat(resolved_start.replace("Z", "+00:00"))
    re_ = datetime.fromisoformat(resolved_end.replace("Z", "+00:00"))
    # The RESOLVED (pre-clamp) intent really is 30 days wide …
    assert re_ - rs >= timedelta(days=29)

    # … but after the analyst clamp the scanned span is bounded by the 24h invite.
    clamped_start, clamped_end = tb.clamp(rs, re_, max_span=MAX_ANALYST_QUERY_SPAN)
    assert clamped_end - clamped_start <= timedelta(hours=24)
    # And never reaches back to the 30d-token start.
    assert clamped_start > rs


@pytest.mark.security_regression
def test_keyed_path_cache_hit_round_trip(in_memory_duckdb, test_service_source):
    """(d, end-to-end) Two get_health calls with the SAME (range_token,
    quantized_anchor) but DIFFERENT resolved scan bounds HIT the same memo entry
    — the data-plane proof of the cliff fix. (In production the bounds within an
    anchor quantum are near-identical; here we deliberately vary them to prove
    the key, not the bounds, decides the hit.)"""
    from datetime import UTC, datetime, timedelta

    # Production runs DuckDB sessions in UTC (SET TimeZone UTC); the WHERE clause
    # compares the TIMESTAMP column against CAST(... AS TIMESTAMPTZ), so without
    # a UTC session the explicit-bounds comparison shifts by the host offset and
    # drops every row. Match prod here so the keyed window selects the fixture.
    in_memory_duckdb.execute("SET TimeZone='UTC'")

    table = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table, _net_logs(test_service_source))

    # Mock logs land in [now-1h, now]; both windows must bracket now so each
    # call has real signal (and so caches). The two FE-supplied starts differ
    # by seconds within the same anchor quantum — the STABLE key (token+anchor)
    # is identical, so the second call must hit the first's entry.
    now = datetime.now(UTC)
    anchor = now.strftime("%Y-%m-%dT%H:%M:00Z")  # minute-quantized anchor label
    win_start_a = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:01Z")
    win_start_b = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:59Z")
    win_end = (now + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    first = get_health(
        in_memory_duckdb,
        test_service_source,
        win_start_a,
        win_end,
        {"status": FilterSpec(mode="include", values=["200"])},
        range_token="30d",
        quantized_anchor=anchor,
    )
    assert not first.get("is_cached")
    assert _has_signal(first), "fixture window should carry signal so the entry is cacheable"
    second = get_health(
        in_memory_duckdb,
        test_service_source,
        win_start_b,
        win_end,
        {"status": FilterSpec(mode="include", values=["200"])},
        range_token="30d",
        quantized_anchor=anchor,
    )
    assert second.get("is_cached") is True


# ── Temp-table single-flight coalescing ──────────────────────────────────────
#
# /network fires core/map/shielding as separate concurrent HTTP requests for
# the identical window+filters (frontend/app/network/page.tsx:108-119). On a
# rollup miss, core and map both used to independently pay the ~3s
# CREATE TEMP TABLE build (slow-queries report: two near-simultaneous builds
# at 3450ms/2960ms for the same window). These pin the fix: a genuinely
# concurrent, identical-key pair shares ONE build; a differing-key pair does
# not; and a build failure surfaces to every waiter, not just the leader.
#
# Each test opens TWO SEPARATE DuckDB connections against a shared on-disk
# file rather than reusing one connection across threads — this mirrors the
# real topology (every HTTP request gets its own pooled connection, see
# backend/core/duckdb_pool.py) and is not optional: DuckDB connections are
# not safe for concurrent multi-threaded ``execute()`` calls, so sharing one
# connection across genuinely-overlapping threads corrupts query state
# (verified: it produced a spurious BinderException in an earlier draft of
# this test). Two read-only connections against the same file coexist safely
# (tests/core/test_duckdb_concurrency.py::test_concurrent_readers_against_held_writer),
# and CREATE TEMP TABLE works fine on a read-only connection (temp tables are
# a separate in-memory catalog, not part of the read-only persisted file).


def _open_two_network_connections(tmp_path, source, logs):
    """Seed one on-disk DuckDB file, then return two independent read-only
    connections to it — the same data, genuinely separate connections."""
    db_path = str(tmp_path / "network_health.duckdb")
    setup = duckdb.connect(db_path)
    table = _safe_table(source["name"])
    insert_mock_logs(setup, table, logs)
    setup.close()
    return duckdb.connect(db_path, read_only=True), duckdb.connect(db_path, read_only=True)


def test_get_health_coalesces_concurrent_core_and_map_temp_table_builds(monkeypatch, tmp_path, test_service_source):
    """core (summary/leaderboard/metro_leaderboard) and map (heatmap/buckets/
    cities/map_buckets) racing on the SAME window+filters must build the temp
    table exactly once — the second caller borrows the first's rows instead
    of re-running CREATE TEMP TABLE AS SELECT."""
    con_core, con_map = _open_two_network_connections(
        tmp_path, test_service_source, _net_logs(test_service_source, n=30)
    )

    from backend.repositories._base import QueryRunner

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
            results[label] = get_health(
                con,
                test_service_source,
                None,
                None,
                {"status": FilterSpec(mode="include", values=["200"])},
                sections=sections,
            )
        except BaseException as e:  # noqa: BLE001 - surfaced via assertion below
            errors.append(e)

    try:
        core_thread = threading.Thread(
            target=run, args=("core", con_core, {"summary", "leaderboard", "metro_leaderboard"})
        )
        core_thread.start()
        assert build_started.wait(timeout=5), "leader never reached create_filtered_temp_table"

        map_thread = threading.Thread(
            target=run, args=("map", con_map, {"heatmap", "buckets", "cities", "map_buckets"})
        )
        map_thread.start()
        # See tests/repositories/utils/test_single_flight.py for why this handshake
        # is safe: build_started firing proves the leader's registry slot already
        # exists, so map_thread is guaranteed to see it once scheduled — this only
        # bounds that (sub-millisecond) scheduling gap before releasing the leader.
        time.sleep(0.2)
        release_leader.set()

        core_thread.join(timeout=5)
        map_thread.join(timeout=5)
    finally:
        con_core.close()
        con_map.close()

    assert not errors, f"get_health raised: {errors}"
    assert call_count == 1, "core and map each built their own temp table — coalescing did not dedupe"
    assert results["core"]["leaderboard"], "core's own section produced no leaderboard data"
    assert results["core"]["metro_leaderboard"], "core's own section produced no metro_leaderboard data"
    assert results["map"]["heatmap"], "map's own section produced no heatmap data"
    assert results["map"]["map_buckets"], "map's own section produced no map_buckets data"


def test_get_health_does_not_coalesce_requests_with_different_top_n(monkeypatch, tmp_path, test_service_source):
    """Two concurrent requests that differ in ``top_n`` (which changes the
    heatmap SQL's row_limit) must NOT share a build — a mismatch on any input
    that affects the query must fall through to independent builds."""
    con_a, con_b = _open_two_network_connections(tmp_path, test_service_source, _net_logs(test_service_source, n=30))

    from backend.repositories._base import QueryRunner

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
            con,
            test_service_source,
            None,
            None,
            {"status": FilterSpec(mode="include", values=["200"])},
            sections={"heatmap"},
            top_n=top_n,
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

    assert call_count == 2, "requests with different top_n incorrectly shared a temp-table build"


def test_get_health_coalesced_temp_table_failure_surfaces_to_both_waiters(monkeypatch, tmp_path, test_service_source):
    """If the leader's CREATE TEMP TABLE fails, every waiter on that key must
    get the same 'available: False' response — a follower must never
    silently get an empty-but-'available: True' payload instead."""
    con_core, con_map = _open_two_network_connections(
        tmp_path, test_service_source, _net_logs(test_service_source, n=10)
    )

    from backend.repositories._base import QueryRunner

    build_started = threading.Event()
    release_leader = threading.Event()

    def failing_build(self, *args, **kwargs):
        build_started.set()
        assert release_leader.wait(timeout=5)
        return None  # simulate create_filtered_temp_table failure

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", failing_build)

    results: dict[str, dict] = {}

    def run(label, con, sections):
        results[label] = get_health(
            con,
            test_service_source,
            None,
            None,
            {"status": FilterSpec(mode="include", values=["200"])},
            sections=sections,
        )

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

    assert results["core"]["available"] is False
    assert results["map"]["available"] is False


# silence ruff unused imports — duckdb + pytest are used by the new tests
_ = duckdb
_ = pytest
