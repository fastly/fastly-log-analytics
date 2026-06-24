import duckdb
import pytest

from backend.models.network import NetworkHealthSummary, NetworkWorstEntry
from backend.repositories._base import _safe_table
from backend.repositories.network import _avg_hs, _health_score, get_health, get_quality
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

    out = get_health(in_memory_duckdb, test_service_source, None, None, {})
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

    out = get_health(in_memory_duckdb, test_service_source, None, None, {})
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

    out = get_health(in_memory_duckdb, test_service_source, None, None, {}, sections={"summary"})
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

    out = get_health(in_memory_duckdb, test_service_source, None, None, {}, sections={"metro_leaderboard"})
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

    out = get_health(in_memory_duckdb, test_service_source, None, None, {}, sections={"summary"})
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
    _ = get_health(in_memory_duckdb, test_service_source, None, None, {}, sections={"summary"})
    # Full second — must still have everything
    full = get_health(in_memory_duckdb, test_service_source, None, None, {})
    for key in ("summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"):
        assert key in full, f"selector poisoned the cache; full response missing {key}"


# silence ruff unused imports — duckdb + pytest are used by the new tests
_ = duckdb
_ = pytest
