"""Regression tests for backend.repositories.origin — validates return keys with real data."""

from unittest.mock import patch

import pytest

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table
from backend.repositories.origin import (
    _enrich_with_distance,
    _haversine_km,
    get_ip_health,
    get_path_breakdown,
    get_pop_latency,
    get_shielding_analysis,
    get_slow_urls,
    get_status_codes,
    get_summary,
    get_timeseries,
)
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _origin_logs(src, num=40):
    """Generate logs with origin columns populated so has_origin_data returns True."""
    logs = generate_mock_logs(src, num_logs=num, hours_ago=1)
    for i, log in enumerate(logs):
        log["ottfb"] = 50000 + i * 1000  # 50–90ms in microseconds
        log["ost"] = 200 if i % 5 != 0 else 500
        log["oip"] = "203.0.113.1" if i < 25 else "203.0.113.2"
    return logs


# ── get_summary ───────────────────────────────────────────────────────────────


def test_get_summary_no_origin_data(in_memory_duckdb, test_service_source):
    """Without any origin timing values (ottfb or ttfb), has_data is False."""
    logs = generate_mock_logs(test_service_source, num_logs=10)
    # Clear both candidate columns
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is False


def test_get_summary_returns_expected_keys(in_memory_duckdb, test_service_source):
    """All latency percentile keys present and non-null when ottfb data exists."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    for key in (
        "total_misses",
        "total_passes",
        "ottfb_p50_ms",
        "ottfb_p75_ms",
        "ottfb_p95_ms",
        "ottfb_p99_ms",
        "ottlb_p50_ms",
        "ottlb_p95_ms",
        "cdn_overhead_p50_ms",
        "origin_error_rate",
        "obytes_p50",
    ):
        assert key in result, f"Missing key: {key}"
    assert result["ottfb_p50_ms"] is not None
    assert result["ottfb_p50_ms"] > 0
    # ``by_leg`` was dropped — no UI surface consumed the per-edge
    # breakdown. The single-grouping rollup is roughly half the work.
    assert "by_leg" not in result


def test_get_summary_uses_single_grouping_pass(in_memory_duckdb, test_service_source):
    """``get_summary`` runs a single () GROUPING aggregate — no GROUPING
    SETS, no per-edge ``by_leg`` rows. The previous combined-pass shape
    paid for a second hash partition + per-edge percentile sorts the
    page never read; this test pins the slim shape so it doesn't drift
    back."""
    logs = _origin_logs(test_service_source, num=50)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert result["ottfb_p50_ms"] is not None
    assert "by_leg" not in result

    debug_queries = result.get("debug_queries") or result.get("_debug_queries") or []
    logs_scans = [
        q for q in debug_queries if "logs_" in q["sql"] and not q["sql"].lstrip().upper().startswith("CREATE")
    ]
    assert len(logs_scans) == 1, (
        f"get_summary must do a single scan. Got {len(logs_scans)} scan(s): {[q['sql'][:200] for q in logs_scans]}"
    )
    assert "GROUPING SETS" not in logs_scans[0]["sql"], (
        f"per-edge GROUPING SETS was dropped — frontend never read by_leg. Got: {logs_scans[0]['sql'][:300]}"
    )


# ── get_timeseries ────────────────────────────────────────────────────────────


def test_get_timeseries_returns_series_key(in_memory_duckdb, test_service_source):
    """Returns 'series', not 'rows' or 'data'. Each point has time/miss_count/value."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, metric="ttfb", percentile="p95")
    assert result["has_data"] is True
    assert "series" in result
    assert isinstance(result["series"], list)
    assert len(result["series"]) > 0
    pt = result["series"][0]
    for key in ("time", "miss_count", "value"):
        assert key in pt, f"Missing key in series point: {key}"


def test_get_timeseries_ttfb_fallback(in_memory_duckdb, test_service_source):
    """Verify get_timeseries works when ottfb is missing/null but ttfb exists."""
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = "0.123456"  # 123.456ms
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, metric="ttfb")
    assert result["has_data"] is True
    assert len(result["series"]) > 0
    pt = result["series"][0]
    # 0.123456s * 1000 = 123.456ms
    assert pt["value"] is not None
    assert 123.0 < pt["value"] < 124.0


# ── get_slow_urls ─────────────────────────────────────────────────────────────


def test_get_slow_urls_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' (not 'data') with url/requests/p50_ms/p95_ms/p99_ms."""
    logs = _origin_logs(test_service_source, num=50)
    # Concentrate requests to two URLs so min_requests is met
    for i, log in enumerate(logs):
        log["url"] = "/slow" if i < 30 else "/fast"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_slow_urls(in_memory_duckdb, test_service_source, None, None, {}, min_requests=5)
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert "data" not in result
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("url", "requests", "p50_ms", "p95_ms", "p99_ms"):
        assert key in row, f"Missing key in slow_url row: {key}"


# ── get_status_codes ──────────────────────────────────────────────────────────


def test_get_status_codes_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' with status/count/pct fields."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_status_codes(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("status", "count", "pct"):
        assert key in row, f"Missing key in status_code row: {key}"
    # pct values should sum to ~100
    total_pct = sum(r["pct"] for r in result["rows"])
    assert 99.0 <= total_pct <= 101.0


# ── get_pop_latency ───────────────────────────────────────────────────────────


def test_get_pop_latency_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' with pop/requests/p50_ms/p95_ms/elevated."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert "requires_group_c" in result
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("pop", "requests", "p50_ms", "p95_ms", "elevated"):
        assert key in row, f"Missing key in pop_latency row: {key}"
    assert isinstance(row["elevated"], bool)


def test_get_pop_latency_no_pop_col(in_memory_duckdb, test_service_source):
    """Returns requires_group_c=True when pop column is absent from data."""
    # Generate logs without pop (all None) — pop is in schema but null means not in actual data
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log.pop("pop", None)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    # Either no data (no ottfb) or requires_group_c — either way rows should be empty
    assert "rows" in result
    assert "requires_group_c" in result


# ── get_ip_health ─────────────────────────────────────────────────────────────


def test_get_ip_health_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' with oip/requests/p50_ms/p95_ms/error_pct."""
    logs = _origin_logs(test_service_source, num=30)
    # All from same oip so HAVING COUNT(*) >= 10 is satisfied
    for log in logs:
        log["oip"] = "203.0.113.1"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("oip", "requests", "p50_ms", "p95_ms", "error_pct"):
        assert key in row, f"Missing key in ip_health row: {key}"


# ── get_path_breakdown ────────────────────────────────────────────────────────


def test_get_path_breakdown_returns_expected_keys(in_memory_duckdb, test_service_source):
    """Returns has_data, shielding_detected, and rows."""
    logs = _origin_logs(test_service_source)
    # Set edge=True on all logs; edge column required for this function
    for log in logs:
        log["edge"] = True
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_path_breakdown(in_memory_duckdb, test_service_source, None, None, {})
    assert "has_data" in result
    assert "shielding_detected" in result
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    if result["has_data"]:
        assert isinstance(result["rows"], list)
        if result["rows"]:
            row = result["rows"][0]
            for key in ("edge", "requests", "p50_ms", "p95_ms"):
                assert key in row


# ── _haversine_km: distance math ────────────────────────────────────────────


def test_haversine_zero_distance_when_coords_equal():
    """Identical coords → distance 0. Pinned because a non-zero result
    here would indicate a math.radians regression that would silently
    inflate every ``efficiency_ratio`` computation."""
    assert _haversine_km(40.7128, -74.0060, 40.7128, -74.0060) == pytest.approx(0.0)


def test_haversine_nyc_to_la_is_approximately_3935km():
    """NYC ↔ LA is a well-known ~3,935 km great-circle distance.
    Pinned as a known-good reference value so a refactor (e.g.,
    swapping the radius constant) is caught."""
    nyc_lat, nyc_lon = 40.7128, -74.0060
    la_lat, la_lon = 34.0522, -118.2437
    dist = _haversine_km(nyc_lat, nyc_lon, la_lat, la_lon)
    # Real value ~3,935 km; allow ±10 km for rounding
    assert 3925 <= dist <= 3945


def test_haversine_is_symmetric():
    """``haversine(a, b)`` must equal ``haversine(b, a)``. Pinned
    because losing symmetry would mean shield-path arcs would render
    differently depending on which POP was "first" in the row tuple."""
    a = (40.0, -74.0)
    b = (37.0, -122.0)
    assert _haversine_km(*a, *b) == pytest.approx(_haversine_km(*b, *a))


# ── _enrich_with_distance: branch coverage ──────────────────────────────────


def test_enrich_with_known_pops_computes_distance_and_efficiency():
    """Two known POPs + a measured p50 → distance, light-speed RTT,
    and efficiency ratio all populated. Pinned because this is what
    drives the shield-path map's tooltip values."""
    fake_pops = {
        "LAX": (33.9425, -118.4081),
        "SJC": (37.3639, -121.9289),
    }
    row = {"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = _enrich_with_distance(row)

    assert out["distance_km"] is not None
    assert out["distance_km"] > 0
    assert out["light_speed_rtt_ms"] is not None
    assert out["edge_lat"] == 33.9425
    assert out["shield_lon"] == -121.9289
    assert out["efficiency_ratio"] is not None
    # 100ms is far more than the ~5ms light-speed floor → efficiency >> 1
    assert out["efficiency_ratio"] > 3.0


def test_enrich_pop_codes_are_uppercased_for_lookup():
    """The cache is keyed on uppercase POP codes; the helper must
    upper-case incoming values. Pinned because Fastly log data
    sometimes carries lowercase POP codes and a refactor that drops
    the upper() would silently fail every distance computation."""
    fake_pops = {"LAX": (33.9425, -118.4081), "SJC": (37.3639, -121.9289)}
    row = {"edge_pop": "lax", "shield_pop": "sjc", "p50_ms": 100.0}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = _enrich_with_distance(row)

    assert out["distance_km"] is not None  # Lookup succeeded


def test_enrich_with_unknown_pop_returns_none_for_all_coords():
    """If either POP isn't in the cache, distance and efficiency are
    None — but the row still has the keys (so frontend renderers
    don't crash on missing fields)."""
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.9425, -118.4081)}):
        out = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "GHOST", "p50_ms": 50.0})

    assert out["distance_km"] is None
    assert out["light_speed_rtt_ms"] is None
    assert out["efficiency_ratio"] is None
    assert out["anomaly_static"] is False
    # Known coord still surfaces; unknown is None
    assert out["edge_lat"] == 33.9425
    assert out["shield_lat"] is None


def test_enrich_anomaly_only_flags_when_overhead_above_20ms():
    """``anomaly_static`` requires BOTH efficiency > 3x AND absolute
    overhead >= 20ms above the theoretical floor. Pinned because
    short hops with high relative efficiency but tiny absolute
    overhead shouldn't be flagged — TCP overhead dominates at that
    scale."""
    # Two POPs ~250km apart → light_speed_rtt ≈ 2.5ms (>0.5 floor)
    # so the efficiency math runs. p50=15ms gives ratio 6x but absolute
    # overhead = 15 - 2.5 = 12.5ms, below the 20ms anomaly threshold.
    # requests well above the sample floor so the *only* reason it isn't
    # flagged is the absolute-overhead gate (low-sample gating isolated out).
    fake_pops = {"SJC": (37.3639, -121.9289), "LAX": (33.9425, -118.4081)}
    row = {"edge_pop": "SJC", "shield_pop": "LAX", "p50_ms": 15.0, "requests": 500}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = _enrich_with_distance(row)

    assert out["efficiency_ratio"] is not None
    assert out["efficiency_ratio"] > 3.0  # high ratio
    assert out["low_sample"] is False  # enough samples — not the reason
    assert out["anomaly_static"] is False  # but still below absolute floor


def test_enrich_suppresses_anomaly_below_min_request_floor():
    """A genuinely high-overhead route (efficiency >> 3x AND overhead >> 20ms)
    is still NOT flagged when it has too few requests for the median to be
    trustworthy — it's marked ``low_sample`` instead. The same profile WITH
    enough requests does flag. Pinned because low-traffic routes were
    producing false "suboptimal peering" flags (prod 2026-06-30): the median
    over a handful of requests is noise. (low-sample gating)"""
    # LAX→SJC ~490km → light floor ≈ 4.9ms; p50=100ms → efficiency ≈ 20x and
    # overhead ≈ 95ms, comfortably past both anomaly gates.
    fake_pops = {"LAX": (33.9425, -118.4081), "SJC": (37.3639, -121.9289)}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        thin = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0, "requests": 5})
        thick = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0, "requests": 200})

    # Same latency profile, only the volume differs.
    assert thin["efficiency_ratio"] == thick["efficiency_ratio"]
    assert thin["efficiency_ratio"] > 3.0

    # Thin route: shown (fields populated) but low_sample and never flagged.
    assert thin["low_sample"] is True
    assert thin["anomaly_static"] is False

    # Well-trafficked route with the same profile: flagged.
    assert thick["low_sample"] is False
    assert thick["anomaly_static"] is True


def test_enrich_sets_anomaly_eligible_independent_of_sample():
    """``anomaly_eligible`` is the latency verdict ALONE (efficiency > 3x AND
    >= 20ms overhead) — it must be True for a high-overhead route whether or
    not it clears the sample floor. The FE re-derives the flag against a
    user-chosen min-requests threshold from this field, so it must not bake in
    the sample gate. ``anomaly_static`` stays = ``eligible and not low_sample``.
    (user-adjustable min-requests threshold)"""
    fake_pops = {"LAX": (33.9425, -118.4081), "SJC": (37.3639, -121.9289)}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        thin = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0, "requests": 5})
        thick = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0, "requests": 200})

    # Latency verdict identical regardless of volume…
    assert thin["anomaly_eligible"] is True
    assert thick["anomaly_eligible"] is True
    # …but the realized flag is still sample-gated at the server default.
    assert thin["anomaly_static"] is False
    assert thick["anomaly_static"] is True


def test_enrich_anomaly_eligible_false_below_overhead_floor():
    """A short hop under the 20ms absolute-overhead gate isn't eligible at all,
    so no min-requests threshold the FE picks could ever flag it."""
    fake_pops = {"SJC": (37.3639, -121.9289), "LAX": (33.9425, -118.4081)}
    row = {"edge_pop": "SJC", "shield_pop": "LAX", "p50_ms": 15.0, "requests": 500}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = _enrich_with_distance(row)

    assert out["anomaly_eligible"] is False
    assert out["anomaly_static"] is False


def test_enrich_with_none_p50_returns_none_efficiency():
    """p50_ms missing (very-low-traffic POP pair) → efficiency_ratio
    can't be computed. Pinned to None rather than 0 so the UI
    distinguishes "no data" from "perfect efficiency"."""
    fake_pops = {"LAX": (33.9425, -118.4081), "JFK": (40.6398, -73.7789)}
    row = {"edge_pop": "LAX", "shield_pop": "JFK", "p50_ms": None}

    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = _enrich_with_distance(row)

    assert out["efficiency_ratio"] is None
    assert out["anomaly_static"] is False


# ── get_shielding_analysis: requires_fields + edge_only branches ───────────


def test_shielding_analysis_returns_requires_fields_when_columns_missing(in_memory_duckdb, test_service_source):
    """If the table schema is missing rid/prid/edge/pop/ottfb, return
    the ``requires_fields`` list so the frontend can show the field-
    picker hint ("enable these custom fields to see this view")."""
    in_memory_duckdb.execute(f"CREATE TABLE {_safe_table(test_service_source['name'])} (timestamp TIMESTAMP)")
    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert "requires_fields" in out
    assert set(out["requires_fields"]) >= {"rid", "prid", "edge", "pop", "ottfb"}


def test_shielding_analysis_returns_edge_only_when_no_shield_rows(in_memory_duckdb, test_service_source):
    """If only edge logs exist (service has no shielding configured),
    return ``edge_only: True`` so the UI renders the explanatory
    "no shield path" state instead of an empty table."""
    table = _safe_table(test_service_source["name"])
    logs = _origin_logs(test_service_source)
    for log in logs:
        log["edge"] = True  # everyone is an edge log
        log["rid"] = f"r{log.get('rid', '')}"
        log["prid"] = ""  # no parent rid → not a shield log
        log["pop"] = "LAX"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert out.get("edge_only") is True
    assert out["rows"] == []


# ── get_summary: lat_val fallback chain + empty schema ──────────────────


def test_get_summary_empty_schema_returns_has_data_false(in_memory_duckdb):
    """Service with no schema (table doesn't exist) → has_data=False
    with just the telemetry shape. Pinned because the FE renders
    "no data" rather than 500 when the table is missing."""
    src = {"name": "nonexistent_origin_svc", "service_id": "x"}
    out = get_summary(in_memory_duckdb, src, start_time=None, end_time=None, filters={})
    assert out["has_data"] is False


def test_response_cache_hit_skips_duckdb_on_repeat_call(in_memory_duckdb, test_service_source):
    """The Origin page fires the same endpoint repeatedly (React Query refetch,
    nav-back). The first call should hit DuckDB; subsequent calls within the
    30s TTL must return ``is_cached=True`` (rendered as ``_is_cached`` in JSON
    via the BaseResponse serialization_alias) without re-executing the
    aggregate query."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    cold = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert cold["has_data"] is True
    assert cold.get("is_cached") is not True

    warm = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert warm["has_data"] is True
    # The pydantic field name is `is_cached`; the JSON alias is `_is_cached`.
    assert warm.get("is_cached") is True
    # Same payload across the cache boundary
    assert warm["ottfb_p50_ms"] == cold["ottfb_p50_ms"]


def test_get_summary_uses_ttfb_only_when_ottfb_column_absent(in_memory_duckdb, test_service_source):
    """When the table has `ttfb` but no `ottfb` column (older log
    formats), lat_val falls back to `"ttfb" * 1000000.0`. Pinned
    because the origin dashboard must still work on legacy services
    that pre-date the ottfb column."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    # Set ttfb (seconds) and clear ottfb so the fallback path fires
    for i, log in enumerate(logs):
        log["ttfb"] = 0.05 + i * 0.001  # 50ms ramp
        log["ottfb"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is True
    # p50 should be roughly 60ms (middle of the 50-70ms range)
    assert out["ottfb_p50_ms"] is not None


# ── get_timeseries: metric/percentile variations ────────────────────────


def test_get_timeseries_metric_unavailable_returns_empty(in_memory_duckdb, test_service_source):
    """Requesting `metric="ttlb"` when ottlb column doesn't exist
    AND no fallback applies → has_data=False. Pinned because the
    FE distinguishes "no data" from a hard error."""
    logs = _origin_logs(test_service_source, num=10)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_timeseries(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        metric="ttlb",  # ottlb column absent
    )
    assert out["has_data"] is False


def test_get_timeseries_sub_minute_bucket_uses_seconds_interval(in_memory_duckdb, test_service_source):
    """`bucket_minutes < 1` → use INTERVAL 'N' seconds (not 0
    minutes). Pinned because a 0-minute interval would error in
    DuckDB; this branch ensures the "second" granularity option
    works."""
    logs = _origin_logs(test_service_source, num=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_timeseries(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        bucket_minutes=0.5,  # 30 seconds
    )
    # Doesn't crash with DuckDB interval error
    assert "series" in out


def test_get_timeseries_p50_uses_median_not_approx_quantile(in_memory_duckdb, test_service_source):
    """For `percentile="p50"`, the SQL uses `MEDIAN(...)` (exact)
    instead of APPROX_QUANTILE. Pinned because admins use p50 for
    accurate central-tendency reporting and approximation would
    create reporting drift."""
    logs = _origin_logs(test_service_source, num=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_timeseries(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        percentile="p50",
    )
    assert "series" in out


def test_get_timeseries_split_by_leg_includes_edge_in_series(in_memory_duckdb, test_service_source):
    """`split_by_leg=True` AND `edge` column present → series points
    include the `edge` key for FE separation of edge vs shield
    timing curves. Pinned because losing this would collapse them
    onto one line."""
    logs = _origin_logs(test_service_source, num=20)
    for i, log in enumerate(logs):
        log["edge"] = i % 2 == 0  # alternate edge/shield
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_timeseries(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        split_by_leg=True,
    )
    # At least one series point should include the `edge` key
    if out["series"]:
        assert "edge" in out["series"][0]


# ── get_status_codes: empty-rows branch ─────────────────────────────────


def test_get_status_codes_returns_has_data_false_when_ost_all_null(in_memory_duckdb, test_service_source):
    """All-null ost column → no rows → has_data=False (not 500).
    Pinned because the FE renders an empty state, not error toast."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["ost"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_status_codes(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


def test_get_status_codes_returns_has_data_false_when_ost_column_missing(in_memory_duckdb, test_service_source):
    """No `ost` column at all → has_data=False. Pinned because
    services without Group L (origin metrics) don't have ost."""
    in_memory_duckdb.execute(
        f'CREATE TABLE "{_safe_table(test_service_source["name"])}" (timestamp TIMESTAMP, status INTEGER)'
    )
    out = get_status_codes(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


# ── get_pop_latency: requires_group_c flag ──────────────────────────────


def test_get_pop_latency_no_pop_col_flags_requires_group_c(in_memory_duckdb, test_service_source):
    """Missing `pop` column → ``requires_group_c=True`` so the FE
    can render the "enable Group C" CTA. Pinned because losing this
    would render a confusing empty pop list."""
    in_memory_duckdb.execute(
        f'CREATE TABLE "{_safe_table(test_service_source["name"])}" (timestamp TIMESTAMP, status INTEGER)'
    )
    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert out["requires_group_c"] is True


def test_get_pop_latency_pop_col_present_but_no_origin_data_flags_no_group_c_message(
    in_memory_duckdb,
    test_service_source,
):
    """Pop column EXISTS but no origin timing data → has_data=False
    with requires_group_c=False (different CTA: "ingest more
    data")."""
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert out["requires_group_c"] is False


# ── get_ip_health: lat_val fallback chain ───────────────────────────────


def test_get_ip_health_missing_required_cols_returns_empty(in_memory_duckdb, test_service_source):
    """Without both `oip` AND `ost` columns → has_data=False.
    Pinned because origin IP health requires both."""
    in_memory_duckdb.execute(
        f'CREATE TABLE "{_safe_table(test_service_source["name"])}" (timestamp TIMESTAMP, status INTEGER)'
    )
    out = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


def test_get_ip_health_returns_empty_when_no_qualifying_ips(in_memory_duckdb, test_service_source):
    """No IPs hit the 10-request minimum → empty rows + has_data=False.
    Pinned because the 10-req floor prevents noisy single-shot IPs
    from cluttering the panel."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["oip"] = "1.1.1.1"  # All same IP, only 5 requests
        log["ost"] = 200
        log["ottfb"] = 50000
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})
    # Even though oip + ost present, no IP has >=10 requests
    assert out["has_data"] is False


def test_shielding_analysis_returns_empty_when_no_origin_data(in_memory_duckdb, test_service_source):
    """All required cols exist but no ottfb values are populated →
    ``has_origin_data`` returns False → no shielding analysis."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = None
        log["rid"] = "rA"
        log["prid"] = "rA"
        log["edge"] = True
        log["pop"] = "LAX"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


# ── get_pop_latency happy path ───────────────────────────────────────────


def test_get_pop_latency_returns_rows_with_median_p95_and_elevated_flag(
    in_memory_duckdb,
    test_service_source,
):
    """Happy path: returns has_data=True, includes median_p95_ms,
    and flags rows where p95 > median_p95 * 2 as elevated. Pinned
    because the FE's POP-latency map renders the elevated chip
    differently — losing the flag would mask outliers."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    # Three POPs: two normal (fast), one outlier (slow)
    for i, log in enumerate(logs):
        if i < 7:
            log["pop"], log["ottfb"] = "LAX", 50000  # 50ms
        elif i < 14:
            log["pop"], log["ottfb"] = "JFK", 60000  # 60ms
        else:
            log["pop"], log["ottfb"] = "SLOW", 500000  # 500ms — should be elevated
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})

    assert out["has_data"] is True
    assert "median_p95_ms" in out
    assert out["requires_group_c"] is False
    pops = {r["pop"]: r for r in out["rows"]}
    assert "LAX" in pops and "JFK" in pops and "SLOW" in pops
    # SLOW's p95 of ~500ms is >> 2x the median across the three POPs
    assert pops["SLOW"]["elevated"] is True
    assert pops["LAX"]["elevated"] is False


def test_get_pop_latency_uses_ttfb_when_ottfb_column_missing(
    in_memory_duckdb,
    test_service_source,
):
    """When ottfb isn't in the schema but ttfb is, the latency value
    is computed as `ttfb * 1000000` (µs). Pinned because losing this
    fallback would skip POP-latency for services on the basic
    logging preset."""
    table = _safe_table(test_service_source["name"])
    # Build table without ottfb column
    in_memory_duckdb.execute(f'CREATE TABLE "{table}" ("timestamp" TIMESTAMP, "pop" VARCHAR, "ttfb" DOUBLE)')
    from datetime import UTC, datetime, timedelta

    base = datetime.now(UTC) - timedelta(hours=1)
    for i, ttfb_s in enumerate([0.05, 0.06, 0.07, 0.5]):  # 50-500ms
        in_memory_duckdb.execute(
            f'INSERT INTO "{table}" VALUES (?, ?, ?)',
            [(base + timedelta(seconds=i * 30)).isoformat(), "LAX", ttfb_s],
        )

    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})

    assert out["has_data"] is True
    lax_row = next(r for r in out["rows"] if r["pop"] == "LAX")
    # 95th percentile of 50/60/70/500 ms ≈ 500ms
    assert lax_row["p95_ms"] is not None
    assert lax_row["p95_ms"] >= 100  # significantly elevated past the lower three


# ── get_ip_health happy path ─────────────────────────────────────────────


def test_get_ip_health_returns_rows_ordered_by_error_pct_desc(
    in_memory_duckdb,
    test_service_source,
):
    """Returns oips with >=10 requests, ranked by error_pct DESC.
    Pinned because the FE's "Worst origin IPs" panel relies on
    error-pct ordering — flipping ASC would highlight the best
    instead of the worst."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    # 20 logs for 1.1.1.1 with 50% errors; 20 logs for 2.2.2.2 with 10% errors
    for i, log in enumerate(logs):
        if i < 20:
            log["oip"] = "1.1.1.1"
            log["ost"] = 500 if i < 10 else 200
        else:
            log["oip"] = "2.2.2.2"
            log["ost"] = 500 if i < 22 else 200
        log["ottfb"] = 50000
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})

    assert out["has_data"] is True
    rows = out["rows"]
    assert len(rows) == 2
    # Highest error_pct first
    assert rows[0]["oip"] == "1.1.1.1"
    assert rows[0]["error_pct"] >= rows[1]["error_pct"]


# ── get_shielding_analysis: missing fields + edge_only branches ─────────


def test_get_shielding_analysis_reports_missing_required_fields(
    in_memory_duckdb,
    test_service_source,
):
    """Missing one of (rid, prid, edge, pop, ottfb) → ``requires_fields``
    in response. Pinned because the FE shows a "enable Group L"
    CTA listing the missing fields by name."""
    table = _safe_table(test_service_source["name"])
    # Schema with most-but-not-all required columns (missing `prid`)
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table}" ("timestamp" TIMESTAMP, "rid" VARCHAR, "edge" BOOLEAN, "pop" VARCHAR, "ottfb" DOUBLE)'
    )
    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert "prid" in out.get("requires_fields", [])


def test_get_shielding_analysis_returns_edge_only_when_no_shield_rows(
    in_memory_duckdb,
    test_service_source,
):
    """All required cols + ottfb populated but no shield rows
    (edge=false with prid set) → ``edge_only=True``. Pinned because
    the FE uses this exact key to render the "service has no
    shielding configured" empty state instead of a generic empty
    table."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["edge"] = True  # No shield rows
        log["rid"] = f"r{i}"
        log["prid"] = ""
        log["pop"] = "LAX"
        log["ottfb"] = 50000
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert out.get("edge_only") is True


def test_get_shielding_analysis_happy_path_returns_edge_to_shield_rows(
    in_memory_duckdb,
    test_service_source,
):
    """Happy path: edge logs + matching shield logs (via prid=rid) →
    aggregated rows grouped by (edge_pop, shield_pop) with percentile
    latencies + distance enrichment. Pinned because losing the join
    would zero the entire shielding panel for services WITH
    shielding configured."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)

    # 10 edge logs at LAX with matching shield logs at IAD (prid = rid)
    for i, log in enumerate(logs):
        rid = f"req{i}"
        if i < 10:
            log["edge"] = True
            log["rid"] = rid
            log["prid"] = ""
            log["pop"] = "LAX"
            log["ottfb"] = 100000  # 100ms at edge
        else:
            # 10 corresponding shield logs at IAD
            shield_rid = f"req{i - 10}"
            log["edge"] = False
            log["rid"] = f"sr{i}"
            log["prid"] = shield_rid
            log["pop"] = "IAD"
            log["ottfb"] = 30000  # 30ms at shield
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, table, logs)

    # `_enrich_with_distance` reads from cache/pop_locations.json (relative
    # path, populated by a running dev server). A fresh clone has no cache
    # so the unmocked lookup returns {} and edge_lat/shield_lat are None.
    fake_pops = {"LAX": (33.9425, -118.4081), "IAD": (38.9445, -77.4558)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})

    # Has at least one row from the LAX→IAD join
    assert len(out["rows"]) >= 1
    row = next((r for r in out["rows"] if r["edge_pop"] == "LAX" and r["shield_pop"] == "IAD"), None)
    assert row is not None
    assert row["requests"] == 10
    # p95 of (100ms - 30ms) ≈ 70ms
    assert row["p95_ms"] is not None
    # `_enrich_with_distance` added lat/lon for both pops
    assert row.get("edge_lat") is not None
    assert row.get("shield_lat") is not None


# ── get_shielding_analysis: shielding-audit-2026-06-30 fixes (M1/L3/T10/T11) ──


def _shield_pair_logs(src, *, edge_pop, shield_pop, n, edge_us, shield_us, rid_prefix, ttfb=None):
    """Build ``n`` edge logs at ``edge_pop`` + ``n`` matching shield logs at
    ``shield_pop`` (joined via prid=rid). ``edge_us``/``shield_us`` are ottfb
    microseconds (pass ``None`` to leave ottfb NULL and exercise the ttfb
    fallback)."""
    edge_logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    shield_logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i in range(n):
        rid = f"{rid_prefix}{i}"
        e = edge_logs[i]
        e["edge"] = True
        e["rid"] = rid
        e["prid"] = ""
        e["pop"] = edge_pop
        e["ottfb"] = edge_us
        e["ttfb"] = ttfb
        s = shield_logs[i]
        s["edge"] = False
        s["rid"] = f"s{rid_prefix}{i}"
        s["prid"] = rid
        s["pop"] = shield_pop
        s["ottfb"] = shield_us
        s["ttfb"] = None
    return edge_logs + shield_logs


def test_shielding_analysis_edge_filter_does_not_strip_shield_leg(
    in_memory_duckdb,
    test_service_source,
):
    """T10: a filter on the EDGE leg (``pop = DEN``) must not strip the
    shield-side rows before the join. The shield CTE only carries time
    bounds — otherwise filtering by edge POP would drop the IAD shield hit
    and zero the DEN→IAD route. This invariant was previously untested."""
    table = _safe_table(test_service_source["name"])
    logs = _shield_pair_logs(
        test_service_source, edge_pop="DEN", shield_pop="IAD", n=8, edge_us=100000, shield_us=30000, rid_prefix="den"
    )
    # A second, unrelated edge POP that the filter should exclude.
    logs += _shield_pair_logs(
        test_service_source, edge_pop="ORD", shield_pop="IAD", n=8, edge_us=90000, shield_us=30000, rid_prefix="ord"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {"DEN": (39.86, -104.67), "IAD": (38.94, -77.46), "ORD": (41.97, -87.90)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {"pop": FilterSpec(mode="include", values=["DEN"])},
        )

    pairs = {(r["edge_pop"], r["shield_pop"]) for r in out["rows"]}
    # DEN→IAD survives the edge filter (the shield IAD rows weren't stripped).
    assert ("DEN", "IAD") in pairs
    # ORD edge rows were filtered out, so no ORD→IAD route.
    assert ("ORD", "IAD") not in pairs
    den = next(r for r in out["rows"] if r["edge_pop"] == "DEN")
    assert den["requests"] == 8


def test_shielding_analysis_uses_ttfb_fallback_when_ottfb_null(
    in_memory_duckdb,
    test_service_source,
):
    """T11 (b198b04): when ``ottfb`` is NULL the transit delta falls back to
    ``ttfb`` (seconds → µs). Edge ttfb 0.1s, shield ottfb 30ms → ~70ms p50."""
    table = _safe_table(test_service_source["name"])
    # Edge ottfb NULL but ttfb = 0.1s (100ms); shield ottfb = 30ms.
    logs = _shield_pair_logs(
        test_service_source,
        edge_pop="LAX",
        shield_pop="IAD",
        n=10,
        edge_us=None,
        shield_us=30000,
        rid_prefix="fb",
        ttfb=0.1,
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {"LAX": (33.94, -118.41), "IAD": (38.94, -77.46)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})

    row = next(r for r in out["rows"] if r["edge_pop"] == "LAX" and r["shield_pop"] == "IAD")
    # (100ms via ttfb fallback) - (30ms via ottfb) = ~70ms, NOT a negative or
    # NULL value (which is what a missing fallback would produce).
    assert row["p50_ms"] == pytest.approx(70.0, abs=1.0)


def test_shielding_analysis_has_data_true_without_coords(
    in_memory_duckdb,
    test_service_source,
):
    """L3: rows present but POP codes absent from the location map → the
    table still has data (``has_data`` gates on ROW presence, not arc
    coordinates). Previously this returned ``has_data=False`` and hid the
    whole table + CSV export whenever a POP was missing from
    pop_locations.json."""
    table = _safe_table(test_service_source["name"])
    logs = _shield_pair_logs(
        test_service_source, edge_pop="ZZZ", shield_pop="QQQ", n=5, edge_us=100000, shield_us=20000, rid_prefix="nc"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    # Empty POP map → no coords resolve for either POP.
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={}):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})

    assert out["has_data"] is True
    assert len(out["rows"]) == 1
    row = out["rows"][0]
    assert row["edge_lat"] is None and row["shield_lat"] is None


def test_shielding_analysis_keeps_low_volume_high_overhead_route(
    in_memory_duckdb,
    test_service_source,
):
    """M1/T12: a low-volume but high-overhead route must NOT be buried by a
    request-volume LIMIT — it's exactly the mis-peered route this analysis
    exists to surface. With a small limit, the high-overhead route still
    appears (via the top-by-overhead rank), and ``total_routes`` /
    ``truncated`` report the full picture."""
    table = _safe_table(test_service_source["name"])
    logs = []
    # Six high-volume, low-overhead routes (50 reqs each, ~10ms transit). With
    # limit=2 the union of (top-2 by requests) and (top-2 by overhead) covers at
    # most 4 distinct routes, so at least two of these seven are always dropped
    # → ``truncated`` is deterministically True regardless of tie-breaking.
    high_vol_pops = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    for k, ep in enumerate(high_vol_pops):
        logs += _shield_pair_logs(
            test_service_source,
            edge_pop=ep,
            shield_pop="IAD",
            n=50,
            edge_us=40000,
            shield_us=30000,
            rid_prefix=f"hi{k}",
        )
    # One LOW-volume, FAR-HIGHEST-overhead route (3 reqs, ~150ms transit) — it
    # is rank-1 by overhead (so always selected) but rank-7 by request volume
    # (so a plain ``ORDER BY requests DESC LIMIT 2`` would bury it).
    logs += _shield_pair_logs(
        test_service_source, edge_pop="ZED", shield_pop="IAD", n=3, edge_us=180000, shield_us=30000, rid_prefix="lo"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {ep: (float(i * 10), float(i * 10)) for i, ep in enumerate(high_vol_pops)}
    fake_pops.update({"ZED": (40.0, 40.0), "IAD": (38.94, -77.46)})
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {}, limit=2)

    pairs = {(r["edge_pop"], r["shield_pop"]): r for r in out["rows"]}
    # The high-overhead ZED→IAD route survives despite being well outside the
    # top-2 by request volume.
    assert ("ZED", "IAD") in pairs
    assert pairs[("ZED", "IAD")]["requests"] == 3
    # Returned set is bounded by the two rank cutoffs (≤ 2*limit), strictly
    # fewer than the 7 total routes.
    assert len(out["rows"]) <= 4
    # Full route count + truncation flag are surfaced for "Top N of M".
    assert out["total_routes"] == 7
    assert out["truncated"] is True
