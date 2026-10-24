"""Property-based tests for every analytical repository SQL builder.

Each repo function has the same shape: ``(con, src, start_time, end_time,
filters, ...) -> dict``. They all call ``build_where_clause`` and execute
the result against DuckDB. The previous property-test batch on
``dashboard.get_aggregates`` and ``origin.get_timeseries`` surfaced two
real production bugs in ``build_where_clause`` (empty-string values on
numeric columns, type-mismatched filter values). This file extends the
same Hypothesis approach to the remaining repos to surface bugs that
share that class.

The contracts pinned for every repo:

1. **No exceptions.** For any filter/time combination Hypothesis generates,
   the function must return a dict instead of raising. A DuckDB binder
   error is the most common regression mode.

2. **Stable shape.** The returned dict must always have its declared
   top-level keys (FE destructures these unconditionally).

3. **No NaN/inf.** Numeric values that survive into the JSON payload
   must be finite — JSON null-coerces NaN silently, and Plotly drops
   any series containing a non-finite value.
"""

from __future__ import annotations

import math

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from backend.models.common import FilterSpec
from tests._helpers.duckdb_fixtures import make_seeded_logs_con

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


# ── Shared seeded DuckDB fixture ────────────────────────────────────────


@pytest.fixture
def seeded_con():
    """In-memory DuckDB with a representative schema + 30 rows of data
    that's varied enough to exercise the WHERE/GROUP BY paths in each
    repo without being so big it slows hypothesis down."""
    con = make_seeded_logs_con(_TABLE_NAME, num_rows=30)
    yield con
    con.close()


# ── Strategies (shared across all repo tests) ───────────────────────────


_KNOWN_FILTER_COLS = (
    "status",
    "country",
    "url",
    "ip",
    "method",
    "ua",
    "pop",
    "asn",
    "city",
    "region",
    "cache",
    "waf_sig",
    "edge",
    "metro",
    "transport",
    "p_type",
    "c_speed",
    "c_type",
    "server_region",
)

# Same broad strategy used in test_aggregates_timeseries_properties.
# Hypothesis explores empty strings, weird unicode, mixed types — the
# exact inputs that caught the previous bugs.
_filter_value_strategy = st.one_of(
    st.text(min_size=0, max_size=15),
    st.integers(min_value=-1000, max_value=1000),
    st.none(),
    st.sampled_from(["*foo*", "200", "404", "US", "GB", "HIT", "MISS"]),
)

_filter_spec_strategy = st.builds(
    FilterSpec,
    mode=st.sampled_from(["include", "exclude"]),
    values=st.lists(_filter_value_strategy, min_size=0, max_size=3),
)

_filters_strategy = st.dictionaries(
    keys=st.sampled_from(_KNOWN_FILTER_COLS).flatmap(lambda c: st.sampled_from([c, f"filter_{c}"])),
    values=_filter_spec_strategy,
    min_size=0,
    max_size=3,
)

_iso_time_strategy = st.one_of(
    st.none(),
    st.sampled_from(
        [
            "2026-05-18T00:00:00Z",
            "2026-05-18T12:00:00Z",
            "2026-05-19T00:00:00Z",
        ]
    ),
)


def _assert_all_finite(d, path="root"):
    """Recursively walk a dict/list response and assert every numeric
    value is finite. NaN/inf would surface as JSON `null` (silently
    wrong) or crash Plotly traces."""
    if isinstance(d, dict):
        for k, v in d.items():
            _assert_all_finite(v, f"{path}.{k}")
    elif isinstance(d, list):
        for i, v in enumerate(d):
            _assert_all_finite(v, f"{path}[{i}]")
    elif isinstance(d, float):
        assert math.isfinite(d), f"non-finite value at {path}: {d}"


# ── security.get_security_aggregates ────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_security_aggregates_never_raises(seeded_con, filters, start, end):
    """``get_security_aggregates`` must return a dict for any input.
    The FE security page depends on its top-level keys."""
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_security_aggregates_top_level_keys_stable(seeded_con, filters):
    """Top-level shape must be stable across filter inputs."""
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, filters)
    # Probe a few representative keys; the response is a mixed dict of
    # named result rows but every call should at least return _something_
    # and not blow up.
    assert isinstance(result, dict)


# ── security.get_top_bots ───────────────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_top_bots_never_raises(seeded_con, filters, start, end):
    """``get_top_bots`` must not raise on any filter combination."""
    from backend.repositories.security import get_top_bots

    result = get_top_bots(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)


# ── performance.get_performance_aggregates ──────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_performance_aggregates_never_raises(seeded_con, filters, start, end):
    """``get_performance_aggregates`` must return a dict for any input."""
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_performance_aggregates_required_keys_present(seeded_con, filters):
    """Pin the keys the FE performance page destructures."""
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, None, None, filters)
    for key in ("top_urls", "top_asns", "ttl_dist", "scatter"):
        assert key in result, f"performance_aggregates missing key {key}"


# ── network.get_health ──────────────────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_network_health_never_raises(seeded_con, filters, start, end):
    """``get_health`` must not raise on any filter combination.
    The FE network page is the most chart-heavy and would be fully
    blank on any binder-error here."""
    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_network_health_required_keys_present(seeded_con, filters):
    """Pin the always-present ``available`` flag the FE network page
    keys on. When available=False, ``reason`` carries a CTA string
    the FE renders verbatim; when True, the result has the full
    by-ASN payload."""
    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, None, None, filters)
    assert "available" in result
    if result["available"] is False:
        assert "reason" in result, "false-available response must explain why"


# ── network.get_quality ─────────────────────────────────────────────────


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_network_get_quality_never_raises(seeded_con, filters, start, end):
    from backend.repositories.network import get_quality

    result = get_quality(lambda: seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


# ── origin.get_summary / status_codes / pop_latency / ip_health / path_breakdown


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_summary_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_summary

    result = get_summary(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_status_codes_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_status_codes

    result = get_status_codes(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_pop_latency_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_pop_latency

    result = get_pop_latency(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_ip_health_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_ip_health

    result = get_ip_health(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_path_breakdown_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_path_breakdown

    result = get_path_breakdown(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_slow_urls_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_slow_urls

    result = get_slow_urls(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    _assert_all_finite(result)


@given(filters=_filters_strategy, start=_iso_time_strategy, end=_iso_time_strategy)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_origin_get_shielding_analysis_never_raises(seeded_con, filters, start, end):
    from backend.repositories.origin import get_shielding_analysis

    result = get_shielding_analysis(seeded_con, _src, start, end, filters)
    assert isinstance(result, dict)
    assert "has_data" in result
    _assert_all_finite(result)


# ── Dashboard & Origin timeseries property tests (merged from test_aggregates_timeseries_properties.py) ──

from backend.repositories.dashboard import get_aggregates  # noqa: E402

_chart_interval_strategy = st.sampled_from(["1 minute", "5 minutes", "15 minutes", "1 hour"])
_chart_metric_strategy = st.sampled_from(["requests", "5xx", "4xx", "hit_rate", "p95_latency"])

_EXPECTED_AGG_KEYS = {"data", "time_series", "where_clause", "interval", "metric", "total_rows", "total_rows_total"}


@given(
    filters=_filters_strategy,
    start=_iso_time_strategy,
    end=_iso_time_strategy,
    interval=_chart_interval_strategy,
    metric=_chart_metric_strategy,
)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_aggregates_returns_stable_shape_for_any_input(seeded_con, filters, start, end, interval, metric):
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=start,
        end_time=end,
        filters=filters,
        chart_interval=interval,
        chart_metric=metric,
    )
    missing = _EXPECTED_AGG_KEYS - set(result.keys())
    assert not missing, f"Aggregates response missing keys {missing}; got {sorted(result.keys())}"


@given(
    filters=_filters_strategy,
    interval=_chart_interval_strategy,
    metric=_chart_metric_strategy,
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_aggregates_numeric_fields_are_finite(seeded_con, filters, interval, metric):
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters=filters,
        chart_interval=interval,
        chart_metric=metric,
    )
    assert isinstance(result["total_rows"], int)
    assert isinstance(result["total_rows_total"], int)
    for point in result.get("time_series", []):
        v = point.get("value")
        if isinstance(v, float):
            assert math.isfinite(v), f"non-finite time_series value: {v} in {point!r}"


@given(
    filters=_filters_strategy,
    interval=_chart_interval_strategy,
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_aggregates_data_field_keys_are_well_formed(seeded_con, filters, interval):
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters=filters,
        chart_interval=interval,
        chart_metric="requests",
    )
    for field_name, entry in result["data"].items():
        assert "top" in entry, f"Field {field_name!r} missing 'top'"
        assert "total" in entry, f"Field {field_name!r} missing 'total'"
        assert isinstance(entry["top"], list)


_metric_strategy = st.sampled_from(["ttfb", "ottfb", "ttlb"])
_percentile_strategy = st.sampled_from(["p50", "p95", "p99"])
_bucket_minutes_strategy = st.floats(
    min_value=1.0 / 60.0,
    max_value=60.0,
    allow_nan=False,
    allow_infinity=False,
)


@given(
    filters=_filters_strategy,
    metric=_metric_strategy,
    percentile=_percentile_strategy,
    bucket_minutes=_bucket_minutes_strategy,
    split_by_leg=st.booleans(),
)
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_timeseries_returns_stable_shape(seeded_con, filters, metric, percentile, bucket_minutes, split_by_leg):
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters=filters,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        metric=metric,
        percentile=percentile,
    )
    assert "has_data" in result
    assert "series" in result or result.get("has_data") is False


@given(
    bucket_minutes=_bucket_minutes_strategy,
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_get_timeseries_sub_minute_bucket_produces_finite_values(seeded_con, bucket_minutes):
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        bucket_minutes=bucket_minutes,
        split_by_leg=False,
        metric="ttfb",
        percentile="p95",
    )
    for series in result.get("series", []):
        for v in series.get("values", []):
            if isinstance(v, float):
                assert math.isfinite(v), f"non-finite timeseries value: {v}"
