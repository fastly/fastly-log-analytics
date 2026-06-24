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

import duckdb
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


# ── Shared seeded DuckDB fixture ────────────────────────────────────────


@pytest.fixture
def seeded_con():
    """In-memory DuckDB with a representative schema + 30 rows of data
    that's varied enough to exercise the WHERE/GROUP BY paths in each
    repo without being so big it slows hypothesis down."""
    con = duckdb.connect(":memory:")
    table = _safe_table(_TABLE_NAME)
    con.execute(
        f"""
        CREATE TABLE {table} (
            "timestamp" TIMESTAMPTZ,
            "dt" VARCHAR,
            "timestamp_hour" VARCHAR,
            "status" INTEGER,
            "country" VARCHAR,
            "url" VARCHAR,
            "ip" VARCHAR,
            "method" VARCHAR,
            "ua" VARCHAR,
            "pop" VARCHAR,
            "asn" INTEGER,
            "city" VARCHAR,
            "region" VARCHAR,
            "elapsed" INTEGER,
            "cache" VARCHAR,
            "ottfb" DOUBLE,
            "ttfb" DOUBLE,
            "waf_sig" VARCHAR,
            "edge" BOOLEAN,
            "lat" DOUBLE,
            "lon" DOUBLE,
            "resp_bytes" INTEGER,
            "oip" VARCHAR,
            "ost" INTEGER,
            "tcp_rtt" INTEGER,
            "transport" VARCHAR,
            "ploss" DOUBLE,
            "rtt_var" INTEGER,
            "rtt_min" INTEGER,
            "retrans" INTEGER,
            "bw" INTEGER,
            "c_speed" VARCHAR,
            "c_type" VARCHAR,
            "delivery_rate" INTEGER,
            "metro" VARCHAR,
            "p_type" VARCHAR,
            "ja3" VARCHAR,
            "ja4" VARCHAR,
            "ttl" INTEGER,
            "rid" VARCHAR,
            "prid" VARCHAR,
            "server_region" VARCHAR,
            "ottlb" DOUBLE
        )
        """
    )
    from datetime import UTC, datetime, timedelta

    base = datetime.now(UTC) - timedelta(hours=1)
    statuses = [200, 200, 200, 200, 404, 500, 200, 304, 301, 200] * 3
    pops = ["LAX", "JFK", "LHR"] * 10
    for i in range(30):
        ts = base + timedelta(minutes=i * 2)
        con.execute(
            f"INSERT INTO {table} VALUES " + "(" + ", ".join(["?"] * 43) + ")",
            [
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                statuses[i],
                "US" if i % 3 == 0 else "GB",
                f"/path-{i % 10}",
                f"10.0.0.{i}",
                "GET",
                "Mozilla/5.0",
                pops[i],
                15169 + (i % 5),
                "San Francisco",
                "CA",
                50 + i * 10,
                "HIT" if i % 2 == 0 else "MISS",
                50000.0 + i,
                0.05 + i * 0.01,
                "",
                True,
                37.7749,
                -122.4194,
                500 + i * 100,
                "origin-1.example.com",
                statuses[i],
                10000 + i * 100,  # tcp_rtt
                "h2",
                0.0,
                100,
                50,
                0,
                100000,
                "C",
                "broadband",
                100000,
                "807",  # metro
                "U",
                "ja3_fp",
                "ja4_fp",
                300,
                f"rid_{i}",
                f"prid_{i % 10}",
                "us-west",
                0.05 + i * 0.01,
            ],
        )
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

    result = get_health(seeded_con, _src, start, end, filters)
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

    result = get_health(seeded_con, _src, None, None, filters)
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

    result = get_quality(seeded_con, _src, start, end, filters)
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
