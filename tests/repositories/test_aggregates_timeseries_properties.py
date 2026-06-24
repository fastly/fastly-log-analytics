"""Property-based tests for the dashboard + origin analytical SQL.

These two SQL builders are downstream of `build_where_clause` and the
field catalog — they're the chokepoints the FE hits on every dashboard
render. Bugs in here typically surface as a binder error or, worse, a
silently-wrong aggregation (e.g. an empty time-series series that the
chart renders as flat zero).

The contract we pin with hypothesis:

1. **Shape invariant.** Whatever filter + window + interval combo the FE
   throws at us, the response keys are stable (`data`, `time_series`,
   `total_rows`, etc. for aggregates; `has_data`, `series` for
   timeseries). The FE destructures these unconditionally.

2. **No exceptions.** The functions must not raise on any realistic
   filter / interval / metric combination. DuckDB binder errors are
   the most common regression mode and the cheapest to surface
   automatically. Two real bugs were caught + fixed by this test:
   the empty-string-on-INTEGER-column crash and the
   non-numeric-string-on-INTEGER-column crash.

3. **No NaN / inf.** Numeric fields in the response must be JSON-
   serializable. NaN / inf would surface as `null` in JSON (silently
   wrong) or crash the FE's Plotly traces.
"""

from __future__ import annotations

import math

import duckdb
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table
from backend.repositories.dashboard import get_aggregates

_TABLE_NAME = "test_service"


@pytest.fixture
def seeded_con():
    """In-memory DuckDB seeded with a representative analytical table."""
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
            "resp_bytes" INTEGER
        )
        """
    )
    from datetime import UTC, datetime, timedelta

    base = datetime.now(UTC) - timedelta(hours=1)
    statuses = [200, 200, 200, 200, 404, 500, 200, 304, 301, 200]
    pops = ["LAX", "JFK", "LHR", "LAX", "JFK", "LAX", "LHR", "JFK", "LAX", "LHR"]
    for i in range(10):
        ts = base + timedelta(minutes=i * 5)
        con.execute(
            f"""INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                statuses[i],
                "US",
                f"/path-{i}",
                f"10.0.0.{i}",
                "GET",
                "Mozilla/5.0",
                pops[i],
                15169 + i,
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
            ],
        )
    yield con
    con.close()


_src = {"name": _TABLE_NAME, "service_id": "tsid"}


# ── Strategies ───────────────────────────────────────────────────────────


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
)

# Values the FE could realistically send. Hypothesis explores empty
# strings, whitespace, unicode, wildcards — all of which previously had
# weird interactions with the IN-clause builder. Two real bugs surfaced
# and were fixed in build_where_clause: empty-string values on numeric
# columns ("Could not convert '' to INT32") and non-numeric strings on
# numeric columns. This strategy is broad on purpose so the next
# regression of that kind surfaces here too.
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
    keys=st.sampled_from(_KNOWN_FILTER_COLS).map(lambda c: st.sampled_from([c, f"filter_{c}"])).flatmap(lambda s: s),
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


_chart_interval_strategy = st.sampled_from(["1 minute", "5 minutes", "15 minutes", "1 hour"])
_chart_metric_strategy = st.sampled_from(["requests", "5xx", "4xx", "hit_rate", "p95_latency"])


# ── get_aggregates property tests ────────────────────────────────────────


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
    """No matter what combination of filters/interval/metric the FE
    sends, the returned dict has the same top-level keys. Pinned
    because the FE destructures these unconditionally — a missing
    key is a TypeError at render time."""
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
    """Numeric values in the response must be finite (no NaN / inf).
    Pinned because JSON serializes NaN as `null` silently, and
    Plotly drops the entire series on a single non-finite value."""
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
    """Every entry in `data` has both `top` (list) and `total` (number)
    keys. Pinned because the FE iterates `Object.entries(data)` and
    destructures both — a missing key is a render-time TypeError."""
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


# ── get_timeseries (origin) property tests ──────────────────────────────


_metric_strategy = st.sampled_from(["ttfb", "ottfb", "ttlb"])
_percentile_strategy = st.sampled_from(["p50", "p95", "p99"])
_bucket_minutes_strategy = st.floats(
    min_value=1.0 / 60.0,  # 1 second — exercises the sub-minute branch
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
    """Origin timeseries: every (metric, percentile, bucket_minutes,
    split_by_leg) combo returns a dict with the required keys. Pinned
    because the FE destructures `has_data` and `series`
    unconditionally — a missing key TypeErrors the Plotly trace."""
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
    """For any bucket_minutes (including sub-minute fractions like
    1/60 = 1s), the returned series values must be finite. This is
    the property half of the `_clamp_to_float` regression."""
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
