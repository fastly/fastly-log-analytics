import pytest

from backend.models.common import FilterSpec
from backend.repositories.utils.filters import build_where_clause


def test_build_where_clause_basic():
    """Test basic date range filtering and partition pruning."""
    start = "2024-01-01T00:00:00Z"
    end = "2024-01-02T00:00:00Z"
    filters = {}

    params, sql = build_where_clause(start, end, filters, inline_params=False, partition_pruning=True)

    assert "timestamp >=" in sql
    assert "timestamp <=" in sql
    # Params now includes original start/end times plus derived `dt` and `hour` partition values for start and end
    assert len(params) == 6
    assert params[0] == start
    assert params[3] == end


def test_build_where_clause_filters():
    """Test include and exclude filters."""
    filters = {
        "status": FilterSpec(mode="include", values=[200, 404]),
        "country": FilterSpec(mode="exclude", values=["US"]),
    }

    params, sql = build_where_clause(None, None, filters, inline_params=False)

    # IN-clause uses CAST(col AS VARCHAR) + stringified params defensively
    # so mixed-type filter values don't crash. See
    # test_aggregates_timeseries_properties for the original property-
    # test failure that surfaced this.
    assert "(CAST(status AS VARCHAR) IN (?, ?))" in sql
    # Numeric params get stringified for the IN-clause comparison
    assert "200" in params
    assert "404" in params

    # Check exclude parameter (country is already VARCHAR in the registry, so CAST is bypassed for performance)
    assert "(country NOT IN (?))" in sql
    assert "US" in params


def test_build_where_clause_inline():
    """Test inline parameterization (used for temporary tables)."""
    filters = {
        "status": FilterSpec(mode="include", values=[200, 404]),
        "country": FilterSpec(mode="exclude", values=["US"]),
    }

    params, sql = build_where_clause(None, None, filters, inline_params=True)

    assert len(params) == 0  # No parameterized query arguments
    # CAST AS VARCHAR + stringified literals matches the behaviour of
    # the wildcard branch and makes mixed-type filter values safe.
    assert "(CAST(status AS VARCHAR) IN ('200', '404'))" in sql
    assert "(country NOT IN ('US'))" in sql


def test_build_where_clause_excludes_rum_beacons():
    """Test that /rum-beacon is unconditionally excluded when url is present in actual_cols."""
    params, sql = build_where_clause(None, None, {}, actual_cols=["url"])
    assert "url != '/rum-beacon' AND url NOT LIKE '/rum-beacon' || chr(63) || '%'" in sql

    # If url is not present, no exclusion should be added
    params, sql = build_where_clause(None, None, {}, actual_cols=["ip"])
    assert "url != '/rum-beacon'" not in sql


class TestBuildWhereClauseExtended:
    def test_no_args_returns_passthrough(self):
        params, sql = build_where_clause(None, None, {})
        assert sql == "1=1"
        assert params == []

    def test_filter_prefix_stripped(self):
        # The frontend sometimes sends "filter_country" or "xfilter_country"
        filters = {"filter_country": FilterSpec(mode="include", values=["US"])}
        _, sql = build_where_clause(None, None, filters)
        assert "country" in sql
        assert "filter_country" not in sql

    def test_xfilter_prefix_stripped(self):
        filters = {"xfilter_country": FilterSpec(mode="exclude", values=["US"])}
        _, sql = build_where_clause(None, None, filters)
        # Since country is VARCHAR in the registry, it is NOT wrapped in CAST defensively
        assert "country NOT IN" in sql

    def test_numeric_suffix_stripped(self):
        # Frontend appends _2, _3 for duplicate filter keys
        filters = {"country_2": FilterSpec(mode="include", values=["DE"])}
        _, sql = build_where_clause(None, None, filters)
        assert "country" in sql
        assert "country_2" not in sql

    def test_wildcard_include(self):
        filters = {"url": FilterSpec(mode="include", values=["/api/*"])}
        _, sql = build_where_clause(None, None, filters, inline_params=True)
        assert "ILIKE '/api/%'" in sql

    def test_wildcard_exclude(self):
        filters = {"url": FilterSpec(mode="exclude", values=["/health*"])}
        _, sql = build_where_clause(None, None, filters, inline_params=True)
        assert "NOT ILIKE '/health%'" in sql

    def test_mixed_exact_and_wildcard(self):
        filters = {"url": FilterSpec(mode="include", values=["/api", "/static/*"])}
        _, sql = build_where_clause(None, None, filters, inline_params=True)
        assert "IN ('/api')" in sql
        assert "ILIKE '/static/%'" in sql

    def test_null_value_in_include(self):
        filters = {"country": FilterSpec(mode="include", values=[None])}
        _, sql = build_where_clause(None, None, filters)
        assert "IS NULL" in sql

    def test_null_value_in_exclude(self):
        filters = {"country": FilterSpec(mode="exclude", values=[None])}
        _, sql = build_where_clause(None, None, filters)
        assert "IS NOT NULL" in sql

    def test_null_mixed_with_values(self):
        filters = {"country": FilterSpec(mode="include", values=["US", None])}
        _, sql = build_where_clause(None, None, filters)
        assert "IN" in sql
        assert "IS NULL" in sql

    def test_empty_values_list_skipped(self):
        filters = {"country": FilterSpec(mode="include", values=[])}
        params, sql = build_where_clause(None, None, filters)
        assert sql == "1=1"

    def test_empty_string_values_stripped_defensively(self):
        """Filter values that are empty / whitespace-only must NOT
        reach the IN-clause builder. Pinned because losing this
        produced ``WHERE ("status" IN (''))`` against an INTEGER
        column and crashed with "Could not convert string '' to
        INT32" — surfaced by the hypothesis property tests for
        get_aggregates. The FE drops empty values before POSTing,
        but a regression on that side would 500 the dashboard."""
        filters = {"country": FilterSpec(mode="include", values=["", "   ", "US"])}
        params, sql = build_where_clause(None, None, filters)
        # Only "US" survives — placeholder count matches param count
        assert sql.count("?") == len(params) == 1
        assert "US" in params

    def test_all_empty_string_values_collapse_to_passthrough(self):
        """When every value in a filter is empty/whitespace, the
        whole filter is dropped (no IN clause, no syntactic error)."""
        filters = {"country": FilterSpec(mode="include", values=["", "   "])}
        params, sql = build_where_clause(None, None, filters)
        assert sql == "1=1"
        assert params == []

    def test_null_filter_still_works_after_empty_string_strip(self):
        """``values=[None]`` (explicit NULL filter) must NOT be
        affected by the empty-string strip. Pinned because the FE
        sends `[None]` to filter "rows where country IS NULL" — if
        we accidentally dropped None alongside empty strings, that
        feature would silently break."""
        filters = {"country": FilterSpec(mode="include", values=[None])}
        params, sql = build_where_clause(None, None, filters)
        assert "IS NULL" in sql

    def test_waf_sig_ind_uses_list_contains(self):
        filters = {"waf_sig_ind": FilterSpec(mode="include", values=["XSS"])}
        _, sql = build_where_clause(None, None, filters)
        assert "list_contains" in sql
        assert "waf_sig" in sql

    def test_waf_sig_ind_exclude(self):
        filters = {"waf_sig_ind": FilterSpec(mode="exclude", values=["SQLi"])}
        _, sql = build_where_clause(None, None, filters)
        assert "NOT list_contains" in sql

    def test_partition_pruning_skipped_when_dt_not_in_actual_cols(self):
        # When actual_cols is provided and "dt" is absent, no dt condition added.
        _, sql = build_where_clause(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", {}, actual_cols=["timestamp"], partition_pruning=True
        )
        assert "dt >=" not in sql
        assert "timestamp >=" in sql

    def test_partition_pruning_included_when_dt_present(self):
        _, sql = build_where_clause(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", {}, actual_cols=["timestamp", "dt"], partition_pruning=True
        )
        assert "dt >=" in sql
        assert "dt <=" in sql

    def test_timezone_aware_iso_parsed_correctly(self):
        # A timestamp with explicit +05:30 should be converted to UTC date correctly.
        _, sql = build_where_clause("2024-06-15T18:30:00+05:30", None, {}, inline_params=True, partition_pruning=True)
        # 18:30 IST == 13:00 UTC → date is still 2024-06-15
        assert "dt >= '2024-06-15'" in sql


# ── _clamp_to_float (regression: origin-latency 1s granularity bug) ─────
#
# Pins the fix for the "1s granularity behaved identically to 1m" bug
# documented in TESTING_PLAN.md's Sidetracks section:
#
#   Before: ``Limit1440 = Annotated[int, AfterValidator(_clamp_to(1440))]``
#   ``int(0.0167) → 0``, then ``max(1, 0) → 1`` so backend always used
#   `INTERVAL '1' minutes` regardless of what the FE requested.
#
#   After: ``Limit1440 = Annotated[float, AfterValidator(_clamp_to_float(1440.0))]``
#   ``max(1/3600, min(1440, 0.0167))`` → 0.0167 preserved.
#
# The only previous regression coverage was a Playwright E2E that doesn't
# run in CI. These unit tests add a fast-feedback CI gate.


class TestClampToFloatPreservesSubMinuteValues:
    def test_clamp_to_float_preserves_one_second_in_minutes(self):
        """``1/60`` (one second expressed as minutes) must NOT be
        rounded to 0 or to 1. Pinned because losing this would
        silently turn a "1s" granularity selector into "1m" — the
        original bug."""
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        out = clamp(1.0 / 60.0)
        assert 0 < out < 1, f"Sub-minute value collapsed to {out}"
        assert abs(out - 1.0 / 60.0) < 1e-9

    def test_clamp_to_float_uses_one_second_lower_bound(self):
        """Anything below ``1/3600`` (one second in minutes) is clamped
        UP to that floor — never to 0 (which would crash interval
        SQL) or to 1m (which would silently disable sub-minute
        granularity)."""
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        out = clamp(0.0)
        assert out == pytest.approx(1.0 / 3600.0)

    def test_clamp_to_float_clamps_to_upper_bound(self):
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        assert clamp(99999.0) == 1440.0

    def test_clamp_to_float_returns_value_unchanged_in_range(self):
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        assert clamp(60.0) == 60.0
        assert clamp(1.5) == 1.5

    def test_limit1440_pydantic_annotation_validates_to_float(self):
        """The ``Limit1440`` Annotated type must surface as a float in
        the validated payload. Pinned because reverting to ``int``
        is exactly the regression we're guarding against — the
        validator silently coerces 0.0167 → 0 → 1."""
        import pydantic

        from backend.models.common import Limit1440

        class _M(pydantic.BaseModel):
            v: Limit1440 = 5.0

        out = _M(v=1.0 / 60.0)
        assert isinstance(out.v, float)
        # Crucially, NOT zero (would crash) and NOT 1.0 (the original bug)
        assert 0 < out.v < 1
        assert abs(out.v - 1.0 / 60.0) < 1e-9
