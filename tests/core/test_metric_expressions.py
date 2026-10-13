"""Unit tests for the canonical metric-expression catalog.

Asserts the EXACT SQL string returned for every availability shape and for
each ``absent`` value the real call sites pass — this is a byte-for-byte
contract (the rollup parquet schema and the repository query SQL both depend
on these literals being unchanged).
"""

from __future__ import annotations

from backend.core import metric_expressions


class TestOriginLatencyUs:
    def test_both_columns_prefers_ottfb_with_ttfb_fallback(self) -> None:
        for cols in ({"ottfb", "ttfb"}, ["ottfb", "ttfb"], {"ottfb", "ttfb", "url"}):
            assert metric_expressions.origin_latency_us(cols) == 'COALESCE("ottfb", "ttfb" * 1000000.0)'

    def test_ottfb_only(self) -> None:
        assert metric_expressions.origin_latency_us({"ottfb"}) == '"ottfb"'

    def test_ttfb_only_scaled_to_us(self) -> None:
        assert metric_expressions.origin_latency_us({"ttfb"}) == '"ttfb" * 1000000.0'

    def test_neither_defaults_to_null_literal(self) -> None:
        # Default absent="NULL" — the repository seed's contract (always
        # valid SQL).
        assert metric_expressions.origin_latency_us(set()) == "NULL"
        assert metric_expressions.origin_latency_us({"url", "pop"}) == "NULL"

    def test_neither_with_absent_none_signals_skip(self) -> None:
        # The rollup writers pass absent=None to mean "skip this service".
        assert metric_expressions.origin_latency_us(set(), absent=None) is None
        assert metric_expressions.origin_latency_us({"url"}, absent=None) is None

    def test_present_branches_ignore_absent_value(self) -> None:
        # absent only affects the neither-present case.
        assert (
            metric_expressions.origin_latency_us({"ottfb", "ttfb"}, absent=None)
            == 'COALESCE("ottfb", "ttfb" * 1000000.0)'
        )
        assert metric_expressions.origin_latency_us({"ottfb"}, absent=None) == '"ottfb"'
        assert metric_expressions.origin_latency_us({"ttfb"}, absent=None) == '"ttfb" * 1000000.0'


class TestBaseSeedDelegation:
    """The repository seed must stay a byte-faithful delegate."""

    def test_origin_latency_us_expr_matches_catalog(self) -> None:
        from backend.repositories._base import origin_latency_us_expr

        for cols in (
            {"ottfb", "ttfb"},
            {"ottfb"},
            {"ttfb"},
            set(),
            {"url"},
        ):
            assert origin_latency_us_expr(cols) == metric_expressions.origin_latency_us(cols, absent="NULL")

        # Never returns None (always valid SQL).
        assert origin_latency_us_expr(set()) == "NULL"
