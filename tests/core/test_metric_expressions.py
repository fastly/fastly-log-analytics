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


class TestNullDefaultHelpersGoldenSql:
    """Each case pins the literal that a repository call site rendered before
    it was moved onto the helpers; a change here changes served SQL."""

    def test_hit_states_literal(self) -> None:
        assert metric_expressions.HIT_STATES == "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"

    def test_assets_column_defaults_present(self) -> None:
        cols = {"resp_header_content_encoding", "resp_bytes", "status", "ttl", "cache"}
        f = metric_expressions.column_or_default
        assert f(cols, "resp_header_content_encoding", "''") == "COALESCE(\"resp_header_content_encoding\", '')"
        assert f(cols, "resp_bytes", "0", cast="BIGINT") == 'COALESCE(CAST("resp_bytes" AS BIGINT), 0)'
        assert f(cols, "status", "200", cast="INTEGER") == 'COALESCE(CAST("status" AS INTEGER), 200)'
        assert f(cols, "ttl", "0.0", cast="DOUBLE") == 'COALESCE(CAST("ttl" AS DOUBLE), 0.0)'
        assert f(cols, "cache", "'MISS'") == "COALESCE(\"cache\", 'MISS')"

    def test_assets_column_defaults_absent(self) -> None:
        f = metric_expressions.column_or_default
        assert f(set(), "resp_header_content_encoding", "''") == "''"
        assert f(set(), "resp_bytes", "0", cast="BIGINT") == "0"
        assert f({"url"}, "status", "200", cast="INTEGER") == "200"
        assert f(set(), "ttl", "0.0", cast="DOUBLE") == "0.0"
        assert f(set(), "cache", "'MISS'") == "'MISS'"

    def test_performance_waterfall_exprs(self) -> None:
        c = metric_expressions.coalesce
        assert c("CAST(ottfb AS DOUBLE) / 1000.0", "0") == "COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0)"
        assert c("CAST(ottlb AS DOUBLE) / 1000.0", "0") == "COALESCE(CAST(ottlb AS DOUBLE) / 1000.0, 0)"

    def test_sessions_sum_exprs(self) -> None:
        s = metric_expressions.sum_or_zero
        assert f"CAST({s('"resp_bytes"')} AS BIGINT)" == 'CAST(COALESCE(SUM("resp_bytes"), 0) AS BIGINT)'
        assert f"CAST({s('"tcp_rtt"', zero='0.0')} AS DOUBLE)" == 'CAST(COALESCE(SUM("tcp_rtt"), 0.0) AS DOUBLE)'

    def test_base_rollup_totals(self) -> None:
        s = metric_expressions.sum_or_zero
        assert s("requests") == "COALESCE(SUM(requests), 0)"
        assert s("num") == "COALESCE(SUM(num), 0)"

    def test_network_total_bytes(self) -> None:
        s = metric_expressions.sum_or_zero
        assert f"{s('resp_bytes')} AS total_bytes" == "COALESCE(SUM(resp_bytes), 0) AS total_bytes"
        assert f"{s('bytes')} AS total_bytes" == "COALESCE(SUM(bytes), 0) AS total_bytes"

    def test_value_live_tail_exprs(self) -> None:
        s = metric_expressions.sum_or_zero
        hit = metric_expressions.HIT_STATES
        assert (
            s("elapsed", zero="0.0", where="cache = 'MISS'")
            == "COALESCE(SUM(elapsed) FILTER (WHERE cache = 'MISS'), 0.0)"
        )
        assert (
            s("resp_bytes", where=f"cache IN {hit}")
            == "COALESCE(SUM(resp_bytes) FILTER (WHERE cache IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')), 0)"
        )
        assert s("resp_bytes") == "COALESCE(SUM(resp_bytes), 0)"
        assert (
            s("elapsed", zero="0.0", where=f"cache IN {hit}")
            == "COALESCE(SUM(elapsed) FILTER (WHERE cache IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')), 0.0)"
        )

    def test_usage_bytes_total(self) -> None:
        assert metric_expressions.coalesce("SUM(resp_bytes)", "0") == "COALESCE(SUM(resp_bytes), 0)"
        assert metric_expressions.coalesce("0", "0") == "COALESCE(0, 0)"
