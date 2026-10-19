"""Unit tests for the pieces ``execute_top_n_rollups`` is built from."""

from __future__ import annotations

from typing import NoReturn

import pytest

from backend.repositories.top_n_rollups import (
    SchemaSeeds,
    TopNPaths,
    build_rolled_sql,
    fold_rows,
    live_topn_fields,
    qualify_limit_expr,
    truncate_top_n,
)

SKIP = frozenset({"rid", "ttfb"})


class TestLiveTopnFields:
    def test_drops_skip_fields_and_keeps_order(self) -> None:
        assert live_topn_fields(["country", "rid", "city", "ttfb"], SKIP, None) == ["country", "city"]

    def test_optional_prefixes_need_a_custom_field(self) -> None:
        fields = ["cmcd_x", "edge_a", "rum_b", "fastly_req_id", "country"]
        assert live_topn_fields(fields, SKIP, None) == ["country"]

    def test_enabled_dashboard_custom_field_is_kept(self) -> None:
        cfg = {
            "custom_fields": [
                {"name": "edge_a"},
                {"name": "rum_b", "enabled": False},
                {"name": "cmcd_x", "show_in_dashboard": False},
            ]
        }
        assert live_topn_fields(["cmcd_x", "edge_a", "rum_b"], SKIP, cfg) == ["edge_a"]

    def test_custom_field_does_not_override_skip(self) -> None:
        assert live_topn_fields(["rid"], SKIP, {"custom_fields": [{"name": "rid"}]}) == []


class TestFoldRows:
    def test_sums_across_row_sets(self) -> None:
        got = fold_rows([("a", "x", 1), ("a", "y", 2)], [("a", "x", 4), ("b", "z", 1)], [])
        assert got == {"a": {"x": 5, "y": 2}, "b": {"z": 1}}

    def test_no_rows(self) -> None:
        assert fold_rows() == {}
        assert fold_rows([], []) == {}


class TestTruncateTopN:
    BY_FIELD = {"a": {"x": 5, "y": 9, "z": 1}, "b": {"p": 2}, "c": {}}

    def test_limit_applies_per_field_in_request_order(self) -> None:
        rows, empty = truncate_top_n(self.BY_FIELD, ["b", "a"], 2, None)
        assert rows == [("b", "p", 2), ("a", "y", 9), ("a", "x", 5)]
        assert empty == []

    def test_per_field_limit_widens_only_its_field(self) -> None:
        rows, _ = truncate_top_n(self.BY_FIELD, ["a", "b"], 1, {"a": 3})
        assert [r for r in rows if r[0] == "a"] == [("a", "y", 9), ("a", "x", 5), ("a", "z", 1)]
        assert [r for r in rows if r[0] == "b"] == [("b", "p", 2)]

    def test_missing_and_empty_fields_are_reported(self) -> None:
        rows, empty = truncate_top_n(self.BY_FIELD, ["ghost", "c", "b"], 5, None)
        assert rows == [("b", "p", 2)]
        assert empty == ["ghost", "c"]

    def test_ties_keep_insertion_order_when_not_truncated(self) -> None:
        rows, _ = truncate_top_n({"a": {"x": 1, "y": 1}}, ["a"], 5, None)
        assert [r[1] for r in rows] == ["x", "y"]


class TestSchemaSeeds:
    def test_seeds_win_and_nothing_is_fetched(self) -> None:
        seeds = SchemaSeeds(["a"], {"a": "VARCHAR"})
        seeds.ensure(self._boom, self._boom)
        assert (seeds.cols, seeds.types) == (["a"], {"a": "VARCHAR"})

    def test_fetches_once_and_reuses(self) -> None:
        calls: list[str] = []
        seeds = SchemaSeeds()

        def cols() -> list[str]:
            calls.append("cols")
            return ["a"]

        def types() -> dict[str, str]:
            calls.append("types")
            return {"a": "INT"}

        seeds.ensure(cols, types)
        seeds.ensure(cols, types)
        assert calls == ["cols", "types"]
        assert (seeds.cols, seeds.types) == (["a"], {"a": "INT"})

    def test_failed_column_lookup_skips_types_and_is_retried_by_the_next_step(self) -> None:
        calls: list[str] = []
        seeds = SchemaSeeds()

        def bad_cols() -> list[str]:
            calls.append("cols")
            raise RuntimeError("nope")

        def types() -> dict[str, str]:
            calls.append("types")
            return {}

        with pytest.raises(RuntimeError):
            seeds.ensure(bad_cols, types)
        with pytest.raises(RuntimeError):
            seeds.ensure(bad_cols, types)
        assert calls == ["cols", "cols"]

    def test_types_failure_keeps_the_columns_already_fetched(self) -> None:
        seeds = SchemaSeeds()

        def bad_types() -> dict[str, str]:
            raise RuntimeError("nope")

        with pytest.raises(RuntimeError):
            seeds.ensure(lambda: ["a"], bad_types)
        assert seeds.cols == ["a"]

    @staticmethod
    def _boom() -> NoReturn:
        raise AssertionError("must not be called")


class TestQualifyLimitExpr:
    def test_plain_limit(self) -> None:
        assert qualify_limit_expr(["a", "b"], 10, None) == "10"

    def test_case_only_for_requested_fields(self) -> None:
        got = qualify_limit_expr(["a", "b"], 10, {"a": 500, "ghost": 9})
        assert got == "CASE field WHEN 'a' THEN 500 ELSE 10 END"

    def test_overrides_for_unread_fields_collapse_to_plain_limit(self) -> None:
        assert qualify_limit_expr(["a"], 3, {"ghost": 9}) == "3"

    def test_quote_in_field_is_doubled(self) -> None:
        assert "WHEN 'o''k' THEN 5" in qualify_limit_expr(["o'k"], 1, {"o'k": 5})


class TestBuildRolledSql:
    def test_nothing_to_read(self) -> None:
        assert build_rolled_sql(TopNPaths(), ["a"], 10, None) is None
        assert not TopNPaths()

    def test_each_source_shape_is_its_own_branch_in_order(self) -> None:
        paths = TopNPaths(["/d.parquet"], ["/h.parquet"], ["/bh.parquet"], ["/bd.parquet"])
        sql = build_rolled_sql(paths, ["a", "b"], 10, None)
        assert sql is not None
        assert sql.count(" UNION ALL ") == 3
        order = [sql.index(p) for p in ("/d.parquet", "/h.parquet", "/bh.parquet", "/bd.parquet")]
        assert order == sorted(order)
        assert sql.count("hive_partitioning=1") == 2
        assert sql.count("hive_partitioning=0") == 2

    def test_only_bundles_are_field_filtered(self) -> None:
        sql = build_rolled_sql(TopNPaths(["/d.parquet"], [], ["/bh.parquet"], []), ["a", "o'k"], 10, None)
        assert sql is not None
        assert sql.count("WHERE field IN ('a', 'o''k')") == 1
        assert sql.index("WHERE field IN") > sql.index("/bh.parquet")

    def test_qualify_uses_per_field_case(self) -> None:
        sql = build_rolled_sql(TopNPaths(["/d.parquet"]), ["a", "b"], 10, {"a": 50})
        assert sql is not None
        assert sql.endswith("<= CASE field WHEN 'a' THEN 50 ELSE 10 END")
