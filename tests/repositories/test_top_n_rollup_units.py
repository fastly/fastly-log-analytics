"""Unit tests for the pieces ``execute_top_n_rollups`` is built from."""

from __future__ import annotations

from backend.repositories.top_n_rollups import fold_rows, live_topn_fields, truncate_top_n

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
