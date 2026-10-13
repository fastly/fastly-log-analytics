"""Tests for the VclBuilder VCL-assembly primitives (B15 consolidation)."""

from __future__ import annotations

import pytest

from backend.provision.vcl_builder import VclBuilder
from backend.utils.vcl_safety import VclSafetyError


class TestLineMode:
    def test_raw_and_build(self) -> None:
        b = VclBuilder()
        b.raw("set req.http.A = 1;").raw("set req.http.B = 2;")
        assert b.build() == "set req.http.A = 1;\nset req.http.B = 2;"

    def test_line_indent(self) -> None:
        b = VclBuilder()
        b.line("outer").line("inner", indent=1).line("deeper", indent=2)
        assert b.build() == "outer\n  inner\n    deeper"

    def test_blank_line_not_indented(self) -> None:
        b = VclBuilder()
        b.line("x").blank().line("y", indent=1)
        assert b.build() == "x\n\n  y"

    def test_lines_bulk(self) -> None:
        b = VclBuilder()
        b.lines(["a", "b"], indent=1)
        assert b.build() == "  a\n  b"

    def test_join_lines_static(self) -> None:
        assert VclBuilder.join_lines(["a", "b", "c"]) == "a\nb\nc"


class TestSectionMode:
    def test_sections_joined_with_blank_line(self) -> None:
        b = VclBuilder()
        b.section("block1").section("block2")
        assert b.build_sections() == "block1\n\nblock2"

    def test_empty_sections_dropped(self) -> None:
        b = VclBuilder()
        b.section("block1").section("").section("   ").section("block2")
        assert b.build_sections() == "block1\n\nblock2"

    def test_join_sections_static_matches_legacy_idiom(self) -> None:
        sections = ["a", "", "  ", "b"]
        assert VclBuilder.join_sections(sections) == "\n\n".join(s for s in sections if s.strip())


class TestIndentStatic:
    def test_indents_each_content_line(self) -> None:
        assert VclBuilder.indent("a\nb") == "  a\n  b"

    def test_preserves_blank_lines(self) -> None:
        assert VclBuilder.indent("a\n\nb") == "  a\n\n  b"

    def test_levels(self) -> None:
        assert VclBuilder.indent("x", levels=2) == "    x"


class TestQuotedEscapingBoundary:
    def test_quotes_plain_value(self) -> None:
        assert VclBuilder().quoted("true") == '"true"'

    def test_routes_through_safety_policy(self) -> None:
        # a quote that would break out of the literal is rejected
        with pytest.raises(VclSafetyError):
            VclBuilder().quoted('a"b')

    def test_rejects_control_char(self) -> None:
        with pytest.raises(VclSafetyError):
            VclBuilder().quoted("tab\there")

    def test_allow_backslash_passthrough(self) -> None:
        assert VclBuilder().quoted(r"\.js", allow_backslash=True) == r'"\.js"'
