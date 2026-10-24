"""Tests for the VclBuilder VCL-assembly primitives (B15 consolidation)."""

from __future__ import annotations

from backend.provision.vcl_builder import VclBuilder


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
