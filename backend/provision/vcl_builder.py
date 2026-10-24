"""Typed VCL assembly builder — the single place that knows how generated
VCL is escaped, indented, and glued together.

Before this, every generator rolled its own string-building strategy:
``"\\n".join(...)`` here, manual ``f"  {line}"`` indentation there,
``"\\n\\n".join(s for s in sections if s.strip())`` section-joining
somewhere else, and ad-hoc ``f'"{value}"'`` quoting with the escaping
check (if any) copy-pasted per call site. That drift is exactly how the
VCL-injection and ordering bugs catalogued in AGENTS.md slipped in.

:class:`VclBuilder` centralizes those mechanics:

  * line/section accumulation (``raw``, ``line``, ``lines``, ``blank``,
    ``section``),
  * deterministic joining (``build`` for lines, ``build_sections`` for
    ``"\\n\\n"``-separated sections that drops empties),
  * indentation (``indent`` static helper + the ``indent=`` arg on line
    appenders),
  * and the ESCAPING BOUNDARY (``quoted``): every value destined for a
    VCL double-quoted literal goes through
    :func:`backend.utils.vcl_safety.validate_quoted_literal` so the
    forbidden-char policy can't drift from the validators.

Trap #14 (regex RHS of ``~`` / ``!~`` must be a bare string LITERAL, not
a quoted-and-escaped value): do NOT use :meth:`quoted` to build a regex
match RHS. The regex value is validated separately (with
``allow_backslash=True``) and substituted verbatim into the ``"..."`` by
the caller. ``quoted`` is for ordinary string-literal values only.
"""

from __future__ import annotations

from collections.abc import Iterable

_INDENT_UNIT = "  "


class VclBuilder:
    """Accumulate VCL lines, then render deterministically."""

    def __init__(self, *, indent_unit: str = _INDENT_UNIT) -> None:
        self._lines: list[str] = []
        self._indent_unit = indent_unit

    # ── line mode ────────────────────────────────────────────────────────
    def raw(self, text: str) -> VclBuilder:
        """Append a block verbatim (internal newlines preserved as-is)."""
        self._lines.append(text)
        return self

    def line(self, text: str = "", *, indent: int = 0) -> VclBuilder:
        """Append a single line, optionally indented ``indent`` levels."""
        self._lines.append(f"{self._indent_unit * indent}{text}" if indent and text else text)
        return self

    def lines(self, items: Iterable[str], *, indent: int = 0) -> VclBuilder:
        """Append each item as its own line (see :meth:`line`)."""
        for item in items:
            self.line(item, indent=indent)
        return self

    def blank(self) -> VclBuilder:
        """Append a single empty line."""
        self._lines.append("")
        return self

    def build(self) -> str:
        """Render accumulated lines, ``"\\n"``-joined."""
        return "\n".join(self._lines)

    # ── static helpers ───────────────────────────────────────────────────
    @staticmethod
    def indent(text: str, levels: int = 1, *, unit: str = _INDENT_UNIT) -> str:
        """Indent every line of ``text`` by ``levels`` units.

        Blank lines are left empty (no trailing indent), matching how the
        generators indent content lines while emitting bare ``""`` blanks.
        """
        prefix = unit * levels
        return "\n".join(f"{prefix}{line}" if line else line for line in text.split("\n"))

    @staticmethod
    def join_lines(items: Iterable[str]) -> str:
        """``"\\n"``-join an iterable of already-rendered lines."""
        return "\n".join(items)

    @staticmethod
    def join_sections(sections: Iterable[str]) -> str:
        """``"\\n\\n"``-join sections, dropping blank/whitespace-only ones."""
        return "\n\n".join(s for s in sections if s.strip())
