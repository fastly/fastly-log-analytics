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


class VclBuilder:
    """Line- and section-joining helpers for VCL generators."""

    @staticmethod
    def join_lines(items: Iterable[str]) -> str:
        """``"\\n"``-join an iterable of already-rendered lines."""
        return "\n".join(items)

    @staticmethod
    def join_sections(sections: Iterable[str]) -> str:
        """``"\\n\\n"``-join sections, dropping blank/whitespace-only ones."""
        return "\n\n".join(s for s in sections if s.strip())
